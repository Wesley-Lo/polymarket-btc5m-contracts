"""Builds the LIVE runtime: arming rules, key loading, client, pre-flight stage 1, and the objects `live.Runtime` needs. Nothing here signs or reads a
credential directly (live_client does); this module only orders the steps and refuses to go on when one fails."""
import json
import time
from dataclasses import dataclass, replace

from .. import transport
from . import live_client as lc
from . import recover
from .alerts import Alerts
from .config import Mode
from .hook import LeanExecEngine, ExecEngine
from .layer import DryRunLayer
from .live_config import LiveConfig
from .live_layer import LiveLayer, live_exec_config
from .liveio import BgOutputs
from .outputs import Outputs
from .preflight import static_preflight
from .reconcile import Reconciler
from .redact import REDACTOR, install
from .rules import PilotRules
from .sender import Sender


class ArmingError(SystemExit):
    pass


def check_arming(mode, accept_flag, lcfg, strategy):
    """The four conditions that must ALL hold before LIVE starts (the fourth, the pre-flight, runs in prepare)."""
    if mode is not Mode.LIVE:
        return
    if not accept_flag:
        raise ArmingError("error: LIVE needs --i-accept-real-money (real orders will be sent with real money)")
    if not lcfg.armed:
        raise ArmingError("error: LIVE is not armed: set \"armed\": true in updown5m/execution/live_config.json (it ships as false)")
    if strategy not in ("v3", "v3.1"):
        raise ArmingError("error: LIVE is built for strategy v3.1 only")


@dataclass
class LiveBundle:
    client: object
    keys: object
    alerts: object
    lcfg: LiveConfig
    pre_text: str
    balance_usd: float


GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"


def market_tokens(start, get=transport.http_get_json, prefix="btc-updown-5m-"):
    """[Up token, Down token] of the 5-minute market starting at `start`, or None when Gamma does not list it (yet)."""
    m = get(GAMMA_MARKETS, params={"slug": f"{prefix}{int(start)}"})
    if not m:
        return None
    m = m[0]
    ids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    outs = json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
    return [ids[outs.index("Up")], ids[outs.index("Down")]]


def startup_warm(client, lcfg, log=print, get=transport.http_get_json, now=time.time):
    """Client warm-up at start: the SDK client is already connected (credentials derived); here it fetches the CURRENT market's metadata and signs a
    dummy BUY + SELL per token (never sent), so the signer and the metadata cache are hot before the first decision. Never fatal: the
    runtime warms every next market again at each roll and when its socket opens. -> True if it worked."""
    try:
        toks = market_tokens(int(now() // 300) * 300, get)
    except Exception as e:
        toks, err = None, f"{type(e).__name__}: {e}"
    else:
        err = "the current market is not listed on Gamma"
    if not toks:
        log(f"warm-up at start skipped ({err}); the runtime warms each market at the roll and when its socket opens")
        return False
    lines, bad = lc.warm_lines(client.warm(toks, refresh=True), "start", lcfg.build_alert_ms)
    for s in lines:
        log(REDACTOR.scrub(s))
    return not bad


def prepare(lcfg, log=print, client_factory=None, geo=lc.geoblock, warm_get=transport.http_get_json):
    """Load the keys, connect, run pre-flight stage 1. -> LiveBundle. Prints which key name is missing (never a value) and exits on any failure."""
    install()
    try:
        miss = lc.missing_keys()
    except RuntimeError as e:                                    # the key store is unreachable (no credentials / no access / no network)
        raise ArmingError(f"error: LIVE refuses to start, cannot read the keys: {e}") from None
    if miss:
        raise ArmingError(f"error: LIVE refuses to start, missing keys in the key store ({lcfg.key_store}, service updown5m): " + ", ".join(miss))
    keys = lc.load_keys()
    client = (client_factory or (lambda k, c: lc.LiveClient(k, c).connect()))(keys, lcfg)
    pre = static_preflight(lcfg, client, keys.get("funder"), transport.LOGS_ROOT.parent / "STOP", transport.LOGS_ROOT / lcfg.rule_stop_file,
                           log=log, geo=geo)
    log(REDACTOR.scrub(pre.text()))
    alerts = Alerts(lcfg, log=log)
    if not pre.ok:
        alerts.send("preflight", "LIVE pre-flight FAILED - not started", REDACTOR.scrub(pre.text()))
        alerts.close()
        client.close()
        raise ArmingError("error: LIVE pre-flight failed (see above); nothing was started")
    startup_warm(client, lcfg, log, warm_get)
    return LiveBundle(client, keys, alerts, lcfg, pre.text(), pre.balance_usd)


def build(runtime, params, cfg, bundle, on_fatal):
    """-> (engine, layer, out). Called from Runtime.__init__ once the session folder exists."""
    lcfg, session, sdir = bundle.lcfg, runtime.session, runtime.sdir
    live_cfg = live_exec_config(cfg, lcfg)
    out = BgOutputs(sdir, session)
    twin_out = Outputs(sdir / "twin", f"{session}-twin")
    twin_cfg = replace(live_cfg, mode="DRY_RUN", max_entries_per_hour=10 ** 6, daily_loss_limit_usd=10 ** 9, stuck_limit_per_day=10 ** 6,
                       kill_switch_file="NO_SUCH_FILE")
    twin = DryRunLayer(twin_cfg, twin_out, f"{session}-twin", Mode.DRY_RUN)
    sender = Sender(bundle.client)
    recon = Reconciler(bundle.client, lcfg, log=print)
    rules = PilotRules(lcfg, on_stop=lambda r, w: None)
    rec = recover.rebuild(recover.find_logs(transport.safe_path("sessions"), exclude=session, now_utc=runtime.started_wall / 1e9, max_age_h=cfg.recover_max_age_h),
                          only_mode="LIVE")
    rec.unresolved = []                     # the exchange is the truth for positions (pre-flight sold anything left); the log only seeds counters
    layer = LiveLayer(live_cfg, lcfg, out, session, sender, recon, bundle.alerts, twin, rules, recovered=rec, log=print, on_fatal=on_fatal)
    rules.on_stop = layer.on_rule_stop
    rules.now = lambda: layer.eng.now_utc if layer.eng is not None else 0.0
    engine_cls = LeanExecEngine if lcfg.lean else ExecEngine
    engine = engine_cls(params, runtime.sink, layer)
    layer.cash_start = layer.cash_now = bundle.balance_usd
    layer.preflight_text = bundle.pre_text
    return engine, layer, out, [twin_out]


def close(runtime, bundle, layer, twin_outs):
    try:
        layer.sender.close()
        layer.recon.close()
        bundle.alerts.close()
    finally:
        for o in twin_outs:
            o.close()
        bundle.client.close()
