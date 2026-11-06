"""python -m updown5m [--minutes N]   (headless collector + paper trader; Ctrl+C shuts down cleanly)"""
import argparse
import asyncio
import sys

from . import params as params_mod
from .execution import LiveTradingNotBuilt, Mode, load_config, resolve_mode
from .live import Runtime


def main():
    ap = argparse.ArgumentParser(prog="python -m updown5m",
                                 description="Read-only Polymarket 5-min BTC Up/Down recorder + paper trader. Nothing is traded.")
    ap.add_argument("--minutes", type=float, default=None, help="stop after N minutes (default: run until Ctrl+C)")
    ap.add_argument("--params", default=None, help="alternative params.json")
    ap.add_argument("--strategy", choices=["v1", "v2", "v3.0", "v3.1", "v3"], default="v3.1",
                    help="v3.1 (default, frozen 2026-10-31): lag strategy with flip-aware edge and flip_2s main track; v3.0: the first v3; "
                         "v2: TWAP model level-vs-market; v1: the original spot model. v1/v2/v3.0 are frozen profiles")
    ap.add_argument("--exec-mode", default=None, help="OFF (default), DRY_RUN (orders are only logged as intents; nothing is signed or sent) or LIVE "
                                                      "(REAL orders; needs --i-accept-real-money, \"armed\": true in execution/live_config.json, and the pre-flight)")
    ap.add_argument("--i-accept-real-money", action="store_true", help="required for --exec-mode LIVE")
    ap.add_argument("--exec-config", default=None, help="alternative execution/config.json")
    ap.add_argument("--profile", choices=["default", "vm"], default="default",
                    help="vm: execution/config_vm.json + live_config_vm.json (no raw feed recording, secrets from Google Secret Manager); "
                         "the VM bundle ships these under the standard names, so it does not need the flag")
    rr = ap.add_mutually_exclusive_group()
    rr.add_argument("--raw-record", dest="raw_record", action="store_true", default=None, help="write the raw feed recordings (default of the default profile)")
    rr.add_argument("--no-raw-record", dest="raw_record", action="store_false", help="do not write the raw feed recordings (default of the vm profile); "
                                                                                       "no decision and no execution-layer output depends on them")
    ap.add_argument("--no-clock-v2", action="store_true", help="do not record the warm-connection clock feed clock_offset_v2")
    a = ap.parse_args()
    params = params_mod.load(a.params, a.strategy)
    from .execution import config as exec_config_mod
    from .execution import live_config as live_config_mod
    exec_config_mod.use_profile(a.profile)
    live_config_mod.use_profile(a.profile)
    try:
        cfg = load_config(a.exec_config)
        mode = resolve_mode(a.exec_mode or cfg.mode)
    except (LiveTradingNotBuilt, ValueError) as e:
        raise SystemExit(f"error: {e}")
    if mode is Mode.DRY_RUN and a.strategy not in ("v3", "v3.1"):
        raise SystemExit("error: the dry-run layer is built for strategy v3.1 only")
    if (mode is Mode.LIVE or (a.profile == "vm" and mode is Mode.DRY_RUN)) and params_mod.sha256_of(params) != params_mod.FROZEN_SHA256["v3.1"]:
        raise SystemExit("error: LIVE and the vm profile run the frozen v3.1 params only (sha256 mismatch); --params must not change them")
    if a.profile == "vm" and a.strategy not in ("v3", "v3.1"):
        raise SystemExit("error: the vm profile runs strategy v3.1 only")
    raw_record = cfg.raw_record if a.raw_record is None else a.raw_record
    live = None
    if mode is Mode.LIVE:
        from .execution import livewire
        from .execution.live_config import load_live_config
        lcfg = load_live_config()
        try:
            livewire.check_arming(mode, a.i_accept_real_money, lcfg, a.strategy)      # flag + armed config + strategy, else refuse
            live = livewire.prepare(lcfg)                                              # keys, client, pre-flight stage 1, else refuse
        except livewire.ArmingError as e:
            print(e, file=sys.stderr, flush=True)
            e.code = 78                       # EX_CONFIG: a refusal to start; the systemd unit does not restart on it (RestartPreventExitStatus=78)
            raise

    async def amain():
        await Runtime(params, a.minutes, exec_mode=mode, exec_cfg=cfg, clock_v2=not a.no_clock_v2, live=live, raw_record=raw_record).run()

    asyncio.run(amain())


if __name__ == "__main__":
    main()
