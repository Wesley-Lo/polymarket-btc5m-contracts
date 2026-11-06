"""The ONLY file in the package that holds credentials, signs, or names an order endpoint (a test enforces this; see tests/test_live.py).

It wraps the official unified Polymarket SDK (`polymarket-client`, pinned in requirements-live.txt). The SDK itself creates the L1/L2
authentication, EIP-712-signs orders and talks to the CLOB; this file only (1) reads the secrets from the Windows Credential Manager
(`keyring`, service "updown5m"), (2) builds + signs a FAK order and posts it as two separately timed steps, (3) asks read-only questions
(trades, positions, balances, geoblock), and (4) turns every SDK outcome into plain data for the rest of the package.

Nothing here logs or prints a secret value. Every secret is registered with the redactor the moment it is loaded."""
import time
from dataclasses import dataclass, field
from decimal import Decimal

from .. import transport
from .redact import REDACTOR

SERVICE = "updown5m"
KEY_NAMES = ("pk", "api_key", "api_secret", "api_passphrase", "funder", "signature_type", "gmail_app_password")
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
# docs.polymarket.com/api-reference/geoblock (read 2026-11-01): regions where NEW orders are rejected (blocked, or close-only on the API).
BLOCKED_REGIONS = {"IR", "SY", "CU", "KP", "UA-43", "UA-14", "UA-09"}
CLOSE_ONLY_API = {"AR", "AU", "BY", "BE", "BI", "BR", "CA-BC", "CA-ON", "CA-AB", "CA-QC", "CF", "CD", "ET", "FR", "DE", "IQ", "IT", "LB", "LY",
                  "MM", "NZ", "NI", "PL", "RU", "SG", "SO", "SK", "SS", "SD", "TW", "TH", "GB", "US", "UM", "VE", "YE", "ZW"}
USDC_UNITS = Decimal(1_000_000)


# ====================================================================== secrets
class GcpStore:
    """Read-only secrets from Google Secret Manager, for the VM (live_config `key_store: "gcp"`). Authentication is the VM's attached
    service account (Application Default Credentials from the metadata server); that account needs only "Secret Manager Secret Accessor" on
    the seven secrets. A value is held in memory only (never written to disk) and is read once per process."""

    def __init__(self, project="", prefix="updown5m-"):
        self.project, self.prefix, self._c, self._cache = project, prefix, None, {}

    def _client(self):
        if self._c is None:
            try:
                from google.cloud import secretmanager
            except ImportError as e:                            # pragma: no cover
                raise RuntimeError("google-cloud-secret-manager is not installed (pip install -r requirements-vm.txt)") from e
            try:
                c = secretmanager.SecretManagerServiceClient(transport="rest")
                if not self.project:
                    import google.auth
                    self.project = google.auth.default()[1] or ""
            except Exception as e:
                raise RuntimeError(f"cannot use Google Secret Manager here ({type(e).__name__}): run this on the VM that has the service account "
                                   "attached (access scope cloud-platform), or use key_store keyring on the laptop") from None
            if not self.project:
                raise RuntimeError("cannot tell the Google Cloud project: set gcp_project in live_config.json")
            self._c = c
        return self._c

    def get_password(self, service, name):
        if name in self._cache:
            return self._cache[name]
        from google.api_core import exceptions as gx
        c = self._client()
        path = f"projects/{self.project}/secrets/{self.prefix}{name}/versions/latest"
        try:
            v = c.access_secret_version(request={"name": path}).payload.data.decode("utf-8").strip()
        except gx.NotFound:
            return None
        except gx.PermissionDenied as e:
            raise RuntimeError(f"Secret Manager refused access to {self.prefix}{name}: grant the VM's service account "
                               f"roles/secretmanager.secretAccessor on that secret ({type(e).__name__})") from None
        except Exception as e:
            raise RuntimeError(f"could not read {self.prefix}{name} from Secret Manager ({type(e).__name__}); check the network, the API is enabled "
                               "and the VM's access scope") from None
        self._cache[name] = v
        return v

    def set_password(self, service, name, value):
        raise RuntimeError("the Google Secret Manager backend is read-only here: create secrets with gcloud (see README_VM.md)")


_STORE = None


def backend_name():
    from .live_config import load_live_config
    return load_live_config().key_store


def _kr():
    """The secret store object (get_password / set_password): the Windows Credential Manager (keyring) or Google Secret Manager, by config."""
    global _STORE
    if _STORE is None:
        from .live_config import load_live_config
        cfg = load_live_config()
        if cfg.key_store == "gcp":
            _STORE = GcpStore(cfg.gcp_project, cfg.gcp_key_prefix)
        elif cfg.key_store == "keyring":
            try:
                import keyring
            except ImportError as e:                                    # pragma: no cover
                raise RuntimeError("the `keyring` package is not installed (pip install -r updown5m/requirements-live.txt)") from e
            _STORE = keyring
        else:
            raise ValueError(f"unknown key_store {cfg.key_store!r}; use keyring or gcp")
    return _STORE


def read_key(name):
    """One value from the Credential Manager, or None. Never printed."""
    if name not in KEY_NAMES:
        raise KeyError(f"unknown key name {name!r}")
    v = _kr().get_password(SERVICE, name)
    return v if v else None


def missing_keys(names=KEY_NAMES):
    return [n for n in names if not read_key(n)]


def prompt_and_store_key(name):
    """Hidden-input prompt, stored straight into the Credential Manager. Nothing is echoed or returned."""
    import getpass
    if name not in KEY_NAMES:
        raise KeyError(f"unknown key name {name!r}")
    if backend_name() != "keyring":
        raise SystemExit("`keys set` stores into the Windows Credential Manager only; on the VM create the secret with gcloud (README_VM.md)")
    v = getpass.getpass(f"value for {name} (input hidden): ")
    if not v:
        raise SystemExit("empty value: nothing stored")
    _kr().set_password(SERVICE, name, v.strip())
    print(f"stored {name} in the Windows Credential Manager (service {SERVICE})")


@dataclass
class KeyBag:
    values: dict = field(default_factory=dict, repr=False)

    def __repr__(self):
        return f"KeyBag(keys={sorted(self.values)})"

    def get(self, name):
        return self.values.get(name)


def load_keys(names=KEY_NAMES):
    """Read every key, register each value with the redactor. -> Secrets; raises KeyError(list of missing names) if any is absent."""
    vals, miss = {}, []
    for n in names:
        v = read_key(n)
        if not v:
            miss.append(n)
        else:
            vals[n] = v
            if n != "signature_type":
                REDACTOR.register(v, n)
    if miss:
        raise KeyError(miss)
    return KeyBag(vals)


def mail_password():
    """The Gmail app password for alerts.py (the only reader of this key besides load_secrets)."""
    v = read_key("gmail_app_password")
    if v:
        REDACTOR.register(v, "gmail_app_password")
    return v


# ====================================================================== results
@dataclass
class SendResult:
    ok: bool = False
    sent: bool = False                  # the POST was actually issued (False: failed while building/signing, nothing left the machine)
    kind: str = ""                      # "" | rejected | rate_limited | unknown (no answer) | error (built nothing)
    order_id: str = ""
    status: str = ""                    # live | matched | delayed (accepted)
    making: Decimal = Decimal(0)        # BUY: USD paid;  SELL: shares given
    taking: Decimal = Decimal(0)        # BUY: shares got; SELL: USD received
    code: str = ""
    message: str = ""
    t_build_ns: int = 0
    t_built_ns: int = 0
    t_send_ns: int = 0
    t_resp_ns: int = 0
    params: dict = field(default_factory=dict)


@dataclass
class TradeInfo:
    id: str
    order_id: str
    side: str
    price: Decimal
    size: Decimal
    fee_rate_bps: Decimal
    status: str                         # MATCHED | MINED | CONFIRMED | RETRYING | FAILED
    matched_at_ms: int = 0
    tx: str = ""


WARM_BUY = {"amount": Decimal("2.5"), "max_price": Decimal("0.5")}      # dummy orders for the warm-up: signed, never posted
WARM_SELL = {"shares": Decimal("5"), "min_price": Decimal("0.5")}


def warm_lines(res, label, limit_ms):
    """Warm-up results -> (printable lines, any_problem). A problem = a failed warm-up or a post-fetch sign slower than `limit_ms`."""
    lines, bad = [], False
    for w in res:
        slow = [s for s in w["sign_ms"] if s > limit_ms]
        bad = bad or not w["ok"] or bool(slow)
        state = "OK" if w["ok"] and not slow else ("FAILED: " + w["error"]) if not w["ok"] else f"SLOW sign (> {limit_ms:g} ms)"
        lines.append(f"warm-up {label} token {w['token'][:12]}..: metadata fetch {w['fetch_ms']} ms, dummy sign BUY/SELL {w['sign_ms']} ms "
                     f"(not sent): {state}")
    return lines, bad


def geo_verdict(g):
    """geoblock reply -> (allowed_to_open, region_label, reason)."""
    c, r = str(g.get("country") or ""), str(g.get("region") or "")
    label = f"{c}-{r}" if c and r else c
    if g.get("blocked") is True:
        return False, label, "polymarket reports this IP as blocked"
    if c in BLOCKED_REGIONS or label in BLOCKED_REGIONS:
        return False, label, "this region is fully blocked"
    if c in CLOSE_ONLY_API or label in CLOSE_ONLY_API:
        return False, label, "this region is close-only on the API: new orders are rejected (only closing is allowed)"
    if c == "CA" and not r:
        return False, label, "Canada without a province: Ontario, BC, Alberta and Quebec are close-only, so eligibility cannot be confirmed"
    return True, label, ""


def geoblock():
    return transport.http_get_json(GEOBLOCK_URL, timeout=8.0)


# ====================================================================== the SDK wrapper
class LiveClient:
    """Interface used by the rest of the package (tests substitute a mock with the same methods)."""

    def __init__(self, secrets, cfg, sdk=None):
        self.cfg = cfg
        self._s = secrets
        self._sdk = sdk
        self.c = None

    def connect(self):
        sdk = self._sdk
        if sdk is None:
            import polymarket as sdk                              # noqa: pinned in requirements-live.txt
            self._sdk = sdk
        have = [self._s.get(k) for k in ("api_key", "api_secret", "api_passphrase")]
        creds = sdk.ApiKeyCreds(key=have[0], secret=have[1], passphrase=have[2]) if all(have) else None
        self.c = sdk.SecureClient.create(private_key=self._s.get("pk"), wallet=self._s.get("funder"), credentials=creds)
        return self

    def close(self):
        if self.c is not None:
            try:
                self.c.close()
            finally:
                self.c = None

    # ---- identity
    def key_address(self):
        return str(self.c.signer.address)

    def account_address(self):
        return str(self.c.wallet)

    def default_account_address(self):
        """The Deposit Wallet the SDK derives from the private key ALONE (no wallet argument). The check against `funder` is only meaningful with this."""
        sdk = self._sdk
        c = sdk.SecureClient.create(private_key=self._s.get("pk"))
        try:
            return str(c.wallet)
        finally:
            c.close()

    def account_kind(self):
        wt = self.c.wallet_type
        v = getattr(wt, "value", wt)
        try:
            return int(v)
        except (TypeError, ValueError):
            n = str(getattr(wt, "name", wt)).upper()
            return 3 if "DEPOSIT" in n else 1 if "PROXY" in n else 2 if ("SAFE" in n or "GNOSIS" in n) else 0

    def derived_credentials(self):
        """The API credentials the SDK derived at connect(): (key, secret, passphrase) for keys.py to store."""
        c = self.c.credentials
        return c.key, c.secret, c.passphrase

    # ---- warm-up (keeps network lookups OFF the order path)
    def _prefetch(self, token_id, refresh):
        """Fill the SDK's order-metadata cache for `token_id`. What the FIRST create_market_order of a market otherwise does inline (measured:
        BUY sign 437 ms, then SELL on the same token 5 ms): GET /markets-by-token/{token} (token -> condition, cached for good) and
        GET /clob-markets/{condition} (tick size, neg-risk, platform fee info; cached 10 min per market, and the condition of BOTH tokens is
        stored from that one answer). refresh=True forces the second GET so the 10-minute lifetime starts now and covers the whole window."""
        ctx = getattr(self.c, "_ctx", None)
        cache = getattr(ctx, "order_metadata", None)
        if cache is None:                                         # other SDK shape: the dummy signs below still fill the cache
            return
        (cache.fetch_current_market if refresh else cache.resolve_market)(ctx, token_id=token_id)

    def warm(self, tokens, refresh=False):
        """For the tokens of ONE market: pre-fetch + cache the metadata, then build and sign a BUY and a SELL per token that are DISCARDED
        (create_market_order never posts; post_order is never called here). -> [dict(token, ok, fetch_ms, sign_ms=[buy, sell], error)]."""
        out = []
        for i, tok in enumerate(tokens):
            w = {"token": tok, "ok": False, "fetch_ms": None, "sign_ms": [], "error": ""}
            try:
                t0 = time.perf_counter_ns()
                self._prefetch(tok, refresh and i == 0)
                w["fetch_ms"] = round((time.perf_counter_ns() - t0) / 1e6, 2)
                for side, kw in (("BUY", WARM_BUY), ("SELL", WARM_SELL)):
                    t0 = time.perf_counter_ns()
                    self.c.create_market_order(side=side, asset_id=tok, order_type="FAK", **kw)
                    w["sign_ms"].append(round((time.perf_counter_ns() - t0) / 1e6, 2))
                w["ok"] = True
            except Exception as e:
                w["error"] = f"{type(e).__name__}: {str(e)[:200]}"
            out.append(w)
        return out

    def prepare(self, token_id):
        """One token, no forced refresh. Returns True if it worked."""
        return self.warm([token_id])[0]["ok"]

    # ---- orders

    def buy(self, token_id, usd, max_price):
        return self._send("BUY", token_id, {"amount": Decimal(str(usd)), "max_price": Decimal(str(max_price))})

    def sell(self, token_id, shares, min_price):
        return self._send("SELL", token_id, {"shares": Decimal(str(shares)), "min_price": Decimal(str(min_price))})

    def _send(self, side, token_id, kw):
        sdk = self._sdk
        r = SendResult(params={"side": side, "order_type": "FAK", "asset_id": token_id, **{k: str(v) for k, v in kw.items()}})
        r.t_build_ns = time.perf_counter_ns()
        try:
            signed = self.c.create_market_order(side=side, asset_id=token_id, order_type="FAK", **kw)
        except Exception as e:                                   # nothing was sent
            r.t_built_ns = r.t_send_ns = r.t_resp_ns = time.perf_counter_ns()
            r.kind, r.code, r.message = "error", type(e).__name__, str(e)[:300]
            return r
        r.t_built_ns = r.t_send_ns = time.perf_counter_ns()
        r.sent = True
        try:
            resp = self.c.post_order(signed)
        except Exception as e:
            r.t_resp_ns = time.perf_counter_ns()
            name = type(e).__name__
            r.code, r.message = name, str(e)[:300]
            r.kind = "rate_limited" if name == "RateLimitError" else "rejected" if name in ("RequestRejectedError", "UserInputError") else "unknown"
            return r
        r.t_resp_ns = time.perf_counter_ns()
        if getattr(resp, "ok", False):
            r.ok, r.order_id, r.status = True, str(resp.order_id), str(resp.status)
            r.making, r.taking = Decimal(resp.making_amount), Decimal(resp.taking_amount)
        else:
            r.kind, r.code, r.message = "rejected", str(getattr(resp, "code", "")), str(getattr(resp, "message", ""))[:300]
        return r

    # ---- read-only queries
    def trades_for_order(self, order_id, token_id, pages=2):
        out = []
        n = 0
        for page in self.c.list_account_trades(asset_id=token_id):
            for t in page.items:
                if str(t.taker_order_id) == str(order_id):
                    out.append(TradeInfo(str(t.id), str(order_id), str(t.side), Decimal(t.price), Decimal(t.size), Decimal(t.fee_rate_bps),
                                         str(t.status), int(t.matched_at.timestamp() * 1000), str(t.transaction_hash or "")))
            n += 1
            if n >= pages:
                break
        return out

    def trades_since(self, token_id, since_ms, pages=2):
        out = []
        n = 0
        for page in self.c.list_account_trades(asset_id=token_id):
            for t in page.items:
                ms = int(t.matched_at.timestamp() * 1000)
                if ms >= since_ms:
                    out.append(TradeInfo(str(t.id), str(t.taker_order_id), str(t.side), Decimal(t.price), Decimal(t.size), Decimal(t.fee_rate_bps),
                                         str(t.status), ms, str(t.transaction_hash or "")))
            n += 1
            if n >= pages:
                break
        return out

    def open_orders(self):
        return [{"id": str(o.id), "asset_id": str(o.asset_id), "side": str(o.side), "price": str(o.price),
                 "size": str(o.original_size), "matched": str(o.size_matched), "status": str(o.status)}
                for page in self.c.list_open_orders() for o in page.items]

    def shares(self, token_id):
        b = self.c.get_balance_allowance(asset_type=self.cfg.conditional_asset_type, asset_id=token_id)
        return float(Decimal(b.balance) / USDC_UNITS)

    def collateral(self):
        """-> (balance in USD, {spender: allowance in USD})."""
        b = self.c.get_balance_allowance(asset_type="COLLATERAL")
        return float(Decimal(b.balance) / USDC_UNITS), {str(k): float(Decimal(v) / USDC_UNITS) for k, v in b.allowances.items()}

    def positions(self, slug_prefix):
        out = []
        for page in self.c.list_positions(user=self.account_address(), status="OPEN"):
            for p in page.items:
                if (p.slug or "").startswith(slug_prefix) and float(p.current_size) > 0:
                    out.append({"asset_id": str(p.asset_id), "slug": p.slug, "size": float(p.current_size), "avg_price": float(p.avg_price)})
        return out

    def closed_only(self):
        return bool(self.c.get_closed_only_mode())

    def cancel_everything(self):
        return self.c.cancel_all()


# ====================================================================== helpers for keys.py (they need the private key, so they live here)
def public_address_of_stored_key():
    """Public address of the stored `pk` (derived offline; the key itself is never returned or printed)."""
    from eth_account import Account
    return str(Account.from_key(read_key("pk")).address)


def probe_account_address():
    """The Deposit Wallet address the SDK derives for the stored `pk` (needs the network: the SDK authenticates while creating the client)."""
    import polymarket as sdk
    c = sdk.SecureClient.create(private_key=read_key("pk"))
    try:
        return str(c.wallet)
    finally:
        c.close()


def derive_and_store_api_credentials():
    """Let the SDK derive the account's API credentials (L1-signed request) and store them as api_key / api_secret / api_passphrase."""
    import polymarket as sdk
    if backend_name() != "keyring":
        raise SystemExit("`keys derive` stores into the Windows Credential Manager only; derive on the laptop, then `keys push-gcp`")
    c = sdk.SecureClient.create(private_key=read_key("pk"), wallet=read_key("funder") or None)
    try:
        k, s, p = c.credentials.key, c.credentials.secret, c.credentials.passphrase
    finally:
        c.close()
    kr = _kr()
    kr.set_password(SERVICE, "api_key", k)
    kr.set_password(SERVICE, "api_secret", s)
    kr.set_password(SERVICE, "api_passphrase", p)
    for n, v in (("api_key", k), ("api_secret", s), ("api_passphrase", p)):
        REDACTOR.register(v, n)
    return True


def push_to_gcp(project, prefix="updown5m-", names=KEY_NAMES, gcloud="gcloud"):
    """LAPTOP -> Secret Manager: copy the stored keys into Google Secret Manager by piping each value to `gcloud secrets ... --data-file=-`
    (stdin only: the value is never printed, never in argv / shell history, never written to a file). Creates the secret, or adds a version."""
    import shutil
    import subprocess
    if backend_name() != "keyring":
        raise SystemExit("push-gcp reads from the laptop's Credential Manager (key_store keyring)")
    # On Windows PowerShell resolves `gcloud` to gcloud.ps1, which subprocess cannot run; which() honours PATHEXT and finds gcloud.cmd.
    exe = shutil.which(gcloud) or shutil.which("gcloud.cmd")
    if not exe:
        raise SystemExit(f"could not find the gcloud CLI ({gcloud!r} or 'gcloud.cmd') on PATH: install the Google Cloud SDK or add its bin folder to PATH")
    gcloud = exe
    done = []
    for n in names:
        v = read_key(n)
        if not v:
            raise SystemExit(f"{n} is missing in the Credential Manager: nothing pushed for it")
        sid = f"{prefix}{n}"
        base = [gcloud, "secrets"]
        r = subprocess.run(base + ["describe", sid, "--project", project, "--format=value(name)"], capture_output=True, text=True)
        if r.returncode == 0:
            cmd = base + ["versions", "add", sid, "--project", project, "--data-file=-"]
        else:
            cmd = base + ["create", sid, "--project", project, "--replication-policy=automatic", "--data-file=-"]
        r = subprocess.run(cmd, input=v.encode("utf-8"), capture_output=True)
        if r.returncode != 0:
            raise SystemExit(f"gcloud failed for {sid} (exit {r.returncode}): {REDACTOR.scrub(r.stderr.decode('utf-8', 'replace')[-300:])}")
        done.append(sid)
    return done
