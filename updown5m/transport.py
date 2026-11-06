"""The ONLY module (with execution/live_client.py, the official SDK's own connection) that touches the network or opens files for writing.

Hard rules enforced here:
  * HTTP GET and public websockets only, to an allowlist of hosts (anything else raises SafetyError).
  * No order / auth / signing code exists anywhere in the package except execution/live_client.py. Websocket sends are limited to
    subscriptions and the text keepalive "PING", and are rejected if they carry credential-like keys.
  * The one non-HTTP channel is SMTP over SSL to SMTP_HOSTS (alert e-mail); an SMTP host is never reachable through the HTTP/WS allowlist.
  * Files may only be written inside updown5m/logs/. A registered text filter (secret redaction) is applied to every text write.
"""
import json
import smtplib
import ssl
import time
from collections import deque
from email.message import EmailMessage
from pathlib import Path
from urllib.parse import urlparse

import httpx
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.frames import Opcode

ALLOWED_HOSTS = frozenset({
    "data-api.binance.vision",
    "data-stream.binance.vision",
    "api.binance.com",
    "stream.binance.com",
    "gamma-api.polymarket.com",
    "clob.polymarket.com",
    "ws-subscriptions-clob.polymarket.com",
    "ws-feed.exchange.coinbase.com",  # Coinbase Exchange public market-data websocket (no auth)
    "api.exchange.coinbase.com",      # Coinbase Exchange public REST
    "fstream.binance.com",            # Binance USD-M futures public market-data websocket (RECORD ONLY: perp bookTicker / aggTrade)
    "polymarket.com",                 # GET https://polymarket.com/api/geoblock only (the LIVE pre-flight's eligibility check)
})
SMTP_HOSTS = frozenset({"smtp.gmail.com"})      # alert e-mail only (SMTP over SSL, port 465); never an HTTP/WS host
LOGS_ROOT = Path(__file__).resolve().parent / "logs"
_FORBIDDEN_WS_KEYS = {"auth", "apikey", "api_key", "secret", "signature", "passphrase", "key", "order", "orders", "token"}
_ALLOWED_WS_TYPES = {"subscribe", "market"}


class SafetyError(Exception):
    pass


def check_url(url):
    u = urlparse(url)
    if u.scheme not in ("http", "https", "ws", "wss"):
        raise SafetyError(f"scheme not allowed: {u.scheme!r}")
    if u.username or u.password:
        raise SafetyError("credentials in URL are not allowed")
    if u.hostname not in ALLOWED_HOSTS:
        raise SafetyError(f"host not allowlisted: {u.hostname}")
    return url


def safe_path(*parts, root=None):
    base = Path(root or LOGS_ROOT).resolve()
    p = Path(base, *parts).resolve()
    if p != base and base not in p.parents:
        raise SafetyError(f"refusing to write outside {base}: {p}")
    return p


_TEXT_FILTER = None        # fn(str) -> str, applied to every text write (the LIVE secret redactor registers itself here)


def set_text_filter(fn):
    global _TEXT_FILTER
    _TEXT_FILTER = fn


class _FilteredFile:
    """Text file wrapper: every write goes through the registered filter first. Everything else is the underlying file."""

    def __init__(self, f):
        self._f = f

    def write(self, s):
        return self._f.write(_TEXT_FILTER(s) if isinstance(s, str) else s)

    def writelines(self, lines):
        for s in lines:
            self.write(s)

    def __getattr__(self, name):
        return getattr(self._f, name)

    def __enter__(self):
        self._f.__enter__()
        return self

    def __exit__(self, *a):
        return self._f.__exit__(*a)

    def __iter__(self):
        return iter(self._f)


def http_get_json(url, params=None, timeout=5.0):
    """One synchronous GET (allowlisted host, no redirects, no custom headers) -> parsed JSON. Used by the LIVE pre-flight / plumbing tool."""
    check_url(url)
    with httpx.Client(timeout=timeout, follow_redirects=False, headers={"User-Agent": "updown5m-readonly/1.0"}) as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        return r.json()


def smtp_send(host, port, user, password, to, subject, body, timeout=15.0):
    """Send one plain-text e-mail over SMTP-over-SSL to an allowlisted SMTP host. Raises on any failure (the caller isolates it)."""
    if host not in SMTP_HOSTS:
        raise SafetyError(f"smtp host not allowlisted: {host}")
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = user, to, subject
    msg.set_content(body)
    with smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl.create_default_context()) as s:
        s.login(user, password)
        s.send_message(msg)


PROTECTED_TOP = ("sessions", "raw")    # recorded data: an existing file in here is never truncated by "w"/"wb"


def is_protected(p):
    rel = Path(p).resolve().relative_to(LOGS_ROOT.resolve()).parts
    return bool(rel) and rel[0] in PROTECTED_TOP


def open_for_write(path, mode="a", *, allow_overwrite=False, **kw):
    """The only way the package opens a file for writing; creates parent dirs inside logs/ only.

    An existing file under logs/sessions or logs/raw cannot be opened with a truncating mode unless the caller (the live
    session rewriting its OWN summary) passes allow_overwrite=True. Replay / compare / study tools therefore cannot
    clobber a recorded session even by accident."""
    p = safe_path(path)
    if "w" in mode and not allow_overwrite and p.exists() and is_protected(p):
        raise SafetyError(f"refusing to overwrite an existing recorded file: {p}")
    p.parent.mkdir(parents=True, exist_ok=True)
    f = open(p, mode, **kw)
    return _FilteredFile(f) if (_TEXT_FILTER is not None and "b" not in mode) else f


def make_dirs(path):
    p = safe_path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def require_new_dir(path):
    """Create `path` for a tool's output; it must not exist yet (or be an empty directory)."""
    p = safe_path(path)
    if p.exists() and (not p.is_dir() or any(p.iterdir())):
        raise SafetyError(f"output folder already exists and is not empty: {p}")
    p.mkdir(parents=True, exist_ok=True)
    return p


def new_report_dir(kind, stamp):
    """logs/reports/<kind>_<stamp>/ (a fresh folder per run; a numeric suffix is added on a same-second collision)."""
    base = safe_path("reports")
    base.mkdir(parents=True, exist_ok=True)
    for i in range(100):
        p = base / (f"{kind}_{stamp}" + (f"_{i}" if i else ""))
        try:
            p.mkdir()
            return p
        except FileExistsError:
            continue
    raise SafetyError("could not create a fresh report folder")


class HttpSession:
    """GET-only client; no redirects (a redirect could leave the allowlist); no custom headers."""

    def __init__(self, timeout=5.0):
        self._c = httpx.AsyncClient(timeout=timeout, follow_redirects=False,
                                    headers={"User-Agent": "updown5m-readonly/1.0"})

    async def get(self, url, params=None):
        check_url(url)
        return await self._c.get(url, params=params)

    async def aclose(self):
        await self._c.aclose()


class StampedConnection(ClientConnection):
    """Stamps each complete data message when its last frame is parsed off the socket (before our reader runs)."""

    def process_event(self, event):
        if getattr(event, "fin", False) and event.opcode in (Opcode.TEXT, Opcode.BINARY, Opcode.CONT):
            self.__dict__.setdefault("_stamps", deque()).append(time.perf_counter_ns())
        super().process_event(event)

    def pop_stamp(self):
        st = self.__dict__.get("_stamps")
        return st.popleft() if st else None


def ws_connect(url, ping_interval=20):
    check_url(url)
    if not url.startswith("wss://"):
        raise SafetyError("websockets must be wss://")
    return websockets.connect(url, create_connection=StampedConnection, max_queue=None, compression=None,
                              max_size=2 ** 23, ping_interval=ping_interval, open_timeout=10, close_timeout=2)


def _check_ws_payload(msg):
    if msg == "PING":
        return
    d = json.loads(msg) if isinstance(msg, str) else None
    if not isinstance(d, dict):
        raise SafetyError("only subscription dicts and PING may be sent")
    if d.get("type") not in _ALLOWED_WS_TYPES:
        raise SafetyError(f"websocket message type not allowed: {d.get('type')}")
    bad = {k.lower() for k in d} & _FORBIDDEN_WS_KEYS
    if bad:
        raise SafetyError(f"credential/order-like keys not allowed: {bad}")


async def ws_send(ws, msg):
    _check_ws_payload(msg)
    await ws.send(msg)
