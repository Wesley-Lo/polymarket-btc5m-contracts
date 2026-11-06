"""LIVE startup pre-flight, stage 1 (before any feed is started). LIVE refuses to start if any check fails. Stage 2 (clock, feeds, latency) needs
live data and runs inside the layer (`LiveLayer.runtime_preflight`); entries stay blocked until it passes.

Checks: all 7 keys present | SDK account address equals the configured `funder` and the account kind equals the configured signature type |
geoblock (new orders are rejected from blocked / close-only regions) | the exchange is not in close-only mode for this account | pUSD balance and
allowances sufficient | no open orders | no leftover BTC-5m positions (a leftover position is sold first with the normal floor, else refuse) |
the STOP and RULE_STOP files are absent."""
import time
from dataclasses import dataclass, field

from .. import transport
from ..book import TICK
from . import live_client as lc
from . import orders as O

CLOB_BOOK = "https://clob.polymarket.com/book"
PREFIX = "btc-updown-5m"


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Pre:
    checks: list = field(default_factory=list)
    balance_usd: float = None

    @property
    def ok(self):
        return all(c.ok for c in self.checks)

    def add(self, name, ok, detail=""):
        self.checks.append(Check(name, bool(ok), detail))
        return bool(ok)

    def text(self):
        lines = ["LIVE pre-flight (stage 1)"] + [f"  [{'OK  ' if c.ok else 'FAIL'}] {c.name}: {c.detail}" for c in self.checks]
        lines.append("RESULT: " + ("ALL PASSED" if self.ok else "FAILED - LIVE will not start"))
        return "\n".join(lines)


def _book(token, get=transport.http_get_json):
    return get(CLOB_BOOK, params={"token_id": token})


def flatten_leftovers(client, positions, floor_pts, log=print, get=transport.http_get_json, pause=1.0):
    """Sell each leftover position once with the normal floor (best bid - floor_pts, rounded up to the tick). -> list of (asset, sold_shares or error)."""
    out = []
    for p in positions:
        try:
            b = _book(p["asset_id"], get)
            tick = O.D(b["tick_size"])
            bids = sorted((O.D(x["price"]) for x in b.get("bids", [])), reverse=True)
            if not bids:
                out.append((p["asset_id"], "no bids"))
                continue
            floor = max(tick, bids[0] - O.D(floor_pts) / 100)
            meta = O.OrderMeta(p["slug"], p["asset_id"], "", tick_t=int(tick * TICK), min_size=float(b.get("min_order_size", 5)))
            intent = O.build_exit(meta, "?", p["asset_id"], p["size"], floor, 0, 1, "preflight")
            if isinstance(intent, O.Reject):
                out.append((p["asset_id"], f"cannot sell: {intent.reason} ({p['size']} shares)"))
                continue
            r = client.sell(p["asset_id"], intent.size, intent.price)
            out.append((p["asset_id"], float(r.making) if r.ok else f"{r.kind}: {r.code} {r.message}"))
            time.sleep(pause)
        except Exception as e:
            out.append((p["asset_id"], f"{type(e).__name__}: {e}"))
    return out


def static_preflight(lcfg, client, funder, kill_path, rule_path, floor_pts=5.0, log=print, geo=lc.geoblock, get=transport.http_get_json, flatten=True):
    """flatten=False (the read-only `keys preflight`): leftover positions are only reported, never sold."""
    pre = Pre()
    miss = lc.missing_keys()
    pre.add("keys", not miss, "all 7 present" if not miss else f"MISSING: {', '.join(miss)}")
    pre.add("STOP file", not kill_path.exists(), "absent" if not kill_path.exists() else f"{kill_path} exists: delete it")
    pre.add("RULE_STOP file", not rule_path.exists(), "absent" if not rule_path.exists() else f"{rule_path} exists: read it, then delete it by hand")
    try:
        ok, label, why = lc.geo_verdict(geo())
        pre.add("geoblock", ok, f"region {label or '?'}: eligible to open positions" if ok else f"region {label or '?'}: {why}")
    except Exception as e:
        pre.add("geoblock", False, f"could not check: {type(e).__name__}: {e}")
    try:
        mine = client.default_account_address()               # derived from the private key alone, so this is a real check of `funder`
        same = mine.lower() == str(funder).lower()
        pre.add("account address", same, "the Deposit Wallet derived from the stored key equals the stored funder" if same
                else "the Deposit Wallet derived from the stored key DIFFERS from the stored funder (run: keys check --online)")
        kind = client.account_kind()
        pre.add("account kind", kind == lcfg.account_kind, f"signature type {kind} (configured {lcfg.account_kind})")
    except Exception as e:
        pre.add("account identity", False, f"{type(e).__name__}: {e}")
    try:
        pre.add("closed-only mode", not client.closed_only(), "this account may open positions" if not client.closed_only() else "account is close-only")
    except Exception as e:
        pre.add("closed-only mode", False, f"{type(e).__name__}: {e}")
    try:
        bal, allow = client.collateral()
        pre.balance_usd = bal
        low = {k[:10] + "..": v for k, v in allow.items() if v < lcfg.min_collateral_usd}
        pre.add("pUSD balance", bal >= lcfg.min_collateral_usd, f"${bal:.2f} (need >= ${lcfg.min_collateral_usd:g})")
        pre.add("allowances", bool(allow) and not low, f"{len(allow)} spenders, all >= ${lcfg.min_collateral_usd:g}" if allow and not low
                else f"missing / too low: {low or 'none returned'} (run the approvals step, see the guide)")
    except Exception as e:
        pre.add("balance / allowances", False, f"{type(e).__name__}: {e}")
    try:
        orders = client.open_orders()
        pre.add("open orders", not orders, "none" if not orders else f"{len(orders)} open order(s): cancel them first")
    except Exception as e:
        pre.add("open orders", False, f"{type(e).__name__}: {e}")
    try:
        pos = client.positions(PREFIX)
        if pos and not flatten:
            pre.add("leftover positions", True, f"{len(pos)} found: {[(p['slug'], p['size']) for p in pos]} (NOT sold by this read-only check; a LIVE start sells them first)")
        elif pos:
            res = flatten_leftovers(client, pos, floor_pts, log, get)
            time.sleep(2.0)
            left = client.positions(PREFIX)
            pre.add("leftover positions", not left, f"{len(pos)} found, sold: {res}; still held: {[(p['slug'], p['size']) for p in left]}" if left
                    else f"{len(pos)} leftover position(s) sold first: {res}")
        else:
            pre.add("leftover positions", True, "none in the BTC 5m markets")
    except Exception as e:
        pre.add("leftover positions", False, f"{type(e).__name__}: {e}")
    return pre
