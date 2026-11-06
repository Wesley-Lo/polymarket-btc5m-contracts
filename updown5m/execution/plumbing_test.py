"""python -m updown5m.execution.plumbing_test --i-accept-real-money        (YOU run this; it places ONE real round trip)

1. full pre-flight (keys, account, geoblock, balance + allowances, open orders, leftovers) + clock / latency checks from live GETs;
2. in the CURRENT 5-minute BTC market: ONE FAK BUY of ~5 shares of the cheaper token at its best ask, only if that ask is within [0.30, 0.70], the
   spread is <= 2 pts and >= 60 s are left; the market-order API takes a dollar amount, so the amount is the 5-share cost rounded UP to the cent
   (you may get 5.00 to ~5.05 shares); exactly one buy, never repeated;
3. wait 2 s, then FAK SELL of the filled shares with the normal floor max(tick, fill - 5 pts), retried at +5 s and +10 s after the fill as in the live ladder;
4. report: every timestamp (sign, send, response), real fill prices and fees (from the trade history), balance before / after, position before / after
   and the reconciliation result; the report is printed, saved under logs/reports/ and e-mailed. Refuses to run twice within 10 minutes."""
import argparse
import json
import math
import time
from datetime import datetime, timezone
from decimal import Decimal

from .. import transport
from ..book import TICK
from ..model import fee_per_share
from ..util import iso_utc
from . import clock as clk
from . import live_client as lc
from . import orders as O
from . import preflight as pf
from .alerts import Alerts
from .live_config import load_live_config
from .redact import REDACTOR, install

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB_TIME = "https://clob.polymarket.com/time"
BN_TIME = "https://data-api.binance.vision/api/v3/time"
LAST = "plumbing_last.json"


def ms(a, b):
    return round((b - a) / 1e6, 2)


def last_run_age():
    p = transport.safe_path(LAST)
    if not p.exists():
        return None
    try:
        return time.time() - json.loads(p.read_text(encoding="utf-8"))["time"]
    except Exception:
        return None


def clock_and_latency(get=transport.http_get_json, n=5):
    """-> (clock offset ms or None, CLOB /time median RTT ms). The offset comes from the WARM clock_offset_v2 method (clock.warm_offset), not
    from cold single requests: those read +79.3 ms on the VM (plumbing 20261102T005517Z) while the warm sampler read ~0 ms."""
    off = clk.warm_offset()[0]
    lat = []
    for _ in range(n):
        p0 = time.perf_counter_ns()
        get(CLOB_TIME)
        lat.append((time.perf_counter_ns() - p0) / 1e6)
    return off, sorted(lat)[len(lat) // 2]


def clock_check(lcfg, off, rtt):
    """-> (ok, line) for the pre-flight report (shared by this tool and `keys preflight`)."""
    ok = off is not None and abs(off) < lcfg.clock_max_abs_offset_ms and rtt < lcfg.latency_max_ms
    o = "n/a (not enough warm samples)" if off is None else f"{off:+.1f} ms"
    return ok, (f"  [{'OK  ' if ok else 'FAIL'}] clock offset vs Binance {o} (warm clock_offset_v2 method, limit {lcfg.clock_max_abs_offset_ms:g}), "
                f"CLOB /time median RTT {rtt:.0f} ms (limit {lcfg.latency_max_ms:g})")


def pick_market(get=transport.http_get_json, now=time.time):
    start = int(now() // 300) * 300
    m = get(GAMMA, params={"slug": f"btc-updown-5m-{start}"})
    if not m:
        raise RuntimeError("no current BTC 5m market listed")
    m = m[0]
    ids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    outs = json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
    cands = []
    tokens = [ids[outs.index("Up")], ids[outs.index("Down")]]
    for name in ("Up", "Down"):
        tok = ids[outs.index(name)]
        b = pf._book(tok, get)
        asks = sorted(O.D(x["price"]) for x in b.get("asks", []))
        bids = sorted((O.D(x["price"]) for x in b.get("bids", [])), reverse=True)
        if asks and bids:
            cands.append({"name": name, "token": tok, "ask": asks[0], "bid": bids[0], "tick": O.D(b["tick_size"]),
                          "min": O.D(b.get("min_order_size", 5)), "fee_rate": float((m.get("feeSchedule") or {}).get("rate", 0.07)),
                          "fee_exp": float((m.get("feeSchedule") or {}).get("exponent", 1))})
    if not cands:
        raise RuntimeError("no two-sided book in either token")
    c = min(cands, key=lambda x: x["ask"])
    c.update(slug=m["slug"], start=start, end=start + 300, condition_id=m.get("conditionId"), tokens=tokens)
    return c


def run(a, client=None, alerts=None, geo=lc.geoblock, get=transport.http_get_json, sleep=time.sleep, now=time.time, out=print):
    lcfg = load_live_config()
    if not a.i_accept_real_money:
        raise SystemExit("error: this places a REAL order; add --i-accept-real-money")
    if not lcfg.armed:
        raise SystemExit("error: not armed: set \"armed\": true in updown5m/execution/live_config.json")
    age = last_run_age()
    if age is not None and age < lcfg.plumbing_min_gap_s:
        raise SystemExit(f"error: the plumbing test ran {age:.0f}s ago; wait {lcfg.plumbing_min_gap_s - age:.0f}s (never twice within 10 minutes)")
    install()
    lines = []

    def say(s=""):
        s = REDACTOR.scrub(s)
        lines.append(s)
        out(s)

    miss = lc.missing_keys()
    if miss:
        raise SystemExit("error: missing keys: " + ", ".join(miss))
    keys = lc.load_keys()
    client = client or lc.LiveClient(keys, lcfg).connect()
    alerts = alerts or Alerts(lcfg)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with transport.open_for_write(LAST, "w", encoding="utf-8", allow_overwrite=True) as f:       # written BEFORE any order: a crash cannot allow a quick re-run
        json.dump({"time": time.time(), "stamp": stamp}, f)
    say(f"updown5m plumbing test {stamp}: ONE real round trip, ~5 shares")
    pre = pf.static_preflight(lcfg, client, keys.get("funder"), transport.LOGS_ROOT.parent / "STOP", transport.LOGS_ROOT / lcfg.rule_stop_file,
                              geo=geo, get=get)
    say(pre.text())
    ok_rt, line = clock_check(lcfg, *clock_and_latency(get))
    say(line)
    if not (pre.ok and ok_rt):
        say("ABORTED: pre-flight failed; no order was sent.")
        return finish(lines, stamp, alerts, ok=False)
    mk = pick_market(get, now)
    left = mk["end"] - now()
    spread = float(mk["ask"] - mk["bid"]) * 100
    say(f"market {mk['slug']} ({left:.0f}s left): cheaper token {mk['name']}, bid {mk['bid']} ask {mk['ask']} spread {spread:.1f} pts, tick {mk['tick']}")
    if not (O.D(lcfg.plumbing_ask_min) <= mk["ask"] <= O.D(lcfg.plumbing_ask_max)):
        say(f"ABORTED: best ask {mk['ask']} is outside [{lcfg.plumbing_ask_min}, {lcfg.plumbing_ask_max}]; no order was sent. Run it again in a minute.")
        return finish(lines, stamp, alerts, ok=False)
    if spread > lcfg.plumbing_max_spread_pts + 1e-9 or left < 60:
        say(f"ABORTED: spread {spread:.1f} pts (max {lcfg.plumbing_max_spread_pts:g}) or only {left:.0f}s left (need 60); no order was sent.")
        return finish(lines, stamp, alerts, ok=False)
    tok = mk["token"]
    wl, wbad = lc.warm_lines(client.warm(mk["tokens"], refresh=True), mk["slug"], lcfg.build_alert_ms)    # metadata + dummy signs, nothing sent
    for s in wl:
        say(s)
    if wbad:
        say("WARNING: the warm-up had a problem (see above); the BUY below may sign slowly")
    bal0, pos0 = client.collateral()[0], client.shares(tok)
    say(f"before: pUSD balance ${bal0:.4f}, position in this token {pos0:.4f} shares")
    usd = Decimal(math.ceil(float(mk["ask"]) * 5 * 100)) / 100
    b = client.buy(tok, usd, mk["ask"])
    show(say, "BUY", b, lcfg.build_alert_ms)
    if not b.ok or b.taking <= 0:
        say("BUY did not fill (nothing to sell). Position after: " + f"{client.shares(tok):.4f}")
        return finish(lines, stamp, alerts, ok=False)
    got = float(b.taking)
    avg = float(b.making) / got
    t_fill = time.monotonic()
    floor = O.round_to_tick(max(mk["tick"], O.D(avg, 6) - O.D(5.0) / 100), mk["tick"], "up")      # the normal exit floor: max(tick, fill - 5 pts)
    sells, held = [], O.shares_down(got)
    for k, step in enumerate((2.0, 5.0, 10.0), 1):
        wait = step - (time.monotonic() - t_fill)
        if wait > 0:
            sleep(wait)
        if mk["end"] - now() < 15 and k > 1:
            say(f"sell attempt {k} skipped: <15 s left in the window (this would be STUCK in a live run)")
            break
        if held < mk["min"]:
            say(f"remaining {held} shares < minimum order size {mk['min']}: cannot be sold (DUST_HELD)")
            break
        s = client.sell(tok, held, floor)
        show(say, f"SELL attempt {k} (+{step:g}s after the fill, floor {floor})", s, lcfg.build_alert_ms)
        sells.append(s)
        if s.ok and s.making > 0:
            held = O.shares_down(float(held) - float(s.making))
            if held <= 0:
                break
    sleep(3.0)
    fees = trade_report(say, client, tok, [("BUY", b)] + [(f"SELL{i}", s) for i, s in enumerate(sells, 1)], mk)
    bal1, pos1 = client.collateral()[0], client.shares(tok)
    sold = sum(float(s.making) for s in sells if s.ok)
    exp = pos0 + got - sold
    say(f"after:  pUSD balance ${bal1:.4f} (change ${bal1 - bal0:+.4f}), position {pos1:.4f} shares (expected {exp:.4f})")
    snap = client.positions(pf.PREFIX)
    left_over = [(p["slug"], p["size"]) for p in snap]
    rec_ok = abs(pos1 - exp) <= lcfg.position_tolerance_shares + 0.02 and not client.open_orders()
    say(f"reconciliation: position {'matches' if abs(pos1 - exp) <= lcfg.position_tolerance_shares + 0.02 else 'DIFFERS from'} the expectation; "
        f"open orders: {'none' if not client.open_orders() else 'SOME'}; open BTC-5m positions: {left_over or 'none'}")
    spent = float(b.making)
    got_back = sum(float(s.taking) for s in sells if s.ok)
    say(f"cash result by the fills: spent ${spent:.4f}, received ${got_back:.4f}, formula fees ${fees:.4f} -> net ${got_back - spent - fees:+.4f} "
        f"(balance change ${bal1 - bal0:+.4f}; compare the two: any gap is a fee/rounding rule to learn)")
    return finish(lines, stamp, alerts, ok=rec_ok)


def show(say, label, r, build_limit_ms=20.0):
    say(f"{label}: ok={r.ok} posted={r.sent} kind={r.kind or '-'} status={r.status or '-'} order={r.order_id[:14] + '..' if r.order_id else '-'} "
        f"making={r.making} taking={r.taking} code={r.code} {r.message}".rstrip())
    if r.t_build_ns:
        build_ms = ms(r.t_build_ns, r.t_built_ns)
        flag = f" SLOW (> {build_limit_ms:g} ms)" if build_ms > build_limit_ms else ""
        say(f"    timing ms: sign {build_ms}{flag}, sign->send {ms(r.t_built_ns, r.t_send_ns)}, "
            f"send->response {ms(r.t_send_ns, r.t_resp_ns)}  (the 250 ms taker delay on crypto markets is inside send->response)")


def trade_report(say, client, token, results, mk):
    total = 0.0
    for label, r in results:
        if not (r.ok and r.order_id):
            continue
        trades = client.trades_for_order(r.order_id, token)
        for t in trades:
            fee = float(t.size) * fee_per_share(float(t.price), mk["fee_rate"], mk["fee_exp"])
            total += fee
            say(f"  trade {label}: {t.side} {t.size} @ {t.price} status {t.status} matched {iso_utc(t.matched_at_ms / 1000)} "
                f"fee_rate_bps {t.fee_rate_bps} -> formula fee ${fee:.5f}")
        if not trades:
            say(f"  trade {label}: not in the trade history yet")
    return total


def finish(lines, stamp, alerts, ok):
    text = "\n".join(lines) + f"\nRESULT: {'COMPLETED' if ok else 'ABORTED / CHECK ABOVE'}\n"
    d = transport.new_report_dir("plumbing", stamp)
    with transport.open_for_write(d / "report.txt", "w", encoding="utf-8") as f:
        f.write(text)
    print(f"report saved: {d / 'report.txt'}")
    alerts.send("plumbing", f"plumbing test {'completed' if ok else 'ABORTED'}", text)
    alerts.drain(30.0)
    return ok


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m updown5m.execution.plumbing_test")
    ap.add_argument("--i-accept-real-money", action="store_true")
    a = ap.parse_args(argv)
    raise SystemExit(0 if run(a) else 1)


if __name__ == "__main__":
    main()
