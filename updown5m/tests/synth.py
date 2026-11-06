"""Deterministic synthetic session (no network): Binance ticks drifting up through two 5-minute windows,
static Polymarket books (Up 0.50/0.52, Down 0.48/0.50), periodic snapshots/deltas/prints, pings, offsets, resolution."""
import json
import math
import random

B0 = 1_700_000_100          # multiple of 300
W1_UP, W1_DOWN = "tokUP1", "tokDN1"
W2_UP, W2_DOWN = "tokUP2", "tokDN2"


def meta(start, up, down, rate=0.07):
    return {"slug": f"btc-updown-5m-{start}", "clobTokenIds": json.dumps([up, down]),
            "outcomes": json.dumps(["Up", "Down"]), "feesEnabled": True,
            "feeSchedule": {"exponent": 1, "rate": rate, "takerOnly": True}, "orderPriceMinTickSize": 0.01}


def lv(levels):
    return [{"price": f"{p:.2f}", "size": str(s)} for p, s in levels]


def book_msg(asset, bids, asks, ts_ms):
    return {"market": "0xabc", "asset_id": asset, "timestamp": str(ts_ms), "hash": "h", "bids": lv(bids),
            "asks": lv(asks), "event_type": "book"}


def rest_msg(asset, bids, asks, ts_ms):
    m = book_msg(asset, bids, asks, ts_ms)
    del m["event_type"]
    return m


UP_BOOK = ([(0.50, 200), (0.49, 200)], [(0.52, 40), (0.53, 40), (0.60, 500)])
DN_BOOK = ([(0.48, 200), (0.47, 200)], [(0.50, 40), (0.51, 40), (0.58, 500)])


def scenario(seed=7, drift=1.5e-4, t_end_offset=420, resolve_at=380, step=0.1, w2_result=None, rest=False, coinbase=False,
             feed_ctl=False, pre_ramp=0.0):
    """Extra flags (all off = the original scenario, byte for byte): rest -> pm_book_rest snapshots 2 s after each pm_connect;
    coinbase -> 1 Hz ticker records (basis +25 USD); feed_ctl -> ws_connect control records at t0; pre_ramp -> drift (per s,
    relative) applied before the first window starts, to make the 60 s TWAP differ from spot."""
    rng = random.Random(seed)
    cb_rng = random.Random(seed + 1)
    recs = []

    def add(t, feed, obj):
        recs.append((int(round(t * 1e9)), feed, json.dumps(obj, separators=(",", ":"))))

    t0 = B0 - 90
    add(t0, "clock_offset", {"server_ms": 1, "send_wall_ns": 1, "recv_wall_ns": 2, "rtt_ms": 100.0, "offset_ms": 0.0})
    add(t0 + 0.001, "gamma_meta", meta(B0, W1_UP, W1_DOWN))
    add(t0 + 0.002, "ctl", {"ev": "pm_connect", "slug": f"btc-updown-5m-{B0}"})
    add(t0 + 0.003, "polymarket", [book_msg(W1_UP, *UP_BOOK, int(t0 * 1000)), book_msg(W1_DOWN, *DN_BOOK, int(t0 * 1000))])
    if feed_ctl:
        for f in ("binance_book", "binance_trade"):
            add(t0 + 0.0005, "ctl", {"ev": "ws_connect", "feed": f})
    if rest:
        add(t0 + 2.0, "pm_book_rest", rest_msg(W1_UP, *UP_BOOK, int(t0 * 1000)))
        add(t0 + 2.001, "pm_book_rest", rest_msg(W1_DOWN, *DN_BOOK, int(t0 * 1000)))
    mid, n, k = 100000.0, 0, 0
    t = t0 + 0.01
    sent_w2 = False
    while t < B0 + t_end_offset:
        n += 1
        in_w1 = t < B0 + 300
        d = drift if (B0 - 20 < t < B0 + 200) else (pre_ramp if t < B0 else 0.0)   # drift up so Up looks underpriced in W1
        mid *= math.exp(d * step + rng.gauss(0, 6e-5) * math.sqrt(step))
        ms = int(t * 1000)
        add(t, "binance_book", {"u": n, "s": "BTCUSDT", "b": f"{mid - 0.005:.8f}", "B": "1.0", "a": f"{mid + 0.005:.8f}", "A": "1.0"})
        add(t + 0.0001, "binance_trade", {"e": "trade", "E": ms, "s": "BTCUSDT", "t": n, "p": f"{mid:.2f}", "q": "0.01", "T": ms, "m": False, "M": True})
        if coinbase and n % 10 == 0:
            cb = mid + 25.0 + cb_rng.gauss(0, 1.5)
            add(t + 0.0009, "coinbase", {"type": "ticker", "product_id": "BTC-USD", "price": f"{cb:.2f}", "best_bid": f"{cb - 0.005:.2f}",
                                         "best_ask": f"{cb + 0.005:.2f}", "time": "2023-11-14T22:15:00.000000Z"})
        if n % 100 == 0:   # every 10 s
            add(t + 0.0002, "latency", {"rtt_ms": 105.0 + (n // 100) % 7 * 5, "server_time_s": "1"})
        if n % 50 == 0:    # every 5 s: full snapshots for the live window
            up, dn = (W1_UP, W1_DOWN) if in_w1 else (W2_UP, W2_DOWN)
            add(t + 0.0003, "polymarket", [book_msg(up, *UP_BOOK, ms), book_msg(dn, *DN_BOOK, ms)])
        if n % 10 == 5:    # every second: a delta that changes a deep level
            up = W1_UP if in_w1 else W2_UP
            add(t + 0.0004, "polymarket", {"market": "0xabc", "price_changes": [{"asset_id": up, "price": "0.40", "size": str(100 + n % 7),
                                                                                  "side": "BUY", "hash": "h", "best_bid": "0.5", "best_ask": "0.52"}],
                                           "timestamp": str(ms), "event_type": "price_change"})
        if n % 30 == 7:    # every 3 s: a sell print far below our virtual bids -> maker fills (through)
            up = W1_UP if in_w1 else W2_UP
            add(t + 0.0005, "polymarket", {"market": "0xabc", "asset_id": up, "price": "0.30", "size": "50", "fee_rate_bps": "0",
                                           "side": "SELL", "timestamp": str(ms), "event_type": "last_trade_price", "transaction_hash": "0x1"})
            dn = W1_DOWN if in_w1 else W2_DOWN
            add(t + 0.0006, "polymarket", {"market": "0xabc", "asset_id": dn, "price": "0.30", "size": "50", "fee_rate_bps": "0",
                                           "side": "SELL", "timestamp": str(ms), "event_type": "last_trade_price", "transaction_hash": "0x2"})
        if not sent_w2 and t > B0 + 292:
            sent_w2 = True
            add(t + 0.0006, "gamma_meta", meta(B0 + 300, W2_UP, W2_DOWN))
            add(t + 0.0007, "ctl", {"ev": "pm_connect", "slug": f"btc-updown-5m-{B0 + 300}"})
            add(t + 0.0008, "polymarket", [book_msg(W2_UP, *UP_BOOK, ms), book_msg(W2_DOWN, *DN_BOOK, ms)])
            if rest:
                add(t + 2.0, "pm_book_rest", rest_msg(W2_UP, *UP_BOOK, ms))
                add(t + 2.001, "pm_book_rest", rest_msg(W2_DOWN, *DN_BOOK, ms))
        t += step
    add(B0 + resolve_at, "gamma_resolution", {"slug": f"btc-updown-5m-{B0}", "closed": True, "outcomes": json.dumps(["Up", "Down"]),
                                              "outcomePrices": json.dumps(["1", "0"])})
    if w2_result:
        up_won = w2_result == "Up"
        add(B0 + 680, "gamma_resolution", {"slug": f"btc-updown-5m-{B0 + 300}", "closed": True, "outcomes": json.dumps(["Up", "Down"]),
                                           "outcomePrices": json.dumps(["1", "0"] if up_won else ["0", "1"])})
    recs.sort(key=lambda r: r[0])
    return recs


def feed(pipeline, recs):
    """Push records through a Pipeline with strictly increasing mono = wall - base."""
    base = recs[0][0] - 1_000_000
    last = 0
    for wall, f, raw in recs:
        mono = max(wall - base, last + 1)
        last = mono
        pipeline.process(f, wall, mono, None, raw)


def scenario_v3(jumps=(), seed=11, t_end_offset=420, resolve_at=380, step=0.1, pm_silence=None, wide_from=None, ctl=(),
                up_result=True, noise=6e-5, wide_dn_from=None, rtts=None, books=None, extra=()):
    """Controlled scenario for strategy-v3 tests. A flat Binance path (small noise) with instant jumps `jumps` = [(seconds after B0,
    bps)], STATIC Polymarket books (Up 0.50/0.52, Down 0.48/0.50) re-sent in full every second, except
    pm_silence=(a, b) seconds after B0 with no Polymarket message at all, and wide_from=s from which the Up book is 0.46/0.52.
    ctl = [(seconds after B0, {"ev": ...}), ...] extra control records; wide_dn_from=s: from then on the Down book is 0.44/0.50.
    rtts=[...] cycles the recorded CLOB round-trip times (default constant 105 ms). Everything else as scenario().
    books=f(seconds after B0) -> (up_book, dn_book) | None replaces the static books while it returns a pair (used by the dry-run execution tests);
    extra=[(seconds after B0, feed, obj), ...] adds arbitrary records. Both default to nothing: the default output is unchanged."""
    rng = random.Random(seed)
    recs = []

    def add(t, feed, obj):
        recs.append((int(round(t * 1e9)), feed, json.dumps(obj, separators=(",", ":"))))

    t0 = B0 - 90
    add(t0, "clock_offset", {"server_ms": 1, "send_wall_ns": 1, "recv_wall_ns": 2, "rtt_ms": 100.0, "offset_ms": 0.0})
    add(t0 + 0.001, "gamma_meta", meta(B0, W1_UP, W1_DOWN))
    add(t0 + 0.002, "ctl", {"ev": "pm_connect", "slug": f"btc-updown-5m-{B0}"})
    add(t0 + 0.0005, "ctl", {"ev": "ws_connect", "feed": "binance_book"})
    add(t0 + 0.0005, "ctl", {"ev": "ws_connect", "feed": "binance_trade"})
    for dt, ev in ctl:
        add(B0 + dt, "ctl", ev)
    jumps = sorted(jumps)
    mid, n, ji = 100000.0, 0, 0
    t = t0 + 0.01
    sent_w2 = False
    last_snap = -10.0
    while t < B0 + t_end_offset:
        n += 1
        mid *= math.exp(rng.gauss(0, noise) * math.sqrt(step))
        while ji < len(jumps) and t >= B0 + jumps[ji][0]:
            mid *= 1.0 + jumps[ji][1] * 1e-4
            ji += 1
        ms = int(t * 1000)
        add(t, "binance_book", {"u": n, "s": "BTCUSDT", "b": f"{mid - 0.005:.8f}", "B": "1.0", "a": f"{mid + 0.005:.8f}", "A": "1.0"})
        if n % 100 == 0:
            add(t + 0.0002, "latency", {"rtt_ms": 105.0 if rtts is None else float(rtts[(n // 100) % len(rtts)]), "server_time_s": "1"})
        in_w1 = t < B0 + 300
        silent = pm_silence is not None and B0 + pm_silence[0] <= t < B0 + pm_silence[1]
        if not silent and t - last_snap >= 1.0:
            last_snap = t
            up, dn = (W1_UP, W1_DOWN) if in_w1 else (W2_UP, W2_DOWN)
            ub = UP_BOOK
            if wide_from is not None and t >= B0 + wide_from:
                ub = ([(0.46, 200), (0.45, 200)], UP_BOOK[1])
            db = DN_BOOK
            if wide_dn_from is not None and t >= B0 + wide_dn_from:
                db = ([(0.44, 200), (0.43, 200)], DN_BOOK[1])
            if books is not None:
                bb = books(t - B0)
                if bb is not None:
                    ub, db = bb
            add(t + 0.0003, "polymarket", [book_msg(up, *ub, ms), book_msg(dn, *db, ms)])
        if not sent_w2 and t > B0 + 292:
            sent_w2 = True
            add(t + 0.0006, "gamma_meta", meta(B0 + 300, W2_UP, W2_DOWN))
            add(t + 0.0007, "ctl", {"ev": "pm_connect", "slug": f"btc-updown-5m-{B0 + 300}"})
            add(t + 0.0008, "polymarket", [book_msg(W2_UP, *UP_BOOK, ms), book_msg(W2_DOWN, *DN_BOOK, ms)])
        t += step
    add(B0 + resolve_at, "gamma_resolution", {"slug": f"btc-updown-5m-{B0}", "closed": True, "outcomes": json.dumps(["Up", "Down"]),
                                              "outcomePrices": json.dumps(["1", "0"] if up_result else ["0", "1"])})
    for dt, f, o in extra:
        add(B0 + dt, f, o)
    recs.sort(key=lambda r: r[0])
    return recs
