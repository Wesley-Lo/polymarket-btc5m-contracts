"""Read-only latency probe: Binance -> Polymarket 5-min BTC Up/Down.

HTTP GET + public websockets only. No keys, no auth, no orders.
Usage: python latency_probe/probe.py --minutes 5
"""
import argparse
import asyncio
import csv
import json
import math
import re
import statistics
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.frames import Opcode

ALLOWED_HOSTS = {
    "data-api.binance.vision",
    "data-stream.binance.vision",
    "api.binance.com",
    "gamma-api.polymarket.com",
    "clob.polymarket.com",
    "ws-subscriptions-clob.polymarket.com",
}
BINANCE_TIME = "https://data-api.binance.vision/api/v3/time"
BINANCE_WS = "wss://data-stream.binance.vision/ws/btcusdt@trade"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RESULTS_DIR = Path(__file__).resolve().parent / "results"

MOVE_BPS = 3.0
MOVE_WINDOW_S = 1.0
REACTION_TIMEOUT_S = 10.0
ADJUST_FRACS = (0.5, 0.8)
MIN_FAIR_MOVE = 0.01  # skip moves whose implied fair-value change is below this
MIN_VOL_SAMPLES = 30
SLUG_RE = re.compile(r"^btc-updown-5m-(\d+)$")


class StampedConnection(ClientConnection):
    """Records when each complete data message was parsed off the socket (queue-time metric)."""

    def process_event(self, event):
        if getattr(event, "fin", False) and event.opcode in (Opcode.TEXT, Opcode.BINARY, Opcode.CONT):
            self.__dict__.setdefault("stamps", deque()).append(time.perf_counter())
        super().process_event(event)

    def pop_stamp(self):
        st = self.__dict__.get("stamps")
        return st.popleft() if st else None


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def check_url(url):
    host = urlparse(url).hostname
    if host not in ALLOWED_HOSTS:
        raise PermissionError(f"host not allowlisted: {host}")
    return url


def utc_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def pct(sorted_vals, p):
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def stats(vals):
    if not vals:
        return {"n": 0}
    s = sorted(vals)
    return {"n": len(s), "min": s[0], "median": pct(s, 50), "p90": pct(s, 90), "p99": pct(s, 99), "max": s[-1]}


class Probe:
    def __init__(self):
        self.offset_ms = 0.0
        self.samples = {}  # metric -> list[(t_utc_ms, value_ms)]
        self.disconnects = {"binance_ws": 0, "polymarket_ws": 0}
        self.switches = []
        self.market = None  # dict(slug, start, up, down)
        self.market_event = asyncio.Event()
        self.best = {}  # asset_id -> (bid, ask)
        self.pending = deque()  # perf_counter times of unresolved Binance moves
        self.moves = 0
        self.no_reaction = 0
        self.window = deque()
        self.last_trigger = -1e9
        self.last_price = None
        self.s0 = None  # Binance price at the start of the current 5-min window
        self.s0_start = None
        self.sec = None
        self.sec_price = None
        self.rets = deque(maxlen=600)  # 1 s log returns for the vol estimate
        self.adjusts = []  # active full-adjustment trackers
        self.adj = {"tracked": 0, "skipped_small_or_no_data": 0, "aborted_at_roll": 0,
                    "reached_50": 0, "reached_80": 0, "unreached_50": 0, "unreached_80": 0}

    # ---- time helpers
    def now_ms(self):
        """Offset-corrected UTC ms."""
        return time.time() * 1000 + self.offset_ms

    def rec(self, metric, value_ms, t_ms=None):
        self.samples.setdefault(metric, []).append((self.now_ms() if t_ms is None else t_ms, value_ms))

    async def get(self, client, url, **kw):
        check_url(url)
        t0 = time.perf_counter()
        r = await client.get(url, **kw)
        dt = (time.perf_counter() - t0) * 1000
        r.raise_for_status()
        return r, dt

    # ---- clock offset
    async def measure_offset(self, n=20):
        best = None
        async with httpx.AsyncClient(timeout=5) as c:
            for _ in range(n):
                ts = time.time() * 1000
                t0 = time.perf_counter()
                r, _ = await self.get(c, BINANCE_TIME)
                rtt = (time.perf_counter() - t0) * 1000
                tr = ts + rtt
                off = r.json()["serverTime"] - (ts + tr) / 2
                if best is None or rtt < best[0]:
                    best = (rtt, off)
                await asyncio.sleep(0.05)
        return best[1], best[0]

    # ---- market discovery
    async def find_market(self, client, start):
        slug = f"btc-updown-5m-{start}"
        m = None
        r, _ = await self.get(client, f"{GAMMA}/markets", params={"slug": slug})
        data = r.json()
        if data:
            m = data[0]
        else:  # fallback: search
            r, _ = await self.get(client, f"{GAMMA}/public-search",
                                  params={"q": "Bitcoin Up or Down 5 minute", "events_status": "active", "limit_per_type": 50})
            now_s = self.now_ms() / 1000
            cands = []
            for ev in r.json().get("events", []):
                for mk in ev.get("markets", []):
                    g = SLUG_RE.match(mk.get("slug", ""))
                    if g and int(g.group(1)) <= now_s < int(g.group(1)) + 300:
                        cands.append(mk)
            m = cands[0] if cands else None
        if not m:
            return None
        ids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
        outs = json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
        up = ids[outs.index("Up")]
        down = ids[outs.index("Down")]
        return {"slug": m["slug"], "start": start, "up": up, "down": down}

    async def market_loop(self, stop_at):
        async with httpx.AsyncClient(timeout=5) as c:
            while time.time() < stop_at:
                start = int(self.now_ms() // 1000 // 300 * 300)
                if self.market is None or self.market["start"] != start:
                    try:
                        m = await self.find_market(c, start)
                    except Exception as e:
                        print(f"[{utc_iso(self.now_ms())}] market lookup error: {e!r}")
                        m = None
                    if m:
                        old = self.market["slug"] if self.market else None
                        self.market = m
                        self.best.clear()
                        self.adj["aborted_at_roll"] += len(self.adjusts)
                        self.adjusts.clear()
                        self.switches.append({"t_utc": utc_iso(self.now_ms()), "from": old, "to": m["slug"]})
                        print(f"[{utc_iso(self.now_ms())} UTC] market switch: {old} -> {m['slug']}")
                        self.market_event.set()
                    else:
                        await asyncio.sleep(0.5)
                        continue
                await asyncio.sleep(0.25)

    async def seed_s0(self):
        m = self.market
        async with httpx.AsyncClient(timeout=5) as c:
            r, _ = await self.get(c, "https://data-api.binance.vision/api/v3/klines",
                                  params={"symbol": "BTCUSDT", "interval": "1m", "startTime": m["start"] * 1000, "limit": 1})
        self.s0, self.s0_start = float(r.json()[0][1]), m["start"]

    # ---- REST
    async def cold_rest(self):
        m = self.market
        for name, url in [("binance_time", BINANCE_TIME), ("clob_time", f"{CLOB}/time"),
                          ("clob_book", f"{CLOB}/book?token_id={m['up']}")]:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    _, dt = await self.get(c, url)
                self.rec(f"rest_cold_{name}", dt)
            except Exception as e:
                print(f"cold {name} failed: {e!r}")

    async def warm_rest(self, name, url_fn, stop_at):
        async with httpx.AsyncClient(timeout=5) as c:
            try:
                await self.get(c, url_fn())  # warm-up, not recorded
            except Exception:
                pass
            nxt = time.perf_counter()
            while time.time() < stop_at:
                nxt += 1.0
                try:
                    _, dt = await self.get(c, url_fn())
                    self.rec(f"rest_warm_{name}", dt)
                except Exception as e:
                    print(f"warm {name} error: {e!r}")
                await asyncio.sleep(max(0, nxt - time.perf_counter()))

    # ---- Binance WS
    def on_binance_trade(self, msg, t_wall_ms, t_mono):
        self.rec("ws_binance_delay", t_wall_ms + self.offset_ms - msg["E"], t_wall_ms + self.offset_ms)
        price = float(msg["p"])
        self.last_price = price
        self.track_vol_and_s0(msg["E"], price)
        w = self.window
        w.append((t_mono, price))
        while w and t_mono - w[0][0] > MOVE_WINDOW_S:
            w.popleft()
        ref = w[0][1]
        if abs(price / ref - 1) * 1e4 >= MOVE_BPS and t_mono - self.last_trigger >= MOVE_WINDOW_S:
            self.last_trigger = t_mono
            self.moves += 1
            self.pending.append(t_mono)
            self.start_adjust(t_mono, ref, price)
        self.expire_pending(t_mono)

    def track_vol_and_s0(self, event_ms, price):
        sec = int(event_ms // 1000)
        if self.sec is not None and sec != self.sec and self.sec_price:
            self.rets.append(math.log(price / self.sec_price))
        if sec != self.sec:
            self.sec, self.sec_price = sec, price
        m = self.market
        if m and self.s0_start != m["start"] and event_ms >= m["start"] * 1000:
            self.s0, self.s0_start = price, m["start"]

    def fair_up(self, price, now_s):
        """P(S_T >= S0) under a driftless random walk with realized 1 s vol."""
        sigma = math.sqrt(sum(r * r for r in self.rets) / len(self.rets))
        t_rem = max(self.market["start"] + 300 - now_s, 1.0)
        return norm_cdf(math.log(price / self.s0) / (sigma * math.sqrt(t_rem)))

    def up_mid(self):
        b = self.best.get(self.market["up"]) if self.market else None
        return (b[0] + b[1]) / 2 if b and b[0] is not None and b[1] is not None else None

    def start_adjust(self, t_mono, ref, price):
        mid0 = self.up_mid()
        if self.s0 is None or self.s0_start != self.market["start"] or len(self.rets) < MIN_VOL_SAMPLES or mid0 is None:
            self.adj["skipped_small_or_no_data"] += 1
            return
        now_s = self.now_ms() / 1000
        delta = self.fair_up(price, now_s) - self.fair_up(ref, now_s)
        if abs(delta) < MIN_FAIR_MOVE:
            self.adj["skipped_small_or_no_data"] += 1
            return
        self.adj["tracked"] += 1
        self.adjusts.append({"t0": t_mono, "mid0": mid0, "delta": delta, "hit": {}})

    def check_adjust(self, t_mono):
        mid = self.up_mid()
        if mid is None:
            return
        for a in self.adjusts:
            frac = (mid - a["mid0"]) / a["delta"]
            for f in ADJUST_FRACS:
                if f not in a["hit"] and frac >= f:
                    a["hit"][f] = (t_mono - a["t0"]) * 1000
                    self.rec(f"pm_adjust_{int(f * 100)}", a["hit"][f])
                    self.adj[f"reached_{int(f * 100)}"] += 1
        self.adjusts = [a for a in self.adjusts if len(a["hit"]) < len(ADJUST_FRACS)]

    def expire_pending(self, now_mono):
        while self.pending and now_mono - self.pending[0] > REACTION_TIMEOUT_S:
            self.pending.popleft()
            self.no_reaction += 1
        keep = []
        for a in self.adjusts:
            if now_mono - a["t0"] > REACTION_TIMEOUT_S:
                for f in ADJUST_FRACS:
                    if f not in a["hit"]:
                        self.adj[f"unreached_{int(f * 100)}"] += 1
            else:
                keep.append(a)
        self.adjusts = keep

    async def binance_ws(self, stop_at):
        while time.time() < stop_at:
            try:
                async with websockets.connect(check_url(BINANCE_WS), max_size=2**20,
                                              create_connection=StampedConnection) as ws:
                    async for raw in ws:
                        t_mono = time.perf_counter()
                        t_arr = ws.pop_stamp()
                        if t_arr is not None:
                            self.rec("queue_binance_ws", (t_mono - t_arr) * 1000)
                        t_wall = time.time() * 1000
                        msg = json.loads(raw)
                        if msg.get("e") == "trade":
                            self.on_binance_trade(msg, t_wall, t_mono)
                        self.rec("proc_binance_ws", (time.perf_counter() - t_mono) * 1000)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.disconnects["binance_ws"] += 1
                print(f"[{utc_iso(self.now_ms())} UTC] binance ws disconnect: {e!r}")
                await asyncio.sleep(1)

    # ---- Polymarket WS
    def update_best(self, asset, bid, ask, t_mono):
        new = (bid, ask)
        old = self.best.get(asset)
        self.best[asset] = new
        if self.market and asset == self.market["up"] and old is not None and old != new:
            self.check_adjust(t_mono)
        if old is not None and old != new and self.pending:
            for t0 in self.pending:
                self.rec("pm_reaction_lag", (t_mono - t0) * 1000)
            self.pending.clear()

    @staticmethod
    def fnum(x):
        return float(x) if x not in (None, "") else None

    def on_pm_event(self, ev, t_wall_ms, t_mono):
        et = ev.get("event_type")
        ts = ev.get("timestamp")
        if ts and et != "book":  # book snapshots are subscribe responses, not live updates
            self.rec("ws_polymarket_delay", t_wall_ms + self.offset_ms - float(ts), t_wall_ms + self.offset_ms)
        if et == "book":
            bids = [float(x["price"]) for x in ev.get("bids", [])]
            asks = [float(x["price"]) for x in ev.get("asks", [])]
            self.update_best(ev["asset_id"], max(bids) if bids else None, min(asks) if asks else None, t_mono)
        elif et == "price_change":
            for pc in ev.get("price_changes", []):
                if "best_bid" in pc:
                    self.update_best(pc["asset_id"], self.fnum(pc["best_bid"]), self.fnum(pc["best_ask"]), t_mono)
        elif et == "best_bid_ask":
            self.update_best(ev["asset_id"], self.fnum(ev.get("best_bid")), self.fnum(ev.get("best_ask")), t_mono)

    async def pm_pinger(self, ws):
        while True:
            await asyncio.sleep(10)
            await ws.send("PING")

    async def pm_roll_watcher(self, ws, slug):
        """Close the socket at a market roll; the new connection subscribes only to the new market."""
        while True:
            await self.market_event.wait()
            self.market_event.clear()
            if self.market["slug"] != slug:
                await ws.close()
                return

    async def polymarket_ws(self, stop_at):
        while self.market is None:
            await asyncio.sleep(0.1)
        while time.time() < stop_at:
            tasks = []
            m = self.market
            try:
                async with websockets.connect(check_url(PM_WS), ping_interval=None, max_size=2**22,
                                              create_connection=StampedConnection) as ws:
                    self.market_event.clear()
                    self.best.clear()
                    await ws.send(json.dumps({"assets_ids": [m["up"], m["down"]], "type": "market",
                                              "custom_feature_enabled": True}))
                    tasks = [asyncio.create_task(self.pm_pinger(ws)),
                             asyncio.create_task(self.pm_roll_watcher(ws, m["slug"]))]
                    async for raw in ws:
                        t_mono = time.perf_counter()
                        t_arr = ws.pop_stamp()
                        if t_arr is not None:
                            self.rec("queue_polymarket_ws", (t_mono - t_arr) * 1000)
                        t_wall = time.time() * 1000
                        if raw in ("PONG", b"PONG"):
                            continue
                        msg = json.loads(raw)
                        for ev in (msg if isinstance(msg, list) else [msg]):
                            self.on_pm_event(ev, t_wall, t_mono)
                        self.rec("proc_polymarket_ws", (time.perf_counter() - t_mono) * 1000)
                if self.market["slug"] == m["slug"]:
                    raise ConnectionError("server closed the connection")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.disconnects["polymarket_ws"] += 1
                print(f"[{utc_iso(self.now_ms())} UTC] polymarket ws disconnect: {e!r}")
                await asyncio.sleep(1)
            finally:
                for t in tasks:
                    t.cancel()

    # ---- reporting
    def med(self, metric):
        v = [x[1] for x in self.samples.get(metric, [])]
        return f"{statistics.median(v):.1f}" if v else "-"

    async def status_loop(self, stop_at):
        while time.time() < stop_at:
            await asyncio.sleep(10)
            self.expire_pending(time.perf_counter())
            print(f"[{utc_iso(self.now_ms())} UTC] median ms | rest binance {self.med('rest_warm_binance_time')} "
                  f"clob_time {self.med('rest_warm_clob_time')} book {self.med('rest_warm_clob_book')} | "
                  f"ws binance {self.med('ws_binance_delay')} pm {self.med('ws_polymarket_delay')} | "
                  f"queue {self.med('queue_binance_ws')}/{self.med('queue_polymarket_ws')} | "
                  f"react {self.med('pm_reaction_lag')} adj50 {self.med('pm_adjust_50')} adj80 {self.med('pm_adjust_80')} (moves {self.moves}, none {self.no_reaction})")

    LABELS = [
        ("rest_cold_binance_time", "REST cold  Binance /time"),
        ("rest_cold_clob_time", "REST cold  CLOB /time"),
        ("rest_cold_clob_book", "REST cold  CLOB /book"),
        ("rest_warm_binance_time", "REST warm  Binance /time"),
        ("rest_warm_clob_time", "REST warm  CLOB /time"),
        ("rest_warm_clob_book", "REST warm  CLOB /book"),
        ("ws_binance_delay", "WS delay   Binance trade"),
        ("ws_polymarket_delay", "WS delay   Polymarket"),
        ("queue_binance_ws", "Queue time Binance WS"),
        ("queue_polymarket_ws", "Queue time Polymarket WS"),
        ("proc_binance_ws", "Processing Binance WS"),
        ("proc_polymarket_ws", "Processing Polymarket WS"),
        ("pm_reaction_lag", "PM first-tick reaction"),
        ("pm_adjust_50", "PM adjusts 50% of fair"),
        ("pm_adjust_80", "PM adjusts 80% of fair"),
    ]

    def summary(self, off0, rtt0, off1, rtt1, t_start, t_end):
        self.expire_pending(time.perf_counter())
        metrics = {k: stats([x[1] for x in self.samples.get(k, [])]) for k, _ in self.LABELS}
        drift = abs(off1 - off0)
        warnings = []
        if abs(off0) > 50 or abs(off1) > 50:
            warnings.append(f"clock offset > 50 ms (start {off0:.1f}, end {off1:.1f})")
        if drift > 20:
            warnings.append(f"clock offset drifted {drift:.1f} ms (> 20 ms)")
        return {
            "start_utc": utc_iso(t_start), "end_utc": utc_iso(t_end),
            "clock_offset_ms": {"start": off0, "start_rtt": rtt0, "end": off1, "end_rtt": rtt1, "drift": drift},
            "warnings": warnings, "metrics_ms": metrics,
            "reaction": {"moves": self.moves, "reacted": len(self.samples.get("pm_reaction_lag", [])),
                         "no_reaction_10s": self.no_reaction, "threshold_bps": MOVE_BPS},
            "adjustment": self.adj,
            "disconnects": self.disconnects, "market_switches": self.switches,
        }

    def print_table(self, s):
        print("\nAll timestamps UTC. Values in ms.")
        print(f"{'metric':<28}{'n':>7}{'min':>9}{'median':>9}{'p90':>9}{'p99':>9}{'max':>9}")
        for k, label in self.LABELS:
            m = s["metrics_ms"][k]
            if m["n"] == 0:
                print(f"{label:<28}{0:>7}")
            else:
                print(f"{label:<28}{m['n']:>7}{m['min']:>9.1f}{m['median']:>9.1f}{m['p90']:>9.1f}{m['p99']:>9.1f}{m['max']:>9.1f}")
        o = s["clock_offset_ms"]
        r = s["reaction"]
        print(f"\nClock offset: start {o['start']:.1f} ms (rtt {o['start_rtt']:.1f}), end {o['end']:.1f} ms "
              f"(rtt {o['end_rtt']:.1f}), drift {o['drift']:.1f} ms")
        for w in s["warnings"]:
            print(f"WARNING: {w}")
        print(f"Reaction: {r['moves']} moves >= {r['threshold_bps']} bps/1s, {r['reacted']} reacted, "
              f"{r['no_reaction_10s']} no reaction within 10 s")
        a = s["adjustment"]
        print(f"Adjustment vs simple fair-value move: tracked {a['tracked']}, "
              f"reached 50% {a['reached_50']} / 80% {a['reached_80']}, "
              f"not reached in 10 s: 50% {a['unreached_50']} / 80% {a['unreached_80']}, "
              f"skipped {a['skipped_small_or_no_data']}, aborted at roll {a['aborted_at_roll']}")
        print(f"Disconnects: {s['disconnects']}  Market switches: {len(s['market_switches'])}")

    def save(self, s):
        RESULTS_DIR.mkdir(exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        (RESULTS_DIR / f"latency_{stamp}.json").write_text(json.dumps(s, indent=2))
        with open(RESULTS_DIR / f"latency_{stamp}.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["metric", "t_utc", "value_ms"])
            rows = [(t, k, v) for k, lst in self.samples.items() for t, v in lst]
            for t, k, v in sorted(rows):
                w.writerow([k, utc_iso(t), f"{v:.3f}"])
        print(f"Saved results/latency_{stamp}.json and .csv")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=5)
    args = ap.parse_args()
    p = Probe()

    print("Measuring clock offset vs Binance...")
    off0, rtt0 = await p.measure_offset()
    p.offset_ms = off0
    print(f"Clock offset {off0:.1f} ms (best rtt {rtt0:.1f} ms)")
    t_start = p.now_ms()
    stop_at = time.time() + args.minutes * 60

    tasks = [asyncio.create_task(p.market_loop(stop_at))]
    while p.market is None:
        await asyncio.sleep(0.1)
    await p.cold_rest()
    try:
        await p.seed_s0()
    except Exception as e:
        print(f"S0 seed failed (adjustment metric starts at next roll): {e!r}")
    tasks += [
        asyncio.create_task(p.binance_ws(stop_at)),
        asyncio.create_task(p.polymarket_ws(stop_at)),
        asyncio.create_task(p.warm_rest("binance_time", lambda: BINANCE_TIME, stop_at)),
        asyncio.create_task(p.warm_rest("clob_time", lambda: f"{CLOB}/time", stop_at)),
        asyncio.create_task(p.warm_rest("clob_book", lambda: f"{CLOB}/book?token_id={p.market['up']}", stop_at)),
        asyncio.create_task(p.status_loop(stop_at)),
    ]
    try:
        await asyncio.sleep(max(0, stop_at - time.time()))
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        t_end = p.now_ms()
        try:
            off1, rtt1 = await p.measure_offset()
        except Exception:
            off1, rtt1 = off0, rtt0
        s = p.summary(off0, rtt0, off1, rtt1, t_start, t_end)
        p.print_table(s)
        p.save(s)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
