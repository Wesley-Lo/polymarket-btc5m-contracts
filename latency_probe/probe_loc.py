"""Location probe for a Binance -> Polymarket trader (read-only, PUBLIC data only: HTTP GET + public websockets; no keys, no orders).

Run it on the machine whose location you want to judge (GCP Tokyo, AWS Tokyo, AWS Dublin, ...):

    python probe_loc.py --minutes 10 [--out results/loc_<label>] [--label aws-dublin]
    python probe_loc.py --analyze results/loc_<label>        # reprint the summary of a saved run (no network)

What it measures (all with the machine's own clock corrected by a WARM clock offset, see below):
  * Binance spot WS delay      btcusdt@trade on wss://data-stream.binance.vision   (receive time - event time E)
  * Binance perp WS delay      btcusdt@aggTrade on wss://fstream.binance.com/market/ws   (receive time - trade time T)
  * Binance REST RTT           GET /api/v3/time on api.binance.com and data-api.binance.vision, kept-open connections (untimed warm-up first)
  * Polymarket CLOB RTT        GET https://clob.polymarket.com/time and GET /book?token_id=<current Up token>, kept-open connection
  * Polymarket market WS delay receive time - event `timestamp` of the market channel of the current btc-updown-5m market
  * Warm clock offset theta    clock_offset_v2 method: kept-open connections, RTT-filtered (within 30 ms of the host minimum), median per host, median of hosts
  * Geoblock                   GET https://polymarket.com/api/geoblock for this machine's IP: the RAW JSON and a verdict from the region tables of
                               docs.polymarket.com/api-reference/geoblock (read 2026-11-02)
The key number:   Binance event -> order at the CLOB  =  Binance WS delay (median)  +  CLOB RTT / 2  (+ 1 ms decision, + ~9 ms signing/processing if you add it).
Absolute feed delays carry the clock uncertainty (half the best warm RTT of the offset hosts plus half the spread between hosts); the RTTs do not.
"""
import argparse
import asyncio
import json
import os
import re
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
import websockets

try:
    import orjson
    loads = orjson.loads
except ImportError:
    loads = json.loads

ALLOWED_HOSTS = {"data-stream.binance.vision", "data-api.binance.vision", "api.binance.com", "fstream.binance.com",
                 "gamma-api.polymarket.com", "clob.polymarket.com", "ws-subscriptions-clob.polymarket.com", "polymarket.com"}
BN_TRADE = "wss://data-stream.binance.vision/ws/btcusdt@trade"
BN_PERP_TRADE = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
GEOBLOCK = "https://polymarket.com/api/geoblock"
TIME_ENDPOINTS = {"binance_api": "https://api.binance.com/api/v3/time", "binance_vision": "https://data-api.binance.vision/api/v3/time"}
SLUG = "btc-updown-5m-{}"

# docs.polymarket.com/api-reference/geoblock (2026-11-02)
OFAC = {"IR", "SY", "CU", "KP", "UA-43", "UA-14", "UA-09"}
CLOSE_ONLY_API = {"AR", "AU", "BY", "BE", "BI", "BR", "CA-BC", "CA-ON", "CA-AB", "CA-QC", "CF", "CD", "ET", "FR", "DE", "IQ", "IT", "LB", "LY", "MM", "NZ",
                  "NI", "KP", "PL", "RU", "SG", "SO", "SK", "SS", "SD", "TW", "TH", "GB", "US", "UM", "VE", "YE", "ZW"}
CLOSE_ONLY_FRONTEND_ONLY = {"IE", "JP", "MT", "NL", "KR"}


def check_url(url):
    host = urlparse(url).hostname
    if host not in ALLOWED_HOSTS:
        raise PermissionError(f"host not allowlisted: {host}")
    return url


def utc_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def pct(sv, p):
    if not sv:
        return float("nan")
    if len(sv) == 1:
        return sv[0]
    k = (len(sv) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(sv) - 1)
    return sv[lo] + (sv[hi] - sv[lo]) * (k - lo)


def geo_verdict(g):
    c, r = str(g.get("country") or ""), str(g.get("region") or "")
    label = f"{c}-{r}" if c and r else c
    keys = {c, label}
    if keys & OFAC:
        return f"BLOCKED completely ({label}: OFAC list)"
    if keys & CLOSE_ONLY_API:
        return f"API CLOSE-ONLY ({label}): new orders are rejected on the API"
    if c in CLOSE_ONLY_FRONTEND_ONLY:
        return (f"API OK, website close-only ({label}): the docs list it as close-only on the FRONTEND only, 'the API itself is not restricted'; "
                f"the endpoint says blocked={g.get('blocked')}")
    if g.get("blocked") is True:
        return f"endpoint says blocked=true for {label}, which is not in the docs tables: treat as NOT usable until Polymarket confirms"
    return f"eligible ({label})"


def clean_theta(offs, keep_ms=30.0):
    """offs: host -> [(wall_ns, rtt, theta)].  -> theta (median of per-host medians over RTT-filtered samples) and the uncertainty."""
    per, best, n = {}, [], 0
    for h, v in offs.items():
        if len(v) < 3:
            continue
        mn = min(x[1] for x in v)
        ok = [x[2] for x in v if x[1] <= mn + keep_ms]
        if len(ok) >= 3:
            per[h] = statistics.median(ok)
            best.append(mn)
            n += len(ok)
    if not per:
        return dict(theta_ms=None, unc_ms=None, per_host={}, n=0)
    spread = (max(per.values()) - min(per.values())) / 2 if len(per) > 1 else 0.0
    return dict(theta_ms=statistics.median(per.values()), unc_ms=min(best) / 2 + spread, per_host=per, n=n)


class Probe:
    def __init__(self, out, minutes, label):
        self.out, self.label = Path(out), label
        self.out.mkdir(parents=True, exist_ok=False)
        self.m0 = time.perf_counter_ns()
        self.w0 = time.time_ns()
        self.stop = time.perf_counter_ns() + int(minutes * 60e9)
        self.stopping = False
        self.theta = 0.0
        self.offs = {h: [] for h in TIME_ENDPOINTS}
        self.rtt = {"binance_api": [], "binance_vision": [], "clob_time": [], "clob_book": []}
        self.raw = {"bn_spot": [], "bn_perp": [], "pm_ws": []}       # (recv_wall_ms_local, exchange_ms)
        self.cur_up = None
        self.geo = None
        self.n_msgs = {"bn_spot": 0, "bn_perp": 0, "pm_ws": 0}

    def wall_ms(self, mono=None):
        return (self.w0 + ((time.perf_counter_ns() if mono is None else mono) - self.m0)) / 1e6

    def now(self):
        return self.wall_ms() + self.theta

    def alive(self):
        return not self.stopping and time.perf_counter_ns() < self.stop

    async def ws_binance(self, name, url, ts_key):
        while self.alive():
            try:
                async with websockets.connect(check_url(url), max_size=2 ** 22, open_timeout=10, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        if not self.alive():
                            break
                        mono = time.perf_counter_ns()
                        try:
                            m = loads(raw)
                            ex = float(m[ts_key])
                        except Exception:
                            continue
                        self.n_msgs[name] += 1
                        self.raw[name].append((self.wall_ms(mono), ex))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[{name}] ws error {e!r}", file=sys.stderr)
                await asyncio.sleep(1.0)

    async def pm_ws(self):
        async with httpx.AsyncClient(timeout=5) as c:
            tasks = []
            tgt = int(self.now() / 1000 // 300 * 300)
            while self.alive():
                tasks.append(asyncio.create_task(self.pm_window(c, tgt)))
                nxt = tgt + 300
                wait = (nxt - 8) - self.now() / 1000
                if wait > 0:
                    await asyncio.sleep(wait)
                tgt = nxt
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def pm_window(self, c, tgt):
        meta = None
        for _ in range(20):
            try:
                r = await c.get(check_url(f"{GAMMA}/events"), params={"slug": SLUG.format(tgt)})
                m = r.json()[0]["markets"][0]
                ids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
                outs = json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
                meta = (ids[outs.index("Up")], ids[outs.index("Down")])
                break
            except Exception:
                await asyncio.sleep(0.5)
        if meta is None:
            return
        up, down = meta
        if self.cur_up is None or self.cur_up[0] < tgt:
            self.cur_up = (tgt, up)
        end_ms = (tgt + 300 + 3) * 1000
        while self.alive() and self.now() < end_ms:
            try:
                async with websockets.connect(check_url(PM_WS), max_size=2 ** 24, open_timeout=10, ping_interval=None) as ws:
                    await ws.send(json.dumps({"assets_ids": [up, down], "type": "market", "custom_feature_enabled": True}))
                    last = time.perf_counter()
                    while self.alive() and self.now() < end_ms:
                        if time.perf_counter() - last > 10:
                            await ws.send("PING")
                            last = time.perf_counter()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        mono = time.perf_counter_ns()
                        if raw == "PONG":
                            continue
                        try:
                            m = loads(raw)
                        except Exception:
                            continue
                        for ev in (m if isinstance(m, list) else [m]):
                            if isinstance(ev, dict) and ev.get("timestamp") and ev.get("event_type") in ("price_change", "book", "last_trade_price", "best_bid_ask"):
                                self.n_msgs["pm_ws"] += 1
                                self.raw["pm_ws"].append((self.wall_ms(mono), float(ev["timestamp"])))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[pm_ws {tgt}] {e!r}", file=sys.stderr)
                await asyncio.sleep(0.5)

    async def clock_host(self, host):
        url = TIME_ENDPOINTS[host]
        async with httpx.AsyncClient(timeout=5, limits=httpx.Limits(max_keepalive_connections=1, keepalive_expiry=120)) as c:
            try:
                await c.get(check_url(url))                       # untimed warm-up
            except Exception:
                pass
            nxt = time.perf_counter()
            while self.alive():
                nxt += 2.0
                try:
                    m0 = time.perf_counter_ns()
                    r = await c.get(check_url(url))
                    m1 = time.perf_counter_ns()
                    srv = float(r.json()["serverTime"])
                    w0, w1 = self.wall_ms(m0), self.wall_ms(m1)
                    rtt = (m1 - m0) / 1e6
                    self.offs[host].append((w1, rtt, srv - (w0 + w1) / 2))
                    self.rtt[host].append(rtt)                    # Binance REST RTT (same warm request)
                    th = clean_theta(self.offs)
                    if th["theta_ms"] is not None:
                        self.theta = th["theta_ms"]
                except Exception as e:
                    print(f"[clock {host}] {e!r}", file=sys.stderr)
                await asyncio.sleep(max(0.0, nxt - time.perf_counter()))

    async def clob_rtt(self):
        async with httpx.AsyncClient(timeout=5, limits=httpx.Limits(max_keepalive_connections=1, keepalive_expiry=120)) as c:
            try:
                await c.get(check_url(f"{CLOB}/time"))
            except Exception:
                pass
            i, nxt = 0, time.perf_counter()
            while self.alive():
                nxt += 1.0
                i += 1
                kind = "clob_book" if (i % 2 == 0 and self.cur_up) else "clob_time"
                url = f"{CLOB}/book?token_id={self.cur_up[1]}" if kind == "clob_book" else f"{CLOB}/time"
                try:
                    m0 = time.perf_counter_ns()
                    r = await c.get(check_url(url))
                    m1 = time.perf_counter_ns()
                    if r.status_code == 200:
                        self.rtt[kind].append((m1 - m0) / 1e6)
                except Exception as e:
                    print(f"[{kind}] {e!r}", file=sys.stderr)
                await asyncio.sleep(max(0.0, nxt - time.perf_counter()))

    async def geoblock(self):
        try:
            async with httpx.AsyncClient(timeout=8, headers={"User-Agent": "Mozilla/5.0"}) as c:
                r = await c.get(check_url(GEOBLOCK))
                self.geo = dict(status=r.status_code, raw=r.text.strip()[:500])
        except Exception as e:
            self.geo = dict(status=None, raw=f"error: {e!r}")

    async def run(self):
        tasks = [self.ws_binance("bn_spot", BN_TRADE, "E"), self.ws_binance("bn_perp", BN_PERP_TRADE, "T"), self.pm_ws(), self.clob_rtt(), self.geoblock()]
        tasks += [self.clock_host(h) for h in TIME_ENDPOINTS]
        tasks = [asyncio.create_task(t) for t in tasks]
        print(f"probe_loc [{self.label}]: output {self.out}; about {(self.stop - time.perf_counter_ns()) / 60e9:.0f} min", flush=True)
        try:
            while self.alive():
                await asyncio.sleep(10)
                print(f"[{utc_iso(self.now())}] theta {self.theta:+.1f} ms  msgs " + " ".join(f"{k}={v}" for k, v in self.n_msgs.items()) +
                      f"  clob_time n={len(self.rtt['clob_time'])}", flush=True)
        finally:
            self.stopping = True
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            th = clean_theta(self.offs)
            data = dict(label=self.label, host=os.environ.get("HOSTNAME") or os.environ.get("COMPUTERNAME"), python=sys.version, start_ms=self.w0 / 1e6,
                        theta=th, rtt=self.rtt, geo=self.geo,
                        raw={k: v[::1] for k, v in self.raw.items()}, n_msgs=self.n_msgs)
            (self.out / "loc.json").write_text(json.dumps(data))


def stats(v):
    s = sorted(v)
    return dict(n=len(s), med=pct(s, 50), p10=pct(s, 10), p90=pct(s, 90), p99=pct(s, 99), mn=s[0] if s else float("nan"))


def summarize(out):
    d = json.loads((Path(out) / "loc.json").read_text())
    th = d["theta"]
    theta = th["theta_ms"] if th["theta_ms"] is not None else float("nan")
    unc = th["unc_ms"] if th["unc_ms"] is not None else float("nan")
    L = [f"probe_loc summary  label={d['label']}  host={d['host']}",
         f"warm clock offset theta {theta:+.1f} ms (added to the local clock); uncertainty of ABSOLUTE feed delays +-{unc:.1f} ms "
         f"(half the best warm RTT + half the host spread; per host {', '.join(f'{k} {v:+.1f}' for k, v in th['per_host'].items())}; {th['n']} clean samples)", ""]
    L.append(f"{'metric':36s}{'n':>7s}{'median':>9s}{'p10':>8s}{'p90':>8s}{'p99':>8s}{'min':>8s}")
    res = {}
    for k, name in (("bn_spot", "Binance spot WS delay (trade E)"), ("bn_perp", "Binance perp WS delay (aggTrade T)"), ("pm_ws", "Polymarket market WS delay")):
        v = [w + theta - x for w, x in d["raw"][k]]
        s = stats(v)
        res[k] = s
        L.append(f"{name:36s}{s['n']:7d}{s['med']:9.1f}{s['p10']:8.1f}{s['p90']:8.1f}{s['p99']:8.1f}{s['mn']:8.1f}   (+-{unc:.0f} ms absolute)")
    for k, name in (("binance_api", "Binance REST RTT api.binance.com"), ("binance_vision", "Binance REST RTT data-api.binance.vision"),
                    ("clob_time", "CLOB GET /time RTT"), ("clob_book", "CLOB GET /book RTT")):
        s = stats(d["rtt"][k])
        res[k] = s
        L.append(f"{name:36s}{s['n']:7d}{s['med']:9.1f}{s['p10']:8.1f}{s['p90']:8.1f}{s['p99']:8.1f}{s['mn']:8.1f}")
    bn, rtt = res["bn_spot"]["med"], res["clob_time"]["med"]
    key = bn + rtt / 2
    L += ["", "KEY NUMBER  Binance event -> order at the CLOB = Binance spot WS delay + CLOB RTT/2",
          f"   median {bn:.1f} + {rtt:.1f}/2 = {key:.0f} ms   (+1 ms decision = {key + 1:.0f}; with ~9 ms signing/processing = {key + 10:.0f}; absolute part +-{unc:.0f} ms)",
          f"   fastest-quartile reading with the p10 delay and the minimum RTT: {res['bn_spot']['p10'] + res['clob_time']['mn'] / 2:.0f} ms",
          "   event -> MATCH for a taker = that + the taker hold (150 or 250 ms, see the decision report);  event -> CANCEL arrival = the same number (cancels are not held)."]
    g = d["geo"] or {}
    L += ["", f"GEOBLOCK  GET {GEOBLOCK}  HTTP {g.get('status')}", f"   raw JSON: {g.get('raw')}"]
    try:
        L.append("   verdict: " + geo_verdict(json.loads(g["raw"])))
    except Exception:
        L.append("   verdict: could not parse the reply: treat as NOT usable")
    (Path(out) / "summary.json").write_text(json.dumps(dict(label=d["label"], theta_ms=theta, unc_ms=unc, key_number_ms=key, stats=res, geo=g), indent=1))
    txt = "\n".join(L)
    (Path(out) / "summary.txt").write_text(txt + "\n")
    return txt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--out", default=None)
    ap.add_argument("--label", default="loc")
    ap.add_argument("--analyze", default=None)
    a = ap.parse_args()
    if a.analyze:
        print(summarize(a.analyze))
        return
    out = a.out or str(Path(__file__).resolve().parent / "results" / f"loc_{a.label}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    p = Probe(out, a.minutes, a.label)
    try:
        asyncio.run(p.run())
    except KeyboardInterrupt:
        pass
    print()
    print(summarize(out))


if __name__ == "__main__":
    main()
