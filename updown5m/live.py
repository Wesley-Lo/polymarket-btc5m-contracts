"""Live runtime: read-only feeds -> queue -> (raw recorder + strategy engine). Nothing is ever traded.

Readers only timestamp a message and put it on the queue. One consumer parses, records the raw line
(recorder thread) and drives the engine. The CLOB latency pinger and clock sampler use their own HTTP sessions.
"""
import asyncio
import json
import shutil
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from . import params as params_mod
from . import summary, transport
from .engine import SLUG_RE, Engine
from .execution import Mode, load_config
from .recorder import NullRecorder, RawRecorder
from .sink import CsvSink
from .util import iso_utc, perf_ns, stamp, wall_ns

BN_BOOK = "wss://data-stream.binance.vision/ws/btcusdt@bookTicker"
BN_TRADE = "wss://data-stream.binance.vision/ws/btcusdt@trade"
# USD-M perpetual, RECORD ONLY (never used in a decision). Binance split the futures streams by route: bookTicker lives under /public,
# aggTrade under /market; the legacy /ws/btcusdt@aggTrade path connects but delivers nothing (verified 2026-10-31).
BN_PERP_BOOK = "wss://fstream.binance.com/public/ws/btcusdt@bookTicker"
BN_PERP_TRADE = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
CB_WS = "wss://ws-feed.exchange.coinbase.com"
PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
BN_TIME = "https://data-api.binance.vision/api/v3/time"


def extract_exch_ts(feed, obj):
    """Exchange timestamp in ms if the payload carries one, else None."""
    try:
        if feed in ("binance_trade", "binance_perp_trade", "binance_perp_book"):
            return int(obj["E"])
        if feed in ("polymarket", "pm_book_rest"):
            o = obj[0] if isinstance(obj, list) and obj else obj
            return int(o["timestamp"]) if isinstance(o, dict) and o.get("timestamp") else None
        if feed == "coinbase" and isinstance(obj, dict) and obj.get("time"):
            return int(datetime.fromisoformat(obj["time"]).timestamp() * 1000)
    except (KeyError, ValueError, TypeError):
        pass
    return None


class Pipeline:
    """Parse -> record raw -> engine. Used identically by the live consumer and by tests."""

    def __init__(self, engine, recorder):
        self.engine, self.recorder = engine, recorder
        self.seq = {}
        self.tracebacks = 0

    def process(self, feed, wall, mono, frame_mono, raw):
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        try:
            obj = json.loads(raw)
            raw_json = raw if "\n" not in raw else json.dumps(obj, separators=(",", ":"))
        except ValueError:
            obj, raw_json = raw, json.dumps(raw)
        seq = self.seq.get(feed, 0) + 1
        self.seq[feed] = seq
        exch = extract_exch_ts(feed, obj)
        if self.recorder is not None:
            self.recorder.put(feed, seq, wall, mono, exch, frame_mono, raw_json)
        try:
            self.engine.handle(feed, seq, wall, mono, exch, obj)
        except Exception:
            self.engine.acc.errors += 1
            if self.tracebacks < 5:
                self.tracebacks += 1
                traceback.print_exc()


class Gaps:
    """logs/gaps.csv: every feed disconnect/reconnect, so backtests can exclude those periods."""

    HEADER = ["session", "feed", "gap_start_utc", "gap_end_utc", "duration_s", "reason"]

    def __init__(self, session):
        self.session = session
        self.open = {}
        self.count = 0
        path = transport.safe_path("gaps.csv")
        new = not path.exists() or path.stat().st_size == 0
        self.f = transport.open_for_write(path, "a", encoding="utf-8", newline="")
        if new:
            self.f.write(",".join(self.HEADER) + "\n")
            self.f.flush()

    def down(self, name, reason):
        if name not in self.open:
            self.open[name] = (wall_ns(), reason)

    def up(self, name):
        g = self.open.pop(name, None)
        if g:
            self._write(name, g, wall_ns(), "")

    def _write(self, name, g, end_ns, suffix):
        self.count += 1
        reason = (g[1] + suffix).replace(",", ";").replace("\n", " ")
        self.f.write(f"{self.session},{name},{iso_utc(g[0] / 1e9)},{iso_utc(end_ns / 1e9) if end_ns else ''},"
                     f"{(end_ns - g[0]) / 1e9 if end_ns else ''},{reason}\n")
        self.f.flush()

    def close(self):
        for name, g in list(self.open.items()):
            self._write(name, g, wall_ns(), " [still down at shutdown]")
        self.open.clear()
        self.f.close()


class PmSession:
    def __init__(self, start):
        self.start, self.ws, self.task, self.closing, self.meta = start, None, None, False, None


class Runtime:
    def __init__(self, params, minutes=None, exec_mode=Mode.OFF, exec_cfg=None, clock_v2=True, live=None, raw_record=True):
        self.p = params
        self.f = params["feeds"]
        self.minutes = minutes
        self.session = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.sdir = transport.make_dirs(Path("sessions") / self.session)
        self.recorder = RawRecorder(transport.LOGS_ROOT / "raw") if raw_record else NullRecorder()
        self.sink = CsvSink(self.sdir, params_mod.markouts_of(params), params_mod.flip_s_of(params),
                            params_mod.rev_of(params))
        self.layer = None
        self.exec_out = None
        self.clock_v2 = clock_v2
        self.exec_cfg = exec_cfg or load_config()
        self.live = live                         # execution.livewire.LiveBundle in LIVE mode
        self.live_close = None
        self.started_wall = wall_ns()
        if exec_mode is Mode.LIVE:
            from .execution import livewire
            self.engine, self.layer, self.exec_out, twin_outs = livewire.build(self, params, self.exec_cfg, live, self.request_fatal)
            self.live_close = lambda: livewire.close(self, live, self.layer, twin_outs)
        elif exec_mode is Mode.DRY_RUN:        # the dry-run layer only OBSERVES the engine (execution/hook.py); nothing is signed or sent
            from .execution import recover
            from .execution.hook import ExecEngine
            from .execution.layer import DryRunLayer
            from .execution.outputs import Outputs
            self.exec_out = Outputs(self.sdir, self.session)
            rec = recover.rebuild(recover.find_logs(transport.safe_path("sessions"), exclude=self.session,
                                                    now_utc=wall_ns() / 1e9, max_age_h=self.exec_cfg.recover_max_age_h), only_mode="DRY_RUN")
            self.layer = DryRunLayer(self.exec_cfg, self.exec_out, self.session, Mode.DRY_RUN, recovered=rec)
            self.engine = ExecEngine(params, self.sink, self.layer)
        else:
            self.engine = Engine(params, self.sink)
        self.pipeline = Pipeline(self.engine, self.recorder)
        self.q = asyncio.Queue()
        self.gaps = Gaps(self.session)
        self.stopping = False
        self.planned_stop = False
        self.abort_wait = False
        self.pm_tasks = []
        self.loop_lag = []   # ms overshoot samples of a 20 ms ticker: how long the event loop was stalled
        self.stop_event = asyncio.Event()
        self.tasks = []
        self.sessions = {}
        self.resolvers = {}
        self.max_lag_ms = 0.0
        self.max_depth = 0
        self.started_wall = wall_ns()
        self.http_meta = transport.HttpSession(timeout=8)
        self.http_book = transport.HttpSession(timeout=8)
        self.warm_q = None                  # LIVE only: warm-up requests (market start, tokens, why, refresh) for live_warmup

    # ------------------------------------------------------------------ queue helpers
    def emit_obj(self, feed, obj):
        w, m = stamp()
        self.q.put_nowait((feed, w, m, None, json.dumps(obj, separators=(",", ":"))))

    def now_utc(self):
        return wall_ns() / 1e9 + self.engine.offset_s

    async def consumer(self):
        n = 0
        while True:
            item = await self.q.get()
            if item is None:
                return
            feed, w, m, fm, raw = item
            self.pipeline.process(feed, w, m, fm, raw)
            lag = (perf_ns() - m) / 1e6
            if lag > self.max_lag_ms:
                self.max_lag_ms = lag
            n += 1
            if n % 256 == 0:
                self.max_depth = max(self.max_depth, self.q.qsize())
                await asyncio.sleep(0)

    # ------------------------------------------------------------------ plain websocket feeds
    async def ws_reader(self, feed, url, subscribe=None):
        while not self.stopping:
            reason = "closed by server"
            try:
                async with transport.ws_connect(url) as ws:
                    if subscribe:
                        await transport.ws_send(ws, json.dumps(subscribe))
                    self.gaps.up(feed)
                    self.emit_obj("ctl", {"ev": "ws_connect", "feed": feed})
                    async for raw in ws:
                        fm = ws.pop_stamp()
                        w, m = stamp()
                        self.q.put_nowait((feed, w, m, fm, raw))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = repr(e)
            if self.stopping:
                break
            self.gaps.down(feed, reason)
            self.emit_obj("ctl", {"ev": "ws_down", "feed": feed, "reason": reason})
            print(f"[{iso_utc(self.now_utc())} UTC] {feed} disconnected: {reason}")
            await asyncio.sleep(1)

    # ------------------------------------------------------------------ polymarket per-market sockets
    async def fetch_meta(self, start):
        slug = f"{self.p['slug_prefix']}{start}"
        while not self.stopping and self.now_utc() < start + 330:
            try:
                r = await self.http_meta.get(f"{GAMMA}/markets", params={"slug": slug})
                data = r.json()
                if data:
                    self.emit_obj("gamma_meta", data[0])
                    ids = json.loads(data[0]["clobTokenIds"])
                    outs = json.loads(data[0]["outcomes"])
                    return {"slug": slug, "tokens": [ids[outs.index("Up")], ids[outs.index("Down")]]}
            except Exception as e:
                print(f"[{iso_utc(self.now_utc())} UTC] gamma meta {slug}: {e!r}")
            await asyncio.sleep(1)
        return None

    async def _pinger(self, ws):
        while True:
            await asyncio.sleep(self.f["pm_ping_every_s"])
            await transport.ws_send(ws, "PING")

    async def _snapshots(self, meta):
        await asyncio.sleep(self.f["book_snapshot_after_connect_s"])
        while True:
            for tok in meta["tokens"]:
                try:
                    r = await self.http_book.get(f"{CLOB}/book", params={"token_id": tok})
                    w, m = stamp()
                    self.q.put_nowait(("pm_book_rest", w, m, None, r.text))
                except Exception as e:
                    self.emit_obj("ctl", {"ev": "book_snapshot_error", "error": repr(e)})
            await asyncio.sleep(self.f["book_snapshot_every_s"])

    async def pm_session(self, sess):
        meta = await self.fetch_meta(sess.start)
        if meta is None:
            return
        sess.meta = meta
        self.request_warm(sess.start, meta["tokens"], "socket_open", True)     # LIVE: re-warm with a forced metadata refresh (no-op otherwise)
        slug, name = meta["slug"], f"polymarket:{meta['slug']}"
        while not sess.closing and not self.stopping:
            reason = "closed by server"
            helpers = []
            try:
                async with transport.ws_connect(PM_WS, ping_interval=None) as ws:
                    sess.ws = ws
                    await transport.ws_send(ws, json.dumps({"assets_ids": meta["tokens"], "type": "market",
                                                            "custom_feature_enabled": True}))
                    self.gaps.up(name)
                    self.emit_obj("ctl", {"ev": "pm_connect", "slug": slug})
                    helpers = [asyncio.create_task(self._pinger(ws)), asyncio.create_task(self._snapshots(meta))]
                    async for raw in ws:
                        fm = ws.pop_stamp()
                        w, m = stamp()
                        self.q.put_nowait(("polymarket", w, m, fm, raw))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = repr(e)
            finally:
                for h in helpers:
                    h.cancel()
                sess.ws = None
            if sess.closing or self.stopping:
                break
            self.gaps.down(name, reason)
            self.emit_obj("ctl", {"ev": "pm_down", "slug": slug, "reason": reason})
            print(f"[{iso_utc(self.now_utc())} UTC] polymarket {slug} disconnected: {reason}")
            await asyncio.sleep(1)
        self.gaps.open.pop(name, None)
        self.emit_obj("ctl", {"ev": "pm_closed", "slug": slug})

    async def resolver(self, start):
        slug = f"{self.p['slug_prefix']}{start}"
        t0 = time.monotonic()
        while time.monotonic() - t0 < self.f["resolution_give_up_s"]:
            try:
                # Gamma caches identical query URLs for minutes (measured), so each poll carries a throwaway `_` parameter.
                r = await self.http_meta.get(f"{GAMMA}/markets", params={"slug": slug, "closed": "true", "_": perf_ns()})
                data = r.json()
                if data and data[0].get("closed"):
                    prices = [float(x) for x in json.loads(data[0].get("outcomePrices") or "[]")]
                    if prices and max(prices) >= 0.99:
                        self.emit_obj("gamma_resolution", data[0])
                        return
            except Exception as e:
                self.emit_obj("ctl", {"ev": "resolution_poll_error", "slug": slug, "error": repr(e)})
            await asyncio.sleep(self.f["resolution_poll_every_s"])
        self.emit_obj("ctl", {"ev": "resolution_gave_up", "slug": slug})

    async def roll_manager(self):
        """Keeps one socket per market: pre-connects to the next market, closes the old one at the boundary."""
        win = self.p["window_s"]
        while not self.stopping:
            now = self.now_utc()
            cur = int(now // win) * win
            want = {cur}
            if cur + win - now <= self.f["preconnect_s"]:
                want.add(cur + win)
            for s in want:
                if s not in self.sessions:
                    sess = PmSession(s)
                    sess.task = asyncio.create_task(self.pm_session(sess))
                    self.pm_tasks.append(sess.task)
                    self.sessions[s] = sess
                    print(f"[{iso_utc(now)} UTC] polymarket: opening socket for {self.p['slug_prefix']}{s}")
            for s in list(self.sessions):
                if s not in want:
                    sess = self.sessions[s]
                    sess.closing = True
                    if sess.ws is not None:
                        asyncio.create_task(sess.ws.close())
                    if s not in self.resolvers:
                        self.resolvers[s] = asyncio.create_task(self.resolver(s))
                    print(f"[{iso_utc(now)} UTC] polymarket: closed socket for {self.p['slug_prefix']}{s}; resolution lookup started")
                    del self.sessions[s]
            await asyncio.sleep(0.1)

    # ------------------------------------------------------------------ side channels (own HTTP sessions)
    async def clob_pinger(self):
        http = transport.HttpSession(timeout=5)
        first = True
        try:
            while not self.stopping:
                t0 = perf_ns()
                try:
                    r = await http.get(f"{CLOB}/time")
                    rtt = (perf_ns() - t0) / 1e6
                    if first:   # includes TCP+TLS setup: log it but keep it out of the latency estimate
                        self.emit_obj("ctl", {"ev": "clob_cold_rtt_ms", "rtt_ms": rtt})
                        first = False
                    else:
                        self.emit_obj("latency", {"rtt_ms": rtt, "server_time_s": r.text.strip()})
                except Exception as e:
                    self.emit_obj("ctl", {"ev": "clob_ping_error", "error": repr(e)})
                await asyncio.sleep(self.f["clob_ping_every_s"])
        finally:
            await http.aclose()

    async def _offset_sample(self, http):
        send = wall_ns()
        t0 = perf_ns()
        r = await http.get(BN_TIME)
        t1 = perf_ns()
        recv = send + (t1 - t0)
        server_ms = r.json()["serverTime"]
        self.emit_obj("clock_offset", {"server_ms": server_ms, "send_wall_ns": send, "recv_wall_ns": recv,
                                       "rtt_ms": (t1 - t0) / 1e6, "offset_ms": server_ms - (send + recv) / 2e6})

    async def clock_sampler(self):
        http = transport.HttpSession(timeout=5)
        try:
            while not self.stopping:
                try:
                    await self._offset_sample(http)
                except Exception as e:
                    self.emit_obj("ctl", {"ev": "clock_sample_error", "error": repr(e)})
                await asyncio.sleep(self.f["clock_offset_every_s"])
        finally:
            await http.aclose()

    async def initial_clock_burst(self, n=10):
        http = transport.HttpSession(timeout=5)
        try:
            for _ in range(n):
                try:
                    await self._offset_sample(http)
                except Exception as e:
                    print(f"clock sample failed: {e!r}")
                await asyncio.sleep(0.15)
        finally:
            await http.aclose()

    async def loop_monitor(self):
        """Measures event-loop stalls (a 20 ms sleep that returns late). Diagnostic only; never feeds the engine."""
        while not self.stopping:
            t0 = perf_ns()
            await asyncio.sleep(0.02)
            self.loop_lag.append(max(0.0, (perf_ns() - t0) / 1e6 - 20.0))
            if len(self.loop_lag) > 3000:
                del self.loop_lag[:1500]

    def loop_lag_stats(self):
        v = sorted(self.loop_lag)
        return (v[len(v) // 2], v[int(len(v) * 0.99)], v[-1]) if v else (0.0, 0.0, 0.0)

    # ------------------------------------------------------------------ status / summaries
    def status_line(self):
        s = self.engine.status()
        now = self.now_utc()
        r = self.recorder
        mbh = r.mb_per_hour()
        w = s["window_start"]

        def n(x, f):
            return "-" if x is None else format(x, f)
        tag = "" if s["warm"] else " (warming sigma)"
        v3 = ""
        if "v3" in s:       # strategy v3: last evaluated move, its anchor / target and the best edge
            m = s["v3"]
            v3 = " | v3 no move yet" if m is None else (
                f" | v3 move {'UP' if m['dir'] > 0 else 'DN'} {m['bps']:.1f}bp {m['age_s']:.0f}s ago anchor {n(m['anchor'], '.3f')} "
                f"target {n(m['target'], '.3f')} edge {n(m['edge'], '+.1f')}pt [{m['action']}]")
        win = f"{iso_utc(w)[11:16]}-{iso_utc(w + 300)[11:16]}Z" if w else "-"
        return (f"[{iso_utc(now)} UTC] win {win} left {n(s['tau'], '.0f')}s | S0 {n(s['s0'], '.2f')} S {n(s['s_t'], '.2f')} "
                f"sig {n(s['sigma'], '.2e')} P(up) {n(s['p_up'], '.3f')} | UP {n(s['up'][0], '.2f')}/{n(s['up'][1], '.2f')} "
                f"DN {n(s['dn'][0], '.2f')}/{n(s['dn'][1], '.2f')} | edge {n(s['best_edge'], '+.1f')}pt | "
                f"lat {s['lat_med']:.0f}/{s['lat_p90']:.0f}ms | trades {s['trades']} net ${s['net']:+.2f} | "
                f"{n(mbh, '.1f')} MB/h dropped {r.dropped} | qlag max {self.max_lag_ms:.0f}ms loop stall p99/max "
                f"{self.loop_lag_stats()[1]:.0f}/{self.loop_lag_stats()[2]:.0f}ms{v3}{tag}"
                + ("" if self.layer is None else " | " + self.layer.status_fields()))

    def summary_extras(self, final=False):
        r = self.recorder.stats()
        return {"now_utc": self.now_utc(), "session": self.session, "start_utc": iso_utc(self.started_wall / 1e9),
                "end_utc": iso_utc(self.now_utc()) if final else None,
                "recorder": {"dropped_messages": r["dropped"], "writer_errors": r["errors"], "messages_by_feed": r["counts"],
                             "mb_written": round(r["bytes_written"] / 1e6, 2), "max_consumer_queue_depth": self.max_depth,
                             "max_consumer_lag_ms": round(self.max_lag_ms, 1),
                             "event_loop_stall_ms_median_p99_max": [round(x, 1) for x in self.loop_lag_stats()]},
                "gaps": {"count": self.gaps.count, "file": "logs/gaps.csv"}}

    async def status_loop(self):
        last_sum = time.monotonic()
        min_free = self.f["min_free_disk_gb"] * 1e9
        while not self.stopping:
            await asyncio.sleep(self.f["status_every_s"])
            self.sink.flush()
            if self.exec_out is not None:
                self.exec_out.flush()
            print(self.status_line())
            free = shutil.disk_usage(transport.LOGS_ROOT).free
            if free < min_free:
                print(f"WARNING: free disk {free / 1e9:.1f} GB < {self.f['min_free_disk_gb']} GB")
            if time.monotonic() - last_sum >= self.f["summary_every_s"]:
                last_sum = time.monotonic()
                summary.write(self.engine, self.p, self.summary_extras(), self.sdir)
                if self.layer is not None:
                    self.layer.write_summary()

    async def exec_idle_tick(self):
        """Dry-run timeouts when ALL feeds went silent (no record would call the layer). Normal operation never reaches it."""
        while not self.stopping:
            await asyncio.sleep(1.0)
            self.layer.guard("idle", self.layer.idle_tick, perf_ns())

    # ------------------------------------------------------------------ LIVE helpers
    def request_fatal(self, reason):
        """LIVE hard stop: called by the layer after it halted (no new entries, position being flattened). Shut the process down cleanly."""
        print(f"[{iso_utc(self.now_utc())} UTC] LIVE HARD STOP: {reason}")
        self.request_stop()

    async def live_runtime_preflight(self):
        """Stage 2 of the pre-flight (clock, feeds, latency need live data). LIVE entries stay blocked until it passes; on timeout LIVE stops."""
        t0 = time.monotonic()
        limit = self.live.lcfg.preflight_runtime_timeout_s
        bad = ["not run yet"]
        while not self.stopping:
            await asyncio.sleep(1.0)
            ok, bad = self.layer.runtime_preflight(perf_ns())
            if ok:
                self.layer.runtime_ok = True
                line = "LIVE pre-flight (stage 2): clock, feeds, latency, STOP files: all OK. Entries are now allowed."
                print(f"[{iso_utc(self.now_utc())} UTC] {line}")
                self.layer.event(None, "PREFLIGHT_OK", reason="stage 2 passed")
                self.live.alerts.send("preflight", "LIVE started: pre-flight passed",
                                      f"{self.live.pre_text}\n{line}\n\nsession {self.session}, budget ${self.live.lcfg.budget_usd:g} per entry")
                return
            if time.monotonic() - t0 > limit:
                self.layer.halt("runtime pre-flight failed: " + "; ".join(bad), "preflight")
                return

    def request_warm(self, start, tokens, why, refresh):
        """Queue a warm-up of one market's two tokens (no-op outside LIVE)."""
        if self.warm_q is not None and tokens:
            self.warm_q.put_nowait((start, list(tokens), why, refresh))

    async def live_warmup(self):
        """Keeps the SDK's per-market lookups OFF the order path (the first sign of a cold market took 437 ms: two CLOB GETs for token -> condition
        and tick size / neg-risk / fee info). One worker, one warm-up at a time, in a thread (the event loop never waits on it): pre-fetch the
        metadata of BOTH tokens, then build + sign (and DISCARD, never post) a BUY and a SELL per token. Every result is recorded (ctl order_warm)."""
        from .execution.live_client import warm_lines
        loop = asyncio.get_running_loop()
        lim = self.live.lcfg.build_alert_ms
        while not self.stopping:
            start, tokens, why, refresh = await self.warm_q.get()
            res = await loop.run_in_executor(None, self.live.client.warm, tokens, refresh)
            lines, bad = warm_lines(res, f"{self.p['slug_prefix']}{start} ({why})", lim)
            for w in res:
                self.emit_obj("ctl", {"ev": "order_warm", "start": start, "why": why, "refresh": refresh, **w})
            for s in lines:
                print(f"[{iso_utc(self.now_utc())} UTC] {s}")
            if bad:
                print(f"[{iso_utc(self.now_utc())} UTC] WARNING: warm-up problem for {self.p['slug_prefix']}{start}; its first real order may sign slowly")

    async def live_roll_prefetch(self):
        """At every window roll: look up the NEXT market on Gamma (lookup only, nothing recorded) and warm its tokens ~5 min ahead. When its socket
        opens 8 s before the start, pm_session warms it again with a forced metadata refresh so the SDK's 10-minute cache covers the whole window."""
        from .execution.livewire import market_tokens
        win = self.p["window_s"]
        last = None
        while not self.stopping:
            cur = int(self.now_utc() // win) * win
            if cur != last:
                last, nxt = cur, cur + win
                toks = None
                for _ in range(6):
                    try:
                        r = await self.http_meta.get(f"{GAMMA}/markets", params={"slug": f"{self.p['slug_prefix']}{nxt}"})
                        d = r.json()
                        if d:
                            toks = market_tokens(nxt, get=lambda url, params=None, _d=d: _d, prefix=self.p["slug_prefix"])
                            break
                    except Exception as e:
                        self.emit_obj("ctl", {"ev": "warm_lookup_error", "start": nxt, "error": repr(e)})
                    await asyncio.sleep(10.0)
                if toks:
                    self.request_warm(nxt, toks, "roll", False)
            await asyncio.sleep(0.5)

    # ------------------------------------------------------------------ lifecycle
    def request_stop(self, planned=False):
        self.planned_stop = self.planned_stop or planned
        if self.stop_event.is_set():
            self.abort_wait = True
        self.stop_event.set()

    async def _wait_live_flat(self, limit_s=90.0):
        """LIVE shutdown: never leave while a real position or order is still being worked (the exit ladder keeps running on the live feeds)."""
        from .execution.state import OPEN
        t0, shown = time.monotonic(), False
        while time.monotonic() - t0 < limit_s and not self.abort_wait:
            tr = self.layer.slot
            if tr is None or tr.state not in OPEN:
                return
            if not shown:
                print(f"[{iso_utc(self.now_utc())} UTC] LIVE: waiting for the open position (trade {tr.id}, {tr.state.value}) to be exited (up to {limit_s:.0f}s) ...")
                shown = True
            await asyncio.sleep(0.5)
        tr = self.layer.slot
        if tr is not None and tr.state in OPEN:
            self.live.alerts.send("shutdown_open", "LIVE stopped with a position still open",
                                  f"trade {tr.id} state {tr.state.value}, held {tr.held:.2f} shares. The next start's pre-flight sells leftovers first.")

    async def _wait_resolutions(self):
        eng, f = self.engine, self.f
        if self.live is not None:
            await self._wait_live_flat()
        if self.planned_stop:
            deadline = time.monotonic() + self.p["window_s"] + f["shutdown_resolution_wait_s"]
            need = lambda: eng.open_windows()
        else:
            deadline = time.monotonic() + f["shutdown_resolution_wait_s"]
            need = lambda: [s for s in eng.open_windows() if s + self.p["window_s"] <= self.now_utc()]
        shown = False
        while need() and time.monotonic() < deadline and not self.abort_wait:
            if not shown:
                print(f"[{iso_utc(self.now_utc())} UTC] waiting for official results of windows {need()} "
                      f"(Ctrl+C again to skip) ...")
                shown = True
            await asyncio.sleep(1)

    async def run(self):
        eng = self.engine
        loop = asyncio.get_running_loop()
        for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGBREAK", None), getattr(signal, "SIGTERM", None)):
            if sig is not None:
                try:
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(self.request_stop))
                except (ValueError, OSError):
                    pass
        params_sha = params_mod.sha256_of(self.p)
        with transport.open_for_write(self.sdir / f"params_{params_sha[:12]}.json", "w", encoding="utf-8") as pf:
            pf.write(params_mod.canonical(self.p))
        with transport.open_for_write(self.sdir / "params.sha256", "w", encoding="utf-8") as pf:
            pf.write(params_sha + "\n")
        if self.live is not None:
            print("updown5m: *** LIVE MODE *** REAL ORDERS WITH REAL MONEY (pilot: FAK taker orders, budget "
                  f"${self.live.lcfg.budget_usd:g} per entry, 1 position at a time). Create updown5m/STOP to stop entries and exit. All times UTC.")
        else:
            print(f"updown5m: READ-ONLY collector + paper trader. NOTHING IS TRADED. All times UTC.")
        if self.layer is not None and self.live is None:
            print(f"execution layer: {self.layer.mode.value} (orders are only LOGGED as intents in live_orders.jsonl; nothing is signed or sent); "
                  f"risk limits and the kill switch file updown5m/{self.exec_cfg.kill_switch_file} are active")
        print(f"session {self.session}  logs: {self.sdir}  params sha256 {params_sha[:16]}...")
        cons = asyncio.create_task(self.consumer())
        await self.initial_clock_burst()
        feeds = [
            asyncio.create_task(self.ws_reader("binance_book", BN_BOOK)),
            asyncio.create_task(self.ws_reader("binance_trade", BN_TRADE)),
            asyncio.create_task(self.ws_reader("coinbase", CB_WS, {"type": "subscribe", "product_ids": ["BTC-USD"],
                                                                    "channels": ["ticker", "matches"]})),
            # record-only perpetual futures feeds (to test later whether perps lead spot); the engine ignores these feeds
            asyncio.create_task(self.ws_reader("binance_perp_book", BN_PERP_BOOK)),
            asyncio.create_task(self.ws_reader("binance_perp_trade", BN_PERP_TRADE)),
            asyncio.create_task(self.roll_manager()),
            asyncio.create_task(self.clob_pinger()),
            asyncio.create_task(self.clock_sampler()),
            asyncio.create_task(self.status_loop()),
            asyncio.create_task(self.loop_monitor()),
        ]
        if self.clock_v2:      # record-only warm-connection clock sampler (feed clock_offset_v2); the frozen engine ignores it
            from .execution import clock as clk
            sampler = clk.ClockV2Sampler(self.emit_obj, self.exec_cfg.clock_v2_period_s)
            for name, url in clk.HOSTS.items():
                feeds.append(asyncio.create_task(sampler.run_host(name, url, lambda: self.stopping)))
        if self.layer is not None:
            feeds.append(asyncio.create_task(self.exec_idle_tick()))
        if self.live is not None:
            feeds.append(asyncio.create_task(self.live_runtime_preflight()))
            self.warm_q = asyncio.Queue()
            feeds.append(asyncio.create_task(self.live_warmup()))
            feeds.append(asyncio.create_task(self.live_roll_prefetch()))
        self.tasks = feeds
        try:
            if self.minutes:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), self.minutes * 60)
                except asyncio.TimeoutError:
                    self.planned_stop = True
                    print(f"[{iso_utc(self.now_utc())} UTC] requested run time reached; finishing open windows ...")
            else:
                await self.stop_event.wait()
            print(f"[{iso_utc(self.now_utc())} UTC] shutting down (no new entries) ...")
            self.emit_obj("ctl", {"ev": "drain"})     # recorded, so replay stops entering at the same instant
            await asyncio.sleep(0.1)
            await self._wait_resolutions()
        except Exception as e:
            if self.live is not None:                   # a crash of the runtime itself (the layer guards its own hooks): alert, then let it propagate
                self.live.alerts.send("crash", f"LIVE runtime crashed: {type(e).__name__}", traceback.format_exc())
            raise
        finally:
            self.stopping = True
            for sess in list(self.sessions.values()):
                sess.closing = True
            for t in feeds + self.pm_tasks:
                t.cancel()
            await asyncio.gather(*feeds, *self.pm_tasks, return_exceptions=True)
            for t in self.resolvers.values():
                t.cancel()
            await asyncio.gather(*self.resolvers.values(), return_exceptions=True)
            await asyncio.sleep(0.2)
            self.q.put_nowait(None)
            await cons
            eng.finalize()
            d, text = summary.write(eng, self.p, self.summary_extras(final=True), self.sdir)
            self.sink.close()
            if self.exec_out is not None:
                print(self.layer.summary(final=True)[0])
                self.exec_out.close()
            if self.live_close is not None:
                self.live_close()
            self.recorder.close()
            self.gaps.close()
            await self.http_meta.aclose()
            await self.http_book.aclose()
            with transport.open_for_write(self.sdir / "session.json", "w", encoding="utf-8") as sf:
                json.dump({"session": self.session, "start_wall_ns": self.started_wall, "end_wall_ns": wall_ns(),
                           "params_sha256": params_sha, "raw_root": "logs/raw", "python": sys.version.split()[0]}, sf, indent=2)
            print(text)
            print(f"final dropped messages: {self.recorder.dropped}")
