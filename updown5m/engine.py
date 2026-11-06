"""Deterministic strategy engine. The SAME code runs live and in replay.

Inputs are recorded records only: engine.handle(feed, seq, wall_ns, mono_ns, exch_ts, obj). The engine never
reads a clock or the network; its notion of "now" is the record's recv_wall_ns plus the latest Binance clock
offset (also a recorded record). Simulated latencies are timers on recv_mono_ns, so replay is exact.

Feeds understood: binance_book, binance_trade, polymarket, pm_book_rest, gamma_meta, gamma_resolution,
latency, clock_offset, ctl. (coinbase is recorded but never used in any decision.)

Strategy selection (params): strategy.version 1/2 (absent) = the original level-vs-market taker on model v1/v2;
strategy.version 3 = lag strategy (strategy_v3.py): Polymarket mid as anchor + the model's predicted change after a fresh Binance move.
  strategy.rev 0 = v3.0 (hold main track, 3 entries/window), rev 1 = v3.1 (flip-aware edge, flip_2s main track, one open position).
"""
import heapq
import json
import math
import re
import statistics
from collections import deque
from types import SimpleNamespace as Rec

from .book import TICK, Book, to_ticks
from .model import (LatencyTracker, SecondGrid, SigmaEwma, average_forecast, fair_up as fair_up_fn, fee_per_share,
                    p_up_v2)
from . import strategy_v3 as sv3
from .util import iso_ny, iso_utc

SLUG_RE = re.compile(r"^btc-updown-5m-(\d+)$")
NS = 1_000_000_000
INF = float("inf")


def _jf(v):
    return json.loads(v) if isinstance(v, str) else v


class Win:
    def __init__(self, start, end):
        self.start, self.end = start, end
        self.inflight, self.attempts, self.position, self.cooldown = {}, {}, {}, {}
        self.trades = []          # filled taker trades awaiting settlement
        self.cand_last = {}
        self.m_cur = {}           # side -> live virtual bid
        self.v3 = {}              # v3.0: track -> Rec(entries, last_dec, side)
        self.m_orders = {"up": [], "down": []}
        self.m_fills = {"up": 0, "down": 0}
        self.m_filled = []        # maker fills awaiting settlement
        self.samples = {}         # calibration tau -> (p_model, up_mid)


class Engine:
    def __init__(self, params, sink):
        p = params
        self.sink = sink
        self.win_s = int(p["window_s"])
        s = p["sigma"]
        self.sigma_est = SigmaEwma(s["halflife_s"], s["floor"], s["cap"], s["initial"], s["warmup_s"])
        lt = p["latency"]
        self.lat = LatencyTracker(lt["initial_median_ms"], lt["initial_p90_ms"], lt["window_n"], lt["min_samples"])
        e = p["entry"]
        sc = p.get("strategy") or {}
        self.v3 = int(sc.get("version", 2)) == 3
        self.rev = int(sc.get("rev", 0)) if self.v3 else 0
        self.v31 = self.rev >= 1
        self.no_first, self.no_last = e["no_entry_first_s"], e["no_entry_last_s"]    # also used by the maker shadow (unchanged)
        self.v3_no_first = sc.get("no_entry_first_s", e["no_entry_first_s"]) if self.v3 else e["no_entry_first_s"]
        self.min_px, self.max_px = e["min_price"], e["max_price"]
        self.size_usd = e["size_usd"]
        self.slip_t = int(round(e["limit_slippage_pts"] * 100))
        self.min_frac = e["min_fill_fraction"]
        self.cooldown_ns = int(e["retry_cooldown_ms"] * 1e6)
        self.max_attempts = sc.get("max_attempts_per_window", e["max_attempts_per_window"]) if self.v3 else e["max_attempts_per_window"]
        thr_list = sc.get("thresholds_pts", e["thresholds_pts"]) if self.v3 else e["thresholds_pts"]
        main_thr = sc.get("main_threshold_pts", e["main_threshold_pts"]) if self.v3 else e["main_threshold_pts"]
        self.tracks = [Rec(name=f"t{t:g}", thr=float(t), main=(float(t) == float(main_thr)), inmove=False)
                       for t in sorted(thr_list, reverse=True)]
        if self.v31 and sc.get("inmove_shadow", True):      # later opportunities inside an already-entered move: a separate SHADOW track
            self.tracks.append(Rec(name=f"t{float(main_thr):g}_inmove", thr=float(main_thr), main=False, inmove=True))
        self.has_inmove = any(tr.inmove for tr in self.tracks)
        self.fee_default = (p["fee"]["default_rate"], p["fee"]["default_exponent"])
        mk = p["maker"]
        self.m_off_t = mk["bid_offset_pts"] * 100
        self.m_cancel_bps, self.m_cancel_after, self.m_cap = mk["cancel_move_bps"], mk["cancel_after_s"], mk["max_fills_per_token_window"]
        self.markouts = list(sc.get("markouts_s", p["markouts_s"])) if self.v3 else list(p["markouts_s"])
        self.cand_min = p["candidates"]["log_min_edge_pts"]
        self.cand_interval_ns = int(p["candidates"]["log_interval_ms"] * 1e6)
        self.series_ns = int(p["series_interval_ms"] * 1e6)
        self.max_gap = p["max_binance_gap_s"]
        self.cal_taus = list(p["calibration_taus_s"])
        self.cal_tol = p["calibration_tolerance_s"]
        # --- model selection (absent keys = the original v1 behaviour, so old session params replay unchanged)
        mc = p.get("model") or {}
        self.v2 = mc.get("version", "v1") == "v2"
        self.twap_s = int(mc.get("twap_s", 60))
        self.m_dist, self.m_df = mc.get("dist", "normal"), float(mc.get("t_df", 5.0))
        self.m_mult, self.m_basis_bps = float(mc.get("sigma_mult", 1.0)), float(mc.get("basis_bps", 0.0))
        self.m_min_var_s = float(mc.get("min_var_s", 1.0))
        self.ptb_mode = mc.get("ptb_mode", "proxy")          # proxy | raw | basis   (price-to-beat usage, see README)
        self.ptb_hl, self.ptb_min_hist = float(mc.get("ptb_basis_halflife_windows", 6)), int(mc.get("ptb_basis_min_windows", 3))
        self.max_filled_s = int(mc.get("max_filled_s", 5))
        # averaging windows of the S0 proxy and of the end value: the W seconds ending at (boundary + offset) (alignment study)
        self.s0_w, self.s0_off = int(mc.get("s0_window_s", self.twap_s)), int(mc.get("s0_offset_s", 0))
        self.end_w, self.end_off = int(mc.get("end_window_s", self.twap_s)), int(mc.get("end_offset_s", 0))
        self.use_cb = bool(mc.get("use_coinbase", False))
        self.cb_hl, self.cb_stale = float(mc.get("coinbase_basis_halflife_s", 300)), float(mc.get("coinbase_stale_s", 5))
        # --- gap rule (absent = off)
        gr = p.get("gap_rule") or {}
        self.gap_on = bool(gr.get("enabled", False))
        self.gap_block_ns = int(float(gr.get("reconnect_block_s", 3)) * 1e9)
        self.gap_need_rest = bool(gr.get("require_rest_sync", True))
        self.gap_feeds = {"binance_book", "binance_trade"} | ({"coinbase"} if self.use_cb else set())
        self.feed_down, self.feed_block_until, self.pm_state = set(), {}, {}

        self.wall_ns = self.mono_ns = 0
        self.now_utc = 0.0
        self.offsets = deque(maxlen=10)
        self.offset_s = 0.0
        self.mid = self.bn_bid = self.bn_ask = None
        self.last_bn_t = None
        self.boundary = {}
        self.last_b = None
        self.markets, self.token_map, self.books = {}, {}, {}
        self.windows = {}
        self.cur = None
        self.timers, self.tcount, self.tid, self.mid_ctr = [], 0, 0, 0
        self.last_series_mono = None
        self.cur_fair = None
        self.best_edge_pts = None
        self.written_windows = set()
        self.entries_enabled = True
        # v2 state
        self.grid = SecondGrid()
        self.proxy_s0 = {}            # window start -> Binance TWAP of the 60 s before the start (None if gappy)
        self.ptb = {}                 # window start -> (price_to_beat, seconds after window start it was first seen)
        self.beta = None              # EMA of (price_to_beat - proxy) over windows
        self.beta_n = 0
        self.beta_used = {}           # window start -> beta estimate available when that window's ptb arrived
        self.bn_mid = None
        self.cb_mid = self.cb_t = self.cb_basis = self.cb_basis_t = None
        # v3 state
        if self.v3:
            if not self.v2:
                raise ValueError("strategy v3 needs model.version = v2")
            self.sc = Rec(move_bps=float(sc.get("move_bps", 2.0)), lookback_ms=float(sc.get("lookback_ms", 500)),
                          anchor=sc.get("anchor", "mid"), target_mode=sc.get("target_mode", "logit"), beta=float(sc.get("beta", 1.0)),
                          max_spread_pts=float(sc.get("max_spread_pts", 4.0)), max_book_age_s=float(sc.get("max_book_age_s", 5.0)),
                          max_entries=int(sc.get("max_entries_per_window", 3)), spacing_s=float(sc.get("min_entry_spacing_s", 15)),
                          flip_s=list(sc.get("flip_s", [2, 5, 10])), pm_keep_s=sc.get("pm_keep_s", 6.0))
            self.detector = sv3.MoveDetector(self.sc.move_bps, self.sc.lookback_ms)
            self.merger = sv3.EventMerger(self.sc.lookback_ms)
            self.pmh = sv3.PmHistory(self.sc.pm_keep_s)
            self.v3_move = None
            self.v3_last = None          # last evaluated move (status line)
            if self.v31:
                self.main_exit = int(sc.get("main_exit_s", 2))              # the main track IS the flip exit at +main_exit s
                if self.main_exit not in self.sc.flip_s:
                    raise ValueError("strategy.main_exit_s must be one of strategy.flip_s")
                self.cooldown_exit_s = float(sc.get("cooldown_after_exit_s", 3))
                self.last_margin_s = float(sc.get("last_margin_s", 2))
                self.hs_window_s = float(sc.get("hs_window_s", 60))
                self.hs_min_samples = int(sc.get("hs_min_samples", 5))
                self.pos = {}            # track -> Rec(open, cooldown_until): one open position at a time per track
                self.hs, self.hs_last = {}, {}   # token -> recent (t, half-spread) samples, 1 per second
        # price_change carries the exchange's own best_bid/best_ask; compare with the rebuilt book. Intermediate states
        # inside one match differ transiently (a full `book` snapshot follows), so count episodes and only call
        # those lasting > 1 s persistent.
        self.pc = {"checked": 0, "episodes": 0, "resolved_by_snapshot": 0, "persistent_gt_1s": 0}
        self.pc_open = {}
        self.acc = Rec(
            trades=[], decisions={t.name: 0 for t in self.tracks}, base_missed={t.name: 0 for t in self.tracks},
            maker_placed=0, maker_cancelled=0, maker_fills=[], windows=[], brier={t: [0.0, 0.0, 0] for t in self.cal_taus},
            book_checks=[0, 0, 0], errors=0, main_filled=0, main_net_base=0.0, unresolved_trades=0,
            rest_checked=0, ticks=0, brier_rows=[], v3_events=0)   # brier_rows: (window start, tau, p_model, p_polymarket_mid, outcome)
        self._dispatch = {
            "binance_book": self._on_bn_book, "binance_trade": self._on_bn_trade, "polymarket": self._on_pm,
            "pm_book_rest": self._on_book_rest, "gamma_meta": self._on_meta, "gamma_resolution": self._on_resolution,
            "latency": self._on_latency, "clock_offset": self._on_clock, "ctl": self._on_ctl,
            "coinbase": self._on_coinbase, "gamma_start": self._on_gamma_start}

    # ------------------------------------------------------------------ core
    def handle(self, feed, seq, wall_ns, mono_ns, exch_ts, obj):
        self._fire_timers(mono_ns)
        self.wall_ns, self.mono_ns = wall_ns, mono_ns
        t = wall_ns / 1e9 + self.offset_s
        if t > self.now_utc:
            self.now_utc = t
        start = int(self.now_utc // self.win_s) * self.win_s
        if start != self.cur:
            self._roll(start)
        fn = self._dispatch.get(feed)
        if fn is not None:
            fn(obj, exch_ts)

    def _at(self, due_mono, fn, *args):
        self.tcount += 1
        heapq.heappush(self.timers, (due_mono, self.tcount, fn, args))

    def _fire_timers(self, mono_ns):
        while self.timers and self.timers[0][0] <= mono_ns:
            due, _, fn, args = heapq.heappop(self.timers)
            fn(due, *args)

    def _utc_of(self, mono):
        return (self.wall_ns + (mono - self.mono_ns)) / 1e9 + self.offset_s

    def _roll(self, new):
        old = self.windows.get(self.cur)
        if old is not None:
            self._maker_close(old)
        self.cur = new
        self.windows.setdefault(new, Win(new, new + self.win_s))
        for s in [s for s in self.windows if s < new - 12 * self.win_s]:   # never got an official result
            self._settle(s, None, unresolved=True)

    # ------------------------------------------------------------------ small feeds
    def _on_clock(self, o, _):
        self.offsets.append((o["rtt_ms"], o["offset_ms"]))
        self.offset_s = min(self.offsets)[1] / 1000.0
        self.sink.write("clock_offset", [iso_utc(self.now_utc), o["offset_ms"], o["rtt_ms"], self.offset_s * 1000])

    def _on_latency(self, o, _):
        self.lat.add(o["rtt_ms"])
        self.sink.write("latency", [iso_utc(self.now_utc), o["rtt_ms"], self.lat.median(), self.lat.p90()])

    def _on_ctl(self, o, _):
        ev = o.get("ev") if isinstance(o, dict) else None
        if ev == "drain":            # run time reached / shutdown requested: finish open positions, open no new ones
            self.entries_enabled = False
        if self.gap_on:
            self._gap_ctl(ev, o)
        if ev in ("pm_down", "pm_connect", "pm_closed"):
            m = SLUG_RE.match(o.get("slug", ""))
            meta = self.markets.get(int(m.group(1))) if m else None
            if meta is not None:
                for tok in (meta.up, meta.down):
                    bk = self.books.get(tok)
                    if bk is not None:
                        bk.valid = False
                        bk.bids, bk.asks, bk._bb, bk._ba = {}, {}, None, None
                        if self.v3:
                            self.pmh.update(tok, self.now_utc, bk)

    # ---- gap rule: no entries while a decision feed is down, for N s after it reconnects, and (Polymarket) until the
    # book has been rebuilt from a REST snapshot taken after the reconnect.
    def _gap_ctl(self, ev, o):
        feed = o.get("feed")
        if ev == "ws_down" and feed in self.gap_feeds:
            self.feed_down.add(feed)
        elif ev == "ws_connect" and feed in self.gap_feeds:
            self.feed_down.discard(feed)
            self.feed_block_until[feed] = self.mono_ns + self.gap_block_ns
        elif ev == "pm_connect":
            self.pm_state[o.get("slug")] = Rec(down=False, since=self.mono_ns, rest=set())
        elif ev == "pm_down":
            self.pm_state[o.get("slug")] = Rec(down=True, since=self.mono_ns, rest=set())
        elif ev == "pm_closed":
            self.pm_state.pop(o.get("slug"), None)

    def _gap_block(self, meta):
        if not self.gap_on:
            return None
        if self.feed_down:
            return "gap_feed_down"
        for f in self.gap_feeds:
            if self.mono_ns < self.feed_block_until.get(f, -1):
                return "gap_feed_reconnect"
        st = self.pm_state.get(meta.slug)
        if st is None:
            return "gap_pm_not_connected"
        if st.down:
            return "gap_pm_down"
        if self.mono_ns < st.since + self.gap_block_ns:
            return "gap_pm_reconnect"
        if self.gap_need_rest and len(st.rest) < 2:
            return "gap_pm_no_rest_sync"
        return None

    def _rest_rebuild(self, o, tok, meta):
        """First REST snapshot after a (re)connect rebuilds that token's book (gap rule); later ones only get compared."""
        st = self.pm_state.get(meta.slug)
        if st is None or st.down or tok in st.rest:
            return
        self.books[tok].snapshot(o.get("bids", ()), o.get("asks", ()))
        st.rest.add(tok)
        if self.v3:
            self.pmh.touch(tok, self.now_utc)
            self.pmh.update(tok, self.now_utc, self.books[tok])

    def _on_meta(self, o, _):
        m = SLUG_RE.match(o.get("slug", ""))
        if not m:
            return
        start = int(m.group(1))
        ids, outs = _jf(o["clobTokenIds"]), _jf(o["outcomes"])
        up, down = ids[outs.index("Up")], ids[outs.index("Down")]
        fs = o.get("feeSchedule") or {}
        if fs and "rate" in fs:
            rate, exp, src = float(fs["rate"]), float(fs.get("exponent", 1)), "gamma"
        else:
            (rate, exp), src = self.fee_default, "default"
        tick_t = int(round(float(o.get("orderPriceMinTickSize") or 0.01) * TICK))
        meta = Rec(slug=o["slug"], start=start, end=start + self.win_s, up=up, down=down, tick_t=tick_t,
                   fee_rate=rate, fee_exp=exp, fee_src=src)
        self.markets[start] = meta
        for tok, side in ((up, "up"), (down, "down")):
            self.token_map[tok] = (start, side)
            self.books.setdefault(tok, Book())

    # ------------------------------------------------------------------ binance
    def _boundary_check(self, t):
        b = int(t // self.win_s) * self.win_s
        if self.last_b is None:
            self.last_b = b
            return
        if b > self.last_b:
            n = (b - self.last_b) // self.win_s
            gap_ok = self.last_bn_t is not None and (t - self.last_bn_t) <= self.max_gap
            if n == 1 and gap_ok and self.mid is not None:
                self.boundary[b] = (self.mid, True)   # last mid before the first tick at/after the boundary
            else:
                for k in range(1, n + 1):
                    self.boundary[self.last_b + k * self.win_s] = (None, False)
            self.last_b = b
            for k in [k for k in self.boundary if k < b - 4 * self.win_s]:
                del self.boundary[k]

    def _s0(self, start):
        b = self.boundary.get(start)
        return b[0] if b and b[1] else None

    def _on_bn_book(self, o, _):
        try:
            bid, ask = float(o["b"]), float(o["a"])
        except (KeyError, TypeError, ValueError):
            return
        t = self.now_utc
        self._boundary_check(t)
        self.bn_bid, self.bn_ask, self.bn_mid = bid, ask, (bid + ask) / 2
        self.mid = self.bn_mid
        if self.use_cb and self.cb_mid is not None and self.cb_basis is not None and t - self.cb_t <= self.cb_stale:
            # Binance + half of Coinbase's deviation from its usual basis (stale Coinbase -> Binance alone)
            self.mid = self.bn_mid + 0.5 * ((self.cb_mid - self.bn_mid) - self.cb_basis)
        self.sigma_est.update(t, self.mid)
        if self.v2:
            self.grid.update(t, self.mid)
        self.last_bn_t = t
        self.acc.ticks += 1
        if self.v3:
            self.v3_move = self.detector.update(t, self.mid)
            if self.v31:
                self._hs_sample(t)
        self._evaluate(from_book=True)

    def _on_coinbase(self, o, _):
        """Record-only unless model.use_coinbase (the Binance+Coinbase test): tracks Coinbase's mid and its slow basis."""
        if not self.use_cb or not isinstance(o, dict) or o.get("type") != "ticker":
            return
        try:
            cb_mid = (float(o["best_bid"]) + float(o["best_ask"])) / 2
        except (KeyError, TypeError, ValueError):
            return
        t = self.now_utc
        self.cb_mid, self.cb_t = cb_mid, t
        if self.bn_mid is not None:
            dev = cb_mid - self.bn_mid
            if self.cb_basis is None:
                self.cb_basis, self.cb_basis_t = dev, t
            else:
                a = 1.0 - 0.5 ** (max(t - self.cb_basis_t, 0.0) / self.cb_hl)
                self.cb_basis += a * (dev - self.cb_basis)
                self.cb_basis_t = t

    # ---- v2: S0 = Gamma price-to-beat when known (ptb_mode), else the Binance 60 s TWAP before the window start
    def _avg(self, end_k, w):
        """Binance average over the w seconds ending at second end_k (w == 0: the mid at that second)."""
        if w > 0:
            return self.grid.mean(end_k - w, end_k, self.max_filled_s)
        return self.grid.g.get(end_k)

    def _proxy(self, start):
        if start not in self.proxy_s0:
            a = start + self.s0_off
            cs = self.grid.cur_sec
            v = self._avg(a, self.s0_w)
            if self.s0_off <= 0 or (cs is not None and cs >= a - 1):   # a later offset is only final once its seconds exist
                self.proxy_s0[start] = v
            return v
        return self.proxy_s0[start]

    def _end_proxy(self, start):
        return self._avg(start + self.win_s + self.end_off, self.end_w)

    def p_at(self, s_t):
        """P(Up) of the configured v2 model right now with the Binance mid replaced by s_t (sigma, S0, tau held fixed)."""
        w = self.windows.get(self.cur)
        ea, vt = average_forecast(self.grid, s_t, self.now_utc, w.end + self.end_off, self.end_w)
        return p_up_v2(ea, self._s0_v2(self.cur), self.sigma_est.sigma, vt, self.m_mult, self.m_basis_bps, self.m_dist, self.m_df,
                       self.m_min_var_s)

    def _s0_v2(self, start):
        px = self._proxy(start)
        p = self.ptb.get(start)
        if p is None or self.ptb_mode == "proxy":
            return px
        if self.ptb_mode == "raw":
            return p[0]
        beta = self.beta_used.get(start)          # "basis": ptb corrected by the persistent Chainlink-vs-Binance basis
        return px if beta is None else p[0] - beta

    def _on_gamma_start(self, o, _):
        """Recorded `gamma_start` records: {"slug", "price_to_beat"} (Gamma eventMetadata.priceToBeat), stamped when seen."""
        if not self.v2 or not isinstance(o, dict):
            return
        m = SLUG_RE.match(o.get("slug", ""))
        if not m or o.get("price_to_beat") is None:
            return
        start, price = int(m.group(1)), float(o["price_to_beat"])
        if start in self.ptb:
            return
        px = self._proxy(start)
        self.ptb[start] = (price, self.now_utc - start)
        self.beta_used[start] = self.beta if self.beta_n >= self.ptb_min_hist else None
        if px is not None:
            lam = 0.5 ** (1.0 / self.ptb_hl)
            d = price - px
            self.beta = d if self.beta is None else lam * self.beta + (1.0 - lam) * d
            self.beta_n += 1

    def _on_bn_trade(self, o, _):
        if not isinstance(o, dict) or "p" not in o:
            return
        t = self.now_utc
        self._boundary_check(t)
        self.last_bn_t = t
        self.acc.ticks += 1
        if self.mid is not None:
            self._evaluate()

    # ------------------------------------------------------------------ polymarket
    def _on_pm(self, obj, _):
        evs = obj if isinstance(obj, list) else [obj]
        for ev in evs:
            if not isinstance(ev, dict):
                continue
            et = ev.get("event_type")
            if et == "price_change":
                last = {}
                for pc in ev.get("price_changes", ()):
                    bk = self.books.get(pc.get("asset_id"))
                    if bk is None or not bk.valid:
                        continue
                    bk.set_level(pc["side"], to_ticks(pc["price"]), float(pc["size"]))
                    last[pc["asset_id"]] = pc
                for tok, pc in last.items():
                    bk = self.books[tok]
                    if "best_bid" in pc and "best_ask" in pc:
                        self.pc["checked"] += 1
                        bb, ba = bk.best_bid(), bk.best_ask()
                        # an empty side is reported as best_bid "0" / best_ask "1", never as a level
                        xb = to_ticks(pc["best_bid"]) if pc["best_bid"] not in ("", None) else 0
                        xa = to_ticks(pc["best_ask"]) if pc["best_ask"] not in ("", None) else TICK
                        bad = (bb[0] if bb else 0) != xb or (ba[0] if ba else TICK) != xa
                        ts = float(ev.get("timestamp") or 0)
                        op = self.pc_open.get(tok)
                        if not bad:
                            self.pc_open.pop(tok, None)
                        elif op is None:
                            self.pc_open[tok] = [ts, False]
                            self.pc["episodes"] += 1
                        elif not op[1] and ts - op[0] > 1000:
                            op[1] = True
                            self.pc["persistent_gt_1s"] += 1
                if self.v3:
                    for tok in last:
                        self.pmh.touch(tok, self.now_utc)
                        self.pmh.update(tok, self.now_utc, self.books[tok])
            elif et == "book":
                bk = self.books.get(ev.get("asset_id"))
                if bk is not None:
                    bk.snapshot(ev.get("bids", ()), ev.get("asks", ()))
                    if self.pc_open.pop(ev.get("asset_id"), None) is not None:
                        self.pc["resolved_by_snapshot"] += 1
                    if self.v3:
                        self.pmh.touch(ev.get("asset_id"), self.now_utc)
                        self.pmh.update(ev.get("asset_id"), self.now_utc, bk)
            elif et == "last_trade_price":
                self._maker_print(ev)

    def _on_book_rest(self, o, _):
        tok = o.get("asset_id")
        info = self.token_map.get(tok)
        bk = self.books.get(tok)
        if info is None or bk is None:
            return
        meta = self.markets[info[0]]
        rb = {to_ticks(x["price"]): float(x["size"]) for x in o.get("bids", ()) if float(x["size"]) > 0}
        ra = {to_ticks(x["price"]): float(x["size"]) for x in o.get("asks", ()) if float(x["size"]) > 0}
        t = iso_utc(self.now_utc)
        if not bk.valid:
            self.sink.write("book_checks", [t, meta.slug, info[1], "rest_snapshot", len(bk.bids), len(bk.asks),
                                            len(rb), len(ra), "", "", "", "", "", "skipped_ws_book_invalid"])
            if self.gap_on:
                self._rest_rebuild(o, tok, meta)
            return
        bbm = (max(rb) if rb else None) == (bk.best_bid() or (None,))[0]
        bam = (min(ra) if ra else None) == (bk.best_ask() or (None,))[0]
        diffs, maxd = 0, 0.0
        for a, b in ((rb, bk.bids), (ra, bk.asks)):
            for k in set(a) | set(b):
                d = abs(a.get(k, 0.0) - b.get(k, 0.0))
                if d > 1e-6:
                    diffs += 1
                    maxd = max(maxd, d)
        res = "match" if diffs == 0 else "MISMATCH"
        self.acc.book_checks[0] += 1
        self.acc.book_checks[1] += diffs == 0
        self.acc.book_checks[2] += bbm and bam
        self.sink.write("book_checks", [t, meta.slug, info[1], "rest_snapshot", len(bk.bids), len(bk.asks), len(rb),
                                        len(ra), int(bbm), int(bam), diffs, maxd, "", res])
        if self.gap_on:
            self._rest_rebuild(o, tok, meta)

    # ------------------------------------------------------------------ evaluation
    def _evaluate(self, from_book=False):
        w = self.windows.get(self.cur)
        meta = self.markets.get(self.cur)
        if w is None or meta is None:
            return
        s0 = self._s0_v2(self.cur) if self.v2 else self._s0(self.cur)
        if s0 is None:
            self.cur_fair = None
            return
        t = self.now_utc
        tau, elapsed = w.end - t, t - w.start
        sig = self.sigma_est.sigma
        if self.v2:
            ea, vt = average_forecast(self.grid, self.mid, t, w.end + self.end_off, self.end_w)
            fair_up = p_up_v2(ea, s0, sig, vt, self.m_mult, self.m_basis_bps, self.m_dist, self.m_df, self.m_min_var_s)
        else:
            fair_up = fair_up_fn(self.mid, s0, sig, tau)
        self.cur_fair = fair_up
        ub, db = self.books.get(meta.up), self.books.get(meta.down)
        self._series(w, meta, s0, sig, tau, fair_up, ub, db)
        if not self.sigma_est.ready or tau <= 0 or elapsed < 0:
            self.best_edge_pts = None
            return
        for target in self.cal_taus:
            if target not in w.samples and tau <= target and target - tau <= self.cal_tol:
                um = ub.mid() if ub is not None and ub.valid else None
                if um is not None:
                    w.samples[target] = (fair_up, um)
        self._maker_step(w, meta, fair_up, tau, elapsed)
        if self.v3:
            if from_book:       # the move detector only advances on Binance book ticks, so v3 decides only on those
                self._v3_step(w, meta, s0, sig, tau, elapsed)
        else:
            self._taker_step(w, meta, fair_up, sig, tau, elapsed, s0)

    def _series(self, w, meta, s0, sig, tau, fair_up, ub, db):
        if self.last_series_mono is not None and self.mono_ns - self.last_series_mono < self.series_ns:
            return
        self.last_series_mono = self.mono_ns

        def bba(bk):
            if bk is None or not bk.valid:
                return None, None
            b, a = bk.best_bid(), bk.best_ask()
            return (b[0] / TICK if b else None), (a[0] / TICK if a else None)
        ubid, uask = bba(ub)
        dbid, dask = bba(db)
        self.sink.write("series", [iso_utc(self.now_utc), iso_utc(w.start), tau, self.mid, s0, sig, fair_up,
                                   ubid, uask, dbid, dask])

    # ------------------------------------------------------------------ taker track
    def _taker_step(self, w, meta, fair_up, sig, tau, elapsed, s0):
        best = None
        common_block = None
        gap = self._gap_block(meta)
        if not self.entries_enabled:
            common_block = "draining"
        elif gap:
            common_block = gap
        elif elapsed < self.no_first:
            common_block = "first_%gs" % self.no_first
        elif tau < self.no_last:
            common_block = "last_%gs" % self.no_last
        for side in ("up", "down"):
            tok = meta.up if side == "up" else meta.down
            bk = self.books.get(tok)
            if bk is None or not bk.valid:
                continue
            ba = bk.best_ask()
            if ba is None:
                continue
            ask_t, ask_sz = ba
            ask = ask_t / TICK
            fair = fair_up if side == "up" else 1.0 - fair_up
            fee = fee_per_share(ask, meta.fee_rate, meta.fee_exp)
            edge = (fair - ask - fee) * 100.0
            if best is None or edge > best:
                best = edge
            if edge < self.cand_min:
                continue
            blocked = common_block or (("price_band" if (ask < self.min_px or ask > self.max_px) else None))
            main_action, shadow, taken = None, [], False
            for tr in self.tracks:
                if edge < tr.thr:
                    if tr.main:
                        main_action = "SHADOW_ONLY(edge<%g)" % tr.thr
                    continue
                reason = blocked or self._track_block(w, tr)
                if reason:
                    if tr.main:
                        main_action = "BLOCKED:" + reason
                    continue
                self._taker_decide(w, meta, tr, side, tok, fair, ask_t, ask_sz, fee, edge, sig, tau, s0)
                taken = True
                if tr.main:
                    main_action = "TAKEN"
                else:
                    shadow.append(tr.name)
            action = (main_action or "n/a") + ("|shadow_entered:" + ",".join(shadow) if shadow else "")
            last = w.cand_last.get(side)
            if taken or last is None or last[1] != action or self.mono_ns - last[0] >= self.cand_interval_ns:
                w.cand_last[side] = (self.mono_ns, action)
                bb = bk.best_bid()
                self.sink.write("candidates", [iso_utc(self.now_utc), iso_ny(self.now_utc), iso_utc(w.start), meta.slug,
                                               side, fair, (bb[0] / TICK) if bb else None, ask, fee, edge, sig, tau, s0,
                                               self.mid, action])
        self.best_edge_pts = best

    def _track_block(self, w, tr):
        if w.position.get(tr.name):
            return "has_position"
        if w.inflight.get(tr.name):
            return "order_in_flight"
        if w.attempts.get(tr.name, 0) >= self.max_attempts:
            return "max_attempts"
        if self.mono_ns < w.cooldown.get(tr.name, -1):
            return "cooldown"
        return None

    def _taker_decide(self, w, meta, tr, side, tok, fair, ask_t, ask_sz, fee, edge, sig, tau, s0, limit_t=None, v3=None):
        self.tid += 1
        lat_med = self.lat.median()
        lat_p90 = max(self.lat.p90(), lat_med)
        ask = ask_t / TICK
        t = Rec(id=self.tid, track=tr.name, main=tr.main, start=w.start, slug=meta.slug, side=side, token=tok, meta=meta,
                dec_utc=self.now_utc, fair=fair, ask=ask, ask_sz=ask_sz, fee=fee, edge=edge, sigma=sig, tau=tau, s0=s0,
                s_t=self.mid, limit_t=(ask_t + self.slip_t) if limit_t is None else limit_t,
                target=math.floor(self.size_usd / ask * 100) / 100,
                lat_med=lat_med, lat_p90=lat_p90, base=None, p90=None, markouts={}, v3=v3, flips={}, markouts_bid={})
        bk_ = self.books.get(tok)
        bb_ = bk_.best_bid() if bk_ is not None and bk_.valid else None
        t.spread_pts = ((ask_t - bb_[0]) / TICK * 100.0) if bb_ else None      # analysis only (never an input to a decision)
        w.inflight[tr.name] = True
        w.attempts[tr.name] = w.attempts.get(tr.name, 0) + 1
        self.acc.decisions[tr.name] += 1
        self._at(self.mono_ns + int(lat_med * 1e6), self._taker_arrive, t, "base")
        self._at(self.mono_ns + int(lat_p90 * 1e6), self._taker_arrive, t, "p90")

    def _simulate_fill(self, t, due):
        bk = self.books.get(t.token)
        miss = lambda why: Rec(status="missed", reason=why, shares=0.0, frac=0.0, avg=None, fee=0.0, cost=0.0, utc=None)
        if bk is None or not bk.valid:
            return miss("book_invalid")
        fills = bk.walk_asks(t.limit_t, t.target)
        shares = sum(q for _, q in fills)
        if shares <= 0:
            return miss("no_liquidity_within_limit")
        frac = shares / t.target
        if frac < self.min_frac:
            return miss("fill_fraction<%g" % self.min_frac)
        cost = sum(px / TICK * q for px, q in fills)
        fee = sum(fee_per_share(px / TICK, t.meta.fee_rate, t.meta.fee_exp) * q for px, q in fills)
        return Rec(status="filled", reason="", shares=shares, frac=frac, avg=cost / shares, fee=fee, cost=cost,
                   utc=self._utc_of(due))

    def _taker_arrive(self, due, t, kind):
        res = self._simulate_fill(t, due)
        w = self.windows.get(t.start)
        if kind == "base":
            if w is not None:
                w.inflight[t.track] = False
            t.base = res
            if res.status == "filled":
                if w is not None:
                    if self.v3:
                        st = w.v3.get(t.track)
                        if st is None:
                            st = w.v3[t.track] = Rec(entries=0, last_dec=-INF, side=t.side)
                        st.entries += 1
                        st.last_dec, st.side = t.dec_utc, t.side
                    else:
                        w.position[t.track] = True
                    w.trades.append(t)
                if self.v31:
                    self.pos[t.track] = Rec(open=True, cooldown_until=-INF)
                for h in self.markouts:
                    self._at(due + int(h * NS), self._markout, t, h)
                if self.v31:
                    for h in self.markouts:
                        self._at(due + int(h * NS), self._markout_bid, t, h)
                if self.v3:
                    for h in self.sc.flip_s:      # v3.1: the sell is a DECISION at +h that then pays the order latency
                        self._at(due + int(h * NS), self._flip_decide if self.v31 else self._flip, t, "base", h)
            else:
                self.acc.base_missed[t.track] += 1
                if w is not None:
                    w.cooldown[t.track] = due + self.cooldown_ns
                self._write_trade(t, None)
        elif t.base is not None and t.base.status == "filled":
            t.p90 = res
            if self.v3 and res.status == "filled":
                for h in self.sc.flip_s:
                    self._at(due + int(h * NS), self._flip_decide if self.v31 else self._flip, t, "p90", h)

    def _markout(self, due, obj, h):
        bk = self.books.get(obj.token)
        m = bk.mid() if bk is not None and bk.valid else None
        px = obj.base.avg if hasattr(obj, "base") else obj.price
        obj.markouts[h] = None if m is None else (m - px) * 100.0

    def _markout_bid(self, due, t, h):
        """v3.1: markout measured at the BID with both taker fees (what leaving at +h would net per share, in points)."""
        bk = self.books.get(t.token)
        b = bk.best_bid() if bk is not None and bk.valid else None
        if b is None or t.base is None or not t.base.shares:
            t.markouts_bid[h] = None
            return
        px = b[0] / TICK
        t.markouts_bid[h] = (px - t.base.avg - t.base.fee / t.base.shares - fee_per_share(px, t.meta.fee_rate, t.meta.fee_exp)) * 100.0

    # ------------------------------------------------------------------ strategy v3: trade the lag
    def _v3_step(self, w, meta, s0, sig, tau, elapsed):
        """Called on every Binance book tick. A detected move + the Polymarket state strictly before it -> an anchored target; buy
        only in the direction of the move when the edge clears the threshold. Uses only data received up to now.
        v3.0: edge = target - ask - fee, evaluated on every tick of a move. v3.1: edge = (target - expected exit half-spread) - ask
        - fee(ask) - fee(expected exit price); the main tracks act only at the FIRST detection of a merged move, later ticks of
        the same move are evaluated by the separate in-move shadow track."""
        self.best_edge_pts = None
        mv = self.v3_move
        ev = self.merger.update(self.now_utc, mv)
        if mv is None:
            return
        v31 = self.v31
        first = ev is not None
        if v31 and not first and not self.has_inmove:
            return
        sc = self.sc
        sg = sv3.compute_signal(self, mv, sc, self.pmh)
        side = "up" if mv.dir > 0 else "down"
        last = self.v3_last = Rec(t=self.now_utc, dir=mv.dir, bps=mv.bps, anchor=sg.anchor, target=sg.target_up, edge=None,
                                  action=("SKIP:" + sg.reason) if sg.reason else "")
        if first:
            self.acc.v3_events += 1
        if sg.reason:
            if first:
                self._v3_log("event", w, meta, mv, sg, side, None, None, None, "SKIP:" + sg.reason)
            return
        tok = meta.up if side == "up" else meta.down
        bk = self.books.get(tok)
        ba = bk.best_ask() if bk is not None and bk.valid else None
        if ba is None:
            last.action = "SKIP:no_ask"
            if first:
                self._v3_log("event", w, meta, mv, sg, side, None, None, None, "SKIP:no_ask")
            return
        ask_t, ask_sz = ba
        ask = ask_t / TICK
        target_side = sg.target_up if side == "up" else 1.0 - sg.target_up
        fee = fee_per_share(ask, meta.fee_rate, meta.fee_exp)
        bb = bk.best_bid()
        spread = (ask_t - bb[0]) / TICK * 100.0 if bb else None
        hs = exit_px = None
        eff = target_side
        if v31:
            hs = self._half_spread(tok, None if spread is None else spread / 200.0)
            if hs is None:
                last.action = "SKIP:no_exit_estimate"
                if first:
                    self._v3_log("event", w, meta, mv, sg, side, ask, fee, None, "SKIP:no_exit_estimate")
                return
            exit_px = min(max(target_side - hs, 0.01), 0.99)               # where we expect to sell: the target less the half-spread
            eff = exit_px - fee_per_share(exit_px, meta.fee_rate, meta.fee_exp)   # net of the second taker fee
        edge = (eff - ask - fee) * 100.0
        self.best_edge_pts = last.edge = edge
        gap = self._gap_block(meta)
        if not self.entries_enabled:
            common = "draining"
        elif gap:
            common = gap
        elif elapsed < self.v3_no_first:
            common = "first_%gs" % self.v3_no_first
        elif v31 and tau <= self.main_exit + max(self.lat.p90(), self.lat.median()) / 1000.0 + self.last_margin_s:
            common = "last_horizon"           # the +main_exit s sell (plus p90 latency and a margin) must still fit inside the window
        elif not v31 and tau < self.no_last:
            common = "last_%gs" % self.no_last
        elif ask < self.min_px or ask > self.max_px:
            common = "price_band"
        elif spread is None or spread > sc.max_spread_pts + 1e-9:
            common = "spread"
        elif self.now_utc - self.pmh.last_msg.get(tok, -INF) > sc.max_book_age_s:
            common = "book_stale"
        else:
            common = None
        main_action, shadow, taken = None, [], False
        if edge >= self.cand_min:
            for tr in self.tracks:
                if v31 and tr.inmove == first:
                    continue                  # first detection -> main tracks; later ticks of the move -> the in-move shadow track
                if edge < tr.thr:
                    if tr.main or tr.inmove:
                        main_action = "SHADOW_ONLY(edge<%g)" % tr.thr
                    continue
                reason = common or self._v3_track_block(w, tr, side)
                if reason:
                    if tr.main or tr.inmove:
                        main_action = "BLOCKED:" + reason
                    continue
                lim = sv3.limit_ticks(ask_t, self.slip_t, meta.tick_t, eff, tr.thr, meta.fee_rate, meta.fee_exp)
                ctx = Rec(dir=mv.dir, bps=mv.bps, anchor=sg.anchor, anchor_mid=sg.anchor_mid, anchor_micro=sg.anchor_micro,
                          p_now=sg.p_now, p_base=sg.p_base, shift=sg.shift, target_up=sg.target_up, spread=spread,
                          move_start=mv.start_t, base_mid=mv.base_mid, mid_now=mv.mid, exit_hs=hs, exit_px=exit_px)
                self._taker_decide(w, meta, tr, side, tok, target_side, ask_t, ask_sz, fee, edge, sig, tau, s0, limit_t=lim, v3=ctx)
                self._v3_log("entry", w, meta, mv, sg, side, ask, fee, edge, "TAKEN:" + tr.name, hs, exit_px)
                taken = True
                if tr.main or tr.inmove:
                    main_action = "TAKEN"
                else:
                    shadow.append(tr.name)
            action = (main_action or "n/a") + ("|shadow_entered:" + ",".join(shadow) if shadow else "")
            if v31 and not first:
                action = "INMOVE:" + action
            lc = w.cand_last.get(side)
            if taken or lc is None or lc[1] != action or self.mono_ns - lc[0] >= self.cand_interval_ns:
                w.cand_last[side] = (self.mono_ns, action)
                self.sink.write("candidates", [iso_utc(self.now_utc), iso_ny(self.now_utc), iso_utc(w.start), meta.slug,
                                               side, target_side, (bb[0] / TICK) if bb else None, ask, fee, edge, sig, tau, s0,
                                               self.mid, action])
        last.action = main_action or ("BLOCKED:" + common if common else "edge<%g" % self.cand_min)
        if first:
            self._v3_log("event", w, meta, mv, sg, side, ask, fee, edge,
                         ("BLOCKED:" + common) if common else (main_action or "no_edge"), hs, exit_px)

    # ---- v3.1: recent median half-spread of a token (1 sample per second, last hs_window_s seconds)
    def _hs_sample(self, t):
        meta = self.markets.get(self.cur)
        if meta is None:
            return
        for tok in (meta.up, meta.down):
            lt = self.hs_last.get(tok)
            if lt is not None and t - lt < 1.0:
                continue
            bk = self.books.get(tok)
            if bk is None or not bk.valid:
                continue
            b, a = bk.best_bid(), bk.best_ask()
            if b is None or a is None:
                continue
            self.hs_last[tok] = t
            dq = self.hs.setdefault(tok, deque())
            dq.append((t, (a[0] - b[0]) / 2.0 / TICK))
            lim = t - self.hs_window_s
            while dq and dq[0][0] < lim:
                dq.popleft()

    def _half_spread(self, tok, fallback):
        dq = self.hs.get(tok)
        if dq and len(dq) >= self.hs_min_samples:
            return statistics.median(v for _, v in dq)
        return fallback

    def _v3_track_block(self, w, tr, side):
        if self.v31:
            # one open position at a time (per track) + a cooldown after each exit; both sides allowed, no per-window caps
            if w.inflight.get(tr.name):
                return "order_in_flight"
            pos = self.pos.get(tr.name)
            if pos is not None:
                if pos.open:
                    return "position_open"
                if self.now_utc < pos.cooldown_until:
                    return "cooldown_after_exit"
            if self.mono_ns < w.cooldown.get(tr.name, -1):
                return "cooldown"
            return None
        st = w.v3.get(tr.name)
        if w.inflight.get(tr.name):
            return "order_in_flight"
        if st is not None:
            if st.entries >= self.sc.max_entries:
                return "max_entries"
            if st.side != side:
                return "opposite_side_held"
            if self.now_utc - st.last_dec < self.sc.spacing_s:
                return "spacing"
        if w.attempts.get(tr.name, 0) >= self.max_attempts:
            return "max_attempts"
        if self.mono_ns < w.cooldown.get(tr.name, -1):
            return "cooldown"
        return None

    def _v3_log(self, kind, w, meta, mv, sg, side, ask, fee, edge, action, hs=None, exit_px=None):
        row = [iso_utc(self.now_utc), kind, iso_utc(w.start), meta.slug, mv.dir, mv.bps, iso_utc(mv.start_t),
               mv.base_mid, mv.mid, sg.anchor_mid, sg.anchor_micro, sg.anchor,
               None if sg.anchor_age is None else sg.anchor_age * 1000.0, sg.anchor_spread, sg.p_now, sg.p_base,
               sg.shift, self.sc.beta, sg.target_up, side, ask, fee, edge, sg.tau, sg.sigma, sg.s0]
        if self.v31:
            row += [None if hs is None else hs * 100.0, exit_px]
        self.sink.write("v3_signals", row + [action])

    def _flip_decide(self, due, t, kind, h):
        """v3.1: the +h s sell is a decision taken now; it reaches the book after the simulated latency (rolling median for the base
        case, p90 for the stress test) and walks the bids as they are THEN."""
        fill = t.base if kind == "base" else t.p90
        if fill is None or fill.status != "filled":
            return
        lat = self.lat.median() if kind == "base" else max(self.lat.p90(), self.lat.median())
        self._at(due + int(lat * 1e6), self._flip, t, kind, h)

    def _flip(self, due, t, kind, h):
        """Exit track: sell the whole fill into the recorded bids (pay the taker fee again). What cannot be sold (window over, book
        gone, bids exhausted) simply stays held to the official resolution. v3.0 sells at fill+h; v3.1 at fill+h+latency."""
        fill = t.base if kind == "base" else t.p90
        if fill is None or fill.status != "filled":
            return
        now_u = self._utc_of(due)
        if now_u >= t.meta.end:
            t.flips[(kind, h)] = Rec(status="window_ended", sold=0.0, proceeds=0.0, fee=0.0, avg=None, utc=now_u)
        else:
            bk = self.books.get(t.token)
            if bk is None or not bk.valid:
                t.flips[(kind, h)] = Rec(status="book_invalid", sold=0.0, proceeds=0.0, fee=0.0, avg=None, utc=now_u)
            else:
                sells = bk.walk_bids(fill.shares)
                sold = sum(q for _, q in sells)
                proceeds = sum(px / TICK * q for px, q in sells)
                fee = sum(fee_per_share(px / TICK, t.meta.fee_rate, t.meta.fee_exp) * q for px, q in sells)
                status = "sold" if sold >= fill.shares - 1e-9 else ("partial" if sold > 0 else "no_bids")
                t.flips[(kind, h)] = Rec(status=status, sold=sold, proceeds=proceeds, fee=fee,
                                         avg=(proceeds / sold) if sold else None, utc=now_u)
        if self.v31 and kind == "base" and h == self.main_exit:      # the main exit closes the position; the cooldown starts now
            pos = self.pos.get(t.track)
            if pos is not None:
                pos.open = False
                pos.cooldown_until = now_u + self.cooldown_exit_s

    def _flip_net(self, t, kind, h, result):
        fill = t.base if kind == "base" else t.p90
        if result is None or fill is None or fill.status != "filled":
            return None
        fl = t.flips.get((kind, h))
        sold, proceeds, fee = (fl.sold, fl.proceeds, fl.fee) if fl is not None else (0.0, 0.0, 0.0)
        win = (t.side == "up") == (result == "Up")
        return proceeds - fee + (fill.shares - sold) * (1.0 if win else 0.0) - fill.cost - fill.fee

    def _write_v3_trade(self, t, result, base_net, p90_net):
        b, s, v = t.base, t.p90, t.v3
        row = [t.id, t.track, int(t.main), iso_utc(t.start), t.slug, t.side, iso_utc(t.dec_utc), t.tau, v.dir, v.bps, v.anchor,
               v.target_up, v.p_now, v.p_base, t.fair, t.ask, t.edge, t.limit_t / TICK, v.spread]
        if self.v31:
            row += [None if v.exit_hs is None else v.exit_hs * 100.0, v.exit_px]
        row += [b.status if b else "", iso_utc(b.utc) if b and b.utc else "", b.shares if b else "", b.avg if b else None,
               b.fee if b else None, b.cost if b else None,
               (s.status if s else ("n/a" if b and b.status == "filled" else "")), s.shares if s else "", s.avg if s else None,
               s.fee if s else None, s.cost if s else None]
        for h in self.sc.flip_s:
            fb = t.flips.get(("base", h))
            row += [fb.status if fb else ""] + ([iso_utc(fb.utc) if fb else ""] if self.v31 else []) + [
                fb.sold if fb else None, fb.avg if fb else None, fb.fee if fb else None,
                self._flip_net(t, "base", h, result), self._flip_net(t, "p90", h, result)]
        if self.v31:
            row += [t.markouts_bid.get(h) for h in self.markouts]
        row += [result if result else ("UNRESOLVED" if b and b.status == "filled" else ""), base_net, p90_net]
        self.sink.write("v3_trades", row)

    # ------------------------------------------------------------------ maker shadow track
    def _maker_step(self, w, meta, fair_up, tau, elapsed):
        elig = self.entries_enabled and not self._gap_block(meta) and elapsed >= self.no_first and tau >= self.no_last
        lat_s = self.lat.median() / 1000.0
        for side in ("up", "down"):
            cur = w.m_cur.get(side)
            fair = fair_up if side == "up" else 1.0 - fair_up
            if cur is not None:
                move = abs(self.mid / cur.mid_ref - 1.0) * 1e4
                if (not elig) or move >= self.m_cancel_bps or (self.now_utc - cur.dec_utc) >= self.m_cancel_after:
                    cur.live_until_u = self.now_utc + lat_s   # the cancel only takes effect after the latency
                    w.m_cur[side] = None
                    self.acc.maker_cancelled += 1
                    cur = None
            if cur is None and elig and w.m_fills[side] < self.m_cap:
                self._maker_place(w, meta, side, fair, lat_s)

    def _maker_place(self, w, meta, side, fair, lat_s):
        tok = meta.up if side == "up" else meta.down
        bk = self.books.get(tok)
        if bk is None or not bk.valid:
            return
        ba = bk.best_ask()
        if ba is None:
            return
        price_t = math.floor((fair * TICK - self.m_off_t) / meta.tick_t) * meta.tick_t
        if price_t < self.min_px * TICK or price_t > self.max_px * TICK or price_t >= ba[0]:
            return
        self.mid_ctr += 1
        px = price_t / TICK
        o = Rec(id=self.mid_ctr, start=w.start, slug=meta.slug, side=side, token=tok, price_t=price_t, price=px,
                shares=math.floor(self.size_usd / px * 100) / 100, dec_utc=self.now_utc,
                live_from_u=self.now_utc + lat_s, live_until_u=INF, fair=fair, mid_ref=self.mid)
        w.m_cur[side] = o
        w.m_orders[side] = [x for x in w.m_orders[side] if x.live_until_u > self.now_utc - 10] + [o]
        self.acc.maker_placed += 1

    def _maker_close(self, w):
        for side, o in list(w.m_cur.items()):
            if o is not None:
                o.live_until_u = self.now_utc
        w.m_cur = {}

    def _maker_print(self, ev):
        info = self.token_map.get(ev.get("asset_id"))
        if info is None:
            return
        w = self.windows.get(info[0])
        if w is None:
            return
        side = info[1]
        ts = ev.get("timestamp")
        pu = float(ts) / 1000.0 if ts else self.now_utc   # exchange time of the print (when it actually happened)
        px_t = to_ticks(ev["price"])
        size = float(ev["size"])
        for o in w.m_orders[side]:
            if getattr(o, "filled", False) or w.m_fills[side] >= self.m_cap:
                continue
            if o.live_from_u <= pu < o.live_until_u and px_t <= o.price_t:
                o.filled = True
                w.m_fills[side] += 1
                if w.m_cur.get(side) is o:
                    w.m_cur[side] = None
                f = Rec(order=o, print_u=pu, print_px=px_t / TICK, print_size=size, through=px_t < o.price_t,
                        shares=min(o.shares, size), price=o.price, token=o.token, markouts={},
                        move_bps=(self.mid / o.mid_ref - 1.0) * 1e4 if self.mid else None)
                w.m_filled.append(f)
                self.acc.maker_fills.append(f)
                for h in self.markouts:
                    self._at(self.mono_ns + int(h * NS), self._markout, f, h)
                break

    # ------------------------------------------------------------------ resolution / settlement
    def _on_resolution(self, o, _):
        m = SLUG_RE.match(o.get("slug", ""))
        if not m:
            return
        start = int(m.group(1))
        prices = [float(x) for x in _jf(o.get("outcomePrices") or "[]")]
        outs = _jf(o.get("outcomes") or "[]")
        if not prices or max(prices) < 0.99:
            return
        result = outs[prices.index(max(prices))]
        self._settle(start, result)
        if self.v2:
            self._write_start_price(start, result, o)

    def _write_start_price(self, start, result, o):
        """v2 only. Gamma's own start/end reference prices (priceToBeat / finalPrice, from the resolution record) next to the
        Binance TWAP proxies: this is the direct measurement of the Binance-vs-Chainlink basis. Analysis only."""
        try:
            md = (o.get("events") or [{}])[0].get("eventMetadata") or {}
        except (AttributeError, IndexError, TypeError):
            md = {}
        gp, gf = md.get("priceToBeat"), md.get("finalPrice")
        px = self._proxy(start)
        ep = self._end_proxy(start)
        seen = self.ptb.get(start)
        bps = lambda a, b: None if (a is None or b is None) else (float(a) / b - 1.0) * 1e4
        self.sink.write("start_prices", [iso_utc(start), "%s%d" % ("btc-updown-5m-", start), gp,
                                         None if seen is None else iso_utc(start + seen[1]), None if seen is None else seen[1],
                                         px, bps(gp, px), gf, ep, bps(gf, ep), result, self.ptb_mode])
        for k in [k for k in self.proxy_s0 if k < start - 20 * self.win_s]:
            self.proxy_s0.pop(k, None)
            self.ptb.pop(k, None)
            self.beta_used.pop(k, None)

    def _write_trade(self, t, result):
        b, s = t.base, t.p90
        base_net = p90_net = None
        if result is not None and b is not None and b.status == "filled":
            win = (t.side == "up") == (result == "Up")
            base_net = (b.shares if win else 0.0) - b.cost - b.fee
            p90_net = 0.0
            if s is not None and s.status == "filled":
                p90_net = (s.shares if win else 0.0) - s.cost - s.fee
        mo = [t.markouts.get(h) for h in self.markouts]
        row = [t.id, t.track, int(t.main), iso_utc(t.start), t.slug, t.side, iso_utc(t.dec_utc), iso_ny(t.dec_utc),
               t.fair, t.ask, t.ask_sz, t.fee, t.edge, t.sigma, t.tau, t.s0, t.s_t, t.limit_t / TICK, t.target,
               t.lat_med, t.lat_p90,
               b.status if b else "", b.reason if b else "", iso_utc(b.utc) if b and b.utc else "",
               b.shares if b else "", b.frac if b else "", b.avg if b else None, b.fee if b else None,
               b.cost if b else None,
               (s.status if s else ("n/a" if b and b.status == "filled" else "")), s.reason if s else "",
               s.shares if s else "", s.frac if s else "", s.avg if s else None, s.fee if s else None,
               s.cost if s else None] + mo + [result if result else ("UNRESOLVED" if b and b.status == "filled" else ""),
                                              base_net, p90_net]
        self.sink.write("trades", row)
        if self.v3:
            self._write_v3_trade(t, result, base_net, p90_net)
        return base_net, p90_net

    def _settle(self, start, result, unresolved=False):
        w = self.windows.get(start)
        res_arg = None if unresolved else result
        if w is not None:
            for t in w.trades:
                bn, pn = self._write_trade(t, res_arg)
                if unresolved:
                    self.acc.unresolved_trades += 1
                    continue
                rec = Rec(track=t.track, main=t.main, start=start, side=t.side, base_net=bn,
                          p90_filled=(t.p90 is not None and t.p90.status == "filled"), p90_net=pn,
                          edge=t.edge, markouts=dict(t.markouts), won=(t.side == "up") == (result == "Up"))
                rec.tau, rec.spread_pts, rec.dec_utc = t.tau, t.spread_pts, t.dec_utc          # analysis fields (all strategies)
                rec.fee_pts = (t.base.fee / t.base.shares * 100.0) if t.base.shares else 0.0
                if self.v3:
                    rec.v3, rec.fill = t.v3, t.base
                    rec.flips, rec.fill_p90 = t.flips, t.p90                  # analysis / tests
                    rec.flip_base = {h: self._flip_net(t, "base", h, result) for h in self.sc.flip_s}
                    rec.flip_p90 = {h: self._flip_net(t, "p90", h, result) for h in self.sc.flip_s}
                    rec.flip_status = {h: (t.flips[("base", h)].status if ("base", h) in t.flips else "none") for h in self.sc.flip_s}
                    rec.markouts_bid = dict(t.markouts_bid)
                self.acc.trades.append(rec)
                if t.main:
                    self.acc.main_filled += 1
                    self.acc.main_net_base += rec.flip_base[self.main_exit] if self.v31 and rec.flip_base[self.main_exit] is not None else bn
            for f in w.m_filled:
                o = f.order
                win = (o.side == "up") == (result == "Up")
                pnl = None if unresolved else f.shares * ((1.0 if win else 0.0) - o.price)
                self.sink.write("maker_shadow", [f.order.id, iso_utc(start), o.slug, o.side, iso_utc(o.dec_utc),
                                                 iso_utc(o.live_from_u), o.price, o.shares, o.fair, o.mid_ref,
                                                 iso_utc(f.print_u), f.print_px, f.print_size, int(f.through), f.shares,
                                                 f.move_bps] + [f.markouts.get(h) for h in self.markouts]
                                + ["UNRESOLVED" if unresolved else result, pnl])
                f.pnl, f.result = pnl, (None if unresolved else result)
            if not unresolved:
                for tau, (pm, um) in w.samples.items():
                    y = 1.0 if result == "Up" else 0.0
                    a = self.acc.brier[tau]
                    self.acc.brier_rows.append((start, tau, pm, um, y))
                    a[0] += (pm - y) ** 2
                    a[1] += (um - y) ** 2
                    a[2] += 1
        if not unresolved and start not in self.written_windows:
            self.written_windows.add(start)
            if self.v2:   # Binance-implied outcome under the TWAP definition: mean of 60 s before start vs last 60 s
                s0 = self._proxy(start)
                end_p = self._end_proxy(start)
            else:
                s0, end_p = self._s0(start), self._s0(start + self.win_s)
            implied = None if s0 is None or end_p is None else ("Up" if end_p >= s0 else "Down")
            mism = None if implied is None else int(implied != result)
            self.sink.write("windows", [iso_utc(start), "%s%d" % ("btc-updown-5m-", start), s0, end_p, result, implied,
                                        mism, int(s0 is not None), int(end_p is not None),
                                        None if (s0 is None or end_p is None) else (end_p / s0 - 1) * 1e4])
            self.acc.windows.append(Rec(start=start, result=result, implied=implied, mismatch=mism))
        if w is not None:
            self.windows.pop(start, None)
        meta = self.markets.pop(start, None)
        if meta is not None:
            for tok in (meta.up, meta.down):
                self.books.pop(tok, None)
                self.token_map.pop(tok, None)
                if self.v3 and self.sc.pm_keep_s is not None:
                    self.pmh.drop(tok)
                if self.v31:
                    self.hs.pop(tok, None)
                    self.hs_last.pop(tok, None)

    def open_windows(self):
        """Windows with filled trades or maker fills still waiting for the official result."""
        return [s for s, w in self.windows.items() if w.trades or w.m_filled]

    def finalize(self):
        """Shutdown: anything still unresolved is written as UNRESOLVED and excluded from statistics."""
        for start in sorted(self.windows):
            w = self.windows[start]
            if w.trades or w.m_filled:
                self._settle(start, None, unresolved=True)
        self.sink.flush()

    # ------------------------------------------------------------------ status
    def status(self):
        meta = self.markets.get(self.cur)
        w = self.windows.get(self.cur)
        s0 = None
        if self.cur:
            s0 = (self._s0_v2(self.cur) if self.cur in self.proxy_s0 else None) if self.v2 else self._s0(self.cur)
        d = {"window_start": self.cur, "tau": (w.end - self.now_utc) if w else None, "s0": s0,
             "s_t": self.mid, "sigma": self.sigma_est.sigma, "p_up": self.cur_fair, "best_edge": self.best_edge_pts,
             "lat_med": self.lat.median(), "lat_p90": self.lat.p90(), "trades": self.acc.main_filled,
             "net": self.acc.main_net_base, "warm": self.sigma_est.ready}
        if self.v3:
            lm = self.v3_last
            d["v3"] = None if lm is None else {"age_s": self.now_utc - lm.t, "dir": lm.dir, "bps": lm.bps, "anchor": lm.anchor,
                                                "target": lm.target, "edge": lm.edge, "action": lm.action}
        for key, tok in (("up", meta.up if meta else None), ("dn", meta.down if meta else None)):
            bk = self.books.get(tok) if tok else None
            b = bk.best_bid() if bk and bk.valid else None
            a = bk.best_ask() if bk and bk.valid else None
            d[key] = (b[0] / TICK if b else None, a[0] / TICK if a else None)
        return d
