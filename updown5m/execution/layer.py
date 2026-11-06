"""The dry-run execution layer: turns every v3.1 MAIN-track paper decision into the order(s) a live bot would send, runs them through a real
order state machine with the risk controls, and scores them next to the paper result. NOTHING is signed or sent.

The layer only READS the engine (books, latency, clock, markets, the paper trade records). It owns its own timer heap and is driven by
`before_record(mono)` (before the engine applies a record: the same book view as the engine's own arrival timers) and `after_record(...)` (after).

Time: every instant is a record-derived UTC second (`engine._utc_of(mono)`), so a replay of the recorded raw data reproduces the dry run
exactly, like the paper engine.
"""
import heapq
import json
import math
import traceback
from decimal import Decimal
from types import SimpleNamespace as Rec

from ..book import TICK
from ..engine import SLUG_RE
from ..util import iso_utc
from . import orders as O
from .clock import ClockEstimator
from .config import Mode
from .exchbook import PmBuffer, Replica, token_events
from .feeds import FeedMonitor
from .fills import EPS, buy_fill, sell_fill
from .risk import Risk
from .state import OPEN, S, check

INF = float("inf")
NS = 1_000_000_000
MAX_ERRORS = 50


class XTrade:
    """Everything about one paper main-track decision on the dry-run side."""

    def __init__(self, t, mono, wall, lat_ms):
        self.id, self.t, self.mono, self.wall, self.lat_ms = t.id, t, mono, wall, lat_ms
        self.state = S.IDLE
        self.block = None
        self.intent = None
        self.order_size = Decimal(0)
        self.shares = self.cost = self.fee = 0.0
        self.avg = None
        self.entry_status, self.entry_miss = "", ""
        self.fill_due = None
        self.fill_utc = None
        self.floor_t = None
        self.sold = self.proceeds = self.sell_fee = 0.0
        self.attempts = []                  # dicts: attempt, step_s, status, shares, avg, fee, floor_hit, reason
        self.next_attempt = 1
        self.gen = 0
        self.waiting = False
        self.kill_pulled = False
        self.since = mono
        self.stuck = False
        self.stuck_reason = ""
        self.dust = False
        self.net = None
        self.result = None
        self.settled = False
        self.theta_ms = None
        self.dx = {}
        self.floor_hit = False
        self.notes = []

    @property
    def held(self):
        return max(self.shares - self.sold, 0.0)


class DryRunLayer:
    _MODE = Mode.DRY_RUN                # LiveLayer overrides: the same machinery, but orders really leave the machine

    def __init__(self, cfg, out, session, mode=Mode.DRY_RUN, recovered=None, kill_exists_fn=None, log=print):
        if mode is not self._MODE:
            raise ValueError(f"this execution layer only runs in {self._MODE.value} mode")
        self.cfg, self.out, self.session, self.mode, self.log = cfg, out, session, mode, log
        self.eng = None
        self.risk = Risk(cfg)
        if kill_exists_fn is not None:
            self.risk.kill.exists_fn = kill_exists_fn
        self.feeds = FeedMonitor(cfg.feed_reconnect_block_s)
        self.clock = ClockEstimator(cfg.clock_rtt_band_ms, cfg.clock_window_s, cfg.clock_min_samples)
        self.buf = PmBuffer(cfg.exch_buffer_s)
        self.ometa = {}                 # slug -> OrderMeta
        self.tok_slug = {}
        self.timers, self.tcount = [], 0
        self.trades = {}
        self.by_window = {}
        self.slot = None
        self.cooldown_until = -INF
        self.draining = False
        self.rows = []                  # finished dryrun_vs_paper rows (dicts)
        self.errors = 0
        self.disabled = False
        self.counts = {"decisions": 0, "entry_sent": 0, "exit_sent": 0, "entry_filled": 0, "entry_partial": 0, "entry_missed": 0,
                       "closed": 0, "stuck": 0, "dust": 0, "settled": 0, "kill_exits": 0}
        self.step_stats = {}            # attempt -> {"sent":n, "full":n, "partial":n, "none":n}
        self.miss_reasons = {}
        self.clock_warned = -INF
        self.recovered = recovered
        self.rec_until = None
        self.rec_open = list(recovered.unresolved) if recovered is not None else []
        if recovered is not None and recovered.ok:
            for u in sorted(recovered.sent_utc):
                self.risk.sent.append(u)
            self.risk.seed_net, self.risk.seed_stuck = dict(recovered.net_by_day), dict(recovered.stuck_by_day)
            self.rec_until = recovered.blocking_until(cfg.recovered_release_s)
        self._boot_event()

    # ------------------------------------------------------------------ plumbing
    def bind(self, eng):
        self.eng = eng

    def _release_recovered(self):
        """Positions from an interrupted earlier run whose result this process can never see: booked as a worst-case loss, then released."""
        loss = sum(u["worst_case_usd"] for u in self.rec_open)
        self.risk.note_net(self.eng.now_utc, loss)
        self.event(None, "RECOVERY_RELEASED", reason=f"{len(self.rec_open)} unresolved position(s) booked at worst case ${loss:.2f}",
                   detail=[u["key"] for u in self.rec_open])
        self.rec_open = []

    def _boot_event(self):
        r = self.recovered
        self.event(None, "START", reason=f"mode={self.mode.value}", detail={"config_sha256": self.cfg.sha256()[:16],
                                                                            "recovered_files": 0 if r is None else len(r.files),
                                                                            "recovered_unresolved": len(self.rec_open)}, utc=None)
        if r is not None and not r.ok:
            self.event(None, "RECOVERY_FAILED", reason=r.error, utc=None)

    def utc(self, mono):
        return self.eng._utc_of(mono)

    def event(self, tr, name, frm=None, to=None, reason="", utc=0.0, **kw):
        u = self.eng.now_utc if (utc == 0.0 and self.eng is not None) else utc
        t = tr.t if tr is not None else None
        meta = t.meta if t is not None else None
        row = dict(time_utc=iso_utc(u) if u else "", trade_id=tr.id if tr is not None else "", event=name,
                   state_from=frm.value if frm else "", state_to=to.value if to else "", reason=reason,
                   slug=meta.slug if meta else "", token_id=t.token if t else "", side=t.side if t else "",
                   window_end_utc=iso_utc(meta.end) if meta else "", held_shares=None if tr is None else round(tr.held, 6))
        row.update(kw)
        self.out.event(**row)

    def go(self, tr, to, reason="", utc=None, **kw):
        check(tr.state, to)
        frm, tr.state = tr.state, to
        tr.since = self.eng.mono_ns
        self.event(tr, "STATE", frm, to, reason, utc=self.eng.now_utc if utc is None else utc, **kw)

    def at(self, due_mono, fn, *args):
        self.tcount += 1
        heapq.heappush(self.timers, (due_mono, self.tcount, fn, args))

    # ------------------------------------------------------------------ record hooks
    def before_record(self, mono_ns):
        if self.rec_open and self.rec_until is not None and self.eng.now_utc >= self.rec_until:
            self._release_recovered()
        while self.timers and self.timers[0][0] <= mono_ns:
            due, _, fn, args = heapq.heappop(self.timers)
            fn(due, *args)
        self._timeouts(mono_ns)
        self._kill_exit(mono_ns)

    def after_record(self, feed, wall_ns, mono_ns, exch_ts, obj):
        if feed == "polymarket":
            self.buf.add(mono_ns, wall_ns, feed, obj)
            if isinstance(obj, (list, dict)):
                for ev in (obj if isinstance(obj, list) else [obj]):
                    if isinstance(ev, dict) and ev.get("event_type") == "tick_size_change":
                        self._tick_change(ev)
        elif feed == "ctl":
            self.buf.add(mono_ns, wall_ns, feed, obj)
            self.feeds.on_ctl(obj, mono_ns)
            if isinstance(obj, dict) and obj.get("ev") == "drain":
                self.draining = True
                self.event(None, "DRAIN", reason="no new entries")
        elif feed == "clock_offset_v2":
            if isinstance(obj, dict):
                self.clock.add(obj.get("host"), obj.get("recv_wall_ns", wall_ns), obj.get("rtt_ms", 1e9), obj.get("offset_ms", 0.0))
        elif feed == "gamma_meta":
            self._meta(obj)
        elif feed == "pm_book_rest":
            self._book_meta(obj)
        elif feed == "gamma_resolution":
            self._resolution(obj)
        self._poll_waiting(mono_ns)

    # ------------------------------------------------------------------ metadata (recorded records only: no requests)
    def _meta(self, o):
        try:
            ids = json.loads(o["clobTokenIds"]) if isinstance(o["clobTokenIds"], str) else o["clobTokenIds"]
            outs = json.loads(o["outcomes"]) if isinstance(o["outcomes"], str) else o["outcomes"]
            up, down = ids[outs.index("Up")], ids[outs.index("Down")]
        except (KeyError, ValueError, TypeError):
            return
        m = O.OrderMeta(o["slug"], up, down, tick_t=int(round(float(o.get("orderPriceMinTickSize") or 0.01) * TICK)),
                        min_size=float(o.get("orderMinSize") or 5), neg_risk=bool(o.get("negRisk", False)), source="gamma_meta")
        self.ometa[o["slug"]] = m
        self.tok_slug[up] = self.tok_slug[down] = o["slug"]

    def _book_meta(self, o):
        slug = self.tok_slug.get(o.get("asset_id")) if isinstance(o, dict) else None
        m = self.ometa.get(slug)
        if m is None:
            return
        try:
            m.tick_t = int(round(float(o["tick_size"]) * TICK))
            m.min_size = float(o["min_order_size"])
            m.neg_risk = bool(o["neg_risk"])
            m.source = "book_rest"
        except (KeyError, ValueError, TypeError):
            pass

    def _tick_change(self, ev):
        m = self.ometa.get(self.tok_slug.get(ev.get("asset_id")))
        if m is not None and ev.get("new_tick_size"):
            new_t = int(round(float(ev["new_tick_size"]) * TICK))
            if new_t != m.tick_t:                                   # the exchange repeats the event per token / message: log real changes only
                self.event(None, "TICK_SIZE_CHANGE", reason=f"{m.slug}: {m.tick_t / TICK:g} -> {ev['new_tick_size']}")
            m.tick_t, m.source = new_t, "tick_size_change"

    def _meta_for(self, t):
        m = self.ometa.get(t.slug)
        if m is None:
            m = O.OrderMeta(t.slug, t.meta.up, t.meta.down, tick_t=t.meta.tick_t, source="default")
            self.ometa[t.slug] = m
        return m

    # ------------------------------------------------------------------ the decision
    def _budget_usd(self):
        return self.cfg.size_usd

    def on_decision(self, t, force=False):
        """`force` (the live layer's simulated twin only): skip the risk / slot / feed checks, the decision was already sent for real."""
        eng = self.eng
        self.counts["decisions"] += 1
        tr = XTrade(t, eng.mono_ns, eng.wall_ns, t.lat_med)
        self.trades[t.id] = tr
        self.by_window.setdefault(t.start, []).append(tr)
        self.event(tr, "DECISION", reason=f"edge={t.edge:.2f}pts limit={t.limit_t / TICK:.4f}", utc=t.dec_utc,
                   side=t.side, price=t.limit_t / TICK)
        reason = None if force else self._entry_block(tr, t)
        meta = self._meta_for(t)
        intent = None
        if reason is None:
            outcome = "Up" if t.side == "up" else "Down"
            intent = O.build_entry(meta, outcome, t.token, t.limit_t, self._budget_usd(), t.id, f"u5m-{self.session}-{t.id}-E1",
                                   paper_target=t.target)
            if isinstance(intent, O.Reject):
                reason = "order_" + intent.reason
        if reason is not None:
            tr.block = reason
            self.risk.roll(t.dec_utc)
            self.risk.note_block(reason)
            self.event(tr, "RISK_BLOCK", reason=reason, utc=t.dec_utc)
            return
        tr.intent, tr.order_size, tr.notes = intent, intent.size, list(intent.notes)
        self._entry_submit(tr, t, intent)

    def _entry_submit(self, tr, t, intent):
        """The order leaves here. Dry run: log the intent and schedule the simulated arrival. (LiveLayer overrides: sign + send.)"""
        eng = self.eng
        send_utc = t.dec_utc + self.cfg.send_delay_ms / 1000.0
        arr_utc = send_utc + tr.lat_ms / 1000.0
        self.out.order(intent.payload(self.mode.value, iso_utc(t.dec_utc), iso_utc(send_utc), iso_utc(arr_utc), iso_utc(t.start),
                                      iso_utc(t.meta.end), int(send_utc * 1000)))
        self.risk.note_sent(t.dec_utc)
        self.counts["entry_sent"] += 1
        self.slot = tr
        self.event(tr, "ENTRY_SENT", reason=intent.client_order_id, utc=t.dec_utc, side=t.side, shares=float(intent.size),
                   price=float(intent.price))
        self.go(tr, S.ENTRY_PENDING, "order sent", utc=t.dec_utc)
        delay_ns = int((self.cfg.send_delay_ms + tr.lat_ms) * 1e6)
        self.at(eng.mono_ns + delay_ns, self._entry_arrive, tr)
        self.at(eng.mono_ns + delay_ns + int((2 * tr.lat_ms + (self.cfg.exit_steps_s[-1] + self.cfg.exch_settle_s) * 1000) * 1e6),
                self._dx_score, tr)

    def _entry_block(self, tr, t):
        eng = self.eng
        if self.draining:
            return "draining"
        r = self.risk.check_entry(t.dec_utc, eng.mono_ns)
        if r:
            return r
        if self.recovered is not None and not self.recovered.ok:
            return "recovery_failed"
        if self.rec_until is not None and t.dec_utc < self.rec_until:
            return "recovery_pending"
        if self._slot_busy(t.dec_utc):
            return "position_open"
        if t.dec_utc < self.cooldown_until:
            return "cooldown"
        bk = eng.books.get(t.token)
        r = self.feeds.entry_block(t.slug, bk is not None and bk.valid, eng.mono_ns)
        if r:
            return r
        return self._clock_block(t.dec_utc)

    def _clock_block(self, utc):
        c = self.cfg
        if c.clock_fixed_theta_ms is not None:
            return None
        theta, backup, dis = self.clock.status()
        if theta is None:
            return "clock_no_samples"
        if backup is None:
            if utc - self.clock_warned > 600:
                self.clock_warned = utc
                self.event(None, "CLOCK_BACKUP_UNAVAILABLE", reason="log only", utc=utc)
            return None
        if dis > c.clock_max_disagreement_ms:
            return "clock_disagree"
        return None

    def _theta(self):
        if self.cfg.clock_fixed_theta_ms is not None:
            return self.cfg.clock_fixed_theta_ms
        return self.clock.theta()

    def _slot_busy(self, utc):
        s = self.slot
        if s is None:
            return False
        if s.state in OPEN:
            return True
        if s.state in (S.STUCK, S.CLOSED) and s.held > EPS and utc < s.t.meta.end:
            return True                  # held to resolution: the slot frees at the window end
        return False

    # ------------------------------------------------------------------ entry arrival
    def _entry_arrive(self, due, tr):
        if tr.state is not S.ENTRY_PENDING:
            return
        eng, t = self.eng, tr.t
        res = buy_fill(eng.books.get(t.token), tr.intent.price_t, float(tr.order_size), t.meta.fee_rate, t.meta.fee_exp)
        self._entry_apply(tr, res, due, self.utc(due))

    def _entry_apply(self, tr, res, due, now):
        """Book the entry result (simulated here, the exchange's real result in LiveLayer) and continue the state machine."""
        t = tr.t
        if res.status == "missed":
            tr.entry_status, tr.entry_miss = "missed", res.reason
            self.counts["entry_missed"] += 1
            self.miss_reasons[res.reason] = self.miss_reasons.get(res.reason, 0) + 1
            self.go(tr, S.MISSED, res.reason, utc=now)
            self.go(tr, S.IDLE, "slot free", utc=now)
            self.cooldown_until = now + self.cfg.cooldown_after_miss_s
            self.slot = None
            return
        tr.shares, tr.cost, tr.fee, tr.avg = res.shares, res.cost, res.fee, res.avg
        tr.entry_status, tr.fill_due, tr.fill_utc = res.status, due, now
        self.counts["entry_filled" if res.status == "filled" else "entry_partial"] += 1
        meta = self._meta_for(t)
        floor_px = max(O.tick_of(meta.tick_t), O.D(res.avg, 6) - O.D(self.cfg.exit_floor_pts) / 100)
        tr.floor_t = int(O.round_to_tick(floor_px, O.tick_of(meta.tick_t), "up") * TICK)
        self.event(tr, "ENTRY_FILL", reason=res.status, utc=now, side=t.side, shares=res.shares, price=res.avg,
                   detail={"fee": res.fee, "cost": res.cost, "order_shares": float(tr.order_size), "floor_price": tr.floor_t / TICK})
        self.go(tr, S.FILLED if res.status == "filled" else S.PARTIAL, "partial fill kept" if res.status == "partial" else "", utc=now)
        if O.shares_down(res.shares) < O.D(meta.min_size):
            self._dust(tr, now, "entry fill below the minimum order size")
            return
        self.at(due + int(self.cfg.exit_steps_s[0] * NS), self._exit_decide, tr, 1, tr.gen)

    # ------------------------------------------------------------------ exits
    def _exit_decide(self, due, tr, k, gen, why=""):
        if gen != tr.gen or tr.state not in (S.FILLED, S.PARTIAL, S.RETRYING):
            return
        eng, t = self.eng, tr.t
        now = self.utc(due)
        left = t.meta.end - now
        if left <= 0:
            self._stuck(tr, now, "window_ended")
            return
        if k > 1 and left < self.cfg.stuck_time_left_s and not why:
            self._stuck(tr, now, f"{left:.1f}s left < {self.cfg.stuck_time_left_s:g}s")
            return
        bk = eng.books.get(t.token)
        blk = self.feeds.exit_block(t.slug, bk is not None and bk.valid)
        if blk:
            tr.next_attempt, tr.waiting = k, True
            if tr.state is not S.RETRYING:
                self.go(tr, S.RETRYING, f"exit waits: {blk}", utc=now)
            return
        meta = self._meta_for(t)
        intent = O.build_exit(meta, "Up" if t.side == "up" else "Down", t.token, tr.held, O.D(tr.floor_t) / TICK, tr.id, k,
                              f"u5m-{self.session}-{tr.id}-X{k}")
        if isinstance(intent, O.Reject):
            self._dust(tr, now, "remaining shares below the minimum order size")
            return
        self._exit_submit(tr, k, intent, due, now, why)

    def _exit_submit(self, tr, k, intent, due, now, why):
        """The exit order leaves here. Dry run: log the intent and schedule the simulated arrival. (LiveLayer overrides: sign + send.)"""
        eng, t = self.eng, tr.t
        lat = eng.lat.median()
        send_utc = now + self.cfg.send_delay_ms / 1000.0
        self.out.order(intent.payload(self.mode.value, iso_utc(now), iso_utc(send_utc), iso_utc(send_utc + lat / 1000.0),
                                      iso_utc(t.start), iso_utc(t.meta.end), int(send_utc * 1000)))
        self.counts["exit_sent"] += 1
        st = self.step_stats.setdefault(k, {"sent": 0, "full": 0, "partial": 0, "none": 0})
        st["sent"] += 1
        tr.waiting, tr.next_attempt = False, k
        self.event(tr, "EXIT_SENT", reason=intent.client_order_id + (f" ({why})" if why else ""), utc=now, side=t.side,
                   shares=float(intent.size), price=float(intent.price))
        self.go(tr, S.EXIT_PENDING, f"attempt {k}" + (f" ({why})" if why else ""), utc=now)
        self.at(due + int((self.cfg.send_delay_ms + lat) * 1e6), self._exit_arrive, tr, k, tr.gen, intent)

    def _exit_arrive(self, due, tr, k, gen, intent):
        if gen != tr.gen or tr.state is not S.EXIT_PENDING:
            return
        eng, t = self.eng, tr.t
        now = self.utc(due)
        if now >= t.meta.end:
            res = Rec(status="missed", reason="window_ended", shares=0.0, proceeds=0.0, fee=0.0, avg=None, floor_hit=False)
        else:
            res = sell_fill(eng.books.get(t.token), intent.price_t, float(intent.size), t.meta.fee_rate, t.meta.fee_exp)
        self._exit_apply(tr, k, res, due, now)

    def _exit_apply(self, tr, k, res, due, now):
        """Book the exit result (simulated here, the exchange's real result in LiveLayer) and continue the state machine."""
        t = tr.t
        st = self.step_stats[k]
        if res.shares > 0:
            tr.sold += res.shares
            tr.proceeds += res.proceeds
            tr.sell_fee += res.fee
        tr.floor_hit = tr.floor_hit or res.floor_hit
        tr.attempts.append({"attempt": k, "step_s": self.cfg.exit_steps_s[min(k, len(self.cfg.exit_steps_s)) - 1], "status": res.status,
                            "shares": res.shares, "avg": res.avg, "fee": res.fee, "floor_hit": res.floor_hit,
                            "reason": getattr(res, "reason", "")})
        st["full" if res.status == "filled" else "partial" if res.status == "partial" else "none"] += 1
        self.event(tr, "EXIT_FILL" if res.shares > 0 else "EXIT_MISS", reason=res.status + (":" + res.reason if getattr(res, "reason", "") else "")
                   + (" floor_hit" if res.floor_hit else ""), utc=now, side=t.side, shares=res.shares, price=res.avg,
                   detail={"fee": res.fee, "proceeds": res.proceeds})
        remaining = tr.held
        if remaining <= 1e-6:
            tr.shares = tr.sold                        # fully out
            self.counts["closed"] += 1
            self.go(tr, S.CLOSED, "fully exited", utc=now)
            self.cooldown_until = now + self.cfg.cooldown_after_close_s
            self._settle(tr, now, None)
            return
        if O.shares_down(remaining) < O.D(self._meta_for(t).min_size):
            self._dust(tr, now, "remainder below the minimum order size")
            return
        left = t.meta.end - now
        if k >= len(self.cfg.exit_steps_s):
            self._stuck(tr, now, "retries exhausted")
        elif left < self.cfg.stuck_time_left_s:
            self._stuck(tr, now, f"{left:.1f}s left < {self.cfg.stuck_time_left_s:g}s")
        else:
            self.go(tr, S.RETRYING, f"{res.status}: {remaining:.2f} shares left", utc=now)
            planned = tr.fill_due + int(self.cfg.exit_steps_s[k] * NS)
            tr.next_attempt = k + 1
            self.at(max(planned, due), self._exit_decide, tr, k + 1, tr.gen)

    def _stuck(self, tr, now, why):
        tr.stuck, tr.stuck_reason = True, why
        self.counts["stuck"] += 1
        self.risk.note_stuck(now)
        self.go(tr, S.STUCK, why, utc=now)
        self.event(tr, "STUCK", reason=why, utc=now)

    def _dust(self, tr, now, why):
        tr.dust = True
        self.counts["dust"] += 1
        self.go(tr, S.CLOSED, "DUST_HELD: " + why, utc=now)
        self.event(tr, "DUST_HELD", reason=why, utc=now, shares=tr.held)
        self.cooldown_until = now + self.cfg.cooldown_after_close_s

    def _poll_waiting(self, mono_ns):
        tr = self.slot
        if tr is None or not tr.waiting or tr.state is not S.RETRYING:
            return
        bk = self.eng.books.get(tr.t.token)
        if self.feeds.exit_block(tr.t.slug, bk is not None and bk.valid) is None:
            tr.waiting = False
            tr.gen += 1
            self._exit_decide(mono_ns, tr, tr.next_attempt, tr.gen, "book_recovered")
        elif self.utc(mono_ns) >= tr.t.meta.end:
            tr.waiting = False
            self._stuck(tr, self.utc(mono_ns), "window_ended while the book was invalid")

    def _kill_exit(self, mono_ns):
        tr = self.slot
        if tr is None or tr.kill_pulled or tr.state not in (S.FILLED, S.PARTIAL, S.RETRYING) or tr.waiting:
            return
        if self.risk.kill.active(mono_ns):
            tr.kill_pulled = True
            tr.gen += 1
            self.counts["kill_exits"] += 1
            self.event(tr, "KILL_SWITCH", reason="exit now")
            self._exit_decide(mono_ns, tr, tr.next_attempt, tr.gen, "kill_switch")

    def _timeouts(self, mono_ns):
        tr = self.slot
        if tr is None:
            return
        c = self.cfg
        if tr.state is S.ENTRY_PENDING and mono_ns - tr.since > c.entry_timeout_s * NS:
            tr.gen += 1
            tr.entry_status, tr.entry_miss = "missed", "timeout"
            self.counts["entry_missed"] += 1
            self.miss_reasons["timeout"] = self.miss_reasons.get("timeout", 0) + 1
            self.go(tr, S.MISSED, "entry timeout")
            self.go(tr, S.IDLE, "slot free")
            self.slot = None
        elif tr.state is S.EXIT_PENDING and mono_ns - tr.since > c.exit_timeout_s * NS:
            tr.gen += 1
            self._stuck(tr, self.eng.now_utc, "exit order timeout")

    # ------------------------------------------------------------------ settlement and scoring
    def _net(self, tr, result):
        win = result is not None and ((tr.t.side == "up") == (result == "Up"))
        return tr.proceeds - tr.sell_fee + (tr.held * (1.0 if win else 0.0)) - tr.cost - tr.fee

    def _settle(self, tr, utc, result):
        """Fully exited trades settle at close (nothing left to resolve); held ones when the official result arrives."""
        tr.result = result
        tr.net = self._net(tr, result)
        tr.settled = True
        self.counts["settled"] += 1
        self.risk.note_net(utc, tr.net)
        self.go(tr, S.SETTLED, "net known", utc=utc, net_usd=tr.net)
        if self.slot is tr and tr.held <= EPS:
            self.slot = None

    def _resolution(self, o):
        m = SLUG_RE.match(o.get("slug", ""))
        if not m:
            return
        start = int(m.group(1))
        try:
            prices = [float(x) for x in (json.loads(o["outcomePrices"]) if isinstance(o["outcomePrices"], str) else o["outcomePrices"])]
            outs = json.loads(o["outcomes"]) if isinstance(o["outcomes"], str) else o["outcomes"]
        except (KeyError, ValueError, TypeError):
            return
        if not prices or max(prices) < 0.99:
            return
        result = outs[prices.index(max(prices))]
        now = self.eng.now_utc
        for tr in self.by_window.pop(start, []):
            if tr.entry_status in ("filled", "partial") and not tr.settled:
                if tr.state in OPEN:                       # cannot happen in a healthy run; never leave it dangling
                    self._stuck(tr, now, "window resolved while the order was still pending")
                self._settle(tr, now, result)
            elif tr.settled:
                tr.result = result
            self._finish_row(tr, result)
        if self.slot is not None and self.slot.settled and self.slot.held <= EPS:
            self.slot = None

    def _dx_score(self, due, tr):
        """Retrospective exchange-timestamp scoring of the same order(s) with the corrected clock offset (see exchbook.py)."""
        t = tr.t
        theta = tr.theta_ms = self._theta()
        if theta is None:
            tr.dx = {"half": {"entry": "no_clock"}, "full": {"entry": "no_clock"}}
            return
        events = token_events(list(self.buf.q), t.token, t.slug, theta)
        meta = self._meta_for(t)
        for label, f in (("half", 0.5), ("full", 1.0)):
            tr.dx[label] = self._chain(Replica(events), tr, theta, f, meta)

    def _chain(self, rep, tr, theta, f, meta):
        c, t = self.cfg, tr.t
        lat = tr.lat_ms
        win_end = t.meta.end
        t1 = tr.wall / 1e6 + theta + lat * f
        r = buy_fill(rep.advance(t1), tr.intent.price_t, float(tr.order_size), t.meta.fee_rate, t.meta.fee_exp)
        out = {"entry": r.status, "reason": getattr(r, "reason", ""), "shares": r.shares, "cost": r.cost, "fee": r.fee, "sold": 0.0,
               "proceeds": 0.0, "sell_fee": 0.0, "attempts": 0, "stuck": False}
        if r.status == "missed":
            return out
        tick = O.tick_of(meta.tick_t)
        floor_t = int(O.round_to_tick(max(tick, O.D(r.avg, 6) - O.D(c.exit_floor_pts) / 100), tick, "up") * TICK)
        if O.shares_down(r.shares) < O.D(meta.min_size):
            return out
        for k, step in enumerate(c.exit_steps_s, 1):
            t_arr = t1 + step * 1000.0 + lat * f
            held = r.shares - out["sold"]
            if t_arr / 1000.0 >= win_end:
                out["stuck"] = True
                break
            size = O.shares_down(held)
            if size < O.D(meta.min_size):
                break
            x = sell_fill(rep.advance(t_arr), floor_t, float(size), t.meta.fee_rate, t.meta.fee_exp)
            out["attempts"] += 1
            if x.shares > 0:
                out["sold"] += x.shares
                out["proceeds"] += x.proceeds
                out["sell_fee"] += x.fee
            if r.shares - out["sold"] <= 1e-6:
                break
            if k == len(c.exit_steps_s) or win_end - t_arr / 1000.0 < c.stuck_time_left_s:
                out["stuck"] = True
                break
        return out

    def _dx_net(self, tr, label, result):
        d = tr.dx.get(label)
        if not d or d.get("entry") in ("missed", "no_clock", None) or result not in ("Up", "Down"):
            return None
        win = (tr.t.side == "up") == (result == "Up")
        held = max(d["shares"] - d["sold"], 0.0)
        return d["proceeds"] - d["sell_fee"] + held * (1.0 if win else 0.0) - d["cost"] - d["fee"]

    # ------------------------------------------------------------------ dryrun_vs_paper
    def _finish_row(self, tr, result):
        eng, t = self.eng, tr.t
        r = result if result in ("Up", "Down") else None
        pb = t.base
        fl = t.flips.get(("base", eng.main_exit)) if hasattr(eng, "main_exit") else None
        paper_net = eng._flip_net(t, "base", eng.main_exit, r) if r and pb is not None and pb.status == "filled" else None
        row = {"paper_trade_id": t.id, "decision_time_utc": iso_utc(t.dec_utc), "window_start_utc": iso_utc(t.start), "slug": t.slug,
               "side": t.side, "official_result": result if result else "UNRESOLVED",
               "paper_status": pb.status if pb else "", "paper_miss_reason": getattr(pb, "reason", "") if pb else "",
               "paper_shares": pb.shares if pb else None, "paper_avg_price": pb.avg if pb else None, "paper_entry_fee_usd": pb.fee if pb else None,
               "paper_exit_status": fl.status if fl else "", "paper_exit_shares": fl.sold if fl else None,
               "paper_exit_avg_price": fl.avg if fl else None, "paper_exit_fee_usd": fl.fee if fl else None, "paper_net_usd": paper_net,
               "dr_sent": int(tr.intent is not None), "dr_block_reason": tr.block or "", "dr_entry_status": tr.entry_status,
               "dr_entry_miss_reason": tr.entry_miss, "dr_order_shares": float(tr.order_size) if tr.intent else None,
               "dr_shares": tr.shares if tr.intent else None, "dr_avg_price": tr.avg, "dr_entry_fee_usd": tr.fee if tr.intent else None,
               "dr_exit_attempts": len(tr.attempts),
               "dr_exit_steps": ";".join(f"{a['step_s']:g}s:{a['status']}" for a in tr.attempts),
               "dr_exit_shares": tr.sold if tr.attempts else None,
               "dr_exit_avg_price": (tr.proceeds / tr.sold) if tr.sold > 0 else None, "dr_exit_fee_usd": tr.sell_fee if tr.attempts else None,
               "dr_held_shares": tr.held if tr.intent and tr.entry_status in ("filled", "partial") else None,
               "dr_final_state": tr.state.value if tr.intent else "BLOCKED", "dr_stuck": int(tr.stuck), "dr_floor_hit": int(tr.floor_hit),
               "dr_net_usd": tr.net if (r or tr.net is not None) else None}
        for label in ("half", "full"):
            d = tr.dx.get(label) or {}
            row[f"dx_{label}_entry_status"] = d.get("entry", "")
            row[f"dx_{label}_shares"] = d.get("shares")
            row[f"dx_{label}_attempts"] = d.get("attempts")
            row[f"dx_{label}_held_shares"] = (d["shares"] - d["sold"]) if d.get("shares") else None
            row[f"dx_{label}_net_usd"] = self._dx_net(tr, label, r)
        reasons = self._reasons(tr, row)
        if row["paper_net_usd"] is not None and row["dr_net_usd"] is not None:
            row["net_diff_usd"] = row["dr_net_usd"] - row["paper_net_usd"]
        row["diff_reasons"] = "|".join(reasons)
        self.out.vs_row(row)
        self.rows.append(row)
        self.trades.pop(tr.id, None)

    def _reasons(self, tr, row):
        out = []
        if tr.block:
            out.append(f"risk_block:{tr.block}")
        for n in tr.notes if tr.entry_status in ("filled", "partial") else ():       # sizing / rounding only matter when something filled
            if n.startswith("size_capped_at_limit"):
                out.append("size_capped_at_limit")
            elif n.startswith("price_rounded"):
                out.append("rounding")
        ps, ds = row["paper_status"], tr.entry_status
        if tr.intent is not None:
            if ps == "missed" and ds in ("filled", "partial"):
                out.append("paper_missed_dryrun_filled")
            if ps == "filled" and ds == "missed":
                out.append(f"dryrun_missed:{tr.entry_miss}")
            if ds == "partial":
                out.append("partial_fill_kept")
        if len(tr.attempts) > 1:
            out.append("retry")
        if tr.floor_hit:
            out.append("floor_hit")
        if tr.dust:
            out.append("dust_held_below_min_size")
        if tr.stuck:
            out.append("stuck")
        if row["paper_exit_status"] == "book_invalid" and tr.sold > 0:
            out.append("book_invalid_recovered")
        elif row["paper_exit_status"] in ("window_ended", "no_bids", "partial") and tr.sold > 0:
            out.append(f"paper_exit_{row['paper_exit_status']}")
        if ps == "filled" and ds == "filled" and abs((row["paper_shares"] or 0) - (row["dr_shares"] or 0)) > 0.011 and not out:
            out.append("size_difference")
        return out

    # ------------------------------------------------------------------ status, summary, shutdown
    def status_fields(self):
        eng = self.eng
        tr = self.slot
        pos = "-"
        if tr is not None and tr.held > EPS:
            pos = f"{tr.t.side.upper()} {tr.held:.2f}@{tr.avg:.3f}"
        state = tr.state.value if tr is not None else S.IDLE.value
        self.risk.roll(eng.now_utc)
        return (f"exec {self.mode.value} {state} pos {pos} net ${self.risk.net_today:+.2f} blocks {sum(self.risk.blocks_today.values())} "
                f"stuck {self.risk.stuck_today} stop {'ON' if self.risk.kill.active(eng.mono_ns) else 'off'}")

    def summary(self, final=False):
        from .report import build
        return build(self, final)

    def write_summary(self, final=False):
        text, data = self.summary(final)
        self.out.write_summary(text, data)
        return text

    def finalize(self):
        """Shutdown: unresolved windows are written as UNRESOLVED; anything still open is logged so a restart can see it."""
        now = self.eng.now_utc
        for start in sorted(self.by_window):
            for tr in self.by_window[start]:
                if tr.state in OPEN or (tr.entry_status in ("filled", "partial") and not tr.settled):
                    self.event(tr, "SHUTDOWN_OPEN", reason=f"state {tr.state.value}, held {tr.held:.2f}", utc=now)
                self._finish_row(tr, None)
        self.by_window.clear()
        self.write_summary(final=True)
        self.out.flush()

    def guard(self, name, fn, *a):
        """Run a layer hook; an exception is counted and printed, never raised into the engine. Too many -> the layer stops opening orders."""
        if self.disabled:
            return
        try:
            fn(*a)
        except Exception:
            self.errors += 1
            if self.errors <= 5:
                self.log(f"[exec] error in {name}:\n{traceback.format_exc()}")
            if self.errors >= MAX_ERRORS:
                self.disabled = True
                self.log("[exec] too many errors: layer disabled")

    def idle_tick(self, mono_ns):
        if self.eng is not None and mono_ns - self.eng.mono_ns > 2 * NS:
            self._timeouts(mono_ns)
