"""The LIVE layer = the dry-run layer with the order transport swapped. Everything else is inherited unchanged: decision flow, risk controls, feed /
clock safety, the state machine, the +2 / +5 / +10 s exit ladder with its floor, partial fills kept, DUST_HELD, STUCK, the STOP file, restart accounting.

What changes:
  * `_entry_submit` / `_exit_submit` hand a Job to the sender thread (build + sign + POST; nothing else on that path) instead of simulating the arrival;
  * the exchange's real answer (order response, then the trade history) replaces the simulated fill: `_entry_apply` / `_exit_apply` run on REAL numbers;
  * an order with no answer is UNKNOWN: never re-sent, resolved through the trade history, the slot stays taken meanwhile;
  * every 30 s and at every window roll the account's positions and open orders are compared with the state machine (a mismatch stops entries, alerts,
    and tries to flatten what should not be there);
  * a DryRunLayer TWIN simulates every decision that was really sent (after it was sent), so each trade has a real result and a simulated one;
  * pilot stop rules S1-S3, reject-burst stop, loss / STUCK limits, e-mail alerts, a hard stop on any unhandled exception.
"""
import time
import traceback
from collections import deque
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace as Rec

from ..book import TICK
from ..engine import SLUG_RE
from ..model import fee_per_share
from ..util import iso_utc
from . import orders as O
from .config import Mode
from .layer import NS, DryRunLayer
from .fills import EPS
from .redact import REDACTOR
from .sender import Job
from .state import OPEN, S

NO_FILL_CODES = ("fak_not_filled", "unmatched")
NO_FILL_TEXT = "no orders found to match"


def _is_nofill(res):
    return res.code in NO_FILL_CODES or NO_FILL_TEXT in (res.message or "").lower()


def live_exec_config(cfg, lcfg):
    """The dry-run ExecConfig with the pilot's limits applied (size, entries per hour, loss and STUCK limits)."""
    return replace(cfg, mode="LIVE", size_usd=lcfg.budget_usd, max_entries_per_hour=lcfg.max_entries_per_hour,
                   daily_loss_limit_usd=lcfg.daily_loss_limit_usd, stuck_limit_per_day=lcfg.stuck_limit_per_day)


class LiveLayer(DryRunLayer):
    _MODE = Mode.LIVE

    def __init__(self, cfg, lcfg, out, session, sender, recon, alerts, twin, rules, recovered=None, kill_exists_fn=None, log=print,
                 clock_ns=time.perf_counter_ns, on_fatal=None, runtime_ok=False):
        self.lcfg, self.sender, self.recon, self.alerts, self.twin, self.rules = lcfg, sender, recon, alerts, twin, rules
        self.results = sender.results
        self.clock_ns = clock_ns
        self.on_fatal = on_fatal
        self.runtime_ok = runtime_ok
        self.halted = None
        self.entries_halted = None
        self.mismatch = None
        self.reject_times = deque()
        self.jobs = {}
        self.live_rows = {}
        self.twin_seen = 0
        self._joined = set()
        self.unk = {}                       # trade id -> dict(leg, attempt, since, lookups)
        self.counters = {"rejects": 0, "unknown": 0, "rate_limited": 0, "late_results": 0, "snapshots": 0, "mismatches": 0, "api_errors": 0,
                         "trade_failed": 0, "corrections": 0, "flattens": 0, "signal_too_old": 0, "slow_signs": 0}
        self.lat_rows = []                  # (decision_to_send_ms, sign_ms, send_to_response_ms)
        self.kill_seen = False
        self.loss_alerted = False
        self.last_recon = None
        self.snap_pending = False
        self.clean_snaps = 0
        self.api_fail = 0
        self.last_window = None
        self.fill_mono = {}                 # token -> mono_ns of the last fill (positions lag the fill on-chain)
        self.cash_start = self.cash_now = None
        self.pre_start = None
        self.day = None
        self.flat_seq = 0
        self._t_dec = 0
        self.preflight_text = ""
        sender.on_sent = self._on_sent
        super().__init__(cfg, out, session, Mode.LIVE, recovered=recovered, kill_exists_fn=kill_exists_fn, log=log)

    # ------------------------------------------------------------------ plumbing
    def bind(self, eng):
        super().bind(eng)
        self.twin.bind(eng)

    def _budget_usd(self):
        return self.lcfg.budget_usd

    def before_record(self, mono_ns):
        self._drain(mono_ns)
        super().before_record(mono_ns)
        self.guard("twin.before", self.twin.before_record, mono_ns)

    def after_record(self, feed, wall_ns, mono_ns, exch_ts, obj):
        super().after_record(feed, wall_ns, mono_ns, exch_ts, obj)
        self.guard("twin.after", self.twin.after_record, feed, wall_ns, mono_ns, exch_ts, obj)
        self._housekeeping(mono_ns)

    def idle_tick(self, mono_ns):
        self._drain(mono_ns)
        super().idle_tick(mono_ns)
        if self.eng is not None:
            self._housekeeping(mono_ns)

    def guard(self, name, fn, *a):
        """A hook that raises: log it (redacted) and HARD STOP. In LIVE an unexpected exception is never survivable."""
        if self.disabled:
            return
        try:
            fn(*a)
        except Exception as e:
            self.errors += 1
            self.log(REDACTOR.scrub(f"[live] unhandled exception in {name}:\n{traceback.format_exc()}"))
            try:
                self.halt(f"unhandled exception in {name}: {type(e).__name__}", alert_kind="crash")
            except Exception:                                      # pragma: no cover
                self.disabled = True

    def halt(self, reason, alert_kind="halt"):
        """No new entries, flatten what we hold, alert, and ask the runtime to shut down. Idempotent."""
        if self.halted:
            return
        self.halted = reason
        self.log(f"[live] HALT: {reason}")
        try:
            self.event(None, "HALT", reason=reason)
        except Exception:
            pass
        self.alerts.send(alert_kind, f"HALT: {reason}", f"{reason}\nsession {self.session}\n{self.status_fields()}")
        tr = self.slot
        if tr is not None and tr.state in (S.FILLED, S.PARTIAL, S.RETRYING) and not tr.waiting:
            try:
                tr.gen += 1
                self._exit_decide(self.eng.mono_ns, tr, tr.next_attempt, tr.gen, "halt")
            except Exception:
                pass
        if self.on_fatal is not None:
            self.on_fatal(reason)

    def _alert(self, kind, subject, body=""):
        self.alerts.send(kind, subject, f"{body}\n\nsession {self.session}  {self.status_fields()}")

    # ------------------------------------------------------------------ entry
    def on_decision(self, t, force=False):
        self._t_dec = self.clock_ns()
        super().on_decision(t, force)
        tr = self.trades.get(t.id)
        if tr is not None and tr.intent is not None:                  # only decisions that were really sent get a simulated twin
            self.guard("twin.on_decision", self.twin.on_decision, t, True)

    def _entry_block(self, tr, t):
        if self.halted:
            return "halted"
        if not self.runtime_ok:
            return "preflight_pending"
        if self.rules.stopped or self.rules.exists():
            return "rule_stop"
        if self.entries_halted:
            return self.entries_halted
        if self.mismatch:
            return "reconcile_mismatch"
        if self.api_fail >= 3:
            return "api_unreachable"
        if (self.clock_ns() - self._t_dec) / 1e6 > self.lcfg.max_signal_age_ms:
            return "signal_stale"
        return super()._entry_block(tr, t)

    def _entry_submit(self, tr, t, intent):
        job = Job(key=tr.id, leg="ENTRY", attempt=1, side="BUY", token=t.token, usd=intent.usd, shares=intent.size, price=intent.price,
                  client_oid=intent.client_order_id, t_decision_ns=self._t_dec, max_age_ms=self.lcfg.max_signal_age_ms,
                  meta={"slug": t.slug, "side": t.side, "limit": str(intent.price), "tick": str(O.tick_of(intent.meta.tick_t))})
        self.jobs[(tr.id, "ENTRY", 1)] = job
        self.slot = tr
        self.go(tr, S.ENTRY_PENDING, "order sent", utc=t.dec_utc)
        self.sender.submit(job)                                       # <-- the order leaves: everything below is bookkeeping
        self.risk.note_sent(t.dec_utc)
        self.rules.note_sent()
        self.counts["entry_sent"] += 1
        self.event(tr, "ENTRY_SENT", reason=intent.client_order_id, utc=t.dec_utc, side=t.side, shares=float(intent.size), price=float(intent.price))

    def _exit_submit(self, tr, k, intent, due, now, why):
        t = tr.t
        job = Job(key=tr.id, leg="EXIT", attempt=k, side="SELL", token=t.token, shares=intent.size, price=intent.price,
                  client_oid=intent.client_order_id, t_decision_ns=self.clock_ns(), max_age_ms=1e12,
                  meta={"slug": t.slug, "side": t.side, "floor": str(intent.price)})
        self.jobs[(tr.id, "EXIT", k)] = job
        tr.waiting, tr.next_attempt = False, k
        self.go(tr, S.EXIT_PENDING, f"attempt {k}" + (f" ({why})" if why else ""), utc=now)
        self.sender.submit(job)
        self.counts["exit_sent"] += 1
        st = self.step_stats.setdefault(k, {"sent": 0, "full": 0, "partial": 0, "none": 0})
        st["sent"] += 1
        self.event(tr, "EXIT_SENT", reason=intent.client_order_id + (f" ({why})" if why else ""), utc=now, side=t.side,
                   shares=float(intent.size), price=float(intent.price))

    def _on_sent(self, job, res):
        """Sender thread, right after the POST returned: queue the 'what was sent' record (no disk here)."""
        self.out.sent({"time_utc": iso_utc(time.time()), "client_order_id": job.client_oid, "trade_id": job.key, "leg": job.leg, "attempt": job.attempt,
                       "params": res.params or {"side": job.side, "asset_id": job.token}, "posted": res.sent, "ok": res.ok, "kind": res.kind,
                       "order_id": res.order_id, "status": res.status, "making": str(res.making), "taking": str(res.taking), "code": res.code,
                       "sign_ms": round((res.t_built_ns - res.t_build_ns) / 1e6, 3) if res.t_build_ns else None,
                       "message": REDACTOR.scrub(res.message or "")})

    # ------------------------------------------------------------------ results from the sender and the reconciler
    def _drain(self, mono_ns):
        while True:
            try:
                job, res = self.results.get_nowait()
            except Exception:
                break
            self._on_result(job, res, mono_ns)
        while True:
            try:
                msg = self.recon.inbox.get_nowait()
            except Exception:
                break
            self._on_recon(msg, mono_ns)

    def _lat_row(self, job, res, tr, result):
        d, ms = job.t_decision_ns, lambda a, b: round((b - a) / 1e6, 3)
        if res.t_build_ns:
            build_ms = ms(res.t_build_ns, res.t_built_ns)
            self.lat_rows.append((ms(d, res.t_send_ns), build_ms, ms(res.t_send_ns, res.t_resp_ns)))
            if build_ms > self.lcfg.build_alert_ms:                  # after the POST returned: never on the decision path
                self.counters["slow_signs"] += 1
                why = (f"{job.leg} attempt {job.attempt} token {job.token[:12]}..: build + sign took {build_ms:.1f} ms "
                       f"(limit {self.lcfg.build_alert_ms:g} ms); the market's metadata was probably not warm")
                self.event(tr, "SLOW_SIGN", reason=why)
                self._alert("slow_sign", f"slow order signing: {build_ms:.0f} ms", why)
        self.out.latency({"trade_id": job.key, "leg": job.leg, "attempt": job.attempt, "decision_utc": iso_utc(tr.t.dec_utc) if tr else "",
                          "queue_ms": ms(job.t_queued_ns, job.t_picked_ns), "decision_to_sign_start_ms": ms(d, res.t_build_ns),
                          "sign_ms": ms(res.t_build_ns, res.t_built_ns), "sign_to_send_ms": ms(res.t_built_ns, res.t_send_ns),
                          "send_to_response_ms": ms(res.t_send_ns, res.t_resp_ns), "decision_to_send_ms": ms(d, res.t_send_ns),
                          "decision_to_response_ms": ms(d, res.t_resp_ns), "result": result})

    def _note_reject(self, tr, res, mono_ns):
        self.counters["rejects"] += 1
        now = self.eng.now_utc
        self.reject_times.append(now)
        while self.reject_times and self.reject_times[0] < now - self.lcfg.reject_burst_window_s:
            self.reject_times.popleft()
        self.event(tr, "REJECT", reason=f"{res.code}: {res.message}"[:200])
        if len(self.reject_times) >= self.lcfg.reject_burst_n and not self.entries_halted:
            self.entries_halted = "reject_burst"
            self._alert("reject_burst", f"{len(self.reject_times)} rejects in {self.lcfg.reject_burst_window_s / 60:g} min: entries stopped",
                        f"last reject: {res.code}: {res.message}")

    def _on_result(self, job, res, mono_ns):
        tr = self.trades.get(job.key) if job.key >= 0 else None
        if job.key < 0:                                              # a flatten order: log only
            self.event(None, "FLATTEN_RESULT", reason=f"{res.kind or 'ok'} {res.code} {res.status} sold {res.making}", utc=self.eng.now_utc)
            self.counters["flattens"] += 1
            return
        self._lat_row(job, res, tr, "ok" if res.ok else (res.kind or "error"))
        if tr is None:
            self.counters["late_results"] += 1
            self.event(None, "LATE_RESULT", reason=f"trade {job.key} {job.leg} already finished: {res.kind or res.status}")
            if res.ok and (res.taking > 0 or res.making > 0):
                self.halt(f"an order for finished trade {job.key} filled after it was written off", "mismatch")
            return
        if res.kind != "unknown":
            self.unk.pop(tr.id, None)
        if job.leg == "ENTRY":
            self._entry_result(tr, job, res, mono_ns)
        else:
            self._exit_result(tr, job, res, mono_ns)

    def _entry_result(self, tr, job, res, mono_ns):
        now = self.utc(mono_ns)
        if tr.state is not S.ENTRY_PENDING:                          # written off earlier (unknown -> missed) and now it answers
            self.counters["late_results"] += 1
            if res.ok and res.taking > 0:
                self.halt(f"entry of trade {tr.id} filled after it was written off as missed", "mismatch")
            return
        if res.kind == "unknown" or (res.ok and res.status == "delayed" and res.taking <= 0):
            self.counters["unknown"] += 1
            self._mark_unknown(tr, "ENTRY", 1, mono_ns)
            return
        if res.ok and res.status == "live":
            self.halt(f"a FAK order rested on the book (order {res.order_id}); cancel it by hand", "mismatch")
            return
        if not res.ok:
            if res.code == "signal_too_old":
                self.counters["signal_too_old"] += 1
            elif res.kind == "rate_limited":
                self.counters["rate_limited"] += 1
                self._note_reject(tr, res, mono_ns)
            elif not _is_nofill(res):
                self._note_reject(tr, res, mono_ns)
            reason = "no_liquidity_within_limit" if _is_nofill(res) else (res.code or res.kind or "rejected")
            self._entry_apply(tr, Rec(status="missed", reason=reason, shares=0.0, cost=0.0, fee=0.0, avg=None), mono_ns, now)
            return
        shares, cost = float(res.taking), float(res.making)
        if shares <= EPS:
            self._entry_apply(tr, Rec(status="missed", reason="no_liquidity_within_limit", shares=0.0, cost=0.0, fee=0.0, avg=None), mono_ns, now)
            return
        avg = cost / shares
        t = tr.t
        fee = fee_per_share(avg, t.meta.fee_rate, t.meta.fee_exp) * shares
        status = "filled" if shares >= float(tr.order_size) - 0.015 else "partial"
        self.fill_mono[t.token] = mono_ns
        self.rules.note_filled()
        self.out.fill({"time_utc": iso_utc(now), "trade_id": tr.id, "leg": "ENTRY", "attempt": 1, "order_id": res.order_id, "source": "response",
                       "order_status": res.status, "shares": shares, "avg_price": avg, "usd": cost, "fee_usd": fee,
                       "fee_source": "formula(avg price; replaced by the trade history)"})
        self._entry_apply(tr, Rec(status=status, reason="", shares=shares, cost=cost, fee=fee, avg=avg), mono_ns, now)
        tr.order_id = res.order_id
        self.recon.settle(tr.id, "ENTRY", 1, res.order_id, t.token)
        if cost > self.lcfg.position_alert_usd:
            self._alert("big_position", f"position ${cost:.2f} > ${self.lcfg.position_alert_usd:g}", f"trade {tr.id}")

    def _exit_result(self, tr, job, res, mono_ns):
        k, now, t = job.attempt, self.utc(mono_ns), tr.t
        if tr.state is not S.EXIT_PENDING:
            self.counters["late_results"] += 1
            if res.ok and res.making > 0:
                self.halt(f"exit of trade {tr.id} filled after it was written off", "mismatch")
            return
        if res.kind == "unknown" or (res.ok and res.status == "delayed" and res.making <= 0):
            self.counters["unknown"] += 1
            self._mark_unknown(tr, "EXIT", k, mono_ns)
            return
        if res.ok and res.status == "live":
            self.halt(f"a FAK sell rested on the book (order {res.order_id}); cancel it by hand", "mismatch")
            return
        if not res.ok:
            if not _is_nofill(res):
                self._note_reject(tr, res, mono_ns)
            rec = Rec(status="missed", reason="no_bids_above_floor" if _is_nofill(res) else (res.code or res.kind or "rejected"), shares=0.0,
                      proceeds=0.0, fee=0.0, avg=None, floor_hit=_is_nofill(res))
            self._exit_apply(tr, k, rec, mono_ns, now)
            return
        shares, proceeds = float(res.making), float(res.taking)
        if shares <= EPS:
            self._exit_apply(tr, k, Rec(status="missed", reason="no_bids_above_floor", shares=0.0, proceeds=0.0, fee=0.0, avg=None, floor_hit=True),
                             mono_ns, now)
            return
        avg = proceeds / shares
        fee = fee_per_share(avg, t.meta.fee_rate, t.meta.fee_exp) * shares
        full = shares >= float(job.shares) - 0.015
        self.out.fill({"time_utc": iso_utc(now), "trade_id": tr.id, "leg": "EXIT", "attempt": k, "order_id": res.order_id, "source": "response",
                       "order_status": res.status, "shares": shares, "avg_price": avg, "usd": proceeds, "fee_usd": fee,
                       "fee_source": "formula(avg price; replaced by the trade history)", "floor_hit": int(not full)})
        self._exit_apply(tr, k, Rec(status="filled" if full else "partial", reason="", shares=shares, proceeds=proceeds, fee=fee, avg=avg,
                                    floor_hit=not full), mono_ns, now)
        self.recon.settle(tr.id, "EXIT", k, res.order_id, t.token)

    # ------------------------------------------------------------------ UNKNOWN orders
    def _mark_unknown(self, tr, leg, attempt, mono_ns):
        if tr.id in self.unk:
            return
        self.unk[tr.id] = {"leg": leg, "attempt": attempt, "since": mono_ns, "lookups": 0}
        self.event(tr, "UNKNOWN", reason=f"{leg} attempt {attempt}: no usable answer; looking it up in the trade history, never re-sending")
        self._alert("unknown_order", f"order with no answer (trade {tr.id}, {leg})", "Resolving through the trade history; not re-sent.")
        self._lookup(tr, leg, attempt)

    def _lookup(self, tr, leg, attempt):
        self.unk[tr.id]["lookups"] += 1
        self.recon.lookup(tr.id, leg, attempt, tr.t.token, int(tr.wall / 1e6) - 2000)

    def _timeouts(self, mono_ns):
        tr = self.slot
        if tr is None:
            return
        lc = self.lcfg
        u = self.unk.get(tr.id)
        if u is None:
            if tr.state in (S.ENTRY_PENDING, S.EXIT_PENDING) and mono_ns - tr.since > lc.unknown_resolve_s * NS:
                self._mark_unknown(tr, "ENTRY" if tr.state is S.ENTRY_PENDING else "EXIT", 1 if tr.state is S.ENTRY_PENDING else tr.next_attempt,
                                   mono_ns)
            return
        if mono_ns - u["since"] > u["lookups"] * lc.unknown_resolve_s * NS and u["lookups"] < 4:
            self._lookup(tr, u["leg"], u["attempt"])

    def _on_lookup(self, key, leg, attempt, trades, mono_ns):
        tr = self.trades.get(key)
        u = self.unk.get(key)
        if tr is None or u is None or u["leg"] != leg or u["attempt"] != attempt:
            return
        now = self.utc(mono_ns)
        side = "BUY" if leg == "ENTRY" else "SELL"
        mine = [t for t in trades if t.side == side]
        if mine:
            size = float(sum(t.size for t in mine))
            usd = float(sum(t.size * t.price for t in mine))
            avg = usd / size
            fee = sum(float(t.size) * fee_per_share(float(t.price), tr.t.meta.fee_rate, tr.t.meta.fee_exp) for t in mine)
            self.unk.pop(key, None)
            self.event(tr, "UNKNOWN_RESOLVED", reason=f"{leg}: found {len(mine)} trade(s), {size:.4f} shares @ {avg:.4f}")
            if leg == "ENTRY":
                st = "filled" if size >= float(tr.order_size) - 0.015 else "partial"
                self._entry_apply(tr, Rec(status=st, reason="", shares=size, cost=usd, fee=fee, avg=avg), mono_ns, now)
                self.rules.note_filled()
            else:
                full = size >= float(tr.held) - 0.015
                self._exit_apply(tr, attempt, Rec(status="filled" if full else "partial", reason="", shares=size, proceeds=usd, fee=fee, avg=avg,
                                                  floor_hit=not full), mono_ns, now)
            return
        if u["lookups"] >= 4 and mono_ns - u["since"] > 3 * self.lcfg.unknown_resolve_s * NS:
            self.unk.pop(key, None)
            self.event(tr, "UNKNOWN_WRITTEN_OFF", reason=f"{leg}: nothing in the trade history after {u['lookups']} lookups")
            if leg == "ENTRY":
                self._entry_apply(tr, Rec(status="missed", reason="unknown_no_fill", shares=0.0, cost=0.0, fee=0.0, avg=None), mono_ns, now)
            else:
                self._exit_apply(tr, attempt, Rec(status="missed", reason="unknown_no_fill", shares=0.0, proceeds=0.0, fee=0.0, avg=None,
                                                  floor_hit=False), mono_ns, now)

    # ------------------------------------------------------------------ corrections from the real trade history
    def _on_recon(self, msg, mono_ns):
        kind = msg[0]
        if kind == "trades":
            self._on_trades(*msg[1:], mono_ns)
        elif kind == "lookup":
            self._on_lookup(*msg[1:], mono_ns)
        elif kind == "snapshot":
            self._on_snapshot(msg[1], msg[2], msg[3], mono_ns)
        elif kind == "cash":
            self.cash_now = msg[1]
            if self.cash_start is None:
                self.cash_start = msg[1]

    def _on_trades(self, key, leg, attempt, trades, final, mono_ns):
        tr = self.trades.get(key)
        if tr is None or not trades:
            return
        t = tr.t
        size = float(sum(x.size for x in trades))
        usd = float(sum(x.size * x.price for x in trades))
        fee = sum(float(x.size) * fee_per_share(float(x.price), t.meta.fee_rate, t.meta.fee_exp) for x in trades)
        failed = [x for x in trades if x.status == "FAILED"]
        self.out.fill({"time_utc": iso_utc(self.utc(mono_ns)), "trade_id": key, "leg": leg, "attempt": attempt, "order_id": trades[0].order_id,
                       "source": "trades", "trade_ids": "|".join(x.id for x in trades), "trade_status": "|".join(sorted({x.status for x in trades})),
                       "shares": size, "avg_price": usd / size if size else None, "usd": usd, "fee_usd": fee,
                       "fee_source": "formula(trade price, market fee schedule)", "note": "final" if final else "interim",
                       "exchange_matched_utc": iso_utc(max(x.matched_at_ms for x in trades) / 1000.0), "local_seen_utc": iso_utc(time.time())})
        if failed:
            self.counters["trade_failed"] += 1
            self._alert("trade_failed", f"trade FAILED on-chain (trade {key} {leg})", f"trade ids {[x.id for x in failed]}")
            self.mismatch = self.mismatch or f"trade {key} {leg} FAILED"
            return
        if leg == "ENTRY" and tr.entry_status in ("filled", "partial"):
            if abs(size - tr.shares) > self.lcfg.position_tolerance_shares:
                self.mismatch = self.mismatch or f"trade {key}: response said {tr.shares:.4f} shares, trade history {size:.4f}"
                self._alert("reconcile_mismatch", "fill size differs between the order response and the trade history", self.mismatch)
                return
            old_net = tr.net
            d_cost, d_fee = usd - tr.cost, fee - tr.fee
            if abs(d_cost) > 1e-9 or abs(d_fee) > 1e-9:
                self.counters["corrections"] += 1
                tr.cost, tr.fee, tr.avg = usd, fee, usd / size
                self._renet(tr, old_net)
        elif leg == "EXIT":
            att = next((a for a in tr.attempts if a["attempt"] == attempt and a["shares"] > 0), None)
            if att is None or abs(size - att["shares"]) > self.lcfg.position_tolerance_shares:
                return
            old_net = tr.net
            old_p = att.get("proceeds", att["shares"] * (att["avg"] or 0.0))
            d_p, d_f = usd - old_p, fee - att["fee"]
            if abs(d_p) > 1e-9 or abs(d_f) > 1e-9:
                self.counters["corrections"] += 1
                tr.proceeds += d_p
                tr.sell_fee += d_f
                att.update(avg=usd / size, fee=fee, proceeds=usd)
                self._renet(tr, old_net)

    def _renet(self, tr, old_net):
        if tr.settled and old_net is not None:
            tr.net = self._net(tr, tr.result)
            self.risk.note_net(self.eng.now_utc, tr.net - old_net)

    # ------------------------------------------------------------------ settle / alerts on the way
    def _settle(self, tr, utc, result):
        super()._settle(tr, utc, result)
        if self.risk.net_today <= -self.lcfg.daily_loss_limit_usd and not self.loss_alerted:
            self.loss_alerted = True
            self._alert("loss_limit", f"daily loss limit reached: ${self.risk.net_today:+.2f}", "Entries are stopped for the rest of the UTC day.")

    def _stuck(self, tr, now, why):
        super()._stuck(tr, now, why)
        self._alert("stuck", f"STUCK position (trade {tr.id}): {why}", f"held {tr.held:.2f} shares of {tr.t.slug} {tr.t.side}")

    def _kill_exit(self, mono_ns):
        active = self.risk.kill.active(mono_ns)
        if active and not self.kill_seen:
            self.kill_seen = True
            self._alert("kill_switch", "STOP file found: entries stopped, exiting any position", "")
        elif not active:
            self.kill_seen = False
        super()._kill_exit(mono_ns)

    def _dx_score(self, due, tr):
        pass                                                         # the simulated twin does the retrospective scoring

    # ------------------------------------------------------------------ rows: real vs paper, real vs twin
    def _finish_row(self, tr, result):
        super()._finish_row(tr, result)
        row = self.rows[-1]
        self.live_rows[row["paper_trade_id"]] = row
        self._try_join()

    def _try_join(self):
        twin_by_id = {r["paper_trade_id"]: r for r in self.twin.rows}
        for tid, lr in list(self.live_rows.items()):
            tw = twin_by_id.get(tid)
            if tw is None or tid in self._joined:
                continue
            self._joined.add(tid)
            self._write_twin_row(lr, tw)

    def _write_twin_row(self, lr, tw):
        causes = []
        if lr["dr_entry_status"] != tw["dr_entry_status"]:
            causes.append(f"entry:{lr['dr_entry_status'] or 'blocked'}!={tw['dr_entry_status']}")
        if lr["dr_avg_price"] is not None and tw["dr_avg_price"] is not None and lr["dr_avg_price"] - tw["dr_avg_price"] > 0.004:
            causes.append("live_fill_price_worse")
        if (lr["dr_shares"] or 0) and (tw["dr_shares"] or 0) and abs(lr["dr_shares"] - tw["dr_shares"]) > 0.011:
            causes.append("size_differs")
        if lr["dr_exit_attempts"] != tw["dr_exit_attempts"]:
            causes.append(f"exit_attempts:{lr['dr_exit_attempts']}vs{tw['dr_exit_attempts']}")
        if lr["dr_stuck"] != tw["dr_stuck"]:
            causes.append("stuck_differs")
        d = None
        if lr["dr_net_usd"] is not None and tw["dr_net_usd"] is not None:
            d = lr["dr_net_usd"] - tw["dr_net_usd"]
        row = {"paper_trade_id": lr["paper_trade_id"], "decision_time_utc": lr["decision_time_utc"], "slug": lr["slug"], "side": lr["side"],
               "official_result": lr["official_result"], "net_diff_usd": d, "diff_causes": "|".join(causes)}
        for src, pre, row_ in (("lr", "live", lr), ("tw", "twin", tw)):
            for a, b in (("entry_status", "dr_entry_status"), ("shares", "dr_shares"), ("avg_price", "dr_avg_price"),
                         ("entry_fee_usd", "dr_entry_fee_usd"), ("exit_steps", "dr_exit_steps"), ("exit_shares", "dr_exit_shares"),
                         ("exit_avg_price", "dr_exit_avg_price"), ("exit_fee_usd", "dr_exit_fee_usd"), ("held_shares", "dr_held_shares"),
                         ("final_state", "dr_final_state"), ("stuck", "dr_stuck"), ("net_usd", "dr_net_usd")):
                row[f"{pre}_{a}"] = row_[b]
        self.out.twin_row(row)
        if lr["dr_entry_status"] in ("filled", "partial") and lr["dr_net_usd"] is not None:
            self.rules.note_trade(lr["dr_net_usd"], tw["dr_net_usd"])

    # ------------------------------------------------------------------ reconciliation of positions / orders
    def _housekeeping(self, mono_ns):
        eng = self.eng
        if eng is None:
            return
        self.risk.roll(eng.now_utc)
        if self.day is None:
            self.day = self.risk.day
        elif self.risk.day != self.day:
            self._alert("daily_summary", f"end-of-day summary {self.day}", self.summary()[0])
            self.day, self.loss_alerted = self.risk.day, False
        if eng.cur != self.last_window or (self.last_recon is None or mono_ns - self.last_recon >= self.lcfg.reconcile_every_s * NS):
            if not self.snap_pending and self.runtime_ok:
                self.snap_pending = True
                self.last_recon = mono_ns
                self.last_window = eng.cur
                self.recon.snapshot()
        self._try_join()

    def _expected_positions(self):
        exp = {}
        for lst in self.by_window.values():
            for tr in lst:
                if tr.entry_status in ("filled", "partial") and tr.held > EPS:
                    exp[tr.t.token] = exp.get(tr.t.token, 0.0) + tr.held
        return exp

    def _busy_tokens(self, mono_ns):
        """Tokens whose position may legitimately differ for a moment: an order is in flight, or the last fill is younger than 45 s."""
        busy = {tr.t.token for lst in self.by_window.values() for tr in lst if tr.state in (S.ENTRY_PENDING, S.EXIT_PENDING)}
        busy |= {tok for tok, m in self.fill_mono.items() if mono_ns - m < 45 * NS}
        return busy

    def _on_snapshot(self, positions, orders, err, mono_ns):
        self.snap_pending = False
        self.counters["snapshots"] += 1
        if err:
            self.api_fail += 1
            self.counters["api_errors"] += 1
            if self.api_fail == 3:
                self._alert("api_unreachable", "account queries failing: entries stopped", err)
            return
        self.api_fail = 0
        busy = self._busy_tokens(mono_ns)
        exp = self._expected_positions()
        act = {p["asset_id"]: p["size"] for p in positions if not self._old_window(p.get("slug"))}
        exp = {k: v for k, v in exp.items() if not self._old_window(self.tok_slug.get(k))}
        problems = []
        for tok in set(exp) | set(act):
            if tok in busy:
                continue
            if abs(act.get(tok, 0.0) - exp.get(tok, 0.0)) > self.lcfg.position_tolerance_shares + 0.02:
                problems.append((tok, act.get(tok, 0.0), exp.get(tok, 0.0)))
        if orders:
            problems.append(("open_orders", len(orders), 0))
        if problems:
            self.clean_snaps = 0
            self.counters["mismatches"] += 1
            txt = "; ".join(f"{p[0][:10]}.. exchange {p[1]:.4f} vs state {p[2]:.4f}" if p[0] != "open_orders" else f"{p[1]} open order(s)" for p in problems)
            if not self.mismatch:
                self.event(None, "RECONCILE_MISMATCH", reason=txt)
                self._alert("reconcile_mismatch", "positions / orders differ from the state machine: entries stopped", txt)
            self.mismatch = txt
            for tok, a, e in problems:
                if tok != "open_orders" and a > e + self.lcfg.position_tolerance_shares and self.slot is None:
                    self._flatten(tok, a - e)
        else:
            self.clean_snaps += 1
            if self.mismatch and self.clean_snaps >= 2:
                self.event(None, "RECONCILE_CLEARED", reason=f"two clean snapshots in a row (was: {self.mismatch})")
                self._alert("reconcile_cleared", "reconciliation clean again: entries allowed", self.mismatch)
                self.mismatch = None

    def _old_window(self, slug):
        """A position of a window older than the previous one is resolved (or being redeemed): the exchange's view of it is not comparable."""
        m = SLUG_RE.match(slug or "")
        return bool(m) and self.eng.cur is not None and int(m.group(1)) < self.eng.cur - 300

    def _entry_apply(self, tr, res, due, now):
        super()._entry_apply(tr, res, due, now)
        self.rules.note_result()

    def _flatten(self, token, shares):
        """Sell what the account holds beyond the state machine's expectation (floor = best bid - exit floor pts). One try per snapshot."""
        bk = self.eng.books.get(token)
        b = bk.best_bid() if bk is not None and bk.valid else None
        if b is None:
            return
        slug = self.tok_slug.get(token)
        meta = self.ometa.get(slug) or O.OrderMeta(slug or "?", token, "", tick_t=100)
        floor = max(O.tick_of(meta.tick_t), O.D(b[0] / TICK, 6) - O.D(self.cfg.exit_floor_pts) / 100)
        self.flat_seq += 1
        intent = O.build_exit(meta, "?", token, shares, floor, -self.flat_seq, 1, f"u5m-{self.session}-F{self.flat_seq}")
        if isinstance(intent, O.Reject):
            self.event(None, "FLATTEN_SKIPPED", reason=f"{intent.reason}: {shares:.4f} shares")
            return
        job = Job(key=-self.flat_seq, leg="FLATTEN", attempt=1, side="SELL", token=token, shares=intent.size, price=intent.price,
                  client_oid=intent.client_order_id, t_decision_ns=self.clock_ns(), max_age_ms=1e12)
        self.event(None, "FLATTEN_SENT", reason=f"{intent.size} shares of {token[:10]}.. at floor {intent.price}")
        self.sender.submit(job)

    # ------------------------------------------------------------------ pre-flight (runtime stage), stop rules
    def runtime_preflight(self, mono_ns):
        """Checks that need live data. -> (done, problems). Entries stay blocked ('preflight_pending') until this passes."""
        eng, c = self.eng, self.lcfg
        bad = []
        theta, backup, dis = self.clock.status()
        if theta is None:
            bad.append("clock_offset_v2: not enough warm samples yet")
        else:
            if abs(theta) >= c.clock_max_abs_offset_ms:
                bad.append(f"clock offset {theta:+.1f} ms is not within +-{c.clock_max_abs_offset_ms:g} ms")
            if backup is not None and dis > self.cfg.clock_max_disagreement_ms:
                bad.append(f"clock hosts disagree by {dis:.1f} ms")
        if self.feeds.down:
            bad.append(f"feeds down: {sorted(self.feeds.down)}")
        meta = eng.markets.get(eng.cur)
        if meta is None or self.feeds.pm.get(meta.slug) is None or self.feeds.pm[meta.slug]["down"]:
            bad.append("current Polymarket market not connected")
        if len(eng.lat.vals) < eng.lat.min or eng.lat.median() > c.latency_max_ms:
            bad.append(f"latency ping not healthy (samples {len(eng.lat.vals)}, median {eng.lat.median():.0f} ms)")
        if self.risk.kill.active(mono_ns):
            bad.append("STOP file exists")
        if self.rules.exists():
            bad.append("RULE_STOP file exists (delete it by hand after reading it)")
        return not bad, bad

    def on_rule_stop(self, rule, why):
        self.event(None, "RULE_STOP", reason=f"{rule}: {why}")
        self._alert("rule_stop", f"PILOT STOPPED by rule {rule}", f"{why}\n\n{self.summary()[0]}")
        self.log(f"[live] pilot stopped by rule {rule}: {why}")

    # ------------------------------------------------------------------ summary and shutdown
    def summary(self, final=False):
        from .livereport import build_live
        return build_live(self, final)

    def write_summary(self, final=False):
        text, data = self.summary(final)
        self.out.write_summary(text, data)
        return text

    def finalize(self):
        self.guard("twin.finalize", self.twin.finalize)
        self._drain(self.eng.mono_ns)
        super().finalize()
        self._try_join()
        for tid, lr in self.live_rows.items():
            if tid not in self._joined:
                self._joined.add(tid)
                self._write_twin_row(lr, {k: None for k in lr} | {"paper_trade_id": tid})
        why = self.halted or (f"pilot rule {self.rules.stopped['rule']}" if self.rules.stopped else None) or "normal shutdown"
        self._alert("stop", f"LIVE stopped: {why}", "")
        if not self.halted:
            self._alert("daily_summary", f"final summary {self.session}", self.summary(True)[0])
