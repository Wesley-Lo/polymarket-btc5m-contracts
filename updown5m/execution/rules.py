"""Pre-registered pilot stop rules (coded, evaluated automatically; the thresholds are in live_config.json and were fixed before any live trade).

  S1  fill rate  (entry orders that filled or partly filled / entry orders with a final outcome)  < 15%  -> stop   (was 40% until 2026-11-01T20:58Z; see logs/reports/pilot_rules_*.md)
  S2  mean REAL net per filled main-track trade, after real fees                       < $0.25      -> stop   ($0.50 per $10 of budget)
  S3  mean (REAL net - simulated-twin net) per filled main-track trade                 < -$0.75     -> stop   ($0.75 at the $5 pilot, scaled by budget/5)

Checkpoints: S1 every `rule_checkpoint_trades` (50) entry orders that reached a final outcome; S2 and S3 every 50 filled trades whose real net is known (a net per
trade only exists for filled trades; S3 needs the twin's net too). All numbers are cumulative since the first live trade of the log.
On a stop: no new entries, an alert with the numbers, a report file, and a RULE_STOP file in updown5m/logs/ that must be deleted by hand before
LIVE will start again."""
import json

from .. import transport
from ..util import iso_utc


class PilotRules:
    def __init__(self, lcfg, on_stop, stop_path=None, now=lambda: 0.0):
        self.c, self.on_stop, self.now = lcfg, on_stop, now
        self.stop_path = stop_path or transport.LOGS_ROOT / lcfg.rule_stop_file
        self.sent = self.done = self.filled = 0
        self.nets, self.diffs = [], []
        self.next_s1 = self.next_s23 = lcfg.rule_checkpoint_trades
        self.stopped = None
        self.checks = []

    def exists(self):
        return self.stop_path.exists()

    def note_sent(self):
        self.sent += 1

    def note_result(self):
        """An entry order reached its final outcome (filled, partly filled or missed). S1 is evaluated on outcomes, never on orders still in flight."""
        self.done += 1
        if self.done >= self.next_s1:
            self.next_s1 += self.c.rule_checkpoint_trades
            self._s1()

    def note_filled(self):
        self.filled += 1

    def note_trade(self, live_net, twin_net):
        """A filled live trade whose real net is known (twin_net None if the twin has no result)."""
        self.nets.append(live_net)
        if twin_net is not None:
            self.diffs.append(live_net - twin_net)
        if len(self.nets) >= self.next_s23:
            self.next_s23 += self.c.rule_checkpoint_trades
            self._s23()

    def _record(self, name, value, threshold, hit):
        self.checks.append({"rule": name, "value": value, "threshold": threshold, "stop": hit, "sent": self.sent, "filled": self.filled,
                            "trades_with_net": len(self.nets)})
        return hit

    def _s1(self):
        rate = self.filled / self.done if self.done else 0.0
        if self._record("S1_fill_rate", rate, self.c.rule_s1_min_fill_rate, rate < self.c.rule_s1_min_fill_rate):
            self.stop("S1", f"fill rate {rate:.1%} of {self.done} entry orders < {self.c.rule_s1_min_fill_rate:.0%}")

    def _s23(self):
        mean = sum(self.nets) / len(self.nets)
        if self._record("S2_mean_net", mean, self.c.s2_threshold, mean < self.c.s2_threshold):
            self.stop("S2", f"mean real net per trade ${mean:+.3f} over {len(self.nets)} trades < ${self.c.s2_threshold:+.2f}")
            return
        if self.diffs:
            d = sum(self.diffs) / len(self.diffs)
            if self._record("S3_live_minus_twin", d, self.c.s3_threshold, d < self.c.s3_threshold):
                self.stop("S3", f"mean (live - twin) per trade ${d:+.3f} over {len(self.diffs)} trades < ${self.c.s3_threshold:+.2f}")

    def stop(self, rule, why):
        if self.stopped:
            return
        self.stopped = {"rule": rule, "why": why}
        try:
            with transport.open_for_write(self.stop_path, "w", encoding="utf-8", allow_overwrite=True) as f:
                json.dump({"rule": rule, "why": why, "time_utc": iso_utc(self.now()), "checks": self.checks}, f, indent=2, default=str)
        except Exception:
            pass
        self.on_stop(rule, why)
