"""Risk controls (all active in DRY_RUN exactly as they would be live). Every block returns a reason that the layer logs."""
import os
from collections import Counter, deque
from datetime import datetime, timezone

from ..transport import LOGS_ROOT


def day_of(utc_s):
    return datetime.fromtimestamp(utc_s, tz=timezone.utc).strftime("%Y-%m-%d")


class KillSwitch:
    """`updown5m/STOP` exists -> stop new entries and exit open positions. Polled at most every `poll_s` (monotonic record time)."""

    def __init__(self, filename="STOP", poll_s=1.0, exists_fn=None):
        self.path = LOGS_ROOT.parent / filename
        self.poll_ns = int(poll_s * 1e9)
        self.exists_fn = exists_fn or (lambda: os.path.exists(self.path))
        self._at = None
        self._on = False

    def active(self, mono_ns):
        if self._at is None or mono_ns - self._at >= self.poll_ns:
            self._at = mono_ns
            try:
                self._on = bool(self.exists_fn())
            except OSError:
                self._on = False
        return self._on


class Risk:
    def __init__(self, cfg, kill=None):
        self.cfg = cfg
        self.kill = kill or KillSwitch(cfg.kill_switch_file, cfg.kill_switch_poll_s)
        self.sent = deque()               # UTC seconds of entry orders sent (trailing hour)
        self.day = None
        self.net_today = 0.0
        self.stuck_today = 0
        self.blocks_today = Counter()
        self.blocks_total = Counter()
        self.stuck_total = 0
        self.seed_net, self.seed_stuck = {}, {}          # per UTC day, rebuilt from earlier sessions' logs after a restart

    def roll(self, utc_s):
        d = day_of(utc_s)
        if d != self.day:
            self.day, self.net_today, self.stuck_today = d, self.seed_net.get(d, 0.0), self.seed_stuck.get(d, 0)
            self.blocks_today = Counter()

    def check_entry(self, utc_s, mono_ns):
        """Account-level limits. -> reason or None. (Slot / feed / clock checks are made by the layer, which owns that state.)"""
        self.roll(utc_s)
        c = self.cfg
        if self.kill.active(mono_ns):
            return "kill_switch"
        if self.net_today <= -c.daily_loss_limit_usd:
            return "daily_loss_limit"
        if self.stuck_today >= c.stuck_limit_per_day:
            return "stuck_limit"
        while self.sent and self.sent[0] < utc_s - 3600.0:
            self.sent.popleft()
        if len(self.sent) >= c.max_entries_per_hour:
            return "hourly_entry_limit"
        return None

    def note_sent(self, utc_s):
        self.sent.append(utc_s)

    def note_block(self, reason):
        self.blocks_today[reason] += 1
        self.blocks_total[reason] += 1

    def note_net(self, utc_s, net):
        self.roll(utc_s)
        self.net_today += net

    def note_stuck(self, utc_s):
        self.roll(utc_s)
        self.stuck_today += 1
        self.stuck_total += 1
