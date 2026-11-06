"""Exchange-timestamp book replica for the retrospective "what would the exchange have shown at the true arrival instant" scoring.

The layer buffers the recent Polymarket and ctl records (references to objects the engine already parsed). At scoring time (after the
exit legs are due, plus a margin for late messages) the events of ONE token are ordered by EXCHANGE timestamp (messages without one: local
receive time + theta) and the book is advanced to each arrival instant. Same method as robustness_sim.Replica ('exch' mode), rewritten here
so the execution package does not import the analysis modules."""
import bisect
from collections import deque

from ..book import Book, to_ticks

KEEP = 25                                  # price levels kept per side of a snapshot (a $10 order never walks deeper)


class PmBuffer:
    def __init__(self, keep_s):
        self.keep_ns = int(keep_s * 1e9)
        self.q = deque()                   # (mono_ns, wall_ns, feed, obj)

    def add(self, mono_ns, wall_ns, feed, obj):
        self.q.append((mono_ns, wall_ns, feed, obj))
        lim = mono_ns - self.keep_ns
        while self.q and self.q[0][0] < lim:
            self.q.popleft()


def _levels(lst):
    out = []
    for x in lst:
        s = float(x["size"])
        if s > 0:
            out.append((to_ticks(x["price"]), s))
    return out


def token_events(items, token, slug, theta_ms):
    """Events of `token` from buffer items -> list of (key_ms, mono, kind, a, b, c): B(snapshot bids, asks) | P(side, ticks, size) | C(ctl)."""
    out = []
    for mono, wall, feed, obj in items:
        recv_true = wall / 1e6 + theta_ms
        if feed == "ctl":
            if isinstance(obj, dict) and obj.get("ev") in ("pm_down", "pm_connect", "pm_closed") and obj.get("slug") == slug:
                out.append((recv_true, mono, "C", obj["ev"], None, None))
            continue
        for ev in (obj if isinstance(obj, list) else [obj]):
            if not isinstance(ev, dict):
                continue
            et = ev.get("event_type")
            ts = ev.get("timestamp")
            key = float(ts) if ts else recv_true
            if et == "price_change":
                for pc in ev.get("price_changes", ()):
                    if pc.get("asset_id") == token:
                        out.append((key, mono, "P", pc["side"], to_ticks(pc["price"]), float(pc["size"])))
            elif et == "book" and ev.get("asset_id") == token:
                out.append((key, mono, "B", ev.get("bids", ()), ev.get("asks", ()), None))
    out.sort(key=lambda e: (e[0], e[1]))
    return out


class Replica:
    """The token's book advanced through exchange-ordered events. advance(t_ms) applies every event with key <= t_ms (only the LAST snapshot
    in the span is applied: earlier ones are superseded)."""

    def __init__(self, events):
        self.ev = events
        self.keys = [e[0] for e in events]
        self.i = 0
        self.bk = Book()

    def advance(self, t_ms):
        end = bisect.bisect_right(self.keys, t_ms)
        last_b = -1
        for j in range(end - 1, self.i - 1, -1):
            if self.ev[j][2] == "B":
                last_b = j
                break
        for j in range(self.i, end):
            k, _, kind, a, b, c = self.ev[j]
            if kind == "B":
                if j == last_b:
                    self._snap(a, b)
            elif kind == "P":
                if self.bk.valid:
                    self.bk.set_level(a, b, c)
            elif kind == "C":
                bk = self.bk
                bk.valid = False
                bk.bids, bk.asks, bk._bb, bk._ba = {}, {}, None, None
        self.i = end
        return self.bk

    def _snap(self, bids, asks):
        bk = self.bk
        bk.bids = dict(sorted(_levels(bids), reverse=True)[:KEEP])
        bk.asks = dict(sorted(_levels(asks))[:KEEP])
        bk._bb = max(bk.bids) if bk.bids else None
        bk._ba = min(bk.asks) if bk.asks else None
        bk.valid = True
