"""Strategy v3: trade the LAG, not the level.

v2 competed with the Polymarket mid on its own level and lost to winner's curse (the biggest model-vs-market disagreements
are the model's mistakes). v3 takes the Polymarket Up mid as the ANCHOR and adds only the change the model attributes to a
fresh Binance move:

    target_up = logistic( logit(anchor) + beta * (logit(P_v2(S_now)) - logit(P_v2(S_base))) )

with sigma, S0 and tau held fixed at detection time, S_base = the Binance mid at the start of the move, S_now = the mid at
detection, anchor = the Polymarket state strictly BEFORE the move started. Only pure, deterministic helpers live here (used by the
engine and by the event study, so the study measures exactly what the strategy trades). No look-ahead: every function takes
only data with recv time <= the decision time.
"""
import bisect
import math
from collections import deque
from types import SimpleNamespace as Rec

from .book import TICK
from .model import fee_per_share

CLIP_LO, CLIP_HI = 0.01, 0.99


def clip_p(p):
    return min(max(p, CLIP_LO), CLIP_HI)


def logit(p):
    p = clip_p(p)
    return math.log(p / (1.0 - p))


def logistic(x):
    return 1.0 / (1.0 + math.exp(-x))


def anchored_target(anchor, p_now, p_base, beta=1.0, mode="logit"):
    """P(Up) the market should reach after absorbing the move. Probabilities are clipped to [0.01, 0.99] before the logit."""
    if mode == "linear":
        return clip_p(anchor + beta * (p_now - p_base))
    return logistic(logit(anchor) + beta * (logit(p_now) - logit(p_base)))


# ------------------------------------------------------------------------------------------------ move detection
class MoveDetector:
    """Online detector: the Binance mid moved >= move_bps within the last lookback_ms. Monotone deques keep O(1) work per tick.

    update(t, mid) -> Rec(dir, bps, start_t, base_mid, t, mid) or None. start_t/base_mid = the extremum tick of the move (the
    last Binance mid before the move began; the latest tick on ties). Uses only ticks with time <= t."""

    def __init__(self, move_bps, lookback_ms):
        self.m, self.L = float(move_bps), lookback_ms / 1000.0
        self.mins, self.maxs = deque(), deque()

    def reset(self):
        self.mins.clear()
        self.maxs.clear()

    def update(self, t, mid):
        mins, maxs = self.mins, self.maxs
        while mins and mins[-1][1] >= mid:
            mins.pop()
        mins.append((t, mid))
        while maxs and maxs[-1][1] <= mid:
            maxs.pop()
        maxs.append((t, mid))
        lim = t - self.L
        while mins[0][0] < lim:
            mins.popleft()
        while maxs[0][0] < lim:
            maxs.popleft()
        lo, hi = mins[0], maxs[0]
        up = (mid / lo[1] - 1.0) * 1e4
        dn = (hi[1] / mid - 1.0) * 1e4
        if up >= self.m and up >= dn:
            return Rec(dir=1, bps=up, start_t=lo[0], base_mid=lo[1], t=t, mid=mid)
        if dn >= self.m:
            return Rec(dir=-1, bps=dn, start_t=hi[0], base_mid=hi[1], t=t, mid=mid)
        return None


class EventMerger:
    """Merges overlapping detections into one event: a detection within `lookback_s` of the previous one, same direction, is the
    same event. update(t, move) returns the Event when `move` OPENS a new event (first detection), else None."""

    def __init__(self, lookback_ms):
        self.L = lookback_ms / 1000.0
        self.cur = None

    def update(self, t, mv):
        if mv is None:
            return None
        c = self.cur
        if c is not None and mv.dir == c.dir and t - c.last_t <= self.L:
            c.last_t = t
            c.n_det += 1
            c.peak_bps = max(c.peak_bps, mv.bps)
            return None
        self.cur = Rec(dir=mv.dir, first_t=t, last_t=t, n_det=1, peak_bps=mv.bps, move=mv)
        return self.cur


# ------------------------------------------------------------------------------------------------ polymarket state history
def state_mid(s):
    return None if s is None or s.bb is None or s.ba is None else (s.bb + s.ba) / 2.0 / TICK


def state_micro(s):
    if s is None or s.bb is None or s.ba is None:
        return None
    tot = s.bbz + s.baz
    return (s.bb + s.ba) / 2.0 / TICK if tot <= 0 else (s.bb * s.baz + s.ba * s.bbz) / tot / TICK


def state_spread_pts(s):
    return None if s is None or s.bb is None or s.ba is None else (s.ba - s.bb) / TICK * 100.0


class PmHistory:
    """Best bid/ask (price in ticks + size) of each token as of every update that changed them, and the time of the last
    message per token. keep_s=None keeps everything (event study); otherwise only the last keep_s seconds plus the one state
    in force at the cut."""

    def __init__(self, keep_s=6.0):
        self.keep = keep_s
        self.ts, self.st, self.last_msg, self.msgs = {}, {}, {}, {}

    def touch(self, tok, t):
        """A message about `tok` arrived at t (even if it changed nothing): freshness is judged on message times, not change times."""
        self.last_msg[tok] = t
        m = self.msgs.setdefault(tok, [])
        m.append(t)
        if self.keep is not None and len(m) > 512:
            i = bisect.bisect_left(m, t - self.keep) - 1
            if i > 0:
                del m[:i]

    def last_msg_before(self, tok, t):
        """Time of the last message STRICTLY before t (None if none)."""
        m = self.msgs.get(tok)
        if not m:
            return None
        i = bisect.bisect_left(m, t) - 1
        return m[i] if i >= 0 else None

    def update(self, tok, t, book):
        """Record the book's current top of book at time t (no-op when unchanged)."""
        if book is None or not book.valid:
            self._add(tok, t, Rec(t=t, bb=None, bbz=0.0, ba=None, baz=0.0, valid=False))
            return
        b, a = book.best_bid(), book.best_ask()
        self._add(tok, t, Rec(t=t, bb=b[0] if b else None, bbz=b[1] if b else 0.0, ba=a[0] if a else None,
                              baz=a[1] if a else 0.0, valid=True))

    def _add(self, tok, t, s):
        ts, st = self.ts.setdefault(tok, []), self.st.setdefault(tok, [])
        if st:
            p = st[-1]
            if (p.bb, p.bbz, p.ba, p.baz, p.valid) == (s.bb, s.bbz, s.ba, s.baz, s.valid):
                return
        ts.append(t)
        st.append(s)
        if self.keep is not None and len(ts) > 128:
            i = bisect.bisect_left(ts, t - self.keep) - 1
            if i > 0:
                del ts[:i]
                del st[:i]

    def before(self, tok, t):
        """State in force just before time t: the last update with time STRICTLY less than t (None if none/invalid)."""
        ts = self.ts.get(tok)
        if not ts:
            return None
        i = bisect.bisect_left(ts, t) - 1
        if i < 0:
            return None
        s = self.st[tok][i]
        return s if s.valid else None

    def at_or_before(self, tok, t):
        ts = self.ts.get(tok)
        if not ts:
            return None
        i = bisect.bisect_right(ts, t) - 1
        if i < 0:
            return None
        s = self.st[tok][i]
        return s if s.valid else None

    def drop(self, tok):
        self.ts.pop(tok, None)
        self.st.pop(tok, None)
        self.last_msg.pop(tok, None)
        self.msgs.pop(tok, None)


# ------------------------------------------------------------------------------------------------ limit price rule
def limit_ticks(ask_t, slip_t, tick_t, target_side, thr_pts, fee_rate, fee_exp):
    """Limit = min(ask + slip, the highest price at which the edge is still >= thr_pts after the taker fee). Prices in ticks."""
    cand = ask_t + slip_t
    while cand > ask_t:
        p = cand / TICK
        if (target_side - p - fee_per_share(p, fee_rate, fee_exp)) * 100.0 >= thr_pts - 1e-9:
            break
        cand -= tick_t
    return max(cand, ask_t)


# ------------------------------------------------------------------------------------------------ the shared signal computation
def compute_signal(eng, mv, cfg, pmh):
    """Everything v3 knows at the detection instant `eng.now_utc` for the move `mv`.

    Returns Rec(reason=None|str, ...). `reason` is the first filter that rejects the event (the quantities that could be computed
    are still filled in so a rejected event can be logged and studied). Uses PM state strictly before the move start."""
    sig = Rec(reason=None, meta=None, w=None, anchor_state=None, anchor_mid=None, anchor_micro=None, anchor=None, anchor_age=None,
              anchor_spread=None, p_now=None, p_base=None, shift=None, target_up=None, s0=None, sigma=None, tau=None)
    meta, w = eng.markets.get(eng.cur), eng.windows.get(eng.cur)
    if meta is None or w is None:
        sig.reason = "no_market"
        return sig
    sig.meta, sig.w = meta, w
    s0 = eng._s0_v2(eng.cur)
    sig.s0, sig.sigma, sig.tau = s0, eng.sigma_est.sigma, w.end - eng.now_utc
    if s0 is None:
        sig.reason = "no_s0"
        return sig
    if not eng.sigma_est.ready:
        sig.reason = "sigma_warmup"
        return sig
    a = pmh.before(meta.up, mv.start_t)
    lm = pmh.last_msg_before(meta.up, mv.start_t)
    sig.anchor_state = a
    if a is None or lm is None:
        sig.reason = "no_anchor"
        return sig
    sig.anchor_mid, sig.anchor_micro = state_mid(a), state_micro(a)
    sig.anchor_age, sig.anchor_spread = mv.start_t - lm, state_spread_pts(a)      # age = time since the last MESSAGE before the move
    if sig.anchor_mid is None:
        sig.reason = "anchor_one_sided"
        return sig
    sig.anchor = sig.anchor_micro if cfg.anchor == "micro" else sig.anchor_mid
    sig.p_now, sig.p_base = eng.p_at(eng.mid), eng.p_at(mv.base_mid)
    sig.shift = logit(sig.p_now) - logit(sig.p_base)
    sig.target_up = anchored_target(sig.anchor, sig.p_now, sig.p_base, cfg.beta, cfg.target_mode)
    last = pmh.last_msg.get(meta.up)
    if last is None or eng.now_utc - last > cfg.max_book_age_s:
        sig.reason = "book_stale"
    elif sig.anchor_age > cfg.max_book_age_s:
        sig.reason = "anchor_stale"
    elif sig.anchor_spread > cfg.max_spread_pts + 1e-9:
        sig.reason = "anchor_spread"
    return sig
