import math
import statistics
from collections import deque


def norm_cdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))


def fee_per_share(p, rate, exponent=1.0):
    """Taker fee per share = rate * (p*(1-p))**exponent (Polymarket crypto_fees_v2: exponent 1, rate 0.07). Makers pay 0."""
    q = min(max(p * (1 - p), 0.0), 0.25)
    return rate * (q if exponent == 1.0 else q ** exponent)


def fair_up(s, s0, sigma, tau, min_tau=1.0):
    """P(Up) = N(ln(S/S0) / (sigma*sqrt(tau))). No drift/momentum/book terms (v1)."""
    tau = max(tau, min_tau)
    return norm_cdf(math.log(s / s0) / (sigma * math.sqrt(tau)))


class SigmaEwma:
    """EWMA of squared 1-second log returns of the Binance mid, half-life `halflife_s`, clamped to [floor, cap].

    One sample per whole second (the last mid before the second boundary); silent seconds count as zero returns.
    """

    def __init__(self, halflife_s, floor, cap, initial, warmup_s):
        self.lam = 0.5 ** (1.0 / halflife_s)
        self.floor, self.cap, self.warmup = floor, cap, warmup_s
        self.var = initial ** 2
        self.cur_sec = None
        self.last_mid = None
        self.prev_sample = None
        self.n = 0

    def update(self, t_s, mid):
        sec = math.floor(t_s)
        if self.cur_sec is None:
            self.cur_sec, self.last_mid = sec, mid
            return
        if sec > self.cur_sec:
            missing = sec - self.cur_sec
            if missing > 600:  # long outage: restart warm-up
                self.prev_sample, self.n = None, 0
            else:
                r = math.log(self.last_mid / self.prev_sample) if self.prev_sample else 0.0
                for i in range(missing):
                    ri = r if i == 0 else 0.0
                    self.var = self.lam * self.var + (1 - self.lam) * ri * ri
                    if self.prev_sample:
                        self.n += 1
            self.prev_sample = self.last_mid
            self.cur_sec = sec
        self.last_mid = mid

    @property
    def ready(self):
        return self.n >= self.warmup

    @property
    def sigma(self):
        return min(max(math.sqrt(self.var), self.floor), self.cap)


class LatencyTracker:
    """Rolling median / p90 (nearest-rank) of live CLOB /time round trips, with start values until enough samples."""

    def __init__(self, init_median_ms, init_p90_ms, window_n, min_samples):
        self.init_med, self.init_p90, self.min = init_median_ms, init_p90_ms, min_samples
        self.vals = deque(maxlen=window_n)

    def add(self, ms):
        self.vals.append(float(ms))

    def median(self):
        return statistics.median(self.vals) if len(self.vals) >= self.min else float(self.init_med)

    def p90(self):
        if len(self.vals) < self.min:
            return max(float(self.init_p90), float(self.init_med))
        s = sorted(self.vals)
        return max(s[math.ceil(0.9 * len(s)) - 1], self.median())


# ---------------------------------------------------------------------------------------------------------------
# Model v2: 60 s TWAP settlement. v1 (above) is untouched and stays selectable via params model.version = "v1".
# ---------------------------------------------------------------------------------------------------------------
def _betacf(a, b, x):
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 3e-14:
            break
    return h


def _betai(a, b, x):
    """Regularised incomplete beta I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def student_t_cdf(x, nu):
    if x == 0.0:
        return 0.5
    p = 0.5 * _betai(nu / 2.0, 0.5, nu / (nu + x * x))
    return 1.0 - p if x > 0 else p


class SecondGrid:
    """Per-second samples of the reference mid. g[k] = last mid before time k (a whole second), i.e. the value of
    the price at boundary k. Seconds with no tick are forward-filled and flagged (filled)."""

    def __init__(self, keep_s=1200):
        self.g = {}
        self.filled = set()
        self.cur_sec = None
        self.last_mid = None
        self.keep = keep_s

    def update(self, t_s, mid):
        sec = math.floor(t_s)
        if self.cur_sec is None:
            self.cur_sec, self.last_mid = sec, mid
            return
        if sec > self.cur_sec:
            if sec - self.cur_sec > 900:       # long outage: restart the grid
                self.g.clear()
                self.filled.clear()
                self.cur_sec, self.last_mid = sec, mid
                return
            for k in range(self.cur_sec + 1, sec + 1):
                self.g[k] = self.last_mid
                if k > self.cur_sec + 1:
                    self.filled.add(k)
            self.cur_sec = sec
            old = sec - self.keep
            if old in self.g:
                for k in [k for k in self.g if k < old]:
                    del self.g[k]
                    self.filled.discard(k)
        self.last_mid = mid

    def mean(self, a, b, max_filled=5):
        """Mean of g[k] for k in [a, b-1] (the 60 s TWAP proxy for [a, b)); None if the window is incomplete/gappy."""
        vals = [self.g.get(k) for k in range(a, b)]
        if any(v is None for v in vals):
            return None
        if sum(1 for k in range(a, b) if k in self.filled) > max_filled:
            return None
        return sum(vals) / len(vals)


def average_forecast(grid, s_t, now, end, W):
    """Expected value of the final W-second average (samples at boundaries end-W .. end-1) and the variance 'time'
    such that Var(avg) = sigma^2 * var_time * S^2:  tau >= W: tau - 2W/3;  tau < W: tau^3 / (3 W^2)."""
    tau = end - now
    if W <= 0:                       # spot at `end` (alignment grid W = 0)
        if tau > 0:
            return s_t, tau
        v = grid.g.get(math.floor(end))
        return (s_t if v is None else v), 0.0
    if tau >= W:
        return s_t, tau - 2.0 * W / 3.0
    a = end - W
    last = min(math.floor(now), end - 1)
    r, n = 0.0, 0
    for k in range(a, last + 1):
        v = grid.g.get(k)
        r += s_t if v is None else v
        n += 1
    ea = (r + (W - n) * s_t) / W
    return ea, max(tau, 0.0) ** 3 / (3.0 * W * W)


def p_up_v2(expected_avg, s0, sigma, var_time, mult=1.0, basis_bps=0.0, dist="normal", df=5.0, min_var_s=1.0):
    """P(final average >= S0). Extra knobs (fitted on the archive, see calibrate_v2): sigma multiplier, an additive
    cross-venue basis std (bps) and a Student-t (unit-variance scaled) instead of a normal."""
    vt = max(var_time, min_var_s)
    sd = math.sqrt((sigma * mult) ** 2 * vt + (basis_bps * 1e-4) ** 2)
    z = (expected_avg - s0) / (s0 * sd)
    if dist == "student_t":
        return student_t_cdf(z * math.sqrt(df / (df - 2.0)), df)
    return norm_cdf(z)
