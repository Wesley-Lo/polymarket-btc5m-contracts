"""Warm-connection clock offset (feed `clock_offset_v2`, RECORD ONLY: the frozen v3.1 engine keeps reading `clock_offset`).

Why: the old sampler timed COLD connections (60 s cadence) and read ~+270 ms too high (clock_calibration_20261101T002739Z). Here every timed
request follows an untimed warm-up request on the same keep-alive session, and the estimator keeps only samples whose RTT is within
`band_ms` of the rolling minimum RTT of that host.

NTP is NOT used (UDP is outside the allowlist; the user has not approved the change). `NtpStub` marks the place and says so.
"""
import asyncio
import statistics
from collections import deque

from .. import transport
from ..util import perf_ns, wall_ns

HOSTS = {"data-api": "https://data-api.binance.vision/api/v3/time", "api": "https://api.binance.com/api/v3/time"}
PRIMARY, BACKUP = "data-api", "api"


class NtpStub:
    """NTP is not approved (allowlist). Adding it needs: a UDP socket in transport.py, an NTP host allowlist and a safety test. Not built."""
    available = False


class ClockEstimator:
    def __init__(self, band_ms=30.0, window_s=1800.0, min_samples=5):
        self.band, self.window, self.min = float(band_ms), float(window_s), int(min_samples)
        self.s = {h: deque() for h in HOSTS}            # host -> (recv_wall_s, rtt_ms, offset_ms)

    def add(self, host, recv_wall_ns, rtt_ms, offset_ms):
        if host not in self.s:
            return
        d = self.s[host]
        d.append((recv_wall_ns / 1e9, float(rtt_ms), float(offset_ms)))
        lim = recv_wall_ns / 1e9 - self.window
        while d and d[0][0] < lim:
            d.popleft()

    def clean(self, host):
        d = self.s.get(host)
        if not d:
            return []
        rmin = min(r for _, r, _ in d)
        return [o for _, r, o in d if r <= rmin + self.band]

    def theta(self, host=PRIMARY):
        c = self.clean(host)
        return statistics.median(c) if len(c) >= self.min else None

    def status(self):
        """-> (theta_ms or None, backup_ms or None, disagreement_ms or None). theta = the number added to the local clock to get true UTC."""
        a, b = self.theta(PRIMARY), self.theta(BACKUP)
        return a, b, (abs(a - b) if a is not None and b is not None else None)


def warm_offset(n=10, gap_s=0.2, http_factory=None, band_ms=30.0):
    """Pre-flight clock check with the clock_offset_v2 method (warm-up request + timed request on the same keep-alive session, RTT band filter,
    median), both hosts in parallel. Replaces the old cold single requests (which read +79 ms on the VM while warm clock_offset_v2 read ~0).
    Synchronous (the pre-flight tools have no event loop). -> (theta_ms or None, backup_ms or None, disagreement_ms or None)."""
    est = ClockEstimator(band_ms=band_ms, min_samples=5)

    def emit(feed, o):
        if feed == "clock_offset_v2":
            est.add(o["host"], o["recv_wall_ns"], o["rtt_ms"], o["offset_ms"])

    s = ClockV2Sampler(emit)

    async def go():
        await asyncio.gather(*(s.burst(name, url, n, gap_s, http_factory) for name, url in HOSTS.items()))
    asyncio.run(go())
    return est.status()


class ClockV2Sampler:
    """Live sampler (own HTTP sessions, allowlisted GETs only). Emits one record per timed sample through `emit(feed, obj)`."""

    def __init__(self, emit, period_s=10.0):
        self.emit, self.period = emit, float(period_s)

    async def _one(self, http, name, url):
        t0 = perf_ns()
        await http.get(url)                                   # warm-up: opens/renews the keep-alive connection, result discarded
        warm = (perf_ns() - t0) / 1e6
        send = wall_ns()
        p0 = perf_ns()
        r = await http.get(url)
        p1 = perf_ns()
        recv = send + (p1 - p0)
        server_ms = r.json()["serverTime"]
        self.emit("clock_offset_v2", {"host": name, "server_ms": server_ms, "send_wall_ns": send, "recv_wall_ns": recv,
                                      "rtt_ms": (p1 - p0) / 1e6, "offset_ms": server_ms - (send + recv) / 2e6, "warmup_rtt_ms": warm})

    async def burst(self, name, url, n, gap_s, http_factory=None):
        """n warm samples of one host on ONE keep-alive session (the pre-flight's short version of run_host)."""
        http = (http_factory or (lambda: transport.HttpSession(timeout=5)))()
        try:
            for _ in range(n):
                try:
                    await self._one(http, name, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.emit("ctl", {"ev": "clock_v2_error", "host": name, "error": repr(e)})
                await asyncio.sleep(gap_s)
        finally:
            await http.aclose()

    async def run_host(self, name, url, stopping):
        http = transport.HttpSession(timeout=5)
        try:
            while not stopping():
                try:
                    await self._one(http, name, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.emit("ctl", {"ev": "clock_v2_error", "host": name, "error": repr(e)})
                await asyncio.sleep(self.period)
        finally:
            await http.aclose()
