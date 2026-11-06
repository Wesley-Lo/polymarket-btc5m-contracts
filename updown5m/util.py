import math
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

if sys.platform == "win32":
    import ctypes

    _ft = ctypes.c_uint64()
    _gst = ctypes.windll.kernel32.GetSystemTimePreciseAsFileTime

    def wall_ns():
        """True UTC in ns. time.time() ticks at only 15.6 ms on Windows/Python 3.12; this is ~1 us."""
        _gst(ctypes.byref(_ft))
        return (_ft.value - 116444736000000000) * 100
else:
    wall_ns = time.time_ns

# time.monotonic_ns() is GetTickCount64 (15.6 ms) on Windows/Python 3.12, so recv_mono_ns uses the QPC-backed clock.
perf_ns = time.perf_counter_ns

_last_mono = 0


def stamp():
    """(wall_ns, mono_ns) with mono strictly increasing so record order is total and replay-stable."""
    global _last_mono
    w = wall_ns()
    m = perf_ns()
    if m <= _last_mono:
        m = _last_mono + 1
    _last_mono = m
    return w, m


def stamp_str():
    """'20261031T010203Z' (true UTC now) for naming report / replay folders."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_utc(t_s):
    """UTC seconds (float) -> '2026-10-30T21:25:03.123Z'. Always true UTC."""
    ms = int(math.floor(t_s * 1000 + 1e-6))
    d = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def iso_ny(t_s):
    """Same instant in New York time with explicit UTC offset (e.g. ...-04:00). Separate, labelled column only."""
    ms = int(math.floor(t_s * 1000 + 1e-6))
    d = datetime.fromtimestamp(ms // 1000, tz=NY)
    z = d.strftime("%z")
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}" + z[:3] + ":" + z[3:]


def fmt(x, digits=10):
    if x is None:
        return ""
    if isinstance(x, float):
        if x != x:
            return ""
        return format(x, f".{digits}g")
    return x
