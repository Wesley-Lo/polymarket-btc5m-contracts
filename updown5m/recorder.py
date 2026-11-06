"""Raw recorder: every message from every feed, unmodified, one JSON line each.

Runs in its own thread (gzip releases the GIL, and the asyncio signal path never waits on disk).
Layout: logs/raw/YYYY-MM-DD/HH/<feed>.jsonl.gz, hourly rotation by recv_wall_ns (UTC), flushed every second.
Line: {"feed","seq","recv_wall_ns","recv_mono_ns","exch_ts","frame_mono_ns","raw"}
  recv_mono_ns  = time.perf_counter_ns() when our reader took the message (strictly increasing, total order)
  frame_mono_ns = same clock, when the websocket frame was parsed (earlier than recv_mono_ns by the queue time)
"""
import gzip
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from . import transport

_STOP = object()


class RawRecorder:
    def __init__(self, root, maxsize=2_000_000):
        self.root = Path(root)
        self.q = queue.Queue(maxsize=maxsize)
        self.dropped = 0
        self.errors = 0
        self.counts = {}
        self.bytes_written = 0
        self.started = time.monotonic()
        self._files = {}   # (feed, hour_idx) -> [raw_fileobj, gzfile, last_tell, last_write_monotonic]
        self._hour_dirs = {}
        self._thread = threading.Thread(target=self._run, daemon=True, name="raw-writer")
        self._thread.start()

    def put(self, feed, seq, wall_ns, mono_ns, exch_ts, frame_mono_ns, raw_json):
        try:
            self.q.put_nowait((feed, seq, wall_ns, mono_ns, exch_ts, frame_mono_ns, raw_json))
        except queue.Full:
            self.dropped += 1

    def close(self, timeout=60):
        self.q.put(_STOP)
        self._thread.join(timeout)

    def stats(self):
        return {"dropped": self.dropped, "errors": self.errors, "queue_depth": self.q.qsize(),
                "bytes_written": self.bytes_written, "counts": dict(self.counts)}

    def mb_per_hour(self):
        el = time.monotonic() - self.started
        return None if el < 30 else self.bytes_written / 1e6 / (el / 3600)

    # --- writer thread
    def _dir_for(self, hour_idx):
        d = self._hour_dirs.get(hour_idx)
        if d is None:
            t = datetime.fromtimestamp(hour_idx * 3600, tz=timezone.utc)
            d = self.root / t.strftime("%Y-%m-%d") / t.strftime("%H")
            self._hour_dirs[hour_idx] = d
        return d

    def _open(self, feed, hour_idx):
        path = self._dir_for(hour_idx) / f"{feed}.jsonl.gz"
        raw = transport.open_for_write(path, "ab")
        gz = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=4)
        ent = [raw, gz, raw.tell(), time.monotonic()]
        self._files[(feed, hour_idx)] = ent
        return ent

    def _write(self, item):
        feed, seq, wall_ns, mono_ns, exch_ts, frame_mono_ns, raw_json = item
        hour_idx = wall_ns // 3_600_000_000_000
        ent = self._files.get((feed, hour_idx)) or self._open(feed, hour_idx)
        line = ('{"feed":"%s","seq":%d,"recv_wall_ns":%d,"recv_mono_ns":%d,"exch_ts":%s,"frame_mono_ns":%s,"raw":%s}\n'
                % (feed, seq, wall_ns, mono_ns, "null" if exch_ts is None else exch_ts,
                   "null" if frame_mono_ns is None else frame_mono_ns, raw_json))
        ent[1].write(line.encode("utf-8"))
        ent[3] = time.monotonic()
        self.counts[feed] = self.counts.get(feed, 0) + 1

    def _flush(self, close_idle=False):
        now = time.monotonic()
        for key in list(self._files):
            raw, gz, last_tell, last_w = self._files[key]
            gz.flush()
            raw.flush()
            t = raw.tell()
            self.bytes_written += t - last_tell
            self._files[key][2] = t
            if close_idle and now - last_w > 120:
                gz.close()
                raw.close()
                del self._files[key]

    def _run(self):
        last = time.monotonic()
        while True:
            try:
                item = self.q.get(timeout=0.25)
            except queue.Empty:
                item = None
            if item is _STOP:
                break
            if item is not None:
                try:
                    self._write(item)
                except Exception:
                    self.errors += 1
            if time.monotonic() - last >= 1.0:
                try:
                    self._flush(close_idle=True)
                except Exception:
                    self.errors += 1
                last = time.monotonic()
        # drain whatever is left, then close
        while True:
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                break
            if item is not _STOP:
                try:
                    self._write(item)
                except Exception:
                    self.errors += 1
        self._flush()
        for raw, gz, _, _ in self._files.values():
            gz.close()
            raw.close()
        self._files.clear()


class NullRecorder:
    """Same interface as RawRecorder, writes nothing (the VM profile: no raw feed recording). It only counts messages per feed, so the
    status line and summary keep their shape. The decisions and every execution-layer output are independent of the recorder."""

    def __init__(self):
        self.dropped = 0
        self.errors = 0
        self.counts = {}
        self.bytes_written = 0

    def put(self, feed, seq, wall_ns, mono_ns, exch_ts, frame_mono_ns, raw_json):
        self.counts[feed] = self.counts.get(feed, 0) + 1

    def close(self, timeout=60):
        pass

    def stats(self):
        return {"dropped": 0, "errors": 0, "queue_depth": 0, "bytes_written": 0, "counts": dict(self.counts)}

    def mb_per_hour(self):
        return None
