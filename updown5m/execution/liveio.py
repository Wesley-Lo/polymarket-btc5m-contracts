"""LIVE output files. Every write is queued to ONE background thread, so nothing on the decision -> sign -> send path (or on the event loop)
waits for a disk. Files (all in the LIVE session's own new folder, written through transport.open_for_write, secrets redacted by its filter):
  live_orders_sent.jsonl  exactly what was sent (request parameters + the exchange's answer; never a signature or credential)
  live_fills.csv          one row per fill report (from the order response and from the trade history)
  live_latency.csv        per order: decision -> sign -> send -> response timestamps in ms
  live_vs_paper.csv       the dry-run layer's per-decision comparison, here with REAL fills in the dr_* columns
  live_vs_twin.csv        per decision: the real result next to the simulated twin's, with the causes of the difference
  live_events.csv         the state machine log (same format as the dry run; the restart recovery reads it)
"""
import csv
import json
import queue
import threading

from .. import transport
from ..util import fmt
from .outputs import Outputs, VS_COLS

FILL_COLS = ["time_utc", "trade_id", "leg", "attempt", "order_id", "source", "order_status", "trade_ids", "trade_status", "shares", "avg_price",
             "usd", "fee_usd", "fee_source", "floor_hit", "exchange_matched_utc", "local_seen_utc", "note"]
LAT_COLS = ["trade_id", "leg", "attempt", "decision_utc", "queue_ms", "decision_to_sign_start_ms", "sign_ms", "sign_to_send_ms",
            "send_to_response_ms", "decision_to_send_ms", "decision_to_response_ms", "result"]
TWIN_COLS = ["paper_trade_id", "decision_time_utc", "slug", "side", "official_result",
             "live_entry_status", "live_shares", "live_avg_price", "live_entry_fee_usd", "live_exit_steps", "live_exit_shares", "live_exit_avg_price",
             "live_exit_fee_usd", "live_held_shares", "live_final_state", "live_stuck", "live_net_usd",
             "twin_entry_status", "twin_shares", "twin_avg_price", "twin_entry_fee_usd", "twin_exit_steps", "twin_exit_shares", "twin_exit_avg_price",
             "twin_exit_fee_usd", "twin_held_shares", "twin_final_state", "twin_stuck", "twin_net_usd", "net_diff_usd", "diff_causes"]


class BgOutputs(Outputs):
    def __init__(self, directory, session):
        super().__init__(directory, session)
        self._open_csv("live_fills.csv", FILL_COLS)
        self._open_csv("live_latency.csv", LAT_COLS)
        self._open_csv("live_vs_paper.csv", VS_COLS)
        self._open_csv("live_vs_twin.csv", TWIN_COLS)
        self._f["sent"] = transport.open_for_write(self.dir / "live_orders_sent.jsonl", "a", encoding="utf-8")
        self._bq = queue.Queue()
        self._pending = 0
        self._lock = threading.Lock()
        self._stop = False
        self._t = threading.Thread(target=self._run, daemon=True, name="live-writer")
        self._t.start()
        self.errors = 0

    # ---- plumbing
    def _post(self, fn, *a, **kw):
        with self._lock:
            self._pending += 1
        self._bq.put((fn, a, kw))

    def _run(self):
        while True:
            item = self._bq.get()
            if item is None:
                return
            fn, a, kw = item
            try:
                fn(*a, **kw)
            except Exception:
                self.errors += 1
            finally:
                with self._lock:
                    self._pending -= 1

    def _row(self, name, cols, row):
        self._w[name].writerow([fmt(row.get(c)) for c in cols])
        self._f[name].flush()

    # ---- queued writers (same names the dry-run layer already calls)
    def order(self, payload):
        pass                                                      # the unsigned-intent file is a dry-run artefact; LIVE writes live_orders_sent.jsonl

    def event(self, **kw):
        self._post(Outputs.event, self, **kw)

    def vs_row(self, row):
        self._post(self._row, "live_vs_paper.csv", VS_COLS, dict(row))

    def sent(self, rec):
        self._post(lambda r: (self._f["sent"].write(json.dumps(r, separators=(",", ":"), default=str) + "\n"), self._f["sent"].flush()), dict(rec))

    def fill(self, row):
        self._post(self._row, "live_fills.csv", FILL_COLS, dict(row))

    def latency(self, row):
        self._post(self._row, "live_latency.csv", LAT_COLS, dict(row))

    def twin_row(self, row):
        self._post(self._row, "live_vs_twin.csv", TWIN_COLS, dict(row))

    def write_summary(self, text, data):
        self._post(self._summary, text, json.loads(json.dumps(data, default=str)))

    def _summary(self, text, data):
        with transport.open_for_write(self.dir / "live_summary.txt", "w", encoding="utf-8", allow_overwrite=True) as f:
            f.write(text)
        with transport.open_for_write(self.dir / "live_summary.json", "w", encoding="utf-8", allow_overwrite=True) as f:
            json.dump(data, f, indent=2, default=str)

    def drain(self, timeout=10.0):
        import time
        end = time.monotonic() + timeout
        while self._pending > 0 and time.monotonic() < end:
            time.sleep(0.01)

    def flush(self):
        self._post(lambda: [f.flush() for f in list(self._f.values())])

    def close(self):
        self.drain(15.0)
        self._bq.put(None)
        self._t.join(5.0)
        super().close()
