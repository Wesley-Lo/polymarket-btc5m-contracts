"""Dry-run output files. Everything goes through transport.open_for_write into the run's own NEW folder (a live session folder or a replay
folder); appends for the logs, an allowed own-file rewrite for the summaries (same rule as summary.write)."""
import csv
import json

from .. import transport
from ..util import fmt

EVENT_COLS = ["time_utc", "session", "trade_id", "event", "state_from", "state_to", "reason", "slug", "token_id", "side", "shares",
              "price", "net_usd", "held_shares", "window_end_utc", "detail"]

VS_COLS = [
    "paper_trade_id", "decision_time_utc", "window_start_utc", "slug", "side", "official_result",
    "paper_status", "paper_miss_reason", "paper_shares", "paper_avg_price", "paper_entry_fee_usd",
    "paper_exit_status", "paper_exit_shares", "paper_exit_avg_price", "paper_exit_fee_usd", "paper_net_usd",
    "dr_sent", "dr_block_reason", "dr_entry_status", "dr_entry_miss_reason", "dr_order_shares", "dr_shares", "dr_avg_price", "dr_entry_fee_usd",
    "dr_exit_attempts", "dr_exit_steps", "dr_exit_shares", "dr_exit_avg_price", "dr_exit_fee_usd", "dr_held_shares", "dr_final_state",
    "dr_stuck", "dr_floor_hit", "dr_net_usd",
    "dx_half_entry_status", "dx_half_shares", "dx_half_attempts", "dx_half_held_shares", "dx_half_net_usd",
    "dx_full_entry_status", "dx_full_shares", "dx_full_attempts", "dx_full_held_shares", "dx_full_net_usd",
    "net_diff_usd", "diff_reasons"]


class Outputs:
    def __init__(self, directory, session):
        self.dir = transport.make_dirs(directory)
        self.session = session
        self._f = {}
        self._w = {}
        self._open_csv("live_events.csv", EVENT_COLS)
        self._open_csv("dryrun_vs_paper.csv", VS_COLS)
        self._f["orders"] = transport.open_for_write(self.dir / "live_orders.jsonl", "a", encoding="utf-8")

    def _open_csv(self, name, cols):
        p = self.dir / name
        new = not p.exists() or p.stat().st_size == 0
        f = transport.open_for_write(p, "a", newline="", encoding="utf-8")
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(cols)
        self._f[name], self._w[name] = f, w

    def order(self, payload):
        self._f["orders"].write(json.dumps(payload, separators=(",", ":"), sort_keys=False) + "\n")
        self._f["orders"].flush()

    def event(self, **kw):
        row = {c: kw.get(c) for c in EVENT_COLS}
        row["session"] = self.session
        if isinstance(row["detail"], (dict, list)):
            row["detail"] = json.dumps(row["detail"], separators=(",", ":"))
        self._w["live_events.csv"].writerow([fmt(row[c]) for c in EVENT_COLS])
        self._f["live_events.csv"].flush()

    def vs_row(self, row):
        self._w["dryrun_vs_paper.csv"].writerow([fmt(row.get(c)) for c in VS_COLS])

    def flush(self):
        for f in self._f.values():
            f.flush()

    def write_summary(self, text, data):
        with transport.open_for_write(self.dir / "dryrun_summary.txt", "w", encoding="utf-8", allow_overwrite=True) as f:
            f.write(text)
        with transport.open_for_write(self.dir / "dryrun_summary.json", "w", encoding="utf-8", allow_overwrite=True) as f:
            json.dump(data, f, indent=2, default=str)

    def close(self):
        for f in self._f.values():
            f.close()
        self._f.clear()
        self._w.clear()
