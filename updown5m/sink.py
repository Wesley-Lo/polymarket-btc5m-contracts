"""CSV output tables. Every timestamp column is true UTC (suffix _utc, ISO 'Z'); New York time only appears
as the separate, explicitly named *_new_york column (with its own UTC offset, never a 'Z')."""
import csv

from . import transport
from .util import fmt


V3_SIGNAL_COLS = ["time_utc", "kind", "window_start_utc", "slug", "move_dir", "move_bps", "move_start_utc", "base_mid", "mid_now",
                  "anchor_mid", "anchor_micro", "anchor_used", "anchor_age_ms", "anchor_spread_pts", "p_now", "p_base", "logit_shift",
                  "beta", "target_up", "side", "ask", "fee", "edge_pts", "tau_s", "sigma", "s0", "action"]


def v3_signal_cols(rev=0):
    return V3_SIGNAL_COLS if rev < 1 else V3_SIGNAL_COLS[:-1] + ["exit_hs_pts", "exit_price_est", "action"]


def v3_trade_cols(flip_s, rev=0, markouts_s=()):
    """rev 1 (v3.1) adds the expected-exit columns, the sell time of each flip and the bid-based markouts (both fees)."""
    cols = ["trade_id", "track", "is_main", "window_start_utc", "slug", "side", "decision_time_utc", "tau_s", "move_dir", "move_bps",
            "anchor_used", "target_up", "p_now", "p_base", "fair_side", "ask_at_signal", "edge_pts", "limit_price", "spread_pts_at_entry"]
    if rev >= 1:
        cols += ["exit_hs_pts", "exit_price_est"]
    cols += ["base_status", "base_fill_time_utc", "base_filled_shares", "base_avg_price", "base_fee_usd", "base_cost_usd",
            "p90_status", "p90_filled_shares", "p90_avg_price", "p90_fee_usd", "p90_cost_usd"]
    for h in flip_s:
        cols += [f"flip_{h:g}s_status"] + ([f"flip_{h:g}s_sell_utc"] if rev >= 1 else []) + [
            f"flip_{h:g}s_sold_shares", f"flip_{h:g}s_avg_sell_price", f"flip_{h:g}s_sell_fee_usd",
            f"flip_{h:g}s_net_usd_base", f"flip_{h:g}s_net_usd_p90"]
    if rev >= 1:
        cols += [f"markout_bid_{h:g}s_pts" for h in markouts_s]
    return cols + ["official_result", "hold_net_usd_base", "hold_net_usd_p90"]


def columns(markouts_s, flip_s=None, rev=0):
    """flip_s is not None only for strategy v3: it adds the v3_signals / v3_trades tables (v1/v2 sessions are unchanged)."""
    mo = [f"markout_{h:g}s_pts" for h in markouts_s]
    cols = _columns(mo)
    if flip_s is not None:
        cols["v3_signals"] = v3_signal_cols(rev)
        cols["v3_trades"] = v3_trade_cols(flip_s, rev, markouts_s)
    return cols


def _columns(mo):
    return {
        "candidates": ["time_utc", "time_new_york", "window_start_utc", "slug", "side", "fair", "bid", "ask", "fee",
                       "edge_pts", "sigma", "tau_s", "s0", "s_t", "action"],
        "trades": ["trade_id", "track", "is_main", "window_start_utc", "slug", "side", "decision_time_utc",
                   "decision_time_new_york", "fair", "ask_at_signal", "ask_size_at_signal", "fee_at_ask", "edge_pts",
                   "sigma", "tau_s", "s0", "s_t", "limit_price", "target_shares", "lat_base_ms", "lat_p90_ms",
                   "base_status", "base_miss_reason", "base_fill_time_utc", "base_filled_shares", "base_fill_fraction",
                   "base_avg_price", "base_fee_usd", "base_cost_usd",
                   "p90_status", "p90_miss_reason", "p90_filled_shares", "p90_fill_fraction", "p90_avg_price",
                   "p90_fee_usd", "p90_cost_usd"] + mo + ["official_result", "base_net_usd", "p90_net_usd"],
        "maker_shadow": ["fill_id", "window_start_utc", "slug", "side", "order_decision_utc", "order_live_from_utc",
                         "order_price", "order_shares", "fair_at_decision", "binance_mid_at_decision", "print_time_utc",
                         "print_price", "print_size", "through", "fill_shares", "binance_move_bps_since_decision"]
                        + mo + ["official_result", "pnl_usd"],
        "windows": ["window_start_utc", "slug", "s0", "binance_end_price", "official_result", "binance_implied_result",
                    "mismatch", "s0_valid", "end_valid", "binance_move_bps"],
        "latency": ["time_utc", "rtt_ms", "rolling_median_ms", "rolling_p90_ms"],
        "clock_offset": ["time_utc", "offset_ms", "rtt_ms", "used_offset_ms"],
        "book_checks": ["time_utc", "slug", "side", "kind", "ws_levels_bid", "ws_levels_ask", "rest_levels_bid",
                        "rest_levels_ask", "best_bid_match", "best_ask_match", "level_diffs", "max_size_diff",
                        "ws_book_age_ms", "result"],
        "series": ["time_utc", "window_start_utc", "tau_s", "s_t", "s0", "sigma", "fair_up", "up_bid", "up_ask",
                   "dn_bid", "dn_ask"],
        "start_prices": ["window_start_utc", "slug", "price_to_beat_gamma", "ptb_seen_live_utc", "ptb_seen_s_after_start",
                         "binance_twap60_before_start", "ptb_minus_binance_bps", "final_price_gamma", "binance_twap60_at_end",
                         "final_minus_binance_bps", "official_result", "ptb_mode"],
    }


class CsvSink:
    def __init__(self, directory, markouts_s, flip_s=None, rev=0):
        self.dir = transport.make_dirs(directory)
        self.cols = columns(markouts_s, flip_s, rev)
        self._f, self._w = {}, {}

    def _open(self, table):
        f = transport.open_for_write(self.dir / f"{table}.csv", "w", newline="", encoding="utf-8")
        w = csv.writer(f, lineterminator="\n")
        w.writerow(self.cols[table])
        self._f[table], self._w[table] = f, w
        return w

    def write(self, table, row):
        w = self._w.get(table) or self._open(table)
        w.writerow([fmt(x) for x in row])

    def flush(self):
        for f in self._f.values():
            f.flush()

    def close(self):
        for table in self.cols:   # create empty tables too so the session folder is always complete
            if table not in self._w:
                self._open(table)
        for f in self._f.values():
            f.close()
        self._f.clear()
        self._w.clear()


class MemorySink:
    """Test helper: collects rows in memory."""

    def __init__(self):
        self.rows = {}

    def write(self, table, row):
        self.rows.setdefault(table, []).append(list(row))

    def flush(self):
        pass

    def close(self):
        pass
