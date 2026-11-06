"""live_summary.txt / .json (hourly and at the end of a LIVE session). Plain facts about REAL orders; no claim about profitability."""
import math


def _pct(vals, q):
    if not vals:
        return None
    s = sorted(vals)
    return s[min(len(s) - 1, max(0, math.ceil(q * len(s)) - 1))]


def _f(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def build_live(L, final=False):
    eng, c = L.eng, L.counts
    rows = L.rows
    live_net = [r["dr_net_usd"] for r in rows if r["dr_net_usd"] is not None and r["dr_entry_status"] in ("filled", "partial")]
    fees = sum((r["dr_entry_fee_usd"] or 0.0) + (r["dr_exit_fee_usd"] or 0.0) for r in rows if r["dr_entry_status"] in ("filled", "partial"))
    twin_by_id = {r["paper_trade_id"]: r for r in L.twin.rows}
    pairs = [(r["dr_net_usd"], twin_by_id[r["paper_trade_id"]]["dr_net_usd"]) for r in rows
             if r["paper_trade_id"] in twin_by_id and r["dr_net_usd"] is not None and twin_by_id[r["paper_trade_id"]]["dr_net_usd"] is not None
             and r["dr_entry_status"] in ("filled", "partial")]
    sent, filled = c["entry_sent"], c["entry_filled"] + c["entry_partial"]
    d2s = [x[0] for x in L.lat_rows]
    sg = [x[1] for x in L.lat_rows]
    rt = [x[2] for x in L.lat_rows]
    theta, backup, dis = L.clock.status()
    data = {
        "session": L.session, "final": final, "mode": "LIVE", "halted": L.halted, "entries_halted": L.entries_halted, "mismatch": L.mismatch,
        "decisions": c["decisions"], "entries_sent": sent, "filled": c["entry_filled"], "partial": c["entry_partial"], "missed": c["entry_missed"],
        "fill_rate": (filled / sent) if sent else None, "exits_sent": c["exit_sent"], "closed": c["closed"], "stuck": c["stuck"], "dust": c["dust"],
        "kill_exits": c["kill_exits"], "counters": dict(L.counters), "risk_blocks": dict(L.risk.blocks_total),
        "trades_with_real_net": len(live_net), "real_net_usd": sum(live_net), "real_fees_usd": fees,
        "twin_pairs": len(pairs), "twin_net_usd": sum(p[1] for p in pairs), "live_minus_twin_usd": sum(p[0] - p[1] for p in pairs),
        "today": {"day": L.risk.day, "real_net_usd": L.risk.net_today, "stuck": L.risk.stuck_today},
        "rules": {"stopped": L.rules.stopped, "checks": L.rules.checks[-6:], "sent": L.rules.sent, "filled": L.rules.filled},
        "latency_ms": {"decision_to_send_p50": _pct(d2s, .5), "decision_to_send_p99": _pct(d2s, .99), "sign_p50": _pct(sg, .5),
                       "sign_p99": _pct(sg, .99), "send_to_response_p50": _pct(rt, .5), "send_to_response_p99": _pct(rt, .99), "n": len(L.lat_rows)},
        "clock": {"theta_ms": theta, "backup_ms": backup, "disagreement_ms": dis},
        "cash": {"start_usd": L.cash_start, "now_usd": L.cash_now, "delta_usd": (L.cash_now - L.cash_start) if (L.cash_now is not None and L.cash_start is not None) else None},
        "alerts": {"queued_sent": len(L.alerts.sent), "failed": L.alerts.failed, "rate_limited": L.alerts.suppressed},
        "writer_errors": L.out.errors, "layer_errors": L.errors,
    }
    t = []
    t.append(f"updown5m LIVE pilot summary (UTC)  session {L.session}  {'FINAL' if final else 'hourly'}  REAL ORDERS WERE SENT BY THIS MODE")
    if L.halted or L.entries_halted or L.mismatch or L.rules.stopped:
        t.append(f"STATE: halted={L.halted} entries_halted={L.entries_halted} mismatch={L.mismatch} rule_stop={L.rules.stopped}")
    t.append(f"decisions {c['decisions']}: entry orders sent {sent}, filled {c['entry_filled']}, partial (kept) {c['entry_partial']}, missed {c['entry_missed']}"
             f"  fill rate {_f(100 * data['fill_rate'], 1) if data['fill_rate'] is not None else '-'}%")
    t.append(f"exits sent {c['exit_sent']}, closed {c['closed']}, STUCK {c['stuck']}, dust held {c['dust']}, kill-switch exits {c['kill_exits']}")
    t.append(f"orders: rejects {L.counters['rejects']}, unknown (no answer) {L.counters['unknown']}, rate limited {L.counters['rate_limited']}, "
             f"late results {L.counters['late_results']}, trade-history corrections {L.counters['corrections']}, failed trades {L.counters['trade_failed']}")
    t.append(f"risk blocks: {dict(L.risk.blocks_total) or 'none'}")
    t.append(f"REAL net (after real fees, resolved trades only): ${sum(live_net):+.2f} over {len(live_net)} filled trades, fees ${fees:.2f}; "
             f"today ({L.risk.day}) ${L.risk.net_today:+.2f}, STUCK today {L.risk.stuck_today}")
    t.append(f"simulated twin on the same decisions: ${sum(p[1] for p in pairs):+.2f} over {len(pairs)} trades; real minus twin ${sum(p[0] - p[1] for p in pairs):+.2f}")
    t.append(f"decision->send ms p50/p99: {_f(_pct(d2s, .5), 2)}/{_f(_pct(d2s, .99), 2)}  (sign {_f(_pct(sg, .5), 2)}/{_f(_pct(sg, .99), 2)}), "
             f"send->response ms p50/p99: {_f(_pct(rt, .5), 0)}/{_f(_pct(rt, .99), 0)}  n={len(L.lat_rows)}   (target decision->send p99 < 5 ms); "
             f"signs > {L.lcfg.build_alert_ms:g} ms: {L.counters.get('slow_signs', 0)}")
    t.append(f"reconciliation: {L.counters['snapshots']} snapshots, {L.counters['mismatches']} mismatches, api errors {L.counters['api_errors']}")
    t.append(f"pilot rules: sent {L.rules.sent}, filled {L.rules.filled}, trades with net {len(L.rules.nets)}; stopped: {L.rules.stopped or 'no'}")
    t.append(f"clock theta/backup/disagreement ms: {_f(theta, 1)}/{_f(backup, 1)}/{_f(dis, 1)}   cash start/now: {_f(L.cash_start)}/{_f(L.cash_now)}")
    t.append(f"alerts: sent {len(L.alerts.sent)}, failed {L.alerts.failed}, rate-limited {L.alerts.suppressed}; layer errors {L.errors}; writer errors {L.out.errors}")
    t.append("")
    t.append("Reading rule: a pilot of tens of trades is a test of the PLUMBING (fills, fees, latency), not evidence of an edge.")
    return "\n".join(t) + "\n", data
