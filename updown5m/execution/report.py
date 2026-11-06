"""dryrun_summary.txt / .json: decisions, intended orders, fills, exit attempts by step, STUCK, risk blocks by reason, dry-run net, paper net,
the difference and its causes. No claim about profitability (the evaluation rule of the paper summary applies: >= 300 trades and a CI)."""
from collections import Counter

from ..util import iso_utc


def _sum(rows, key):
    v = [r[key] for r in rows if r.get(key) not in (None, "")]
    return sum(v), len(v)


def build(layer, final=False):
    eng, rk, c = layer.eng, layer.risk, layer.counts
    rows = layer.rows
    both = [r for r in rows if r.get("paper_net_usd") not in (None, "") and r.get("dr_net_usd") not in (None, "")]
    paper_all, n_paper = _sum(rows, "paper_net_usd")
    paper_clean = sum(r["paper_net_usd"] for r in rows if r.get("paper_net_usd") not in (None, "") and r["paper_exit_status"] == "sold")
    n_clean = sum(1 for r in rows if r.get("paper_net_usd") not in (None, "") and r["paper_exit_status"] == "sold")
    dr, n_dr = _sum(rows, "dr_net_usd")
    dxh, n_dxh = _sum(rows, "dx_half_net_usd")
    dxf, n_dxf = _sum(rows, "dx_full_net_usd")
    causes = {}
    for r in both:
        for reason in (r.get("diff_reasons") or "none").split("|"):
            a = causes.setdefault(reason, [0, 0.0])
            a[0] += 1
            a[1] += r.get("net_diff_usd") or 0.0
    d = {
        "generated_utc": iso_utc(eng.now_utc), "session": layer.session, "mode": layer.mode.value, "final": final,
        "config_sha256": layer.cfg.sha256(),
        "decisions_main_track": c["decisions"], "risk_blocks_total": dict(rk.blocks_total), "risk_blocks_today": dict(rk.blocks_today),
        "intended_orders": {"entry": c["entry_sent"], "exit": c["exit_sent"]},
        "entries": {"filled": c["entry_filled"], "partial": c["entry_partial"], "missed": c["entry_missed"],
                    "miss_reasons": dict(layer.miss_reasons)},
        "exit_attempts_by_step": {str(k): v for k, v in sorted(layer.step_stats.items())},
        "closed_fully": c["closed"], "dust_held": c["dust"], "stuck": c["stuck"], "kill_switch_exits": c["kill_exits"],
        "settled": c["settled"], "layer_errors": layer.errors, "layer_disabled": layer.disabled,
        "dry_run": {"net_usd": dr, "n": n_dr}, "dry_run_exchange_time_half_rtt": {"net_usd": dxh, "n": n_dxh},
        "dry_run_exchange_time_full_rtt": {"net_usd": dxf, "n": n_dxf},
        "paper": {"net_usd_all_fills": paper_all, "n": n_paper, "net_usd_clean_exits_only": paper_clean, "n_clean": n_clean},
        "difference_dryrun_minus_paper": {"net_usd": sum(r["net_diff_usd"] for r in both), "n_compared": len(both),
                                          "by_reason_count_and_usd": {k: [v[0], v[1]] for k, v in sorted(causes.items())}},
        "daily": {"day": rk.day, "net_usd": rk.net_today, "stuck": rk.stuck_today},
        "clock": dict(zip(("theta_ms", "backup_ms", "disagreement_ms"), layer.clock.status())),
        "clock_fixed_theta_ms": layer.cfg.clock_fixed_theta_ms,
    }
    L = [f"updown5m DRY-RUN execution summary (all times UTC)  generated {d['generated_utc']}  {'FINAL' if final else 'hourly'}",
         f"session {layer.session}  mode {layer.mode.value}  NOTHING WAS SIGNED OR SENT  config sha256 {d['config_sha256'][:16]}",
         "",
         f"main-track decisions {c['decisions']}: intended entry orders {c['entry_sent']}, exit orders {c['exit_sent']}",
         f"entries: filled {c['entry_filled']}, partial (kept) {c['entry_partial']}, missed {c['entry_missed']} {dict(layer.miss_reasons)}",
         "exit attempts by step (1 = +2 s, 2 = +5 s, 3 = +10 s after the entry fill): " +
         (", ".join(f"#{k}: sent {v['sent']} full {v['full']} partial {v['partial']} none {v['none']}" for k, v in sorted(layer.step_stats.items())) or "none"),
         f"closed fully {c['closed']}, dust held {c['dust']}, STUCK {c['stuck']}, kill-switch exits {c['kill_exits']}",
         f"risk blocks (total): {dict(rk.blocks_total) or 'none'}   today: {dict(rk.blocks_today) or 'none'}",
         "",
         f"dry-run net (receive-order book, same view as the paper simulator): ${dr:+.2f} over {n_dr} resolved/closed decisions",
         f"dry-run net, exchange-time book, arrival decision + RTT/2: ${dxh:+.2f} (n={n_dxh});  decision + full RTT: ${dxf:+.2f} (n={n_dxf})",
         f"paper net (main track, +2 s flip, same decisions): ${paper_all:+.2f} over {n_paper} fills; excluding exits that fell back to hold "
         f"(book_invalid etc.): ${paper_clean:+.2f} over {n_clean}",
         f"dry-run minus paper on the {len(both)} decisions where both have a result: ${d['difference_dryrun_minus_paper']['net_usd']:+.2f}",
         "  causes (a decision can carry several; count, sum of its net difference): " +
         (", ".join(f"{k}: {v[0]}, ${v[1]:+.2f}" for k, v in sorted(causes.items())) or "none"),
         f"today ({rk.day}): dry-run net ${rk.net_today:+.2f}, STUCK {rk.stuck_today}",
         f"clock (warm theta / backup / disagreement ms): {d['clock']}" +
         (f"; clock check skipped, fixed theta {layer.cfg.clock_fixed_theta_ms} ms used for exchange-time scoring" if layer.cfg.clock_fixed_theta_ms is not None else ""),
         f"layer errors {layer.errors}{' (DISABLED)' if layer.disabled else ''}",
         "",
         "Reading rule: these are SIMULATED fills on the recorded book; fewer than 300 trades and no confidence interval means nothing here is evidence of an edge."]
    return "\n".join(L) + "\n", d
