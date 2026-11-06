"""Session summary (text + json) built from the engine's accumulators. Pure function of recorded data."""
import json
import random
import statistics

from .util import iso_utc

LIMITATIONS_V1 = [
    "Binance mid is only a PROXY for the settlement price. These markets settle on a Chainlink BTC/USD 60 s TWAP "
    "stream (market description + cryptoMarketConfig twapLookbackSeconds=60); the v1 pricing model does not model that averaging.",
    "Paper fills use the book as received over the websocket at (decision time + simulated latency); real fills would "
    "also face queue priority, other takers and our own market impact.",
    "Maker fills treat any print at/through our price during the live interval as a full fill (queue position ignored): optimistic.",
]
LIMITATIONS_V3 = [
    "Strategy v3 anchors on the Polymarket mid and trades only the model's predicted CHANGE after a fresh Binance move (the lag); it cannot "
    "profit from a better level. The detector setting, S0 alignment and beta were chosen on a few hours of recording at most: unproven.",
    "Paper fills use the book as received over the websocket at (decision time + simulated latency); real fills would also face queue "
    "priority, other takers and our own market impact. Flip exits sell into the recorded bids and pay the taker fee again.",
    "Maker fills treat any print at/through our price during the live interval as a full fill (queue position ignored): optimistic.",
]
LIMITATIONS_V31 = [
    "Strategy v3.1 anchors on the Polymarket mid and trades only the model's predicted CHANGE after a fresh Binance move (the lag). Its edge "
    "subtracts the expected exit half-spread (recent median) and the second taker fee; the main exit is a sell at +2 s that pays the simulated "
    "order latency and walks the recorded bids at arrival. Parameters were frozen on 2026-10-31 after a few hours of recording: unproven.",
    "Paper fills use the book as received over the websocket at (decision time + simulated latency); real fills would also face queue "
    "priority, other takers and our own market impact.",
    "Maker fills treat any print at/through our price during the live interval as a full fill (queue position ignored): optimistic.",
    "The Binance USD-M perpetual feeds (binance_perp_book / binance_perp_trade) are recorded only; no decision uses them.",
]
LIMITATIONS_V2 = [
    "Model v2 prices the 60 s TWAP settlement from Binance (S0 = mean mid of the 60 s before the start; terminal 60 s average modelled). Binance is still a "
    "proxy: measured residual vs Chainlink's own 5-minute move is ~0.66 bps std (archive, 176 windows). Gamma's priceToBeat is published only ~5 min "
    "after the window starts, so it cannot be a live input.",
] + LIMITATIONS_V1[1:]


def _bootstrap(per_window, n, seed):
    """per_window: list of (net_sum, n_trades) for windows with >=1 trade. 95% CI of net per trade, resampling windows."""
    if not per_window:
        return None
    rng = random.Random(seed)
    k = len(per_window)
    vals = []
    for _ in range(n):
        sn = st = 0
        for _ in range(k):
            a, b = per_window[rng.randrange(k)]
            sn += a
            st += b
        vals.append(sn / st if st else 0.0)
    vals.sort()
    return vals[int(0.025 * n)], vals[min(int(0.975 * n), n - 1)]


def _net(t, key, exit_h):
    """Net USD of a trade for the track's scoring exit: hold (exit_h None) or the flip at +exit_h s (v3.1 main exit). None = not scored."""
    if exit_h is None:
        return t.base_net if key == "base" else (t.p90_net if t.p90_filled else None)
    return (t.flip_base if key == "base" else t.flip_p90).get(exit_h)


def _stats(trades, key, markouts, exit_h=None):
    fl = [t for t in trades if _net(t, key, exit_h) is not None]
    net = [_net(t, key, exit_h) for t in fl]
    return {"n": len(fl), "net_usd": sum(net), "net_per_trade": (sum(net) / len(net)) if net else None,
            "hit_rate": (sum(1 for x in net if x > 0) / len(net)) if net else None}


def build(engine, params, extras):
    acc = engine.acc
    d = {"generated_utc": iso_utc(extras["now_utc"]), "session": extras.get("session"),
         "start_utc": extras.get("start_utc"), "end_utc": extras.get("end_utc")}
    tracks = {}
    xh = getattr(engine, "main_exit", None) if getattr(engine, "v31", False) else None     # v3.1: every track is scored by the flip at +main_exit s
    d["main_exit"] = "flip_%gs" % xh if xh is not None else "hold"
    for tr in engine.tracks:
        ts = [t for t in acc.trades if t.track == tr.name]
        st = _stats(ts, "base", engine.markouts, xh)
        tracks[tr.name] = {"threshold_pts": tr.thr, "main": tr.main, "decisions": acc.decisions[tr.name],
                           "missed_at_base_latency": acc.base_missed[tr.name], "base": st, "p90": _stats(ts, "p90", engine.markouts, xh)}
        if tr.main:
            pw = {}
            pw90 = {}
            for t in ts:
                nb, n9 = _net(t, "base", xh), _net(t, "p90", xh)
                if nb is not None:
                    a = pw.setdefault(t.start, [0.0, 0])
                    a[0] += nb
                    a[1] += 1
                if n9 is not None:
                    a = pw90.setdefault(t.start, [0.0, 0])
                    a[0] += n9
                    a[1] += 1
            b = params["bootstrap"]
            tracks[tr.name]["base"]["ci95_net_per_trade"] = _bootstrap([tuple(v) for v in pw.values()], b["n"], b["seed"])
            tracks[tr.name]["p90"]["ci95_net_per_trade"] = _bootstrap([tuple(v) for v in pw90.values()], b["n"], b["seed"])
            tracks[tr.name]["windows_with_trades"] = len(pw)
    d["taker_tracks"] = tracks
    main = next(t for t in engine.tracks if t.main).name
    mn = tracks[main]
    min_trades = params["evaluation"]["min_trades"]
    ci = mn["base"].get("ci95_net_per_trade")
    demonstrated = mn["base"]["n"] >= min_trades and ci is not None and ci[0] > 0
    d["evaluation"] = {"min_trades": min_trades, "main_track": main, "n_trades": mn["base"]["n"],
                       "ci95_net_per_trade_usd": ci, "edge_demonstrated": bool(demonstrated),
                       "small_sample_is_noise": mn["base"]["n"] < min_trades}
    fills = acc.maker_fills
    mo = {}
    for h in engine.markouts:
        v = [f.markouts.get(h) for f in fills if f.markouts.get(h) is not None]
        mo[f"{h:g}s"] = {"n": len(v), "mean_pts": statistics.fmean(v) if v else None}
    settled = [f for f in fills if getattr(f, "pnl", None) is not None]
    d["maker_shadow"] = {"orders_placed": acc.maker_placed, "orders_cancelled": acc.maker_cancelled, "fills": len(fills),
                         "fills_through": sum(1 for f in fills if f.through), "markouts_pts": mo,
                         "settled_fills": len(settled), "pnl_usd": sum(f.pnl for f in settled),
                         "pnl_per_share": (sum(f.pnl for f in settled) / sum(f.shares for f in settled)) if settled else None}
    br = {}
    tot = [0.0, 0.0, 0]
    for tau, (sm, sp, n) in acc.brier.items():
        br[str(tau)] = {"n": n, "model": sm / n if n else None, "polymarket_mid": sp / n if n else None}
        tot[0] += sm
        tot[1] += sp
        tot[2] += n
    br["all"] = {"n": tot[2], "model": tot[0] / tot[2] if tot[2] else None, "polymarket_mid": tot[1] / tot[2] if tot[2] else None}
    d["brier"] = br
    known = [w for w in acc.windows if w.mismatch is not None]
    d["binance_vs_official"] = {"windows_resolved": len(acc.windows), "comparable": len(known),
                                "mismatches": sum(w.mismatch for w in known),
                                "mismatch_rate": (sum(w.mismatch for w in known) / len(known)) if known else None}
    bc = acc.book_checks
    d["book_rebuild"] = {"rest_snapshots_compared": bc[0], "exact_matches": bc[1], "best_bid_ask_matches": bc[2],
                         "price_change_vs_exchange_best": dict(engine.pc)}
    d["latency_ms"] = {"median": engine.lat.median(), "p90": engine.lat.p90(), "samples": len(engine.lat.vals)}
    d["recording"] = extras.get("recorder", {})
    d["gaps"] = extras.get("gaps", {})
    d["engine_errors"] = acc.errors
    d["unresolved_trades_excluded"] = acc.unresolved_trades
    mc = params.get("model") or {}
    d["model"] = dict(mc) if mc.get("version") == "v2" else {"version": "v1"}
    d["gap_rule"] = dict(params.get("gap_rule") or {"enabled": False})
    d["limitations"] = LIMITATIONS_V2 if mc.get("version") == "v2" else LIMITATIONS_V1
    if getattr(engine, "v3", False):
        main_tr = [t for t in acc.trades if t.main]
        ex = {"hold": {"n": len(main_tr), "net_usd": sum(t.base_net for t in main_tr),
                       "p90_net_usd": sum(t.p90_net for t in main_tr if t.p90_filled)}}
        for h in engine.sc.flip_s:
            v = [t.flip_base[h] for t in main_tr if t.flip_base.get(h) is not None]
            v90 = [t.flip_p90[h] for t in main_tr if t.flip_p90.get(h) is not None]
            ex[f"flip_{h:g}s"] = {"n": len(v), "net_usd": sum(v), "p90_net_usd": sum(v90),
                                  "sold": sum(1 for t in main_tr if t.flip_status.get(h) == "sold")}
        d["strategy_v3"] = {"config": dict(params["strategy"]), "events_detected": acc.v3_events, "main_track_exits": ex}
        d["limitations"] = LIMITATIONS_V31 if getattr(engine, "v31", False) else LIMITATIONS_V3
    return d, render(d)


def _f(x, nd=4):
    return "n/a" if x is None else (f"{x:.{nd}f}" if isinstance(x, float) else str(x))


def render(d):
    L = [f"updown5m session summary (all times UTC)  generated {d['generated_utc']}",
         f"session {d.get('session')}  start {d.get('start_utc')}  end {d.get('end_utc')}",
         f"pricing model {d['model']}   gap rule {d['gap_rule']}"]
    v3 = d.get("strategy_v3")
    if v3:
        c = v3["config"]
        L += [f"STRATEGY v3 (lag): detector m={c['move_bps']} bps within L={c['lookback_ms']} ms, anchor {c['anchor']}, {c['target_mode']} target, beta {c['beta']}; "
              f"{v3['events_detected']} merged move events seen; " + (
                  f"entries only at the first detection of a move, one open position per track + {c['cooldown_after_exit_s']} s cooldown after each exit, "
                  f"edge net of the expected exit half-spread and the second taker fee"
                  if c.get("rev", 0) >= 1 else
                  f"up to {c['max_entries_per_window']} entries/window >= {c['min_entry_spacing_s']} s apart"),
              "  main-track exits scored from the SAME entries (net USD at base latency | p90-latency stress):"]
        for k, e in v3["main_track_exits"].items():
            L.append(f"     {k:9s} n={e['n']:>3}  net ${e['net_usd']:+8.2f}  | p90 ${e['p90_net_usd']:+8.2f}" + (f"  (sold {e['sold']})" if "sold" in e else "")
                     + ("   <- MAIN" if k == d.get("main_exit") else ""))
    L += ["", "TAKER TRACKS (paper, $10/trade; scored by the MAIN EXIT: %s)" % d.get("main_exit", "hold") +
          ("  [hold, flip_5s, flip_10s are shadow exits of the same entries; the sell pays latency and a second taker fee]"
           if d.get("main_exit", "hold") != "hold" else " to official resolution")]
    for name, t in d["taker_tracks"].items():
        b, p = t["base"], t["p90"]
        L.append(f"  {name} (edge>={t['threshold_pts']:g} pts){' [MAIN]' if t['main'] else ' [shadow]'}: decisions {t['decisions']}, "
                 f"missed {t['missed_at_base_latency']}, filled {b['n']}, net ${_f(b['net_usd'], 2)}, "
                 f"hit rate {_f(b['hit_rate'], 3)}, net/trade ${_f(b['net_per_trade'], 3)} | "
                 f"p90-latency: filled {p['n']}, net ${_f(p['net_usd'], 2)}, net/trade ${_f(p['net_per_trade'], 3)}")
        if t["main"]:
            L.append(f"      95% bootstrap CI net/trade (resampling windows): base {b.get('ci95_net_per_trade')}, "
                     f"p90 {p.get('ci95_net_per_trade')}  [{t.get('windows_with_trades', 0)} windows with trades]")
    e = d["evaluation"]
    L += ["", "EVALUATION RULE: edge is only 'demonstrated' with >= %d trades AND 95%% CI lower bound > 0." % e["min_trades"],
          f"  main-track trades: {e['n_trades']}  -> edge demonstrated: {'YES' if e['edge_demonstrated'] else 'NO'}"]
    if e["small_sample_is_noise"]:
        L.append(f"  WARNING: n={e['n_trades']} < {e['min_trades']}: small samples are noise. Do not read these P&L numbers as evidence.")
    m = d["maker_shadow"]
    L += ["", "MAKER SHADOW (virtual bids at fair-3 pts; fill only on a real print at/through; maker fee 0)",
          f"  placed {m['orders_placed']}, cancelled {m['orders_cancelled']}, fills {m['fills']} (through {m['fills_through']}), "
          f"settled {m['settled_fills']}, pnl ${_f(m['pnl_usd'], 2)}, pnl/share ${_f(m['pnl_per_share'], 4)}",
          "  markout of held token mid vs fill price (pts; negative = adverse selection): " +
          ", ".join(f"{k}: {_f(v['mean_pts'], 2)} (n={v['n']})" for k, v in m["markouts_pts"].items())]
    L += ["", "BRIER SCORE (lower is better; model P(Up) vs Polymarket Up mid, same samples, official outcome)"]
    for k, v in d["brier"].items():
        L.append(f"  tau {k:>4}: n={v['n']}  model {_f(v['model'])}  polymarket {_f(v['polymarket_mid'])}")
    b = d["binance_vs_official"]
    L += ["", f"BINANCE vs OFFICIAL: {b['mismatches']} mismatches in {b['comparable']} comparable windows "
              f"(rate {_f(b['mismatch_rate'], 3)}); {b['windows_resolved']} windows resolved",
          f"BOOK REBUILD: {d['book_rebuild']}", f"LATENCY used (rolling CLOB /time RTT): {d['latency_ms']}",
          f"RECORDING: {d['recording']}", f"GAPS: {d['gaps']}",
          f"engine errors: {d['engine_errors']}   unresolved trades excluded: {d['unresolved_trades_excluded']}", "",
          "KNOWN LIMITATIONS"] + [f"  - {x}" for x in d["limitations"]]
    return "\n".join(L) + "\n"


def write(engine, params, extras, directory):
    from . import transport
    d, text = build(engine, params, extras)
    # the live session rewrites its OWN summary files (hourly + at shutdown); nothing else may truncate them
    with transport.open_for_write(transport.safe_path(directory) / "session_summary.txt", "w", encoding="utf-8",
                                  allow_overwrite=True) as f:
        f.write(text)
    with transport.open_for_write(transport.safe_path(directory) / "session_summary.json", "w", encoding="utf-8",
                                  allow_overwrite=True) as f:
        json.dump(d, f, indent=2, default=str)
    return d, text
