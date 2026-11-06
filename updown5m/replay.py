"""Replay recorded raw data through the SAME engine code as live.

    python -m updown5m.replay <session folder> [--out DIR]

Raw files (logs/raw/YYYY-MM-DD/HH/<feed>.jsonl.gz) are merged across feeds in recv_mono_ns order and fed to
Engine.handle exactly as the live consumer did, restricted to the session's wall-clock range. The outputs are then
compared byte-for-byte with the session's own CSVs.
"""
import argparse
import gzip
import heapq
import json
from pathlib import Path

from . import params as params_mod
from . import transport
from .engine import Engine
from .sink import CsvSink
from .util import stamp_str

TABLES = ["candidates", "trades", "maker_shadow", "windows", "book_checks", "series", "latency", "clock_offset"]


def feed_files(raw_root):
    out = {}
    for p in sorted(Path(raw_root).glob("*/*/*.jsonl.gz")):
        out.setdefault(p.name[:-len(".jsonl.gz")], []).append(p)
    return out


def _read(paths, w0, w1):
    for p in paths:
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue   # torn last line after a crash
                    w = d["recv_wall_ns"]
                    if (w0 is not None and w < w0) or (w1 is not None and w > w1):
                        continue
                    yield d["recv_mono_ns"], d
        except (EOFError, OSError):
            continue


def iter_records(raw_root, w0=None, w1=None, feeds=None):
    files = feed_files(raw_root)
    gens = [_read(paths, w0, w1) for feed, paths in files.items() if feeds is None or feed in feeds]
    for _, d in heapq.merge(*gens, key=lambda x: x[0]):
        yield d


def replay(raw_root, out_dir, params, w0=None, w1=None, progress=False):
    transport.require_new_dir(out_dir)   # a replay never writes into an existing (session) folder
    sink = CsvSink(out_dir, params_mod.markouts_of(params), params_mod.flip_s_of(params),
                   params_mod.rev_of(params))
    eng = Engine(params, sink)
    n = 0
    for d in iter_records(raw_root, w0, w1):
        try:
            eng.handle(d["feed"], d["seq"], d["recv_wall_ns"], d["recv_mono_ns"], d["exch_ts"], d["raw"])
        except Exception:
            eng.acc.errors += 1   # same policy as the live pipeline
        n += 1
        if progress and n % 500000 == 0:
            print(f"  replayed {n:,} records")
    eng.finalize()
    sink.close()
    return eng, n


def compare(dir_a, dir_b, tables=TABLES):
    res = {}
    for t in tables:
        a, b = Path(dir_a) / f"{t}.csv", Path(dir_b) / f"{t}.csv"
        res[t] = a.exists() and b.exists() and a.read_bytes() == b.read_bytes()
    return res


def main():
    ap = argparse.ArgumentParser(prog="python -m updown5m.replay")
    ap.add_argument("session", help="session folder (logs/sessions/<stamp>)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    sdir = Path(a.session).resolve()
    meta = json.loads((sdir / "session.json").read_text())
    pfile = next(sdir.glob("params_*.json"))
    params = params_mod.AttrDict(json.loads(pfile.read_text()))
    raw_root = transport.LOGS_ROOT.parent / meta["raw_root"]
    # default: a NEW folder logs/replays/<session>_<stamp>; never inside the session folder, never an existing folder
    out = Path(a.out) if a.out else transport.LOGS_ROOT / "replays" / f"{sdir.name}_{stamp_str()}"
    eng, n = replay(raw_root, out, params, meta["start_wall_ns"], meta["end_wall_ns"], progress=True)
    print(f"replayed {n:,} records -> {out}")
    # every table the live session wrote (v2 adds start_prices, v3 adds v3_signals / v3_trades) must be reproduced byte for byte
    res = compare(sdir, out, [t for t in TABLES + ["start_prices", "v3_signals", "v3_trades"] if (sdir / f"{t}.csv").exists()])
    for t, ok in res.items():
        print(f"  {t:14s} {'IDENTICAL' if ok else 'DIFFERENT'}")
    raise SystemExit(0 if all(res.values()) else 1)


if __name__ == "__main__":
    main()
