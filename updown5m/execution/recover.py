"""Restart recovery: rebuild the dry-run state from the append-only `live_events.csv` logs of earlier sessions. Never assume flat.

Rules: a trade whose last logged state is a working state (ENTRY_PENDING .. RETRYING) was interrupted by the restart; a trade that holds
shares to resolution (STUCK / dust) is waiting for an official result this process will never receive. Either blocks new entries until
its window has ended plus `recovered_release_s`; on release it is booked as a WORST-CASE loss (cost + fee) into today's net.
A log that cannot be read blocks entries for the whole run (`recovery_failed`)."""
import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .state import OPEN, S

WORKING = {s.value for s in OPEN}
HELD = {S.STUCK.value, S.CLOSED.value}


def parse_utc(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()


@dataclass
class Recovered:
    ok: bool = True
    sent_utc: list = field(default_factory=list)
    net_by_day: dict = field(default_factory=dict)
    stuck_by_day: dict = field(default_factory=dict)
    unresolved: list = field(default_factory=list)       # dicts: key, slug, state, window_end, held, worst_case_usd
    files: list = field(default_factory=list)
    error: str = ""

    def blocking_until(self, release_s):
        return max((u["window_end"] + release_s for u in self.unresolved), default=None)


def find_logs(sessions_root, exclude=None, now_utc=None, max_age_h=24.0):
    out = []
    for d in sorted(Path(sessions_root).glob("*/live_events.csv")):
        if exclude and d.parent.name == exclude:
            continue
        if now_utc is not None:
            try:
                st = datetime.strptime(d.parent.name, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
            if now_utc - st > max_age_h * 3600.0:
                continue
        out.append(d)
    return out


def file_mode(path):
    """The mode a session's log was written in ("DRY_RUN" / "LIVE"), from its START event; "" if unknown."""
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["event"] == "START":
                return r["reason"].partition("=")[2]
            break
    return ""


def rebuild(paths, only_mode=None):
    """`only_mode`: read only session logs written in that mode, so dry-run and LIVE bookkeeping (daily loss, STUCK count, entries per
    hour) never mix. None = every log (the original behaviour)."""
    rec = Recovered()
    trades = {}
    try:
        for p in paths:
            if only_mode is not None and file_mode(p) != only_mode:
                continue
            rec.files.append(str(p))
            with open(p, newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    key = (r["session"], r["trade_id"])
                    t = trades.setdefault(key, {"state": None, "slug": r["slug"], "window_end": None, "held": 0.0, "cost": 0.0})
                    ev, day = r["event"], r["time_utc"][:10]
                    if ev == "ENTRY_SENT":
                        rec.sent_utc.append(parse_utc(r["time_utc"]))
                    if r["window_end_utc"]:
                        t["window_end"] = parse_utc(r["window_end_utc"])
                    if r["state_to"]:
                        t["state"] = r["state_to"]
                    if r["held_shares"] not in ("", None):
                        t["held"] = float(r["held_shares"])
                    if ev == "ENTRY_FILL" and r["shares"] and r["price"]:
                        t["cost"] = float(r["shares"]) * float(r["price"]) + float((json.loads(r["detail"]) if r["detail"] else {}).get("fee", 0.0))
                    if r["state_to"] == "SETTLED" and r["net_usd"]:
                        rec.net_by_day[day] = rec.net_by_day.get(day, 0.0) + float(r["net_usd"])
                    if ev == "STUCK":
                        rec.stuck_by_day[day] = rec.stuck_by_day.get(day, 0) + 1
        for key, t in trades.items():
            st = t["state"]
            if st in WORKING or (st in HELD and t["held"] > 1e-9):
                rec.unresolved.append({"key": "/".join(key), "slug": t["slug"], "state": st, "window_end": t["window_end"] or 0.0,
                                       "held": t["held"], "worst_case_usd": -t["cost"]})
    except Exception as e:                       # unreadable log: never assume flat
        rec.ok, rec.error = False, repr(e)
    return rec
