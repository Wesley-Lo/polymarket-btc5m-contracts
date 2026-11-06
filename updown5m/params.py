"""Single params file loader. All tunables live in params.json; every start saves a copy plus its SHA-256.

The shipped params.json holds the CURRENT default strategy (v3.1). v1, v2 and v3.0 stay selectable as frozen profiles
(`load(profile="v1" | "v2" | "v3.0")`, `python -m updown5m --strategy v1|v2|v3.0|v3.1`): a profile overlays only the keys that differ."""
import copy
import hashlib
import json
from pathlib import Path

PARAMS_FILE = Path(__file__).resolve().parent / "params.json"

# Frozen definitions of the earlier strategies (what they were when they were the shipped default).
PROFILES = {
    "v1": {"model": {"version": "v1"}, "gap_rule": {"enabled": False}, "strategy": {"version": 1}},
    # v3.0 = the first v3 (anchor + predicted change, hold main track, 3 entries/window), exactly as shipped on 2026-10-31
    "v3.0": {"strategy": {"version": 3, "move_bps": 1.0, "lookback_ms": 250, "anchor": "mid", "target_mode": "logit", "beta": 1.0,
                          "thresholds_pts": [6, 4, 3, 2], "main_threshold_pts": 4, "max_entries_per_window": 3,
                          "min_entry_spacing_s": 15, "max_spread_pts": 4.0, "max_book_age_s": 5.0, "max_attempts_per_window": 6,
                          "flip_s": [2, 5, 10], "markouts_s": [0.5, 1, 2, 5, 10], "pm_keep_s": 10.0}},
    "v2": {"model": {"version": "v2", "twap_s": 60, "dist": "normal", "t_df": 5.0, "sigma_mult": 1.3, "basis_bps": 0.0,
                     "min_var_s": 1.0, "max_filled_s": 5, "ptb_mode": "proxy", "ptb_basis_halflife_windows": 6,
                     "ptb_basis_min_windows": 3, "use_coinbase": False, "coinbase_basis_halflife_s": 300, "coinbase_stale_s": 5},
           "gap_rule": {"enabled": True, "reconnect_block_s": 3, "require_rest_sync": True},
           "strategy": {"version": 2}},
}


# SHA-256 (canonical JSON, see sha256_of) of the complete params of each frozen strategy. tests/test_v31.py compares these: a change to a
# frozen strategy's parameters must be a deliberate new version, never an edit. v3.1 is frozen until the long run has been evaluated.
FROZEN_SHA256 = {
    "v3.0": "410ac1613cf88fcd2166863178eaba97be93a8388fc22dad5c85f4aca57c1434",
    "v3.1": "0dd0e21c099623d88172b693dc99b3dc7ef8978cac52b5f74875f0cd9013a585",
}


class AttrDict(dict):
    def __getattr__(self, k):
        try:
            v = self[k]
        except KeyError:
            raise AttributeError(k) from None
        return AttrDict(v) if isinstance(v, dict) else v


def canonical(d):
    return json.dumps(d, sort_keys=True, separators=(",", ":"))


def sha256_of(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()


def load(path=None, profile=None):
    with open(path or PARAMS_FILE, encoding="utf-8") as f:
        p = AttrDict(json.load(f))
    if profile in ("v3", "v3.1"):          # the shipped default
        profile = None
    if profile:
        for k, v in copy.deepcopy(PROFILES[profile]).items():
            p[k] = v
    return p


# ---- strategy-specific views of a params dict (absent keys = the original behaviour, so old sessions replay unchanged)
def is_v3(p):
    return int((p.get("strategy") or {}).get("version", 2)) == 3


def markouts_of(p):
    return list(p["strategy"].get("markouts_s", p["markouts_s"])) if is_v3(p) else list(p["markouts_s"])


def rev_of(p):
    """0 = v3.0 rules, 1 = v3.1 rules (flip-aware edge, flip_2s main track, single position, first-detection entries)."""
    return int((p.get("strategy") or {}).get("rev", 0)) if is_v3(p) else 0


def flip_s_of(p):
    """Flip-exit horizons (v3 only; None for v1/v2 so their output tables are unchanged)."""
    return list(p["strategy"].get("flip_s", [2, 5, 10])) if is_v3(p) else None


if __name__ == "__main__":      # python -m updown5m.params  -> the params hash of every selectable strategy
    for name in ("v1", "v2", "v3.0", "v3.1"):
        h = sha256_of(load(profile=name))
        frozen = FROZEN_SHA256.get(name)
        print(f"{name:5s} sha256 {h}" + ("" if frozen is None else ("   FROZEN: matches" if frozen == h else "   FROZEN: MISMATCH (expected %s)" % frozen)))
