"""LIVE pilot configuration (execution/live_config.json). `armed` ships as false: LIVE only starts when it is true AND `--exec-mode LIVE`
AND `--i-accept-real-money` AND the startup pre-flight passes. The strategy parameters (params.json, frozen v3.1 hash) are NOT touched."""
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

LIVE_CONFIG_FILE = Path(__file__).resolve().parent / "live_config.json"
PROFILE = "default"                       # "vm": execution/live_config_vm.json (secrets from Google Secret Manager); chosen by `--profile vm`


def use_profile(name):
    global PROFILE
    if name not in ("default", "vm"):
        raise ValueError(f"unknown profile {name!r}; use default or vm")
    PROFILE = name


def profile_file(name=None):
    name = name or PROFILE
    if name == "default":
        return LIVE_CONFIG_FILE
    f = LIVE_CONFIG_FILE.with_name(f"live_config_{name}.json")
    return f if f.exists() else LIVE_CONFIG_FILE          # the VM bundle ships the vm profile under the standard name


@dataclass
class LiveConfig:
    armed: bool = False
    # --- sizing and limits (pilot)
    budget_usd: float = 5.0                  # cap per entry; shares = floor(budget / limit); the exchange minimum is 5 shares
    min_shares: float = 5.0
    max_entries_per_hour: int = 20
    daily_loss_limit_usd: float = 25.0       # realised, from real fills and real fees
    stuck_limit_per_day: int = 2
    position_alert_usd: float = 15.0
    reject_burst_n: int = 3
    reject_burst_window_s: float = 600.0
    max_signal_age_ms: float = 500.0         # an entry is never sent (or retried) when its signal is older than this
    build_alert_ms: float = 20.0             # a real order whose build + sign took longer than this is logged SLOW_SIGN and e-mailed
    # --- reconciliation
    reconcile_every_s: float = 30.0
    settle_poll_every_s: float = 1.0
    settle_timeout_s: float = 90.0
    unknown_resolve_s: float = 8.0           # an order with no answer is UNKNOWN; look it up in the trade history after this long
    position_tolerance_shares: float = 0.011
    # --- pre-registered pilot stop rules (B7)
    rule_checkpoint_trades: int = 50
    rule_s1_min_fill_rate: float = 0.15
    rule_s2_min_net_per_10usd: float = 0.50  # mean live net per trade below this (scaled by budget/10) stops the pilot
    rule_s3_min_diff_per_5usd: float = -0.75  # mean (live net - twin net) per trade below this (scaled by budget/5) stops the pilot
    rule_stop_file: str = "RULE_STOP"
    # --- pre-flight
    min_collateral_usd: float = 10.0
    clock_max_abs_offset_ms: float = 100.0
    latency_max_ms: float = 1000.0
    preflight_runtime_timeout_s: float = 120.0
    account_kind: int = 3                  # Deposit Wallet (docs: every account wallet deployed on or after 2026-05-04)
    conditional_asset_type: str = "CONDITIONAL"
    # --- alerts
    alert_to: str = "you@example.com"
    alert_from: str = "you@example.com"
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 465
    alert_min_interval_s: float = 600.0
    # --- lean mode / misc
    lean: bool = True
    # --- where the 7 secrets live: "keyring" (Windows Credential Manager, the laptop) or "gcp" (Google Secret Manager, the VM)
    key_store: str = "keyring"
    gcp_project: str = ""                    # "" = the project the VM runs in (from its metadata)
    gcp_key_prefix: str = "updown5m-"     # secret ids are <prefix><key name>, e.g. updown5m-pk
    plumbing_min_gap_s: float = 600.0
    plumbing_ask_min: float = 0.30
    plumbing_ask_max: float = 0.70
    plumbing_max_spread_pts: float = 2.0

    def sha256(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @property
    def s2_threshold(self):
        return self.rule_s2_min_net_per_10usd * self.budget_usd / 10.0

    @property
    def s3_threshold(self):
        return self.rule_s3_min_diff_per_5usd * self.budget_usd / 5.0


def load_live_config(path=None, **overrides):
    with open(path or profile_file(), encoding="utf-8") as f:
        d = json.load(f)
    unknown = (set(d) | set(overrides)) - set(LiveConfig.__dataclass_fields__)
    if unknown:
        raise ValueError(f"unknown live config keys: {sorted(unknown)}")
    d.update({k: v for k, v in overrides.items() if v is not None})
    return LiveConfig(**d)
