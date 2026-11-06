"""Execution-layer configuration (execution/config.json). The strategy parameters (params.json, the frozen v3.1 hash) are NOT touched."""
import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path

CONFIG_FILE = Path(__file__).resolve().parent / "config.json"
PROFILE = "default"                       # "vm": execution/config_vm.json (no raw feed recording); chosen by `--profile vm`


def use_profile(name):
    global PROFILE
    if name not in ("default", "vm"):
        raise ValueError(f"unknown profile {name!r}; use default or vm")
    PROFILE = name


def profile_file(name=None):
    name = name or PROFILE
    if name == "default":
        return CONFIG_FILE
    f = CONFIG_FILE.with_name(f"config_{name}.json")
    return f if f.exists() else CONFIG_FILE          # the VM bundle ships the vm profile under the standard name


class Mode(str, Enum):
    OFF = "OFF"
    DRY_RUN = "DRY_RUN"
    LIVE = "LIVE"


class LiveTradingNotBuilt(RuntimeError):
    """Kept for import compatibility: the dry-run build raised this for LIVE. LIVE is now built, behind explicit arming (see livewire.py)."""


def resolve_mode(value):
    """Mode from a string/enum. LIVE resolves, but LIVE only STARTS through livewire.check_arming (flag + armed config) and the pre-flight."""
    try:
        return value if isinstance(value, Mode) else Mode(str(value).strip().upper())
    except ValueError:
        raise ValueError(f"unknown execution mode {value!r}; use OFF, DRY_RUN or LIVE") from None


@dataclass
class ExecConfig:
    mode: str = "OFF"
    main_track: str = "t4"
    size_usd: float = 10.0
    max_open_positions: int = 1
    max_entries_per_hour: int = 30
    daily_loss_limit_usd: float = 25.0
    stuck_limit_per_day: int = 2
    cooldown_after_close_s: float = 3.0
    cooldown_after_miss_s: float = 1.0
    send_delay_ms: float = 0.0
    exit_steps_s: list = field(default_factory=lambda: [2, 5, 10])
    exit_floor_pts: float = 5.0
    stuck_time_left_s: float = 15.0
    entry_timeout_s: float = 5.0
    exit_timeout_s: float = 5.0
    feed_reconnect_block_s: float = 3.0
    kill_switch_file: str = "STOP"
    kill_switch_poll_s: float = 1.0
    clock_max_disagreement_ms: float = 25.0
    clock_rtt_band_ms: float = 30.0
    clock_window_s: float = 1800
    clock_min_samples: int = 5
    clock_fixed_theta_ms: float = None      # replays of sessions recorded before clock_offset_v2 existed: skip the check, use this theta
    exch_buffer_s: float = 45.0
    exch_settle_s: float = 3.0
    recover_max_age_h: float = 24
    recovered_release_s: float = 900
    clock_v2_period_s: float = 10.0
    raw_record: bool = True                 # False (the `vm` profile): do not write the raw feed recordings. Not part of sha256(): it changes no decision or output

    def sha256(self):
        d = asdict(self)
        d.pop("raw_record")
        return hashlib.sha256(json.dumps(d, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_config(path=None, **overrides):
    with open(path or profile_file(), encoding="utf-8") as f:
        d = json.load(f)
    unknown = set(d) | set(overrides)
    unknown -= set(ExecConfig.__dataclass_fields__)
    if unknown:
        raise ValueError(f"unknown execution config keys: {sorted(unknown)}")
    d.update({k: v for k, v in overrides.items() if v is not None})
    return ExecConfig(**d)
