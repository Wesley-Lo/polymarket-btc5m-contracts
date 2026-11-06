"""VM sanity subset (these tests ship in the VM bundle; they need no network, no recordings and nothing outside the package):
frozen params hash, every module imports, no credential / signing / order code outside live_client.py, raw recording on vs off gives
byte-identical decisions and execution outputs, the vm profile, the Google Secret Manager key store (fake client) and the laptop->VM key push."""
import ast
import importlib
import json
import re
import shutil
import sys
import uuid
from pathlib import Path

import pytest

from updown5m import params as P
from updown5m import transport
from updown5m.execution import config as exec_config
from updown5m.execution import live_client as lc
from updown5m.execution import live_config
from updown5m.execution.config import Mode, load_config
from updown5m.execution.hook import ExecEngine
from updown5m.execution.layer import DryRunLayer
from updown5m.execution.outputs import Outputs
from updown5m.live import Pipeline
from updown5m.recorder import NullRecorder, RawRecorder
from updown5m.sink import CsvSink
from updown5m.tests import synth

PKG = Path(transport.__file__).parent


@pytest.fixture()
def workdir():
    d = Path("_tests") / ("vm_" + uuid.uuid4().hex[:8])
    yield d
    shutil.rmtree(transport.safe_path(d), ignore_errors=True)


# ================================================================= frozen params, imports, safety
def test_params_are_the_frozen_v31():
    assert P.sha256_of(P.load()) == P.sha256_of(P.load(profile="v3.1")) == P.FROZEN_SHA256["v3.1"]
    assert P.FROZEN_SHA256["v3.1"].startswith("0dd0e21c")


# The modules the VM runs: the transitive import graph of `python -m updown5m` (DRY_RUN and LIVE), execution.keys, execution.alerts,
# execution.plumbing_test and updown5m.params (static AST graph, confirmed by an import trace of a real DRY_RUN).
VM_MODULES = [
    "updown5m",
    "updown5m.book",
    "updown5m.engine",
    "updown5m.execution",
    "updown5m.execution.alerts",
    "updown5m.execution.clock",
    "updown5m.execution.config",
    "updown5m.execution.exchbook",
    "updown5m.execution.feeds",
    "updown5m.execution.fills",
    "updown5m.execution.hook",
    "updown5m.execution.keys",
    "updown5m.execution.layer",
    "updown5m.execution.live_client",
    "updown5m.execution.live_config",
    "updown5m.execution.live_layer",
    "updown5m.execution.liveio",
    "updown5m.execution.livereport",
    "updown5m.execution.livewire",
    "updown5m.execution.orders",
    "updown5m.execution.outputs",
    "updown5m.execution.plumbing_test",
    "updown5m.execution.preflight",
    "updown5m.execution.reconcile",
    "updown5m.execution.recover",
    "updown5m.execution.redact",
    "updown5m.execution.report",
    "updown5m.execution.risk",
    "updown5m.execution.rules",
    "updown5m.execution.sender",
    "updown5m.execution.state",
    "updown5m.live",
    "updown5m.model",
    "updown5m.params",
    "updown5m.recorder",
    "updown5m.sink",
    "updown5m.strategy_v3",
    "updown5m.summary",
    "updown5m.transport",
    "updown5m.util",
]


def test_every_vm_module_imports_and_the_bundle_holds_no_other_module():
    for n in VM_MODULES:
        importlib.import_module(n)
    importlib.import_module("updown5m.__main__")
    present = set()
    for f in PKG.rglob("*.py"):
        if "tests" in f.parts or "__pycache__" in f.parts or "logs" in f.parts:
            continue
        parts = ["updown5m"] + list(f.relative_to(PKG).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        present.add(".".join(parts))
    present.discard("updown5m.__main__")
    assert set(VM_MODULES) <= present
    if (PKG / "clock_recheck.py").exists():
        return                                    # the laptop checkout also holds the research tools
    assert present == set(VM_MODULES), sorted(present - set(VM_MODULES))


FORBIDDEN_NAME = re.compile(r"(?i)((?<![a-z])(sign|signer|signature|signed|signing)(?![a-z])|private_?key|secret|mnemonic|hmac|keccak|ecdsa|"
                            r"eth_account|web3|wallet|passphrase|api_?key|py_clob_client|dotenv)")
FORBIDDEN_URL = re.compile(r"(?i)(/(order|orders|cancel|cancel-orders|cancel-all|auth|heartbeat|heartbeats|rfq)(/|\?|$)|api[-_]key|derive-api)")
FORBIDDEN_IMPORTS = {"requests", "aiohttp", "urllib3", "web3", "eth_account", "eth_keys", "nacl", "cryptography", "dotenv", "py_clob_client",
                     "socket", "google", "keyring", "polymarket", "getpass"}


def test_credentials_signing_and_order_endpoints_exist_only_in_live_client():
    hits = []
    for f in sorted(PKG.rglob("*.py")):
        if "tests" in f.parts or "__pycache__" in f.parts or "logs" in f.parts or f.name == "live_client.py":
            continue
        tree = ast.parse(f.read_text(encoding="utf-8"))
        doc = {id(n.body[0].value) for n in ast.walk(tree) if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
               and n.body and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
        for n in ast.walk(tree):
            names = []
            if isinstance(n, ast.Name):
                names.append(n.id)
            elif isinstance(n, ast.Attribute):
                names.append(n.attr)
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(n.name)
            elif isinstance(n, (ast.arg, ast.keyword)) and getattr(n, "arg", None):
                names.append(n.arg)
            elif isinstance(n, (ast.Import, ast.ImportFrom)):
                for m in [a.name for a in n.names] + ([n.module] if isinstance(n, ast.ImportFrom) and n.module else []):
                    if m.split(".")[0] in FORBIDDEN_IMPORTS and not (f.name == "transport.py" and m == "smtplib"):
                        hits.append((f.name, n.lineno, "import", m))
                    names.append(m)
            elif isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc:
                if (n.value.startswith(("http", "/", "ws")) or "://" in n.value) and FORBIDDEN_URL.search(n.value):
                    hits.append((f.name, n.lineno, "url", n.value))
            hits += [(f.name, getattr(n, "lineno", 0), "name", x) for x in names
                     if FORBIDDEN_NAME.search(x) and not (f.name == "transport.py" and x in ("_FORBIDDEN_WS_KEYS", "_check_ws_payload"))]
    assert not hits, hits


# ================================================================= raw recording off changes nothing
def _vparams():
    p = P.load()
    p["gap_rule"] = {"enabled": True, "reconnect_block_s": 3, "require_rest_sync": False}
    p["strategy"] = {**p["strategy"], "move_bps": 2.0, "lookback_ms": 500}
    return p


def _run(workdir, recs, recorder):
    out_dir = transport.safe_path(workdir)
    transport.require_new_dir(out_dir)
    p = _vparams()
    sink = CsvSink(out_dir, P.markouts_of(p), P.flip_s_of(p), P.rev_of(p))
    out = Outputs(out_dir, "t")
    cfg = load_config(mode="DRY_RUN", clock_fixed_theta_ms=14.6)
    layer = DryRunLayer(cfg, out, "t", Mode.DRY_RUN, kill_exists_fn=lambda: False)
    eng = ExecEngine(p, sink, layer)
    synth.feed(Pipeline(eng, recorder), recs)
    eng.finalize()
    sink.close()
    out.close()
    if recorder is not None:
        recorder.close()
    return out_dir


def test_raw_recording_off_gives_byte_identical_decisions_and_execution_outputs(workdir):
    recs = synth.scenario_v3(jumps=[(40, 9.0), (130, -9.0), (200, 9.0)], noise=3e-5, rtts=[105])
    rec_on = RawRecorder(transport.safe_path(workdir) / "raw_on_only")
    on = _run(Path(workdir) / "on", recs, rec_on)
    off = _run(Path(workdir) / "off", recs, NullRecorder())
    none = _run(Path(workdir) / "none", recs, None)
    fa = {f.name for f in on.iterdir() if f.is_file()}
    assert fa == {f.name for f in off.iterdir() if f.is_file()} == {f.name for f in none.iterdir() if f.is_file()}
    assert {"live_events.csv", "live_orders.jsonl", "dryrun_vs_paper.csv", "dryrun_summary.txt"} <= fa
    for n in sorted(fa):
        assert (on / n).read_bytes() == (off / n).read_bytes() == (none / n).read_bytes(), n
    assert "DECISION" in (on / "live_events.csv").read_text(encoding="utf-8")          # the comparison is not vacuous
    assert any(transport.safe_path(workdir, "raw_on_only").rglob("*.jsonl.gz"))        # the recorder arm really wrote raw files


def test_null_recorder_has_the_recorder_interface_and_writes_nothing():
    r = NullRecorder()
    r.put("binance_book", 1, 1, 1, None, None, "{}")
    r.put("binance_book", 2, 2, 2, None, None, "{}")
    assert r.stats() == {"dropped": 0, "errors": 0, "queue_depth": 0, "bytes_written": 0, "counts": {"binance_book": 2}}
    assert r.dropped == 0 and r.mb_per_hour() is None
    r.close()


# ================================================================= vm profile and flags
def test_raw_record_is_not_part_of_the_config_hash():
    a, b = load_config(raw_record=True), load_config(raw_record=False)
    assert a.raw_record and not b.raw_record and a.sha256() == b.sha256()


def test_vm_profile_files_turn_raw_recording_off_and_gcp_on():
    vm_cfg, vm_live = PKG / "execution" / "config_vm.json", PKG / "execution" / "live_config_vm.json"
    if not vm_cfg.exists():
        pytest.skip("bundle: the vm profile is shipped as the standard config.json / live_config.json (checked in the next test)")
    c, l = load_config(vm_cfg), live_config.load_live_config(vm_live)
    assert c.raw_record is False and c.mode == "DRY_RUN" and l.armed is False and l.key_store == "gcp"
    assert load_config().raw_record is True                                           # the laptop default is unchanged


def test_bundle_standard_config_files_are_the_vm_ones_and_disarmed():
    if (PKG / "execution" / "config_vm.json").exists():
        pytest.skip("laptop checkout: the standard files are the laptop's")
    c, l = load_config(), live_config.load_live_config()
    assert c.raw_record is False and c.mode in ("DRY_RUN", "OFF") and l.armed is False and l.key_store == "gcp"
    assert exec_config.profile_file("vm") == exec_config.CONFIG_FILE and live_config.profile_file("vm") == live_config.LIVE_CONFIG_FILE   # --profile vm still works


def _main(monkeypatch, *argv):
    from updown5m import __main__ as m
    monkeypatch.setattr(exec_config, "PROFILE", "default")           # main() selects a profile globally; restore it after the test
    monkeypatch.setattr(live_config, "PROFILE", "default")
    monkeypatch.setattr(sys, "argv", ["updown5m", *argv])
    return m.main()


def test_flags_raw_record_and_no_raw_record_are_mutually_exclusive(monkeypatch):
    with pytest.raises(SystemExit) as e:
        _main(monkeypatch, "--raw-record", "--no-raw-record")
    assert e.value.code == 2


def test_live_and_vm_profile_refuse_params_that_are_not_the_frozen_v31(monkeypatch, workdir):
    d = P.load()
    d["strategy"] = {**d["strategy"], "move_bps": 99.0}
    alt = transport.safe_path(workdir, "alt_params.json")
    alt.parent.mkdir(parents=True, exist_ok=True)
    alt.write_text(json.dumps(d), encoding="utf-8")
    with pytest.raises(SystemExit) as e:
        _main(monkeypatch, "--params", str(alt), "--exec-mode", "LIVE", "--i-accept-real-money")
    assert "frozen v3.1" in str(e.value)
    with pytest.raises(SystemExit) as e:
        _main(monkeypatch, "--params", str(alt), "--exec-mode", "DRY_RUN", "--profile", "vm")
    assert "frozen v3.1" in str(e.value)


def test_vm_profile_refuses_any_strategy_but_v31(monkeypatch):
    with pytest.raises(SystemExit) as e:
        _main(monkeypatch, "--strategy", "v2", "--exec-mode", "OFF", "--profile", "vm")
    assert "v3.1 only" in str(e.value)


# ================================================================= Google Secret Manager key store, key push
class _Resp:
    def __init__(self, v):
        self.payload = type("P", (), {"data": v.encode()})()


class _FakeSM:
    def __init__(self, store):
        self.store, self.calls = store, []

    def access_secret_version(self, request):
        self.calls.append(request["name"])
        from google.api_core import exceptions as gx
        if request["name"] not in self.store:
            raise gx.NotFound("nope")
        v = self.store[request["name"]]
        if isinstance(v, Exception):
            raise v
        return _Resp(v)


def test_gcp_key_store_reads_latest_version_caches_and_is_read_only():
    pytest.importorskip("google.api_core")
    base = "projects/proj/secrets/updown5m-"
    fake = _FakeSM({base + "pk/versions/latest": "0xabc\n", base + "funder/versions/latest": "0xdef"})
    st = lc.GcpStore("proj", "updown5m-")
    st._c = fake
    assert st.get_password("updown5m", "pk") == "0xabc" and st.get_password("updown5m", "pk") == "0xabc"
    assert st.get_password("updown5m", "api_key") is None                   # missing -> None, like keyring
    assert fake.calls.count(base + "pk/versions/latest") == 1               # in-memory cache, one read per process
    with pytest.raises(RuntimeError):
        st.set_password("updown5m", "pk", "x")


def test_gcp_permission_denied_message_names_the_secret_and_role_but_no_value():
    gx = pytest.importorskip("google.api_core.exceptions")
    st = lc.GcpStore("proj", "updown5m-")
    st._c = _FakeSM({"projects/proj/secrets/updown5m-pk/versions/latest": gx.PermissionDenied("denied")})
    with pytest.raises(RuntimeError) as e:
        st.get_password("updown5m", "pk")
    assert "updown5m-pk" in str(e.value) and "secretAccessor" in str(e.value)


def test_key_store_follows_the_config(monkeypatch):
    monkeypatch.setattr(lc, "_STORE", None)
    monkeypatch.setattr(live_config, "load_live_config", lambda path=None, **kw: live_config.LiveConfig(key_store="gcp", gcp_project="p"))
    st = lc._kr()
    assert isinstance(st, lc.GcpStore) and st.project == "p"
    monkeypatch.setattr(lc, "_STORE", None)
    monkeypatch.setattr(live_config, "load_live_config", lambda path=None, **kw: live_config.LiveConfig(key_store="nonsense"))
    with pytest.raises(ValueError):
        lc._kr()


def test_key_set_and_derive_refuse_on_the_gcp_backend(monkeypatch):
    monkeypatch.setattr(lc, "backend_name", lambda: "gcp")
    with pytest.raises(SystemExit):
        lc.prompt_and_store_key("pk")
    with pytest.raises(SystemExit):
        lc.derive_and_store_api_credentials()
    with pytest.raises(SystemExit):
        lc.push_to_gcp("proj")


def test_push_to_gcp_pipes_values_over_stdin_only(monkeypatch):
    import subprocess
    store = {n: "VALUE-" + n for n in lc.KEY_NAMES}
    monkeypatch.setattr(lc, "backend_name", lambda: "keyring")
    monkeypatch.setattr(lc, "read_key", lambda n: store.get(n))
    seen = []

    def fake_run(cmd, input=None, capture_output=None, text=None):
        seen.append((list(cmd), input))
        rc = 1 if cmd[2] == "describe" else 0                               # no secret exists yet -> create
        return subprocess.CompletedProcess(cmd, rc, stdout="" if text else b"", stderr="" if text else b"")
    monkeypatch.setattr(subprocess, "run", fake_run)
    import shutil
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/gcloud" if name == "gcloud" else None)
    done = lc.push_to_gcp("proj")
    assert done == ["updown5m-" + n for n in lc.KEY_NAMES]
    creates = [(c, i) for c, i in seen if c[2] == "create"]
    assert len(creates) == 7
    for cmd, inp in creates:
        assert "--data-file=-" in cmd and inp is not None
        assert not any("VALUE-" in a for a in cmd)                          # no value in argv (so none in shell history / process list)


def test_push_to_gcp_resolves_gcloud_via_which(monkeypatch):
    import shutil
    import subprocess
    store = {n: "VALUE-" + n for n in lc.KEY_NAMES}
    monkeypatch.setattr(lc, "backend_name", lambda: "keyring")
    monkeypatch.setattr(lc, "read_key", lambda n: store.get(n))
    seen = []

    def fake_run(cmd, input=None, capture_output=None, text=None):
        seen.append((list(cmd), input))
        return subprocess.CompletedProcess(cmd, 0, stdout="" if text else b"", stderr="" if text else b"")
    monkeypatch.setattr(subprocess, "run", fake_run)
    cmd_path = r"C:\SDK\google-cloud-sdk\bin\gcloud.cmd"
    # which("gcloud") resolves to gcloud.cmd via PATHEXT (the Windows case)
    monkeypatch.setattr(shutil, "which", lambda name: cmd_path if name == "gcloud" else None)
    lc.push_to_gcp("proj")
    assert seen and all(c[0] == cmd_path for c, _ in seen)
    for cmd, inp in seen:
        assert not any("VALUE-" in a for a in cmd)                          # values still go over stdin only
    assert all(inp is not None for c, inp in seen if c[2] == "versions")
    # which("gcloud") finds nothing, fall back to which("gcloud.cmd")
    seen.clear()
    monkeypatch.setattr(shutil, "which", lambda name: cmd_path if name == "gcloud.cmd" else None)
    lc.push_to_gcp("proj")
    assert seen and all(c[0] == cmd_path for c, _ in seen)
    # neither is found: a clear one-line error and no subprocess call at all
    seen.clear()
    monkeypatch.setattr(shutil, "which", lambda name: None)
    with pytest.raises(SystemExit) as e:
        lc.push_to_gcp("proj")
    assert "gcloud" in str(e.value) and "PATH" in str(e.value)
    assert seen == []


def test_key_store_problems_are_one_line_errors_not_tracebacks(monkeypatch, capsys):
    pytest.importorskip("google.api_core")
    st = lc.GcpStore("proj", "updown5m-")
    st._c = _FakeSM({"projects/proj/secrets/updown5m-pk/versions/latest": ConnectionError("network down")})
    with pytest.raises(RuntimeError) as e:
        st.get_password("updown5m", "pk")
    assert "could not read updown5m-pk" in str(e.value)
    from updown5m.execution import keys
    monkeypatch.setattr(lc, "_STORE", st)
    monkeypatch.setattr(lc, "backend_name", lambda: "gcp")
    with pytest.raises(SystemExit) as x:
        keys.main(["check"])
    assert str(x.value).startswith("error: could not read updown5m-pk") and "Traceback" not in capsys.readouterr().out


def test_live_start_refuses_with_exit_78_when_the_key_store_is_unreachable(monkeypatch):
    from updown5m.execution import livewire
    monkeypatch.setattr(lc, "missing_keys", lambda names=lc.KEY_NAMES: (_ for _ in ()).throw(RuntimeError("no credentials")))
    with pytest.raises(livewire.ArmingError) as e:
        livewire.prepare(live_config.LiveConfig(armed=True))
    assert "cannot read the keys: no credentials" in str(e.value)
