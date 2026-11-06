"""python -m updown5m.execution.keys <check|set NAME|derive>   (no secret value is ever printed)

  check [--online]  which of the 7 keys exist (present / missing only), the signer address derived from `pk` (public), and with --online whether the
                    wallet the SDK derives for that key equals the stored `funder` (the account wallet) and its signature type
  set NAME          hidden-input prompt that stores one key in the Windows Credential Manager (service `updown5m`)
  derive            ask the SDK to derive the API credentials from `pk` and store api_key / api_secret / api_passphrase
  preflight         READ-ONLY pre-flight: every start-up check (keys, STOP files, geoblock, account, balance, allowances, open orders, leftover positions
                    reported but NOT sold) plus the clock offset and CLOB round trip. Places no order.
  push-gcp          LAPTOP only: copy the stored keys into Google Secret Manager (each value piped to gcloud over stdin; never shown)
The backend (Windows Credential Manager `keyring`, or Google Secret Manager `gcp` on the VM) comes from live_config.json `key_store`.
"""
import argparse

from .. import transport
from . import live_client as lc
from .live_config import load_live_config
from .redact import install


def check(online=False):
    cfg = load_live_config()
    print(f"secrets backend: {cfg.key_store}")
    missing = lc.missing_keys()
    for n in lc.KEY_NAMES:
        print(f"  {n:20s} {'MISSING' if n in missing else 'present'}")
    if "pk" in missing:
        print("signer address: cannot derive (pk missing)")
        return False
    print(f"signer address (public): {lc.public_address_of_stored_key()}")
    ok = not missing
    funder = lc.read_key("funder")
    if funder:
        print(f"configured funder (public): {funder}")
    st = lc.read_key("signature_type")
    if st and str(st).strip() != str(cfg.account_kind):
        print(f"WARNING: stored signature_type {st} differs from live_config.json ({cfg.account_kind})")
        ok = False
    if online:
        w = lc.probe_account_address()
        same = bool(funder) and w.lower() == funder.lower()
        print(f"wallet the SDK derives for this key (public): {w}")
        print(f"matches the stored funder: {'YES' if same else 'NO'}")
        ok = ok and same
    else:
        print("(run with --online to compare the SDK-derived wallet with `funder`)")
    print("RESULT:", "OK" if ok else "NOT READY (missing keys or mismatch)")
    return ok


def preflight():
    """Everything LIVE checks before it starts, without selling or sending anything."""
    from . import preflight as pf
    from .plumbing_test import clock_and_latency, clock_check
    cfg = load_live_config()
    print(f"secrets backend: {cfg.key_store}")
    miss = lc.missing_keys()
    if miss:
        print("keys MISSING: " + ", ".join(miss))
        return False
    held = lc.load_keys()
    client = lc.LiveClient(held, cfg).connect()
    try:
        pre = pf.static_preflight(cfg, client, held.get("funder"), transport.LOGS_ROOT.parent / "STOP", transport.LOGS_ROOT / cfg.rule_stop_file, flatten=False)
        print(pre.text())
        ok, line = clock_check(cfg, *clock_and_latency(transport.http_get_json))
        print(line)
    finally:
        client.close()
    print("RESULT:", "READY" if pre.ok and ok else "NOT READY")
    return pre.ok and ok


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m updown5m.execution.keys")
    ap.add_argument("cmd", choices=["check", "set", "derive", "push-gcp", "preflight"])
    ap.add_argument("name", nargs="?")
    ap.add_argument("--online", action="store_true")
    ap.add_argument("--project", default=None, help="push-gcp: the Google Cloud project id")
    ap.add_argument("--prefix", default="updown5m-", help="push-gcp: secret id prefix (default updown5m-)")
    a = ap.parse_args(argv)
    install()
    try:
        _dispatch(a)
    except RuntimeError as e:                       # a store problem (no credentials, no access, no network): one line, no traceback
        raise SystemExit(f"error: {e}")


def _dispatch(a):
    if a.cmd == "check":
        raise SystemExit(0 if check(a.online) else 1)
    if a.cmd == "preflight":
        raise SystemExit(0 if preflight() else 1)
    if a.cmd == "push-gcp":
        if not a.project:
            raise SystemExit("push-gcp needs --project <google cloud project id>")
        done = lc.push_to_gcp(a.project, a.prefix)
        print("pushed to Secret Manager (values not shown): " + ", ".join(done))
        return
    if a.cmd == "set":
        if a.name not in lc.KEY_NAMES:
            raise SystemExit(f"name must be one of {', '.join(lc.KEY_NAMES)}")
        lc.prompt_and_store_key(a.name)
    else:
        if "pk" in lc.missing_keys(("pk",)):
            raise SystemExit("store `pk` first")
        lc.derive_and_store_api_credentials()
        print("api_key / api_secret / api_passphrase derived and stored (values not shown)")


if __name__ == "__main__":
    main()
