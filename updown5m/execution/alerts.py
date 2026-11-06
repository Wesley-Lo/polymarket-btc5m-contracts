"""E-mail alerts (Gmail SMTP over SSL, to the owner only). Never blocks or crashes trading: `send()` only queues; one background thread does
the SMTP work; every failure is logged and swallowed. At most one mail per alert kind per `min_interval_s` (the daily summary is exempt).
The Gmail app password is read lazily by live_client (the only credential reader) when the first mail is sent.

    python -m updown5m.execution.alerts test      # sends one test mail"""
import queue
import threading
import time

from .. import transport
from . import live_client
from .redact import REDACTOR

EXEMPT = {"daily_summary", "test", "preflight"}


class Alerts:
    def __init__(self, cfg, log=print, send_fn=None, password_fn=None, clock=time.monotonic, start=True):
        self.cfg, self.log, self.clock = cfg, log, clock
        self._send = send_fn or transport.smtp_send
        self._pw = password_fn or live_client.mail_password
        self._last = {}
        self._q = queue.Queue()
        self.sent, self.failed, self.suppressed = [], 0, 0
        self._pending = 0
        self._lock = threading.Lock()
        self._thread = None
        if start:
            self._thread = threading.Thread(target=self._run, daemon=True, name="alerts")
            self._thread.start()

    def send(self, kind, subject, body=""):
        """Queue one alert. Returns True if queued, False if rate-limited. Never raises."""
        try:
            now = self.clock()
            if kind not in EXEMPT:
                last = self._last.get(kind)
                if last is not None and now - last < self.cfg.alert_min_interval_s:
                    self.suppressed += 1
                    return False
            self._last[kind] = now
            with self._lock:
                self._pending += 1
            self._q.put((kind, REDACTOR.scrub(f"[updown5m] {subject}"), REDACTOR.scrub(body)))
            return True
        except Exception as e:                                    # pragma: no cover
            self.log(f"[alerts] could not queue {kind}: {e!r}")
            return False

    def _run(self):
        while True:
            item = self._q.get()
            if item is None:
                return
            try:
                self._deliver(*item)
            finally:
                with self._lock:
                    self._pending -= 1

    def _deliver(self, kind, subject, body):
        try:
            self._send(self.cfg.smtp_host, self.cfg.smtp_port, self.cfg.alert_from, self._pw(), self.cfg.alert_to, subject, body)
            self.sent.append(kind)
        except Exception as e:
            self.failed += 1
            self.log(REDACTOR.scrub(f"[alerts] could not send {kind!r}: {type(e).__name__}: {e}"))

    def drain(self, timeout=20.0):
        """Wait (bounded) until queued mails were attempted; used at shutdown and by the CLI."""
        end = time.monotonic() + timeout
        while self._pending > 0 and time.monotonic() < end:
            time.sleep(0.05)

    def close(self):
        self.drain(10.0)
        self._q.put(None)


def main(argv=None):
    import argparse
    from .live_config import load_live_config
    ap = argparse.ArgumentParser(prog="python -m updown5m.execution.alerts")
    ap.add_argument("cmd", choices=["test"])
    a = ap.parse_args(argv)
    cfg = load_live_config()
    from .redact import install
    install()
    al = Alerts(cfg)
    al.send("test", "test alert", "This is a test e-mail from updown5m. If you can read it, alerts work. Nothing was traded.")
    al.drain(30.0)
    if al.failed:
        raise SystemExit("test e-mail FAILED (see the message above; check the app password and that 2-step verification + an app password exist)")
    print(f"test e-mail sent to {cfg.alert_to}")


if __name__ == "__main__":
    main()
