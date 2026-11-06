"""Redaction layer for LIVE: sensitive VALUES (registered at startup by live_client) are replaced by `[REDACTED:<label>]` in everything that is
logged, printed, e-mailed or written to a file. Matching is on exact values (a market's condition id is also 64 hex digits, so a "looks like a
key" pattern would destroy normal data); values shorter than 6 characters are never registered (too ambiguous).

`install()` registers the scrubber as transport's text filter, so EVERY text write of the package is covered, not just the ones that remember to call it."""
import re
import threading

from .. import transport

_MIN_LEN = 6


class Redactor:
    def __init__(self):
        self._vals = {}
        self._lock = threading.Lock()
        self._rx = None

    def register(self, value, label):
        v = None if value is None else str(value)
        if not v or len(v) < _MIN_LEN:
            return
        with self._lock:
            self._vals[v] = label
            for variant in (v[2:] if v.lower().startswith("0x") else "0x" + v,):      # a key shown with / without its 0x prefix
                if len(variant) >= _MIN_LEN:
                    self._vals.setdefault(variant, label)
            self._rx = re.compile("|".join(re.escape(x) for x in sorted(self._vals, key=len, reverse=True)))

    def scrub(self, text):
        rx = self._rx
        if rx is None or not isinstance(text, str):
            return text
        return rx.sub(lambda m: f"[REDACTED:{self._vals.get(m.group(0), 'x')}]", text)

    def scrub_obj(self, obj):
        if isinstance(obj, str):
            return self.scrub(obj)
        if isinstance(obj, dict):
            return {self.scrub(k) if isinstance(k, str) else k: self.scrub_obj(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return type(obj)(self.scrub_obj(v) for v in obj)
        return obj

    def clear(self):
        with self._lock:
            self._vals.clear()
            self._rx = None


REDACTOR = Redactor()


def install():
    transport.set_text_filter(REDACTOR.scrub)


def uninstall():
    transport.set_text_filter(None)
    REDACTOR.clear()


def safe_print(*a, **kw):
    print(*[REDACTOR.scrub(x) if isinstance(x, str) else x for x in a], **kw)
