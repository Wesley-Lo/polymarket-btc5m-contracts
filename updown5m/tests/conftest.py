"""Safety net for every test: no test may read or write the REAL key store (Windows Credential Manager / Secret Manager) or reach a real exchange
through the LIVE start-up path. A test that needs keys installs its own fake (monkeypatch lc._kr / lc.missing_keys / lc.load_keys ...), which
replaces this guard for the length of that test."""
import pytest

from updown5m.execution import live_client as lc


class _BlockedStore:
    def get_password(self, service, name):
        raise AssertionError("a test tried to read the REAL key store; install a fake (monkeypatch lc._kr)")

    def set_password(self, service, name, value):
        raise AssertionError("a test tried to write the REAL key store")


@pytest.fixture(autouse=True)
def no_real_key_store(monkeypatch):
    monkeypatch.setattr(lc, "_STORE", _BlockedStore())
    yield
