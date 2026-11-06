"""ExecEngine: the frozen engine with the dry-run layer attached. engine.py is NOT modified; this subclass (same pattern as
robustness_core.XEngine) only OBSERVES: it never changes an engine input, state or output. The layer runs inside try/except (`guard`)."""
from ..engine import Engine


class ExecEngine(Engine):
    def __init__(self, params, sink, layer):
        super().__init__(params, sink)
        if not self.v31:
            raise ValueError("the dry-run layer is built for strategy v3.1")
        self.layer = layer
        layer.bind(self)

    def _at(self, due_mono, fn, *args):
        super()._at(due_mono, fn, *args)
        if fn == self._taker_arrive:                                  # a paper decision: the base-latency arrival timer was just set
            t, kind = args
            if kind == "base" and t.main and t.track == self.layer.cfg.main_track:
                self.layer.guard("on_decision", self.layer.on_decision, t)

    def handle(self, feed, seq, wall_ns, mono_ns, exch_ts, obj):
        L = self.layer
        L.guard("before_record", L.before_record, mono_ns)                                # layer timers see the book BEFORE this record, like the engine's own timers
        try:
            super().handle(feed, seq, wall_ns, mono_ns, exch_ts, obj)
        finally:
            L.guard("after_record", L.after_record, feed, wall_ns, mono_ns, exch_ts, obj)

    def finalize(self):
        super().finalize()
        self.layer.guard("finalize", self.layer.finalize)


class LeanSink:
    """LIVE lean mode: drops the tables only shadow analysis needs (series, candidate log, maker shadow); everything else passes through."""
    DROPPED = ("series", "candidates", "maker_shadow")

    def __init__(self, inner):
        self._inner = inner

    def write(self, table, row):
        if table not in self.DROPPED:
            self._inner.write(table, row)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class LeanExecEngine(ExecEngine):
    """LIVE lean mode (latency): only the main track (t4) is evaluated. The shadow thresholds, the in-move shadow track and the maker shadow are
    switched off by trimming `tracks` / overriding the maker and series hooks; none of them feeds a main-track decision (checked by
    tests/test_live.py: identical main-track decisions with and without). The +5 s / +10 s paper shadow EXITS of the main track are left on:
    they are the paper engine's own bookkeeping of the same entry, run on timers (not on the decision -> send path), and the frozen summary code
    expects their columns."""

    def __init__(self, params, sink, layer):
        super().__init__(params, LeanSink(sink), layer)
        self.tracks = [t for t in self.tracks if t.main]
        self.has_inmove = False

    def _series(self, *a, **k):
        pass

    def _maker_step(self, *a, **k):
        pass

    def _maker_print(self, *a, **k):
        pass

    def _maker_close(self, *a, **k):
        pass
