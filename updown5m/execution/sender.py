"""The order path. The decision hands a `Job` to this thread with one queue.put; this thread builds + signs + posts it through the live client
and puts the result on `results` for the event loop to apply. Nothing else happens between the decision and the POST: no disk, no e-mail, no
logging (the 'sent' record is queued AFTER the POST returned). A stale entry (signal older than `max_signal_age_ms` when it is picked up) is
never sent. An entry is sent at most once: there is no retry here, and a timeout is reported as `unknown`, never re-sent."""
import queue
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal

from .live_client import SendResult

NS = 1_000_000


@dataclass
class Job:
    key: int                         # the trade id (or a negative id for a flatten order)
    leg: str                         # ENTRY | EXIT | FLATTEN
    attempt: int
    side: str                        # BUY | SELL
    token: str
    usd: Decimal = Decimal(0)        # BUY: dollars to spend
    shares: Decimal = Decimal(0)     # SELL: shares to sell (BUY: the intended shares, for the record)
    price: Decimal = Decimal(0)      # BUY: max price (the strategy's limit); SELL: min price (the floor)
    client_oid: str = ""
    t_decision_ns: int = 0
    t_queued_ns: int = 0
    t_picked_ns: int = 0
    max_age_ms: float = 500.0
    meta: dict = field(default_factory=dict)


class Sender:
    def __init__(self, client, results=None, on_sent=None, sync=False):
        self.client = client
        self.results = results if results is not None else queue.SimpleQueue()
        self.on_sent = on_sent
        self.sync = sync
        self._q = queue.SimpleQueue()
        self._thread = None
        self.in_flight = 0
        if not sync:
            self._thread = threading.Thread(target=self._run, daemon=True, name="order-sender")
            self._thread.start()

    def submit(self, job):
        job.t_queued_ns = time.perf_counter_ns()
        self.in_flight += 1
        if self.sync:
            self._do(job)
        else:
            self._q.put(job)

    def _run(self):
        while True:
            job = self._q.get()
            if job is None:
                return
            self._do(job)

    def _do(self, job):
        job.t_picked_ns = time.perf_counter_ns()
        try:
            if job.leg == "ENTRY" and (job.t_picked_ns - job.t_decision_ns) / NS > job.max_age_ms:
                res = SendResult(kind="error", code="signal_too_old", message="entry signal older than the limit; not sent",
                                 t_build_ns=job.t_picked_ns, t_built_ns=job.t_picked_ns, t_send_ns=job.t_picked_ns, t_resp_ns=job.t_picked_ns)
            elif job.side == "BUY":
                res = self.client.buy(job.token, job.usd, job.price)
            else:
                res = self.client.sell(job.token, job.shares, job.price)
        except Exception as e:                                    # a client bug must not kill the thread; we cannot know whether it was sent
            t = time.perf_counter_ns()
            res = SendResult(kind="unknown", sent=True, code=type(e).__name__, message=str(e)[:200], t_build_ns=job.t_picked_ns,
                             t_built_ns=job.t_picked_ns, t_send_ns=job.t_picked_ns, t_resp_ns=t)
        self.in_flight -= 1
        self.results.put((job, res))
        if self.on_sent is not None:
            try:
                self.on_sent(job, res)
            except Exception:
                pass

    def close(self):
        if self._thread is not None:
            self._q.put(None)
            self._thread.join(5.0)
