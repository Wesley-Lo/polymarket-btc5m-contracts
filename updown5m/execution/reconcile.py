"""Reconciliation: trust the exchange, not memory. A worker thread asks the exchange (through the live client, read-only calls) and posts plain
messages to `inbox` for the event loop:

  ("trades", key, leg, attempt, [TradeInfo], final)   the trade history of one order; `final` once every trade is CONFIRMED / FAILED (or timed out)
  ("lookup", key, leg, attempt, [TradeInfo])          answer to an UNKNOWN order: every trade of that token since the order was sent
  ("snapshot", positions, open_orders, error)         the account's open BTC-5m positions and open orders (FAK never rests: any open order is a mismatch)
  ("cash", balance_usd)                               collateral balance, for the cash audit

Nothing here sends an order."""
import queue
import threading
import time

TERMINAL = {"CONFIRMED", "FAILED"}


class Reconciler:
    def __init__(self, client, lcfg, log=print, sync=False, slug_prefix="btc-updown-5m"):
        self.client, self.c, self.log, self.sync, self.slug_prefix = client, lcfg, log, sync, slug_prefix
        self.inbox = queue.SimpleQueue()
        self._q = queue.SimpleQueue()
        self._thread = None
        self.errors = 0
        if not sync:
            self._thread = threading.Thread(target=self._run, daemon=True, name="reconciler")
            self._thread.start()

    # ---- requests (called from the event loop; they only queue)
    def settle(self, key, leg, attempt, order_id, token):
        self._submit(("settle", key, leg, attempt, order_id, token))

    def lookup(self, key, leg, attempt, token, since_ms):
        self._submit(("lookup", key, leg, attempt, token, since_ms))

    def snapshot(self):
        self._submit(("snapshot",))

    def cash(self):
        self._submit(("cash",))

    def _submit(self, task):
        if self.sync:
            self._do(task)
        else:
            self._q.put(task)

    def _run(self):
        while True:
            task = self._q.get()
            if task is None:
                return
            self._do(task)

    def _do(self, task):
        try:
            getattr(self, "_" + task[0])(*task[1:])
        except Exception as e:
            self.errors += 1
            self.log(f"[reconcile] {task[0]} failed: {type(e).__name__}: {e}")
            if task[0] == "snapshot":
                self.inbox.put(("snapshot", None, None, f"{type(e).__name__}: {e}"))

    # ---- workers
    def _settle(self, key, leg, attempt, order_id, token):
        t0 = time.monotonic()
        seen = None
        while time.monotonic() - t0 < self.c.settle_timeout_s:
            trades = self.client.trades_for_order(order_id, token)
            if trades:
                final = all(t.status in TERMINAL for t in trades)
                sig = tuple((t.id, t.status) for t in trades)
                if sig != seen:
                    seen = sig
                    self.inbox.put(("trades", key, leg, attempt, trades, final))
                if final:
                    return
            if self.sync:
                return
            time.sleep(self.c.settle_poll_every_s)
        self.inbox.put(("trades", key, leg, attempt, self.client.trades_for_order(order_id, token), True))

    def _lookup(self, key, leg, attempt, token, since_ms):
        self.inbox.put(("lookup", key, leg, attempt, self.client.trades_since(token, since_ms)))

    def _snapshot(self):
        pos = self.client.positions(self.slug_prefix)
        orders = self.client.open_orders()
        self.inbox.put(("snapshot", pos, orders, ""))

    def _cash(self):
        self.inbox.put(("cash", self.client.collateral()[0]))

    def close(self):
        if self._thread is not None:
            self._q.put(None)
            self._thread.join(5.0)
