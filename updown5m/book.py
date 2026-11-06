"""Order book rebuilt from Polymarket websocket snapshots + deltas. Prices are integer ticks of 1e-4."""

TICK = 10000


def to_ticks(p):
    return int(round(float(p) * TICK))


class Book:
    __slots__ = ("bids", "asks", "valid", "_bb", "_ba")

    def __init__(self):
        self.bids, self.asks = {}, {}
        self.valid = False
        self._bb = self._ba = None

    def snapshot(self, bids, asks):
        self.bids = {to_ticks(x["price"]): float(x["size"]) for x in bids if float(x["size"]) > 0}
        self.asks = {to_ticks(x["price"]): float(x["size"]) for x in asks if float(x["size"]) > 0}
        self._bb = max(self.bids) if self.bids else None
        self._ba = min(self.asks) if self.asks else None
        self.valid = True

    def set_level(self, side, ticks, size):
        d = self.bids if side == "BUY" else self.asks
        if size > 0:
            d[ticks] = size
            if side == "BUY":
                if self._bb is None or ticks > self._bb:
                    self._bb = ticks
            elif self._ba is None or ticks < self._ba:
                self._ba = ticks
        else:
            d.pop(ticks, None)
            if side == "BUY" and ticks == self._bb:
                self._bb = max(d) if d else None
            elif side == "SELL" and ticks == self._ba:
                self._ba = min(d) if d else None

    def best_bid(self):
        return (self._bb, self.bids[self._bb]) if self._bb is not None else None

    def best_ask(self):
        return (self._ba, self.asks[self._ba]) if self._ba is not None else None

    def mid(self):
        if self._bb is None or self._ba is None:
            return None
        return (self._bb + self._ba) / 2 / TICK

    def walk_asks(self, limit_ticks, shares):
        """Take asks from the best price upward while price <= limit, up to `shares`. -> [(ticks, qty)]."""
        out, need = [], shares
        for px in sorted(self.asks):
            if px > limit_ticks or need <= 1e-9:
                break
            q = min(self.asks[px], need)
            out.append((px, q))
            need -= q
        return out

    def walk_bids(self, shares):
        """Sell `shares` into the bids from the best price downward (no price limit). -> [(ticks, qty)] (may total < shares)."""
        out, need = [], shares
        for px in sorted(self.bids, reverse=True):
            if need <= 1e-9:
                break
            q = min(self.bids[px], need)
            out.append((px, q))
            need -= q
        return out
