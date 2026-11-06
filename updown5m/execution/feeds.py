"""Feed-safety state for the entry/exit checks, driven by the recorded `ctl` records (the same ones the engine's gap rule reads)."""

DECISION_FEEDS = ("binance_book", "binance_trade")


class FeedMonitor:
    def __init__(self, reconnect_block_s=3.0):
        self.block_ns = int(reconnect_block_s * 1e9)
        self.down = set()                 # decision feeds currently down
        self.until = {}                   # feed -> mono_ns until which it counts as just reconnected
        self.pm = {}                      # slug -> dict(down, since_mono)

    def on_ctl(self, o, mono_ns):
        ev = o.get("ev") if isinstance(o, dict) else None
        feed, slug = o.get("feed"), o.get("slug")
        if ev == "ws_down" and feed in DECISION_FEEDS:
            self.down.add(feed)
        elif ev == "ws_connect" and feed in DECISION_FEEDS:
            self.down.discard(feed)
            self.until[feed] = mono_ns + self.block_ns
        elif ev == "pm_connect":
            self.pm[slug] = {"down": False, "since": mono_ns}
        elif ev == "pm_down":
            self.pm[slug] = {"down": True, "since": mono_ns}
        elif ev == "pm_closed":
            self.pm.pop(slug, None)

    def entry_block(self, slug, book_valid, mono_ns):
        """Reason an ENTRY must not be sent now, else None: a decision feed is down or just reconnected, or the market's socket/book is not usable."""
        if self.down:
            return "feed_gap"
        if any(mono_ns < u for u in self.until.values()):
            return "feed_reconnect"
        st = self.pm.get(slug)
        if st is None:
            return "pm_not_connected"
        if st["down"]:
            return "pm_gap"
        if mono_ns < st["since"] + self.block_ns:
            return "pm_reconnect"
        if not book_valid:
            return "pm_book_invalid"
        return None

    def exit_block(self, slug, book_valid):
        """Reason an EXIT cannot be sent now (Polymarket side only), else None."""
        st = self.pm.get(slug)
        if st is not None and st["down"]:
            return "pm_gap"
        if not book_valid:
            return "pm_book_invalid"
        return None
