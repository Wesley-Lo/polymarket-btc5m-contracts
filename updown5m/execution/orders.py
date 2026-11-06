"""Order construction for the dry run: pure functions, no I/O, no randomness, nothing signed.

An order is an INTENT: the unsigned fields a live bot would put in a CLOB order (Polymarket CLOB V2 docs, fetched 2026-11-01):
  * order type FAK (fill what is available now, cancel the rest, a partial fill is kept; never rests on the book), expiration "0";
  * price on the market's tick, shares rounded DOWN to 2 decimals, USD amount = price x shares rounded to the tick's amount decimals,
    amounts as 6-decimal integers (maker/taker amounts: BUY maker = USD, taker = shares; SELL maker = shares, taker = USD);
  * minimum order size (`min_order_size`, 5 shares for these markets).
"""
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_UP, Decimal

TICK = 10000                       # engine prices are integer multiples of 1e-4
SIZE_DECIMALS = 2

# tick size -> (price decimals, size decimals, USD-amount decimals)   (docs: "Precision by tick size")
PRECISION = {
    Decimal("0.1"): (1, 2, 3), Decimal("0.01"): (2, 2, 4), Decimal("0.005"): (3, 2, 5),
    Decimal("0.0025"): (4, 2, 6), Decimal("0.001"): (3, 2, 5), Decimal("0.0001"): (4, 2, 6),
}


def D(x, nd=6):
    """float/int/str -> Decimal without binary noise."""
    return x if isinstance(x, Decimal) else Decimal(format(x, f".{nd}f")) if isinstance(x, float) else Decimal(str(x))


def tick_of(tick_t):
    """Engine tick (integer of 1e-4) -> Decimal tick, e.g. 100 -> 0.01."""
    return Decimal(int(tick_t)) / TICK


def precision(tick):
    p = PRECISION.get(D(tick).normalize())
    if p is None:
        p = PRECISION.get(D(tick))
    if p is None:
        raise ValueError(f"unsupported tick size {tick}")
    return p


def round_to_tick(price, tick, how):
    """how: 'down' | 'up' | 'nearest'."""
    mode = {"down": ROUND_FLOOR, "up": ROUND_CEILING, "nearest": ROUND_HALF_EVEN}[how]
    return ((D(price) / tick).to_integral_value(rounding=mode)) * tick


def shares_down(x, nd=SIZE_DECIMALS):
    return D(x).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_DOWN)


def usd_amount(price, shares, amount_dec):
    """Docs rule: price x shares; with more decimals than allowed, round up to (decimals + 4) then down to `decimals`."""
    a = D(price) * D(shares)
    q = Decimal(1).scaleb(-amount_dec)
    if a != a.quantize(q, rounding=ROUND_DOWN):
        a = a.quantize(Decimal(1).scaleb(-(amount_dec + 4)), rounding=ROUND_UP)
    return a.quantize(q, rounding=ROUND_DOWN)


def micro(x):
    """Decimal -> 6-decimal integer string (5.20 -> '5200000')."""
    return str(int((D(x) * 1_000_000).to_integral_value(rounding=ROUND_DOWN)))


@dataclass
class OrderMeta:
    """Per-market order constraints, from the recorded gamma_meta / pm_book_rest records (no extra requests)."""
    slug: str
    up: str
    down: str
    tick_t: int = 100
    min_size: float = 5.0
    neg_risk: bool = False
    source: str = "default"


@dataclass
class Reject:
    reason: str
    detail: str = ""


@dataclass
class Intent:
    leg: str                       # ENTRY | EXIT
    attempt: int
    side: str                      # BUY | SELL
    token_id: str
    outcome: str                   # Up | Down
    price: Decimal
    size: Decimal
    price_t: int                   # price in engine ticks (1e-4)
    meta: OrderMeta
    client_order_id: str
    trade_id: int
    notes: list = field(default_factory=list)
    order_type: str = "FAK"
    maker_amount: str = ""
    taker_amount: str = ""
    usd: Decimal = Decimal(0)

    def payload(self, mode, decision_utc, send_utc, arrival_utc, window_start_utc, window_end_utc, timestamp_ms):
        m = self.meta
        return {
            "schema": "polymarket-clob-v2-intent/unsigned", "mode": mode, "client_order_id": self.client_order_id,
            "trade_id": self.trade_id, "leg": self.leg, "attempt": self.attempt,
            "order_type": self.order_type, "post_only": False, "expiration": "0",
            "token_id": self.token_id, "outcome": self.outcome, "side": self.side,
            "price": str(self.price), "size": str(self.size), "usd_amount": str(self.usd),
            "maker_amount": self.maker_amount, "taker_amount": self.taker_amount,
            "market": {"slug": m.slug, "window_start_utc": window_start_utc, "window_end_utc": window_end_utc},
            "tick_size": str(tick_of(m.tick_t)), "neg_risk": m.neg_risk, "min_order_size": format(D(m.min_size).normalize(), "f"),
            "metadata_source": m.source, "timestamp_ms": timestamp_ms,
            "decision_time_utc": decision_utc, "intended_send_time_utc": send_utc, "simulated_arrival_time_utc": arrival_utc,
            "notes": list(self.notes),
        }


def _finish(i, tick):
    pd, sd, ad = precision(tick)
    if i.side == "BUY":
        i.maker_amount, i.taker_amount = micro(i.usd), micro(i.size)
    else:
        i.maker_amount, i.taker_amount = micro(i.size), micro(i.usd)
    return i


def build_entry(meta, outcome, token_id, limit_t, budget_usd, trade_id, client_order_id, paper_target=None):
    """BUY FAK at the v3.1 limit price. Price rounded DOWN to the tick (never above the strategy's limit); shares = budget / limit rounded
    down, so the worst-case pre-fee spend never exceeds the budget. -> Intent | Reject."""
    tick = tick_of(meta.tick_t)
    pd, sd, ad = precision(tick)
    raw = Decimal(int(limit_t)) / TICK
    price = round_to_tick(raw, tick, "down")
    notes = []
    if price != raw:
        notes.append(f"price_rounded:{raw}->{price}")
    if price <= 0 or price >= 1:
        return Reject("price_out_of_range", str(price))
    size = shares_down(D(budget_usd) / price)
    if paper_target is not None and size < D(paper_target) - Decimal("0.001"):
        notes.append(f"size_capped_at_limit:{paper_target}->{size}")
    if size < D(meta.min_size):
        return Reject("below_min_size", f"{size}<{meta.min_size}")
    i = Intent("ENTRY", 1, "BUY", token_id, outcome, price, size, int(price * TICK), meta, client_order_id, trade_id, notes)
    i.usd = usd_amount(price, size, ad)
    return _finish(i, tick)


def build_exit(meta, outcome, token_id, shares, floor_price, trade_id, attempt, client_order_id):
    """SELL FAK with a safety floor: price = the floor rounded UP to the tick (never below it); size = the held shares rounded DOWN to 2
    decimals. A size below the minimum order size cannot be sold at all -> Reject('below_min_size') (the caller holds it as dust)."""
    tick = tick_of(meta.tick_t)
    pd, sd, ad = precision(tick)
    raw = D(floor_price)
    price = min(max(round_to_tick(raw, tick, "up"), tick), Decimal(1) - tick)
    notes = []
    if price != raw:
        notes.append(f"floor_rounded:{raw}->{price}")
    size = shares_down(shares)
    if size < D(meta.min_size):
        return Reject("below_min_size", f"{size}<{meta.min_size}")
    i = Intent("EXIT", attempt, "SELL", token_id, outcome, price, size, int(price * TICK), meta, client_order_id, trade_id, notes)
    i.usd = usd_amount(price, size, ad)
    return _finish(i, tick)
