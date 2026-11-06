"""Fill simulation of a FAK order against a Book (the engine's own Book class): take what the book offers inside the limit/floor, keep any
partial fill, cancel the rest. BUY uses the engine's `Book.walk_asks` (the same function as the paper simulator); SELL adds the price floor
the paper sell does not have. Fees: taker, rate x p x (1 - p) per share at each matched price (`model.fee_per_share`)."""
from types import SimpleNamespace as Rec

from ..book import TICK
from ..model import fee_per_share

EPS = 1e-9


def buy_fill(book, limit_t, shares, fee_rate, fee_exp):
    """-> Rec(status, shares, cost, fee, avg). status: filled | partial | missed; reason when missed."""
    if book is None or not book.valid:
        return Rec(status="missed", reason="book_invalid", shares=0.0, cost=0.0, fee=0.0, avg=None)
    fills = book.walk_asks(limit_t, shares)
    got = sum(q for _, q in fills)
    if got <= EPS:
        return Rec(status="missed", reason="no_liquidity_within_limit", shares=0.0, cost=0.0, fee=0.0, avg=None)
    cost = sum(px / TICK * q for px, q in fills)
    fee = sum(fee_per_share(px / TICK, fee_rate, fee_exp) * q for px, q in fills)
    return Rec(status="filled" if got >= shares - 1e-6 else "partial", reason="", shares=got, cost=cost, fee=fee, avg=cost / got)


def sell_fill(book, floor_t, shares, fee_rate, fee_exp):
    """SELL FAK: bids from the best price down to `floor_t`. -> Rec(status, shares, proceeds, fee, avg, floor_hit).
    floor_hit = the floor stopped the sale while shares were still unsold and the book had bids below the floor."""
    if book is None or not book.valid:
        return Rec(status="missed", reason="book_invalid", shares=0.0, proceeds=0.0, fee=0.0, avg=None, floor_hit=False)
    out, need, below = [], shares, False
    for px in sorted(book.bids, reverse=True):
        if need <= EPS:
            break
        if px < floor_t:
            below = True
            break
        q = min(book.bids[px], need)
        out.append((px, q))
        need -= q
    got = sum(q for _, q in out)
    if got <= EPS:
        return Rec(status="missed", reason="no_bids_above_floor" if book.bids else "no_bids", shares=0.0, proceeds=0.0, fee=0.0,
                   avg=None, floor_hit=bool(below))
    proceeds = sum(px / TICK * q for px, q in out)
    fee = sum(fee_per_share(px / TICK, fee_rate, fee_exp) * q for px, q in out)
    return Rec(status="filled" if got >= shares - 1e-6 else "partial", reason="", shares=got, proceeds=proceeds, fee=fee,
               avg=proceeds / got, floor_hit=bool(below) and need > EPS)
