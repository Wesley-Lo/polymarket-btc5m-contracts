# Detailed results

All times UTC. Trades are $10 paper size unless noted. Confidence intervals are 95% bootstrap intervals that resample whole 5-minute windows. Parameters for v3.1 were frozen (sha256 `0dd0e21c...`) before every test below.

## 1. Strategy v3.1

- Signal: Binance spot mid moves >= 1 bp within 250 ms.
- Anchor: Polymarket Up mid from strictly before the move.
- Target: anchor shifted in logit space by the pricing model's predicted change (model: normal distribution on a 60 s Binance average start price, terminal variance for a 60 s settlement average).
- Entry: edge = target - ask - entry fee - expected exit fee - expected exit half-spread >= 4 pts; limit = min(ask + 1 pt, highest price keeping edge >= 4); price band 0.10-0.90; one position at a time, 3 s cooldown.
- Exit: sell 2 s after the fill (pays latency, walks the bids, second taker fee).
- Fees: taker 0.07 x p x (1 - p) per share (confirmed exactly by real fills); makers pay 0.

## 2. Paper results (17 h frozen run, 2026-10-31 05:59 to 23:02)

Pre-registered rule: PASS if the CI for mean net per trade is above zero AND net at p90 latency is positive.

| Measure | Result |
|---|---|
| Signals / fills | 280 / 216 (77%) |
| Clean trades (3 exits hit an invalid book after socket reconnects, excluded) | 213 |
| Net | +$657.43 |
| Mean per trade | +$3.09, CI [+2.47, +3.80] |
| Winners | 175 / 213 (82%), median +$1.34, worst -$2.10 |
| Net at p90 latency | +$540.85 |
| Net without the top 10 trades | +$464.68 |
| Verdict | PASS |

Other observations: a simulated maker track lost money to adverse selection (-3.6 to -4.3 pts markouts); entries below 0.40 produced 69% of profit; entries at 0.80 and above earned almost nothing

## 3. Robustness (pre-registered R1-R4)

| Check | Result |
|---|---|
| R1: +100 ms latency | +$3.07 per fill, CI lower bound +2.40. PASS |
| R2: competition-adjusted queue (others' prints removed from available depth) | +$3.20 per fill, CI lower bound +2.54. PASS |
| R3: book rebuilt on exchange timestamps, arrival at decision + full RTT | +$3.26 per fill, 59% fill rate (after fixing a clock bug, below). PASS |
| R4: top 10 trades removed | +$464.68. PASS |

Latency decay: profit per fill stayed positive up to +1000 ms, but the fill rate fell from 77% to 12%.

Clock bug found during robustness: the clock-offset sampler reconnected for every sample, and connection setup biased the offset by about +268 ms. The true offset was about +15 ms (confirmed against NTP and warm samples).

## 4. Taker hold re-score

Re-scoring with a 250 ms hold on entries and exits (exchange-time book): fill rate 22-35%, about +$0.66 to +$1.04 per decision. Still positive on paper, which turned out to be optimistic (section 6).

## 5. Dry run and plumbing

- The dry-run layer built the exact orders a live bot would send (fill-and-kill entries, sell floors, retries, partial fills kept, 5-share minimum) and matched paper results on the 17 h data.
- Plumbing test from a Hong Kong VM: one real 5-share buy and sell. Balance reconciled to the cent; fees matched the formula. First order signing took 437 ms because the client fetched market metadata; pre-fetching and warm-up cut it to ~5 ms.

## 6. Live pilots (Hong Kong VM, $5 per entry)

| Measure | Result |
|---|---|
| Entry orders sent | 37 (two pilots) |
| Filled | **0** |
| Simulated twin on the same signals | 16 fills |
| Decision -> send | p50 8.9 ms |
| Send -> response (includes the hold) | p50 492 ms, p99 771 ms |
| CLOB round trip from Hong Kong | ~230 ms |
| Real money lost | $0 in the pilots (about $0.43 in plumbing tests) |

The twin was optimistic because it did not model the hold and saw Polymarket's book ~150 ms late from Hong Kong.

## 7. Why: makers react before a held taker order can match

From the recordings (exchange time, corrected clock):

- After a 1 bp Binance move, the first size reduction at the stale best price comes a median **~56 ms** later (73% cancels); the level is fully gone at a median ~275 ms.
- Polymarket's best quote first moves the same way a median ~254 ms after the move.
- Physical floor for Binance (AWS Tokyo) -> Polymarket CLOB (AWS London): ~105 ms one way from anywhere.
- Incumbent cancels faster than that floor means they react to the first ticks of a move, not to the 1 bp crossing.

Server locations measured (CLOB round trip / Binance feed delay): Hong Kong ~213-233 / 31 ms; Netherlands 30 / 119 ms; Zurich 30 / 148 ms. Total time from a Binance move to an order reaching Polymarket is ~110-150 ms everywhere; the hold comes on top.

## 8. Maker alternatives (backtests, not traded)

| Test | Result |
|---|---|
| Maker v4: quote near fair, cancel on 1 bp moves, Netherlands latency | Held-out net per fill -$0.080 [-0.52, +0.38]; FAIL |
| Maker v5: continuous repricing on every tick, Tokyo latency, rate-limited | Held-out net per fill -$0.065 [-0.092, -0.038] on 2,596 fills; +1 s markout -0.94 pts; FAIL |
| v5 at near-zero latency | About break-even (-$0.002 per fill) |

About 85% of v5's fills came after other makers at the same price had started cancelling: a new maker is the quote left standing when a move continues.

## 9. Conclusion

The Binance -> Polymarket lag on these markets is real and measurable, but it cannot be captured at retail latency. As a taker, every order is held long enough for makers to cancel. As a maker, competitive quotes leave no spread and fills are dominated by adverse selection.
