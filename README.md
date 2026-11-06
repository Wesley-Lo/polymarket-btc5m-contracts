# polymarket-btc5m-lag

A research and trading system for Polymarket's 5-minute "Bitcoin Up or Down" markets. It found a real, measurable lag between Binance and Polymarket, built a strategy (v3.1) that passed every paper test, and then failed live: **37 real orders, 0 fills**. The reason is a Polymarket rule, not a bug: on these markets, taker orders are held before matching, while market makers can cancel instantly.

This repo contains the frozen v3.1 strategy, the dry-run and live execution layer, and a write-up of how the edge was found and why it cannot be captured. It is published as a documented negative result. **It is not a profitable strategy and is not financial advice.**

## The idea

Each Polymarket 5-minute market asks whether BTC will be higher at the end of the window than at the start. Binance is where BTC's price moves first. When BTC jumps on Binance, the fair probability of "Up" changes immediately, but Polymarket's order book takes time to catch up. Buying the stale side right after a Binance move, and selling once Polymarket reprices, should earn the difference.

## How the edge was found

1. **Latency study.** A small probe measured that after a Binance move, Polymarket's first price reaction comes within about 200 ms, but its mid covers only ~50% of the move after ~1 s and ~90% after ~2 s.
2. **Settlement price.** The markets settle on a Chainlink 60 s average (TWAP), not spot. Modelling the start price as a 60 s average of Binance raised outcome accuracy from 89% to 97% and improved the Brier score from 0.18 to 0.12.
3. **Winner's curse.** A better pricing model (v2) still lost money: trading the biggest model-vs-market disagreements selects the model's mistakes. The fix was to stop competing with the market's level.
4. **v3 / v3.1: trade only the lag.** Anchor on Polymarket's own mid just before a Binance move, add the model's predicted change, enter only when the edge clears both taker fees and the exit spread (>= 4 points), and sell 2 s after the fill.

## Results

| Stage | Result |
|---|---|
| Paper, 17 h frozen run (pre-registered) | 213 trades, +$657, **+$3.09 per $10 trade**, 95% CI [+2.47, +3.80], 82% win rate. PASS |
| Robustness (+100 ms latency, competition-adjusted queue, exchange-time book, top 10 removed) | All four PASS on profit per fill; fill rate was the weak point |
| Dry run with real order mechanics (partials, minimum size, exit floors, retries) | Matched paper on the same data (+$657 vs +$657) |
| Plumbing test (one real buy and sell) | Worked end to end; real fees matched the model exactly |
| **Live pilots from a Hong Kong VM** | **37 entry orders, 0 filled.** Simulated twin filled 16 on the same signals |

Full numbers: [docs/RESULTS.md](docs/RESULTS.md).

## Why it fails: Polymarket's taker hold

On selected crypto markets, Polymarket holds **marketable (taker) orders** for a short time before matching them (documented as 250 ms on the order-lifecycle page and 150 ms in a later changelog entry). **Cancels of resting maker orders are not held.**

So after a Binance move:
- Market makers see the move and cancel their stale quotes. In the recorded data, the first cancel at the best price comes a median **~56 ms** after a 1 bp move, which is faster than the ~105 ms it physically takes for a signal to travel from Binance (Tokyo) to Polymarket (London). They react to the first ticks of a move, not to the move itself.
- A taker's order, even one sent instantly, cannot match until its hold expires. By then the stale quote is gone.

Being the maker instead was also tested (two backtests, including continuous repricing at Tokyo latency). Both failed their pre-registered rules: competitive quotes leave almost no spread, and about 85% of fills came after other makers at the same price had already started cancelling. Even at near-zero latency the best maker configuration only broke even.

The hold works exactly as designed: it protects liquidity providers from latency arbitrage, which is what this strategy is.

## What is in this repo

```
updown5m/                 frozen v3.1 strategy and runtime
  engine.py, strategy_v3.py, model.py, book.py   pricing, signal and paper simulation
  live.py, recorder.py, sink.py, transport.py    feeds, recording, allowlisted network access
  replay.py                                       deterministic replay of recorded sessions
  execution/                                      dry-run and live order layer (state machine,
                                                  risk limits, kill switch, reconciliation, alerts)
  params.json, params.py                          frozen params (v3.1 sha256 0dd0e21c...)
latency_probe/            read-only latency measurement tools
deploy/                   systemd service and control script for a Linux VM
docs/RESULTS.md           detailed results and methodology
precommit_check.py        scan for anything that looks like a credential
```

## Running it

Python 3.12.

```bash
python -m venv .venv && source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r updown5m/requirements.txt

python -m updown5m.params                                    # check the frozen v3.1 hash
python -m updown5m --exec-mode DRY_RUN --minutes 60          # paper + dry-run orders, nothing is sent
python -m updown5m.replay updown5m/logs/sessions/<stamp>     # replay a recorded session exactly
python latency_probe/probe.py --minutes 10                   # measure your own latencies
```

LIVE mode exists but is deliberately hard to start: it needs `--exec-mode LIVE`, the flag `--i-accept-real-money`, `"armed": true` in `updown5m/execution/live_config.json`, and a passing pre-flight (keys, balance, geoblock region, clock, latency). Credentials are read at runtime from the OS keyring or a cloud secrets manager and are never stored in the repo.

## Safety and compliance

- No API keys, private keys or wallet addresses are included. `.gitignore` excludes logs, recordings, keys and environment files; run `python precommit_check.py .` before committing.
- Only trade where Polymarket permits it. The pre-flight checks Polymarket's geoblock endpoint for the machine's IP and refuses restricted regions; nothing here hides or changes your location.
- This is research code. It loses money live at retail latency. Use it to learn, not to trade.

## Lessons

- Paper results can be honest and still useless: the simulator assumed stale quotes would still be there when the order arrived.
- Measure the venue's rules as carefully as the market: one documented matching rule decided the whole outcome.
- Pre-registered pass rules, held-out data and tiny live tests found this out for about $0.50 in real losses.

## License

MIT. See [LICENSE](LICENSE). Provided as is, with no warranty; nothing here is financial advice.
