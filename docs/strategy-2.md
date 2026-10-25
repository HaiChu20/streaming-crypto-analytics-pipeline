# Strategy 2: big-move model (≥0.5%)

Status: **plan**, written 2026-10-25. Nothing below is trained or validated yet.
Strategy 1 (the 1–2 minute direction model) is finished and frozen; see
[Strategy 1](strategy-1.md).

## 1. The question

> In the next hour, will the price move **+0.5% before −0.5%**, **−0.5% before
> +0.5%**, or **neither**? And for the next four hours, the same question
> with **±1%**.

This is a *first-touch* (triple-barrier) target: two price barriers plus a time
limit. It matches how a trade is actually managed — a take-profit, a stop-loss
and a time stop — so the label is the trade's outcome.

Why change from Strategy 1: its top signals average about **1.8 bps (0.018%)**,
while trading costs are **5–80 bps per side**. A 0.5% move is **50 bps**, large
enough to survive perpetual-futures fees if the hit rate is high enough
(section 9).

### What one signal looks like

| Field | Example | Meaning |
| --- | --- | --- |
| Time / asset / target | 2026-11-02 14:05 UTC · BTC · 0.5%/1h | When the signal was made |
| `p_up_first` / `p_down_first` / `p_none` | 0.46 / 0.19 / 0.35 | Probabilities of the three outcomes |
| Direction | Long | The larger of up/down |
| Confidence (win rate) | 0.71 = 0.46 / (0.46 + 0.19) | Chance of TP before SL, if a barrier is hit |
| Entry | Next minute's open, e.g. 70 120 | Reference entry price |
| Take-profit / stop-loss | 70 471 (+0.5%) / 69 769 (−0.5%) | The barriers |
| Time stop | 15:06 UTC | Close at market if neither is hit |
| Expected value after costs | +0.04% = 0.46·0.5 − 0.19·0.5 + 0.35·0 − 0.10 | Section 9 formula; trade only if above a threshold |

The example numbers are illustrative only, not model output.

## 2. Keeping the strategies separate

| Area | Rule |
| --- | --- |
| Strategy 1 model code — `engine/` (features-v7), `research/`, `inference/`, `docs/strategy-1.md` | **Frozen.** Strategy 2 never edits it. |
| Shared data layer — `collectors/`, `archiver/`, `config/markets.yaml`, Kafka, Allas archive | Kept as is. Strategy 2 may read the raw archive and import shared libraries read-only (for example `collectors/common`). Its own new collectors and settings go under `strategy2/`. |
| Strategy 2 | Everything new lives in `strategy2/` and `docs/strategy-2.md`, with its own feature set (`s2-features-v1`), labels, training, evaluation, signals and tests. Anything adapted from Strategy 1 is **copied** into `strategy2/`, not shared. |

Strategy 2 features are a **new, small set built for hour-scale moves**, not an
extension of the ~829 Strategy 1 columns. Most Strategy 1 inputs describe the
next seconds (queue sizes, 1-second flow) and carry little information about
the next hour.

Planned layout (Python + Polars + LightGBM; no Rust needed at minute resolution):

```text
strategy2/
  download.py    fetch public history (checksummed, resumable, never overwrites)
  bars.py        1-minute bar table per asset from the downloaded files
  features.py    s2-features-v1 (section 7), causal as-of joins
  labels.py      first-touch labels on minute bars (section 6)
  train.py       walk-forward LightGBM, calibration
  evaluate.py    accuracy, selective win rate, trade simulator with costs, bootstrap
  signal.py      section 1 signal record
  config/        assets, barriers, costs, data sources
  live/          later: pollers + live feature builder (phase 6)
tests/strategy2/ unit tests and small fixtures
```

## 3. What the research says

The rating is about how much the evidence should change our plan, not about
the authors.

| Idea | Evidence | Strength | What it means here |
| --- | --- | --- | --- |
| **Volatility clusters and is forecastable** | HAR-type models with realized variance beat GARCH for Bitcoin, most at short horizons; jump terms help ([Bergsli et al. 2022](https://uis.brage.unit.no/uis-xmlui/handle/11250/2980915); [HAR jump studies](https://eprints.nottingham.ac.uk/57681)). | Strong | Whether ±0.5% is touched at all is mainly a volatility question. Recent realized volatility at several lookbacks is the core of the "will it move" part. |
| **Order-flow predictability fades with horizon** | Deep order-flow models predict well only about two price changes ahead ([Kolm, Turiel & Westray](https://papers.ssrn.com/abstract=3900141)). | Strong | Confirms that Strategy 1's book features won't carry over. Use flow summed over 15 min–4 h instead. |
| **Quarter-hour opening order imbalance** | Bursts at 1-, 5- and 15-minute clock marks from algorithmic trading; the *opening order imbalance predicts 4–12 hour returns* on Binance perps, BTC/ETH/XRP/SOL/DOGE/ADA, 2021–2024 ([Kim & Hansen 2026](https://arxiv.org/abs/2607.09426)). | Medium (new preprint) | Directly on target and cheap to compute from 1-minute taker volume. A headline feature to test. |
| **Clock periodicity in volatility** | Hour-of-day, day-of-week and within-hour volatility patterns, partly tied to funding times (00/08/16 UTC on Binance) ([Hansen, Kim & Kimbrough](https://arxiv.org/abs/2109.12142)). | Strong | Calendar features help predict whether a barrier is touched. |
| **Crowded leverage, carry and crashes** | High futures carry predicts crashes ([Schmeling, Schrimpf & Todorov, BIS](https://www.bis.org/publ/work1087.htm)). Perp prices deviate from no-arbitrage more than FX ([He et al.](https://arxiv.org/abs/2212.06888)). Liquidations cascade in thin books ([Oct 2025 preprint](https://ideas.repec.org/p/arx/papers/2608.03616.html)). | Medium–strong | Funding, basis, open-interest changes and long/short ratios describe how fragile positioning is, which mainly helps with the *down-first* side and tail moves. |
| **Implied volatility** | Bitcoin implied-vol indices and their variance risk premium are well defined from Deribit data ([Alexander & Imeraj](https://sro.sussex.ac.uk/id/eprint/91094/)). Option net buying pressure is informative ([arXiv 2109.02776](https://arxiv.org/abs/2109.02776)). | Medium | DVOL and implied-minus-realized volatility help predict touches. Option flow is a later test. |
| **Intraday momentum** | Early-session returns predict late-session returns when volume is high ([Shen, Urquhart & Wang 2022](https://research.birmingham.ac.uk/en/publications/bitcoin-intraday-time-series-momentum/)). Hourly momentum after abnormal days ([Caporale & Plastun](https://cesifo.org/en/publications/2019/working-paper/momentum-effects-cryptocurrency-market-after-one-day-abnormal)). | Medium | Returns over 1–24 h and same-day abnormal-move flags. |
| **Machine learning on short horizons** | Gradient boosting and RNNs beat random at 1–60 minutes; technical features matter most ([Jaquart, Dann & Weinhardt 2021](https://publikationen.bibliothek.kit.edu/1000150665)). | Medium | Supports LightGBM on price, volume and flow features first. Exotic data second. |
| **BTC leads altcoins** | Bitcoin leads Cardano by about 57 s on average ([Anderson 2023](https://businessperspectives.org/journals/investment-management-and-financial-innovations/issue-421/a-tick-by-tick-level-measurement-of-the-lead-lag-duration-between-cryptocurrencies-the-case-of-bitcoin-versus-cardano)). At tick level the lead can reverse ([Southampton thesis](https://eprints.soton.ac.uk/504578/1/Final_Thesis_for_archive.pdf)). | Medium | For SOL/XRP/DOGE, add BTC's recent move and the coin's residual against BTC. |
| **Macro announcements** | Bitcoin's 30-minute price reaction to most US macro news is small, larger for hawkish forward guidance ([NY Fed SR1052](https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr1052.pdf)). Volatility rises around CPI/FOMC ([Volmex](https://volmex.finance/Macroeconomic%20Announcements%20Fuel%20Volatility%20An%20In%20Depth%20Analysis.pdf)). | Medium | Event timing helps predict touches, not direction. We already keep a CPI/NFP/FOMC calendar. |
| **Polymarket crypto markets** | Bitcoin contracts are calibrated but show *no predictive content for next-hour returns* ([AAU study](https://projekter.aau.dk/the-informational-content-of-prediction-markets-evidence-from-polymarket-bitcoin-contracts-628468cf.html)). Prices deviate from option-implied odds mostly in tails ([IFCA paper](https://fc26.ifca.ai/defi/papers/market-efficiency-prediction-markets.pdf)). | Weak for direction | Cheap to collect; test it, but expect little. It may be more useful as a *trading venue* (section 12). |
| **Coinbase premium** | Widely watched as a US-demand gauge, but no formal test found. A related Grayscale-premium study found strong daily predictability ([SMU](https://ink.library.smu.edu.sg/lkcsb_research/7447)). | Weak–medium | Cheap from Coinbase vs Binance prices. A tier-2 test. |
| **On-chain flows, stablecoins, ETF flows** | Exchange net inflows mostly predict volatility, rarely returns ([arXiv 2411.06327](https://arxiv.org/abs/2411.06327)). Tether issuance had no return effect ([Wei 2018](https://ideas.repec.org/a/eee/ecolet/v171y2018icp19-22.html)). ETF flows relate to price *levels*, weak for next-period returns ([Mazur & Polyzos 2025](https://zuscholars.zu.ac.ae/works/7267)). | Weak at hour scale | Daily data, mostly paid. Not in v1. |
| **Triple-barrier labels and meta-labeling** | The standard way to label trades with take-profit, stop-loss and time limit (López de Prado, *Advances in Financial ML*, 2018). Applied to BTC/ETH ([Financial Innovation 2025](https://link.springer.com/article/10.1186/s40854-025-00866-w)). | Method | Our label design (section 6). Meta-labeling (a second model deciding *whether to act*) is a later option. |

**Takeaway:** the best-supported inputs for hour-scale moves are **volatility
state, flow over minutes to hours, leverage and positioning, clock and events,
and implied volatility**. The full limit order book matters much less at this
horizon, so we mainly use depth summaries (±1–5% bands) instead.

## 4. Data

### What we already record (own archive, from 2026-06-23)

Kraken, Binance, OKX spot and perp books, trades, mark/funding, Binance
liquidations, Kraken Futures ticker (funding, open interest) and Deribit DVOL.
Coinbase and Bybit since their collectors were added. **Four months is too
little for hour-scale training:** about 2,900 non-overlapping hours. It is
used for live features, cross-checks and the most recent test window.

### Free public history (checked live on 2026-10-25)

| Source | Dataset | Coverage | Use |
| --- | --- | --- | --- |
| [Binance public data](https://github.com/binance/binance-public-data) USD-M perps | `klines` 1m: OHLC, volume, trade count, taker-buy volume | Years; all five coins | Bars, labels, volatility, flow |
| | `metrics` every 5 min: open interest, top-trader and global long/short ratios, taker long/short volume ratio | From 2020-09-01 (BTC); SOL/DOGE present | Positioning |
| | `fundingRate` (monthly), `premiumIndexKlines` 1m, `markPriceKlines`, `indexPriceKlines` | From 2020-01 | Funding, basis, predicted funding |
| | `bookDepth` every 30 s: depth and notional within ±0.2/1/2/3/4/5% | From 2023-01-01 | Liquidity and asymmetry |
| | `aggTrades`, `trades` | Years, large files | Large-trade share (tier 2) |
| Binance spot | `klines` 1m (and 1s), `aggTrades` | Years | Spot vs perp flow, Polymarket's resolution source |
| [Deribit](https://docs.deribit.com/api-reference/market-data/public-get_last_trades_by_currency_and_time.md) | DVOL history; all option trades with IV via `history.deribit.com` | Options since early years (tested 2024-01) | Implied vol, option flow |
| OKX `market-data-history` | Daily trades; daily L2 400-level order book files (~420 MB/day BTC swap) | Daily files | Cross-venue checks |
| Bybit [public.bybit.com](https://public.bybit.com/) | Daily perp trades | From 2020-03-25 | Second-venue flow |
| Coinbase REST candles | 1-minute candles | Years | Coinbase premium |
| [Polymarket](https://docs.polymarket.com/api-reference/markets/get-prices-history) Gamma + CLOB `prices-history` | Hourly, 15-min and 5-min "Up or Down" for BTC/ETH/SOL/XRP/DOGE; "hit X or Y first" markets | Since each market listed | Crowd probability (tier 2) |

Polymarket's hourly BTC market resolves on the **Binance BTC/USDT spot 1h
candle** (close ≥ open → "Up"). Past hourly markets traded about
**$20k–50k each** (two checked on 2026-10-24).

Not free, or not needed now: liquidation history before our own recording
(Binance stopped publishing `liquidationSnapshot`; Coinglass and Tardis are
paid), on-chain flows (Glassnode/CryptoQuant), news sentiment, and intraday
equity index futures.

### From you (planned)

OKX spot/futures files; SOL and DOGE (and possibly XRP) data; Polymarket data.
Send the date range and file layout and I'll add them to `strategy2/download.py`
or a loader. Public history already covers most of these, so your files mainly
add depth and cross-checks.

### Reference price and venue

Labels use the **Binance USDT-margined perpetual** trade price for each coin:
it has the longest free history, is among the most liquid markets, and is
close to the venues we'd trade on. The BTC basis between major perps is a few
basis points, small next to a 50 bps barrier. A cross-venue check against
Kraken/OKX prices on our own archive days is part of phase 2.

## 5. Measured base rates

How often barriers are hit, sampled every minute, entry at the minute's open:

**Binance perps, 2026-01-01 to 2026-03-31** (1-minute candles):

| Coin | 0.5%/1h touched | up first | down first | Up share | 1%/4h touched | Median 1 h range |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTC | 47.4% | 22.9% | 24.5% | 48.4% | 51.5% | 0.64% |
| ETH | 63.2% | 30.2% | 32.9% | 47.9% | 67.3% | 0.86% |
| SOL | 69.0% | 33.1% | 35.9% | 48.0% | 72.0% | 0.94% |
| XRP | 66.1% | 31.9% | 34.2% | 48.2% | 67.9% | 0.86% |
| DOGE | 71.8% | 34.0% | 37.8% | 47.4% | 73.8% | 0.95% |

**Kraken spot mid, 2026-06-23 to 2026-09-25** (earlier measurement): BTC
24.7% (0.5%/1h) and 26.6% (1%/4h); ETH 39.7% and 43.3%; up share 54–62%.

What this tells us:

- **Touch frequency swings with the volatility regime:** BTC 47% in Q1 versus
  25% in summer. Predicting *whether* a move happens is the most learnable part.
- **The up/down split also flips by regime:** 48% up in Q1 versus 54–62% in
  summer. Direction must come from current conditions, never from a fixed bias.
- **Altcoins touch more often** (66–72% versus 47%) at the same fixed 0.5%,
  which gives more trade opportunities. Five coins (BTC, ETH, SOL, XRP, DOGE)
  is enough. All have Binance history and Polymarket markets, and none are
  meme-only microcaps.
- **Both barriers inside the same minute is rare** (≤0.04%), so 1-minute
  candles are enough for labels. Ties are left out.

## 6. Target and labels

- **Decision times:** every 5 minutes, at the minute boundary.
- **Entry:** the next minute's open (one minute of latency).
- **Targets:** `A` ±0.5% within 60 minutes; `B` ±1% within 240 minutes.
  Barriers are relative to entry.
- **Outcome:** up-first, down-first or none, from 1-minute high/low. Same-minute
  double touches are left out, and an incomplete horizon (data gap) means no label.
- **Recorded with each label:** time to touch, maximum adverse excursion, and
  the return at the time stop, for the trade simulator.
- Fixed percentage barriers, as requested. Volatility-scaled barriers are a
  later experiment, not v1.

The existing 1-second barrier code on this branch is moved into `strategy2/`
(phase 0). The minute-bar version is simpler, and both are checked against
each other on the same days.

## 7. Features: `s2-features-v1`

Every feature at time *t* uses only data published by *t*. Five-minute
`metrics` rows are used 5 minutes after their `create_time`, `bookDepth` 30 s
after its timestamp, and funding only after settlement. "z" means a z-score
against the asset's own trailing 30 days. About **60–80 features per asset**,
far fewer than Strategy 1.

| Group | Features | Source (history / live) |
| --- | --- | --- |
| **A. Volatility state** | Realized vol over 15m, 1h, 4h, 24h and 7d (HAR terms). Parkinson high–low vol. Up and down semivariance. Jump share: largest 1m move / RV(1h). Vol of vol. **Barrier distance in sigmas** = 0.5% / (σ̂₁ₕ), the key first-touch input | 1m klines / live 1m bars from the collected aggTrade stream |
| **B. Trend and location** | Returns over 15m, 1h, 4h, 24h. Same-day abnormal-move flag. Distance to the 24h and 7d high/low in σ units. Deviation from 1h/4h/24h VWAP. Distance to the nearest round number (BTC $1,000) | 1m klines |
| **C. Flow** | Taker-buy imbalance over 15m, 1h, 4h. **Quarter-hour opening imbalance**: taker imbalance in the first minute of each 15 min, averaged over 1h/4h ([Kim & Hansen](https://arxiv.org/abs/2607.09426)). Relative volume (1h vs 7d same-hour mean). Trade-count z. Spot-minus-perp taker imbalance | Perp and spot 1m klines (taker-buy volume) |
| **D. Leverage and positioning** | Open interest change over 1h, 4h, 24h. OI change × price change sign (new longs, new shorts, short cover, long liquidation). OI / 24h volume. Funding rate, its z, and predicted funding from the 1m premium index. Minutes to next funding. Top-trader account and position long/short ratios and their 4h change. Global long/short ratio. Taker long/short volume ratio | `metrics`, `fundingRate`, `premiumIndexKlines` / live REST polls every 5 min + collected markPrice stream |
| **E. Book liquidity** | Bid and ask depth within ±1%, ±2%, ±5% (Binance publishes ±0.2% only from 2026, so v1 leaves it out). Bid/ask asymmetry per band. Depth change over 1h. Depth z versus 7d (thin book, bigger moves) | `bookDepth` from 2023 / live from the collected depth stream (parity check needed, see risks) |
| **F. Implied volatility** | DVOL level, 1h and 24h change, DVOL / realized-vol ratio (variance risk premium). BTC/ETH only, used as market context for alts | Deribit DVOL history / collected DVOL |
| **G. Clock and events** | Hour-of-day and day-of-week (cyclic). Minute within the hour. US cash-equity open window. Minutes to and since CPI/NFP/FOMC. Deribit expiry (Fri 08:00 UTC; month and quarter ends) | Calendar (copy of `config/events.csv`) |
| **H. Cross-asset** (alts) | BTC returns over 15m/1h/4h. Coin return minus β×BTC return over 1h/4h. BTC RV(1h). ETH/BTC 4h trend | Klines |
| **Tier 2, tested after v1** | Coinbase premium (Coinbase BTC-USD vs Binance BTCUSDT) over 1h/4h. Polymarket hourly implied P(up) minus a drift-only baseline. Deribit option flow (call vs put premium bought, block trades, 1h). Liquidation notional by side over 15m/1h (own archive only, from 2026-06). Large-trade share (aggTrades ≥ $100k) | Section 4 |

v1 leaves out on-chain, ETF flows, stablecoin supply, news sentiment and equity
futures: the evidence at hour scale is weak and/or the data is paid. A row of
our raw order book is not a v1 input either.

## 8. Model

- **One 3-class LightGBM per target** (up-first, down-first, none), trained
  across all five coins with an asset ID. This pools data; per-asset models
  are compared as an ablation. Small regularized trees, like Strategy 1, with
  settings tuned only on development folds.
- **Baselines the model must beat:**
  1. Volatility-only: a first-passage (Brownian) probability from σ̂₁ₕ with zero drift.
  2. Recent-drift sign (momentum).
  3. Reversal.
  4. Class frequencies of the training window.
- **Diagnostics reported separately:**
  - "Will it move?": AUC of 1 − p_none versus the volatility-only baseline.
  - "Which side?": accuracy of up versus down among rows that touched.
- Probabilities are calibrated on inner validation data (isotonic per class,
  if needed), because confidence and expected value depend on them.

## 9. Signals, stop-loss, take-profit and costs

- **Take-profit / stop-loss:** symmetric at the barrier (±0.5% or ±1%). Time
  stop at 60 or 240 minutes. Asymmetric variants, for example a 0.5% TP with a
  0.35% SL, need their own labels and come after v1.
- **Confidence:** `p_win = p_dir / (p_up + p_down)`, plus `p_move = 1 − p_none`.
- **Expected value per trade** (in %, barrier `b`, round-trip cost `c`):
  `EV = p_dir·b − p_opp·b + p_none·E[timeout return] − c`.
  Trade only when EV exceeds a threshold chosen on development folds.
- **One position per asset at a time.** Signals during an open position are
  ignored, so the simulator's trade count is a real schedule.
- **Simulator costs** (configurable):
  - taker fee each side;
  - 1 bp entry slippage and 2 bps stop slippage;
  - TP fills only if price trades through the barrier;
  - funding paid if a funding time falls inside the holding period.

**Break-even win rate** among trades that hit a barrier: `p* = 0.5 + c / (2b)`.
Time-outs make the real requirement a little higher.

| Venue (illustrative fees) | Round-trip cost c | ±0.5% target | ±1% target |
| --- | ---: | ---: | ---: |
| Perp, maker entry + taker exit (≈0.02% + 0.05%) | 0.07% | 57% | 53.5% |
| Perp, taker both sides (≈0.05% + 0.05%) | 0.10% | 60% | 55% |
| Perp, taker + slippage | 0.14% | 64% | 57% |
| Kraken spot retail taker (up to 0.40–0.80% per side) | 0.80%+ | impossible | 90%+ |

The fees above come from secondary 2026 comparisons:
- Binance, about 0.02% maker / 0.05% taker; Bybit, 0.02% / 0.055%; Hyperliquid, 0.015% / 0.045% ([Bitsgap](https://bitsgap.com/blog/hyperliquid-fees-vs-binance-and-bybit-whats-actually-cheaper));
- Kraken Futures, about 0.02% / 0.05% ([Datawallet](https://www.datawallet.com/crypto/kraken-fees-explained));
- Kraken spot, up to 0.80% taker ([Kraken support](https://support.kraken.com/articles/201893638-fee-schedule)).

**Strategy 2 only makes sense on perpetual futures with low fees.** Which perp
venues an EU/Finland account may use, and at what leverage, must be checked
directly for your account: under EU rules some offshore venues are restricted,
and perps may fall under derivatives rules. This plan has no leverage
requirement; a 1× position is enough to test the edge.

## 10. Evaluation

- **Development window:** 2023-01-01 → 2026-03-31 (`bookDepth` starts 2023-01;
  2021–22 without depth is an optional extension).
- **Walk-forward:** monthly test folds over the last 24 months, with an
  expanding training window. Purge label overlap (4 h) and leave a one-day
  embargo between training and test.
- **Strategy 2 lockbox:** 2026-04-01 → 2026-10-20. It is frozen now and opened
  once, after the model, thresholds and rules are fixed. Disclosure: barrier
  touch *frequencies* for 06-23 → 09-25 were already looked at (section 5),
  but no model has been fitted on these months. After the lockbox, forward
  paper-trading runs on new days from 2026-10-21.
- **Metrics:**
  - log loss and calibration;
  - touch AUC;
  - side accuracy among touched rows;
  - selective win rate at the top 20/10/5/2% confidence;
  - per trade after costs: expectancy, trades/month, daily Sharpe, max drawdown;
  - a paired weekly-block bootstrap against each baseline.

**Acceptance criteria** (development folds, per target, before opening the lockbox):

1. Log loss beats every baseline, with the bootstrap 95% CI of the gain excluding 0.
2. Touch AUC beats the volatility-only baseline.
3. At the chosen threshold, the win rate's lower 95% bound is above the break-even `p*` for 0.10% costs.
4. Net expectancy after costs is positive in each of 2023, 2024, 2025 and 2026 Q1, and over at least 100 trades.
5. Calibration: per-decile |predicted − realised| ≤ 3 pp.

If v1 fails these, we report it plainly and test the tier-2 features or
volatility-scaled barriers; we don't loosen the criteria.

## 11. Build phases

| Phase | Work | Done when |
| --- | --- | --- |
| **0. Separate** | Create `strategy2/`. Move the barrier-label code and its tests from `research/` into it. Restore `research/` to its `main` version on this branch | `git diff main -- engine research inference docs/strategy-1.md` is empty; all tests pass |
| **1. Data** | `strategy2/download.py` for section 4's tier-1 sources, five coins, 2023-01 → 2026-10-20, into `data/strategy2/raw` (git-ignored) on the development Mac | Checksums pass; per-day coverage report; reruns download nothing new |
| **2. Bars + labels** | 1-minute bars, labels A/B, the four baselines | Section 5 rates reproduced; minute labels match the 1-second labels on archive days (except documented ties); no lookahead (tests with shifted data) |
| **3. Features + model** | `s2-features-v1`, walk-forward LightGBM, simulator, HTML/JSON report | Report covers every section 10 metric for both targets and five coins |
| **4. Ablations + tier 2** | Group-by-group removal; Coinbase premium, Polymarket, option flow, liquidations | Each group kept only if its bootstrap gain CI excludes 0 |
| **5. Lockbox** | One run on 2026-04-01 → 2026-10-20 | Result recorded either way |
| **6. Live (paper)** | 5-minute REST pollers (OI, long/short ratios), live 1-minute bars, Polymarket and Deribit pollers, signal publisher, dashboard panel | Live features equal replayed features on the same hours; signals logged with outcomes |

### Stage details

**Where things run.** Download, bars, labels and features are built on the
development Mac: about 5 GB of raw input, minutes of CPU. The finished feature
and label Parquet files (`data/strategy2/features/`) are uploaded to Allas.
Roihu only runs training and evaluation from those files. The minute-level
features need no Rust engine: Polars handles them in seconds.

**Data layout.** The downloader mirrors each source's own paths under one
root, so every file can be traced back to its URL:

```text
ROOT/binance/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09.zip   (+ .CHECKSUM)
ROOT/binance/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-09-15.zip
ROOT/binance/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09.zip
ROOT/deribit/dvol/BTC/1h/2026-09.csv
ROOT/coverage.csv          one row per expected file: present / missing / bytes
```

**Tier-1 download** (phase 1, about 4.7 GB zipped for 2023-01 → 2026-10 and
five coins):

| Dataset | Files | Approximate size |
| --- | --- | ---: |
| Perp 1m klines | Monthly + daily for the current month | 0.4 GB |
| Spot 1m klines | Monthly + daily | 0.5 GB |
| Perp 1m premium index | Monthly + daily | 0.2 GB |
| Funding rate | Monthly | <0.01 GB |
| Metrics (5 min) | Daily | 0.08 GB |
| Book depth (30 s) | Daily | 3.5 GB |
| Deribit DVOL, BTC/ETH | Hourly via API; 1-minute history only reaches back about two months | <0.01 GB |

Tier 2 (phase 4) adds Coinbase 1m candles, Polymarket hourly markets, Deribit
option trades, liquidations from our own archive, and perp aggTrades. The
aggTrades alone are about 20 GB a year for BTC, so they are downloaded only if a
tier-2 test needs them.

**Format quirks to handle:**
- Older Binance CSVs have no header row.
- Spot timestamps are in microseconds from 2025-01-01.
- `metrics` rows are not sorted.
- Some `bookDepth` days are partial (2023-01-15 is almost empty).

These cases are reported in the coverage file, never filled in silently.

| Stage | Main files | Tests |
| --- | --- | --- |
| 0 | `strategy2/labels_1s.py` (self-contained copy of the 1-second barrier code), `strategy2/config/kraken_barrier_1s.yaml` | Moved barrier tests pass; `research/` equals `main` |
| 1 | `strategy2/download.py` | Offline tests with a fake HTTP server: checksum failure, 404, resume, no overwrite |
| 2 | `strategy2/bars.py`, `strategy2/labels.py`, `strategy2/baselines.py`, `strategy2/base_rates.py` | Synthetic bars: up, down, none, same-minute tie, incomplete window; no-lookahead tests; header and timestamp quirks |
| 3 | `strategy2/features.py`, `train.py`, `evaluate.py`, `signal.py` | Every feature shifted-data test (no future values); simulator hand-computed cases |

## 12. Risks and open questions

- **Live parity for `bookDepth`.** Binance's ±5% depth may need more book
  levels than our depth stream keeps. Phase 2 compares our book against the
  public file on archive days. If they don't match, only the bands we can
  reproduce stay in.
- **Regime dependence.** Touch rates and up share changed sharply between Q1
  and summer 2026. Monthly folds and per-year results make this visible.
- **Few independent outcomes.** Labels overlap, and one move can trigger many
  5-minute signals. One-position-per-asset trades and the weekly-block
  bootstrap are the honest counts.
- **Polymarket as a venue, not a feature.** Its hourly BTC market pays on
  close ≥ open of the Binance spot 1h candle, which is a different question.
  Our features could price it. Possible future strategy, out of scope here.
- **Meta-labeling with Strategy 1.** Strategy 1 could later time entries after
  a Strategy 2 signal, using only its saved outputs. Not in v1, to keep the
  strategies separate.

## Sources

Papers and data documentation are linked inline in sections 3, 4 and 9. Data
availability (Binance public data folders and start dates, `metrics` and
`bookDepth` columns, Bybit public trades, OKX daily files, Deribit history,
Polymarket markets and resolution rules) was checked directly on 2026-10-25.
