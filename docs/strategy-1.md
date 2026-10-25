# Strategy 1: 1–2 minute direction model

Strategy 1 asks a narrow question: will the Kraken spot BTC/USD or ETH/USD
mid-price be lower, exactly unchanged, or higher in one or two minutes? It is a
short-term research and execution-timing signal. The results below describe
predictions on development weeks, with an independent test still to come.

## What a prediction contains

The mid-price is halfway between the best bid and best ask. The direction model
returns three probabilities: **down**, **flat** and **up**. Flat means an exactly
unchanged endpoint mid-price, rather than a band of small moves. Separate outputs
estimate the move and the largest adverse move during the horizon for a long or
short position. A basis point (bps) is 0.01%, so 100 bps is 1%.

For the main results, features are observed at time t and entry is delayed by one
second. The target compares the mid-price at t+1 s with t+1 s+60 s or t+1 s+120 s.
This tests sensitivity to acting later; it does not simulate an order being filled.

## Inputs and model

The Rust engine's features-v7 combines Kraken spot/perpetual, Binance
spot/perpetual and Deribit DVOL information: book shape, trades, order flow,
volatility, funding/positioning where available, and cross-venue relationships.
The research matrix starts with about 829 columns, including 20 cross-venue lead
features. Those 20 describe five leader/follower pairs using gaps relative to
one- and five-minute averages and changes over one and five seconds.

Preprocessing: **≈414 constant inputs dropped (exact count varies slightly per
model; 415 used)**. Empty/constant columns add no information. OKX parsing exists
in the engine, but these historical results do not include new OKX data. Coinbase and Bybit collection is implemented; their
engine adapters are pending.

The direction model is LightGBM, a sequence of small decision trees whose outputs
combine into probabilities. The chosen settings constrain complexity:

| Setting | Value | Purpose |
| --- | --- | --- |
| `num_leaves` | 7 | Small trees |
| `min_data_in_leaf` | 2000 | Avoid rules supported by very few rows |
| `feature_fraction` | 0.5 | Sample half the features per tree |
| `bagging_fraction`, `bagging_freq` | 0.7, 1 | Sample training rows each iteration |
| `lambda_l2` | 10 | Regularise leaf values |
| Early stopping | Inner chronological validation; up to 1000 rounds | Choose tree count without using outer test labels |

Return models use the same regularisation overrides. Stop-distance models are
separate quantile models; their parameters are controlled by `--risk-params`.
The expected move in the example below is the classifier's probability-weighted
class mean return, with class means learned on its training fold.

## How evaluation works

Development data spans **2026-06-23 through 2026-09-25**. There are 12 weekly
walk-forward folds: each trains on earlier data and predicts a later week.
Overlapping future labels are purged at the boundary. The default fold embargo
equals the forecast horizon: 60 seconds for the 60-second models and 120 seconds
for the 120-second models. One whole day, 2026-09-26, separates development
from lockbox #1.
The usual model-row spacing is ten seconds; the engine emits one-second features.

OOF (out-of-fold) predictions are the concatenation of those held-out weeks.
Comparisons use matching rows/folds and a paired moving-day block bootstrap:
1-, 3- and 7-day blocks, seed 17, 1000 draws. Resampling days in blocks accounts
for clustered signals better than treating every overlapping row as independent.

Confidence is `abs(p_up - p_down)`. Each fold sets its nominal top-50/20/10/5%
thresholds using only inner-validation predictions. Actual test coverage varies.
Overall classifier accuracy uses the largest of the three class probabilities;
selective direction uses `sign(p_up - p_down)`. Both accuracy summaries exclude
actual flat outcomes. Momentum follows the preceding same-horizon price move;
reversal takes its opposite. Unchanged historical mids can make these baselines
predict flat, so beating reversal alone is not evidence of a large trading edge.

**Lockbox #1, 2026-09-27 through 2026-10-20, was already used once for earlier
five-minute models.** It is no longer an untouched test for new hypotheses.
Lockbox #2, from new days on or after 2026-10-21, is pending for the short models;
the intended minimum is four weeks, with candidates and rules frozen before use.

## Results

Kraken spot, **one-second entry delay**, OOF over 12 folds. Accuracy excludes flat
outcomes; confidence subsets have fewer and more clustered observations.
The saved run is `v7-kraken-s10-d1`, with 415 retained inputs.

| Target | Model | Reversal baseline | Momentum baseline | Top 50% confidence | Top 20% | Top 5% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTC 60 s | 57.25% | 41.77% | 48.19% | 68.6% | 75.6% | 82.5% |
| BTC 120 s | 56.63% | 46.02% | 48.76% | 63.3% | 68.9% | 76.1% |
| ETH 60 s | 53.75% | 46.41% | 48.93% | 59.0% | 65.3% | 73.4% |
| ETH 120 s | 53.61% | 48.76% | 48.70% | 56.9% | 60.8% | 67.2% |

The confidence columns also show how accuracy changes with selectivity without
requiring a chart renderer. Higher hit rates come with lower coverage.

Acting immediately (zero-second delay), the earlier unpruned v7 run scored
**58.14% for BTC 60 s** and **54.48% for ETH 60 s**. Binance BTCUSDT spot with a
one-second delay scored **56.06% at 60 s** and **54.59% at 120 s**. Delay changes
the labels, so these are sensitivity checks rather than paired comparisons.

Earlier horizon experiments also show accuracy decaying as the forecast reaches
further ahead. These are separate experiment configurations, not additional rows
of the release table:

| BTC reference / experiment | 60 s | 120 s | 300 s |
| --- | ---: | ---: | ---: |
| Kraken, earlier Kraken-spot-only experiment | 57.7% | 57.1% | 55.1% |
| Binance, earlier short-horizon experiment | 56.8% | 55.2% | 53.3% |

## A real prediction, explained

This is an OOF BTC 60 s row from **2026-09-25 23:54:50 UTC**, entering at
**23:54:51 UTC** (one-second delay), from `v7-kraken-s10-d1`.

| Field | Value | Meaning |
| --- | --- | --- |
| `p_down` / `p_flat` / `p_up` | 60.4% / 27.9% / 11.7% | Probabilities of the three endpoint outcomes |
| Confidence | 0.49; selected in the top-5% group | Absolute difference between up/down probabilities |
| Expected move | −0.0165%, or −1.65 bps | Probability-weighted training-class mean return |
| Short stop q80 / q90 | 0.0 / 0.25 bps | Estimated upper quantiles of a rise against a short |
| Long stop q80 / q90 | 2.4 / 2.7 bps | Estimated upper quantiles of a fall against a long |
| Observed outcome | −0.71 bps; down, correct | Mid-price change from delayed entry to endpoint |

A q90 distance aims to cover the adverse move in nine out of ten comparable
cases; q80 aims for eight out of ten. It is an estimate, not a guaranteed stop
execution price. Negative distances are clipped to zero, and q90 is kept at least
as large as q80. Prices and outcomes here exclude execution costs.

## Are the probabilities calibrated?

For BTC 60 s, group non-flat OOF outcomes by the predicted up-share
`p_up / (p_up + p_down)`. The realised up rates are close to the corresponding
predicted shares:

| Predicted up-share bin | Realised up rate |
| --- | ---: |
| 0.0–0.1 | 0.04 |
| 0.1–0.2 | 0.14 |
| 0.2–0.3 | 0.25 |
| 0.3–0.4 | 0.37 |
| 0.4–0.5 | 0.46 |
| 0.5–0.6 | 0.54 |
| 0.6–0.7 | 0.64 |
| 0.7–0.8 | 0.76 |
| 0.8–0.9 | 0.87 |
| 0.9–1.0 | 0.97 |

The directional probabilities look reasonably honest on these development rows.
This check conditions on a non-flat outcome; it does not establish full
three-class calibration or guarantee calibration on future days.

## What we learned

Here, pp means percentage points of accuracy; a move from 55.0% to 55.5% is +0.5 pp.

- Cross-venue inputs help at 60 s: about +0.4–0.5 pp; the gain survives a one-second delay.
- On the recorder's receive clock, Kraken perp leads other venues by about 100 ms; Kraken spot's response develops over roughly ten seconds. Feed batching and network location can affect this measurement.
- Features-v7 lead inputs add roughly +0.1–0.25 pp and improve log loss across the tested models; slow gaps relative to their 1–5 minute means carry the gain.
- BTC↔ETH context is borderline: about +0.26 pp for BTC in the tested five-minute comparison, with no reliable log-loss improvement.
- Bigger trees give no reliable overall gain and worse calibration; the small regularised trees remain the choice.
- Denser five-second training samples give no reliable gain over ten-second sampling on matched evaluation rows.
- Dropping approximately 414 constant inputs leaves essentially the same accuracy and makes training about one-third faster in the measured comparison.

## Limits and practical meaning

The top-5% 60-second rows average about **1.8 bps per signal in the predicted
direction** before costs, with roughly **470 selected rows per day** for BTC.
Using the three-class argmax, non-flat hit rates are **82.5% for BTC and 72.8%
for ETH**. ETH's selective up-versus-down rule scores 73.4%, as in the table.
These overlapping rows are signals, not independent trades or a feasible schedule of round trips.

An illustrative low-volume taker fee range of **0.40–0.80% (40–80 bps) per side**,
depending on the Kraken pair/schedule, dwarfs a 1.8 bps gross move; see the
[Kraken fee schedules](https://www.kraken.com/features/fee-schedule) for the
applicable market. Strategy 1 is therefore a research/execution-timing signal,
not a standalone trading strategy. There is no spread, slippage, fill model or
net-profit backtest here.

These results are development-fold OOF results. Many experiments informed model
selection, so the development period cannot serve as a fresh confirmation.
Lockbox #2 remains essential. The tiny expected moves also limit what even a
correct prediction can achieve after trading costs.

## How to reproduce

From the repository root, install the dependencies with `uv sync` and build Rust.
`ARCHIVE` is your copy of the raw archive; `WORK` is a separate output directory.
Use the historical Kraken/Binance/Deribit archive and receive-time replay for the
reported setup. Additional venues or a different clock define a new experiment.

```bash
(cd engine && cargo build --release --locked -p engine-replay)
# One day; repeat for the archive dates needed by the experiment.
engine/target/release/engine-replay --root ARCHIVE --date 2026-09-25 \
  --out WORK/features
# Alternatively, the launcher replays all closed days in a range.
scripts/train_offline.sh ARCHIVE WORK --jobs 2 --replay-only \
  --from 2026-06-23 --to 2026-10-20

PYTHON_DOTENV_DISABLED=1 uv run python -m research.train WORK \
  --version strategy-1 --label-spec research/targets_kraken_short.yaml \
  --target BTC_spot --target ETH_spot --horizon-s 60 --horizon-s 120 \
  --early-stopping --rounds 1000 --lockbox-from 2026-09-27 \
  --entry-delay-s 1 --fold-days 7 --min-train-days 14 \
  --max-folds 12 --stride-s 10 \
  --params '{"num_leaves":7,"min_data_in_leaf":2000,"feature_fraction":0.5,"bagging_fraction":0.7,"bagging_freq":1,"lambda_l2":10}'

PYTHON_DOTENV_DISABLED=1 uv run python -m research.compare A B \
  --blocks 1 3 7 --draws 1000 --seed 17
```

A and B are saved model directories or `oof.parquet` paths with identical rows,
folds, labels and entry delay. The comparison reports B minus A; it refuses to
silently intersect mismatched samples. Constant removal is the training default.
The training command reserves dates at/after 2026-09-27 and does not open them.
Use a new model version for another run; saved versions are not overwritten.

Exact reproduction also needs the same raw days, engine version, calendar, replay
mode and saved parameters. Code and dependencies alone do not include the full
research archive or models. See the [public overview](../README.md),
[engine guide](../engine/README.md) and [research guide](../research/README.md).
