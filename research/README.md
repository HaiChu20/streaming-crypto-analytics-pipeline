# Offline baseline research

Run from the repository root (never needs Kafka, Docker, AWS, or `.env`):

```sh
PYTHON_DOTENV_DISABLED=1 uv run python -m research.train data --version baseline-v1
PYTHON_DOTENV_DISABLED=1 uv run pytest -q
```

Input is `ROOT/features/date=D/features.parquet`. Use only complete archived days
for production research (plan §7); the command also accepts one Parquet file for
tests. `--output-root` defaults to ROOT. Models go to
`ROOT/models/<BTC_spot|BTC_perp|ETH_spot|ETH_perp>/<300|900|1800>/<version>/`:
native LightGBM regressor/classifier text, `metadata.json`, and
`training_rows.parquet` recording each training row's replay mode, availability,
engine version and label endpoints. An aggregate JSON report goes at output ROOT.
Existing model versions are never overwritten. Nothing is uploaded unless
`--upload` is supplied; it uses boto3 and `S3_BUCKET` with the same `models/...` keys.

`targets.yaml` fixes label version v1 and the four Binance references. Labels use
Python integer arithmetic on `mid_e9` (including half-atom mids), converting to
float only after subtracting and multiplying by 100. They require a valid,
non-warm reference at t and a valid positive midpoint at exactly t+h. Missing or
invalid endpoints stay null; prices are never interpolated. Direction is the sign.

The default stride is 10 UTC seconds. Each target's matrix uses only that asset's
instrument rows at the same tick, with `exchange_market_` prefixes (cross rows use
`cross_cross_`). Absolute mid/bid/ask/microprice/mark levels and identity fields
are excluded from predictors. Book validity/warm-up flags and stationary returns,
spread, depth, flows, basis, funding and open interest remain. Nulls are left for
LightGBM. `available_ns` is the latest emission of any same-tick contributing row,
the earliest prediction time; it is audit metadata, never a feature. Reference
trailing 300- and 900-second returns are also included, using only exact historical endpoints.

Walk-forward folds use expanding time-ordered prefixes and disjoint ordered test
blocks, with no shuffle, tuning or early stopping on the test folds. Training
rows are purged until both their label endpoints and label emissions precede the
test start; their input features must also be available by then. Prediction rows
emitted after their target endpoint are excluded. The gap is strictly greater
than the label horizon. `--purge-s` may
increase this gap but cannot reduce it. Models are finally refit on all labeled
rows; reported metrics are held-out fold predictions, never final-fit scores.

MAE/RMSE use percentage points. Directional accuracy excludes actual flat returns;
the zero baseline predicts flat and thus scores zero on non-flat cases. Spearman
IC uses average ranks for ties and is null for a constant series. The momentum
baseline uses the trailing 300-second return for h=300 and scales the trailing
900-second return by h/900 otherwise; its metric `n` reports
coverage when historical endpoints are missing. Classifier direction is argmax
over [-1,0,1]; its implied return for MAE/RMSE/IC is probabilities weighted by each
class's mean return estimated on that fold's training data alone. Model metadata
contains the final class means for inference.

Several weeks of data are required for meaningful evaluation. A roughly nine-hour
sample tests plumbing and leakage guards only. These predictive metrics do not
constitute a trading backtest with fees, spread, funding and execution latency.

## Five-minute forecasts and stop distances

The supported horizons are 300, 900 and 1800 seconds (5, 15 and 30 minutes).
Both trailing `momentum_return_300s` and `momentum_return_900s` are predictors.
The five-minute momentum baseline uses the 300-second return directly; the other
baselines keep using the 900-second return multiplied by h/900. Both momentum
features require exact historical endpoints whose emissions are already known.

A stop distance describes how far the reference mid price moved against a long
or short position before the horizon ended. `adverse_long_bps` is the largest dip
below the starting mid; `adverse_short_bps` is the largest rise above it. Distances
are nonnegative basis points (100 bps = 1%). `favorable_long_bps` and
`favorable_short_bps` record the corresponding best moves for reporting. The
window includes t+1 through t+h and excludes t. All h whole-second reference rows
must exist, have valid non-warm books and positive `mid_e9`; the start must also
be usable. Off-grid rows cannot replace missing seconds. Missing paths remain
null for stop training only; endpoint-return training keeps its existing rules.
Label availability is the emission time of the t+h row, as for return labels.
Dense one-second grids and reverse rolling minima/maxima/counts avoid scanning
each path separately. Price subtraction and multiplication use Int128 integers
before the final floating-point division, preserving tiny moves at large prices.
These are **mid-price** stops: spread, slippage and execution costs are excluded.

Each side has LightGBM quantile models at q80 and q90. In plain words, a q90 stop
aims to cover the adverse move in nine out of ten comparable situations. Its
held-out exceedance rate (`hit_rate`, actual adverse move strictly greater than
the predicted distance) should be near 10%; q80 aims for 20%. This is an empirical
calibration goal, not a guarantee. `pinball_loss` measures the accuracy of a
quantile forecast; lower is better. Negative predictions become zero, and q90 is
raised to at least q80 during both evaluation and inference.

All four models use the same features, purged fold boundaries and availability
rules as the return models. Only rows with non-null excursion labels can train
or score them. Two baselines are fitted on each training fold: its unconditional
adverse-move quantile, and `k * reference rv_300s`, with k equal to the same
quantile of adverse/rv over positive finite rv. Missing/negative rv is excluded
from that baseline; zero rv predicts zero if k is available. Volatility tercile
edges are also fitted on training rows alone. Regime hit rates help reveal stops
that understate risk during volatile periods. Tied edges can yield empty groups;
missing rv has no regime. Overall metrics concatenate held-out rows, using each
fold's own regime assignment.

Artifacts include `stop_long_q80.txt`, `stop_long_q90.txt`,
`stop_short_q80.txt` and `stop_short_q90.txt` when path labels are available.
`metadata.json` records `stop_models`, `stop_labeled_rows`, `stop_metrics` and
`folds[].stop_metrics`. Each model's metrics contain `quantile`,
`target_hit_rate`, and `model`, `unconditional`, `volatility_scaled` results:
`n`, `hit_rate`, `pinball_loss`, and `by_volatility_tercile` (low/medium/high).
Fold metrics also record `train_rows`, `training_quantile_bps`, `volatility_k`,
and `volatility_tercile_edges`; a fold with no training paths reports its status.

Inference emits `stop_{long|short}_bps_q{80|90}` and matching
`stop_{long|short}_price_q{80|90}` using that tick's reference `mid_e9`.
Long stop price = mid * (1 - bps/10000); short = mid * (1 + bps/10000).
Older model directories still load and emit null stop fields. Both ClickHouse
prediction tables have nullable columns so older messages without stop fields
remain ingestible. Existing deployments need the corresponding additive column
migration before consuming the expanded messages; the init SQL defines fresh
installations.


## Evaluation and timing controls

`--params` overrides return/classifier defaults; `--risk-params` overrides stop
parameters. Both accept an inline JSON object or a `.json` file:

```sh
python -m research.train WORK --params '{"num_leaves":7,"lambda_l2":10}' \
  --risk-params risk.json --early-stopping --inner-valid-days 7 --early-stopping-rounds 50
```

Early stopping selects rounds using the last whole UTC training days, with
purged inner training rows, then refits the complete outer training fold.
`--rounds` is the maximum; insufficient inner history falls back to it. Final
models follow the same procedure. Outer test labels never choose rounds.
`regressor_shrunk` adds an inner-validation-fitted scale in [0,1] when early
stopping is enabled; the original regressor stays available.

The always-on `ridge` control uses training-only median filling, missing
indicators, standardisation and chunked normal equations. Alpha is factor ×
training rows / 1000: factor 100 normally, or inner selection from 1/10/100/1000.
Its frozen model is `ridge.npz`.

Repeat `--exclude-group NAME` or `--exclude-features REGEX` to remove predictors.
`--list-feature-groups` counts a one-day sample without fitting. Groups:
`calendar`, `liquidations`, `l3`, `flow`, `returns`, `volatility`, `book`,
`positioning`, `cross`, `quality`, `venue_kraken`, `venue_binance`, `context`.
Groups overlap; read ablations separately, not additively. Momentum is in returns;
DVOL is in volatility. Unmatched/all-removing exclusions fail. Metadata saves
excluded names and the retained count and ordered feature SHA256.

`--entry-delay-s D` accepts a non-negative integer (default 0). Features and
momentum/reversal stay at t; returns and excursions span t+D to t+D+h.
Delayed endpoints drive purging and the development cutoff. Lockbox evaluation
uses the saved delay. `day_phase` uses feature time: 0 for 00:00–01:00 UTC, 1 for
rest, in both pooled and lockbox summaries.

`oof.parquet` saves each outer-test tick, feature availability, label
start/end/availability, delay, fold, return/direction; regressor, optional shrunk
regressor, ridge, classifier expected return, `p_down/p_flat/p_up`,
`p_up_minus_down`, zero, momentum, reversal, prior probabilities/expected return;
regime assignments, four stops, adverse labels, stop baselines and optional
shuffle predictions. Adjacent `metadata.json` records parameters and choices.

JSON/HTML report log loss, Brier and paired bootstrap differences: whole UTC
days in moving blocks of 1/3/7, 1000 fixed-seed draws, mean and 2.5/50/97.5
percentiles. Positive accuracy differences and negative loss differences favour
the model. `OUTPUT/experiments.jsonl` records arguments, git state, parameters,
split, feature schema, statuses and metrics, including skipped/runtime-failed runs.
`--lockbox-ledger PATH` defaults to `ROOT/lockbox_ledger.jsonl` even with another
output root. Overlapping dates require `--i-understand-lockbox-reuse`; acknowledged
entries record reuse.

### Kraken-only v2 archive training

The offline launcher passes `--exchange NAME`, `--market NAME`,
`--exchange-clock-ms N`, and `--skip-marker-only-days` to replay. For the older
native Kraken spot WS v2 archive (no receive timestamps), use:

```sh
scripts/train_offline.sh ARCHIVE WORK --jobs 1 \
  --exchange kraken --market spot --exchange-clock-ms 35 --skip-marker-only-days \
  --label-spec research/targets_kraken.yaml --train-from 2025-11-01
```

Discovery and duplicate checks use the selected venue/market; no other venues
are required. An engine skip (status 3) becomes
`WORK/features/date=D/skipped.json`, recording the reason, input paths, engine
version and replay mode. It counts as complete on resubmission with the same
options, appears in `WORK/logs/run-summary-*.json` and stdout, and never enters
training: input discovery globs only `features/date=*/features.parquet`. If every
day is skipped, the launcher finishes successfully without training. Use a new
work directory when changing replay clocks, or rebuild all stale days explicitly;
a skipped result cannot replace an existing feature day (use a new work directory).
Duplicate canonical streams across archive directories fail even for cached days.

`research.train --label-spec PATH` defaults to `research/targets.yaml`.
`targets_kraken.yaml` keeps the same calendar/horizons, has no context assets or
perpetual references, and labels BTC/ETH spot from Kraken's native `BTC/USD` and
`ETH/USD` symbols. Reports and model metadata save the complete spec;
`--open-lockbox` uses that saved spec even if the current/default spec changes.
A training run requires one replay mode across all input files, including dates
reserved for the lockbox. Exchange-clock replay records, for example,
`ideal+exchange_clock_35ms`; it cannot mix with receive-time `ideal` or another delay.

`--train-from YYYY-MM-DD` sets the earliest UTC training tick for every fold and
the final refit. It is recorded in reports, model metadata and `experiments.jsonl`.
Both runs load the same inputs: fold placement remains anchored at the first
labeled UTC day plus `min_train_days`, with the latest `max_folds` nonempty
calendar blocks retained (default 12 weekly blocks). Test windows and test rows
stay unchanged. A cutoff leaving any selected fold with no training rows is an
error identifying that fold; it never silently drops or moves test folds.

### Comparing two runs

Run `python -m research.compare A B --blocks 1 3 7 --draws 1000 --seed 17
--json-out comparison.json`, where A and B are model directories or their
`oof.parquet` paths. The table reports B minus A with 2.5/50/97.5 percentiles
from paired moving UTC-day blocks, plus each run's scores and valid row/day
counts. Higher directional accuracy and lower log loss, multiclass Brier, MSE
and stop pinball loss are better. Row keys, order, folds, entry delays and
labels must match; adjacent metadata, when present for both, must agree on
target, horizon, label spec version and date split. There is no row intersection.
Optional shrunk predictions and stop models are compared when present in both.
Finite masks follow each run's report; each draw divides summed scores by that
run's valid-row count, so missing stop labels and unequal day sizes retain their
proper weights. A metric with no valid rows is reported as `n/a` (JSON `null`).
