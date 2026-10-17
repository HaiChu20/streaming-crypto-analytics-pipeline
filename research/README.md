# Offline baseline research

Run from the repository root (never needs Kafka, Docker, AWS, or `.env`):

```sh
PYTHON_DOTENV_DISABLED=1 uv run python -m research.train data --version baseline-v1
PYTHON_DOTENV_DISABLED=1 uv run pytest -q
```

Input is `ROOT/features/date=D/features.parquet`. Use only complete archived days
for production research (plan §7); the command also accepts one Parquet file for
tests. `--output-root` defaults to ROOT. Models go to
`ROOT/models/<BTC_spot|BTC_perp|ETH_spot|ETH_perp>/<900|1800>/<version>/`:
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
the earliest prediction time; it is audit metadata, never a feature. A reference
trailing 900-second return is also included, using only exact historical endpoints.

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
baseline scales the trailing 900-second return by h/900; its metric `n` reports
coverage when historical endpoints are missing. Classifier direction is argmax
over [-1,0,1]; its implied return for MAE/RMSE/IC is probabilities weighted by each
class's mean return estimated on that fold's training data alone. Model metadata
contains the final class means for inference.

Several weeks of data are required for meaningful evaluation. A roughly nine-hour
sample tests plumbing and leakage guards only. These predictive metrics do not
constitute a trading backtest with fees, spread, funding and execution latency.
