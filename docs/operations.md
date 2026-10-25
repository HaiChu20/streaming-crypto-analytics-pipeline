# Operations and reproduction

Run commands from the repository root. Python 3.12, uv, Rust and Docker cover the
local workflow. `ARCHIVE` and `WORK` are placeholders for your input archive and
output directory. Keep source archives separate from generated research output.
For the public system overview, see the [README](../README.md); the full release
model settings are in [Strategy 1](strategy-1.md#how-to-reproduce).

## Recording and topic layout

The source of truth is [config/markets.yaml](../config/markets.yaml). Kraken L3
requires API credentials; the other configured feeds use public endpoints.
Without the Kraken key, its L3 connection is skipped. Sequence/checksum checks,
connection markers and reconnects make interruptions visible in the archive.

**Raw format v3:** native JSON with exact decimal token strings, receive time,
connection ID and Kafka offset, under
`data/raw/<Exchange>/<Spot|Futures|Index>/<Pair>/YYYYMMDD_<channel>.jsonl`.
Kafka envelopes remain v1. Raw files are replayable records, rather than a
normalized table of prices; control markers describe connection changes.

OKX parsing exists already, but the [Strategy 1 results](strategy-1.md) use the historical
Kraken/Binance/Deribit inputs. Coinbase and Bybit are excluded from live-engine
assignment until their parsers exist. Current configuration creates 33 raw topics
with two asset partitions each; the engine consumes the 46 supported partitions.

## Quickstart: collect and record locally

Use Python 3.12, uv and Docker. The process helper is for macOS and uses
`caffeinate`; its logs and PID files go under `data/logs` and `data/state`.
Run these commands from the repository root:

```bash
uv sync
# Export Kraken API settings if L3 collection is wanted.
docker compose --env-file /dev/null -f docker/kafka/docker-compose.yml up -d
uv run python scripts/create_topics.py
scripts/pipeline.sh start
scripts/pipeline.sh status
uv run python scripts/check_raw.py
# Later: scripts/pipeline.sh stop
```

The helper starts six collectors and the raw recorder. Kafka listens at
`localhost:9092`, with its UI at `http://localhost:8080`. Optional inference starts
when `INFERENCE_MODEL_VERSION` is set. Configure `DATA_DIR` and other settings
through exported variables or a local environment file. Closed days have metadata
sidecars; files still being written end in `.open`.

For remote archiving, `archiver/archive_daily.py` uploads to any S3-compatible
store through the standard SDK. Configure its environment for your existing store:

| Setting | Purpose |
| --- | --- |
| `S3_BUCKET` | Required destination bucket |
| `AWS_ENDPOINT_URL_S3` | Custom S3 endpoint; `AWS_ENDPOINT_URL` is the SDK-wide fallback |
| `AWS_REGION` | Region expected by the endpoint (also shown in the daily report's cost estimate) |
| `AWS_PROFILE` | SDK profile to use; the archiver defaults to `crypto-archiver` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` | Environment credentials, if used instead of another standard SDK credential provider |

Credentials resolve through the standard SDK chain, including environment,
existing profiles and available role providers. Use an existing profile when
selecting one; no credential setup or infrastructure provisioning runs here.

```bash
uv run python -m archiver.archive_daily --date YYYY-MM-DD
uv run python -m archiver.archive_daily --verify YYYY-MM-DD
```

`--verify` downloads and verifies a completed day. Local collection does not
require object storage. Engine journals/checkpoints are not included in the raw
daily archive.

## Replay and train offline

Use a batch/HPC machine for a multi-month archive, or a local machine for small
samples. Install uv/Python and Rust, run `uv sync`, and provide a raw archive root.
`ARCHIVE` and `WORK` below stand for paths you choose. Keep a separate work root
for each feature engine version or replay clock.

```bash
# Build once, then replay one closed UTC day.
(cd engine && cargo build --release --locked -p engine-replay)
engine/target/release/engine-replay --root ARCHIVE --date 2026-09-25 \
  --out WORK/features

# Or replay a date range in bounded parallel jobs, without training yet.
scripts/train_offline.sh ARCHIVE WORK --jobs 2 \
  --from 2026-06-23 --to 2026-10-20 --replay-only

PYTHON_DOTENV_DISABLED=1 uv run python -m research.train WORK \
  --version strategy-1 --label-spec research/targets_kraken_short.yaml \
  --target BTC_spot --target ETH_spot --horizon-s 60 --horizon-s 120 \
  --early-stopping --rounds 1000 --lockbox-from 2026-09-27 \
  --entry-delay-s 1 --fold-days 7 --max-folds 12 \
  --params '{"num_leaves":7,"min_data_in_leaf":2000,"feature_fraction":0.5,"bagging_fraction":0.7,"bagging_freq":1,"lambda_l2":10}'

# Compare saved model directories or OOF Parquet files on identical rows.
PYTHON_DOTENV_DISABLED=1 uv run python -m research.compare A B
```

Replay writes `WORK/features/date=YYYY-MM-DD/features.parquet`. Training saves
models, metadata and `oof.parquet` under `WORK/models/<target>/<seconds>/<version>`;
JSON/HTML reports and experiment/lockbox ledgers live under the work root.
Versions are never overwritten. The launcher can also train directly with its
supported flags; use the separate Python command for `--params` and early stopping.
Use `python -m research.train --help` for all controls. The commands above keep
the lockbox closed; freezing and opening it is a separate, deliberate operation.

<a id="deribit-dvol-p22-features-v5"></a>
### Deribit DVOL

The collector polls closed one-minute BTC/ETH implied-volatility candles. Replay
adds DVOL levels/changes and a realised-volatility gap to cross rows. The
[feature contract](../engine/replay/README.md#deribit-dvol-features-v5) defines causal
availability and staleness. For a separate historical archive, the server-side
`scripts/backfill_dvol.py --help` explains receive-day backfill; it refuses to
overwrite existing archive content.

## Live features, inference and dashboard

Build with `(cd engine && cargo build --release --locked)`, then run
`engine/target/release/engine-live --root data` against the local broker.
It starts at the latest supported raw offsets, emits `features.1s.v1`, and records
checkpoints/cutoffs for reproducible `as_live` replay. It waits for fresh snapshots;
a restart does not automatically restore the previous session.

Run `uv run python -m inference.service --models-root WORK --version strategy-1`
to consume features and publish `predictions.v1`. Model files and saved metadata
must match the feature schema. Start ClickHouse/Grafana with the instructions in
[dashboard operations](../docker/dashboard/README.md); Grafana is at
`http://127.0.0.1:3000`. These are local development services. See
[engine documentation](../engine/README.md) for timing and recovery limits.

## Tests

```bash
PYTHON_DOTENV_DISABLED=1 uv run pytest -q
# With an existing uv environment: PYTHON_DOTENV_DISABLED=1 .venv/bin/python -m pytest -q
(cd engine && cargo test --release --locked)
(cd engine && cargo clippy --release --locked --all-targets -- -D warnings)
(cd engine && cargo fmt --check)
```

Python tests exercise collectors, recorder/archive, labels, evaluation and serving
with fixtures and stubs. Rust tests cover book reconstruction, feature semantics,
archive replay and live/replay parity. They do not require live exchange access.

