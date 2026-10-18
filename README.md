# BTC/ETH market data pipeline

Collects BTC and ETH **spot + perpetual** market data from Kraken, Binance and OKX. Every message passes through Kafka to a replayable raw archive on local disk: one JSONL file per channel per symbol per UTC day. Later phases add an S3 daily archive, a Rust feature engine, a dashboard, and 15/30-minute return models.

- **Plan / architecture:** [plan.md](plan.md), the only plan.
- **Progress:** [STATUS.md](STATUS.md).

## What is collected

| | Kraken | Binance | OKX |
|---|---|---|---|
| Spot | BTC/USD, ETH/USD: L2 `book`, L3 `level3`, `trade` | BTCUSDT, ETHUSDT: `bookTicker`, `depth@100ms` + REST snapshot, `trade` | BTC-USDT, ETH-USDT: `bbo-tbt`, `books`, `trades-all` |
| Perp | PF_XBTUSD, PF_ETHUSD: `book`, `trade`, `ticker` | BTCUSDT, ETHUSDT: `bookTicker`, `depth@100ms` + REST snapshot, `trade`, `aggTrade`, `markPrice@1s`, `forceOrder` | -SWAP: `bbo-tbt`, `books`, `trades`, `trades-all` |

Each message is wrapped with our local receive time (`recv_ns`) and stored byte-for-byte. Connection `start` / `stop` / `drop` events are written in-band, next to the data they affect.

## Run it (macOS)

Requires Docker Desktop and [uv](https://docs.astral.sh/uv/); uv installs Python 3.12 if needed.

```bash
uv sync                                                  # dependencies
cp .env.example .env                                     # add the Kraken key (only needed for L3)
docker compose -f docker/kafka/docker-compose.yml up -d  # Kafka on localhost:9092, UI on :8080
uv run python scripts/create_topics.py                   # 22 raw topics, 2 partitions each
scripts/pipeline.sh start                                # 3 collectors + recorder
scripts/pipeline.sh status                               # stop | rotate (make-before-break reconnect)
uv run python scripts/check_raw.py                       # data-quality report on data/raw
```

**Other commands:**
- `uv run python scripts/probe_feeds.py --seconds 60`: subscribes to every feed without Kafka, saves test fixtures, and prints rates and GiB/day.
- `uv run pytest`: unit tests.

Raw disk files use **v3**, matching the user's archive:
`data/raw/<Exchange>/<Spot|Futures>/<Pair>/<YYYYMMDD>_<channel>.jsonl`.
Binance, OKX and Kraken Futures wrap the native message in `data`; Kraken spot
inserts metadata into the native message. Decimals are exact strings. Our files
add `conn` and Kafka `offset`; readers also accept archive files without them and
case-insensitive directory prefixes such as `KrakenRaw/Futurev6`.
Binance l2 splits into `depth` and `snapshot` files (anchor on sync, periodic every
7 minutes); recovery uses the maximum offset across both. Invalid JSON or metadata
collisions retain exact text in `raw`. Connection markers delimit epochs, closed
files end with a rollover, and `.jsonl.open` files are still being written.

Convert v1 or v2 into a new tree (exclusive-create; originals stay untouched):

```bash
PYTHON_DOTENV_DISABLED=1 uv run python scripts/convert_raw.py data/raw --output-root /path/to/new-raw
PYTHON_DOTENV_DISABLED=1 uv run python scripts/check_raw.py --root /path/to/new-raw --present-only
```

The user's v3 archive needs no conversion. Gzip inputs stay gzip; incomplete tails
are skipped only for `.open` files. Kafka envelopes and probe fixtures remain v1.
`check_raw --json` includes per-file line/CRC counts, connection counts, and trade ID
duplicates and observed gaps. Trade ID gaps are informational. Use
`scripts/pipeline.sh restart <name>` to restart just one service.

## AWS archive setup (S3 + Terraform)

Run from the repo root. Prerequisites: [uv](https://docs.astral.sh/uv/),
Terraform >=1.13.4 and <2.0, and the AWS CLI (`brew install terraform awscli`).
Run `uv sync` to create `.venv`. Configure an admin profile with permissions to
manage S3, IAM users/policies/access keys, and AWS Budgets, for example
`aws configure --profile archive-admin` (enter the admin access key ID, secret
access key, and default region). An existing configured admin profile also works.

Copy `.env.example` to `.env` if you have not already. Fill in only:

- `AWS_ADMIN_PROFILE`: the configured admin profile name, e.g. `archive-admin`.
- `S3_BUCKET`: a globally unique S3 bucket name, at most 55 characters so the
  generated IAM user name fits AWS's limit.
- `TF_VAR_budget_email`: the address that should receive budget alerts.

Keep `AWS_PROFILE=crypto-archiver` different from the admin profile. The supplied
`AWS_REGION=eu-north-1` and `TF_VAR_budget_usd=10` can stay as they are;
`TF_VAR_bucket_name` and `TF_VAR_region` derive from the bucket and region.
Then run this one setup command and approve Terraform's proposed changes when prompted:

```bash
scripts/aws_setup.sh
```

It creates a bucket with public access blocked, AES256 encryption, enforced bucket
ownership, and cleanup of incomplete multipart uploads after seven days. It also
creates `<bucket>-archiver` with only bucket listing and object read/write permissions,
and a monthly USD budget (default $10) with 80% actual and 100% forecast email alerts.
The script creates an access key if the archive profile has none and stores it in
that local AWS CLI profile. Terraform state stays in `infra/terraform/`; retain it.

Run one past UTC day's archive manually; the module loads `.env` itself:

```bash
.venv/bin/python -m archiver.archive_daily --date 2026-10-17
.venv/bin/python -m archiver.archive_daily --verify 2026-10-17
```

Closed files are compressed and verified; open/missing streams keep the manifest
provisional. Leave `ARCHIVE_DELETE_LOCAL_AFTER_H` empty to retain local raw files.
Optional scheduling is available in `scripts/install_launchd.sh`.

Credential-free infrastructure checks (tests use a mocked AWS provider and plans only):

```bash
terraform -chdir=infra/terraform init -backend=false # once, to install providers
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform validate
terraform -chdir=infra/terraform test
```

## Layout

```text
collectors/        exchange collectors: binance.py, okx.py, kraken.py; common/ (config, envelope, sinks, runner)
archiver/          record_raw.py + rawfmt.py: Kafka → data/raw/<Exchange>/<Market>/<Pair>/<YYYYMMDD>_<channel>.jsonl
config/            markets.yaml: exchanges, symbols, feeds, partitions, rotation
scripts/           create_topics.py, probe_feeds.py, convert_raw.py, check_raw.py, pipeline.sh
docker/kafka/      single KRaft broker + kafka-ui
tests/             unit tests + fixtures (live captures, Kraken checksum examples)
```

## Dashboard (phase 4)

With Kafka running on `crypto-net`:

```bash
# Optional before first start: export GRAFANA_ADMIN_PASSWORD='your-local-password'
docker compose --env-file /dev/null -f docker/dashboard/docker-compose.yml up -d
```

Open [Grafana](http://127.0.0.1:3000/d/crypto-market): `admin` /
`local-dashboard-admin` by default; anonymous access is off.
[ClickHouse HTTP](http://127.0.0.1:8123/ping) is also loopback-only.
ClickHouse consumes `features.1s.v1` and, in phase 6, `predictions.v1` via
`kafka:29092`; missing topics are retried without creation. The BTC/ETH dashboard
refreshes every 5s and uses `FINAL` to collapse duplicate identities. Data persists
in named volumes with a 30-day TTL. See [dashboard operations](docker/dashboard/README.md)
for pinned versions, accounts, ingestion behavior, memory caps, and verification.

## Live inference

Set `INFERENCE_MODELS_ROOT=data` and `INFERENCE_MODEL_VERSION=<version>` in `.env`
to enable inference with `scripts/pipeline.sh start`; an empty version disables it.
The root contains `models/<target>/<horizon_s>/<version>/` from `research.train`.
`status` and `stop` include inference when enabled. Models load locally by default.

```bash
PYTHON_DOTENV_DISABLED=1 uv run python -m inference.service --models-root data --version baseline-v1
# Optional filters: --target BTC_spot --horizon-s 900
# Explicit opt-in S3 download: --download (requires S3_BUCKET)
```

Inference starts at the latest `features.1s.v1` offsets in its own consumer group,
uses completed ticks at the model stride, and emits keyed `predictions.v1` rows:
model version, target, horizon, tick/production timestamps, percent return and
direction (-1/0/+1). Missing momentum endpoints remain NaN; missing, invalid or
warming reference books skip prediction. Stats every 60s include skips, malformed
rows, wall-clock latency and per-tick build/predict compute latency. Shutdown
flushes the producer and leaves the final incomplete tick unpredicted.

Grafana compares predictions with exact `mid_e9` endpoints on the versioned
Binance reference and shows directional hit rate for matured, non-flat outcomes
in the visible range. Roughly nine-hour models verify plumbing only; several
weeks are needed for meaningful evaluation.

## Daily report

```bash
PYTHON_DOTENV_DISABLED=1 uv run python scripts/report_daily.py --date 2026-10-17 --allow-open
```

Writes new `data/reports/YYYYMMDD.md` and `.json` files (existing reports are refused).
Use `--root` for another data directory; omit `--allow-open` to require closed inputs.
The scan is read-only, bounds live-file reads at their captured sizes, and parses native
payloads only for a deterministic freshness sample. Reports include coverage, per-stream
drops and deduplicated connection incidents, minute rates, offset gaps, freshness, gzip
estimates, disk runway and S3 projections. Closed metadata supplies verified counts;
open-file metadata is ignored. Gzip estimates use up to 64 MiB per stream; freshness
uses up to 8,192 byte-spaced frames per stream, with missing clocks excluded.
Prices come from exported `S3_PRICE_GB_MONTH` / `S3_PRICE_PUT_PER_1000` or the defaults
in `.env.example`; the report never loads `.env` or contacts AWS/Kafka. Local archive
retention does not expire S3 objects, so the report shows growing S3 monthly run-rate.

## Train on another machine (GPU server)

Clone this repository, install [uv](https://docs.astral.sh/uv/getting-started/installation/)
and [Rust via rustup](https://rustup.rs), then run:

```bash
git clone <repository-url>
cd streaming-crypto-analytics-pipeline
uv sync
scripts/train_offline.sh /path/to/archive /path/to/work --jobs 2 --version btc-spot-v1
```

The command builds release replay, discovers closed v3 days, and publishes
`work/features/date=D/features.parquet` atomically. It resumes completed days;
interrupted staging stays unpublished and is redone. Per-day logs/JSON reports
are in `work/logs/`; models are in `work/models/`. `--jobs` defaults to half the
CPU count; `--from YYYY-MM-DD` / `--to YYYY-MM-DD` filter discovered dates.
Failed replay days stop training unless explicitly allowed with
`--allow-missing-days`. Training uses all completed days in the work directory;
use a separate work directory for a separate date selection, and a new version
for each training run.

`BTC_spot` keeps Binance BTCUSDT spot as its reference. Missing Binance/OKX perps
or ETH do not cause fallback to another instrument: absent references are reported
as skipped. `context_assets: [USDT]` in `research/targets.yaml` adds same-tick,
`ctx_usdt_<exchange>_<market>_*` predictors, including USDT/USD `premium_bps =
10000*(mid-1)`. Missing context is null; its emission time participates in every
availability and purge check. No labeled history (including these two-minute
excerpts) yields an explicit “not enough labeled rows” report, no models, and exit
0; invalid inputs/configuration still fail. LightGBM uses two CPU threads for this
baseline; a GPU is not required.

Local Mac measurements on the two-minute excerpts, **not full-day input**, were
33,511 spot/L3 records → 691,160 full-day output rows in 0.657 s (51,016 records/s,
29.9 MiB peak RSS), and 75,235 futures records → 172,794 rows in 0.243 s (310,109
records/s, 21.1 MiB RSS). These are release replay's JSON timings plus
`/usr/bin/time -l` RSS, excluding build and Python training. Ingestion-only warm
medians were 286,586 and 656,071 records/s respectively (see
[replay measurement method](engine/replay/README.md#v3-fixture-checks-and-throughput)).
Estimate a full day's ingestion time as its record count divided by the applicable
measured ingestion rate, then add tick/Parquet work; full days will take longer
and depend on L3 activity, storage and server speed. Replay streams input, so RAM
is driven by books and pending buffers rather than day length; parallel jobs
multiply worker memory. Measure representative full days before choosing job
counts. Python training additionally holds all input features and wide matrices
in RAM; the replay RSS figures do not estimate training memory.
