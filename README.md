# BTC/ETH market data pipeline

Collects BTC and ETH **spot + perpetual** market data from Kraken, Binance and OKX. Every message passes through Kafka to a replayable raw archive on local disk: one JSONL file per feed per symbol per UTC day. Later phases add an S3 daily archive, a Rust feature engine, a dashboard, and 15/30-minute return models.

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

## Phase 2: S3 archive

Requires Terraform >=1.13.4 and the AWS CLI (`brew install terraform awscli`). Fill the AWS/S3
fields in `.env`: `AWS_ADMIN_PROFILE` names an existing admin profile used only during setup;
`AWS_PROFILE` defaults to `crypto-archiver`, the least-privilege profile used by the daily job.
Then run:

```bash
scripts/aws_setup.sh
scripts/install_launchd.sh
```

This creates a private, AES256-encrypted bucket, a monthly budget with email alerts, and an
archiver IAM user/profile with only list/read/write permissions. It is safe to rerun:
`terraform apply` is idempotent, and a key is created only once, when the archiver profile has
no access key configured. The key goes directly into the named AWS profile, never into the
repo or Terraform state. Terraform uses local state in
`infra/terraform/`; keep that state safe and retain `.terraform.lock.hcl` in version control.
The launchd installer schedules the hourly job (minute 15); its UTC logs go to
`data/logs/archive.log`. The job catches up past UTC dates, compresses and verifies closed
files, and writes each S3 manifest last. Open/missing streams keep a day provisional.
Kraken auth streams with no files and no API key are skipped; existing files are archived
regardless of today's key. Offset gaps are recorded even when a day is complete. Engine
files will be added when the engine exists.

```bash
uv run python -m archiver.archive_daily --date 2026-10-17
uv run python -m archiver.archive_daily --verify 2026-10-17
```

Local deletion is disabled while `ARCHIVE_DELETE_LOCAL_AFTER_H` is empty. After verifying
downloaded days, set it to `48` to follow the plan: raw files and metadata are removed only
after the complete manifest has aged 48 hours and every object's metadata matches its hash.
The temporary `.gz` is removed after each verified upload.

## Layout

```text
collectors/        exchange collectors: binance.py, okx.py, kraken.py; common/ (config, envelope, sinks, runner)
archiver/          record_raw.py: Kafka → data/raw/date=…/exchange=…/market=…/feed=…/<SYMBOL>.jsonl
config/            markets.yaml: exchanges, symbols, feeds, partitions, rotation
scripts/           create_topics.py, probe_feeds.py, check_raw.py, pipeline.sh
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
