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

## Layout

```text
collectors/        exchange collectors: binance.py, okx.py, kraken.py; common/ (config, envelope, sinks, runner)
archiver/          record_raw.py: Kafka → data/raw/date=…/exchange=…/market=…/feed=…/<SYMBOL>.jsonl
config/            markets.yaml: exchanges, symbols, feeds, partitions, rotation
scripts/           create_topics.py, probe_feeds.py, check_raw.py, pipeline.sh
docker/kafka/      single KRaft broker + kafka-ui
tests/             unit tests + fixtures (live captures, Kraken checksum examples)
```
