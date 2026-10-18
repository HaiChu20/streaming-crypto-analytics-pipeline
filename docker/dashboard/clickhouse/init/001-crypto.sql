CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.features
(
    exchange LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    asset LowCardinality(String),
    tick_ns Int64,
    available_ns Int64,
    engine_version LowCardinality(String),
    book_valid Bool,
    book_age_ms Nullable(Float64),
    warmup Bool,
    mid_e9 Nullable(Int64),
    trade_count Nullable(Int64),
    liq_count_1s Nullable(Int64),
    l3_adds_1s Nullable(Int64),
    l3_modifies_1s Nullable(Int64),
    l3_deletes_1s Nullable(Int64),
    best_bid Nullable(Float64),
    best_ask Nullable(Float64),
    mid Nullable(Float64),
    microprice Nullable(Float64),
    spread_bps Nullable(Float64),
    bid_depth10 Nullable(Float64),
    ask_depth10 Nullable(Float64),
    imbalance10 Nullable(Float64),
    volume_base Nullable(Float64),
    taker_buy_base Nullable(Float64),
    taker_sell_base Nullable(Float64),
    ret_1s Nullable(Float64),
    ret_10s Nullable(Float64),
    ret_60s Nullable(Float64),
    rv_60s Nullable(Float64),
    rv_300s Nullable(Float64),
    funding_rate Nullable(Float64),
    mark_price Nullable(Float64),
    open_interest Nullable(Float64),
    liq_base_1s Nullable(Float64),
    basis_bps_binance Nullable(Float64),
    basis_bps_okx Nullable(Float64),
    basis_bps_kraken Nullable(Float64),
    spot_dev_bps_okx Nullable(Float64),
    spot_dev_bps_kraken Nullable(Float64),
    ts DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Nano(tick_ns)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(tick_ns))
ORDER BY (exchange, market, symbol, tick_ns, engine_version)
TTL toDateTime(ts) + INTERVAL 30 DAY DELETE;

-- Missing topics cause metadata warnings, not DDL failure. Consumption starts
-- when the topic exists. No topic administration is performed by this stack.
CREATE TABLE IF NOT EXISTS crypto.features_kafka
(
    exchange LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    asset LowCardinality(String),
    tick_ns Int64,
    available_ns Int64,
    engine_version LowCardinality(String),
    book_valid Bool,
    book_age_ms Nullable(Float64),
    warmup Bool,
    mid_e9 Nullable(Int64),
    trade_count Nullable(Int64),
    liq_count_1s Nullable(Int64),
    l3_adds_1s Nullable(Int64),
    l3_modifies_1s Nullable(Int64),
    l3_deletes_1s Nullable(Int64),
    best_bid Nullable(Float64),
    best_ask Nullable(Float64),
    mid Nullable(Float64),
    microprice Nullable(Float64),
    spread_bps Nullable(Float64),
    bid_depth10 Nullable(Float64),
    ask_depth10 Nullable(Float64),
    imbalance10 Nullable(Float64),
    volume_base Nullable(Float64),
    taker_buy_base Nullable(Float64),
    taker_sell_base Nullable(Float64),
    ret_1s Nullable(Float64),
    ret_10s Nullable(Float64),
    ret_60s Nullable(Float64),
    rv_60s Nullable(Float64),
    rv_300s Nullable(Float64),
    funding_rate Nullable(Float64),
    mark_price Nullable(Float64),
    open_interest Nullable(Float64),
    liq_base_1s Nullable(Float64),
    basis_bps_binance Nullable(Float64),
    basis_bps_okx Nullable(Float64),
    basis_bps_kraken Nullable(Float64),
    spot_dev_bps_okx Nullable(Float64),
    spot_dev_bps_kraken Nullable(Float64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'features.1s.v1',
    kafka_group_name = 'clickhouse-features',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 1;

-- Skip at most one malformed message per block; larger failures halt the block
-- and appear in the ClickHouse logs. Do not silently discard an unbounded feed.
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.features_ingest
TO crypto.features AS SELECT * FROM crypto.features_kafka;

CREATE TABLE IF NOT EXISTS crypto.predictions
(
    model_version LowCardinality(String),
    target String,
    horizon_s UInt16,
    tick_ns Int64,
    available_ns Int64,
    pred_return_pct Float64,
    direction Int8,
    ts DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Nano(tick_ns)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(tick_ns))
ORDER BY (target, horizon_s, tick_ns, model_version)
TTL toDateTime(ts) + INTERVAL 30 DAY DELETE;

-- Missing topics cause metadata warnings, not DDL failure. Consumption starts
-- when the topic exists. No topic administration is performed by this stack.
CREATE TABLE IF NOT EXISTS crypto.predictions_kafka
(
    model_version LowCardinality(String),
    target String,
    horizon_s UInt16,
    tick_ns Int64,
    available_ns Int64,
    pred_return_pct Float64,
    direction Int8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'predictions.v1',
    kafka_group_name = 'clickhouse-predictions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 1;

-- Skip at most one malformed message per block; larger failures halt the block
-- and appear in the ClickHouse logs. Do not silently discard an unbounded feed.
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.predictions_ingest
TO crypto.predictions AS SELECT * FROM crypto.predictions_kafka;

