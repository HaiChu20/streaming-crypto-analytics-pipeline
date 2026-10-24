Captured by scripts/probe_feeds.py from the public Advanced Trade WebSocket on
2026-10-24 UTC for 60 seconds, without authentication, retaining the first 50
records per BTC-USD/ETH-USD stream. To keep the fixture directory below 300 KB,
each L2 snapshot retains only its best 50 bid and 50 offer levels and is
re-serialized. Subsequent update frames and trade payloads remain verbatim.
These excerpts test routing and sequence checks, not full-book reconstruction.
../coinbase-probe-summary.json records full live rates and original untrimmed
snapshot sizes, plus steady traffic/daily rotation and approximate gzip estimates.
