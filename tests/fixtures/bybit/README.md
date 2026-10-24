Captured by scripts/probe_feeds.py from the public v5 spot and linear WebSockets
on 2026-10-24 UTC for 180 seconds without authentication. Each file retains at
most 50 records per symbol/connection, including real liquidation messages.
Native payloads are verbatim. ../bybit-probe-summary.json records rates measured
across the full probe, not just these excerpts. All 7,129 depth-200 deltas across
BTC/ETH spot/linear passed u=previous_u+1 and increasing seq with zero drops.
Daily volume estimates exclude initial snapshots and add one daily rotation
snapshot per stream; gzip estimates use retained subsequent envelopes.
