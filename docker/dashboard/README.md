# Dashboard operations

With Kafka already running on `crypto-net`, start the dashboard services:

```bash
# Optional: export GRAFANA_ADMIN_PASSWORD='your-local-password' before first start.
# Explicitly bypass .env; Compose uses exported variables and the local default.
docker compose --env-file /dev/null -f docker/dashboard/docker-compose.yml up -d
```

Open [Grafana](http://127.0.0.1:3000/d/crypto-market) (login `admin`, default password
`local-dashboard-admin`) or [ClickHouse HTTP](http://127.0.0.1:8123/ping).
Anonymous access is disabled. The Grafana password initializes its persistent volume
on first start; changing the environment afterward does not reset an existing login.
Pins checked on 2026-10-17 against Docker Hub tag `last_updated` timestamps:

| Component | Pin | Published / pushed (UTC) |
|---|---|---|
| ClickHouse (newest LTS line) | `26.8.15.10` | 2026-10-01 18:42:02 |
| Grafana (stable) | `13.2.3` | 2026-09-29 07:33:07 |
| Official ClickHouse datasource | `4.22.1` | 2026-10-05 16:02:25 |

These are the newest server tags at least 14 days old at verification time.
ClickHouse `26.8.16.41` was pushed 2026-10-03 17:27 UTC (too recent), while
Grafana `13.2.4` was pushed 2026-10-13. The plugin is the newest compatible
release; its requirement is Grafana `>=11.6.0-0` (the image age rule applies to
server images). Both existing named volumes were reused during the upgrade.
Sources: [ClickHouse tag](https://hub.docker.com/v2/repositories/clickhouse/clickhouse-server/tags/26.8.15.10),
[Grafana tag](https://hub.docker.com/v2/repositories/grafana/grafana/tags/13.2.3),
[ClickHouse LTS release](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.8.15.10-lts),
and [plugin catalog](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/).
The datasource uses the local read-only `dashboard` / `dashboard-local` account.
This is a loopback-only development stack; the internal default ClickHouse user is
available to local administration through `docker compose ... exec clickhouse clickhouse-client`.

The live engine publishes `features.1s.v1`; ClickHouse consumes via `kafka:29092`
(group `clickhouse-features`) into `crypto.features`. The implemented
`inference/service.py` consumes features, loads saved models and publishes
`predictions.v1`, which ClickHouse ingests with group `clickhouse-predictions`.
Missing topics produce metadata warnings and are retried automatically once created by the normal topic setup;
this stack never creates topics. New groups begin at the earliest retained offset.
At most one malformed message per block is skipped (`kafka_skip_broken_messages=1`);
larger failures halt the block and are logged. Data lives in named volumes with a
30-day TTL. ReplacingMergeTree uses the engine row identity, and every dashboard
query reads `FINAL` to collapse redeliveries immediately. The asset selector covers
BTC (including Kraken XBT) and ETH. Prediction panels populate when inference
is running with compatible saved models and receives usable feature ticks.
Book validity/invalid tick counts expose drop/resync periods. ClickHouse is capped
at 3 GiB (2.5 GiB tracked server memory), and Grafana at 768 MiB.
Init SQL runs only on a fresh ClickHouse data volume; configuration/provisioning
files are mounted read-only. Stop only these services with the same Compose file
and `down` (without `-v`) to preserve data.

Verification (requires the services to be running):

```bash
python3 docker/dashboard/verify.py
```

The integration check validates schema, datasource health, provisioning, duplicate
identities, both assets across all panels, and realized returns using a query-only
prediction fixture. It inserts six reserved `synthetic-test` feature identities
twice and synchronously removes only that engine version in a `finally` block.
Do not use `synthetic-test` as a real engine version. It prints container RAM usage.
