"""Create the raw.* Kafka topics from config/markets.yaml (idempotent).

    uv run python scripts/create_topics.py

One partition per asset (BTC -> 0, ETH -> 1), replication 1, 72h retention plus a per-partition size cap.
Never delete and recreate a topic: archived offsets refer to it.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from collectors.common.config import all_streams, kafka_bootstrap, load_config  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-only", action="store_true", help="create/check only feature, cutoff and prediction output topics")
    args = parser.parse_args()
    cfg = load_config()
    kcfg = cfg["kafka"]
    partitions = len(cfg["partitions"])
    topics = sorted({s.topic for s in all_streams(cfg)})
    desired = {} if args.engine_only else {t: partitions for t in topics}
    desired.update({"features.1s.v1": 1, "engine.cutoffs.v1": 1, "predictions.v1": 1})
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap()})
    existing = admin.list_topics(timeout=10).topics

    settings = {
        "retention.ms": str(kcfg["retention_hours"] * 3600 * 1000),
        "retention.bytes": str(kcfg["retention_bytes_per_partition"]),
        "cleanup.policy": "delete",
        "max.message.bytes": "8388608",
    }
    new = [NewTopic(t, num_partitions=count, replication_factor=1, config=settings) for t, count in desired.items() if t not in existing]
    for topic, future in admin.create_topics(new).items() if new else []:
        future.result()
        print(f"created  {topic}")
    for topic, expected in desired.items():
        if topic in existing:
            count = len(existing[topic].partitions)
            status = "ok" if count == expected else f"WRONG partition count {count} (expected {expected})"
            print(f"exists   {topic}: {status}")
    print(f"{len(desired)} topics checked (derived outputs: 1 partition each)")


if __name__ == "__main__":
    main()
