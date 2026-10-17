"""Create the raw.* Kafka topics from config/markets.yaml (idempotent).

    uv run python scripts/create_topics.py

One partition per asset (BTC -> 0, ETH -> 1), replication 1, 72h retention plus a per-partition size cap.
Never delete and recreate a topic: archived offsets refer to it (plan.md §6).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from collectors.common.config import all_streams, kafka_bootstrap, load_config  # noqa: E402


def main() -> None:
    cfg = load_config()
    kcfg = cfg["kafka"]
    partitions = len(cfg["partitions"])
    topics = sorted({s.topic for s in all_streams(cfg)})
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap()})
    existing = admin.list_topics(timeout=10).topics

    settings = {
        "retention.ms": str(kcfg["retention_hours"] * 3600 * 1000),
        "retention.bytes": str(kcfg["retention_bytes_per_partition"]),
        "cleanup.policy": "delete",
        "max.message.bytes": "8388608",
    }
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1, config=settings) for t in topics if t not in existing]
    for topic, future in admin.create_topics(new).items() if new else []:
        future.result()
        print(f"created  {topic}")
    for topic in topics:
        if topic in existing:
            count = len(existing[topic].partitions)
            status = "ok" if count == partitions else f"WRONG partition count {count} (expected {partitions})"
            print(f"exists   {topic}: {status}")
    print(f"{len(topics)} raw topics, {partitions} partitions each")


if __name__ == "__main__":
    main()
