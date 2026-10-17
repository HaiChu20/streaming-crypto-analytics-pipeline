"""Topic creation can be restricted to the authorized engine outputs."""
from types import SimpleNamespace

import pytest

from scripts import create_topics


@pytest.mark.parametrize("engine_only", [False, True])
def test_engine_topics_have_one_partition_and_raw_creation_is_optional(monkeypatch, engine_only):
    created = []
    monkeypatch.setattr(create_topics, "load_config", lambda: {"kafka": {"retention_hours": 72, "retention_bytes_per_partition": 2**31}, "partitions": {"BTC": 0, "ETH": 1}})
    monkeypatch.setattr(create_topics, "all_streams", lambda cfg: [SimpleNamespace(topic="raw.test.v1")])
    monkeypatch.setattr(create_topics, "kafka_bootstrap", lambda: "localhost:9092")
    monkeypatch.setattr(create_topics.sys, "argv", ["create_topics"] + (["--engine-only"] if engine_only else []))

    class Admin:
        def __init__(self, config):
            assert config == {"bootstrap.servers": "localhost:9092"}

        def list_topics(self, timeout):
            return SimpleNamespace(topics={})

        def create_topics(self, topics):
            created.extend(topics)
            return {t.topic: SimpleNamespace(result=lambda: None) for t in topics}

    monkeypatch.setattr(create_topics, "AdminClient", Admin)
    create_topics.main()
    assert {t.topic: t.num_partitions for t in created} == ({"features.1s.v1": 1, "engine.cutoffs.v1": 1, "predictions.v1": 1} | ({} if engine_only else {"raw.test.v1": 2}))
    assert all(t.config["retention.ms"] == "259200000" for t in created)
    assert all(t.replication_factor == 1 for t in created)
