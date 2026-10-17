import orjson

from collectors.common.config import all_streams, load_config, stream_index
from collectors.common.envelope import control_record, data_record

STREAM = next(s for s in all_streams(load_config()) if s.exchange == "binance" and s.feed == "l2")


def test_payload_is_byte_identical():
    payload = '{"p":"0.00100000","q":45285.20,"x":1e-8}'  # trailing zeros / exponent must survive
    rec = orjson.loads(data_record(STREAM, "update", "c1", 7, 123, payload))
    assert rec["payload"] == payload
    assert (rec["kind"], rec["conn"], rec["seq"], rec["recv_ns"]) == ("update", "c1", 7, 123)


def test_control_record():
    rec = orjson.loads(control_record(STREAM, "drop", "c1", 8, 124, "sequence_gap", {"U": 5}))
    assert rec["kind"] == "control" and rec["event"] == "drop" and rec["reason"] == "sequence_gap"
    assert "payload" not in rec


def test_config_maps_each_stream_to_one_partition():
    cfg = load_config()
    streams = all_streams(cfg)
    assert len(streams) == 44
    assert len({s.topic for s in streams}) == 22
    assert len(stream_index(cfg)) == 44  # (topic, partition) is unique per stream
