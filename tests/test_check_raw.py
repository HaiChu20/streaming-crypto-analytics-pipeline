"""Offline checker verdicts and bounded trade overlap history."""

import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import orjson
import pytest

from collectors.common.config import Stream
from scripts import check_raw

BOOK = Stream("okx", "spot", "l2", "BTC", "BTC-USDT", "books", "raw.test", 0)
TRADES = replace(BOOK, feed="trades", channel="trades")


def data(kind, payload, conn="a"):
    return {"kind": kind, "conn": conn, "payload": orjson.dumps(payload).decode()}


def control(event, conn="a"):
    return {"kind": "control", "conn": conn, "event": event}


def book(kind, seq, prev=0, conn="a"):
    return data(kind, {"data": [{"seqId": seq, "prevSeqId": prev}]}, conn)


def trade(tid, conn="a"):
    return data("trade", {"data": [{"tradeId": tid}]}, conn)


def check(tmp_path, records, stream=BOOK):
    path = tmp_path / "raw.jsonl"
    path.write_bytes(b"".join(orjson.dumps(dict(r, offset=i)) + b"\n" for i, r in enumerate(records)))
    return check_raw.check_stream(stream, [path], depth=10, deep=True)


@pytest.mark.parametrize("next_event,errors", [("drop", 0), ("stop", 1), ("start", 1), (None, 1)])
def test_book_break_deferred_verdict(tmp_path, next_event, errors):
    records = [book("snapshot", 10), book("update", 12, prev=11)]
    if next_event:
        records.append(control(next_event))
    result = check(tmp_path, records)
    assert len(result["errors"]) == errors
    if errors:
        assert result["errors"][0].endswith("not marked by a drop")
    assert result["events"]["explained_breaks"] == int(next_event == "drop")


def test_break_followed_by_data_is_not_explained_by_later_drop(tmp_path):
    result = check(tmp_path, [book("snapshot", 10), book("update", 12, 11),
                              book("update", 13, 12), control("drop")])
    assert len(result["errors"]) == 1
    assert not result["events"]["explained_breaks"]


def test_pending_break_waits_for_same_conn(tmp_path):
    result = check(tmp_path, [book("snapshot", 10), book("update", 12, 11),
                              book("update", 1, conn="b"), control("drop")])
    assert not result["errors"]
    assert result["events"]["explained_breaks"] == 1


@pytest.mark.parametrize("exchange,market,payload", [
    ("okx", "spot", {"data": [{"seqId": 2, "prevSeqId": 1}]}),
    ("kraken", "spot", {"data": [{"checksum": 0, "bids": [], "asks": []}]}),
    ("kraken", "perp", {"seq": 2}),
])
@pytest.mark.parametrize("started", [False, True])
def test_update_before_snapshot_requires_scanned_start(tmp_path, exchange, market, payload, started):
    records = [control("start")] if started else []
    records.append(data("update", payload))
    result = check(tmp_path, records, replace(BOOK, exchange=exchange, market=market))
    assert len(result["errors"]) == int(started)
    if started:
        assert "before first snapshot" in result["errors"][0]


def test_kraken_bad_snapshot_can_be_explained(tmp_path):
    records = [data("snapshot", {"data": [{"checksum": 1, "bids": [], "asks": []}]}), control("drop")]
    result = check(tmp_path, records, replace(BOOK, exchange="kraken"))
    assert not result["errors"]
    assert result["events"]["explained_breaks"] == 1


def test_binance_stale_snapshot_waits_for_retry(tmp_path):
    records = [data("update", {"data": {"U": 10, "u": 20}}),
               data("snapshot", {"lastUpdateId": 1}),
               data("update", {"data": {"U": 21, "u": 30}}),
               data("snapshot", {"lastUpdateId": 25}),
               data("update", {"data": {"U": 31, "u": 40}})]
    result = check(tmp_path, records, replace(BOOK, exchange="binance"))
    assert not result["errors"]
    assert result["events"]["explained_breaks"] == 0


@pytest.mark.parametrize("overlap", [False, True])
def test_duplicate_trade_within_conn_is_error(tmp_path, overlap):
    records = [trade("1")] if overlap else []
    result = check(tmp_path, records + [trade("1", "b"), trade("1", "b")], TRADES)
    assert len(result["errors"]) == 1
    assert "within conn b" in result["errors"][0]


def test_overlap_trade_ids_in_live_and_recent_conns(tmp_path):
    result = check(tmp_path, [trade("1"), trade("1", "b"), control("drop"), trade("1", "c")], TRADES)
    assert not result["errors"]
    assert result["overlap_dup_trades"] == 2


def test_trade_history_retains_only_two_ended_conns(tmp_path):
    records = [trade("1"), control("drop"), trade("2", "b"), control("stop", "b"),
               trade("3", "c"), control("drop", "c"),
               trade("1", "d"), trade("2", "d"), trade("3", "d")]
    result = check(tmp_path, records, TRADES)
    assert not result["errors"]
    assert result["overlap_dup_trades"] == 2


def test_empty_ended_conns_also_expire_trade_history(tmp_path):
    result = check(tmp_path, [trade("1"), control("stop"), control("start", "b"), control("stop", "b"),
                              control("start", "c"), control("stop", "c"), trade("1", "d")], TRADES)
    assert not result["errors"]
    assert result["overlap_dup_trades"] == 0


@pytest.mark.parametrize("has_key", [False, True])
def test_missing_streams_and_auth_skip(tmp_path, monkeypatch, capsys, has_key):
    if has_key:
        monkeypatch.setenv("KRAKEN_API_KEY", "")  # only presence matters
    else:
        monkeypatch.delenv("KRAKEN_API_KEY", raising=False)
    monkeypatch.setattr(check_raw, "data_dir", lambda: tmp_path)
    monkeypatch.setattr("sys.argv", ["check_raw", "--date", "2026-10-17"])
    with pytest.raises(SystemExit) as exc:
        check_raw.main()
    assert exc.value.code == 1
    output = capsys.readouterr().out
    missing = 44 if has_key else 42
    assert output.count("MISSING") == missing
    assert output.count("skipped (no API key)") == (0 if has_key else 2)
    assert f"\n{missing} errors\n" in output


def pipeline_copy(tmp_path):
    script = tmp_path / "scripts/pipeline.sh"
    script.parent.mkdir()
    script.write_text((Path(__file__).resolve().parents[1] / "scripts/pipeline.sh").read_text())
    return script


def test_pipeline_stop_waits_for_owned_process(tmp_path):
    script = pipeline_copy(tmp_path)
    state = tmp_path / "data/state"
    state.mkdir(parents=True)
    child = subprocess.Popen(["sleep", "60"])
    (state / "binance.pid").write_text(str(child.pid))
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            exited = pool.submit(child.wait)
            result = subprocess.run(["bash", str(script), "stop"], capture_output=True, text=True, timeout=5)
            assert result.returncode == 0
            assert exited.result(timeout=1) is not None
            assert child.returncode is not None
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait()


def test_pipeline_start_detects_failed_processes(tmp_path):
    script = pipeline_copy(tmp_path)
    python = tmp_path / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/usr/bin/env bash\nexit 1\n")
    python.chmod(0o755)
    result = subprocess.run(["bash", str(script), "start"], capture_output=True, text=True, timeout=8)
    assert result.returncode == 1
    for name in ("binance", "okx", "kraken", "recorder"):
        assert f"FAILED to start {name}, see data/logs/{name}.log" in result.stdout


@pytest.mark.parametrize("term_fails", [False, True])
def test_pipeline_stop_timeout_reports_survivors(tmp_path, term_fails):
    script = pipeline_copy(tmp_path)
    kill = 'kill() { [[ "$1" != "-TERM" ]]; }' if term_fails else 'kill() { return 0; }'
    script.write_text(script.read_text().replace(
        'case "${1:-}" in',
        kill + '\nsleep() { echo "$1" >> data/sleeps; }\ncase "${1:-}" in',
    ))
    state = tmp_path / "data/state"
    state.mkdir(parents=True)
    (state / "binance.pid").write_text("999999999")
    result = subprocess.run(["bash", str(script), "stop"], capture_output=True, text=True, timeout=5)
    assert result.returncode == 1
    assert "still alive after 60s: binance" in result.stderr
    assert (tmp_path / "data/sleeps").read_text().splitlines() == ["0.5"] * 120
