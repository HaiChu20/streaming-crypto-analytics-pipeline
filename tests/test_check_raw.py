"""Offline checker verdicts and bounded trade overlap history."""

import gzip
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
    missing = 66 if has_key else 64
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
            result = subprocess.run(["bash", str(script), "stop"], capture_output=True, text=True, timeout=30)
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
    result = subprocess.run(["bash", str(script), "start"], capture_output=True, text=True, timeout=30)
    assert result.returncode == 1
    for name in ("binance", "okx", "kraken", "coinbase", "bybit", "deribit", "recorder"):
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
    result = subprocess.run(["bash", str(script), "stop"], capture_output=True, text=True, timeout=30)
    assert result.returncode == 1
    assert "still alive after 60s: binance" in result.stderr
    assert (tmp_path / "data/sleeps").read_text().splitlines() == ["0.5"] * 120


@pytest.mark.parametrize("later_gap", [False, True])
def test_collector_crash_carrier_stays_checkable(tmp_path, monkeypatch, later_gap):
    calls = []
    original_feed = check_raw.BookCheck.feed

    def feed(self, kind, payload):
        calls.append(kind)
        return original_feed(self, kind, payload)

    monkeypatch.setattr(check_raw.BookCheck, "feed", feed)
    crash = dict(control("drop", "new"), reason="collector_crash")
    records = [crash, control("start", "new"), book("snapshot", 10, conn="new"),
               book("update", 11, 10, "new"), book("update", 12, 11, "new")]
    if later_gap:
        records.append(book("update", 14, 13, "new"))
    result = check(tmp_path, records)
    assert calls == ["snapshot", "update", "update"] + (["update"] if later_gap else [])
    assert len(result["errors"]) == int(later_gap)
    if later_gap:
        assert "seqId break" in result["errors"][0]
    assert result["events"]["drop:collector_crash"] == 1
    assert result["events"]["start"] == 1


@pytest.mark.parametrize('feed', ['l2', 'l3'])
@pytest.mark.parametrize('bad', [False, True])
def test_v2_kraken_crc_pass_and_fail(tmp_path, feed, bad):
    import json
    from archiver import rawfmt
    stream = replace(BOOK, exchange='kraken', symbol='BTC/USD', feed=feed)
    source = Path(f'tests/fixtures/kraken_docs/{feed}_checksum_example.json')
    payload = source.read_text()
    if bad:
        p = json.loads(payload, parse_float=str)
        p['data'][0]['checksum'] = 1
        payload = json.dumps(p)
    path = tmp_path/'20261016_l2.jsonl'
    e = dict(kind='snapshot', payload=payload, recv_ns=1, conn='c', exchange='kraken')
    path.write_bytes(rawfmt.line(e, 0) + rawfmt.rollover('20261017'))
    result = check_raw.check_stream(stream, [path], 100, True)
    assert result['crc_fail'] == int(bad)
    assert result['crc_pass'] == int(not bad)
    assert len(result['errors']) == int(bad)
    assert result['file_stats'][0]['lines'] == 2


def test_v2_discovery_and_date_filter(tmp_path):
    from archiver.record_raw import stream_path
    for day in ['2026-10-15', '2026-10-16']:
        p = stream_path(tmp_path, BOOK, day)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.with_name(p.name+'.open').touch()
    found = check_raw.files_for(tmp_path, BOOK, ['2026-10-16'])
    assert len(found) == 1 and found[0].name == '20261016_book.jsonl.open'
    assert check_raw.files_for(tmp_path/'raw', BOOK, None, raw_root=True) == check_raw.files_for(tmp_path, BOOK, None)


@pytest.mark.parametrize("v2_suffix", ["", ".open", ".gz"])
@pytest.mark.parametrize("v1_suffix", ["", ".open", ".gz"])
@pytest.mark.parametrize("dates", [None, ["2026-10-17"]])
def test_v2_day_ignores_truncated_v1_copy(tmp_path, v2_suffix, v1_suffix, dates):
    from archiver import rawfmt
    from archiver.record_raw import stream_path

    v2 = stream_path(tmp_path, TRADES, "2026-10-17")
    v2 = v2.with_name(v2.name + v2_suffix)
    v1 = tmp_path / "raw/date=2026-10-17/exchange=okx/market=spot/feed=trades/BTC-USDT.jsonl"
    v1 = v1.with_name(v1.name + v1_suffix)
    records = [dict(trade(str(i)), recv_ns=1, exchange="okx") for i in range(2)]
    for path, content in [
        (v2, b"".join(rawfmt.line(r, i) for i, r in enumerate(records))),
        (v1, orjson.dumps(dict(trade("different"), offset=0)) + b"\n"),
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(content) if path.suffix == ".gz" else content)

    paths = check_raw.files_for(tmp_path, TRADES, dates)
    assert paths == [v2]
    assert check_raw.files_for(tmp_path / "raw", TRADES, dates, raw_root=True) == paths
    result = check_raw.check_stream(TRADES, paths, depth=None, deep=True)
    assert result["files"] == 1
    assert result["records"] == 2
    assert not result["errors"]


def test_v2_precedence_is_per_stream_and_day(tmp_path):
    from archiver.record_raw import stream_path

    v2 = stream_path(tmp_path, BOOK, "2026-10-17")
    old_day = tmp_path / "raw/date=2026-10-16/exchange=okx/market=spot/feed=l2/BTC-USDT.jsonl"
    other_stream = tmp_path / "raw/date=2026-10-17/exchange=okx/market=spot/feed=trades/BTC-USDT.jsonl"
    for path in (v2, old_day, other_stream):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    assert check_raw.files_for(tmp_path, BOOK, None) == [old_day, v2]
    assert check_raw.files_for(tmp_path, TRADES, ["2026-10-17"]) == [other_stream]


def test_restart_only_targets_named_service(tmp_path):
    script = pipeline_copy(tmp_path)
    # Fake all process operations, including start; no signals or real services.
    text = script.read_text().replace('case "${1:-}" in', '''
alive() { [[ "$1" == binance && ! -f data/stopped ]]; }
kill() { echo "$*" >> data/signals; touch data/stopped; }
nohup() { echo "$*" >> data/started; }
sleep() { :; }
case "${1:-}" in''')
    # The post-start liveness check is also simulated.
    text = text.replace('if ! alive "$name"; then echo "FAILED to start $name, see data/logs/$name.log" >&2; exit 1; fi', ':')
    script.write_text(text)
    state = tmp_path/'data/state'
    state.mkdir(parents=True)
    (state/'binance.pid').write_text('999999999')
    result = subprocess.run(['bash', str(script), 'restart', 'binance'], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0
    assert (tmp_path/'data/signals').read_text().strip() == '-TERM 999999999'
    assert '-m collectors binance' in (tmp_path/'data/started').read_text()
    assert not (state/'okx.pid').exists()
    bad = subprocess.run(['bash', str(script), 'restart', 'unknown'], capture_output=True, text=True)
    assert bad.returncode == 2


def test_v2_raw_fallback_checks_native_collision_and_skips_invalid_text(tmp_path):
    import json
    from archiver import rawfmt
    stream = replace(BOOK, exchange='kraken', symbol='BTC/USD')
    doc = Path('tests/fixtures/kraken_docs/l2_checksum_example.json').read_text()
    native = json.loads(doc, parse_float=str)
    native['offset'] = 999  # force exact-raw fallback
    e = dict(kind='snapshot', payload=json.dumps(native), recv_ns=1, conn='c', exchange='kraken',
             market='spot', symbol='BTC/USD', feed='l2')
    path = tmp_path/'20261016_l2.jsonl'
    path.write_bytes(rawfmt.line(e, 0) + rawfmt.line(dict(e, kind='update', payload='bad json'), 1))
    result = check_raw.check_stream(stream, [path], 100, True)
    assert result['records'] == 2
    assert result['crc_pass'] == 1
    assert not result['errors']


@pytest.mark.parametrize('feed', ['l2','l3'])
def test_real_archive_kraken_crc(feed):
    root = Path('tests/fixtures/archive')
    stream = next(s for s in check_raw.discovered_streams(root) if s.exchange == 'kraken' and s.market == 'spot' and s.feed == feed and s.symbol == 'BTC/USDT')
    result = check_raw.check_stream(stream, check_raw.files_for(root, stream, None, raw_root=True), None, True)
    assert not result['errors'] and result['crc_pass'] > 0 and result['crc_fail'] == 0


def test_binance_archive_snapshot_is_used_and_periodic_is_ignored(tmp_path):
    stream = replace(BOOK, exchange='binance', symbol='BTCUSDT')
    from archiver import rawfmt
    paths = []
    base = dict(exchange='binance', market='spot', feed='l2', symbol='BTCUSDT', conn='c')
    for channel, rows in [
        ('depth', [(0, 'update', {'U':101,'u':102}), (3, 'update', {'U':103,'u':104}), (4, 'update', {'U':107,'u':108})]),
        ('snapshot', [(1, 'snapshot', {'lastUpdateId':101}), (2, 'snapshot', {'lastUpdateId':9999})])]:
        p = tmp_path/f'20261016_{channel}.jsonl'
        p.write_bytes(b''.join(rawfmt.line(base | dict(kind=kind, recv_ns=offset+1, payload=orjson.dumps(native).decode(), event='periodic' if offset == 2 else 'anchor'), offset) for offset, kind, native in rows))
        paths.append(p)
    result = check_raw.check_stream(stream, paths, None, True)
    assert len(result['errors']) == 1 and 'update-id break U=107' in result['errors'][0]
    assert not any('offset hole' in error for error in result['errors'])
