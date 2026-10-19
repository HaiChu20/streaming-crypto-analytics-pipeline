"""Offline DVOL contracts using the documented REST response shape."""
import asyncio
from datetime import date
import json
from pathlib import Path

import httpx
import pytest

from archiver import rawfmt
from archiver.record_raw import Recorder
from collectors import deribit
from collectors.common import runner
from collectors.common.config import load_config
from scripts.backfill_dvol import backfill, day_content, request_page
from scripts.check_raw import stream_records

TS = 1792195200000
DAY = date(2026, 10, 17)


def page(rows, continuation=None):
    return '{"jsonrpc":"2.0","result":{"data":[' + ','.join(rows) + '],"continuation":' + json.dumps(continuation) + '}}'


def row(ts, close='52.30000000'):
    return f'[{ts},52.3000,5.240E+1,52.10000000000001,{close}]'


class Http:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    async def get(self, url, params):
        assert url == deribit.URL
        self.calls.append(dict(params))
        reply = next(self.replies)
        status, text = reply if isinstance(reply, tuple) else (200, reply)
        return httpx.Response(status, text=text, request=httpx.Request('GET', url))


class Sink:
    def __init__(self):
        self.rows = []
        self.lost = set()
        self.accept = True

    def send(self, stream, value):
        if not self.accept:
            self.lost.add(stream.key)
            return False
        self.rows.append((stream, value))
        return True

    def has_room(self):
        return self.accept

    def clear_lost(self, keys):
        self.lost -= keys


async def no_sleep(_):
    pass


def test_poller_closed_sorted_dedup_and_v3_reader_roundtrip(monkeypatch, tmp_path):
    monkeypatch.setattr(deribit.time, 'time_ns', lambda: (TS + 185000) * 1_000_000)
    monkeypatch.setattr(runner, '_last_recv_ns', 0)
    payload = page([row(TS + 180000), row(TS + 120000), row(TS + 60000), row(TS)])
    http, sink = Http([payload, payload, payload]), Sink()
    poller = deribit.DvolPoller(http, sink, load_config())
    btc, eth = poller.streams
    asyncio.run(poller.poll_once(btc))
    asyncio.run(poller.poll_once(btc))
    asyncio.run(poller.poll_once(eth))
    data = [(s, json.loads(v)) for s, v in sink.rows if json.loads(v)['kind'] == 'update']
    assert [(s.symbol, json.loads(e['payload'])[0]) for s, e in data] == [
        (currency, TS + lag) for currency in ('BTC', 'ETH') for lag in (0, 60000, 120000)]
    assert data[0][1]['payload'] == row(TS)
    assert all(e['recv_ns'] == (TS + 185000) * 1_000_000 for _, e in data)
    assert [s.partition for s in poller.streams] == [0, 1]
    assert {s.topic for s in poller.streams} == {'raw.deribit.index.dvol.v1'}
    assert http.calls[0] == dict(currency='BTC', start_timestamp=TS-115000,
                                 end_timestamp=TS+185000, resolution=60)
    recorder = Recorder(tmp_path, {(btc.topic, 0): btc})
    for offset, (stream, value) in enumerate((s, v) for s, v in sink.rows if s.symbol == 'BTC'):
        recorder.handle(stream.topic, stream.partition, offset, value)
    recorder.shutdown()
    path = tmp_path / 'raw/Deribit/Index/BTC/20261017_dvol.jsonl.open'
    assert rawfmt.archive_identity(path) == ('deribit', 'index', 'BTC', 'dvol')
    decoded = [item[3] for item in stream_records(path, {'lines': 0})]
    candle = decoded[1]
    assert candle['data'] == [TS, '52.3000', '5.240E+1', '52.10000000000001', '52.30000000']
    assert candle['channel'] == 'dvol' and candle['offset'] == 1
    assert rawfmt.archive_identity(Path('DeribitRaw/IndexV2/ETH/20261017_dvol.jsonl'))[:2] == ('deribit','index')


def test_documented_fixture_number_tokens_and_invalid_shape():
    rows, continuation = deribit.decode_page(Path('tests/fixtures/deribit_docs/dvol.json').read_text())
    assert rows[0].text == row(TS)
    assert continuation is None
    fixture = Path('tests/fixtures/dvol/Deribit/Index/BTC/20261017_dvol.jsonl').read_bytes()
    assert rawfmt.line(dict(exchange='deribit', market='index', feed='dvol', symbol='BTC',
                           kind='update', conn='D', recv_ns=(TS+65000)*1_000_000,
                           payload=rows[0].text), 0) == fixture
    for text in [page(['[0,"1",2,3,4]']), page(['[1,1,2,3,4]']), page(['[0,1,2,3,NaN]']),
                 page(['[0,1,2,3,-1]']), '{"error":{"message":"bad request"}}']:
        with pytest.raises((ValueError, KeyError)):
            deribit.decode_page(text)


def test_poller_drop_backoff_restart_keeps_dedup(monkeypatch):
    monkeypatch.setattr(deribit.time, 'time_ns', lambda: (TS + 185000) * 1_000_000)
    monkeypatch.setattr(runner, '_last_recv_ns', 0)
    stop, rotate = asyncio.Event(), asyncio.Event()
    replies = [page([row(TS)]), page([row(TS)]), (500, 'failed'), page([row(TS)]), page([row(TS)])]
    http, sink = Http(replies), Sink()
    poller = deribit.DvolPoller(http, sink, load_config())
    delays = []

    async def wait_for(awaitable, timeout):
        awaitable.close()
        delays.append(timeout)
        if len(delays) == 4:
            stop.set()
            return True
        raise TimeoutError

    monkeypatch.setattr(deribit.asyncio, 'wait_for', wait_for)
    asyncio.run(poller.run(stop, rotate))
    records = [json.loads(v) for _, v in sink.rows]
    assert len([e for e in records if e['kind'] == 'update']) == 2
    drops = [e for e in records if e.get('event') == 'drop']
    assert drops[0]['reason'] == 'disconnect'
    btc_starts = [e for e in records if e.get('event') == 'start' and e['symbol'] == 'BTC']
    assert len(btc_starts) == 2 and btc_starts[0]['conn'] != btc_starts[1]['conn']
    assert 1 <= delays[2] < 2


def test_backfill_pagination_recv_day_close_meta_and_idempotence(tmp_path):
    # Newest-first pages overlap at the continuation; previous day's 23:59
    # candle belongs in this receive day's file, this day's 23:59 does not.
    replies = [page([row(TS + 86340000), row(TS + 86280000), row(TS)], TS),
               page([row(TS), row(TS-60000)])]
    http = Http(replies * 2)
    async def go():
        a = await backfill(http, tmp_path, DAY, DAY, ['BTC'], sleep=no_sleep, now_ms=TS+86400000)
        b = await backfill(http, tmp_path, DAY, DAY, ['BTC'], sleep=no_sleep, now_ms=TS+86400000)
        return a, b
    a, b = asyncio.run(go())
    target = a[0][0]
    assert a[0][1] == 'written' and b[0][1] == 'skipped identical'
    assert http.calls[1]['end_timestamp'] == TS
    assert http.calls[0]['start_timestamp'] == TS-70000
    lines = [json.loads(x) for x in target.read_bytes().splitlines()]
    assert [r['data'][0] for r in lines[:-1]] == [TS-60000, TS, TS+86280000]
    assert [r['offset'] for r in lines[:-1]] == [0, 1, 2]
    assert all(r['recv_time_ns'] == (r['data'][0] + 70000) * 1_000_000 for r in lines[:-1])
    assert all(r['conn'] == 'backfill' for r in lines[:-1])
    assert lines[-1] == dict(marker='rollover', new_period='20261018', timestamp='2026-10-18T00:00:00+00:00')
    meta = json.loads(target.with_name(target.name+'.meta.json').read_text())
    assert meta['foreign_day_records'] == 0 and meta['offset_gaps'] == []
    assert meta['synthetic_offsets'] and meta['receive_lag_ms'] == 70000
    assert meta['first_offset'] == 0 and meta['last_offset'] == 2
    before = target.read_bytes()
    with pytest.raises(FileExistsError, match='different DVOL content'):
        asyncio.run(backfill(Http([page([row(TS, '99')])]), tmp_path, DAY, DAY, ['BTC'],
                             sleep=no_sleep, now_ms=TS+86400000))
    assert target.read_bytes() == before


def test_backfill_retries_429_and_5xx_only():
    sleeps = []
    async def sleep(delay):
        sleeps.append(delay)
    http = Http([(429, ''), (503, ''), page([])])
    assert asyncio.run(request_page(http, 'ETH', TS, TS+1000, sleep))[0] == []
    assert sleeps == [1, 2, 0.25]
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(request_page(Http([(400, '')]), 'ETH', TS, TS+1000, sleep))
    with pytest.raises(ValueError, match='did not move backwards'):
        asyncio.run(day_content(Http([page([], TS+86400000)]), 'BTC', DAY, no_sleep))


def test_backfill_refuses_current_days_and_existing_open_files(tmp_path):
    with pytest.raises(ValueError, match='closed UTC dates'):
        asyncio.run(backfill(Http([]), tmp_path, DAY, DAY, ['BTC'], now_ms=TS))
    path = rawfmt.path(tmp_path, 'deribit', 'index', 'ETH', DAY.isoformat(), 'dvol')
    path.parent.mkdir(parents=True)
    opened = path.with_name(path.name + '.open')
    opened.write_bytes(b'existing live data\n')
    with pytest.raises(FileExistsError, match='representation'):
        asyncio.run(backfill(Http([page([])]), tmp_path, DAY, DAY, ['ETH'], sleep=no_sleep, now_ms=TS+86400000))
    assert opened.read_bytes() == b'existing live data\n' and not path.exists()


def test_poller_kafka_recovery_and_previous_crash_markers(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(deribit.time, 'time_ns', lambda: (TS + 185000) * 1_000_000)
    monkeypatch.setattr(runner, '_last_recv_ns', 0)
    stop, rotate = asyncio.Event(), asyncio.Event()
    sink = Sink()
    session = SimpleNamespace(id='new', crash_detail=lambda: {'prev_session': 'crashed'})
    poller = deribit.DvolPoller(Http([page([row(TS)])] * 4), sink, load_config(), session)
    iterations = 0

    async def wait_for(awaitable, timeout):
        nonlocal iterations
        awaitable.close()
        iterations += 1
        if iterations == 2:
            sink.lost.add(poller.streams[0].key)
        if iterations == 3:
            stop.set()
            return True
        raise TimeoutError

    monkeypatch.setattr(deribit.asyncio, 'wait_for', wait_for)
    asyncio.run(poller.run(stop, rotate))
    records = [json.loads(v) for _, v in sink.rows]
    assert [r['reason'] for r in records if r.get('event') == 'drop'] == [
        'collector_crash', 'collector_crash', 'kafka_unavailable']
    assert len([r for r in records if r['kind'] == 'update' and r['symbol'] == 'BTC']) == 2
    assert len([r for r in records if r['kind'] == 'update' and r['symbol'] == 'ETH']) == 1
    assert not sink.lost


@pytest.mark.parametrize('remaining', [0, 1])
def test_poller_flush_marks_only_clean_session_stopped(monkeypatch, remaining):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    class Client:
        async def __aenter__(self):
            return Http([])
        async def __aexit__(self, *_):
            pass

    monkeypatch.setattr(deribit.httpx, 'AsyncClient', lambda **_: Client())
    monkeypatch.setattr(deribit.DvolPoller, 'run', AsyncMock())
    sink = SimpleNamespace(run=AsyncMock(), flush=Mock(return_value=remaining))
    session = SimpleNamespace(heartbeat=AsyncMock(), stop=Mock(), crash_detail=lambda: None)
    clean = asyncio.run(deribit.run_poller(sink, load_config(), session, asyncio.Event(), asyncio.Event()))
    assert clean == (remaining == 0)
    assert session.stop.call_count == (1 if clean else 0)
    sink.flush.assert_called_once_with(30)
