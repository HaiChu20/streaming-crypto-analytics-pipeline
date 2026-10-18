import json
from pathlib import Path

import pytest

from archiver import rawfmt


def envelope(payload, **kwargs):
    return dict(kind="update", recv_ns=123, conn="c", payload=payload, **kwargs)


def test_every_envelope_fixture_is_lossless():
    count = 0
    for path in Path("tests/fixtures").glob("*/*/*.jsonl"):
        with path.open() as f:
            for source in f:
                e = json.loads(source)
                if "payload" not in e:
                    continue
                original = json.loads(e["payload"], parse_float=str)
                actual = json.loads(rawfmt.line(e, 7), parse_float=str)
                for key in ("recv_time_ns", "conn", "offset"):
                    actual.pop(key)
                if e["exchange"] == "binance":
                    original = original.get("data", original)
                if e["exchange"] != "kraken" or e["market"] == "perp":
                    actual = actual["data"]
                assert actual == original, path
                count += 1
    assert count > 7000


@pytest.mark.parametrize("text", ['0.00060000', '1e-8', '-1.20E+3', '1.00000000000000000001'])
def test_float_token_text_is_unchanged(text):
    result = json.loads(rawfmt.line(envelope('{"x":' + text + ',"data":[]}'), 0))
    assert result["x"] == text
    assert list(result) == ["x", "recv_time_ns", "conn", "offset", "data"]


@pytest.mark.parametrize("number", [2**63, 2**64, -(2**70), 10**100])
def test_large_integers_remain_integers(number):
    assert json.loads(rawfmt.line(envelope('{"x":%d}' % number), 0))["x"] == number


@pytest.mark.parametrize("payload", ['[]', 'null', '"hello"', 'not json', '1'])
def test_non_object_keeps_raw_text(payload):
    assert json.loads(rawfmt.line(envelope(payload), 3)) == dict(channel=None, recv_time_ns=123, conn="c", offset=3, raw=payload)


@pytest.mark.parametrize('native', ['recv_time_ns', 'conn', 'offset'])
def test_collision_stores_exact_payload_and_warns_once_per_stream(native, caplog):
    stream = ('kraken', 'spot', 'BTC/USD', 'l2')
    e = envelope('{"' + native + '":999,"data":[1.2300]}', exchange=stream[0], market=stream[1],
                 symbol=stream[2], feed=stream[3])
    before = rawfmt.collisions[stream]
    with caplog.at_level('WARNING'):
        r = json.loads(rawfmt.line(e, 3))
        rawfmt.line(e, 4)
    assert r == dict(channel='book', recv_time_ns=123, conn='c', offset=3, raw=e['payload'])
    assert rawfmt.collisions[stream] == before + 2
    assert len([m for m in caplog.records if 'collides with raw metadata' in m.message]) == int(before == 0)


def test_binance_snapshot_native_type_is_nested():
    text = '{"type":"snapshot","lastUpdateId":1,"bids":[]}'
    e = dict(kind='snapshot', exchange='binance', market='spot', symbol='BTCUSDT', feed='l2',
             recv_ns=123, conn='c', payload=text)
    assert json.loads(rawfmt.line(e, 8))['data'] == json.loads(text)


def test_marker_detail_collision_nests_only_clashing_keys():
    e = dict(kind='control', event='start', recv_ns=123, conn='c', feed='l2',
             detail={'channel':'book', 'offset':999, 'reason':'native', 'instrument':{'tick_size':'0.1'}})
    r = json.loads(rawfmt.line(e, 8))
    assert r['offset'] == 8 and r['channel'] == 'book'
    assert r['instrument'] == {'tick_size':'0.1'}
    assert r['reason'] == 'native'
    assert r['detail'] == {'channel':'book', 'offset':999}


@pytest.mark.parametrize("event,mapped", list(rawfmt.EVENTS.items()))
def test_markers(event, mapped):
    r = json.loads(rawfmt.line(dict(kind="control", recv_ns=123, conn="c", event=event,
                                   detail={"channel": "book", "instrument": {"pair_decimals": 1}}), 4))
    assert r["event"] == mapped and r["channel"] == "book"
    assert r["instrument"] == {"pair_decimals": 1}
    assert r["offset"] == 4


def test_json_document_fixtures_are_lossless():
    for path in Path('tests/fixtures').rglob('*.json'):
        text = path.read_text()
        original = json.loads(text, parse_float=str)
        actual = json.loads(rawfmt.line(envelope(text), 7), parse_float=str)
        for key in ('recv_time_ns', 'conn', 'offset'):
            actual.pop(key)
        assert actual == original, path


def test_control_path_and_native_channel():
    assert str(rawfmt.path(Path('raw'), 'control', '', '', '2026-10-16', '')) == 'raw/control/20261016_control.jsonl'
    e = dict(kind='control', event='drop', conn='c', recv_ns=1, exchange='kraken', feed='l2', reason='disconnect')
    assert json.loads(rawfmt.line(e, 0))['channel'] == 'book'


def test_invalid_json_constants_remain_raw():
    text = '{"x":NaN}'
    assert json.loads(rawfmt.line(envelope(text), 0))['raw'] == text


def test_escaped_unicode_surrogate_is_preserved():
    text = '{"x":"\\ud800"}'
    assert json.loads(rawfmt.line(envelope(text), 0))['x'] == json.loads(text)['x']


def test_legacy_fixture_native_data_is_lossless():
    import gzip
    from scripts.convert_raw import LegacyState, legacy_line
    for path in Path('tests/fixtures/legacy').rglob('*.gz'):
        state = LegacyState(path.name)
        with gzip.open(path, 'rt') as f:
            for offset, text in enumerate(f):
                original = json.loads(text, parse_float=str)
                result = json.loads(legacy_line(original, offset, state, 'book', None))
                if original.get('marker'):
                    continue
                native = {k:v for k,v in original.items() if k not in ('recv_time_ns','conn','offset')}
                actual = {k:v for k,v in result.items() if k not in ('recv_time_ns','conn','offset')}
                assert actual == native, path


@pytest.mark.parametrize("source", sorted(Path("tests/fixtures/archive").rglob("*.gz")), ids=lambda p: str(p.relative_to("tests/fixtures/archive")))
def test_archive_data_lines_match_bytes(source):
    import gzip
    exchange, market, pair, file = rawfmt.archive_identity(source)
    feed = next(f for f, (c, _) in rawfmt.CHANNELS[exchange].items() if c == file)
    if feed == "snapshot":
        feed = "l2"
    shapes = set()
    with gzip.open(source, "rb") as f:
        for original in f:
            rec = rawfmt.loads(original.decode())
            if rec.get("marker"):
                continue
            native = ({k: v for k, v in rec.items() if k != "recv_time_ns"}
                      if exchange == "kraken" and market == "spot" else rec["data"])
            kind = "snapshot" if (file == "snapshot" or native.get("type") == "snapshot"
                                    or native.get("action") == "snapshot" or native.get("feed", "").endswith("_snapshot")) else "update"
            shape = kind, rec.get("feed"), rec.get("channel")
            if shape in shapes:
                continue
            shapes.add(shape)
            if exchange == "binance" and kind != "snapshot":
                native = dict(stream="combined@stream", data=native)
            e = dict(exchange=exchange, market=market, symbol=pair, feed=feed, kind=kind,
                     payload=rawfmt.dumps(native).decode(), recv_ns=rec["recv_time_ns"], conn="ours")
            if "event" in rec:
                e["event"] = rec["event"]
            result = rawfmt.loads(rawfmt.line(e, 42).decode())
            result.pop("conn")
            result.pop("offset")
            assert rawfmt.dumps(result) + b"\n" == original
    assert shapes


@pytest.mark.parametrize('exchange,market,feed,name,file', [
    ('binance','spot','l1','bookTicker','bookticker'),
    ('binance','perp','trades_agg','aggTrade','aggtrades'),
    ('binance','perp','mark','markPrice','markprice'),
    ('binance','perp','liquidations','forceOrder','forceorder'),
    ('okx','perp','trades','tradesAll','tradesall'),
    ('okx','spot','trades_agg','trades','trades'),
    ('kraken','perp','l2','book','book'),
])
def test_wrapped_channels_paths_decimals_and_fallback(exchange, market, feed, name, file):
    e = envelope('{"price":1.200E-8,"quantity":0.00001000}', exchange=exchange, market=market,
                 feed=feed, symbol='PAIR')
    r = json.loads(rawfmt.line(e, 0))
    assert r['feed' if exchange == 'kraken' else 'channel'] == name
    assert r['data'] == {'price':'1.200E-8','quantity':'0.00001000'}
    p = rawfmt.path(Path('raw'), exchange, market, 'PAIR', '2026-10-16', feed)
    assert p.parts[-3] == ('Spot' if market == 'spot' else 'Futures')
    assert p.name == f'20261016_{file}.jsonl'
    r = json.loads(rawfmt.line(dict(e, payload='[1.200E-8]'), 1))
    assert r['channel'] == name and r['raw'] == '[1.200E-8]'


def test_archive_synthetic_headers_and_optional_offsets():
    headers = rawfmt.Headers('file')
    assert headers.read({'data':[]}, 0)['conn'] == 'file#pre'
    marker = dict(marker='ws_event', event='connected', conn='native', timestamp='2026-10-16T00:00:00.123456789Z')
    rec = headers.read(marker, 1)
    assert rec['conn'] == 'native#1' and rec['offset'] == 1
    assert rec['recv_time_ns'] % 10**9 == 123456789
    assert headers.read({'data':[]}, 2)['conn'] == 'native#1'
    assert headers.read(marker, 3)['conn'] == 'native#2'
    explicit = headers.read({'conn':'ours', 'data':[]}, 4)
    assert explicit['conn'] == 'ours' and explicit['offset'] == 4


def test_control_decimal_tokens_are_exact_too():
    e = b'{"kind":"control","event":"start","exchange":"okx","feed":"l2","recv_ns":1,"conn":"c","detail":{"instrument":{"ctVal":0.00100}}}'
    assert json.loads(rawfmt.line(e, 0))['instrument']['ctVal'] == '0.00100'
