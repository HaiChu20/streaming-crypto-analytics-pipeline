import gzip
import json
from pathlib import Path

import pytest

from archiver import rawfmt
from collectors.common.config import all_streams, load_config
from scripts.convert_raw import convert, legacy_line, LegacyState
from scripts.check_raw import check_stream


def test_v1_preserves_offsets_connection_open_and_tail(tmp_path):
    source = tmp_path / 'v1/date=2026-10-16/exchange=kraken/market=spot/feed=l2/BTC-USD.jsonl.open'
    source.parent.mkdir(parents=True)
    e = dict(v=1, exchange='kraken', market='spot', feed='l2', symbol='BTC/USD', kind='snapshot',
             conn='original', recv_ns=1792108800000000000, payload='{"data":[{"price":1.2300}]}', offset=42)
    original = json.dumps(e).encode() + b'\n{"partial":'
    source.write_bytes(original)
    out = tmp_path / 'v2'
    report = convert(tmp_path / 'v1', out)
    target = out / 'kraken/spot/BTC-USD/20261016_l2.jsonl.open'
    r = json.loads(target.read_bytes())
    assert not target.with_name(target.name + '.meta.json').exists()
    assert r['offset'] == 42 and r['conn'] == 'original'
    assert r['data'][0]['price'] == '1.2300'
    assert report[0]['partial_bytes'] == len(b'{"partial":')
    assert source.read_bytes() == original
    before = target.read_bytes()
    with pytest.raises(FileExistsError):
        convert(tmp_path / 'v1', out)
    assert target.read_bytes() == before


def test_closed_v1_rollover_and_no_clobber_meta(tmp_path):
    source = tmp_path / 'v1/date=2026-10-16/exchange=okx/market=spot/feed=l2/BTC-USDT.jsonl'
    source.parent.mkdir(parents=True)
    e = dict(kind='control', event='start', conn='c', recv_ns=1792108800000000000, offset=7, detail={})
    source.write_text(json.dumps(e) + '\n')
    report = convert(tmp_path/'v1', tmp_path/'v2')
    target = Path(report[0]['output'])
    assert json.loads(target.read_bytes().splitlines()[-1])['marker'] == 'rollover'
    assert json.loads(target.with_name(target.name+'.meta.json').read_bytes())['format'] == 'v2'


def test_legacy_gzip_epochs_metadata_crc(tmp_path):
    instrument = dict(pair_decimals=1, lot_decimals=8, tick_size='0.1')
    ir = tmp_path/'instruments/date=2026-10-17/exchange=kraken/market=spot/feed=l2/BTC-USD.jsonl'
    ir.parent.mkdir(parents=True)
    ir.write_text(json.dumps(dict(event='start', detail=dict(instrument=instrument)))+'\n')
    reports = convert(Path('tests/fixtures/legacy'), tmp_path/'out', legacy=True, exchange='kraken', market='spot',
                      instrument_root=tmp_path/'instruments')
    assert len(reports) == 3
    streams = all_streams(load_config())
    for report in reports:
        path = Path(report['output'])
        with gzip.open(path, 'rt') as f:
            rows = [json.loads(x) for x in f]
        assert len(rows) <= 200
        assert [r['offset'] for r in rows if r.get('marker') != 'rollover'] == list(range(len(rows)))
        connected = [r for r in rows if r.get('event') == 'connected']
        assert len(connected) == 2
        assert connected[0]['conn'].endswith('#1') and connected[1]['conn'].endswith('#2')
        assert connected[0]['instrument'] == instrument and connected[0]['instrument_source'] == 'copied'
        assert rows[0]['event'] == 'disconnected' and rows[0]['reason'] == 'rotation'
        error = next(r for r in rows if r.get('event') == 'connection_error')
        assert error['reason'] == 'disconnect' and error['exc_type'] == 'ConnectionClosedError'
        meta = json.loads(path.with_name(path.name+'.meta.json').read_bytes())
        assert meta['synthetic_offsets'] is True
        feed = path.name.split('_')[1].split('.')[0]
        stream = next(s for s in streams if s.exchange=='kraken' and s.market=='spot' and s.asset=='BTC' and s.feed==feed)
        result = check_stream(stream, [path], 100, True)
        assert not result['errors']
        if feed in ('l2','l3'):
            assert result['crc_pass'] > 0 and result['crc_fail'] == 0
    with pytest.raises(FileExistsError):
        convert(Path('tests/fixtures/legacy'), tmp_path/'out', legacy=True, exchange='kraken', market='spot')


def test_legacy_rollover_kept():
    rec = dict(marker='rollover', new_period='20261017', timestamp='2026-10-17T00:00:00Z')
    assert json.loads(legacy_line(rec, 8, LegacyState('file'), 'book', None)) == rec


def test_legacy_markers_and_data_share_active_carrier():
    state = LegacyState('20261016_book.jsonl.gz')
    def marker(event):
        return dict(marker='ws_event', event=event, conn='Spot:book-1', timestamp='2026-10-16T00:00:00Z')
    data = dict(channel='book', type='update', recv_time_ns=1792108800000000000, data=[])
    records = [marker('connected'), data, marker('connection_error'), marker('connected'), data]
    out = [json.loads(legacy_line(r, i, state, 'book', None)) for i, r in enumerate(records)]
    assert [r['conn'] for r in out] == ['Spot:book-1#1']*3 + ['Spot:book-1#2']*2
    assert state.orphan_records == 0


def test_legacy_unconnected_data_has_separate_orphan_epochs():
    state = LegacyState('file')
    data = dict(channel='book', type='update', recv_time_ns=1, data=[])
    def emit(rec, i):
        return json.loads(legacy_line(rec, i, state, 'book', None))['conn']
    first = emit(data, 0)
    live = emit(dict(marker='ws_event', event='connected', conn='Spot:book-1', recv_time_ns=2), 1)
    assert emit(data, 2) == live
    assert emit(dict(marker='ws_event', event='connection_error', recv_time_ns=3), 3) == live
    after = emit(data, 4)
    assert emit(data, 5) == after
    assert len({first, live, after}) == 3
    assert state.orphan_records == 3


def test_legacy_open_has_no_metadata(tmp_path):
    source = tmp_path / 'legacy/BTC-USD/20261016_book.jsonl.open'
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps(dict(marker='ws_event', event='connected', conn='c', recv_time_ns=1)) + '\n')
    report = convert(tmp_path / 'legacy', tmp_path / 'out', legacy=True, exchange='kraken', market='spot')
    target = Path(report[0]['output'])
    assert target.name.endswith('.open')
    assert not target.with_name(target.name + '.meta.json').exists()
