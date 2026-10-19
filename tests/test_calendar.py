"""Shared calendar timing, provenance and updater contracts (no network)."""
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from research.calendar import Calendar, calendar_features
from scripts.update_events import local_release, parse_bls_archive, parse_fed, parse_ics

FIXTURES = Path(__file__).parent / 'fixtures' / 'calendar'
EVENTS = Path(__file__).parents[1] / 'config' / 'events.csv'


def ns(value):
    return int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1_000_000_000)


def features(*values):
    return calendar_features(pl.Series('tick_ns', [ns(v) for v in values]), Calendar.load(EVENTS))


def test_dst_release_conversion():
    assert local_release('cpi', '2024-01-11') == '2024-01-11T13:30:00Z'
    assert local_release('nfp', '2024-07-05') == '2024-07-05T12:30:00Z'
    assert local_release('fomc', '2024-01-31') == '2024-01-31T19:00:00Z'
    assert local_release('fomc', '2024-07-31') == '2024-07-31T18:00:00Z'


def test_macro_exact_cap_and_coverage():
    frame = features('2025-10-24T12:29:00Z', '2025-10-24T12:30:00Z',
                     '2022-12-31T23:59:59Z', '2026-11-01T00:00:00Z')
    assert frame['cal_cpi_min_to_next'].to_list() == [1., 10080., None, None]
    assert frame['cal_cpi_min_since_last'][1] == 0.
    assert frame['cal_hour_sin'].null_count() == 0
    assert all(t == pl.Float64 for t in frame.schema.values())


def test_shutdown_dates():
    releases = [r[1] for r in Calendar.load(EVENTS).rows if r[0] == 'cpi']
    assert '2025-10-24T12:30:00Z' in releases
    assert not any(r.startswith('2025-11') for r in releases)
    assert '2025-12-18T13:30:00Z' in releases


@pytest.mark.parametrize('tick,expected', [
    ('2024-03-29T07:59:00Z', (1., 1., 1., 1.)),
    ('2024-05-31T07:59:00Z', (1., 1., 1., 40321.)),
    ('2024-03-29T08:00:00Z', (1440., 10080., 40320., 131040.)),
])
def test_expiry_edges(tick, expected):
    frame = features(tick)
    actual = tuple(frame[f'cal_expiry_min_to_{kind}'][0] for kind in
                   ('daily', 'weekly', 'monthly', 'quarterly'))
    assert actual == expected


@pytest.mark.parametrize('tick,expected', [
    ('2024-03-08T14:29:59Z', 0.), ('2024-03-08T14:30:00Z', 1.),
    ('2024-03-08T21:00:00Z', 0.), ('2024-03-11T13:30:00Z', 1.),
    ('2024-03-11T20:00:00Z', 0.), ('2024-11-04T14:30:00Z', 1.),
    ('2024-03-09T15:00:00Z', 0.),
])
def test_session_boundaries(tick, expected):
    assert features(tick)['cal_us_session'][0] == expected


def test_updater_parsers():
    rows = parse_bls_archive((FIXTURES / 'bls.html').read_text(), 'cpi')
    assert [r[1] for r in rows] == ['2025-10-24T12:30:00Z', '2025-12-18T13:30:00Z']
    upcoming = parse_ics((FIXTURES / 'bls.ics').read_text())
    assert {r[:2] for r in upcoming} == {('cpi', '2026-11-10T13:30:00Z'),
                                           ('nfp', '2026-11-06T13:30:00Z')}
    fed = parse_fed((FIXTURES / 'fed.html').read_text())
    assert ('fomc', '2023-02-01T19:00:00Z') in {r[:2] for r in fed}
    assert not any(r[1].startswith('2025-08-22') for r in fed)


def test_clocks_fractional_weekday_and_funding():
    frame = features('2024-01-01T06:00:00Z', '2024-01-06T00:00:00Z',
                     '2024-01-07T00:00:00Z', '2024-01-01T08:00:00Z')
    from math import sin, tau
    assert frame['cal_hour_sin'][0] == pytest.approx(1.)
    assert frame['cal_dow_sin'][0] == pytest.approx(sin(.25 / 7 * tau))
    assert frame['cal_weekend'].to_list() == [0., 1., 1., 0.]
    assert frame['cal_funding_min_to_8h'].to_list() == [120., 480., 480., 480.]


def test_macro_values_null_near_coverage_edges():
    frame = features('2023-01-01T00:00:00Z', '2026-10-31T23:59:59Z',
                     '2025-10-24T12:31:00Z')
    assert frame['cal_cpi_min_since_last'].to_list() == [None, None, 1.]
    assert frame['cal_cpi_min_to_next'].null_count() == 2


def test_expiry_year_rollover():
    frame = features('2024-12-27T08:00:00Z', '2025-01-31T08:00:00Z')
    assert frame['cal_expiry_min_to_monthly'].to_list() == [35 * 1440., 28 * 1440.]
    assert frame['cal_expiry_min_to_quarterly'].to_list() == [91 * 1440., 56 * 1440.]


def test_empty_and_duplicate_unsorted_ticks():
    cal = Calendar.load(EVENTS)
    empty = calendar_features(pl.Series('tick_ns', [], dtype=pl.Int64), cal)
    assert empty.shape == (0, 17)
    values = ['2024-07-05T12:30:00Z', '2024-01-11T13:30:00Z', '2024-07-05T12:30:00Z']
    frame = features(*values)
    assert frame.row(0) == frame.row(2)
    assert frame['cal_nfp_min_since_last'][0] == 0.
    assert frame['cal_cpi_min_since_last'][1] == 0.


def test_repository_relative_path_independent_of_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert Calendar.load('config/events.csv').sha256 == Calendar.load(EVENTS).sha256


def test_build_matrix_includes_calendar_and_can_disable():
    from inference.service import SCHEMA
    from research.dataset import build_matrix
    from research.labels import load_spec
    tick = ns('2024-07-05T12:30:00Z')
    row = dict(exchange='binance', market='spot', symbol='BTCUSDT', asset='BTC',
               tick_ns=tick, available_ns=tick, engine_version='test', replay_mode='as_live',
               book_valid=True, warmup=False, mid_e9=100_000_000_000)
    matrix, names = build_matrix(pl.DataFrame([row], schema=SCHEMA), 'BTC_spot')
    assert 'cal_nfp_min_since_last' in names
    assert matrix['cal_nfp_min_since_last'][0] == 0.
    assert names == sorted(names)
    spec = load_spec()
    spec.pop('calendar')
    assert not any(n.startswith('cal_') for n in build_matrix(pl.DataFrame([row], schema=SCHEMA),
                                                            'BTC_spot', spec=spec)[1])


def test_training_metadata_and_native_model_calendar_validation(tmp_path, caplog):
    from inference.service import SCHEMA, load_models
    from research.labels import load_spec
    from research.train import train_target
    from scripts.update_events import render_events
    import json
    import numpy as np
    start = ns('2024-01-01T00:00:00Z')
    rows = [dict(exchange='binance', market='spot', symbol='BTCUSDT', asset='BTC',
                 tick_ns=start + t * 1_000_000_000, available_ns=start + t * 1_000_000_000,
                 engine_version='test', replay_mode='as_live', book_valid=True, warmup=False,
                 mid_e9=100_000_000_000 + int(100_000_000 * np.sin(t / 170)))
            for t in range(0, 7201, 10)]
    path = tmp_path / 'events.csv'
    path.write_bytes(EVENTS.read_bytes())
    spec = dict(load_spec(), calendar=str(path))
    report = train_target(pl.DataFrame(rows, schema=SCHEMA), 'BTC_spot', 900, tmp_path,
                          'calendar', n_folds=2, rounds=1, spec=spec)
    cal = Calendar.load(path)
    assert report['calendar'] == cal.metadata(report['data_window']['end_ns'])
    assert len(report['calendar']['sha256']) == 64
    assert report['calendar']['covers_through'] == '2026-10-31'
    stored = json.loads((Path(report['model_dir']) / 'metadata.json').read_text())
    assert stored['calendar'] == report['calendar']
    assert len(load_models(tmp_path, 'calendar')) == 1
    # Add only a future date: identical historical events allow the new sha.
    path.write_text(render_events([*cal.rows, ('cpi', '2026-11-10T13:30:00Z', 'fixture')],
                                  cal.covers_from, '2026-11-10'))
    assert len(load_models(tmp_path, 'calendar')) == 1
    # Move a historical release: refuse before loading model artifacts.
    changed = [(event, '2023-02-14T13:31:00Z' if stamp == '2023-02-14T13:30:00Z' else stamp, source)
               for event, stamp, source in cal.rows]
    path.write_text(render_events(changed, cal.covers_from, cal.covers_through))
    with pytest.raises(ValueError, match='No matching models'):
        load_models(tmp_path, 'calendar')
    assert 'past events/coverage differ' in caplog.text


def calendar_service(path=EVENTS):
    from inference.service import Model, Service
    from research.labels import load_spec
    from types import SimpleNamespace
    import numpy as np
    cal = Calendar.load(path)
    meta = dict(target='BTC_spot', horizon_s=900, version='calendar', stride_s=10,
                label_spec=dict(load_spec(), calendar=str(path)),
                features=sorted(calendar_features(pl.Series('tick_ns', [ns('2024-01-01T00:00:00Z')]), cal).columns),
                engine_version=['test'], replay_modes=['as_live'],
                data_window={'end_ns': ns('2024-01-01T00:00:00Z')})
    meta['calendar'] = cal.metadata(meta['data_window']['end_ns'])
    produced, vectors = [], []
    model = Model(meta, SimpleNamespace(predict=lambda x, **kw: vectors.append(x) or np.array([.1])),
                  SimpleNamespace(predict=lambda x, **kw: np.array([[.1, .2, .7]])))
    service = Service([model], SimpleNamespace(produce=lambda *args, **kw: produced.append(kw)))
    return service, produced, vectors


def complete_tick(service, stamp):
    tick = ns(stamp)
    service.tick = tick
    service.rows = {('binance', 'spot', 'BTCUSDT'): dict(
        exchange='binance', market='spot', symbol='BTCUSDT', asset='BTC', tick_ns=tick,
        available_ns=tick, engine_version='test', replay_mode='as_live', book_valid=True,
        warmup=False, mid_e9=100_000_000_000)}
    service.complete()


def test_live_calendar_coverage_and_vector_parity(caplog):
    svc, produced, vectors = calendar_service()
    complete_tick(svc, '2025-10-24T12:30:00Z')
    assert len(produced) == 1
    import numpy as np
    expected = features('2025-10-24T12:30:00Z').select(svc.models[0].metadata['features']).to_numpy()
    np.testing.assert_array_equal(vectors[0], expected)
    complete_tick(svc, '2026-11-01T00:00:00Z')
    complete_tick(svc, '2026-11-01T00:00:10Z')
    assert svc.skipped['calendar_coverage'] == 2 and len(produced) == 1
    assert caplog.text.count('outside calendar coverage') == 1


def test_live_file_change_rechecked_and_past_change_refused(tmp_path, caplog):
    from scripts.update_events import render_events
    path = tmp_path / 'events.csv'
    path.write_bytes(EVENTS.read_bytes())
    svc, produced, _ = calendar_service(path)
    complete_tick(svc, '2025-10-24T12:30:00Z')
    cal = Calendar.load(path)
    path.write_text(render_events([r for r in cal.rows if r[1] != '2023-02-14T13:30:00Z'],
                                  cal.covers_from, cal.covers_through))
    complete_tick(svc, '2025-10-24T12:30:10Z')
    assert len(produced) == 1 and svc.skipped['inference_error'] == 1
    assert 'past events/coverage differ' in caplog.text


def test_missing_provenance_and_coverage_shrink_refused(tmp_path):
    from research.calendar import verify_calendar
    from scripts.update_events import render_events
    cal = Calendar.load(EVENTS)
    end = ns('2024-01-01T00:00:00Z')
    meta = {'calendar': cal.metadata(end), 'data_window': {'end_ns': end}}
    with pytest.raises(ValueError, match='provenance missing'):
        verify_calendar(cal, {})
    path = tmp_path / 'events.csv'
    path.write_text(render_events(cal.rows, cal.covers_from, '2026-09-30'))
    with pytest.raises(ValueError, match='coverage differ'):
        verify_calendar(Calendar.load(path), meta)


def test_past_changes_require_explicit_acceptance_and_diff(tmp_path, capsys):
    from scripts.update_events import write_update
    path = tmp_path / 'events.csv'
    path.write_bytes(EVENTS.read_bytes())
    original = path.read_bytes()
    cal = Calendar.load(path)
    rows = [(e, '2023-02-14T13:31:00Z' if t == '2023-02-14T13:30:00Z' else t, s)
            for e, t, s in cal.rows]
    with pytest.raises(ValueError, match='--accept-changes'):
        write_update(path, rows, cal.covers_from, cal.covers_through)
    assert path.read_bytes() == original
    assert 'changed 1 rows' in capsys.readouterr().out
    write_update(path, rows, cal.covers_from, cal.covers_through, accept_changes=True)
    assert Calendar.load(path).rows != cal.rows
    first = path.read_bytes()
    write_update(path, list(reversed(rows)), cal.covers_from, cal.covers_through)
    assert path.read_bytes() == first  # stable sort and bytes


def test_removed_past_row_and_dry_run(tmp_path):
    from scripts.update_events import write_update
    path = tmp_path / 'events.csv'
    path.write_bytes(EVENTS.read_bytes())
    cal = Calendar.load(path)
    with pytest.raises(ValueError, match='--accept-changes'):
        write_update(path, cal.rows[1:], cal.covers_from, cal.covers_through)
    original = path.read_bytes()
    write_update(path, cal.rows[1:], cal.covers_from, cal.covers_through,
                 accept_changes=True, dry_run=True)
    assert path.read_bytes() == original


def test_refresh_preserves_history_and_replaces_future():
    from datetime import date
    from scripts.update_events import refreshed_rows
    cal = Calendar.load(EVENTS)
    actual = {e: [r for r in cal.rows if r[0] == e and '2026' in r[1]] for e in ('cpi', 'nfp')}
    fed = [r for r in cal.rows if r[0] == 'fomc']
    upcoming = parse_ics((FIXTURES / 'bls.ics').read_text())
    rows, through = refreshed_rows(cal, actual, upcoming, fed, date(2026, 10, 19))
    assert through == '2026-11-06'
    assert ('cpi', '2023-02-14T13:30:00Z') in {r[:2] for r in rows}
    assert ('nfp', '2026-11-06T13:30:00Z') in {r[:2] for r in rows}
    with pytest.raises(ValueError, match='No cpi archive'):
        refreshed_rows(cal, dict(actual, cpi=[]), upcoming, fed, date(2026, 10, 19))
    with pytest.raises(ValueError, match='No upcoming cpi'):
        refreshed_rows(cal, actual, [], fed, date(2026, 10, 19))


def test_updater_blocked_source_aborts_without_writing(tmp_path, monkeypatch):
    import httpx
    from scripts import update_events as updater
    path = tmp_path / 'events.csv'
    path.write_bytes(EVENTS.read_bytes())
    original = path.read_bytes()
    calls = []

    class Client:
        def __init__(self, **options):
            assert 'User-Agent' in options['headers']
            assert '@' not in options['headers']['User-Agent']

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url):
            calls.append(url)
            return httpx.Response(403, request=httpx.Request('GET', url))

    monkeypatch.setattr(updater.httpx, 'Client', Client)
    with pytest.raises(httpx.HTTPStatusError):
        updater.main(['--output', str(path)])
    assert calls == [updater.ARCHIVES['cpi']]
    assert path.read_bytes() == original


@pytest.mark.parametrize('corrected', ['2026-01-13T13:31:00Z', '2026-01-14T13:30:00Z'])
def test_archive_correction_replaces_old_stamp_before_guard(tmp_path, corrected):
    from datetime import date
    from scripts.update_events import refreshed_rows, write_update
    cal = Calendar.load(EVENTS)
    # A time correction on a span boundary must remove the original timestamp.
    archives = {e: [r for r in cal.rows if r[0] == e and r[1].startswith('2026')]
                for e in ('cpi', 'nfp')}
    event, old_stamp, source = archives['cpi'][0]
    archives['cpi'][0] = (event, corrected, source)
    upcoming = parse_ics((FIXTURES / 'bls.ics').read_text())
    rows, through = refreshed_rows(cal, archives, upcoming,
                                  [r for r in cal.rows if r[0] == 'fomc'], date(2026, 10, 19))
    assert (event, old_stamp, source) not in rows
    path = tmp_path / 'events.csv'
    path.write_bytes(EVENTS.read_bytes())
    with pytest.raises(ValueError, match='--accept-changes'):
        write_update(path, rows, cal.covers_from, through)


def test_shutdown_nfp_and_no_notation_votes():
    cal = Calendar.load(EVENTS)
    releases = {r[1] for r in cal.rows if r[0] == 'nfp'}
    assert '2025-11-20T13:30:00Z' in releases
    assert '2025-12-16T13:30:00Z' in releases
    assert '2025-11-07T13:30:00Z' not in releases
    assert not any(r[0] == 'fomc' and r[1].startswith('2025-08-22') for r in cal.rows)
    assert len(cal.rows) == 130


def test_updater_direct_script_entrypoint():
    import os
    import subprocess
    import sys
    result = subprocess.run([sys.executable, str(EVENTS.parents[1] / 'scripts/update_events.py'), '--help'],
                            env={**os.environ, 'PYTHON_DOTENV_DISABLED': '1'}, capture_output=True, text=True)
    assert result.returncode == 0
    assert '--accept-changes' in result.stdout


def test_macro_coverage_requires_complete_seven_day_windows():
    from research.calendar import DAY
    cal = Calendar.load(EVENTS)
    ticks = [cal.start_ns + 7 * DAY - 1, cal.start_ns + 7 * DAY,
             cal.stop_ns - 7 * DAY, cal.stop_ns - 7 * DAY + 1,
             ns('2026-10-31T00:00:00Z')]
    frame = calendar_features(pl.Series('tick_ns', ticks), cal)
    expected = [False, True, True, False, False]
    assert [cal.contains(t) for t in ticks] == expected
    macro = [c for c in frame.columns if any(c.startswith(f'cal_{e}_') for e in ('fomc', 'cpi', 'nfp'))]
    for column in macro:
        assert frame[column].is_not_null().to_list() == expected
    for column in set(frame.columns) - set(macro):
        assert frame[column].null_count() == 0


def test_live_calendar_seven_day_boundary(caplog, monkeypatch):
    monkeypatch.setattr('inference.service.time.time_ns', lambda: ns('2028-01-01T00:00:00Z'))
    svc, produced, _ = calendar_service()
    complete_tick(svc, '2026-10-25T00:00:00Z')
    assert len(produced) == 1
    complete_tick(svc, '2026-10-25T00:00:10Z')
    assert len(produced) == 1
    assert svc.skipped['calendar_coverage'] == 1
    assert '7-day' in caplog.text
    assert 'run scripts/update_events.py' in caplog.text


@pytest.mark.parametrize('days,accepted', [(3, False), (7, False), (8, True)])
def test_verify_calendar_protects_training_lookahead(tmp_path, days, accepted):
    from datetime import timedelta
    from research.calendar import verify_calendar
    from scripts.update_events import render_events
    path = tmp_path / 'events.csv'
    end = ns('2024-01-10T00:00:00Z')
    release = datetime.fromisoformat('2024-01-10T00:00:00+00:00') + timedelta(days=days)
    original = ('cpi', release.strftime('%Y-%m-%dT%H:%M:%SZ'), 'fixture')
    path.write_text(render_events([original], '2024-01-01', '2024-02-01'))
    cal = Calendar.load(path)
    meta = {'calendar': cal.metadata(end), 'data_window': {'end_ns': end}}
    assert meta['calendar']['past_rows'] == ([] if accepted else [list(original[:2])])
    changed = ('cpi', (release + timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ'), 'fixture')
    path.write_text(render_events([changed], cal.covers_from, cal.covers_through))
    if accepted:
        verify_calendar(Calendar.load(path), meta)
    else:
        with pytest.raises(ValueError, match='past events/coverage differ'):
            verify_calendar(Calendar.load(path), meta)
