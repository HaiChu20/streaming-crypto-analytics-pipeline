"""Offline aggTrades fixtures: bounded parsing, integrity, resume and planning."""

from datetime import date
import hashlib
from io import BytesIO
import threading
import time
from zipfile import ZipFile, ZIP_DEFLATED

import httpx
import polars as pl
import pytest

from strategy2 import download as module
from strategy2.aggflow import COLUMNS, SCHEMA, aggregate_zip
from strategy2.download import ALL_COINS, COINS, DATASETS, EXTRA_COINS, main, plan_downloads
from strategy2.sources import load_aggflow


def zipped(rows, header=False):
    output = BytesIO()
    with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
        archive.writestr('aggTrades.csv', ((','.join(COLUMNS) + '\n') if header else '')
                         + '\n'.join(','.join(map(str, row)) for row in rows) + '\n')
    return output.getvalue()


def trade(price=1, qty=1, stamp=0, maker=False):
    return [1, price, qty, 1, 1, stamp, str(maker).lower()]


def item():
    return plan_downloads(date(2023, 1, 1), date(2023, 1, 31), ('BTC',), ('aggflow',))[0]


class Chunks(httpx.SyncByteStream):
    def __init__(self, content):
        self.content = content

    def __iter__(self):
        for offset in range(0, len(self.content), 97):
            yield self.content[offset:offset + 97]


def archive_client(content, checksum=None):
    expected = checksum or hashlib.sha256(content).hexdigest()

    def handler(request):
        if request.url.path.endswith('.CHECKSUM'):
            return httpx.Response(200, content=f'{expected} archive.zip\n'.encode())
        assert request.headers['user-agent'] == module.USER_AGENT
        return httpx.Response(200, stream=Chunks(content))

    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.mark.parametrize('header', [False, True])
def test_header_side_buckets_flooring_and_batch_boundaries(header):
    # Both sides at every edge, plus values just below each boundary.
    notionals = [0, 999, 1000, 9999, 10000, 99999, 100000, 999999, 1000000, 2000000]
    rows = [trade(value, stamp=60_001, maker=maker)
            for maker in (False, True) for value in notionals]
    rows += [trade(7, 2, 59_999), trade(3, 2, 120_000, True), trade(2, 4, 60_000)]
    frame = aggregate_zip(BytesIO(zipped(rows, header)), block_size=180)
    assert frame.schema == SCHEMA
    assert frame['minute_ms'].to_list() == [0, 60_000, 120_000]
    first, middle, last = frame.to_dicts()
    assert first['n'] == 1 and first['buy_qty'] == 2 and first['buy_notional'] == 14
    assert first['sell_qty'] == first['max_sell_notional'] == 0
    assert middle['n'] == 21
    for side in ('buy', 'sell'):
        assert [middle[f'{side}_b{i}'] for i in range(5)] == [
            999 + (8 if side == 'buy' else 0), 10999, 109999, 1099999, 3000000]
        assert middle[f'max_{side}_notional'] == 2000000
        assert middle[f'{side}_notional'] == sum(notionals) + (8 if side == 'buy' else 0)
    assert last['sell_qty'] == 2 and last['sell_notional'] == 6
    assert last['buy_qty'] == last['max_buy_notional'] == 0


def test_download_checksum_metadata_resume_and_loader(tmp_path):
    task = item()
    content = zipped([trade(100, 2, 1672531200001), trade(200, 3, 1672531260000, True)], True)
    with archive_client(content) as client:
        row = module.run_downloads([task], tmp_path, client=client)[0]
    assert row['status'] == 'downloaded' and row['bytes'] == len(content)
    path = tmp_path / task.path
    before = path.read_bytes(), path.stat().st_mtime_ns
    with httpx.Client(transport=httpx.MockTransport(lambda request: pytest.fail('resume did HTTP'))) as client:
        resumed = module.download(task, tmp_path, client)
    assert resumed['status'] == 'present' and resumed['bytes'] == len(content)
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    assert not list(tmp_path.rglob('*.zip')) and not list(tmp_path.rglob('*.part'))
    frame = load_aggflow(tmp_path, 'btcusdt', 1672531200000, 1672531260000)
    assert frame.height == 1
    assert frame['available_ms'].to_list() == [1672531260000]
    coverage = pl.read_csv(tmp_path / 'coverage.csv')
    assert coverage['bytes'].to_list() == [len(content)]


@pytest.mark.parametrize('checksum', ['0' * 64, 'bad'])
def test_checksum_failure_writes_nothing(tmp_path, checksum):
    with archive_client(zipped([trade()]), checksum) as client:
        result = module.download(item(), tmp_path, client)
    assert result['status'] == 'checksum_failed'
    assert not list(tmp_path.iterdir())


def test_missing_and_malformed_csv_are_isolated(tmp_path):
    tasks = plan_downloads(date(2023, 1, 1), date(2023, 1, 31), ('BTC', 'ETH'), ('aggflow',))
    malformed = zipped([trade('not-a-price')])
    expected = hashlib.sha256(malformed).hexdigest()

    def handler(request):
        if 'ETHUSDT' in request.url.path:
            return httpx.Response(404)
        return httpx.Response(200, content=f'{expected}\n'.encode()) if request.url.path.endswith(
            '.CHECKSUM') else httpx.Response(200, stream=Chunks(malformed))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        rows = module.run_downloads(tasks, tmp_path, client=client)
    assert [row['status'] for row in rows] == ['error', 'missing']
    assert not list(tmp_path.rglob('*.parquet'))
    assert (tmp_path / 'coverage.csv').exists()


def test_partial_final_month_and_new_coins():
    tasks = plan_downloads(date(2023, 1, 15), date(2023, 2, 2), ALL_COINS, ('aggflow',))
    assert len(tasks) == 15 * 3
    btc = [task for task in tasks if task.coin == 'BTC']
    assert [task.period for task in btc] == ['2023-01', '2023-02-01', '2023-02-02']
    assert btc[0].url == (module.BINANCE + 'futures/um/monthly/aggTrades/BTCUSDT/'
                          'BTCUSDT-aggTrades-2023-01.zip')
    assert btc[1].url.endswith('/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-02-01.zip')
    assert [task.path for task in btc] == [f'binance/aggflow/BTC/{task.period}.parquet' for task in btc]
    old_sets = plan_downloads(date(2023, 1, 1), date(2023, 1, 31), EXTRA_COINS, DATASETS)
    assert not any(task.dataset == 'dvol' for task in old_sets)
    for coin in EXTRA_COINS:
        assert {task.dataset for task in old_sets if task.coin == coin} == set(DATASETS) - {
            'dvol', *(['coinbase_klines'] if coin in ('BNB', 'TRX') else [])}


def test_coinbase_skips_bnb_trx_and_cli_all(tmp_path, capsys):
    tasks = plan_downloads(date(2023, 1, 1), date(2023, 1, 1), ALL_COINS, ('coinbase_klines',))
    assert len(tasks) == 13
    assert not any(task.coin in ('BNB', 'TRX') for task in tasks)
    assert main(['--root', str(tmp_path), '--from', '2023-01-01', '--to', '2023-01-31',
                 '--coins', 'all', '--datasets', 'aggflow', '--dry-run']) == 0
    assert '15 files planned' in capsys.readouterr().out
    assert main(['--root', str(tmp_path), '--from', '2023-01-01', '--to', '2023-01-01',
                 '--coins', 'BNB,TRX', '--datasets', 'coinbase_klines', '--dry-run']) == 0
    output = capsys.readouterr().out
    assert 'Coverage note' in output and 'BNB, TRX' in output and '0 files planned' in output
    assert not list(tmp_path.iterdir())


def test_cli_defaults_unchanged(tmp_path, monkeypatch):
    planned = []
    monkeypatch.setattr(module, 'run_downloads', lambda tasks, *a, **kw: planned.extend(tasks) or [])
    assert main(['--root', str(tmp_path), '--from', '2023-01-01', '--to', '2023-01-01']) == 0
    assert {task.coin for task in planned} == set(COINS)
    assert {task.dataset for task in planned} == set(DATASETS)


def test_loader_overlap_and_conflict(tmp_path):
    folder = tmp_path / 'binance/aggflow/BTC'
    folder.mkdir(parents=True)
    frame = aggregate_zip(BytesIO(zipped([trade(stamp=1672531200001)])))
    frame.write_parquet(folder / '2023-01.parquet')
    frame.write_parquet(folder / '2023-01-01.parquet')
    assert load_aggflow(tmp_path, 'BTC', 1672531200000, 1672531260000).height == 1
    frame.with_columns(pl.lit(2, dtype=pl.Int64).alias('n')).write_parquet(folder / '2023-01-01.parquet')
    with pytest.raises(ValueError, match='conflicting duplicate'):
        load_aggflow(tmp_path, 'BTC', 1672531200000, 1672531260000)
    assert load_aggflow(tmp_path, 'ETH', 0, 60_000).schema == {**SCHEMA, 'available_ms': pl.Int64}


@pytest.mark.parametrize('jobs,limit', [(1, 2), (4, 2), (4, 1)])
def test_aggflow_concurrency_limit(tmp_path, monkeypatch, jobs, limit):
    tasks = plan_downloads(date(2023, 1, 1), date(2023, 1, 31), ALL_COINS, ('aggflow',))
    lock = threading.Lock()
    active, peak = 0, 0

    def worker(task, root, client):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(.01)
        with lock:
            active -= 1
        return module._row(task, 'missing')

    monkeypatch.setattr(module, 'download', worker)
    with httpx.Client(transport=httpx.MockTransport(lambda request: pytest.fail('HTTP'))) as client:
        module.run_downloads(tasks, tmp_path, jobs, client, aggflow_jobs=limit)
    assert peak == min(jobs, limit)


def test_archive_ram_cap_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(module, 'AGGFLOW_ZIP_RAM_LIMIT', 10)
    with archive_client(zipped([trade()])) as client:
        with pytest.raises(ValueError, match='RAM limit'):
            module.download(item(), tmp_path, client)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('header', [False, True])
def test_empty_archive_has_typed_empty_output(header):
    output = BytesIO()
    with ZipFile(output, 'w') as archive:
        archive.writestr('empty.csv', ','.join(COLUMNS) + '\n' if header else '')
    frame = aggregate_zip(BytesIO(output.getvalue()))
    assert frame.height == 0 and frame.schema == SCHEMA


def test_stream_retry_restarts_hash_and_buffer(tmp_path, monkeypatch):
    content = zipped([trade()])
    attempts = []
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: None)

    class Interrupted(httpx.SyncByteStream):
        def __iter__(self):
            yield content[:30]
            raise httpx.ReadError('interrupted')

    def handler(request):
        if request.url.path.endswith('.CHECKSUM'):
            return httpx.Response(200, content=hashlib.sha256(content).hexdigest().encode())
        attempts.append(request)
        return httpx.Response(200, stream=Interrupted() if len(attempts) == 1 else Chunks(content))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = module.download(item(), tmp_path, client)
    assert result['status'] == 'downloaded' and result['bytes'] == len(content)
    assert len(attempts) == 2


def test_concurrent_writer_is_never_overwritten(tmp_path, monkeypatch):
    task = item()
    content = zipped([trade()])
    original_publish = module._atomic_write
    path = tmp_path / task.path
    other = aggregate_zip(BytesIO(zipped([trade(2)])))

    def racing_publish(destination, data):
        destination.parent.mkdir(parents=True, exist_ok=True)
        other.write_parquet(destination)
        return original_publish(destination, data)

    monkeypatch.setattr(module, '_atomic_write', racing_publish)
    with archive_client(content) as client:
        result = module.download(task, tmp_path, client)
    assert result['status'] == 'conflict'
    assert pl.read_parquet(path).equals(other)


def test_jobs_caps_mixed_dataset_concurrency(tmp_path, monkeypatch):
    tasks = plan_downloads(date(2023, 1, 1), date(2023, 1, 31), ALL_COINS,
                           ('aggflow', 'perp_klines'))
    lock = threading.Lock()
    active, peak = 0, 0

    def worker(task, root, client):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(.005)
        with lock:
            active -= 1
        return module._row(task, 'missing')

    monkeypatch.setattr(module, 'download', worker)
    with httpx.Client(transport=httpx.MockTransport(lambda request: pytest.fail('HTTP'))) as client:
        module.run_downloads(tasks, tmp_path, 3, client)
    assert peak == 3
