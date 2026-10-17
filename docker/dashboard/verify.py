#!/usr/bin/env python3
"""Local integration check; only deletes features with engine_version=synthetic-test."""
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ['docker', 'compose', '--env-file', '/dev/null', '-f',
           str(ROOT / 'docker/dashboard/docker-compose.yml')]
PASSWORD = os.environ.get('GRAFANA_ADMIN_PASSWORD', 'local-dashboard-admin')
AUTH = base64.b64encode(('admin:' + PASSWORD).encode()).decode()


def sql(query, data=None):
    result = subprocess.run(COMPOSE + ['exec', '-T', 'clickhouse', 'clickhouse-client',
                                       '--query', query], input=data, text=True,
                            capture_output=True, check=True)
    return result.stdout.strip()


def api(path, body=None, authenticated=True):
    headers = {'Content-Type': 'application/json'}
    if authenticated:
        headers['Authorization'] = 'Basic ' + AUTH
    request = Request('http://127.0.0.1:3000' + path,
                      data=None if body is None else json.dumps(body).encode(),
                      headers=headers)
    try:
        with urlopen(request, timeout=45) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f'{path}: HTTP {exc.code}: {exc.read().decode()}') from exc


def query_grafana(target, start_ms, end_ms):
    result = api('/api/ds/query', {'from': str(start_ms), 'to': str(end_ms),
                                  'queries': [{**target, 'intervalMs': 5000,
                                               'maxDataPoints': 1000}]})
    for ref, value in result.get('results', {}).items():
        assert not value.get('error'), (ref, value)
        assert value.get('status', 200) < 400, (ref, value)
    assert result.get('results'), result
    return result


def main():
    # Compare every raw field and its nullability to the actual Rust definition.
    rust = (ROOT / 'engine/core/src/row.rs').read_text()
    mapping = {'String': 'LowCardinality(String)', 'i64': 'Int64', 'bool': 'Bool',
               'Option<f64>': 'Nullable(Float64)', 'Option<i64>': 'Nullable(Int64)'}
    expected = {name: mapping[typ] for name, typ in
                re.findall(r'pub (\w+): ([^,]+),', rust)}
    actual = json.loads(sql('DESCRIBE TABLE crypto.features FORMAT JSON'))['data']
    assert {c['name']: c['type'] for c in actual if c['name'] != 'ts'} == expected
    assert next(c for c in actual if c['name'] == 'ts')['default_type'] == 'MATERIALIZED'
    print(f'Schema matches all {len(expected)} Rust fields; ts is MATERIALIZED.', flush=True)
    print('Tables: ' + sql('SHOW TABLES FROM crypto'), flush=True)

    for attempt in range(36):
        try:
            assert api('/api/health')['database'] == 'ok'
            break
        except Exception:
            if attempt == 35:
                raise
            if attempt % 3 == 0:
                print('Waiting for Grafana and pinned plugin installation...', flush=True)
            time.sleep(5)
    grafana_health = api('/api/health')
    assert grafana_health['version'] == '13.2.3', grafana_health
    assert sql('SELECT version()') == '26.8.15.10'
    print('Grafana health: database=ok; version=13.2.3; ClickHouse version=26.8.15.10', flush=True)
    health = api('/api/datasources/uid/crypto-clickhouse/health')
    assert health['status'] == 'OK', health
    print('Datasource health: ' + json.dumps(health), flush=True)
    dashboard = api('/api/dashboards/uid/crypto-market')
    assert dashboard['meta']['provisioned'] is True
    assert dashboard['dashboard']['refresh'] == '5s'
    expected_panels = json.loads((ROOT / 'dashboards/market.json').read_text())['panels']
    for attempt in range(12):
        if dashboard['dashboard']['panels'] == expected_panels:
            break
        time.sleep(2)
        dashboard = api('/api/dashboards/uid/crypto-market')
    assert dashboard['dashboard']['panels'] == expected_panels, 'Provisioned queries have not reloaded'
    print(f'Dashboard provisioned: crypto-market; {len(dashboard["dashboard"]["panels"])} panels; refresh=5s', flush=True)
    plugins = api('/api/plugins')
    plugin = next(p for p in plugins if p['id'] == 'grafana-clickhouse-datasource')
    assert plugin['info']['version'] == '4.22.1', plugin
    print('ClickHouse plugin version: 4.22.1', flush=True)

    end_s = int(time.time()) - 2
    rows = []
    for i, offset in enumerate([1800, 900, 0]):
        rows.append(dict(exchange='binance', market='spot', symbol='BTCUSDT',
                         tick_ns=(end_s-offset)*10**9, available_ns=(end_s-offset)*10**9+250000000,
                         engine_version='synthetic-test', book_valid=True, warmup=False,
                         book_age_ms=10.0, mid=100.0+i, mid_e9=(100+i)*10**9, spread_bps=1.5,
                         bid_depth10=4.0, ask_depth10=3.0, imbalance10=1/7,
                         taker_buy_base=0.2, taker_sell_base=0.1, liq_base_1s=0.0))
    rows += [{**rows[-1], 'market': 'perp', 'funding_rate': 0.0001, 'open_interest': 50.0},
             {**rows[-1], 'exchange': 'cross', 'market': 'cross', 'symbol': 'BTC',
              'basis_bps_binance': 2.0, 'spot_dev_bps_okx': -1.0},
             {**rows[-1], 'symbol': 'ETHUSDT', 'mid': 50.0}]
    # Exact duplicates exercise the same identity as an at-least-once redelivery.
    payload = '\n'.join(json.dumps(row) for row in rows) + '\n'
    start_ms, end_ms = (end_s-3600)*1000, (end_s+5)*1000
    try:
        sql('INSERT INTO crypto.features FORMAT JSONEachRow', payload)
        sql('INSERT INTO crypto.features FORMAT JSONEachRow', payload)
        count = int(sql("SELECT count() FROM crypto.features FINAL WHERE engine_version='synthetic-test'"))
        assert count == len(rows), count
        print(f'Duplicate check: {2*len(rows)} inserted; FINAL count={count}.', flush=True)
        fixture_counts = {}
        for asset in ['BTC', 'ETH']:
            for panel in dashboard['dashboard']['panels']:
                target = {**panel['targets'][0]}
                target['rawSql'] = target['rawSql'].replace('${asset:sqlstring}', "'"+asset+"'")
                result = query_grafana(target, start_ms, end_ms)
                frame_rows = sum(len(next(iter(frame.get('data', {}).get('values', [])), []))
                                 for value in result['results'].values()
                                 for frame in value.get('frames', []))
                fixture_counts[(asset, panel['title'])] = frame_rows
                print(f'Query OK: {asset}: {panel["title"]}: {frame_rows} rows', flush=True)
        assert fixture_counts[('BTC', 'Mid per exchange / market')] >= 3
        assert fixture_counts[('ETH', 'Mid per exchange / market')] >= 1
        assert fixture_counts[('BTC', 'Perp / spot basis (bps)')] >= 1

        # Query-only prediction fixture: never writes synthetic predictions.
        target = {**next(p for p in dashboard['dashboard']['panels'] if p['title'] == 'Live predicted vs realized return (%)')['targets'][0]}
        prediction_tick = rows[1]['tick_ns']
        replacement = ("FROM (SELECT 'BTC_spot' AS target, toUInt16(900) AS horizon_s, "
                       f"toInt64({prediction_tick}) AS tick_ns, "
                       "fromUnixTimestamp64Nano(tick_ns) AS ts, 'synthetic-model' AS model_version, "
                       "toFloat64(2.0) AS pred_return_pct, toInt8(1) AS direction) AS p")
        target['rawSql'] = target['rawSql'].replace('FROM crypto.predictions AS p FINAL', replacement)
        target['rawSql'] = target['rawSql'].replace('${asset:sqlstring}', "'BTC'")
        response = query_grafana(target, start_ms, end_ms)
        values = [v for r in response['results'].values() for frame in r.get('frames', [])
                  for field, column in zip(frame['schema']['fields'], frame['data']['values'])
                  if field['type'] == 'number' for v in column if v is not None]
        expected_return = 100*(102/101-1)
        assert any(abs(v-expected_return) < 1e-8 for v in values), values
        assert any(abs(v-2.0) < 1e-8 for v in values), values
        print(f'Prediction join query fixture: predicted=2%; realized={expected_return:.9f}%.', flush=True)
        target = {**next(p for p in dashboard['dashboard']['panels'] if p['title'] == 'Live directional hit rate (%)')['targets'][0]}
        target['rawSql'] = target['rawSql'].replace('FROM crypto.predictions AS p FINAL', replacement)
        target['rawSql'] = target['rawSql'].replace('${asset:sqlstring}', "'BTC'")
        response = query_grafana(target, start_ms, end_ms)
        values = [v for r in response['results'].values() for frame in r.get('frames', [])
                  for field, column in zip(frame['schema']['fields'], frame['data']['values'])
                  if field['type'] == 'number' for v in column if v is not None]
        assert any(abs(v-100.0) < 1e-8 for v in values), values
        print('Directional hit rate query fixture: 100%.', flush=True)
    finally:
        sql("ALTER TABLE crypto.features DELETE WHERE engine_version='synthetic-test' SETTINGS mutations_sync=2")
        remaining = int(sql("SELECT count() FROM crypto.features FINAL WHERE engine_version='synthetic-test'"))
        assert remaining == 0, remaining
        print('Synthetic feature cleanup: remaining=0; synchronous mutation completed.', flush=True)
    print(subprocess.run(COMPOSE + ['ps'], text=True, capture_output=True, check=True).stdout)
    ids = subprocess.run(COMPOSE + ['ps', '-q'], text=True, capture_output=True, check=True).stdout.split()
    print(subprocess.run(['docker', 'stats', '--no-stream', '--format',
                          '{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'] + ids,
                         text=True, capture_output=True, check=True).stdout)


if __name__ == '__main__':
    main()
