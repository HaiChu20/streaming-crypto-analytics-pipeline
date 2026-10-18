"""Offline v3 report tests; all inputs and outputs are synthetic tmp_path trees."""
import gzip
from datetime import date
from pathlib import Path

import orjson
import pytest

from scripts.report_daily import (
    DAY, NS, REPO, build_report, costs, event_time_ns, exchange_summary, markdown,
    scan_stream, utc_ns,
)
import yaml

D = date(2026, 10, 17)
START = utc_ns('2026-10-17T00:00:00Z')


def marker(event, sec, conn='a', reason=None, **extra):
    r = dict(marker='ws_event', event=event, conn=conn,
             recv_time_ns=START + int(sec*NS), offset=0)
    if reason:
        r['reason'] = reason
    return r | extra


def data(sec, conn='a', **extra):
    return dict(recv_time_ns=START+int(sec*NS), conn=conn, offset=0) | extra


def write(root, rows, *, token='20261017', exchange='binance', market='spot',
          feed='l1', symbol='BTCUSDT', suffix='', offset=0):
    from archiver import rawfmt
    p = rawfmt.path(root/'raw', exchange, market, symbol, token, feed)
    p = p.with_name(p.name + suffix)
    p.parent.mkdir(parents=True, exist_ok=True)
    raw = b''.join(orjson.dumps(dict(r, offset=i+offset))+b'\n' for i,r in enumerate(rows))
    if suffix == '.gz':
        with gzip.open(p,'wb') as f:
            f.write(raw)
    else:
        p.write_bytes(raw)
    return p


def test_midnight_carry_in_drop_and_next_day_reconnect(tmp_path):
    prev = write(tmp_path,[marker('connected',-100), marker('connection_error',-10,reason='disconnect')],token='20261016')
    current = write(tmp_path,[marker('connected',20,conn='b'), marker('connection_error',86390,conn='b',reason='disconnect')],offset=2)
    following = write(tmp_path,[marker('connected',86410,conn='c')],token='20261018',offset=4)
    r,_ = scan_stream(current,D,prev,following)
    assert r['active_seconds'] == 86370
    assert r['coverage_pct'] == pytest.approx(86370/864)
    assert [o['downtime_s'] for o in r['outages']] == [30,20]
    assert [o['day_downtime_s'] for o in r['outages']] == [20,10]
    assert r['outages'][0]['carry_in']
    assert r['offset_gaps'] == []


def test_active_epoch_carries_in_and_overlap_is_union(tmp_path):
    prev = write(tmp_path,[marker('connected',-100),data(-1)],token='20261016')
    p = write(tmp_path,[data(0),marker('connected',10,conn='b'),
                        marker('disconnected',20),data(86400-1,conn='b')],offset=2)
    r,_=scan_stream(p,D,prev)
    assert r['coverage_pct']==100
    assert r['rate_per_minute']['n']==1440
    assert r['rate_per_minute']['p50']==0


def test_shared_connection_deduplicates_incidents(tmp_path):
    streams=[]
    for i,feed in enumerate(('l1','l2','trades')):
        p=write(tmp_path,[marker('connected',0),
                         marker('connection_error',10+i*.0001,reason='disconnect'),
                         marker('connected',12+i*.0001,conn='b')],feed=feed)
        s,_=scan_stream(p,D); streams.append(s)
    r=exchange_summary(streams,{'binance':[]},{s['stream']:'binance.spot.spot' for s in streams},START,START+DAY)['binance']
    assert r['disconnect_markers']==3
    assert r['disconnect_incidents']==1
    assert r['total_downtime_s']==2
    assert len(r['incidents'][0]['streams'])==3


@pytest.mark.parametrize('exchange,market,payload',[
    ('binance','spot',{'data':{'E':1792195200000,'T':1792195199000}}),
    ('binance','perp',{'data':{'T':1792195200000}}),
    ('binance','perp',{'data':{'o':{'T':1792195200000}}}),
    ('okx','spot',{'data':[{'ts':'1792195199000'},{'ts':'1792195200000'}]}),
    ('kraken','spot',{'data':[{'timestamp':'2026-10-17T00:00:00Z'}]}),
    ('kraken','perp',{'time':1792195200000}),
    ('kraken','perp',{'trades':[{'time':1792195199000},{'time':1792195200000}]}),
    ('binance','spot',{'raw':'{"data":{"E":1792195200000}}'}),
])
def test_freshness_clocks(exchange,market,payload):
    assert event_time_ns(exchange,market,payload)==START


def test_iso_nanoseconds_and_missing_clock():
    assert utc_ns('2026-10-17T00:00:00.123456789Z')==START+123456789
    assert event_time_ns('binance','spot',{'data':{'u':1}}) is None
    assert event_time_ns('kraken','spot',{'raw':'[]'}) is None


def test_gzip_estimate_and_sample_freshness(tmp_path):
    p=write(tmp_path,[marker('connected',0),data(.125,data={'E':1792195200000})])
    r,sample=scan_stream(p,D)
    assert r['gz_status']=='estimated'
    assert r['gzip_sample_bytes']==p.stat().st_size
    assert r['gz_bytes']==len(gzip.compress(p.read_bytes(),compresslevel=6))
    assert sample==[125.0]
    assert r['freshness_ms']['p99']==125
    with gzip.open(p.with_name(p.name+'.gz'),'wb') as f:
        f.write(p.read_bytes())
    r,_=scan_stream(p,D)
    assert r['gz_status']=='actual'
    assert r['gz_bytes']==p.with_name(p.name+'.gz').stat().st_size


def test_open_snapshot_ignores_stray_meta_and_partial_tail(tmp_path):
    p=write(tmp_path,[marker('connected',0),data(60)],suffix='.open')
    p.with_name(p.name+'.meta.json').write_bytes(b'{"lines":999999}')
    with p.open('ab') as f:
        f.write(b'{"partial":')
    r,_=scan_stream(p,D)
    assert r['lines']==2 and r['counts_source']=='scan'
    assert r['partial_tail_bytes']==len(b'{"partial":')
    assert r['active_seconds']==60
    with pytest.raises(ValueError, match='open files'):
        build_report(D,tmp_path)
    report=build_report(D,tmp_path,True,env={})
    assert 'estimated' in markdown(report)
    assert report['provisional']
    assert report['spool']['offset_gaps']==0


def test_crash_uncertainty_and_offset_boundary_gap(tmp_path):
    prev=write(tmp_path,[data(-10)],token='20261016',offset=7)
    p=write(tmp_path,[marker('connection_error',20,reason='collector_crash'),
                      marker('connected',30,conn='b')],offset=10)
    r,_=scan_stream(p,D,prev)
    assert r['uncertain_seconds']==20
    assert r['active_seconds']==86400-30
    assert r['offset_gaps']==[[8,10]]
    report=build_report(D,tmp_path,env={})
    assert report['spool']['offset_gaps']==1
    assert 'needed:' in report['spool']['recommendation']


def test_closed_metadata_counts_and_archive_not_doubled(tmp_path):
    p=write(tmp_path,[marker('connected',0),data(100)],offset=12)
    meta=dict(lines=2,bytes=p.stat().st_size,offset_gaps=[[5,9]])
    p.with_name(p.name+'.meta.json').write_bytes(orjson.dumps(meta))
    with gzip.open(p.with_name(p.name+'.gz'),'wb') as f:
        f.write(p.read_bytes())
    r=build_report(D,tmp_path,env={})
    assert len(r['streams'])==1
    assert r['streams'][0]['counts_source']=='closed metadata (verified)'
    assert r['streams'][0]['offset_gaps']==[[5,9]]


def test_cost_math_and_local_retention_not_s3_retention():
    cfg=yaml.safe_load((REPO/'config/markets.yaml').read_text())
    r=costs(10*1024**3,1024**3,44,100*1024**3,cfg,
            {'S3_PRICE_GB_MONTH':'0.023','S3_PRICE_PUT_PER_1000':'0.005','ARCHIVE_DELETE_LOCAL_AFTER_H':'48'})
    assert r['storage_30day_month_usd']==pytest.approx(.69)
    assert r['put_month_usd']==pytest.approx(68*30*.005/1000)
    assert r['storage_nday_month_usd']==pytest.approx(.046)
    assert r['disk_runway_days']==pytest.approx(100/11)
    assert r['kafka_reserve_bytes']==30*1024**3
    assert r['disk_runway_after_kafka_reserve_days']==pytest.approx(70/11)
    assert r['s3_retention'].startswith('unlimited')


def test_censored_drop_and_kafka_spool(tmp_path):
    p=write(tmp_path,[marker('connected',0),marker('connection_error',10,reason='kafka_unavailable'),data(100)],suffix='.open')
    r=build_report(D,tmp_path,True,env={})
    assert r['spool']['kafka_unavailable']==1
    assert r['streams'][0]['outages'][0]['censored']
    assert r['streams'][0]['outages'][0]['downtime_s']==90


def test_full_missing_day_reported_as_multi_day_gap(tmp_path):
    write(tmp_path,[marker('connected',0),data(60)],feed='l1')
    write(tmp_path,[marker('connection_error',-10,reason='disconnect')],feed='l2',token='20261016')
    write(tmp_path,[marker('connected',86410,conn='b')],feed='l2',token='20261018')
    r=build_report(D,tmp_path,env={})
    gap=next(x for x in r['missing_day_gaps'] if x['stream']=='binance.spot.l2.BTCUSDT')
    assert gap['status']=='disconnect'
    assert gap['next_connected_ns']-gap['previous_record_ns']==DAY+20*NS


def test_distinct_incidents_on_same_connection_are_not_merged(tmp_path):
    p=write(tmp_path,[marker('connected',0),marker('connection_error',10,reason='sequence_gap'),
                     marker('connected',11),marker('connection_error',20,reason='sequence_gap'),marker('connected',22)])
    s,_=scan_stream(p,D)
    summary=exchange_summary([s],{'binance':[]},{},START,START+DAY)['binance']
    assert summary['distinct_incidents']==2
    assert summary['total_downtime_s']==3


def test_gzip_only_input(tmp_path):
    p=write(tmp_path,[marker('connected',0),data(60,data={'T':1792195200000})],suffix='.gz')
    r,_=scan_stream(p,D)
    assert r['lines']==2 and r['gz_status']=='actual'
    assert r['bytes']>r['gz_bytes']


def test_report_refuses_existing_output_before_scan(tmp_path,monkeypatch):
    import sys
    from scripts import report_daily
    reports=tmp_path/'reports'; reports.mkdir()
    target=reports/'20261017.md'; target.write_text('keep')
    monkeypatch.setattr(sys,'argv',['report_daily','--date','2026-10-17','--root',str(tmp_path)])
    with pytest.raises(SystemExit):
        report_daily.main()
    assert target.read_text()=='keep'


def test_small_compression_windows_do_not_overlap(tmp_path):
    p=write(tmp_path,[marker('connected',0),data(60,padding='x'*(1500*1024))])
    r,_=scan_stream(p,D)
    assert r['gzip_sample_bytes']==p.stat().st_size
    assert r['gz_bytes']==len(gzip.compress(p.read_bytes(),compresslevel=6))


@pytest.mark.parametrize('day', [date(2026,9,15), date(2026,10,8)])
def test_user_archive_report_optional_metadata(day):
    root = Path('tests/fixtures/archive')
    report = build_report(day, root, env={})
    assert report['streams']
    assert report['spool']['offset_gaps'] == 0
    assert all(s['foreign_day_records'] == 0 for s in report['streams'])
    assert all(s['connections'] >= 0 for s in report['streams'])
    assert any(s['freshness_ms']['n'] > 0 for s in report['streams'])


def test_report_binance_l2_partition_offset_union(tmp_path):
    write(tmp_path, [data(1), data(2)], feed='l2', offset=0)
    # Replace offset 1 with 2, leaving offset 1 in snapshot.
    depth = tmp_path/'raw/Binance/Spot/BTCUSDT/20261017_depth.jsonl'
    depth.write_bytes(depth.read_bytes().replace(b'"offset":1', b'"offset":2'))
    write(tmp_path, [data(1.5)], feed='snapshot', offset=1)
    report = build_report(D, tmp_path, env={})
    assert report['spool']['offset_gaps'] == 0
