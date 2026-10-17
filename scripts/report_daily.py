"""Read-only UTC stability/cost report; outputs exclusively to root/reports/YYYYMMDD.{md,json}.

No dotenv, broker, or AWS access. Data payloads are parsed only at deterministic byte
positions; all record headers/markers are scanned. Open inputs stop at their initial
size and incomplete tails are ignored. Memory is capped independently of data volume.
"""

import argparse
import gzip
import math
import os
import re
import resource
import shutil
import sys
import time
import zlib
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import orjson
import yaml

REPO = Path(__file__).resolve().parents[1]
NS = 1_000_000_000
DAY = 86400 * NS
GIB = 1024**3
BLOCK = 1024**2
MAX_LINE = 16 * BLOCK
SAMPLE_BYTES = 64 * BLOCK  # per stream, 64 evenly spaced compression windows
FRESH_SAMPLES = 8192  # at most this many data records per stream
EVENT_LIMIT = 10000  # fail explicitly, never silently truncate incident output
PRICE_SOURCE = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-north-1/index.json"
PRICE_FETCHED = "2026-10-17"
# Recorder-owned v2 header: matching the entire triplet prevents native nested keys
# from being mistaken for recorder timestamps/offsets. Both orders occur in v2.
HEADER = re.compile(rb'"recv_time_ns"\s*:\s*(\d+)\s*,\s*"conn"\s*:\s*"([^"\\]*)"\s*,\s*"offset"\s*:\s*(\d+)')
MARKER = re.compile(rb'^\s*\{\s*"marker"\s*:')


def utc_ns(value):
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
        return int(float(value) * 1_000_000)  # native numeric event times are milliseconds
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    # Keep ns precision for ISO strings rather than rounding epoch floats.
    fraction = re.search(r'\.(\d+)', value)
    return int(dt.replace(microsecond=0).timestamp()) * NS + int((fraction[1] if fraction else '')[:9].ljust(9, '0'))


def event_time_ns(exchange, market, rec):
    """One freshness value per sampled frame, using its latest native event time.

    Batched/snapshot messages use the newest member; old snapshot trades are not
    interpreted as transport lag. Frames without a native clock are counted missing.
    """
    if 'raw' in rec:
        try:
            rec = orjson.loads(rec['raw'])
        except (orjson.JSONDecodeError, TypeError):
            return None
    if not isinstance(rec, dict):
        return None
    data = rec.get('data', rec)
    if exchange == 'binance':
        if not isinstance(data, dict):
            return None
        value = data.get('E', data.get('T'))
        # Liquidation timestamps may live in the nested order.
        if value is None and isinstance(data.get('o'), dict):
            value = data['o'].get('T')
        return utc_ns(value) if value is not None else None
    if exchange == 'okx':
        items = data if isinstance(data, list) else [data]
        values = [utc_ns(x['ts']) for x in items if isinstance(x, dict) and x.get('ts') is not None]
    elif market == 'spot':
        items = data if isinstance(data, list) else [data]
        values = [utc_ns(x['timestamp']) for x in items if isinstance(x, dict) and x.get('timestamp')]
    else:
        items = rec.get('trades', [rec])
        values = [utc_ns(x['time']) for x in items if isinstance(x, dict) and x.get('time') is not None]
    return max(values) if values else None


def quantiles(values):
    """Nearest-rank quantiles (negative freshness is retained to expose clock skew)."""
    if not values:
        return dict(n=0, p50=None, p99=None, max=None)
    ordered = sorted(values)
    return dict(n=len(ordered), p50=ordered[math.ceil(len(ordered)*.5)-1],
                p99=ordered[math.ceil(len(ordered)*.99)-1], max=ordered[-1])


def lines(path, size):
    """Yield complete lines in the captured file prefix with a strict line-size cap."""
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rb') as f:
        pos = 0
        while path.suffix == '.gz' or pos < size:
            limit = MAX_LINE + 1 if path.suffix == '.gz' else min(MAX_LINE + 1, size-pos)
            line = f.readline(limit)
            if not line:
                break
            if len(line) > MAX_LINE:
                raise ValueError(f'line exceeds {MAX_LINE} bytes: {path}')
            if not line.endswith(b'\n'):
                if path.name.endswith('.open'):
                    break
                raise ValueError(f'incomplete closed file: {path}')
            yield pos, line
            pos += len(line)


def last_record(path):
    """Bounded tail context; gzip needs a sequential read, ordinary files seek."""
    size = path.stat().st_size
    if path.suffix == '.gz':
        last = None
        for _, line in lines(path, size):
            if b'"recv_time_ns"' in line:
                last = line
        return orjson.loads(last) if last else None
    with path.open('rb') as f:
        f.seek(max(0, size - MAX_LINE - 1))
        block = f.read(MAX_LINE + 1)
    rows = block.split(b'\n')[:-1]
    if size > MAX_LINE + 1:
        rows = rows[1:]
    for row in reversed(rows):
        if HEADER.search(row) or MARKER.match(row):
            rec = orjson.loads(row)
            if 'offset' in rec and 'recv_time_ns' in rec:
                return rec
    return None


def union_seconds(intervals, start, end):
    total, right = 0, start
    for a, b in sorted(intervals):
        a, b = max(a, start), min(b, end)
        if b > max(a, right):
            total += b - max(a, right)
            right = b
    return total / NS


def chosen_files(directory, feed):
    """Collapse raw/gzip counterparts; never count archived copies twice."""
    found = {}
    for p in directory.glob(f'*_{feed}.jsonl*'):
        if not p.name.endswith(('.jsonl', '.jsonl.open', '.jsonl.gz')):
            continue
        day = p.name[:8]
        if len(day) != 8 or not day.isdigit():
            continue
        priority = 0 if p.name.endswith('.jsonl') else 1 if p.suffix == '.gz' else 2
        if day not in found or priority < found[day][0]:
            found[day] = (priority, p)
    return {d: p for d, (_, p) in found.items()}


def scan_stream(path, day, previous=None, following=None, *, sample_bytes=SAMPLE_BYTES):
    start = int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp()) * NS
    end = start + DAY
    exchange, market, symbol = path.parts[-4:-1]
    feed = path.name.split('_', 1)[1].split('.', 1)[0]
    size = path.stat().st_size
    is_open = path.name.endswith('.open')
    meta_path = path.with_name(path.name + '.meta.json')
    meta = orjson.loads(meta_path.read_bytes()) if not is_open and meta_path.exists() else None
    # A compressed input's physical size is not its uncompressed sampling extent.
    extent = meta['bytes'] if meta else size if path.suffix != '.gz' else None
    if extent is None:
        # Without metadata use a bounded first-64MB gzip sample; label its bias.
        extent = sample_bytes
    stride = max(1, extent // FRESH_SAMPLES)
    window_count = min(64, max(1, math.ceil(extent / BLOCK)))
    window_size = min(BLOCK, max(1, sample_bytes // window_count))
    windows = [(i * extent // window_count, min((i+1)*extent//window_count, i * extent // window_count + window_size))
               for i in range(window_count)]
    wi, sampled_bytes, compressed_bytes = 0, 0, 0
    compressor = zlib.compressobj(6, zlib.DEFLATED, 31)
    fresh, fresh_missing, sampled_records, next_sample = [], 0, 0, 0
    minute_counts = [0] * 1440
    counts, drops = Counter(), Counter()
    active, covered, uncertain, outages, pending = {}, [], [], [], []
    prior = last_record(previous) if previous else None
    last_ns = prior.get('recv_time_ns') if prior else None
    prev_offset = prior.get('offset') if prior else None
    previous_file_offset = prev_offset
    if prior:
        event = prior.get('event') if prior.get('marker') == 'ws_event' else None
        if event not in ('connection_error', 'disconnected'):
            active[prior['conn']] = start
        elif event == 'connection_error':
            pending.append(dict(conn=prior['conn'], reason=prior.get('reason', 'unknown'),
                                start_ns=prior['recv_time_ns'], carry_in=True,
                                error=prior.get('error', prior.get('exc_type', ''))))
    offset_gaps, first_ns, first_offset = [], None, None
    byte_count, line_count, data_count, foreign = 0, 0, 0, 0
    boundary_gap = None
    for pos, line in lines(path, size):
        line_count += 1
        byte_count += len(line)
        # Compress deterministic evenly spaced byte windows as one gzip-6 sample.
        while wi < len(windows) and windows[wi][1] <= pos:
            wi += 1
        j = wi
        while j < len(windows) and windows[j][0] < pos + len(line):
            lo, hi = max(pos, windows[j][0]), min(pos + len(line), windows[j][1])
            if hi > lo:
                piece = line[lo-pos:hi-pos]
                sampled_bytes += len(piece)
                compressed_bytes += len(compressor.compress(piece))
            j += 1
        marker = bool(MARKER.match(line))
        if marker:
            rec = orjson.loads(line)
            if rec.get('marker') == 'rollover':
                continue
            ns, conn, offset = rec['recv_time_ns'], rec['conn'], rec['offset']
        else:
            match = HEADER.search(line)
            if not match:
                raise ValueError(f'not a recorder v2 header: {path}:{line_count}')
            ns, conn, offset = int(match[1]), match[2].decode(), int(match[3])
        if first_ns is None:
            first_ns, first_offset = ns, offset
            if previous_file_offset is not None and not (meta or {}).get('synthetic_offsets') and offset != previous_file_offset + 1:
                boundary_gap = [previous_file_offset + 1, offset]
        if not meta and prev_offset is not None and offset != prev_offset + 1:
            offset_gaps.append([prev_offset+1, offset])
            if len(offset_gaps) > EVENT_LIMIT:
                raise ValueError(f'too many offset gaps (> {EVENT_LIMIT}): {path}')
        prev_offset = offset
        foreign += not start <= ns < end
        if marker:
            event = rec['event']
            counts[event] += 1
            if event == 'connected':
                active.setdefault(conn, max(start, ns))
                for outage in pending:
                    outage['end_ns'] = ns
                    outages.append(outage)
                pending.clear()
            elif event in ('connection_error', 'disconnected'):
                if conn in active:
                    covered.append((active.pop(conn), ns))
                if event == 'connection_error':
                    reason = rec.get('reason', 'unknown')
                    drops[reason] += 1
                    pending.append(dict(conn=conn, reason=reason, start_ns=ns, carry_in=False,
                                        error=rec.get('error', rec.get('exc_type', ''))))
                    if reason == 'collector_crash':
                        before = last_ns if last_ns is not None else rec.get('prev_last_alive_ns', ns)
                        uncertain.append((before, ns))
                        # The crashed session's other epochs cannot still be active.
                        for since in active.values():
                            covered.append((since, before))
                        active.clear()
            if len(outages) + len(pending) + len(covered) + len(uncertain) + len(active) > EVENT_LIMIT:
                raise ValueError(f'too many control events (> {EVENT_LIMIT}): {path}')
        else:
            data_count += 1
            if start <= ns < end:
                minute_counts[(ns-start)//(60*NS)] += 1
            if pos >= next_sample and sampled_records < FRESH_SAMPLES:
                sampled_records += 1
                next_sample = (pos // stride + 1) * stride
                native = event_time_ns(exchange, market, orjson.loads(line))
                if native is not None:
                    fresh.append((ns-native)/1_000_000)
                else:
                    fresh_missing += 1
            # Data at a day boundary can carry an epoch opened on an earlier day.
            if first_ns == ns and not prior:
                active.setdefault(conn, max(start, ns))
        last_ns = ns
    compressed_bytes += len(compressor.flush())
    cutoff = min(end, last_ns if last_ns is not None else start) if is_open else end
    for since in active.values():
        covered.append((since, cutoff))
    # Only seek beyond the day when a disconnect is still awaiting its next start.
    if pending and following:
        for _, line in lines(following, following.stat().st_size):
            if MARKER.match(line):
                rec = orjson.loads(line)
                if rec.get('event') == 'connected':
                    for outage in pending:
                        outage['end_ns'] = rec['recv_time_ns']
                        outages.append(outage)
                    pending.clear()
                    break
    for outage in pending:
        outage['end_ns'] = None
        outages.append(outage)
    for outage in outages:
        stop = outage['end_ns'] if outage['end_ns'] is not None else cutoff
        outage['downtime_s'] = max(0, stop-outage['start_ns'])/NS
        outage['day_downtime_s'] = max(0, min(end, stop)-max(start, outage['start_ns']))/NS
        outage['censored'] = outage['end_ns'] is None
    if meta:
        if meta['bytes'] != byte_count or meta['lines'] != line_count:
            raise ValueError(f'closed metadata count mismatch: {path}')
        offset_gaps = list(meta.get('offset_gaps', []))
        if boundary_gap:
            offset_gaps.insert(0, boundary_gap)
    gz = path if path.suffix == '.gz' else path.with_name(path.name.removesuffix('.open') + '.gz')
    gz_actual = gz.exists()
    gzip_bytes = gz.stat().st_size if gz_actual else round(byte_count*compressed_bytes/sampled_bytes) if sampled_bytes else 0
    # Subtract uncertain overlap from active coverage.
    overlap = [(max(a,c), min(b,d)) for a,b in covered for c,d in uncertain if min(b,d)>max(a,c)]
    coverage = union_seconds(covered, start, end) - union_seconds(overlap, start, end)
    result = dict(stream=f'{exchange}.{market}.{feed}.{symbol}', exchange=exchange, market=market,
                  symbol=symbol, feed=feed, path=str(path), open=is_open, captured_size=size,
                  lines=meta['lines'] if meta else line_count, bytes=meta['bytes'] if meta else byte_count,
                  counts_source='closed metadata (verified)' if meta else 'scan', data_lines=data_count,
                  partial_tail_bytes=size-byte_count if path.suffix != '.gz' else 0,
                  gz_bytes=gzip_bytes, gz_status='actual' if gz_actual else 'estimated',
                  gzip_sample_bytes=sampled_bytes, gzip_sample_ratio=compressed_bytes/sampled_bytes if sampled_bytes else None,
                  sample_method='evenly spaced byte windows' if path.suffix != '.gz' or meta else 'first 64 MiB (gzip without metadata)',
                  connections=counts['connected'], disconnects=counts['connection_error'], drops=dict(drops),
                  coverage_pct=coverage/864, active_seconds=coverage,
                  uncertain_intervals=[dict(start_ns=a,end_ns=b) for a,b in uncertain],
                  uncertain_seconds=union_seconds(uncertain,start,end), outages=outages,
                  offset_gaps=offset_gaps, foreign_day_records=foreign,
                  first_recv_ns=first_ns, last_recv_ns=last_ns if first_ns is not None else None,
                  first_offset=first_offset, last_offset=prev_offset,
                  rate_per_minute=quantiles(minute_counts), freshness_ms=quantiles(fresh),
                  freshness_sampled_records=sampled_records, freshness_without_clock=fresh_missing)
    return result, fresh


def costs(raw_bytes, gz_bytes, files, free, cfg, env):
    gb_price = float(env.get('S3_PRICE_GB_MONTH', '.023'))
    put_price = float(env.get('S3_PRICE_PUT_PER_1000', '.005'))
    retention = env.get('ARCHIVE_DELETE_LOCAL_AFTER_H', '').strip()
    local_days = float(retention)/24 if retention else None
    if any(not math.isfinite(x) or x < 0 for x in (gb_price, put_price, local_days or 0)):
        raise ValueError('prices/retention must be finite nonnegative numbers')
    # Hourly provisional manifest budget; complete days may require only one write.
    manifest_writes = 24
    puts = files + manifest_writes
    growth = raw_bytes + gz_bytes
    kafka = cfg['kafka']
    partitions = len(cfg['partitions']) * sum(len(c['feeds']) for ex in cfg['exchanges'].values()
                  for market in ex.values() for c in market['connections'].values())
    kafka_reserve = min(raw_bytes*kafka['retention_hours']/24,
                        partitions*kafka['retention_bytes_per_partition'])
    return dict(raw_gb_day=raw_bytes/1e9, gz_gb_day=gz_bytes/1e9,
                daily_raw_bytes=raw_bytes, daily_gz_bytes=gz_bytes,
                prices=dict(gb_month=gb_price, put_per_1000=put_price, region=env.get('AWS_REGION','eu-north-1'),
                            gb_source='environment' if 'S3_PRICE_GB_MONTH' in env else 'AWS default',
                            put_source='environment' if 'S3_PRICE_PUT_PER_1000' in env else 'AWS default',
                            default_source=PRICE_SOURCE, default_fetched=PRICE_FETCHED),
                s3_billing_gb_unit_bytes=GIB, files_per_day=files, manifest_writes_per_day=manifest_writes,
                put_requests_per_day=puts, put_month_usd=puts*30*put_price/1000,
                storage_30day_month_usd=gz_bytes/GIB*30*gb_price,
                total_30day_month_usd=gz_bytes/GIB*30*gb_price+puts*30*put_price/1000,
                local_retention_days=local_days,
                storage_nday_month_usd=gz_bytes/GIB*local_days*gb_price if local_days is not None else None,
                s3_retention='unlimited; local deletion does not expire S3 objects',
                free_bytes=free, local_raw_gz_growth_bytes_day=growth,
                disk_runway_days=free/growth if growth else None,
                kafka_retention_hours=kafka['retention_hours'], kafka_reserve_bytes=kafka_reserve,
                disk_runway_after_kafka_reserve_days=max(0,free-kafka_reserve)/growth if growth else None)


def exchange_summary(streams, samples, labels, start, end):
    output = {}
    for exchange in sorted({s['exchange'] for s in streams}):
        selected = [s for s in streams if s['exchange'] == exchange]
        incidents = {}
        for s in selected:
            for event in s['outages']:
                # Per-stream emission timestamps differ; old conn identifies the epoch,
                # reason + a 1s tolerance distinguishes repeated drops on an epoch.
                base = (event['conn'], event['reason'])
                bucket = incidents.setdefault(base, [])
                item = next((x for x in reversed(bucket) if abs(x['start_ns']-event['start_ns']) <= NS), None)
                if item is None:
                    item = dict(event, streams=[], connection=labels.get(s['stream'], s['stream']))
                    bucket.append(item)
                item['streams'].append(s['stream'])
                item['start_ns'] = min(item['start_ns'], event['start_ns'])
                if event['end_ns'] is not None:
                    item['end_ns'] = max(item['end_ns'] or event['end_ns'], event['end_ns'])
                item['downtime_s'] = max(item['downtime_s'], event['downtime_s'])
                item['day_downtime_s'] = max(item['day_downtime_s'],event['day_downtime_s'])
        unique = [x for bucket in incidents.values() for x in bucket]
        disconnects = [x for x in unique if x['reason']=='disconnect' and not x['carry_in']]
        reasons = Counter(x['reason'] for x in unique if not x['carry_in'])
        top_errors = Counter(x['error'] or '(no error detail)' for x in disconnects)
        by_connection = {}
        for label in sorted({x['connection'] for x in unique}):
            events = [x for x in disconnects if x['connection']==label]
            by_connection[label] = dict(incidents=len(events),
                                       duration_s=quantiles([x['downtime_s'] for x in events]))
        output[exchange] = dict(disconnect_markers=sum(s['drops'].get('disconnect',0) for s in selected),
            connection_errors=sum(s['disconnects'] for s in selected),
            disconnect_incidents=len(disconnects), distinct_incidents=sum(not x['carry_in'] for x in unique),
            carry_in_incidents=sum(x['carry_in'] for x in unique), top_reasons=dict(reasons.most_common()),
            top_disconnect_errors=dict(top_errors.most_common()),
            total_downtime_s=sum(x['day_downtime_s'] for x in unique),
            wall_clock_outage_s=union_seconds([(x['start_ns'],x['end_ns'] or end) for x in unique],start,end),
            longest_outage_s=max((x['downtime_s'] for x in unique), default=0),
            disconnect_duration_s=quantiles([x['downtime_s'] for x in disconnects]),
            by_connection=by_connection, incidents=unique, freshness_ms=quantiles(samples[exchange]),
            raw_bytes=sum(s['bytes'] for s in selected), gz_bytes=sum(s['gz_bytes'] for s in selected))
    return output


def build_report(day, root, allow_open=False, *, env=None):
    env = os.environ if env is None else env
    cfg = yaml.safe_load((REPO/'config/markets.yaml').read_text())
    token = day.strftime('%Y%m%d')
    paths, missing, labels, missing_gaps = [], [], {}, []
    for exchange, markets in cfg['exchanges'].items():
        for market, spec in markets.items():
            for conn_name, conn in spec['connections'].items():
                for feed in conn['feeds']:
                    for symbol in spec['symbols'].values():
                        symbol = symbol.replace('/', '-')
                        key = f'{exchange}.{market}.{feed}.{symbol}'
                        labels[key] = f'{exchange}.{market}.{conn_name}'
                        found = chosen_files(root/'raw'/exchange/market/symbol, feed)
                        if token in found:
                            older, newer = sorted(d for d in found if d < token), sorted(d for d in found if d > token)
                            paths.append((found[token], found[older[-1]] if older else None, found[newer[0]] if newer else None))
                        else:
                            missing.append(key)
                            older = sorted(d for d in found if d < token)
                            newer = sorted(d for d in found if d > token)
                            if older:
                                prior = last_record(found[older[-1]])
                                if prior:
                                    next_start = None
                                    if newer:
                                        for _, line in lines(found[newer[0]], found[newer[0]].stat().st_size):
                                            if MARKER.match(line):
                                                rec = orjson.loads(line)
                                                if rec.get('event') == 'connected':
                                                    next_start = rec['recv_time_ns']
                                                    break
                                    missing_gaps.append(dict(stream=key, previous_record_ns=prior['recv_time_ns'],
                                        next_connected_ns=next_start,
                                        status='disconnect' if prior.get('event') == 'connection_error' else 'uncertain'))
    if not paths:
        raise ValueError(f'no v2 streams for {day}')
    if not allow_open and any(p.name.endswith('.open') for p,_,_ in paths):
        raise ValueError('open files present; use --allow-open for a provisional report')
    streams, samples = [], defaultdict(list)
    for path, previous, following in paths:
        result, fresh = scan_stream(path,day,previous,following)
        streams.append(result)
        samples[result['exchange']].extend(fresh)
        print(f"{result['stream']}: {result['lines']:,} lines, {result['bytes']/1e9:.3f} GB",file=sys.stderr,flush=True)
    start = int(datetime.combine(day,datetime.min.time(),timezone.utc).timestamp())*NS
    end = start+DAY
    first = min(s['first_recv_ns'] for s in streams if s['first_recv_ns'] is not None)
    last = max(s['last_recv_ns'] for s in streams if s['last_recv_ns'] is not None)
    span = min(DAY, max(NS, last-first))/NS
    raw, gz = sum(s['bytes'] for s in streams), sum(s['gz_bytes'] for s in streams)
    factor = 86400/span
    cost = costs(raw*factor,gz*factor,len(streams),shutil.disk_usage(root).free,cfg,env)
    now_ns = time.time_ns()
    for s in streams:
        s['recorded_tail_age_s'] = max(0,(min(now_ns,end)-(s['last_recv_ns'] or start))/NS)
    kafka_drops = sum(s['drops'].get('kafka_unavailable',0) for s in streams)
    gaps = sum(len(s['offset_gaps']) for s in streams)
    return dict(date=day.isoformat(), provisional=bool(missing or any(s['open'] for s in streams)),
                missing_streams=missing, missing_day_gaps=missing_gaps, streams=streams, exchanges=exchange_summary(streams,samples,labels,start,end),
                observed_raw_bytes=raw, observed_gz_bytes=gz, observation_span_s=span, daily_projection_factor=factor,
                costs=cost, spool=dict(kafka_unavailable=kafka_drops, offset_gaps=gaps,
                recorder_lag='Kafka consumer lag unavailable offline; recorded_tail_age_s is an activity-dependent proxy',
                max_recorded_tail_age_s=max(s['recorded_tail_age_s'] for s in streams),
                recommendation='Disk spool needed: kafka_unavailable drops or offset gaps exist.' if kafka_drops or gaps
                else 'Disk spool not needed on observed evidence: no kafka_unavailable drops or offset gaps.'),
                generated_at=datetime.now(timezone.utc).isoformat())


def fmt(value, places=3):
    return 'n/a' if value is None else f'{value:.{places}f}'


def markdown(r):
    c, p = r['costs'], r['costs']['prices']
    out = [f"# Daily report: {r['date']} (UTC)", '',
        f"Status: {'provisional' if r['provisional'] else 'closed'}; {len(r['streams'])} streams. "
        f"Missing/not recorded: {', '.join(r['missing_streams']) or 'none'}.", '',
        '## Stability', '',
        '| Exchange | disconnect markers | incidents | downtime (connection-seconds) | longest (s) | freshness p50 / p99 / max (ms) |',
        '|---|---:|---:|---:|---:|---:|']
    for ex,s in r['exchanges'].items():
        q=s['freshness_ms']
        out.append(f"| {ex} | {s['disconnect_markers']} | {s['disconnect_incidents']} | {s['total_downtime_s']:.3f} | {s['longest_outage_s']:.3f} | {fmt(q['p50'])} / {fmt(q['p99'])} / {fmt(q['max'])} |")
    out.append('')
    for gap in r['missing_day_gaps']:
        out.append(f"Missing full-day stream gap: {gap['stream']} ({gap['status']}), previous record {gap['previous_record_ns']}, next connected {gap['next_connected_ns']}.")
    out += ['', 'Each connection incident emits a marker into every stream it carries. Incident deduplication uses old connection ID, reason and starts within 1 second; reconnects use the next connected on each stream. Downtime sums distinct connection incidents; simultaneous outages can overlap. Carry-in incidents are included in downtime, separately counted in JSON.', '']
    for ex,s in r['exchanges'].items():
        out.append(f"- {ex}: reasons (distinct incidents) {s['top_reasons']}; disconnect error details {s['top_disconnect_errors']}; carry-in {s['carry_in_incidents']}; simultaneous-outage union {s['wall_clock_outage_s']:.3f}s.")
        for label,v in s['by_connection'].items():
            out.append(f"  - {label}: {v['incidents']} disconnect incidents; duration p50/p99 {fmt(v['duration_s']['p50'])}/{fmt(v['duration_s']['p99'])}s.")
    out += ['', '## Per stream', '',
        '| Stream | lines | raw bytes | gzip bytes (status) | connected / errors | drops | coverage % | uncertain s | msg/min p50 / p99 | freshness ms p50 / p99 / max |',
        '|---|---:|---:|---:|---:|---|---:|---:|---:|---:|']
    for s in r['streams']:
        q=s['freshness_ms']; rate=s['rate_per_minute']
        out.append(f"| {s['stream']} | {s['lines']} | {s['bytes']} | {s['gz_bytes']} ({s['gz_status']}) | {s['connections']} / {s['disconnects']} | {s['drops']} | {s['coverage_pct']:.4f} | {s['uncertain_seconds']:.3f} | {rate['p50']} / {rate['p99']} | {fmt(q['p50'])} / {fmt(q['p99'])} / {fmt(q['max'])} |")
    out += ['', 'Coverage is the union of active connection epochs divided by 86,400s, minus collector-crash uncertainty. Open streams stop at their captured last record; unobserved time is not credited as coverage. Prior-file tails supply carry-in state and offset continuity; pending drops seek the next available day’s first connected. Unresolved disconnects are censored at the captured last record (duration lower bounds). Per-minute rates include all 1,440 UTC minutes (zero minutes included).', '',
        'Freshness is recv_time_ns minus the latest native event clock in each sampled frame (Binance E then T; OKX ts; Kraken spot timestamp; futures time). Up to 8,192 deterministic byte-spaced frames per stream; values without clocks are excluded, negative values retained. p99/max describe this sample, not the entire day. Binance spot bookTicker has no event clock. Gzip-6 estimates use up to 64 MiB of evenly spaced byte windows per stream; actual local .gz sizes take precedence. All data lines are header-scanned; closed metadata supplies verified counts and stored offset gaps. Full event and uncertainty intervals are in JSON.', '',
        '## Disk and S3', '',
        f"Observed raw/gzip: {r['observed_raw_bytes']/1e9:.3f}/{r['observed_gz_bytes']/1e9:.3f} GB; gzip is estimated unless marked actual above. Observation span {r['observation_span_s']/3600:.3f}h; projected 24h raw/gzip {c['raw_gb_day']:.3f}/{c['gz_gb_day']:.3f} GB/day (factor {r['daily_projection_factor']:.4f}). Partial-day projections assume the same activity rate.", '',
        f"Free disk {c['free_bytes']/1e9:.3f} GB; raw+gzip growth {c['local_raw_gz_growth_bytes_day']/1e9:.3f} GB/day; runway {fmt(c['disk_runway_days'],2)} days. Kafka retention {c['kafka_retention_hours']}h, capped reserve {c['kafka_reserve_bytes']/1e9:.3f} GB; conservative runway after reserving that amount {fmt(c['disk_runway_after_kafka_reserve_days'],2)} days. Free space already reflects current Kafka disk use; this extra reserve is conservative and broker/Docker usage is not measured. Gzip is transient in the archiver; raw+gzip growth is a conservative no-deletion scenario.", '',
        f"Prices used: ${p['gb_month']}/billable GB-month ({p['gb_source']}); ${p['put_per_1000']}/1,000 PUT/COPY/POST/LIST ({p['put_source']}); region {p['region']}. Billable GB = 2^30 bytes; displayed GB = 10^9 bytes. Defaults: [AWS Price List API, eu-north-1]({p['default_source']}), fetched {p['default_fetched']}. The report makes no network requests.", '',
        f"At 30 days of stored data: S3 Standard monthly storage run-rate ${c['storage_30day_month_usd']:.4f}; PUT ${c['put_month_usd']:.4f}/month ({c['files_per_day']} files + {c['manifest_writes_per_day']} manifest writes/day); total ${c['total_30day_month_usd']:.4f}/month. This models one PUT per object and an hourly provisional-manifest budget; multipart/retries and other request classes are excluded. A 30-day retention policy would make this steady-state, but S3 currently keeps ALL days: actual run-rate continues to grow linearly with stored days.", '',
        f"ARCHIVE_DELETE_LOCAL_AFTER_H: {fmt(c['local_retention_days']*24,1) if c['local_retention_days'] is not None else 'unset (local copies retained)'}. " +
        (f"N={c['local_retention_days']:.3f} days would hold ${c['storage_nday_month_usd']:.4f}/month of S3 storage if an S3 expiry policy existed; this setting deletes only local copies." if c['local_retention_days'] is not None else 'No N-day local retention configured; S3 retention remains unlimited.'), '',
        '## Disk spool', '', r['spool']['recommendation'], '',
        f"kafka_unavailable markers={r['spool']['kafka_unavailable']}; offset gaps (including prior-day boundary)={r['spool']['offset_gaps']}. {r['spool']['recorder_lag']}. Maximum recorded tail age {r['spool']['max_recorded_tail_age_s']:.3f}s; sparse feeds can be old without recorder lag.", '',
        f"Runtime {r.get('runtime_seconds',0):.3f}s; peak RSS {r.get('peak_rss_bytes',0)/1024**2:.1f} MiB.", '']
    return '\n'.join(out)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date',required=True,type=date.fromisoformat)
    parser.add_argument('--root',type=Path,default=Path('data'))
    parser.add_argument('--allow-open',action='store_true')
    args=parser.parse_args()
    token=args.date.strftime('%Y%m%d')
    outputs=[args.root/'reports'/f'{token}{suffix}' for suffix in ('.md','.json')]
    if any(p.exists() for p in outputs):
        parser.error('report already exists; existing reports are never overwritten')
    start=time.perf_counter()
    try:
        report=build_report(args.date,args.root,args.allow_open)
        report['runtime_seconds']=time.perf_counter()-start
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        report['peak_rss_bytes']=rss if sys.platform=='darwin' else rss*1024
        outputs[0].parent.mkdir(parents=True,exist_ok=True)
        with outputs[0].open('x') as f:
            f.write(markdown(report))
        with outputs[1].open('xb') as f:
            f.write(orjson.dumps(report,option=orjson.OPT_INDENT_2))
    except (ValueError,OSError) as error:
        parser.error(str(error))
    print(f"{outputs[0]}\n{outputs[1]}\nRuntime {report['runtime_seconds']:.3f}s; peak RSS {report['peak_rss_bytes']/1024**2:.1f} MiB")


if __name__=='__main__':
    main()
