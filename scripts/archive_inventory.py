"""Stat-only v3 archive inventory; never reads payloads or writes into the archive."""
import argparse
from collections import defaultdict
from datetime import date, timedelta
import json
import os
from pathlib import Path
import re
import sys

# Direct script invocation adds scripts/, rather than the repository, to sys.path.
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archiver.rawfmt import archive_identity
from research.labels import load_spec

FILENAME = re.compile(r'(\d{8})_([a-z0-9]+)\.jsonl(?:\.(gz|open))?$')


def channels(exchange, market):
    # Same channel acceptance as engine/replay/src/input.rs::channel_feed.
    return ({'deribit': {'dvol'} if market == 'index' else set(),
             'binance': {'depth', 'snapshot', 'bookticker', 'trades'} |
                        ({'aggtrades', 'markprice', 'forceorder'} if market == 'perp' else set()),
             'okx': {'book', 'bbo', 'trades', 'tradesall'},
             'kraken': {'book', 'trades'} | ({'level3'} if market == 'spot' else {'ticker'} if market == 'perp' else set())}
            .get(exchange, set()))


def asset_for(pair):
    pair = pair.upper().removeprefix('PF_').removeprefix('PI_')
    if pair.startswith(('BTC', 'XBT')):
        return 'BTC'
    if pair.startswith('ETH'):
        return 'ETH'
    return None


def scan_archive(archive_root, spec=None):
    archive_root = Path(archive_root)
    root = archive_root / 'raw' if (archive_root / 'raw').is_dir() else archive_root
    groups = {}
    with os.scandir(root) as exchanges:
        for exchange in exchanges:
            if not exchange.is_dir(follow_symlinks=False):
                continue
            # Canonical prefix rules come from the recorder/readers helper.
            canonical, _, _, _ = archive_identity(Path(exchange.name) / 'spot' / 'pair' / '20000101_book.jsonl')
            if canonical is None:
                continue
            with os.scandir(exchange.path) as markets:
                for market in markets:
                    if not market.is_dir(follow_symlinks=False):
                        continue
                    ex, mk, _, _ = archive_identity(Path(exchange.name) / market.name / 'pair' / '20000101_book.jsonl')
                    if mk is None:
                        continue
                    accepted = channels(ex, mk)
                    with os.scandir(market.path) as pairs:
                        for pair in pairs:
                            if not pair.is_dir(follow_symlinks=False):
                                continue
                            with os.scandir(pair.path) as files:
                                for entry in files:
                                    match = FILENAME.fullmatch(entry.name)
                                    if not match or match[2] not in accepted or not entry.is_file(follow_symlinks=False):
                                        continue
                                    try:
                                        day = date.fromisoformat(match[1])
                                    except ValueError:
                                        continue
                                    key = ex, mk, pair.name, match[2]
                                    group = groups.setdefault(key, {'days': set(), 'closed_days': set(), 'total_bytes': 0, 'open_files': [], 'files': 0})
                                    group['days'].add(day)
                                    group['total_bytes'] += entry.stat(follow_symlinks=False).st_size
                                    group['files'] += 1
                                    if match[3] == 'open':
                                        group['open_files'].append(str(Path(entry.path).relative_to(archive_root)))
                                    else:
                                        group['closed_days'].add(day)
    entries, source_days = [], defaultdict(set)
    for (exchange, market, pair, channel), values in sorted(groups.items()):
        days = sorted(values['days'])
        missing = [days[0] + timedelta(days=i) for i in range((days[-1] - days[0]).days + 1)
                   if days[0] + timedelta(days=i) not in values['days']]
        entries.append({'exchange': exchange, 'market': market, 'pair': pair, 'channel': channel,
                        'first_day': days[0].isoformat(), 'last_day': days[-1].isoformat(),
                        'number_of_days': len(days), 'missing_days': [d.isoformat() for d in missing],
                        'closed_days': [d.isoformat() for d in sorted(values['closed_days'])],
                        'total_bytes': values['total_bytes'], 'files': values['files'],
                        'open_files': sorted(values['open_files'])})
        asset = asset_for(pair)
        if asset:
            source_days[asset, exchange].update(values['closed_days'])
    targets = {}
    for asset, markets in (spec or load_spec())['references'].items():
        for market, ref in markets.items():
            present = [e for e in entries if e['exchange'] == ref['exchange'] and e['market'] == ref['market']
                       and e['pair'] == ref['symbol'] and e['channel'] in ('snapshot', 'depth', 'book') and e['closed_days']]
            targets[f'{asset}_{market}'] = {'reference': ref, 'trainable': bool(present),
                'notes': 'Closed reference book files present; payload validity, warmup, label coverage and sufficient history still require replay.'}
    features = {}
    for exchange, market, channel in [('binance', 'perp', 'forceorder'), ('binance', 'perp', 'markprice'),
                                      ('kraken', 'perp', 'ticker'), ('kraken', 'perp', 'trades'), ('deribit', 'index', 'dvol')]:
        features[f'{exchange}_{market}_{channel}'] = sorted({asset_for(e['pair']) or e['pair'] for e in entries
            if (e['exchange'], e['market'], e['channel']) == (exchange, market, channel) and e['closed_days']})
    common = {}
    for asset in ('BTC', 'ETH'):
        venues = {exchange: days for (base, exchange), days in source_days.items() if base == asset}
        days = set.intersection(*venues.values()) if venues else set()
        common[asset] = {'exchanges': sorted(venues), 'days': [d.isoformat() for d in sorted(days)]}
    return {'archive_root': str(archive_root), 'entries': entries, 'targets': targets,
            'feature_sources': features, 'common_days': common,
            'notes': 'Days/bytes count all accepted copies, including open files; readiness and common days use closed files only. No payloads opened.'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive_root', type=Path)
    parser.add_argument('--json', dest='json_path', type=Path)
    args = parser.parse_args(argv)
    if args.json_path and args.json_path.resolve().is_relative_to(args.archive_root.resolve()):
        parser.error('--json must be outside the archive (inventory is read-only)')
    result = scan_archive(args.archive_root)
    print('Exchange  Market  Pair              Channel      First       Last        Days  Missing  Bytes  Open')
    for e in result['entries']:
        print(f"{e['exchange']:9} {e['market']:7} {e['pair']:17} {e['channel']:12} {e['first_day']}  {e['last_day']}  "
              f"{e['number_of_days']:4}  {len(e['missing_days']):7}  {e['total_bytes']:10}  {len(e['open_files']):4}")
        if e['missing_days']:
            print('  Gaps: ' + ', '.join(e['missing_days']))
        if e['open_files']:
            print('  Open: ' + ', '.join(e['open_files']))
    for key in ('targets', 'feature_sources', 'common_days'):
        print(key + ': ' + json.dumps(result[key], sort_keys=True))
    print(result['notes'])
    if args.json_path:
        args.json_path.write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    main()
