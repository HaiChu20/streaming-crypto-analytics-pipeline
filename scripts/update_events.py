"""Refresh the versioned public calendar. Run from the repo: uv run python -m scripts.update_events."""
import argparse
import calendar as month_calendar
from datetime import date, datetime, timezone
import csv
import html
import io
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.calendar import Calendar, ROOT  # noqa: E402

BLS = 'https://www.bls.gov/'
ARCHIVES = {'cpi': BLS + 'bls/news-release/cpi.htm', 'nfp': BLS + 'bls/news-release/empsit.htm'}
ICS = BLS + 'schedule/news_release/bls.ics'
FED = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
UA = 'CryptoAnalyticsCalendar/1.0 (public economic release schedule updater)'


def local_release(event: str, day: str) -> str:
    hour, minute = (14, 0) if event == 'fomc' else (8, 30)
    dt = datetime.combine(date.fromisoformat(day), datetime.min.time()).replace(
        hour=hour, minute=minute, tzinfo=ZoneInfo('America/New_York'))
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_bls_archive(text: str, event: str):
    prefix = {'cpi': 'cpi', 'nfp': 'empsit'}[event]
    # Only actual archived filenames count. A 'Not published' table cell has no
    # stamp and must never manufacture a release for its reference month.
    stamps = sorted(set(re.findall(rf'{prefix}_(\d{{8}})\.(?:htm|txt|pdf)', text, re.I)))
    rows = []
    for stamp in stamps:
        day = datetime.strptime(stamp, '%m%d%Y').date().isoformat()
        rows.append((event, local_release(event, day), ARCHIVES[event]))
    return sorted(rows, key=lambda r: r[1])


def parse_ics(text: str):
    text = re.sub(r'\r?\n[ \t]', '', text)  # RFC 5545 line unfolding
    rows = []
    for block in re.findall(r'BEGIN:VEVENT\s*(.*?)END:VEVENT', text, re.S):
        fields = dict(line.split(':', 1) for line in block.splitlines() if ':' in line)
        if fields.get('STATUS', '').strip() == 'CANCELLED':
            continue
        title = fields.get('SUMMARY', '').strip()
        event = {'Consumer Price Index': 'cpi', 'Employment Situation': 'nfp'}.get(title)
        if not event:
            continue
        key = next((k for k in fields if k.split(';')[0] == 'DTSTART'), None)
        if key is None:
            raise ValueError(f'Missing DTSTART for {title}')
        value = fields[key].strip()
        if value.endswith('Z'):
            dt = datetime.strptime(value, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
        else:
            match = re.search(r'TZID="?([^;\"]+)', key)
            zone = ZoneInfo(match[1] if match else 'America/New_York')
            dt = datetime.strptime(value, '%Y%m%dT%H%M%S').replace(tzinfo=zone)
        # Release times are the published 08:30 NY, even across DST changes.
        rows.append((event, local_release(event, dt.astimezone(ZoneInfo('America/New_York')).date().isoformat()), ICS))
    return sorted(set(rows), key=lambda r: (r[1], r[0]))


def plain(text):
    return html.unescape(re.sub('<[^>]+>', '', text)).strip()


def parse_fed(text: str):
    headings = list(re.finditer(r'(\d{4}) FOMC Meetings', text))
    rows = []
    months = {name[:3].lower(): i for i, name in enumerate(month_calendar.month_name) if name}
    for i, heading in enumerate(headings):
        year = int(heading[1])
        section = text[heading.end():headings[i + 1].start() if i + 1 < len(headings) else len(text)]
        pairs = re.findall(r'<div class="[^\"]*fomc-meeting__month[^\"]*"[^>]*>(.*?)</div>\s*'
                           r'<div class="[^\"]*fomc-meeting__date[^\"]*"[^>]*>(.*?)</div>', section, re.S)
        for month_text, day_text in pairs:
            day_text = plain(day_text)
            if 'notation' in day_text.lower():
                continue
            month_text = plain(month_text).split('/')[-1].split('-')[-1]
            month = months[month_text[:3].lower()]
            # Jan/Feb 31-1, Apr/May 30-1, and ordinary 30-31 meetings.
            days = re.findall(r'\d+', day_text)
            if not days:
                raise ValueError(f'Unknown FOMC meeting date: {day_text}')
            day = date(year, month, int(days[-1])).isoformat()
            rows.append(('fomc', local_release('fomc', day), FED))
    return sorted(set(rows), key=lambda r: r[1])


def render_events(rows, covers_from: str, covers_through: str, note=''):
    output = io.StringIO()
    output.write(f'# covers_from: {covers_from}\n# covers_through: {covers_through}\n')
    output.write('# Public BLS releases (08:30 NY) and FOMC statements (14:00 NY); zoneinfo DST.\n')
    if note:
        output.write('# ' + note + '\n')
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(['event', 'release_utc', 'source'])
    writer.writerows(sorted(set(rows), key=lambda r: (r[1], r[0])))
    return output.getvalue()


def write_update(path: Path, rows, covers_from: str, covers_through: str,
                 accept_changes=False, today: date | None = None, dry_run=False):
    old = Calendar.load(path) if path.exists() else None
    before = set(old.rows) if old else set()
    after = set(rows)
    added, removed = after - before, before - after
    # A change is a removed+added release for the same event/date (time/source
    # corrections); date moves are still visible as one removal and one addition.
    changed = {(a, b) for a in removed for b in added if (a[0], a[1][:10]) == (b[0], b[1][:10])}
    print(f'Added {len(added)} rows; changed {len(changed)} rows; removed {len(removed)} rows')
    for prefix, values in [('+', added), ('-', removed)]:
        for row in sorted(values, key=lambda r: (r[1], r[0])):
            print(prefix, ','.join(row))
    today = today or datetime.now(timezone.utc).date()
    past_removed = [r for r in removed if r[1][:10] <= today.isoformat()]
    coverage_shrank = old and (covers_from > old.covers_from or covers_through < old.covers_through)
    if (past_removed or coverage_shrank) and not accept_changes:
        raise ValueError('Changed/removed past rows or reduced coverage require --accept-changes; file unchanged')
    if not dry_run:
        path.write_text(render_events(rows, covers_from, covers_through))


def refreshed_rows(old: Calendar, archives: dict, upcoming, fed, today: date):
    """Replace authoritative source intervals; retain history absent from rolling pages.

    Archive min/max release months delimit refreshed history. The ICS refreshes future
    dates only, so cancelled schedules cannot erase actual archived releases.
    """
    rows = list(old.rows)
    for event, actual in archives.items():
        if not actual:
            raise ValueError(f'No {event} archive releases parsed; refusing incomplete update')
        lo, hi = min(r[1][:7] for r in actual), max(r[1][:7] for r in actual)
        rows = [r for r in rows if not (r[0] == event and lo <= r[1][:7] <= hi)]
        rows.extend(actual)
    if not fed:
        raise ValueError('No FOMC meetings parsed')
    years = {r[1][:4] for r in fed}
    rows = [r for r in rows if not (r[0] == 'fomc' and r[1][:4] in years)] + fed
    latest = []
    for event in ('cpi', 'nfp'):
        future = [r for r in upcoming if r[0] == event and r[1][:10] > today.isoformat()]
        if not future:
            raise ValueError(f'No upcoming {event} releases parsed; coverage cannot be extended')
        end = max(r[1][:10] for r in future)
        latest.append(end)
        # Actual archives take precedence if a feed still carries stale schedules.
        actual_days = {r[1][:10] for r in archives[event]}
        rows = [r for r in rows if not (r[0] == event and today.isoformat() < r[1][:10] <= end
                                       and r[1][:10] not in actual_days)]
        rows.extend(r for r in future if r[1][:10] not in actual_days)
    through = min(*latest, max(years) + '-12-31')
    # Keep one deterministic source per event/time; the actual archive wins.
    unique = {r[:2]: r for r in rows if r[1][:10] >= old.covers_from}
    return list(unique.values()), through


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'config/events.csv')
    parser.add_argument('--accept-changes', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    old = Calendar.load(args.output)
    # Fetch each public page once. A blocked or malformed source aborts before
    # writing; no retries, alternate endpoints, or guessing missing releases.
    with httpx.Client(headers={'User-Agent': UA}, timeout=30) as client:
        pages = {}
        for url in [*ARCHIVES.values(), ICS, FED]:
            response = client.get(url)
            response.raise_for_status()
            pages[url] = response.text
    today = datetime.now(timezone.utc).date()
    rows, through = refreshed_rows(old, {e: parse_bls_archive(pages[u], e) for e, u in ARCHIVES.items()},
                                   parse_ics(pages[ICS]), parse_fed(pages[FED]), today)
    write_update(args.output, rows, old.covers_from, through, args.accept_changes, today, args.dry_run)
    print(f'Coverage: {old.covers_from} through {through}')


if __name__ == '__main__':
    main()
