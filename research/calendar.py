"""Deterministic UTC clocks and public event schedules shared by train and live.

Macro features require complete schedule coverage for seven days before and
after each tick. Live coverage must extend at least seven days ahead.

NYSE regular hours model weekdays only (no holiday calendar). The funding clock
assumes Binance/OKX BTC/ETH 8-hour settlement; feed-derived times will replace it.
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import csv
import hashlib
import io
from pathlib import Path

import polars as pl

NS = 1_000_000_000
DAY = 86400 * NS
MINUTE = 60 * NS
CAP = 10080.
CAP_NS = int(CAP) * MINUTE
ROOT = Path(__file__).resolve().parents[1]


def calendar_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else ROOT / path


@dataclass(frozen=True)
class Calendar:
    rows: tuple[tuple[str, str, str], ...]
    covers_from: str
    covers_through: str
    sha256: str

    @classmethod
    def load(cls, path: str | Path):
        content = calendar_path(path).read_bytes()
        text = content.decode('utf-8')
        headers = dict(line[1:].strip().split(':', 1) for line in text.splitlines()
                       if line.startswith('#') and ':' in line)
        start, end = (headers[k].strip() for k in ('covers_from', 'covers_through'))
        if date.fromisoformat(start) > date.fromisoformat(end):
            raise ValueError('Calendar coverage is reversed')
        reader = csv.DictReader(io.StringIO('\n'.join(
            line for line in text.splitlines() if not line.startswith('#'))))
        if reader.fieldnames != ['event', 'release_utc', 'source']:
            raise ValueError('Calendar columns must be event,release_utc,source')
        rows = []
        for row in reader:
            event, release, source = (row[k] for k in reader.fieldnames)
            if event not in ('fomc', 'cpi', 'nfp') or not source:
                raise ValueError(f'Invalid calendar row: {row}')
            if datetime.strptime(release, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT%H:%M:%SZ') != release:
                raise ValueError(f'Invalid UTC release: {release}')
            rows.append((event, release, source))
        if len({r[:2] for r in rows}) != len(rows):
            raise ValueError('Duplicate calendar event/time')
        return cls(tuple(sorted(rows, key=lambda r: (r[1], r[0]))), start, end,
                   hashlib.sha256(content).hexdigest())

    @property
    def start_ns(self):
        return int(datetime.fromisoformat(self.covers_from).replace(tzinfo=timezone.utc).timestamp()) * NS

    @property
    def stop_ns(self):
        end = date.fromisoformat(self.covers_through) + timedelta(days=1)
        return int(datetime.combine(end, datetime.min.time(), timezone.utc).timestamp()) * NS

    def contains(self, tick_ns: int) -> bool:
        """Whether the complete seven-day macro look windows are covered."""
        return self.start_ns + CAP_NS <= tick_ns <= self.stop_ns - CAP_NS

    def past_rows(self, end_ns: int):
        """Events that could affect training through end_ns, including lookahead."""
        return [list(r[:2]) for r in self.rows if release_ns(r[1]) <= end_ns + CAP_NS]

    def metadata(self, end_ns: int):
        return dict(sha256=self.sha256, covers_from=self.covers_from,
                    covers_through=self.covers_through, past_rows=self.past_rows(end_ns))


def release_ns(value: str) -> int:
    return int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()) * NS


def verify_calendar(calendar: Calendar, metadata: dict) -> None:
    """Allow future schedule extensions only when training history is unchanged."""
    recorded = metadata.get('calendar')
    if not recorded:
        raise ValueError('Calendar provenance missing from model metadata; retrain the model')
    end = metadata['data_window']['end_ns']
    if calendar.sha256 != recorded['sha256']:
        if ('past_rows' not in recorded or calendar.past_rows(end) != recorded['past_rows']
                or calendar.covers_from > recorded['covers_from']
                or calendar.covers_through < recorded['covers_through']):
            raise ValueError('Calendar sha256 changed and past events/coverage differ from training')


def calendar_features(tick_ns: pl.Series, calendar: Calendar | str | Path) -> pl.DataFrame:
    """Vectorized features in input order; no market/history or wall-clock inputs.

    A release at t is in since, never next. Missing neighbours saturate at 7 days.
    Macro values require the full +/-7-day window within coverage, including
    the whole covers_through UTC date. Other clocks always compute. All outputs
    are Float64.
    """
    if not isinstance(calendar, Calendar):
        calendar = Calendar.load(calendar)
    frame = pl.DataFrame({'tick_ns': tick_ns.cast(pl.Int64)}).with_row_index('_order')
    t = pl.col('tick_ns')
    dt = t.cast(pl.Datetime('ns', 'UTC'))
    midnight = dt.dt.truncate('1d').cast(pl.Int64)
    day_fraction = (t % DAY) / DAY
    weekday = dt.dt.weekday() - 1
    ny = dt.dt.convert_time_zone('America/New_York')
    ny_min = ny.dt.hour().cast(pl.Int64) * 60 + ny.dt.minute()
    frame = frame.with_columns(
        (day_fraction * (2 * 3.141592653589793)).sin().alias('cal_hour_sin'),
        (day_fraction * (2 * 3.141592653589793)).cos().alias('cal_hour_cos'),
        ((weekday + day_fraction) * (2 * 3.141592653589793 / 7)).sin().alias('cal_dow_sin'),
        ((weekday + day_fraction) * (2 * 3.141592653589793 / 7)).cos().alias('cal_dow_cos'),
        (weekday >= 5).alias('cal_weekend'),
        ((ny.dt.weekday() <= 5) & (ny_min >= 570) & (ny_min < 960)).alias('cal_us_session'),
    )
    # All expiry candidates are constructed from UTC midnight. Exact boundaries
    # roll forward, including Friday/quarter-end coincidences.
    daily = midnight + 8 * 3600 * NS
    weekly = daily + ((5 - dt.dt.weekday().cast(pl.Int64)) % 7) * DAY

    def last_friday(day):
        end = day.dt.month_end()
        return (end.cast(pl.Int64) - ((end.dt.weekday().cast(pl.Int64) - 5) % 7) * DAY
                + 8 * 3600 * NS)

    month = dt.dt.truncate('1mo')
    quarter = dt.dt.truncate('1q').dt.offset_by('2mo')
    candidates = {
        'daily': (daily, daily + DAY),
        'weekly': (weekly, weekly + 7 * DAY),
        'monthly': (last_friday(month), last_friday(month.dt.offset_by('1mo'))),
        'quarterly': (last_friday(quarter), last_friday(quarter.dt.offset_by('3mo'))),
    }
    frame = frame.with_columns(*[
        ((pl.when(first > t).then(first).otherwise(following) - t) / MINUTE)
        .alias(f'cal_expiry_min_to_{kind}') for kind, (first, following) in candidates.items()
    ], ((8 * 3600 * NS - t % (8 * 3600 * NS)) / MINUTE).alias('cal_funding_min_to_8h'))
    frame = frame.sort('tick_ns')
    covered = (t >= calendar.start_ns + CAP_NS) & (t <= calendar.stop_ns - CAP_NS)
    for event in ('fomc', 'cpi', 'nfp'):
        events = pl.DataFrame({'_release': [release_ns(r[1]) for r in calendar.rows if r[0] == event]},
                              schema={'_release': pl.Int64}).sort('_release')
        for kind, strategy, exact in [('to_next', 'forward', False), ('since_last', 'backward', True)]:
            frame = frame.join_asof(events, left_on='tick_ns', right_on='_release',
                                    strategy=strategy, allow_exact_matches=exact)
            delta = pl.col('_release') - t if strategy == 'forward' else t - pl.col('_release')
            frame = frame.with_columns(
                pl.when(covered).then((delta / MINUTE).clip(0, CAP).fill_null(CAP))
                .otherwise(None).alias(f'cal_{event}_min_{kind}')
            ).drop('_release')
    return frame.sort('_order').select(pl.col('^cal_.*$').cast(pl.Float64))
