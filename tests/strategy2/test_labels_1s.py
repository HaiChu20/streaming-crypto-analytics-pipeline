"""First-touch barriers and exact integer thresholds, independent of Strategy 1."""

from fractions import Fraction
import json

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from strategy2.labels_1s import (NS, _exact_exit_returns, exact_return_pct, load_spec,
                                 make_labels)


def frame(mids):
    n = len(mids)
    return pl.DataFrame({"exchange": ["kraken"] * n, "market": ["spot"] * n,
                         "symbol": ["BTC/USD"] * n, "tick_ns": np.arange(n) * NS,
                         "available_ns": np.arange(n) * NS + 250_000_000,
                         "mid_e9": mids, "book_valid": [True] * n, "warmup": [False] * n})


def test_barrier_spec_preserves_kraken_references_and_calendar():
    spec = load_spec()
    assert spec["version"] == "v1-kraken-spot-barrier"
    assert spec["calendar"] == "config/events.csv"
    assert spec["horizons_s"] == [3600, 14400]
    assert spec["barriers_pct"] == {3600: .5, 14400: 1.}
    assert spec["context_assets"] == []
    assert spec["references"] == {
        "BTC": {"spot": {"exchange": "kraken", "market": "spot", "symbol": "BTC/USD"}},
        "ETH": {"spot": {"exchange": "kraken", "market": "spot", "symbol": "ETH/USD"}}}


def spec_for(horizon, pct):
    return dict(load_spec(), barriers_pct={horizon: pct})


@pytest.mark.parametrize('mids,direction,exit_mid', [
    ([10000, 10051, 9940, 10010], 1, 10051),
    ([10000, 9949, 10060, 10010], -1, 9949),
    ([10000, 10049, 9951, 10010], 0, 10010),
    ([10000, 10050, 9940, 10010], 1, 10050),
    ([10000, 9950, 10060, 10010], -1, 9950),
    ([10000, 10000, 10000, 10050], 1, 10050),
])
def test_first_touch_order_neither_equality_and_endpoint(mids, direction, exit_mid):
    rows = frame(mids)
    labels = make_labels(rows, 'BTC_spot', 3, spec_for(3, .5))
    assert labels['direction'][0] == direction
    assert labels['return_pct'][0] == exact_return_pct(mids[0], exit_mid)
    assert labels['label_end_ns'][0] == 3 * NS
    assert labels['label_available_ns'][0] == 3 * NS + 250_000_000
    fall = max(0, mids[0] - min(mids[1:])) * 10_000 / mids[0]
    rise = max(0, max(mids[1:]) - mids[0]) * 10_000 / mids[0]
    assert labels['adverse_long_bps'][0] == fall
    assert labels['adverse_short_bps'][0] == rise
    assert labels['favorable_long_bps'][0] == rise
    assert labels['favorable_short_bps'][0] == fall
    assert labels['direction'][-1] is None
    assert_frame_equal(labels, make_labels(rows, 'BTC_spot', 3, json.loads(json.dumps(spec_for(3, .5)))))


def test_same_second_tie_is_null_and_full_path_excursions_remain():
    # One mid cannot touch both positive-width barriers in a single second.
    # Zero width exercises the explicitly required ambiguity rule.
    labels = make_labels(frame([100, 100, 102]), 'BTC_spot', 2, spec_for(2, 0))
    assert labels['return_pct'][0] is labels['direction'][0] is None
    assert labels['label_end_ns'][0] == 2 * NS
    assert labels['label_available_ns'][0] == 2 * NS + 250_000_000
    assert labels['favorable_long_bps'][0] == 200


@pytest.mark.parametrize('defect', ['missing', 'invalid', 'warm', 'null', 'zero', 'offgrid'])
@pytest.mark.parametrize('second', [0, 2, 3])
def test_whole_window_required_even_after_an_early_touch(defect, second):
    rows = frame([10000, 10100, 9900, 10000])
    if defect == 'missing':
        rows = rows.filter(pl.col('tick_ns') != second * NS)
        if second == 0:
            return
    else:
        column, value = {'invalid': ('book_valid', False), 'warm': ('warmup', True),
                         'null': ('mid_e9', None), 'zero': ('mid_e9', 0),
                         'offgrid': ('tick_ns', second * NS + 1)}[defect]
        rows = rows.with_columns(pl.when(pl.col('tick_ns') == second * NS).then(pl.lit(value))
                                .otherwise(pl.col(column)).alias(column))
    labels = make_labels(rows, 'BTC_spot', 3, spec_for(3, .5))
    assert labels['direction'][0] is labels['return_pct'][0] is None
    assert labels['adverse_long_bps'][0] is None


@pytest.mark.parametrize('start,pct,delta,direction', [
    (2**53 + 1, '0.00000000000001', 1, 1),
    (2**53 + 1, '0.00000000000001', -1, -1),
    (2**63 - 2, '0.00000000000000001', 1, 1),
    (2**63 - 2, .5, 1, 0),  # upper threshold exceeds Int64; no overflow or false touch
    (200 * (2**53 + 1), .5, 2**53 + 1, 1),  # exact equality above 2**53
    (200 * (2**53 + 1), .5, -(2**53 + 1), -1),
])
def test_large_integer_thresholds_and_returns(start, pct, delta, direction):
    # An unrelated null must not coerce the entire dense price array to Float64.
    rows = frame([start, start + delta, start, None, start])
    labels = make_labels(rows, 'BTC_spot', 2, spec_for(2, pct))
    assert labels['direction'][0] == direction
    end = start + delta if direction else start
    expected = float(Fraction(100 * (end - start), start))
    assert labels['return_pct'][0] == expected
    if direction:
        assert labels['return_pct'][0] != 0


@pytest.mark.parametrize('pct', [-.5, float('nan'), float('inf'), 'bad'])
def test_invalid_barrier_pct_rejected(pct):
    with pytest.raises(ValueError, match='finite and non-negative'):
        make_labels(frame([100, 101]), 'BTC_spot', 1, spec_for(1, pct))


def test_empty_and_offgrid_inputs():
    for rows in (frame([100, 101]).head(0),
                 frame([100, 101]).with_columns(pl.col('tick_ns') + 1)):
        labels = make_labels(rows, 'BTC_spot', 1, spec_for(1, .5))
        assert labels.height == rows.height
        assert labels['direction'].null_count() == rows.height
        assert_frame_equal(labels.select('label_end_ns'), rows.select(pl.col('tick_ns') + NS)
                           .rename({'tick_ns': 'label_end_ns'}))


@pytest.mark.parametrize('horizon', [1, 2, 3, 4, 7, 8, 15, 32, 61])
def test_vectorized_search_matches_exact_brute_force(horizon):
    rng = np.random.default_rng(718)
    mids = rng.integers(9900, 10101, size=150).tolist()
    pct = Fraction('0.5')
    labels = make_labels(frame(mids), 'BTC_spot', horizon, spec_for(horizon, .5))
    for i, start in enumerate(mids[:-horizon]):
        expected_direction, end = 0, mids[i + horizon]
        for candidate in mids[i + 1:i + horizon + 1]:
            up = candidate * 100 * pct.denominator >= start * (100 * pct.denominator + pct.numerator)
            down = candidate * 100 * pct.denominator <= start * (100 * pct.denominator - pct.numerator)
            if up or down:
                expected_direction, end = int(up) - int(down), candidate
                break
        assert labels['direction'][i] == expected_direction
        assert labels['return_pct'][i] == exact_return_pct(start, end)


def test_vectorized_exit_returns_match_python_integer_division_bit_for_bit():
    rng = np.random.default_rng(1718)
    starts = rng.integers(1, 2**63 - 1, size=1024)
    exits = rng.integers(1, 2**63 - 1, size=1024)
    # Adjacent integers and binary exponent boundaries exercise cancellation,
    # scaling, and ties-to-even rounding at both ends of the Int64 price range.
    starts = np.r_[starts, [1, 2**53 + 1, 2**63 - 2, 2**63 - 1, 100, 100]]
    exits = np.r_[exits, [2**63 - 1, 2**53 + 2, 2**63 - 1, 1, 100, 101]]
    expected = np.array([exact_return_pct(a, b) for a, b in zip(starts, exits)])
    np.testing.assert_array_equal(_exact_exit_returns(pl.Series(starts), exits), expected)


@pytest.mark.parametrize("horizon", [0, -1, 5])
def test_invalid_or_unconfigured_horizon_rejected(horizon):
    with pytest.raises(ValueError):
        make_labels(frame([100, 101]), "BTC_spot", horizon)
