"""Exact endpoint returns and complete-path mid-price excursions (no spread/slippage)."""

from fractions import Fraction
from pathlib import Path

import numpy as np
import polars as pl
import yaml

NS = 1_000_000_000
SPEC_PATH = Path(__file__).with_name("targets.yaml")
LABEL_INPUTS = ["exchange", "market", "symbol", "tick_ns", "available_ns",
                "mid_e9", "book_valid", "warmup"]


def load_spec(path: str | Path = SPEC_PATH) -> dict:
    return yaml.safe_load(Path(path).read_text())


def reference_for(target: str, spec: dict | None = None) -> dict:
    """Resolve a configured <asset>_<market> reference, without a symbol fallback."""
    asset, market = target.rsplit("_", 1)
    return (spec or load_spec())["references"][asset][market]


def reference_rows(rows: pl.DataFrame, target: str, spec: dict | None = None) -> pl.DataFrame:
    ref = reference_for(target, spec)
    result = rows.filter(pl.all_horizontal([pl.col(k) == v for k, v in ref.items()]))
    if result["tick_ns"].n_unique() != result.height:
        raise ValueError(f"Duplicate reference ticks for {target}")
    return result.sort("tick_ns")


def reference_present(rows: pl.DataFrame, target: str, spec: dict | None = None) -> bool:
    """Availability is about instrument rows, independently of book validity."""
    ref = reference_for(target, spec)
    return bool(rows.select(pl.all_horizontal([pl.col(k) == v for k, v in ref.items()]).any()).item())


def exact_return_pct(start: int, end: int) -> float:
    # Subtract/multiply Python integers before division: even a one-unit move at
    # prices above 2**53 survives. mid_e9 also preserves half-atom book mids.
    return (100 * (int(end) - int(start))) / int(start)


def barrier_pct_for(horizon_s: int, spec: dict):
    """YAML uses integer horizon keys; saved JSON metadata uses string keys."""
    barriers = spec.get("barriers_pct", {})
    return barriers.get(horizon_s, barriers.get(str(horizon_s)))


def make_labels(rows: pl.DataFrame, target: str, horizon_s: int,
                spec: dict | None = None) -> pl.DataFrame:
    if horizon_s <= 0:
        raise ValueError("horizon_s must be positive")
    spec = spec or load_spec()
    if barrier_pct_for(horizon_s, spec) is not None:
        return _make_barrier_labels(rows, target, horizon_s, spec)
    ref = reference_rows(rows.select(LABEL_INPUTS), target, spec)
    future = ref.select(
        (pl.col("tick_ns") - horizon_s * NS).alias("tick_ns"),
        pl.col("mid_e9").alias("future_mid_e9"),
        pl.col("book_valid").alias("future_valid"),
        pl.col("available_ns").alias("label_available_ns"),
    )
    joined = ref.join(future, on="tick_ns", how="left")
    returns, directions = [], []
    for tick, valid, warmup, start, end, end_valid in joined.select(
        "tick_ns", "book_valid", "warmup", "mid_e9", "future_mid_e9", "future_valid"
    ).iter_rows():
        usable = (tick % NS == 0 and valid and warmup is False and end_valid
                  and start is not None and end is not None and start > 0 and end > 0)
        returns.append(exact_return_pct(start, end) if usable else None)
        directions.append(((end > start) - (end < start)) if usable else None)
    return joined.select("tick_ns", "label_available_ns").with_columns(
        (pl.col("tick_ns") + horizon_s * NS).alias("label_end_ns"),
        pl.Series("return_pct", returns, dtype=pl.Float64),
        pl.Series("direction", directions, dtype=pl.Int8),
    ).join(make_excursion_labels(rows, target, horizon_s, spec), on="tick_ns", how="left")


def _first_touch(prices: pl.Series, starts: np.ndarray, horizon_s: int,
                 upper: np.ndarray, lower: np.ndarray, up_possible: np.ndarray) -> np.ndarray:
    """First crossing index, or start+h+1 when neither barrier is touched.

    Binary lifting skips a block only when its min/max stay strictly between
    the exact integer barriers. At most log2(h)+1 vectorized passes are needed;
    rolling extrema take O(n) per pass. Only the current block size is retained,
    so auxiliary memory is O(n), rather than an O(n log h) sparse table. There
    are no Python loops over ticks or windows. Null/invalid paths are filtered
    before this search; they cannot acquire a partial-window label.
    """
    position = starts + 1
    end = starts + horizon_s + 1
    for power in range(horizon_s.bit_length() - 1, -1, -1):
        size = 1 << power
        eligible = np.flatnonzero(position + size <= end)
        if not len(eligible):
            continue
        indices = position[eligible]
        # A backward window ending at i+size-1 is the forward block at i.
        indices = indices + size - 1
        maxima = prices.rolling_max(size, min_samples=1).to_numpy()
        clear = (maxima[indices] < upper[eligible]) | ~up_possible[eligible]
        del maxima
        minima = prices.rolling_min(size, min_samples=1).to_numpy()
        clear &= minima[indices] > lower[eligible]
        del minima
        position[eligible[clear]] += size
    return position


def _exact_exit_returns(starts: pl.Series, exits: np.ndarray) -> np.ndarray:
    """Vectorized exact_return_pct, including correctly rounded Float64 division.

    Int128 numerator arithmetic alone is insufficient: casting numerator and
    denominator to Float64 before division can differ from Python integer
    division by one ULP. Round a 53-bit integer significand using the exact
    quotient/remainder, then apply its binary exponent. The initial floating
    exponent is only a scaling hint and is corrected by an integer comparison.
    Bounded batches keep Int128 temporaries small, independently of input size.
    """
    result = np.empty(len(exits), dtype=np.float64)
    for offset in range(0, len(exits), 262_144):
        count = min(262_144, len(exits) - offset)
        frame = pl.DataFrame({"start": starts.slice(offset, count),
                              "exit": exits[offset:offset + count]}).select(
            pl.col("start").cast(pl.Int128),
            ((pl.col("exit").cast(pl.Int128) - pl.col("start").cast(pl.Int128)) * 100).alias("num"))
        approximate = frame.select(pl.col("num") / pl.col("start")).to_series().to_numpy()
        frame = frame.with_columns(pl.Series("shift", 53 - np.frexp(approximate)[1]))
        two = pl.lit(2, dtype=pl.Int128)
        frame = frame.with_columns(
            (pl.col("num").abs() * two.pow(pl.col("shift").clip(lower_bound=0))).alias("scaled_num"),
            (pl.col("start") * two.pow((-pl.col("shift")).clip(lower_bound=0))).alias("den"))
        adjust = (pl.col("scaled_num") > 0) & (pl.col("scaled_num") < pl.col("den") * 2**52)
        frame = frame.with_columns(
            pl.when(adjust).then(pl.col("scaled_num") * 2).otherwise(pl.col("scaled_num")).alias("scaled_num"),
            (pl.col("shift") + adjust.cast(pl.Int32)).alias("shift"))
        quotient = pl.col("scaled_num") // pl.col("den")
        twice_remainder = (pl.col("scaled_num") % pl.col("den")) * 2
        round_up = ((twice_remainder > pl.col("den"))
                    | ((twice_remainder == pl.col("den")) & (quotient % 2 == 1)))
        rounded = frame.select(
            ((quotient + round_up.cast(pl.Int128)) * pl.col("num").sign()).cast(pl.Float64).alias("mantissa"),
            "shift")
        result[offset:offset + count] = np.ldexp(rounded["mantissa"].to_numpy(), -rounded["shift"].to_numpy())
    return result


def _make_barrier_labels(rows: pl.DataFrame, target: str, horizon_s: int,
                         spec: dict) -> pl.DataFrame:
    """Complete-window first-touch labels, with conservative horizon endpoints."""
    raw_pct = barrier_pct_for(horizon_s, spec)
    try:
        pct = Fraction(str(raw_pct))
    except (ValueError, ZeroDivisionError) as exc:
        raise ValueError("barrier pct must be finite and non-negative") from exc
    if pct < 0:
        raise ValueError("barrier pct must be finite and non-negative")
    numerator, scale = pct.numerator, 100 * pct.denominator
    # All threshold products (including the ceil adjustment) must fit Int128.
    if (scale + numerator + 1) * (2**63) >= 2**127:
        raise ValueError("barrier pct precision exceeds Int128 arithmetic")
    ref = reference_rows(rows.select(LABEL_INPUTS), target, spec)
    whole = ref.filter(pl.col("tick_ns") % NS == 0)
    if whole.is_empty():
        return ref.select("tick_ns").with_columns(
            pl.lit(None, dtype=pl.Int64).alias("label_available_ns"),
            (pl.col("tick_ns") + horizon_s * NS).alias("label_end_ns"),
            pl.lit(None, dtype=pl.Float64).alias("return_pct"),
            pl.lit(None, dtype=pl.Int8).alias("direction"),
            *[pl.lit(None, dtype=pl.Float64).alias(name) for name in
              ("adverse_long_bps", "adverse_short_bps", "favorable_long_bps", "favorable_short_bps")],
        )
    grid = pl.DataFrame({"tick_ns": pl.int_range(
        whole["tick_ns"].min(), whole["tick_ns"].max() + NS, NS, eager=True)
    }).join(whole.select("tick_ns", "mid_e9", "book_valid", "warmup", "available_ns"),
            on="tick_ns", how="left").select(
        "tick_ns", "mid_e9", "available_ns",
        (pl.col("book_valid") & ~pl.col("warmup")
         & (pl.col("mid_e9") > 0)).fill_null(False).alias("usable"),
    )

    def forward(expr, operation):
        return getattr(expr.reverse(), operation)(
            window_size=horizon_s, min_samples=horizon_s).reverse().shift(-1)

    grid = grid.with_columns(
        (pl.col("usable") & (forward(pl.col("usable").cast(pl.Int32), "rolling_sum")
                              == horizon_s)).fill_null(False).alias("complete"),
        pl.col("available_ns").shift(-horizon_s).alias("label_available_ns"),
    ).drop("available_ns", "usable")
    starts = np.flatnonzero(grid["complete"].to_numpy())
    start = pl.col("mid_e9").cast(pl.Int128)
    integer_scale = pl.lit(scale, dtype=pl.Int128)
    upper_product = start * pl.lit(scale + numerator, dtype=pl.Int128)
    thresholds = grid[starts].select(
        ((upper_product + scale - 1) // integer_scale).clip(0, 2**63 - 1)
            .cast(pl.Int64).alias("upper"),
        ((start * pl.lit(scale - numerator, dtype=pl.Int128)) // integer_scale)
            .clip(0, 2**63 - 1).cast(pl.Int64).alias("lower"),
        (upper_product <= pl.lit((2**63 - 1) * scale, dtype=pl.Int128)).alias("up_possible"),
    )
    positions = _first_touch(grid["mid_e9"].fill_null(0), starts, horizon_s,
                             thresholds["upper"].to_numpy(), thresholds["lower"].to_numpy(),
                             thresholds["up_possible"].to_numpy())
    touched = positions <= starts + horizon_s
    # No touch exits at the exact horizon endpoint; otherwise at first touch.
    exits = np.where(touched, positions, starts + horizon_s)
    exit_prices = grid["mid_e9"].fill_null(0).to_numpy()[exits]
    up = touched & thresholds["up_possible"].to_numpy() & (exit_prices >= thresholds["upper"].to_numpy())
    down = touched & (exit_prices <= thresholds["lower"].to_numpy())
    ambiguous = up & down
    returns = np.full(grid.height, np.nan)
    returns[starts] = _exact_exit_returns(grid["mid_e9"][starts], exit_prices)
    directions = np.zeros(grid.height, dtype=np.int8)
    directions[starts] = up.astype(np.int8) - down.astype(np.int8)
    valid = np.zeros(grid.height, dtype=bool)
    valid[starts] = ~ambiguous
    del thresholds, positions, exits, exit_prices, ambiguous
    fall = ((start - forward(pl.col("mid_e9"), "rolling_min").cast(pl.Int128))
            .clip(lower_bound=0) * 10_000) / start
    rise = ((forward(pl.col("mid_e9"), "rolling_max").cast(pl.Int128) - start)
            .clip(lower_bound=0) * 10_000) / start
    paths = grid.select(
        "tick_ns", "label_available_ns",
        (pl.col("tick_ns") + horizon_s * NS).alias("label_end_ns"),
        pl.when(pl.Series(valid)).then(pl.Series(returns)).otherwise(None).alias("return_pct"),
        pl.when(pl.Series(valid)).then(pl.Series(directions)).otherwise(None).alias("direction"),
        *[pl.when(pl.col("complete")).then(expr).otherwise(None).alias(name)
          for name, expr in zip(("adverse_long_bps", "adverse_short_bps",
                                 "favorable_long_bps", "favorable_short_bps"), (fall, rise, rise, fall))],
    )
    # Off-grid starts are null but retain the same conservative endpoint fields.
    return ref.select("tick_ns").join(paths, on="tick_ns", how="left").with_columns(
        (pl.col("tick_ns") + horizon_s * NS).alias("label_end_ns"))


def make_excursion_labels(rows: pl.DataFrame, target: str, horizon_s: int,
                          spec: dict | None = None) -> pl.DataFrame:
    """Worst/best mid-price moves in (t, t+h], in positive basis points.

    Stops are measured on the mid price, without spread or slippage. A dense
    whole-second grid makes rolling valid counts detect missing seconds as well
    as invalid/warm books. Off-grid rows cannot replace a missing second. Reverse
    rolling extrema/counts are O(n), independent of the horizon, with no row loops.
    Integer Int128 subtraction/multiplication avoids both Float64 cancellation
    above 2**53 and Int64 overflow; only the final ratio is floating point.
    """
    if horizon_s <= 0:
        raise ValueError("horizon_s must be positive")
    ref = reference_rows(rows.select(LABEL_INPUTS), target, spec)
    names = ["adverse_long_bps", "adverse_short_bps",
             "favorable_long_bps", "favorable_short_bps"]
    whole = ref.filter(pl.col("tick_ns") % NS == 0).select(
        "tick_ns", "mid_e9", "book_valid", "warmup", "available_ns")
    if whole.is_empty():
        return ref.select("tick_ns").with_columns(
            pl.lit(None, dtype=pl.Float64).alias(name) for name in names
        )
    grid = pl.DataFrame({"tick_ns": pl.int_range(
        whole["tick_ns"].min(), whole["tick_ns"].max() + NS, NS, eager=True)
    }).join(whole, on="tick_ns", how="left").with_columns(
        (pl.col("book_valid") & ~pl.col("warmup")
         & (pl.col("mid_e9") > 0)).fill_null(False).alias("usable")
    )

    def forward(expr, operation):
        return getattr(expr.reverse(), operation)(
            window_size=horizon_s, min_samples=horizon_s).reverse().shift(-1)

    grid = grid.with_columns(
        forward(pl.col("usable").cast(pl.Int64), "rolling_sum").alias("valid_seconds"),
        forward(pl.col("mid_e9"), "rolling_min").alias("path_min"),
        forward(pl.col("mid_e9"), "rolling_max").alias("path_max"),
    )
    usable = pl.col("usable") & (pl.col("valid_seconds") == horizon_s)
    start = pl.col("mid_e9").cast(pl.Int128)
    fall = ((start - pl.col("path_min").cast(pl.Int128)).clip(lower_bound=0) * 10_000) / start
    rise = ((pl.col("path_max").cast(pl.Int128) - start).clip(lower_bound=0) * 10_000) / start
    paths = grid.select("tick_ns", *[
        pl.when(usable).then(expr).otherwise(None).alias(name)
        for name, expr in zip(names, [fall, rise, rise, fall])
    ])
    return ref.select("tick_ns").join(paths, on="tick_ns", how="left")


def momentum_return(rows: pl.DataFrame, target: str, horizon_s: int,
                    spec: dict | None = None) -> pl.DataFrame:
    """Trailing return in percent; exact historical endpoint only."""
    ref = reference_rows(rows.select(LABEL_INPUTS), target, spec)
    past = ref.select(
        (pl.col("tick_ns") + horizon_s * NS).alias("tick_ns"),
        pl.col("mid_e9").alias("past_mid_e9"),
        pl.col("book_valid").alias("past_valid"),
        pl.col("available_ns").alias("past_available_ns"),
    )
    joined = ref.join(past, on="tick_ns", how="left")
    values = []
    for r in joined.select("book_valid", "warmup", "mid_e9", "past_mid_e9",
                           "past_valid", "past_available_ns", "available_ns").iter_rows():
        valid, warmup, end, start, past_valid, past_available, available = r
        usable = (valid and warmup is False and past_valid and start is not None
                  and end is not None and start > 0 and end > 0
                  and past_available <= available)
        values.append(exact_return_pct(start, end) if usable else None)
    return joined.select("tick_ns").with_columns(
        pl.Series(f"momentum_return_{horizon_s}s", values, dtype=pl.Float64)
    )


def momentum_900s(rows: pl.DataFrame, target: str, spec: dict | None = None) -> pl.DataFrame:
    """Trailing 900-second return; preserves the historical endpoint rule."""
    return momentum_return(rows, target, 900, spec)


def momentum_300s(rows: pl.DataFrame, target: str, spec: dict | None = None) -> pl.DataFrame:
    """Trailing 300-second return with the same endpoint rule as momentum_900s."""
    return momentum_return(rows, target, 300, spec)
