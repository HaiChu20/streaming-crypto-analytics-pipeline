"""Exact endpoint returns and complete-path mid-price excursions (no spread/slippage)."""

from pathlib import Path

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


def make_labels(rows: pl.DataFrame, target: str, horizon_s: int,
                spec: dict | None = None) -> pl.DataFrame:
    if horizon_s <= 0:
        raise ValueError("horizon_s must be positive")
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
