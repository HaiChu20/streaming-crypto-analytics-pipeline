"""Exact endpoint labels. No interpolation, forward fill, or float price conversion."""

from pathlib import Path

import polars as pl
import yaml

NS = 1_000_000_000
SPEC_PATH = Path(__file__).with_name("targets.yaml")


def load_spec(path: str | Path = SPEC_PATH) -> dict:
    return yaml.safe_load(Path(path).read_text())


def reference_for(target: str, spec: dict | None = None) -> dict:
    """Target names are BTC_spot, BTC_perp, ETH_spot, ETH_perp."""
    asset, market = target.split("_")
    return (spec or load_spec())["references"][asset][market]


def reference_rows(rows: pl.DataFrame, target: str, spec: dict | None = None) -> pl.DataFrame:
    ref = reference_for(target, spec)
    result = rows.filter(pl.all_horizontal([pl.col(k) == v for k, v in ref.items()]))
    if result["tick_ns"].n_unique() != result.height:
        raise ValueError(f"Duplicate reference ticks for {target}")
    return result.sort("tick_ns")


def exact_return_pct(start: int, end: int) -> float:
    # Subtract/multiply Python integers before division: even a one-unit move at
    # prices above 2**53 survives. mid_e9 also preserves half-atom book mids.
    return (100 * (int(end) - int(start))) / int(start)


def make_labels(rows: pl.DataFrame, target: str, horizon_s: int,
                spec: dict | None = None) -> pl.DataFrame:
    if horizon_s <= 0:
        raise ValueError("horizon_s must be positive")
    ref = reference_rows(rows, target, spec)
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
    )


def momentum_900s(rows: pl.DataFrame, target: str, spec: dict | None = None) -> pl.DataFrame:
    """Trailing 900-second return in percent; exact historical endpoint only."""
    ref = reference_rows(rows, target, spec)
    past = ref.select(
        (pl.col("tick_ns") + 900 * NS).alias("tick_ns"),
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
        pl.Series("momentum_return_900s", values, dtype=pl.Float64)
    )
