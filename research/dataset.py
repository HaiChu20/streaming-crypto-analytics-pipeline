"""Same-tick wide matrices with per-row replay provenance and availability."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from research.labels import NS, load_spec, momentum_900s, reference_for, reference_rows

# Absolute prices are non-stationary. Timestamps/provenance never enter X.
IDENTITY = {"exchange", "market", "symbol", "tick_ns", "available_ns", "engine_version",
            "replay_mode", "asset"}
PRICES = {"mid_e9", "mid", "best_bid", "best_ask", "microprice", "mark_price"}
NON_FEATURES = {"asset", "tick_ns", "available_ns", "engine_version", "replay_mode",
                "label_end_ns", "label_available_ns", "return_pct", "direction"}


def feature_paths(root: str | Path) -> list[Path]:
    path = Path(root)
    paths = [path] if path.is_file() else sorted(path.glob("features/date=*/features.parquet"))
    if not paths:
        raise ValueError(f"No feature Parquet files under {path}")
    return paths


def read_features(paths: list[Path]) -> pl.DataFrame:
    frames = []
    for path in paths:
        metadata = pq.read_metadata(path).metadata or {}
        mode = metadata.get(b"mode", metadata.get(b"replay_mode", b"")).decode()
        if mode not in {"as_live", "ideal"}:
            raise ValueError(f"Missing/unknown replay mode in {path}: {mode!r}")
        frame = pl.read_parquet(path).with_columns(pl.lit(mode).alias("replay_mode"))
        version = metadata.get(b"engine_version", b"").decode()
        if version and frame.filter(pl.col("engine_version") != version).height:
            raise ValueError(f"Engine version disagrees with metadata in {path}")
        frames.append(frame)
    rows = pl.concat(frames, how="diagonal_relaxed")
    keys = ["exchange", "market", "symbol", "tick_ns"]
    if rows.select(keys).unique().height != rows.height:
        raise ValueError("Duplicate instrument ticks (including overlapping inputs)")
    if rows.filter((pl.col("tick_ns") % NS != 0) |
                   (pl.col("available_ns") < pl.col("tick_ns"))).height:
        raise ValueError("Feature ticks must be whole seconds and available_ns >= tick_ns")
    return rows


def asset_expr() -> pl.Expr:
    symbol = pl.col("symbol").str.to_uppercase()
    return (pl.when(symbol.str.contains("BTC|XBT")).then(pl.lit("BTC"))
            .when(symbol.str.contains("ETH")).then(pl.lit("ETH")))


def build_matrix(rows: pl.DataFrame, target: str, stride_s: int = 10,
                 spec: dict | None = None) -> tuple[pl.DataFrame, list[str]]:
    if stride_s <= 0:
        raise ValueError("stride_s must be positive")
    spec = spec or load_spec()
    asset = target.split("_")[0]
    reference_for(target, spec)  # validate the versioned reference
    ref = reference_rows(rows, target, spec)
    matrix = ref.filter(
        pl.col("book_valid") & ~pl.col("warmup") & (pl.col("mid_e9") > 0)
        & (pl.col("tick_ns") % (stride_s * NS) == 0)
    ).select("tick_ns")
    selected = rows.with_columns(asset_expr().alias("asset")).filter(
        (pl.col("asset") == asset) & (pl.col("tick_ns") % (stride_s * NS) == 0)
    ).join(matrix, on="tick_ns", how="semi")
    conflicts = selected.group_by("tick_ns").agg(
        pl.col("replay_mode").n_unique().alias("modes"),
        pl.col("engine_version").n_unique().alias("versions"),
    ).filter((pl.col("modes") != 1) | (pl.col("versions") != 1))
    if conflicts.height:
        raise ValueError("Cannot mix replay modes or engine versions within one feature tick")
    # A row can only be used once ALL same-tick inputs have actually been emitted.
    # available_ns is provenance, not a predictor; nominal feature time stays tick_ns.
    provenance = selected.group_by("tick_ns").agg(
        pl.col("available_ns").max(),
        pl.col("replay_mode").unique().sort().str.join("+").alias("replay_mode"),
        pl.col("engine_version").unique().sort().str.join("+").alias("engine_version"),
    )
    matrix = matrix.join(provenance, on="tick_ns", how="left")
    feature_cols = [c for c, dtype in rows.schema.items()
                    if c not in IDENTITY | PRICES and (dtype.is_numeric() or dtype == pl.Boolean)]
    # At most one native symbol per exchange/market/asset: fail rather than silently mix.
    instruments = selected.select("exchange", "market", "symbol").unique().sort(
        "exchange", "market", "symbol"
    )
    prefixes = set()
    for exchange, market, symbol in instruments.iter_rows():
        prefix = f"{exchange}_{market}_"
        if prefix in prefixes:
            raise ValueError(f"Multiple symbols map to {prefix}{asset}")
        prefixes.add(prefix)
        part = selected.filter((pl.col("exchange") == exchange)
                               & (pl.col("market") == market) & (pl.col("symbol") == symbol))
        matrix = matrix.join(part.select("tick_ns", *[
            pl.col(c).alias(prefix + c) for c in feature_cols
        ]), on="tick_ns", how="left")
    matrix = matrix.join(momentum_900s(rows, target, spec), on="tick_ns", how="left")
    matrix = matrix.with_columns(pl.lit(asset).alias("asset")).sort("tick_ns")
    features = sorted(c for c in matrix.columns if c not in NON_FEATURES)
    return matrix, features
