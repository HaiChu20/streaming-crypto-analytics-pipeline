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
    base = pl.coalesce(
        symbol.str.extract(r"^([^/-]+)[/-]", 1),
        symbol.str.extract(r"^(?:PF_)?(.+?)(?:FDUSD|USDT|USDC|USD|BTC|ETH|EUR)$", 1),
        symbol,
    )
    return pl.when(base == "XBT").then(pl.lit("BTC")).otherwise(base)


def build_matrix(rows: pl.DataFrame, target: str, stride_s: int = 10,
                 spec: dict | None = None) -> tuple[pl.DataFrame, list[str]]:
    if stride_s <= 0:
        raise ValueError("stride_s must be positive")
    spec = spec or load_spec()
    asset = target.rsplit("_", 1)[0]
    context_assets = sorted(set(spec.get("context_assets", [])) - {asset})
    reference_for(target, spec)  # validate the versioned reference
    ref = reference_rows(rows, target, spec)
    matrix = ref.filter(
        pl.col("book_valid") & ~pl.col("warmup") & (pl.col("mid_e9") > 0)
        & (pl.col("tick_ns") % (stride_s * NS) == 0)
    ).select("tick_ns")
    asset_column = asset_expr()
    if "asset" in rows.columns:
        # Older rows can omit the asset or contain an empty serde default.
        asset_column = pl.when(pl.col("asset").is_not_null() & (pl.col("asset") != "")).then(pl.col("asset")).otherwise(asset_column)
    selected = rows.with_columns(asset_column.alias("asset")).filter(
        pl.col("asset").is_in([asset, *context_assets])
        & (pl.col("tick_ns") % (stride_s * NS) == 0)
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
    # One symbol per venue/market/asset; context and target have separate namespaces.
    instruments = selected.select("asset", "exchange", "market", "symbol").unique().sort(
        "asset", "exchange", "market", "symbol"
    )
    prefixes = set()
    for base, exchange, market, symbol in instruments.iter_rows():
        context = base != asset
        prefix = (f"ctx_{base.lower()}_" if context else "") + f"{exchange}_{market}_"
        if prefix in prefixes:
            raise ValueError(f"Multiple symbols map to {prefix}{base}")
        prefixes.add(prefix)
        part = selected.filter((pl.col("asset") == base) & (pl.col("exchange") == exchange)
                               & (pl.col("market") == market) & (pl.col("symbol") == symbol))
        expressions = [pl.col(c).alias(prefix + c) for c in feature_cols]
        if context and base == "USDT":
            # USDT/USD price is stationary around one and supplies the USD/USDT
            # premium, while absolute BTC prices remain excluded from predictors.
            dollar_pair = symbol.upper() in {"USDTUSD", "USDT-USD", "USDT/USD"}
            premium = (pl.when(pl.lit(dollar_pair) & pl.col("book_valid")
                               & ~pl.col("warmup") & (pl.col("mid_e9") > 0))
                       .then((pl.col("mid_e9") - NS) / NS * 10_000)
                       .otherwise(None).alias(prefix + "premium_bps"))
            expressions.append(premium)
        matrix = matrix.join(part.select("tick_ns", *expressions), on="tick_ns", how="left")
    # Entirely absent context venues still have null predictors, including when
    # the context asset is absent from every file. Match the observed venue set.
    venues = selected.select("exchange", "market").unique().sort("exchange", "market").iter_rows()
    for exchange, market in venues:
        for base in context_assets:
            prefix = f"ctx_{base.lower()}_{exchange}_{market}_"
            if prefix not in prefixes:
                expressions = [pl.lit(None, dtype=rows.schema[c]).alias(prefix + c)
                               for c in feature_cols]
                if base == "USDT":
                    expressions.append(pl.lit(None, dtype=pl.Float64).alias(prefix + "premium_bps"))
                matrix = matrix.with_columns(expressions)
    matrix = matrix.join(momentum_900s(rows, target, spec), on="tick_ns", how="left")
    matrix = matrix.with_columns(pl.lit(asset).alias("asset")).sort("tick_ns")
    features = sorted(c for c in matrix.columns if c not in NON_FEATURES)
    return matrix, features
