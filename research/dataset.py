"""Same-tick wide matrices with per-row replay provenance and availability."""

from datetime import date
from pathlib import Path
import re

import polars as pl
import pyarrow.parquet as pq

from research.calendar import Calendar, calendar_features
from research.labels import (LABEL_INPUTS, NS, load_spec, momentum_300s, momentum_900s,
                             reference_for, reference_rows)

# Absolute prices are non-stationary. Timestamps/provenance never enter X.
IDENTITY = {"exchange", "market", "symbol", "tick_ns", "available_ns", "engine_version",
            "replay_mode", "asset"}
PRICES = {"mid_e9", "mid", "best_bid", "best_ask", "microprice", "mark_price"}
NON_FEATURES = {"asset", "tick_ns", "available_ns", "engine_version", "replay_mode",
                "label_end_ns", "label_available_ns", "return_pct", "direction",
                "adverse_long_bps", "adverse_short_bps", "favorable_long_bps", "favorable_short_bps"}

FEATURE_GROUPS = {
    'calendar': r'^cal_',
    'liquidations': r'_liq_',
    'l3': r'_l3_',
    'flow': r'_(?:flow|ofi1|ofi10|trades|vol)_\d+s$|_(?:taker_buy_base|taker_sell_base|trade_count|volume_base)$',
    'returns': r'_(?:ret|return)_\d+s$|^momentum_',
    'volatility': r'_(?:rv|range_bps)_\d+s$|_dvol(?:$|_chg_\d+s$|_rv_gap$)',
    'book': r'_(?:spread_bps|imbalance10|microprice_dev_bps|ask_depth10|bid_depth10|depth_(?:ask|bid)_\d+bps|book_age_ms)$',
    'positioning': r'_(?:funding_|open_interest$|oi_chg_pct_|mark_index_bps$|basis_bps_)',
    'cross': r'^cross_cross_',
    'quality': r'_(?:book_valid|warmup|cov_\d+s)$',
    'venue_kraken': r'^kraken_',
    'venue_binance': r'^binance_',
    'context': r'^ctx_',
}


def select_features(features, exclude_groups=(), exclude_features=()):
    """Preserve saved feature order; every requested exclusion must match input."""
    excluded = set()
    patterns = []
    for name in exclude_groups:
        if name not in FEATURE_GROUPS:
            raise ValueError(f'Unknown feature group: {name}')
        patterns.append((name, FEATURE_GROUPS[name]))
    patterns.extend((pattern, pattern) for pattern in exclude_features)
    for name, pattern in patterns:
        try:
            matches = {f for f in features if re.search(pattern, f)}
        except re.error as exc:
            raise ValueError(f'Invalid feature exclusion regex {pattern!r}: {exc}') from exc
        if not matches:
            raise ValueError(f'Feature exclusion matches nothing: {name}')
        excluded.update(matches)
    retained = [f for f in features if f not in excluded]
    if not retained and patterns:
        raise ValueError('Feature exclusions remove every feature')
    return retained, {'groups': list(exclude_groups), 'patterns': list(exclude_features), 'excluded': sorted(excluded)}


def feature_paths(root: str | Path) -> list[Path]:
    path = Path(root)
    paths = [path] if path.is_file() else sorted(path.glob("features/date=*/features.parquet"))
    if not paths:
        raise ValueError(f"No feature Parquet files under {path}")
    return paths


def read_features(paths: list[Path], targets=None, spec: dict | None = None, *,
                  stride_s: int | None = None, before_ns: int | None = None) -> pl.DataFrame:
    """Load stride features plus dense reference label inputs, or full rows by default."""
    spec = spec or load_spec()
    if stride_s is not None and stride_s <= 0:
        raise ValueError("stride_s must be positive")
    references = [reference_for(target, spec) for target in (targets or [
        f'{asset}_{market}' for asset, markets in spec['references'].items() for market in markets
    ])] if stride_s is not None else []
    assets = ({target.rsplit('_', 1)[0] for target in targets} |
              set(spec.get('context_assets', []))) if targets else None
    frames = []
    for path in paths:
        metadata = pq.read_metadata(path).metadata or {}
        mode = metadata.get(b"mode", metadata.get(b"replay_mode", b"")).decode()
        if mode not in {"as_live", "ideal"}:
            raise ValueError(f"Missing/unknown replay mode in {path}: {mode!r}")
        query = pl.scan_parquet(path)
        if before_ns is not None:
            query = query.filter(pl.col('tick_ns') < before_ns)
        if assets:
            derived = asset_expr()
            keep = derived.is_in(sorted(assets))
            if 'asset' in query.collect_schema().names():
                instrument_asset = (pl.when(pl.col('asset').is_not_null() & (pl.col('asset') != ''))
                                    .then(pl.col('asset')).otherwise(derived))
                keep = instrument_asset.is_in(sorted(assets)) | (
                    (pl.col('exchange') == 'cross') & derived.is_in(sorted(assets)))
            query = query.filter(keep)
        version = metadata.get(b"engine_version", b"").decode()
        queries = [query]
        if stride_s is not None:
            stride = pl.col('tick_ns') % (stride_s * NS) == 0
            reference = pl.any_horizontal([
                pl.all_horizontal([pl.col(k) == v for k, v in ref.items()]) for ref in references
            ])
            dense_columns = list(dict.fromkeys([
                *LABEL_INPUTS, 'engine_version',
                *(['asset'] if 'asset' in query.collect_schema().names() else []),
            ]))
            # Collect separately: a shared-scan cache must not materialize all
            # columns for the dense branch. The predicates are disjoint.
            queries = [query.filter(stride),
                       query.filter(~stride & reference).select(dense_columns)]
        for part in queries:
            frame = part.with_columns(pl.lit(mode).alias("replay_mode")).collect()
            if version and frame.filter(pl.col("engine_version") != version).height:
                raise ValueError(f"Engine version disagrees with metadata in {path}")
            frames.append(frame)
    rows = pl.concat(frames, how="diagonal_relaxed", rechunk=False)
    del frames, frame, queries, query, part
    keys = ["exchange", "market", "symbol", "tick_ns"]
    if rows.select(keys).unique().height != rows.height:
        raise ValueError("Duplicate instrument ticks (including overlapping inputs)")
    if rows.filter((pl.col("tick_ns") % NS != 0) |
                   (pl.col("available_ns") < pl.col("tick_ns"))).height:
        raise ValueError("Feature ticks must be whole seconds and available_ns >= tick_ns")
    return rows


def feature_dates(paths: list[Path], ticks=None) -> list[str]:
    """UTC dates from features/date= paths, independent of target availability."""
    parsed = []
    for path in paths:
        match = re.fullmatch(r'date=(\d{4}-\d{2}-\d{2})', path.parent.name)
        if match:
            parsed.append(date.fromisoformat(match.group(1)))
    if parsed and len(parsed) == len(paths):
        return sorted({day.isoformat() for day in parsed})
    if ticks is None:
        raise ValueError('Feature inputs must use features/date=D paths')
    import numpy as np
    from research.evaluation import utc_day
    return sorted({utc_day(tick) for tick in np.asarray(ticks, dtype=np.int64)})


def asset_expr() -> pl.Expr:
    symbol = pl.col("symbol").str.to_uppercase()
    base = pl.coalesce(
        symbol.str.extract(r"^([^/-]+)[/-]", 1),
        symbol.str.extract(r"^(?:PF_)?(.+?)(?:FDUSD|USDT|USDC|USD|BTC|ETH|EUR)$", 1),
        symbol,
    )
    return pl.when(base == "XBT").then(pl.lit("BTC")).otherwise(base)


def build_matrix(rows: pl.DataFrame, target: str, stride_s: int = 10,
                 spec: dict | None = None, *,
                 calendar: Calendar | None = None) -> tuple[pl.DataFrame, list[str]]:
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
    normalized = rows.with_columns(asset_column.alias("asset"))
    # In lean mode, context presence requires rows at stride ticks; off-stride-only context is omitted.
    present_context = set(normalized['asset'].unique().to_list()) & set(context_assets)
    selected = normalized.filter(
        pl.col("asset").is_in([asset, *context_assets])
        & (pl.col("tick_ns") % (stride_s * NS) == 0)
    ).join(matrix, on="tick_ns", how="semi")
    del normalized
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
                       # One floating multiply keeps scalar/live and vector/training
                       # execution identical (a divide then multiply can differ by 1 ULP).
                       .then((pl.col("mid_e9") - NS) * (10_000 / NS))
                       .otherwise(None).alias(prefix + "premium_bps"))
            expressions.append(premium)
        matrix = matrix.join(part.select("tick_ns", *expressions), on="tick_ns", how="left")
    # Fill missing venues only for context assets observed somewhere in input.
    venues = selected.select("exchange", "market").unique().sort("exchange", "market").iter_rows()
    for exchange, market in venues:
        for base in sorted(present_context):
            prefix = f"ctx_{base.lower()}_{exchange}_{market}_"
            if prefix not in prefixes:
                expressions = [pl.lit(None, dtype=rows.schema[c]).alias(prefix + c)
                               for c in feature_cols]
                if base == "USDT":
                    expressions.append(pl.lit(None, dtype=pl.Float64).alias(prefix + "premium_bps"))
                matrix = matrix.with_columns(expressions)
    matrix = matrix.join(momentum_900s(rows, target, spec), on="tick_ns", how="left")
    matrix = matrix.join(momentum_300s(rows, target, spec), on="tick_ns", how="left")
    matrix = matrix.with_columns(pl.lit(asset).alias("asset")).sort("tick_ns")
    if spec.get("calendar"):
        cal = calendar or Calendar.load(spec["calendar"])
        matrix = matrix.hstack(calendar_features(matrix["tick_ns"], cal))
    features = sorted(c for c in matrix.columns if c not in NON_FEATURES)
    return matrix, features
