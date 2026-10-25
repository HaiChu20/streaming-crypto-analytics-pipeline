# Response to the 2026-10-22 model review

Reply to [model_review_2026-10-22.md](model_review_2026-10-22.md), [data_layers_2026-10-22.md](data_layers_2026-10-22.md) and internal plan notes (not published). Written by the project's lead reviewer after checking the review's code claims at `3509e7e`.

## Verdict

The review is careful and mostly correct. Its core message is right: **measure better, then use existing data better, then add outside data.** Every code finding I spot-checked holds up (table below).

Three things need changing before it is used as the task list:
- **Task numbers clash:** P25–P27 already name finished work.
- **Missing practical blockers:** training memory, a reversal baseline and model regularization.
- **Too heavy for this project:** the scope is a personal showcase with a ~4-month archive, so the 16 tasks need trimming to an order that fits it.

## New since the review: first real multi-month result

The review says no multi-month archive had been trained. That is no longer true.

**Data and replay (CSC Roihu, engine `0.1.0-features-v6`):**
- 120 days replayed: 2026-06-23 → 10-20, Binance + Kraken, spot + perp, BTC + ETH, plus Deribit DVOL.
- **Checks:** 0 Kraken CRC failures; 0 data anomalies.
- **Flagged days:** 2 — 07-02 (recorder network trouble, Binance 92% valid) and 09-11 (no Binance recording).

**Date split** (path-based, last 20% as lockbox):

| Part | Dates | Days |
|---|---|---|
| Development | 06-23 → 09-25 | 95 |
| Embargo | 09-26 | 1 |
| Lockbox | 09-27 → 10-20 | 24 (unopened) |

**Pilot: BTC_spot, 15-minute horizon, weekly walk-forward**
- Out-of-fold predictions: 686,703.
- Training matrix: 801,178 rows × 1,444 features.

| Predictor | MAE (pp) | RMSE | Direction | Spearman IC |
|---|---|---|---|---|
| zero | **0.1188** | **0.1833** | — | — |
| momentum (trailing 900 s) | 0.1736 | 0.2596 | 48.4% | −0.035 |
| LightGBM regressor | 0.1282 | 0.1953 | 51.1% | 0.016 |
| LightGBM classifier | 0.1207 | 0.1851 | **52.0%** | **0.042** |

**How to read it:**
- **Signal size:** about 7,700 independent 15-minute windows means a direction standard error near 0.6 pp. The classifier's edge is therefore plausible but small.
- **Mean reversion:** momentum is *negative*, so a "bet on reversal" rule would score about 51.6%. The classifier beats that by only about 0.4 pp.
- **Magnitude:** the regressor loses to zero on MAE and RMSE.

**Cost and pending runs:**
- **Resources:** 163 GB peak memory, 25 min, 30 BU.
- **Still running:** full runs `full-v1-btc` and `full-v1-eth` (6 target/horizons each, `--shuffle-test`).

**Conclusions should wait for the full runs.**

## Code findings: verified

| Review claim | Check | Result |
|---|---|---|
| Lockbox guard compares exact `(first_day, last_day)` pairs only | `research/train.py` ~line 382 | **Confirmed.** Partial overlap bypasses it, and the ledger lives in the output root, so a new output dir starts a fresh ledger |
| Offline replay defaults to `ideal` timing (`available_ns = T + 250 ms`, assumed) | `engine/replay/src/main.rs`, replay README | **Confirmed.** `train_offline.sh` does not pass `--mode` |
| Only USDT context; no BTC↔ETH context | `research/targets.yaml` | **Confirmed.** The Allas archive also has **no USDT/USD pair**, so USDT context is empty there today |
| Importance/SHAP describes the final fit, not held-out value | `research/evaluation.py` | **Confirmed** |
| Daily replays start cold | engine warm-up 300 s; windows up to 3,600 s | **Confirmed.** Long-window features are null for the first hour of every day (~4% of rows) |
| Classifier publishes argmax, not probabilities | `inference/service.py` ~line 280 | **Confirmed.** Probabilities are computed, then reduced to `direction` |

## Corrections and additions

1. **Rename the tasks.**
   - `P25` (symlinked archives, `379e768`), `P26` (features-v6, `d102266`) and `P27` (`snapshot_error` markers, `3509e7e`) are already used in commits and internal progress notes (not published).
   - Rename the review's tasks to a separate track, e.g. **M1–M16** (M1 = review P25 … M16 = review P40), so history and plan agree.
2. **Training memory is a practical blocker the review misses.**
   - **Cause:** `research/dataset.read_features` loads every column of every one-second row for the asset. The pilot needed 163 GB for one asset; BTC + ETH in one job would need over 300 GB.
   - **Fix:** labels and momentum need one-second rows only for the *reference* instrument, and only a few columns. All other rows are needed only at the 10-second stride.
   - **Expected effect:** memory should drop several-fold, with byte-identical matrices (testable on fixtures).
   - **Priority:** this directly sets BU cost on Roihu, so it belongs early.
3. **Add a reversal baseline next to momentum:** sign-flipped trailing return, h/900 scaled. It is the honest bar the model has to clear.
4. **Model capacity vs sample size.**
   - **Current settings:** `num_leaves=15`, `min_data_in_leaf=20`, 100 rounds at lr 0.05, no early stopping, 1,444 features.
   - **Problem:** that is weak regularization for ~7,700 effectively independent outcomes, and the regressor losing to zero fits that picture.
   - **Do first:** M3 (inner-fold early stopping and a small fixed search, larger `min_data_in_leaf`, feature/bagging fractions). This comes before adding any new features.
5. **History length is the biggest limit.**
   - **The issue:** four months is one market regime, which limits what any feature or target study can prove.
   - **Recording:** keep recording.
   - **Probe:** whether Binance's public archive (trades, aggTrades, futures metrics/OI) can extend *some* features further back. That would give a reduced-feature model a longer history, without pretending L2 depth exists where it doesn't.
6. **Cold start.** Before building checkpoint machinery, test a simple pre-roll: replay the previous day's last hour without emitting rows. This is compatible with the batch job if the previous day's files are downloaded too.
7. **Scope.** The full provenance design (hash manifests for every input, vintage schemas, a per-source admission manifest) is right for a fund. For this project, keep the parts that protect conclusions: OOF files, the experiment ledger, a split manifest and the lockbox ledger. Defer the rest until an outside source is actually admitted.

## Recommended order

Each step keeps the date split and lockbox fixed. Small steps, and one change at a time where a result is attributed.

| Step | Work (review task) | Done when |
|---|---|---|
| 1 | Persist OOF rows; day-block bootstrap intervals; reversal + class-prior baselines; lockbox interval-overlap guard with a configurable ledger path (M1 parts, M2) | Aggregates reproduce from saved OOF; exact, partial and contained lockbox reuse are rejected |
| 2 | Lean loader (addition 2) | Identical matrices on fixtures; peak memory measured on Roihu |
| 3 | Ridge/logistic controls; regularization + inner early stopping, ≤24 configs per target/horizon (M3) | Tuned trees vs controls on identical OOF rows, with intervals |
| 4 | BTC↔ETH context and small lag sets; then a volatility-scaled neutral-band target (M4, M5) | Each family kept or dropped by paired comparison |
| 5 | `ideal` vs `as_live` on a slice; pre-roll test (M6) | Timing gap and cold-start effect quantified |
| Later | Probabilities in serving (M14); GDELT attention pilot (M9); options/social/on-chain (M10–M13); neural challenger and trading costs/backtest (M16) | Only after steps 1–4 show a stable edge |
| Last | Freeze the candidate and rules, then open the lockbox **once** (M15) | Final report with uncertainty; a negative result is acceptable |

## On the data-layer guide

I agree with its ranking: existing data first, free sources before paid, and event risk before sentiment direction. Two practical notes:

- **Outside sources mostly start from today.** Most of them have no trustworthy point-in-time history for June–October 2026. They must be collected forward, so they cannot be tested on the current archive for months.
- **Recording first, if any is added now.** The cheap next step would be *recording* (e.g. USDT/USD on Kraken and Binance, Binance OI), so that history exists when the model work is ready for it.
