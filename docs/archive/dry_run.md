# P7 real-data dry run

Run: 2026-10-17; 10-second stride, 3 expanding folds, 100 boosting rounds, two LightGBM threads.

**Roughly nine hours of valid data is far too little for meaningful results. Meaningful evaluation requires several weeks. This run verifies the offline pipeline and leakage guards only.**

Input: `<scratch>/features/date=2026-10-17/features.parquet` (read-only).

Input rows: 1,112,572, 14 instrument/cross streams. The file spans about 22 hours, including the invalid trailing period; about 9 hours have valid reference books. Replay mode: `ideal`; engine: `0.1.0-ideal-v1`.

Pinned via `uv add`, retaining exclude-newer: polars 1.34.0, pyarrow 21.0.0, lightgbm 4.6.0. numpy/scipy are transitive LightGBM dependencies; no pandas or scikit-learn.

`PYTHON_DOTENV_DISABLED=1 uv run pytest -q`: **144 passed in 7.09s**, including 25 research cases. Dry run measured with `/usr/bin/time -l`: **24.63 s elapsed, 28.16 s user, 8.11 s system, 1,074,577,408 bytes peak RSS (~1.00 GiB)**, alongside pytest.

Label coverage below is conditional on a valid, non-warm reference row at the 10-second stride, rather than all ticks in the file. Invalid/missing future endpoints are excluded. No late-prediction rows were excluded on this input.

| Target | Horizon (s) | Matrix rows | Labeled rows | Coverage | Features | Train/test rows by fold | Minimum gap (s) |
|---|---:|---:|---:|---:|---:|---|---:|
| BTC_spot | 900 | 3110 | 3014 | 96.91% | 197 | 663/754, 1417/753, 2170/754 | 910 |
| BTC_spot | 1800 | 3110 | 2924 | 94.02% | 197 | 551/731, 1282/731, 2013/731 | 1810 |
| BTC_perp | 900 | 3102 | 3000 | 96.71% | 197 | 660/750, 1410/750, 2160/750 | 910 |
| BTC_perp | 1800 | 3102 | 2908 | 93.75% | 197 | 547/727, 1274/727, 2001/727 | 1810 |
| ETH_spot | 900 | 3110 | 3014 | 96.91% | 197 | 663/754, 1417/753, 2170/754 | 910 |
| ETH_spot | 1800 | 3110 | 2924 | 94.02% | 197 | 551/731, 1282/731, 2013/731 | 1810 |
| ETH_perp | 900 | 3101 | 2998 | 96.68% | 197 | 659/750, 1409/749, 2158/750 | 910 |
| ETH_perp | 1800 | 3101 | 2906 | 93.71% | 197 | 546/727, 1273/726, 1999/727 | 1810 |

Held-out metrics concatenate the three test folds. MAE/RMSE are percentage points; directional accuracy excludes actual flats. Momentum uses the trailing 900-second return scaled by h/900. Its slightly smaller n is due to invalid past endpoints. The zero baseline predicts flat (0% directional accuracy on non-flat cases); its IC is undefined. Classifier return metrics use probability-weighted training-fold class mean returns; classifier direction uses argmax.

| Target | Horizon | Model | n | MAE | RMSE | Direction accuracy | Spearman IC |
|---|---:|---|---:|---:|---:|---:|---:|
| BTC_spot | 900 | regressor | 2261 | 0.060433 | 0.076840 | 52.28% | 0.0339 |
| BTC_spot | 900 | classifier | 2261 | 0.066443 | 0.082445 | 51.92% | 0.0153 |
| BTC_spot | 900 | zero | 2261 | 0.041841 | 0.055090 | 0.00% | null |
| BTC_spot | 900 | momentum | 2255 | 0.071905 | 0.091143 | 36.36% | -0.4097 |
| BTC_spot | 1800 | regressor | 2193 | 0.096061 | 0.110714 | 36.94% | -0.0457 |
| BTC_spot | 1800 | classifier | 2193 | 0.101514 | 0.114372 | 32.83% | -0.1518 |
| BTC_spot | 1800 | zero | 2193 | 0.051224 | 0.061152 | 0.00% | null |
| BTC_spot | 1800 | momentum | 2187 | 0.107769 | 0.135701 | 36.95% | -0.3082 |
| BTC_perp | 900 | regressor | 2250 | 0.059719 | 0.075851 | 53.89% | 0.0604 |
| BTC_perp | 900 | classifier | 2250 | 0.067617 | 0.083569 | 52.34% | 0.0287 |
| BTC_perp | 900 | zero | 2250 | 0.041187 | 0.054892 | 0.00% | null |
| BTC_perp | 900 | momentum | 2238 | 0.070852 | 0.091032 | 35.39% | -0.4171 |
| BTC_perp | 1800 | regressor | 2181 | 0.095906 | 0.110616 | 38.56% | -0.0338 |
| BTC_perp | 1800 | classifier | 2181 | 0.097103 | 0.109823 | 36.82% | -0.0975 |
| BTC_perp | 1800 | zero | 2181 | 0.050128 | 0.060348 | 0.00% | null |
| BTC_perp | 1800 | momentum | 2169 | 0.105365 | 0.133853 | 35.96% | -0.3011 |
| ETH_spot | 900 | regressor | 2261 | 0.080830 | 0.103631 | 51.75% | 0.1338 |
| ETH_spot | 900 | classifier | 2261 | 0.069132 | 0.089260 | 55.78% | 0.3073 |
| ETH_spot | 900 | zero | 2261 | 0.058384 | 0.077136 | 0.00% | null |
| ETH_spot | 900 | momentum | 2255 | 0.100168 | 0.128287 | 35.51% | -0.4281 |
| ETH_spot | 1800 | regressor | 2193 | 0.091144 | 0.108762 | 47.89% | 0.0994 |
| ETH_spot | 1800 | classifier | 2193 | 0.095197 | 0.113760 | 52.70% | 0.1278 |
| ETH_spot | 1800 | zero | 2193 | 0.066844 | 0.082920 | 0.00% | null |
| ETH_spot | 1800 | momentum | 2187 | 0.154026 | 0.195464 | 31.86% | -0.5005 |
| ETH_perp | 900 | regressor | 2249 | 0.094329 | 0.115657 | 51.98% | 0.0890 |
| ETH_perp | 900 | classifier | 2249 | 0.072806 | 0.094411 | 54.75% | 0.2049 |
| ETH_perp | 900 | zero | 2249 | 0.057807 | 0.076690 | 0.00% | null |
| ETH_perp | 900 | momentum | 2236 | 0.098991 | 0.127426 | 35.61% | -0.4270 |
| ETH_perp | 1800 | regressor | 2180 | 0.091426 | 0.110105 | 47.49% | 0.1052 |
| ETH_perp | 1800 | classifier | 2180 | 0.087947 | 0.105492 | 51.22% | 0.1991 |
| ETH_perp | 1800 | zero | 2180 | 0.066423 | 0.082479 | 0.00% | null |
| ETH_perp | 1800 | momentum | 2167 | 0.151892 | 0.192905 | 32.69% | -0.4828 |

The zero-return baseline has lower MAE than both learned models in all eight combinations on this sample. No claim about predictive usefulness follows from these few overlapping hours.

Full JSON (includes per-fold metrics, windows, purge evidence, feature lists and metadata): `<scratch>/p7/research_report_dry-run-final.json`.

Final models, metadata and per-training-row provenance: `<scratch>/p7/models/<target>/<900|1800>/dry-run-final`.

All 24 folds passed the recorded endpoint, label-emission and feature-emission checks against test start. Model save/load is covered by the synthetic end-to-end test. Upload was tested with a stub client only; no AWS call was made. No live-pipeline/Docker/Kafka operation or git write was performed.
