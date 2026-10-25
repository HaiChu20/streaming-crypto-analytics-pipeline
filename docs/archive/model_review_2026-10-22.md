# Model review: BTC/ETH signals at 5, 15 and 30 minutes

Review date: 2026-10-22. Code inspected at HEAD `3509e7e1adb90c4039db3efc3e5960a1c6b1b7a8`, including the working tree. This document carries the substantive findings, recommendations, paper references and fund discussion from the chat review into the repository. It is a research assessment, not evidence that the models are profitable.

**Final-round update:** a multi-month BTC-spot/15-minute pilot became available after this initial review. Its inspected results, the other reviewer's corrections and the reduced project direction are in [the final reply](final_reply_2026-10-22.md). That reply and internal plan notes (not published) supersede this note's initial evidence limits and broad task priorities. The initial review below is retained as the record of the earlier assessment.

For an implementing AI: read this document, then [the additional-data research](data_layers_2026-10-22.md), then internal plan notes (not published). The canonical task list lives in internal plan notes (not published); these research notes explain its rationale. Recheck code before implementing because this is a dated snapshot.

## Assessment

The engineering foundation is good: raw archives, a common Rust engine for replay and live operation, explicit availability timestamps, separate spot/perp targets, and time-ordered evaluation. The evidence for a predictive edge is still insufficient. The highest-value next work is improving the research experiment and extracting more from existing inputs, followed by controlled tests of additional information sources.

The model is already more than a limit-order-book model. It uses trade flow, returns, realized volatility, cross-venue basis, funding/positioning fields, liquidations, calendar features and Deribit DVOL where available. A column in the schema does not prove that a particular venue and historical period supplies valid values.

The checked-in [dry-run report](dry_run.md) covers roughly nine hours and an older configuration. Its zero-return baseline had lower MAE than the learned predictions in the reported comparisons. That is a plumbing result, not a reliable verdict on the current model. No current multi-month archive was trained or evaluated during this review.

The earlier review ran:

```sh
.venv/bin/python -m pytest -q tests/test_research.py tests/test_evaluation.py tests/test_risk.py tests/test_inference.py tests/test_calendar.py
```

Result: **151 passed in 24.43 seconds**. This verifies tested software behavior, not trading performance. The follow-up documentation work does not rerun training or claim a new model result.

## What exists and what matters next

| Area | Current implementation | Main implication |
|---|---|---|
| Targets | Four Binance references: BTC/ETH × spot/perp; 300/900/1800 seconds | Five-minute forecasting is already implemented. Keep target markets distinct. |
| Features | Same-tick wide matrix, own asset plus USDT context; stationary market features | ETH is not currently BTC context, and BTC is not ETH context. |
| Returns/direction | Exact endpoint midpoint return; direction is its sign | Exact-zero flat class is not a useful economic neutral zone. |
| Models | Fixed LightGBM regression and three-class classification | Need serious linear baselines, inner validation and calibrated probabilities before expensive models. |
| Evaluation | Weekly expanding folds, purging/embargo, date-based final lockbox | Already substantial; add persistent OOF rows, uncertainty and stronger experiment accounting. |
| Risk | Quantile models of adverse excursions along whole-second midpoint paths | Useful research output; neither executable stop guarantees nor subsecond risk measurement. |
| Serving | Common matrix builder; classification argmax published | Publish probabilities and enforce explicit prediction-age/version policy. |

Code entry points: [targets](../../research/targets.yaml), [matrix](../../research/dataset.py), [labels](../../research/labels.py), [training](../../research/train.py), [evaluation](../../research/evaluation.py), [risk](../../research/risk.py), [calendar](../../research/calendar.py), [serving](../../inference/service.py), [Rust features](../../engine/core/src/features.rs), [Rust windows](../../engine/core/src/windows.rs), [feature row](../../engine/core/src/row.rs).

## 1. Improve the forecast contract and targets

For each target and horizon, separate three questions:

1. What return distribution or expected return do we predict?
2. What is the probability of a meaningful upward, neutral or downward move?
3. How much adverse movement and volatility might occur before the horizon ends?

The current sign classifier treats any positive move as up and any negative move as down; flat means exactly equal endpoint prices. A new, explicitly versioned target can use a causal volatility-scaled neutral band. Its threshold must depend only on information available at the decision time, with the choice fixed on development data. Because fees are deferred, do not call this a profitable-trade threshold.

Compare raw-return squared-error regression, a robust objective such as Huber, and a volatility-normalized target. Use trailing volatility with a floor, never realized future volatility, for normalization. When rescaling a prediction, use the same decision-time scale. L1 targets a conditional median; Huber changes the estimand too. Do not label every robust-loss prediction an expected mean return without checking that interpretation.

Retain the probability vector at inference. Measure log loss, Brier score and reliability by horizon, asset and regime; fit any calibration only on earlier, held-out predictions within the training/development process. A rule that abstains on uncertain predictions should be evaluated at explicit coverage levels. Accuracy on the selected subset must be reported alongside how often the system predicts.

Regression direction and classifier argmax can disagree without a software bug: mean, median and probability of a positive return answer different questions. Preserve and explain those outputs rather than silently forcing agreement. General probability-calibration methods are supported by [Guo et al. (2017)](https://arxiv.org/abs/1706.04599); their results do not establish that a particular calibrator will work for this nonstationary financial dataset.

## 2. Make research results trustworthy

At a ten-second sample stride, 5/15/30-minute labels span approximately 30/90/180 sample intervals. Neighboring outcomes strongly overlap. Purging protects train/test boundaries; it does not make test observations independent.

Persist **every out-of-fold prediction**, including target, horizon, fold, model version, feature timestamp, availability/decision timestamp, label endpoint, realized return, predicted return, class probabilities and risk forecasts. The current code retains predictions during evaluation but primarily writes aggregate results and training provenance. Persistent OOF records enable fair paired comparisons, reliability plots, later policy experiments and reproducibility.

Estimate uncertainty with time blocks, such as day blocks and a sensitivity check using longer blocks. Use the same blocks for baseline and challenger. Report daily and fold-level distributions, volatility/event regimes, market hours, sample counts and coverage. Do not bootstrap individual ten-second rows as independent samples. Few days remain few independent market episodes even when there are many rows.

Add train-only class-prior/majority baselines, ridge return regression and logistic direction classification. The zero predictor is useful for return loss, but it predicts flat and therefore scores zero on the existing non-flat directional metric. Beating that directional score is not evidence of useful classification.

Maintain an experiment ledger: every attempted feature family, parameter search, label definition and rejected result counts. Repeatedly choosing winners on outer folds can overfit those folds. Reserve the final lockbox until the candidate and evaluation rules are frozen. [Bailey et al., The Probability of Backtest Overfitting](https://escholarship.org/uc/item/4w1110bb) motivates this discipline; its framework does not replace chronological validation.

There is a concrete lockbox guard weakness in `research/train.py`: prior use is checked against exact `(first_day, last_day)` pairs. A partially overlapping date range can bypass that comparison. Check interval intersections and record the scope of historical evaluation across model versions and output roots.

The current SHAP analysis uses the final fit and its training matrix. It describes that model's fitted behavior. It does not establish out-of-sample feature value. Prefer grouped feature ablation on identical held-out rows; use held-out SHAP as an explanation, with correlated-feature limitations made explicit.

Remove universal rules that IC must be 0.02–0.05 or accuracy 51–55%, or that a high IC automatically proves a bug. Expected values depend on target definition, market, sampling, regime and selection. Unexpectedly high performance deserves an audit, not a universal numerical verdict. Costs determine economic usefulness later.

## 3. Use existing information more effectively

Prioritize these hypotheses before new subscriptions:

- **BTC/ETH context:** causal returns, flows, volatility and basis from the other asset, with explicit asset-prefixed columns and staleness limits.
- **Lagged cross-venue effects:** distributed lags of returns, OFI and trades, rather than assuming simultaneous wide columns capture all lead–lag structure.
- **Persistence and acceleration:** short versus longer flow intensity, sign persistence, and changes in liquidity conditions. Compute disjoint-window flow from raw sums; subtracting differently normalized OFI ratios does not recover disjoint raw OFI.
- **Basis and positioning changes:** perp/spot basis changes, OI changes, mark/index gaps, funding-time interactions and liquidation bursts. Many of these fields already exist; audit actual coverage first.
- **Liquidity response:** depth replenishment after aggressive trading, resilience, cancellation/addition ratios where genuine Kraken L3 data supports them. Do not reintroduce inexact L2-minus-trades cancellation inference.
- **Relative scale:** flow relative to depth or activity, depth relative to trailing typical depth, and OI changes with explicit units. Fit statistical normalizers only on past/training data.

Correct the interpretation of cross-venue premiums. Kraken quotes USD and Binance quotes USDT, so their raw ratio mixes venue demand with the USD value of USDT. A proposed comparable premium is:

```text
premium_bps = 10000 * (kraken_mid_USD / (binance_mid_USDT * USDTUSD) - 1)
```

Keep the raw spread and stablecoin dislocation as separate features; require causal, fresh USDT/USD observations. If the conversion feed is unavailable, the adjusted premium is missing, not an assumed one-dollar conversion.

The rationale comes from [Cont, Kukanov & Stoikov](https://arxiv.org/abs/1011.6402), [Kolm, Turiel & Westray](https://onlinelibrary.wiley.com/doi/abs/10.1111/mafi.12413), [Cont, Cucuringu & Zhang](https://arxiv.org/html/2112.13213), and [Albers et al. on fragmented Bitcoin markets](https://arxiv.org/html/2108.09750). Their markets, horizons and experiments differ; they motivate tests, not expected performance promises.

## 4. Treat the horizons as different forecasting problems

Much successful LOB literature predicts very short event horizons or fractions of a second. Evidence at two price changes or 500 milliseconds cannot be transferred directly to a 30-minute forecast.

Measure a horizon curve using the same samples and a fixed diagnostic set of horizons. The working hypotheses are that five minutes benefits more from persistent order flow; fifteen minutes may benefit from basis/positioning and flow persistence; thirty minutes may depend more on volatility, events and broader market state. These are hypotheses, not established rankings. There is no requirement that measured edge decrease monotonically with horizon.

Keep separate horizon outputs. Positive five-minute return and negative thirty-minute return can describe an initial rise followed by reversal. Expected returns need not be monotone. In contrast, realized maximum adverse excursion over nested horizons is nondecreasing, making horizon consistency relevant to risk forecasts.

## 5. Improve models in a controlled sequence

Use zero/momentum and class-prior baselines, then ridge/logistic, then properly tuned LightGBM. CatBoost is a useful independent tree challenger. Only after the feature and validation framework is stable should a small MLP or a multi-resolution temporal convolutional network justify its additional complexity. A small sequence model can compare a fine recent history with coarser longer context; it need not ingest every raw book update.

Tune inside each outer training fold using chronological inner splits and early stopping. Keep preprocessing, feature selection and probability calibration inside that boundary. Predeclare a finite search budget; do not search until outer-fold results look attractive.

Compare expanding and rolling windows, with recency weighting as a separate experiment. The optimal amount of history is empirical; there is no established eighteen-month ceiling. Avoid blindly combining fold models of incompatible ages and training windows. Assess whether a small causal ensemble improves held-out stability.

Dense fixed-horizon labels often have nearly constant uniqueness weights away from boundaries. Such weights do not solve statistical dependence; inspect the actual weight distribution before expecting a benefit.

[Grinsztajn et al. (2022)](https://arxiv.org/abs/2207.08815) supports testing trees seriously on medium-size tabular data, but is not a finance theorem. [DeepLOB](https://arxiv.org/abs/1808.03668), [LOBCAST](https://arxiv.org/html/2308.01915), [TLOB](https://arxiv.org/html/2502.15757), and [LOBFrame](https://arxiv.org/html/2403.09267) offer architectures and caution about distribution shift, benchmark construction and actionability. A fashionable architecture does not repair invalid timestamps or a repeatedly used test set.

## 6. Strengthen volatility and risk forecasts

Add a separate future-realized-volatility task, with a simple persistence/EWMA baseline and a heterogeneous autoregressive-style baseline over trailing scales. [Corsi's HAR model](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1365738) motivates the multi-scale structure; adapting it to these horizons is a project experiment.

For the existing stop-distance quantiles, report pinball loss and empirical exceedance/coverage for each side and horizon. Check calibration specifically on predictions that a later signal rule selects, not only on all rows. Split by volatility and scheduled events, while showing uncertainty and counts for small groups.

Calibrators may use only matured outcomes whose entire horizon is observed. If adaptive conformal methods are tested, [Gibbs & Candès (2021)](https://arxiv.org/abs/2106.00170) concerns adaptation under distribution shift; long-run coverage does not promise conditional coverage during crashes. Recheck calibration after enforcing quantile or horizon ordering.

The label path is sampled at whole seconds. Subsecond excursions may be missed. Midpoints are not executable bid/ask prices, and a predicted quantile is not a guaranteed stop fill. These limitations should appear in output documentation.

## 7. Resolve replay and live timing assumptions

The offline wrapper currently defaults through replay to `ideal` timing. The replay documentation describes idealized availability at nominal tick plus 250 milliseconds. That is an assumption, not measured delivery latency. Compare candidate results under `as_live` replay and record the mode in every artifact.

Independent daily replays start with fresh engine state. Evaluate midnight cold starts against a continuous replay and an explicit pre-roll or checkpoint method. Preserve independently reproducible days by declaring and hashing each day's initialization inputs, rather than adding hidden dependence on the previous process.

Labels currently start from the nominal tick midpoint, while inputs may arrive later. For a decision made later than that tick, report the resulting timing difference and eventually evaluate a versioned decision-time entry target. Reject inputs or predictions that have exceeded an explicit age limit or already passed their label endpoint. Define strict engine/schema/calendar compatibility instead of merely warning on incompatible versions.

Historical event calendars also need a knowledge timestamp. A release date corrected after a postponement was not necessarily known weeks earlier. Current DVOL backfill uses a synthetic receive lag; retain that assumption explicitly and distinguish it from observed live receive timestamps.

## Reading guide and limits of transfer

These are primary research references, with narrow takeaways. Some were accessible as full HTML, others only through author abstracts or publisher abstracts; this review is not a replication or a claim to have read every paywalled full text.

| Paper | Relevant lesson | Limitation for this project |
|---|---|---|
| [Cont, Kukanov & Stoikov, price impact](https://arxiv.org/abs/1011.6402) | Order-flow imbalance and depth explain short-run price changes. | Contemporaneous impact is not automatically a tradable future forecast. |
| [Kolm, Turiel & Westray, deep order flow](https://onlinelibrary.wiley.com/doi/abs/10.1111/mafi.12413) | Stationary flow representations and multi-horizon modeling are useful. | Equity results; effective horizon around a few price changes. |
| [Cont, Cucuringu & Zhang, cross-impact](https://arxiv.org/html/2112.13213) | Test lagged other-asset flow and its decay. | Equity setting; reported portfolio results do not establish net crypto profitability. |
| [Albers et al., fragmented Bitcoin markets](https://arxiv.org/html/2108.09750) | Cross-venue information and comparatively simple models matter. | Very short forecast horizons. |
| [DeepLOB](https://arxiv.org/abs/1808.03668) | CNN/LSTM architecture for book sequences. | Equity benchmark and short-horizon labels. |
| [LOBCAST](https://arxiv.org/html/2308.01915) | Compare architecture generalization on new data. | A benchmark warning, not proof that neural models never work. |
| [TLOB](https://arxiv.org/html/2502.15757) | Attention architecture, meaningful simple baselines, sensitivity to spread-aware labels. | Short BTC sample and horizon/domain differences. |
| [LOBFrame](https://arxiv.org/html/2403.09267) | Forecast accuracy and usable transactions differ. | Its experimental setting differs from ours. |
| [Grinsztajn et al., tabular benchmarks](https://arxiv.org/abs/2207.08815) | Tree ensembles deserve a strong baseline role. | Generic datasets, not minute-level crypto proof. |
| [Bailey et al., backtest overfitting](https://escholarship.org/uc/item/4w1110bb) | Account for selection among many experiments. | Must adapt to the project's temporal dependence. |
| [Gârleanu & Pedersen, dynamic trading](https://www.nber.org/papers/w15205) | Signal decay and trading frictions affect desired positions. | Relevant to the later portfolio stage, currently deferred. |
| [Jensen, Kelly, Malamud & Pedersen, implementable frontier](https://academic.oup.com/rfs/article/39/10/3035/8524346) | Evaluate prediction jointly with implementability. | Does not provide a ready-made intraday crypto execution model. |

Additional calibration, volatility and market-making references are linked in the relevant sections. The companion [data-layer guide](data_layers_2026-10-22.md) covers sentiment, news, macro events, options and on-chain research and APIs.

## What can be learned from Jane Street and systematic funds

Public evidence supports a broad research process, not an exact reconstruction of any firm's proprietary trading model.

[Jane Street's machine-learning page](https://www.janestreet.com/join-jane-street/machine-learning/) describes neural models, substantial compute and problems involving nonstationarity and market impact. Its [Signals and Threads interview, Finding Signal in the Noise](https://signalsandthreads.com/finding-signal-in-the-noise/) discusses research iteration, reproducibility and evolving modeling tools. These sources do not disclose the production features, architecture, target definitions, datasets or performance of a particular current strategy.

[Two Sigma's investment-management description](https://www.twosigma.com/businesses/investment-management/) separates data, modeling, portfolio construction and execution. A useful project design inferred from this public process is:

```text
Point-in-time market and external data
                 ↓
Return probabilities + return magnitude + risk forecasts
                 ↓
Desired position, subject to risk and inventory constraints
                 ↓
Execution decisions and observed fills
                 ↓
Monitoring, attribution and controlled model updates
```

The inference is that the edge is a complete, carefully measured system, not one algorithm. Different firms and desks operate at different horizons. Market making adds inventory risk, adverse selection and fill probability to price prediction. [Avellaneda & Stoikov (2008)](https://www.tandfonline.com/doi/abs/10.1080/14697680701381228) is a useful mathematical introduction to inventory-sensitive quotes; it is not a description of Jane Street's present implementation.

The transferable practices are accurate timestamps, good baselines, many hypotheses evaluated carefully, reproducibility, stable deployment and measurement after selection. Fees, spread, slippage, funding and latency remain deferred by the user's current scope, but they must enter before claiming an economically deployable trading strategy.

## Recommended order

First persist OOF predictions, strengthen lockbox accounting and add credible baselines. Then tune trees and test additional structure in existing features. Next compare targets, training windows, probability and risk calibration. Add external sources individually with point-in-time evidence and identical evaluation rows. Freeze a candidate for untouched testing and a later shadow period before investing heavily in neural architectures. The concrete dependencies and acceptance criteria are in internal plan notes (not published).
