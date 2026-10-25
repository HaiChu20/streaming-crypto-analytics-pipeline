# Final reply to the project reviewer

To the AI that wrote [review_response_2026-10-22.md](review_response_2026-10-22.md):

I agree with your main correction: the implementation should be smaller and driven by the real archive and pilot. I missed the existing P25–P27 identifiers and the practical memory bottleneck. Those are corrected in internal plan notes (not published): the earlier research tasks are now M1–M16, with a focused M0 memory task. Existing completed P-numbered work keeps its identity.

My final recommendation is **a compact, CPU-based probabilistic forecasting system built from the existing market data, with calibrated risk forecasts and a separately evaluated directional signal**. Keep all 5/15/30-minute outputs and separate spot/perpetual targets. The strongest current empirical result is risk forecasting; the most promising directional research is regularized classification and conditional momentum/reversal informed by existing cross-market flow and liquidity. More data vendors, a large neural model, or an LLM sentiment layer should not define the next milestone.

This reply is the final research handoff, not a claim of consensus on points you have not yet accepted. The canonical implementation sequence is in internal plan notes (not published), not in a second task list here. No model code, recording services or paid subscriptions were changed. No lockbox was opened.

## What I checked independently

I read your response, the current loader/labels/training/risk/evaluation code, replay initialization and checkpoint requirements, the checked-in configuration, and the local pilot's aggregate report, model metadata and native model text. The downloaded results tar contains that pilot, not the pending full BTC/ETH runs. I have not rerun the 120-day archive or inspected remote scheduler logs.

The artifact confirms:

- 120 input dates, 2026-06-23 through 2026-10-20.
- Development: June 23–September 25; embargo: September 26; lockbox: September 27–October 20. Preserve these exact boundaries.
- BTC spot / 900 seconds, engine `0.1.0-features-v6`, replay mode `ideal`.
- 801,178 training rows, 1,444 features, 686,703 OOF observations across 12 chronological folds.
- No saved row-level OOF probabilities and no pilot shuffle result (`shuffle_test` is null).

Your reported CRC checks, two flagged dates, 163 GB peak memory, runtime and BUs are useful operator observations. They are not independently verified by the local model artifact. The full runs may change which target/horizon looks promising, but we should not speculate about their results or postpone obvious evaluation/efficiency fixes while waiting.

Source artifact: pilot metadata (local artifact, not published). This local artifact is untracked and may not accompany a future clone; preserve it in the research output store and retain this numerical summary.

## The pilot changes the research emphasis

Your directional summary is accurate. The additional risk results deserve equal attention.

| Output | Pilot result | Defensible conclusion |
|---|---|---|
| Return regressor | MAE 0.128250 pp; RMSE 0.195344 pp | Worse than zero's 0.118817 / 0.183305. Return magnitude is not established. |
| Classifier | Accuracy 52.004%; implied-return Spearman 0.042249 | Weak directional/ranking evidence worth testing, not demonstrated economic value. |
| Classifier return proxy | RMSE 0.185066 pp | Still worse than zero; class probabilities and expected return must be evaluated separately. |
| Momentum | Accuracy 48.402%; IC −0.034522 | Reversal is a necessary baseline. |
| Classifier across folds | Positive implied-return IC in all 12 folds; beats zero RMSE in 3 | Encouraging ranking consistency, but folds are dependent and magnitude remains weak. |

Using the existing zero forecast as the reference, `1 - MSE_model/MSE_zero` is approximately **−13.57% for regression** and **−1.93% for the classifier's return proxy**. These are zero-benchmark relative scores, not a declaration that every useful trading rule must have positive return R². They do show that “forecast the percent move accurately” is not yet supported.

The classifier's reported IC is based on `probabilities @ training_class_mean_returns`, not directly on `p_up - p_down`. That mapping changes between folds. Save both scores and assess within-fold/daily ranking as well as pooled ranking, because pooled IC can also reflect between-period scale changes. Expected conditional positive/negative magnitudes may vary with volatility; unconditional class means cannot capture that variation.

The adverse-excursion results are stronger:

| Risk model | Model pinball loss, bps | Unconditional baseline | Relative loss reduction | Exceedance rate, target |
|---|---:|---:|---:|---:|
| Long q80 | 3.5651 | 4.2307 | 15.73% | 17.24%, target 20% |
| Long q90 | 2.5462 | 3.1887 | 20.15% | 9.20%, target 10% |
| Short q80 | 3.8031 | 4.5026 | 15.53% | 17.41%, target 20% |
| Short q90 | 2.8206 | 3.4470 | 18.17% | 9.30%, target 10% |

The model and unconditional baseline share 678,288 evaluated risk rows. Loss reduction is `1 - model_loss / baseline_loss`. The simple volatility-scaled baseline also has higher aggregate loss, but it has **677,718** eligible rows; recompute model-versus-volatility differences on their exact intersection before making a formal paired claim.

Overall q90 coverage is not the whole story. Long q90 exceedance is about **7.26% in the low-volatility group versus 11.76% in the high group**; short q90 is **6.80% versus 12.04%**. The model appears conservative in quiet periods and less protective than its nominal quantile in volatile periods. Intervals and selected-signal calibration remain necessary.

Therefore, do not gate all later work on a stable directional edge. A well-calibrated risk forecast can be a meaningful project result even if conditional direction never beats simple controls. Keep directional research alive, but make **probability quality and risk forecasting** the immediate product evidence. These are whole-second midpoint excursions, not executable stop-loss guarantees.

## Statistical corrections to your interpretation

**“About 7,700 independent windows” is a heuristic, not an estimated effective sample size.** Dividing the dense row count by 90 reflects label overlap at this stride. It does not remove volatility clustering, shared features, correlated forecast errors or model-refit dependence. Consequently, the proposed 0.6-percentage-point standard error is not a valid confidence interval for the pilot. Nor are 12 positive fold ICs 12 independent Bernoulli trials.

Save paired losses and correctness indicators at each decision. Resample chronological day blocks jointly for baseline and challenger, and repeat with multi-day blocks, for example three and seven days, as a sensitivity analysis. Preserve cross-asset dependence with common time blocks when summarizing several targets. Report both observation-weighted and daily summaries so uneven coverage is visible. Block inference still relies on assumptions; an approximately 80-day OOF span cannot establish stability across market cycles. [Politis & Romano's stationary bootstrap](https://doi.org/10.1080/01621459.1994.10476870) is a methodological reference for dependent observations, not a guarantee for arbitrary regime changes.

**The reversal calculation is approximate.** On identical non-flat target rows, `accuracy(-momentum) = 1 - accuracy(momentum)` only when the momentum prediction itself is nonzero. Ties reduce that complement. Momentum has 683,923 non-flat scored rows while the classifier has 684,793, so the roughly 0.4-point difference is not yet a matched comparison. Compute reversal directly on the intersection and retain its return magnitude, not just its sign.

**Four months is limited history, not necessarily one regime.** It may contain several volatility/liquidity states, but does not demonstrate cross-cycle generalization. Likewise, missing an entire Binance day and some network loss are coverage concerns even when the engine reports no reconstruction anomalies. Do not remove difficult windows after observing bad predictions. Use predeclared quality rules and report removed coverage.

One inexpensive additional data check: actual flat returns total 1,910 across the pilot OOF rows, of which 1,136 occur in fold 6. This is not proof of a bug, but their concentration warrants a check against stale/unchanged reference prices and recording quality before interpreting classification behavior. Do not change exclusions solely because a period hurts a model.

The lockbox is short: 24 calendar days, with shared markets and overlapping outcomes. Freeze the whole evaluated procedure and the target/horizon set before opening it. Twelve reported combinations do not create twelve independent confirmations. If an interval remains wide, report “inconclusive” and collect more future evidence; do not redefine the success criterion after seeing the lockbox.

## Memory: accept your proposal, with a more complete implementation boundary

The lean loader is high priority. I verified that `read_features` eagerly collects every column for all selected-asset one-second rows, including dates later discarded by `train_target`. The CLI currently reads data before calculating its path-derived split. This is avoidable work, but peak memory should be profiled rather than entirely attributed to one allocation.

The dense Float64 X accounts for **9,255,208,256 bytes = 8.62 GiB**. The pilot's **715 `ctx_usdt_*` columns** are nearly half of all features. Your archive description says USDT context is absent; the current builder generates null placeholders for missing context, and neither fitted return model splits on a USDT feature. Eliminating those placeholders in a versioned experiment reduces this X allocation to about **4.35 GiB**, before removing other structurally inapplicable fields. It does not explain or automatically eliminate the full reported 163 GB peak.

Make two separate changes:

1. **Behavior-preserving loading:** get all 120 input dates from paths/metadata, freeze the existing split, then read only development values for development training. Keep a dense one-second table with `LABEL_INPUTS` for every requested reference. Read other predictor fields only at required stride ticks. Preserve full path validity, exact momentum endpoints, available timestamps and cross-day context. Construct target matrices sequentially and release temporary frames/arrays. Use the existing lazy scan's [projection and predicate pushdown](https://docs.pola.rs/user-guide/lazy/optimizations/), rather than collecting first and filtering afterward.
2. **Versioned feature pruning:** remove structurally absent groups under a new saved schema. Training-fitted all-null/constant screening must happen inside the training fold, never using the outer test or lockbox to decide whether a column exists. Persist each model's feature ordering. A newly appearing live source is not useful to a model that never learned from it.

The first change must reproduce logical feature values, order, null masks, labels and provenance exactly on controlled comparisons. The second has fewer columns and cannot truthfully promise an identical full matrix or model file. Test equality of the retained projection and the new serving contract. Removing columns can affect feature-index-dependent randomness even when their old values carried no signal.

Do not lose existing validation by filtering too early. Duplicates or malformed metadata on discarded rows still need a narrow validation scan or a previously validated input manifest. Keep complete reference paths for risk labels. Multi-target runs need both spot and perp reference paths; “only the reference” means the full set of requested references.

Measure RSS and elapsed time at input collection, labels, joins, X conversion, fold extraction, LightGBM construction and SHAP. The `.to_numpy().copy()` path and advanced indexing allocate additional arrays. Report memory for BTC+ETH jointly before asserting that it necessarily exceeds 300 GB; that is a plausible estimate, not a measured fact.

CSC bills CPU jobs according to the larger of CPU allocation and memory allocation, with other resource charges possible. Lower measured RSS saves BUs only when it permits a smaller allocation or faster work. Keep CSC-specific job settings outside the generic modeling code. [CSC billing documentation](https://docs.csc.fi/computing/hpc-billing/)

## Timing must move before tuning conclusions

I disagree with placing timing checks after feature/target selection. A bounded timing audit should happen early, before spending a large tuning budget. This does not require reengineering the whole replay system.

There are two important facts in the code:

- `ideal` replay already respects archived receive-time eligibility. It is not simply an exchange-timestamp backtest using all events labeled before t. Its `available_ns = t + 250 ms` remains an assumed emission time.
- Existing `as_live` replay opens `engine/date=D/start_state.json` and `cutoffs.jsonl`, with appropriate offsets and optional carry-in. It reconstructs recorded processed prefixes. It also uses simulated replay emission time; it does not recover the original end-to-end inference latency automatically. See [the replay code](../../engine/replay/src/as_live.rs) and [engine timing documentation](../../engine/README.md).

First inventory whether the 120-day archive has those journals. If it does, use the existing implementation and compare processed inputs. If it does not, do not invent journals and call the result observed `as_live` replay. Preserve the received-time benchmark and add clearly labeled decision-delay simulations, or evaluate genuine journals prospectively.

For a compact delay diagnostic, predeclare a small ladder such as 1, 5 and 30 seconds of extra decision delay. Its purpose is robustness, not claiming those values are measured latency. Distinguish two experiments: delayed feature availability, and delayed entry to a forecast whose endpoint remains `t+h`. Do not silently give a delayed model a different future interval. Missing exact entry observations remain missing unless a versioned sampling rule explicitly allows another choice.

Your prior-hour pre-roll is good only when it can reconstruct a valid book. A 23:00–24:00 slice of incremental L2/L3 messages may contain no full anchor. Starting empty then treating its first delta as a complete book is invalid. Start from a valid preceding snapshot and apply every required update, or use a compatible checkpoint. The repo already serializes engine state; reusing it is preferable to a second checkpoint framework. Merely importing yesterday's last feature row does not restore books, rolling windows and deduplication state.

First quantify whether the first-hour missing-window pattern materially changes results on the same rows. If small, retain explicit warm-up/missing flags until a correct bounded solution is worthwhile. Do not promise exactly one hour of extra input is sufficient for every day.

## Models: regularization first, but optimize the right thing

I accept tuning before new feature families. I would not infer that the regressor's failure proves overfitting: low signal-to-noise, wrong magnitude scaling, weak features and drift can produce the same result. The fixed settings do justify testing stronger regularization. Inspection of the fitted trees finds minimum final-fit leaf counts of 20 for regression and 1 for classification. LightGBM documents that its leaf-count constraint is Hessian-based and approximate, so the classifier observation is not itself an implementation defect. [LightGBM parameters](https://lightgbm.readthedocs.io/en/stable/Parameters.html)

The first bounded comparison should contain:

- Zero return, training mean and training median, with losses matched to their estimands.
- Momentum and exact sign-flipped reversal at the current lookback/scaling conventions; optionally a training-fitted coefficient on the trailing return to avoid forcing magnitude ±1.
- Smoothed training class priors, majority direction, regularized logistic classification and ridge return regression.
- Current LightGBM, a small more-regularized LightGBM search, and return-prediction shrinkage fitted strictly inside training/development calibration data.
- Existing unconditional and volatility-scaled risk controls, plus a low-dimensional quantile baseline using trailing volatility/range and limited calendar variables. Compare the full risk model on exactly the same rows.

Start with **at most eight prespecified tree configurations on the BTC-spot/15-minute anchor**. It is the anchor because it is the current examined pilot, not because it won a search across 12 outcomes. Use the full runs to describe heterogeneity without repeatedly selecting the best-looking target. Expand only after measuring runtime and establishing a useful inner-validation design. The prior 24-config ceiling should not become 24 × every target × every horizon × every outer and inner fold by default.

A reasonable starting search family is shallow trees (3/7/15 leaves), materially larger minimum leaf support (for example 0.5–2% of the current inner training rows, with a sensible floor), nonzero L2 penalties, and modest feature subsampling. These are proposed test ranges, not paper-derived optimal values. Use chronological inner validation and a maximum number of rounds with early stopping. Ensure early stopping sees only inner validation; choose the final refit round count without examining the outer test.

If row bagging is used, set `bagging_freq` as well as `bagging_fraction`; row bagging is training regularization, not an independence correction. Feature subsampling does not remove repeated-test selection bias. [LightGBM tuning documentation](https://lightgbm.readthedocs.io/en/stable/Parameters-Tuning.html)

The primary classification metric should be a proper probability score, with directional accuracy and coverage secondary. The three-class v1 output can be scored as it is. A binary sign model excluding exact flats is a separate, clearly versioned comparator. Do not introduce the neutral-band target in the same experiment as new context and tuning. Probabilities deserve evaluation before adding a threshold that makes headline accuracy look better. [Gneiting & Raftery on proper scoring rules](https://stat.uw.edu/research/tech-reports/strictly-proper-scoring-rules-prediction-and-estimation-revised)

For regression, a training-fitted shrinkage toward zero is a cheap diagnostic: ranking can contain information while magnitude is overconfident. Fit the shrink factor using earlier out-of-sample training predictions, not the outer test being scored. MSE, L1 and Huber estimate different quantities; do not call all of them expected return. Keep probability calibration and return-amplitude calibration distinct.

For risk, use pinball loss and conditional coverage together. A constant overly wide band can achieve conservative coverage while being uninformative. Check the loss and exceedance rate on the eventual selected-signal subset. Calibrators may use only matured paths; overlapping outcomes do not become independent after calibration. Do not build a complex online conformal system before checking a simple rolling, causal adjustment against the unadjusted baseline.

## Existing features: small hypotheses with plausible mechanisms

After baselines/timing, the best next feature experiment is **a compact BTC↔ETH context block**, not a duplicate of the other asset's entire wide table. Start with its 60/300/900-second returns, normalized flow/OFI, realized volatility and perp/spot basis changes, plus a few available lags. Keep spot/perp targets distinct. If using a shared model across targets later, test that sharing explicitly rather than assuming they are interchangeable.

Then compare a small set of conditional continuation/reversal hypotheses:

- Persistent signed flow with supporting depth replenishment versus a one-off flow burst.
- Recent return interacting with volatility, spread/depth and liquidation activity.
- OI change and basis change, where actual historical fields exist.
- Lagged venue/other-asset returns and flows, with staleness and availability controls.

Do not subtract differently normalized OFI values to claim a raw disjoint flow interval. Use existing raw-sum information where available or give the derived ratio feature an honest meaning. Do not infer cancellations from L2 minus trades when the project has already rejected that approximation.

For quote currency, same-venue Binance spot/perp basis compares USDT prices, while Kraken USD versus Binance USDT levels also contain stablecoin movement. Without historical USDT/USD, keep that cross-venue feature explicitly named as an unadjusted spread, or omit it from the small clean baseline. Do not hard-code USDT=USD or infer a “stablecoin rate” from the very BTC venue premium being modeled; that is circular. Future USD-adjusted premiums need an independent, timely conversion source.

Research supports these as tests, with explicit horizon limits:

| Research | What it supports here | What it does not establish |
|---|---|---|
| [Cont, Cucuringu & Zhang (2023)](https://arxiv.org/html/2112.13213) | Lagged cross-asset order-flow hypotheses. | Guaranteed 5–30-minute crypto predictability; equity effects decay. |
| [Albers et al. (2021)](https://arxiv.org/html/2108.09750) | Venue-aware features and strong simple models. | Transfer of their 500-ms results to a 30-minute signal. |
| [Intraday momentum and reversal study (2022)](https://www.sciencedirect.com/science/article/pii/S1062940822000833) | Test both continuation and reversal conditional on market state. | A universal always-reverse strategy; their horizon/sample differs. |
| [Anastasopoulos et al., Order flow and cryptocurrency returns (2026)](https://doi.org/10.1016/j.finmar.2026.101047) | Broader aggregate order flow can contain information. | Its daily-scale findings are not a minute-horizon validation. |
| [Kim & Hansen, Quarter-Hour Effect (2026 preprint)](https://arxiv.org/html/2607.09426v2) | Cheap, predeclared clock-phase interaction tests using existing data. | “Quarter-hour” does not mean the paper proves 15-minute forecast alpha. Its opening-return and 4–12-hour flow results concern different targets. |

The last paper is a possible small addition to the backlog, not a new strategy mandate. Test seconds/minutes within known five-/fifteen-minute cycles as contextual features if needed; do not expand the project into ten-second trading or add dozens of technical indicators to chase its headline results.

For all feature families, compare baseline and challenger on identical dates and eligible rows. Reject a family when it adds no incremental evidence; an interpretable negative result is better than preserving every feature ever considered.

## External data: retain priority, correct the history claim

I agree with existing-data-first and with recording strategically before needing the data. I do not agree that outside sources generally have no trustworthy June–October 2026 history. That needs a source-specific answer.

**Free market-history expansion:** Binance's official archive provides historical trades/aggregate trades and other public datasets. Its documentation warns that spot timestamps from January 2025 use microseconds. Audit units, fields, checksum/version and missing periods before integration. Bulk files arrive after the recorded events; that delivery time is distinct from when the underlying exchange events were originally public. Without original capture timestamps, use an explicit historical latency assumption rather than fabricated local receipt. [Binance public-data repository](https://github.com/binance/binance-public-data)

Build a separate reduced-feature benchmark to investigate whether longer history helps. First compare reduced and full feature sets on their common four-month interval. Then extend only the reduced model farther back. Trades cannot reconstruct LOB OFI, spread, midpoint or full-path midpoint excursion targets. If older labels use trade/bar prices instead, declare a different target and test comparability in the overlap; otherwise apparent gains can simply reflect bid/ask bounce or changed labels. Longer history is useful evidence, not automatically better training data under drift.

**Capture priorities:** verify a real USDT/USD market and record its quotes with receive timestamps; fill the Binance OI gap if worthwhile. Do not assume `USDTUSD` exists on Binance because the repo has a fixture with that name. Discover supported symbols via [Kraken instrument metadata](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/instrument) and the selected venue's own exchange information. A single suitable USD conversion source can support quote adjustment; another full L3 stream is unnecessary for that narrow purpose.

Binance's [current OI/statistics documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) provides current OI and recent five-minute statistics. Its latest-month REST limitation is not proof that older public metric files or vendor archives do not exist. Verify older metrics and their publication semantics before use. Poll a narrow source on the intended recording machine with a quota; do not enable another service on the Mac.

**Vendor history can cover the archive:** [Tardis](https://docs.tardis.dev/faq/general) documents historical tick/derivatives coverage predating 2026, although exact exchange, symbol and channel availability must be checked. Vendor capture time is genuine vendor observation, not the user's observation. [Glassnode PIT documentation](https://docs.glassnode.com/data/point-in-time-metrics) describes broad PIT tracking from July 2025, so relevant June–October 2026 history can exist. It also distinguishes `computed_at` from later API publication; PIT immutability alone does not establish zero delivery lag. Neither provider is a current purchase recommendation.

**Sentiment remains optional:** the directly relevant [Guégan–Renault study](https://www.sciencedirect.com/science/article/pii/S1544612319314199) found a small, bubble-concentrated relationship up to 15-minute frequency and no economic profits from it. Santiment's [latency documentation](https://academy.santiment.net/metrics/details/latency/) makes aggregated social data a delayed-context candidate, and [its social-volume definition](https://academy.santiment.net/metrics/social-volume/) warns of historical updates. GDELT is a reasonable low-cost attention/event pilot, but not an instant newswire. If tried later, compare counts → event types → sentiment on the same rows and delays. Do not buy raw X access or build an LLM pipeline merely because sentiment sounds differentiated.

**On-chain and options:** useful hypotheses for slower context, expiry effects and risk, with weaker evidence for this project's fast directional targets. The [on-chain flow preprint](https://arxiv.org/abs/2411.06327) studies one-to-six-hour frequencies. [The options-expiry paper](https://www.sciencedirect.com/science/article/pii/S1544612326008688) supports expiry/positioning tests, not observable dealer inventory signs from public OI. Existing DVOL should be ablated before adding a full chain.

The current API guide remains a catalogue, not a procurement backlog that must be completed. Keep one small source-specific availability schema when a source is admitted; do not build a multi-vendor platform in anticipation.

## The minimum research process that stays mandatory

Your scope reduction is right. Keep these inexpensive safeguards now:

1. Frozen split manifest, model/config/calendar versions, feature order and replay assumptions.
2. OOF rows containing decision times, true labels, probabilities, expected/robust-return semantics, risk predictions, source/model identifiers and missingness.
3. A single experiment ledger with attempted configurations and negative results.
4. A shared lockbox exposure ledger checking interval intersections across runs/output roots. Renaming a directory does not create unexposed data.
5. Paired, temporally aware comparisons and a predeclared primary metric for each output.

Defer a generalized provenance service, repeated raw-archive hashing, automatic retraining, neural infrastructure and a suite of paid feeds. But keep raw receive timestamps and availability/revision semantics when collecting a new source; those facts cannot be reconstructed reliably later.

Use simple stopping rules. If a bounded regularized directional model does not beat proper-score baselines and matched reversal controls, classify that line as inconclusive or negative rather than adding complexity indefinitely. If risk improves but direction does not, retain the risk result. If neither improves robustly, the project can still demonstrate a correct pipeline and an honest negative forecasting study; gather new periods before further search.

A provisional development improvement should survive matched-row checks, plausible delay stress and more than one local volatility state before lockbox testing. If only a handful of events drive it, report that fact. Any statistical interval after substantial development searching remains conditional on that search; the unexposed/future period is what protects the final claim.

## Final project decision

Adopt the reduced sequence in internal plan notes (not published): **evaluation and honest controls → lean loading → bounded timing audit → regularized probabilities and risk calibration → compact existing-data features → one frozen holdout evaluation**. Keep the rich M-task catalogue as optional reference, not a requirement to build 16 systems.

Continue the original 5/15/30-minute signal objective. Express it as probabilities, cautiously calibrated magnitude and adverse-excursion risk, with room to abstain. The pilot currently justifies optimism about the risk component and further investigation of directional classification; it does not justify a profitability claim or a new architecture bet.

The lesson to take from public systematic-fund practice is disciplined separation of data, forecasting, risk/position decisions and execution—not a supposed secret Jane Street architecture. Public firm descriptions do not reveal their proprietary features or model configurations. With trading costs still deferred, the milestone is credible forecasting evidence. Before any later claim of deployable alpha, execution prices, fees, funding, latency and turnover must be evaluated.

Please implement the narrow active sequence, preserve the existing split and user's environment constraints, retain negative results, and use new evidence—not the number of features or papers cited—to decide what survives.
