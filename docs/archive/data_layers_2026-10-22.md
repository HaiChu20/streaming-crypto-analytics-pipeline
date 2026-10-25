# Additional data for 5/15/30-minute BTC/ETH forecasts

Research checked: **2026-10-22**. Read alongside [the model review](model_review_2026-10-22.md) and the implementation tickets in internal plan notes (not published).

**Final-round priority update:** [the final reviewer reply](final_reply_2026-10-22.md) narrows the active work to existing-data modeling, evaluation, memory and timing. This document remains an optional API catalogue. It is not an instruction to implement every source.

## Decision

Use the existing market information better first. Then test external inputs individually. The strongest practical starting hypotheses are event-related **risk**, positioning/liquidity state, and cross-asset/cross-venue context. News and sentiment deserve experiments, but published evidence does not justify promising a robust five-minute directional edge.

Default budget assumption: **free/public sources first; paid sources optional**. The newly inspected pilot uses archive dates **2026-06-23–2026-10-20**; subscription budget remains unknown. No paid access, accounts, collectors, downloads of historical datasets or service deployments were activated during this research. APIs below were reviewed through documentation, not authenticated end-to-end tests. Provider prices, coverage and access need rechecking when implementation begins.

An API with five-minute buckets may deliver those buckets twenty minutes later. Historical access does not imply historical first-publication timestamps. A chart of revised values is not necessarily a valid training source.

## Evidence map

Evidence strength here concerns relevance and research design, not a guarantee of an edge. `Hypothesis` means our proposed transfer to this repository. Publisher/author abstracts and accessible HTML were examined; paywalled results have not been independently reproduced.

| ID | Primary research | Finding relevant to this project | Transfer and limitation |
|---|---|---|---|
| E1 | [Cont, Cucuringu & Zhang, cross-impact of order flow (2023)](https://arxiv.org/html/2112.13213) | Lagged cross-asset flow can help forecasts; effects decay. | Supports testing BTC↔ETH context. Equity evidence does not establish crypto minute-horizon gains. |
| E2 | [Albers et al., fragmented Bitcoin markets (2021)](https://arxiv.org/html/2108.09750) | Cross-venue information supports short-horizon Bitcoin forecasts. | Supports venue-aware features, but its subsecond setting is much shorter than our targets. |
| E3 | [Guégan & Renault, social sentiment and Bitcoin (2021)](https://www.sciencedirect.com/science/article/pii/S1544612319314199) | Roughly one million StockTwits messages from 2017–2019; relationship up to 15-minute frequency, concentrated around the bubble, too small for economic profits in the study. | Directly relevant negative/mixed evidence. Test sentiment incrementally after market controls; do not assume sentiment works. Publisher abstract/introduction reviewed. |
| E4 | [Huynh, Tuning into the news (2026)](https://doi.org/10.1016/j.jbef.2026.101183) | Sentiment conditions crypto reactions to macroeconomic news. | Motivation for event × sentiment interactions and risk context; not proof that scoring headlines predicts executable returns. |
| E5 | [Scheduled FOMC statements and intraday macro event risk (2026)](https://www.sciencedirect.com/science/article/abs/pii/S1544612326006021) | Scheduled announcements create intraday volatility and trading-volume bursts. | Prioritize event-risk calibration; the existing calendar already implements part of this idea. Publisher abstract/indexed text reviewed. |
| E6 | [Benigno & Rosa, The Bitcoin–Macro Disconnect, NY Fed (2023)](https://www.newyorkfed.org/research/staff_reports/sr1052) | Earlier evidence found limited Bitcoin responses to macro surprises. | Opposing evidence: effects vary by sample and regime. No fixed rule that CPI/FOMC predicts a particular sign. |
| E7 | [Weiss et al., Bitcoin option expiration, gamma exposure, and intraday price reversals (2026)](https://www.sciencedirect.com/science/article/pii/S1544612326008688) | Expiry-related reversals concentrate around elevated ATM open interest and estimated negative gamma exposure. | Justifies expiry/positioning hypotheses. Public OI does not identify actual dealer inventory signs. Publisher abstract reviewed. |
| E8 | [Chi, Chu & Hao, on-chain flows (2024; revised 2025)](https://arxiv.org/abs/2411.06327) | At 1–6-hour frequencies, USDT exchange inflows predict BTC/ETH returns in the study; ETH inflows have different signs; BTC inflows mostly do not predict returns. | Preprint, 2017–2023 sample, much slower horizons. Use slow context as an optional test. Abstract reviewed. |
| E9 | [Liu & Son, social-data selection and collection (2024)](https://www.nature.com/articles/s41599-024-03316-7) | Collection and source selection affect sentiment reliability. | Motivation for source coverage, deduplication and dataset audits; not a directional trading result. |

Evidence does not currently select a particular vendor's proprietary sentiment score for this strategy. There is also no validated claim here that Coinbase premium, an ES/NQ feed, or an options-skew feature improves this exact model. Those are labeled hypotheses with acceptance tests in the plan.

## Priority and procurement matrix

| Layer | Proposed role | First source | History route | Priority / reason |
|---|---|---|---|---|
| Existing BTC/ETH and venue context | Return, volatility and risk | Current archive | Current archive | **A:** no subscription; fix contextual features and coverage first. |
| Perpetual OI/funding/basis | Positioning and liquidation regime | Existing Kraken/Binance feeds; Binance REST for gaps | Existing archive; public Binance files to audit; optional Tardis | **A:** natural extension, but confirm each field's units and availability. |
| Macro calendar / release surprise | Event risk; conditional return response | Existing official calendar; optional Trading Economics | Official schedule vintages; vendor first-release history if verified | **A** for existing calendar audit; **B** for paid surprise data. |
| News attention and event types | Burst/risk context, then sentiment | GDELT; optional Alpha Vantage | GDELT bulk; vendor historical API subject to vintage audit | **B:** free pilot feasible; latency limits five-minute use. |
| Social activity / sentiment | Attention and slow regime, conditional direction | Santiment; raw X only with a budget | SanAPI or X search, subject to historical-version constraints | **C:** weak/mixed directional evidence; cost, revisions and latency. |
| Options term structure / skew / expiry OI | Risk and expiry context | Deribit public API | Optional Tardis options-chain history | **B/C:** DVOL already exists; full-chain data adds substantial work. |
| Coinbase spot | Additional USD venue flow | Coinbase Exchange | Forward collection or vendor tick history; candles only for bar features | **C:** plausible but needs incremental evidence. |
| ES/NQ futures | Broad risk-state context | Databento CME | Paid historical data, with session and roll handling | **C:** adds coverage/licensing complexity; not first purchase. |
| On-chain exchange flows | Slow positioning/context | Glassnode PIT metrics | Metric-specific point-in-time history | **D:** hour-scale evidence and chain/provider delay. |

Priority is not an instruction to implement every source. One source that fails the coverage or timing audit should be deferred without blocking the core-model work.

## API catalogue

Endpoint examples are request shapes, not executed commands or promises about account entitlements. Use environment/secret-store keys, redacted logs and configured quotas. Preserve raw responses and provider error messages. Every adapter needs a bounded one-day probe before a large backfill.

### A1. Binance positioning: targeted gap filling

Official [USDⓈ-M market-data documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) documents these public endpoints:

```text
GET https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
GET https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=500
GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=...&endTime=...
```

The statistics endpoint starts at five-minute resolution, has a 500-record maximum, and documents only the latest month through that endpoint. Its timestamp is a period-end timestamp. Funding settlement history is different from a predicted next funding rate; retain distinct fields. Binance mark-price streaming does not provide all OI data needed by this project.

The [Binance public archive browser](https://data.binance.vision/?prefix=data/futures/um/daily/metrics/BTCUSDT/) is a candidate for older metrics, with the [official download repository](https://github.com/binance/binance-public-data) as background. The listing was not exposed by the text browser, so actual metric-file coverage, fields and first-publication timing remain **unverified**. Do not conclude that all Binance history is limited to one month, or that old OI files are already usable point-in-time data.

Proposed features: OI percent changes over 5/15/60 minutes, price × OI-change interactions, funding-time interaction, and liquidation intensity relative to activity. Keep contract/base/notional units explicit. Audit existing Kraken OI first. A Binance OI adapter fills a particular gap; do not silently enable previously excluded OKX streams.

### A2. Macro releases: existing schedules, then optional Trading Economics

Keep the existing [calendar code](../../research/calendar.py) and official [Federal Reserve](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) / [BLS](https://www.bls.gov/schedule/) sources. Schedule-only features are minutes to/from an event and event category; they do not contain release surprises.

[Trading Economics streaming documentation](https://docs.tradingeconomics.com/economic_calendar/streaming/) exposes economic releases with actual, forecast, previous and revised fields. Its [point-in-time page](https://docs.tradingeconomics.com/economic_calendar/point-in-time/) documents a country/indicator/date route:

```text
GET https://api.tradingeconomics.com/calendar/country/{countries}/indicator/{indicators}/{start}/{end}?c={API_KEY}&f=json
```

The page claims historical values as originally available, but the example does not clearly expose an independent as-of/vintage selector. **Verify revision semantics and intraday availability with a known revised release before labeling the adapter PIT.** Historical dates alone are insufficient. Access, streaming entitlements, retention and price need an account-specific quote; none was obtained.

Proposed features: after release, `(actual_first_release - consensus_before_release) / trailing_surprise_scale`, release type, revision surprise when actually announced, and elapsed time since release. Before release, only known schedule and previously available expectations may be used. `TEForecast` is a provider model estimate, not automatically the market consensus. FOMC policy surprises cannot be inferred from a rate change alone without an expectation measure. A thirty-minute post-announcement futures change is unavailable at the announcement instant.

Test primarily on future volatility and adverse-excursion calibration, with return direction secondary. Distinguish a scheduled-date revision from the eventual actual release date in the existing calendar.

### B1. GDELT: free news-attention pilot

Official sources: [GDELT data products](https://www.gdeltproject.org/data.html) and [DOC 2.0 API documentation](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/). GKG 2.0 bulk updates are published at fifteen-minute intervals. The DOC documentation describes a rolling search window rather than an unlimited archive; its published three-month window and query limits must be rechecked with a small current probe. Use bulk history for older periods.

```text
GET https://api.gdeltproject.org/api/v2/doc/doc
    ?query=(bitcoin OR ethereum)
    &mode=artlist&format=json&maxrecords=250
    &startdatetime=YYYYMMDDHHMMSS&enddatetime=YYYYMMDDHHMMSS&sort=datedesc
```

No API key is needed for the public interface. Use narrow time partitions, detect saturated results, and audit coverage rather than treating the first capped response as complete history. A fifteen-minute data product is a weak candidate for prompt five-minute reactions.

Proposed first features: deduplicated article counts, unique-source counts, article age, burst z-scores and event-type flags (hack, exchange outage, regulation, ETF, macro). Start with attention, not polarity. GKG tone is document-level language tone, not necessarily an asset-specific price forecast. Join at observed index/batch availability. Publisher publication time alone does not prove the article was available to this pipeline then. Historical current article bodies can include later edits; do not silently use them as original text.

### B2. Alpha Vantage: optional news/sentiment API

The current [official documentation](https://www.alphavantage.co/documentation/) marks `NEWS_SENTIMENT` **Premium** and documents historical and live news, crypto tickers, time filters and a maximum of 1,000 returned items:

```text
GET https://www.alphavantage.co/query
    ?function=NEWS_SENTIMENT&tickers=CRYPTO:BTC
    &time_from=YYYYMMDDTHHMM&time_to=YYYYMMDDTHHMM
    &sort=EARLIEST&limit=1000&apikey={API_KEY}
```

Comma-separated tickers select articles mentioning them simultaneously, so query BTC and ETH separately unless intersection is intentional. The [general support page](https://www.alphavantage.co/support/) advertises 25 free requests/day for most datasets; that is **not a promise of free NEWS_SENTIMENT access**. Exact plan price, earliest usable news date and first-seen/revision timestamps remain unverified.

Proposed use: compare counts, relevance-weighted sentiment and event types against the same market baseline. Partition saturated queries recursively; if the minimum query interval is still saturated, mark it incomplete. Provider sentiment scores may have changed algorithms over history; record score/model-version information if supplied and treat absent vintage guarantees as a research limitation.

### C1. Santiment: convenient social context with important delays

Use [SanAPI](https://academy.santiment.net/sanapi/accessing-the-api/) at `POST https://api.santiment.net/graphql`, with `Authorization: Apikey {API_KEY}`. A documented query shape is:

```graphql
{
  getMetric(metric: "social_volume_total") {
    timeseriesDataJson(
      selector: {slug: "bitcoin"}
      from: "2026-09-01T00:00:00Z"
      to: "2026-09-02T00:00:00Z"
      interval: "5m"
    )
  }
}
```

[Social Volume](https://academy.santiment.net/metrics/social-volume/) has five-minute buckets, but can receive additions for twelve hours and revisions to the previous month's history when labels or processing change. The [latency page](https://academy.santiment.net/metrics/details/latency/) reports social-volume median availability near ten minutes and P95 near twenty minutes; social dominance is slower. These are provider observations, not a latency guarantee for our account.

Inspect the [metric catalogue](https://api.santiment.net/available_metrics), [sentiment metric definitions](https://academy.santiment.net/metrics/sentiment-metrics/) and [access restrictions](https://academy.santiment.net/metrics/details/access/). Resolution, history and recent-data access differ by metric and plan. Do not assume every sentiment metric is five-minute. Price and BTC/ETH historical coverage require verification.

Proposed use: social-volume surprise, source composition, sentiment change and disagreement as delayed context for 15/30-minute risk and return experiments. First test volume alone. Save successive observations so revisions remain visible. A simple twelve-hour lag does not cure later monthly recalculations. Historical-only revised values belong in an explicitly assumption-based experiment until genuine vintages are available.

### C2. X: raw messages, potentially expensive

Official [search documentation](https://docs.x.com/x-api/posts/search/introduction) distinguishes recent search, covering seven days, from full-archive search. Recent requests return up to 100 posts; full archive up to 500, subject to account access:

```text
GET https://api.x.com/2/tweets/search/recent?query=...&start_time=...&end_time=...
GET https://api.x.com/2/tweets/search/all?query=...&start_time=...&end_time=...
Authorization: Bearer {TOKEN}
```

The [pricing documentation](https://docs.x.com/x-api/getting-started/pricing) currently lists pay-per-use post reads at $0.005 per post. At that listed unit price, 100,000 posts cost about $500 and one million about $5,000, before other charges or changes. Recheck the account's pricing and hard budget controls before making requests. Older subscription-price articles are not a valid budget basis.

Proposed use only after a cost estimate: asset-aware message counts, unique authors, polarity, disagreement and message novelty, with bot/repost sensitivity checks. Historical likes/reposts observed today contain future engagement relative to the original post. Do not use them as historical-at-posting features. Deleted posts, search coverage and changes in source population also make backfilled history differ from live ingestion. Keep original creation time, first observation, edit versions and query version separately.

### C3. StockTwits and CryptoPanic: not dependencies yet

The paper E3 used StockTwits, but [StockTwits' developer page](https://api.stocktwits.com/developers) currently says new API registrations are closed while the platform is reviewed. Do not plan around obtaining a new free key. Existing authorized access would require its own history and entitlement audit.

[CryptoPanic's developer page](https://cryptopanic.com/developers/api/) did not expose enough documentation in this research to verify current endpoint versions, historical access and pricing. Keep it as an unverified alternative, not an implementation-ready API. Do not copy guessed endpoints from old blog posts into the collector.

### B/C1. Deribit options beyond the existing DVOL feed

The repo already collects closed-minute DVOL candles. For options, official methods include [book summaries by currency](https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency), [ticker](https://docs.deribit.com/api-reference/market-data/public-ticker) and [instrument discovery](https://docs.deribit.com/api-reference/market-data/public-get_instruments). Use the configured production base, not documentation testnet examples:

```text
https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option
https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option
https://www.deribit.com/api/v2/public/ticker?instrument_name={DISCOVERED_INSTRUMENT}
```

These methods supply current instrument metadata, OI, quotes and option measures, with ticker fields such as IV/Greeks where applicable. Current snapshots and historical trades do **not** reconstruct past complete option chains. Use forward observations or a vendor archive; confirm per-channel history before acquisition.

Proposed features: constant-maturity ATM IV, near/far term slope, 25-delta risk reversal with a documented sign convention, ATM OI near expiry, and lagged put/call trade activity. Quote filtering, moneyness/expiry interpolation and currency/contract conversions must be fixed in the feature specification. Reject stale/wide/invalid quotes; do not extrapolate absent strikes without declaring it.

Do not label an OI-based proxy as measured dealer gamma: public OI does not reveal who is long or short. Start with unsigned concentration and expiry proximity. Any signed inventory assumption is a separately named scenario. Test risk forecasts first and expiry-specific return behavior separately.

### C4. Tardis: optional historical market-data purchase

[Tardis CSV documentation](https://docs.tardis.dev/downloadable-csv-files/overview) describes daily files:

```text
https://datasets.tardis.dev/v1/{exchange}/{data_type}/{YYYY}/{MM}/{DD}/{symbol}.csv.gz
```

The first day of each month is offered as a free sample; other history requires the relevant access. The [data FAQ](https://docs.tardis.dev/faq/data) lists normalized types including `options_chain`, `derivative_ticker`, quotes, trades and book data. Availability varies by exchange and channel. The [general FAQ](https://docs.tardis.dev/faq/general) lists Deribit history from 2019-03-30 and Binance USDT futures from 2019-11-17; those dates are not a guarantee for every instrument or field. Pricing and exact required history need a quote.

Proposed acquisition: request only the overlap of the user's archive with BTC/ETH options-chain or missing derivative fields; first validate a free sample's schema and timing. Discover provider exchange/symbol identifiers from metadata rather than inventing aliases. Preserve vendor capture time as vendor time, not our original receive time. CSV serialization must be checked against the repo's Float64/exact-price requirements. Raw-format access and licenses may differ from normalized CSV access.

### C5. Coinbase: separate tick-data and bar-data experiments

Use [Coinbase Exchange WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) for BTC-USD and ETH-USD. Heartbeats and trade identifiers support gap detection; some match messages may be missed, requiring recovery. Verify current channel authentication and subscription requirements during the probe.

For historical bars, [product candles](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles) provides:

```text
GET https://api.exchange.coinbase.com/products/BTC-USD/candles
    ?granularity=60&start={ISO_TIME}&end={ISO_TIME}
```

The endpoint is capped at 300 candles per request. It does not supply historical LOB updates. Candles cannot reconstruct midpoint premiums, OFI or book depth. Use a licensed tick archive such as an appropriately covered Tardis product for those features, or collect forward.

Proposed tests: USD-adjusted premium and its change, cross-venue return lags, then flow/OFI when true tick history exists. Keep a candle-only test distinct. Apply the USDT/USD adjustment described in the model review. E2 motivates cross-venue tests; it does not establish this specific premium as alpha.

### C6. Databento: optional ES/NQ context

[Databento's getting-started guide](https://databento.com/docs/getting-started/build-first-app) documents historical queries; the [cost endpoint](https://databento.com/docs/api-historical/metadata/metadata-get-cost) estimates a query's charge before requesting data:

```text
GET https://hist.databento.com/v0/metadata.get_cost
GET https://hist.databento.com/v0/timeseries.get_range
Parameters: dataset=GLBX.MDP3, schema=ohlcv-1m,
            symbols={EXPLICIT_ES_OR_NQ_CONTRACTS}, start=..., end=...
Authentication: HTTP Basic, API key as username, empty password
```

Historical usage and live exchange entitlements have separate cost considerations; obtain an exact estimate and license terms. No purchase is needed for the initial core-model experiments.

Proposed features: completed-bar returns, realized volatility, trading-session state and rolling crypto/equity co-movement. Account for US daylight saving, holidays, futures breaks and weekends. Do not forward-fill a Friday quote as fresh Sunday information. Fix a causal contract-roll rule; selecting the most active contract using future volume leaks information. This is a plausible macro-state experiment, not a paper-proven five-minute trading signal.

### D1. Glassnode point-in-time on-chain flows

Prefer explicit [point-in-time metrics](https://docs.glassnode.com/data/point-in-time-metrics) over currently revised exchange-label history. [The PIT API catalogue](https://docs.glassnode.com/basic-api/endpoints/pit) documents metric-specific routes, for example:

```text
GET https://api.glassnode.com/v1/metrics/transactions/transfers_volume_to_exchanges_mean_pit
    ?a=BTC&i=10m&s={UNIX_START}&u={UNIX_END}&api_key={API_KEY}
```

That example is a **mean**, not total inflow. Select and verify an appropriate total/net-flow metric for E8; do not substitute means for sums. Asset, chain, resolution, history and plan entitlement are metric-specific. Glassnode's PIT documentation says broad PIT tracking began in July 2025, with earlier history for selected BTC/ETH metrics. This can cover the project's June–October 2026 interval, subject to exact metric access. Preserve `computed_at` where supplied, but the provider documents an additional delay before API publication. Its [PIT introduction](https://research.glassnode.com/introducing-point-in-time-data/) explains why calculation and chain delays matter; ten-minute resolution does not mean ten-minute availability.

Proposed features: available exchange inflow/outflow changes, USDT flow context where the exact asset/chain metric exists, and robust trailing deviations over 1/6/24 hours. Keep chain and entity-label versions. Exchange deposits can be internal wallet movements and are not guaranteed intent to sell. E8 is hour-scale preprint evidence; test thirty-minute risk context before considering fast directional use. Historical access and price need confirmation.

## Common data contract for all adapters

This is a proposed schema specification, not a new running service. Preserve raw provider responses first; normalized records should include:

```text
source, channel_or_metric, source_record_id, asset, instrument, units
event_time_ns                  # event creation or measurement time
interval_start_ns/end_ns       # if an aggregate; never consume unfinished bars
provider_published_ns          # nullable; documented semantics required
provider_first_seen_ns         # nullable; distinct from publisher creation time
provider_computed_ns           # nullable; e.g. PIT computation timestamp
observed_recv_ns               # real local receipt; never synthetic
retrieved_at_ns                # when a historical response was downloaded
revision_id, payload_sha256, parser_version, source_query_version
availability_basis            # observed_live | vendor_pit | assumed
assumed_available_ns           # nullable; explicit research assumption only
quality_flags, missing_reason, raw_payload_location
```

For live records, availability starts no earlier than actual receipt and derived computation completion. For vendor PIT history, use the proven publication/first-seen/computation semantics plus a declared delivery assumption; keep that basis distinguishable from local observation. For backfills with unknown vintage, no timestamp arithmetic can manufacture historical certainty. An assumed lag is an experiment parameter, not a fact.

Append revisions rather than replacing history. At a decision time, choose only a record version that had become available by then, using a backward as-of join and source-specific age limits. Ties need deterministic ordering. Record feature age and missingness. A delayed older revision must not overwrite newer state. Zero mentions from a complete observed interval is different from an API outage or a capped incomplete query.

Use the existing Rust engine for market transformations. For asynchronous external context, add a shared causal transformation path used by both offline matrix building and live inference, analogous to the existing Python calendar integration. Do not create unrelated offline/live implementations of the same news or social feature. Longer windows need explicit warm-up/pre-roll and feature-version provenance.

## Sentiment experiment specification

The optional implementation task is M9 in internal plan notes (not published). Its research contract is:

1. Freeze asset queries, languages, source population and deduplication policy before evaluation. `BTC`/`ETH` ambiguities need asset matching; count syndicated copies once under a causal policy.
2. Compare four nested feature sets on the same eligible rows: market baseline; baseline + attention; baseline + attention + event type; baseline + those + sentiment. This separates information arrival from polarity.
3. Start with deterministic/lexicon scoring as a control. A frozen financial text model is a challenger, not an assumed improvement. Evaluate asset-specific polarity and uncertainty; generic negative language can describe an event unrelated to the asset.
4. If an LLM is used, lock model/version, prompt, input text, output schema and scoring time. Exclude future article updates and retrieved future context. A later pretrained model may remember historical events; flag this contamination risk and prefer prospective evaluation or a model with an appropriate training cutoff. Text-classification accuracy alone is not forecast value.
5. Build only causal 5/15/60-minute counts, sentiment changes and disagreement features. Apply measured availability before selecting a window. A five-minute bucket arriving twenty minutes late is old information.
6. Include existing return, volatility, flow and calendar controls, because people often post in response to price moves. Test incremental future prediction; do not call correlation or Granger results causal trading evidence.
7. Evaluate Brier/log loss, return loss/IC, future-volatility loss and risk calibration separately. Report event/non-event and high/low-attention periods with block uncertainty. Compare natural latency with additional delay stress. Keep a negative result if sentiment adds nothing beyond counts.

## Source admission and purchase decision

Before paying for or integrating a source, produce a small manifest containing exact required dates, BTC/ETH coverage, resolution, record counts, missing intervals, latency quantiles, revisions, currency/unit definitions, monthly request estimate, one-off history cost, recurring cost, license and storage/redistribution limits. Record `unknown` honestly where a provider does not document an item.

For each source, compare its exact overlap period with a baseline trained and tested on that same period. Also report how much of the full archive is excluded. Do not compare an augmented model on a recent bull month with a baseline on a full year. A data provider's outages must be evaluated as outages, not removed only from the challenger.

The free-first path is current archives → evaluation/model improvements → existing calendar/positioning audit → GDELT attention pilot and public-feed probes. Paid choices should be made after measuring the specific gap: historical options/venue data, point-in-time macro surprises, social metrics, or slow on-chain context. Raw X should not be the default purchase.

Daily Fear & Greed, daily ETF flow totals, daily Google Trends and broad on-chain valuation ratios are low-priority context candidates. Their daily publication timing and revisions must be respected; interpolation onto one-second rows creates no new information. Neither a daily feature nor an expensive subscription should be added just to increase the column count.
