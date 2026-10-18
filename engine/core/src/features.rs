use crate::{ENGINE_VERSION, Result, SECOND, row::Row};
use crate::{Fixed, Kind, Outcome, Record, Side, StreamBook};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

fn error(message: impl Into<String>) -> crate::Error {
    crate::Error(message.into())
}

pub fn asset(symbol: &str) -> Option<&'static str> {
    match symbol {
        "BTCUSDT" | "BTC-USDT" | "BTC-USDT-SWAP" | "BTC/USD" | "PF_XBTUSD" => Some("BTC"),
        "ETHUSDT" | "ETH-USDT" | "ETH-USDT-SWAP" | "ETH/USD" | "PF_ETHUSD" => Some("ETH"),
        _ => None,
    }
}

type Key = (String, String, String);
const UNIT: i64 = 100_000_000;
/// Silence longer than 60 seconds expires book features and each perp-extras source.
pub const STALE_S: i64 = 60;
fn fresh(last: Option<i128>, tick: i64) -> bool {
    last.is_some_and(|ns| i128::from(tick) - ns <= i128::from(STALE_S) * i128::from(SECOND))
}
fn fixed(v: &Value) -> Result<i64> {
    let text = if let Some(s) = v.as_str() {
        s.to_owned()
    } else {
        v.to_string()
    };
    Ok(text.parse::<Fixed>()?.0)
}
fn number(v: &Value) -> Option<f64> {
    let n = if let Some(s) = v.as_str() {
        s.parse().ok()?
    } else {
        v.as_f64()?
    };
    if f64::is_finite(n) { Some(n) } else { None }
}
fn scale(q: i128, factor: i64) -> f64 {
    q as f64 / UNIT as f64 * factor as f64 / UNIT as f64
}

#[derive(Default, Serialize, Deserialize)]
struct Gate {
    active: Option<String>,
    retired: BTreeSet<String>,
    factors: BTreeMap<String, i64>,
}
impl Gate {
    fn start(&mut self, r: &Record) -> Result<()> {
        if !self.retired.contains(&r.conn) {
            let factor = if r.exchange == "okx" && r.market == "perp" {
                r.detail
                    .as_ref()
                    .and_then(|d| d["instrument"]["ctVal"].as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse::<Fixed>().map(|f| f.0))
                    .transpose()?
            } else {
                Some(UNIT)
            };
            if let Some(factor) = factor {
                if factor <= 0 {
                    return Err(error("ctVal must be positive"));
                }
                self.factors.insert(r.conn.clone(), factor);
            }
        }
        Ok(())
    }
    fn end(&mut self, id: &str) {
        self.retired.insert(id.into());
        self.factors.remove(id);
    }
    fn crash(&mut self, carrying: &str) {
        if let Some(old) = self.active.take() {
            if old != carrying {
                self.end(&old);
            } else {
                self.factors.remove(&old);
            }
        }
    }
    fn accept(&mut self, id: &str) -> bool {
        if self.retired.contains(id) {
            return false;
        }
        if self.active.as_deref() != Some(id)
            && let Some(old) = self.active.replace(id.into())
        {
            self.end(&old);
        }
        true
    }
    fn factor(&self, r: &Record) -> Option<i64> {
        if r.exchange == "okx" && r.market == "perp" {
            self.factors.get(&r.conn).copied()
        } else {
            Some(UNIT)
        }
    }
}
#[derive(Default, Serialize, Deserialize)]
struct Bucket {
    count: i64,
    volume: i128,
    buy: i128,
    sell: i128,
    unscaled_trades: bool,
    malformed_trades: bool,
    liq_count: i64,
    liq: i128,
    l3: [i64; 3],
}
#[derive(Default, Serialize, Deserialize)]
struct Instrument {
    l2: Option<StreamBook>,
    l3: Option<StreamBook>,
    gates: BTreeMap<String, Gate>,
    seen: BTreeSet<String>,
    ids: HashSet<String>,
    fifo: VecDeque<String>,
    #[serde(default)]
    uncertain_trades: bool,
    buckets: BTreeMap<i64, Bucket>,
    history: VecDeque<Option<i64>>,
    returns: VecDeque<Option<f64>>,
    first_mid: Option<i64>,
    last_recv: BTreeMap<String, i128>,
    extras_source: Option<String>,
    funding: Option<f64>,
    mark: Option<f64>,
    oi: Option<f64>,
}
impl Instrument {
    fn remember(&mut self, id: String) -> bool {
        if !self.ids.insert(id.clone()) {
            return false;
        }
        self.fifo.push_back(id);
        if self.fifo.len() > 50_000 {
            self.ids.remove(&self.fifo.pop_front().unwrap());
        }
        true
    }
    fn consume(&mut self, r: &Record, current_tick: Option<i64>) -> Result<()> {
        self.seen.insert(r.feed.clone());
        let crash = r.kind == Kind::Control
            && r.event.as_deref() == Some("drop")
            && r.reason.as_deref() == Some("collector_crash");
        if crash
            && self
                .gates
                .get(&r.feed)
                .is_some_and(|g| g.retired.contains(&r.conn))
        {
            return Ok(());
        }
        // Retired connection records are ignored for freshness as well as book/extras state.
        if !self
            .gates
            .get(&r.feed)
            .is_some_and(|g| g.retired.contains(&r.conn))
        {
            let feed = match r.feed.as_str() {
                "l2" => Some("l2"),
                "mark" => Some("mark"),
                "ticker" => Some("ticker"),
                _ => None,
            };
            if let Some(feed) = feed {
                self.last_recv.insert(feed.into(), r.recv_ns);
            }
        }
        if r.kind == Kind::Control {
            let gate = self.gates.entry(r.feed.clone()).or_default();
            match r.event.as_deref() {
                Some("start") => gate.start(r)?,
                Some("stop" | "drop") => {
                    if (crash || gate.active.as_deref() == Some(&r.conn))
                        && matches!(r.feed.as_str(), "mark" | "ticker")
                    {
                        self.mark = None;
                        self.funding = None;
                        self.oi = None;
                    }
                    if crash {
                        gate.crash(&r.conn);
                    } else {
                        gate.end(&r.conn);
                    }
                }
                _ => {}
            }
        }
        if matches!(r.feed.as_str(), "l2" | "l3") {
            let state = if r.feed == "l2" {
                &mut self.l2
            } else {
                &mut self.l3
            };
            if state.is_none() {
                *state = Some(StreamBook::new(r)?);
            }
            let state = state.as_mut().unwrap();
            let outcome = state.consume(r)?;
            let gate = self.gates.entry(r.feed.clone()).or_default();
            if r.kind == Kind::Snapshot && !matches!(outcome, Outcome::Ignored) {
                gate.accept(&r.conn);
            }
            if matches!(outcome, Outcome::Break(_)) && !crash {
                gate.end(&r.conn);
            }
            if r.feed == "l3" && outcome == Outcome::Applied {
                let counts = state.l3_event_counts().unwrap();
                let bucket = self
                    .buckets
                    .entry(window_tick(r.recv_ns, current_tick)?)
                    .or_default();
                for (total, count) in bucket.l3.iter_mut().zip(counts) {
                    *total += count;
                }
            }
            return Ok(());
        }
        if r.kind == Kind::Control || matches!(r.feed.as_str(), "l1" | "trades_agg") {
            return Ok(());
        }
        let gate = self.gates.entry(r.feed.clone()).or_default();
        if !gate.accept(&r.conn) {
            return Ok(());
        }
        let factor = gate.factor(r);
        let payload: Value = serde_json::from_str(&r.payload)?;
        match r.feed.as_str() {
            "trades" => {
                let trades: Vec<&Value> = match (r.exchange.as_str(), r.market.as_str()) {
                    ("binance", _) => vec![&payload["data"]],
                    ("okx", _) | ("kraken", "spot") => payload["data"]
                        .as_array()
                        .ok_or_else(|| error("missing trade data"))?
                        .iter()
                        .collect(),
                    ("kraken", "perp") if r.kind == Kind::Snapshot => payload["trades"]
                        .as_array()
                        .ok_or_else(|| error("missing futures trade snapshot"))?
                        .iter()
                        .collect(),
                    ("kraken", "perp") => vec![&payload],
                    _ => return Err(error("unsupported trade exchange")),
                };
                for trade in trades {
                    let (id, qty, buy) = match r.exchange.as_str() {
                        "binance" => (
                            trade["t"].to_string(),
                            fixed(&trade["q"])?,
                            !trade["m"]
                                .as_bool()
                                .ok_or_else(|| error("missing buyer-maker flag"))?,
                        ),
                        "okx" => (
                            trade["tradeId"]
                                .as_str()
                                .ok_or_else(|| error("missing tradeId"))?
                                .into(),
                            fixed(&trade["sz"])?,
                            taker_buy(trade)?,
                        ),
                        "kraken" => (
                            if r.market == "spot" {
                                trade["trade_id"].to_string()
                            } else {
                                trade["uid"]
                                    .as_str()
                                    .ok_or_else(|| error("missing uid"))?
                                    .into()
                            },
                            fixed(&trade["qty"])?,
                            taker_buy(trade)?,
                        ),
                        _ => unreachable!(),
                    };
                    if id == "null" {
                        return Err(error("missing trade ID"));
                    }
                    if !self.remember(id) || r.kind == Kind::Snapshot {
                        continue;
                    }
                    let bucket = self
                        .buckets
                        .entry(window_tick(r.recv_ns, current_tick)?)
                        .or_default();
                    bucket.count += 1;
                    let Some(factor) = factor else {
                        bucket.unscaled_trades = true;
                        continue;
                    };
                    let volume = i128::from(qty) * i128::from(factor);
                    bucket.volume = bucket
                        .volume
                        .checked_add(volume)
                        .ok_or_else(|| error("trade volume overflow"))?;
                    if buy {
                        bucket.buy = bucket
                            .buy
                            .checked_add(volume)
                            .ok_or_else(|| error("buy volume overflow"))?;
                    } else {
                        bucket.sell = bucket
                            .sell
                            .checked_add(volume)
                            .ok_or_else(|| error("sell volume overflow"))?;
                    }
                }
            }
            "mark" => {
                self.extras_source = Some("mark".into());
                let data = &payload["data"];
                self.funding = number(&data["r"]);
                self.mark = number(&data["p"]);
            }
            "ticker" => {
                self.extras_source = Some("ticker".into());
                self.funding = number(&payload["funding_rate"]);
                self.mark = number(&payload["markPrice"]);
                self.oi = number(&payload["openInterest"]);
            }
            "liquidations" => {
                if !self.remember(format!("liq:{}", payload["data"]["o"])) {
                    return Ok(());
                }
                let bucket = self
                    .buckets
                    .entry(window_tick(r.recv_ns, current_tick)?)
                    .or_default();
                bucket.liq_count += 1;
                // forceOrder is sampled; z is cumulative filled quantity of the pushed order.
                let quantity = i128::from(fixed(&payload["data"]["o"]["z"])?) * i128::from(UNIT);
                bucket.liq = bucket
                    .liq
                    .checked_add(quantity)
                    .ok_or_else(|| error("liquidation volume overflow"))?;
            }
            _ => {}
        }
        Ok(())
    }
    fn row(&mut self, key: &Key, tick: i64) -> Result<Row> {
        let mut row = Row {
            exchange: key.0.clone(),
            market: key.1.clone(),
            symbol: key.2.clone(),
            tick_ns: tick,
            available_ns: tick + 250_000_000,
            engine_version: ENGINE_VERSION.into(),
            warmup: true,
            book_age_ms: self
                .last_recv
                .get("l2")
                .map(|ns| (i128::from(tick) - ns) as f64 / 1_000_000.0),
            ..Default::default()
        };
        if let Some(state) = &self.l2
            && fresh(self.last_recv.get("l2").copied(), tick)
            && state.valid()
            && let Some(book) = state.book()
            && let (Some((bid, bq)), Some((ask, aq))) = (book.best_bid(), book.best_ask())
        {
            let factor = if key.0 == "okx" && key.1 == "perp" {
                self.gates
                    .get("l2")
                    .and_then(|g| state.active_connection().and_then(|id| g.factors.get(id)))
                    .copied()
            } else {
                Some(UNIT)
            };
            if let Some(factor) = factor
                && bid < ask
                && let Some(mid_e9) = book.mid().and_then(|mid| i64::try_from(mid.0).ok())
            {
                row.book_valid = true;
                row.best_bid = Some(bid.0 as f64 / UNIT as f64);
                row.best_ask = Some(ask.0 as f64 / UNIT as f64);
                let mid = book.mid().unwrap();
                row.mid = Some(mid.0 as f64 / 1_000_000_000.0);
                row.mid_e9 = Some(mid_e9);
                let total = i128::from(bq.0) + i128::from(aq.0);
                if total > 0 {
                    row.microprice = Some(
                        (i128::from(ask.0) * i128::from(bq.0)
                            + i128::from(bid.0) * i128::from(aq.0)) as f64
                            / total as f64
                            / UNIT as f64,
                    );
                }
                row.spread_bps = Some(
                    (i128::from(ask.0) - i128::from(bid.0)) as f64 / UNIT as f64 / row.mid.unwrap()
                        * 10000.0,
                );
                let bid = book
                    .top(Side::Bid, 10)
                    .iter()
                    .map(|(_, q)| i128::from(q.0))
                    .sum::<i128>();
                let ask = book
                    .top(Side::Ask, 10)
                    .iter()
                    .map(|(_, q)| i128::from(q.0))
                    .sum::<i128>();
                row.bid_depth10 = Some(scale(bid, factor));
                row.ask_depth10 = Some(scale(ask, factor));
                if bid + ask > 0 {
                    row.imbalance10 = Some((bid - ask) as f64 / (bid + ask) as f64);
                }
            }
        }
        if row.mid.is_some() {
            self.first_mid.get_or_insert(tick);
        }
        row.warmup = self
            .first_mid
            .is_none_or(|first| tick - first < 300 * SECOND);
        let past = |lag: usize| {
            self.history
                .len()
                .checked_sub(lag)
                .and_then(|i| self.history[i])
        };
        let ret = |lag| {
            row.mid_e9
                .zip(past(lag))
                .filter(|(a, b)| *a > 0 && *b > 0)
                .map(|(a, b)| (a as f64 / b as f64).ln())
        };
        row.ret_1s = ret(1);
        row.ret_10s = ret(10);
        row.ret_60s = ret(60);
        self.returns.push_back(row.ret_1s);
        if self.returns.len() > 300 {
            self.returns.pop_front();
        }
        let rv = |n| {
            if self.returns.len() < n {
                None
            } else {
                self.returns
                    .iter()
                    .rev()
                    .take(n)
                    .try_fold(0.0, |sum, r| r.map(|r| sum + r * r))
                    .map(f64::sqrt)
            }
        };
        row.rv_60s = rv(60);
        row.rv_300s = rv(300);
        self.history.push_back(row.mid_e9);
        if self.history.len() > 300 {
            self.history.pop_front();
        }
        let bucket = self.buckets.remove(&tick).unwrap_or_default();
        self.buckets.retain(|t, _| *t > tick);
        if self.seen.contains("trades") {
            if !bucket.malformed_trades && !self.uncertain_trades {
                row.trade_count = Some(bucket.count);
            }
            let calibrated = !self.uncertain_trades
                && !bucket.malformed_trades
                && !bucket.unscaled_trades
                && (key.0 != "okx"
                    || key.1 != "perp"
                    || self
                        .gates
                        .get("trades")
                        .and_then(|gate| gate.active.as_ref().and_then(|id| gate.factors.get(id)))
                        .is_some());
            if calibrated {
                row.volume_base = Some(bucket.volume as f64 / 10_000_000_000_000_000.0);
                row.taker_buy_base = Some(bucket.buy as f64 / 10_000_000_000_000_000.0);
                row.taker_sell_base = Some(bucket.sell as f64 / 10_000_000_000_000_000.0);
            }
        }
        if self.seen.contains("liquidations") {
            row.liq_count_1s = Some(bucket.liq_count);
            row.liq_base_1s = Some(bucket.liq as f64 / 10_000_000_000_000_000.0);
        }
        if self.seen.contains("l3") {
            row.l3_adds_1s = Some(bucket.l3[0]);
            row.l3_modifies_1s = Some(bucket.l3[1]);
            row.l3_deletes_1s = Some(bucket.l3[2]);
        }
        if fresh(
            self.extras_source
                .as_deref()
                .and_then(|source| self.last_recv.get(source))
                .copied(),
            tick,
        ) {
            row.funding_rate = self.funding;
            row.mark_price = self.mark;
        }
        if fresh(self.last_recv.get("ticker").copied(), tick) {
            row.open_interest = self.oi;
        }
        Ok(row)
    }
}
fn taker_buy(trade: &Value) -> Result<bool> {
    match trade["side"].as_str() {
        Some("buy") => Ok(true),
        Some("sell") => Ok(false),
        _ => Err(error("invalid taker side")),
    }
}
/// Late records belong to the tick that processes them, never a closed bucket.
pub fn window_tick(ns: i128, current_tick: Option<i64>) -> Result<i64> {
    let ns = i64::try_from(ns).map_err(|_| error("receive timestamp out of i64 range"))?;
    let tick = ns.div_euclid(SECOND) * SECOND + if ns.rem_euclid(SECOND) > 0 { SECOND } else { 0 };
    Ok(current_tick.map_or(tick, |current| tick.max(current)))
}

#[derive(Default, Serialize, Deserialize)]
pub struct Engine {
    #[serde(with = "instrument_entries")]
    instruments: BTreeMap<Key, Instrument>,
    pub records: u64,
    pub book_breaks: u64,
    pub book_break_reasons: BTreeMap<String, u64>,
    pub data_anomalies: BTreeMap<String, u64>,
}
impl Engine {
    /// Active L2 source and validity for reporting overlap handovers.
    pub fn book_sources(&self) -> BTreeMap<(String, String, String), (String, bool)> {
        self.instruments
            .iter()
            .filter_map(|(key, state)| {
                let book = state.l2.as_ref()?;
                Some((
                    key.clone(),
                    (book.active_connection()?.to_owned(), book.valid()),
                ))
            })
            .collect()
    }
    pub fn consume(&mut self, r: &Record) -> Result<()> {
        self.consume_inner(r, None)
    }
    pub fn consume_at_tick(&mut self, r: &Record, tick: i64) -> Result<()> {
        self.consume_inner(r, Some(tick))
    }
    fn consume_inner(&mut self, r: &Record, current_tick: Option<i64>) -> Result<()> {
        let key = (r.exchange.clone(), r.market.clone(), r.symbol.clone());
        if r.kind == Kind::Control
            && r.event.as_deref() == Some("drop")
            && r.reason.as_deref() == Some("malformed_envelope")
        {
            let state = self.instruments.entry(key).or_default();
            state.seen.insert(r.feed.clone());
            if matches!(r.feed.as_str(), "l2" | "l3") {
                let book = if r.feed == "l2" {
                    &mut state.l2
                } else {
                    &mut state.l3
                };
                if let Some(book) = book {
                    let conn = if r.conn == "__unknown__" {
                        book.active_connection().unwrap_or(&r.conn).to_owned()
                    } else {
                        r.conn.clone()
                    };
                    if book.invalidate_malformed(&conn) {
                        self.book_breaks += 1;
                        *self
                            .book_break_reasons
                            .entry("malformed".into())
                            .or_default() += 1;
                    }
                    state.gates.entry(r.feed.clone()).or_default().end(&conn);
                }
            } else if r.feed == "trades" {
                if let Ok(tick) = window_tick(r.recv_ns, current_tick) {
                    state.buckets.entry(tick).or_default().malformed_trades = true;
                } else {
                    state.uncertain_trades = true;
                }
            } else if matches!(r.feed.as_str(), "mark" | "ticker") {
                state.mark = None;
                state.funding = None;
                state.oi = None;
            }
            *self
                .data_anomalies
                .entry(format!(
                    "{}/{}/{}/{}: malformed_envelope",
                    r.exchange, r.market, r.symbol, r.feed
                ))
                .or_default() += 1;
            self.records += 1;
            return Ok(());
        }
        let state = self.instruments.entry(key).or_default();
        let before = state.l2.as_ref().map_or(0, |b| b.counters().breaks)
            + state.l3.as_ref().map_or(0, |b| b.counters().breaks);
        if let Err(error) = state.consume(r, current_tick) {
            let stream = if r.feed == "l2" {
                &mut state.l2
            } else {
                &mut state.l3
            };
            if matches!(r.feed.as_str(), "l2" | "l3") {
                if let Some(stream) = stream {
                    stream.invalidate_malformed(&r.conn);
                }
                state.gates.entry(r.feed.clone()).or_default().end(&r.conn);
            } else if r.feed == "trades" {
                if let Ok(tick) = window_tick(r.recv_ns, current_tick) {
                    state.buckets.entry(tick).or_default().malformed_trades = true;
                }
            } else if r.feed == "mark" || r.feed == "ticker" {
                state.mark = None;
                state.funding = None;
                state.oi = None;
            }
            *self
                .data_anomalies
                .entry(format!(
                    "{}/{}/{}/{}: {}",
                    r.exchange, r.market, r.symbol, r.feed, error
                ))
                .or_default() += 1;
        }
        let after = state.l2.as_ref().map_or(0, |b| b.counters().breaks)
            + state.l3.as_ref().map_or(0, |b| b.counters().breaks);
        self.book_breaks += after - before;
        if after > before {
            let book = if r.feed == "l3" { &state.l3 } else { &state.l2 };
            let reason = match book.as_ref().and_then(|b| b.last_break()) {
                Some(crate::BreakReason::Drop { reason }) => {
                    format!("drop:{}", reason.as_deref().unwrap_or("unspecified"))
                }
                Some(crate::BreakReason::SequenceGap { .. }) => "sequence_gap".into(),
                Some(crate::BreakReason::ChecksumMismatch { .. }) => "checksum_mismatch".into(),
                Some(crate::BreakReason::MissingSnapshot) => "missing_snapshot".into(),
                _ => "malformed".into(),
            };
            *self.book_break_reasons.entry(reason).or_default() += after - before;
        }
        self.records += 1;
        Ok(())
    }
    pub fn tick(&mut self, tick: i64) -> Result<Vec<Row>> {
        for (key, state) in &mut self.instruments {
            if let Some(book) = &mut state.l2
                && book.valid()
                && book
                    .book()
                    .and_then(crate::Book::mid)
                    .is_some_and(|mid| i64::try_from(mid.0).is_err())
            {
                let conn = book.active_connection().unwrap().to_owned();
                if book.invalidate_malformed(&conn) {
                    self.book_breaks += 1;
                    *self
                        .book_break_reasons
                        .entry("malformed".into())
                        .or_default() += 1;
                    *self
                        .data_anomalies
                        .entry(format!("{}/{}/{}: mid_e9 overflow", key.0, key.1, key.2))
                        .or_default() += 1;
                }
            }
        }
        let mut rows = self
            .instruments
            .iter_mut()
            .map(|(key, s)| s.row(key, tick))
            .collect::<Result<Vec<_>>>()?;
        let assets = rows
            .iter()
            .filter_map(|r| asset(&r.symbol))
            .collect::<BTreeSet<_>>();
        let mid = |ex: &str, market: &str, asset_: &str| {
            rows.iter()
                .find(|r| {
                    r.exchange == ex && r.market == market && asset(&r.symbol) == Some(asset_)
                })
                .and_then(|r| r.mid)
        };
        let mut cross = Vec::new();
        for asset_ in assets {
            let mut row = Row {
                exchange: "cross".into(),
                market: "cross".into(),
                symbol: asset_.into(),
                tick_ns: tick,
                available_ns: tick + 250_000_000,
                engine_version: ENGINE_VERSION.into(),
                warmup: rows
                    .iter()
                    .filter(|r| asset(&r.symbol) == Some(asset_))
                    .any(|r| r.warmup),
                ..Default::default()
            };
            let ratio = |a: Option<f64>, b: Option<f64>| {
                a.zip(b)
                    .filter(|(_, b)| *b > 0.0)
                    .map(|(a, b)| (a / b - 1.0) * 10000.0)
            };
            row.basis_bps_binance = ratio(
                mid("binance", "perp", asset_),
                mid("binance", "spot", asset_),
            );
            row.basis_bps_okx = ratio(mid("okx", "perp", asset_), mid("okx", "spot", asset_));
            row.basis_bps_kraken =
                ratio(mid("kraken", "perp", asset_), mid("kraken", "spot", asset_));
            row.spot_dev_bps_okx =
                ratio(mid("okx", "spot", asset_), mid("binance", "spot", asset_));
            row.spot_dev_bps_kraken = ratio(
                mid("kraken", "spot", asset_),
                mid("binance", "spot", asset_),
            );
            cross.push(row);
        }
        rows.extend(cross);
        rows.sort_by(|a, b| {
            (&a.exchange, &a.market, &a.symbol).cmp(&(&b.exchange, &b.market, &b.symbol))
        });
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trade_ids_are_bounded_and_recent_duplicates_rejected() {
        let mut state = Instrument::default();
        for i in 0..50_001 {
            assert!(state.remember(i.to_string()));
        }
        assert_eq!(state.ids.len(), 50_000);
        assert_eq!(state.fifo.len(), 50_000);
        assert!(!state.remember("50000".into()));
        assert!(state.remember("0".into()));
    }
}

// JSON object keys cannot be tuples. Store the sorted instrument map as entries.
mod instrument_entries {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        map: &BTreeMap<Key, Instrument>,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<BTreeMap<Key, Instrument>, D::Error> {
        Ok(Vec::<(Key, Instrument)>::deserialize(d)?
            .into_iter()
            .collect())
    }
}
