use crate::{ENGINE_VERSION, Result, SECOND, row::Row};
use crate::{Fixed, Kind, Outcome, Record, Side, StreamBook};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

fn error(message: impl Into<String>) -> crate::Error {
    crate::Error(message.into())
}

/// Base currency from native Binance, OKX, Kraken spot or PF futures symbols.
pub fn asset(symbol: &str) -> Option<&str> {
    let base = if let Some(pair) = symbol.strip_prefix("PF_") {
        strip_quote(pair)?
    } else if let Some((base, _)) = symbol.split_once(['/', '-']) {
        base
    } else {
        strip_quote(symbol)?
    };
    if base.is_empty() {
        None
    } else if base == "XBT" {
        Some("BTC")
    } else {
        Some(base)
    }
}
fn strip_quote(pair: &str) -> Option<&str> {
    ["FDUSD", "USDT", "USDC", "USD", "BTC", "ETH", "EUR"]
        .into_iter()
        .find_map(|quote| pair.strip_suffix(quote).filter(|base| !base.is_empty()))
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
    liq_base: [i128; 2],
    liq_count_sides: [i64; 2],
    l3: [i64; 3],
    l3_sides: [i64; 4],
    ofi: [i128; 2],
}
impl Bucket {
    fn liquidation(&mut self, quantity: i128, buy: bool) -> Result<()> {
        let side = usize::from(buy);
        self.liq = self
            .liq
            .checked_add(quantity)
            .ok_or_else(|| error("liquidation volume overflow"))?;
        self.liq_base[side] = self.liq_base[side]
            .checked_add(quantity)
            .ok_or_else(|| error("liquidation side volume overflow"))?;
        self.liq_count += 1;
        self.liq_count_sides[side] += 1;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct L1 {
    bid: i64,
    ask: i64,
    update_id: u64,
}
#[derive(Default, Serialize, Deserialize)]
struct Instrument {
    #[serde(default)]
    l1: Option<L1>,
    l2: Option<StreamBook>,
    l3: Option<StreamBook>,
    gates: BTreeMap<String, Gate>,
    seen: BTreeSet<String>,
    ids: HashSet<String>,
    fifo: VecDeque<String>,
    #[serde(default)]
    uncertain_trades: bool,
    buckets: BTreeMap<i64, Bucket>,
    memory: crate::windows::Memory,
    last_tick: Option<i64>,
    history: VecDeque<Option<i64>>,
    oi_history: VecDeque<Option<f64>>,
    returns: VecDeque<Option<f64>>,
    first_mid: Option<i64>,
    last_recv: BTreeMap<String, i128>,
    extras_source: Option<String>,
    funding: Option<f64>,
    next_funding_ms: Option<f64>,
    mark_index_bps: Option<f64>,
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
                    if r.feed == "l1" && (crash || gate.active.as_deref() == Some(&r.conn)) {
                        self.l1 = None;
                        self.last_recv.remove("l1");
                    }
                    if (crash || gate.active.as_deref() == Some(&r.conn))
                        && matches!(r.feed.as_str(), "mark" | "ticker")
                    {
                        self.mark = None;
                        self.funding = None;
                        self.next_funding_ms = None;
                        self.mark_index_bps = None;
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
            let before = state.flow_book(r).map(crate::Book::top10);
            let factor = self
                .gates
                .get("l2")
                .and_then(|g| g.factor(r))
                .or_else(|| (r.exchange != "okx" || r.market != "perp").then_some(UNIT));
            let outcome = state.consume(r)?;
            if outcome == Outcome::Applied
                && let (Some(before), Some(after), Some(factor)) =
                    (before, state.flow_book(r), factor)
            {
                let flow = crate::windows::ofi(&before, &after.top10());
                let bucket = self
                    .buckets
                    .entry(window_tick(r.recv_ns, current_tick)?)
                    .or_default();
                for (total, value) in bucket.ofi.iter_mut().zip(flow) {
                    *total += value * i128::from(factor);
                }
            }
            let gate = self.gates.entry(r.feed.clone()).or_default();
            if r.kind == Kind::Snapshot && !matches!(outcome, Outcome::Ignored) {
                gate.accept(state.active_connection().unwrap_or(&r.conn));
            }
            if matches!(outcome, Outcome::Break(_)) && !crash && r.exchange != "binance" {
                gate.end(&r.conn);
            }
            if r.feed == "l3"
                && outcome == Outcome::Applied
                && state.active_connection() == Some(r.conn.as_str())
                && state.valid()
            {
                let counts = state.l3_event_counts().unwrap();
                let bucket = self
                    .buckets
                    .entry(window_tick(r.recv_ns, current_tick)?)
                    .or_default();
                for (total, count) in bucket
                    .l3_sides
                    .iter_mut()
                    .zip(state.l3_side_counts().unwrap())
                {
                    *total += count;
                }
                for (total, count) in bucket.l3.iter_mut().zip(counts) {
                    *total += count;
                }
            }
            return Ok(());
        }
        if r.kind == Kind::Control || r.feed == "trades_agg" {
            return Ok(());
        }
        if r.feed == "l1" {
            if r.exchange != "binance" {
                return Ok(());
            }
            let gate = self.gates.entry(r.feed.clone()).or_default();
            let same_connection = gate.active.as_deref() == Some(&r.conn);
            if !gate.accept(&r.conn) {
                return Ok(());
            }
            if !same_connection {
                self.l1 = None;
                self.last_recv.remove("l1");
            }
            let payload: Value = serde_json::from_str(&r.payload)?;
            let data = payload.get("data").unwrap_or(&payload);
            let update_id = data["u"]
                .as_u64()
                .ok_or_else(|| error("missing L1 update ID"))?;
            if self.l1.as_ref().is_some_and(|l1| update_id <= l1.update_id) {
                return Ok(());
            }
            self.l1 = Some(L1 {
                bid: fixed(&data["b"])?,
                ask: fixed(&data["a"])?,
                update_id,
            });
            if let Some(last) = self.last_recv.get_mut("l1") {
                *last = r.recv_ns;
            } else {
                self.last_recv.insert("l1".into(), r.recv_ns);
            }
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
                    ("binance", _) => vec![payload.get("data").unwrap_or(&payload)],
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
                    if !self.remember(id) || r.kind == Kind::Snapshot || qty == 0 {
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
                    if r.exchange == "kraken"
                        && r.market == "perp"
                        && trade["type"].as_str() == Some("liquidation")
                    {
                        // Kraken side is taker side; the liquidated party is the taker.
                        bucket.liquidation(volume, buy)?;
                    }
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
                let data = payload.get("data").unwrap_or(&payload);
                self.funding = number(&data["r"]);
                self.mark = number(&data["p"]);
                self.next_funding_ms = number(&data["T"]);
                self.mark_index_bps = mark_index(self.mark, number(&data["i"]));
            }
            "ticker" => {
                self.extras_source = Some("ticker".into());
                self.funding = number(&payload["relative_funding_rate"]);
                self.mark = number(&payload["markPrice"]);
                self.oi = number(&payload["openInterest"]);
                self.next_funding_ms = number(&payload["next_funding_rate_time"]);
                self.mark_index_bps = mark_index(self.mark, number(&payload["index"]));
            }
            "liquidations" => {
                let data = payload.get("data").unwrap_or(&payload);
                if !self.remember(format!("liq:{}", data["o"])) {
                    return Ok(());
                }
                let bucket = self
                    .buckets
                    .entry(window_tick(r.recv_ns, current_tick)?)
                    .or_default();
                // forceOrder is sampled; z is cumulative filled quantity of the pushed order.
                let quantity = i128::from(fixed(&data["o"]["z"])?) * i128::from(UNIT);
                let buy = match data["o"]["S"].as_str() {
                    Some("BUY") => true,
                    Some("SELL") => false,
                    _ => return Err(error("invalid forceOrder side")),
                };
                bucket.liquidation(quantity, buy)?;
            }
            _ => {}
        }
        Ok(())
    }
    /// The same validated L2 source used by features and read-only mid sampling.
    fn valid_book(&self, key: &Key, tick: i64) -> Option<(&crate::Book, i64)> {
        let state = self.l2.as_ref()?;
        if !fresh(self.last_recv.get("l2").copied(), tick) || !state.valid() {
            return None;
        }
        let book = state.book()?;
        let (bid, _) = book.best_bid()?;
        let (ask, _) = book.best_ask()?;
        if bid >= ask {
            return None;
        }
        let factor = if key.0 == "okx" && key.1 == "perp" {
            *self
                .gates
                .get("l2")?
                .factors
                .get(state.active_connection()?)?
        } else {
            UNIT
        };
        Some((book, factor))
    }
    /// Exact e9 midpoint used only by the cross-venue lead features.
    fn fast_mid(&self, key: &Key, tick: i64) -> Option<i128> {
        if key.0 == "binance"
            && fresh(self.last_recv.get("l1").copied(), tick)
            && self.gates.get("l1").is_some_and(|gate| {
                gate.active
                    .as_ref()
                    .is_some_and(|id| !gate.retired.contains(id))
            })
            && let Some(l1) = &self.l1
            && l1.bid < l1.ask
        {
            return Some((i128::from(l1.bid) + i128::from(l1.ask)) * 5);
        }
        self.valid_book(key, tick)?.0.mid().map(|mid| mid.0)
    }
    fn row(&mut self, key: &Key, tick: i64) -> Result<Row> {
        if let Some(last) = self.last_tick {
            for _ in ((last + SECOND)..tick).step_by(SECOND as usize) {
                self.oi_history.push_back(None);
                if self.oi_history.len() > 3600 {
                    self.oi_history.pop_front();
                }
                self.history.push_back(None);
                if self.history.len() > 3600 {
                    self.history.pop_front();
                }
                self.returns.push_back(None);
                if self.returns.len() > 300 {
                    self.returns.pop_front();
                }
            }
        }
        self.last_tick = Some(tick);
        let mut depth = [0; 2];
        let mut row = Row {
            exchange: key.0.clone(),
            market: key.1.clone(),
            symbol: key.2.clone(),
            asset: asset(&key.2).unwrap_or_default().into(),
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
        if let Some((book, factor)) = self.valid_book(key, tick)
            && let (Some((bid, bq)), Some((ask, aq))) = (book.best_bid(), book.best_ask())
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
                    (i128::from(ask.0) * i128::from(bq.0) + i128::from(bid.0) * i128::from(aq.0))
                        as f64
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
            depth = [total * i128::from(factor), (bid + ask) * i128::from(factor)];
            row.bid_depth10 = Some(scale(bid, factor));
            row.ask_depth10 = Some(scale(ask, factor));
            let bands = book
                .bands(mid_e9)
                .map(|side| side.map(|v| v.map(|v| scale(v, factor))));
            [
                row.depth_bid_10bps,
                row.depth_bid_25bps,
                row.depth_bid_50bps,
            ] = bands[0];
            [
                row.depth_ask_10bps,
                row.depth_ask_25bps,
                row.depth_ask_50bps,
            ] = bands[1];
            if bid + ask > 0 {
                row.imbalance10 = Some((bid - ask) as f64 / (bid + ask) as f64);
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
        let long_returns = [ret(300), ret(900), ret(1800), ret(3600)];
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
        if self.history.len() > 3600 {
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
        let liquidations = key.1 == "perp"
            && match key.0.as_str() {
                "binance" => self.seen.contains("liquidations"),
                "kraken" => self.seen.contains("trades"),
                _ => false,
            };
        if liquidations {
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
            row.funding_min_to_next = self
                .next_funding_ms
                .map(|ms| (ms - tick as f64 / 1_000_000.0) / 60000.0);
            row.mark_index_bps = self.mark_index_bps;
        }
        if fresh(self.last_recv.get("ticker").copied(), tick) {
            row.open_interest = self.oi;
        }
        for (target, lag) in [
            (&mut row.oi_chg_pct_300s, 300),
            (&mut row.oi_chg_pct_900s, 900),
            (&mut row.oi_chg_pct_3600s, 3600),
        ] {
            *target = self
                .oi_history
                .len()
                .checked_sub(lag)
                .and_then(|i| self.oi_history[i])
                .zip(row.open_interest)
                .filter(|(past, _)| *past > 0.0)
                .map(|(past, now)| 100.0 * (now - past) / past);
        }
        self.oi_history.push_back(row.open_interest);
        if self.oi_history.len() > 3600 {
            self.oi_history.pop_front();
        }
        let valid = row.book_valid && !row.warmup;
        let covered = valid && (!self.seen.contains("trades") || row.trade_count.is_some());
        let sample = crate::windows::Sample {
            valid: i64::from(valid),
            covered: i64::from(covered),
            mid: row.mid_e9.filter(|_| valid),
            depth: if valid { depth } else { [0; 2] },
            ofi: if valid { bucket.ofi } else { [0; 2] },
            volume: if covered && row.volume_base.is_some() {
                bucket.volume
            } else {
                0
            },
            flow: if covered && row.volume_base.is_some() {
                bucket.buy - bucket.sell
            } else {
                0
            },
            trades: if covered { bucket.count } else { 0 },
            unscaled: i64::from(
                covered && self.seen.contains("trades") && row.volume_base.is_none(),
            ),
            l3: if valid { bucket.l3_sides } else { [0; 4] },
            // The longer RVs sum every existing one-second return, just as
            // the legacy return history does (including warmup endpoints).
            r2: row.ret_1s.map_or(0.0, |r| r * r),
            liq_base: if covered { bucket.liq_base } else { [0; 2] },
            liq_count: if covered {
                bucket.liq_count_sides
            } else {
                [0; 2]
            },
        };
        row.memory = self.memory.update(
            tick,
            sample,
            self.seen.contains("trades"),
            self.seen.contains("l3"),
            liquidations,
        );
        for ((target, cov), ret) in [
            (&mut row.memory.ret_300s, row.memory.cov_300s),
            (&mut row.memory.ret_900s, row.memory.cov_900s),
            (&mut row.memory.ret_1800s, row.memory.cov_1800s),
            (&mut row.memory.ret_3600s, row.memory.cov_3600s),
        ]
        .into_iter()
        .zip(long_returns)
        {
            *target = cov.filter(|v| *v > 0.0).and(ret);
        }
        row.memory.microprice_dev_bps = row
            .microprice
            .zip(row.mid)
            .filter(|(_, mid)| *mid > 0.0)
            .map(|(micro, mid)| (micro - mid) / mid * 10000.0);
        Ok(row)
    }
}
fn mark_index(mark: Option<f64>, index: Option<f64>) -> Option<f64> {
    mark.zip(index)
        .filter(|(_, i)| *i > 0.0)
        .map(|(m, i)| (m - i) / i * 10000.0)
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

/// Closed minute candles; timestamps and availability remain distinct in checkpoints.
#[derive(Serialize, Deserialize)]
struct DvolCandle {
    available_ns: i128,
    close: f64,
}
#[derive(Default, Serialize, Deserialize)]
struct Dvol {
    candles: BTreeMap<i64, DvolCandle>,
}
impl Dvol {
    fn consume(&mut self, r: &Record) -> Result<()> {
        // A connection loss cannot invalidate a previously observed closed candle.
        if r.kind == Kind::Control {
            return Ok(());
        }
        let payload: Value = serde_json::from_str(&r.payload)?;
        let row = payload
            .as_array()
            .filter(|row| row.len() == 5)
            .ok_or_else(|| error("DVOL requires [timestamp,open,high,low,close]"))?;
        let ts_ms = row[0]
            .as_i64()
            .or_else(|| row[0].as_str().and_then(|v| v.parse().ok()))
            .filter(|ts| *ts >= 0 && *ts % 60_000 == 0)
            .ok_or_else(|| error("invalid DVOL candle timestamp"))?;
        let ts = ts_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| error("DVOL timestamp overflow"))?;
        if i128::from(ts) + 60 * i128::from(SECOND) > r.recv_ns {
            return Err(error("DVOL candle is not closed at receive time"));
        }
        let close = number(&row[4])
            .filter(|v| *v >= 0.0)
            .ok_or_else(|| error("invalid DVOL close"))?;
        self.candles.entry(ts).or_insert(DvolCandle {
            available_ns: r.recv_ns.max(i128::from(ts) + 70 * i128::from(SECOND)),
            close,
        });
        Ok(())
    }
    fn usable(&self, tick: i64) -> Option<f64> {
        let (ts, candle) = self
            .candles
            .range(..=tick)
            .rev()
            .find(|(_, candle)| candle.available_ns <= i128::from(tick))?;
        (tick - ts <= 180 * SECOND).then_some(candle.close)
    }
    fn fill(&mut self, row: &mut Row, realized: Option<f64>) {
        let tick = row.tick_ns;
        row.dvol = self.usable(tick);
        for (window, target) in [
            (300, &mut row.dvol_chg_300s),
            (900, &mut row.dvol_chg_900s),
            (3600, &mut row.dvol_chg_3600s),
        ] {
            *target = row
                .dvol
                .zip(self.usable(tick - window * SECOND))
                .map(|(now, then)| now - then);
        }
        row.dvol_rv_gap = row
            .dvol
            .zip(realized)
            .map(|(iv, rv)| iv - rv * (365.0_f64 * 24.0).sqrt() * 100.0);
        // Changes only need one hour plus endpoint freshness; also retain pending candles.
        self.candles
            .retain(|ts, _| *ts >= tick - (3600 + 180) * SECOND);
    }
}

/// Five leader/follower pairs, each sampled on the one-second cross-row grid.
#[derive(Default, Serialize, Deserialize)]
struct LeadHistory {
    last_tick: Option<i64>,
    gaps: [VecDeque<Option<f64>>; 5],
}
impl LeadHistory {
    fn update(&mut self, tick: i64, gaps: [Option<f64>; 5]) -> [[Option<f64>; 4]; 5] {
        let skipped = self.last_tick.map_or(0, |last| {
            ((tick - last) / SECOND - 1).clamp(0, 300) as usize
        });
        self.last_tick = Some(tick);
        let mut values = [[None; 4]; 5];
        for ((history, gap), values) in self.gaps.iter_mut().zip(gaps).zip(&mut values) {
            for _ in 0..skipped {
                if history.len() == 300 {
                    history.pop_front();
                }
                history.push_back(None);
            }
            for (target, lag) in values[2..].iter_mut().zip([1, 5]) {
                *target = gap
                    .zip(history.len().checked_sub(lag).and_then(|i| history[i]))
                    .map(|(now, past)| now - past);
            }
            if history.len() == 300 {
                history.pop_front();
            }
            history.push_back(gap);
            if gap.is_none() {
                continue;
            }
            for (target, window) in values[..2].iter_mut().zip([60, 300]) {
                let (sum, count) = history
                    .iter()
                    .rev()
                    .take(window)
                    .flatten()
                    .fold((0.0, 0), |(sum, count), value| (sum + value, count + 1));
                *target = gap
                    .filter(|_| count >= window / 2)
                    .map(|now| now - sum / count as f64);
            }
        }
        values
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Engine {
    #[serde(with = "instrument_entries")]
    instruments: BTreeMap<Key, Instrument>,
    #[serde(default)]
    dvol: BTreeMap<String, Dvol>,
    #[serde(default)]
    leads: BTreeMap<String, LeadHistory>,
    pub records: u64,
    pub book_breaks: u64,
    pub book_break_reasons: BTreeMap<String, u64>,
    pub data_anomalies: BTreeMap<String, u64>,
}
impl Engine {
    /// Per-stream continuity and checksum diagnostics, independent of freshness.
    pub fn book_reports(&self) -> BTreeMap<String, crate::BookReport> {
        self.instruments
            .iter()
            .flat_map(|(key, state)| {
                [("l2", &state.l2), ("l3", &state.l3)]
                    .into_iter()
                    .filter_map(move |(feed, book)| {
                        book.as_ref().map(|book| {
                            (
                                format!("{}/{}/{}/{}", key.0, key.1, key.2, feed),
                                crate::BookReport {
                                    valid: book.valid(),
                                    counters: book.counters(),
                                },
                            )
                        })
                    })
            })
            .collect()
    }
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
    /// Read an existing book without advancing feature histories or warmup state.
    /// Warmup is anchored to the first valid one-second feature tick.
    pub fn mid_e9(&self, key: &(String, String, String), tick: i64) -> Option<i64> {
        let state = self.instruments.get(key)?;
        if state
            .first_mid
            .is_none_or(|first| tick - first < 300 * SECOND)
        {
            return None;
        }
        let (book, _) = state.valid_book(key, tick)?;
        i64::try_from(book.mid()?.0).ok()
    }
    pub fn consume(&mut self, r: &Record) -> Result<()> {
        self.consume_inner(r, None)
    }
    pub fn consume_at_tick(&mut self, r: &Record, tick: i64) -> Result<()> {
        self.consume_inner(r, Some(tick))
    }
    fn consume_inner(&mut self, r: &Record, current_tick: Option<i64>) -> Result<()> {
        if r.exchange == "deribit" && r.market == "index" && r.feed == "dvol" {
            self.records += 1;
            if matches!(r.symbol.as_str(), "BTC" | "ETH") {
                if let Err(error) = self.dvol.entry(r.symbol.clone()).or_default().consume(r) {
                    *self
                        .data_anomalies
                        .entry(format!("deribit/index/{}/dvol: {}", r.symbol, error))
                        .or_default() += 1;
                }
            } else {
                *self
                    .data_anomalies
                    .entry("deribit/index/dvol: unknown currency".into())
                    .or_default() += 1;
            }
            return Ok(());
        }
        let key = (r.exchange.clone(), r.market.clone(), r.symbol.clone());
        if r.event.as_deref() == Some("corrupt_message") {
            *self
                .data_anomalies
                .entry(format!(
                    "{}/{}/{}/{}: corrupt_message",
                    r.exchange, r.market, r.symbol, r.feed
                ))
                .or_default() += 1;
            self.records += 1;
            return Ok(());
        }
        if r.symbol == "__unknown__" {
            // No pair can be inferred from a Kafka partition. Invalidate all
            // observed instruments on this feed rather than guessing a symbol.
            let symbols = self
                .instruments
                .keys()
                .filter(|key| key.0 == r.exchange && key.1 == r.market)
                .map(|key| key.2.clone())
                .collect::<Vec<_>>();
            if !symbols.is_empty() {
                let count = symbols.len() as u64;
                for symbol in symbols {
                    let mut targeted = r.clone();
                    targeted.symbol = symbol;
                    self.consume_inner(&targeted, current_tick)?;
                }
                self.records -= count - 1;
            } else {
                *self
                    .data_anomalies
                    .entry(format!(
                        "{}/{}/{}: malformed_envelope without symbol",
                        r.exchange, r.market, r.feed
                    ))
                    .or_default() += 1;
                self.records += 1;
            }
            return Ok(());
        }
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
                state.next_funding_ms = None;
                state.mark_index_bps = None;
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
                state.next_funding_ms = None;
                state.mark_index_bps = None;
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
            .chain(self.dvol.keys().map(String::as_str))
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let mid = |ex: &str, market: &str, asset_: &str| {
            rows.iter()
                .find(|r| {
                    r.exchange == ex
                        && r.market == market
                        && asset(&r.symbol) == Some(asset_)
                        && r.mid.is_some()
                })
                .and_then(|r| r.mid)
        };
        let mut cross = Vec::new();
        for asset_name in assets {
            let asset_ = asset_name.as_str();
            let mut row = Row {
                exchange: "cross".into(),
                market: "cross".into(),
                symbol: asset_.into(),
                asset: asset_.into(),
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
            let fast = |ex: &str, market: &str| {
                self.instruments.iter().find_map(|(key, state)| {
                    (key.0 == ex && key.1 == market && asset(&key.2) == Some(asset_))
                        .then(|| state.fast_mid(key, tick))
                        .flatten()
                })
            };
            let kp = fast("kraken", "perp");
            let ks = fast("kraken", "spot");
            let bs = fast("binance", "spot");
            let bp = fast("binance", "perp");
            let gaps = [(kp, ks), (bs, ks), (bp, ks), (kp, bs), (kp, bp)].map(|(a, b)| {
                a.zip(b)
                    .filter(|(a, b)| *a > 0 && *b > 0)
                    .map(|(a, b)| 10000.0 * (a as f64 / b as f64).ln())
            });
            let values = self
                .leads
                .entry(asset_.into())
                .or_default()
                .update(tick, gaps);
            [
                row.lead_kp_ks_gap_dev_60s,
                row.lead_kp_ks_gap_dev_300s,
                row.lead_kp_ks_gap_chg_1s,
                row.lead_kp_ks_gap_chg_5s,
            ] = values[0];
            [
                row.lead_bs_ks_gap_dev_60s,
                row.lead_bs_ks_gap_dev_300s,
                row.lead_bs_ks_gap_chg_1s,
                row.lead_bs_ks_gap_chg_5s,
            ] = values[1];
            [
                row.lead_bp_ks_gap_dev_60s,
                row.lead_bp_ks_gap_dev_300s,
                row.lead_bp_ks_gap_chg_1s,
                row.lead_bp_ks_gap_chg_5s,
            ] = values[2];
            [
                row.lead_kp_bs_gap_dev_60s,
                row.lead_kp_bs_gap_dev_300s,
                row.lead_kp_bs_gap_chg_1s,
                row.lead_kp_bs_gap_chg_5s,
            ] = values[3];
            [
                row.lead_kp_bp_gap_dev_60s,
                row.lead_kp_bp_gap_dev_300s,
                row.lead_kp_bp_gap_chg_1s,
                row.lead_kp_bp_gap_chg_5s,
            ] = values[4];
            if let Some(dvol) = self.dvol.get_mut(asset_) {
                let realized = rows
                    .iter()
                    .find(|r| {
                        r.exchange == "binance"
                            && r.market == "spot"
                            && asset(&r.symbol) == Some(asset_)
                            && r.memory.rv_3600s.is_some()
                    })
                    .and_then(|r| r.memory.rv_3600s);
                dvol.fill(&mut row, realized);
            }
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
    use serde_json::json;
    fn message(
        exchange: &str,
        market: &str,
        feed: &str,
        kind: &str,
        conn: &str,
        ns: i64,
        payload: Value,
    ) -> Record {
        Record::parse(
            &json!({"exchange":exchange,"market":market,"symbol":"BTCUSDT",
            "feed":feed,"kind":kind,"conn":conn,"seq":1,"recv_ns":ns,"payload":payload.to_string()})
            .to_string(),
        )
        .unwrap()
    }
    fn okx(kind: &str, conn: &str, ns: i64, seq: i64, prev: i64, bid: i64, qty: i64) -> Record {
        message(
            "okx",
            "spot",
            "l2",
            kind,
            conn,
            ns,
            json!({"data":[{"seqId":seq,"prevSeqId":prev,"checksum":0,
                "bids":[[bid.to_string(),qty.to_string()]],"asks":[["10000","3"]]}]}),
        )
    }
    fn l1(market: &str, conn: &str, ns: i64, id: u64, bid: &str, ask: &str) -> Record {
        message(
            "binance",
            market,
            "l1",
            "update",
            conn,
            ns,
            json!({"u":id,"b":bid,"a":ask}),
        )
    }
    fn lead_book(exchange: &str, market: &str, ns: i64, mid: i64) -> Record {
        let bid = (mid - 1).to_string();
        let ask = (mid + 1).to_string();
        let payload = if exchange == "binance" {
            json!({"lastUpdateId":1,"bids":[[bid,"1"]],"asks":[[ask,"1"]]})
        } else {
            let mut data = json!({"seq":ns / SECOND,"bids":[{"price":bid,"qty":"1"}],
                "asks":[{"price":ask,"qty":"1"}]});
            if market == "spot" {
                let mut book = crate::Book::default();
                book.objects(&data).unwrap();
                data["checksum"] = json!(crate::kraken::l2_checksum(&book));
                json!({"data":[data]})
            } else {
                data
            }
        };
        message(exchange, market, "l2", "snapshot", "B", ns, payload)
    }
    #[test]
    fn binance_l1_spot_and_futures_wrapped_and_unwrapped_exact_mid() {
        for market in ["spot", "perp"] {
            for wrapped in [false, true] {
                let mut state = Instrument::default();
                let key = ("binance".into(), market.into(), "BTCUSDT".into());
                let mut record = l1(market, "A", SECOND, 10, "100.00000000", "102.00000001");
                if market == "perp" || wrapped {
                    let mut data: Value = serde_json::from_str(&record.payload).unwrap();
                    if market == "perp" {
                        data["e"] = json!("bookTicker");
                        data["E"] = json!(123);
                        data["T"] = json!(122);
                    }
                    record.payload = if wrapped {
                        json!({"stream":"btcusdt@bookTicker","data":data}).to_string()
                    } else {
                        data.to_string()
                    };
                }
                state.consume(&record, None).unwrap();
                assert_eq!(state.fast_mid(&key, SECOND), Some(101_000_000_005));
                assert_eq!(state.last_recv.get("l1"), Some(&i128::from(SECOND)));
                assert!(state.row(&key, SECOND).unwrap().mid.is_none());
            }
        }
    }
    #[test]
    fn fast_mid_falls_back_to_valid_l2_on_stale_locked_or_crossed_l1() {
        let key = ("binance".into(), "spot".into(), "BTCUSDT".into());
        let mut state = Instrument::default();
        state
            .consume(&lead_book("binance", "spot", SECOND, 100), None)
            .unwrap();
        assert_eq!(state.fast_mid(&key, SECOND), Some(100_000_000_000));
        state
            .consume(&l1("spot", "A", SECOND, 1, "109", "111"), None)
            .unwrap();
        assert_eq!(state.fast_mid(&key, 61 * SECOND), Some(110_000_000_000));
        // Refresh L2 independently; L1 is now just beyond STALE_S.
        state
            .consume(
                &message(
                    "binance",
                    "spot",
                    "l2",
                    "update",
                    "B",
                    62 * SECOND,
                    json!({"U":2,"u":2,"b":[],"a":[]}),
                ),
                None,
            )
            .unwrap();
        assert_eq!(state.fast_mid(&key, 62 * SECOND), Some(100_000_000_000));
        for (id, bid, ask) in [(2, "110", "110"), (3, "111", "109")] {
            state
                .consume(&l1("spot", "A", 62 * SECOND, id, bid, ask), None)
                .unwrap();
            assert_eq!(state.fast_mid(&key, 62 * SECOND), Some(100_000_000_000));
        }
        state.l2.as_mut().unwrap().invalidate_malformed("B");
        assert!(state.fast_mid(&key, 62 * SECOND).is_none());
    }
    #[test]
    fn l1_connection_handover_ordering_retirement_stop_drop_and_crash() {
        let key = ("binance".into(), "spot".into(), "BTCUSDT".into());
        for (event, crash, carrying) in [
            ("stop", false, "B"),
            ("drop", false, "B"),
            ("drop", true, "C"),
            ("drop", true, "B"),
        ] {
            let mut state = Instrument::default();
            state
                .consume(&l1("spot", "A", SECOND, 100, "99", "101"), None)
                .unwrap();
            // Sequence IDs are scoped to the connection.
            state
                .consume(&l1("spot", "B", 2 * SECOND, 1, "109", "111"), None)
                .unwrap();
            for record in [
                l1("spot", "A", 3 * SECOND, 999, "199", "201"),
                l1("spot", "B", 4 * SECOND, 1, "199", "201"),
                l1("spot", "B", 5 * SECOND, 0, "199", "201"),
            ] {
                state.consume(&record, None).unwrap();
            }
            assert_eq!(state.fast_mid(&key, 5 * SECOND), Some(110_000_000_000));
            assert_eq!(state.last_recv["l1"], i128::from(2 * SECOND));
            let mut control = message(
                "binance",
                "spot",
                "l1",
                "control",
                "A",
                6 * SECOND,
                Value::Null,
            );
            control.event = Some("drop".into());
            control.reason = Some("collector_crash".into());
            state.consume(&control, None).unwrap(); // Retired crash cannot clear B.
            assert!(state.l1.is_some());
            control.reason = None;
            control.event = Some("stop".into());
            control.conn = "pending".into();
            state.consume(&control, None).unwrap(); // Inactive stop cannot clear B.
            assert!(state.l1.is_some());
            control.event = Some(event.into());
            control.reason = crash.then(|| "collector_crash".into());
            control.conn = carrying.into();
            state.consume(&control, None).unwrap();
            assert!(state.l1.is_none() && !state.last_recv.contains_key("l1"));
            assert!(state.fast_mid(&key, 6 * SECOND).is_none());
            let conn = if crash { carrying } else { "C" };
            state
                .consume(&l1("spot", conn, 7 * SECOND, 1, "119", "121"), None)
                .unwrap();
            assert_eq!(state.fast_mid(&key, 7 * SECOND), Some(120_000_000_000));
            state
                .consume(&l1("spot", "A", 8 * SECOND, 1000, "199", "201"), None)
                .unwrap();
            assert_eq!(state.fast_mid(&key, 8 * SECOND), Some(120_000_000_000));
        }
    }
    #[test]
    fn non_binance_l1_and_okx_bbo_do_not_supply_fast_mid() {
        for feed in ["l1", "bbo"] {
            let mut state = Instrument::default();
            state
                .consume(
                    &message(
                        "okx",
                        "spot",
                        feed,
                        "update",
                        "A",
                        SECOND,
                        json!({"data":[{"bidPx":"99","askPx":"101"}]}),
                    ),
                    None,
                )
                .unwrap();
            assert!(
                state
                    .fast_mid(&("okx".into(), "spot".into(), "BTC-USDT".into()), SECOND)
                    .is_none()
            );
            assert!(state.l1.is_none() && !state.last_recv.contains_key("l1"));
        }
    }
    #[test]
    fn lead_history_means_changes_half_coverage_and_skipped_seconds() {
        let mut history = LeadHistory::default();
        for t in 0..=300 {
            let values = history.update(t * SECOND, [Some(t as f64), None, None, None, None]);
            assert_eq!(
                values[0][0],
                (t >= 29).then_some(if t < 60 { t as f64 / 2.0 } else { 29.5 })
            );
            assert_eq!(
                values[0][1],
                (t >= 149).then_some(if t < 300 { t as f64 / 2.0 } else { 149.5 })
            );
            assert_eq!(values[0][2], (t >= 1).then_some(1.0));
            assert_eq!(values[0][3], (t >= 5).then_some(5.0));
            assert_eq!(values[1..], [[None; 4]; 4]);
        }
        let values = history.update(302 * SECOND, [Some(302.0), None, None, None, None]);
        assert_eq!(values[0][2..], [None, Some(5.0)]);
        assert_eq!(history.gaps[0].len(), 300);
        assert_eq!(history.gaps[0][298], None);
        assert_eq!(history.update(303 * SECOND, [None; 5]), [[None; 4]; 5]);
        let mut history = LeadHistory::default();
        for t in 0..30 {
            history.update(t * SECOND, [Some(t as f64); 5]);
        }
        let values = history.update(61 * SECOND, [Some(61.0); 5]);
        // Only 28 previous gaps plus t=61 survive in the last 60 seconds.
        assert_eq!(values, [[None; 4]; 5]);
        let values = history.update(62 * SECOND, [Some(62.0); 5]);
        assert_eq!(values[0], [None, None, Some(1.0), None]);
        assert_eq!(
            history.update(1000 * SECOND, [Some(1000.0); 5]),
            [[None; 4]; 5]
        );
        assert!(history.gaps.iter().all(|gaps| gaps.len() == 300));
    }
    #[test]
    fn cross_lead_pairs_use_fast_mids_and_only_same_asset_past_ticks() {
        let mut engine = Engine::default();
        let mut expected: [Vec<f64>; 5] = Default::default();
        for t in 0..=150 {
            for (market, mid) in [("spot", 100), ("perp", 110 + t)] {
                engine
                    .consume(&lead_book("kraken", market, t * SECOND, mid))
                    .unwrap();
            }
            for (market, mid) in [("spot", 105 + 2 * t), ("perp", 108 + t)] {
                engine
                    .consume(&l1(
                        market,
                        "A",
                        t * SECOND,
                        t as u64,
                        &(mid - 1).to_string(),
                        &(mid + 1).to_string(),
                    ))
                    .unwrap();
            }
            let mut other = l1("spot", "A", t * SECOND, t as u64, "999", "1001");
            other.symbol = "ETHUSDT".into();
            engine.consume(&other).unwrap();
            let rows = engine.tick(t * SECOND).unwrap();
            for row in &rows {
                let json = serde_json::to_value(row).unwrap();
                let columns = json
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| name.starts_with("lead_"))
                    .collect::<Vec<_>>();
                assert_eq!(columns.len(), 20);
                if row.exchange != "cross" || row.asset == "ETH" {
                    assert!(columns.iter().all(|(_, value)| value.is_null()));
                }
            }
            let cross = rows
                .iter()
                .find(|row| row.exchange == "cross" && row.asset == "BTC")
                .unwrap();
            let json = serde_json::to_value(cross).unwrap();
            let kp = (110 + t) as f64;
            let bs = (105 + 2 * t) as f64;
            let bp = (108 + t) as f64;
            for (((pair, (a, b)), history), index) in ["kp_ks", "bs_ks", "bp_ks", "kp_bs", "kp_bp"]
                .into_iter()
                .zip([(kp, 100.0), (bs, 100.0), (bp, 100.0), (kp, bs), (kp, bp)])
                .zip(&mut expected)
                .zip(0..)
            {
                let gap = 10000.0 * (a / b).ln();
                history.push(gap);
                assert_eq!(engine.leads["BTC"].gaps[index].back(), Some(&Some(gap)));
                for (suffix, window) in [("gap_dev_60s", 60), ("gap_dev_300s", 300)] {
                    let values = &history[history.len().saturating_sub(window)..];
                    let actual = json[format!("lead_{pair}_{suffix}")].as_f64();
                    if values.len() < window / 2 {
                        assert!(actual.is_none());
                    } else {
                        let mean = values.iter().sum::<f64>() / values.len() as f64;
                        assert!((actual.unwrap() - (gap - mean)).abs() < 1e-9);
                    }
                }
                for (suffix, lag) in [("gap_chg_1s", 1), ("gap_chg_5s", 5)] {
                    let actual = json[format!("lead_{pair}_{suffix}")].as_f64();
                    let expected = history.len().checked_sub(lag + 1).map(|i| gap - history[i]);
                    assert_eq!(actual, expected);
                }
            }
        }
        // A missing Kraken spot leg nulls all three features targeting it.
        let mut stop = message(
            "kraken",
            "spot",
            "l2",
            "control",
            "B",
            151 * SECOND,
            Value::Null,
        );
        stop.event = Some("stop".into());
        engine.consume(&stop).unwrap();
        let mut restored: Engine =
            serde_json::from_slice(&serde_json::to_vec(&engine).unwrap()).unwrap();
        let rows = engine.tick(151 * SECOND).unwrap();
        assert_eq!(
            serde_json::to_value(&rows).unwrap(),
            serde_json::to_value(restored.tick(151 * SECOND).unwrap()).unwrap()
        );
        let cross = serde_json::to_value(
            rows.iter()
                .find(|row| row.exchange == "cross" && row.asset == "BTC")
                .unwrap(),
        )
        .unwrap();
        for pair in ["kp_ks", "bs_ks", "bp_ks"] {
            for suffix in ["gap_dev_60s", "gap_dev_300s", "gap_chg_1s", "gap_chg_5s"] {
                assert!(cross[format!("lead_{pair}_{suffix}")].is_null());
            }
        }
        assert!(cross["lead_kp_bs_gap_dev_60s"].as_f64().is_some());
    }
    #[test]
    fn checkpoint_without_l1_and_lead_state_deserializes() {
        let mut checkpoint = crate::session::Checkpoint::new(SECOND, BTreeMap::new());
        checkpoint
            .engine
            .consume(&lead_book("binance", "spot", 0, 100))
            .unwrap();
        checkpoint.engine.tick(0).unwrap();
        let mut old = serde_json::to_value(checkpoint).unwrap();
        old["engine"].as_object_mut().unwrap().remove("leads");
        for entry in old["engine"]["instruments"].as_array_mut().unwrap() {
            entry[1].as_object_mut().unwrap().remove("l1");
        }
        let restored: crate::session::Checkpoint = serde_json::from_value(old).unwrap();
        restored.validate().unwrap();
        assert!(restored.engine.leads.is_empty());
        let (key, state) = restored.engine.instruments.first_key_value().unwrap();
        assert!(state.l1.is_none());
        assert_eq!(state.fast_mid(key, SECOND), Some(100_000_000_000));
    }
    #[test]
    fn fixture_existing_columns_are_byte_identical_with_and_without_l1() {
        let mut records = [
            include_str!("../../../tests/fixtures/binance/spot/l2.jsonl"),
            include_str!("../../../tests/fixtures/binance/spot/trades.jsonl"),
            include_str!("../../../tests/fixtures/binance/spot/l1.jsonl"),
            include_str!("../../../tests/fixtures/binance/perp/l2.jsonl"),
            include_str!("../../../tests/fixtures/binance/perp/trades.jsonl"),
            include_str!("../../../tests/fixtures/binance/perp/l1.jsonl"),
        ]
        .into_iter()
        .flat_map(str::lines)
        .map(|line| Record::parse(line).unwrap())
        .collect::<Vec<_>>();
        records.sort_by_key(|r| r.recv_ns);
        let start = window_tick(records[0].recv_ns, None).unwrap();
        let end = window_tick(records.last().unwrap().recv_ns, None).unwrap() + 301 * SECOND;
        let mut with = Engine::default();
        let mut without = Engine::default();
        let mut records = records.iter().peekable();
        for tick in (start..=end).step_by(SECOND as usize) {
            while records
                .peek()
                .is_some_and(|r| r.recv_ns <= i128::from(tick))
            {
                let record = records.next().unwrap();
                with.consume(record).unwrap();
                if record.feed != "l1" {
                    without.consume(record).unwrap();
                }
            }
            let old_columns = |rows: Vec<Row>| {
                let mut json = serde_json::to_value(rows).unwrap();
                for row in json.as_array_mut().unwrap() {
                    row.as_object_mut()
                        .unwrap()
                        .retain(|name, _| !name.starts_with("lead_"));
                }
                serde_json::to_vec(&json).unwrap()
            };
            assert_eq!(
                old_columns(with.tick(tick).unwrap()),
                old_columns(without.tick(tick).unwrap())
            );
        }
        assert!(with.instruments.values().any(|state| state.l1.is_some()));
        assert!(with.data_anomalies.is_empty(), "{:?}", with.data_anomalies);
    }
    #[test]
    fn ofi_hook_ignores_snapshot_invalid_resync_and_overlapping_connections() {
        let mut state = Instrument::default();
        state
            .consume(&okx("snapshot", "A", 1, 1, 0, 99, 2), None)
            .unwrap();
        assert!(state.buckets.is_empty());
        state
            .consume(&okx("update", "A", 2, 2, 1, 99, 5), None)
            .unwrap();
        assert_eq!(state.buckets[&SECOND].ofi, [3 * i128::from(UNIT).pow(2); 2]);
        // B is not active, even if its stream produces an update outcome.
        state
            .consume(&okx("update", "B", 3, 1, 0, 99, 100), None)
            .unwrap();
        state
            .consume(&okx("update", "A", 4, 4, 999, 99, 100), None)
            .unwrap();
        // Invalid A and replacement snapshot cannot create flow.
        state
            .consume(&okx("update", "A", 5, 5, 4, 99, 101), None)
            .unwrap();
        state
            .consume(&okx("snapshot", "C", 6, 1, 0, 100, 1000), None)
            .unwrap();
        state
            .consume(&okx("update", "A", 7, 6, 5, 99, 1000), None)
            .unwrap();
        assert_eq!(state.buckets[&SECOND].ofi, [3 * i128::from(UNIT).pow(2); 2]);
    }
    #[test]
    fn binance_sync_catchup_and_first_delta_produce_zero_ofi() {
        for buffered in [false, true] {
            let mut state = Instrument::default();
            let delta = |u, qty| {
                message(
                    "binance",
                    "spot",
                    "l2",
                    "update",
                    "A",
                    2,
                    json!({"U":u,"u":u,"b":[["99",qty]],"a":[]}),
                )
            };
            if buffered {
                state.consume(&delta(2, "5"), None).unwrap();
            }
            state
                .consume(
                    &message(
                        "binance",
                        "spot",
                        "l2",
                        "snapshot",
                        "A",
                        1,
                        json!({"lastUpdateId":1,"bids":[["99","2"]],"asks":[["101","3"]]}),
                    ),
                    None,
                )
                .unwrap();
            if !buffered {
                state.consume(&delta(2, "5"), None).unwrap();
            }
            assert!(state.buckets.is_empty());
            state.consume(&delta(3, "7"), None).unwrap();
            assert_eq!(state.buckets[&SECOND].ofi, [2 * i128::from(UNIT).pow(2); 2]);
            // Break, then catch up buffered updates under a fresh snapshot.
            state.consume(&delta(5, "100"), None).unwrap();
            state.consume(&delta(7, "1000"), None).unwrap();
            state
                .consume(
                    &message(
                        "binance",
                        "spot",
                        "l2",
                        "snapshot",
                        "A",
                        3,
                        json!({"lastUpdateId":6,"bids":[["99","2"]],"asks":[["101","3"]]}),
                    ),
                    None,
                )
                .unwrap();
            assert_eq!(state.buckets[&SECOND].ofi, [2 * i128::from(UNIT).pow(2); 2]);
        }
    }
    #[test]
    fn window_numerator_matches_sixty_message_buckets_and_contract_units() {
        let key = ("okx".into(), "perp".into(), "BTCUSDT".into());
        let mut state = Instrument {
            first_mid: Some(-1000 * SECOND),
            ..Default::default()
        };
        let mut start = okx("snapshot", "A", 1, 1, 0, 99, 1);
        start.market = "perp".into();
        let mut metadata = start.clone();
        metadata.kind = Kind::Control;
        metadata.event = Some("start".into());
        metadata.detail = Some(json!({"instrument":{"ctVal":"0.01"}}));
        state.consume(&metadata, None).unwrap();
        state.consume(&start, None).unwrap();
        let mut total = 0i128;
        let mut depth = 0i128;
        let mut normalized = None;
        for t in 1..=60 {
            let mut update = okx("update", "A", t * SECOND, t + 1, t, 99, t + 1);
            update.market = "perp".into();
            state.consume(&update, None).unwrap();
            total += state.buckets[&(t * SECOND)].ofi[0];
            depth += i128::from(t + 4) * i128::from(UNIT) * 1_000_000;
            normalized = state.row(&key, t * SECOND).unwrap().memory.ofi1_60s;
        }
        assert_eq!(total, 60 * i128::from(UNIT) * 1_000_000);
        assert_eq!(normalized, Some(2.0 * total as f64 * 60.0 / depth as f64));
    }
    #[test]
    fn long_returns_ranges_and_existing_strict_rv_with_a_gap() {
        let key = ("okx".into(), "spot".into(), "BTCUSDT".into());
        let mut state = Instrument {
            first_mid: Some(-1000 * SECOND),
            ..Default::default()
        };
        let mut last = Row::default();
        for t in 0..=3600 {
            // All snapshots: no OFI, but a known varying midpoint at every tick.
            state
                .consume(&okx("snapshot", "A", t * SECOND, t, 0, 100 + t, 2), None)
                .unwrap();
            last = state.row(&key, t * SECOND).unwrap();
        }
        let current = (3700.0 + 10000.0) / 2.0;
        for (lag, ret) in [
            (300, last.memory.ret_300s),
            (900, last.memory.ret_900s),
            (1800, last.memory.ret_1800s),
            (3600, last.memory.ret_3600s),
        ] {
            let previous = (3700.0 - lag as f64 + 10000.0) / 2.0;
            assert!((ret.unwrap() - (current / previous).ln()).abs() < 1e-15);
        }
        assert_eq!(last.memory.cov_3600s, Some(1.0));
        assert_eq!(last.memory.ofi1_10s, Some(0.0));
        let gap = state.row(&key, 3602 * SECOND).unwrap();
        assert!(gap.ret_1s.is_none() && gap.rv_60s.is_none() && gap.rv_300s.is_none());
        assert_eq!(gap.memory.cov_10s, Some(0.9));
        assert!(gap.memory.rv_3600s.unwrap() > 0.0);
    }
    #[test]
    fn coverage_masks_warmup_and_unknown_trade_seconds() {
        let key = ("okx".into(), "spot".into(), "BTCUSDT".into());
        let mut state = Instrument::default();
        state
            .consume(&okx("snapshot", "A", 0, 0, 0, 99, 2), None)
            .unwrap();
        state.seen.insert("trades".into());
        for t in 0..=310 {
            state
                .consume(&okx("snapshot", "A", t * SECOND, t, 0, 99, 2), None)
                .unwrap();
            if t == 305 {
                state
                    .buckets
                    .entry(t * SECOND)
                    .or_default()
                    .malformed_trades = true;
            }
            let row = state.row(&key, t * SECOND).unwrap();
            if t == 9 {
                assert_eq!(row.memory.cov_10s, Some(0.0));
                assert!(row.memory.vol_10s.is_none());
            }
            if t == 309 {
                assert_eq!(row.memory.cov_10s, Some(0.9));
                assert_eq!(row.memory.trades_10s, Some(0));
            }
        }
    }
    #[test]
    fn microprice_deviation_and_cross_memory_nulls() {
        let mut engine = Engine::default();
        engine
            .consume(&okx("snapshot", "A", 1, 1, 0, 100, 1))
            .unwrap();
        let rows = engine.tick(SECOND).unwrap();
        let row = rows.iter().find(|r| r.exchange == "okx").unwrap();
        let expected = (row.microprice.unwrap() - row.mid.unwrap()) / row.mid.unwrap() * 10000.0;
        assert_eq!(row.memory.microprice_dev_bps, Some(expected));
        let cross = rows.iter().find(|r| r.exchange == "cross").unwrap();
        let json = serde_json::to_value(cross).unwrap();
        for name in [
            "funding_min_to_next",
            "mark_index_bps",
            "oi_chg_pct_300s",
            "oi_chg_pct_900s",
            "oi_chg_pct_3600s",
            "depth_bid_10bps",
            "depth_bid_25bps",
            "depth_bid_50bps",
            "depth_ask_10bps",
            "depth_ask_25bps",
            "depth_ask_50bps",
        ] {
            assert!(json[name].is_null(), "cross column {name}");
        }
        assert!(
            serde_json::to_value(&cross.memory)
                .unwrap()
                .as_object()
                .unwrap()
                .values()
                .all(Value::is_null)
        );
    }
    #[test]
    fn silent_seconds_age_windows_and_uncalibrated_volume_stays_unknown() {
        let key = ("okx".into(), "perp".into(), "BTCUSDT".into());
        let mut state = Instrument {
            first_mid: Some(-1000 * SECOND),
            ..Default::default()
        };
        let mut snapshot = okx("snapshot", "A", 1, 0, 0, 99, 2);
        snapshot.market = "perp".into();
        let mut start = snapshot.clone();
        start.kind = Kind::Control;
        start.event = Some("start".into());
        start.detail = Some(json!({"instrument":{"ctVal":"0.01"}}));
        state.consume(&start, None).unwrap();
        state.consume(&snapshot, None).unwrap();
        let trade = message(
            "okx",
            "perp",
            "trades",
            "trade",
            "T",
            SECOND,
            json!({"data":[{"tradeId":"1","sz":"2","side":"buy"}]}),
        );
        state.consume(&trade, None).unwrap();
        for t in 1..=61 {
            let row = state.row(&key, t * SECOND).unwrap();
            if t == 10 {
                assert_eq!(row.memory.cov_10s, Some(1.0));
                assert_eq!(row.memory.trades_10s, Some(1));
                assert!(row.memory.vol_10s.is_none() && row.memory.flow_10s.is_none());
            }
            if t == 60 {
                assert_eq!(row.memory.cov_60s, Some(1.0));
            }
            if t == 61 {
                assert_eq!(row.memory.cov_60s, Some(59.0 / 60.0));
            }
        }
    }
    #[test]
    fn partial_rv_uses_only_existing_log_returns_and_strict_rv_stays_null() {
        let key = ("okx".into(), "spot".into(), "BTCUSDT".into());
        let mut state = Instrument {
            first_mid: Some(-1000 * SECOND),
            ..Default::default()
        };
        for t in 0..=903 {
            let mid = match t {
                900 => 110,
                901 => 99,
                _ => 100,
            };
            let payload = json!({"data":[{"seqId":t,"prevSeqId":0,"checksum":0,
                "bids":[[(mid-1).to_string(),"1"]],"asks":[[(mid+1).to_string(),"1"]]}]});
            let record = message("okx", "spot", "l2", "snapshot", "A", t * SECOND, payload);
            state.consume(&record, None).unwrap();
            if t == 902 {
                state.l2.as_mut().unwrap().invalidate_malformed("A");
            }
            let row = state.row(&key, t * SECOND).unwrap();
            if t == 903 {
                let expected = (1.1_f64.ln().powi(2) + 0.9_f64.ln().powi(2)).sqrt();
                // Reuse a fresh connection after the invalidation below.
                assert!((row.memory.rv_900s.unwrap() - expected).abs() < 1e-14);
                assert!(row.rv_60s.is_none() && row.rv_300s.is_none());
            }
            if t == 902 {
                // Replace only the source before the following snapshot.
                let replacement = message(
                    "okx",
                    "spot",
                    "l2",
                    "snapshot",
                    "B",
                    903 * SECOND,
                    json!({"data":[{"seqId":903,"prevSeqId":0,"checksum":0,"bids":[["99","1"]],"asks":[["101","1"]]}]}),
                );
                state.consume(&replacement, None).unwrap();
            }
        }
    }
    #[test]
    fn long_rv_includes_existing_warmup_returns_once_window_has_coverage() {
        let key = ("okx".into(), "spot".into(), "BTCUSDT".into());
        let mut state = Instrument::default();
        for t in 0..=900 {
            let mid = if t < 100 { 100 } else { 110 };
            state
                .consume(
                    &message(
                        "okx",
                        "spot",
                        "l2",
                        "snapshot",
                        "A",
                        t * SECOND,
                        json!({"data":[{"seqId":t,"prevSeqId":0,"checksum":0,
                "bids":[[(mid-1).to_string(),"1"]],"asks":[[(mid+1).to_string(),"1"]]}]}),
                    ),
                    None,
                )
                .unwrap();
            let row = state.row(&key, t * SECOND).unwrap();
            if t == 900 {
                assert!(row.memory.cov_900s.unwrap() > 0.0);
                assert!((row.memory.rv_900s.unwrap() - 1.1_f64.ln()).abs() < 1e-15);
            }
        }
    }
    #[test]
    fn forced_order_sides_filled_quantity_and_overlap_dedup() {
        let mut state = Instrument::default();
        for (conn, side, qty) in [
            ("A", "SELL", "0.1"),
            ("B", "SELL", "0.1"),
            ("B", "BUY", "0.2"),
            ("A", "BUY", "10"),
        ] {
            state
                .consume(
                    &message(
                        "binance",
                        "perp",
                        "liquidations",
                        "update",
                        conn,
                        SECOND,
                        json!({"o":{"S":side,"z":qty,"q":"100"}}),
                    ),
                    None,
                )
                .unwrap();
        }
        let bucket = &state.buckets[&SECOND];
        assert_eq!(bucket.liq_count_sides, [1, 1]);
        assert_eq!(
            bucket.liq_base,
            [1_000_000_000_000_000, 2_000_000_000_000_000]
        );
        let row = state
            .row(&("binance".into(), "perp".into(), "BTCUSDT".into()), SECOND)
            .unwrap();
        assert_eq!(row.liq_count_1s, Some(2));
        assert_eq!(row.liq_base_1s, Some(0.3));
    }
    #[test]
    fn binance_zero_quantity_trades_seed_dedup_without_flow() {
        for maker in [false, true] {
            let mut state = Instrument::default();
            let trade = |id, price, qty, maker, kind| {
                message(
                    "binance",
                    "perp",
                    "trades",
                    "trade",
                    "A",
                    SECOND,
                    json!({"t":id,"p":price,"q":qty,"m":maker,"X":kind}),
                )
            };
            for record in [
                trade(1, "100", "2.5", false, "MARKET"),
                trade(2, "0", "0", maker, "NA"),
                trade(3, "100", "1.25", true, "MARKET"),
                trade(2, "0", "0", maker, "NA"),
                // Even a positive-quantity replay must find the placeholder's ID.
                trade(2, "100", "100", maker, "MARKET"),
            ] {
                state.consume(&record, None).unwrap();
            }
            assert_eq!(state.ids.len(), 3);
            assert_eq!(state.fifo.len(), 3);
            assert!(state.ids.contains("2"));
            let row = state
                .row(&("binance".into(), "perp".into(), "BTCUSDT".into()), SECOND)
                .unwrap();
            assert_eq!(row.trade_count, Some(2));
            assert_eq!(row.volume_base, Some(3.75));
            assert_eq!(row.taker_buy_base, Some(2.5));
            assert_eq!(row.taker_sell_base, Some(1.25));
        }
    }
    #[test]
    fn zero_quantity_trades_skip_other_venues_and_liquidations() {
        for (exchange, market, payload) in [
            (
                "okx",
                "spot",
                json!({"data":[{"tradeId":"1","sz":"0","side":"buy"}]}),
            ),
            (
                "okx",
                "perp",
                json!({"data":[{"tradeId":"1","sz":"0","side":"sell"}]}),
            ),
            (
                "kraken",
                "spot",
                json!({"data":[{"trade_id":1,"qty":"0","side":"buy"}]}),
            ),
            (
                "kraken",
                "perp",
                json!({"uid":"1","qty":"0","side":"sell","type":"liquidation"}),
            ),
        ] {
            let mut state = Instrument::default();
            state
                .consume(
                    &message(exchange, market, "trades", "trade", "A", SECOND, payload),
                    None,
                )
                .unwrap();
            assert_eq!(state.ids.len(), 1, "{exchange}/{market}");
            assert!(state.buckets.is_empty(), "{exchange}/{market}");
        }
    }
    #[test]
    fn kraken_liquidation_is_forced_taker_side_and_still_a_trade() {
        let mut state = Instrument::default();
        let trade = |conn, id, side, qty, kind| {
            message(
                "kraken",
                "perp",
                "trades",
                "trade",
                conn,
                SECOND,
                json!({"uid":id,"qty":qty,"side":side,"type":kind}),
            )
        };
        state.consume(&message("kraken", "perp", "trades", "snapshot", "A", SECOND,
            json!({"trades":[{"uid":"old","qty":"100","side":"sell","type":"liquidation"}]})), None).unwrap();
        for r in [
            trade("A", "old", "sell", "100", "liquidation"),
            trade("A", "1", "sell", "2.5", "liquidation"),
            trade("B", "1", "sell", "2.5", "liquidation"),
            trade("B", "2", "buy", "1.25", "liquidation"),
            trade("B", "3", "buy", "3", "fill"),
        ] {
            state.consume(&r, None).unwrap();
        }
        assert_eq!(state.buckets[&SECOND].liq_count_sides, [1, 1]);
        let key = ("kraken".into(), "perp".into(), "PF_XBTUSD".into());
        let row = state.row(&key, SECOND).unwrap();
        assert_eq!(row.trade_count, Some(3));
        assert_eq!(row.volume_base, Some(6.75));
        assert_eq!(row.taker_buy_base, Some(4.25));
        assert_eq!(row.taker_sell_base, Some(2.5));
        assert_eq!(row.liq_count_1s, Some(2));
        assert_eq!(row.liq_base_1s, Some(3.75));
        assert_eq!(state.row(&key, 2 * SECOND).unwrap().liq_count_1s, Some(0));
        for (ex, market) in [("kraken", "spot"), ("okx", "perp"), ("binance", "spot")] {
            let row = state
                .row(&(ex.into(), market.into(), "BTCUSDT".into()), 3 * SECOND)
                .unwrap();
            assert!(row.liq_count_1s.is_none() && row.memory.liq_sell_count_10s.is_none());
        }
    }
    #[test]
    fn kraken_ticker_uses_relative_funding_and_clears_when_missing() {
        let key = ("kraken".into(), "perp".into(), "PF_XBTUSD".into());
        let mut state = Instrument::default();
        // Rate fields from archive/KrakenRaw/Futurev6/PF_XBTUSD/20261008_ticker.jsonl.gz.
        let mut payload = json!({
            "funding_rate":"1.3911361257310142",
            "relative_funding_rate":"0.000016940145833333"
        });
        state
            .consume(
                &message(
                    "kraken",
                    "perp",
                    "ticker",
                    "update",
                    "A",
                    SECOND,
                    payload.clone(),
                ),
                None,
            )
            .unwrap();
        assert_eq!(
            state.row(&key, SECOND).unwrap().funding_rate,
            Some(0.000016940145833333)
        );
        payload
            .as_object_mut()
            .unwrap()
            .remove("relative_funding_rate");
        state
            .consume(
                &message(
                    "kraken",
                    "perp",
                    "ticker",
                    "update",
                    "A",
                    2 * SECOND,
                    payload,
                ),
                None,
            )
            .unwrap();
        let row = state.row(&key, 2 * SECOND).unwrap();
        assert_eq!(row.funding_rate, None);
        assert!(serde_json::to_value(row).unwrap()["funding_rate"].is_null());
    }
    #[test]
    fn funding_countdown_same_message_gap_and_freshness() {
        for (ex, feed) in [("binance", "mark"), ("kraken", "ticker")] {
            let key = (ex.into(), "perp".into(), "BTCUSDT".into());
            let mut state = Instrument::default();
            let payload = if ex == "binance" {
                json!({"data":{"r":"0.1","p":"101","i":"100","T":120000}})
            } else {
                json!({"relative_funding_rate":"0.1","markPrice":"101","index":"100","next_funding_rate_time":120000})
            };
            state
                .consume(
                    &message(ex, "perp", feed, "update", "A", SECOND, payload),
                    None,
                )
                .unwrap();
            let row = state.row(&key, SECOND).unwrap();
            assert_eq!(row.funding_min_to_next, Some(119.0 / 60.0));
            assert_eq!(row.mark_index_bps, Some(100.0));
            assert_eq!(
                state.row(&key, 61 * SECOND).unwrap().funding_min_to_next,
                Some(59.0 / 60.0)
            );
            let row = state.row(&key, 62 * SECOND).unwrap();
            assert!(
                row.funding_rate.is_none()
                    && row.funding_min_to_next.is_none()
                    && row.mark_index_bps.is_none()
            );
            // New mark without index or funding time cannot reuse an older message's fields.
            let payload = if ex == "binance" {
                json!({"p":"102"})
            } else {
                json!({"markPrice":"102"})
            };
            state
                .consume(
                    &message(ex, "perp", feed, "update", "B", 63 * SECOND, payload),
                    None,
                )
                .unwrap();
            let row = state.row(&key, 63 * SECOND).unwrap();
            assert!(row.mark_index_bps.is_none() && row.funding_min_to_next.is_none());
            let payload = if ex == "binance" {
                json!({"p":"101","i":"100","T":120000})
            } else {
                json!({"markPrice":"101","index":"100","next_funding_rate_time":120000})
            };
            state
                .consume(
                    &message(ex, "perp", feed, "update", "B", 64 * SECOND, payload),
                    None,
                )
                .unwrap();
            let mut stop = message(ex, "perp", feed, "control", "B", 65 * SECOND, Value::Null);
            stop.event = Some("stop".into());
            state.consume(&stop, None).unwrap();
            let row = state.row(&key, 65 * SECOND).unwrap();
            assert!(row.mark_index_bps.is_none() && row.funding_min_to_next.is_none());
        }
        assert_eq!(mark_index(Some(100.0), Some(0.0)), None);
    }
    #[test]
    fn oi_changes_require_fresh_endpoints_but_allow_interior_gaps() {
        let key = ("kraken".into(), "perp".into(), "PF_XBTUSD".into());
        let mut state = Instrument::default();
        for t in 0..=3600 {
            // Endpoint at t=100 expires; fresh carried OI at t=300 is from t=299.
            if t != 300 && (t % 60 == 0 || t == 299) && !(60..=180).contains(&t) {
                state
                    .consume(
                        &message(
                            "kraken",
                            "perp",
                            "ticker",
                            "update",
                            "A",
                            t * SECOND,
                            json!({"openInterest":1000+t}),
                        ),
                        None,
                    )
                    .unwrap();
            }
            let row = state.row(&key, t * SECOND).unwrap();
            if t == 299 {
                assert!(row.oi_chg_pct_300s.is_none());
            }
            if t == 300 {
                assert_eq!(row.oi_chg_pct_300s, Some(29.9));
            }
            if t == 400 {
                assert!(row.oi_chg_pct_300s.is_none());
            }
            if t == 900 {
                assert_eq!(row.oi_chg_pct_900s, Some(90.0));
            }
            if t == 3600 {
                assert_eq!(row.oi_chg_pct_3600s, Some(360.0));
            }
        }
        let row = state.row(&key, 3661 * SECOND).unwrap();
        assert!(row.oi_chg_pct_300s.is_none() && row.oi_chg_pct_3600s.is_none());
        // Skipped wall-clock ticks are missing endpoints, never carried over a gap.
        state
            .consume(
                &message(
                    "kraken",
                    "perp",
                    "ticker",
                    "update",
                    "A",
                    3961 * SECOND,
                    json!({"openInterest":5000}),
                ),
                None,
            )
            .unwrap();
        assert!(
            state
                .row(&key, 3961 * SECOND)
                .unwrap()
                .oi_chg_pct_300s
                .is_none()
        );
    }
    #[test]
    fn zero_or_missing_oi_endpoint_is_unknown() {
        let key = ("kraken".into(), "perp".into(), "PF_XBTUSD".into());
        let mut state = Instrument::default();
        for t in 0..=301 {
            let payload = if t == 0 {
                json!({"openInterest":0})
            } else if t == 1 {
                json!({})
            } else {
                json!({"openInterest":100})
            };
            state
                .consume(
                    &message(
                        "kraken",
                        "perp",
                        "ticker",
                        "update",
                        "A",
                        t * SECOND,
                        payload,
                    ),
                    None,
                )
                .unwrap();
            let row = state.row(&key, t * SECOND).unwrap();
            assert!(row.oi_chg_pct_300s.is_none());
        }
        assert_eq!(
            state.row(&key, 302 * SECOND).unwrap().oi_chg_pct_300s,
            Some(0.0)
        );
    }
    #[test]
    fn bands_scale_okx_contracts_and_remain_null_without_calibration_or_valid_book() {
        let key = ("okx".into(), "perp".into(), "BTCUSDT".into());
        let mut state = Instrument::default();
        let r = message(
            "okx",
            "perp",
            "l2",
            "snapshot",
            "A",
            SECOND,
            json!({"data":[{"seqId":1,"prevSeqId":0,"checksum":0,
            "bids":[["99.99","2"],["99.9","3"],["99","100"]],
            "asks":[["100.01","4"],["100.1","5"],["101","200"]]}]}),
        );
        state.consume(&r, None).unwrap();
        assert!(state.row(&key, SECOND).unwrap().depth_bid_10bps.is_none());
        let mut metadata = r.clone();
        metadata.kind = Kind::Control;
        metadata.event = Some("start".into());
        metadata.detail = Some(json!({"instrument":{"ctVal":"0.01"}}));
        state.consume(&metadata, None).unwrap();
        state.consume(&r, None).unwrap();
        let row = state.row(&key, 2 * SECOND).unwrap();
        assert_eq!(row.depth_bid_10bps, Some(0.05));
        assert_eq!(row.depth_ask_50bps, Some(0.09));
        assert_eq!(row.bid_depth10, Some(1.05));
        state.l2.as_mut().unwrap().invalidate_malformed("A");
        assert!(
            state
                .row(&key, 3 * SECOND)
                .unwrap()
                .depth_ask_10bps
                .is_none()
        );
    }
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

#[cfg(test)]
#[path = "dvol_tests.rs"]
mod dvol_tests;
