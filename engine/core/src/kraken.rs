use crate::{
    Book, Error, Fixed, Result, Side,
    fixed::{decimal, kraken_fmt},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cell::OnceCell,
    collections::{BTreeMap, BTreeSet, HashMap},
};

pub(crate) fn l2_checksum(book: &Book) -> u32 {
    let mut text = String::new();
    for level in book
        .asks
        .values()
        .take(10)
        .chain(book.bids.values().rev().take(10))
    {
        text.push_str(&kraken_fmt(&level.price_text));
        text.push_str(&kraken_fmt(&level.size_text));
    }
    crc32fast::hash(text.as_bytes())
}

fn order_timestamp(text: &str) -> Result<String> {
    let bytes = text.as_bytes();
    let pair = |at: usize| -> Option<u8> {
        let a = *bytes.get(at)?;
        let b = *bytes.get(at + 1)?;
        if a.is_ascii_digit() && b.is_ascii_digit() {
            Some((a - b'0') * 10 + b - b'0')
        } else {
            None
        }
    };
    let valid = (22..=30).contains(&bytes.len())
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[19] == b'.'
        && bytes[bytes.len() - 1] == b'Z'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit);
    if !valid {
        return Err(Error("invalid L3 order timestamp".into()));
    }
    let year: u16 = text[..4]
        .parse()
        .map_err(|_| Error("invalid L3 order timestamp".into()))?;
    let month = pair(5).ok_or_else(|| Error("invalid L3 order timestamp".into()))?;
    let day = pair(8).ok_or_else(|| Error("invalid L3 order timestamp".into()))?;
    let hour = pair(11).ok_or_else(|| Error("invalid L3 order timestamp".into()))?;
    let minute = pair(14).ok_or_else(|| Error("invalid L3 order timestamp".into()))?;
    let second = pair(17).ok_or_else(|| Error("invalid L3 order timestamp".into()))?;
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0 || day == 0 || day > days || hour > 23 || minute > 59 || second > 59 {
        return Err(Error("invalid L3 order timestamp".into()));
    }
    if bytes.len() == 30 {
        return Ok(text.to_owned());
    }
    let mut normalized = String::with_capacity(30);
    normalized.push_str(&text[..text.len() - 1]);
    for _ in 0..30 - bytes.len() {
        normalized.push('0');
    }
    normalized.push('Z');
    Ok(normalized)
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Queue {
    price_text: String,
    #[serde(skip)]
    checksum_price: OnceCell<String>,
    orders: Vec<Order>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Order {
    id: String,
    qty: String,
    timestamp: String,
    #[serde(skip)]
    checksum_qty: OnceCell<String>,
}

/// Individual orders in timestamp/arrival order, truncated by price-level depth.
#[derive(Serialize, Deserialize, Debug)]
pub struct L3Book {
    depth: usize,
    #[serde(default)]
    side_counts: [i64; 4],
    sides: [BTreeMap<i64, Queue>; 2],
    where_: HashMap<String, (usize, i64)>,
}

impl L3Book {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            side_counts: [0; 4],
            sides: Default::default(),
            where_: HashMap::new(),
        }
    }
    pub(crate) fn side_counts(&self) -> [i64; 4] {
        self.side_counts
    }
    pub fn apply(&mut self, data: &Value, snapshot: bool) -> Result<()> {
        self.apply_counted(data, snapshot).map(|_| ())
    }
    /// Counts add/modify/delete at each event's top-ten rank, before deletes and after adds.
    pub(crate) fn apply_counted(&mut self, data: &Value, snapshot: bool) -> Result<[i64; 3]> {
        self.apply_inner(data, snapshot, None)
    }
    pub(crate) fn apply_counted_into(
        &mut self,
        data: &Value,
        snapshot: bool,
        aggregate: &mut Book,
    ) -> Result<[i64; 3]> {
        self.apply_inner(data, snapshot, Some(aggregate))
    }
    fn apply_inner(
        &mut self,
        data: &Value,
        snapshot: bool,
        aggregate: Option<&mut Book>,
    ) -> Result<[i64; 3]> {
        let mut counts = [0; 3];
        self.side_counts = [0; 4];
        let mut dirty = BTreeSet::new();
        if snapshot {
            self.sides = Default::default();
            self.where_.clear();
        }
        for (side, key) in ["bids", "asks"].into_iter().enumerate() {
            let Some(orders) = data[key].as_array() else {
                continue;
            };
            for order in orders {
                let oid = order["order_id"]
                    .as_str()
                    .ok_or_else(|| Error("missing order_id".into()))?;
                let event = if snapshot {
                    "add"
                } else {
                    order["event"].as_str().unwrap_or("")
                };
                let before = self.where_.get(oid).copied();
                let was_top = before.is_some_and(|(s, p)| self.top10(s, p));
                // An add may replace an order already inside the subscribed depth.
                // A modify also carries its new queue timestamp and can change price.
                if let Some((s, p)) = self.where_.remove(oid)
                    && let Some(queue) = self.sides[s].get_mut(&p)
                {
                    dirty.insert((s, p));
                    queue.orders.retain(|entry| entry.id != oid);
                    if queue.orders.is_empty() {
                        self.sides[s].remove(&p);
                    }
                }
                if event != "delete" {
                    let price_text = decimal(&order["limit_price"])?;
                    let price = price_text.parse::<Fixed>()?.0;
                    dirty.insert((side, price));
                    let qty = decimal(&order["order_qty"])?;
                    let timestamp = order_timestamp(
                        order["timestamp"]
                            .as_str()
                            .ok_or_else(|| Error("missing L3 order timestamp".into()))?,
                    )?;
                    let queue = self.sides[side].entry(price).or_insert_with(|| Queue {
                        price_text,
                        checksum_price: OnceCell::new(),
                        orders: Vec::new(),
                    });
                    let append = queue
                        .orders
                        .last()
                        .is_none_or(|entry| entry.timestamp <= timestamp);
                    let position = if append {
                        queue.orders.len()
                    } else {
                        queue
                            .orders
                            .partition_point(|entry| entry.timestamp <= timestamp)
                    };
                    let entry = Order {
                        id: oid.to_owned(),
                        qty,
                        timestamp,
                        checksum_qty: OnceCell::new(),
                    };
                    if append {
                        queue.orders.push(entry);
                    } else {
                        queue.orders.insert(position, entry);
                    }
                    self.where_.insert(oid.to_owned(), (side, price));
                }
                if !snapshot {
                    let is_top = self
                        .where_
                        .get(oid)
                        .is_some_and(|(s, p)| self.top10(*s, *p));
                    match event {
                        "add" if is_top => {
                            counts[0] += 1;
                            self.side_counts[side] += 1;
                        }
                        "modify" if was_top || is_top => counts[1] += 1,
                        "delete" if was_top => {
                            counts[2] += 1;
                            self.side_counts[2 + before.unwrap().0] += 1;
                        }
                        _ => {}
                    }
                }
            }
        }
        for side in 0..2 {
            while self.sides[side].len() > self.depth {
                let (price, queue) = if side == 0 {
                    self.sides[side].pop_first()
                } else {
                    self.sides[side].pop_last()
                }
                .expect("nonempty side");
                dirty.insert((side, price));
                for order in queue.orders {
                    self.where_.remove(&order.id);
                }
            }
        }
        if let Some(aggregate) = aggregate {
            if snapshot {
                *aggregate = Book::default();
            }
            for (side, price) in dirty {
                let book_side = if side == 0 { Side::Bid } else { Side::Ask };
                if let Some(queue) = self.sides[side].get(&price) {
                    let mut size = 0i64;
                    for order in &queue.orders {
                        size = size
                            .checked_add(order.qty.parse::<Fixed>()?.0)
                            .ok_or_else(|| Error("L3 level size overflow".into()))?;
                    }
                    aggregate.set_text(
                        book_side,
                        queue.price_text.clone(),
                        Fixed(size).to_string(),
                    )?;
                } else if side == 0 {
                    aggregate.bids.remove(&price);
                } else {
                    aggregate.asks.remove(&price);
                }
            }
        }
        Ok(counts)
    }
    fn top10(&self, side: usize, price: i64) -> bool {
        if side == 0 {
            self.sides[side].keys().rev().take(10).any(|p| *p == price)
        } else {
            self.sides[side].keys().take(10).any(|p| *p == price)
        }
    }
    pub fn checksum(&self) -> u32 {
        let mut hash = crc32fast::Hasher::new();
        for queue in self.sides[1]
            .values()
            .take(10)
            .chain(self.sides[0].values().rev().take(10))
        {
            let price = queue
                .checksum_price
                .get_or_init(|| kraken_fmt(&queue.price_text));
            for order in &queue.orders {
                hash.update(price.as_bytes());
                let qty = order.checksum_qty.get_or_init(|| kraken_fmt(&order.qty));
                hash.update(qty.as_bytes());
            }
        }
        hash.finalize()
    }
    pub fn orders_at(&self, side: Side, price: Fixed) -> Vec<(&str, Fixed)> {
        self.sides[if side == Side::Bid { 0 } else { 1 }]
            .get(&price.0)
            .map_or_else(Vec::new, |q| {
                q.orders
                    .iter()
                    .map(|order| {
                        (
                            order.id.as_str(),
                            order.qty.parse().expect("validated quantity"),
                        )
                    })
                    .collect()
            })
    }
    pub fn book(&self) -> Result<Book> {
        let mut book = Book::default();
        for side in 0..2 {
            for queue in self.sides[side].values() {
                let mut size = 0i64;
                for order in &queue.orders {
                    size = size
                        .checked_add(order.qty.parse::<Fixed>()?.0)
                        .ok_or_else(|| Error("L3 level size overflow".into()))?;
                }
                book.set_text(
                    if side == 0 { Side::Bid } else { Side::Ask },
                    queue.price_text.clone(),
                    Fixed(size).to_string(),
                )?;
            }
        }
        Ok(book)
    }
}

#[cfg(test)]
mod flow_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn side_event_counts_use_top_ten_and_stored_delete_side() {
        let order = |id: &str, price: i64, event: &str| {
            json!({"order_id":id,
            "limit_price":price.to_string(),"order_qty":"1","event":event,
            "timestamp":"2026-10-16T00:00:01.000000000Z"})
        };
        let mut book = L3Book::new(100);
        book.apply(
            &json!({"bids":(0..11).map(|n| order(&n.to_string(),100-n,"add")).collect::<Vec<_>>(),
            "asks":[order("ask",101,"add")]}),
            true,
        )
        .unwrap();
        assert_eq!(book.side_counts(), [0; 4]);
        book.apply(
            &json!({"bids":[order("newbid",102,"add"),order("far",1,"add")],
            "asks":[order("newask",103,"add")]}),
            false,
        )
        .unwrap();
        assert_eq!(book.side_counts(), [1, 1, 0, 0]);
        // Side in payload may differ: deletes count the stored side, as existing counts do.
        book.apply(
            &json!({"asks":[{"event":"delete","order_id":"0"},
            {"event":"delete","order_id":"ask"},{"event":"delete","order_id":"far"}]}),
            false,
        )
        .unwrap();
        assert_eq!(book.side_counts(), [0, 0, 1, 1]);
        book.apply(&json!({"bids":[order("newbid",102,"modify")]}), false)
            .unwrap();
        assert_eq!(book.side_counts(), [0; 4]);
    }
}
