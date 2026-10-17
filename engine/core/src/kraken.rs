use crate::{
    Book, Error, Fixed, Result, Side,
    fixed::{decimal, kraken_fmt},
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

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

#[derive(Debug, Default)]
struct Queue {
    price_text: String,
    orders: Vec<(String, String)>,
}

/// Individual orders in insertion/queue order, truncated by price-level depth.
#[derive(Debug)]
pub struct L3Book {
    depth: usize,
    sides: [BTreeMap<i64, Queue>; 2],
    where_: HashMap<String, (usize, i64)>,
}

impl L3Book {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            sides: Default::default(),
            where_: HashMap::new(),
        }
    }
    pub fn apply(&mut self, data: &Value, snapshot: bool) -> Result<()> {
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
                if event == "delete" {
                    let fallback = if order["limit_price"].is_null() {
                        0
                    } else {
                        decimal(&order["limit_price"])?.parse::<Fixed>()?.0
                    };
                    let (s, p) = self.where_.remove(oid).unwrap_or((side, fallback));
                    if let Some(queue) = self.sides[s].get_mut(&p) {
                        queue.orders.retain(|(id, _)| id != oid);
                        if queue.orders.is_empty() {
                            self.sides[s].remove(&p);
                        }
                    }
                } else if event == "modify" && self.where_.contains_key(oid) {
                    let (s, p) = self.where_[oid];
                    let qty = decimal(&order["order_qty"])?;
                    let queue = self.sides[s].get_mut(&p).expect("indexed order");
                    queue
                        .orders
                        .iter_mut()
                        .find(|(id, _)| id == oid)
                        .expect("indexed order")
                        .1 = qty;
                } else {
                    let price_text = decimal(&order["limit_price"])?;
                    let price = price_text.parse::<Fixed>()?.0;
                    let qty = decimal(&order["order_qty"])?;
                    let queue = self.sides[side].entry(price).or_insert_with(|| Queue {
                        price_text,
                        orders: Vec::new(),
                    });
                    if let Some((_, size)) = queue.orders.iter_mut().find(|(id, _)| id == oid) {
                        *size = qty;
                    } else {
                        queue.orders.push((oid.to_owned(), qty));
                    }
                    self.where_.insert(oid.to_owned(), (side, price));
                }
            }
        }
        for side in 0..2 {
            while self.sides[side].len() > self.depth {
                let (_, queue) = if side == 0 {
                    self.sides[side].pop_first()
                } else {
                    self.sides[side].pop_last()
                }
                .expect("nonempty side");
                for (oid, _) in queue.orders {
                    self.where_.remove(&oid);
                }
            }
        }
        Ok(())
    }
    pub fn checksum(&self) -> u32 {
        let mut text = String::new();
        for queue in self.sides[1]
            .values()
            .take(10)
            .chain(self.sides[0].values().rev().take(10))
        {
            for (_, qty) in &queue.orders {
                text.push_str(&kraken_fmt(&queue.price_text));
                text.push_str(&kraken_fmt(qty));
            }
        }
        crc32fast::hash(text.as_bytes())
    }
    pub fn orders_at(&self, side: Side, price: Fixed) -> Vec<(&str, Fixed)> {
        self.sides[if side == Side::Bid { 0 } else { 1 }]
            .get(&price.0)
            .map_or_else(Vec::new, |q| {
                q.orders
                    .iter()
                    .map(|(id, qty)| (id.as_str(), qty.parse().expect("validated quantity")))
                    .collect()
            })
    }
    pub fn book(&self) -> Result<Book> {
        let mut book = Book::default();
        for side in 0..2 {
            for queue in self.sides[side].values() {
                let mut size = 0i64;
                for (_, qty) in &queue.orders {
                    size = size
                        .checked_add(qty.parse::<Fixed>()?.0)
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
