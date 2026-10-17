use crate::{Fixed, Mid, Result, fixed::decimal};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Level {
    pub size: i64,
    pub price_text: String,
    pub size_text: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Book {
    pub(crate) bids: BTreeMap<i64, Level>,
    pub(crate) asks: BTreeMap<i64, Level>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Book {
    pub fn best_bid(&self) -> Option<(Fixed, Fixed)> {
        self.bids
            .last_key_value()
            .map(|(p, l)| (Fixed(*p), Fixed(l.size)))
    }
    pub fn best_ask(&self) -> Option<(Fixed, Fixed)> {
        self.asks
            .first_key_value()
            .map(|(p, l)| (Fixed(*p), Fixed(l.size)))
    }
    pub fn top(&self, side: Side, n: usize) -> Vec<(Fixed, Fixed)> {
        match side {
            Side::Bid => self
                .bids
                .iter()
                .rev()
                .take(n)
                .map(|(p, l)| (Fixed(*p), Fixed(l.size)))
                .collect(),
            Side::Ask => self
                .asks
                .iter()
                .take(n)
                .map(|(p, l)| (Fixed(*p), Fixed(l.size)))
                .collect(),
        }
    }
    pub fn mid(&self) -> Option<Mid> {
        Some(Mid((i128::from(self.best_bid()?.0.0)
            + i128::from(self.best_ask()?.0.0))
            * 5))
    }
    pub(crate) fn set(&mut self, side: Side, price: &Value, size: &Value) -> Result<()> {
        let price_text = decimal(price)?;
        let size_text = decimal(size)?;
        self.set_text(side, price_text, size_text)
    }
    pub(crate) fn set_text(
        &mut self,
        side: Side,
        price_text: String,
        size_text: String,
    ) -> Result<()> {
        let price = price_text.parse::<Fixed>()?.0;
        let size = size_text.parse::<Fixed>()?.0;
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if size == 0 {
            levels.remove(&price);
        } else if let Some(level) = levels.get_mut(&price) {
            // Python Decimal dictionary keys retain their first inserted representation.
            level.size = size;
            level.size_text = size_text;
        } else {
            levels.insert(
                price,
                Level {
                    size,
                    price_text,
                    size_text,
                },
            );
        }
        Ok(())
    }
    pub(crate) fn arrays(&mut self, data: &Value, bid: &str, ask: &str) -> Result<()> {
        for (key, side) in [(bid, Side::Bid), (ask, Side::Ask)] {
            if let Some(levels) = data[key].as_array() {
                for level in levels {
                    self.set(side, &level[0], &level[1])?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn objects(&mut self, data: &Value) -> Result<()> {
        for (key, side) in [("bids", Side::Bid), ("asks", Side::Ask)] {
            if let Some(levels) = data[key].as_array() {
                for level in levels {
                    self.set(side, &level["price"], &level["qty"])?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn truncate(&mut self, depth: usize) {
        while self.bids.len() > depth {
            self.bids.pop_first();
        }
        while self.asks.len() > depth {
            self.asks.pop_last();
        }
    }
}
