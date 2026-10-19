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
    /// Allocation-free rank snapshot for message-level OFI.
    pub(crate) fn top10(&self) -> [[Option<(i64, i64)>; 10]; 2] {
        let mut levels = [[None; 10]; 2];
        for (slot, (price, level)) in levels[0].iter_mut().zip(self.bids.iter().rev()) {
            *slot = Some((*price, level.size));
        }
        for (slot, (price, level)) in levels[1].iter_mut().zip(&self.asks) {
            *slot = Some((*price, level.size));
        }
        levels
    }
    /// Native size within each band. A side must extend strictly beyond its edge.
    /// Integer comparisons retain half-atom midpoints and exact boundary inclusion.
    pub(crate) fn bands(&self, mid_e9: i64) -> [[Option<i128>; 3]; 2] {
        let mut result = [[None; 3]; 2];
        for (side, levels) in [&self.bids, &self.asks].into_iter().enumerate() {
            for (i, bps) in [10, 25, 50].into_iter().enumerate() {
                let edge = i128::from(mid_e9) * (10000 + if side == 0 { -bps } else { bps });
                let price = |p: i64| i128::from(p) * 10 * 10000;
                let extends = if side == 0 {
                    levels
                        .first_key_value()
                        .is_some_and(|(p, _)| price(*p) < edge)
                } else {
                    levels
                        .last_key_value()
                        .is_some_and(|(p, _)| price(*p) > edge)
                };
                if extends {
                    let size = if side == 0 {
                        levels
                            .iter()
                            .rev()
                            .take_while(|(p, _)| price(**p) >= edge)
                            .map(|(_, l)| i128::from(l.size))
                            .sum()
                    } else {
                        levels
                            .iter()
                            .take_while(|(p, _)| price(**p) <= edge)
                            .map(|(_, l)| i128::from(l.size))
                            .sum()
                    };
                    result[side][i] = Some(size);
                }
            }
        }
        result
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

#[cfg(test)]
mod band_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn band_edges_include_boundary_but_require_a_level_strictly_beyond_it() {
        let mut book = Book::default();
        for (side, levels) in [
            (
                Side::Bid,
                [("99.99", "1"), ("99.9", "2"), ("99.75", "4"), ("99.5", "8")],
            ),
            (
                Side::Ask,
                [
                    ("100.01", "3"),
                    ("100.1", "5"),
                    ("100.25", "7"),
                    ("100.5", "9"),
                ],
            ),
        ] {
            for (p, q) in levels {
                book.set(side, &json!(p), &json!(q)).unwrap();
            }
        }
        let unit = 100_000_000i128;
        assert_eq!(
            book.bands(100_000_000_000),
            [
                [Some(3 * unit), Some(7 * unit), None],
                [Some(8 * unit), Some(15 * unit), None]
            ]
        );
        // Each side is assessed independently; strictly beyond the 50bps edge.
        book.set(Side::Bid, &json!("99.49"), &json!("100")).unwrap();
        assert_eq!(book.bands(100_000_000_000)[0][2], Some(15 * unit));
        assert_eq!(book.bands(100_000_000_000)[1][2], None);
        let mut truncated = Book::default();
        truncated
            .set(Side::Bid, &json!("99.99"), &json!("1"))
            .unwrap();
        truncated
            .set(Side::Ask, &json!("100.01"), &json!("1"))
            .unwrap();
        assert_eq!(truncated.bands(100_000_000_000), [[None; 3]; 2]);
    }
}
