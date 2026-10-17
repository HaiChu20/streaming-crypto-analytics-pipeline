use crate::{Book, Outcome, Result, integer};
use serde_json::Value;

#[derive(Debug, Default)]
pub(crate) struct Sync {
    pending: Vec<Value>,
    last: Option<i64>,
    prev: Option<i64>,
    waiting: bool,
}

impl Sync {
    pub fn apply(
        &mut self,
        book: &mut Book,
        data: &Value,
        snapshot: bool,
        perp: bool,
    ) -> Result<Outcome> {
        if snapshot {
            let last = integer(data, "lastUpdateId")?;
            let events = std::mem::take(&mut self.pending)
                .into_iter()
                .filter(|d| {
                    d["u"]
                        .as_i64()
                        .is_some_and(|u| if perp { u >= last } else { u > last })
                })
                .collect::<Vec<_>>();
            self.last = Some(last);
            self.prev = None;
            self.waiting = true;
            *book = Book::default();
            book.arrays(data, "bids", "asks")?;
            let mut applied = 0;
            for event in events {
                let u = integer(&event, "u")?;
                if perp {
                    let result = self.delta(book, &event, true)?;
                    if matches!(result, Outcome::Stale | Outcome::Break(_)) {
                        self.last = None;
                        self.prev = None;
                        return Ok(Outcome::Stale);
                    }
                } else {
                    let first = integer(&event, "U")?;
                    let local = self.last.unwrap();
                    if i128::from(first) > i128::from(local) + 1 {
                        self.last = None;
                        return Ok(Outcome::Stale);
                    }
                    book.arrays(&event, "b", "a")?;
                    self.last = Some(local.max(u));
                    self.waiting = false;
                }
                applied += 1;
            }
            Ok(Outcome::Snapshot {
                buffered_updates: applied,
            })
        } else if self.last.is_none() {
            // Validate continuity IDs before buffering the complete delta.
            integer(data, "U")?;
            integer(data, "u")?;
            if perp {
                integer(data, "pu")?;
            }
            self.pending.push(data.clone());
            Ok(Outcome::Buffered)
        } else {
            let outcome = self.delta(book, data, perp)?;
            Ok(if outcome == Outcome::Stale {
                Outcome::gap(
                    self.last.unwrap().saturating_add(i64::from(!perp)),
                    integer(data, "U")?,
                )
            } else {
                outcome
            })
        }
    }

    fn delta(&mut self, book: &mut Book, data: &Value, perp: bool) -> Result<Outcome> {
        let first = integer(data, "U")?;
        let end = integer(data, "u")?;
        let last = self.last.expect("snapshot present");
        if perp {
            if let Some(previous) = self.prev {
                let pu = integer(data, "pu")?;
                if pu != previous {
                    return Ok(Outcome::gap(previous, pu));
                }
            } else {
                if end < last {
                    return Ok(Outcome::Ignored);
                }
                if !(first <= last && last <= end) {
                    return Ok(Outcome::Stale);
                }
            }
            self.prev = Some(end);
        } else {
            if if self.waiting {
                end <= last
            } else {
                end < last
            } {
                return Ok(Outcome::Ignored);
            }
            if i128::from(first) > i128::from(last) + 1 {
                return Ok(if self.waiting {
                    Outcome::Stale
                } else {
                    Outcome::gap(last.saturating_add(1), first)
                });
            }
            self.waiting = false;
            self.last = Some(end);
        }
        book.arrays(data, "b", "a")?;
        Ok(Outcome::Applied)
    }
}
