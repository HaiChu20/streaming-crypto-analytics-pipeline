use crate::{Book, Outcome, Result, integer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;

/// At most 100,000 deltas per unsynced connection; evict oldest, then let
/// lastUpdateId continuity reject an anchor that predates retained history.
const MAX_PENDING: usize = 100_000;

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct Sync {
    pending: VecDeque<Value>,
    last: Option<i64>,
    prev: Option<i64>,
    waiting: bool,
}

impl Sync {
    pub(crate) fn synced(&self) -> bool {
        self.last.is_some() && !self.waiting
    }
    pub fn apply(
        &mut self,
        book: &mut Book,
        data: &Value,
        snapshot: bool,
        perp: bool,
        counters: &mut crate::Counters,
    ) -> Result<Outcome> {
        if snapshot {
            if self.last.is_some() {
                counters.snapshots_ignored += 1;
                return Ok(Outcome::Ignored);
            }
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
            if self.pending.len() == MAX_PENDING {
                self.pending.pop_front();
                counters.pending_evicted += 1;
            }
            self.pending.push_back(data.clone());
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
        self.waiting = false;
        book.arrays(data, "b", "a")?;
        Ok(Outcome::Applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Counters;
    use serde_json::json;
    #[test]
    fn pending_is_bounded_and_snapshot_ignores_obsolete_buffered_deltas() {
        let mut sync = Sync::default();
        let mut book = Book::default();
        let mut counters = Counters::default();
        for id in 1..=MAX_PENDING + 1 {
            let delta = json!({"U":id,"u":id,"b":[],"a":[]});
            assert_eq!(
                sync.apply(&mut book, &delta, false, false, &mut counters)
                    .unwrap(),
                Outcome::Buffered
            );
        }
        assert_eq!(sync.pending.len(), MAX_PENDING);
        assert_eq!(sync.pending.front().unwrap()["u"], 2);
        assert_eq!(counters.pending_evicted, 1);
        let anchor = json!({"lastUpdateId":MAX_PENDING,"bids":[["10","1"]],"asks":[["11","1"]]});
        assert_eq!(
            sync.apply(&mut book, &anchor, true, false, &mut counters)
                .unwrap(),
            Outcome::Snapshot {
                buffered_updates: 1
            }
        );
        assert!(sync.pending.is_empty());
        let periodic = json!({"lastUpdateId":0,"bids":[],"asks":[]});
        assert_eq!(
            sync.apply(&mut book, &periodic, true, false, &mut counters)
                .unwrap(),
            Outcome::Ignored
        );
        assert_eq!(counters.snapshots_ignored, 1);
        assert!(book.best_bid().is_some());
    }
}
