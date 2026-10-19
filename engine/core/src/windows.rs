//! Exact integer window sums and bounded, deterministic rolling extrema.
use crate::SECOND;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const WINDOWS: [usize; 6] = [10, 60, 300, 900, 1800, 3600];
const BASE_SCALE: f64 = 10_000_000_000_000_000.0;

/// Prices and sizes have eight implied decimals; OFI remains in size atoms.
pub(crate) type Levels = [[Option<(i64, i64)>; 10]; 2];
pub(crate) fn ofi(before: &Levels, after: &Levels) -> [i128; 2] {
    let mut result = [0; 2];
    for i in 0..10 {
        let mut e = 0;
        for side in 0..2 {
            if let (Some((p, q)), Some((next_p, next_q))) = (before[side][i], after[side][i]) {
                if side == 0 {
                    e += i128::from(next_q) * i128::from(next_p >= p)
                        - i128::from(q) * i128::from(next_p <= p);
                } else {
                    e += i128::from(q) * i128::from(next_p >= p)
                        - i128::from(next_q) * i128::from(next_p <= p);
                }
            }
        }
        if i == 0 {
            result[0] = e;
        }
        result[1] += e;
    }
    result
}

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct Sample {
    pub covered: i64,
    pub valid: i64,
    pub volume: i128,
    pub flow: i128,
    pub trades: i64,
    pub unscaled: i64,
    pub ofi: [i128; 2],
    /// Twice the mean side depth, in the same calibrated units as OFI.
    pub depth: [i128; 2],
    pub l3: [i64; 4],
    pub mid: Option<i64>,
    pub r2: f64,
    pub liq_base: [i128; 2], // sell, buy; calibrated 1e-16 base units
    pub liq_count: [i64; 2],
}
#[derive(Default, Serialize, Deserialize)]
struct Window {
    sum: Sample,
    r2_correction: f64,
    nonzero_returns: i64,
    min: VecDeque<(i64, i64)>,
    max: VecDeque<(i64, i64)>,
}
impl Window {
    fn add(&mut self, sample: Sample, sign: i64) {
        self.sum.covered += sign * sample.covered;
        self.sum.valid += sign * sample.valid;
        self.sum.volume += i128::from(sign) * sample.volume;
        self.sum.flow += i128::from(sign) * sample.flow;
        self.sum.trades += sign * sample.trades;
        self.sum.unscaled += sign * sample.unscaled;
        let adjusted = sign as f64 * sample.r2 - self.r2_correction;
        let next = self.sum.r2 + adjusted;
        self.r2_correction = (next - self.sum.r2) - adjusted;
        self.sum.r2 = next;
        self.nonzero_returns += sign * i64::from(sample.r2 != 0.0);
        if self.nonzero_returns == 0 {
            self.sum.r2 = 0.0;
            self.r2_correction = 0.0;
        }
        for i in 0..2 {
            self.sum.ofi[i] += i128::from(sign) * sample.ofi[i];
            self.sum.depth[i] += i128::from(sign) * sample.depth[i];
            self.sum.liq_base[i] += i128::from(sign) * sample.liq_base[i];
            self.sum.liq_count[i] += sign * sample.liq_count[i];
        }
        for i in 0..4 {
            self.sum.l3[i] += sign * sample.l3[i];
        }
    }
    fn push(&mut self, tick: i64, sample: Sample, width: usize) {
        let expired = tick - width as i64 * SECOND;
        while self.min.front().is_some_and(|(t, _)| *t <= expired) {
            self.min.pop_front();
        }
        while self.max.front().is_some_and(|(t, _)| *t <= expired) {
            self.max.pop_front();
        }
        if let Some(mid) = sample.mid {
            while self.min.back().is_some_and(|(_, m)| *m >= mid) {
                self.min.pop_back();
            }
            while self.max.back().is_some_and(|(_, m)| *m <= mid) {
                self.max.pop_back();
            }
            self.min.push_back((tick, mid));
            self.max.push_back((tick, mid));
        }
        self.add(sample, 1);
    }
}
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Memory {
    ring: VecDeque<Sample>,
    windows: [Window; 6],
    first_tick: Option<i64>,
    last_tick: Option<i64>,
}
#[derive(Default, Clone, Copy)]
struct Values {
    cov: Option<f64>,
    vol: Option<f64>,
    flow: Option<f64>,
    trades: Option<i64>,
    ofi1: Option<f64>,
    ofi10: Option<f64>,
    range: Option<f64>,
    l3: [Option<i64>; 4],
    rv: Option<f64>,
    liq_base: [Option<f64>; 2],
    liq_count: [Option<i64>; 2],
}
impl Memory {
    fn push(&mut self, tick: i64, sample: Sample) {
        self.first_tick.get_or_insert(tick);
        for (window, width) in self.windows.iter_mut().zip(WINDOWS) {
            if self.ring.len() >= width {
                window.add(self.ring[self.ring.len() - width], -1);
            }
            window.push(tick, sample, width);
        }
        if self.ring.len() == 3600 {
            self.ring.pop_front();
        }
        self.ring.push_back(sample);
        self.last_tick = Some(tick);
    }
    pub(crate) fn update(
        &mut self,
        tick: i64,
        sample: Sample,
        trades: bool,
        l3: bool,
        liquidations: bool,
    ) -> MemoryColumns {
        // Callers normally tick every second. Explicit gaps must still age every window.
        if let Some(last) = self.last_tick {
            for missing in ((last + SECOND)..tick).step_by(SECOND as usize) {
                self.push(missing, Sample::default());
            }
        }
        self.push(tick, sample);
        let mut values = [Values::default(); 6];
        for ((value, window), width) in values.iter_mut().zip(&self.windows).zip(WINDOWS) {
            if tick - self.first_tick.unwrap() < (width as i64 - 1) * SECOND {
                continue;
            }
            let sum = &window.sum;
            value.cov = Some(sum.covered as f64 / width as f64);
            if sum.covered == 0 {
                continue;
            }
            if trades {
                if sum.unscaled == 0 {
                    value.vol = Some(sum.volume as f64 / BASE_SCALE);
                    value.flow = Some(sum.flow as f64 / BASE_SCALE);
                }
                value.trades = Some(sum.trades);
            }
            if liquidations {
                value.liq_base = sum.liq_base.map(|v| Some(v as f64 / BASE_SCALE));
                value.liq_count = sum.liq_count.map(Some);
            }
            let normalized = |i| {
                (sum.depth[i] > 0)
                    .then(|| 2.0 * sum.ofi[i] as f64 * sum.valid as f64 / sum.depth[i] as f64)
            };
            value.ofi1 = normalized(0);
            value.ofi10 = normalized(1);
            value.range = sample.mid.filter(|m| *m > 0).and_then(|mid| {
                window
                    .max
                    .front()
                    .zip(window.min.front())
                    .map(|((_, hi), (_, lo))| {
                        (i128::from(*hi) - i128::from(*lo)) as f64 / mid as f64 * 10000.0
                    })
            });
            value.rv = Some(sum.r2.max(0.0).sqrt());
            if l3 {
                value.l3 = sum.l3.map(Some);
            }
        }
        MemoryColumns::from_values(values)
    }
}

macro_rules! columns {
    ($(($i:tt, $cov:ident, $vol:ident, $flow:ident, $trades:ident, $ofi1:ident, $ofi10:ident,
        $range:ident, $ab:ident, $aa:ident, $db:ident, $da:ident,
        $lsb:ident, $lbb:ident, $lsc:ident, $lbc:ident)),* $(,)?) => {
        #[derive(Debug, Default, Clone, Serialize, Deserialize)]
        #[serde(default)]
        pub struct MemoryColumns {
            $(pub $cov: Option<f64>, pub $vol: Option<f64>, pub $flow: Option<f64>,
              pub $trades: Option<i64>, pub $ofi1: Option<f64>, pub $ofi10: Option<f64>,
              pub $range: Option<f64>, pub $ab: Option<i64>, pub $aa: Option<i64>,
              pub $db: Option<i64>, pub $da: Option<i64>,
              pub $lsb: Option<f64>, pub $lbb: Option<f64>,
              pub $lsc: Option<i64>, pub $lbc: Option<i64>,)*
            pub ret_300s: Option<f64>, pub ret_900s: Option<f64>,
            pub ret_1800s: Option<f64>, pub ret_3600s: Option<f64>,
            pub rv_900s: Option<f64>, pub rv_1800s: Option<f64>, pub rv_3600s: Option<f64>,
            pub microprice_dev_bps: Option<f64>,
        }
        impl MemoryColumns {
            fn from_values(v: [Values; 6]) -> Self {
                Self {
                    $($cov: v[$i].cov, $vol: v[$i].vol, $flow: v[$i].flow,
                      $trades: v[$i].trades, $ofi1: v[$i].ofi1, $ofi10: v[$i].ofi10,
                      $range: v[$i].range, $ab: v[$i].l3[0], $aa: v[$i].l3[1],
                      $db: v[$i].l3[2], $da: v[$i].l3[3],
                      $lsb: v[$i].liq_base[0], $lbb: v[$i].liq_base[1],
                      $lsc: v[$i].liq_count[0], $lbc: v[$i].liq_count[1],)*
                    rv_900s: v[3].rv, rv_1800s: v[4].rv, rv_3600s: v[5].rv,
                    ..Default::default()
                }
            }
        }
    }
}

columns! {
    (0, cov_10s, vol_10s, flow_10s, trades_10s, ofi1_10s, ofi10_10s, range_bps_10s, l3_add_bid_10s, l3_add_ask_10s, l3_del_bid_10s, l3_del_ask_10s, liq_sell_base_10s, liq_buy_base_10s, liq_sell_count_10s, liq_buy_count_10s),
    (1, cov_60s, vol_60s, flow_60s, trades_60s, ofi1_60s, ofi10_60s, range_bps_60s, l3_add_bid_60s, l3_add_ask_60s, l3_del_bid_60s, l3_del_ask_60s, liq_sell_base_60s, liq_buy_base_60s, liq_sell_count_60s, liq_buy_count_60s),
    (2, cov_300s, vol_300s, flow_300s, trades_300s, ofi1_300s, ofi10_300s, range_bps_300s, l3_add_bid_300s, l3_add_ask_300s, l3_del_bid_300s, l3_del_ask_300s, liq_sell_base_300s, liq_buy_base_300s, liq_sell_count_300s, liq_buy_count_300s),
    (3, cov_900s, vol_900s, flow_900s, trades_900s, ofi1_900s, ofi10_900s, range_bps_900s, l3_add_bid_900s, l3_add_ask_900s, l3_del_bid_900s, l3_del_ask_900s, liq_sell_base_900s, liq_buy_base_900s, liq_sell_count_900s, liq_buy_count_900s),
    (4, cov_1800s, vol_1800s, flow_1800s, trades_1800s, ofi1_1800s, ofi10_1800s, range_bps_1800s, l3_add_bid_1800s, l3_add_ask_1800s, l3_del_bid_1800s, l3_del_ask_1800s, liq_sell_base_1800s, liq_buy_base_1800s, liq_sell_count_1800s, liq_buy_count_1800s),
    (5, cov_3600s, vol_3600s, flow_3600s, trades_3600s, ofi1_3600s, ofi10_3600s, range_bps_3600s, l3_add_bid_3600s, l3_add_ask_3600s, l3_del_bid_3600s, l3_del_ask_3600s, liq_sell_base_3600s, liq_buy_base_3600s, liq_sell_count_3600s, liq_buy_count_3600s),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cont_bid_up_same_down_and_ask_mirrors() {
        for (side, next_p, next_q, expected) in [
            (0, 101, 7, 7),
            (0, 100, 7, 2),
            (0, 99, 7, -5),
            (1, 99, 7, -7),
            (1, 100, 7, -2),
            (1, 101, 7, 5),
        ] {
            let mut before = [[None; 10]; 2];
            let mut after = before;
            before[side][0] = Some((100, 5));
            after[side][0] = Some((next_p, next_q));
            assert_eq!(ofi(&before, &after), [expected, expected]);
        }
    }
    #[test]
    fn level_three_changes_and_missing_levels() {
        let mut before = [[None; 10]; 2];
        before[0][2] = Some((98, 10));
        let mut after = before;
        after[0][2] = Some((98, 14));
        after[1][0] = Some((101, 100)); // missing before: zero
        assert_eq!(ofi(&before, &after), [0, 4]);
        assert_eq!(ofi(&after, &[[None; 10]; 2]), [0, 0]);
    }
    fn sample(n: i64) -> Sample {
        Sample {
            covered: 1,
            valid: 1,
            volume: i128::from(n) * 10_000_000_000_000_000,
            flow: -i128::from(n) * 10_000_000_000_000_000,
            trades: n,
            unscaled: 0,
            ofi: [i128::from(n), 2 * i128::from(n)],
            depth: [20, 200],
            mid: Some(100 + n),
            l3: [n, 2 * n, 3 * n, 4 * n],
            r2: 0.25,
            ..Default::default()
        }
    }
    #[test]
    fn null_until_full_zero_coverage_and_optional_feeds() {
        let mut m = Memory::default();
        for t in 1..10 {
            assert!(
                m.update(t * SECOND, sample(1), true, true, false)
                    .cov_10s
                    .is_none()
            );
        }
        let row = m.update(10 * SECOND, sample(1), false, false, false);
        assert_eq!(row.cov_10s, Some(1.0));
        assert!(row.vol_10s.is_none() && row.l3_add_bid_10s.is_none());
        for t in 11..=20 {
            m.update(t * SECOND, Sample::default(), true, true, false);
        }
        let row = m.update(21 * SECOND, Sample::default(), true, true, false);
        assert_eq!(row.cov_10s, Some(0.0));
        assert!(row.ofi1_10s.is_none() && row.rv_900s.is_none() && row.trades_10s.is_none());
    }
    #[test]
    fn gaps_take_wall_seconds_and_unknown_trades_reduce_coverage() {
        let mut m = Memory::default();
        m.update(SECOND, sample(1), true, true, false);
        let row = m.update(10 * SECOND, sample(2), true, true, false);
        assert_eq!(row.cov_10s, Some(0.2));
        assert_eq!(row.vol_10s, Some(3.0));
        let unknown = Sample {
            covered: 0,
            volume: 0,
            flow: 0,
            trades: 0,
            ..sample(3)
        };
        let row = m.update(11 * SECOND, unknown, true, true, false);
        assert_eq!(row.cov_10s, Some(0.1));
        assert_eq!(row.trades_10s, Some(2));
        assert_eq!(row.ofi1_10s, Some(0.5)); // 5 OFI / 10 mean depth
    }
    #[test]
    fn exact_eviction_all_windows_and_per_side_l3() {
        let mut m = Memory::default();
        for t in 1..=7201 {
            let row = m.update(t * SECOND, sample(t), true, true, false);
            if t >= 3600 {
                for (window, width) in m.windows.iter().zip(WINDOWS) {
                    let expected = (t - width as i64 + 1..=t).sum::<i64>();
                    assert_eq!(window.sum.trades, expected);
                    assert_eq!(
                        window.sum.volume,
                        i128::from(expected) * 10_000_000_000_000_000
                    );
                    assert_eq!(
                        window.sum.ofi,
                        [i128::from(expected), 2 * i128::from(expected)]
                    );
                    assert_eq!(
                        window.sum.l3,
                        [expected, 2 * expected, 3 * expected, 4 * expected]
                    );
                }
                assert_eq!(row.trades_3600s, Some((t - 3599..=t).sum()));
            }
        }
        assert_eq!(m.ring.len(), 3600);
    }
    #[test]
    fn liquidations_share_volume_gating_and_exact_eviction_for_every_window() {
        let mut memory = Memory::default();
        let unit = 10_000_000_000_000_000i128;
        for t in 1..=7201 {
            let mut s = sample(t);
            s.liq_base = [i128::from(t) * unit, i128::from(t) * unit * 2];
            s.liq_count = [t, 2 * t];
            let row = memory.update(t * SECOND, s, true, true, true);
            let json = serde_json::to_value(row).unwrap();
            for (window, w) in memory.windows.iter().zip(WINDOWS) {
                let count = format!("liq_sell_count_{w}s");
                if t < w as i64 {
                    assert!(json[&count].is_null());
                    continue;
                }
                let sum = (t - w as i64 + 1..=t).sum::<i64>();
                assert_eq!(
                    window.sum.liq_base,
                    [i128::from(sum) * unit, i128::from(sum) * unit * 2]
                );
                assert_eq!(json[&count], sum);
                let actual = json[format!("liq_buy_base_{w}s")].as_f64().unwrap();
                assert!((actual - (2 * sum) as f64).abs() < actual * 1e-15);
            }
        }
        let absent = memory.update(7202 * SECOND, Sample::default(), true, true, false);
        assert!(absent.liq_sell_base_10s.is_none());
        for t in 7203..=7212 {
            memory.update(t * SECOND, Sample::default(), true, true, true);
        }
        let empty = memory.update(7213 * SECOND, Sample::default(), true, true, true);
        assert_eq!(empty.cov_10s, Some(0.0));
        assert!(empty.liq_sell_count_10s.is_none());
    }
    #[test]
    fn hand_range_ofi_and_partial_realized_volatility() {
        let mut m = Memory::default();
        for t in 1..=900 {
            let mut s = sample(t);
            s.r2 = if t == 899 {
                0.09
            } else if t == 900 {
                0.16
            } else {
                0.0
            };
            let row = m.update(t * SECOND, s, true, true, false);
            if t == 900 {
                assert_eq!(row.rv_900s, Some(0.5));
                assert_eq!(row.range_bps_10s, Some(90.0)); // (1000 - 991) / 1000
                assert_eq!(row.ofi1_10s, Some(895.5));
                assert_eq!(row.ofi10_10s, Some(179.1));
                assert_eq!(row.l3_add_bid_10s, Some(8955));
                assert_eq!(row.l3_del_ask_10s, Some(35820));
            }
        }
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;
    #[test]
    fn exactly_ninety_eight_columns_and_rv_returns_to_exact_zero() {
        assert_eq!(
            serde_json::to_value(MemoryColumns::default())
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            98
        );
        let mut memory = Memory::default();
        for t in 0..=1800 {
            let row = memory.update(
                t * SECOND,
                Sample {
                    covered: 1,
                    valid: 1,
                    r2: if t < 100 { 0.1 } else { 0.0 },
                    ..Default::default()
                },
                false,
                false,
                false,
            );
            if t == 1800 {
                assert_eq!(row.rv_900s, Some(0.0));
            }
        }
    }
}
