use crate::{ENGINE_VERSION, Result, row::Row};
use parquet::{
    basic::Compression,
    data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    format::KeyValue,
    schema::parser::parse_message_type,
};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Writer {
    writer: SerializedFileWriter<File>,
    rows: Vec<Row>,
    target: PathBuf,
    temp: PathBuf,
    pub count: u64,
}
impl Writer {
    pub fn new(target: &Path) -> Result<Self> {
        Self::new_mode(target, "ideal")
    }
    pub fn new_mode(target: &Path, mode: &str) -> Result<Self> {
        fs::create_dir_all(target.parent().unwrap())?;
        let temp = target.with_extension(format!("parquet.{}.tmp", std::process::id()));
        let file = File::options().write(true).create_new(true).open(&temp)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_key_value_metadata(Some(vec![
                KeyValue::new("mode".into(), mode.to_string()),
                KeyValue::new("warmup_s".into(), "300".to_string()),
                KeyValue::new("engine_version".into(), ENGINE_VERSION.to_string()),
            ]))
            .build();
        let writer = SerializedFileWriter::new(
            file,
            Arc::new(parse_message_type(SCHEMA)?),
            Arc::new(props),
        )?;
        Ok(Self {
            writer,
            rows: Vec::with_capacity(8192),
            target: target.into(),
            temp,
            count: 0,
        })
    }
    pub fn push(&mut self, row: Row) -> Result<()> {
        self.rows.push(row);
        self.count += 1;
        if self.rows.len() >= 8192 {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut group = self.writer.next_row_group()?;
        macro_rules! required {
            ($kind:ty, $value:expr) => {{
                let values = self.rows.iter().map($value).collect::<Vec<_>>();
                let mut column = group.next_column()?.unwrap();
                column.typed::<$kind>().write_batch(&values, None, None)?;
                column.close()?;
            }};
        }
        macro_rules! optional {
            ($kind:ty, $($field:ident).+) => {{
                let values = self
                    .rows
                    .iter()
                    .filter_map(|r| r.$($field).+)
                    .collect::<Vec<_>>();
                let defs = self
                    .rows
                    .iter()
                    .map(|r| i16::from(r.$($field).+.is_some()))
                    .collect::<Vec<_>>();
                let mut column = group.next_column()?.unwrap();
                column
                    .typed::<$kind>()
                    .write_batch(&values, Some(&defs), None)?;
                column.close()?;
            }};
        }
        required!(ByteArrayType, |r: &Row| ByteArray::from(
            r.exchange.as_str()
        ));
        required!(ByteArrayType, |r: &Row| ByteArray::from(r.market.as_str()));
        required!(ByteArrayType, |r: &Row| ByteArray::from(r.symbol.as_str()));
        required!(Int64Type, |r: &Row| r.tick_ns);
        required!(Int64Type, |r: &Row| r.available_ns);
        required!(ByteArrayType, |r: &Row| ByteArray::from(
            r.engine_version.as_str()
        ));
        required!(BoolType, |r: &Row| r.book_valid);
        required!(BoolType, |r: &Row| r.warmup);
        optional!(Int64Type, mid_e9);
        optional!(Int64Type, trade_count);
        optional!(Int64Type, liq_count_1s);
        optional!(Int64Type, l3_adds_1s);
        optional!(Int64Type, l3_modifies_1s);
        optional!(Int64Type, l3_deletes_1s);
        optional!(DoubleType, best_bid);
        optional!(DoubleType, best_ask);
        optional!(DoubleType, mid);
        optional!(DoubleType, microprice);
        optional!(DoubleType, spread_bps);
        optional!(DoubleType, bid_depth10);
        optional!(DoubleType, ask_depth10);
        optional!(DoubleType, imbalance10);
        optional!(DoubleType, volume_base);
        optional!(DoubleType, taker_buy_base);
        optional!(DoubleType, taker_sell_base);
        optional!(DoubleType, ret_1s);
        optional!(DoubleType, ret_10s);
        optional!(DoubleType, ret_60s);
        optional!(DoubleType, rv_60s);
        optional!(DoubleType, rv_300s);
        optional!(DoubleType, funding_rate);
        optional!(DoubleType, mark_price);
        optional!(DoubleType, open_interest);
        optional!(DoubleType, liq_base_1s);
        optional!(DoubleType, basis_bps_binance);
        optional!(DoubleType, basis_bps_okx);
        optional!(DoubleType, basis_bps_kraken);
        optional!(DoubleType, spot_dev_bps_okx);
        optional!(DoubleType, spot_dev_bps_kraken);
        optional!(DoubleType, book_age_ms);
        optional!(DoubleType, memory.cov_10s);
        optional!(DoubleType, memory.vol_10s);
        optional!(DoubleType, memory.flow_10s);
        optional!(Int64Type, memory.trades_10s);
        optional!(DoubleType, memory.ofi1_10s);
        optional!(DoubleType, memory.ofi10_10s);
        optional!(DoubleType, memory.range_bps_10s);
        optional!(Int64Type, memory.l3_add_bid_10s);
        optional!(Int64Type, memory.l3_add_ask_10s);
        optional!(Int64Type, memory.l3_del_bid_10s);
        optional!(Int64Type, memory.l3_del_ask_10s);
        optional!(DoubleType, memory.cov_60s);
        optional!(DoubleType, memory.vol_60s);
        optional!(DoubleType, memory.flow_60s);
        optional!(Int64Type, memory.trades_60s);
        optional!(DoubleType, memory.ofi1_60s);
        optional!(DoubleType, memory.ofi10_60s);
        optional!(DoubleType, memory.range_bps_60s);
        optional!(Int64Type, memory.l3_add_bid_60s);
        optional!(Int64Type, memory.l3_add_ask_60s);
        optional!(Int64Type, memory.l3_del_bid_60s);
        optional!(Int64Type, memory.l3_del_ask_60s);
        optional!(DoubleType, memory.cov_300s);
        optional!(DoubleType, memory.vol_300s);
        optional!(DoubleType, memory.flow_300s);
        optional!(Int64Type, memory.trades_300s);
        optional!(DoubleType, memory.ofi1_300s);
        optional!(DoubleType, memory.ofi10_300s);
        optional!(DoubleType, memory.range_bps_300s);
        optional!(Int64Type, memory.l3_add_bid_300s);
        optional!(Int64Type, memory.l3_add_ask_300s);
        optional!(Int64Type, memory.l3_del_bid_300s);
        optional!(Int64Type, memory.l3_del_ask_300s);
        optional!(DoubleType, memory.cov_900s);
        optional!(DoubleType, memory.vol_900s);
        optional!(DoubleType, memory.flow_900s);
        optional!(Int64Type, memory.trades_900s);
        optional!(DoubleType, memory.ofi1_900s);
        optional!(DoubleType, memory.ofi10_900s);
        optional!(DoubleType, memory.range_bps_900s);
        optional!(Int64Type, memory.l3_add_bid_900s);
        optional!(Int64Type, memory.l3_add_ask_900s);
        optional!(Int64Type, memory.l3_del_bid_900s);
        optional!(Int64Type, memory.l3_del_ask_900s);
        optional!(DoubleType, memory.cov_1800s);
        optional!(DoubleType, memory.vol_1800s);
        optional!(DoubleType, memory.flow_1800s);
        optional!(Int64Type, memory.trades_1800s);
        optional!(DoubleType, memory.ofi1_1800s);
        optional!(DoubleType, memory.ofi10_1800s);
        optional!(DoubleType, memory.range_bps_1800s);
        optional!(Int64Type, memory.l3_add_bid_1800s);
        optional!(Int64Type, memory.l3_add_ask_1800s);
        optional!(Int64Type, memory.l3_del_bid_1800s);
        optional!(Int64Type, memory.l3_del_ask_1800s);
        optional!(DoubleType, memory.cov_3600s);
        optional!(DoubleType, memory.vol_3600s);
        optional!(DoubleType, memory.flow_3600s);
        optional!(Int64Type, memory.trades_3600s);
        optional!(DoubleType, memory.ofi1_3600s);
        optional!(DoubleType, memory.ofi10_3600s);
        optional!(DoubleType, memory.range_bps_3600s);
        optional!(Int64Type, memory.l3_add_bid_3600s);
        optional!(Int64Type, memory.l3_add_ask_3600s);
        optional!(Int64Type, memory.l3_del_bid_3600s);
        optional!(Int64Type, memory.l3_del_ask_3600s);
        optional!(DoubleType, memory.ret_300s);
        optional!(DoubleType, memory.ret_900s);
        optional!(DoubleType, memory.ret_1800s);
        optional!(DoubleType, memory.ret_3600s);
        optional!(DoubleType, memory.rv_900s);
        optional!(DoubleType, memory.rv_1800s);
        optional!(DoubleType, memory.rv_3600s);
        optional!(DoubleType, memory.microprice_dev_bps);
        required!(ByteArrayType, |r: &Row| ByteArray::from(r.asset.as_str()));
        optional!(DoubleType, memory.liq_sell_base_10s);
        optional!(DoubleType, memory.liq_buy_base_10s);
        optional!(Int64Type, memory.liq_sell_count_10s);
        optional!(Int64Type, memory.liq_buy_count_10s);
        optional!(DoubleType, memory.liq_sell_base_60s);
        optional!(DoubleType, memory.liq_buy_base_60s);
        optional!(Int64Type, memory.liq_sell_count_60s);
        optional!(Int64Type, memory.liq_buy_count_60s);
        optional!(DoubleType, memory.liq_sell_base_300s);
        optional!(DoubleType, memory.liq_buy_base_300s);
        optional!(Int64Type, memory.liq_sell_count_300s);
        optional!(Int64Type, memory.liq_buy_count_300s);
        optional!(DoubleType, memory.liq_sell_base_900s);
        optional!(DoubleType, memory.liq_buy_base_900s);
        optional!(Int64Type, memory.liq_sell_count_900s);
        optional!(Int64Type, memory.liq_buy_count_900s);
        optional!(DoubleType, memory.liq_sell_base_1800s);
        optional!(DoubleType, memory.liq_buy_base_1800s);
        optional!(Int64Type, memory.liq_sell_count_1800s);
        optional!(Int64Type, memory.liq_buy_count_1800s);
        optional!(DoubleType, memory.liq_sell_base_3600s);
        optional!(DoubleType, memory.liq_buy_base_3600s);
        optional!(Int64Type, memory.liq_sell_count_3600s);
        optional!(Int64Type, memory.liq_buy_count_3600s);
        optional!(DoubleType, funding_min_to_next);
        optional!(DoubleType, mark_index_bps);
        optional!(DoubleType, oi_chg_pct_300s);
        optional!(DoubleType, oi_chg_pct_900s);
        optional!(DoubleType, oi_chg_pct_3600s);
        optional!(DoubleType, depth_bid_10bps);
        optional!(DoubleType, depth_bid_25bps);
        optional!(DoubleType, depth_bid_50bps);
        optional!(DoubleType, depth_ask_10bps);
        optional!(DoubleType, depth_ask_25bps);
        optional!(DoubleType, depth_ask_50bps);
        optional!(DoubleType, dvol);
        optional!(DoubleType, dvol_chg_300s);
        optional!(DoubleType, dvol_chg_900s);
        optional!(DoubleType, dvol_chg_3600s);
        optional!(DoubleType, dvol_rv_gap);
        group.close()?;
        self.rows.clear();
        Ok(())
    }
    pub fn finish(mut self) -> Result<u64> {
        self.flush()?;
        self.writer.close()?;
        File::open(&self.temp)?.sync_all()?;
        fs::rename(&self.temp, &self.target)?;
        File::open(self.target.parent().unwrap())?.sync_all()?;
        Ok(self.count)
    }
}
const SCHEMA: &str = r#"message features {
  REQUIRED BINARY exchange (UTF8);
  REQUIRED BINARY market (UTF8);
  REQUIRED BINARY symbol (UTF8);
  REQUIRED INT64 tick_ns;
  REQUIRED INT64 available_ns;
  REQUIRED BINARY engine_version (UTF8);
  REQUIRED BOOLEAN book_valid;
  REQUIRED BOOLEAN warmup;
  OPTIONAL INT64 mid_e9;
  OPTIONAL INT64 trade_count;
  OPTIONAL INT64 liq_count_1s;
  OPTIONAL INT64 l3_adds_1s;
  OPTIONAL INT64 l3_modifies_1s;
  OPTIONAL INT64 l3_deletes_1s;
  OPTIONAL DOUBLE best_bid;
  OPTIONAL DOUBLE best_ask;
  OPTIONAL DOUBLE mid;
  OPTIONAL DOUBLE microprice;
  OPTIONAL DOUBLE spread_bps;
  OPTIONAL DOUBLE bid_depth10;
  OPTIONAL DOUBLE ask_depth10;
  OPTIONAL DOUBLE imbalance10;
  OPTIONAL DOUBLE volume_base;
  OPTIONAL DOUBLE taker_buy_base;
  OPTIONAL DOUBLE taker_sell_base;
  OPTIONAL DOUBLE ret_1s;
  OPTIONAL DOUBLE ret_10s;
  OPTIONAL DOUBLE ret_60s;
  OPTIONAL DOUBLE rv_60s;
  OPTIONAL DOUBLE rv_300s;
  OPTIONAL DOUBLE funding_rate;
  OPTIONAL DOUBLE mark_price;
  OPTIONAL DOUBLE open_interest;
  OPTIONAL DOUBLE liq_base_1s;
  OPTIONAL DOUBLE basis_bps_binance;
  OPTIONAL DOUBLE basis_bps_okx;
  OPTIONAL DOUBLE basis_bps_kraken;
  OPTIONAL DOUBLE spot_dev_bps_okx;
  OPTIONAL DOUBLE spot_dev_bps_kraken;
  OPTIONAL DOUBLE book_age_ms;
  OPTIONAL DOUBLE cov_10s;
  OPTIONAL DOUBLE vol_10s;
  OPTIONAL DOUBLE flow_10s;
  OPTIONAL INT64 trades_10s;
  OPTIONAL DOUBLE ofi1_10s;
  OPTIONAL DOUBLE ofi10_10s;
  OPTIONAL DOUBLE range_bps_10s;
  OPTIONAL INT64 l3_add_bid_10s;
  OPTIONAL INT64 l3_add_ask_10s;
  OPTIONAL INT64 l3_del_bid_10s;
  OPTIONAL INT64 l3_del_ask_10s;
  OPTIONAL DOUBLE cov_60s;
  OPTIONAL DOUBLE vol_60s;
  OPTIONAL DOUBLE flow_60s;
  OPTIONAL INT64 trades_60s;
  OPTIONAL DOUBLE ofi1_60s;
  OPTIONAL DOUBLE ofi10_60s;
  OPTIONAL DOUBLE range_bps_60s;
  OPTIONAL INT64 l3_add_bid_60s;
  OPTIONAL INT64 l3_add_ask_60s;
  OPTIONAL INT64 l3_del_bid_60s;
  OPTIONAL INT64 l3_del_ask_60s;
  OPTIONAL DOUBLE cov_300s;
  OPTIONAL DOUBLE vol_300s;
  OPTIONAL DOUBLE flow_300s;
  OPTIONAL INT64 trades_300s;
  OPTIONAL DOUBLE ofi1_300s;
  OPTIONAL DOUBLE ofi10_300s;
  OPTIONAL DOUBLE range_bps_300s;
  OPTIONAL INT64 l3_add_bid_300s;
  OPTIONAL INT64 l3_add_ask_300s;
  OPTIONAL INT64 l3_del_bid_300s;
  OPTIONAL INT64 l3_del_ask_300s;
  OPTIONAL DOUBLE cov_900s;
  OPTIONAL DOUBLE vol_900s;
  OPTIONAL DOUBLE flow_900s;
  OPTIONAL INT64 trades_900s;
  OPTIONAL DOUBLE ofi1_900s;
  OPTIONAL DOUBLE ofi10_900s;
  OPTIONAL DOUBLE range_bps_900s;
  OPTIONAL INT64 l3_add_bid_900s;
  OPTIONAL INT64 l3_add_ask_900s;
  OPTIONAL INT64 l3_del_bid_900s;
  OPTIONAL INT64 l3_del_ask_900s;
  OPTIONAL DOUBLE cov_1800s;
  OPTIONAL DOUBLE vol_1800s;
  OPTIONAL DOUBLE flow_1800s;
  OPTIONAL INT64 trades_1800s;
  OPTIONAL DOUBLE ofi1_1800s;
  OPTIONAL DOUBLE ofi10_1800s;
  OPTIONAL DOUBLE range_bps_1800s;
  OPTIONAL INT64 l3_add_bid_1800s;
  OPTIONAL INT64 l3_add_ask_1800s;
  OPTIONAL INT64 l3_del_bid_1800s;
  OPTIONAL INT64 l3_del_ask_1800s;
  OPTIONAL DOUBLE cov_3600s;
  OPTIONAL DOUBLE vol_3600s;
  OPTIONAL DOUBLE flow_3600s;
  OPTIONAL INT64 trades_3600s;
  OPTIONAL DOUBLE ofi1_3600s;
  OPTIONAL DOUBLE ofi10_3600s;
  OPTIONAL DOUBLE range_bps_3600s;
  OPTIONAL INT64 l3_add_bid_3600s;
  OPTIONAL INT64 l3_add_ask_3600s;
  OPTIONAL INT64 l3_del_bid_3600s;
  OPTIONAL INT64 l3_del_ask_3600s;
  OPTIONAL DOUBLE ret_300s;
  OPTIONAL DOUBLE ret_900s;
  OPTIONAL DOUBLE ret_1800s;
  OPTIONAL DOUBLE ret_3600s;
  OPTIONAL DOUBLE rv_900s;
  OPTIONAL DOUBLE rv_1800s;
  OPTIONAL DOUBLE rv_3600s;
  OPTIONAL DOUBLE microprice_dev_bps;
  REQUIRED BINARY asset (UTF8);
  OPTIONAL DOUBLE liq_sell_base_10s;
  OPTIONAL DOUBLE liq_buy_base_10s;
  OPTIONAL INT64 liq_sell_count_10s;
  OPTIONAL INT64 liq_buy_count_10s;
  OPTIONAL DOUBLE liq_sell_base_60s;
  OPTIONAL DOUBLE liq_buy_base_60s;
  OPTIONAL INT64 liq_sell_count_60s;
  OPTIONAL INT64 liq_buy_count_60s;
  OPTIONAL DOUBLE liq_sell_base_300s;
  OPTIONAL DOUBLE liq_buy_base_300s;
  OPTIONAL INT64 liq_sell_count_300s;
  OPTIONAL INT64 liq_buy_count_300s;
  OPTIONAL DOUBLE liq_sell_base_900s;
  OPTIONAL DOUBLE liq_buy_base_900s;
  OPTIONAL INT64 liq_sell_count_900s;
  OPTIONAL INT64 liq_buy_count_900s;
  OPTIONAL DOUBLE liq_sell_base_1800s;
  OPTIONAL DOUBLE liq_buy_base_1800s;
  OPTIONAL INT64 liq_sell_count_1800s;
  OPTIONAL INT64 liq_buy_count_1800s;
  OPTIONAL DOUBLE liq_sell_base_3600s;
  OPTIONAL DOUBLE liq_buy_base_3600s;
  OPTIONAL INT64 liq_sell_count_3600s;
  OPTIONAL INT64 liq_buy_count_3600s;
  OPTIONAL DOUBLE funding_min_to_next;
  OPTIONAL DOUBLE mark_index_bps;
  OPTIONAL DOUBLE oi_chg_pct_300s;
  OPTIONAL DOUBLE oi_chg_pct_900s;
  OPTIONAL DOUBLE oi_chg_pct_3600s;
  OPTIONAL DOUBLE depth_bid_10bps;
  OPTIONAL DOUBLE depth_bid_25bps;
  OPTIONAL DOUBLE depth_bid_50bps;
  OPTIONAL DOUBLE depth_ask_10bps;
  OPTIONAL DOUBLE depth_ask_25bps;
  OPTIONAL DOUBLE depth_ask_50bps;
  OPTIONAL DOUBLE dvol;
  OPTIONAL DOUBLE dvol_chg_300s;
  OPTIONAL DOUBLE dvol_chg_900s;
  OPTIONAL DOUBLE dvol_chg_3600s;
  OPTIONAL DOUBLE dvol_rv_gap;
}"#;
