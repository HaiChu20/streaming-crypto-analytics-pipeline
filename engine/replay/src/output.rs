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
            ($kind:ty, $field:ident) => {{
                let values = self
                    .rows
                    .iter()
                    .filter_map(|r| r.$field)
                    .collect::<Vec<_>>();
                let defs = self
                    .rows
                    .iter()
                    .map(|r| i16::from(r.$field.is_some()))
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
        required!(ByteArrayType, |r: &Row| ByteArray::from(r.asset.as_str()));
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
  REQUIRED BINARY asset (UTF8);
}"#;
