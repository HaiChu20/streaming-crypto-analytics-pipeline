//! Bounded, wide snapshots of the replay's existing L2 books.
use crate::{Result, error, features::Engine, input::StreamPath};
use parquet::{
    basic::Compression,
    data_type::Int64Type,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    format::KeyValue,
    schema::parser::parse_message_type,
};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

type Key = (String, String, String);

/// Preserve ASCII letters/digits/underscore; replace every other character with `_`.
pub fn column_name(key: &Key) -> String {
    let symbol: String = key
        .2
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{}_{}_{}_mid_e9", key.0, key.1, symbol)
}

pub struct MidWriter {
    writer: SerializedFileWriter<File>,
    keys: Vec<Key>,
    ticks: Vec<i64>,
    columns: Vec<Vec<Option<i64>>>,
    target: PathBuf,
    temp: PathBuf,
    count: u64,
}
impl MidWriter {
    pub fn new(target: &Path, paths: &[PathBuf], grid_ms: u64, mode: &str) -> Result<Self> {
        if target.exists() {
            return Err(error("mids output already exists; use a new output path"));
        }
        let mut keys = BTreeSet::new();
        for path in paths {
            let stream = StreamPath::new(path)?;
            if matches!(stream.feed.as_str(), "l2" | "l3") {
                keys.insert((stream.exchange, stream.market, stream.symbol));
            }
        }
        let keys: Vec<_> = keys.into_iter().collect();
        let mut names = BTreeSet::new();
        let mut schema = String::from("message mids { REQUIRED INT64 tick_ns;\n");
        for key in &keys {
            let name = column_name(key);
            if !names.insert(name.clone()) {
                return Err(error(format!("mid column name collision: {name}")));
            }
            schema.push_str(&format!("OPTIONAL INT64 {name};\n"));
        }
        schema.push('}');
        let parent = target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let temp = target.with_extension(format!("parquet.{}.tmp", std::process::id()));
        let file = File::options().write(true).create_new(true).open(&temp)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_key_value_metadata(Some(vec![
                KeyValue::new("mode".into(), mode.to_string()),
                KeyValue::new("mid_grid_ms".into(), grid_ms.to_string()),
                KeyValue::new("engine_version".into(), crate::ENGINE_VERSION.to_string()),
                KeyValue::new("instruments".into(), serde_json::to_string(&keys)?),
            ]))
            .build();
        let writer = SerializedFileWriter::new(
            file,
            Arc::new(parse_message_type(&schema)?),
            Arc::new(props),
        )?;
        let columns = keys.iter().map(|_| Vec::with_capacity(8192)).collect();
        Ok(Self {
            writer,
            keys,
            ticks: Vec::with_capacity(8192),
            columns,
            target: target.into(),
            temp,
            count: 0,
        })
    }
    pub fn push(&mut self, tick: i64, engine: &Engine) -> Result<()> {
        self.ticks.push(tick);
        for (key, column) in self.keys.iter().zip(&mut self.columns) {
            column.push(engine.mid_e9(key, tick));
        }
        self.count += 1;
        if self.ticks.len() >= 8192 {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.ticks.is_empty() {
            return Ok(());
        }
        let mut group = self.writer.next_row_group()?;
        let mut column = group.next_column()?.unwrap();
        column
            .typed::<Int64Type>()
            .write_batch(&self.ticks, None, None)?;
        column.close()?;
        for values in &mut self.columns {
            let data: Vec<_> = values.iter().flatten().copied().collect();
            let defs: Vec<_> = values.iter().map(|v| i16::from(v.is_some())).collect();
            let mut column = group.next_column()?.unwrap();
            column
                .typed::<Int64Type>()
                .write_batch(&data, Some(&defs), None)?;
            column.close()?;
            values.clear();
        }
        group.close()?;
        self.ticks.clear();
        Ok(())
    }
    pub fn finish(mut self) -> Result<u64> {
        self.flush()?;
        self.writer.close()?;
        File::open(&self.temp)?.sync_all()?;
        fs::rename(&self.temp, &self.target)?;
        Ok(self.count)
    }
}
