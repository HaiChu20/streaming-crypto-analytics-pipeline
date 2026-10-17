use crate::{Result, error};
use engine_core::session::Checkpoint;
use std::{
    fs::{self, File},
    io::{BufReader, Write},
    path::Path,
};

pub fn save(path: &Path, state: &Checkpoint) -> Result<()> {
    state.validate()?;
    let parent = path
        .parent()
        .ok_or_else(|| error("state path needs a parent"))?;
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = File::options().write(true).create_new(true).open(&temp)?;
    serde_json::to_writer(&mut file, state)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
pub fn load(path: &Path) -> Result<Checkpoint> {
    let state: Checkpoint = serde_json::from_reader(BufReader::new(File::open(path)?))?;
    state.validate()?;
    Ok(state)
}

/// UTC day formatting without a clock, timezone dependency, or extra crate.
pub fn utc_date(tick_ns: i64) -> String {
    let mut days = tick_ns.div_euclid(86_400_000_000_000);
    let mut year = 1970;
    let leap = |y| y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    loop {
        let count = if leap(year) { 366 } else { 365 };
        if days < count {
            break;
        }
        days -= count;
        year += 1;
    }
    let months = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for count in months {
        if days < count {
            break;
        }
        days -= count;
        month += 1;
    }
    format!("{year:04}-{month:02}-{:02}", days + 1)
}
