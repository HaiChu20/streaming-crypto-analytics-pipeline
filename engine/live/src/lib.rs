//! Local Kafka transport helpers; feature calculations remain in engine_core.
use engine_core::session::Queued;
use engine_replay::{Result, error};

/// Match archiver.record_raw byte for byte: retain the original envelope and add
/// only its Kafka coordinates. The exchange frame inside payload is never changed.
pub fn archived(value: &[u8], topic: &str, partition: i32, offset: i64) -> Result<Queued> {
    let envelope = std::str::from_utf8(value)?;
    let body = envelope
        .get(..envelope.len().saturating_sub(1))
        .ok_or_else(|| error("empty Kafka envelope"))?;
    let line = format!(
        "{body},\"topic\":{},\"partition\":{partition},\"offset\":{offset}}}\n",
        serde_json::to_string(topic)?
    );
    let record = engine_replay::input::recover_archived(&line, Some((topic, partition, offset)))?;
    Ok(Queued { record, line })
}
