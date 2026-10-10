use serde_json::{Map, Value};

use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::{
    OFFSETS_PROPERTY_PREFIX, OffsetFormatVersion, QueryId, SnapshotId,
};

const FORMAT_KEY: &str = "format-version";
const PENDING_EPOCH_KEY: &str = "pending-epoch";
const STARTING_HEAD_KEY: &str = "starting-head";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartingMark {
    pub head: Option<SnapshotId>,
}

fn corrupt(detail: &str) -> MicroBatchError {
    MicroBatchError::Catalog(format!("repark.cdc starting mark is corrupt: {detail}"))
}

impl StartingMark {
    #[must_use]
    pub fn property_key(query: QueryId) -> String {
        format!("{OFFSETS_PROPERTY_PREFIX}{query}")
    }

    #[must_use]
    pub fn property_value(&self) -> String {
        let head = self.head.map_or(Value::Null, |id| Value::from(id.get()));
        format!(
            "{{\"{FORMAT_KEY}\":{format},\"{PENDING_EPOCH_KEY}\":0,\"{STARTING_HEAD_KEY}\":{head}}}",
            format = OffsetFormatVersion::CURRENT.get()
        )
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn from_property(value: &str) -> Result<Option<StartingMark>, MicroBatchError> {
        let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(value) else {
            return Ok(None);
        };
        let Some(pending) = fields.get(PENDING_EPOCH_KEY) else {
            return Ok(None);
        };
        if pending.as_u64() != Some(0) {
            return Err(corrupt(&format!(
                "{PENDING_EPOCH_KEY} is {pending}, and only epoch 0 can be pending"
            )));
        }
        let format = fields.get(FORMAT_KEY).and_then(Value::as_u64);
        if format != Some(u64::from(OffsetFormatVersion::CURRENT.get())) {
            return Err(MicroBatchError::UnsupportedOffsetFormat {
                found: format.map_or_else(|| String::from("none"), |found| found.to_string()),
                supported: OffsetFormatVersion::CURRENT.get(),
            });
        }
        Ok(Some(StartingMark {
            head: starting_head(&fields)?,
        }))
    }
}

fn starting_head(fields: &Map<String, Value>) -> Result<Option<SnapshotId>, MicroBatchError> {
    match fields.get(STARTING_HEAD_KEY) {
        Some(Value::Null) => Ok(None),
        Some(head) => head
            .as_i64()
            .map(|id| Some(SnapshotId::new(id)))
            .ok_or_else(|| corrupt(&format!("{STARTING_HEAD_KEY} is {head}, not a snapshot id"))),
        None => Err(corrupt(&format!("{STARTING_HEAD_KEY} is missing"))),
    }
}
