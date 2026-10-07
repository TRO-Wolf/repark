use std::collections::HashMap;
use std::fmt;
use std::vec::Vec;

use iceberg::table::Table;
use repark_common::Generation;
use uuid::Uuid;

use crate::microbatch::error::MicroBatchError;

pub const FORMAT_VERSION_KEY: &str = "repark.cdc.format-version";
pub const QUERY_ID_KEY: &str = "repark.cdc.query-id";
pub const RUN_ID_KEY: &str = "repark.cdc.run-id";
pub const EPOCH_KEY: &str = "repark.cdc.epoch";
pub const GENERATION_KEY: &str = "repark.cdc.generation";
pub const OFFSETS_KEY: &str = "repark.cdc.offsets";
pub const OFFSETS_PROPERTY_PREFIX: &str = "repark.cdc.offsets.";
pub const SPARK_QUERY_ID_KEY: &str = "spark.sql.streaming.queryId";
pub const SPARK_EPOCH_ID_KEY: &str = "spark.sql.streaming.epochId";

const QUERY_NAMESPACE_SEED: &[u8] = b"repark.cdc.query-id.v1";
const QUERY_NAME_ABSENT: u8 = 0x00;
const QUERY_NAME_PRESENT: u8 = 0x01;
const OFFSET_TABLE_KEY: &str = "table";
const OFFSET_TABLE_UUID_KEY: &str = "table-uuid";
const OFFSET_SNAPSHOT_ID_KEY: &str = "snapshot-id";
const OFFSET_POSITION_KEY: &str = "position";
const STAMP_FORMAT_KEY: &str = "format-version";
const STAMP_RUN_KEY: &str = "run-id";
const STAMP_EPOCH_KEY: &str = "epoch";
const STAMP_GENERATION_KEY: &str = "generation";
const STAMP_INPUTS_KEY: &str = "inputs";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(i64);

impl SnapshotId {
    #[must_use]
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilePosition(u64);

impl FilePosition {
    #[must_use]
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableUuid(Uuid);

impl TableUuid {
    #[must_use]
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> Uuid {
        self.0
    }

    #[must_use]
    pub fn of(table: &Table) -> Self {
        Self(table.metadata().uuid())
    }
}

impl fmt::Display for TableUuid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(Uuid);

impl QueryId {
    #[must_use]
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> Uuid {
        self.0
    }

    #[must_use]
    pub fn derive(sink: TableUuid, query_name: Option<&str>) -> Self {
        let namespace = Uuid::new_v5(&Uuid::NAMESPACE_OID, QUERY_NAMESPACE_SEED);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(sink.get().as_bytes());
        match query_name {
            Some(name) => {
                bytes.push(QUERY_NAME_PRESENT);
                let length = u32::try_from(name.len()).unwrap_or(u32::MAX);
                bytes.extend_from_slice(&length.to_be_bytes());
                bytes.extend_from_slice(name.as_bytes());
            }
            None => bytes.push(QUERY_NAME_ABSENT),
        }
        Self(Uuid::new_v5(&namespace, &bytes))
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunId(Uuid);

impl RunId {
    #[must_use]
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> Uuid {
        self.0
    }

    #[must_use]
    pub fn fresh() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Epoch(u64);

impl Epoch {
    pub const FIRST: Epoch = Epoch(0);

    #[must_use]
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn next(self) -> Epoch {
        Epoch(self.0.saturating_add(1))
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OffsetFormatVersion(u32);

impl OffsetFormatVersion {
    pub const CURRENT: OffsetFormatVersion = OffsetFormatVersion(1);

    #[must_use]
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InputOffset {
    pub table: TableUuid,
    pub table_name: String,
    pub snapshot: SnapshotId,
    pub position: FilePosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetVector(Vec<InputOffset>);

impl OffsetVector {
    #[must_use]
    pub fn single(offset: InputOffset) -> Self {
        Self(vec![offset])
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn try_from_inputs(inputs: Vec<InputOffset>) -> Result<Self, MicroBatchError> {
        if inputs.is_empty() {
            return Err(MicroBatchError::Catalog(String::from(
                "offset vector needs at least one input",
            )));
        }
        let mut sorted = inputs;
        sorted.sort_by_key(|input| input.table.get());
        if let Some(pair) = sorted
            .windows(2)
            .find(|pair| pair[0].table == pair[1].table)
        {
            return Err(MicroBatchError::Catalog(format!(
                "offset vector holds table {table} twice",
                table = pair[0].table
            )));
        }
        Ok(Self(sorted))
    }

    #[must_use]
    pub fn inputs(&self) -> &[InputOffset] {
        &self.0
    }

    #[must_use]
    pub fn get(&self, table: TableUuid) -> Option<&InputOffset> {
        self.0.iter().find(|input| input.table == table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkRecord {
    pub format: OffsetFormatVersion,
    pub query: QueryId,
    pub run: RunId,
    pub epoch: Epoch,
    pub generation: Generation,
    pub offsets: OffsetVector,
}

impl SinkRecord {
    #[must_use]
    pub fn summary_entries(&self, door: SinkDoor) -> Vec<(String, String)> {
        let offsets = serde_json::to_string(&offsets_json(&self.offsets)).unwrap_or_default();
        let mut entries = Vec::from([
            (
                FORMAT_VERSION_KEY.to_string(),
                self.format.get().to_string(),
            ),
            (QUERY_ID_KEY.to_string(), self.query.get().to_string()),
            (RUN_ID_KEY.to_string(), self.run.get().to_string()),
            (EPOCH_KEY.to_string(), self.epoch.get().to_string()),
            (
                GENERATION_KEY.to_string(),
                self.generation.get().to_string(),
            ),
            (OFFSETS_KEY.to_string(), offsets),
        ]);
        if matches!(door, SinkDoor::Table) {
            entries.push((SPARK_QUERY_ID_KEY.to_string(), self.query.get().to_string()));
            entries.push((SPARK_EPOCH_ID_KEY.to_string(), self.epoch.get().to_string()));
        }
        entries
    }

    #[must_use]
    pub fn property(&self) -> (String, String) {
        let key = format!("{OFFSETS_PROPERTY_PREFIX}{query}", query = self.query);
        let mut fields = serde_json::Map::new();
        fields.insert(
            STAMP_FORMAT_KEY.to_string(),
            serde_json::Value::from(self.format.get()),
        );
        fields.insert(
            STAMP_RUN_KEY.to_string(),
            serde_json::Value::String(self.run.get().to_string()),
        );
        fields.insert(
            STAMP_EPOCH_KEY.to_string(),
            serde_json::Value::from(self.epoch.get()),
        );
        fields.insert(
            STAMP_GENERATION_KEY.to_string(),
            serde_json::Value::from(self.generation.get().get()),
        );
        fields.insert(STAMP_INPUTS_KEY.to_string(), offsets_json(&self.offsets));
        let value = serde_json::to_string(&serde_json::Value::Object(fields)).unwrap_or_default();
        (key, value)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn from_summary(
        summary: &HashMap<String, String>,
    ) -> Result<Option<SinkRecord>, MicroBatchError> {
        let Some(version_text) = summary.get(FORMAT_VERSION_KEY) else {
            return Ok(None);
        };
        let format = match version_text.parse::<u32>() {
            Ok(version) if version == OffsetFormatVersion::CURRENT.get() => {
                OffsetFormatVersion::CURRENT
            }
            Ok(version) => {
                return Err(MicroBatchError::UnsupportedOffsetFormat {
                    found: version,
                    supported: OffsetFormatVersion::CURRENT.get(),
                });
            }
            Err(_) => {
                return Err(MicroBatchError::Catalog(format!(
                    "repark.cdc stamp value for {FORMAT_VERSION_KEY} is not a number: {version_text:?}"
                )));
            }
        };
        let query = QueryId::new(parse_stamp_uuid(
            stamp_text(summary, QUERY_ID_KEY)?,
            QUERY_ID_KEY,
        )?);
        let run = RunId::new(parse_stamp_uuid(
            stamp_text(summary, RUN_ID_KEY)?,
            RUN_ID_KEY,
        )?);
        let epoch = Epoch::new(parse_stamp_number(
            stamp_text(summary, EPOCH_KEY)?,
            EPOCH_KEY,
        )?);
        let generation_raw =
            parse_stamp_number(stamp_text(summary, GENERATION_KEY)?, GENERATION_KEY)?;
        let generation = Generation::new(generation_raw).ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "repark.cdc stamp value for {GENERATION_KEY} must be positive"
            ))
        })?;
        let offsets_text = stamp_text(summary, OFFSETS_KEY)?;
        let offsets_value: serde_json::Value =
            serde_json::from_str(offsets_text).map_err(|error| {
                MicroBatchError::Catalog(format!(
                    "repark.cdc stamp value for {OFFSETS_KEY} is not JSON: {error}"
                ))
            })?;
        let Some(entries) = offsets_value.as_array() else {
            return Err(MicroBatchError::Catalog(format!(
                "repark.cdc stamp value for {OFFSETS_KEY} is not an array"
            )));
        };
        let offsets = parse_stamp_inputs(entries)?;
        Ok(Some(SinkRecord {
            format,
            query,
            run,
            epoch,
            generation,
            offsets,
        }))
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn from_property(query: QueryId, value: &str) -> Result<SinkRecord, MicroBatchError> {
        let parsed: serde_json::Value = serde_json::from_str(value).map_err(|error| {
            MicroBatchError::Catalog(format!("repark.cdc stamp is not JSON: {error}"))
        })?;
        let format_raw = stamp_u64(&parsed, STAMP_FORMAT_KEY)?;
        let format = if format_raw == u64::from(OffsetFormatVersion::CURRENT.get()) {
            OffsetFormatVersion::CURRENT
        } else {
            return Err(MicroBatchError::UnsupportedOffsetFormat {
                found: u32::try_from(format_raw).unwrap_or(u32::MAX),
                supported: OffsetFormatVersion::CURRENT.get(),
            });
        };
        let run = RunId::new(parse_stamp_uuid(
            stamp_str(&parsed, STAMP_RUN_KEY)?,
            STAMP_RUN_KEY,
        )?);
        let epoch = Epoch::new(stamp_u64(&parsed, STAMP_EPOCH_KEY)?);
        let generation_raw = stamp_u64(&parsed, STAMP_GENERATION_KEY)?;
        let generation = Generation::new(generation_raw).ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "repark.cdc stamp value for {STAMP_GENERATION_KEY} must be positive"
            ))
        })?;
        let inputs_member = stamp_member(&parsed, STAMP_INPUTS_KEY)?;
        let Some(entries) = inputs_member.as_array() else {
            return Err(MicroBatchError::Catalog(format!(
                "repark.cdc stamp value for {STAMP_INPUTS_KEY} is not an array"
            )));
        };
        let offsets = parse_stamp_inputs(entries)?;
        Ok(SinkRecord {
            format,
            query,
            run,
            epoch,
            generation,
            offsets,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDoor {
    Table,
    ForeachBatch,
}

#[must_use]
pub fn spark_source_offset_json(offset: &InputOffset) -> String {
    format!(
        "{{\"version\":1,\"snapshot_id\":{snapshot},\"position\":{position},\"scan_all_files\":false}}",
        snapshot = offset.snapshot.get(),
        position = offset.position.get()
    )
}

#[must_use]
fn input_offset_json(offset: &InputOffset) -> serde_json::Value {
    let mut fields = serde_json::Map::new();
    fields.insert(
        OFFSET_TABLE_KEY.to_string(),
        serde_json::Value::String(offset.table_name.clone()),
    );
    fields.insert(
        OFFSET_TABLE_UUID_KEY.to_string(),
        serde_json::Value::String(offset.table.get().to_string()),
    );
    fields.insert(
        OFFSET_SNAPSHOT_ID_KEY.to_string(),
        serde_json::Value::from(offset.snapshot.get()),
    );
    fields.insert(
        OFFSET_POSITION_KEY.to_string(),
        serde_json::Value::from(offset.position.get()),
    );
    serde_json::Value::Object(fields)
}

#[must_use]
fn offsets_json(offsets: &OffsetVector) -> serde_json::Value {
    serde_json::Value::Array(offsets.inputs().iter().map(input_offset_json).collect())
}

fn stamp_text<'stamp>(
    summary: &'stamp HashMap<String, String>,
    key: &str,
) -> Result<&'stamp str, MicroBatchError> {
    summary
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| MicroBatchError::Catalog(format!("repark.cdc stamp misses {key}")))
}

fn parse_stamp_uuid(text: &str, key: &str) -> Result<Uuid, MicroBatchError> {
    Uuid::parse_str(text).map_err(|_| {
        MicroBatchError::Catalog(format!(
            "repark.cdc stamp value for {key} is not a uuid: {text:?}"
        ))
    })
}

fn parse_stamp_number(text: &str, key: &str) -> Result<u64, MicroBatchError> {
    text.parse::<u64>().map_err(|_| {
        MicroBatchError::Catalog(format!(
            "repark.cdc stamp value for {key} is not a number: {text:?}"
        ))
    })
}

fn stamp_member<'value>(
    value: &'value serde_json::Value,
    key: &str,
) -> Result<&'value serde_json::Value, MicroBatchError> {
    value
        .get(key)
        .ok_or_else(|| MicroBatchError::Catalog(format!("repark.cdc stamp misses {key}")))
}

fn stamp_u64(value: &serde_json::Value, key: &str) -> Result<u64, MicroBatchError> {
    let member = stamp_member(value, key)?;
    member.as_u64().ok_or_else(|| {
        MicroBatchError::Catalog(format!("repark.cdc stamp value for {key} is not a number"))
    })
}

fn stamp_str<'value>(
    value: &'value serde_json::Value,
    key: &str,
) -> Result<&'value str, MicroBatchError> {
    let member = stamp_member(value, key)?;
    member.as_str().ok_or_else(|| {
        MicroBatchError::Catalog(format!("repark.cdc stamp value for {key} is not a string"))
    })
}

fn stamp_i64(value: &serde_json::Value, key: &str) -> Result<i64, MicroBatchError> {
    let member = stamp_member(value, key)?;
    member.as_i64().ok_or_else(|| {
        MicroBatchError::Catalog(format!("repark.cdc stamp value for {key} is not a number"))
    })
}

fn parse_input_offset(value: &serde_json::Value) -> Result<InputOffset, MicroBatchError> {
    let table = TableUuid::new(parse_stamp_uuid(
        stamp_str(value, OFFSET_TABLE_UUID_KEY)?,
        OFFSET_TABLE_UUID_KEY,
    )?);
    let table_name = stamp_str(value, OFFSET_TABLE_KEY)?.to_string();
    let snapshot = SnapshotId::new(stamp_i64(value, OFFSET_SNAPSHOT_ID_KEY)?);
    let position = FilePosition::new(stamp_u64(value, OFFSET_POSITION_KEY)?);
    Ok(InputOffset {
        table,
        table_name,
        snapshot,
        position,
    })
}

fn parse_stamp_inputs(entries: &[serde_json::Value]) -> Result<OffsetVector, MicroBatchError> {
    let mut inputs = Vec::with_capacity(entries.len());
    for entry in entries {
        inputs.push(parse_input_offset(entry)?);
    }
    OffsetVector::try_from_inputs(inputs)
}

#[cfg(test)]
mod tests {
    use iceberg::NamespaceIdent;
    use iceberg::TableCreation;
    use iceberg::TableIdent;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use tempfile::TempDir;

    use super::*;

    const ORACLE_SINK: &str = "12345678-1234-5678-1234-567812345678";
    const ORACLE_NAMED_ORDERS: &str = "d1243454-ef41-5afd-844f-9067d1e5b4de";
    const ORACLE_UNNAMED: &str = "b86143e5-76ad-5b6b-bf6b-65ce0c90fb12";

    fn fixture_uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).expect("valid fixture uuid")
    }

    fn input_offset(table: &str, name: &str, snapshot: i64, position: u64) -> InputOffset {
        InputOffset {
            table: TableUuid::new(fixture_uuid(table)),
            table_name: String::from(name),
            snapshot: SnapshotId::new(snapshot),
            position: FilePosition::new(position),
        }
    }

    fn sink_record() -> SinkRecord {
        let offsets = OffsetVector::single(input_offset(ORACLE_SINK, "bronze.events", 42, 3));
        SinkRecord {
            format: OffsetFormatVersion::CURRENT,
            query: QueryId::new(fixture_uuid("aaaaaaaa-0000-4000-8000-000000000001")),
            run: RunId::new(fixture_uuid("bbbbbbbb-0000-4000-8000-000000000002")),
            epoch: Epoch::new(2),
            generation: Generation::new(1).expect("positive generation"),
            offsets,
        }
    }

    fn summary_map(record: &SinkRecord, door: SinkDoor) -> HashMap<String, String> {
        record.summary_entries(door).into_iter().collect()
    }

    #[test]
    fn newtypes_new_get_round_trip() {
        assert_eq!(SnapshotId::new(-5).get(), -5);
        assert_eq!(FilePosition::new(0).get(), 0);
        let raw = fixture_uuid(ORACLE_SINK);
        assert_eq!(TableUuid::new(raw).get(), raw);
        assert_eq!(QueryId::new(raw).get(), raw);
        assert_eq!(RunId::new(raw).get(), raw);
        assert_eq!(Epoch::new(9).get(), 9);
        assert_eq!(OffsetFormatVersion::new(1).get(), 1);
        assert_eq!(OffsetFormatVersion::CURRENT.get(), 1);
    }

    #[test]
    fn epoch_first_next_get() {
        assert_eq!(Epoch::FIRST.get(), 0);
        assert_eq!(Epoch::FIRST.next().get(), 1);
        assert_eq!(Epoch::new(5).next().get(), 6);
    }

    #[test]
    fn run_id_fresh_yields_unique_v4() {
        let first = RunId::fresh();
        let second = RunId::fresh();
        assert_ne!(first, second);
        assert_eq!(first.get().get_version(), Some(uuid::Version::Random));
    }

    #[test]
    fn query_id_derive_matches_independent_vector() {
        let sink = TableUuid::new(fixture_uuid(ORACLE_SINK));
        assert_eq!(
            QueryId::derive(sink, Some("orders")).get(),
            fixture_uuid(ORACLE_NAMED_ORDERS)
        );
        assert_eq!(
            QueryId::derive(sink, None).get(),
            fixture_uuid(ORACLE_UNNAMED)
        );
    }

    #[test]
    fn query_id_derive_is_deterministic_and_sensitive() {
        let sink = TableUuid::new(fixture_uuid(ORACLE_SINK));
        let other = TableUuid::new(fixture_uuid("22345678-1234-5678-1234-567812345678"));
        assert_eq!(
            QueryId::derive(sink, Some("orders")),
            QueryId::derive(sink, Some("orders"))
        );
        assert_ne!(
            QueryId::derive(sink, Some("orders")),
            QueryId::derive(sink, Some("shipments"))
        );
        assert_ne!(
            QueryId::derive(sink, Some("orders")),
            QueryId::derive(sink, None)
        );
        assert_ne!(QueryId::derive(sink, None), QueryId::derive(sink, Some("")));
        assert_ne!(
            QueryId::derive(sink, Some("orders")),
            QueryId::derive(other, Some("orders"))
        );
    }

    #[tokio::test]
    async fn table_uuid_of_matches_table_metadata() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let path = warehouse.path().to_str().expect("utf-8 warehouse path");
        let catalog = crate::memory_catalog(path).await.expect("memory catalog");
        catalog
            .create_namespace(&NamespaceIdent::new(String::from("sales")), HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("build schema");
        let creation = TableCreation::builder()
            .name(String::from("events"))
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog
            .create_table(&NamespaceIdent::new(String::from("sales")), creation)
            .await
            .expect("create table");
        let ident = TableIdent::new(
            NamespaceIdent::new(String::from("sales")),
            String::from("events"),
        );
        let table = catalog.load_table(&ident).await.expect("load table");
        assert_eq!(TableUuid::of(&table).get(), table.metadata().uuid());
    }

    #[test]
    fn offset_vector_single_get_inputs() {
        let first = input_offset(ORACLE_SINK, "bronze.a", 10, 1);
        let second = input_offset("22345678-1234-5678-1234-567812345678", "bronze.b", 20, 2);
        let vector =
            OffsetVector::try_from_inputs(vec![first.clone(), second.clone()]).expect("two inputs");
        assert_eq!(vector.inputs().len(), 2);
        assert_eq!(
            vector.get(first.table).expect("first input").table_name,
            "bronze.a"
        );
        assert_eq!(
            vector.get(second.table).expect("second input").table_name,
            "bronze.b"
        );
        let missing = TableUuid::new(fixture_uuid("33345678-1234-5678-1234-567812345678"));
        assert_eq!(vector.get(missing), None);
        let single = OffsetVector::single(first.clone());
        assert_eq!(single.inputs(), std::slice::from_ref(&first));
    }

    #[test]
    fn offset_vector_try_from_inputs_sorts_by_table_uuid() {
        let lower = input_offset(ORACLE_SINK, "bronze.a", 10, 1);
        let higher = input_offset("22345678-1234-5678-1234-567812345678", "bronze.b", 20, 2);
        let vector =
            OffsetVector::try_from_inputs(vec![higher, lower.clone()]).expect("two inputs sort");
        assert_eq!(vector.inputs()[0], lower);
        assert_eq!(vector.inputs()[1].table_name, "bronze.b");
    }

    #[test]
    fn offset_vector_try_from_inputs_refuses_empty_and_duplicates() {
        let empty = OffsetVector::try_from_inputs(Vec::new());
        assert!(
            matches!(empty, Err(MicroBatchError::Catalog(message)) if message.contains("at least one input"))
        );
        let input = input_offset(ORACLE_SINK, "bronze.a", 10, 1);
        let duplicated = OffsetVector::try_from_inputs(vec![input.clone(), input]);
        assert!(
            matches!(duplicated, Err(MicroBatchError::Catalog(message)) if message.contains("twice"))
        );
    }

    #[test]
    fn summary_entries_round_trip_on_both_doors() {
        let record = sink_record();
        let table_entries = record.summary_entries(SinkDoor::Table);
        assert_eq!(table_entries.len(), 8);
        let table_summary: HashMap<String, String> = table_entries.into_iter().collect();
        assert_eq!(
            table_summary
                .get(SPARK_QUERY_ID_KEY)
                .expect("spark query id"),
            &record.query.get().to_string()
        );
        assert_eq!(
            table_summary
                .get(SPARK_EPOCH_ID_KEY)
                .expect("spark epoch id"),
            &record.epoch.get().to_string()
        );
        let back = SinkRecord::from_summary(&table_summary)
            .expect("parse table stamp")
            .expect("stamp present");
        assert_eq!(back, record);

        let batch_entries = record.summary_entries(SinkDoor::ForeachBatch);
        assert_eq!(batch_entries.len(), 6);
        let batch_summary: HashMap<String, String> = batch_entries.into_iter().collect();
        assert!(!batch_summary.contains_key(SPARK_QUERY_ID_KEY));
        assert!(!batch_summary.contains_key(SPARK_EPOCH_ID_KEY));
        let back = SinkRecord::from_summary(&batch_summary)
            .expect("parse batch stamp")
            .expect("stamp present");
        assert_eq!(back, record);
    }

    #[test]
    fn property_round_trip_through_from_property() {
        let record = sink_record();
        let (key, value) = record.property();
        assert_eq!(
            key,
            format!("{OFFSETS_PROPERTY_PREFIX}{query}", query = record.query)
        );
        let back = SinkRecord::from_property(record.query, &value).expect("parse property stamp");
        assert_eq!(back, record);
    }

    #[test]
    fn from_summary_returns_none_without_stamp() {
        let empty: HashMap<String, String> = HashMap::new();
        assert_eq!(SinkRecord::from_summary(&empty).expect("no stamp"), None);
        let foreign: HashMap<String, String> =
            HashMap::from([(String::from("operation"), String::from("append"))]);
        assert_eq!(
            SinkRecord::from_summary(&foreign).expect("foreign stamp"),
            None
        );
    }

    #[test]
    fn from_summary_refuses_bad_version_and_partial_stamp() {
        let future: HashMap<String, String> =
            HashMap::from([(FORMAT_VERSION_KEY.to_string(), String::from("2"))]);
        assert!(matches!(
            SinkRecord::from_summary(&future),
            Err(MicroBatchError::UnsupportedOffsetFormat {
                found: 2,
                supported: 1
            })
        ));
        let garbage: HashMap<String, String> =
            HashMap::from([(FORMAT_VERSION_KEY.to_string(), String::from("abc"))]);
        assert!(matches!(
            SinkRecord::from_summary(&garbage),
            Err(MicroBatchError::Catalog(message)) if message.contains(FORMAT_VERSION_KEY)
        ));
        let mut partial = summary_map(&sink_record(), SinkDoor::Table);
        partial.remove(EPOCH_KEY).expect("epoch present");
        assert!(matches!(
            SinkRecord::from_summary(&partial),
            Err(MicroBatchError::Catalog(message)) if message.contains(EPOCH_KEY)
        ));
        let mut corrupt = summary_map(&sink_record(), SinkDoor::Table);
        corrupt.insert(OFFSETS_KEY.to_string(), String::from("not json"));
        assert!(matches!(
            SinkRecord::from_summary(&corrupt),
            Err(MicroBatchError::Catalog(message)) if message.contains("is not JSON")
        ));
    }

    #[test]
    fn from_property_refuses_corrupt_value() {
        let record = sink_record();
        let corrupt = SinkRecord::from_property(record.query, "not json");
        assert!(matches!(
            corrupt,
            Err(MicroBatchError::Catalog(message)) if message.contains("is not JSON")
        ));
        let missing = SinkRecord::from_property(
            record.query,
            r#"{"format-version":1,"run-id":"bbbbbbbb-0000-4000-8000-000000000002","epoch":2,"generation":1}"#,
        );
        assert!(matches!(
            missing,
            Err(MicroBatchError::Catalog(message)) if message.contains("inputs")
        ));
    }

    #[test]
    fn spark_source_offset_json_matches_spark_shape() {
        let offset = input_offset(ORACLE_SINK, "bronze.events", 42, 3);
        assert_eq!(
            spark_source_offset_json(&offset),
            "{\"version\":1,\"snapshot_id\":42,\"position\":3,\"scan_all_files\":false}"
        );
    }
}
