use std::collections::{HashMap, HashSet};

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::error::{DataFusionError, Result};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, PrimitiveLiteral,
};
use iceberg::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::write::merge::iceberg_err;

#[derive(Default)]
pub(super) struct SupersededLegacyDeletes {
    pub positions: HashMap<String, Vec<u64>>,
    pub files: Vec<DataFile>,
}

impl SupersededLegacyDeletes {
    pub(super) fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

fn is_deletion_vector(delete_file: &DataFile) -> bool {
    delete_file.file_format() == DataFileFormat::Puffin
}

fn referenced_data_file_location(delete_file: &DataFile) -> Option<String> {
    if delete_file.content_type() == DataContentType::EqualityDeletes {
        return None;
    }
    if let Some(referenced) = delete_file.referenced_data_file() {
        return Some(referenced);
    }
    let lower = delete_file
        .lower_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    let upper = delete_file
        .upper_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    match (lower.literal(), upper.literal()) {
        (PrimitiveLiteral::String(lower), PrimitiveLiteral::String(upper)) if lower == upper => {
            Some(lower.clone())
        }
        _ => None,
    }
}

pub(super) async fn collect_superseded_legacy_deletes(
    table: &Table,
    touched: &HashSet<&str>,
    snapshot_id: Option<i64>,
) -> Result<SupersededLegacyDeletes> {
    let mut superseded = SupersededLegacyDeletes::default();
    if touched.is_empty() {
        return Ok(superseded);
    }
    let metadata = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => metadata.snapshot_by_id(id),
        None => metadata.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(superseded);
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .map_err(iceberg_err)?;

    let mut candidates: Vec<(DataFile, String, Option<i64>)> = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(iceberg_err)?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let delete_file = entry.data_file();
            if delete_file.content_type() != DataContentType::PositionDeletes
                || is_deletion_vector(delete_file)
            {
                continue;
            }
            let Some(referenced) = referenced_data_file_location(delete_file) else {
                continue;
            };
            if !touched.contains(referenced.as_str()) {
                continue;
            }
            candidates.push((delete_file.clone(), referenced, entry.sequence_number()));
        }
    }
    if candidates.is_empty() {
        return Ok(superseded);
    }

    let mut live_data: HashMap<String, Option<i64>> = HashMap::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(iceberg_err)?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            if !touched.contains(file.file_path()) {
                continue;
            }
            live_data.insert(file.file_path().to_string(), entry.sequence_number());
        }
    }

    for (delete_file, referenced, delete_sequence) in candidates {
        let Some(data_sequence) = live_data.get(referenced.as_str()).copied() else {
            continue;
        };
        if !applies(delete_sequence, data_sequence) {
            continue;
        }
        let positions = read_position_delete_file(table, &delete_file, &referenced).await?;
        superseded
            .positions
            .entry(referenced)
            .or_default()
            .extend(positions);
        superseded.files.push(delete_file);
    }
    Ok(superseded)
}

fn applies(delete_sequence: Option<i64>, data_sequence: Option<i64>) -> bool {
    match (delete_sequence, data_sequence) {
        (Some(delete_sequence), Some(data_sequence)) => delete_sequence >= data_sequence,
        _ => true,
    }
}

async fn read_position_delete_file(
    table: &Table,
    delete_file: &DataFile,
    referenced: &str,
) -> Result<Vec<u64>> {
    let bytes = table
        .file_io()
        .new_input(delete_file.file_path())
        .map_err(iceberg_err)?
        .read()
        .await
        .map_err(iceberg_err)?;
    let path = delete_file.file_path();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|error| parquet_err(path, &error))?;
    let arrow_schema = builder.schema().clone();
    let path_index = column_index(
        &arrow_schema,
        RESERVED_FIELD_ID_DELETE_FILE_PATH,
        RESERVED_COL_NAME_DELETE_FILE_PATH,
        path,
    )?;
    let pos_index = column_index(
        &arrow_schema,
        RESERVED_FIELD_ID_DELETE_FILE_POS,
        RESERVED_COL_NAME_DELETE_FILE_POS,
        path,
    )?;
    let reader = builder.build().map_err(|error| parquet_err(path, &error))?;
    let mut positions = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| parquet_err(path, &error))?;
        let paths = batch
            .column(path_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "legacy position delete `{path}`: column `{RESERVED_COL_NAME_DELETE_FILE_PATH}` is not Utf8"
                ))
            })?;
        let ordinals = batch
            .column(pos_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "legacy position delete `{path}`: column `{RESERVED_COL_NAME_DELETE_FILE_POS}` is not Int64"
                ))
            })?;
        for row in 0..batch.num_rows() {
            if paths.is_null(row) || ordinals.is_null(row) || paths.value(row) != referenced {
                continue;
            }
            let ordinal = ordinals.value(row);
            let ordinal = u64::try_from(ordinal).map_err(|_| {
                DataFusionError::Internal(format!(
                    "legacy position delete `{path}`: negative row position {ordinal} for data file `{referenced}`"
                ))
            })?;
            positions.push(ordinal);
        }
    }
    Ok(positions)
}

fn parquet_err(delete_path: &str, error: &dyn std::fmt::Display) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(format!(
        "legacy position delete `{delete_path}`: {error}"
    ))))
}

fn column_index(
    schema: &datafusion::arrow::datatypes::Schema,
    field_id: i32,
    name: &str,
    delete_path: &str,
) -> Result<usize> {
    let by_field_id = schema.fields().iter().position(|field| {
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|raw| raw.parse::<i32>().ok())
            == Some(field_id)
    });
    by_field_id
        .or_else(|| schema.fields().iter().position(|field| field.name() == name))
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "legacy position delete `{delete_path}`: no column with field id {field_id} or name `{name}`"
            ))
        })
}
