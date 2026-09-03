use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, ManifestList, PrimitiveLiteral,
};
use iceberg::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::write::merge::iceberg_err;

const LEGACY_IO_CONCURRENCY: usize = 8;

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

fn referenced_data_file_location(delete_file: &DataFile) -> Option<Cow<'_, str>> {
    if delete_file.content_type() == DataContentType::EqualityDeletes {
        return None;
    }
    if let Some(referenced) = delete_file.referenced_data_file() {
        return Some(Cow::Owned(referenced));
    }
    let lower = delete_file
        .lower_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    let upper = delete_file
        .upper_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    match (lower.literal(), upper.literal()) {
        (PrimitiveLiteral::String(lower), PrimitiveLiteral::String(upper)) if lower == upper => {
            Some(Cow::Borrowed(lower.as_str()))
        }
        _ => None,
    }
}

async fn load_manifests(
    table: &Table,
    manifest_list: &ManifestList,
    content: ManifestContentType,
) -> Result<Vec<iceberg::spec::Manifest>> {
    let file_io = table.file_io().clone();
    stream::iter(
        manifest_list
            .entries()
            .iter()
            .filter(|manifest_file| manifest_file.content == content),
    )
    .map(move |manifest_file| {
        let file_io = file_io.clone();
        async move { manifest_file.load_manifest(&file_io).await }
    })
    .buffer_unordered(LEGACY_IO_CONCURRENCY)
    .try_collect()
    .await
    .map_err(iceberg_err)
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

    let delete_manifests =
        load_manifests(table, &manifest_list, ManifestContentType::Deletes).await?;
    let mut candidates: Vec<(DataFile, String, Option<i64>)> = Vec::new();
    for manifest in &delete_manifests {
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
            if !touched.contains(referenced.as_ref()) {
                continue;
            }
            let referenced = referenced.into_owned();
            candidates.push((delete_file.clone(), referenced, entry.sequence_number()));
        }
    }
    if candidates.is_empty() {
        return Ok(superseded);
    }

    let data_manifests = load_manifests(table, &manifest_list, ManifestContentType::Data).await?;
    let mut live_data: HashMap<&str, Option<i64>> = HashMap::new();
    for manifest in &data_manifests {
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            if !touched.contains(file.file_path()) {
                continue;
            }
            live_data.insert(file.file_path(), entry.sequence_number());
        }
    }

    let applicable: Vec<(DataFile, String)> = candidates
        .into_iter()
        .filter_map(|(delete_file, referenced, delete_sequence)| {
            let data_sequence = live_data.get(referenced.as_str()).copied()?;
            applies(delete_sequence, data_sequence).then_some((delete_file, referenced))
        })
        .collect();
    if applicable.is_empty() {
        return Ok(superseded);
    }

    let read: Vec<(DataFile, String, Vec<u64>)> = stream::iter(applicable.into_iter().map(
        |(delete_file, referenced)| async move {
            let positions = read_position_delete_file(table, &delete_file, &referenced).await?;
            Ok::<_, DataFusionError>((delete_file, referenced, positions))
        },
    ))
    .buffer_unordered(LEGACY_IO_CONCURRENCY)
    .try_collect()
    .await?;

    for (delete_file, referenced, positions) in read {
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
    let path = delete_file.file_path();
    let bytes = table
        .file_io()
        .new_input(path)
        .map_err(iceberg_err)?
        .read()
        .await
        .map_err(iceberg_err)?;
    positions_from_parquet(bytes, path, referenced, delete_file.record_count())
}

fn positions_from_parquet<R: parquet::file::reader::ChunkReader + 'static>(
    source: R,
    path: &str,
    referenced: &str,
    record_count: u64,
) -> Result<Vec<u64>> {
    positions_from_parquet_projected(source, path, referenced, record_count, true)
}

fn positions_from_parquet_projected<R: parquet::file::reader::ChunkReader + 'static>(
    source: R,
    path: &str,
    referenced: &str,
    record_count: u64,
    project: bool,
) -> Result<Vec<u64>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(source)
        .map_err(|error| parquet_err(path, &error))?;
    let builder =
        match reserved_projection(builder.schema(), builder.parquet_schema()).filter(|_| project) {
            Some(mask) => builder.with_projection(mask),
            None => builder,
        };
    let reader = builder.build().map_err(|error| parquet_err(path, &error))?;
    let capacity = usize::try_from(record_count).unwrap_or_default();
    let mut positions = Vec::with_capacity(capacity);
    for batch in reader {
        let batch = batch.map_err(|error| parquet_err(path, &error))?;
        let projected = batch.schema();
        let path_index = column_index(
            &projected,
            RESERVED_FIELD_ID_DELETE_FILE_PATH,
            RESERVED_COL_NAME_DELETE_FILE_PATH,
            path,
        )?;
        let pos_index = column_index(
            &projected,
            RESERVED_FIELD_ID_DELETE_FILE_POS,
            RESERVED_COL_NAME_DELETE_FILE_POS,
            path,
        )?;
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

fn reserved_projection(
    schema: &ArrowSchema,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Option<ProjectionMask> {
    if parquet_schema.num_columns() != schema.fields().len() {
        return None;
    }
    let path = leaf_index(
        schema,
        RESERVED_FIELD_ID_DELETE_FILE_PATH,
        RESERVED_COL_NAME_DELETE_FILE_PATH,
    )?;
    let pos = leaf_index(
        schema,
        RESERVED_FIELD_ID_DELETE_FILE_POS,
        RESERVED_COL_NAME_DELETE_FILE_POS,
    )?;
    Some(ProjectionMask::leaves(parquet_schema, [path, pos]))
}

fn leaf_index(schema: &ArrowSchema, field_id: i32, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|raw| raw.parse::<i32>().ok())
                == Some(field_id)
        })
        .or_else(|| {
            schema
                .fields()
                .iter()
                .position(|field| field.name() == name)
        })
}

fn parquet_err(delete_path: &str, error: &dyn std::fmt::Display) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(format!(
        "legacy position delete `{delete_path}`: {error}"
    ))))
}

fn column_index(
    schema: &ArrowSchema,
    field_id: i32,
    name: &str,
    delete_path: &str,
) -> Result<usize> {
    leaf_index(schema, field_id, name).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "legacy position delete `{delete_path}`: no column with field id {field_id} or name `{name}`"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn field(name: &str, data_type: DataType, field_id: i32) -> Field {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    fn write_delete_file(dir: &tempfile::TempDir, with_row_column: bool) -> std::fs::File {
        let mut fields = vec![
            field(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ];
        let paths = StringArray::from(vec!["a.parquet", "b.parquet", "a.parquet"]);
        let ordinals = Int64Array::from(vec![7_i64, 99, 3]);
        let mut columns: Vec<datafusion::arrow::array::ArrayRef> =
            vec![Arc::new(paths), Arc::new(ordinals)];
        if with_row_column {
            fields.insert(0, field("row", DataType::Int32, 2_147_483_544));
            columns.insert(0, Arc::new(Int32Array::from(vec![1_i32, 2, 3])));
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).expect("batch");
        let path = dir.path().join(if with_row_column {
            "with-row.parquet"
        } else {
            "plain.parquet"
        });
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        std::fs::File::open(&path).expect("open")
    }

    #[test]
    fn positions_are_filtered_to_the_referenced_data_file() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let source = write_delete_file(&dir, false);
        let positions =
            positions_from_parquet(source, "d.parquet", "a.parquet", 3).expect("positions");
        assert_eq!(
            positions,
            vec![7, 3],
            "only the rows naming the referenced data file are taken"
        );
    }

    fn write_wide_delete_file(
        dir: &tempfile::TempDir,
        rows: usize,
        with_row_column: bool,
    ) -> std::path::PathBuf {
        let mut fields = vec![
            field(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ];
        let paths = StringArray::from(vec!["a.parquet"; rows]);
        let ordinals = Int64Array::from(
            (0..rows)
                .map(|row| i64::try_from(row).unwrap_or_default())
                .collect::<Vec<_>>(),
        );
        let mut columns: Vec<datafusion::arrow::array::ArrayRef> =
            vec![Arc::new(paths), Arc::new(ordinals)];
        if with_row_column {
            fields.push(field("row", DataType::Utf8, 2_147_483_544));
            columns.push(Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| format!("row-{row}-{}", "payload".repeat(8)))
                    .collect::<Vec<_>>(),
            )));
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).expect("batch");
        let path = dir.path().join(format!("wide-{with_row_column}.parquet"));
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        path
    }

    #[test]
    #[ignore = "measurement: projected vs full decode of a 100k-position legacy delete file"]
    fn measure_projected_decode() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let rows = 100_000usize;
        let record_count = u64::try_from(rows).unwrap_or_default();
        for with_row_column in [false, true, false, true] {
            let path = write_wide_delete_file(&dir, rows, with_row_column);
            let bytes = std::fs::metadata(&path).expect("meta").len();

            let started = std::time::Instant::now();
            let source = std::fs::File::open(&path).expect("open");
            let full = positions_from_parquet_projected(
                source,
                "d.parquet",
                "a.parquet",
                record_count,
                false,
            )
            .expect("read");
            let full_elapsed = started.elapsed();

            let started = std::time::Instant::now();
            let source = std::fs::File::open(&path).expect("open");
            let projected = positions_from_parquet(source, "d.parquet", "a.parquet", record_count)
                .expect("read");
            let projected_elapsed = started.elapsed();

            assert_eq!(full.len(), rows);
            assert_eq!(projected.len(), rows);
            println!(
                "MEASURE projected-decode: row_column={with_row_column} file={bytes} B                  full={full_elapsed:?} projected={projected_elapsed:?}"
            );
        }
    }

    #[test]
    fn a_leading_row_column_is_projected_away_without_shifting_the_reserved_columns() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let source = write_delete_file(&dir, true);
        let positions =
            positions_from_parquet(source, "d.parquet", "a.parquet", 3).expect("positions");
        assert_eq!(
            positions,
            vec![7, 3],
            "the optional `row` column is never decoded and never shifts `file_path` / `pos`"
        );
    }
}
