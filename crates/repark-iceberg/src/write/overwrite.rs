//! OV1 full-table overwrite using stage-then-swap.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use futures::Stream;
use futures::StreamExt;
use futures::future::ready;
use futures::stream::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFile;
use iceberg::table::Table;

use crate::write::append::write_partitioned_data_files_from_stream_with_concurrency;
use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::write_data_files_from_stream_with_concurrency;
use crate::write::store_assign::refuse_unless_write_store_assignable;

/// Table property key for `INSERT OVERWRITE` isolation.
pub const WRITE_OVERWRITE_ISOLATION_LEVEL: &str = "write.overwrite.isolation-level";

/// Isolation level for full-table overwrite §5 validations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverwriteIsolation {
    /// Reject concurrent conflicting deletes only (default when property is absent).
    Snapshot,
    /// Reject concurrent conflicting deletes **and** data.
    Serializable,
}

/// Parse `write.overwrite.isolation-level` exactly as the fork provider does.
/// # Errors
/// [`DataFusionError::Plan`] when the property is present but not a recognized name.
pub fn parse_overwrite_isolation(table: &Table) -> Result<Option<OverwriteIsolation>> {
    match table
        .metadata()
        .properties()
        .get(WRITE_OVERWRITE_ISOLATION_LEVEL)
    {
        None => Ok(Some(OverwriteIsolation::Snapshot)),
        Some(name) if name.eq_ignore_ascii_case("none") => Ok(None),
        Some(name) => match name.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Some(OverwriteIsolation::Serializable)),
            "snapshot" => Ok(Some(OverwriteIsolation::Snapshot)),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid isolation level: {name}"
            ))),
        },
    }
}

/// Stream source batches to staged data files without catalog mutation (OV1 exclusive).
/// # Errors
/// Schema convert, arity/cast/required-omit, or stream write failures as [`DataFusionError`].
pub async fn write_overwrite_staged_files_from_stream<S>(
    table: &Table,
    stream: S,
    column_names: Vec<String>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let write_schema: SchemaRef =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).map_err(iceberg_err)?);
    let mapped = stream
        .map(move |item| {
            let batch = item?;
            positional_map_overwrite_batch(&batch, &write_schema, &column_names)
        })
        .try_filter(|batch| ready(batch.num_rows() > 0));
    if table.metadata().default_partition_spec().is_unpartitioned() {
        write_data_files_from_stream_with_concurrency(table, mapped, concurrency).await
    } else {
        write_partitioned_data_files_from_stream_with_concurrency(table, mapped, concurrency).await
    }
}

/// Commit a full-table overwrite that replaces all live data with `staged_files`.
/// # Errors
/// Invalid isolation property, apply, or catalog commit as [`DataFusionError`].
pub async fn commit_overwrite_replace_all(
    catalog: &std::sync::Arc<dyn iceberg::Catalog>,
    table: &iceberg::table::Table,
    staged_files: Vec<iceberg::spec::DataFile>,
) -> Result<iceberg::table::Table> {
    crate::write::overwrite_commit::commit_overwrite_replace_all_to(
        catalog,
        table,
        staged_files,
        None,
    )
    .await
}

/// SQL INSERT OVERWRITE positional assignment onto the Iceberg write schema (D9).
/// # Errors
/// Fails on arity mismatch, unknown/ambiguous name, required-omit, or a non-assignable pair.
pub fn positional_map_overwrite_batch(
    batch: &RecordBatch,
    table_schema: &SchemaRef,
    column_names: &[String],
) -> Result<RecordBatch> {
    use datafusion::arrow::compute::CastOptions;
    use datafusion::arrow::datatypes::Field;

    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let field_count = table_schema.fields().len();
    let columns: Vec<datafusion::arrow::array::ArrayRef> = if column_names.is_empty() {
        positional_map_all_columns(batch, table_schema, field_count, &strict)?
    } else {
        positional_map_column_list(batch, table_schema, column_names, field_count, &strict)?
    };
    // Table field names + types (positional D9).
    let fields: Vec<Field> = table_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let has_nulls = columns[index].null_count() > 0;
            field
                .as_ref()
                .clone()
                .with_nullable(field.is_nullable() || has_nulls)
        })
        .collect();
    let out_schema = Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        fields,
        table_schema.metadata().clone(),
    ));
    RecordBatch::try_new(out_schema, columns).map_err(|error| {
        DataFusionError::Execution(format!(
            "INSERT OVERWRITE positional batch rebuild failed: {error}"
        ))
    })
}

/// Positional cast of every source column onto the full table schema (empty column list).
fn positional_map_all_columns(
    batch: &RecordBatch,
    table_schema: &SchemaRef,
    field_count: usize,
    strict: &datafusion::arrow::compute::CastOptions<'static>,
) -> Result<Vec<datafusion::arrow::array::ArrayRef>> {
    use datafusion::arrow::compute::cast_with_options;

    if batch.num_columns() != field_count {
        return Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE column count mismatch: source has {} columns, target table \
             has {field_count} (SQL INSERT is positional — OV1 D9)",
            batch.num_columns()
        )));
    }
    table_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            // WI-1: ANSI store assignment BEFORE the kernel.
            refuse_unless_write_store_assignable(
                "INSERT OVERWRITE",
                field.name(),
                batch.column(index).data_type(),
                field.data_type(),
            )?;
            cast_with_options(batch.column(index), field.data_type(), strict).map_err(|error| {
                DataFusionError::Execution(format!(
                    "INSERT OVERWRITE cast of source column {index} to `{}` ({}) failed: {error}",
                    field.name(),
                    field.data_type()
                ))
            })
        })
        .collect()
}

/// Positional map into a column list + null-fill unmentioned (required omit → refuse).
fn positional_map_column_list(
    batch: &RecordBatch,
    table_schema: &SchemaRef,
    column_names: &[String],
    field_count: usize,
    strict: &datafusion::arrow::compute::CastOptions<'static>,
) -> Result<Vec<datafusion::arrow::array::ArrayRef>> {
    use datafusion::arrow::array::new_null_array;
    use datafusion::arrow::compute::cast_with_options;

    if batch.num_columns() != column_names.len() {
        return Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE column-list arity mismatch: source has {} columns, list has \
             {} (SQL INSERT is positional — OV1 D9)",
            batch.num_columns(),
            column_names.len()
        )));
    }
    let mut listed_indices: Vec<usize> = Vec::with_capacity(column_names.len());
    for name in column_names {
        listed_indices.push(resolve_table_field_index_case_insensitive(
            table_schema,
            name,
        )?);
    }
    let mut columns: Vec<Option<datafusion::arrow::array::ArrayRef>> =
        (0..field_count).map(|_| None).collect();
    for (source_index, &target_index) in listed_indices.iter().enumerate() {
        let field = table_schema.field(target_index);
        if columns[target_index].is_some() {
            // Duplicate list entry (same target twice, possibly different case spellings).
            return Err(DataFusionError::Plan(format!(
                "INSERT OVERWRITE column list names `{name}` more than once (duplicate target \
                 field `{}`)",
                field.name(),
                name = column_names[source_index]
            )));
        }
        // WI-1: ANSI store assignment BEFORE the kernel (same gate as the all-columns arm).
        refuse_unless_write_store_assignable(
            "INSERT OVERWRITE",
            field.name(),
            batch.column(source_index).data_type(),
            field.data_type(),
        )?;
        let casted = cast_with_options(batch.column(source_index), field.data_type(), strict)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "INSERT OVERWRITE cast of list column `{}` to `{}` ({}) failed: {error}",
                    column_names[source_index],
                    field.name(),
                    field.data_type()
                ))
            })?;
        columns[target_index] = Some(casted);
    }
    columns
        .into_iter()
        .enumerate()
        .map(|(index, maybe)| {
            if let Some(array) = maybe {
                Ok(array)
            } else {
                let field = table_schema.field(index);
                if !field.is_nullable() {
                    return Err(DataFusionError::Plan(format!(
                        "INSERT OVERWRITE column list omits required field `{}` — refusing \
                         wipe (no default / null-fill for non-nullable)",
                        field.name()
                    )));
                }
                Ok(new_null_array(field.data_type(), batch.num_rows()))
            }
        })
        .collect()
}

/// Case-insensitive unique resolve of `name` against table Arrow fields (list-column path).
fn resolve_table_field_index_case_insensitive(
    table_schema: &SchemaRef,
    name: &str,
) -> Result<usize> {
    let mut found: Option<usize> = None;
    for (index, field) in table_schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(name) {
            if found.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "INSERT OVERWRITE column `{name}` is ambiguous under case-insensitive matching"
                )));
            }
            found = Some(index);
        }
    }
    found.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "INSERT OVERWRITE column `{name}` does not exist in the target table"
        ))
    })
}

fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

// OV1 unit pins — isolation parse matrix + replace-all commit (same commit as code).
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{DataFile, NestedField, Operation, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::write::append::append;
    use crate::write::merge::{OPERATION_ID_PROP, write_data_files};

    async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
        let path = warehouse
            .path()
            .to_str()
            .expect("utf-8 warehouse path")
            .to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
                )
                .await
                .expect("build memory catalog"),
        );
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("create namespace");
        catalog
    }

    fn table_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "key", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("build schema")
    }

    async fn create_table(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        properties: HashMap<String, String>,
    ) -> TableIdent {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(table_schema())
            .properties(properties)
            .build();
        catalog
            .create_table(&NamespaceIdent::new("sales".to_string()), creation)
            .await
            .expect("create table");
        TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
    }

    fn consumer_batch(keys: &[Option<i32>], payloads: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys.to_vec())),
                Arc::new(StringArray::from(payloads.to_vec())),
            ],
        )
        .expect("build consumer batch")
    }

    async fn read_back_sorted(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
    ) -> Vec<(Option<i32>, Option<String>)> {
        let table = catalog.load_table(ident).await.expect("load table");
        let scan = table
            .scan()
            .select(["key", "payload"])
            .build()
            .expect("build scan");
        let batches: Vec<RecordBatch> = scan
            .to_arrow()
            .await
            .expect("scan to_arrow")
            .try_collect()
            .await
            .expect("collect scan batches");
        let mut rows = Vec::new();
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("key Int32");
            let payloads = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload Utf8");
            for row in 0..batch.num_rows() {
                let key = (!keys.is_null(row)).then(|| keys.value(row));
                let payload = (!payloads.is_null(row)).then(|| payloads.value(row).to_string());
                rows.push((key, payload));
            }
        }
        rows.sort();
        rows
    }

    /// Isolation parse: absent → Snapshot (BUG-004 default).
    #[test]
    fn parse_isolation_absent_is_snapshot() {
        // Build a minimal in-memory table via blocking runtime for property-less metadata.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let warehouse = TempDir::new().expect("temp");
            let catalog = memory_catalog(&warehouse).await;
            let ident = create_table(&catalog, "t_absent", HashMap::new()).await;
            let table = catalog.load_table(&ident).await.expect("load");
            assert_eq!(
                parse_overwrite_isolation(&table).expect("parse"),
                Some(OverwriteIsolation::Snapshot)
            );
        });
    }

    /// Isolation parse: explicit `snapshot` → Snapshot (case-insensitive).
    #[test]
    fn parse_isolation_snapshot_case_insensitive() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let warehouse = TempDir::new().expect("temp");
            let catalog = memory_catalog(&warehouse).await;
            let ident = create_table(
                &catalog,
                "t_snap",
                HashMap::from([(
                    WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                    "SnapShot".to_string(),
                )]),
            )
            .await;
            let table = catalog.load_table(&ident).await.expect("load");
            assert_eq!(
                parse_overwrite_isolation(&table).expect("parse"),
                Some(OverwriteIsolation::Snapshot)
            );
        });
    }

    /// Isolation parse: `serializable` → Serializable.
    #[test]
    fn parse_isolation_serializable() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let warehouse = TempDir::new().expect("temp");
            let catalog = memory_catalog(&warehouse).await;
            let ident = create_table(
                &catalog,
                "t_ser",
                HashMap::from([(
                    WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                    "serializable".to_string(),
                )]),
            )
            .await;
            let table = catalog.load_table(&ident).await.expect("load");
            assert_eq!(
                parse_overwrite_isolation(&table).expect("parse"),
                Some(OverwriteIsolation::Serializable)
            );
        });
    }

    /// Isolation parse: `none` → no policy (distinct from absent).
    #[test]
    fn parse_isolation_none_disables_validates() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let warehouse = TempDir::new().expect("temp");
            let catalog = memory_catalog(&warehouse).await;
            let ident = create_table(
                &catalog,
                "t_none",
                HashMap::from([(
                    WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                    "NONE".to_string(),
                )]),
            )
            .await;
            let table = catalog.load_table(&ident).await.expect("load");
            assert_eq!(parse_overwrite_isolation(&table).expect("parse"), None);
        });
    }

    /// Isolation parse: invalid → loud Plan with provider message shape.
    #[test]
    fn parse_isolation_invalid_is_loud() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let warehouse = TempDir::new().expect("temp");
            let catalog = memory_catalog(&warehouse).await;
            let ident = create_table(
                &catalog,
                "t_bad",
                HashMap::from([(
                    WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                    "read-committed".to_string(),
                )]),
            )
            .await;
            let table = catalog.load_table(&ident).await.expect("load");
            let error = parse_overwrite_isolation(&table).expect_err("must refuse");
            let message = error.to_string();
            assert!(
                message.contains("Invalid isolation level: read-committed"),
                "provider message shape, got: {message}"
            );
        });
    }

    /// Primitive: staged files replace all prior rows; operation-id stamp present.
    #[tokio::test]
    async fn commit_overwrite_replace_all_swaps_rows_and_stamps_op_id() {
        let warehouse = TempDir::new().expect("temp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t_swap", HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")])],
        )
        .await
        .expect("seed");

        let table_at_start = catalog.load_table(&ident).await.expect("load start");
        let staged = write_data_files(
            &table_at_start,
            vec![consumer_batch(
                &[Some(9), Some(10)],
                &[Some("x"), Some("y")],
            )],
        )
        .await
        .expect("stage");
        assert!(
            staged.iter().map(DataFile::record_count).sum::<u64>() > 0,
            "staged rows present"
        );

        let committed = commit_overwrite_replace_all(&catalog, &table_at_start, staged)
            .await
            .expect("commit overwrite");
        let snapshot = committed
            .metadata()
            .current_snapshot()
            .expect("overwrite snapshot");
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        let op_id = snapshot
            .summary()
            .additional_properties
            .get(OPERATION_ID_PROP)
            .expect("engine.operation-id stamp");
        Uuid::parse_str(op_id).expect("stamp is UUID");

        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(9), Some("x".to_string())),
                (Some(10), Some("y".to_string())),
            ],
        );
    }

    /// Primitive empty-files wipe (unit-test surface only — SQL non-empty arm must not call).
    #[tokio::test]
    async fn commit_overwrite_replace_all_empty_files_wipes() {
        let warehouse = TempDir::new().expect("temp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t_wipe", HashMap::new()).await;
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("seed");
        let table = catalog.load_table(&ident).await.expect("load");
        commit_overwrite_replace_all(&catalog, &table, Vec::new())
            .await
            .expect("empty-files wipe is correct for the primitive");
        assert!(
            read_back_sorted(&catalog, &ident).await.is_empty(),
            "prior rows must be gone"
        );
    }

    /// Invalid isolation fails at helper entry — no catalog pointer move.
    #[tokio::test]
    async fn commit_overwrite_invalid_isolation_refuses_before_commit() {
        let warehouse = TempDir::new().expect("temp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t_inv",
            HashMap::from([(
                WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                "read-committed".to_string(),
            )]),
        )
        .await;
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("seed");
        let table = catalog.load_table(&ident).await.expect("load");
        let error = commit_overwrite_replace_all(&catalog, &table, Vec::new())
            .await
            .expect_err("invalid isolation must refuse");
        assert!(
            error
                .to_string()
                .contains("Invalid isolation level: read-committed"),
            "got: {error}"
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![(Some(1), Some("a".to_string()))],
            "prior rows intact after refused commit"
        );
    }

    /// Serializable: concurrent append after stream-start pin is rejected.
    #[tokio::test]
    async fn commit_overwrite_serializable_rejects_concurrent_append() {
        let warehouse = TempDir::new().expect("temp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t_ser_occ",
            HashMap::from([(
                WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                "serializable".to_string(),
            )]),
        )
        .await;
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("seed");
        let table_at_pin = catalog.load_table(&ident).await.expect("load pin");
        let staged = write_data_files(
            &table_at_pin,
            vec![consumer_batch(&[Some(9)], &[Some("x")])],
        )
        .await
        .expect("stage");

        // Concurrent append after the pin (same race class as provider serializable OW).
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("concurrent append");

        let error = commit_overwrite_replace_all(&catalog, &table_at_pin, staged)
            .await
            .expect_err("serializable must reject concurrent data");
        let message = error.to_string();
        assert!(
            message.contains("Found conflicting files") || message.contains("conflict"),
            "must be added-data conflict, got: {message}"
        );
        // Prior+concurrent rows still present (commit refused).
        let rows = read_back_sorted(&catalog, &ident).await;
        assert!(
            rows.contains(&(Some(1), Some("a".to_string())))
                && rows.contains(&(Some(2), Some("b".to_string()))),
            "rows after refused OCC: {rows:?}"
        );
    }

    /// Snapshot default: concurrent append is tolerated (deletes-only validation).
    #[tokio::test]
    async fn commit_overwrite_snapshot_tolerates_concurrent_append() {
        let warehouse = TempDir::new().expect("temp");
        let catalog = memory_catalog(&warehouse).await;
        // Absent property → snapshot.
        let ident = create_table(&catalog, "t_snap_occ", HashMap::new()).await;
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("seed");
        let table_at_pin = catalog.load_table(&ident).await.expect("load pin");
        let staged = write_data_files(
            &table_at_pin,
            vec![consumer_batch(&[Some(9)], &[Some("x")])],
        )
        .await
        .expect("stage");
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("concurrent append");

        commit_overwrite_replace_all(&catalog, &table_at_pin, staged)
            .await
            .expect("snapshot isolation tolerates concurrent append");
        // Full wipe wins — concurrent append is replaced (not rejected).
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![(Some(9), Some("x".to_string()))],
        );
    }

    /// `none`: no §5 validates — concurrent append still replaced (commit succeeds).
    #[tokio::test]
    async fn commit_overwrite_none_commits_without_validates() {
        let warehouse = TempDir::new().expect("temp");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t_none_occ",
            HashMap::from([(
                WRITE_OVERWRITE_ISOLATION_LEVEL.to_string(),
                "none".to_string(),
            )]),
        )
        .await;
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("seed");
        let table_at_pin = catalog.load_table(&ident).await.expect("load pin");
        let staged = write_data_files(
            &table_at_pin,
            vec![consumer_batch(&[Some(9)], &[Some("x")])],
        )
        .await
        .expect("stage");
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("concurrent append");
        commit_overwrite_replace_all(&catalog, &table_at_pin, staged)
            .await
            .expect("none = no validates");
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![(Some(9), Some("x".to_string()))],
        );
    }

    /// Arity mismatch (empty column list) refuses loud.
    #[test]
    fn positional_map_all_columns_arity_mismatch_refuses() {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Utf8,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec![Some("only")]))],
        )
        .expect("source");
        let error = positional_map_overwrite_batch(&source, &table_schema, &[])
            .expect_err("arity must refuse");
        assert!(
            error.to_string().contains("column count mismatch"),
            "got: {error}"
        );
    }

    /// Column-list arity mismatch refuses.
    #[test]
    fn positional_map_column_list_arity_mismatch_refuses() {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("extra", DataType::Int32, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        )
        .expect("source");
        let error = positional_map_overwrite_batch(&source, &table_schema, &["key".to_string()])
            .expect_err("list arity must refuse");
        assert!(
            error.to_string().contains("column-list arity mismatch"),
            "got: {error}"
        );
    }

    /// D9 mutation proof: reordered source columns keep SELECT order values under table names.
    #[test]
    fn positional_map_reordered_columns_keep_permuted_values() {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        // Table schema (a, b); source batch is SELECT b, a → columns named b, a with values w/x.
        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Utf8, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("w"), Some("y")])),
                Arc::new(StringArray::from(vec![Some("x"), Some("z")])),
            ],
        )
        .expect("source batch");
        let mapped =
            positional_map_overwrite_batch(&source, &table_schema, &[]).expect("positional map");
        assert_eq!(mapped.schema().field(0).name(), "a");
        assert_eq!(mapped.schema().field(1).name(), "b");
        let col_a = mapped
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a Utf8");
        let col_b = mapped
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("b Utf8");
        // Positional: source col0 (w,y) → a; source col1 (x,z) → b.
        assert_eq!(col_a.value(0), "w");
        assert_eq!(col_b.value(0), "x");
        assert_eq!(col_a.value(1), "y");
        assert_eq!(col_b.value(1), "z");
    }

    /// Column-list path: listed fields filled positionally; unmentioned optional null-filled.
    #[test]
    fn positional_map_column_list_null_fills_unmentioned_optional() {
        use datafusion::arrow::array::{Array, Int32Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "key",
            DataType::Int32,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![Some(7)]))],
        )
        .expect("source");
        let mapped = positional_map_overwrite_batch(&source, &table_schema, &["key".to_string()])
            .expect("list map");
        let keys = mapped
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key");
        let payloads = mapped
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload");
        assert_eq!(keys.value(0), 7);
        assert!(
            payloads.is_null(0),
            "unmentioned optional must be null-filled"
        );
    }

    /// Unknown column-list name refuses.
    #[test]
    fn positional_map_column_list_unknown_name_refuses() {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "x",
            DataType::Int32,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .expect("source");
        let error =
            positional_map_overwrite_batch(&source, &table_schema, &["no_such_col".to_string()])
                .expect_err("unknown name must refuse");
        assert!(
            error
                .to_string()
                .contains("does not exist in the target table"),
            "got: {error}"
        );
    }

    /// Case-ambiguous table fields refuse list resolve.
    #[test]
    fn positional_map_column_list_ambiguous_field_refuses() {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        // Two table fields that collide under case-insensitive match.
        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("KEY", DataType::Int32, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "x",
            DataType::Int32,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .expect("source");
        let error = positional_map_overwrite_batch(&source, &table_schema, &["key".to_string()])
            .expect_err("ambiguous must refuse");
        assert!(error.to_string().contains("ambiguous"), "got: {error}");
    }

    /// Duplicate column-list target refuses (no silent last-wins).
    #[test]
    fn positional_map_column_list_duplicate_target_refuses() {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k1", DataType::Int32, true),
            Field::new("k2", DataType::Int32, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        )
        .expect("source");
        let error = positional_map_overwrite_batch(
            &source,
            &table_schema,
            &["key".to_string(), "KEY".to_string()],
        )
        .expect_err("duplicate target must refuse");
        let message = error.to_string();
        assert!(
            message.contains("more than once") || message.contains("duplicate"),
            "got: {message}"
        );
    }

    /// Required omit on column list refuses (no wipe path at this layer — SQL refuses commit).
    #[test]
    fn positional_map_column_list_required_omit_refuses() {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let table_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec![Some("only")]))],
        )
        .expect("source");
        let error =
            positional_map_overwrite_batch(&source, &table_schema, &["payload".to_string()])
                .expect_err("required key omit must refuse");
        assert!(
            error.to_string().contains("required field `key`"),
            "got: {error}"
        );
    }
}
