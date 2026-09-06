//! Public bulk append through the fork's `fast_append` transaction path.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures::channel::mpsc;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::{FieldMatchMode, RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::{Catalog, TableIdent};
use uuid::Uuid;

use crate::write::conform::{conform_batch, conform_batches, write_default_column_names};
use crate::write::distribution::{route_partitioned_stream, send_routed};
use crate::write::merge::{OPERATION_ID_PROP, write_data_files_with_concurrency};
use crate::write::writer_props::writer_properties_for;
use crate::write::{concurrency::WriteConcurrency, file_order::ascending_partition_order};

/// Append record batches to an Iceberg table — the sanctioned add-only commit path.
/// # Errors
/// A missing table surfaces the catalog load error; a non-Parquet default is `NotImplemented`.
pub async fn append(
    catalog: &Arc<dyn Catalog>,
    table_ident: &TableIdent,
    batches: Vec<RecordBatch>,
) -> Result<Table> {
    let table = catalog.load_table(table_ident).await.map_err(iceberg_err)?;
    reject_unsupported_append(&table)?;

    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = conform_batches(&write_schema, &write_default_columns, &batches)?;

    // Public `append` has no session handle — use the engine default concurrency (4).
    let concurrency = WriteConcurrency::default();
    let new_files = if conformed.is_empty() {
        Vec::new()
    } else if table.metadata().default_partition_spec().is_unpartitioned() {
        write_data_files_with_concurrency(&table, conformed, concurrency).await?
    } else {
        fanout_data_files_with_concurrency(&table, conformed, concurrency).await?
    };
    commit_append(catalog, &table, new_files).await
}

/// The one remaining append scope gate, checked before any IO: only Parquet data files are written.
fn reject_unsupported_append(table: &Table) -> Result<()> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "append writes only Parquet data files yet (table default is {file_format})"
        )));
    }
    Ok(())
}

/// Write batches as identity-partitioned Parquet files, sibling of `write_data_files`.
/// # Errors
/// A batch with a missing, extra, or duplicate column (unless the missing column carries an
/// Iceberg `write-default`), an uncastable/overflowing value, or a NULL
pub async fn write_partitioned_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFile>> {
    write_partitioned_data_files_with_concurrency(table, batches, WriteConcurrency::default()).await
}

/// [`write_partitioned_data_files`] with explicit [`WriteConcurrency`].
/// # Errors
/// Same as [`write_partitioned_data_files`].
pub async fn write_partitioned_data_files_with_concurrency(
    table: &Table,
    batches: Vec<RecordBatch>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = conform_batches(&write_schema, &write_default_columns, &batches)?;
    if conformed.is_empty() {
        return Ok(Vec::new());
    }
    fanout_data_files_with_concurrency(table, conformed, concurrency).await
}

/// Streaming sibling of `write_partitioned_data_files`: fan out each batch as it arrives.
/// # Errors
/// A batch with a missing, extra, or duplicate column (unless the missing column carries an
/// Iceberg `write-default`), an uncastable/overflowing value, or a NULL
pub async fn write_partitioned_data_files_from_stream<S>(
    table: &Table,
    stream: S,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    write_partitioned_data_files_from_stream_with_concurrency(
        table,
        stream,
        WriteConcurrency::default(),
    )
    .await
}

/// [`write_partitioned_data_files_from_stream`] with explicit [`WriteConcurrency`].
/// # Errors
/// Same as [`write_partitioned_data_files_from_stream`].
pub async fn write_partitioned_data_files_from_stream_with_concurrency<S>(
    table: &Table,
    stream: S,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed =
        stream.map(move |item| conform_batch(&write_schema, &write_default_columns, &item?));
    fanout_conformed_stream_with_concurrency(table, conformed, concurrency).await
}

/// The identity-partition fanout core over ALREADY-CONFORMED batches (callers: [`append`] after
async fn fanout_data_files_with_concurrency(
    table: &Table,
    batches: Vec<RecordBatch>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    fanout_conformed_stream_with_concurrency(
        table,
        futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        concurrency,
    )
    .await
}

/// Identity-partition fanout with optional concurrent batch workers.
async fn fanout_conformed_stream_with_concurrency<S>(
    table: &Table,
    mut conformed: S,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let max_concurrent = concurrency.max_concurrent_files.max(1);
    if max_concurrent == 1 {
        return crate::write::distribution::fanout_sorted_serial(table, &mut conformed).await;
    }

    // P1-R1: abort flag so workers do not close fanouts after a source/sibling failure.
    let aborted = Arc::new(AtomicBool::new(false));
    let mut senders = Vec::with_capacity(max_concurrent);
    let mut worker_futures = Vec::with_capacity(max_concurrent);
    for _ in 0..max_concurrent {
        let (tx, rx) = mpsc::channel::<RecordBatch>(1);
        let aborted = Arc::clone(&aborted);
        worker_futures.push(async move {
            let stream = rx.map(Ok::<RecordBatch, DataFusionError>);
            crate::write::distribution::fanout_sorted_stream(table, stream, aborted).await
        });
        senders.push(tx);
    }

    let aborted_for_dispatch = Arc::clone(&aborted);
    let mut routed = route_partitioned_stream(table, max_concurrent, conformed)?;
    let dispatcher = async move {
        loop {
            match routed.next().await {
                None => {
                    drop(senders);
                    return Ok::<(), DataFusionError>(());
                }
                Some(Ok(parts)) => {
                    if !send_routed(&mut senders, parts).await {
                        aborted_for_dispatch.store(true, Ordering::SeqCst);
                        drop(senders);
                        return Err(DataFusionError::Execution(
                            "partitioned write worker channel closed before the source was exhausted"
                                .into(),
                        ));
                    }
                }
                Some(Err(error)) => {
                    aborted_for_dispatch.store(true, Ordering::SeqCst);
                    drop(senders);
                    return Err(error);
                }
            }
        }
    };

    let (dispatch_result, worker_results) =
        futures::future::join(dispatcher, futures::future::join_all(worker_futures)).await;

    let written: usize = worker_results.iter().flatten().map(Vec::len).sum();
    let mut files = Vec::with_capacity(written);
    let mut first_worker_error: Option<DataFusionError> = None;
    for result in worker_results {
        match result {
            Ok(part) => files.extend(part),
            Err(error) if first_worker_error.is_none() => first_worker_error = Some(error),
            Err(_) => {}
        }
    }
    if let Some(error) = first_worker_error {
        return Err(error);
    }
    dispatch_result?;
    Ok(ascending_partition_order(files))
}

/// Single-writer fanout loop (the historical serial body of `fanout_conformed_stream`).
pub(crate) async fn fanout_conformed_stream_serial<S>(
    table: &Table,
    conformed: &mut S,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let no_abort = AtomicBool::new(false);
    let files = fanout_conformed_stream_serial_with_abort(table, conformed, &no_abort).await?;
    Ok(ascending_partition_order(files))
}

/// Serial fanout that checks `aborted` between batches and after the stream ends.
pub(crate) async fn fanout_conformed_stream_serial_with_abort<S>(
    table: &Table,
    conformed: &mut S,
    aborted: &AtomicBool,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )
    .map_err(iceberg_err)?;

    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        writer_properties_for(table)?,
        table.metadata().current_schema().clone(),
        FieldMatchMode::Name,
    );
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(iceberg_err)?;
    let file_name_generator =
        DefaultFileNameGenerator::new(Uuid::new_v4().to_string(), None, file_format);
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        table_props.write_target_file_size_bytes,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut fanout = FanoutWriter::new(DataFileWriterBuilder::new(rolling_builder));

    while let Some(batch) = conformed.try_next().await? {
        if aborted.load(Ordering::SeqCst) {
            // Drop fanout without close — no finished partial DataFiles.
            return Ok(Vec::new());
        }
        if batch.num_rows() == 0 {
            continue;
        }
        for (partition_key, partition_batch) in splitter.split(&batch).map_err(iceberg_err)? {
            if let Err(error) = fanout.write(partition_key, partition_batch).await {
                aborted.store(true, Ordering::SeqCst);
                return Err(iceberg_err(error));
            }
        }
    }
    if aborted.load(Ordering::SeqCst) {
        return Ok(Vec::new());
    }
    fanout.close().await.map_err(iceberg_err)
}

/// One stamped `fast_append` commit: `ENGINE_CONTRACT` §4 INSERT/append with MERGE's stamp class.
/// # Errors
/// Returns the fork's transaction/commit error (folded to this crate's error type) when the append
pub async fn commit_append(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    new_files: Vec<DataFile>,
) -> Result<Table> {
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let action = tx
        .fast_append()
        .add_data_files(new_files)
        .set_snapshot_properties(summary);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(catalog.as_ref()).await.map_err(iceberg_err)
}

/// Fold an iceberg error into the DataFusion error this crate's callers carry.
fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    crate::catalog::iceberg_to_datafusion(err)
}

/// Append pins (downstream ask A1).
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use iceberg::expr::{Predicate, Reference};
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, Datum, Literal, ManifestContentType, NestedField,
        Operation, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
    };
    use iceberg::{CatalogBuilder, Namespace, NamespaceIdent, TableCommit, TableCreation};
    use tempfile::TempDir;

    use super::*;

    /// An in-memory Iceberg catalog over a local-FS warehouse with a `sales` namespace.
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

    /// `key int` (required unless `nullable_key`) + `payload string` (optional).
    fn table_schema(nullable_key: bool) -> Schema {
        let key = if nullable_key {
            NestedField::optional(1, "key", Type::Primitive(PrimitiveType::Int))
        } else {
            NestedField::required(1, "key", Type::Primitive(PrimitiveType::Int))
        };
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                key.into(),
                NestedField::optional(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("build schema")
    }

    /// Create `sales.<name>`.
    async fn create_table(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        nullable_key: bool,
        partitioned: bool,
        properties: HashMap<String, String>,
    ) -> TableIdent {
        let creation = if partitioned {
            let spec = UnboundPartitionSpec::builder()
                .add_partition_field(1, "key_part", Transform::Identity)
                .expect("add identity partition field")
                .build();
            TableCreation::builder()
                .name(name.to_string())
                .schema(table_schema(nullable_key))
                .properties(properties)
                .partition_spec(spec)
                .build()
        } else {
            TableCreation::builder()
                .name(name.to_string())
                .schema(table_schema(nullable_key))
                .properties(properties)
                .build()
        };
        catalog
            .create_table(&NamespaceIdent::new("sales".to_string()), creation)
            .await
            .expect("create table");
        TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
    }

    /// A consumer-shaped batch: plain Arrow fields.
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

    /// Scan read-back asserts Int32/Utf8 types, then returns sorted `(key, payload)` rows.
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
            assert_eq!(
                batch.schema().field(0).data_type(),
                &DataType::Int32,
                "key must read back as Int32 (value AND type)"
            );
            assert_eq!(
                batch.schema().field(1).data_type(),
                &DataType::Utf8,
                "payload must read back as Utf8 (value AND type)"
            );
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("key column downcasts to Int32Array");
            let payloads = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload column downcasts to StringArray");
            for row in 0..batch.num_rows() {
                let key = (!keys.is_null(row)).then(|| keys.value(row));
                let payload = (!payloads.is_null(row)).then(|| payloads.value(row).to_string());
                rows.push((key, payload));
            }
        }
        rows.sort();
        rows
    }

    /// The live DATA-file entries in the current snapshot's manifests.
    async fn live_data_files(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<DataFile> {
        let table = catalog.load_table(ident).await.expect("load table");
        let metadata = table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Vec::new();
        };
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("load manifest list");
        let mut files = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("load manifest");
            for entry in manifest.entries() {
                if entry.is_alive() {
                    files.push(entry.data_file().clone());
                }
            }
        }
        files
    }

    /// The data-file paths a filtered scan PLANS.
    async fn planned_paths(table: &Table, predicate: Predicate) -> HashSet<String> {
        let scan = table
            .scan()
            .with_filter(predicate)
            .select(["key", "payload"])
            .build()
            .expect("build filtered scan");
        let tasks: Vec<_> = scan
            .plan_files()
            .await
            .expect("plan files")
            .try_collect()
            .await
            .expect("collect planned tasks");
        tasks
            .iter()
            .map(|task| task.data_file_path().to_string())
            .collect()
    }

    /// Number of snapshots in the table's history.
    async fn snapshot_count(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> usize {
        let table = catalog.load_table(ident).await.expect("load table");
        table.metadata().snapshots().count()
    }

    /// The boxed-future return type of an `#[async_trait]` `Catalog` method.
    type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

    /// Catalog wrapper that lands a competing PUBLIC append in the victim's first `update_table`.
    #[derive(Debug)]
    struct ConflictInjector {
        inner: Arc<dyn Catalog>,
        victim_ident: TableIdent,
        update_table_attempts: AtomicUsize,
    }

    impl Catalog for ConflictInjector {
        fn list_namespaces<'life0, 'life1, 'async_trait>(
            &'life0 self,
            parent: Option<&'life1 NamespaceIdent>,
        ) -> BoxedCatalogFuture<'async_trait, Vec<NamespaceIdent>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.list_namespaces(parent)
        }

        fn create_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> BoxedCatalogFuture<'async_trait, Namespace>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.create_namespace(namespace, properties)
        }

        fn get_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, Namespace>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.get_namespace(namespace)
        }

        fn namespace_exists<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, bool>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.namespace_exists(namespace)
        }

        fn update_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.update_namespace(namespace, properties)
        }

        fn drop_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.drop_namespace(namespace)
        }

        fn list_tables<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, Vec<TableIdent>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.list_tables(namespace)
        }

        fn create_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
            creation: TableCreation,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.create_table(namespace, creation)
        }

        fn load_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.load_table(table)
        }

        fn drop_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.drop_table(table)
        }

        fn table_exists<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, bool>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.table_exists(table)
        }

        fn rename_table<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            src: &'life1 TableIdent,
            dest: &'life2 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.rename_table(src, dest)
        }

        fn register_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
            metadata_location: String,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.register_table(table, metadata_location)
        }

        fn update_table<'life0, 'async_trait>(
            &'life0 self,
            commit: TableCommit,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let attempt = self.update_table_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    // The victim's `do_commit` has already refreshed its base.
                    append(
                        &self.inner,
                        &self.victim_ident,
                        vec![consumer_batch(&[Some(2)], &[Some("competitor")])],
                    )
                    .await
                    .expect("the injected competing append must commit");
                }
                self.inner.update_table(commit).await
            })
        }
    }

    /// PIN P1 — unpartitioned append via the NEW public API round-trips value AND type.
    #[tokio::test]
    async fn append_unpartitioned_roundtrip_value_and_type() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, false, HashMap::new()).await;

        let committed = append(
            &catalog,
            &ident,
            vec![
                consumer_batch(&[Some(1), Some(2)], &[Some("a"), None]),
                consumer_batch(&[Some(3)], &[Some("c")]),
            ],
        )
        .await
        .expect("unpartitioned append must commit");

        let snapshot = committed
            .metadata()
            .current_snapshot()
            .expect("append produced a snapshot");
        assert_eq!(snapshot.summary().operation, Operation::Append);
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("a".to_string())),
                (Some(2), None),
                (Some(3), Some("c".to_string())),
            ],
        );
    }

    /// PIN P2.
    #[tokio::test]
    async fn append_partitioned_single_value_stamps_datafile_partition() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(
                &[Some(7), Some(7), Some(7)],
                &[Some("a"), Some("b"), Some("c")],
            )],
        )
        .await
        .expect("partitioned append must commit");

        let files = live_data_files(&catalog, &ident).await;
        assert!(!files.is_empty(), "the append must produce data files");
        let mut rows = 0;
        for file in &files {
            assert_eq!(
                file.partition().fields(),
                &[Some(Literal::int(7))],
                "every DataFile must carry partition value key_part=7, got {:?}",
                file.partition()
            );
            rows += file.record_count();
        }
        assert_eq!(
            rows, 3,
            "manifest record counts must cover all appended rows"
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(7), Some("a".to_string())),
                (Some(7), Some("b".to_string())),
                (Some(7), Some("c".to_string())),
            ],
        );
    }

    /// PIN U1-P12.
    #[tokio::test]
    async fn write_partitioned_data_files_conforms_view_typed_batches() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;

        // `category string` identity-partitioned, field name = column name (the CTAS shape).
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "category", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("build schema");
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "category", Transform::Identity)
            .expect("add identity partition field")
            .build();
        let creation = TableCreation::builder()
            .name("viewkeys".to_string())
            .schema(schema)
            .partition_spec(spec)
            .build();
        catalog
            .create_table(&NamespaceIdent::new("sales".to_string()), creation)
            .await
            .expect("create table");
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "viewkeys".to_string(),
        );
        let table = catalog.load_table(&ident).await.expect("load table");

        // A batch whose partition column is Utf8View — the DataFusion view-type class.
        let batch_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("category", DataType::Utf8View, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringViewArray::from(vec![Some("a"), Some("b"), Some("a")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
        )
        .expect("build view-typed batch");

        let files = write_partitioned_data_files(&table, vec![batch])
            .await
            .expect("a view-typed partition column must conform and write");
        let mut partitions: Vec<_> = files
            .iter()
            .map(|file| file.partition().fields().to_vec())
            .collect();
        partitions.sort_by_key(|fields| format!("{fields:?}"));
        assert_eq!(
            partitions,
            vec![
                vec![Some(Literal::string("a"))],
                vec![Some(Literal::string("b"))],
            ],
            "one file per distinct category, each stamped with its own value"
        );
        assert_eq!(
            files.iter().map(DataFile::record_count).sum::<u64>(),
            3,
            "all rows must be written"
        );
    }

    /// PIN P3.
    #[tokio::test]
    async fn append_partitioned_multi_value_fanout_splits_files_and_roundtrips() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        // Interleaved keys within a batch AND across batches — the unsorted consumer shape.
        append(
            &catalog,
            &ident,
            vec![
                consumer_batch(
                    &[Some(1), Some(2), Some(1), Some(2), Some(1)],
                    &[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
                ),
                consumer_batch(&[Some(2), Some(1)], &[Some("f"), Some("g")]),
            ],
        )
        .await
        .expect("multi-partition append must commit");

        let files = live_data_files(&catalog, &ident).await;
        let mut rows_by_partition: HashMap<Option<Literal>, u64> = HashMap::new();
        for file in &files {
            let slot = file.partition().fields().first().cloned().flatten();
            *rows_by_partition.entry(slot).or_insert(0) += file.record_count();
        }
        assert_eq!(
            rows_by_partition,
            HashMap::from([(Some(Literal::int(1)), 4), (Some(Literal::int(2)), 3),]),
            "one file set per partition value, manifest counts matching the routed rows"
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("a".to_string())),
                (Some(1), Some("c".to_string())),
                (Some(1), Some("e".to_string())),
                (Some(1), Some("g".to_string())),
                (Some(2), Some("b".to_string())),
                (Some(2), Some("d".to_string())),
                (Some(2), Some("f".to_string())),
            ],
        );
    }

    /// PIN P4.
    #[tokio::test]
    async fn append_partitioned_scan_prunes_planned_files_to_matching_partition() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(
                &[Some(1), Some(2), Some(1), Some(2)],
                &[Some("a"), Some("b"), Some("c"), Some("d")],
            )],
        )
        .await
        .expect("append must commit");

        let files = live_data_files(&catalog, &ident).await;
        let key1_paths: HashSet<String> = files
            .iter()
            .filter(|file| file.partition().fields() == [Some(Literal::int(1))])
            .map(|file| file.file_path().to_string())
            .collect();
        let all_paths: HashSet<String> = files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        assert!(
            !key1_paths.is_empty() && key1_paths.len() < all_paths.len(),
            "the fanout must have produced distinct file sets per partition ({all_paths:?})"
        );

        let table = catalog.load_table(&ident).await.expect("load table");
        let planned = planned_paths(&table, Reference::new("key").equal_to(Datum::int(1))).await;
        assert_eq!(
            planned, key1_paths,
            "a key=1 scan must plan ONLY the key_part=1 files"
        );

        // And the filtered read returns exactly the matching rows.
        let scan = table
            .scan()
            .with_filter(Reference::new("key").equal_to(Datum::int(1)))
            .select(["key", "payload"])
            .build()
            .expect("build filtered scan");
        let batches: Vec<RecordBatch> = scan
            .to_arrow()
            .await
            .expect("filtered to_arrow")
            .try_collect()
            .await
            .expect("collect filtered rows");
        let mut rows: Vec<(i32, String)> = Vec::new();
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("key column is Int32");
            let payloads = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload column is Utf8");
            for row in 0..batch.num_rows() {
                rows.push((keys.value(row), payloads.value(row).to_string()));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, "a".to_string()), (1, "c".to_string())],
            "the pruned scan must return exactly the key=1 rows"
        );
    }

    /// PIN P5.
    #[tokio::test]
    async fn append_partitioned_null_key_lands_null_partition_slot() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", true, true, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1), None], &[Some("a"), Some("b")])],
        )
        .await
        .expect("null-key append must commit (Java writes a null partition slot)");

        let files = live_data_files(&catalog, &ident).await;
        let slots: HashSet<Option<Literal>> = files
            .iter()
            .map(|file| file.partition().fields().first().cloned().flatten())
            .collect();
        assert_eq!(
            slots,
            HashSet::from([Some(Literal::int(1)), None]),
            "one file set under key_part=1 and one under the NULL slot"
        );
        let null_file = files
            .iter()
            .find(|file| file.partition().fields() == [None])
            .expect("a data file with the null partition slot exists");
        assert!(
            null_file.file_path().contains("key_part=null"),
            "the null partition renders `key_part=null` in the path (Java parity), got {}",
            null_file.file_path()
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (None, Some("b".to_string())),
                (Some(1), Some("a".to_string())),
            ],
        );
    }

    /// PIN P6a — append to a missing table is a loud error naming the table; nothing lands.
    #[tokio::test]
    async fn append_missing_table_errors_loud() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let missing = TableIdent::new(NamespaceIdent::new("sales".to_string()), "nope".to_string());

        let error = append(
            &catalog,
            &missing,
            vec![consumer_batch(&[Some(1)], &[None])],
        )
        .await
        .expect_err("append to a missing table must fail");
        assert!(
            error.to_string().contains("nope"),
            "the error must name the missing table, got: {error}"
        );
    }

    /// PIN P6b.
    #[tokio::test]
    async fn append_schema_mismatch_errors_and_commits_nothing() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        // Missing column — zero-row batch, so the check must run BEFORE empties are dropped.
        let missing_payload = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .expect("build zero-row batch");
        let error = append(&catalog, &ident, vec![missing_payload])
            .await
            .expect_err("a batch missing a target column must fail");
        assert!(
            error.to_string().contains("missing column `payload`"),
            "must name the missing column, got: {error}"
        );

        // Extra column.
        let extra = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("key", DataType::Int32, true),
                Field::new("payload", DataType::Utf8, true),
                Field::new("intruder", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int32Array::from(vec![9])),
            ],
        )
        .expect("build extra-column batch");
        let error = append(&catalog, &ident, vec![extra])
            .await
            .expect_err("a batch with an unknown column must fail");
        assert!(
            error.to_string().contains("column `intruder`"),
            "must name the extra column, got: {error}"
        );

        // NULL in the required key column.
        let error = append(
            &catalog,
            &ident,
            vec![consumer_batch(&[None], &[Some("a")])],
        )
        .await
        .expect_err("a NULL in a required column must fail");
        assert!(
            error.to_string().contains("key"),
            "must name the non-nullable column, got: {error}"
        );

        // Nothing landed: no snapshot, no files, no rows.
        assert_eq!(snapshot_count(&catalog, &ident).await, 0);
        assert!(live_data_files(&catalog, &ident).await.is_empty());
        assert!(read_back_sorted(&catalog, &ident).await.is_empty());
    }

    /// PIN P7.
    #[tokio::test]
    async fn append_empty_input_commits_empty_append_snapshot_java_parity() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        let committed = append(&catalog, &ident, vec![])
            .await
            .expect("zero-batch append must commit (Java BatchAppend parity)");
        let snapshot = committed
            .metadata()
            .current_snapshot()
            .expect("empty append still produces a snapshot");
        assert_eq!(snapshot.summary().operation, Operation::Append);
        assert!(
            snapshot
                .summary()
                .additional_properties
                .contains_key(OPERATION_ID_PROP),
            "the empty snapshot is stamped"
        );
        assert_eq!(snapshot_count(&catalog, &ident).await, 1);
        assert!(live_data_files(&catalog, &ident).await.is_empty());

        append(&catalog, &ident, vec![consumer_batch(&[], &[])])
            .await
            .expect("zero-row-batch append must commit too");
        assert_eq!(snapshot_count(&catalog, &ident).await, 2);
        assert!(live_data_files(&catalog, &ident).await.is_empty());
        assert!(read_back_sorted(&catalog, &ident).await.is_empty());
    }

    /// PIN P8a.
    #[tokio::test]
    async fn append_commit_seam_racing_append_both_land() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, false, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("base append");
        let stale = catalog.load_table(&ident).await.expect("load stale handle");

        // A concurrent public append lands AFTER the stale handle was taken.
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("concurrent append");

        // Write real files against the stale handle, then commit from it.
        let write_schema = Arc::new(
            schema_to_arrow_schema(stale.metadata().current_schema()).expect("arrow schema"),
        );
        let conformed = conform_batches(
            &write_schema,
            &HashSet::new(),
            &[consumer_batch(&[Some(3)], &[Some("c")])],
        )
        .expect("conform batch");
        let files = crate::write::write_data_files(&stale, conformed)
            .await
            .expect("write files");
        commit_append(&catalog, &stale, files)
            .await
            .expect("the stale-based append must land via refresh-and-re-apply, not fail");

        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("a".to_string())),
                (Some(2), Some("b".to_string())),
                (Some(3), Some("c".to_string())),
            ],
            "both racing appends and the base row must all be live (append×append commutes)"
        );
        assert_eq!(snapshot_count(&catalog, &ident).await, 3);
    }

    /// PIN P8a.
    #[tokio::test]
    async fn append_racing_append_both_land() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("first append");
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("second append");

        assert_eq!(snapshot_count(&catalog, &ident).await, 2);
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("a".to_string())),
                (Some(2), Some("b".to_string())),
            ],
        );
    }

    /// PIN P8b.
    #[tokio::test]
    async fn public_append_files_trip_serializable_validation_pinned_before_append() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, false, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("base append");
        let table_at_pin = catalog
            .load_table(&ident)
            .await
            .expect("load pinned handle");
        let pin = table_at_pin
            .metadata()
            .current_snapshot()
            .expect("pinned snapshot")
            .snapshot_id();

        // The concurrent add goes through the REAL public append.
        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("concurrent public append");

        // A MERGE-shaped serializable add-only commit pinned BEFORE that append must reject.
        let insert = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/merge-insert.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("build synthetic insert file");
        let tx = Transaction::new(&table_at_pin);
        let action = tx
            .overwrite_files()
            .add_files(vec![insert])
            .conflict_detection_filter(Predicate::AlwaysTrue)
            .validate_no_conflicting_data()
            .validate_from_snapshot(pin);
        let tx = action.apply(tx).expect("apply overwrite");
        let error = tx
            .commit(catalog.as_ref())
            .await
            .expect_err("the serializable validation must reject the public append's files");
        assert_eq!(error.kind(), iceberg::ErrorKind::DataInvalid);
        assert!(!error.retryable(), "a validation conflict is non-retryable");
        assert!(
            error.message().contains("Found conflicting files"),
            "must be the added-data conflict, got: {}",
            error.message()
        );
    }

    /// PIN P9.
    #[tokio::test]
    async fn append_snapshot_stamps_engine_operation_id() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        let first = append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("a")])],
        )
        .await
        .expect("first append");
        let first_snapshot = first.metadata().current_snapshot().expect("first snapshot");
        assert_eq!(first_snapshot.summary().operation, Operation::Append);
        let first_id = first_snapshot
            .summary()
            .additional_properties
            .get(OPERATION_ID_PROP)
            .expect("append snapshot carries the engine.operation-id stamp");
        Uuid::from_str(first_id).expect("the stamp is a UUID");

        let second = append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2)], &[Some("b")])],
        )
        .await
        .expect("second append");
        let second_id = second
            .metadata()
            .current_snapshot()
            .expect("second snapshot")
            .summary()
            .additional_properties
            .get(OPERATION_ID_PROP)
            .expect("second stamp present")
            .clone();
        assert_ne!(
            first_id, &second_id,
            "operation ids are unique per commit (the §8 reconciliation key)"
        );
    }

    /// PIN P11.
    #[tokio::test]
    async fn append_a1_acceptance_identity_partitioned_end_to_end() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        // Bulk: several unsorted batches, three partitions.
        append(
            &catalog,
            &ident,
            vec![
                consumer_batch(&[Some(10), Some(30)], &[Some("a"), Some("b")]),
                consumer_batch(
                    &[Some(20), Some(10), Some(30)],
                    &[Some("c"), Some("d"), Some("e")],
                ),
                consumer_batch(&[Some(20)], &[Some("f")]),
            ],
        )
        .await
        .expect("the A1 bulk append must commit");

        // Rows land correctly (value AND type).
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(10), Some("a".to_string())),
                (Some(10), Some("d".to_string())),
                (Some(20), Some("c".to_string())),
                (Some(20), Some("f".to_string())),
                (Some(30), Some("b".to_string())),
                (Some(30), Some("e".to_string())),
            ],
        );

        // Partition pruning at plan level: key=20 plans only the key_part=20 file set.
        let files = live_data_files(&catalog, &ident).await;
        let key20_paths: HashSet<String> = files
            .iter()
            .filter(|file| file.partition().fields() == [Some(Literal::int(20))])
            .map(|file| file.file_path().to_string())
            .collect();
        let all_paths: HashSet<String> = files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        let table = catalog.load_table(&ident).await.expect("load table");
        let planned = planned_paths(&table, Reference::new("key").equal_to(Datum::int(20))).await;
        assert_eq!(
            planned, key20_paths,
            "pruned plan touches only key_part=20 files"
        );
        assert!(
            planned.len() < all_paths.len(),
            "pruning must exclude the other partitions' files"
        );
    }

    /// PIN P7.
    #[tokio::test]
    async fn append_bucket_partition_routes_by_fork_hash() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int32Type;
        use iceberg::transform::create_transform_function;

        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "key_bucket", Transform::Bucket(4))
            .expect("add bucket partition field")
            .build();
        let creation = TableCreation::builder()
            .name("bucketed".to_string())
            .schema(table_schema(false))
            .partition_spec(spec)
            .properties(HashMap::new())
            .build();
        catalog
            .create_table(&NamespaceIdent::new("sales".to_string()), creation)
            .await
            .expect("create bucketed table");
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "bucketed".to_string(),
        );

        // Distinct keys are spread so IDENTITY fallthrough cannot look like a bucket ordinal.
        let keys: Vec<i32> = vec![1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233];
        let payloads: Vec<String> = keys.iter().map(|k| format!("p{k}")).collect();
        let key_opts: Vec<Option<i32>> = keys.iter().map(|k| Some(*k)).collect();
        let payload_opts: Vec<Option<&str>> = payloads.iter().map(|p| Some(p.as_str())).collect();

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&key_opts, &payload_opts)],
        )
        .await
        .expect("bucket-partitioned append must commit via computed-mode fanout");

        // Self-oracle: the fork's own Bucket(4) transform over the same keys is the ground truth.
        let bucket_fn = create_transform_function(&Transform::Bucket(4)).expect("bucket fn");
        let buckets = bucket_fn
            .transform(Arc::new(Int32Array::from(keys.clone())))
            .expect("apply bucket transform");
        let buckets = buckets.as_primitive::<Int32Type>();
        let valid_slots: HashSet<Option<Literal>> = (0..4).map(|b| Some(Literal::int(b))).collect();
        let mut expected_rows_by_bucket: HashMap<Option<Literal>, u64> = HashMap::new();
        for row in 0..buckets.len() {
            let bucket = buckets.value(row);
            *expected_rows_by_bucket
                .entry(Some(Literal::int(bucket)))
                .or_insert(0) += 1;
        }
        assert!(
            expected_rows_by_bucket.len() >= 2,
            "the key spread must fan out across >= 2 buckets to prove routing, got {expected_rows_by_bucket:?}"
        );

        let files = live_data_files(&catalog, &ident).await;
        let mut actual_rows_by_bucket: HashMap<Option<Literal>, u64> = HashMap::new();
        for file in &files {
            let slot = file.partition().fields().first().cloned().flatten();
            assert!(
                valid_slots.contains(&slot),
                "committed partition slot must be a bucket ordinal 0..4 (not the identity key), got {slot:?}"
            );
            *actual_rows_by_bucket.entry(slot).or_insert(0) += file.record_count();
        }
        assert_eq!(
            actual_rows_by_bucket, expected_rows_by_bucket,
            "manifest bucket routing must match the fork's own Bucket(4) hash"
        );

        // Full round-trip: every row survives, value AND type.
        let mut expected: Vec<(Option<i32>, Option<String>)> = keys
            .iter()
            .zip(payloads.iter())
            .map(|(k, p)| (Some(*k), Some(p.clone())))
            .collect();
        expected.sort();
        assert_eq!(read_back_sorted(&catalog, &ident).await, expected);
    }

    /// PIN P13.
    #[tokio::test]
    async fn append_non_parquet_default_rejected() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(
            &catalog,
            "t",
            false,
            true,
            HashMap::from([("write.format.default".to_string(), "avro".to_string())]),
        )
        .await;

        let error = append(&catalog, &ident, vec![consumer_batch(&[Some(1)], &[None])])
            .await
            .expect_err("a non-Parquet default must be rejected");
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "expected NotImplemented, got: {error}"
        );
        assert!(
            error.to_string().contains("avro"),
            "the error must name the table's format, got: {error}"
        );
        assert_eq!(snapshot_count(&catalog, &ident).await, 0, "nothing lands");
    }

    /// PIN P14.
    #[tokio::test]
    async fn append_duplicate_column_batch_rejected_and_commits_nothing() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2), Some(1)], &[Some("a"), Some("b")])],
        )
        .await
        .expect("seed append");

        let duplicate = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("key", DataType::Int32, true),
                Field::new("key", DataType::Int32, true),
                Field::new("payload", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![999])),
                Arc::new(StringArray::from(vec!["dup"])),
            ],
        )
        .expect("build duplicate-column batch");
        let error = append(&catalog, &ident, vec![duplicate])
            .await
            .expect_err("a batch with duplicate column names must fail loudly, never drop one");
        assert!(
            error.to_string().contains("`key` is ambiguous")
                && error.to_string().contains("`key`, `key`"),
            "the error must name the ambiguous target and both colliding copies, got: {error}"
        );

        // Nothing landed: the seed snapshot is still the only one, rows unchanged.
        assert_eq!(
            snapshot_count(&catalog, &ident).await,
            1,
            "the rejected append must not commit"
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("b".to_string())),
                (Some(2), Some("a".to_string())),
            ],
        );
    }

    /// PIN P15.
    #[tokio::test]
    async fn append_overflowing_cast_rejected_and_commits_nothing() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", true, true, HashMap::new()).await;

        let overflowing = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("payload", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![5_000_000_000_i64])),
                Arc::new(StringArray::from(vec!["overflow"])),
            ],
        )
        .expect("build overflowing batch");
        let error = append(&catalog, &ident, vec![overflowing])
            .await
            .expect_err("an Int64 value overflowing the Int32 key must error, never NULL-fill");
        assert!(
            error.to_string().contains("5000000000") && error.to_string().contains("Int32"),
            "the error must name the overflowing value and target type, got: {error}"
        );

        assert_eq!(snapshot_count(&catalog, &ident).await, 0, "nothing lands");
        assert!(live_data_files(&catalog, &ident).await.is_empty());
        assert!(read_back_sorted(&catalog, &ident).await.is_empty());
    }

    /// PIN P16.
    #[tokio::test]
    async fn append_reordered_widening_batch_conforms_by_name_and_roundtrips() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        // Columns deliberately reversed versus the write schema `[key, payload]`.
        let reordered = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("payload", DataType::Utf8, true),
                Field::new("key", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![2_i64, 1, 2])),
            ],
        )
        .expect("build reordered batch");
        append(&catalog, &ident, vec![reordered])
            .await
            .expect("a reordered batch with in-range Int64 keys must conform by name");

        // Manifest level: the widened keys route to THEIR partitions, not positional ones.
        let files = live_data_files(&catalog, &ident).await;
        let mut rows_by_partition: HashMap<Option<Literal>, u64> = HashMap::new();
        for file in &files {
            let slot = file.partition().fields().first().cloned().flatten();
            *rows_by_partition.entry(slot).or_insert(0) += file.record_count();
        }
        assert_eq!(
            rows_by_partition,
            HashMap::from([(Some(Literal::int(1)), 1), (Some(Literal::int(2)), 2)]),
            "the cast keys must carry their own partition values in the manifest"
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("b".to_string())),
                (Some(2), Some("a".to_string())),
                (Some(2), Some("c".to_string())),
            ],
        );
    }

    /// PIN PL-1.
    #[tokio::test]
    async fn append_case_insensitive_column_match_conforms_and_roundtrips() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        // Source columns UPPERCASED versus the write schema `[key, payload]`.
        let upper_cased = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("KEY", DataType::Int32, true),
                Field::new("PAYLOAD", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![2, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("build upper-cased batch");
        append(&catalog, &ident, vec![upper_cased])
            .await
            .expect("a case-differing source batch must conform by name (Spark default)");

        // Manifest: rows route to the partition of THEIR resolved `key`, not a positional guess.
        let files = live_data_files(&catalog, &ident).await;
        let mut rows_by_partition: HashMap<Option<Literal>, u64> = HashMap::new();
        for file in &files {
            let slot = file.partition().fields().first().cloned().flatten();
            *rows_by_partition.entry(slot).or_insert(0) += file.record_count();
        }
        assert_eq!(
            rows_by_partition,
            HashMap::from([(Some(Literal::int(1)), 1), (Some(Literal::int(2)), 2)]),
            "case-resolved keys must carry their own partition values in the manifest"
        );
        // read_back_sorted downcasts to Int32/Utf8 — the value AND Arrow type on the scan path.
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("b".to_string())),
                (Some(2), Some("a".to_string())),
                (Some(2), Some("c".to_string())),
            ],
        );
    }

    /// PIN PL-2.
    #[tokio::test]
    async fn append_case_ambiguous_columns_rejected_naming_both() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        append(
            &catalog,
            &ident,
            vec![consumer_batch(&[Some(2), Some(1)], &[Some("a"), Some("b")])],
        )
        .await
        .expect("seed append");

        let ambiguous = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("key", DataType::Int32, true),
                Field::new("KEY", DataType::Int32, true),
                Field::new("payload", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![999])),
                Arc::new(StringArray::from(vec!["dup"])),
            ],
        )
        .expect("build case-colliding batch");
        let error = append(&catalog, &ident, vec![ambiguous])
            .await
            .expect_err("two source columns colliding on one target must fail loudly");
        assert!(
            error.to_string().contains("`key` is ambiguous")
                && error.to_string().contains("`key`, `KEY`"),
            "the error must name the ambiguous target and both colliding source columns, got: {error}"
        );

        // Nothing landed: the seed snapshot is still the only one, rows unchanged.
        assert_eq!(
            snapshot_count(&catalog, &ident).await,
            1,
            "the rejected ambiguous append must not commit"
        );
        assert_eq!(
            read_back_sorted(&catalog, &ident).await,
            vec![
                (Some(1), Some("b".to_string())),
                (Some(2), Some("a".to_string())),
            ],
        );
    }

    /// PIN PL-3.
    #[tokio::test]
    async fn append_missing_target_column_rejected_and_commits_nothing() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;

        // Only `key` — the required-optional `payload` target column is absent from the source.
        let missing_payload = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .expect("build missing-column batch");
        let error = append(&catalog, &ident, vec![missing_payload])
            .await
            .expect_err("a batch missing a target column must fail loudly");
        assert!(
            error.to_string().contains("missing column `payload`"),
            "the error must name the missing target column, got: {error}"
        );
        assert_eq!(snapshot_count(&catalog, &ident).await, 0, "nothing lands");
        assert!(read_back_sorted(&catalog, &ident).await.is_empty());
    }

    /// PIN P17.
    #[tokio::test]
    async fn append_midflight_commit_conflict_retries_and_both_appends_land() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let inner = memory_catalog(&warehouse).await;
        // Retry backoff is deterministic: the conflict is injected by call count, never by timing.
        let ident = create_table(
            &inner,
            "t",
            false,
            true,
            HashMap::from([
                ("commit.retry.min-wait-ms".to_string(), "1".to_string()),
                ("commit.retry.max-wait-ms".to_string(), "5".to_string()),
            ]),
        )
        .await;

        let injector = Arc::new(ConflictInjector {
            inner: inner.clone(),
            victim_ident: ident.clone(),
            update_table_attempts: AtomicUsize::new(0),
        });
        let injecting_catalog: Arc<dyn Catalog> = injector.clone();

        append(
            &injecting_catalog,
            &ident,
            vec![consumer_batch(&[Some(1)], &[Some("victim")])],
        )
        .await
        .expect("the victim append must land via the fork's CAS-rejection retry, not fail");

        assert_eq!(
            injector.update_table_attempts.load(Ordering::SeqCst),
            2,
            "exactly two update_table attempts: the rejected CAS, then the successful retry"
        );
        assert_eq!(
            snapshot_count(&inner, &ident).await,
            2,
            "one snapshot per landed append — no lost update, no duplicate commit"
        );
        assert_eq!(
            read_back_sorted(&inner, &ident).await,
            vec![
                (Some(1), Some("victim".to_string())),
                (Some(2), Some("competitor".to_string())),
            ],
            "both the victim and the mid-flight competitor must be live"
        );
    }

    /// WG-2 P3.
    #[tokio::test]
    async fn write_partitioned_data_files_from_stream_aborts_on_midstream_error() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_table(&catalog, "t", false, true, HashMap::new()).await;
        let table = catalog.load_table(&ident).await.expect("load table");

        let items: Vec<Result<RecordBatch>> = vec![
            Ok(consumer_batch(&[Some(1)], &[Some("a")])),
            Ok(consumer_batch(&[Some(2)], &[Some("b")])),
            Err(DataFusionError::Execution(
                "injected mid-stream source failure".to_string(),
            )),
        ];
        let error = write_partitioned_data_files_from_stream(&table, futures::stream::iter(items))
            .await
            .expect_err("a mid-stream source error must abort the streaming write");
        assert!(
            error
                .to_string()
                .contains("injected mid-stream source failure"),
            "the seam must surface the source error loudly, got: {error}"
        );
        // The seam never commits — the table stays at its empty initial state (no snapshot).
        assert!(
            live_data_files(&catalog, &ident).await.is_empty(),
            "an aborted streaming write must commit nothing"
        );
    }
}
