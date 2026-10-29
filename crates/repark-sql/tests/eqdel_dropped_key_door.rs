use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::parquet::file::properties::WriterProperties;
use iceberg::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileFormat, SchemaRef};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::{
    AvroWriterBuilder, FileWriterBuilder, OrcWriterBuilder, ParquetWriterBuilder,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const READ: &str = "SELECT data FROM ice.ns.t ORDER BY data";
const DROP_KEY: &str = "ALTER TABLE ice.ns.t DROP COLUMN id";
const RE_ADD_KEY: &str = "ALTER TABLE ice.ns.t ADD COLUMN id INT";
const SURVIVORS: [&str; 3] = ["a", "c", "e"];
const DELETE_FORMATS: [&str; 3] = ["parquet", "avro", "orc"];
const EQUALITY_DELETE_PREFIX: &str = "eqdel";

#[derive(Clone, Copy)]
enum Evolution {
    Dropped,
    DroppedAndReAdded,
}

const EVOLUTIONS: [Evolution; 2] = [Evolution::Dropped, Evolution::DroppedAndReAdded];

struct Reads {
    same_session: Result<Vec<String>, String>,
    fresh_session: Result<Vec<String>, String>,
    native_door: Result<Vec<String>, String>,
}

impl Reads {
    fn each(&self) -> [(&'static str, &Result<Vec<String>, String>); 3] {
        [
            ("same session", &self.same_session),
            ("fresh session", &self.fresh_session),
            ("native door", &self.native_door),
        ]
    }
}

fn ident() -> TableIdent {
    TableIdent::from_strs(["ns", "t"]).expect("ident")
}

fn bare_session(dialect: Arc<dyn SqlDialect>, spark: bool) -> ReparkSession {
    let mut builder = ReparkSession::builder().with_sql_dialect(dialect);
    if spark {
        builder = builder.with_extension(Arc::new(SparkExtension));
    }
    builder.build().expect("session")
}

async fn spark_session(warehouse: &Path) -> ReparkSession {
    let session = bare_session(Arc::new(SparkDialect), true);
    session
        .register_memory_catalog("ice", warehouse.to_str().expect("path"))
        .await
        .expect("catalog");
    session
}

async fn adopted(
    dialect: Arc<dyn SqlDialect>,
    spark: bool,
    warehouse: &Path,
    metadata_location: &str,
) -> ReparkSession {
    let handle = repark_iceberg::catalog::memory_catalog(warehouse.to_str().expect("path"))
        .await
        .expect("memory catalog");
    handle
        .create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new())
        .await
        .expect("namespace");
    handle
        .register_table(&ident(), metadata_location.to_string())
        .await
        .expect("register table");
    let session = bare_session(dialect, spark);
    session
        .register_iceberg_catalog("ice", handle)
        .await
        .expect("catalog");
    session
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, sql: &str) {
    attempt(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

fn catalog(session: &ReparkSession) -> Arc<dyn Catalog> {
    session
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone()
}

async fn load(session: &ReparkSession) -> Table {
    catalog(session)
        .load_table(&ident())
        .await
        .expect("load table")
}

async fn seeded(data_format: &str) -> (ReparkSession, TempDir) {
    let warehouse = TempDir::new().expect("warehouse");
    let session = spark_session(warehouse.path()).await;
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.t (id INT, data STRING) USING iceberg TBLPROPERTIES \
             ('format-version'='2', 'write.format.default'='{data_format}')"
        ),
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (10, 'a'), (20, 'b'), (30, 'c'), (40, 'd'), (50, 'e')",
    )
    .await;
    (session, warehouse)
}

async fn write_equality_delete<B: FileWriterBuilder>(
    table: &Table,
    format: DataFileFormat,
    make_builder: impl FnOnce(SchemaRef) -> B,
) -> DataFile {
    let schema = table.metadata().current_schema();
    let key = schema.field_by_name("id").expect("key column").id;
    let config = EqualityDeleteWriterConfig::new(vec![key], schema.clone()).expect("config");
    let projected = Arc::new(
        arrow_schema_to_schema(config.projected_arrow_schema_ref()).expect("projected schema"),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        make_builder(projected),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata()).expect("location generator"),
        DefaultFileNameGenerator::new(EQUALITY_DELETE_PREFIX.to_string(), None, format),
    );
    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
        .unpartitioned()
        .build(None)
        .await
        .expect("equality-delete writer");
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("arrow schema"));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![20, 40])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x", "x"])) as ArrayRef,
        ],
    )
    .expect("key batch");
    writer.write(batch).await.expect("write keys");
    writer
        .close()
        .await
        .expect("close writer")
        .into_iter()
        .next()
        .expect("one equality-delete file")
}

async fn commit_equality_delete(session: &ReparkSession, delete_format: &str) {
    let table = load(session).await;
    let file = match delete_format {
        "avro" => write_equality_delete(&table, DataFileFormat::Avro, AvroWriterBuilder::new).await,
        "orc" => write_equality_delete(&table, DataFileFormat::Orc, OrcWriterBuilder::new).await,
        _ => {
            write_equality_delete(&table, DataFileFormat::Parquet, |schema| {
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema)
            })
            .await
        }
    };
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .row_delta()
        .add_deletes(vec![file])
        .apply(transaction)
        .expect("apply row delta");
    transaction
        .commit(catalog(session).as_ref())
        .await
        .expect("commit row delta");
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("listing") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

fn file_name(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
}

fn equality_delete_extensions(warehouse: &TempDir) -> Vec<String> {
    files_under(warehouse.path())
        .iter()
        .filter(|path| file_name(path).starts_with(EQUALITY_DELETE_PREFIX))
        .map(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

fn data_files(warehouse: &TempDir, data_format: &str) -> Vec<String> {
    files_under(warehouse.path())
        .iter()
        .filter(|path| {
            path.parent().is_some_and(|parent| parent.ends_with("data"))
                && !file_name(path).starts_with(EQUALITY_DELETE_PREFIX)
                && path
                    .extension()
                    .is_some_and(|extension| extension == data_format)
        })
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

fn strings(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = cast(batch.column(0), &DataType::Utf8).expect("data as utf8");
            let column = column.as_string::<i32>();
            (0..batch.num_rows())
                .map(|row| column.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn read(session: &ReparkSession) -> Result<Vec<String>, String> {
    attempt(session, READ)
        .await
        .map(|batches| strings(&batches))
}

async fn evolved_reads(
    data_format: &str,
    delete_format: &str,
    evolution: Evolution,
) -> (Reads, TempDir) {
    let (session, warehouse) = seeded(data_format).await;
    commit_equality_delete(&session, delete_format).await;
    assert_eq!(
        equality_delete_extensions(&warehouse),
        vec![delete_format],
        "the equality-delete file is {delete_format}"
    );
    assert_eq!(read(&session).await, Ok(survivors()), "before the drop");
    let key = load(&session)
        .await
        .metadata()
        .current_schema()
        .field_by_name("id")
        .expect("key column")
        .id;
    run(&session, DROP_KEY).await;
    if let Evolution::DroppedAndReAdded = evolution {
        run(&session, RE_ADD_KEY).await;
        let re_added = load(&session)
            .await
            .metadata()
            .current_schema()
            .field_by_name("id")
            .expect("re-added key column")
            .id;
        assert_ne!(re_added, key, "the re-added key takes a new field id");
    }
    let table = load(&session).await;
    let metadata_location = table.metadata_location().expect("metadata location");
    let fresh = adopted(
        Arc::new(SparkDialect),
        true,
        warehouse.path(),
        metadata_location,
    )
    .await;
    let native = adopted(
        Arc::new(AnsiDialect),
        false,
        warehouse.path(),
        metadata_location,
    )
    .await;
    let reads = Reads {
        same_session: read(&session).await,
        fresh_session: read(&fresh).await,
        native_door: read(&native).await,
    };
    (reads, warehouse)
}

fn survivors() -> Vec<String> {
    SURVIVORS.iter().map(ToString::to_string).collect()
}

fn label(data_format: &str, delete_format: &str, evolution: Evolution, read: &str) -> String {
    let evolution = match evolution {
        Evolution::Dropped => "dropped",
        Evolution::DroppedAndReAdded => "dropped and re-added",
    };
    format!("{data_format} data, {delete_format} delete, key {evolution}, {read}")
}

async fn assert_survivors(evolution: Evolution) {
    for delete_format in DELETE_FORMATS {
        let (reads, _warehouse) = evolved_reads("parquet", delete_format, evolution).await;
        for (name, outcome) in reads.each() {
            assert_eq!(
                outcome,
                &Ok(survivors()),
                "{}",
                label("parquet", delete_format, evolution, name)
            );
        }
    }
}

async fn assert_loud_or_correct(data_format: &str) {
    for delete_format in ["parquet", data_format] {
        for evolution in EVOLUTIONS {
            let (reads, warehouse) = evolved_reads(data_format, delete_format, evolution).await;
            let data = data_files(&warehouse, data_format);
            assert_eq!(data.len(), 1, "one {data_format} data file: {data:?}");
            for (name, outcome) in reads.each() {
                let case = label(data_format, delete_format, evolution, name);
                match outcome {
                    Ok(rows) => assert_eq!(rows, &survivors(), "{case}: wrong rows"),
                    Err(error) => assert!(
                        error.contains("DataInvalid") && error.contains(&data[0]),
                        "{case}: the refusal must say DataInvalid and name {}: {error}",
                        data[0]
                    ),
                }
            }
        }
    }
}

#[tokio::test]
async fn a_dropped_equality_delete_key_on_parquet_data_reads_back_the_survivors() {
    assert_survivors(Evolution::Dropped).await;
}

#[tokio::test]
async fn a_dropped_and_re_added_equality_delete_key_on_parquet_data_reads_back_the_survivors() {
    assert_survivors(Evolution::DroppedAndReAdded).await;
}

#[tokio::test]
async fn a_dropped_equality_delete_key_on_avro_data_is_loud_or_correct() {
    assert_loud_or_correct("avro").await;
}

#[tokio::test]
async fn a_dropped_equality_delete_key_on_orc_data_is_loud_or_correct() {
    assert_loud_or_correct("orc").await;
}
