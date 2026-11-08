use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use iceberg::spec::{
    FormatVersion, ListType, NestedField, PrimitiveType, Schema, StructType, Type,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_core::{DataFusionDialect, ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Door {
    Ansi,
    NativeDataFusion,
    Spark,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    V2Cow,
    V2Mor,
    V3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verb {
    Delete,
    Update,
    IdentityUpdate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rows {
    Unchanged,
    AllDeleted,
    OneDeleted,
    AllFlagZ,
    OneFlagZ,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lineage {
    Untouched,
    Some,
    All,
}

#[derive(Clone, Copy, Debug)]
struct Cell {
    name: &'static str,
    verb: Verb,
    predicate: Option<&'static str>,
    rows: Rows,
    snapshot_changed: bool,
    files_changed: bool,
    lineage: Lineage,
}

const fn empty(name: &'static str, verb: Verb, predicate: &'static str) -> Cell {
    Cell {
        name,
        verb,
        predicate: Some(predicate),
        rows: Rows::Unchanged,
        snapshot_changed: false,
        files_changed: false,
        lineage: Lineage::Untouched,
    }
}

const fn control(
    name: &'static str,
    verb: Verb,
    predicate: Option<&'static str>,
    rows: Rows,
    files_changed: bool,
    lineage: Lineage,
) -> Cell {
    Cell {
        name,
        verb,
        predicate,
        rows,
        snapshot_changed: true,
        files_changed,
        lineage,
    }
}

const STRUCT_NULL: &str = "st = CAST(NULL AS STRUCT<x: INT>)";

const CELLS: [Cell; 21] = [
    empty("a-false", Verb::Delete, "false"),
    empty("a-false", Verb::Update, "false"),
    empty("a-null", Verb::Delete, "NULL"),
    empty("a-null", Verb::Update, "NULL"),
    empty("b-struct-null", Verb::Delete, STRUCT_NULL),
    empty("b-struct-null", Verb::Update, STRUCT_NULL),
    empty("b-arr-and-false", Verb::Delete, "arr[1] = 1 AND 1 = 0"),
    empty("b-arr-and-false", Verb::Update, "arr[1] = 1 AND 1 = 0"),
    empty("u-required-is-null", Verb::Delete, "id IS NULL"),
    empty("u-required-is-null", Verb::Update, "id IS NULL"),
    empty("c-one-eq-zero", Verb::Delete, "1 = 0"),
    empty("c-one-eq-zero", Verb::Update, "1 = 0"),
    empty("c-prim-eq-null", Verb::Delete, "prim_col = NULL"),
    empty("c-prim-eq-null", Verb::Update, "prim_col = NULL"),
    control(
        "c-true",
        Verb::Delete,
        Some("true"),
        Rows::AllDeleted,
        false,
        Lineage::All,
    ),
    control(
        "c-true",
        Verb::Update,
        Some("true"),
        Rows::AllFlagZ,
        true,
        Lineage::All,
    ),
    control(
        "c-no-where",
        Verb::Delete,
        None,
        Rows::AllDeleted,
        false,
        Lineage::All,
    ),
    control(
        "c-no-where",
        Verb::Update,
        None,
        Rows::AllFlagZ,
        true,
        Lineage::All,
    ),
    control(
        "c-selective",
        Verb::Delete,
        Some("id = 1"),
        Rows::OneDeleted,
        true,
        Lineage::Some,
    ),
    control(
        "c-selective",
        Verb::Update,
        Some("id = 1"),
        Rows::OneFlagZ,
        true,
        Lineage::Some,
    ),
    control(
        "c-identity-update",
        Verb::IdentityUpdate,
        Some("id = 1"),
        Rows::Unchanged,
        true,
        Lineage::Some,
    ),
];

const SEEDED: [(i64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fact {
    Snapshot,
    Files,
}

struct Residue {
    door: Door,
    kind: Option<Kind>,
    cell: &'static str,
    verb: Verb,
    flips: Fact,
}

const RESIDUES: [Residue; 4] = [
    Residue {
        door: Door::Spark,
        kind: None,
        cell: "u-required-is-null",
        verb: Verb::Delete,
        flips: Fact::Snapshot,
    },
    Residue {
        door: Door::Ansi,
        kind: None,
        cell: "u-required-is-null",
        verb: Verb::Delete,
        flips: Fact::Snapshot,
    },
    Residue {
        door: Door::NativeDataFusion,
        kind: Some(Kind::V2Mor),
        cell: "c-true",
        verb: Verb::Delete,
        flips: Fact::Files,
    },
    Residue {
        door: Door::NativeDataFusion,
        kind: Some(Kind::V2Mor),
        cell: "c-no-where",
        verb: Verb::Delete,
        flips: Fact::Files,
    },
];

#[derive(Debug, PartialEq, Eq)]
struct State {
    rows: Vec<(i64, String)>,
    snapshot: (usize, Option<i64>),
    files: Vec<String>,
    lineage: Option<Vec<(i64, i64, i64)>>,
}

struct Doors {
    spark: ReparkSession,
    native: ReparkSession,
    native_root: String,
    _spark_warehouse: TempDir,
    _native_warehouse: TempDir,
}

fn mode(kind: Kind) -> &'static str {
    if kind == Kind::V2Mor {
        "merge-on-read"
    } else {
        "copy-on-write"
    }
}

fn format_version(kind: Kind) -> u8 {
    if kind == Kind::V3 { 3 } else { 2 }
}

fn verb_token(verb: Verb) -> &'static str {
    match verb {
        Verb::Delete => "delete",
        Verb::Update => "update",
        Verb::IdentityUpdate => "identity",
    }
}

fn table_name(door: Door, cell: &Cell) -> String {
    let door_token = match door {
        Door::Ansi => "ansi",
        Door::NativeDataFusion => "native",
        Door::Spark => "spark",
    };
    format!(
        "t_{}_{}_{door_token}",
        cell.name.replace('-', "_"),
        verb_token(cell.verb)
    )
}

fn statement(table: &str, cell: &Cell) -> String {
    let head = match cell.verb {
        Verb::Delete => format!("DELETE FROM ice.ns.{table}"),
        Verb::Update => format!("UPDATE ice.ns.{table} SET flag = 'z'"),
        Verb::IdentityUpdate => format!("UPDATE ice.ns.{table} SET flag = flag"),
    };
    match cell.predicate {
        Some(predicate) => format!("{head} WHERE {predicate}"),
        None => head,
    }
}

fn dialect(door: Door) -> Arc<dyn SqlDialect> {
    match door {
        Door::Ansi => Arc::new(AnsiDialect),
        Door::NativeDataFusion => Arc::new(DataFusionDialect),
        Door::Spark => Arc::new(SparkDialect),
    }
}

fn session_of(doors: &Doors, door: Door) -> &ReparkSession {
    if door == Door::Spark {
        &doors.spark
    } else {
        &doors.native
    }
}

async fn attempt(
    session: &ReparkSession,
    door: Door,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let frame = session
        .sql_with(&dialect(door), sql)
        .await
        .map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, door: Door, sql: &str) -> Vec<RecordBatch> {
    attempt(session, door, sql)
        .await
        .unwrap_or_else(|error| panic!("{door:?} `{sql}`: {error}"))
}

async fn open_doors() -> Doors {
    let spark_warehouse = TempDir::new().expect("spark warehouse");
    let native_warehouse = TempDir::new().expect("native warehouse");
    let spark_dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let spark = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(spark_dialect)
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .build()
        .expect("spark session");
    let ansi_dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let native = ReparkSession::builder()
        .with_sql_dialect(ansi_dialect)
        .build()
        .expect("native session");
    for (session, warehouse, door) in [
        (&spark, &spark_warehouse, Door::Spark),
        (&native, &native_warehouse, Door::Ansi),
    ] {
        session
            .register_memory_catalog("ice", warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        run(session, door, "CREATE SCHEMA ice.ns").await;
    }
    let native_root = native_warehouse.path().to_str().expect("utf8").to_string();
    Doors {
        spark,
        native,
        native_root,
        _spark_warehouse: spark_warehouse,
        _native_warehouse: native_warehouse,
    }
}

fn table_schema() -> Schema {
    let element = NestedField::list_element(7, Type::Primitive(PrimitiveType::Int), false);
    let x = NestedField::optional(6, "x", Type::Primitive(PrimitiveType::Int));
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "prim_col", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "flag", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "st", Type::Struct(StructType::new(vec![x.into()]))).into(),
            NestedField::optional(5, "arr", Type::List(ListType::new(element.into()))).into(),
        ])
        .build()
        .expect("schema")
}

async fn create_through_the_catalog(doors: &Doors, kind: Kind, table: &str) {
    let session = &doors.native;
    let creation = TableCreation::builder()
        .name(table.to_string())
        .location(format!("{}/ns/{table}", doors.native_root))
        .schema(table_schema())
        .format_version(if kind == Kind::V3 {
            FormatVersion::V3
        } else {
            FormatVersion::V2
        })
        .properties(HashMap::from([
            ("write.delete.mode".to_string(), mode(kind).to_string()),
            ("write.update.mode".to_string(), mode(kind).to_string()),
        ]))
        .build();
    let catalog: Arc<dyn Catalog> = session
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone();
    catalog
        .create_table(&NamespaceIdent::new("ns".to_string()), creation)
        .await
        .expect("create through the catalog");
    session
        .refresh_catalog_provider("ice")
        .await
        .expect("refresh the provider");
}

async fn create_seeded(doors: &Doors, door: Door, kind: Kind, table: &str) {
    if door == Door::Spark {
        let version = format_version(kind);
        let mode = mode(kind);
        run(
            &doors.spark,
            Door::Spark,
            &format!(
                "CREATE TABLE ice.ns.{table} (id BIGINT NOT NULL, prim_col INT, flag STRING, \
                 st STRUCT<x: INT>, arr ARRAY<INT>) USING iceberg TBLPROPERTIES \
                 ('format-version' = '{version}', 'write.delete.mode' = '{mode}', \
                 'write.update.mode' = '{mode}')"
            ),
        )
        .await;
        run(
            &doors.spark,
            Door::Spark,
            &format!(
                "INSERT INTO ice.ns.{table} VALUES (1, 10, 'a', named_struct('x', 1), \
                 array(1)), (2, 20, 'b', named_struct('x', 2), array(2)), \
                 (3, NULL, 'c', NULL, NULL)"
            ),
        )
        .await;
        return;
    }
    create_through_the_catalog(doors, kind, table).await;
    run(
        &doors.native,
        Door::NativeDataFusion,
        &format!(
            "INSERT INTO ice.ns.{table} VALUES (1, 10, 'a', named_struct('x', 1), ARRAY[1]), \
             (2, 20, 'b', named_struct('x', 2), ARRAY[2]), (3, NULL, 'c', NULL, NULL)"
        ),
    )
    .await;
}

fn ids_and_flags(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = cast(batch.column(0), &DataType::Int64).expect("id as Int64");
        let ids = ids.as_any().downcast_ref::<Int64Array>().expect("Int64");
        let flags = cast(batch.column(1), &DataType::Utf8).expect("flag as Utf8");
        let flags = flags.as_any().downcast_ref::<StringArray>().expect("Utf8");
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), flags.value(index).to_string()));
        }
    }
    rows
}

fn int64_column(batch: &RecordBatch, index: usize) -> Int64Array {
    let column = cast(batch.column(index), &DataType::Int64).expect("Int64");
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .clone()
}

fn lineage_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut found = Vec::new();
    for batch in batches {
        let ids = int64_column(batch, 0);
        let row_ids = int64_column(batch, 1);
        let sequences = int64_column(batch, 2);
        for index in 0..batch.num_rows() {
            found.push((
                ids.value(index),
                row_ids.value(index),
                sequences.value(index),
            ));
        }
    }
    found
}

fn data_files(location: &str) -> Vec<String> {
    let root = PathBuf::from(location.trim_start_matches("file://")).join("data");
    let mut found = Vec::new();
    let mut pending = vec![root.clone()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(relative(&root, &path));
            }
        }
    }
    found.sort();
    found
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("under the data root")
        .to_string_lossy()
        .into_owned()
}

async fn observe(doors: &Doors, door: Door, kind: Kind, table: &str) -> State {
    let session = session_of(doors, door);
    let read_door = if door == Door::Spark {
        Door::Spark
    } else {
        Door::NativeDataFusion
    };
    let rows = ids_and_flags(
        &run(
            session,
            read_door,
            &format!("SELECT id, flag FROM ice.ns.{table} ORDER BY id"),
        )
        .await,
    );
    let catalog: Arc<dyn Catalog> = session
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone();
    let loaded = catalog
        .load_table(&TableIdent::from_strs(["ns", table]).expect("ident"))
        .await
        .expect("load");
    let metadata = loaded.metadata();
    let lineage = if kind == Kind::V3 && door == Door::Spark {
        Some(lineage_rows(
            &run(
                session,
                read_door,
                &format!(
                    "SELECT id, _row_id, _last_updated_sequence_number FROM ice.ns.{table} \
                     ORDER BY id"
                ),
            )
            .await,
        ))
    } else {
        None
    };
    State {
        rows,
        snapshot: (metadata.snapshots().count(), metadata.current_snapshot_id()),
        files: data_files(metadata.location()),
        lineage,
    }
}

fn expected_rows(rows: Rows) -> Vec<(i64, String)> {
    SEEDED
        .iter()
        .filter(|(id, _)| !(rows == Rows::AllDeleted || (rows == Rows::OneDeleted && *id == 1)))
        .map(|(id, flag)| {
            let flagged = rows == Rows::AllFlagZ || (rows == Rows::OneFlagZ && *id == 1);
            (*id, if flagged { "z" } else { flag }.to_string())
        })
        .collect()
}

fn lineage_class(before: &[(i64, i64, i64)], after: &[(i64, i64, i64)]) -> Lineage {
    let changed = before.iter().filter(|pair| !after.contains(pair)).count();
    if changed == 0 {
        Lineage::Untouched
    } else if changed == before.len() {
        Lineage::All
    } else {
        Lineage::Some
    }
}

fn reported_counts(batches: &[RecordBatch]) -> Vec<u64> {
    let mut counts = Vec::new();
    for batch in batches {
        if let Ok(index) = batch.schema().index_of("count") {
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("count is UInt64");
            counts.extend(column.values().iter().copied());
        }
    }
    counts
}

fn flips(door: Door, kind: Kind, cell: &Cell, fact: Fact) -> bool {
    RESIDUES.iter().any(|residue| {
        residue.door == door
            && residue.kind.is_none_or(|only| only == kind)
            && residue.cell == cell.name
            && residue.verb == cell.verb
            && residue.flips == fact
    })
}

async fn check_cell(doors: &Doors, door: Door, kind: Kind, cell: &Cell) -> Vec<String> {
    let table = table_name(door, cell);
    create_seeded(doors, door, kind, &table).await;
    let before = observe(doors, door, kind, &table).await;
    let label = format!("{door:?} {kind:?} {} {:?}", cell.name, cell.verb);
    let mut failures = Vec::new();
    if before.rows != expected_rows(Rows::Unchanged) {
        failures.push(format!("{label}: seeded rows {:?}", before.rows));
    }
    let sql = statement(&table, cell);
    let answer = run(session_of(doors, door), door, &sql).await;
    let after = observe(doors, door, kind, &table).await;
    let snapshot_changed = cell.snapshot_changed != flips(door, kind, cell, Fact::Snapshot);
    let files_changed = cell.files_changed != flips(door, kind, cell, Fact::Files);
    if after.rows != expected_rows(cell.rows) {
        failures.push(format!("{label}: rows {:?}", after.rows));
    }
    if (after.snapshot != before.snapshot) != snapshot_changed {
        failures.push(format!(
            "{label}: snapshot {:?} -> {:?}",
            before.snapshot, after.snapshot
        ));
    }
    if (after.files != before.files) != files_changed {
        failures.push(format!(
            "{label}: files {:?} -> {:?}",
            before.files, after.files
        ));
    }
    if let (Some(lineage_before), Some(lineage_after)) = (&before.lineage, &after.lineage) {
        let surviving = if cell.rows == Rows::AllDeleted || cell.rows == Rows::OneDeleted {
            Lineage::Untouched
        } else {
            lineage_class(lineage_before, lineage_after)
        };
        let expected = if cell.rows == Rows::AllDeleted || cell.rows == Rows::OneDeleted {
            Lineage::Untouched
        } else {
            cell.lineage
        };
        if surviving != expected {
            failures.push(format!(
                "{label}: lineage {lineage_before:?} -> {lineage_after:?}"
            ));
        }
    }
    if cell.rows == Rows::Unchanged
        && !cell.snapshot_changed
        && reported_counts(&answer).iter().any(|count| *count != 0)
    {
        failures.push(format!("{label}: reported {:?}", reported_counts(&answer)));
    }
    failures
}

async fn check_door(door: Door) {
    let mut failures = Vec::new();
    for kind in [Kind::V2Cow, Kind::V2Mor, Kind::V3] {
        let doors = open_doors().await;
        for cell in &CELLS {
            failures.extend(check_cell(&doors, door, kind, cell).await);
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_native_door_path_writes_nothing_for_an_empty_dml_on_every_table_kind() {
    check_door(Door::NativeDataFusion).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ansi_door_writes_nothing_for_an_empty_dml_on_every_table_kind() {
    check_door(Door::Ansi).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_spark_door_writes_nothing_for_an_empty_dml_on_every_table_kind() {
    check_door(Door::Spark).await;
}
