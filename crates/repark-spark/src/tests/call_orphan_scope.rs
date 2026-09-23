use std::path::{Path, PathBuf};

use repark_core::ReparkSession;

use super::common::*;
use crate::{SparkDialect, SparkExtension};

pub(super) async fn fallback_session(warehouse: &TempDir) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    submit(&session, "CREATE NAMESPACE ice.ns").await;
    session
}

pub(super) async fn submit(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

pub(super) async fn ctas(session: &ReparkSession, warehouse: &TempDir, table: &str) -> PathBuf {
    submit(
        session,
        &format!("CREATE TABLE ice.ns.{table} USING iceberg AS SELECT 1 AS id"),
    )
    .await;
    let table_dir = warehouse
        .path()
        .join("repark_ctas")
        .join("ice")
        .join("ns")
        .join(table);
    assert!(
        table_dir.join("metadata").is_dir(),
        "a namespace without a location places {table} under the shared fallback root"
    );
    table_dir
}

pub(super) fn plant(table_dir: &Path, name: &str, age_days: u64) -> PathBuf {
    let data_dir = table_dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let path = data_dir.join(name);
    std::fs::write(&path, b"PAR1junk").expect("write orphan");
    let stamp = std::time::SystemTime::now() - std::time::Duration::from_secs(age_days * 86_400);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("reopen orphan")
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(stamp)
                .set_accessed(stamp),
        )
        .expect("age the orphan");
    path
}

pub(super) fn referenced_data_file(table_dir: &Path) -> PathBuf {
    std::fs::read_dir(table_dir.join("data"))
        .expect("data dir")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .expect("the CTAS wrote one data file")
}

pub(super) async fn live_rows(session: &ReparkSession, table: &str) -> usize {
    session
        .sql(&format!("SELECT id FROM ice.{table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

pub(super) async fn call_rows(session: &ReparkSession, sql: &str) -> Result<Vec<String>, String> {
    let batches = session
        .sql(sql)
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().field(0).name(), "orphan_file_location");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        for index in 0..column.len() {
            out.push(column.value(index).to_string());
        }
    }
    Ok(out)
}

#[tokio::test]
async fn call_remove_orphan_files_sweeps_a_fallback_tables_own_directory() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t')",
    )
    .await
    .expect("a table's own directory under the fallback root is sweepable");
    assert_eq!(listed.len(), 1, "one row per orphan, got {listed:?}");
    assert!(
        listed[0].ends_with("/ns/t/data/orphan-file.parquet"),
        "{listed:?}"
    );
    assert!(!orphan.exists(), "the armed default run deletes the orphan");
    assert_eq!(
        live_rows(&session, "ns.t").await,
        1,
        "the live data file survives"
    );

    let young = plant(&table_dir, "orphan-file.parquet", 1);
    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t')",
    )
    .await
    .expect("young sweep runs");
    assert!(
        listed.is_empty(),
        "a 1-day-old orphan is inside the default 3-day window: {listed:?}"
    );
    assert!(young.exists(), "the young orphan is kept");
}

#[tokio::test]
async fn call_remove_orphan_files_refuses_a_location_holding_another_table() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let own_dir = ctas(&session, &warehouse, "a").await;
    let other_dir = ctas(&session, &warehouse, "b").await;
    let own_orphan = plant(&own_dir, "orphan-file.parquet", 10);
    let other_orphan = plant(&other_dir, "orphan-file.parquet", 10);
    let other_live = referenced_data_file(&other_dir);
    let namespace_dir = warehouse.path().join("repark_ctas").join("ice").join("ns");

    for location in [
        namespace_dir.display().to_string(),
        other_dir.display().to_string(),
        format!("file://{}", namespace_dir.display()),
        format!("{}/", other_dir.display()),
        format!("{}/./", namespace_dir.display()),
        format!("{}//b", namespace_dir.display()),
        format!("{}/a/..", namespace_dir.display()),
    ] {
        let err = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'ns.a', location => '{location}')"
            ),
        )
        .await
        .expect_err("a location holding another table's files must refuse");
        assert!(
            err.contains("ice.ns.b") && err.contains("CALL remove_orphan_files refuses"),
            "the refusal must name the other table: {err}"
        );
    }
    assert!(own_orphan.exists(), "a refused sweep deletes nothing");
    assert!(other_orphan.exists(), "a refused sweep deletes nothing");
    assert!(
        other_live.exists(),
        "the other table's live file is untouched"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_sweeps_one_fallback_table_beside_a_sibling() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let own_dir = ctas(&session, &warehouse, "a").await;
    let other_dir = ctas(&session, &warehouse, "b").await;
    let own_orphan = plant(&own_dir, "orphan-file.parquet", 10);
    let other_orphan = plant(&other_dir, "orphan-file.parquet", 10);
    let other_live = referenced_data_file(&other_dir);

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.a')",
    )
    .await
    .expect("a sibling table in the same namespace does not block the own-directory sweep");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert!(
        listed[0].ends_with("/ns/a/data/orphan-file.parquet"),
        "{listed:?}"
    );
    assert!(!own_orphan.exists(), "the swept table's orphan is deleted");
    assert!(
        other_orphan.exists(),
        "the sibling's orphan is not in scope"
    );
    assert!(other_live.exists(), "the sibling's live file is untouched");
    assert_eq!(live_rows(&session, "ns.a").await, 1);
    assert_eq!(live_rows(&session, "ns.b").await, 1);
}

#[tokio::test]
async fn call_remove_orphan_files_refuses_a_location_holding_a_table_of_another_namespace() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let own_dir = ctas(&session, &warehouse, "a").await;
    let namespace_dir = warehouse.path().join("repark_ctas").join("ice").join("ns");
    let foreign_dir = namespace_dir.join("other");
    submit(
        &session,
        &format!(
            "CREATE NAMESPACE ice.other LOCATION '{}'",
            foreign_dir.display()
        ),
    )
    .await;
    submit(
        &session,
        "CREATE TABLE ice.other.c USING iceberg AS SELECT 1 AS id",
    )
    .await;
    let foreign_table = foreign_dir.join("c");
    assert!(foreign_table.join("metadata").is_dir());
    let own_orphan = plant(&own_dir, "orphan-file.parquet", 10);
    let foreign_orphan = plant(&foreign_table, "orphan-file.parquet", 10);
    let foreign_live = referenced_data_file(&foreign_table);

    let err = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'ns.a', location => '{}')",
            namespace_dir.display()
        ),
    )
    .await
    .expect_err("a table of another namespace inside the scan must refuse");
    assert!(
        err.contains("holds table `ice.other.c`"),
        "the refusal must name the other namespace's table: {err}"
    );
    assert!(own_orphan.exists(), "a refused sweep deletes nothing");
    assert!(foreign_orphan.exists(), "a refused sweep deletes nothing");
    assert!(foreign_live.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_refuses_a_location_holding_a_table_spelled_through_an_alias() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let own_dir = ctas(&session, &warehouse, "a").await;
    std::fs::create_dir_all(warehouse.path().join("detour")).unwrap();
    submit(
        &session,
        &format!(
            "CREATE NAMESPACE ice.al LOCATION '{}/detour/../aliased'",
            warehouse.path().display()
        ),
    )
    .await;
    submit(
        &session,
        "CREATE TABLE ice.al.t USING iceberg AS SELECT 1 AS id",
    )
    .await;
    let own_orphan = plant(&own_dir, "orphan-file.parquet", 10);

    let err = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'ns.a', location => '{}/aliased')",
            warehouse.path().display()
        ),
    )
    .await
    .expect_err("a table whose metadata spells its location through `..` is still found");
    assert!(err.contains("holds table `ice.al.t`"), "{err}");
    assert!(own_orphan.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_refuses_a_location_holding_a_nested_namespace_table() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use iceberg::{NamespaceIdent, TableCreation};
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let own_dir = ctas(&session, &warehouse, "a").await;
    let inner_dir = own_dir.parent().unwrap().join("inner");
    let catalog = session.catalogs_snapshot().get("ice").unwrap().clone();
    let inner = NamespaceIdent::from_strs(["ns", "inner"]).unwrap();
    catalog
        .create_namespace(&inner, std::collections::HashMap::new())
        .await
        .unwrap();
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();
    catalog
        .create_table(
            &inner,
            TableCreation::builder()
                .name("d".to_string())
                .location(inner_dir.join("d").display().to_string())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let own_orphan = plant(&own_dir, "orphan-file.parquet", 10);

    let err = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'ns.a', location => '{}')",
            inner_dir.display()
        ),
    )
    .await
    .expect_err("a table of a nested namespace inside the scan must refuse");
    assert!(
        err.contains("holds table `ice.ns.inner.d`"),
        "the refusal must name the nested table: {err}"
    );
    assert!(own_orphan.exists(), "a refused sweep deletes nothing");
}

#[tokio::test]
async fn call_remove_orphan_files_refuses_the_warehouse_and_the_fallback_root() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);

    for location in [
        warehouse.path().display().to_string(),
        warehouse.path().join("repark_ctas").display().to_string(),
        warehouse
            .path()
            .join("repark_ansi_ctas")
            .display()
            .to_string(),
    ] {
        let err = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'ns.t', location => '{location}')"
            ),
        )
        .await
        .expect_err("the warehouse and the fallback root never sweep");
        assert!(err.contains("shared CTAS fallback root"), "{err}");
    }
    assert!(orphan.exists(), "a refused sweep deletes nothing");
}

pub(super) async fn register_file_list(session: &ReparkSession, rows: &[(String, i64)]) {
    let selects: Vec<String> = rows
        .iter()
        .map(|(path, seconds)| {
            format!(
                "SELECT '{path}' AS file_path, \
                 CAST(from_unixtime({seconds}) AS TIMESTAMP) AS last_modified"
            )
        })
        .collect();
    let frame = session.sql(&selects.join(" UNION ALL ")).await.unwrap();
    session
        .create_or_replace_temp_view_from("v", &frame)
        .unwrap();
}

async fn file_list_view(session: &ReparkSession, entries: &[&Path]) {
    let rows: Vec<(String, i64)> = entries
        .iter()
        .map(|path| (path.display().to_string(), 0))
        .collect();
    register_file_list(session, &rows).await;
}

fn seconds_ago(days: i64) -> i64 {
    chrono::Utc::now().timestamp() - days * 86_400
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_dry_run_lists_the_view_orphans_verbatim() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let live = referenced_data_file(&table_dir);
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);
    file_list_view(&session, &[&live, &orphan]).await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(\
             table => 'ns.t', dry_run => true, file_list_view => 'v')",
    )
    .await
    .expect("file_list_view is accepted");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(orphan.exists(), "dry_run deletes nothing");
    assert!(live.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_armed_deletes_only_the_listed_orphans() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let live = referenced_data_file(&table_dir);
    let listed_orphan = plant(&table_dir, "orphan-listed.parquet", 10);
    let unlisted_orphan = plant(&table_dir, "orphan-unlisted.parquet", 10);
    file_list_view(&session, &[&live, &listed_orphan]).await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
    )
    .await
    .expect("armed file_list_view runs");
    assert_eq!(listed, vec![listed_orphan.display().to_string()]);
    assert!(!listed_orphan.exists(), "the listed orphan is deleted");
    assert!(
        unlisted_orphan.exists(),
        "a file the view does not list is not a candidate"
    );
    assert!(live.exists(), "a referenced file is never an orphan");
    assert_eq!(
        live_rows(&session, "ns.t").await,
        1,
        "the live data file survives"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_keeps_rows_inside_the_older_than_window() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let old = plant(&table_dir, "orphan-old.parquet", 10);
    let young = plant(&table_dir, "orphan-young.parquet", 10);
    register_file_list(
        &session,
        &[
            (old.display().to_string(), seconds_ago(10)),
            (young.display().to_string(), seconds_ago(1)),
        ],
    )
    .await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
    )
    .await
    .expect("armed file_list_view runs");
    assert_eq!(
        listed,
        vec![old.display().to_string()],
        "a row whose last_modified is inside the default 3-day window is not a candidate"
    );
    assert!(!old.exists(), "the old listed orphan is deleted");
    assert!(
        young.exists(),
        "the young row is kept even though its file on disk is old: the view's timestamp rules"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_skips_paths_outside_the_scan_location() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let inside = plant(&table_dir, "orphan-inside.parquet", 10);
    let outside = plant(
        &warehouse.path().join("outside"),
        "orphan-outside.parquet",
        10,
    );
    let escaped = table_dir.parent().unwrap().join("escape.parquet");
    std::fs::write(&escaped, b"PAR1junk").unwrap();
    let escape_spelling = format!("{}/../escape.parquet", table_dir.display());
    register_file_list(
        &session,
        &[
            (inside.display().to_string(), 0),
            (outside.display().to_string(), 0),
            (escape_spelling, 0),
        ],
    )
    .await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
    )
    .await
    .expect("armed file_list_view runs");
    assert_eq!(
        listed,
        vec![inside.display().to_string()],
        "a view path outside the scan location is never a candidate"
    );
    assert!(!inside.exists(), "the in-scope orphan is deleted");
    assert!(
        outside.exists(),
        "a path outside the table directory is kept"
    );
    assert!(
        escaped.exists(),
        "a `..` spelling that leaves the table directory is kept"
    );
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_keeps_a_live_file_named_through_an_alias() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let live = referenced_data_file(&table_dir);
    let live_name = live.file_name().unwrap().to_str().unwrap().to_string();
    let data_dir = table_dir.join("data");
    let aliases = [
        format!("file://{}/../data/{live_name}", data_dir.display()),
        format!("{}/./{live_name}", data_dir.display()),
        format!("{}//{live_name}", data_dir.display()),
    ];
    for (index, alias) in aliases.into_iter().enumerate() {
        let orphan = plant(&table_dir, &format!("orphan-{index}.parquet"), 10);
        register_file_list(
            &session,
            &[(alias.clone(), 0), (orphan.display().to_string(), 0)],
        )
        .await;
        let listed = call_rows(
            &session,
            "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v')",
        )
        .await
        .expect("armed file_list_view runs");
        assert_eq!(listed, vec![orphan.display().to_string()], "{alias}");
        assert!(!orphan.exists(), "{alias}: the real orphan is deleted");
        assert!(
            live.exists(),
            "{alias}: an alias of the live file is the live file"
        );
        assert_eq!(live_rows(&session, "ns.t").await, 1, "{alias}");
    }
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_matches_a_table_location_that_holds_an_alias() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    std::fs::create_dir_all(warehouse.path().join("detour")).unwrap();
    submit(
        &session,
        &format!(
            "CREATE NAMESPACE ice.al LOCATION '{}/detour/../aliased'",
            warehouse.path().display()
        ),
    )
    .await;
    submit(
        &session,
        "CREATE TABLE ice.al.t USING iceberg AS SELECT 1 AS id",
    )
    .await;
    let catalog = session.catalogs_snapshot().get("ice").unwrap().clone();
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs(["al", "t"]).unwrap())
        .await
        .unwrap();
    assert!(
        table.metadata().location().contains("/detour/../aliased/t"),
        "the table's own metadata spells its location through `..`: {}",
        table.metadata().location()
    );
    let table_dir = warehouse.path().join("aliased").join("t");
    let live = referenced_data_file(&table_dir);
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);
    file_list_view(&session, &[&live, &orphan]).await;

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'al.t', file_list_view => 'v')",
    )
    .await
    .expect("armed file_list_view runs");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(!orphan.exists());
    assert!(
        live.exists(),
        "the canonical spelling of a file the metadata names through `..` is referenced"
    );
    assert_eq!(live_rows(&session, "al.t").await, 1);
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_resolves_an_aliased_scan_location() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);
    file_list_view(&session, &[&orphan]).await;

    let listed = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'ns.t', dry_run => true, \
             file_list_view => 'v', location => 'file://{}/data/..')",
            table_dir.display()
        ),
    )
    .await
    .expect("an aliased spelling of the table directory is the table directory");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(orphan.exists());
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_near_misses_refuse() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let table_dir = ctas(&session, &warehouse, "t").await;
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);

    let err = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(\
             table => 'ns.t', dry_run => true, file_list_view => 'no_such_view')",
    )
    .await
    .expect_err("a missing view refuses");
    assert!(
        err.contains("[TABLE_OR_VIEW_NOT_FOUND]") && err.contains("`no_such_view`"),
        "Spark answers table or view not found: {err}"
    );
    assert!(orphan.exists());

    file_list_view(&session, &[&orphan]).await;
    let err = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'v', \
             location => '{}')",
            warehouse.path().display()
        ),
    )
    .await
    .expect_err("the warehouse root refuses in file_list_view mode too");
    assert!(err.contains("shared CTAS fallback root"), "{err}");
    assert!(orphan.exists());

    let frame = session
        .sql(&format!(
            "SELECT '{}' AS file_path, 0 AS last_modified",
            orphan.display()
        ))
        .await
        .unwrap();
    session
        .create_or_replace_temp_view_from("untimed", &frame)
        .unwrap();
    let err = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t', file_list_view => 'untimed')",
    )
    .await
    .expect_err("a view without a timestamp last_modified refuses");
    assert!(
        err.contains("Invalid last_modified column") && err.contains("is not a timestamp"),
        "{err}"
    );
    assert!(orphan.exists());
}

pub(super) async fn file_scheme_table(
    session: &ReparkSession,
    warehouse: &TempDir,
) -> (PathBuf, PathBuf) {
    let namespace_dir = warehouse.path().join("fq");
    submit(
        session,
        &format!(
            "CREATE NAMESPACE ice.fq LOCATION 'file://{}'",
            namespace_dir.display()
        ),
    )
    .await;
    submit(
        session,
        "CREATE TABLE ice.fq.t USING iceberg AS SELECT 1 AS id",
    )
    .await;
    let table_dir = namespace_dir.join("t");
    let live = referenced_data_file(&table_dir);
    let stamp = std::time::SystemTime::now() - std::time::Duration::from_hours(10 * 24);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&live)
        .expect("reopen the live file")
        .set_modified(stamp)
        .expect("age the live file");
    (table_dir, live)
}

pub(super) fn prefix_conflict_message(pairs: &str) -> String {
    format!(
        "DataInvalid => Unable to determine whether certain files are orphan. Metadata references \
         files that match listed/provided files except for authority/scheme. Please, inspect the \
         conflicting authorities/schemes and provide which of them are equal by further \
         configuring the action via equalSchemes() and equalAuthorities() methods. Set the prefix \
         mismatch mode to 'IGNORE' to skip remaining locations with conflicting \
         authorities/schemes or to 'DELETE' iff you are ABSOLUTELY confident that remaining \
         conflicting authorities/schemes are different. It will be impossible to recover deleted \
         files. Conflicting authorities/schemes: [{pairs}]."
    )
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_prefix_conflicts_match_the_listing_path() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let (table_dir, live) = file_scheme_table(&session, &warehouse).await;
    register_file_list(&session, &[(live.display().to_string(), 0)]).await;

    for (extra, pairs) in [
        ("", "(file, )"),
        (
            ", equal_authorities => map('', 'hostA')",
            "(file, ), (hostA, )",
        ),
    ] {
        let view = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'fq.t', file_list_view => 'v'{extra})"
            ),
        )
        .await
        .expect_err("a bare view path against file: metadata is a scheme conflict");
        let listing = call_rows(
            &session,
            &format!(
                "CALL ice.system.remove_orphan_files(table => 'fq.t', location => '{}'{extra})",
                table_dir.display()
            ),
        )
        .await
        .expect_err("a bare listing against file: metadata is the same conflict");
        assert_eq!(view, prefix_conflict_message(pairs));
        assert_eq!(view, listing, "both doors emit the fork's conflict text");
    }
    assert!(live.exists(), "a prefix conflict deletes nothing");
}

#[tokio::test]
async fn call_remove_orphan_files_file_list_view_names_an_authority_conflict() {
    let warehouse = TempDir::new().unwrap();
    let session = fallback_session(&warehouse).await;
    let (_, live) = file_scheme_table(&session, &warehouse).await;
    register_file_list(
        &session,
        &[(format!("file://localhost{}", live.display()), 0)],
    )
    .await;

    let err = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'fq.t', file_list_view => 'v', \
         equal_authorities => map('', 'hostA'))",
    )
    .await
    .expect_err("a mapped empty authority against localhost is an authority conflict");
    assert_eq!(err, prefix_conflict_message("(hostA, localhost)"));
    assert!(live.exists(), "a prefix conflict deletes nothing");
}
