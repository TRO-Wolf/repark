use std::path::Path;

use iceberg::TableIdent;
use iceberg::table::Table;
use repark_core::ReparkSession;

use super::call_orphan_scope::{
    call_error, call_rows, plan_message, plant, referenced_data_file, submit,
};
use super::common::*;
use crate::{SparkDialect, SparkExtension};

async fn memory_session(warehouse: &TempDir, catalogs: &[&str]) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    for name in catalogs {
        session
            .register_memory_catalog(name, warehouse.path().to_str().unwrap())
            .await
            .unwrap();
    }
    session
}

async fn create_with_row(session: &ReparkSession, table: &str, id: i32) {
    submit(session, &format!("CREATE TABLE {table} (id INT)")).await;
    submit(session, &format!("INSERT INTO {table} VALUES ({id})")).await;
}

async fn ids(session: &ReparkSession, table: &str) -> Vec<i32> {
    let batches = session
        .sql(&format!("SELECT id FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        out.extend(column.iter().flatten());
    }
    out.sort_unstable();
    out
}

async fn load(session: &ReparkSession, catalog: &str, parts: &[&str]) -> Table {
    session
        .catalogs_snapshot()
        .get(catalog)
        .unwrap()
        .clone()
        .load_table(&TableIdent::from_strs(parts).unwrap())
        .await
        .unwrap()
}

fn metadata_files(table: &Table) -> Vec<String> {
    let mut files: Vec<String> = table
        .metadata()
        .metadata_log()
        .iter()
        .map(|entry| entry.metadata_file.clone())
        .chain(table.metadata_location().map(str::to_string))
        .collect();
    files.sort();
    files
}

fn age_tree(dir: &Path, days: u64) {
    let stamp = std::time::SystemTime::now() - std::time::Duration::from_secs(days * 86_400);
    for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            age_tree(&path, days);
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("reopen")
                .set_modified(stamp)
                .expect("age");
        }
    }
}

fn foreign_metadata_refusal(
    table_arg: &str,
    scan: &str,
    file: &str,
    uuid: &str,
    own_uuid: &str,
) -> String {
    format!(
        "CALL remove_orphan_files refuses to sweep `{table_arg}`: path `{scan}` holds `{file}`, \
         the metadata file of another table (table-uuid `{uuid}`; the swept table's is \
         `{own_uuid}`), such as a table of another catalog or session on the same warehouse. \
         This procedure deletes every file the swept table's own metadata does not reference, \
         which would include that table's live files. Give the table its own LOCATION \
         (`CREATE TABLE ... LOCATION '<path>'`), then sweep it."
    )
}

fn inside_other_table_refusal(
    table_arg: &str,
    scan: &str,
    other: &str,
    other_location: &str,
    own_location: &str,
) -> String {
    format!(
        "CALL remove_orphan_files refuses to sweep `{table_arg}`: path `{scan}` lies inside table \
         `{other}` at `{other_location}` and outside the swept table's own location \
         `{own_location}`. This procedure deletes every file the swept table's own metadata does \
         not reference, which would include that table's live files. Sweep a path that holds \
         only this table's files."
    )
}

fn file_spellings(path: &Path) -> [String; 3] {
    [
        path.display().to_string(),
        format!("file://{}", path.display()),
        format!("file:{}", path.display()),
    ]
}

async fn assert_co_tenant_refused(
    sweeper: &ReparkSession,
    sweeper_catalog: &str,
    owner: &ReparkSession,
    owner_catalog: &str,
    table_dir: &Path,
) {
    let swept = load(sweeper, sweeper_catalog, &["ns", "t"]).await;
    let foreign = load(owner, owner_catalog, &["ns", "t"]).await;
    let own_files = metadata_files(&swept);
    let first_foreign = metadata_files(&foreign)
        .into_iter()
        .find(|file| !own_files.contains(file))
        .expect("the co-tenant wrote its own metadata files");
    let orphan = plant(table_dir, "orphan-file.parquet", 10);
    age_tree(table_dir, 10);

    let err = call_error(
        sweeper,
        &format!("CALL {sweeper_catalog}.system.remove_orphan_files(table => 'ns.t')"),
    )
    .await;
    assert_eq!(
        plan_message(err),
        foreign_metadata_refusal(
            "ns.t",
            &table_dir.display().to_string(),
            &first_foreign,
            &foreign.metadata().uuid().to_string(),
            &swept.metadata().uuid().to_string(),
        )
    );
    for scan in file_spellings(table_dir) {
        let err = call_error(
            sweeper,
            &format!(
                "CALL {sweeper_catalog}.system.remove_orphan_files(table => 'ns.t', \
                 location => '{scan}')"
            ),
        )
        .await;
        assert_eq!(
            plan_message(err),
            foreign_metadata_refusal(
                "ns.t",
                &scan,
                &first_foreign,
                &foreign.metadata().uuid().to_string(),
                &swept.metadata().uuid().to_string(),
            ),
            "{scan}"
        );
    }
    assert!(orphan.exists(), "a refused sweep deletes nothing");
}

#[tokio::test]
async fn call_orphan_cotenancy_two_catalogs_on_one_warehouse_refuse() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["m1", "m2"]).await;
    for catalog in ["m1", "m2"] {
        submit(&session, &format!("CREATE NAMESPACE {catalog}.ns")).await;
    }
    create_with_row(&session, "m1.ns.t", 1).await;
    create_with_row(&session, "m2.ns.t", 2).await;
    let table_dir = warehouse.path().join("ns").join("t");
    assert_eq!(
        load(&session, "m2", &["ns", "t"])
            .await
            .metadata()
            .location(),
        table_dir.display().to_string()
    );

    assert_co_tenant_refused(&session, "m1", &session, "m2", &table_dir).await;
    assert_eq!(ids(&session, "m2.ns.t").await, vec![2]);
    assert_eq!(ids(&session, "m1.ns.t").await, vec![1]);
}

#[tokio::test]
async fn call_orphan_cotenancy_two_sessions_with_one_catalog_name_refuse() {
    let warehouse = TempDir::new().unwrap();
    let first = memory_session(&warehouse, &["ice"]).await;
    let second = memory_session(&warehouse, &["ice"]).await;
    for session in [&first, &second] {
        submit(session, "CREATE NAMESPACE ice.ns").await;
    }
    create_with_row(&first, "ice.ns.t", 1).await;
    create_with_row(&second, "ice.ns.t", 2).await;
    let table_dir = warehouse.path().join("ns").join("t");

    assert_co_tenant_refused(&first, "ice", &second, "ice", &table_dir).await;
    assert_eq!(ids(&second, "ice.ns.t").await, vec![2]);
    assert_eq!(ids(&first, "ice.ns.t").await, vec![1]);
}

#[tokio::test]
async fn call_orphan_cotenancy_location_inside_another_table_refuses() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.ns").await;
    create_with_row(&session, "ice.ns.a", 1).await;
    create_with_row(&session, "ice.ns.b", 2).await;
    let own_dir = warehouse.path().join("ns").join("a");
    let other_dir = warehouse.path().join("ns").join("b");
    let other_live = referenced_data_file(&other_dir);
    age_tree(&other_dir, 10);

    for scan in file_spellings(&other_dir.join("data")) {
        let err = call_error(
            &session,
            &format!("CALL ice.system.remove_orphan_files(table => 'ns.a', location => '{scan}')"),
        )
        .await;
        assert_eq!(
            plan_message(err),
            inside_other_table_refusal(
                "ns.a",
                &scan,
                "ice.ns.b",
                &other_dir.display().to_string(),
                &own_dir.display().to_string(),
            ),
            "{scan}"
        );
    }
    assert!(
        other_live.exists(),
        "the other table's live file is untouched"
    );
    assert_eq!(ids(&session, "ice.ns.b").await, vec![2]);
}

#[tokio::test]
async fn call_orphan_cotenancy_own_history_metadata_copy_is_swept() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.ns").await;
    create_with_row(&session, "ice.ns.t", 1).await;
    let table_dir = warehouse.path().join("ns").join("t");
    let table = load(&session, "ice", &["ns", "t"]).await;
    let history = table
        .metadata()
        .metadata_log()
        .first()
        .expect("the insert logged the create's metadata file")
        .metadata_file
        .clone();
    let copy = table_dir.join("data").join("00000-copy.metadata.json");
    std::fs::copy(&history, &copy).expect("copy the history file");
    age_tree(&table_dir, 10);

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t')",
    )
    .await
    .expect("a copy of the table's own metadata is its own orphan");
    assert_eq!(listed, vec![copy.display().to_string()]);
    assert!(!copy.exists(), "the copy is deleted");
    assert!(Path::new(&history).exists(), "the logged file is kept");
    assert_eq!(ids(&session, "ice.ns.t").await, vec![1]);
}

#[tokio::test]
async fn call_orphan_cotenancy_stray_non_metadata_file_is_swept() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.ns").await;
    create_with_row(&session, "ice.ns.t", 1).await;
    let table_dir = warehouse.path().join("ns").join("t");
    let stray = table_dir.join("metadata").join("stray.json");
    std::fs::write(&stray, b"not table metadata").unwrap();
    age_tree(&table_dir, 10);

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t')",
    )
    .await
    .expect("a stray file that is not named *.metadata.json is never probed");
    assert_eq!(listed, vec![stray.display().to_string()]);
    assert!(!stray.exists());
    assert_eq!(ids(&session, "ice.ns.t").await, vec![1]);
}

#[tokio::test]
async fn call_orphan_cotenancy_single_catalog_default_sweep_deletes_the_orphan() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.ns").await;
    create_with_row(&session, "ice.ns.t", 1).await;
    submit(&session, "INSERT INTO ice.ns.t VALUES (2)").await;
    let table_dir = warehouse.path().join("ns").join("t");
    age_tree(&table_dir, 10);
    let orphan = plant(&table_dir, "orphan-file.parquet", 10);

    let listed = call_rows(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t')",
    )
    .await
    .expect("a table alone on the warehouse sweeps its own directory");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(!orphan.exists());
    assert_eq!(ids(&session, "ice.ns.t").await, vec![1, 2]);
}

async fn nested_namespace_session(warehouse: &TempDir) -> (ReparkSession, Table) {
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use iceberg::{NamespaceIdent, TableCreation};
    let session = memory_session(warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.a").await;
    let catalog = session.catalogs_snapshot().get("ice").unwrap().clone();
    let nested = NamespaceIdent::from_strs(["a", "t"]).unwrap();
    catalog
        .create_namespace(&nested, HashMap::new())
        .await
        .unwrap();
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let swept = catalog
        .create_table(
            &nested,
            TableCreation::builder()
                .name("x".to_string())
                .location(
                    warehouse
                        .path()
                        .join("a")
                        .join("t")
                        .join("x")
                        .display()
                        .to_string(),
                )
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    create_with_row(&session, "ice.a.y", 2).await;
    assert_eq!(
        load(&session, "ice", &["a", "y"])
            .await
            .metadata()
            .location(),
        warehouse.path().join("a").join("y").display().to_string()
    );
    (session, swept)
}

async fn nested_guards(session: &ReparkSession, swept: &Table, scan: &str) {
    use crate::call::remove_orphan_files::{
        refuse_scan_over_foreign_metadata, refuse_scan_over_other_tables,
        refuse_shared_temp_fallback_location,
    };
    let policy = session.catalogs_snapshot().location_policy("ice");
    let catalog = session.catalogs_snapshot().get("ice").unwrap().clone();
    refuse_shared_temp_fallback_location(policy.as_ref(), scan, "a.t.x")
        .unwrap_or_else(|error| panic!("{scan}: {error}"));
    refuse_scan_over_other_tables(
        policy.as_ref(),
        catalog.as_ref(),
        "ice",
        swept,
        scan,
        "a.t.x",
    )
    .await
    .unwrap_or_else(|error| panic!("{scan}: {error}"));
    refuse_scan_over_foreign_metadata(policy.as_ref(), swept, scan, "a.t.x")
        .await
        .unwrap_or_else(|error| panic!("{scan}: {error}"));
}

#[tokio::test]
async fn call_orphan_cotenancy_nested_namespace_table_data_dir_passes_the_guards() {
    let warehouse = TempDir::new().unwrap();
    let (session, swept) = nested_namespace_session(&warehouse).await;
    let scan = warehouse.path().join("a").join("t").join("x").join("data");
    for spelling in file_spellings(&scan) {
        nested_guards(&session, &swept, &spelling).await;
    }
    create_with_row(&session, "ice.a.t", 3).await;
    assert_eq!(
        load(&session, "ice", &["a", "t"])
            .await
            .metadata()
            .location(),
        warehouse.path().join("a").join("t").display().to_string()
    );
    for spelling in file_spellings(&scan) {
        nested_guards(&session, &swept, &spelling).await;
    }
}

#[tokio::test]
async fn call_orphan_cotenancy_table_located_inside_another_table_sweeps_its_data_dir() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.a").await;
    submit(&session, "CREATE NAMESPACE ice.o").await;
    create_with_row(&session, "ice.a.t", 1).await;
    let own_dir = warehouse.path().join("a").join("t").join("x");
    submit(
        &session,
        &format!(
            "CREATE TABLE ice.o.x (id INT) LOCATION '{}'",
            own_dir.display()
        ),
    )
    .await;
    submit(&session, "INSERT INTO ice.o.x VALUES (2)").await;
    let host_live = referenced_data_file(&warehouse.path().join("a").join("t"));
    age_tree(&own_dir, 10);
    age_tree(&warehouse.path().join("a").join("t").join("data"), 10);
    let orphan = plant(&own_dir, "orphan-file.parquet", 10);

    let listed = call_rows(
        &session,
        &format!(
            "CALL ice.system.remove_orphan_files(table => 'o.x', location => '{}')",
            own_dir.join("data").display()
        ),
    )
    .await
    .expect("a path inside the swept table's own location is sweepable");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(!orphan.exists());
    assert!(host_live.exists());
    assert_eq!(ids(&session, "ice.o.x").await, vec![2]);
    assert_eq!(ids(&session, "ice.a.t").await, vec![1]);
}

#[tokio::test]
async fn call_orphan_cotenancy_unreadable_metadata_file_refuses() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["ice"]).await;
    submit(&session, "CREATE NAMESPACE ice.ns").await;
    create_with_row(&session, "ice.ns.t", 1).await;
    let table_dir = warehouse.path().join("ns").join("t");
    let unreadable = table_dir
        .join("metadata")
        .join("00009-broken.metadata.json");
    std::fs::write(&unreadable, b"{ not json").unwrap();
    age_tree(&table_dir, 10);

    let err = call_error(
        &session,
        "CALL ice.system.remove_orphan_files(table => 'ns.t')",
    )
    .await;
    let message = plan_message(err);
    let prefix = format!(
        "CALL remove_orphan_files refuses to sweep `ns.t`: path `{}` holds `{}`, a metadata file \
         that is not in the swept table's metadata log and cannot be read as table metadata (",
        table_dir.display(),
        unreadable.display()
    );
    assert!(message.starts_with(&prefix), "{message}");
    assert!(
        message.ends_with(
            "), so it may belong to another table whose live files this procedure would delete. \
             Give the table its own LOCATION (`CREATE TABLE ... LOCATION '<path>'`), then sweep \
             it."
        ),
        "{message}"
    );
    assert!(unreadable.exists());
    assert_eq!(ids(&session, "ice.ns.t").await, vec![1]);
}
