use super::super::*;
use super::common::*;
use super::wap_id::{WAP_DDL, seed as wap_seed_table, set_wap};

const KEY: &str = "review-test-key";

struct State {
    snapshots: Vec<i64>,
    files: std::collections::HashSet<String>,
    objects: usize,
}

async fn load_enc(catalogs: &CatalogRegistry, table: &str) -> iceberg::table::Table {
    let ident = TableIdent::from_strs(["sales", table]).expect("ident");
    catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load")
}

async fn capture(wh: &TempDir, catalogs: &CatalogRegistry, table: &str) -> State {
    let loaded = load_enc(catalogs, table).await;
    let mut snapshots: Vec<i64> = loaded
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    snapshots.sort_unstable();
    State {
        snapshots,
        files: live_data_file_paths(catalogs, table).await,
        objects: count_objects(wh.path()),
    }
}

async fn assert_unchanged(wh: &TempDir, catalogs: &CatalogRegistry, table: &str, before: &State) {
    let after = capture(wh, catalogs, table).await;
    assert_eq!(after.snapshots, before.snapshots);
    assert_eq!(after.files, before.files);
    assert_eq!(after.objects, before.objects);
}

async fn attempt(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> datafusion::error::DataFusionError {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("write unexpectedly succeeded: {sql}"),
            Err(error) => error,
        },
        Err(error) => error,
    }
}

fn assert_refusal(error: datafusion::error::DataFusionError, table: &str) {
    let mapped = repark_core::engine_err(error);
    assert!(
        matches!(&mapped, repark_core::Error::NotImplemented(_)),
        "refusal must map to Unsupported, got: {mapped:?}"
    );
    assert_eq!(
        mapped.exception_class(),
        repark_core::ErrorClass::Unsupported
    );
    let message = mapped.to_string();
    for needle in [
        table,
        "encryption.key-id",
        "no table encryption",
        "plaintext",
        "ENC-1",
    ] {
        assert!(
            message.contains(needle),
            "refusal must name {needle}, got: {message}"
        );
    }
    assert!(
        !message.contains(KEY),
        "refusal must never echo the key value, got: {message}"
    );
}

async fn create_keyed(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '{KEY}')"
        ),
    )
    .await;
}

async fn seed_keyed(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')"),
    )
    .await;
}

async fn setup_enc(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    setup_allow_create_format_version_3(wh).await
}

#[tokio::test]
async fn encrypted_table_write_must_refuse_without_encryption_support() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    create_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_insert_select_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    create_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.enc SELECT * FROM src",
    )
    .await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_insert_with_statement_options_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "compression-codec".to_string(),
        "zstd".to_string(),
    )])
    .expect("options");
    let error = crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.enc VALUES (3, 'c')",
        &std::collections::HashSet::<String>::new(),
        &options,
    )
    .await
    .expect_err("options insert must refuse");
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_insert_by_name_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.enc BY NAME SELECT 3 AS id, 'c' AS name",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_insert_overwrite_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.enc SELECT 3, 'c'",
    )
    .await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_replace_where_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.enc (id INT, name STRING, cat STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.enc VALUES (1, 'a', 'x'), (2, 'b', 'y')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.enc SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')"),
    )
    .await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.enc REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_ctas_refuses_without_leaving_a_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    let objects_before = count_objects(warehouse.path());
    let error = attempt(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.ctas_enc USING iceberg TBLPROPERTIES ('format-version' = '3', \
             'encryption.key-id' = '{KEY}') AS SELECT 1 AS id, 'a' AS name"
        ),
    )
    .await;
    assert_refusal(error, "ice.sales.ctas_enc");
    let ident = TableIdent::from_strs(["sales", "ctas_enc"]).expect("ident");
    assert!(
        catalogs
            .get("ice")
            .expect("ice")
            .load_table(&ident)
            .await
            .is_err()
    );
    assert_eq!(count_objects(warehouse.path()), objects_before);
}

#[tokio::test]
async fn encrypted_create_or_replace_refuses_and_keeps_the_existing_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.enc (id INT, name STRING) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        &format!(
            "CREATE OR REPLACE TABLE ice.sales.enc USING iceberg TBLPROPERTIES \
             ('format-version' = '3', 'encryption.key-id' = '{KEY}') AS SELECT 2 AS id, 'b' AS name"
        ),
    )
    .await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![(1, "a".to_string())]
    );
}

#[tokio::test]
async fn replace_onto_keyed_table_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.enc AS SELECT 2 AS id, 'b' AS name",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_merge_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.enc AS t USING (SELECT 1 AS id, 'z' AS name) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_update_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.enc SET name = 'z' WHERE id = 1",
    )
    .await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_delete_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "DELETE FROM ice.sales.enc WHERE id = 1").await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_whole_delete_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "DELETE FROM ice.sales.enc").await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_aliased_delete_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.twin (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.twin VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.twin AS x WHERE x.id = 1",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.twin").await,
        vec![(2, "b".to_string())]
    );
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.enc AS x WHERE x.id = 1",
    )
    .await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_case_folded_update_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.twin (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.twin VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ICE.SALES.TWIN SET name = 'z' WHERE id = 1",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.twin").await,
        vec![(1, "z".to_string()), (2, "b".to_string())]
    );
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "UPDATE ICE.SALES.ENC SET name = 'z' WHERE id = 1",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_truncate_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.enc").await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_rewrite_data_files_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.enc')",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_rewrite_manifests_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.enc')",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_rewrite_position_delete_files_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.enc')",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_add_files_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let dest = warehouse.path().join("addsrc");
    run(
        &ctx,
        &catalogs,
        &format!(
            "COPY (SELECT 3 AS id, 'c' AS name) TO '{}' STORED AS PARQUET",
            dest.display()
        ),
    )
    .await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.add_files(table => 'sales.enc', source_table => '`parquet`.`{}`')",
            dest.display()
        ),
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_publish_changes_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    wap_seed_table(&ctx, &catalogs, WAP_DDL).await;
    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.t SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')"),
    )
    .await;
    let before = capture(&warehouse, &catalogs, "t").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "CALL ice.system.publish_changes(table => 'sales.t', wap_id => 'w1')",
    )
    .await;
    assert_refusal(error, "sales.t");
    assert_unchanged(&warehouse, &catalogs, "t", &before).await;
}

#[tokio::test]
async fn encrypted_table_direct_append_commit_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let table = load_enc(&catalogs, "enc").await;
    let catalog = catalogs.get("ice").expect("ice");
    let error =
        repark_iceberg::write::commit_append_with_summary(catalog, &table, Vec::new(), &[], None)
            .await
            .expect_err("direct sink commit must refuse");
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_run_maintenance_dry_run_runs() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "orders").await;
    let policy = repark_core::parse_maintenance_policy(
        "default",
        "[default.maintenance]\ntarget_file_size_bytes = 536870912\nsnapshot_retain_last = 5\n",
    )
    .expect("policy");
    catalogs.set_maintenance_policy("default", policy);
    let before = capture(&warehouse, &catalogs, "orders").await;
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders')",
    )
    .await;
    assert_unchanged(&warehouse, &catalogs, "orders", &before).await;
}

#[tokio::test]
async fn encrypted_table_expire_snapshots_runs() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.enc (id INT, name STRING) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (2, 'b')").await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.enc SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.expire_snapshots(table => 'sales.enc', retain_last => 1)",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
}

#[tokio::test]
async fn encrypted_table_remove_orphan_files_runs() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.remove_orphan_files(table => 'sales.enc')",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
}

#[tokio::test]
async fn encrypted_table_rollback_to_snapshot_runs() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.enc (id INT, name STRING) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    let first = load_enc(&catalogs, "enc")
        .await
        .metadata()
        .current_snapshot_id()
        .expect("s1");
    run(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (2, 'b')").await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.enc SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.enc', snapshot_id => {first})"
        ),
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![(1, "a".to_string())]
    );
}

#[tokio::test]
async fn encrypted_table_unset_key_restores_writes() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (3, 'c')").await;
    assert_refusal(error, "ice.sales.enc");
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.enc UNSET TBLPROPERTIES ('encryption.key-id')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (3, 'c')").await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string())
        ]
    );
}

#[tokio::test]
async fn encrypted_table_empty_key_value_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.enc (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '')",
    )
    .await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_lookalike_keys_stay_writable() {
    for (table, property) in [
        ("look_a", "encryption.keyid"),
        ("look_b", "encryption.key-id-x"),
    ] {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_enc(&warehouse).await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg \
                 TBLPROPERTIES ('format-version' = '3', '{property}' = '{KEY}')"
            ),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a')"),
        )
        .await;
        assert_eq!(
            table_rows(&ctx, &catalogs, &format!("ice.sales.{table}")).await,
            vec![(1, "a".to_string())],
            "{property} must not refuse"
        );
    }
}

#[tokio::test]
async fn encrypted_table_select_runs() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
}

#[tokio::test]
async fn encrypted_table_create_and_empty_scan_run() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    create_keyed(&ctx, &catalogs, "enc").await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.enc").await,
        Vec::<(i32, String)>::new()
    );
    let table = load_enc(&catalogs, "enc").await;
    assert_eq!(
        table
            .metadata()
            .properties()
            .get("encryption.key-id")
            .map(String::as_str),
        Some(KEY)
    );
}

#[tokio::test]
async fn encrypted_table_explain_insert_runs_without_writing() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    run(
        &ctx,
        &catalogs,
        "EXPLAIN INSERT INTO ice.sales.enc VALUES (3, 'c')",
    )
    .await;
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_explain_analyze_insert_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    seed_keyed(&ctx, &catalogs, "enc").await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(
        &ctx,
        &catalogs,
        "EXPLAIN ANALYZE INSERT INTO ice.sales.enc VALUES (3, 'c')",
    )
    .await;
    assert_refusal(error, "sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn version_two_table_with_key_refuses_insert() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.enc (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('encryption.key-id' = '{KEY}')"
        ),
    )
    .await;
    let before = capture(&warehouse, &catalogs, "enc").await;
    let error = attempt(&ctx, &catalogs, "INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    assert_refusal(error, "ice.sales.enc");
    assert_unchanged(&warehouse, &catalogs, "enc", &before).await;
}

#[tokio::test]
async fn table_without_key_stays_writable_on_every_door() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_enc(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.twin (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.twin VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.twin SELECT 3, 'c'",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.twin").await,
        vec![(3, "c".to_string())]
    );
    run(&ctx, &catalogs, "DELETE FROM ice.sales.twin WHERE id = 3").await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.twin").await,
        Vec::<(i32, String)>::new()
    );
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.twin')",
    )
    .await;
    run(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.twin").await;
}
