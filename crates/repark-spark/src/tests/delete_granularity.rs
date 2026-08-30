//! pins: mw-9-delete-granularity/C-009, C-011
//! MW-9 — Spark-door `write.delete.granularity` (MOR-2).

use super::super::*;
use super::common::*;

async fn seed_six_data_files(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    for id in 1..=6 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
}

async fn merge_all_six(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "MERGE INTO ice.sales.{table} AS t USING (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL \
             SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) AS s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = 'merged'"
        ),
    )
    .await;
}

async fn delete_file_count(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> usize {
    rows(
        ctx,
        catalogs,
        &format!("SELECT * FROM ice.sales.{table}.files WHERE content = 1"),
    )
    .await
}

/// pins: mw-9-delete-granularity/C-003, C-006
#[tokio::test]
async fn explicit_partition_granularity_writes_one_delete_file() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.part (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', \
         'write.delete.granularity' = 'partition')",
    )
    .await;
    seed_six_data_files(&ctx, &catalogs, "part").await;
    merge_all_six(&ctx, &catalogs, "part").await;
    assert_eq!(
        delete_file_count(&ctx, &catalogs, "part").await,
        1,
        "explicit partition: one delete file for the unpartitioned table"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.part").await,
        6
    );
}

/// pins: mw-9-delete-granularity/C-002
#[tokio::test]
async fn explicit_file_granularity_matches_the_unset_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.fileg (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', \
         'write.delete.granularity' = 'file')",
    )
    .await;
    seed_six_data_files(&ctx, &catalogs, "fileg").await;
    merge_all_six(&ctx, &catalogs, "fileg").await;
    assert_eq!(delete_file_count(&ctx, &catalogs, "fileg").await, 6);
}

/// pins: mw-9-delete-granularity/C-004
#[tokio::test]
async fn unknown_delete_granularity_refuses_before_any_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bad (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', \
         'write.delete.granularity' = 'banana')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.bad VALUES (1, 'a')").await;
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.bad AS t USING (SELECT 1 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.v = 'x'",
    )
    .await
    .expect_err("unknown granularity must refuse")
    .to_string();
    assert!(
        err.contains("write.delete.granularity")
            && err.contains("'file'")
            && err.contains("'partition'")
            && err.contains("banana"),
        "refuse must name the property and both legal values: {err}"
    );
    assert_eq!(
        delete_file_count(&ctx, &catalogs, "bad").await,
        0,
        "a refused MERGE must not commit a delete file"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.bad").await,
        1
    );
}

/// pins: mw-9-delete-granularity/C-005
#[tokio::test]
async fn fork_table_provider_delete_is_not_this_writer() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.del (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')",
    )
    .await;
    seed_six_data_files(&ctx, &catalogs, "del").await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.del WHERE id IN (1, 2, 3, 4, 5, 6)",
    )
    .await;
    assert_eq!(
        delete_file_count(&ctx, &catalogs, "del").await,
        1,
        "SQL DELETE goes through iceberg-datafusion, which has no granularity knob \
         (ENGINE_CONTRACT §7); MW-9 is the RePark MERGE writer"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.del").await,
        0
    );
}

/// pins: mw-9-delete-granularity/C-007
#[tokio::test]
async fn alter_set_granularity_is_honored_on_the_next_merge() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.flip (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    seed_six_data_files(&ctx, &catalogs, "flip").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.flip SET TBLPROPERTIES ('write.delete.granularity' = 'partition')",
    )
    .await;
    merge_all_six(&ctx, &catalogs, "flip").await;
    assert_eq!(
        delete_file_count(&ctx, &catalogs, "flip").await,
        1,
        "the next MERGE must read the property from current table metadata"
    );
}

/// pins: mw-9-delete-granularity/C-004
#[tokio::test]
async fn unknown_granularity_refuses_identity_update_before_any_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.badu (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read', \
         'write.delete.granularity' = 'banana')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.badu VALUES (1, 'a')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.keysu (id INT) USING iceberg TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.keysu VALUES (1)").await;
    let err = execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.badu SET v = 'x' WHERE id IN (SELECT id FROM ice.sales.keysu)",
    )
    .await
    .expect_err("unknown granularity must refuse identity UPDATE")
    .to_string();
    assert!(
        err.contains("write.delete.granularity")
            && err.contains("'file'")
            && err.contains("'partition'")
            && err.contains("banana"),
        "refuse must name the property and both legal values: {err}"
    );
    assert_eq!(
        delete_file_count(&ctx, &catalogs, "badu").await,
        0,
        "a refused identity UPDATE must not commit a delete file"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.badu").await,
        1
    );
}

/// pins: mw-9-delete-granularity/C-005
#[tokio::test]
async fn fork_table_provider_update_is_not_this_writer() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.updf (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read')",
    )
    .await;
    seed_six_data_files(&ctx, &catalogs, "updf").await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.updf SET v = 'x' WHERE id IN (1, 2, 3, 4, 5, 6)",
    )
    .await;
    assert_eq!(
        delete_file_count(&ctx, &catalogs, "updf").await,
        1,
        "SQL UPDATE with a literal IN list goes through iceberg-datafusion, which has no \
         granularity knob (ENGINE_CONTRACT §7); MW-9 is the RePark MERGE / identity writer"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.updf").await,
        6
    );
}
