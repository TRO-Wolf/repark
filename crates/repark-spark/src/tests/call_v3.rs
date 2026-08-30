//! Pins format-v3 maintenance behavior, including lineage and dangling-delete guards.

use super::super::*;
use super::common::*;

use iceberg::spec::FormatVersion;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

/// Upgrade an engine-created v2 table to v3 through the fork's transaction action.
async fn upgrade_to_v3(catalog: &Arc<dyn Catalog>, ident: &TableIdent) {
    let table = catalog.load_table(ident).await.expect("load for upgrade");
    let transaction = Transaction::new(&table);
    let action = transaction
        .upgrade_table_version()
        .set_format_version(FormatVersion::V3);
    let transaction = action.apply(transaction).expect("apply upgrade");
    transaction
        .commit(catalog.as_ref())
        .await
        .expect("commit upgrade");
}

/// Six single-row data files, which is one over Spark's `min-input-files` floor of five.
async fn seed_six_files(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} AS SELECT 1 AS id, 'a' AS name"),
    )
    .await;
    for index in 2..=6 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
}

/// The fixture is genuinely v3, asserted before anything is concluded from it.
#[tokio::test]
async fn v3_fixture_really_is_format_v3() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_six_files(&ctx, &catalogs, "fixture_check").await;
    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "fixture_check"]).unwrap();

    let before = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        before.metadata().format_version(),
        FormatVersion::V2,
        "the engine creates v2 tables; if this ever changes the guard's domain changes with it"
    );

    upgrade_to_v3(catalog, &ident).await;

    let after = catalog.load_table(&ident).await.unwrap();
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
}

/// pins: rp-2-fork-repin/C-004
/// pins: rp-3-fork-repin/C-005
/// **The guard.** `rewrite_data_files` refuses a v3 table instead of compacting it.
#[tokio::test]
async fn call_rewrite_data_files_refuses_a_v3_table_rather_than_reassigning_row_lineage() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_six_files(&ctx, &catalogs, "v3_rewrite").await;
    let catalog = catalogs.get("ice").expect("ice catalog");
    upgrade_to_v3(
        catalog,
        &TableIdent::from_strs(["sales", "v3_rewrite"]).unwrap(),
    )
    .await;

    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v3_rewrite')",
    )
    .await
    .expect_err("rewrite_data_files must refuse a v3 table, not silently reassign row lineage")
    .to_string();

    assert!(
        err.contains("row lineage"),
        "the refusal has to name row lineage, or an operator cannot tell why it fired: {err}"
    );
    assert!(
        err.contains("V3"),
        "the refusal has to name the format version that triggered it: {err}"
    );
}

/// The incidental control: v2 still compacts, and still reports five columns.
#[tokio::test]
async fn call_rewrite_data_files_still_compacts_a_v2_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_six_files(&ctx, &catalogs, "v2_rewrite").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v2_rewrite')",
    )
    .await
    .expect("v2 rewrite must still run")
    .collect()
    .await
    .expect("collect v2 rewrite");
    let batch = &batches[0];

    assert_eq!(batch.schema().fields().len(), 5, "Spark's five columns");
    let rewritten = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("rewritten_data_files_count is Int32")
        .value(0);
    assert_eq!(
        rewritten, 6,
        "six single-row files must still be compacted on v2"
    );
}

/// The guard's blast-radius argument, pinned instead of asserted.
// pins: v3-2-create-v3-opt-in/C-004, C-008
#[tokio::test]
async fn the_engine_still_cannot_produce_a_v3_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v2only AS SELECT 1 AS id, 'a' AS name",
    )
    .await;

    for sql in [
        "CREATE TABLE ice.sales.c3 (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
        "CREATE TABLE ice.sales.c4 USING iceberg TBLPROPERTIES ('format-version' = '3') \
         AS SELECT 1 AS id",
        "ALTER TABLE ice.sales.v2only SET TBLPROPERTIES ('format-version' = '3')",
        "ALTER TABLE ice.sales.v2only SET TBLPROPERTIES ('format-version' = '3', 'k' = 'v')",
    ] {
        let outcome = match execute(&ctx, &catalogs, sql).await {
            Ok(frame) => frame.collect().await.err().map(|err| err.to_string()),
            Err(err) => Some(err.to_string()),
        };
        assert!(
            outcome.is_some(),
            "this door produced a v3 table, so the guard's blast-radius claim is wrong: {sql}"
        );
    }

    // And the table it did create is still v2.
    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "v2only"]).unwrap();
    assert_eq!(
        catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .format_version(),
        FormatVersion::V2
    );
}

/// pins: v3-2-create-v3-opt-in/C-005, C-011, C-015
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn opt_in_create_produces_v3_and_rewrite_still_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3opt (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.v3opt SELECT 1 AS id",
    )
    .await;

    let catalog = catalogs.get("ice").expect("ice catalog");
    let ident = TableIdent::from_strs(["sales", "v3opt"]).unwrap();
    assert_eq!(
        catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .format_version(),
        FormatVersion::V3
    );

    let rewrite = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.v3opt')",
    )
    .await
    .expect_err("rewrite_data_files must still refuse an engine-created v3 table")
    .to_string();
    assert!(
        rewrite.contains("row lineage") && rewrite.contains("V3"),
        "V3-LINEAGE-1 must still fire on opt-in CREATE: {rewrite}"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3mor (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.v3mor SELECT 1 AS id",
    )
    .await;
    let merge = match execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.v3mor AS t USING (SELECT 1 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
    )
    .await
    {
        Ok(frame) => frame.collect().await.err().map(|err| err.to_string()),
        Err(err) => Some(err.to_string()),
    };
    let merge = merge.expect("merge-on-read MERGE must still refuse a v3 table");
    assert!(
        merge.contains("V3"),
        "non-v2 MoR MERGE guard must still fire: {merge}"
    );
}
