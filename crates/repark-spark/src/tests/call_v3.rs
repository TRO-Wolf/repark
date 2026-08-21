//! V3-0 — what the `CALL` procedures do when the table is format-v3.
//!
//! Split from `call.rs` on subject, the same way `call_orphan.rs` was: these tests are all about
//! one table property rather than one procedure. The fixture is built by upgrading a table the
//! engine created, not by reading a Spark-written one, so the pins run in CI with no oracle. The
//! Spark measurements that motivated them are in `task/v3-0-charter-ledger.md`.
//!
//! Registry rows `V3-LINEAGE-1` and `V3-DANGLE-1`.

use super::super::*;
use super::common::*;

use iceberg::spec::FormatVersion;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

/// Upgrade a table the engine created from v2 to v3, through the fork's own transaction action.
///
/// This is the only way to get a v3 table into a test: `CREATE TABLE` refuses `format-version`
/// (`create_table.rs`), and CTAS refuses it too, so nothing on the engine's own surface produces
/// one. Going through `Transaction` rather than hand-writing metadata means the fixture is a v3
/// table the fork itself considers valid, which is the point — a hand-rolled one could be wrong
/// in exactly the way that makes the pin below pass for the wrong reason.
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

/// Six single-row data files, which is one over Spark's `min-input-files` floor of five, so the
/// rewrite has real work to do and a zero result cannot be mistaken for "nothing to compact".
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
///
/// Without this the refusal pin below would still pass if `upgrade_to_v3` silently no-opped and
/// left a v2 table behind — it would just be passing for a different reason than the one claimed.
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

/// **The guard.** `rewrite_data_files` refuses a v3 table instead of compacting it.
///
/// Measured against Spark 4.0.1 + Iceberg 1.10.0 on a six-file v3 table with deletion vectors:
/// Spark carries `_row_id` and `_last_updated_sequence_number` through the rewrite unchanged
/// (`id=5099` kept `row_id=599, seq=6` on both sides of the CALL). The engine's rewrite produced
/// the right rows and **reassigned every row's lineage** (`row_id=691, seq=9`), which tells a
/// downstream consumer that all 546 rows were updated when none were. The fork's
/// `maintenance/rewrite_data_files.rs` has no row-lineage handling at all, so this is not
/// something the engine can fix in the CALL router.
///
/// Refusing is stricter than Spark, which does the rewrite correctly. It is the same trade MW-2
/// took for deletion vectors: a loud refusal beats a silent, plausible, wrong result on a
/// procedure an operator runs unattended.
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
///
/// The guard is a format-version test, so the failure mode worth pinning is that it fires one
/// version too early and quietly disables compaction on every table the engine actually creates.
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
///
/// `V3-LINEAGE-1` claims the refusal costs nothing on tables this engine wrote, because this
/// engine cannot write a v3 table. That claim is doing real work — it is why a refusal stricter
/// than Spark is defensible — so every door to a v3 table is pinned here rather than left to the
/// prose. This test lives beside the guard for that reason, not because it is about `CALL`.
///
/// The `ALTER` doors are the ones worth having. They are refused **one layer down**, by the
/// fork's `set_properties` rejecting reserved properties — nothing in this engine looks at
/// `format-version` on the `ALTER` path. That makes it an upstream behaviour the guard's argument
/// depends on, and the fork's own doc comment on that function describes the opposite policy
/// ("the corresponding action is performed"). If the fork ever matches its comment, this pin goes
/// red and the reachability claim gets revisited before the guard does.
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

    // And the table it did create is still v2 — a refusal that fired after a partial upgrade
    // would satisfy the loop above while breaking the claim.
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
