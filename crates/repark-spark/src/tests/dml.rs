/// `DELETE FROM … WHERE` passes through to the fork provider's `delete_from` (copy-on-write
/// default) and removes exactly the matched rows. Lock-down for the D1 adapter slice: `RePark`
/// adds no code here — DataFusion 52.2 plans SQL DML onto the `TableProvider`.
use super::super::*;
use super::common::*;

#[tokio::test]
async fn delete_where_copy_on_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE id = 2").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 2);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 2").await,
        0
    );
}

/// `DELETE FROM t` with no WHERE empties the table (the provider's predicate-None path).
#[tokio::test]
async fn delete_all_rows_empties_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(&ctx, &catalogs, "DELETE FROM ice.sales.t").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

/// `UPDATE … SET … WHERE` passes through to the provider's `update` (copy-on-write default):
/// matched rows take the SET values, unmatched rows survive byte-identical.
#[tokio::test]
async fn update_where_copy_on_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET name = 'updated' WHERE id > 1",
    )
    .await;

    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE name = 'updated'"
        )
        .await,
        2
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE id = 1 AND name = 'a'"
        )
        .await,
        1
    );
}

/// `write.delete.mode = merge-on-read` threads through CTAS `TBLPROPERTIES` and the provider
/// takes the merge-on-read path (position deletes / DVs); the merged read hides the deleted row. The
/// mode-dispatch internals are the fork's tests' job — this locks `RePark`'s property plumbing
/// plus the merged read through our registered provider.
#[tokio::test]
async fn delete_merge_on_read_mode() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t \
             TBLPROPERTIES('write.delete.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;

    run(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE name = 'b'").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 2);
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE name = 'b'"
        )
        .await,
        0
    );
}

/// `write.update.mode = merge-on-read`: the merge-on-read UPDATE (delete + re-insert in one commit)
/// reads back with the new values through our registered provider.
#[tokio::test]
async fn update_merge_on_read_mode() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t \
             TBLPROPERTIES('write.update.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET name = 'X' WHERE id = 3",
    )
    .await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE id = 3 AND name = 'X'"
        )
        .await,
        1
    );
}

/// BUG-001: merge-on-read DELETE on a table that evolved to unpartitioned (multi-spec history) refuses.
#[tokio::test]
async fn bug001_mor_delete_refuses_unpartitioned_after_partition_evolution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.hzrd (id INT, category STRING) USING iceberg \
             TBLPROPERTIES('write.delete.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.hzrd VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd ADD PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd DROP PARTITION FIELD category",
    )
    .await;
    let table = load_sales_table(&catalogs, "hzrd").await;
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned()
            || table
                .metadata()
                .default_partition_spec()
                .fields()
                .is_empty(),
        "post-DROP default must be unpartitioned for the hazard"
    );
    assert!(
        table.metadata().partition_specs_iter().len() > 1,
        "need multi-spec history for the hazard valve"
    );

    let err = execute(&ctx, &catalogs, "DELETE FROM ice.sales.hzrd WHERE id = 1")
        .await
        .expect_err("BUG-001 must refuse MoR DELETE on evolved unpartitioned");
    let text = err.to_string();
    assert!(
        text.contains("merge-on-read")
            && (text.contains("under-delete")
                || text.contains("partition_key")
                || text.contains("write_position_deletes")),
        "message must name the fork MoR hazard, got {text}"
    );
    assert!(
        text.contains("copy-on-write") || text.contains("MERGE INTO"),
        "message must name COW/MERGE workaround, got {text}"
    );
    // Rows must be untouched (refuse before provider DML).
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.hzrd").await,
        2
    );
}

/// BUG-001: non-evolved unpartitioned merge-on-read DELETE still passes (single-spec history).
#[tokio::test]
async fn bug001_mor_delete_allows_never_evolved_unpartitioned() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.safe \
             TBLPROPERTIES('write.delete.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;
    let table = load_sales_table(&catalogs, "safe").await;
    assert!(table.metadata().default_partition_spec().is_unpartitioned());
    assert_eq!(table.metadata().partition_specs_iter().len(), 1);

    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.safe WHERE name = 'b'",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.safe").await,
        2
    );
}

/// BUG-001: currently-partitioned merge-on-read DELETE passes even with multi-spec history.
#[tokio::test]
async fn bug001_mor_delete_allows_partitioned_after_evolution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.parted (id INT, category STRING) USING iceberg \
             TBLPROPERTIES('write.delete.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.parted VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.parted ADD PARTITION FIELD category",
    )
    .await;
    let table = load_sales_table(&catalogs, "parted").await;
    assert!(!table.metadata().default_partition_spec().is_unpartitioned());
    assert!(table.metadata().partition_specs_iter().len() > 1);

    run(&ctx, &catalogs, "DELETE FROM ice.sales.parted WHERE id = 1").await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.parted").await,
        1
    );
}

/// BUG-001 critic F-A2-C3-001: mixed-case / padded `write.delete.mode` must still refuse.
#[tokio::test]
async fn bug001_mor_delete_refuses_mixed_case_mode_property() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.hzrd_case (id INT, category STRING) USING iceberg \
             TBLPROPERTIES('write.delete.mode' = 'Merge-On-Read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.hzrd_case VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd_case ADD PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd_case DROP PARTITION FIELD category",
    )
    .await;
    let err = execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.hzrd_case WHERE id = 1",
    )
    .await
    .expect_err("mixed-case MoR mode must not under-refuse");
    let text = err.to_string();
    assert!(
        text.contains("merge-on-read") || text.contains("under-delete"),
        "must refuse mixed-case mode, got {text}"
    );
}

/// BUG-001 critic F-A2-C1-001: aliases must not under-refuse the merge-on-read multi-spec valve.
#[tokio::test]
async fn bug001_mor_delete_refuses_when_table_aliased() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.hzrd_alias (id INT, category STRING) USING iceberg \
             TBLPROPERTIES('write.delete.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.hzrd_alias VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd_alias ADD PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd_alias DROP PARTITION FIELD category",
    )
    .await;
    for sql in [
        "DELETE FROM ice.sales.hzrd_alias AS t WHERE t.id = 1",
        "DELETE FROM ice.sales.hzrd_alias t WHERE t.id = 1",
    ] {
        let err = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("aliased MoR DELETE must still hit BUG-001 valve");
        let text = err.to_string();
        assert!(
            text.contains("merge-on-read")
                && (text.contains("under-delete")
                    || text.contains("partition_key")
                    || text.contains("write_position_deletes")),
            "aliased DELETE must name MoR hazard, sql={sql:?}, got {text}"
        );
    }
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.hzrd_alias").await,
        2
    );
}

/// BUG-001 critic F-A2-C1-003: UPDATE hazard refuse (write.update.mode) + alias path.
#[tokio::test]
async fn bug001_mor_update_refuses_unpartitioned_after_partition_evolution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.hzrd_upd (id INT, category STRING, name STRING) USING iceberg \
             TBLPROPERTIES('write.update.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.hzrd_upd VALUES (1, 'a', 'x'), (2, 'b', 'y')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd_upd ADD PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.hzrd_upd DROP PARTITION FIELD category",
    )
    .await;
    for sql in [
        "UPDATE ice.sales.hzrd_upd SET name = 'z' WHERE id = 1",
        "UPDATE ice.sales.hzrd_upd AS t SET name = 'z' WHERE t.id = 1",
    ] {
        let err = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("MoR UPDATE multi-spec unpartitioned must refuse");
        let text = err.to_string();
        assert!(
            text.contains("UPDATE") && text.contains("merge-on-read"),
            "UPDATE refuse must name verb+mode, sql={sql:?}, got {text}"
        );
        assert!(
            text.contains("copy-on-write") || text.contains("MERGE INTO"),
            "must name workaround, sql={sql:?}, got {text}"
        );
    }
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.hzrd_upd WHERE name = 'x'"
        )
        .await,
        1,
        "refuse must leave rows untouched"
    );
}
