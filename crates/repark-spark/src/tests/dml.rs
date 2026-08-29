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

/// Merge-on-read DELETE refuses after a table evolves to unpartitioned (multiple specs remain).
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

/// A non-evolved unpartitioned merge-on-read DELETE still passes with one spec.
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

/// A currently-partitioned merge-on-read DELETE passes even with multiple specs.
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

/// BUG-001: mixed-case and padded `write.delete.mode` must still refuse.
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

/// BUG-001: aliases must not under-refuse the merge-on-read multi-spec valve.
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

/// BUG-001: UPDATE hazard refusal covers `write.update.mode` and its alias path.
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

// === G3-E8 — the subquery-predicate DML valve ===============================================
//
// The defect these pin: a subquery in a DELETE/UPDATE `WHERE` clause is lost at DataFusion's DML
// planning boundary (`extract_dml_filters` recovers nothing from the semi/anti/mark join the
// optimizer decorrelated it into), and an empty filter list is the fork provider's spelling of
// "no WHERE clause" — so the statement matched EVERY row. Recorded pre-guard behaviour and the
// full statement-form matrix: task/g3e8-guard-ledger.md.

/// A target + a one-key source table, the G3-E8 fixture. `keys` holds exactly `(2, 'K')`, and
/// `'K'` appears in no `tgt` row, so an assignment sourced from it is observable.
async fn g3e8_setup(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tgt AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.keys AS SELECT 2 AS id, 'K' AS name",
    )
    .await;
    (ctx, catalogs)
}

/// The seeded contents of `ice.sales.tgt` — what every refusal must leave untouched.
fn g3e8_seed() -> Vec<(i32, String)> {
    vec![
        (1, "a".to_string()),
        (2, "b".to_string()),
        (3, "c".to_string()),
    ]
}

/// Assert the refusal is the G3-E8 valve's OWN message — the defect class, the consequence, the
/// MERGE workaround, and that support returns. A generic planner error must NOT satisfy this.
fn assert_g3e8_message(text: &str, verb: &str, sql: &str) {
    assert!(
        text.contains("subquery predicates are silently mis-executed"),
        "must name the defect class, sql={sql:?}, got {text}"
    );
    assert!(
        text.contains("G3-E8"),
        "must name the defect id, sql={sql:?}, got {text}"
    );
    assert!(
        text.contains(verb),
        "must name the refused verb {verb}, sql={sql:?}, got {text}"
    );
    assert!(
        text.contains("MERGE INTO"),
        "must name the MERGE INTO workaround, sql={sql:?}, got {text}"
    );
    assert!(
        text.contains("Support returns when the underlying fix lands"),
        "must say support returns with the fix, sql={sql:?}, got {text}"
    );
}

/// The confirmed repro (intake G3-E8): `DELETE … WHERE id IN (SELECT …)` emptied the table.
/// The identity path now deletes exactly the matching row (Spark `{1,3}`).
#[tokio::test]
async fn g3e8_delete_in_subquery_deletes_exactly_the_matching_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    let sql = "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)";

    execute(&ctx, &catalogs, sql)
        .await
        .expect("uncorrelated DELETE IN must execute on the identity path");

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
        "IN-DELETE must remove only id=2 — pre-fix this table was EMPTY"
    );
}

/// Quoted target + temp-view source: same product spelling, different target/source forms.
#[tokio::test]
async fn g3e8_delete_in_subquery_quoted_and_temp_view_source_execute() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM \"ice\".\"sales\".\"tgt\" WHERE id IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("quoted-target IN must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM src WHERE id = 2)",
    )
    .await
    .expect("temp-view source IN must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
    );
}

/// Uncorrelated `DELETE … NOT IN (SELECT …)` — Spark 3VL, not set-difference.
#[tokio::test]
async fn g3e8_delete_not_in_subquery_deletes_non_matching_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("uncorrelated DELETE NOT IN must execute on the identity path");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(2, "b".to_string())],
        "NOT IN must keep only the key row — Spark {{2}}"
    );
}

/// The NULL trap: ANY NULL in the subquery ⇒ NOT IN matches zero rows.
#[tokio::test]
async fn g3e8_delete_not_in_subquery_with_null_key_deletes_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(&ctx, &catalogs, "DROP TABLE ice.sales.keys").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.keys AS \
         SELECT 2 AS id, 'K' AS name UNION ALL SELECT CAST(NULL AS INT), 'N'",
    )
    .await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("NOT IN + NULL subquery must execute (and match nothing)");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        g3e8_seed(),
        "Spark 3VL: a NULL in the subquery makes NOT IN UNKNOWN for every row"
    );
}

/// Empty subquery: `x NOT IN ()` is vacuously TRUE — delete every row.
#[tokio::test]
async fn g3e8_delete_not_in_empty_subquery_deletes_every_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(&ctx, &catalogs, "DELETE FROM ice.sales.keys").await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("NOT IN empty subquery must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        Vec::<(i32, String)>::new(),
        "empty NOT IN matches every row"
    );
}

/// Quoted target + FROM-less: same product spelling, different target forms.
#[tokio::test]
async fn g3e8_delete_not_in_quoted_and_fromless_execute() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM \"ice\".\"sales\".\"tgt\" WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("quoted-target NOT IN must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(2, "b".to_string())],
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("FROM-less NOT IN must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(2, "b".to_string())],
    );
}

/// `[NOT] EXISTS` uncorrelated (all-or-nothing) and correlated (semi/anti-join).
#[tokio::test]
async fn g3e8_delete_exists_uncorrelated_and_correlated_execute() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
    )
    .await
    .expect("uncorrelated nonempty EXISTS must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        Vec::<(i32, String)>::new(),
        "non-empty uncorrelated EXISTS deletes every row"
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys WHERE id = 999)",
    )
    .await
    .expect("uncorrelated empty-result EXISTS must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        g3e8_seed(),
        "empty-result uncorrelated EXISTS deletes nothing"
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)",
    )
    .await
    .expect("uncorrelated nonempty NOT EXISTS must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        g3e8_seed(),
        "non-empty uncorrelated NOT EXISTS deletes nothing"
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE EXISTS \
         (SELECT 1 FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
    )
    .await
    .expect("correlated EXISTS must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE NOT EXISTS \
         (SELECT 1 FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
    )
    .await
    .expect("correlated NOT EXISTS must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(2, "b".to_string())],
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
    )
    .await
    .expect("FROM-less uncorrelated EXISTS must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        Vec::<(i32, String)>::new(),
    );
}

/// Correlated `DELETE … IN` matches correlated `EXISTS`.
#[tokio::test]
async fn g3e8_delete_correlated_in_deletes_exactly_the_matching_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.tgt WHERE id IN \
         (SELECT k.id FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
    )
    .await
    .expect("correlated IN DELETE must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE ice.sales.tgt WHERE id IN \
         (SELECT k.id FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
    )
    .await
    .expect("FROM-less correlated IN must execute");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
    );
}

/// Identity `UPDATE … SET <scalar> WHERE col IN (SELECT …)`.
#[tokio::test]
async fn g3e8_update_in_subquery_rewrites_only_the_matching_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("UPDATE IN must execute on the identity path");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![
            (1, "a".to_string()),
            (2, "z".to_string()),
            (3, "c".to_string()),
        ],
        "UPDATE IN must rewrite only id=2 — pre-fix every row became 'z'"
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.tgt SET name = 'z', id = id + 10 \
         WHERE id IN (SELECT id FROM ice.sales.keys)",
    )
    .await
    .expect("multi-column UPDATE IN must execute");
    let mut rows = table_rows(&ctx, &catalogs, "ice.sales.tgt").await;
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string()),
            (3, "c".to_string()),
            (12, "z".to_string()),
        ],
    );
}

/// Every DELETE subquery spelling the recon proved silently wrong refuses, and none of them
/// touches a row. One fresh table per form, so a leaked write cannot hide behind a later one.
#[tokio::test]
async fn g3e8_delete_subquery_family_all_refuse() {
    for sql in [
        // IN / NOT IN are the product hole (executed by g3e8_delete_in_subquery_* /
        // g3e8_delete_not_in_*). Residual refuse:
        // negated, disjunctive and conjunctive positions (the AND form silently PARTIALLY
        // over-deleted — the surviving conjunct was applied alone)
        "DELETE FROM ice.sales.tgt WHERE NOT (id IN (SELECT id FROM ice.sales.keys))",
        "DELETE FROM ice.sales.tgt WHERE id = 1 OR id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id > 1 AND id IN (SELECT id FROM ice.sales.keys)",
        // EXISTS / NOT EXISTS / correlated IN now execute. Residual refuse:
        // quantified comparison (Spark 4.1.2 parse-fails ANY/ALL — permanent v1 valve)
        "DELETE FROM ice.sales.tgt WHERE id > ANY (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id > ALL (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id = ANY (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id <> ALL (SELECT id FROM ice.sales.keys)",
        // nested subquery inside the subquery's own FROM
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM (SELECT id FROM ice.sales.keys) \
         AS inner_alias)",
        // correlated AGGREGATE scalar subquery — the spelling that shares a parse tree with the
        // uncorrelated scalar form that works today, and destroys the table
        "DELETE FROM ice.sales.tgt WHERE id = (SELECT max(k.id) FROM ice.sales.keys k \
         WHERE k.name = ice.sales.tgt.name)",
        // uncorrelated scalar subquery — DELIBERATELY over-refused (see the ledger): correct
        // today, syntactically inseparable from the correlated form above
        "DELETE FROM ice.sales.tgt WHERE id = (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE (SELECT count(*) FROM ice.sales.keys) > 0",
        // the remaining ⚠️ (correct-today, over-refused) scalar comparisons — pinned so the
        // The "over-refused" list is a list of pins, not prose (L2 N8).
        "DELETE FROM ice.sales.tgt WHERE id > (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id <> (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE (SELECT count(*) FROM ice.sales.keys) > 99",
        // subquery over a TEMP VIEW — still uncorrelated IN, now executed
        // (g3e8_delete_in_subquery_from_temp_view_source_executes). Residual refuse:
        // === parser spellings covered by the matrix (L1 M-4 / F-D) =============================
        // They are NOT safe-because-uncorrelated: the boundary is per-shape, not
        // correlated-vs-uncorrelated. Pre-guard behaviour, executed under the neutered valve and
        // recorded in task/g3e8-guard-ledger.md §2: all three EMPTIED the table.
        // an AGGREGATE scalar inside IN — an uncorrelated aggregate, still refused
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT max(id) FROM ice.sales.keys)",
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = g3e8_setup(&wh).await;
        let Err(err) = execute(&ctx, &catalogs, sql).await else {
            panic!("G3-E8 must refuse: {sql}")
        };
        assert_g3e8_message(&err.to_string(), "DELETE", sql);
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
            g3e8_seed(),
            "a refused DELETE must not touch a row, sql={sql:?}"
        );
    }
}

/// Every UPDATE subquery spelling the recon proved silently wrong refuses, and none of them
/// rewrites a value. Pre-guard each of these set EVERY row to the SET value.
#[tokio::test]
async fn g3e8_update_subquery_family_all_refuse() {
    for sql in [
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE EXISTS (SELECT 1 FROM ice.sales.keys k \
         WHERE k.id = ice.sales.tgt.id)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id = (SELECT max(k.id) FROM ice.sales.keys k \
         WHERE k.name = ice.sales.tgt.name)",
        // an assignment subquery is NOT the gated thing — the WHERE subquery is
        "UPDATE ice.sales.tgt SET name = (SELECT max(name) FROM ice.sales.keys) \
         WHERE id IN (SELECT id FROM ice.sales.keys)",
        // deliberate over-refusal: the uncorrelated scalar WHERE subquery works today
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id = (SELECT max(id) FROM ice.sales.keys)",
        // F-D's spellings on the UPDATE verb too — the boundary is per-shape, not per-verb
        "UPDATE ice.sales.tgt SET name = 'z' WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT max(id) FROM ice.sales.keys)",
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = g3e8_setup(&wh).await;
        let Err(err) = execute(&ctx, &catalogs, sql).await else {
            panic!("G3-E8 must refuse: {sql}")
        };
        assert_g3e8_message(&err.to_string(), "UPDATE", sql);
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
            g3e8_seed(),
            "a refused UPDATE must not rewrite a value, sql={sql:?}"
        );
    }
}

/// The adjacent NEGATIVE half: non-subquery DELETE/UPDATE predicates still execute, and produce
/// exactly the rows they always did. A guard that refused these would be an over-refusal.
#[tokio::test]
async fn g3e8_non_subquery_dml_still_executes() {
    for (sql, expected) in [
        (
            "DELETE FROM ice.sales.tgt WHERE id = 2",
            vec![(1, "a".to_string()), (3, "c".to_string())],
        ),
        (
            "DELETE FROM ice.sales.tgt WHERE id IN (1, 2)",
            vec![(3, "c".to_string())],
        ),
        (
            "DELETE FROM ice.sales.tgt WHERE id BETWEEN 2 AND 3",
            vec![(1, "a".to_string())],
        ),
        (
            "DELETE FROM ice.sales.tgt WHERE name LIKE 'b%' OR id = 3",
            vec![(1, "a".to_string())],
        ),
        (
            "UPDATE ice.sales.tgt SET name = 'z' WHERE id = 2",
            vec![
                (1, "a".to_string()),
                (2, "z".to_string()),
                (3, "c".to_string()),
            ],
        ),
        (
            "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (1, 3)",
            vec![
                (1, "z".to_string()),
                (2, "b".to_string()),
                (3, "z".to_string()),
            ],
        ),
        // No WHERE at all — the provider's genuine match-all, which the guard must not disturb.
        ("DELETE FROM ice.sales.tgt", vec![]),
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = g3e8_setup(&wh).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
            expected,
            "non-subquery DML must still execute, sql={sql:?}"
        );
    }
}

/// The other adjacent negatives: the two verbs the recon proved UNAFFECTED still run with a
/// subquery. Guarding either would break working surface — `INSERT` hands DataFusion a whole
/// `ExecutionPlan` (no filter recovery), and MERGE is RePark-owned end to end.
#[tokio::test]
async fn g3e8_insert_and_merge_with_subqueries_still_execute() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tgt SELECT * FROM src WHERE id IN \
         (SELECT id FROM ice.sales.keys)",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ],
        "INSERT … SELECT with a subquery filter must insert exactly the matching row"
    );

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.tgt AS t USING (SELECT id, name FROM ice.sales.keys) AS s \
         ON t.id = s.id WHEN MATCHED THEN DELETE",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
        "MERGE over a subquery source must delete exactly the matched row — the workaround the \
         refusal message recommends has to actually work"
    );
}

/// An `UPDATE … SET col = (SELECT …)` with a NON-subquery WHERE — and with NO `WHERE` at all —
/// is deliberately NOT gated: an assignment subquery is either correct (this pin) or a loud plan
/// error, never silently wrong. If a future change starts gating assignments, this pin reds and
/// the decision gets re-made. Both spellings are pinned, because the matrix carries both and an
/// An unpinned matrix row is a claim, not a fact (L2 N8).
#[tokio::test]
async fn g3e8_update_set_subquery_without_where_subquery_still_executes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.tgt SET name = (SELECT max(name) FROM ice.sales.keys) WHERE id = 2",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![
            (1, "a".to_string()),
            (2, "K".to_string()),
            (3, "c".to_string()),
        ],
        "only the matching row takes the assignment"
    );

    // The same assignment with NO `WHERE`: a genuine match-all, which the valve must not gate.
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.tgt SET name = (SELECT max(name) FROM ice.sales.keys)",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![
            (1, "K".to_string()),
            (2, "K".to_string()),
            (3, "K".to_string()),
        ],
        "an assignment subquery with no WHERE rewrites every row — deliberately ungated"
    );
}

/// ===========================================================================================
/// CTE-prefixed DML (`WITH … DELETE/UPDATE`) — a KNOWN un-valved attachment that is LOUD today
/// (L1 N-1 / F-E).
///
/// sqlparser parses `WITH c AS (…) DELETE …` as a `Query` whose body is `SetExpr::Delete`, so it
/// never reaches the router's `Statement::Delete` arm NOR the passthrough's — the valve does not
/// see it. It is nonetheless safe today because DataFusion refuses to plan that shape at all
/// (`NotImplemented: Query DELETE … not implemented yet`), which is loud and writes nothing.
///
/// This pin exists so the gap cannot reopen SILENTLY: the day DataFusion learns to plan
/// `SetExpr::Delete`, this test reds, and whoever bumps DataFusion has to route the form through
/// the valve in the same change instead of shipping a second silent-data-loss window. The
/// non-subquery spelling is included so the pin reds on the DataFusion change rather than on any
/// change to the valve.
/// ===========================================================================================
#[tokio::test]
async fn g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing() {
    for sql in [
        "WITH c AS (SELECT id FROM ice.sales.keys) \
         DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM c)",
        "WITH c AS (SELECT id FROM ice.sales.keys) \
         UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM c)",
        // subquery-free: this one reds only if DataFusion starts planning the shape.
        "WITH c AS (SELECT id FROM ice.sales.keys) DELETE FROM ice.sales.tgt WHERE id = 2",
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = g3e8_setup(&wh).await;
        let outcome = execute(&ctx, &catalogs, sql).await.map(|_| ());
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
            g3e8_seed(),
            "CTE-prefixed DML must write nothing, sql={sql:?}, outcome={outcome:?}"
        );
        let Err(err) = outcome else {
            panic!(
                "CTE-prefixed DML now PLANS ({sql}) — route it through the G3-E8 valve before \
                 relaxing this pin: the valve is attached to `Statement::Delete`/`::Update`, and \
                 this shape is a `Query` with a `SetExpr::Delete` body"
            )
        };
        let text = err.to_string();
        assert!(
            text.contains("not implemented yet"),
            "today's behaviour is a LOUD NotImplemented, sql={sql:?}, got {text}"
        );
    }
}

/// Guard ORDER: on a table that trips BOTH data-loss valves, the cheap sync G3-E8 valve fires
/// first and the async BUG-001 metadata load never happens. Without the ordering pin a later
/// refactor could silently swap them and change which message a user sees.
#[tokio::test]
async fn g3e8_subquery_valve_precedes_the_mor_multi_spec_valve() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.both (id INT, category STRING) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.both VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.both ADD PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.both DROP PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bothkeys AS SELECT 2 AS id, 'b' AS category",
    )
    .await;

    // Non-subquery DELETE on this table still hits the BUG-001 valve (the control that proves
    // the hazard is real and the two messages are distinguishable).
    let mor = execute(&ctx, &catalogs, "DELETE FROM ice.sales.both WHERE id = 1")
        .await
        .expect_err("BUG-001 must still refuse the non-subquery DELETE")
        .to_string();
    assert!(
        mor.contains("merge-on-read"),
        "control must be the BUG-001 message, got {mor}"
    );

    let sql = "DELETE FROM ice.sales.both WHERE id IN \
               (SELECT max(id) FROM ice.sales.bothkeys)";
    let both = execute(&ctx, &catalogs, sql)
        .await
        .expect_err("a still-refused subquery DELETE on a BUG-001 table must still refuse")
        .to_string();
    assert_g3e8_message(&both, "DELETE", sql);
    assert!(
        !both.contains("merge-on-read"),
        "the G3-E8 valve must fire FIRST (cheap, sync) — got the BUG-001 message: {both}"
    );
}

/// ===========================================================================================
/// The FROM-less `DELETE <table> WHERE …` family exercises the executing-parse valve (L1 M-1).
///
/// The session parser accepts FROM-less DELETE while the router parser rejects it. The G3-E8
/// valve therefore runs on the executing parse so the subquery predicate cannot fail open.
/// ===========================================================================================
#[tokio::test]
async fn g3e8_fromless_delete_in_subquery_deletes_exactly_the_matching_row() {
    for sql in [
        "DELETE ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "delete ice.sales.tgt where id in (select id from ice.sales.keys)",
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = g3e8_setup(&wh).await;
        execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|error| panic!("FROM-less IN must execute, sql={sql:?}: {error}"));
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
            vec![(1, "a".to_string()), (3, "c".to_string())],
            "FROM-less IN must delete only id=2, sql={sql:?}"
        );
    }
}

/// Residual FROM-less subquery spellings stay refused at the executing parse (F-A).
#[tokio::test]
async fn g3e8_fromless_delete_subquery_family_all_refuse() {
    for sql in [
        // F9 — FROM-less residual (IN / NOT IN / [NOT] EXISTS / correlated IN now execute)
        "DELETE ice.sales.tgt WHERE id IN (SELECT max(id) FROM ice.sales.keys)",
        "DELETE ice.sales.tgt WHERE id = ANY (SELECT id FROM ice.sales.keys)",
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = g3e8_setup(&wh).await;
        let outcome = execute(&ctx, &catalogs, sql).await.map(|_| ());
        assert_eq!(
            table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
            g3e8_seed(),
            "a FROM-less subquery DELETE must not touch a row, sql={sql:?}, outcome={outcome:?}"
        );
        let Err(err) = outcome else {
            panic!("G3-E8 must refuse the FROM-less form: {sql}")
        };
        assert_g3e8_message(&err.to_string(), "DELETE", sql);
    }
}

/// The adjacent negative for the FROM-less family (F10): a FROM-less DELETE with a plain
/// predicate is not a subquery statement and must keep executing exactly as it did — the valve's
/// new attachment point must not turn the passthrough into an over-refusal.
#[tokio::test]
async fn g3e8_fromless_non_subquery_delete_still_executes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = g3e8_setup(&wh).await;
    run(&ctx, &catalogs, "DELETE ice.sales.tgt WHERE id = 2").await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.tgt").await,
        vec![(1, "a".to_string()), (3, "c".to_string())],
        "a FROM-less DELETE with a plain predicate must still delete exactly the matched row"
    );
}
