/// Unknown or deferred procedures fail loudly and list the supported set.
use super::super::*;
use super::common::*;

/// MW-1: Spark 4.0.1 plus Iceberg 1.10.0 defines the six-column `expire_snapshots` schema.
fn assert_expire_schema_is_sparks(batch: &datafusion::arrow::array::RecordBatch) {
    assert_eq!(batch.num_columns(), 6, "expire result schema is Spark's");
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "deleted_data_files_count",
            "deleted_position_delete_files_count",
            "deleted_equality_delete_files_count",
            "deleted_manifest_files_count",
            "deleted_manifest_lists_count",
            "deleted_statistics_files_count",
        ]
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable()),
        "Spark declares all six expire columns nullable"
    );
}

#[tokio::test]
async fn call_unknown_procedure_refuses_loud_listing_supported() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.not_a_real_proc(table => 'sales.t')",
    )
    .await
    .expect_err("unknown CALL must fail loud");
    let message = error.to_string();
    assert!(
        message.contains("not supported") || message.contains("not_a_real_proc"),
        "error must name the unknown proc, got: {message}"
    );
    assert!(
        message.contains("expire_snapshots")
            && message.contains("rewrite_data_files")
            && message.contains("rollback_to_snapshot"),
        "error must list supported procedures, got: {message}"
    );
}

/// I3: `rollback_to_snapshot` restores the prior multiset; result columns match Spark.
#[tokio::test]
async fn call_rollback_to_snapshot_restores_multiset() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.roll AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "roll".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.roll SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 head before rollback");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.roll").await,
        4
    );

    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.roll', snapshot_id => {s1})"
        ),
    )
    .await
    .expect("rollback CALL");
    let batches = result.collect().await.expect("collect rollback result");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["previous_snapshot_id", "current_snapshot_id"]
    );
    let previous = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("previous_snapshot_id i64")
        .value(0);
    let current = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("current_snapshot_id i64")
        .value(0);
    // C1-Q-003: both result columns are load-bearing (not only current).
    assert_eq!(
        previous, s2,
        "previous_snapshot_id must be pre-rollback head"
    );
    assert_eq!(current, s1);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.roll").await,
        3,
        "after rollback read must equal s1 multiset (3 rows)"
    );
}

/// I3 load-bearing safety: expire keeps tag/branch-reachable snapshots (R133).
#[tokio::test]
async fn call_expire_snapshots_keeps_tag_reachable() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.exp AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "exp".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.exp SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 intermediate");
    assert_ne!(
        s1, s2,
        "fixture must produce distinct intermediate snapshot"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.exp SELECT 5 AS id, 'e' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s3 = table.metadata().current_snapshot_id().expect("s3 head");
    let snap_count_before = table.metadata().snapshots().count();
    assert!(snap_count_before >= 3);
    assert_ne!(s2, s3);

    // Tag at s1 — expire must not remove a ref-reachable snapshot (R133).
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().create_tag("keep_s1", s1);
    let tx = action.apply(tx).expect("apply tag");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit tag");

    // older_than = far future so age would expire every snapshot.
    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.exp', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire CALL");
    let batches = result.collect().await.expect("collect expire result");
    assert_expire_schema_is_sparks(&batches[0]);

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(
        table.metadata().snapshot_by_id(s1).is_some(),
        "tag-reachable snapshot s1 must survive expire (R133 safety pin)"
    );
    assert!(
        table.metadata().snapshot_for_ref("keep_s1").is_some(),
        "tag keep_s1 must still resolve"
    );
    assert!(
        table.metadata().snapshot_by_id(s2).is_none(),
        "untagged intermediate s2 must be expired — proves expire ran (C1-Q-001); \
             no-op CALL would leave s2 and still keep s1"
    );
    assert!(
        table.metadata().snapshot_by_id(s3).is_some(),
        "main head s3 retained by retain_last=1"
    );
    // Current read still works.
    assert!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.exp").await >= 3);
}

/// C2-Q-001: branch-reachable snapshot survives expire (not only tags).
#[tokio::test]
async fn call_expire_snapshots_keeps_branch_reachable() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.expb AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "expb".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expb SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 intermediate");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expb SELECT 5 AS id, 'e' AS name",
    )
    .await;

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().create_branch("audit", s1);
    let tx = action.apply(tx).expect("apply branch");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit branch");

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.expb', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire CALL");

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(
        table.metadata().snapshot_by_id(s1).is_some(),
        "branch-reachable s1 must survive expire"
    );
    assert!(
        table.metadata().snapshot_for_ref("audit").is_some(),
        "branch audit must still resolve"
    );
    assert!(
        table.metadata().snapshot_by_id(s2).is_none(),
        "untagged intermediate s2 must expire (dual probe)"
    );
}

/// I3: `rewrite_data_files` preserves row multiset and reduces file count on multi-small files.
#[tokio::test]
async fn call_rewrite_data_files_preserves_rows_and_reduces_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Seed ≥5 tiny files (default min_input_files=5) so bin-pack qualifies.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rw AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    for index in 2..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.rw SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
    let before_ids =
        time_travel_id_multiset(&ctx, &catalogs, "SELECT CAST(id AS INT) FROM ice.sales.rw").await;
    assert_eq!(before_ids, vec![1, 2, 3, 4, 5, 6]);

    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "rw".into());
    let files_before_count = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert!(
        files_before_count >= 5,
        "fixture must have ≥5 small files, got {files_before_count}"
    );

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rw')",
    )
    .await
    .expect("rewrite CALL");
    let batches = result.collect().await.expect("collect rewrite result");
    let batch = &batches[0];
    let rewritten = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap()
        .value(0);
    assert!(
        rewritten >= 2,
        "expected some files rewritten, got {rewritten}"
    );

    let after_ids =
        time_travel_id_multiset(&ctx, &catalogs, "SELECT CAST(id AS INT) FROM ice.sales.rw").await;
    assert_eq!(
        after_ids, before_ids,
        "rewrite must preserve row multiset byte-exactly (ids)"
    );

    let files_after_count = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    assert!(
        files_after_count < files_before_count,
        "rewrite must reduce file count ({files_after_count} < {files_before_count})"
    );
}

/// rewrite `strategy` / `sort_order` other than binpack → loud unsupported (R135 deferred).
#[tokio::test]
async fn call_rewrite_sort_strategy_refuses_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.t', strategy => 'sort')",
    )
    .await
    .expect_err("sort strategy must refuse");
    let message = error.to_string();
    assert!(
        message.contains("sort") && message.contains("not supported"),
        "got: {message}"
    );
    assert!(
        message.contains("R135") || message.contains("binpack") || message.contains("zOrder"),
        "must name R135 deferred list, got: {message}"
    );

    // C1-L-001: positional strategy must refuse the same way — never silent binpack.
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.t', 'sort')",
    )
    .await
    .expect_err("positional sort strategy must refuse (not binpack)");
    let message = error.to_string();
    assert!(
        message.contains("sort") && message.contains("not supported"),
        "positional sort must refuse loud, got: {message}"
    );

    // C2-Q-003: positional binpack is accepted (not a blanket positional refuse).
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.t', 'binpack')",
    )
    .await
    .expect("positional binpack must be accepted");

    // C2-Q-002: third positional exceeds supported arity (not silent ignore).
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.t', 'binpack', 'id ASC')",
    )
    .await
    .expect_err("third positional must refuse");
    let message = error.to_string();
    assert!(
        message.contains("at most") || message.contains("positional"),
        "excess positional must name arity, got: {message}"
    );
}

/// C4-Q-001: expire `older_than` accepts TIMESTAMP string form (Spark docs example shape).
#[tokio::test]
async fn call_expire_older_than_timestamp_string() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.texp AS SELECT * FROM src",
    )
    .await;
    // Far-future timestamp string — age would expire everything; retain_last=1 keeps head.
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.expire_snapshots(\
             table => 'sales.texp', older_than => '2099-01-01 00:00:00', retain_last => 1)",
    )
    .await
    .expect("TIMESTAMP string older_than must parse and run");
    assert!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.texp").await >= 1);
}

/// C4-Q-002: three-part table identity on CALL (`catalog.ns.table`) resolves.
#[tokio::test]
async fn call_table_three_part_ident() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t3 AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "t3".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t3 SELECT 4 AS id, 'd' AS name",
    )
    .await;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(\
                 table => 'ice.sales.t3', snapshot_id => {s1})"
        ),
    )
    .await
    .expect("three-part table ident on CALL");
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t3").await, 3);
}

/// C3-Q-001: `retain_last` must be `>= 1` or CALL fails at plan time.
#[tokio::test]
async fn call_expire_retain_last_zero_refuses_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.expire_snapshots(table => 'sales.t', retain_last => 0)",
    )
    .await
    .expect_err("retain_last=0 must refuse");
    let message = error.to_string();
    assert!(
        message.contains("retain_last") && (message.contains(">= 1") || message.contains('1')),
        "got: {message}"
    );
}

/// C3-Q-002: mixing named and positional CALL args refuses (Spark procedures).
#[tokio::test]
async fn call_mixed_named_and_positional_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rollback_to_snapshot('sales.t', snapshot_id => 1)",
    )
    .await
    .expect_err("mixed args must refuse");
    let message = error.to_string();
    assert!(
        message.contains("mixing") || message.contains("named and positional"),
        "got: {message}"
    );
}

/// C3-Q-003: expire accepts full positional form (`table`, `older_than`, `retain_last`).
#[tokio::test]
async fn call_expire_positional_args() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.expp AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "expp".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expp SELECT 4 AS id, 'd' AS name",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.expp SELECT 5 AS id, 'e' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.manage_snapshots().create_tag("keep", s1);
    let tx = action.apply(tx).expect("tag");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit tag");

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.expire_snapshots('sales.expp', {older_than_ms}, 1)"),
    )
    .await
    .expect("positional expire");
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(table.metadata().snapshot_by_id(s1).is_some());
}

/// C2-Q-004: rollback to a non-ancestor snapshot fails loud (fork R98).
#[tokio::test]
async fn call_rollback_non_ancestor_refuses_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.roll2 AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "roll2".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table.metadata().current_snapshot_id().expect("s1");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.roll2 SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table.metadata().current_snapshot_id().expect("s2");

    // Roll main to s1 — s2 remains in history as a *descendant*, not an ancestor.
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.roll2', snapshot_id => {s1})"
        ),
    )
    .await
    .expect("rollback to s1");

    let error = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.roll2', snapshot_id => {s2})"
        ),
    )
    .await
    .expect_err("non-ancestor s2 must refuse");
    let message = error.to_string();
    assert!(
        !message.is_empty(),
        "must fail loud on non-ancestor snapshot_id, got empty"
    );
    // Table still at s1 multiset (failed CALL must not move main).
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.roll2").await,
        3
    );
}

/// First-row Int64 cell of a named expire-result column.
fn expire_result_i64(batches: &[RecordBatch], name: &str) -> i64 {
    let index = batches[0].schema().index_of(name).expect("column present");
    batches[0]
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 count column")
        .value(0)
}

/// pins: rp-1-fork-repin/C-009
/// The expire result splits content files into Spark's typed columns.
#[tokio::test]
async fn call_expire_splits_content_files_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mor (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    for id in 1..=3 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.mor VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "mor".into());
    let pre_merge = catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("pre-merge snapshot");

    // Two merge-on-read MERGEs, each rewriting the same row: position deletes accumulate.
    for value in ["x", "y"] {
        run(
            &ctx,
            &catalogs,
            &format!(
                "MERGE INTO ice.sales.mor AS t USING (SELECT 1 AS id, '{value}' AS v) AS s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v"
            ),
        )
        .await;
    }
    // Append-only files after the MERGEs so expired *data* count ≠ expired *position-delete* count.
    for id in 10..=11 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.mor VALUES ({id}, 'extra{id}')"),
        )
        .await;
    }
    let delete_files = i64::try_from(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.mor.files WHERE content != 0",
        )
        .await,
    )
    .expect("delete-file count fits i64");
    assert!(
        delete_files > 0,
        "the MERGEs must actually write position deletes, else the split below proves nothing"
    );

    // Roll back past both MERGEs: their delete files are now unreachable from any live snapshot.
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.mor', snapshot_id => {pre_merge})"
        ),
    )
    .await
    .expect("rollback CALL");

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.mor', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire CALL");
    let batches = result.collect().await.expect("collect expire result");
    let data = expire_result_i64(&batches, "deleted_data_files_count");
    let position = expire_result_i64(&batches, "deleted_position_delete_files_count");
    let equality = expire_result_i64(&batches, "deleted_equality_delete_files_count");

    assert_eq!(
        position, delete_files,
        "every stranded position delete must be reported under Spark's position-delete column, \
         not funnelled into the data-file count"
    );
    assert_eq!(
        equality, 0,
        "nothing here writes equality deletes — a measured control, not a placeholder"
    );
    // pins: rp-1-fork-repin/C-009
    // Two MERGEs strand two data files plus two post-MERGE appends (no extra deletes).
    assert_eq!(
        data, 4,
        "two MERGE data files + two post-MERGE appends; got {data}"
    );
    assert_ne!(
        data, position,
        "data and position columns must disagree so a view swap cannot stay green"
    );
}

/// Maintenance procedures execute under both remote catalog policies.
#[tokio::test]
async fn call_runs_against_both_remote_catalog_policies() {
    for policy in [
        LocationPolicy::RequireExplicitLocation,
        LocationPolicy::ServiceManagedLocation,
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        run(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.fence (id INT) USING iceberg",
        )
        .await;
        run(&ctx, &catalogs, "INSERT INTO ice.sales.fence VALUES (1)").await;

        let mut remote = CatalogRegistry::new();
        remote.insert(
            "ice".to_string(),
            Arc::clone(&catalogs["ice"]),
            policy.clone(),
        );

        for procedure in [
            "expire_snapshots(table => 'sales.fence')",
            "rewrite_data_files(table => 'sales.fence')",
        ] {
            execute(&ctx, &remote, &format!("CALL ice.system.{procedure}"))
                .await
                .unwrap_or_else(|error| {
                    panic!("{policy:?} must execute {procedure} after MW-1, got: {error}")
                });
        }
    }
}

/// MW-1 refusal preservation: an unknown catalog still refuses, on every policy.
#[tokio::test]
async fn call_still_refuses_an_unknown_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL nosuchcatalog.system.expire_snapshots(table => 'sales.t')",
    )
    .await
    .expect_err("unknown catalog must refuse");
    let message = error.to_string();
    assert!(
        message.contains("nosuchcatalog"),
        "refusal must name the unknown catalog, got: {message}"
    );
}

/// MW-2: Spark's four-column `rewrite_position_delete_files` schema, types, and nullability.
fn assert_rpdf_schema_is_sparks(batch: &datafusion::arrow::array::RecordBatch) {
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "rewritten_delete_files_count",
            "added_delete_files_count",
            "rewritten_bytes_count",
            "added_bytes_count",
        ]
    );
    let types: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    assert_eq!(
        types,
        vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Int64,
            DataType::Int64,
        ],
        "Spark: two ints then two bigints"
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable()),
        "Spark declares all four rewrite_position_delete_files columns NON-nullable, unlike \
         expire_snapshots' six"
    );
}

/// Read an `Int32` or `Int64` result column as `i64`.
pub(super) fn call_count(batch: &datafusion::arrow::array::RecordBatch, name: &str) -> i64 {
    let index = batch.schema().index_of(name).expect("column present");
    let column = batch.column(index);
    column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .map_or_else(
            || {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int32 or Int64 count column")
                    .value(0)
            },
            |array| i64::from(array.value(0)),
        )
}

/// Build a merge-on-read table.
async fn seed_mor_delete_files(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    data_files: i32,
    merges: i32,
) -> usize {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, v STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', \
             'write.delete.granularity' = 'partition')"
        ),
    )
    .await;
    for id in 1..=data_files {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    for id in 1..=merges {
        run(
            ctx,
            catalogs,
            &format!(
                "MERGE INTO ice.sales.{table} AS t USING (SELECT {id} AS id, 'm{id}' AS v) AS s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v"
            ),
        )
        .await;
    }
    rows(
        ctx,
        catalogs,
        &format!("SELECT * FROM ice.sales.{table}.files WHERE content = 1"),
    )
    .await
}

/// pins: rp-1-fork-repin/C-008
/// MW-2: `rewrite_position_delete_files` compacts position deletes and reports Spark's counts.
#[tokio::test]
async fn call_rewrite_position_delete_files_compacts_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let before = seed_mor_delete_files(&ctx, &catalogs, "mor", 8, 8).await;
    assert_eq!(
        before, 8,
        "fixture must strand 8 position-delete files, else the compaction below proves nothing"
    );
    let live_before = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.mor").await;

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.mor')",
    )
    .await
    .expect("rewrite_position_delete_files CALL");
    let batches = result.collect().await.expect("collect rpdf result");
    let batch = &batches[0];
    assert_rpdf_schema_is_sparks(batch);

    assert_eq!(
        call_count(batch, "rewritten_delete_files_count"),
        8,
        "Spark rewrote all 8; so must this engine"
    );
    assert_eq!(
        call_count(batch, "added_delete_files_count"),
        1,
        "one compacted file per (spec, partition) group — Spark's partition-granularity answer"
    );
    let rewritten_bytes = call_count(batch, "rewritten_bytes_count");
    let added_bytes = call_count(batch, "added_bytes_count");
    assert!(
        rewritten_bytes > added_bytes && added_bytes > 0,
        "compaction must shrink the delete-file footprint and still write something: \
         rewritten={rewritten_bytes} added={added_bytes}"
    );

    let after = rows(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.mor.files WHERE content = 1",
    )
    .await;
    assert_eq!(after, 1, "8 live position-delete files became 1");
    // The correctness half.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.mor").await,
        live_before,
        "compaction must not change the live row set"
    );
}

/// MW-2: nothing to compact is a zero result, not an error.
#[tokio::test]
async fn call_rewrite_position_delete_files_is_a_zero_result_when_there_is_nothing_to_do() {
    for (table, merges) in [("clean", 0), ("single", 1)] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        let before = seed_mor_delete_files(&ctx, &catalogs, table, 8, merges).await;
        assert_eq!(before, usize::try_from(merges).expect("small"));

        let result = execute(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.rewrite_position_delete_files(table => 'sales.{table}')"),
        )
        .await
        .expect("rewrite_position_delete_files CALL");
        let batches = result.collect().await.expect("collect rpdf result");
        let batch = &batches[0];
        assert_rpdf_schema_is_sparks(batch);
        for column in [
            "rewritten_delete_files_count",
            "added_delete_files_count",
            "rewritten_bytes_count",
            "added_bytes_count",
        ] {
            assert_eq!(
                call_count(batch, column),
                0,
                "{table}: {column} must be zero when there is nothing to compact"
            );
        }
        assert_eq!(
            rows(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{table}.files WHERE content = 1")
            )
            .await,
            before,
            "{table}: a zero result must leave the delete files alone"
        );
    }
}

/// pins: rp-1-fork-repin/C-007
/// The position-delete planner follows Spark's `min-input-files = 5` floor.
#[tokio::test]
async fn call_mor1_compacts_below_sparks_min_input_files_floor() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let before = seed_mor_delete_files(&ctx, &catalogs, "mor", 8, 4).await;
    assert_eq!(
        before, 4,
        "four delete files — one below Spark's floor of 5"
    );
    let live_before = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.mor").await;

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.mor')",
    )
    .await
    .expect("rewrite_position_delete_files CALL");
    let batches = result.collect().await.expect("collect rpdf result");
    let batch = &batches[0];

    // Equality with Spark: a 4-file group is below the floor, so both return zeros.
    assert_eq!(
        call_count(batch, "rewritten_delete_files_count"),
        0,
        "RP-1: four files is below min-input-files = 5; Spark and this engine both decline"
    );
    assert_eq!(call_count(batch, "added_delete_files_count"), 0);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.mor").await,
        live_before,
        "declining to compact must not change the live row set"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.mor.files WHERE content = 1"
        )
        .await,
        4,
        "the four delete files stay; the planner did not rewrite them"
    );
}

/// pins: rp-1-fork-repin/C-007, C-008
/// Exact Spark floor: five files is the smallest group that compact.
#[tokio::test]
async fn call_rpdf_compacts_at_sparks_min_input_files_floor() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let before = seed_mor_delete_files(&ctx, &catalogs, "floor5", 8, 5).await;
    assert_eq!(
        before, 5,
        "five delete files — Spark's min-input-files floor"
    );
    let live_before = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.floor5").await;

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.floor5')",
    )
    .await
    .expect("rewrite_position_delete_files CALL");
    let batches = result.collect().await.expect("collect rpdf result");
    let batch = &batches[0];
    assert_eq!(call_count(batch, "rewritten_delete_files_count"), 5);
    assert_eq!(call_count(batch, "added_delete_files_count"), 1);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.floor5").await,
        live_before,
        "compact must not change the live row set"
    );
}

/// pins: mw-9-delete-granularity/C-001, C-006, C-009
/// Spark-default `write.delete.granularity = 'file'`.
#[tokio::test]
async fn call_mor2_merge_writes_one_position_delete_per_data_file_by_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.gran (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    for id in 1..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.gran VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.gran.files WHERE content = 0"
        )
        .await,
        6,
        "six distinct data files, so Spark's file granularity would write six delete files"
    );

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.gran AS t USING (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL \
         SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) AS s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = 'merged'",
    )
    .await;

    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.gran.files WHERE content = 1"
        )
        .await,
        6,
        "MOR-2 closed: Spark default file granularity writes one delete file per data file"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.gran").await,
        6,
        "the granularity divergence changes file layout, not the live row set"
    );
}

/// MW-2 keeps the austerity `rewrite_data_files` already has.
#[tokio::test]
async fn call_rewrite_position_delete_files_refuses_options_and_where() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_delete_files(&ctx, &catalogs, "mor", 3, 2).await;

    for (argument, needle) in [
        ("options => map('a', 'b')", "options map is not supported"),
        ("where => 'id = 1'", "where filter is not supported"),
    ] {
        let err = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_position_delete_files(table => 'sales.mor', {argument})"
            ),
        )
        .await
        .expect_err("deferred argument must refuse");
        let message = err.to_string();
        assert!(
            message.contains(needle),
            "refusal must name the deferred argument, got: {message}"
        );
    }
}

/// `rewrite_data_files` returns Spark's five-column schema, including `removed_delete_files_count`.
#[tokio::test]
async fn call_rewrite_data_files_returns_sparks_five_columns() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rw5 AS SELECT 1 AS id, 'a' AS name",
    )
    .await;
    for index in 2..=6 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.rw5 SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rw5')",
    )
    .await
    .expect("rewrite CALL");
    let batches = result.collect().await.expect("collect rewrite result");
    let batch = &batches[0];

    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "rewritten_data_files_count",
            "added_data_files_count",
            "rewritten_bytes_count",
            "failed_data_files_count",
            "removed_delete_files_count",
        ]
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable()),
        "Spark declares all five rewrite_data_files columns non-nullable"
    );
    assert!(
        call_count(batch, "rewritten_data_files_count") >= 2,
        "the fixture must actually compact, else the columns beside it prove nothing"
    );
    assert_eq!(
        call_count(batch, "removed_delete_files_count"),
        0,
        "no dangling delete removal runs on this path, and Spark's default reports 0 too"
    );
}

/// MW-2 guard: it does not fire on the format-v2 tables this engine writes.
#[tokio::test]
async fn call_rewrite_position_delete_files_guard_passes_a_v2_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let before = seed_mor_delete_files(&ctx, &catalogs, "mor", 8, 8).await;
    assert_eq!(before, 8, "eight Parquet position deletes, no vectors");

    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "mor".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert_eq!(
        live_deletion_vector_count(&table).await,
        0,
        "a v2 merge-on-read table this engine wrote holds no deletion vectors"
    );

    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.mor')",
    )
    .await
    .expect("the guard must not refuse a table it can actually compact");
}

/// MW-2 guard: a table with NO current snapshot is not a vector table.
#[tokio::test]
async fn call_deletion_vector_guard_handles_a_table_with_no_snapshot() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.empty (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "empty".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert!(
        table.metadata().current_snapshot().is_none(),
        "fixture must have no snapshot, else it does not exercise the early return"
    );
    assert_eq!(live_deletion_vector_count(&table).await, 0);

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.empty')",
    )
    .await
    .expect("an empty table compacts to four zeros, it does not refuse");
    let batches = result.collect().await.expect("collect rpdf result");
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 0);
}
