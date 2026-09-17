use super::super::*;
use super::common::*;
use datafusion::arrow::array::{Array, AsArray};

async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    std::thread::sleep(std::time::Duration::from_millis(25));
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z' AS name",
    )
    .await;
    std::thread::sleep(std::time::Duration::from_millis(25));
    run(ctx, catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH feat").await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t.branch_feat SELECT 8 AS id, 'y' AS name",
    )
    .await;
    std::thread::sleep(std::time::Duration::from_millis(25));
}

async fn branch_head(ctx: &SessionContext, catalogs: &CatalogRegistry, name: &str) -> i64 {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT snapshot_id FROM ice.sales.t.refs WHERE name = '{name}'"),
    )
    .await
    .expect("refs read")
    .collect()
    .await
    .unwrap();
    batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

async fn snapshot_count(ctx: &SessionContext, catalogs: &CatalogRegistry) -> usize {
    rows(
        ctx,
        catalogs,
        "SELECT snapshot_id FROM ice.sales.t.snapshots",
    )
    .await
}

fn call_row_text(batch: &RecordBatch, row: usize) -> Vec<String> {
    batch
        .columns()
        .iter()
        .map(|column| match column.data_type() {
            DataType::Utf8 => {
                let texts = column.as_string::<i32>();
                if texts.is_null(row) {
                    "NULL".to_string()
                } else {
                    texts.value(row).to_string()
                }
            }
            DataType::Int64 => {
                let ints = column.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                if ints.is_null(row) {
                    "NULL".to_string()
                } else {
                    ints.value(row).to_string()
                }
            }
            other => panic!("unexpected CALL output type {other:?}"),
        })
        .collect()
}

async fn call_text(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Vec<String>, Vec<String>) {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("CALL {sql:?} failed: {error}"))
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1, "one output batch for {sql:?}");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "one output row for {sql:?}");
    let schema = batch
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
        .collect();
    (schema, call_row_text(batch, 0))
}

#[tokio::test]
async fn fast_forward_moves_branch_and_reports_refs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    let before = snapshot_count(&ctx, &catalogs).await;
    let main_before = branch_head(&ctx, &catalogs, "main").await;
    let feat = branch_head(&ctx, &catalogs, "feat").await;
    assert_ne!(main_before, feat);
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'main', 'feat')",
    )
    .await;
    assert_eq!(
        schema,
        vec![
            "branch_updated:Utf8",
            "previous_ref:Int64",
            "updated_ref:Int64"
        ]
    );
    assert_eq!(
        row,
        vec![
            "main".to_string(),
            main_before.to_string(),
            feat.to_string()
        ]
    );
    assert_eq!(branch_head(&ctx, &catalogs, "main").await, feat);
    assert_eq!(snapshot_count(&ctx, &catalogs).await, before);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t ORDER BY id").await,
        vec![1, 2, 3, 8, 9]
    );
}

#[tokio::test]
async fn fast_forward_named_and_to_tag_and_new_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG v1").await;
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward(table => 'sales.t', branch => 'main', to => 'feat')",
    )
    .await;
    assert_eq!(schema[0], "branch_updated:Utf8");
    assert_eq!(row[0], "main");
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'back', 'v1')",
    )
    .await;
    assert_eq!(
        schema,
        vec![
            "branch_updated:Utf8",
            "previous_ref:Int64",
            "updated_ref:Int64"
        ]
    );
    assert_eq!(row[0], "back");
    assert_eq!(row[1], "NULL");
    let v1 = branch_head(&ctx, &catalogs, "v1").await;
    assert_eq!(row[2], v1.to_string());
    assert_eq!(branch_head(&ctx, &catalogs, "back").await, v1);
}

#[tokio::test]
async fn fast_forward_refusals_name_the_ref() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG v1").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH old").await;
    for (sql, needle) in [
        (
            "CALL ice.system.fast_forward('sales.t', 'main', 'nope')",
            "Ref does not exist: nope",
        ),
        (
            "CALL ice.system.fast_forward('sales.t', 'v1', 'feat')",
            "Ref v1 is a tag not a branch",
        ),
        (
            "CALL ice.system.fast_forward('sales.t', 'feat', 'old')",
            "Cannot fast-forward: feat is not an ancestor of old",
        ),
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("must refuse");
        assert!(
            error.to_string().contains(needle),
            "for {sql:?} want {needle:?}, got {error}"
        );
    }
}

#[tokio::test]
async fn cherrypick_replays_branch_snapshot_onto_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 10 AS id, 'w' AS name",
    )
    .await;
    let before = snapshot_count(&ctx, &catalogs).await;
    let staged = branch_head(&ctx, &catalogs, "feat").await;
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.cherrypick_snapshot('sales.t', {staged})"),
    )
    .await;
    assert_eq!(
        schema,
        vec!["source_snapshot_id:Int64", "current_snapshot_id:Int64"]
    );
    assert_eq!(row[0], staged.to_string());
    let current: i64 = row[1].parse().unwrap();
    assert_ne!(current, staged);
    assert_eq!(snapshot_count(&ctx, &catalogs).await, before + 1);
    assert_eq!(branch_head(&ctx, &catalogs, "main").await, current);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t ORDER BY id").await,
        vec![1, 2, 3, 8, 9, 10]
    );
    let error = execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.cherrypick_snapshot('sales.t', {staged})"),
    )
    .await
    .expect_err("duplicate pick must refuse");
    assert!(
        error
            .to_string()
            .contains("already picked to create ancestor"),
        "duplicate pick refusal, got {error}"
    );
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.cherrypick_snapshot('sales.t', 123456789)",
    )
    .await
    .expect_err("unknown snapshot must refuse");
    assert!(
        error
            .to_string()
            .contains("Cannot cherry-pick unknown snapshot ID: 123456789"),
        "unknown snapshot refusal, got {error}"
    );
}

#[tokio::test]
async fn cherrypick_ancestor_and_delete_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    let main = branch_head(&ctx, &catalogs, "main").await;
    let error = execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.cherrypick_snapshot('sales.t', {main})"),
    )
    .await
    .expect_err("ancestor pick must refuse");
    assert!(
        error.to_string().contains("already an ancestor"),
        "ancestor refusal, got {error}"
    );
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH del").await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.t.branch_del WHERE id = 1",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 10 AS id, 'w' AS name",
    )
    .await;
    let staged = branch_head(&ctx, &catalogs, "del").await;
    let error = execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.cherrypick_snapshot('sales.t', {staged})"),
    )
    .await
    .expect_err("delete pick must refuse");
    assert!(
        error
            .to_string()
            .contains("not append, dynamic overwrite, or fast-forward"),
        "delete refusal, got {error}"
    );
}

#[tokio::test]
async fn set_current_snapshot_by_id_and_ref() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    let before = snapshot_count(&ctx, &catalogs).await;
    let main_before = branch_head(&ctx, &catalogs, "main").await;
    let feat = branch_head(&ctx, &catalogs, "feat").await;
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.set_current_snapshot('sales.t', {feat})"),
    )
    .await;
    assert_eq!(
        schema,
        vec!["previous_snapshot_id:Int64", "current_snapshot_id:Int64"]
    );
    assert_eq!(row, vec![main_before.to_string(), feat.to_string()]);
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        "CALL ice.system.set_current_snapshot(table => 'sales.t', ref => 'feat')",
    )
    .await;
    assert_eq!(schema[0], "previous_snapshot_id:Int64");
    assert_eq!(row[1], feat.to_string());
    assert_eq!(snapshot_count(&ctx, &catalogs).await, before);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t ORDER BY id").await,
        vec![1, 2, 3, 8, 9]
    );
}

#[tokio::test]
async fn set_current_snapshot_refusals() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    let feat = branch_head(&ctx, &catalogs, "feat").await;
    for (sql, needle) in [
        (
            format!("CALL ice.system.set_current_snapshot('sales.t', {feat}, 'feat')"),
            "Either snapshot_id or ref must be provided, not both",
        ),
        (
            "CALL ice.system.set_current_snapshot('sales.t')".to_string(),
            "Either snapshot_id or ref must be provided, not both",
        ),
        (
            "CALL ice.system.set_current_snapshot('sales.t', 123456789)".to_string(),
            "Cannot roll back to unknown snapshot id: 123456789",
        ),
        (
            "CALL ice.system.set_current_snapshot(table => 'sales.t', ref => 'nope')".to_string(),
            "Cannot find matching snapshot ID for ref nope",
        ),
    ] {
        let error = execute(&ctx, &catalogs, &sql)
            .await
            .expect_err("must refuse");
        assert!(
            error.to_string().contains(needle),
            "for {sql:?} want {needle:?}, got {error}"
        );
    }
}

#[tokio::test]
async fn rollback_to_timestamp_selects_latest_older_ancestor() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs).await;
    let before = snapshot_count(&ctx, &catalogs).await;
    let table = load_sales_table(&catalogs, "t").await;
    let metadata = table.metadata();
    let head = metadata.current_snapshot_id().unwrap();
    let mut chain = Vec::new();
    let mut current = Some(head);
    while let Some(id) = current {
        let snapshot = metadata.snapshot_by_id(id).unwrap();
        chain.push((id, snapshot.timestamp_ms()));
        current = snapshot.parent_snapshot_id();
    }
    assert!(chain.len() >= 2);
    let oldest_ms = chain.iter().map(|(_, ts)| *ts).min().unwrap();
    let newest_ms = chain.iter().map(|(_, ts)| *ts).max().unwrap();
    assert!(newest_ms > oldest_ms);
    let wall = chrono::DateTime::from_timestamp_millis(oldest_ms + 1)
        .unwrap()
        .naive_utc()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string();
    let (schema, row) = call_text(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.rollback_to_timestamp('sales.t', TIMESTAMP '{wall}')"),
    )
    .await;
    assert_eq!(
        schema,
        vec!["previous_snapshot_id:Int64", "current_snapshot_id:Int64"]
    );
    assert_eq!(row[0], head.to_string());
    let current: i64 = row[1].parse().unwrap();
    assert_eq!(
        metadata.snapshot_by_id(current).unwrap().timestamp_ms(),
        oldest_ms
    );
    assert_eq!(snapshot_count(&ctx, &catalogs).await, before);
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rollback_to_timestamp('sales.t', TIMESTAMP '2000-01-01 00:00:00')",
    )
    .await
    .expect_err("an ancient timestamp must refuse");
    assert!(
        error
            .to_string()
            .contains("Cannot roll back, no valid snapshot older than: 946684800000"),
        "ancient timestamp refusal, got {error}"
    );
    assert_eq!(snapshot_count(&ctx, &catalogs).await, before);
}
