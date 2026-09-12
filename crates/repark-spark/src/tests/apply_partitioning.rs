use super::super::*;
use super::common::*;

struct ApplyRow {
    step: i32,
    procedure: String,
    arguments: String,
    status: String,
    result: String,
    plan_id: String,
}

async fn apply_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<ApplyRow> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("apply_partitioning runs")
        .collect()
        .await
        .expect("collect apply frame");
    assert_eq!(batches.len(), 1, "apply frame is one batch");
    let batch = &batches[0];
    let schema = batch.schema();
    let names: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "step",
            "procedure",
            "arguments",
            "status",
            "result",
            "plan_id",
        ]
    );
    assert_eq!(
        batch.schema().field_with_name("step").unwrap().data_type(),
        &DataType::Int32
    );
    for name in ["procedure", "arguments", "status", "result", "plan_id"] {
        assert_eq!(
            batch.schema().field_with_name(name).unwrap().data_type(),
            &DataType::Utf8,
            "{name} reads back as Utf8"
        );
    }
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("step is Int32");
    let strings = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {index} is Utf8"))
    };
    let procedures = strings(1);
    let arguments = strings(2);
    let statuses = strings(3);
    let results = strings(4);
    let plan_ids = strings(5);
    (0..batch.num_rows())
        .map(|row| ApplyRow {
            step: ordinals.value(row),
            procedure: procedures.value(row).to_string(),
            arguments: arguments.value(row).to_string(),
            status: statuses.value(row).to_string(),
            result: results.value(row).to_string(),
            plan_id: plan_ids.value(row).to_string(),
        })
        .collect()
}

async fn seed_table(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (ts TIMESTAMP, id INT) USING iceberg"),
    )
    .await;
    for day in 1..=6 {
        run(
            ctx,
            catalogs,
            &format!(
                "INSERT INTO ice.sales.{table} SELECT TIMESTAMP '2026-01-{day:02} 00:00:00' AS ts, {day} AS id FROM src"
            ),
        )
        .await;
    }
}

async fn plan_id_of(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    candidate: &str,
) -> String {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.{table}', target_file_size_bytes => 1024)"
        ),
    )
    .await
    .expect("plan_partitioning runs")
    .collect()
    .await
    .expect("collect plan frame");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let labels = batch
        .column_by_name("candidate")
        .expect("candidate")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("candidate is Utf8");
    let ids = batch
        .column_by_name("plan_id")
        .expect("plan_id")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("plan_id is Utf8");
    for row in 0..batch.num_rows() {
        if labels.value(row) == candidate {
            return ids.value(row).to_string();
        }
    }
    let available: Vec<_> = (0..batch.num_rows())
        .map(|row| labels.value(row).to_string())
        .collect();
    panic!("candidate `{candidate}` missing, got {available:?}");
}

async fn current_snapshot(catalogs: &CatalogRegistry, table: &str) -> i64 {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_snapshot_id()
        .expect("table has a current snapshot")
}

#[tokio::test]
async fn apply_unknown_argument_names_the_accepted_set() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.apply_partitioning(table => 'sales.ghost', plan_id => 'abc', bogus_key => 1)",
    )
    .await
    .expect_err("unknown key must refuse");
    let message = error.to_string();
    assert!(
        message.contains("unknown CALL argument `bogus_key`")
            && message.contains("dry_run")
            && message.contains("plan_id")
            && message.contains("table"),
        "got: {message}"
    );
}

#[tokio::test]
async fn apply_missing_plan_id_is_required() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.apply_partitioning(table => 'sales.ghost')",
    )
    .await
    .expect_err("plan_id is required");
    assert!(error.to_string().contains("plan_id"), "got: {error}");
}

#[tokio::test]
async fn apply_default_dry_run_commits_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "dry").await;
    let before = current_snapshot(&catalogs, "dry").await;
    let plan_id = plan_id_of(&ctx, &catalogs, "dry", "days(ts)").await;
    let frame = apply_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.dry', plan_id => '{plan_id}')"
        ),
    )
    .await;
    assert!(!frame.is_empty(), "dry-run frame lists the steps");
    assert!(
        frame.iter().all(|row| {
            row.status == "dry_run" && row.plan_id == plan_id && row.result.is_empty()
        }),
        "every row is dry_run for the requested plan"
    );
    assert!(
        frame.iter().any(|row| row.procedure == "ALTER TABLE"),
        "days(ts) plans an ADD PARTITION FIELD step"
    );
    for name in [
        "rewrite_data_files",
        "rewrite_manifests",
        "expire_snapshots",
    ] {
        assert!(
            frame.iter().any(|row| row.procedure == name),
            "chain includes {name}"
        );
    }
    assert_eq!(current_snapshot(&catalogs, "dry").await, before);
    let table = load_sales_table(&catalogs, "dry").await;
    assert!(table.metadata().default_partition_spec().is_unpartitioned());
}

#[tokio::test]
async fn apply_dry_run_false_commits_the_chain() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "wet").await;
    let before = current_snapshot(&catalogs, "wet").await;
    let plan_id = plan_id_of(&ctx, &catalogs, "wet", "days(ts)").await;
    let frame = apply_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.wet', plan_id => '{plan_id}', dry_run => false)"
        ),
    )
    .await;
    assert!(
        frame.iter().all(|row| {
            row.status == "applied" && row.plan_id == plan_id && !row.result.is_empty()
        }),
        "every row is applied for the requested plan"
    );
    assert_eq!(frame[0].step, 1);
    assert_eq!(frame[0].procedure, "ALTER TABLE");
    assert!(
        frame[0].arguments.contains("ADD PARTITION FIELD days(ts)"),
        "got: {}",
        frame[0].arguments
    );
    let procedures: Vec<_> = frame.iter().map(|row| row.procedure.as_str()).collect();
    assert!(
        procedures.windows(3).any(|window| window
            == [
                "rewrite_data_files",
                "rewrite_manifests",
                "expire_snapshots"
            ]),
        "maintenance chain is in order, got {procedures:?}"
    );
    let after = current_snapshot(&catalogs, "wet").await;
    assert_ne!(after, before, "a real apply commits at least one snapshot");
    let table = load_sales_table(&catalogs, "wet").await;
    assert!(!table.metadata().default_partition_spec().is_unpartitioned());
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.wet").await,
        18
    );
}

#[tokio::test]
async fn apply_unknown_plan_id_refuses_naming_table_and_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "miss").await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.apply_partitioning(table => 'sales.miss', plan_id => 'deadbeefdeadbeef')",
    )
    .await
    .expect_err("unknown plan_id must refuse");
    let message = error.to_string();
    assert!(
        message.contains("sales.miss")
            && message.contains("deadbeefdeadbeef")
            && message.contains("snapshot")
            && message.contains("not from this table"),
        "got: {message}"
    );
}

#[tokio::test]
async fn apply_stale_plan_id_after_commit_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "stale").await;
    let plan_id = plan_id_of(&ctx, &catalogs, "stale", "days(ts)").await;
    let _ = apply_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.stale', plan_id => '{plan_id}', dry_run => false)"
        ),
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.stale', plan_id => '{plan_id}', dry_run => false)"
        ),
    )
    .await
    .expect_err("stale plan_id must refuse");
    let message = error.to_string();
    assert!(
        message.contains("sales.stale")
            && message.contains(plan_id.as_str())
            && message.contains("snapshot"),
        "got: {message}"
    );
}

#[tokio::test]
async fn apply_branch_besides_main_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "branched").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.branched CREATE BRANCH feat",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.apply_partitioning(table => 'sales.branched', plan_id => 'abc')",
    )
    .await
    .expect_err("non-main branch must refuse");
    let message = error.to_string();
    assert!(
        message.contains("feat") && message.contains("branch") && message.contains("main"),
        "refusal names the branch and main, got: {message}"
    );
}

#[tokio::test]
async fn apply_preserves_sort_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "sorted").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sorted WRITE ORDERED BY (id)",
    )
    .await;
    let before = load_sales_table(&catalogs, "sorted").await;
    let before_id = before.metadata().default_sort_order_id();
    let before_fields = before.metadata().default_sort_order().fields.clone();
    assert!(!before_fields.is_empty(), "fixture carries a sort order");
    let plan_id = plan_id_of(&ctx, &catalogs, "sorted", "days(ts)").await;
    let _ = apply_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.sorted', plan_id => '{plan_id}', dry_run => false)"
        ),
    )
    .await;
    let after = load_sales_table(&catalogs, "sorted").await;
    assert_eq!(after.metadata().default_sort_order_id(), before_id);
    assert_eq!(after.metadata().default_sort_order().fields, before_fields);
}

#[tokio::test]
async fn apply_multi_spec_rewrites_to_one_current_spec() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "multi").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.multi ADD PARTITION FIELD days(ts)",
    )
    .await;
    let evolved = load_sales_table(&catalogs, "multi").await;
    assert!(
        evolved.metadata().partition_specs_iter().len() > 1,
        "fixture carries more than one partition spec"
    );
    let plan_id = plan_id_of(&ctx, &catalogs, "multi", "identity(id)").await;
    let _ = apply_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.multi', plan_id => '{plan_id}', dry_run => false)"
        ),
    )
    .await;
    let specs = live_data_file_spec_ids(&catalogs, "multi").await;
    assert_eq!(
        specs.len(),
        1,
        "live data files share one spec, got {specs:?}"
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.multi ORDER BY id, ts"
        )
        .await,
        18
    );
}

#[tokio::test]
async fn apply_unpartitioned_candidate_has_no_ddl_step() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "none").await;
    let plan_id = plan_id_of(&ctx, &catalogs, "none", "unpartitioned").await;
    let frame = apply_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.apply_partitioning(table => 'sales.none', plan_id => '{plan_id}')"
        ),
    )
    .await;
    assert!(
        frame.iter().all(|row| row.procedure != "ALTER TABLE"),
        "unpartitioned has no DDL step, got {:?}",
        frame
            .iter()
            .map(|row| row.procedure.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(frame[0].procedure, "rewrite_data_files");
    assert_eq!(frame[0].step, 1);
}
