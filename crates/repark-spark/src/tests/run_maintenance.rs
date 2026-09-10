use super::super::*;
use super::common::*;

struct PlanRow {
    ordinal: i32,
    procedure: String,
    arguments: String,
    status: String,
    result: String,
}

fn full_policy() -> &'static str {
    "[default.maintenance]\n\
     target_file_size_bytes = 536870912\n\
     snapshot_retain_last = 5\n\
     snapshot_older_than = \"7d\"\n\
     orphan_older_than = \"3d\"\n\
     rewrite_manifests = true\n\
     position_delete_ratio = 0.3\n\
     \n\
     [default.maintenance.tables.\"ice.sales.orders\"]\n\
     target_file_size_bytes = 268435456\n\
     snapshot_retain_last = 20\n"
}

fn stamp_policy(catalogs: &mut CatalogRegistry, document: &str) {
    let policy =
        repark_core::parse_maintenance_policy("default", document).expect("policy fixture parses");
    catalogs.set_maintenance_policy("default", policy);
}

async fn dry_run_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<PlanRow> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("run_maintenance dry run")
        .collect()
        .await
        .expect("collect plan");
    assert_eq!(batches.len(), 1, "plan is one batch");
    let batch = &batches[0];
    let schema = batch.schema();
    let names: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        names,
        vec!["step", "procedure", "arguments", "status", "result"]
    );
    assert_eq!(
        batch.schema().field_with_name("step").unwrap().data_type(),
        &DataType::Int32
    );
    for name in ["procedure", "arguments", "status", "result"] {
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
    (0..batch.num_rows())
        .map(|row| PlanRow {
            ordinal: ordinals.value(row),
            procedure: procedures.value(row).to_string(),
            arguments: arguments.value(row).to_string(),
            status: statuses.value(row).to_string(),
            result: results.value(row).to_string(),
        })
        .collect()
}

async fn seed_twenty_files_three_snapshots(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await;
    for id in 2..=20 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.orders SELECT {id} AS id, 'x' AS name"),
        )
        .await;
    }
    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    run(
        ctx,
        catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(table => 'sales.orders', \
             older_than => {older_than_ms}, retain_last => 3)"
        ),
    )
    .await;
    assert_eq!(
        rows(ctx, catalogs, "SELECT * FROM ice.sales.orders.data_files").await,
        20,
        "fixture holds 20 small files"
    );
    assert_eq!(
        rows(ctx, catalogs, "SELECT * FROM ice.sales.orders.snapshots").await,
        3,
        "fixture holds 3 snapshots"
    );
}

async fn seed_mor_with_deletes(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.mord (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    for id in 1..=5 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.mord VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    for value in ["x", "y"] {
        run(
            ctx,
            catalogs,
            &format!(
                "MERGE INTO ice.sales.mord AS t USING (SELECT 1 AS id, '{value}' AS v) AS s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v"
            ),
        )
        .await;
    }
    assert!(
        rows(ctx, catalogs, "SELECT * FROM ice.sales.mord.delete_files").await >= 1,
        "merge-on-read merges leave live delete files"
    );
}

async fn sum_bytes(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> i64 {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("byte sum query")
        .collect()
        .await
        .expect("collect byte sum");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("byte sum is Int64");
    assert!(column.is_valid(0), "coalesced sum is never null");
    column.value(0)
}

#[tokio::test]
async fn run_maintenance_dry_run_plans_every_step_with_planned_status() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_mor_with_deletes(&ctx, &catalogs).await;
    stamp_policy(&mut catalogs, full_policy());
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.mord', position_delete_ratio => 0.0)",
    )
    .await;
    assert_eq!(plan.len(), 5, "every D-4 step plans, got {}", plan.len());
    let procedures: Vec<_> = plan.iter().map(|row| row.procedure.as_str()).collect();
    assert_eq!(
        procedures,
        vec![
            "rewrite_position_delete_files",
            "rewrite_data_files",
            "rewrite_manifests",
            "expire_snapshots",
            "remove_orphan_files",
        ]
    );
    let ordinals: Vec<_> = plan.iter().map(|row| row.ordinal).collect();
    assert_eq!(ordinals, vec![1, 2, 3, 4, 5]);
    for row in &plan {
        assert_eq!(row.status, "planned", "dry run marks every row planned");
        assert_eq!(row.result, "", "dry run carries no procedure result yet");
        assert!(
            row.arguments.starts_with("CALL ice.system."),
            "arguments render the CALL as issued, got: {}",
            row.arguments
        );
        assert!(
            row.arguments.contains("'sales.mord'"),
            "arguments echo the caller table spelling, got: {}",
            row.arguments
        );
    }
    assert_eq!(
        plan[0].arguments,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.mord')"
    );
    assert_eq!(
        plan[2].arguments,
        "CALL ice.system.rewrite_manifests(table => 'sales.mord')"
    );
    assert!(
        plan[1]
            .arguments
            .contains("target-file-size-bytes', '536870912'"),
        "rewrite carries the profile size, got: {}",
        plan[1].arguments
    );
    assert!(
        plan[3].arguments.contains("retain_last => 5"),
        "expire carries the profile retain_last, got: {}",
        plan[3].arguments
    );
    assert!(
        plan[3].arguments.contains("older_than => "),
        "expire renders the computed older_than, got: {}",
        plan[3].arguments
    );
    assert!(
        plan[4].arguments.contains("older_than => "),
        "orphan cleanup renders the computed older_than, got: {}",
        plan[4].arguments
    );
}

#[tokio::test]
async fn run_maintenance_dry_run_defaults_to_true() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_twenty_files_three_snapshots(&ctx, &catalogs).await;
    stamp_policy(&mut catalogs, full_policy());
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders')",
    )
    .await;
    assert!(
        plan.iter().all(|row| row.status == "planned"),
        "omitting dry_run plans instead of applying"
    );
}

#[tokio::test]
async fn run_maintenance_delete_ratio_gate_below_skips_step_1() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_twenty_files_three_snapshots(&ctx, &catalogs).await;
    stamp_policy(&mut catalogs, full_policy());
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders')",
    )
    .await;
    assert!(
        plan.iter()
            .all(|row| row.procedure != "rewrite_position_delete_files"),
        "zero delete bytes stay below the 0.3 gate"
    );
    let ordinals: Vec<_> = plan.iter().map(|row| row.ordinal).collect();
    assert_eq!(
        ordinals,
        vec![2, 3, 4, 5],
        "ordinals keep their D-4 numbers"
    );
}

#[allow(clippy::cast_precision_loss)]
#[tokio::test]
async fn run_maintenance_delete_ratio_gate_at_runs_step_1() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_mor_with_deletes(&ctx, &catalogs).await;
    stamp_policy(&mut catalogs, full_policy());
    let data_bytes = sum_bytes(
        &ctx,
        &catalogs,
        "SELECT COALESCE(SUM(file_size_in_bytes), 0) FROM ice.sales.mord.files WHERE content = 0",
    )
    .await;
    let delete_bytes = sum_bytes(
        &ctx,
        &catalogs,
        "SELECT COALESCE(SUM(file_size_in_bytes), 0) FROM ice.sales.mord.delete_files",
    )
    .await;
    assert!(
        data_bytes > 0 && delete_bytes > 0,
        "gate fixture carries both sides"
    );
    let measured = delete_bytes as f64 / data_bytes as f64;
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.run_maintenance(table => 'sales.mord', position_delete_ratio => {measured:?})"
        ),
    )
    .await;
    assert_eq!(
        plan[0].procedure, "rewrite_position_delete_files",
        "a ratio exactly at the threshold admits step 1"
    );
    assert_eq!(plan[0].ordinal, 1);
}

#[tokio::test]
async fn run_maintenance_inline_wins_over_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_twenty_files_three_snapshots(&ctx, &catalogs).await;
    stamp_policy(&mut catalogs, full_policy());
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders', snapshot_retain_last => 2)",
    )
    .await;
    let expire = plan
        .iter()
        .find(|row| row.procedure == "expire_snapshots")
        .expect("expire step plans");
    assert!(
        expire.arguments.contains("retain_last => 2"),
        "inline beats the table entry, got: {}",
        expire.arguments
    );
    let rewrite = plan
        .iter()
        .find(|row| row.procedure == "rewrite_data_files")
        .expect("rewrite step plans");
    assert!(
        rewrite
            .arguments
            .contains("target-file-size-bytes', '268435456'"),
        "the table entry still feeds keys with no inline override, got: {}",
        rewrite.arguments
    );
    let unoverridden = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders')",
    )
    .await;
    let expire = unoverridden
        .iter()
        .find(|row| row.procedure == "expire_snapshots")
        .expect("expire step plans");
    assert!(
        expire.arguments.contains("retain_last => 20"),
        "the table entry beats the profile value, got: {}",
        expire.arguments
    );
}

#[tokio::test]
async fn run_maintenance_no_policy_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.ghost')",
    )
    .await
    .expect_err("no policy and no inline keys must refuse");
    let message = error.to_string();
    assert!(
        message.contains(
            "run_maintenance: no [default.maintenance] table and no inline keys for sales.ghost"
        ),
        "D-6 names the profile and the table, got: {message}"
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lonely AS SELECT * FROM src",
    )
    .await;
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.lonely', target_file_size_bytes => 100)",
    )
    .await;
    assert!(
        plan.iter().any(|row| row.procedure == "rewrite_data_files"),
        "inline keys alone satisfy D-6"
    );
}

#[tokio::test]
async fn run_maintenance_apply_refuses_until_step_3() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    stamp_policy(&mut catalogs, full_policy());
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.t', dry_run => false)",
    )
    .await
    .expect_err("apply must refuse loud until step 3 lands it");
    let message = error.to_string();
    assert!(
        message.contains("dry_run") && message.contains("not supported"),
        "refusal names the mode, got: {message}"
    );
}

#[tokio::test]
async fn run_maintenance_unknown_inline_key_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.t', bogus_key => 1)",
    )
    .await
    .expect_err("unknown inline key must refuse");
    let message = error.to_string();
    assert!(
        message.contains("unknown CALL argument `bogus_key`"),
        "got: {message}"
    );
}

#[tokio::test]
async fn run_maintenance_negative_inline_integer_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.t', target_file_size_bytes => -1)",
    )
    .await
    .expect_err("negative inline integer must refuse");
    let message = error.to_string();
    assert!(
        message.contains("CALL argument `target_file_size_bytes` must be a non-negative integer"),
        "got: {message}"
    );
}

#[tokio::test]
async fn run_maintenance_malformed_inline_duration_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.t', snapshot_older_than => '7x')",
    )
    .await
    .expect_err("malformed inline duration must refuse naming the key");
    let message = error.to_string();
    assert!(
        message.contains("run_maintenance.snapshot_older_than")
            && message.contains("invalid duration"),
        "got: {message}"
    );
}

#[tokio::test]
async fn run_maintenance_adaptive_partitioning_inline_reserved() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.t', adaptive_partitioning => true)",
    )
    .await
    .expect_err("the reserved key must refuse inline too");
    let message = error.to_string();
    assert!(
        message.contains("adaptive_partitioning") && message.contains("not yet supported"),
        "got: {message}"
    );
}
