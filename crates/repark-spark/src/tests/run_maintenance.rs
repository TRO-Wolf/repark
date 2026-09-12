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

async fn apply_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<PlanRow> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("run_maintenance apply")
        .collect()
        .await
        .expect("collect apply frame");
    plan_rows_from_batches(&batches)
}

async fn dry_run_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<PlanRow> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("run_maintenance dry run")
        .collect()
        .await
        .expect("collect plan");
    plan_rows_from_batches(&batches)
}

fn plan_rows_from_batches(batches: &[RecordBatch]) -> Vec<PlanRow> {
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

async fn data_file_count(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> usize {
    rows(
        ctx,
        catalogs,
        &format!("SELECT * FROM ice.{table}.data_files"),
    )
    .await
}

async fn snapshot_count(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> usize {
    rows(
        ctx,
        catalogs,
        &format!("SELECT * FROM ice.{table}.snapshots"),
    )
    .await
}

fn plant_stray(table_dir: &std::path::Path, name: &str, age_days: u64) {
    let data_dir = table_dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let path = data_dir.join(name);
    std::fs::write(&path, b"not really parquet").expect("write stray");
    let stamp = std::time::SystemTime::now() - std::time::Duration::from_secs(age_days * 86_400);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("reopen stray to age it")
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(stamp)
                .set_accessed(stamp),
        )
        .expect("age the stray");
}

#[tokio::test]
async fn apply_binpacks_to_expected_count() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_twenty_files_three_snapshots(&ctx, &catalogs).await;
    stamp_policy(
        &mut catalogs,
        "[default.maintenance]\ntarget_file_size_bytes = 67108864\n",
    );
    let applied = apply_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders', dry_run => false)",
    )
    .await;
    assert_eq!(
        applied.len(),
        2,
        "steps 2 and 4 apply, got {}",
        applied.len()
    );
    assert!(
        applied.iter().all(|row| row.status == "ran"),
        "every planned step ran"
    );
    assert_eq!(
        data_file_count(&ctx, &catalogs, "sales.orders").await,
        1,
        "twenty small files binpack into one"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        22,
        "every row stays readable after the rewrite"
    );
}

#[tokio::test]
async fn apply_expires_to_retain_last() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_twenty_files_three_snapshots(&ctx, &catalogs).await;
    stamp_policy(
        &mut catalogs,
        "[default.maintenance]\nsnapshot_older_than = \"0d\"\nsnapshot_retain_last = 1\n",
    );
    let applied = apply_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders', dry_run => false)",
    )
    .await;
    let expire = applied
        .iter()
        .find(|row| row.procedure == "expire_snapshots")
        .expect("expire step applied");
    assert_eq!(expire.status, "ran", "expire ran, got: {}", expire.result);
    assert_eq!(
        snapshot_count(&ctx, &catalogs, "sales.orders").await,
        1,
        "snapshots expire down to retain_last"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        22,
        "every row stays readable after the expiry"
    );
}

#[tokio::test]
async fn apply_removes_orphan() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sweep AS SELECT * FROM src",
    )
    .await;
    plant_stray(
        &wh.path().join("sales").join("sweep"),
        "orphan-0.parquet",
        10,
    );
    stamp_policy(
        &mut catalogs,
        "[default.maintenance]\norphan_older_than = \"3d\"\n",
    );
    let applied = apply_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.sweep', dry_run => false)",
    )
    .await;
    let orphan = applied
        .iter()
        .find(|row| row.procedure == "remove_orphan_files")
        .expect("orphan step applied");
    assert_eq!(
        orphan.status, "ran",
        "orphan sweep ran, got: {}",
        orphan.result
    );
    assert!(
        orphan.result.contains("orphan-0.parquet"),
        "the result names the removed stray, got: {}",
        orphan.result
    );
    assert!(
        !wh.path()
            .join("sales")
            .join("sweep")
            .join("data")
            .join("orphan-0.parquet")
            .exists(),
        "the aged stray is gone from the table location"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.sweep").await,
        3,
        "live rows stay readable after the sweep"
    );
}

#[tokio::test]
async fn failed_step_stops_chain() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.fragile AS SELECT * FROM src",
    )
    .await;
    stamp_policy(
        &mut catalogs,
        "[default.maintenance]\ntarget_file_size_bytes = 0\nrewrite_manifests = true\n",
    );
    let applied = apply_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.fragile', dry_run => false)",
    )
    .await;
    assert_eq!(
        applied.len(),
        3,
        "steps 2, 3 and 4 report, got {}",
        applied.len()
    );
    assert_eq!(applied[0].procedure, "rewrite_data_files");
    assert_eq!(applied[0].status, "failed", "the bad size fails step 2");
    assert!(
        applied[0].result.contains("target-file-size-bytes"),
        "the failed row carries the fork refusal, got: {}",
        applied[0].result
    );
    assert_eq!(applied[1].status, "skipped");
    assert_eq!(applied[2].status, "skipped");
    assert_eq!(applied[1].result, "");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.fragile").await,
        3,
        "the table stays readable after the stopped chain"
    );
}

#[tokio::test]
async fn apply_omits_gate_skipped_steps_like_dry_run() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    seed_twenty_files_three_snapshots(&ctx, &catalogs).await;
    stamp_policy(&mut catalogs, full_policy());
    let planned = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders')",
    )
    .await;
    let applied = apply_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.run_maintenance(table => 'sales.orders', dry_run => false)",
    )
    .await;
    let planned_ordinals: Vec<i32> = planned.iter().map(|row| row.ordinal).collect();
    let applied_ordinals: Vec<i32> = applied.iter().map(|row| row.ordinal).collect();
    assert_eq!(planned_ordinals, vec![2, 3, 4, 5]);
    assert_eq!(
        applied_ordinals, planned_ordinals,
        "apply reports exactly the planned steps"
    );
    assert!(
        applied.iter().all(|row| row.status == "ran"),
        "a clean apply runs every planned step"
    );
}

fn session_toml_file(directory: &TempDir, text: &str) -> std::path::PathBuf {
    let path = directory.path().join("repark.toml");
    std::fs::write(&path, text).expect("write session repark.toml");
    path
}

async fn session_with_policy_file(
    warehouse: &TempDir,
    file_directory: &TempDir,
    text: &str,
) -> repark_core::ReparkSession {
    use std::sync::Arc;

    use repark_core::ReparkSession;

    use crate::{SparkDialect, SparkExtension};
    let path = session_toml_file(file_directory, text);
    let session = ReparkSession::builder()
        .from_config_file(Some(path))
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .expect("session builds from the policy file");
    let warehouse_text = warehouse.path().to_str().expect("utf8 warehouse");
    session
        .register_memory_catalog("ice", warehouse_text)
        .await
        .expect("memory catalog registers");
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse_text}/sales"))]),
        )
        .await
        .expect("namespace creates");
    session
}

#[tokio::test]
async fn a_repark_toml_policy_reaches_run_maintenance() {
    let warehouse = TempDir::new().unwrap();
    let file_directory = TempDir::new().unwrap();
    let session = session_with_policy_file(
        &warehouse,
        &file_directory,
        "[default.maintenance]\ntarget_file_size_bytes = 536870912\nsnapshot_retain_last = 7\n",
    )
    .await;
    session
        .sql("CREATE TABLE ice.sales.t (id INT) USING iceberg")
        .await
        .expect("create")
        .collect()
        .await
        .expect("collect create");
    let batches = session
        .sql("CALL ice.system.run_maintenance(table => 'sales.t')")
        .await
        .expect("the file policy plans with no inline keys")
        .collect()
        .await
        .expect("collect plan");
    let planned = plan_rows_from_batches(&batches);
    let rewrite = planned
        .iter()
        .find(|row| row.procedure == "rewrite_data_files")
        .expect("rewrite step plans");
    assert!(
        rewrite
            .arguments
            .contains("target-file-size-bytes', '536870912'"),
        "rewrite carries the file size, got: {}",
        rewrite.arguments
    );
    let expire = planned
        .iter()
        .find(|row| row.procedure == "expire_snapshots")
        .expect("expire step plans");
    assert!(
        expire.arguments.contains("retain_last => 7"),
        "expire carries the file retain_last, got: {}",
        expire.arguments
    );
}

#[tokio::test]
async fn the_d6_refusal_names_the_active_profile() {
    let warehouse = TempDir::new().unwrap();
    let file_directory = TempDir::new().unwrap();
    let session = session_with_policy_file(
        &warehouse,
        &file_directory,
        "[default.session]\nbatch_size = 4096\n",
    )
    .await;
    session
        .sql("CREATE TABLE ice.sales.t (id INT) USING iceberg")
        .await
        .expect("create")
        .collect()
        .await
        .expect("collect create");
    let error = session
        .sql("CALL ice.system.run_maintenance(table => 'sales.t')")
        .await
        .expect_err("no policy anywhere must refuse");
    assert!(
        error
            .to_string()
            .contains("run_maintenance: no [default.maintenance] table and no inline keys"),
        "the refusal names the active profile, got: {error}"
    );
}

async fn register_s3t_kind(ctx: &SessionContext, catalogs: &mut CatalogRegistry) {
    let memory = Arc::clone(catalogs.get("ice").expect("ice is registered"));
    repark_iceberg::catalog::register_iceberg_catalog(ctx, "s3t", memory.clone())
        .await
        .expect("the s3t provider registers");
    catalogs.insert(
        "s3t".to_string(),
        memory,
        LocationPolicy::ServiceManagedLocation,
    );
}

#[tokio::test]
async fn run_maintenance_on_s3_tables_marks_the_orphan_step_skipped() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.s3tm AS SELECT * FROM src",
    )
    .await;
    register_s3t_kind(&ctx, &mut catalogs).await;
    let plan = dry_run_rows(
        &ctx,
        &catalogs,
        "CALL s3t.system.run_maintenance(table => 'sales.s3tm', orphan_older_than => '3d')",
    )
    .await;
    let orphan = plan
        .iter()
        .find(|row| row.procedure == "remove_orphan_files")
        .expect("the orphan step still plans");
    assert_eq!(
        orphan.status, "skipped",
        "a table bucket cannot be listed, got: {}",
        orphan.status
    );
    assert!(
        orphan
            .result
            .contains("table buckets do not support listing"),
        "the skipped row names the reason, got: {}",
        orphan.result
    );
    assert!(
        orphan.result.contains("unreferencedFileRemoval"),
        "the skipped row names the remedy, got: {}",
        orphan.result
    );
    for row in &plan {
        if row.procedure != "remove_orphan_files" {
            assert_eq!(
                row.status, "planned",
                "{} still plans normally, got: {}",
                row.procedure, row.status
            );
        }
    }
}

#[tokio::test]
async fn run_maintenance_apply_on_s3_tables_skips_orphan_and_runs_the_rest() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.s3ta AS SELECT * FROM src",
    )
    .await;
    register_s3t_kind(&ctx, &mut catalogs).await;
    let applied = apply_rows(
        &ctx,
        &catalogs,
        "CALL s3t.system.run_maintenance(table => 'sales.s3ta', orphan_older_than => '3d', \
         dry_run => false)",
    )
    .await;
    let orphan = applied
        .iter()
        .find(|row| row.procedure == "remove_orphan_files")
        .expect("the orphan step still reports");
    assert_eq!(orphan.status, "skipped", "got: {}", orphan.status);
    assert!(
        orphan
            .result
            .contains("table buckets do not support listing"),
        "the skipped row names the reason, got: {}",
        orphan.result
    );
    for row in applied
        .iter()
        .filter(|row| row.procedure != "remove_orphan_files")
    {
        assert_eq!(
            row.status, "ran",
            "{} still runs, got: {}",
            row.procedure, row.result
        );
    }
}
