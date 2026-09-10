use super::super::*;
use super::common::*;
use datafusion::arrow::array::Float64Array;

struct PlanRow {
    candidate: String,
    score: f64,
    partitions: i64,
    files_at_target: i64,
    ddl: String,
    calls: String,
    plan_id: String,
    notes: String,
}

async fn plan_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<PlanRow> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .expect("plan_partitioning runs")
        .collect()
        .await
        .expect("collect plan frame");
    assert_eq!(batches.len(), 1, "plan frame is one batch");
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
            "candidate",
            "score",
            "projected_partitions",
            "projected_files_at_target",
            "ddl",
            "calls",
            "plan_id",
            "notes",
        ]
    );
    assert_eq!(
        batch.schema().field_with_name("score").unwrap().data_type(),
        &DataType::Float64
    );
    assert_eq!(
        batch
            .schema()
            .field_with_name("projected_partitions")
            .unwrap()
            .data_type(),
        &DataType::Int64
    );
    assert_eq!(
        batch
            .schema()
            .field_with_name("projected_files_at_target")
            .unwrap()
            .data_type(),
        &DataType::Int64
    );
    let strings = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {index} is Utf8"))
    };
    let candidates = strings(0);
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("score is Float64");
    let partitions = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("projected_partitions is Int64");
    let files_at_target = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("projected_files_at_target is Int64");
    let ddls = strings(4);
    let calls = strings(5);
    let plan_ids = strings(6);
    let notes = strings(7);
    assert!(batch.num_rows() > 0, "frame holds at least one candidate");
    (0..batch.num_rows())
        .map(|row| PlanRow {
            candidate: candidates.value(row).to_string(),
            score: scores.value(row),
            partitions: partitions.value(row),
            files_at_target: files_at_target.value(row),
            ddl: ddls.value(row).to_string(),
            calls: calls.value(row).to_string(),
            plan_id: plan_ids.value(row).to_string(),
            notes: notes.value(row).to_string(),
        })
        .collect()
}

async fn files_total_bytes(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> i64 {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "SELECT COALESCE(SUM(file_size_in_bytes), 0) FROM {table}.files WHERE content = 0"
        ),
    )
    .await
    .expect("byte sum query")
    .collect()
    .await
    .expect("collect byte sum");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("byte sum is Int64")
        .value(0)
}

fn day_label(ordinal: i32) -> String {
    if ordinal < 31 {
        format!("2026-01-{:02}", ordinal + 1)
    } else if ordinal < 59 {
        format!("2026-02-{:02}", ordinal - 31 + 1)
    } else {
        format!("2026-03-{:02}", ordinal - 59 + 1)
    }
}

async fn seed_days90(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.days90 (ts TIMESTAMP, note STRING) USING iceberg",
    )
    .await;
    for file in 0..9 {
        let arms: Vec<String> = (0..10)
            .map(|day| {
                let stamp = day_label(file * 10 + day);
                format!(
                    "SELECT TIMESTAMP '{stamp} 00:00:00' AS ts, CAST(NULL AS STRING) AS note FROM src"
                )
            })
            .collect();
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.days90 {}", arms.join(" UNION ALL ")),
        )
        .await;
    }
}

async fn seed_regions(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.regions (region STRING) USING iceberg",
    )
    .await;
    for region in ["g00", "g01", "g02", "g03"] {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.regions SELECT '{region}' AS region FROM src"),
        )
        .await;
    }
}

#[tokio::test]
async fn plan_days_ts_ranks_first_on_ninety_day_fixture() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_days90(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.days90").await;
    assert!(total > 0, "fixture carries bytes to score");
    let target = total / 90;
    assert!(target > 0, "target covers ninety days, got {total}");
    let plan = plan_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.days90', target_file_size_bytes => {target})"
        ),
    )
    .await;
    assert_eq!(plan[0].candidate, "days(ts)");
    assert!(
        (plan[0].score - 0.0).abs() < f64::EPSILON,
        "winner scores zero, got: {}",
        plan[0].score
    );
    assert_eq!(plan[0].partitions, 90);
    assert!(
        plan[0].ddl.contains("days(ts)"),
        "winner ddl names the transform, got: {}",
        plan[0].ddl
    );
    let ordered = plan.iter().map(|row| row.score).collect::<Vec<_>>();
    let mut ranked = ordered.clone();
    ranked.sort_by(f64::total_cmp);
    assert_eq!(ordered, ranked, "frame ranks best first");
    assert!(
        plan[0].files_at_target > 0,
        "winner projects files at target"
    );
    assert!(
        plan[0].calls.contains("rewrite_data_files"),
        "calls chain the rewrite, got: {}",
        plan[0].calls
    );
}

#[tokio::test]
async fn plan_frame_carries_d1_columns_with_arrow_types() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_days90(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.days90").await;
    let plan = plan_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.days90', target_file_size_bytes => {})",
            total / 90
        ),
    )
    .await;
    assert!(plan.len() >= 5, "four grains plus unpartitioned");
}

#[tokio::test]
async fn plan_boundless_column_is_excluded_and_named_on_last_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_days90(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.days90").await;
    let plan = plan_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.days90', target_file_size_bytes => {})",
            total / 90
        ),
    )
    .await;
    assert!(
        plan.iter().all(|row| !row.candidate.contains("note")),
        "boundless column takes no candidate"
    );
    let last = plan.last().expect("frame is never empty");
    assert!(
        last.notes.contains("note"),
        "last row names the skipped column, got: {}",
        last.notes
    );
}

#[tokio::test]
async fn plan_unpartitioned_is_a_candidate() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_days90(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.days90").await;
    let plan = plan_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.days90', target_file_size_bytes => {})",
            total / 90
        ),
    )
    .await;
    assert!(
        plan.iter().any(|row| row.candidate == "unpartitioned"),
        "no-partitioning candidate present"
    );
}

#[tokio::test]
async fn plan_id_repeats_per_candidate_and_differs_across_candidates() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_days90(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.days90").await;
    let sql = format!(
        "CALL ice.system.plan_partitioning(table => 'sales.days90', target_file_size_bytes => {})",
        total / 90
    );
    let first = plan_rows(&ctx, &catalogs, &sql).await;
    let second = plan_rows(&ctx, &catalogs, &sql).await;
    assert_eq!(first.len(), second.len());
    for pair in first.iter().zip(second.iter()) {
        assert_eq!(pair.0.candidate, pair.1.candidate);
        assert_eq!(pair.0.plan_id, pair.1.plan_id, "plan id is stable");
        assert!(!pair.0.plan_id.is_empty(), "plan id is never empty");
    }
    let mut ids: Vec<_> = first.iter().map(|row| row.plan_id.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), first.len(), "one plan id per candidate");
}

#[tokio::test]
async fn plan_every_row_carries_the_r001_file_count_caveat() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_days90(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.days90").await;
    let plan = plan_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.days90', target_file_size_bytes => {})",
            total / 90
        ),
    )
    .await;
    for row in &plan {
        assert!(
            row.notes.contains("AP-0-R-001") && row.notes.contains("projected_files_at_target"),
            "R-001 caveat on every row, got: {}",
            row.notes
        );
    }
}

#[tokio::test]
async fn plan_argument_refusals_name_the_key() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let missing = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.plan_partitioning(table => 'sales.ghost')",
    )
    .await
    .expect_err("target is required");
    assert!(
        missing.to_string().contains("target_file_size_bytes"),
        "got: {missing}"
    );
    for sql in [
        "CALL ice.system.plan_partitioning(table => 'sales.ghost', target_file_size_bytes => 0)",
        "CALL ice.system.plan_partitioning(table => 'sales.ghost', target_file_size_bytes => -5)",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("target must be positive");
        assert!(error.to_string().contains("positive"), "got: {error}");
    }
    let unknown = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.plan_partitioning(table => 'sales.ghost', target_file_size_bytes => 8, bogus_key => 1)",
    )
    .await
    .expect_err("unknown key must refuse");
    assert!(
        unknown
            .to_string()
            .contains("unknown CALL argument `bogus_key`"),
        "got: {unknown}"
    );
}

#[tokio::test]
async fn plan_branch_besides_main_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.branched AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.branched CREATE BRANCH feat",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.plan_partitioning(table => 'sales.branched', target_file_size_bytes => 1024)",
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
async fn plan_multi_spec_table_reports_the_single_spec_rewrite() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.evolved (ts TIMESTAMP, id INT) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.evolved SELECT CAST(DATE '2026-02-01' AS TIMESTAMP) AS ts, 1 AS id FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.evolved ADD PARTITION FIELD days(ts)",
    )
    .await;
    let plan = plan_rows(
        &ctx,
        &catalogs,
        "CALL ice.system.plan_partitioning(table => 'sales.evolved', target_file_size_bytes => 1024)",
    )
    .await;
    assert!(
        plan.iter().any(|row| row.notes.contains("one spec")),
        "multi-spec rewrite reported"
    );
}

#[tokio::test]
async fn plan_identity_region_ranks_first_at_twice_target() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_regions(&ctx, &catalogs).await;
    let total = files_total_bytes(&ctx, &catalogs, "ice.sales.regions").await;
    assert!(total > 0, "fixture carries bytes to score");
    let target = total / 8;
    assert!(target > 0, "target leaves headroom, got {total}");
    let plan = plan_rows(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.plan_partitioning(table => 'sales.regions', target_file_size_bytes => {target})"
        ),
    )
    .await;
    assert_eq!(plan[0].candidate, "identity(region)");
    assert!(
        (plan[0].score - 0.0).abs() < f64::EPSILON,
        "winner scores zero, got: {}",
        plan[0].score
    );
    assert_eq!(plan[0].partitions, 4);
}
