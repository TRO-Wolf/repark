use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect, displayable};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use tempfile::TempDir;

use super::super::df_guards::skipping_limit::{ENFORCE_SORTING_RULE_NAME, sealed_for_test};
use crate::ReparkSession;

const GRID: &str = include_str!("skipping_limit_grid.tsv");
const SERIES_ROWS: &str = "SELECT value AS v, value % 7 AS k FROM generate_series(1, 5000)";
const SHUFFLED_ROWS: &str = "SELECT value AS v, value % 7 AS k FROM generate_series(1, 5000) \
     ORDER BY (value * 7919) % 5003";
const REPORTED: &str = "SELECT v FROM (SELECT v FROM m ORDER BY v LIMIT 5 OFFSET 4990) ORDER BY v";
const REPORTED_ROWS: &str = "5 [4991,4992,4993,4994,4995]";
const SORT_ABOVE_OFFSET: &str =
    "SELECT v FROM (SELECT v FROM m ORDER BY v OFFSET 4990) ORDER BY v DESC";
const SORT_ABOVE_OFFSET_ROWS: &str = "10 [5000,4999,4998,4997,4996,4995,4994,4993,4992,4991]";
const PARTITION_COUNTS: [usize; 3] = [1, 2, 16];
const ONE_PARTITION: &str = "one-partition";
const EVERY_PARTITION: &str = "every-partition";
const CONTROL: &str = "control";
const SEAL_NAME: &str = "SealedSkippingLimitExec";

struct Cell {
    name: &'static str,
    family: &'static str,
    sql: &'static str,
    expected: &'static str,
}

struct Sources {
    session: ReparkSession,
    _parquet: TempDir,
    _warehouse: TempDir,
}

fn cells() -> Vec<Cell> {
    GRID.lines()
        .map(|line| {
            let mut fields = line.split('\t');
            let mut next = || fields.next().unwrap();
            Cell {
                name: next(),
                family: next(),
                sql: next(),
                expected: next(),
            }
        })
        .collect()
}

fn over(sql: &str, table: &str) -> String {
    sql.replace(" t ", &format!(" {table} "))
        .replace(" t)", &format!(" {table})"))
}

async fn run(context: &SessionContext, sql: &str) {
    context.sql(sql).await.unwrap().collect().await.unwrap();
}

async fn answer(context: &SessionContext, sql: &str) -> String {
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let values: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).unwrap())
                .collect();
            rows.push(values.join("/"));
        }
    }
    format!("{} [{}]", rows.len(), rows.join(","))
}

async fn physical_plan(context: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    context
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

async fn plan_text(context: &SessionContext, sql: &str) -> String {
    let plan = physical_plan(context, sql).await;
    displayable(plan.as_ref()).indent(false).to_string()
}

async fn iceberg_table(session: &ReparkSession, root: &str) {
    session.register_memory_catalog("ice", root).await.unwrap();
    let handle = session.catalogs_snapshot().get("ice").cloned().unwrap();
    let namespace = NamespaceIdent::new("s".to_string());
    handle
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "v", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "k", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("i".to_string())
        .location(format!("{root}/s/i"))
        .schema(schema)
        .properties(HashMap::new())
        .build();
    handle.create_table(&namespace, creation).await.unwrap();
    session.refresh_catalog_provider("ice").await.unwrap();
    session
        .sql(&format!("INSERT INTO ice.s.i {SHUFFLED_ROWS}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

async fn sources(partitions: usize) -> Sources {
    let session = ReparkSession::builder()
        .target_partitions(partitions)
        .build()
        .unwrap();
    let context = session.context();
    run(context, &format!("CREATE VIEW t AS {SERIES_ROWS}")).await;
    run(context, &format!("CREATE TABLE m AS {SERIES_ROWS}")).await;
    let parquet = tempfile::tempdir().unwrap();
    let file = parquet.path().join("p.parquet");
    context
        .sql(SHUFFLED_ROWS)
        .await
        .unwrap()
        .write_parquet(
            file.to_str().unwrap(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .unwrap();
    context
        .register_parquet("p", file.to_str().unwrap(), ParquetReadOptions::default())
        .await
        .unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    iceberg_table(&session, warehouse.path().to_str().unwrap()).await;
    Sources {
        session,
        _parquet: parquet,
        _warehouse: warehouse,
    }
}

fn stock_context(session: &ReparkSession) -> SessionContext {
    let state = session.context().state();
    let rules = state
        .physical_optimizers()
        .iter()
        .map(|rule| {
            if rule.name() == ENFORCE_SORTING_RULE_NAME {
                Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule + Send + Sync>
            } else {
                Arc::clone(rule)
            }
        })
        .collect();
    SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(rules)
            .build(),
    )
}

async fn wrong_cells(context: &SessionContext, table: &str, family: Option<&str>) -> Vec<String> {
    let mut wrong = Vec::new();
    for cell in cells() {
        if family.is_some_and(|family| family != cell.family) {
            continue;
        }
        let got = answer(context, &over(cell.sql, table)).await;
        if got != cell.expected {
            wrong.push(format!(
                "{} on {table}: got {got}, Spark answers {}",
                cell.name, cell.expected
            ));
        }
    }
    wrong
}

async fn wrong_on_memory_table(partitions: usize, family: &str) -> Vec<String> {
    let sources = sources(partitions).await;
    wrong_cells(sources.session.context(), "m", Some(family)).await
}

async fn wrong_at_each_partition_count(table: &str, family: Option<&str>) -> Vec<String> {
    let mut wrong = Vec::new();
    for partitions in PARTITION_COUNTS {
        let sources = sources(partitions).await;
        for cell in wrong_cells(sources.session.context(), table, family).await {
            wrong.push(format!("{partitions} partitions: {cell}"));
        }
    }
    wrong
}

#[test]
fn grid_fixture_carries_every_recorded_cell() {
    let cells = cells();
    let count = |family: &str| cells.iter().filter(|cell| cell.family == family).count();
    assert_eq!(
        (
            count(ONE_PARTITION),
            count(EVERY_PARTITION),
            count(CONTROL),
            cells.len()
        ),
        (12, 9, 52, 73)
    );
    assert!(
        cells
            .iter()
            .any(|cell| cell.name == "reported" && over(cell.sql, "m") == REPORTED)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_partition_family_answers_spark_rows_at_one_partition() {
    assert_eq!(
        wrong_on_memory_table(1, ONE_PARTITION).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_partition_family_answers_spark_rows_at_two_partitions() {
    assert_eq!(
        wrong_on_memory_table(2, ONE_PARTITION).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_partition_family_answers_spark_rows_at_sixteen_partitions() {
    assert_eq!(
        wrong_on_memory_table(16, ONE_PARTITION).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_partition_family_answers_spark_rows_at_each_partition_count() {
    assert_eq!(
        wrong_at_each_partition_count("m", Some(EVERY_PARTITION)).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn control_cells_keep_spark_rows_at_each_partition_count() {
    assert_eq!(
        wrong_at_each_partition_count("m", Some(CONTROL)).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn grid_answers_spark_rows_over_a_sorted_series_view() {
    assert_eq!(
        wrong_at_each_partition_count("t", None).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn grid_answers_spark_rows_over_a_parquet_scan() {
    assert_eq!(
        wrong_at_each_partition_count("p", None).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn grid_answers_spark_rows_over_an_iceberg_scan() {
    assert_eq!(
        wrong_at_each_partition_count("ice.s.i", None).await,
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_enforce_sorting_still_loses_the_rows_at_one_partition_only() {
    let one = sources(1).await;
    assert_eq!(answer(&stock_context(&one.session), REPORTED).await, "0 []");
    assert_eq!(answer(one.session.context(), REPORTED).await, REPORTED_ROWS);
    let sixteen = sources(16).await;
    assert_eq!(
        answer(&stock_context(&sixteen.session), REPORTED).await,
        REPORTED_ROWS
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_enforce_sorting_still_sorts_below_an_offset() {
    for partitions in PARTITION_COUNTS {
        let sources = sources(partitions).await;
        assert_eq!(
            answer(&stock_context(&sources.session), SORT_ABOVE_OFFSET).await,
            "10 [10,9,8,7,6,5,4,3,2,1]",
            "{partitions} partitions"
        );
        assert_eq!(
            answer(sources.session.context(), SORT_ABOVE_OFFSET).await,
            SORT_ABOVE_OFFSET_ROWS,
            "{partitions} partitions"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reported_plan_keeps_the_inner_top_k_as_wide_as_skip_plus_fetch() {
    let sources = sources(1).await;
    let guarded = plan_text(sources.session.context(), REPORTED).await;
    assert!(
        guarded.contains("GlobalLimitExec: skip=4990, fetch=5"),
        "{guarded}"
    );
    assert!(guarded.contains("TopK(fetch=4995)"), "{guarded}");
    let stock = plan_text(&stock_context(&sources.session), REPORTED).await;
    assert!(
        stock.contains("GlobalLimitExec: skip=4990, fetch=0"),
        "{stock}"
    );
    assert!(stock.contains("TopK(fetch=4990)"), "{stock}");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_seal_survives_into_a_final_plan() {
    for partitions in [1, 16] {
        let sources = sources(partitions).await;
        for cell in cells() {
            for table in ["m", "t"] {
                let text = plan_text(sources.session.context(), &over(cell.sql, table)).await;
                assert!(!text.contains(SEAL_NAME), "{}: {text}", cell.name);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn statements_without_offset_plan_exactly_as_stock_datafusion() {
    let statements = [
        "SELECT 1",
        "SELECT v FROM m ORDER BY v LIMIT 5",
        "SELECT v FROM m ORDER BY v",
        "SELECT v FROM (SELECT v FROM m ORDER BY v LIMIT 5) ORDER BY v DESC",
        "SELECT k, count(*) AS c FROM m GROUP BY k ORDER BY c DESC, k LIMIT 3",
        "SELECT v, row_number() OVER (PARTITION BY k ORDER BY v) AS r FROM m ORDER BY v LIMIT 7",
        "SELECT a.v FROM m a JOIN t b ON a.v = b.v ORDER BY a.v LIMIT 5",
        "SELECT v FROM p ORDER BY v LIMIT 5",
        "SELECT v, k FROM (SELECT v, k FROM m ORDER BY v LIMIT 5) ORDER BY v, k",
        "SELECT v FROM (SELECT v FROM m LIMIT 100) ORDER BY v",
        "SELECT v FROM (SELECT v FROM m UNION ALL SELECT v FROM t) ORDER BY v LIMIT 5",
        "SELECT v FROM m LIMIT 5",
    ];
    for partitions in PARTITION_COUNTS {
        let sources = sources(partitions).await;
        let stock = stock_context(&sources.session);
        for sql in statements {
            assert_eq!(
                plan_text(sources.session.context(), sql).await,
                plan_text(&stock, sql).await,
                "{sql} at {partitions} partitions"
            );
        }
    }
}

#[tokio::test]
async fn guarded_rule_list_is_stock_datafusion_in_order_then_the_repark_rules() {
    let session = ReparkSession::new().unwrap();
    let state = session.context().state();
    let installed: Vec<&str> = state
        .physical_optimizers()
        .iter()
        .map(|rule| rule.name())
        .collect();
    let stock = PhysicalOptimizer::new();
    let stock: Vec<&str> = stock.rules.iter().map(|rule| rule.name()).collect();
    assert_eq!(installed[..stock.len()], stock[..]);
    assert_eq!(installed.len(), stock.len() + 3);
    assert_eq!(
        stock
            .iter()
            .filter(|name| **name == ENFORCE_SORTING_RULE_NAME)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn seal_reports_its_limit_and_neither_runs_nor_takes_a_child() {
    let sources = sources(1).await;
    let context = sources.session.context();
    let limit = physical_plan(context, "SELECT v FROM m ORDER BY v LIMIT 5 OFFSET 4990").await;
    let seal = sealed_for_test(Arc::clone(&limit));
    assert!(seal.children().is_empty());
    assert_eq!(seal.name(), SEAL_NAME);
    assert_eq!(
        displayable(seal.as_ref()).one_line().to_string().trim(),
        SEAL_NAME
    );
    assert_eq!(
        seal.output_ordering().map(ToString::to_string),
        limit.output_ordering().map(ToString::to_string)
    );
    assert!(seal.output_ordering().is_some());
    assert_eq!(
        seal.partition_statistics(None).unwrap().num_rows,
        limit.partition_statistics(None).unwrap().num_rows
    );
    let refusal = collect(Arc::clone(&seal), context.task_ctx())
        .await
        .unwrap_err()
        .to_string();
    assert!(refusal.contains("does not execute"), "{refusal}");
    let same = Arc::clone(&seal).with_new_children(Vec::new()).unwrap();
    assert!(Arc::ptr_eq(&same, &seal));
    let refusal = Arc::clone(&seal)
        .with_new_children(vec![limit])
        .unwrap_err()
        .to_string();
    assert!(refusal.contains("takes no child, got 1"), "{refusal}");
}
