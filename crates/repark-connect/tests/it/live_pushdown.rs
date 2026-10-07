use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::ScalarValue;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use repark_common::{SourceIdentity, SourceKind};
use repark_connect::{MAX_POSTGRES_DAYS, MIN_POSTGRES_DAYS, PostgresSource};

use crate::live_pg::{Cell, LIVE, url};
use crate::pushdown::{FixedZone, find_scan};

const SEED: &str = "
CREATE TYPE {s}.mood AS ENUM ('sad', 'ok', 'happy');
CREATE COLLATION {s}.und (provider = icu, locale = 'und');
CREATE COLLATION {s}.und_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
CREATE TABLE {s}.edges (
  id int8 PRIMARY KEY, small int2, qty int4, big int8, amount numeric(10,2), ratio numeric,
  paid bool, day date, at timestamptz, wall timestamp, note text, code varchar(20),
  padded char(5), score float8, icu text COLLATE {s}.und, folded text COLLATE {s}.und_ci,
  token uuid, mood {s}.mood, span interval
);
INSERT INTO {s}.edges (id, score)
  SELECT g, CASE WHEN g < 115 THEN 0.5 ELSE 2.0 END FROM generate_series(100, 119) g;
INSERT INTO {s}.edges (id) VALUES (1);
INSERT INTO {s}.edges VALUES
  (2, 32767, 1, 9223372036854775807, 100.00, 0.0000000000000000005, true, '4714-11-24 BC',
   '4714-11-24 00:00:00+00 BC', '2024-11-03 01:30', 'B', 'B', 'a', 'NaN', 'a', 'abc',
   'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'sad', '1 day'),
  (3, -32768, 2, -9223372036854775808, 100.01, 0.000000000000000001, false, '0001-01-01',
   '2024-03-10 12:00:00+00', '2024-11-03 07:00', 'a', 'a', 'b', '-0', 'B', 'ABC', NULL, 'ok',
   '2 days'),
  (4, 0, 3, 0, -0.50, -1.5, NULL, '2024-03-10', '1970-01-01 00:00:00+00', '1970-01-01', 'é',
   'é', 'a  ', 0, 'A', 'abz', NULL, 'happy', NULL),
  (5, 1, 4, 1, 99.99, 2, true, '5874897-12-31', '294247-01-10 04:00:54.775807+00',
   '2000-01-01', 'abc', 'a%b', 'c', 1.5, 'b', NULL, NULL, NULL, NULL),
  (6, 2, 5, 2, 0, 0, false, '1999-12-31', '2000-01-01 00:00:00+00', '1999-12-31 23:59:59.999999',
   'ABC', 'a\\b', NULL, -1, NULL, NULL, NULL, NULL, NULL),
  (7, 3, 6, 3, NULL, NULL, NULL, NULL, NULL, NULL, 'a%b', 'axb', NULL, NULL, NULL, NULL, NULL,
   NULL, NULL),
  (8, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'a\\b', '', NULL, NULL, NULL, NULL,
   NULL, NULL, NULL),
  (9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, NULL, NULL,
   NULL, NULL, NULL);
";

const COLUMNS: [&str; 18] = [
    "small", "qty", "big", "amount", "ratio", "paid", "day", "at", "wall", "note", "code",
    "padded", "score", "icu", "folded", "token", "mood", "span",
];

enum Filter<'a> {
    Sql(&'a str),
    Expr(Expr),
}

struct Edges {
    cell: Cell,
    table: String,
    on: SessionContext,
    off: SessionContext,
}

fn mount(cell: &Cell, extra: &[(&str, &str)], pushdown: &str) -> SessionContext {
    let mut props = BTreeMap::from([
        ("url".to_string(), url()),
        ("sslmode".to_string(), "disable".to_string()),
        ("application_name".to_string(), cell.app.clone()),
        ("pushdown_predicate".to_string(), pushdown.to_string()),
    ]);
    for (key, value) in extra {
        props.insert((*key).to_string(), (*value).to_string());
    }
    let identity = SourceIdentity::unassigned("pg".to_string(), SourceKind::Postgres);
    let zone = Arc::new(FixedZone {
        offset_micros: -5 * 3_600_000_000,
        label: "-05:00",
    });
    let context = SessionContext::new();
    context.register_catalog("pg", PostgresSource::mount(&identity, props, zone));
    context
}

impl Edges {
    async fn open(extra: &[(&str, &str)]) -> Edges {
        let cell = Cell::open().await;
        cell.sql(&SEED.replace("{s}", &cell.schema)).await;
        Edges {
            table: format!("pg.{}.edges", cell.schema),
            on: mount(&cell, extra, "true"),
            off: mount(&cell, extra, "false"),
            cell,
        }
    }

    async fn frame(&self, context: &SessionContext, filter: &Filter<'_>) -> DataFrame {
        let frame = match filter {
            Filter::Sql(sql) => {
                let statement = format!("SELECT * FROM {} WHERE {sql}", self.table);
                context.sql(&statement).await.expect(LIVE)
            }
            Filter::Expr(expr) => {
                let table = context.table(self.table.as_str()).await.expect(LIVE);
                table.filter(expr.clone()).expect("filter")
            }
        };
        frame
            .select_columns(&["id"])
            .expect("id")
            .sort(vec![col("id").sort(true, false)])
            .expect("sort")
    }

    async fn rows(&self, context: &SessionContext, filter: &Filter<'_>) -> (Vec<i64>, bool) {
        let frame = self.frame(context, filter).await;
        let plan = frame.create_physical_plan().await.expect(LIVE);
        let scanned = find_scan(&plan).is_some();
        let batches = collect(plan, context.task_ctx()).await.expect(LIVE);
        (ids(&batches), scanned)
    }

    async fn ids(&self, context: &SessionContext, filter: &Filter<'_>) -> (Vec<i64>, Split) {
        let frame = self.frame(context, filter).await;
        let plan = frame.create_physical_plan().await.expect(LIVE);
        let split = Split::of(&plan);
        let batches = collect(plan, context.task_ctx()).await.expect(LIVE);
        (ids(&batches), split)
    }

    async fn check(&self, filter: Filter<'_>, pushed: usize, residual: usize) -> Vec<i64> {
        let (on, split) = self.ids(&self.on, &filter).await;
        let (off, unpushed) = self.ids(&self.off, &filter).await;
        assert_eq!(on, off, "rows differ with pushdown: {split:?}");
        assert_eq!(
            (split.pushed.len(), split.residual.len()),
            (pushed, residual),
            "{split:?}"
        );
        assert!(
            unpushed.pushed.is_empty(),
            "pushdown_predicate = false pushed {unpushed:?}"
        );
        on
    }

    async fn sql(&self, filter: &str, pushed: usize, residual: usize) -> Vec<i64> {
        self.check(Filter::Sql(filter), pushed, residual).await
    }

    async fn close(self) {
        self.cell.close().await;
    }
}

#[derive(Debug)]
struct Split {
    pushed: Vec<String>,
    residual: Vec<String>,
    limit: Option<u64>,
}

impl Split {
    fn of(plan: &Arc<dyn ExecutionPlan>) -> Split {
        let scan = find_scan(plan).expect("one Postgres scan");
        Split {
            pushed: scan
                .pushed_filters()
                .iter()
                .map(ToString::to_string)
                .collect(),
            residual: scan
                .residual_filters()
                .iter()
                .map(ToString::to_string)
                .collect(),
            limit: scan.pushed_limit(),
        }
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p01_null_tests_push_live() {
    let edges = Edges::open(&[]).await;
    for column in COLUMNS {
        let nulls = edges.sql(&format!("{column} IS NULL"), 1, 0).await;
        assert!(nulls.contains(&1), "{column}: {nulls:?}");
        let values = edges.sql(&format!("{column} IS NOT NULL"), 1, 0).await;
        assert!(!values.contains(&1), "{column}: {values:?}");
    }
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p02_boolean_tests_push_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("paid", 1, 0).await, [2, 5]);
    assert_eq!(edges.sql("NOT paid", 1, 0).await, [3, 6]);
    let not_true = edges.sql("paid IS NOT TRUE", 1, 0).await;
    assert!(
        not_true.contains(&4) && not_true.contains(&3),
        "{not_true:?}"
    );
    for filter in [
        "paid IS FALSE",
        "paid IS NOT FALSE",
        "paid IS UNKNOWN",
        "paid = false",
    ] {
        edges.sql(filter, 1, 0).await;
    }
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p03_integer_comparison_pushes_cross_width_live() {
    let edges = Edges::open(&[]).await;
    assert!(edges.sql("small = 100000", 1, 0).await.is_empty());
    assert_eq!(edges.sql("small <> -100000 AND small < 0", 2, 0).await, [3]);
    assert_eq!(edges.sql("big = 9223372036854775807", 1, 0).await, [2]);
    assert_eq!(edges.sql("big <= -9223372036854775808", 1, 0).await, [3]);
    let distinct = edges.sql("qty IS DISTINCT FROM 3", 1, 0).await;
    assert!(
        distinct.contains(&1) && !distinct.contains(&4),
        "{distinct:?}"
    );
    assert_eq!(edges.sql("qty IS NOT DISTINCT FROM 3", 1, 0).await, [4]);
    assert_eq!(edges.sql("5 > qty AND qty >= 3", 2, 0).await, [4, 5]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p04_decimal_comparison_pushes_live() {
    let edges = Edges::open(&[]).await;
    let finer = "amount >= CAST(100.005 AS DECIMAL(12,3))";
    assert_eq!(edges.sql(finer, 1, 0).await, [3]);
    assert_eq!(
        edges.sql("amount < CAST(-0.4 AS DECIMAL(2,1))", 1, 0).await,
        [4]
    );
    let rounded = "ratio = CAST(0.000000000000000001 AS DECIMAL(38,18))";
    assert_eq!(edges.sql(rounded, 1, 0).await, [2, 3]);
    assert_eq!(edges.sql("ratio > 1", 1, 0).await, [5]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p05_temporal_comparison_pushes_inside_range_live() {
    let edges = Edges::open(&[("prefer_timestamp_ntz", "true")]).await;
    assert_eq!(edges.sql("day >= DATE '2024-01-01'", 1, 0).await, [4, 5]);
    assert_eq!(edges.sql("day < DATE '1000-01-01'", 1, 0).await, [2, 3]);
    let instant = |micros| ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into()));
    let at = "at < TIMESTAMP '2000-01-01 00:00:00' AND at > TIMESTAMP '1900-01-01 00:00:00'";
    assert_eq!(edges.sql(at, 2, 0).await, [4]);
    let far = instant(253_402_300_800_000_000);
    assert_eq!(
        edges
            .check(Filter::Expr(col("at").gt(lit(far))), 1, 0)
            .await,
        [5]
    );
    let wall = "wall >= TIMESTAMP '1999-12-31 23:59:59.999999' AND wall < TIMESTAMP '2000-01-02'";
    assert_eq!(edges.sql(wall, 2, 0).await, [5, 6]);
    let first = i32::try_from(MIN_POSTGRES_DAYS).expect("days");
    let earliest = col("day").lt_eq(lit(ScalarValue::Date32(Some(first))));
    assert_eq!(edges.check(Filter::Expr(earliest), 1, 0).await, [2]);
    let before = col("day").lt(lit(ScalarValue::Date32(Some(first - 1))));
    assert!(edges.check(Filter::Expr(before), 0, 1).await.is_empty());
    let micros = MIN_POSTGRES_DAYS * 86_400_000_000;
    let first_instant = col("at").eq(lit(instant(micros)));
    assert_eq!(edges.check(Filter::Expr(first_instant), 1, 0).await, [2]);
    let earlier = col("at").gt(lit(instant(micros - 1)));
    assert_eq!(edges.check(Filter::Expr(earlier), 0, 1).await.len(), 5);
    let last = i32::try_from(MAX_POSTGRES_DAYS).expect("days");
    let latest = col("day").eq(lit(ScalarValue::Date32(Some(last))));
    assert_eq!(edges.check(Filter::Expr(latest), 1, 0).await, [5]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p06_text_comparison_is_code_point_order_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("note < 'a'", 1, 0).await, [2, 6, 9]);
    assert_eq!(edges.sql("icu < 'B'", 1, 0).await, [4]);
    assert_eq!(edges.sql("code > 'z'", 1, 0).await, [4]);
    assert_eq!(
        edges.sql("note >= 'a' AND note <= 'abc'", 2, 0).await,
        [3, 5, 7, 8]
    );
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p06b_text_equality_ignores_nondeterministic_collation_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("folded = 'abc'", 1, 0).await, [2]);
    assert_eq!(edges.sql("folded <> 'abc'", 1, 0).await, [3, 4]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p06c_nul_literal_stays_residual_live() {
    let edges = Edges::open(&[]).await;
    let nul = col("note").eq(lit("a\0b"));
    assert!(edges.check(Filter::Expr(nul), 0, 1).await.is_empty());
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p07_in_list_three_valued_live() {
    let edges = Edges::open(&[]).await;
    let null_item = Filter::Sql("qty NOT IN (1, NULL)");
    let (on, _) = edges.rows(&edges.on, &null_item).await;
    let (off, _) = edges.rows(&edges.off, &null_item).await;
    assert!(on.is_empty() && off.is_empty(), "{on:?} {off:?}");
    assert_eq!(
        edges.sql("qty IN (1, 2, 3, 4, 7)", 1, 0).await,
        [2, 3, 4, 5]
    );
    assert_eq!(edges.sql("qty NOT IN (1, 2, 3, 4)", 1, 0).await, [6, 7]);
    assert_eq!(
        edges.sql("note IN ('a', 'B', 'é', 'zz')", 1, 0).await,
        [2, 3, 4]
    );
    let items = (0..257)
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(edges.sql(&format!("qty IN ({items})"), 0, 1).await.len(), 6);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p08_between_renders_inclusive_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("qty BETWEEN 2 AND 4", 2, 0).await, [3, 4, 5]);
    assert_eq!(edges.sql("qty NOT BETWEEN 2 AND 4", 1, 0).await, [2, 6, 7]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p09_like_pushes_well_formed_patterns_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("note LIKE '_'", 1, 0).await, [2, 3, 4]);
    assert_eq!(edges.sql(r"code LIKE 'a\%b'", 1, 0).await, [5]);
    assert_eq!(edges.sql(r"note LIKE 'a\\b'", 1, 0).await, [8]);
    assert_eq!(edges.sql(r"note NOT LIKE 'a%'", 1, 0).await, [2, 4, 6, 9]);
    assert_eq!(edges.sql("note ILIKE 'a%'", 0, 1).await, [3, 5, 6, 7, 8]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p10_logic_pushes_only_exact_children_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(
        edges.sql("qty = 1 OR lower(note) = 'abc'", 0, 1).await,
        [2, 5, 6]
    );
    assert_eq!(edges.sql("qty = 1 OR note = 'abc'", 1, 0).await, [2, 5]);
    assert_eq!(
        edges
            .sql("NOT (qty = 1 OR note = 'abc') AND qty < 4", 3, 0)
            .await,
        [3, 4]
    );
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn p11_limit_pushes_only_without_residual_live() {
    let edges = Edges::open(&[]).await;
    for context in [&edges.on, &edges.off] {
        let residual = format!("SELECT id FROM {} WHERE score > 1 LIMIT 5", edges.table);
        let plan = context.sql(&residual).await.expect(LIVE);
        let plan = plan.create_physical_plan().await.expect(LIVE);
        assert_eq!(Split::of(&plan).limit, None);
        let batches = collect(plan, context.task_ctx()).await.expect(LIVE);
        assert_eq!(ids(&batches).len(), 5, "LIMIT 5 over a residual filter");
    }
    let exact = format!("SELECT id FROM {} WHERE qty > 0 LIMIT 3", edges.table);
    let plan = edges.on.sql(&exact).await.expect(LIVE);
    let plan = plan.create_physical_plan().await.expect(LIVE);
    assert_eq!(Split::of(&plan).limit, Some(3));
    let batches = collect(plan, edges.on.task_ctx()).await.expect(LIVE);
    assert_eq!(ids(&batches).len(), 3);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn r01_float_comparisons_stay_residual_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("score = 0", 0, 1).await, [4]);
    let above = edges.sql("score > 1.0", 0, 1).await;
    assert!(
        above.contains(&2) && above.contains(&5),
        "NaN sorts above: {above:?}"
    );
    assert_eq!(edges.sql("score < 0", 0, 1).await, [3, 6]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn r02_bpchar_comparisons_stay_residual_live() {
    let edges = Edges::open(&[]).await;
    assert!(edges.sql("padded = 'a'", 0, 1).await.is_empty());
    assert_eq!(edges.sql("padded = 'a    '", 0, 1).await, [2, 4]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn r03_ltz_timestamp_comparisons_stay_residual_live() {
    let edges = Edges::open(&[]).await;
    let instant =
        ScalarValue::TimestampMicrosecond(Some(1_730_613_600_000_000), Some("UTC".into()));
    let after = col("wall").gt(lit(instant));
    assert_eq!(edges.check(Filter::Expr(after), 0, 1).await, [2, 3]);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn r04_functions_arithmetic_and_casts_stay_residual_live() {
    let edges = Edges::open(&[]).await;
    assert_eq!(edges.sql("big + 1 > 5", 0, 1).await, Vec::<i64>::new());
    assert_eq!(edges.sql("lower(note) = 'abc'", 0, 1).await, [5, 6]);
    assert_eq!(edges.sql("mood = 'happy'", 0, 1).await, [4]);
    assert_eq!(
        edges.sql("CAST(span AS VARCHAR) = '1 day'", 0, 1).await,
        [2]
    );
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn r05_pushdown_predicate_false_pushes_nothing_live() {
    let edges = Edges::open(&[]).await;
    for filter in ["qty = 1", "note IS NULL", "paid"] {
        let (_, split) = edges.ids(&edges.off, &Filter::Sql(filter)).await;
        assert!(split.pushed.is_empty(), "{filter}: {split:?}");
        assert_eq!(split.residual.len(), 1, "{filter}");
    }
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn timestamp_columns_are_placed_in_the_session_zone_live() {
    let edges = Edges::open(&[]).await;
    let ntz = mount(&edges.cell, &[("prefer_timestamp_ntz", "true")], "true");
    let statement = format!("SELECT wall FROM {} WHERE id = 2", edges.table);
    let placed = edges
        .on
        .sql(&statement)
        .await
        .expect(LIVE)
        .collect()
        .await
        .expect(LIVE);
    let wall = ntz
        .sql(&statement)
        .await
        .expect(LIVE)
        .collect()
        .await
        .expect(LIVE);
    let column = placed[0].column(0);
    let zone = DataType::Timestamp(TimeUnit::Microsecond, Some("-05:00".into()));
    assert_eq!(column.data_type(), &zone);
    let placed = column.as_primitive::<TimestampMicrosecondType>().value(0);
    let wall = wall[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(placed - wall, 5 * 3_600_000_000);
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn explain_analyze_reports_rows_bytes_and_time_per_scan_live() {
    let edges = Edges::open(&[]).await;
    let statement = format!(
        "EXPLAIN ANALYZE SELECT id FROM {} WHERE qty > 0",
        edges.table
    );
    let batches = edges
        .on
        .sql(&statement)
        .await
        .expect(LIVE)
        .collect()
        .await
        .expect(LIVE);
    let text = arrow::util::pretty::pretty_format_batches(&batches)
        .expect("text")
        .to_string();
    let line = text
        .lines()
        .find(|line| line.contains("PostgresScanExec:"))
        .expect("the scan line");
    for metric in [
        "output_rows=6",
        "output_batches=1",
        "bytes_received=",
        "time_to_first_byte=",
        "elapsed_compute=",
    ] {
        assert!(line.contains(metric), "{metric} missing: {line}");
    }
    let statement = format!("SELECT id FROM {} WHERE qty > 0", edges.table);
    let plan = edges.on.sql(&statement).await.expect(LIVE);
    let plan = plan.create_physical_plan().await.expect(LIVE);
    collect(Arc::clone(&plan), edges.on.task_ctx())
        .await
        .expect(LIVE);
    let scan = find_scan(&plan).expect("the scan");
    let metrics = scan.metrics().expect("metrics");
    let sum = |name: &str| {
        let values = metrics
            .iter()
            .filter(|metric| metric.value().name() == name);
        values
            .map(|metric| metric.value().as_usize())
            .reduce(|a, b| a + b)
    };
    assert_eq!(sum("output_rows"), Some(6), "{metrics}");
    assert!(
        sum("bytes_received").is_some_and(|bytes| bytes > 19),
        "{metrics}"
    );
    assert!(
        sum("time_to_first_byte").is_some_and(|nanos| nanos > 0),
        "{metrics}"
    );
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_batch_past_the_memory_pool_is_resources_exhausted_live() {
    let cell = Cell::open().await;
    let table = format!("{}.many", cell.schema);
    cell.sql(&format!(
        "CREATE TABLE {table} AS SELECT g::int8 AS id FROM generate_series(1, 50000) g"
    ))
    .await;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(16 * 1024, 1.0)
        .build_arc()
        .expect("runtime");
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let mounted = mount(&cell, &[], "true");
    let catalog = mounted.catalog("pg").expect("pg");
    context.register_catalog("pg", catalog);
    let statement = format!("SELECT id FROM pg.{table}");
    let frame = context.sql(&statement).await.expect(LIVE);
    let error = frame
        .collect()
        .await
        .expect_err("an 8192-row batch exceeds 16 KiB");
    assert!(error.to_string().contains("PostgresScan"), "{error}");
    let roomy = mount(&cell, &[], "true");
    let rows = roomy
        .sql(&statement)
        .await
        .expect(LIVE)
        .collect()
        .await
        .expect(LIVE);
    assert_eq!(ids(&rows).len(), 50_000);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_missing_relation_is_table_not_found_live() {
    let edges = Edges::open(&[]).await;
    let missing = format!("SELECT * FROM pg.{}.absent", edges.cell.schema);
    let error = edges.on.sql(&missing).await.expect_err("no such relation");
    assert!(error.to_string().contains("not found"), "{error}");
    let refused = format!(
        "SELECT * FROM pg.{}.\"{}\"",
        edges.cell.schema,
        "x".repeat(64)
    );
    let error = edges
        .on
        .sql(&refused)
        .await
        .expect_err("a 64-byte name refuses");
    assert!(error.to_string().contains("63-byte limit"), "{error}");
    edges.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn pushed_compare_keeps_the_index_live() {
    let cell = Cell::open().await;
    let table = format!("{}.wide", cell.schema);
    cell.sql(&format!(
        "CREATE TABLE {table} AS SELECT g::int4 AS id, md5(g::text) AS pad \
         FROM generate_series(1, 1000000) g; CREATE INDEX ON {table} (id); ANALYZE {table}"
    ))
    .await;
    cell.sql("SELECT pg_catalog.set_config('repark.p0', '424242', false)")
        .await;
    let explained = cell
        .admin
        .query(
            &format!(
                "EXPLAIN SELECT id FROM {table} WHERE id OPERATOR(pg_catalog.=) \
                 pg_catalog.current_setting('repark.p0')::pg_catalog.int4"
            ),
            &[],
        )
        .await
        .expect("explain");
    let lines: Vec<String> = explained.iter().map(|row| row.get(0)).collect();
    println!("D-M3 plan:\n{}", lines.join("\n"));
    assert!(lines.iter().any(|line| line.contains("Index")), "{lines:?}");
    cell.close().await;
}
