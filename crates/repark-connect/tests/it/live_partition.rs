use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{AsArray, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::Int64Type;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_common::{SourceIdentity, SourceKind};
use repark_connect::{
    ConnectError, LaneStream, PartitionSpec, PostgresSource, ScanMeter, ScanRequest, SettingsDoor,
    ValueRefusal, scan_lanes, stride_cuts, strides,
};

use crate::live_pg::{Cell, LIVE, Reader, url};
use crate::pushdown::{FixedZone, find_scan};

const ROWS: i64 = 5000;

const WIDE: &str = "
CREATE TABLE {s}.wide (
  id int8 PRIMARY KEY, k int4, small int2, amount numeric(12,2), ratio numeric, day date,
  wall timestamp, at timestamptz, span interval, token uuid, doc json, docb jsonb, note text
);
INSERT INTO {s}.wide
  SELECT g,
    CASE WHEN g % 97 = 0 THEN NULL WHEN g % 89 = 0 THEN -100000 - g
         WHEN g % 83 = 0 THEN 100000 + g ELSE (g * 7919) % 1000 END,
    CASE WHEN g % 101 = 0 THEN NULL ELSE (g % 300)::int2 - 150 END,
    g * 1.25, g / 7.0, DATE '2000-01-01' + (g % 9000)::int,
    TIMESTAMP '2001-01-01' + g * INTERVAL '1 minute',
    TIMESTAMPTZ '2001-01-01 00:00:00+00' + g * INTERVAL '1 second', g * INTERVAL '1 hour',
    md5(g::text)::uuid, json_build_object('g', g), jsonb_build_object('g', g), 'n' || g
  FROM generate_series(1, 5000) g;
";

fn spec(column: &str, lower: i64, upper: i64, count: i64) -> PartitionSpec {
    PartitionSpec {
        column: column.to_string(),
        lower_bound: lower.to_string(),
        upper_bound: upper.to_string(),
        num_partitions: count,
    }
}

fn source(cell: &Cell, extra: &[(&str, &str)]) -> Arc<PostgresSource> {
    let mut props = BTreeMap::from([
        ("url".to_string(), url()),
        ("sslmode".to_string(), "disable".to_string()),
        ("application_name".to_string(), cell.app.clone()),
    ]);
    for (key, value) in extra {
        props.insert((*key).to_string(), (*value).to_string());
    }
    let identity = SourceIdentity::unassigned("pg".to_string(), SourceKind::Postgres);
    let zone = Arc::new(FixedZone {
        offset_micros: -5 * 3_600_000_000,
        label: "-05:00",
    });
    PostgresSource::new(&identity, props, SettingsDoor::ReparkToml, zone)
}

async fn register(
    context: &SessionContext,
    source: &Arc<PostgresSource>,
    cell: &Cell,
    name: &str,
    relation: &str,
    partition: Option<&PartitionSpec>,
) {
    let table = source.resolve(cell.relation(relation)).await.expect(LIVE);
    let table = match partition {
        Some(partition) => table.partitioned(partition).expect("partitioned"),
        None => table,
    };
    context
        .register_table(name, Arc::new(table))
        .expect("register");
}

static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn plan(context: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    let frame = context.sql(sql).await.expect(sql);
    frame.create_physical_plan().await.expect(sql)
}

async fn read(context: &SessionContext, sql: &str) -> RecordBatch {
    let plan = plan(context, sql).await;
    let schema = plan.schema();
    let batches = collect(plan, context.task_ctx()).await.expect(sql);
    concat_batches(&schema, &batches).expect("one batch")
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .iter()
        .flatten()
        .collect()
}

async fn drained(cell: &Cell, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while cell.backends().await != expected {
        assert!(
            Instant::now() < deadline,
            "{} backends, expected {expected}",
            cell.backends().await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn none_busy(cell: &Cell) {
    let sql = "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND state <> \
               'idle' AND backend_type = 'client backend'";
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let busy: i64 = cell
            .admin
            .query_one(sql, &[&cell.app])
            .await
            .expect(sql)
            .get(0);
        if busy == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "{busy} backends still busy");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_partitioned_read_equals_the_unpartitioned_read_across_the_types() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    cell.sql(&WIDE.replace("{s}", &cell.schema)).await;
    let plans = [
        (spec("k", 0, 1000, 4), 4, 4),
        (spec("id", 1, ROWS, 8), 8, 4),
        (spec("small", -150, 150, 3), 3, 3),
        (spec("k", 0, 1000, 16), 16, 4),
        (spec("k", 400, 600, 4), 4, 4),
        (spec("k", 2_000_000, 3_000_000, 4), 4, 4),
        (spec("id", i64::MIN, i64::MAX, 7), 7, 4),
        (spec("K", 0, 3, 10), 3, 3),
    ];
    let context = SessionContext::new();
    let shared = source(&cell, &[]);
    register(&context, &shared, &cell, "plain", "wide", None).await;
    let expected = read(&context, "SELECT * FROM plain ORDER BY id").await;
    assert_eq!(expected.num_rows(), usize::try_from(ROWS).expect("rows"));
    assert_eq!(expected.num_columns(), 13);
    for (index, (partition, strides, connections)) in plans.iter().enumerate() {
        let name = format!("parts{index}");
        register(&context, &shared, &cell, &name, "wide", Some(partition)).await;
        let sql = format!("SELECT * FROM {name} ORDER BY id");
        let planned = plan(&context, &sql).await;
        let scan = find_scan(&planned).expect("one Postgres scan");
        assert_eq!(scan.strides().len(), *strides, "{partition:?}");
        assert_eq!(scan.max_connections(), *connections, "{partition:?}");
        let got = read(&context, &sql).await;
        assert_eq!(got, expected, "{partition:?}");
    }
    cell.sql(&format!(
        "CREATE TABLE {}.clock (id int8, t time)",
        cell.schema
    ))
    .await;
    let clock = shared.resolve(cell.relation("clock")).await;
    assert!(
        matches!(clock, Err(ConnectError::UnmappedType { row, .. }) if row == "CONNECT-DECL-pg-time"),
        "`time` refuses at resolution, before any stride is planned"
    );
    cell.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn null_and_out_of_bounds_rows_arrive_exactly_once() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    cell.sql(&WIDE.replace("{s}", &cell.schema)).await;
    let context = SessionContext::new();
    let shared = source(&cell, &[]);
    let partition = spec("k", 0, 1000, 4);
    register(&context, &shared, &cell, "parts", "wide", Some(&partition)).await;
    let all = read(&context, "SELECT id FROM parts").await;
    let mut seen = ids(&all);
    seen.sort_unstable();
    assert_eq!(seen, (1..=ROWS).collect::<Vec<i64>>(), "every id once");
    let nulls = cell
        .count(&format!(
            "SELECT count(*) FROM {}.wide WHERE k IS NULL",
            cell.schema
        ))
        .await;
    let outside = cell
        .count(&format!(
            "SELECT count(*) FROM {}.wide WHERE k < 0 OR k > 1000",
            cell.schema
        ))
        .await;
    assert!(nulls > 40 && outside > 100, "{nulls} {outside}");
    let got_nulls = read(&context, "SELECT id FROM parts WHERE k IS NULL").await;
    assert_eq!(i64::try_from(got_nulls.num_rows()).expect("rows"), nulls);
    let got_outside = read(&context, "SELECT id FROM parts WHERE k < 0 OR k > 1000").await;
    assert_eq!(
        i64::try_from(got_outside.num_rows()).expect("rows"),
        outside
    );
    cell.close().await;
}

const MOVING: &str = "
CREATE TABLE {s}.moving (id int8 PRIMARY KEY, k int4, pad text);
INSERT INTO {s}.moving
  SELECT g, CASE WHEN g % 50 = 0 THEN NULL ELSE g % 1000 END, repeat('x', 200)
  FROM generate_series(1, 40000) g;
";

const WRITER: &str = "
UPDATE {s}.moving SET k = (k + 500) % 1000 WHERE id % 3 = 0;
DELETE FROM {s}.moving WHERE id % 7 = 0;
INSERT INTO {s}.moving SELECT g, g % 1000, 'new' FROM generate_series(40001, 45000) g;
UPDATE {s}.moving SET k = 10 WHERE k IS NULL;
";

fn lane_requests(resolved: &Arc<repark_connect::ResolvedSource>, count: i64) -> Vec<ScanRequest> {
    strides(&stride_cuts(0, 1000, count).expect("planned"))
        .iter()
        .map(|stride| {
            ScanRequest::new(Arc::clone(resolved))
                .project(&[0, 1])
                .expect("projection")
                .stride(1, *stride)
                .expect("stride")
        })
        .collect()
}

async fn drain(lanes: Vec<LaneStream>, first: Vec<RecordBatch>) -> Vec<(i64, Option<i32>)> {
    let mut batches = first;
    for mut lane in lanes {
        while let Some(batch) = lane.next().await {
            batches.push(batch.expect(LIVE));
        }
    }
    let mut rows: Vec<(i64, Option<i32>)> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch.column(0).as_primitive::<Int64Type>();
            let keys = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int32Type>();
            ids.values()
                .iter()
                .copied()
                .zip(keys.iter())
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort_unstable();
    rows
}

async fn snapshot_holds(max_lanes: usize, stride_count: i64) {
    let cell = Cell::open().await;
    cell.sql(&MOVING.replace("{s}", &cell.schema)).await;
    let reader = Reader::new(&cell.settings(&[("batch_rows", "500")]));
    let resolved = reader.resolve(cell.relation("moving")).await.expect(LIVE);
    let before = drain(
        Vec::new(),
        reader
            .read(
                ScanRequest::new(Arc::clone(&resolved))
                    .project(&[0, 1])
                    .expect("projection"),
            )
            .await
            .expect(LIVE),
    )
    .await;
    assert_eq!(before.len(), 40_000);
    let mut lanes = scan_lanes(
        Arc::clone(&reader.pool),
        lane_requests(&resolved, stride_count),
        max_lanes,
        reader.options,
        ScanMeter::default(),
    )
    .await
    .expect(LIVE);
    assert_eq!(lanes.len(), max_lanes);
    let first = lanes
        .first_mut()
        .expect("a lane")
        .next()
        .await
        .expect("a batch")
        .expect(LIVE);
    cell.sql(&WRITER.replace("{s}", &cell.schema)).await;
    let changed = cell
        .count(&format!("SELECT count(*) FROM {}.moving", cell.schema))
        .await;
    assert_ne!(changed, 40_000, "the writer committed between strides");
    let during = drain(lanes, vec![first]).await;
    assert_eq!(during.len(), before.len(), "no row twice, no row lost");
    assert_eq!(during, before, "the strides read one snapshot");
    drained(&cell, 1).await;
    assert_eq!(reader.pool.idle_count(), 1, "one connection pooled clean");
    let after = drain(
        Vec::new(),
        reader
            .read(
                ScanRequest::new(resolved)
                    .project(&[0, 1])
                    .expect("projection"),
            )
            .await
            .expect(LIVE),
    )
    .await;
    assert_eq!(
        i64::try_from(after.len()).expect("rows"),
        changed,
        "a pooled connection sees the present again"
    );
    cell.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_writer_between_strides_never_changes_a_partitioned_read() {
    let _turn = TURN.lock().await;
    snapshot_holds(4, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn one_connection_reads_its_strides_in_one_snapshot() {
    let _turn = TURN.lock().await;
    snapshot_holds(1, 4).await;
    snapshot_holds(2, 8).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn filters_projection_and_limit_compose_with_the_strides() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    cell.sql(&WIDE.replace("{s}", &cell.schema)).await;
    let context = SessionContext::new();
    let partition = spec("k", 0, 1000, 4);
    let off = [("pushdown_predicate", "false")];
    let shared = source(&cell, &[]);
    let unpushed = source(&cell, &off);
    register(&context, &shared, &cell, "plain", "wide", None).await;
    register(&context, &shared, &cell, "parts", "wide", Some(&partition)).await;
    register(
        &context,
        &unpushed,
        &cell,
        "unpushed",
        "wide",
        Some(&partition),
    )
    .await;
    for filter in [
        "k > 100 AND k < 700",
        "k IS NULL OR small = 7",
        "amount > 1000 AND lower(note) LIKE 'n1%'",
        "id IN (1, 97, 89, 83, 4999) OR k = 500",
        "day >= DATE '2010-01-01' AND at < TIMESTAMPTZ '2001-01-01 00:30:00+00'",
        "k > 5000000",
    ] {
        let shape = |table: &str| {
            format!("SELECT note, id, amount FROM {table} WHERE {filter} ORDER BY id")
        };
        let expected = read(&context, &shape("plain")).await;
        assert_eq!(read(&context, &shape("parts")).await, expected, "{filter}");
        assert_eq!(
            read(&context, &shape("unpushed")).await,
            expected,
            "{filter}"
        );
    }
    let counted = read(&context, "SELECT count(*) FROM parts").await;
    assert_eq!(ids(&counted), [ROWS], "an empty projection counts rows");

    let limited = plan(&context, "SELECT id FROM parts LIMIT 7").await;
    let scan = find_scan(&limited).expect("one Postgres scan");
    assert_eq!(scan.pushed_limit(), Some(7));
    assert!(
        scan.strides()
            .iter()
            .all(|stride| stride.statement().copy.contains(" LIMIT 7)"))
    );
    let from_scan = collect(scan_node(&limited), context.task_ctx())
        .await
        .expect(LIVE);
    let scan_rows: usize = from_scan.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(scan_rows, 7, "the scan caps the strides at the limit");
    let seven = read(&context, "SELECT id FROM parts LIMIT 7").await;
    let mut seven = ids(&seven);
    seven.sort_unstable();
    seven.dedup();
    assert_eq!(seven.len(), 7);
    assert!(seven.iter().all(|id| (1..=ROWS).contains(id)));

    let filtered = read(&context, "SELECT id FROM parts WHERE k < 300 LIMIT 9").await;
    assert_eq!(filtered.num_rows(), 9);
    let residual = plan(
        &context,
        "SELECT id FROM parts WHERE lower(note) LIKE 'n4%' LIMIT 5",
    )
    .await;
    assert_eq!(
        find_scan(&residual).expect("scan").pushed_limit(),
        None,
        "a residual keeps the limit above the scan"
    );
    let residual_rows = read(
        &context,
        "SELECT id FROM parts WHERE lower(note) LIKE 'n4%' LIMIT 5",
    )
    .await;
    assert_eq!(residual_rows.num_rows(), 5);
    let beyond = read(&context, "SELECT id FROM parts LIMIT 100000").await;
    assert_eq!(beyond.num_rows(), usize::try_from(ROWS).expect("rows"));
    let none = read(&context, "SELECT id FROM parts LIMIT 0").await;
    assert_eq!(none.num_rows(), 0);
    let skipped = read(
        &context,
        "SELECT id FROM parts ORDER BY id LIMIT 5 OFFSET 4990",
    )
    .await;
    assert_eq!(ids(&skipped), [4991, 4992, 4993, 4994, 4995]);
    cell.close().await;
}

fn scan_node(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.name() == "PostgresScanExec" {
        return Arc::clone(plan);
    }
    plan.children()
        .into_iter()
        .map(scan_node)
        .next()
        .expect("one Postgres scan")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_refused_value_keeps_its_contract_in_every_stride() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.odd (id int8 PRIMARY KEY, k int4, ratio numeric);
         INSERT INTO {schema}.odd
           SELECT g, g, CASE WHEN g = 2600 THEN 'NaN'::numeric ELSE g END
           FROM generate_series(1, 4000) g;"
    ))
    .await;
    let context = SessionContext::new();
    let shared = source(&cell, &[]);
    let partition = spec("k", 0, 4000, 4);
    register(&context, &shared, &cell, "plain", "odd", None).await;
    register(&context, &shared, &cell, "parts", "odd", Some(&partition)).await;
    let refusal_of = |error: datafusion::error::DataFusionError| {
        let found = error.find_root();
        match found {
            datafusion::error::DataFusionError::External(inner) => inner
                .downcast_ref::<ConnectError>()
                .cloned()
                .expect("a ConnectError"),
            other => panic!("{other:?}"),
        }
    };
    let mut reasons = Vec::new();
    for table in ["plain", "parts"] {
        let planned = plan(&context, &format!("SELECT id, ratio FROM {table}")).await;
        let error = collect(planned, context.task_ctx())
            .await
            .expect_err("NaN refuses");
        match refusal_of(error) {
            ConnectError::UnrepresentableValue { column, reason, .. } => {
                assert_eq!(column.as_ref(), "ratio");
                reasons.push(reason);
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        reasons,
        [ValueRefusal::NumericNaN, ValueRefusal::NumericNaN]
    );

    let planned = plan(&context, "SELECT id, ratio FROM parts").await;
    let mut stream =
        datafusion::physical_plan::execute_stream(planned, context.task_ctx()).expect("stream");
    let mut delivered = Vec::new();
    let mut refused = false;
    while let Some(next) = stream.next().await {
        let Ok(batch) = next else {
            refused = true;
            assert!(
                stream.next().await.is_none(),
                "the stream ends at the error"
            );
            break;
        };
        delivered.extend(ids(&batch));
    }
    assert!(refused);
    assert!(
        !delivered.contains(&2600),
        "the refused row is never delivered"
    );
    let third_stride: Vec<i64> = delivered
        .iter()
        .copied()
        .filter(|id| (2000..3000).contains(id))
        .collect();
    assert!(
        third_stride.iter().all(|id| *id < 2600),
        "only the rows before the refusal leave its stride"
    );
    drop(stream);
    none_busy(&cell).await;
    assert!(
        cell.backends().await <= 1,
        "at most the one pooled connection of a stride that had finished"
    );
    let clean = read(&context, "SELECT id FROM parts WHERE id <> 2600").await;
    assert_eq!(clean.num_rows(), 3999, "the column is not selected");
    cell.close().await;
}

const BIG: &str = "
CREATE TABLE {s}.big (id int8 PRIMARY KEY, pad text);
INSERT INTO {s}.big SELECT g, repeat('y', 100) FROM generate_series(1, 400000) g;
";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn cancel_aborts_every_connection_and_leaves_no_backend() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    cell.sql(&BIG.replace("{s}", &cell.schema)).await;
    let context = SessionContext::new();
    let partition = spec("id", 1, 400_000, 4);
    let rows = [("batch_rows", "200")];
    let shared = source(&cell, &rows);
    register(&context, &shared, &cell, "parts", "big", Some(&partition)).await;
    let planned = plan(&context, "SELECT id, pad FROM parts").await;
    let mut stream =
        datafusion::physical_plan::execute_stream(planned, context.task_ctx()).expect("stream");
    stream.next().await.expect("a batch").expect(LIVE);
    assert_eq!(cell.backends().await, 4, "four connections were copying");
    drop(stream);
    drained(&cell, 0).await;
    none_busy(&cell).await;
    let again = read(&context, "SELECT count(*) FROM parts").await;
    assert_eq!(ids(&again), [400_000], "the source reads again afterwards");
    cell.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn num_partitions_above_the_pool_never_opens_past_it() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    cell.sql(&BIG.replace("{s}", &cell.schema)).await;
    let context = SessionContext::new();
    let partition = spec("id", 1, 400_000, 16);
    let narrow = [("pool_max_size", "2"), ("batch_rows", "1000")];
    let shared = source(&cell, &narrow);
    register(&context, &shared, &cell, "parts", "big", Some(&partition)).await;
    let planned = plan(&context, "SELECT id FROM parts").await;
    let scan = find_scan(&planned).expect("scan");
    assert_eq!((scan.strides().len(), scan.max_connections()), (16, 2));
    let mut stream =
        datafusion::physical_plan::execute_stream(planned, context.task_ctx()).expect("stream");
    let mut seen = Vec::new();
    let mut most = 0;
    let mut batches = 0;
    while let Some(batch) = stream.next().await {
        seen.extend(ids(&batch.expect(LIVE)));
        batches += 1;
        if batches % 40 == 0 {
            most = most.max(cell.backends().await);
        }
    }
    assert_eq!(most, 2, "sixteen strides ran on the pool's two connections");
    seen.sort_unstable();
    assert_eq!(seen, (1..=400_000).collect::<Vec<i64>>());
    drained(&cell, 1).await;
    let again = read(&context, "SELECT count(*) FROM parts").await;
    assert_eq!(ids(&again), [400_000]);
    drained(&cell, 1).await;
    cell.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_busy_pool_narrows_a_partitioned_read_and_never_fails_it() {
    let _turn = TURN.lock().await;
    let cell = Cell::open().await;
    cell.sql(&BIG.replace("{s}", &cell.schema)).await;
    let reader = Reader::new(&cell.settings(&[("pool_checkout_timeout_ms", "2000")]));
    let resolved = reader.resolve(cell.relation("big")).await.expect(LIVE);
    let held = reader.pool.checkout_up_to(3).await.expect(LIVE);
    assert_eq!(held.len(), 3);
    let requests: Vec<ScanRequest> = strides(&stride_cuts(1, 400_000, 8).expect("planned"))
        .iter()
        .map(|stride| {
            ScanRequest::new(Arc::clone(&resolved))
                .project(&[0])
                .expect("projection")
                .stride(0, *stride)
                .expect("stride")
        })
        .collect();
    let lanes = scan_lanes(
        Arc::clone(&reader.pool),
        requests.clone(),
        8,
        reader.options,
        ScanMeter::default(),
    )
    .await
    .expect("one free connection is enough");
    assert_eq!(
        lanes.len(),
        1,
        "it took the one free permit and did not wait"
    );
    let mut count = 0;
    for mut lane in lanes {
        while let Some(batch) = lane.next().await {
            count += batch.expect(LIVE).num_rows();
        }
    }
    assert_eq!(count, 400_000);
    let all = reader.pool.checkout().await.expect("the lane's connection");
    let exhausted = scan_lanes(
        Arc::clone(&reader.pool),
        requests,
        8,
        reader.options,
        ScanMeter::default(),
    )
    .await;
    assert!(
        matches!(exhausted, Err(ConnectError::PoolExhausted { .. })),
        "with no connection free the first checkout queues, then refuses"
    );
    drop((held, all));
    cell.close().await;
}
