use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, Float64Array, Int64Array, RecordBatch,
};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int64Type, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, LogicalPlanBuilder, Partitioning, TableProviderFilterPushDown, col,
};

use super::udf::{normalize_array, normalize_zero_call, normalize_zero_udf};
use crate::ReparkSession;
use crate::extension::{SessionExtension, SignedZeroPolicy, contains_normalize_zero};

const NEG_NAN_64: u64 = 0xfff8_0000_0000_0000;
const NEG_NAN_32: u32 = 0xffc0_0000;
const WRAP: &str = "__repark_normalize_zero__(";

fn doubles() -> [Option<f64>; 6] {
    [
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        Some(f64::from_bits(NEG_NAN_64)),
        None,
        Some(1.5),
    ]
}

fn others() -> [Option<f64>; 6] {
    [
        Some(0.0),
        Some(-0.0),
        Some(f64::from_bits(NEG_NAN_64)),
        Some(f64::NAN),
        Some(1.5),
        Some(1.5),
    ]
}

fn narrow(value: Option<f64>) -> Option<f32> {
    value.map(|value| {
        if value.to_bits() == NEG_NAN_64 {
            f32::from_bits(NEG_NAN_32)
        } else {
            value as f32
        }
    })
}

struct KeysOff;

impl SessionExtension for KeysOff {
    fn signed_zero_policy(&self) -> SignedZeroPolicy {
        SignedZeroPolicy {
            literals: true,
            keys: false,
        }
    }
}

fn seeded(session: ReparkSession) -> ReparkSession {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
        Field::new("e", DataType::Float64, true),
        Field::new("g", DataType::Float32, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(1..=6)),
        Arc::new(Float64Array::from(doubles().to_vec())),
        Arc::new(Float32Array::from(doubles().map(narrow).to_vec())),
        Arc::new(Float64Array::from(others().to_vec())),
        Arc::new(Float32Array::from(others().map(narrow).to_vec())),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("seed batch");
    for name in ["t", "u"] {
        let table =
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]]).expect("seed table");
        session
            .context()
            .register_table(name, Arc::new(table))
            .expect("register seed");
    }
    let ints = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let int_batch = RecordBatch::try_new(
        Arc::clone(&ints),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![0, 0, 1])),
            Arc::new(Int64Array::from(vec![5, -5, 2])),
        ],
    )
    .expect("int batch");
    let table = MemTable::try_new(ints, vec![vec![int_batch]]).expect("int table");
    session
        .context()
        .register_table("k", Arc::new(table))
        .expect("register k");
    session
}

fn session() -> ReparkSession {
    seeded(ReparkSession::builder().build().expect("core session"))
}

fn keys_off_session() -> ReparkSession {
    seeded(
        ReparkSession::builder()
            .with_extension(Arc::new(KeysOff))
            .build()
            .expect("keys-off session"),
    )
}

async fn ints(session: &ReparkSession, sql: &str) -> Vec<i64> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut out = Vec::new();
    for batch in &batches {
        let column = datafusion::arrow::compute::cast(batch.column(0), &DataType::Int64)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let column = column.as_primitive::<Int64Type>();
        out.extend((0..column.len()).map(|row| {
            if column.is_null(row) {
                -1
            } else {
                column.value(row)
            }
        }));
    }
    out
}

async fn optimized(session: &ReparkSession, sql: &str) -> LogicalPlan {
    session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .into_optimized_plan()
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn key_cases(d: &str) -> Vec<(String, Vec<i64>, bool)> {
    let e = if d == "d" { "e" } else { "g" };
    let rows = |sql: &str| sql.replace("{d}", d).replace("{e}", e);
    vec![
        (
            rows("SELECT count(*) FROM (SELECT {d} FROM t GROUP BY {d})"),
            vec![4],
            false,
        ),
        (
            rows("SELECT count(*) FROM (SELECT {d}, count(*) FROM t GROUP BY ROLLUP({d}))"),
            vec![5],
            false,
        ),
        (
            rows(
                "SELECT count(*) FROM (SELECT {d}, id % 2 AS p, count(*) FROM t GROUP BY GROUPING SETS (({d}), ({d}, id % 2)))",
            ),
            vec![10],
            false,
        ),
        (
            rows("SELECT count(*) FROM (SELECT DISTINCT {d} FROM t)"),
            vec![4],
            false,
        ),
        (rows("SELECT count(DISTINCT {d}) FROM t"), vec![3], false),
        (
            rows("SELECT count(DISTINCT {d}) * 10 + count(DISTINCT id % 2) FROM t"),
            vec![32],
            false,
        ),
        (
            rows("SELECT count(*) FROM t JOIN u ON t.{d} = u.{d}"),
            vec![9],
            false,
        ),
        (
            rows("SELECT count(*) FROM t LEFT JOIN u ON t.{d} = u.{d}"),
            vec![10],
            false,
        ),
        (
            rows("SELECT count(*) FROM t FULL JOIN u ON t.{d} = u.{d}"),
            vec![11],
            false,
        ),
        (
            rows("SELECT count(*) FROM t, u WHERE t.{d} = u.{d}"),
            vec![9],
            false,
        ),
        (
            rows("SELECT count(*) FROM t JOIN u USING ({d})"),
            vec![9],
            false,
        ),
        (
            rows("SELECT count(*) FROM t JOIN u ON t.{d} = u.d"),
            vec![9],
            false,
        ),
        (
            rows("SELECT id FROM t WHERE {d} IN (SELECT {d} FROM u WHERE id = 1) ORDER BY id"),
            vec![1, 2],
            true,
        ),
        (
            rows(
                "SELECT id FROM t WHERE {d} NOT IN (SELECT {d} FROM u WHERE id IN (1, 3)) ORDER BY id",
            ),
            vec![6],
            true,
        ),
        (
            rows(
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.{d} = t.{d} AND u.id = 4) ORDER BY id",
            ),
            vec![3, 4],
            true,
        ),
        (
            rows(
                "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.{d} = t.{d} AND u.id = 1) ORDER BY id",
            ),
            vec![3, 4, 5, 6],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE {d} = (SELECT max({d}) FROM u WHERE id = 1) ORDER BY id"),
            vec![1, 2],
            true,
        ),
        (
            rows(
                "SELECT count(*) FROM (SELECT {d} FROM t WHERE id = 1 INTERSECT SELECT {d} FROM u WHERE id = 2)",
            ),
            vec![1],
            false,
        ),
        (
            rows(
                "SELECT count(*) FROM (SELECT {d} FROM t WHERE id IN (1, 3) EXCEPT SELECT {d} FROM u WHERE id IN (2, 4))",
            ),
            vec![0],
            false,
        ),
        (
            rows("SELECT count(*) FROM (SELECT {d} FROM t UNION SELECT {d} FROM u)"),
            vec![4],
            false,
        ),
        (
            rows("SELECT count(*) OVER (PARTITION BY {d}) FROM t ORDER BY id"),
            vec![2, 2, 2, 2, 1, 1],
            true,
        ),
        (
            rows("SELECT rank() OVER (ORDER BY {d} NULLS LAST) FROM t ORDER BY id"),
            vec![1, 1, 4, 4, 6, 3],
            true,
        ),
        (
            rows("SELECT count(*) OVER (PARTITION BY {d} ORDER BY {d}) FROM t ORDER BY id"),
            vec![2, 2, 2, 2, 1, 1],
            true,
        ),
        (
            rows("SELECT count(DISTINCT {d}) OVER () FROM t ORDER BY id LIMIT 1"),
            vec![3],
            true,
        ),
        (
            rows("SELECT id FROM t ORDER BY {d} NULLS LAST, id"),
            vec![1, 2, 6, 3, 4, 5],
            true,
        ),
        (
            rows("SELECT id FROM t ORDER BY {d} DESC NULLS LAST, id"),
            vec![3, 4, 6, 1, 2, 5],
            true,
        ),
        (
            rows("SELECT id FROM t ORDER BY {d} DESC NULLS LAST, id LIMIT 3"),
            vec![3, 4, 6],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE {d} = {e} ORDER BY id"),
            vec![1, 2, 3, 4, 6],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE {d} < {e} OR {d} > {e} ORDER BY id"),
            vec![],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE {d} IS DISTINCT FROM {e} ORDER BY id"),
            vec![5],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE {d} BETWEEN {e} AND {e} ORDER BY id"),
            vec![1, 2, 3, 4, 6],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE {d} IN ({e}, 7.0) ORDER BY id"),
            vec![1, 2, 3, 4, 6],
            true,
        ),
        (
            rows("SELECT id FROM t WHERE CASE {d} WHEN {e} THEN true ELSE false END ORDER BY id"),
            vec![1, 2, 3, 4, 6],
            true,
        ),
        (
            rows("SELECT count(*) FILTER (WHERE {d} = {e}) FROM t"),
            vec![5],
            false,
        ),
        (
            rows("SELECT count(*) FROM t JOIN u ON t.id = u.id AND t.{d} >= u.{e}"),
            vec![5],
            false,
        ),
        (
            rows("SELECT count(*) FROM (SELECT DISTINCT ON ({d}) {d}, id FROM t ORDER BY {d}, id)"),
            vec![4],
            false,
        ),
    ]
}

fn integer_key_cases() -> Vec<(&'static str, Vec<i64>, bool)> {
    vec![
        (
            "SELECT count(*) FROM (SELECT CAST(a AS DOUBLE) / CAST(b AS DOUBLE) AS x FROM k GROUP BY CAST(a AS DOUBLE) / CAST(b AS DOUBLE))",
            vec![2],
            false,
        ),
        (
            "SELECT count(*) FROM (SELECT DISTINCT CAST(a AS DOUBLE) * CAST(b AS DOUBLE) FROM k WHERE id < 3)",
            vec![1],
            false,
        ),
        (
            "SELECT count(*) FROM k x JOIN k y ON CAST(x.a AS DOUBLE) / CAST(x.b AS DOUBLE) = CAST(y.a AS DOUBLE) / CAST(y.b AS DOUBLE)",
            vec![5],
            false,
        ),
        (
            "SELECT count(*) OVER (PARTITION BY CAST(a AS DOUBLE) / CAST(b AS DOUBLE)) FROM k ORDER BY id",
            vec![2, 2, 1],
            true,
        ),
    ]
}

async fn assert_cases<S: AsRef<str>>(session: &ReparkSession, cases: &[(S, Vec<i64>, bool)]) {
    for (sql, expected, _) in cases {
        assert_eq!(
            &ints(session, sql.as_ref()).await,
            expected,
            "{}",
            sql.as_ref()
        );
    }
}

#[tokio::test]
async fn every_key_site_treats_both_zeros_and_every_nan_as_one_value_on_both_float_types() {
    let session = session();
    for column in ["d", "f"] {
        assert_cases(&session, &key_cases(column)).await;
    }
    assert_cases(&session, &integer_key_cases()).await;
}

#[tokio::test]
async fn the_keys_switch_off_restores_the_bit_pattern_keys_on_that_session_only() {
    let off = keys_off_session();
    assert_eq!(
        ints(&off, "SELECT count(*) FROM (SELECT d FROM t GROUP BY d)").await,
        vec![6]
    );
    assert_eq!(
        ints(&off, "SELECT count(*) FROM t JOIN u ON t.d = u.d").await,
        vec![5]
    );
    assert_eq!(
        ints(&off, "SELECT id FROM t WHERE d = 0.0 ORDER BY id").await,
        vec![1, 2]
    );
    let plan = optimized(&off, "SELECT count(*) FROM t JOIN u ON t.d = u.d").await;
    assert!(!format!("{}", plan.display_indent()).contains(WRAP));
    let on = session();
    assert_eq!(
        ints(&on, "SELECT count(*) FROM (SELECT d FROM t GROUP BY d)").await,
        vec![4]
    );
}

#[tokio::test]
async fn a_set_statement_cannot_reach_the_door_policy() {
    let session = session();
    for statement in [
        "SET repark.sql.signedZero.normalizeKeys = false",
        "SET datafusion.optimizer.max_passes = 1",
    ] {
        let _ = session.sql(statement).await;
    }
    assert_eq!(
        ints(
            &session,
            "SELECT count(*) FROM (SELECT d FROM t GROUP BY d)"
        )
        .await,
        vec![4]
    );
    assert_eq!(
        ints(&session, "SELECT count(*) FROM t JOIN u ON t.d = u.d").await,
        vec![9]
    );
}

#[tokio::test]
async fn group_and_distinct_outputs_store_the_positive_zero() {
    let session = session();
    for sql in [
        "SELECT d FROM t WHERE id IN (1, 2) GROUP BY d",
        "SELECT DISTINCT d FROM t WHERE id IN (1, 2)",
        "SELECT d FROM t WHERE id = 1 UNION SELECT d FROM u WHERE id = 2",
    ] {
        let batches = session
            .sql(sql)
            .await
            .expect(sql)
            .collect()
            .await
            .expect(sql);
        let values: Vec<u64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Float64Type>()
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values, vec![0.0f64.to_bits()], "{sql}");
    }
    let stored = session
        .sql("SELECT d FROM t WHERE id = 1 UNION ALL SELECT d FROM t WHERE id = 1")
        .await
        .expect("union all")
        .collect()
        .await
        .expect("union all");
    let bits: Vec<u64> = stored
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Float64Type>()
                .values()
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(bits, vec![(-0.0f64).to_bits(); 2]);
    let projected = session
        .sql("SELECT d, e FROM t WHERE id = 4")
        .await
        .expect("projection")
        .collect()
        .await
        .expect("projection");
    let d = projected[0]
        .column(0)
        .as_primitive::<Float64Type>()
        .value(0);
    assert_eq!(d.to_bits(), NEG_NAN_64);
}

fn wrap_count(plan: &LogicalPlan) -> usize {
    format!("{}", plan.display_indent()).matches(WRAP).count()
}

#[tokio::test]
async fn each_float_key_carries_one_wrapper_and_a_second_optimize_is_the_identity() {
    let session = session();
    let state = session.context().state();
    let cases: &[(&str, usize)] = &[
        ("SELECT count(*) FROM (SELECT d FROM t GROUP BY d)", 1),
        ("SELECT d, count(*) FROM t GROUP BY ROLLUP(d)", 1),
        ("SELECT count(*) FROM t JOIN u ON t.d = u.d", 2),
        ("SELECT count(*) FROM t JOIN u ON t.f = u.d", 2),
        ("SELECT count(*) OVER (PARTITION BY d ORDER BY d) FROM t", 1),
        ("SELECT id FROM t ORDER BY d, id", 1),
        ("SELECT id FROM t WHERE d = e", 2),
        (
            "SELECT count(DISTINCT d) * 10 + count(DISTINCT id % 2) FROM t",
            1,
        ),
        (
            "SELECT x.d FROM (SELECT d FROM t GROUP BY d) x JOIN u ON x.d = u.d",
            2,
        ),
        ("SELECT d, count(*) FROM t GROUP BY d ORDER BY d", 1),
    ];
    for (sql, expected) in cases {
        let plan = optimized(&session, sql).await;
        let rendered = format!("{}", plan.display_indent());
        assert_eq!(wrap_count(&plan), *expected, "{sql}\n{rendered}");
        let again = state.optimize(&plan).expect("second optimize");
        assert_eq!(format!("{}", again.display_indent()), rendered, "{sql}");
        for line in rendered.lines().filter(|line| line.contains("TableScan")) {
            assert!(!line.contains(WRAP), "{sql}\n{rendered}");
        }
    }
}

#[tokio::test]
async fn output_names_and_qualifiers_survive_the_wrap() {
    let on = session();
    let off = keys_off_session();
    for sql in [
        "SELECT d, count(*) FROM t GROUP BY d",
        "SELECT t.d FROM t GROUP BY t.d",
        "SELECT count(DISTINCT d), count(DISTINCT f) FROM t",
        "SELECT rank() OVER (PARTITION BY d ORDER BY f) FROM t",
        "SELECT d = e, d IN (e, 1.0), CASE d WHEN e THEN 1 END FROM t",
        "SELECT d, count(*) FROM t GROUP BY CUBE(d)",
        "SELECT d, f, count(*) FROM t GROUP BY GROUPING SETS ((d), (d, f))",
        "SELECT DISTINCT d, f FROM t",
        "SELECT d FROM t UNION SELECT d FROM u",
        "SELECT count(*) FILTER (WHERE d = e) FROM t",
    ] {
        let names = |plan: &LogicalPlan| {
            plan.schema()
                .iter()
                .map(|(qualifier, field)| {
                    (
                        qualifier.map(ToString::to_string),
                        field.name().clone(),
                        field.data_type().clone(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let wrapped = optimized(&on, sql).await;
        assert!(wrap_count(&wrapped) > 0, "{sql}");
        assert_eq!(names(&wrapped), names(&optimized(&off, sql).await), "{sql}");
    }
}

#[tokio::test]
async fn prepare_then_execute_wraps_each_key_once() {
    let session = session();
    session
        .sql(
            "PREPARE p(BIGINT) AS SELECT count(*) FROM (SELECT d FROM t WHERE id <= $1 GROUP BY d)",
        )
        .await
        .expect("prepare")
        .collect()
        .await
        .expect("prepare");
    assert_eq!(ints(&session, "EXECUTE p(2)").await, vec![1]);
    let plan = optimized(&session, "EXECUTE p(6)").await;
    assert_eq!(wrap_count(&plan), 1, "{}", plan.display_indent());
    session
        .sql("PREPARE q(BIGINT) AS SELECT count(*) FROM t JOIN u ON t.d = u.d WHERE t.id <= $1")
        .await
        .expect("prepare join")
        .collect()
        .await
        .expect("prepare join");
    assert_eq!(ints(&session, "EXECUTE q(2)").await, vec![4]);
    let plan = optimized(&session, "EXECUTE q(6)").await;
    assert_eq!(wrap_count(&plan), 2, "{}", plan.display_indent());
}

#[tokio::test]
async fn a_view_and_a_hand_built_repartition_wrap_their_keys_once() {
    let session = session();
    session
        .sql("CREATE VIEW grouped AS SELECT d, count(*) AS c FROM t GROUP BY d")
        .await
        .expect("view")
        .collect()
        .await
        .expect("view");
    assert_eq!(
        ints(&session, "SELECT count(*) FROM grouped").await,
        vec![4]
    );
    let plan = optimized(&session, "SELECT g.d FROM grouped g JOIN u ON g.d = u.d").await;
    assert_eq!(wrap_count(&plan), 2, "{}", plan.display_indent());
    let scan = session
        .context()
        .table("t")
        .await
        .expect("t")
        .into_unoptimized_plan();
    let plan = LogicalPlanBuilder::from(scan)
        .repartition(Partitioning::Hash(vec![col("d"), col("id")], 4))
        .expect("repartition")
        .build()
        .expect("build");
    let state = session.context().state();
    let optimized = state.optimize(&plan).expect("optimize");
    let rendered = format!("{}", optimized.display_indent());
    assert!(
        rendered.contains("Repartition: Hash(__repark_normalize_zero__(d), id)"),
        "{rendered}"
    );
    assert_eq!(
        format!(
            "{}",
            state.optimize(&optimized).expect("again").display_indent()
        ),
        rendered
    );
}

#[tokio::test]
async fn a_recursive_union_dedups_both_zeros() {
    let sql = "WITH RECURSIVE r(x) AS (SELECT d AS x FROM t WHERE id IN (1, 2) UNION SELECT x FROM r WHERE x > 1.0) SELECT CAST(x AS BIGINT) FROM r";
    assert_eq!(ints(&session(), sql).await, vec![0]);
    assert_eq!(ints(&keys_off_session(), sql).await, vec![0, 0]);
    let plan = optimized(&session(), sql).await;
    assert_eq!(wrap_count(&plan), 2, "{}", plan.display_indent());
}

#[tokio::test]
async fn no_provider_in_use_takes_a_wrapped_filter_as_exact() {
    let session = session();
    let wrapped = normalize_zero_call(col("d")).eq(normalize_zero_call(col("e")));
    assert!(contains_normalize_zero(&wrapped));
    assert!(!contains_normalize_zero(&col("d").eq(col("e"))));
    let memory = session
        .context()
        .table_provider("t")
        .await
        .expect("memtable");
    let classes = memory
        .supports_filters_pushdown(&[&wrapped])
        .expect("classify");
    assert!(
        classes
            .iter()
            .all(|class| *class != TableProviderFilterPushDown::Exact)
    );
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("seed.parquet");
    session
        .sql(&format!(
            "COPY (SELECT * FROM t) TO '{}' STORED AS PARQUET",
            path.display()
        ))
        .await
        .expect("copy")
        .collect()
        .await
        .expect("copy");
    session
        .context()
        .register_parquet("p", path.to_string_lossy().as_ref(), Default::default())
        .await
        .expect("register parquet");
    let listing = session
        .context()
        .table_provider("p")
        .await
        .expect("listing");
    let classes = listing
        .supports_filters_pushdown(&[&wrapped])
        .expect("classify");
    assert!(
        classes
            .iter()
            .all(|class| *class != TableProviderFilterPushDown::Exact)
    );
    assert_eq!(
        ints(&session, "SELECT id FROM p WHERE d = e ORDER BY id").await,
        vec![1, 2, 3, 4, 6]
    );
    let plan = optimized(&session, "SELECT id FROM p WHERE d = e").await;
    let rendered = format!("{}", plan.display_indent());
    let filter = rendered
        .lines()
        .find(|line| line.trim_start().starts_with("Filter:"))
        .unwrap_or_else(|| panic!("the filter stays above the scan\n{rendered}"));
    assert_eq!(filter.matches(WRAP).count(), 2, "{rendered}");
}

#[test]
fn the_kernel_returns_its_input_when_nothing_needs_a_change() {
    let input: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.5),
        None,
        Some(f64::NAN),
        Some(0.0),
    ]));
    let output = normalize_array(Arc::clone(&input)).expect("normalize");
    assert!(Arc::ptr_eq(&input, &output));
    let input32: ArrayRef = Arc::new(Float32Array::from(vec![Some(1.5f32), None]));
    let output32 = normalize_array(Arc::clone(&input32)).expect("normalize");
    assert!(Arc::ptr_eq(&input32, &output32));
}

#[test]
fn the_kernel_maps_both_zeros_and_every_nan_and_keeps_nulls_and_other_bits() {
    let payload = f64::from_bits(0x7ff8_0000_0000_0001);
    let input: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        None,
        Some(f64::from_bits(NEG_NAN_64)),
        Some(payload),
        Some(-2.5),
        Some(f64::NEG_INFINITY),
    ]));
    let output = normalize_array(input).expect("normalize");
    let output = output.as_primitive::<Float64Type>();
    assert!(output.is_null(1));
    assert_eq!(output.null_count(), 1);
    let bits: Vec<u64> = [0, 2, 3, 4, 5]
        .iter()
        .map(|row| output.value(*row).to_bits())
        .collect();
    assert_eq!(
        bits,
        vec![
            0.0f64.to_bits(),
            f64::NAN.to_bits(),
            f64::NAN.to_bits(),
            (-2.5f64).to_bits(),
            f64::NEG_INFINITY.to_bits()
        ]
    );
    let input32: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(-0.0f32),
        Some(f32::from_bits(NEG_NAN_32)),
        None,
        Some(3.25f32),
    ]));
    let output32 = normalize_array(input32).expect("normalize");
    let output32 = output32.as_primitive::<Float32Type>();
    assert_eq!(output32.value(0).to_bits(), 0.0f32.to_bits());
    assert_eq!(output32.value(1).to_bits(), f32::NAN.to_bits());
    assert!(output32.is_null(2));
    assert_eq!(output32.value(3).to_bits(), 3.25f32.to_bits());
}

#[test]
fn the_udf_maps_a_scalar_and_keeps_its_field() {
    let udf = normalize_zero_udf();
    let field = Arc::new(Field::new("d", DataType::Float32, false));
    let args = datafusion::logical_expr::ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.0)))],
        arg_fields: vec![Arc::new(Field::new("d", DataType::Float64, true))],
        number_rows: 1,
        return_field: Arc::new(Field::new(udf.name(), DataType::Float64, true)),
        config_options: Arc::default(),
    };
    let ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) =
        udf.invoke_with_args(args).expect("invoke")
    else {
        panic!("scalar result expected");
    };
    assert_eq!(value.to_bits(), 0.0f64.to_bits());
    let returned = udf
        .return_field_from_args(datafusion::logical_expr::ReturnFieldArgs {
            arg_fields: &[Arc::clone(&field)],
            scalar_arguments: &[None],
        })
        .expect("field");
    assert_eq!(returned.data_type(), &DataType::Float32);
    assert!(!returned.is_nullable());
}
