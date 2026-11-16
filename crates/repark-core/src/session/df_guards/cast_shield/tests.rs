use std::sync::Arc;

use datafusion::arrow::array::{Decimal128Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::MemTable;

use super::udf::{CAST_SHIELD_NAME, cast_preimage, is_lossless, is_narrowing};
use super::{IN_RANGE_LIMIT, shield_plan};
use crate::ReparkSession;

const DEC: DataType = DataType::Decimal128(10, 2);

const VALUES: [(i64, Option<i128>); 12] = [
    (1, Some(50)),
    (2, Some(100)),
    (3, Some(125)),
    (4, Some(150)),
    (5, Some(199)),
    (6, Some(-125)),
    (7, Some(-50)),
    (8, Some(250)),
    (9, Some(-150)),
    (10, Some(9_999_999_999)),
    (11, Some(-9_999_999_999)),
    (12, None),
];

fn bounds(source: &DataType, target: &DataType, constant: &ScalarValue) -> Option<(i128, i128)> {
    match cast_preimage(source, target, constant)? {
        (ScalarValue::Decimal128(Some(lower), ..), ScalarValue::Decimal128(Some(upper), ..)) => {
            Some((lower, upper))
        }
        _ => None,
    }
}

#[test]
fn truncation_ranges_follow_the_sign_of_the_constant() {
    let int = DataType::Int32;
    assert_eq!(
        bounds(&DEC, &int, &ScalarValue::Int32(Some(1))),
        Some((100, 200))
    );
    assert_eq!(
        bounds(&DEC, &int, &ScalarValue::Int32(Some(-1))),
        Some((-199, -99))
    );
    assert_eq!(
        bounds(&DEC, &int, &ScalarValue::Int32(Some(0))),
        Some((-99, 100))
    );
    assert_eq!(
        bounds(&DEC, &DataType::Int64, &ScalarValue::Int64(Some(7))),
        Some((700, 800))
    );
}

#[test]
fn rounding_ranges_are_half_away_from_zero() {
    let dec0 = DataType::Decimal128(10, 0);
    let dec1 = DataType::Decimal128(10, 1);
    let k = |unscaled, precision, scale| ScalarValue::Decimal128(Some(unscaled), precision, scale);
    assert_eq!(bounds(&DEC, &dec0, &k(1, 10, 0)), Some((50, 150)));
    assert_eq!(bounds(&DEC, &dec0, &k(-1, 10, 0)), Some((-149, -49)));
    assert_eq!(bounds(&DEC, &dec0, &k(0, 10, 0)), Some((-49, 50)));
    assert_eq!(bounds(&DEC, &dec1, &k(13, 10, 1)), Some((125, 135)));
    assert_eq!(bounds(&DEC, &dec1, &k(-13, 10, 1)), Some((-134, -124)));
}

#[test]
fn overflow_capable_casts_and_unrepresentable_bounds_decline() {
    assert_eq!(
        bounds(&DEC, &DataType::Int8, &ScalarValue::Int8(Some(1))),
        None
    );
    assert_eq!(
        bounds(
            &DataType::Decimal128(38, 0),
            &DataType::Int32,
            &ScalarValue::Int32(Some(1))
        ),
        None
    );
    assert_eq!(
        bounds(
            &DataType::Decimal128(10, 3),
            &DataType::Decimal128(3, 2),
            &ScalarValue::Decimal128(Some(999), 3, 2)
        ),
        None
    );
    assert_eq!(
        bounds(
            &DataType::Decimal128(3, 2),
            &DataType::Decimal128(2, 1),
            &ScalarValue::Decimal128(Some(10), 2, 1)
        ),
        None
    );
    assert_eq!(
        bounds(
            &DataType::Int64,
            &DataType::Int32,
            &ScalarValue::Int32(Some(1))
        ),
        None
    );
    assert_eq!(
        bounds(
            &DEC,
            &DataType::Int32,
            &ScalarValue::Int32(Some(99_999_999))
        ),
        None
    );
    assert_eq!(
        bounds(
            &DEC,
            &DataType::Int32,
            &ScalarValue::Int32(Some(-99_999_999))
        ),
        Some((-9_999_999_999, -9_999_999_899))
    );
    assert_eq!(
        bounds(&DEC, &DataType::Int32, &ScalarValue::Int32(None)),
        None
    );
    assert_eq!(
        bounds(&DEC, &DataType::Int32, &ScalarValue::Int64(Some(1))),
        None
    );
}

#[test]
fn the_narrowing_predicate_is_exact_by_type() {
    let cases = [
        (DataType::Int64, DataType::Int32, true),
        (DataType::Int32, DataType::Int16, true),
        (DataType::Int32, DataType::Int64, false),
        (DEC, DataType::Int64, true),
        (DataType::Decimal128(10, 0), DataType::Int64, false),
        (DataType::Decimal128(10, 0), DataType::Int32, true),
        (DataType::Decimal128(9, 0), DataType::Int32, false),
        (DEC, DataType::Decimal128(10, 1), true),
        (DEC, DataType::Decimal128(9, 2), true),
        (DEC, DataType::Decimal128(12, 2), false),
        (DEC, DataType::Float64, false),
        (DataType::Float64, DataType::Int32, false),
        (DataType::Utf8, DataType::Int32, false),
        (
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Date32,
            false,
        ),
    ];
    for (source, target, narrowing) in cases {
        assert_eq!(
            is_narrowing(&source, &target),
            narrowing,
            "{source} -> {target}"
        );
    }
    assert!(is_lossless(&DataType::Int32, &DataType::Int64));
    assert!(is_lossless(&DataType::Int32, &DataType::Decimal128(11, 1)));
    assert!(!is_lossless(&DataType::Int32, &DataType::Decimal128(9, 0)));
    assert!(is_lossless(&DEC, &DataType::Decimal128(12, 3)));
    assert!(!is_lossless(&DEC, &DataType::Int64));
}

fn seeded_session() -> ReparkSession {
    let session = ReparkSession::builder().build().expect("core session");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("c", DEC, true),
    ]));
    let decimals =
        Decimal128Array::from(VALUES.iter().map(|(_, value)| *value).collect::<Vec<_>>())
            .with_precision_and_scale(10, 2)
            .expect("decimal column");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                VALUES.iter().map(|(id, _)| *id),
            )),
            Arc::new(decimals),
        ],
    )
    .expect("seed batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("seed table");
    session
        .context()
        .register_table("t", Arc::new(table))
        .expect("register seed");
    session
}

async fn ids(session: &ReparkSession, sql: &str) -> Vec<i64> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        ids.extend(column.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn narrowing_casts_compare_as_the_cast_values() {
    let session = seeded_session();
    let cases: [(&str, &[i64]); 14] = [
        ("CAST(c AS INT) = 1", &[2, 3, 4, 5]),
        ("1 = CAST(c AS INT)", &[2, 3, 4, 5]),
        ("CAST(c AS INT) > 1", &[8, 10]),
        ("CAST(c AS INT) <= 0", &[1, 6, 7, 9, 11]),
        ("CAST(c AS INT) = -1", &[6, 9]),
        ("CAST(c AS INT) IN (1, 2)", &[2, 3, 4, 5, 8]),
        ("CAST(c AS INT) NOT IN (1, 0)", &[6, 8, 9, 10, 11]),
        ("CAST(c AS INT) BETWEEN 1 AND 2", &[2, 3, 4, 5, 8]),
        ("CAST(c AS DECIMAL(10,0)) = 1", &[1, 2, 3]),
        ("CAST(c AS DECIMAL(10,0)) = -1", &[6, 7]),
        ("CAST(c AS DECIMAL(10,0)) < -1", &[9, 11]),
        ("CAST(c AS DECIMAL(10,1)) = 1.3", &[3]),
        ("CAST(CAST(c AS INT) AS BIGINT) = 1", &[2, 3, 4, 5]),
        ("TRY_CAST(c AS INT) IS NOT DISTINCT FROM 1", &[2, 3, 4, 5]),
    ];
    for (predicate, expected) in cases {
        let sql = format!("SELECT id FROM t WHERE {predicate}");
        assert_eq!(ids(&session, &sql).await, expected, "{predicate}");
    }
}

#[tokio::test]
async fn a_cast_defined_in_a_subquery_is_shielded_after_the_filter_moves() {
    let session = seeded_session();
    let sql = "SELECT id FROM (SELECT id, CAST(c AS INT) AS k FROM t) WHERE k = 1";
    assert_eq!(ids(&session, sql).await, [2, 3, 4, 5]);
}

#[tokio::test]
async fn the_select_list_keeps_its_names_and_the_rule_is_idempotent() {
    let session = seeded_session();
    let state = session.context().state();
    let sql = "SELECT CAST(c AS INT) = 1, CAST(c AS INT) IN (1, 2) FROM t";
    let logical = state.create_logical_plan(sql).await.expect("plan");
    let optimized = state.optimize(&logical).expect("optimize");
    assert_eq!(
        optimized.schema().field_names(),
        logical.schema().field_names()
    );
    let analyzed = state
        .analyzer()
        .execute_and_check(logical, state.config_options(), |_, _| {})
        .expect("analyze");
    let again = shield_plan(analyzed.clone()).expect("second pass");
    assert!(!again.transformed);
    assert_eq!(again.data, analyzed);
}

fn shield_count(plan: &datafusion::logical_expr::LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|inner| {
                if matches!(inner, datafusion::logical_expr::Expr::ScalarFunction(call) if call.name() == CAST_SHIELD_NAME) {
                    count += 1;
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })
        })
    })
    .expect("walk");
    count
}

#[tokio::test]
async fn an_in_list_past_the_cap_stays_per_row() {
    let session = seeded_session();
    let state = session.context().state();
    let long: Vec<String> = (0..=i64::try_from(IN_RANGE_LIMIT).expect("cap"))
        .map(|k| k.to_string())
        .collect();
    let short = "CAST(c AS INT) IN (1, 2, 3, 4, 5)";
    let past = format!("CAST(c AS INT) IN ({})", long.join(", "));
    let plan = |predicate: String| {
        let state = state.clone();
        async move {
            let logical = state
                .create_logical_plan(&format!("SELECT id FROM t WHERE {predicate}"))
                .await
                .expect("plan");
            state.optimize(&logical).expect("optimize")
        }
    };
    assert_eq!(shield_count(&plan(short.to_string()).await), 0);
    assert_eq!(shield_count(&plan(past.clone()).await), 1);
    assert_eq!(
        ids(&session, &format!("SELECT id FROM t WHERE {past}")).await,
        [1, 2, 3, 4, 5, 7, 8]
    );
}

fn analyzer_only(session: &ReparkSession) -> datafusion::prelude::SessionContext {
    let state = session.context().state();
    let rules: Vec<_> = state
        .optimizers()
        .iter()
        .filter(|rule| rule.name() != "repark_cast_shield")
        .cloned()
        .collect();
    let stripped = datafusion::execution::SessionStateBuilder::new_from_existing(state)
        .with_optimizer_rules(rules)
        .build();
    datafusion::prelude::SessionContext::new_with_state(stripped)
}

async fn frame_ids(frame: datafusion::dataframe::DataFrame) -> Vec<i64> {
    let batches = frame
        .select_columns(&["id"])
        .expect("id")
        .collect()
        .await
        .expect("collect");
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        ids.extend(column.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

async fn ruling_one_shapes(context: &datafusion::prelude::SessionContext) -> [Vec<i64>; 4] {
    use datafusion::logical_expr::{cast, col, lit};
    let subquery = context
        .sql("SELECT id FROM (SELECT id, CAST(c AS INT) AS k FROM t) WHERE k = 1")
        .await
        .expect("s17");
    let view = context
        .sql("SELECT id FROM w WHERE k = 1")
        .await
        .expect("s16");
    let derived = context
        .table("t")
        .await
        .expect("t")
        .with_column("k", cast(col("c"), DataType::Int32))
        .expect("k");
    let parsed = derived.parse_sql_expr("k = 1").expect("parse");
    let by_text = derived.clone().filter(parsed).expect("s21");
    let by_column = derived.filter(col("k").eq(lit(1))).expect("s22");
    [
        frame_ids(subquery).await,
        frame_ids(view).await,
        frame_ids(by_text).await,
        frame_ids(by_column).await,
    ]
}

#[tokio::test]
async fn the_ruling_one_shapes_need_the_optimizer_placement() {
    let session = seeded_session();
    for statement in [
        "CREATE VIEW v1 AS SELECT id, c FROM t",
        "CREATE VIEW w AS SELECT id, CAST(c AS INT) AS k FROM v1",
    ] {
        session
            .context()
            .sql(statement)
            .await
            .expect(statement)
            .collect()
            .await
            .expect(statement);
    }
    let right = vec![2, 3, 4, 5];
    assert_eq!(
        ruling_one_shapes(session.context()).await,
        [right.clone(), right.clone(), right.clone(), right]
    );
    let wrong = vec![2];
    assert_eq!(
        ruling_one_shapes(&analyzer_only(&session)).await,
        [wrong.clone(), wrong.clone(), wrong.clone(), wrong]
    );
}
