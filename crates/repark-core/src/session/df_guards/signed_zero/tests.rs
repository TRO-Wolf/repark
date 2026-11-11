use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Float32Array, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::optimizer::{AnalyzerRule, OptimizerContext, OptimizerRule};

use super::SignedZeroLiterals;
use crate::ReparkSession;

const DOUBLES: [Option<f64>; 8] = [
    Some(-0.0),
    Some(0.0),
    None,
    Some(f64::NAN),
    Some(f64::INFINITY),
    Some(f64::NEG_INFINITY),
    Some(1.5),
    Some(-2.5),
];

const FLOATS: [Option<f32>; 8] = [
    Some(-0.0),
    Some(0.0),
    None,
    Some(f32::NAN),
    Some(f32::INFINITY),
    Some(f32::NEG_INFINITY),
    Some(1.5),
    Some(-2.5),
];

const OPERATORS: [(&str, Op); 8] = [
    ("=", Op::Eq),
    ("<>", Op::NotEq),
    ("<", Op::Lt),
    ("<=", Op::LtEq),
    (">", Op::Gt),
    (">=", Op::GtEq),
    ("IS NOT DISTINCT FROM", Op::NullSafeEq),
    ("IS DISTINCT FROM", Op::Distinct),
];

const ZERO_SHAPES: [&str; 9] = [
    "0.0",
    "-0.0",
    "0",
    "-0",
    "0e5",
    "CAST('-0.0' AS DOUBLE)",
    "CAST(0 AS DOUBLE)",
    "(1.0 - 1.0)",
    "abs(-0.0)",
];

#[derive(Clone, Copy, Debug)]
enum Op {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    NullSafeEq,
    Distinct,
}

impl Op {
    fn swapped(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            symmetric => symmetric,
        }
    }

    fn truth(self, value: Option<f64>) -> Option<bool> {
        let Some(value) = value else {
            return match self {
                Self::NullSafeEq => Some(false),
                Self::Distinct => Some(true),
                _ => None,
            };
        };
        let order = if value.is_nan() {
            Ordering::Greater
        } else {
            value.partial_cmp(&0.0).unwrap_or(Ordering::Greater)
        };
        Some(match self {
            Self::Eq | Self::NullSafeEq => order == Ordering::Equal,
            Self::NotEq | Self::Distinct => order != Ordering::Equal,
            Self::Lt => order == Ordering::Less,
            Self::LtEq => order != Ordering::Greater,
            Self::Gt => order == Ordering::Greater,
            Self::GtEq => order != Ordering::Less,
        })
    }
}

fn seeded_session() -> ReparkSession {
    let session = ReparkSession::builder().build().expect("core session");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(1..=8)),
            Arc::new(Float64Array::from(DOUBLES.to_vec())),
            Arc::new(Float32Array::from(FLOATS.to_vec())),
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

fn expected(op: Op, negate: bool) -> Vec<i64> {
    (1..=8)
        .zip(DOUBLES)
        .filter(|(_, value)| op.truth(*value) == Some(!negate))
        .map(|(id, _)| id)
        .collect()
}

fn predicate(column: &str, op: &str, zero: &str, zero_on_left: bool, negate: bool) -> String {
    let comparison = if zero_on_left {
        format!("{zero} {op} {column}")
    } else {
        format!("{column} {op} {zero}")
    };
    if negate {
        format!("NOT ({comparison})")
    } else {
        comparison
    }
}

#[tokio::test]
async fn every_zero_shape_compares_with_ieee_equality_on_both_float_types() {
    let session = seeded_session();
    let mut checked = 0;
    for (sql_op, op) in OPERATORS {
        for zero in ZERO_SHAPES {
            for column in ["d", "f"] {
                for (zero_on_left, negate) in [(false, false), (true, false), (false, true)] {
                    let effective = if zero_on_left { op.swapped() } else { op };
                    let pred = predicate(column, sql_op, zero, zero_on_left, negate);
                    let sql = format!("SELECT id FROM t WHERE {pred}");
                    assert_eq!(
                        ids(&session, &sql).await,
                        expected(effective, negate),
                        "{sql}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert_eq!(checked, 432);
}

#[tokio::test]
async fn lists_ranges_cases_and_computed_operands_follow_ieee_zero() {
    let session = seeded_session();
    let cases: &[(&str, &[i64])] = &[
        ("d IN (0.0, 1.5)", &[1, 2, 7]),
        ("d IN (-0.0)", &[1, 2]),
        ("d IN (CAST('-0.0' AS DOUBLE), NULL)", &[1, 2]),
        ("d NOT IN (0.0, 1.5)", &[4, 5, 6, 8]),
        ("d NOT IN (-0.0)", &[4, 5, 6, 7, 8]),
        ("d NOT IN (0.0, NULL)", &[]),
        ("f IN (0, 1.5)", &[1, 2, 7]),
        ("0.0 IN (d, 7.0)", &[1, 2]),
        ("0.0 NOT IN (d)", &[4, 5, 6, 7, 8]),
        ("d BETWEEN -1.0 AND 0.0", &[1, 2]),
        ("d BETWEEN 0.0 AND 1.5", &[1, 2, 7]),
        ("d BETWEEN CAST('-0.0' AS DOUBLE) AND 0.0", &[1, 2]),
        ("d NOT BETWEEN 0.0 AND 1.0", &[4, 5, 6, 7, 8]),
        ("f BETWEEN -0.0 AND -0.0", &[1, 2]),
        ("d = 0.0 AND d = -0.0", &[1, 2]),
        ("d * 0 = 0", &[1, 2, 7, 8]),
        ("d * 0 <> 0", &[4, 5, 6]),
        ("d >= CAST(0 AS DOUBLE) * 1", &[1, 2, 4, 5, 7]),
        ("CAST(f AS DOUBLE) = 0.0", &[1, 2]),
        ("CASE d WHEN 0.0 THEN true ELSE false END", &[1, 2]),
        ("CASE d WHEN -0.0 THEN true ELSE false END", &[1, 2]),
        ("CASE d WHEN 0 THEN true ELSE false END", &[1, 2]),
        ("CASE d WHEN 1.5 THEN false WHEN 0.0 THEN true END", &[1, 2]),
        ("CASE f WHEN CAST('-0.0' AS DOUBLE) THEN true END", &[1, 2]),
        ("CASE d WHEN CAST('NaN' AS DOUBLE) THEN true END", &[4]),
        ("CASE 0.0 WHEN d THEN true END", &[1, 2]),
        (
            "(SELECT CAST('-0.0' AS DOUBLE) = 0.0)",
            &[1, 2, 3, 4, 5, 6, 7, 8],
        ),
        ("(SELECT CAST('-0.0' AS DOUBLE) < 0.0)", &[]),
        ("id IN (SELECT id FROM t WHERE d = -0.0)", &[1, 2]),
    ];
    for (pred, wanted) in cases {
        let sql = format!("SELECT id FROM t WHERE {pred}");
        assert_eq!(ids(&session, &sql).await, wanted.to_vec(), "{sql}");
    }
}

#[tokio::test]
async fn a_prepared_comparison_binds_ieee_zero_at_execute() {
    let session = seeded_session();
    for (statement, wanted) in [
        ("PREPARE p1(DOUBLE) AS SELECT id FROM t WHERE d = $1", None),
        ("EXECUTE p1(0.0)", Some(vec![1, 2])),
        ("EXECUTE p1(-0.0)", Some(vec![1, 2])),
        ("PREPARE p2(DOUBLE) AS SELECT id FROM t WHERE d >= $1", None),
        ("EXECUTE p2(0.0)", Some(vec![1, 2, 4, 5, 7])),
        ("PREPARE p3(DOUBLE) AS SELECT id FROM t WHERE d <> $1", None),
        ("EXECUTE p3(-0.0)", Some(vec![4, 5, 6, 7, 8])),
        ("PREPARE p4(INT) AS SELECT id FROM t WHERE d <= $1", None),
        ("EXECUTE p4(0)", Some(vec![1, 2, 6, 8])),
    ] {
        match wanted {
            None => {
                session
                    .sql(statement)
                    .await
                    .unwrap_or_else(|error| panic!("{statement}: {error}"))
                    .collect()
                    .await
                    .unwrap_or_else(|error| panic!("{statement}: {error}"));
            }
            Some(wanted) => assert_eq!(ids(&session, statement).await, wanted, "{statement}"),
        }
    }
}

#[tokio::test]
async fn the_rewrite_keeps_every_output_name() {
    let session = seeded_session();
    for sql in [
        "SELECT d = 0.0, d <> -0.0, d IS NOT DISTINCT FROM 0.0 FROM t",
        "SELECT d IN (0.0, 1.5), d BETWEEN -0.0 AND 1.0, f = 0 FROM t",
        "SELECT CASE d WHEN 0.0 THEN 'z' ELSE 'n' END FROM t",
        "SELECT d = 1.0 - 1.0 AS computed, count(*) FILTER (WHERE d = 0.0) FROM t GROUP BY d = 1.0 - 1.0",
    ] {
        let state = session.context().state();
        let logical = state.create_logical_plan(sql).await.expect("plan");
        let optimized = state.optimize(&logical).expect("optimize");
        assert_eq!(
            optimized.schema().field_names(),
            logical.schema().field_names(),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn the_bound_form_is_a_fixpoint_of_both_seats() {
    let session = seeded_session();
    let state = session.context().state();
    let sql = "SELECT d = 0.0 AS z FROM t WHERE (d <> -0.0) OR (f IN (0.0, 1.5)) \
               OR (d IS DISTINCT FROM 0) OR (d BETWEEN 0.0 AND 1.0) OR (CASE d WHEN 0.0 THEN true END)";
    let logical = state.create_logical_plan(sql).await.expect("plan");
    let analyzed = state
        .analyzer()
        .execute_and_check(logical, state.config_options(), |_, _| {})
        .expect("analyze");
    let rendered = format!("{}", analyzed.display_indent());
    assert!(
        rendered.contains("t.d >= Float64(-0) AND t.d <= Float64(0) AS z"),
        "{rendered}"
    );
    assert!(!rendered.contains("t.d = Float64(0)"), "{rendered}");
    let again = AnalyzerRule::analyze(
        &SignedZeroLiterals,
        analyzed.clone(),
        state.config_options(),
    )
    .expect("re-analyze");
    assert_eq!(format!("{}", again.display_indent()), rendered);
    let optimized = state.optimize(&analyzed).expect("optimize");
    let rewritten =
        OptimizerRule::rewrite(&SignedZeroLiterals, optimized, &OptimizerContext::new())
            .expect("re-optimize");
    assert!(
        !rewritten.transformed,
        "{}",
        rewritten.data.display_indent()
    );
}

#[tokio::test]
async fn seat_1b_rewrites_a_zero_made_by_constant_folding() {
    let session = seeded_session();
    let state = session.context().state();
    let logical = state
        .create_logical_plan("SELECT id FROM t WHERE d = 1.0 - 1.0")
        .await
        .expect("plan");
    let analyzed = state
        .analyzer()
        .execute_and_check(logical, state.config_options(), |_, _| {})
        .expect("analyze");
    let optimized = state.optimize(&analyzed).expect("optimize");
    let rendered = format!("{}", optimized.display_indent());
    assert!(
        rendered.contains("t.d >= Float64(-0) AND t.d <= Float64(0)"),
        "{rendered}"
    );
}

#[tokio::test]
async fn seat_1b_hands_the_next_pass_a_simplified_plan() {
    let session = seeded_session();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(1..=3)),
            Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.5])),
        ],
    )
    .expect("non-null batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("non-null table");
    session
        .context()
        .register_table("nn", Arc::new(table))
        .expect("register non-null table");
    let sql = "SELECT id FROM nn WHERE k <=> 1.0 - 1.0";
    assert_eq!(ids(&session, sql).await, vec![1, 2]);
    let state = session.context().state();
    let logical = state.create_logical_plan(sql).await.expect("plan");
    let analyzed = state
        .analyzer()
        .execute_and_check(logical, state.config_options(), |_, _| {})
        .expect("analyze");
    let mut passes = 0;
    let optimized = state
        .optimizer()
        .optimize(analyzed, &state, |_, rule| {
            if rule.name() == "signed_zero_literals" {
                passes += 1;
            }
        })
        .expect("optimize");
    let rendered = format!("{}", optimized.display_indent());
    assert!(!rendered.contains("IS NOT NULL"), "{rendered}");
    assert_eq!(passes, 2, "{rendered}");
}
