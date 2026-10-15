use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;

const COLUMNS: [(&str, TimeUnit, bool); 4] = [
    ("a_us_tz", TimeUnit::Microsecond, true),
    ("a_us", TimeUnit::Microsecond, false),
    ("a_ns", TimeUnit::Nanosecond, false),
    ("a_ns_tz", TimeUnit::Nanosecond, true),
];

fn column_type(unit: TimeUnit, zoned: bool) -> DataType {
    DataType::Timestamp(unit, zoned.then(|| Arc::<str>::from("UTC")))
}

fn fixture_batch() -> RecordBatch {
    let mut fields: Vec<Field> = COLUMNS
        .iter()
        .map(|(name, unit, zoned)| Field::new(*name, column_type(*unit, *zoned), true))
        .collect();
    fields.push(Field::new("flag", DataType::Boolean, false));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMicrosecondArray::from(vec![1_000_000i64, 2_000_000]).with_timezone("UTC"),
        ),
        Arc::new(TimestampMicrosecondArray::from(vec![
            1_000_000i64,
            2_000_000,
        ])),
        Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_000i64,
            2_000_000_000,
        ])),
        Arc::new(
            TimestampNanosecondArray::from(vec![1_000_000_000i64, 2_000_000_000])
                .with_timezone("UTC"),
        ),
        Arc::new(BooleanArray::from(vec![true, false])),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .unwrap_or_else(|error| panic!("build fixture batch: {error}"))
}

fn with_tables(session: ReparkSession) -> ReparkSession {
    for table in ["t", "u"] {
        session
            .context()
            .register_batch(table, fixture_batch())
            .unwrap_or_else(|error| panic!("register {table}: {error}"));
    }
    session
}

fn spark_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    with_tables(
        ReparkSession::builder()
            .with_extension(Arc::new(SparkExtension))
            .with_sql_dialect(dialect)
            .build()
            .unwrap_or_else(|error| panic!("build spark door: {error}")),
    )
}

fn ansi_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    with_tables(
        ReparkSession::builder()
            .with_sql_dialect(dialect)
            .build()
            .unwrap_or_else(|error| panic!("build ansi door: {error}")),
    )
}

fn doors() -> [(&'static str, ReparkSession); 2] {
    [("spark", spark_door()), ("ansi", ansi_door())]
}

fn mixed_pairs() -> Vec<(&'static str, &'static str)> {
    let mut pairs = Vec::new();
    for (left, left_unit, _) in COLUMNS {
        for (right, right_unit, _) in COLUMNS {
            if left_unit != right_unit {
                pairs.push((left, right));
            }
        }
    }
    pairs
}

fn unit_of(data_type: &DataType) -> Option<TimeUnit> {
    match data_type {
        DataType::Timestamp(unit, _) => Some(*unit),
        _ => None,
    }
}

async fn result_type(session: &ReparkSession, sql: &str) -> DataType {
    optimized(session, sql)
        .await
        .schema()
        .field(0)
        .data_type()
        .clone()
}

async fn optimized(session: &ReparkSession, sql: &str) -> LogicalPlan {
    session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
        .into_optimized_plan()
        .unwrap_or_else(|error| panic!("optimize `{sql}`: {error}"))
}

fn join_operand_types(plan: &LogicalPlan) -> Vec<(DataType, DataType)> {
    let mut found = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Join(join) = node {
            for (left, right) in &join.on {
                let left_type = left
                    .get_type(join.left.schema())
                    .unwrap_or_else(|error| panic!("left key type: {error}"));
                let right_type = right
                    .get_type(join.right.schema())
                    .unwrap_or_else(|error| panic!("right key type: {error}"));
                found.push((left_type, right_type));
            }
            if let Some(filter) = join.filter.as_ref() {
                filter
                    .apply(|expr| {
                        if let Expr::BinaryExpr(BinaryExpr { left, right, .. }) = expr {
                            let left_type = left.get_type(&join.schema);
                            let right_type = right.get_type(&join.schema);
                            if let (Ok(left_type), Ok(right_type)) = (left_type, right_type)
                                && unit_of(&left_type).is_some()
                                && unit_of(&right_type).is_some()
                            {
                                found.push((left_type, right_type));
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .unwrap_or_else(|error| panic!("walk join filter: {error}"));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap_or_else(|error| panic!("walk plan: {error}"));
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn written_type_names_plan_to_the_spark_types_on_both_doors() {
    let wall = "2024-01-02 03:04:05";
    let cells = [
        (
            format!("SELECT CAST('{wall}' AS TIMESTAMP)"),
            TimeUnit::Microsecond,
            true,
            false,
        ),
        (
            format!("SELECT TIMESTAMP '{wall}'"),
            TimeUnit::Microsecond,
            true,
            false,
        ),
        (
            format!("SELECT CAST('{wall}' AS TIMESTAMP_NTZ)"),
            TimeUnit::Microsecond,
            false,
            false,
        ),
        (
            format!("SELECT CAST('{wall}' AS timestamp_ns)"),
            TimeUnit::Nanosecond,
            false,
            false,
        ),
        (
            format!("SELECT CAST('{wall}' AS timestamptz_ns)"),
            TimeUnit::Nanosecond,
            true,
            true,
        ),
    ];
    for (door, session) in doors() {
        for (sql, unit, spark_zoned, ansi_zoned) in &cells {
            let zoned = if door == "spark" {
                spark_zoned
            } else {
                ansi_zoned
            };
            let got = result_type(&session, sql).await;
            let DataType::Timestamp(got_unit, got_zone) = &got else {
                panic!("{door}: `{sql}` answered {got:?}");
            };
            assert_eq!(got_unit, unit, "{door}: unit of `{sql}`");
            assert_eq!(got_zone.is_some(), *zoned, "{door}: zone of `{sql}`");
        }
    }
}

async fn first_micros(session: &ReparkSession, sql: &str) -> (DataType, i64) {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let column = batches[0].column(0);
    let ticks = column
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap_or_else(|| panic!("`{sql}` answered {:?}", column.data_type()))
        .value(0);
    (column.data_type().clone(), ticks)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ansi_door_plans_timestamp_as_the_wall_and_timestamptz_as_the_instant() {
    let session = ansi_door();
    let wall = "2024-01-02 03:04:05.123456789";
    let wall_micros = 1_704_164_645_123_456;
    let cells = [
        (
            format!("SELECT CAST('{wall}' AS TIMESTAMP)"),
            column_type(TimeUnit::Microsecond, false),
            wall_micros,
        ),
        (
            format!("SELECT TIMESTAMP '{wall}'"),
            column_type(TimeUnit::Microsecond, false),
            wall_micros,
        ),
        (
            format!("SELECT CAST('{wall}' AS TIMESTAMPTZ)"),
            column_type(TimeUnit::Microsecond, true),
            wall_micros,
        ),
        (
            "SELECT CAST(1 AS TIMESTAMP)".to_owned(),
            column_type(TimeUnit::Microsecond, false),
            1_000_000,
        ),
        (
            "SELECT CAST(1 AS TIMESTAMPTZ)".to_owned(),
            column_type(TimeUnit::Microsecond, true),
            1_000_000,
        ),
    ];
    for (sql, expected_type, expected_ticks) in &cells {
        let (got_type, got_ticks) = first_micros(&session, sql).await;
        assert_eq!(&got_type, expected_type, "type of `{sql}`");
        assert_eq!(got_ticks, *expected_ticks, "ticks of `{sql}`");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_unit_positions_take_the_finest_unit_on_both_doors() {
    for (door, session) in doors() {
        let array = if door == "spark" {
            "array"
        } else {
            "make_array"
        };
        for (left, right) in mixed_pairs() {
            let cells = [
                format!("SELECT coalesce({left}, {right}) AS v FROM t"),
                format!("SELECT {left} AS v FROM t UNION ALL SELECT {right} AS v FROM t"),
                format!("SELECT CASE WHEN flag THEN {left} ELSE {right} END AS v FROM t"),
            ];
            for sql in &cells {
                let got = result_type(&session, sql).await;
                assert_eq!(
                    unit_of(&got),
                    Some(TimeUnit::Nanosecond),
                    "{door}: `{sql}` answered {got:?}"
                );
            }
            let sql = format!("SELECT {array}({left}, {right}) AS v FROM t");
            let got = result_type(&session, &sql).await;
            let DataType::List(element) = &got else {
                panic!("{door}: `{sql}` answered {got:?}");
            };
            assert_eq!(
                unit_of(element.data_type()),
                Some(TimeUnit::Nanosecond),
                "{door}: `{sql}` answered {got:?}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn join_keys_share_the_finest_unit_in_the_optimized_plan() {
    for (door, session) in doors() {
        for (left, right) in mixed_pairs() {
            for operator in ["=", "<", "<=>"] {
                if door == "ansi" && operator == "<=>" {
                    continue;
                }
                let sql =
                    format!("SELECT a.flag FROM t a JOIN u b ON a.{left} {operator} b.{right}");
                let plan = optimized(&session, &sql).await;
                let operands = join_operand_types(&plan);
                assert!(
                    !operands.is_empty(),
                    "{door}: no join operands in `{sql}`\n{plan}"
                );
                for (left_type, right_type) in operands {
                    assert_eq!(
                        unit_of(&left_type),
                        Some(TimeUnit::Nanosecond),
                        "{door}: left key of `{sql}` is {left_type:?}\n{plan}"
                    );
                    assert_eq!(
                        unit_of(&right_type),
                        Some(TimeUnit::Nanosecond),
                        "{door}: right key of `{sql}` is {right_type:?}\n{plan}"
                    );
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn equal_units_plan_no_cast_on_both_doors() {
    for (door, session) in doors() {
        for (name, _, _) in COLUMNS {
            let sql = format!("SELECT coalesce({name}, {name}) AS v FROM t WHERE {name} = {name}");
            let plan = optimized(&session, &sql).await;
            let mut casts = 0usize;
            plan.apply(|node| {
                for expr in node.expressions() {
                    expr.apply(|inner| {
                        if matches!(inner, Expr::Cast(_) | Expr::TryCast(_)) {
                            casts += 1;
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap_or_else(|error| panic!("walk plan: {error}"));
            assert_eq!(casts, 0, "{door}: `{sql}` planned a cast\n{plan}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ansi_door_resolves_no_nanosecond_kernel_by_name() {
    let session = ansi_door();
    for name in [
        "__repark_cast_timestamp_ns__",
        "__repark_cast_timestamptz_ns__",
    ] {
        let sql = format!("SELECT {name}('2026-01-02 03:04:05')");
        assert!(
            session.sql(&sql).await.is_err(),
            "the ANSI door resolves `{name}` by name"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_widened_position_equals_its_written_nanosecond_cast_on_both_doors() {
    for (door, session) in doors() {
        for (name, zoned) in [("a_us", false), ("a_us_tz", true)] {
            let target = if zoned {
                "timestamptz_ns"
            } else {
                "timestamp_ns"
            };
            let sibling = if zoned { "a_ns_tz" } else { "a_ns" };
            let widened = format!("SELECT coalesce({name}, {sibling}) AS v FROM t");
            let written =
                format!("SELECT coalesce(CAST({name} AS {target}), {sibling}) AS v FROM t");
            let mut answers = Vec::new();
            for sql in [&widened, &written] {
                let batches = session
                    .sql(sql)
                    .await
                    .unwrap_or_else(|error| panic!("{door}: plan `{sql}`: {error}"))
                    .collect()
                    .await
                    .unwrap_or_else(|error| panic!("{door}: run `{sql}`: {error}"));
                answers.push(
                    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                        .unwrap_or_else(|error| panic!("{door}: print `{sql}`: {error}"))
                        .to_string(),
                );
            }
            assert_eq!(
                answers[0], answers[1],
                "{door}: `{widened}` against `{written}`"
            );
        }
    }
}
