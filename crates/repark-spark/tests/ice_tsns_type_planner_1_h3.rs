use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::prelude::SessionContext;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};

fn type_columns() -> Vec<(String, String, DataType)> {
    vec![
        (
            String::from("us_tz"),
            String::from("a_us_tz"),
            DataType::Timestamp(TimeUnit::Microsecond, Some(String::from("UTC").into())),
        ),
        (
            String::from("us"),
            String::from("a_us"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            String::from("ns"),
            String::from("a_ns"),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
        (
            String::from("ns_tz"),
            String::from("a_ns_tz"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(String::from("UTC").into())),
        ),
    ]
}

fn fixture_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a_us_tz", type_columns()[0].2.clone(), true),
        Field::new("a_us", type_columns()[1].2.clone(), true),
        Field::new("a_ns", type_columns()[2].2.clone(), true),
        Field::new("a_ns_tz", type_columns()[3].2.clone(), true),
        Field::new("flag", DataType::Boolean, false),
    ]));
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
    RecordBatch::try_new(schema, columns)
        .unwrap_or_else(|error| panic!("build fixture batch: {error}"))
}

fn plain_session() -> SessionContext {
    let context = SessionContext::new();
    context
        .register_batch("t", fixture_batch())
        .unwrap_or_else(|error| panic!("register fixture on plain session: {error}"));
    context
}

fn repark_session() -> SessionContext {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap_or_else(|error| panic!("build spark session: {error}"));
    session
        .context()
        .register_batch("t", fixture_batch())
        .unwrap_or_else(|error| panic!("register fixture on spark session: {error}"));
    session.context().clone()
}

fn flat(error: String) -> String {
    error.replace('\n', " ").replace('|', " ")
}

fn find_eq_operands(expression: &Expr) -> Option<(Expr, Expr)> {
    let mut found = None;
    expression
        .apply(|node| {
            if found.is_some() {
                return Ok(TreeNodeRecursion::Stop);
            }
            if let Expr::BinaryExpr(BinaryExpr { left, right, op }) = node {
                if *op == Operator::Eq {
                    found = Some(((**left).clone(), (**right).clone()));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .ok()?;
    found
}

fn projection_eq_types(plan: &LogicalPlan) -> Option<(DataType, DataType)> {
    let input_schema = plan.inputs().first()?.schema();
    let LogicalPlan::Projection(projection) = plan else {
        return None;
    };
    for expression in &projection.expr {
        if let Some((left, right)) = find_eq_operands(expression) {
            let left_type = left.get_type(input_schema).ok()?;
            let right_type = right.get_type(input_schema).ok()?;
            return Some((left_type, right_type));
        }
    }
    None
}

fn join_key_types(plan: &LogicalPlan) -> Option<(DataType, DataType, String)> {
    let mut found = None;
    plan.apply(|node| {
        if found.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::Join(join) = node {
            if let Some((left, right)) = join.on.first() {
                let left_type = left.get_type(&join.schema).ok();
                let right_type = right.get_type(&join.schema).ok();
                if let (Some(left_type), Some(right_type)) = (left_type, right_type) {
                    found = Some((left_type, right_type, String::from("on")));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            if let Some(filter) = join.filter.as_ref() {
                if let Some((left, right)) = find_eq_operands(filter) {
                    let left_type = left.get_type(&join.schema).ok();
                    let right_type = right.get_type(&join.schema).ok();
                    if let (Some(left_type), Some(right_type)) = (left_type, right_type) {
                        found = Some((left_type, right_type, String::from("filter")));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok()?;
    found
}

async fn run_pair(
    context: &SessionContext,
    label: &str,
    array_spelling: &str,
    left: &(String, String, DataType),
    right: &(String, String, DataType),
    lines: &mut Vec<String>,
) {
    let (left_tag, left_col, _) = left;
    let (right_tag, right_col, _) = right;
    let coalesce_sql = format!("SELECT coalesce({left_col}, {right_col}) AS v FROM t");
    match context.sql(&coalesce_sql).await {
        Ok(frame) => {
            let dtype = frame.schema().field(0).data_type().clone();
            lines.push(format!(
                "{label} | coalesce | {left_tag} | {right_tag} | {dtype:?}"
            ));
        }
        Err(error) => {
            lines.push(format!(
                "{label} | coalesce | {left_tag} | {right_tag} | ERROR | {}",
                flat(error.to_string())
            ));
        }
    }
    let union_sql =
        format!("SELECT {left_col} AS v FROM t UNION ALL SELECT {right_col} AS v FROM t");
    match context.sql(&union_sql).await {
        Ok(frame) => {
            let dtype = frame.schema().field(0).data_type().clone();
            lines.push(format!(
                "{label} | union | {left_tag} | {right_tag} | {dtype:?}"
            ));
        }
        Err(error) => {
            lines.push(format!(
                "{label} | union | {left_tag} | {right_tag} | ERROR | {}",
                flat(error.to_string())
            ));
        }
    }
    let array_sql = format!("SELECT {array_spelling}({left_col}, {right_col}) AS v FROM t");
    match context.sql(&array_sql).await {
        Ok(frame) => {
            let dtype = frame.schema().field(0).data_type().clone();
            lines.push(format!(
                "{label} | array({array_spelling}) | {left_tag} | {right_tag} | {dtype:?}"
            ));
        }
        Err(error) => {
            lines.push(format!(
                "{label} | array({array_spelling}) | {left_tag} | {right_tag} | ERROR | {}",
                flat(error.to_string())
            ));
        }
    }
    let case_sql =
        format!("SELECT CASE WHEN flag THEN {left_col} ELSE {right_col} END AS v FROM t");
    match context.sql(&case_sql).await {
        Ok(frame) => {
            let dtype = frame.schema().field(0).data_type().clone();
            lines.push(format!(
                "{label} | case | {left_tag} | {right_tag} | {dtype:?}"
            ));
        }
        Err(error) => {
            lines.push(format!(
                "{label} | case | {left_tag} | {right_tag} | ERROR | {}",
                flat(error.to_string())
            ));
        }
    }
    let eq_sql = format!("SELECT {left_col} = {right_col} AS v FROM t");
    match context.sql(&eq_sql).await {
        Ok(frame) => match projection_eq_types(frame.logical_plan()) {
            Some((left_type, right_type)) => {
                lines.push(format!(
                    "{label} | eq | {left_tag} | {right_tag} | {left_type:?} = {right_type:?}"
                ));
            }
            None => {
                lines.push(format!(
                    "{label} | eq | {left_tag} | {right_tag} | ERROR | no Eq found"
                ));
            }
        },
        Err(error) => {
            lines.push(format!(
                "{label} | eq | {left_tag} | {right_tag} | ERROR | {}",
                flat(error.to_string())
            ));
        }
    }
    let join_sql = format!("SELECT * FROM t a JOIN t b ON a.{left_col} = b.{right_col}");
    match context.sql(&join_sql).await {
        Ok(frame) => match join_key_types(frame.logical_plan()) {
            Some((left_type, right_type, clause)) => {
                lines.push(format!(
                    "{label} | join | {left_tag} | {right_tag} | {left_type:?} = {right_type:?} via {clause}"
                ));
            }
            None => {
                lines.push(format!(
                    "{label} | join | {left_tag} | {right_tag} | ERROR | no Join found"
                ));
            }
        },
        Err(error) => {
            lines.push(format!(
                "{label} | join | {left_tag} | {right_tag} | ERROR | {}",
                flat(error.to_string())
            ));
        }
    }
}

#[tokio::test]
async fn h3_coercion_lattice_walk_runs() {
    let plain = plain_session();
    let repark = repark_session();
    let plain_array = if plain.udf("array").is_ok() {
        "array"
    } else {
        "make_array"
    };
    let repark_array = if repark.udf("array").is_ok() {
        "array"
    } else {
        "make_array"
    };
    let types = type_columns();
    let mut lines: Vec<String> = Vec::new();
    for left in &types {
        for right in &types {
            run_pair(&plain, "plain", plain_array, left, right, &mut lines).await;
        }
    }
    for left in &types {
        for right in &types {
            run_pair(&repark, "repark", repark_array, left, right, &mut lines).await;
        }
    }
    lines.sort();
    for line in &lines {
        println!("{line}");
    }
    println!(
        "walked 16 pairs over 6 operators on plain({plain_array}) and repark({repark_array}), {} lines",
        lines.len()
    );
    assert!(
        lines.len() == 192,
        "walk printed {} lines, expected 192",
        lines.len()
    );
}
