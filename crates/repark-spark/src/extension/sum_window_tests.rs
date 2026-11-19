use std::sync::Arc;

use crate::{SparkDialect, SparkExtension};
use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, WindowFunctionDefinition};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use repark_core::ReparkSession;

const MAX38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
const FRAME: &str = "ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW";

fn session(ansi: bool) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.ansi.enabled",
            if ansi { "true" } else { "false" },
        )
        .build()
        .expect("session");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d10", DataType::Decimal128(10, 2), true),
        Field::new("d38", DataType::Decimal128(38, 0), true),
        Field::new("b", DataType::Int64, true),
        Field::new("x", DataType::Float64, true),
    ]));
    let decimals = |values: Vec<i128>, precision: u8, scale: i8| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .expect("decimal"),
        )
    };
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            decimals(vec![100, 200, 300, 400, 500], 10, 2),
            decimals(vec![MAX38, MAX38, -MAX38, -MAX38, 5], 38, 0),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ],
    )
    .expect("batch");
    session
        .context()
        .register_batch("t", batch)
        .expect("register");
    session
}

fn window_functions(plan: &LogicalPlan) -> Vec<(Expr, Arc<datafusion::common::DFSchema>)> {
    let mut found = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Window(window) = node {
            for expr in &window.window_expr {
                let expr = match expr {
                    Expr::Alias(alias) => alias.expr.as_ref().clone(),
                    other => other.clone(),
                };
                found.push((expr, Arc::clone(window.input.schema())));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk");
    found
}

fn retracts(expr: &Expr, schema: &datafusion::common::DFSchema) -> Result<bool, String> {
    let Expr::WindowFunction(window) = expr else {
        return Err(format!("not a window function: {expr}"));
    };
    let WindowFunctionDefinition::AggregateUDF(udaf) = &window.fun else {
        return Err(format!("re-scanned as a WindowUDF: {expr}"));
    };
    let fields: Vec<FieldRef> = window
        .params
        .args
        .iter()
        .map(|arg| arg.to_field(schema).map(|(_, field)| field))
        .collect::<Result<_, _>>()
        .map_err(|error| error.to_string())?;
    let arrow_schema = Schema::new(
        fields
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    let exprs: Vec<Arc<dyn PhysicalExpr>> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>)
        .collect();
    let return_field = udaf
        .return_field(&fields)
        .map_err(|error| error.to_string())?;
    let accumulator = udaf
        .create_sliding_accumulator(AccumulatorArgs {
            return_field,
            schema: &arrow_schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: udaf.name(),
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &fields,
        })
        .map_err(|error| error.to_string())?;
    Ok(accumulator.supports_retract_batch())
}

#[tokio::test]
async fn every_sum_arm_stays_a_retracting_aggregate_window_in_both_ansi_modes() {
    let arms = [
        "sum(d10)",
        "sum(d38)",
        "sum(b)",
        "sum(x)",
        "try_sum(d10)",
        "try_sum(d38)",
    ];
    let select: Vec<String> = arms
        .iter()
        .enumerate()
        .map(|(index, arm)| format!("{arm} OVER ({FRAME}) AS w{index}"))
        .collect();
    let sql = format!("SELECT {} FROM t", select.join(", "));
    for ansi in [true, false] {
        let session = session(ansi);
        let plan = session
            .sql(&sql)
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("analyze and optimize");
        let windows = window_functions(&plan);
        assert_eq!(windows.len(), arms.len(), "{plan}");
        for (expr, schema) in &windows {
            assert_eq!(retracts(expr, schema), Ok(true), "ansi {ansi}: {expr}");
        }
    }
}

#[tokio::test]
async fn a_retracting_window_at_p38_checks_each_frame_in_the_analyzed_mode() {
    let sql = format!("SELECT sum(d38) OVER ({FRAME}) AS w FROM t ORDER BY id");
    let error = session(true)
        .sql(&sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect_err("ANSI on raises for the frame past 38 digits");
    assert!(
        error.to_string().contains("[NUMERIC_VALUE_OUT_OF_RANGE]"),
        "{error}"
    );
    let batches = session(false)
        .sql(&sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("ANSI off answers");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal");
    let rows: Vec<Option<i128>> = (0..column.len())
        .map(|row| column.is_valid(row).then(|| column.value(row)))
        .collect();
    assert_eq!(
        rows,
        vec![Some(MAX38), None, Some(MAX38), Some(0), Some(-MAX38 + 5)]
    );
}
