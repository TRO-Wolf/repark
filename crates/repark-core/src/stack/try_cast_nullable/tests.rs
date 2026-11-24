use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    Cast, Expr, LogicalPlanBuilder, Operator, TryCast, binary_expr, col, lit,
};
use datafusion::physical_expr::expressions::{Column, TryCastExpr};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, displayable};

use super::{
    NULLABLE_FIELD_NAME, NullableFieldPhysicalExpr, nullable_field_call, nullable_field_udf,
    repair_inlined_try_cast_plan, shield_try_cast_expr, shield_try_cast_plan,
};
use crate::ReparkSession;

fn nonnull_table() -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("b", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("s_dt", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![4_294_967_297_i64, 1])),
            Arc::new(StringArray::from(vec!["abc", "7"])),
            Arc::new(StringArray::from(vec!["notadt", "2024-01-15"])),
        ],
    )
    .expect("nonnull batch");
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable"))
}

fn try_cast_expr(inner: Expr, target: DataType) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(inner), target))
}

#[test]
fn shield_plan_returns_borrowed_without_try_cast_or_with_bare_try_cast() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let scan = LogicalPlanBuilder::empty(false).build().expect("empty");
    assert!(matches!(
        shield_try_cast_plan(&scan).expect("shield empty"),
        Cow::Borrowed(_)
    ));

    let source: Arc<dyn datafusion::datasource::TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("empty memtable"));
    let bare_filter = LogicalPlanBuilder::scan(
        "t",
        datafusion::datasource::provider_as_source(source),
        None,
    )
    .expect("scan")
    .filter(binary_expr(
        try_cast_expr(col("k"), DataType::Int64),
        Operator::Gt,
        lit(500_i64),
    ))
    .expect("filter")
    .project(vec![try_cast_expr(col("s"), DataType::Int32).alias("x")])
    .expect("project")
    .build()
    .expect("plan");
    assert!(matches!(
        shield_try_cast_plan(&bare_filter).expect("shield bare"),
        Cow::Borrowed(_)
    ));
}

#[test]
fn shield_expr_wraps_only_under_consumers_and_is_idempotent() {
    let bare = binary_expr(
        try_cast_expr(col("k"), DataType::Int64),
        Operator::Gt,
        lit(500_i64),
    );
    assert_eq!(shield_try_cast_expr(bare.clone()).expect("bare"), bare);

    let abs_udf = datafusion::functions::math::abs();
    let wrapped_neg = abs_udf.call(vec![Expr::Negative(Box::new(try_cast_expr(
        col("s"),
        DataType::Int32,
    )))]);
    let once = shield_try_cast_expr(wrapped_neg).expect("once");
    assert!(once.to_string().contains(NULLABLE_FIELD_NAME), "{once}");
    let twice = shield_try_cast_expr(once.clone()).expect("twice");
    assert_eq!(once, twice);

    let cast_wrapped = abs_udf.call(vec![Expr::Cast(Cast::new(
        Box::new(try_cast_expr(col("s"), DataType::Int32)),
        DataType::Int64,
    ))]);
    let cast_shielded = shield_try_cast_expr(cast_wrapped).expect("cast shielded");
    assert_eq!(
        cast_shielded
            .to_string()
            .matches(NULLABLE_FIELD_NAME)
            .count(),
        1,
        "{cast_shielded}"
    );

    let nested_try = abs_udf.call(vec![try_cast_expr(
        try_cast_expr(col("s"), DataType::Int32),
        DataType::Int64,
    )]);
    let nested_shielded = shield_try_cast_expr(nested_try).expect("nested");
    assert_eq!(
        nested_shielded
            .to_string()
            .matches(NULLABLE_FIELD_NAME)
            .count(),
        1,
        "{nested_shielded}"
    );

    let outer_bare_inner_fn = try_cast_expr(
        abs_udf.call(vec![try_cast_expr(col("s"), DataType::Int32)]),
        DataType::Utf8,
    );
    let inner_only = shield_try_cast_expr(outer_bare_inner_fn).expect("inner only");
    assert!(
        matches!(inner_only, Expr::TryCast(_)),
        "outer bare try_cast must stay unwrapped: {inner_only}"
    );
    assert_eq!(
        inner_only.to_string().matches(NULLABLE_FIELD_NAME).count(),
        1,
        "{inner_only}"
    );
}

#[test]
fn nullable_field_udf_preserves_schema_name_and_metadata() {
    let raw = try_cast_expr(col("s"), DataType::Int32);
    let wrapped = nullable_field_call(raw.clone());
    assert_eq!(
        wrapped.schema_name().to_string(),
        raw.schema_name().to_string()
    );

    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_string(), "42".to_string());
    let nonnull = Arc::new(Field::new("s", DataType::Int32, false).with_metadata(metadata.clone()));
    let udf = nullable_field_udf();
    let returned = udf
        .return_field_from_args(datafusion::logical_expr::ReturnFieldArgs {
            arg_fields: &[nonnull],
            scalar_arguments: &[None],
        })
        .expect("return field");
    assert!(returned.is_nullable());
    assert_eq!(returned.data_type(), &DataType::Int32);
    assert_eq!(returned.metadata(), &metadata);
}

#[test]
fn repair_inlined_try_cast_wraps_projection_consumer_arguments() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let raw_try: Arc<dyn PhysicalExpr> = Arc::new(TryCastExpr::new(
        Arc::new(Column::new("s", 0)),
        DataType::Int32,
    ));
    assert!(
        !raw_try
            .return_field(&schema)
            .expect("raw field")
            .is_nullable(),
        "DataFusion 54.1 TryCastExpr::return_field bug precondition"
    );

    let call_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        "abs",
        datafusion::functions::math::abs(),
        vec![Arc::clone(&raw_try)],
        Arc::new(Field::new("r", DataType::Int32, true)),
        Arc::new(datafusion::config::ConfigOptions::default()),
    ));
    let projection: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(raw_try, "bare"),
                ProjectionExpr::new(call_expr, "wrapped"),
            ],
            input,
        )
        .expect("projection"),
    );

    let repaired = repair_inlined_try_cast_plan(Arc::clone(&projection)).expect("repair");
    assert_eq!(repaired.schema(), projection.schema());
    let proj = repaired
        .downcast_ref::<ProjectionExec>()
        .expect("ProjectionExec");
    assert!(
        proj.expr()[0].expr.downcast_ref::<TryCastExpr>().is_some(),
        "bare TryCastExpr must stay unwrapped"
    );
    let fn_expr = proj.expr()[1]
        .expr
        .downcast_ref::<ScalarFunctionExpr>()
        .expect("ScalarFunctionExpr");
    let wrapped_arg = &fn_expr.args()[0];
    assert!(
        wrapped_arg
            .downcast_ref::<NullableFieldPhysicalExpr>()
            .is_some(),
        "inlined TryCastExpr inside ScalarFunctionExpr must be wrapped"
    );
    assert!(
        wrapped_arg
            .return_field(&schema)
            .expect("repaired return_field")
            .is_nullable()
    );
}

#[test]
fn repair_inlined_try_cast_wraps_filter_consumer_arguments() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let raw_try: Arc<dyn PhysicalExpr> = Arc::new(TryCastExpr::new(
        Arc::new(Column::new("s", 0)),
        DataType::Int32,
    ));
    let bare_filter_pred: Arc<dyn PhysicalExpr> = datafusion::physical_expr::expressions::binary(
        Arc::clone(&raw_try),
        Operator::Gt,
        datafusion::physical_expr::expressions::lit(ScalarValue::Int32(Some(0))),
        &schema,
    )
    .expect("bare pred");
    let bare_filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(bare_filter_pred, Arc::clone(&input)).expect("bare filter"));
    let repaired_bare = repair_inlined_try_cast_plan(Arc::clone(&bare_filter)).expect("bare ok");
    assert!(Arc::ptr_eq(&repaired_bare, &bare_filter));

    let fn_in_filter: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        "abs",
        datafusion::functions::math::abs(),
        vec![Arc::clone(&raw_try)],
        Arc::new(Field::new("r", DataType::Int32, true)),
        Arc::new(datafusion::config::ConfigOptions::default()),
    ));
    let unshielded_filter_pred = datafusion::physical_expr::expressions::binary(
        fn_in_filter,
        Operator::Gt,
        datafusion::physical_expr::expressions::lit(ScalarValue::Int32(Some(0))),
        &schema,
    )
    .expect("unshielded pred");
    let unshielded_filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(unshielded_filter_pred, input).expect("unshielded filter"));
    let repaired_filter = repair_inlined_try_cast_plan(unshielded_filter).expect("repaired filter");
    let predicate = repaired_filter
        .downcast_ref::<FilterExec>()
        .expect("FilterExec")
        .predicate();
    assert!(
        predicate
            .children()
            .into_iter()
            .next()
            .expect("lhs")
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("ScalarFunctionExpr")
            .args()[0]
            .downcast_ref::<NullableFieldPhysicalExpr>()
            .is_some()
    );
}

#[tokio::test]
async fn native_session_executes_functions_over_try_cast_and_preserves_bare_plans() {
    let session = ReparkSession::new().expect("session");
    session
        .context()
        .register_table("nn", nonnull_table())
        .expect("register nn");

    let queries = [
        "SELECT id, round(TRY_CAST(s AS DOUBLE), 1) AS r FROM nn ORDER BY id",
        "SELECT id, round(-TRY_CAST(s AS DOUBLE), 1) AS r FROM nn ORDER BY id",
        "SELECT id, date_part('year', TRY_CAST(s_dt AS DATE)) AS r FROM nn ORDER BY id",
        "SELECT id, strpos(TRY_CAST(TRY_CAST(s AS INT) AS VARCHAR), '7') AS r FROM nn ORDER BY id",
        "SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM nn ORDER BY id",
        "SELECT round(TRY_CAST(s AS DOUBLE), 1) FROM nn ORDER BY id",
        "EXPLAIN ANALYZE SELECT id, round(TRY_CAST(s AS DOUBLE), 1) AS r FROM nn ORDER BY id",
    ];
    for sql in queries {
        let batches = session
            .context()
            .sql(sql)
            .await
            .expect(sql)
            .collect()
            .await
            .expect(sql);
        assert!(!batches.is_empty(), "{sql}");
        if !sql.starts_with("EXPLAIN") {
            let schema = batches[0].schema();
            let result_index = schema.fields().len() - 1;
            assert!(schema.field(result_index).is_nullable(), "{sql}");
            assert!(batches[0].column(result_index).is_null(0), "{sql}");
            assert!(!batches[0].column(result_index).is_null(1), "{sql}");
        }
    }

    let bare_plan = session
        .context()
        .sql("SELECT id FROM nn WHERE TRY_CAST(id AS BIGINT) > 1")
        .await
        .expect("bare sql")
        .create_physical_plan()
        .await
        .expect("bare plan");
    let rendered = displayable(bare_plan.as_ref()).indent(true).to_string();
    assert!(
        !rendered.contains(NULLABLE_FIELD_NAME),
        "bare WHERE TRY_CAST must never be wrapped: {rendered}"
    );

    let plain_plan = session
        .context()
        .sql("SELECT id, b + 1 AS b1 FROM nn WHERE id > 1")
        .await
        .expect("plain sql")
        .create_physical_plan()
        .await
        .expect("plain plan");
    let plain_rendered = displayable(plain_plan.as_ref()).indent(true).to_string();
    assert!(
        !plain_rendered.contains(NULLABLE_FIELD_NAME),
        "plain plan must stay untouched: {plain_rendered}"
    );
}
