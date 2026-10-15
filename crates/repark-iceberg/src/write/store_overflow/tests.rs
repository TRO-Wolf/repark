use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use super::*;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.add_analyzer_rule(Arc::new(StoreOverflowCast));
    crate::write::store_cast::register_store_cast_udfs(&ctx);
    let target = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(Arc::clone(&target), vec![vec![]]).unwrap()),
    )
    .unwrap();
    let source = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&source),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![1e19, 42.0])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "s",
        Arc::new(MemTable::try_new(source, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> Result<(), String> {
    match ctx.sql(sql).await {
        Err(error) => Err(error.to_string()),
        Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
    }
}

fn overflow_head(message: &str, from: &str, to: &str, column: &str) {
    assert!(
        message.contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
        "{message}"
    );
    assert!(message.contains(&format!("\"{from}\" type")), "{message}");
    assert!(message.contains(&format!("\"{to}\" type")), "{message}");
    assert!(message.contains(&format!("`{column}`")), "{message}");
    assert!(message.contains("SQLSTATE: 22003"), "{message}");
}

#[tokio::test]
async fn insert_select_const_overflow_refuses_at_plan() {
    let ctx = ctx();
    for value in ["1e19", "-1e19", "1e100"] {
        let message = run(&ctx, &format!("INSERT INTO t SELECT 10, {value}"))
            .await
            .expect_err("const overflow must refuse");
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }
}

#[tokio::test]
async fn insert_select_const_overflow_refuses_over_empty_source() {
    let ctx = ctx();
    let empty = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
    ]));
    ctx.register_table(
        "e",
        Arc::new(MemTable::try_new(empty, vec![vec![]]).unwrap()),
    )
    .unwrap();
    let message = run(&ctx, "INSERT INTO t SELECT id, 1e19 FROM e")
        .await
        .expect_err("const overflow over empty source must refuse");
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
}

#[tokio::test]
async fn insert_values_overflow_refuses_at_plan() {
    let ctx = ctx();
    let message = run(&ctx, "INSERT INTO t VALUES (10, 1e19)")
        .await
        .expect_err("values overflow must refuse");
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
}

#[tokio::test]
async fn insert_column_overflow_refuses_at_collect() {
    let ctx = ctx();
    let message = run(&ctx, "INSERT INTO t SELECT id, d FROM s")
        .await
        .expect_err("column overflow must refuse");
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
}

#[tokio::test]
async fn insert_through_a_subquery_folds_like_spark() {
    let ctx = ctx();
    let message = run(
        &ctx,
        "INSERT INTO t SELECT id, v FROM (SELECT 1 AS id, 1e19 AS v) s",
    )
    .await
    .expect_err("subquery const must refuse");
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
}

#[tokio::test]
async fn in_range_fractional_values_store_truncated() {
    let ctx = ctx();
    run(&ctx, "INSERT INTO t SELECT 10, 42.0")
        .await
        .expect("42.0 stores");
    run(&ctx, "INSERT INTO t SELECT 11, 1.5")
        .await
        .expect("1.5 stores");
    run(&ctx, "INSERT INTO t SELECT 12, -1.5")
        .await
        .expect("-1.5 stores");
    let rows = ctx
        .sql("SELECT id, v FROM t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values: Vec<i64> = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![42, 1, -1]);
}

#[tokio::test]
async fn two_to_the_63_stores_max_like_spark() {
    let ctx = ctx();
    run(&ctx, "INSERT INTO t SELECT 10, 9.223372036854776e18")
        .await
        .expect("2^63 stores");
    let rows = ctx
        .sql("SELECT v FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let value = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(value, i64::MAX);
}

#[tokio::test]
async fn int_pairs_and_strings_keep_their_old_text() {
    let ctx = ctx();
    let narrow = run(&ctx, "INSERT INTO t SELECT 10, CAST(1 AS BIGINT)").await;
    assert!(narrow.is_ok(), "widening stores");
    let message = run(&ctx, "INSERT INTO t SELECT 'abc', 'def'")
        .await
        .expect_err("strings must refuse");
    assert!(
        !message.contains("CAST_OVERFLOW_IN_TABLE_INSERT"),
        "{message}"
    );
}

#[tokio::test]
async fn memtable_update_is_lazy_like_spark() {
    let ctx = ctx();
    run(&ctx, "INSERT INTO t VALUES (1, 0)").await.unwrap();
    run(&ctx, "UPDATE t SET v = 1e19 WHERE false")
        .await
        .expect("where-false stays success");
    let message = run(&ctx, "UPDATE t SET v = 1e19")
        .await
        .expect_err("matched const refuses");
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
}

#[tokio::test]
async fn divisor_guards_swap_under_a_store() {
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct DummyGuard {
        signature: Signature,
    }
    impl ScalarUDFImpl for DummyGuard {
        fn name(&self) -> &str {
            STORE_DIVISOR_GUARD_NAME
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            Ok(arg_types[0].clone())
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(args.args[0].clone())
        }
    }
    let guard = Arc::new(datafusion::logical_expr::ScalarUDF::from(DummyGuard {
        signature: Signature::any(1, datafusion::logical_expr::Volatility::Immutable),
    }));
    let issue = store_issue("v", &DataType::Float64, &DataType::Int64).unwrap();
    let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
        Box::new(Expr::Literal(ScalarValue::Float64(Some(0.0)), None)),
        datafusion::logical_expr::Operator::Divide,
        Box::new(Expr::ScalarFunction(ScalarFunction::new_udf(
            guard,
            vec![Expr::Literal(ScalarValue::Float64(Some(0.0)), None)],
        ))),
    ));
    let swapped = swap_guards_in_expr(expr, &issue);
    let text = format!("{swapped:?}");
    assert!(text.contains("StoreIntGuard"), "{text}");
    assert!(!text.contains("DummyGuard"), "{text}");
}

async fn stored_pair(config: datafusion::prelude::SessionConfig) -> (i32, i32) {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Fields, Int32Type};
    let ctx = SessionContext::new_with_config(config);
    ctx.add_analyzer_rule(Arc::new(StoreOverflowCast));
    crate::write::store_cast::register_store_cast_udfs(&ctx);
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]);
    let target = Arc::new(Schema::new(vec![Field::new(
        "st",
        DataType::Struct(fields),
        true,
    )]));
    ctx.register_table(
        "pairs",
        Arc::new(MemTable::try_new(target, vec![vec![]]).unwrap()),
    )
    .unwrap();
    run(
        &ctx,
        "INSERT INTO pairs SELECT named_struct('b', CAST(2 AS INT), 'a', CAST(1 AS INT))",
    )
    .await
    .unwrap();
    let batches = ctx
        .sql("SELECT st FROM pairs")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let stored = batches[0].column(0).as_struct();
    let field = |name: &str| -> i32 {
        stored
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Int32Type>()
            .value(0)
    };
    (field("a"), field("b"))
}

#[tokio::test]
async fn an_insert_pairs_struct_fields_by_position_unless_its_statement_says_by_name() {
    let positional = stored_pair(datafusion::prelude::SessionConfig::new()).await;
    assert_eq!(positional, (2, 1));
    let by_name = stored_pair(
        datafusion::prelude::SessionConfig::new().with_option_extension(
            crate::write::struct_pairing::StatementPairing {
                source_by_name: true,
                case_sensitive: false,
            },
        ),
    )
    .await;
    assert_eq!(by_name, (1, 2));
}
