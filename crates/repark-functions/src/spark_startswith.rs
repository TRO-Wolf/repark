use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use crate::bitmap_agg::spark_type_name;
use crate::java_double::{java_double_strings, java_float_strings};

#[must_use]
pub fn startswith_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkStartsWith::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![startswith_udf()]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug)]
struct SparkStartsWith {
    signature: Signature,
}

impl SparkStartsWith {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkStartsWith {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkStartsWith {}

impl Hash for SparkStartsWith {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_nested_container(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => true,
        DataType::Dictionary(_, value_type) => is_nested_container(value_type),
        _ => false,
    }
}

fn wrong_num_args(got: usize) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `startswith` requires 2 parameters but the \
         actual number is {got}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

fn datatype_refusal(position: &str, data_type: &DataType) -> DataFusionError {
    let got = spark_type_name(data_type);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"startswith\" due to data \
         type mismatch: The {position} parameter requires the \"STRING\" type, however the \
         argument has the type \"{got}\". SQLSTATE: 42K09"
    ))
}

fn coerce_one(position: &str, data_type: &DataType) -> Result<DataType> {
    if is_nested_container(data_type) {
        return Err(datatype_refusal(position, data_type));
    }
    match data_type {
        DataType::Float32 | DataType::Float64 => Ok(data_type.clone()),
        _ => Ok(DataType::Utf8),
    }
}

fn render_strings(array: &ArrayRef) -> Result<StringArray> {
    match array.data_type() {
        DataType::Float64 => Ok(java_double_strings(array.as_primitive::<Float64Type>())),
        DataType::Float32 => Ok(java_float_strings(array.as_primitive::<Float32Type>())),
        _ => {
            let casted = cast(array, &DataType::Utf8)?;
            Ok(casted.as_string::<i32>().clone())
        }
    }
}

impl ScalarUDFImpl for SparkStartsWith {
    crate::shim_udf_boilerplate!("startswith");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [first, second] = arg_types else {
            return Err(wrong_num_args(arg_types.len()));
        };
        Ok(vec![
            coerce_one("first", first)?,
            coerce_one("second", second)?,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = arrays.as_slice() else {
            return Err(wrong_num_args(arrays.len()));
        };
        let left = render_strings(left)?;
        let right = render_strings(right)?;
        let mut builder = BooleanBuilder::with_capacity(left.len());
        for row in 0..left.len() {
            if left.is_null(row) || right.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(left.value(row).starts_with(right.value(row)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::prelude::SessionContext;

    use super::startswith_udf;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(startswith_udf().as_ref().clone());
        ctx
    }

    fn ctx_register_all() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn one_bool(ctx: &SessionContext, sql: &str) -> Option<bool> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let values = batches[0].column(0).as_boolean();
        if values.is_null(0) {
            None
        } else {
            Some(values.value(0))
        }
    }

    async fn id_column(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let values = batches[0].column(0).as_primitive::<Int64Type>();
        (0..values.len()).map(|row| values.value(row)).collect()
    }

    #[tokio::test]
    async fn where_prefix_answers_one_two_with_null_and_empty_excluded() {
        let ctx = ctx();
        let ids = id_column(
            &ctx,
            "SELECT id FROM (VALUES (1, 'apple'), (2, 'apricot'), (3, 'banana'), \
             (4, CAST(NULL AS VARCHAR)), (6, ''), (7, 'Zeta')) AS t(id, s) \
             WHERE startswith(s, 'ap') ORDER BY id",
        )
        .await;
        assert_eq!(ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn scalar_answers_match_spark() {
        let ctx = ctx();
        let cases: &[(&str, Option<bool>)] = &[
            ("SELECT startswith('apple', 'ap')", Some(true)),
            ("SELECT startswith('banana', 'ap')", Some(false)),
            ("SELECT startswith(CAST(NULL AS VARCHAR), 'ap')", None),
            ("SELECT startswith(NULL, 'ap')", None),
            ("SELECT startswith('', 'ap')", Some(false)),
            ("SELECT startswith('abc', CAST(NULL AS VARCHAR))", None),
            ("SELECT startswith('abc', NULL)", None),
            ("SELECT startswith('abc', '')", Some(true)),
            ("SELECT startswith('', '')", Some(true)),
            ("SELECT startswith(CAST(NULL AS VARCHAR), '')", None),
            ("SELECT startswith('日本語テスト', '日本')", Some(true)),
            ("SELECT startswith('日本語テスト', '本日')", Some(false)),
            ("SELECT startswith('é', '')", Some(true)),
            ("SELECT startswith('Apple', 'ap')", Some(false)),
            ("SELECT startswith('ap', 'apple')", Some(false)),
            ("SELECT startswith('xap', 'ap')", Some(false)),
            ("SELECT startswith('apple', 'ap%')", Some(false)),
        ];
        for (sql, expected) in cases {
            assert_eq!(one_bool(&ctx, sql).await, *expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn non_string_inputs_stringify_like_spark() {
        let ctx = ctx();
        let cases: &[(&str, Option<bool>)] = &[
            ("SELECT startswith(123, 'a')", Some(false)),
            ("SELECT startswith(123, '1')", Some(true)),
            ("SELECT startswith('abc', 1)", Some(false)),
            ("SELECT startswith('1.5', 1.5)", Some(true)),
            ("SELECT startswith(true, 't')", Some(true)),
            ("SELECT startswith(1.5, '1')", Some(true)),
            (
                "SELECT startswith(CAST('Infinity' AS DOUBLE), 'Inf')",
                Some(true),
            ),
            (
                "SELECT startswith(CAST('-Infinity' AS DOUBLE), '-Inf')",
                Some(true),
            ),
            ("SELECT startswith(CAST('NaN' AS DOUBLE), 'Na')", Some(true)),
            (
                "SELECT startswith(CAST(-1e300 AS DOUBLE), '-1')",
                Some(true),
            ),
            (
                "SELECT startswith(CAST(-1e300 AS DOUBLE), '-1e')",
                Some(false),
            ),
            (
                "SELECT startswith(CAST('Infinity' AS FLOAT), 'Inf')",
                Some(true),
            ),
            (
                "SELECT startswith(CAST(10.50 AS DECIMAL(6, 2)), '10.50')",
                Some(true),
            ),
            ("SELECT startswith(DATE '2024-01-01', '2024')", Some(true)),
            (
                "SELECT startswith(TIMESTAMP '2024-01-01 12:00:00', '2024')",
                Some(true),
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(one_bool(&ctx, sql).await, *expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn binary_inputs_decode_like_spark() {
        let ctx = ctx();
        assert_eq!(
            one_bool(&ctx, "SELECT startswith(X'6162', 'a')").await,
            Some(true)
        );
        assert_eq!(
            one_bool(&ctx, "SELECT startswith('ab', X'61')").await,
            Some(true)
        );
    }

    async fn refusal_message(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Ok(frame) => match frame.collect().await {
                Ok(_) => panic!("{sql} must refuse"),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        }
    }

    #[tokio::test]
    async fn wrong_arity_refuses_with_spark_shape() {
        let ctx = ctx();
        for (sql, got) in [
            ("SELECT startswith()", 0),
            ("SELECT startswith('abc')", 1),
            ("SELECT startswith('abc', 'a', 'b')", 3),
        ] {
            let message = refusal_message(&ctx, sql).await;
            assert!(
                message.contains("[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]")
                    && message.contains("requires 2 parameters")
                    && message.contains(&format!("actual number is {got}")),
                "{sql}: {message}"
            );
        }
    }

    #[tokio::test]
    async fn nested_inputs_refuse_with_spark_shape() {
        let ctx = ctx();
        for sql in [
            "SELECT startswith([1, 2], 'a')",
            "SELECT startswith('a', [1, 2])",
            "SELECT startswith(named_struct('a', 1), 'a')",
        ] {
            let message = refusal_message(&ctx, sql).await;
            assert!(
                message.contains("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]")
                    && message.contains("requires the \"STRING\" type"),
                "{sql}: {message}"
            );
        }
    }

    #[tokio::test]
    async fn register_all_keeps_datafusion_starts_with() {
        let ctx = ctx_register_all();
        assert_eq!(
            one_bool(&ctx, "SELECT starts_with('apple', 'ap')").await,
            Some(true)
        );
        assert_eq!(
            one_bool(&ctx, "SELECT startswith('apple', 'ap')").await,
            Some(true)
        );
    }
}
