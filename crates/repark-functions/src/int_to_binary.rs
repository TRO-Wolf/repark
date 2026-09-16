use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, Int8Array, Int16Array, Int32Array, Int64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::ansi::spark_ansi_enabled_from_options;

pub(crate) const INT_TO_BINARY_NAME: &str = "__repark_int_to_binary__";

#[derive(Debug, Default)]
pub(crate) struct IntToBinaryCast;

impl AnalyzerRule for IntToBinaryCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi_enabled = spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, ansi_enabled))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "int_to_binary_cast"
    }
}

fn rewrite_plan(plan: LogicalPlan, ansi_enabled: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema, ansi_enabled))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

#[allow(clippy::missing_errors_doc)]
fn rewrite_expr(expr: Expr, schema: &DFSchema, ansi_enabled: bool) -> Result<Transformed<Expr>> {
    match expr {
        Expr::Cast(cast) => rewrite_cast(cast, schema, ansi_enabled),
        Expr::TryCast(cast) => refuse_try_cast(cast, schema),
        other => Ok(Transformed::no(other)),
    }
}

#[allow(clippy::missing_errors_doc)]
fn rewrite_cast(cast: Cast, schema: &DFSchema, ansi_enabled: bool) -> Result<Transformed<Expr>> {
    if !matches!(cast.field.data_type(), DataType::Binary) {
        return Ok(Transformed::no(Expr::Cast(cast)));
    }
    let Ok(source) = cast.expr.get_type(schema) else {
        return Ok(Transformed::no(Expr::Cast(cast)));
    };
    if is_binary_legal_source(&source) {
        return Ok(Transformed::no(Expr::Cast(cast)));
    }
    if !ansi_enabled && is_integral_source(&source) {
        return Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(int_to_binary_udf(), vec![cast.expr.as_ref().clone()]),
        )));
    }
    Err(illegal_binary_cast_error(&cast.expr, &source, false))
}

#[allow(clippy::missing_errors_doc)]
fn refuse_try_cast(cast: TryCast, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !matches!(cast.field.data_type(), DataType::Binary) {
        return Ok(Transformed::no(Expr::TryCast(cast)));
    }
    let Ok(source) = cast.expr.get_type(schema) else {
        return Ok(Transformed::no(Expr::TryCast(cast)));
    };
    if is_binary_legal_source(&source) {
        return Ok(Transformed::no(Expr::TryCast(cast)));
    }
    Err(illegal_binary_cast_error(&cast.expr, &source, true))
}

fn is_binary_legal_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Null
    )
}

fn is_integral_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn illegal_binary_cast_error(
    inner: &Expr,
    source: &DataType,
    is_try_cast: bool,
) -> DataFusionError {
    let keyword = if is_try_cast { "TRY_CAST" } else { "CAST" };
    let from = spark_source_type_name(source);
    if !is_try_cast && is_integral_source(source) {
        DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION] Cannot resolve \"{keyword}({inner} AS BINARY)\" \
             due to data type mismatch: cannot cast \"{from}\" to \"BINARY\" with ANSI mode on.\n\
             If you have to cast \"{from}\" to \"BINARY\", you can set \"spark.sql.ansi.enabled\" as 'false'. \
             SQLSTATE: 42K09"
        ))
    } else {
        DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"{keyword}({inner} AS BINARY)\" \
             due to data type mismatch: cannot cast \"{from}\" to \"BINARY\". SQLSTATE: 42K09"
        ))
    }
}

fn spark_source_type_name(source: &DataType) -> String {
    match source {
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Interval(IntervalUnit::YearMonth) => "INTERVAL YEAR TO MONTH".to_string(),
        DataType::Interval(_) => "INTERVAL DAY".to_string(),
        other => other.to_string(),
    }
}

#[must_use]
pub(crate) fn int_to_binary_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IntToBinary::new()))
}

#[derive(Debug)]
struct IntToBinary {
    signature: Signature,
}

impl IntToBinary {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for IntToBinary {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for IntToBinary {}

impl Hash for IntToBinary {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for IntToBinary {
    crate::shim_udf_boilerplate!("__repark_int_to_binary__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(data_type) if is_integral_source(data_type) => Ok(DataType::Binary),
            _ => Err(DataFusionError::Plan(format!(
                "'{INT_TO_BINARY_NAME}' expects one integral argument"
            ))),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{INT_TO_BINARY_NAME}' expects one argument"))
        })?;
        Ok(Field::new(self.name(), DataType::Binary, first.is_nullable()).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.first() {
            Some(ColumnarValue::Scalar(scalar)) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                encode_scalar(scalar)?,
            ))),
            Some(ColumnarValue::Array(array)) => Ok(ColumnarValue::Array(Arc::new(encode_array(
                array.as_ref(),
            )?))),
            None => Err(DataFusionError::Execution(format!(
                "'{INT_TO_BINARY_NAME}' expects one argument"
            ))),
        }
    }
}

#[allow(clippy::missing_errors_doc)]
fn encode_scalar(value: &ScalarValue) -> Result<Option<Vec<u8>>> {
    match value {
        ScalarValue::Int8(bit) => Ok(bit.map(|n| n.to_be_bytes().to_vec())),
        ScalarValue::Int16(bit) => Ok(bit.map(|n| n.to_be_bytes().to_vec())),
        ScalarValue::Int32(bit) => Ok(bit.map(|n| n.to_be_bytes().to_vec())),
        ScalarValue::Int64(bit) => Ok(bit.map(|n| n.to_be_bytes().to_vec())),
        other => Err(DataFusionError::Execution(format!(
            "'{INT_TO_BINARY_NAME}' cannot encode {other} as BINARY"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
fn encode_array(array: &dyn Array) -> Result<BinaryArray> {
    if let Some(ints) = array.as_any().downcast_ref::<Int8Array>() {
        let mut builder = BinaryBuilder::with_capacity(ints.len(), ints.len());
        for index in 0..ints.len() {
            if ints.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(ints.value(index).to_be_bytes());
            }
        }
        return Ok(builder.finish());
    }
    if let Some(ints) = array.as_any().downcast_ref::<Int16Array>() {
        let mut builder = BinaryBuilder::with_capacity(ints.len(), ints.len() * 2);
        for index in 0..ints.len() {
            if ints.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(ints.value(index).to_be_bytes());
            }
        }
        return Ok(builder.finish());
    }
    if let Some(ints) = array.as_any().downcast_ref::<Int32Array>() {
        let mut builder = BinaryBuilder::with_capacity(ints.len(), ints.len() * 4);
        for index in 0..ints.len() {
            if ints.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(ints.value(index).to_be_bytes());
            }
        }
        return Ok(builder.finish());
    }
    if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let mut builder = BinaryBuilder::with_capacity(ints.len(), ints.len() * 8);
        for index in 0..ints.len() {
            if ints.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(ints.value(index).to_be_bytes());
            }
        }
        return Ok(builder.finish());
    }
    Err(DataFusionError::Execution(format!(
        "'{INT_TO_BINARY_NAME}' cannot encode {} as BINARY",
        array.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::Float64Array;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with_ansi(enabled: bool) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), enabled);
        let ctx = SessionContext::new_with_config(config);
        ctx.add_analyzer_rule(Arc::new(IntToBinaryCast));
        ctx
    }

    async fn bytes_of(ctx: &SessionContext, sql: &str) -> (Vec<Option<Vec<u8>>>, DataType, bool) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let field = batches[0].schema().field(0).clone();
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap_or_else(|| panic!("expected Binary for {sql}"));
        let values = (0..column.len())
            .map(|row| (!column.is_null(row)).then(|| column.value(row).to_vec()))
            .collect();
        (values, field.data_type().clone(), field.is_nullable())
    }

    async fn refusal(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
        }
    }

    #[tokio::test]
    async fn ansi_off_encodes_each_width_big_endian() {
        let ctx = ctx_with_ansi(false);
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(1 AS TINYINT) AS BYTEA) AS b").await,
            (vec![Some(vec![0x01])], DataType::Binary, false)
        );
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(1 AS SMALLINT) AS BYTEA) AS b").await,
            (vec![Some(vec![0x00, 0x01])], DataType::Binary, false)
        );
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(1 AS INT) AS BYTEA) AS b").await,
            (
                vec![Some(vec![0x00, 0x00, 0x00, 0x01])],
                DataType::Binary,
                false
            )
        );
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(-1 AS INT) AS BYTEA) AS b").await,
            (
                vec![Some(vec![0xff, 0xff, 0xff, 0xff])],
                DataType::Binary,
                false
            )
        );
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(305419896 AS INT) AS BYTEA) AS b").await,
            (
                vec![Some(vec![0x12, 0x34, 0x56, 0x78])],
                DataType::Binary,
                false
            )
        );
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(1 AS BIGINT) AS BYTEA) AS b").await,
            (
                vec![Some(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01])],
                DataType::Binary,
                false
            )
        );
    }

    #[tokio::test]
    async fn ansi_off_null_stays_null_and_nullable() {
        let ctx = ctx_with_ansi(false);
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(NULL AS INT) AS BYTEA) AS b").await,
            (vec![None], DataType::Binary, true)
        );
    }

    #[tokio::test]
    async fn ansi_off_string_cast_is_untouched() {
        let ctx = ctx_with_ansi(false);
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST('ab' AS BYTEA) AS b").await,
            (vec![Some(b"ab".to_vec())], DataType::Binary, false)
        );
    }

    #[tokio::test]
    async fn ansi_off_never_castable_sources_refuse_without_suggestion() {
        let ctx = ctx_with_ansi(false);
        for (sql, source) in [
            ("SELECT CAST(CAST(1.5 AS FLOAT) AS BYTEA) AS b", "FLOAT"),
            ("SELECT CAST(CAST(1.5 AS DOUBLE) AS BYTEA) AS b", "DOUBLE"),
            (
                "SELECT CAST(CAST(1.5 AS DECIMAL(10,2)) AS BYTEA) AS b",
                "DECIMAL(10,2)",
            ),
            ("SELECT CAST(true AS BYTEA) AS b", "BOOLEAN"),
            ("SELECT CAST(DATE '2024-01-01' AS BYTEA) AS b", "DATE"),
            (
                "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' AS BYTEA) AS b",
                "TIMESTAMP",
            ),
            (
                "SELECT CAST(INTERVAL '1' DAY AS BYTEA) AS b",
                "INTERVAL DAY",
            ),
        ] {
            let message = refusal(&ctx, sql).await;
            assert!(
                message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
                    && message.contains(source)
                    && !message.contains("CAST_WITH_CONF_SUGGESTION"),
                "{sql}: expected WITHOUT_SUGGESTION naming {source}, got {message}"
            );
        }
    }

    #[tokio::test]
    async fn ansi_off_try_cast_of_int_refuses_without_suggestion() {
        let ctx = ctx_with_ansi(false);
        let message = refusal(&ctx, "SELECT TRY_CAST(1 AS BYTEA) AS b").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
                && !message.contains("CAST_WITH_CONF_SUGGESTION"),
            "TRY_CAST must never carry the conf suggestion, got {message}"
        );
    }

    #[tokio::test]
    async fn ansi_on_integrals_refuse_with_conf_suggestion_and_remedy() {
        let ctx = ctx_with_ansi(true);
        for (sql, source) in [
            ("SELECT CAST(CAST(1 AS TINYINT) AS BYTEA) AS b", "TINYINT"),
            ("SELECT CAST(CAST(1 AS SMALLINT) AS BYTEA) AS b", "SMALLINT"),
            ("SELECT CAST(CAST(1 AS INT) AS BYTEA) AS b", "INT"),
            ("SELECT CAST(CAST(1 AS BIGINT) AS BYTEA) AS b", "BIGINT"),
            ("SELECT CAST(CAST(NULL AS INT) AS BYTEA) AS b", "INT"),
        ] {
            let message = refusal(&ctx, sql).await;
            assert!(
                message.contains("[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION]")
                    && message.contains(&format!("cannot cast \"{source}\" to \"BINARY\""))
                    && message.contains("spark.sql.ansi.enabled")
                    && message.contains("SQLSTATE: 42K09"),
                "{sql}: expected CONF_SUGGESTION naming {source}, got {message}"
            );
        }
    }

    #[tokio::test]
    async fn ansi_on_never_castable_sources_keep_without_suggestion() {
        let ctx = ctx_with_ansi(true);
        let message = refusal(&ctx, "SELECT CAST(CAST(1.5 AS DOUBLE) AS BYTEA) AS b").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
                && !message.contains("CAST_WITH_CONF_SUGGESTION"),
            "DOUBLE keeps WITHOUT_SUGGESTION under ANSI on, got {message}"
        );
    }

    #[tokio::test]
    async fn missing_carrier_defaults_to_ansi_on_refusal() {
        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(IntToBinaryCast));
        let message = refusal(&ctx, "SELECT CAST(1 AS BYTEA) AS b").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION]"),
            "no carrier must refuse like ANSI on, got {message}"
        );
    }

    #[tokio::test]
    async fn double_analysis_encodes_exactly_once() {
        let ctx = ctx_with_ansi(false);
        let plan = ctx
            .state()
            .create_logical_plan("SELECT CAST(CAST(1 AS INT) AS BYTEA) AS b")
            .await
            .unwrap();
        let once = crate::analyze_eagerly(&ctx.state(), plan).unwrap();
        let twice = crate::analyze_eagerly(&ctx.state(), once.clone()).unwrap();
        assert_eq!(
            format!("{}", twice.display_indent()),
            format!("{}", once.display_indent()),
            "a second analysis must not rewrap the encoder"
        );
        assert_eq!(
            bytes_of(&ctx, "SELECT CAST(CAST(1 AS INT) AS BYTEA) AS b")
                .await
                .0,
            vec![Some(vec![0x00, 0x00, 0x00, 0x01])]
        );
    }

    #[test]
    fn kernel_reports_binary_and_inherits_nullability() {
        let udf = IntToBinary::new();
        assert_eq!(
            udf.return_type(&[DataType::Int32]).unwrap(),
            DataType::Binary
        );
        assert!(
            udf.return_type(&[DataType::Float64]).is_err(),
            "a float input must fail loud"
        );
        let nullable = Arc::new(Field::new("v", DataType::Int64, true));
        let field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[nullable],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(field.is_nullable(), "nullability follows the input");
    }

    #[test]
    fn kernel_encodes_scalars_and_arrays_with_nulls() {
        assert_eq!(
            encode_scalar(&ScalarValue::Int8(Some(1))).unwrap(),
            Some(vec![0x01])
        );
        assert_eq!(
            encode_scalar(&ScalarValue::Int16(Some(-1))).unwrap(),
            Some(vec![0xff, 0xff])
        );
        assert_eq!(encode_scalar(&ScalarValue::Int32(None)).unwrap(), None);
        assert!(
            encode_scalar(&ScalarValue::Float64(Some(1.5))).is_err(),
            "a float scalar must fail loud"
        );
        let array = Int32Array::from(vec![Some(1), None, Some(-1)]);
        let encoded = encode_array(&array).unwrap();
        assert_eq!(encoded.len(), 3);
        assert_eq!(encoded.value(0), &[0x00, 0x00, 0x00, 0x01]);
        assert!(encoded.is_null(1));
        assert_eq!(encoded.value(2), &[0xff, 0xff, 0xff, 0xff]);
        let floats = Float64Array::from(vec![Some(1.5)]);
        assert!(
            encode_array(&floats).is_err(),
            "a float array must fail loud"
        );
    }
}
