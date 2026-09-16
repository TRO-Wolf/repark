use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::datetime::local_datetime_from_micros;
use crate::temporal_ctor::resolve_session_zone;

pub(crate) fn unsupported_time_type() -> DataFusionError {
    DataFusionError::Plan(
        "[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported. SQLSTATE: 0A000".to_string(),
    )
}

#[must_use]
pub(crate) fn make_time_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(TimeRefusal::new("make_time")))
}

#[must_use]
pub(crate) fn to_time_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(TimeRefusal::new("to_time")))
}

#[must_use]
pub(crate) fn time_diff_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(TimeRefusal::new("time_diff")))
}

#[must_use]
pub(crate) fn time_trunc_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(TimeRefusal::new("time_trunc")))
}

#[must_use]
pub(crate) fn current_time_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(CurrentTime::new()))
}

#[must_use]
pub(crate) fn type_of_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTypeof::new()))
}

#[must_use]
pub(crate) fn hour_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DatePartWithoutTime::hour()))
}

#[must_use]
pub(crate) fn minute_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DatePartWithoutTime::minute()))
}

#[must_use]
pub(crate) fn second_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DatePartWithoutTime::second()))
}

pub(crate) fn time_cast_guard_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(TimeCastGuard)
}

#[derive(Debug)]
struct TimeRefusal {
    name: &'static str,
    signature: Signature,
}

impl TimeRefusal {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }

    fn arity(&self) -> (usize, usize) {
        match self.name {
            "make_time" | "time_diff" => (3, 3),
            "to_time" => (1, 2),
            _ => (2, 2),
        }
    }
}

impl PartialEq for TimeRefusal {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for TimeRefusal {}

impl Hash for TimeRefusal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for TimeRefusal {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(unsupported_time_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Err(unsupported_time_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let (low, high) = self.arity();
        if arg_types.len() < low || arg_types.len() > high {
            let want = if low == high {
                low.to_string()
            } else {
                format!("{low} or {high}")
            };
            return exec_err!(
                "'{}' expects {} arguments, got {}",
                self.name,
                want,
                arg_types.len()
            );
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(unsupported_time_type())
    }
}

#[derive(Debug)]
struct CurrentTime {
    signature: Signature,
}

impl CurrentTime {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl PartialEq for CurrentTime {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for CurrentTime {}

impl Hash for CurrentTime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn current_time_precision(args: &[ColumnarValue]) -> Result<i64> {
    if args.is_empty() {
        return Ok(6);
    }
    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => Ok(*value),
        ColumnarValue::Scalar(ScalarValue::Int64(None) | ScalarValue::Null) => Ok(6),
        _ => Err(DataFusionError::Plan(
            "current_time: precision must be an INT literal".to_string(),
        )),
    }
}

fn current_time_out_of_range(precision: i64) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve \"current_time({precision})\" \
         due to data type mismatch: The `precision` must be between [0, 6] \
         (current value = {precision}). SQLSTATE: 42K09"
    ))
}

fn current_time_nanos(precision: i64, options: &ConfigOptions) -> Result<i64> {
    if !(0..=6).contains(&precision) {
        return Err(current_time_out_of_range(precision));
    }
    let zone = resolve_session_zone(options)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let micros = i64::try_from(now.as_micros()).map_err(|_| {
        DataFusionError::Execution("current_time: system time overflows i64".to_string())
    })?;
    let wall = local_datetime_from_micros(micros, zone).ok_or_else(|| {
        DataFusionError::Execution("current_time: system time out of range".to_string())
    })?;
    let day_micros = wall.and_utc().timestamp_micros().rem_euclid(86_400_000_000);
    let factor = 10_i64.pow(u32::try_from(6 - precision).map_err(|_| {
        DataFusionError::Execution("current_time: precision out of range".to_string())
    })?);
    Ok((day_micros - day_micros.rem_euclid(factor)) * 1_000)
}

impl ScalarUDFImpl for CurrentTime {
    crate::shim_udf_boilerplate!("current_time");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Nanosecond))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Field::new(self.name(), DataType::Time64(TimeUnit::Nanosecond), false).into())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 1 {
            return exec_err!(
                "'current_time' expects 0 or 1 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![DataType::Int64; arg_types.len()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let precision = current_time_precision(&args.args)?;
        let nanos = current_time_nanos(precision, args.config_options.as_ref())?;
        Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(
            nanos,
        ))))
    }
}

#[derive(Debug)]
struct SparkTypeof {
    signature: Signature,
}

impl SparkTypeof {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkTypeof {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkTypeof {}

impl Hash for SparkTypeof {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn spark_type_name(data_type: &DataType) -> Result<String> {
    match data_type {
        DataType::Null => Ok("void".to_string()),
        DataType::Boolean => Ok("boolean".to_string()),
        DataType::Int8 => Ok("tinyint".to_string()),
        DataType::Int16 => Ok("smallint".to_string()),
        DataType::Int32 => Ok("int".to_string()),
        DataType::Int64 => Ok("bigint".to_string()),
        DataType::Float32 => Ok("float".to_string()),
        DataType::Float64 => Ok("double".to_string()),
        DataType::Decimal128(precision, scale)
        | DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal256(precision, scale) => Ok(format!("decimal({precision},{scale})")),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok("string".to_string()),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Ok("binary".to_string())
        }
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Time32(TimeUnit::Second) => Ok("time(0)".to_string()),
        DataType::Time32(TimeUnit::Millisecond) => Ok("time(3)".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("time(6)".to_string()),
        DataType::Timestamp(_, None) => Ok("timestamp_ntz".to_string()),
        DataType::Timestamp(_, Some(_)) => Ok("timestamp".to_string()),
        DataType::List(field) | DataType::LargeList(field) => {
            Ok(format!("array<{}>", spark_type_name(field.data_type())?))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => Ok(format!(
                "map<{},{}>",
                spark_type_name(pair[0].data_type())?,
                spark_type_name(pair[1].data_type())?
            )),
            _ => Err(typeof_unimplemented(data_type)),
        },
        DataType::Struct(fields) => {
            let mut rendered = String::from("struct<");
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    rendered.push(',');
                }
                rendered.push_str(field.name());
                rendered.push(':');
                rendered.push_str(&spark_type_name(field.data_type())?);
            }
            rendered.push('>');
            Ok(rendered)
        }
        other => Err(typeof_unimplemented(other)),
    }
}

fn typeof_unimplemented(data_type: &DataType) -> DataFusionError {
    DataFusionError::NotImplemented(format!("typeof({data_type}) is not implemented"))
}

fn typeof_arity(arg_count: usize) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `typeof` requires 1 parameters but the actual \
         number is {arg_count}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

impl ScalarUDFImpl for SparkTypeof {
    crate::shim_udf_boilerplate!("typeof");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(typeof_arity(arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(typeof_arity(args.args.len()));
        }
        let name = match &args.args[0] {
            ColumnarValue::Array(array) => spark_type_name(array.data_type())?,
            ColumnarValue::Scalar(scalar) => spark_type_name(&scalar.data_type())?,
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))))
    }
}

#[derive(Debug)]
struct DatePartWithoutTime {
    name: &'static str,
    inner: Arc<ScalarUDF>,
}

impl DatePartWithoutTime {
    fn hour() -> Self {
        Self {
            name: "hour",
            inner: crate::datetime::hour_udf(),
        }
    }

    fn minute() -> Self {
        Self {
            name: "minute",
            inner: crate::datetime::minute_udf(),
        }
    }

    fn second() -> Self {
        Self {
            name: "second",
            inner: crate::datetime::second_udf(),
        }
    }

    fn refuse_time(arg_types: &[DataType]) -> Result<()> {
        if arg_types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Time32(_) | DataType::Time64(_)))
        {
            return Err(unsupported_time_type());
        }
        Ok(())
    }
}

impl PartialEq for DatePartWithoutTime {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for DatePartWithoutTime {}

impl Hash for DatePartWithoutTime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for DatePartWithoutTime {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Self::refuse_time(arg_types)?;
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        Self::refuse_time(&arg_types)?;
        self.inner.return_field_from_args(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_types: Vec<DataType> = args
            .args
            .iter()
            .map(|value| match value {
                ColumnarValue::Array(array) => array.data_type().clone(),
                ColumnarValue::Scalar(scalar) => scalar.data_type(),
            })
            .collect();
        Self::refuse_time(&arg_types)?;
        self.inner.invoke_with_args(args)
    }
}

#[derive(Debug, Default)]
struct TimeCastGuard;

fn is_string_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            ScalarValue::Utf8(_) | ScalarValue::Utf8View(_) | ScalarValue::LargeUtf8(_),
            _,
        )
    )
}

fn is_time_cast(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(cast) => {
            matches!(
                cast.field.data_type(),
                DataType::Time32(_) | DataType::Time64(_)
            ) && !is_string_literal(&cast.expr)
        }
        Expr::TryCast(cast) => {
            matches!(
                cast.field.data_type(),
                DataType::Time32(_) | DataType::Time64(_)
            ) && !is_string_literal(&cast.expr)
        }
        _ => false,
    }
}

impl AnalyzerRule for TimeCastGuard {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| {
            node.map_expressions(|expr| {
                if expr.exists(|node| Ok(is_time_cast(node)))? {
                    return Err(unsupported_time_type());
                }
                Ok(Transformed::no(expr))
            })
        })
        .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "time_cast_guard"
    }
}

pub(crate) fn install_time_cast_guard(ctx: &SessionContext) {
    ctx.add_analyzer_rule(time_cast_guard_rule());
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array, StringArray, Time64NanosecondArray};
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx.add_analyzer_rule(time_cast_guard_rule());
        ctx
    }

    async fn error_of(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => match frame.collect().await {
                Err(error) => error.to_string(),
                Ok(_) => "planned".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn time_family_refusals_name_unsupported_time_type() {
        let ctx = ctx();
        for sql in [
            "SELECT make_time(1, 2, 3) AS v",
            "SELECT to_time('10:30') AS v",
            "SELECT time_diff('HOUR', TIME'08:30:00', TIME'12:34:56') AS v",
            "SELECT time_trunc('HOUR', TIME'12:34:56') AS v",
        ] {
            let error = error_of(&ctx, sql).await;
            assert!(error.contains("[UNSUPPORTED_TIME_TYPE]"), "{sql}: {error}");
        }
    }

    #[tokio::test]
    async fn time_family_refusal_arities_report_expected_counts() {
        let ctx = ctx();
        let error = error_of(&ctx, "SELECT make_time(1, 2) AS v").await;
        assert!(error.contains("expects 3 arguments"), "{error}");
        let error = error_of(&ctx, "SELECT time_trunc('HOUR') AS v").await;
        assert!(error.contains("expects 2 arguments"), "{error}");
    }

    #[tokio::test]
    async fn current_time_answers_time64_and_rejects_precision_7() {
        let ctx = ctx();
        for sql in [
            "SELECT current_time AS v",
            "SELECT current_time() AS v",
            "SELECT current_time(0) AS v",
            "SELECT current_time(3) AS v",
        ] {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let array = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap();
            assert!(!array.is_null(0), "{sql} was NULL");
        }
        let error = error_of(&ctx, "SELECT current_time(7) AS v").await;
        assert!(
            error.contains("[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]"),
            "{error}"
        );
        assert!(
            error.contains("between [0, 6] (current value = 7)"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn typeof_answers_spark_names() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT typeof(CAST(1 AS INT)) AS v, typeof('a') AS w")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(row.value(0), "int");
        let row = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(row.value(0), "string");
        let batches = ctx
            .sql("SELECT typeof(TIME'12:34:56') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(row.value(0), "time(6)");
    }

    #[tokio::test]
    async fn hour_over_time_refuses_and_over_timestamp_answers() {
        let ctx = ctx();
        let error = error_of(&ctx, "SELECT hour(current_time()) AS v").await;
        assert!(error.contains("[UNSUPPORTED_TIME_TYPE]"), "{error}");
        let error = error_of(&ctx, "SELECT minute(TIME'12:34:56') AS v").await;
        assert!(error.contains("[UNSUPPORTED_TIME_TYPE]"), "{error}");
        let batches = ctx
            .sql("SELECT hour(TIMESTAMP'2024-01-01 01:00:00') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(row.value(0), 1);
    }

    #[tokio::test]
    async fn time_cast_guard_fires_on_cast_and_spares_literals() {
        let ctx = ctx();
        let error = error_of(&ctx, "SELECT CAST(NULL AS TIME) AS v").await;
        assert!(error.contains("[UNSUPPORTED_TIME_TYPE]"), "{error}");
        let batches = ctx
            .sql("SELECT TIME'12:34:56' AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let batches = ctx
            .sql("SELECT CAST('12:34:56' AS TIME) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }
}
