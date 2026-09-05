use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

use crate::datetime::{
    JavaPatternToken, compile_java_pattern, format_compiled_java_pattern,
    local_datetime_from_micros,
};
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

const DEFAULT_PATTERN: &str = "yyyy-MM-dd HH:mm:ss";
const MICROS_PER_SECOND: i64 = 1_000_000;

#[must_use]
pub fn from_unixtime_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFromUnixtime::new()))
}

#[derive(Debug)]
struct SparkFromUnixtime {
    signature: Signature,
}

impl SparkFromUnixtime {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkFromUnixtime {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFromUnixtime {}

impl Hash for SparkFromUnixtime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkFromUnixtime {
    crate::shim_udf_boilerplate!("from_unixtime");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("from_unixtime", DataType::Utf8, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 2 || arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "'from_unixtime' expects 1 or 2 arguments, got {}",
                arg_types.len()
            )));
        }
        if !is_epoch_intake(&arg_types[0]) {
            return Err(DataFusionError::Plan(format!(
                "'from_unixtime' expects an integer, float, decimal, or string unix time, got {}",
                arg_types[0]
            )));
        }
        if arg_types.len() == 2 {
            return Ok(vec![DataType::Int64, DataType::Utf8]);
        }
        Ok(vec![DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let seconds = cast(arrays[0].as_ref(), &DataType::Int64)?;
        let seconds = seconds.as_primitive::<Int64Type>();
        let zone =
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?;
        let format_array;
        let formats = if arrays.len() == 2 {
            format_array = cast(arrays[1].as_ref(), &DataType::Utf8)?;
            Some(format_array.as_string::<i32>())
        } else {
            None
        };
        let mut cached: Option<(String, Vec<JavaPatternToken>)> = None;
        let mut builder = StringBuilder::with_capacity(seconds.len(), seconds.len() * 19);
        for row in 0..seconds.len() {
            if seconds.is_null(row) {
                builder.append_null();
                continue;
            }
            let pattern = match formats {
                Some(values) => {
                    if values.is_null(row) {
                        builder.append_null();
                        continue;
                    }
                    values.value(row)
                }
                None => DEFAULT_PATTERN,
            };
            let needs_compile = match &cached {
                Some((previous, _)) => previous.as_str() != pattern,
                None => true,
            };
            if needs_compile {
                let tokens = compile_java_pattern(pattern)?;
                cached = Some((pattern.to_string(), tokens));
            }
            let tokens = cached.as_ref().map(|(_, tokens)| tokens.as_slice());
            let Some(tokens) = tokens else {
                return Err(DataFusionError::Execution(
                    "from_unixtime: internal pattern cache miss".to_string(),
                ));
            };
            let micros = seconds.value(row).wrapping_mul(MICROS_PER_SECOND);
            match local_datetime_from_micros(micros, zone) {
                Some(datetime) => {
                    builder.append_value(format_compiled_java_pattern(datetime, tokens)?);
                }
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn is_epoch_intake(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, values) => is_epoch_intake(values),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_in(zone: &str) -> SessionContext {
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udf(from_unixtime_udf().as_ref().clone());
        ctx
    }

    async fn one_string(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        assert_eq!(batches.len(), 1);
        let batch: &RecordBatch = &batches[0];
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        array.is_valid(0).then(|| array.value(0).to_string())
    }

    #[tokio::test]
    async fn renders_epoch_in_utc() {
        let ctx = ctx_in("UTC");
        assert_eq!(
            one_string(&ctx, "SELECT from_unixtime(0) AS v").await,
            Some("1970-01-01 00:00:00".to_string())
        );
    }

    #[tokio::test]
    async fn renders_epoch_in_new_york() {
        let ctx = ctx_in("America/New_York");
        assert_eq!(
            one_string(&ctx, "SELECT from_unixtime(0) AS v").await,
            Some("1969-12-31 19:00:00".to_string())
        );
    }

    #[tokio::test]
    async fn renders_explicit_pattern() {
        let ctx = ctx_in("UTC");
        assert_eq!(
            one_string(&ctx, "SELECT from_unixtime(0, 'yyyy/MM/dd') AS v").await,
            Some("1970/01/01".to_string())
        );
    }

    #[tokio::test]
    async fn null_in_null_out() {
        let ctx = ctx_in("UTC");
        assert_eq!(
            one_string(&ctx, "SELECT from_unixtime(CAST(NULL AS BIGINT)) AS v").await,
            None
        );
        assert_eq!(
            one_string(&ctx, "SELECT from_unixtime(0, CAST(NULL AS STRING)) AS v").await,
            None
        );
    }

    #[tokio::test]
    async fn boolean_argument_refuses() {
        let ctx = ctx_in("UTC");
        let error = ctx
            .sql("SELECT from_unixtime(true) AS v")
            .await
            .expect_err("a boolean unix time must not plan");
        assert!(error.to_string().contains("from_unixtime"), "got {error}");
    }
}
