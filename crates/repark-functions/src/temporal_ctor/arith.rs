use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, TimestampMicrosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::{
    WallFields, broadcast_arrays, datetime_overflow, exec_error, is_last_day, nullable_wall,
    plan_error, resolve_session_zone,
};
use crate::ansi::spark_ansi_enabled_from_options;
use crate::datetime::local_datetime_from_micros;

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(MonthsBetween::new())),
        Arc::new(ScalarUDF::new_from_impl(ConvertTimezone::new())),
        Arc::new(ScalarUDF::new_from_impl(LocalTimestamp::new())),
    ]
}

#[must_use]
pub fn months_between_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(MonthsBetween::new()))
}

#[must_use]
pub fn convert_timezone_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(ConvertTimezone::new()))
}

#[must_use]
pub fn localtimestamp_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(LocalTimestamp::new()))
}

#[derive(Debug)]
struct MonthsBetween {
    signature: Signature,
}

impl MonthsBetween {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for MonthsBetween {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for MonthsBetween {}

impl Hash for MonthsBetween {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for MonthsBetween {
    crate::shim_udf_boilerplate!("months_between");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if matches!(arg_types.len(), 2 | 3) {
            Ok(arg_types.to_vec())
        } else {
            Err(plan_error(format!(
                "'months_between' expects 2 or 3 arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = broadcast_arrays(&arrays);
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let zone = resolve_session_zone(args.config_options.as_ref())?;
        let width = arrays.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<f64>> = Vec::with_capacity(width);
        for row in 0..width {
            let round = if arrays.len() > 2 {
                if let Some(round) = round_argument(arrays[2].as_ref(), row)? {
                    round
                } else {
                    values.push(None);
                    continue;
                }
            } else {
                true
            };
            let first = nullable_wall(&arrays[0], row, zone, ansi)?;
            let second = nullable_wall(&arrays[1], row, zone, ansi)?;
            match (first, second) {
                (Some(first), Some(second)) => {
                    values.push(Some(months_between_walls(&first, &second, round)));
                }
                _ => values.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
    }
}

fn round_argument(array: &dyn Array, row: usize) -> Result<Option<bool>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => {
            let casted = cast(array, &DataType::Boolean)
                .map_err(|_| exec_error("boolean cast failed".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| exec_error("boolean cast failed".to_string()))?;
            Ok(Some(values.value(row)))
        }
        DataType::Null => Ok(None),
        other => Err(plan_error(format!(
            "'months_between' expects a BOOLEAN roundOff argument, got {other}"
        ))),
    }
}

fn months_between_walls(first: &WallFields, second: &WallFields, round: bool) -> f64 {
    if first.year == second.year
        && first.month == second.month
        && first.day == second.day
        && first.day_micros == second.day_micros
    {
        return 0.0;
    }
    let months =
        i64::from(first.year - second.year) * 12 + i64::from(first.month) - i64::from(second.month);
    if first.day == second.day
        || (is_last_day(first.year, first.month, first.day)
            && is_last_day(second.year, second.month, second.day))
    {
        #[allow(clippy::cast_precision_loss)]
        return months as f64;
    }
    #[allow(clippy::cast_precision_loss)]
    let micros_span = (first.day_micros - second.day_micros) as f64 / 86_400_000_000.0;
    #[allow(clippy::cast_precision_loss)]
    let (whole, day_span) = if first.day > second.day {
        (
            months as f64,
            f64::from(first.day - second.day) + micros_span,
        )
    } else {
        (
            (months - 1) as f64,
            f64::from(31 + first.day - second.day) + micros_span,
        )
    };
    let value = whole + day_span / 31.0;
    if round {
        (value * 1e8).round() / 1e8
    } else {
        value
    }
}

#[derive(Debug)]
struct ConvertTimezone {
    signature: Signature,
}

impl ConvertTimezone {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for ConvertTimezone {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ConvertTimezone {}

impl Hash for ConvertTimezone {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for ConvertTimezone {
    crate::shim_udf_boilerplate!("convert_timezone");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if matches!(arg_types.len(), 2 | 3) {
            Ok(arg_types.to_vec())
        } else {
            Err(plan_error(format!(
                "'convert_timezone' expects 2 or 3 arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let session_fixed = super::ZoneSource::Fixed(Some(super::ZoneOffset::Named(
            resolve_session_zone(args.config_options.as_ref())?,
        )));
        let (first_source, second_source) = if args.args.len() == 3 {
            (
                super::hoisted_zone(zone_scalar(&args.args, 0)?)?,
                super::hoisted_zone(zone_scalar(&args.args, 1)?)?,
            )
        } else {
            (
                session_fixed,
                super::hoisted_zone(zone_scalar(&args.args, 0)?)?,
            )
        };
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = broadcast_arrays(&arrays);
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let session_zone = resolve_session_zone(args.config_options.as_ref())?;
        let zone_error =
            || super::plan_error("make_timestamp timezone expects a STRING".to_string());
        let worked: Vec<ArrayRef> = arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                if index < arrays.len().saturating_sub(1) {
                    super::precast_column(array, &DataType::Utf8, zone_error())
                } else {
                    Ok(ArrayRef::clone(array))
                }
            })
            .collect::<Result<_>>()?;
        let width = worked.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<i64>> = Vec::with_capacity(width);
        for row in 0..width {
            let zones = if worked.len() == 3 {
                (
                    resolve_zone(&first_source, worked[0].as_ref(), row)?,
                    resolve_zone(&second_source, worked[1].as_ref(), row)?,
                )
            } else {
                (
                    resolve_zone(&first_source, worked[0].as_ref(), row)?,
                    resolve_zone(&second_source, worked[0].as_ref(), row)?,
                )
            };
            let wall = nullable_wall(&worked[worked.len() - 1], row, session_zone, ansi)?;
            match (zones, wall) {
                ((Some(source), Some(target)), Some(wall)) => {
                    match shift_wall(&wall, source, target, ansi)? {
                        Some(micros) => values.push(Some(micros)),
                        None => values.push(None),
                    }
                }
                _ => values.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            TimestampMicrosecondArray::from(values),
        )))
    }
}

fn shift_zone(array: &dyn Array, row: usize) -> Result<Option<super::ZoneOffset>> {
    super::zone_argument(array, row)
}

fn zone_scalar(args: &[ColumnarValue], index: usize) -> Result<&ColumnarValue> {
    args.get(index)
        .ok_or_else(|| plan_error("'convert_timezone' expects 2 or 3 arguments".to_string()))
}

fn resolve_zone(
    source: &super::ZoneSource,
    array: &dyn Array,
    row: usize,
) -> Result<Option<super::ZoneOffset>> {
    match source {
        super::ZoneSource::Rows => shift_zone(array, row),
        super::ZoneSource::Fixed(zone) => Ok(*zone),
    }
}

fn shift_wall(
    wall: &WallFields,
    source: super::ZoneOffset,
    target: super::ZoneOffset,
    ansi: bool,
) -> Result<Option<i64>> {
    let date = NaiveDate::from_ymd_opt(wall.year, wall.month, wall.day);
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
        u32::try_from(wall.day_micros.div_euclid(1_000_000)).unwrap_or(0),
        u32::try_from(wall.day_micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0),
    );
    let (Some(date), Some(time)) = (date, time) else {
        if ansi {
            return Err(super::invalid_date_text(wall.month, wall.day));
        }
        return Ok(None);
    };
    let instant = super::wall_to_instant_micros(date.and_time(time), source);
    match instant.and_then(|at| super::instant_to_wall_micros(at, target)) {
        Some(wall) => Ok(Some(wall.and_utc().timestamp_micros())),
        None => {
            if ansi {
                Err(datetime_overflow())
            } else {
                Ok(None)
            }
        }
    }
}

#[derive(Debug)]
struct LocalTimestamp {
    signature: Signature,
}

impl LocalTimestamp {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl PartialEq for LocalTimestamp {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LocalTimestamp {}

impl Hash for LocalTimestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for LocalTimestamp {
    crate::shim_udf_boilerplate!("localtimestamp");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs<'_>,
    ) -> Result<arrow::datatypes::FieldRef> {
        let _ = args;
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(Vec::new())
        } else {
            Err(plan_error(format!(
                "'localtimestamp' expects no arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone = resolve_session_zone(args.config_options.as_ref())?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let micros = i64::try_from(now.as_micros()).map_err(|_| {
            DataFusionError::Execution("localtimestamp: system time overflows i64".to_string())
        })?;
        let wall = local_datetime_from_micros(micros, zone).ok_or_else(|| {
            DataFusionError::Execution("localtimestamp: system time out of range".to_string())
        })?;
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::TimestampMicrosecond(
                Some(wall.and_utc().timestamp_micros()),
                None,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with(ansi: bool, zone: &str) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
        let config = crate::session_time_zone::with_session_time_zone(config, zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                out.push(
                    datafusion::arrow::util::display::array_value_to_string(column, row).unwrap(),
                );
            }
        }
        out
    }

    #[tokio::test]
    async fn months_between_last_days_is_whole() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT months_between(DATE'2024-03-31', DATE'2024-02-29') AS v"
            )
            .await,
            vec!["1.0".to_string()]
        );
    }

    #[tokio::test]
    async fn months_between_garbage_names_cast_input() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT months_between('garbage', '1996-10-30') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("[CAST_INVALID_INPUT]"), "{error}");
    }

    #[tokio::test]
    async fn convert_timezone_shifts_wall() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT convert_timezone('Europe/Brussels', 'America/Los_Angeles',                  TIMESTAMP'2024-03-10 01:30:00') AS v"
            )
            .await,
            vec!["2024-03-09T16:30:00".to_string()]
        );
    }

    #[tokio::test]
    async fn localtimestamp_is_not_null() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(&ctx, "SELECT localtimestamp() IS NOT NULL AS v").await,
            vec!["true".to_string()]
        );
    }
}
