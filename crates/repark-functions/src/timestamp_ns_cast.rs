use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use arrow::array::timezone::Tz;
use arrow::compute::kernels::cast_utils::string_to_datetime;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, TimestampMicrosecondArray, TimestampNanosecondBuilder, new_null_array,
};
use datafusion::arrow::compute::{CastOptions, cast, cast_with_options};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, FieldRef, Int64Type, TimeUnit, TimestampMicrosecondType,
    TimestampNanosecondType,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Values, Volatility,
};

use crate::ansi::spark_ansi_enabled_from_options;
use crate::datetime::localize_wall_micros_in_zone;
use crate::instant_ts::string_carries_timezone;
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

pub const TIMESTAMP_NS_CAST_NAME: &str = "__repark_cast_timestamp_ns__";
pub const TIMESTAMPTZ_NS_CAST_NAME: &str = "__repark_cast_timestamptz_ns__";
pub const NARROW_TIMESTAMP_NS_NAME: &str = "__repark_narrow_timestamp_ns__";

const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

static TIMESTAMP_NS_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampNsCast::new(false))));
static TIMESTAMPTZ_NS_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampNsCast::new(true))));

#[must_use]
pub fn timestamp_ns_cast_udf(zoned: bool) -> Arc<ScalarUDF> {
    if zoned {
        Arc::clone(&TIMESTAMPTZ_NS_CAST)
    } else {
        Arc::clone(&TIMESTAMP_NS_CAST)
    }
}

#[must_use]
pub fn timestamp_ns_cast_expr(expr: Expr, zoned: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        timestamp_ns_cast_udf(zoned),
        vec![expr],
    ))
}

#[must_use]
pub fn timestamp_ns_target(data_type: &DataType) -> Option<bool> {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => Some(zone.is_some()),
        _ => None,
    }
}

#[must_use]
pub fn is_timestamp_ns_cast(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), TIMESTAMP_NS_CAST_NAME | TIMESTAMPTZ_NS_CAST_NAME)
    )
}

#[must_use]
pub fn is_temporal_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

fn is_string_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn target_type(zoned: bool) -> DataType {
    if zoned {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC")))
    } else {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }
}

const fn target_name(zoned: bool) -> &'static str {
    if zoned {
        "TIMESTAMPTZ_NS"
    } else {
        "TIMESTAMP_NS"
    }
}

#[derive(Debug)]
struct SparkTimestampNsCast {
    zoned: bool,
    signature: Signature,
}

impl SparkTimestampNsCast {
    fn new(zoned: bool) -> Self {
        Self {
            zoned,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }

    fn checked_source(&self, data_type: &DataType) -> Result<()> {
        if matches!(data_type, DataType::Null)
            || is_string_source(data_type)
            || is_temporal_source(data_type)
        {
            return Ok(());
        }
        Err(DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"CAST(value AS {target})\" \
             due to data type mismatch: cannot cast \"{data_type}\" to \"{target}\". SQLSTATE: 42K09",
            target = target_name(self.zoned)
        )))
    }
}

impl PartialEq for SparkTimestampNsCast {
    fn eq(&self, other: &Self) -> bool {
        self.zoned == other.zoned
    }
}

impl Eq for SparkTimestampNsCast {}

impl Hash for SparkTimestampNsCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTimestampNsCast {
    fn name(&self) -> &str {
        if self.zoned {
            TIMESTAMPTZ_NS_CAST_NAME
        } else {
            TIMESTAMP_NS_CAST_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(source) = arg_types.first() {
            self.checked_source(source)?;
        }
        Ok(target_type(self.zoned))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = match args.arg_fields.first() {
            Some(field) => {
                self.checked_source(field.data_type())?;
                field.is_nullable() || is_string_source(field.data_type())
            }
            None => true,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            target_type(self.zoned),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone =
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?;
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let conversion = Conversion {
            zoned: self.zoned,
            zone,
            ansi,
        };
        match args.args.first() {
            Some(ColumnarValue::Array(array)) => {
                Ok(ColumnarValue::Array(conversion.convert(array)?))
            }
            Some(ColumnarValue::Scalar(scalar)) => {
                let converted = conversion.convert(&scalar.to_array()?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &converted, 0,
                )?))
            }
            None => Err(DataFusionError::Plan(format!(
                "'{}' expects one argument",
                self.name()
            ))),
        }
    }
}

struct Conversion {
    zoned: bool,
    zone: Tz,
    ansi: bool,
}

impl Conversion {
    fn convert(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let target = target_type(self.zoned);
        match array.data_type() {
            DataType::Null => Ok(new_null_array(&target, array.len())),
            source if is_string_source(source) => self.convert_strings(array),
            DataType::Timestamp(_, source_zone) if source_zone.is_some() == self.zoned => {
                self.widen_same_kind(array, &target)
            }
            DataType::Timestamp(unit, source_zone) => {
                let per_tick = nanos_per_tick(*unit);
                let ticks = cast(array.as_ref(), &DataType::Int64)?;
                let ticks = ticks.as_primitive::<Int64Type>();
                let source_name = if source_zone.is_some() {
                    "TIMESTAMP"
                } else {
                    "TIMESTAMP_NTZ"
                };
                self.build(ticks.len(), |row| {
                    if ticks.is_null(row) {
                        return Ok(None);
                    }
                    let value = ticks.value(row);
                    let nanos = value
                        .checked_mul(per_tick)
                        .and_then(|nanos| self.place(nanos, source_zone.is_some()));
                    self.or_overflow(nanos, value, source_name)
                })
            }
            DataType::Date32 | DataType::Date64 => {
                let days = cast(array.as_ref(), &DataType::Date32)?;
                let days = days.as_primitive::<Date32Type>();
                self.build(days.len(), |row| {
                    if days.is_null(row) {
                        return Ok(None);
                    }
                    let value = days.value(row);
                    let nanos = i64::from(value)
                        .checked_mul(NANOS_PER_DAY)
                        .and_then(|wall| self.place(wall, false));
                    self.or_overflow(nanos, value, "DATE")
                })
            }
            other => Err(DataFusionError::Plan(format!(
                "cannot cast \"{other}\" to \"{}\"",
                target_name(self.zoned)
            ))),
        }
    }

    fn widen_same_kind(&self, array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
        let options = CastOptions {
            safe: true,
            ..CastOptions::default()
        };
        let widened = cast_with_options(array.as_ref(), target, &options)?;
        if !self.ansi || widened.null_count() == array.null_count() {
            return Ok(widened);
        }
        let DataType::Timestamp(unit, source_zone) = array.data_type() else {
            return Ok(widened);
        };
        let ticks = cast(array.as_ref(), &DataType::Int64)?;
        let ticks = ticks.as_primitive::<Int64Type>();
        let source_name = if source_zone.is_some() {
            "TIMESTAMP"
        } else {
            "TIMESTAMP_NTZ"
        };
        let per_tick = nanos_per_tick(*unit);
        match (0..ticks.len())
            .find(|row| ticks.is_valid(*row) && ticks.value(*row).checked_mul(per_tick).is_none())
        {
            Some(row) => self
                .or_overflow(None, ticks.value(row), source_name)
                .map(|_| widened),
            None => Ok(widened),
        }
    }

    fn convert_strings(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let utf8 = cast(array.as_ref(), &DataType::Utf8)?;
        let texts = utf8.as_string::<i32>();
        self.build(texts.len(), |row| {
            if texts.is_null(row) {
                return Ok(None);
            }
            let text = texts.value(row);
            match self.parse(text) {
                Some(nanos) => Ok(Some(nanos)),
                None if self.ansi => Err(DataFusionError::Execution(malformed(
                    text,
                    target_name(self.zoned),
                ))),
                None => Ok(None),
            }
        })
    }

    fn build(
        &self,
        len: usize,
        mut value_at: impl FnMut(usize) -> Result<Option<i64>>,
    ) -> Result<ArrayRef> {
        let mut builder = TimestampNanosecondBuilder::with_capacity(len);
        for row in 0..len {
            builder.append_option(value_at(row)?);
        }
        let built = builder.finish();
        Ok(if self.zoned {
            Arc::new(built.with_timezone("UTC"))
        } else {
            Arc::new(built)
        })
    }

    fn parse(&self, text: &str) -> Option<i64> {
        let trimmed = text.trim();
        let parsed = string_to_datetime(&Utc, trimmed).ok()?;
        let nanos = parsed.timestamp_nanos_opt()?;
        self.place(nanos, string_carries_timezone(trimmed))
    }

    fn place(&self, nanos: i64, is_instant: bool) -> Option<i64> {
        match (is_instant, self.zoned) {
            (true, true) | (false, false) => Some(nanos),
            (false, true) => localize_wall_nanos(nanos, self.zone),
            (true, false) => session_wall_nanos(nanos, self.zone),
        }
    }

    fn or_overflow(
        &self,
        nanos: Option<i64>,
        value: impl Display,
        source: &str,
    ) -> Result<Option<i64>> {
        match nanos {
            Some(nanos) => Ok(Some(nanos)),
            None if self.ansi => Err(DataFusionError::Execution(format!(
                "[CAST_OVERFLOW] The value {value} of the type \"{source}\" cannot be cast to \
                 \"{target}\" due to an overflow. Use `try_cast` to tolerate overflow and return \
                 NULL instead. SQLSTATE: 22003",
                target = target_name(self.zoned)
            ))),
            None => Ok(None),
        }
    }
}

const fn nanos_per_tick(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => NANOS_PER_MICRO,
        TimeUnit::Nanosecond => 1,
    }
}

fn localize_wall_nanos(wall: i64, zone: Tz) -> Option<i64> {
    let micros = localize_wall_micros_in_zone(wall.div_euclid(NANOS_PER_MICRO), zone)?;
    micros
        .checked_mul(NANOS_PER_MICRO)?
        .checked_add(wall.rem_euclid(NANOS_PER_MICRO))
}

fn session_wall_nanos(instant: i64, zone: Tz) -> Option<i64> {
    DateTime::from_timestamp_nanos(instant)
        .with_timezone(&zone)
        .naive_local()
        .and_utc()
        .timestamp_nanos_opt()
}

fn malformed(value: &str, target: &str) -> String {
    format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"{target}\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    )
}

static NARROW_TIMESTAMP_NS: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(NarrowTimestampNs::new())));

#[must_use]
pub fn narrow_timestamp_ns_expr(expr: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&NARROW_TIMESTAMP_NS),
        vec![expr],
    ))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NarrowTimestampNs {
    signature: Signature,
}

impl NarrowTimestampNs {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for NarrowTimestampNs {
    fn name(&self) -> &str {
        NARROW_TIMESTAMP_NS_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(narrowed_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), narrowed_type(), nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone =
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?;
        match args.args.first() {
            Some(ColumnarValue::Array(array)) => Ok(ColumnarValue::Array(narrow(array, zone)?)),
            Some(ColumnarValue::Scalar(scalar)) => {
                let narrowed = narrow(&scalar.to_array()?, zone)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &narrowed, 0,
                )?))
            }
            None => Err(DataFusionError::Plan(format!(
                "'{NARROW_TIMESTAMP_NS_NAME}' expects one argument"
            ))),
        }
    }
}

fn narrowed_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
}

fn narrow(array: &ArrayRef, zone: Tz) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, source_zone) = array.data_type() else {
        return Err(DataFusionError::Plan(format!(
            "'{NARROW_TIMESTAMP_NS_NAME}' expects a timestamp, found \"{}\"",
            array.data_type()
        )));
    };
    let micros: TimestampMicrosecondArray = if *unit == TimeUnit::Nanosecond {
        array
            .as_primitive::<TimestampNanosecondType>()
            .unary(|ticks| ticks.div_euclid(NANOS_PER_MICRO))
    } else {
        let target = DataType::Timestamp(TimeUnit::Microsecond, source_zone.clone());
        cast(array.as_ref(), &target)?
            .as_primitive::<TimestampMicrosecondType>()
            .clone()
    };
    let micros = if source_zone.is_some() {
        micros
    } else {
        micros.try_unary(|wall| {
            localize_wall_micros_in_zone(wall, zone).ok_or_else(|| {
                ArrowError::ComputeError(
                    "cannot localize zoneless timestamp into session timezone: out of range"
                        .to_string(),
                )
            })
        })?
    };
    Ok(Arc::new(micros.with_timezone("UTC")))
}

pub(crate) fn conform_values_timestamp_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Values(values) = plan else {
        return Ok(plan);
    };
    let actions = values_actions(&values);
    if actions.is_empty() {
        return Ok(LogicalPlan::Values(values));
    }
    let mut rows = values.values;
    let mut fields: Vec<(Option<datafusion::common::TableReference>, FieldRef)> = values
        .schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();
    for (column, action) in actions {
        match action {
            ValuesAction::Widen(zoned, types) => {
                let declared = fields[column].1.data_type().clone();
                for (row, found) in rows.iter_mut().zip(&types) {
                    if *found != declared && is_temporal_source(found) {
                        let cell = std::mem::take(&mut row[column]);
                        row[column] = timestamp_ns_cast_expr(cell, zoned);
                    }
                }
            }
            ValuesAction::Retype(found) => {
                let field = &mut fields[column].1;
                *field = Arc::new(field.as_ref().clone().with_data_type(found));
            }
        }
    }
    let schema = DFSchema::new_with_metadata(fields, values.schema.metadata().clone())?;
    Ok(LogicalPlan::Values(Values {
        schema: Arc::new(schema),
        values: rows,
    }))
}

enum ValuesAction {
    Widen(bool, Vec<DataType>),
    Retype(DataType),
}

fn values_actions(values: &Values) -> Vec<(usize, ValuesAction)> {
    let empty = DFSchema::empty();
    let mut actions = Vec::new();
    for (column, field) in values.schema.fields().iter().enumerate() {
        let declared = field.data_type();
        let Some(zoned) = timestamp_ns_target(declared) else {
            continue;
        };
        let Ok(types) = values
            .values
            .iter()
            .map(|row| row[column].get_type(&empty))
            .collect::<Result<Vec<DataType>>>()
        else {
            continue;
        };
        if types.iter().all(|found| found == declared) {
            continue;
        }
        if types.contains(declared) {
            actions.push((column, ValuesAction::Widen(zoned, types)));
        } else if let Some(first) = types.first()
            && types.iter().all(|found| found == first)
            && matches!(first, DataType::Timestamp(_, _))
        {
            actions.push((column, ValuesAction::Retype(first.clone())));
        }
    }
    actions
}

#[cfg(test)]
mod tests;
