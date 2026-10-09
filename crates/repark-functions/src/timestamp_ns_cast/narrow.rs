use std::sync::{Arc, LazyLock};

use arrow::array::timezone::Tz;
use datafusion::arrow::array::{ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType, TimestampNanosecondType,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::{NANOS_PER_MICRO, NARROW_TIMESTAMP_NS_NAME};
use crate::datetime::localize_wall_micros_in_zone;
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

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

pub(super) fn narrow(array: &ArrayRef, zone: Tz) -> Result<ArrayRef> {
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
