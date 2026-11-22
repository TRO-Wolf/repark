use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};

use super::ticks_per_second;

#[must_use]
pub fn unix_seconds_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkUnixFloor::new(Target::Seconds)))
}

#[must_use]
pub fn unix_millis_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkUnixFloor::new(Target::Millis)))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![unix_seconds_udf(), unix_millis_udf()]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Seconds,
    Millis,
}

impl Target {
    const fn name(self) -> &'static str {
        match self {
            Self::Seconds => "unix_seconds",
            Self::Millis => "unix_millis",
        }
    }

    const fn per_second(self) -> i64 {
        match self {
            Self::Seconds => 1,
            Self::Millis => 1_000,
        }
    }
}

#[derive(Debug)]
struct SparkUnixFloor {
    signature: Signature,
    target: Target,
}

impl SparkUnixFloor {
    fn new(target: Target) -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Timestamp)],
                Volatility::Immutable,
            ),
            target,
        }
    }

    fn floor(&self, array: &dyn Array) -> Result<Int64Array> {
        let from = match array.data_type() {
            DataType::Timestamp(unit, _) => ticks_per_second(*unit),
            DataType::Null => return Ok(Int64Array::new_null(array.len())),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "'{}' expects a TIMESTAMP argument, got {other}",
                    self.name()
                )));
            }
        };
        let ticks = cast(array, &DataType::Int64)?;
        ticks
            .as_primitive::<Int64Type>()
            .iter()
            .map(|value| {
                value
                    .map(|ticks| rescale_floor(self.name(), ticks, from, self.target.per_second()))
                    .transpose()
            })
            .collect()
    }
}

fn ntz_refusal(name: &str, argument: &str) -> DataFusionError {
    let mut characters = argument.chars();
    let is_column_name = characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|rest| rest.is_ascii_alphanumeric() || rest == '_');
    if is_column_name {
        DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}({argument})\" due \
             to data type mismatch: The first parameter requires the \"TIMESTAMP\" type, however \
             \"{argument}\" has the type \"TIMESTAMP_NTZ\". SQLSTATE: 42K09"
        ))
    } else {
        DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}\" due to data type \
             mismatch: The first parameter requires the \"TIMESTAMP\" type, however the argument \
             has the type \"TIMESTAMP_NTZ\". SQLSTATE: 42K09"
        ))
    }
}

fn rescale_floor(name: &str, ticks: i64, from: i64, to: i64) -> Result<i64> {
    if from >= to {
        Ok(ticks.div_euclid(from / to))
    } else {
        ticks.checked_mul(to / from).ok_or_else(|| {
            DataFusionError::Execution(format!("'{name}' of {ticks} overflows BIGINT"))
        })
    }
}

impl PartialEq for SparkUnixFloor {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl Eq for SparkUnixFloor {}

impl Hash for SparkUnixFloor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkUnixFloor {
    fn name(&self) -> &str {
        self.target.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if let Some(field) = args.arg_fields.first()
            && matches!(field.data_type(), DataType::Timestamp(_, None))
        {
            return Err(ntz_refusal(self.name(), field.name()));
        }
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            [ColumnarValue::Array(array)] => {
                Ok(ColumnarValue::Array(Arc::new(self.floor(array.as_ref())?)))
            }
            [ColumnarValue::Scalar(scalar)] => {
                let floored = self.floor(scalar.to_array()?.as_ref())?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &floored, 0,
                )?))
            }
            other => Err(DataFusionError::Plan(format!(
                "'{}' expects 1 argument, got {}",
                self.name(),
                other.len()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        TimestampMicrosecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };

    fn floored(target: Target, array: &dyn Array) -> Vec<Option<i64>> {
        SparkUnixFloor::new(target)
            .floor(array)
            .expect("a timestamp input")
            .iter()
            .collect()
    }

    #[test]
    fn unix_seconds_and_millis_floor_a_pre_epoch_fraction() {
        let micros = TimestampMicrosecondArray::from(vec![
            Some(-500_000),
            Some(-1),
            Some(-1_500_000),
            Some(-302_486_399_750_000),
            Some(-1_000_000),
            Some(1_736_942_459_500_000),
            Some(253_402_300_799_999_999),
            None,
        ])
        .with_timezone("UTC");
        assert_eq!(
            floored(Target::Seconds, &micros),
            vec![
                Some(-1),
                Some(-1),
                Some(-2),
                Some(-302_486_400),
                Some(-1),
                Some(1_736_942_459),
                Some(253_402_300_799),
                None
            ]
        );
        assert_eq!(
            floored(Target::Millis, &micros),
            vec![
                Some(-500),
                Some(-1),
                Some(-1_500),
                Some(-302_486_399_750),
                Some(-1_000),
                Some(1_736_942_459_500),
                Some(253_402_300_799_999),
                None
            ]
        );
    }

    #[test]
    fn every_input_unit_rescales_to_the_target() {
        let nanos = TimestampNanosecondArray::from(vec![Some(-1), Some(1_999_999)]);
        assert_eq!(floored(Target::Seconds, &nanos), vec![Some(-1), Some(0)]);
        assert_eq!(floored(Target::Millis, &nanos), vec![Some(-1), Some(1)]);
        let seconds = TimestampSecondArray::from(vec![Some(-2), Some(i64::MAX)]);
        assert_eq!(
            floored(Target::Seconds, &seconds),
            vec![Some(-2), Some(i64::MAX)]
        );
        let error = SparkUnixFloor::new(Target::Millis)
            .floor(&seconds)
            .expect_err("i64::MAX seconds overflow the millisecond count");
        assert!(
            error.to_string().contains("overflows BIGINT"),
            "got {error}"
        );
    }

    #[test]
    fn a_timestamp_ntz_argument_is_sparks_input_type_refusal() {
        let column = ntz_refusal("unix_seconds", "ntz").to_string();
        assert!(
            column.contains(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"unix_seconds(ntz)\" \
                 due to data type mismatch: The first parameter requires the \"TIMESTAMP\" type, \
                 however \"ntz\" has the type \"TIMESTAMP_NTZ\". SQLSTATE: 42K09"
            ),
            "got {column}"
        );
        let literal = ntz_refusal("unix_millis", "TimestampMicrosecond(-500000, None)").to_string();
        assert!(
            literal.contains("Cannot resolve \"unix_millis\" due to data type mismatch"),
            "got {literal}"
        );
        assert!(
            literal.contains("the argument has the type \"TIMESTAMP_NTZ\""),
            "got {literal}"
        );
    }

    #[test]
    fn a_null_or_non_timestamp_argument_answers_in_kind() {
        let udf = unix_seconds_udf();
        assert_eq!(udf.name(), "unix_seconds");
        assert_eq!(unix_millis_udf().name(), "unix_millis");
        let nulls = SparkUnixFloor::new(Target::Seconds)
            .floor(&datafusion::arrow::array::NullArray::new(2))
            .expect("an untyped NULL");
        assert_eq!(nulls.null_count(), 2);
        let error = SparkUnixFloor::new(Target::Seconds)
            .floor(&Int64Array::from(vec![1]))
            .expect_err("BIGINT is not a timestamp");
        assert!(
            error.to_string().contains("expects a TIMESTAMP argument"),
            "got {error}"
        );
    }
}
