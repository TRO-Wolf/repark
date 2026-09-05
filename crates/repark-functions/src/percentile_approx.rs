use std::hash::{Hash, Hasher};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

use crate::quantile_summaries::{DEFAULT_ACCURACY, QuantileSummaries};

#[must_use]
pub fn percentile_approx_udaf() -> Arc<AggregateUDF> {
    Arc::new(
        AggregateUDF::new_from_impl(SparkPercentileApprox::new())
            .with_aliases(["approx_percentile"]),
    )
}

#[derive(Debug)]
struct SparkPercentileApprox {
    signature: Signature,
}

impl SparkPercentileApprox {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for SparkPercentileApprox {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkPercentileApprox {}

impl Hash for SparkPercentileApprox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn percentages_are_list(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_)
    )
}

fn return_data_type(value_type: &DataType, percentage_type: &DataType) -> DataType {
    if percentages_are_list(percentage_type) {
        DataType::List(Arc::new(Field::new("item", value_type.clone(), true)))
    } else {
        value_type.clone()
    }
}

fn percentages_from_scalar(value: &ScalarValue) -> Result<Vec<f64>> {
    match value {
        ScalarValue::List(list) => {
            if list.is_empty() {
                return Ok(Vec::new());
            }
            if list.is_null(0) {
                return exec_err!("percentile_approx percentage must not be null");
            }
            let inner = list.value(0);
            let mut percentages = Vec::with_capacity(inner.len());
            for index in 0..inner.len() {
                if inner.is_null(index) {
                    percentages.push(0.0);
                    continue;
                }
                percentages.push(scalar_as_f64(&ScalarValue::try_from_array(&inner, index)?)?);
            }
            Ok(percentages)
        }
        ScalarValue::Null => exec_err!("percentile_approx percentage must not be null"),
        other => {
            if other.is_null() {
                return exec_err!("percentile_approx percentage must not be null");
            }
            Ok(vec![scalar_as_f64(other)?])
        }
    }
}

fn validated_percentages(value: &ScalarValue) -> Result<Vec<f64>> {
    let percentages = percentages_from_scalar(value)?;
    for percentage in &percentages {
        if !(0.0..=1.0).contains(percentage) {
            return exec_err!(
                "percentile_approx percentage {percentage} is out of range [0.0, 1.0]"
            );
        }
    }
    Ok(percentages)
}

fn extract_percentages(array: &ArrayRef) -> Result<Vec<f64>> {
    if array.is_empty() {
        return Ok(Vec::new());
    }
    validated_percentages(&ScalarValue::try_from_array(array, 0)?)
}

#[allow(clippy::cast_precision_loss)]
fn scalar_as_f64(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Float64(Some(number)) => Ok(*number),
        ScalarValue::Float32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Decimal128(Some(number), _, scale) => {
            let scale = i32::from(*scale);
            Ok((*number as f64) / 10f64.powi(scale))
        }
        ScalarValue::Int32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int16(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int8(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt16(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt8(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int64(Some(number)) => {
            let Ok(narrow) = i32::try_from(*number) else {
                return exec_err!("percentile_approx Int64 percentage does not fit i32");
            };
            Ok(f64::from(narrow))
        }
        ScalarValue::UInt64(Some(number)) => {
            let Ok(narrow) = u32::try_from(*number) else {
                return exec_err!("percentile_approx UInt64 percentage does not fit u32");
            };
            Ok(f64::from(narrow))
        }
        other => exec_err!("percentile_approx percentage must be numeric, got {other}"),
    }
}

fn accuracy_from_scalar(value: &ScalarValue) -> Result<i64> {
    if value.is_null() {
        return exec_err!("percentile_approx accuracy must not be null");
    }
    let parsed = match value {
        ScalarValue::Int64(Some(number)) => *number,
        ScalarValue::Int32(Some(number)) => i64::from(*number),
        ScalarValue::Int16(Some(number)) => i64::from(*number),
        ScalarValue::Int8(Some(number)) => i64::from(*number),
        ScalarValue::UInt32(Some(number)) => i64::from(*number),
        ScalarValue::UInt16(Some(number)) => i64::from(*number),
        ScalarValue::UInt8(Some(number)) => i64::from(*number),
        ScalarValue::UInt64(Some(number)) => {
            let Ok(narrow) = i64::try_from(*number) else {
                return exec_err!("percentile_approx UInt64 accuracy does not fit i64");
            };
            narrow
        }
        other => {
            return exec_err!("percentile_approx accuracy must be an integer, got {other}");
        }
    };
    if !(1..=i64::from(i32::MAX)).contains(&parsed) {
        return exec_err!("percentile_approx accuracy {parsed} is out of range (0, 2147483647]");
    }
    Ok(parsed)
}

fn extract_accuracy(array: &ArrayRef) -> Result<Option<i64>> {
    if array.is_empty() {
        return Ok(None);
    }
    Ok(Some(accuracy_from_scalar(&ScalarValue::try_from_array(
        array, 0,
    )?)?))
}

fn literal_scalar(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let erased: &dyn std::any::Any = expr.as_ref();
    erased
        .downcast_ref::<Literal>()
        .map(|literal| literal.value().clone())
}

fn value_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
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
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

#[allow(clippy::cast_precision_loss)]
fn relative_error(accuracy: i64) -> f64 {
    1.0 / (accuracy as f64)
}

#[allow(clippy::cast_precision_loss)]
fn value_as_f64(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Float64(Some(number)) => Ok(*number),
        ScalarValue::Float32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int8(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int16(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int64(Some(number)) => Ok(*number as f64),
        ScalarValue::UInt8(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt16(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt64(Some(number)) => Ok(*number as f64),
        ScalarValue::Decimal128(Some(number), _, scale) => {
            let divisor = 10_f64.powi(i32::from(*scale));
            Ok((*number as f64) / divisor)
        }
        ScalarValue::Date32(Some(days)) => Ok(f64::from(*days)),
        ScalarValue::Date64(Some(millis)) => Ok(*millis as f64),
        ScalarValue::TimestampSecond(Some(stamp), _) => Ok((i128::from(*stamp) * 1_000_000) as f64),
        ScalarValue::TimestampMillisecond(Some(stamp), _) => {
            Ok((i128::from(*stamp) * 1_000) as f64)
        }
        ScalarValue::TimestampMicrosecond(Some(stamp), _) => Ok(*stamp as f64),
        ScalarValue::TimestampNanosecond(Some(stamp), _) => Ok((stamp / 1_000) as f64),
        other => exec_err!("percentile_approx does not support {other} values"),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn decimal_unscaled(value: f64, precision: u8, scale: i8) -> Result<i128> {
    if !value.is_finite() {
        return exec_err!("percentile_approx decimal answer is not finite");
    }
    let text = format!("{value:?}");
    let cut = text.find('e').or_else(|| text.find('E'));
    let (mantissa, exponent) = match cut {
        Some(index) => {
            let parsed: i32 = text[index + 1..].parse().map_err(|_| {
                datafusion::common::DataFusionError::Execution(
                    "percentile_approx decimal answer has a bad exponent".to_string(),
                )
            })?;
            (text[..index].to_string(), parsed)
        }
        None => (text.clone(), 0),
    };
    let digits_text = mantissa.trim_start_matches('-');
    let (whole, fraction) = match digits_text.split_once('.') {
        Some((head, tail)) => (head, tail),
        None => (digits_text, ""),
    };
    let mut digits = String::with_capacity(whole.len() + fraction.len());
    digits.push_str(whole);
    digits.push_str(fraction);
    let digits = digits.trim_start_matches('0');
    let magnitude: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().map_err(|_| {
            datafusion::common::DataFusionError::Execution(
                "percentile_approx decimal answer does not fit".to_string(),
            )
        })?
    };
    let shift = i64::from(exponent) - i64::from(fraction.len() as u8) + i64::from(scale);
    let rounded = if shift >= 0 {
        let factor = 10_i128.checked_pow(shift as u32).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "percentile_approx decimal answer does not fit".to_string(),
            )
        })?;
        magnitude.checked_mul(factor).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "percentile_approx decimal answer does not fit".to_string(),
            )
        })?
    } else if shift < -38 {
        0
    } else {
        let divisor = 10_i128.pow((-shift) as u32);
        let quotient = magnitude / divisor;
        let remainder = magnitude % divisor;
        if remainder >= divisor - remainder {
            quotient + 1
        } else {
            quotient
        }
    };
    let signed = if mantissa.starts_with('-') {
        -rounded
    } else {
        rounded
    };
    let bound = 10_i128.pow(u32::from(precision));
    if signed <= -bound || signed >= bound {
        return exec_err!("percentile_approx decimal answer does not fit the column type");
    }
    Ok(signed)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn double_to_scalar(value: f64, data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Int8 => Ok(ScalarValue::Int8(Some(value as i8))),
        DataType::Int16 => Ok(ScalarValue::Int16(Some(value as i16))),
        DataType::Int32 => Ok(ScalarValue::Int32(Some(value as i32))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(value as i64))),
        DataType::UInt8 => Ok(ScalarValue::UInt8(Some(value as u8))),
        DataType::UInt16 => Ok(ScalarValue::UInt16(Some(value as u16))),
        DataType::UInt32 => Ok(ScalarValue::UInt32(Some(value as u32))),
        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(value as u64))),
        DataType::Float32 => Ok(ScalarValue::Float32(Some(value as f32))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(value))),
        DataType::Decimal128(precision, scale) => {
            let unscaled = decimal_unscaled(value, *precision, *scale)?;
            Ok(ScalarValue::Decimal128(Some(unscaled), *precision, *scale))
        }
        DataType::Date32 => Ok(ScalarValue::Date32(Some(value as i32))),
        DataType::Date64 => Ok(ScalarValue::Date64(Some(value as i64))),
        DataType::Timestamp(unit, zone) => {
            let micros = value as i64;
            match unit {
                TimeUnit::Second => Ok(ScalarValue::TimestampSecond(
                    Some(micros / 1_000_000),
                    zone.clone(),
                )),
                TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(
                    Some(micros / 1_000),
                    zone.clone(),
                )),
                TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(
                    Some(micros),
                    zone.clone(),
                )),
                TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(
                    Some(micros.saturating_mul(1_000)),
                    zone.clone(),
                )),
            }
        }
        other => exec_err!("percentile_approx does not support {other} results"),
    }
}

#[derive(Debug)]
struct PercentileAccumulator {
    summary: QuantileSummaries,
    percentages: Option<Vec<f64>>,
    accuracy_known: bool,
    value_type: DataType,
    return_list: bool,
}

impl PercentileAccumulator {
    fn with_accuracy(
        accuracy: i64,
        percentages: Option<Vec<f64>>,
        accuracy_known: bool,
        value_type: DataType,
        return_list: bool,
    ) -> Self {
        Self {
            summary: QuantileSummaries::new(relative_error(accuracy)),
            percentages,
            accuracy_known,
            value_type,
            return_list,
        }
    }

    fn typed_null(&self) -> Result<ScalarValue> {
        if self.return_list {
            return Ok(ScalarValue::new_null_list(self.value_type.clone(), true, 1));
        }
        ScalarValue::try_from(&self.value_type)
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(column) = values.first() else {
            return Ok(());
        };
        if self.percentages.is_none()
            && let Some(percentage_column) = values.get(1)
            && !percentage_column.is_empty()
        {
            self.percentages = Some(extract_percentages(percentage_column)?);
        }
        if !self.accuracy_known
            && self.summary.count() == 0
            && self.summary.buffered_count() == 0
            && let Some(accuracy_column) = values.get(2)
            && let Some(accuracy) = extract_accuracy(accuracy_column)?
        {
            self.summary = QuantileSummaries::new(relative_error(accuracy));
            self.accuracy_known = true;
        }
        for row in 0..column.len() {
            if column.is_null(row) {
                continue;
            }
            let scalar = ScalarValue::try_from_array(column, row)?;
            self.summary.insert(value_as_f64(&scalar)?);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.summary.count() == 0 && self.summary.buffered_count() == 0 {
            return self.typed_null();
        }
        let Some(percentages) = self.percentages.as_ref() else {
            return exec_err!("percentile_approx missing percentage");
        };
        if percentages.is_empty() {
            return self.typed_null();
        }
        let doubles = self.summary.query(percentages);
        let mut picked = Vec::with_capacity(doubles.len());
        for value in doubles {
            picked.push(double_to_scalar(value, &self.value_type)?);
        }
        let Some(first) = picked.first() else {
            return self.typed_null();
        };
        if self.return_list {
            let value_type = first.data_type();
            return Ok(ScalarValue::List(ScalarValue::new_list_nullable(
                &picked,
                &value_type,
            )));
        }
        Ok(first.clone())
    }

    fn size(&self) -> usize {
        let percentages = self
            .percentages
            .as_ref()
            .map_or(0, |known| known.len() * size_of::<f64>());
        size_of_val(self) + self.summary.size_bytes() + percentages
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let blob = self.summary.to_bytes();
        let known = self.percentages.clone().unwrap_or_default();
        let percentages: Vec<ScalarValue> = known
            .iter()
            .map(|percentage| ScalarValue::Float64(Some(*percentage)))
            .collect();
        Ok(vec![
            ScalarValue::Binary(Some(blob)),
            ScalarValue::List(ScalarValue::new_list_nullable(
                &percentages,
                &DataType::Float64,
            )),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(blob_column) = states.first() else {
            return Ok(());
        };
        let Some(blobs) = blob_column.as_any().downcast_ref::<BinaryArray>() else {
            return exec_err!("percentile_approx state must be binary");
        };
        for row in 0..blobs.len() {
            if blobs.is_null(row) {
                continue;
            }
            let Some(other) = QuantileSummaries::from_bytes(blobs.value(row)) else {
                return exec_err!("percentile_approx state is not a valid sketch");
            };
            self.summary.merge(&other);
        }
        if self.percentages.is_none()
            && let Some(percentage_state) = states.get(1)
            && !percentage_state.is_empty()
        {
            self.percentages = Some(extract_percentages(percentage_state)?);
        }
        Ok(())
    }
}

impl AggregateUDFImpl for SparkPercentileApprox {
    fn name(&self) -> &'static str {
        "percentile_approx"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(value_type) = arg_types.first() else {
            return exec_err!("percentile_approx requires a value column");
        };
        let Some(percentage_type) = arg_types.get(1) else {
            return exec_err!("percentile_approx requires a percentage");
        };
        Ok(return_data_type(value_type, percentage_type))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let Some(value_expr) = acc_args.exprs.first() else {
            return exec_err!("percentile_approx requires a value column");
        };
        let Some(percentage_expr) = acc_args.exprs.get(1) else {
            return exec_err!("percentile_approx requires a percentage");
        };
        let value_type = value_expr.data_type(acc_args.schema)?;
        if !value_type_supported(&value_type) {
            return exec_err!("percentile_approx does not support {value_type} values");
        }
        let percentage_type = percentage_expr.data_type(acc_args.schema)?;
        let percentages = match literal_scalar(percentage_expr) {
            Some(scalar) => Some(validated_percentages(&scalar)?),
            None => None,
        };
        let (accuracy, accuracy_known) = match acc_args.exprs.get(2) {
            None => (DEFAULT_ACCURACY, true),
            Some(expr) => match literal_scalar(expr) {
                Some(scalar) => (accuracy_from_scalar(&scalar)?, true),
                None => (DEFAULT_ACCURACY, false),
            },
        };
        Ok(Box::new(PercentileAccumulator::with_accuracy(
            accuracy,
            percentages,
            accuracy_known,
            value_type,
            percentages_are_list(&percentage_type),
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "summary"),
                DataType::Binary,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "percentages"),
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )),
        ])
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, Int64Array};

    use super::*;

    fn accumulator(
        percentages: Option<Vec<f64>>,
        value_type: DataType,
        return_list: bool,
    ) -> PercentileAccumulator {
        PercentileAccumulator::with_accuracy(
            DEFAULT_ACCURACY,
            percentages,
            true,
            value_type,
            return_list,
        )
    }

    #[test]
    fn accuracy_validation_rejects_bad_values() {
        for bad in [
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(-3)),
            ScalarValue::Int64(Some(2_147_483_648)),
            ScalarValue::Null,
            ScalarValue::Int32(None),
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Utf8(Some("2".to_string())),
        ] {
            assert!(accuracy_from_scalar(&bad).is_err(), "{bad:?}");
        }
        for good in [2_i64, 10, 100, 10_000, 2_147_483_647] {
            assert_eq!(
                accuracy_from_scalar(&ScalarValue::Int64(Some(good)))
                    .expect("a valid accuracy parses"),
                good
            );
        }
    }

    #[test]
    fn percentage_validation_rejects_bad_values() {
        for bad in [
            ScalarValue::Float64(Some(1.5)),
            ScalarValue::Float64(Some(-0.5)),
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Null,
        ] {
            assert!(validated_percentages(&bad).is_err(), "{bad:?}");
        }
        let with_null = ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Float64(Some(0.5)), ScalarValue::Float64(None)],
            &DataType::Float64,
        ));
        assert_eq!(
            validated_percentages(&with_null).expect("a null element reads as zero"),
            vec![0.5, 0.0]
        );
        let empty = ScalarValue::List(ScalarValue::new_list_nullable(&[], &DataType::Float64));
        assert!(
            validated_percentages(&empty)
                .expect("an empty array parses")
                .is_empty()
        );
    }

    #[test]
    fn decimal_quantizer_pins() {
        for (value, precision, scale, unscaled) in [
            (3.3, 10, 2, 330),
            (1.1, 10, 2, 110),
            (100.25, 10, 2, 10_025),
            (2.5, 3, 0, 3),
            (0.07, 10, 2, 7),
            (123.0, 10, 2, 12_300),
        ] {
            assert_eq!(
                decimal_unscaled(value, precision, scale)
                    .expect("a round-tripping decimal quantizes"),
                unscaled,
                "{value}"
            );
        }
        assert_eq!(
            decimal_unscaled(-2.5, 3, 0).expect("a negative half rounds away from zero"),
            -3
        );
        assert!(decimal_unscaled(100.0, 2, 0).is_err());
        assert!(decimal_unscaled(f64::INFINITY, 10, 2).is_err());
    }

    #[test]
    fn accumulator_answers_discrete_ranks() {
        let mut acc = accumulator(Some(vec![0.5]), DataType::Int64, false);
        let values: ArrayRef = Arc::new(Int64Array::from((1..=200_i64).collect::<Vec<_>>()));
        let percentages: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 200]));
        acc.update_batch(&[values, percentages])
            .expect("two hundred integers update");
        assert_eq!(
            acc.evaluate().expect("a median evaluates"),
            ScalarValue::Int64(Some(100))
        );
    }

    #[test]
    fn accumulator_skips_nulls_and_merges_state() {
        let mut left = accumulator(Some(vec![0.5]), DataType::Int64, false);
        let values: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(1_i64), Some(2), Some(3), None]));
        let percentages: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 4]));
        left.update_batch(&[values, percentages])
            .expect("nulls skip");
        let states = left.state().expect("a state serializes");
        let arrays: Vec<ArrayRef> = states
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<_>>()
            .expect("state scalars build arrays");
        let mut right = accumulator(Some(vec![0.5]), DataType::Int64, false);
        let more: ArrayRef = Arc::new(Int64Array::from(vec![4, 5, 6]));
        let more_percentages: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 3]));
        right
            .update_batch(&[more, more_percentages])
            .expect("more rows update");
        right.merge_batch(&arrays).expect("a state merges");
        assert_eq!(
            right.evaluate().expect("a merged median evaluates"),
            ScalarValue::Int64(Some(3))
        );
    }

    #[test]
    fn empty_evaluates_to_null() {
        let mut acc = accumulator(None, DataType::Int64, false);
        assert_eq!(
            acc.evaluate().expect("an empty group evaluates"),
            ScalarValue::Int64(None)
        );
        let mut listed = accumulator(Some(Vec::new()), DataType::Int64, true);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let percentages: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 3]));
        listed
            .update_batch(&[values, percentages])
            .expect("rows update under an empty array");
        assert!(
            listed
                .evaluate()
                .expect("an empty percentage array evaluates")
                .is_null()
        );
    }

    #[test]
    fn unsupported_value_type_rejected() {
        assert!(value_type_supported(&DataType::Int64));
        assert!(value_type_supported(&DataType::Float64));
        assert!(value_type_supported(&DataType::Decimal128(10, 2)));
        assert!(value_type_supported(&DataType::Date32));
        assert!(value_type_supported(&DataType::Timestamp(
            TimeUnit::Microsecond,
            None
        )));
        assert!(!value_type_supported(&DataType::Utf8));
        assert!(!value_type_supported(&DataType::Boolean));
        assert!(value_as_f64(&ScalarValue::Utf8(Some("1".to_string()))).is_err());
        assert!(double_to_scalar(1.0, &DataType::Utf8).is_err());
    }
}
