use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float32Type, Float64Type, Int64Type,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};

use crate::json::reader::{java_double_text, java_float_text};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};

mod groups;

pub(crate) const BITMAP_BYTES: usize = 4096;
pub(crate) const BITMAP_BITS: i64 = 32768;

#[must_use]
pub fn functions() -> Vec<Arc<AggregateUDF>> {
    vec![
        bitmap_construct_agg_udaf(),
        bitmap_or_agg_udaf(),
        bitmap_and_agg_udaf(),
    ]
}

#[must_use]
pub fn bitmap_construct_agg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(BitmapAgg::construct()))
}

#[must_use]
pub fn bitmap_or_agg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(BitmapAgg::or_agg()))
}

#[must_use]
pub fn bitmap_and_agg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(BitmapAgg::and_agg()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BitmapFold {
    Construct,
    Or,
    And,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BitmapAgg {
    signature: Signature,
    fold: BitmapFold,
}

impl BitmapAgg {
    fn construct() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            fold: BitmapFold::Construct,
        }
    }

    fn or_agg() -> Self {
        Self {
            signature: bitmap_payload_signature(),
            fold: BitmapFold::Or,
        }
    }

    fn and_agg() -> Self {
        Self {
            signature: bitmap_payload_signature(),
            fold: BitmapFold::And,
        }
    }

    fn identity_bitmap(fold: BitmapFold) -> [u8; BITMAP_BYTES] {
        [identity_byte(fold); BITMAP_BYTES]
    }

    fn identity_scalar(fold: BitmapFold) -> ScalarValue {
        ScalarValue::Binary(Some(vec![identity_byte(fold); BITMAP_BYTES]))
    }
}

impl AggregateUDFImpl for BitmapAgg {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        match self.fold {
            BitmapFold::Construct => "bitmap_construct_agg",
            BitmapFold::Or => "bitmap_or_agg",
            BitmapFold::And => "bitmap_and_agg",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        refuse_payload(self.name(), self.fold, None, arg_types)?;
        Ok(DataType::Binary)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types: Vec<DataType> = arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let argument = arg_fields.first().map(|field| field.name().clone());
        refuse_payload(self.name(), self.fold, argument.as_deref(), &arg_types)?;
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::Plan(format!(
                "{}(DISTINCT ...) is not supported",
                self.name()
            )));
        }
        Ok(Box::new(BitmapAccumulator::new(self.fold)))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.is_distinct {
            return Err(DataFusionError::Plan(format!(
                "{}(DISTINCT ...) is not supported",
                self.name()
            )));
        }
        Ok(Box::new(groups::BitmapGroupsAccumulator::new(self.fold)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(self.name(), "bitmap"),
            DataType::Binary,
            false,
        ))])
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(Self::identity_scalar(self.fold))
    }
}

#[derive(Debug)]
struct BitmapAccumulator {
    fold: BitmapFold,
    bits: [u8; BITMAP_BYTES],
}

impl BitmapAccumulator {
    fn new(fold: BitmapFold) -> Self {
        Self {
            fold,
            bits: BitmapAgg::identity_bitmap(fold),
        }
    }

    fn update_positions(&mut self, values: &ArrayRef) -> Result<()> {
        update_positions_into(&mut self.bits, values)
    }

    fn update_bitmaps(&mut self, values: &ArrayRef) -> Result<()> {
        let values = coerce_bitmap_column(values)?;
        for_each_binary(&values, |incoming| {
            fold_incoming(&mut self.bits, incoming, self.fold);
            Ok(())
        })
    }
}

fn bitmap_payload_signature() -> Signature {
    Signature::user_defined(Volatility::Immutable)
}

pub(crate) fn coerce_bitmap_column(values: &ArrayRef) -> Result<ArrayRef> {
    match values.data_type() {
        DataType::Binary => Ok(Arc::clone(values)),
        DataType::LargeBinary | DataType::BinaryView => cast(values, &DataType::Binary)
            .map_err(|error| DataFusionError::Execution(error.to_string())),
        other => exec_err!("bitmap aggregate expected BINARY, got {other}"),
    }
}

fn payload_allowed(fold: BitmapFold, data_type: &DataType) -> bool {
    match fold {
        BitmapFold::Or | BitmapFold::And => matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ),
        BitmapFold::Construct => matches!(
            data_type,
            DataType::Null
                | DataType::Int8
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
                | DataType::Decimal256(_, _)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
        ),
    }
}

fn refuse_payload(
    name: &str,
    fold: BitmapFold,
    argument: Option<&str>,
    arg_types: &[DataType],
) -> Result<()> {
    let Some(data_type) = arg_types.first() else {
        return exec_err!("{name} expects one argument");
    };
    if arg_types.len() != 1 {
        return exec_err!("{name} expects one argument, got {}", arg_types.len());
    }
    if payload_allowed(fold, data_type) {
        return Ok(());
    }
    let wanted = match fold {
        BitmapFold::Construct => "BIGINT",
        BitmapFold::Or | BitmapFold::And => "BINARY",
    };
    let got = spark_type_name(data_type);
    if let Some(argument) = argument {
        plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}({argument})\" \
             due to data type mismatch: The first parameter requires the \"{wanted}\" type, \
             however \"{argument}\" has the type \"{got}\". SQLSTATE: 42K09"
        )
    } else {
        plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}\" due to data \
             type mismatch: The first parameter requires the \"{wanted}\" type, however the \
             argument has the type \"{got}\". SQLSTATE: 42K09"
        )
    }
}

fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "VOID".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Float16
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Union(_, _) => data_type.to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP_LTZ".to_string(),
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        DataType::Struct(fields) => {
            let inner = fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), spark_type_name(field.data_type())))
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{inner}>")
        }
        DataType::Map(entry, _) => match entry.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => format!(
                "MAP<{}, {}>",
                spark_type_name(pair[0].data_type()),
                spark_type_name(pair[1].data_type())
            ),
            _ => "MAP".to_string(),
        },
        DataType::Dictionary(_, values) => spark_type_name(values),
        DataType::RunEndEncoded(_, values) => spark_type_name(values.data_type()),
    }
}

fn malformed_bigint_cast(value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"BIGINT\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    ))
}

fn parse_bigint_cell(raw: &str) -> Result<i64> {
    raw.trim()
        .parse::<i64>()
        .map_err(|_| malformed_bigint_cast(raw))
}

pub(crate) fn construct_positions(column: &ArrayRef) -> Result<Vec<Option<i64>>> {
    if matches!(column.data_type(), DataType::Null) {
        return Ok(vec![None; column.len()]);
    }
    if matches!(
        column.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        let casted = cast(column, &DataType::Utf8)?;
        let strings = casted.as_string::<i32>();
        let mut positions = Vec::with_capacity(strings.len());
        for row in 0..strings.len() {
            if strings.is_null(row) {
                positions.push(None);
            } else {
                positions.push(Some(parse_bigint_cell(strings.value(row))?));
            }
        }
        return Ok(positions);
    }
    let casted = if column.data_type() == &DataType::Int64 {
        Arc::clone(column)
    } else {
        cast(column, &DataType::Int64)?
    };
    let positions = casted.as_primitive::<Int64Type>();
    for row in 0..positions.len() {
        if positions.is_null(row) && !column.is_null(row) {
            return Err(cast_overflow_error(column, row));
        }
    }
    let mut result = Vec::with_capacity(positions.len());
    for row in 0..positions.len() {
        if positions.is_valid(row) {
            result.push(Some(positions.value(row)));
        } else {
            result.push(None);
        }
    }
    Ok(result)
}

fn cast_overflow_error(source: &ArrayRef, row: usize) -> DataFusionError {
    let (value, spark_type) = overflow_cell_text(source, row);
    DataFusionError::Execution(format!(
        "[CAST_OVERFLOW] The value {value} of the type \"{spark_type}\" cannot be cast to \
         \"BIGINT\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL \
         instead. SQLSTATE: 22003"
    ))
}

fn overflow_cell_text(source: &ArrayRef, row: usize) -> (String, String) {
    match source.data_type() {
        DataType::Float64 => {
            let value = source.as_primitive::<Float64Type>().value(row);
            let text = java_double_text(value);
            let text = if value.is_finite() {
                format!("{text}D")
            } else {
                text
            };
            (text, "DOUBLE".to_string())
        }
        DataType::Float32 => {
            let value = source.as_primitive::<Float32Type>().value(row);
            let text = java_float_text(value);
            let text = if value.is_finite() {
                format!("{text}F")
            } else {
                text
            };
            (text, "FLOAT".to_string())
        }
        DataType::Decimal128(_, _) => {
            let text = source.as_primitive::<Decimal128Type>().value_as_string(row);
            (format!("{text}BD"), spark_type_name(source.data_type()))
        }
        DataType::Decimal256(_, _) => {
            let text = source.as_primitive::<Decimal256Type>().value_as_string(row);
            (format!("{text}BD"), spark_type_name(source.data_type()))
        }
        _ => (
            ScalarValue::try_from_array(source, row)
                .map_or_else(|_| "invalid".to_string(), |scalar| scalar.to_string()),
            spark_type_name(source.data_type()),
        ),
    }
}

pub(crate) fn identity_byte(fold: BitmapFold) -> u8 {
    match fold {
        BitmapFold::Construct | BitmapFold::Or => 0,
        BitmapFold::And => 0xff,
    }
}

pub(crate) fn invalid_bitmap_position(position: i64) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[INVALID_BITMAP_POSITION] The 0-indexed bitmap position {position} is out of bounds. \
         The bitmap has 32768 bits (4096 bytes). SQLSTATE: 22003"
    ))
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(crate) fn set_bit(bits: &mut [u8], position: i64) -> Result<()> {
    if !(0..BITMAP_BITS).contains(&position) {
        return Err(invalid_bitmap_position(position));
    }
    let byte_index = (position as usize) >> 3;
    let shift = (position as u32) & 7;
    bits[byte_index] |= 1_u8 << shift;
    Ok(())
}

fn fold_words(destination: &mut [u8], source: &[u8], is_and: bool) {
    let width = destination.len().min(source.len());
    let word_bytes = width - (width % 8);
    let (dest_words, dest_tail) = destination[..width].split_at_mut(word_bytes);
    let (source_words, source_tail) = source[..width].split_at(word_bytes);
    for (dest, src) in dest_words
        .chunks_exact_mut(8)
        .zip(source_words.chunks_exact(8))
    {
        let mut dest_bytes = [0_u8; 8];
        dest_bytes.copy_from_slice(dest);
        let mut source_bytes = [0_u8; 8];
        source_bytes.copy_from_slice(src);
        let folded = if is_and {
            u64::from_ne_bytes(dest_bytes) & u64::from_ne_bytes(source_bytes)
        } else {
            u64::from_ne_bytes(dest_bytes) | u64::from_ne_bytes(source_bytes)
        };
        dest.copy_from_slice(&folded.to_ne_bytes());
    }
    if is_and {
        for (dest, src) in dest_tail.iter_mut().zip(source_tail) {
            *dest &= *src;
        }
    } else {
        for (dest, src) in dest_tail.iter_mut().zip(source_tail) {
            *dest |= *src;
        }
    }
}

pub(crate) fn fold_incoming(destination: &mut [u8], incoming: &[u8], fold: BitmapFold) {
    let width = incoming.len().min(destination.len());
    match fold {
        BitmapFold::And => {
            fold_words(&mut destination[..width], &incoming[..width], true);
            destination[width..].fill(0);
        }
        BitmapFold::Construct | BitmapFold::Or => {
            fold_words(&mut destination[..width], &incoming[..width], false);
        }
    }
}

pub(crate) fn update_positions_into(bits: &mut [u8], values: &ArrayRef) -> Result<()> {
    for position in construct_positions(values)?.into_iter().flatten() {
        set_bit(bits, position)?;
    }
    Ok(())
}

pub(crate) fn for_each_binary(
    values: &ArrayRef,
    mut visit: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let values = coerce_bitmap_column(values)?;
    let array = values.as_binary::<i32>();
    for index in 0..array.len() {
        if array.is_valid(index) {
            visit(array.value(index))?;
        }
    }
    Ok(())
}

pub(crate) fn packed_bitmaps_to_array(bits: Vec<u8>) -> Result<ArrayRef> {
    if !bits.len().is_multiple_of(BITMAP_BYTES) {
        return exec_err!("bitmap packed length is not a multiple of {BITMAP_BYTES}");
    }
    let groups = bits.len() / BITMAP_BYTES;
    let mut offsets = Vec::with_capacity(groups + 1);
    for group in 0..=groups {
        let offset = i32::try_from(group * BITMAP_BYTES).map_err(|_| {
            DataFusionError::Execution("bitmap group count exceeds Binary offset range".to_string())
        })?;
        offsets.push(offset);
    }
    BinaryArray::try_new(OffsetBuffer::new(offsets.into()), Buffer::from(bits), None)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

impl Accumulator for BitmapAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(column) = values.first() else {
            return exec_err!("bitmap aggregate missing argument column");
        };
        match self.fold {
            BitmapFold::Construct => self.update_positions(column),
            BitmapFold::Or | BitmapFold::And => self.update_bitmaps(column),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(column) = states.first() else {
            return exec_err!("bitmap aggregate missing state column");
        };
        self.update_bitmaps(column)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.bits.to_vec())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.bits.to_vec()))])
    }
}

#[cfg(test)]
mod tests;
