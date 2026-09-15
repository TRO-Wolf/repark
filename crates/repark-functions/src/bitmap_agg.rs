use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
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
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    DataType::Utf8View,
                ],
                Volatility::Immutable,
            ),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
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
    Signature::uniform(
        1,
        vec![
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
        ],
        Volatility::Immutable,
    )
}

pub(crate) fn coerce_bitmap_column(values: &ArrayRef) -> Result<ArrayRef> {
    match values.data_type() {
        DataType::Binary => Ok(Arc::clone(values)),
        DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => cast(values, &DataType::Binary)
            .map_err(|error| DataFusionError::Execution(error.to_string())),
        other => exec_err!("bitmap aggregate expected BINARY, got {other}"),
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
    let casted = if values.data_type() == &DataType::Int64 {
        Arc::clone(values)
    } else {
        cast(values, &DataType::Int64)?
    };
    let positions = casted.as_primitive::<Int64Type>();
    if positions.null_count() == 0 {
        for &position in positions.values() {
            set_bit(bits, position)?;
        }
    } else {
        for (index, &position) in positions.values().iter().enumerate() {
            if positions.is_valid(index) {
                set_bit(bits, position)?;
            }
        }
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
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use super::BITMAP_BYTES;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    fn ctx_with_empty_bitmaps() -> SessionContext {
        let ctx = ctx();
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
        let array: ArrayRef = Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new()));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("empty binary batch");
        ctx.register_batch("empty_bitmaps", batch)
            .expect("register empty_bitmaps");
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().expect("one batch")
    }

    fn binary_cell(batch: &RecordBatch, column: usize) -> Vec<u8> {
        let schema = batch.schema();
        let field = schema.field(column);
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(!field.is_nullable());
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("BinaryArray");
        assert!(array.is_valid(0));
        array.value(0).to_vec()
    }

    fn int64_cell(batch: &RecordBatch, column: usize) -> i64 {
        let schema = batch.schema();
        let field = schema.field(column);
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable());
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert!(array.is_valid(0));
        array.value(0)
    }

    fn int32_cell(batch: &RecordBatch, column: usize) -> i32 {
        let schema = batch.schema();
        let field = schema.field(column);
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(!field.is_nullable());
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert!(array.is_valid(0));
        array.value(0)
    }

    fn construct_fixture_bitmap() -> Vec<u8> {
        let mut bits = vec![0_u8; BITMAP_BYTES];
        bits[0] = 0x07;
        bits[BITMAP_BYTES - 1] = 0x40;
        bits
    }

    #[tokio::test]
    async fn construct_agg_sets_bits_zero_one_two_and_last_and_ignores_null() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b, \
             bitmap_count(bitmap_construct_agg(bitmap_bit_position(x))) AS c \
             FROM VALUES (1), (2), (3), (32767), (NULL) AS t(x)",
        )
        .await;
        assert_eq!(binary_cell(&batch, 0), construct_fixture_bitmap());
        assert_eq!(int64_cell(&batch, 1), 4);
    }

    #[tokio::test]
    async fn or_and_agg_fold_grouped_bitmaps() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT bitmap_count(bitmap_or_agg(b)) AS o, \
             bitmap_count(bitmap_and_agg(b)) AS a \
             FROM (SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b \
                   FROM VALUES (1, 1), (2, 1), (2, 2), (3, 2) AS t(x, g) \
                   GROUP BY g)",
        )
        .await;
        assert_eq!(int64_cell(&batch, 0), 3);
        assert_eq!(int64_cell(&batch, 1), 1);
    }

    #[tokio::test]
    async fn empty_input_construct_and_or_are_zeros_and_is_ones() {
        let ctx = ctx();
        let construct = batch(
            &ctx,
            "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b \
             FROM VALUES (1) AS t(x) WHERE false",
        )
        .await;
        let empty = ctx_with_empty_bitmaps();
        let or_agg = batch(&empty, "SELECT bitmap_or_agg(b) AS o FROM empty_bitmaps").await;
        let and_agg = batch(&empty, "SELECT bitmap_and_agg(b) AS a FROM empty_bitmaps").await;
        assert_eq!(binary_cell(&construct, 0), vec![0_u8; BITMAP_BYTES]);
        assert_eq!(binary_cell(&or_agg, 0), vec![0_u8; BITMAP_BYTES]);
        assert_eq!(binary_cell(&and_agg, 0), vec![0xff_u8; BITMAP_BYTES]);
    }

    #[tokio::test]
    async fn and_agg_of_one_bitmap_has_length_4096() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT length(bitmap_and_agg(b)) AS n \
             FROM (SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b \
                   FROM VALUES (1) AS t(x))",
        )
        .await;
        let schema = batch.schema();
        match schema.field(0).data_type() {
            DataType::Int32 => assert_eq!(int32_cell(&batch, 0), 4096),
            DataType::Int64 => assert_eq!(int64_cell(&batch, 0), 4096),
            other => panic!("unexpected length type {other:?}"),
        }
    }

    #[tokio::test]
    async fn out_of_range_position_names_invalid_bitmap_position() {
        let ctx = ctx();
        for sql in [
            "SELECT bitmap_construct_agg(x) FROM VALUES (32768) AS t(x)",
            "SELECT bitmap_construct_agg(x) FROM VALUES (-1) AS t(x)",
        ] {
            let error = ctx
                .sql(sql)
                .await
                .expect("plan")
                .collect()
                .await
                .expect_err("out of range must refuse");
            let message = error.to_string();
            assert!(
                message.contains("[INVALID_BITMAP_POSITION]"),
                "got {message}"
            );
            assert!(message.contains("32768 bits (4096 bytes)"), "got {message}");
            assert!(message.contains("SQLSTATE: 22003"), "got {message}");
        }
        let batch = batch(
            &ctx,
            "SELECT bitmap_count(bitmap_construct_agg(x)) AS c FROM VALUES (32767) AS t(x)",
        )
        .await;
        assert_eq!(int64_cell(&batch, 0), 1);
    }

    #[tokio::test]
    async fn short_binary_or_and_normalize_to_4096() {
        let ctx = ctx();
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"\x01"[..])]));
        let short = RecordBatch::try_new(schema, vec![array]).expect("short binary");
        ctx.register_batch("short_bitmaps", short)
            .expect("register short");
        let or_agg = batch(
            &ctx,
            "SELECT length(bitmap_or_agg(b)) AS l, bitmap_count(bitmap_or_agg(b)) AS c \
             FROM short_bitmaps",
        )
        .await;
        match or_agg.schema().field(0).data_type() {
            DataType::Int32 => assert_eq!(int32_cell(&or_agg, 0), 4096),
            DataType::Int64 => assert_eq!(int64_cell(&or_agg, 0), 4096),
            other => panic!("unexpected length type {other:?}"),
        }
        assert_eq!(int64_cell(&or_agg, 1), 1);
        let and_agg = batch(
            &ctx,
            "SELECT length(bitmap_and_agg(b)) AS l, bitmap_count(bitmap_and_agg(b)) AS c \
             FROM short_bitmaps",
        )
        .await;
        match and_agg.schema().field(0).data_type() {
            DataType::Int32 => assert_eq!(int32_cell(&and_agg, 0), 4096),
            DataType::Int64 => assert_eq!(int64_cell(&and_agg, 0), 4096),
            other => panic!("unexpected length type {other:?}"),
        }
        assert_eq!(int64_cell(&and_agg, 1), 1);
        let mut long_bytes = vec![0_u8; BITMAP_BYTES + 1];
        long_bytes[0] = 0x01;
        long_bytes[BITMAP_BYTES] = 0x01;
        let long_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
        let long_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(long_bytes.as_slice())]));
        let long = RecordBatch::try_new(long_schema, vec![long_array]).expect("long binary");
        ctx.register_batch("long_bitmaps", long)
            .expect("register long");
        let long_agg = batch(
            &ctx,
            "SELECT length(bitmap_or_agg(b)) AS l FROM long_bitmaps",
        )
        .await;
        match long_agg.schema().field(0).data_type() {
            DataType::Int32 => assert_eq!(int32_cell(&long_agg, 0), 4096),
            DataType::Int64 => assert_eq!(int64_cell(&long_agg, 0), 4096),
            other => panic!("unexpected length type {other:?}"),
        }
    }

    #[tokio::test]
    async fn unbounded_partition_window_answers() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT bitmap_count(bitmap_construct_agg(x) OVER (PARTITION BY g)) AS c \
             FROM VALUES (1, 1), (1, 2), (2, 3) AS t(g, x) ORDER BY g",
        )
        .await;
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let counts: Vec<i64> = (0..array.len()).map(|index| array.value(index)).collect();
        assert_eq!(counts, vec![2, 2, 1]);
    }
}
