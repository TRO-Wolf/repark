use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

const BITMAP_BYTES: usize = 4096;
const BITMAP_BITS: i64 = 32768;

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
enum BitmapFold {
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
                ],
                Volatility::Immutable,
            ),
            fold: BitmapFold::Construct,
        }
    }

    fn or_agg() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
            fold: BitmapFold::Or,
        }
    }

    fn and_agg() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
            fold: BitmapFold::And,
        }
    }

    fn identity_byte(fold: BitmapFold) -> u8 {
        match fold {
            BitmapFold::Construct | BitmapFold::Or => 0,
            BitmapFold::And => 0xff,
        }
    }

    fn identity_bitmap(fold: BitmapFold) -> [u8; BITMAP_BYTES] {
        [Self::identity_byte(fold); BITMAP_BYTES]
    }

    fn identity_scalar(fold: BitmapFold) -> ScalarValue {
        ScalarValue::Binary(Some(vec![Self::identity_byte(fold); BITMAP_BYTES]))
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

    fn create_sliding_accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!(
            "Aggregate can not be used as a sliding accumulator because retract_batch is not implemented"
        )
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

    fn set_bit(bits: &mut [u8; BITMAP_BYTES], position: i64) -> Result<()> {
        if !(0..BITMAP_BITS).contains(&position) {
            return exec_err!(
                "bitmap_construct_agg position {position} is outside [0, {}]",
                BITMAP_BITS - 1
            );
        }
        let unsigned = u32::try_from(position).map_err(|_| {
            DataFusionError::Internal(format!(
                "bitmap_construct_agg position {position} does not fit u32"
            ))
        })?;
        let byte_index = usize::try_from(unsigned / 8).map_err(|_| {
            DataFusionError::Internal(
                "bitmap_construct_agg byte index does not fit usize".to_string(),
            )
        })?;
        let shift = u8::try_from(unsigned % 8).map_err(|_| {
            DataFusionError::Internal("bitmap_construct_agg bit shift does not fit u8".to_string())
        })?;
        bits[byte_index] |= 1_u8 << shift;
        Ok(())
    }

    fn fold_bytes(bits: &mut [u8; BITMAP_BYTES], incoming: &[u8], fold: BitmapFold) -> Result<()> {
        if incoming.len() != BITMAP_BYTES {
            return exec_err!(
                "bitmap aggregate expected BINARY of {BITMAP_BYTES} bytes, got {}",
                incoming.len()
            );
        }
        match fold {
            BitmapFold::Construct | BitmapFold::Or => {
                for (destination, source) in bits.iter_mut().zip(incoming.iter()) {
                    *destination |= *source;
                }
            }
            BitmapFold::And => {
                for (destination, source) in bits.iter_mut().zip(incoming.iter()) {
                    *destination &= *source;
                }
            }
        }
        Ok(())
    }

    fn update_positions(&mut self, values: &ArrayRef) -> Result<()> {
        let casted = cast(values, &DataType::Int64)?;
        let positions = casted.as_primitive::<Int64Type>();
        for position in positions.iter().flatten() {
            Self::set_bit(&mut self.bits, position)?;
        }
        Ok(())
    }

    fn update_bitmaps(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Binary => {
                for incoming in values.as_binary::<i32>().iter().flatten() {
                    Self::fold_bytes(&mut self.bits, incoming, self.fold)?;
                }
            }
            DataType::LargeBinary => {
                for incoming in values.as_binary::<i64>().iter().flatten() {
                    Self::fold_bytes(&mut self.bits, incoming, self.fold)?;
                }
            }
            DataType::BinaryView => {
                for incoming in values.as_binary_view().iter().flatten() {
                    Self::fold_bytes(&mut self.bits, incoming, self.fold)?;
                }
            }
            other => {
                return exec_err!("bitmap aggregate expected BINARY, got {other}");
            }
        }
        Ok(())
    }
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
    async fn sliding_frame_refuses_loudly() {
        let ctx = ctx();
        for sql in [
            "SELECT bitmap_construct_agg(bitmap_bit_position(x)) \
             OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM VALUES (1), (2) AS t(x)",
            "SELECT bitmap_or_agg(b) \
             OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM (SELECT g, bitmap_construct_agg(bitmap_bit_position(x)) AS b \
                   FROM VALUES (1, 1), (2, 2) AS t(x, g) GROUP BY g)",
            "SELECT bitmap_and_agg(b) \
             OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM (SELECT g, bitmap_construct_agg(bitmap_bit_position(x)) AS b \
                   FROM VALUES (1, 1), (2, 2) AS t(x, g) GROUP BY g)",
        ] {
            let planned = ctx.sql(sql).await;
            let message = match planned {
                Ok(frame) => frame
                    .collect()
                    .await
                    .expect_err("sliding frame must refuse")
                    .to_string(),
                Err(error) => error.to_string(),
            };
            let lower = message.to_ascii_lowercase();
            assert!(
                lower.contains("retract_batch") || lower.contains("sliding"),
                "got {message}"
            );
        }
    }
}
