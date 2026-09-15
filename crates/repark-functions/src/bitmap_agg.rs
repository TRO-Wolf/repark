use std::sync::Arc;

use datafusion::logical_expr::AggregateUDF;

#[must_use]
pub fn functions() -> Vec<Arc<AggregateUDF>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, BinaryArray, Int32Array, Int64Array};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    const BITMAP_BYTES: usize = 4096;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
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
        let batch = batch(
            &ctx,
            "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b, \
             bitmap_or_agg(CAST(NULL AS BINARY)) AS o, \
             bitmap_and_agg(CAST(NULL AS BINARY)) AS a \
             FROM VALUES (1) AS t(x) WHERE false",
        )
        .await;
        assert_eq!(binary_cell(&batch, 0), vec![0_u8; BITMAP_BYTES]);
        assert_eq!(binary_cell(&batch, 1), vec![0_u8; BITMAP_BYTES]);
        assert_eq!(binary_cell(&batch, 2), vec![0xff_u8; BITMAP_BYTES]);
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
            "SELECT bitmap_or_agg(CAST(NULL AS BINARY)) \
             OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM VALUES (1), (2) AS t(x)",
            "SELECT bitmap_and_agg(CAST(NULL AS BINARY)) \
             OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM VALUES (1), (2) AS t(x)",
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
