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

async fn plan_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected refusal for {sql}"),
        },
    }
}

async fn assert_refusal(
    ctx: &SessionContext,
    function: &str,
    wanted: &str,
    sql: &str,
    argument: Option<&str>,
    spark_type: &str,
) {
    let message = plan_error(ctx, sql).await;
    assert!(
        message.contains("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]"),
        "class missing for {sql}: {message}"
    );
    assert!(
        message.contains(&format!("Cannot resolve \"{function}(")),
        "call missing for {sql}: {message}"
    );
    if let Some(argument) = argument {
        assert!(
            message.contains(&format!("Cannot resolve \"{function}({argument})\"")),
            "rendering missing for {sql}: {message}"
        );
    }
    assert!(
        message.contains(&format!(
            "The first parameter requires the \"{wanted}\" type"
        )),
        "wanted missing for {sql}: {message}"
    );
    assert!(
        message.contains(&format!("has the type \"{spark_type}\"")),
        "type missing for {sql}: {message}"
    );
    assert!(
        message.contains("SQLSTATE: 42K09"),
        "state missing for {sql}: {message}"
    );
}

#[tokio::test]
async fn or_agg_refuses_non_binary_with_spark_class() {
    let ctx = ctx();
    let cases = [
        (
            "SELECT bitmap_or_agg(x) FROM VALUES (CAST(1 AS INT)), (CAST(2 AS INT)) AS t(x)",
            Some("x"),
            "INT",
        ),
        (
            "SELECT bitmap_or_agg(bitmap_bit_position(x)) FROM VALUES (1), (2) AS t(x)",
            None,
            "BIGINT",
        ),
        (
            "SELECT bitmap_or_agg(x) FROM VALUES (CAST(1.0 AS FLOAT)) AS t(x)",
            Some("x"),
            "FLOAT",
        ),
        (
            "SELECT bitmap_or_agg(x) FROM VALUES (CAST(1.0 AS DOUBLE)) AS t(x)",
            Some("x"),
            "DOUBLE",
        ),
        (
            "SELECT bitmap_or_agg(x) FROM VALUES (true) AS t(x)",
            Some("x"),
            "BOOLEAN",
        ),
        (
            "SELECT bitmap_or_agg(x) FROM VALUES ('abc') AS t(x)",
            Some("x"),
            "STRING",
        ),
        (
            "SELECT bitmap_or_agg(x) FROM VALUES (CAST(1.5 AS DECIMAL(2,1))) AS t(x)",
            Some("x"),
            "DECIMAL(2,1)",
        ),
        (
            "SELECT bitmap_or_agg(x) FROM VALUES (DATE'2020-01-01') AS t(x)",
            Some("x"),
            "DATE",
        ),
        (
            "SELECT bitmap_or_agg(NULL) FROM VALUES (1) AS t(x)",
            Some("NULL"),
            "VOID",
        ),
    ];
    for (sql, argument, spark_type) in cases {
        assert_refusal(&ctx, "bitmap_or_agg", "BINARY", sql, argument, spark_type).await;
    }
}

#[tokio::test]
async fn and_agg_refuses_non_binary_with_spark_class() {
    let ctx = ctx();
    let cases = [
        (
            "SELECT bitmap_and_agg(x) FROM VALUES (CAST(1 AS INT)), (CAST(2 AS INT)) AS t(x)",
            Some("x"),
            "INT",
        ),
        (
            "SELECT bitmap_and_agg(x) FROM VALUES (true) AS t(x)",
            Some("x"),
            "BOOLEAN",
        ),
        (
            "SELECT bitmap_and_agg(x) FROM VALUES ('abc') AS t(x)",
            Some("x"),
            "STRING",
        ),
    ];
    for (sql, argument, spark_type) in cases {
        assert_refusal(&ctx, "bitmap_and_agg", "BINARY", sql, argument, spark_type).await;
    }
}

#[tokio::test]
async fn construct_agg_refuses_non_bigint_with_spark_class() {
    let ctx = ctx();
    let cases = [
        (
            "SELECT bitmap_construct_agg(x) FROM VALUES (true) AS t(x)",
            "bitmap_construct_agg(x)",
            "BOOLEAN",
        ),
        (
            "SELECT bitmap_construct_agg(x) FROM VALUES (X'01') AS t(x)",
            "bitmap_construct_agg(x)",
            "BINARY",
        ),
        (
            "SELECT bitmap_construct_agg(x) FROM VALUES (DATE'2020-01-01') AS t(x)",
            "bitmap_construct_agg(x)",
            "DATE",
        ),
        (
            "SELECT bitmap_construct_agg(x) FROM VALUES \
                 (CAST('2020-01-01 00:00:00' AS TIMESTAMP)) AS t(x)",
            "bitmap_construct_agg(x)",
            "TIMESTAMP",
        ),
    ];
    for (sql, call, spark_type) in cases {
        let message = plan_error(&ctx, sql).await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]"),
            "class missing for {sql}: {message}"
        );
        assert!(
            message.contains(&format!("Cannot resolve \"{call}\"")),
            "call missing for {sql}: {message}"
        );
        assert!(
            message.contains("The first parameter requires the \"BIGINT\" type"),
            "wanted missing for {sql}: {message}"
        );
        assert!(
            message.contains(&format!("has the type \"{spark_type}\"")),
            "type missing for {sql}: {message}"
        );
        assert!(
            message.contains("SQLSTATE: 42K09"),
            "state missing for {sql}: {message}"
        );
    }
}

#[tokio::test]
async fn construct_agg_malformed_string_raises_cast_invalid_input() {
    let ctx = ctx();
    let cases = [
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES ('abc') AS t(x)",
            "abc",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES ('') AS t(x)",
            "",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES ('1.5') AS t(x)",
            "1.5",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES ('1'), ('abc') AS t(x)",
            "abc",
        ),
    ];
    for (sql, value) in cases {
        let message = plan_error(&ctx, sql).await;
        assert!(
            message.contains("[CAST_INVALID_INPUT]"),
            "class missing for {sql}: {message}"
        );
        assert!(
            message.contains(&format!(
                "The value '{value}' of the type \"STRING\" cannot be cast to \"BIGINT\""
            )),
            "value missing for {sql}: {message}"
        );
        assert!(
            message.contains("SQLSTATE: 22018"),
            "state missing for {sql}: {message}"
        );
    }
}

#[tokio::test]
async fn construct_agg_answers_numeric_trimmed_and_null() {
    let ctx = ctx();
    let cases = [
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) AS c \
                 FROM VALUES (CAST(1.7 AS FLOAT)) AS t(x)",
            1,
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) AS c \
                 FROM VALUES (CAST(2.0 AS DOUBLE)) AS t(x)",
            1,
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) AS c \
                 FROM VALUES (CAST(1.5 AS DECIMAL(2,1))) AS t(x)",
            1,
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) AS c FROM VALUES (' 1 ') AS t(x)",
            1,
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) AS c \
                 FROM VALUES (CAST(3 AS INT)) AS t(x)",
            1,
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(NULL)) AS c FROM VALUES (1) AS t(x)",
            0,
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(CAST(NULL AS BIGINT))) AS c \
                 FROM VALUES (1) AS t(x)",
            0,
        ),
    ];
    for (sql, want) in cases {
        let batch = batch(&ctx, sql).await;
        assert_eq!(int64_cell(&batch, 0), want, "wrong count for {sql}");
    }
}

#[tokio::test]
async fn grouped_construct_agg_answers_trimmed_strings() {
    let ctx = ctx();
    let batch = batch(
        &ctx,
        "SELECT bitmap_count(bitmap_construct_agg(x)) AS c \
             FROM VALUES (' 1 ', 1), ('2', 1) AS t(x, g) GROUP BY g",
    )
    .await;
    assert_eq!(int64_cell(&batch, 0), 2);
}

#[tokio::test]
async fn construct_agg_nonfinite_and_huge_numerics_raise_cast_overflow() {
    let ctx = ctx();
    let cases = [
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES \
             (CAST('NaN' AS DOUBLE)) AS t(x)",
            "NaN",
            "DOUBLE",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES \
             (CAST('Infinity' AS DOUBLE)) AS t(x)",
            "Infinity",
            "DOUBLE",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES \
             (CAST('-Infinity' AS DOUBLE)) AS t(x)",
            "-Infinity",
            "DOUBLE",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES \
             (CAST('NaN' AS FLOAT)) AS t(x)",
            "NaN",
            "FLOAT",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES \
             (CAST('1e30' AS DOUBLE)) AS t(x)",
            "1.0E30D",
            "DOUBLE",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) FROM VALUES \
             (CAST('99999999999999999999' AS DECIMAL(20,0))) AS t(x)",
            "99999999999999999999BD",
            "DECIMAL(20,0)",
        ),
    ];
    for (sql, value, spark_type) in cases {
        let message = plan_error(&ctx, sql).await;
        assert!(
            message.contains("[CAST_OVERFLOW]"),
            "class missing for {sql}: {message}"
        );
        assert!(
            message.contains(&format!(
                "The value {value} of the type \"{spark_type}\" cannot be cast to \"BIGINT\""
            )),
            "value missing for {sql}: {message}"
        );
        assert!(
            message.contains("SQLSTATE: 22003"),
            "state missing for {sql}: {message}"
        );
    }
}

#[tokio::test]
async fn construct_agg_overflow_raises_on_grouped_and_window_paths() {
    let ctx = ctx();
    for sql in [
        "SELECT g, bitmap_count(bitmap_construct_agg(x)) AS c \
         FROM VALUES (1, CAST('NaN' AS DOUBLE)) AS t(g, x) GROUP BY g",
        "SELECT bitmap_count(bitmap_construct_agg(x) OVER ()) AS c \
         FROM VALUES (CAST('NaN' AS DOUBLE)) AS t(x)",
    ] {
        let message = plan_error(&ctx, sql).await;
        assert!(
            message.contains("[CAST_OVERFLOW]"),
            "class missing for {sql}: {message}"
        );
        assert!(
            message.contains("The value NaN of the type \"DOUBLE\" cannot be cast to \"BIGINT\""),
            "value missing for {sql}: {message}"
        );
        assert!(
            message.contains("SQLSTATE: 22003"),
            "state missing for {sql}: {message}"
        );
    }
}
