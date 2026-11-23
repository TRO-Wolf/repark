use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, DictionaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::ansi::with_spark_ansi_config;
use crate::cast_map::leaf::{Mode, narrow_numeric};
use crate::cast_map::{
    ARROW_CAST_OPERAND_NAME, SparkCoercionCastMark, insert_string_integral_before_coercion,
    narrow_cast_parts, register,
};

fn narrow_context(ansi: bool) -> SessionContext {
    let mut rules =
        insert_string_integral_before_coercion(datafusion::optimizer::Analyzer::new().rules)
            .unwrap();
    rules.push(Arc::new(SparkCoercionCastMark));
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(with_spark_ansi_config(SessionConfig::new(), ansi))
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    register(&ctx);
    ctx
}

fn first_cell(batches: &[RecordBatch]) -> String {
    let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
    array_value_to_string(batch.column(0).as_ref(), 0).unwrap()
}

#[test]
fn signed_integer_narrowing_wraps_in_legacy_and_raises_cast_overflow_in_ansi() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(4_294_967_297i64),
        Some(2_147_483_648i64),
        Some(-2_147_483_649i64),
        None,
    ]));
    let wrapped = narrow_numeric(&source, &DataType::Int32, Mode::Legacy).unwrap();
    let ints = wrapped.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ints.value(0), 1);
    assert_eq!(ints.value(1), i32::MIN);
    assert_eq!(ints.value(2), i32::MAX);
    assert!(ints.is_null(3));

    let error = narrow_numeric(&source, &DataType::Int32, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(
            "[CAST_OVERFLOW] The value 4294967297L of the type \"BIGINT\" cannot be cast to \
             \"INT\" due to an overflow."
        ),
        "{error}"
    );
    assert!(error.contains("SQLSTATE: 22003"), "{error}");

    let small: ArrayRef = Arc::new(Int16Array::from(vec![Some(128i16), Some(-129i16)]));
    let tiny = narrow_numeric(&small, &DataType::Int8, Mode::Legacy).unwrap();
    let bytes = tiny.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(bytes.value(0), -128);
    assert_eq!(bytes.value(1), 127);

    let small_err = narrow_numeric(&small, &DataType::Int8, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        small_err.contains("The value 128S of the type \"SMALLINT\" cannot be cast to \"TINYINT\""),
        "{small_err}"
    );
}

#[test]
fn decimal_to_integral_truncates_wraps_and_formats_bd_overflow() {
    let source: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(12_800i128),
            Some(-12_899i128),
            Some(-12_900i128),
            Some(30_099i128),
            None,
        ])
        .with_precision_and_scale(10, 2)
        .unwrap(),
    );
    let wrapped = narrow_numeric(&source, &DataType::Int8, Mode::Legacy).unwrap();
    let bytes = wrapped.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(bytes.value(0), -128);
    assert_eq!(bytes.value(1), -128);
    assert_eq!(bytes.value(2), 127);
    assert_eq!(bytes.value(3), 44);
    assert!(bytes.is_null(4));

    let error = narrow_numeric(&source, &DataType::Int8, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(
            "[CAST_OVERFLOW] The value 128.00BD of the type \"DECIMAL(10,2)\" cannot be cast to \
             \"TINYINT\""
        ),
        "{error}"
    );

    let dec38: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(4_294_967_297i128)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    let as_int = narrow_numeric(&dec38, &DataType::Int32, Mode::Legacy).unwrap();
    assert_eq!(
        as_int
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn decimal_to_decimal_rounds_half_up_nulls_in_legacy_and_raises_in_ansi() {
    let source: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(9_994i128),
            Some(9_995i128),
            Some(-9_994i128),
            Some(-9_995i128),
            Some(10_000i128),
            None,
        ])
        .with_precision_and_scale(10, 3)
        .unwrap(),
    );
    let target = DataType::Decimal128(3, 2);
    let legacy = narrow_numeric(&source, &target, Mode::Legacy).unwrap();
    let out = legacy.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(out.value(0), 999);
    assert!(out.is_null(1));
    assert_eq!(out.value(2), -999);
    assert!(out.is_null(3));
    assert!(out.is_null(4));
    assert!(out.is_null(5));

    let error = narrow_numeric(&source, &target, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  9.995 cannot be represented as \
             Decimal(3, 2)."
        ),
        "{error}"
    );
    assert!(error.contains("SQLSTATE: 22003"), "{error}");
}

#[test]
fn float_and_double_narrowing_follows_jvm_saturation_and_spark_error_text() {
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(1e10f32),
        Some(-1e10f32),
        Some(f32::NAN),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(-0.0f32),
        None,
    ]));
    let tiny = narrow_numeric(&floats, &DataType::Int8, Mode::Legacy).unwrap();
    let bytes = tiny.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(bytes.value(0), -1);
    assert_eq!(bytes.value(1), 0);
    assert_eq!(bytes.value(2), 0);
    assert_eq!(bytes.value(3), -1);
    assert_eq!(bytes.value(4), 0);
    assert_eq!(bytes.value(5), 0);
    assert!(bytes.is_null(6));

    let float_err = narrow_numeric(&floats, &DataType::Int8, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        float_err.contains("The value 1.0E10 of the type \"FLOAT\" cannot be cast to \"TINYINT\""),
        "{float_err}"
    );

    let doubles: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(9_007_199_254_740_992.0f64),
        Some(9_223_372_036_854_775_808.0f64),
        Some(f64::NAN),
        Some(f64::INFINITY),
    ]));
    let as_int = narrow_numeric(&doubles, &DataType::Int32, Mode::Legacy).unwrap();
    let ints = as_int.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ints.value(0), i32::MAX);
    assert_eq!(ints.value(1), i32::MAX);
    assert_eq!(ints.value(2), 0);
    assert_eq!(ints.value(3), i32::MAX);

    let as_small = narrow_numeric(&doubles, &DataType::Int16, Mode::Legacy).unwrap();
    let shorts = as_small.as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!(shorts.value(0), -1);

    let two_to_63: ArrayRef = Arc::new(Float64Array::from(vec![Some(
        9_223_372_036_854_775_808.0f64,
    )]));
    for mode in [Mode::Legacy, Mode::Ansi] {
        let bigint = narrow_numeric(&two_to_63, &DataType::Int64, mode).unwrap();
        assert_eq!(
            bigint
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MAX
        );
    }

    let past_i64: ArrayRef = Arc::new(Float64Array::from(vec![Some(
        9_223_372_036_854_777_856.0f64,
    )]));
    let dbl_err = narrow_numeric(&past_i64, &DataType::Int64, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        dbl_err.contains("D of the type \"DOUBLE\" cannot be cast to \"BIGINT\""),
        "{dbl_err}"
    );

    let nan_input: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN)]));
    let nan_error = narrow_numeric(&nan_input, &DataType::Int32, Mode::Ansi)
        .unwrap_err()
        .to_string();
    assert!(
        nan_error.contains("The value NaN of the type \"DOUBLE\" cannot be cast to \"INT\""),
        "{nan_error}"
    );
}

#[test]
fn dictionary_and_null_inputs_narrow_cleanly() {
    let keys = Int32Array::from(vec![Some(0), Some(1), None]);
    let values: ArrayRef = Arc::new(Int64Array::from(vec![300i64, -129i64]));
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
    let out = narrow_numeric(&dict, &DataType::Int8, Mode::Legacy).unwrap();
    let bytes = out.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(bytes.value(0), 44);
    assert_eq!(bytes.value(1), 127);
    assert!(bytes.is_null(2));

    let nulls = datafusion::arrow::array::new_null_array(&DataType::Null, 2);
    let typed = narrow_numeric(&nulls, &DataType::Decimal128(3, 2), Mode::Legacy).unwrap();
    assert_eq!(typed.data_type(), &DataType::Decimal128(3, 2));
    assert_eq!(typed.null_count(), 2);
}

#[tokio::test]
async fn analyzer_rewrites_narrow_casts_preserves_nullability_and_leaves_safe_casts() {
    let ctx = narrow_context(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Int64, false),
        Field::new("d", DataType::Decimal128(10, 2), false),
        Field::new("f", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4_294_967_297i64])),
            Arc::new(
                Decimal128Array::from(vec![12_800i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(Float64Array::from(vec![9_007_199_254_740_992.0f64])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let frame = ctx
        .sql(
            "SELECT CAST(b AS INT) AS bi, CAST(d AS TINYINT) AS dt, CAST(d AS DECIMAL(3,2)) AS dd, \
             CAST(f AS INT) AS fi, CAST(d AS INT) AS safe_di, CAST(d AS DECIMAL(10,1)) AS safe_dd, \
             TRY_CAST(b AS INT) AS tc FROM t",
        )
        .await
        .unwrap();
    let plan = ctx.state().optimize(frame.logical_plan()).unwrap();
    let out_schema = plan.schema();
    assert!(!out_schema.field(0).is_nullable());
    assert!(out_schema.field(1).is_nullable());
    assert!(out_schema.field(2).is_nullable());
    assert!(out_schema.field(3).is_nullable());

    let shown = plan.display_indent().to_string();
    assert!(shown.contains("__repark_narrow_to_int__"), "{shown}");
    assert!(shown.contains("__repark_narrow_to_tinyint__"), "{shown}");
    assert!(
        shown.contains("__repark_narrow_to_decimal_3_2__"),
        "{shown}"
    );
    assert!(shown.contains("TRY_CAST("), "{shown}");
    assert!(
        !shown.contains("__repark_narrow_to_decimal_10_1__"),
        "{shown}"
    );

    let batches = frame.collect().await.unwrap();
    let batch = &batches[0];
    assert_eq!(
        array_value_to_string(batch.column(0).as_ref(), 0).unwrap(),
        "1"
    );
    assert_eq!(
        array_value_to_string(batch.column(1).as_ref(), 0).unwrap(),
        "-128"
    );
    assert!(batch.column(2).is_null(0));
    assert_eq!(
        array_value_to_string(batch.column(3).as_ref(), 0).unwrap(),
        "2147483647"
    );
    assert_eq!(
        array_value_to_string(batch.column(4).as_ref(), 0).unwrap(),
        "128"
    );
    assert_eq!(
        array_value_to_string(batch.column(5).as_ref(), 0).unwrap(),
        "128.0"
    );
    assert!(batch.column(6).is_null(0));
}

#[tokio::test]
async fn narrow_cast_respects_arrow_operand_values_outer_null_join_and_parts() {
    for (sql, expected) in [
        (
            format!(
                "SELECT CAST({ARROW_CAST_OPERAND_NAME}(b) AS INT) AS r \
                 FROM (VALUES (CAST(4294967297 AS BIGINT))) AS t(b)"
            ),
            "Can't cast value 4294967297",
        ),
        (
            format!(
                "SELECT CAST({ARROW_CAST_OPERAND_NAME}(d) AS DECIMAL(3,2)) AS r \
                 FROM (VALUES (CAST(10.00 AS DECIMAL(10,2)))) AS t(d)"
            ),
            "too large to store",
        ),
        (
            "SELECT * FROM (VALUES (CAST(CAST(4294967297 AS BIGINT) AS INT))) AS t(r)".to_owned(),
            "Can't cast value 4294967297",
        ),
    ] {
        let error = narrow_context(false)
            .sql(&sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{sql}: {error}");
    }

    let ctx = narrow_context(false);
    let outer = ctx
        .sql(
            "SELECT r FROM (SELECT CAST(d AS DECIMAL(3,2)) AS r \
             FROM (VALUES (CAST(10.00 AS DECIMAL(10,2)))) AS t(d)) AS u WHERE r IS NULL",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(outer.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let joined = ctx
        .sql(
            "SELECT l.id FROM (VALUES (1)) AS l(id) \
             JOIN (VALUES (CAST(4294967297 AS BIGINT))) AS r(b) ON l.id = CAST(r.b AS INT)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(first_cell(&joined), "1");

    let frame = ctx
        .sql(
            "SELECT CAST(b AS INT), CAST(d AS DECIMAL(3,2)) \
             FROM (VALUES (CAST(4294967297 AS BIGINT), CAST(10.00 AS DECIMAL(10,2)))) AS t(b, d)",
        )
        .await
        .unwrap();
    let analyzed = ctx
        .state()
        .analyzer()
        .execute_and_check(
            frame.logical_plan().clone(),
            ctx.state().config_options(),
            |_, _| {},
        )
        .unwrap();
    let datafusion::logical_expr::LogicalPlan::Projection(projection) = analyzed else {
        panic!("expected projection");
    };
    let (_, int_target) = narrow_cast_parts(&projection.expr[0].clone().unalias()).unwrap();
    assert_eq!(int_target, DataType::Int32);
    let (_, dec_target) = narrow_cast_parts(&projection.expr[1].clone().unalias()).unwrap();
    assert_eq!(dec_target, DataType::Decimal128(3, 2));
}
