use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion::prelude::SessionConfig;

use super::{NESTED_NTZ_CAST_NAME, nested_ntz_cast_udf};
use crate::timestamp_ntz_cast::timestamp_ntz_cast_udf;

const INSTANT: i64 = 1_767_323_045_123_456;

fn options_at(zone: &str) -> Arc<datafusion::common::config::ConfigOptions> {
    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    config.options().clone()
}

fn zoned(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
}

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
}

fn ntz_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn tagged(name: &str, data_type: DataType, nullable: bool, id: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_string(), id.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

fn ticks(array: &ArrayRef) -> Vec<Option<i64>> {
    let values = array.as_primitive::<TimestampMicrosecondType>();
    (0..values.len())
        .map(|row| values.is_valid(row).then(|| values.value(row)))
        .collect()
}

fn leaf_ticks(zone: &str, input: ArrayRef) -> Vec<Option<i64>> {
    let udf = timestamp_ntz_cast_udf(false);
    let field = Arc::new(Field::new("v", input.data_type().clone(), true));
    let rows = input.len();
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&field)],
            scalar_arguments: &[None],
        })
        .expect("leaf field");
    let value = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![field],
            number_rows: rows,
            return_field,
            config_options: options_at(zone),
        })
        .expect("leaf");
    let ColumnarValue::Array(array) = value else {
        panic!("leaf array");
    };
    ticks(&array)
}

fn invoke(
    zone: &str,
    field: FieldRef,
    value: ColumnarValue,
    paths: &str,
) -> Result<(FieldRef, ColumnarValue), DataFusionError> {
    let udf = nested_ntz_cast_udf();
    let path_field = Arc::new(Field::new("paths", DataType::Utf8, false));
    let path_scalar = ScalarValue::Utf8(Some(paths.to_string()));
    let fields = [Arc::clone(&field), Arc::clone(&path_field)];
    let scalars = [None, Some(&path_scalar)];
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &scalars,
    })?;
    let rows = match &value {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => 1,
    };
    let output = udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![value, ColumnarValue::Scalar(path_scalar)],
        arg_fields: vec![field, path_field],
        number_rows: rows,
        return_field: Arc::clone(&return_field),
        config_options: options_at(zone),
    })?;
    Ok((return_field, output))
}

fn struct_of(fields: Vec<Field>, children: Vec<ArrayRef>, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(StructArray::try_new(Fields::from(fields), children, nulls).expect("struct"))
}

fn field_id(field: &Field) -> Option<&str> {
    field.metadata().get("PARQUET:field_id").map(String::as_str)
}

#[test]
fn one_leaf_matches_the_wall_kernel_in_three_zones() {
    let instants = zoned(vec![Some(INSTANT), None]);
    let numbers = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
    let input = struct_of(
        vec![
            tagged("v", ts_type(), true, "2"),
            tagged("n", DataType::Int32, false, "3"),
        ],
        vec![Arc::clone(&instants), Arc::clone(&numbers)],
        None,
    );
    let field = Arc::new(tagged("st", input.data_type().clone(), true, "1"));
    for zone in ["UTC", "America/New_York", "Asia/Kolkata"] {
        let expected = leaf_ticks(zone, Arc::clone(&instants));
        let (returned, value) = invoke(
            zone,
            Arc::clone(&field),
            ColumnarValue::Array(Arc::clone(&input)),
            "v",
        )
        .expect(zone);
        let ColumnarValue::Array(array) = value else {
            panic!("array");
        };
        let struct_array = array.as_struct();
        assert_eq!(returned.data_type(), array.data_type(), "{zone}");
        assert_eq!(returned.name(), "st");
        assert_eq!(field_id(returned.as_ref()), Some("1"));
        assert_eq!(ticks(struct_array.column(0)), expected, "{zone}");
        assert!(Arc::ptr_eq(struct_array.column(1), &numbers), "{zone}");
        assert_eq!(field_id(struct_array.fields()[0].as_ref()), Some("2"));
        assert_eq!(field_id(struct_array.fields()[1].as_ref()), Some("3"));
        assert!(!struct_array.fields()[1].is_nullable());
        assert_eq!(struct_array.fields()[0].data_type(), &ntz_type());
    }
}

#[test]
fn a_null_struct_keeps_its_validity_and_a_null_leaf_stays_null() {
    let instants = zoned(vec![Some(INSTANT)]);
    let numbers = Arc::new(Int32Array::from(vec![Some(4)])) as ArrayRef;
    let input = struct_of(
        vec![
            Field::new("v", ts_type(), true),
            Field::new("n", DataType::Int32, true),
        ],
        vec![instants, Arc::clone(&numbers)],
        Some(NullBuffer::from(vec![false])),
    );
    let field = Arc::new(Field::new("st", input.data_type().clone(), true));
    let (_, value) = invoke("UTC", field, ColumnarValue::Array(input), "v").expect("null struct");
    let ColumnarValue::Array(array) = value else {
        panic!("array");
    };
    assert!(array.is_null(0));
    assert!(Arc::ptr_eq(array.as_struct().column(1), &numbers));

    let leaf = struct_of(
        vec![
            Field::new("v", ts_type(), true),
            Field::new("n", DataType::Int32, true),
        ],
        vec![
            zoned(vec![None]),
            Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef,
        ],
        None,
    );
    let field = Arc::new(Field::new("st", leaf.data_type().clone(), true));
    let (_, value) = invoke("UTC", field, ColumnarValue::Array(leaf), "v").expect("null leaf");
    let ColumnarValue::Array(array) = value else {
        panic!("array");
    };
    assert!(!array.is_null(0));
    assert_eq!(ticks(array.as_struct().column(0)), vec![None]);
    assert_eq!(
        array
            .as_struct()
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
            .value(0),
        9
    );
}

#[test]
fn a_null_inner_struct_at_depth_two_stays_null() {
    let inner = struct_of(
        vec![
            Field::new("v", ts_type(), true),
            Field::new("n", DataType::Int32, false),
        ],
        vec![
            zoned(vec![Some(INSTANT)]),
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![false])),
    );
    let sibling = Arc::new(Int32Array::from(vec![Some(7)])) as ArrayRef;
    let input = struct_of(
        vec![
            Field::new("i", inner.data_type().clone(), true),
            Field::new("n", DataType::Int32, true),
        ],
        vec![inner, Arc::clone(&sibling)],
        None,
    );
    let field = Arc::new(Field::new("st", input.data_type().clone(), true));
    let (_, value) = invoke("UTC", field, ColumnarValue::Array(input), "i.v").expect("inner null");
    let ColumnarValue::Array(array) = value else {
        panic!("array");
    };
    let outer = array.as_struct();
    assert!(!outer.is_null(0));
    assert!(outer.column(0).is_null(0));
    assert!(Arc::ptr_eq(outer.column(1), &sibling));
    let DataType::Struct(fields) = outer.column(0).data_type() else {
        panic!("inner type");
    };
    assert!(!fields[1].is_nullable());
    assert_eq!(fields[0].data_type(), &ntz_type());
}

#[test]
fn twenty_fields_pass_the_untouched_children_through() {
    let mut fields = Vec::new();
    let mut children = Vec::new();
    for index in 0..19i32 {
        fields.push(Field::new(format!("f{index:02}"), DataType::Int32, true));
        children.push(Arc::new(Int32Array::from(vec![Some(index)])) as ArrayRef);
    }
    fields.push(Field::new("v", ts_type(), true));
    children.push(zoned(vec![Some(INSTANT)]));
    let originals = children.clone();
    let input = struct_of(fields, children, None);
    let field = Arc::new(Field::new("st", input.data_type().clone(), true));
    let (_, value) =
        invoke("America/New_York", field, ColumnarValue::Array(input), "v").expect("wide");
    let ColumnarValue::Array(array) = value else {
        panic!("array");
    };
    let struct_array = array.as_struct();
    for (index, original) in originals.iter().enumerate().take(19) {
        assert!(
            Arc::ptr_eq(struct_array.column(index), original),
            "f{index:02}"
        );
    }
    let expected = leaf_ticks("America/New_York", Arc::clone(&originals[19]));
    assert_eq!(ticks(struct_array.column(19)), expected);
}

#[test]
fn depth_three_converts_only_the_selected_leaf() {
    let instant = zoned(vec![Some(INSTANT)]);
    let inner_n = Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef;
    let deepest = struct_of(
        vec![
            Field::new("v", ts_type(), true),
            Field::new("n", DataType::Int32, false),
        ],
        vec![Arc::clone(&instant), Arc::clone(&inner_n)],
        None,
    );
    let middle_m = Arc::new(Int32Array::from(vec![Some(8)])) as ArrayRef;
    let middle = struct_of(
        vec![
            Field::new("j", deepest.data_type().clone(), true),
            Field::new("m", DataType::Int32, true),
        ],
        vec![deepest, Arc::clone(&middle_m)],
        None,
    );
    let outer_k = Arc::new(Int32Array::from(vec![Some(5)])) as ArrayRef;
    let input = struct_of(
        vec![
            Field::new("i", middle.data_type().clone(), true),
            Field::new("k", DataType::Int32, true),
        ],
        vec![middle, Arc::clone(&outer_k)],
        None,
    );
    let field = Arc::new(Field::new("st", input.data_type().clone(), true));
    let (_, value) = invoke(
        "Asia/Kolkata",
        field,
        ColumnarValue::Array(Arc::clone(&input)),
        "i.j.v",
    )
    .expect("depth");
    let ColumnarValue::Array(array) = value else {
        panic!("array");
    };
    let outer = array.as_struct();
    assert!(Arc::ptr_eq(outer.column(1), &outer_k));
    let middle = outer.column(0).as_struct();
    assert!(Arc::ptr_eq(middle.column(1), &middle_m));
    let deepest = middle.column(0).as_struct();
    assert!(Arc::ptr_eq(deepest.column(1), &inner_n));
    assert!(!deepest.fields()[1].is_nullable());
    assert_eq!(
        ticks(deepest.column(0)),
        leaf_ticks("Asia/Kolkata", instant)
    );
}

#[test]
fn a_scalar_struct_converts_its_one_row_and_a_null_scalar_is_typed() {
    let input = struct_of(
        vec![
            Field::new("v", ts_type(), true),
            Field::new("n", DataType::Int32, true),
        ],
        vec![
            zoned(vec![Some(INSTANT)]),
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
        ],
        None,
    );
    let field = Arc::new(Field::new("st", input.data_type().clone(), true));
    let scalar = ScalarValue::Struct(Arc::new(input.as_struct().clone()));
    let (returned, value) = invoke(
        "UTC",
        Arc::clone(&field),
        ColumnarValue::Scalar(scalar),
        "v",
    )
    .expect("scalar");
    let ColumnarValue::Scalar(scalar) = value else {
        panic!("scalar");
    };
    assert_eq!(scalar.data_type(), returned.data_type().clone());
    let ScalarValue::Struct(array) = scalar else {
        panic!("struct scalar");
    };
    assert_eq!(
        ticks(array.column(0)),
        leaf_ticks("UTC", zoned(vec![Some(INSTANT)]))
    );

    let (_, value) =
        invoke("UTC", field, ColumnarValue::Scalar(ScalarValue::Null), "v").expect("null scalar");
    let ColumnarValue::Scalar(scalar) = value else {
        panic!("null scalar");
    };
    assert!(scalar.is_null());
    assert_eq!(scalar.data_type(), returned.data_type().clone());
    let DataType::Struct(fields) = scalar.data_type() else {
        panic!("null type");
    };
    assert_eq!(fields[0].data_type(), &ntz_type());
}

#[test]
fn an_unresolvable_path_or_a_non_literal_is_a_plan_error() {
    let input = struct_of(
        vec![
            Field::new("v", ts_type(), true),
            Field::new("n", DataType::Int32, true),
        ],
        vec![
            zoned(vec![Some(INSTANT)]),
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
        ],
        None,
    );
    let field = Arc::new(Field::new("st", input.data_type().clone(), true));
    for paths in ["missing", "n", "v.x", "i.v", ""] {
        let error = invoke(
            "UTC",
            Arc::clone(&field),
            ColumnarValue::Array(Arc::clone(&input)),
            paths,
        )
        .expect_err(paths);
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{paths}: {error}"
        );
    }
    let udf = nested_ntz_cast_udf();
    let path_field = Arc::new(Field::new("paths", DataType::Utf8, true));
    let error = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&field), path_field],
            scalar_arguments: &[None, None],
        })
        .expect_err("column path");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert_eq!(nested_ntz_cast_udf().name(), NESTED_NTZ_CAST_NAME);
    assert!(
        crate::instant_ts::functions()
            .iter()
            .any(|udf| udf.name() == NESTED_NTZ_CAST_NAME)
    );
}
