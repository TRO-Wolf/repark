use datafusion::arrow::array::{Array, Float32Array, Float64Array, StringArray, StringBuilder};

pub(crate) use repark_common::java_double::with_java_double_text;
pub use repark_common::java_double::{
    java_double_text, java_double_text_len, java_float_text, java_float_text_len,
    with_java_float_text,
};

pub(crate) fn java_double_strings(values: &Float64Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 24);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            with_java_double_text(values.value(index), |text| builder.append_value(text));
        }
    }
    builder.finish()
}

pub(crate) fn java_float_strings(values: &Float32Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 24);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            with_java_float_text(values.value(index), |text| builder.append_value(text));
        }
    }
    builder.finish()
}
