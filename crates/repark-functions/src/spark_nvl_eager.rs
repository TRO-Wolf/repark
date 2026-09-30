use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, UInt32Array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::compute::kernels::nullif::nullif as arrow_nullif;
use datafusion::arrow::compute::{CastOptions, cast_with_options, concat, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::ansi::spark_ansi_enabled_from_options;
use crate::cast_map::{spark_cast_ansi_zoned, spark_cast_legacy_zoned};
use crate::session_time_zone::session_time_zone_from_options;
use crate::spark_nvl::wrong_num_args;

#[must_use]
pub fn nvl_pick_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(NvlPick::new()))
}

#[must_use]
pub fn nullif_compare_udf(common: &DataType) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(NullifCompare::new(common.clone())))
}

#[must_use]
pub fn nvl_pick_expr(first: Expr, second: Expr, first_nullable: bool) -> Expr {
    let trust_first = first_nullability_trusted(&first);
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(NvlPick::with_trust(
            trust_first,
            first_nullable,
        ))),
        vec![first, second],
    ))
}

fn first_nullability_trusted(first: &Expr) -> bool {
    !first
        .exists(|node| {
            Ok(matches!(node, Expr::Case(_))
                || matches!(node, Expr::ScalarFunction(call) if call.func.name().eq_ignore_ascii_case("if")))
        })
        .unwrap_or(true)
}

#[must_use]
pub fn nullif_compare_expr(first: Expr, second: Expr, common: &DataType) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        nullif_compare_udf(common),
        vec![first, second],
    ))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![nvl_pick_udf()]
}

#[must_use]
pub fn kernel_covers_scalar(common: &DataType) -> bool {
    !matches!(
        common,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

fn materialize(values: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    let mut width = 0usize;
    let mut saw_array = false;
    for value in values {
        if let ColumnarValue::Array(array) = value {
            saw_array = true;
            width = width.max(array.len());
        }
    }
    if !saw_array {
        width = 1;
    }
    let mut arrays = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ColumnarValue::Scalar(scalar) => arrays.push(scalar.to_array_of_size(width)?),
            ColumnarValue::Array(array) => {
                if array.len() != width {
                    return exec_err!(
                        "nvl eager got mismatched argument widths {} and {width}",
                        array.len()
                    );
                }
                arrays.push(Arc::clone(array));
            }
        }
    }
    Ok(arrays)
}

fn pick_rows(first: &ArrayRef, second: &ArrayRef, use_first: &[bool]) -> Result<ArrayRef> {
    let width = first.len();
    let combined = concat(&[first.as_ref(), second.as_ref()])?;
    let mut indices = Vec::with_capacity(width);
    for (row, take_first) in use_first.iter().enumerate() {
        let index = if *take_first { row } else { row + width };
        let index = u32::try_from(index).map_err(|_| {
            datafusion::common::DataFusionError::Execution("row index overflow".to_owned())
        })?;
        indices.push(index);
    }
    let indices = UInt32Array::from(indices);
    Ok(take(&combined, &indices, None)?)
}

fn first_present(first: &ArrayRef, second: &ArrayRef) -> Result<ArrayRef> {
    let mask: Vec<bool> = (0..first.len())
        .map(|row| !matches!(first.data_type(), DataType::Null) && !first.is_null(row))
        .collect();
    pick_rows(first, second, &mask)
}

fn needs_spark_shaped_cast(from_type: &DataType, common: &DataType) -> bool {
    matches!(
        from_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
    ) && matches!(common, DataType::Timestamp(_, _))
}

fn is_string_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn cast_to_common(
    array: &ArrayRef,
    common: &DataType,
    zone: &str,
    ansi: bool,
    now: DateTime<Utc>,
) -> Result<ArrayRef> {
    if array.data_type() == common {
        return Ok(Arc::clone(array));
    }
    if let DataType::Dictionary(_, values) = array.data_type()
        && is_string_source(values)
    {
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        let unpacked = cast_with_options(array.as_ref(), values.as_ref(), &options)?;
        return cast_to_common(&unpacked, common, zone, ansi, now);
    }
    if is_string_source(array.data_type()) {
        if ansi {
            return spark_cast_ansi_zoned(array, common, zone, now);
        }
        return spark_cast_legacy_zoned(array, common, zone, now);
    }
    if needs_spark_shaped_cast(array.data_type(), common) {
        return spark_cast_ansi_zoned(array, common, zone, now);
    }
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    Ok(cast_with_options(array.as_ref(), common, &options)?)
}

fn compare_array(
    original: &ColumnarValue,
    materialized: &ArrayRef,
    common: &DataType,
    zone: &str,
    ansi: bool,
    now: DateTime<Utc>,
) -> Result<ArrayRef> {
    let ColumnarValue::Scalar(scalar) = original else {
        return cast_to_common(materialized, common, zone, ansi, now);
    };
    if scalar.data_type() == *common {
        return Ok(Arc::clone(materialized));
    }
    let one = scalar.to_array_of_size(1)?;
    let casted = cast_to_common(&one, common, zone, ansi, now)?;
    match ScalarValue::try_from_array(&casted, 0) {
        Ok(cast_scalar) => Ok(cast_scalar.to_array_of_size(materialized.len())?),
        Err(_) => cast_to_common(materialized, common, zone, ansi, now),
    }
}

#[allow(
    clippy::float_cmp,
    reason = "Spark nullif compares floats with == plus NaN equality"
)]
fn float_nullif_match(
    left: &ArrayRef,
    right: &ArrayRef,
    common: &DataType,
) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return exec_err!(
            "float nullif got mismatched argument widths {} and {}",
            left.len(),
            right.len()
        );
    }
    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    if common == &DataType::Float32 {
        let left = left.as_primitive::<Float32Type>();
        let right = right.as_primitive::<Float32Type>();
        return Ok(zipped_float_match(
            left.values(),
            right.values(),
            nulls,
            |first, second| first == second || (first.is_nan() && second.is_nan()),
        ));
    }
    let left = left.as_primitive::<Float64Type>();
    let right = right.as_primitive::<Float64Type>();
    Ok(zipped_float_match(
        left.values(),
        right.values(),
        nulls,
        |first, second| first == second || (first.is_nan() && second.is_nan()),
    ))
}

fn zipped_float_match<T: Copy>(
    left: &[T],
    right: &[T],
    nulls: Option<NullBuffer>,
    equal: impl Fn(T, T) -> bool,
) -> BooleanArray {
    let mut values = BooleanBufferBuilder::new(left.len());
    for (first, second) in left.iter().zip(right.iter()) {
        values.append(equal(*first, *second));
    }
    BooleanArray::new(values.finish(), nulls)
}

#[derive(Debug)]
struct NvlPick {
    signature: Signature,
    trust_first: bool,
    first_nullable: bool,
}

impl NvlPick {
    fn new() -> Self {
        Self::with_trust(false, true)
    }

    fn with_trust(trust_first: bool, first_nullable: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            trust_first,
            first_nullable,
        }
    }
}

impl PartialEq for NvlPick {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NvlPick {}

impl Hash for NvlPick {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for NvlPick {
    crate::shim_udf_boilerplate!("__repark_nvl_pick");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [first, _] = arg_types else {
            return Err(wrong_num_args("__repark_nvl_pick", 2, arg_types.len()));
        };
        Ok(first.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [first, second] = args.arg_fields else {
            return Err(wrong_num_args(
                "__repark_nvl_pick",
                2,
                args.arg_fields.len(),
            ));
        };
        let nullable = if self.trust_first {
            self.first_nullable && second.is_nullable()
        } else {
            second.is_nullable()
        };
        Ok(Arc::new(Field::new(
            "__repark_nvl_pick",
            first.data_type().clone(),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(wrong_num_args("__repark_nvl_pick", 2, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = materialize(&args.args)?;
        let [first, second] = arrays.as_slice() else {
            return Err(wrong_num_args("__repark_nvl_pick", 2, arrays.len()));
        };
        Ok(ColumnarValue::Array(first_present(first, second)?))
    }
}

#[derive(Debug)]
struct NullifCompare {
    common: DataType,
    signature: Signature,
}

impl NullifCompare {
    fn new(common: DataType) -> Self {
        Self {
            common,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for NullifCompare {
    fn eq(&self, other: &Self) -> bool {
        self.common == other.common
    }
}

impl Eq for NullifCompare {}

impl Hash for NullifCompare {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.common.hash(state);
    }
}

impl ScalarUDFImpl for NullifCompare {
    crate::shim_udf_boilerplate!("__repark_nullif_compare");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [first, _] = arg_types else {
            return Err(wrong_num_args(
                "__repark_nullif_compare",
                2,
                arg_types.len(),
            ));
        };
        Ok(first.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [first, _] = args.arg_fields else {
            return Err(wrong_num_args(
                "__repark_nullif_compare",
                2,
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            "__repark_nullif_compare",
            first.data_type().clone(),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(wrong_num_args(
                "__repark_nullif_compare",
                2,
                arg_types.len(),
            ));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone = session_time_zone_from_options(args.config_options.as_ref());
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let now = Utc::now();
        let arrays = materialize(&args.args)?;
        let [first, second] = arrays.as_slice() else {
            return Err(wrong_num_args("__repark_nullif_compare", 2, arrays.len()));
        };
        if matches!(first.data_type(), DataType::Null)
            || matches!(second.data_type(), DataType::Null)
        {
            return Ok(ColumnarValue::Array(Arc::clone(first)));
        }
        let [first_value, second_value] = args.args.as_slice() else {
            return Err(wrong_num_args(
                "__repark_nullif_compare",
                2,
                args.args.len(),
            ));
        };
        let left = compare_array(first_value, first, &self.common, zone, ansi, now)?;
        let right = compare_array(second_value, second, &self.common, zone, ansi, now)?;
        let compared: BooleanArray = if matches!(self.common, DataType::Float32 | DataType::Float64)
        {
            float_nullif_match(&left, &right, &self.common)?
        } else {
            eq(&left, &right)?
        };
        Ok(ColumnarValue::Array(arrow_nullif(
            first.as_ref(),
            &compared,
        )?))
    }
}
