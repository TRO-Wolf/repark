use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, MapArray, MutableArrayData, make_array};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

fn plan_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

fn exec_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn list_element_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            Some(child)
        }
        _ => None,
    }
}

fn spark_list(element: &FieldRef) -> DataType {
    DataType::List(Arc::new(Field::new(
        "element",
        element.data_type().clone(),
        element.is_nullable(),
    )))
}

fn spark_array_element(arg_types: &[DataType]) -> Result<DataType> {
    match crate::collection::coerce::spark_common_element(arg_types) {
        Some(data_type) => Ok(data_type),
        None if arg_types.iter().all(|t| t == &DataType::Null) => Ok(DataType::Null),
        None => Err(plan_error(format!(
            "[DATATYPE_MISMATCH.CREATE_ARRAY] Cannot resolve \"array\" due to data type \
             mismatch: input to function `array` should have been a common data type family, \
             but got {arg_types:?}. SQLSTATE: 42K09"
        ))),
    }
}

fn return_list_element(args: &ScalarFunctionArgs) -> Result<FieldRef> {
    list_element_field(args.return_field.data_type())
        .cloned()
        .ok_or_else(|| plan_error("collection kernel expected a list return field"))
}

fn to_list_i32(array: &ArrayRef) -> Result<ListArray> {
    match array.data_type() {
        DataType::List(_) => Ok(array.as_list::<i32>().clone()),
        DataType::LargeList(child) => {
            let casted = cast(array.as_ref(), &DataType::List(Arc::clone(child)))?;
            casted
                .as_any()
                .downcast_ref::<ListArray>()
                .cloned()
                .ok_or_else(|| exec_error("large-list to list cast failed"))
        }
        other => Err(exec_error(format!("expected a list input, got {other}"))),
    }
}

fn build_list(
    element: FieldRef,
    mutable: MutableArrayData<'_>,
    offsets: Vec<i32>,
    validity: Vec<bool>,
) -> ArrayRef {
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = validity
        .iter()
        .any(|flag| !flag)
        .then(|| NullBuffer::new(BooleanBuffer::from(validity)));
    Arc::new(ListArray::new(
        element,
        offsets,
        make_array(mutable.freeze()),
        nulls,
    ))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkMakeArray {
    signature: Signature,
}

impl ScalarUDFImpl for SparkMakeArray {
    fn name(&self) -> &'static str {
        "make_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element = spark_array_element(arg_types)?;
        Ok(DataType::List(Arc::new(Field::new(
            "element",
            element,
            arg_types.iter().any(|t| t == &DataType::Null),
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let element = spark_array_element(&arg_types)?;
        let contains_null = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new("element", element, contains_null))),
            false,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let element = return_list_element(&args)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.is_empty() {
            let list = ListArray::new(
                element,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32; args.number_rows + 1])),
                arrow::array::new_empty_array(&DataType::Null),
                None,
            );
            return Ok(ColumnarValue::Array(Arc::new(list)));
        }
        let shaped: Vec<ArrayRef> = arrays
            .iter()
            .map(|array| {
                if array.data_type() == element.data_type() {
                    Ok(Arc::clone(array))
                } else {
                    cast(array.as_ref(), element.data_type()).map_err(DataFusionError::from)
                }
            })
            .collect::<Result<_>>()?;
        let row_count = shaped.first().map_or(0, Array::len);
        let child_data: Vec<_> = shaped.iter().map(arrow::array::Array::to_data).collect();
        let child_refs: Vec<_> = child_data.iter().collect();
        let capacity: usize = child_data.iter().map(arrow::array::ArrayData::len).sum();
        let mut mutable = MutableArrayData::new(child_refs, true, capacity);
        let mut offsets = Vec::with_capacity(row_count + 1);
        offsets.push(0i32);
        for row in 0..row_count {
            for index in 0..child_data.len() {
                mutable.extend(index, row, row + 1);
            }
            offsets.push(
                i32::try_from(mutable.len())
                    .map_err(|_| exec_error("make_array element count does not fit i32"))?,
            );
        }
        let list = build_list(element, mutable, offsets, Vec::new());
        Ok(ColumnarValue::Array(list))
    }
}

#[must_use]
pub fn make_array_udf() -> Arc<ScalarUDF> {
    Arc::new(
        ScalarUDF::from(SparkMakeArray {
            signature: Signature::user_defined(Volatility::Immutable),
        })
        .with_aliases(["array"]),
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkArrayElement {
    signature: Signature,
}

impl ScalarUDFImpl for SparkArrayElement {
    fn name(&self) -> &'static str {
        "array_element"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let _ = arg_types;
        Err(plan_error(
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `array_element`.",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let _ = args;
        Err(plan_error(
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `array_element`.",
        ))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let _ = arg_types;
        Err(plan_error(
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `array_element`.",
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let _ = args;
        Err(exec_error(
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `array_element`.",
        ))
    }
}

#[must_use]
pub fn array_element_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayElement {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkSlice {
    signature: Signature,
}

fn slice_param_error(param: &str, message: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[INVALID_PARAMETER_VALUE.{param}] The value of parameter(s) `{param}` in `slice` is \
         invalid: {message} SQLSTATE: 22023"
    ))
}

impl ScalarUDFImpl for SparkSlice {
    fn name(&self) -> &'static str {
        "slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| plan_error("slice expects 3 arguments"))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let array = args
            .arg_fields
            .first()
            .ok_or_else(|| plan_error("slice expects an array first argument"))?;
        let Some(element) = list_element_field(array.data_type()) else {
            return Err(plan_error(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Parameter 1 requires the \
                 (ARRAY OF T) type, however `slice` has the input type {}.",
                array.data_type()
            )));
        };
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            spark_list(element),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return Err(plan_error(format!(
                "'slice' expects 3 arguments, got {}",
                arg_types.len()
            )));
        }
        Ok(vec![arg_types[0].clone(), DataType::Int64, DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let element = return_list_element(&args)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [array_arg, start_arg, length_arg] = <[ArrayRef; 3]>::try_from(arrays)
            .map_err(|_| plan_error("slice expects 3 arguments"))?;
        let array = to_list_i32(&array_arg)?;
        let starts = cast(start_arg.as_ref(), &DataType::Int64)?;
        let starts = starts.as_primitive::<Int64Type>();
        let lengths = cast(length_arg.as_ref(), &DataType::Int64)?;
        let lengths = lengths.as_primitive::<Int64Type>();
        let element_data = array.values().to_data();
        let capacity: usize = (0..array.len())
            .map(|row| {
                if array.is_null(row) || starts.is_null(row) || lengths.is_null(row) {
                    0usize
                } else {
                    let length = lengths.value(row).max(0);
                    usize::try_from(length).unwrap_or(usize::MAX)
                }
            })
            .sum::<usize>()
            .min(element_data.len());
        let mut mutable = MutableArrayData::new(vec![&element_data], true, capacity);
        let mut offsets = Vec::with_capacity(array.len() + 1);
        offsets.push(0i32);
        let mut validity = Vec::with_capacity(array.len());
        let total = i64::try_from(element_data.len())
            .map_err(|_| exec_error("slice element count does not fit i64"))?;
        for row in 0..array.len() {
            if array.is_null(row) || starts.is_null(row) || lengths.is_null(row) {
                validity.push(false);
            } else {
                let start = starts.value(row);
                let length = lengths.value(row);
                if start == 0 {
                    return Err(slice_param_error(
                        "START",
                        "Expects a positive or a negative value for `start`, but got 0.",
                    ));
                }
                if length < 0 {
                    return Err(slice_param_error(
                        "LENGTH",
                        &format!("Expects `length` greater than or equal to 0, but got {length}."),
                    ));
                }
                let first = if start > 0 { start - 1 } else { total + start };
                let first = first.max(0).min(total);
                let last = first.saturating_add(length).min(total);
                if last > first {
                    mutable.extend(
                        0,
                        usize::try_from(first).unwrap_or(0),
                        usize::try_from(last).unwrap_or(0),
                    );
                }
                validity.push(true);
            }
            offsets.push(
                i32::try_from(mutable.len())
                    .map_err(|_| exec_error("slice element count does not fit i32"))?,
            );
        }
        let list = build_list(element, mutable, offsets, validity);
        Ok(ColumnarValue::Array(list))
    }
}

#[must_use]
pub fn slice_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSlice {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkArrayRepeat {
    signature: Signature,
}

impl ScalarUDFImpl for SparkArrayRepeat {
    fn name(&self) -> &'static str {
        "array_repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element = arg_types.first().cloned().unwrap_or(DataType::Null);
        Ok(DataType::List(Arc::new(Field::new_list_field(
            element, true,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let (element_type, element_nullable) = args.arg_fields.first().map_or_else(
            || (DataType::Null, true),
            |field| (field.data_type().clone(), field.is_nullable()),
        );
        let element = Arc::new(Field::new("element", element_type, element_nullable));
        let nullable = args
            .arg_fields
            .get(1)
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(element),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(plan_error(format!(
                "'array_repeat' expects 2 arguments, got {}",
                arg_types.len()
            )));
        }
        Ok(vec![arg_types[0].clone(), DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let element = return_list_element(&args)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [value_arg, count_arg] = <[ArrayRef; 2]>::try_from(arrays)
            .map_err(|_| plan_error("array_repeat expects 2 arguments"))?;
        let value: ArrayRef = if value_arg.data_type() == element.data_type() {
            value_arg
        } else {
            cast(value_arg.as_ref(), element.data_type())?
        };
        let counts = cast(count_arg.as_ref(), &DataType::Int64)?;
        let counts = counts.as_primitive::<Int64Type>();
        let value_data = value.to_data();
        let capacity: usize = (0..value.len())
            .map(|row| {
                if counts.is_null(row) {
                    0usize
                } else {
                    usize::try_from(counts.value(row).max(0)).unwrap_or(usize::MAX)
                }
            })
            .sum::<usize>()
            .min(value_data.len().saturating_mul(value.len()).max(1024));
        let mut mutable = MutableArrayData::new(vec![&value_data], true, capacity);
        let mut offsets = Vec::with_capacity(value.len() + 1);
        offsets.push(0i32);
        let mut validity = Vec::with_capacity(value.len());
        for row in 0..value.len() {
            if counts.is_null(row) {
                validity.push(false);
            } else {
                let count = counts.value(row);
                if count > i64::from(i32::MAX) {
                    return Err(exec_error(format!(
                        "[INVALID_PARAMETER_VALUE.LENGTH] The value of parameter(s) `count` in \
                         `array_repeat` is invalid: cannot create an array with {count} \
                         elements."
                    )));
                }
                for _ in 0..count.max(0) {
                    mutable.extend(0, row, row + 1);
                }
                validity.push(true);
            }
            offsets.push(
                i32::try_from(mutable.len())
                    .map_err(|_| exec_error("array_repeat element count does not fit i32"))?,
            );
        }
        let list = build_list(element, mutable, offsets, validity);
        Ok(ColumnarValue::Array(list))
    }
}

#[must_use]
pub fn array_repeat_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayRepeat {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkMapSide {
    signature: Signature,
    keys: bool,
}

fn map_entry_fields(data_type: &DataType) -> Option<(&FieldRef, &FieldRef)> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    match (fields.first(), fields.get(1)) {
        (Some(key), Some(value)) => Some((key, value)),
        _ => None,
    }
}

impl ScalarUDFImpl for SparkMapSide {
    fn name(&self) -> &'static str {
        if self.keys { "map_keys" } else { "map_values" }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let side = arg_types
            .first()
            .and_then(|data_type| map_entry_fields(data_type))
            .ok_or_else(|| plan_error("map_keys/map_values expect a map argument"))?;
        let data_type = if self.keys {
            side.0.data_type().clone()
        } else {
            side.1.data_type().clone()
        };
        Ok(DataType::List(Arc::new(Field::new_list_field(
            data_type, true,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let map = args
            .arg_fields
            .first()
            .ok_or_else(|| plan_error(format!("'{}' expects a map argument", self.name())))?;
        let (key, value) = map_entry_fields(map.data_type()).ok_or_else(|| {
            plan_error(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Parameter 1 requires the MAP type, \
                 however `{}` has the input type {}.",
                self.name(),
                map.data_type()
            ))
        })?;
        let data_type = if self.keys {
            key.data_type().clone()
        } else {
            value.data_type().clone()
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new("element", data_type, true))),
            map.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let element = return_list_element(&args)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [map_arg] = <[ArrayRef; 1]>::try_from(arrays)
            .map_err(|_| plan_error("map_keys/map_values expect 1 argument"))?;
        let map = map_arg
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| plan_error("map_keys/map_values expect a map argument"))?;
        let values = if self.keys {
            Arc::clone(map.keys())
        } else {
            Arc::clone(map.values())
        };
        let list = ListArray::new(element, map.offsets().clone(), values, map.nulls().cloned());
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

#[must_use]
pub fn map_keys_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkMapSide {
        signature: Signature::user_defined(Volatility::Immutable),
        keys: true,
    }))
}

#[must_use]
pub fn map_values_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkMapSide {
        signature: Signature::user_defined(Volatility::Immutable),
        keys: false,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SparkListShape {
    Preserve,
    DropElementNulls,
    LeftWins,
    Union,
}

#[derive(Debug)]
struct SparkListOp {
    name: &'static str,
    inner: Arc<ScalarUDF>,
    signature: Signature,
    shape: SparkListShape,
}

impl PartialEq for SparkListOp {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkListOp {}

impl std::hash::Hash for SparkListOp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

fn cast_list_args(args: &mut ScalarFunctionArgs, targets: &[DataType]) -> Result<()> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let mut casted = Vec::with_capacity(arrays.len());
    for (index, array) in arrays.into_iter().enumerate() {
        match targets.get(index) {
            Some(target) if array.data_type() != target => {
                casted.push(cast(array.as_ref(), target)?);
            }
            _ => casted.push(array),
        }
    }
    args.args = casted.into_iter().map(ColumnarValue::Array).collect();
    for (index, field) in args.arg_fields.iter_mut().enumerate() {
        if let Some(target) = targets.get(index) {
            *field = Arc::new(Field::new(
                field.name(),
                target.clone(),
                field.is_nullable(),
            ));
        }
    }
    Ok(())
}

impl ScalarUDFImpl for SparkListOp {
    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let first = arg_types
            .first()
            .ok_or_else(|| plan_error(format!("'{}' expects an array argument", self.name)))?;
        match self.shape {
            SparkListShape::Preserve | SparkListShape::LeftWins => Ok(first.clone()),
            SparkListShape::DropElementNulls => {
                let element = list_element_field(first).ok_or_else(|| {
                    plan_error(format!(
                        "'{}' expects an array argument, got {first}",
                        self.name
                    ))
                })?;
                Ok(DataType::List(Arc::new(Field::new(
                    "element",
                    element.data_type().clone(),
                    false,
                ))))
            }
            SparkListShape::Union => {
                let left = list_element_field(first).ok_or_else(|| {
                    plan_error(format!(
                        "'{}' expects array arguments, got {first}",
                        self.name
                    ))
                })?;
                let right = list_element_field(arg_types.get(1).ok_or_else(|| {
                    plan_error(format!("'{}' expects two array arguments", self.name))
                })?)
                .ok_or_else(|| plan_error(format!("'{}' expects array arguments", self.name)))?;
                let common = crate::collection::coerce::spark_common_element(&[
                    left.data_type().clone(),
                    right.data_type().clone(),
                ])
                .ok_or_else(|| {
                    plan_error(format!(
                        "[DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES] The `{}` function requires \
                         ARRAY input with the same element type. SQLSTATE: 42K09",
                        self.name
                    ))
                })?;
                Ok(DataType::List(Arc::new(Field::new(
                    "element",
                    common,
                    left.is_nullable() || right.is_nullable(),
                ))))
            }
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let data_type = self.return_type(&arg_types)?;
        let nullable = match self.shape {
            SparkListShape::Preserve | SparkListShape::DropElementNulls => args
                .arg_fields
                .first()
                .is_some_and(|field| field.is_nullable()),
            SparkListShape::LeftWins | SparkListShape::Union => {
                args.arg_fields.iter().any(|field| field.is_nullable())
            }
        };
        Ok(Arc::new(Field::new(self.name, data_type, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match self.shape {
            SparkListShape::Union => {
                let element = return_list_element(&args)?;
                let target = DataType::List(element);
                cast_list_args(&mut args, &[target.clone(), target])?;
            }
            SparkListShape::LeftWins => {
                let targets = args.args.first().and_then(|array| {
                    let array_type = array.data_type();
                    list_element_field(&array_type)
                        .map(|element| vec![array_type.clone(), element.data_type().clone()])
                });
                if let Some(targets) = targets {
                    cast_list_args(&mut args, &targets)?;
                }
            }
            SparkListShape::Preserve | SparkListShape::DropElementNulls => {}
        }
        let expected = args.return_field.data_type().clone();
        let out = self.inner.inner().invoke_with_args(args)?;
        match out {
            ColumnarValue::Array(array) if array.data_type() != &expected => {
                Ok(ColumnarValue::Array(cast(array.as_ref(), &expected)?))
            }
            ColumnarValue::Scalar(scalar) if scalar.data_type() != expected => {
                Ok(ColumnarValue::Scalar(scalar.cast_to(&expected)?))
            }
            other => Ok(other),
        }
    }
}

fn spark_list_op(
    name: &'static str,
    inner: Arc<ScalarUDF>,
    shape: SparkListShape,
) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkListOp {
        name,
        inner,
        signature: Signature::user_defined(Volatility::Immutable),
        shape,
    }))
}

#[must_use]
pub fn array_distinct_udf() -> Arc<ScalarUDF> {
    spark_list_op(
        "array_distinct",
        datafusion::functions_nested::set_ops::array_distinct_udf(),
        SparkListShape::Preserve,
    )
}

#[must_use]
pub fn array_compact_udf() -> Arc<ScalarUDF> {
    spark_list_op(
        "array_compact",
        datafusion::functions_nested::array_compact::array_compact_udf(),
        SparkListShape::DropElementNulls,
    )
}

#[must_use]
pub fn array_remove_udf() -> Arc<ScalarUDF> {
    spark_list_op(
        "array_remove",
        datafusion::functions_nested::remove::array_remove_udf(),
        SparkListShape::LeftWins,
    )
}

#[must_use]
pub fn array_union_udf() -> Arc<ScalarUDF> {
    spark_list_op(
        "array_union",
        datafusion::functions_nested::set_ops::array_union_udf(),
        SparkListShape::Union,
    )
}
