use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, ListArray, TimestampMicrosecondArray,
};
use datafusion::common::ScalarValue;

use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, IntervalMonthDayNano, IntervalUnit, TimeUnit,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::session_time_zone::session_time_zone_from_options;

mod rows;

#[must_use]
pub fn sequence_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSequence::new()))
}

#[derive(Debug)]
struct SparkSequence {
    signature: Signature,
}

impl SparkSequence {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSequence {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSequence {}

impl Hash for SparkSequence {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Int,
    Date,
    Timestamp,
}

fn family_of(data_type: &DataType) -> Option<Family> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Some(Family::Int),
        DataType::Date32 | DataType::Date64 => Some(Family::Date),
        DataType::Timestamp(_, _) => Some(Family::Timestamp),
        _ => None,
    }
}

fn int_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn wrong_input_types(arg_types: &[DataType]) -> DataFusionError {
    let rendered: Vec<String> = arg_types
        .iter()
        .map(crate::collection::spark_type_name)
        .collect();
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES] Cannot resolve \"sequence({})\" due to \
         data type mismatch: `sequence` uses the wrong parameter type. The parameter type must \
         conform to:",
        rendered.join(", ")
    ))
}

fn month_day_nano() -> DataType {
    DataType::Interval(IntervalUnit::MonthDayNano)
}

fn plan_sequence(arg_types: &[DataType]) -> Result<Family> {
    if arg_types.len() != 2 && arg_types.len() != 3 {
        return Err(DataFusionError::Plan(format!(
            "'sequence' expects (start, stop[, step]), got {} argument(s)",
            arg_types.len()
        )));
    }
    let present: Vec<&DataType> = arg_types
        .iter()
        .filter(|arg_type| **arg_type != DataType::Null)
        .collect();
    if present.iter().any(|arg_type| int_rank(arg_type).is_some())
        && present
            .iter()
            .all(|arg_type| int_rank(arg_type).is_some() || is_string_type(arg_type))
    {
        return Ok(Family::Int);
    }
    if let Some(family) = temporal_string_family(&arg_types[..2]) {
        let step_type = arg_types.get(2).unwrap_or(&DataType::Null);
        if *step_type != DataType::Null && *step_type != month_day_nano() {
            return Err(wrong_input_types(arg_types));
        }
        return Ok(family);
    }
    let start_family = if arg_types[0] == DataType::Null {
        None
    } else {
        Some(family_of(&arg_types[0]).ok_or_else(|| wrong_input_types(arg_types))?)
    };
    let stop_family = if arg_types[1] == DataType::Null {
        None
    } else {
        Some(family_of(&arg_types[1]).ok_or_else(|| wrong_input_types(arg_types))?)
    };
    let family = match (start_family, stop_family) {
        (Some(first), Some(second)) if first == second => first,
        (Some(first), None) | (None, Some(first)) => first,
        _ => return Err(wrong_input_types(arg_types)),
    };
    let step_type = arg_types.get(2).unwrap_or(&DataType::Null);
    match family {
        Family::Int => {
            if *step_type != DataType::Null && family_of(step_type) != Some(Family::Int) {
                return Err(wrong_input_types(arg_types));
            }
            for arg_type in arg_types {
                if *arg_type != DataType::Null && int_rank(arg_type).is_none() {
                    return Err(wrong_input_types(arg_types));
                }
            }
            Ok(family)
        }
        Family::Date | Family::Timestamp => {
            if *step_type != DataType::Null && *step_type != month_day_nano() {
                return Err(wrong_input_types(arg_types));
            }
            Ok(family)
        }
    }
}

fn temporal_string_family(bounds: &[DataType]) -> Option<Family> {
    let [start, stop] = bounds else {
        return None;
    };
    let sibling = if is_string_type(start) && !is_string_type(stop) {
        family_of(stop)
    } else if is_string_type(stop) && !is_string_type(start) {
        family_of(start)
    } else {
        return None;
    };
    match sibling {
        Some(Family::Date | Family::Timestamp) => sibling,
        _ => None,
    }
}

fn sequence_element(family: Family, arg_types: &[DataType]) -> DataType {
    match family {
        Family::Int => {
            if arg_types.iter().any(is_string_type) {
                return DataType::Int64;
            }
            let mut widest = DataType::Int32;
            let mut rank: Option<u8> = None;
            for arg_type in arg_types {
                if let Some(next) = int_rank(arg_type)
                    && rank.is_none_or(|current| next > current)
                {
                    rank = Some(next);
                    widest = arg_type.clone();
                }
            }
            widest
        }
        Family::Date => DataType::Date32,
        Family::Timestamp => {
            if arg_types[0] == DataType::Null || is_string_type(&arg_types[0]) {
                arg_types[1].clone()
            } else {
                arg_types[0].clone()
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct SequenceStringBounds;

impl AnalyzerRule for SequenceStringBounds {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if crate::ansi::spark_ansi_enabled_from_options(config) {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(check_plan_node).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "sequence_string_bounds"
    }
}

fn check_plan_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    plan.map_expressions(|expr| {
        expr.transform_up(|node| {
            if let Expr::ScalarFunction(function) = &node
                && function.func.name() == "sequence"
            {
                refuse_string_bounds(&function.args, &schema)?;
            }
            Ok(Transformed::no(node))
        })
    })
}

fn refuse_string_bounds(args: &[Expr], schema: &DFSchema) -> Result<()> {
    let mut arg_types = Vec::with_capacity(args.len());
    for arg in args {
        let Ok(arg_type) = arg.get_type(schema) else {
            return Ok(());
        };
        arg_types.push(arg_type);
    }
    if arg_types.iter().any(is_string_type) {
        return Err(wrong_input_types(&arg_types));
    }
    Ok(())
}

#[must_use]
pub fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![Arc::new(SequenceStringBounds)]
}

impl ScalarUDFImpl for SparkSequence {
    crate::shim_udf_boilerplate!("sequence");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let family = plan_sequence(arg_types)?;
        Ok(DataType::List(Arc::new(Field::new(
            "element",
            sequence_element(family, arg_types),
            false,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable())
            || declared.iter().any(is_string_type);
        Ok(Arc::new(Field::new(
            "sequence",
            self.return_type(&declared)?,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_sequence(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            return_field,
            config_options,
            ..
        } = args;
        let DataType::List(element) = return_field.data_type() else {
            return exec_err!("sequence needs a list return");
        };
        let zone = session_time_zone_from_options(config_options.as_ref());
        let now = Utc::now();
        if arg_values
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)))
        {
            return match element.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    scalar_ints(element, &arg_values)
                }
                DataType::Date32 => scalar_dates(element, &arg_values, zone, now),
                DataType::Timestamp(_, _) => scalar_timestamps(element, &arg_values, zone, now),
                other => exec_err!("sequence cannot build {other} elements"),
            };
        }
        let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
        let row_count = arrays.first().map_or(0, Array::len);
        match element.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                invoke_ints(&arrays, row_count, element)
            }
            DataType::Date32 => invoke_dates(&arrays, row_count, element, zone, now),
            DataType::Timestamp(_, _) => invoke_timestamps(&arrays, row_count, element, zone, now),
            other => exec_err!("sequence cannot build {other} elements"),
        }
    }
}

fn finish_one(element: &FieldRef, values: ArrayRef, valid: bool) -> Result<ColumnarValue> {
    let end = fit_i32(values.len())?;
    finish_list(element, vec![0, end], vec![valid], !valid, values)
}

fn scalar_int(value: &ColumnarValue) -> Result<Option<i64>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        ColumnarValue::Scalar(scalar) => {
            let single = as_i64(&scalar.to_array_of_size(1)?)?;
            Ok(if single.is_null(0) {
                None
            } else {
                Some(single.value(0))
            })
        }
        ColumnarValue::Array(_) => exec_err!("sequence needs scalar bounds on the scalar path"),
    }
}

fn scalar_days(value: &ColumnarValue, zone: &str, now: DateTime<Utc>) -> Result<Option<i32>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        ColumnarValue::Scalar(scalar) => {
            let single = as_date_days(&scalar.to_array_of_size(1)?, zone, now)?;
            Ok(if single.is_null(0) {
                None
            } else {
                Some(single.value(0))
            })
        }
        ColumnarValue::Array(_) => exec_err!("sequence needs scalar bounds on the scalar path"),
    }
}

fn scalar_interval(value: &ColumnarValue) -> Result<Option<IntervalMonthDayNano>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(v)) => Ok(*v),
        _ => exec_err!("sequence needs a month-day-nano step on the scalar path"),
    }
}

fn scalar_micros(
    value: &ColumnarValue,
    target: &DataType,
    zone: &str,
    now: DateTime<Utc>,
) -> Result<Option<i64>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        ColumnarValue::Scalar(scalar) => {
            let single = as_micros(&scalar.to_array_of_size(1)?, target, zone, now)?;
            Ok(if single.is_null(0) {
                None
            } else {
                Some(single.value(0))
            })
        }
        ColumnarValue::Array(_) => exec_err!("sequence needs scalar bounds on the scalar path"),
    }
}

fn scalar_ints(element: &FieldRef, args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let start = scalar_int(&args[0])?;
    let stop = scalar_int(&args[1])?;
    let stride = if args.len() > 2 {
        Some(scalar_int(&args[2])?)
    } else {
        None
    };
    match (start, stop, stride) {
        (Some(start), Some(stop), None) => {
            let shaped = shape_ints(element, rows::int_row(start, stop, None)?)?;
            finish_one(element, shaped, true)
        }
        (Some(start), Some(stop), Some(Some(given))) => {
            let shaped = shape_ints(element, rows::int_row(start, stop, Some(given))?)?;
            finish_one(element, shaped, true)
        }
        _ => {
            let shaped = shape_ints(element, Vec::new())?;
            finish_one(element, shaped, false)
        }
    }
}

fn scalar_dates(
    element: &FieldRef,
    args: &[ColumnarValue],
    zone: &str,
    now: DateTime<Utc>,
) -> Result<ColumnarValue> {
    let start = scalar_days(&args[0], zone, now)?;
    let stop = scalar_days(&args[1], zone, now)?;
    let stride = if args.len() > 2 {
        Some(scalar_interval(&args[2])?)
    } else {
        None
    };
    if let (Some(start), Some(stop), stride) = (start, stop, stride) {
        let explicit = match stride {
            None => None,
            Some(None) => {
                let shaped = shape_dates(Vec::new());
                return finish_one(element, shaped, false);
            }
            Some(Some(given)) => Some(given),
        };
        match rows::date_row(start, stop, explicit)? {
            None => {
                let shaped = shape_dates(Vec::new());
                finish_one(element, shaped, false)
            }
            Some(pieces) => {
                let shaped = shape_dates(pieces);
                finish_one(element, shaped, true)
            }
        }
    } else {
        let shaped = shape_dates(Vec::new());
        finish_one(element, shaped, false)
    }
}

fn scalar_timestamps(
    element: &FieldRef,
    args: &[ColumnarValue],
    zone: &str,
    now: DateTime<Utc>,
) -> Result<ColumnarValue> {
    let target = element.data_type();
    let start = scalar_micros(&args[0], target, zone, now)?;
    let stop = scalar_micros(&args[1], target, zone, now)?;
    let stride = if args.len() > 2 {
        Some(scalar_interval(&args[2])?)
    } else {
        None
    };
    if let (Some(start), Some(stop), stride) = (start, stop, stride) {
        let explicit = match stride {
            None => None,
            Some(None) => {
                let shaped = shape_timestamps(element, Vec::new());
                return finish_one(element, shaped, false);
            }
            Some(Some(given)) => Some(given),
        };
        match rows::timestamp_row(start, stop, explicit)? {
            None => {
                let shaped = shape_timestamps(element, Vec::new());
                finish_one(element, shaped, false)
            }
            Some(pieces) => {
                let shaped = shape_timestamps(element, pieces);
                finish_one(element, shaped, true)
            }
        }
    } else {
        let shaped = shape_timestamps(element, Vec::new());
        finish_one(element, shaped, false)
    }
}

fn shape_ints(element: &FieldRef, values: Vec<i64>) -> Result<ArrayRef> {
    let out_of_range = |value: i64| {
        DataFusionError::Execution(format!(
            "sequence value {value} does not fit {}",
            element.data_type()
        ))
    };
    match element.data_type() {
        DataType::Int8 => {
            let mut narrow = Vec::with_capacity(values.len());
            for value in values {
                narrow.push(i8::try_from(value).map_err(|_| out_of_range(value))?);
            }
            Ok(Arc::new(Int8Array::from(narrow)))
        }
        DataType::Int16 => {
            let mut narrow = Vec::with_capacity(values.len());
            for value in values {
                narrow.push(i16::try_from(value).map_err(|_| out_of_range(value))?);
            }
            Ok(Arc::new(Int16Array::from(narrow)))
        }
        DataType::Int32 => {
            let mut narrow = Vec::with_capacity(values.len());
            for value in values {
                narrow.push(i32::try_from(value).map_err(|_| out_of_range(value))?);
            }
            Ok(Arc::new(Int32Array::from(narrow)))
        }
        _ => Ok(Arc::new(Int64Array::from(values))),
    }
}

fn shape_dates(values: Vec<i32>) -> ArrayRef {
    Arc::new(Date32Array::from(values))
}

fn shape_timestamps(element: &FieldRef, values: Vec<i64>) -> ArrayRef {
    match element.data_type() {
        DataType::Timestamp(_, None) => Arc::new(TimestampMicrosecondArray::from(values)),
        _ => Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")),
    }
}

fn invoke_ints(arrays: &[ArrayRef], row_count: usize, element: &FieldRef) -> Result<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let starts = as_i64(&arrays[0])?;
    let stops = as_i64(&arrays[1])?;
    let strides = if arrays.len() > 2 {
        Some(as_i64(&arrays[2])?)
    } else {
        None
    };
    let mut values: Vec<i64> = Vec::new();
    for row in 0..row_count {
        let explicit = strides.as_ref().map(|strides| {
            if strides.is_null(row) {
                None
            } else {
                Some(strides.value(row))
            }
        });
        if starts.is_null(row) || stops.is_null(row) || explicit == Some(None) {
            validity.push(false);
            any_null = true;
        } else {
            let start = starts.value(row);
            let stop = stops.value(row);
            let stride = rows::resolve_int_stride(start, stop, explicit.flatten())?;
            rows::push_ints(&mut values, start, stop, stride)?;
            validity.push(true);
        }
        offsets.push(fit_i32(values.len())?);
    }
    let shaped = shape_ints(element, values)?;
    finish_list(element, offsets, validity, any_null, shaped)
}

fn invoke_dates(
    arrays: &[ArrayRef],
    row_count: usize,
    element: &FieldRef,
    zone: &str,
    now: DateTime<Utc>,
) -> Result<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let starts = as_date_days(&arrays[0], zone, now)?;
    let stops = as_date_days(&arrays[1], zone, now)?;
    let strides = if arrays.len() > 2 {
        Some(as_interval(&arrays[2])?)
    } else {
        None
    };
    let mut values: Vec<i32> = Vec::new();
    for row in 0..row_count {
        let step_null = strides.as_ref().is_some_and(|strides| strides.is_null(row));
        if starts.is_null(row) || stops.is_null(row) || step_null {
            validity.push(false);
            any_null = true;
        } else {
            let interval = strides.as_ref().map(|strides| strides.value(row));
            match rows::date_row(starts.value(row), stops.value(row), interval) {
                Err(error) => return Err(error),
                Ok(None) => {
                    validity.push(false);
                    any_null = true;
                }
                Ok(Some(pieces)) => {
                    validity.push(true);
                    values.extend(pieces);
                }
            }
        }
        offsets.push(fit_i32(values.len())?);
    }
    let shaped = shape_dates(values);
    finish_list(element, offsets, validity, any_null, shaped)
}

fn invoke_timestamps(
    arrays: &[ArrayRef],
    row_count: usize,
    element: &FieldRef,
    zone: &str,
    now: DateTime<Utc>,
) -> Result<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let target = element.data_type();
    let starts = as_micros(&arrays[0], target, zone, now)?;
    let stops = as_micros(&arrays[1], target, zone, now)?;
    let strides = if arrays.len() > 2 {
        Some(as_interval(&arrays[2])?)
    } else {
        None
    };
    let mut values: Vec<i64> = Vec::new();
    for row in 0..row_count {
        let step_null = strides.as_ref().is_some_and(|strides| strides.is_null(row));
        if starts.is_null(row) || stops.is_null(row) || step_null {
            validity.push(false);
            any_null = true;
        } else {
            let interval = strides.as_ref().map(|strides| strides.value(row));
            match rows::timestamp_row(starts.value(row), stops.value(row), interval) {
                Err(error) => return Err(error),
                Ok(None) => {
                    validity.push(false);
                    any_null = true;
                }
                Ok(Some(pieces)) => {
                    validity.push(true);
                    values.extend(pieces);
                }
            }
        }
        offsets.push(fit_i32(values.len())?);
    }
    let shaped = shape_timestamps(element, values);
    finish_list(element, offsets, validity, any_null, shaped)
}

fn downcast_primitive<T: datafusion::arrow::array::ArrowPrimitiveType>(
    array: &ArrayRef,
    what: &str,
) -> Result<datafusion::arrow::array::PrimitiveArray<T>> {
    array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::PrimitiveArray<T>>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution(format!("sequence {what} values unreadable")))
}

fn as_i64(array: &ArrayRef) -> Result<Int64Array> {
    if array.data_type() == &DataType::Int64 {
        return downcast_primitive(array, "int");
    }
    if is_string_type(array.data_type()) {
        let casted =
            crate::cast_map::spark_cast_ansi_zoned(array, &DataType::Int64, "UTC", Utc::now())?;
        return downcast_primitive(&casted, "int");
    }
    downcast_primitive(&cast(array.as_ref(), &DataType::Int64)?, "int")
}

fn as_date_days(array: &ArrayRef, zone: &str, now: DateTime<Utc>) -> Result<Date32Array> {
    if array.data_type() == &DataType::Date32 {
        return downcast_primitive(array, "date");
    }
    if is_string_type(array.data_type()) {
        let casted = crate::cast_map::spark_cast_ansi_zoned(array, &DataType::Date32, zone, now)?;
        return downcast_primitive(&casted, "date");
    }
    downcast_primitive(&cast(array.as_ref(), &DataType::Date32)?, "date")
}

fn microsecond_utc() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn timestamp_string_target(element: &DataType) -> DataType {
    match element {
        DataType::Timestamp(_, None) => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => microsecond_utc(),
    }
}

fn as_micros(
    array: &ArrayRef,
    target: &DataType,
    zone: &str,
    now: DateTime<Utc>,
) -> Result<TimestampMicrosecondArray> {
    if array.data_type() == &microsecond_utc() {
        return downcast_primitive(array, "timestamp");
    }
    if is_string_type(array.data_type()) {
        let casted = crate::cast_map::spark_cast_ansi_zoned(
            array,
            &timestamp_string_target(target),
            zone,
            now,
        )?;
        return downcast_primitive(&casted, "timestamp");
    }
    match array.data_type() {
        DataType::Timestamp(_, _) => {
            downcast_primitive(&cast(array.as_ref(), &microsecond_utc())?, "timestamp")
        }
        _ => Err(DataFusionError::Execution(
            "sequence timestamp arm needs timestamp values".to_owned(),
        )),
    }
}

fn as_interval(array: &ArrayRef) -> Result<IntervalMonthDayNanoArray> {
    downcast_primitive(array, "interval")
}

fn fit_i32(value: usize) -> Result<i32> {
    i32::try_from(value)
        .map_err(|_| DataFusionError::Execution("sequence row count does not fit i32".to_owned()))
}

fn finish_list(
    element: &FieldRef,
    offsets: Vec<i32>,
    validity: Vec<bool>,
    any_null: bool,
    values: ArrayRef,
) -> Result<ColumnarValue> {
    let nulls = if any_null {
        Some(NullBuffer::from(validity))
    } else {
        None
    };
    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        Arc::clone(element),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    )?)))
}

#[cfg(test)]
mod tests;
