use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StructArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, lit,
};

use crate::session_time_zone::session_time_zone_from_options;

pub const NESTED_NTZ_CAST_NAME: &str = "__repark_cast_nested_ntz__";

static NESTED_NTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(NestedNtzCast::new())));

#[must_use]
pub fn nested_ntz_cast_udf() -> Arc<ScalarUDF> {
    Arc::clone(&NESTED_NTZ_CAST)
}

#[must_use]
pub fn nested_ntz_cast_expr(value: Expr, paths: &str) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        nested_ntz_cast_udf(),
        vec![value, lit(paths)],
    ))
}

#[derive(Debug)]
struct NestedNtzCast {
    signature: Signature,
}

impl NestedNtzCast {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
}

impl PartialEq for NestedNtzCast {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NestedNtzCast {}

impl Hash for NestedNtzCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn plan_error(detail: &str) -> DataFusionError {
    DataFusionError::Plan(format!("'{NESTED_NTZ_CAST_NAME}' {detail}"))
}

fn is_timestamp(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _))
}

struct Group {
    name: String,
    tails: Vec<Vec<String>>,
}

fn parse_paths(text: &str) -> Result<Vec<Vec<String>>> {
    if text.is_empty() {
        return Err(plan_error("requires a path literal"));
    }
    let mut paths = Vec::new();
    for path in text.split(',') {
        if path.is_empty() || path.split('.').any(str::is_empty) {
            return Err(plan_error(&format!("cannot resolve path '{path}'")));
        }
        paths.push(path.split('.').map(str::to_string).collect());
    }
    Ok(paths)
}

fn path_text(scalar: Option<&ScalarValue>) -> Result<String> {
    match scalar {
        Some(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
        ) => Ok(text.clone()),
        _ => Err(plan_error("requires a string literal of source paths")),
    }
}

fn group_paths(paths: &[Vec<String>]) -> Result<Vec<Group>> {
    let mut groups: Vec<Group> = Vec::new();
    for path in paths {
        let Some((head, tail)) = path.split_first() else {
            return Err(plan_error("cannot resolve an empty path"));
        };
        if let Some(group) = groups.iter_mut().find(|group| group.name == *head) {
            group.tails.push(tail.to_vec());
        } else {
            groups.push(Group {
                name: head.clone(),
                tails: vec![tail.to_vec()],
            });
        }
    }
    Ok(groups)
}

enum ChildPlan {
    Keep,
    Leaf,
    Nested(Vec<Vec<String>>),
}

fn plan_children(fields: &Fields, paths: &[Vec<String>]) -> Result<Vec<ChildPlan>> {
    let groups = group_paths(paths)?;
    let mut seen = vec![false; groups.len()];
    let mut plans = Vec::with_capacity(fields.len());
    for field in fields {
        let Some(index) = groups
            .iter()
            .position(|group| group.name.as_str() == field.name().as_str())
        else {
            plans.push(ChildPlan::Keep);
            continue;
        };
        seen[index] = true;
        let leaf = groups[index].tails.iter().any(Vec::is_empty);
        let nested: Vec<Vec<String>> = groups[index]
            .tails
            .iter()
            .filter(|tail| !tail.is_empty())
            .cloned()
            .collect();
        if leaf && !nested.is_empty() {
            return Err(plan_error(&format!(
                "cannot resolve '{}' as both a leaf and a struct",
                field.name()
            )));
        }
        if leaf {
            if !is_timestamp(field.data_type()) {
                return Err(plan_error(&format!(
                    "cannot resolve '{}' to a timestamp",
                    field.name()
                )));
            }
            plans.push(ChildPlan::Leaf);
        } else if !matches!(field.data_type(), DataType::Struct(_)) {
            return Err(plan_error(&format!(
                "cannot resolve '{}' through a struct",
                field.name()
            )));
        } else {
            plans.push(ChildPlan::Nested(nested));
        }
    }
    if let Some(index) = seen.iter().position(|hit| !*hit) {
        return Err(plan_error(&format!(
            "cannot resolve '{}'",
            groups[index].name
        )));
    }
    Ok(plans)
}

fn retype(data_type: &DataType, paths: &[Vec<String>]) -> Result<DataType> {
    let DataType::Struct(fields) = data_type else {
        return Err(plan_error("requires a struct"));
    };
    let plans = plan_children(fields, paths)?;
    let mut rebuilt = Vec::with_capacity(fields.len());
    for (field, plan) in fields.iter().zip(plans) {
        let data_type = match plan {
            ChildPlan::Keep => field.data_type().clone(),
            ChildPlan::Leaf => DataType::Timestamp(TimeUnit::Microsecond, None),
            ChildPlan::Nested(nested) => retype(field.data_type(), &nested)?,
        };
        rebuilt.push(Arc::new(field.as_ref().clone().with_data_type(data_type)));
    }
    Ok(DataType::Struct(Fields::from(rebuilt)))
}

fn ntz_child(input: &ArrayRef, args: &ScalarFunctionArgs) -> Result<ArrayRef> {
    match crate::timestamp_ltz_ntz::ntz_single(input, args, false)? {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array(),
    }
}

fn convert_struct(
    array: &StructArray,
    paths: &[Vec<String>],
    args: &ScalarFunctionArgs,
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = retype(array.data_type(), paths)? else {
        return Err(plan_error("requires a struct"));
    };
    let plans = plan_children(array.fields(), paths)?;
    let mut children = Vec::with_capacity(plans.len());
    for (index, plan) in plans.into_iter().enumerate() {
        let child = Arc::clone(array.column(index));
        let converted = match plan {
            ChildPlan::Keep => child,
            ChildPlan::Leaf => ntz_child(&child, args)?,
            ChildPlan::Nested(nested) => {
                let Some(inner) = child.as_struct_opt() else {
                    return Err(plan_error("requires a struct"));
                };
                convert_struct(inner, &nested, args)?
            }
        };
        children.push(converted);
    }
    Ok(Arc::new(StructArray::try_new(
        fields,
        children,
        array.nulls().cloned(),
    )?))
}

fn read_zone(args: &ScalarFunctionArgs) -> Result<()> {
    let zone = session_time_zone_from_options(args.config_options.as_ref());
    zone.parse::<Tz>().map(|_| ()).map_err(|error| {
        DataFusionError::Execution(format!(
            "session timezone {zone:?} could not be resolved at query time ({error})"
        ))
    })
}

impl ScalarUDFImpl for NestedNtzCast {
    fn name(&self) -> &str {
        NESTED_NTZ_CAST_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(plan_error(
            "return type comes from the struct field and the path literal",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(source) = args.arg_fields.first() else {
            return Err(plan_error("requires a struct and a path literal"));
        };
        if !matches!(source.data_type(), DataType::Struct(_)) {
            return Err(plan_error("requires a struct"));
        }
        let paths = parse_paths(&path_text(args.scalar_arguments.get(1).copied().flatten())?)?;
        let data_type = retype(source.data_type(), &paths)?;
        Ok(Arc::new(source.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        read_zone(&args)?;
        let Some(source) = args.args.first() else {
            return Err(plan_error("requires a struct and a path literal"));
        };
        let paths = parse_paths(&path_text(match args.args.get(1) {
            Some(ColumnarValue::Scalar(scalar)) => Some(scalar),
            _ => None,
        })?)?;
        match source {
            ColumnarValue::Array(array) => {
                let Some(struct_array) = array.as_struct_opt() else {
                    return Err(plan_error("requires a struct"));
                };
                Ok(ColumnarValue::Array(convert_struct(
                    struct_array,
                    &paths,
                    &args,
                )?))
            }
            ColumnarValue::Scalar(ScalarValue::Null) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_new_null(args.return_field.data_type())?,
            )),
            ColumnarValue::Scalar(ScalarValue::Struct(array)) => {
                let converted = convert_struct(array, &paths, &args)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    converted.as_ref(),
                    0,
                )?))
            }
            ColumnarValue::Scalar(scalar) if scalar.is_null() => Ok(ColumnarValue::Scalar(
                ScalarValue::try_new_null(args.return_field.data_type())?,
            )),
            _ => Err(plan_error("requires a struct")),
        }
    }
}

#[cfg(test)]
mod tests;
