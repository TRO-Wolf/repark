use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::InSubquery;
use datafusion::logical_expr::{
    Cast, DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Subquery,
};

const MAX_PRECISION: i16 = 38;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Compare,
    Unify,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    Scalar(DataType),
    Struct(Vec<Option<Shape>>),
    List(Box<Shape>),
    Map(Option<Box<Shape>>, Option<Box<Shape>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Double,
    Spark(Shape),
}

fn integer_digits(data_type: &DataType) -> Option<i16> {
    match data_type {
        DataType::Int8 | DataType::UInt8 => Some(3),
        DataType::Int16 | DataType::UInt16 => Some(5),
        DataType::Int32 | DataType::UInt32 => Some(10),
        DataType::Int64 | DataType::UInt64 => Some(20),
        _ => None,
    }
}

fn decimal_digits(data_type: &DataType) -> Option<(i16, i16)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
            if *scale >= 0 =>
        {
            Some((i16::from(*precision) - i16::from(*scale), i16::from(*scale)))
        }
        _ => None,
    }
}

fn is_double_class_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
}

fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}

#[must_use]
pub(crate) fn literal_digits(expr: &Expr) -> Option<i16> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    let magnitude = match value {
        ScalarValue::Int8(Some(value)) => i128::from(*value),
        ScalarValue::Int16(Some(value)) => i128::from(*value),
        ScalarValue::Int32(Some(value)) => i128::from(*value),
        ScalarValue::Int64(Some(value)) => i128::from(*value),
        ScalarValue::UInt8(Some(value)) => i128::from(*value),
        ScalarValue::UInt16(Some(value)) => i128::from(*value),
        ScalarValue::UInt32(Some(value)) => i128::from(*value),
        ScalarValue::UInt64(Some(value)) => i128::from(*value),
        _ => return None,
    }
    .unsigned_abs();
    i16::try_from(magnitude.checked_ilog10().map_or(1, |log| log + 1)).ok()
}

fn decimal128(precision: i16, scale: i16) -> Option<DataType> {
    Some(DataType::Decimal128(
        u8::try_from(precision).ok()?,
        i8::try_from(scale).ok()?,
    ))
}

fn fold<'a>(
    operands: impl IntoIterator<Item = (&'a DataType, Option<i16>)>,
    mode: Mode,
) -> Option<(i16, i16)> {
    let mut decimal = false;
    let mut range = 0;
    let mut scale = 0;
    for (data_type, literal) in operands {
        let (digits, places) = if let Some(found) = decimal_digits(data_type) {
            decimal = true;
            found
        } else if let Some(form) = integer_digits(data_type) {
            match (mode, literal) {
                (Mode::Compare, Some(own)) => (own, 0),
                _ => (form, 0),
            }
        } else if *data_type == DataType::Null {
            continue;
        } else {
            return None;
        };
        range = range.max(digits);
        scale = scale.max(places);
    }
    decimal.then_some((range, scale))
}

#[must_use]
pub(crate) fn spark_decimal_type<'a>(
    operands: impl IntoIterator<Item = (&'a DataType, Option<i16>)>,
    mode: Mode,
) -> Option<DataType> {
    let (range, scale) = fold(operands, mode)?;
    if range + scale <= MAX_PRECISION {
        return decimal128(range + scale, scale);
    }
    decimal128(MAX_PRECISION, (MAX_PRECISION - range).max(0))
}

#[must_use]
pub(crate) fn spark_decimal_common_type<'a>(
    operands: impl IntoIterator<Item = (&'a DataType, Option<i16>)>,
    mode: Mode,
) -> Option<DataType> {
    let (range, scale) = fold(operands, mode)?;
    if range + scale <= MAX_PRECISION {
        return None;
    }
    decimal128(MAX_PRECISION, (MAX_PRECISION - range).max(0))
}

fn struct_shape(types: &[&DataType]) -> Option<Shape> {
    let DataType::Struct(first) = types.first()? else {
        return None;
    };
    let mut all = Vec::with_capacity(types.len());
    for data_type in types {
        let DataType::Struct(fields) = data_type else {
            return None;
        };
        let same = fields.len() == first.len()
            && fields
                .iter()
                .zip(first.iter())
                .all(|(field, other)| field.name().eq_ignore_ascii_case(other.name()));
        if !same {
            return None;
        }
        all.push(fields);
    }
    let shapes = (0..first.len())
        .map(|position| {
            common_shape(
                &all.iter()
                    .map(|fields| fields[position].data_type())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    shapes
        .iter()
        .any(Option::is_some)
        .then_some(Shape::Struct(shapes))
}

fn map_parts(data_type: &DataType) -> Option<(&DataType, &DataType)> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(pair) = entries.data_type() else {
        return None;
    };
    (pair.len() == 2).then(|| (pair[0].data_type(), pair[1].data_type()))
}

fn common_shape(types: &[&DataType]) -> Option<Shape> {
    let present = types
        .iter()
        .copied()
        .filter(|data_type| **data_type != DataType::Null)
        .collect::<Vec<_>>();
    match present.first()? {
        DataType::Struct(_) => struct_shape(&present),
        DataType::List(_) | DataType::LargeList(_) => {
            let elements = present
                .iter()
                .map(|data_type| match data_type {
                    DataType::List(field) | DataType::LargeList(field) => Some(field.data_type()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            common_shape(&elements).map(|shape| Shape::List(Box::new(shape)))
        }
        DataType::Map(_, _) => {
            let parts = present
                .iter()
                .map(|data_type| map_parts(data_type))
                .collect::<Option<Vec<_>>>()?;
            let keys = parts.iter().map(|(key, _)| *key).collect::<Vec<_>>();
            let values = parts.iter().map(|(_, value)| *value).collect::<Vec<_>>();
            let key = common_shape(&keys).map(Box::new);
            let value = common_shape(&values).map(Box::new);
            (key.is_some() || value.is_some()).then_some(Shape::Map(key, value))
        }
        _ => spark_decimal_common_type(present.iter().map(|found| (*found, None)), Mode::Unify)
            .map(Shape::Scalar),
    }
}

#[must_use]
pub(crate) fn target_of_types(
    types: &[&DataType],
    literals: &[Option<i16>],
    mode: Mode,
    decimals: bool,
) -> Option<Target> {
    let mut decimal = false;
    let mut float = false;
    let mut nested = false;
    for data_type in types {
        decimal |= is_double_class_decimal(data_type);
        float |= is_float(data_type);
        nested |= is_nested(data_type);
    }
    if decimal && float {
        return Some(Target::Double);
    }
    if !decimals {
        return None;
    }
    if nested {
        return match mode {
            Mode::Unify => common_shape(types).map(Target::Spark),
            Mode::Compare => None,
        };
    }
    let operands = types
        .iter()
        .enumerate()
        .map(|(position, data_type)| (*data_type, literals.get(position).copied().flatten()));
    spark_decimal_common_type(operands, mode).map(|found| Target::Spark(Shape::Scalar(found)))
}

#[must_use]
pub(crate) fn target_of(
    exprs: &[&Expr],
    schema: &DFSchema,
    mode: Mode,
    decimals: bool,
) -> Option<Target> {
    let types = exprs
        .iter()
        .map(|expr| expr.get_type(schema).ok())
        .collect::<Option<Vec<_>>>()?;
    let literals = if mode == Mode::Compare {
        exprs.iter().map(|expr| literal_digits(expr)).collect()
    } else {
        Vec::new()
    };
    target_of_types(&types.iter().collect::<Vec<_>>(), &literals, mode, decimals)
}

fn retyped(field: &FieldRef, shape: &Shape) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(apply(field.data_type(), shape)),
    )
}

#[must_use]
pub(crate) fn apply(data_type: &DataType, shape: &Shape) -> DataType {
    match (shape, data_type) {
        (Shape::Scalar(target), found)
            if decimal_digits(found).is_some() || integer_digits(found).is_some() =>
        {
            target.clone()
        }
        (Shape::Struct(shapes), DataType::Struct(fields)) if shapes.len() == fields.len() => {
            DataType::Struct(
                fields
                    .iter()
                    .zip(shapes)
                    .map(|(field, shape)| match shape {
                        Some(shape) => retyped(field, shape),
                        None => Arc::clone(field),
                    })
                    .collect(),
            )
        }
        (Shape::List(shape), DataType::List(field)) => DataType::List(retyped(field, shape)),
        (Shape::List(shape), DataType::LargeList(field)) => {
            DataType::LargeList(retyped(field, shape))
        }
        (Shape::Map(key, value), DataType::Map(entries, sorted)) => {
            let DataType::Struct(pair) = entries.data_type() else {
                return data_type.clone();
            };
            if pair.len() != 2 {
                return data_type.clone();
            }
            let part = |field: &FieldRef, shape: &Option<Box<Shape>>| match shape {
                Some(shape) => retyped(field, shape),
                None => Arc::clone(field),
            };
            let pair = vec![part(&pair[0], key), part(&pair[1], value)];
            DataType::Map(
                Arc::new(Field::new(
                    entries.name(),
                    DataType::Struct(pair.into()),
                    entries.is_nullable(),
                )),
                *sorted,
            )
        }
        _ => data_type.clone(),
    }
}

#[must_use]
pub(crate) fn retarget_typed(expr: Expr, found: &DataType, target: &Target) -> Expr {
    let wanted = match target {
        Target::Double if is_double_class_decimal(found) => DataType::Float64,
        Target::Double => return expr,
        Target::Spark(shape) => apply(found, shape),
    };
    if wanted == *found {
        return expr;
    }
    Expr::Cast(Cast::new(Box::new(expr), wanted))
}

#[must_use]
pub(crate) fn retarget(expr: Expr, schema: &DFSchema, target: &Target) -> Expr {
    match expr.get_type(schema) {
        Ok(found) => retarget_typed(expr, &found, target),
        Err(_) => expr,
    }
}

pub(crate) fn array_contains_target(
    args: &[Expr],
    schema: &DFSchema,
    decimals: bool,
) -> Option<Target> {
    let [haystack, needle] = args else {
        return None;
    };
    let haystack = haystack.get_type(schema).ok()?;
    let (DataType::List(element) | DataType::LargeList(element)) = &haystack else {
        return None;
    };
    let needle = needle.get_type(schema).ok()?;
    match target_of_types(&[element.data_type(), &needle], &[], Mode::Unify, decimals)? {
        Target::Double => None,
        spark @ Target::Spark(_) => Some(spark),
    }
}

pub(crate) fn subquery_type(subquery: &Subquery) -> Option<DataType> {
    subquery
        .subquery
        .schema()
        .fields()
        .first()
        .map(|field| field.data_type().clone())
}

pub(crate) fn in_subquery_target(
    in_subquery: &InSubquery,
    schema: &DFSchema,
    decimals: bool,
) -> Option<Target> {
    let operand = in_subquery.expr.get_type(schema).ok()?;
    let column = subquery_type(&in_subquery.subquery)?;
    target_of_types(&[&operand, &column], &[], Mode::Unify, decimals)
}

pub(crate) fn widen_array_contains(
    args: Vec<Expr>,
    schema: &DFSchema,
    decimals: bool,
) -> Vec<Expr> {
    let Some(Target::Spark(shape)) = array_contains_target(&args, schema, decimals) else {
        return args;
    };
    let mut args = args.into_iter();
    let (Some(haystack), Some(needle)) = (args.next(), args.next()) else {
        return Vec::new();
    };
    let list = Target::Spark(Shape::List(Box::new(shape.clone())));
    let element = Target::Spark(shape);
    vec![
        retarget(haystack, schema, &list),
        retarget(needle, schema, &element),
    ]
}

pub(crate) fn conform_insert(dml: DmlStatement) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    };
    let table = dml.target.schema();
    if projection.expr.len() != table.fields().len() {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let source = projection.input.schema();
    let mut changed = false;
    let mut exprs = Vec::with_capacity(projection.expr.len());
    for (expr, field) in projection.expr.iter().zip(table.fields()) {
        let found = expr.get_type(source)?;
        if found == *field.data_type()
            || decimal_digits(&found).is_none()
            || decimal_digits(field.data_type()).is_none()
        {
            exprs.push(expr.clone());
            continue;
        }
        changed = true;
        let inner = match expr {
            Expr::Alias(alias) => alias.expr.as_ref().clone(),
            other => other.clone(),
        };
        exprs.push(
            Expr::Cast(Cast::new(Box::new(inner), field.data_type().clone())).alias(field.name()),
        );
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let input = Projection::try_new(exprs, Arc::clone(&projection.input))?;
    Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(LogicalPlan::Projection(input)),
        ..dml
    })))
}

#[cfg(test)]
mod tests;
