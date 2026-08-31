//! Convert a Spark `rewrite_data_files` `where` string to an Iceberg file-selection predicate.

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema, Type};

const MAX_WHERE_DEPTH: usize = 64;

/// Parse `where_sql` against `schema` as an Iceberg file-selection predicate.
///
/// # Errors
/// Spark's wrapper `Cannot parse predicates in where option: {expr}` on any conversion failure.
pub(in crate::call) fn parse_rewrite_where(where_sql: &str, schema: &Schema) -> Result<Predicate> {
    if where_sql.trim().is_empty() || where_sql.contains(';') {
        return Err(where_parse_error(where_sql));
    }
    let dialect = DatabricksDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(where_sql)
        .map_err(|_| where_parse_error(where_sql))?;
    let expr = parser
        .parse_expr()
        .map_err(|_| where_parse_error(where_sql))?;
    if !matches!(parser.peek_token().token, Token::EOF) {
        return Err(where_parse_error(where_sql));
    }
    expr_to_predicate(&expr, schema, 0).map_err(|_| where_parse_error(where_sql))
}

fn where_parse_error(where_sql: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Cannot parse predicates in where option: {where_sql}"
    ))
}

fn expr_to_predicate(expr: &Expr, schema: &Schema, depth: usize) -> Result<Predicate> {
    if depth >= MAX_WHERE_DEPTH {
        return Err(DataFusionError::Plan(
            "where predicate is too nested".to_string(),
        ));
    }
    match expr {
        Expr::Nested(inner) => expr_to_predicate(inner, schema, depth + 1),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(!expr_to_predicate(inner, schema, depth + 1)?),
        Expr::IsNull(inner) => {
            let name = column_name(inner, schema)?;
            Ok(Reference::new(name).is_null())
        }
        Expr::IsNotNull(inner) => {
            let name = column_name(inner, schema)?;
            Ok(Reference::new(name).is_not_null())
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
            ..
        } => in_list_predicate(inner, list, *negated, schema),
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => between_predicate(inner, low, high, *negated, schema),
        Expr::BinaryOp { left, op, right } => binary_predicate(left, op, right, schema, depth),
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(true),
            ..
        }) => Ok(Predicate::AlwaysTrue),
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(false),
            ..
        }) => Ok(Predicate::AlwaysFalse),
        _ => Err(DataFusionError::Plan(
            "unsupported where expression".to_string(),
        )),
    }
}

fn binary_predicate(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &Schema,
    depth: usize,
) -> Result<Predicate> {
    match op {
        BinaryOperator::And => Ok(expr_to_predicate(left, schema, depth + 1)?
            .and(expr_to_predicate(right, schema, depth + 1)?)),
        BinaryOperator::Or => Ok(expr_to_predicate(left, schema, depth + 1)?
            .or(expr_to_predicate(right, schema, depth + 1)?)),
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => comparison_predicate(left, op, right, schema),
        _ => Err(DataFusionError::Plan(
            "unsupported where operator".to_string(),
        )),
    }
}

fn comparison_predicate(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &Schema,
) -> Result<Predicate> {
    if let Ok(name) = column_name(left, schema) {
        let field_type = primitive_type_of(schema, &name)?;
        let datum = literal_datum(right, &field_type)?;
        return apply_comparison(name, op, datum, false);
    }
    if let Ok(name) = column_name(right, schema) {
        let field_type = primitive_type_of(schema, &name)?;
        let datum = literal_datum(left, &field_type)?;
        return apply_comparison(name, op, datum, true);
    }
    Err(DataFusionError::Plan(
        "where comparison needs a column and a literal".to_string(),
    ))
}

fn apply_comparison(
    name: String,
    op: &BinaryOperator,
    datum: Datum,
    swapped: bool,
) -> Result<Predicate> {
    let reference = Reference::new(name);
    let predicate = match (op, swapped) {
        (BinaryOperator::Eq, _) => reference.equal_to(datum),
        (BinaryOperator::NotEq, _) => reference.not_equal_to(datum),
        (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => reference.less_than(datum),
        (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => {
            reference.less_than_or_equal_to(datum)
        }
        (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => reference.greater_than(datum),
        (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => {
            reference.greater_than_or_equal_to(datum)
        }
        _ => {
            return Err(DataFusionError::Plan(
                "unsupported where comparison".to_string(),
            ));
        }
    };
    Ok(predicate)
}

fn in_list_predicate(
    inner: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &Schema,
) -> Result<Predicate> {
    let name = column_name(inner, schema)?;
    let field_type = primitive_type_of(schema, &name)?;
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        values.push(literal_datum(item, &field_type)?);
    }
    let predicate = Reference::new(name).is_in(values);
    if negated {
        Ok(!predicate)
    } else {
        Ok(predicate)
    }
}

fn between_predicate(
    inner: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    schema: &Schema,
) -> Result<Predicate> {
    let name = column_name(inner, schema)?;
    let field_type = primitive_type_of(schema, &name)?;
    let low_datum = literal_datum(low, &field_type)?;
    let high_datum = literal_datum(high, &field_type)?;
    let lower = Reference::new(name.clone()).greater_than_or_equal_to(low_datum);
    let upper = Reference::new(name).less_than_or_equal_to(high_datum);
    let predicate = lower.and(upper);
    if negated {
        Ok(!predicate)
    } else {
        Ok(predicate)
    }
}

fn column_name(expr: &Expr, schema: &Schema) -> Result<String> {
    let raw = match expr {
        Expr::Identifier(ident) => ident.value.as_str(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| ident.value.as_str())
            .ok_or_else(|| DataFusionError::Plan("empty column name".to_string()))?,
        _ => {
            return Err(DataFusionError::Plan(
                "where column is not an identifier".to_string(),
            ));
        }
    };
    let field = schema
        .field_by_name_case_insensitive(raw)
        .ok_or_else(|| DataFusionError::Plan(format!("unknown where column `{raw}`")))?;
    Ok(field.name.clone())
}

fn primitive_type_of(schema: &Schema, name: &str) -> Result<PrimitiveType> {
    let field = schema
        .field_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("unknown where column `{name}`")))?;
    match field.field_type.as_ref() {
        Type::Primitive(primitive) => Ok(primitive.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "where column `{name}` is not a primitive type"
        ))),
    }
}

fn literal_datum(expr: &Expr, field_type: &PrimitiveType) -> Result<Datum> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => value_to_datum(value, field_type),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            let Expr::Value(ValueWithSpan {
                value: Value::Number(raw, _),
                ..
            }) = inner.as_ref()
            else {
                return Err(DataFusionError::Plan(
                    "where literal negation is not a number".to_string(),
                ));
            };
            number_to_datum(&format!("-{raw}"), field_type)
        }
        _ => Err(DataFusionError::Plan(
            "where literal is not a scalar value".to_string(),
        )),
    }
}

fn value_to_datum(value: &Value, field_type: &PrimitiveType) -> Result<Datum> {
    match value {
        Value::Number(raw, _) => number_to_datum(raw, field_type),
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => match field_type {
            PrimitiveType::String => Ok(Datum::string(text)),
            PrimitiveType::Int | PrimitiveType::Long => number_to_datum(text, field_type),
            _ => Err(DataFusionError::Plan(
                "where string literal does not match column type".to_string(),
            )),
        },
        Value::Boolean(flag) => match field_type {
            PrimitiveType::Boolean => Ok(Datum::bool(*flag)),
            _ => Err(DataFusionError::Plan(
                "where boolean literal does not match column type".to_string(),
            )),
        },
        _ => Err(DataFusionError::Plan(
            "where literal type is not supported".to_string(),
        )),
    }
}

fn number_to_datum(raw: &str, field_type: &PrimitiveType) -> Result<Datum> {
    match field_type {
        PrimitiveType::Int => raw
            .parse::<i32>()
            .map(Datum::int)
            .map_err(|_| DataFusionError::Plan("where int literal does not fit i32".to_string())),
        PrimitiveType::Long => raw
            .parse::<i64>()
            .map(Datum::long)
            .map_err(|_| DataFusionError::Plan("where long literal does not fit i64".to_string())),
        PrimitiveType::Float => raw
            .parse::<f32>()
            .map(Datum::float)
            .map_err(|_| DataFusionError::Plan("where float literal is not f32".to_string())),
        PrimitiveType::Double => raw
            .parse::<f64>()
            .map(Datum::double)
            .map_err(|_| DataFusionError::Plan("where double literal is not f64".to_string())),
        _ => Err(DataFusionError::Plan(
            "where numeric literal does not match column type".to_string(),
        )),
    }
}
