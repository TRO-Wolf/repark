use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, DataType, Expr, Ident, UnaryOperator, Value, ValueWithSpan,
};
use iceberg::Catalog;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{DataFile, Datum, NestedFieldRef, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::write::commit_error::commit_result;
use crate::write::commit_target::{maybe_to_branch, snapshot_id_for_commit};
use crate::write::illegal_argument::illegal_argument_error;
use crate::write::overwrite::OverwriteIsolation;
use crate::write::summary_collision::{EngineSummary, extras_need_removed_files};
use crate::write::write_options::{isolation_with_override, summary_with_extras};

const MAX_FILTER_DEPTH: usize = 64;

struct Converter<'a> {
    schema: &'a Schema,
    whole: &'a Expr,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Comparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    NullSafe,
    NotNullSafe,
}

enum Literal {
    Value(Datum),
    Null,
    AboveRange,
    BelowRange,
}

#[allow(clippy::missing_errors_doc)]
pub fn spark_overwrite_filter(expr: &Expr, schema: &Schema) -> Result<Predicate> {
    let converter = Converter {
        schema,
        whole: expr,
    };
    if let Some(folded) = converter.folded(expr, false, 0)? {
        return Err(cannot_convert(folded));
    }
    converter.convert(expr, false, 0)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_overwrite_by_filter_with_summary(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    predicate: Predicate,
    branch: Option<&str>,
    summary_extra: &[(String, String)],
    isolation_override: Option<&str>,
) -> Result<Table> {
    if extras_need_removed_files(summary_extra) {
        return Err(DataFusionError::NotImplemented(
            "INSERT INTO … REPLACE WHERE cannot set a snapshot property that names an engine \
             summary field"
                .to_string(),
        ));
    }
    let isolation = isolation_with_override(table, isolation_override)?;
    let engine = EngineSummary::for_overwrite(table, &staged_files, branch);
    let (operation_id, summary) = summary_with_extras(summary_extra, &engine)?;
    let tx = Transaction::new(table);
    let mut action = tx
        .overwrite_files()
        .overwrite_by_row_filter(predicate)
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let action = maybe_to_branch(action, branch, |action, name| action.to_branch(name));
    let tx = action
        .apply(tx)
        .map_err(crate::catalog::iceberg_to_datafusion)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

fn cannot_convert(rendered: impl std::fmt::Display) -> DataFusionError {
    illegal_argument_error(format!(
        "Cannot convert Spark predicate to Iceberg expression: {rendered}"
    ))
}

fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

impl Comparison {
    fn of(op: &BinaryOperator) -> Option<Self> {
        match op {
            BinaryOperator::Eq => Some(Self::Eq),
            BinaryOperator::NotEq => Some(Self::NotEq),
            BinaryOperator::Lt => Some(Self::Lt),
            BinaryOperator::LtEq => Some(Self::LtEq),
            BinaryOperator::Gt => Some(Self::Gt),
            BinaryOperator::GtEq => Some(Self::GtEq),
            BinaryOperator::Spaceship => Some(Self::NullSafe),
            _ => None,
        }
    }

    fn negated(self) -> Self {
        match self {
            Self::Eq => Self::NotEq,
            Self::NotEq => Self::Eq,
            Self::Lt => Self::GtEq,
            Self::LtEq => Self::Gt,
            Self::Gt => Self::LtEq,
            Self::GtEq => Self::Lt,
            Self::NullSafe => Self::NotNullSafe,
            Self::NotNullSafe => Self::NullSafe,
        }
    }

    fn mirrored(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            other => other,
        }
    }

    fn holds_for_every_value(self, literal: &Literal) -> bool {
        match literal {
            Literal::AboveRange => matches!(self, Self::Lt | Self::LtEq | Self::NotEq),
            Literal::BelowRange => matches!(self, Self::Gt | Self::GtEq | Self::NotEq),
            Literal::Value(_) | Literal::Null => false,
        }
    }
}

impl Converter<'_> {
    fn refuse(&self) -> DataFusionError {
        cannot_convert(self.whole)
    }

    fn folded(&self, expr: &Expr, negated: bool, depth: usize) -> Result<Option<String>> {
        if depth >= MAX_FILTER_DEPTH {
            return Ok(None);
        }
        match expr {
            Expr::Nested(inner) => self.folded(inner, negated, depth + 1),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => self.folded(inner, !negated, depth + 1),
            Expr::BinaryOp { left, op, right } => {
                let Some(comparison) = Comparison::of(op) else {
                    return Ok(None);
                };
                let comparison = if negated {
                    comparison.negated()
                } else {
                    comparison
                };
                let Some((field, literal, comparison)) = self.operands(left, comparison, right)?
                else {
                    return Ok(None);
                };
                Ok(match self.literal(literal, &field)? {
                    Literal::Null
                        if !matches!(
                            comparison,
                            Comparison::NullSafe | Comparison::NotNullSafe
                        ) =>
                    {
                        Some("null".to_string())
                    }
                    bound @ (Literal::AboveRange | Literal::BelowRange) => {
                        Some(if comparison.holds_for_every_value(&bound) {
                            format!("({} IS NOT NULL) OR (null)", field.name)
                        } else {
                            "null".to_string()
                        })
                    }
                    _ => None,
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated: listed_negated,
            } if !(negated ^ *listed_negated) => {
                let Some(field) = self.column_of(inner)? else {
                    return Ok(None);
                };
                let mut out_of_range = true;
                for item in list {
                    out_of_range &= matches!(
                        self.literal(item, &field)?,
                        Literal::AboveRange | Literal::BelowRange
                    );
                }
                Ok(out_of_range.then(|| "null".to_string()))
            }
            _ => Ok(None),
        }
    }

    fn convert(&self, expr: &Expr, negated: bool, depth: usize) -> Result<Predicate> {
        if depth >= MAX_FILTER_DEPTH {
            return Err(self.refuse());
        }
        match expr {
            Expr::Nested(inner) => self.convert(inner, negated, depth + 1),
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(flag),
                ..
            }) => Ok(if *flag ^ negated {
                Predicate::AlwaysTrue
            } else {
                Predicate::AlwaysFalse
            }),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => self.convert(inner, !negated, depth + 1),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                let reference = Reference::new(self.column(inner)?.name.clone());
                Ok(if matches!(expr, Expr::IsNull(_)) ^ negated {
                    reference.is_null()
                } else {
                    reference.is_not_null()
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated: listed_negated,
            } => self.in_list(inner, list, *listed_negated ^ negated),
            Expr::Between {
                expr: inner,
                negated: listed_negated,
                low,
                high,
            } => {
                if *listed_negated ^ negated {
                    let below = self.comparison(inner, Comparison::Lt, low)?;
                    Ok(below.or(self.comparison(inner, Comparison::Gt, high)?))
                } else {
                    let above = self.comparison(inner, Comparison::GtEq, low)?;
                    Ok(above.and(self.comparison(inner, Comparison::LtEq, high)?))
                }
            }
            Expr::Like {
                negated: listed_negated,
                any: false,
                expr: inner,
                pattern,
                escape_char: None,
            } => {
                let like = self.like(inner, pattern)?;
                Ok(if *listed_negated ^ negated {
                    !like
                } else {
                    like
                })
            }
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    let left = self.convert(left, negated, depth + 1)?;
                    let right = self.convert(right, negated, depth + 1)?;
                    Ok(if matches!(op, BinaryOperator::And) ^ negated {
                        left.and(right)
                    } else {
                        left.or(right)
                    })
                }
                _ => {
                    let comparison = Comparison::of(op).ok_or_else(|| self.refuse())?;
                    let comparison = if negated {
                        comparison.negated()
                    } else {
                        comparison
                    };
                    self.comparison(left, comparison, right)
                }
            },
            _ => Err(self.refuse()),
        }
    }

    fn column(&self, expr: &Expr) -> Result<NestedFieldRef> {
        self.column_of(expr)?.ok_or_else(|| self.refuse())
    }

    fn column_of(&self, expr: &Expr) -> Result<Option<NestedFieldRef>> {
        let Expr::Identifier(ident) = unnest(expr) else {
            return Ok(None);
        };
        let fields = self.schema.as_struct().fields();
        if let Some(field) = fields.iter().find(|field| field.name == ident.value) {
            return match field.field_type.as_ref() {
                Type::Primitive(_) => Ok(Some(Arc::clone(field))),
                _ => Err(self.refuse()),
            };
        }
        if fields
            .iter()
            .any(|field| field.name.eq_ignore_ascii_case(&ident.value))
        {
            return Err(missing_field(ident, self.schema));
        }
        Err(self.refuse())
    }

    fn operands<'e>(
        &self,
        left: &'e Expr,
        comparison: Comparison,
        right: &'e Expr,
    ) -> Result<Option<(NestedFieldRef, &'e Expr, Comparison)>> {
        if let Some(field) = self.column_of(left)? {
            return Ok(Some((field, right, comparison)));
        }
        Ok(self
            .column_of(right)?
            .map(|field| (field, left, comparison.mirrored())))
    }

    fn comparison(&self, left: &Expr, comparison: Comparison, right: &Expr) -> Result<Predicate> {
        let (field, literal, comparison) = self
            .operands(left, comparison, right)?
            .ok_or_else(|| self.refuse())?;
        let reference = Reference::new(field.name.clone());
        let datum = match self.literal(literal, &field)? {
            Literal::Value(datum) => datum,
            Literal::Null => {
                return match comparison {
                    Comparison::NullSafe => Ok(reference.is_null()),
                    Comparison::NotNullSafe => Ok(reference.is_not_null()),
                    _ => Err(self.refuse()),
                };
            }
            Literal::AboveRange | Literal::BelowRange => return Err(self.refuse()),
        };
        Ok(match comparison {
            Comparison::Eq | Comparison::NullSafe => reference.equal_to(datum),
            Comparison::NotEq | Comparison::NotNullSafe => !reference.equal_to(datum),
            Comparison::Lt => reference
                .clone()
                .is_not_null()
                .and(reference.less_than(datum)),
            Comparison::LtEq => reference
                .clone()
                .is_not_null()
                .and(reference.less_than_or_equal_to(datum)),
            Comparison::Gt => reference.greater_than(datum),
            Comparison::GtEq => reference.greater_than_or_equal_to(datum),
        })
    }

    fn in_list(&self, inner: &Expr, list: &[Expr], negated: bool) -> Result<Predicate> {
        let field = self.column(inner)?;
        let mut values: Vec<Datum> = Vec::with_capacity(list.len());
        let mut has_null = false;
        for item in list {
            match self.literal(item, &field)? {
                Literal::Value(datum) => {
                    if !values.contains(&datum) {
                        values.push(datum);
                    }
                }
                Literal::Null => has_null = true,
                Literal::AboveRange | Literal::BelowRange => {}
            }
        }
        let reference = Reference::new(field.name.clone());
        match (values.len(), has_null) {
            (0, _) => Err(self.refuse()),
            (1, false) => {
                let equal = reference.equal_to(values.remove(0));
                Ok(if negated { !equal } else { equal })
            }
            _ if negated => Ok(reference
                .clone()
                .is_not_null()
                .and(reference.is_not_in(values))),
            _ => Ok(reference.is_in(values)),
        }
    }

    fn like(&self, inner: &Expr, pattern: &Expr) -> Result<Predicate> {
        let field = self.column(inner)?;
        if !matches!(
            field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::String)
        ) {
            return Err(self.refuse());
        }
        let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) = unnest(pattern)
        else {
            return Err(self.refuse());
        };
        let reference = Reference::new(field.name.clone());
        let wildcard = |text: &str| text.contains(['%', '_', '\\']);
        if !wildcard(text) {
            return Ok(reference.equal_to(Datum::string(text)));
        }
        match text.strip_suffix('%') {
            Some(prefix) if !prefix.is_empty() && !wildcard(prefix) => {
                Ok(reference.starts_with(Datum::string(prefix)))
            }
            _ => Err(self.refuse()),
        }
    }

    fn literal(&self, expr: &Expr, field: &NestedFieldRef) -> Result<Literal> {
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            return Err(self.refuse());
        };
        match unnest(expr) {
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }) => Some(Literal::Null),
            Expr::Value(ValueWithSpan {
                value: Value::Number(raw, _),
                ..
            }) => number_literal(raw, primitive),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => match unnest(inner) {
                Expr::Value(ValueWithSpan {
                    value: Value::Number(raw, _),
                    ..
                }) => number_literal(&format!("-{raw}"), primitive),
                _ => None,
            },
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
                ..
            }) => text_datum(text, primitive).map(Literal::Value),
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(flag),
                ..
            }) if *primitive == PrimitiveType::Boolean => Some(Literal::Value(Datum::bool(*flag))),
            Expr::TypedString(typed) if typed.data_type == DataType::Date => {
                match (&typed.value.value, primitive) {
                    (Value::SingleQuotedString(text), PrimitiveType::Date) => {
                        Datum::date_from_str(text.trim()).ok().map(Literal::Value)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
        .ok_or_else(|| self.refuse())
    }
}

fn number_literal(raw: &str, primitive: &PrimitiveType) -> Option<Literal> {
    if let Some(datum) = number_datum(raw, primitive) {
        return Some(Literal::Value(datum));
    }
    let whole = match primitive {
        PrimitiveType::Int | PrimitiveType::Long => integral_text(raw)?,
        _ => return None,
    };
    let digits = whole.strip_prefix('-').unwrap_or(whole);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(if whole.starts_with('-') {
        Literal::BelowRange
    } else {
        Literal::AboveRange
    })
}

fn number_datum(raw: &str, primitive: &PrimitiveType) -> Option<Datum> {
    match primitive {
        PrimitiveType::Int => integral_text(raw)?.parse::<i32>().ok().map(Datum::int),
        PrimitiveType::Long => integral_text(raw)?.parse::<i64>().ok().map(Datum::long),
        PrimitiveType::Double => raw.parse::<f64>().ok().map(Datum::double),
        PrimitiveType::Float => raw.parse::<f32>().ok().map(Datum::float),
        _ => None,
    }
}

fn integral_text(raw: &str) -> Option<&str> {
    match raw.split_once('.') {
        None => Some(raw),
        Some((whole, fraction)) if fraction.bytes().all(|byte| byte == b'0') => Some(whole),
        Some(_) => None,
    }
}

fn text_datum(text: &str, primitive: &PrimitiveType) -> Option<Datum> {
    let trimmed = text.trim();
    match primitive {
        PrimitiveType::String => Some(Datum::string(text)),
        PrimitiveType::Int => trimmed.parse::<i32>().ok().map(Datum::int),
        PrimitiveType::Long => trimmed.parse::<i64>().ok().map(Datum::long),
        PrimitiveType::Double => trimmed.parse::<f64>().ok().map(Datum::double),
        PrimitiveType::Float => trimmed.parse::<f32>().ok().map(Datum::float),
        PrimitiveType::Date => Datum::date_from_str(trimmed).ok(),
        PrimitiveType::Boolean => match trimmed.to_ascii_lowercase().as_str() {
            "true" => Some(Datum::bool(true)),
            "false" => Some(Datum::bool(false)),
            _ => None,
        },
        _ => None,
    }
}

fn missing_field(ident: &Ident, schema: &Schema) -> DataFusionError {
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            let optional = if field.required {
                "required"
            } else {
                "optional"
            };
            format!(
                "{}: {}: {optional} {}",
                field.id, field.name, field.field_type
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "Cannot find field '{}' in struct: struct<{fields}>",
        ident.value
    ))
}

#[cfg(test)]
mod tests;
