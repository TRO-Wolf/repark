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

#[allow(clippy::missing_errors_doc)]
pub fn spark_overwrite_filter(expr: &Expr, schema: &Schema) -> Result<Predicate> {
    if let Some(folded) = null_comparison(expr) {
        return Err(cannot_convert(folded));
    }
    let converter = Converter {
        schema,
        whole: expr,
    };
    converter.convert(expr, 0)
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

fn null_comparison(expr: &Expr) -> Option<&'static str> {
    let Expr::BinaryOp { left, op, right } = unnest(expr) else {
        return None;
    };
    let comparison = matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    );
    (comparison && (is_null_literal(left) || is_null_literal(right))).then_some("null")
}

fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(
        unnest(expr),
        Expr::Value(ValueWithSpan {
            value: Value::Null,
            ..
        })
    )
}

fn contains_in_list(expr: &Expr, depth: usize) -> bool {
    if depth >= MAX_FILTER_DEPTH {
        return true;
    }
    match expr {
        Expr::InList { .. } => true,
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
            contains_in_list(inner, depth + 1)
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_in_list(left, depth + 1) || contains_in_list(right, depth + 1)
        }
        _ => false,
    }
}

impl Converter<'_> {
    fn refuse(&self) -> DataFusionError {
        cannot_convert(self.whole)
    }

    fn convert(&self, expr: &Expr, depth: usize) -> Result<Predicate> {
        if depth >= MAX_FILTER_DEPTH {
            return Err(self.refuse());
        }
        match expr {
            Expr::Nested(inner) => self.convert(inner, depth + 1),
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(flag),
                ..
            }) => Ok(if *flag {
                Predicate::AlwaysTrue
            } else {
                Predicate::AlwaysFalse
            }),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => self.negated(inner, depth),
            Expr::IsNull(inner) => Ok(Reference::new(self.column(inner)?.name.clone()).is_null()),
            Expr::IsNotNull(inner) => {
                Ok(Reference::new(self.column(inner)?.name.clone()).is_not_null())
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => self.in_list(inner, list, *negated),
            Expr::Between {
                expr: inner,
                negated,
                low,
                high,
            } => {
                let lower = self.comparison(inner, &BinaryOperator::GtEq, low)?;
                let upper = self.comparison(inner, &BinaryOperator::LtEq, high)?;
                let range = lower.and(upper);
                Ok(if *negated { !range } else { range })
            }
            Expr::Like {
                negated,
                any: false,
                expr: inner,
                pattern,
                escape_char: None,
            } => {
                let like = self.like(inner, pattern)?;
                Ok(if *negated { !like } else { like })
            }
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Ok(self
                    .convert(left, depth + 1)?
                    .and(self.convert(right, depth + 1)?)),
                BinaryOperator::Or => Ok(self
                    .convert(left, depth + 1)?
                    .or(self.convert(right, depth + 1)?)),
                _ => self.comparison(left, op, right),
            },
            _ => Err(self.refuse()),
        }
    }

    fn negated(&self, inner: &Expr, depth: usize) -> Result<Predicate> {
        let child = unnest(inner);
        if let Expr::InList {
            expr: column,
            list,
            negated: false,
        } = child
        {
            return self.in_list(column, list, true);
        }
        if contains_in_list(child, 0) {
            return Err(self.refuse());
        }
        Ok(!self.convert(child, depth + 1)?)
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

    fn comparison(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<Predicate> {
        let (field, literal, swapped) = match self.column_of(left)? {
            Some(field) => (field, right, false),
            None => (self.column(right)?, left, true),
        };
        let reference = Reference::new(field.name.clone());
        let datum = self.datum(literal, &field)?;
        let Some(datum) = datum else {
            return match op {
                BinaryOperator::Spaceship => Ok(reference.is_null()),
                _ => Err(self.refuse()),
            };
        };
        match (op, swapped) {
            (BinaryOperator::Eq | BinaryOperator::Spaceship, _) => Ok(reference.equal_to(datum)),
            (BinaryOperator::NotEq, _) => Ok(!reference.equal_to(datum)),
            (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => {
                Ok(reference.less_than(datum))
            }
            (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => {
                Ok(reference.less_than_or_equal_to(datum))
            }
            (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => {
                Ok(reference.greater_than(datum))
            }
            (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => {
                Ok(reference.greater_than_or_equal_to(datum))
            }
            _ => Err(self.refuse()),
        }
    }

    fn in_list(&self, inner: &Expr, list: &[Expr], negated: bool) -> Result<Predicate> {
        let field = self.column(inner)?;
        let mut values = Vec::with_capacity(list.len());
        for item in list {
            match self.datum(item, &field)? {
                Some(datum) => values.push(datum),
                None if negated => return Err(self.refuse()),
                None => {}
            }
        }
        if values.is_empty() {
            return Err(self.refuse());
        }
        let reference = Reference::new(field.name.clone());
        if negated {
            return Ok(reference
                .clone()
                .is_not_null()
                .and(reference.is_not_in(values)));
        }
        Ok(reference.is_in(values))
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

    fn datum(&self, expr: &Expr, field: &NestedFieldRef) -> Result<Option<Datum>> {
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            return Err(self.refuse());
        };
        match unnest(expr) {
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }) => Some(None),
            Expr::Value(ValueWithSpan {
                value: Value::Number(raw, _),
                ..
            }) => number_datum(raw, primitive).map(Some),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => match unnest(inner) {
                Expr::Value(ValueWithSpan {
                    value: Value::Number(raw, _),
                    ..
                }) => number_datum(&format!("-{raw}"), primitive).map(Some),
                _ => None,
            },
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
                ..
            }) => text_datum(text, primitive).map(Some),
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(flag),
                ..
            }) if *primitive == PrimitiveType::Boolean => Some(Some(Datum::bool(*flag))),
            Expr::TypedString(typed) if typed.data_type == DataType::Date => {
                match (&typed.value.value, primitive) {
                    (Value::SingleQuotedString(text), PrimitiveType::Date) => {
                        Datum::date_from_str(text.trim()).ok().map(Some)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
        .ok_or_else(|| self.refuse())
    }
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
