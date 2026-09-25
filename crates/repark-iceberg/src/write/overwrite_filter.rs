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
    OutOfRange { above: bool, decimal: bool },
    Fractional { floor: Datum, ceil: Datum },
    Boundary { datum: Datum, at_max: bool },
}

enum Folded {
    Leaf { predicate: Predicate, text: String },
    Opaque(String),
    Null,
    FalseIfNotNull(String),
    TrueIfNotNull(String),
    And(Box<Folded>, Box<Folded>),
    Or(Box<Folded>, Box<Folded>),
}

#[allow(clippy::missing_errors_doc)]
pub fn spark_overwrite_filter(expr: &Expr, schema: &Schema) -> Result<Predicate> {
    let converter = Converter { schema };
    let folded = converter.lower(expr, false, 0)?;
    let mut conjuncts = Vec::new();
    split_conjuncts(folded, &mut conjuncts);
    let mut predicate = Predicate::AlwaysTrue;
    for conjunct in &conjuncts {
        let iceberg = conjunct
            .predicate()
            .ok_or_else(|| cannot_convert(conjunct.render()))?;
        predicate = predicate.and(iceberg);
    }
    Ok(predicate)
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

fn split_conjuncts(folded: Folded, out: &mut Vec<Folded>) {
    match folded {
        Folded::And(left, right) => {
            split_conjuncts(*left, out);
            split_conjuncts(*right, out);
        }
        Folded::FalseIfNotNull(column) => {
            out.push(Folded::Leaf {
                predicate: Reference::new(column.clone()).is_null(),
                text: format!("{column} IS NULL"),
            });
            out.push(Folded::Null);
        }
        other => out.push(other),
    }
}

impl Folded {
    fn constant(value: bool) -> Self {
        Self::Leaf {
            predicate: if value {
                Predicate::AlwaysTrue
            } else {
                Predicate::AlwaysFalse
            },
            text: value.to_string(),
        }
    }

    fn leaf(predicate: Predicate, text: String) -> Self {
        Self::Leaf { predicate, text }
    }

    fn constant_value(&self) -> Option<bool> {
        match self {
            Self::Leaf {
                predicate: Predicate::AlwaysTrue,
                ..
            } => Some(true),
            Self::Leaf {
                predicate: Predicate::AlwaysFalse,
                ..
            } => Some(false),
            _ => None,
        }
    }

    fn and(self, other: Self) -> Self {
        match (self.constant_value(), other.constant_value()) {
            (Some(false), _) | (_, Some(false)) => Self::constant(false),
            (Some(true), _) => other,
            (_, Some(true)) => self,
            _ => Self::And(Box::new(self), Box::new(other)),
        }
    }

    fn or(self, other: Self) -> Self {
        match (self.constant_value(), other.constant_value()) {
            (Some(true), _) | (_, Some(true)) => Self::constant(true),
            (Some(false), _) => other,
            (_, Some(false)) => self,
            _ => Self::Or(Box::new(self), Box::new(other)),
        }
    }

    fn predicate(&self) -> Option<Predicate> {
        match self {
            Self::Leaf { predicate, .. } => Some(predicate.clone()),
            Self::And(left, right) => Some(left.predicate()?.and(right.predicate()?)),
            Self::Or(left, right) => Some(left.predicate()?.or(right.predicate()?)),
            Self::Opaque(_) | Self::Null | Self::FalseIfNotNull(_) | Self::TrueIfNotNull(_) => None,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Leaf { text, .. } | Self::Opaque(text) => text.clone(),
            Self::Null => "null".to_string(),
            Self::FalseIfNotNull(column) => format!("({column} IS NULL) AND (null)"),
            Self::TrueIfNotNull(column) => format!("({column} IS NOT NULL) OR (null)"),
            Self::And(left, right) => format!("({}) AND ({})", left.render(), right.render()),
            Self::Or(left, right) => format!("({}) OR ({})", left.render(), right.render()),
        }
    }

    fn negated(self) -> Self {
        match self {
            Self::Leaf { predicate, text } => match predicate {
                Predicate::AlwaysTrue => Self::constant(false),
                Predicate::AlwaysFalse => Self::constant(true),
                other => Self::leaf(!other, format!("NOT ({text})")),
            },
            Self::Opaque(text) => Self::Opaque(format!("NOT ({text})")),
            Self::Null => Self::Null,
            Self::FalseIfNotNull(column) => Self::TrueIfNotNull(column),
            Self::TrueIfNotNull(column) => Self::FalseIfNotNull(column),
            Self::And(left, right) => left.negated().or(right.negated()),
            Self::Or(left, right) => left.negated().and(right.negated()),
        }
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

    fn symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::NullSafe | Self::NotNullSafe => "<=>",
        }
    }

    fn holds_for_every_value_below(self, above: bool) -> bool {
        if above {
            matches!(self, Self::Lt | Self::LtEq | Self::NotEq)
        } else {
            matches!(self, Self::Gt | Self::GtEq | Self::NotEq)
        }
    }
}

fn boundary(
    comparison: Comparison,
    at_max: bool,
    reference: Reference,
    datum: Datum,
    name: String,
    equal: String,
) -> Folded {
    match (comparison, at_max) {
        (Comparison::Gt, true) | (Comparison::Lt, false) => Folded::FalseIfNotNull(name),
        (Comparison::LtEq, true) | (Comparison::GtEq, false) => Folded::TrueIfNotNull(name),
        (Comparison::Eq | Comparison::NullSafe | Comparison::GtEq | Comparison::LtEq, _) => {
            Folded::leaf(reference.equal_to(datum), equal)
        }
        (Comparison::NotEq | Comparison::NotNullSafe | Comparison::Lt | Comparison::Gt, _) => {
            Folded::leaf(!reference.equal_to(datum), format!("NOT ({equal})"))
        }
    }
}

impl Converter<'_> {
    fn lower(&self, expr: &Expr, negated: bool, depth: usize) -> Result<Folded> {
        let opaque = || {
            Folded::Opaque(if negated {
                format!("NOT ({expr})")
            } else {
                expr.to_string()
            })
        };
        if depth >= MAX_FILTER_DEPTH {
            return Ok(opaque());
        }
        match expr {
            Expr::Nested(inner) => self.lower(inner, negated, depth + 1),
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(flag),
                ..
            }) => Ok(Folded::constant(*flag ^ negated)),
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }) => Ok(Folded::Null),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => self.lower(inner, !negated, depth + 1),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                let Some(field) = self.column_of(inner)? else {
                    return Ok(opaque());
                };
                let reference = Reference::new(field.name.clone());
                Ok(if matches!(expr, Expr::IsNull(_)) ^ negated {
                    Folded::leaf(reference.is_null(), format!("{} IS NULL", field.name))
                } else {
                    Folded::leaf(
                        reference.is_not_null(),
                        format!("{} IS NOT NULL", field.name),
                    )
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated: listed_negated,
            } => Ok(self
                .in_list(inner, list, *listed_negated ^ negated)?
                .unwrap_or_else(opaque)),
            Expr::Between {
                expr: inner,
                negated: listed_negated,
                low,
                high,
            } => {
                let folded = if *listed_negated ^ negated {
                    self.comparison(inner, Comparison::Lt, low)?
                        .zip(self.comparison(inner, Comparison::Gt, high)?)
                        .map(|(below, above)| below.or(above))
                } else {
                    self.comparison(inner, Comparison::GtEq, low)?
                        .zip(self.comparison(inner, Comparison::LtEq, high)?)
                        .map(|(above, below)| above.and(below))
                };
                Ok(folded.unwrap_or_else(opaque))
            }
            Expr::Like {
                negated: listed_negated,
                any: false,
                expr: inner,
                pattern,
                escape_char: None,
            } => Ok(match self.like(inner, pattern)? {
                Some(like) if *listed_negated ^ negated => like.negated(),
                Some(like) => like,
                None => opaque(),
            }),
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    let left = self.lower(left, negated, depth + 1)?;
                    let right = self.lower(right, negated, depth + 1)?;
                    Ok(if matches!(op, BinaryOperator::And) ^ negated {
                        left.and(right)
                    } else {
                        left.or(right)
                    })
                }
                _ => {
                    let Some(comparison) = Comparison::of(op) else {
                        return Ok(opaque());
                    };
                    let comparison = if negated {
                        comparison.negated()
                    } else {
                        comparison
                    };
                    Ok(self
                        .comparison(left, comparison, right)?
                        .unwrap_or_else(opaque))
                }
            },
            _ => Ok(opaque()),
        }
    }

    fn column_of(&self, expr: &Expr) -> Result<Option<NestedFieldRef>> {
        let Expr::Identifier(ident) = unnest(expr) else {
            return Ok(None);
        };
        let fields = self.schema.as_struct().fields();
        if let Some(field) = fields.iter().find(|field| field.name == ident.value) {
            return Ok(match field.field_type.as_ref() {
                Type::Primitive(_) => Some(Arc::clone(field)),
                _ => None,
            });
        }
        if fields
            .iter()
            .any(|field| field.name.eq_ignore_ascii_case(&ident.value))
        {
            return Err(missing_field(ident, self.schema));
        }
        Ok(None)
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

    fn comparison(
        &self,
        left: &Expr,
        comparison: Comparison,
        right: &Expr,
    ) -> Result<Option<Folded>> {
        let Some((field, literal, comparison)) = self.operands(left, comparison, right)? else {
            return Ok(None);
        };
        let Some(literal_value) = Self::literal(literal, &field) else {
            return Ok(None);
        };
        let name = field.name.clone();
        let reference = Reference::new(name.clone());
        let text = |value: &dyn std::fmt::Display, comparison: Comparison| {
            format!("{name} {} {value}", comparison.symbol())
        };
        Ok(Some(match literal_value {
            Literal::Null => match comparison {
                Comparison::NullSafe => {
                    Folded::leaf(reference.is_null(), format!("{name} IS NULL"))
                }
                Comparison::NotNullSafe => {
                    Folded::leaf(reference.is_not_null(), format!("{name} IS NOT NULL"))
                }
                _ => Folded::Null,
            },
            Literal::OutOfRange { above, decimal } => match comparison {
                Comparison::NullSafe => Folded::constant(false),
                Comparison::NotNullSafe => Folded::constant(true),
                Comparison::Eq => Folded::FalseIfNotNull(name),
                Comparison::NotEq => Folded::TrueIfNotNull(name),
                range => {
                    let holds = range.holds_for_every_value_below(above);
                    match (decimal, holds) {
                        (true, holds) => Folded::constant(holds),
                        (false, true) => Folded::TrueIfNotNull(name),
                        (false, false) => Folded::FalseIfNotNull(name),
                    }
                }
            },
            Literal::Fractional { floor, ceil } => match comparison {
                Comparison::NullSafe => Folded::constant(false),
                Comparison::NotNullSafe => Folded::constant(true),
                Comparison::Eq => Folded::FalseIfNotNull(name),
                Comparison::NotEq => Folded::TrueIfNotNull(name),
                Comparison::Lt | Comparison::LtEq => Folded::leaf(
                    reference
                        .clone()
                        .is_not_null()
                        .and(reference.less_than_or_equal_to(floor.clone())),
                    text(&floor, Comparison::LtEq),
                ),
                Comparison::Gt | Comparison::GtEq => Folded::leaf(
                    reference.greater_than_or_equal_to(ceil.clone()),
                    text(&ceil, Comparison::GtEq),
                ),
            },
            Literal::Boundary { datum, at_max } => {
                let equal = text(literal, Comparison::Eq);
                boundary(comparison, at_max, reference, datum, name, equal)
            }
            Literal::Value(datum) => {
                let rendered = text(literal, comparison);
                match comparison {
                    Comparison::Eq | Comparison::NullSafe => {
                        Folded::leaf(reference.equal_to(datum), text(literal, Comparison::Eq))
                    }
                    Comparison::NotEq | Comparison::NotNullSafe => Folded::leaf(
                        !reference.equal_to(datum),
                        format!("NOT ({})", text(literal, Comparison::Eq)),
                    ),
                    Comparison::Lt => Folded::leaf(
                        reference
                            .clone()
                            .is_not_null()
                            .and(reference.less_than(datum)),
                        rendered,
                    ),
                    Comparison::LtEq => Folded::leaf(
                        reference
                            .clone()
                            .is_not_null()
                            .and(reference.less_than_or_equal_to(datum)),
                        rendered,
                    ),
                    Comparison::Gt => Folded::leaf(reference.greater_than(datum), rendered),
                    Comparison::GtEq => {
                        Folded::leaf(reference.greater_than_or_equal_to(datum), rendered)
                    }
                }
            }
        }))
    }

    fn in_list(&self, inner: &Expr, list: &[Expr], negated: bool) -> Result<Option<Folded>> {
        let Some(field) = self.column_of(inner)? else {
            return Ok(None);
        };
        let mut values: Vec<(Datum, &Expr)> = Vec::with_capacity(list.len());
        let mut has_null = false;
        for item in list {
            match Self::literal(item, &field) {
                Some(Literal::Value(datum) | Literal::Boundary { datum, .. }) => {
                    if !values.iter().any(|(seen, _)| *seen == datum) {
                        values.push((datum, item));
                    }
                }
                Some(Literal::Null) => has_null = true,
                Some(Literal::OutOfRange { .. } | Literal::Fractional { .. }) => {}
                None => return Ok(None),
            }
        }
        let name = field.name.clone();
        let reference = Reference::new(name.clone());
        let folded = match (values.len(), has_null) {
            (0, true) => Folded::Null,
            (0, false) => {
                let unreachable = Folded::FalseIfNotNull(name);
                return Ok(Some(if negated {
                    unreachable.negated()
                } else {
                    unreachable
                }));
            }
            (1, false) => {
                let (datum, item) = values.remove(0);
                Folded::leaf(reference.equal_to(datum), format!("{name} = {item}"))
            }
            _ => {
                let items = values
                    .iter()
                    .map(|(_, item)| item.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let datums: Vec<Datum> = values.into_iter().map(|(datum, _)| datum).collect();
                if negated {
                    return Ok(Some(Folded::leaf(
                        reference
                            .clone()
                            .is_not_null()
                            .and(reference.is_not_in(datums)),
                        format!("NOT ({name} IN ({items}))"),
                    )));
                }
                Folded::leaf(reference.is_in(datums), format!("{name} IN ({items})"))
            }
        };
        Ok(Some(if negated { folded.negated() } else { folded }))
    }

    fn like(&self, inner: &Expr, pattern: &Expr) -> Result<Option<Folded>> {
        let Some(field) = self.column_of(inner)? else {
            return Ok(None);
        };
        if !matches!(
            field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::String)
        ) {
            return Ok(None);
        }
        let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) = unnest(pattern)
        else {
            return Ok(None);
        };
        let reference = Reference::new(field.name.clone());
        let wildcard = |text: &str| text.contains(['%', '_', '\\']);
        if !wildcard(text) {
            return Ok(Some(Folded::leaf(
                reference.equal_to(Datum::string(text)),
                format!("{} = {pattern}", field.name),
            )));
        }
        Ok(match text.strip_suffix('%') {
            Some(prefix) if !prefix.is_empty() && !wildcard(prefix) => Some(Folded::leaf(
                reference.starts_with(Datum::string(prefix)),
                format!("{} LIKE {pattern}", field.name),
            )),
            _ => None,
        })
    }

    fn literal(expr: &Expr, field: &NestedFieldRef) -> Option<Literal> {
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            return None;
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
    }
}

fn number_literal(raw: &str, primitive: &PrimitiveType) -> Option<Literal> {
    let long = match primitive {
        PrimitiveType::Int => false,
        PrimitiveType::Long => true,
        _ => return number_datum(raw, primitive).map(Literal::Value),
    };
    let negative = raw.starts_with('-');
    let unsigned = raw.strip_prefix('-').unwrap_or(raw);
    let written_decimal = unsigned.contains('.');
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return None;
    }
    let out_of_range = |decimal: bool| Literal::OutOfRange {
        above: !negative,
        decimal,
    };
    let Ok(magnitude) = whole.parse::<i128>() else {
        return Some(out_of_range(true));
    };
    let truncated = if negative { -magnitude } else { magnitude };
    let (low, high) = if long {
        (i128::from(i64::MIN), i128::from(i64::MAX))
    } else {
        (i128::from(i32::MIN), i128::from(i32::MAX))
    };
    let beyond_i64 = truncated < i128::from(i64::MIN) || truncated > i128::from(i64::MAX);
    let decimal = long || beyond_i64;
    if fraction.bytes().all(|byte| byte == b'0') {
        if truncated < low || truncated > high {
            return Some(out_of_range(decimal));
        }
        let datum = integer_datum(truncated, primitive)?;
        if written_decimal && !long && (truncated == low || truncated == high) {
            return Some(Literal::Boundary {
                datum,
                at_max: truncated == high,
            });
        }
        return Some(Literal::Value(datum));
    }
    let (floor, ceil) = if negative {
        (truncated - 1, truncated)
    } else {
        (truncated, truncated + 1)
    };
    if floor < low || ceil > high {
        return Some(out_of_range(decimal));
    }
    Some(Literal::Fractional {
        floor: integer_datum(floor, primitive)?,
        ceil: integer_datum(ceil, primitive)?,
    })
}

fn integer_datum(value: i128, primitive: &PrimitiveType) -> Option<Datum> {
    match primitive {
        PrimitiveType::Int => i32::try_from(value).ok().map(Datum::int),
        PrimitiveType::Long => i64::try_from(value).ok().map(Datum::long),
        _ => None,
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
