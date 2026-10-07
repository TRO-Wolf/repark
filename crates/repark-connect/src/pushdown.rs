use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr, Like, Operator, TableProviderFilterPushDown,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;

use crate::discover::{CastType, ResolvedSource};
use crate::ident::PgIdent;
use crate::read::postgres::{ParamSlot, ScanRequest};
use crate::types::postgres::PostgresMapping;

pub const MAX_IN_LIST: usize = 256;
pub const MIN_POSTGRES_DAYS: i64 = -2_440_588;
pub const MAX_POSTGRES_DAYS: i64 = 2_145_042_905;
pub const TEXT_COLLATION: &str = "COLLATE pg_catalog.\"C\"";
pub const UTF8_ENCODING: &str = "UTF8";

const MICROS_PER_DAY: i64 = 86_400_000_000;
const ESCAPE: char = '\\';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnClass {
    Boolean,
    Integer {
        bytes: u8,
    },
    Decimal {
        precision: u8,
        scale: i8,
        rounded: bool,
    },
    Date,
    Timestamp,
    Timestamptz,
    Text,
    NullTestOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PushColumn {
    field: String,
    ident: PgIdent,
    class: ColumnClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub sql: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pushdown {
    enabled: bool,
    columns: Vec<PushColumn>,
    schema: Option<DFSchemaRef>,
}

struct Binder {
    base: usize,
    values: Vec<String>,
}

impl Binder {
    fn bind(&mut self, text: String, cast: &str) -> Option<String> {
        let slot = ParamSlot::new(u16::try_from(self.base + self.values.len()).ok()?)?;
        self.values.push(text);
        Some(format!("{slot}::{cast}"))
    }
}

enum Value {
    Bound(String),
    Null(String),
}

impl Value {
    fn sql(&self) -> &str {
        match self {
            Value::Bound(sql) | Value::Null(sql) => sql,
        }
    }
}

impl Pushdown {
    #[must_use]
    pub fn new(resolved: &ResolvedSource, schema: &Schema, enabled: bool) -> Pushdown {
        let utf8 = resolved.server_encoding.as_ref() == UTF8_ENCODING;
        let columns = resolved
            .columns
            .iter()
            .zip(schema.fields())
            .map(|(column, field)| {
                let class = match column.planned.mapping() {
                    PostgresMapping::Boolean => ColumnClass::Boolean,
                    PostgresMapping::Int16 => ColumnClass::Integer { bytes: 2 },
                    PostgresMapping::Int32 => ColumnClass::Integer { bytes: 4 },
                    PostgresMapping::Int64 => ColumnClass::Integer { bytes: 8 },
                    PostgresMapping::Numeric(target) => ColumnClass::Decimal {
                        precision: target.precision(),
                        scale: target.scale(),
                        rounded: column.cast
                            != CastType::Numeric {
                                precision: i32::from(target.precision()),
                                scale: i32::from(target.scale()),
                            },
                    },
                    PostgresMapping::Date => ColumnClass::Date,
                    PostgresMapping::Timestamp
                        if matches!(field.data_type(), DataType::Timestamp(_, None)) =>
                    {
                        ColumnClass::Timestamp
                    }
                    PostgresMapping::Timestamptz => ColumnClass::Timestamptz,
                    PostgresMapping::Utf8
                        if utf8 && matches!(column.cast, CastType::Catalog("text" | "varchar")) =>
                    {
                        ColumnClass::Text
                    }
                    _ => ColumnClass::NullTestOnly,
                };
                PushColumn {
                    field: field.name().clone(),
                    ident: column.name.clone(),
                    class,
                }
            })
            .collect();
        let schema = DFSchema::try_from(schema.clone()).ok().map(Arc::new);
        Pushdown {
            enabled,
            columns,
            schema,
        }
    }

    #[must_use]
    pub fn class(&self, field: &str) -> Option<ColumnClass> {
        self.find(field).map(|column| column.class)
    }

    #[must_use]
    pub fn support(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if self.exact(filter) {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        }
    }

    #[must_use]
    pub fn exact(&self, filter: &Expr) -> bool {
        self.render(filter, 0).is_some() && self.settled(filter)
    }

    fn settled(&self, filter: &Expr) -> bool {
        let Some(schema) = &self.schema else {
            return false;
        };
        let bare = filter.clone().transform(|expr| {
            Ok(match expr {
                Expr::Column(column) => {
                    Transformed::yes(Expr::Column(Column::new_unqualified(column.name)))
                }
                other => Transformed::no(other),
            })
        });
        let Ok(bare) = bare.map(|bare| bare.data) else {
            return false;
        };
        let context = SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .build();
        ExprSimplifier::new(context)
            .simplify(bare.clone())
            .is_ok_and(|simplified| simplified == bare)
    }

    #[must_use]
    pub fn render(&self, filter: &Expr, base: usize) -> Option<Rendered> {
        if !self.enabled {
            return None;
        }
        let mut binder = Binder {
            base,
            values: Vec::new(),
        };
        let sql = self.expr(filter, &mut binder)?;
        Some(Rendered {
            sql,
            values: binder.values,
        })
    }

    #[must_use]
    pub fn split(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        filters
            .iter()
            .cloned()
            .partition(|filter| self.exact(filter))
    }

    #[must_use]
    pub fn push(&self, pushed: &[Expr], request: ScanRequest) -> Option<ScanRequest> {
        pushed.iter().try_fold(request, |request, filter| {
            let rendered = self.render(filter, request.bound_values())?;
            request.filter(rendered.sql, rendered.values)
        })
    }

    fn find(&self, field: &str) -> Option<&PushColumn> {
        self.columns.iter().find(|column| column.field == field)
    }

    fn column(&self, expr: &Expr) -> Option<&PushColumn> {
        match expr {
            Expr::Column(column) => self.find(&column.name),
            _ => None,
        }
    }

    fn operand(&self, expr: &Expr) -> Option<&PushColumn> {
        let Expr::Cast(cast) = expr else {
            return self.column(expr);
        };
        let column = self.column(&cast.expr)?;
        let widens = match (column.class, cast.field.data_type()) {
            (ColumnClass::Integer { bytes }, DataType::Int16) => bytes <= 2,
            (ColumnClass::Integer { bytes }, DataType::Int32) => bytes <= 4,
            (ColumnClass::Integer { .. }, DataType::Int64) => true,
            (
                ColumnClass::Decimal {
                    precision, scale, ..
                },
                DataType::Decimal128(to_p, to_s),
            ) => {
                *to_s >= scale
                    && i16::from(*to_p) - i16::from(*to_s)
                        >= i16::from(precision) - i16::from(scale)
            }
            _ => false,
        };
        widens.then_some(column)
    }

    fn expr(&self, expr: &Expr, binder: &mut Binder) -> Option<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And | Operator::Or => {
                    let left = self.expr(left, binder)?;
                    let right = self.expr(right, binder)?;
                    let word = if *op == Operator::And { "AND" } else { "OR" };
                    Some(format!("({left} {word} {right})"))
                }
                _ => self.comparison(left, *op, right, binder),
            },
            Expr::Not(inner) => Some(format!("(NOT {})", self.expr(inner, binder)?)),
            Expr::IsNull(inner) => Some(format!("({} IS NULL)", self.column(inner)?.ident)),
            Expr::IsNotNull(inner) => Some(format!("({} IS NOT NULL)", self.column(inner)?.ident)),
            Expr::IsTrue(inner) => self.truth(inner, "IS TRUE", binder),
            Expr::IsFalse(inner) => self.truth(inner, "IS FALSE", binder),
            Expr::IsUnknown(inner) => self.truth(inner, "IS UNKNOWN", binder),
            Expr::IsNotTrue(inner) => self.truth(inner, "IS NOT TRUE", binder),
            Expr::IsNotFalse(inner) => self.truth(inner, "IS NOT FALSE", binder),
            Expr::IsNotUnknown(inner) => self.truth(inner, "IS NOT UNKNOWN", binder),
            Expr::Column(_) => self
                .column(expr)
                .filter(|column| column.class == ColumnClass::Boolean)
                .map(|column| column.ident.to_string()),
            Expr::Between(between) => self.between(between, binder),
            Expr::InList(list) => self.in_list(list, binder),
            Expr::Like(like) => self.like(like, binder),
            _ => None,
        }
    }

    fn truth(&self, inner: &Expr, test: &str, binder: &mut Binder) -> Option<String> {
        Some(format!("({} {test})", self.expr(inner, binder)?))
    }

    fn comparison(
        &self,
        left: &Expr,
        op: Operator,
        right: &Expr,
        binder: &mut Binder,
    ) -> Option<String> {
        let (column, literal, op) = match (self.operand(left), left, right) {
            (Some(column), _, Expr::Literal(literal, _)) => (column, literal, op),
            (None, Expr::Literal(literal, _), _) => (self.operand(right)?, literal, flip(op)?),
            _ => return None,
        };
        let symbol = match op {
            Operator::Eq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => "=",
            Operator::NotEq => "<>",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            _ => return None,
        };
        let operand = operand_sql(column);
        let value = value(column.class, literal, binder)?;
        let compare = format!("{operand} OPERATOR(pg_catalog.{symbol}) {}", value.sql());
        Some(match (op, value) {
            (Operator::IsDistinctFrom, Value::Null(_)) => format!("({} IS NOT NULL)", column.ident),
            (Operator::IsNotDistinctFrom, Value::Null(_)) => format!("({} IS NULL)", column.ident),
            (Operator::IsDistinctFrom, Value::Bound(_)) => format!("(({compare}) IS NOT TRUE)"),
            (Operator::IsNotDistinctFrom, Value::Bound(_)) => format!("(({compare}) IS TRUE)"),
            _ => format!("({compare})"),
        })
    }

    fn between(&self, between: &Between, binder: &mut Binder) -> Option<String> {
        let low = self.comparison(&between.expr, Operator::GtEq, &between.low, binder)?;
        let high = self.comparison(&between.expr, Operator::LtEq, &between.high, binder)?;
        let range = format!("({low} AND {high})");
        Some(if between.negated {
            format!("(NOT {range})")
        } else {
            range
        })
    }

    fn in_list(&self, list: &InList, binder: &mut Binder) -> Option<String> {
        if list.list.is_empty() || list.list.len() > MAX_IN_LIST {
            return None;
        }
        let column = self.operand(&list.expr)?;
        let operand = operand_sql(column);
        let items = list
            .list
            .iter()
            .map(|item| match item {
                Expr::Literal(literal, _) => {
                    let value = value(column.class, literal, binder)?;
                    Some(format!("{operand} OPERATOR(pg_catalog.=) {}", value.sql()))
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let any = format!("({})", items.join(" OR "));
        Some(if list.negated {
            format!("(NOT {any})")
        } else {
            any
        })
    }

    fn like(&self, like: &Like, binder: &mut Binder) -> Option<String> {
        if like.case_insensitive || !matches!(like.escape_char, None | Some(ESCAPE)) {
            return None;
        }
        let column = self
            .column(&like.expr)
            .filter(|column| column.class == ColumnClass::Text)?;
        let Expr::Literal(
            ScalarValue::Utf8(Some(pattern))
            | ScalarValue::LargeUtf8(Some(pattern))
            | ScalarValue::Utf8View(Some(pattern)),
            _,
        ) = like.pattern.as_ref()
        else {
            return None;
        };
        if !well_formed(pattern) {
            return None;
        }
        let pattern = binder.bind(pattern.clone(), "pg_catalog.text")?;
        let op = if like.negated { "!~~" } else { "~~" };
        Some(format!(
            "({} {TEXT_COLLATION} OPERATOR(pg_catalog.{op}) {pattern})",
            column.ident
        ))
    }
}

fn flip(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Eq | Operator::NotEq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => {
            op
        }
        _ => return None,
    })
}

fn operand_sql(column: &PushColumn) -> String {
    match column.class {
        ColumnClass::Text => format!("{} {TEXT_COLLATION}", column.ident),
        ColumnClass::Decimal {
            precision,
            scale,
            rounded: true,
        } => format!("{}::pg_catalog.numeric({precision},{scale})", column.ident),
        _ => column.ident.to_string(),
    }
}

fn value(class: ColumnClass, literal: &ScalarValue, binder: &mut Binder) -> Option<Value> {
    let integer = |value: Option<i64>| value.map(|value| value.to_string());
    let (text, cast) = match (class, literal) {
        (ColumnClass::Boolean, ScalarValue::Boolean(value)) => {
            (value.map(|value| value.to_string()), "pg_catalog.bool")
        }
        (ColumnClass::Integer { .. }, ScalarValue::Int8(value)) => {
            (integer(value.map(i64::from)), "pg_catalog.int2")
        }
        (ColumnClass::Integer { .. }, ScalarValue::Int16(value)) => {
            (integer(value.map(i64::from)), "pg_catalog.int2")
        }
        (ColumnClass::Integer { .. }, ScalarValue::Int32(value)) => {
            (integer(value.map(i64::from)), "pg_catalog.int4")
        }
        (ColumnClass::Integer { .. }, ScalarValue::Int64(value)) => {
            (integer(*value), "pg_catalog.int8")
        }
        (ColumnClass::Decimal { .. }, ScalarValue::Decimal128(value, _, scale)) => (
            value.map(|value| decimal_text(value, *scale)),
            "pg_catalog.numeric",
        ),
        (ColumnClass::Date, ScalarValue::Date32(value)) => {
            let days = value.map(i64::from);
            if days.is_some_and(|days| !(MIN_POSTGRES_DAYS..=MAX_POSTGRES_DAYS).contains(&days)) {
                return None;
            }
            (days.map(date_text), "pg_catalog.date")
        }
        (ColumnClass::Timestamp, ScalarValue::TimestampMicrosecond(value, None)) => {
            let text = match value {
                Some(micros) => Some(timestamp_text(*micros, "")?),
                None => None,
            };
            (text, "pg_catalog.timestamp")
        }
        (ColumnClass::Timestamptz, ScalarValue::TimestampMicrosecond(value, Some(_))) => {
            let text = match value {
                Some(micros) => Some(timestamp_text(*micros, "+00")?),
                None => None,
            };
            (text, "pg_catalog.timestamptz")
        }
        (
            ColumnClass::Text,
            ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value),
        ) => {
            if value.as_ref().is_some_and(|text| text.contains('\0')) {
                return None;
            }
            (value.clone(), "pg_catalog.text")
        }
        _ => return None,
    };
    Some(match text {
        Some(text) => Value::Bound(binder.bind(text, cast)?),
        None => Value::Null(format!("NULL::{cast}")),
    })
}

fn well_formed(pattern: &str) -> bool {
    if pattern.contains('\0') {
        return false;
    }
    let mut chars = pattern.chars();
    while let Some(next) = chars.next() {
        if next == ESCAPE && !matches!(chars.next(), Some('%' | '_' | ESCAPE)) {
            return false;
        }
    }
    true
}

#[must_use]
pub fn decimal_text(value: i128, scale: i8) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let digits = value.unsigned_abs().to_string();
    let Ok(scale) = usize::try_from(scale) else {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{sign}{digits}{zeros}");
    };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

fn civil(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn ymd(days: i64) -> (String, &'static str) {
    let (year, month, day) = civil(days);
    if year <= 0 {
        (format!("{:04}-{month:02}-{day:02}", 1 - year), " BC")
    } else {
        (format!("{year:04}-{month:02}-{day:02}"), "")
    }
}

#[must_use]
pub fn date_text(days: i64) -> String {
    let (date, era) = ymd(days);
    format!("{date}{era}")
}

#[must_use]
pub fn timestamp_text(micros: i64, zone: &str) -> Option<String> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    if days < MIN_POSTGRES_DAYS {
        return None;
    }
    let within = micros.rem_euclid(MICROS_PER_DAY);
    let (date, era) = ymd(days);
    let (hour, minute) = (within / 3_600_000_000, within / 60_000_000 % 60);
    let (second, fraction) = (within / 1_000_000 % 60, within % 1_000_000);
    Some(format!(
        "{date} {hour:02}:{minute:02}:{second:02}.{fraction:06}{zone}{era}"
    ))
}
