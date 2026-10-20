use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, MergeAction, MergeClause, MergeInsertExpr, MergeInsertKind,
    ObjectName, Query, SelectItem, SetExpr, Value,
};
use iceberg::table::Table;

const MICROS_HINT: &str = "A value that is not provably a TIMESTAMP_NTZ wall time is not written \
                           into a nested TIMESTAMP_NTZ field while the session time zone is not \
                           UTC: cast the leaf itself, as in named_struct('<field>', CAST(<value> \
                           AS TIMESTAMP_NTZ), ...) or array(CAST(<value> AS TIMESTAMP_NTZ)), to \
                           store the session wall time (a CAST of the whole struct, array or map \
                           is refused, and in an UPDATE assign the field itself), or set \
                           spark.sql.session.timeZone to UTC for the statement.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NaiveLeaf {
    Nanos,
    Micros,
}

impl NaiveLeaf {
    fn unit(self) -> TimeUnit {
        match self {
            Self::Nanos => TimeUnit::Nanosecond,
            Self::Micros => TimeUnit::Microsecond,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Supply {
    Absent,
    Null,
    Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Feed<'a> {
    Expr {
        value: &'a Expr,
        path: Vec<String>,
    },
    SourceColumn {
        position: Option<usize>,
        name: Option<String>,
    },
    SourceStar,
    Opaque,
}

#[derive(Clone, Debug, PartialEq)]
struct Supplied<'a> {
    supply: Supply,
    feeds: Vec<Feed<'a>>,
}

impl<'a> Supplied<'a> {
    fn absent() -> Self {
        Self {
            supply: Supply::Absent,
            feeds: Vec::new(),
        }
    }

    fn null() -> Self {
        Self {
            supply: Supply::Null,
            feeds: Vec::new(),
        }
    }

    fn value(feed: Feed<'a>) -> Self {
        Self {
            supply: Supply::Value,
            feeds: vec![feed],
        }
    }

    fn join(mut self, other: Self) -> Self {
        self.supply = self.supply.max(other.supply);
        self.feeds.extend(other.feeds);
        self
    }
}

pub struct InsertSupply<'a> {
    pub listed: &'a [String],
    pub source: Option<&'a Query>,
    pub partitioned: bool,
    pub positional: bool,
    pub by_name: bool,
}

pub enum NestedWrite<'a> {
    Insert(InsertSupply<'a>),
    Update(&'a [Assignment]),
    Merge(&'a [MergeClause]),
    Unreadable,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NestedSupply<'a> {
    pub column: String,
    pub data_type: DataType,
    pub leaf: Vec<String>,
    pub feeds: Vec<Feed<'a>>,
}

fn is_naive(data_type: &DataType, leaf: NaiveLeaf) -> bool {
    matches!(data_type, DataType::Timestamp(unit, None) if *unit == leaf.unit())
}

#[must_use]
pub fn nested_naive_leaf(data_type: &DataType, leaf: NaiveLeaf) -> Option<Vec<String>> {
    let below = |field: &Field, step: &str| {
        let mut path = if is_naive(field.data_type(), leaf) {
            Some(Vec::new())
        } else {
            nested_naive_leaf(field.data_type(), leaf)
        }?;
        path.insert(0, step.to_string());
        Some(path)
    };
    match data_type {
        DataType::Struct(fields) => fields.iter().find_map(|field| below(field, field.name())),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => {
                below(&pair[0], "key").or_else(|| below(&pair[1], "value"))
            }
            _ => None,
        },
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => below(field, "element"),
        _ => None,
    }
}

#[must_use]
pub fn holds_zoned_timestamp(data_type: &DataType) -> bool {
    holds_timestamp(data_type, true)
}

#[must_use]
pub fn holds_naive_timestamp(data_type: &DataType) -> bool {
    holds_timestamp(data_type, false)
}

#[must_use]
pub fn casts_zoned_to_naive(source: &DataType, target: &DataType) -> bool {
    holds_zoned_timestamp(source) && holds_naive_timestamp(target)
}

fn holds_timestamp(data_type: &DataType, zoned: bool) -> bool {
    match data_type {
        DataType::Timestamp(_, zone) => zone.is_some() == zoned,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| holds_timestamp(field.data_type(), zoned)),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| holds_timestamp(field.data_type(), zoned)),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => holds_timestamp(field.data_type(), zoned),
        DataType::Dictionary(_, values) => holds_timestamp(values, zoned),
        DataType::RunEndEncoded(_, values) => holds_timestamp(values.data_type(), zoned),
        _ => false,
    }
}

fn reaches_leaf(data_type: &DataType, path: &[String], leaf: NaiveLeaf) -> bool {
    let Some((step, rest)) = path.split_first() else {
        return is_naive(data_type, leaf) || nested_naive_leaf(data_type, leaf).is_some();
    };
    let DataType::Struct(fields) = data_type else {
        return true;
    };
    fields
        .iter()
        .find(|field| same(field.name(), step))
        .is_none_or(|field| reaches_leaf(field.data_type(), rest, leaf))
}

#[must_use]
pub fn lands_on_naive_leaf(data_type: &DataType, path: &[String], leaf: NaiveLeaf) -> bool {
    let Some((step, rest)) = path.split_first() else {
        return is_naive(data_type, leaf);
    };
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    fields
        .iter()
        .find(|field| same(field.name(), step))
        .is_some_and(|field| lands_on_naive_leaf(field.data_type(), rest, leaf))
}

#[must_use]
pub fn nested_naive_refusal(
    table: &str,
    column: &str,
    leaf: &[String],
    unit: NaiveLeaf,
) -> DataFusionError {
    let quoted = |part: &String| format!("`{}`", part.replace('`', "``"));
    let path = std::iter::once(column.to_string())
        .chain(leaf.iter().cloned())
        .collect::<Vec<_>>();
    let path = path.iter().map(quoted).collect::<Vec<_>>().join(".");
    let (target, reason) = match unit {
        NaiveLeaf::Nanos => (
            "TIMESTAMP_NS",
            format!(
                "A nested timestamp_ns leaf is not writable yet: omit the column `{column}` or \
                 supply NULL for it."
            ),
        ),
        NaiveLeaf::Micros => ("TIMESTAMP_NTZ", MICROS_HINT.to_string()),
    };
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the \
         table {table}: Cannot safely cast {path} to \"{target}\". {reason} SQLSTATE: KD000"
    ))
}

#[allow(clippy::missing_errors_doc)]
pub fn nested_supplies<'a>(
    table: &Table,
    write: &NestedWrite<'a>,
    leaf: NaiveLeaf,
) -> Result<Vec<NestedSupply<'a>>> {
    let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let mut answered = Vec::new();
    for field in schema.fields() {
        let Some(path) = nested_naive_leaf(field.data_type(), leaf) else {
            continue;
        };
        let supplied = match write {
            NestedWrite::Insert(insert) => insert_supply(insert, &names, field.name()),
            NestedWrite::Update(assignments) => assigned(assignments, field.name()),
            NestedWrite::Merge(clauses) => merged(clauses, &names, field.name()),
            NestedWrite::Unreadable => Supplied::value(Feed::Opaque),
        };
        if supplied.supply != Supply::Value {
            continue;
        }
        let feeds = supplied
            .feeds
            .into_iter()
            .filter(|feed| match feed {
                Feed::Expr { path: into, .. } => reaches_leaf(field.data_type(), into, leaf),
                _ => true,
            })
            .collect();
        answered.push(NestedSupply {
            column: field.name().clone(),
            data_type: field.data_type().clone(),
            leaf: path,
            feeds,
        });
    }
    Ok(answered)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_nested_ns_supply(table: &Table, label: &str, write: &NestedWrite<'_>) -> Result<()> {
    match nested_supplies(table, write, NaiveLeaf::Nanos)?.first() {
        Some(supply) => Err(nested_naive_refusal(
            label,
            &supply.column,
            &supply.leaf,
            NaiveLeaf::Nanos,
        )),
        None => Ok(()),
    }
}

fn same(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Value(value) => matches!(value.value, Value::Null),
        Expr::Nested(inner) => is_null(inner),
        _ => false,
    }
}

fn last_part(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
}

fn items(query: &Query) -> Option<Vec<(Option<String>, bool)>> {
    match query.body.as_ref() {
        SetExpr::Select(select) => select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) => {
                    let name = match expr {
                        Expr::Identifier(ident) => Some(ident.value.clone()),
                        Expr::CompoundIdentifier(parts) => {
                            parts.last().map(|ident| ident.value.clone())
                        }
                        _ => None,
                    };
                    Some((name, is_null(expr)))
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    Some((Some(alias.value.clone()), is_null(expr)))
                }
                _ => None,
            })
            .collect(),
        SetExpr::Values(values) => {
            let width = values.rows.first()?.content.len();
            let ragged = values.rows.iter().any(|row| row.content.len() != width);
            (!ragged).then(|| {
                (0..width)
                    .map(|index| {
                        let null = values.rows.iter().all(|row| is_null(&row.content[index]));
                        (None, null)
                    })
                    .collect()
            })
        }
        _ => None,
    }
}

fn at<'a>(found: Option<&(Option<String>, bool)>, feed: Feed<'a>) -> Supplied<'a> {
    match found {
        Some((_, true)) => Supplied::null(),
        _ => Supplied::value(feed),
    }
}

fn insert_supply<'a>(insert: &InsertSupply<'_>, fields: &[&str], column: &str) -> Supplied<'a> {
    let found = insert.source.and_then(items);
    let by_position = || {
        let targets: Vec<&str> = if insert.listed.is_empty() {
            fields.to_vec()
        } else {
            insert.listed.iter().map(String::as_str).collect()
        };
        let Some(index) = targets.iter().position(|target| same(target, column)) else {
            return Supplied::absent();
        };
        let feed = Feed::SourceColumn {
            position: (!insert.partitioned).then_some(index),
            name: None,
        };
        match &found {
            Some(found) if !insert.partitioned => at(found.get(index), feed),
            _ => Supplied::value(feed),
        }
    };
    let by_name = || {
        let feed = Feed::SourceColumn {
            position: None,
            name: Some(column.to_string()),
        };
        let Some(found) = &found else {
            return Supplied::value(feed);
        };
        let named = found
            .iter()
            .find(|(name, _)| name.as_deref().is_some_and(|name| same(name, column)));
        match named {
            Some(named) => at(Some(named), feed),
            None if found.iter().any(|(name, _)| name.is_none()) => Supplied::value(feed),
            None => Supplied::absent(),
        }
    };
    match (insert.positional, insert.by_name) {
        (true, true) => by_position().join(by_name()),
        (false, true) => by_name(),
        _ => by_position(),
    }
}

fn wild(name: &str) -> bool {
    name.starts_with("__repark_")
}

fn assigned<'a>(assignments: &'a [Assignment], column: &str) -> Supplied<'a> {
    if assignments.is_empty() {
        return Supplied::value(Feed::Opaque);
    }
    assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Supplied::value(Feed::Opaque);
            };
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|part| part.as_ident().map(|ident| ident.value.clone()))
                .collect::<Option<Vec<String>>>()
                .unwrap_or_default();
            if parts.is_empty() {
                return Supplied::value(Feed::Opaque);
            }
            if parts.iter().any(|part| wild(part)) {
                return Supplied::value(Feed::SourceStar);
            }
            let Some(named) = parts.iter().position(|part| same(part, column)) else {
                return Supplied::absent();
            };
            let whole = parts.last().is_some_and(|part| same(part, column));
            if whole && is_null(&assignment.value) {
                Supplied::null()
            } else {
                Supplied::value(Feed::Expr {
                    value: &assignment.value,
                    path: parts[named + 1..].to_vec(),
                })
            }
        })
        .fold(Supplied::absent(), Supplied::join)
}

fn inserted<'a>(insert: &'a MergeInsertExpr, fields: &[&str], column: &str) -> Supplied<'a> {
    let listed: Option<Vec<String>> = insert.columns.iter().map(last_part).collect();
    let Some(listed) = listed else {
        return Supplied::value(Feed::Opaque);
    };
    if listed.iter().any(|name| wild(name)) {
        return Supplied::value(Feed::SourceStar);
    }
    let targets: Vec<&str> = if listed.is_empty() {
        fields.to_vec()
    } else {
        listed.iter().map(String::as_str).collect()
    };
    let Some(index) = targets.iter().position(|target| same(target, column)) else {
        return Supplied::absent();
    };
    match &insert.kind {
        MergeInsertKind::Values(values) if !values.rows.is_empty() => values
            .rows
            .iter()
            .map(|row| match row.content.get(index) {
                Some(value) if is_null(value) => Supplied::null(),
                Some(value) => Supplied::value(Feed::Expr {
                    value,
                    path: Vec::new(),
                }),
                None => Supplied::value(Feed::Opaque),
            })
            .fold(Supplied::absent(), Supplied::join),
        MergeInsertKind::Values(_) | MergeInsertKind::Row => Supplied::value(Feed::Opaque),
    }
}

fn merged<'a>(clauses: &'a [MergeClause], fields: &[&str], column: &str) -> Supplied<'a> {
    clauses
        .iter()
        .map(|clause| match &clause.action {
            MergeAction::Update(update) => assigned(&update.assignments, column),
            MergeAction::Insert(insert) => inserted(insert, fields, column),
            MergeAction::Delete { .. } => Supplied::absent(),
        })
        .fold(Supplied::absent(), Supplied::join)
}

#[cfg(test)]
mod tests;
