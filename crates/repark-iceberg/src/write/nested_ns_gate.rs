use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, MergeAction, MergeClause, MergeInsertExpr, MergeInsertKind,
    ObjectName, Query, SelectItem, SetExpr, Value,
};
use iceberg::table::Table;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Supply {
    Absent,
    Null,
    Value,
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

#[must_use]
pub fn nested_ns_leaf(data_type: &DataType) -> Option<Vec<String>> {
    let below = |field: &Field, step: &str| {
        let naive = matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        let mut path = if naive {
            Some(Vec::new())
        } else {
            nested_ns_leaf(field.data_type())
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
pub fn nested_ns_refusal(table: &str, column: &str, leaf: &[String]) -> DataFusionError {
    let quoted = |part: &String| format!("`{}`", part.replace('`', "``"));
    let path = std::iter::once(column.to_string())
        .chain(leaf.iter().cloned())
        .collect::<Vec<_>>();
    let path = path.iter().map(quoted).collect::<Vec<_>>().join(".");
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the \
         table {table}: Cannot safely cast {path} to \"TIMESTAMP_NS\". A nested timestamp_ns \
         leaf is not writable yet: omit the column `{column}` or supply NULL for it. SQLSTATE: \
         KD000"
    ))
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_nested_ns_supply(table: &Table, label: &str, write: &NestedWrite<'_>) -> Result<()> {
    let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    for field in schema.fields() {
        let Some(leaf) = nested_ns_leaf(field.data_type()) else {
            continue;
        };
        let supply = match write {
            NestedWrite::Insert(insert) => insert_supply(insert, &names, field.name()),
            NestedWrite::Update(assignments) => assigned(assignments, field.name()),
            NestedWrite::Merge(clauses) => merged(clauses, &names, field.name()),
            NestedWrite::Unreadable => Supply::Value,
        };
        if supply == Supply::Value {
            return Err(nested_ns_refusal(label, field.name(), &leaf));
        }
    }
    Ok(())
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

fn at(found: Option<&(Option<String>, bool)>) -> Supply {
    match found {
        Some((_, true)) => Supply::Null,
        _ => Supply::Value,
    }
}

fn insert_supply(insert: &InsertSupply<'_>, fields: &[&str], column: &str) -> Supply {
    let found = insert.source.and_then(items);
    let by_position = || {
        let targets: Vec<&str> = if insert.listed.is_empty() {
            fields.to_vec()
        } else {
            insert.listed.iter().map(String::as_str).collect()
        };
        let Some(index) = targets.iter().position(|target| same(target, column)) else {
            return Supply::Absent;
        };
        match &found {
            Some(found) if !insert.partitioned => at(found.get(index)),
            _ => Supply::Value,
        }
    };
    let by_name = || {
        let Some(found) = &found else {
            return Supply::Value;
        };
        let named = found
            .iter()
            .find(|(name, _)| name.as_deref().is_some_and(|name| same(name, column)));
        match named {
            Some(named) => at(Some(named)),
            None if found.iter().any(|(name, _)| name.is_none()) => Supply::Value,
            None => Supply::Absent,
        }
    };
    match (insert.positional, insert.by_name) {
        (true, true) => by_position().max(by_name()),
        (false, true) => by_name(),
        _ => by_position(),
    }
}

fn wild(name: &str) -> bool {
    name.starts_with("__repark_")
}

fn assigned(assignments: &[Assignment], column: &str) -> Supply {
    if assignments.is_empty() {
        return Supply::Value;
    }
    assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Supply::Value;
            };
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|part| part.as_ident().map(|ident| ident.value.clone()))
                .collect::<Option<Vec<String>>>()
                .unwrap_or_default();
            if parts.is_empty() || parts.iter().any(|part| wild(part)) {
                return Supply::Value;
            }
            if !parts.iter().any(|part| same(part, column)) {
                return Supply::Absent;
            }
            let whole = parts.last().is_some_and(|part| same(part, column));
            if whole && is_null(&assignment.value) {
                Supply::Null
            } else {
                Supply::Value
            }
        })
        .max()
        .unwrap_or(Supply::Value)
}

fn inserted(insert: &MergeInsertExpr, fields: &[&str], column: &str) -> Supply {
    let listed: Option<Vec<String>> = insert.columns.iter().map(last_part).collect();
    let Some(listed) = listed else {
        return Supply::Value;
    };
    if listed.iter().any(|name| wild(name)) {
        return Supply::Value;
    }
    let targets: Vec<&str> = if listed.is_empty() {
        fields.to_vec()
    } else {
        listed.iter().map(String::as_str).collect()
    };
    let Some(index) = targets.iter().position(|target| same(target, column)) else {
        return Supply::Absent;
    };
    match &insert.kind {
        MergeInsertKind::Values(values) => {
            let null = !values.rows.is_empty()
                && values
                    .rows
                    .iter()
                    .all(|row| row.content.get(index).is_some_and(is_null));
            if null { Supply::Null } else { Supply::Value }
        }
        MergeInsertKind::Row => Supply::Value,
    }
}

fn merged(clauses: &[MergeClause], fields: &[&str], column: &str) -> Supply {
    clauses
        .iter()
        .map(|clause| match &clause.action {
            MergeAction::Update(update) => assigned(&update.assignments, column),
            MergeAction::Insert(insert) => inserted(insert, fields, column),
            MergeAction::Delete { .. } => Supply::Absent,
        })
        .max()
        .unwrap_or(Supply::Absent)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::{InsertSupply, Supply, assigned, insert_supply, merged, nested_ns_leaf};

    fn item(data_type: DataType) -> Arc<Field> {
        Arc::new(Field::new("item", data_type, true))
    }

    fn record(name: &str, data_type: DataType) -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("n", DataType::Int32, true),
            Field::new(name, data_type, true),
        ]))
    }

    fn map(key: DataType, value: DataType) -> DataType {
        let entries = DataType::Struct(Fields::from(vec![
            Field::new("key", key, false),
            Field::new("value", value, true),
        ]));
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
    }

    #[test]
    fn every_container_kind_is_searched_for_a_naive_nanosecond_leaf() {
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let found: [(DataType, &[&str]); 12] = [
            (record("v", nanos.clone()), &["v"]),
            (record("i", record("v", nanos.clone())), &["i", "v"]),
            (DataType::List(item(nanos.clone())), &["element"]),
            (DataType::LargeList(item(nanos.clone())), &["element"]),
            (DataType::ListView(item(nanos.clone())), &["element"]),
            (DataType::LargeListView(item(nanos.clone())), &["element"]),
            (
                DataType::FixedSizeList(item(nanos.clone()), 2),
                &["element"],
            ),
            (
                DataType::List(item(record("v", nanos.clone()))),
                &["element", "v"],
            ),
            (
                record("a", DataType::List(item(nanos.clone()))),
                &["a", "element"],
            ),
            (map(DataType::Utf8, nanos.clone()), &["value"]),
            (map(nanos.clone(), DataType::Int32), &["key"]),
            (
                map(DataType::Utf8, record("v", nanos.clone())),
                &["value", "v"],
            ),
        ];
        for (data_type, path) in found {
            let leaf = nested_ns_leaf(&data_type);
            let expected: Vec<String> = path.iter().map(ToString::to_string).collect();
            assert_eq!(leaf, Some(expected), "{data_type}");
        }
        let zoned = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00")));
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        for data_type in [
            nanos,
            record("v", zoned.clone()),
            record("v", micros.clone()),
            DataType::List(item(zoned.clone())),
            DataType::List(item(micros.clone())),
            map(zoned, micros),
            DataType::Int64,
        ] {
            assert_eq!(nested_ns_leaf(&data_type), None, "{data_type}");
        }
    }

    fn statement(sql: &str) -> Statement {
        let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).expect(sql);
        parsed.remove(0)
    }

    const FIELDS: [&str; 3] = ["id", "st", "k"];

    fn insert(sql: &str, by_name: bool) -> Supply {
        let Statement::Insert(insert) = statement(sql) else {
            panic!("not an insert: {sql}");
        };
        let listed: Vec<String> = insert.columns.iter().map(ToString::to_string).collect();
        let supply = InsertSupply {
            listed: &listed,
            source: insert.source.as_deref(),
            partitioned: false,
            positional: !by_name,
            by_name,
        };
        insert_supply(&supply, &FIELDS, "st")
    }

    #[test]
    fn an_insert_supplies_the_column_unless_it_omits_it_or_gives_a_bare_null() {
        for (sql, by_name, expected) in [
            ("INSERT INTO t VALUES (1, x, 0)", false, Supply::Value),
            ("INSERT INTO t VALUES (1, NULL, 0)", false, Supply::Null),
            (
                "INSERT INTO t VALUES (1, NULL, 0), (2, x, 0)",
                false,
                Supply::Value,
            ),
            (
                "INSERT INTO t VALUES (1, (NULL), 0), (2, NULL, 0)",
                false,
                Supply::Null,
            ),
            (
                "INSERT INTO t VALUES (1, CAST(NULL AS INT), 0)",
                false,
                Supply::Value,
            ),
            ("INSERT INTO t (id, k) VALUES (1, 0)", false, Supply::Absent),
            ("INSERT INTO t (id, ST) VALUES (1, x)", false, Supply::Value),
            (
                "INSERT INTO t (k, st) SELECT 1, NULL FROM s",
                false,
                Supply::Null,
            ),
            ("INSERT INTO t SELECT id, v, 0 FROM s", false, Supply::Value),
            (
                "INSERT INTO t SELECT id, NULL, 0 FROM s",
                false,
                Supply::Null,
            ),
            ("INSERT INTO t SELECT * FROM s", false, Supply::Value),
            ("INSERT INTO t SELECT id FROM s", false, Supply::Value),
            (
                "INSERT INTO t SELECT a, b, c FROM s UNION ALL SELECT a, b, c FROM s",
                false,
                Supply::Value,
            ),
            ("INSERT INTO t TABLE s", false, Supply::Value),
            (
                "INSERT INTO t SELECT 0 AS k, id FROM s",
                true,
                Supply::Absent,
            ),
            (
                "INSERT INTO t SELECT 0 AS k, NULL AS st, id FROM s",
                true,
                Supply::Null,
            ),
            (
                "INSERT INTO t SELECT 0 AS k, v AS St, id FROM s",
                true,
                Supply::Value,
            ),
            (
                "INSERT INTO t SELECT 0 AS k, s.st, id FROM s",
                true,
                Supply::Value,
            ),
            (
                "INSERT INTO t SELECT 0 AS k, f(v), id FROM s",
                true,
                Supply::Value,
            ),
            ("INSERT INTO t SELECT * FROM s", true, Supply::Value),
        ] {
            assert_eq!(insert(sql, by_name), expected, "{sql}");
        }
    }

    #[test]
    fn a_partition_clause_or_a_frame_written_by_name_is_read_as_a_value() {
        let Statement::Insert(parsed) = statement("INSERT INTO t SELECT id, NULL, 0 FROM s") else {
            panic!("not an insert");
        };
        let supply = |partitioned, positional, by_name| {
            let supply = InsertSupply {
                listed: &[],
                source: parsed.source.as_deref(),
                partitioned,
                positional,
                by_name,
            };
            insert_supply(&supply, &FIELDS, "st")
        };
        assert_eq!(supply(false, true, false), Supply::Null);
        assert_eq!(supply(true, true, false), Supply::Value);
        assert_eq!(supply(false, true, true), Supply::Value);
        let absent = InsertSupply {
            listed: &[],
            source: None,
            partitioned: false,
            positional: true,
            by_name: false,
        };
        assert_eq!(insert_supply(&absent, &FIELDS, "st"), Supply::Value);
    }

    #[test]
    fn an_assignment_supplies_the_column_when_any_part_of_its_path_names_it() {
        for (sql, expected) in [
            ("UPDATE t SET k = 1", Supply::Absent),
            ("UPDATE t SET st = x", Supply::Value),
            ("UPDATE t SET k = 1, ST = x", Supply::Value),
            ("UPDATE t SET st = NULL", Supply::Null),
            ("UPDATE t SET t.st = NULL", Supply::Null),
            ("UPDATE t SET st.v = NULL", Supply::Value),
            ("UPDATE t SET st.v = x", Supply::Value),
            ("UPDATE t SET t.st.v = x", Supply::Value),
            ("UPDATE t SET (id, st) = (1, x)", Supply::Value),
            (
                "UPDATE t SET __repark_merge_star_sentinel__ = 1",
                Supply::Value,
            ),
        ] {
            let Statement::Update(update) = statement(sql) else {
                panic!("not an update: {sql}");
            };
            assert_eq!(assigned(&update.assignments, "st"), expected, "{sql}");
        }
        assert_eq!(assigned(&[], "st"), Supply::Value);
    }

    #[test]
    fn a_merge_supplies_the_column_through_any_one_clause() {
        let on = "MERGE INTO t USING s ON t.id = s.id";
        for (clauses, expected) in [
            ("WHEN MATCHED THEN DELETE", Supply::Absent),
            ("WHEN MATCHED THEN UPDATE SET k = 1", Supply::Absent),
            ("WHEN MATCHED THEN UPDATE SET st = s.v", Supply::Value),
            ("WHEN MATCHED THEN UPDATE SET t.st.v = s.v", Supply::Value),
            ("WHEN MATCHED THEN UPDATE SET st = NULL", Supply::Null),
            (
                "WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, 0)",
                Supply::Absent,
            ),
            (
                "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
                Supply::Value,
            ),
            (
                "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, NULL)",
                Supply::Null,
            ),
            (
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v, 0)",
                Supply::Value,
            ),
            (
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, NULL, 0)",
                Supply::Null,
            ),
            (
                "WHEN NOT MATCHED THEN INSERT (__repark_merge_star_sentinel__) VALUES (1)",
                Supply::Value,
            ),
            (
                "WHEN MATCHED THEN UPDATE SET k = 1 \
                 WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
                Supply::Value,
            ),
            (
                "WHEN MATCHED THEN UPDATE SET st = NULL \
                 WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, 0)",
                Supply::Null,
            ),
        ] {
            let sql = format!("{on} {clauses}");
            let Statement::Merge(merge) = statement(&sql) else {
                panic!("not a merge: {sql}");
            };
            assert_eq!(merged(&merge.clauses, &FIELDS, "st"), expected, "{sql}");
        }
    }
}
