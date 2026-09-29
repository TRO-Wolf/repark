use std::collections::HashSet;

use datafusion::common::{Column, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use repark_common::names::NameRule;

use super::written_names::qualifier_matches;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildSort {
    Bound(String),
    Hidden,
    Unresolved,
}

#[must_use]
pub fn sort_through_child(plan: &LogicalPlan, written: &[String], rule: NameRule) -> ChildSort {
    let LogicalPlan::Projection(projection) = plan else {
        return ChildSort::Unresolved;
    };
    let Some((name, qualifier)) = written.split_last() else {
        return ChildSort::Unresolved;
    };
    let want = match qualifier {
        [] => None,
        [table] => Some(TableReference::bare(table.as_str())),
        [schema, table] => Some(TableReference::partial(schema.as_str(), table.as_str())),
        [catalog, schema, table] => Some(TableReference::full(
            catalog.as_str(),
            schema.as_str(),
            table.as_str(),
        )),
        _ => return ChildSort::Unresolved,
    };
    let hits: Vec<Column> = projection
        .input
        .schema()
        .iter()
        .filter(|(held, field)| {
            rule.matches(name, field.name())
                && want
                    .as_ref()
                    .is_none_or(|want| held.is_some_and(|held| qualifier_matches(want, held, rule)))
        })
        .map(|(held, field)| Column::new(held.cloned(), field.name()))
        .collect();
    let [child] = hits.as_slice() else {
        return ChildSort::Unresolved;
    };
    projection
        .expr
        .iter()
        .zip(projection.schema.fields())
        .find(|(expr, _)| source_column(expr).is_some_and(|source| same_column(source, child)))
        .map_or(ChildSort::Hidden, |(_, field)| {
            ChildSort::Bound(field.name().clone())
        })
}

#[must_use]
pub fn same_source_fields(plan: &LogicalPlan, fields: &[String]) -> bool {
    let LogicalPlan::Projection(projection) = plan else {
        return false;
    };
    let mut sources: Vec<&Column> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for field in fields {
        if !seen.insert(field.as_str()) {
            continue;
        }
        let Some(source) = output_source(projection, field) else {
            return false;
        };
        if !sources.iter().any(|held| same_column(held, source)) {
            sources.push(source);
        }
    }
    sources.len() == 1
}

fn output_source<'a>(projection: &'a Projection, field: &str) -> Option<&'a Column> {
    let mut outputs = projection
        .expr
        .iter()
        .zip(projection.schema.fields())
        .filter(|(_, output)| output.name() == field);
    let (expr, _) = outputs.next()?;
    if outputs.next().is_some() {
        return None;
    }
    source_column(expr)
}

fn source_column(expr: &Expr) -> Option<&Column> {
    let mut current = expr;
    while let Expr::Alias(alias) = current {
        current = alias.expr.as_ref();
    }
    match current {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}

fn same_column(left: &Column, right: &Column) -> bool {
    left.name == right.name && left.relation == right.relation
}

pub(super) fn written_column(written: &str) -> Column {
    let mut parts: Vec<String> = Vec::new();
    let mut part = String::new();
    let mut quoted = false;
    let mut chars = written.chars().peekable();
    while let Some(next) = chars.next() {
        match (next, quoted) {
            ('`', true) if chars.peek() == Some(&'`') => {
                chars.next();
                part.push('`');
            }
            ('`', _) => quoted = !quoted,
            ('.', false) => parts.push(std::mem::take(&mut part)),
            (other, _) => part.push(other),
        }
    }
    parts.push(part);
    if quoted || parts.iter().any(String::is_empty) {
        return Column::new_unqualified(written);
    }
    match parts.as_slice() {
        [table, name] => Column::new(Some(TableReference::bare(table.as_str())), name),
        _ => Column::new_unqualified(parts.join(".")),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::logical_plan::table_scan;
    use datafusion::logical_expr::{LogicalPlan, lit};

    use super::{ChildSort, same_source_fields, sort_through_child, written_column};
    use repark_common::names::NameRule::{Exact, IgnoreCase};

    fn projected(fields: &[&str], exprs: Vec<datafusion::logical_expr::Expr>) -> LogicalPlan {
        let schema = Schema::new(
            fields
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        );
        table_scan(Some("s"), &schema, None)
            .unwrap()
            .project(exprs)
            .unwrap()
            .build()
            .unwrap()
    }

    fn written(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_string()).collect()
    }

    fn named(name: &str) -> datafusion::logical_expr::Expr {
        datafusion::logical_expr::Expr::Column(Column::from_name(name))
    }

    #[test]
    fn sort_keys_resolve_through_the_projection_child_like_spark() {
        let twins = projected(
            &["id", "v", "Data"],
            vec![named("v").alias("t1"), (named("v") + lit(1)).alias("t2")],
        );
        assert_eq!(
            sort_through_child(&twins, &written(&["v"]), IgnoreCase),
            ChildSort::Bound("t1".to_string())
        );
        assert_eq!(
            sort_through_child(&twins, &written(&["V"]), IgnoreCase),
            ChildSort::Bound("t1".to_string())
        );
        assert_eq!(
            sort_through_child(&twins, &written(&["V"]), Exact),
            ChildSort::Unresolved
        );
        assert_eq!(
            sort_through_child(&twins, &written(&["s", "v"]), IgnoreCase),
            ChildSort::Bound("t1".to_string())
        );
        let hidden = projected(
            &["id", "v"],
            vec![
                (named("v") + lit(1)).alias("v"),
                (named("v") + lit(2)).alias("w"),
            ],
        );
        assert_eq!(
            sort_through_child(&hidden, &written(&["v"]), IgnoreCase),
            ChildSort::Hidden
        );
        let case_twins = projected(&["v", "V"], vec![named("v"), named("V")]);
        assert_eq!(
            sort_through_child(&case_twins, &written(&["v"]), IgnoreCase),
            ChildSort::Unresolved
        );
        let scan = table_scan(
            Some("s"),
            &Schema::new(vec![Field::new("v", DataType::Int64, true)]),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            sort_through_child(&scan, &written(&["v"]), IgnoreCase),
            ChildSort::Unresolved
        );
    }

    #[test]
    fn one_source_column_is_one_attribute() {
        let pair = projected(
            &["v", "w"],
            vec![named("v").alias("v"), named("v").alias("V")],
        );
        assert!(same_source_fields(&pair, &written(&["v", "V"])));
        let distinct = projected(&["v", "V"], vec![named("v"), named("V")]);
        assert!(!same_source_fields(&distinct, &written(&["v", "V"])));
        let computed = projected(&["v"], vec![named("v"), (named("v") + lit(1)).alias("V")]);
        assert!(!same_source_fields(&computed, &written(&["v", "V"])));
        assert!(!same_source_fields(&pair, &written(&["v", "missing"])));
    }

    #[test]
    fn written_names_render_as_spark_prints_them() {
        let render = |written: &str| {
            let column = written_column(written);
            (column.relation.map(|table| table.to_string()), column.name)
        };
        assert_eq!(render("`x.y`"), (None, "x.y".to_string()));
        assert_eq!(render("`a``b`"), (None, "a`b".to_string()));
        assert_eq!(
            render("L.`x.y`"),
            (Some("L".to_string()), "x.y".to_string())
        );
        assert_eq!(render("`unclosed"), (None, "`unclosed".to_string()));
    }
}
