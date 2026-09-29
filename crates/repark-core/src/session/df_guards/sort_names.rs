use std::collections::HashSet;

use datafusion::common::{Column, TableReference};
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Projection};
use repark_common::names::NameRule;

use super::written_names::qualifier_matches;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildSort {
    Bound(String),
    Child(String),
    Hidden,
    Unresolved,
}

#[must_use]
pub fn sort_through_child(plan: &LogicalPlan, written: &[String], rule: NameRule) -> ChildSort {
    let Some((projection, distinct)) = nearest_projection(plan) else {
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
    let input = projection.input.schema();
    let hits: Vec<Column> = input
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
    if let Some((_, field)) = projection
        .expr
        .iter()
        .zip(projection.schema.fields())
        .find(|(expr, _)| source_column(expr).is_some_and(|source| same_column(source, child)))
    {
        return ChildSort::Bound(field.name().clone());
    }
    let named = |fields: &datafusion::arrow::datatypes::Fields| {
        fields
            .iter()
            .filter(|field| *field.name() == child.name)
            .count()
    };
    if distinct || named(projection.schema.fields()) > 0 || named(input.fields()) != 1 {
        return ChildSort::Hidden;
    }
    ChildSort::Child(child.name.clone())
}

fn nearest_projection(plan: &LogicalPlan) -> Option<(&Projection, bool)> {
    let mut current = plan;
    let mut distinct = false;
    loop {
        current = match current {
            LogicalPlan::Projection(projection) => return Some((projection, distinct)),
            LogicalPlan::Filter(filter) => filter.input.as_ref(),
            LogicalPlan::Limit(limit) => limit.input.as_ref(),
            LogicalPlan::Sort(sort) => sort.input.as_ref(),
            LogicalPlan::Distinct(Distinct::All(input)) => {
                distinct = true;
                input.as_ref()
            }
            _ => return None,
        };
    }
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

#[must_use]
pub fn twin_identities(plan: &LogicalPlan, fresh: &[bool]) -> Vec<String> {
    let fields = plan.schema().fields();
    let projection =
        attribute_projection(plan).filter(|projection| projection.expr.len() == fields.len());
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let own = || format!("own:{}", field.name());
            if fresh.get(index).copied().unwrap_or(false) {
                return own();
            }
            projection
                .and_then(|projection| {
                    let source = source_column(projection.expr.get(index)?)?;
                    let at = projection.input.schema().maybe_index_of_column(source)?;
                    Some(format!("col:{at}"))
                })
                .unwrap_or_else(own)
        })
        .collect()
}

fn attribute_projection(plan: &LogicalPlan) -> Option<&Projection> {
    let mut current = plan;
    loop {
        current = match current {
            LogicalPlan::Projection(projection) => return Some(projection),
            LogicalPlan::Filter(filter) => filter.input.as_ref(),
            LogicalPlan::Limit(limit) => limit.input.as_ref(),
            LogicalPlan::Sort(sort) => sort.input.as_ref(),
            LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
            LogicalPlan::Distinct(Distinct::All(input)) => input.as_ref(),
            _ => return None,
        };
    }
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
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, lit};

    use super::{
        ChildSort, same_source_fields, sort_through_child, twin_identities, written_column,
    };
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
    fn twin_identities_prove_one_attribute_or_stay_distinct() {
        let twins = projected(
            &["id", "v"],
            vec![
                named("v").alias("e0"),
                named("v").alias("e1"),
                lit(100).alias("e2"),
                (named("v") + lit(1)).alias("e3"),
                named("id"),
            ],
        );
        let above = LogicalPlanBuilder::from(twins.clone())
            .filter(named("id").gt(lit(0)))
            .unwrap()
            .alias("x")
            .unwrap()
            .limit(0, Some(2))
            .unwrap()
            .build()
            .unwrap();
        let want = written(&["col:1", "col:1", "own:e2", "own:e3", "col:0"]);
        assert_eq!(twin_identities(&twins, &[]), want);
        assert_eq!(twin_identities(&above, &[false; 5]), want);
        assert_eq!(
            twin_identities(&twins, &[false, true]),
            written(&["col:1", "own:e1", "own:e2", "own:e3", "col:0"])
        );
        let scan = table_scan(
            Some("s"),
            &Schema::new(vec![
                Field::new("v", DataType::Int64, true),
                Field::new("w", DataType::Int64, true),
            ]),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(twin_identities(&scan, &[]), written(&["own:v", "own:w"]));
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

    #[test]
    fn sort_keys_resolve_through_the_nearest_projection_below() {
        let bound = ChildSort::Bound("t1".to_string());
        let twins = projected(
            &["id", "v"],
            vec![named("v").alias("t1"), (named("v") + lit(1)).alias("t2")],
        );
        let hidden = projected(
            &["id", "v"],
            vec![
                (named("v") + lit(1)).alias("t1"),
                (named("v") + lit(2)).alias("t2"),
            ],
        );
        let over = |plan: &LogicalPlan, distinct: bool| {
            let builder = LogicalPlanBuilder::from(plan.clone())
                .filter(lit(true))
                .unwrap()
                .limit(0, Some(10))
                .unwrap();
            let builder = if distinct {
                builder.distinct().unwrap()
            } else {
                builder
            };
            builder.build().unwrap()
        };
        let key = written(&["v"]);
        assert_eq!(
            sort_through_child(&over(&twins, false), &key, IgnoreCase),
            bound
        );
        assert_eq!(sort_through_child(&over(&twins, true), &key, Exact), bound);
        let child = ChildSort::Child("v".to_string());
        assert_eq!(sort_through_child(&hidden, &key, IgnoreCase), child);
        assert_eq!(
            sort_through_child(&over(&hidden, false), &key, Exact),
            child
        );
        assert_eq!(
            sort_through_child(&over(&hidden, true), &key, IgnoreCase),
            ChildSort::Hidden
        );
        let aliased = LogicalPlanBuilder::from(hidden)
            .alias("q")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            sort_through_child(&aliased, &key, IgnoreCase),
            ChildSort::Unresolved
        );
    }
}
