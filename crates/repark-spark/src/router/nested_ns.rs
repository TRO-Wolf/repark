use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, JoinType};
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{Distinct, Expr, ExprSchemable, LogicalPlan, TableScan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Insert, Query, Statement, TableFactor, TableObject, TableWithJoins, Value,
    ValueWithSpan, With, visit_expressions,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::table::Table;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_functions::session_time_zone::session_time_zone_from_options;
use repark_functions::timestamp_ntz_cast::{TIMESTAMP_NTZ_CAST_NAME, TRY_TIMESTAMP_NTZ_CAST_NAME};
use repark_iceberg::write::nested_ns_gate::{
    Feed, InsertSupply, NaiveLeaf, NestedSupply, NestedWrite, casts_zoned_to_naive,
    holds_naive_timestamp, holds_zoned_timestamp, lands_on_naive_leaf, naive_into_instant,
    nested_naive_refusal, nested_supplies, refuse_nested_ns_supply, type_at,
};
use repark_iceberg::write::statement_writes::executed_writes;

use super::insert_positional::replace_where::parse_replace_where;
use crate::catalog_ops::{iceberg_err, name_parts};
use crate::insert_overwrite::object_name_last;
use crate::normalize::{object_name_from_table_with_joins, parse_single_normalized};
use crate::view_ddl::temp_view::definition_plan;
use crate::write_to_branch::{
    RefSelectorKind, qualify_table_parts, split_write_ref_parts, written_targets,
};

#[derive(Clone, Copy)]
enum Scope<'a> {
    Insert(Option<&'a Query>, Option<&'a With>),
    Update(&'a TableWithJoins),
    Merge(&'a TableFactor, &'a TableFactor),
    Unreadable,
}

const UTC_ALIASES: [&str; 21] = [
    "UTC",
    "UCT",
    "Universal",
    "Zulu",
    "Z",
    "GMT",
    "GMT0",
    "GMT+0",
    "GMT-0",
    "Greenwich",
    "Etc/UTC",
    "Etc/UCT",
    "Etc/Universal",
    "Etc/Zulu",
    "Etc/GMT",
    "Etc/GMT0",
    "Etc/GMT+0",
    "Etc/GMT-0",
    "Etc/Greenwich",
    "+00:00",
    "-00:00",
];

fn names_utc(zone: &str) -> bool {
    UTC_ALIASES.contains(&zone)
}

fn session_is_utc(ctx: &SessionContext) -> bool {
    let state = ctx.state();
    names_utc(session_time_zone_from_options(state.config().options()))
}

pub(super) async fn refuse_nested_supply(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<()> {
    let source_by_name = write_options.source_by_name;
    let micros = !session_is_utc(ctx);
    let rewritten = super::rewrite_sql_for_execute(sql, catalogs);
    let evolving = crate::merge::schema_evolution::strip_schema_evolution(&rewritten);
    let sql = evolving.as_deref().unwrap_or(rewritten.as_str());
    if let Ok(Some(replace)) = parse_replace_where(sql) {
        let write = NestedWrite::Insert(InsertSupply {
            listed: &[],
            source: Some(replace.source()),
            partitioned: false,
            positional: true,
            by_name: source_by_name,
        });
        let scope = Scope::Insert(Some(replace.source()), None);
        let target = name_parts(replace.table());
        return refuse(ctx, catalogs, &target, &write, scope, (micros, false)).await;
    }
    let stripped = crate::insert_by_name::strip_insert_by_name(sql)
        .ok()
        .flatten();
    let by_name = stripped.is_some();
    let parsed = parse_single_normalized(stripped.as_deref().unwrap_or(sql));
    let Ok(Some((statement, _, _))) = parsed else {
        if let Some((analyze, explained)) = explained_statement(sql) {
            if !analyze {
                return Ok(());
            }
            let inner = Box::pin(refuse_nested_supply(
                ctx,
                catalogs,
                &explained,
                write_options,
            ));
            return inner.await;
        }
        for parts in written_targets(sql) {
            let write = NestedWrite::Unreadable;
            refuse(
                ctx,
                catalogs,
                &parts,
                &write,
                Scope::Unreadable,
                (micros, false),
            )
            .await?;
        }
        return Ok(());
    };
    let prepared = matches!(statement, Statement::Prepare { .. });
    let micros = micros || prepared;
    let parameterised = prepared && holds_a_parameter(&statement);
    let outer = match &statement {
        Statement::Query(query) => query.with.as_ref(),
        _ => None,
    };
    for (index, write) in executed_writes(&statement).iter().enumerate() {
        let by_name = by_name && index == 0;
        match write {
            Statement::Insert(insert) => {
                let flags = (by_name, source_by_name, micros, parameterised);
                refuse_insert(ctx, catalogs, insert, outer, flags).await?;
            }
            Statement::Update(update) => {
                if let Some(name) = object_name_from_table_with_joins(&update.table) {
                    let write = NestedWrite::Update(&update.assignments);
                    let scope = typed_unless(parameterised, Scope::Update(&update.table));
                    refuse(
                        ctx,
                        catalogs,
                        &name_parts(name),
                        &write,
                        scope,
                        (micros, true),
                    )
                    .await?;
                }
            }
            Statement::Merge(merge) => {
                if let TableFactor::Table { name, .. } = &merge.table {
                    let write = NestedWrite::Merge(&merge.clauses);
                    let scope =
                        typed_unless(parameterised, Scope::Merge(&merge.table, &merge.source));
                    refuse(
                        ctx,
                        catalogs,
                        &name_parts(name),
                        &write,
                        scope,
                        (micros, true),
                    )
                    .await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn holds_a_parameter(statement: &Statement) -> bool {
    visit_expressions(statement, |expr| {
        if matches!(
            expr,
            SqlExpr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            })
        ) {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

fn typed_unless(parameterised: bool, scope: Scope<'_>) -> Scope<'_> {
    if parameterised {
        Scope::Unreadable
    } else {
        scope
    }
}

fn explained_statement(sql: &str) -> Option<(bool, String)> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut words = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)));
    match words.next()? {
        (_, Token::Word(head)) if head.value.eq_ignore_ascii_case("EXPLAIN") => {}
        _ => return None,
    }
    let mut analyze = false;
    for (index, token) in words {
        match token {
            Token::Word(word) if word.value.eq_ignore_ascii_case("ANALYZE") => analyze = true,
            Token::Word(word) if word.value.eq_ignore_ascii_case("VERBOSE") => {}
            _ => {
                let rest: String = tokens[index..].iter().map(ToString::to_string).collect();
                return Some((analyze, rest));
            }
        }
    }
    None
}

async fn refuse_insert(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    outer: Option<&With>,
    (by_name, source_by_name, micros, parameterised): (bool, bool, bool, bool),
) -> Result<()> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let listed: Vec<String> = insert.columns.iter().map(object_name_last).collect();
    let write = NestedWrite::Insert(InsertSupply {
        listed: &listed,
        source: insert.source.as_deref(),
        partitioned: insert
            .partitioned
            .as_ref()
            .is_some_and(|spec| !spec.is_empty()),
        positional: !by_name,
        by_name: by_name || source_by_name,
    });
    let scope = typed_unless(
        parameterised,
        Scope::Insert(insert.source.as_deref(), outer),
    );
    let store = insert.overwrite || by_name;
    refuse(
        ctx,
        catalogs,
        &name_parts(name),
        &write,
        scope,
        (micros, store),
    )
    .await
}

async fn refuse(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    written: &[String],
    write: &NestedWrite<'_>,
    scope: Scope<'_>,
    zone: (bool, bool),
) -> Result<()> {
    let mut targets = vec![written.to_vec()];
    if let Some((table_parts, RefSelectorKind::Branch(_))) = split_write_ref_parts(written) {
        targets.push(table_parts);
    }
    for parts in targets {
        refuse_at(ctx, catalogs, parts, write, scope, zone).await?;
    }
    Ok(())
}

async fn refuse_at(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    parts: Vec<String>,
    write: &NestedWrite<'_>,
    scope: Scope<'_>,
    (micros, store): (bool, bool),
) -> Result<()> {
    let parts = qualify_table_parts(ctx, parts);
    let [catalog_name, namespace @ .., leaf] = parts.as_slice() else {
        return Ok(());
    };
    let (Some(catalog), Ok(namespace)) = (
        catalogs.get(catalog_name),
        NamespaceIdent::from_vec(namespace.to_vec()),
    ) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, leaf.clone());
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(iceberg_err(error)),
    };
    let label = std::iter::once(catalog_name.as_str())
        .chain(ident.namespace().iter().map(String::as_str))
        .chain(std::iter::once(ident.name()))
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    refuse_nested_ns_supply(&table, &label, write)?;
    if micros {
        refuse_zoned_micros(ctx, catalogs, &table, &label, write, scope).await?;
    }
    if micros && store {
        refuse_naive_into_instant(ctx, catalogs, &table, &label, write, scope).await?;
    }
    Ok(())
}

async fn refuse_naive_into_instant(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &Table,
    label: &str,
    write: &NestedWrite<'_>,
    scope: Scope<'_>,
) -> Result<()> {
    for supply in nested_supplies(table, write, NaiveLeaf::Instant)? {
        if Box::pin(carries_a_naive_leaf(ctx, catalogs, label, &supply, scope)).await {
            return Err(nested_naive_refusal(
                label,
                &supply.column,
                &supply.leaf,
                NaiveLeaf::Instant,
            ));
        }
    }
    Ok(())
}

async fn carries_a_naive_leaf(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    label: &str,
    supply: &NestedSupply<'_>,
    scope: Scope<'_>,
) -> bool {
    let mut values = Vec::new();
    for feed in &supply.feeds {
        let planned = match (feed, scope) {
            (Feed::Expr { value, path }, Scope::Update(_) | Scope::Merge(..)) => {
                if !lands_on_naive_leaf(&supply.data_type, path, NaiveLeaf::Instant) {
                    values.push((value.to_string(), path.clone()));
                }
                None
            }
            (Feed::SourceColumn { position, name }, Scope::Insert(Some(source), outer)) => {
                let sql = match outer {
                    Some(with) => format!("{with} {source}"),
                    None => source.to_string(),
                };
                column_type(ctx, catalogs, &sql, *position, name.as_deref()).await
            }
            (Feed::SourceStar, Scope::Merge(_, source)) => {
                let sql = format!("SELECT * FROM {source}");
                column_type(ctx, catalogs, &sql, None, Some(&supply.column)).await
            }
            _ => None,
        };
        if planned.is_some_and(|planned| naive_into_instant(&planned, &supply.data_type)) {
            return true;
        }
    }
    if values.is_empty() {
        return false;
    }
    let from = match scope {
        Scope::Update(table) => format!("{label}{}", alias_of(&table.relation)),
        Scope::Merge(target, source) => format!("{label}{}, {source}", alias_of(target)),
        Scope::Insert(..) | Scope::Unreadable => return false,
    };
    let projection = values
        .iter()
        .enumerate()
        .map(|(index, (value, _))| format!("{value} AS __repark_feed_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let Some(plan) = planned(ctx, catalogs, &format!("SELECT {projection} FROM {from}")).await
    else {
        return false;
    };
    let update = matches!(scope, Scope::Update(_));
    values.iter().enumerate().any(|(index, (_, path))| {
        let carried = plan
            .schema()
            .fields()
            .get(index)
            .zip(type_at(&supply.data_type, path))
            .is_some_and(|(field, target)| naive_into_instant(field.data_type(), target));
        carried && !(update && lineage_has(&plan, &[index], shifts_in_an_update))
    })
}

async fn column_type(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    position: Option<usize>,
    name: Option<&str>,
) -> Option<DataType> {
    let plan = planned(ctx, catalogs, sql).await?;
    let fields = plan.schema().fields();
    let picked = name
        .and_then(|name| {
            fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
        })
        .or_else(|| position.and_then(|position| fields.get(position)))?;
    Some(picked.data_type().clone())
}

async fn refuse_zoned_micros(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &Table,
    label: &str,
    write: &NestedWrite<'_>,
    scope: Scope<'_>,
) -> Result<()> {
    for supply in nested_supplies(table, write, NaiveLeaf::Micros)? {
        if !Box::pin(proven_naive(ctx, catalogs, label, &supply, scope)).await {
            return Err(nested_naive_refusal(
                label,
                &supply.column,
                &supply.leaf,
                NaiveLeaf::Micros,
            ));
        }
    }
    Ok(())
}

async fn proven_naive(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    label: &str,
    supply: &NestedSupply<'_>,
    scope: Scope<'_>,
) -> bool {
    let mut values = Vec::new();
    for feed in &supply.feeds {
        let naive = match (feed, scope) {
            (Feed::Expr { value, path }, Scope::Update(_) | Scope::Merge(..)) => {
                let above = !lands_on_naive_leaf(&supply.data_type, path, NaiveLeaf::Micros);
                values.push((value.to_string(), above));
                true
            }
            (Feed::SourceColumn { position, name }, Scope::Insert(Some(source), outer)) => {
                let sql = match outer {
                    Some(with) => format!("{with} {source}"),
                    None => source.to_string(),
                };
                naive_columns(ctx, catalogs, &sql, *position, name.as_deref()).await
            }
            (Feed::SourceStar, Scope::Merge(_, source)) => {
                let sql = format!("SELECT * FROM {source}");
                naive_columns(ctx, catalogs, &sql, None, Some(&supply.column)).await
            }
            _ => false,
        };
        if !naive {
            return false;
        }
    }
    if values.is_empty() {
        return true;
    }
    let from = match scope {
        Scope::Update(table) => format!("{label}{}", alias_of(&table.relation)),
        Scope::Merge(target, source) => format!("{label}{}, {source}", alias_of(target)),
        Scope::Insert(..) | Scope::Unreadable => return false,
    };
    let projection = values
        .iter()
        .enumerate()
        .map(|(index, (value, _))| format!("{value} AS __repark_feed_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let Some(plan) = planned(ctx, catalogs, &format!("SELECT {projection} FROM {from}")).await
    else {
        return false;
    };
    let fields = plan.schema().fields();
    let columns: Vec<usize> = (0..fields.len()).collect();
    if fields.len() != values.len()
        || fields
            .iter()
            .any(|field| holds_zoned_timestamp(field.data_type()))
        || lineage_has(&plan, &columns, casts_a_zoned_value_to_naive)
    {
        return false;
    }
    let above_leaf = values.iter().any(|(_, whole)| *whole);
    !(matches!(scope, Scope::Update(_))
        && above_leaf
        && lineage_has(&plan, &columns, shifts_in_an_update))
}

async fn naive_columns(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    position: Option<usize>,
    name: Option<&str>,
) -> bool {
    let Some(plan) = planned(ctx, catalogs, sql).await else {
        return false;
    };
    let fields = plan.schema().fields();
    let mut picked: Vec<usize> = position
        .filter(|position| *position < fields.len())
        .into_iter()
        .collect();
    if let Some(name) = name {
        picked.extend(
            fields
                .iter()
                .enumerate()
                .filter(|(_, field)| field.name().eq_ignore_ascii_case(name))
                .map(|(index, _)| index),
        );
    }
    if picked.is_empty() {
        picked = (0..fields.len()).collect();
    }
    !picked
        .iter()
        .filter_map(|index| fields.get(*index))
        .any(|field| holds_zoned_timestamp(field.data_type()))
        && !lineage_has(&plan, &picked, casts_a_zoned_value_to_naive)
}

async fn planned(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Option<LogicalPlan> {
    let options = crate::write_options::StatementWriteOptions::default();
    let frame = Box::pin(super::execute_calibrated(
        ctx, catalogs, sql, None, &options,
    ))
    .await
    .ok()?;
    Some(frame.logical_plan().clone())
}

fn alias_of(factor: &TableFactor) -> String {
    match factor {
        TableFactor::Table {
            alias: Some(alias), ..
        } => format!(" AS {}", alias.name),
        _ => String::new(),
    }
}

type Test = fn(&Expr, &DFSchema) -> bool;

const MAX_LINEAGE_STEPS: usize = 65_536;

enum Next<'a> {
    Column(&'a LogicalPlan, usize),
    Lateral(&'a LogicalPlan, usize, &'a LogicalPlan),
    Outer(&'a Column),
    Whole(&'a LogicalPlan),
    View(Box<LogicalPlan>, Option<usize>),
}

fn lineage_has(plan: &LogicalPlan, columns: &[usize], test: Test) -> bool {
    let mut roots = vec![(plan.clone(), columns.to_vec())];
    let mut steps = 0_usize;
    while let Some((root, columns)) = roots.pop() {
        let mut seen = HashSet::new();
        let mut pending: Vec<(&LogicalPlan, usize, Vec<&LogicalPlan>)> = columns
            .iter()
            .map(|column| (&root, *column, Vec::new()))
            .collect();
        while let Some((node, column, scopes)) = pending.pop() {
            if !seen.insert((std::ptr::from_ref(node), column)) {
                continue;
            }
            steps += 1;
            let mut next = Vec::new();
            if steps > MAX_LINEAGE_STEPS || step(node, column, test, &mut next) {
                return true;
            }
            for reach in next {
                match reach {
                    Next::Column(input, index) => pending.push((input, index, scopes.clone())),
                    Next::Lateral(input, index, outer) => {
                        let mut inner = scopes.clone();
                        inner.push(outer);
                        pending.push((input, index, inner));
                    }
                    Next::Outer(column) => {
                        let reached = scopes.iter().enumerate().rev().find_map(|(depth, scope)| {
                            let index = scope.schema().maybe_index_of_column(column)?;
                            Some((*scope, index, scopes[..depth].to_vec()))
                        });
                        match reached {
                            Some(reached) => pending.push(reached),
                            None => return true,
                        }
                    }
                    Next::Whole(plan) => {
                        if plan_has(plan, test)
                            || (!scopes.is_empty()
                                && plan_has(plan, refers_out)
                                && scopes.iter().any(|scope| plan_has(scope, test)))
                        {
                            return true;
                        }
                    }
                    Next::View(view, Some(index)) => roots.push((*view, vec![index])),
                    Next::View(view, None) => {
                        if plan_has(&view, test) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

fn step<'a>(node: &'a LogicalPlan, column: usize, test: Test, next: &mut Vec<Next<'a>>) -> bool {
    let passed = match node {
        LogicalPlan::Projection(projection) => {
            return match projection.expr.get(column) {
                Some(expr) => feeds(node, [expr], test, next),
                None => whole(node, next),
            };
        }
        LogicalPlan::Window(window) => {
            match column.checked_sub(window.input.schema().fields().len()) {
                None => Some(window.input.as_ref()),
                Some(index) => {
                    return match window.window_expr.get(index) {
                        Some(expr) => feeds(node, [expr], test, next),
                        None => whole(node, next),
                    };
                }
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            let exprs = aggregate.group_expr.iter().chain(&aggregate.aggr_expr);
            return feeds(node, exprs, test, next);
        }
        LogicalPlan::Values(values) => {
            let exprs = values.values.iter().filter_map(|row| row.get(column));
            return feeds(node, exprs, test, next);
        }
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                next.push(Next::Column(input.as_ref(), column));
            }
            return false;
        }
        LogicalPlan::Join(join) => {
            let width = join.left.schema().fields().len();
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    match column.checked_sub(width) {
                        None => Some(join.left.as_ref()),
                        Some(index) => {
                            next.push(Next::Lateral(join.right.as_ref(), index, &join.left));
                            return false;
                        }
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => Some(join.left.as_ref()),
                JoinType::RightSemi | JoinType::RightAnti => Some(join.right.as_ref()),
                JoinType::LeftMark | JoinType::RightMark => None,
            }
        }
        LogicalPlan::TableScan(scan) => {
            scanned(scan, column, next);
            return false;
        }
        LogicalPlan::EmptyRelation(_) => return false,
        LogicalPlan::Filter(filter) => Some(filter.input.as_ref()),
        LogicalPlan::Sort(sort) => Some(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => Some(limit.input.as_ref()),
        LogicalPlan::Repartition(repartition) => Some(repartition.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => Some(alias.input.as_ref()),
        LogicalPlan::Subquery(subquery) => Some(subquery.subquery.as_ref()),
        LogicalPlan::Distinct(Distinct::All(input)) => Some(input.as_ref()),
        _ => None,
    };
    match passed {
        Some(input) => {
            next.push(Next::Column(input, column));
            false
        }
        None => whole(node, next),
    }
}

fn whole<'a>(node: &'a LogicalPlan, next: &mut Vec<Next<'a>>) -> bool {
    next.push(Next::Whole(node));
    false
}

fn scanned(scan: &TableScan, column: usize, next: &mut Vec<Next<'_>>) {
    let Ok(provider) = source_as_provider(&scan.source) else {
        return;
    };
    let Some(view) = definition_plan(provider.as_ref()) else {
        return;
    };
    let index = match &scan.projection {
        Some(projection) => projection.get(column).copied(),
        None => Some(column),
    };
    let aligned = view.schema().fields().len() == provider.schema().fields().len();
    next.push(Next::View(Box::new(view), index.filter(|_| aligned)));
}

fn feeds<'a>(
    node: &'a LogicalPlan,
    exprs: impl IntoIterator<Item = &'a Expr>,
    test: Test,
    next: &mut Vec<Next<'a>>,
) -> bool {
    let schema = input_schema(node);
    let mut found = false;
    let mut columns: Vec<&'a Column> = Vec::new();
    for expr in exprs {
        let _ = expr.apply(|inner| {
            if test(inner, &schema) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            match inner {
                Expr::Column(column) => columns.push(column),
                Expr::OuterReferenceColumn(_, column) => next.push(Next::Outer(column)),
                Expr::ScalarSubquery(subquery) => {
                    let body = subquery.subquery.as_ref();
                    match node.inputs().first() {
                        Some(input) => next.push(Next::Lateral(body, 0, input)),
                        None => next.push(Next::Column(body, 0)),
                    }
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if found {
            return true;
        }
    }
    let inputs = node.inputs();
    for column in columns {
        let resolved = inputs.iter().find_map(|input| {
            input
                .schema()
                .maybe_index_of_column(column)
                .map(|index| (*input, index))
        });
        match resolved {
            Some((input, index)) => next.push(Next::Column(input, index)),
            None if inputs.is_empty() => return true,
            None => next.extend(inputs.iter().map(|input| Next::Whole(input))),
        }
    }
    false
}

fn input_schema(node: &LogicalPlan) -> DFSchema {
    let inputs = node.inputs();
    let mut schema = DFSchema::empty();
    if inputs.is_empty() {
        schema.merge(node.schema());
    }
    for input in inputs {
        schema.merge(input.schema());
    }
    schema
}

fn plan_has(plan: &LogicalPlan, test: Test) -> bool {
    let mut pending = vec![plan.clone()];
    while let Some(next) = pending.pop() {
        let mut found = false;
        let _ = next.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Ok(provider) = source_as_provider(&scan.source)
                && let Some(view) = definition_plan(provider.as_ref())
            {
                pending.push(view);
            }
            let inputs = node.inputs();
            let mut schema = DFSchema::empty();
            if inputs.is_empty() {
                schema.merge(node.schema());
            }
            for input in inputs {
                schema.merge(input.schema());
            }
            node.apply_expressions(|expr| {
                expr.apply(|inner| {
                    if test(inner, &schema) {
                        found = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        });
        if found {
            return true;
        }
    }
    false
}

fn casts_a_zoned_value_to_naive(expr: &Expr, schema: &DFSchema) -> bool {
    let (input, target) = match expr {
        Expr::Cast(cast) => (cast.expr.as_ref(), cast.field.data_type()),
        Expr::TryCast(cast) => (cast.expr.as_ref(), cast.field.data_type()),
        _ => return false,
    };
    holds_naive_timestamp(target)
        && input
            .get_type(schema)
            .map_or(true, |source| casts_zoned_to_naive(&source, target))
}

fn refers_out(expr: &Expr, _: &DFSchema) -> bool {
    matches!(expr, Expr::OuterReferenceColumn(..))
}

fn shifts_in_an_update(expr: &Expr, _: &DFSchema) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function)
            if matches!(
                function.func.name(),
                TIMESTAMP_NTZ_CAST_NAME | TRY_TIMESTAMP_NTZ_CAST_NAME
            )
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion::common::{Column, DFSchema, ScalarValue};
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Projection, cast, lit};

    use repark_functions::session_time_zone::SessionTimeZoneConfig;

    use super::{casts_a_zoned_value_to_naive, lineage_has, names_utc};

    fn instant_cast() -> LogicalPlan {
        let instant = lit(ScalarValue::TimestampMicrosecond(
            Some(0),
            Some("UTC".into()),
        ));
        let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
        LogicalPlanBuilder::empty(true)
            .project(vec![cast(instant, naive).alias("a")])
            .and_then(LogicalPlanBuilder::build)
            .expect("plan")
    }

    #[test]
    fn a_column_that_resolves_in_no_input_reads_every_input_whole() {
        let fields = vec![Field::new("b", DataType::Int32, true)];
        let schema =
            DFSchema::from_unqualified_fields(fields.into(), HashMap::new()).expect("schema");
        let outer = Projection::try_new_with_schema(
            vec![Expr::Column(Column::from_name("b"))],
            Arc::new(instant_cast()),
            Arc::new(schema),
        )
        .expect("projection");
        let outer = LogicalPlan::Projection(outer);
        assert!(lineage_has(&outer, &[0], casts_a_zoned_value_to_naive));
        assert!(lineage_has(
            &instant_cast(),
            &[0],
            casts_a_zoned_value_to_naive
        ));
    }

    #[test]
    fn an_outer_reference_no_enclosing_join_resolves_fails_closed() {
        let field = Arc::new(Field::new("c", DataType::Int32, true));
        let outer = Expr::OuterReferenceColumn(field, Column::from_name("c"));
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![outer.alias("s")])
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        assert!(lineage_has(&plan, &[0], casts_a_zoned_value_to_naive));
    }

    #[test]
    fn an_unmodelled_node_that_refers_out_reads_the_other_side_whole() {
        let naive = Arc::new(Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let outer = Expr::OuterReferenceColumn(naive, Column::from_name("a"));
        let lateral = LogicalPlanBuilder::empty(true)
            .distinct_on(vec![lit(1)], vec![outer.alias("s")], None)
            .and_then(LogicalPlanBuilder::build)
            .expect("lateral");
        let plan = LogicalPlanBuilder::from(instant_cast())
            .cross_join(lateral)
            .and_then(LogicalPlanBuilder::build)
            .expect("join");
        assert!(lineage_has(&plan, &[1], casts_a_zoned_value_to_naive));
    }

    #[test]
    fn only_a_pure_utc_alias_counts_as_utc() {
        for alias in [
            "UTC",
            "UCT",
            "Universal",
            "Zulu",
            "Z",
            "GMT",
            "GMT0",
            "GMT+0",
            "GMT-0",
            "Greenwich",
            "Etc/UTC",
            "Etc/UCT",
            "Etc/Universal",
            "Etc/Zulu",
            "Etc/GMT",
            "Etc/GMT0",
            "Etc/GMT+0",
            "Etc/GMT-0",
            "Etc/Greenwich",
            "+00:00",
            "-00:00",
        ] {
            let mut carrier = SessionTimeZoneConfig::default();
            carrier.set_zone(alias);
            assert!(names_utc(alias), "{alias}");
            assert!(names_utc(carrier.zone()), "{alias} as {}", carrier.zone());
        }
        for zone in [
            "Africa/Abidjan",
            "Atlantic/Reykjavik",
            "Europe/London",
            "America/New_York",
            "+01:00",
            "Etc/GMT+1",
        ] {
            let mut carrier = SessionTimeZoneConfig::default();
            carrier.set_zone(zone);
            assert!(!names_utc(zone), "{zone}");
            assert!(!names_utc(carrier.zone()), "{zone} as {}", carrier.zone());
        }
    }
}
