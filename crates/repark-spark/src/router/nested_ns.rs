use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::{ExprSchemable, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Insert, Query, Statement, TableFactor, TableObject, TableWithJoins, With,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::table::Table;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_functions::session_time_zone::session_time_zone_from_options;
use repark_iceberg::write::nested_ns_gate::{
    Feed, InsertSupply, NaiveLeaf, NestedSupply, NestedWrite, holds_zoned_timestamp,
    lands_on_naive_leaf, nested_naive_refusal, nested_supplies, refuse_nested_ns_supply,
};
use repark_iceberg::write::statement_writes::executed_writes;

use super::insert_positional::replace_where::parse_replace_where;
use crate::catalog_ops::{iceberg_err, name_parts};
use crate::insert_overwrite::object_name_last;
use crate::normalize::{object_name_from_table_with_joins, parse_single_normalized};
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

fn session_is_utc(ctx: &SessionContext) -> bool {
    let state = ctx.state();
    matches!(
        session_time_zone_from_options(state.config().options()),
        "UTC" | "+00:00" | "-00:00"
    )
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
        return refuse(ctx, catalogs, &target, &write, scope, micros).await;
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
            refuse(ctx, catalogs, &parts, &write, Scope::Unreadable, micros).await?;
        }
        return Ok(());
    };
    let micros = micros || matches!(statement, Statement::Prepare { .. });
    let outer = match &statement {
        Statement::Query(query) => query.with.as_ref(),
        _ => None,
    };
    for (index, write) in executed_writes(&statement).iter().enumerate() {
        let by_name = by_name && index == 0;
        match write {
            Statement::Insert(insert) => {
                let flags = (by_name, source_by_name, micros);
                refuse_insert(ctx, catalogs, insert, outer, flags).await?;
            }
            Statement::Update(update) => {
                if let Some(name) = object_name_from_table_with_joins(&update.table) {
                    let write = NestedWrite::Update(&update.assignments);
                    let scope = Scope::Update(&update.table);
                    refuse(ctx, catalogs, &name_parts(name), &write, scope, micros).await?;
                }
            }
            Statement::Merge(merge) => {
                if let TableFactor::Table { name, .. } = &merge.table {
                    let write = NestedWrite::Merge(&merge.clauses);
                    let scope = Scope::Merge(&merge.table, &merge.source);
                    refuse(ctx, catalogs, &name_parts(name), &write, scope, micros).await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
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
    (by_name, source_by_name, micros): (bool, bool, bool),
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
    let scope = Scope::Insert(insert.source.as_deref(), outer);
    refuse(ctx, catalogs, &name_parts(name), &write, scope, micros).await
}

async fn refuse(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    written: &[String],
    write: &NestedWrite<'_>,
    scope: Scope<'_>,
    micros: bool,
) -> Result<()> {
    let mut targets = vec![written.to_vec()];
    if let Some((table_parts, RefSelectorKind::Branch(_))) = split_write_ref_parts(written) {
        targets.push(table_parts);
    }
    for parts in targets {
        refuse_at(ctx, catalogs, parts, write, scope, micros).await?;
    }
    Ok(())
}

async fn refuse_at(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    parts: Vec<String>,
    write: &NestedWrite<'_>,
    scope: Scope<'_>,
    micros: bool,
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
    Ok(())
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
    let fields: Vec<FieldRef> = plan.schema().fields().iter().cloned().collect();
    if fields.len() != values.len()
        || fields
            .iter()
            .any(|field| holds_zoned_timestamp(field.data_type()))
    {
        return false;
    }
    let above_leaf = values.iter().any(|(_, whole)| *whole);
    !(matches!(scope, Scope::Update(_)) && above_leaf && converts_a_zoned_value(&plan))
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
    let fields: Vec<FieldRef> = plan.schema().fields().iter().cloned().collect();
    let mut picked: Vec<&FieldRef> = position
        .and_then(|position| fields.get(position))
        .into_iter()
        .collect();
    if let Some(name) = name {
        picked.extend(
            fields
                .iter()
                .filter(|field| field.name().eq_ignore_ascii_case(name)),
        );
    }
    if picked.is_empty() {
        picked = fields.iter().collect();
    }
    !picked
        .iter()
        .any(|field| holds_zoned_timestamp(field.data_type()))
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

fn converts_a_zoned_value(plan: &LogicalPlan) -> bool {
    let mut zoned = false;
    let _ = plan.apply(|node| {
        let inputs = node.inputs();
        let Some(input) = inputs.first() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let schema = input.schema();
        node.apply_expressions(|expr| {
            expr.apply(|inner| {
                let typed = inner.get_type(schema.as_ref());
                if typed.is_err() || typed.is_ok_and(|data_type| holds_zoned_timestamp(&data_type))
                {
                    zoned = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    });
    zoned
}
