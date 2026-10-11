use std::ops::ControlFlow;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Insert, Statement, TableFactor, TableObject, visit_statements,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::nested_ns_gate::{InsertSupply, NestedWrite, refuse_nested_ns_supply};

use super::insert_positional::replace_where::parse_replace_where;
use crate::catalog_ops::{iceberg_err, name_parts};
use crate::insert_overwrite::object_name_last;
use crate::normalize::{object_name_from_table_with_joins, parse_single_normalized};
use crate::write_to_branch::{
    RefSelectorKind, qualify_table_parts, split_write_ref_parts, written_targets,
};

pub(super) async fn refuse_nested_supply(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &crate::write_options::StatementWriteOptions,
) -> Result<()> {
    let source_by_name = write_options.source_by_name;
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
        return refuse(ctx, catalogs, &name_parts(replace.table()), &write).await;
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
            refuse(ctx, catalogs, &parts, &NestedWrite::Unreadable).await?;
        }
        return Ok(());
    };
    for (index, write) in executed_writes(&statement).iter().enumerate() {
        let by_name = by_name && index == 0;
        match write {
            Statement::Insert(insert) => {
                refuse_insert(ctx, catalogs, insert, by_name, source_by_name).await?;
            }
            Statement::Update(update) => {
                if let Some(name) = object_name_from_table_with_joins(&update.table) {
                    let write = NestedWrite::Update(&update.assignments);
                    refuse(ctx, catalogs, &name_parts(name), &write).await?;
                }
            }
            Statement::Merge(merge) => {
                if let TableFactor::Table { name, .. } = &merge.table {
                    let write = NestedWrite::Merge(&merge.clauses);
                    refuse(ctx, catalogs, &name_parts(name), &write).await?;
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

fn executed_writes(statement: &Statement) -> Vec<Statement> {
    match statement {
        Statement::Explain {
            analyze, statement, ..
        } => {
            if *analyze {
                executed_writes(statement)
            } else {
                Vec::new()
            }
        }
        Statement::Prepare { statement, .. } => executed_writes(statement),
        carrier => {
            let mut writes = Vec::new();
            let _ = visit_statements(carrier, |nested| {
                if matches!(
                    nested,
                    Statement::Insert(_) | Statement::Update(_) | Statement::Merge(_)
                ) {
                    writes.push(nested.clone());
                }
                ControlFlow::<()>::Continue(())
            });
            writes
        }
    }
}

async fn refuse_insert(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    by_name: bool,
    source_by_name: bool,
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
    refuse(ctx, catalogs, &name_parts(name), &write).await
}

async fn refuse(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    written: &[String],
    write: &NestedWrite<'_>,
) -> Result<()> {
    let mut targets = vec![written.to_vec()];
    if let Some((table_parts, RefSelectorKind::Branch(_))) = split_write_ref_parts(written) {
        targets.push(table_parts);
    }
    for parts in targets {
        refuse_at(ctx, catalogs, parts, write).await?;
    }
    Ok(())
}

async fn refuse_at(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    parts: Vec<String>,
    write: &NestedWrite<'_>,
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
    refuse_nested_ns_supply(&table, &label, write)
}
