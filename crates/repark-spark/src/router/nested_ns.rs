use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, SetExpr, Statement, TableFactor, TableObject};
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::nested_ns_gate::{InsertSupply, NestedWrite, refuse_nested_ns_supply};

use super::insert_positional::replace_where::parse_replace_where;
use crate::catalog_ops::{iceberg_err, name_parts};
use crate::insert_overwrite::object_name_last;
use crate::normalize::{object_name_from_table_with_joins, parse_single_normalized};
use crate::write_to_branch::{
    RefSelectorKind, qualify_table_parts, split_write_ref_parts, write_target_parts,
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
        return match write_target_parts(sql) {
            Some((head, parts)) if matches!(head.as_str(), "INSERT" | "UPDATE" | "MERGE") => {
                refuse(ctx, catalogs, &parts, &NestedWrite::Unreadable).await
            }
            _ => Ok(()),
        };
    };
    match &statement {
        Statement::Insert(insert) => {
            refuse_insert(ctx, catalogs, insert, by_name, source_by_name).await
        }
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Insert(Statement::Insert(insert)) => {
                refuse_insert(ctx, catalogs, insert, by_name, source_by_name).await
            }
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Merge(_) => {
                match write_target_parts(&query.body.to_string()) {
                    Some((_, parts)) => {
                        refuse(ctx, catalogs, &parts, &NestedWrite::Unreadable).await
                    }
                    None => Ok(()),
                }
            }
            _ => Ok(()),
        },
        Statement::Update(update) => match object_name_from_table_with_joins(&update.table) {
            Some(name) => {
                let write = NestedWrite::Update(&update.assignments);
                refuse(ctx, catalogs, &name_parts(name), &write).await
            }
            None => Ok(()),
        },
        Statement::Merge(merge) => match &merge.table {
            TableFactor::Table { name, .. } => {
                let write = NestedWrite::Merge(&merge.clauses);
                refuse(ctx, catalogs, &name_parts(name), &write).await
            }
            _ => Ok(()),
        },
        _ => Ok(()),
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
