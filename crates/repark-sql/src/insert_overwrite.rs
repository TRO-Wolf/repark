//! ANSI-door `INSERT OVERWRITE … PARTITION (…)` — Q9 still omits whole-table overwrite.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{Insert, ObjectName, TableObject};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::EngineContext;
use repark_iceberg::write::{
    OverwriteMode, OverwritePlan, commit_overwrite_by_row_filter, commit_overwrite_replace_all,
    commit_replace_partitions, partition_overwrite_request_from_exprs, plan_overwrite,
    stage_static_partition_overwrite_files, static_partition_source_columns,
    write_overwrite_staged_files_from_stream,
};

use crate::schema_ddl::{catalog_handle, name_parts};

/// Route a parsed `INSERT OVERWRITE`: PARTITION forms execute; whole-table stays Q9.
/// # Errors
/// Q9 refusal, parse, staging, or commit failures as [`DataFusionError`].
pub(crate) async fn execute_insert_overwrite(
    cx: &EngineContext<'_>,
    insert: &Insert,
) -> Result<DataFrame> {
    match &insert.partitioned {
        Some(partition_exprs) => execute_partition_overwrite(cx, insert, partition_exprs).await,
        None => Err(crate::refusals::insert_overwrite(&insert.table.to_string())),
    }
}

async fn execute_partition_overwrite(
    cx: &EngineContext<'_>,
    insert: &Insert,
    partition_exprs: &[datafusion::sql::sqlparser::ast::Expr],
) -> Result<DataFrame> {
    let table_name = match &insert.table {
        TableObject::TableName(name) => name,
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => {
            return Err(DataFusionError::Plan(
                "INSERT OVERWRITE … PARTITION requires an Iceberg table name target".to_string(),
            ));
        }
    };
    let (catalog_name, ident) = resolve_iceberg_ident(table_name)?;
    let handle = catalog_handle(cx.catalogs, &catalog_name)?;
    let table = handle.load_table(&ident).await.map_err(|error| {
        DataFusionError::Plan(format!(
            "INSERT OVERWRITE … PARTITION target `{table_name}` could not be loaded: {error}"
        ))
    })?;
    let request = partition_overwrite_request_from_exprs(partition_exprs)?;
    let mode = OverwriteMode {
        session_dynamic: repark_core::partition_overwrite_mode_from_ctx(cx.ctx).is_dynamic(),
        ..OverwriteMode::default()
    };
    let overwrite_plan = plan_overwrite(&table, &request, mode)?;
    let source = insert.source.as_ref().ok_or_else(|| {
        DataFusionError::Plan(
            "INSERT OVERWRITE … PARTITION requires a SELECT or VALUES source".to_string(),
        )
    })?;
    let listed: Vec<String> = insert
        .columns
        .iter()
        .filter_map(|name| {
            name.0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.clone())
        })
        .collect();
    let reserved = static_partition_source_columns(&table, overwrite_plan.equalities())?;
    let filled = repark_iceberg::write::insert_defaults::overwrite_source_with_defaults(
        table.metadata().current_schema(),
        &listed,
        &reserved,
        source,
    )?;
    let column_names = filled.columns;
    let belt = repark_core::PreExecute::new(cx.ctx, cx.catalogs);
    let plan = belt.plan(&filled.sql).await?;
    belt.guard(&plan)?;
    let source_df = belt.execute(plan).await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(cx.ctx);
    let staged_files = if overwrite_plan.equalities().is_empty() {
        let stream = source_df.execute_stream().await?;
        write_overwrite_staged_files_from_stream(&table, stream, column_names, concurrency).await?
    } else {
        let batches = source_df.collect().await?;
        stage_static_partition_overwrite_files(
            &table,
            batches,
            overwrite_plan.equalities(),
            &column_names,
            concurrency,
        )
        .await?
    };
    match overwrite_plan {
        OverwritePlan::RowFilter(spec) => {
            commit_overwrite_by_row_filter(handle, &table, staged_files, spec.predicate).await?;
        }
        OverwritePlan::ReplacePartitions(_) => {
            commit_replace_partitions(handle, &table, staged_files).await?;
        }
        OverwritePlan::WholeTable => {
            let staged_files = staged_files
                .into_iter()
                .filter(|file| file.record_count() > 0)
                .collect();
            commit_overwrite_replace_all(handle, &table, staged_files).await?;
        }
    }
    let leaf = ident.namespace().as_ref().last().cloned().ok_or_else(|| {
        DataFusionError::Plan(
            "INSERT OVERWRITE … PARTITION target namespace has no leaf".to_string(),
        )
    })?;
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        cx.ctx,
        Arc::clone(handle),
        &catalog_name,
        &[&leaf],
    )
    .await?;
    cx.ctx.read_empty()
}

fn resolve_iceberg_ident(table_name: &ObjectName) -> Result<(String, TableIdent)> {
    let parts = name_parts(table_name);
    let [catalog, namespace @ .., table] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE … PARTITION target must be `catalog.schema.table`, got `{table_name}`"
        )));
    };
    if namespace.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE … PARTITION target must be `catalog.schema.table`, got `{table_name}`"
        )));
    }
    let ident = TableIdent::new(
        NamespaceIdent::from_strs(namespace).map_err(|error| {
            DataFusionError::Plan(format!(
                "INSERT OVERWRITE … PARTITION namespace is invalid: {error}"
            ))
        })?,
        table.clone(),
    );
    Ok((catalog.clone(), ident))
}
