use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{Insert, TableObject};
use repark_core::EngineContext;

use crate::schema_ddl::catalog_handle;

pub(crate) async fn try_execute_session_insert(
    cx: &EngineContext<'_>,
    insert: &Insert,
    sql: &str,
    listed: Option<&[String]>,
    preloaded: Option<iceberg::table::Table>,
) -> Result<Option<DataFrame>> {
    if insert.overwrite
        || insert.replace_into
        || insert.partitioned.is_some()
        || !repark_iceberg::write::session_write_conf_is_set(cx.ctx)
    {
        return Ok(None);
    }
    let TableObject::TableName(table_name) = &insert.table else {
        return Ok(None);
    };
    let statement = datafusion::sql::sqlparser::ast::Statement::Insert(insert.clone());
    let Some((catalog_name, ident)) =
        repark_iceberg::write::insert_defaults::insert_target(&statement)
    else {
        return Ok(None);
    };
    if cx.catalogs.get(&catalog_name).is_none() {
        return Ok(None);
    }
    let handle = catalog_handle(cx.catalogs, &catalog_name)?;
    let Ok(table) = handle.load_table(&ident).await else {
        return Ok(None);
    };
    if insert.source.is_none() {
        return Ok(None);
    }
    let plan = crate::router::delegate_plan(cx, sql, listed, preloaded).await?;
    let source_df = repark_core::PreExecute::from_engine_context(cx)
        .execute(insert_input(&plan)?)
        .await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(cx.ctx);
    let (snapshot_extra, staging) = repark_iceberg::write::resolve_empty_session_write(cx.ctx)?;
    let staged = repark_iceberg::write::stage_overwrite_files_with(
        &table,
        stream,
        Vec::new(),
        concurrency,
        &staging,
    )
    .await?;
    repark_iceberg::write::commit_append_with_summary(
        handle,
        &table,
        staged,
        &snapshot_extra,
        None,
    )
    .await?;
    let leaf = ident.namespace().as_ref().last().cloned().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "INSERT target `{table_name}` has no namespace leaf"
        ))
    })?;
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        cx.ctx,
        std::sync::Arc::clone(handle),
        &catalog_name,
        &[&leaf],
    )
    .await?;
    cx.ctx.read_empty().map(Some)
}

fn insert_input(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    match plan {
        datafusion::logical_expr::LogicalPlan::Dml(dml)
            if matches!(dml.op, datafusion::logical_expr::WriteOp::Insert(_)) =>
        {
            Ok(dml.input.as_ref().clone())
        }
        other => Err(DataFusionError::Plan(format!(
            "session-conf INSERT planned a `{}`, not an insert",
            other.display()
        ))),
    }
}
