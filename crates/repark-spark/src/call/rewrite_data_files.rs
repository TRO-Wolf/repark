//! `CALL <catalog>.system.rewrite_data_files(…)` over the fork's `RewriteDataFiles`.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::Catalog;
use iceberg::maintenance::RewriteDataFiles;

use super::rewrite_where::parse_rewrite_where;
use super::{CallArgs, bytes_as_i64, count_as_i32, resolve_table_ident};
use crate::call_args::expr_as_string;
use crate::{iceberg_err, reregister};

/// Execute `CALL <catalog>.system.rewrite_data_files(table => …)`.
///
/// # Errors
/// Plan / `NotImplemented` / iceberg commit failures as [`DataFusionError`].
pub(super) async fn execute_rewrite_data_files(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    args.reject_unknown_named(&[
        "table",
        "strategy",
        "sort_order",
        "options",
        "where",
        "remove-dangling-deletes",
    ])?;
    args.reject_excess_positional(2)?;
    refuse_unsupported_strategy(args)?;
    if args.has_named("sort_order") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_data_files sort_order is not supported — fork R135 deferred \
             (sort / zOrder strategies); only default binpack is available"
                .to_string(),
        ));
    }
    if args.has_named("options") {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_data_files options map is not supported in v1 — use table \
             properties / defaults (fork R135 binpack defaults: min_input_files=5, …)"
                .to_string(),
        ));
    }

    let table_arg = args.require_string("table", 0)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;

    let remove_dangling_deletes = args
        .optional_bool("remove-dangling-deletes", None)?
        .unwrap_or(false);
    let where_predicate = match args.optional_string("where")? {
        Some(where_sql) => Some(parse_rewrite_where(
            where_sql.as_str(),
            table.metadata().current_schema(),
        )?),
        None => None,
    };
    let mut action = RewriteDataFiles::new(table).remove_dangling_deletes(remove_dangling_deletes);
    if let Some(predicate) = where_predicate {
        action = action.filter(predicate);
    }
    let result = action
        .execute(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;
    rewrite_result_dataframe(ctx, &result)
}

fn refuse_unsupported_strategy(args: &CallArgs) -> Result<()> {
    let strategy = if let Some(named) = args.optional_string("strategy")? {
        Some(named)
    } else if args.positional.len() > 1 {
        Some(expr_as_string(&args.positional[1], "strategy")?)
    } else {
        None
    };
    let Some(strategy) = strategy else {
        return Ok(());
    };
    let normalized = strategy.trim().to_ascii_lowercase();
    if normalized == "binpack" {
        return Ok(());
    }
    if normalized == "sort" {
        return Err(DataFusionError::NotImplemented(format!(
            "CALL rewrite_data_files strategy `{strategy}` is not supported — only \
             binpack is ported (fork R135 deferred: sort / zOrder strategies)"
        )));
    }
    Err(DataFusionError::Plan(format!(
        "unsupported strategy: {strategy}. Only binpack or sort is supported"
    )))
}

fn rewrite_result_dataframe(
    ctx: &SessionContext,
    result: &iceberg::maintenance::RewriteDataFilesResult,
) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("rewritten_data_files_count", DataType::Int32, false),
        Field::new("added_data_files_count", DataType::Int32, false),
        Field::new("rewritten_bytes_count", DataType::Int64, false),
        Field::new("failed_data_files_count", DataType::Int32, false),
        Field::new("removed_delete_files_count", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.rewritten_data_files_count,
            )?])),
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.added_data_files_count,
            )?])),
            Arc::new(Int64Array::from(vec![bytes_as_i64(
                result.rewritten_bytes_count,
            )?])),
            Arc::new(Int32Array::from(vec![0])),
            Arc::new(Int32Array::from(vec![count_as_i32(
                result.removed_delete_files_count,
            )?])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}
