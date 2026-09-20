//! `CALL <catalog>.system.rewrite_data_files(…)` over the fork's `RewriteDataFiles`.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::expr::Predicate;
use iceberg::maintenance::RewriteDataFiles;
use iceberg::{Catalog, TableIdent, table::Table};

use super::rewrite_options::{
    RewriteOptions, extract_option_pairs, has_option_key, parse_rdf_options,
};
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
    let table_arg = args.require_string("table", 0)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let pairs = extract_option_pairs(args, "rewrite_data_files")?;
    let mut options = parse_rdf_options(&pairs, &table)?;
    if !has_option_key(&pairs, "remove-dangling-deletes") {
        options.remove_dangling_deletes = args.optional_bool("remove-dangling-deletes", None)?;
    }
    let where_predicate = match args.optional_string("where")? {
        Some(where_sql) => Some(parse_rewrite_where(
            where_sql.as_str(),
            table.metadata().current_schema(),
        )?),
        None => None,
    };
    Box::pin(run_rewrite(
        ctx,
        catalog,
        catalog_name,
        &ident,
        table,
        where_predicate,
        options,
    ))
    .await
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
pub(super) async fn run_rewrite(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    ident: &TableIdent,
    table: Table,
    where_predicate: Option<Predicate>,
    options: RewriteOptions,
) -> Result<DataFrame> {
    let remove_dangling = options.remove_dangling_deletes.unwrap_or(false);
    let mut action = RewriteDataFiles::new(table).remove_dangling_deletes(remove_dangling);
    if let Some(predicate) = where_predicate {
        action = action.filter(predicate);
    }
    if let Some(size) = options.target_file_size_bytes {
        action = action.target_file_size_bytes(size);
    }
    if let Some(size) = options.min_file_size_bytes {
        action = action.min_file_size_bytes(u64::try_from(size).unwrap_or(0));
    }
    if let Some(size) = options.max_file_size_bytes {
        action = action.max_file_size_bytes(u64::try_from(size).unwrap_or(0));
    }
    if let Some(count) = options.min_input_files {
        action = action.min_input_files(count);
    }
    if let Some(count) = options.delete_file_threshold {
        action = action.delete_file_threshold(count);
    }
    if let Some(ratio) = options.delete_ratio_threshold {
        action = action.delete_ratio_threshold(ratio);
    }
    if let Some(size) = options.max_file_group_size_bytes {
        action = action.max_file_group_size_bytes(size);
    }
    if let Some(flag) = options.use_starting_sequence_number {
        action = action.use_starting_sequence_number(flag);
    }
    action = action.rewrite_all(options.rewrite_all);
    action = action.partial_progress(options.partial_progress_enabled);
    if let Some(value) = options.partial_progress_max_commits
        && let Ok(commits) = usize::try_from(value)
    {
        action = action.partial_progress_max_commits(commits);
    }
    if let Some(value) = options.output_spec_id
        && let Ok(id) = i32::try_from(value)
    {
        action = action.output_spec_id(id);
    }
    action = action.rewrite_job_order(options.rewrite_job_order.into());
    if let Some(value) = options.max_concurrent_file_group_rewrites
        && let Ok(limit) = usize::try_from(value)
    {
        action = action.max_concurrent_file_group_rewrites(limit);
    }
    let result = Box::pin(action.execute(catalog.as_ref()))
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
