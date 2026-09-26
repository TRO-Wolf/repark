//! `CALL <catalog>.system.rewrite_data_files(…)` over the fork's `RewriteDataFiles`.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::expr::Predicate;
use iceberg::maintenance::{RewriteDataFiles, RewriteStrategy, ZOrderSpec};
use iceberg::spec::{SortField, SortOrder, Transform, Type};
use iceberg::{Catalog, Error, TableIdent, table::Table};
use repark_core::illegal_argument_error;

use super::compute_table_stats::spark_type_name;
use super::params::params_for;
use super::rewrite_options::{
    RewriteOptions, extract_option_pairs, has_option_key, parse_rdf_options,
};
use super::rewrite_where::parse_rewrite_where;
use super::{CallArgs, bytes_as_i64, count_as_i32, resolve_table_ident};
use crate::call_args::bind;
use crate::sort_order_parse::{
    OrderParseError, WriteOrderField, ZOrderScan, parse_identity_sort_order, parse_zorder_columns,
};
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
    let bound = bind(
        args,
        params_for("rewrite_data_files"),
        &["remove-dangling-deletes"],
    )?;
    let strategy_arg = bound.optional_string("strategy")?;
    let sort_order_arg = bound.optional_string("sort_order")?;
    let table_arg = bound.require_string("table")?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let strategy = resolve_strategy(strategy_arg.as_deref(), sort_order_arg.as_deref(), &table)?;
    let pairs = extract_option_pairs(&bound, "rewrite_data_files")?;
    let mut options = parse_rdf_options(&pairs, &table, strategy)?;
    if !has_option_key(&pairs, "remove-dangling-deletes") {
        options.remove_dangling_deletes = bound.optional_bool("remove-dangling-deletes")?;
    }
    let branch = bound.optional_string("branch")?;
    if branch.is_some() && options.remove_dangling_deletes.unwrap_or(false) {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_data_files 'branch' cannot be combined with 'remove-dangling-deletes' \
             yet -- the fork's dangling-delete pass reads main's head, not the named branch"
                .to_string(),
        ));
    }
    let where_predicate = match bound.optional_string("where")? {
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
        branch.as_deref(),
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
    branch: Option<&str>,
) -> Result<DataFrame> {
    let remove_dangling = options.remove_dangling_deletes.unwrap_or(false);
    let mut action = RewriteDataFiles::new(table)
        .remove_dangling_deletes(remove_dangling)
        .strategy(options.strategy.clone());
    if let Some(name) = branch {
        action = action.branch(name);
    }
    if let Some(count) = options.shuffle_partitions_per_file {
        action = action.shuffle_partitions_per_file(count);
    }
    if let Some(factor) = options.compression_factor {
        action = action.compression_factor(factor);
    }
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
    let result = match Box::pin(action.execute(catalog.as_ref())).await {
        Err(error) if is_fork_validation_refusal(&error) => {
            return Err(illegal_argument_error(error.message().to_string()));
        }
        result => result.map_err(iceberg_err)?,
    };

    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, Arc::clone(&catalog), catalog_name, &namespace).await?;
    rewrite_result_dataframe(ctx, &result)
}

fn is_fork_validation_refusal(error: &Error) -> bool {
    const NEEDLES: [&str; 4] = [
        "Cannot sort data without a valid sort order",
        "Cannot ZOrder when no columns are specified",
        "Cannot find column '",
        "snapshot ref '",
    ];
    let message = error.to_string();
    NEEDLES.iter().any(|needle| message.contains(needle))
}

enum SortTerms {
    Absent,
    Identity(Vec<WriteOrderField>),
    ZOrder(Vec<String>),
}

fn resolve_strategy(
    strategy: Option<&str>,
    sort_order: Option<&str>,
    table: &Table,
) -> Result<RewriteStrategy> {
    if strategy.is_none() && sort_order.is_none() {
        return Ok(RewriteStrategy::BinPack);
    }
    let terms = match sort_order {
        None => SortTerms::Absent,
        Some(text) => match parse_zorder_columns(text) {
            ZOrderScan::Mixed => {
                return Err(illegal_argument_error(format!(
                    "Cannot mix identity sort columns and a Zorder sort expression: {text}"
                )));
            }
            ZOrderScan::Columns(columns) => SortTerms::ZOrder(columns),
            ZOrderScan::Absent => SortTerms::Identity(
                parse_identity_sort_order(text).map_err(|error| call_order_error(error, text))?,
            ),
        },
    };
    let normalized = strategy.map(|raw| raw.trim().to_ascii_lowercase());
    match normalized.as_deref() {
        None | Some("sort") => match terms {
            SortTerms::ZOrder(columns) => Ok(RewriteStrategy::ZOrder(ZOrderSpec::new(columns))),
            SortTerms::Identity(fields) => {
                Ok(RewriteStrategy::Sort(call_sort_order(&fields, table)?))
            }
            SortTerms::Absent => Ok(RewriteStrategy::SortByTableOrder),
        },
        Some("binpack") if sort_order.is_some() => Err(illegal_argument_error(
            "Cannot set rewrite mode, it has already been set to BIN-PACK".to_string(),
        )),
        Some("binpack") => Ok(RewriteStrategy::BinPack),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported strategy: {}. Only binpack or sort is supported",
            strategy.unwrap_or_default()
        ))),
    }
}

fn call_order_error(error: OrderParseError, text: &str) -> DataFusionError {
    match error {
        OrderParseError::Transform { name } => DataFusionError::NotImplemented(format!(
            "CALL rewrite_data_files sort_order transform `{name}(…)` is not supported yet — \
             only identity sort columns and zorder(…) are ported"
        )),
        _ => illegal_argument_error(format!("Unable to parse sortOrder: {text}")),
    }
}

fn call_sort_order(fields: &[WriteOrderField], table: &Table) -> Result<SortOrder> {
    let schema = table.metadata().current_schema();
    let mut builder = SortOrder::builder();
    for field in fields {
        let Some(source_id) = schema.field_id_by_name(&field.name) else {
            return Err(illegal_argument_error(format!(
                "Cannot find field '{}' in struct: {}",
                field.name,
                spark_type_name(&Type::Struct(schema.as_struct().clone()))
            )));
        };
        builder.with_sort_field(
            SortField::builder()
                .source_id(source_id)
                .transform(Transform::Identity)
                .direction(field.direction)
                .null_order(field.null_order)
                .build(),
        );
    }
    builder.build(schema).map_err(iceberg_err)
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
