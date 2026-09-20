use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::Catalog;
use iceberg::maintenance::{RewriteTablePath, RewriteTablePathResult};
use repark_iceberg::catalog::write_text_file;

use super::{CallArgs, illegal_argument, resolve_table_ident};
use crate::call_args::expr_as_string;
use crate::iceberg_err;

struct RewriteArgs {
    table_arg: String,
    source: String,
    target: String,
    staging: Option<String>,
    create_file_list: bool,
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_rewrite_table_path(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    let parsed = parse_rewrite_args(args)?;

    let ident = resolve_table_ident(catalog_name, &parsed.table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let location = table.metadata().location().to_string();
    let metadata_file = table
        .metadata_location()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "rewrite_table_path loaded a table with no metadata location — refusing \
                 rather than reporting a version the engine cannot name"
                    .to_string(),
            )
        })?
        .to_string();
    refuse_prefix_mismatch(&metadata_file, &location, &parsed.source)?;

    let staging = parsed
        .staging
        .unwrap_or_else(|| default_staging_location(&location));
    let manifest_count = i32::try_from(table.metadata().snapshots().count()).map_err(|_| {
        DataFusionError::Plan(
            "CALL rewrite_table_path snapshot count does not fit i32 (refusing to \
             fabricate MAX)"
                .to_string(),
        )
    })?;
    let file_io = table.file_io().clone();

    let result = RewriteTablePath::new(table)
        .rewrite_location_prefix(parsed.source.clone(), parsed.target.clone())
        .staging_location(staging.clone())
        .execute(&file_io)
        .await
        .map_err(iceberg_err)?;

    let mut lines = copy_plan_lines(&result);
    lines.push(format!(
        "{},{}",
        result.staged_metadata_location,
        staged_metadata_target(&staging, &result.staged_metadata_location, &parsed.target)?
    ));
    lines.sort();
    lines.dedup();
    let file_list_location = if parsed.create_file_list {
        let path = format!("{}/file-list", staging.trim_end_matches('/'));
        write_text_file(&file_io, &path, &lines.join("\n"))
            .await
            .map_err(iceberg_err)?;
        path
    } else {
        "N/A".to_string()
    };
    let latest_version = metadata_file
        .rsplit('/')
        .next()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "rewrite_table_path metadata location has no file name — refusing rather \
                 than reporting a version the engine cannot name"
                    .to_string(),
            )
        })?
        .to_string();

    rewrite_result_dataframe(
        ctx,
        &latest_version,
        &file_list_location,
        manifest_count,
        staged_delete_count(&staging, &result)?,
    )
}

fn parse_rewrite_args(args: &CallArgs) -> Result<RewriteArgs> {
    args.reject_unknown_named(&[
        "table",
        "source_prefix",
        "target_prefix",
        "staging_location",
        "create_file_list",
        "start_version",
        "end_version",
    ])?;
    args.reject_excess_positional(7)?;
    if args.has_named("start_version") || args.has_named("end_version") || args.positional.len() > 5
    {
        return Err(DataFusionError::NotImplemented(
            "CALL rewrite_table_path start_version / end_version select an incremental \
             version range, which the owned fork's RewriteTablePath does not implement \
             (full rewrite only) — refusing rather than rewriting the wrong versions"
                .to_string(),
        ));
    }

    let source = args.require_string("source_prefix", 1)?;
    let target = args.require_string("target_prefix", 2)?;
    if source.trim().is_empty() || target.trim().is_empty() {
        return Err(DataFusionError::Plan(
            "CALL rewrite_table_path requires non-empty `source_prefix` and \
             `target_prefix`"
                .to_string(),
        ));
    }
    Ok(RewriteArgs {
        table_arg: args.require_string("table", 0)?,
        source,
        target,
        staging: optional_string_positional(args, "staging_location", 3)?,
        create_file_list: args
            .optional_bool("create_file_list", Some(4))?
            .unwrap_or(true),
    })
}

fn refuse_prefix_mismatch(metadata_file: &str, location: &str, source: &str) -> Result<()> {
    let trimmed = source.trim_end_matches('/');
    let with_sep = format!("{trimmed}/");
    for path in [metadata_file, location] {
        if path != trimmed && !path.starts_with(&with_sep) {
            return Err(illegal_argument(format!(
                "Path {path}/ does not start with {with_sep}"
            )));
        }
    }
    Ok(())
}

fn default_staging_location(location: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |span| span.as_nanos());
    let pid = std::process::id();
    format!(
        "{}/metadata/copy-table-staging-{nanos:x}-{pid:x}",
        location.trim_end_matches('/')
    )
}

fn copy_plan_lines(result: &RewriteTablePathResult) -> Vec<String> {
    result
        .copy_plan
        .iter()
        .map(|(from, to)| format!("{from},{to}"))
        .collect()
}

fn staged_metadata_target(staging: &str, staged_location: &str, target: &str) -> Result<String> {
    let staging_sep = format!("{}/", staging.trim_end_matches('/'));
    let relative = staged_location.strip_prefix(&staging_sep).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "rewrite_table_path staged {staged_location} outside its own staging {staging}"
        ))
    })?;
    Ok(format!("{}/{}", target.trim_end_matches('/'), relative))
}

fn staged_delete_count(staging: &str, result: &RewriteTablePathResult) -> Result<i32> {
    let staging_sep = format!("{}/", staging.trim_end_matches('/'));
    let count = result
        .copy_plan
        .iter()
        .filter(|(from, _)| from.starts_with(&staging_sep) && from.ends_with(".parquet"))
        .count();
    i32::try_from(count).map_err(|_| {
        DataFusionError::Plan(
            "CALL rewrite_table_path staged delete count does not fit i32 (refusing to \
             fabricate MAX)"
                .to_string(),
        )
    })
}

#[allow(clippy::missing_errors_doc)]
fn rewrite_result_dataframe(
    ctx: &SessionContext,
    latest_version: &str,
    file_list_location: &str,
    manifest_count: i32,
    delete_count: i32,
) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("latest_version", DataType::Utf8, false),
        Field::new("file_list_location", DataType::Utf8, false),
        Field::new(
            "rewritten_manifest_file_paths_count",
            DataType::Int32,
            false,
        ),
        Field::new("rewritten_delete_file_paths_count", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![latest_version])),
            Arc::new(StringArray::from(vec![file_list_location])),
            Arc::new(Int32Array::from(vec![manifest_count])),
            Arc::new(Int32Array::from(vec![delete_count])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

fn optional_string_positional(
    args: &CallArgs,
    name: &str,
    position: usize,
) -> Result<Option<String>> {
    if let Some(expr) = args.named.get(name) {
        return expr_as_string(expr, name).map(Some);
    }
    if let Some(expr) = args.positional.get(position) {
        return expr_as_string(expr, name).map(Some);
    }
    Ok(None)
}
