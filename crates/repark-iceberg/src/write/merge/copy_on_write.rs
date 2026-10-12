use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt};
use iceberg::Catalog;
use iceberg::spec::{DataFile, ManifestContentType};
use iceberg::table::Table;
use repark_common::spark_error;
use tracing::Instrument;

use super::insert::{insert_stream_checked, update_stream_checked};
use super::merge_sql::MergeSql;
use super::{
    MergeSpec, MergeTarget, commit_on_ref, cow_scratch, iceberg_err, not_matched_by_source,
    note_discovery_path_alloc, require_non_null_i64, session_staging, skip_cardinality, stream_sql,
    strict_cast,
};
use crate::write::concurrency::concurrency_from_ctx;
use crate::write::session_write_conf::resolve_empty_session_write;

pub(super) async fn plan_and_commit_cow(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &MergeSpec,
    target: &MergeTarget<'_>,
    sql: &MergeSql<'_>,
) -> Result<()> {
    let MergeTarget {
        table,
        write_schema,
        snapshot_id,
        partitions: _,
        conflict_filter: _,
    } = *target;
    let (affected, new_files, snapshot_extra) = async {
        let affected = if not_matched_by_source::is_present(spec) {
            if !spec.matched.is_empty() {
                let _ = affected_files(ctx, sql, skip_cardinality(spec)).await?;
            }
            not_matched_by_source::all_current_data_file_paths(table).await?
        } else if spec.matched.is_empty() {
            Vec::new()
        } else {
            affected_files(ctx, sql, skip_cardinality(spec)).await?
        };
        let mut streams: Vec<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> =
            Vec::new();
        let mut rewrite_scratches = cow_scratch::MergeScratchGuard::new(ctx);
        if !affected.is_empty() {
            let rewrite_scratch = cow_scratch::maybe_register_file_scoped_rewrite_target(
                ctx,
                table,
                snapshot_id,
                write_schema,
                &affected,
            )?;
            let rewrite_sql = if let Some(ref rewrite_name) = rewrite_scratch {
                rewrite_scratches.push(rewrite_name.clone());
                sql.rewrite_sql_allowlisted(rewrite_name, write_schema)
            } else {
                let path_table = cow_scratch::register_affected_paths_table(ctx, &affected)?;
                rewrite_scratches.push(path_table.clone());
                sql.rewrite_sql_path_semijoin(sql.target_name, &path_table, write_schema)
            };
            let rewrite_stream =
                update_stream_checked(ctx, sql, &rewrite_sql, write_schema).await?;
            streams.push(Box::pin(rewrite_stream));
        }
        for index in 0..spec.not_matched.len() {
            let insert_sql = sql.insert_sql(index, table, write_schema)?;
            streams.push(Box::pin(
                insert_stream_checked(ctx, &insert_sql, write_schema).await?,
            ));
        }
        let concurrency = concurrency_from_ctx(ctx);
        let (snapshot_extra, staging) = resolve_empty_session_write(ctx)?;
        let chained = futures::stream::iter(streams).flatten();
        let write_result = session_staging::write_new_data_files_from_stream_with(
            table,
            write_schema,
            chained,
            concurrency,
            &staging,
        )
        .await;
        drop(rewrite_scratches);
        Ok::<_, DataFusionError>((affected, write_result?, snapshot_extra))
    }
    .instrument(tracing::info_span!("merge.join"))
    .await?;

    let file_count = new_files.len() as u64;
    let affected_entries = resolve_affected_data_files(table, snapshot_id, &affected).await?;
    tracing::info_span!("merge.write_data", files = file_count).in_scope(|| ());
    commit_on_ref(
        catalog,
        table,
        snapshot_id,
        affected_entries,
        new_files,
        &target.conflict_filter,
        spec.commit_branch.as_deref(),
        &snapshot_extra,
    )
    .instrument(tracing::info_span!("merge.commit", files = file_count))
    .await
}

pub(crate) async fn resolve_affected_data_files(
    table: &Table,
    snapshot_id: Option<i64>,
    affected: &[String],
) -> Result<Vec<DataFile>> {
    async {
        if affected.is_empty() {
            return Ok(Vec::new());
        }
        let metadata = table.metadata();
        let snapshot = match snapshot_id {
            Some(id) => metadata.snapshot_by_id(id),
            None => metadata.current_snapshot(),
        }
        .ok_or_else(|| {
            DataFusionError::Internal(
                "affected files exist but the table has no snapshot".to_string(),
            )
        })?;
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .map_err(iceberg_err)?;
        let wanted: HashSet<&str> = affected.iter().map(String::as_str).collect();
        let mut files = Vec::with_capacity(affected.len());
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(iceberg_err)?;
            for entry in manifest.entries() {
                if entry.is_alive() && wanted.contains(entry.data_file().file_path()) {
                    files.push(entry.data_file().clone());
                }
            }
        }
        if files.len() != wanted.len() {
            return Err(DataFusionError::Internal(format!(
                "resolved {} of {} affected data files in the pinned snapshot's manifests",
                files.len(),
                wanted.len()
            )));
        }
        Ok(files)
    }
    .instrument(tracing::info_span!(
        "merge.resolve_affected",
        affected = affected.len()
    ))
    .await
}

async fn affected_files(
    ctx: &SessionContext,
    sql: &MergeSql<'_>,
    skip_cardinality: bool,
) -> Result<Vec<String>> {
    let mut stream = stream_sql(ctx, &sql.match_discovery_sql()).await?;
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        fold_discovery_batch_into_affected(&batch, &mut seen, skip_cardinality)?;
    }
    Ok(seen.into_iter().collect())
}

fn fold_discovery_batch_into_affected(
    batch: &RecordBatch,
    seen: &mut HashSet<String>,
    skip_cardinality: bool,
) -> Result<()> {
    if batch.num_columns() < 4 {
        return Err(DataFusionError::Internal(format!(
            "match-discovery batch has {} columns; expected >= 4",
            batch.num_columns()
        )));
    }
    let files_col = cast_with_options(batch.column(0), &DataType::Utf8, &strict_cast())?;
    let paths = files_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery `_file` is not a string array".into())
        })?;
    let positions = cast_with_options(batch.column(1), &DataType::Int64, &strict_cast())?;
    let ordinals = positions
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery `_pos` is not an Int64 array".into())
        })?;
    let counts = cast_with_options(batch.column(2), &DataType::Int64, &strict_cast())?;
    let match_counts = counts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery match_count is not Int64".into())
        })?;
    let mutated = cast_with_options(batch.column(3), &DataType::Int64, &strict_cast())?;
    let is_mutated = mutated
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery is_mutated is not Int64".into())
        })?;
    for row in 0..batch.num_rows() {
        if paths.is_null(row) || ordinals.is_null(row) {
            return Err(DataFusionError::Internal(
                "match-discovery row has a NULL `_file`/`_pos` row identity".to_string(),
            ));
        }
        let match_count = require_non_null_i64(match_counts, row, "match_count")?;
        let is_mutated_flag = require_non_null_i64(is_mutated, row, "is_mutated")?;
        if match_count > 1 && !skip_cardinality {
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::MERGE_CARDINALITY_VIOLATION,
                &[],
            )));
        }
        if is_mutated_flag == 1 {
            let path_str = paths.value(row);
            if !seen.contains(path_str) {
                note_discovery_path_alloc();
                seen.insert(path_str.to_string());
            }
        }
    }
    Ok(())
}
