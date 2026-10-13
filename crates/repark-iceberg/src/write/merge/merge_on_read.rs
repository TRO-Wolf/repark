use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt};
use iceberg::Catalog;
use repark_common::spark_error;
use tracing::Instrument;

use super::insert::{insert_stream_checked, update_stream_checked};
use super::merge_sql::MergeSql;
use super::{
    MergeSpec, MergeTarget, commit_row_delta_on_ref_with_partitions, drain_partition_sink,
    not_matched_by_source, require_non_null_i64, session_staging, skip_cardinality, strict_cast,
};
use crate::write::concurrency::concurrency_from_ctx;
use crate::write::session_write_conf::resolve_empty_session_write;

pub(super) async fn plan_and_commit_mor(
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
        partitions,
        conflict_filter,
    } = target;
    let (table, write_schema, snapshot_id) = (*table, *write_schema, *snapshot_id);
    let (pairs, data_files, snapshot_extra, staging) = async {
        let mut streams: Vec<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> =
            Vec::new();
        let mut pairs = if spec.matched.is_empty() {
            Vec::new()
        } else {
            let (pairs, update_batches) = matched_work_mor(
                ctx,
                sql,
                write_schema,
                skip_cardinality(spec),
                &sql.matched_work_sql(write_schema),
            )
            .await?;
            if !update_batches.is_empty() {
                streams.push(Box::pin(futures::stream::iter(
                    update_batches.into_iter().map(Ok),
                )));
            }
            pairs
        };
        if not_matched_by_source::is_present(spec) {
            let (nmbs_pairs, nmbs_updates) = matched_work_mor(
                ctx,
                sql,
                write_schema,
                true,
                &not_matched_by_source::mor_work_sql(sql, write_schema),
            )
            .await?;
            pairs.extend(nmbs_pairs);
            if !nmbs_updates.is_empty() {
                streams.push(Box::pin(futures::stream::iter(
                    nmbs_updates.into_iter().map(Ok),
                )));
            }
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
        let data_files = session_staging::write_new_data_files_from_stream_with(
            table,
            write_schema,
            chained,
            concurrency,
            &staging,
        )
        .await?;
        Ok::<_, DataFusionError>((pairs, data_files, snapshot_extra, staging))
    }
    .instrument(tracing::info_span!("merge.join"))
    .await?;

    let file_count = data_files.len() as u64;
    let concurrency = concurrency_from_ctx(ctx);
    tracing::info_span!("merge.write_data", files = file_count).in_scope(|| ());
    commit_row_delta_on_ref_with_partitions(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        conflict_filter,
        spec.commit_branch.as_deref(),
        drain_partition_sink(partitions),
        &snapshot_extra,
        &staging,
    )
    .await
}

async fn matched_work_mor(
    ctx: &SessionContext,
    sql: &MergeSql<'_>,
    write_schema: &SchemaRef,
    skip_cardinality: bool,
    work_sql: &str,
) -> Result<(
    Vec<crate::write::position_delete::PositionDeletePair>,
    Vec<RecordBatch>,
)> {
    let mut stream = update_stream_checked(ctx, sql, work_sql, write_schema).await?;
    let mut path_intern: HashMap<String, usize> = HashMap::new();
    let mut unique_paths: Vec<std::sync::Arc<str>> = Vec::new();
    let mut seen_pair: HashSet<(usize, i64)> = HashSet::new();
    let mut pair_indices: Vec<(usize, i64)> = Vec::new();
    let mut update_batches = Vec::new();
    let data_field_count = write_schema.fields().len();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        consume_matched_work_batch(
            &batch,
            write_schema,
            data_field_count,
            &mut path_intern,
            &mut unique_paths,
            &mut seen_pair,
            &mut pair_indices,
            &mut update_batches,
            skip_cardinality,
        )?;
    }
    let pairs = pair_indices
        .into_iter()
        .map(|(path_index, pos)| (std::sync::Arc::clone(&unique_paths[path_index]), pos))
        .collect();
    Ok((pairs, update_batches))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn consume_matched_work_batch(
    batch: &RecordBatch,
    write_schema: &SchemaRef,
    data_field_count: usize,
    path_intern: &mut HashMap<String, usize>,
    unique_paths: &mut Vec<std::sync::Arc<str>>,
    seen_pair: &mut HashSet<(usize, i64)>,
    pair_indices: &mut Vec<(usize, i64)>,
    update_batches: &mut Vec<RecordBatch>,
    skip_cardinality: bool,
) -> Result<()> {
    if batch.num_columns() < 5 + data_field_count {
        return Err(DataFusionError::Internal(format!(
            "matched_work batch has {} columns; expected >= {}",
            batch.num_columns(),
            5 + data_field_count
        )));
    }
    let projected_field_count = batch.num_columns() - 5;
    let files = cast_with_options(batch.column(0), &DataType::Utf8, &strict_cast())?;
    let paths = files
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("matched_work `_file` is not a string array".into())
        })?;
    let positions = cast_with_options(batch.column(1), &DataType::Int64, &strict_cast())?;
    let ordinals = positions
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work `_pos` is not Int64".into()))?;
    let counts = cast_with_options(batch.column(2), &DataType::Int64, &strict_cast())?;
    let match_counts = counts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work match_count is not Int64".into()))?;
    let mutated = cast_with_options(batch.column(3), &DataType::Int64, &strict_cast())?;
    let is_mutated = mutated
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work is_mutated is not Int64".into()))?;
    let updated = cast_with_options(batch.column(4), &DataType::Int64, &strict_cast())?;
    let is_update = updated
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work is_update is not Int64".into()))?;

    let mut update_indices: Vec<u32> = Vec::new();
    for row in 0..batch.num_rows() {
        if paths.is_null(row) || ordinals.is_null(row) {
            return Err(DataFusionError::Internal(
                "matched_work row has a NULL `_file`/`_pos` row identity".to_string(),
            ));
        }
        let match_count = require_non_null_i64(match_counts, row, "match_count")?;
        let is_mutated_flag = require_non_null_i64(is_mutated, row, "is_mutated")?;
        let is_update_flag = require_non_null_i64(is_update, row, "is_update")?;
        if match_count > 1 && !skip_cardinality {
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::MERGE_CARDINALITY_VIOLATION,
                &[],
            )));
        }
        let path_str = paths.value(row);
        let path_index = if let Some(&index) = path_intern.get(path_str) {
            index
        } else {
            let index = unique_paths.len();
            let path_arc: std::sync::Arc<str> = std::sync::Arc::from(path_str);
            unique_paths.push(std::sync::Arc::clone(&path_arc));
            path_intern.insert(path_str.to_string(), index);
            index
        };
        let pos = ordinals.value(row);
        if is_mutated_flag == 1 && seen_pair.insert((path_index, pos)) {
            pair_indices.push((path_index, pos));
        }
        if is_update_flag == 1 {
            update_indices.push(u32::try_from(row).map_err(|_| {
                DataFusionError::Internal("matched_work row index exceeds u32".into())
            })?);
        }
    }
    if !update_indices.is_empty() {
        let data_fields: Vec<datafusion::arrow::datatypes::Field> = (0..projected_field_count)
            .map(|index| {
                if index < data_field_count {
                    let source = write_schema.field(index);
                    datafusion::arrow::datatypes::Field::new(
                        source.name(),
                        batch.column(5 + index).data_type().clone(),
                        source.is_nullable(),
                    )
                } else {
                    batch.schema().field(5 + index).as_ref().clone()
                }
            })
            .collect();
        let data_columns: Vec<ArrayRef> = (0..projected_field_count)
            .map(|index| batch.column(5 + index).clone())
            .collect();
        let data_batch =
            RecordBatch::try_new(Arc::new(ArrowSchema::new(data_fields)), data_columns)?;
        let indices = datafusion::arrow::array::UInt32Array::from(update_indices);
        let taken = datafusion::arrow::compute::take_record_batch(&data_batch, &indices)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        update_batches.push(taken);
    }
    Ok(())
}
