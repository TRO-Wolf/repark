use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::Date32Array;
use datafusion::arrow::array::Date64Array;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::array::LargeStringArray;
use datafusion::arrow::array::PrimitiveArray;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::array::StringViewArray;
use datafusion::arrow::array::StructArray;
use datafusion::arrow::datatypes::ArrowPrimitiveType;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Int8Type;
use datafusion::arrow::datatypes::Int16Type;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::datatypes::TimestampMicrosecondType;
use datafusion::arrow::datatypes::TimestampMillisecondType;
use datafusion::arrow::datatypes::TimestampNanosecondType;
use datafusion::arrow::datatypes::TimestampSecondType;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::TableIdent;
use iceberg::spec::TableMetadata;

use super::plan_partitioning_score::BUCKET_WIDTHS;
use super::plan_partitioning_score::ColumnKind;
use super::plan_partitioning_score::Grain;
use super::plan_partitioning_score::RatedColumn;
use super::plan_partitioning_score::ScoredSpec;
use super::plan_partitioning_score::SpecPart;
use super::plan_partitioning_score::TemporalGrain;
use super::plan_partitioning_score::ValueKey;
use super::plan_partitioning_score::accumulate;
use super::plan_partitioning_score::column_kind;
use super::plan_partitioning_score::identity_choice;
use super::plan_partitioning_score::is_better;
use super::plan_partitioning_score::part_label;
use super::plan_partitioning_score::score_pair;
use super::plan_partitioning_score::score_single;
use super::plan_partitioning_score::spec_label;
use super::{CallArgs, resolve_table_ident};
use crate::{catalog_handle, iceberg_err};
use repark_core::CatalogRegistry;

const RESIDUE_NOTE: &str = "AP-0-R-001: projected_files_at_target derives from pre-rewrite file bytes and measured 76-88% high on the AP-0 beds; projected_partitions measured exact";

pub(super) async fn execute_plan_partitioning(
    ctx: &SessionContext,
    catalog_name: &str,
    args: &CallArgs,
    catalogs: &CatalogRegistry,
) -> Result<DataFrame> {
    args.reject_unknown_named(&["table", "target_file_size_bytes"])?;
    args.reject_excess_positional(2)?;
    let table_arg = args.require_string("table", 0)?;
    let target = parse_target(args)?;
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog_handle(catalogs, catalog_name)?
        .load_table(&ident)
        .await
        .map_err(iceberg_err)?;
    let snapshot_id = table.metadata().current_snapshot_id().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "plan_partitioning refuses table `{table_arg}`: it has no current snapshot to plan against"
        ))
    })?;
    refuse_extra_branches(
        &read_branch_names(ctx, catalogs, catalog_name, &ident).await?,
        &table_arg,
    )?;
    let spec_count = table.metadata().partition_specs_iter().len();
    let inventory = inventory(table.metadata());
    let files = read_files(ctx, catalogs, catalog_name, &ident).await?;
    let rated = rate_columns(&inventory.columns, &files);
    let inputs = PlanInputs {
        table_arg: &table_arg,
        catalog_name,
        snapshot_id,
        target,
        rated: &rated,
        sizes: &files.sizes,
        unsupported: &inventory.unsupported,
        spec_count,
    };
    let rows = build_plan(&inputs)?;
    plan_dataframe(ctx, &rows)
}

fn parse_target(args: &CallArgs) -> Result<u64> {
    let raw = args
        .optional_i64("target_file_size_bytes", Some(1))?
        .ok_or_else(|| {
            DataFusionError::Plan(
                "plan_partitioning requires `target_file_size_bytes` (named or positional #1)"
                    .to_string(),
            )
        })?;
    u64::try_from(raw)
        .ok()
        .filter(|target| *target > 0)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "plan_partitioning `target_file_size_bytes` must be a positive integer, got {raw}"
            ))
        })
}

fn refuse_extra_branches(branches: &[String], table_arg: &str) -> Result<()> {
    if let Some(name) = branches.first() {
        return Err(DataFusionError::Plan(format!(
            "plan_partitioning refuses table `{table_arg}`: branch `{name}` exists besides `main`; drop the branch before planning"
        )));
    }
    Ok(())
}

struct Inventory {
    columns: Vec<(String, ColumnKind)>,
    unsupported: Vec<(String, String)>,
}

fn inventory(metadata: &TableMetadata) -> Inventory {
    let mut columns = Vec::new();
    let mut unsupported = Vec::new();
    for field in metadata.current_schema().as_struct().fields() {
        match column_kind(&field.field_type) {
            Some(kind) => columns.push((field.name.clone(), kind)),
            None => unsupported.push((
                field.name.clone(),
                "unsupported type for partitioning".to_string(),
            )),
        }
    }
    Inventory {
        columns,
        unsupported,
    }
}

fn quote_ident(part: &str) -> String {
    format!("\"{}\"", part.replace('"', "\"\""))
}

fn metadata_path(catalog_name: &str, ident: &TableIdent, suffix: &str) -> String {
    let mut parts = vec![catalog_name.to_string()];
    parts.extend(ident.namespace().as_ref().iter().cloned());
    parts.push(ident.name().to_string());
    parts.push(suffix.to_string());
    parts
        .iter()
        .map(|part| quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

async fn collect_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let frame = Box::pin(crate::router::execute(ctx, catalogs, sql)).await?;
    frame.collect().await
}

async fn read_branch_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    ident: &TableIdent,
) -> Result<Vec<String>> {
    let path = metadata_path(catalog_name, ident, "refs");
    let batches = collect_sql(
        ctx,
        catalogs,
        &format!("SELECT \"name\", \"type\" FROM {path}"),
    )
    .await?;
    let mut branches = Vec::new();
    for batch in &batches {
        let names = batch
            .column_by_name("name")
            .ok_or_else(|| {
                DataFusionError::Plan(format!("refs table over `{path}` missed column `name`"))
            })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "refs table over `{path}` column `name` is not Utf8"
                ))
            })?;
        let kinds = batch
            .column_by_name("type")
            .ok_or_else(|| {
                DataFusionError::Plan(format!("refs table over `{path}` missed column `type`"))
            })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "refs table over `{path}` column `type` is not Utf8"
                ))
            })?;
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "BRANCH" && names.value(row) != "main" {
                branches.push(names.value(row).to_string());
            }
        }
    }
    branches.sort();
    Ok(branches)
}

struct FilesSnapshot {
    sizes: Vec<u64>,
    metrics: Vec<StructArray>,
}

async fn read_files(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    ident: &TableIdent,
) -> Result<FilesSnapshot> {
    let path = metadata_path(catalog_name, ident, "files");
    let batches = collect_sql(
        ctx,
        catalogs,
        &format!(
            "SELECT \"file_size_in_bytes\", \"readable_metrics\" FROM {path} WHERE \"content\" = 0"
        ),
    )
    .await?;
    let mut sizes = Vec::new();
    let mut metrics = Vec::new();
    for batch in &batches {
        let raw_sizes = batch
            .column_by_name("file_size_in_bytes")
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "files table over `{path}` missed column `file_size_in_bytes`"
                ))
            })?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "files table over `{path}` column `file_size_in_bytes` is not Int64"
                ))
            })?;
        for row in 0..batch.num_rows() {
            if !raw_sizes.is_valid(row) {
                return Err(DataFusionError::Plan(format!(
                    "files table over `{path}` answered a null `file_size_in_bytes`"
                )));
            }
            sizes.push(u64::try_from(raw_sizes.value(row)).map_err(|_| {
                DataFusionError::Plan(format!(
                    "files table over `{path}` file size does not fit u64"
                ))
            })?);
        }
        let column = batch
            .column_by_name("readable_metrics")
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "files table over `{path}` missed column `readable_metrics`"
                ))
            })?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "files table over `{path}` column `readable_metrics` is not a struct"
                ))
            })?
            .clone();
        metrics.push(column);
    }
    Ok(FilesSnapshot { sizes, metrics })
}

enum NormScalar {
    EpochSecs(i64),
    IntValue(i64),
    TextValue(String),
}

fn timestamp_scalar<Raw>(array: &dyn Array, index: usize, divisor: i64) -> Option<NormScalar>
where
    Raw: ArrowPrimitiveType<Native = i64>,
{
    let values = array.as_any().downcast_ref::<PrimitiveArray<Raw>>()?;
    Some(NormScalar::EpochSecs(
        values.value(index).div_euclid(divisor),
    ))
}

fn int_scalar<Raw>(array: &dyn Array, index: usize) -> Option<NormScalar>
where
    Raw: ArrowPrimitiveType,
    i64: TryFrom<Raw::Native>,
{
    let values = array.as_any().downcast_ref::<PrimitiveArray<Raw>>()?;
    i64::try_from(values.value(index))
        .ok()
        .map(NormScalar::IntValue)
}

fn norm_scalar(array: &dyn Array, index: usize) -> Option<NormScalar> {
    if array.is_null(index) {
        return None;
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            timestamp_scalar::<TimestampSecondType>(array, index, 1)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            timestamp_scalar::<TimestampMillisecondType>(array, index, 1_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            timestamp_scalar::<TimestampMicrosecondType>(array, index, 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            timestamp_scalar::<TimestampNanosecondType>(array, index, 1_000_000_000)
        }
        DataType::Date32 => {
            let values = array.as_any().downcast_ref::<Date32Array>()?;
            i64::from(values.value(index))
                .checked_mul(86_400)
                .map(NormScalar::EpochSecs)
        }
        DataType::Date64 => {
            let values = array.as_any().downcast_ref::<Date64Array>()?;
            values
                .value(index)
                .div_euclid(86_400_000)
                .checked_mul(86_400)
                .map(NormScalar::EpochSecs)
        }
        DataType::Int8 => int_scalar::<Int8Type>(array, index),
        DataType::Int16 => int_scalar::<Int16Type>(array, index),
        DataType::Int32 => int_scalar::<Int32Type>(array, index),
        DataType::Int64 => int_scalar::<Int64Type>(array, index),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|values| NormScalar::TextValue(values.value(index).to_string())),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|values| NormScalar::TextValue(values.value(index).to_string())),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|values| NormScalar::TextValue(values.value(index).to_string())),
        _ => None,
    }
}

fn rate_columns(columns: &[(String, ColumnKind)], files: &FilesSnapshot) -> Vec<RatedColumn> {
    columns
        .iter()
        .map(|(name, kind)| rate_column(name, *kind, files))
        .collect()
}

fn column_bounds(files: &FilesSnapshot, name: &str) -> Vec<Option<(NormScalar, NormScalar)>> {
    let mut bounds = Vec::new();
    for batch in &files.metrics {
        let Some(column) = batch.column_by_name(name) else {
            for _ in 0..batch.len() {
                bounds.push(None);
            }
            continue;
        };
        let Some(metrics) = column.as_any().downcast_ref::<StructArray>() else {
            for _ in 0..batch.len() {
                bounds.push(None);
            }
            continue;
        };
        let lower = metrics.column_by_name("lower_bound");
        let upper = metrics.column_by_name("upper_bound");
        for row in 0..batch.len() {
            let pair = match (lower, upper) {
                (Some(low), Some(high)) => match (
                    norm_scalar(low.as_ref(), row),
                    norm_scalar(high.as_ref(), row),
                ) {
                    (Some(first), Some(second)) => Some((first, second)),
                    _ => None,
                },
                _ => None,
            };
            bounds.push(pair);
        }
    }
    bounds
}

fn pair_as_numbers(
    kind: ColumnKind,
    pair: Option<&(NormScalar, NormScalar)>,
) -> Option<(i64, i64)> {
    match (kind, pair) {
        (
            ColumnKind::Timestamp | ColumnKind::Date,
            Some((NormScalar::EpochSecs(low), NormScalar::EpochSecs(high))),
        ) => Some((*low, *high)),
        (ColumnKind::Int, Some((NormScalar::IntValue(low), NormScalar::IntValue(high)))) => {
            Some((*low, *high))
        }
        _ => None,
    }
}

fn pair_as_texts(
    kind: ColumnKind,
    pair: Option<&(NormScalar, NormScalar)>,
) -> Option<(String, String)> {
    match (kind, pair) {
        (ColumnKind::Text, Some((NormScalar::TextValue(low), NormScalar::TextValue(high)))) => {
            Some((low.clone(), high.clone()))
        }
        _ => None,
    }
}

fn rate_column(name: &str, kind: ColumnKind, files: &FilesSnapshot) -> RatedColumn {
    let raw = column_bounds(files, name);
    let mut numbers = Vec::with_capacity(raw.len());
    let mut texts = Vec::with_capacity(raw.len());
    for pair in &raw {
        numbers.push(pair_as_numbers(kind, pair.as_ref()));
        texts.push(pair_as_texts(kind, pair.as_ref()));
    }
    RatedColumn {
        name: name.to_string(),
        kind,
        numbers,
        texts,
    }
}

struct PlanFrameRow {
    label: String,
    score: f64,
    partitions: i64,
    projected: i64,
    ddl: String,
    calls: String,
    plan_id: String,
    notes: String,
}

fn plan_id(snapshot_id: i64, candidate: &str) -> String {
    let mut hasher = DefaultHasher::new();
    snapshot_id.hash(&mut hasher);
    candidate.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn render_table(table_arg: &str) -> String {
    table_arg.replace('\'', "''")
}

fn ddl_for(table_arg: &str, parts: &[SpecPart]) -> String {
    parts
        .iter()
        .map(|part| {
            format!(
                "ALTER TABLE {} ADD PARTITION FIELD {}",
                render_table(table_arg),
                part_label(part)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn calls_for(catalog_name: &str, table_arg: &str) -> String {
    let table = render_table(table_arg);
    [
        "rewrite_data_files",
        "rewrite_manifests",
        "expire_snapshots",
    ]
    .iter()
    .map(|procedure| format!("CALL {catalog_name}.system.{procedure}(table => '{table}')"))
    .collect::<Vec<_>>()
    .join("; ")
}

fn count_as_i64(count: usize) -> Result<i64> {
    i64::try_from(count).map_err(|_| {
        DataFusionError::Plan(format!(
            "plan_partitioning count {count} does not fit i64 (refusing to fabricate MAX)"
        ))
    })
}

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
fn projected_as_i64(projected: f64) -> Result<i64> {
    if projected.is_finite() && projected >= 0.0 && projected <= i64::MAX as f64 {
        Ok(projected as i64)
    } else {
        Err(DataFusionError::Plan(format!(
            "plan_partitioning projected file count {projected} does not fit i64"
        )))
    }
}

struct PlanInputs<'a> {
    table_arg: &'a str,
    catalog_name: &'a str,
    snapshot_id: i64,
    target: u64,
    rated: &'a [RatedColumn],
    sizes: &'a [u64],
    unsupported: &'a [(String, String)],
    spec_count: usize,
}

struct ScoredSingles {
    scored: Vec<ScoredSpec>,
    skipped: Vec<(String, String)>,
    best: Vec<(String, usize)>,
}

fn grains_for(column: &RatedColumn, skipped: &mut Vec<(String, String)>) -> Vec<Grain> {
    match column.kind {
        ColumnKind::Timestamp | ColumnKind::Date => vec![
            Grain::Temporal(TemporalGrain::Years),
            Grain::Temporal(TemporalGrain::Months),
            Grain::Temporal(TemporalGrain::Days),
            Grain::Temporal(TemporalGrain::Hours),
        ],
        ColumnKind::Int | ColumnKind::Text => match identity_choice(column) {
            None => {
                skipped.push((
                    column.name.clone(),
                    "no readable bounds in any file".to_string(),
                ));
                Vec::new()
            }
            Some(true) => vec![Grain::Identity],
            Some(false) => BUCKET_WIDTHS
                .iter()
                .map(|width| Grain::Bucket(*width))
                .collect(),
        },
    }
}

fn score_singles(inputs: &PlanInputs) -> ScoredSingles {
    let mut scored: Vec<ScoredSpec> = Vec::new();
    let mut skipped: Vec<(String, String)> = inputs.unsupported.to_vec();
    let mut best: Vec<(String, usize)> = Vec::new();
    for column in inputs.rated {
        let grains = grains_for(column, &mut skipped);
        let mut column_best: Option<usize> = None;
        for grain in grains {
            if let Some(candidate) =
                score_single(&column.name, grain, column, inputs.sizes, inputs.target)
            {
                let better = match column_best {
                    None => true,
                    Some(index) => is_better(
                        candidate.score,
                        &candidate.label,
                        scored[index].score,
                        &scored[index].label,
                    ),
                };
                scored.push(candidate);
                if better {
                    column_best = Some(scored.len() - 1);
                }
            }
        }
        if let Some(index) = column_best {
            best.push((column.name.clone(), index));
        }
    }
    ScoredSingles {
        scored,
        skipped,
        best,
    }
}

fn push_pairs(scored: &mut Vec<ScoredSpec>, best: &mut [(String, usize)], target: u64) {
    best.sort_by(|left, right| {
        let first = &scored[left.1];
        let second = &scored[right.1];
        first
            .score
            .total_cmp(&second.score)
            .then_with(|| first.label.cmp(&second.label))
            .then_with(|| left.0.cmp(&right.0))
    });
    let top: Vec<usize> = best.iter().take(3).map(|(_, index)| *index).collect();
    for (position, first) in top.iter().enumerate() {
        for second in top.iter().skip(position + 1) {
            let pair = score_pair(&scored[*first], &scored[*second], target);
            scored.push(pair);
        }
    }
}

fn push_unpartitioned(scored: &mut Vec<ScoredSpec>, sizes: &[u64], target: u64) {
    let items: Vec<(u64, Vec<ValueKey>)> = sizes
        .iter()
        .map(|size| (*size, vec![ValueKey::Number(0)]))
        .collect();
    let (score, partitions, projected) = accumulate(&items, target);
    scored.push(ScoredSpec {
        label: spec_label(&[]),
        parts: Vec::new(),
        score,
        partitions,
        projected,
        items,
        note: String::new(),
    });
}

fn table_notes(spec_count: usize) -> Vec<String> {
    let mut notes = vec![RESIDUE_NOTE.to_string()];
    if spec_count > 1 {
        notes.push(format!(
            "table carries {spec_count} partition specs; apply would rewrite to one spec"
        ));
    }
    notes
}

fn render_rows(
    inputs: &PlanInputs,
    scored: &[ScoredSpec],
    skipped: &[(String, String)],
) -> Result<Vec<PlanFrameRow>> {
    let table_notes = table_notes(inputs.spec_count);
    let skipped_note = skipped
        .iter()
        .map(|(name, reason)| format!("no candidate for {name}: {reason}"))
        .collect::<Vec<_>>()
        .join("; ");
    let last = scored.len().saturating_sub(1);
    let mut rows = Vec::with_capacity(scored.len());
    for (index, candidate) in scored.iter().enumerate() {
        let mut notes = candidate.note.clone();
        for table_note in &table_notes {
            if !notes.is_empty() {
                notes.push_str("; ");
            }
            notes.push_str(table_note);
        }
        if index == last && !skipped_note.is_empty() {
            if !notes.is_empty() {
                notes.push_str("; ");
            }
            notes.push_str(&skipped_note);
        }
        rows.push(PlanFrameRow {
            label: candidate.label.clone(),
            score: candidate.score,
            partitions: count_as_i64(candidate.partitions)?,
            projected: projected_as_i64(candidate.projected)?,
            ddl: ddl_for(inputs.table_arg, &candidate.parts),
            calls: calls_for(inputs.catalog_name, inputs.table_arg),
            plan_id: plan_id(inputs.snapshot_id, &candidate.label),
            notes,
        });
    }
    Ok(rows)
}

fn build_plan(inputs: &PlanInputs) -> Result<Vec<PlanFrameRow>> {
    let singles = score_singles(inputs);
    let mut scored = singles.scored;
    push_unpartitioned(&mut scored, inputs.sizes, inputs.target);
    let mut best = singles.best;
    push_pairs(&mut scored, &mut best, inputs.target);
    scored.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.projected.total_cmp(&right.projected))
            .then_with(|| left.label.cmp(&right.label))
    });
    render_rows(inputs, &scored, &singles.skipped)
}

fn plan_dataframe(ctx: &SessionContext, rows: &[PlanFrameRow]) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("candidate", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
        Field::new("projected_partitions", DataType::Int64, false),
        Field::new("projected_files_at_target", DataType::Int64, false),
        Field::new("ddl", DataType::Utf8, false),
        Field::new("calls", DataType::Utf8, false),
        Field::new("plan_id", DataType::Utf8, false),
        Field::new("notes", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.label.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.score).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.partitions).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.projected).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.ddl.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.calls.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.plan_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.notes.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_id_is_stable_per_pair_and_unique_per_candidate() {
        assert_eq!(plan_id(7, "days(ts)"), plan_id(7, "days(ts)"));
        assert_ne!(plan_id(7, "days(ts)"), plan_id(7, "months(ts)"));
        assert_ne!(plan_id(7, "days(ts)"), plan_id(8, "days(ts)"));
    }

    #[test]
    fn extra_branch_refusal_names_the_branch() {
        assert!(refuse_extra_branches(&[], "sales.t").is_ok());
        let error = refuse_extra_branches(&["feat".to_string()], "sales.t").expect_err("branch");
        let message = error.to_string();
        assert!(message.contains("feat") && message.contains("main"));
    }
}
