use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, ScalarValue, TableReference};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::SessionState;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::Cast;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, Partitioning, Projection, TableScan,
};
use datafusion::prelude::{DataFrame, SessionContext, col, lit};

use crate::text_scan::TextTableProvider;
use crate::{Result, engine_err};

use super::METADATA_COLUMN_NAME;
use super::error::FileMetadataError;
use super::scan::{FileHit, FileKind, FileMetadataScan, collect_file_hits};
use super::status::marker_kind;
use super::udf::{file_metadata_call, metadata_outer_field};
async fn reread_single_file(
    context: &SessionContext,
    kind: &FileKind,
    scan: &TableScan,
    read_path: &str,
) -> Result<DataFrame> {
    match kind {
        FileKind::Parquet => context
            .read_parquet(
                read_path,
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .map_err(engine_err),
        FileKind::Csv { options } => {
            crate::read_options::read_csv_path(context, read_path, options).await
        }
        FileKind::Json { options } => {
            let flag = crate::read_options::secret_column_flag(options)?;
            let json_options = crate::json_read_options_from_map(options)?;
            let frame = context
                .read_json(read_path, json_options)
                .await
                .map_err(engine_err)?;
            crate::read_options::apply_secret_column_flag(flag, frame.schema().as_ref())?;
            Ok(frame)
        }
        FileKind::Text => {
            let provider = source_as_provider(&scan.source).map_err(engine_err)?;
            let marker = provider
                .as_ref()
                .downcast_ref::<FileMetadataScan>()
                .ok_or_else(|| {
                    crate::Error::Analysis("text metadata scan lost its file marker".to_string())
                })?;
            let inner = marker
                .inner()
                .as_ref()
                .downcast_ref::<TextTableProvider>()
                .ok_or_else(|| {
                    crate::Error::Analysis(
                        "text metadata scan wraps an unexpected provider".to_string(),
                    )
                })?;
            let single = inner.for_single_file(read_path);
            context.read_table(single).map_err(engine_err)
        }
    }
}

fn cast_to_field(expr: Expr, field: &Field) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), field.data_type().clone())).alias(field.name())
}

async fn build_file_branch(
    context: &SessionContext,
    kind: &FileKind,
    scan: &TableScan,
    hit: &FileHit,
    data_fields: &[Field],
    partition_fields: &[Field],
    with_row_index: bool,
) -> Result<LogicalPlan> {
    let reread = reread_single_file(context, kind, scan, &hit.read_path).await?;
    let (_, reread_plan) = reread.into_parts();
    let reread_names = reread_plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut projections = Vec::with_capacity(data_fields.len() + partition_fields.len() + 1);
    for field in data_fields {
        let source = if reread_names.iter().any(|name| name == field.name()) {
            col(field.name())
        } else {
            Expr::Cast(Cast::new(
                Box::new(lit(ScalarValue::Null)),
                field.data_type().clone(),
            ))
        };
        projections.push(cast_to_field(source, field));
    }
    for field in partition_fields {
        let value = hit
            .partition_names
            .iter()
            .position(|name| name == field.name())
            .and_then(|index| hit.partition_values.get(index))
            .cloned()
            .unwrap_or(ScalarValue::Null);
        projections.push(cast_to_field(lit(value), field));
    }
    let funneled = LogicalPlanBuilder::from(reread_plan)
        .repartition(Partitioning::RoundRobinBatch(1))
        .map_err(engine_err)?
        .build()
        .map_err(engine_err)?;
    let (sequenced, row_index) = if with_row_index {
        let windowed = LogicalPlanBuilder::from(funneled)
            .window(vec![row_number_udwf().call(vec![])])
            .map_err(engine_err)?
            .build()
            .map_err(engine_err)?;
        let output_name = windowed
            .schema()
            .fields()
            .iter()
            .next_back()
            .map(|field| field.name().clone())
            .ok_or_else(|| {
                engine_err(datafusion::error::DataFusionError::Internal(
                    "row_number window produced no output field".to_string(),
                ))
            })?;
        let stepped = Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr {
            left: Box::new(Expr::Cast(Cast::new(
                Box::new(col(output_name)),
                DataType::Int64,
            ))),
            op: Operator::Minus,
            right: Box::new(lit(1_i64)),
        })
        .alias("__repark_row_index");
        let passthrough = reread_names
            .iter()
            .map(col)
            .chain(std::iter::once(stepped))
            .collect::<Vec<_>>();
        let sequenced = LogicalPlanBuilder::from(windowed)
            .project(passthrough)
            .map_err(engine_err)?
            .build()
            .map_err(engine_err)?;
        (sequenced, col("__repark_row_index"))
    } else {
        (funneled, lit(ScalarValue::Null))
    };
    let mut udf_args = vec![
        lit(hit.file_path.clone()),
        lit(hit.file_name.clone()),
        lit(hit.file_size),
        lit(0_i64),
        lit(hit.file_size),
        lit(ScalarValue::TimestampNanosecond(
            Some(hit.modification_nanos),
            Some("UTC".into()),
        )),
    ];
    if with_row_index {
        udf_args.push(row_index);
    }
    projections.push(file_metadata_call(udf_args, with_row_index).alias(METADATA_COLUMN_NAME));
    LogicalPlanBuilder::from(sequenced)
        .project(projections)
        .map_err(engine_err)?
        .build()
        .map_err(engine_err)
}
fn conform_branch(plan: LogicalPlan, target: &DFSchema) -> Result<LogicalPlan> {
    let schema = plan.schema();
    let projections = target
        .fields()
        .iter()
        .map(|field| {
            let source = if schema.has_column_with_unqualified_name(field.name()) {
                col(field.name())
            } else {
                Expr::Cast(Cast::new(
                    Box::new(lit(ScalarValue::Null)),
                    field.data_type().clone(),
                ))
            };
            cast_to_field(source, field)
        })
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(plan)
        .project(projections)
        .map_err(engine_err)?
        .build()
        .map_err(engine_err)
}
fn exact_output_schema(
    scan: &TableScan,
    hits: &[FileHit],
    with_row_index: bool,
) -> DataFusionResult<DFSchema> {
    let mut fields: Vec<(Option<TableReference>, Arc<Field>)> = scan
        .source
        .schema()
        .fields()
        .iter()
        .map(|field| (None, Arc::clone(field)))
        .collect();
    if let Some(first) = hits.first() {
        for (name, value) in first
            .partition_names
            .iter()
            .zip(first.partition_values.iter())
        {
            if fields.iter().any(|(_, field)| field.name() == name) {
                continue;
            }
            let data_type = value.data_type();
            fields.push((None, Arc::new(Field::new(name, data_type, true))));
        }
    }
    fields.push((None, metadata_outer_field(with_row_index)));
    DFSchema::new_with_metadata(fields, HashMap::new())
}
pub(crate) async fn augment_scan(
    state: &SessionState,
    plan: &LogicalPlan,
    scan: &TableScan,
    kind: &FileKind,
) -> std::result::Result<LogicalPlan, FileMetadataError> {
    let with_row_index = matches!(kind, FileKind::Parquet);
    let hits = match kind {
        FileKind::Text => text_file_hits(scan)?,
        _ => {
            let probe = DataFrame::new(state.clone(), plan.clone());
            let physical = probe.create_physical_plan().await.map_err(|error| {
                FileMetadataError::engine(format!("metadata file listing failed: {error}"))
            })?;
            collect_file_hits(&physical)
        }
    };
    let context = SessionContext::new_with_state(state.clone());
    let source_schema = scan.source.schema();
    let data_fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let partition_fields = hits
        .first()
        .map(|hit| {
            hit.partition_names
                .iter()
                .zip(hit.partition_values.iter())
                .filter(|(name, _)| !data_fields.iter().any(|field| field.name() == *name))
                .map(|(name, value)| Field::new(name, value.data_type(), true))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut branches = Vec::with_capacity(hits.len().max(1));
    if hits.is_empty() {
        let false_plan = LogicalPlanBuilder::from(plan.clone())
            .filter(lit(false))
            .map_err(|error| FileMetadataError::engine(error.to_string()))?
            .build()
            .map_err(|error| FileMetadataError::engine(error.to_string()))?;
        let mut projections = data_fields
            .iter()
            .map(|field| cast_to_field(col(field.name()), field))
            .collect::<Vec<_>>();
        for field in &partition_fields {
            projections.push(cast_to_field(lit(ScalarValue::Null), field));
        }
        let mut dummy_args = vec![
            lit(String::new()),
            lit(String::new()),
            lit(0_i64),
            lit(0_i64),
            lit(0_i64),
            lit(ScalarValue::TimestampNanosecond(
                Some(0),
                Some("UTC".into()),
            )),
        ];
        if with_row_index {
            dummy_args.push(lit(0_i64));
        }
        projections
            .push(file_metadata_call(dummy_args, with_row_index).alias(METADATA_COLUMN_NAME));
        branches.push(
            LogicalPlanBuilder::from(false_plan)
                .project(projections)
                .map_err(|error| FileMetadataError::engine(error.to_string()))?
                .build()
                .map_err(|error| FileMetadataError::engine(error.to_string()))?,
        );
    } else {
        for hit in &hits {
            let branch = build_file_branch(
                &context,
                kind,
                scan,
                hit,
                &data_fields,
                &partition_fields,
                with_row_index,
            )
            .await
            .map_err(|error| FileMetadataError::engine(error.to_string()))?;
            branches.push(branch);
        }
    }
    let target = branches
        .first()
        .map(|branch| branch.schema().as_ref().clone())
        .ok_or_else(|| FileMetadataError::unresolved(METADATA_COLUMN_NAME))?;
    let mut unioned: Option<LogicalPlan> = None;
    for branch in branches {
        let conformed = conform_branch(branch, &target)
            .map_err(|error| FileMetadataError::engine(error.to_string()))?;
        unioned = Some(match unioned {
            None => conformed,
            Some(left) => LogicalPlanBuilder::from(left)
                .union(conformed)
                .map_err(|error| FileMetadataError::engine(error.to_string()))?
                .build()
                .map_err(|error| FileMetadataError::engine(error.to_string()))?,
        });
    }
    let unioned = unioned.ok_or_else(|| FileMetadataError::unresolved(METADATA_COLUMN_NAME))?;
    let exact = exact_output_schema(scan, &hits, with_row_index)
        .map_err(|error| FileMetadataError::engine(error.to_string()))?;
    let union_schema = unioned.schema();
    let final_projections = exact
        .fields()
        .iter()
        .map(|field| {
            let source = if union_schema.has_column_with_unqualified_name(field.name()) {
                col(field.name())
            } else {
                Expr::Cast(Cast::new(
                    Box::new(lit(ScalarValue::Null)),
                    field.data_type().clone(),
                ))
            };
            cast_to_field(source, field)
        })
        .collect::<Vec<_>>();
    let finished =
        Projection::try_new_with_schema(final_projections, Arc::new(unioned), Arc::new(exact))
            .map_err(|error| FileMetadataError::engine(error.to_string()))?;
    let replacement = LogicalPlan::Projection(finished);
    plan.clone()
        .transform_up(|node| {
            if let LogicalPlan::TableScan(candidate) = &node
                && marker_kind(candidate).is_some()
            {
                return Ok(Transformed::yes(replacement.clone()));
            }
            Ok(Transformed::no(node))
        })
        .map(|transformed| transformed.data)
        .map_err(|_| FileMetadataError::unresolved(METADATA_COLUMN_NAME))
}
fn text_file_hits(scan: &TableScan) -> std::result::Result<Vec<FileHit>, FileMetadataError> {
    let provider = source_as_provider(&scan.source)
        .map_err(|error| FileMetadataError::engine(error.to_string()))?;
    let marker = provider
        .as_ref()
        .downcast_ref::<FileMetadataScan>()
        .ok_or_else(|| FileMetadataError::unresolved(METADATA_COLUMN_NAME))?;
    let inner = marker
        .inner()
        .as_ref()
        .downcast_ref::<TextTableProvider>()
        .ok_or_else(|| FileMetadataError::unresolved(METADATA_COLUMN_NAME))?;
    let mut hits = Vec::new();
    for (file, partition_values) in inner.metadata_files() {
        let metadata = std::fs::metadata(&file).map_err(|error| {
            FileMetadataError::engine(format!("text metadata stat failed: {error}"))
        })?;
        let file_size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
        let modification_nanos = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
            .unwrap_or(0);
        let display = file.display().to_string();
        let file_name = file
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or(display.clone());
        hits.push(FileHit {
            read_path: display.clone(),
            file_path: format!("file:{display}"),
            file_name,
            file_size,
            modification_nanos,
            partition_names: inner
                .metadata_partition_fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            partition_values,
        });
    }
    Ok(hits)
}
