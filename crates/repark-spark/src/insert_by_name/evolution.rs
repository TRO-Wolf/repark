use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::Catalog;
use iceberg::table::Table;
use repark_core::CatalogRegistry;

use crate::catalog_ops::{namespace_schema_name, reregister};
use crate::write_options::StatementWriteOptions;

pub(super) fn extra_source_columns(
    targets: &[String],
    sources: &[super::SourceName],
    case_sensitive: bool,
) -> Vec<String> {
    sources
        .iter()
        .filter(|name| {
            !targets
                .iter()
                .any(|target| super::same_name(target, &name.resolved, case_sensitive))
        })
        .map(|name| name.resolved.clone())
        .collect()
}

pub(super) fn columns_to_add(
    ctx: &SessionContext,
    table: &Table,
    targets: &[String],
    sources: &[super::SourceName],
    case_sensitive: bool,
    write_options: &StatementWriteOptions,
) -> Result<Vec<String>> {
    let extras = extra_source_columns(targets, sources, case_sensitive);
    if extras.is_empty() || !repark_iceberg::write::accepts_any_schema(table) {
        return Ok(Vec::new());
    }
    if !write_options.merge_schema(ctx) {
        return Err(repark_core::illegal_argument_error(format!(
            "Field {} not found in source schema",
            extras[0]
        )));
    }
    Ok(extras)
}

fn union_input_schema(arrow: &ArrowSchema, added: &[String]) -> Result<ArrowSchema> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(arrow.fields().len());
    for field in arrow.fields() {
        if field.data_type() == &DataType::Null {
            if added.iter().any(|name| name == field.name()) {
                return Err(DataFusionError::Plan(format!(
                    "cannot add column `{}` by schema merging: the source gives it no type \
                     (every value is NULL)",
                    field.name()
                )));
            }
            continue;
        }
        fields.push(Arc::clone(field));
    }
    Ok(ArrowSchema::new(fields))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_with_evolution(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    branch: Option<&str>,
    projection_sql: &str,
    added: &[String],
) -> Result<DataFrame> {
    let source_df = crate::spark_ast::execute_passthrough(ctx, catalogs, projection_sql).await?;
    let arrow = union_input_schema(source_df.schema().as_arrow(), added)?;
    let incoming = repark_iceberg::write::incoming_schema(&arrow)?;
    let evolved = repark_iceberg::write::evolve_schema(catalog, table, incoming).await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    let staged = if evolved
        .metadata()
        .default_partition_spec()
        .is_unpartitioned()
    {
        repark_iceberg::write::write_data_files_from_stream_with_concurrency(
            &evolved,
            stream,
            concurrency,
        )
        .await?
    } else {
        repark_iceberg::write::write_partitioned_data_files_from_stream_with_concurrency(
            &evolved,
            stream,
            concurrency,
        )
        .await?
    };
    repark_iceberg::write::commit_append_to(catalog, &evolved, staged, branch).await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, Arc::clone(catalog), catalog_name, &namespace).await?;
    ctx.read_empty()
}
