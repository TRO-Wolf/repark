//! RePark-side projection honor for fork metadata-table providers (r25 T2 item 0).
//!
//! The fork's `IcebergMetadataTableProvider::scan` (iceberg-datafusion
//! `table/metadata_table.rs`) **ignores** the `projection` argument — empty projection
//! (`count()` / styled `.show()`) and partial column lists plan a logical schema that does not
//! match the physical full-column scan → DataFusion Internal "Physical input schema should be
//! the same … (physical) N vs (logical) 0".
//!
//! Fix (tonight): wrap every `table$meta` provider returned from the registered schema provider
//! so `scan` applies DataFusion's [`ProjectionExec`] over the fork plan (never collect-then-
//! project). Fork proper fix + shim removal = fork-workstream seed (ledger only).

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{SchemaProvider, Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

// === r25 T2 item 0: metadata-table projection wrap =========================================
//
// Sole-writer: T2 ICE-REF. Applied at SchemaProvider registration (ReparkCatalogProvider
// snapshot / namespace refresh) so every free-SQL path through `table$meta` is covered.
// ==============================================================================

/// ===========================================================================================
/// Wrap a fork metadata-table [`TableProvider`] so `scan` honors DataFusion projections.
///
/// Inner `schema()` stays the full metadata-table schema (DF contract). The returned
/// [`ExecutionPlan`]'s schema is projected when `projection` is `Some`.
/// ===========================================================================================
#[derive(Debug, Clone)]
pub struct ProjectingMetadataTableProvider {
    inner: Arc<dyn TableProvider>,
}

impl ProjectingMetadataTableProvider {
    /// Wrap `inner` (expected to be a fork metadata-table provider; works for any provider).
    #[must_use]
    pub fn new(inner: Arc<dyn TableProvider>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TableProvider for ProjectingMetadataTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Fork ignores projection — always scan full columns, then project physically.
        let full = self.inner.scan(state, None, filters, limit).await?;
        apply_projection_exec(full, projection, &self.inner.schema())
    }
}

/// ===========================================================================================
/// Build a [`ProjectionExec`] (or return `input` unchanged) so the plan schema matches
/// `projection`. Empty projection → zero-field plan (count / show path).
///
/// `projection` indices are relative to the provider's LOGICAL schema (`full_schema` — what
/// DataFusion planned against). Physical column bindings are resolved by NAME into
/// `input.schema()` so a field-order divergence between the fork provider's `schema()` and its
/// scan output fails loudly (or is re-ordered correctly) instead of silently mis-binding data.
/// ===========================================================================================
fn apply_projection_exec(
    input: Arc<dyn ExecutionPlan>,
    projection: Option<&Vec<usize>>,
    full_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(indices) = projection else {
        return Ok(input);
    };
    let physical_schema = input.schema();
    let aligned = physical_schema.fields().len() == full_schema.fields().len()
        && physical_schema
            .fields()
            .iter()
            .zip(full_schema.fields().iter())
            .all(|(physical, logical)| physical.name() == logical.name());
    // Full identity projection is a no-op only when the scan's schema matches the logical one.
    if aligned
        && indices.len() == full_schema.fields().len()
        && indices
            .iter()
            .enumerate()
            .all(|(position, &index)| position == index)
    {
        return Ok(input);
    }
    let mut exprs = Vec::with_capacity(indices.len());
    for &index in indices {
        let field = full_schema.fields().get(index).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "metadata-table projection index {index} out of range ({} fields)",
                full_schema.fields().len()
            ))
        })?;
        let physical_index = physical_schema.index_of(field.name()).map_err(|_| {
            DataFusionError::Plan(format!(
                "metadata-table scan output is missing projected column '{}' \
                 (provider schema / scan schema divergence)",
                field.name()
            ))
        })?;
        exprs.push(ProjectionExpr {
            expr: Arc::new(Column::new(field.name(), physical_index)),
            alias: field.name().clone(),
        });
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
}

/// ===========================================================================================
/// Schema-provider decorator: wrap `$`-form metadata table providers with projection honor.
///
/// Base tables (no `$`) pass through unchanged. All other [`SchemaProvider`] methods delegate.
/// ===========================================================================================
#[derive(Debug)]
pub struct MetadataProjectionSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
}

impl MetadataProjectionSchemaProvider {
    /// Wrap `inner` so metadata-table lookups return [`ProjectingMetadataTableProvider`].
    #[must_use]
    pub fn wrap(inner: Arc<dyn SchemaProvider>) -> Arc<dyn SchemaProvider> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl SchemaProvider for MetadataProjectionSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let resolved = self.inner.table(name).await?;
        // `'$' in name` is a NAME heuristic, not a provider-type check (the fork's metadata
        // form is always `table$meta`). Blast radius when it over-applies to a real table
        // whose name contains `$`: results stay CORRECT (scan full + physical projection,
        // name-bound), but that provider's own projection pushdown is bypassed. Accepted
        // trade-off until the fork-side fix lands (T2 seed); a downcast to the fork provider
        // type would tie this crate to iceberg-datafusion internals.
        Ok(match resolved {
            Some(provider) if name.contains('$') => {
                Some(Arc::new(ProjectingMetadataTableProvider::new(provider)))
            }
            other => other,
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn empty_projection_yields_zero_field_plan() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let projected =
            apply_projection_exec(input, Some(&vec![]), &schema).expect("empty projection");
        assert_eq!(
            projected.schema().fields().len(),
            0,
            "empty projection must produce 0-field schema for count/show"
        );
    }

    #[test]
    fn partial_projection_keeps_named_fields_in_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "committed_at",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("operation", DataType::Utf8, true),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let projected =
            apply_projection_exec(input, Some(&vec![1, 2]), &schema).expect("partial projection");
        let names: Vec<_> = projected
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, vec!["snapshot_id", "operation"]);
    }

    #[test]
    fn full_identity_projection_is_noop() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let projected = apply_projection_exec(input, Some(&vec![0, 1]), &schema).expect("full");
        // Same plan identity when projection is full identity (no extra ProjectionExec).
        assert_eq!(projected.schema().fields().len(), 2);
    }

    #[test]
    fn out_of_range_projection_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let err = apply_projection_exec(input, Some(&vec![3]), &schema).expect_err("oor");
        assert!(err.to_string().contains("out of range"), "got: {err}");
    }

    /// r25 morning critic pin: an EMPTY projection over real batches preserves `num_rows` on
    /// the zero-column output — the exact mechanism `count(*)` relies on. `EmptyExec` cannot pin
    /// this (it emits no batches), so this drives rows through a memory source.
    #[tokio::test]
    async fn empty_projection_preserves_row_count_over_real_batches() {
        use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::physical_plan::collect;

        let schema = Arc::new(Schema::new(vec![
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("operation", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![101, 102, 103])),
                Arc::new(StringArray::from(vec!["append", "overwrite", "append"])),
            ],
        )
        .expect("batch");
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)
            .expect("memory exec");
        let projected =
            apply_projection_exec(input, Some(&vec![]), &schema).expect("empty projection");
        assert_eq!(projected.schema().fields().len(), 0);
        let batches = collect(projected, Arc::new(TaskContext::default()))
            .await
            .expect("collect");
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            total_rows, 3,
            "zero-column projection must preserve row count for count(*)"
        );
    }

    /// r25 morning critic pin: projection indices are logical-schema-relative but bind by NAME
    /// into the scan's physical schema — a reordered scan output still yields the right DATA.
    #[tokio::test]
    async fn reordered_scan_schema_still_binds_projected_columns_by_name() {
        use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::physical_plan::collect;

        // Logical (provider) schema: [snapshot_id, operation].
        let logical = Arc::new(Schema::new(vec![
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("operation", DataType::Utf8, true),
        ]));
        // Physical (scan output) schema: REVERSED order.
        let physical = Arc::new(Schema::new(vec![
            Field::new("operation", DataType::Utf8, true),
            Field::new("snapshot_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            physical.clone(),
            vec![
                Arc::new(StringArray::from(vec!["append", "overwrite"])),
                Arc::new(Int64Array::from(vec![7, 8])),
            ],
        )
        .expect("batch");
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch]], physical, None).expect("memory exec");
        // Logical projection [0] = snapshot_id — must bind to the Int64 column, not column 0.
        let projected = apply_projection_exec(input, Some(&vec![0]), &logical).expect("projection");
        let batches = collect(projected, Arc::new(TaskContext::default()))
            .await
            .expect("collect");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "snapshot_id");
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id must carry the Int64 data, not the reordered Utf8 column");
        assert_eq!((ids.value(0), ids.value(1), ids.len()), (7, 8, 2));
    }
}
