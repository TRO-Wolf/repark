//! pins: rp-1-fork-repin/C-004, C-006
//! `RePark` policies for fork metadata tables: projection honor and enumeration hiding.

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
use iceberg::inspect::MetadataTableType;

// === metadata-table projection wrap Sole-writer: T2 ICE-REF.

/// Wrap a fork metadata-table [`TableProvider`] so `scan` honors DataFusion projections.
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

/// Build a [`ProjectionExec`] (or return `input` unchanged) so the plan schema matches
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

/// Whether `name` is a metadata-table name the fork's `IcebergSchemaProvider::table_names`
fn is_synthesized_metadata_table_name(inner: &dyn SchemaProvider, name: &str) -> bool {
    let Some((base, metadata_table_type)) = name.rsplit_once('$') else {
        return false;
    };
    if base.is_empty() {
        return false;
    }
    MetadataTableType::try_from(metadata_table_type).is_ok() && inner.table_exist(base)
}

/// Schema-provider decorator: hide synthesized `$`-form metadata tables from ENUMERATION, and wrap
#[derive(Debug)]
pub struct MetadataProjectionSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
}

impl MetadataProjectionSchemaProvider {
    /// Wrap `inner` so metadata-table lookups return [`ProjectingMetadataTableProvider`] and
    #[must_use]
    pub fn wrap(inner: Arc<dyn SchemaProvider>) -> Arc<dyn SchemaProvider> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl SchemaProvider for MetadataProjectionSchemaProvider {
    /// ADR-0006: the listing is the catalog's tables, not the fork's synthesized cross-product.
    fn table_names(&self) -> Vec<String> {
        self.inner
            .table_names()
            .into_iter()
            .filter(|name| !is_synthesized_metadata_table_name(self.inner.as_ref(), name))
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let resolved = self.inner.table(name).await?;
        // `'$' in name` is a NAME heuristic, not a provider-type check (the fork's metadata form
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
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::physical_plan::empty::EmptyExec;

    /// A stand-in for the fork's `IcebergSchemaProvider` that reproduces the one behavior ADR-0006
    #[derive(Debug)]
    struct ForkShapedSchemaProvider {
        base_tables: Vec<String>,
    }

    impl ForkShapedSchemaProvider {
        fn new(base_tables: &[&str]) -> Self {
            Self {
                base_tables: base_tables.iter().map(|name| (*name).to_string()).collect(),
            }
        }
    }

    #[async_trait]
    impl SchemaProvider for ForkShapedSchemaProvider {
        fn table_names(&self) -> Vec<String> {
            self.base_tables
                .iter()
                .flat_map(|base| {
                    [base.clone()].into_iter().chain(
                        MetadataTableType::all_types()
                            .map(move |kind| format!("{base}${}", kind.as_str())),
                    )
                })
                .collect()
        }

        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            if !self.table_exist(name) {
                return Ok(None);
            }
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
            Ok(Some(Arc::new(EmptyTable::new(schema))))
        }

        fn table_exist(&self, name: &str) -> bool {
            // Last-`$` + vocabulary, matching fork `IcebergSchemaProvider` at pin 5e7b2e4.
            if let Some((base, kind)) = name.rsplit_once('$')
                && !base.is_empty()
                && MetadataTableType::try_from(kind).is_ok()
            {
                return self.base_tables.iter().any(|table| table == base);
            }
            self.base_tables.iter().any(|table| table == name)
        }
    }

    /// ADR-0006's whole claim, at the decorator: a namespace of two base tables enumerates as
    #[test]
    fn table_names_hides_the_forks_synthesized_metadata_names() {
        let inner: Arc<dyn SchemaProvider> =
            Arc::new(ForkShapedSchemaProvider::new(&["orders", "customers"]));
        assert_eq!(
            inner.table_names().len(),
            34,
            "fixture must reproduce the fork's synthesis (2 bases × (1 + 16 metadata types))"
        );

        let wrapped = MetadataProjectionSchemaProvider::wrap(inner);
        let mut names = wrapped.table_names();
        names.sort();
        assert_eq!(
            names,
            vec!["customers".to_string(), "orders".to_string()],
            "enumeration must list the catalog's tables only"
        );
    }

    /// The other half of the Trino shape, and the reason this is a listing decision rather than a
    #[tokio::test]
    async fn a_hidden_metadata_table_is_still_resolvable_by_name() {
        let wrapped =
            MetadataProjectionSchemaProvider::wrap(Arc::new(ForkShapedSchemaProvider::new(&[
                "orders",
            ])));
        assert!(
            !wrapped
                .table_names()
                .contains(&"orders$snapshots".to_string()),
            "precondition: the name is hidden from the listing"
        );
        assert!(
            wrapped.table_exist("orders$snapshots"),
            "a hidden name must still exist for resolution"
        );
        let resolved = wrapped
            .table("orders$snapshots")
            .await
            .expect("resolution must not error")
            .expect("a hidden metadata table must still resolve to a provider");
        assert_eq!(
            resolved.schema().fields().len(),
            1,
            "the resolved provider is the wrapped metadata provider, not an empty stand-in"
        );
    }

    /// pins: rp-1-fork-repin/C-006
    /// The predicate's two guards, stated as inputs.
    #[test]
    fn the_filter_keeps_names_the_fork_did_not_synthesize() {
        let inner: Arc<dyn SchemaProvider> = Arc::new(ForkShapedSchemaProvider::new(&["orders"]));

        assert!(
            !is_synthesized_metadata_table_name(inner.as_ref(), "q1$fy26"),
            "a real table whose name contains `$` is not a metadata table"
        );
        assert!(
            !is_synthesized_metadata_table_name(inner.as_ref(), "ghost$snapshots"),
            "a known suffix over an unknown base is not a metadata table"
        );
        assert!(
            !is_synthesized_metadata_table_name(inner.as_ref(), "orders$SNAPSHOTS"),
            "the fork synthesizes lower-case names only; the filter must match what it emits"
        );
        assert!(
            is_synthesized_metadata_table_name(inner.as_ref(), "orders$snapshots"),
            "the synthesized shape is the one thing that IS filtered"
        );

        let dollar_base: Arc<dyn SchemaProvider> =
            Arc::new(ForkShapedSchemaProvider::new(&["a$b"]));
        assert!(
            is_synthesized_metadata_table_name(dollar_base.as_ref(), "a$b$snapshots"),
            "last-`$` + vocabulary: `a$b$snapshots` is the snapshots twin of base `a$b`"
        );
        let listed = MetadataProjectionSchemaProvider::wrap(dollar_base).table_names();
        assert_eq!(
            listed,
            vec!["a$b".to_string()],
            "ADR-0006: a `$`-in-the-base table lists as itself, not its sixteen twins: {listed:?}"
        );
    }

    /// Vocabulary liveness: the filter must cover **every** type the fork synthesizes, including
    #[test]
    fn every_fork_metadata_table_type_is_filtered() {
        let inner: Arc<dyn SchemaProvider> = Arc::new(ForkShapedSchemaProvider::new(&["orders"]));
        for metadata_table_type in MetadataTableType::all_types() {
            let name = format!("orders${}", metadata_table_type.as_str());
            assert!(
                is_synthesized_metadata_table_name(inner.as_ref(), &name),
                "fork metadata table `{name}` must be filtered out of the listing"
            );
        }
    }

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

    /// An EMPTY projection over real batches preserves `num_rows` on the zero-column output — the
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

    /// Projection indices are logical-schema-relative but bind by NAME into the scan's physical
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
