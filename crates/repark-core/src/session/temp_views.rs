//! Temp views — the session-local registration family (`createOrReplaceTempView` and friends).

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;
use repark_common::{Error, Result};

use super::ReparkSession;
use crate::engine_err;

impl ReparkSession {
    /// Register `batches` as a replaceable in-memory view named `name`.
    /// # Errors
    /// Returns [`Error::DataFusion`] if `batches` is empty (no schema to infer) or registration
    pub fn create_or_replace_temp_view(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let schema = batches
            .first()
            .ok_or_else(|| {
                Error::DataFusion(format!(
                    "cannot register temp view '{name}': no batches to infer a schema from"
                ))
            })?
            .schema();
        let table = MemTable::try_new(schema, vec![batches]).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Register a planned [`DataFrame`] as a replaceable temp view named `name`.
    /// # Errors
    /// Returns [`Error::DataFusion`] if registration fails; [`Error::Analysis`] for a qualified
    pub fn create_or_replace_temp_view_from(&self, name: &str, frame: &DataFrame) -> Result<()> {
        self.replace_view(name, frame.clone().into_view())
    }

    /// Collect `frame` once and register a [`MemTable`] temp view.
    /// # Errors
    /// # Errors Returns [`Error::DataFusion`] if collect or registration fails.
    pub async fn materialize_dataframe_as_temp_view(
        &self,
        name: &str,
        frame: DataFrame,
    ) -> Result<()> {
        self.register_collected_memtable(name, frame, None).await
    }

    // === cache-honesty ===
    /// Collect once into a [`MemTable`] with an optional post-collect `max_bytes` guard.
    /// # Errors
    /// # Errors Returns [`Error::DataFusion`] if collect or registration fails.
    pub async fn materialize_dataframe_as_cache_view(
        &self,
        name: &str,
        frame: DataFrame,
        max_bytes: Option<u64>,
    ) -> Result<()> {
        self.register_collected_memtable(name, frame, max_bytes)
            .await
    }

    /// Register pre-built Arrow [`RecordBatch`]es as a [`MemTable`] temp view.
    /// # Errors
    /// # Errors Returns [`Error::DataFusion`] if `MemTable` construction or registration fails.
    pub fn register_record_batches_as_temp_view(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Declare a temp view sorted by `keys` after verifying ASC NULLS LAST ordering.
    /// # Errors
    /// Returns `Error::Analysis` for a qualified name, unknown view, or non-in-memory provider.
    pub async fn declare_temp_view_sorted(
        &self,
        name: &str,
        keys: &[String],
        tighten_nulls: bool,
    ) -> Result<()> {
        if keys.is_empty() {
            return Err(Error::Analysis(
                "declared-sorted view: at least one key column is required".to_string(),
            ));
        }
        // R6-1: resolve through the temp-view choke point so declare reads the same registration.
        let reference = self.temp_view_ref(name)?;
        let provider = self
            .context()
            .table_provider(reference.clone())
            .await
            .map_err(|_| {
                Error::Analysis(format!("declared-sorted view: no temp view named '{name}'"))
            })?;
        let provider_any: &dyn std::any::Any = provider.as_ref();
        if provider_any.downcast_ref::<MemTable>().is_none() {
            return Err(Error::Analysis(format!(
                "declared-sorted view: '{name}' is not an in-memory frame — sortedness \
                 declarations support createDataFrame/cache views only"
            )));
        }
        let schema = provider.schema();
        let batches = self
            .context()
            .table(reference)
            .await
            .map_err(engine_err)?
            .collect()
            .await
            .map_err(engine_err)?;
        crate::sorted_view::verify_batches_sorted(&schema, &batches, keys)?;
        let (schema, batches) =
            crate::sorted_view::apply_declare_nullability(schema, batches, keys, tighten_nulls)?;
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions)
            .map_err(engine_err)?
            .with_sort_order(crate::sorted_view::declared_sort_order(keys));
        self.replace_view(name, Arc::new(table))
    }

    /// Collect `frame` once, re-stamp tighten provenance, then register a `MemTable`.
    async fn register_collected_memtable(
        &self,
        name: &str,
        frame: DataFrame,
        max_bytes: Option<u64>,
    ) -> Result<()> {
        let plan = frame.logical_plan().clone();
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame.collect().await.map_err(engine_err)?;
        if let Some(limit) = max_bytes {
            let total: u64 = batches
                .iter()
                .map(|batch| u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX))
                .fold(0_u64, u64::saturating_add);
            if total > limit {
                return Err(Error::Config(format!(
                    "cache materialize size {total} bytes exceeds repark.cache.max_bytes={limit}; \
                     raise the conf or avoid cache()/persist() on this plan (single-node MemTable \
                     pin; no disk spill)"
                )));
            }
        }
        let (schema, batches) =
            crate::sorted_view::apply_tighten_provenance_on_materialize(&plan, schema, batches)?;
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Resolve names through the build-time home and re-check its provider identity before use.
    pub(super) fn temp_view_ref(&self, name: &str) -> Result<TableReference> {
        let reference = crate::temp_view::temp_view_ref(&self.temp_view_home, name)?;
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(reference)
    }

    /// Build a home reference from an already-parsed segment after checking provider identity.
    pub(super) fn temp_view_ref_from_segment(
        &self,
        segment: &str,
        quoted: bool,
    ) -> Result<TableReference> {
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(crate::temp_view::temp_view_ref_from_segment(
            &self.temp_view_home,
            segment,
            quoted,
        ))
    }

    /// The session's temp-view home as `[catalog, schema]`.
    /// # Errors
    /// [`Error::Analysis`] when this session has no session-local temp-view home left.
    pub fn temp_view_home(&self) -> Result<Vec<String>> {
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(vec![
            self.temp_view_home.catalog.clone(),
            self.temp_view_home.schema.clone(),
        ])
    }

    /// Resolve a one-part name to the home-qualified `[catalog, schema, table]` reference.
    /// # Errors
    /// [`Error::Analysis`] when this session has no session-local temp-view home left (a catalog
    pub fn resolve_temp_view_home_ref(&self, name: &str) -> Result<Option<Vec<String>>> {
        let Ok(parts) = crate::parse_table_identifier_segments(name) else {
            return Ok(None);
        };
        let [view] = parts.as_slice() else {
            return Ok(None);
        };
        let quoted = name.trim().starts_with(['"', '`']);
        let reference = self.temp_view_ref_from_segment(view, quoted)?;
        if self
            .context()
            .table_exist(reference.clone())
            .map_err(engine_err)?
        {
            Ok(Some(vec![
                reference.catalog().unwrap_or_default().to_string(),
                reference.schema().unwrap_or_default().to_string(),
                reference.table().to_string(),
            ]))
        } else {
            Ok(None)
        }
    }

    /// Register or replace a view through the single temp-view name seam.
    fn replace_view(
        &self,
        name: &str,
        provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        let reference = self.temp_view_ref(name)?;
        self.context()
            .deregister_table(reference.clone())
            .map_err(engine_err)?;
        self.context()
            .register_table(reference, provider)
            .map_err(engine_err)?;
        Ok(())
    }

    /// Drop a temp view (PySpark `spark.catalog.dropTempView`).
    /// # Errors
    /// Returns `Error::DataFusion` if the name cannot be resolved as a table reference.
    pub fn drop_temp_view(&self, name: &str) -> Result<bool> {
        let reference = self.temp_view_ref(name)?;
        Ok(self
            .context()
            .deregister_table(reference)
            .map_err(engine_err)?
            .is_some())
    }
}
