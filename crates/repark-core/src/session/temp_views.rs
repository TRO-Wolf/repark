//! Temp views — the session-local registration family (`createOrReplaceTempView` and friends).
//!
//! Split out of `session.rs` (SQM round 6) when the R6-1 choke-point fix pushed that file past
//! its line ceiling: the temp-view surface is one coherent family (register / replace /
//! materialize / cache / declare-sorted / drop), and every member of it shares the ONE name
//! resolution in [`crate::temp_view`]. `ReparkSession`'s other entry points stay in `session.rs`.

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;
use repark_common::{Error, Result};

use super::ReparkSession;
use crate::engine_err;

impl ReparkSession {
    /// Register `batches` as a replaceable in-memory view named `name` (PySpark
    /// `createOrReplaceTempView`). The schema is taken from the first batch.
    ///
    /// `name` must be **single-part** and the view is **session-local** — see
    /// [`Self::temp_view_ref`] (R6-1).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if `batches` is empty (no schema to infer) or registration
    /// fails; [`Error::Analysis`] for a qualified `name`.
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

    /// Register a planned [`DataFrame`] as a replaceable temp view named `name` (PySpark
    /// `DataFrame.createOrReplaceTempView`). The view is lazy, like Spark's: each reference
    /// re-executes the plan (materialize with the batch overload above when that matters).
    ///
    /// `name` must be **single-part** and the view is **session-local** — see
    /// [`Self::temp_view_ref`] (R6-1).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if registration fails; [`Error::Analysis`] for a qualified
    /// `name`.
    pub fn create_or_replace_temp_view_from(&self, name: &str, frame: &DataFrame) -> Result<()> {
        self.replace_view(name, frame.clone().into_view())
    }

    /// ===========================================================================================
    /// Collect `frame` **once** and re-register as a [`MemTable`] temp view so subsequent scans
    /// are table scans, not re-execution of a VALUES (or other) body (R-PERF-VALUES).
    ///
    /// Empty results still register (schema from the plan, zero batches) so `.count()` / filters
    /// on an empty createDataFrame stay correct.
    ///
    /// **Contract (r23 CACHE1):** this entry point is the createDataFrame / VALUES path. Do **not**
    /// change its collect-once semantics for cache/persist — use
    /// [`Self::materialize_dataframe_as_cache_view`] for the cache path (caller-level branch).
    /// ===========================================================================================
    ///
    /// `name` must be **single-part** and the view is **session-local** — see
    /// [`Self::temp_view_ref`] (R6-1).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if collect or registration fails; [`Error::Analysis`] for a
    /// qualified `name`.
    pub async fn materialize_dataframe_as_temp_view(
        &self,
        name: &str,
        frame: DataFrame,
    ) -> Result<()> {
        self.register_collected_memtable(name, frame, None).await
    }

    // === r23 CACHE1: cache-honesty ===
    /// ===========================================================================================
    /// Cache-path materialize: collect once into a [`MemTable`] temp view with an optional
    /// `max_bytes` size guard (facade conf ``repark.cache.max_bytes``).
    ///
    /// **Caller-level branch (OTH-014 / CACHE1):** `cache()` / `persist()` use this entry point.
    /// createDataFrame / VALUES keep [`Self::materialize_dataframe_as_temp_view`] (byte-identical
    /// collect-once; no size threshold — data was already Python-resident).
    ///
    /// Single-node [`MemTable`] only — no disk spill despite Spark `MEMORY_AND_DISK*` names. When
    /// `max_bytes` is `Some(limit)` and the collected Arrow array memory exceeds `limit`, the
    /// batches are dropped and [`Error::Config`] names the conf key (loud memory contract).
    /// Size is measured **after** `collect` (refuses the pin; peak during collect is still
    /// O(result)). `None` or an absent conf = no size guard (`FairSpillPool` still bounds
    /// execution).
    /// ===========================================================================================
    ///
    /// `name` must be **single-part** and the view is **session-local** — see
    /// [`Self::temp_view_ref`] (R6-1).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if collect or registration fails; [`Error::Config`] when
    /// `max_bytes` is exceeded; [`Error::Analysis`] for a qualified `name`.
    pub async fn materialize_dataframe_as_cache_view(
        &self,
        name: &str,
        frame: DataFrame,
        max_bytes: Option<u64>,
    ) -> Result<()> {
        self.register_collected_memtable(name, frame, max_bytes)
            .await
    }

    /// ===========================================================================================
    /// Register pre-built Arrow [`RecordBatch`]es as a [`MemTable`] temp view (R-PERF-ARROW-CDF).
    ///
    /// Used by createDataFrame after Python builds a `pyarrow.Table` — skips VALUES SQL entirely.
    /// Empty `batches` still registers when `schema` is provided (zero-row frame).
    /// ===========================================================================================
    ///
    /// `name` must be **single-part** and the view is **session-local** — see
    /// [`Self::temp_view_ref`] (R6-1).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if `MemTable` construction or registration fails;
    /// [`Error::Analysis`] for a qualified `name` (pinned by
    /// `qualified_temp_view_name_refuses_and_persists_nothing` in `tests/temp_view_doors.rs`).
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

    /// Declare an in-memory temp view as pre-sorted by `keys` (engine field names), so the
    /// re-registered [`MemTable`] advertises the ordering and DataFusion elides redundant
    /// `SortExec`s (SE-1). The claim is ALWAYS verified (O(n) adjacent-pair pass, ASC NULLS
    /// LAST) before anything is replaced — a wrong claim refuses loudly and the original
    /// registration stays untouched. `tighten_nulls` is the c+ lever: after verify, a NULL
    /// in a key refuses; otherwise verified-null-free keys flip to non-nullable.
    ///
    /// # Errors
    /// [`Error::Analysis`] for a qualified `name`, an unknown view, a non-in-memory provider, an
    /// unknown key, empty keys, data that is not sorted as declared, or a NULL key under tighten;
    /// [`Error::DataFusion`] for engine-level scan/registration failures.
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
        // R6-1: resolve through the temp-view choke point so the declare path reads the SAME
        // registration a one-part `createOrReplaceTempView` wrote, whatever `SET` did since.
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

    /// Collect `frame` once, re-stamp tighten provenance when any plan source is
    /// tighten-derived (SE-1 R-A), then register a `MemTable`. Shared by createDataFrame
    /// materialize and cache/persist/checkpoint.
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

    /// The temp-view name choke point — see [`crate::temp_view`] (R6-1). A qualified name
    /// refuses; a one-part name is pinned to this session's build-time home; and the home itself
    /// is re-checked live, so a catalog registered over the build-time default catalog cannot
    /// turn this API into a catalog write (round-6 critic S1).
    ///
    /// # Errors
    /// [`Error::Analysis`] when `name` is not a single-part identifier, or when this session has
    /// no session-local temp-view home left.
    pub(super) fn temp_view_ref(&self, name: &str) -> Result<TableReference> {
        let reference = crate::temp_view::temp_view_ref(&self.temp_view_home, name)?;
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(reference)
    }

    /// The same home check for the paths that build their reference from an already-parsed
    /// segment (`table_exists`) or read the home directly (`list_temp_view_names`).
    ///
    /// # Errors
    /// [`Error::Analysis`] when this session has no session-local temp-view home left.
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

    /// The session's temp-view home as `[catalog, schema]` — the spelling a product read path
    /// prefixes a session-local view with so the engine cannot re-resolve it against the LIVE
    /// `datafusion.catalog.default_catalog` (R7-1). The home is checked live: a session whose
    /// home was taken over by a registered catalog has no temp-view home to name.
    ///
    /// # Errors
    /// [`Error::Analysis`] when this session has no session-local temp-view home left.
    pub fn temp_view_home(&self) -> Result<Vec<String>> {
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(vec![
            self.temp_view_home.catalog.clone(),
            self.temp_view_home.schema.clone(),
        ])
    }

    /// ===========================================================================================
    /// The home-qualified `[catalog, schema, table]` a one-part temp-view `name` resolves to —
    /// `None` when no view of that name lives in this session's temp-view home (R7-1).
    /// ===========================================================================================
    ///
    /// R6-1 pinned the temp-view WRITE to the build-time home; a product READ path that emits the
    /// caller's BARE name still resolves it against the LIVE
    /// `datafusion.catalog.default_catalog`, so after `SET datafusion.catalog.default_catalog =
    /// <other>` a minted view was invisible to `spark.table` / cache / re-scan while
    /// `table_exists` (which asks the home) said it was there (MEASURED on `3910ac7`; see
    /// `task/se1-declared-sorted-ledger.md` round 7). Product read paths ask THIS for the
    /// spelling to emit instead of assuming the bare name still resolves home.
    ///
    /// The probe mirrors [`Self::table_exists`]'s one-part arm exactly (quote-aware split, the
    /// already-parsed segment overload, `SessionContext::table_exist`), so a caller cannot get a
    /// `Some(..)` here that `table_exists` calls absent — the two answers come from one lookup
    /// shape. A qualified `name` is not a temp-view spelling and answers `None` (not an error):
    /// the callers are resolvers deciding *whether* the name is a temp view.
    ///
    /// This is deliberately NOT wired into raw SQL bodies — `session.sql("SELECT * FROM v")`
    /// keeps DataFusion's own live-default resolution, pinned by
    /// `set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read`.
    ///
    /// # Errors
    /// [`Error::Analysis`] when this session has no session-local temp-view home left (a catalog
    /// was registered over it — see [`crate::temp_view::assert_home_intact`]);
    /// [`Error::DataFusion`] when the engine lookup fails.
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

    /// The shared "create OR REPLACE" registration: `register_table` errors on a name clash, so
    /// drop any existing view first (a no-op returning `Ok(None)` when absent).
    ///
    /// R6-1: the name goes through [`Self::temp_view_ref`] — this is the single seam every
    /// registering caller shares (batch/plan `create_or_replace_temp_view*`,
    /// `register_record_batches_as_temp_view`, `register_collected_memtable` for
    /// materialize/cache/checkpoint, and `declare_temp_view_sorted`'s re-registration).
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

    /// Drop a temp view (PySpark `spark.catalog.dropTempView`). Returns whether a view of that
    /// name existed — dropping a missing view is not an error, matching PySpark.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the name cannot be resolved as a table reference, or
    /// [`Error::Analysis`] for a qualified name (R6-1 — symmetric with registration: a name that
    /// can never be created as a temp view cannot be dropped as one either).
    pub fn drop_temp_view(&self, name: &str) -> Result<bool> {
        let reference = self.temp_view_ref(name)?;
        Ok(self
            .context()
            .deregister_table(reference)
            .map_err(engine_err)?
            .is_some())
    }
}
