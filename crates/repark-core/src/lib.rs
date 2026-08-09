//! repark-core — the Session-centric engine API (v1 `repark-session`, re-homed).
//!
//! [`ReparkSession`] constructs the DataFusion `SessionContext` (memory pool, batch size,
//! partitions, write knobs as `ConfigExtension`s), holds the iceberg `Catalog` handles
//! ([`CatalogRegistry`]) with their [`LocationPolicy`], and exposes the near-drop-in PySpark
//! entrypoints: `sql`, catalog/namespace registration, the reader family
//! (`read_parquet`/`read_csv`/`read_json`/`read_iceberg_table`), the temp-view family, and the
//! listing helpers. All execution routes through the [`ExecutionBackend`] seam — today a local
//! execution-context holder over in-process DataFusion, whose minimal surface is a future
//! extension point rather than a distribution abstraction (`backend.rs` doc / `ARCHITECTURE.md`).
//!
//! The two phase-cut seams are the crate's only inversions of v1 (design §3): [`SqlDialect`]
//! (how a statement front end plugs into `sql` — [`DataFusionDialect`] is the phase-1 default)
//! and [`SessionExtension`] (what a door installs at `build()` time). Bindings import THIS
//! crate only; doors import repark-core + repark-iceberg. The frame handle is DataFusion's
//! [`DataFrame`], re-exported — no wrapper (omissions ledger O-6).

mod backend;
mod catalog_config;
mod catalog_state;
mod dialect;
mod error_map;
mod extension;
mod idents;
mod object_store_s3;
mod read_options;
mod runtime;
mod session;
mod time_travel;

// --- The Session surface (v1 names, courtesy `Session` alias). ---
pub use session::ReparkSession as Session;
pub use session::{DATAFUSION_CONFIG_PREFIX, ReparkSession, ReparkSessionBuilder, TimeTravelOpts};

// --- Seams. ---
pub use backend::{ExecutionBackend, SingleNodeBackend};
pub use dialect::{DataFusionDialect, EngineContext, SqlDialect};
pub use extension::SessionExtension;

// --- The embedding's executor handle (EC-5 / design §4 Q7). Additive: the TYPE is named here;
// core never constructs one and never blocks — the INSTANCE lives in the embedding.
pub use runtime::EngineRuntime;

// --- Catalog configuration + engine-side registry (hoisted). ---
pub use catalog_config::{CatalogKind, CatalogSpec, parse_catalog_specs};
pub use catalog_state::{CatalogRegistry, LocationPolicy};

// --- Time travel (hoisted): spec + parsers + the reader-options path. ---
pub use time_travel::{
    TimeTravelSpec, parse_timestamp_to_ms, parse_version_value, read_table_at, resolve_snapshot_id,
    snapshot_id_as_of_time,
};

// --- Error surface: the classifier fold + the seed re-export (bindings import one crate). ---
pub use error_map::engine_err;
pub use repark_common::{Error, ErrorClass, Result};

// --- Frame handle: DataFusion `DataFrame` re-exported — no wrapper (design §3 / O-6). ---
pub use datafusion::prelude::DataFrame;

// Crate-internal re-exports (v1 lib.rs scope, minus the deferred excel/postgres folds).
// v1's two `#[cfg(test)] pub(crate) use` companions live in `session.rs` — the module split
// made it the test cohort's parent module.
pub(crate) use error_map::{iceberg_err, resolve_s3_region_override};
pub(crate) use idents::parse_table_identifier_segments;
pub(crate) use read_options::{
    csv_read_options_from_map, csv_utf8_schema_from_path, json_read_options_from_map,
};
