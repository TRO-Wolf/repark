//! repark-core — the Session-centric engine API (v1 `repark-session`, re-homed).
//!
//! [`ReparkSession`] constructs the DataFusion `SessionContext` (memory pool, batch size,
//! partitions, write knobs as `ConfigExtension`s), holds the iceberg `Catalog` handles
//! ([`CatalogRegistry`]) with their [`LocationPolicy`], and exposes the near-drop-in PySpark
//! entrypoints: `sql`, catalog/namespace registration, the reader family
//! (`read_parquet`/`read_csv`/`read_json`/`read_iceberg_table`), the temp-view family, and the
//! listing helpers. All execution routes through the [`ExecutionBackend`] seam.
//!
//! The two phase-cut seams are the crate's only inversions of v1 (design §3): [`SqlDialect`]
//! (how a statement front end plugs into `sql` — [`DataFusionDialect`] is the phase-1 default)
//! and [`SessionExtension`] (what a door installs at `build()` time). Bindings import THIS
//! crate only; doors import repark-core + repark-iceberg. The frame handle is DataFusion's
//! [`DataFrame`], re-exported — no wrapper (omissions ledger O-6).
//!
//! PORT IN PROGRESS (phase-1 PR-C): the session module tree lands staged-then-wired so every
//! commit compiles — `session.rs` and its support modules are wired here only once the four
//! forced edits (design §5) land; until then the staged files are not part of the crate.

mod backend;
mod catalog_config;
mod catalog_state;
mod time_travel;

// --- Seams. ---
pub use backend::{ExecutionBackend, SingleNodeBackend};

// --- Catalog configuration + engine-side registry (hoisted). ---
pub use catalog_config::{CatalogKind, CatalogSpec, parse_catalog_specs};
pub use catalog_state::{CatalogRegistry, LocationPolicy};

// --- Time travel (hoisted): spec + parsers + the reader-options path. ---
pub use time_travel::{
    TimeTravelSpec, parse_timestamp_to_ms, parse_version_value, read_table_at, resolve_snapshot_id,
    snapshot_id_as_of_time,
};

// --- Error surface: the seed re-export (bindings import one crate). ---
pub use repark_common::{Error, ErrorClass, Result};

// --- Frame handle: DataFusion `DataFrame` re-exported — no wrapper (design §3 / O-6). ---
pub use datafusion::prelude::DataFrame;
