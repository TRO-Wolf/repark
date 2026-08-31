//! repark-core — the Session-centric engine API.

mod backend;
mod catalog_config;
mod catalog_state;
mod dialect;
mod dynamic_flatten;
mod error_map;
mod extension;
mod idents;
mod lineage_columns;
mod namespace_create;
mod object_store_s3;
mod pre_execute;
mod read_options;
mod runtime;
mod session;
mod session_time_zone;
mod sorted_view;
mod temp_view;
mod time_travel;

// --- The Session surface (v1 names, courtesy `Session` alias).
pub use session::ReparkSession as Session;
pub use session::{DATAFUSION_CONFIG_PREFIX, ReparkSession, ReparkSessionBuilder, TimeTravelOpts};

// === Session timezone ===
pub use session_time_zone::{
    DEFAULT_SESSION_TIME_ZONE, SESSION_TIME_ZONE_KEY, SessionTimeZone, resolve_session_time_zone,
};

// --- Seams.
pub use backend::{ExecutionBackend, SingleNodeBackend};
pub use dialect::{DataFusionDialect, EngineContext, SqlDialect};

// === Pre-execute belt ===
pub use extension::{SessionBuildConf, SessionExtension};
pub use pre_execute::PreExecute;

// --- The embedding's executor handle (EC-5 / design §4 Q7).
pub use runtime::EngineRuntime;

// --- Catalog configuration + engine-side registry (hoisted).
pub use catalog_config::{CatalogKind, CatalogSpec, parse_catalog_specs};
pub use catalog_state::{CatalogRegistry, LocationPolicy, memory_warehouse_fallback_root};
pub use namespace_create::refuse_contradictory_namespace_location;

// === Time travel ===
pub use time_travel::{
    TimeTravelSpec, next_temp_view_name, parse_timestamp_to_ms, parse_version_value, read_table_at,
    resolve_snapshot_id, snapshot_id_as_of_time,
};

pub use lineage_columns::{LineagePins, prepare_lineage_sql, sql_mentions_lineage_columns};

// --- Error surface: the classifier fold + the seed re-export (bindings import one crate).
pub use error_map::engine_err;
pub use repark_common::{Error, ErrorClass, Result};

// === SE-1 tightenNulls ===
pub use sorted_view::{
    TIGHTEN_NULLS_METADATA_KEY, TIGHTEN_NULLS_METADATA_VALUE,
    refuse_iceberg_create_of_tightened_ddl, refuse_iceberg_create_of_tightened_plan,
    refuse_iceberg_create_of_tightened_schema, schema_is_tighten_derived,
    strip_tighten_export_metadata, tightened_field_names,
};

// --- Frame handle: DataFusion `DataFrame` re-exported — no wrapper (design §3 / O-6).
pub use datafusion::prelude::DataFrame;

// --- Plan-rewrite kernels (no DataFrame newtype).
pub use dynamic_flatten::{DynamicFlattenOptions, dynamic_flatten};

// v1's two `#[cfg(test)] pub(crate) use` companions live in `session.rs` — the module split
pub(crate) use error_map::{iceberg_err, resolve_s3_region_override};
pub(crate) use idents::parse_table_identifier_segments;
pub(crate) use read_options::{
    csv_read_options_from_map, csv_utf8_schema_from_path, json_read_options_from_map,
};
