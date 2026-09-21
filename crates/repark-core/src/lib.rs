//! repark-core — the Session-centric engine API.

mod backend;
mod catalog_config;
mod catalog_kind;
mod catalog_state;
pub mod column_resolution;
mod config_file;
mod dialect;
mod dynamic_flatten;
mod error_map;
mod extension;
mod freq_items;
mod idents;
mod isnan;
mod lineage_columns;
mod metadata_columns;
mod na_fill;
mod named_sources;
mod namespace_create;
mod nlj_build_reset;
mod object_store_s3;
mod orc_footer;
mod orc_scan;
mod orc_schema;
mod partition_discovery;
mod partition_overwrite_mode;
mod partition_timestamp;
mod pool_refusals;
mod pre_execute;
mod range_table;
mod read_options;
mod runtime;
mod session;
mod session_owner;
mod session_time_zone;
pub mod silver;
mod sorted_view;
mod spark_nullable;
mod stack;
mod temp_view;
mod text_glob;
mod text_io;
mod text_partition;
mod text_partition_fallback;
mod text_scan;
mod text_schema;
pub mod time_travel;
mod transpose;
mod unknown_routine;
mod update_fields;

// --- The Session surface (v1 names, courtesy `Session` alias).
pub use session::ReparkSession as Session;
pub use session::{
    DATAFUSION_CONFIG_PREFIX, ReparkSession, ReparkSessionBuilder, resolve_bound_expr,
    resolve_scoped_expr, resolve_subquery_plan,
};

// === Session timezone ===
pub use session_time_zone::{
    DEFAULT_SESSION_TIME_ZONE, SESSION_TIME_ZONE_KEY, SessionTimeZone, canonical_session_zone_id,
    parse_runtime_session_zone_value, resolve_session_time_zone,
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
pub use catalog_config::{CatalogKind, CatalogSpec, parse_catalog_specs, prop_key_is_secret};
pub use catalog_state::{CatalogRegistry, LocationPolicy, memory_warehouse_fallback_root};
pub use config_file::config_file_pairs;
pub use config_file::maintenance::{
    MaintenancePolicy, TablePolicy, parse_duration, parse_maintenance_policy,
};
pub use na_fill::{FillBuild, na_fill_expr};
pub use named_sources::{NamedSource, SourceRow};
pub use namespace_create::refuse_contradictory_namespace_location;
pub use partition_overwrite_mode::{
    OverwriteIntent, PARTITION_OVERWRITE_MODE_KEY, PartitionOverwriteMode,
    PartitionOverwriteModeConfig, parse_partition_overwrite_mode,
    partition_overwrite_mode_from_config_map, partition_overwrite_mode_from_ctx,
    partition_overwrite_mode_from_options, with_partition_overwrite_mode,
};
pub use session_owner::{DescribeOwnerConfig, session_owner_snapshot, with_session_owner};

// === Time travel ===
pub use time_travel::{
    ReaderTimeTravel, branch_time_travel_refusal, invalid_version_pin, parse_version_value,
    resolve_reader_spec, resolve_snapshot_id,
};

pub use lineage_columns::{LineagePins, prepare_lineage_sql, sql_mentions_lineage_columns};
pub use metadata_columns::{
    MetadataColumnPins, prepare_metadata_column_sql, sql_mentions_metadata_columns,
};

// --- Error surface: the classifier fold + the seed re-export (bindings import one crate).
pub use error_map::{
    IllegalArgumentMarker, engine_err, engine_err_for_sql, illegal_argument_error,
};
pub use pool_refusals::{
    PoolRefusalLog, REFUSAL_CONTAINMENT_NOTE, RefusalRecordingPool, pool_refusal_log,
};
pub use repark_common::{Error, ErrorClass, Result};
pub use unknown_routine::map_unknown_routine_message;

// === SE-1 tightenNulls ===
pub use sorted_view::{
    TIGHTEN_NULLS_METADATA_KEY, TIGHTEN_NULLS_METADATA_VALUE,
    refuse_iceberg_create_of_tightened_ddl, refuse_iceberg_create_of_tightened_plan,
    refuse_iceberg_create_of_tightened_schema, schema_is_tighten_derived,
    strip_tighten_export_metadata, tightened_field_names,
};
pub use spark_nullable::relax_schema_to_nullable;

// --- Frame handle: DataFusion `DataFrame` re-exported — no wrapper (design §3 / O-6).
pub use datafusion::prelude::DataFrame;

// --- Plan-rewrite kernels (no DataFrame newtype).
pub use dynamic_flatten::{DynamicFlattenOptions, dynamic_flatten};
pub use freq_items::{FREQ_ITEMS_OUTPUT_PREFIX, freq_items};
pub use isnan::{register_repark_isnan, repark_isnan_call, repark_isnan_udf};
pub use stack::{
    StackLabels, StackQueryPlanner, StackRewrite, apply_labeled_stack, apply_stack, register_stack,
    stack_udf,
};
pub use transpose::{
    SparkTransposeError, TRANSPOSE_OUTPUT_PREFIX, TransposeError, TransposeOutcome, transpose_frame,
};
pub use update_fields::{register_update_fields, update_fields_call, update_fields_udf};
mod plan_canonical;
mod plan_introspect;
pub use plan_introspect::{input_files, same_semantics, semantic_hash};

#[must_use]
pub fn built_with_debug_assertions() -> bool {
    cfg!(debug_assertions)
}

// v1's two `#[cfg(test)] pub(crate) use` companions live in `session.rs` — the module split
pub(crate) use error_map::{iceberg_err, resolve_s3_region_override};
pub(crate) use idents::parse_table_identifier_segments;
pub use orc_scan::OrcReadOptions;
pub(crate) use read_options::json_read_options_from_map;
pub use text_io::write_text_frame;
pub use text_partition::write_text_partitioned;
