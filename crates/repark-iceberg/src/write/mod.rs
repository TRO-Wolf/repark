//! Spark-semantics write adapter over the owned iceberg-rust fork.

pub mod alter;
pub mod append;
mod append_fanout_serial;
pub mod column_move;
mod commit_error;
pub mod commit_target;
pub use commit_target::commit_append_to;
pub mod concurrency;
pub(crate) mod conflict_filter;
pub(crate) mod conform;
pub mod data_format;
pub(crate) mod distribution;
pub(crate) mod file_order;
pub mod file_scoped_rewrite;
pub mod format_version;
#[cfg(test)]
mod hadoop_stale_commit;
/// Shared Spark/DF `quote_ident` + path-escape needles (CQ-006/007).
pub mod idents;
pub mod illegal_argument;
pub mod insert_defaults;
/// WI-2: the plain-INSERT store-assignment gate, as an `AnalyzerRule` over `LogicalPlan::Dml`.
pub mod insert_gate;
pub mod merge;
pub mod meta_delete;
mod name_resolution;
pub mod nested_column;
pub mod nested_type_sql;
pub mod output_spec;
/// OV1 exclusive full-table overwrite commit (stage-then-swap).
pub mod overwrite;
pub mod overwrite_commit;
pub mod overwrite_filter;
pub mod overwrite_scope;
/// Partition-scoped INSERT OVERWRITE (static row-filter + dynamic replace-partitions).
pub mod partition_overwrite;
pub mod partition_spec;
pub mod partition_write;
pub(crate) mod position_delete;
/// Identity DELETE/UPDATE (G3-E8 A1): SELECT over pinned `(_file, _pos)`, MERGE write arms.
pub mod predicate_dml;
pub mod replace_schema;
pub mod scan_concurrency;
pub mod scan_prune;
pub mod schema_evolution;
pub mod session_write_conf;
pub mod set_location;
/// Product snapshot-ref helpers (CREATE/DROP BRANCH|TAG) + test-support seam.
pub mod snapshot_refs;
pub mod sort_order;
mod static_value;
/// The ANSI store-assignment matrix — ONE home for MERGE and the non-MERGE insert/append lowerings.
pub(crate) mod store_assign;
pub mod summary_collision;
/// Test-support-only snapshot-ref helpers (`_testing_create_ref`).
pub mod testing_support;
/// Whole-table `TRUNCATE TABLE` (delete-only empty overwrite).
pub mod truncate;
pub mod unsupported;
#[cfg(test)]
mod unsupported_tests;
pub mod update_cast;
pub mod write_options;
pub mod writer_partitioning;
pub mod writer_plan;
pub mod writer_props;

pub use commit_error::{CommitStateUnknownError, commit_err, is_commit_state_unknown};
pub use illegal_argument::{
    IllegalArgumentMarker, NumberFormatMarker, illegal_argument_error, number_format_error,
};
pub use schema_evolution::{
    ACCEPT_ANY_SCHEMA_PROP, accepts_any_schema, evolve_merge_schema, evolve_schema, incoming_schema,
};
pub use snapshot_refs::{
    SnapshotRefKind, SnapshotRefRetention, create_branch_on_empty_table,
    create_or_replace_snapshot_ref, create_snapshot_ref, create_snapshot_ref_with_retention,
    drop_snapshot_ref, list_snapshot_refs, refuse_ref_write_on_format_v1, replace_snapshot_ref,
};
pub use testing_support::testing_create_ref;
pub use unsupported::{UnsupportedMarker, unsupported_error, unsupported_message_error};

pub use file_scoped_rewrite::{FILE_SCOPED_REWRITE_KEY, file_scoped_rewrite_from_config_map};
pub use scan_concurrency::{
    SCAN_CONCURRENCY_LIMIT_KEY, ScanConcurrency, scan_concurrency_from_config_map,
    scan_concurrency_from_ctx, with_scan_concurrency,
};
pub use scan_prune::{
    SCAN_PRUNING_KEY, file_scoped_rewrite_from_ctx, scan_pruning_from_config_map,
    scan_pruning_from_ctx, with_file_scoped_rewrite, with_merge_session_knobs, with_scan_pruning,
};

pub use append::{
    append, commit_append, write_partitioned_data_files, write_partitioned_data_files_from_stream,
    write_partitioned_data_files_from_stream_with_concurrency,
    write_partitioned_data_files_with_concurrency,
};
pub use concurrency::{
    DEFAULT_MAX_CONCURRENT_FILES, MAX_CONCURRENT_FILES_KEY, WriteConcurrency,
    concurrency_from_config_map, concurrency_from_ctx, with_write_concurrency,
};
pub use insert_gate::InsertStoreAssignment;
pub use merge::{
    write_data_files, write_data_files_from_stream, write_data_files_from_stream_with_concurrency,
    write_data_files_with_concurrency,
};
pub use output_spec::{
    parse_output_spec_id, staged_spec_is_partitioned, staging_table, validate_output_spec_id,
};
pub use overwrite::{
    OverwriteIsolation, WRITE_OVERWRITE_ISOLATION_LEVEL, commit_overwrite_replace_all,
    parse_overwrite_isolation, positional_map_overwrite_batch,
    write_overwrite_staged_files_from_stream,
};
pub use overwrite_commit::{commit_overwrite_replace_all_to, commit_replace_write};
pub use overwrite_filter::{commit_overwrite_by_filter_with_summary, spark_overwrite_filter};
pub use overwrite_scope::{
    OVERWRITE_MODE_OPTION, OverwriteIntent, OverwriteMode, OverwritePlan, OverwriteScope,
    overwrite_mode_option_is_dynamic, plan_overwrite, replace_partitions_is_noop,
    validated_static_equalities,
};
pub use partition_overwrite::{
    PartitionEquality, PartitionLiteral, PartitionOverwriteRequest, StaticPartitionOverwrite,
    commit_overwrite_by_row_filter, commit_overwrite_by_row_filter_to, commit_replace_partitions,
    commit_replace_partitions_to, inject_static_partition_columns,
    partition_overwrite_request_from_exprs, stage_static_partition_overwrite_files,
    static_partition_source_columns,
};
pub use partition_write::{WRITTEN_FILES_COL_NAME, write_data_files_from_plan};
pub use position_delete::{MorDmlKind, refuse_mor_unpartitioned_multi_spec_dml};
pub use repark_common::{Error, Result};
pub use replace_schema::replacement_schema;
pub use session_write_conf::{
    IcebergSessionWriteConf, SESSION_CODEC_KEY, SESSION_LEVEL_KEY, SESSION_SNAPSHOT_PREFIX,
    SessionWriteView, apply_session_extras, apply_session_write_key, resolve_empty_session_write,
    resolve_write_for_session, session_write_conf_from_config_map, session_write_conf_from_ctx,
    session_write_conf_from_options, session_write_conf_is_set, unset_session_write_key,
    with_session_write_conf,
};
pub use summary_collision::EngineSummary;
pub use truncate::{commit_truncate, commit_truncate_to};
pub use write_options::{
    WriterStagingOverrides, append_staged_with_options, append_with_statement_options,
    commit_append_with_summary, commit_overwrite_by_row_filter_with_summary,
    commit_overwrite_replace_all_with_summary, commit_replace_partitions_with_summary,
    commit_replace_write_with_summary, isolation_with_override, stage_overwrite_files_with,
    stage_partitioned_stream_with_overrides, stage_static_partition_overwrite_files_with,
    stage_unpartitioned_stream_with_overrides, stage_unpartitioned_with_overrides,
    summary_with_extras,
};
pub use writer_partitioning::{
    SaveTarget, WriterLayout, check_layout_matches_catalog_table, check_layout_matches_table,
    decide_save_target, provided_transforms, save_target_names_table, table_transforms,
};
pub use writer_plan::{
    WriterAction, WriterPlan, WriterRefusal, WriterRequest, WriterStatement,
    missing_column_message, missing_column_name, plan_writer,
};
pub use writer_props::{
    ACCEPTED_CODECS, COMPRESSION_CODEC_PROP, COMPRESSION_LEVEL_PROP, parse_compression,
    parse_target_file_size, target_file_size_with, writer_properties_for, writer_properties_with,
};
