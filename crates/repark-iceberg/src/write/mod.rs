//! Spark-semantics write adapter over the owned iceberg-rust fork.

pub mod alter;
pub mod append;
pub mod commit_target;
pub mod concurrency;
pub(crate) mod conform;
pub(crate) mod file_order;
pub mod file_scoped_rewrite;
pub mod format_version;
/// Shared Spark/DF `quote_ident` + path-escape needles (CQ-006/007).
pub mod idents;
/// WI-2: the plain-INSERT store-assignment gate, as an `AnalyzerRule` over `LogicalPlan::Dml`.
pub mod insert_gate;
pub mod merge;
mod name_resolution;
/// OV1 exclusive full-table overwrite commit (stage-then-swap).
pub mod overwrite;
pub mod overwrite_commit;
/// Partition-scoped INSERT OVERWRITE (static row-filter + dynamic replace-partitions).
pub mod partition_overwrite;
pub(crate) mod position_delete;
/// Identity DELETE/UPDATE (G3-E8 A1): SELECT over pinned `(_file, _pos)`, MERGE write arms.
pub mod predicate_dml;
pub mod scan_concurrency;
pub mod scan_prune;
/// Product snapshot-ref helpers (CREATE/DROP BRANCH|TAG) + test-support seam.
pub mod snapshot_refs;
/// The ANSI store-assignment matrix — ONE home for MERGE and the non-MERGE insert/append lowerings.
pub(crate) mod store_assign;
/// Test-support-only snapshot-ref helpers (`_testing_create_ref`).
pub mod testing_support;
/// Whole-table `TRUNCATE TABLE` (delete-only empty overwrite).
pub mod truncate;
pub mod writer_props;

pub use snapshot_refs::{
    SnapshotRefKind, SnapshotRefRetention, create_or_replace_snapshot_ref, create_snapshot_ref,
    create_snapshot_ref_with_retention, drop_snapshot_ref, replace_snapshot_ref,
};
pub use testing_support::testing_create_ref;

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
pub use overwrite::{
    OverwriteIsolation, WRITE_OVERWRITE_ISOLATION_LEVEL, commit_overwrite_replace_all,
    parse_overwrite_isolation, positional_map_overwrite_batch,
    write_overwrite_staged_files_from_stream,
};
pub use overwrite_commit::commit_overwrite_replace_all_to;
pub use partition_overwrite::{
    EMPTY_DYNAMIC_OVERWRITE_NEEDLE, PartitionEquality, PartitionLiteral, PartitionOverwritePlan,
    PartitionOverwriteRequest, StaticPartitionOverwrite, commit_overwrite_by_row_filter,
    commit_overwrite_by_row_filter_to, commit_replace_partitions, commit_replace_partitions_to,
    inject_static_partition_columns, partition_overwrite_request_from_exprs,
    plan_partition_overwrite, refuse_empty_dynamic_overwrite,
    stage_static_partition_overwrite_files,
};
pub use position_delete::{MorDmlKind, refuse_mor_unpartitioned_multi_spec_dml};
pub use repark_common::{Error, Result};
pub use truncate::{commit_truncate, commit_truncate_to};
pub use writer_props::{
    ACCEPTED_CODECS, COMPRESSION_CODEC_PROP, COMPRESSION_LEVEL_PROP, parse_compression,
    writer_properties_for,
};
