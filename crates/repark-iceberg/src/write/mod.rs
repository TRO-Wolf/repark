//! Spark-semantics write adapter — the thin `RePark` surface over the owned iceberg-rust fork.
//!
//! Per ADR-0002 (owned-fork pivot, 2026-06-06) the heavy table-format machinery —
//! `OverwriteFiles` / `RowDelta` / `RewriteFiles` / `DeleteFiles` actions, position-delete / DV
//! writers, `UpdateSchema`, snapshot & maintenance — lives in the **owned fork**, not here (the
//! `position_delete` module DRIVES the fork's `PositionDeleteFileWriter`; it does not reimplement
//! one). This
//! module only translates Spark write semantics (`MERGE INTO` / `DELETE` / `UPDATE` /
//! `INSERT OVERWRITE` / `ALTER ... COLUMN`) onto the fork's native actions plus an OCC retry loop.
//! No hand-rolled `SnapshotProducer` / `TableCommit` here.
//!
//! Primitives landed: [`alter`] (`ALTER TABLE` SET/UNSET TBLPROPERTIES + RENAME TO + schema
//! evolution via fork `UpdateSchema` — I6 — on the public `Transaction` / `Catalog` API),
//! [`merge`] (`MERGE INTO`, copy-on-write AND
//! merge-on-read — the fork's `ENGINE_CONTRACT` §6 makes MERGE engine-owned; the merge-on-read arm
//! writes position-delete files via [`position_delete`] and commits them with the new data files in
//! ONE `RowDelta`), [`predicate_dml`] (G3-E8 identity DELETE/UPDATE over `(_file, _pos)`),
//! and [`append`] (the public bulk
//! append — downstream ask A1 — committing add-only through the stamped `fast_append` path with
//! identity-partition fanout). Ordinary (non-subquery) `DELETE` / `UPDATE` still ride DataFusion
//! onto the fork's `TableProvider` (ADR-0003). Non-empty `INSERT OVERWRITE` stage-then-swap commits via
//! [`overwrite::commit_overwrite_replace_all`] (OV1 exclusive — Q9); empty wipe stays at the SQL
//! router (C1-Q-001).
//!
//! **Error boundary (C1-CRATE-001 honesty):** this module re-exports `repark_common::{Error, Result}`
//! for MERGE / append, but the `alter` primitives still return `iceberg::Result` and the SQL
//! layer folds those errors. A full retype of every public surface onto `repark_common::Error` is
//! deferred — the session/PyO3 classifier remains the real FFI boundary.

pub mod alter;
pub mod append;
pub mod concurrency;
pub mod file_scoped_rewrite;
/// Shared Spark/DF `quote_ident` + path-escape needles (r23 QI1 / CQ-006/007).
pub mod idents;
pub mod merge;
mod name_resolution;
/// OV1 exclusive full-table overwrite commit (stage-then-swap). CACHE1 must not call (Q9).
pub mod overwrite;
pub(crate) mod position_delete;
/// Identity DELETE/UPDATE (G3-E8 A1): SELECT over pinned `(_file, _pos)`, MERGE write arms.
pub mod predicate_dml;
pub mod scan_concurrency;
pub mod scan_prune;
/// Product snapshot-ref helpers (I5 CREATE/DROP BRANCH|TAG) + test-support seam.
pub mod snapshot_refs;
/// The ANSI store-assignment matrix (`Cast.canANSIStoreAssign`) — ONE home for MERGE and the
/// non-MERGE insert/append lowerings (WI-1 hoist out of `merge/insert.rs`).
pub(crate) mod store_assign;
/// Test-support-only snapshot-ref helpers (`_testing_create_ref`). Product SQL routes via
/// [`snapshot_refs`]; this seam stays for existing fixtures (I1 / I5).
pub mod testing_support;
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
pub use merge::{
    write_data_files, write_data_files_from_stream, write_data_files_from_stream_with_concurrency,
    write_data_files_with_concurrency,
};
pub use overwrite::{
    OverwriteIsolation, WRITE_OVERWRITE_ISOLATION_LEVEL, commit_overwrite_replace_all,
    parse_overwrite_isolation, positional_map_overwrite_batch,
    write_overwrite_staged_files_from_stream,
};
pub use position_delete::{MorDmlKind, refuse_mor_unpartitioned_multi_spec_dml};
pub use repark_common::{Error, Result};
pub use writer_props::{
    ACCEPTED_CODECS, COMPRESSION_CODEC_PROP, COMPRESSION_LEVEL_PROP, parse_compression,
    writer_properties_for,
};
