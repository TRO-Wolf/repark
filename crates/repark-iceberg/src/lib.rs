//! repark-iceberg — the Iceberg surface: catalog wiring + the Spark-semantics write adapter.
//!
//! Two independent module trees merged from the v1 crates (declared-rename unit):
//! [`catalog`] (v1 `repark-catalog` — Glue primary + S3 Tables secondary + memory builders,
//! `CatalogProvider` registration, scheme-based `FileIO` selection) and [`write`] (v1
//! `repark-write` — MERGE INTO, append, overwrite, ALTER, snapshot refs over the owned
//! iceberg-rust fork). Public names are unchanged: v1 `repark_catalog::X` is
//! `repark_iceberg::catalog::X`, v1 `repark_write::Y` is `repark_iceberg::write::Y`. The crate
//! root additionally re-exports the union of the two v1 crate-root re-export lists.

pub mod catalog;
pub mod write;

/// Shared test-only tracing harness (forced-edit class 6): one global subscriber carrying
/// both v1 capture layers, so the merged test binary keeps each v1 harness's per-binary
/// global-subscriber invariant.
#[cfg(test)]
mod test_tracing;

/// Fork-pin proof (ADR-0001): names + exercises fork-only public API, so the test target
/// compile-fails on a silent crates.io registry fallback.
#[cfg(test)]
mod fork_pin_tests;

// v1 repark-catalog crate-root surface (names unchanged).
pub use catalog::{
    CATALOG_LISTING_STRATEGY, MetadataProjectionSchemaProvider, NAMESPACE_LOCATION_PROPERTY,
    NAMESPACE_LOCATION_URI_PROPERTY, ProjectingMetadataTableProvider, ReparkCatalogProvider,
    build_iceberg_catalog_provider, drop_catalog_namespace_from_provider, file_io_for_location,
    glue_catalog, invalidate_catalog_namespaces, list_namespace_names, list_table_names,
    memory_catalog, mirror_namespace_location_keys, rebuild_catalog_provider,
    register_iceberg_catalog, reregister_catalog_provider, resolve_namespace_location,
    s3tables_catalog, storage_factory_for_location,
};

// v1 repark-write crate-root surface (names unchanged).
pub use write::{
    ACCEPTED_CODECS, COMPRESSION_CODEC_PROP, COMPRESSION_LEVEL_PROP, DEFAULT_MAX_CONCURRENT_FILES,
    Error, FILE_SCOPED_REWRITE_KEY, MAX_CONCURRENT_FILES_KEY, OverwriteIsolation, Result,
    SCAN_CONCURRENCY_LIMIT_KEY, SCAN_PRUNING_KEY, ScanConcurrency, SnapshotRefKind,
    SnapshotRefRetention, WRITE_OVERWRITE_ISOLATION_LEVEL, WriteConcurrency, append, commit_append,
    commit_overwrite_replace_all, concurrency_from_config_map, concurrency_from_ctx,
    create_or_replace_snapshot_ref, create_snapshot_ref, create_snapshot_ref_with_retention,
    drop_snapshot_ref, file_scoped_rewrite_from_config_map, file_scoped_rewrite_from_ctx,
    parse_compression, parse_overwrite_isolation, positional_map_overwrite_batch,
    replace_snapshot_ref, scan_concurrency_from_config_map, scan_concurrency_from_ctx,
    scan_pruning_from_config_map, scan_pruning_from_ctx, testing_create_ref,
    with_file_scoped_rewrite, with_merge_session_knobs, with_scan_concurrency, with_scan_pruning,
    with_write_concurrency, write_data_files, write_data_files_from_stream,
    write_data_files_from_stream_with_concurrency, write_data_files_with_concurrency,
    write_overwrite_staged_files_from_stream, write_partitioned_data_files,
    write_partitioned_data_files_from_stream,
    write_partitioned_data_files_from_stream_with_concurrency,
    write_partitioned_data_files_with_concurrency, writer_properties_for,
};
