//! repark-iceberg — the Iceberg surface: catalog wiring + the Spark-semantics write adapter.

pub mod catalog;
pub mod write;

#[cfg(test)]
mod tests;

// v1 repark-catalog crate-root surface (names unchanged).
pub use catalog::{
    CATALOG_LISTING_STRATEGY, CatalogCaches, DEFAULT_METADATA_CACHE_ENTRIES, IcebergCacheSettings,
    METADATA_CACHE_ENTRIES_KEY, METADATA_CACHE_ENTRIES_KEY_ALT, METADATA_CACHE_KEY,
    METADATA_CACHE_KEY_ALT, NAMESPACE_LOCATION_PROPERTY, NAMESPACE_LOCATION_URI_PROPERTY,
    ReparkCatalogProvider, build_iceberg_catalog_provider, drop_catalog_namespace_from_provider,
    file_io_for_location, glue_catalog, iceberg_to_datafusion, invalidate_catalog_namespaces,
    list_namespace_names, list_table_names, memory_catalog, memory_catalog_cached,
    mirror_namespace_location_keys, rebuild_catalog_provider, register_iceberg_catalog,
    reregister_catalog_provider, resolve_namespace_location, s3tables_catalog,
    storage_factory_for_location,
};

// v1 repark-write crate-root surface (names unchanged).
pub use write::{
    ACCEPTED_CODECS, COMPRESSION_CODEC_PROP, COMPRESSION_LEVEL_PROP, DEFAULT_MAX_CONCURRENT_FILES,
    Error, FILE_SCOPED_REWRITE_KEY, InsertStoreAssignment, MAX_CONCURRENT_FILES_KEY,
    OverwriteIsolation, Result, SCAN_CONCURRENCY_LIMIT_KEY, SCAN_PRUNING_KEY, ScanConcurrency,
    SnapshotRefKind, SnapshotRefRetention, WRITE_OVERWRITE_ISOLATION_LEVEL, WriteConcurrency,
    append, commit_append, commit_overwrite_replace_all, concurrency_from_config_map,
    concurrency_from_ctx, create_or_replace_snapshot_ref, create_snapshot_ref,
    create_snapshot_ref_with_retention, drop_snapshot_ref, file_scoped_rewrite_from_config_map,
    file_scoped_rewrite_from_ctx, parse_compression, parse_overwrite_isolation,
    positional_map_overwrite_batch, replace_snapshot_ref, scan_concurrency_from_config_map,
    scan_concurrency_from_ctx, scan_pruning_from_config_map, scan_pruning_from_ctx,
    testing_create_ref, with_file_scoped_rewrite, with_merge_session_knobs, with_scan_concurrency,
    with_scan_pruning, with_write_concurrency, write_data_files, write_data_files_from_stream,
    write_data_files_from_stream_with_concurrency, write_data_files_with_concurrency,
    write_overwrite_staged_files_from_stream, write_partitioned_data_files,
    write_partitioned_data_files_from_stream,
    write_partitioned_data_files_from_stream_with_concurrency,
    write_partitioned_data_files_with_concurrency, writer_properties_for,
};
