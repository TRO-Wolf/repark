# perf-scan-1-plan-once pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-iceberg/src/write/merge/tests/partition_sink.rs::three_concurrent_target_scan_executes_plan_data_manifests_once
C-002: crates/repark-iceberg/src/write/merge/tests/partition_sink.rs::a_multi_manifest_identity_scan_records_the_touched_path, crates/repark-iceberg/src/write/merge/tests/partition_sink.rs::manifest_partitions
C-003: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
C-004: none (records)
