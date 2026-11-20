# map — repark-iceberg/src/write

## Layout

Thin Spark-semantics write adapter over the owned iceberg-rust fork. The heavy
table-format machinery (actions, position-delete writers, `UpdateSchema`, snapshot
management) lives in the fork; this tree translates Spark write semantics onto the
fork's native actions plus an OCC retry loop. One row per area; every leaf and
submap links its files. The pre-condensation record is
[history copy](../../../../docs/history/map-ratchet-1/crates-repark-iceberg-src-write-map-2026-11-20.md).

| area | files | charter |
|---|---|---|
| manifest | [mod.rs](mod.rs), [testing_support.rs](testing_support.rs) | module manifest; test-only support |
| append | [append.rs](append.rs), [append_fanout_serial.rs](append_fanout_serial.rs), [staged_publish.rs](staged_publish.rs), [overwrite_commit.rs](overwrite_commit.rs), [hadoop_stale_commit.rs](hadoop_stale_commit.rs), [commit_error.rs](commit_error.rs), [commit_target.rs](commit_target.rs) | bulk append, staged publish, commit targets and errors |
| overwrite | [overwrite.rs](overwrite.rs), [overwrite_filter/map.md](overwrite_filter/map.md), [overwrite_filter.rs](overwrite_filter.rs), [overwrite_scope.rs](overwrite_scope.rs), [partition_overwrite.rs](partition_overwrite.rs), [partitioned_files.rs](partitioned_files.rs), [partition_write.rs](partition_write.rs) | full-table and partition overwrites, scopes |
| predicate | [predicate_dml/map.md](predicate_dml/map.md), [predicate_dml.rs](predicate_dml.rs), [conflict_filter.rs](conflict_filter.rs), [position_delete.rs](position_delete.rs), [position_delete_batch.rs](position_delete_batch.rs), [file_scoped_rewrite.rs](file_scoped_rewrite.rs) | identity DELETE/UPDATE, conflict filters, position deletes |
| merge | [merge/map.md](merge/map.md) | MERGE INTO semantics |
| statement | [statement_writes/map.md](statement_writes/map.md), [statement_writes.rs](statement_writes.rs), [insert_gate.rs](insert_gate.rs), [insert_defaults/map.md](insert_defaults/map.md), [insert_defaults.rs](insert_defaults.rs), [sql_text.rs](sql_text.rs), [static_value.rs](static_value.rs) | statement writes, INSERT gate, defaults |
| conform | [conform.rs](conform.rs), [conform_stage/map.md](conform_stage/map.md), [conform_stage.rs](conform_stage.rs), [store_assign.rs](store_assign.rs), [store_cast.rs](store_cast.rs), [store_fold.rs](store_fold.rs), [store_overflow/map.md](store_overflow/map.md), [store_overflow.rs](store_overflow.rs), [negated_null_store.rs](negated_null_store.rs), [void_store.rs](void_store.rs), [ntz_store.rs](ntz_store.rs) | schema conform, store assignment, casts, overflow |
| evolution | [schema_evolution/map.md](schema_evolution/map.md), [schema_evolution.rs](schema_evolution.rs), [struct_pairing/map.md](struct_pairing/map.md), [struct_pairing.rs](struct_pairing.rs), [nested_column.rs](nested_column.rs), [nested_type_sql.rs](nested_type_sql.rs), [nested_ns_gate/map.md](nested_ns_gate/map.md), [nested_ns_gate.rs](nested_ns_gate.rs), [name_resolution.rs](name_resolution.rs), [map_key_change.rs](map_key_change.rs), [column_move.rs](column_move.rs) | schema evolution, struct pairing, nested writes |
| partition | [partition_spec/map.md](partition_spec/map.md), [partition_spec.rs](partition_spec.rs), [distribution/map.md](distribution/map.md), [distribution.rs](distribution.rs), [fanout_order.rs](fanout_order.rs), [fanout_order_tests.rs](fanout_order_tests.rs), [file_order.rs](file_order.rs), [writer_partitioning.rs](writer_partitioning.rs), [output_spec.rs](output_spec.rs) | partition specs, hash distribution, file order |
| offsets | [sink_offsets/map.md](sink_offsets/map.md), [sink_offsets.rs](sink_offsets.rs), [sink_offsets_append_fence_tests.rs](sink_offsets_append_fence_tests.rs), [sink_offsets_epoch_tests.rs](sink_offsets_epoch_tests.rs), [sink_offsets_fence_tests.rs](sink_offsets_fence_tests.rs), [sink_offsets_isolation_tests.rs](sink_offsets_isolation_tests.rs), [sink_offsets_probe_tests.rs](sink_offsets_probe_tests.rs), [sink_offsets_scope_tests.rs](sink_offsets_scope_tests.rs), [sink_offsets_tests.rs](sink_offsets_tests.rs), [sink_offsets_walk_tests.rs](sink_offsets_walk_tests.rs) | micro-batch sink offsets and fences |
| admin | [meta_delete/map.md](meta_delete/map.md), [meta_delete.rs](meta_delete.rs), [snapshot_refs.rs](snapshot_refs.rs), [table_admin.rs](table_admin.rs), [set_location.rs](set_location.rs), [truncate.rs](truncate.rs), [alter.rs](alter.rs), [session_write_conf.rs](session_write_conf.rs), [write_options.rs](write_options.rs), [writer_props.rs](writer_props.rs), [writer_plan.rs](writer_plan.rs), [scan_concurrency.rs](scan_concurrency.rs), [scan_prune.rs](scan_prune.rs) | table admin, refs, ALTER, session confs, scans |
| misc | [data_format.rs](data_format.rs), [format_version.rs](format_version.rs), [idents.rs](idents.rs), [illegal_argument.rs](illegal_argument.rs), [unsupported.rs](unsupported.rs), [unsupported_tests.rs](unsupported_tests.rs), [encryption.rs](encryption.rs), [encryption_tests.rs](encryption_tests.rs), [sort_order.rs](sort_order.rs), [summary_collision.rs](summary_collision.rs), [update_cast.rs](update_cast.rs), [concurrency.rs](concurrency.rs), [resource_bound.rs](resource_bound.rs) | formats, refusals, encryption, sort, bounds |

Rules for new files: a new leaf joins the row of its area; a new area is a new row
with its submap.

## Purpose

`DELETE`/`UPDATE`/`INSERT` need no adapter: DataFusion plans them onto the fork's
`iceberg-datafusion` `TableProvider`. Plain `INSERT`'s store gate is an
`AnalyzerRule` ([insert_gate.rs](insert_gate.rs)), one stage before the cast, where
the pre-cast source type is still in the plan. Error boundary: this tree
re-exports `repark_common::{Error, Result}` for MERGE/append; the fold lives in
repark-core's error map.

## I want to...

| ...do this | go to |
|---|---|
| Stage and commit an append or overwrite | the `append` or `overwrite` row |
| Change identity DELETE/UPDATE or conflict scope | the `predicate` row |
| Change MERGE INTO semantics | the `merge` row |
| Change a statement write or the INSERT gate | the `statement` row |
| Ask whether a type pair may be written | [store_assign.rs](store_assign.rs) (`ansi_store_assignable`) |
| Change schema evolution or nested writes | the `evolution` row |
| Change partitioning, distribution or file order | the `partition` row |
| Change the micro-batch sink | the `offsets` row |
| Change table admin, refs or session confs | the `admin` row |
| Read the pre-condensation record | the history copy under Layout |
| Run the suite | `cargo test -p repark-iceberg write::` (all on `MemoryCatalog`) |

## Pointers

- Up: [../map.md](../map.md)
- Fork contract: `docs/ENGINE_CONTRACT.md` in the owned fork
- Pre-condensation record: [history copy](../../../../docs/history/map-ratchet-1/crates-repark-iceberg-src-write-map-2026-11-20.md)

## Debug

| Symptom | First check |
|---|---|
| SET/UNSET TBLPROPERTIES not landing | the action must be `.apply(tx)`'d and `tx.commit(catalog)` awaited |
| `append` rows land in one partition | fanout must pass each split group's own `PartitionKey`; inspect `DataFile.partition` |
| Streaming CTAS OOMs | use the `_from_stream` writers over `execute_stream()`, never `collect()` |
| A partitioned CTAS writes writers x values files | `hash_distribution` must wrap the input when the spec is partitioned and `writers > 1` |
| MERGE OOMs on a large target | target must register as a `StreamingTable`, never a full-target `MemTable` |
| MERGE produces duplicates | multiple-source-match must error like Spark; serializable commits carry `validate_no_conflicting_data` |
| Rejected MERGE OCC commit left Parquet files behind | abort must `FileIO::delete` writer-result paths only, never re-derive from manifests |

First checks: `cargo test -p repark-iceberg write::`. Escalate to: [../../map.md#debug](../../map.md).
