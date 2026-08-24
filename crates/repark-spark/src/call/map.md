# map — repark-spark/src/call

## Purpose

Per-procedure bodies for the maintenance `CALL` router (`../call.rs`). The router keeps argument
parsing, table-ident resolution and the procedures already in it; a procedure moves here
when its body plus its measured-parity documentation would push `call.rs` over the 1500-line
`check_rust_file_size` ceiling. MW-6 opened the directory with `rewrite_manifests`; `call.rs`
keeps the six it already held (`expire_snapshots`, `rewrite_data_files`,
`rewrite_position_delete_files`, `remove_orphan_files`, `rollback_to_snapshot`,
`register_table`).

## Contents

- `rewrite_manifests.rs` — **MW-6**: `CALL <catalog>.system.rewrite_manifests(table => …)` over
  the fork's `RewriteManifestsAction` (`transaction/rewrite_manifests.rs`). The action returns no
  counts, so Spark's two columns are read from the new snapshot's summary
  (`manifests-replaced` → `rewritten_manifests_count`, `manifests-created` →
  `added_manifests_count`). Three guards make the answer Spark's rather than the fork's: a table
  with no snapshot returns zeros where the action errors; Spark's no-op rule (one matching
  manifest already at target size) returns zeros and commits nothing; and a zero answer refuses
  while two or more delete manifests stay uncompacted, because the fork rewrites data manifests
  only (registry `MANIFEST-1`). `rewrite_if` pins Java's default current-spec filter; `spec_id`
  refuses and `use_caching` is an accepted no-op (registry `MANIFEST-2`). Above
  `commit.manifest.target-size-bytes` the two engines write a different NUMBER of manifests, so
  `added_manifests_count` diverges there (registry `MANIFEST-3`); `rewritten_manifests_count`
  agrees at every size measured.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../tests/call_manifests.rs](../tests/call_manifests.rs) and
  `python/repark/tests/test_maintenance_call.py`
- Divergences: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
  rows `MANIFEST-1`, `MANIFEST-2`

## Debug

| Symptom | First check |
|---|---|
| A rewrite answered `1, 1` on an already-compacted table | The no-op guard: Spark's rule is `targetNumManifests == 1 && matching.size() == 1` |
| A rewrite refused on a merge-on-read table | The delete-manifest guard — compact the delete FILES first with `rewrite_position_delete_files` |
| The counts disagree with Spark on a table whose spec evolved | `rewrite_if` must filter to `default_partition_spec_id` |
| `added_manifests_count` disagrees with Spark and the table is above the manifest target size | Expected — registry `MANIFEST-3`; the fork rolls on an estimate where Java repartitions into `ceil(total / target)` |
| The commit succeeded but the counts errored | The fork stopped writing `manifests-replaced` / `manifests-created`; the summary is the only source |

First checks: `cargo test -p repark-spark call_manifests::`.
