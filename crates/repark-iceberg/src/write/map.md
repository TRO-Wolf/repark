# map — repark-iceberg/src/write

## Purpose

The **thin Spark-semantics write adapter** over the owned iceberg-rust fork (v1 `repark-write`,
ported byte-faithful). The heavy table-format machinery (`OverwriteFiles` / `RowDelta` /
`RewriteFiles` actions, position-delete writers, `UpdateSchema`, snapshot management) lives in
the fork; this tree only translates Spark write semantics onto the fork's native actions plus an
OCC retry loop. `DELETE`/`UPDATE`/`INSERT` need no adapter — DataFusion plans them onto the
fork's `iceberg-datafusion` `TableProvider`.

**Error boundary:** re-exports `repark_common::{Error, Result}` for MERGE/append, but the
`alter` and `snapshot_refs` primitives still return `iceberg::Result` — the fold lives in
repark-core's error map.

## Contents

- `mod.rs` (v1 `lib.rs`) — module decls + the public re-export list (names unchanged from v1):
  `Error`/`Result`, write/scan concurrency knobs, `writer_props`, the `write_data_files*` +
  `write_partitioned_data_files*` families (bounded-memory stream variants; K concurrent file
  writers, default 4, K=1 serial), `append`, the overwrite stage-then-swap surface, and the
  snapshot-ref helpers.
- `merge/` — the RePark-owned `MERGE INTO` executor (copy-on-write AND merge-on-read per
  `write.merge.mode`, fork ENGINE_CONTRACT §6). See [merge/map.md](merge/map.md).
- `append.rs` — `append(catalog, ident, batches)`: public bulk append — conform (missing /
  extra / duplicate column = loud error; strict casts, overflow never NULLs) → identity-
  partition fanout write → ONE stamped `fast_append` commit (append×append commutes via the
  fork's refresh-and-re-apply retry; empty input commits an empty stamped snapshot). Also
  `write_partitioned_data_files(_from_stream)` — the partitioned staged-write core.
- `overwrite.rs` — exclusive full-table `INSERT OVERWRITE` stage-then-swap:
  `write_overwrite_staged_files_from_stream` (positional map + stream stage) +
  `commit_overwrite_replace_all` + `parse_overwrite_isolation` (absent→snapshot | snapshot |
  serializable | none | invalid-loud).
- `alter.rs` — `ALTER TABLE` primitives on iceberg-rust public API: SET/UNSET TBLPROPERTIES
  (`alter_table_properties(sets, unsets)` commits both as ONE action — no half-applied state),
  `rename_table`, schema evolution (`apply_schema_changes` / `SchemaChange` → fork
  `UpdateSchema`), partition-spec evolution (`apply_partition_spec_changes` /
  `PartitionSpecChange` → fork `UpdatePartitionSpec`). Return `iceberg::Result`.
- `snapshot_refs.rs` — product CREATE/DROP/REPLACE BRANCH|TAG helpers over fork
  `ManageSnapshots` (+ retention setters). Write-to-branch STOP documented (fork FastAppend
  always MAIN_BRANCH).
- `testing_support.rs` — `testing_create_ref` (wraps `create_snapshot_ref`) for fixtures only;
  product SQL routes via `snapshot_refs`.
- `concurrency.rs` — `repark.write.max-concurrent-files` (default 4, ≥1 or loud): DataFusion
  `ConfigExtension` (`ReparkWriteConfig`) + builder-map parse (hyphen + underscore). Parallel
  drivers share an abort flag so source/worker errors skip `finish`/`close`.
- `scan_concurrency.rs` — `repark.scan.concurrency-limit` (optional; unset = fork default) for
  the MERGE target scan's `with_concurrency_limit`.
- `scan_prune.rs` — MERGE target-scan pruning + ON bare-equality parser + residual bounds
  (`repark.merge.scan-pruning`, default true); `ReparkMergeConfig` also carries
  `file_scoped_rewrite`.
- `file_scoped_rewrite.rs` — filter `FileScanTask`s by affected-path allowlist
  (`repark.merge.file-scoped-rewrite`); refuses a non-empty allowlist matching zero or partial
  path set (survivor-loss guard).
- `name_resolution.rs` (crate-private) — the shared case-insensitive by-name column resolver
  (Spark `spark.sql.caseSensitive=false` conform semantics); used by both `append` conform and
  merge star expansion so the two surfaces cannot drift.
- `position_delete.rs` (crate-private; two `pub` re-exports via `mod.rs`) — merge-on-read
  WRITE primitive: turn `(_file, _pos)` pairs into committable position-delete `DataFile`s by
  driving the fork's production `PositionDeleteFileWriter`. Owns sort order (ascending
  `(file_path, pos)`) and partition stamping (each delete file carries the `(spec_id,
  partition)` of the data file it deletes from, resolved from the snapshot's DATA manifests —
  never the table's current default spec). Also hosts the BUG-001 P0 valve
  (`MorDmlKind` + `refuse_mor_unpartitioned_multi_spec_dml`, hoisted from the v1 SQL crate in
  phase-2 PR-3b): refuse merge-on-read SQL DELETE/UPDATE when the current default spec is
  unpartitioned and multi-spec history exists — the fork position-delete fast-path under-delete
  hazard this file's stamping discipline exists to avoid. The SQL door resolves the target and
  calls it; the door's `bug001_*` battery pins it end to end.
- `idents.rs` — shared Spark/DF `quote_ident_spark` + path-escape needles + `probes` tables
  (single source; MERGE `quote_ident` delegates here).
- `writer_props.rs` — Parquet `WriterProperties` from Iceberg
  `write.parquet.compression-codec` (+ optional level). Default **zstd** when absent (Java
  Iceberg 1.4+ parity); accepted `zstd|snappy|gzip|lz4|uncompressed`; unknown = loud error.
  Shared by append / MERGE data files / position deletes.

## I want to...

| ...do this | go to |
|---|---|
| ALTER TABLE properties / rename / schema / partition evolution | `alter.rs` |
| Bulk-append batches through the sanctioned commit path | `append.rs` (`append`) |
| Stream a SELECT into a staged (CTAS) write with bounded memory | `write_data_files_from_stream` (`merge/mod.rs`) / `write_partitioned_data_files_from_stream` (`append.rs`) |
| Stage + commit full-table INSERT OVERWRITE | `overwrite.rs` |
| Cap concurrent Iceberg file writers (session conf) | `repark.write.max-concurrent-files` via `concurrency.rs` |
| Parquet compression codec (table property) | `writer_props.rs` |
| Change MERGE INTO semantics | [merge/map.md](merge/map.md) |
| Wire DELETE/UPDATE/INSERT OVERWRITE | nothing here — DataFusion → fork `TableProvider` |
| CREATE/DROP BRANCH or TAG | `snapshot_refs.rs` |

## Pointers

- Up: [../map.md](../map.md)
- Fork contract: `docs/ENGINE_CONTRACT.md` in the owned fork.

## Debug

| Symptom | First check |
|---|---|
| SET/UNSET TBLPROPERTIES not landing | the action must be `.apply(tx)`'d and `tx.commit(catalog)` awaited; empty-action commit no-ops |
| `append` rows land in one partition | fanout must pass EACH split group's own `PartitionKey` to `FanoutWriter::write`; inspect `DataFile.partition` in committed manifests |
| UNSET errors "present in both removal and update set" | a key was both set and removed in one action — the router only passes disjoint keys |
| Streaming CTAS OOMs / collects the whole SELECT | must use the `_from_stream` writers over `execute_stream()`, never `collect()` |
| Parallel write left partial files after a failed MERGE | abort flag must skip `finish()`/`close()` |
| MERGE OOMs on a large target | target must register as a `StreamingTable` (`(_file, _pos)` identity), never a full-target `MemTable` |
| MERGE produces duplicates | multiple-source-match must **error** (like Spark); both commit arms must carry `validate_no_conflicting_data` |
| Conflict-retry corrupts data | on commit conflicts re-read the target; don't cache stale file lists |

First checks: `cargo test -p repark-iceberg write::` (all on `MemoryCatalog`). Escalate to:
[../../map.md#debug](../../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in task/p3e-facade-ledger.md.
