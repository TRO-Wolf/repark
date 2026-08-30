# map — repark-iceberg/src/write

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

The **thin Spark-semantics write adapter** over the owned iceberg-rust fork (v1 `repark-write`,
ported byte-faithful). The heavy table-format machinery (`OverwriteFiles` / `RowDelta` /
`RewriteFiles` actions, position-delete writers, `UpdateSchema`, snapshot management) lives in
the fork; this tree only translates Spark write semantics onto the fork's native actions plus an
OCC retry loop. `DELETE`/`UPDATE`/`INSERT` need no adapter — DataFusion plans them onto the
fork's `iceberg-datafusion` `TableProvider`.
Source documentation may retain model provenance; code-quality grade tags stay outside code.
Source comments are condensed to API and safety contracts; executable behavior is unchanged.

**The gap WI-1 named, closed by WI-2 (2026-08-15):** plain `INSERT` still has no adapter here —
DataFusion's own `insert_to_plan` injects the `CAST` and hands a schema-conformed plan straight to
the fork's `IcebergTableProvider::insert_into` — so the gate could not be a call site on a write
lowering. It is an `AnalyzerRule` instead (`insert_gate.rs`), one stage EARLIER, where the
pre-cast source type is still in the plan. `INSERT INTO … SELECT`, `writeTo().append()` and
`write.insertInto()` now refuse the `Date32 → Int32` reinterpretation (`18262`) that Spark
refuses. Named residual: a literal `INSERT INTO … VALUES` row — see `insert_gate.rs`.

**Error boundary:** re-exports `repark_common::{Error, Result}` for MERGE/append, but the
`alter` and `snapshot_refs` primitives still return `iceberg::Result` — the fold lives in
repark-core's error map.

## Contents

- `mod.rs` (v1 `lib.rs`) — module decls + the public re-export list (names unchanged from v1):
  `Error`/`Result`, write/scan concurrency knobs, `writer_props`, the `write_data_files*` +
  `write_partitioned_data_files*` families (bounded-memory stream variants; K concurrent file
  writers, default 4, K=1 serial), `append`, the overwrite stage-then-swap surface, and the
  snapshot-ref helpers. `store_assign` is declared `pub(crate)` — an internal predicate, never
  a public surface.
- `merge/` — the RePark-owned `MERGE INTO` executor (copy-on-write AND merge-on-read per
  `write.merge.mode`, fork ENGINE_CONTRACT §6). See [merge/map.md](merge/map.md).
- `row_lineage_guard.rs` (crate-private; `refuse_v3_cow_dml` re-exported) — **V3R-1
  (2026-08-25, `V3-COW-1`); RP-2 (2026-08-27, fork `ce92a7bf`):** the format-v3 row-DML guard,
  two seats — the write-mode resolvers (`predicate_dml.rs`, `merge/mod.rs`) and the passthrough
  valve both doors call beside the BUG-001 valve. The resolvers refuse every v3 table. The
  passthrough valve lifts the plain-`WHERE` DELETE on v3, including on a table that already
  carries deletion vectors (RP-3 / F-17 at `d408da42`), and still refuses every v3 UPDATE
  (V3-3). pins: rp-3-fork-repin/C-004
- `predicate_dml.rs` — **G3-E8 A1-identity** (`execute_predicate_dml`): evaluate the original
  `WHERE` as a SELECT over the pinned `(_file, _pos)` streaming target, then commit through the
  MERGE COW/MoR write arms honoring `write.delete.mode` / `write.update.mode` / isolation —
  **never** `write.merge.mode`. Product hole is the valve allow-list (uncorrelated
  `DELETE … IN` / `NOT IN (SELECT …)`, including the NULL 3VL trap, `[NOT] EXISTS` ±
  correlation, correlated IN, and identity `UPDATE … SET <scalar> WHERE col IN`). ANY/ALL
  stay refused (Spark 4.1.2 parse-fails quantified comparisons). Pins:
  [predicate_dml/predicate_dml_tests.rs](predicate_dml/predicate_dml_tests.rs) +
  [predicate_dml/predicate_dml_update_tests.rs](predicate_dml/predicate_dml_update_tests.rs)
  — **LRS-5 (2026-08-20):** moved into the canonical module tree, `#[path]` gone. Isolation
  property pins (M19 / A10: no trim, `to_ascii_lowercase`, default serializable,
  garbage ⇒ Plan `Invalid isolation level: {name}`) live in those two test
  files. **MW-9:** `resolve_write_mode` parses `write.delete.granularity` on the
  MoR arm before identity UPDATE/DELETE writes parquet (same refuse-before-IO
  class as `resolve_merge_mode`). Ledger:
  [`../../../../task/r1-g3e8-pr4-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-14-r1-g3e8-pr4-ledger.md).
- `append.rs` — `append(catalog, ident, batches)`: public bulk append — conform (missing /
  extra / duplicate column = loud error; **WI-1** ANSI store-assignment gate then strict casts,
  overflow never NULLs) → identity-partition fanout write → ONE stamped `fast_append` commit
  (append×append commutes via the fork's refresh-and-re-apply retry; empty input commits an
  empty stamped snapshot). Also `write_partitioned_data_files(_from_stream)` — the partitioned
  staged-write core. **V3-1:** `iceberg_err` now goes through `catalog::iceberg_to_datafusion`
  so a Hadoop `vN.metadata.json` pointer names the convention (registry `V3-ADOPT-1`).
- `overwrite.rs` — exclusive full-table `INSERT OVERWRITE` stage-then-swap:
  `write_overwrite_staged_files_from_stream` (positional map + **WI-1** store-assignment gate +
  stream stage) + `commit_overwrite_replace_all` + `parse_overwrite_isolation`
  (absent→snapshot | snapshot | serializable | none | invalid-loud).
- `insert_gate.rs` — **WI-2 (2026-08-15):** `InsertStoreAssignment`, an `AnalyzerRule` over
  `LogicalPlan::Dml(WriteOp::Insert(_))` that runs `store_assign.rs`'s matrix — imported, never
  duplicated — against the pre-cast types in the synthesized projection's INPUT schema. Registered
  by `repark_spark::SparkExtension::register`, BEFORE `repark_functions::analyzer_rules()`, so a
  `DATE → INT` insert cites Spark's WRITE class rather than the CAST class. Judges exactly
  `Alias(Cast(Column(c), target))`: that shape is provably the conform cast DataFusion
  synthesized, while a user-written explicit `CAST` (legal Spark — the user's stated intent)
  reaches this projection already conformed, as a bare column, and is invisible to the rule.
  Named residual: `Cast(Literal, …)` inside a `Values` node, where the synthesized and explicit
  forms are byte-identical. Ledger:
  [`../../../../task/wi2-g6-cast-integrity-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-16-wi2-g6-cast-integrity-ledger.md).
- `store_assign.rs` (crate-private) — **WI-1 (2026-08-15):** the ONE home for Spark's ANSI
  store-assignment matrix (`Cast.canANSIStoreAssign` → Arrow):
  `ansi_store_assignable` / `normalize_for_assignment` /
  `refuse_unless_ansi_store_assignable` (`MERGE `-labelled callers, class
  `INCOMPATIBLE_DATA_FOR_TABLE` — byte-identical #111/#135 text) and
  `refuse_unless_write_store_assignable` (non-MERGE write paths, sub-class
  `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`). Hoisted out of `merge/insert.rs`, which
  had the only two call sites in the tree, so `append.rs` / `overwrite.rs` share the predicate
  instead of forking a second one. Needle `not ANSI-store-assignable`. Named narrowing: the
  write-path entry point excuses NESTED pairs (the v1 matrix judges them by identity, which
  would be a NEW refusal on paths that conform `List<Utf8View>` → `List<Utf8>` correctly today).
  **Not** a CAST-legality matrix — see `planning/hardening/G63-DATE-INT-DESIGN.md` §3.3.
  Ledger: [`../../../../task/wi1-insert-store-gate-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-15-wi1-insert-store-gate-ledger.md).
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
  `file_scoped_rewrite`. **MG-1 (2026-08-15):** char-boundary ON scanners (`char_indices`);
  skip-conjunct helpers (`identical_int_key_width`, `unique_schema_field`,
  `residual_bounds_predicate`) — identical Int32/Int64 only, probe failures skip, source
  column resolved case-insensitively then quoted. Ledger:
  [`../../../../task/mg1-scanprune-hardening-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-15-mg1-scanprune-hardening-ledger.md).
- `file_scoped_rewrite.rs` — filter `FileScanTask`s by affected-path allowlist
  (`repark.merge.file-scoped-rewrite`); refuses a non-empty allowlist matching zero or partial
  path set (survivor-loss guard). Test helper `dummy_task` constructs `#183` Arc innards
  (`data_file_path: Arc<str>`, `project_field_ids: Arc<[i32]>`, `deletes: Arc<[…]>`).
- `name_resolution.rs` (crate-private) — the shared case-insensitive by-name column resolver
  (Spark `spark.sql.caseSensitive=false` conform semantics); used by both `append` conform and
  merge star expansion so the two surfaces cannot drift.
- `position_delete.rs` (crate-private; two `pub` re-exports via `mod.rs`) — merge-on-read
  WRITE primitive: turn `(_file, _pos)` pairs into committable position-delete `DataFile`s by
  driving the fork's production `PositionDeleteFileWriter`. Owns sort order (ascending
  `(file_path, pos)`), `write.delete.granularity` grouping (**MW-9:** unset → Spark `file`;
  `'partition'` → one file per `(spec_id, partition)`), and partition stamping (each delete
  file carries the `(spec_id, partition)` of the data file it deletes from, resolved from the
  snapshot's DATA manifests — never the table's current default spec). Unpartitioned groups keep `partition_key = None`;
  fork #239 (`d408da42`) errors on `build(None)` with no spec, so that path chains `.unpartitioned()`.
  An evolved unpartitioned spec whose id is not 0 also chains `.with_partition_spec(spec)`
  so the fork does not fall back to stamping spec 0 (**M16**,
  [`../../../../task/m16-posdelete-specid-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-15-m16-posdelete-specid-ledger.md)).
  pins: rp-3-fork-repin/C-002
  `#182` `PartitionKey::new` is fallible (`validate_partition_data`); this module maps
  `iceberg::Error` through `iceberg_err`. Also hosts the BUG-001 P0 valve
  (`MorDmlKind` + `refuse_mor_unpartitioned_multi_spec_dml`, hoisted from the v1 SQL crate in
  phase-2 PR-3b): refuse merge-on-read SQL DELETE/UPDATE when the current default spec is
  unpartitioned and multi-spec history exists — the fork position-delete fast-path under-delete
  hazard this file's stamping discipline exists to avoid. The SQL door resolves the target and
  calls it; the door's `bug001_*` battery pins it end to end. MERGE is never gated here.
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
| Identity DELETE/UPDATE (subquery `WHERE`) | `predicate_dml.rs` (`execute_predicate_dml`) |
| Wire ordinary DELETE/UPDATE/INSERT OVERWRITE | DataFusion → fork `TableProvider` (non-subquery) |
| Ask whether a `(source, target)` type pair may be written | `store_assign.rs` (`ansi_store_assignable`) |
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
| Rejected MERGE OCC commit left new Parquet files in the warehouse | commit-error abort must `FileIO::delete` writer-result paths only (`merge/abort.rs`); never re-derive from manifests; never delete `affected` |
| MERGE OOMs on a large target | target must register as a `StreamingTable` (`(_file, _pos)` identity), never a full-target `MemTable` |
| MERGE produces duplicates | multiple-source-match must **error** (like Spark); serializable (default) commit arms carry `validate_no_conflicting_data`; snapshot isolation drops it (`write.merge.isolation-level`) |
| Conflict-retry corrupts data | on commit conflicts re-read the target; don't cache stale file lists |
| MoR MERGE on a spec-evolved unpartitioned table loud-fails `Partition value is not compatible` | position-delete writer must `.with_partition_spec` the resolved unpartitioned spec when it is not spec 0; `partition_key` stays `None` |

First checks: `cargo test -p repark-iceberg write::` (all on `MemoryCatalog`). Escalate to:
[../../map.md#debug](../../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `append.rs` — the example table literal in
  `append_a1_acceptance_identity_partitioned_end_to_end`, now `"t"` like every other
  `create_table` call in the file.
