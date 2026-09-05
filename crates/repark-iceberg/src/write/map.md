# map — repark-iceberg/src/write

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002). Clippy doc_markdown backticks added.

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
  `write.merge.mode`, fork ENGINE_CONTRACT §6). DML-A adds `WHEN NOT MATCHED BY SOURCE`.
  See [merge/map.md](merge/map.md).
- `predicate_dml.rs` — **V3-8 (2026-09-02):** the COW rewrite carries stored `_row_id` /
  `_last_updated_sequence_number` on format-v3 (scratch from `merge::row_lineage`, survivors and
  updated rows projected through `predicate_dml/lineage.rs`), so `row_lineage_guard.rs` lost its
  last caller and was deleted with it — registry `V3-COW-1` FIXED.
  **V3-9 (2026-09-02):** `resolve_write_mode`'s merge-on-read format gate went from
  `!= FormatVersion::V2` to `< FormatVersion::V2` (the shape `resolve_merge_mode` already
  used), so v3 predicate DML falls through to `commit_row_delta_kind` →
  `merge::dv_close::prepare_row_delta_deletes`, which already branches V2 parquet position
  deletes / V3 `close_touched_dv_containers`. No new deletion-vector code — registry
  `V3-MOR-1` FIXED. The `write.delete.granularity` parse stays as a validation gate on both
  versions even though a v3 deletion vector is file-scoped by construction. The per-row
  `Arc<str>` for a matched row's data-file path is reused when the path is unchanged
  (`predicate_dml/lineage.rs::push_identity_pair`), so a single-file DELETE allocates once
  rather than once per row.
  pins: v3-8-subquery-where-lineage/C-002; v3-9-mor-predicate-dml-dv/C-003, C-009
- `predicate_dml.rs` — **RP-7 (2026-09-02):** the identity scratch scan is no longer built with
  `filter: None`. `identity_scan_residual` re-parses `selection_sql` and, for a POSITIVE
  uncorrelated `IN` or a positive `EXISTS` whose correlation is one bare equality, derives the
  source key's min/max through the same `residual_bounds_predicate` MERGE uses (PERF-04) and
  pushes it onto the target scan. `NOT IN` / `NOT EXISTS` keep the unfiltered scan, and
  `repark.merge.scan-pruning=false` turns it off.
  **Safety.** Two conditions, and both are load-bearing. (1) Ownership must be EXACT:
  `predicate_dml/residual.rs` classifies each side of the correlation ONCE, the way
  `scan_prune::parse_column_ref` does, and derives NO residual when a qualifier resolves to
  neither owner or to BOTH. A target alias that shadows the subquery relation's alias or bare
  table name is rejected outright, because Spark resolves the inner name to the subquery's own
  relation: `DELETE FROM t s WHERE EXISTS (SELECT 1 FROM src s WHERE s.id = s.id)` is
  UNCORRELATED and deletes every row, and an independent per-side classification read it as a
  correlation and pruned. That is a wrong answer, not a slow one — it was measured on Spark
  4.1.2 and it is why the classification is one resolution per side.
  (2) Given exact ownership the push cannot drop a matching row: the identity scan is
  match-discovery only (the COW arm re-reads survivors through its own allowlisted scan) and a
  min/max range is a superset of the key set.
  `collect_identity_pairs` / `collect_identity_update_rows` consume `execute_stream()` and
  `reserve(batch.num_rows())` per batch instead of collecting the whole result first.
  pins: rp-7-f18-repin/C-005
- `predicate_dml.rs` — **G3-E8 A1-identity** (`execute_predicate_dml`): evaluate the original
  `WHERE` as a SELECT over the pinned `(_file, _pos)` streaming target, then commit through the
  MERGE COW/MoR write arms honoring `write.delete.mode` / `write.update.mode` / isolation —
  **never** `write.merge.mode`. Product hole is the valve allow-list (uncorrelated
  `DELETE … IN` / `NOT IN (SELECT …)`, including the NULL 3VL trap, `[NOT] EXISTS` ±
  correlation, correlated IN, identity `UPDATE … SET <scalar> WHERE col IN`, and
  **RP-9 r2:** a three-part `DELETE … WHERE <scalar comparison>` via
  `predicate_dml/plain.rs` so the production partition map reaches F-23; UPDATE,
  literal `IN`, and branch selectors stay on the fork). ANY/ALL
  stay refused (Spark 4.1.2 parse-fails quantified comparisons). Pins:
  [predicate_dml/tests/predicate_dml.rs](predicate_dml/tests/predicate_dml.rs) +
  [predicate_dml/tests/update.rs](predicate_dml/tests/update.rs) +
  [predicate_dml/tests/plain.rs](predicate_dml/tests/plain.rs)
  pins: rp-9-repin-f23/C-005
  — **LRS-5 (2026-08-20):** moved into the canonical module tree, `#[path]` gone. Isolation
  property pins (M19 / A10: no trim, `to_ascii_lowercase`, default serializable,
  garbage ⇒ Plan `Invalid isolation level: {name}`) live in those two test
  files. **MW-9:** `resolve_write_mode` parses `write.delete.granularity` on the
  MoR arm before identity UPDATE/DELETE writes parquet (same refuse-before-IO
  class as `resolve_merge_mode`). Ledger:
  [`../../../../task/r1-g3e8-pr4-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-14-r1-g3e8-pr4-ledger.md).
- `file_order.rs` — **V3-11 (2026-09-02):** `ascending_partition_order` stable-sorts one
  commit's `Vec<DataFile>` by partition value ascending (spec-field order, nulls first,
  primitive literals ascending) before the files reach the manifest, so `first_row_id`
  assignment is deterministic. Each write path sorts exactly **once**: the serial fanout entry
  `append.rs::fanout_conformed_stream_serial` sorts what its single writer closed, the
  concurrent path sorts only where the worker vectors are concatenated (sized from their
  summed lengths), and `merge/row_lineage.rs` sorts its own fanout close. Unpartitioned
  commits sort to a no-op. Cost is file-count work, not per-row work (1e6 rows / 8 partitions:
  2.810/2.850/2.875 s with, 2.973/2.943/3.010 s without). The name is the rule, **not** a Spark
  claim: Spark's own order is the Java `HashMap` bucket index of the partition struct, decoded
  in registry `V3-FILEORDER-1`, and the two coincide only on collision-free monotonic sets —
  `{0,1}`, `{0,1,2}`, `{0,1,2,3}`, `bucket(4, ·)` — not on five or more int partitions,
  strings, multi-field specs, `truncate`/`days`, or a null slot arriving after a non-null.
  Plain `INSERT INTO` on a partitioned table never reaches this module: the fork's `TaskWriter`
  owns it. **RP-8 (2026-09-03):** fork ask **F-20** landed (`#261`), so `FanoutWriter::close`
  drains ascending too and `F-v3-10-partition-file-order` is FIXED — one ordering rule now holds
  on every writer that reaches a repark table, the fork's included, and `V3-FILEORDER-1` covers
  that path as well.
  pins: v3-11-row-id-determinism/C-001, C-003, C-006, C-007
  pins: rp-8-repin-f21-f22/C-004
- `conform.rs` — batch conforming for the append write path (name resolution, WI-1 store
  assignment, strict casts), split from `append.rs` (file-size ratchet, 2026-09-01;
  append.rs baseline 1886). A missing
  column whose Iceberg field carries a `write-default` builds against the reduced schema so the
  fork's `DataFileWriter::write` fills it (**V3-6 C-005**).
  **CTAS-VIEW-1 (2026-09-03):** `conform_batch_retaining_unmapped_columns` is the unpartitioned
  stream-writer map; it calls `conform_batch` then keeps MERGE lineage extras (`_row_id`).
  Matching types skip `try_new` so CAST-NULL empty overwrite keeps source nullability.
  pins: ctas-view-1-conform-stream/C-002
- `append.rs` — `append(catalog, ident, batches)`: public bulk append — conform
  ([conform.rs](conform.rs): missing /
  extra / duplicate column = loud error, except a missing column whose Iceberg field carries a
  `write-default`: conform builds that batch against the reduced schema and the fork's
  `DataFileWriter::write` fills it — **V3-6 C-005**; **WI-1** ANSI store-assignment gate then
  strict casts, overflow never NULLs) → identity-partition fanout write → ONE stamped
  `fast_append` commit
  (append×append commutes via the fork's refresh-and-re-apply retry; empty input commits an
  empty stamped snapshot). Also `write_partitioned_data_files(_from_stream)` — the partitioned
  staged-write core. **V3-1 / RP-3 C-008:** `iceberg_err` goes through
  `catalog::iceberg_to_datafusion`; Hadoop `vN.metadata.json` writes bump to `v(N+1)`
  (registry `V3-ADOPT-1` FIXED).
- `truncate.rs` — whole-table `TRUNCATE TABLE` (DML-C): `commit_truncate` is
  `commit_overwrite_replace_all` with no added files (fork stamps `Operation::Delete`).
  `commit_truncate_to` commits onto a named branch.
  pins: dml-c-truncate/C-001, C-005
  pins: rp-5-fork-repin/C-004
- `commit_target.rs` — `maybe_to_branch` / `snapshot_id_for_commit` for named-ref commits.
  pins: rp-5-fork-repin/C-004
- `overwrite_commit.rs` — full-table overwrite commit, optional `to_branch`.
  pins: rp-5-fork-repin/C-004
- `overwrite.rs` — exclusive full-table `INSERT OVERWRITE` stage-then-swap:
  `write_overwrite_staged_files_from_stream` (positional map + **WI-1** store-assignment gate +
  stream stage) + `commit_overwrite_replace_all` + `parse_overwrite_isolation`
  (absent→snapshot | snapshot | serializable | none | invalid-loud).
- `conform.rs` — **DATE-FN-1 (2026-09-04):** the identity arm of
  `conform_batch_retaining_unmapped_columns` rebuilds the batch against the write schema so
  leaked Iceberg `PARQUET:field_id` metadata from a multi-table join cannot scramble CTAS
  columns. pins: date-fn-1-spark-date-spelling/C-002
  **V3-COV (2026-09-03):** the `SourceMatch::Unique` arm returns the source array
  unchanged when its Arrow type already equals the target field's, before the store-assignment
  check and the cast kernel. This is the bulk-append hot path and the identity case is the common
  one; the guard and the strict cast still run for every pair that actually differs.
  pins: v3-cov-statement-coverage/C-004
- `partition_write.rs` — **PERF-ICE-WRITEPATH-1 (2026-09-05):** `IcebergPartitionWriteExec`, the
  CTAS write node. One output partition per input partition, each draining exactly that partition
  through the existing serial writer; `execute_stream` coalesces the node and the coalesce spawns
  one task per partition, which is where the parquet encode and zstd of the writers stop sharing
  a task. RePark spawns nothing and gains no dependency: the parallelism is the DataFusion
  executor's, which is why this is a node and not a `tokio::spawn` (`clippy.toml` bans that, the
  rust-code-quality scan bans routing around it through `JoinSet` or a helper crate, and `tokio`
  is a dev-dependency of this crate).
  **One writer per input partition, not `min(cap, partitions)`.** A writer that drained several
  input partitions in sequence measured 738 ms against 547 ms for one-each on the partitioned 1e6
  CTAS, and worse, it is unbounded in memory: DataFusion's repartition channels are unbounded
  per output partition and only gate when EVERY channel is non-empty, so the partitions a writer
  has not reached yet buffer whole. `repark.write.max-concurrent-files` therefore selects between
  one writer over a `CoalescePartitionsExec` (cap 1, one data file) and one writer per partition
  (cap 2 or more); it still bounds the stream write paths that INSERT, MERGE, overwrite and
  predicate DML use, which this node does not touch. The knob is read from the session
  configuration, so it is a builder `.config(...)`, not a post-build `conf.set`.
  **Determinism is content-derived, because the DataFusion partition index is NOT stable.**
  Round 1 ordered the committed files by the writer index and claimed reproducibility; the round-2
  critic refuted it, and the instrumented measurement says why: over eight UNEQUAL source files,
  six identical v3 CTAS gave six different partition-index-to-source-file assignments (partition 1
  read the 3,000-row file in one run and the 40,000-row file in the next), so the writer index is
  a property of that execution, not of the statement. `stable_commit_order`
  ([file_order.rs](file_order.rs)) therefore sorts the committed files by partition value first
  (V3-11 unchanged), then by each field's lower bound in field-id order, then the upper bounds,
  then record count, file size and path — a total order that is a function of the DATA. Six runs
  of the refuting fixture now commit ONE manifest record-count sequence and ONE `first_row_id`
  map. The boundary: this makes the commit reproducible whenever the scan's row-to-file grouping
  is itself stable; if a scan split the same rows into different file groups, no writer-side
  ordering could restore it, and files that tie on every bound fall back to the path, which
  carries a fresh UUID.
  An input error raises a shared flag: siblings stop taking `Ok` batches and close what they hold,
  and the failure sweep deletes **every data file the attempt created** — the completed files
  plus every parquet that appeared under the table's data root since the attempt began, which is
  how the failing writer's own rolled files are reclaimed (round-2 S2-2: a 64 KiB target file size
  left 9 of them behind before this).
  In-module pins: every input partition gets its own writer and its own data file, holding exactly
  that partition's rows in writer-index order, and a one-task drive of the same four partitions
  answers identically; the returned files carry writer-index order over three runs; and a late
  failure in one partition leaves no parquet file in the warehouse. There is deliberately no
  wall-clock assertion in the unit suite — under `cargo test` on a loaded box the fixed cost of
  four small parquet writes swamped the injected delay (a 6.4 s floor against a 6.0 s delayed
  run), so the timing evidence lives in
  [../../../../docs/perf/iceberg-write-baseline.md](../../../../docs/perf/iceberg-write-baseline.md)
  instead, where it is measured on a release module. `make verify` is green with the pin in place.
  The vectorized partition splitter this path calls on every partitioned write is fork ask
  **F-28** on `f-28-vectorized-partition-splitter`: the splitter lexsorts the partition-value
  columns, reads group boundaries with `arrow_ord::partition` and materializes one
  `Literal::Struct` per group instead of one per row, keeping the row-wise path for Float,
  Double, Unknown and empty partition types, where Arrow total-order equality is not Iceberg
  `Struct` equality (`-0.0` and `0.0` are one group under `OrderedFloat` and two under total
  order). It is NOT consumed here: the pin bump is its own PR
  ([../../../../docs/fork-sync.md](../../../../docs/fork-sync.md)), so the fork half is measured
  through a temporary, never-committed path override.
  pins: perf-ice-writepath-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-011
- `partition_overwrite.rs` — **V3-COV (2026-09-03):** the module-private `StaticPartitionPlan`
  resolves the spec
  bindings and the `PARTITION (k=v)` map ONCE per commit and `stage_static_partition_overwrite_files`
  streams the batches through it instead of resolving per batch and collecting them all first;
  `inject_static_partition_columns` stays as the one-batch wrapper. `store_assign_source_column` runs the
  append path's `refuse_unless_write_store_assignable` and then a strict cast when a source
  column's Arrow type differs from its target field's, so a `SELECT` source producing
  DataFusion's view string representation writes instead of failing
  (`column types must match schema types, expected Utf8 but found Utf8View`); the `VALUES`
  spelling always worked, which is why DML-B never saw it. Registry `V3-COV-1` FIXED.
  Streaming moved the injection failure point: a batch whose column will not store-assign now
  refuses mid-stream, after earlier batches have already been written, where the collect-first
  shape refused before any file was staged. What that leaves behind is staged data files no
  commit ever references — the `OverwriteFiles` commit is still all-or-nothing and the table
  state is untouched either way — so the residue is orphaned files for
  `remove_orphan_files`, not a partial overwrite.
  pins: v3-cov-statement-coverage/C-004
  **DML-B:** static `PARTITION (k=v)` via
  `overwrite_files` / `overwrite_by_row_filter` + `validate_added_files_match_overwrite_filter`
  (pin `commit_rejects_added_file_outside_overwrite_filter`);
  dynamic `PARTITION (k)` / empty `PARTITION ()` via `replace_partitions`; empty-input
  dynamic guard names the three empty-dynamic surfaces (STATIC wipe, writeTo no-op, RePark
  refuse) in its rustdoc and error. `commit_*_to` variants pass `.to_branch`.
  pins: dml-b-insert-overwrite/C-001, C-002, C-004
  pins: rp-5-fork-repin/C-004 V3-COV pins in this file: a view-string source conforms to its Utf8 target instead of failing the rebuild (V3-COV-1); the identity arm hands the same buffer back while a non-assignable pair still refuses.
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
  **CTAS-VIEW-1 (2026-09-03):** `BinaryView` is a binary-width variant with `Binary`/`LargeBinary`
  (same class as `Utf8View` among string widths), so parquet-read binary columns store-assign.
  pins: ctas-view-1-conform-stream/C-002
- `alter.rs` — `ALTER TABLE` primitives on iceberg-rust public API: SET/UNSET TBLPROPERTIES
  (**V3-10:** the combined `alter_table_properties` seat moved to `format_version.rs`; the three
  atomicity tests stay here beside the `CommitFaultCatalog` harness they need and now drive
  `set_properties_and_format_version` — one action, no half-applied state),
  `rename_table`, schema evolution (`apply_schema_changes` / `SchemaChange` → fork
  `UpdateSchema`), partition-spec evolution (`apply_partition_spec_changes` /
  `PartitionSpecChange` → fork `UpdatePartitionSpec`). Return `iceberg::Result`.
- `format_version.rs` — **V3-10:** `set_properties_and_format_version` folds the fork's
  `UpgradeFormatVersionAction` and `UpdatePropertiesAction` into ONE transaction, so an ALTER
  carrying `format-version` beside another key is one metadata commit as it is on Spark; nothing
  is committed when there is neither an upgrade nor a property to write, which is why requesting
  the version a table already has writes no metadata file. It takes the table the door already
  loaded, so an upgrading ALTER loads once rather than twice, and takes `sets` by value (still
  generic over the hasher, which `clippy::implicit_hasher` requires of an exported signature)
  because both doors own theirs. `format_version_number` reads the resolver's SIGNED version off a loaded
  table and `format_version_from_number` errors rather than falling back, so an out-of-domain
  number can never be silently taken as v2 and a negative request reaches the downgrade branch
  rather than the parse branch. It is also the seat the old `alter::alter_table_properties` folded
  into (`target: None`); that function had no production caller left. Its entry points carry
  `#[allow(clippy::missing_errors_doc)]` in place of the `# Errors` doc comment the
  no-code-comments ruling forbids; every error they raise comes from the fork.
  pins: v3-10-upgrade-v2-to-v3/C-003, C-005
- `snapshot_refs.rs` — product CREATE/DROP/REPLACE BRANCH|TAG helpers over fork
  `ManageSnapshots` (+ retention setters). Write-to-branch routing lives in the Spark
  door (`repark-spark` `write_to_branch.rs`) and the `to_branch` / `with_commit_branch`
  commit seats.
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
  **RDF-1 (2026-09-02):** the Parquet properties come from
  `writer_props::position_delete_writer_properties_for`, not the plain data-file builder, so the
  `file_path` and `pos` bounds are exact and a delete file naming ONE data file is file-scoped.
  pins: rdf-1-position-delete-bounds/C-002
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
  **RDF-1 (2026-09-02):** position deletes take a second builder,
  `position_delete_writer_properties_for`, which adds the fork's own
  `position_delete_writer_properties()` truncation setting
  (`set_statistics_truncate_length(None)`) to that codec. parquet-rs truncates statistics at 64
  bytes by default; a truncated statistic is not `min_is_exact`, and the fork's metrics
  aggregator drops an inexact bound — so every RePark-written position delete reached the
  manifest with NO `file_path` bound, was never file-scoped, and was invisible to
  `tooHighDeleteRatio`. The setting is read from the fork rather than restated, so a fork
  policy change carries. Registry `RDF-1`.
  pins: rdf-1-position-delete-bounds/C-002

## I want to...

| ...do this | go to |
|---|---|
| ALTER TABLE properties / rename / schema / partition evolution | `alter.rs` |
| Bulk-append batches through the sanctioned commit path | `append.rs` (`append`) |
| Stream a SELECT into a staged (CTAS) write with bounded memory | `write_data_files_from_stream` (`merge/mod.rs`) / `write_partitioned_data_files_from_stream` (`append.rs`) |
| Stage + commit full-table INSERT OVERWRITE | `overwrite.rs` |
| Stage + commit partition-scoped INSERT OVERWRITE | `partition_overwrite.rs` |
| Cap concurrent Iceberg file writers (session conf) | `repark.write.max-concurrent-files` via `concurrency.rs` |
| Parquet compression codec (table property) | `writer_props.rs` |
| Parquet statistics properties for a position-delete file | `writer_props.rs` (`position_delete_writer_properties_for`) |
| Change MERGE INTO semantics | [merge/map.md](merge/map.md) |
| Identity DELETE/UPDATE (subquery `WHERE` and RP-9 r2 plain `WHERE`) | `predicate_dml.rs` (`execute_predicate_dml`) |
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
