# Handoff — engine → owned `iceberg-rust` fork, 2026-08-23

**To:** the orchestrator of the owned fork (`TRO-Wolf/iceberg-rust`).
**From:** the RePark engine side, after the 2026-08-23 evaluations recorded in
[roadmap-intake-2026-08-23.md](roadmap-intake-2026-08-23.md) (Track B: merge-on-read
readiness at format v2; Track A: window-operator work).
**Engine pin at handoff:** `[patch.crates-io]` rev **`0c5fd58d4ab73a0113a8b28b717cf5d002b0f8f2`**
(workspace `Cargo.toml`, the SSOT). Fork `main` is **`e69f7b0a`, 18 commits past the pin** (the
pin is a genuine ancestor, so the repin math holds). *Correction 2026-08-23:* the first draft
named `6258bb0` as `main`; that commit is on an unmerged lane branch — the engine side had read
a detached checkout. **File:line references below were read at the pin**; the fork orchestrator
re-reads each at `main` before acting, and has done so for F-1, F-4 and F-8 (see those items).

**What this is.** Every item the engine side found that belongs in the fork rather than in the
engine, written so the fork orchestrator can charter units without re-deriving the evidence.
Each item names what the engine observed, where in the fork it lives at the pinned rev, the
Java 1.10.0 reference, the ask, and **the engine pin that flips when it lands** — that flip is
the acceptance, not a description. The fork's own contract
(`AGENTS.md`, `docs/ENGINE_CONTRACT.md`, `docs/parity/GAP_MATRIX.md`, `docs/testing.md` in the
fork) governs *how* the work is done; this document only says *what* and *why*.

**Retirement.** This handoff closes when each F-item below has a fork PR number or a dated
"declined / permanent gap" ruling in the fork's `GAP_MATRIX.md`. The engine side then runs one
repin unit per landed batch (§5) and archives this file.

---

## 1. Ground rules the engine side is relying on

- **Capability status lives only in the fork.** The engine's registry and docs link to
  `GAP_MATRIX.md` rows; they never restate them. Every item here should end as a row change
  there (🟡 → ✅, or a dated permanent-gap note), so the engine's pointers stay true.
- **Upstream-mergeability is optional.** Java `iceberg-core` 1.10.0 parity is the oracle
  (bytecode-verified where the fork already does that); `apache/iceberg-rust` is a source of
  cherry-picks, not a constraint.
- **Additive over breaking.** The engine reaches into the surfaces in §2. A change that
  alters one (a renamed accessor, a result type that loses a field, a `Catalog` trait method
  that newly gains a default) is legal, but it must be named in the PR so the engine's repin unit
  can plan for it rather than discover it.
- **Never vendored, never forked twice.** The fork stays a separate repo; the engine consumes
  it by rev-pin only. Nothing here asks for a DataFusion fork — DataFusion is a normal upstream
  dependency on both sides (`iceberg-datafusion` tracks the family the engine pins, 54.1.0
  today).

## 2. The surfaces the engine consumes at the pinned rev

What the engine calls, so a change to any of it is a named event. Grouped by engine module.

| Engine module | Fork surface consumed |
|---|---|
| `repark-iceberg/src/write/merge/` (MERGE, RePark-owned) | `Transaction` + `OverwriteFiles` (COW), `RowDelta` (MOR) with `validate_no_conflicting_data` / `validate_no_conflicting_data_files` / `validate_data_files_exist`; the commit-retry loop; `ErrorKind::CommitStateUnknown` + `commit.status-check.*` reconciliation; `FanoutWriter`; `PositionDeleteFileWriter` (+ `.with_partition_spec`); `PartitionKey::new` (fallible since fork #182); the `(_file, _pos)` reserved metadata columns on scan |
| `write/append.rs`, `overwrite.rs` | `fast_append`, `StagedTableTransaction::{begin_create, begin_replace}`, `FileIO` |
| `write/alter.rs`, `snapshot_refs.rs` | `UpdateSchema`, `UpdatePartitionSpec`, table-property set/unset in one action, `rename_table`, `ManageSnapshots` (branch/tag CRUD, rollback, retention setters) |
| `repark-spark/src/call.rs` (`CALL system.*`) | `maintenance::{RewriteDataFiles, RewritePositionDeleteFiles, DeleteOrphanFiles}` and their result types; `ExpireSnapshots` + `commit_and_clean` → `CleanupReport`; rollback via `ManageSnapshots`; `Catalog::register_table` |
| `catalog/provider.rs` (`NamespaceScopedCatalog`) | the whole `Catalog` trait — **14 required + 16 defaulted methods** at the last audit; 13 of 16 defaults explicitly forwarded, 3 stated omissions. **A new defaulted method is a repin duty on the engine side; name it in the PR.** |
| `catalog/metadata_projection.rs` | `iceberg-datafusion` metadata-table `TableProvider` (`scan` + `projection`), `IcebergSchemaProvider::table_names` |
| ordinary `INSERT` / `DELETE` / `UPDATE` | `iceberg-datafusion` `IcebergTableProvider` incl. `insert_into` and the DML path |
| format v3 | reading Puffin deletion vectors (R117); `register_table` of a Spark-written v3 table; the engine **refuses** v3 compaction and MOR writes on v3 by its own guard |

## 3. The queue

### F-0 (P0, fork-found 2026-08-22) — missing operation type in the two conflict guards

- **Fork observation (not in the first draft).** The fork orchestrator reports a
  silent-corruption gap: an operation type is missing from the two conflict guards, so rows
  come back after a delete that commits successfully. The engine did not see it because its
  serializable arm validates with an `AlwaysTrue` conflict filter (engine DML-5 over-rejection).
- **Ask.** Take it **before F-2** — a silent-corruption fix outranks a reporting split, and it
  is reported as cheap. Name the finding's ledger/row id in the PR so the engine can cite it.
- **Engine follow-up.** Verify whether the engine's `write.merge.isolation-level = snapshot`
  arm (which drops `validate_no_conflicting_data`) is exposed, and pin it either way; the
  DML-5 over-rejection is not a guard the engine may lean on for correctness.

Priority is the engine side's view of workload impact; the fork orchestrator sequences against
its own open campaigns. **P1** = on the critical path of a production MOR deployment; **P2** =
unblocks a chartered or proposed engine unit; **P3** = real, not urgent.

### F-1 — `RewritePositionDeleteFiles` admission gate — **DONE fork-side (2026-08-22, four PRs ending fork #213)**

> **Corrected 2026-08-23 by the fork orchestrator.** At fork `main` the position-delete planner
> carries the full three-part admission gate with the floor at five and **shares** the data-file
> planner's constants (`rewrite_position_delete_files.rs` imports `MIN_INPUT_FILES_DEFAULT`).
> No fork work remains. **Engine repin duty:** this is a *breaking default change* — the floor
> moved 2 → 5, so a caller wanting the old behaviour must say so explicitly. The engine flips
> `call_mor1_compacts_below_sparks_min_input_files_floor` to equality, retires MOR-1, and checks
> no engine test relied on two-file compaction. Original item kept for the record:

- **Engine observation.** Registry row **MOR-1**: the engine compacts any group of ≥ 2 position-
  delete files; Spark declines below `min-input-files = 5`. Measured on the live oracle
  (PySpark 4.0.1 + Iceberg 1.10.0, `write.delete.granularity = 'partition'`): 1 file → both
  zeros; 2 and 4 → Spark zeros, fork compacts; 8 → both `rewritten = 8, added = 1`. File layout
  only — contents never differ.
- **Fork location.** `crates/iceberg/src/maintenance/rewrite_position_delete_files.rs` ~L220:
  `if entries.len() < 2 { … }` is the only admission rule. The sibling
  `rewrite_data_files.rs` already ports Java's `SizeBasedFileRewritePlanner` gate
  (`MIN_INPUT_FILES_DEFAULT = 5`, `enoughInputFiles || enoughContent || tooMuchContent`,
  ~L45–113 and L169–270).
- **Java reference.** `RewritePositionDeleteFilesSparkAction` →
  `BinPackRewritePositionDeletePlanner extends SizeBasedFileRewritePlanner`; target size from
  `write.delete.target-file-size-bytes`.
- **Ask.** Give the position-delete planner the gate the data-file planner has — share it,
  don't copy it. Keep the builder options (`min_input_files`, target size) so the engine can
  expose Spark's `options` map later (engine R135).
- **Acceptance.** Engine pin
  `crates/repark-spark/src/tests/call.rs::call_mor1_compacts_below_sparks_min_input_files_floor`
  was written to go **red** when the fork matches Spark; the engine flips it to equality and
  retires MOR-1. Fork-side: the 2/4/8-file battery above as unit tests.

### F-2 (P1) — `expire_snapshots` cleanup report split by content type

- **Engine observation.** Spark's `expire_snapshots` returns six columns
  (`deleted_data_files_count`, `deleted_position_delete_files_count`,
  `deleted_equality_delete_files_count`, `deleted_manifest_files_count`,
  `deleted_manifest_lists_count`, `deleted_statistics_files_count`; all `bigint`, nullable —
  read from the 1.10.0 jar's `OUTPUT_TYPE`). The engine can only report four honestly because
  the fork funnels every content file into one vector.
- **Fork location.** `crates/iceberg/src/transaction/expire_cleanup.rs:225` `CleanupReport` —
  `deleted_content_files: Vec<String>` is documented as Java's `"data"` funnel (data files,
  position/equality delete files, DV puffins together).
- **Java reference.** `ExpireSnapshotsSparkAction` counts by `FileContent` (`DATA`,
  `POSITION_DELETES`, `EQUALITY_DELETES`); a deletion vector is a `POSITION_DELETES` entry in
  puffin format. Verify against 1.10.0 bytecode which bucket DVs land in before pinning.
- **Ask.** Classify deleted content files by their manifest entry's content (the walk already
  has the entry in hand) and expose typed vectors or counts **additively** — keep
  `deleted_content_files` so the engine's current accessor still compiles through the repin.
- **Acceptance.** Engine `execute_expire_snapshots` emits Spark's full six columns; the
  disclosure table in `crates/repark-spark/src/call.rs` retires its "four of six" note; MW-5
  registers or retires the row. Also close the nullability divergence the MW design §5 noted
  (Spark nullable, engine non-nullable) on the engine side at the same time.

### F-3 (P2) — `RewriteDataFiles`: dangling-delete removal and `removed_delete_files_count`

- **Engine observation.** The engine's `rewrite_data_files` result reports
  `removed_delete_files_count` as a constant `0` by construction, because the fork's action
  does not compose dangling-delete removal and the engine refuses the `options` map. On **v3**
  the oracle reported `6` with no option set (a DV dies with the file it is scoped to) — that
  path is engine-guarded today (V3-LINEAGE-1) and belongs to F-7.
- **Fork location.** `crates/iceberg/src/maintenance/rewrite_data_files.rs` (the action) and
  `maintenance/remove_dangling_delete_files.rs` (the sub-action already exists, ✅ R153).
- **Java reference.** `RewriteDataFilesSparkAction` option `remove-dangling-deletes`
  (`REMOVE_DANGLING_DELETES_DEFAULT = false`, javap-verified on the engine side) →
  `RemoveDanglingDeletesSparkAction`; the result's `removedDeleteFilesCount()`.
- **Ask.** Compose the existing sub-action behind the Java option on `RewriteDataFiles`, and
  expose the count on the result.
- **Acceptance.** The engine stops hard-coding `0` when it accepts the `options` map (engine
  R135 work, charters after this lands); pin on a MOR table: merge → rewrite with the option →
  `removed_delete_files_count > 0` and the delete manifest no longer lists the files.

### F-4 — `RewriteManifests` result counts — **ANSWERED (2026-08-23)**

> **The fork orchestrator's answer.** The counts exist but are not on the return value: the
> action computes created / kept / replaced / entries-processed in a private struct and returns
> the generic commit type. **They are readable today from the snapshot summary** — the commit
> writes `manifests-created`, `manifests-kept`, `manifests-replaced`, `entries-processed`
> (`transaction/rewrite_manifests.rs` ~L351–358 at `main`, pinned by a fork test). Engine MW-6
> maps `added_manifests_count ← manifests-created`, `rewritten_manifests_count ←
> manifests-replaced` from the new snapshot's summary; a typed result can follow later. Spec
> targeting: implicit (groups never mix specs) but no filter to target one — MW-6 refuses a
> `spec_id` argument loud. `use_caching`: no equivalent; documented no-op. Original ask kept:

- **Engine observation.** Engine unit **MW-6** wires `CALL system.rewrite_manifests`. Spark's
  result is `rewritten_manifests_count:int`, `added_manifests_count:int`.
- **Fork location.** `crates/iceberg/src/transaction/rewrite_manifests.rs:113`
  `RewriteManifestsAction` (R100 ✅).
- **Ask.** Confirm the action's result exposes the rewritten and added manifest sets (or
  counts); add them if it returns only the committed table. Confirm `spec_id` targeting is
  available and whether Java's `use_caching` has a meaning here (probably a documented no-op).
- **Acceptance.** A one-line answer is enough to start MW-6; the engine pins the two counts
  against the jar's `OUTPUT_TYPE`.

### F-5 (P2) — `ReplacePartitions` remainder (R104 🟡)

- **Engine observation.** Registry **DML-1**: every `INSERT OVERWRITE … PARTITION (…)` form and
  `writeTo().overwritePartitions()` / `overwrite(condition)` refuse, because the only alternative
  is a silent whole-table replace or a sibling-partition wipe. Common in Airflow backfills.
- **Fork location.** `crates/iceberg/src/transaction/replace_partitions.rs` — dynamic overwrite
  ✅ (interop 2026-06-10/11/15); 🟡 on multi-spec conflict interop and the **static**
  `replaceByRowFilter` / explicit-partition APIs.
- **Java reference.** `BaseReplacePartitions`; Spark's `DynamicOverwrite` and
  `OverwriteByFilter` write paths; `OverwriteFiles.overwriteByRowFilter`.
- **Ask.** Close the static path (explicit partition tuple and row-filter forms) and the
  multi-spec conflict interop so the row goes ✅. The engine's **DML-B** unit is blocked on this.
- **Acceptance.** Engine pins `insert_overwrite.rs::empty_insert_overwrite_partition_refuses_full_wipe`
  and `…_nonempty_refuses_whole_table_replace` flip from refuse-loud to partition-scoped
  behaviour with an oracle row; `overwritePartitions()` leaves the guide's refuse list.

### F-6 (P3) — branch commit target (REF-1)

- **Engine observation.** Registry **REF-1**: `INSERT` / `UPDATE` / `DELETE` / `MERGE` to
  `t.branch_<name>` refuse loud because every snapshot-producing action in the fork updates
  `main`; writing to `main` while the statement names a branch is a silent wrong-target write.
- **Fork location.** `crates/iceberg/src/transaction/snapshot.rs` ~L1591 and ~L1609 stamp
  `MAIN_BRANCH` unconditionally; no `to_branch` on any producer.
- **Java reference.** `SnapshotUpdate.toBranch(String)` on every snapshot-producing action;
  `SnapshotProducer.targetBranch()`; validation runs against the branch head, not `main`.
- **Ask.** A `to_branch(name)` on the snapshot producer, threaded through each action builder,
  with validation and retry resolved against the named ref. WAP (`write.wap.enabled` /
  `stage_only`) is **not** requested.
- **Acceptance.** Engine pin `crates/repark-spark/src/tests/ref_ddl.rs::write_to_branch_refuses_loud_naming_fork_gap`
  is written to go red when a commit target exists; the engine then routes branch-targeted DML.

### F-7 (A12-owned; unblocked 2026-08-23 — see the addendum below) — format v3 compaction

Listed so the fork plans it; as of 2026-08-21 the engine's V3-2+ units deliberately waited
for the MW campaign to close (that wait is over — the addendum below), and the engine refuses
these paths today.

- **V3-LINEAGE-1** — `RewriteFiles` / `RewriteDataFiles` must carry row lineage (`_row_id`,
  `_last_updated_sequence_number`) through compaction unchanged, as Spark does; the engine
  refuses v3 rewrite until it does. The same carry applies to **any action that rewrites an
  existing row** — the COW DML path (`OverwriteFiles`) included; engine registry queue
  `V3-COW-1` (2026-08-23) records that path as reachable and unmeasured engine-side.
- **B-MOR-3** — `RewritePositionDeleteFiles` refuses live Puffin deletion vectors; a DV-aware
  rewrite (or a DV-specific action) is the fork's call.
- **V3-DANGLE-1** — a v3 compaction must drop the DVs scoped to the files it rewrote (Spark
  reported `removed_delete_files_count = 6` there with no option set). Unreachable on the engine
  side while V3-LINEAGE-1 holds; whoever lifts that guard owns this.

*Addendum 2026-08-23:* the owner set v1.0's north star as **full production-grade format-v3**
(engine charter: `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md`), and the MW campaign
closed the same day — the engine-side wait recorded above is over. F-7 plus **F-13/F-14/F-15**
below (added with that ruling) are the fork lane's v3 spine; their priority now tracks the
engine's V3-2+ sequencing rather than "when the owner sequences it".

### F-8 (P2) — metadata-table `TableProvider` projection and name synthesis — **half corrected**

> **Corrected 2026-08-23.** (a) The synthesized `<base>$<type>` names **do resolve** at `main`
> (`schema.rs:157–172`: last-`$` + vocabulary parse, `a$b` included) — the engine map's
> "unresolvable" residue note is stale and retires at the repin. **But the engine's enumeration
> filter does not go with it**: it exists for *enumeration* parity (engine ADR-0006 — Spark's
> `SHOW TABLES` lists only catalog entries, never the synthesized metadata names), and `table_names`
> at `main` (`schema.rs:210–218`) still synthesizes them. The original ask below said "either
> stop synthesizing or make them resolvable — the engine does not mind which"; that was
> misworded. **The ask is: stop synthesizing in `table_names` (resolution stays).** The filter's
> removal criterion is unchanged. (b) The scan still ignores `projection` — open as written.

- **Engine observation.** Two engine-side shims exist only because of this, both re-verified at
  every repin: `catalog/metadata_projection.rs` (the fork's metadata-table `scan` ignores
  `projection`) and the enumeration filter (the fork's `IcebergSchemaProvider::table_names`
  synthesizes `<base>$<metadata-type>` for every table, none of which resolve through the fork —
  engine ADR-0006). The projection gap is **not yet filed** in `GAP_MATRIX.md`.
- **Fork location.** `crates/integrations/datafusion/src/table/metadata_table.rs:100`
  (`_projection: Option<&Vec<usize>>` unused); the schema provider's `table_names`.
- **Ask.** (a) Honor `projection` in the metadata-table scan, including the empty projection
  (`SELECT count(*)`). (b) Stop synthesizing the `$`-names in `table_names` (keep resolution;
  an opt-in listing is fine). (c) File both as matrix rows.
- **Acceptance.** Engine deletes `metadata_projection.rs`'s shim (its stated removal criterion)
  and the enumeration filter, and re-runs the two emptiness pins
  (`crates/repark-sql/tests/introspection.rs`, `crates/repark-spark/src/tests.rs`).

### F-9 (P3) — S3 Tables `register_table`

- **Engine observation.** V3-1 wired `CALL system.register_table`; Glue registers, S3 Tables
  returns `FeatureUnsupported` from the fork and the engine surfaces that loud.
- **Fork location.** `crates/catalog/s3tables/src/catalog.rs:722` `register_table` (arguments
  ignored, unsupported).
- **Ask.** Establish whether the S3 Tables API exposes a register-by-metadata-location
  operation (the Iceberg REST `register` endpoint, if the service's REST surface serves it). If
  yes, implement; if no, record the row as a permanent service gap so the engine's guide can
  say so with a citation instead of "refuses in the fork".
- **Acceptance.** Either an engine pin on a live S3 Tables register (tier-2), or a matrix row
  the engine guide links.

### F-10 (P2, Track A) — declared sort order → DataFusion output ordering

- **Engine observation.** Track A's **W-2**: the TA serving shapes pay an O(n log n) `SortExec`
  before every `OVER (PARTITION BY symbol ORDER BY ts)` because an Iceberg scan advertises no
  ordering. DataFusion's bounded window operator streams in bounded memory whenever the input
  ordering already satisfies the window. The engine's SE-1 (`sorted_view.rs`) proves the
  pattern on temp views: verify-then-advertise so `EnforceSorting` elides the sort.
- **Fork location.** `crates/integrations/datafusion/src/table/mod.rs:179` `scan` — the
  `ExecutionPlan` it builds carries no `output_ordering` / equivalence properties.
- **Java reference.** `DataFile.sortOrderId()`; `Table.sortOrder()`; Spark's
  `SparkScanBuilder` does **not** expose ordering to Spark's planner either — this is a
  DataFusion-integration feature, not a Java-parity item, so the matrix row is new.
- **Ask.** When a scan's every data file carries the table's active `sort_order_id` (and the
  partitioning makes the ordering per-partition rather than global), expose the sort order as
  the scan's output ordering, per DataFusion partition. **Trust model is verify-or-refuse:** a
  file with a different or null `sort_order_id` disables the claim for that scan; never
  advertise an ordering a file might not have. No rewrite of any file.
- **Acceptance.** Engine W-2: an `OVER (PARTITION BY symbol ORDER BY ts)` over a table written
  sorted by `(symbol, ts)` plans with no `SortExec` (EXPLAIN pin, execution-layer evidence per the
  engine's PR-D3 remainder), and the result is bit-identical to the sorted path.

### F-11 (P3) — `ExpireSnapshots` remainder (R133 🟡)

`IncrementalFileCleanup`, `cleanExpiredMetadata`, and ref-age (`max_ref_age_ms`). The engine's
`expire_snapshots` procedure today takes `older_than` / `retain_last`; it adds Spark's
`clean_expired_metadata` argument when the fork has it. No engine unit is blocked.

### F-12 (P3) — R157 credentialed real-catalog slice

The unknown-outcome reconciliation is unit-proven in the fork and the row stays 🟡 only for the
credentialed catalogs. Engine **MW-4** (merged as #218) exercises Glue commits under MOR —
compact + expire on a live catalog — once its post-merge `aws-acceptance` dispatch runs; S3
Tables has no MOR leg yet (engine MW-4b, owner-gated). That transcript is engine-side evidence
the fork can cite, but the fork's own interop for R157 is the fork's to run. Coordinate rather
than duplicate.

### F-13 (north-star spine, added 2026-08-23) — Puffin deletion-vector write path

The prerequisite for engine unit **V3-3** (merge-on-read DML on v3), the largest engine unit on
the v1.0 path.

- **Engine observation:** MOR `DELETE`/`UPDATE`/`MERGE` refuse on v3 by guard
  (`repark-iceberg/src/write/merge/mod.rs` — "V3 mandates Puffin deletion vectors, which the
  fork's `PositionDeleteFileWriter` does not produce", GAP row **R113**). v3 forbids new
  position-delete files, so the engine cannot get a v3 MOR write wrong today — it cannot attempt
  one.
- **Fork location:** re-read at `main` — the engine track design references "the fork's
  `DVFileWriter`"; verify what exists (own surface, upstream cherry-pick candidate, or absent)
  before chartering, and record the answer in `GAP_MATRIX.md` either way.
- **Java reference:** `DVFileWriter` / `BaseDVFileWriter` in `iceberg-core` 1.10.0 (Puffin
  `delete-vector-v1` blobs, file-scoped, one vector per referenced data file), and `RowDelta`
  admitting DV files.
- **The ask:** a production DV write surface the engine's MOR arm can drive the way it drives
  `PositionDeleteFileWriter` today — partition-spec stamping included — plus `RowDelta`
  admission and the v2→v3 rule that a v3 commit carries DVs, never new position-delete files.
- **Engine pin that flips:** none exists for MOR-on-v3 today — the guard-class pin in
  `repark-iceberg/src/write/merge/` tests exercises **V1**
  (`mor_on_v1_table_is_rejected_before_any_write`) and stays a refusal. V3-3 lands the first
  v3 MOR pin, and that pin is the acceptance.

### F-14 (north-star spine, added 2026-08-23) — `MetadataLocation` Hadoop pointer math

- **Engine observation:** registry **V3-ADOPT-1** — a table registered from a Hadoop-convention
  pointer (`vN.metadata.json`) reads correctly but every write fails: the fork's
  `MetadataLocation` parser requires `<version>-<uuid>.metadata.json` to compute the next
  pointer. Spark's Hadoop catalog writes `v(N+1).metadata.json` itself.
- **Fork location:** the `MetadataLocation` parser (re-read at `main`; engine evidence isolated
  the cause by renaming the identical file to a version-uuid shape, after which writes
  succeeded).
- **Java reference:** `HadoopTableOperations` version-pointer scheme in 1.10.0.
- **The ask:** either compute the next pointer for `vN.metadata.json` names as Spark does, or a
  dated permanent-gap ruling in `GAP_MATRIX.md`; the engine's refusal message already names both
  conventions.
- **Engine pin that flips:**
  `repark-spark/src/tests/call_register.rs::call_register_table_of_hadoop_named_metadata_writes_name_the_convention`
  — retargeted from "the refusal names the convention" to "the write succeeds" if the fork takes
  the first branch.

### F-15 (north-star spine, added 2026-08-23) — v3 type system and default values

The prerequisite for engine unit **V3-6** (v3 types) and the H6 VARIANT design's fork-gated
increments.

- **Engine observation:** none yet — no engine surface can reach a v3 type today, which is
  itself the point: this item exists so the fork plans the dependency order before V3-6
  charters. No engine pin yet; V3-6's charter lands the first.
- **The ask:** the v3 schema-model delta — `variant`, `geometry`, `geography`, `timestamp_ns` /
  `timestamptz_ns`, `unknown`, and column `initial-default` / `write-default` — carried through
  metadata (de)serialization and the Parquet IO mapping, feature by feature with `GAP_MATRIX.md`
  rows (a per-type "not yet" row is a fine first landing). This item closes on the PR that
  lands the schema-model delta with a `GAP_MATRIX.md` row per type — "not yet" rows
  included — which satisfies the retirement rule above.
- **Java reference:** `org.apache.iceberg.types` and the v3 spec's type/default-value sections,
  1.10.0 bytecode where the spec is ambiguous.

## 4. Not fork work — do not pick these up

Listed so they are not re-proposed fork-side:

- **MOR-2** (`write.delete.granularity`): the grouping is in the engine's MOR writer
  (`repark-iceberg/src/write/position_delete.rs`); the fork's `PositionDeleteFileWriter`
  writes as given, matching Java. Engine MW-9.
- Wiring `rewrite_manifests`, `remove_dangling_delete_files`, `convert_equality_delete_files`
  as `CALL` procedures — engine-side (MW-6 and "watch" items).
- `WHEN NOT MATCHED BY SOURCE`, `TRUNCATE`, partition-scoped overwrite *statements* — engine
  (`MERGE` is engine-owned by contract §6; DML-B needs only F-5 from the fork).
- DataFusion window-operator work (Track A W-0/W-1/W-3) — engine or upstream DataFusion, never
  the fork.

## 5. The repin protocol (what happens on the engine side when an item lands)

One engine repin unit per landed batch, never a passenger on another change:

1. Bump the `[patch.crates-io]` rev together with the family (`datafusion` + `datafusion-spark`
   + `arrow*`/`parquet` + `rust-toolchain.toml`) only if the fork moved its base; otherwise the
   rev alone. `cargo update`, `Cargo.lock` checked in.
2. Re-verify the two standing repin duties: the metadata-projection shim's removal criterion
   (F-8) and the `Catalog` trait surface re-enumeration (§2). Re-run the two metadata-table
   emptiness pins.
3. Flip the acceptance pin named in the F-item; retire or re-point the registry row; update
   the guide and the crate maps in the same change.
4. Record the take/skip decision with its date (engine `AGENTS.md` "Version-pin contract").

**What the engine side needs in each fork PR description:** the F-item it closes, the
`GAP_MATRIX.md` row it moves, and any consumed-surface change from §2 (a renamed accessor, a
changed result type, a new defaulted `Catalog` method) named explicitly.
