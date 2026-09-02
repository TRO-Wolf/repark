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

**Fork V3 production plan closed out 2026-09-02** (fork `#250`–`#259`, fork `main` past the
engine pin `fb0cacfa`). Rows: R109 / R136 / R166 / R168 ✅; R107 / R110 / R114 / R135 / R157 🟡
with named residues; gate 8 (credentialed AWS) is owner-run; the plan's PR-7 closeout ledger
names RePark's gate 9. Row state and residues stay single-homed in the fork's `GAP_MATRIX.md`;
the engine consumes `#258`/`#259` at the next repin. Every other F-item is consumed or
ruled (F-9 by fork `#233`, row R126, a dated service gap); still open here: F-10, F-11, F-12.

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
| `catalog/metadata_projection.rs` (retired RP-5) | `iceberg-datafusion` metadata-table `TableProvider` (`scan` + `projection`), `IcebergSchemaProvider::table_names` — fork F-8 consumed; engine shim deleted |
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
- **Consumed (RP-5, 2026-09-01).** The property is exposed: `resolve_merge_isolation` reads
  `write.merge.isolation-level`; snapshot skips `validate_no_conflicting_data`. Pin
  `commit_insert_only_snapshot_isolation_commits_through_conflicting_concurrent_append`.
  pins: rp-5-fork-repin/C-007

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

*Taken by RP-2 (2026-08-27):* the CALL accepts `'remove-dangling-deletes' => true` (quoted-name
CALL grammar) and reports the fork's true count; default stays false; the Java-faithful
unpartitioned-single-spec early return measured. Pin:
`call_rewrite_data_files_remove_dangling_deletes_reports_a_true_count`.

*V3-5 (2026-08-31):* RP-4 lifted V3-LINEAGE-1. A v3 compact drops in-scope Puffin DVs
without the option and reports a true `removed_delete_files_count` (six-file fixture
= 6, Arrow Int32; V3E-3 partitioned = 2). Registry `V3-DANGLE-1` FIXED. The v2 option
half is unchanged.

- **Engine observation.** *Was (RP-2):* `removed_delete_files_count` hard-coded `0` and
  the v3 path guarded by V3-LINEAGE-1. *Now (V3-5):* the CALL forwards the fork count;
  v3 apply-path DV drop is `6` on the six-file fixture with `'remove-dangling-deletes'`
  off. On **v2**, the option-off default is still `0` (Java's).
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

### F-5 (P2) — `ReplacePartitions` remainder (R104 🟡) — answered fork #217 (2026-08-23)

- **Engine observation.** Registry **DML-1**: every `INSERT OVERWRITE … PARTITION (…)` form and
  `writeTo().overwritePartitions()` / `overwrite(condition)` refuse, because the only alternative
  is a silent whole-table replace or a sibling-partition wipe. Common in Airflow backfills.
- **Fork location.** `crates/iceberg/src/transaction/replace_partitions.rs` — dynamic overwrite
  ✅ (interop 2026-06-10/11/15); 🟡 on multi-spec conflict interop and the **static**
  `replaceByRowFilter` / explicit-partition APIs.
- **Java reference.** `BaseReplacePartitions`; Spark's `DynamicOverwrite` and
  `OverwriteByFilter` write paths; `OverwriteFiles.overwriteByRowFilter`.
- **Ask.** Close the static path (explicit partition tuple and row-filter forms) and the
  multi-spec conflict interop so the row goes ✅.
- **Answered (2026-08-29 correction).** Fork PR #217 (`798a0c8ce`) landed 2026-08-23 and is an
  ancestor of the engine pin `ce92a7bf`. The static half of the ask was void: Java routes static
  `PARTITION (k=v)` through `OverwriteFiles.overwriteByRowFilter`, not `ReplacePartitions`, and
  the fork already exposes `overwrite_by_row_filter`. **DML-B is not blocked**; the multi-spec
  interop leg is optional. Build recipe:
  [../epic-term/release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md) v0.6.
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
- **Landed (RP-4, 2026-08-31, fork #244).** `SnapshotUpdate.to_branch` exists
  (`crates/iceberg/src/transaction/to_branch.rs`). The engine does not call it yet; REF
  consumes the surface. The refuse pin stays until REF routes branch-targeted DML.
- **REF addendum (2026-09-01) — the gap moved; F-6 did not close it.** Measured at pin
  `33be9a0`, `to_branch` reached the seven snapshot-producing *transaction actions*
  (`FastAppendAction`, `MergeAppendAction`, `OverwriteFilesAction`, `ReplacePartitionsAction`,
  `RewriteFilesAction`, `RowDeltaAction`, `DeleteFilesAction`). It did **not** reach the surface
  the engine's statements execute through. `INSERT`, `UPDATE` and `DELETE` fall through the Spark
  door to DataFusion and commit inside `iceberg-datafusion`'s `IcebergTableProvider` and its
  commit exec, which build `tx.fast_append()` with no commit target — `grep -n branch
  crates/integrations/datafusion/src/table/mod.rs` at the pin returns nothing.
- **Ask, restated (F-6b).** A commit target on `IcebergTableProvider` (or on its write / delete /
  update physical plans) that is handed to the action's `to_branch`. Until then REF cannot route
  branch-targeted DML: RePark owns commit construction for `MERGE`, `INSERT OVERWRITE`, `TRUNCATE`
  and CTAS, but not for `INSERT INTO` / `UPDATE` / `DELETE`, so building only the RePark-owned
  half would make one statement family write to a branch while its sibling refuses. REF declined
  that split and kept the whole write leg refused. WAP is still **not** requested here; RePark's
  own WAP surface is DECLARED (registry REF-3).
- **Acceptance.** Engine pin
  `crates/repark-spark/src/tests/ref_ddl.rs::write_to_branch_refuses_loud_naming_fork_gap`
  is written to go red when a commit target exists on that provider; the engine then routes
  branch-targeted DML. Since REF (2026-09-01) the pin also asserts the refusal cites pin
  `33be9a0` and not the superseded `b009ac1`, so a stale reason cannot survive a repin.
- **Consumed (RP-5, 2026-09-01, F-6b `#245` + F-6c `#249`).** Write-to-branch lands. Pins in
  `crates/repark-spark/src/tests/write_to_branch.rs`. Registry REF-1 FIXED.
  pins: rp-5-fork-repin/C-004

### F-7 (A12-owned; unblocked 2026-08-23 — see the addendum below) — format v3 compaction

*Partially consumed by RP-2 (2026-08-27, fork `ce92a7bf`):* U2 measured Spark-clean — a COW
`DELETE` preserves every survivor's `_row_id`/seq and the `next_row_id` counter matches
Spark's allocate-then-suppress exactly. U1 measured RED: `RewriteDataFiles` still reassigns
every `_row_id` (0..11 → 12..23, seq → 13) — V3-LINEAGE-1 stays; the lift belongs to V3-5 on
a fork rev that carries it. U3 (`RewritePositionDeleteFiles` on v3, fork #227) is RP-3's C-007;
RP-3 also re-measures U1 at its frozen SHA (C-005).

*RP-3 at `d408da42` (2026-08-30):* U1 still reassigns (`V3-LINEAGE-1` stays). U3's v3 arm
converts parquet position deletes to DVs; on a DV-only fixture it is a zero-result no-op and
`B-MOR-3` stays.

*RP-4 at `33be9a0` (2026-08-31):* U1 / F-7 slice 1 (`#243`) carries lineage through
`rewrite_data_files`. Engine CALL + PySpark 4.1.2 + Iceberg 1.11.0 read-back is Spark-equal
(`V3-LINEAGE-1` FIXED). F-6 `#244` `to_branch` exists on the fork; no engine caller this unit
(REF consumes it).

*RP-6 at `fb0cacfa` (2026-09-01):* `#255` PR-3 MoR UPDATE keeps `_row_id` and advances seq
(F-7 preserve-half Spark-equal). Sequential COW DELETE is Spark-equal on the single-file
layout; **F-rp3-c7 is a layout artefact, not a defect** — a two-file seed, consumed.
pins: rp-6-fork-repin/C-002, C-003

*V3-7 / V3-8 (2026-09-02), engine-side, no fork ask:* the RePark-owned COW and MoR writers
carry `_row_id` through MERGE (V3-7) and through the subquery-`WHERE` COW rewrite (V3-8).
`V3-COW-1` is **FIXED** and its refusal seat deleted; F-7 has no engine-side residue left.
The MoR subquery-`WHERE` cell stays unserved on predicate DML's V2-only delete-file gate —
engine unit **V3-9**, not a fork ask.

Listed so the fork plans it; as of 2026-08-21 the engine's V3-2+ units deliberately waited
for the MW campaign to close (that wait is over — the addendum below).

- **V3-LINEAGE-1** — **FIXED 2026-08-31 (RP-4 / fork #243).** `RewriteDataFiles` carries
  `_row_id` / `_last_updated_sequence_number` through compaction Spark-equal; the public
  CALL is lifted. RP-6 lifts plain-`WHERE` COW/MoR UPDATE and sequential COW DELETE;
  V3-7 lifts MERGE and V3-8 the subquery-`WHERE` COW rewrite (registry `V3-COW-1` FIXED
  2026-09-02).
- **B-MOR-3** — `RewritePositionDeleteFiles` still refuses live Puffin deletion vectors
  (OD-2). V3-5 measured that DV compaction is `rewrite_data_files`, not this action.
- **V3-DANGLE-1** — **FIXED 2026-08-31 (V3-5).** `RewriteDataFiles` at `33be9a0` drops
  in-scope DVs; engine CALL reports `removed_delete_files_count = 6` on the six-file
  v3 MOR fixture. `where => 'part = 0'` keeps the sibling vector.

*Addendum 2026-08-23:* the owner set v1.0's north star as **full production-grade format-v3**
(engine charter: `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md`), and the MW campaign
closed the same day — the engine-side wait recorded above is over. F-7 plus **F-13/F-14/F-15**
below (added with that ruling) are the fork lane's v3 spine; their priority now tracks the
engine's V3-2+ sequencing rather than "when the owner sequences it".

*Addendum 2026-08-25 (PROC-1 unit-3 ruling):* **B-MOR-3 → extend `RewritePositionDeleteFiles`
(R136) to v3, with no DV-specific action.** Three cases:

- **DV-only tables** return **truthful zeros** — nothing to rewrite — so the engine retires its
  refusal seat rather than refuse loud.
- **v3 Parquet position deletes** rewrite into **one DV per data file**, merged with any existing
  DV for that file.
- **dangling DVs** are compaction's job, not this action's (R137 / V3-DANGLE-1, below).

**Acceptance:** the result exposes the four counts with DVs counted as delete files; the engine's
B-MOR-3 refusal pin
(`crates/repark-spark/src/tests/call_register.rs::call_rewrite_position_delete_files_refuses_spark_written_puffin_vectors`)
retires at the repin, replaced by a Spark-compared pin. **Sequenced after F-13** — the DV write
path is its prerequisite.

On **V3-DANGLE-1:** *V3-5 (2026-08-31):* RP-4 lifted V3-LINEAGE-1; the engine CALL drops
in-scope DVs and reports the true count (`removed_delete_files_count = 6` on the
six-file fixture). Registry FIXED. Open question to the fork: does apply-path DV drop
also run on the `OverwriteFiles` (COW DML) path?

### F-8 (P2) — metadata-table `TableProvider` projection and name synthesis — **consumed RP-5**

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
- **Consumed (RP-5, 2026-09-01, F-8 `#247`).** Shim deleted. Pins
  `metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door` and
  `metadata_table_projection_honor_all_types` pin the fork.
  pins: rp-5-fork-repin/C-003

### F-9 (P3) — S3 Tables `register_table`

*Ruled fork #233 (2026-08-28, row R126: dated service gap); taken by **RP-3** (C-008).*

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

*Consumed by RP-2 (2026-08-27, fork `ce92a7bf`):* measured engine-side — a plain-`WHERE`
MOR `DELETE` on a DV-free v3 table commits Puffin DVs (one per touched data file; merge and supersession are RP-3's to measure) that PySpark
4.1.2 + Iceberg 1.11.0 reads back to the same live set. On DV-carrying tables the same
statement resurrected a DV-deleted row (measured; guard stays). UPDATE / MERGE / DV-carrying
tables remain V3-3's measurement surface.

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

*Landed fork #235 (2026-08-28):* `MetadataLocation` parses Hadoop `vN.metadata.json` and bumps
to uncompressed `v(N+1).metadata.json` (Java `HadoopTableOperations` 1.10.0); Hive/REST names
unchanged; gzip suffixes parse. Taken by **RP-3** — the engine pin retargets to "the write
succeeds" and registry `V3-ADOPT-1` moves to FIXED. Residue on fork row R167: no
`version-hint.text` writer, no exists-fail rename.

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

*`write_default` filled at `DataFileWriter::write` — fork #233 (2026-08-28); carried, not
consumed, by **RP-3** (C-009). The rest of F-15 stays V3-6's substrate.*

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

*V3-6 (2026-09-01) audit addendum, measured at fork pin `33be9a0`:*

- **R91 (`unknown`) divergence — consumed RP-5 (2026-09-01, fork `#246`).**
  At `33be9a0` a Null `unknown` column committed an unreadable parquet file and the
  scan then refused. At `00cdde0` the parquet write refuses
  `Writing the unknown column 'u' is not supported yet`. Engine pin
  `fork_unknown_write_refuses_naming_the_column`.
  pins: v3-6-v3-types/C-004
  pins: rp-5-fork-repin/C-006
- **R88 (variant) — verified covered.** C-002's binary-variant finding (schema maps to the
  canonical Arrow extension type; write refuses at `ParquetWriterBuilder::build`; scan refuses
  per file task via `reject_variant_projection`) is exactly R88's recorded remainder, so no
  new handoff entry was filed for it. Engine pins:
  `fork_variant_arrow_maps_and_parquet_write_refuses` / `fork_variant_scan_refuses_naming_the_type`
  (pins: v3-6-v3-types/C-002); registry row `V3-VARIANT-SHRED-1` cites both.

### F-16 (P1, added 2026-08-24 from MW-7) — `RewriteDataFiles`: the delete-RATIO candidate clause

*RP-5 (2026-09-01, F-16r `#248`): re-measured at pin `00cdde0`. The 2,500-row pin stayed GREEN; RDF-1 stays BACKLOG. The 1e7 × 50 driver was not re-run. See C-005 in the RP-5 ledger.*

*Landed fork #232 (2026-08-27) with v3 DV removal accounting; taken by **RP-3** (C-006).
RP-3 C-006 (2026-08-30, `d408da42`): the 1e7×50 MOR driver still ends at 8 delete files /
10,000,000 delete records. The 2,500-row pin retains the gap. F-16 did not close this shape.*

- **Engine observation.** MW-7 ran 1e7 rows × 50 MERGEs through the full maintenance sequence
  and the merge-on-read table ended it holding **8 position-delete files with 10,000,000 delete
  records**, still reading at 2.0-2.5× a copy-on-write control. The mechanism is not dangling
  deletes and not `write.delete.granularity`: the surviving delete files name **live** data
  files. Those files are correctly sized, so `outsideDesiredFileSizeRange` does not select them;
  `delete_file_threshold` defaults to `usize::MAX`, so `tooManyDeletes` does not either; and the
  third Java clause is deferred here. A data file whose rows are **100 % deleted** is therefore
  never a rewrite candidate, and its dead rows are retained without bound under a runbook that
  is being followed correctly. Reproduced at 2,500 rows: a 68,523 B file in-band for a 64 KiB
  target, one MERGE deleting all of it, still live after the complete sequence.
- **Fork location.** `crates/iceberg/src/maintenance/rewrite_data_files.rs` — the deferral is
  stated at `:66-67` and `:138-140` ("the delete-RATIO candidate clause is not exposed (it needs
  per-file known-deleted-record accounting); only the delete-COUNT threshold
  (`delete_file_threshold`) is wired. The ratio clause never fires here"), and
  `DELETE_FILE_THRESHOLD_DEFAULT: usize = usize::MAX` at `:177`. Read at pin `5e7b2e4`; re-read
  at `main` before acting.
- **Java reference.** `BinPackRewriteFilePlanner`: `DELETE_RATIO_THRESHOLD_DEFAULT = 0.3` with a
  live `tooHighDeleteRatio` clause — a delete-laden file is a candidate regardless of size. On
  the live Spark 4.0.1 + Iceberg 1.10.0 oracle the same sequence ends at **zero** delete files
  and zero delete records, at both `write.delete.granularity` settings, with
  `remove-dangling-deletes` OFF (jar default `false`, javap-verified). So this is a real
  behaviour gap, not an option the engine forgot to pass.
- **Ask.** Expose `delete_ratio_threshold` on `RewriteDataFiles` with Java's `0.3` default and
  wire `tooHighDeleteRatio` into the candidate filter. It needs per-file known-deleted-record
  accounting, which is why it was deferred; the planner already has each task's attached delete
  files, so the accounting is the work.
- **Acceptance.** The engine pin that flips is
  `python/repark/tests/test_mw7_scale_smoke.py::test_delete_laden_in_band_file_survives_the_runbook`
  — a characterization pin, written to go RED when this lands. Its fixture's 100 %-dead in-band
  file must become a candidate, be rewritten, and take its delete file with it, leaving the
  table at zero delete files with the same 2,500 rows. Registry row **RDF-1** retires with it.
- **Relationship to F-3.** F-3 is the `remove-dangling-deletes` option for delete files whose
  data file is GONE. This item is the other half: delete files whose data file is still there
  and never gets selected. Landing F-3 alone does not close RDF-1, and the oracle shows why —
  Spark reaches zero with that option off.
- **Residue 2 (RP-5, 2026-09-01, after F-16r `#248`).** The ratio clause counts only
  file-scoped position deletes (`referenced_data_file` present or equal file-path bounds).
  Partition-granularity deletes and bounds-absent position deletes do not raise it. The MW-7
  2,500-row pin sets `write.delete.granularity = 'partition'`, so F-16r left that pin GREEN.
  The MW-8 partitioned runbook's in-band seeds were rewritten. Ask: count partition-scoped
  deletes in `tooHighDeleteRatio` the way Java does. Registry RDF-1 stays BACKLOG on the MW-7
  pin.
  *RP-6 (2026-09-01, pin `fb0cacfa`): residue 2 is not in this range (fork F-16r ledger
  still names partition-scoped survival). RDF-1 stays BACKLOG. pins: rp-6-fork-repin/C-004*
- **Residue 2, re-homed (RDF-1, 2026-09-02).** Half of residue 2 was never the fork's. Fork
  PR `#259` refuted the "bounds-absent" half fork-side: the fork's own writers set
  `position_delete_writer_properties()`, and at pin `fb0cacfa` a probe of the MW-7 shape
  reclaims. The bounds were absent because **RePark's** MERGE writer built its Parquet
  properties from the table codec alone and inherited parquet-rs's 64-byte statistics
  truncation, which drops the `file_path` bound. That half is fixed in RePark (registry RDF-1)
  and is no longer an ask here. **What remains of residue 2 is one line:** a delete file that
  names two or more data files has unequal `file_path` bounds by construction, so no bounds
  fix can make it file-scoped. Ask, unchanged: count those deletes in `tooHighDeleteRatio`
  the way Java does. The Acceptance bullet above is superseded — the flipped pin is
  `test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies`, and the shape that
  still waits on the fork is
  `crates/repark-spark/src/tests/call_rewrite_dangling.rs::call_rewrite_data_files_keeps_a_partition_delete_that_names_two_data_files`.

### F-17 (north-star blocker, added 2026-08-28) — shared-Puffin DV sibling closure

*Landed fork #237 (2026-08-28, same day):* `close_touched_dv_containers` /
`rewrite_siblings_for_dropped_references` extracted into core, sibling data sequences stamped
on `RowDelta`, 18 pins (sabotage mutation 11 red), Java reads the survivors. **The closure is a
call the engine's own MOR path must make** — the fork's DataFusion `delete.rs` calls it; the
engine's `plan_and_commit_mor` → `commit_row_delta` commits through `RowDelta` directly. Taken
by **RP-3** (C-003 wiring, C-004 matrix). The fork's named residue — a Spark-job-written
shared-Puffin fixture, row R114 🟡 — is exactly the engine's `v3-spark-part-dv`; RP-3 reports
the result back.

- **Engine observation.** One Spark-written Puffin contains two deletion-vector blobs. The
  `part=0` blob deletes id 2, and the `part=1` blob deletes id 5. An engine DELETE of id 1
  touches only `part=0`. The expected live set is `{3,4,6}`; the measured set is `{3,4,5,6}`.
- **Mechanism.** The DML path loads only the deletion vector for the touched data file. Commit
  removes the old delete manifest entry by Puffin path, which also removes the untouched
  sibling blob at that path. The replacement Puffin contains only the touched data file's
  merged positions. The sibling delete is therefore lost even though the transaction commits.
- **Existing fork primitive.** Fork PR #232 already solved sibling closure for maintenance in
  `crates/iceberg/src/maintenance/rewrite_data_files_dv.rs`. Reuse or generalize that closure;
  do not create a second Puffin-copy implementation for DML.
- **Ask.** When DELETE or UPDATE supersedes one blob in a shared Puffin, copy every still-live
  sibling blob into the replacement and remove all superseded entries atomically. Preserve each
  sibling's referenced data file, partition, spec id, sequence metadata, blob type, properties,
  and payload. Recompute and publish correct offsets, lengths, and file size for the replacement
  Puffin; those physical values need not match the old container.
- **Acceptance.** Build two data files in different partitions and one Puffin holding a DV for
  each. Java or Spark writes the input, fork DML touches one data file, and Java reads the exact
  survivor rows. A sabotage variant that omits sibling carry must fail. Pre-write failure must
  leave no new Puffin, manifest, or orphan object.
- **Engine pin that flips.** RP-2 keeps its broad live-DV refusal, including a second-DELETE pin
  and the shared-Puffin fixture. RP-3 retargets those pins only after one immutable fork SHA
  includes F-17, then runs the complete engine-written, Spark-written, shared-Puffin, multi-file,
  and equality-delete-plus-DV matrix.

## 4. Not fork work — do not pick these up

Listed so they are not re-proposed fork-side:

- **MOR-2** (`write.delete.granularity`): the grouping is in the engine's MOR writer
  (`repark-iceberg/src/write/position_delete.rs`); the fork's `PositionDeleteFileWriter`
  writes as given, matching Java. Engine MW-9. *(2026-08-24: MW-7's delete-retention finding
  looks adjacent but is NOT this — it reproduces at both granularities on the Spark oracle. It
  is F-16 above.)*
- Wiring `rewrite_manifests`, `remove_dangling_delete_files`, `convert_equality_delete_files`
  as `CALL` procedures — engine-side (MW-6 and "watch" items).
- `WHEN NOT MATCHED BY SOURCE`, `TRUNCATE`, partition-scoped overwrite *statements* — engine
  (`MERGE` is engine-owned by contract §6; DML-B's fork need, F-5, was answered in #217 — nothing outstanding).
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
