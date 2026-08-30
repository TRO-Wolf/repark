# Charter ledger — RP-3 · one frozen fork repin (F-17, F-14, F-7 U3, F-16, F-9, F-15, R114) + the DV input-state matrix

**Date:** 2026-08-28 · **Branch:** `feat/rp-3-fork-repin` (opens when the owner confirms this
gate) · **Base:** `main` after the RP-2 salvage PR · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
§5 (the repin protocol) · **Path:** STANDARD (code changes: the repin, the engine wiring of the
fork's DV container closure, every pin the matrix flips; one Actor cycle, one Critic pass with a
fresh execution through each door whose guard lifts).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** RP-2 ([completed ledger](../archive/2026-08/2026-08-28-rp-2-fork-repin-ledger.md)) left every
live-DV input guarded because the engine's DELETE on a Spark-written shared Puffin resurrected a
row (finding F-rp2-1: the Puffin held two DV blobs, the engine superseded the container by path
and dropped the untouched sibling). The fork fixed the invariant as **F-17** (fork #237,
2026-08-28: `close_touched_dv_containers` / `rewrite_siblings_for_dropped_references` in core,
sibling data sequences stamped on `RowDelta`, 18 pins, sabotage mutation red, Java read-back
green) and, in the same range, landed **F-14** (#235, Hadoop `vN.metadata.json` pointer math),
**F-7 U3** (#227), **F-16** + DV removal accounting (#232), **F-9** + **F-15** (#233) and
**H7-P1** with the public **R114** DV discovery API (#239). Fork `main` at
`d408da42fb91` (#240, docs) is the frozen target; a later fork landing does not widen this
unit. **The closure is opt-in for callers:** the fork's own DataFusion `delete.rs` calls
`close_touched_dv_containers(table, &new_positions)`; the engine's `plan_and_commit_mor` →
`commit_row_delta` talks to `RowDelta` directly and must make the same call, or the repin alone
leaves the resurrection reachable. Not in this unit: the MOR UPDATE / MERGE lift beyond what the
matrix measures (V3-3), lineage-preserving compaction (V3-5), the v3 types (V3-6).

## PROPOSITION LEDGER — RP-3 — 2026-08-28

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `d408da42fb91` (full SHA recorded on the repin commit) and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`. | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff main -- Cargo.toml rust-toolchain.toml` empty outside the revs. | **PROVEN** | Five `[patch.crates-io]` revs and six lock sources (`iceberg`, `iceberg-catalog-glue`, `iceberg-catalog-s3tables`, `iceberg-datafusion`, `iceberg-sketches`, `iceberg-storage-opendal`) are `d408da42fb91db2010662fe1da3783b82fa6e1ed`; zero `ce92a7bf` remain. `cargo check --locked --workspace` exit 0 (0.37s incremental, 2026-08-29). `git diff origin/main -- rust-toolchain.toml` empty. Cargo.toml vs origin/main is the five revs plus the pins citation line. Family freeze: datafusion 54.1.0, datafusion-spark 54.1.0, arrow*/parquet 58.4.0, rust-toolchain 1.96.0. Citation: `crates/repark-iceberg/map.md`. |
| C-002 | The two standing repin duties hold at the new rev (`NamespaceScopedCatalog` forwards every required `Catalog` method; the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`), and the "what changed under us" note lists every BEHAVIOR / BREAKING change in `ce92a7bf..d408da42` (#227, #230, #232, #233, #235, #237, #239) with the engine site that absorbs it. | Trait diff; fork `metadata_table.rs`; the two metadata-table pins; the note in §3. | **PROVEN** | Range `ce92a7bf..d408da42` is 12 commits (listed in §6). Charter-named **#230 is not in this range** — recorded, not invented. Later fork `main` `d4f55e1` (#241) does not widen this unit. `Catalog` trait still 14 required + 16 defaulted; no method added or removed. Three `NamespaceScopedCatalog` omissions still compose. Shim stays: `iceberg-datafusion` `table/metadata_table.rs` `scan` still takes `_projection` and ignores it. `IcebergSchemaProvider::try_new` is still lazy. `#239` public path verified: `iceberg::live_deletion_vectors_by_data_file` (`lib.rs:107`); `iceberg::spec::is_deletion_vector` (re-export, `spec/mod.rs:67`). What-changed table in §6. Citation: `crates/repark-iceberg/map.md`. |
| C-003 | **F-17 wired engine-side.** The engine's MOR DELETE / UPDATE / MERGE path calls the fork's `close_touched_dv_containers` for every touched data file's live DV and commits the replacement containers through `RowDelta` with the fork's sibling sequence stamping, so no untouched sibling blob is lost; a sabotage build that skips the call must red the shared-Puffin pin. | The wiring in `crates/repark-iceberg/src/write/merge/mod.rs` (`plan_and_commit_mor`, `commit_row_delta`); the fixture pin flipped from refuse to Spark-equal rows; the mutation run recorded here. | **PROVEN** | `dv_close.rs` + `commit_row_delta_kind` V3 arm. Pin `write::merge::dv_close::tests::shared_puffin_row_delta_keeps_the_untouched_sibling`: live ids [3,4,6], two Puffin DVs, sibling sequence unchanged, old container path not live. V3 DELETE arms `validate_deleted_files` (Java skip-delete divergence); V2 DELETE keeps Java skip. Sabotage (add only `None`-stamped, remove nothing) red: `DataInvalid => Cannot commit deletion vector ... already carries a live deletion vector ... pass the superseded file to RowDelta::remove_deletes_many`. Restored; pin green. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-004 | **The DV input-state matrix, per door.** Every reachable cell runs through both SQL doors and the facade, values and Arrow types asserted through `collect` / `to_arrow`: (1) DV-free first MOR DELETE — Puffin DV committed, Spark reads identical rows; (2) engine-written DV then a second MOR DELETE — positions merged, old DV superseded, exactly one live DV; (3) Spark-written DV then MOR DELETE on the same data file — same result as (2); (4) shared Puffin, touch one of several blobs — untouched siblings stay effective (`v3-spark-part-dv`: `DELETE id = 1` → `{3,4,6}`); (5) one DELETE touching several files and partitions — one correct DV per data file, spec and partition correct; (6) equality delete + DV (`v3-spark-eq-dv`) — neither class lost; (7) DV-free COW sequential DELETE statements — rows and lineage Spark-equal; (8) an unsafe state — loud pre-write refusal, bytes and rows unchanged. The `V3-COW-1` live-DV refusal lifts only for cells green on all three doors; a red cell stays refused and is filed as a fork or engine finding. | One pin per cell per door; the PySpark 4.1.2 + Iceberg 1.11.0 read-back of every engine commit; `count_live_deletion_vectors` either replaced by the R114 public `live_deletion_vectors_by_data_file` or kept with a stated reason. | OPEN | The Spark-job-written shared-Puffin fixture is the fork's named F-17 residue (GAP row R114 🟡): report cell (4)'s result back to the fork. |
| C-005 | **F-7 U1 re-measured at the frozen SHA.** `CALL system.rewrite_data_files` on the v3 fixture either carries `_row_id` / `_last_updated_sequence_number` through compaction Spark-equal (guard lifts, `V3-LINEAGE-1` → FIXED, dated) or still reassigns (guard stays; the measured divergence filed against the fork row it waits on before V3-5 charters). A green fork row R166 is not evidence. | RP-2's §3 C-004 driver re-run; Spark read-back of both state copies. | **PROVEN** | §11: the direct d408 action rewrites 12 files into one. PySpark 4.1.2 + Iceberg 1.11.0 reads equal values and Arrow types but reassigned `_row_id` and sequence 13 for every retained row. `V3-LINEAGE-1` stays. |
| C-006 | **F-16 measured** (transferred from #254 C-009). MW-7's 1e7×50 MERGE-then-maintain sequence on a merge-on-read table ends at zero delete files and zero delete records with the default `delete-ratio-threshold` (0.3); the MW-7 pin flips from "documents the gap" to "asserts zero"; the maintenance runbook drops its residual-delete caveat. | The 2,500-row reproduction first, the 1e7 run once; the pin; the runbook diff. | OPEN | — |
| C-007 | **F-7 U3 measured** (from #254 C-011). `CALL system.rewrite_position_delete_files` on the adopted v3 fixture no longer refuses (`B-MOR-3`): the fork's v3 DV arm runs, the Spark read-back is unchanged before and after, and a second run converges — or it stays refused with the measured reason recorded against fork row R136. | Both doors + facade on the V3E-3 fixture; rows + `sum(id)`; `.delete_files` before / after / after-again. | OPEN | R136's v3 arm is ENGINE-FIRST (no Java oracle); Spark read identity is the measurement. |
| C-008 | **F-9 taken, F-14 measured** (from #254 C-010 and the F-14 landing). S3 Tables `register_table` refuses naming the dated service gap (fork row R126, #233) and the guide / registry cite it; a table registered from a Hadoop `vN.metadata.json` pointer takes a write and the next pointer is `v(N+1).metadata.json` (fork #235) — `call_register_table_of_hadoop_named_metadata_writes_name_the_convention` retargets from "the refusal names the convention" to "the write succeeds" and registry `V3-ADOPT-1` moves to FIXED, dated. | Grep the guide and registry; the retargeted pins; the Hadoop-pointer write on both doors. | **PROVEN** | Spark `call_register_table_of_hadoop_named_metadata_writes_name_the_convention` INSERT commits `v2.metadata.json`; ANSI `ansi_hadoop_named_metadata_write_bumps_to_the_next_hadoop_pointer` same. S3 Tables CALL pin `call_register_table_on_s3_tables_names_the_dated_service_gap` cites R126. Registry `V3-ADOPT-1` FIXED; `S3T-1` admitted. Guide cites R126. Hadoop error rewrite removed. Citation: `crates/repark-spark/src/tests/call_register.rs`, `crates/repark-sql/src/v3_cow.rs`. |
| C-009 | **F-15 carried, not consumed** (from #254 C-012). The repin compiles and every gate passes with the fork's `write_default` fill in `DataFileWriter::write`; no engine surface sets a `write_default`, so the append fixtures are byte-flat before / after, and V3-6's charter gains the note that the fork surface exists. | Fixture byte comparison; the V3-6 note. | OPEN | — |
| C-010 | The documents say what the pins prove: north star §3 rows (MOR DML, COW DML, `rewrite_data_files`, `rewrite_position_delete_files`, adoption), STATUS, the slate, the handoff (F-7 U3 / F-9 / F-14 / F-15 / F-16 / F-17 marked with fork PR and date; take / skip per "Version-pin contract"), `docs/fork-sync.md`, crate maps and the divergence registry in lockstep; V3-3 chartered from C-004's red cells. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar`, the plan-pin test. | OPEN | Closes on the departure commit. |
| C-011 | Green on the whole surface: `make preflight`, the parity suite (`python/repark-parity/tests`), and the V3E-3 / V3E-4 / V3E-5 fixture pins pass at the new rev. | Gate output attached. | OPEN | Closes at readiness. |

VERDICT: OPEN — 11 clauses, 5 PROVEN (C-001, C-002, C-003, C-005, C-008), 0 REJECTED. The gate
passes when every row is PROVEN with its pin (`pins: rp-3-fork-repin/C-NNN`) and the owner
confirms.

## 2. Sequence

1. Pickup ritual (`make ledger-archive`, drift checks), then the repin commit (C-001) alone —
   the compile is the first measurement; the what-changed note (C-002).
2. Wire the closure (C-003) before any guard moves; then the matrix (C-004) cell by cell in a
   scratch build, each cell's Spark read-back into this ledger; lift only the cells green on all
   three doors.
3. Re-measure U1 (C-005); F-16 (C-006); U3 (C-007); F-9 / F-14 (C-008); F-15 (C-009).
4. Truth-up (C-010), gates (C-011), Critic pass with a novel input per door whose guard lifted,
   departure commit; report cell (4) to the fork (row R114).

## 3. Pickup — what the next agent needs to know

- Read the [RP-2 completed ledger](../archive/2026-08/2026-08-28-rp-2-fork-repin-ledger.md) first: finding
  F-rp2-1 is the defect the guard holds, with its mechanism and the fixture path.
- The fork's F-17 evidence: fork PR #237 — `crates/iceberg/src/delete_vector_container.rs`,
  `crates/iceberg/src/transaction/row_delta.rs`, and
  `crates/integrations/datafusion/src/physical_plan/delete.rs` (the call site to mirror); the
  18-case `shared_puffin_dv` suite; `dev/java-interop/run-interop-dv-sql.sh`.
- Engine seats: `crates/repark-iceberg/src/write/row_lineage_guard.rs` (`refuse_v3_cow_dml`,
  `count_live_deletion_vectors`), `crates/repark-iceberg/src/write/merge/mod.rs`
  (`plan_and_commit_mor`, `commit_row_delta`), the fixtures under
  `crates/repark-spark/src/tests/fixtures/v3-spark-part-dv/` and `v3-spark-eq-dv/`.
- Pins that flip: `ansi_cow_delete_on_a_dv_carrying_v3_table_refuses`,
  `adopted_v3_mor_second_delete_refuses_while_a_deletion_vector_is_live` (both doors), the
  facade MOR pair in `python/repark/tests/test_v3_cow_dml.py`, and the nightly
  `test_v3_live_oracle.py` `partdv` DELETE expectation (`V3-COW-1` → Spark-equal rows).
- Oracle: PySpark 4.1.2 + Iceberg 1.11.0 on this box; measure first, every assertion, the
  incidental controls included.

## 4. Session handoff — 2026-08-29 (reboot breakpoint)

Owner asked for a stop so the machine can reboot. Nothing past C-001 is committed.
Resume on `feat/rp-3-fork-repin` at `4b68684` (pickup: CC-2 archived). Disk 529 G free.

**Uncommitted (keep these files):** `Cargo.toml` + `Cargo.lock` — all five `iceberg*`
`[patch.crates-io]` revs and the lock sources are `d408da42fb91db2010662fe1da3783b82fa6e1ed`.
Family freeze holds: `datafusion` 54.1.0, `arrow*`/`parquet` 58.4, `rust-toolchain.toml`
1.96.0 unchanged vs `origin/main`. `cargo check --locked --workspace` exited 0 earlier this
session (~23.6s). Root `Cargo.toml` already cites `pins: rp-3-fork-repin/C-001, C-002`; the
grammar roots are `crates/` / `python/` / `scripts/`, so that comment does **not** pin — add
the citation under `crates/repark-iceberg/map.md` (or `src/catalog/map.md`) before flipping
C-001/C-002 to PROVEN.

**SEPMO:** STANDARD one-PR unit. Pickup is done. PRE_EXECUTION_REVIEW, the proportionality
rubric, and the first Actor SLR are **not yet filed**. Do that in the ledger before the
C-001 commit. Do not start Critic, `make preflight`, or departure.

**C-002 measurements already taken (do not re-walk the fork):**

- Range `ce92a7bf..d408da42` is **12 commits, ahead_by 12**: #227 F-7 U3, #228 docs, #231
  docs, #232 F-16, #233 F-9/F-15, #235 F-14, #234 docs, #236 docs, #237 F-17, #238 docs,
  #239 H7-P1/R114, #240 SEPMO move. Charter C-002 named **#230 — it is not in this range**;
  record that, do not invent it. Later fork `main` `d4f55e1` (#241) must not widen the unit.
- `Catalog` trait: still **14 required + 16 defaulted**; no method added or removed. The
  three stated omissions on `NamespaceScopedCatalog` still compose. Shim stays:
  `iceberg-datafusion` `table/metadata_table.rs` `scan` still takes `_projection` and
  ignores it. `IcebergSchemaProvider::try_new` is still lazy (`schema.rs`).
- F-17 public seam at the pin: `close_touched_dv_containers(table, &HashMap<String, Vec<u64>>)`
  → `DvContainerClose { added: Vec<(DataFile, Option<i64>)>, removed }`;
  `RowDeltaAction::add_delete_file_with_sequence_number`;
  `apply_dv_container_close` in fork `delete.rs` (~L354–368); V3 DELETE arms
  `validate_deleted_files` (~L511) — named Java skip-delete divergence. Engine
  `commit_row_delta_kind` still uses `.add_deletes` and arms `validate_deleted_files` only
  for `RowDeltaKind::Merge`. Engine `write_position_deletes` is still Parquet-only; V3
  forbids new parquet position deletes. MERGE v3 MoR and identity MoR still refuse non-V2
  (`resolve_merge_mode` L410, `resolve_write_mode` L823). Do **not** lift MERGE v3 (V3-3);
  wire the shared `commit_row_delta_kind` and lift only what C-004 measures.
- File-size: `merge/mod.rs` is an exact 2565-line exception — **do not grow it**. Put the
  DV close helper in a new module under the 1000-line default (new file comment ceiling is
  zero). `position_delete.rs` is an exact 1033-line exception.

**Resume order:** (1) file PER + STANDARD rubric + SLR-C-001 in this ledger; (2) crate-map
`pins:` + the §3 what-changed table for the 12 commits; (3) commit C-001+C-002; (4) C-003
wire `close_touched_dv_containers` **before any `V3-COW-1` lift**, with a sabotage pin that
skipping the call reds the shared-Puffin resurrection; C-003 decision: arm
`validate_deleted_files` on V3 DELETE like the fork, record it; (5) C-004 cells 1–4 on all
three doors. No preflight on an incomplete matrix.

## 5. Process record — 2026-08-29 (PER + first Actor SLR)

Handoff `~/.claude/plans/rp-3-fork-repin-handoff.md` (2026-08-29 night) is the executable
plan for this unit. It does not amend the frozen charter. One finding from the tree
changes how C-003 is pinned, not what C-003 requires:

- Plain `WHERE` DELETE on a v3 table already goes through the fork's DataFusion
  `TableProvider` (`delete.rs`), which already calls `close_touched_dv_containers` at
  `d408da42`. Cells 1–8 of the matrix exercise that path. The engine change on that path
  is the guard (C-004).
- Engine `commit_row_delta_kind` is not reachable from any door on v3 in this unit
  (`resolve_merge_mode` / `resolve_write_mode` still refuse non-V2; UPDATE stays
  `MorDmlKind::Update`). C-003 still wires that seam so V3-3 inherits a DV-correct
  commit rather than a Parquet position-delete one. Its pin is a crate-internal Rust
  test that calls the seam on the shared-Puffin fixture, plus a recorded sabotage
  mutation. Do not hunt for a door-level C-003 pin.
- Do not lift MERGE or UPDATE on v3. That is V3-3.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-rp-3-fork-repin
  pr_unit: rp-3-fork-repin
  criteria:
    blast_radius: FAIL (repark-iceberg seam + guard, repark-spark CALL path, three door test trees, registry)
    reversibility: PASS (one revert commit; no migration; fixtures untouched)
    size: FAIL (> 150 lines, > 5 files)
    novelty: PASS (no new dependency; the closure is fork API already in the pin)
    sensitivity: FAIL (data-integrity path: deletion vectors, resurrection defect)
    clarity: PASS (charter frozen 2026-08-28; 11 clauses, 0 REJECTED; C-001/C-002 proven 2026-08-29)
  path: STANDARD
  recorded_by: Orchestrator
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-RP3-PLAN
  agent: Orchestrator
  action: execute PR-carved charter rp-3-fork-repin (one PR, WP-0..WP-6)
  charter_trace: C-001..C-011
  preconditions:
    - pickup ritual done: SATISFIED (branch = origin/main 5815481 + 4b68684)
    - repin compiles: SATISFIED (cargo check --locked --workspace exit 0 at d408da42, 2026-08-29)
    - fork range measured: SATISFIED (12 commits, #230 absent, trait 14+16, shim stays, §6)
    - oracle JVM: SATISFIED (/usr/lib/jvm/zulu-17-amd64 present; default java is 11 and is not used)
    - oracle provision step: SATISFIED (handoff §6 names uv sync --extra record before the first Spark read-back; PySpark is not in .venv at this review)
  success_condition: every clause PROVEN with a cited pin, make preflight + make py-test exit 0, matrix cells recorded with Spark read-back, PR open
  step_risks:
    - shared-Puffin sibling lost in the engine seam: HANDLED(C-003 mutation run recorded)
    - a cell red on one door: HANDLED(guard narrows; hand back per handoff §10)
    - merge/mod.rs grows past 2565: HANDLED(new module; size-neutral arm)
  contingencies:
    - matrix incomplete at 12 h: EXECUTABLE(additive — ledger session handoff, no PR)
    - C-007 red: EXECUTABLE(additive — guard stays, R136 filed)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-rp-3-fork-repin
  slr: SLR-RP3-PLAN
  plan_checklist:
    charter_frozen: SATISFIED (owner opened feat/rp-3-fork-repin; charter dated 2026-08-28)
    carving_clause_complete:
      forward:  SATISFIED (C-001..C-011 → one PR unit)
      backward: SATISFIED (the unit traces to all eleven)
    rubric_recorded: SATISFIED (1/1 STANDARD)
    bindings_resolved: SATISFIED (.agents/skills/sepmo/binding-manifest.md; green = make preflight + make py-test)
    contingencies_executable: SATISFIED (both additive)
  verdict: PROCEED
  gap_route: "—"
  gap_detail: "—"
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-001
  agent: actor
  action: commit the repin (C-001) and the what-changed note (C-002)
  charter_trace: C-001, C-002
  preconditions:
    - pin in tree: SATISFIED (five iceberg* revs + six lock sources = d408da42fb91db2010662fe1da3783b82fa6e1ed)
    - family freeze: SATISFIED (datafusion 54.1.0, arrow/parquet 58.4.0, rust-toolchain.toml identical to origin/main)
    - compile: SATISFIED (cargo check --locked --workspace exit 0)
    - crate-map citation: SATISFIED (crates/repark-iceberg/map.md pins: rp-3-fork-repin/C-001, C-002)
    - what-changed table: SATISFIED (ledger §6)
  success_condition: cargo check --locked exit 0 and the grammar gate resolves pins: rp-3-fork-repin/C-001, C-002 from crates/repark-iceberg/map.md
  step_risks:
    - grammar does not count root Cargo.toml: HANDLED(citation lives in crates/repark-iceberg/map.md)
    - #230 invented: HANDLED(range listed; #230 recorded as absent)
  contingencies:
    - grammar red: EXECUTABLE(additive — fix the citation, recommit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 6. What changed under us (C-002)

Range `ce92a7bfe2c1be569ed0de1178ed410e8ec3a117..d408da42fb91db2010662fe1da3783b82fa6e1ed`
is **12 commits**. Compare:
`https://github.com/TRO-Wolf/iceberg-rust/compare/ce92a7bfe2c1be569ed0de1178ed410e8ec3a117...d408da42fb91db2010662fe1da3783b82fa6e1ed`

Charter C-002 named **#230. It is not in this range.** Recorded; not invented. Later fork
`main` `d4f55e1` (#241) does not widen this unit.

| Fork PR | Change | Engine site that absorbs it |
|---|---|---|
| #227 F-7 U3 | `RewritePositionDeleteFiles` v3 DV arm (ENGINE-FIRST) | engine B-MOR-3 guard in `crates/repark-spark/src/call.rs` — C-007 |
| #232 F-16 | `RewriteDataFiles` delete-ratio clause + V3 DV removal accounting | `call.rs::execute_rewrite_data_files` / `call_args.rs` option grammar — C-006 |
| #233 F-9 / F-15 | S3 Tables `register_table` refuses naming R126; `DataFileWriter::write` fills `write_default` | docs + registry citation (C-008); no engine caller sets `write_default` (C-009) |
| #235 F-14 | `MetadataLocation` Hadoop `vN.metadata.json` → `v(N+1)` pointer math | `crates/repark-spark/src/tests/call_register.rs` pin retargets (C-008) |
| #237 F-17 | `close_touched_dv_containers`, `DvContainerClose`, `RowDelta::add_delete_file_with_sequence_number` / `remove_deletes_many`; fork DataFusion DELETE + UPDATE arms call it | guard lift (C-004); engine seam wiring (C-003) |
| #239 H7-P1 / R114 | `with_file_prune_only` on scan builders; `iceberg::live_deletion_vectors_by_data_file` (`lib.rs:107`); `iceberg::spec::is_deletion_vector` (`spec/mod.rs:67`); **BREAKING:** `DataFileWriter` / `PositionDeleteFileWriter` `build(None)` with no spec now errors — callers must `unpartitioned()` or `with_partition_spec` | engine `position_delete.rs::write_position_deletes_for_partition` chains `.unpartitioned()` on the spec-0 path (C-002 addendum). Guard's `count_live_deletion_vectors` replacement decision (C-004); no prune call site required |
| #228, #231, #234, #236, #238, #240 | docs / SEPMO move | none |

## 7. WP-1 baseline at `d408da42` (2026-08-29)

`cargo test -p repark-iceberg --lib -- write::` — **261 passed, 35 failed**, all one
assertion: `DataInvalid => writer was built with neither a PartitionSpec nor a PartitionKey`.
Fork #239 (`resolve_partition_spec_id`): `build(None)` with no spec is now an error.
The engine's spec-0 unpartitioned position-delete path still passed `None`/`None`.
Absorbed in `position_delete.rs` (size-neutral; C-002 addendum). Not a C-003 patch.

`cargo test -p repark-spark -- tests::v3_cow tests::v3e3 tests::v3e4 tests::call_v3 tests::call_register`
— 40 passed, **1 failed**: `call_register_table_of_hadoop_named_metadata_writes_name_the_convention`
still expects `expire_snapshots` to refuse a Hadoop `vN.metadata.json` pointer. That is
C-008's F-14 pin, expected red until the retarget. Not a halt.

`cargo test -p repark-sql -- v3_cow` — 14 passed, 0 failed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-002-ADDENDUM
  agent: actor
  action: absorb fork #239 PositionDeleteFileWriter unpartitioned() requirement
  charter_trace: C-002
  preconditions:
    - WP-1 reds diagnosed: SATISFIED (35 tests, one DataInvalid; git log -S names #239)
    - F-14 spark pin is C-008: SATISFIED (expected red; not this commit)
  success_condition: the 35 write:: tests pass; position_delete.rs stays 1033 lines
  step_risks:
    - mixed into C-003 F-17 wiring: HANDLED(separate commit before any DV close)
    - file-size exception grows: HANDLED(comment shortened by 2; builder match adds 2)
  contingencies:
    - a second writer site still red: EXECUTABLE(additive — find DataFileWriterBuilder::new without spec/key)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-003
  agent: actor
  action: wire close_touched_dv_containers into commit_row_delta_kind and pin the shared-Puffin seam
  charter_trace: C-003
  preconditions:
    - C-001/C-002 committed: SATISFIED (6667905, 087d778)
    - WP-1 writer API absorbed: SATISFIED (#239 unpartitioned(); V2 occ pin still green)
    - merge/mod.rs size: SATISFIED (2565 after size-neutral arm)
  success_condition: shared_puffin_row_delta_keeps_the_untouched_sibling green; sabotage red recorded; V2 occ pin still green
  step_risks:
    - merge/mod.rs grows: HANDLED(new module; exact 2565 held)
    - door-level pin hunt: HANDLED(crate-internal seam pin)
  contingencies:
    - sabotage does not red: EXECUTABLE(additive — do not ship)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-004-CELLS-1-4
  agent: actor
  action: lift the live-DV DELETE refusal and pin cells 1-4 on Spark and ANSI doors
  charter_trace: C-004
  preconditions:
    - C-003 seam green: SATISFIED (81c34a8)
    - cell 2/4 watched red: SATISFIED (Spark second-delete expect_err; ANSI refuse panics)
  success_condition: Spark and ANSI cells 1-4 green; UPDATE/MERGE still refuse
  step_risks:
    - facade unmeasured: HANDLED(C-004 stays OPEN until the facade pins run)
    - cells 5-8 unmeasured: HANDLED(C-004 stays OPEN)
  contingencies:
    - a cell red on one door: EXECUTABLE(additive — narrow the guard; hand back)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 8. C-004 cells 1–4 (Rust doors, 2026-08-29)

Watched red at the pre-lift guard, then green after the lift:

- Cell 1 Spark `adopted_v3_mor_delete_commits_a_puffin_deletion_vector` stayed green.
- Cell 2 Spark refuse pin red (`expect_err` got a DataFrame). Flipped to
  `adopted_v3_mor_second_delete_merges_into_the_live_deletion_vector` — rows `{1}`, one Puffin.
- Cell 2 ANSI twin red (`must fail`). Flipped to `…second_merges`.
- Cell 3 Spark `partitioned_v3_dv_delete_id_3_merges_into_the_touched_file` green:
  live `{1,4,6}`, two DVs, record_count sum 3.
- Cell 3 ANSI `ansi_mor_delete_on_a_spark_written_dv_merges_into_that_file` green.
- Cell 4 ANSI refuse pin red (`DELETE must refuse`). Flipped to
  `ansi_mor_delete_on_a_shared_puffin_keeps_the_untouched_sibling` — live `{3,4,6}`.
- Cell 4 Spark `partitioned_v3_dv_delete_id_1_keeps_the_untouched_sibling` green.

UPDATE/MERGE refusal pins still green. Facade cell-2 pin flipped in tree; not yet
executed (needs maturin). Cells 5–8 not yet measured. C-004 remains OPEN.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-004-REMAINDER
  agent: actor
  action: execute the facade matrix cells 1-4 and measure or pin C-004 cells 5-8 through every reachable door
  charter_trace: C-004
  preconditions:
    - branch head: SATISFIED (a20259f; C-001..C-003 committed and C-004 Rust cells 1-4 green)
    - scope ruling: SATISFIED (v3 WHERE DELETE uses fork DataFusion; UPDATE and MERGE remain V3-3)
    - facade build path: SATISFIED (make py-test-facade is the bound local facade gate)
    - disk headroom: SATISFIED (629G free, 2026-08-29)
  success_condition: each C-004 cell is pinned through every reachable door with Arrow values and types, or a measured unsafe state remains loudly refused before a write
  step_risks:
    - an unmeasured cell requires a V3-3 write path: HANDLED(stop and report; do not lift UPDATE or MERGE)
    - a facade failure masks a Rust-door pass: HANDLED(run the bound facade suite before changing its pins)
    - a live-DV state loses a delete class or sibling: HANDLED(assert rows, delete-file metadata, and snapshot state)
  contingencies:
    - a matrix cell fails at the fork DataFusion door: EXECUTABLE(additive — preserve or narrow the guard and record exact evidence)
    - R114 cannot replace the helper: EXECUTABLE(additive — record the public-API incompatibility and retain the helper only with proof)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 9. C-004 remainder evidence (2026-08-30)

The `count_live_deletion_vectors` seam now delegates to fork R114 public API
`iceberg::live_deletion_vectors_by_data_file`; no local manifest walker remains.

- Cells 1–4 facade evidence:
  `env UV_CACHE_DIR=/tmp/rp3-uv-cache PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=.venv .venv/bin/python -m pytest python/repark/tests/test_v3_cow_dml.py python/repark/tests/test_v3e3_fixtures.py python/repark/tests/test_v3_live_oracle.py -q`
  → `11 passed, 3 skipped in 2.32s`. The skipped rows are the explicit JVM-live oracle tier;
  the local facade matrix uses `to_arrow` and asserts values and Arrow types.
- Cells 5–6 Spark door:
  `cargo test -p repark-spark --lib -- tests::v3e3::` → `10 passed`; cross-partition DELETE
  retains one R114-discovered DV per data file with spec 0 and partitions 0/1, and equality-delete
  plus DV retains both delete classes.
- Cells 5–6 ANSI door:
  `cargo test -p repark-sql --lib -- v3_partitioned_equality_deletes` → `6 passed`; the facade
  rows above exercise the same fixture states through `to_arrow`.
- Cell 7 RePark door result: Spark-door
  `tests::v3_cow::adopted_v3_cow_sequential_deletes_keep_the_second_snapshot_lineage`, ANSI-door
  `v3_cow::ansi_adopted_v3_cow_sequential_deletes_keep_the_second_snapshot_lineage`, and facade
  `test_facade_adopted_v3_cow_sequential_deletes_keep_second_snapshot_lineage` each assert typed
  live rows `{(1, "a")}` and `(next_row_id, first_row_id, added_rows) = (6, 5, 1)` after
  DELETE id=2 then DELETE id=3. The two Rust modules passed in their focused commands below.
- Cell 8: Spark-door
  `tests::v3e4::update_and_merge_still_refuse_on_the_appended_v3_table`, ANSI-door
  `v3_partitioned_equality_deletes::ansi_partitioned_dv_update_refuses_before_writing`, and facade
  `test_partitioned_dv_update_and_rewrite_refuse_pre_write` assert `V3-COW-1` for live-DV UPDATE,
  typed rows unchanged, and fixture bytes unchanged; the Rust doors also assert unchanged snapshot.
  `rewrite_position_delete_files` stays separately refused, so C-007 can re-measure it later.

Focused command results:

- `cargo test -p repark-spark --lib -- tests::v3_cow::` → `12 passed`.
- `cargo test -p repark-spark --lib -- tests::v3e3::` → `10 passed`.
- `cargo test -p repark-sql --lib -- v3_cow::` → `11 passed`.
- `cargo test -p repark-sql --lib -- v3_partitioned_equality_deletes` → `6 passed`.
- `cargo fmt --check`, `git diff --check`, and
  `.venv/bin/ruff check python/repark/tests/test_v3_cow_dml.py python/repark/tests/test_v3_live_oracle.py`
  → green.

The direct PySpark 4.1.2 + Iceberg 1.11.0 C7 transcript remains pending the parent-owned local
loopback run. The Spark command is supplied to the parent verbatim. The failed sandbox attempts
(read-only Ivy cache, then loopback socket denial) are environment attempts, not behavior evidence.
**C-004 remains OPEN until that read-back returns.**

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-004-REMAINDER-CONCLUSION
  agent: actor
  action: assess the C-004 remainder after targeted three-door pins and validation
  charter_trace: C-004
  preconditions:
    - targeted Rust and facade pins: SATISFIED
    - UPDATE and MERGE scope ruling: SATISFIED (still refused)
    - C7 Spark read-back: PENDING (parent-owned local loopback execution)
  success_condition: all eight cells have three-door pins and the C7 Spark lineage transcript agrees
  tripwire_scan: CLEAN
  uncertainty: C7 Spark transcript is not yet received
  verdict: HOLD
  escalation: parent owns the one local loopback command
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-004-C7-RED
  agent: actor
  action: refuse a second v3 copy-on-write DELETE after an overwrite snapshot before the fork can reassign lineage
  charter_trace: C-004
  preconditions:
    - PySpark 4.1.2 transcript: SATISFIED (second DELETE leaves next-row-id 5 and writes zero added rows)
    - RePark three-door result: SATISFIED (second DELETE advances next-row-id 5 to 6 and writes one added row)
    - owner ruling: SATISFIED (fix only if local and narrow; otherwise pre-write refuse and file the true owner)
    - ownership: SATISFIED (plain WHERE DELETE delegates to fork iceberg-datafusion)
  success_condition: the first COW DELETE remains green; a second DELETE after an overwrite snapshot raises V3-COW-1 before snapshot, rows, or lineage change on all doors
  step_risks:
    - guard blocks merge-on-read DELETE: HANDLED(check write.delete.mode before the snapshot predicate)
    - guard broadens to UPDATE or MERGE: HANDLED(MorDmlKind::Delete only)
    - stale C7 success pin hides the divergence: HANDLED(replace all three success nodes with immutable refusal pins)
  contingencies:
    - current snapshot summary cannot classify the fork rewrite: EXECUTABLE(stop and report; do not use a broader counter heuristic)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 10. C-004 C7 Spark correction and safe disposition (2026-08-30)

The parent-owned PySpark 4.1.2 + Iceberg 1.11.0 read-back returned this exact C7 transcript:

| Step | Live rows | `next-row-id` | `(first-row-id, added-rows)` |
|---|---|---:|---:|
| Seed | `1`, `2`, `3` | 3 | `(0, 3)` |
| `DELETE id = 2` | `1`, `3` | 5 | `(3, 2)` |
| `DELETE id = 3` | `1` | 5 | `(5, 0)` |

The prior C7 success claim in §9 is superseded. RePark's unguarded second COW DELETE produced
`next-row-id = 6` and `(first-row-id, added-rows) = (5, 1)`. The fork-owned
`iceberg-datafusion` COW delete rewrites surviving rows through `StreamingDataFileWriter` and
`OverwriteFiles`; its `FirstRowIdPolicy::Suppress` path drops the Spark lineage rule. The true
owner is the fork, not V3-3.

`refuse_v3_cow_dml` now fails before the fork write only when all of these hold: format v3 or
later, `MorDmlKind::Delete`, delete mode is not merge-on-read, and the current snapshot operation
is `Overwrite`. The first COW DELETE stays available and Spark-equal. A second COW DELETE in the
measured unsafe state raises `V3-COW-1` with rows, snapshot, lineage, and facade metadata
unchanged. The guard does not reach merge-on-read DELETE, UPDATE, or MERGE.

Final focused evidence after the guard:

- `cargo test -p repark-spark --lib -- tests::v3_cow::` → `12 passed`.
- `cargo test -p repark-spark --lib -- tests::v3e3::` → `10 passed`.
- `cargo test -p repark-sql --lib -- v3_cow::` → `11 passed`.
- `cargo test -p repark-sql --lib -- v3_partitioned_equality_deletes` → `6 passed`.
- `env UV_CACHE_DIR=/tmp/rp3-uv-cache PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=.venv .venv/bin/python -m pytest python/repark/tests/test_v3e3_fixtures.py python/repark/tests/test_v3_live_oracle.py python/repark/tests/test_v3_cow_dml.py -q` → `11 passed, 3 skipped in 2.27s`.

The three skipped facade rows are the separately gated live JVM-oracle tier. C7's actual Spark
read-back is recorded above. C8 remains a separate live-DV UPDATE pre-write refusal with byte and
row preservation on all reachable doors. **C-004 remains OPEN for the orchestrator's charter
verdict: C7 is now measured and safely refused, but is not a green COW-lineage implementation.**

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-004-C7-RED-CONCLUSION
  agent: actor
  action: validate the C7 pre-write guard across both Rust doors and the facade
  charter_trace: C-004
  preconditions:
    - Spark transcript: SATISFIED (recorded in §10)
    - first COW DELETE: SATISFIED (Spark-equal 5, 3, 2)
    - second COW DELETE guard: SATISFIED (V3-COW-1 before table change on three doors)
    - C8 unsafe live-DV UPDATE guard: SATISFIED (rows, snapshot, and bytes unchanged)
  success_condition: no unmeasured unsafe COW lineage rewrite can commit through a reachable door
  tripwire_scan: CLEAN
  uncertainty: charter closure classification for a measured red C7 belongs to the orchestrator
  verdict: HOLD
  escalation: C7 remains a fork-owned lineage defect; do not route it to V3-3
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-005-C-006-MEASURE
  agent: actor
  action: remeasure F-7 U1 at d408da42 and execute the bounded MW-7 F-16 measurement
  charter_trace: C-005, C-006
  preconditions:
    - fork pin: SATISFIED (workspace resolves d408da42fb91db2010662fe1da3783b82fa6e1ed)
    - local Spark oracle: SATISFIED (.venv imports PySpark 4.1.2; Zulu 17 and Iceberg 1.11.0 jar are present)
    - disk headroom: SATISFIED (/ has 621G free, 2026-08-30)
    - C-004 shared edits: SATISFIED (known user-owned diff; preserve unchanged)
  success_condition: C-005 records a direct-action before/after lineage result with Spark read-back; C-006 records the 2,500-row pin result and one 1e7 x 50 MOR transcript without timing assertions
  step_risks:
    - public CALL guard hides a direct-action lineage defect: HANDLED(exercise the fork action below the guard)
    - a partial 1e7 run is mistaken for F-16 closure: HANDLED(record each checkpoint and classify a failed run as evidence, not closure)
    - scratch evidence is deleted before it is durable: HANDLED(record the transcript before scoped cleanup)
    - concurrent actor edits are overwritten: HANDLED(append only to the ledger; do not edit C-004 files)
  contingencies:
    - lineage is Spark-equal: EXECUTABLE(stop and request a scope ruling before changing V3-LINEAGE-1)
    - local Spark cannot execute: EXECUTABLE(record the environment failure and return HOLD without changing guards or the C-006 pin)
  tripwire_scan: CLEAN (only known concurrent C-004 files and this append are modified)
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-005-SPARK-RUNTIME-RETRY
  agent: actor
  action: rerun the C-005 Spark read-back using the already-cached Iceberg runtime jar rather than Ivy package resolution
  charter_trace: C-005
  preconditions:
    - direct action committed: SATISFIED (12 rewritten, 1 added; output pointer captured in task scratch)
    - first Spark attempt: FAILED AS ENVIRONMENT (Ivy tried to write its per-run descriptor under a read-only shared cache before creating a session)
    - pinned runtime jar: SATISFIED (iceberg-spark-runtime-4.1_2.13-1.11.0.jar is present)
  success_condition: PySpark 4.1.2 reads both captured metadata pointers through a Hadoop catalog and prints values, Arrow schema, and lineage
  step_risks:
    - resolving another artifact changes the oracle basis: HANDLED(use the exact cached 1.11.0 jar)
    - the failure is misreported as a lineage measurement: HANDLED(record it only as environment evidence)
  contingencies:
    - local loopback remains denied: EXECUTABLE(request the required escalated local run; do not classify C-005)
  tripwire_scan: CLEAN (append-only ledger and task-owned scratch only)
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-006-FULL-MOR-MEASURE
  agent: actor
  action: execute the one exact 10,000,000-row, 50-MERGE, merge-on-read MW-7 measurement
  charter_trace: C-006
  preconditions:
    - exact driver and parameters: SATISFIED (python/repark-parity/bench/mw7/run_mw7.py inspected; charter-bound values are available)
    - small-scale closure pin: SATISFIED (test_delete_laden_in_band_file_survives_the_runbook passed 2026-08-30)
    - disk headroom: SATISFIED (/ has 619G free, 2026-08-30)
    - task scratch ownership: SATISFIED (a new /tmp/rp3-c006-* directory will be created for this measurement only)
  success_condition: one completed MOR transcript records row count at checkpoints 10 through 50, pre-maintenance delete counts, post-position-rewrite delete count, and post-data-rewrite delete counts without timing or unstable-file-count assertions
  step_risks:
    - process ends before the final census: HANDLED(record the failed measurement as non-closure and retain task scratch until its evidence is durable)
    - a timing or unstable data-file count becomes a correctness assertion: HANDLED(record only required row and delete counts)
    - shared branch files change during the long run: HANDLED(do not edit repository code or maps during the measurement)
  contingencies:
    - local resource exhaustion: EXECUTABLE(stop the process and report the exact failure; no cleanup before durable evidence)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-005-ORACLE-RECORD
  agent: actor
  action: append the parent-owned C-005 PySpark oracle transcript and update its clause verdict
  charter_trace: C-005
  preconditions:
    - direct fork action: SATISFIED (12 data files rewritten; one replacement data file committed)
    - parent-owned PySpark oracle: SATISFIED (exit 0; rows and Arrow types equal while lineage differs)
    - closure rule: SATISFIED (C-005 permits measured reassignment when the public guard remains)
  success_condition: the append names both metadata pointers, exact value/type/lineage result, retained guard, and C-005 PROVEN verdict without changing any guard or driver
  step_risks:
    - equal rows are mistaken for equal lineage: HANDLED(record every id-to-row-id result and sequence-number result)
    - a task-scratch path is lost before evidence is durable: HANDLED(record the pointers and oracle transcript before cleanup)
    - parent-owned C-004 or C-006 work is overwritten: HANDLED(append only; alter only C-005 verdict cell)
  contingencies:
    - transcript contradicts the direct-action output: EXECUTABLE(leave C-005 OPEN and report the conflict)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-006-BOUNDARY-STOP
  agent: actor
  action: stop the in-progress task-owned C-006 10,000,000-row measurement at the parent-requested handoff boundary
  charter_trace: C-006
  preconditions:
    - parent handoff boundary: SATISFIED (finish only C-005; leave C-006 OPEN)
    - run ownership: SATISFIED (/tmp/rp3-c006-O7ff3y was created by this actor)
    - measurement state: SATISFIED (no checkpoint or result JSON has been emitted)
  success_condition: the local measurement process is interrupted, no partial result is classified as evidence, and C-006 stays OPEN with its next command recorded
  step_risks:
    - a partial merge snapshot is read as a closure result: HANDLED(do not inspect or record its table state as C-006 evidence)
    - another actor's process is interrupted: HANDLED(target only this actor's tracked session 68538)
  contingencies:
    - session no longer exists: EXECUTABLE(record that no partial result was emitted and do not retry)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 11. C-005 F-7 U1 direct-action remeasurement (2026-08-30)

The task-owned Rust driver registered the metadata at
`/tmp/rp3-c005-iiwzEh/warehouse/ns/rw_t/metadata/00012-2b05639e-11dc-4512-a48b-f4521b222f27.metadata.json`,
called the fork `RewriteDataFiles` action directly, and reported
`rewritten_data_files_count=12`, `added_data_files_count=1`, and
`removed_delete_files_count=0`. The resulting metadata pointer was
`/tmp/rp3-c005-iiwzEh/warehouse/ns/rw_t/metadata/00013-f68cca55-3a29-4384-b85e-c4922f9a7330.metadata.json`.

The parent-owned local PySpark oracle used the cached Iceberg 1.11.0 runtime after correcting
the task scratch registration call from `CALL system.register_table` to
`CALL local.system.register_table`:

```text
env JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 VIRTUAL_ENV=.venv .venv/bin/python /tmp/rp3-c005-iiwzEh/read_spark.py /tmp/rp3-c005-iiwzEh /tmp/rp3-c005-iiwzEh/warehouse/ns/rw_t/metadata/00012-2b05639e-11dc-4512-a48b-f4521b222f27.metadata.json /tmp/rp3-c005-iiwzEh/warehouse/ns/rw_t/metadata/00013-f68cca55-3a29-4384-b85e-c4922f9a7330.metadata.json
```

It exited 0. Both Arrow schemas were exactly
`id:int64, _row_id:int64, _last_updated_sequence_number:int64`. Both tables contained ids 1
through 12. Before rewrite, `(id, _row_id, _last_updated_sequence_number)` was
`(1,0,1)`, `(2,1,2)`, `(3,2,3)`, `(4,3,4)`, `(5,4,5)`, `(6,5,6)`, `(7,6,7)`,
`(8,7,8)`, `(9,8,9)`, `(10,9,10)`, `(11,10,11)`, `(12,11,12)`. After rewrite, the
ids still ran 1 through 12, but `_row_id` was `(1,22)`, `(2,21)`, `(3,20)`, `(4,23)`,
`(5,19)`, `(6,17)`, `(7,16)`, `(8,15)`, `(9,18)`, `(10,14)`, `(11,13)`, `(12,12)`;
every `_last_updated_sequence_number` was 13.

Therefore values and Arrow types remain equal, but both lineage fields differ. C-005 is
**PROVEN as a measured red**: F-7 U1 still reassigns lineage at `d408da42`, so the public
`V3-LINEAGE-1` guard remains unchanged. This is a direct fork-action result, not evidence from
the guarded public `CALL` path.

## 12. C-006 preliminary observation and boundary stop (2026-08-30)

The existing closure pin ran without retargeting:

```text
env UV_CACHE_DIR=/tmp/rp3-uv-cache PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=.venv .venv/bin/python -m pytest python/repark/tests/test_mw7_scale_smoke.py::test_delete_laden_in_band_file_survives_the_runbook -q
```

It passed: `1 passed in 1.36s`. This is preliminary only. Its MOR fixture sets
`write.delete.granularity = 'partition'`, while the fork's F-16 candidate condition counts only
delete files with `referenced_data_file`; it neither proves nor disproves the required 1e7 result.

The exact full MOR command was started under the prior instruction in task-owned
`/tmp/rp3-c006-O7ff3y`, then interrupted at the parent-requested handoff boundary with exit 130.
It emitted no checkpoint and did not write `result.json`; it is not C-006 evidence. C-006 remains
**OPEN**. The next actor must start a fresh task-owned scratch and run exactly:

```text
env UV_CACHE_DIR=/tmp/rp3-uv-cache PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=.venv .venv/bin/python python/repark-parity/bench/mw7/run_mw7.py --rows 10000000 --merges 50 --partitions 8 --touch-fraction 0.02 --checkpoint-every 10 --reps 7 --target-file-size-bytes 4194304 --modes mor --scratch /tmp/rp3-c006-<fresh> --out /tmp/rp3-c006-<fresh>/result.json
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-005-SCRATCH-CLEANUP
  agent: actor
  action: remove the task-owned C-005 scratch directory after recording its direct-action and Spark-oracle evidence
  charter_trace: C-005
  preconditions:
    - durable evidence: SATISFIED (§11 records both metadata pointers, command, schemas, values, and lineage)
    - cleanup target ownership: SATISFIED (/tmp/rp3-c005-iiwzEh was created by this actor)
    - target scope: SATISFIED (only the exact C-005 scratch directory will be removed)
  success_condition: /tmp/rp3-c005-iiwzEh no longer exists and no C-006 or shared artifact is removed
  step_risks:
    - useful evidence is deleted: HANDLED(complete §11 transcript precedes cleanup)
    - a sibling task artifact is deleted: HANDLED(use the explicit C-005 path without a glob)
  contingencies:
    - target absent: EXECUTABLE(record it as already clean and remove nothing else)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-005-GUARD-PIN-CITATION
  agent: actor
  action: cite the existing Spark-door v3 rewrite refusal pin as C-005's retained-guard regression test
  charter_trace: C-005
  preconditions:
    - direct-action divergence: SATISFIED (§11 proves the retained guard is required)
    - guard pin exists: SATISFIED (call_rewrite_data_files_refuses_a_v3_table_rather_than_reassigning_row_lineage fails if V3-LINEAGE-1 is removed)
    - map read: SATISFIED (crates/repark-spark/src/tests/map.md)
  success_condition: the guard test and its directory map cite rp-3-fork-repin/C-005 without changing tested behavior
  step_risks:
    - citation implies the public guard measured the fork action: HANDLED(§11 distinguishes the direct action from the retained-guard regression test)
    - map drift: HANDLED(update the test-directory map in the same change)
  contingencies:
    - focused pin fails: EXECUTABLE(leave C-005 evidence durable and report the gate red)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-005-PIN-VERIFY
  agent: actor
  action: run the focused Spark-door retained-guard pin and ledger grammar gate after adding C-005's citation
  charter_trace: C-005
  preconditions:
    - citation change: SATISFIED (call_v3 test and tests map cite C-005)
    - focused test command: SATISFIED (cargo test -p repark-spark --lib call_rewrite_data_files_refuses_a_v3_table_rather_than_reassigning_row_lineage)
    - grammar command: SATISFIED (make check-ledger-grammar)
  success_condition: the guard test passes and ledger grammar resolves C-005's pin citation
  step_risks:
    - test passes while grammar still lacks the citation: HANDLED(run the grammar gate after the focused test)
    - shared concurrent changes cause an unrelated failure: HANDLED(report exact output without modifying another slice)
  contingencies:
    - either command fails: EXECUTABLE(stop C-005 conclusion and report the exact gate failure)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-C-004-C-005
  agent: orchestrator
  action: gate and commit the completed C-004 matrix and C-005 remeasurement as a clean handoff boundary
  charter_trace: C-004, C-005
  preconditions:
    - C-004 technical matrix: SATISFIED (§§9-10 record seven green cells and one guarded fork-owned red)
    - C-005 remeasurement: SATISFIED (§11 records the direct action and Spark lineage divergence)
    - C-006 boundary: SATISFIED (§12 leaves the scale run OPEN and excludes the interrupted run from evidence)
    - disk headroom: SATISFIED (618G free, 2026-08-30)
  success_condition: make verify passes on the shipping diff, the completed slice is committed, and the worktree is clean
  step_risks:
    - an interim commit implies RP-3 completion: HANDLED(commit message names only the completed matrix and remeasurement)
    - partial C-006 output is retained as evidence: HANDLED(clean the exact task scratch after the stop transcript is durable)
    - a gate failure is hidden to obtain a clean tree: HANDLED(stop before commit and report the exact red)
  contingencies:
    - make verify fails: EXECUTABLE(keep the diff uncommitted and hand off the exact failure)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-RUST-SIZE-RATCHET
  agent: orchestrator
  action: ratchet the exact Rust file-size baseline after the C-004 helper-test removal
  charter_trace: C-004
  preconditions:
    - make verify red: SATISFIED (call.rs is 1361 lines below its exact 1407-line baseline)
    - source shrink: SATISFIED (obsolete private DV-walker tests were removed with the R114 public API replacement)
    - sanctioned response: SATISFIED (the gate requires the baseline to ratchet down, never up)
  success_condition: the baseline is 1361 and make verify passes without restoring dead test code
  step_risks:
    - the ratchet hides unrelated growth: HANDLED(update only the exact call.rs entry to the measured count)
  contingencies:
    - another gate fails: EXECUTABLE(record the next exact red before any commit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-VERIFY-CACHE-RETRY
  agent: orchestrator
  action: rerun the handoff gate with the task-owned writable uv cache
  charter_trace: C-004, C-005
  preconditions:
    - source gates before Ruff: SATISFIED (format, clippy, panic ban, size, and Python structure passed)
    - first retry failure: FAILED AS ENVIRONMENT (uv could not create a lock in the read-only global cache)
    - task cache: SATISFIED (/tmp/rp3-uv-cache is writable and holds the locked tools)
  success_condition: UV_CACHE_DIR=/tmp/rp3-uv-cache make verify exits zero
  step_risks:
    - a tool-version change masks the gate: HANDLED(the Makefile still resolves its pinned versions)
  contingencies:
    - the retry fails: EXECUTABLE(record the exact non-cache red and stop before commit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-VERIFY-TOOL-DIR-RETRY
  agent: orchestrator
  action: rerun the handoff gate with both writable uv cache and tool directories
  charter_trace: C-004, C-005
  preconditions:
    - cache retry result: FAILED AS ENVIRONMENT (uv then reached its read-only global tool directory)
    - task tool directory: SATISFIED (/tmp/rp3-uv-tools is an exact task-owned writable target)
    - locked tool artifact: SATISFIED (the task cache contains the pinned Ruff package)
  success_condition: the Makefile provisions its pinned tool offline and make verify exits zero
  step_risks:
    - an online resolution delays the boundary: HANDLED(use the populated task cache and no source changes)
  contingencies:
    - provisioning still fails: EXECUTABLE(stop and hand off the environment red with all code gates already green)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-VERIFY-OFFLINE-RETRY
  agent: orchestrator
  action: refresh the task uv cache from the local pinned Ruff artifact and rerun verify offline
  charter_trace: C-004, C-005
  preconditions:
    - pinned local artifact: SATISFIED (global cache contains Ruff 0.15.22)
    - online tool attempt: FAILED AS ENVIRONMENT (sandbox DNS cannot reach PyPI)
    - copy scope: SATISFIED (read-only global uv cache to task-owned /tmp/rp3-uv-cache)
  success_condition: UV_OFFLINE=1 with writable task cache and tool directories completes make verify
  step_risks:
    - an unpinned Ruff version runs: HANDLED(the Makefile requests exactly 0.15.22 and the matching local artifact exists)
  contingencies:
    - offline resolution fails: EXECUTABLE(stop with the exact environment red and no commit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-PYTHON-FORMAT
  agent: orchestrator
  action: apply the pinned formatter to the two C-004 Python test files named by make verify
  charter_trace: C-004
  preconditions:
    - format gate red: SATISFIED (Ruff names only test_v3_live_oracle.py and test_v3e3_fixtures.py)
    - lint gate: SATISFIED (Ruff check passed before the format check)
    - formatter version: SATISFIED (Ruff 0.15.22 from the task-owned offline cache)
  success_condition: both files are formatted and the full make verify rerun exits zero
  step_risks:
    - formatting reaches unrelated files: HANDLED(pass the two exact paths only)
  contingencies:
    - verify finds a semantic red: EXECUTABLE(stop before commit and report it)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-HANDOFF-STOP-C-008-RED
  agent: orchestrator
  action: stop at the completed C-004/C-005 boundary after make verify exposes the chartered C-008 red
  charter_trace: C-004, C-005, C-008
  preconditions:
    - completed slice checks: SATISFIED (format, lint, clippy, structure, docs, and focused C-004/C-005 pins passed)
    - full Rust result: FAILED (589 passed, 1 failed in repark-spark)
    - failing pin: SATISFIED (call_register_table_of_hadoop_named_metadata_writes_name_the_convention)
    - failure ownership: SATISFIED (C-008 explicitly owns retargeting the pre-#235 refusal to a successful Hadoop vN write)
    - owner boundary request: SATISFIED (stop at the nearest honest handoff point)
  success_condition: no partial C-008 implementation or false green claim is added; the exact red and next command remain in the handoff
  step_risks:
    - committing a known-red unit: HANDLED(no commit at this boundary)
    - the interrupted scale run is mistaken for C-006 evidence: HANDLED(§12 records exit 130 and no result)
    - task scratch consumes disk after handoff: HANDLED(remove only /tmp/rp3-c006-O7ff3y after this record)
  contingencies:
    - the next session resumes: EXECUTABLE(start at C-006 or C-008 using §§12 and the frozen clause rows)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: STOP
  escalation: C-008 must be implemented before make verify and any commit can pass
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-008-RETARGET
  agent: actor
  action: retarget the Hadoop vN write pin, pin S3 Tables R126, and mark V3-ADOPT-1 FIXED
  charter_trace: C-008
  preconditions:
    - handoff red: SATISFIED (call_register_table_of_hadoop_named_metadata_writes_name_the_convention still expected a Hadoop write refusal)
    - fork F-14: SATISFIED (MetadataLocation bumps vN to v(N+1) at d408da42)
    - fork F-9: SATISFIED (S3 Tables register_table returns FeatureUnsupported naming R126 before AWS)
    - both doors: SATISFIED (Spark CALL INSERT; ANSI Catalog::register_table then INSERT)
  success_condition: Hadoop write commits v2.metadata.json on Spark and ANSI; S3 Tables CALL cites R126; V3-ADOPT-1 FIXED; guide cites R126; iceberg_to_datafusion no longer claims Hadoop writes fail
  step_risks:
    - expire_snapshots no-op is mistaken for a write: HANDLED(INSERT is the asserted commit)
    - Hadoop wrap keeps a false operator message: HANDLED(rewrite removed; fold stays External(iceberg::Error))
    - S3 Tables CALL hits AWS: HANDLED(fork returns FeatureUnsupported before any service call)
  contingencies:
    - INSERT does not bump the Hadoop pointer: EXECUTABLE(leave C-008 OPEN and report the exact filename)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 13. C-008 F-9 / F-14 (2026-08-30)

The Hadoop pin no longer expects a write refusal. Spark-door
`call_register_table_of_hadoop_named_metadata_writes_name_the_convention` registers
`v1.metadata.json`, inserts one row, and asserts the next pointer is `v2.metadata.json` with
four live rows. ANSI-door `ansi_hadoop_named_metadata_write_bumps_to_the_next_hadoop_pointer`
does the same through `Catalog::register_table` then `INSERT`.

S3 Tables `CALL s3t.system.register_table` is pinned by
`call_register_table_on_s3_tables_names_the_dated_service_gap`: `FeatureUnsupported` names
R126 and `no register-by-metadata-location` with no AWS call. Guide and registry cite that
gap as `S3T-1`.

Registry `V3-ADOPT-1` is FIXED 2026-08-30 (fork #235). The `iceberg_to_datafusion` Hadoop
rewrite is removed so an invalid metadata name cannot still claim Hadoop writes fail.

C-006, C-007, C-009, C-010, C-011 remain OPEN. C-004 remains OPEN for the orchestrator
classification of the measured C7 refusal.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-008-VERIFY
  agent: actor
  action: run make verify after the C-008 retarget and commit the shipping tree
  charter_trace: C-004, C-005, C-008
  preconditions:
    - focused pins: SATISFIED (Spark Hadoop write, S3 Tables R126, ANSI Hadoop write)
    - disk: SATISFIED (599G free)
    - uv dirs: SATISFIED (/tmp/rp3-uv-cache and /tmp/rp3-uv-tools)
  success_condition: UV_CACHE_DIR=/tmp/rp3-uv-cache UV_TOOL_DIR=/tmp/rp3-uv-tools make verify exits 0
  step_risks:
    - a known C-004/C-005 file is omitted from the commit: HANDLED(commit the full shipping tree)
  contingencies:
    - verify reds: EXECUTABLE(no commit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

`make verify` exited 0 (148s) with `UV_CACHE_DIR=/tmp/rp3-uv-cache UV_TOOL_DIR=/tmp/rp3-uv-tools`.
`git diff --check` clean. C-006 remains OPEN with the §12 command.
