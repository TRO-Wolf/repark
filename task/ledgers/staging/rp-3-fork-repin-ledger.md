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
| C-005 | **F-7 U1 re-measured at the frozen SHA.** `CALL system.rewrite_data_files` on the v3 fixture either carries `_row_id` / `_last_updated_sequence_number` through compaction Spark-equal (guard lifts, `V3-LINEAGE-1` → FIXED, dated) or still reassigns (guard stays; the measured divergence filed against the fork row it waits on before V3-5 charters). A green fork row R166 is not evidence. | RP-2's §3 C-004 driver re-run; Spark read-back of both state copies. | OPEN | — |
| C-006 | **F-16 measured** (transferred from #254 C-009). MW-7's 1e7×50 MERGE-then-maintain sequence on a merge-on-read table ends at zero delete files and zero delete records with the default `delete-ratio-threshold` (0.3); the MW-7 pin flips from "documents the gap" to "asserts zero"; the maintenance runbook drops its residual-delete caveat. | The 2,500-row reproduction first, the 1e7 run once; the pin; the runbook diff. | OPEN | — |
| C-007 | **F-7 U3 measured** (from #254 C-011). `CALL system.rewrite_position_delete_files` on the adopted v3 fixture no longer refuses (`B-MOR-3`): the fork's v3 DV arm runs, the Spark read-back is unchanged before and after, and a second run converges — or it stays refused with the measured reason recorded against fork row R136. | Both doors + facade on the V3E-3 fixture; rows + `sum(id)`; `.delete_files` before / after / after-again. | OPEN | R136's v3 arm is ENGINE-FIRST (no Java oracle); Spark read identity is the measurement. |
| C-008 | **F-9 taken, F-14 measured** (from #254 C-010 and the F-14 landing). S3 Tables `register_table` refuses naming the dated service gap (fork row R126, #233) and the guide / registry cite it; a table registered from a Hadoop `vN.metadata.json` pointer takes a write and the next pointer is `v(N+1).metadata.json` (fork #235) — `call_register_table_of_hadoop_named_metadata_writes_name_the_convention` retargets from "the refusal names the convention" to "the write succeeds" and registry `V3-ADOPT-1` moves to FIXED, dated. | Grep the guide and registry; the retargeted pins; the Hadoop-pointer write on both doors. | OPEN | — |
| C-009 | **F-15 carried, not consumed** (from #254 C-012). The repin compiles and every gate passes with the fork's `write_default` fill in `DataFileWriter::write`; no engine surface sets a `write_default`, so the append fixtures are byte-flat before / after, and V3-6's charter gains the note that the fork surface exists. | Fixture byte comparison; the V3-6 note. | OPEN | — |
| C-010 | The documents say what the pins prove: north star §3 rows (MOR DML, COW DML, `rewrite_data_files`, `rewrite_position_delete_files`, adoption), STATUS, the slate, the handoff (F-7 U3 / F-9 / F-14 / F-15 / F-16 / F-17 marked with fork PR and date; take / skip per "Version-pin contract"), `docs/fork-sync.md`, crate maps and the divergence registry in lockstep; V3-3 chartered from C-004's red cells. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar`, the plan-pin test. | OPEN | Closes on the departure commit. |
| C-011 | Green on the whole surface: `make preflight`, the parity suite (`python/repark-parity/tests`), and the V3E-3 / V3E-4 / V3E-5 fixture pins pass at the new rev. | Gate output attached. | OPEN | Closes at readiness. |

VERDICT: OPEN — 11 clauses, 3 PROVEN (C-001, C-002, C-003), 0 REJECTED. The gate passes when every
row is PROVEN with its pin (`pins: rp-3-fork-repin/C-NNN`) and the owner confirms.

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
