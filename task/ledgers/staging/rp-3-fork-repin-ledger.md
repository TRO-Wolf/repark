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
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `d408da42fb91` (full SHA recorded on the repin commit) and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`. | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff main -- Cargo.toml rust-toolchain.toml` empty outside the revs. | OPEN | Closes on the repin commit — the compile is the first measurement. |
| C-002 | The two standing repin duties hold at the new rev (`NamespaceScopedCatalog` forwards every required `Catalog` method; the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`), and the "what changed under us" note lists every BEHAVIOR / BREAKING change in `ce92a7bf..d408da42` (#227, #230, #232, #233, #235, #237, #239) with the engine site that absorbs it. | Trait diff; fork `metadata_table.rs`; the two metadata-table pins; the note in §3. | OPEN | Which public signatures moved? Known: #237's `RowDelta::add_delete_file_with_sequence_number`, #239's `with_file_prune_only` and public `live_deletion_vectors_by_data_file` / `spec::is_deletion_vector`. |
| C-003 | **F-17 wired engine-side.** The engine's MOR DELETE / UPDATE / MERGE path calls the fork's `close_touched_dv_containers` for every touched data file's live DV and commits the replacement containers through `RowDelta` with the fork's sibling sequence stamping, so no untouched sibling blob is lost; a sabotage build that skips the call must red the shared-Puffin pin. | The wiring in `crates/repark-iceberg/src/write/merge/mod.rs` (`plan_and_commit_mor`, `commit_row_delta`); the fixture pin flipped from refuse to Spark-equal rows; the mutation run recorded here. | OPEN | Arm `validate_deleted_files` over every replacement reference as the fork's V3 DELETE now does (the named Java skip-delete divergence), or stay on Java's skip? Decide, record, pin. |
| C-004 | **The DV input-state matrix, per door.** Every reachable cell runs through both SQL doors and the facade, values and Arrow types asserted through `collect` / `to_arrow`: (1) DV-free first MOR DELETE — Puffin DV committed, Spark reads identical rows; (2) engine-written DV then a second MOR DELETE — positions merged, old DV superseded, exactly one live DV; (3) Spark-written DV then MOR DELETE on the same data file — same result as (2); (4) shared Puffin, touch one of several blobs — untouched siblings stay effective (`v3-spark-part-dv`: `DELETE id = 1` → `{3,4,6}`); (5) one DELETE touching several files and partitions — one correct DV per data file, spec and partition correct; (6) equality delete + DV (`v3-spark-eq-dv`) — neither class lost; (7) DV-free COW sequential DELETE statements — rows and lineage Spark-equal; (8) an unsafe state — loud pre-write refusal, bytes and rows unchanged. The `V3-COW-1` live-DV refusal lifts only for cells green on all three doors; a red cell stays refused and is filed as a fork or engine finding. | One pin per cell per door; the PySpark 4.1.2 + Iceberg 1.11.0 read-back of every engine commit; `count_live_deletion_vectors` either replaced by the R114 public `live_deletion_vectors_by_data_file` or kept with a stated reason. | OPEN | The Spark-job-written shared-Puffin fixture is the fork's named F-17 residue (GAP row R114 🟡): report cell (4)'s result back to the fork. |
| C-005 | **F-7 U1 re-measured at the frozen SHA.** `CALL system.rewrite_data_files` on the v3 fixture either carries `_row_id` / `_last_updated_sequence_number` through compaction Spark-equal (guard lifts, `V3-LINEAGE-1` → FIXED, dated) or still reassigns (guard stays; the measured divergence filed against the fork row it waits on before V3-5 charters). A green fork row R166 is not evidence. | RP-2's §3 C-004 driver re-run; Spark read-back of both state copies. | OPEN | — |
| C-006 | **F-16 measured** (transferred from #254 C-009). MW-7's 1e7×50 MERGE-then-maintain sequence on a merge-on-read table ends at zero delete files and zero delete records with the default `delete-ratio-threshold` (0.3); the MW-7 pin flips from "documents the gap" to "asserts zero"; the maintenance runbook drops its residual-delete caveat. | The 2,500-row reproduction first, the 1e7 run once; the pin; the runbook diff. | OPEN | — |
| C-007 | **F-7 U3 measured** (from #254 C-011). `CALL system.rewrite_position_delete_files` on the adopted v3 fixture no longer refuses (`B-MOR-3`): the fork's v3 DV arm runs, the Spark read-back is unchanged before and after, and a second run converges — or it stays refused with the measured reason recorded against fork row R136. | Both doors + facade on the V3E-3 fixture; rows + `sum(id)`; `.delete_files` before / after / after-again. | OPEN | R136's v3 arm is ENGINE-FIRST (no Java oracle); Spark read identity is the measurement. |
| C-008 | **F-9 taken, F-14 measured** (from #254 C-010 and the F-14 landing). S3 Tables `register_table` refuses naming the dated service gap (fork row R126, #233) and the guide / registry cite it; a table registered from a Hadoop `vN.metadata.json` pointer takes a write and the next pointer is `v(N+1).metadata.json` (fork #235) — `call_register_table_of_hadoop_named_metadata_writes_name_the_convention` retargets from "the refusal names the convention" to "the write succeeds" and registry `V3-ADOPT-1` moves to FIXED, dated. | Grep the guide and registry; the retargeted pins; the Hadoop-pointer write on both doors. | OPEN | — |
| C-009 | **F-15 carried, not consumed** (from #254 C-012). The repin compiles and every gate passes with the fork's `write_default` fill in `DataFileWriter::write`; no engine surface sets a `write_default`, so the append fixtures are byte-flat before / after, and V3-6's charter gains the note that the fork surface exists. | Fixture byte comparison; the V3-6 note. | OPEN | — |
| C-010 | The documents say what the pins prove: north star §3 rows (MOR DML, COW DML, `rewrite_data_files`, `rewrite_position_delete_files`, adoption), STATUS, the slate, the handoff (F-7 U3 / F-9 / F-14 / F-15 / F-16 / F-17 marked with fork PR and date; take / skip per "Version-pin contract"), `docs/fork-sync.md`, crate maps and the divergence registry in lockstep; V3-3 chartered from C-004's red cells. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar`, the plan-pin test. | OPEN | Closes on the departure commit. |
| C-011 | Green on the whole surface: `make preflight`, the parity suite (`python/repark-parity/tests`), and the V3E-3 / V3E-4 / V3E-5 fixture pins pass at the new rev. | Gate output attached. | OPEN | Closes at readiness. |

VERDICT: OPEN — 11 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN
with its pin (`pins: rp-3-fork-repin/C-NNN`) and the owner confirms.

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
