# Charter ledger — V3-3 · v3 UPDATE and MERGE (the remaining DML after RP-3)

**Date:** 2026-08-30 · **Branch:** opens after RP-3 merges · **Base:** `main` after RP-3 ·
**Path:** STANDARD. **Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** RP-3 measured the DV input-state matrix. Live-DV DELETE merge and shared-Puffin
sibling keep are green. UPDATE and MERGE on v3 still refuse (`V3-COW-1` / write-mode resolver).
A second copy-on-write DELETE after an overwrite snapshot is a **fork** lineage defect
(F-rp3-c7), not this unit.

## PROPOSITION LEDGER — V3-3 — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `UPDATE` on a DV-free v3 table is Spark-equal on values, Arrow types, and lineage through both SQL doors and the facade, or stays a pre-write `V3-COW-1` with the measured reason. | Three-door pins + Spark 4.1.2 + Iceberg 1.11.0 read-back. | **PROVEN** | Keep-refusal. Spark preserves `_row_id` `(1,0,1),(2,1,2),(3,2,1)` after UPDATE; engine lift Spark-read-back reassigns `(1,3,2),(2,4,2),(3,5,2)` (MOR: updated row `1→3`). Pins: `adopted_v3_cow_update_refuses_rather_than_reassign_row_lineage` (Spark + ANSI), facade `test_facade_adopted_v3_cow_dml_refuses_and_leaves_the_table_untouched`. Citation: `crates/repark-spark/src/tests/v3_cow.rs`. |
| C-002 | `MERGE INTO` on v3 is Spark-equal on the same three doors, or stays refused with the measured reason. Do not lift by routing through a lineage-reassigning writer. | Three-door pins + Spark read-back. | **PROVEN** | Keep-refusal. Spark MERGE keeps matched `_row_id` and assigns a new id only to the insert; engine lift Spark-read-back reassigns every row. Did not lift through OverwriteFiles `FirstRowIdPolicy::Suppress`. Pins: `adopted_v3_cow_merge_refuses_with_unset_and_explicit_mode`, `adopted_v3_mor_merge_still_refuses` (Spark + ANSI), facade MERGE arm. Citation: `crates/repark-spark/src/tests/v3_cow.rs`. |
| C-003 | Documents match the pins: `V3-COW-1`, north star MOR/COW rows, STATUS. F-rp3-c7 stays a fork finding. | Registry, north star, STATUS. | **PROVEN** | Registry `V3-COW-1` records the 2026-08-30 Spark vs engine `_row_id` matrix; north star MOR/COW rows name V3-3 keep-refusal; STATUS Next is V3-4; F-rp3-c7 stays a fork finding. Citation: `python/repark-parity/tests/test_v3r_1_rulings.py`. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Actor coverage attestation

Filed with the keep-refusal so the grammar gate can read PROVEN clauses. The Critic
re-attacks; this block records the Actor measurement, not Critic convergence.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-3-dml
  cycle: actor
  risk_tier: standard
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001 and C-002 accept keep-refusal. Spark 4.1.2 + Iceberg 1.11.0 preserves
        `_row_id` on UPDATE and MERGE; an engine lift Spark-read-back reassigns.
        The guard stays. C-003 documents that matrix.
      artifacts: [PROGRESS.md, crates/repark-spark/src/tests/v3_cow.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-2
      status: ATTACKED
      evidence: >
        DV-free COW, MOR, and unset write-mode recipes; 3-row seed; MERGE match+insert;
        incidental COW DELETE control. Live-DV UPDATE stays refused (RP-3 cell 8).
      artifacts: [/tmp/v3-3-spark.out, adopted_v3_cow_update_refuses_rather_than_reassign_row_lineage]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Shared helper asserts snapshot, lineage counters, live rows, and format version
        are unchanged after a refused UPDATE or MERGE.
      artifacts: [assert_cow_refused_untouched]
    - id: AT-4
      status: N/A
      justification: The unit does not add a concurrent writer or change OCC retry.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, credentials, or path-injection surface.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Spark read-back of engine-written tables is the integrity check. COW UPDATE
        reassigned every survivor; MOR UPDATE reassigned the updated row; COW MERGE
        reassigned every row. That is why the lift is refused.
      artifacts: [/tmp/v3-3-spark-readback.py]
    - id: AT-7
      status: N/A
      justification: Keep-refusal adds no hot path or unbounded write.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Fork OverwriteFiles and RowDelta use FirstRowIdPolicy::Suppress. Lifting the
        guard would call those writers. Charter forbids that shortcut. F-rp3-c7 stays
        a fork finding.
      artifacts: [iceberg-datafusion physical_plan/delete.rs, transaction/overwrite_files.rs]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Refusal text names V3-COW-1, row lineage, the verb, and reassigns on all three
        doors.
      artifacts: [row_lineage_guard.rs, test_facade_adopted_v3_cow_dml_refuses_and_leaves_the_table_untouched]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Red-first: the reassigns needle failed on the pre-V3-3 UPDATE message, then
        went green after the measured-reason text. Existing second-DELETE and v2
        controls stayed green.
      artifacts: [PROGRESS.md red-first, adopted_v3_cow_update_refuses_rather_than_reassign_row_lineage]
  reattested: []
```

## 2. Out of scope

- Sequential COW DELETE lineage (RP-3 F-rp3-c7 / fork `FirstRowIdPolicy::Suppress`).
- `rewrite_data_files` lineage (V3-5 / `V3-LINEAGE-1`).
- Compacting live Puffin DVs (V3-5 / `B-MOR-3`).
- v3 types (V3-6).
