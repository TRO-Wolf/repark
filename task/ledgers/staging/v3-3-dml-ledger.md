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
| C-001 | `UPDATE` on a DV-free v3 table is Spark-equal on values, Arrow types, and lineage through both SQL doors and the facade, or stays a pre-write `V3-COW-1` with the measured reason. | Three-door pins + Spark 4.1.2 + Iceberg 1.11.0 read-back. | OPEN | RP-3 cell 8 keeps live-DV UPDATE refused. |
| C-002 | `MERGE INTO` on v3 is Spark-equal on the same three doors, or stays refused with the measured reason. Do not lift by routing through a lineage-reassigning writer. | Three-door pins + Spark read-back. | OPEN | RP-3 left MERGE refused. |
| C-003 | Documents match the pins: `V3-COW-1`, north star MOR/COW rows, STATUS. F-rp3-c7 stays a fork finding. | Registry, north star, STATUS. | OPEN | Closes on departure. |

VERDICT: OPEN — 3 clauses, 0 PROVEN, 0 REJECTED.

## 1. Out of scope

- Sequential COW DELETE lineage (RP-3 F-rp3-c7 / fork `FirstRowIdPolicy::Suppress`).
- `rewrite_data_files` lineage (V3-5 / `V3-LINEAGE-1`).
- Compacting live Puffin DVs (V3-5 / `B-MOR-3`).
- v3 types (V3-6).
