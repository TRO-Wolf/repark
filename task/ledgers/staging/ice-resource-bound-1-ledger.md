# Unit ledger — ICE-RESOURCE-BOUND-1 (U-11) step 0 · the measured bound, the proposed bounds, the open rulings

**Date:** 2026-10-26 · **Branch:** `feat/ice-resource-bound-1` · **Base:** `ab1b9bc8` (main) · **Builder commits:** `e1ccb0a4`, `ed157eea` · **Builder model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Clerk:** Claude Haiku 5.5 (`claude-haiku-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).

**Order:** [../../wo/ice-resource-bound-1/order.md](../../wo/ice-resource-bound-1/order.md), its "Step 0 outcome" block; **table:** [../../wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md](../../wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md); **card:** [ICE-RESOURCE-BOUND-1](../../roadmap/mid-term/ice-resource-bound-1-card-2026-10-20.md).
**This ledger stays in staging.** Its rows are appended, never rewritten; it moves to `completed/` in the unit's last commit.

## Step 0, the measurement (2026-10-26)

The probe ran 261 cells on format v2 and v3: the census, PP-01, PP-02, PA-11 and the Spark 4.1.2 controls. Each cell ran in its own process under a 40 GiB scope. The largest process peak was 14.1 GiB, and no cell reached the cap. The pool peak is not readable from any session surface, so that column is `n/a` in every row. No product code changed. The owner rules the three proposed bounds and the five questions in the order before step 2.

## Clauses

| id | claim | evidence | status | proof |
|---|---|---|---|---|
| C-001 | PP-01 (proposed): an ordered write whose ordered input Arrow bytes exceed 0.10 x pool refuses, with the snapshot unchanged. | Table: ordered D4 at 4M and 16M rows, growth/input 3.22 to 4.68; the worst cell is 4.68 (4M rows, v2, 32 partitions, one writer). At the bound 1.47 x pool against an allowance of 1.667 x pool, margin 1.14; 819 MiB at the 8 GiB default pool. | **OPEN** | The owner rules the bound and Q2 (key and default). Step 2 writes the red pins. |
| C-002 | PP-02 (proposed): a MERGE whose updated-row bytes (matched plus NOT MATCHED BY SOURCE) exceed 0.05 x pool refuses, with the snapshot unchanged. | Table: growth minus pool at least 8.90 x target bytes (4M rows, v3, 10 % plus NOT MATCHED BY SOURCE, pool 2048 MiB); 1.45 x pool, margin 1.15; 410 MiB at the 8 GiB default pool. Below 512 MiB pools the in-pool sort refuses first on every 200k and 1M target. | **OPEN** | The owner rules the bound, Q1 (the refusal text) and Q2. Step 2 writes the red pins. |
| C-003 | PA-11 (proposed): a changelog view whose source bytes in the window exceed 0.0625 x pool refuses before it collects the source. | Table: growth/source 7.38 to 7.83 on all 12 changelog cells (1M, 4M and 16M rows; v2 and v3; `SELECT id ... LIMIT 10` and `SELECT count(id)`). 1.49 x pool, margin 1.12; 512 MiB at the 8 GiB default pool. A 32 MiB pool never refused. | **OPEN** | The owner rules the bound and Q3 (PA-10, whose detector sets the pool). Step 2 writes the red pins. |
| C-004 | Every pool refusal left the snapshot count unchanged, and every commit added exactly one snapshot. | Table column `snapshots`: each refusal reads `N->N` (for example `32->32` on PP-02 cells at 32, 128 and 512 MiB pools); each commit reads `N->N+1`. | **OPEN** | Holds on the measured cells only. The proof is a step 5 pin that reads the snapshot count after each refusal and each commit, on format v2 and v3. No such pin exists yet. |
| C-005 | The static door reading and the runtime door record name the same door for a MERGE `UPDATE SET t.a = s.a` on a leaf `DOUBLE` column. | Static reading D8; runtime record D9 on copy-on-write (census cell, pool 2048 MiB) and on merge-on-read (PP-02 cells, v2 and v3). The probe's census row is `D8 (D9)`. | **OPEN** | The mismatch is carded as [WRITE-DOOR-CLASSIFIER-D8-D9-1](../../roadmap/mid-term/write-door-classifier-d8-d9-1-card-2026-10-26.md). It closes when that card retires, with a pin on both reads. |
