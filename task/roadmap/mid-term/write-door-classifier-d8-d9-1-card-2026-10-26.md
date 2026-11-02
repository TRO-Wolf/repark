# Card WRITE-DOOR-CLASSIFIER-D8-D9-1: the static door classifier and the runtime door record name different doors for a MERGE update

**Date:** 2026-10-26. **Filed by:** Claude (claude-haiku-5-5), docs clerk, on the orchestrator's order from the ICE-RESOURCE-BOUND-1 step 0 hand-back. Found by the step 0 probe.

**Status:** open. Grade C (clerk): the measurement is recorded here; no build order is written.

**Why:** the static door reading, `write_door_of(sql, options)`, answers D8 for a Spark MERGE whose matched clause is `UPDATE SET t.a = s.a`. The per-session runtime record, `last_write_doors`, answers D9 after the same statement runs. A caller that asks both questions gets two door names for one statement.

**Measured** (the step 0 probe on branch `feat/ice-resource-bound-1`, head `ed157eea`, 2026-10-26):

- `write_door_of` on `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.a = s.a` gives D8.
- `last_write_doors` after the same statement gives D9, on a copy-on-write target (the census cell at pool 2048 MiB) and on a merge-on-read target (the PP-02 cells, 200k to 16M rows, v2 and v3).
- The probe's column `a` is `DOUBLE`. WRITE-SPINE-1 ledger C-010 and C-016 record the same static and runtime split for the corpus's `SET t.c = s.c` cells: the runtime answers D9 only for a leaf `TIMESTAMP` or `TIMESTAMP_NTZ` column, and D8 for a struct, array or map. A leaf `DOUBLE` is not in that corpus; the probe records D9 there. This card covers a case the WRITE-SPINE-1 divergence record does not name.

**Retires:** when the static reading and the runtime record agree on a MERGE whole-column update of a leaf `DOUBLE` column, on copy-on-write and on merge-on-read, and a pin asserts the agreed door on both reads. The WRITE-SPINE-1 pins for this class (C-010 and C-016) are revised in the same change.

**Relation to WRITE-SPINE-1:** the door naming is part of the door matrix and the conform stage that [WRITE-SPINE-1](write-spine-1-card-2026-10-22.md) owns. This card is not scheduled; the owner picks its place against WRITE-SPINE-1's steps.

**Where to look:** the probe, `task/wo/ice-resource-bound-1/probe_bound.py` (`merge_statement`); the table, [step-0-bound-table-2026-10-26.md](../../wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md); the unit ledger, [ice-resource-bound-1-ledger.md](../../ledgers/completed/ice-resource-bound-1-ledger.md), clause C-005.
