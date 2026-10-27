# Card NESTED-LITERAL-SEAM-1: a nested date or wall literal under a row-merge CASE, and a constant nested value, stay on main's UTC reading

**Date:** 2026-10-25. **Filed by:** the WRITE-SPINE-1 step (2) clerk round (`claude-haiku-5-5`), from findings F-2 and F-3 and ruling WS2-25 (orchestrator, 2026-10-25).

**Status:** closed 2026-10-27 by NESTED-LITERAL-SEAM-1 (folded before step (4), by WS2-25, as filed).

**Closed 2026-10-27** by NESTED-LITERAL-SEAM-1, ledger [nested-literal-seam-1-ledger.md](../../ledgers/staging/nested-literal-seam-1-ledger.md): rows C-001 to C-007, C-009 to C-011, C-013 and C-014 PROVEN; C-012 OPEN (the D8 CAST and TRY_CAST forms stay ZS-9-refused, a step (4) question). The measured move set is 52 cells, not the 328 projected by the Retires line below; ledger C-013 records the count correction.

**Why:** step (2)'s site-B rewrite seams only an `arrow_cast` whose argument reads a column. Two measured failures set that limit. (F-2) On D8, a nested literal under the row-merge CASE failed with `column types must match schema types`, because the projected list loses its `PARQUET:field_id` metadata under the volatile UDF; 24 of those 40 cells were EQUAL on main. (F-3) A constant nested value seamed outside a CASE stored `TRY_CAST(instant AS TIMESTAMP_NTZ)` as the UTC wall, and the seam then read it in New York, so the existing pin `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant` failed. The cause of F-2 was not found inside the step's budget.

**Measured:** the 40 F5 nested-literal cells (32 on D8, 8 on D10) stay DIFFERENT, as on main. Under mutant A+B, step (4) would see 80 D8 and D10 literal cells non-EQUAL, because ZS-9 retirement removes the refusal those cells now hit.

**Retires:** when the 40 literal cells move to EQUAL under the seam, and the census recount holds at 328 moves with no other cell moving.

**Relation:** WRITE-SPINE-1 step (2), findings F-2 and F-3 in the [step (2) ledger section](../../ledgers/staging/write-spine-1-ledger.md); the census figures are in the [evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md).

**Verification round 1, 2026-10-25 (Muse MAX, probe P25):** the same mechanism appears on a partitioned UPDATE target. `UPDATE t SET st = named_struct('a', DATE '2026-01-02') WHERE k = 0` on a partitioned `(id INT, st STRUCT<a: TIMESTAMP>, k INT)` stores `1767312000000000` (UTC, base and head in both zones) where Spark stores the session midnight (New York `1767330000000000`, Kolkata `1767292200000000`). Outside the census; the same fix scope as F-3.
