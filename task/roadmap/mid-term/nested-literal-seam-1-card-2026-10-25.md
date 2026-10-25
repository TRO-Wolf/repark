# Card NESTED-LITERAL-SEAM-1: a nested date or wall literal under a row-merge CASE, and a constant nested value, stay on main's UTC reading

**Date:** 2026-10-25. **Filed by:** the WRITE-SPINE-1 step (2) clerk round (`claude-haiku-5-5`), from findings F-2 and F-3 and ruling WS2-25 (orchestrator, 2026-10-25).

**Status:** open. Fold before step (4), by WS2-25. Grade C (clerk): no build order is written.

**Why:** step (2)'s site-B rewrite seams only an `arrow_cast` whose argument reads a column. Two measured failures set that limit. (F-2) On D8, a nested literal under the row-merge CASE failed with `column types must match schema types`, because the projected list loses its `PARQUET:field_id` metadata under the volatile UDF; 24 of those 40 cells were EQUAL on main. (F-3) A constant nested value seamed outside a CASE stored `TRY_CAST(instant AS TIMESTAMP_NTZ)` as the UTC wall, and the seam then read it in New York, so the existing pin `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant` failed. The cause of F-2 was not found inside the step's budget.

**Measured:** the 40 F5 nested-literal cells (32 on D8, 8 on D10) stay DIFFERENT, as on main. Under mutant A+B, step (4) would see 80 D8 and D10 literal cells non-EQUAL, because ZS-9 retirement removes the refusal those cells now hit.

**Retires:** when the 40 literal cells move to EQUAL under the seam, and the census recount holds at 328 moves with no other cell moving.

**Relation:** WRITE-SPINE-1 step (2), findings F-2 and F-3 in the [step (2) ledger section](../../ledgers/staging/write-spine-1-ledger.md); the census figures are in the [evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md).
