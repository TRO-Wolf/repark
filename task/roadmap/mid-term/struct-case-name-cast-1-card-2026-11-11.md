# Card STRUCT-CASE-NAME-CAST-1: struct-operand refusals on the head build show "Unsupported CAST from Struct" with no class

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the seat-family evidence on PR #1295 (the head-leg refusal text in `task/wo/struct-compare-seat-family-1/evidence/pin_cells.py`, the `CARDS` table).

**Status:** filed 2026-11-11, not dispatched. Severity **S3**, class only. Pre-existing or introduced: not recorded in the verdicts, and the clerk did not measure the base leg for these cells. This is an open question for the owner (see the hand-back of this records PR).

**Measured:** the pin file maps the head-leg text "Unsupported CAST from Struct" to this card. It is named on 107 lines of `python/repark/tests/sql/data/struct_compare_seat_family_cells.jsonl`; Spark gives the class for each cell it names.

**Close:** each cell refuses with Spark's class, or answers as Spark does, with the pins updated.
