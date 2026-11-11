# Card STRUCT-TYPE-UNION-ACCEPTS-REORDER-1: CASE, IF, coalesce and array accept a reordered struct pair that Spark refuses

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Fable evaluation of 2026-11-09 (cells 17 to 20) and the Fable brief of 2026-11-09 (section 3.2).

**Status:** filed 2026-11-09, not dispatched; S3, accepts what Spark refuses. No wrong value was measured. It is a class outside unit `STRUCT-COMPARE-BY-POSITION-1` and waits on its own ruling.

**Why:** `CASE`, `IF`, `coalesce` and `array` over a struct pair whose fields are declared in a different order answer on RePark, where Spark raises `DATATYPE_MISMATCH.DATA_DIFF_TYPES`. Pre-existing on 1.5.6 and head, measured there; earlier wheels not measured.

**Close:** a pin per expression form asserting Spark's refusal on every door, or a recorded ruling that the acceptance stays with the reason.

## Measured acceptances (2026-11-10)

Rows from section 4 of the frontier review memo of 2026-11-10, measured on Spark 4.1.2 and on RePark's head, main and 1.5.6. D4 itself is unchanged.

- `greatest` and `least`: Spark `DATATYPE_MISMATCH.DATA_DIFF_TYPES`; RePark answers by name.
- `array_position`, `arrays_overlap`, `array_union`, `array_intersect` and `array_except`: Spark `ARRAY_FUNCTION_DIFF_TYPES` or `BINARY_ARRAY_DIFF_TYPES` (the memo does not give the class per function); RePark answers.
- The `map(…)` constructor and `map_concat` with struct keys: Spark refuses (the memo does not record the class); RePark runs both, and reading the result fails with Python's `TypeError: unhashable type: 'dict'`. See STRUCT-KEY-MAP-READ-1.
