# Card STRUCT-POSITION-ERROR-NAMES-1: the DataFrame string door shows internal column names in BINARY_OP_DIFF_TYPES messages

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 re-check item R2 on PR #1295 (`verify-fam-opus-re/verdict.md`).

**Status:** filed 2026-11-11, not dispatched. Severity **S4**. Class (b): both sides refuse with the same class; only the text differs. Introduced by STRUCT-COMPARE-SEAT-FAMILY-1 (the unit at a30aa5e2), except for two base cells.

**Measured:**

- On the DataFrame string door (crossJoin frames), `BINARY_OP_DIFF_TYPES` messages render `__repark_l_<plan>_<n>_s` or `__repark_r_...` where Spark renders `s`.
- 46 head records: 18 simple `CASE`, 14 `nullif`, 8 `BETWEEN` and 6 `NOT BETWEEN`. Base shows it on 2 of them (`BETWEEN`, `df_typed`).
- Q0459: Spark `"(commonexpressionref() >= s)"`; head `"(commonexpressionref() >= __repark_r_7bc1dc566cd8_3_s)"`.
- C05 on a `MAP` operand, via `F.expr`: head `"(__repark_l_..._1_m = named_struct(a, 1, b, 2))"`; Spark `"(m = named_struct(a, 1, b, 2))"`.
- The delta's `visible()` strip covers only the new `INVALID_ORDERING_TYPE` branch. `binary_op_diff_types` and `common_ref_mismatch` still render the internal name.

**Close:** the messages on the string door render the visible column name, as Spark does, on the 46 records and C05.
