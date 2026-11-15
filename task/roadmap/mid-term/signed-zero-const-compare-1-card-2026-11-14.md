# Card SIGNED-ZERO-CONST-COMPARE-1: two FLOAT/DOUBLE constant zeros compare as one value

**Date:** 2026-11-14. **Filed by:** the SIGNED-ZERO-CONST-COMPARE-1 lane (`claude-opus-5-5`). **Sources:** the owner's
ruling of 2026-11-14, about 13:38 UTC, verbatim "Merge and go with the split plan", adopting Fable's guidance
(`Fable reply — DECIMAL-DOUBLE stack 2026-11-14.md`, finding 3 and step 2b); the inventory critic of 2026-11-14
(`VERDICT: GAPS`, items M1–M8).

**Status:** built on branch `fix/signed-zero-const-compare-1`, in review. Independent of the decimal work.

## The fault

- `CAST('0.0' AS DOUBLE) = CAST('-0.0' AS DOUBLE)` was false on main `d843d37e`. Spark 4.1.2 says true.
- **Where:** DataFusion's constant folding (`ConstEvaluator` in `simplify_expressions`) evaluates a comparison of two
  folded operands with Arrow's total order.
- **Why the literal seat missed it:** the seat recognises a literal, a negation or a cast of a literal.
  - On the Spark door and the DataFrame API a string-to-float cast becomes `__repark_parse_java_double__`.
  - Constant arithmetic is never a literal.
  - The seat that runs after `simplify_expressions` sees only the folded boolean.
- **Measured** (`task/wo/signed-zero-const-compare-1/evidence/step0/table.md`): 33,458 cells, 4,096 comparison
  cells WRONG, including constant-condition `DELETE`, `UPDATE` and `MERGE`; nothing over a column.

## The fix

- The literal seat also runs **before** `simplify_expressions`.
- There it folds each column-free, non-volatile float operand of a comparison alone. Comparisons covered:
  `=`, `<>`, `<=>`, `IS [NOT] DISTINCT FROM`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `[NOT] BETWEEN`, simple `CASE`.
  Searched `CASE` and `IF` are reached through their inner comparison.
- An operand that folds to a zero is substituted. The existing bound form then applies.
- The optimizer applies the rule through subquery plans.
- Two product files: `crates/repark-core/src/session/df_guards/signed_zero/literals.rs` and
  `crates/repark-core/src/session/df_guards.rs`.
- **After** (`task/wo/signed-zero-const-compare-1/evidence/after/table.md`): all 4,096 in-scope cells reach Spark's
  answer, no cell that matched Spark on main moves, and the 4,816 out-of-scope WRONG cells keep their main answers.
- **Pins:** `python/repark/tests/sql/test_signed_zero_const_compare_1.py` and four seat tests in
  `crates/repark-core/src/session/df_guards/signed_zero/tests.rs`.

## Not changed, and where those cells go

| cells | owner |
|---|---|
| NaN constants (2,496 step-0 cells) | NAN-COMPARE-LITERAL-1; a folded NaN is never substituted |
| `nullif` with two constant zeros (Spark: equal, so NULL) | new cells for FUNC-FLOAT-EQ-1 |
| `array_contains(array(c), c')` with constant zeros (Spark: true) | COLLECTION-FLOAT-EQ-1 |
| `greatest`/`least` ties between `0.0` and `-0.0` (Spark keeps the first argument) | [GREATEST-LEAST-ZERO-TIE-1](greatest-least-zero-tie-1-card-2026-11-14.md), not a SIGN-ONLY declaration |
| native-door `IF`, `array(...)` and `MERGE` | refused on main; door gaps, not this fault |
