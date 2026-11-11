# map — repark-core/src/session/df_guards/signed_zero

## Purpose

SIGNED-ZERO-NORMALIZE-1 part A (2026-11-11). Spark compares `-0.0` and `0.0` as equal; arrow's
float kernels and DataFusion's `ScalarValue` order them by totalOrder. This directory holds the
literal seats that make every float comparison against a zero literal answer with IEEE equality.
Part B (key sites, the UDF) adds `keys.rs` and `udf.rs` here. Design: the unit's order and sketch
under `task/wo/signed-zero-normalize-1/`.

## Contents

- [../signed_zero.rs](../signed_zero.rs) — the module root. `install_literal_seat` adds seat 1 to
  a built session; `../df_guards.rs` calls it from `finish_session_build` (after the dialect's
  `on_session_built`, so after the door rules and `FoldSparkNumericCasts`) and pushes seat 1b
  right after each `simplify_expressions` in `unnest_safe_optimizer_rules`.
- [literals.rs](literals.rs) — `signed_zero_literals`, one rule object that is both the analyzer
  rule (seat 1) and the optimizer rule (seat 1b). A zero operand is a `Float32`/`Float64` zero
  literal, a `Cast`/`TryCast` to a float type of a literal whose `cast_to` value is a zero, or
  `Negative` of either. The rewrite, with `e` the other operand: `e < z` → `e < -0.0`,
  `e <= z` → `e <= +0.0`, `e > z` → `e > +0.0`, `e >= z` → `e >= -0.0`, `e = z` → the bound
  pair `e >= -0.0 AND e <= +0.0`, `e <> z` → `e < -0.0 OR e > +0.0`, `<=>` → `e IS NOT NULL AND`
  the pair, `IS DISTINCT FROM` → `e IS NULL OR` the complement. A zero on the left is swapped.
  `IN` drops its zero items and adds the pair (`NOT IN`: the complement); a zero `IN` operand
  becomes the disjunction of its comparisons. `BETWEEN` with a zero side, and a simple `CASE` whose
  operand or a `WHEN` is a zero, expand first. The output is canonical, so a second pass is the
  identity (`Transformed::no`). NaN results do not move: NaN sits outside `[-0.0, +0.0]` under
  totalOrder. Rules: `NamePreserver` keeps every output name; a node with no candidate is not
  rebuilt; a rewrite that would repeat a volatile operand is skipped; a `GroupingSet` is left
  alone (key sites are part B). It visits `Filter`, `TableScan.filters` (prepared plans), join
  filters, projections and subqueries.
- [tests.rs](tests.rs) — P1 on a core session over an eight-row MemTable (±0, NULL, NaN, ±Inf,
  1.5, -2.5): eight operators × nine zero shapes (float, signed, integer, exponent, string and
  integer casts, two folded) × `DOUBLE`/`FLOAT` × side × `NOT`, against the Spark truth (zeros
  equal, NaN greatest and equal to itself, NULL rules of each operator); lists, ranges, simple
  `CASE`, computed operands, literal against literal; native `PREPARE`/`EXECUTE`; names; the
  fixpoint of both seats; seat 1b on a folded zero.
