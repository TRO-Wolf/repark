# map — repark-core/src/session/df_guards/signed_zero

## Purpose

SIGNED-ZERO-NORMALIZE-1 part A (2026-11-11). Spark compares `-0.0` and `0.0` as equal; arrow's
float kernels and DataFusion's `ScalarValue` order them by totalOrder. This directory holds the
literal seats that make every float comparison against a zero literal answer with IEEE equality.
Part B part 1 (2026-11-12) adds the key seat and its UDF: every flat FLOAT/DOUBLE key treats `-0.0`
and `0.0` as one value and every NaN as one value, greatest, as Spark does. Design: the unit's order and sketch
under `task/wo/signed-zero-normalize-1/`.

## Contents

- [../signed_zero.rs](../signed_zero.rs) — the module root. Each seat is installed inside `Switched`,
  which reads the door's `SignedZeroPolicy` (`literals` for seats 1/1b, `keys` for seat 2; the
  `SessionExtension::signed_zero_policy` hook, both `true` on the Spark door and on native, no user
  config, so a `SET` cannot reach it) and answers `Transformed::no` at the root when off; the rule
  list is the same on every door. `register_normalize_zero` registers the UDF beside
  `register_repark_isnan`; `key_seat` is appended after the loop in `unnest_safe_optimizer_rules`,
  so seat 2 runs last in every pass. `install_literal_seat` adds seat 1 to
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
  rebuilt; a rewrite that would repeat a volatile operand is skipped; a `GroupingSet` (`ROLLUP`, `CUBE`,
  `GROUPING SETS`) has each member rewritten under that member's own saved name, since an alias on
  the set itself is invalid (delta, verifier B1); a bare float key stays part B's. It visits `Filter`, `TableScan.filters` (prepared plans), join
  filters, projections and subqueries. As seat 1b it re-runs `SimplifyExpressions` on each node it
  rewrites, so a fold-made zero leaves nothing for the next pass: an `IS NOT NULL` on a non-null
  operand used to cost a third optimizer pass (cost case (b), `evidence/item4/cost.md`).
- [udf.rs](udf.rs) — `__repark_normalize_zero__` (name in `repark_common::udf_names`): one
  `Float32`/`Float64` argument, the input's field and nullability, `Immutable`. NULL passes, any NaN
  becomes the canonical positive NaN, `-0.0` becomes `+0.0`, every other value keeps its bits
  (`repark_common::float_canon`, shared with `is_duplicated` and `freqItems`). The kernel returns
  the input array itself when nothing needs a change; otherwise one pass. `simplify` collapses a
  directly nested call.
- [keys.rs](keys.rs) — seat 2, `signed_zero_keys` (`ApplyOrder::BottomUp`). Wraps each float key:
  `Aggregate` group expressions (each `GroupingSet` member under its own name), distinct aggregate
  arguments and aggregate `ORDER BY` keys; `Join::on` pairs (null-aware anti joins too, Q1) and the
  join filter's comparisons; `Window` partition and order keys and distinct arguments; `Sort` and
  `Repartition` keys; non-literal comparisons, `BETWEEN`, `IN` lists and simple `CASE` in `Filter`,
  `Projection`, aggregate and window expressions; a surviving `Distinct::All` and a distinct
  `RecursiveQuery` through a projection. Never a literal (a NaN or `-0.0` literal in a key slot is
  canonicalised in place), a plain value, a function argument or `TableScan.filters`. Names:
  `NamePreserver` in `Projection`, `Aggregate` and `Window` only. Fast exit (guard (a)): a node
  whose inputs carry no float column and whose expressions hold no float cast, float literal,
  function call or placeholder is left alone. Idempotence: a key that is already a call, or a
  column whose defining expression below (through `Projection`, `Aggregate` group keys,
  `SubqueryAlias`, `Window`, `Filter`, `Sort`, `Limit`, `Repartition`, `Join`) is one, is skipped.
- [key_tests.rs](key_tests.rs) — P5r over a MemTable (±0, both NaN signs, NULL, 1.5, on `DOUBLE`
  and `FLOAT`): every node kind against the Spark truth, integer-derived keys, `DISTINCT ON`,
  a recursive `UNION`; the `keys`-off session (bit-pattern keys) and a `SET` that cannot reach the
  policy; group, distinct and set outputs store `+0.0` while `UNION ALL` and projections keep the
  stored bits; P6 one wrapper per key with a second optimize the identity, names, qualifiers and
  types equal to the keys-off plan, `PREPARE`/`EXECUTE`, a view, a hand-built `Repartition`; the
  guard (d) pins (no provider takes a wrapped filter as `Exact`, the Filter stays above a Parquet
  scan, rows equal the truth); the kernel's fast path (same `ArrayRef`), null buffer, bits, scalar
  input and field.
- [tests.rs](tests.rs) — P1 on a core session over an eight-row MemTable (±0, NULL, NaN, ±Inf,
  1.5, -2.5): eight operators × nine zero shapes (float, signed, integer, exponent, string and
  integer casts, two folded) × `DOUBLE`/`FLOAT` × side × `NOT`, against the Spark truth (zeros
  equal, NaN greatest and equal to itself, NULL rules of each operator); lists, ranges, simple
  `CASE`, computed operands, literal against literal; native `PREPARE`/`EXECUTE`; names; the
  fixpoint of both seats; seat 1b on a folded zero; seat 1b's output costs no further pass; one
  pin per grouping-set kind against the Spark truth, and their names; a volatile operand
  (`random()`) under `=`, `<>`, `<=>`, `IN`, `BETWEEN` and simple `CASE` stays unrewritten while
  `random() < 0.0` rewrites, so the operand is never repeated.

## Part B part 1 outside this directory (their maps are at their size baselines)

- `repark-core/src/extension.rs` (now `pub mod extension`) — `SignedZeroPolicy` and
  `SessionExtension::signed_zero_policy`; `contains_normalize_zero`, which
  `repark-spark/src/view_ddl/temp_view.rs`'s `ReplanningTempView` asks before it answers `Exact`, so
  no provider in use takes a wrapped filter as `Exact` (guard (d), option (a), ruled 2026-11-12).
  `SparkExtension` (`repark-spark/src/extension.rs`) and `NativeDoorTypes`
  (`repark-python/src/session_runtime.rs`, where the native carve-out would set `keys` off) set
  both switches on.
- `repark-common/src/float_canon.rs` — the one canonicalisation, re-exported as
  `repark_core::float_canon` and used by `freq_items.rs` and `repark-python`'s `is_duplicated.rs`;
  `repark-common/src/udf_names.rs` holds `NORMALIZE_ZERO_NAME`.
- `df_guards.rs` and `session.rs` — registration, the seat order and the policy hand-off.

