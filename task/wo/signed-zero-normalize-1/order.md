# SIGNED-ZERO-NORMALIZE-1: order, part A (SIGNED-ZERO-LITERALS)

**Grade:** B (guided). **Builder:** Claude Opus 5.5 (`claude-opus-5-5`).
**Review:** Opus 5.5 and Muse MAX in parallel, on the same head and the same tables (R7, design-heavy). The cap is one
build, one critique, one delta and one delta re-check. The scoped-review rule (2026-11-09) and the named-seat rule
(2026-11-10) bind.
**Release:** v1.5.7.
**Split (sketch §11, adopted):** part A (literals) is this order and its PR. Part B (keys, perf-gated) gets its own
order after A merges, on the same lane.

**Design source.** [sketch.md](sketch.md) of 2026-11-08 and its domain table, [evidence/](evidence/map.md) (#1278).
The sketch applies **as amended below**. Where they disagree, this order governs.

## Rulings this order carries (owner, verbatim)

- **"G yes and defaults"** (2026-11-08). The unit is a v2 label blocker, one Opus 5.5 lane, both doors. Sketch §12's
  twelve sub-questions take their defaults:
  1. the bound form;
  2. seat 1 after the door rules;
  3. seat 1b;
  4. the metadata-delete `literal_datum` guard in this unit;
  5. the fork change carded as `FORK-FLOAT-ZERO-BOUNDS-1`;
  6. the truth column is Spark SQL semantics, with Spark-on-Iceberg zero-only answers declared `PRUNED-WRONG`;
  7. canonical positive NaN for the UDF;
  8. the split, A then B;
  9. the perf gate;
  10. per-door Trino booleans;
  11. the `<=>` widening arms in part A;
  12. `nullif` and other function-internal float comparisons carded as `FUNC-FLOAT-EQ-1`.
- **"Go with defaults"** (2026-11-09), unit C ruling 6: the insertion inventory for every analyzer-seat brief, and
  the sketch gains the review-cap memo §7's three additions. Two bind part A:
  - **Read through `Cast(Literal)`.** `d = 0` becomes `d = CAST(Int64(0) AS Float64)` under `type_coercion`, and is
    folded only later. Seat 1 recognises a zero beneath a cast. Pin both spellings.
  - **Prepared plans.** At `EXECUTE` the predicate may sit in `TableScan.filters`, or be rewritten by
    `UnwrapCastInComparison`. Seat 1 visits `TableScan.filters` as well as `Filter`. The rows are
    `PREPARE … WHERE d = $1; EXECUTE p(0.0)` and `EXECUTE p(-0.0)`, on the native door.

  The third addition, seat-2 idempotence, is part B's.
- **"Q2–Q7 as pasted but they have my full go ahead and sign off"** (2026-11-10): the table-completeness check and
  the named-seat rule (Q3), and the "consumers of the pairing" inventory row (Q4). Both bind from this unit on.
- **"PREPARE: A"** (2026-11-10): the Spark door will refuse `PREPARE` under `SPARK-DOOR-PREPARE-1`, which is carded
  and not dispatched.
  - Until it is built, the Spark door's `PREPARE` rows are measured as they behave today.
  - The native door's `PREPARE` rows are in the clause.
- **"Yes let's do it"** (2026-11-11): this unit starts now.

## Rebase onto today's main

The sketch was written against `bd9c5437`, and the domain table measured the `fdc61f26` wheel. Since then, unit C
(#1283), #1290, #1292 and #1295 changed both analyzer lists and the DataFrame binders. **Item 1 re-verifies every
`file:line` the sketch cites, on main `68b09d18`.** It records each moved or changed seat in `evidence/inventory.md`.
A seat whose premise no longer holds is a HALT for a ruling.

## Clause: part A's measured cells

- **From #1276's tables** (`task/wo/followup-dml-conjunct-and-signed-zero/`): Table B rows c1–c7 and p15, and the
  extension rows E1, E3, E6, E7 and R1–R3.
- **From the domain table:** D1–D7, D11 and D13.
- **The order's additions** (completeness check, below).

**The truth column is Spark SQL semantics** (ruling G, default 6).

**Item 1 re-measures the RePark legs on main `68b09d18`** with the committed probes. The Spark legs stand as
committed. The base for every move is the re-measured leg, not `fdc61f26`.

### Table-completeness check (Q3; orchestrator, 2026-11-11)

| Named seat (part A) | Table rows | Disposition |
|---|---|---|
| Operators `= <> < <= > >= <=>`, `IS DISTINCT FROM`, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `NOT(…)` | c1–c7, D4 | carried |
| Five routes: `spark.sql`, `filter(str)`, `F.expr`, typed, native | D2, D4, D5 | carried |
| Column types `DOUBLE` and `FLOAT` | D5 | carried |
| Literal shapes: float, integer zero beneath a cast, folded, decimal and string casts | D2, D3; P1's literal-shape axis | carried. **Add** (item 1): the integer-literal spelling `d = 0` and `d = -0` on every route (memo §7) |
| Statements: SELECT, DELETE, UPDATE, `INSERT … SELECT`, CTAS, MERGE condition | D7, p15 | carried |
| Kinds: v2 copy-on-write, v2 merge-on-read, v3 | D7, p15 | carried |
| The metadata-only DELETE and the conflict-filter text translators | D1, plus the §5 Rust test | carried |
| The `<=>` widening arms | E6 | carried |
| Reciprocal trig | D13 | carried |
| Names and schema | D6 | carried |
| Stored values kept | D11 | carried |
| `PREPARE` / `EXECUTE`, native door | — | **add** (item 1), the memo §7 rows |
| `PREPARE` / `EXECUTE`, Spark door | — | **add** (item 1): measure as is (ruling A, carded) |
| `CASE d WHEN 0.0` (simple `CASE`), `nullif(d, 0.0)` | D8 (`CASE`, part B), D17 (`nullif`, carded) | simple `CASE` over a literal zero is a literal seat: **add** it to part A (item 1). `nullif` stays carded (`FUNC-FLOAT-EQ-1`) |
| Key sites: GROUP BY, DISTINCT, joins, set operations, windows, ORDER BY | D8–D10, D12, D14–D16 | **part B**, struck from part A in writing |

## Design

Sketch §1, §2, §4 (as far as part A uses it), §5 and §6 apply, with these bindings:

1. **Seat 1, the literal rewrite** (§2). In `repark-core` `df_guards/signed_zero/literals.rs`, installed after the
   door rules (`ReparkSessionBuilder::build`, post-register).
   - It turns every float comparison against a zero literal into the bound form, which is a fixpoint.
   - It reads beneath a cast, and visits `Filter`, `TableScan.filters`, join filters and every expression node.
   - It runs on both doors.
2. **Seat 1b.** The same rewrite right after `simplify_expressions`, for zeros produced by folding.
3. **Ordering against today's rules.** The rule must coexist with:
   - unit C's `struct_setop_names` (after `type_coercion`);
   - #1292/#1295's `struct_operands_by_position` (before `type_coercion`);
   - the binders' `duplicate_field_operands_by_position`.

   Item 1's insertion inventory states where seat 1 sits relative to each of them, and why that is safe.
4. **The `literal_datum` guard** (§5). `crates/repark-iceberg/src/write/conflict_filter.rs` and `meta_delete.rs`
   refuse a float zero literal, so the metadata delete falls back to the row-level path. The bound-form translation
   is carded.
5. **The `<=>` widening arms** in `is_float_comparison`.
6. **The reciprocal-trig repair** (D13).
7. **Names.** No generated column name, `printSchema` or `EXPLAIN` text may show the bound form where base showed the
   user's comparison (D6). Pin it.
8. **Out of part A:**
   - seat 2 and the `__repark_normalize_zero__` UDF (part B);
   - the fork change (carded);
   - `nullif` and function-internal comparisons (`FUNC-FLOAT-EQ-1`);
   - the Spark door's `PREPARE` acceptance (`SPARK-DOOR-PREPARE-1`).

## Inventories (item 1, before code)

- **Insertion inventory** (2026-11-09): every row of the review-cap memo §7's list, answered for seat 1 and seat 1b
  on today's main.
- **Type-variant inventory** (2026-11-09):
  - (1) the float representations that reach the seat, per source: `Float64` and `Float32` (and `Float16`, if any
    door produces it), `Dictionary` and `RunEndEncoded` of float, pandas and polars inputs, Iceberg `float` and
    `double`;
  - (2) the pairs DataFusion's comparison coercion unifies with a float operand: integer, decimal and string
    literals;
  - (3) what the rule does with each.
- **Consumers of the pairing** (Q4): read from the pinned DataFusion 54.1.0 source. List every consumer of float
  **equality or ordering** that the bound form cannot reach:
  - the `type_coercion` arms;
  - functions with comparable or equality semantics (`nullif`, `greatest`, `least`, the array functions);
  - plan nodes that store pairs (`Join.on`, set-operation inputs, aggregate group keys);
  - physical-plan hashing.

  Each entry is a part-A seat in the table, a part-B seat, or carded with its id.

## Pins

- **Sketch §10:** P1 (the seat-1 table, including the fixpoint case and the integer-literal spelling), P2 (per door),
  P3 (DML), P4 (the text translators) and P6 (names), for part A's cells.
- **The `PREPARE` / `EXECUTE` rows** (native).
- **The simple-`CASE`-over-zero row.**
- **Mutation record:**
  - disabling seat 1 turns the literal rows red;
  - disabling seat 1b turns D2 red;
  - removing the `literal_datum` guard turns P4 and D1 red;
  - removing the `<=>` arms brings back the E6 refusal.
- **Both build profiles:** the facade gate runs in the **ci profile** and in **release**. Per-cell captures catch
  pyo3's `PanicException`.
- **Tests go in the same commit as the code they pin.** Every expected value comes from a measured cell.

## Halts

- A sketch premise that no longer holds on today's main.
- A base-EQUAL cell newly refused or newly wrong, unless it is a measured S3 over-refusal carded with its cell id.
- Any cell outside part A's clause that moves, unless it moves to Spark's measured outcome and is named in the
  evidence.
- Any change to the outcome of #1292's or #1295's tables, or to unit C's, #1290's, #1292's or #1295's pins.
- `UPDATE-SCHEMA-BATCH-MISMATCH-1` (evidence `cards.md`) reproducing more often after the change than before. Run 50
  repeats per kind, before and after.
- The cost bar exceeded: ≤ 1.05x median of 3 for the plan-time rule, the standing coercion bar.
  - (a): the domain table's `spark.sql` statements on the facade;
  - (b): a planning-only probe of 300 float columns and 50 comparisons against zero.

## Rulings after item 1 (orchestrator, as the order's author, 2026-11-11)

- **F1: design point 4's premise does not hold, and the guard becomes explicit.** On `68b09d18`, as on `bd9c5437`,
  `literal_datum` already refuses every float-zero spelling. `Decimal::parse` returns `None` for a literal with no
  significant digit (`conflict_filter.rs:368`), and `a_zero_float_literal_leaves_the_filter_unscoped` pins it. So
  the metadata-only DELETE already falls back, and D1's wrong rows are row-level misses that seat 1 fixes.
  - Make the guard a named float-zero check in `literal_datum`, so it no longer rests on the parser's side effect.
  - Add the sketch §5 hand-built-bounds `meta_delete` test.
  - The mutation is "remove the named check and let the parser accept zero".
- **F2: simple `CASE` over a float operand, Spark door.** Add a `Case` arm to `widen_decimal_against_float`
  (`spark_literal_typing.rs`), beside the `<=>` arms. It casts decimal `WHEN` operands to the float type when the
  `CASE` operand is a float, so seat 1 sees float comparisons.
  - The A3 rows are inside the clause: the order named the seat.
- **F3: the legacy division guard is carded.** With ANSI off, `guard_zero_divisor` builds `nullif(divisor, 0.0)`,
  which `FUNC-FLOAT-EQ-1` covers (ruling G, default 12). The cells are `X-div-1`, `-2` and `-4`.
  - Do not change `repark-functions` in this unit.
- **F4: design point 7 means names.** Pin output column names, `printSchema` and the aliases `EXPLAIN` prints. The
  `EXPLAIN` text of `Filter` lines may show the bound form.
- **F5:** D6-3 to D6-6 (`GROUP BY` / `DISTINCT`) pin names only in part A, and their values in part B.
- **Cards** for the records clerk:
  - `FLOAT16-DECIMAL-COERCION-1`, S3: a pandas `float16` column refuses `d = 0.0` at type coercion;
  - `ANSI-REMAINDER-CLASS-1`, S3: `%` by zero raises `DIVIDE_BY_ZERO` where Spark raises `REMAINDER_BY_ZERO`.
