# DECIMAL-DOUBLE-UNION-COERCION-1 — the design sketch (measured; no product code)

**Author:** Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh, 2026-11-13. **Rulings carried:** owner
"Defaults" of 2026-11-13 (about 12:12 UTC): an xhigh design sketch first, the build on the next free lane;
#888 (NVL-TYPE-COERCION-1) closes and its table folds in here; one xhigh critic reviews
[inventory.md](inventory.md) before any code. **Measured:** RePark main `04d84ab9` (the
`pkg-04d84ab9` package, Spark door, DataFrame API and native door) against Spark 4.1.2, ANSI on and
off; Iceberg 1.11.0 runtime for the writes. Provenance: [evidence/map.md](evidence/map.md).

## The fault, in one paragraph

Spark gives a DECIMAL beside a FLOAT or DOUBLE the common type `double` in every context (its
`findWiderTypeForDecimal`, the same in both ANSI modes). DataFusion's `decimal_coercion` gives the wider
of the decimal and `decimal(30,15)` (`decimal(14,7)` for FLOAT), and RePark inherits it in two places:
multi-row `VALUES` at SQL planning, and `type_coercion` (analyzer idx 10) for every other construct.
`decimal(30,15)` truncates every double below 1e-15 to 0, drops the sign of `-0.0`, and cannot hold NaN,
±Infinity or anything at or above 1e15, so those raise (`simplify_expressions` at planning over
literals, an Arrow cast error over columns). RePark's only counter-seat, `widen_decimal_against_float`
(idx 6), covers a decimal **literal** in four comparison shapes. The table
([domain-table.md](domain-table.md)) confirms the 2026-11-11 placement and widens it: the defect is in
every unifying construct, on the Spark door and the DataFrame API, in both ANSI modes, and it writes
wrong bits into Iceberg tables through `INSERT … SELECT`, `MERGE`, `UPDATE` and CTAS.

## What the table says (summary; the table has the cells)

- **Size:** 4,436 compared cells (1,985 SQL and 203 DataFrame cells in each ANSI mode, 30 Iceberg
  writes in each mode), the 612-cell #888 replay, and the 1,985 native-door cells for information.
- **Wrong writes: 28** (14 per mode, the same 14 in both): `INSERT … SELECT … UNION [ALL]` (1e-20 and
  `-0.0` stored as `0.0`), `INSERT` of a `coalesce` and of an `array` element, a FLOAT target, `UPDATE …
  SET CASE` and `SET coalesce`, `MERGE` from a `UNION` source and from a `VALUES` source (matched and
  inserted rows), `MERGE … SET CASE`, `MERGE … INSERT VALUES (coalesce(…))`, CTAS (the column is
  **created** `decimal(30,15)`), and the DataFrame `union(…).writeTo(…).append()`. Seven more write
  cells raise where Spark writes (1e30, NaN, the reversed union, `greatest`, `IF` in `UPDATE`, a `MERGE`
  with 1e30).
- **Wrong reads: 688** (344 per mode): 306 SQL and 38 DataFrame cells per mode return another number,
  lose the sign of zero, or count differently (`0.0 IN (dd)`, an equi-join of `0.0` with a `1e-20`
  column, `F.col("dd").isin(Decimal("0.0"))`); 395 more per mode raise where Spark answers (368 SQL, 27
  DataFrame).
- **What is already right:** `INSERT … VALUES` (each cell is cast to the target column at planning),
  arithmetic with a DOUBLE, `nvl` / `ifnull` in SQL (DataFusion's signature casts the decimal to
  `double`), the comparison and `IN`-list shapes with a decimal **literal**, and the `F.expr` fragments
  (the `F.expr` context parses `0.1` as `double`, which is itself a literal-typing divergence, not this
  root). `selectExpr`, string `filter`, `F.nvl` and `F.ifnull` are WRONG.
- **The placement holds:** the defect is where the 2026-11-11 probe put it (`decimal_coercion`), and
  wider: every unifying construct, both doors, both modes. The halt condition does not apply.
- **One measured trap for the build:** `ins_union_dec_round` is EQUAL today only because the union
  keeps `1.255` as a decimal. Spark stores the double through its own double-to-decimal cast (1.26);
  RePark's direct double-to-DECIMAL(10,2) store rounds the binary value to 1.25
  (`ins_values_dec_round`, WRONG on main). Once the union is `double`, that cell turns WRONG unless the
  store cast rounds as Spark does (Q5).

## Seat proposals

Two, after the inventory. The #888 lesson decides between them: **the common type must be decided
before `type_coercion`, never repaired after it**, so no parent is ever coerced against a type a later
rule changes.

### Proposal 1 (recommended): one pre-coercion Spark common-type rule, `SparkDecimalFloatWiden`

- **Where the type is decided:** before `type_coercion`, at the `spark_integral_literal` seat (idx 6),
  as a second pass of the same rule or a sibling rule inserted next to it by
  `insert_literal_rule_before_coercion` (`crates/repark-spark/src/spark_literal_typing.rs:42-53`), which
  seats it in **both** production lists that matter: the session list (inventory B) and the `F.expr`
  list (inventory C, `expr_build.rs:297`). For each unifying context it computes the operand types and,
  when any operand is FLOAT or DOUBLE and any is DECIMAL, casts every DECIMAL operand (literal, column or
  expression) and every FLOAT operand to `Float64`. DataFusion's `type_coercion` then sees only floats
  and answers `Float64` through `numerical_coercion`, exactly Spark's answer. The contexts, from the
  inventory: `Union` inputs per column (a projection over each input, the shape
  `timestamp_unit_widen.rs:417-475` already uses), `Values` cells (rebuilt through
  `LogicalPlanBuilder::values`, the shape `rewrite_values` `:175-195` already uses, because D1 fixed the
  schema at planning), CASE THEN/ELSE and `if`, the shared-argument functions (`coalesce`, `nvl2` after
  the first argument, `greatest`, `least`; `array` / `make_array` and `map` values if the
  owner folds containers, Q3), comparisons, `IN` lists, `BETWEEN` and simple-CASE `WHEN` for any decimal
  operand (generalizing `widen_decimal_against_float` from literals to columns and expressions), the
  `IN (subquery)` operand against the subquery's output column, equi-join key pairs, and the two inputs
  of INTERSECT / EXCEPT (both projected, so the output type is `double` as in Spark, not the left
  side's). **`nullif(a, b)` is the one exception:** Spark compares in `double` but returns `a`'s own
  type (measured: `nullif(<decimal>, <double>)` is `decimal(p,s)` on Spark, the `nullif_lr` row), so the
  rule widens only the comparison, as `CASE WHEN CAST(a AS DOUBLE) = CAST(b AS DOUBLE) THEN NULL ELSE a
  END` or an equivalent, never `a` itself.
- **How parents see the new type:** the rule runs bottom-up with `recompute_schema`, before any
  coercion, so every parent is coerced once, at idx 10, against `Float64`. No post-coercion rule re-types
  anything, which removes the #888 fault by construction. Two planner-time casts exist above the
  rewritten nodes and stay valid because they cast from whatever the child is: `INSERT`'s cast to the
  target column (inventory D3) and a user `CAST(c AS …)`. The second `type_coercion` (idx 28) is stable
  on a `Float64` union. The Spark door analyzes twice (eager, then execute), so the rule must be
  idempotent: after one pass no decimal sits beside a float, and the gate is false.
- **Composes with `widen_decimal_against_float`:** it subsumes it. The literal-only comparison widening
  (`spark_literal_typing.rs:264-427`) retires into the general rule in the same PR, so there is one
  decision, not two that can disagree. Its four pins (`decimal_eq_double_casts_the_literal` and the
  three siblings, `:799-938`) move to the new rule unchanged.
- **The nvl family (from #888):** `nvl` and `ifnull` keep DataFusion's `Signature::uniform` over
  `SUPPORTED_NVL_TYPES` (no decimal in the list), which happens to answer `double` here; the rule must
  not change them (the two #888 cells that share this root stay EQUAL), and the 26 #888 cells where that
  same signature turns `nvl(decimal, integral|decimal)` into `double` stay with NVL-TYPE-COERCION-2.
  `nvl2`, `nullif` and `coalesce` are DataFusion UDFs with their own coercion and are in scope. On the
  DataFrame API `F.nvl` / `F.ifnull` do not reach the `nvl` UDF (measured WRONG where the SQL spelling is
  EQUAL); the build reads the facade path before it decides whether the rule must cover them.
- **Gate and cost:** the rule must not walk expression types on a plan with no decimal or no float.
  The gate is one pass over node schemas and literals (a decimal field or literal **and** a float field
  or literal anywhere in the plan); only then does it compute operand types. Expected product files:
  `crates/repark-spark/src/spark_literal_typing.rs` (the insertion and the retirement) and a new module
  beside it (`spark_literal_typing/decimal_float.rs`, so the 962-line file does not grow), the
  `crates/repark-spark/src/map.md` rows, a Rust unit-test module, the facade pins
  (`python/repark/tests/sql/test_decimal_double_union_coercion_1.py` and a recorded Spark JSON), the
  staging ledger. **Cost cases a build must run** (the 1.05x gates): the C-012 like-for-like set; a
  decimal-free plan of 200 projections (gate false); a 200-branch `UNION ALL` mixing a decimal and a
  double column (every branch projected); a 10,000-row `VALUES` with one double cell (the rebuild is
  O(rows × columns)); a 50-deep nested CASE over a decimal and a double column; a 1,000-column projection
  of decimal and double columns; and the facade suite wall time.
- **Risks:** (1) a `Union` the rule rebuilds is rebuilt again by `repark_strip_attribute_ids`
  (`attr_id.rs:729-784`) and by `spark_integral_literal`'s stale-union path (`:120-135`); both derive
  the schema from the inputs, which the rule has already typed, so both keep `Float64` — the build pins
  it. (2) `struct_setop_names` (idx 11) and the struct pairing rules see union inputs the rule wrapped
  in a projection; a struct column is never a decimal-beside-float scalar, but the build runs the struct
  set-operation suites. (3) INSERT into a DECIMAL column from a union now stores through a `double`
  (Spark's path), so the store-assignment rounding and overflow follow Spark's double-to-decimal cast;
  the table's DECIMAL(10,2) write cells pin it.

### Proposal 2: change the decision itself, a patched `decimal_coercion`

- **Where:** inventory A1, `datafusion-expr-common` `binary.rs:1039`, through a `[patch.crates-io]` fork
  of `datafusion-expr-common` 54.1.0 (the way the iceberg-rust family is patched today): decimal beside
  FLOAT or DOUBLE answers `Float64`. Both D1 (`VALUES` at planning) and idx 10 call it, so the type is
  right from the first plan, with no rebuild and no rule.
- **How parents see it:** they never see a decimal; the #888 fault cannot arise.
- **Composes:** `widen_decimal_against_float` becomes dead and is deleted.
- **Product files:** `Cargo.toml` `[patch.crates-io]`, `Cargo.lock`, a maintained fork branch of the
  DataFusion crate, the same pins as proposal 1.
- **Cost:** zero planning cost. **Against it:** it breaks the version-pin contract's family freeze
  (`Cargo.toml` pins `rp-3-fork-repin/C-001, C-002`) and adds a fork to carry through every DataFusion
  bump; it is not door-aware, so it also changes the native door and the Rust-embedded ANSI door (whose
  contract is DataFusion semantics) and every internal caller of `comparison_coercion` (pruning,
  statistics, join planning); and it is not mode-aware, which this fault does not need but the nvl family
  (NVL-TYPE-COERCION-2) does, so that unit would still need a RePark seat.

### Rejected: a post-coercion repair

Rewriting `CAST(<float> AS Decimal128(30,15))` back to `Float64` after `type_coercion` is the #888 shape:
every parent has already been coerced against the decimal type. Not proposed.

## The unit split

**One unit, DECIMAL-DOUBLE-UNION-COERCION-1, proposal 1, in two slices in one PR**, both inside the
scoped label (Spark door, scalar columns, the writes first):

1. **S1 — set operations, `VALUES` and the writes they feed:** UNION / UNION ALL / UNION DISTINCT, the
   DataFrame `union` / `unionByName`, INTERSECT / EXCEPT DISTINCT and `intersect` / `subtract`, multi-row
   `VALUES` (and `INSERT … SELECT … FROM VALUES`), the CTE, temp-view and subquery forms; pinned by the
   wrong-write cells (`INSERT`, `MERGE` sources, CTAS) first.
2. **S2 — expression unification and comparison:** CASE / `IF`, `coalesce`, `nvl2`, `nullif`, `greatest`,
   `least`, comparisons, `IN` lists, `BETWEEN`, equi-join keys, the DataFrame column functions; retires
   `widen_decimal_against_float`; pinned by the `UPDATE … SET CASE`, `MERGE … SET CASE` and the
   join-count cells.

**Carded, with the measured answers in the table:**

- **DECIMAL-DOUBLE-CONTAINER-1** — `array(…)` and `map(…)` element and value types (WRONG and overflow on
  the Spark door; outside the scalar label). Same seat; folds into S2 if the owner says so (Q3).
- **DECIMAL-FLOAT-ARITH-1** — `decimal + FLOAT` answers `float` (DataFusion `numerical_coercion`,
  inventory A6) where Spark answers `double`: the `ctl_add/*/flt_*` cells. A different seat (arithmetic).
- **NUMERIC-STRING-UNION-CONTROLS** — the control operands diverge on their own roots, mode-dependent
  (a string digit beside a double, an integral beside a FLOAT, NULL in set operations); recorded, not
  this unit.
- **IN-SUBQUERY-PROJECTION-1** — `x IN (SELECT …)` in a select list raises on RePark for every operand
  class (`Physical plan does not support logical expression InSubquery`); not this root.
- Native door: bare decimal literals parse as `double` there (DataFusion), so most native cells are
  EQUAL by accident; DECIMAL columns and `CAST(… AS DECIMAL)` still fault. Outside the label (Q2).

## Owner questions

Each has a default; the build proceeds on the default unless the owner rules otherwise.

- **Q1 — the seat.** Proposal 1 (a pre-coercion RePark rule, retiring `widen_decimal_against_float`) or
  proposal 2 (a patched `decimal_coercion`, breaking the family freeze)? **Default: proposal 1.** Rests on
  the inventory (two decision points, both upstream of every write) and on the native-door matrix
  (proposal 2 would move the native door, whose contract is DataFusion's).
- **Q2 — the native door.** Leave `repark.sql()` on DataFusion's answer, or route it through the same
  rule? **Default: leave it, record it.** Rests on the native matrix: bare decimal literals are already
  `double` there (EQUAL), and only `CAST(… AS DECIMAL)` and DECIMAL columns fault.
- **Q3 — containers.** Do `array(…)` and `map(…)` ride in S2 (same seat, about 96 more cells per mode)
  or wait for DECIMAL-DOUBLE-CONTAINER-1? **Default: card them; fold into S2 only if the S2 cost runs
  stay inside 1.05x with containers in.** Rests on the `array` and `map_value` rows and
  `ins_array_elem_tiny` (a wrong write through an array element, outside the scalar label).
- **Q4 — stored Iceberg views.** A view a pre-fix build created stores `decimal(30,15)` in its view
  schema. After the fix, does the stored schema win (the view keeps reading a decimal until re-created)
  or the re-planned `double`? **Default: the stored schema wins, as Spark binds a view to its stored
  schema; the release note tells the user to re-create such views.** Rests on inventory F2; not measured
  across builds here (one build only).
- **Q5 — the double-to-DECIMAL store rounding.** Spark stores a DOUBLE 1.255 into DECIMAL(10,2) as 1.26,
  RePark as 1.25 (`ins_values_dec_round`, WRONG on main, both modes). This unit would expose the same
  rounding on the union path (`ins_union_dec_round`, EQUAL today). Fix the store cast in this unit, or
  card it and accept that one cell turns from EQUAL to WRONG? **Default: card it as a separate S1
  (DOUBLE-DECIMAL-STORE-ROUND-1) that lands first, and pin `ins_union_dec_round` in this unit as the
  guard.**
- **Q6 — FLOAT arithmetic.** `decimal + FLOAT` answers `float` (Spark `double`): card
  (DECIMAL-FLOAT-ARITH-1) or fold into S2? **Default: card.** Rests on the `ctl_add` rows with the two
  FLOAT operands.

Up: [map.md](map.md).
