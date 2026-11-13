# DECIMAL-DOUBLE-UNION-COERCION-1 — the design sketch (measured; no product code)

**Author:** Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh, 2026-11-13. **Rulings carried:** owner
"Defaults" of 2026-11-13 (about 12:12 UTC): an xhigh design sketch first, the build on the next free lane;
#888 (NVL-TYPE-COERCION-1) closes and its table folds in here; one xhigh critic reviews
[inventory.md](inventory.md) before any code. **Measured:** RePark main `04d84ab9` (the
`pkg-04d84ab9` package, Spark door, DataFrame API and native door) against Spark 4.1.2, ANSI on and
off; Iceberg 1.11.0 runtime for the writes. Provenance: [evidence/map.md](evidence/map.md). **Revised the same day** after the xhigh inventory
critic (`VERDICT: GAPS`): its doors re-measured in both modes, a second seat for `VALUES`, the
INTERSECT / EXCEPT discriminator, the RecursiveQuery exclusion, the signed-zero ordering, cost and
pruning, and rewritten questions.

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

## What the table says (revised after the inventory critic)

- **Size:** 4,540 compared cells in both ANSI modes: 1,985 SQL, 203 DataFrame and 30 write cells per mode
  in the first pass; 28 select and 24 write cells per mode in the revision. Also the 612-cell #888 replay
  and 1,985 native-door cells, for information. Every root cell has the same verdict in both modes.
- **Wrong writes: 66** (33 per mode).
  - First pass, 14 per mode: `INSERT … SELECT … UNION [ALL]`, `INSERT` of a `coalesce` and of an
    `array` element, a FLOAT target, `UPDATE … SET CASE` and `SET coalesce`, `MERGE` from a union or a
    `VALUES` source, `MERGE … SET CASE`, `MERGE … INSERT VALUES (coalesce(…))`, CTAS (the column is
    created `decimal(30,15)`), the DataFrame union append.
  - Revision, 19 per mode: `INSERT OVERWRITE` and `INSERT … BY NAME` from `VALUES`, `SELECT … FROM
    VALUES` and a union; `INSERT … VALUES` into STRING and DECIMAL(38,18) targets; RTAS; `DELETE`,
    `UPDATE … WHERE` and `MERGE … ON` with a decimal column against a double column (copy-on-write and
    merge-on-read); `MERGE … ON` of `-0.0` against `0.00`; DataFrame `insertInto` and
    `overwritePartitions`.
- **Wrong reads: 702** (351 per mode): the first pass's 344, plus struct, array and map leaves in
  `UNION` and `CASE` and a `VALUES` of a double beside a `CAST(… AS DECIMAL(30,15))`. Another 395 cells
  per mode raise where Spark answers.
- **Already right:** `INSERT … VALUES` into DOUBLE, FLOAT and BIGINT targets only (the first pass said
  `INSERT … VALUES` without the target condition; the STRING and DECIMAL(38,18) cells correct it);
  arithmetic with a DOUBLE; SQL `nvl` / `ifnull`; comparisons with a decimal literal; the `F.expr`
  fragments.
- **The placement holds** (`decimal_coercion`), and a third mechanism joins it: a target-typed `VALUES`
  forms no common type at all (inventory D2, D3, G).
- **Measured traps for the build:** the store rounding (`ins_union_dec_round`, Q5′); eight signed-zero
  shapes that turn from EQUAL to WRONG when widened before SIGNED-ZERO-NORMALIZE-1 part B; a user cast
  to the common type in `VALUES` that must survive (`values_B_user_cast`); user `LEFT SEMI` / `LEFT
  ANTI JOIN` output types that must not move; recursive CTEs, which Spark does not widen.

## The revised design: two seats, both before `type_coercion`

The #888 lesson still decides: the common type is fixed **before** `type_coercion`, never repaired after
it, so every parent is coerced once against the final type. The critic showed one analyzer rule cannot do
it alone: `infer_data` / `infer_inner` cast every multi-row `VALUES` cell at SQL planning, and the
planner's cast is byte-identical to a user's (`values_A_planner_cast` and `values_B_user_cast` print the
same initial plan; Spark answers `double` for A and `decimal(30,15)` for B). So `VALUES` gets its own seat,
upstream of planning.

### Seat V — `VALUES`, marked at the Spark-door parse and typed at the head of the analyzer list

- **Marker.** At every Spark-door seat that plans user SQL text (inventory G, "the planning seats a
  VALUES marker must reach": the passthrough in `column_resolution.rs`, which `INSERT OVERWRITE` and
  `BY NAME` reach through `execute_passthrough`; `merge_fragments.rs` for the MERGE source; the CTAS and
  RTAS query; the temp-view and Iceberg-view re-planning), one AST pass wraps each cell of a multi-row
  `VALUES` in a no-op marker UDF registered on the Spark-door session (`__repark_values_cell(x)`, return
  type = argument type). DataFusion then plans `CAST(marker(x) AS T)` for its own cast, while a user cast
  stays inside: `marker(CAST(x AS T))`. The existing `SetExpr::Values` rewrites (`insert_arity.rs:53`,
  `void_type.rs:131`, `insert_timestamp_ns.rs:237`) show where the pass sits.
- **Consumer.** An analyzer rule seated **first** in the Spark-door list, ahead of
  `spark_timestamp_unit_widen` (idx 1, whose `widen_values_dates` reads `Values` cells and must never see a
  marker). Per `Values` column it reads the marker arguments' types. If a DECIMAL and a FLOAT or DOUBLE
  are among them, Spark's common type is `double`; otherwise it keeps the planner's type. It then emits:
  - **standalone `VALUES`** (D1, G1, G2, the MERGE source, CTAS / RTAS): `CAST(x AS Float64)` per cell
    and a rebuild through `LogicalPlanBuilder::values`, so the node and every parent (`recompute_schema`)
    read `double` before idx 10. This fixes `values_A`, `values_C` (`double` 0.0, 2.0, Spark's answer) and
    every `VALUES` row of the matrices, and leaves `values_B` unchanged (both marker arguments are already
    `decimal(30,15)`).
  - **target-typed `VALUES`** (D2, and D3's positional leak): `CAST(CAST(x AS Float64) AS <target>)`, the
    cast through the common type in Spark's order (inline table first, then the store cast). The node
    keeps the target schema, so `insert_to_plan`'s projection, which added no cast because the types
    matched (`statement.rs:2475-2480`), stays valid. This fixes `values_string`, `select_values_string`
    and `values_dec38`, and keeps `values_float`, `values_bigint` and the `ins_values_*` cells EQUAL.
  - **no mix:** the planner's own cast, markers dropped. The plan is identical to today's.
- **Idempotent and closed:** after the consumer no marker remains, so the second analyzer pass is a no-op.
  The consumer refuses (a plan error naming the unit) if a marker appears outside a `Values` cell, so a
  seat that marks without consuming fails loudly instead of answering.
- **Out of reach, on purpose:** the native door (no marker; Q2) and generated SQL in
  `crates/repark-iceberg/src/write/` that wraps a source the passthrough already typed (the build's
  inventory step confirms each hit).

### Seat E — `SparkDecimalFloatWiden` at idx 6 (proposal 1, revised)

- **Contexts:** loose `Union` inputs per column; CASE THEN/ELSE and `IF`; `coalesce`, `nvl2` after the
  first argument, `greatest`, `least` (and `array` / `map` constructor arguments, Q3′); comparisons, `IN`
  lists, `BETWEEN`, simple-CASE `WHEN` and the `IN (subquery)` operand for any decimal operand (literal,
  column or expression); equi-join key pairs. `nullif` widens only its comparison and keeps its first
  argument's type. The DML predicates of inventory G3 (`DELETE`, `UPDATE … WHERE`, `MERGE … ON`) plan
  through the session list, so they follow without a seat of their own.
- **INTERSECT / EXCEPT discriminator:** there is no set-operation node, only a `LeftSemi` / `LeftAnti`
  join. The rule widens it, projecting **both** inputs so the output is `double` as in Spark, only in the
  shape `struct_setop::refuse_in_join` already recognizes
  (`crates/repark-core/src/session/df_guards/struct_setop.rs:116-160`): a `Distinct` left input,
  `NullEqualsNull`, no filter, as many `on` pairs as either side has fields, and every pair a positional
  key column. The ALL forms refuse at the statement seat since #1314, so `Distinct` suffices. A user `LEFT
  SEMI` / `LEFT ANTI JOIN` (`NullEqualsNothing`) does not match; only its key comparison widens, and its
  output keeps the left type (`semi_join_left_type`, `anti_join_left_type` pin it).
- **RecursiveQuery excluded:** Spark keeps the anchor's type (`rcte_dbl_anchor_one_col`) and refuses a
  decimal anchor with a double recursive term (`CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`). The planner already
  casts the recursive term to the anchor (`builder.rs:176-194`). The rule widens expressions inside each
  term but never the anchor/recursive unification.
- **Nested leaves excluded** (inventory G6, and B idx 9's narrow struct case): the rule casts top-level
  operands only. The six nested cells are pinned **unchanged** and carded (DECIMAL-DOUBLE-NESTED-1).
- **Composes with `widen_decimal_against_float`:** subsumes and retires it (`spark_literal_typing.rs:264-427`);
  its four pins move to the new rule.
- **The gate, restated:** a plan-wide "a decimal and a float are present" gate stays true on **every**
  query over a table holding a DECIMAL and a DOUBLE column, and on both analyzer passes
  (`spark_ast.rs:273` and `:295`). The rule therefore decides per node: it shares
  `spark_integral_literal`'s existing walk at idx 6 and computes operand types only at the unifying
  nodes listed above, so a plan with no unifying construct pays one type lookup per such node, not per
  expression.

### What a patched `decimal_coercion` (proposal 2) would and would not cover

- **Would cover:** D1 `VALUES` (`values_A`, `values_C`; `values_B` stays EQUAL); every idx-10 context,
  including the DML predicates and the nested leaves (the union and comparison coercions recurse into
  struct, list and map fields) and B idx 9's struct operands; CTAS and RTAS.
- **Would not cover:** the target-typed `VALUES` (D2, D3: no common type is ever formed, so `values_string`,
  `select_values_string` and `values_dec38` stay WRONG; it still needs seat V's target branch); the
  INTERSECT / EXCEPT output type (the join keeps the left side's type; only the keys change); the
  signed-zero flips (the same eight shapes break). It also moves the native and the Rust-embedded ANSI
  doors and every DataFusion-internal caller of `comparison_coercion`, and breaks the family freeze.
- **Verdict:** not the primary seat. It is the cheapest way to the nested leaves, which is why Q3′ cards
  them instead of asking for it.

### Ordering: after SIGNED-ZERO-NORMALIZE-1 part B and the store rounding

- **The build starts only after SIGNED-ZERO-NORMALIZE-1 part B (key sites) merges.** Its pins are the
  eight shapes the revision measured EQUAL today and WRONG widened, in both modes: `zero_eq`,
  `zero_join`, `zero_intersect`, `zero_except`, `zero_union_distinct`, `zero_group_by`,
  `zero_in_subquery`, `delete_zero`. The build re-measures them on part B's head before it writes a
  line; each must be EQUAL in its widened form there.
- **`merge_on_zero`** moves the other way (WRONG today, EQUAL widened) and becomes a pin of the fix.
- **`ins_union_dec_round`** turns WRONG unless the double-to-DECIMAL store rounds as Spark does, so
  DOUBLE-DECIMAL-STORE-ROUND-1 lands first (Q5′).

### Cost and pruning

- **Measured on main:** no decimal-column predicate reaches the Iceberg scan. DataFusion would unwrap a
  decimal cast (`casts.rs:80-98`), but the fork converts no decimal literal to an Iceberg datum
  (`expr_to_predicate.rs:610-643`), so `d = 0.5`, today's `d = CAST(0.5 AS DOUBLE)` and the widened
  `CAST(d AS DOUBLE) = 0.5` all scan three of three files. A double column prunes (`v = 0.5`, one file).
  Widening therefore loses no pruning today. Widening a decimal **literal or expression** beside a double
  column folds to a double literal and pushes, so those predicates gain pruning.
- **Keeping it later:** if the fork learns decimal datums, the widened decimal-column predicate would
  forgo pruning the decimal form would get. The design answer is an implied, pushdown-only conjunct: for a
  widened `CAST(d AS DOUBLE) op <double constant>` inside a filter, also push `d` against the constant
  rounded outward to `d`'s scale, a superset, while the double predicate stays the exact filter. It is
  not built now (Q7); the build pins the measured "no decimal pushdown" rows so the day the fork changes,
  the pin fails and the conjunct is built.
- **Cost cases a build must run** (the 1.05x gates):
  - the C-012 like-for-like set;
  - a decimal-free 200-projection plan;
  - a wide plan over a table with DECIMAL and DOUBLE columns and **no** unifying construct, through both
    analyzer passes;
  - a 200-branch `UNION ALL` mixing a decimal and a double column;
  - a 10,000-row `VALUES` with one double cell (the marker pass and the consumer's rebuild);
  - a 50-deep CASE over a decimal and a double column;
  - a 1,000-column projection of decimal and double columns;
  - a pruned Iceberg scan, a double column against a decimal literal (one of N files, unchanged or
    better) and a decimal column against a double (N of N, unchanged);
  - a `DELETE` with a decimal-column predicate against a double on a multi-file table (files rewritten,
    unchanged);
  - the facade suite wall time.

### Rejected: a post-coercion repair

Rewriting `CAST(<float> AS Decimal128(30,15))` after `type_coercion` is the #888 shape, and for `VALUES` it
cannot tell a planner cast from a user cast. Not proposed.

## The unit split (revised)

**One unit, DECIMAL-DOUBLE-UNION-COERCION-1: seat V and seat E, two slices in one PR, opened only after
SIGNED-ZERO-NORMALIZE-1 part B and DOUBLE-DECIMAL-STORE-ROUND-1 merge.** Both slices are inside the
scoped label, and both carry wrong writes.

1. **S1 — seat V and the set operations, with the writes they feed:** standalone and target-typed
   `VALUES` (`SELECT`, `INSERT … VALUES` into any scalar target, `INSERT OVERWRITE`, `BY NAME`, the MERGE
   source, CTAS and RTAS); UNION, UNION ALL, `union` / `unionByName`; INTERSECT / EXCEPT DISTINCT with the
   discriminator; the DataFrame `insertInto`, `overwritePartitions` and append.
2. **S2 — seat E, expressions and predicates:** CASE / `IF`, `coalesce`, `nvl2`, `nullif`, `greatest`,
   `least`, the `array` / `map` constructors (Q3′); comparisons, `IN`, `BETWEEN`, equi-join keys; **the
   predicate writes** (`DELETE`, `UPDATE … WHERE`, `MERGE … ON`, copy-on-write and merge-on-read), which
   make S2 label-blocking, not read-only.

**Carded, with the measured answers in the table:**

- **DECIMAL-DOUBLE-NESTED-1** — nested struct, array and map leaves in `UNION` and `CASE` (six cells per
  mode) and B idx 9's struct operands; pinned unchanged by this unit.
- **DOUBLE-DECIMAL-STORE-ROUND-1** — RePark stores the double 1.255 into DECIMAL(10,2) as 1.25, Spark as
  1.26 (`ins_values_dec_round`); a prerequisite (Q5′).
- **DECIMAL-FLOAT-ARITH-1** — `decimal + FLOAT` answers `float`, Spark `double`.
- **INSERT-SOURCE-VALUES-ARITY-1** — the target schema leaks by position into a narrower `VALUES` inside an
  `INSERT … SELECT` and raises (`ins_case_tiny`).
- **RECURSIVE-CTE-JOIN-ERROR-1** — the two-column recursive CTEs raise `Join Error` on RePark.
- **IN-SUBQUERY-PROJECTION-1** — `x IN (SELECT …)` in a select list raises for every operand class.
- **NUMERIC-STRING-UNION-CONTROLS** — the control operands' own roots, mode-dependent.
- The native door: recorded only (Q2).

## Owner questions (revised; each names the question it replaces)

- **Q1′ (replaces Q1) — the seat pair.** Seat V (an AST marker consumed at the head of the list) plus
  seat E (the idx-6 rule), or a patched `decimal_coercion` plus seat V's target branch? **Default: seat V
  plus seat E.** Rests on `values_A` / `values_B` / `values_C`, `values_string`, `values_dec38`, the
  semi / anti join pins, and the native matrix (a patch would move that door).
- **Q2 (unchanged) — the native door.** **Default: leave it on DataFusion's answer and record it.** Rests
  on the native matrix: bare decimal literals are already `double` there.
- **Q3′ (replaces Q3) — containers and nested leaves.** Fold the `array` / `map` **constructors** into S2
  and card the **nested leaves**? **Default: yes.** The default for constructors moves from "card" to
  "fold" because `ins_array_elem_tiny` is a scalar wrong write inside the label, and the constructors are
  arguments of the same walk. Rests on the `array` and `map_value` rows, `ins_array_elem_tiny`, and the
  six nested revision cells.
- **Q4 (unchanged) — stored Iceberg views.** A pre-fix view stores `decimal(30,15)`. **Default: the stored
  schema wins, as Spark binds a view to its stored schema; the release note says to re-create.** Rests on
  inventory F2 (not measured across builds).
- **Q5′ (replaces Q5) — prerequisites.** Does the build wait for SIGNED-ZERO-NORMALIZE-1 part B and
  DOUBLE-DECIMAL-STORE-ROUND-1? **Default: yes, both.** The nine shapes that would otherwise turn from
  EQUAL to WRONG are its pins. Rests on the zero table, `delete_zero`, `ins_union_dec_round` and
  `ins_values_dec_round`.
- **Q6 (unchanged) — FLOAT arithmetic.** **Default: card (DECIMAL-FLOAT-ARITH-1).** Rests on the
  `ctl_add` FLOAT rows.
- **Q7 (new) — pruning.** Build the pushdown-only decimal conjunct now, or when the fork learns decimal
  datums? **Default: later; pin today's "no decimal pushdown".** Rests on the four `explain/*` rows.
- **Q8 (new) — `MERGE … ON` of `-0.0` against `0.00`.** Spark 4.1.2 with Iceberg 1.11.0 does not match it,
  although `-0.0 = 0.00` answers true in a `SELECT`. Follow Spark's `MERGE` (the widened form already
  does), or treat it as a Spark quirk? **Default: follow Spark, and pin both.** Rests on
  `merge_on_zero_today`, `merge_on_zero_widened` and `zero_eq_today`.

Up: [map.md](map.md).
