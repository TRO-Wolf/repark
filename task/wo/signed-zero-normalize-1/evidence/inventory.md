# Item 1 for SIGNED-ZERO-NORMALIZE-1 part A: rebase, re-measure, additions, inventories

Builder `claude-opus-5-5`, 2026-11-11, on `fix/signed-zero-normalize-1` at the order commit `0c18b6e8` (main
`68b09d18`, #1295 merged). Docs and probes only; no product code. The base build is the clone's `.venv` release
build of #1295, whose product code equals main. The DataFusion source is the pinned 54.1.0 from the cargo registry,
and the fork is the pinned `66901f02` (`Cargo.lock:3301-3303`, unchanged).

Cells are cited by id: `c…`, `m…` from Table B, `E…`/`R…` from the extension table, `D…` from
[domain-table.md](domain-table.md), and this item's `A1-…` (integer-literal spelling), `A2-…` (`PREPARE` /
`EXECUTE`), `A3-…` (simple `CASE`) and `X-div-…` (the legacy division guard). The raw legs and the runners are in
[item1/](item1/map.md).

## 1. The sketch's citations on today's main

Every RePark `file:line` in sketch §0–§2, §4–§6 and §9–§11 was opened at `68b09d18`. Lines that moved or whose code
changed:

| Sketch cite | Today | What changed |
|---|---|---|
| `df_guards.rs:18-30` (submodules) | `:18-33` | unit C added `struct_setop` (and its `pub use`) and `struct_walk` |
| `df_guards.rs:92-96` (`register_repark_isnan` in the context builder) | `:98` (builder `:70-101`) | moved by the three physical-rule lines |
| `df_guards.rs:89` (`ParallelWindowRule`) | `:92` | moved |
| `df_guards.rs:100-103` (base analyzer list) | `:103-107` | **changed**: the list is now `insert_struct_setop_after_coercion(window_rescan list)`, so unit C's `struct_setop_names` sits right after `type_coercion` |
| `df_guards.rs:106-127` (`unnest_safe_optimizer_rules`) | `:110-131` | moved; same `match rule.name()` shape, so seat 1b is one new arm on `"simplify_expressions"` |
| `repark-spark/src/extension.rs:135-144` (`configure_analyzer_rules`) | `:137-146` | **changed**: #1292/#1295 insert `struct_operands_by_position` before `type_coercion` |
| `repark-spark/src/extension.rs:149-164` (`register`) | `:150-169` | **changed**: registers the `duplicate_field_operands` expression planner; `FoldSparkNumericCasts` is at `:164` |
| `repark-spark/src/extension.rs:17-21` | `:19-23` | moved |
| `expr_build.rs:263-298` (`build_expr_context`) | `:276-305` | **changed**: the `F.expr` list now also carries `insert_struct_setop_after_coercion` and `insert_expression_struct_position_rule_before_coercion` |

Unchanged at the cited lines: `session.rs:319-327` (`ext.register` `:323`, `on_session_built` `:327`; the file is
exactly at the 1000-line ceiling), `spark_literal_typing.rs:42-52, 140, 264-385` (`is_float_comparison` `:275-285`
still lacks `IsDistinctFrom`/`IsNotDistinctFrom`), `spark_typed.rs:18, 43-83`, `expr_build.rs:24-34, 46-72,
225-251`, `function_dispatch.rs:208, 212`, `conflict_filter.rs:67-72, 82-101, 160-228, 284-330`,
`meta_delete.rs:15, 225-248, 275-286`, `delete_update.rs:36-39`, `repark-sql/src/router.rs:243`, `isnan.rs:15-17`,
`freq_items.rs:153-160`, `is_duplicated.rs:138-152`, `repark-python/src/session.rs:26, 112-131, 150-166`,
`session_runtime.rs:404-427`, `bool_decimal.rs:22, 28-31`, `copy_on_write.rs:230-235`, `temp_views.rs:166`,
`temp_view.rs:82-87`, `pushdown.rs:149-160`, `codec.rs:97-98`, `window_rescan.rs:22-27, 51`, `subquery.rs:387`,
`dynamic_flatten.rs:356`, `generator.rs:760`, `predicate_dml.rs:223`, `merge/mod.rs:480`, `scan_batches.rs:99-109`,
and `repark-core/Cargo.toml` (no `repark-functions` edge).

### 1.1 The premise that does not hold (for a ruling)

Sketch §0.2 and §5 say the SQL-text translators "emit `equal_to(Datum::double(0.0))` and `is_in(...)` through the
shared `literal_datum`", and order design point 4 asks this unit to add a guard there. **On `68b09d18` (and on
`bd9c5437`: the two files have no diff) `literal_datum` already returns `None` for every float or double zero.**
`number_datum` keeps a `Float`/`Double` datum only when `is_exactly` holds (`conflict_filter.rs:317-330`), and
`is_exactly` needs `Decimal::parse`, which returns `None` when the literal has no significant digit
(`conflict_filter.rs:368-370`). So `0.0`, `-0.0`, `0`, `-0` and `0e5` all fail; a cast or any other expression is
already `None`. The guard has a test since `e7857f9f8` (ICE-OCC-SCOPED-1, 2026-09-18):
`a_zero_float_literal_leaves_the_filter_unscoped` (`f = 0.0` and `d = -0` give `TRUE`).

Consequences:
- The metadata-only DELETE already declines every float-zero predicate (`exact_predicate` fails), and the
  statement takes the row-level path. D1's RePark rows are row-level totalOrder misses, not metadata deletes, so
  seat 1 alone should move them.
- The conflict filter already widens a float-zero conjunct to `AlwaysTrue`.
- The guard is implicit: it is a side effect of the exactness check, and only the conflict-filter test holds it.

## 2. The RePark legs re-measured on main

The committed probes and runners, run on the base build (repark 1.5.6, `68b09d18` product code), compared with the
committed `fdc61f26` legs cell by cell (`item1/labels.py`, the probes' own label functions).

| Leg | Cells | Moves against `fdc61f26` |
|---|---|---|
| Domain reads (`probe_signed_zero.py --part reads`), every group | 533 | 0 |
| Domain DML (`--part dml`), every group | 177 | 2, both `UPDATE … WHERE d <=> 0.0` on `zsplit` (`D7-update-8`): `-cow` EQUAL → refused `Mismatch between schema and batches`; `-v3` refused → EQUAL. This is `UPDATE-SCHEMA-BATCH-MISMATCH-1` (cards.md) changing kind between runs |
| Table B (`probe_followup.py --table signed-zero`), c1–c7 and the rest | 190 | 0 |
| Extension E1, E3, E6, E7, R1–R3 (`item1/ext_replay.py`) | 109 | 0 |
| p15 (`probe_envelope.py --pids p15`) | 26 | 0 |

The extension's runner was never committed. `ext_replay.py` replays every committed RePark cell of those rows on the
same seeds. The E3 seed (`x_pi`) is reconstructed from the table's prose: two 10,000-row inserts with 1000-row pages,
`-0.0` on ids 1–1000, `0.0` on ids 19,001–20,000 and `1.5` elsewhere. It returns the committed answer: ids
19,001–20,000.

The base for every move in item 3 is these legs (`item1/raw/repark-68b09d18-*.jsonl`).

## 3. The order's additions, measured on Spark and main

Spark 4.1.2 (the oracle, under the JVM lock) and the base build, with `item1/add_rows.py` on the domain probe's
seeds and layouts. The truth is Spark on the inline `VALUES` view. Spark has no `PREPARE`, so the `A2` truth is
Spark's literal twin (`d = CAST('-0.0' AS DOUBLE)` and so on).

| Rows | What | Cells on main | Against the truth |
|---|---|---|---|
| A1-1…13 | integer-literal spelling on all five routes: `d = 0`, `d = -0`, `d <> 0`, `d <= -0`, `d >= 0`, `d < 0`, `d > -0`, `d IN (0, 1)`, `d BETWEEN -1 AND 0`, `0 = d`, `tf = 0`, `tf = -0`, `d <=> 0` | 65 | 50 wrong (10 forms × 5 routes), 15 EQUAL (`<= -0`, `> -0` and `BETWEEN -1 AND 0`: under totalOrder the bound already covers both zeros) |
| A2-1…7 | `PREPARE p(DOUBLE) AS … WHERE d = $1` with `EXECUTE p(0.0)` / `p(-0.0)`, `>=`, `<=`, `<>`, `IN`, and an `INT` parameter; native and the Spark door | 14 | 13 wrong, 1 EQUAL. The Spark door accepts `PREPARE` today (measured as is, ruling A). Its `-0.0` parameter parses as a decimal, so the sign is lost |
| A3-1…7, p1, p2 | simple `CASE d WHEN 0.0 …`, `WHEN -0.0`, `WHEN CAST('-0.0' AS DOUBLE)`, `WHEN 0`, a later zero arm, `tf`, a NaN arm; in `WHERE` on four routes and in a projection on two | 32 | 18 wrong, 4 EQUAL (the NaN arm), **10 refused on the Spark door and `filter(str)`** with `Cannot cast to Decimal128(30, 15). Overflowing on NaN` (see §8, F2) |

## 4. Insertion inventory (2026-11-09 rule), for seat 1 and seat 1b

The guarded property: every float comparison against a zero operand, written or produced, is answered with IEEE
equality (`-0.0 = 0.0`) at execution.

| # | Where evidence is created, moved or destroyed | Seat 1 | Seat 1b | Because |
|---|---|---|---|---|
| 1 | The session analyzer list | **yes**, last of the core-built rules | n/a | Installed from `ReparkSessionBuilder::build` after `ext.register` and `on_session_built`, so it runs after `type_coercion`, the Spark register-time rules and `FoldSparkNumericCasts`. Only the Python `install_shared_analyzer_rules` (integer overflow, bool→decimal) and the native `PlannedTimestampCast` follow it; none builds a float comparison |
| 2 | The `F.expr` list (`build_expr_context`) | **no, not needed** | no | `plan_expr_column` runs the analyzer only (`analyze_eagerly`), never the optimizer, so nothing folds there. The fragment is embedded in the consumer plan, and the consumer session's analyzer and optimizer run both seats at execution. The rewrite is a fixpoint, so a later pass over an already rewritten form is the identity. Held by the `f-expr` cells of every row |
| 3 | `create_view` stores the body after `apply_type_coercion` | **yes, at scan** | **yes** | Coercion only; the view is planned through the session state when it is scanned, which runs the full analyzer and optimizer. Spark-door temporary views replan through the session (`temp_views.rs:166`) |
| 4 | The bare optimizer in the `PREPARE` arm (`context/mod.rs:750-760`) | **no at `PREPARE`; yes at `EXECUTE`** | **yes, both** | `PREPARE` stores an optimized, unanalyzed plan; `d = $1` is not a zero then, but `push_down_filter` may copy it into `TableScan.filters`. `EXECUTE` substitutes literals (`:1582-1587`) and the plan runs the full analyzer and optimizer, so seat 1 must visit `TableScan.filters` as well as `Filter`. Rows `A2-*` |
| 5 | SQL-planner coercions: `to_recursive_query`, `INSERT` planning | **yes** | **yes** | Both cast projected values to a target type; neither builds a comparison. A comparison inside either is planned later like any other |
| 6 | `SimplifyExpressions` folds computed zeros (`d = 1 - 1.0`, `abs(-0.0)`, `CAST(0 AS DOUBLE) * 1`, native `CAST('-0.0' AS DOUBLE)`), and `unwrap_cast_in_comparison` inside it | n/a | **yes, right after it** | Seat 1b exists for these (D2). `unwrap_cast` may turn `CAST(tf AS DOUBLE) >= -0.0` into `tf >= Float32(-0.0)`; the bound form survives a narrowing because both bounds are exact in `Float32` |
| 7 | `OptimizeUnions` / `UnionsToFilter` | n/a | **yes** | Both run before `SimplifyExpressions`, so seat 1b sees the merged filter in the same pass |
| 8 | `PushDownFilter` into `TableScan.filters`, and its `simplify_predicates` | already rewritten | already rewritten | Seat 1b runs before `push_down_filter`. After the rewrite no float equality to a zero is left, so `simplify_predicates`' bound merges are exact |
| 9 | `ExtractEquijoinPredicate`, `EliminateFilter`, `CommonSubexprEliminate`, `OptimizeProjections` | n/a | n/a | A literal zero is never an equijoin key; the others neither create nor drop a comparison. `CommonSubexprEliminate` shares a non-column `e` that the bound form repeats |
| 10 | Stored plans re-entering: views, prepared plans, `cache()`, DataFrame temp views | **yes** | **yes** | Each re-enters through the session's analyzer and optimizer (rows 3 and 4); `cache()` evaluates the predicate once, under both seats |
| 11 | Bound `filter(str)` / `where(str)` and the typed Column API | **yes** | **yes** | Expressions built on the frame's session or by `PyColumn`, analyzed by the consumer session at execution |
| 12 | Native door (`PyReparkSession::native`) | **yes** | **yes** | The same builder with `NativeDoorTypes`; the install is in `build`, so every core session and a future server door inherit it |
| 13 | Metadata-only DELETE (`meta_delete.rs`) and the conflict filter (`conflict_filter.rs`) | separate seat | separate seat | SQL-text translators outside the plan. Both already refuse a float zero (§1.1), so the DELETE falls through to the row-level path, which carries both seats |
| 14 | Iceberg scan pushdown at the fork (`literal_binds_soundly`) | n/a | n/a | Zero-literal float predicates are not pushed (sketch §0.1; pin unchanged). The bound form keeps every file and page whose bounds meet `[-0.0, +0.0]`, so it stays exact if the guard is ever relaxed |
| 15 | Facade-synthesised comparisons: `reciprocal_trig_or_inf` (`expr_build.rs:24-34`) and the legacy division guard `nullif(divisor, 0.0)` (`repark-functions/src/analyzer.rs:252-267`) | **yes** for the trig `CASE`; **no** for `nullif` | as seat 1 | Seat 1 makes the trig `WHEN divisor = 0.0` IEEE, which flips `csc(-0.0)` to `+Inf`; design point 6 repairs it. The division guard is a `nullif`, a function-internal equality the bound form cannot reach (§6, §8 F3) |

**Order against today's rules (design point 3).**
- `struct_operands_by_position` (#1292/#1295, before `type_coercion`) and the binders'
  `duplicate_field_operands_by_position`: they relabel struct operands of struct comparisons and never produce a
  float operand. Seat 1 runs after them and skips every non-float comparison, so neither sees the other's output.
- `struct_setop_names` (unit C, right after `type_coercion` in the base list): it renames set-operation struct
  fields and builds no comparison. Seat 1 runs later and does not touch struct types.
- `SparkIntegralLiteral` (before `type_coercion`) widens a decimal literal against a float operand to
  `CAST(… AS DOUBLE)`, and `FoldSparkNumericCasts` folds it. Seat 1 runs after both, and its recogniser also
  accepts the unfolded cast.

## 5. Type-variant inventory (2026-11-09 rule)

**(1) Float representations that reach the seat**, measured on main with `item1/type_variants.py` (`d = 0.0`,
`d = -0.0` and `d = 0` on `filter(str)`, typed and `spark.sql` over a temp view).

| Source | Type at the seat | Seat 1 |
|---|---|---|
| Iceberg `double` / `float` | `Float64` / `Float32` | rewrites (`Float32` bounds for `tf`) |
| pandas `float64`, `float32`; polars `Float64`, `Float32`; a run-end-encoded array through pandas | `DoubleType` (`Float64`) | rewrites |
| pandas `float16` | `FloatType` in the schema. `spark.sql` and `filter(str)` `d = 0.0` refuse at `type_coercion` today (no `Float16`/decimal coercion), typed `== -0.0` answers `[4]` | typed: rewrites through `CAST(d AS DOUBLE)`. The SQL refusal is outside the seats (card F4) |
| pandas categorical of floats (dictionary) | `DoubleType`; pandas itself folds `-0.0` and `0.0` into one category | rewrites; nothing further to do |
| `Float16`/`Dictionary` from other producers | none found on any door | out (sketch §10: "Float16 and dictionary-encoded float keys") |

**(2) Pairs DataFusion's comparison coercion unifies with a float operand** (`comparison_coercion`,
`type_coercion/binary.rs:924`; arms at `analyzer/type_coercion.rs:616` (binary), `:625` (`BETWEEN`), `:660`
(`IN`), `:694` (`CASE`)):
- integer literal: the literal is cast, `d = CAST(Int64(0) AS Float64)`. Seat 1 reads through the cast (A1);
- decimal literal: the **float operand** is cast to the decimal type, so no float comparison remains. On the Spark
  door `SparkIntegralLiteral` widens the literal first for `= <> < <= > >=`, `IN` and `BETWEEN`; part A adds `<=>`
  and `IS DISTINCT FROM` (E6, R3) and, if ruled, simple `CASE` (F2). The native door parses `0.0` as `Float64`;
- string literal: the literal is cast, `d = CAST(Utf8('-0.0') AS Float64)`. Seat 1 evaluates the cast with
  `ScalarValue::cast_to` (the evaluation `FoldSparkNumericCasts` uses) and reads its sign.

**(3) What the rule does with each:** a zero operand is a `Float32`/`Float64` literal, a `Cast`/`TryCast` to
`Float32`/`Float64` of a literal whose cast value is a zero, or `Negative` of either. Anything else is left alone,
including a decimal comparison (no float operand) and any non-literal pair (part B).

## 6. Consumers of the pairing (Q4), from the DataFusion 54.1.0 source

Every consumer of float equality or ordering that the bound form cannot reach, with its disposition.

| Consumer | Where | Disposition |
|---|---|---|
| `type_coercion` `CASE` arm: simple `CASE e WHEN w` compares inside `CaseExpr`, with no `BinaryExpr` | `type_coercion.rs:694`; `physical-expr` `CaseExpr` | **part A seat** (A3): seat 1 expands a simple `CASE` whose `WHEN` holds a float zero literal into the searched form, whose `e = w` comparisons it then rewrites. Non-literal `WHEN` operands are part B |
| `nullif(d, 0.0)` and the legacy division guard `nullif(divisor, 0)` | `datafusion-functions` `nullif` (an `eq` kernel); `repark-functions/src/analyzer.rs:266` | `FUNC-FLOAT-EQ-1` (carded). The division guard is new evidence (§8 F3): `X-div-1` (`1.0D / CAST('-0.0' AS DOUBLE)`, ANSI off) is `-inf` where Spark answers NULL, and `X-div-2` (`%`) is `NaN` where Spark answers NULL |
| `greatest` / `least`, `approx_distinct`, `mode`, `median` ties | `datafusion-functions` and `functions-aggregate` | `FUNC-FLOAT-EQ-1` (D17) |
| `min` / `max` | `functions-aggregate` | declared SIGN-ONLY (sketch §6) |
| array and map functions (`array_contains`, `array_position`, `array_remove`, `array_distinct`, `array_union`/`intersect`/`except`, `array_has*`, map subscript), `collect_set` | `datafusion-functions-nested` | `COLLECTION-FLOAT-EQ-1` (D17) |
| struct and array equality with a float field (`named_struct('x', d) = named_struct('x', 0.0)`) | arrow `eq` on nested arrays | `NESTED-FLOAT-KEY-1` |
| `Join.on`, set operations, `Aggregate` group keys, `Distinct`, `Window` partition and order keys, `Sort`, `Repartition`, recursive `DISTINCT`, distinct aggregate arguments | logical plan nodes | **part B** (seat 2) |
| `Join.filter` and any non-literal comparison | expressions | **part B**; a literal-zero comparison inside a join filter is seat 1 |
| Physical hashing: `HashJoinExec` keys, `AggregateExec` row encoding, `RepartitionExec` hash, sort-merge join and `TopK` comparators | `physical-plan` | **part B** (wrapped keys) |
| `InListExpr`'s static hash set | `physical-expr/src/expressions/in_list.rs` | **part A, by construction**: seat 1 leaves no zero item in a float `IN` list |
| Parquet bloom filters via `LiteralGuarantee` (equality and `IN` literals) | `datafusion-pruning` | **part A, by construction**: the bound form carries no float equality, so the bloom filter is never consulted for a zero |
| Row-group and page pruning on min/max statistics | `datafusion-pruning` | exact for the bound form (sketch §2) |
| `ScalarValue` order in `simplify_predicates`, interval and equivalence analysis (`d = 0.0` makes `d` a constant) | `datafusion-optimizer`, `physical-expr` | **part A, by construction**: no float equality to a zero survives seat 1/1b |

## 7. Cards proposed from item 1

- `FLOAT16-DECIMAL-COERCION-1` (S3 over-refusal): a pandas `float16` column refuses `d = 0.0` on `spark.sql` and
  `filter(str)` at `type_coercion` (`item1/raw/repark-68b09d18-type-variants.jsonl`).
- `ANSI-REMAINDER-CLASS-1` (aside, error class): `1.0D % CAST('-0.0' AS DOUBLE)` under ANSI raises
  `DIVIDE_BY_ZERO` on RePark where Spark raises `REMAINDER_BY_ZERO` (`item1/raw/div-*.jsonl`).
- The legacy division guard (F3), unless it is ruled into this unit.

## 8. Questions owed before item 2

- **F1 (RULING), design point 4.** The `literal_datum` guard already exists (§1.1). Lean: keep the point as "pin it
  and make it explicit". Add a named float-zero check to `literal_datum` for `Float`/`Double`, so a future change to
  `Decimal::parse` cannot drop it silently. Add the §5 hand-built-bounds `meta_delete` test (`delete_predicate` is
  `None` for `d = 0.0`, `d = -0.0`, `d IN (0.0, 1.0)` and `0.0 = d`; both rows deleted). The mutation is "remove the
  zero check and let `Decimal::parse` accept zero": P4 and D1 turn red after seat 1.
- **F2 (RULING), simple `CASE` on the Spark door.** A3 refuses on `spark.sql` and `filter(str)` (10 cells),
  because `SparkIntegralLiteral` widens decimal literals only in `BinaryExpr`, `IN` and `BETWEEN`. Coercion then
  casts the double operand of `CASE d WHEN 0.0` to `Decimal128(30, 15)`, and the NaN and Infinity rows overflow.
  Seat 1 cannot see a float comparison there. Lean: add a `Case` arm to `widen_decimal_against_float`, owned by
  `repark-spark` beside the `<=>` arms: cast decimal-literal `WHEN` operands to `Float64` when `Case::expr` is float.
  It also moves non-zero simple `CASE` over a double column from a decimal to a double comparison, which is Spark's
  typing.
- **F3 (RULING), the legacy division guard.** `guard_zero_divisor` wraps a float divisor in `nullif(divisor, 0.0)`
  when ANSI is off. `nullif` compares under totalOrder, so a `-0.0` divisor gives `-inf` or `NaN` where Spark gives
  NULL (`X-div-1`, `X-div-2`, `X-div-4`). This is a consumer the sketch's census did not list. Lean: card it under
  `FUNC-FLOAT-EQ-1` with these cells (ruling G default 12 carded function-internal comparisons). The alternative is to
  build the guard as `CASE WHEN divisor = 0.0 THEN NULL ELSE divisor END`, which seat 1 makes IEEE, but that touches
  `repark-functions`.
- **F4 (RULING), design point 7's "EXPLAIN text".** Seat 1 is a plan rewrite, so the `Filter` lines of `EXPLAIN`
  necessarily show the bound form. `NamePreserver` keeps every output name, so `printSchema`, `.columns` and the
  projection aliases `EXPLAIN` prints keep the user's comparison. Lean: read point 7 as names (output columns,
  schema, and the aliases `EXPLAIN` shows), and pin those. A display shim for predicate text is out of scope.
- **F5 (note).** D6-3 to D6-6 are `GROUP BY` / `DISTINCT` key sites. Their values are part B. Part A pins only their
  names and schema, plus D6-1 and D6-2's values.
