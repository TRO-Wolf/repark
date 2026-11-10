# Step 4.1 / 4.2 evaluation — struct naming and pairing, and the expression door — 2026-11-09

**Author:** Fable 5.1 (consult; routing and design only, no build).
**Inputs measured:** Spark 4.1.2 on the box (oracle); RePark head `b8a703c3` (unit C, PR #1283) built in the scratch clone; the published `repark==1.5.6` wheel in a fresh venv; DataFusion 54.1.0 and arrow-cast 58.4.0 as locked; the parked branch `8e929978` (PR #1267) read through the GitHub API. Probe scripts and raw output: session scratchpad `eval42/` (`cases.py`, `spark_probe.py`, `repark_probe.py`, `dml_probe.py`, `wheel_probe.py`, `*_out.txt`). No repo file touched.
**Scope:** the combined evaluation the owner asked for on 2026-11-09: a measured table of struct pairing sites, the two-list diff for Step 4.1, the order decision, and unit cards. It is not a build brief.

## Summary

1. **A wrong write is shipping today, outside unit C's clause.** On the Spark door, `s = named_struct('b', 4, 'a', 3)` pairs by name where Spark pairs by position. Measured on 1.5.6 and on head: `SELECT ... WHERE` picks the wrong row, and `UPDATE`, `DELETE` and `MERGE ... ON` act on the wrong row of an Iceberg table. `IN (...)`, `isin`, `filter(str)`, `selectExpr`, `F.expr` and `JOIN ... ON` show the same pairing. Spark's answers are in §1. PR #1283 lists "comparison operators on structs (Step 3.2)" as carded; Step 3.2 was never built, so nothing refuses these today. This is the same class as unit C's motivation and should be rated with it (S1, DML reach measured).
2. **Step 4.2 is not undesigned. It is built and parked.** PR #1267 at `8e929978` carries `StructOperandsByPosition` (865 lines: comparisons, `IN`, `IN (subquery)` refusal, `UNION` inputs, lists and maps, zero-copy rename), a `struct(...)` naming pass on the SQL door (313 lines) and `F.struct` naming, a duplicate-name planner, and a 1031-cell struct domain table across 21 seats including `delete`, `update_where`, `merge_on`, `view`, `cte`, `df_filter`, `insert_select` (988 equal, 43 reasoned). Both verifiers proved those claims over four rounds. It was paused on F9 (string-expression door bypasses the naming pass, which is the Step 4.1 gap in miniature) and F10 (duplicate-name planner covers binary comparisons only).
3. **The mechanism, corrected.** By-name pairing lives in two upstream layers, arrow-cast `cast_struct_to_struct` and DataFusion `nested_struct::cast_column`, both pinned crates. Flipping them is not an option, and the file-read path needs them by name (measured: Spark itself reads a reordered Parquet file back by name). The fix is to rename the right operand's struct fields to the left operand's names before type coercion so no reordering cast is ever asked for. That is exactly what the parked rule does. 4.2 therefore does not need Step 4.1 first; it needs the rule in both analyzer lists, which unit C's delta 2 already does for its own rule.
4. **Step 4.1 inventory.** The expression context differs from the session in seven analyzer rules, the optimizer and physical rule lists, four core UDF registrations, the store-cast and TA UDFs, the dialect's temp-view rule, and every configuration carrier (ANSI, zone, case flag). Bound `filter(str)` and `where(str)` already plan through the frame's session; only the unbound `F.expr` and `selectExpr` path uses the process-global context. The full unit (five binding questions, cache inventory) stays where the plan put it. 4.2 needs only its first bullet, one entry point per rule in both lists.
5. **Recommended order.** Finish unit C as planned. Then carve the comparison slice out of #1267 as its own unit from main, ahead of the label, because it stops a shipped wrong write and Spark's refusal pattern makes a pure refusal unit over-refuse. Then the set-operation slice, which retires unit C's refusal for `UNION` only, since Spark itself refuses reordered `EXCEPT` and `INTERSECT`. Then naming. Then the full 4.1. Cards in §6, decisions owed in §8.

## 1. Measured table

Left operand `named_struct('a', 1, 'b', 2)`, right operand reordered `named_struct('b', 2, 'a', 1)` or `('b', 3, 'a', 4)` unless stated. "RePark" is head `b8a703c3`, Spark door; the 1.5.6 column is the published wheel where measured. Row order inside a set operation is not asserted.

| # | Cell | Spark 4.1.2 | RePark head | 1.5.6 wheel | Class |
|---|---|---|---|---|---|
| 1 | `UNION ALL` reordered | positional, names from left: `{1,2},{3,4}` | refused (unit C) | by name `{1,2},{4,3}` | C fixes the wheel's wrong answer; 4.2 flips to Spark's |
| 2 | `UNION ALL` different names (`x,y`) | positional `{1,2},{1,2}` | refused | not measured | same |
| 3 | `UNION` distinct, same values reordered | two rows (positional) | refused | not measured | same |
| 4 | `UNION ALL` reordered with a type clash at one position | error INCOMPATIBLE_COLUMN_TYPE | refused | not measured | EQUAL class |
| 5 | `UNION ALL` field-count mismatch | error | refused | not measured | EQUAL class |
| 6 | `NULL` first, then reordered operands | positional `{1,2},{3,4}` | **answers by name `{4,3}`** | not measured | F-A, delta 2 |
| 7 | `array(struct)` reordered in `UNION ALL` | positional | refused | not measured | same as 1 |
| 8 | `EXCEPT` reordered | **error** INCOMPATIBLE_COLUMN_TYPE | refused | not measured | EQUAL class; stays a refusal after 4.2 |
| 9 | `INTERSECT` reordered | **error** | refused | not measured | same as 8 |
| 10 | `=` reordered | `false` | **`true`** | **`true`** | wrong answer |
| 11 | `=` different names | `true` | refused (Unsupported CAST) | not measured | over-refusal, safe |
| 12 | `=` field-count mismatch | error | refused | not measured | EQUAL class |
| 13 | `<` reordered | `true` | **`false`** | not measured | wrong answer |
| 14 | `IN (list)` reordered | `false` | **`true`** | **`true`** | wrong answer |
| 15 | `IN (list)` different names | `true` | refused | not measured | over-refusal, safe |
| 16 | `IN (subquery)` reordered | `false` | refused (physical plan) | not measured | safe today; parked rule refuses with Spark's text |
| 17 | `CASE WHEN ... THEN ab ELSE ba` | **error** DATA_DIFF_TYPES | answers `struct<b,a>` | not measured | accepts what Spark refuses |
| 18 | `IF(x, ab, ba)` | error | answers | not measured | same as 17 |
| 19 | `coalesce(typed null, ba)` | error | answers | not measured | same as 17 |
| 20 | `array(ab, ba)` | error | answers `array<struct<b,a>>` | not measured | same as 17 |
| 21 | `array_contains(array(ab), ba)` | error | refused (same text) | not measured | EQUAL |
| 22 | `JOIN ... ON l.s = r.s` reordered | no match | **one match** | not measured | wrong answer |
| 23 | `GROUP BY s` over a reordered union | two groups | refused (union inside) | not measured | follows 1 |
| 24 | `CAST(named_struct('b',3,'a',4) AS STRUCT<a,b>)` | **positional `{3,4}`** | **by name `{4,3}`** | **by name** | wrong answer, explicit user cast |
| 25 | `CAST(xy AS STRUCT<a,b>)` | positional `{1,2}` | refused | not measured | over-refusal, safe |
| 26 | `struct(1, 2)`, `struct(x, y+1)`, `struct(x AS p, y)` | `col1,col2` / `x,col2` / `p,y` | `c0,c1` / `c0,c1` / `p,c1` | not measured | naming; parked branch fixes |
| 27 | `F.struct(lit(1), col('x'), col('y')+1)` | `col1,x,col3` | `1,x,(y + 1)` | not measured | naming; parked branch fixes |
| 28 | DataFrame `union` reordered | positional | refused | not measured | as 1 |
| 29 | DataFrame `unionByName` reordered | **by name, nested too: `{4,3}`** | refused | not measured | over-refusal; after 4.2 must stay by name |
| 30 | `Column.isin(F.expr(ba))` | `false` | **`true`** | not measured | wrong answer |
| 31 | `col == F.expr(ba)`, `filter("s = ba")`, `selectExpr("s = ba")` | `false` | **`true`** on all three | not measured | wrong answer on every string door |
| 32 | `INSERT INTO t(struct<a,b>) SELECT ba` | positional `{3,4}` | positional `{3,4}` | not measured | EQUAL |
| 33 | `INSERT INTO ... SELECT xy` | positional | positional | not measured | EQUAL |
| 34 | `INSERT INTO t BY NAME SELECT ba` | by name `{8,7}` | not measured | not measured | open |
| 35 | Parquet file written as `struct<b,a>`, read back under `struct<a,b>` | **by name** | path append refused (type mismatch) | not measured | file reads stay by name; refusal is safe |
| 36 | `WHERE s = named_struct('b', 4, 'a', 3)` on rows `{3,4}`, `{4,3}` | row 2 | **row 1** | **row 1** | DML predicate |
| 37 | `UPDATE ... SET id = 99 WHERE` same predicate | would update row 2 | **updated row 1** | not measured | **wrong write** |
| 38 | `DELETE FROM ... WHERE` same predicate | would delete row 2 | **deleted row 1** | **deleted row 1** | **wrong write, shipped** |
| 39 | `MERGE ... ON t.s = u.k` with `k = named_struct('b', 3, 'a', 4)` WHEN MATCHED DELETE | matches `{4,3}` only | **deleted every row** | not measured | **wrong write** |
| 40 | `UNION ALL BY NAME` (SQL) | parse error | refused by the struct rule | not measured | DataFusion syntax Spark lacks; card |

Native door (`repark.sql`): cells 10, 13, 14, 22 and 24 show the same by-name answers; the native door is stock DataFusion by design (2026-11-08 memo), so these are divergence records, not Spark-door defects.

## 2. Where pairing happens

**Type level** (`datafusion-expr-common` `type_coercion/binary.rs`). `struct_coercion` (:1265) with equal field counts: if every left name exists on the right, coerce by name and emit the **left** order (:1292, `coerce_struct_by_name` :1340); otherwise coerce by position, keeping left names (:1370). `comparison_coercion` reaches it last (:939); `type_union_resolution_coercion` has its own by-name arm (:734) and `try_type_union_resolution_with_struct` demands identical key strings (:800). `comparison_coercion` serves `=`, `<`, `IN` lists (`get_coerce_type_for_list`), join keys (`coerce_join` :192 routes through `coerce_binary_op` :292) and `CASE x WHEN`; `type_union_coercion` serves `CASE ... THEN/ELSE`, `coalesce`, `array`, `UNION`.

**Cast level.** The analyzer inserts `Cast(right AS left_type)` wherever types differ (`coerce_binary_op`, `InList` arm :660, `coerce_union` :223, `coerce_plan_expr_for_schema`). The cast executes through `ColumnarValue::cast_to` → `cast_array_by_name` (`columnar_value.rs:306`) → DataFusion `nested_struct::cast_column` when `requires_nested_struct_cast` (by name, NULL-filling missing fields), else arrow-cast `cast_struct_to_struct` (`arrow-cast/src/cast/mod.rs:2223`: positional fast path when names already match in order, by name when all names exist, positional fallback otherwise). The physical `CastExpr` refuses at build time when `validate_data_type_compatibility` fails, which is cells 11, 15 and 25. So DataFusion is internally inconsistent: a reordered pair is paired by name, a renamed pair is refused rather than paired positionally.

**Consequences.** (a) Neither routine is RePark code. (b) `SimplifyExpressions` folds `CAST(named_struct(...))` into a struct literal with the target's names, destroying the evidence (measured 2026-11-09 in the review-cap memo). (c) If the right operand already carries the left's names, the type level finds `equals_datatype`, inserts no cast, and the arrow fast path is positional. **Renaming before coercion is sufficient and touches no cast.** (d) File reads: Spark reads a `struct<b,a>` file back under `struct<a,b>` by name (cell 35). The schema adapter must keep `nested_struct::cast_column`. A query-level rename never reaches it.

## 3. What #1267 already built, and what it lacks

| Component on `8e929978` | Lines | What it does | Status after four rounds |
|---|---|---|---|
| `normalize/struct_position.rs` `StructOperandsByPosition` | 865 | Analyzer rule before `type_coercion`: for `=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`, `IN` list, wraps the right operand in `__repark_struct_by_position__`, a UDF whose return type carries the left names and whose body renames the array without copying (`renamed_array`); recurses into lists and maps; refuses clashes with Spark's error text; refuses `IN (subquery)` mismatches with Spark's text; renames `UNION` inputs through a `Projection` against input 0 | proven (F5, F7, F8); union arm has unit C's four reach gaps |
| `DuplicateFieldOperands` `ExprPlanner` | in same file | Duplicate-named operands: positional `_1.._n` types on both sides, then compare | F10 open: binary only; `IN` reaches a DataFusion debug assertion |
| `keyword_lower/struct_names.rs` `name_struct_fields_in_sql` | 313 | Token-level rewrite of the SQL text inserting Spark's `col1..colN` / attribute names into `struct(...)` calls before parsing | proven on the SQL door (F2, F4); F9 open: only call site is the router |
| `F.struct` naming (`functions_collections.py`, `column/display.rs`) | small | Spark's names on the DataFrame door | proven (F6) |
| `struct_domain_grid.py` + 1031 cells + Rust table test | ~1000 | 21 seats × shapes × type pairs; 462 Spark-error cells, 263 row cells, 306 ok cells | 988 equal, 43 reasoned, 2 oracle defects |

Gaps against today's knowledge: the union arm compares every input with input 0 before coercion, so it misses F-A (NULL first), F-B (view body already coerced), F-C (PREPARE body already optimized) and F-D (recursive CTE term already cast), the four findings that spent unit C's cap. The table has no `UNION` reorder shapes with stored bodies (its `view` and `cte` seats hold comparisons). It has no door axis for string expressions (F9). Its naming pass is a text rewrite, where the master plan's Step 4.2 asks for an AST rewrite before `SqlToRel`; the two differ in failure mode (text offsets vs AST shape), not in result, and the choice belongs to the naming unit's sketch.

## 4. Step 4.1 inventory: the two lists

Session (`df_guards.rs` `context_with_df_54_1_rule_guards` :69): default analyzer + `sliding_frame_rescan` + `struct_setop_names` (before `type_coercion`) → extension `configure_analyzer_rules` (`repark-spark/extension.rs:135`: higher-order preparation, Spark literal rule, **`join_condition_refusals`**, map subscript) → dialect hook (Spark dialect adds none) → `register` adds **`InsertStoreAssignment`**, post-coercion literal rules, `FoldSparkNumericCasts`, `SparkProjectionDisplay`, **`StackRewrite`**, **`StoreOverflowCast`**, store-cast UDFs, TA UDFs; the builder sets the unnest-safe optimizer list, the skip-safe physical list plus three physical rules, and registers stack, update_fields, isnan and the single-row guard. Config carriers come from `install_spark_session_conf`.

Expression context (`expr_build.rs` `build_expr_context` :270): default analyzer + `struct_setop_names` + higher-order preparation + Spark literal rule + map subscript + `FoldSparkNumericCasts` + `SparkProjectionDisplay` + post-coercion literal rules; Databricks dialect, identifier normalization off; Spark type planner; `StackQueryPlanner`; `register_all`, `spark_as`, `suffix_literal`. Process-global `OnceLock`.

| Missing from the expression context | Effect on a string expression |
|---|---|
| `sliding_frame_rescan`, `join_condition_refusals`, `InsertStoreAssignment`, `StackRewrite`, `StoreOverflowCast` | a rule that fires at construction in the session does not fire here; most re-fire when the frame's plan is analyzed, refusals that key on the raw shape do not |
| optimizer and physical lists | none at construction (the expression is extracted before optimization) |
| core UDFs (stack, update_fields, isnan, single-row guard), TA UDFs, store-cast UDFs | unknown-routine errors or different resolution for those names |
| configuration carriers | ANSI, zone and case flag read defaults, not the session's |
| dialect temp-view rule | names resolve differently |

Call sites today: bound `filter(str)` and `where(str)` go through `parse_canonical_predicate`, which uses **the frame's own session** (`frame.inner().parse_sql_expr`, :53) with a folded-qualifier fallback on a grown clone of that session; the unbound path (`PyColumn::plan_sql_text` :42, used by `F.expr` and `selectExpr`) uses the global context. Two private parsers, `merge/nested_assign.rs:426` and `write/conflict_filter.rs:74`, parse text with `sqlparser` alone and render SQL back; they are not planning seats. `UnionByName` is Rust (`dataframe_names::union_frames`), top-level by name.

Reading: **4.1's first bullet (retire the global context and the hand-copied list) is a few hundred lines and is what 4.2 and the parked F9 need; the rest of 4.1 (binding contract, cache inventory) is a separate design and stays where the plan put it.** Every analyzer rule that must reach string expressions is already inserted by a shared function called from both places (`insert_struct_setop_before_coercion`, the literal rule, map subscript), so the standing rule for new rules is "one insert function, two call sites, one test asserting both", until the lists are one.

## 5. The order decision

Evidence for moving the comparison slice ahead of the label:

- It is a shipped wrong write (cells 36–39), measured on 1.5.6, on an Iceberg table, through DELETE, UPDATE and MERGE. Unit C was dispatched for the same class on set operations.
- A pure refusal (Step 3.2 as written) would refuse cell 11 and 15, which Spark answers `true`, and would have to derive Spark's names anyway to know when to refuse. The parked rule derives them and answers. Cost is similar; the refusal over-refuses.
- The implementation exists and has been through four review rounds. Carving it to its own branch from main costs a cherry-pick and the F10 generalisation, not a design.
- It does not need 4.1: the rule inserts through one function into both lists, as unit C does today.

Evidence against: it is one of the two engine-wide units the 2026-11-08 R2a Option B kept off the label's path; it changes answers, not just refusals; a missed reach produces new wrong data where a refusal produces an error. The mitigation is the table (§6) and unit C's insertion-inventory rule.

**Recommendation.** Amend R2a: the comparison slice goes ahead of the label as a fix unit carved from #1267; the set-operation slice follows unit C's merge; naming and the full 4.1 keep their place. Spark refuses reordered `EXCEPT`/`INTERSECT`, so unit C's refusal is already Spark's answer there and the set-operation slice only flips `UNION`.

## 6. Unit cards

### U-A `STRUCT-COMPARE-BY-POSITION-1` (Opus 5.5 build; first)

- **Owning layer:** `repark-spark` analyzer rule, inserted before `type_coercion` through one function called from `extension.rs` `configure_analyzer_rules` and `expr_build.rs` `build_expr_context`.
- **Doors:** Spark SQL door; DataFrame typed API (`==`, `<`, `isin`, `join(on=)`); DataFrame string expressions (`filter(str)`, `where(str)`, `F.expr`, `selectExpr`); DML seats (`WHERE` of DELETE and UPDATE, `MERGE ON`, `WHEN MATCHED AND`); views and CTEs holding comparisons.
- **Clause:** a struct-against-struct `=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`, `IN` list and join key pairs fields by position with Spark's type rule; `IN (subquery)` refuses with Spark's text (Spark answers it positionally; carded); clashes refuse with Spark's text; duplicate-named operands per R4, generalised to `InList` (F10).
- **Source:** `8e929978` `struct_position.rs` comparison arms, minus the union arm; cast-transparent operand read (unwrap `Alias`, one `Cast`/`TryCast`) so a view body's inserted cast is renamed beneath rather than compared above.
- **Table:** the parked grid's comparison and DML seats re-measured on Spark, plus a door axis (SQL, typed API, string expressions), plus the eight rows of §1 cells 10–16, 22, 30, 31, 36–39 as pins; both profiles; case flag both ways.
- **Insertion inventory (§7).**
- **Not in the unit:** set operations (unit C holds them), `CASE`/`IF`/`coalesce`/`array` acceptance (cells 17–20: refuse in the same rule if the table shows it is one arm, else card), `CAST` by position (cell 24, card `STRUCT-USER-CAST-POSITIONAL-1`), naming.
- **Size:** medium; the rule is written, the table generator exists, the door axis and F10 are new.

### U-B `STRUCT-SETOP-BY-POSITION-1` (Opus 5.5 build; after unit C merges)

- Replaces unit C's `UNION` refusal with the rename; keeps the refusal for `EXCEPT`/`INTERSECT` (Spark's answer) and for type clashes and field-count mismatches.
- Seat: the post-coercion position unit C's delta 2 established, reading beneath the cast DataFusion inserted and replacing it with the rename; `RecursiveQuery` arm; PREPARE belt arm as unit C carded.
- Table: unit C's 211-row set-operation table with every refusal cell flipped to Spark's measured answer, plus cells 6, 7, 23, 28.
- `unionByName`: Spark pairs nested fields by name (cell 29); the rename must not apply on that path, which is a separate Rust entry (`union_frames`), so the pin is "unionByName nested reorder answers `{4,3}`".

### U-C `STRUCT-NAMING-1` (Opus 5.5 sketch then Muse build)

- `struct(...)` names on both doors (cells 26, 27), from `8e929978` `struct_names.rs` and the `F.struct` change, with one entry point called by the router and by the expression-string path (the F9 fix). Sketch decides text rewrite vs AST rewrite. Duplicate names per R4.

### U-D `EXPR-DOOR-UNIFY-1` (unchanged from the master plan)

- The five binding questions and the cache inventory. Its first bullet (retire the global context) can be pulled forward as a clerk-grade unit once U-A and U-C have proved the two-call-site pattern, because at that point the diff in §4 is the whole specification.

## 7. Insertion inventory for U-A (per the 2026-11-09 rule)

Evidence is the field-name list on each struct operand.

| Where evidence is created, moved or destroyed | Before U-A's seat? | Handling |
|---|---|---|
| `SqlToRel` names unnamed `struct(...)` args `c{i}` (`datafusion-sql expr/mod.rs:833`); `struct` UDF return type `c{pos}` (`functions core/struct.rs:110`) | created before | U-C renames; U-A compares whatever names exist |
| `F.struct` builds names from Python display strings | created before | U-C |
| `type_coercion` inserts `Cast(right AS left_type)` in `coerce_binary_op`, `InList`, `coerce_join` | after the seat | none needed when the seat renames first |
| `create_view` runs `apply_type_coercion` and stores the coerced body; inlined at scan build | before, for a view body | cast-transparent read: rename beneath the cast and drop it |
| PREPARE arm runs the bare optimizer; `SimplifyExpressions` folds literal casts | before, evidence destroyed for literal operands | belt arm on `Statement(Prepare)` as unit C carded; column operands survive and are renamed at EXECUTE |
| Recursive CTE term cast by the SQL planner | before | not a comparison seat; U-B |
| Metadata-only DELETE door (`try_metadata_delete_door`) evaluates the predicate outside the analyzer | separate seat | table row: a struct predicate must fall through to the passthrough plan or refuse; measured, not assumed |
| `conflict_filter.rs` widens a text predicate to an Iceberg `Predicate` | separate | struct terms become `AlwaysTrue` (safe, wider); table row |
| `nested_assign.rs` renders assignment SQL | separate | not a comparison |
| `DuplicateFieldOperands` `ExprPlanner` runs at planning, before any analyzer | before | keep; generalise to `InList`/`InSubquery` |
| Native door `PreExecute` | separate session | out of clause; divergence record |

## 8. Decisions owed to the owner

- **D1.** Carve U-A from #1267 onto a new branch from main (recommended), or reorganise #1267 whole per the 2026-11-07 proposal. The ANSI-subscript half of #1267 was proven in every round and can close on its own.
- **D2.** Amend R2a so U-A runs before the label as a fix unit (recommended), replacing the never-built Step 3.2 refusal. Default if not amended: build Step 3.2 as a refusal from the same rule, which over-refuses cells 11 and 15.
- **D3.** `EXCEPT`/`INTERSECT` reorder stays a refusal permanently (Spark's answer). Recommended yes.
- **D4.** Cells 17–20 (`CASE`, `IF`, `coalesce`, `array` over reordered structs): Spark refuses, RePark answers. Fold into U-A if the table shows one arm, else card. Default: card.
- **D5.** Cell 24, explicit `CAST` to a struct type pairs by name where Spark pairs by position. Card `STRUCT-USER-CAST-POSITIONAL-1` (recommended) or fold into U-B.
- **D6.** Cell 34, `INSERT ... BY NAME` with nested reorder: unmeasured on RePark; clerk row.
- **D7.** Rating: cells 36–39 as S1 with DML reach on 1.5.6, recorded in the staging ledger beside unit C's clause.

## 9. Not measured here

Trino as the native reference; duplicate-name operands (R4 cells are in the parked table); ANSI off; case-insensitive sessions; map-keyed structs beyond one level; aggregates and windows over structs (`collect_list`, `PARTITION BY s`); the distributed and server doors; the PyPI wheel on the DataFrame string doors (cells 30–31 measured on head only). Each is a clerk row before U-A's brief goes out.
