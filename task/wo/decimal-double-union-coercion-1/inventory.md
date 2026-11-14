# DECIMAL-DOUBLE-UNION-COERCION-1 — the insertion inventory

By the insertion-inventory rule (owner, 2026-11-09). The property: **a DECIMAL meets a DOUBLE or a FLOAT
and the common type is chosen.** For each seat the answer is "seen at the seat: yes / no, because", with
file:line in the vendored DataFusion 54.1.0 (cargo registry sources `datafusion-*-54.1.0`, the version
`Cargo.lock` pins; DataFusion is not patched, only the iceberg-rust family is) and in RePark at
`16974986` (origin/main when this sketch was cut). Every RePark line below was re-read at that head.

## How the inventory was searched (repeatable)

1. **The type decision itself.** `grep -n "fn decimal_coercion\|fn binary_numeric_coercion\|fn
   type_union_resolution\|fn type_union_coercion\|fn comparison_coercion\|Float64 => Some(Decimal128"`
   over `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs`, then every caller of those five
   functions: `grep -rn "type_union_resolution\|type_union_coercion\|comparison_coercion\|
   try_type_union_resolution" datafusion-{expr,optimizer,sql,functions,functions-nested}-54.1.0/src`.
2. **RePark callers of the same functions:** `grep -rn --include=*.rs -E "type_union_resolution|
   coerce_union_schema|decimal_coercion|binary_numeric_coercion|comparison_coercion|type_union_coercion|
   get_coerce_type_for" crates` (non-test hits: `create_map.rs:63`, `map_concat.rs:80`,
   `array_insert.rs:59/62`, `struct_position.rs:954`, `struct_position/forms.rs:258`).
3. **Every analyzer list:** `grep -rn --include=*.rs -E "with_analyzer_rules|Analyzer::with_rules|
   Analyzer::new\(\)|add_analyzer_rule|configure_analyzer_rules|execute_and_check|analyze_eagerly"
   crates`, then for each hit `grep -n "#\[cfg(test)\]"` in the same file to drop test-module hits.
4. **The live list, not the source:** `EXPLAIN VERBOSE` on both doors prints every analyzer and
   optimizer rule in order ([evidence/raw/rule-lists.json](evidence/raw/rule-lists.json), probe
   [evidence/probes/rule_lists.py](evidence/probes/rule_lists.py)).
5. **Rules that rebuild a `Union` or `Values` (they re-derive the schema):** `grep -rn --include=*.rs
   "LogicalPlan::Union\|Union::try_new\|LogicalPlanBuilder::values\|LogicalPlan::Values" crates`.
6. **Stored plans:** `grep -rn --include=*.rs "ViewTable::new\|into_view\|store_prepared\|
   register_collected_memtable\|apply_type_coercion" crates` and the same in
   `datafusion-54.1.0/src/execution/context/mod.rs`.
7. **Measured confirmation:** the domain table ([domain-table.md](domain-table.md)) runs each seat's
   construct; a seat this inventory calls "no" must show no divergence in its cells.

## A. Where the type is decided (DataFusion)

| # | seat | file:line | seen? |
|---|---|---|---|
| A1 | `decimal_coercion` | `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs:1039`; decimal + non-decimal → `get_common_decimal_type` `:1130` → `coerce_numeric_type_to_decimal128` `:1227` (`Float64 => Decimal128(30, 15)` `:1239`, `Float32 => Decimal128(14, 7)`) | **yes — this is the fault.** Spark's `findWiderTypeForDecimal` answers `double` for any decimal beside a FLOAT or DOUBLE; DataFusion answers the wider of the decimal and `(30,15)` / `(14,7)`. |
| A2 | `binary_numeric_coercion` | `binary.rs:1018` (calls A1 before `numerical_coercion` `:1464`) | yes, through A1. |
| A3 | `type_union_coercion` (UNION, CASE THEN/ELSE, `nvl2`) | `binary.rs:886` → A2 `:890` | yes, through A2. |
| A4 | `type_union_resolution` (VALUES, `coalesce`, `make_array`) | `binary.rs:604` → `type_union_resolution_coercion` `:672` → A2 `:775` | yes, through A2. |
| A5 | `comparison_coercion` (`=`, `IN` list, `IN (subquery)`, BETWEEN, simple CASE, `greatest`/`least`, `nullif`, join keys) | `binary.rs:924` → A2 `:929` | yes, through A2. |
| A6 | arithmetic (`mathematics_numerical_coercion`) | `binary.rs:1426` → `numerical_coercion` `:1464` (`(Float64, _) => Float64`, then `(_, Float32) => Float32`) | **no for DOUBLE**, because arithmetic never calls A1: decimal + double is already `double`, as in Spark. **Yes, differently, for FLOAT:** decimal + FLOAT answers `float` where Spark answers `double` (the `ctl_add/*/flt_*` cells); a separate seat, carded as DECIMAL-FLOAT-ARITH-1. |

## B. The session analyzer list (Spark door)

The Spark-door context is built at `crates/repark-core/src/session/df_guards.rs:78-91`:
`analyzer_rules_with_df_54_1_rule_guards` (`:110-113`, DataFusion's `Analyzer::new().rules` through
`window_rescan.rs:24` and `struct_setop::insert_struct_setop_after_coercion`), then
`SparkExtension::configure_analyzer_rules` (`crates/repark-spark/src/extension.rs:146-156`), then the
dialect's `configure_analyzer_rules`; `SparkExtension::register` appends the post-coercion rules
(`extension.rs:170-178`, `repark_functions::analyzer_rules()` at `crates/repark-functions/src/registration.rs:26-56`
less `spark_integer_literal`), and `finish_session_build` appends `signed_zero_literals`
(`df_guards.rs:105-108`, `df_guards/signed_zero.rs:10`). The measured order (`EXPLAIN VERBOSE`, Spark door):

| idx | rule | sees the property? |
|---|---|---|
| 0 | `resolve_grouping_function` | no: grouping only. |
| 1 | `spark_timestamp_unit_widen` (`timestamp_unit_widen.rs:578-620`) | **sees the Union, Values and the shared-argument functions** (`SHARED_ARGUMENTS` `:16-24`: `coalesce`, `nvl`, `ifnull`, `greatest`, `least`, `array`, `make_array`; `nvl2`, `if` after the first `:25`) **but acts only on timestamp units**. The nearest template for a fix: one pre-coercion walk over the same contexts. |
| 2 | `spark_fractional_division` | no: division only. |
| 3 | `higher_order_preparation` | no. |
| 4 | `spark_float_stringify` (`java_double.rs:520-558`, `rewrite_coalesce_args`) | sees `coalesce` with a float argument, but folds only UTF-8 literals to the float type; a decimal argument passes untouched. |
| 5 | `spark_decimal_precision` | no: decimal arithmetic precision; never a decimal beside a float. |
| 6 | `spark_integral_literal` (`spark_literal_typing.rs:24-36`, `rewrite_plan` `:113`) | **partly.** `widen_decimal_against_float` (`:264-274`) casts a decimal **literal** to `Float64` in a comparison, `IN` list, `BETWEEN` and simple-`CASE` `WHEN` when the other side is FLOAT/DOUBLE. It does not see a decimal **column** or a `CAST(… AS DECIMAL)` expression (`is_decimal_literal` `:333-341`), nor set operations, `VALUES`, CASE THEN/ELSE, `IF`, `coalesce`, `nvl2`, `nullif`, `greatest`, `least`, `array`, `map`. It also runs only when the plan holds an Int64, UInt64 or decimal literal (`plan_may_narrow` `:62-80`). It rebuilds a stale `Union` with loose types (`:120-135`) and a narrowed `Values` through `LogicalPlanBuilder::values` (`:175-195`), which re-runs A4. |
| 7–9 | `join_condition_refusals`, `spark_map_subscript`, `struct_operands_by_position` | 7 and 8: no. **9: yes, narrowly (corrected after the critic):** `struct_operands_by_position` and its SQL-planning `ExprPlanner` `duplicate_field_operands` (registered at `extension.rs:163`) decide the output struct type of `nullif` and comparisons over duplicate-named structs through `comparison_coercion` (`normalize/struct_position/forms.rs:258`, `struct_position.rs:954`); a decimal leaf beside a double leaf takes `(30,15)` there. Nested only; not probed. |
| **10** | **`type_coercion`** (`datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs`) | **yes — the seat that applies A1 to every analyzed construct:** `coerce_union` `:223` (`coerce_union_schema_with_schema` `:1190`, A3 at `:1234`); `coerce_join` `:192` (INTERSECT / EXCEPT keys, A5); `InSubquery` `:483-505` (A5 `:495`); `Between` `:633`; `InList` `:660` (`get_coerce_type_for_list`, A5); `Case` `:694` → `coerce_case_expression` `:1026` (`get_coerce_type_for_case_expression`, A3, `type_coercion/other.rs:65`); `ScalarFunction` `:698` → `coerce_arguments_for_signature` `:1004` (each UDF's `coerce_types`: `coalesce.rs:137` A4, `nvl2.rs:126` A3, `greatest.rs:141` / `least.rs:154` A5 via `greatest_least_utils.rs:130`, `make_array.rs:116` A4, `nullif.rs:82` `Signature::comparable` A5; `nvl.rs:92` is `Signature::uniform` over `SUPPORTED_NVL_TYPES` `:65-80`, which lists no decimal, so `nvl` / `ifnull` cast every decimal to `Float64` and **answer Spark's `double` here by accident**). |
| 11 | `struct_setop_names` | no: struct set operations after coercion. |
| 12 | `sliding_frame_rescan` | no. |
| 13 | `repark_insert_store_assignment` | sees the INSERT source type after coercion (decimal(30,15) into a DOUBLE column is a legal store), so it cannot see the loss. |
| 14–27 | `spark_negate_null_decimal`, `lambda_rebind`, `spark_decimal_precision`, `spark_decimal_rewrite`, `spark_nullability`, `spark_integer_overflow`, `spark_expr_semantics`, `int_to_binary_cast`, `spark_float_stringify`, `array_cardinality_ceiling`, `csv_fold`, `time_cast_guard`, `spark_ltz_timestamp_cast`, `interval_string_cast` | no: they see the already-coerced plan; the decimal casts are in place. |
| **28** | **`type_coercion` (second instance**, `registration.rs:44`) | yes, but on a plan idx 10 already coerced: it re-applies A1 to anything a rule in 14–27 introduced. A fix that works before idx 10 must hold its types through idx 28 (a `Float64` union is stable under A3). |
| 29–43 | `lambda_rebind`, `spark_map_rewrite`, `spark_map_subscript`, the time-window rules, `generator_rewrite`, `resolve_grouping_id`, `fold_spark_numeric_casts`, `spark_projection_display`, `stack_rewrite`, `repark_store_overflow_cast`, `signed_zero_literals`, `spark_integer_overflow`, `bool_decimal_cast` | no. |

The whole list runs **twice** per Spark-door SQL statement: `analyze_eagerly`
(`crates/repark-spark/src/spark_ast.rs:273`, `registration.rs:64-71`) and again inside
`execute_logical_plan` (`spark_ast.rs:295`). `EXPLAIN VERBOSE` on the Spark door therefore shows the
casts already present at idx 0: the trace is of the second run.

## C. Every other production analyzer list

| list | file:line | sees the property? |
|---|---|---|
| `F.expr` / `selectExpr` / string `filter` context, `build_expr_context` | `crates/repark-python/src/column/expr_build.rs:288-312` | **yes**: DataFusion's list with the same pre-coercion inserts (`analyzer_rules_with_higher_order_preparation`, `insert_literal_rule_before_coercion`, …) and `type_coercion`; the parsed expression is then placed in the frame's plan and analyzed again by the session list (B). A seat in B alone leaves the fragment's own analysis on the old type until the outer pass; a seat added through `insert_literal_rule_before_coercion` lands in both lists. |
| native door (`repark.sql()`), the Python native session | `crates/repark-python/src/session_runtime.rs:414-426` (timestamp widen, map subscript, `SparkMapRewrite`) | yes: plain DataFusion `type_coercion`; no `spark_integral_literal`, no RePark decimal seat. Outside the label (the native door is DataFusion semantics). |
| Rust-embedded ANSI door, `AnsiDialect` | `crates/repark-sql/src/dialect.rs:21-29` | yes, same as the native door. |
| `LeafCaster` (map/struct key-by-position leaf casts) | `crates/repark-functions/src/collection/key_by_position.rs:44-50` | only inside one leaf-cast expression it builds itself (a cast of one leaf to its declared type); no union or multi-argument unification reaches it. No. |
| `plan_introspect` | `crates/repark-core/src/plan_introspect.rs:159, 185` | runs the session list for introspection; it sees the property exactly as B does and its result is not executed. |
| `refuse_unanalyzed_prepare` | `crates/repark-core/src/pre_execute.rs:95-104` | runs the session list on a PREPARE body to validate it and **discards the result** (see E2). |
| test-only lists | `grouping.rs:497`, `fractional_division.rs:237`, `timestamp_unit_widen.rs:679`, `decimal_precision.rs:769`, `spark_literal_typing.rs:757-790` | not production (inside `#[cfg(test)]`). |

## D. SQL-planner-time decisions (before any analyzer rule)

| # | seat | file:line | seen? |
|---|---|---|---|
| D1 | multi-row `VALUES` without a target schema | `datafusion-sql-54.1.0/src/values.rs:53-55` → `LogicalPlanBuilder::values` `builder.rs:211` → `infer_data` `:298`, A4 at `:328` | **yes — decided before the analyzer.** The `Values` node's schema is already `decimal(30,15)` when idx 0 runs; a fix must rebuild the node (as `rewrite_values` does), not only re-type its parents. |
| D2 | `INSERT … VALUES` (target schema in the planner context) | `values.rs:53-57` → `values_with_schema` `builder.rs:243` → `infer_values_from_schema` `:269` | **yes, by omission (corrected after the critic).** Spark resolves the inline table first (a decimal beside a double gives `double`) and then store-casts to the target; RePark casts each cell straight to the target and forms no common type. Equal for DOUBLE, FLOAT and BIGINT targets (`ins_values_*`, `values_float`, `values_bigint`); **WRONG for STRING and DECIMAL(38,18) targets** (`values_string`, `values_dec38`): decimal → double → target differs from decimal → target. |
| D3 | `INSERT … SELECT` | `statement.rs:2358` `insert_to_plan`; `set_table_schema` `:2463`; the source column cast to the target `:2479` | **no** at the projection cast: it casts the already-typed source; the loss happened in A3 / A4 inside the source query. **But the target schema leaks by position into every `VALUES` in the source** (read at `values.rs:53`), so D2's omission applies there too (`select_values_string` WRONG). This holds only for DataFusion's `insert_to_plan`: RePark's own write doors plan the source standalone (section G), so their `VALUES` take D1's `(30,15)`. A nested `VALUES` in the source takes the target schema too (D2 path): measured, `ins_select_values_tiny` writes Spark's bits by that accident, and `ins_case_tiny` (a one-column `VALUES` under a two-column target) raises `Inconsistent data length across values list`. |
| D4 | UNION / UNION ALL / `unionByName` | `set_expr.rs:134-161` → `builder.rs:869-893` → `union` `:1907` → `Union::try_new_with_loose_types` `plan.rs:3027` | **no** at planning: the loose union takes no common type; it is chosen at idx 10 (`coerce_union`). |
| D5 | INTERSECT / EXCEPT (DISTINCT) | `set_expr.rs:164-173` → `intersect_or_except` `builder.rs:1378` (a semi / anti join on all columns) | **no** at planning; the join keys meet A5 at idx 10 (`coerce_join`), and the output keeps the **left** side's type, so Spark's widened `double` cannot appear. The ALL forms refuse at the statement seat since #1314 (`crates/repark-core/src/pre_execute/setop_all.rs`); they were not measured here. |
| D6 | recursive CTE | `builder.rs:176` `to_recursive_query` → `coerce_plan_expr_for_schema` (`expr_rewriter/mod.rs:221`) to the **static term's** schema | **yes, differently:** no common type; the recursive term is cast to the anchor's type. **Spark does not widen here either:** it keeps a `double` anchor (`rcte_dbl_anchor_one_col` EQUAL) and refuses a decimal anchor with a double recursive term (`CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`, `rcte_dec_anchor`). A fix must exclude `RecursiveQuery`. RePark's `Join Error` on the two-column recursive CTEs is a separate fault. |
| D7 | `IN (subquery)`, scalar subquery | planned as `InSubquery` / `ScalarSubquery`; coerced at idx 10 (`type_coercion.rs:483-505`) | no at planning. |

## E. Optimizer rules that fold or move the evidence

They never choose the type; they act on the casts idx 10 left.

| rule | file:line | effect |
|---|---|---|
| `simplify_expressions` | `simplify_expressions/simplify_exprs.rs:53`; `ConstEvaluator` `expr_simplifier.rs:482, 545` | folds `CAST(<double literal> AS Decimal128(30,15))` at plan time: `1e-20` becomes `0`, `-0.0` becomes `0`, and NaN, ±Infinity and `1e30` fail planning (`Optimizer rule 'simplify_expressions' failed … Cannot cast to Decimal128(30, 15). Overflowing on NaN`). That is where most `repark-raises` cells raise; over a column the same cast fails or truncates at execution. |
| `optimize_unions` | `optimize_unions.rs:41-70` | flattens nested unions under the coerced schema; moves nothing new. |
| `push_down_filter` | `push_down_filter.rs:965-985` | pushes a filter on the union's (decimal) column into each branch, so `WHERE c > 0` over `0.0 UNION ALL 1e-20` compares the already-truncated decimal (cell `extra/subquery_union_filter`). |
| `optimize_projections` | `optimize_projections/mod.rs:85, 303` | merges the cast projections; no type change. |
| `repark_strip_attribute_ids` | `crates/repark-core/src/session/df_guards/attr_id.rs:729-784` | rebuilds every `Union` (`try_new`, then `try_new_by_name`, then loose types) from its inputs' schemas; a fix that types the inputs `Float64` survives it. |

## F. Stored plans re-entering

| store | file:line | seen? |
|---|---|---|
| F1 Spark-door SQL temp view | `crates/repark-spark/src/view_ddl/temp_view.rs:115-134` (re-plans the definition at each scan), creation schema `:169`, `conform_to_creation_schema` `:481` | yes, twice: the definition is re-planned through the Spark door (B), then **cast back to the schema captured at creation**. A fix changes both together in one session; nothing persists. |
| F2 Iceberg (persistent) view | `view_ddl/read.rs:125-147` (`expand_view_body` re-plans the stored SQL text) | yes, at each read through B. A view a pre-fix build created records `decimal(30,15)` in its Iceberg view schema; the build must say whether a stored schema wins over the re-planned type (owner question Q4). |
| F3 DataFrame temp view (`createOrReplaceTempView`) | `crates/repark-core/src/session/temp_views.rs:42` (`DataFrame::into_view`) | yes: the unanalyzed frame plan is stored and analyzed with the outer query (B). |
| F4 `df.cache()`, `CACHE TABLE` | `temp_views.rs:49-67`, `register_collected_memtable` `:146` | the values are materialized at cache time; a cached decimal(30,15) result stays one until re-cached. Not a seat; measured in `rule-lists.json` (`stored_plans`). |
| F5 native `CREATE VIEW` | `datafusion-54.1.0/src/execution/context/mod.rs:931-970` (`apply_type_coercion`, an off-list `Analyzer::with_rules([TypeCoercion])`) | yes, off-list: only bare `type_coercion` runs, so no RePark pre-coercion rule is applied before the stored plan's types are fixed. Native door only. On the Spark door the input reaches it already analyzed by `analyze_eagerly`. |
| F6 PREPARE / EXECUTE | DataFusion PREPARE arm `context/mod.rs:734-763` (**bare optimizer, no analyzer**, `:755`); `pre_execute.rs:95-104` validates and discards | Spark door: the stored body was analyzed by `analyze_eagerly` before the arm (B applies). Native door: the stored body is never analyzed by the session list; `EXECUTE` analyzes the substituted plan. Measured in `rule-lists.json` (`prepare_execute`). Spark 4.1.2 has no `PREPARE`; RePark-only. |

## The one-line answer

The common type is chosen in **two** places: at SQL planning for multi-row `VALUES` (D1), and at
`type_coercion` idx 10 for everything else (B), both through DataFusion's `decimal_coercion` (A1). A
third place **skips** it: a target-typed `VALUES` (D2, and D3's leak) casts each cell straight to the
target column, where Spark forms the common type first (corrected after the critic). The
only existing RePark counter-seat, `widen_decimal_against_float` (B idx 6), covers decimal **literals**
in four comparison shapes. Everything downstream (idx 28, the optimizer, the writes, the stored plans)
inherits the decision.

## G. Revision after the inventory critic (2026-11-13): the doors the first pass missed

The xhigh critic (`VERDICT: GAPS`) measured these on ANSI on; this sketch re-measured each in both ANSI
modes (domain table, section "Revision"). Every file:line below was re-read at `16974986`.

| # | door | file:line | seen? |
|---|---|---|---|
| G1 | `INSERT OVERWRITE` (VALUES, SELECT-FROM-VALUES, UNION sources) | `crates/repark-spark/src/insert_overwrite.rs:453-455` (`overwrite_source_with_default_fills`, then `spark_ast::execute_passthrough(materialize_sql)`) | **yes:** the source is planned standalone, with no target schema in the planner context, so its `VALUES` take D1's `(30,15)` and its union takes idx 10's. Three wrong writes per mode. |
| G2 | `INSERT INTO … BY NAME` | `crates/repark-spark/src/insert_by_name.rs:95, 217` (`execute_passthrough(projection_sql)`) | **yes**, as G1. Two wrong writes per mode. |
| G3 | `DELETE`, `UPDATE … WHERE`, `MERGE … ON` comparing a decimal column with a double column | `crates/repark-iceberg/src/write/predicate_dml.rs:387-412`; `crates/repark-iceberg/src/write/merge/merge_sql.rs:228-238` (`ON {on}` pasted into generated SQL) | **yes:** the predicate is planned through the session list and A5 types `v = d` as `decimal(30,15)`, so `1e-20 = 0.00` holds. Six wrong writes per mode (copy-on-write and merge-on-read). The comparison seat is therefore write-bearing, not read-only. |
| G4 | RTAS (`CREATE OR REPLACE TABLE … AS`) | the CTAS path, `crates/repark-spark/src/ctas.rs` | **yes**, as CTAS: the created column is `decimal(30,15)`. Two wrong writes per mode. |
| G5 | DataFrame `write.insertInto`, `writeTo().overwritePartitions()` | the DataFrame union (D4 at idx 10) | **yes.** Two wrong writes per mode. |
| G6 | nested leaves in set operations and CASE (struct, array, map) | `type_union_coercion` `binary.rs:886` recursing into struct, list and map fields (`struct_coercion`, `list_coercion`, `map_coercion` `:895-901`) at idx 10 | **yes:** each decimal leaf beside a double leaf takes `(30,15)`. Six wrong reads per mode. |
| G7 | Iceberg pruning of a decimal predicate | DataFusion unwraps casts of decimal and integral columns only (`datafusion-expr-common-54.1.0/src/casts.rs:80-98`); the fork strips lossless integral casts only (`expr_to_predicate.rs:382-400`) and converts no decimal literal to an Iceberg datum (`scalar_value_to_datum` `:610-643`, no `Decimal128` arm) | **not a type seat; a cost seat.** Measured on main: `WHERE d = 0.5` and `WHERE d = CAST(0.5 AS DOUBLE)` over a three-file table both push **no** predicate (`predicate:[] N=3`), and a double column pushes `v = 0.5` and reads one file. Widening a decimal column therefore loses no pruning today; widening a decimal **literal** beside a double column gains it. |

Checked by the critic and **not** doors (not re-checked here): MERGE's synthesized rewrite `CASE` (each
SET is cast to the target first, `merge_sql.rs:125` → `insert.rs:330`); identity UPDATE; `meta_delete`
(refuses DECIMAL fields, `conflict_filter.rs:317-331`); `na.fill`; DataFrame `intersect` / `subtract` (they go
through Spark-door SQL over temp views, so the session list covers them); `CALL … rewrite_data_files(where
=> …)` (compaction preserves values).

### The Spark-door planning seats a VALUES marker must reach

A multi-row `VALUES` is typed by `infer_data` / `infer_inner` before any analyzer rule, and the planner's
cast is byte-identical to a user cast (`values_A_planner_cast` and `values_B_user_cast` print the same
initial plan and Spark answers them differently). The revised design marks each cell in the AST, so every
seat that plans user SQL text must apply the marker. `grep -rn --include=*.rs
"plan_statement_with_column_repair\|\.statement_to_plan(\|create_logical_plan(\|ctx\.sql(" crates`
lists them; the ones that plan **user** text are `crates/repark-core/src/column_resolution.rs:94, 131,
137, 142, 193` (the passthrough, which G1 and G2 reach through `execute_passthrough`),
`crates/repark-spark/src/merge_fragments.rs:57, 65` (the MERGE source and conditions), the CTAS / RTAS
query, and the Spark-door view re-planning (`view_ddl/temp_view.rs:415` `plan_definition`,
`view_ddl/read.rs` `expand_view_body`). The other hits in `crates/repark-iceberg/src/write/` plan
generated SQL around a source the passthrough already typed; the build's inventory step confirms each.
The existing AST rewrites of `SetExpr::Values` (`insert_arity.rs:53`, `void_type.rs:131`,
`insert_timestamp_ns.rs:237`, `void_type/ltz_values_store.rs:33`) show the shape.

## H. Step 0 of the build, on the stack base `ff34baf5` (2026-11-14): seen at seat V / seat E

The owner's stack ruling (2026-11-14, "Go with recommendation") builds this unit on DOUBLE-DECIMAL-STORE-ROUND-1's
reviewed head `ff34baf5` (main `6f9f2aaa` plus the store fix). Every RePark line in this section was re-read at
`ff34baf5`; the cites in A–G hold there except where this section moves them. Main is now `340aba98`: #1331 adds
a native-door write refusal (`crates/repark-python/src/session_write_options.rs`, views, database sources and
in-memory overwrites) and touches no seat here, so main's package is the `6f9f2aaa` build. The re-measured table
is [evidence/step0/table.md](evidence/step0/table.md).

### What moved since `16974986`

- **`ff34baf5` adds a consumer of every comparison and union cast.** `bool_decimal_cast`
  (`crates/repark-functions/src/bool_decimal.rs:41-115`; `float_decimal_cast` `:78-115`) is the last rule of
  every Python session's list (installed at `crates/repark-python/src/session.rs:26`: Spark-door idx 44,
  native-door idx 10). It turns every float-to-`Decimal128` cast into `__repark_float_to_decimal__`, the JDK 17
  text kernel, without asking who made the cast: a written cast, a store cast and a **coercion** cast alike. So
  `d = v` now compares `d` with the text-rounded `v`, which is verifier 2's F1 family (it matches Spark for
  some cells and not others; main's Arrow cast does the same with different cells). It is absent from the
  F.expr context (`crates/repark-python/src/column/expr_build.rs:288-322`).
- **#1326 (SIGNED-ZERO-NORMALIZE-1 part B part 1)** adds `repark_optimizer_seat_config`
  (`crates/repark-core/src/session/df_guards.rs:83-145`, installed `:180`), Spark-door idx 41; float-key
  normalisation runs as optimizer rules (`df_guards.rs:158, 169`) on both doors (`extension.rs:146-151`,
  `session_runtime.rs:408-413`). Six of the eight signed-zero shapes moved; the table's signed-zero section
  gives each.
- **Lines that shifted:** `df_guards.rs` context build `:147-159`, `finish_session_build` `:174-181`, the list
  helper `:184-187`; `crates/repark-spark/src/view_ddl/temp_view.rs` scan `:115-143`, `conform_to_creation_schema`
  `:490`, `plan_definition` `:424`.
- **The live lists** ([evidence/step0/rule-lists.md](evidence/step0/rule-lists.md)) are identical on main and
  `ff34baf5`: the Spark door runs 45 rules (6 `spark_integral_literal`; 10 and 28 `type_coercion`; 41
  `repark_optimizer_seat_config`; 42 `signed_zero_literals`; 44 `bool_decimal_cast`), the native door 12 (no
  `spark_integral_literal`; `type_coercion` at 3).

### How this section was searched

- The seven searches above, re-run at `ff34baf5`.
- `grep -rn --include=*.rs "plan_statement_with_column_repair\|\.statement_to_plan(\|create_logical_plan(\|ctx\.sql(\|sql_to_statement\|sql_to_expr" crates`:
  165 hits, 37 outside test modules, each read.
- The Spark-door dispatch, `crates/repark-spark/src/router.rs:292-422`, read arm by arm.
- `grep -rn --include=*.rs "source_from_sql\|selection_sql\|render_for_reparse" crates`, to follow the text
  the write doors re-plan.
- Every "yes" or "no" below that a cell can show is measured in the step-0 table, on main and on `ff34baf5`.

### The answers

| # | door | where the type is created or consumed | seen at seat V | seen at seat E |
|---|---|---|---|---|
| H1 | Spark-door SQL (every statement the router passes through) | `router.rs:292` `execute_inner` → `crates/repark-spark/src/spark_ast.rs:190` `execute_passthrough_inner`: parse `:204`, AST rewrites `:211-235`, plan `:249`, `analyze_eagerly` `:273`, execute `:295` (the second analyzer pass) | **yes, but the pass needs two positions.** `refuse_insert_void_values` (`:222`), `timestamp_typed_values_cells` (`:224`) and `rewrite_insert_markers` (`:227-235`) read literal `VALUES` cells and must see them unwrapped, so for an INSERT the pass sits after `:235` and before `:249`. A `DELETE` or `UPDATE` leaves earlier, at `try_execute_identity_dml` (`:213`), so for those the pass sits before `:213` (their `VALUES` can only be in a subquery, which the INSERT rewrites never read). **A second parse seat:** `restate_range_frames_and_replan` (`spark_ast.rs:551-573`) re-parses the original text for RANGE frames over datetimes and needs the same pass. | **yes**, both passes. **But not through `spark_integral_literal`'s gate.** That rule returns at once unless the plan holds an Int64, UInt64 or decimal literal or a `count(*)` (`crates/repark-spark/src/spark_literal_typing.rs:25-27`, `plan_may_narrow` `:62-80`), so `WHERE d = v` is never walked. The sketch's "shares the existing walk" must read: shares the walk, with its own gate (a DECIMAL and a FLOAT or DOUBLE operand at a unifying node). |
| H2 | DataFrame API | `union` / `unionByName`: `python/repark/src/repark/spark/dataframe/core.py:2581, 2596` → `crates/repark-python/src/dataframe/mod.rs:658, 683` (DataFusion's loose union). `intersect` / `subtract`: `core.py:2608-2628, 2664` (`_sql_binary_set_op` → `sql_built`, Spark-door SQL over temp views). Column functions (`coalesce`, `greatest`, `least`, `when`, `nvl2`, `isin`, `between`) and join conditions build `Expr`s typed only at analysis. The writes (`insertInto`, `writeTo(...).append()` / `overwritePartitions()`) carry the frame's analyzed plan. | no `VALUES` here (an `s.sql("… VALUES …")` frame is H1) | **yes**, every one: the frame plan is analyzed by the session list. Measured: `df_*`, `dfcol/*` and the new `dfx/*` cells. `createDataFrame` forms no engine common type (Python schema inference): not a seat, not measured. |
| H3 | `F.expr`, `selectExpr`, string `filter` | `expr_build.rs:288-322` `build_expr_context`: its own list carries `insert_literal_rule_before_coercion` (`:297`); the fragment is then analyzed again by the session list | **no**: `sql_to_expr_with_alias` (`expr_build.rs:94`) parses an expression outside the passthrough, so a `VALUES` in a fragment's subquery is not marked (not measured; a lead) | **yes**, if the seat-E rule is inserted with `insert_literal_rule_before_coercion` (or beside it in both lists). **Correction to the sketch:** "the F.expr fragments are already right" holds only for a decimal **literal**, because this context does not set `parse_float_as_decimal` and `0.1` is a DOUBLE there. A decimal column or an explicit `CAST(… AS DECIMAL)` in a fragment is wrong on main and on `ff34baf5` (the `dfx/*` cells). |
| H4 | `VALUES`: seat V's planning seats | Through the passthrough (H1): standalone `VALUES`; `INSERT … VALUES` and `INSERT … SELECT` (`router.rs:426` → `:493` `execute_passthrough_by`); CTAS and RTAS (`crates/repark-spark/src/ctas.rs:204`); `INSERT OVERWRITE` (`insert_overwrite.rs:224, 455, 550, 603, 709, 724`); `BY NAME` (`insert_by_name.rs:217`); view bodies (`temp_view.rs:424` → `view_ddl/read.rs:286` → `view_dispatch.rs` `execute_view_body_query` → the passthrough); `DELETE` and `UPDATE` (`router/delete_update.rs:39, 90` → the passthrough → `try_execute_identity_dml`, `spark_ast.rs:373`). | **yes** for all of these, through H1's pass (the DML position for `DELETE` / `UPDATE`). They re-render their predicate and SET text (`render_for_reparse`, `crates/repark-iceberg/src/write/predicate_dml/plain.rs:176, 253`) and re-plan it with `ctx.sql` (`predicate_dml.rs:376`, `predicate_dml/cow_commit.rs:60, 131`); a marker call survives the rendering as a function call and reaches the consumer in the session list. **MERGE: no, not at the sketch's seat.** MERGE leaves the router at `router.rs:388-389` for `crates/repark-spark/src/merge.rs:46` with the parsed AST and never enters the passthrough. Its source text is rendered at `merge.rs:537-560` (`source_table`, `render_for_reparse` `:553`) and executed by `ctx.sql` of generated SQL (`crates/repark-iceberg/src/write/merge/mod.rs:516`, `merge/insert.rs:122`); `merge_fragments.rs:55-65`, which the sketch names, plans only `SELECT * … LIMIT 0` to read the source's column names. **The MERGE marker seat is `merge.rs`, before `source_table` (`:147`).** Seats that plan a source alone to decide void, NULL or timestamp types (`void_type.rs:379, 421, 529`, `void_type/insert_source_types.rs:52, 138`, `void_type/ltz_values_store.rs:471-482`, `update_cast.rs:160`, `insert_overwrite.rs:95`, `scan_prune.rs:280`) read planner types before analysis, which a marker leaves as today's, and decide nothing between DECIMAL and DOUBLE: no. | n/a: seat E never types a `Values` node; it sees the parents after seat V. |
| H5 | set operations | UNION and UNION ALL loose at planning (D4); INTERSECT / EXCEPT DISTINCT as a semi or anti join (D5); recursive CTE (D6) | n/a | **yes**: idx 6 sees the loose `Union` before idx 10 (`rewrite_plan` already rebuilds a stale union, `spark_literal_typing.rs:120-135`) and the semi or anti join (the discriminator, `crates/repark-core/src/session/df_guards/struct_setop.rs:116-160`). `RecursiveQuery` is excluded by design. |
| H6 | CASE, `IF`, `coalesce`, `nvl2`, `nullif`, `greatest`, `least`, `array`, `map` | `Expr::Case` and each UDF's `coerce_types` at idx 10 (A3–A5) | n/a | **yes**: each is an `Expr` the idx-6 walk reaches (`transform_up_with_subqueries`, `spark_literal_typing.rs:28`). `nvl` / `ifnull` stay `double` by accident. |
| H7 | comparisons, `IN`, `BETWEEN`, `IN (subquery)`, join keys | `BinaryExpr`, `InList`, `Between`, `InSubquery` (subquery plans through `transform_up_with_subqueries`), `Join.on` pairs and `filter` | n/a | **yes**, with H1's gate. |
| H8 | the DML predicates | `DELETE` / `UPDATE … WHERE` (H4's re-render, `ctx.sql` on the session context); `MERGE … ON` (`on_sql` rendered at `merge.rs:176`, pasted at `crates/repark-iceberg/src/write/merge/merge_sql.rs:138, 166, 211, 233, 325`) | as H4 | **yes**: every re-plan is `ctx.sql` on the session context, so the session list analyzes it. `scan_prune.rs` prunes integer keys only (`identical_int_key_width` `:248`): not a seat. |
| H9 | consumers after the type is chosen (new at `ff34baf5`) | `bool_decimal_cast` (above); the Iceberg conform stage stores a float leaf into a DECIMAL target through the same kernel (`crates/repark-iceberg/src/write/conform_stage.rs:306-314`, `conform.rs`) | the target branch's `CAST(CAST(x AS DOUBLE) AS DECIMAL(p,s))` is a store cast, which this kernel rounds as Spark does (`values_dec38`) | after seat E no comparison or union casts a float to a decimal, so `bool_decimal_cast` sees only written casts and stores, where it matches Spark. |
| H10 | native door (record only, Q2) | the `repark.sql()` list (`crates/repark-python/src/session_runtime.rs:420-432`; 12 rules, `type_coercion` at 3, `bool_decimal_cast` at 10) | no (no marker; Q2) | no (no idx-6 rule; Q2). **Moved by `ff34baf5`:** the native door's `d = v` now text-rounds `v` through `bool_decimal_cast`, and its `SELECT` / `DELETE` / `UPDATE … WHERE d = v` lose rows that main and Spark keep (the `dml-native` cells). Seat E does not reach this door, so the head keeps them unless the owner rules otherwise (step-0 question N1). |
| H11 | Rust-embedded ANSI door (`crates/repark-sql/src/dialect.rs:21-29`) | plain `type_coercion`; no Python session, so no `bool_decimal_cast` | no | no; unchanged by `ff34baf5`. |
| H12 | **the widening cast itself** (new; every seat-E answer runs through it) | `CAST(<decimal> AS DOUBLE)` is Arrow's `cast_decimal_to_float` (`arrow-cast-58.4.1/src/cast/mod.rs:2319-2321`, the version `Cargo.lock` pins): the unscaled `i128` is rounded to `f64` and then divided by an `f64` power of ten, two roundings. Spark's `Decimal.toDouble` is `BigDecimal.doubleValue()`, correctly rounded. No RePark rule replaces it on either build. | n/a | **yes, and it is wrong at high scales.** Measured ([evidence/step0/table.md](evidence/step0/table.md), `dec_to_double.py`, 5,455 plain-text values): RePark's DECIMAL→DOUBLE differs from Spark's in 967 values at DECIMAL(38,18), 724 at (30,15) and 118 at (38,10), and in none at (20,6) or (10,2), identically on main and `ff34baf5`. A widened comparison or union over a decimal of scale 10 or more therefore inherits a one-ulp error that main's decimal comparison did not have. Seat E is not correct at those scales until this cast is (step-0 question N3). |

### The MERGE … WHEN NOT MATCHED BY SOURCE control

A copy-on-write `MERGE … WHEN NOT MATCHED BY SOURCE THEN DELETE` whose source matches one target row twice
writes that row twice on Spark 4.1.2 with Iceberg 1.11.0 as on RePark (`ctl/cow/merge_by_source_dupkey`: `[1, 1]`
on both; merge-on-read keeps one). So `z/cow/merge_by_source` (`[1, 1, 2, 3, 3]` on main, `[1, 2, 3]` on Spark)
is this unit's coercion fault (1e-20 matches 0.00 under `decimal(30,15)`), not a MERGE fault.

Up: [map.md](map.md).
