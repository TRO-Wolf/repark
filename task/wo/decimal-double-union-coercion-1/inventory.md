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
| 7–9 | `join_condition_refusals`, `spark_map_subscript`, `struct_operands_by_position` | no. |
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
| D2 | `INSERT … VALUES` (target schema in the planner context) | `values.rs:53-57` → `values_with_schema` `builder.rs:243` → `infer_values_from_schema` `:269` | **no**, because each cell is cast straight to the target column type; no common type is formed. Measured: `ins_values_*` write Spark's bits. |
| D3 | `INSERT … SELECT` | `statement.rs:2358` `insert_to_plan`; `set_table_schema` `:2463`; the source column cast to the target `:2479` | **no** at this seat: it casts the already-typed source; the loss happened in A3 / A4 inside the source query. A nested `VALUES` in the source takes the target schema too (D2 path): measured, `ins_select_values_tiny` writes Spark's bits by that accident, and `ins_case_tiny` (a one-column `VALUES` under a two-column target) raises `Inconsistent data length across values list`. |
| D4 | UNION / UNION ALL / `unionByName` | `set_expr.rs:134-161` → `builder.rs:869-893` → `union` `:1907` → `Union::try_new_with_loose_types` `plan.rs:3027` | **no** at planning: the loose union takes no common type; it is chosen at idx 10 (`coerce_union`). |
| D5 | INTERSECT / EXCEPT (DISTINCT) | `set_expr.rs:164-173` → `intersect_or_except` `builder.rs:1378` (a semi / anti join on all columns) | **no** at planning; the join keys meet A5 at idx 10 (`coerce_join`), and the output keeps the **left** side's type, so Spark's widened `double` cannot appear. The ALL forms refuse at the statement seat since #1314 (`crates/repark-core/src/pre_execute/setop_all.rs`); they were not measured here. |
| D6 | recursive CTE | `builder.rs:176` `to_recursive_query` → `coerce_plan_expr_for_schema` (`expr_rewriter/mod.rs:221`) to the **static term's** schema | **yes, differently:** no common type; the recursive term is cast to the anchor's type. |
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
`type_coercion` idx 10 for everything else (B), both through DataFusion's `decimal_coercion` (A1). The
only existing RePark counter-seat, `widen_decimal_against_float` (B idx 6), covers decimal **literals**
in four comparison shapes. Everything downstream (idx 28, the optimizer, the writes, the stored plans)
inherits the decision.

Up: [map.md](map.md).
