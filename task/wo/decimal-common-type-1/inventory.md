# DECIMAL-COMMON-TYPE-1 — inventory of the seats that pick a decimal common type

The insertion-inventory rule: every seat that picks one type for two or more decimals, or for an integer and a
decimal, answers "seen at the seat: yes / no, because", with file:line. "Seen" means: would a fix placed in
`SparkDecimalFloatWiden`'s walk (seat A of the [sketch](sketch.md)) see the operands before this seat picks the
type? Lines are DataFusion 54.1.0 (the crates.io sources in the cargo registry) and RePark main `e50ed17f`.

**How searched:** `grep` for `comparison_coercion`, `type_union_coercion`, `type_union_resolution`,
`try_type_union_resolution`, `binary_numeric_coercion`, `decimal_coercion` and `get_wider_decimal_type` over
`crates/` and the DataFusion 54.1.0 crates (`datafusion-expr-common`, `datafusion-expr`, `datafusion-optimizer`,
`datafusion-functions`, `datafusion-functions-nested`); `grep` for every `fn coerce_types` in `repark-functions`;
the analyzer and optimizer rule lists read from `crates/repark-spark/src/extension.rs` and
`crates/repark-core/src/session/df_guards.rs`; the DataFrame, DML and native entry points read from
`repark-python` and `repark-iceberg`. Each "yes" was confirmed by a measured cell in the
[domain table](domain-table.md) that answers through that seat.

## DataFusion's rule (the source of every wrong cell)

| seat | file:line | picks | seen? |
|---|---|---|---|
| `get_wider_decimal_type` | `datafusion-expr-common/src/type_coercion/binary.rs:1160` | `(range + scale, scale)`, then `create_decimal128_type` (`:1508`) caps precision at 38 and keeps the scale | **yes**, because every caller below reaches it only after the analyzer rules inserted before `type_coercion` ran; the fix casts the operands first so the types are equal and this function is not consulted (`comparison_coercion` returns early on equal types, `:925`) |
| integer operands | `binary.rs:1227` (`coerce_numeric_type_to_decimal128`; `Int64` → `decimal(20,0)` at `:1235`) | `TINYINT` (3,0), `SMALLINT` (5,0), `INT` (10,0), `BIGINT` (20,0) — Spark's forms | **yes**, the same way; the fix needs these forms and **one exception**: an integer *literal* in a comparison takes its own digits (measured: `w30 = 1`, `h = 1` agree on main only because both engines keep the decimal's scale) |
| `decimal_coercion` | `binary.rs:1039`, from `binary_numeric_coercion` `:1018` | the two above | yes |
| `comparison_coercion` | `binary.rs:924` | `=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>` via `coerce_binary_op`; `IN` lists (`analyzer/type_coercion.rs:671` → `type_coercion/other.rs:41`); `BETWEEN` (`type_coercion.rs:633-647`); `IN (subquery)` (`:495`) and quantified comparisons (`:531`); simple `CASE` operand against `WHEN` (`other.rs:53`); join keys (`coerce_join`, `type_coercion.rs:193`); `greatest`/`least` (`datafusion-functions/src/core/greatest_least_utils.rs:119`, `:130`) | **yes** for every one: all are expressions or join keys the walk visits (`is_candidate`, `decimal_float_widen.rs:114`; `join_widening`, `:372`); measured wrong on main at every one |
| `type_union_coercion` | `binary.rs:886` | `UNION` / `UNION ALL` schemas (`coerce_union_schema_with_schema`, `type_coercion.rs:1190`, fold at `:1234`); `CASE` `THEN`/`ELSE` (`other.rs:65`, `:73`) | **yes**: `rewrite_union` (`decimal_float_widen.rs:302`) and `widen_case` (`:500`); the union is visited per binary node bottom up ([evidence/raw/explain-union-repark.txt](evidence/raw/explain-union-repark.txt): the initial plan nests `Union(Union(c, d), a)` as Spark does), which reproduces Spark's double rounding |
| `type_union_resolution` / `try_type_union_resolution` | `binary.rs:604`, `:786` | `coalesce` (`datafusion-functions/src/core/coalesce.rs:137-142`), `make_array` / `array` (`datafusion-functions-nested/src/make_array.rs:244-249`), multi-row `VALUES` (`datafusion-expr/src/logical_plan/builder.rs:269`, `:328`) | functions **yes** (`ALL_ARGUMENTS`, `decimal_float_widen.rs:17`); `VALUES` **no**, because the planner types the `Values` node before any analyzer rule runs — only seat V (below) sees it |
| `nvl` / `ifnull` signature | `datafusion-functions/src/core/nvl.rs:62-77`, `:88-96` | `Signature::uniform` over a list with **no decimal**: both decimals become `double` | the walk sees `nvl` (`ALL_ARGUMENTS`) but casting to Spark's decimal does not help: the signature still coerces to `double`. **Another cause** (`nvl_same` agrees in value, not type); card it |
| `nullif` signature | `datafusion-functions/src/core/nullif.rs:76-82` | `Signature::comparable`: the result is the coerced type (Spark returns the first argument's type) | the walk sees it (`widen_function`, `:589`, rewrites `nullif` to `CASE`); the result-type rule is sweep family 6 |
| `EXCEPT` / `INTERSECT` | planned as `LeftAnti` / `LeftSemi` over `Distinct` (keys via `coerce_join`) | the keys compare in the common type; the output keeps the **left** side's type (Spark: the common type) | the walk sees it (`is_set_operation`, `:350`, projects both sides); the output type is sweep family 5 |
| window `RANGE` frame offset | `type_coercion.rs:926` (`coerce_frame_bound`), `:965` | the offset is cast to the key's type | not a common type; main refuses a decimal offset of any decimal type before reaching it (measured `range_same_type`); not this unit |
| `create_map` / `map_concat` / `array_insert` (RePark) | `repark-functions/src/collection/create_map.rs:63`, `map_concat.rs:80`, `array_insert.rs:59-62` | `comparison_coercion` of the values | `map` **yes** (`MAP_CONSTRUCTORS`, `decimal_float_widen.rs:27`); `map_concat` and `array_insert` **no**, because they are not in the walk's lists: add them (measured: `map_concat` wrong on main) |
| `array_contains` (RePark) | `repark-functions/src/collection/array_contains.rs:50`, `coerce_types` at `:76` | the needle against the array's element type | **no**, not in the walk's lists; measured wrong on main; add it |
| `array_union` | `datafusion-functions-nested/src/set_ops.rs` | unifies the element types | answers where Spark refuses (measured `M`); not a common-type fix: card |
| struct fields in a union | `type_union_coercion` → `struct_coercion` (`binary.rs:1265`) | field-wise | **no**: `union_positions` (`decimal_float_widen.rs:250`) classes a whole field, so a struct field is `Other`; measured `u_struct` wrong; the shared function must recurse into struct fields (or card it) |
| struct position normaliser (RePark) | `repark-spark/src/normalize/struct_position/forms.rs:258`, `struct_position.rs:954` | `comparison_coercion` of struct pairs | runs after the walk (`extension.rs:163`); a struct comparison is out of the measured set; a build pin |

## RePark's own seats

| seat | file:line | role | seen? |
|---|---|---|---|
| `SparkDecimalFloatWiden` | `crates/repark-spark/src/extension/decimal_float_widen.rs:30`, inserted before `type_coercion` at `extension.rs:160` (`insert_decimal_float_widen_before_coercion`, `decimal_float_widen.rs:46`) | detects with `plan_widens` (`:237`) / `node_widens` (`:206`) / `expr_widens` (`:126`), rewrites with `transform_up_with_subqueries(rewrite_plan)` (`:656`) | **the seat** (seat A). Today `mixed` (`:79`) needs a decimal **and** a float, so a decimal pair never enters. The file is 912 lines on `e50ed17f` (#1341 added `DecimalInRanges`, `:680-909`), not 661 |
| `rewrite_union` / `rebuild_stale_union` (#1341) | `decimal_float_widen.rs:302`, `:317` | projects each branch's decimal positions to `double`; rebuilds a union whose schema went stale | yes; the decimal-pair case projects to Spark's common type instead of `double`, through the same `project_to_double` shape (`:277`) generalised to a target type |
| join keys / set operations | `join_widening` `:372`, `rewrite_join` `:395`, `is_set_operation` `:350` | casts keys; projects both sides of `EXCEPT` / `INTERSECT` | yes; `USING` on a full outer join refuses on main before (measured), so it cannot be pinned until that refusal is fixed |
| `DecimalInRanges` (optimizer, #1341) | `decimal_float_widen.rs:756`, registered `extension.rs:181` | rewrites `__repark_decimal_to_double__` `IN` lists | not a common-type seat; it acts only on the double form, which the decimal-pair path never produces |
| seat V, `SparkValuesTyping` | `crates/repark-spark/src/values_marker/consume.rs:20`, inserted first (`insert_values_typing_first`, `:44`, at `extension.rs:166`) | retypes a marked multi-row `VALUES` column; `column_placement` (`:254`) folds `type_union_resolution` (`:275`) and returns `None` unless the column mixes decimal and float (`:278`) | **yes, through one change**: `column_placement` calls the shared function for an all-decimal / integer column and places it `Standalone` (retype the field, cast each cell) or `Target` (`INSERT … VALUES`, `type_insert_source`, `:90`: cast each cell to Spark's type, then the planner's cast to the target). Measured `v_three`: `VALUES` rounds **once** per row (an n-ary fold), unlike nested unions |
| the literal typing rules | `crate::spark_literal_typing` (`extension.rs:159`) | types a plain `1.25` as `decimal(3,2)` and an integer literal as Spark does | read, not changed: the shared function needs to know an integer literal from an integer column (`Expr::Literal` at the walk) |
| #1345's shield | `crates/repark-core/src/session/df_guards/cast_shield.rs:25`; analyzer rule appended last (`df_guards.rs:211`), optimizer rule before `simplify_expressions` and at the end (`df_guards.rs:238`, `:250`); `is_narrowing` `cast_shield/udf.rs:167` | wraps a narrowing cast of a column compared with a constant so the simplifier cannot unwrap it, with a `preimage` for pruning | **yes, it keeps this unit's casts**: a cast of `DECIMAL(10,2)` to `decimal(38,0)` is narrowing (`to_scale < scale`, `udf.rs:170`) and reads input, so `shield_operand` (`cast_shield.rs:188`) wraps it in a comparison with a constant (`shield_comparison`, `:218`; `IN` lists `:254`; `BETWEEN` `:239`). The constant side's cast folds. Pin it: the 18 column-loses-digits constant cells on `t` (`c = CAST('1' AS DECIMAL(38,0))` and its eight siblings, select list and `WHERE`) on the head |
| `FoldSparkNumericCasts`, `StoreOverflowCast`, `InsertStoreAssignment`, `FloatDecimalStoreRule` | `extension.rs:183`, `:188`, `:191`, `df_guards.rs:206` | store casts after the source is typed | not common-type seats: they see the union's type once the walk fixed it; measured wrong writes all trace to the source's type |

## Doors

| door | path | seen? |
|---|---|---|
| Spark-door SQL | `SparkExtension::configure_analyzer_rules` (`extension.rs:154-167`) | yes |
| DataFrame API | `PyDataFrame.union` / `union_by_name` (`crates/repark-python/src/dataframe/mod.rs:658`, `:683`) build a binary `Union` through DataFusion's builder (`datafusion-expr/src/logical_plan/builder.rs:1907`); `when`, `isin`, `between`, `coalesce`, `greatest`, `join` build expressions analysed by the same session | **yes**: the frame's session is the Spark door's, so the same analyzer runs; measured wrong on main, control pair agrees |
| `F.expr` / `filter(str)` | the same session's SQL expression planner | yes |
| Iceberg stores | `INSERT … SELECT`, CTAS, RTAS, `INSERT … VALUES` plan their source through the session | yes (source typing); seat V for `INSERT … VALUES` |
| Iceberg DML | `DELETE` / `UPDATE` predicates rendered and run through `ctx.sql` (`crates/repark-iceberg/src/write/predicate_dml.rs:376`); `MERGE` builds SQL text (`write/merge/merge_sql.rs`) | **yes**: the statements re-enter the session analyzer; part 1's DML pins (`python/repark/tests/sql/test_decimal_double_coercion_1.py:53-66`) prove the walk reaches them |
| Iceberg file pruning | the fork's predicate conversion (`expr_to_predicate.rs`, rev `b6e578d`, per Fable's reading) pushes through a cast only for lossless pairs | not a type seat; with #1345's shield the cast is a function with a `preimage`, so pruning uses the exact range; measured: the partitioned and plain tables answer alike on main |
| native `repark.sql()` | `PyReparkSession::native` (`crates/repark-python/src/session_tests.rs:47` pins it "not Spark-doored") | **no**, because the native session installs no `SparkExtension`, so neither the walk nor seat V runs; measured 612 of 655 cells wrong, the same classes. Out of scope as in parts 1 and 2: card it |

Up: [map.md](map.md).
