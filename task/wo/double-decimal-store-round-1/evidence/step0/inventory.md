# DOUBLE-DECIMAL-STORE-ROUND-1 step 0 — the inventory

By the insertion-inventory rule (owner, 2026-11-09). The property: **a FLOAT or DOUBLE value becomes a
DECIMAL value.** For each seat: file:line, whether it rounds the binary value or the text, and "seen: yes /
no, because". Dependency sources are the cargo registry copies the lock pins: `arrow-cast-58.4.1`,
`datafusion-*-54.1.0`, and the iceberg-rust fork at `92242cb4`. RePark lines are at `2707924e` (main when
this step was cut).

## How the inventory was searched (repeatable)

1. **The kernel.** `grep -n "fn cast_floating_point_to_decimal\|\.round()" arrow-cast-58.4.1/src/cast/decimal.rs`
   and its dispatch `grep -n "cast_floating_point_to_decimal" arrow-cast-58.4.1/src/cast/mod.rs`.
2. **Every DataFusion path into the kernel:** `grep -n "DEFAULT_CAST_OPTIONS\|safe:"` in
   `datafusion-physical-expr-54.1.0/src/expressions/{cast,try_cast}.rs`; `grep -n "pub fn cast_to"` in
   `datafusion-common-54.1.0/src/scalar/mod.rs`; `grep -n "struct ConstEvaluator\|Expr::Cast"` in
   `datafusion-optimizer-54.1.0/src/simplify_expressions/expr_simplifier.rs`; `grep -n "Float64 =>
   Some(Decimal128\|Float32 => Some(Decimal128"` in `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs`.
3. **Every direct Arrow cast call in RePark:** `grep -rn --include=*.rs -E "cast_with_options\(|compute::cast\(|
   kernels::cast::cast|\bcast\(&|arrow_cast::cast|cast::cast\(" crates` less `tests/` (66 lines, 36 files);
   each target type read at the call.
4. **Every RePark cast builder or folder:** `grep -rln --include=*.rs -E "Expr::Cast|Expr::TryCast|
   cast_with_options|CastOptions" crates`, then the ones that build or fold a cast to a decimal target:
   `grep -rn "fn store_assignment_cast_sql\|fn store_assignment_then_sql\|store_cast_udf_for_target\|
   FoldSparkNumericCasts\|nullable_spark_cast\|fn cast\b\|fn try_cast\b" crates`.
5. **Every analyzer list that holds a cast folder:** `grep -rn "FoldSparkNumericCasts\|
   install_shared_analyzer_rules" crates` (the session list, the F.expr context list, a test).
6. **The iceberg-rust fork:** `grep -rn "cast_with_options\|compute::cast\b\|kernels::cast" crates/iceberg/src`.
7. **Measured confirmation:** each door's error text in [raw/](raw/map.md) names the seat that raised it
   (Arrow's own text, `Optimizer rule 'simplify_expressions' failed`, or `INSERT OVERWRITE cast of source
   column`), and `EXPLAIN VERBOSE` on the literal shapes shows which rule folds (below, C2).

## A. The kernel every door reaches (Arrow 58.4.1)

| # | seat | file:line | rounds | seen? |
|---|---|---|---|---|
| A1 | `cast_floating_point_to_decimal` | `arrow-cast-58.4.1/src/cast/decimal.rs:764-802`; `mul = 10^scale` `:775`; `D::Native::from_f64((mul * v.as_()).round())` `:780` (safe) and `:788` (strict); precision check `:798` | **neither the text nor the exact binary value: the f64 product `10^s · v`, itself rounded to a double, then `f64::round` (half away from zero).** `1.255 · 100 = 125.49999999999999` gives 125 (Spark 1.26); `2.675 · 100 = 267.5` gives 268 (agrees by accident); `1.005 · 10^18` gives `1.004999999999999872`. A non-finite value fails `from_f64`: strict raises `Cannot cast to Decimal128(p, s). Overflowing on NaN`, safe gives NULL. | **yes — this is the fault.** |
| A2 | the dispatch | `arrow-cast-58.4.1/src/cast/mod.rs:2412-2423` (`Float32`, `Float64` arms) | a FLOAT is widened to f64 (`v.as_()`) and takes A1 | yes, through A1. |

## B. DataFusion 54.1.0

| # | seat | file:line | seen? |
|---|---|---|---|
| B1 | physical `CAST` | `datafusion-physical-expr-54.1.0/src/expressions/cast.rs:37-45` (`DEFAULT_CAST_OPTIONS`, `safe: false`), `evaluate` `:246` | **yes:** every unfolded `Expr::Cast` to a decimal; overflow and non-finite values raise. |
| B2 | physical `TRY_CAST` | `datafusion-physical-expr-54.1.0/src/expressions/try_cast.rs:90` (`safe: true`) | yes: the same A1 rounding; overflow and non-finite give NULL. |
| B3 | `ScalarValue::cast_to` | `datafusion-common-54.1.0/src/scalar/mod.rs:4216-4221` (a one-row array through A1) | yes: every literal fold (B4, C2). |
| B4 | constant folding | `datafusion-optimizer-54.1.0/src/simplify_expressions/expr_simplifier.rs:482` (`ConstEvaluator`), `:652` (casts are foldable) | **yes:** the literal doors (`sel_cast_lit`, `ins_values`, the UPDATE literals) fold here; an overflow row raises `Optimizer rule 'simplify_expressions' failed` (measured). A RePark UDF that is `Immutable` folds here too, so a seat that replaces the cast keeps the folding. |
| B5 | the float-meets-decimal common type | `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs:1227-1239` (`Float32 => Decimal128(14, 7)`, `Float64 => Decimal128(30, 15)`) | **yes, a second rounding:** where a float meets a decimal (the `UPDATE` rewrite's `CASE` over the old decimal value, `CASE`/`coalesce`/`UNION` with a decimal) the float is cast to `(30,15)` or `(14,7)` through A1 first, then rescaled half-up. Measured on the `update` door: DOUBLE `1.255` gives `1.26` by accident; FLOAT gives 14 wrong values per mode and DOUBLE `2.82879384806159E17` into `(38,0)` gives `...003`. The type choice is the DECIMAL-DOUBLE-UNION-COERCION-1 unit's; the rounding inside is this unit's. |
| B6 | the iceberg-rust fork's read promotion | `crates/iceberg/src/arrow/field_id_stamping.rs:440-455` (`promote_array`) | no, because it promotes a data file column to the table type on read, and Iceberg has no float-to-decimal promotion. |

## C. RePark plan seats (an `Expr::Cast` in an analyzed plan)

| # | seat | file:line | seen? |
|---|---|---|---|
| C1 | the Spark-door `CAST` | SQL `CAST` lowers to `Expr::Cast`; `spark_decimal_rewrite` (`crates/repark-functions/src/decimal_spark.rs:120-124`) wraps a non-nullable operand in `__repark_decimal_cast_nullable__` (`crates/repark-functions/src/decimal_cast.rs:16-38`; `decimal_cast_can_overflow` `:130-152` is true for every float, `:144-149`) and keeps the `Expr::Cast` | **yes:** no RePark kernel; B1 or B4, then A1. |
| C2 | `fold_spark_numeric_casts` | `crates/repark-spark/src/spark_typed.rs:22-84` (`scalar.cast_to` `:77`, decimal targets foldable `:60-74`); session list `crates/repark-spark/src/extension.rs:175`; F.expr list `crates/repark-python/src/column/expr_build.rs:303` | **yes, on the F.expr list only.** On the session list C1 runs first, so a float literal under a decimal cast is a `ScalarFunction`, not a literal, and is not folded here (measured `EXPLAIN VERBOSE` of `CAST(1.255D AS DECIMAL(10,2))` and `CAST(CAST(1.255 AS DOUBLE) AS DECIMAL(10,2))`: the outer cast survives every analyzer rule and folds in B4). In the F.expr list C2 runs before `spark_door_post_coercion_rules`, so `F.expr("CAST(1.255D AS DECIMAL(10,2))")` folds through B3 here. Not measured in the table. |
| C3 | store assignment | `repark_insert_store_assignment` (`crates/repark-iceberg/src/write/insert_gate.rs:28`); UPDATE `store_assignment_cast_sql` (`crates/repark-iceberg/src/write/update_cast.rs:13-30`); MERGE `store_assignment_then_sql` (`crates/repark-iceberg/src/write/merge/insert.rs:330-332`); the store kernels are integer targets only (`crates/repark-iceberg/src/write/store_cast.rs:478-486`), so `repark_store_overflow_cast` (`store_overflow.rs:47`, `store_issue` `:69` needs a kernel) leaves a decimal target alone | **yes:** every INSERT, CTAS, UPDATE, MERGE and DataFrame append store into DECIMAL is a plain `CAST` (B1, A1). Measured: the same Arrow text on all of them. |
| C4 | the DataFrame API | `Column.cast` `crates/repark-python/src/column/mod.rs:823-834` (`Expr::Cast`); `Column.try_cast` `:844-856` (`Expr::TryCast`) | yes: `df_cast` measured; `try_cast` is B2. |
| C5 | the template seat | `bool_decimal_cast`, `crates/repark-functions/src/bool_decimal.rs:23-88`: an analyzer rule appended last by `install_shared_analyzer_rules` (`:28-31`, called at `crates/repark-python/src/session.rs:26`) that rewrites `Cast(Boolean → Decimal)` into a UDF | sees every `Expr::Cast` into DECIMAL on the session list after both `type_coercion` passes, the store rules and C2; today it acts on BOOLEAN only. |
| C6 | container casts | map casts: `rewrite_map_casts` (`crates/repark-functions/src/cast_map/rewrite.rs:295`) → `spark_cast` complex arm (`cast_map.rs:306`) or leaf (`cast_map/leaf.rs:454`), both A1; array and struct casts are Arrow's recursive cast, A1 per leaf; map-key conform `crates/repark-functions/src/collection/key_by_position.rs:228` | **yes, not reachable from a scalar seat:** the leaf is cast inside Arrow or inside a UDF, never as an `Expr::Cast` of a float. Measured (`cast_array`, `cast_struct`, `cast_map_value`, `cast_map_key`). |
| C7 | the struct-key subscript's leaf | `spark_map_subscript` (CAST-DOUBLE-TO-DECIMAL-ROUNDING-1, ledger C-044) | yes; measured `subscript_struct`, `subscript_array`. Whether the leaf is an `Expr::Cast` (then C5 reaches it) or a nested cast (C6) is read in the build. |

## D. RePark batch seats (Arrow on arrays, outside any plan)

| # | seat | file:line | seen? |
|---|---|---|---|
| D1 | the write conform store cast | `entry_store_cast` `crates/repark-iceberg/src/write/conform.rs:268-279` (`cast_with_options`, strict) | **yes — a second seat no plan rule reaches.** Callers: `conform.rs:175` (append conform), `overwrite.rs:164` (INSERT OVERWRITE, all columns), `overwrite.rs:223` (column list: DataFrame `overwritePartitions`), `partition_overwrite.rs:91-102` (static-partition INSERT OVERWRITE). Measured: `ins_overwrite` and `df_overwrite` raise `INSERT OVERWRITE cast of source column ... failed`, so their source batch reaches here as DOUBLE. |
| D2 | the static partition value | `crates/repark-iceberg/src/write/static_value.rs:21-34` | no, because `PartitionLiteral` has no float variant (`partition_overwrite.rs:39-48`). |
| D3 | the Postgres write chokepoint | `crates/repark-core/src/session/write_postgres.rs:550` | yes for a DOUBLE column into a `NUMERIC` target; a door outside this table (C-4 connector), not measured. |
| D4 | the rest of step 3's hits | `temp_views.rs:549` (cache materialize, casts only where the batch and plan types differ), `transpose.rs:229` (its own common type), `lambda_rebind.rs:661` (to the input-derived return type), `write/distribution.rs:93` (to the column's own type), `store_cast.rs:274` (integer targets), the temporal and string casts | no, because none takes a float source to a decimal target. |

## What a fix must reach

Every door in the table ends in A1. Two kinds of seat feed it: a plan `Expr::Cast` / `Expr::TryCast` (B1-B5,
C1-C4, C7 if scalar) and a batch cast (D1). A seat at C5 (end of the session list) reaches every plan door
measured here, including the UPDATE door's `(30,15)` intermediate; it does not reach D1, the F.expr fold
(C2), container leaves (C6) or the Postgres chokepoint (D3).

Up: [map.md](map.md).
