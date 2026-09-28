# map — repark-functions/src/integer_spark

## Purpose

File-backed submodules of `../integer_spark.rs` (Spark integer `+` / `-` / `*` overflow,
F-Y10-1). A rule that shares the integer UDF names but has its own seat in the analyzer lives
here so `integer_spark.rs` stays under its `check_rust_file_size` ceiling.

## Contents

- `fractional_division.rs` — **WO INTDIV-1 (2026-09-28):** `SparkFractionalDivision`
  (`spark_fractional_division`). Spark types `/` over two integers as DOUBLE; DataFusion types it
  as the integer operand type until `SparkExprSemantics` rewrites it, which runs after
  `TypeCoercion` and after `SparkIntegerOverflow`. Anything that reads the operand type first
  therefore baked integer math into the plan: the integer `ExprPlanner` armed `h * 2` over a
  derived-table, CTE or view column `h = id / 2` with truncating `CAST(h AS BIGINT)`s, and
  `TypeCoercion` cast a derived INT quotient to BIGINT for `sum(h)`. The rule is seated
  pre-coercion (first of the `analyzer_rules_with_higher_order_preparation` inserts, immediately
  before `higher_order_preparation`) and walks every plan node bottom-up with subqueries, so each
  scope's schema is DOUBLE before its parent is read:
  1. an integer `/` integer `BinaryExpr` (no lambda variable inside) becomes
     `CAST(l AS DOUBLE) / CAST(r AS DOUBLE)` — the same shape `SparkExprSemantics` produced, which
     then only adds its zero-divisor guard;
  2. a `__repark_spark_int_{add,sub,mul}__` call whose cast-wrapped operand is now a non-integer
     number is unarmed back to the plain `BinaryExpr` of the uncast operands, which the following
     `TypeCoercion` types as Spark does (DOUBLE).
  A plan with no `/` anywhere is returned untouched. The ANSI (Trino) door never seats the rule,
  so its integer division is unchanged; genuinely integral arithmetic never reaches either branch,
  so its overflow behaviour (`ARITHMETIC_OVERFLOW` under ANSI) is unchanged. The inline tests build
  the production analyzer order (the pre-coercion seat plus `analyzer_rules()`) over a
  BIGINT/INT fixture and pin Spark's recorded values and types.
  pins: intdiv-1/C-001, C-002, C-003
