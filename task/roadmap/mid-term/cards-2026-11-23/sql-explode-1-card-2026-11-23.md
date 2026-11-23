# Card SQL-EXPLODE-1: `explode` and `explode_outer` in a SQL select list are unresolved on the SQL doors

**Date:** 2026-11-23. **Filed and built by:** the builder (`claude-opus-5-5`), branch
`fix/sql-explode-extract-fields-1` off main `efa72bb7`, together with
[EXTRACT-FIELD-ALIASES-1](../cards-2026-11-19/extract-field-aliases-1-card-2026-11-19.md). **Source:** a
calendar-dimension build that could not run on RePark: `SELECT explode(sequence(DATE 'a', DATE 'b', INTERVAL 1
DAY)) AS d`, then a projection of date parts of `d`, among them `CAST(extract(YEAROFWEEK FROM d) AS SMALLINT)`.
**Severity:** S2 (loud: every spelling refuses).

**Status:** **FIXED on the branch** (not merged).

## The defect

On main `efa72bb7`, `explode` and `explode_outer` in a SQL select list raise `[UNRESOLVED_ROUTINE] Cannot resolve
routine `explode``, even `SELECT explode(array(1, 2))`, on `spark.sql`, `selectExpr` and `F.expr`. The typed
`F.explode` / `F.explode_outer` answer through their own `unnest` path. `posexplode`, `inline` and `json_tuple`
already resolve there through `GeneratorRewrite` (FNP-GEN-1); `explode` was never one of its names.

## Measured

Literals (empty, NULL, NULL elements, nested, beside other columns, the `sequence` form over a leap day, a year end,
the New York daylight-saving changes, pre-1970 and the ISO week-53 years), a temp view, an Iceberg table, a CTE, a
subquery, a projection over a generator subquery, maps, and the write statements read cold; the oracle is
[`sql_explode_1_spark_oracle.json`](../../../../python/repark/tests/functions/sql_explode_1_spark_oracle.json)
(PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13:1.11.0`, a local Hadoop catalog, session zones `UTC` and
`America/New_York`, ANSI on and off). Spark answers every cell the same in the four modes.

## The fix

`crates/repark-functions/src/generator.rs` (with `__repark_gen_ordinality` / `__repark_gen_field` split into
`crates/repark-functions/src/generator/udfs.rs`): `explode` and `explode_outer` join the placeholder UDFs, and
`GeneratorKind::Explode` expands them as `posexplode` does without the position column: an `ARRAY` projects
`col` (a single `AS` alias renames it), a `MAP` projects `key` and `value`, and the outer spelling turns a NULL or
empty input into one NULL row. Outer guards widen non-nullable list element fields (`sequence`, `split`, etc.) to
nullable and build the one-NULL-element list with `ListArray::try_new` (fixing `explode_outer` and
`posexplode_outer` over non-nullable-element lists without panicking; `ordinality` likewise uses
`ListArray::try_new`). Over an `ARRAY` the placeholder types as the element (Spark's nullability: the element's, or
nullable for the outer spelling) and exposes `col` as its plan `schema_name`, so unaliased outer subqueries
(`SELECT *`, `SELECT col`, `WHERE col`, `GROUP BY col`), `ORDER BY col`, `CREATE TABLE … AS SELECT`,
`INSERT INTO … SELECT`, and `INSERT OVERWRITE … SELECT` resolve against `col` before `GeneratorRewrite` runs.
Explicit user aliases on `explode` / `explode_outer` (including names starting with `explode(` / `explode_outer(`)
are kept, and a single alias like ``AS `posexplode(custom)` `` refuses `[COLUMN_ALIASES_MISMATCH]` rather than
being dropped. The Spark door, `selectExpr`, and the write statements get them; `F.expr` gets them over a table or
view with `.alias(...)`. The typed path is unchanged.

On the head every scoped cell on `spark.sql`, `selectExpr`, and the write statements answers as Spark in the four
modes (rows, names, types), including `explode_outer` / `posexplode_outer` over `sequence` and `split`, unaliased
subqueries / `ORDER BY col` / `INSERT INTO` / `INSERT OVERWRITE` / `CTAS`, explicit `explode(…)` aliases, and the
calendar-dimension shape: a projection of `year`, `extract(YEAROFWEEK …)` and `extract(DAYOFWEEK …)` cast to
`SMALLINT` and `weekofyear` over `explode(sequence(DATE, DATE, INTERVAL 1 DAY))`, and its CTAS and
`INSERT … SELECT` into a local Iceberg table read back cold (a fresh session reading the table path; the stored
`SMALLINT` is an Iceberg `int`, as in Spark).

## Pins

`python/repark/tests/functions/test_sql_explode_1.py` over the oracle, driven by the recorder
`_record_sql_explode_1.py` (with `test_extract_field_aliases_1.py`: 94 items total, all green on the head and red
on main, on the pre-delta PR head, and on both mutants); Rust `repark-functions` `generator::tests` (30 tests,
including non-nullable-element outer lists, unaliased subquery / `ORDER BY col`, and explicit `explode(…)` aliases).

## Still unlike Spark (out of this unit's seat)

- An `array(...)` or `map(...)` literal's element is nullable, so `explode(array(1, 2))` types `col` nullable
  where Spark says not (typed `F.explode` the same on main); `sequence` keeps its element non-null. `dayofmonth`
  over an exploded date is nullable where Spark says not.
- `F.expr("explode(...)")` over a `spark.range` frame raises `No field named __repark_gen_col`; unaliased over a
  view it answers Spark's rows with both facade and engine column name `explode(arr)` (Spark `col`), and unaliased
  `F.expr("explode(m)")` on a map refuses `[COLUMN_ALIASES_MISMATCH]` because `F.expr` attaches its display alias.
  Over a view with `.alias(...)` it answers as Spark. `F.expr("<any expression> AS x")` refuses for every expression.
- Multi-column generators (`explode` on a `MAP`, `posexplode`, `inline`) still emit a single placeholder column
  before `GeneratorRewrite`, so an outer query cannot read `key` / `value` of a map explode or `pos` / `col` of a
  `posexplode` inside a subquery. Spark's `AS (k, v)` multi-alias refuses (FNP9-GENERATORS-1); one alias on a map
  or `posexplode` raises `COLUMN_ALIASES_MISMATCH` (Spark `UDTF_ALIAS_NUMBER_MISMATCH`).
- `LATERAL VIEW [OUTER] explode(...)` refuses (`LATERAL VIEWS` not implemented, BL-19-LATERAL-1).
- Two generators in one select list refuse; Spark 4.1.2 answers them.
- `explode(...) + 1` and `explode(explode(...))` refuse as `UNSUPPORTED_GENERATOR`, Spark's class with its
  `NESTED_IN_EXPRESSIONS` subclass. `posexplode(array(10, 20))` types `col` nullable (the literal above).
- Typed `F.explode` of a `MAP` refuses (`unnest() can only be applied to array, struct and null`).
- A `TIMESTAMP` `sequence` with `INTERVAL 1 DAY` across the New York spring change answers other rows than Spark
  (the sequence, not the generator).
- The local Hadoop catalog of a fresh session does not list the namespace or tables an earlier session wrote, so
  the cold read uses the table path.
- **Design question (not decided here):** the native `repark.sql` door has no Spark function set at all
  (`array`, `sequence`, `year`, `typeof`, `posexplode` are unresolved), so `explode` cannot answer there without
  a routing decision for that door. The door is unchanged.

Up: [map.md](map.md).
