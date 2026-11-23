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

See [the domain table](#domain-table) below and the oracle
[`sql_explode_1_spark_oracle.json`](../../../../python/repark/tests/functions/sql_explode_1_spark_oracle.json)
(PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13:1.11.0`, a local Hadoop catalog, session zones `UTC` and
`America/New_York`, ANSI on and off). Spark answers every cell the same in the four modes.

## The fix

`crates/repark-functions/src/generator.rs`: `explode` and `explode_outer` join the placeholder UDFs, and
`GeneratorKind::Explode` expands them as `posexplode` does without the position column: an `ARRAY` projects
`col` (a single `AS` alias renames it), a `MAP` projects `key` and `value`, and the outer spelling turns a NULL or
empty input into one NULL row. Over an `ARRAY` the placeholder types as the element (Spark's nullability: the
element's, or nullable for the outer spelling), so the SQL planner types an outer query over a generator subquery,
and the write statements' target checks, against the element before `GeneratorRewrite` runs. The Spark door,
`selectExpr` and the write statements (`CREATE TABLE … AS SELECT`, `INSERT … SELECT`) get them; `F.expr` gets
them over a table or view with `.alias(...)`. The typed path is unchanged.

On the head every scoped cell on `spark.sql` and `selectExpr` answers as Spark in the four modes (rows, names,
types), the calendar-dimension shape included: a projection of `year`, `extract(YEAROFWEEK …)` and
`extract(DAYOFWEEK …)` cast to `SMALLINT` and `weekofyear` over `explode(sequence(DATE, DATE, INTERVAL 1 DAY))`,
and its CTAS and `INSERT … SELECT` into a local Iceberg table read back cold (a fresh session reading the table
path; the stored `SMALLINT` is an Iceberg `int`, as in Spark).

## Pins

`python/repark/tests/functions/test_sql_explode_1.py` over the oracle, driven by the recorder
`_record_sql_explode_1.py` (58 items with the extract file: green on the head, 44 red on main; reverting
`generator.rs` reds 24, all in this file, with `UNRESOLVED_ROUTINE`); Rust `repark-functions`
`generator::tests` (`explode_*` and `a_projection_over_an_explode_subquery_reads_the_element`).

## Still unlike Spark (out of this unit's seat)

- An `array(...)` or `map(...)` literal's element is nullable, so `explode(array(1, 2))` types `col` nullable
  where Spark says not (typed `F.explode` the same on main); `sequence` keeps its element non-null. `dayofmonth`
  over an exploded date is nullable where Spark says not.
- `F.expr("explode(...)")` over a `spark.range` frame raises `No field named __repark_gen_col`; unaliased over a
  view it answers Spark's rows but the facade names the column `explode(arr)` (engine name `col`). Over a view
  with `.alias(...)` it answers as Spark. `F.expr("<any expression> AS x")` refuses for every expression.
- A `MAP` placeholder still types as the map, so an outer query cannot read `key` / `value` of a map explode
  inside a subquery. Spark's `AS (k, v)` multi-alias refuses (FNP9-GENERATORS-1); one alias on a map raises
  `COLUMN_ALIASES_MISMATCH` (Spark `UDTF_ALIAS_NUMBER_MISMATCH`).
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
