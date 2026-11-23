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
Delta 2: `GeneratorAliasMark`, the first analyzer rule on the Spark door (before `type_coercion`), turns every
explicit alias on a generator call into the `__repark_gen_alias` marker, so `GeneratorRewrite` takes user names only
from the marker and names bare outputs from the argument type; coercion's restored names (a `CASE`, `filter`,
`transform`, `coalesce` argument) no longer read as aliases, and an alias spelled as the call
(``AS `posexplode(arr)` ``) refuses as Spark does. `stamp_attribute_ids` marks before it stamps: a frame alias
becomes the marker and a bare call is held in `__repark_gen_bare` (its own plan name), so a stamp, stripped or not,
never reads as a user alias and moves onto a one-output generator's output; `.select("col")`,
`.withColumn`, `groupBy("col")` and a self-join after `F.expr("explode(arr)")` answer as Spark. A bare `explode`
beside a select item named `col` gets a plan name that cannot collide (`spark_rewrites/bare_explode.rs`), so a map
there answers `key`, `value`. `PyColumn::is_bare_generator` and `_rewrap_rebound_column` keep `_GeneratorColumn` and unaliased
bare generator `F.expr` calls unaliased (`_projection_name=None`), so the Spark door, `selectExpr`, the write
statements, and `F.expr` over a table or view (both aliased and unaliased) all answer as Spark. The typed path is
unchanged.

On the head every scoped cell on `spark.sql`, `selectExpr`, `F.expr` over a table or view, and the write statements
answers as Spark in the four modes (rows, names, types), including `explode_outer` / `posexplode_outer` over
`sequence` and `split`, unaliased subqueries / `ORDER BY col` / `INSERT INTO` / `INSERT OVERWRITE` / `CTAS`,
explicit `explode(…)` aliases, and the calendar-dimension shape: a projection of `year`, `extract(YEAROFWEEK …)`
and `extract(DAYOFWEEK …)` cast to `SMALLINT` and `weekofyear` over `explode(sequence(DATE, DATE, INTERVAL 1 DAY))`,
and its CTAS and `INSERT … SELECT` into a local Iceberg table read back cold (a fresh session reading the table
path; the stored `SMALLINT` is an Iceberg `int`, as in Spark).

## Pins

`python/repark/tests/functions/test_sql_explode_1.py` over the oracle, driven by the recorder
`_record_sql_explode_1.py` (with `test_extract_field_aliases_1.py`: 94 items total, all green on the head and red
on main, on the pre-delta PR head, and on both mutants; delta 2 adds 16: coerced arguments, `col` siblings, frame
operations, and the `array_repeat` record, the first 12 red on the delta-1 head); Rust `repark-functions`
`generator::tests` (35 tests), `repark-spark` `spark_rewrites::bare_explode::tests` and the extension seat pin.

## Still unlike Spark (out of this unit's seat)

- An `array(...)` or `map(...)` literal's element is nullable, so `explode(array(1, 2))` types `col` nullable
  where Spark says not (typed `F.explode` the same on main); `sequence` keeps its element non-null. `dayofmonth`
  over an exploded date is nullable where Spark says not.
- `F.expr("explode(...)")` over a `spark.range` frame raises `No field named __repark_gen_col`. `F.expr("<any
  expression> AS x")` refuses for every expression. Over a table or view, aliased and unaliased array `F.expr`
  answer as Spark (`columns`, `schema`, rows); unaliased map `F.expr("explode(m)")` / `F.expr("explode_outer(m)")`
  answer Spark's exact `schema`, `to_arrow()`, and rows while `DataFrame.columns` reports `['id', 'col']`
  (unanalyzed placeholder name, same as `F.posexplode` on main).
- Multi-column generators (`explode` on a `MAP`, `posexplode`, `inline`) still emit a single placeholder column
  before `GeneratorRewrite`, so an outer query cannot read `key` / `value` of a map explode or `pos` / `col` of a
  `posexplode` inside a subquery. Spark's `AS (k, v)` multi-alias refuses (FNP9-GENERATORS-1); one alias on a map
  or `posexplode` raises `COLUMN_ALIASES_MISMATCH` (Spark `UDTF_ALIAS_NUMBER_MISMATCH`).
- `F.expr("posexplode(arr)")`, `F.expr("inline(a_struct)")` and `F.expr("explode(m)")` selected bare in a DataFrame
  answer Spark's rows under the placeholder column name (`DataFrame.columns`), as on `442887339`.
- `array_repeat` over an array argument fails in its own kernel (`ListArray expected data type … 'element'`) on
  main and on the head, with or without a generator, so `explode_outer(array_repeat(sequence(1, 2), 0))` fails
  (the seat is `array_repeat`, not the generator).
- `SELECT col, explode(arr)` beside a `col` column refuses (two `col` fields); Spark answers both. A `*` that holds
  a `col` column beside a bare map `explode` refuses too (the rewrite sees only named siblings).
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
