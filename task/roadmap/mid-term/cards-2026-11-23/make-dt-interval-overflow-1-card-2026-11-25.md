# Card MAKE-DT-INTERVAL-OVERFLOW-1: `make_dt_interval` answers NULL on overflow and types its result as a string

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from the separate shapes on the
TRY-CAST-NONNULL-FIELD-1 card, re-measured here. **Severity:** S1 (owner, 2026-11-25): a silent wrong read. Where
Spark raises, RePark answers NULL, and where Spark answers a large interval, RePark answers NULL too.
**Status:** fixed on `fix/make-dt-interval-overflow-1` (PR #1435; delta-2 product head `f84548819`), not merged.

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head), in the UTC session zone. The
same answers hold in both ANSI modes. `i` is an `int` column holding `1` and `2147483647`; the sources are an
in-memory frame and a Parquet file.

| Statement | Spark 4.1.2 | Main |
|---|---|---|
| `SELECT id, make_dt_interval(i, 0, 0, 0.0) AS r FROM t` | `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` | **`1 day`, then `NULL`**; `r` is `string` |
| `SELECT id, make_dt_interval(0, i, 0, 0.0) AS r FROM t` | `1:00:00`, then `89478485 days, 7:00:00`; `r` is `interval day to second` | **`1:00:00`, then `NULL`**; `r` is `string` |
| `SELECT make_dt_interval(2147483647, 0, 0, 0.0) AS r` | `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` | **`NULL`**; `r` is `string` |

The TRY-CAST-NONNULL-FIELD-1 card records two more cells of the same function: over an Iceberg NOT NULL column the
overflow crashes (`Column 'r' is declared as non-nullable`), and a NaN seconds argument does the same.

## Seat

`crates/repark-functions/src/expr_fn.rs:677` builds the call from `datafusion_spark`'s `make_dt_interval`, which
returns NULL when the day-time interval overflows and whose result the facade types as a string. Spark's
`IntervalUtils.makeDayTimeInterval` raises the overflow and returns `DayTimeIntervalType(DAY, SECOND)`.

## First step

Measure the domain first: each argument at its overflow edge, negative values, fractional seconds, NULL and NaN
arguments, every door, Parquet, Iceberg and in-memory sources, `dtypes` and the stored type of a CTAS. Then decide
whether a RePark UDF replaces the call. Patching the `datafusion-spark` crate is out of scope.

## Fix (2026-11-25)

RePark's own `make_dt_interval` (`crates/repark-functions/src/temporal_ctor/day_time.rs`) replaces the upstream kernel
by name: Spark's exact adds raise `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` in both modes, every in-range value
answers, seconds follow Spark's implicit `decimal(18, 6)` cast, and the field is nullable as Spark declares it.
`Duration(µs)` is named and typed `interval day to second` (`crates/repark-spark/src/type_table.rs`,
`python/repark/src/repark/spark/column_fields.py`). Measured over 1434 cells in both ANSI modes: 1010 fixed, 70 equal
on both, 0 moved off Spark. Pins: `python/repark/tests/sql/test_make_dt_interval_overflow_1.py` (1064 tests: 1004
red on main, 60 controls), 13 Rust tests in `day_time_tests.rs` (11 before delta 1) and one in
`crates/repark-spark/src/type_table/tests.rs`. Evidence:
[task/wo/make-dt-interval-overflow-1/evidence/](../../../wo/make-dt-interval-overflow-1/evidence/summary.md).

## Still unlike Spark (measured on head)

The full list with cell counts is in the evidence summary. The shapes that touch this function:

- **Parquet, a narrowing projection under a filter.** `SELECT id, make_dt_interval(d, h, m, s) FROM pq WHERE id = 1`
  raises the overflow of a filtered-out row (20 cells), where Spark answers the kept row. DataFusion's
  `FilterExec::try_swapping_with_projection` moves the projection below the filter. Main does the same for every
  raising expression (`CAST(d + h + m AS SMALLINT)` raises `CAST_OVERFLOW`), so this needs its own card.
  In 14 of the 20 cells (7 per ANSI mode, `pq#1`, `#4`, `#7` to `#10` and `#13`) main answered Spark's rows under the
  `string` type, so these cells keep their values on main and raise on head. No cell equal to Spark on main moves.
- **`F.expr` decimal literals.** `F.expr("make_dt_interval(0, 0, 0, 0.1)")` types `0.1` as `double`, so `r` is nullable
  (Spark: not nullable), and `-999999999999.999999` rounds to `-1E12` and answers NULL or raises (44 cells).
- **SQL `999999999999.9999D`** arrives as `1E12` and answers NULL or raises (2 cells).
- **`CAST(x AS STRING)`** renders `1 days 2 hours 3 mins 4.500000 secs`, not `INTERVAL '1 02:03:04.5' DAY TO SECOND`.
- **`extract`** answers totals (`26` hours) rather than fields (`2`).
- **Refused consumers:** `-x`, `x * 2`, `x / 2`, `DATE + x`, `typeof(x)`, `avg(x)`, `try_add(x, y)`, and strings or
  decimals as days, hours or minutes.
- **Writes:** CTAS into Iceberg refuses on both engines with different text; `write.parquet` stores `duration[us]`
  where Spark stores `int64`.
- **Doubles as days or hours.** `make_dt_interval(1.5D)` and `make_dt_interval(1, 2.7D)` refuse at planning, where
  Spark answers `1 day` and `1 day, 2:00:00` in both modes; main refuses identically.
- **Planning cost of long sums, present on main.** A SQL sum of `n` `make_dt_interval` calls plans in about `n²` time on
  main and head alike (`n = 1000`: main 120 s, head 101 s).

## Delta 1 (2026-11-25): reading the type back

The widening made `DataFrame.schema` hand out `DayTimeIntervalType`, which RePark's own type parser refused, so
`StructType.fromJson(json.loads(df.schema.json())) == df.schema` raised where main and Spark answer `True`. Delta 1:
- the native DDL parser and Python's atomic token parser read Spark's ANSI interval spellings (`interval day to second`,
  `interval hour to minute`, `interval second`, `interval year to month`), so `fromJson`, `fromDDL` and `simpleString`
  round trips parse;
- `Column.cast` and `try_cast` to `DayTimeIntervalType` or its spellings cast a duration or NULL and raise
  `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` for any other type (Spark refuses `timestamp` and `double`);
- `repark_type_to_arrow(DayTimeIntervalType())` is `duration[us]`, as PySpark's `to_arrow_type`;
- a double seconds argument converts through a numeric fast path, and the overflow text is built only on the error
  path. Over one million rows (median of 5, two rounds): double 0.032–0.037 s against main 0.028–0.031 s (was 0.32 s),
  decimal 0.047 s against 0.052–0.061 s (was 0.19 s), int 0.020 s against 0.035 s (was 0.043 s). The verifier
  measured doubles at 1.29–1.31× main, and these runs reach 1.33× (0.0371 s against 0.0279 s). Doubles that take the
  exact path (about 2.8e8 s or more, or on a `.5` boundary at 6 places) cost about 8× main: 0.33 s against 0.039 s.

Consumers measured in [consumers.md](../../../wo/make-dt-interval-overflow-1/evidence/consumers.md): 116 cells, 34
fixed, 16 equal on both, 0 moved off Spark. The verifier's 191 widening cells and 211 edge cells move off Spark nowhere.
Still unlike Spark there:
- **`createDataFrame(rows, schema)`** with a `DayTimeIntervalType` field given as a `StructType` (or `fromJson`)
  stores the `timedelta` as text typed `string`, as on main. Given as a DDL string, main refuses the schema
  (`PySparkTypeError: createDataFrame schema string must be a DDL field list`); delta 1 made that answer a silent
  `string`, and delta 2 restores main's refusal.
- **`describe("t")` / `describe("id", "t")`** refuse with `describe/summary columns must be numeric or string`, where
  Spark drops the interval column (main answered statistics for it); `describe()` with no arguments equals Spark.
- **Python UDFs** declaring `DayTimeIntervalType` refuse (`does not map to a concrete Arrow scalar type`), as on main.
- **Casts from other types:** `CAST(int AS INTERVAL DAY TO SECOND)` and a string cast refuse, where Spark answers
  seconds or parses the text; `selectExpr("CAST(t AS INTERVAL DAY TO SECOND)")` and the SQL spelling are parser errors,
  as on main.
- **`spark.read.schema("t interval day to second").json(…)`** refuses, where Spark reads the column.
- **`from_json(j, "t interval day to second")`** raises `PARSE_SYNTAX_ERROR` on main and head, where Spark answers.
- **A Python UDF's input** arrives as the text `"1 day, 2:03:04.500000"`, not a `timedelta`, on main and head.
- **`toPandas()` dtypes** are `timedelta64[us]`, where Spark gives `timedelta64[ns]`.
- **`write.parquet` then `read.schema(…).parquet`** refuses on main and head (`schema(...) is not applied on parquet
  reads yet`), where Spark reads the interval back.
- **`summary()`** refuses on main and head, where Spark answers.

## Delta 2 (2026-11-25): the DDL-string schema and `interval X to X`

- `createDataFrame(rows, "id int, x interval day to second")` (any ANSI interval field, nested too) refuses again with
  main's `PySparkTypeError` (`python/repark/src/repark/spark/session/create_dataframe_schema.py::_parse_schema_ddl`);
  `applyInPandas` and `mapInPandas` DDL return schemas take the same path. A `StructType` schema, including one built
  by `fromDDL` or `fromJson`, still stores the text as `string`, as on main.
- `interval X to X` and reversed ranges raise as Spark's `PARSE_SYNTAX_ERROR` does
  (`crates/repark-spark/src/type_table/parse.rs::ansi_interval_type`), on `fromDDL`, `_parse_datatype_string`,
  `read.schema` and `createDataFrame`.
- A JSON schema's interval type name that is not in Spark's exact lower-case, single-spaced form
  (`INTERVAL HOUR TO SECOND`, `interval  minute  to  second`, ` interval day`) refuses again with main's `ValueError`,
  as Spark refuses it (`python/repark/src/repark/spark/types.py::_parse_datatype_json_value`). Delta 1 had lower-cased
  the name before parsing, which moved four cells off Spark.
- Still unlike Spark, refused on main: PySpark's `fromJson` reads interval type names by prefix, so it takes
  `interval day to day` (as `interval day`) and `interval year to day`; RePark refuses those 35 spellings.
  `Column.cast` and SQL `CAST` to any interval other than `interval day to second` refuse; `read.schema` with an
  interval field refuses on CSV, JSON and Parquet.
- Pins: `python/repark/tests/types/test_make_dt_interval_ddl_schema.py` (124 tests; 19 red on main; the 12 JSON-door
  tests, 6 red before the guard) and five more refusals in `crates/repark-spark/src/type_table/tests.rs`.

Up: [map.md](map.md).
