# task/wo/make-dt-interval-overflow-1/evidence/summary.md — MAKE-DT-INTERVAL-OVERFLOW-1 measurement evidence

## Provenance

- **RePark:** main `8c07d6197` (package built from #1429's tested head `57c92b41`; main adds only Markdown) and the
  branch `fix/make-dt-interval-overflow-1` at `c17c744af` (release wheels).
- **Spark:** live PySpark 4.1.2 (`local[2]`, Java 17, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, local Hadoop
  catalog), both ANSI modes, session time zone `UTC`, probes run with `TZ=UTC`.
- **Scripts:** [probe_make_dt_interval.py](probe_make_dt_interval.py) (one run per engine and ANSI mode: 720 Spark
  cells, 726 RePark cells with 6 native-door controls), [compare_make_dt_interval.py](compare_make_dt_interval.py)
  (the table), [record_pin_cells.py](record_pin_cells.py) (writes the facade pin's Spark oracle).
- **Sources:** a literal row, an in-memory frame (`createDataFrame`, NOT NULL schema; RePark refuses an infinite float
  there, so row 8 is absent), a Parquet file, and Iceberg tables with NOT NULL (`nn`) and nullable (`nl`) columns.
- **Full table:** [domain-table.md](domain-table.md). Each cell compares the result column `r` only (type,
  nullability, rows), because RePark's `createDataFrame` reports `id` nullable.

## Verdicts (both ANSI modes, 1434 cells)

| Verdict | Cells | Meaning |
|---|---:|---|
| fixed | 1010 | unlike Spark on main, equal on head |
| = / = | 70 | equal on main and on head (neighbours and controls) |
| ≠ / ≠ (moved) | 294 | unlike Spark on both; most moved only in the type label (`string` → `interval day to second`) |
| ≠ / ≠ (unmoved) | 60 | unlike Spark on both, identical answers |
| **moved off Spark** | **0** | equal on main, unlike on head |

## Root cause

`datafusion-spark` 54.1.0's `make_dt_interval` totals hours and minutes in `i32` and appends NULL on every checked
overflow; its result field is nullable only from the inputs and a non-finite literal, so an overflow or a NaN over a
NOT NULL column crashes (`Column 'r' is declared as non-nullable`). The facade typed `Duration(µs)` as `string` on
three surfaces: the logical key behind `DataFrame.schema` (`arrow_name_at_depth`), the descriptor table
(`spark_type_from_arrow`) and the `DESCRIBE` name.

## Fix

`crates/repark-functions/src/temporal_ctor/day_time.rs` registers RePark's `make_dt_interval` over the upstream one
(Spark's `IntervalUtils.makeDayTimeInterval`: exact `i64` adds in Spark's order, seconds through the implicit
`decimal(18, 6)` cast, Spark's nullability). `Duration(µs)` is named and typed `interval day to second`, as PySpark's
own Arrow mapping types `duration[us]` (`DayTimeIntervalType`).

## Seat inventory (`path:line` on main `8c07d6197`)

| # | Seat | `path:line` | Door(s) | Why it exists / what it does on main | Head |
|---|---|---|---|---|---|
| 1 | `datafusion-spark` registration | `crates/repark-functions/src/lib.rs:107` | Spark SQL, `selectExpr`, `F.expr`, temp-view bodies (all through `register_all`) | `register_all` installs every `datafusion_spark` scalar UDF, `make_dt_interval` among them, so the SQL name resolves to the upstream kernel | unchanged; overridden by seat 2 |
| 2 | RePark family registration | `crates/repark-functions/src/registration.rs:14` (`temporal_ctor::register`) | same as seat 1 | registers the `temporal_ctor` family after seat 1; a later registration of the same name wins | `temporal_ctor::functions()` now chains `day_time::functions()` (`crates/repark-functions/src/temporal_ctor.rs`), so RePark's `make_dt_interval` replaces the upstream one by name |
| 3 | Upstream kernel (root cause) | `datafusion-spark-54.1.0/src/function/datetime/make_dt_interval.rs:102` (`return_field_from_args`), `:139` (kernel; NULL at `:180` and `:193`), `:201` (`make_interval_dt_nano`) | every door that resolves seat 1 | `i32` `day*24+hour` and `total_hours*60+min` overflow at `|days| > 1491308`/`|hours| > 35791394`, and every `checked_*` miss appends NULL; nullability comes from the inputs plus a non-finite *literal* only, so a NaN column or an overflow over a NOT NULL column crashes (`Column 'r' is declared as non-nullable`); seconds are coerced to `Float64` (decimal precision lost) | no longer reached; not patched (out of scope) |
| 4 | Column builder | `crates/repark-functions/src/expr_fn.rs:677` | `F.make_dt_interval` | built `datafusion_spark::...::make_dt_interval().call(args)` | builds `temporal_ctor::day_time::make_dt_interval_udf()` |
| 5 | Column dispatch | `crates/repark-python/src/column/function_dispatch.rs:722` | `F.make_dt_interval` | arity check (> 4 refuses with `call_scalar(make_dt_interval) expects at most 4 args`), then seat 4 | unchanged |
| 6 | Python wrapper | `python/repark/src/repark/spark/functions_datetime.py:181` (`:209` nullary, `:213` call) | `F.make_dt_interval` | PySpark-shaped signature; literal indices and the foldable nullary call | unchanged |
| 7 | Facade typing (native table) | `crates/repark-spark/src/type_table.rs:211` (`spark_type_from_arrow`) | `dtypes`, `df.schema`, `printSchema`, `schema.json()`, every door | `Duration(µs)` fell through to `_ => string`, so `make_dt_interval`, `ts - ts` and `sum` over durations all typed `string` | `Duration(µs)` → `DayTimeInterval { day, second }` (`interval day to second`) |
| 8 | Facade typing (Python fallback) | `python/repark/src/repark/spark/types.py:1259` (`_arrow_type_to_repark_python`) | used when the FFI descriptor call raises | `duration[us]` fell through to `StringType()` | unchanged; not reached for `duration[us]`, because seat 7 now types it through the descriptor |
| 9 | Rendering of `CAST(... AS STRING)` | `crates/repark-functions/src/temporal_ctor/intervals.rs:657` (`rewrite_interval_cast`) and `:514` (`IntervalToString`) | Spark SQL, `F.col(...).cast("string")` | rewrites a cast of an interval or duration to RePark's calendar text (`1 days 2 hours 3 mins 4.500000 secs`); Spark prints `INTERVAL '1 02:03:04.5' DAY TO SECOND` | unchanged (still unlike Spark) |
| 10 | DESCRIBE type name | `crates/repark-spark/src/type_table.rs:123` (`arrow_name_at_depth`) | `DESCRIBE` of a view | renders `Duration(Microsecond)` from the Arrow debug name | `interval day to second`, as Spark (the `describe · view` cells) |
| 11 | `extract` / `date_part` over a duration | DataFusion `date_part` over `Duration(µs)` (RePark's `spark_date_part.rs:156` handles `Interval(DayTime)` only) | Spark SQL | `extract(HOUR …)` answers total hours (`26`), not the hour field (`2`) | unchanged (still unlike Spark) |
| 12 | Aggregates over a duration | `crates/repark-functions/src/aggregate.rs:125` (`sum`), `crates/repark-functions/src/interval_avg.rs` (`avg`) | Spark SQL | accept `Duration(µs)` | unchanged; now typed `interval day to second` through seat 7 |
| 13 | `collect()` rendering | pyarrow `duration[us]` → `datetime.timedelta` | every door | already renders `timedelta`, as PySpark does | unchanged |
| 14 | Iceberg write | iceberg-rust `crates/iceberg/src/arrow/schema.rs` (`Unsupported Arrow data type`) | CTAS / INSERT into Iceberg | refuses `Unsupported Arrow data type: Duration(µs)`; Spark refuses `Not a supported type: interval day to second` | unchanged (both refuse) |
| 15 | Parquet write | arrow-rs Parquet writer | `write.parquet` | stores `duration[us]` with the Arrow schema; Spark stores `int64` with its own row metadata | unchanged |
| 16 | Native door | `crates/repark-python/src/session.rs:165` (`register_native_door_functions`) | `PyReparkSession.native().sql` | registers no `make_dt_interval`: `UNRESOLVED_ROUTINE` | unchanged (control) |

## Still unlike Spark on head (measured)

The 354 unlike cells of the table, plus the 6 `write.parquet` cells the table leaves out.

| Shape | Cells | Spark 4.1.2 | Head | Cause (not this unit's seat) |
|---|---:|---|---|---|
| Parquet source, `make_dt_interval(d, h, m, s)` with `WHERE id = k`, file holding an overflow row | 20 | answers the kept row | raises `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` (main answered without raising, typed `string`; in 14 cells, 7 per mode, those were Spark's rows) | DataFusion 54.1.0 `FilterExec::try_swapping_with_projection` (`filter.rs:590`) moves a narrowing projection below the filter, so the projection runs on filtered-out rows; the same on main for every raising expression (`CAST(d + h + m AS SMALLINT)` raises `CAST_OVERFLOW`, `make_ym_interval(d, h)` raises) |
| `F.expr` door, decimal literal seconds (`F.expr("make_dt_interval(0, 0, 0, 0.1)")`) | 40 | nullable `false` | nullable `true` (main `false`) | the `F.expr` door types `0.1` as `double`, where Spark types `decimal(1,1)`; a double seconds argument is nullable in Spark too |
| `F.expr` door, `-999999999999.999999` seconds | 4 | `-11574075 days, 22:13:20.000001` | NULL (ANSI off), `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION` (ANSI on); main 1 µs off | same `F.expr` literal typing: the double rounds to `-1E12` |
| SQL door `999999999999.9999D` | 2 | `11574074 days, 1:46:39.9999` | NULL / `NUMERIC_VALUE_OUT_OF_RANGE` (main: `1E12` seconds) | the SQL door's `D` literal arrives as `1E12` |
| in-memory source nullability (`make_dt_interval` and `ts - TIMESTAMP …`) | 122 | `r` nullable `false` over NOT NULL `d`, `h`, `m` | `true` | RePark's `createDataFrame` drops NOT NULL from the schema |
| temp-view door over the in-memory source | 64 | answers | `TABLE_OR_VIEW_NOT_FOUND` (`__repark_cdf_…`) | identical on main |
| `CAST(… AS STRING)` (SQL, `selectExpr`, `F.expr`, `Column.cast`) | 20 | `INTERVAL '1 02:03:04.5' DAY TO SECOND` | `1 days 2 hours 3 mins 4.500000 secs` | `intervals.rs` `IntervalToString` renders calendar text |
| `extract(HOUR|MINUTE|SECOND …)` | 6 | the field (`2`, `3`, `4.500000` as `tinyint`/`decimal(8,6)`) | totals (`26`, `1563`, `93784`) as `int` | DataFusion `date_part` over `Duration` |
| `-x`, `x * 2`, `x / 2`, `DATE + x`, `typeof(x)`, `avg(x)`, `try_add` | 18 | answer | refuse (`type_coercion`, `Cannot coerce`, `NOT_IMPLEMENTED`) | duration arithmetic and `typeof` in DataFusion; identical on main |
| `make_dt_interval(106751991) + make_dt_interval(1)` | 2 | `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` | `Arithmetic overflow` (main: NULL operand) | DataFusion duration addition |
| strings or decimals as days, hours or minutes; string seconds | 18 | answer (implicit cast) | refuse at planning, as on main | coercion kept from upstream |
| `CAST(4294967297 AS BIGINT)` days | 6 | wraps to 1 (ANSI off), `CAST_OVERFLOW` (ANSI on) | `simplify_expressions` cast error, as on main | DataFusion `Int64 → Int32` cast |
| `INTERVAL '…'` literals, `make_ym_interval` collect | 12 | answer / `NOT_IMPLEMENTED` | refuse, as on main | neighbours, unchanged |
| `ts + x` over the row whose result leaves the timestamp range | 6 | `year 247007 is out of range` | `date value out of range` | both raise; different text |
| CTAS into Iceberg | 6 | `Not a supported type: interval day to second` | `Unsupported Arrow data type: Duration(µs)` | both refuse |
| `write.parquet` stored type | 6 | `int64` | `duration[us]` | arrow-rs writer, unchanged |
| `createDataFrame([timedelta])`, `createDataFrame(rows, schema)` round trip, `CAST(x AS INTERVAL DAY TO SECOND)` | 6 | `interval day to second` | `string` / parse error, as on main | `createDataFrame` keeps a `timedelta` as text; the parser has no ANSI interval type |
| `CASE WHEN true THEN x END` | 2 | nullable `false` | nullable `true`, as on main | DataFusion types a `CASE` with no `ELSE` nullable |

Up: [../../map.md](../../map.md).
