# STRING-HOUR-MINUTE-SECOND-1 — step 0 sketch (measured, no product code)

**Unit.** On main, `hour(c)`, `minute(c)` and `second(c)` over a `STRING` answer `0`, silently, where Spark 4.1.2
parses the text as a timestamp and answers the field
([card](../../roadmap/mid-term/cards-2026-11-19/string-hour-minute-second-1-card-2026-11-19.md): S1 by class,
label-blocking, Fable's guidance under the owner's delegation of 2026-11-19). Measured on main `2fdab5c7` with the
package `pkg-main-4c365814` (`git diff --name-only 4c365814 2fdab5c7 -- crates` lists only the `sum` files) against
Spark 4.1.2 and Iceberg 1.11.0 at `local[1]`. `N` and `S` are the brief's scratch roots; raw runs are under
`N/string-hms-out/`. Documents and probes only.

## 1. Spark's rule, from the jar

Read with `javap -c -p` from `spark-catalyst_2.13-4.1.2.jar` ([evidence/javap.md](evidence/javap.md)):

- **Which expression.** `hour(x)` builds `Hour(x)` unless `x` is `TIME` (`HourExpressionBuilder`); `minute`,
  `second` likewise. `extract(HOUR FROM x)` and `date_part('HOUR', x)` build the same `Hour(x)`
  (`DatePart.parseExtractField`); `extract(SECOND …)` builds `SecondWithFraction`, a `decimal(8,6)`.
- **Which type a `STRING` becomes.** `Hour`, `Minute`, `Second` are `GetTimeField`: `ImplicitCastInputTypes` with
  `inputTypes = [AnyTimestampType]`. In both `TypeCoercion` (ANSI off) and `AnsiTypeCoercion` (ANSI on),
  `STRING → AnyTimestampType` is `AnyTimestampType.defaultConcreteType`, which is `TimestampType`: **the instant
  (`TIMESTAMP_LTZ`), never `spark.sql.timestampType`.** `year`, `quarter`, `month`, `dayofmonth`, `dayofweek`,
  `dayofyear`, `weekofyear`, `weekday` are `GetDateField` with `inputTypes = [DateType]`: a `STRING` becomes a `DATE`.
- **Which cast.** The implicit cast is a plain `Cast(c, TimestampType, None, EvalMode.fromSQLConf)`: the zone is
  filled with the session zone, the mode is ANSI when `spark.sql.ansi.enabled` is on and LEGACY otherwise. So
  `hour(c)` is exactly `hour(CAST(c AS TIMESTAMP_LTZ))`.
- **Which zone.** The text is read in the session zone (a text with an offset or `Z` keeps its own instant), and the
  field is read in the session zone (`zoneIdInEval`; UTC only for a `TIMESTAMP_NTZ` input).
- **Text it cannot parse.** ANSI on: `CAST_INVALID_INPUT`; ANSI off: NULL. A `NULL` is NULL.
- **Measured** ([domain-table.md](domain-table.md) §3): Spark's `hour(c)`, `minute(c)`, `second(c)` equal its
  `<f>(CAST(c AS TIMESTAMP))` on all 3,072 `TIMESTAMP_LTZ` projection cells, and Spark's `EXPLAIN` prints
  `hour(cast(c as timestamp), Some(<zone>))` for both. Under `TIMESTAMP_NTZ` they differ on 96 of 768 cells
  (`off`, `zulu`, `gap`, `tonly`): the written cast follows `spark.sql.timestampType`, the implicit one does not.
- **The neighbours** take the same implicit cast (`date_format`, `date_trunc`: `TimestampType`), or parse with a
  format (`unix_timestamp`, `to_timestamp(c, fmt)`: `ToTimestamp` accepts a `STRING` as is).

## 2. What main does

[inventory.md](inventory.md) has every site. In short:

- `hour` / `minute` / `second` are `DatePartWithoutTime` around `DatePartUdf`, whose `coerce_types` sends every
  string to `Date32` (`coerce_date_arg`, `datetime.rs:176-178`). `TypeCoercion` writes `hour(CAST(c AS Date32))`
  (`EXPLAIN`, [evidence/raw/map.md](evidence/raw/)), and arrow's `date_part(Date32, Hour)` is `0` for every
  date. That is Spark's rule for `year`, not for `hour`.
- The written `hour(CAST(c AS TIMESTAMP))` plans as `hour(to_timestamp(c))`: on the Spark door the LTZ cast rule
  (`SparkLtzTimestampCast`) runs after DataFusion's own `type_coercion` (inventory C4) and turns any cast of a
  string to the instant into `to_timestamp`, Spark's string grammar in the session zone, ANSI-aware. It sees
  coercion-inserted casts too: `EXTRACT(HOUR FROM c)` plans as `date_part('HOUR', __repark_cast_timestamp_ns__(c))`.
- `EXTRACT` and `F.extract` / `F.date_part` reach DataFusion's `date_part` (a string is a naive nanosecond wall, no
  session zone, `SECOND` an integer); SQL `date_part('f', c)` reaches datafusion-spark's `SparkDatePart`, which does
  not accept a string and fails to plan.
- `year` … `weekday` and `CAST(c AS DATE)` use arrow's string-to-`Date32` parse, which neither trims nor ignores
  the time part the way Spark's `stringToDate` does; `date_trunc` and `date_format` parse the text with arrow at
  invoke, NULL on failure, and read an offset text as a UTC wall.

## 3. The domain table

[domain-table.md](domain-table.md): 75,032 cells per engine (60,184 in the four `TIMESTAMP_LTZ` configurations,
14,848 in two `TIMESTAMP_NTZ` ones): 16 texts, four inputs (literal, Iceberg v2 `STRING` column, Parquet column, temp
view), the four projection doors (SQL, `F.<f>(col)`, `selectExpr`, `F.expr`), six `WHERE` / `GROUP BY` / `ORDER BY`
doors, 58 functions, ANSI off and on, `UTC` and `America/Los_Angeles`.

- **The card's three functions** (1,024 projection cells each, LTZ): `hour` and `minute` 128 EQUAL, 384 WRONG,
  512 RAISE; `second` 256 / 256 / 512. WRONG is main's `0` on every text with a time of day (`full`, `tsep`,
  `frac`, `off`, `zulu`, `gap`), silent, on every input, door, mode and zone. RAISE is main's arrow `Date32` cast
  refusing texts Spark parses (`lead`, `trail`, `nosec`, `tonly`, `yonly`) or answers NULL for (`empty`, `bad`,
  `badtime` with ANSI off). The 72 multi-row cells per function all RAISE (the table holds those texts).
- **Seat forecast** (§4 there): main's own `<f>(CAST(c AS TIMESTAMP))` answers Spark's `<f>(c)` on 1,014, 1,024 and
  1,024 of 1,024 cells, and 251, 256, 256 of 256 NTZ cells. The 15 left are one family: `F.expr` over a string
  literal outside UTC reads the text as UTC (neighbour N8), present today on the written cast.
- **Neighbours** (§6 there, N1–N11): the date fields and `CAST(c AS DATE)` (arrow's date parse: the UTC date of an
  offset text, silent; refusals of texts Spark reads), `date_trunc` / `date_format` (NULL for texts Spark reads, an
  offset read as a UTC wall), `extract(SECOND …)` an integer for Spark's `decimal(8,6)`, SQL `date_part` over a
  string refused, three `extract` fields and two aliases refused, `extract(HOUR …)` over the DST gap, the `F.expr`
  literal cast, `unix_timestamp` over `lead` / `gap`, `date_format` with `SSSSSS`, and the written
  `CAST(… AS TIMESTAMP)` under `TIMESTAMP_NTZ` keeping an offset text's instant.

## 4. The seats

Two seats, both Rust, both in `crates/repark-functions/src/time_family.rs` (667 lines, default ceiling 1000).
`datetime.rs` is at its exact baseline (1,655, `scripts/check_rust_file_size.py`): neither seat adds a line there.

**Seat A (coercion; the written cast's plan).** `DatePartWithoutTime::coerce_types` (`time_family.rs:444-446`)
answers `instant_ts::ltz_timestamp_type()` (`Timestamp(µs, "UTC")`, `instant_ts.rs:33`) for a `Utf8`, `LargeUtf8`
or `Utf8View` argument and delegates every other type as today. DataFusion's `type_coercion` writes
`CAST(c AS Timestamp(µs, "UTC"))`; `SparkLtzTimestampCast`, later in the same analysis on the Spark door (inventory
C4), turns it into `to_timestamp(c)`; the inner `DatePartUdf` sees an instant and reads the field in the session
zone (`instant_part`). That is Spark's rule: the implicit cast is the written cast.

- Files: `time_family.rs` (667 → about 680, ceiling 1,000; the Rust tests in its `mod tests`) and one facade pin
  file. No other product file.
- Cells it moves: the forecast, measured on main's own path: `hour` 1,014, `minute` and `second` 1,024 of 1,024 LTZ
  cells, 251 / 256 / 256 of 256 NTZ; the 10 + 5 `F.expr` literal cells stay (N8). The 216 multi-row cells have no
  control column; the build measures them.
- `EXPLAIN` changes: `hour(CAST(c AS Date32))` becomes `hour(to_timestamp(c))`, today's plan of
  `hour(CAST(c AS TIMESTAMP))` (`main-explain.jsonl`).
- Risks: (1) it depends on the Spark door's rule order: a door that runs DataFusion's coercion without
  `SparkLtzTimestampCast` after it (the Rust `AnsiDialect` door and the native door, `install_planned_timestamp_cast`)
  keeps an arrow cast that reads a zoneless text as UTC. Not one of this card's four doors; the build records those
  doors' answers. (2) Under `TIMESTAMP_NTZ` the coercion's cast goes through `rewrite_cast_as_ntz`
  (`instant_ts.rs:691-737`), so seat A answers whatever main's written cast answers there: today the instant's answer
  (the forecast), but N11 is a gap of that cast, and a fix of N11 alone would make seat A wrong under NTZ (Q6).

**Seat B (kernel).** `DatePartWithoutTime::coerce_types` leaves a string argument as it is (no cast in the plan),
and `invoke_with_args` (`time_family.rs:448-459`) turns a string array into the instant with
`spark_string_timestamp::cast_columnar_strings_to_ltz(value, session zone, Raise if ANSI else Null)`
(`spark_string_timestamp.rs:93`), the kernel one-argument `to_timestamp` runs (`instant_ts.rs:247-257`), then hands
it to the inner `DatePartUdf`.

- Files: `time_family.rs` (about +30), one facade pin file.
- Cells it moves: the same cells as seat A on the four doors (the same parser and the same field read); it reads
  the instant whatever `spark.sql.timestampType` says and whatever door's rules ran, as Spark's rule does. The 15
  `F.expr` literal cells probably move too (the zone is read at invoke, not where the literal cast is folded); not
  measured, the build measures.
- `EXPLAIN`: `hour(c)`, no cast (Spark prints a cast). The ANSI condition is the same `CAST_INVALID_INPUT`.
- Risks: a second caller of the string kernel (the same function, so the two cannot drift); invoke-time parsing of
  a literal is folded by DataFusion's simplifier under the planning session's settings, as today's literal casts are.

**Recommendation: seat A** with Q6's NTZ pin. Its forecast is measured cell by cell on main's own path, it is
Spark's plan shape, and it is about ten lines; seat B if the owner prefers door- and `timestampType`-independence
over the plan shape.

## 5. The cost cases

Measured on main ([evidence/raw/main-cost.json](evidence/raw/main-cost.json), `probes/cost.py`, 2,000,000 rows of
one Iceberg v2 table, `sum(<field>(…))`, nine runs round robin, median, lane A cores, a reading under the shared
measure lock):

| row | today on main | seat A (and B) |
|---|---|---|
| K1 `hour(c)`, `c STRING` | 0.0433 s (answers `0`) | about 0.071 s, **1.64x of today**: the parse; main's `hour(CAST(c AS TIMESTAMP))` (K1x) is 0.0712 s and answers right (22,993,024 as `hour(ts)`) |
| K1x `hour(CAST(c AS TIMESTAMP))` | 0.0712 s | unchanged: seat A's plan **is** this plan |
| K2 `hour(ts)`, `ts TIMESTAMP` (held 1.05x) | 0.0224 s | about 1.00x: neither seat touches a timestamp argument |
| K3 `year(c)`, K5 `minute(c)`, K4 `extract(HOUR FROM c)` | 0.0441 s, 0.0384 s, 0.0471 s | `year`, `extract` unchanged; `minute` as K1 |
| C-012 | not measured here | about 1.00x unless C-012 applies `hour` / `minute` / `second` to a string (a reading; the build runs it) |

K1 cannot hold 1.05x of today: today's time is the time of reading a midnight. Q4.

## 6. The unit split

- **One unit, one build round, one verifier** (the one-round cap): `hour`, `minute`, `second` over a `STRING` on
  the four doors, by seat A (or B). One product file (`time_family.rs`); it does not touch `datetime.rs`,
  `instant_ts.rs` or the reader files, so it can go to lane A beside unit 0 (CSV) and unit 1 (reader parse).
- **Pins:** a facade file over the 16 texts × 4 inputs × 4 doors × 4 configurations for the three functions, plus
  the NTZ cells (Q6) and the multi-row doors, against a recorded Spark oracle file (this step's answers; `tonly`'s
  date part does not reach these fields), red on main for the 1,536 + 216 not-EQUAL cells; Rust tests in
  `time_family.rs` for the coercion (a string becomes the instant, `TIME` still refuses, `Date32` unchanged) and the
  `EXPLAIN` shape; K1, K1x, K2 and C-012.
- **The neighbours N1–N11** are not this unit: the records clerk files them as cards (Q2, Q3). N1 and N3 share this
  card's cause (a `STRING` argument coerced or parsed without Spark's cast) and are silent on label-scope columns.

## 7. Questions

Each can be ruled from the domain table.

1. **The seat.** (a) seat A, the coercion to the instant (default: forecast measured on 3,072 + 768 cells, 3,062 +
   763 EQUAL, the rest N8; Spark's plan shape; about ten lines); (b) seat B, the kernel (door- and
   `timestampType`-independent; `EXPLAIN` without the cast; forecast inferred). Cells: domain table §4, §2.
2. **The neighbours' scope.** (a) this unit stays `hour` / `minute` / `second`; N1–N11 become cards (default: the
   one-round cap; N3's functions sit in `datetime.rs`, at its exact baseline); (b) fold N3 (`date_trunc`,
   `date_format`: the same implicit cast in Spark) into this unit by the same coercion, an in-place arm in
   `coerce_to_timestamp_micros`. Cells: domain table §5 rows `date_trunc_*`, `date_format_*`; §6 N3.
3. **The neighbours' severity.** (a) the silent ones (N1, N3, N4, N8, N11) S1 by class and label-blocking, as this
   card; the loud ones (N2, N5, N6, N9, N10) S2 (default: the class rule Fable applied to D4); (b) all S2, one
   follow-up card. Cells: domain table §6.
4. **K1's bar.** (a) hold K1 at 1.05x of main's `hour(CAST(c AS TIMESTAMP))` (0.0712 s), and K2 and C-012 at
   1.05x of today (default: today's K1 is the cost of a wrong answer); (b) no bar on K1; (c) 1.05x of today's K1
   (0.0433 s), which no parse meets (K1x is 1.64x). Rows: §5.
5. **N8, the `F.expr` string-literal cast read as UTC.** (a) a card (default: present today on the written cast; 10
   LTZ + 5 NTZ cells; seat A neither causes nor fixes it); (b) in this unit (then seat B, or a second seat on the
   expression-string session's zone). Cells: domain table §4.
6. **Seat A under `TIMESTAMP_NTZ`.** (a) pin `hour` / `minute` / `second` over `off`, `zulu`, `gap`, `tonly`
   under `TIMESTAMP_NTZ` to Spark's instant answers in this unit, so a later N11 fix that moves them goes red
   (default); (b) take seat B, which does not read `spark.sql.timestampType`. Cells: domain table §3, §4 (NTZ).

Up: [map.md](map.md).
