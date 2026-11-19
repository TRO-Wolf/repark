# STRING-HOUR-MINUTE-SECOND-1 — the domain table (main `2fdab5c7` against Spark 4.1.2)

Every cell is one expression over one text, on one input, door and configuration, answered by main
(`pkg-main-4c365814`) and by Spark 4.1.2 with Iceberg 1.11.0 (`local[1]`). The probes are in
[evidence/probes/](evidence/probes/map.md); the counts below are `cmp.py`'s
([evidence/verdicts/summary.md](evidence/verdicts/summary.md)); every `hour` / `minute` / `second`, `extract` /
`date_part` time-field and `CAST` control cell is in [evidence/verdicts/hms-cells.tsv](evidence/verdicts/hms-cells.tsv);
the full 75,032-row verdict file stays in `N/string-hms-out/verdicts.jsonl`.

## 1. The cells

- **Configurations:** ANSI off (`a0`) and on (`a1`) × session zone `UTC` and `America/Los_Angeles` (`la`), with
  `spark.sql.timestampType=TIMESTAMP_LTZ`; and two `TIMESTAMP_NTZ` configurations (ANSI on, both zones, inputs `L`
  and `I`).
- **Inputs:** `L` a string literal (`F.lit` on the frame doors), `I` a `STRING` column of an Iceberg v2 table (one
  table per text, written by the engine itself), `P` a Parquet column (one file per text, written once by Spark and
  read by both engines), `V` a temp view over `createDataFrame`.
- **Texts (16):** `date` `2024-01-01`; `full` `2024-01-01 03:04:05`; `tsep` `2024-06-01T23:30:00`; `frac`
  `…03:04:05.123456`; `off` `2024-01-01T03:04:05+05:30`; `zulu` `…T03:04:05Z`; `lead` / `trail` two spaces before /
  after `full`; `empty` `''`; `null`; `bad` `not a time`; and five more: `nosec` `2024-01-01 03:04`, `tonly`
  `03:04:05`, `badtime` `2024-01-01 25:00:00`, `gap` `2024-03-10 02:30:00` (the Los Angeles spring gap), `yonly`
  `2024`.
- **Functions (58):** `hour`, `minute`, `second`; `year`, `quarter`, `month`, `dayofmonth`, `dayofweek`,
  `dayofyear`, `weekofyear`, `weekday`; `extract(<f> FROM c)` and `date_part('<f>', c)` for `YEAR`, `YEAROFWEEK`,
  `QUARTER`, `MONTH`, `WEEK`, `DAY`, `DAYOFWEEK`, `DAYOFWEEK_ISO`, `DOY`, `HOUR`, `MINUTE`, `SECOND`; five aliases
  (`extract(H …)`, `extract(HRS …)`, `date_part('min', …)`, `date_part('S', …)`, `extract(SECONDS …)`);
  `CAST(date_trunc('<u>', c) AS STRING)` for ten units; `unix_timestamp(c)`; `date_format(c, 'HH')` and
  `date_format(c, 'yyyy-MM-dd HH:mm:ss.SSSSSS')`; controls `hour` / `minute` / `second` of `CAST(c AS TIMESTAMP)`,
  `CAST(CAST(c AS TIMESTAMP) AS STRING)`, `CAST(CAST(c AS DATE) AS STRING)`.
- **Projection doors:** SQL `SELECT f(c) FROM src`, `F.<f>(col)`, `selectExpr`, `F.expr`: 58 functions in one
  `SELECT`, one query per function when that raises.
- **`WHERE` / `GROUP BY` / `ORDER BY` doors** (SQL and frame, six), over a 12-row table of the texts Spark parses
  (`date` … `zulu`, `lead`, `trail`, `null`, `nosec`, `gap`, `yonly`) on `I`, `P`, `V`, for 11 functions; `WHERE
  f(c) = <the answer for full>`, `GROUP BY f(c)` with counts, `ORDER BY f(c) NULLS FIRST, k`.
- **Classes.** **EQUAL**: the same value, or the same error condition. **WRONG**: main answers a value and Spark
  another value, or Spark raises. **RAISE**: main raises where Spark answers, or raises another condition.

**Size:** 60,184 cells in the four `TIMESTAMP_LTZ` configurations (4 × (14,848 projection + 198 multi-row)), 14,848
in the two `TIMESTAMP_NTZ` ones: 75,032 per engine.

## 2. `hour`, `minute`, `second` (the card)

| function | cells | EQUAL | WRONG | RAISE |
|---|---|---|---|---|
| `hour` | 1,024 projection + 72 multi-row | 128 | 384 | 512 + 72 |
| `minute` | the same | 128 | 384 | 512 + 72 |
| `second` | the same | 256 | 256 | 512 + 72 |

- **WRONG (silent), every input, every door, both modes, both zones:** `full`, `tsep`, `frac`, `off`, `zulu`, `gap`
  for `hour` and `minute` (main `0`; Spark `3`, `23`, `3`, `21`/`13`, `3`/`19`, `2`/`3` for `hour` in UTC / LA),
  and `full`, `frac`, `off`, `zulu` for `second` (`tsep` and `gap` have second `0`). 16 cells per text and
  configuration (4 inputs × 4 doors).
- **RAISE, both modes:** `lead`, `trail`, `empty`, `bad`, `nosec`, `tonly`, `badtime`, `yonly`: main's arrow
  `Utf8 → Date32` cast raises (`Cast error: Cannot cast string …`) at planning (literal) or execution (columns).
  Spark answers `lead`, `trail`, `nosec`, `tonly`, `yonly` in both modes, NULL for `empty`, `bad`, `badtime` with ANSI
  off and `CAST_INVALID_INPUT` with it on.
- **EQUAL:** `date` (a midnight: `0` on both) and `null`.
- **The multi-row doors:** all 72 cells per function RAISE: the table holds `lead`, `trail`, `nosec`, `yonly`, and
  main's cast raises for the whole query.
- **No door, input or zone is spared:** the four projection doors and the four inputs give the same verdict in
  every cell of these three functions ([evidence/verdicts/summary.md](evidence/verdicts/summary.md), "Door spread":
  none listed for them).

Literal SQL answers, Spark / main, ANSI off, `America/Los_Angeles`:

| text | `hour` | `minute` | `second` | `extract(SECOND …)` | `year` | `hour(CAST(c AS TIMESTAMP))` |
|---|---|---|---|---|---|---|
| `full` | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| `tsep` | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| `off` | 13 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / **2023** | 13 / 13 |
| `zulu` | 19 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 19 / 19 |
| `lead` | 3 / raise | 4 / raise | 5 / raise | 5.000000 / 5 | 2024 / raise | 3 / 3 |
| `nosec` | 3 / raise | 4 / raise | 0 / raise | 0.000000 / NULL | 2024 / raise | 3 / 3 |
| `bad` | NULL / raise | NULL / raise | NULL / raise | NULL / NULL | NULL / raise | NULL / NULL |
| `gap` | 3 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 3 / 3 |

## 3. Spark checks

- **Implicit cast = explicit cast:** Spark's `hour(c)`, `minute(c)`, `second(c)` equal its
  `<f>(CAST(c AS TIMESTAMP))` on all 3,072 `TIMESTAMP_LTZ` projection cells; Spark's `EXPLAIN` prints the same
  `hour(cast(c as timestamp), Some(<zone>))` for both ([evidence/raw/map.md](evidence/raw/map.md)).
- **`spark.sql.timestampType=TIMESTAMP_NTZ`:** they differ on 96 of 768 cells, wherever an NTZ cast and an instant
  cast of the text disagree: `off` (both zones) and `zulu` (Los Angeles), the offset dropped by the NTZ cast; `gap`
  (Los Angeles), which has no gap as a wall; `tonly`, which the NTZ cast refuses (`CAST_INVALID_INPUT`) and the
  instant cast reads as today. The written `CAST … AS TIMESTAMP` follows `spark.sql.timestampType`; the implicit cast
  stays the instant: the jar's rule (sketch §1), measured.
- **Unparsable text:** NULL with ANSI off, `CAST_INVALID_INPUT` with it on (`empty`, `bad`, `badtime`).

## 4. The seat forecast: main's own `CAST(c AS TIMESTAMP)` path

Seat A (sketch §4) gives `hour(c)` the plan `hour(CAST(c AS TIMESTAMP))` has today. Main's answer for the
`cast_*` control, put against Spark's answer for the bare function, cell by cell:

| function | LTZ cells EQUAL | WRONG | NTZ cells EQUAL | WRONG |
|---|---|---|---|---|
| `hour` | 1,014 of 1,024 | 10 | 251 of 256 | 5 |
| `minute` | 1,024 of 1,024 | 0 | 256 of 256 | 0 |
| `second` | 1,024 of 1,024 | 0 | 256 of 256 | 0 |

The 15 residue cells are one cell family: **`F.expr` over a string literal, Los Angeles** (`date`, `full`, `tsep`,
`frac`, `gap`; `a0-la`, `a1-la`, `a1-la-ntz`): `F.expr("hour(CAST('2024-01-01 03:04:05' AS TIMESTAMP))")` answers
`19` (the text read as UTC), SQL, `selectExpr` and `F.hour(F.lit(…).cast('timestamp'))` answer `3`.
`CAST(CAST(c AS TIMESTAMP) AS STRING)` is wrong on the same 10 LTZ cells. `minute` and `second` hide it only
because Los Angeles is a whole-hour offset. It is a gap of the written cast on that door today, not of this unit's
seat (sketch Q5).

## 5. Everything else, by function and text

Not-EQUAL projection cells; each entry counts cells out of 64 (4 configurations × 4 inputs × 4 doors); `W` WRONG,
`R` RAISE, `.` all EQUAL.

| function | date | full | tsep | frac | off | zulu | lead | trail | empty | null | bad | nosec | tonly | badtime | gap | yonly |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hour | . | W64 | W64 | W64 | W64 | W64 | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | W64 | R64 |
| minute | . | W64 | W64 | W64 | W64 | W64 | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | W64 | R64 |
| second | . | W64 | . | W64 | W64 | W64 | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| year | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| quarter | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| month | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| dayofmonth | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| dayofweek | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| dayofyear | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| weekofyear | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| weekday | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| extract_YEAR | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_YEAROFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| extract_QUARTER | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_MONTH | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_WEEK | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_DAY | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_DAYOFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R32 | R64 | R32 | R64 | R32 | R64 | R64 | R64 |
| extract_DAYOFWEEK_ISO | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| extract_DOY | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_HOUR | . | . | . | . | . | . | . | . | . | . | . | W32R32 | W32R32 | . | W32 | W32R32 |
| extract_MINUTE | . | . | . | . | . | . | . | . | . | . | . | W32R32 | W32R32 | . | . | W32R32 |
| extract_SECOND | W64 | W64 | W64 | W64 | W64 | W64 | W64 | W64 | . | . | . | W32R32 | W32R32 | . | W64 | W32R32 |
| date_part_YEAR | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_YEAROFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| date_part_QUARTER | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_MONTH | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_WEEK | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_DAY | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_DAYOFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| date_part_DAYOFWEEK_ISO | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| date_part_DOY | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_HOUR | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W8R48 | W8R56 |
| date_part_MINUTE | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | R48 | W8R56 |
| date_part_SECOND | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W16R48 | W8R56 |
| extract_H | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W8R48 | W8R56 |
| extract_HRS | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W8R48 | W8R56 |
| date_part_min | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | R48 | W8R56 |
| date_part_S | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W16R48 | W8R56 |
| extract_SECONDS | W64 | W64 | W64 | W64 | W64 | W64 | W64 | W64 | . | . | . | W32R32 | W32R32 | . | W64 | W32R32 |
| date_trunc_YEAR | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_QUARTER | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MONTH | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_WEEK | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_DAY | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_HOUR | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MINUTE | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_SECOND | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MILLISECOND | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MICROSECOND | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| unix_timestamp | . | . | . | . | . | . | W48 | . | . | . | . | . | . | . | W12R12 | . |
| date_format_HH | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | W32 | W64 |
| date_format_full | R64 | R64 | R64 | R64 | R64 | R64 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | R64 | W64 |
| cast_hour | W2 | W2 | W2 | W2 | . | . | . | . | . | . | . | . | . | . | W2 | . |
| cast_minute | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . |
| cast_second | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . |
| cast_ts | W2 | W2 | W2 | W2 | . | . | . | . | . | . | . | . | . | . | W2 | . |
| cast_date | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |

## 6. The neighbours (not this card; sketch §6 and Q2–Q4)

| id | what | cells | loud? |
|---|---|---|---|
| N1 | `year` … `weekday` and `CAST(c AS DATE)` over `off`: the UTC date (`2023-12-31`) where Spark reads the text's date | 64 per function | **silent** |
| N2 | the same functions raise on `lead`, `trail`, `nosec`, `tonly`, `badtime`, `yonly` (Spark answers) and on `empty`, `bad` with ANSI off (Spark NULL) | 512 per function | loud |
| N3 | `date_trunc`, `date_format`: NULL where Spark answers (`lead`, `trail`, `nosec`, `tonly`, `yonly`) or raises (`empty`, `bad`, `badtime` with ANSI on); the `off` / `zulu` texts read as a UTC wall in Los Angeles | 32–64 per text | **silent** |
| N4 | `extract(SECOND …)`, `date_part('SECOND' / 'S', …)`: an integer where Spark answers `decimal(8,6)` (`5` against `5.000000`, `5.123456`), every parsed text | 672 + | **silent** |
| N5 | SQL `date_part('<f>', <string>)`: fails to plan (`SparkDatePart`), every field | 48 of 64 per text | loud |
| N6 | `extract` / `date_part` of `YEAROFWEEK`, `DAYOFWEEK`, `DAYOFWEEK_ISO` and the aliases `H`, `HRS` on the SQL doors: refused or unparsed | 48–64 per text | loud |
| N7 | `extract(HOUR …)` over `gap` in Los Angeles: `2` where Spark answers `3` (the naive wall is not resolved in the session zone); `nosec`, `tonly`, `yonly` raise or are NULL | 32 per text | silent / loud |
| N8 | `F.expr` string-literal `CAST(… AS TIMESTAMP)` read as UTC outside UTC (§4) | 10 LTZ + 5 NTZ | **silent** |
| N9 | `unix_timestamp(lead)`: SQL answers where Spark raises or answers NULL (no trim in Spark's format parse); `gap` raises `CANNOT_PARSE_TIMESTAMP` in Los Angeles | 60 | silent / loud |
| N10 | `date_format(c, '…SSSSSS')`: raises on every parsed text | 448 | loud |
| N11 | `CAST(<offset text> AS TIMESTAMP)` under `TIMESTAMP_NTZ` keeps the instant (Spark drops the offset) | 52 of 256 `cast_hour` NTZ cells | **silent** |

Up: [map.md](map.md).
