# TEMP-VIEW-SESSION-SETTINGS-1 — domain table (measured)

Measured 2026-11-17 on main `c22d61a1` (package `pkg-dct-7bf32859`, product diff to `c22d61a1` empty) and, for the
`avg` cells only, #1355's package `pkg-avg-27d6149f`, against Spark 4.1.2. Every cell, both engines, is in
[evidence/raw/](evidence/raw/map.md): `verdicts.jsonl` pairs them, `headline.md` counts them, `summary.md` adds the
door-by-direction table, the follows grid and every write, nest, reader-side, `now` and unset cell.

## Axes and size

- **Keys and values:** ANSI (`true`/`false`), the session time zone (`UTC`/`America/Los_Angeles`), `caseSensitive`,
  `timeParserPolicy` (`CORRECTED`/`LEGACY`), `retainFractionDigitsOnTruncate`, `timestampType`, and the current
  namespace (`USE lc.ns1`/`lc.ns2`). Every key is created and read under both values, in both directions.
- **Doors (creation):** SQL `CREATE TEMP VIEW` (`sql`); DataFrame `createOrReplaceTempView` (`df`); a DataFrame held
  across the `SET` (`held`); `spark.table("v")` held, over a SQL view (`tsql`) and over a DataFrame view (`tdf`); SQL
  over SQL (`vss`), SQL over DataFrame (`vsd`), DataFrame over SQL (`vds`); a SQL view renamed after the `SET`
  (`ren`); an Iceberg catalog view (`cat`, Spark's in-memory catalog, RePark's memory catalog). The `held`, `tsql`,
  `tdf`, `df` and `vds` doors are read once under the creation value before the `SET`; their copies `heldf`, `tsqlf`,
  `tdff`, `dff`, `vdsf` are first touched after it (this matters on Spark, below).
- **Positions (reader):** plain, a CTE, a cross join, a scalar subquery, on the SQL, DataFrame and catalog doors of
  the ANSI and zone families; plain elsewhere.
- **Bodies:** 17 ANSI-gated (`/`, `%`, `+` overflow, `avg` past 38 digits, `CAST` to `SMALLINT`, `DATE`, `DOUBLE`,
  `INT`, `CAST(true AS DECIMAL(1,1))`, `hex(CAST(int AS BINARY))`, `CAST('abc' AS DOUBLE)`, `map['missing']`,
  `element_at(map, …)`, `element_at(array, 5)`, `array[5]`, `sequence(1, '3')`, a struct-key subscript with renamed
  fields); 10 zone-gated (`hour`, `CAST(ts AS STRING)`, `CAST(ts AS DATE)`, `date_format`, text to timestamp, a
  `TIMESTAMP '…'` literal, `from_unixtime(0)`, date to timestamp, `date_trunc`, `current_timezone()`); 3 case, 2
  parser, 2 decimal, 2 timestamp-type, 1 namespace body.
- **Also:** writes (below), two-level views with different settings at each level, reader-side expressions over a
  view, `current_timezone()` and `current_timestamp()` across the view boundary, the unset case.
- **Size:** 2,238 read cells and 96 write cells per engine on main, 70 read and 12 write `avg` cells on #1355, 76
  direct queries, 32 nested reads, 30 reader-side, 4 `now`, 25 unset cells: 2,583 measured comparisons, plus 1,033
  creation, rename, fixture and conf steps recorded. Sampling: every body runs on every door in both directions; the four
  positions run only on the three doors where the reader's planner sees the view's plan.

## Headline (cells whose created and read values differ)

From `headline.md`: **1,414 cells; 740 agree with Spark; 512 view gaps (393 silent, 119 loud); 162 direct gaps.** A
view gap is a wrong cell whose two direct queries agree with Spark (both raising counts as agreeing). A direct gap is a
cell whose body already differs from Spark without a view; those say nothing about views and are listed in §6.

| family | reads: view gaps (silent / loud) | writes: wrong (silent / loud) | nested and reader-side | doors with no view gap |
|---|---|---|---|---|
| ANSI, created on → read off | 74 / 10 | 14 / 0 | 2 / 2, both directions | `held`, `tdf`, `heldf`, `tdff`, `cat` |
| ANSI, created off → read on | 0 / 86 | 1 / 12 | (in the row above) | same |
| zone, both directions | 250 / 0 | 36 / 0 | 16 / 0 | `held`, `tdf`, `cat`; `heldf`, `tdff` differ only for `current_timezone()` |
| `caseSensitive` | 0 / 3 | — | — | `df`, `held`, `heldf`, `dff` |
| `retainFractionDigitsOnTruncate` | 0 / 6 | — | — | `df`, `held`, `heldf`, `dff` |
| `timeParserPolicy`, `timestampType`, namespace | 0 / 0 | — | — | all (the first two are direct gaps: RePark does not honour the runtime value) |

**The zone half is silent and sits inside the label's ANSI-on scope** (all 250 zone read gaps and 36 zone write gaps
ran with ANSI on). **The ANSI half is silent only for a reader on ANSI off** (on → off); a reader on ANSI on meets it
as a loud raise where Spark answers (off → on, 86 cells), and only after a `SET` inside the session.

## 1. Which setting each engine follows (plain position)

`C` creation, `R` reader, `=` the two direct answers agree (not discriminating), `N` neither. Each cell is
`Spark/RePark`, created-first direction then the other. Full grid: `summary.md`.

| family | SQL view, view over view, rename, `spark.table` over SQL | DataFrame view | held frame | Iceberg catalog view |
|---|---|---|---|---|
| ANSI, analysis-time bodies (`/`, `%`, `bool_dec`, `int_bin`, `dbl_lit`, struct subscript) | C/R | C/C (by accident: §3) | C/C | R/R |
| ANSI, execution-time bodies (`+`, `CAST(s AS DOUBLE)`) | C/R | C/R | C/C | R/R |
| ANSI, `avg` (#1355) | C/R | C/C one way, C/R the other | C/C | — |
| zone, nine value bodies | C/R | C/R | C/C | R/R |
| zone, the `TIMESTAMP '…'` literal | C/R | C/C | C/C | R/R |
| `current_timezone()` | R/R | R/R | C/C if acted on before the `SET`; R/C on a fresh frame | R/R |
| `caseSensitive` (`SELECT A FROM cs`) | C/R | C/C | C/C | — |
| decimal legacy switch | C/N (RePark refuses `CANNOT_UP_CAST_DATATYPE`) | C/C | C/C | — |
| `timeParserPolicy` | R (Spark) | C if acted on before the `SET`, R if not | same | — |
| namespace (`USE`) | C/C | C/C | C/C | — |

**Spark's own rule, measured:** every temp-view door uses the creation setting, except `current_timezone()`
(reader) and `timeParserPolicy` (whoever first evaluates the expression: Spark builds the parser lazily from
`SQLConf.get`); an Iceberg catalog view follows the reader throughout. A held Spark Dataset fixes analysis-time
settings at creation and optimizer-time ones (`current_timezone()`) at its first action.

## 2. Cells (plain position; `=` same as Spark on that door, bold the RePark answer where it differs)

The Spark column is the SQL view's answer; on the catalog door Spark answers as the reader (the direct query under
the read value), and `=` there means RePark matches that.

**ANSI, created on, read off:**

| body | Spark | SQL view | DataFrame view | held | `spark.table` held | SQL over SQL | catalog view |
|---|---|---|---|---|---|---|---|
| `10 / b` | RAISE `DIVIDE_BY_ZERO` | **null** | = | = | **null** | **null** | = |
| `10 % b` | RAISE `REMAINDER_BY_ZERO` | **null** | = | = | **null** | **null** | — |
| `a + 1` | RAISE `ARITHMETIC_OVERFLOW` | **-2147483648** | **-2147483648** | = | = | **-2147483648** | = |
| `avg(x)` (#1355) | RAISE `ARITHMETIC_OVERFLOW` | **null** | **null** | = | **null** | **null** | — |
| `CAST(t AS DECIMAL(1,1))` | RAISE `NUMERIC_VALUE_OUT_OF_RANGE` | **null** | = | = | **null** | **null** | — |
| `CAST(s AS DOUBLE)` | RAISE `CAST_INVALID_INPUT` | **null** | **null** | = | = | **null** | — |
| `mk[named_struct('j',1,'k',2)]` | 7 | **RAISE `DATATYPE_MISMATCH`** | = | = | **RAISE** | **RAISE** | — |

**ANSI, created off, read on:**

| body | Spark | SQL view | DataFrame view | held | `spark.table` held | SQL over SQL | catalog view |
|---|---|---|---|---|---|---|---|
| `10 / b` | null | **RAISE** | = | = | **RAISE** | **RAISE** | = |
| `a + 1` | -2147483648 | **RAISE** | **RAISE** | = | = | **RAISE** | = |
| `avg(x)` (#1355) | null | **RAISE** | = | = | = | **RAISE** | — |
| `hex(CAST(a AS BINARY))` | 7FFFFFFF | **RAISE `CAST_WITH_CONF_SUGGESTION`** | = | = | **RAISE** | **RAISE** | — |
| `CAST('abc' AS DOUBLE)` | null | **RAISE** | = | = | **RAISE** | **RAISE** | — |
| `CAST(s AS DOUBLE)` | null | **RAISE** | **RAISE** | = | = | **RAISE** | — |

**Zone, created UTC, read Los Angeles (the reverse direction mirrors it, every cell):**

| body | Spark | SQL view | DataFrame view | held | `spark.table` held | SQL over SQL | catalog view |
|---|---|---|---|---|---|---|---|
| `hour(ts)` | 2 | **18** | **18** | = | = | **18** | = |
| `CAST(ts AS STRING)` | 2024-01-01 02:30:00 | **2023-12-31 18:30:00** | **…18:30:00** | = | = | **…18:30:00** | = |
| `CAST(ts AS DATE)` | 2024-01-01 | **2023-12-31** | **2023-12-31** | = | = | **2023-12-31** | — |
| `date_format(ts, 'yyyy-MM-dd HH')` | 2024-01-01 02 | **2023-12-31 18** | **…18** | = | = | **…18** | — |
| `unix_timestamp(CAST(s AS TIMESTAMP))` | 1704076200 | **1704105000** | **1704105000** | = | = | **1704105000** | — |
| `unix_timestamp(TIMESTAMP '…')` | 1704076200 | **1704105000** | = | = | **1704105000** | **1704105000** | = |
| `from_unixtime(0)` | 1970-01-01 00:00:00 | **1969-12-31 16:00:00** | **…16:00:00** | = | = | **…16:00:00** | — |
| `unix_timestamp(CAST(dt AS TIMESTAMP))` | 1704067200 | **1704096000** | **1704096000** | = | = | **1704096000** | — |
| `CAST(date_trunc('DAY', ts) AS STRING)` | 2024-01-01 00:00:00 | **2023-12-31 00:00:00** | **…** | = | = | **…** | — |
| `current_timezone()` | America/Los_Angeles | = | = | = (C/C) | = | = | = |

- The CTE, join and scalar-subquery positions repeat the plain answer on every door and body (SQL door: 15 ANSI and
  18 zone gaps per position; DataFrame door: 5 and 16).
- The renamed view and every view-over-view door answer as the SQL view does; SQL over DataFrame answers as the
  DataFrame view does. A rename keeps nothing from creation today.

## 3. Mechanism notes the cells show

- **The DataFrame door keeps division's creation mode by accident.** RePark's registered frame plan already carries
  the creation-time analyzer rewrite, and the reader's analyzer wraps it again: created on, read off, the plan is
  `10 / nullif(__repark_ansi_nonzero_divisor__(CAST(b AS Float64)), 0)` (`evidence/raw/explain-main.txt`), so the
  guard raises first. Created off, read on, `nullif` runs first and the guard sees NULL. Every analysis-time rule
  that wraps rather than replaces behaves this way; execution-time readers (`a + 1`) follow the reader.
- **`spark.table("v")` over a SQL view, held across the `SET`, is a mixed case:** `scan_replanned` plans the text
  with the live `self.ctx`, so SQL planning and analysis follow the live session (division, the literal), while the
  UDFs follow the held frame's snapshot (`a + 1`, `hour`).
- **Held frames match Spark on collect, not on write** (§4).

## 4. Writes (every wrong write)

A view or held frame created under one value, written under the other, the stored rows read back. Spark refuses
(nothing stored) where it raises.

| door | ANSI wrong (on → off / off → on) | zone wrong (both directions) | what RePark stores |
|---|---|---|---|
| `CREATE TABLE … AS SELECT … FROM sql_view` | 3 / 3 (`/`, `+`, `avg`) | 8 (`hour`, `str`, `date`, literal) | the writer's settings: NULL or wrapped values where Spark raises; values in the writer's zone |
| `INSERT INTO … SELECT … FROM sql_view` | 3 / 3 | 8 | same |
| `spark.table(sql_view).writeTo(…).create()` | 3 / 3 | 8 | same |
| `CREATE TABLE … AS SELECT … FROM df_view` | 2 / 1 (`+`, `avg`) | 6 (not the literal) | same |
| held frame `.write.parquet(path)` | 2 / 1 | 6 | same |
| held frame `.writeTo(…).create()` | 1 / 2 | 0 | ANSI as the writer; the zone matches because Spark itself follows the writer here |

**63 wrong writes** (51 silent, 12 loud), including 9 `avg` writes on #1355. The held-frame write doors go through a
scratch temp view (`writer_layout.py:95-121`), so a held frame that collects as Spark does writes under the writer's
settings. Spark's own writers disagree with each other: `write.parquet` stores the creation zone, `writeTo` re-analyses
the parsed plan and stores the writer's zone while keeping the parse-time ANSI mode of `+` and `/` (and the writer's
for `avg`, which is bound at analysis). Not measured: `saveAsTable`, `insertInto`, S3 paths, `MERGE` with a frame
source (inventory rows 7-9).

## 5. Traps, measured on today's code

- **Reader-side expressions (trap 2):** a filter `WHERE 10 / b IS NULL` and projections `10 / b`, `a + 1` over a view
  whose body is neutral follow the reader on both engines and both doors (18 ANSI cells, all equal today). The zone
  cells put the view's own column beside the reader's: `SELECT h, hour(ts) … WHERE hour(ts) = 18` over a UTC view read
  in Los Angeles is `2|18` on Spark and `18|18` on RePark — the reader's half is right, the view's half is the gap.
  These cells are the pin for "pushed filters stay in the reader's settings".
- **`current_timezone()` and `current_timestamp()` (trap 1):** `SELECT tz, current_timezone(), ct =
  current_timestamp() FROM v` answers the reader's zone twice and `true` on both engines, both doors, both
  directions (4 cells, equal today).
- **Nested views with different settings at each level (trap 4):** inner created under one value, outer under the
  other, read under each. Spark keeps each level's own setting (`inner.m` and `outer.m2` differ); RePark applies the
  reader's to both: 12 of 32 cells wrong (4 ANSI, 8 zone), on all four door pairs.
- **The unset case:** `SET spark.sql.ansi.enabled` answers `true` on Spark and `<undefined>` on RePark. A SQL view
  created with the flag never set and read under off: `10 / b` and `a + 1` answer NULL and `-2147483648` where Spark
  raises; the DataFrame view keeps division (the accident) and wraps `a + 1` (5 wrong of 25).

## 6. Direct gaps seen in passing (not view findings; the owner has not seen the new ones)

- `element_at(array(1,2), 5)` and `array(1,2)[5]` answer NULL with ANSI on; Spark raises
  `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` and `INVALID_ARRAY_INDEX`. **New.**
- `sequence(1, '3')` refuses `DATATYPE_MISMATCH` with ANSI on; Spark answers `[1,2,3]` (it refuses with ANSI off).
  This is VIEW-ANSI-CAPTURE-1's own body: on today's code it cannot show the view gap, because RePark refuses it
  directly.
- `10 % 0` with ANSI on raises `DIVIDE_BY_ZERO`; Spark raises `REMAINDER_BY_ZERO`.
- `CAST(2147483647 AS SMALLINT)`, `CAST('abc' AS DATE)`, `CAST('abc' AS INT)` raise Arrow's text in both modes;
  Spark raises `CAST_OVERFLOW` or `CAST_INVALID_INPUT` with ANSI on and answers `-1` or NULL with ANSI off.
- `CAST(true AS DECIMAL(1,1))` raises `NUMERIC_VALUE_OUT_OF_RANGE` without Spark's `.WITH_SUGGESTION` subclass.
- `CREATE TEMP VIEW v AS SELECT CAST('abc' AS DOUBLE)` with ANSI on refuses at creation (RePark folds the literal at
  analysis); Spark creates the view and raises when it is read.
- `SET spark.sql.legacy.timeParserPolicy=LEGACY` does not reach the parsing functions (`unix_timestamp('2024-1-1',
  'yyyy-MM-dd')` still raises); `SET spark.sql.timestampType=TIMESTAMP_NTZ` does not reach the engine (`typeof` stays
  `timestamp`).
- A relation with twin columns `a`, `A` (written under `caseSensitive=true`) read under `false`: Spark refuses
  `AMBIGUOUS_REFERENCE` even from a held frame; RePark's held frame answers (`evidence/raw/cs-*.json`).
- A held frame first acted on after a zone `SET`: `current_timezone()` answers the creation zone on RePark, the
  reader's on Spark (6 cells).

Up: [map.md](map.md).
