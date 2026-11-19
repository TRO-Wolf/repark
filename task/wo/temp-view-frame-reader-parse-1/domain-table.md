# TEMP-VIEW-FRAME-READER-PARSE-1 — the domain table (measured)

**Builds:** main `4c365814` (`main`), main before part 2 `6286ba19` (`part1`), Spark 4.1.2. **Cells:** 5,732 per RePark
build (3,030 cross-setting, the rest matching controls), [evidence/raw/cells-verdicts.jsonl](evidence/raw/map.md); every
table below is printed by `probes/tables.py` ([evidence/raw/tables.txt](evidence/raw/map.md)).

**Verdicts.** EQUAL: the rows equal Spark's, or both raise. WRONG: RePark answers other rows, or answers where Spark
raises. RAISE: RePark raises (or cannot make the frame or view) where Spark answers. No Spark: `smartCsv` and the two
emulation readers. **Gap class** of a cross-setting cell that is not EQUAL: **view** when both matching twins (the cell
made and read under the creating settings, and under the reading settings) are EQUAL, so only the change of settings
makes it wrong; **direct** when a twin is not EQUAL (RePark is wrong without any view). Writes have no matching twin;
their direct cells are named from the zone set.

**Sampled, and how.** Bodies are placed only in the doors that carry an expression (`eb`, `jb`, `ob`, `osb`, `sb`);
the bare doors (`d`, `f`, `e`, `j`, `o`, `sa`, `osa`) carry `ts` itself. The outer query is always
`SELECT <columns>, unix_timestamp(ts) AS u FROM <view>`, decided under the reading settings by both engines. The
timestamp-type and ANSI sets use two bodies each and fewer readers; the write set uses three sources, four writes and
seven readers. No aggregate is in scope, so no `target_partitions` series.

**Readers.** `json_s` `schema(…).json`, `json_f` `format("json").schema(…).load`, `json_m` a two-file directory,
`json_i` inferred, `json_tz` `option("timeZone")`, `csv_s` `schema(…).csv`, `csv_f` `format("csv").schema(…).load`,
`csv_i` `inferSchema`, `csv_n` `nullValue` with `inferSchema`, `csv_ns` `nullValue` without, `csv_0` no schema,
`csv_tf` `option("timestampFormat")`, `smart` `smartCsv` (control, no Spark), `pq` Parquet (control). **Emulation:**
`emu` is a Parquet file written from the JSON frame under the reading zone, so its instants are the ones Spark's scan
parses (the zone half of any seat that parses at scan time under the reader's zone); `csv_fix` is the CSV read with the
timestamp column kept as text (`utf8_columns`) and cast, the shape the JSON reader already has.

**Doors.** `d` the frame view; `f` `filter("n > 0")`; `e` `selectExpr("n", "n + 1 AS n1", "ts")`; `eb`
`selectExpr("n", "ts", <body>)`; `j` a USING join with a Parquet frame view that carries `hour(ts) AS ph`; `jb` the
join with a body; `o`, `ob` a frame over the frame view; `sa` `CREATE TEMP VIEW … AS SELECT *`, `sb` with a body;
`osa`, `osb` a frame over the SQL view. **Bodies:** `hour(ts)`, `CAST(ts AS STRING)`, `CAST(ts AS DATE)`,
`date_format(ts, 'yyyy-MM-dd HH:mm')`, `current_timezone()`; the ANSI set adds `n * 2000000000` and two malformed
files (a timestamp text `not a time`, an integer text `2147483648` for an `INT` field).

## 1. Headline

| set | cells | cross-setting | main EQUAL / WRONG / RAISE / no Spark | part 1 | main against part 1 |
|---|---|---|---|---|---|
| Z (zone) | 4,096 | 2,048 | 2,214 / 842 / 528 / 512 | 2,086 / 970 / 528 / 512 | 208 moves: `pq` 68 and `emu` 60 gained, `smart` 80 changed |
| A (ANSI) | 884 | 442 | 362 / 43 / 411 / 68 | 354 / 47 / 415 / 68 | 8 moves: `pq` gained |
| T (timestamp type) | 408 | 204 | 198 / 142 / 0 / 68 | the same | 0 |
| S (SQL file sources) | 8 | 0 | 0 / 0 / 8 / 0 | the same | 0 |
| W (writes) | 336 | 336 | 152 / 136 / 0 / 48 | 120 / 168 / 0 / 48 | 80 moves: `pq` 16 and `emu` 16 gained, `smart` 48 changed |

**Part 2 moved nothing here.** In every set, **0 cells** of the JSON and CSV lineage (every reader whose leaf is a JSON
or CSV scan, `csv_fix` included) differ between main and part 1, in both ANSI modes and both directions. The moves
are part 2's Parquet answers (gained, never lost) and `smartCsv` (ruled accepted, 2026-11-18).

## 2. The zone (the blocking half)

### 2.1 By reader, cross-setting cells (both directions, both ANSI modes)

| reader | EQUAL | view | direct | no Spark | total |
|---|---|---|---|---|---|
| `json_s`, `json_f`, `json_m` (each) | 54 | 74 | 0 | 0 | 128 |
| `csv_i`, `csv_n` (each) | 54 | 74 | 0 | 0 | 128 |
| `csv_s`, `csv_f` (each) | 22 | 0 | 106 | 0 | 128 |
| `json_i` | 104 | 4 | 20 | 0 | 128 |
| `csv_ns`, `csv_0` (each) | 100 | 4 | 24 | 0 | 128 |
| `json_tz`, `csv_tf` (each) | 0 | 0 | 128 (RAISE: the option is refused) | 0 | 128 |
| `pq` (control) | 128 | 0 | 0 | 0 | 128 |
| `emu` (against Spark's `json_s` cell) | 128 | 0 | 0 | 0 | 128 |
| `csv_fix` (against Spark's `csv_s` cell) | 54 | 74 | 0 | 0 | 128 |
| `smart` | — | — | — | 128 | 128 |

Matching cells: `json_s`, `json_f`, `json_m`, `csv_i`, `csv_n`, `pq`, `emu` 128 of 128 EQUAL; `csv_s`, `csv_f` 64 of
128 (every cell read under the Los Angeles zone is wrong); `json_i` 108, `csv_ns` and `csv_0` 104; `csv_fix` **128 of
128** EQUAL to Spark's `csv_s`.

**The view gap in the zone key: 382 cells**, all in the JSON and CSV lineage: 74 in each of `json_s`, `json_f`,
`json_m`, `csv_i`, `csv_n`, and 4 in each of `json_i`, `csv_ns`, `csv_0` (the `j` door). By pass: `ansi_true la_utc`
108, `ansi_true utc_la` 83, `ansi_false la_utc` 108, `ansi_false utc_la` 83 (the two directions differ only in
`CAST(ts AS DATE)`, which changes day for fewer hours from the Los Angeles side).

### 2.2 By door and body (cross-setting cells, all readers)

| door | EQUAL | view | direct | no Spark |
|---|---|---|---|---|
| `d`, `f`, `o`, `sa`, `osa` (each) | 44 | **0** | 12 | 8 |
| `e` | 36 | 0 | 20 | 8 |
| `j` | 8 | **32** | 16 | 8 |
| `eb`, `jb`, `ob`, `osb`, `sb` (each, five bodies) | 122 | **70** | 88 | 40 |

| body (in the body doors; `ts` in the bare doors) | EQUAL | view | direct | no Spark |
|---|---|---|---|---|
| `hour(ts)` | 40 | 100 | 140 | 40 |
| `CAST(ts AS STRING)` | 100 | 100 | 80 | 40 |
| `date_format(ts, …)` | 100 | 100 | 80 | 40 |
| `CAST(ts AS DATE)` | 150 | 50 | 80 | 40 |
| `current_timezone()` | 220 | **0** | 60 | 40 |
| `ts` | 264 | 32 | 96 | 56 |

What the shape says: **`ts` itself is right in every bare door** (both engines parse under the reading zone: Spark at
scan, RePark because the lineage answers as part 1 does). Every expression **above** the parse that reads the zone is
wrong (Spark decides it under the frame's zone, RePark under the reader's), through every door. `current_timezone()`
follows the reader in both. The `j` door's 32 view cells are the **Parquet** frame view's own `hour(ts) AS ph`: the
joined frame's lineage reaches the JSON scan, so the exemption takes the Parquet side with it (inventory E4).

Example (`Z/ansi_true/la_utc/json_s/eb_hour`): Spark `(1, 19, 1704078000)`, main and part 1 `(1, 3, 1704078000)`;
`emu` `(1, 19, 1704078000)`.

## 3. ANSI

| reader | EQUAL | view | direct | no Spark | total |
|---|---|---|---|---|---|
| `json_g` (good JSON, schema) | 24 | **10** | 0 | 0 | 34 |
| `json_sts`, `csv_sts` (malformed timestamp text; each) | 12 | **10** | 12 | 0 | 34 |
| `json_s`, `json_f`, `csv_s` (both malformed values; each) | 0 | 0 | 34 | 0 | 34 |
| `json_sov`, `csv_sov` (overflowing integer text; each) | 0 | 0 | 34 | 0 | 34 |
| `json_i`, `csv_i` (inferred, both malformed; each) | 32 | 0 | 2 | 0 | 34 |
| `csv_g` (good CSV, `inferSchema`) | 24 | 0 | 10 | 0 | 34 |
| `pq` (control) | 34 | 0 | 0 | 0 | 34 |
| `smart` | — | — | — | 34 | 34 |

**The view gap in the ANSI key: 30 cells**, all the body `n * 2000000000` above the parse, five body doors each
direction. Made with ANSI on and read with it off (`A/true_false/json_g/eb_mul`): Spark raises `ARITHMETIC_OVERFLOW`,
main answers `-294967296` (**silent**). Made off and read on: Spark answers `-294967296`, main raises (loud).
`pq` answers as Spark in both (part 2). **The parse itself has no ANSI view gap**, because RePark's parse is wrong
without any view: a malformed timestamp text raises `CAST_INVALID_INPUT` whenever the reading ANSI is on, and an
integer text that overflows `INT` raises an Arrow cast error in every mode, where Spark's PERMISSIVE parser answers
`NULL` in both modes (direct, loud). `csv_g`: RePark's inference types `n` `BIGINT` where Spark infers `INT`, so the
product does not overflow (direct).

## 4. The timestamp type

| reader | EQUAL | view | direct | no Spark |
|---|---|---|---|---|
| `json_s`, `csv_i` (each) | 17 | 0 | 17 | 0 |
| `csv_s` | 7 | 0 | 27 | 0 |
| `json_i` | 24 | 0 | 10 | 0 |
| `pq` | 34 | 0 | 0 | 0 |

**No view gap.** Both engines fix the column's type when the frame is made (`T/LTZ_NTZ/*` answers as `T/LTZ_LTZ/*`
on both sides). The direct cells: a frame made under `TIMESTAMP_NTZ` reads the text as a Los Angeles wall clock in
`unix_timestamp` (`1704106800`) where Spark reads it as UTC (`1704078000`), in the matching pass too; `csv_s` adds the
zone gap of §2.

## 5. Writes (made under one zone, written under the other, read back under UTC, ANSI on and off)

| source → write | EQUAL | WRONG | no Spark |
|---|---|---|---|
| `d` (the frame view) → CTAS, `INSERT INTO`, `writeTo().create()`, `insertInto` (each) | 22 | 2 | 4 |
| `eb_hour` (a frame view with `hour(ts)`) → each write | 8 | 16 | 4 |
| `sb_hour` (a SQL view adding `hour(ts)`) → each write | 8 | 16 | 4 |

By reader: `json_s`, `json_i`, `csv_i` 32 wrong of 48 each; `csv_s` 40 of 48; `pq` and `emu` **48 of 48 EQUAL**.

**Every wrong write (136):** `json_s`, `json_i`, `csv_i`, `csv_s` × (`eb_hour`, `sb_hour`) × (CTAS, `INSERT INTO`,
`writeTo`, `insertInto`), in all four passes (128 cells; the view gap: the written `hour` is the reader's, Spark writes
the frame's); and `csv_s` `d` × the four writes in `utc_la`, both ANSI modes (8 cells; the direct gap of §2: the CSV
timestamp parsed as UTC is written). Example: `W/ansi_true/la_utc/json_s/sb_hour/ctas` Spark `(1, 19, 1704078000)`,
main `(1, 3, 1704078000)`.

## 6. The SQL file-source doors

`CREATE TEMPORARY VIEW … USING json OPTIONS (path …)`, `… (n INT, ts TIMESTAMP) USING csv …`, a SQL view over the
latter and `` SELECT … FROM json.`path` ``: 8 cells, all RAISE on main and part 1 (a parse refusal and a missing
table), Spark answers. Not a door of this card (inventory P16).

## 7. Direct gaps seen in passing (not view gaps; each a card candidate)

| # | gap | cells | loud? |
|---|---|---|---|
| D1 | `schema(…).csv` / `format("csv").schema(…)`: a `TIMESTAMP` field is parsed as UTC whatever the session zone (DataFusion infers a naive timestamp; the cast reads it as UTC), so every read under a non-UTC zone is wrong, views or not, and is written wrong | 106 + 106 zone cells, 27 type cells, 8 writes | **silent** |
| D2 | malformed timestamp text and overflowing integer text in a JSON or CSV parse raise where Spark's PERMISSIVE parser answers `NULL` | the A set's direct cells | loud |
| D3 | `unix_timestamp` of a `TIMESTAMP_NTZ` column reads the session zone | T, 17 per reader | silent |
| D4 | `hour()` over a timestamp **string** column answers `0` (`json_i`, `csv_ns`, `csv_0`, matching settings too) | 20 matching and 20 cross-setting cells per reader | **silent** |
| D5 | CSV `inferSchema` types small integers `BIGINT` (Spark `INT`) | `csv_g` 10 | silent (only overflow shows it) |
| D6 | a SQL view over a `smartCsv` frame view raises `TABLE_OR_VIEW_NOT_FOUND` (`__repark_cdf_*`), main and part 1, reads and writes | 96 Z (12 per pass), 16 W | loud |
| D7 | SQL file-source doors refused | 8 | loud |
| D8 | `timeZone`, `timestampFormat` reader options refused (by design, `reader_support.py:584`) | 256 cross-setting and 256 matching | loud |

Up: [map.md](map.md).
