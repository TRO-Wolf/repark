# CSV-SCHEMA-TIMESTAMP-ZONE-1 — the bar (step 0's sketch §6, unit 0), measured

**Builds.** Main: package `S/ddu-work/pkg-main-4c365814` (main `6903fb48` has the same product tree). Head: package
`S/ddu-work/pkg-csvts-186c34b0` (`BUILT_FROM` `186c34b005acce5605a02929a4c1f6ba6853eb3e`). Spark 4.1.2: step 0's
recorded cells (PR #1364) for the table; the extra shapes were measured live. Every RePark run printed
`repark._native.__file__` from its package. Raw cells and verdicts: [evidence/raw/](evidence/raw/map.md).

## The bar, line by line

| bar line | measured | met |
|---|---|---|
| `csv_s`, `csv_f` matching 128 of 128 each, as `csv_fix` | 128 of 128 each (main 64); every answer equals `csv_fix`'s (256 of 256 each) | yes |
| cross-setting as `csv_fix`'s 54 / 74 | 54 EQUAL, 74 view gap, 0 direct each (main 22 / 0 / 106) | yes |
| the 8 `csv_s` `d` writes EQUAL | 8 of 8 (CTAS, `INSERT INTO`, `writeTo().create()`, `insertInto`, `utc_la`, both ANSI modes) | yes |
| no other cell moves | step 0's table: no verdict moves outside `csv_s` and `csv_f`; reader sweep 0 EQUAL lost; write sweep 0 cells differ | **one exception: 14 T cells lost EQUAL** (below) |
| `EXPLAIN` changes only for CSV reads with a timestamp field | of step 0's 16 plans, only `csv_s`'s 4 change (`pq`'s differ by part-file name only) | yes |

## Step 0's table (5,732 cells), main against the head

| set | main EQUAL / WRONG / RAISE / no Spark | head | cells moved |
|---|---|---|---|
| Z | 2,214 / 842 / 528 / 512 | 2,406 / 650 / 528 / 512 | 256: `csv_s`, `csv_f` 96 WRONG → EQUAL and 32 WRONG → WRONG each |
| A | 362 / 43 / 411 / 68 | the same | 68 answers, 0 verdicts: `csv_sov` RAISE → RAISE (the error text) |
| T | 198 / 142 / 0 / 68 | 218 / 122 / 0 / 68 | 68, all `csv_s`: 34 WRONG → EQUAL, 20 WRONG → WRONG, **14 EQUAL → WRONG** |
| S | 0 / 0 / 8 / 0 | the same | 0 |
| W | 152 / 136 / 0 / 48 | 160 / 128 / 0 / 48 | 24, all `csv_s`: 8 `d` writes WRONG → EQUAL, 16 `eb_hour`/`sb_hour` WRONG → WRONG |

- **Z.** The 32 WRONG → WRONG cells per reader are the 74 view cells' body columns (`hour`, `CAST`, `date_format`
  above the parse, decided under the reader's zone: unit 1's gap, as `json_s`); their `u` is now Spark's.
- **A.** `csv_sov` (an `INT` text that overflows) has a parseable timestamp column, so the re-read fires and the
  integer is cast from text: `Can't cast value 2147483648 to type Int32` becomes `Cannot cast string '2147483648' to
  value of Int32 type`. RAISE before and after (D2, loud); `csv_s`'s malformed file has no zone-less column and does
  not move.
- **W.** The 16 are the view gap of unit 1 (`json_s` alike).
- **T, the exception (14 EQUAL → WRONG).** The bare doors (`d`, `e`, `f`, `j`, `o`, `sa`, `osa`) of `NTZ_LTZ` and
  `NTZ_NTZ`: a frame made under `spark.sql.timestampType=TIMESTAMP_NTZ`. Spark types the field `TIMESTAMP_NTZ` and
  `unix_timestamp` reads its wall clock as UTC (`1704078000`). RePark's DDL types `TIMESTAMP` as `TIMESTAMP_LTZ`
  whatever the key (`evidence/probes/q2.py`: `TimestampType` and dtype `timestamp` for CSV and JSON alike), so main's
  UTC parse matched Spark's number by coincidence while its `hour` (`19`) and `CAST(ts AS STRING)`
  (`2023-12-31 19:00:00`) were wrong; the head parses under the session zone, so `hour` (`3`) and the string are
  Spark's and `unix_timestamp` is `1704106800`. The head's `csv_s` answers equal its `json_s` answers in all 68 T
  cells. Root cause: the DDL ignores `spark.sql.timestampType` (step 0 filed the symptom as D3); a card candidate,
  not this unit's seat.

## The reader sweep and the write sweep

- **Reader sweep** (`readers.py`, 1,176 cells here; four passes, both directions, both ANSI modes, `PG_URL` unset;
  Spark's recording from part 2's delta 3): main 734 EQUAL / 434 not / 8 no Spark, head 752 / 416 / 8. 40 moved: `csv_s`
  18 not → EQUAL and 20 not → not (the body cells above); `ice_t` 2 EQUAL → EQUAL (time travel relative to the
  clock). **0 EQUAL lost.**
- **Write sweep** (`wsweep.py`, 3,100 cells, both ANSI modes): **0 cells differ** between main and the head.

## Shapes outside step 0's table (`evidence/probes/x.py`, Spark live; 115 cells, three zones, both ANSI modes)

Main 44 EQUAL / 71 WRONG, head 91 / 24; 51 moved, none lost. 47 WRONG → EQUAL: the DST gap and overlap, ISO `T`
text, a header mapped by name and by position, two `TIMESTAMP` fields, an empty field, a directory, a `StructType`
schema, `format("csv").load`, `hour`/`CAST`/`DATE` bodies, a `TIMESTAMP` under a `TIMESTAMP_NTZ` session (values),
and `mixed`. 4 WRONG → WRONG: `hdrrev` (header `ts,n`; Spark applies the schema by position and answers NULLs,
RePark maps by name; now parsed under the session zone). Unchanged and still WRONG: `epoch` (an integer text in a
`TIMESTAMP` field; Spark NULL, RePark the epoch second), `nots`/`mixednots`. Unchanged and EQUAL: `TIMESTAMP_NTZ`
fields (no defect), date-only text (the scan's `Date32` cast is zone-aware), fractional text, `nullValue`.

**A side effect, named.** On a local path any `utf8_columns` makes the Rust pre-scan read every column as text
(`csv_utf8_schema_from_path`), so a schema read with a `TIMESTAMP` field now casts every column from its text, as
the `nullValue` and `multiLine` schema reads already did; on a remote or compressed path only the named columns are
re-read. `mixed` shows it: a `STRING` field over `007` answers Spark's `007` beside a `TIMESTAMP` field, and still
`7` without one (`nots`, `mixednots`: a silent direct gap, proposed D12).

## `EXPLAIN`, before and after (`csv_s` frame view made under Los Angeles, `WHERE n > 0`)

Main: `Projection: CAST(?table?.column_1 AS Int32) AS n, CAST(?table?.column_2 AS Timestamp(µs, "UTC")) AS ts` over
`Filter: __repark_cast_shield__(CAST(?table?.column_1 AS Int32)) > Int32(0)` and the scan's `partial_filters` with the
same shielded predicate. Head: `Projection: CAST(?table?.column_1 AS Int32) AS n, to_timestamp(?table?.column_2) AS ts`
over `Filter: CAST(?table?.column_1 AS Int32) > Int32(0)` (a cast from text is not a narrowing cast, so the shield
does not wrap it); the physical plan changes the same way. Full plans: `evidence/raw/main-E.jsonl`, `head-E.jsonl`.

## Pins

`python/repark/tests/io/test_csv_schema_timestamp_zone_1.py`, 28 cells (both ANSI modes: `csv_s`/`csv_f` matching
in both zones, a frame view and a SQL view read in the other zone both ways, the four writes, six shapes in two
zones), values from Spark 4.1.2's recordings. Main: 28 failed; head package: 28 passed
(`evidence/raw/pins-main.log`, `pins-head.log`).

Up: [map.md](map.md).
