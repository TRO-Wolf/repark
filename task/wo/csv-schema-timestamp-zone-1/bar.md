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

**A named gain outside step 0's table: the LMT offset-seconds read (card CSV-READ-OFFSET-SECONDS-1).** A CSV
written under New York from `TIMESTAMP '1850-06-01 10:00:00'` holds `1850-06-01T10:00:00.000-04:56:02`; read back with
`schema("id INT, t TIMESTAMP")` it refused on main (`Invalid timezone "-04:56:02"`, the scan's typed parse) and now
answers Spark 4.1.2 in every column (`evidence/probes/lmt.py`, `evidence/raw/lmt.txt`): `(1, '1850-06-01 10:00:00',
-3773725438)` read under New York and `(1, '1850-06-01 14:56:02', -3773725438)` under UTC. RAISE → EQUAL; the pin
`python/repark/tests/timestamps/test_text_write_timestamp_zone_1.py::test_lmt_offset_seconds_csv_schema_read_answers_spark`
replaces the refusal pin (red on main, green on the head). The same file read with `t STRING` still refuses on main
and the head alike (no `TIMESTAMP` field, so no re-read; Spark answers the text).

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

## Fable's guidance (2026-11-19)

Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC. It is guidance, not an owner ruling. The
delegation, verbatim: "Go with your recommendations and tonight you don’t even need to wait on me even if fable says
that it’s my call. Merge what you need, follow the goal and good luck".

**Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — CSV schema timestamp zone
unit 0 2026-11-19.md`), §2 and §3. Fable's marks are kept: **[M]** measurement, **[R]** reading, **[Rec]**
recommendation.

**The 14 cells are a named exception (§2).** **[Rec]** A named exception in the bar. Unit 0 must not keep main's
answers; the builder was right to decline. Keeping them would leave `hour` and the string wrong and would lose the
builder-mode gain.

**[M]** One row `2024-01-01 03:00:00`, zone Los Angeles, `schema("n INT, ts TIMESTAMP")`:

| key set by | build | reader | type | `unix_timestamp` | `hour` | string |
|---|---|---|---|---|---|---|
| runtime `SET` | main | CSV | `timestamp` | 1704078000 | 19 | `2023-12-31 19:00:00` |
| runtime `SET` | main | JSON | `timestamp` | 1704106800 | 3 | `2024-01-01 03:00:00` |
| runtime `SET` | head | CSV | `timestamp` | 1704106800 | 3 | `2024-01-01 03:00:00` |
| runtime `SET` | head | JSON | `timestamp` | 1704106800 | 3 | `2024-01-01 03:00:00` |
| builder | main | CSV | `timestamp` | 1704078000 | 19 | `2023-12-31 19:00:00` |
| builder | main | JSON | `timestamp_ntz` | 1704078000 | 3 | `2024-01-01 03:00:00` |
| builder | head | CSV | `timestamp_ntz` | 1704078000 | 3 | `2024-01-01 03:00:00` |
| builder | head | JSON | `timestamp_ntz` | 1704078000 | 3 | `2024-01-01 03:00:00` |

- **[M]** Under the builder opt-in the head answers Spark in all three columns, and main does not. Spark's values are the
  builder's recording (`1704078000`, `3`, the wall-clock string). That is a gain the bar does not count yet.
- **[M]** Under a runtime `SET` the head's CSV equals JSON on both builds. The JSON cell was already WRONG on main in
  `unix_timestamp`. Unit 0 moved CSV onto the answer the rest of the engine already gives. `CAST(.. AS TIMESTAMP)` under
  a runtime `SET` is still `timestamp` on both builds.
- **The card:** TIMESTAMP-TYPE-RUNTIME-SET-1 (S2, not label-blocking), filed on the cards map.

**A named side gain, and what the card covers (§3).** A `STRING` field beside a `TIMESTAMP` field now answers Spark's
text, as a named side gain. The card CSV-STRING-FIELD-TEXT-1 covers the rest (S1 by class, label-blocking).

## Delta 1 (2026-11-19): the verifier's F1 and F2

**The verifier's findings** (Claude Opus 5.5, effort medium, 2026-11-19; verdict FAIL on head `ffe8e301`; recorded here
as the verifier's, not re-rated):

- **F1 (class a, S2, loud).** A `DECIMAL` field over exponent text (`1e2`, `1.5E-1`) beside a `TIMESTAMP` field, on a
  local path: EQUAL → RAISE under UTC (6 cells), WRONG → RAISE elsewhere. Spark and main answer `100.00` and `0.15`.
  Cause: the all-text pre-scan meeting the text → decimal cast.
- **F2 (class a, S2, silent; S1 by class if the owner applies unit 0's test).** A `TIMESTAMP_NTZ` field over a DST-gap
  wall clock (`2024-03-10 02:30:00`) beside a `TIMESTAMP` field, Los Angeles and New York: EQUAL → WRONG (8 cells), an
  hour forward. Cause: RePark's string → `TIMESTAMP_NTZ` cast resolves the gap through the session zone.
- **N1 (class c, S3).** `1e3`, `2.0` or `1.5` in an `INT`/`BIGINT` field, and epoch text in a second `TIMESTAMP` field
  with ANSI on, beside a `TIMESTAMP` field: Spark NULL, main a silent wrong value, the head a raw Arrow cast error.
- **R1, R2 (records).** The registry entry called two filed cards "proposed" and did not quote the owner; the PR body
  still said the bar misses awaited Fable.

**The remedy.** The orchestrator's call under the owner's delegation (verbatim in [map.md](map.md)): Fable's option
(b), which the consult had declined on cost alone. `read_options.rs` is the second product file.

- `read_csv_path` takes the all-Utf8 first-record pre-scan for `nullValue`, `multiLine`, and a `utf8_columns` read under
  `inferSchema`. Any other `utf8_columns` read lets DataFusion infer, then re-reads only the named columns as Utf8
  (`csv_utf8_column_schema`), as the `file://` and remote paths already did. So the other fields keep the scan's typed
  parse: F1's decimal is main's again.
- `_load_csv` names only the zone-less columns that the schema does not read into a `TIMESTAMP_NTZ` field
  (`_session_timestamp_text_columns`, pairing fields to columns as `_apply_reader_schema_semantics` does), and drops
  `inferschema` from the re-read's options (a user schema overrides it). **[R]** F2's NTZ column is itself a zone-less
  timestamp column, so without this it would still be named and cast from text.

**The caller inventory** (`git grep -n utf8_columns -- crates python`):

| caller | relied on the all-text read | measured |
|---|---|---|
| `reader.py` `_load_csv` (this unit) | only for side effects | the `STRING`-field text gain and the ANSI-off NULL for epoch text in a second `TIMESTAMP` field revert to main's answers (below) |
| `reader_support.py` `_finish_csv_infer_schema` (CSV-INFER-PERF-1) | **yes** | the intermediate package `pkg-csvts-0b45f6a6` (named-only on every path) lost 6 cells on a local path, by names and values: a column DataFusion types Null from its first 1,000 rows dropped a late value (4) and an empty header became `""`, not `_c1` (2). It gained 4 (a `comment` first line; untrimmed header names). So the `inferSchema` re-read keeps the pre-scan; on the delta 1 package its 32 cells are byte-equal to main |
| `read_options.rs` (the consumer); the user option bag | — | never taken from the user (`test_csv_infer_perf_1.py` pins it) |

**The bar** (package `S/ddu-work/pkg-csvts-1ee847b7`, `BUILT_FROM` `1ee847b7`, against main `pkg-main-4c365814`, the old
head `pkg-csvts-186c34b0` and Spark 4.1.2; EQUAL / RAISE / WRONG, main → old head → delta 1):

| line | main | old head | delta 1 | lost against main |
|---|---|---|---|---|
| the verifier's `vp.py` (329 cells, 8 BOTHERR) | 137 / 68 / 116 | 222 / 70 / 29 | 220 / 66 / 35 | 4: the accepted runtime-`SET` cells (`F/rtset`), as the old head |
| `vp.py` builder opt-in (12) | 6 / 0 / 6 | 12 / 0 / 0 | 12 / 0 / 0 | 0 |
| `vp2.py` text-cast matrix (432) | 246 / 40 / 146 | 320 / 60 / 52 | 328 / 40 / 64 | 0 |
| `vp3.py` `file://` and exponent (84) | 28 / 12 / 44 | 60 / 18 / 6 | 60 / 12 / 12 | 0 |
| `vp4.py` NTZ gap (42) | 20 / 6 / 16 | 16 / 6 / 20 | 24 / 6 / 12 | 0 |
| `csv_s`, `csv_f` matching | 64 of 128 each | 128 each | 128 of 128 each | 0 |
| cross-setting `csv_s`, `csv_f` | 22 / 0 view / 106 direct | 54 / 74 / 0 | 54 EQUAL / 74 view / 0 direct each | 0 |
| the 8 `csv_s` `d` writes | 0 | 8 | 8 of 8 | 0 |
| the LMT offset-seconds read (2) | RAISE | EQUAL | EQUAL; the pin passes | 0 |
| `x.py` (115) | 44 EQUAL / 71 WRONG | 91 / 24 | 85 / 30 | 0 |
| step 0's table (5,732) | Z 2,214, A 362, T 198, W 152 EQUAL | Z 2,406, A 362, T 218, W 160 | the old head's verdicts in every set | the 14 runtime-`SET` T cells, the named exception |
| reader sweep (1,176) | 732 EQUAL, 2 BOTHERR | 750, 2 | 750, 2 | 0 (1,174 cells equal the old head's; 2 clock-relative `ice_t` errors) |
| write sweep (3,100) | — | — | 0 cells differ from main or the old head | 0 |
| `delta1.py` (84, below) | — | — | — | 0 |

- **F1 reaches Spark.** `vp2` `{true,false}/{utc,la}/decimal102_{1,2}/ts` and `vp3` `*/local/dexp_ts` (UTC, Los
  Angeles, Tokyo) are EQUAL: RAISE → EQUAL against the old head; under UTC EQUAL on main too.
- **F2 reaches Spark.** `vp4` `{true,false}/{los_angeles,new_york}/ntz_gap_ts_{empty,z}` are EQUAL again (8), and
  `vp.py` `A/{true,false}/la/ntz_ts` is EQUAL (WRONG on main and the old head).
- **Moved for the worse against the old head (none against main), 16 cells.** The named side gain is withdrawn: a
  `STRING` field beside a `TIMESTAMP` field answers main's `7` for `007` again (`vp3` `local/mix` 6, `vp.py`
  `T/*/hdr/tsf` 2, `x.py` `mixed` 6), which the filed card CSV-STRING-FIELD-TEXT-1 covers; and epoch text in a second
  `TIMESTAMP` field under ANSI off answers main's epoch second, not Spark's NULL (`vp.py` `A/false/*/epoch2`, 2).
- **`A`'s 68 `csv_sov` answers** (an overflowing `INT` beside a `TIMESTAMP` field) are main's error text again
  (`Can't cast value 2147483648 to type Int32`): RAISE → RAISE.

**N1, against Spark and main.** Every N1 cell now answers main's value: WRONG on main, RAISE on the old head, WRONG on
delta 1. `vp2` `int_1`, `int_2`, `bigint_1` (`1e3`, `2.0`), both zones and ANSI modes (12); `vp.py`
`T/*/int_from_frac/ts` (2) and `A/true/*/epoch2` (2); `delta1.py` `N1/*` (12). For `1e3`, `2.0`, `1.5` in an `INT`
field Spark answers NULL and main `1000`, `2`, `1`; for `1704067200` in a `TIMESTAMP` field Spark answers NULL and main
the epoch second. Silent on main and here; D2's class (a malformed value answers a value or raises where Spark answers
NULL).

**`delta1.py`** (84 cells, Spark live; [evidence/raw/delta1-cells.jsonl](evidence/raw/delta1-cells.jsonl)): F1's
`decimal_exponent` EQUAL in both zones and ANSI modes (main WRONG under Los Angeles, the old head RAISE). F2's
`ntz_beside` (the `TIMESTAMP` field holds zone-less text), `ntz_empty_ts`, `ntz_z_ts` and the `file://` twin EQUAL in
both zones (Los Angeles: main WRONG or EQUAL, the old head WRONG). Two header shapes stay WRONG on all three builds (a
header mapped by name where Spark maps by position). A `STRING` field over ISO `T` text and a `DATE` field beside a
`TIMESTAMP` field keep the old head's EQUAL (a zone-less column read into a `STRING` field is still re-read). The
`inferSchema` cells (32): byte-equal to main.

**`EXPLAIN`** (`x.py`, a `csv_s` frame view, `WHERE n > 0`, a local path; `nots` has no `TIMESTAMP` field and is
unchanged): only the timestamp projection differs from main, and `n` keeps its cast shield again.

```
main:     Projection: CAST(?table?.column_1 AS Int32) AS n, CAST(?table?.column_2 AS Timestamp(µs, "UTC")) AS ts
            Filter: __repark_cast_shield__(CAST(?table?.column_1 AS Int32)) > Int32(0)
old head: Projection: CAST(?table?.column_1 AS Int32) AS n, to_timestamp(?table?.column_2) AS ts
            Filter: CAST(?table?.column_1 AS Int32) > Int32(0)
delta 1:  Projection: CAST(?table?.column_1 AS Int32) AS n, to_timestamp(?table?.column_2) AS ts
            Filter: __repark_cast_shield__(CAST(?table?.column_1 AS Int32)) > Int32(0)
```

Of step 0's 16 plans only `csv_s`'s 4 differ from main, the same 4 as the old head.

**Pins.** Four shapes in `python/repark/tests/io/test_csv_schema_timestamp_zone_1.py`
(`test_a_schema_timestamp_field_leaves_the_other_fields_on_the_typed_scan`: `decimal_exponent` and three
`TIMESTAMP_NTZ` shapes, UTC and Los Angeles, both ANSI modes, Spark 4.1.2's live answers): 8 cells, red on the old head
(8 failed, 28 passed), green on delta 1 (the file and the LMT file: 283 passed, 1 skipped); main fails 32. The Rust
test `read_options::tests::a_local_csv_utf8_columns_read_types_only_the_named_columns_as_utf8` (a local path, header
`n,ts,d`: `utf8_columns=ts` types `Int64`, `Utf8`, `Float64`; with `nullValue` or `inferSchema` all Utf8) fails on the
old head's condition (`left: [("n", Utf8), ("ts", Utf8), ("d", Utf8)]`) and passes on delta 1.

**F2's root, a card candidate for the clerk (not filed here).** RePark's string → `TIMESTAMP_NTZ` cast moves a DST-gap
wall clock an hour forward under a DST session zone: `CAST('2024-03-10 02:30:00' AS TIMESTAMP_NTZ)`, the JSON reader's
`TIMESTAMP_NTZ` field and the CSV `nullValue` `TIMESTAMP_NTZ` read, WRONG on main, the old head and delta 1 under Los
Angeles and New York (`vp4` `ntz_gap_{cast,json,nullv}`). Spark keeps the wall clock.

Raw: [evidence/raw/delta1-bar.txt](evidence/raw/delta1-bar.txt).

Up: [map.md](map.md).
