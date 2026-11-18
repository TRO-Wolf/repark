# TEMP-VIEW-SESSION-SETTINGS-1 part 2 — the bar (delta 1)

**Head:** product commit `513208ce` (`74c22dfe` plus delta 1), package `pkg-view2-513208ce` (`BUILT_FROM`
`513208ceb6dc…`). **Against:** main before part 1 (`pre`, `pkg-avg-27d6149f`), main with part 1 (`part1`,
`pkg-view1-c54f5fc7`), the build's head (`74c22dfe`) and Spark 4.1.2. Every set ran fresh on every build at
14:35–14:47 UTC, so the current-time cells pair. Pairing [probes/bar2.py](probes/map.md); cells [raw/](raw/map.md).

## The build's bar (3,004 cells: `run.py` both start modes, `rev.py`, `rc.py`, the delta 1 sweep)

| against | WRONG → EQUAL | WRONG → WRONG (changed) | EQUAL → RAISE | EQUAL → WRONG | matching-settings moves |
|---|---|---|---|---|---|
| part1 | 261 | 10 | 5 | 0 | 0 |
| pre | 622 | 33 | 9 | 0 | 0 |

The same counts as `74c22dfe`'s bar; the delta moves no cell of it (the 12 cells whose answers differ from the
build's run are current-time hours, paired within each run). Outside part 2's scope: the sweep's 40 WRONG → EQUAL and 8
WRONG → WRONG (`date_add_where`, the same refusal text-prefixed), part 1's `nest/read/sqldf/utc/m2` WRONG → EQUAL; no
move in the held door, the catalog views or the direct queries. **Part 2's targets:** of the 665 part 2 cells EQUAL on
`74c22dfe` (the 196 of the step 0's target among them), none is lost. Part 2's non-EQUAL cells left: 213, the build's
classes (`raw/part2-left.jsonl`).

## Named classes, cell for cell

| cell | head verdict | Spark | part1 | head |
|---|---|---|---|---|
| `write/writeto-held/on/off/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| `write/writeto-held/off/on/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] |
| `filter_sensitive/df/fdiv/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| `filter_sensitive/df/fdiv/off_on` | EQUAL | [["5.0"], ["double"]] | [["5.0"], ["double"]] | [["5.0"], ["double"]] |
| `filter/or_mixed/df/fdiv/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| `filter/or_mixed/df/fdiv/off_on` | EQUAL | [["5.0"], ["double"]] | [["5.0"], ["double"]] | [["5.0"], ["double"]] |
| `filter/add_sens/df/fadd/on_off` | REPARK_RAISES | [["2"], ["int"]] | [["2"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| `filter/agg_key_sens/df/fagg/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |

The ruled filter class (question 11 (a)): the four `/on_off` DataFrame-door cells, EQUAL → RAISE, loud, accepted; their
`/off_on` twins stay EQUAL. Question 4's `writeTo` `avg` divergences: `on/off` EQUAL → RAISE, `off/on` answers where
Spark raises (both recorded under question 4, outside the label's ANSI-on scope).

## The verifier's probes and Fable's probe

| set | cells | head against part1 | notes |
|---|---|---|---|
| `frames.py` | 30 | 4 WRONG → EQUAL (`frame/dfc`, `dff`, `dfl`, `dflim`, the verifier's class (c) S3 note); 0 other moves | the four JSON cells (`json/direct/read_UTC`, `json/frame_utc/direct_read_la`, `json/frame_utc_view_tk/read_la`, `json/sqlview_utc/read_UTC`) answer as part 1 and Spark again |
| `attack.py` | 133 | 2 WRONG → EQUAL, 1 WRONG → WRONG (`nest/df_over_sql`) | |
| `attack2.py`, `attack3.py`, `case.py` | 16, 7, 7 | 0 | `overlay/start_time` pairs again (it compares UTC's and Kiritimati's `current_date()`, which differ after 10:00 UTC; the verifier's files were recorded at 04:30) |
| Fable's `probe.py` | 14 | 0 answers moved; `EXPLAIN` differs only by the warehouse path | `jf.filter("n > 0")` and `jf.selectExpr("n + 1 AS n1", "ts")` back at Spark's answer; `hour(ts)` WRONG as on part 1 |

## The JSON and CSV lineage held at part 1's answers

The reader sweep (1,328 cells; 524 in the JSON and CSV lineage: JSON with and without a schema, CSV with a schema,
`inferSchema`, `nullValue` with and without `inferSchema`, and a JSON frame joined with a marked Parquet frame view):
**0 moves of any kind against part 1** in the lineage, in both ANSI modes and both directions. Against `74c22dfe` the
lineage returns to part 1's answers, including 20 `csv_s` cells that `74c22dfe` happened to answer as Spark (its zone
expression above the parse; part 1 and main answer them WRONG). Every other reader keeps part 2's answer: Parquet,
text, ORC, the Iceberg reads and Postgres move only in the views whose own expressions read the zone (36 cells each,
WRONG → EQUAL where Spark answers; Postgres has no Spark answer); `createDataFrame` likewise (8 cells; its leaf instant
is a creation-time direct gap on every build). Excel refuses on every build. `smartCsv`: 16 of 20 cells move against
part 1, none against `74c22dfe` (see the judgement call). Pins: `test_temp_view_reader_lineage.py` (eight cells,
both modes): head 16 passed; `74c22dfe` 16 failed; part 1 and main 16 passed. Rust:
`a_json_reader_lineage_keeps_todays_provider_through_views`.

## The stated mechanism, corrected (delta 1)

The build's hand-back said RePark's JSON source reads the session zone at scan time. It does not. The facade's JSON and
CSV readers build their typed parse as an ordinary projection over a scan of strings
(`python/repark/src/repark/spark/session/reader.py:662`, `_apply_reader_schema_semantics`): `EXPLAIN` shows
`Projection: CAST(?table?.n AS Int32) AS n, to_timestamp(?table?.ts) AS ts` over `TableScan: ?table?`. So the zone
lives in a frame expression, the "a frame that only scans" exemption misses it, and the parse cannot be told apart
from a user's own cast. Spark parses under the reader's settings at scan time and decides everything above the parse
under the frame's. Closing such a frame under its own settings parsed under the creation zone.

**Delta 1's remedy (Fable's guidance under the owner's delegation, not an owner ruling):** a view of either door whose
definition reaches a JSON or CSV listing scan, through any number of views, answers exactly as part 1's package does.
`reader_lineage` (`crates/repark-spark/src/view_ddl/frame_settings.rs`) walks the plan through markers, `FrameView`,
`ReplanningTempView` definitions and any provider that carries a plan, for a `ListingTable` whose format is
`JsonFormat` or `CsvFormat`. The frame door keeps `into_view`, with any inner marker unwrapped (`unmarked`), so the
plan is part 1's. The SQL door keeps part 1's inlined-frame fallback, restored unchanged in `temp_settings.rs`, and
`temp_view.rs` takes it only when the re-planned body reaches such a scan. Every Parquet-backed and Iceberg-backed
frame keeps part 2's answer.

## The reader inventory

Every reader that builds expressions over its scan, with the settings those expressions read. "Moves" is the reader
sweep's head against part 1 (`raw/readers-*.jsonl`): a direct frame view, `filter`, `selectExpr` without and with a
zone-sensitive expression, a frame over the frame view (two forms), a SQL view `SELECT *` and one adding `hour(ts)`
over each, and a frame over the SQL view; made under one zone and read under the other, both directions, both ANSI
modes.

| reader | file:line | reader-built expressions | settings they read | head against part 1 | class |
|---|---|---|---|---|---|
| JSON, user schema | `reader.py:512` `_load_json` → `:704`, `:719`, `:722`, `:727` (`CAST(col AS <type>)`, typed `NULL`); scan `repark-core/src/session.rs:797` (`ListingTable`, JSON) | the typed parse | zone (`CAST(string AS TIMESTAMP)`), the timestamp-type carrier (`TIMESTAMP` is LTZ or NTZ), ANSI (a malformed or overflowing value raises or answers NULL; Spark's parser answers NULL) | 0 moves | **the JSON and CSV lineage** |
| JSON, inferred | `reader.py:537` → `reader_support.py:544` (`CAST(CAST(col AS STRING) AS TIMESTAMP)` for an inferred `timestamp_ntz` column; the JSON inference yields none here) and `reader.py:684` with `nullValue` | the naive-timestamp cast, the `nullValue` `CASE` | zone and the timestamp-type carrier when a naive timestamp is inferred; none in the measured cells | 0 moves | the JSON and CSV lineage |
| CSV, user schema | `reader.py:494` `_load_csv` → `:704`–`:727`; scan `repark-core/src/read_options.rs:212` (`ListingTable`, CSV) | the typed parse | zone, the timestamp-type carrier, ANSI | 0 moves | the JSON and CSV lineage |
| CSV, `inferSchema` | `reader.py:738` → `reader_support.py:389` `_finish_csv_infer_schema` (re-read with `utf8_columns` `:427`, `try_cast` probes `:495` run once at creation, `CAST(col AS <promoted>)` `:524`) and `:544` | the timestamp promotion and the leftover numeric promotion | zone and the timestamp-type carrier (timestamps), ANSI (the promoted casts; the probes run under the creation state) | 0 moves | the JSON and CSV lineage |
| CSV, `nullValue` | `reader.py:684` (`CASE WHEN CAST(col AS STRING) = token THEN NULL`) over an all-string scan (`read_options.rs:225`), then `reader_support.py:447` `_promote_csv_string_types` with `inferSchema` | the null token, the promotion | none for the token; zone, the timestamp-type carrier and ANSI for the promoted casts | 0 moves | the JSON and CSV lineage |
| CSV, no schema, `inferSchema=false` | `reader.py:757` (`CAST(col AS STRING)`), `_cN` renames | the string cast | none for string columns | 0 moves | the JSON and CSV lineage |
| `smartCsv` (RePark extension, not a Spark API) | `reader.py:120` → `python/repark/src/repark/spark/_csv_smart.py:774` `load_smart_csv`: the file is read in Python at creation, `createDataFrame` of strings `:858`, `selectExpr("CAST(CASE WHEN c = '' THEN NULL ELSE c END AS <type>)")` `:873` | the typed casts | zone, the timestamp-type carrier, ANSI (decimal overflow raises) | moves: a view now parses under the creation zone, as `74c22dfe` did (`raw/smart-*.jsonl`) | **not detected**: its leaf is an in-memory table, not a file scan; see "A judgement call" |
| text | `reader_text.py:47` → `repark-core/src/text_scan.rs:639`; the zone read once at `:652` to place partition values inside `TextTableProvider` | none | the zone, captured in the provider at creation | no leaf move; derived views move as Parquet's do | not this class |
| ORC | `reader_orc.py:50` → `repark-core/src/orc_scan.rs:955`; the zone read at `:956` (partition values, the writer-zone wall clock); user schema `orc_schema.rs:211` (alias, `CAST(int AS STRING)`, typed `NULL`) | settings-free | the zone, captured in the provider | no leaf move; derived views move as Parquet's do | not this class |
| Excel | `reader.py:307` → `repark-python/src/session.rs:276` | — | — | refuses on every build (`UnsupportedOperationException`, not in this build) | no frame |
| Postgres | `reader.py:770` → `repark-core/src/session/read_postgres.rs:80` (a bare scan `:128`, the zone localiser captured `:114`) | none | the zone, captured in the provider | no leaf move; derived views move as Parquet's do (no Spark answer: no JDBC driver) | not this class |
| Iceberg time travel | `reader.py:362`, `:895` → `repark-core/src/session.rs:820` `read_iceberg_table` | none | the zone, once, to parse `timestampAsOf` at creation; the snapshot is pinned in the provider | no leaf move; derived views move as Parquet's do | not this class |
| Iceberg incremental | `reader_incremental.py:13` → `repark-python/src/session_sources.rs:99` | none | none at read (snapshot ids resolved at creation) | no leaf move; derived views move as Parquet's do | not this class |
| `createDataFrame` | values converted at creation | none at read | the zone at creation (naive datetimes; Spark uses the process zone, a creation-time direct gap on every build) | derived views move as Parquet's do | not this class |
| Parquet (control) | `reader.py:50` | none | none | derived views move to Spark | not this class |

## A judgement call: `smartCsv`

`smartCsv` builds settings-sensitive casts over its rows, so it is a reader of the inventory's kind, but its leaf is
the in-memory table `createDataFrame` makes, so the source detection cannot see it, and a frame view of it moves
against part 1 (part 1 parsed under the reader's zone; the head, like `74c22dfe`, under the creation zone). There is no
Spark answer: it is not a Spark API. I did not halt. The file is read and its rows built at creation, so the frame
is literally `createDataFrame` plus `selectExpr` casts, and for that composition Spark binds the creation zone; the
head's answer is that one. Holding it at part 1's answer would need a marker from the facade, which is a different
remedy and a file outside the cap. **For the orchestrator:** if part 1's answer is wanted, it belongs to
TEMP-VIEW-FRAME-READER-PARSE-1's unit.

## Task 2: `crossJoin` and the exact-key join door — built, measured, backed out

The sweep (`raw/delta1/joins-*.jsonl`, 219 cells): inner, left, right, full, semi, anti and cross, keyed by USING
and by a condition, the held frame on each side and on both, zone (both directions) and ANSI (both directions), plus
matching settings. The fix, each side's plan through the seat before `join_exact_sides` analyses the join
(`raw/delta1/task2.patch`: `frame_lineage.rs` and one `extension.rs` helper), moved 74 cells WRONG → EQUAL and 2 EQUAL
cells away from Spark:

| cell | Spark | without the fix (head) | with the fix |
|---|---|---|---|
| EQUAL → RAISE | `ansi/on_off/left_anti/cond_both` |  |  | RAISE ARITHMETIC_OVERFLOW |
| EQUAL → RAISE | `ansi/on_off/left_anti/cond_left` |  |  | RAISE ARITHMETIC_OVERFLOW |

A held ANSI-on frame on the left of a left-anti join now raises; Spark pushes the anti join below the projection
(`PushDownLeftSemiAntiJoin`) and answers empty. RePark raises the same way under matching settings
(`ansi/match/cond_left_anti`, `ansi/match/left_anti`, every build). By the back-out rule the commit was dropped before
it was made; `EXPLAIN` on matching settings was identical with and without it. The head's join cells: 13 WRONG → EQUAL
against part 1 (SQL views and CTAS over held joins, through the seat), 0 EQUAL → WRONG.

## The other callers that analyse a held plan under the live state (measured, not fixed)

| caller | cells | result on every build (pre, part1, head) |
|---|---|---|
| `repark-python/src/column/expr_build.rs:187` (`withColumn`, `select(F.expr(...))` over a held frame) | 8 | zone EQUAL; ANSI WRONG in both directions, unchanged on every build |
| `repark-python/src/dataframe/mod.rs:183` (`dtypes`) | 4 | EQUAL |
| `join_on_names` (`dataframe/mod.rs`, a USING join) | 16 | a held right side answers under the live settings, WRONG, unchanged on every build |
| `repark-spark/src/spark_ast.rs:277` (a SQL view, `spark.sql` over a view of a held join) | 8 | WRONG → EQUAL on part 2 (through the seat) |
| `repark-spark/src/ctas.rs:207` (CTAS over a held frame's view) | 5 | WRONG → EQUAL on part 2; the `on_off` CTAS raises as Spark does |
| `spark.sql(query, h=frame)` | 4 | refused on every build (keyword arguments not supported), a direct gap |

## Cards

- **TEMP-VIEW-FRAME-READER-PARSE-1 — filed, label-blocking** for ICE-V2-FLAT-AWS-LABEL-1 by the owner's ruling
  (2026-11-18, about 14:30 UTC, verbatim: "We better go with B to be safe"; B: the card blocks the label, part 2 still
  merges with the residue named, since no cell gets worse than main, and the card's own unit must land before the label
  can be declared). Severity: S1 by class (the 2026-11-17 "S1 by class" ruling; the time-zone half blocks). Spark parses
  a JSON or CSV source under the reader's settings and decides everything above the parse under the frame's. Cells: the
  reader sweep's non-EQUAL JSON and CSV lineage cells (330 of 524, `raw/delta1/readers-*.jsonl`), Fable's
  `json/derived_hour/*`, the verifier's `csv/sqlview_hour_utc/*`; `74c22dfe`'s 20 `csv_s` EQUAL cells are the shape
  the unit must reach. Its inventory is the table above; `smartCsv` is its open question.
- **Proposed, for the owner (severities proposed):** TEMP-VIEW-FRAME-CROSSJOIN-1 (the exact-key join door and
  `crossJoin` analyse both held sides under the live state; silent; S1 by class; the left-anti gap below first);
  HELD-FRAME-USING-JOIN-1 (`join_on_names` keeps only the left frame's state; silent; S1 by class);
  HELD-FRAME-DERIVED-ANSI-1 (`expr_build.rs:187`, `withColumn` and `select(F.expr(...))` over a held ANSI frame;
  S2); LEFT-ANTI-PROJECTION-PUSHDOWN-1 (a left-anti or left-semi join evaluates the left projection on every row; loud;
  S3).

## The cap

Task 1 used three of the build's seven product files (`frame_settings.rs`, `temp_settings.rs`, `temp_view.rs`).
Fable's guidance under the owner's delegation allowed an eighth, `crates/repark-python/src/frame_lineage.rs`, plus one
`extension.rs` helper, for task 2 only; it was used for the backed-out build and is not in the branch.

Up: [map.md](map.md).
