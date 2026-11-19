# TEMP-VIEW-FRAME-READER-PARSE-1 — the insertion inventory (owner rule of 2026-11-09)

Every place a JSON or CSV reader's parse expressions are built, and every place such a plan enters a view, a write or a
join, on main `4c365814`. "Seen at the seat" means: does part 2's seat (`frame_settings.rs`, `temp_settings.rs`,
`temp_view.rs`) see this plan, and does it today keep the reader lineage at part 1's answer (`reader_lineage`). The
cells column cites [domain-table.md](domain-table.md).

**How I searched.** `grep -rn` over `crates/` (`*.rs`, tests excluded) for `read_json`, `read_csv`, `JsonFormat`,
`CsvFormat`, `JsonReadOptions`, `CsvReadOptions`, `register_csv`, `register_json`, `NDJSON` (case-insensitive); over
`python/repark/src` for `.read_json(`, `.read_csv(`, `_load_json`, `_load_csv`, `_apply_reader_schema_semantics`,
`load_smart_csv`, `_finish_csv_infer_schema`, `_promote_csv_string_types`, `_cast_inferred_naive_timestamps`; for the
entries, `reader_lineage`, `frame_plan(`, `opened(`, `inlines_settings_reading_frame`, `_repark_writer_`,
`_repark_writer_v2_`, `join_exact_sides`, `join_on_names`, `analyze_eagerly`, `register_collected_memtable`; and the
SQL door for a file source (`USING csv`, `USING json`, `` json.`path` ``), which has no product hit (measured below).
Tests and `map.md` files are not doors.

## 1. Where the parse is built

| # | site (file:line) | what it builds | settings it reads, and when | seen at the seat |
|---|---|---|---|---|
| P1 | `python/repark/src/repark/spark/session/reader.py:274` `json()`, `:413` `load()` → `:461`, → `:512` `_load_json` → `:518` `session.read_json` | the scan, then `_apply_reader_schema_semantics` (`:537`) | none at the call | yes: the scan is found by `reader_lineage` (`frame_settings.rs:314`, `reader_scan` `:346`) |
| P2 | `reader.py:65` `csv()` → `:118`, `load()` → `:457`, → `:494` `_load_csv` → `:505` `session.read_csv` | the scan, then `:505` `_apply_reader_schema_semantics` | none at the call | yes, as P1 |
| P3 | `reader.py:662` `_apply_reader_schema_semantics`, `:684` | `CASE WHEN CAST(col AS STRING) = token THEN NULL ELSE col END` (`nullValue`) | none (a string compare) | yes, as part of the lineage; the expression itself is settings-free |
| P4 | `reader.py:704`, `:719`, `:727` | `CAST(col AS <schema type>)` (`to_timestamp(col)` for a JSON string, `CAST(col AS Timestamp(µs, "UTC"))` for a CSV naive timestamp; `evidence/raw/main-E.jsonl`) | zone and ANSI at analysis and physical planning of the **query** that reads it, never at the call; the target type is fixed at the call (T cells, [domain-table.md](domain-table.md) §4) | yes, and indistinguishable from a user's cast: the reason for this card |
| P5 | `reader.py:722` | `CAST(NULL AS <type>)` for a schema field the file lacks | none | as P4 |
| P6 | `reader.py:747`, `:752` | `_cN` renames | none | as P4 |
| P7 | `reader.py:757` | `CAST(col AS STRING)` (`inferSchema` false, no schema) | none for a string; the naive-timestamp-to-string cast reads none in the measured cells | as P4 |
| P8 | `reader.py:738` → `reader_support.py:389` `_finish_csv_infer_schema`; `:427` re-read with `utf8_columns`; `:403`, `:411`, `:436` → `:447` `_promote_csv_string_types` | the promotion: `try_cast` probes `:495` and `:487` (run **once, at the call**, under the creation state), then `CAST(col AS <promoted>)` `:524`; `:415`, `:440` string casts for all-null columns | the probes: the creation zone, ANSI and timestamp type, at the call; the casts: as P4 | as P4; the probes' decision is creation-time, as Spark's `inferSchema` is (Spark infers at `DataFrameReader` time) |
| P9 | `reader.py:761` → `reader_support.py:531` `_cast_inferred_naive_timestamps`, `:544` | `CAST(CAST(col AS STRING) AS TIMESTAMP)` for an inferred naive timestamp | as P4 | as P4 |
| P10 | `reader.py:120` `smartCsv` → `:152`, `:261` → `python/repark/src/repark/spark/_csv_smart.py:774` `load_smart_csv`: the file read in Python, `createDataFrame` of strings `:858`, `selectExpr(CAST(CASE … END AS <type>))` `:873` | the typed casts over an in-memory table | as P4 | **no**: its leaf is a `__repark_cdf_*` memory table, not a file scan; ruled accepted (2026-11-18, "Defaults on all three"); a SQL view over its frame view raises `TABLE_OR_VIEW_NOT_FOUND` on main and part 1 (domain table §7, D6) |
| P11 | `python/repark/src/repark/spark/session/session_core.py:903` `read_csv`, `:913` `read_json` | the engine entry the facade calls | none | as P1, P2 |
| P12 | `crates/repark-python/src/session.rs:237` `read_csv`, `:256` `read_json` | the binding | none | as P1, P2 |
| P13 | `crates/repark-core/src/session.rs:797` `read_json` → `SessionContext::read_json` (`:810`) | a `ListingTable` over `JsonFormat`; DataFusion's inference (strings stay `Utf8`, integers `Int64`) | none | yes, the `reader_scan` target |
| P14 | `crates/repark-core/src/read_options.rs:212` `read_csv_path` → `:234`, `:243` | a `ListingTable` over `CsvFormat`; DataFusion's inference (a timestamp text becomes a naive timestamp, which P4's cast reads as UTC: D1); `:225` the all-`Utf8` schema when `nullValue`, `utf8_columns` or `multiLine` is set | none | yes, as P13 |
| P15 | `crates/repark-core/src/session/path_write/append.rs:203`, `:241` | a read of an existing CSV or JSON **destination** during an append | none | not a door: a write-side schema check, never a user frame |
| P16 | the SQL file-source doors (`` SELECT * FROM json.`path` ``, `CREATE TEMPORARY VIEW … USING json/csv OPTIONS (path …)`) | none on main | — | not a door on main: both refuse (domain table §6, `evidence/raw/main-S.jsonl`) |
| P17 | `from_json`, `from_csv`, `to_timestamp` written by a user | a user's own expression | as P4 | yes, and correctly decided under the frame's settings by part 2; not a reader parse |

## 2. Where such a plan enters a view, a write or a join

| # | entry (file:line) | path | what part 2 does with a JSON or CSV lineage | seen at the seat |
|---|---|---|---|---|
| E1 | `createOrReplaceTempView` → `crates/repark-core/src/session/temp_views.rs:45` → `:66` `seat.wrap` → `crates/repark-spark/src/view_ddl/frame_settings.rs:260` `frame_view` | the frame door | `:268` `reader_lineage` → `into_view` with any inner marker unwrapped (`unmarked`): part 1's plan, read under the live settings | yes; exempted |
| E2 | `CREATE TEMP VIEW … AS SELECT … FROM <frame view>` → `crates/repark-spark/src/view_ddl/temp_view.rs:177` | the SQL door | `reader_lineage(&body) && inlines_settings_reading_frame(…)` (`temp_settings.rs:180`) → `Ok(None)`: part 1's inlined-frame fallback | yes; exempted |
| E3 | a frame over a frame view or a SQL view (`s.table(v).filter(…)`) | a `TableScan` of the view's provider inside a new frame, registered again through E1 | the walk follows `definition_plan` and `get_logical_plan` through `FrameView` and `ReplanningTempView` (`frame_settings.rs:331`) and finds the scan | yes; exempted |
| E4 | `frame.join(<frame view>, "n")` → `crates/repark-python/src/dataframe/mod.rs:571` `join_on_names` | a USING join | the joined frame's lineage reaches the JSON scan, so the **whole** joined frame, Parquet side included, takes E1's exemption; the Parquet frame view inside loses its part 2 answer (`ph` 3 where Spark answers 19, domain table §2.2) | yes; exempted, wider than the parse |
| E5 | `frame.join(other, condition)` and `crossJoin` → `crates/repark-python/src/frame_lineage.rs:338` `join_exact_sides` (`:379` analyses under the live state) | the exact-key door | as E4, plus TEMP-VIEW-FRAME-CROSSJOIN-1 (S1, carded) | no: analysed under the live state before any seat; carded |
| E6 | CTAS `CREATE TABLE … AS SELECT * FROM <view>` → `crates/repark-spark/src/ctas.rs:207` `analyze_eagerly` | a write | the view's provider is planned by E1/E2's choice: the lineage writes part 1's answer | yes, through the provider |
| E7 | `INSERT INTO … SELECT * FROM <view>` → `crates/repark-spark/src/spark_ast.rs:277` `analyze_eagerly` | a write | as E6 | yes, through the provider |
| E8 | `writeTo(…)` → `python/repark/src/repark/spark/dataframe/writer_readwriter.py:954` → `writer_layout.py:95` `run_through_temp_view` with `_repark_writer_v2_` → E1 → `frame_settings.rs:274` `writer_v2` (`with_zone_of(live)`) | a write through a writer view | the writer view wraps `s.table(<view>)`, whose lineage reaches the scan: exempted at E1 before `writer_v2` is read | yes; exempted |
| E9 | `write.insertInto(…)` → `writer_readwriter.py:215` → `:722` `run_through_temp_view` with `_repark_writer_` → E1 → E7 | a write through a writer view | as E8 | yes; exempted |
| E10 | `select(F.expr(…))`, `withColumn` → `crates/repark-python/src/column/expr_build.rs:187` | a held frame analysed under the live state | outside the view seat (HELD-FRAME-DERIVED-ANSI-1, S2, carded) | no; carded |
| E11 | `cache()`, `persist()` → `temp_views.rs:173` `register_collected_memtable` | materialisation | the frame is collected at its first action under that action's settings | not this card's door (part 2's seat 3; not measured here) |

## 3. Revision (critic round 1): entries the first pass missed or left unmeasured

The critic (Opus 5.5 medium, 2026-11-19, `VERDICT: GAPS`) found these; each is now measured on main against Spark 4.1.2
([domain-table.md](domain-table.md) §8, data crossing midnight both ways).

| # | entry (file:line) | path | main against Spark, per column | seen at the seat |
|---|---|---|---|---|
| E5 (re-read) | `frame.join(other, condition)`, `crossJoin` → `crates/repark-python/src/frame_lineage.rs:379` `analyze_built_plan`, `:380` `DataFrame::new(state, plan)` | the joined frame **is the analysed plan** | `cj`, `cjb`, `cross` 12 cells: `ts` and `u` EQUAL, `ph` and `b` WRONG | yes (exempted); any marker stripped during analysis is gone from this frame, so seat (a) must keep its marker through analysis (sketch §4) |
| E11 (re-read) | `cache()`, `CACHE TABLE` → `crates/repark-core/src/session/temp_views.rs:173` `register_collected_memtable` (`:184` analyse, `:189` optimise, `:193` plan, under the frame's own state) | materialisation | `cache_mat`, `cachetable` 8 EQUAL; `cache_lazy` 4 WRONG in `ts`, `u` and `b` (main and part 1): RePark materialises when the view is registered, Spark at the first action | the parse is planned at materialisation, so no seat that plans it later moves `cache_lazy` (sketch §7 Q8) |
| E12 | a held frame acted on after the settings change (`nv`, `nv_hour`) | the frame's action | `nv` 4 EQUAL; `nv_hour` 4 WRONG in `b` (Parquet gained under part 2) | yes; the JSON and CSV lineage takes the exemption |
| E13 | `unionByName` with a Parquet frame → `python/repark/src/repark/spark/dataframe/core.py:2596` | a union | 4 WRONG in `b` on **both** branches; `pq` 2 EQUAL | yes; exempted, wider than the parse (as E4) |
| E14 | `createOrReplaceTempView` of one frame twice, the second time under the reading settings (E1 twice) | re-registration | 4 WRONG in `b`; `pq` 2 EQUAL (Spark keeps the frame's settings) | yes; exempted |
| E15 | a SQL view joining a JSON frame view with a Parquet frame view → `crates/repark-spark/src/view_ddl/temp_view.rs:177` | the SQL door | 4 WRONG in `b`; `ph` EQUAL on main, WRONG on part 1 (part 2 moved these 4 from one WRONG to another) | yes; the SQL fallback |
| E16 | a zone-reading `filter` above the parse (`filter("hour(ts) >= 12")`) → E1 | the frame door | 4 WRONG by rows: Spark keeps 2 rows (`la_utc`) or 0 (`utc_la`), main 1 | yes; exempted |
| E17 | views three deep under three zones (`ff`, `fs`, `sf`) → E1, E2, E3 at each layer | nesting | `json_s`, `csv_i` 36 WRONG in `h2` (36) and `h1` (24), `ts` and `u` EQUAL; `pq` 18 EQUAL | yes; the inner layer is decided with `config` set to the enclosing layer's settings (`frame_settings.rs:462-467`) |

**Not doors** (the critic's search, confirmed): no Spark Connect door (`crates/repark-connect` is a Postgres
connector); the native door is SQL-only (`python/repark/src/repark/__init__.py:97`) and refuses file-source SQL (P16);
the global temp-view doors are refused (`python/repark/src/repark/spark/dataframe/core.py:2503`). So no session
without the frame seat can build a reader frame.

## 4. Where an analysed plan is kept or read (seat (a)'s marker now survives analysis)

**How I searched.** `grep -rn` over `crates/` (tests excluded) for `execute_and_check`, `analyze_eagerly(` and
`analyze_built_plan(`, then each caller read for what it does with the analysed plan; `DataFrame::new(` (39 sites) and
`execute_logical_plan(` checked for one fed by an analysed plan (two: E5 and CTAS); `LogicalPlan::Extension` for the
hand-written walkers that stop at an extension node. The column says what a `ReaderParse` node inside the analysed plan
does there.

| # | site (file:line) | what it does with the analysed plan | with the marker inside | action |
|---|---|---|---|---|
| A1 | `crates/repark-python/src/frame_lineage.rs:379`, `:380` | **keeps it** as the joined frame (E5) | kept; decided when the frame is planned | none beyond the seat; the bar checks `ts` and `u` per column |
| A2 | `crates/repark-spark/src/view_ddl/frame_settings.rs:467`, `:468` | keeps it inside `FrameView::closed` | kept; decided by the optimizer inside the closed view's `plan_under_overlay` | none |
| A3 | `crates/repark-spark/src/view_ddl/temp_settings.rs:304` | `plan_under_overlay`: analyses, then optimises (`:337`) and plans (`:340`) under the overlay | decided under the overlay's settings against the live session's | none |
| A4 | `crates/repark-core/src/session/temp_views.rs:184` | cache materialisation: optimises (`:189`) and plans (`:193`) at once | decided at materialisation (E11) | none |
| A5 | `crates/repark-spark/src/ctas.rs:207`, `:208` | CTAS: `execute_logical_plan(analysed)`, run through the optimizer | decided when the write runs | none |
| A6 | `crates/repark-spark/src/spark_ast.rs:277` | `INSERT INTO`: analyses, conforms (`conform_insert_narrowed_ints`), fills defaults, then runs | kept through generic rewrites; decided when it runs | the write sweep |
| A7 | `crates/repark-python/src/column/expr_build.rs:187` | `F.expr`: takes the projection **expression** out of the analysed fragment (`:170-215`) | never reaches an expression | none |
| A8 | `crates/repark-python/src/dataframe/mod.rs:183`; `crates/repark-core/src/pre_execute.rs:99`; `crates/repark-spark/src/void_type.rs:285`; `crates/repark-iceberg/src/write/void_store.rs:30`, `conform_stage.rs:274`, `ntz_store.rs:121`, `store_overflow.rs:60` (callers `merge/insert.rs:123`, `:203`, `predicate_dml/lineage.rs:97`) | the schema or the column types only | the node's schema is its input's | none |
| A9 | `crates/repark-core/src/plan_introspect.rs:167`, `:193` (`sameSemantics`, `semanticHash`) | hashes the analysed plan through `write_plan` (`:315`), whose catch-all hashes any other node as `Opaque` plus its `Debug` (`:444`) | **changes the hash**: the `Debug` carries attribute ids, so two reads of one file would stop being the same | `write_plan` hashes a `ReaderParse` as its input: a product file (sketch §4) |
| A10 | `crates/repark-core/src/session/df_guards/attr_id.rs:642` | a hand-written walk that returns an `Extension` untouched, never descending | ids under the node would not be stripped | the decision runs before `repark_strip_attribute_ids` (optimizer index 0) |
| A11 | `crates/repark-python/src/dataframe/mod.rs:147` `strip_for_execution` (`attr_id.rs:383`) | `transform_up_with_subqueries` over the unanalysed plan | sees through the node (its input is a child); the private unanalysed copy is planned separately, by its own optimizer | none |
| A12 | `crates/repark-core/src/session/df_guards/join_exact.rs:23` `join_exact_sides`, called at `frame_lineage.rs:374` | rewrites both sides' **unanalysed** plans | a side may now be topped by the node | the reader sweep's join cells |

Measured beside it: `sameSemantics` and `semanticHash` agree with Spark today in 15 of 15 checks (two reads of one
file, `select("*")`, equal and unequal filters, for `json_s`, `csv_i` and `pq`; domain table §8.6).

Up: [map.md](map.md).
