claude-opus-5-5
VERDICT: GAPS

# Critic verdict: TEMP-VIEW-FRAME-READER-PARSE-1 step 0 (`13b6d45c`)

I read and probed only. I edited nothing, made no commits, and the workspace `git status` is clean. All my probes,
logs and raw cells live under `N/reader-critic/`:
- probes: `probes/gen.py`, `probe.py`, `nest.py`, `jc.py`, `cmp.py`
- raw cells: `raw/{main,part1,spark}-{Z,ZF,X,XC,W,C,D1,A,N,J}.jsonl`
- verdicts: `verdicts.jsonl`, `logs/cmp.txt`, `logs/sample-agreement.txt`
- jar disassembly: `jar/*.javap.txt`

Every RePark run printed `repark._native.__file__` from the `pkg-main-4c365814` or `pkg-main-6286ba19` package it was
meant to use. Every Spark run used 4.1.2 in the c26 oracle.

**In one line:** the reading of Spark is right, the inventory is accurate, and the table reproduces. Seat (a) is the
right direction, but as written it has two mechanical holes and one door left out (sections 6.1 to 6.3). Step 0 must
amend the seat and add the named cells before any build.

## 1. Spark's rule, read from the jars myself (step 2)

I disassembled these classes from `spark-sql_2.13-4.1.2.jar` and `spark-catalyst_2.13-4.1.2.jar`:
`JsonFileFormat`, `CSVFileFormat`, `FileFormat`, `SessionStateHelper`, `FileSourceScanExec`, `ResolveTimeZone$` and
`ViewHelper$`.

- **The parse at scan time.** Both `JsonFileFormat.buildReader` and `CSVFileFormat.buildReader` build their options
  through `getJsonOptions` or `getCsvOptions`, then `getSqlConf(sparkSession)`.
  - `SessionStateHelper.getSqlConf` is `sparkSession.sessionState().conf()` (`jar/SessionStateHelper.javap.txt:44-50`),
    which is the session's live conf, not `SQLConf.get`.
  - From that conf the parse reads `sessionLocalTimeZone` and `columnNameOfCorruptRecord`
    (`JsonFileFormat.javap.txt:250-275`, `CSVFileFormat.javap.txt:210-227`). CSV also reads `csvColumnPruning`.
  - The parse mode and the formats come from the relation's options map.
  - `FileSourceScanExec` calls `buildReaderWithPartitionValues` lazily (`FileSourceScanExec.javap.txt:1099`).
- **Inference.** `inferSchema` runs through the same option builders at the `DataFrameReader` call.
- **Expressions above the relation.** `ResolveTimeZone$` resolves `TIME_ZONE_AWARE_EXPRESSION` once, at analysis.
- **The temp view.** `ViewHelper$.createTemporaryViewRelation` takes the stored-analysed-plan branch when
  `storeAnalyzedPlanForView` is set or when `originalText.isEmpty`. A DataFrame view has no SQL text, so it always
  takes that branch: `prepareTemporaryViewStoringAnalyzedPlan` plus `Some(prepareTemporaryViewPlan(...))`, with no
  captured confs (`ViewHelper$.javap.txt:717-822`, branch at byte 176). A SQL view takes `prepareTemporaryView`, which
  captures through `sqlConfigsToProps` and holds no plan.

**Against step 0's §2:** no differences. One addition step 0 does not state, measured below: a cached frame
materialises its parse under the zone in force at materialisation. For `CACHE TABLE` that is the creating zone. For a
lazy `.cache()`, Spark materialises at the first action, under the reading zone.

## 2. The independent search (step 3)

I searched these (grep, tests excluded):
- in `python/repark/src`: `read_json(`, `read_csv(`, `_apply_reader_schema_semantics`, `_finish_csv_infer_schema`,
  `_cast_inferred_naive_timestamps`, `load_smart_csv`
- in `crates`: `read_csv`, `read_json`, `CsvFormat`, `JsonFormat`, `ListingTable`, `register_csv`, `register_json`,
  `analyze_eagerly`, `analyze_built_plan`, `execute_and_check`, `FrameViewSeat`, `create_or_replace_temp_view_from`,
  `materialize_dataframe_as_temp_view`

I ran `test -e` on all 19 paths the inventory cites: every one exists. I read every cited line (P1–P17, E1–E11), and
each says what the inventory says, for example `reader.py:684` (the nullValue `F.when`), `reader_support.py:544` (the
`cast("string").cast("timestamp")`), `frame_settings.rs:268`/`:314`/`:346`, `temp_view.rs:177`,
`frame_lineage.rs:379`, `ctas.rs:207` and `spark_ast.rs:277`.

What I found beyond the inventory:
- **No Spark Connect door exists.** `crates/repark-connect` is a Postgres connector (`src/read/postgres.rs`). The native
  door is SQL-only (`python/repark/src/repark/__init__.py:97`). File-source SQL is refused (P16), so no seat-less
  session can build a reader frame. The inventory does not say this; it settles the "eighth file" question (§6.5).
- **The global temp-view doors are refused** (`dataframe/core.py:2503`). Not a door.
- **E5 matters more than the inventory says** (§6.1). `frame_lineage.rs:379-380` stores the **analysed** plan as the
  joined frame's plan: `analyze_built_plan` returns it and `DataFrame::new(state, plan)` keeps it.
- **E11 has a measured silent gap** (§4, `cache_lazy`). The inventory marks it "not measured".

## 3. Re-measurement on main and Spark with my own probes (step 4)

The data has two rows: `(1, '2024-01-01 03:00:00')` and `(0, '2024-06-01 23:30:00')`. The second row crosses midnight
in both directions. The Parquet control holds the same instants, plus a third row.

I generated 853 cells per engine. Of these, 594 share a key with step 0's `cells-verdicts.jsonl`. Their verdicts
agree on 587 of the 594.

| sample asked | measured | step 0 class | agree |
|---|---|---|---|
| ≥40 of 382 zone view cells | 107 | view, WRONG | 107 / 107 |
| ≥10 of 30 ANSI view cells | **30** (all) | view: 15 WRONG (silent), 15 RAISE (loud) | 30 / 30 |
| ≥24 of 136 wrong writes | 68 (CTAS, `INSERT INTO`, `writeTo`, `insertInto` × `json_s`, `json_i`, `csv_i`, `csv_s` × `eb_hour`, `sb_hour` × both directions, ANSI on; plus `csv_s` `d` in `utc_la`), rows read back under UTC | WRONG | 68 / 68 |
| D1 ≥8 | 16 (`csv_s`, `csv_f` × no view, `d`, `eb_hour` × `la_la`, `utc_la`) | WRONG | 16 / 16; `utc_utc` 8 / 8 EQUAL |
| 10 matching controls | 60 (`C` set, `D1` `utc_utc`, A matching) | EQUAL | 60 / 60 |
| direct and matching wrong cells | 97 + 22 | direct | all agree |

**The 7 disagreements** are all `CAST(ts AS DATE)` in `utc_la`:
- `Z/ansi_true/utc_la/{json_s,json_f,json_m,csv_i,csv_n}/eb_date`
- `ZF/ansi_false/utc_la/{json_s,csv_i}/sb_date`

Step 0 calls these EQUAL; I measure WRONG. Spark gives `2024-06-02` (the frame's UTC day of 06:30Z) where main gives
`2024-06-01`. The difference comes from the data, not the engine: step 0's rows never cross midnight from that side.
So step 0's 83-cell `utc_la` passes undercount the class, and a seat that got `utc_la` dates wrong would pass its bar.

## 4. The shapes step 0 missed (step 5)

| shape | cells | main against Spark | reading |
|---|---|---|---|
| `filter("hour(ts) >= 12")` on the JSON or CSV frame, then a view read under the other zone (`X/*/ffilter`) | 4 | **WRONG, silent rows lost or gained.** Spark keeps 2 rows (`la_utc`) or 0 (`utc_la`); main keeps 1. | A view gap in a filter above the parse. Step 0's `f` door uses `n > 0`, which reads no zone, so it never saw this. |
| `unionByName` of a JSON or CSV frame and a Parquet frame, `hour` above (`X/*/union_hour`, `J/*/pq/uj`) | 6 | WRONG on **both branches**: the Parquet rows get hour `23/3/4` where Spark gives `16/19/20` | The exemption takes the Parquet branch with it, like E4. |
| `createOrReplaceTempView` twice, the second time under the reading zone (`X/*/twice_hour`) | 4 + 2 `pq` | WRONG (the view gap); `pq` EQUAL | Spark keeps the frame's zone. A first Spark run had shared its `CACHE TABLE` across cells; I separated the cache cells and re-ran, and these figures are from the clean run. |
| `.cache()` with no action, then the view, read under the other zone (`XC/*/cache_lazy_hour`) | 4 | **WRONG in `ts` itself**: main parses under the **creating** zone (`2024-06-02 06:30:00` where Spark has `2024-06-01 23:30:00`), and so does part 1 | RePark materialises at registration, Spark at the first action. This is a silent S1-class parse gap at E11, the card's own class. It is idiomatic code. |
| `.cache()` plus `count()`, and `CACHE TABLE` (`XC/*/cache_mat_hour`, `cachetable_hour`) | 8 + 4 `pq` | EQUAL | Both engines parse at materialisation. Seat (a) must not move these. |
| `frame.join(pq_frame, condition)`, `crossJoin`, and a SQL view joining a JSON frame view with a Parquet frame view (`J/*/{cj,cjb,cross,sql_join}`) | 16 | WRONG (the Parquet side's `ph`, and `hour` above); **`ts` itself EQUAL today** | See §6.1: seat (a) as written would make `ts` wrong in `cj`, `cjb` and `cross`. |
| `spark.table(v)` after `createOrReplaceTempView` (`Z/*/t`, `ob`) | 44 | `ob`: the view gap as step 0 found | My `t` door applied `hour` to a column already cast to text. That is a probe artefact, and it re-measures D4 instead (`hour` of a string answers `0`, `pq` too). |
| frame views nested three deep with three different zones: create A, wrap B, read C (`N/*/{ff,fs,sf}`; zones LA, UTC and a third) | 54 | `json_s`, `csv_i`: 36 of 36 WRONG; `pq`: 18 of 18 EQUAL | Spark parses under C, the live session, and evaluates each layer's `hour` under its own A or B (`N/LA_UTC_TK/json_s/ff`: Spark `h1 10, h2 18`, main `3, 3`). Step 0 never made layers under different settings, so its two-zone table cannot tell "the live session" from "the enclosing frame". |
| `multiLine` JSON (a pretty top-level array) | 6 | RAISE: `Not valid JSON: EOF while parsing an object` | A loud direct gap, not a view door (D9 candidate). |
| a compressed CSV (`*.csv.gz` in a directory), with a schema and with `inferSchema` | 8 | RAISE: `No files found` | Loud direct gap (D10 candidate). |
| CSV with a header and `inferSchema` over two files whose types differ (`1`, `1.5`) | 4 | RAISE: `Fail to merge schema field 'n' … Int64 … Float64`; Spark gives `DOUBLE` | Loud direct gap (D11 candidate). |

**"Part 2 moved nothing here" is slightly too strong.** `J/*/{json_s,csv_i}/sql_join` changed answer between part 1
and main (4 cells, WRONG to WRONG): part 2 gives the Parquet frame view's `ph` its own answer inside a SQL join. The
claim holds for step 0's own doors.

## 5. Spellability of the `ReaderParse` marker (step 6, first bullet)

The marker is not spellable.
- It is a `LogicalPlan::Extension` node. SQL has no syntax for one.
- `F.expr` and `selectExpr` build expressions, never plan nodes (`expr_build.rs:170-215` extracts the projection
  expression from the planned fragment).
- RePark has no Spark Connect door, and the native door is SQL-only.
- The only spelling is the facade's private PyDataFrame method.

So the DECIMAL-DIVIDE-LITERAL-1 F2 hazard does not apply, provided the rule matches by node type and never by
expression shape. `_cast_inferred_naive_timestamps` emits `CAST(CAST(col AS STRING) AS TIMESTAMP)`
(`reader_support.py:544`), the very shape F2 warned about. The marker must not be inferred from it.

## 6. Seat (a): my judgement (step 6)

### 6.1 Stripping the marker in the first analyzer rule loses it from plans RePark keeps after analysis

**This is a gap in the seat as written.** `FrameSettingsRule` runs at analyzer index 0 (`df_guards.rs:173-174`),
after `ApplyFunctionRewrites`, which DataFusion always runs first (`datafusion-optimizer-54.1.0/src/analyzer/mod.rs`
`execute_and_check`). Under seat (a), every analysis would strip `ReaderParse`. But `join_exact_sides`
(`frame_lineage.rs:379-380`; the `join(other, condition)` and `crossJoin` doors, E5) keeps the **analysed** plan as
the joined frame. A JSON frame joined that way therefore has no marker by the time it is registered.

With `reader_lineage` removed, the joined frame view becomes an ordinary `FrameView`. When read under other settings it
closes under the frame's settings, and the typed parse (`to_timestamp(ts)`) is then planned under the **creating**
zone. **Cells that would move:** `J/{la_utc,utc_la}/{json_s,csv_i}/{cj,cjb,cross}`, 12 cells in my set. `ts` and `u`
are EQUAL there today and would become WRONG.

The cells are WRONG today already (the Parquet `ph`), and E5 is carded outside unit 1's bar. So the bar as written
("0 EQUAL cells lost") cannot see a parse that has newly gone wrong. Either:
- strip the marker in the first **optimizer** rule (the `StripAttributeIds` position, `df_guards.rs:228`). The analysed
  plan keeps the marker, and the optimized and `EXPLAIN` plans still equal main's. Or:
- keep a lineage fallback wherever an analysed plan is stored. `join_exact_sides` is the known one; the builder must
  inventory the others by grepping `DataFrame::new(` after `analyze_*`.

Whichever is chosen, unit 1's bar must check the `ts` and `u` columns of the E5 cells, not only the cell verdict.

### 6.2 "The config the rule is deciding under" is not always the reader's

Inside `closed_scan`, the input is analysed under the **frame's** options (`frame_settings.rs:462-467`). An inner
`FrameSettingsNode` is then decided with `config` set to the enclosing frame's settings, not the live session's. Spark
parses under the live session at every depth (N set, 36 cells).

The seat therefore needs one of two things:
- the outermost decision replaces every `ReaderParse` in the subtree, descending through inner markers' inputs, before
  any nested analysis; or
- `ReaderParseView` reads the session's `LiveOptions` (already carried by `FrameView.reader`, `frame_settings.rs:161`)
  when it is scanned, never the `config` of an enclosing analysis.

The SQL door half (`ReaderZone` grown into full settings, `temp_settings.rs:41`) is right in kind and needs the same
cells (`N/*/fs`, `sf`).

**What "the outermost reader" means:** the session that runs the query. Every layer in between keeps its own settings.

### 6.3 Writes, and a SQL view over a frame view

These do give Spark's answer once 6.1 and 6.2 hold. The 68 view writes I sampled go through the same providers. `pq`
and `emu` are EQUAL through all four write doors in both my data and step 0's.

One case to check: the `_repark_writer_v2_` view takes `with_zone_of(live)` (`frame_settings.rs:274`). That is a
nested frame view, so 6.2 applies to `writeTo`.

### 6.4 Removing `reader_lineage` and the SQL fallback

Both can go only after 6.1. Until then, the E5 cells above move. The `j`, `jb`, union and `sql_join` doors gain the
Parquet side's part 2 answer, as predicted.

`cache_lazy` (§4) moves under neither seat: the collect runs at registration.

### 6.5 The file count and the cost

**Files.** There is no eighth file for a seat-less session: no seat-less door can build a reader frame (§2). If 6.1
moves the strip into an optimizer rule installed in `df_guards.rs`, that is the eighth file. Budget it, or place the
rule through the seat's existing install path. Also note that `frame_settings.rs` is already 495 lines; check the
file-size ratchet before adding `ReaderParseView` there.

**Cost.** Every JSON or CSV query now carries the marker, so `FrameSettingsRule` does a full
`transform_down_with_subqueries` on every analysis of every JSON or CSV plan, views or not. A single plan walk is no
longer enough. A 1.00–1.01x cost is plausible, but K1–K4 all read through a view. **Add K0:** a plain JSON or CSV read
with no view (`count`, plus `max(hour(ts))` with a filter), held at 1.05x.

## 7. The split and the questions (step 7)

**Unit 0 (D1, `reader.py` only) before unit 1 is right.** I reproduced D1 in 16 of 16 cells, views or not, while
`utc_utc` stays EQUAL. It is silent and moves matching cells, so it cannot share unit 1's bar. Carding seat (b) is
right: D2 is loud, and I measured 42 A-set RAISE cells, all the parse.

**The defaults that rest on measurements all hold:**
- Q1 rests on `emu` and `pq`, which are EQUAL in every set.
- Q2 rests on the D1 counts, which I reproduced.
- Q4 rests on `pq` 34 of 34; mine is 18 of 18 across the ANSI passes.
- Q6 rests on D4, which I reproduced in the `C` and `t` doors.

**Missing owner questions:**
1. **`.cache()` with no action, then a view** (`cache_lazy`, §4). This is silent, the parse itself is wrong, and it
   is idiomatic. Options: (i) fold it into unit 1 by materialising lazily, at the first scan; (ii) give it its own S1
   card, label-blocking because it is the zone half; (iii) disclose it. Default (ii). Cells: `XC/*/cache_lazy_hour`,
   4 of 4 WRONG on main and part 1; `cache_mat` and `cachetable` 8 of 8 EQUAL.
2. **E5 under seat (a)** (§6.1). Must unit 1 keep the E5 parse at main's answer? Default: yes, with the strip in the
   optimizer, and E5's `ph` left to TEMP-VIEW-FRAME-CROSSJOIN-1. Cells: `J/*/{cj,cjb,cross}`, 12.
3. **D9–D11** (`multiLine` JSON array, `*.csv.gz`, multi-file `inferSchema` merge). All loud. Default: card them S3,
   not label-blocking. Cells: 18 `X` cells.

## 8. What I require before the build

1. Amend seat (a) for §6.1 (where the strip happens, and the E5 inventory) and §6.2 (the parse planned under the live
   session at every depth). Then restate the file cap.
2. Add these cells to `domain-table.md` and to unit 1's bar, measured on main and Spark:
   - data that crosses midnight in both directions (the 7 `utc_la` date cells);
   - the zone-sensitive frame `filter` (4);
   - JSON or CSV unioned with Parquet (6);
   - three-zone nesting `ff`, `fs`, `sf` over `json_s`, `csv_i` and `pq` (54);
   - E5 `cj`, `cjb`, `cross`, with `ts` and `u` checked per column (12);
   - the cache shapes, with `cache_mat` and `cachetable` held EQUAL and `cache_lazy` named (12);
   - `createOrReplaceTempView` twice (6).
3. Add a K0 cost case (§6.5).
4. Add the three questions in §7 to the consult file, or record why they are not owner questions.
5. Narrow "0 lineage cells moved" to step 0's doors, and note `J/*/sql_join`.
