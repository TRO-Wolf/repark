# TEMP-VIEW-FRAME-READER-PARSE-1 — step 0 sketch (measured, no product code)

Card: TEMP-VIEW-FRAME-READER-PARSE-1, S1 by class, label-blocking for ICE-V2-FLAT-AWS-LABEL-1 (owner, 2026-11-18, about
14:30 UTC: "We better go with B to be safe"); `smartCsv` keeps the creation zone (owner, about 15:00 UTC: "Defaults on
all three"). Measured on main `4c365814` and on `6286ba19` (part 1 only) against Spark 4.1.2. The table:
[domain-table.md](domain-table.md); the insertion inventory: [inventory.md](inventory.md); the cells:
[evidence/](evidence/).

## 1. Findings in short

- **Spark parses a JSON or CSV file at scan time under the reading session's settings, and binds every expression
  above the relation when the frame is analysed** (§2, read from the jars and measured).
- **RePark's JSON and CSV lineage is wrong only above the parse.** `ts` itself answers as Spark in every bare door,
  both directions, both ANSI modes. Every zone-reading expression above it (`hour`, `CAST(ts AS STRING)`,
  `CAST(ts AS DATE)`, `date_format`) is decided under the reader's zone where Spark uses the frame's: **382 view cells**
  in the zone key, through every door (frame, `filter`, `selectExpr`, frame over frame, SQL view, frame over SQL view,
  join), plus **30** in the ANSI key (`n * 2000000000` above the parse; silent when made with ANSI on and read with it
  off) and **128 wrong writes** (CTAS, `INSERT INTO`, `writeTo`, `insertInto`, each from a frame view and a SQL view
  carrying `hour`). `spark.sql.timestampType` has **no** view gap: both engines fix the type when the frame is made.
- **Part 2 moved nothing in step 0's doors:** 0 of the lineage's cells there differ between main and part 1 (5,732
  cells per build). It did move 4 cells of a door step 0 lacked, from one WRONG to another: a SQL view joining a JSON
  frame view with a Parquet frame view (`sql_join`, §9).
- **The fix is predictable without a build.** A Parquet file holding the instants Spark's scan parses (`emu`), put
  through the same frames, views, joins and writes on main, answers as Spark's JSON cells in **128 of 128** zone cells
  and **48 of 48** writes: once the parse is decided under the reader's settings and nothing else is, part 2's seat
  already gives Spark's answer above it.
- **A direct gap the inventory missed (D1, silent):** `schema(…).csv` and `format("csv").schema(…)` parse a
  `TIMESTAMP` field as UTC whatever the session zone, views or not: 64 of 128 matching cells wrong, every write from
  Los Angeles wrong. Keeping the column as text and casting it, as the JSON reader does (`csv_fix`, through the existing
  `utf8_columns` option), answers **128 of 128** matching cells as Spark. Seven more direct gaps are listed in the
  domain table §7.
- **Recommended:** seat (a) (mark the reader's parse; close the frame under its own settings but plan the marked
  parse under the reader's), with D1 as its own small unit first; seat (b) (a typed, PERMISSIVE parse inside a Rust
  scan) carded as the Rust-first follow-up. Eleven owner questions (§7). Seat (a) was amended after the critic's round
  (§4, §9): the marker survives analysis and is decided in the first optimizer rule, under the live session at every
  depth.

## 2. Spark's rule, read from the 4.1.2 jars and measured

Excerpts: [evidence/jar/javap-file-source.txt](evidence/jar/). The view-capture side was read by part 1's step 0
(`CapturesConfig.sqlConfigsToProps`, `View$.effectiveSQLConf`, `Analyzer$.retainResolutionConfigsForAnalysis`:
[part 1's sketch §2](../temp-view-session-settings-1/sketch.md)).

| when | what is read | class and method |
|---|---|---|
| the `DataFrameReader` call (`resolveRelation`) | the user schema's types (`TIMESTAMP` is LTZ or NTZ by the creating `spark.sql.timestampType`); with `inferSchema`, the inference, under the creating session's zone, timestamp type and formats | `JsonFileFormat.inferSchema` / `CSVFileFormat.inferSchema` → `getJsonOptions` / `getCsvOptions` → `getSqlConf(sparkSession)`; the relation keeps only the user's options map (`HadoopFsRelation.options`) |
| analysis of the frame | every time-zone-aware expression above the relation takes the creating zone; the ANSI mode of `+`, `*` and casts is fixed when the expression is built | `ResolveTimeZone$$anonfun$1` (`timeZoneId` empty → `withTimeZone(conf.sessionLocalTimeZone)`) |
| the temp view | a DataFrame view stores the analysed plan and no settings; a SQL view captures settings and re-analyses its text under them | `ViewHelper$.createTemporaryViewRelation` → `prepareTemporaryViewStoringAnalyzedPlan` (no SQL text) or `prepareTemporaryView` → `sqlConfigsToProps` |
| scan time (the scan's RDD is built lazily, at execution) | the parse zone (`sessionLocalTimeZone`, unless the `timeZone` option is set), `columnNameOfCorruptRecord` (unless the option is set), `csvColumnPruning` (CSV); formats and the parse mode from the options | `FileSourceScanExec.inputRDD$lzycompute` → `FileFormat.buildReaderWithPartitionValues(relation.sparkSession, …, relation.options, …)` → `JsonFileFormat.buildReader` / `CSVFileFormat.buildReader` → `getJsonOptions` / `getCsvOptions` → `SessionStateHelper.getSqlConf` = `sparkSession.sessionState().conf()`: the session's **live** conf, never a view's captured conf (that scopes analysis only) |
| never | ANSI: a value that does not parse is a bad record, `NULL` under PERMISSIVE in both ANSI modes | (`A` cells: Spark answers `NULL` in all four pairings) |

**Measured** (domain table §2): made under Los Angeles and read under UTC, `json_s` and `csv_s` give
`unix_timestamp(ts)` `1704078000` (the text parsed under the reader's UTC) and `hour(ts)` `19` (the frame's zone);
read the other way, `1704106800` and `3`. `current_timezone()` follows the reader (part 1's `ComputeCurrentTime`).
`option("timeZone", "UTC")` pins the parse in Spark (RePark refuses the option).

## 3. RePark on main

The facade builds the typed parse as an ordinary projection over a string scan (`reader.py:662`,
`_apply_reader_schema_semantics`). `EXPLAIN` on main (`evidence/raw/main-E.jsonl`), the same under matching and
differing settings because the lineage is exempted:

```
Projection: CAST(?table?.n AS Int32) AS n, to_timestamp(?table?.ts) AS ts
  Filter: __repark_cast_shield__(CAST(?table?.n AS Int32)) > Int32(0)
    TableScan: ?table? projection=[n, ts], partial_filters=[__repark_cast_shield__(CAST(?table?.n AS Int32)) > Int32(0)]
```

For CSV with a schema: `CAST(?table?.column_2 AS Timestamp(µs, "UTC"))` over DataFusion's inferred naive timestamp,
which is D1. Part 2's `reader_lineage` (`frame_settings.rs:268`, `:314`) sends any frame whose plan reaches a JSON or CSV
`ListingTable` to `into_view` (part 1's plan, all under the reader's settings), and the SQL door to part 1's fallback
(`temp_view.rs:177`).

## 4. Seats

The `emu` reader measures the target for any seat that decides the parse under the reader's zone and leaves the rest
to part 2: 128 of 128 zone cells, 48 of 48 writes. The `pq` reader measures part 2's machinery for the ANSI half: 34 of
34 (the `n * 2000000000` body answers Spark's under both directions).

### Seat (a), amended — mark the reader's parse; decide it in the first optimizer rule, under the live session (recommended)

Amended after the critic's round (§9): the first sketch stripped the marker in the first analyzer rule and planned the
parse under "the config the rule is deciding under"; both are measured wrong below.

- **The node.** `ReaderParse` (one input, no expressions, its input's schema), defined beside `FrameViewSeat` in
  `repark-core/src/extension.rs`. It holds the parse plan twice, as its input (analysed like any child) and as a private
  unanalysed copy, and the session's `LiveOptions` (the handle `FrameView.reader` already carries,
  `frame_settings.rs:161`). The facade wraps the frame `_apply_reader_schema_semantics` returns through one private
  native method, only when the session carries the frame seat. It cannot be spelled: SQL has no syntax for a plan node,
  `F.expr` and `selectExpr` build expressions only, and there is no Spark Connect door (inventory §3). The rule matches
  the node by type, never by expression shape: `_cast_inferred_naive_timestamps` emits
  `CAST(CAST(col AS STRING) AS TIMESTAMP)`, the very shape the DECIMAL-DIVIDE-LITERAL-1 verifier's F2 warned about.
- **Analysis leaves it in place.** `FrameSettingsRule` (analyzer index 0) no longer touches the reader lineage, so
  every analysed plan RePark keeps carries the node: the exact-key join's frame (E5, `frame_lineage.rs:379-380`) and
  `FrameView::closed` (`frame_settings.rs:468`); inventory §4 lists every seat that keeps or reads an analysed plan.
- **The decision is the first optimizer rule**, `repark_reader_parse`, put before `repark_strip_attribute_ids`
  (`df_guards.rs:228`) through the frame seat, as `FrameSettingsRule` is at `:173`. For each node it reads the live
  session's settings (`LiveOptions::read`) and compares them with the settings the plan is being optimised under
  (`TempViewSettings::same_as`):
  - **the same:** the node becomes its analysed input, and the optimizer's second rule on sees main's analysed plan;
  - **different:** the node becomes a closed scan of its unanalysed copy under the live settings (`FrameView::closed`
    with the live session's `TempViewSettings` and `closed_scan`'s aliasing projection, `:476-491`). The closed view's
    `plan_under_overlay` analyses, optimises and plans the parse under the live settings; a filter above it keeps its
    settings-sensitive conjuncts above, under the enclosing settings (`temp_settings.rs:307-311`).
- **Why the optimizer, and why first** (DataFusion 54.1.0, read from the registry copy):
  `SessionState::optimize` runs the analyzer, then the optimizer (`datafusion-54.1.0/src/execution/session_state.rs:666-739`); the
  analyzer runs `ApplyFunctionRewrites` and then the session's rules in order (`datafusion-optimizer-54.1.0/src/analyzer/mod.rs:118-167`),
  the seat's at index 0; the optimizer runs its rule list in order on every pass, recursing with `rewrite_with_subqueries`
  (`optimizer.rs:388-405`). A plan analysed now may be read later under other settings (E5), so the decision is final
  only when the plan is about to run. It must come before `push_down_filter` and `optimize_projections` (they stop at an
  unknown node: under matching settings the filter must reach the scan, as on main; under differing settings it must
  not cross the parse) and before `repark_strip_attribute_ids` (`attr_id.rs:642` returns an extension untouched).
  `datafusion.optimizer.max_passes = 0` and `skip_failed_rules = true` are refused (`df_guards.rs:56-59`, `:119-121`),
  so the rule always runs. Two physical-planner calls exist outside a `DataFrame` (`temp_settings.rs:340`,
  `temp_views.rs:193`), each after the optimizer; a node that escaped would fail loud ("no installed planner").
- **Why the live session at every depth.** Spark's file source reads `sparkSession.sessionState().conf()` when the scan
  is built (§2), whatever view or captured conf encloses it. `LiveOptions::read` returns that session's current conf at
  planning time, so a layer decided under its enclosing layer's settings still parses under the session running the
  query (the critic's §6.2). The SQL door needs no growth of `ReaderZone` for the parse; `ReaderZone` keeps serving
  `current_timezone()`.
- **Why the unanalysed copy.** Analysis under an enclosing frame's settings may bind ANSI or zone decisions into the
  parse's casts. Planning the copy whole under the live settings reproduces main's parse exactly (main plans the whole
  lineage under the reader's live settings), so D2 stays as on main.
- **`reader_lineage`, `reader_scan`, `unmarked`** and the SQL door's fallback (`temp_view.rs:177`, with
  `inlines_settings_reading_frame` and its helpers, `temp_settings.rs:180-262`) are removed.
- **Files (cap 9):** (1) `reader.py`; (2) one file under `crates/repark-python/src/dataframe/` (the private method);
  (3) `repark-core/src/extension.rs` (the node, `LiveOptions` from a context, the seat's optimizer-rule slot);
  (4) `repark-core/src/session/df_guards.rs` (the eighth file the first sketch named: insert the seat's optimizer rule
  at index 0); (5) `repark-core/src/plan_introspect.rs` (new: `write_plan` hashes the node as its input, inventory A9);
  (6) `frame_settings.rs` (the rule and the decision, minus the lineage walk: about 495 → 530 lines, under the
  1,000-line default of `scripts/check_rust_file_size.py`); (7) `temp_settings.rs` and (8) `temp_view.rs` (removals);
  (9) `reader_support.py` only if a promotion path returns a frame past the wrap. No session without the seat can make a
  reader frame (inventory §3), so no other install path is needed.
- **Cells it moves, predicted without a build.** The emulation reader parsed under the live session (`emu`, `emu_r`,
  `emu_live`) answers as Spark's JSON cells through every door: the first sketch's 128 zone cells and 48 writes, and
  in this round 18 of 18 new-door cells, 18 of 18 three-zone nested cells and 128 of 128 midnight zone cells
  (domain table §8). The same file parsed where the first sketch would have parsed it (`emu_c`, the creating zone, for
  a marker stripped in analysis; `emu_mid`, the enclosing layer, for the inner decision) is WRONG in `ts` and `u` in
  every such cell. Unchanged: D1 (unless unit 0 lands first), D2, D9–D11, `smartCsv`, `cache_lazy` (Q8).
- **`EXPLAIN` under matching settings.** Plain `EXPLAIN` and every `df.explain` mode (`EXPLAIN` for simple, extended
  and codegen, `EXPLAIN FORMAT TREE`, `EXPLAIN ANALYZE`: `dataframe/explain.py:8-14`) print the optimised logical plan
  and the physical plan only; DataFusion keeps the stage plans for `VERBOSE` (`session_state.rs:667-731`). The first
  optimizer rule returns main's analysed plan, so both are main's, provided every analyzer rule treats the node as a
  transparent child: the builder diffs `EXPLAIN` over the reader sweep. `EXPLAIN VERBOSE`, a DataFusion spelling that
  Spark refuses, prints each analyzer stage (the node shows there) and one more optimizer line: Q11. `EXPLAIN EXTENDED`
  is refused on main by the parser, unchanged.
- **Cost.** Matching path: one `exists` walk per optimizer pass for every plan (as `holds_marker` does) and one
  `LiveOptions::read` per node; K0 holds it at 1.05x. The differing path plans one more closed view per parse
  (measured, not held).
- **Sweep risk: medium.** Every JSON and CSV frame carries the node until its plan is optimised: the reader sweep
  (1,328 cells) and the write sweep (3,100) re-run in full, with the seats of inventory §4 (A6, A9–A12) and the
  analyzer rules after index 0 checked over a plan with the node. Side effect to measure: a frame cached after a
  settings change parses under the live settings at materialisation (Spark's rule; no cell here).

### Seat (b) — a typed parse inside a Rust scan (Rust-first; carded)

- **Mechanism.** A `ReaderParseTable` provider in `repark-core` wraps the string `ListingTable` and the target
  schema; its `scan` plans the inner scan and a parse projection built with the reader's zone (`ReaderZone` when an
  overlay is active, else the state's zone), PERMISSIVE: a value that does not parse is `NULL`, as Spark's parser. The
  frame above it is then a frame that only scans, or ordinary.
- **`reader_lineage`:** removed, as in (a).
- **Files: nine** (`reader.py`, `reader_support.py`, `repark-python/src/session.rs`, `repark-core/src/session.rs`,
  `read_options.rs`, a new `repark-core/src/reader_parse.rs`, `frame_settings.rs`, `temp_settings.rs`,
  `temp_view.rs`): over a seven-file cap, so two units.
- **Moves under matching settings:** D2 (every malformed or overflowing value: RAISE → Spark's `NULL`) and D1; the
  logical `EXPLAIN` loses the parse `Projection` line (the physical plan can keep its shape), so `EXPLAIN` under
  matching settings is **not** main's.
- **Cost risk: high.** Main pushes the filter into the JSON scan (`partial_filters` above); the provider must rewrite
  pushed filters to the raw columns and prune by column, or the cost cases regress.
- **Sweep risk: high.** Every JSON and CSV read changes provider (`nullValue`, `inferSchema`, multi-line,
  compression, the S3 prefix path, the secret-column flag).

### Seat (c) — (a) plus the CSV text parse, considered and split out

`csv_fix` shows that passing `utf8_columns` for a schema's `TIMESTAMP` fields (`read_options.rs:240` already honours
it) makes the CSV parse the JSON reader's `to_timestamp` over text: 128 of 128 matching cells EQUAL, and its cross
cells then match `json_s`'s, so (a) closes them. It is one change in `reader.py`, but it moves matching-settings cells
(WRONG → EQUAL: 128 zone, 27 type, 8 writes) and changes the CSV plan, which (a)'s bar forbids. Proposed as unit 0 (§6).

## 5. The cost cases each build runs

Like-for-like medians against main, the matching path held to **1.05x**: (K0) a plain JSON read and a plain CSV read
(with a schema) and **no view**, two million rows, `count()` and `filter("n > <half>").selectExpr("max(hour(ts))")`,
because every JSON and CSV plan now carries the node, views or not (the critic's §6.5); (K1) a JSON frame view (`n INT, ts
TIMESTAMP`, two million rows) read with `SELECT count(*), max(hour(ts))`; (K2) the same over CSV with a schema; (K3)
`WHERE n > <half>` through the K1 view (the filter pushed into the scan); (K4) C-012. The differing-settings K1–K3 are
measured and reported. Unit 0 runs K0 (CSV), K2 and C-012 only.

## 6. The unit split (one build, one critique, one delta, one re-check each)

| unit | card | seat | files | bar |
|---|---|---|---|---|
| 0 | CSV-SCHEMA-TIMESTAMP-ZONE-1 (proposed, D1) | `utf8_columns` for a schema's `TIMESTAMP` fields in `_load_csv` | `reader.py` | `csv_s`, `csv_f` matching 128 of 128 each as `csv_fix`; cross as `csv_fix`'s 54 / 74; the 8 `csv_s` `d` writes EQUAL; no other cell moves; `EXPLAIN` changes only for CSV reads with a `TIMESTAMP` field (named) |
| 1 | TEMP-VIEW-FRAME-READER-PARSE-1 | (a), amended | at most nine (§4) | per column, below |
| later | READER-TYPED-SCAN-1 (proposed, (b), D2) | (b) | nine, in two units | not label-blocking (D2 is loud) |

Unit 0 lands first so unit 1's bar is measured on a correct CSV parse; if unit 0 is declined, unit 1's bar excludes
`csv_s` and `csv_f`.

**Unit 1's bar, checked per column** (a cell is EQUAL only if every column is; a column EQUAL on main must stay EQUAL,
so a cell already WRONG in one column cannot hide a move in another):

- **To EQUAL in every column:** the 382 zone view cells of the base data and the 430 of the midnight data (WRONG only
  in `b` or `ph`; `u` held EQUAL in all), the 30 ANSI view cells, the 128 view writes; `ffilter` 4 (rows),
  `union_hour` 4, `twice_hour` 4, `sql_join` 4 (`b`); the E5 cells `cj`, `cjb`, `cross` 12 (`ph`, `b`), with `ts` and
  `u` held EQUAL in each (Q9); the three-zone nesting 36 (`h1`, `h2`), with `ts`, `u` and `h3` held EQUAL; the held
  frame `nv_hour` 4 (`b`, a named gain outside the views).
- **Held EQUAL in every column:** `nv` 4; `cache_mat` and `cachetable` 8; every `pq` cell and every emulation cell
  against its own Spark cell (domain table §2, §5, §8); the matching passes of both data sets (0 moves);
  `sameSemantics` and `semanticHash` 15 of 15.
- **Named, unchanged:** `cache_lazy` 4 (Q8), D1 (unless unit 0 lands first), D2, D4, D9–D11 18 (Q10), `smartCsv`.
- **Also:** 0 EQUAL cells lost in the reader sweep (1,328), the write sweep (3,100) and part 2's bar sets; plain
  `EXPLAIN` and every `df.explain` mode under matching settings equal to main's over the reader sweep (`VERBOSE`: Q11);
  K0–K4 at most 1.05x.

## 7. Owner questions (for Fable's consult tonight; each rules from the domain table alone)

1. **The seat.** (a), amended (§4) [**default**]; (b) Rust typed scan; (a) now and (b) carded. Cells: §2.1, §5;
   `emu` 128 / 128 and 48 / 48; round 1: `emu_r` 18 / 18, `emu_live` 18 / 18, midnight `emu` 128 / 128, and the
   first sketch's placements `emu_c`, `emu_mid` WRONG in `ts` and `u` (domain table §8); (b)'s matching-settings moves
   are the A set's direct cells.
2. **D1, the CSV schema timestamp parsed as UTC** (silent, every non-UTC session, views or not, written wrong).
   (i) its own card CSV-SCHEMA-TIMESTAMP-ZONE-1, S1 by class, label-blocking (time-zone half), unit 0 before unit 1
   [**default**]; (ii) folded into unit 1 as a second commit with its own bar; (iii) carded, not label-blocking. Cells:
   `csv_s`, `csv_f` 64 of 128 matching wrong, 106 of 128 cross direct; T 27; W 8; `csv_fix` 128 of 128.
3. **D2, the parse is not PERMISSIVE** (loud: a malformed or overflowing value raises where Spark answers `NULL`).
   Card READER-TYPED-SCAN-1 with seat (b), S3, not label-blocking [**default**]; or require it before the label.
   Cells: A set `json_s`, `json_f`, `csv_s`, `json_sov`, `csv_sov` 34 of 34 direct each; `json_sts`, `csv_sts` 12 each.
4. **The ANSI half in the lineage** (30 cells; silent when made with ANSI on and read with it off, loud the other way).
   Fixed by unit 1 with no extra file [**default**]; or carded apart. Cells: domain table §3, `pq` 34 of 34.
5. **`smartCsv` and D6** (a SQL view over a `smartCsv` frame view raises `TABLE_OR_VIEW_NOT_FOUND`, main and part 1).
   Keep the ruled creation-zone behaviour and card D6 as S3 [**default**]; or mark `smartCsv`'s casts as a reader parse
   in unit 1 (its answers would then follow Spark's file rule, which is not a Spark API). Cells: Z `smart` 128 (no
   Spark), D6 96 + 16.
6. **D4, `hour()` over a timestamp string answers `0`** (silent, matching settings: `json_i`, `csv_ns`, `csv_0` 20 each).
   Card it, S2, not label-blocking: the label covers typed timestamp columns [**default**]; or S1 and label-blocking.
7. **D3, `unix_timestamp` of a `TIMESTAMP_NTZ` value reads the session zone** (silent, T set 17 per reader). Card it,
   with the first step measuring whether an Iceberg `timestamp` (NTZ) column reaches it [**default**]; or fold into
   unit 1 (a function, not a view: outside the card).

8. **A lazy `.cache()` then a view, read under other settings** (`cache_lazy`, silent, `ts` itself wrong: RePark
   materialises when the view is registered, under the creating settings; Spark at the first action, under the
   reading ones). (i) Fold into unit 1 by materialising a lazy cache at its first action (`temp_views.rs:173` and the
   facade's cache path: over unit 1's cap); (ii) its own card, TEMP-VIEW-CACHE-LAZY-PARSE-1, S1 by class,
   label-blocking (the zone half) [**default**]; (iii) disclose, not blocking. Cells (domain table §8.3): `cache_lazy`
   4 of 4 WRONG in `ts`, `u` and `b` on main and part 1; `emu_r` through the same door answers as Spark (a lazy
   materialisation would); `cache_mat` and `cachetable` 8 of 8 EQUAL in every column, held by unit 1; `pq` 6 of 6
   EQUAL.
9. **The exact-key join and `crossJoin` (E5) under seat (a).** (i) Unit 1's bar holds `ts` and `u` EQUAL per column
   in all 12 cells **and** requires `ph` and `b` EQUAL [**default**]: `emu_r` through `cj`, `cjb` and `cross` answers
   as Spark in every column (6 of 6 a direction) and `pq` is EQUAL there on main, so part 2's seat already answers the
   rest once the parse survives; (ii) hold `ts` and `u` only and leave `ph` and `b` to TEMP-VIEW-FRAME-CROSSJOIN-1 (the
   critic's default). Cells: §8.2 `cj`, `cjb`, `cross` (12: `ph` or `b` WRONG, `ts` and `u` EQUAL on main);
   `emu_c` (the first sketch's analyzer strip) WRONG in `ts` and `u` in 6 of 6.
10. **The loud direct gaps D9–D11** (a `multiLine` JSON array, a directory of `*.csv.gz`, a two-file `inferSchema`
    whose types differ). Card them S3, not label-blocking, with D2 [**default**]; or require them before the label.
    Cells: §8.5, 18 of 18 RAISE on main (Spark answers, `DOUBLE` for the merged column).
11. **`EXPLAIN VERBOSE` under matching settings.** Seat (a) keeps plain `EXPLAIN` and every `df.explain` mode main's,
    but `VERBOSE` prints the analyzer stages, where the node shows, and one more optimizer line. (i) Accept and
    disclose: `VERBOSE` is DataFusion's spelling and Spark refuses it [**default**]; (ii) require it byte-equal, which
    means stripping in the analyzer and losing E5 (12 cells, `ts` and `u` WRONG, measured by `emu_c`) unless a
    fallback is added at every seat that keeps an analysed plan. Cells: §8.6 (`EXPLAIN` accepted by both; `EXPLAIN
    VERBOSE` 199–219 lines on main, `PARSE_SYNTAX_ERROR` in Spark; `EXPLAIN EXTENDED` refused by main's parser).

**Not owner questions, and why.** The held-frame door (`nv_hour`, 4 WRONG in `b` on main) follows Spark's rule
through the same seat and is predicted EQUAL by `emu_r`: it is a named gain in unit 1's bar, not a choice.
`sameSemantics` and `semanticHash` (15 of 15 as Spark) are a constraint on the build (inventory A9), not a choice.

**Every default re-checked against the round-1 cells.** Q1 holds: the amended (a) is predicted by `emu_r` and
`emu_live` in every new door, and the first sketch's (a) is measured wrong there (`emu_c`, `emu_mid`). Q2 holds: the
midnight recount keeps D1 as found (§8.1). Q3 holds, and Q10 joins it. Q4 holds (no new ANSI cell). Q5 holds (no
`smartCsv` cell moved). Q6 holds (§8.1). Q7 holds (no new timestamp-type cell).

## 8. What the brief got wrong, or left open

- "RePark decides both under the reader's": true, and the parse half is **not** wrong: `ts` is EQUAL in every bare
  door. Only the expressions above the parse are wrong; the unit must not move the parse's own cells.
- The timestamp type is listed as a key: it has no view gap (fixed at creation in both engines).
- The malformed timestamp text and the overflowing integer text are direct gaps (D2), not view gaps; the lineage's
  ANSI view gap is an expression above the parse.
- `option("timeZone")` and `timestampFormat` are refused by RePark (D8); the SQL file-source doors do not exist on
  main (D7). Neither is a view door.
- Part 2's inventory says the CSV-with-schema parse reads the zone; on main it does not (D1).
- `task/wo/map.md` has no exact baseline in `scripts/check_map_size.py` (ceiling 16,384 bytes), so there is no
  baseline to lower.

## 9. Revision (critic round 1)

**The critic's verdict** (Opus 5.5 medium, 2026-11-19): `VERDICT: GAPS`. Its reading of Spark, the inventory and the
table reproduced (587 of 594 shared cells agree; the 7 others are `CAST(ts AS DATE)` cells this sketch's data could
not see); seat (a) was the right direction with two mechanical holes and a missing door. A copy:
[evidence/raw-r1/critic-verdict.md](evidence/raw-r1/). Its §8 work list, each item closed with evidence:

1. **Seat (a), amended** (§4). *Where the marker goes:* it survives analysis and is decided by the first optimizer
   rule, because RePark keeps analysed plans (E5's joined frame, `FrameView::closed`; inventory §4, A1–A12) and a
   marker stripped by the first analyzer rule is gone from them. Measured without a build: `emu_c` (the parse where an
   analyzer strip leaves it) is WRONG in `ts` and `u` in all 6 E5 cells a direction, where `emu_r` is EQUAL in every
   column. *Nested views:* the node decides under the live session's settings read through `LiveOptions`, not the
   enclosing analysis' `config`; `emu_live` answers all 18 three-zone cells as Spark, `emu_mid` (the enclosing layer)
   none. *Files:* the cap is nine; `df_guards.rs` is the eighth (the optimizer seat) and `plan_introspect.rs` is new
   (inventory A9). *`EXPLAIN`:* main's under matching settings for every Spark spelling; `VERBOSE` is Q11.
2. **The cells** (domain table §8): the midnight recount (382 → 430 zone view cells; the critic's 7 stand for 48);
   `ffilter` 4; the unions 6; the three-zone nesting 54; E5 12 checked per column; the cache shapes 12 (+ 6 `pq`); the
   double registration 6; plus the held frame (8) and the loud gaps D9–D11 (18). All are in unit 1's bar (§6), per
   column.
3. **K0** (§5): a plain JSON and CSV read with no view, held at 1.05x.
4. **The questions** (§7): Q8 (`cache_lazy`), Q9 (E5 under seat (a)), Q10 (D9–D11), and Q11 (`EXPLAIN VERBOSE`), new
   in this round; every earlier default re-checked against the new cells and kept.
5. **Narrowed:** "part 2 moved nothing" holds for step 0's doors (and on the midnight data); part 2 moved the 4
   `sql_join` cells from one WRONG to another (part 1 WRONG in `b` and `ph`, main in `b` only; §1, domain table §1).
6. **The records:** this section; domain table §8; inventory §3 (E5 and E11 re-read, E12–E17, the non-doors) and §4
   (the analysed-plan seats); raw cells, verdicts and the summary in [evidence/raw-r1/](evidence/raw-r1/).

## 10. Fable's guidance (2026-11-19)

The delegation, owner, 2026-11-19, about 01:28 UTC, verbatim: "Go with your recommendations and tonight you don’t even need to wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good luck".

This section is guidance, not a ruling. Fable (`claude-fable-5-2`, medium effort) built nothing and edited no repository. Its marks are kept: [M] measurement, [R] reading, [Rec] recommendation. Its four probes ran on main `4c365814` and Spark 4.1.2, 60 cells; the scripts and raw output are in `<scratchpad>/fable-rp/`.

### 10.1 Is amended seat (a) sound? (Fable's §1)

- **Answer:** yes, the seat is sound. Fable found no hole in where the decision is made. No second critic round is needed before the build. [Rec]
- **G1, the closed copy needs an analyzer.** [R] `closed_scan` analyses its input before it builds `FrameView::closed`, whose declared schema is the analysed plan's. [Rec] `ReaderParse` carries the analyzer too, taken from the state at wrap time; the differing branch analyses the copy under the live options, then calls `FrameView::closed`. No new file. Cell: any differing-settings read of `csv_i`, for example `Z/ansi_true/la_utc/csv_i/eb_hour`; Fable expects a loud schema error without the fix.
- **G2, the rule must walk subqueries itself.** [Rec] `repark_reader_parse` uses `transform_down_with_subqueries`; a JSON frame view inside an `IN` or scalar subquery is then inlined under matching settings, and a rule that misses it fails loud ("no installed planner"). Cells: `subq/*`, 6 cells in G2, EQUAL on main and must stay EQUAL; add the matching-settings pass of the same query. [Clerk note: G2 says 6 cells for `subq/*`, while the §1.4 table below gives `subq` as 4 of 4 EQUAL. Both are copied as Fable wrote them; the builder counts the cells.]
- **G3, analyzer rules that key on the plan root.** [R] A bare reader frame's root is now the node, so `SparkProjectionDisplay` (`spark_typed.rs:89-97`) returns early, where on main it sees the parse `Projection`. [Rec] Add one named cell: a headerless CSV with no schema (the `_cN` renames), `.columns` and `EXPLAIN`, under matching settings.
- **G4, where the `LiveOptions` handle is built.** [R] Built inline at `temp_views.rs:57-66` today; the sketch says "`LiveOptions` from a context" in `extension.rs` (file 3). [Rec] Build it in `extension.rs` and leave `temp_views.rs` untouched. Use one handle per session, shared by every node, so the fallback in `LiveOptions::read` (`extension.rs:39-47`) is the session's last read, never a frame's creation settings. If the builder wants `temp_views.rs` to call the shared constructor, it takes the conditional ninth slot (`reader_support.py`); if both are needed, halt and report.
- **§1.4, the cell family the bar lacks.** [M] A held JSON or CSV frame, built before a `SET` and acted on directly after it, parses under the creating zone on main, and `ts` is wrong silently with no view: 12 cells WRONG in `ts` with no view, and 4 path writes WRONG in `b`. [R] Seat (a) should fix `direct`, `cjdirect` and `cache_after` (the differing branch) and `pathwrite` (through the scratch view, like `eb_hour`); `udf` is not predictable from here and belongs with `cache_lazy` (Q8) if it materialises at column build. [Rec] Add `direct`, `cjdirect`, `cache_after` and `pathwrite` to unit 1's bar as "to EQUAL in every column" (14 cells over `json_s` and `csv_i`, both directions; `cache_after` over both readers); add `subq` as held EQUAL; add `udf` as measured and reported. Unit 1's PR text must say plainly that it changes answers with no view involved: the sketch's "the parse's own cells must not move" (§8) holds only for the doors step 0 measured.
- **§1.5, second critic round.** [Rec] No. G1 to G4 and §1.4 go into the builder's brief as written here; the build's bar and the post-build verifier check them.

| Q | ruling to act on | why |
|---|---|---|
| 1 | (a) amended now; (b) carded as READER-TYPED-SCAN-1 | [M] `emu_r` and `emu_live` predict (a) in every door; (b) moves matching cells and `EXPLAIN`. |
| 2 | (i) own card CSV-SCHEMA-TIMESTAMP-ZONE-1, S1, label-blocking, unit 0 | [M] Silent, 64 of 128 matching cells, written wrong. It moves matching cells, so it cannot share unit 1's bar. |
| 3 | Default: card READER-TYPED-SCAN-1, S3, not label-blocking | [M] Loud in all 34 direct cells. |
| 4 | Default: fixed by unit 1, no extra file | [M] `pq` 34 of 34. |
| 5 | Default: keep the ruled `smartCsv` behaviour; card D6 as S3 | The owner ruled it on 2026-11-18. D6 is loud. |
| 6 | **Changed from the default: card D4 as S1 by class and label-blocking.** Own unit, not unit 1 | [M] It is not a reader gap. On main, `hour(c)`, `minute(c)` and `second(c)` over a `STRING` column of an **Iceberg v2 table** answer `0` for `'2024-01-01 03:04:05'` and `'2024-06-01 23:30:00'`, under matching settings. `year`, `month`, `dayofmonth`, `date_format`, `to_date` and `unix_timestamp` over the same column are right, and `hour(CAST(c AS TIMESTAMP))` is right. So it is three functions, silent, on a scalar column the label covers. I did not run Spark for these ten expressions; Spark's answers `3`, `4`, `5` are the domain table's D4 reading. |
| 7 | Default: card D3; its first step measures an Iceberg `timestamp` (NTZ) column. Run that measurement this week | [R] An Iceberg NTZ column is in the label's scope. One clerk-grade probe settles whether it blocks. |
| 8 | (ii) own card, S1 by class, label-blocking. **Widen it to the materialisation-timing family** | [M] `cache_lazy` 4 of 4 WRONG in `ts`. [R] The `udf` door above may be the same family. Its step 0 runs after unit 1 merges, because unit 1 changes when a held frame parses. |
| 9 | (i) require `ph` and `b` EQUAL in the 12 E5 cells, with a stop rule | [M] `emu_r` 6 of 6 a direction and `pq` EQUAL on main. [Rec] Stop rule: if any of the 12 is not EQUAL in `ph` or `b` after the build, the builder adds no file; it reports the cell and leaves it to TEMP-VIEW-FRAME-CROSSJOIN-1. `ts` and `u` stay a hard bar. |
| 10 | Default: card D9–D11 as S3 with D2 | [M] 18 of 18 loud. |
| 11 | (i) accept and disclose | [M] Spark refuses `EXPLAIN VERBOSE`. Requiring it byte-equal loses E5 (`emu_c` WRONG in `ts`). |

- **Where Fable would still want the owner:** Q6 and Q8, each adding a label-blocking card; for the owner's morning read.

### 10.2 The file cap of nine (Fable's §3)

- **Cap:** accept nine. Cut nothing. [Rec]
- **File 5 (`plan_introspect.rs`) is required:** an unknown node is hashed as `Opaque` plus its `Debug`, which breaks the 15 of 15 `sameSemantics` agreement. [R]
- **Files 7 and 8 are required:** `temp_view.rs:22` imports `reader_lineage`, and the helpers left behind in `temp_settings.rs:180-262` would be dead code. [R]
- **The ninth slot is conditional** and holds `reader_support.py` or `temp_views.rs`, never both (G4). [Rec]
- **`ReaderParse` needs `PartialEq`, `Hash` and `PartialOrd` over its input only**, as `FrameSettingsNode` does (`frame_settings.rs:94-116`); neither the copy nor the handle takes part. [Rec]

### 10.3 The order (Fable's §4)

Back to back on lane A, stacked, not folded.

1. Unit 0 first, as its own PR: one file, and it moves matching cells, so it needs its own bar.
2. Unit 1 branches from unit 0's head once unit 0's local gates are green; it does not wait for unit 0's verifier or merge. Both units edit `reader.py`, so stacking avoids a conflict.
3. Unit 0's verifier (Opus 5.5 medium) runs in lane B's gap, between SUM-DEC-I128WRAP-1 finishing and the grouped decimal `avg` unit starting. That keeps the two-agent cap.
4. Unit 0 merges on green CI and a PASS. Unit 1 then rebases onto main.
5. If unit 0 fails its verifier, unit 1 keeps building; its bar excludes `csv_s` and `csv_f` until unit 0 lands, as sketch §6 already says.

[R] Seven hours fits unit 0 whole and unit 1's build and first sweep. Unit 1's verifier and delta may run past the window; the sweeps (1,328 reader cells, 3,100 write cells) are not shortened to fit.

### 10.4 What step 0 and the critic both missed (Fable's §5)

- **[M] The held frame acted on directly (§1.4):** 12 cells WRONG in `ts` on main with no view, plus 4 path writes WRONG in `b`. Seat (a) should fix them; the bar must hold it to that.
- **[M] D4 reaches Iceberg `STRING` columns (Q6):** it was filed as a reader gap. It is a function gap on label-scope tables.
- **[R] G1:** the closed copy needs an analyzer handle; the sketch's mechanism skips `closed_scan`'s analysis step.
- **[R] G3:** rules keyed on the plan root; the inventory's search could not find them.
- **[R] `PREPARE`:** a prepared SQL statement over a frame view keeps the plan it built. Under seat (a) the node is decided when the statement is executed, which is Spark's rule. [Rec] Add one cell: `PREPARE` under A, `SET`, `EXECUTE` under B, over a JSON frame view with `hour(ts)`. Report it; do not hold the unit to it.
- **[R] The micro-batch door:** `microbatch/relation.rs:156` takes a batch frame's parts. The track is frozen, so this is a note for its unfreeze, not for unit 1.

### 10.5 Probe folder

`<scratchpad>/fable-rp/` holds `p2.py`, `p3.py` and `p4.py`, and the raw outputs `main-p2.txt`, `spark-p2-raw.txt`, `main-p3.txt`, `spark-p3.txt` and `main-p4.txt`. Every RePark run printed `_native` from the `pkg-main-4c365814` package.

Up: [map.md](map.md).
