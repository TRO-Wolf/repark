# TEMP-VIEW-FRAME-READER-PARSE-1 — step 0 sketch (measured, no product code)

Card: TEMP-VIEW-FRAME-READER-PARSE-1, S1 by class, label-blocking for ICE-V2-FLAT-AWS-LABEL-1 (owner, 2026-11-18, about
14:30 UTC: "We better go with B to be safe"); `smartCsv` keeps the creation zone (owner, about 15:00 UTC: "Defaults on
all three"). Measured on main `4c365814` and on `6286ba19` (part 1 only) against Spark 4.1.2. The table:
[domain-table.md](domain-table.md); the insertion inventory: [inventory.md](inventory.md); the cells:
[evidence/](evidence/map.md).

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
- **Part 2 moved nothing here:** 0 of the lineage's cells differ between main and part 1 (5,732 cells per build).
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
  scan) carded as the Rust-first follow-up. Seven owner questions (§7).

## 2. Spark's rule, read from the 4.1.2 jars and measured

Excerpts: [evidence/jar/javap-file-source.txt](evidence/jar/map.md). The view-capture side was read by part 1's step 0
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

### Seat (a) — mark the reader's parse; plan it under the reader's settings when the frame is closed (recommended)

- **Mechanism.** A logical node `ReaderParse` (no expressions; its input's schema), defined beside `FrameViewSeat` in
  `repark-core/src/extension.rs`. The facade wraps the frame `_apply_reader_schema_semantics` returns through one
  private native method; no SQL text and no public DataFrame call can produce the node, so a user's own
  `CAST(… AS TIMESTAMP)` is never taken for it (the DECIMAL-DIVIDE-LITERAL-1 verifier's F2). `FrameSettingsRule`
  (already the first analyzer rule, `df_guards.rs:173`) strips every `ReaderParse` it meets, so the analysed plan is
  main's. `closed_scan` (`frame_settings.rs:462`) first replaces each `ReaderParse` in the held frame by a scan of a
  `ReaderParseView` that plans its input under the **reader's** settings (the `config` the rule is deciding under);
  then it analyses the rest under the frame's settings, as today. The SQL door does the same in `plan_under_overlay`
  (`temp_settings.rs:294`) with the outermost reader's settings: `ReaderZone` (`temp_settings.rs:41`) grows into the
  outermost reader's full `TempViewSettings`, so nested overlays keep the parse under the session that runs the query.
- **`reader_lineage`:** removed, with `unmarked` and the SQL door's fallback (`temp_view.rs:177`; the fallback in
  `temp_settings.rs` if nothing else calls it). The JSON frame joined with a Parquet frame view gets the Parquet side's
  part 2 answer back (domain table §2.2, `j`).
- **Files (cap 7):** `reader.py`, `crates/repark-python/src/dataframe/` (one private method), `repark-core/src/extension.rs`,
  `frame_settings.rs`, `temp_settings.rs`, `temp_view.rs`; `reader_support.py` only if a promotion path returns past
  the wrap. If a session without the seat can build a reader frame, the strip must live in a rule every session
  installs (`df_guards.rs`), an eighth file: the builder checks first.
- **Cells it moves (predicted by `emu` and `pq`):** the 382 zone view cells and 30 ANSI view cells to EQUAL, the 128
  view writes to EQUAL. Unchanged: D1 (unless unit 0 lands first), D2 (the parse keeps the reader's ANSI, as on main),
  `smartCsv`.
- **`EXPLAIN` under matching settings:** main's, by construction (the node is gone before any optimizer rule). The
  builder also compares `df.explain(True)`: if it prints the unanalysed plan, the node shows there and must be hidden
  or named.
- **Cost:** the matching path adds no plan walk (`holds_marker` already walks); expected 1.00–1.01x, held to 1.05x on
  the cost cases below. The differing path plans one more overlay per parse (measured, not held).
- **Sweep risk: medium.** Until analysis every JSON and CSV frame carries the node; code that matches a reader frame's
  unanalysed plan (`scans_only`, `replanning_scan`, `inputFiles`, join attribute tokens, `unionByName`, the native
  `explain`) sees an Extension. The 1,328-cell reader sweep and the 3,100-cell write sweep re-run in full.

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

Like-for-like medians against main, the matching path held to **1.05x**: (K1) a JSON frame view (`n INT, ts
TIMESTAMP`, two million rows) read with `SELECT count(*), max(hour(ts))`; (K2) the same over CSV with a schema; (K3)
`WHERE n > <half>` through the K1 view (the filter pushed into the scan); (K4) C-012. The differing-settings K1–K3 are
measured and reported. Unit 0 runs K2 and C-012 only.

## 6. The unit split (one build, one critique, one delta, one re-check each)

| unit | card | seat | files | bar |
|---|---|---|---|---|
| 0 | CSV-SCHEMA-TIMESTAMP-ZONE-1 (proposed, D1) | `utf8_columns` for a schema's `TIMESTAMP` fields in `_load_csv` | `reader.py` | `csv_s`, `csv_f` matching 128 of 128 each as `csv_fix`; cross as `csv_fix`'s 54 / 74; the 8 `csv_s` `d` writes EQUAL; no other cell moves; `EXPLAIN` changes only for CSV reads with a `TIMESTAMP` field (named) |
| 1 | TEMP-VIEW-FRAME-READER-PARSE-1 | (a) | the seven of §4 | the 382 zone, 30 ANSI and 128 write view cells EQUAL; 0 EQUAL cells lost in this table, the reader sweep, the write sweep and part 2's bar sets; matching settings: 0 moves and `EXPLAIN` main's; K1–K4 at most 1.05x |
| later | READER-TYPED-SCAN-1 (proposed, (b), D2) | (b) | nine, in two units | not label-blocking (D2 is loud) |

Unit 0 lands first so unit 1's bar is measured on a correct CSV parse; if unit 0 is declined, unit 1's bar excludes
`csv_s` and `csv_f`.

## 7. Owner questions (for Fable's consult tonight; each rules from the domain table alone)

1. **The seat.** (a) marker and closing [**default**]; (b) Rust typed scan; (a) now and (b) carded. Cells: §2.1, §5;
   `emu` 128 / 128 and 48 / 48; (b)'s matching-settings moves are the A set's direct cells.
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

Up: [map.md](map.md).
