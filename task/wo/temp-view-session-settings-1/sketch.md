# TEMP-VIEW-SESSION-SETTINGS-1 — step 0 sketch (measured; absorbs VIEW-ANSI-CAPTURE-1)

**Date:** 2026-11-17. **Sketcher:** Claude Opus 5.5 (`claude-opus-5-5`), step 0 only: documents and probes, no product
code. **Rulings carried:** owner, 2026-11-17, evening, verbatim: "dispatch Opus xhigh effort levels for these fixes,
then use Opus 5.5 medium verifiers for the time and not muse, at least until these things are closed out". The
orchestrator **reads** "these fixes" as this unit, both parts and the time-zone half, and SUM-DEC-I128WRAP-1; that is
a reading, not the owner's words. **Not ruled:** the severity, whether the time-zone half blocks
ICE-V2-FLAT-AWS-LABEL-1, and whether the differing-settings path is held to the 1.05x gate (§7). Design reading under
test: Fable's reply of 2026-11-17 ("plan the view's body under a session state that carries the settings captured at
creation"). Cards: [temp-view-session-settings-1](../../roadmap/mid-term/temp-view-session-settings-1-card-2026-11-02.md),
[view-ansi-capture-1](../../roadmap/mid-term/view-ansi-capture-1-card-2026-10-01.md). Measured:
[domain-table.md](domain-table.md); seats: [inventory.md](inventory.md); raw: [evidence/](evidence/map.md).

## 1. Findings in short

- **1,414 cells read or write a view under settings other than its creation settings; 512 are view gaps (393 silent,
  119 loud)** and 162 are direct gaps that say nothing about views (domain table §6). 63 of the view gaps are wrong
  writes (51 silent).
- **The time-zone half is the larger one and it is silent with ANSI on:** 250 read gaps and 36 wrong writes, every
  zone-reading function, on every temp-view door but the held frame (and the DataFrame door for the `TIMESTAMP '…'`
  literal).
- **The ANSI half:** 170 read gaps; silent (NULL or a wrapped integer where Spark raises) only for a reader on ANSI
  off; a reader on ANSI on meets it as a raise where Spark answers, after a `SET` inside the session.
- **Two more keys open the gap on the SQL door:** `caseSensitive` (3 cells) and the decimal legacy switch (6 cells,
  RePark refuses `CANNOT_UP_CAST_DATATYPE`). `timeParserPolicy` and `timestampType` do not reach RePark's parsing at
  runtime (direct gaps), and Spark lets `timeParserPolicy` follow the reader anyway.
- **Fable's design reading holds.** DataFusion fixes a function's settings when the physical plan is built, and a held
  `DataFrame` carries its creation `SessionState`; the held frame matches Spark on collect in every ANSI and zone cell
  but `current_timezone()` on a frame first acted on after the `SET` (6 cells).
- **New: held frames do not match Spark on write.** Every DataFrame write door registers the frame as a scratch temp
  view and runs SQL under the live session (`writer_layout.py:95-121`), so the write follows the writer's settings
  (12 wrong writes from held frames). Part 2 covers it only if it captures the **frame's** settings.
- **Iceberg catalog views follow the reader on both engines (64 cells, 0 view gaps); leave them alone.**

## 2. Spark's rule, read from the 4.1.2 jar and measured

Read with `javap -c -p` from `spark-catalyst_2.13-4.1.2.jar` and `spark-sql_2.13-4.1.2.jar` (excerpts:
[evidence/jar/javap-excerpts.txt](evidence/jar/map.md)):

- **`CapturesConfig.sqlConfigsToProps(conf, prefix)`** = `getModifiedConf(conf)` (every entry of
  `SQLConf.getAllConfs` with `isModifiable` and `shouldCaptureConfig`) plus `SESSION_LOCAL_TIMEZONE` and
  `ANSI_ENABLED` at their current values when not already in the modified set, each stored under `prefix + key`.
- **`shouldCaptureConfig(key)`** is true when the key is in `configAllowList` (`DISABLE_HINTS`) or matches no prefix
  of `configPrefixDenyList` (`MAX_NESTED_VIEW_DEPTH`, `spark.sql.optimizer.`, `spark.sql.codegen.`,
  `spark.sql.execution.`, `spark.sql.shuffle.`, `spark.sql.adaptive.`, five `spark.sql.hive.convert*` keys,
  `ADDITIONAL_REMOTE_REPOSITORIES`) and is not in `singlePassResolverDenyList`.
- **`ViewHelper$.createTemporaryViewRelation`:** when `storeAnalyzedPlanForView` is off and the view has SQL text it
  calls `prepareTemporaryView`, which reaches `generateViewProperties` → `sqlConfigsToProps`; otherwise
  `prepareTemporaryViewStoringAnalyzedPlan` stores the analysed plan and no settings (the DataFrame door).
- **`View$.effectiveSQLConf(configs, isTempView, version)`:** unless `useCurrentSQLConfigsForView` is on for a
  persistent view, builds a fresh `SQLConf`, sets every captured key, then
  **`Analyzer$.retainResolutionConfigsForAnalysis`** copies from the reader only `RETAINED_ANALYSIS_FLAGS` (the five
  Hive convert keys, `planChangeLog.level`, `expressionTreeChangeLog.level`) and every `spark.sql.catalog.*` key, and
  `trySetAnsiValue` sets ANSI from the creating version when `ASSUME_ANSI_FALSE_IF_NOT_PERSISTED` is on and nothing
  was captured. A key that was not captured falls back to Spark's default, not to the reader's value.

**What the measurement adds** (domain table §1): the captured settings govern **analysis** of the body only. Spark
inlines the analysed body into the reader's plan, so whatever reads `SQLConf.get` later follows the reader:
`current_timezone()` (`ReplaceCurrentLike` in the optimizer) and `timeParserPolicy` (the parser is built lazily on
first evaluation; an object evaluated before the `SET` keeps it). A held Dataset analyses at creation and optimizes
at its first action. `writeTo` re-analyses the parsed plan under the writer's settings (zone follows the writer, the
parse-time ANSI mode of `+` and `/` stays); `write.parquet` writes the analysed plan (creation).

## 3. The key inventory (detail in [inventory.md](inventory.md) §1)

**Capture:** ANSI and the session zone always (as Spark does), `caseSensitive` and the decimal legacy switch, and the
engine's `timestampType` carrier (no gap today; it costs nothing and is right once the runtime value reaches the
engine). The catalog and namespace are already captured (`TempViewDefinition.catalog`, `.namespace`; 14 cells equal).
**Follow the reader:** `current_timezone()`, `timeParserPolicy`, the `datafusion.*` keys, the write keys. Readers:
ANSI 6 analyzer rules on main (7 with #1355's `aggregate.rs:301`) and 24 UDF call sites; the zone 14 UDF call sites, 6
planning-time readers and 4 router readers; case sensitivity at SQL planning (`repark-spark/src/lib.rs:95`).

## 4. The mechanism, tested against DataFusion 54.1.0 (cargo registry sources)

- **Settings are fixed at physical planning — confirmed.** `create_physical_expr` gives each `ScalarFunctionExpr` the
  planning state's `execution_props.config_options` (`datafusion-physical-expr-54.1.0/src/planner.rs:342-351`); the
  expression keeps them (`scalar_function.rs:57`) and hands them to every call (`:260`). The physical planner passes
  `session_state.execution_props()` (`datafusion-54.1.0/src/physical_planner.rs:298`, `:653`). Analyzer rules get
  `&ConfigOptions` from the same state (`session_state.rs:666-700`).
- **A held `DataFrame` keeps its creation state — confirmed.** `SessionContext::state()` clones the shared state and
  marks it (`execution/context/mod.rs:2036-2039`); `DataFrame::new` boxes that clone (`dataframe/mod.rs:258-262`);
  `create_physical_plan` and `task_ctx` use it (`:301-302`, `:1557-1559`). So analysis, optimisation and execution all
  see creation settings — the held column of the domain table.
- **How a view's plan enters:** `LogicalPlanBuilder::scan` inlines `get_logical_plan()` at plan-build time
  (`datafusion-expr-54.1.0/src/logical_plan/builder.rs:517-540`); `DataFrame::into_view` is a provider that returns
  its plan there (`dataframe/mod.rs:2643-2652`). `ViewTable::scan` puts pushed filters, the projection and the limit
  on the stored plan and plans it under the **reader's** state (`datafusion-catalog-54.1.0/src/view.rs:109-150`).
  RePark's SQL view stays a closed `TableScan` (`explain-main.txt`: `TableScan: ?table?`).
- **Why the DataFrame door keeps division's creation mode** (Fable did not trace it): the registered plan already
  carries the creation-time rewrite and the reader's `SparkExprSemantics` (`analyzer.rs:22-27`, `:204-233`) wraps it
  again; the inner guard decides. Execution-time readers follow the reader.

**The four traps:**

1. **`now` and the refresh.** `SessionState::mark_start_execution` (`session_state.rs:856-858`) calls
   `ExecutionProps::mark_start_execution` (`datafusion-expr-54.1.0/src/execution_props.rs:116-119`), which restamps
   `query_execution_start_time` and the alias generator while it installs the options. `config_options` is a public
   field (`execution_props.rs:64`), so the build sets it directly on a clone of the reader's state and never calls
   `mark_start_execution` or `SessionContext::state()` for the body's physical planning. Pin: `ct =
   current_timestamp()` across the view boundary is `true` (4 cells, equal today).
2. **Pushed filters.** `ReplanningTempView::supports_filters_pushdown` answers `Exact` (`temp_view.rs:82-96`) and
   receives no session, so it cannot compare settings. When the settings differ, the scan plans the body without the
   pushed filters, projection and limit under the captured settings, then applies them as physical operators built
   with the reader's state over the body's output. Pins: the reader-side cells (18 ANSI, 12 zone).
3. **Renames.** `renamed_temp_view` rebuilds `TempViewDefinition` field by field (`temp_view.rs:272-279`); the new
   field must be copied there. Measured: a renamed view answers as the SQL view does today (29 view gaps in 54 cells).
4. **Nested views.** Each `ReplanningTempView` lays its own settings over the state it is given, so an inner view
   keeps its own when planned inside an outer view's body. A DataFrame view inside a SQL view is decided while the
   outer body is planned under the outer's captured settings, which is not the live session: Part 2's decision must
   read the planning state, not the live session (§5). Pins: 32 nested cells, 12 wrong today.

## 5. Seats

### Part 1 — SQL `CREATE TEMP VIEW` (seat: `ReplanningTempView::scan_replanned`, `temp_view.rs:115-144`)

- **1A (recommended): overlay at the scan.** `TempViewDefinition` (`:30-37`) gains the captured settings, taken at
  `execute_create_temp_view` (`execute.rs:118-141`) from the planning state of the `CREATE` statement. In
  `scan_replanned`, when the captured settings equal both the reader's (`state.config_options()`) and the live
  session's, run today's code unchanged. Otherwise clone the reader's `SessionState`, write the captured values into
  its options and into `execution_props.config_options`, plan the stored text through
  `SessionContext::new_with_state` (`context/mod.rs:368`) so router-time readers (case folding, the literal) see
  them, rewrite `current_timezone()` in the body to the reader's zone literal, physical-plan the body under the
  overlaid state, and put the pushed filters, projection and limit on top under the reader's state. The "equal" test
  also fixes the `spark.table("v")` mixed case: a held reader whose snapshot differs from the live session no longer
  plans the text under the live session.
  - **Cost:** when settings match, the plan is today's plan (the view was already a closed scan) plus one comparison
    of a few fields per scan.
  - **Risk:** the overlaid state must keep the reader's catalog list and the view-expansion guard
    (`self.catalogs.view_expansion_guard()`, `:122`), and `refuse_dropped_dependency` and
    `conform_to_creation_schema` (`:133`, `:141`) run as today.
- **1B: store the planned body, as the DataFrame door does.** Keep the creation-time `DataFrame` and plan it at scan
  under its own snapshot. Simpler (no SQL planning under an overlay), but it stops re-planning the text, so dropped
  or changed dependencies stop being re-resolved (Spark re-analyses the text under the captured settings), and it
  turns the SQL door into the DataFrame door's double-analysis. Not recommended.
- **Keys:** §3's capture list. **`current_timezone()`:** the body rewrite above. **Catalog views and the native door:**
  untouched — catalog views are `ViewTable`s built in `view_ddl/read.rs:147`, and the native door does not build a
  `ReplanningTempView`.
- **Cases it closes:** the SQL view, SQL over SQL, DataFrame over SQL (the inner level), the renamed view,
  `spark.table` over a SQL view (except `current_timezone()` on a fresh held frame), the SQL-door writes, the unset
  case on the SQL door: **352 cells, 42 of them wrong writes** (`CREATE TABLE … AS SELECT`, `INSERT … SELECT`,
  `spark.table(v).writeTo`, 14 each).

### Part 2 — DataFrame `createOrReplaceTempView` (seat: `temp_views.rs:42-44`, `frame.clone().into_view()`)

Capture the settings from the **frame's own state** (`DataFrame::into_parts`, `dataframe/mod.rs:1673`, or
`task_ctx().session_config()`), not the session's at registration. That is what makes the scratch-view write doors
(inventory rows 7-10) and a frame registered after a `SET` behave as Spark's analysed plan does.

- **2A (recommended): a marker at inline time, decided by the analyzer.** The provider's `get_logical_plan()` returns
  the frame's plan wrapped in a small extension node carrying the captured settings; a first analyzer rule compares
  the node's settings with the analyzer's `&ConfigOptions` — the state that is really planning, which inside a SQL
  view is that view's captured settings — and either unwraps it (today's plan exactly) or replaces it with a closed
  `TableScan` over a provider that plans the frame under its own state with the reader's `now`, the reader's zone for
  `current_timezone()`, and the pushed filters on top under the reader's state (trap 2).
  - **How the provider sees the reader's settings: it does not; the analyzer does.** This is the decision Fable left
    to the sketch.
  - **Risk:** every walker that looks through an inlined view before analysis must see through the node; the build
    lists them (`temp_view.rs:375-422`, `:229-290`, `sorted_view.rs:276` and the attribute-id walkers) and keeps the
    SQL-door wrapper (a bare `ReplanningTempView` scan, inventory row 2) unwrapped.
- **2B: compare in `get_logical_plan()` against the live session.** The provider holds `ctx.state_ref()` and returns
  `Some(plan)` when the frame's settings equal the live ones, `None` otherwise. No new node; right for direct reads
  and for held readers (inlining happens when the reader frame is built). **Wrong** for a DataFrame view inside a SQL
  view whose captured settings differ from the live ones (the nest-df-sql pins), and `get_logical_plan()` is also
  called outside planning (`temp_view.rs:245`, `:261`, `:355`), where `None` would change what those walkers find.
- **Keys:** the same list. **Catalog views and the native door:** untouched; the build confirms that the native
  door's temp views do not reach `temp_views.rs:42-44`, or keeps them on today's path.
- **Cases it closes:** the DataFrame view, SQL over DataFrame, the nested levels created on this door, the held-frame
  write doors (`write.parquet`, `writeTo`, `CREATE TABLE … AS SELECT … FROM df_view`): **156 cells, 21 of them wrong
  writes** (9, 3 and 9).
- **A Spark split to decide (owner question 4):** with frame capture, `held.writeTo(…)` stores the creation zone;
  Spark's `writeTo` stores the writer's zone (8 cells equal today would become wrong). `write.parquet` stores the
  creation zone on Spark (6 cells wrong today become right).

### The cost cases each build runs (Part 1 on SQL views, Part 2 on DataFrame views)

1. A view read with matching settings.
2. A filter pushed through such a view.
3. A join over a view (a DataFrame view for Part 2).
4. C-012, at most 1.05x on held rows.
5. For Part 2, the DataFrame write doors with matching settings (`saveAsTable`, `writeTo`, `write.parquet`): every
   DataFrame write passes the seat.
6. The differing-settings path, measured and reported (owner question 3).

## 6. The unit split

**Confirmed: two parts, each its own unit under the one-round cap,** because the seats are separate and each closes
its own doors with its own pins: **Part 1** (352 cells; seat 1A), then **Part 2** (156 cells; seat 2A). Changes to
Fable's split:

- Part 2's scope grows to the scratch-view write doors and must capture the frame's settings; its bar includes the 21
  DataFrame-door wrong writes.
- Part 1 must land first or with Part 2's nested pins skipped: 2A's nested decision reads the outer view's captured
  settings, which only Part 1 installs.
- **Outside both parts (no seat here):** `current_timezone()` in a held frame first acted on after a zone `SET`
  (6 cells, the frame's snapshot fixes it at creation; Spark at first action). Card it separately.
- **Every wrong write, by part:** Part 1 — `CREATE TABLE … AS SELECT … FROM sql_view` (14), `INSERT INTO … SELECT …
  FROM sql_view` (14), `spark.table(sql_view).writeTo(…).create()` (14); Part 2 — `CREATE TABLE … AS SELECT … FROM
  df_view` (9), held `write.parquet` (9), held `writeTo(…).create()` (3). Unmeasured doors on the same seats:
  `saveAsTable`, `insertInto`, S3 paths, `MERGE` with a frame source.

## 7. Owner questions (each with a default)

1. **Severity.** Default: **S1** for the family (silent different values on the Spark door; zone cells with ANSI on;
   wrong writes). Cells: domain table §2, §4.
2. **Label status of each half.** Default: **the time-zone half blocks ICE-V2-FLAT-AWS-LABEL-1** (250 silent read gaps
   and 36 silent wrong writes with ANSI on); **the ANSI half does not** (silent only for a reader on ANSI off, outside
   A1; a reader on ANSI on sees a raise, 86 cells, which is loud). Both halves are built in the same two parts.
3. **The 1.05x gate on the differing path.** Default: **hold only the matching path** (cost cases 1-5) to 1.05x;
   measure and report the differing path (case 6). Cells: none yet; the build measures.
4. **Spark's two write behaviours for a held frame.** Default: **follow creation on every write door** (as Spark's
   `write.parquet` and as the analysed plan), and record Spark's `writeTo` zone behaviour as an accepted divergence (8
   cells, today equal by accident). The alternative keeps `writeTo` on the writer's zone.
5. **`current_timezone()` on a held frame** (6 cells, outside both parts). Default: card it at S3 and do not build it
   in this unit.
6. **The direct gaps seen in passing** (domain table §6). Default: card the ANSI-on array index (`element_at`,
   `array[i]` answer NULL where Spark raises; silent, ANSI on) at S1 for its own triage, since it sits in the label's
   scope; leave the rest to their existing cards (string casts, `sequence`, error text).
7. **VIEW-ANSI-CAPTURE-1's `sequence(1, '3')` body** cannot show the view gap while RePark refuses it directly.
   Default: pin its view cells after the `sequence` coercion is fixed, not in this unit.

## 8. What the brief and Fable's reply got wrong, or left open

- Seven analyzer-time ANSI readers is the count on #1355's head; **main has six** (`aggregate.rs:301` is #1355's). The
  execution-time count is **24**, not about 25.
- **"The held DataFrame matches Spark in all 40 cells"** holds for collect only; Fable's held frames were read before
  the `SET`, which also hid Spark's first-action rule for `current_timezone()`. Held frames diverge on write (§1).
- **"A map subscript"** as an ANSI body: `map['missing']` and `element_at(map, 'missing')` answer NULL in both modes on
  Spark 4.1.2. The ANSI-gated subscript is the nested (struct) key one (`map_subscript.rs:106`), measured here.
- **"When the settings differ, the view should decline them"** (pushed filters): the provider has no session at
  `supports_filters_pushdown`; the sketch applies them above the body instead (trap 2).
- The Python door is `crates/repark-python/src/session.rs:362-372`, not `:355-372`.
- `temp_views.rs:49-74` (cache views) are eager materialisations: nothing is planned at read, so they are not seats.
- Fable did not read the native door, case sensitivity or other keys; this step read case sensitivity, the decimal
  legacy switch, `timeParserPolicy`, `timestampType` and the namespace, and left the native door out with the reason
  above.

Up: [map.md](map.md).
