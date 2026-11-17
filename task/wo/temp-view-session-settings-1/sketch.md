# TEMP-VIEW-SESSION-SETTINGS-1 — step 0 sketch (measured, revised after the critic; absorbs VIEW-ANSI-CAPTURE-1)

**Date:** 2026-11-17. **Sketcher:** Claude Opus 5.5 (`claude-opus-5-5`), step 0 only: documents and probes, no product
code. **Revision:** a second Claude Opus 5.5 (`claude-opus-5-5`) builder, after the critic's verdict (Claude Opus 5.5
medium, **UNSOUND**: the "filters on top" rule). Every critic claim was re-measured on main `6f80793b` and Spark 4.1.2
before it was written down; §9 records where this revision agrees and where it does not.

**Rulings carried:**
- Owner, 2026-11-17, evening, verbatim: "dispatch Opus xhigh effort levels for these fixes, then use Opus 5.5 medium
  verifiers for the time and not muse, at least until these things are closed out". The orchestrator **reads** "these
  fixes" as this unit, both parts and the time-zone half, and SUM-DEC-I128WRAP-1; that is a reading, not the owner's
  words.
- Owner, 2026-11-17, verbatim "Defaults, merge 1355", adopting Fable's defaults. Step 0's questions 1 to 3 are
  **ruled**: S1 by class; the time-zone half blocks the label ICE-V2-FLAT-AWS-LABEL-1, which is scoped to ANSI on; only
  the matching-settings path is held to 1.05x, and the differing path is measured and reported (§7).

Design reading under test: Fable's reply of 2026-11-17 ("plan the view's body under a session state that carries the
settings captured at creation"). Cards:
[temp-view-session-settings-1](../../roadmap/mid-term/temp-view-session-settings-1-card-2026-11-02.md),
[view-ansi-capture-1](../../roadmap/mid-term/view-ansi-capture-1-card-2026-10-01.md). Measured:
[domain-table.md](domain-table.md); seats: [inventory.md](inventory.md); raw: [evidence/](evidence/map.md).

## 1. Findings in short

- **2,725 comparisons; 1,527 cells read or write a view under settings other than its creation settings; 574 are view
  gaps (451 silent, 123 loud)**, 787 agree with Spark and 166 are direct gaps that say nothing about views (domain
  table §6, §7). Step 0 measured 2,583 of them; this revision added 142 new comparisons and re-measured 14 step 0 write
  cells on main `6f80793b` (every answer reproduced). **67 of the view gaps are wrong writes (55 silent).**
- **The time-zone half is the larger one and it is silent with ANSI on:** every zone-reading function, on every
  temp-view door, on the held frame's derivations through facade scratch views, and in `localtimestamp()` and
  `hour(now())` inside a view (Spark folds them with the creation zone).
- **The ANSI half:** silent (NULL or a wrapped integer where Spark raises) only for a reader on ANSI off; a reader on
  ANSI on meets it as a raise where Spark answers, after a `SET` inside the session.
- **Two more keys open the gap on the SQL door:** `caseSensitive` (3 cells) and the decimal legacy switch (6 cells,
  RePark refuses `CANNOT_UP_CAST_DATATYPE`). `timeParserPolicy` and `timestampType` do not reach RePark's parsing at
  runtime (direct gaps); `spark.sql.defaultCatalog` follows the reader on both engines (3 cells equal).
- **Fable's design reading holds.** DataFusion fixes a function's settings when the physical plan is built, and a held
  `DataFrame` carries its creation `SessionState`. A held frame **collected directly** matches Spark in every ANSI and
  zone cell but `current_timezone()` on a frame first acted on after the `SET` (6 cells).
- **Corrected: the held door is not gap-free.** A held frame *derived* after the `SET` through a facade scratch view
  follows the reader: 22 of 36 derivation cells (`selectExpr`, a conditioned join, `crossJoin`, a global aggregate with
  a literal, `explode`, `unpivot`, `intersect`, `subtract`, `cache()`, `createOrReplaceTempView` and `registerTempTable`
  after the `SET`), all silent. Held-frame writes go the same way (`writer_layout.py:95-121`): 16 wrong writes from
  held frames.
- **A third seat:** `register_collected_memtable` plans every materialisation (`cache()`, `persist`, `CACHE TABLE`,
  `materialize_as_temp_view`) under the live session, not the frame's own state (`temp_views.rs:146-168`).
- **Iceberg catalog views follow the reader on both engines (64 cells, 0 view gaps); leave them alone.**

## 2. Spark's rule, read from the 4.1.2 jar and measured

Read with `javap -c -p` from `spark-catalyst_2.13-4.1.2.jar` and `spark-sql_2.13-4.1.2.jar` (excerpts:
[evidence/jar/](evidence/jar/map.md)):

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

**What the measurement adds** (domain table §1, §7): the captured settings govern **analysis** of the body only. Spark
inlines the analysed body into the reader's plan, so whatever reads `SQLConf.get` later follows the reader:
- `current_timezone()`: **`ComputeCurrentTime`** (optimizer) replaces `CurrentTimeZone` with a literal of the
  optimizer's `conf.sessionLocalTimeZone` (`ComputeCurrentTime$.apply`; the `timezone$1` field;
  [evidence/jar/javap-current-time.txt](evidence/jar/map.md)). Step 0 credited `ReplaceCurrentLike`; its match arms
  are `CurrentDatabase`, `CurrentCatalog` and `CurrentUser` only. **Corrected.**
- The same rule folds `CurrentDate` and `LocalTimestamp` with the **expression's** `zoneId`, which analysis fixed under
  the captured zone, so `current_date()`, `localtimestamp()` and the zone of `hour(now())` follow **creation** in a
  view (domain table §7.5).
- `timeParserPolicy`: the parser is built lazily on first evaluation; an object evaluated before the `SET` keeps it.
- A held Dataset analyses at creation and optimizes at its first action. Spark's writers split: `writeTo` re-analyses
  the parsed plan (the zone follows the writer; the parse-time ANSI modes of `+`, `/` and casts stay; `avg`'s follows
  the writer); `write.parquet`, `saveAsTable` and `insertInto` write the analysed plan (creation, domain table §7.8).
- A rename keeps the creation settings under both spellings, `ALTER VIEW` and `ALTER TABLE`; `CREATE OR REPLACE` and
  `ALTER VIEW … AS` re-capture; `RESET` and `spark.conf.set` change only the reader, as `SET` does.

## 3. The key inventory (detail in [inventory.md](inventory.md) §1)

**Capture:** ANSI and the session zone always (as Spark does), `caseSensitive` and the decimal legacy switch, and the
engine's `timestampType` carrier (no gap today; it costs nothing and is right once the runtime value reaches the
engine). The catalog and namespace are already captured (`TempViewDefinition.catalog`, `.namespace`; 14 cells equal).
**Follow the reader:** `current_timezone()`, `timeParserPolicy`, `spark.sql.defaultCatalog` (`current_catalog()` in a
view follows the reader on both engines), the `datafusion.*` keys, the write keys. Readers: ANSI 7 analyzer rules on
main (`aggregate.rs:301` landed with #1355) and 24 UDF call sites; the zone 14 UDF call sites, 6 planning-time readers
and 4 router readers; case sensitivity at SQL planning (`repark-spark/src/lib.rs:95`).

## 4. The mechanism, tested against DataFusion 54.1.0 (cargo registry sources)

- **Settings are fixed at physical planning — confirmed.** `create_physical_expr` gives each `ScalarFunctionExpr` the
  planning state's `execution_props.config_options` (`datafusion-physical-expr-54.1.0/src/planner.rs:342-351`); the
  expression keeps them (`scalar_function.rs:57`) and hands them to every call (`:260`) through
  `ScalarFunctionArgs::config_options` (`datafusion-expr-54.1.0/src/udf.rs:425`). The physical planner passes
  `session_state.execution_props()` (`datafusion-54.1.0/src/physical_planner.rs:298`, `:653`). Analyzer rules get
  `&ConfigOptions` from the same state (`session_state.rs:666-738`).
- **A held `DataFrame` keeps its creation state — confirmed.** `SessionContext::state()` clones the shared state and
  marks it (`execution/context/mod.rs:2036-2039`); `DataFrame::new` boxes that clone (`dataframe/mod.rs:258-262`);
  `create_physical_plan` and `task_ctx` use it (`:301-302`, `:1557-1559`); `into_parts` hands both back (`:1673`).
- **How a view's plan enters:** `LogicalPlanBuilder::scan` inlines `get_logical_plan()` at plan-build time
  (`datafusion-expr-54.1.0/src/logical_plan/builder.rs:517-540`, only when the scan has no filters);
  `DataFrame::into_view` is a provider that returns its plan there (`dataframe/mod.rs:2643-2652`). `ViewTable::scan`
  puts pushed filters, the projection and the limit on the stored plan and calls `state.create_physical_plan`, which
  analyses and optimizes again under the **reader's** state (`datafusion-catalog-54.1.0/src/view.rs:109-150`,
  `session_state.rs:752-760`). RePark's SQL view stays a closed `TableScan` (`explain-main.txt`: `TableScan: ?table?`).
- **Why the DataFrame door keeps division's creation mode** (Fable did not trace it): the registered plan already
  carries the creation-time rewrite and the reader's `SparkExprSemantics` (`analyzer.rs:22-27`, `:204-233`) wraps it
  again; the inner guard decides. Execution-time readers follow the reader.

**The four traps:**

1. **`now`, the refresh and the current-time functions.** `SessionState::mark_start_execution`
   (`session_state.rs:856-858`) calls `ExecutionProps::mark_start_execution`
   (`datafusion-expr-54.1.0/src/execution_props.rs:116-119`), which restamps `query_execution_start_time` and the alias
   generator while it installs the options. `config_options` is a public field (`execution_props.rs:64`), so the build
   sets it directly on a clone of the reader's state and never calls `mark_start_execution` or
   `SessionContext::state()` for the body's physical planning. Under that overlay, `current_date()` folds in its
   `simplify` with the overlay's zone and the reader's start time (`repark-functions/src/current_date.rs:112-116`),
   and `localtimestamp()` and `hour(…)` read the overlay's zone at execution (`temporal_ctor/arith.rs:397-399`), so all
   three follow creation as on Spark without a rule of their own. Pins: `ct = current_timestamp()` across the view
   boundary is `true` (4 cells, equal today); the 12 current-time cells of domain table §7.5.
2. **Pushed filters keep the reader's semantics (revised; step 0's "filters on top" rule is withdrawn).** Spark pushes a
   reader filter below the view's projection, and each expression keeps the mode it was analysed under. Over a
   `SELECT b, 10 / b AS m` view created on and read off, `WHERE b <> 0` answers `5.0` on Spark and on RePark today; a
   body planned closed with the filter above it would raise. So on the differing path the reader's filters go into the
   body's **logical** plan, and the build gets the reader's semantics from four DataFusion facts:
   - **The filters that reach `scan` are already analysed by the reader.** `SessionState::optimize` runs the analyzer
     before the optimizer (`session_state.rs:732-738`), and `PushDownFilter` hands the provider post-analysis
     predicates (`datafusion-optimizer-54.1.0/src/push_down_filter.rs:1144-1205`, `supports_filters_pushdown` at
     `:1161-1163`). RePark's analysis-time ANSI rules write the mode into the expression (the guard function or
     `nullif`, `evidence/raw/explain-main.txt`), so the analysis-time half of a filter is the reader's.
   - **The body is analysed alone, then the filter goes on top, then the plan is only optimized.** The differing path
     calls `analyzer().execute_and_check` (`session_state.rs:628`) on the body under the overlay, adds the filter, the
     projection and the limit as `ViewTable::scan` builds them, then `optimizer().optimize` (`:633`) and
     `query_planner().create_physical_plan` (`:661`) — the sequence `register_collected_memtable` already uses
     (`repark-core/src/session/temp_views.rs:153-168`). It never calls `state.create_physical_plan`, which would
     analyse the reader's filter again under the captured settings.
   - **The optimizer moves the filter below the body's projection and substitutes the body's expressions**
     (`push_down_filter.rs:872-885`; `rewrite_projection`, `:1320-1373`; `replace_cols_by_name`, `:1360-1363`). A pushed
     `m > 1` becomes `(10 / b) > 1` with the body's own `10 / b`, so the body part keeps creation and the filter part
     keeps the reader: Spark's mixed answer.
   - **An execution-time reader inside the reader's own filter would take the overlay's options** when the combined
     plan is physically planned (`planner.rs:342-351`). So only **settings-free** conjuncts enter the body: columns,
     literals, comparison and boolean operators, `IS [NOT] NULL/TRUE/FALSE`, `IN` lists and `BETWEEN` over those, and
     casts between numeric types. Every other conjunct (any scalar function, arithmetic — RePark's integer `+` reads
     the flag at execution, `integer_spark.rs:367` — string, date or timestamp casts) is planned above the body under
     the reader's state, with the limit above it. DataFusion never pushes volatile predicates either (`:1155-1158`,
     `:1341`).
   - **The physical pushdown does not rescue a filter left above** — confirmed: `ProjectionExec` remaps a parent filter
     (`datafusion-physical-plan-54.1.0/src/projection.rs:383-411`) and reports pushed only if its child accepts it
     (`:413-420`, `if_all`); `FilterExec` keeps every unaccepted filter at its own place
     (`datafusion-physical-plan-54.1.0/src/filter.rs:637-700`). A `MemTable` or a parquet scan without row-level
     pushdown leaves it above the projection.
   - **Residual, loud only:** a deterministic filter selects the same rows above or below a projection unless the body
     raises on a row the filter drops, so a settings-sensitive reader conjunct can only turn a Spark answer into a
     raise. Measured: `WHERE b + 0 <> 0`, created on and read off, `5.0` on Spark (2 cells) — the rule above would
     raise. Owner question 11 asks how close the differing path must be.
   - Pins: 12 `filter_below`, 4 `filter_on_m`, 4 `LIMIT` and 4 `filter_sensitive` cells (domain table §7.1) and step
     0's 30 reader-side cells.
3. **Renames.** `renamed_temp_view` rebuilds `TempViewDefinition` field by field (`temp_view.rs:272-279`, `:288` for
   `definition_plan`); the new field must be copied there. Measured: `ALTER TABLE v RENAME TO w` answers as the SQL
   view does today; `ALTER VIEW v RENAME TO w` of a temp view **refuses** on RePark (`TABLE_OR_VIEW_NOT_FOUND`, also
   with no `SET`: a direct gap, §6). Step 0's 54 rename steps renamed with `ALTER TABLE` on RePark (49; 5 had no view
   because creation was refused) and `ALTER VIEW` on Spark; Spark answers the same under both spellings (2 cells), so
   those cells stand as the `ALTER TABLE` door.
4. **Nested views.** Each `ReplanningTempView` lays its own settings over the state it is given, so an inner view
   keeps its own when planned inside an outer view's body. A DataFrame view inside a SQL view is decided while the
   outer body is planned under the outer's captured settings, which is not the live session: Part 2's decision must
   read the planning state, not the live session (§5). Pins: 32 nested cells, 12 wrong today.

## 5. Seats

### Part 1 — SQL `CREATE TEMP VIEW` (seat: `ReplanningTempView::scan_replanned`, `temp_view.rs:115-144`)

- **1A (recommended): overlay at the scan.** `TempViewDefinition` (`:30-37`) gains the captured settings, taken at
  `execute_create_temp_view` (`execute.rs:118-141`) from the planning state of the `CREATE` statement, on every
  `CREATE OR REPLACE` too (domain table §7.6). In `scan_replanned`, when the captured settings equal both the reader's
  (`state.config_options()`) and the live session's, run today's code unchanged. Otherwise:
  1. clone the reader's `SessionState` and write the captured values into its options and into
     `execution_props.config_options` (trap 1);
  2. plan the stored text through `SessionContext::new_with_state` (`context/mod.rs:368`) so router-time readers (case
     folding, the literal) see them, and analyse the body alone (`analyzer().execute_and_check`);
  3. rewrite **only** `current_timezone()` to the reader's zone: walk the analysed body with
     `LogicalPlan::transform_up_with_subqueries` (`datafusion-expr-54.1.0/src/logical_plan/tree_node.rs:777`) and
     `map_expressions` (`:494`), and replace each call of RePark's `CurrentTimezone`
     (`repark-functions/src/session_time_zone.rs:244-271`, which reads `args.config_options` at `:266`) with a Utf8
     literal of `session_time_zone_display_from_options(state.config_options())`, aliased to the call's name.
     `current_date()`, `localtimestamp()` and `now()` are left alone (trap 1);
  4. add the settings-free pushed conjuncts, the projection and, when every conjunct went in, the limit (trap 2);
     optimize; physical-plan under the overlay;
  5. plan the settings-sensitive conjuncts and the limit above it under the reader's state.
  The "equal" test also fixes the `spark.table("v")` mixed case: a held reader whose snapshot differs from the live
  session no longer plans the text under the live session.
  - **Cost:** when settings match, the plan is today's plan (the view was already a closed scan) plus one comparison
    of a few fields per scan.
  - **Risk:** the overlaid state must keep the reader's catalog list and the view-expansion guard
    (`self.catalogs.view_expansion_guard()`, `:122`), and `refuse_dropped_dependency` and
    `conform_to_creation_schema` (`:133`, `:141`) run as today. `definition_plan` (`temp_view.rs:179`, copied at `:288`)
    stays planned under the creation settings; its readers expect that (inventory row 18).
- **1B: store the planned body, as the DataFrame door does.** Keep the creation-time `DataFrame` and plan it at scan
  under its own snapshot. Simpler (no SQL planning under an overlay), but it stops re-planning the text, so dropped
  or changed dependencies stop being re-resolved (Spark re-analyses the text under the captured settings), and it
  turns the SQL door into the DataFrame door's double-analysis. Not recommended.
- **Keys:** §3's capture list. **Catalog views and the native door:** untouched — catalog views are `ViewTable`s
  built in `view_ddl/read.rs:147`, and the native door does not build a `ReplanningTempView`.
- **Cases it closes:** the SQL view, SQL over SQL, DataFrame over SQL (the inner level), the `ALTER TABLE` rename,
  `spark.table` over a SQL view (except `current_timezone()` on a fresh held frame), the SQL-door writes, the unset
  case on the SQL door, and from this revision `filter_on_m`, the SQL-view caches (the cached frame and the read both
  plan the view under its captured settings), `CREATE OR REPLACE`, the self-join, `RESET`, `spark.conf.set` and the
  current-time functions on the SQL door: **375 cells (352 + 23), 42 of them wrong writes** (`CREATE TABLE … AS
  SELECT`, `INSERT … SELECT`, `spark.table(v).writeTo`, 14 each).

### Part 2 — DataFrame `createOrReplaceTempView` (seat: `temp_views.rs:42-44`, `frame.clone().into_view()`)

Capture the settings from the **frame's own state** (`DataFrame::into_parts`, `dataframe/mod.rs:1673`, or
`task_ctx().session_config()`), not the session's at registration. That is what makes the scratch-view doors —
the write doors (inventory rows 7-9) and the derivations (row 20) — and a frame registered after a `SET` behave as
Spark's analysed plan does.

- **2A (recommended): a marker at inline time, decided by the analyzer.** The provider's `get_logical_plan()` returns
  the frame's plan wrapped in a small extension node carrying the captured settings; a first analyzer rule compares
  the node's settings with the analyzer's `&ConfigOptions` (`AnalyzerRule::analyze`,
  `datafusion-optimizer-54.1.0/src/analyzer/mod.rs:58`) — the state that is really planning, which inside a SQL
  view is that view's captured settings — and either unwraps it (today's plan exactly) or replaces it with a closed
  `TableScan` over a provider that analyses the frame under its own state with the reader's `now`, rewrites
  `current_timezone()` to the reader's zone and takes the pushed filters by trap 2's rule.
  - **How the provider sees the reader's settings: it does not; the analyzer does.** This is the decision Fable left
    to the sketch.
  - **Rule order (checked).** DataFusion runs `ApplyFunctionRewrites` before every analyzer rule, but only when a
    function rewrite is registered (`analyzer/mod.rs:137-148`). None is: `with_default_features`
    (`datafusion-54.1.0/src/execution/session_state.rs:1165-1207`) registers none, DataFusion 54.1.0 has no
    `FunctionRewrite` implementation outside tests, and RePark registers none (`grep -rn
    "FunctionRewrite\|register_function_rewrite" crates`: no hit). So the marker rule is first when it is inserted at
    index 0 **after** `extension.configure_analyzer_rules` and `dialect.configure_analyzer_rules`
    (`repark-core/src/session/df_guards.rs:170-172`), which may prepend. Rules added after the build
    (`df_guards.rs:206-211`, `SparkExprSemantics`) run after it. The build pins both:
    `state.analyzer().function_rewrites()` (`analyzer/mod.rs:112`) is empty, and the marker rule is at index 0.
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
- **The write doors split as Spark's do (owner question 4):** `write.parquet`, `saveAsTable`, `insertInto` (and the
  unmeasured S3 paths and `MERGE` source) take the frame's captured settings; `writeTo` takes the captured ANSI and
  the writer's zone, which is Spark's re-analysis of the parsed plan.
- **Cases it closes:** the DataFrame view, SQL over DataFrame, the nested levels created on this door, the held-frame
  write doors, `CREATE TABLE … AS SELECT … FROM df_view`, and from this revision the held derivations (22 cells),
  `registerTempTable` (2), and the DataFrame-door current-time, replace, self-join, `RESET` and `conf.set` cells:
  **195 cells (156 + 39), 25 of them wrong writes** (`CREATE TABLE … AS SELECT … FROM df_view` 9, held
  `write.parquet` 9, held `writeTo` 3, held `saveAsTable` 2, held `insertInto` 2).

### Seat 3 — materialisation (`register_collected_memtable`, `temp_views.rs:146-168`)

`df.cache()`, `persist`, `CACHE TABLE`, `materialize_as_temp_view` and the facade's materialised scratch tables all
reach `register_collected_memtable`, which analyses, optimizes and physically plans the frame under the **live**
`self.context().state()` (`:153-168`). **Confirmed** (domain table §7.3: a held frame cached after the `SET` follows
the reader, 2 of 2 wrong).

- **3A (recommended): plan under the frame's own state.** Take the state from `frame.into_parts()`
  (`dataframe/mod.rs:1673`) and run the same three calls (`analyzer()`, `optimizer()`, `query_planner()`) on it; the
  catalog list is shared, so the frame still sees the session's tables. The matching case is today's plan: the
  frame's state then equals the live one.
- **3B: keep the live state and re-wrap.** Register the frame through Part 2's marker first and materialise the
  scan; one path for every door, but it pays the marker on every `cache()`. Not recommended.
- It belongs to Part 2 (the same principle: capture from the frame). `CACHE TABLE`'s own frame
  (`catalog_surface.py:530-558`, `inner.sql_built("SELECT * FROM v")`) is planned under the live session over the
  view, so with Parts 1 and 2 it materialises under the view's captured settings; a SQL read after it does not use the
  cached frame (`_cached_tables` is never consulted by a SQL read) but re-plans the view, which then also follows the
  captured settings. By that reading the 6 cache cells of §7.2 close without cache substitution; the build pins them.
  Substitution is cache honesty, not settings (owner question 8).

### The cost cases each build runs (matching path held to 1.05x, ruled; differing path measured and reported)

Part 1 on SQL views, Part 2 on DataFrame views and the facade scratch views; every matching case also pins `EXPLAIN`
identity with today's plan, not only its timing.

1. A view read with matching settings.
2. A filter pushed through such a view.
3. A join over a view (a DataFrame view for Part 2).
4. A view over a view.
5. A query with **no** view (for Part 2 it pays the new analyzer rule's walk).
6. C-012, at most 1.05x on held rows.
7. For Part 2, the facade scratch-view operations with matching settings: `selectExpr`, a conditioned join, `unpivot`,
   a set operation, `explode` in `select`, a global aggregate with a literal, and the write doors through
   `run_through_temp_view` (`saveAsTable`, `insertInto`, `writeTo`, `write.parquet`). These carry more traffic through
   seat 2A than user views do.
8. For Part 2, `cache()` of a frame (seat 3).
9. The differing-settings path, measured and reported (ruled).

## 6. The unit split

**Confirmed: two parts, each its own unit under the one-round cap,** because the seats are separate and each closes
its own doors with its own pins: **Part 1** (375 cells; seat 1A), then **Part 2** (195 cells; seats 2A and 3A).
Changes to Fable's split:

- Part 2's scope grows to the scratch-view write doors, the scratch-view derivations and the materialisation seat, and
  it must capture the frame's settings; its bar includes the 25 DataFrame-door and held-frame wrong writes.
- Part 1 must land first or with Part 2's nested pins skipped: 2A's nested decision reads the outer view's captured
  settings, which only Part 1 installs.
- **Outside both parts (no seat here):** `current_timezone()` in a held frame first acted on after a zone `SET`
  (6 cells; the frame's snapshot fixes it at creation, Spark at first action). The direct gaps of §6 of the domain
  table (owner questions 6 and 10).
- **Every wrong write, by part:** Part 1 — `CREATE TABLE … AS SELECT … FROM sql_view` (14), `INSERT INTO … SELECT …
  FROM sql_view` (14), `spark.table(sql_view).writeTo(…).create()` (14); Part 2 — `CREATE TABLE … AS SELECT … FROM
  df_view` (9), held `write.parquet` (9), held `writeTo(…).create()` (3), held `saveAsTable` (2), held `insertInto`
  (2). Unmeasured doors on the same seats: S3 paths, `MERGE` with a frame source.

## 7. Owner questions

**Ruled** (owner, 2026-11-17, verbatim "Defaults, merge 1355", adopting Fable's defaults):

1. **Severity: S1 by class.** Ruled.
2. **Label: the time-zone half blocks the label ICE-V2-FLAT-AWS-LABEL-1, which is scoped to ANSI on.** Ruled.
3. **The cost gate: only the matching-settings path is held to 1.05x; the differing path is measured and reported.**
   Ruled.

**Open** (each with a default and the cells it rests on):

4. **Spark's write doors for a held frame (restated; step 0's default contradicted question 2).** Step 0's default,
   "follow creation on every write door", would turn 6 held `writeTo` zone cells that are equal today into silent wrong
   answers with ANSI on, inside the label's scope (the other 2 of the 8 are the `TIMESTAMP '…'` literal, creation on
   both engines and under any choice). **Default: follow Spark door by door** — `write.parquet`, `saveAsTable`,
   `insertInto`, the S3 paths and a `MERGE` source take the frame's captured settings (10 zone cells wrong today
   become right: 6 `write.parquet`, 2 `saveAsTable`, 2 `insertInto`); `writeTo` takes the captured ANSI and the writer's zone (the 8 zone cells stay equal, the 2 `+` cells become
   right), and the 2 `avg` `writeTo` cells on #1355's head are recorded as divergences (one equal today becomes a loud
   raise, one silent today stays; Spark binds `avg`'s mode in the writer's re-analysis, and each has the frame or the
   writer on ANSI off, outside the label's scope). Cells: domain
   table §7.8, §4. Alternative: keep `writeTo` on today's path (the writer's settings for every key).
5. **`current_timezone()` on a held frame** (6 cells, outside both parts). Default: card it at S3 and do not build it
   in this unit.
6. **`element_at(array, 5)` and `array[5]` with ANSI on** answer NULL where Spark raises. They are already carded:
   D10 of [NESTED-EXPRESSION-ANSWERS-1](../../roadmap/mid-term/nested-expression-answers-1-card-2026-11-05.md) (S2 in
   the nested rule; the card's 2026-11-17 append records Fable's `element_at` cell). No new card. **Question: do the new
   cells re-rate D10?** Default: no — they are D10's own shapes on direct queries (4 cells, domain table §6), and a view
   carries the same answer. The rest of §6 stays with its existing cards.
7. **VIEW-ANSI-CAPTURE-1's `sequence(1, '3')` body** cannot show the view gap while RePark refuses it directly.
   Default: pin its view cells after the `sequence` coercion is fixed, not in this unit.
8. **Cache substitution** (SQL reads of a `CACHE TABLE`d view re-plan it instead of reading the cached rows,
   `catalog_surface.py:530-558`). Default: **not in this unit; card it separately** as cache honesty. Cells: the 6
   cache cells (domain table §7.2) close through Parts 1 and 2 and seat 3 without substitution (a reading, pinned by
   the build); substitution still differs from Spark when the data or a non-deterministic body changes after the
   cache.
9. **The held derivations in Part 2's bar.** Default: **yes** — the 22 derivation cells, the 2 `registerTempTable`
   cells and the 4 new held-frame write cells go into Part 2's bar, with seat 3 for the 2 `cache()` cells. 11 of the
   22 are zone cells with ANSI on, inside the label's scope. Cells: domain table §7.3, §7.8.
10. **Carding the direct gaps** `ALTER VIEW <temp> RENAME TO` (refuses `TABLE_OR_VIEW_NOT_FOUND`, also with no `SET`),
    `ALTER VIEW <temp> AS …` (refused by design, `ALTER_VIEW_AS_REFUSAL`) and `CACHE TABLE … AS SELECT` (`Unsupported
    SQL statement`). Default: **one card, S3** (loud refusals; Spark answers), proposed as
    TEMP-VIEW-ALTER-AND-CACHE-AS-1, beside the two narrower open cards
    [ALTER-VIEW-RENAME-OVER-TABLE-1](../../roadmap/mid-term/alter-view-rename-over-table-1-card-2026-11-02.md) and
    [ALTER-VIEW-DIGIT-NAME-1](../../roadmap/mid-term/alter-view-digit-name-1-card-2026-11-02.md). When built,
    `ALTER VIEW … AS` must re-capture the settings and the rename must copy them (the cells show Spark does both).
    Cells: domain table §7.4 (5 cells).
11. **How close to Spark the filter handling must be on the differing path.** Default: **trap 2's rule** —
    settings-free conjuncts are pushed into the body, settings-sensitive conjuncts and the limit stay above under the
    reader's state; the residual is loud only (2 `filter_sensitive` cells raise where Spark answers `5.0`). The
    alternative, closest to Spark: wrap each settings-reading call of the reader's filter in a delegating
    `ScalarUDFImpl` that replaces `ScalarFunctionArgs::config_options` (`udf.rs:425`) with the reader's options and
    never simplifies, so every conjunct and the limit can enter the body; it costs a wrapper per call on the differing
    path only, and the wrapper must delegate the function's identity (`equals`, `return_field_from_args`, the name in
    `EXPLAIN`). Cells: domain table §7.1.

## 8. What the brief, Fable's reply and step 0 got wrong, or left open

- Seven analyzer-time ANSI readers is the count with #1355's `aggregate.rs:301`; step 0's main `c22d61a1` had six, and
  main `6f80793b` has seven. The execution-time count is **24**, not about 25.
- **"The held DataFrame matches Spark in all 40 cells"** holds for collect only; Fable's held frames were read before
  the `SET`, which also hid Spark's first-action rule for `current_timezone()`. Held frames diverge on write and on
  every derivation through a facade scratch view (§1).
- **"A map subscript"** as an ANSI body: `map['missing']` and `element_at(map, 'missing')` answer NULL in both modes on
  Spark 4.1.2. The ANSI-gated subscript is the nested (struct) key one (`map_subscript.rs:106`), measured here.
- **"When the settings differ, the view should decline them"** (pushed filters): the provider has no session at
  `supports_filters_pushdown`. Step 0 put them above the body instead, which the critic refuted; trap 2's revised rule
  pushes the settings-free ones into the body's logical plan.
- The Python door is `crates/repark-python/src/session.rs:362-372`, not `:355-372`.
- Step 0 said `temp_views.rs:49-74` (cache views) are not seats because nothing is planned at read. The read is not a
  seat, but the materialisation is: seat 3.
- Step 0 said `current_timezone()` escapes through `ReplaceCurrentLike`; it is `ComputeCurrentTime` (§2), which also
  makes `current_date()`, `localtimestamp()` and `now()`'s zone follow creation.
- Step 0 said the `ALTER VIEW` rename works on both engines; it refuses on RePark (§4 trap 3).
- Fable did not read the native door, case sensitivity or other keys; step 0 read case sensitivity, the decimal
  legacy switch, `timeParserPolicy`, `timestampType` and the namespace, this revision `spark.sql.defaultCatalog`; the
  native door stays out with the reason above.

## 9. The critic's verdict, item by item (re-measured)

1. **The filter rule (UNSOUND).** Agreed: 12 `filter_below` cells equal on both engines today (`5.0`, `7`), and the
   physical pushdown does not move a filter below a projection whose source refuses it (`projection.rs:383-420`,
   `filter.rs:637-700`). Added: `filter_on_m` off→on (the SQL door raises where Spark answers `5.0`), `LIMIT` off→on,
   and the `filter_sensitive` cells that show the residual of the revised rule.
2. **Seat 3.** Agreed (`temp_views.rs:146-168` plans under `self.context().state()`; 2 of 2 held `cache()` cells wrong).
3. **Cache.** Agreed on the cells (6 of 6 SQL and DataFrame cache cells wrong, `isCached` `true`; the held frame
   cached before the `SET` is equal). Differs on the consequence: the settings seats close them without substitution
   (a reading), so substitution is its own question.
4. **The held door.** Agreed: the critic's 12 operations give 10 of 24 wrong, as it counted; six more operations give
   22 of 36. Agreed that `registerTempTable` is a user door. Corrected the inventory line for `selectExpr`: it reaches
   its scratch view through `filter_quote._select_expr_frame` (`core.py:1930`), not one of the six lines.
5. **Renames.** Agreed that `ALTER VIEW` refuses; the refusal is returned by `alter_view_missing_target`
   (`execute.rs:298-315`, reached at `:242` because the temp view is not a catalog view), not by the `RenameTo` arm at
   `:266`, which only a catalog view reaches. Step 0's raw data fell back to `ALTER TABLE` in 49 of the 54 steps; the
   other 5 had no view (creation refused on RePark).
6. **Current-time functions.** Agreed (`ComputeCurrentTime`, read in the jar; 8 of 8 discriminating cells wrong). The
   critic measured the date cell with a third zone; this revision keeps to the two test zones, where the date differs
   only between 00:00 and 08:00 UTC, so its 4 date cells did not discriminate at run time; the mechanism
   (`current_date.rs:112-116`) carries it, and the build re-runs it inside that window.
7. **Missing inventory rows.** Agreed on all; `spark.sql.defaultCatalog` is now measured discriminating (the critic's
   cell set it to its own default value) and stays equal.
8. **Missed shapes.** Agreed: `CREATE OR REPLACE`, the self-join, `RESET` and `spark.conf.set` all follow the reader on
   RePark and creation (or the replacement's settings) on Spark, except where the DataFrame door's division accident
   matches (16 of 20 wrong).
9. **Rule order.** Agreed that `ApplyFunctionRewrites` would run first; differs on the consequence: none is
   registered, so the seat stays (§5, 2A).
10. **Cost cases.** Agreed; added (§5).
11. **Owner questions.** Agreed on the Q4/Q2 conflict, but the measured count is 6 cells, not 8 (the 2 literal cells
    stay equal under any choice); Q4 is restated with a default that keeps them right. Q6 now cites D10. Questions
    8 to 11 are the critic's (a) to (d).

Up: [map.md](map.md).
