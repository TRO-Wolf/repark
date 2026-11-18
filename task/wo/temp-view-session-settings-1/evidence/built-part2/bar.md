# TEMP-VIEW-SESSION-SETTINGS-1 part 2 — the bar (delta 3)

**Head:** product commit `9aa2f182` (delta 3, after the merge of main `6286ba19`), package `pkg-view2-9aa2f182`
(`BUILT_FROM` `9aa2f18253b9b31bb1a529c3039b52ce6e31c7eb`); delta 3's section is below. Before it: product commit `9d71e030`
(`74c22dfe`, delta 1 `513208ce`, delta 2), package `pkg-view2-9d71e030`
(`BUILT_FROM` `9d71e03057524debfbb9f60554fa12fffe7da98c`). **Against:** main before part 1 (`pre`,
`pkg-avg-27d6149f`), main with part 1 (`part1`, `pkg-view1-c54f5fc7`), delta 1's head (`513208ce`) and Spark 4.1.2.
Delta 2 re-ran every RePark set fresh on the head, `part1` and `pre` at 17:05–17:07 UTC; Spark's sets are delta 1's
(14:35–14:47 UTC) except the current-time set (`rev.py`), re-run on all four at 17:06–17:07 so its hours pair. Pairing
[probes/bar2.py](probes/map.md); cells [raw/](raw/map.md), delta 2's in [raw/delta2/](raw/map.md).

## The build's bar (3,004 cells: `run.py` both start modes, `rev.py`, `rc.py`, the delta 1 sweep)

| against | WRONG → EQUAL | WRONG → WRONG (changed) | EQUAL → RAISE | EQUAL → WRONG | matching-settings moves |
|---|---|---|---|---|---|
| part1 | 261 | 10 | 5 | 0 | 0 |
| pre | 622 | 33 | 9 | 0 | 0 |

Delta 2's head gives the same verdict as delta 1's in every one of the 3,004 cells (12 answers per build differ
from delta 1's files: current-time hours, paired within each run). The same counts as `74c22dfe`'s bar; delta 1 moved
no cell of it (the 12 cells whose answers differ from the
build's run are current-time hours, paired within each run). Outside part 2's scope: the sweep's 40 WRONG → EQUAL and 8
WRONG → WRONG (`date_add_where`, the same refusal text-prefixed), part 1's `nest/read/sqldf/utc/m2` WRONG → EQUAL; no
move in the held door, the catalog views or the direct queries. **Part 2's targets:** of the 665 part 2 cells EQUAL on
`74c22dfe` (the 196 of the step 0's target among them), none is lost. Part 2's non-EQUAL cells left: 213, the build's
classes (`raw/part2-left.jsonl`).

**The matching-settings claim, corrected (delta 2).** "0 matching-settings moves" and "`EXPLAIN` identical to main's"
covered reads only. Writes under matching settings were not measured, and `513208ce` wrote 216 cells that main and
Spark refuse (below). With delta 2, matching settings move nothing on either side: 0 moves in the bar's matching
cells, 0 in the write sweep's 3,100 cells, and the 22 + 7 matching plans of `explain10.py` and `explain_joins.py`
identical to main's (scratch paths and scratch-view names aside).

## Delta 2: writes under matching settings

**The finding (PR #1362's CI).** `test_store_ts_to_numeric_1.py::test_store_assignment_cells_replay_spark` failed on
both part 2 packages: with one session in `UTC` and no setting changed, `s.sql("SELECT 127 AS id, -NULL AS c")`
written into a `DATE` or `BOOLEAN` column through `writeTo(...).append()` or `insertInto` stored a NULL where main and
Spark refuse with `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` (Spark types `-NULL` as `DOUBLE`).

**The cause.** Not the write path taking the frame's analysed plan. The store-assignment check
(`repark-iceberg/src/write/negated_null_store.rs`, `column_origin`, `:79-143`, called from
`repark-spark/src/void_type/insert_source_types.rs:82` and `:151`, `update_cast.rs:166` and
`repark-iceberg/src/write/merge/insert.rs:130` and `:255`) re-plans the write's source from SQL text and walks the
unanalysed plan to find where a `NULL` column comes from; any node it does not know answers `Other`, and only
`Negated` refuses. DataFusion inlines a view's `get_logical_plan()` at plan-build time
(`datafusion-expr-54.1.0/src/logical_plan/builder.rs:517-540`), and the build's `FrameView::get_logical_plan`
(`frame_settings.rs:156-158` at `513208ce`) returned the `FrameSettings` marker for every reader, so the walk met a
`LogicalPlan::Extension` between the view's alias and the frame's projection and the refusal never ran. Every write
door reads its source through a scratch temp view (`_repark_writer_`, `_repark_writer_v2_`,
`__repark_merge_src_`) or a user's DataFrame view, so all of them lost it, and the differing path lost it too.

**The fix, inside the build's seven files** (`9d71e030`; the cap count stays 7). The marker stays only where the
analyzer alone knows the planning settings: a SQL view's body, re-planned by `ReplanningTempView`
(`temp_view.rs:147` and `:168`, each wrapped in `frame_settings.rs` `view_body`, a poll-scoped depth counter).
Anywhere else a registered frame view decides when it is inlined (`frame_settings.rs` `FrameView::inlined_for_reader`),
against the session's live options read through a weak `LiveOptions` handle (`repark-core/src/extension.rs`,
built in `session/temp_views.rs` from the context's state, `try_read_recursive`): its own plan when they match (the
plan `into_view` gave, so every pre-analysis walker sees main's plan), the closed `TableScan` the analyzer rule would
have built when they differ (the walker reads its analysed plan through `get_logical_plan`), the marker when no live
reader is present. A frame inlined at the top level is planned under the live options: a SQL statement plans and
analyses under one state, and a held `DataFrame` analyses under the snapshot taken when it was built. Clippy's
`single_match` at `frame_settings.rs:174` is now an `if let`.

**The write sweep** ([probes/wsweep.py](probes/map.md), [raw/delta2/wsweep.jsonl](raw/map.md)): one session per
build and ANSI mode, settings never changed; 13 doors (`writeTo` `append`, `overwrite`, `overwritePartitions`,
`create`, `createOrReplace`; `insertInto` and its overwrite; `saveAsTable` append and overwrite; `mergeInto`; SQL
`INSERT` and `MERGE` from a DataFrame view; `write.parquet`); 5 sources (a bare literal, a derived subquery, a frame
built with `select` and `withColumn`, a frame over a temp view, a `createDataFrame` frame) of 5 values (`1`, `NULL`,
`-NULL`, `CAST(NULL AS <type>)`, `-1.5D`); into `DATE`, `BOOLEAN`, `BIGINT`, `DECIMAL(10,2)`, `TIMESTAMP` and `STRING`
(the source-typed doors once per source): 1,550 cells per mode. The S3 paths were not measured (no object store on
this box); they read their source through the same scratch view.

| build | cells | differ from main (`part1`) | against Spark (6 doors, 1,400 cells) |
|---|---|---|---|
| `pre` (`pkg-avg-27d6149f`) | 3,100 | 0 | — |
| `part1` (`pkg-view1-c54f5fc7`) | 3,100 | — | 1,376 EQUAL, 24 direct gaps |
| delta 1 (`pkg-view2-513208ce`) | 3,100 | 216 refused → wrote | 1,280 EQUAL (96 EQUAL → WRONG) |
| head (`pkg-view2-9d71e030`) | 3,100 | **0** | 1,376 EQUAL, the same 24 |

Delta 1's 216: `-NULL` into `DATE`, `BOOLEAN` and `TIMESTAMP` from the literal, derived, `select`/`withColumn` and
temp-view sources, on 9 doors (`append`, `overwrite`, `overwritePartitions`, `insertInto` and its overwrite,
`saveAsTable` append, `mergeInto`, SQL `INSERT` and `MERGE` from a view), both modes. The 24 direct gaps, identical on
main and the head: `writeTo(...).create()` of a `-NULL` column refuses (`Invalid type for c: unknown`) where Spark
writes `DOUBLE` (8); `write.parquet` of `NULL` and `-NULL` stores `int` where Spark stores `void` and `double` (16).
Spark ran `append`, `insertInto`, SQL `INSERT` from a view, `mergeInto`, `create` and `write.parquet`.

**The differing path.** The build's write cells (the 25 wrong writes and question 4's doors: `write/ctas-df`,
`parquet-held`, `writeto-held` and the rest, 112 cells) give the same verdicts as delta 1 (above). A `-NULL` frame made
under the other ANSI mode or `America/Los_Angeles` and written under the session's settings through `writeTo`,
`insertInto` or SQL `INSERT` from its view: delta 1 wrote, the head refuses as main does (Spark's type for `-NULL` and
its store-assignment rule read neither setting).

**Pins.** `test_temp_view_session_settings_2.py::test_a_negated_null_write_refuses_as_spark`, per ANSI mode and door
(`writeTo`, `insertInto`, SQL `INSERT` from a view), each over the bare and derived sources, `DATE` and `BOOLEAN`, a
frame made under matching settings, the other ANSI mode and the other zone (72 cells in 6 tests): head 6 passed;
`pkg-view2-513208ce` 6 failed; `pkg-view1-c54f5fc7` and `pkg-avg-27d6149f` 6 passed. The CI test
`test_store_ts_to_numeric_1.py` passes on the head. Rust (`view_ddl/frame_settings/tests.rs`):
`a_reader_under_the_frames_settings_inlines_the_frames_own_plan` (the inlined plan is the frame's own, and a view body
keeps the marker), `the_negated_null_store_check_reads_through_a_frame_view_on_both_paths`; `view_ddl::` 70 passed.

## Delta 3: the live read under a contended lock (the verifier's F1)

**The finding (the verifier's F1, class (a), S2; `pkg-view2-9d71e030` and the merged `pkg-view2-d4423a69`).** Delta 2's
live read (`session/temp_views.rs`, `try_read_recursive`) returns nothing while another thread holds the session
state's write lock, and `FrameView::inlined_for_reader` (`frame_settings.rs:201-203`) then inlined the
`FrameSettings` marker, which the store-assignment walker (`negated_null_store::column_origin`) cannot read. With two
threads looping `spark.conf.set` on keys outside the four captured settings, `-NULL` written into `DATE` and
`BOOLEAN` landed 44 and 32 times in 1,000 writes on `9d71e030` and 27 on `d4423a69`; main 0; the head with no setter
thread 0. Every `spark.conf.set` takes that write lock, and planning runs with the GIL released, so any second Python
thread could open the window.

**The owner's ruling (2026-11-18, about 20:25 UTC, verbatim):** "Go with your recommendation, dispatch 1362 delta 3".
The recommendation: fix F1 inside the build's seven product files, with the verifier's race probe as a test, and stop
if the fix needs an eighth.

**The remedy (`9aa2f182`, two of the seven files, cap 7).** A bounded wait with a last-known snapshot as its fallback:
- `repark-core/src/session/temp_views.rs`: the reader waits for the state lock with `try_read_recursive_for` for at
  most `LIVE_READ_WAIT` (one second) instead of failing at once.
- `repark-core/src/extension.rs`: `LiveOptions` keeps the options it last read, starting from the session's options
  when the view is created, and answers them when the wait expires or the session is gone. For a registered frame view
  `read` therefore always answers, and `inlined_for_reader` always chooses its own plan or the closed scan. The marker
  is left only inside a SQL view body (`view_body`, by design) and for a provider built with no reader (none in
  production).

Why this remedy: the wait gives the exact live answer whenever a writer releases, which the lock audit below shows
every writer does at once; the bound means even a future write-lock holder that planned on its own thread could cost
only one bounded wait, never a hang. A snapshot alone was not enough: with no wait, a contended read right after a
`SET` of a captured key would decide under the setting before it. The other remedy the verifier named,
`column_origin` walking through the marker's input, is in `repark-iceberg`, an eighth product file, and was not
needed.

**The lock audit.** Every taker of the state's write lock, each a pure in-memory update that plans nothing, awaits
nothing, calls no Python and takes no lock a planner holds:

| site | what it does under the lock |
|---|---|
| `repark-python/src/session_runtime.rs:232`, `:270`, `:295`, `:315`, `:335`, `:352`, `:375`, `:396`, `:414` (`spark.conf.set` / `unset` for the decimal-truncate flag, minimum precision, ANSI, case sensitivity, self-join, schema merge, overwrite mode, WAP, zone) | sets one carrier field; `:414` also swaps the runtime zone's `Arc` under its own lock, whose readers clone and release |
| `repark-core/src/session/write_options.rs:20`, `:33`, `:40` | sets the time-parser policy or an Iceberg session write key |
| `repark-core/src/session/path_write.rs:845` | registers a text-write path in a mutex-held map |
| `repark-core/src/session/spill.rs:258` | rebuilds the runtime env and the state from a clone |
| `repark-core/src/microbatch/driver.rs:331` | installs the streaming manager extension |
| `repark-core/src/spark_nullable.rs:78`, `session/text_write_format/file_format.rs:249`, `:253` | registers a file format |
| `repark-spark/src/use_ddl.rs:40`, `repark-iceberg/src/write/conform_stage.rs:114` | writes the session-defaults or stage-record extension |
| `repark-spark/src/extension.rs:181`, `repark-functions/src/integer_spark.rs:35`, `decimal_spark.rs:37` | registers an expression planner at build |
| `repark-core/src/microbatch/run.rs:409`, `repark-python/src/column/expr_build.rs:353` | a context of its own, not the session's |
| DataFusion `SessionContext` (`context/mod.rs`): `SET` / `RESET` (`:1125`, `:1157`), runtime variables (`:1182`, `:1225`), `PREPARE`'s store (`:761`, after the optimizer ran outside the lock), `CREATE CATALOG` (`:1044`), function, catalog-list and table-options registration | updates config, registries or the prepared-plan map; the only callback, `with_updated_config` (`instant_ts.rs:227`), builds a UDF |

No thread holds the write lock while it plans, so a read that waits never deadlocks; the facade plans with the GIL
released, and no writer waits for the GIL while it holds the lock.

**Tests (red on the pre-fix code, green on the head).**
- Rust (`view_ddl/frame_settings/tests.rs`): `a_held_state_write_lock_never_inlines_the_marker_outside_a_view_body`
  holds the write lock on another thread across the bound: the matching frame view inlines its own plan and the
  differing one the closed scan; `a_live_read_waits_for_a_writer_and_decides_under_its_setting` has the writer turn
  ANSI off before it releases: the read waits and closes the ANSI-on frame. With `extension.rs` and `temp_views.rs` at
  `d4423a69` both fail at their "no marker" assertion; with the fix, `view_ddl::` 72 passed.
  `the_marker_is_decided_under_the_planning_state_not_the_live_one` is now
  `inside_a_sql_view_body_the_marker_is_decided_under_the_planning_state`: outside a body the view inlines the closed
  scan; inside one the marker, which the rule unwraps under the frame's settings and closes under the reader's.
- Facade (`test_temp_view_session_settings_2.py::test_a_negated_null_write_refuses_while_another_thread_sets_the_conf`,
  from the verifier's `race.py`): two threads loop `spark.conf.set` on `caseSensitive` and the decimal-truncate flag
  beside six writer threads (`writeTo`, `insertInto` and SQL `INSERT` from a same-mode and an other-mode view, into
  `DATE` and `BOOLEAN`), 100 writes each; every write must refuse and both tables stay empty. It ran in about 4 s per call. On the head it passed 5 of 5 runs; on `pkg-view2-d4423a69` it failed 10 of 10, each with at least 17 to 33 writes landed across four or five of the six door and target cells; on main now (`pkg-main-6286ba19`) it passes, with delta 2's negated-null pins.

**Race and hang** ([probes/race.py](probes/map.md), the verifier's probe with a duration mode; outputs in
[raw/delta3/](raw/map.md)): five doors on five threads (`writeTo` and `insertInto` of a bare frame, `writeTo` from an
other-mode view, `insertInto` from a same-mode view, SQL `INSERT` from an other-mode view) beside two setter threads.

| build | run | writes | landed | rows stored | `conf.set` calls overlapped | slowest write (s) |
|---|---|---|---|---|---|---|
| `d4423a69` (merged delta 2) | 5 × 1,000 | 5,000 | **165** (`writeTo` 39, `insertInto` 42, `insertInto` from the same-mode view 36, SQL `INSERT` from the other-mode view 48, `writeTo` from the other-mode view 0) | 87 / 78 | 5.43 M | 0.40 |
| head `9aa2f182`, run 1 | 5 × 1,000 | 5,000 | **0** | 0 / 0 | 5.51 M | 1.02 |
| head `9aa2f182`, run 2 | 5 × 1,000 | 5,000 | **0** | 0 / 0 | 7.22 M | 0.53 |
| main now `6286ba19`, run 1 | 5 × 1,000 | 5,000 | 0 | 0 / 0 | 5.24 M | 0.44 |
| main now `6286ba19`, run 2 | 5 × 1,000 | 5,000 | 0 | 0 / 0 | 7.58 M | 0.47 |
| head, hang run: 75 s under `timeout 300` | 75 s | 15,132 | **0** | 0 / 0 | 10.19 M | 0.21 |
| head, hang run with the setters also flipping ANSI and the zone (the captured keys) | 75 s | 15,229 | **0** | 0 / 0 | 9.51 M | 0.25 |

Every write refused with `CANNOT_SAFELY_CAST`; no other error. Both 75-second runs ended on their own clock, well
inside the timeout: no deadlock and no hang. One write in the head's first run took 1.02 s; run 2 records every write
over 0.5 s (two, at 0.51 s and 0.53 s; main's slowest 0.47 s), and the hang runs' slowest is 0.25 s across 30,000
writes, so a contended read waits milliseconds, not the bound.

**The bar** (package `pkg-view2-9aa2f182`, `BUILT_FROM` `9aa2f182…`; main now `pkg-main-6286ba19`). Every check as delta 2's:
- **The write sweep** ([probes/wsweep.py](probes/map.md), 13 doors, 3,100 cells, both ANSI modes): the head and main
  now answer **the same in all 3,100 cells**.
- **The pins:** the part 1 and part 2 pin files, `test_temp_view_reader_lineage.py`, `test_store_ts_to_numeric_1.py`
  and DECIMAL-DIVIDE-LITERAL-1's `test_decimal_divide_literal_1.py`: 2,041 passed, 20 xfailed (DECIMAL-DIVIDE-LITERAL-1's
  standing xfails).
- **The build's bar** (3,004 cells, fresh on the head, `part1` and `pre` at 21:10–21:12 UTC; `rev.py` re-run on all four
  at 21:12 so its hours pair): **the same verdict as delta 2's in all 2,978 paired cells**, the same counts against
  part 1 (261 WRONG → EQUAL, 10 WRONG → WRONG (changed), 5 EQUAL → RAISE, 0 EQUAL → WRONG, 0 matching-settings
  moves) and against `pre` (622 / 33 / 9 / 0 / 0); only the 12 current-time hours differ in text, paired within the run.
- **The verifier's probes, Fable's probe and the reader, join and `smartCsv` sweeps** (1,478 cells): 0 cells EQUAL on
  part 1 and not on the head; against delta 2's head four answers differ, all artefacts (two of Fable's plans print
  this lane's partition count and the package path, two time-travel refusals the run's own timestamp).
- **`EXPLAIN` under matching settings:** `explain10.py` (22) and `explain_joins.py` (7) identical to main now's,
  scratch paths aside; the cost cases' plans in [cost.md](cost.md).

**The cap.** Delta 3 touched two of the build's seven product files (`repark-core/src/extension.rs` and
`repark-core/src/session/temp_views.rs`) and no other: **seven of seven**. Test and map files:
`frame_settings/tests.rs`, the part 2 pin file and four `map.md` rows; the two excepted maps keep their exact
baselines.

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
| Fable's `probe.py` | 14 | 0 answers moved; `EXPLAIN` differs only by the warehouse path and (delta 2) the lane's partition count | `jf.filter("n > 0")` and `jf.selectExpr("n + 1 AS n1", "ts")` back at Spark's answer; `hour(ts)` WRONG as on part 1 |

Delta 2 re-ran these sets with the reader, join and `smartCsv` sweeps (1,622 cells; the Postgres cells not run, with no
server up) on the head, `part1` and `pre` against Spark's delta 1 files ([probes/probes_cmp.py](probes/map.md)): 0
cells EQUAL on part 1 and not on the head. Against delta 1's head, 0 answers moved but four artefacts of the run: two
of Fable's plans print the partition count of this lane's ten cores, and two time-travel refusals print the run's
own timestamp.

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
TEMP-VIEW-FRAME-READER-PARSE-1's unit. **Ruled by the owner (2026-11-18, about 15:00 UTC, "Defaults on all three"):**
the head's behaviour is accepted, and any change folds into TEMP-VIEW-FRAME-READER-PARSE-1.

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
  the unit must reach. Its inventory is the table above; `smartCsv` folds into it (ruled, below).
- **Ruled by the owner (2026-11-18, about 15:00 UTC, verbatim: "Defaults on all three, extend the window 4 hours"):**
  TEMP-VIEW-FRAME-CROSSJOIN-1 (the exact-key join door and `crossJoin` analyse both held sides under the live state;
  silent; the left-anti gap below first) and HELD-FRAME-USING-JOIN-1 (`join_on_names` keeps only the left frame's
  state; silent): **S1, label-blocking**; HELD-FRAME-DERIVED-ANSI-1 (`expr_build.rs:187`, `withColumn` and
  `select(F.expr(...))` over a held ANSI frame): **S2**; LEFT-ANTI-PROJECTION-PUSHDOWN-1 (a left-anti or left-semi
  join evaluates the left projection on every row; loud): **S3**; `smartCsv`: **the head's behaviour is accepted**, and
  any change folds into TEMP-VIEW-FRAME-READER-PARSE-1.

## The cap

Task 1 used three of the build's seven product files (`frame_settings.rs`, `temp_settings.rs`, `temp_view.rs`).
Fable's guidance under the owner's delegation allowed an eighth, `crates/repark-python/src/frame_lineage.rs`, plus one
`extension.rs` helper, for task 2 only; it was used for the backed-out build and is not in the branch. Delta 2 touched
four of the seven (`repark-core/src/extension.rs`, `repark-core/src/session/temp_views.rs`, `frame_settings.rs`,
`temp_view.rs`) and no other product file: **seven of seven**. Delta 3 touched two of the seven
(`repark-core/src/extension.rs`, `repark-core/src/session/temp_views.rs`) and no other: **seven of seven**.

Up: [map.md](map.md).
