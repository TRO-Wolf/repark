# Card TEMP-VIEW-SESSION-SETTINGS-1: a temp view is analysed under the SQL settings of the session that reads it, where Spark keeps the creating session's

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the observations of the MAP-STRUCT-KEY-SUBSCRIPT-1 round-5 Opus verdict. The observations are not counted against that unit.

**Status:** step 0 PR #1356, revised after two critic rounds; S1 by class; the time-zone half blocks the label, the ANSI half does not (owner, 2026-11-17). Absorbs VIEW-ANSI-CAPTURE-1. Head equals base for the cells below.

**Why:** Spark stores the creating session's SQL configs with a temp view. RePark re-analyses the view under the setting of the session that reads it. The verdict finds that the subscript follows the door's other ANSI-gated rules here, so the gap belongs to the views. The card VIEW-ANSI-CAPTURE-1 names the same re-analysis for `sequence` and CAST; the cells below add a struct-key subscript and the arithmetic cells.

**Measured (Spark 4.1.2 against RePark on the head, the verdict's observations):**

```text
created under ANSI on, read under ANSI off:
  CREATE OR REPLACE TEMP VIEW v_on AS SELECT id, m[named_struct('j', 1, 'k', 2)] AS x FROM c
                                                       Spark [[1, 7], [2, None]]    RePark refuses (renamed struct key)
created under ANSI off, read under ANSI on:
  SELECT 1/0                                           Spark NULL                   RePark DIVIDE_BY_ZERO
  SELECT CAST(2147483647 AS INT) + 1                   Spark -2147483648            RePark ARITHMETIC_OVERFLOW
created under ANSI on, read under ANSI off:
  SELECT 1/0                                           Spark DIVIDE_BY_ZERO         RePark NULL
```

**Fix (for the orchestrator to grade):** analyse a temp view under the SQL settings it was created under, as Spark does.

**Retires:** the card when the view cells above answer as Spark does, pinned.

**Relation:** [VIEW-ANSI-CAPTURE-1](view-ansi-capture-1-card-2026-10-01.md) (the same re-analysis for `sequence` and CAST; whether the two are one gap is for the orchestrator to judge); [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (observations only). Up: [map.md](map.md).

## Dated append (2026-11-17, evening): the Fable consult, and the step 0

**The owner's words, 2026-11-17, evening (verbatim; no time given):**

1. "Lets get everything draw up for a fable agent"
2. "dispatch Opus xhigh effort levels for these fixes, then use Opus 5.5 medium verifiers for the time and not muse, at least until these things are closed out"

**The orchestrator's reading of message 2, not the owner's words:** "these fixes" are this card, both parts, including the time-zone half, and SUM-DEC-I128WRAP-1. Both are dispatched now as step 0s, ahead of the order. Builders run on Opus 5.5 at effort xhigh; verifiers and critics run on Opus 5.5 at effort medium, not Muse, until these units close.

**Spark's rule** (Fable's reading of the Spark 4.1.2 jar, by `javap`; Fable's reply, section 2):

- `CapturesConfig.sqlConfigsToProps` always captures the session time zone and the ANSI flag, set or not. It also captures every modifiable key the session has set, except the optimizer, codegen, execution, shuffle and adaptive prefixes and a few Hive keys.
- `View.effectiveSQLConf` builds a fresh configuration from the captured keys. A key that was not captured falls back to Spark's default, not to the reader's value.
- `ViewHelper.createTemporaryViewRelation` stores the analysed plan for the DataFrame door (no SQL text), or when `storeAnalyzedPlanForView` is set. Otherwise it stores the SQL text and the captured keys.
- Fable's reading: the verdict's wording, "stores the analyzed aggregate", holds for the DataFrame door only.

**Door table** (Fable's measurements on the head; main is the same except `avg`):

| body | SQL temp view | DataFrame temp view | held DataFrame |
|---|---|---|---|
| `10 / b` | reader's setting | creation, as Spark | creation, as Spark |
| `a + 1` | reader's | reader's | creation, as Spark |
| `avg(x)` | reader's | one way only (see below) | creation, as Spark |
| `hour`, `CAST(ts AS STRING)`, `CAST(ts AS DATE)`, `date_format`, text to timestamp | reader's | reader's | creation, as Spark |
| a `TIMESTAMP '…'` literal | reader's | creation, as Spark | creation, as Spark |
| `current_timezone()` | reader's, as Spark | reader's, as Spark | creation, as Spark |

Fable's statement, from the same probes: Spark uses the creation setting in every cell on both view doors, with one exception, `current_timezone()` inside a view, which follows the reader. The held DataFrame matches Spark in all 40 cells of these ten bodies (Fable's measurement).

**Time-zone table** (Fable's measurements; ANSI on throughout; view created under UTC and read under America/Los_Angeles, Fable's test values; the reverse direction differs the same way):

| view body | Spark | main and head |
|---|---|---|
| `hour(ts)` | 2 | 18 |
| `CAST(ts AS STRING)` | 2024-01-01 02:30:00 | 2023-12-31 18:30:00 |
| `CAST(ts AS DATE)` | 2024-01-01 | 2023-12-31 |
| `date_format(ts, 'yyyy-MM-dd HH')` | 2024-01-01 02 | 2023-12-31 18 |
| `unix_timestamp(CAST(s AS TIMESTAMP))` | 1704076200 | 1704105000 |

These differences are silent: different values, not a raise against a NULL (Fable's measurement).

**The `avg` cells from #1355** (SQL temp view, ANSI modes as created and read). The on-to-off and off-to-on rows are the verdict's V1 and V2; the other two rows are the consult's table.

| created → read | Spark 4.1.2 | main | head (#1355) |
|---|---|---|---|
| on → on | RAISE `ARITHMETIC_OVERFLOW` | RAISE `Arithmetic Overflow in AvgAccumulator` | RAISE `ARITHMETIC_OVERFLOW` |
| on → off | RAISE `ARITHMETIC_OVERFLOW` | RAISE `Arithmetic Overflow in AvgAccumulator` | NULL |
| off → off | NULL | RAISE (condition-less) | NULL |
| off → on | NULL | RAISE (condition-less) | RAISE `ARITHMETIC_OVERFLOW` |

Not reconciled: the consult's table calls main's on-to-off text condition-less; the verdict says same-mode reads agree everywhere, while the consult's off-to-off row has main raising where Spark answers NULL. The records do not settle these.

On the DataFrame door, the one-way cell (Fable's measurement on the head): created off and read on answers NULL, as Spark does; created on and read off answers NULL where Spark raises.

**Fable's recommendations** (recommendations, not rulings):

- **Severity:** S1 by class, a silent difference on the Spark door. The reach is narrow: one session must change a setting between creating a temp view and reading it.
- **Label:** the ANSI half does not block under A1, since a session that stays on ANSI on cannot meet it. The time-zone half sits inside the ANSI-on scope, and Fable leans to building it. The owner may scope it out, as A1 did for ANSI off. This is the owner's decision.
- **Place in the order:** after DATE-PART-SESSION-ZONE-1, beside it, because the zone oracle and probes carry over. Not ahead of DECIMAL-DIVIDE-LITERAL-1 or DOUBLE-DECIMAL-DATAFRAME-1.
- **Delivery, in two parts,** each its own unit under the one-round cap. Part 1, SQL `CREATE TEMP VIEW`: one seat, no change of plan shape. Part 2, DataFrame `createOrReplaceTempView`: an inline-or-closed switch and cost rows. Fable would hold only the matching path to the 1.05x gate. Whether the path taken when settings differ is held to it is the owner's decision.
- **Catalog views:** leave them as they are. Spark follows the reader for Iceberg catalog views (Fable's measurement, on Spark only).

Fable's fix shape and step 0 list are in section 5 of the reply, "Fable reply — AVG verifier F1 and view settings 2026-11-17.md" (outside the repository, named and not linked). They are not copied here.

## Dated append (2026-11-17, later): the rulings

**Owner, 2026-11-17, evening (no time given), verbatim "Defaults, merge 1355".** The owner adopted Fable's defaults: this unit is S1 by class; its time-zone half blocks ICE-V2-FLAT-AWS-LABEL-1 and its ANSI half does not; only the matching-settings path is held to the 1.05x gate, and the other path is measured and reported; and verifier briefs name a view read under settings other than its creation settings as an accepted class until this unit lands. The list is as given for the [rulings record](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section, not the owner's words.

**Owner, 2026-11-17, later the same evening, verbatim "defaults on both".** The owner adopted the step 0's defaults for questions 4 to 11 (the sketch's section 7 on PR #1356's branch, not on this branch):

- Q4, held-frame writes: follow Spark door by door. `writeTo` takes the creation ANSI setting and the writer's zone.
- Q5: `current_timezone()` on a fresh held frame is carded at S3, not in this unit: [HELD-FRAME-CURRENT-TIMEZONE-1](consult-2026-11-17/held-frame-current-timezone-1-card-2026-11-17.md).
- Q6: `element_at(array, 5)` and `array[5]` stay under D10 of [NESTED-EXPRESSION-ANSWERS-1](nested-expression-answers-1-card-2026-11-05.md). No re-rating.
- Q7: the `sequence(1, '3')` view cells are pinned after the `sequence` fix.
- Q8: cache substitution is not in this unit: [CACHE-HONESTY-1](consult-2026-11-17/cache-honesty-1-card-2026-11-17.md), not rated.
- Q9: part 2's bar takes the held derivations: 22 derivation cells, 2 `registerTempTable` cells, 4 held-write cells, and seat 3.
- Q10: the direct gaps get one S3 card: [TEMP-VIEW-ALTER-AND-CACHE-AS-1](consult-2026-11-17/temp-view-alter-and-cache-as-1-card-2026-11-17.md).
- Q11, option (a): the revised filter rule stands. The re-critic measured 7 loud cells among 36 differing-path view cells (of 44 filter cells), none silent; 6 of the 7 agree with Spark today and are ruled an accepted class. The wrapper alternative is carded as [VIEW-FILTER-SETTINGS-WRAPPER-1](consult-2026-11-17/view-filter-settings-wrapper-1-card-2026-11-17.md), not rated. Neither option closes the 2 AND-GUARD-ANSI-1 cells or the `(a+1)+0` cell.

The sketch's build pins from the round 2 critic are its section 10 (on PR #1356's branch).

## Built (part 1, the SQL door) — 2026-11-18

**Branch:** `fix/temp-view-session-settings-1-part1`, product and pins `12c0680c` (not pushed). **Builder:** Claude Opus 5.5
(`claude-opus-5-5`), effort xhigh.

**Rulings carried (owner, 2026-11-17, verbatim):** "dispatch Opus xhigh effort levels for these fixes, then use Opus 5.5
medium verifiers for the time and not muse, at least until these things are closed out"; "Defaults, merge 1355"; "defaults
on both" (question 11 (a): the revised filter rule, its 6 loud cells an accepted class, the wrapper carded as
VIEW-FILTER-SETTINGS-WRAPPER-1; question 7: no `sequence(1, '3')` view pins; question 10: the `ALTER VIEW` rename and `AS`
gaps carded as TEMP-VIEW-ALTER-AND-CACHE-AS-1; question 8: cache substitution not this unit's).

**Seat 1A at `ReplanningTempView::scan_replanned`.**
- `crates/repark-spark/src/view_ddl/temp_settings.rs` (new): `TempViewSettings` (ANSI, the session zone, the decimal
  legacy switch, the timestamp-type carrier); the overlay of the reader's state (its start time and catalogs, the
  captured values, the outermost reader's zone carried down a nest); the closed settings-free allow-list; the
  `current_timezone()` rewrite; the body planned under the overlay with the free conjuncts inside and the sensitive ones
  and the limit above it through the reader's `create_physical_expr`.
- `temp_view.rs`: the definition carries the settings; the matching test (captured equals the reader's and the live
  session's, and no outer reader zone differs) keeps today's code; renames copy the settings.
- `execute.rs`: the capture at `execute_create_temp_view`, on every `CREATE OR REPLACE`. One `mod` line in `mod.rs`.
- Three product files and one `mod` line (cap 5). `supports_filters_pushdown` is unchanged.
- **Two builder decisions, named for the verifier:** (1) a body that inlines a DataFrame view whose plan reads settings
  keeps today's path, because the overlay would otherwise re-plan that frame under the SQL view's settings (measured:
  an EQUAL → WRONG `nest-df-sql` cell and moved `vsd` cells, both part 2's); (2) `caseSensitive` is not captured
  (captured, 4 equal twin-column refusals turn into silent answers). Corrected 2026-11-18 (Delta 1, Fable's
  measurement): Spark captures `spark.sql.caseSensitive` into `viewSQLConfigs` and resolves the body's names under the
  captured value; the refusal is the file relation's, under the reader's setting (`AMBIGUOUS_REFERENCE` from
  `LogicalPlan.resolve(StructType)`, `COLUMN_ALREADY_EXISTS` from `DataSource.resolveRelation`), not a general
  twin-column check, and "at every door, a held frame included" holds for the file fixture only. Proposed card:
  TEMP-VIEW-CASE-SENSITIVE-CAPTURE-1 (S3, loud, not label-blocking; the file relation's reader-side refusal first).

**Bar** ([evidence/built-part1/bar.md](../../wo/temp-view-session-settings-1/evidence/built-part1/bar.md)): 2,772 paired
cells; 363 WRONG → EQUAL in part 1, 0 EQUAL → WRONG, 4 EQUAL → RAISE (the ruled class: `or_mixed`, `agg_key_sens`,
`and_free` on the SQL door, and the step 0's own `filter_sensitive` cell that question 11's default names); no move in
part 2, the held door, the catalog views, the direct queries, or any cell whose settings match. Of part 1's 375 view
gaps, 348 answer as Spark, 21 raise as Spark does with RePark's condition text, 7 are creation steps refused by a
direct gap; the rest are named: HELD-FRAME-CURRENT-TIMEZONE-1 (2), `caseSensitive` (3), part 2's outer level (1).
Pins: 772 pass on the head, 606 fail on main.

**Cost** ([evidence/built-part1/cost.md](../../wo/temp-view-session-settings-1/evidence/built-part1/cost.md)), 5 alternating runs, each sample ≥ 200 ms, `EXPLAIN` identical on every matching case:

| row | base (s) | head (s) | ratio | control | bar |
|---|---|---|---|---|---|
| SQL view read, matching | 0.0122 | 0.0118 | 0.961 | 0.834 to 1.098 | held |
| filter pushed through it | 0.0273 | 0.0266 | 0.973 | 0.967 to 1.016 | held |
| join over it | 0.0185 | 0.0189 | 1.023 | 0.961 to 1.106 | held |
| view over a view | 0.0128 | 0.0124 | 0.967 | 0.854 to 1.140 | held |
| no view | 0.0110 | 0.0114 | 1.033 | 0.974 to 1.117 | held |
| C-012 `c012p6` / `c012rest` | 39.88 / 43.78 | 41.11 / 45.88 | 1.031 / 1.048 | 0.489 to 1.014 / 0.987 to 1.081 | held |
| differing: read, filter, join, view over a view | — | — | 0.999, 0.989, 1.046, 0.954 | — | reported |

**Still unlike Spark:** Part 2 (the DataFrame door, its caches and held derivations); the ruled class;
VIEW-FILTER-SETTINGS-WRAPPER-1; AND-GUARD-ANSI-1; HELD-FRAME-CURRENT-TIMEZONE-1; TEMP-VIEW-ALTER-AND-CACHE-AS-1;
`caseSensitive` (proposed TEMP-VIEW-CASE-SENSITIVE-CAPTURE-1).

**Delta 1 (2026-11-18):** a Fable medium consult under the owner's overnight delegation ("Proceed with the plans, use
defaults, when it comes to any roadblocks or questions spin up a Fable medium session to ask for guidance. Merge any PRs
needed as long as they get us to our end goal"; guidance, not an owner ruling) declined the inlined-frame fallback as
built on one measured silent zone cell (a DataFrame frame `ts >= '…'` made under `America/Los_Angeles`, inlined into a
SQL view made under `UTC`, read under `America/Los_Angeles`: Spark 1, main 1, part 1 2), adopted the `caseSensitive`
exclusion with Spark's mechanism corrected above, and confirmed `filter_sensitive/sql/fdiv/on_off` in the ruled class
(the SQL door's accepted EQUAL → RAISE count is 4). Product and pins `c54f5fc7`, one file (`temp_settings.rs`; the cap
stays 3 of 5): the SQL door analyses the body eagerly and folds that coercion into a timestamp literal, so an inlined
frame now reads settings unless its analysed plan holds only bare column references under a closed list of plan nodes.
A 232-cell sweep (both ANSI modes, both orders; `timestamp`/`date` against strings, string arithmetic and casts, mixed
`CASE`, implicit function casts, decimal widening, two levels): part 1 had 20 EQUAL → WRONG, the head 0, answering every
cell as main does. The bar re-run (2,772 cells): 0 moves against part 1 as built, the same as part 1 against main. Cost:
every held row ≤ 1.05x (C-012 0.997 / 0.994, timed inside the lock after a runner flaw was found). Records:
[bar.md](../../wo/temp-view-session-settings-1/evidence/built-part1/bar.md) and
[cost.md](../../wo/temp-view-session-settings-1/evidence/built-part1/cost.md) "Delta 1".


## Built (part 2, the DataFrame door and materialisation) — 2026-11-18

**Branch:** `fix/temp-view-session-settings-1-part2` (draft PR #1362), product and pins `74c22dfe`, delta 1 `513208ce`, delta 2 `9d71e030` and delta 3 `9aa2f182`. **Builder:** Claude Opus 5.5
(`claude-opus-5-5`), effort xhigh.

**Rulings carried (owner, verbatim):** 2026-11-17 "dispatch Opus xhigh effort levels for these fixes, then use Opus 5.5
medium verifiers for the time and not muse, at least until these things are closed out"; "Defaults, merge 1355";
"defaults on both" (question 4: the write doors follow Spark door by door, `writeTo` the captured ANSI and the writer's
zone; question 5: HELD-FRAME-CURRENT-TIMEZONE-1 carded, not built; question 8: cache substitution not this unit's;
question 9: the 22 held derivations, the 2 `registerTempTable` cells and the 4 new held-frame write cells in this bar,
seat 3 for the 2 `cache()` cells; question 11 (a): the revised filter rule, its loud residual accepted, the wrapper
carded as VIEW-FILTER-SETTINGS-WRAPPER-1). 2026-11-18 (overnight): "Proceed with the plans, use defaults, when it comes
to any roadblocks or questions spin up a Fable medium session to ask for guidance. Merge any PRs needed as long as they
get us to our end goal"; "Extend the window for 6 hours". Part 1's calls (Fable, under that delegation): the fallback
replaced here; `caseSensitive` stays uncaptured (TEMP-VIEW-CASE-SENSITIVE-CAPTURE-1). 2026-11-18, about 14:30 UTC: "We
better go with B to be safe" (TEMP-VIEW-FRAME-READER-PARSE-1 label-blocking, S1 by class); about 15:00 UTC: "Defaults on
all three, extend the window 4 hours" (the proposed cards' severities below, and `smartCsv`).

**Seat 2A, a marker decided by the analyzer.**
- `crates/repark-spark/src/view_ddl/frame_settings.rs` (new): `frame_view` captures the four carriers from the frame's
  own state (`into_parts`); a `_repark_writer_v2_` scratch view takes the writer's zone (question 4); the SQL door's
  bare `ReplanningTempView` scan, a frame that only scans and (delta 1) a frame whose lineage reaches a JSON or CSV
  reader scan keep `into_view`. `FrameView` inlines a `FrameSettings`
  extension node; `FrameSettingsRule`, at analyzer index 0 after the extension's and the dialect's rules, unwraps it
  when the planning state's settings match (today's plan) and otherwise closes it as a `TableScan` over the frame
  analysed under its own settings and planned by part 1's overlay (the reader's start time, `current_timezone()` at
  the reader's zone, the settings-free pushed conjuncts inside). `opened` and `definition_plan` read through a closed
  frame.
- `temp_settings.rs`: the inlined-frame fallback removed, then restored by delta 1 for the JSON and CSV lineage only; settings compare by effective value (an unset carrier equals
  its default); the pushdown answer shared. `temp_view.rs`: the fallback call kept only when the re-planned body reaches a JSON or CSV scan (delta 1); the reference and dependency
  walks read through closed frames; `definition_plan` answers a frame view.
- Core: `extension.rs` (`FrameViewSeat`), `session/temp_views.rs` (the seat; seat 3A), `session/df_guards.rs` (the
  rule at index 0), `plan_introspect.rs` (the S3 overwrite check and the semantic hash read a plan-carrying source).
  One registration line in the Spark `extension.rs`, one `mod` line. **Seven product files (cap 7).**

**Seat 3A:** `register_collected_memtable` plans under `frame.into_parts()`'s state, restamped by
`mark_start_execution` (the frame's options, the materialisation's `now()`, pin (f)).

**Delta 1 (2026-11-18): the JSON and CSV reader lineage.** The build halted on four EQUAL → WRONG cells of part 1's
verifier and stated the cause as "RePark's JSON source reads the session zone at scan time". That was wrong: the
facade's JSON and CSV readers build their typed parse as an ordinary projection over a scan of strings
(`reader.py` `_apply_reader_schema_semantics`), so the zone lives in a frame expression and closing the frame parsed
under the creation zone. Fable's guidance under the owner's delegation (not an owner ruling) set the invariant: a view
of either door whose definition reaches a JSON or CSV listing scan, through any number of views, answers exactly as
part 1's package does. `reader_lineage` (`frame_settings.rs`) detects it by source; such a frame keeps `into_view` with
its inner markers unwrapped, and a SQL view whose re-planned body reaches one keeps part 1's inlined-frame fallback
(restored in `temp_settings.rs`, taken in `temp_view.rs` for this lineage only). Commit `513208ce`, three of the seven
files; no new product file. Reader inventory, sweep and judgement call (`smartCsv`):
[bar.md](../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md).

**Delta 2 (2026-11-18): writes under matching settings.** PR #1362's CI failed
`test_store_ts_to_numeric_1.py::test_store_assignment_cells_replay_spark`: with no setting changed, `-NULL` written
into a `DATE`, `BOOLEAN` or `TIMESTAMP` column through `writeTo`, `insertInto`, `saveAsTable`, `mergeInto` or SQL
`INSERT` or `MERGE` from a DataFrame view stored a NULL where main and Spark refuse with
`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`; the differing path did the same. The cause: the store-assignment
check (`repark-iceberg` `negated_null_store::column_origin`) walks the write source's unanalysed plan, and any node it
does not know ends the walk without a refusal; the build inlined the `FrameSettings` marker for every reader. The fix
(`9d71e030`, four of the seven files, cap 7): the marker stays only inside a SQL view's body, re-planned by
`ReplanningTempView` (`view_body`); elsewhere a frame view decides when it is inlined, against the session's live
options read through a weak `LiveOptions` handle (`repark-core` `extension.rs`, `session/temp_views.rs`): its own plan
when they match (main's plan, for every walker), the closed `TableScan` when they differ. The build's and delta 1's
claim of no matching-settings moves covered reads; the write sweep (13 doors, 5 sources of 5 values, 6 target types,
both ANSI modes, 3,100 cells) now shows writes too: 0 cells differ from main (delta 1: 216), and against Spark on six
doors 1,376 of 1,400 EQUAL, the same 24 direct gaps as main.

**Delta 3 (2026-11-18): the live read under a contended lock.** The verifier of #1362 (Opus 5.5) found F1, class (a),
S2: delta 2's live read (`try_read_recursive`) failed while another thread held the session state's write lock (every
`spark.conf.set` takes it), the frame view inlined the `FrameSettings` marker, and `-NULL` into `DATE` or `BOOLEAN` was
stored where main and Spark refuse: 27 to 44 of 1,000 writes beside two `conf.set` threads. The owner's ruling
(2026-11-18, about 20:25 UTC, verbatim): "Go with your recommendation, dispatch 1362 delta 3" (fix F1 inside the seven
files, with the verifier's race probe as a test; stop at an eighth). The fix (`9aa2f182`, two of the seven files, cap
7): the read waits for the lock at most one second (`try_read_recursive_for`), and `LiveOptions` answers the options it
last read, from the view's creation on, when the wait expires, so outside a SQL view body a frame view never inlines
the marker and nothing waits unbounded. No write-lock holder plans, awaits or takes a planner's lock (the audit is in
bar.md). Measured with the verifier's probe (five doors, two setter threads): the head 0 of 5,000 writes landed in each of two runs, the merged delta 2 (`d4423a69`) 165, main now (`6286ba19`) 0; two 75-second hang runs on the head (one with the setters also flipping ANSI and the zone) landed 0 of about 15,000 writes each and ended inside their timeout. Pins: a facade race test (fails 10 of 10 runs on `d4423a69`, about 4 s) and two Rust tests holding the state's write lock. The re-check verifier (Opus 5.5 medium) passed `a3369e93` on 2026-11-19 with no class (a) or (b) finding, and its N1 (class (c), S3) is proposed as a card, rating the owner's.

**The cap exception, not used:** Fable's guidance under the owner's delegation allowed an eighth product file,
`crates/repark-python/src/frame_lineage.rs`, plus one helper in `repark-core/src/extension.rs`, for `crossJoin` and the
exact-key join door. The fix was built (each side's plan through the seat before `join_exact_sides` analyses the join)
and measured: 74 cells WRONG → EQUAL, but 2 EQUAL cells moved away from Spark
(`ansi/on_off/left_anti/cond_left`, `ansi/on_off/left_anti/cond_both`: a held ANSI-on frame on the left of a left-anti
join now raises `ARITHMETIC_OVERFLOW`; Spark pushes the anti join below the projection and answers empty, a direct gap
RePark shows under matching settings too). By the back-out rule it was dropped before commit; the patch and its
measurements are kept in the evidence.

**Filed card, label-blocking (owner, 2026-11-18, about 14:30 UTC, verbatim: "We better go with B to be safe"; B:
TEMP-VIEW-FRAME-READER-PARSE-1 blocks ICE-V2-FLAT-AWS-LABEL-1; part 2 still merges with the residue named, since no cell
gets worse than main; the card's own unit must land before the label can be declared).** TEMP-VIEW-FRAME-READER-PARSE-1,
S1 by class (the 2026-11-17 "S1 by class" ruling; the time-zone half blocks): Spark parses a JSON or CSV source under
the reader's settings and decides everything above the parse under the frame's; RePark's JSON and CSV lineage answers
as part 1 and main do. Its cells, the reader inventory with the settings each reader-built expression reads, and the
`smartCsv` question are in bar.md.

**Cards ruled by the owner (2026-11-18, about 15:00 UTC, verbatim: "Defaults on all three, extend the window 4 hours"):**
TEMP-VIEW-FRAME-CROSSJOIN-1 (`crossJoin` and the exact-key join door analyse both held sides under the live state;
silent; the left-anti direct gap is its prerequisite) and HELD-FRAME-USING-JOIN-1 (a USING join keeps only the left
frame's state, `join_on_names`, so a held right side answers under the live settings; silent): **S1, label-blocking**;
HELD-FRAME-DERIVED-ANSI-1 (`withColumn` and `select(F.expr(...))` over a held ANSI frame answer unlike Spark on every
build, the column door's eager analysis, `column/expr_build.rs:187`): **S2**; LEFT-ANTI-PROJECTION-PUSHDOWN-1 (a
left-anti or left-semi join evaluates the left projection on every row, where Spark pushes the join below it; loud):
**S3**; `smartCsv`: the head's behaviour is accepted, and any change folds into TEMP-VIEW-FRAME-READER-PARSE-1.

**Bar** ([evidence/built-part2/bar.md](../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md), package `pkg-view2-9d71e030`; delta 3 on `pkg-view2-9aa2f182` gives every verdict as delta 2's, the write sweep 3,100 of 3,100 as main now, 0 of 10,000 racing writes landed): 3,004 cells, against part 1 261 WRONG → EQUAL, 10 WRONG → WRONG (changed), 5 EQUAL → RAISE (the ruled filter class's four DataFrame-door cells and question 4's `writeTo` `avg`), 0 EQUAL → WRONG, 0 matching-settings moves, every verdict as delta 1's; none of the 665 part 2 EQUAL targets lost; the JSON and CSV lineage 0 moves against part 1 (524 sweep cells); the write sweep under matching settings 0 cells differ from main in 3,100.

**Cost** ([cost.md](../../wo/temp-view-session-settings-1/evidence/built-part2/cost.md), delta 2 on `pkg-view2-9d71e030` against `pkg-view1-c54f5fc7`, 5 alternating runs, 10 for the one row over the bar): every held matching row within 1.05x (0.980x to 1.032x; C-012 1.016x and 1.001x; `match_unpivot` 1.056x after 5 runs, 1.004x after 10); the differing path, reported, 0.982x to 1.054x. Delta 3 on `pkg-view2-9aa2f182` against main now (`pkg-main-6286ba19`), the seven held rows: 0.972x to 1.037x.

**Append (2026-11-19, ruling 10):** part 2 merged as #1362 (`4c365814`) on 2026-11-19 about 01:30 UTC, on the owner's word of item 10 ([rulings](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). The residue cards are filed in [cards-2026-11-19/](cards-2026-11-19/map.md): [TEMP-VIEW-FRAME-READER-PARSE-1](cards-2026-11-19/temp-view-frame-reader-parse-1-card-2026-11-19.md), [TEMP-VIEW-FRAME-CROSSJOIN-1](cards-2026-11-19/temp-view-frame-crossjoin-1-card-2026-11-19.md), [HELD-FRAME-USING-JOIN-1](cards-2026-11-19/held-frame-using-join-1-card-2026-11-19.md), [HELD-FRAME-DERIVED-ANSI-1](cards-2026-11-19/held-frame-derived-ansi-1-card-2026-11-19.md), [LEFT-ANTI-PROJECTION-PUSHDOWN-1](cards-2026-11-19/left-anti-projection-pushdown-1-card-2026-11-19.md) and [TEMP-VIEW-FRAME-CONF-RACE-1](cards-2026-11-19/temp-view-frame-conf-race-1-card-2026-11-19.md).
