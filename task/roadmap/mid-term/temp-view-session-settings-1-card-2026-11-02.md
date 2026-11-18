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
