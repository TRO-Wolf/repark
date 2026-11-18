# Card TEMP-VIEW-SESSION-SETTINGS-1: a temp view is analysed under the SQL settings of the session that reads it, where Spark keeps the creating session's

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the observations of the MAP-STRUCT-KEY-SUBSCRIPT-1 round-5 Opus verdict. The observations are not counted against that unit.

**Status:** filed; grade not set (the orchestrator grades it); not dispatched. Head equals base for the cells below.

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
  (captured, 4 equal twin-column refusals turn into silent answers; Spark's twin-column check reads the reader's
  setting at every door). Proposed card: TEMP-VIEW-CASE-SENSITIVE-CAPTURE-1.

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
