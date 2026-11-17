# Card TEMP-VIEW-SESSION-SETTINGS-1: a temp view is analysed under the SQL settings of the session that reads it, where Spark keeps the creating session's

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the observations of the MAP-STRUCT-KEY-SUBSCRIPT-1 round-5 Opus verdict. The observations are not counted against that unit.

**Status:** step 0 dispatched 2026-11-17 (the orchestrator's reading of the owner's words, quoted in the dated append below); absorbs VIEW-ANSI-CAPTURE-1; severity and label status not ruled. Head equals base for the cells below.

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
