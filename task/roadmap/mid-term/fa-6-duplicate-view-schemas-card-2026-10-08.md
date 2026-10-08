# Card FA-6: temp views over frames with duplicate display names

**Date:** 2026-10-08. **Filed by:** Claude (Opus 5.5), build lane, on the orchestrator's fold-1
ruling for #1002. **Source:** the withdrawn FA-6 half of the FA-5/FA-6 unit
([ledger](../../ledgers/staging/fa-5-6-ledger.md), "Why FA-6 was withdrawn (2026-10-08)") and
its verifier's verdict (`/tmp/oc-worker/direct/wo/fa-5-6/verify/verdict.json`, harness `h.py`,
cells `c_views.py` and `d.py`).

**Status:** filed, **needs an owner decision**. Not scheduled. Registry row FA-6 in
`docs/spark-sql-iceberg-parity.md` stays a declared refusal until this card is ruled.

**Retires:** when the owner rules one of the two outcomes below and, for the first, the unit
that builds it merges.

## Why

`createOrReplaceTempView`, `createTempView`, `createGlobalTempView` and
`createOrReplaceGlobalTempView` refuse a frame whose display names hold exact duplicates
(`[COLUMN_ALREADY_EXISTS]`). Spark 4.1.2 registers the view: `SELECT *` answers the display
names, a reference to a repeated name refuses `AMBIGUOUS_REFERENCE`, and a repeated name whose
copies are one attribute answers.

A first design was built and withdrawn (2026-10-08). It registered the view under unique engine
names, laid the display names over the frames the SQL door returned, and added an audit that
re-created Spark's ambiguity after DataFusion had planned. The audit modelled the scopes its
author thought of. Every scope it did not model answered **without an error and wrongly**:

- `NATURAL JOIN` computed the common columns over engine names, so a repeated display name was
  never a join key (a cross join, or a subset of Spark's keys);
- a bare repeated name inside `EXISTS` or `LATERAL` missed in the inner scope and bound to the
  outer relation's column;
- a qualified reference to a repeated name answered the case twin's column under the default
  case rule.

Main cannot produce any of these, because main does not register the view. The ruling: a
refusal at registration is safe; a registered view that answers wrongly is not.

## The ask

Decide between two outcomes.

1. **A design in which the planner's own scoping resolves names over a duplicate-name view.**
   The repeated display name must be what name resolution sees, in every scope the planner
   already models, so that `NATURAL`, `USING`, subqueries, `LATERAL`, set operations and both
   case rules need no audit after the fact. A design that keeps unique engine names and
   patches ambiguity on afterwards is the one that failed and is out. Candidate directions,
   none measured: a relation node that expands to per-column qualifiers the planner treats as
   one relation name, or a planner-side hook at identifier resolution that knows a field's
   display name and its attribute identity. DataFusion is a normal upstream dependency and is
   not forked, so a change to its schema rule is not a candidate. The design note must say where DataFusion 54.1.0 refuses today
   (`DFSchema::check_names`, reached through `TableScan::try_new` and
   `SubqueryAlias::try_new`) and how the design gets past it without an audit.
2. **The declared refusal stays.** Registry row FA-6 keeps its wording, and this card closes
   as declined with the date and the reason.

The two global doors are unsupported for every frame (no `global_temp` catalog). That gap is
its own row and is not part of this ask.

## Acceptance grid

Outcome 1 is done when every cell below equals live Spark 4.1.2 on rows, columns and types, or
on condition, SQLSTATE and text, with no cell answering where Spark refuses. The views are the
verifier's: `vj` (self-join on `l.id == r.id`), `vm` (join with a second table on `id`), `vu`
(`USING (id)`), `vs` (`select('id', 'id', 's')`), `vn` (a union of two same-origin repeats),
`v3` (three repeats), `vt` (`id`, `v AS id`, `s AS ID`), beside `plain(id, w)` and `ps(id, s)`.

| Shape | Spark 4.1.2 |
|---|---|
| `SELECT * FROM <view>`; `spark.table`; `DESCRIBE`; `listColumns`; cached | display names, rows and types |
| `SELECT id FROM vj`, `vj.id`, an alias, a derived table, a CTE; in `WHERE`, `GROUP BY`, an aggregate, an expression | `AMBIGUOUS_REFERENCE`, the relation as written on each candidate, the reference in its written spelling (`ID` stays `ID`) |
| The same references on `vs`, `vn`, `v3` (repeats of one attribute), through `WHERE`, `GROUP BY`, `ORDER BY`, a window, `* EXCEPT`, a join condition, a derived table, a CTE, `CACHE TABLE` | answers |
| `SELECT * FROM <view> NATURAL JOIN plain` and `ps`, both orders, all seven views (32 cells) | Spark's keys, rows and columns (for example `vm NATURAL JOIN plain` is one row, columns `id, s, v, id, t, w`) |
| `SELECT * FROM <view> JOIN plain USING (id)`, both orders | answers (joins on the first `id`) |
| `SELECT * FROM plain WHERE EXISTS (SELECT 1 FROM vj WHERE id = 1)`, the same with `id = 7` | `AMBIGUOUS_REFERENCE` ``[`vj`.`id`, `vj`.`id`]`` |
| `SELECT * FROM plain, LATERAL (SELECT id AS x FROM vj)`; `LATERAL (SELECT v FROM vm WHERE id = 7)` | `AMBIGUOUS_REFERENCE` |
| `SELECT vt.id FROM vt`, `SELECT a.id FROM vt a` under `caseSensitive=false` | `AMBIGUOUS_REFERENCE`, three candidates |
| `SELECT Id FROM vt` under `caseSensitive=true` | `UNRESOLVED_COLUMN`, suggestions ``[`ID`, `id`, `id`]`` |
| `SELECT id FROM vj CROSS JOIN plain` | `AMBIGUOUS_REFERENCE` ``[`plain`.`id`, `vj`.`id`, `vj`.`id`]`` |
| `SELECT plain.id FROM vj CROSS JOIN plain ORDER BY id`; `SELECT plain.id AS id … ORDER BY id` | answers |
| `SELECT * FROM vj ORDER BY id`; `SELECT 1 FROM vj ORDER BY s`; `SELECT t FROM vm ORDER BY id`; `HAVING max(id) > 0` | `UNRESOLVED_COLUMN.WITH_SUGGESTION` |
| `SELECT * EXCEPT (id, s) FROM vj` | `AMBIGUOUS_REFERENCE` naming `id`, the first written |
| `CREATE TABLE … AS SELECT * FROM vj`; `CREATE TEMP VIEW … AS SELECT * FROM vj`; a frame of the view written to parquet or `saveAsTable` | `COLUMN_ALREADY_EXISTS` |
| `CREATE VIEW pv AS SELECT * FROM vj` | `INVALID_TEMP_OBJ_REFERENCE` |
| A unique column named like an internal name (`__repark_dup_0_id`) on a view, in `SELECT *`, `DESCRIBE`, `spark.table`, `listColumns`; `SELECT 1 AS __repark_dup_3_x` | the user's name, unchanged |
| `explain()` and `EXPLAIN` over the view | no engine-only name, or an owner ruling that `EXPLAIN` is exempt |
| Every statement that touches no such view | plans, answers and cost identical to the base |

The step-0 and follow-up Spark cells are recorded verbatim in the ledger; the verifier's
recorded answers are `c_spark.json` beside its harness.
