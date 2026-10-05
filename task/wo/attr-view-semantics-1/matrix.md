# ATTR-VIEW-SEMANTICS-1 measurement matrix (2026-10-04)

Probe: [`views_probe.py`](views_probe.py), one JSON per engine beside this file ([`spark.json`](spark.json), [`repark152.json`](repark152.json), [`stack.json`](stack.json)); run as `python views_probe.py <spark|repark152|stack> <out.json>`.
Banner (live): Spark 4.1.2, session zone America/New_York.
Engines: Spark 4.1.2 (`/tmp/sparkenv`), RePark 1.5.2 PyPI wheel, ATTR-ID-1 stack `d5c97862`
(read-only). Source frame `d = [(1,10),(2,20)]` with columns `id, v`, registered as `tv`.
Stack id prefix `af64f9cb5983bf4b` elided below; only the trailing counter is shown.

## Attribute ids (mint vs carry)

| shape | Spark 4.1.2 | stack `d5c97862` | stack vs Spark |
|---|---|---|---|
| `d` | `id#0L, v#1L` | `…0001, …0002` | EQUAL (both stamp the source) |
| `table("tv")` twice | `id#0L, v#1L` both reads | `…0001, …0002` both reads | EQUAL (both carry) |
| `SELECT * FROM tv` | `id#0L, v#1L` | `…0001, …0002` | EQUAL (both carry) |
| `SELECT v FROM tv` | `v#1L` | `…0002` | EQUAL (both carry) |
| `SELECT v + 0 AS v FROM tv` | `v#2L` (new) | `…0003` (new) | EQUAL (both mint) |
| `SELECT v AS v FROM tv` | `v#3L` (new) | `…0002` (kept) | DIFFERS (pre-existing on stack; main has no ids) |
| `d2` / `table("tv2")` | `id#0L, v#1L, w#15` | `…0001, …0002, …000a` | EQUAL (both carry + stamp `w`) |
| `sv` (`CREATE TEMP VIEW sv AS SELECT * FROM tv`) | `id#16L, v#17L` (new) | `…0001, …0002` (kept) | DIFFERS (pre-existing on stack; main has no ids) |
| `global_temp.gv` | `id#0L, v#1L` (kept) | unsupported | DIFFERS (F16, both RePark engines) |

1.5.2 has no `repark._native.attribute_ids`, so every id row is N/A there by construction.

## Outcome rows (answer vs error class)

OK rows show `collect()` values. Errors show class + condition + first line (truncated).

| shape | Spark 4.1.2 | 1.5.2 | stack | 1.5.2 vs Spark | stack vs Spark |
|---|---|---|---|---|---|
| V1 `table(tv).select(d.v)` | OK `[(10,),(20,)]` | OK same | OK same | EQUAL | EQUAL |
| V2 `sql(*).select(d.v)` | OK `[(10,),(20,)]` | OK same | OK same | EQUAL | EQUAL |
| V3 `sql(v+0 AS v).select(d.v)` | ERR AnalysisException MISSING_ATTRIBUTES | OK `[(10,),(20,)]` | OK `[(10,),(20,)]` | DIFFERS (pre-existing) | DIFFERS (pre-existing) |
| V4 `sql(v AS v).select(d.v)` | ERR AnalysisException MISSING_ATTRIBUTES | OK `[(10,),(20,)]` | OK `[(10,),(20,)]` | DIFFERS (pre-existing) | DIFFERS (pre-existing) |
| V5 `d.join(table, d.id==t.id)` | OK 4-col rows | OK same | OK same | EQUAL | EQUAL |
| V6 `table.join(table, t1.id==t2.id)` | OK 4-col rows | OK same | OK same | EQUAL | EQUAL |
| V6b `t1.join(t2,"id").select(t1.v)` | OK `[(10,),(20,)]` | ERR AMBIGUOUS_REFERENCE | ERR AMBIGUOUS_REFERENCE | DIFFERS (pre-existing) | DIFFERS (pre-existing) |
| V7 SQL self-join | OK `[(10,10),(20,20)]` | OK same | OK same | EQUAL | EQUAL |
| V8 `table(tv2).select(d.v, d2.w)` | OK `[(10,1),(20,1)]` | OK same | OK same | EQUAL | EQUAL |
| V9 `table(sv).select(d.v)` | ERR AnalysisException MISSING_ATTRIBUTES | OK `[(10,),(20,)]` | OK `[(10,),(20,)]` | DIFFERS (pre-existing) | DIFFERS (pre-existing) |
| V10 global temp view | OK `[(10,),(20,)]` | ERR UnsupportedOperationException (no global_temp catalog) | ERR UnsupportedOperationException (same) | DIFFERS (F16, pre-existing) | DIFFERS (F16) |
| V11 re-register, `table(tv).select(d.v)` | OK `[(10,),(20,)]` | OK same | OK same | EQUAL | EQUAL |
| V12 `tv` replaced by `e`, `.select(d.v)` | ERR AnalysisException MISSING_ATTRIBUTES | OK `[(10,),(20,)]` | OK `[(10,),(20,)]` | DIFFERS (pre-existing) | DIFFERS (pre-existing) |
| V13 `tv` replaced by `e`, `.select(e.v)` | OK `[(10,),(20,)]` | OK same | OK same | EQUAL | EQUAL |
| V14 view over `d.filter(...)` | OK `[(20,)]` | OK same | OK same | EQUAL | EQUAL |
| V15 view over aggregate, `.select(d.v)` | ERR AnalysisException MISSING_ATTRIBUTES | OK `[(10,),(20,)]` | OK `[(10,),(20,)]` | DIFFERS (pre-existing) | DIFFERS (pre-existing) |
| V-3a `table(sv).write.parquet(...)` + read back | OK `[(1,10),(2,20)]` | OK same | ERR PySparkException `Internal error: strip reached a keyed TableScan; providers are born clean.` | EQUAL | DIFFERS (new on stack) |
| V-3b `table(sv).groupBy("id").count()` | OK `[(1,1),(2,1)]` | OK same | ERR PySparkException `Internal error: Physical input schema should be the same as the one converted from logical input schema.` | EQUAL | DIFFERS (new on stack) |

Spark's MISSING_ATTRIBUTES condition in full: `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`,
e.g. `Resolved attribute(s) "v" missing from "v" in operator !Project [v#1L]`.

## Counts (18 outcome rows)

- 1.5.2 vs Spark: 11 EQUAL, 7 DIFFER (V3, V4, V6b, V9, V10, V12, V15).
- stack vs Spark: 9 EQUAL, 9 DIFFER (V3, V4, V6b, V9, V10, V12, V15, V-3a, V-3b).
- New on the stack (EQUAL on main, DIFFERS on stack): V-3a, V-3b only.
- Every other stack difference already differs on main. No row is EQUAL on the stack while DIFFERING on main.
