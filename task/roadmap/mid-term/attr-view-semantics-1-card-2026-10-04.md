# Card ATTR-VIEW-SEMANTICS-1 — SQL aliases and SQL-defined views mint ids in Spark; measured on Spark 4.1.2, 1.5.2 and the stack

**Date:** 2026-10-04 · **Filed by:** Muse Spark 1.3 (contributor), for the orchestrator · **Source:** the PERF-ATTR-STAMP-2 step 1 sketch §3 (`/tmp/oc-worker/direct/wo/attr-id-1/stamp2-step1/sketch.md`, lines 294–328) and the #930 verifier's V-3 (`/tmp/oc-worker/direct/wo/attr-id-1/stamp2-o1/verify-handback.json`, finding V-3; repro scripts `verify/leak_scope.py` and `verify/leak_repro.py` in `/tmp/oc-worker/direct/wo/attr-id-1/stamp2-o1/verify/`). No product code in this filing.

## Why

Spark resolves DataFrame columns by attribute id. This filing re-measures the two rules the sketch states, on the live oracle:

1. **SQL aliases and SQL-defined views mint new attribute ids.** `SELECT v + 0 AS v FROM tv` outputs `v#2L`, `SELECT v AS v FROM tv` outputs `v#3L`, and `CREATE TEMP VIEW sv AS SELECT * FROM tv` outputs `id#16L, v#17L`, all fresh against the source frame's `id#0L, v#1L`. Bare references carry instead: `table("tv")` twice, `SELECT * FROM tv`, `SELECT v FROM tv` and the global temp view all keep `id#0L, v#1L`, and `withColumn("w", lit(1))` carries `id#0L, v#1L` plus stamped `w#15`.
2. **A foreign column that is not in the plan raises MISSING_ATTRIBUTES instead of resolving.** `.select(d["v"])` over each minted frame fails with `AnalysisException`, condition `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`, e.g. `Resolved attribute(s) "v" missing from "v" in operator !Project [v#1L]` (V3, V4), `... missing from "id", "v" in operator !Project [v#1L]` (V9, V12, V15).

Oracle banner (live): Spark 4.1.2, session zone America/New_York.

## The measured matrix

Probe `/tmp/oc-worker/direct/wo/attr-view-semantics-1/views_probe.py`, one JSON per engine in `/tmp/oc-worker/direct/wo/attr-view-semantics-1/runs/` (`spark.json`, `repark152.json`, `stack.json`); summary table `/tmp/oc-worker/direct/wo/attr-view-semantics-1/matrix.md`. Engines: Spark 4.1.2, RePark 1.5.2 (PyPI wheel), the ATTR-ID-1 stack at `d5c97862` (read-only). Source frame `d = [(1, 10), (2, 20)]`, columns `id, v`, registered as `tv`. Stack id prefix elided to the trailing counter.

### Attribute ids

Main 1.5.2 has no `repark._native.attribute_ids`, so every id row is N/A there by construction.

| shape | Spark 4.1.2 | stack `d5c97862` | verdict |
|---|---|---|---|
| `d` | `id#0L, v#1L` | `…0001, …0002` | EQUAL (both stamp the source) |
| `table("tv")`, read twice | `id#0L, v#1L` both times | `…0001, …0002` both times | EQUAL (both carry) |
| `SELECT * FROM tv` | `id#0L, v#1L` | `…0001, …0002` | EQUAL (both carry) |
| `SELECT v FROM tv` | `v#1L` | `…0002` | EQUAL (both carry) |
| `SELECT v + 0 AS v FROM tv` | `v#2L` (new) | `…0003` (new) | EQUAL (both mint) |
| `SELECT v AS v FROM tv` | `v#3L` (new) | `…0002` (kept) | DIFFERS |
| `d2` / `table("tv2")` | `id#0L, v#1L, w#15` | `…0001, …0002, …000a` | EQUAL (both carry `id, v`, stamp `w`) |
| `sv` (`CREATE TEMP VIEW sv AS SELECT * FROM tv`) | `id#16L, v#17L` (new) | `…0001, …0002` (kept) | DIFFERS |
| `global_temp.gv` | `id#0L, v#1L` (kept) | unsupported | DIFFERS (F16, both RePark engines) |

### Outcomes

OK rows show `collect()` values. Errors show class, condition and first line, truncated.

| shape | Spark 4.1.2 | 1.5.2 | stack | 1.5.2 vs Spark | stack vs Spark |
|---|---|---|---|---|---|
| V1 `table(tv).select(d.v)` | OK `[(10,), (20,)]` | OK same | OK same | EQUAL | EQUAL |
| V2 `sql(*).select(d.v)` | OK `[(10,), (20,)]` | OK same | OK same | EQUAL | EQUAL |
| V3 `sql(v+0 AS v).select(d.v)` | ERR MISSING_ATTRIBUTES | OK `[(10,), (20,)]` | OK `[(10,), (20,)]` | DIFFERS | DIFFERS |
| V4 `sql(v AS v).select(d.v)` | ERR MISSING_ATTRIBUTES | OK `[(10,), (20,)]` | OK `[(10,), (20,)]` | DIFFERS | DIFFERS |
| V5 `d.join(table, d.id==t.id)` | OK 4-col rows | OK same | OK same | EQUAL | EQUAL |
| V6 `table.join(table, t1.id==t2.id)` | OK 4-col rows | OK same | OK same | EQUAL | EQUAL |
| V6b `t1.join(t2, "id").select(t1.v)` | OK `[(10,), (20,)]` | ERR AMBIGUOUS_REFERENCE | ERR AMBIGUOUS_REFERENCE | DIFFERS | DIFFERS |
| V7 SQL self-join | OK `[(10, 10), (20, 20)]` | OK same | OK same | EQUAL | EQUAL |
| V8 `table(tv2).select(d.v, d2.w)` | OK `[(10, 1), (20, 1)]` | OK same | OK same | EQUAL | EQUAL |
| V9 `table(sv).select(d.v)` | ERR MISSING_ATTRIBUTES | OK `[(10,), (20,)]` | OK `[(10,), (20,)]` | DIFFERS | DIFFERS |
| V10 global temp view | OK `[(10,), (20,)]` | ERR UnsupportedOperationException (no global_temp catalog) | ERR UnsupportedOperationException (same) | DIFFERS (F16) | DIFFERS (F16) |
| V11 re-register, `table(tv).select(d.v)` | OK `[(10,), (20,)]` | OK same | OK same | EQUAL | EQUAL |
| V12 `tv` replaced by `e`, `.select(d.v)` | ERR MISSING_ATTRIBUTES | OK `[(10,), (20,)]` | OK `[(10,), (20,)]` | DIFFERS | DIFFERS |
| V13 `tv` replaced by `e`, `.select(e.v)` | OK `[(10,), (20,)]` | OK same | OK same | EQUAL | EQUAL |
| V14 view over `d.filter(...)` | OK `[(20,)]` | OK same | OK same | EQUAL | EQUAL |
| V15 view over aggregate, `.select(d.v)` | ERR MISSING_ATTRIBUTES | OK `[(10,), (20,)]` | OK `[(10,), (20,)]` | DIFFERS | DIFFERS |
| V-3a `table(sv).write.parquet(...)`, read back | OK `[(1, 10), (2, 20)]` | OK same | ERR PySparkException `Internal error: strip reached a keyed TableScan; providers are born clean.` | EQUAL | DIFFERS |
| V-3b `table(sv).groupBy("id").count()` | OK `[(1, 1), (2, 1)]` | OK same | ERR PySparkException `Internal error: Physical input schema should be the same as the one converted from logical input schema.` | EQUAL | DIFFERS |

Counts over the 18 outcome rows: 1.5.2 vs Spark 11 EQUAL / 7 DIFFER (V3, V4, V6b, V9, V10, V12, V15); stack vs Spark 9 EQUAL / 9 DIFFER (the same seven plus V-3a and V-3b). No row is EQUAL on the stack while DIFFERING on main.

## Where it lives

Attribute ids exist only on the ATTR-ID-1 stack (`wip/feat/attr-id-1-2026-10-01`); main resolves by name. Therefore:

- **Stack-only rows:** the two id-mint misses (`SELECT v AS v`, the `sv` provider keeping source ids) and the two V-3 internal errors (V-3a, V-3b). Both V-3 shapes answer Spark on main today.
- **Already differing on main, because main resolves by name:** V3, V4, V9, V12, V15 (Spark raises MISSING_ATTRIBUTES; both RePark engines answer) and V6b (Spark answers; both RePark engines raise AMBIGUOUS_REFERENCE).
- **V10** differs on both engines for an unrelated reason: no global temp view support (F16).

## Scope, as proposed

- (a) `CREATE TEMP VIEW … AS SELECT` registers an id-free or re-minted provider. Proposed pins: the `sv` id row mints; V9 raises MISSING_ATTRIBUTES; V-3a and V-3b answer Spark — e.g. proposed `python/repark/tests/test_attr_view_semantics_1.py::test_sql_view_provider_mints`, `::test_sql_view_foreign_column_missing_attributes`, `::test_sql_view_write_parquet_answers`, `::test_sql_view_groupby_count_answers`.
- (b) SQL aliases mint. Proposed pins: the `SELECT v AS v` id row mints; V4 raises MISSING_ATTRIBUTES — e.g. proposed `python/repark/tests/test_attr_view_semantics_1.py::test_sql_alias_mints`, `::test_sql_alias_foreign_column_missing_attributes`.
- (c) A foreign column not in the plan raises MISSING_ATTRIBUTES. Proposed pins: V3, V12 and V15 raise with Spark's condition — e.g. proposed `python/repark/tests/test_attr_view_semantics_1.py::test_expr_alias_foreign_column_missing_attributes`, `::test_replaced_view_foreign_column_missing_attributes`, `::test_agg_view_foreign_column_missing_attributes`.
- (d) The two V-3 internal errors become answers that equal Spark. Proposed pins: the V-3a/V-3b pins named under (a), plus a regression pin that no `Internal error` text escapes either shape.

No code is designed here; the pin names above are proposals, not citations.

## Out of scope

V10 and global temp views (F16). The measurement confirms the carve-out: `createOrReplaceGlobalTempView` is unsupported on both RePark engines (`createGlobalTempView is not supported yet`), while Spark keeps source ids through the global view and answers V10. Nothing in this round moves that boundary.

## Questions for the owner

1. **Grade.** Lean: B with an Opus design sketch, since the unit touches identity semantics (mint points, provider registration, the MISSING_ATTRIBUTES refusal).
2. **Release.** Lean: with or right after the stack merge, not 1.5.3 — every scope item except V6b needs attribute ids, which exist only on the stack, and (c) converts answers into errors.
3. **Whether (c) can break user code that resolves today.** Lean: yes for same-name foreign columns (the V12 and V15 shapes answer on main today), but only on shapes Spark already refuses — so (c) ships with the stack merge and a release note, never as a silent patch.
