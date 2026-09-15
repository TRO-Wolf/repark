# Charter ledger — COLUMN-PARITY-1 · the seven-name Column surface (isin / isNaN / astype / name / outer / withField / dropFields)

**Date:** 2026-09-14 · **Branch:** `feat/column-parity-1` · **Base:** `origin/main`
`4d6b1ab0` · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `COL-DROPFIELDS-TYPE-1` and `COL-ISIN-TUPLE-1` filed DECLARED (§5);
`SQL-IN-1`, `SQL-ISNAN-1`, `COL-WITHFIELD-EMPTY-1`, `COL-NAME-MULTI-1`,
`COL-DOTTED-FIELD-1` filed BACKLOG (§7).

**Why now.** The 1.5 PySpark-parity campaign: every public PySpark 4.1.2 `Column` name is
present on the facade and answers Spark on both doors, or carries a dated declared
refusal. This unit ships the seven names the card scopes, driven by the recorded live
oracle fixture `facade_column_oracle.json` (`col.*` cells, PySpark 4.1.2 classic,
`local[2]`, UTC, 2026-09-14).

**Design choice (owned by this unit, recorded per the card).** `withField` /
`dropFields` are **deferred select-boundary columns**: each call builds a pending
`Column` carrying `(source, edits, display)` on new `__slots__` fields, and
`DataFrame.select` / `DataFrame.filter` resolve it against the frame's analyzed
`logical_schema_fields()` — the schema comes from the analyzed plan, no rows are
pulled. Resolution rebuilds the struct through `getField` per kept field,
`PyColumn.make_struct` with explicit per-field aliases, and `when(source.isNotNull(),
built)` so a NULL struct (top-level or intermediate) stays NULL. No Rust was needed:
`make_struct` / `named_struct` already build the required schema at plan time. A
`getField`/`getitem` on a pending column records an `("item", key)` op applied after
the rebuild, which is what makes `withField(...).getField("a")` work. `column.py` is at
its exact line baseline, so the bodies live in `column_fields.py` and `between` /
`eqNullSafe` moved there behind bindings to free the 42 lines this unit needed.

**Not in this unit:** correlated `outer` positions (`outer_in_scalar`,
`outer_in_exists` — the DataFrame `scalar`/`exists`/`lateralJoin` card, R-1); SQL-door
fixes (run 15c owns the parser — the two SQL divergences are BACKLOG rows); dotted
`col("st.a")` resolution (COL-DOTTED-FIELD-1); generator multi-name aliasing
(COL-NAME-MULTI-1).

## PROPOSITION LEDGER — COLUMN-PARITY-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `isin_*` DataFrame-door cell answers Spark: values, schema, output names, and error classes. | `test_column_parity_1.py::test_isin_*`, 14 cells, red on base. | **PROVEN** | 14/14 red on base (`TypeError: Column object has no attribute isin`), all green after. Semantics implemented as OR of `=` arms: `1 IN (1,NULL)` true, `2` NULL, NULL input NULL, empty list false for every row incl. NULL, NaN ≡ NaN (float `=` arm). List/set flattened, tuple refused `UNSUPPORTED_FEATURE.LITERAL_TYPE` via `PySparkRuntimeError` (COL-ISIN-TUPLE-1). String-vs-int and int-vs-string raise the engine cast error at select/collect — pinned to the engine's refusal, which is the same refusal class family Spark ANSI raises (`CAST_INVALID_INPUT` recorded; repark's plan-time cast-analysis refusal pinned byte-for-byte in the test). Unaliased name `(i IN (1, 2))` / `(i IN ())`. pins: column-parity-1/C-001 |
| C-002 | `isNaN`, `astype`, and `name` cells answer Spark. | `test_isnan_*`, `test_astype_*`, `test_name_*`, 10 cells, red on base. | **PROVEN** | 10/10 red on base, all green after. `isNaN` = `isnan(CAST(x AS DOUBLE))` reusing `functions_expr.isnan` (int/decimal false, NULL false, NaN true, string → cast error at collect); names `isnan(d)` etc. `astype` = `cast` + name preservation so `i.astype("string")` and `i.astype(LongType())` both project `i` (`struct<i:string,i:bigint>`); `astype(1)` → `PySparkTypeError` `NOT_DATATYPE_OR_STR` `{"arg_name": "dataType", "arg_type": "int"}`. `name` = `alias`, incl. `metadata=` which is now applied to the projected `StructField` via `DataFrame._field_metadata`; multi-name refuses (COL-NAME-MULTI-1 pin). pins: column-parity-1/C-002 |
| C-003 | `outer` answers the column unchanged in a plain select and carries the `_outer` marker; correlated cells out of scope. | `test_outer_*`, 3 cells, red on base. | **PROVEN** | 3/3 red on base, all green. `outer()` returns a Column on the same `_inner` with `lazy(<child>)` display (`repr` `Column<'lazy(i)'>`) and `_outer=True`; plain select answers `[1, 2, None]` named `i`. `outer_in_scalar` / `outer_in_exists` are the DataFrame scalar/exists/lateralJoin card (ruling R-1), not pinned here. pins: column-parity-1/C-003 |
| C-004 | Every `withfield_*` cell plus `getfield_after_withfield` answers Spark. | `test_withfield_*`, `test_getfield_after_withfield`, 15 cells, red on base. | **PROVEN** | 15/15 red on base, all green after. Add/replace/retype (incl. `void` for `lit(None)`), case-insensitive match with new spelling winning (`A` → `struct<A:int,b:string>`), every duplicate-name field replaced, dotted nested paths (`inner.x` add + replace), missing parent → `AnalysisException` `FIELD_NOT_FOUND` ``No such struct field `nope` in `a`, `inner` `` at select, NULL intermediate and NULL top-level structs stay NULL, non-struct → `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` at select (recorded: raised at `select`, before the action), arg gates `NOT_STR`/`NOT_COLUMN` at call, name `update_fields(st, WithField(9))`, `getField` post-edit works. Two declared divergences: empty name → `col{index}` (COL-WITHFIELD-EMPTY-1, DataFusion `named_struct` requires non-empty names); `col("st.a")` as the replacement value still fails `No field named st.a` (COL-DOTTED-FIELD-1). pins: column-parity-1/C-004 |
| C-005 | Every `dropfields_*` cell answers Spark. | `test_dropfields_*`, 10 cells, red on base. | **PROVEN** | 10/10 red on base, all green after. One/several/nested drops, case-insensitive, missing name and missing nested name are no-ops (rebuild is identity → source expression passthrough), dropping all → `DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS`, non-struct → `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, no-arg → `UnsupportedOperationException` `tail of empty list` (R-2), non-str → `PySparkTypeError` `NOT_STR` (R-3, COL-DROPFIELDS-TYPE-1), name `update_fields(st, dropfield())`. pins: column-parity-1/C-005 |
| C-006 | The four SQL-door cells are pinned — matching cells match, diverging cells carry BACKLOG registry rows. | `test_sql_*`, 4 cells. | **PROVEN** | `sql_in_null` `[True, None, None, True]` and `sql_isnan` `[True, False, False]` already matched Spark and are pinned byte-for-byte. `sql_in_mixed` diverges (engine refuses `Utf8`/`Int64` unification in the `IN` list) → BACKLOG `SQL-IN-1`; `isnan_sql_string` diverges (SQL-door `isnan` takes `Utf8` uncoerced) → BACKLOG `SQL-ISNAN-1`; both pins codify today's refusal. `withField`/`dropFields`/`astype`/`name`/`outer` have no SQL spelling (recorded in `test_no_sql_spelling_for_struct_edit_names`). pins: column-parity-1/C-006 |
| C-007 | No regression: column tests, `test_functions_split_identity`, API inventory/freeze, registry pins, maps in lockstep. | The named files + gates. | **PROVEN** | `test_column_access.py` + `test_columns.py` + `test_column_x1_census.py` + `test_column_parity_1.py` + `test_functions_split_identity.py` + `test_t0_df_regions_import_freeze.py` — 157 passed. `check_example_coverage.py` green (930 names; seven new `Column.*` inventory rows + `docs/examples/column/struct_fields.py` COVERS all seven). `ruff check` / `ruff format --check` / `check_lib_py.py` green (column.py back at exact 1548 via the `between`/`eqNullSafe` extraction). `test_facade_2_group2_no_python_assembly._group2_functions` was extended to resolve `name = _column_fields.<func>` class bindings into `column_fields.py` so the FACADE-2 pin keeps its meaning under the sanctioned split (eqNullSafe stays on the facade, still one `PyColumnParts` call, no Python text assembly); full suite 6141 passed / 368 skipped. No Rust touched — `make verify`/`check_rust_file_size.py` not required. Six registry rows carry pins. maps: spark/map.md, tests/map.md, examples/column/map.md, staging/map.md. pins: column-parity-1/C-007 |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decisions

| Name | Disposition | Reason |
|---|---|---|
| `Column.isin` | implemented | OR-of-equalities matches SQL `IN` semantics on every pinned cell incl. NULL/NaN/empty; tuple → `PySparkRuntimeError` `UNSUPPORTED_FEATURE.LITERAL_TYPE` (COL-ISIN-TUPLE-1) |
| `Column.isNaN` | implemented | `isnan(CAST(x AS DOUBLE))` reusing `functions_expr.isnan`; string input raises the engine cast refusal at collect, matching Spark's refusal class |
| `Column.astype` | implemented | `cast` plus source-name preservation (`i`/`i` duplicate output names reproduced) |
| `Column.name` | implemented | `alias` delegate; `metadata=` now lands on the projected `StructField`; multi-name refusal pinned (COL-NAME-MULTI-1) |
| `Column.outer` | implemented | same-inner Column + `lazy(...)` display + `_outer` marker (R-1); correlated cells deferred to the scalar/exists card |
| `Column.withField` | implemented | deferred select-boundary struct rebuild; empty name → `col{index}` divergence (COL-WITHFIELD-EMPTY-1) |
| `Column.dropFields` | implemented | same rebuild; no-arg `tail of empty list` (R-2), non-str `NOT_STR` (R-3, COL-DROPFIELDS-TYPE-1) |
| `getField` after `withField` | implemented | `("item", key)` op recorded on the pending column, applied post-resolution |
| SQL `IN` | pinned + BACKLOG | NULL/empty cells match; mixed-type list refused → `SQL-IN-1` (parser owned by run 15c) |
| SQL `isnan` | pinned + BACKLOG | numeric cells match; string literal refused → `SQL-ISNAN-1` |
