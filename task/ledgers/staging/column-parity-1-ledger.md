# Charter ledger — COLUMN-PARITY-1 · the seven-name Column surface (isin / isNaN / astype / name / outer / withField / dropFields)

**Date:** 2026-09-14 · **Branch:** `feat/column-parity-1` · **Base:** `7693ef23`
(round 1 committed as `2aa65406`; this round rebases the follow-up onto it) ·
**Model:** swe-2-high (round 1) + muse-spark-1.3-contributor (critic round + re-check round 2, 2026-09-15) ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Round 2 base:** `99b456c8` (rebase of `feat/column-parity-1` onto `origin/main` post-#600; both sides kept).
**Registry:** `COL-DROPFIELDS-TYPE-1` and `COL-ISIN-TUPLE-1` filed DECLARED (§5);
`SQL-IN-1`, `SQL-ISNAN-1`, `COL-WITHFIELD-EMPTY-1`, `COL-NAME-MULTI-1`,
`COL-DOTTED-FIELD-1`, `COL-ISIN-1` filed BACKLOG (§7); round 2 files BACKLOG
`COL-GROUPKEY-NAME-1` (§7) and appends the chain pin to `COL-WITHFIELD-EMPTY-1`.

**Why now.** The 1.5 PySpark-parity campaign: every public PySpark 4.1.2 `Column` name is
present on the facade and answers Spark on both doors, or carries a dated declared
refusal. This unit ships the seven names the card scopes, driven by the recorded live
oracle fixture `facade_column_oracle.json` (`col.*` cells, PySpark 4.1.2 classic,
`local[2]`, UTC, 2026-09-14).

**Design choice (owned by this unit, recorded per the card).** Round 1 built
`withField` / `dropFields` as **deferred select-boundary columns**; the critic (L-001
.. L-004) proved that design wrong and the orchestrator ruled a Rust redesign (R-4),
which this round implements. `withField` / `dropFields` now build Spark's
`UpdateFields` as a real DataFusion `ScalarUDF` (`update_fields`) in `repark-core`,
at the crate root beside the `stack` internal-UDF precedent (`stack.rs` holds the
unit's own UDF the same way; NOT `repark-functions`, NOT the `repark-spark` function
registry, NOT `column/function_dispatch.rs`, NOT the SQL parser). The UDF takes the
struct expression plus literal op tags / field paths plus value expressions, applies
edits sequentially in `return_field_from_args` (case-insensitive, every duplicate
replaced, new spelling wins, `with` appends, `drop` is a no-op when absent, dotted
paths walk nested structs, the three Spark error classes), and the kernel masks every
output child with the parent validity so a NULL struct (top or intermediate) stays
NULL through field extraction (the same union-validity pattern as
`dynamic_flatten/null_mask.rs`). The facade binds one `PyColumnParts.update_fields`
call per `withField` / `dropFields` (chains nest); the deferred machinery
(`_struct_edits`, `resolve_struct_edit_column`, the dummy column, the select/filter
hooks) is deleted. `isin` builds one native `IN`-list (`in_list`; empty → `lit(False)`)
instead of a left-deep OR chain (the perf reviewer's P1-1: quadratic build + 12.9 s
plan at 1 000 literals + SIGSEGV at 10 000). `isNaN` calls the `repark_isnan` engine
UDF (float/string test DOUBLE with a strict cast; date/timestamp/boolean/int/decimal
answer false). A Python-only route was allowed only for arg checks and name binding;
the ledger's per-name table states the one-line why per name. `column.py` stays near
its exact line baseline (baseline re-recorded at the new exact count); bodies live in
`column_fields.py`.

**Not in this unit:** correlated `outer` positions (`outer_in_scalar`,
`outer_in_exists` — the DataFrame `scalar`/`exists`/`lateralJoin` card, R-1); SQL-door
fixes (run 15c owns the parser — SQL-IN-1 / SQL-ISNAN-1 stay BACKLOG rows); dotted
`col("st.a")` resolution (COL-DOTTED-FIELD-1); generator two-column naming
(COL-NAME-MULTI-1, now first-name-wins with a red-capable pin).

## PROPOSITION LEDGER — COLUMN-PARITY-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `isin_*` DataFrame-door cell answers Spark: values, schema, output names, and error classes. | `test_column_parity_1.py::test_isin_*`, 14 cells, red on base. | **PROVEN** | 14/14 red on base (`TypeError: Column object has no attribute isin`), all green after round 1 (OR-of-`=` arms) and still green after the critic round rewires to one native `IN`-list (`PyColumnParts.in_list`; empty list stays `lit(False)`): `1 IN (1,NULL)` true, `2` NULL, NULL input NULL, empty false incl. NULL, NaN ≡ NaN, same `(i IN (1, 2))` / `(i IN ())` names. List/set flattened, tuple refused `UNSUPPORTED_FEATURE.LITERAL_TYPE` via `PySparkRuntimeError` (COL-ISIN-TUPLE-1). String-vs-int and int-vs-string raise the engine cast error at collect — BACKLOG COL-ISIN-1 (repark `Cannot cast` vs Spark `CAST_INVALID_INPUT`), pins assert today's class and message. pins: column-parity-1/C-001 |
| C-002 | `isNaN`, `astype`, and `name` cells answer Spark. | `test_isnan_*`, `test_astype_*`, `test_name_*`, 10 cells, red on base. | **PROVEN** | 10/10 red on base, all green after round 1 and still green after the critic round rewires `isNaN` to the `repark_isnan` engine UDF (float NaN mask, NULL false, string strict-DOUBLE coercion → cast error at collect; date/timestamp/boolean/int/decimal false per L-007); names `isnan(d)` etc. unchanged. `astype` = `cast` + name preservation so `i.astype("string")` and `i.astype(LongType())` both project `i` (`struct<i:string,i:bigint>`); `astype(1)` → `PySparkTypeError` `NOT_DATATYPE_OR_STR` `{"arg_name": "dataType", "arg_type": "int"}`. `name` = `alias`, incl. `metadata=` on the projected `StructField` via `DataFrame._field_metadata`; a later `alias`/`name` without `metadata=` now drops earlier metadata (L-009, Spark builds a new alias); multi-name is first-name-wins on a working generator (COL-NAME-MULTI-1, L-005 fix). pins: column-parity-1/C-002 |
| C-003 | `outer` answers the column unchanged in a plain select and carries the `_outer` marker; correlated cells out of scope. | `test_outer_*`, 3 cells, red on base. | **PROVEN** | 3/3 red on base, all green. `outer()` returns a Column on the same `_inner` with `lazy(<child>)` display (`repr` `Column<'lazy(i)'>`) and `_outer=True`; plain select answers `[1, 2, None]` named `i`. `outer_in_scalar` / `outer_in_exists` are the DataFrame scalar/exists/lateralJoin card (ruling R-1), not pinned here. pins: column-parity-1/C-003 |
| C-004 | Every `withfield_*` cell plus `getfield_after_withfield` answers Spark. | `test_withfield_*`, `test_getfield_after_withfield`, 15 cells, red on base. | **PROVEN** | 15/15 red on base, all green after round 1 (deferred rebuild) and still green after the critic round replaces the rebuild with the native `update_fields` UDF. Add/replace/retype (incl. `void` for `lit(None)`), case-insensitive match with new spelling winning (`A` → `struct<A:int,b:string>`), every duplicate-name field replaced, dotted nested paths (`inner.x` add + replace), missing parent → `AnalysisException` `FIELD_NOT_FOUND` ``No such struct field `nope` in `a`, `inner` `` at select, NULL intermediate and NULL top-level structs stay NULL (kernel masks output children with the parent validity), non-struct → `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` at select, arg gates `NOT_STR`/`NOT_COLUMN` at call, name `update_fields(st, WithField(9))`, `getField` post-edit works in every position (filter/`==`, `when`, arithmetic, `orderBy`, `groupBy`, join, nested value, `outer()`). Two divergences stand: empty name → `col{index}` (COL-WITHFIELD-EMPTY-1); `col("st.a")` as the replacement value still fails `No field named st.a` (COL-DOTTED-FIELD-1). pins: column-parity-1/C-004 |
| C-005 | Every `dropfields_*` cell answers Spark. | `test_dropfields_*`, 10 cells, red on base. | **PROVEN** | 10/10 red on base, all green after round 1 and still green after the critic round replaces the rebuild with the native `update_fields` UDF. One/several/nested drops, case-insensitive, missing name and missing nested name are no-ops, dropping all → `DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS`, non-struct → `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, no-arg → `UnsupportedOperationException` `tail of empty list` (R-2), non-str → `PySparkTypeError` `NOT_STR` (R-3, COL-DROPFIELDS-TYPE-1), name `update_fields(st, dropfield())`. pins: column-parity-1/C-005 |
| C-006 | The four SQL-door cells are pinned — matching cells match, diverging cells carry BACKLOG registry rows. | `test_sql_*`, 4 cells. | **PROVEN** | `sql_in_null` `[True, None, None, True]` and `sql_isnan` `[True, False, False]` already matched Spark and are pinned byte-for-byte. `sql_in_mixed` → BACKLOG `SQL-IN-1`, corrected per L-006 to an error-class divergence (both engines refuse; repark `Cannot cast` vs Spark `CAST_INVALID_INPUT`); `isnan_sql_string` diverges (SQL-door `isnan` takes `Utf8` uncoerced) → BACKLOG `SQL-ISNAN-1`; both pins codify today's refusal. `withField`/`dropFields`/`astype`/`name`/`outer` have no SQL spelling — pinned as `Invalid function` refusals (L-011 fix: the old `assert True` now asserts all five spellings refuse). pins: column-parity-1/C-006 |
| C-007 | No regression: column tests, `test_functions_split_identity`, API inventory/freeze, registry pins, maps in lockstep. | The named files + gates. | **PROVEN** | Round 1: `test_column_access.py` + `test_columns.py` + `test_column_x1_census.py` + `test_column_parity_1.py` + `test_functions_split_identity.py` + `test_t0_df_regions_import_freeze.py` — 157 passed; full suite 6141 passed / 368 skipped. Critic round: full facade suite 6331 passed / 368 skipped; full parity suite 757 passed / 2 skipped / 12 xfailed. `check_example_coverage.py` green (`struct_fields.py` still covers all seven names; parity surface count 930 → 937, column family 40 → 47, recorded in-test per the brief). `ruff check` / `ruff format --check` / `check_lib_py.py` / `check_rust_file_size.py` green (`column.py` 1548 → 1532 / `core.py` 4044 → 4040 / `plan_collapse.py` 1057 → 1054 re-recorded exact in both tables); `make verify` green except the sanctioned missing-ATTESTATION finding. Round-1 debt repaired here: `build_api_freeze.py` follows `alias = _module.func` bindings (the `column_fields` split had blinded the frozen `Column.between` params; pins exact again), the pr245 literal inventory gains the `column.py` row, the eight `join_sql` golden bytes for struct field access moved via the sanctioned re-record (R-4 join requirement; `sql_expr` bytes and the hostile-ident pin unchanged). Registry rows all carry pins. maps: spark/map.md, dataframe/map.md, tests/map.md, parity-tests/map.md, scripts/map.md, core/src/map.md + new update_fields/map.md + isnan/map.md, column/map.md, session/map.md, staging/map.md. pins: column-parity-1/C-007 |
| C-008 | Critic round: `UpdateFields` in Rust (R-4) + L-005..L-010; every existing `withfield_*` / `dropfields_*` cell still green. | The 13 new pins + Rust unit tests, red on the deferred tree. | **PROVEN** | Red first on the deferred tree: 11 failed, 1 passed (`test_withfield_chain_replaces_just_added`, `test_withfield_nested_chain_replaces_just_added`, `test_withfield_then_drop_just_added`, `test_dropfields_then_with_appends`, `test_struct_edit_in_filter_eq`, `test_struct_edit_in_filter_drop_and_null`, `test_struct_edit_in_select_expr_and_when`, `test_struct_edit_in_orderby_groupby`, `test_struct_edit_in_join_and_nested_value`, `test_isnan_non_float_types`, `test_name_later_alias_drops_metadata` — duplicate-`c` schemas, `No field named __repark_unresolved_struct_edit__`, `Invalid function 'withfield'`, `Unsupported CAST Date32 to Float64`, sticky metadata; `test_withfield_case_conf_no_effect` passed pre/post by design since it locks the unwired-conf behavior), plus `test_isin_wide_literal_list` SIGSEGV (exit 139, P1-1) on the old OR chain. All 13 green after; Rust `update_fields` (19 tests) + `isnan` (6 tests) green. Sequential answers (add-then-replace keeps one `c = 2`, add-then-drop removes, drop-then-add appends `a` after `b`, every L-004 position answers) are UNMEASURED on live Spark — Spark's documented `UpdateFields` sequential rule, listed for the next oracle round — as are L-007 date/timestamp/boolean, L-009 metadata drop, and the L-010 conf premise (`spark.sql.caseSensitive` is stored but read by no engine path, so matching stays case-insensitive). pins: column-parity-1/C-008 |

| C-009 | Re-check round (L-101..L-103, perf P2): struct/array/map `isNaN` refuses at plan; unaliased group-key names and the empty-name chain carry pins; the masked-path timing is recorded. | The 5 new pins + 3 new Rust tests, L-101 red on the pre-fix tree. | **PROVEN** | Red first: `test_isnan_struct_refuses` + `test_isnan_array_refuses` FAILED pre-fix (`DID NOT RAISE AnalysisException` — both answered False); green after the `return_field_from_args` refusal + release rebuild. The 2 group-key pins and the empty-chain pin codify today's answers and passed pre/post by design. Date/timestamp/boolean `isNaN` pins still answer false. Timing (1e6 rows, 20-field struct, 10 % null parents, `withField` + `to_arrow`, median of 5 after 1 warmup): 37.1 ms before, 31.8 ms after (final binary). Rust `update_fields` (19 tests) + `isnan` (9 tests) green. The exact Spark refusal wording stays UNMEASURED live (no JVM in this lane); only the class, the function name and the type name are pinned. pins: column-parity-1/C-009 |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decisions

| Name | Disposition | Reason |
|---|---|---|
| `Column.isin` | implemented | one native `IN`-list (`in_list`; Rust-first kernel, Python binds shapes) matches SQL `IN` on every pinned cell incl. NULL/NaN/empty/Column operands; tuple → `PySparkRuntimeError` `UNSUPPORTED_FEATURE.LITERAL_TYPE` (COL-ISIN-TUPLE-1); 10 000 literals plan fast, no deep OR tree |
| `Column.isNaN` | implemented | `repark_isnan` engine UDF (Rust-first; Python binds the name): float NaN mask, string strict-DOUBLE coercion; date/timestamp/boolean/int/decimal answer false (L-007) |
| `Column.astype` | implemented | `cast` plus source-name preservation (`i`/`i` duplicate output names reproduced) |
| `Column.name` | implemented | `alias` delegate; `metadata=` lands on the projected `StructField`; later alias without `metadata=` drops it (L-009); multi-name first-name-wins (COL-NAME-MULTI-1, L-005) |
| `Column.outer` | implemented | same-inner Column + `lazy(...)` display + `_outer` marker (R-1); correlated cells deferred to the scalar/exists card |
| `Column.withField` | implemented | `update_fields` engine UDF (Rust-first; Python binds shapes, arg gates stay Python): sequential edits, NULL-masked kernel; empty name → `col{index}` divergence (COL-WITHFIELD-EMPTY-1) |
| `Column.dropFields` | implemented | same UDF; no-arg `tail of empty list` (R-2), non-str `NOT_STR` (R-3, COL-DROPFIELDS-TYPE-1) |
| `getField` after `withField` | implemented | native `get_field` on the `update_fields` expression; struct field SQL renders bracket form so join conditions parse |
| SQL `IN` | pinned + BACKLOG | NULL cell matches; mixed-type list refused by both engines with different classes → `SQL-IN-1` (parser owned by run 15c) |
| SQL `isnan` | pinned + BACKLOG | numeric cells match; string literal refused → `SQL-ISNAN-1` |
| `Column.isNaN` on struct/array/map | implemented | planning refusal in `repark_isnan` (`return_field_from_args`, L-101, Rust-first): `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` naming `isnan(<arg>)` and the Spark type; date/timestamp/boolean pins still answer false |
| Unaliased `groupBy(<expr>)` key name | pinned + BACKLOG | engine expression string for any non-column key (L-102): `COL-GROUPKEY-NAME-1`; groups and counts correct |
| `withField("", ...)` chain | pinned | second `""` appends `col3` (L-103); pin appended to `COL-WITHFIELD-EMPTY-1` |

## Critic-round observations (2026-09-14)

- Struct field access now carries a join-ON-only bracket fragment
  (`(child)['field']`, via `PyColumnParts.field_join_sql`): this DataFusion version
  refuses dot access on parenthesized expressions even for plain columns, so join
  conditions over plain `getField` failed identically before this round. Free-SQL
  `sql_expr` keeps the dot form byte-identical (the hostile-ident pin and the
  `sql_expr` goldens are untouched; only the eight `join_sql` golden bytes moved, via
  the sanctioned re-record). The plain-join repair is a side effect, reported in the
  handback.
- Bare `getField` on a NULL struct leaks the child default (measured: `0` for int in
  a groupBy key); `update_fields` output masks children with the parent validity, so
  its extractions answer NULL per Spark. The bare-`getField` leak is engine behavior
  outside this unit.
- A `withField` column over an ambiguous post-join struct behaves like every other
  compound expression (no origin rebind inside UDF args); single-struct frames are
  unaffected.
- The parity-audit skill's live-measurement steps (JVM banner, `make parity-live`)
  belong to the orchestrator's live tier; this lane cannot start a JVM. All
  UNMEASURED answers above are listed for the next oracle round.

## Re-check round 2 (2026-09-15, L-101..L-103 + perf P2)

- L-101 is a Rust planning refusal because the argument type is known at plan time
  and every sibling analysis error already refuses there; `update_fields::spark_sql_type`
  is now shared (`pub(crate)`) so the named type cannot drift from the sibling mapping,
  with `STRUCT<>` / `ARRAY<>` / `MAP<>` rendering for containers. The exact Spark refusal
  wording is UNMEASURED live (no JVM in this lane); only the error class, the function
  name and the type name are pinned. `F.isnan` is untouched (it already refuses
  non-numeric arguments through its own kernel).
- Perf P2 landed as a bitmap combine, not the literal drop: dropping the child mask
  would red the pinned `null_parent_child_values_masked`,
  `null_intermediate_child_values_masked` and `with_appends_new_field` kernel tests
  (a null child under a null parent is the pinned contract), and `nullif` never
  deep-copied values — it already rebuilt `ArrayData` on shared buffers, so the real
  saving is the `Vec<bool>` / `BooleanArray` roundtrip. The combine keeps a `Null`-type
  early return (`withField("a", lit(None))` void replacement cannot carry a null
  bitmap — caught by the C-004 pin before commit). Timing (1e6 rows, 20-field
  struct, 10 % null parents, `withField` + `to_arrow`, median of 5 after 1 warmup):
  37.1 ms before, 31.8 ms after (final binary).
- Perf P3s are ledger notes only, no change: untargeted filter/schema microseconds
  and the `nan_mask` scalar row loop stay as the re-check measured them; neither is
  user-visible at production scale.
- Rebase collision: `origin/main` #600 implements `withMetadata` / `to` through
  `alias(name, metadata=)`, which the round-1 `_field_metadata` overlay surfaced on
  `schema` (3 DF-METADATA-1 pins + the `schema_reconcile` example went red on the
  rebased tree). This round projects a plain rename at both `surface_a.py` call sites
  (the engine has no metadata plumbing, so the stamped dict drops per DF-METADATA-1)
  and trues up the row's mechanism sentence; `name(..., metadata=)` keeps its C-002
  oracle pin and `alias` validation is untouched.
- The parity-audit skill's live tier (JVM banner, `make parity-live`) stays with the
  orchestrator; every Spark-side wording above is labeled UNMEASURED for the next
  oracle round. pins: column-parity-1/C-009

## Orchestrator rulings after round 2 (run 15b, G-2)

- R-5 (2026-09-15): round 2's rebase repair stripped `metadata=` from `DataFrame.to` and `withMetadata` (merged DF-SURFACE-A-1) so
  that the DF-METADATA-1 loss pins stayed green. That removed Spark-matching behaviour to preserve a BACKLOG pin — the registry's
  retirement rule runs the other way. The orchestrator reverted both hunks, measured every position with this branch's alias
  metadata (stamp `{"k": "v"}`, replace `{"j": 1}`, cache `{"k": "v"}`, `to()` target override `{"tgt": "2"}` now answer Spark;
  `filter`, `select`, `withColumn`, `join`, `union`, parquet round trip and `to()` source-keep still `{}`), re-pinned the four
  Spark-matching positions and narrowed DF-METADATA-1 to the rest. pins: column-parity-1/C-009

