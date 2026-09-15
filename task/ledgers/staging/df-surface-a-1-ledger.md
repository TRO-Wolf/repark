# Charter ledger — DF-SURFACE-A-1 · seven-name DataFrame surface (critic round 1)

**Date:** 2026-09-14 · **Branch:** `feat/df-surface-a-1` · **Base:** `bc024735` (rebased;
implementation `f20e94e7` → `5607d926` onto FACADE-4 step 1 `7693ef23` + ROW-TUPLE-1) ·
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-TO-1` and `DF-CHECKPOINT-1` DECLARED in
`docs/spark-sql-iceberg-parity.md` §5; `LOGICAL-WIDTH-1` gains the `to_narrow` pin;
`DF-METADATA-1` filed BACKLOG in §7; `EX-DF-11` untouched.

**Scope.** The 1.5 PySpark-parity campaign's DataFrame card, after critic round 1's
binding rulings: seven names stay on the facade — `to`, `withMetadata`,
`registerTempTable`, `checkpoint`, `sparkSession`, `isLocal`, `executionInfo` — each
answering the live PySpark 4.1.2 oracle cells in `facade_dataframe_surface_oracle.json`
(copied unchanged to `python/repark/tests/`), or a dated DECLARED divergence with
Spark's own error class. `inputFiles` and `semanticHash` left the unit under R-5 to
the future Rust unit `DF-PLAN-INTROSPECT-1` (plan introspection belongs in Rust; the
EXPLAIN-text path broke on real inputs — L-006, L-007, L-009).

**Critic round 1 rulings (binding, orchestrator 2026-09-14).**

- **R-5 — scope split.** `inputFiles` and `semanticHash` removed completely:
  bindings, `surface_a.py` bodies, pins, inventory/freeze rows, map rows — moved to
  "Out of scope" below.
- **R-6 — no schema sticker.** `_schema_override` deleted (slots, init, schema
  reporting, freeze tables, docs). `to()` builds a real projection; `schema` reports
  the engine's plan. The `to(struct<a:smallint>)` → `int` report is existing
  LOGICAL-WIDTH-1; the `to_narrow` pin codifies it.
- **R-7 — metadata rides the engine.** `withMetadata` and `to()`'s metadata arms go
  through `Column.alias(name, metadata=...)`. The engine drops field metadata at every
  measured position — `filter`, `select`, `withColumn`, `join`, `union`,
  `cache`/`eager`, parquet round trip, `to()` both arms — so no Python sticker:
  `DF-METADATA-1` BACKLOG pins codify today's loss. L-010 (Spark's rule: `to()` keeps
  source metadata unless the target carries non-empty metadata) is pinned as the same
  loss.
- **R-8 — store assignment.** L-001: atomic → `StringType` allowed (numeric, boolean,
  date, timestamp, decimal); string → numeric/boolean/date and boolean → numeric stay
  refused `INVALID_COLUMN_OR_FIELD_DATA_TYPE`; decimal → decimal allowed only on
  widening (integer digits and scale do not shrink). L-002: nested reconciliation by
  name — `named_struct` for structs (reorder, drop extra, NULL-fill missing nullable,
  refuse missing non-nullable), `transform` for array elements,
  `transform_values`/`transform_keys` for maps; no `struct<…>`/`array<…>` type strings
  to `cast` (engine raises `ParseException: unknown cast type`).
- **R-9 — owning session.** `sparkSession` returns the creating facade session even
  after a `newSession` promotion: `_alive_token["facade_session"]` stores the owner at
  session construction (one packed dict line keeps `session_core.py`'s exact 2304
  baseline).

**Red-first.** Round 1: `test_df_surface_a_1.py` ran on the base tree before any edit —
all 20 pins failed (`PySparkAttributeError [ATTRIBUTE_NOT_SUPPORTED]`, names absent).
Round 2 (this remediation): the rewritten pin file ran against `5607d926` (the
pre-remediation commit) — 11 pins red, codifying the critic findings before the fix:

- L-001 / R-8: `test_to_store_assignment_atomic_to_string` (long→string refused on the
  old matrix — Spark allows it), `test_to_store_assignment_refusals` (decimal widening
  refused on the old matrix).
- L-002 / R-8: `test_to_nested_identity_roundtrip`,
  `test_to_nested_struct_reconcile`, `test_to_nested_array_map_element_cast` (the old
  code passed `struct<…>`/`array<…>` strings to `cast` → `ParseException: unknown cast
  type`).
- L-005 / R-9: `test_spark_session_survives_new_session_promotion` (the old property
  followed the promoted session, not the creator).
- L-008: `test_with_metadata_unknown_column_raises_unresolved` (the old miss raised a
  bare `AnalysisException` with no `UNRESOLVED_COLUMN.WITH_SUGGESTION` attachment).
- R-6 / R-7 codifications: `test_to_narrow_reports_logical_width_1`,
  `test_with_metadata_dropped_at_stamp_df_metadata_1`,
  `test_with_metadata_dropped_across_positions_df_metadata_1`,
  `test_to_metadata_arms_drop_df_metadata_1`.

## PROPOSITION LEDGER — DF-SURFACE-A-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `to(schema)` answers the `to_*` oracle cells: output columns are the schema's fields in its order, matched case-insensitively with the schema's spelling winning, extra source columns dropped, a nullable miss fills NULL, a non-nullable requirement fed by a nullable source raises `NULLABLE_COLUMN_OR_FIELD` in Spark's exact text, and a refused cast raises `INVALID_COLUMN_OR_FIELD_DATA_TYPE` in Spark's exact text. | `test_df_surface_a_1.py` `to_*` pins, red on base. | **PROVEN** | Cells `to_reorder_cast`, `to_case`, `to_narrow`, `to_missing`, `to_nullability`, `to_bad_cast`, `to_not_schema` red on base (name absent), green after; `to_narrow` additionally pinned under LOGICAL-WIDTH-1 in round 2 (the engine reports `int` for `smallint`, no sticker per R-6). Store assignment follows R-8: numeric↔numeric both directions, atomic→string, decimal widening; refused pairs: string→numeric/boolean/date, boolean→numeric, decimal shrinking digits or scale — each `INVALID_COLUMN_OR_FIELD_DATA_TYPE` in Spark's text. pins: df-surface-a-1/C-001 |
| C-002 | `withMetadata(columnName, metadata)` answers the `withMetadata*` cells: `NOT_DICT` on a non-dict, `UNRESOLVED_COLUMN.WITH_SUGGESTION` on a miss (L-008), replace-not-merge, order unchanged; metadata rides the `alias(metadata=)` path with today's loss codified by `DF-METADATA-1`. | `test_df_surface_a_1.py` `withMetadata*` pins. | **PROVEN** | Cells `withMetadata`, `withMetadata_replaces`, `withMetadata_order`, `withMetadata_not_dict`, `withMetadata_missing` red on base, green after; the miss now raises `AnalysisException` with `_spark_error_class`/`getErrorClass()`/`getCondition()` = `UNRESOLVED_COLUMN.WITH_SUGGESTION` attached through the `_integral` helper pattern (L-008 red: bare exception before). The engine's alias ignores `metadata` — survival measured at stamp/filter/select/withColumn/join/union/cache/eager/parquet-round-trip and `to()`'s L-010 arms, all loss, filed BACKLOG `DF-METADATA-1`; no Python sticker (R-7). pins: df-surface-a-1/C-002 |
| C-003 | `registerTempTable` warns `FutureWarning`, returns `None`, delegates to `createOrReplaceTempView`, replaces an existing view; `checkpoint(eager)` answers `localCheckpoint(eager)` on a new frame; `sparkSession` is the owning facade session by identity, surviving `newSession` promotion (R-9). | `test_df_surface_a_1.py` `registerTempTable*`/`checkpoint*`/`sparkSession*` pins. | **PROVEN** | Cells `registerTempTable`, `registerTempTable_return`, `registerTempTable_replace`, `checkpoint_with_dir`, `checkpoint_lazy`, `sparkSession_is`, `sparkSession_type` red on base, green after. `checkpoint` spawns `_identity_child()` then runs the `localCheckpoint` body — new frame, same rows. `sparkSession` reads `_alive_token["facade_session"]` (R-9): `test_spark_session_survives_new_session_promotion` was red pre-fix (the property followed the promoted session), green after. `type(df.sparkSession)` is repark's `ReparkSession`, exported as `SparkSession`; the pin asserts class identity. pins: df-surface-a-1/C-003 |
| C-004 | `isLocal()` answers `False` on every measured shape (R-4). | `test_df_surface_a_1.py` `isLocal*` pins. | **PROVEN** | `isLocal` cells red on base, green after. Measured shapes: `createDataFrame`, `range`, parquet read, `select 1`, `values`, filtered local, empty — all `False`. `inputFiles` left the unit under R-5 (was this clause's second half; see Out of scope). pins: df-surface-a-1/C-004 |
| C-005 | `executionInfo` raises `PySparkValueError` `CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF` with Spark's exact message. | `test_df_surface_a_1.py` `executionInfo` pin. | **PROVEN** | Cell `executionInfo` red on base, green after — Spark classic's own answer verbatim. `semanticHash` left the unit under R-5 (was this clause's second half; see Out of scope). pins: df-surface-a-1/C-005 |
| C-006 | Registry rows `DF-TO-1`/`DF-CHECKPOINT-1` (DECLARED, §5) and `DF-METADATA-1` (BACKLOG, §7) land dated with pins; `LOGICAL-WIDTH-1`'s pin list gains `to_narrow`; the fixture is copied unchanged; every touched directory's `map.md` updates in the same commit; the freeze tables name exactly the seven names. | The files and the freeze test. | **PROVEN** | §5 carries `DF-TO-1` (R-1) and `DF-CHECKPOINT-1` (R-3) citing their cells; §7 carries `DF-METADATA-1` with the per-position loss pins; `LOGICAL-WIDTH-1` cites `test_to_narrow_reports_logical_width_1`; `facade_dataframe_surface_oracle.json` is a byte-identical copy; `dataframe/map.md`, `session/map.md`, `tests/map.md`, `parity-tests/map.md`, `scripts/map.md`, `docs/examples/dataframe/map.md`, `task/ledgers/staging/map.md` all carry the round's rows; `_dfcore_1_expected.py` names the seven names, `surface_a` in both submodule sets, and no `_schema_override`/`inputFiles`/`semanticHash`. pins: df-surface-a-1/C-006 |
| C-007 | No regression: `test_dataframe*.py`, the API inventory/freeze test, and the `sameSemantics` pins stay green alongside the pin file. | The suites + gates. | **PROVEN** | `test_dfcore_1_exports.py` and `test_dfcore_4b_exports.py` green with the seven-name tables; the full C-007 suite and both full-suite runs are recorded under Gates. `sameSemantics` is byte-identical. pins: df-surface-a-1/C-007 |
| C-008 | Critic round 1 lands completely: R-5..R-9 applied, the seven-name example coverage exists (`schema_reconcile.py`, `session_and_checkpoint.py`), and the L-001/L-002/L-005/L-008 findings have red-first evidence and green pins. | The red run above + the two example scripts + gates. | **PROVEN** | Red evidence (11 failing pins on `5607d926`, listed in Red-first); post-fix `test_df_surface_a_1.py` 24 passed. Examples: `schema_reconcile.py` covers `DataFrame.to`/`withMetadata`/`registerTempTable`; `session_and_checkpoint.py` covers `DataFrame.isLocal`/`executionInfo`/`sparkSession`/`checkpoint`; neither mentions `inputFiles`/`semanticHash`; the example inventory moves 932 → 939 (dataframe family 153 → 160) and `test_ex_0_example_coverage.py` pins it. `check_example_coverage.py` clean; `check_ledger_grammar.py` clean except the orchestrator-owned `COVERAGE_ATTESTATION` block. pins: df-surface-a-1/C-008 |

## Per-name decisions

| Name | Disposition | Reason |
|---|---|---|
| `to` | implemented | Answers all `to_*` cells incl. the DF-TO-1 declared `NOT_STRUCT` refusal; real projections under R-8 store assignment, nested shapes reconciled through `named_struct`/`transform`/`transform_values`/`transform_keys` (R-8); no `_schema_override` (R-6). |
| `withMetadata` | implemented | Answers all `withMetadata*` cells over `alias(metadata=)`; missing column carries `UNRESOLVED_COLUMN.WITH_SUGGESTION` (L-008); engine-side metadata loss codified `DF-METADATA-1` (R-7). |
| `registerTempTable` | implemented | `FutureWarning` + `None` + `createOrReplaceTempView` delegation, replaces. |
| `checkpoint` | implemented (declared divergence) | R-3: `localCheckpoint` semantics on a new frame; Spark's `checkpoint_nodir` refusal and reliable-storage write are DF-CHECKPOINT-1. |
| `sparkSession` | implemented | Owning-session identity via `_alive_token["facade_session"]` (R-9), pinned through `newSession` promotion. |
| `isLocal` | implemented | R-4: always `False` on every measured shape. |
| `executionInfo` | implemented | Spark-classic refusal verbatim (`CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF`). |
| `inputFiles` | moved | R-5 — to Rust unit DF-PLAN-INTROSPECT-1 (EXPLAIN-text plan reads broke on real inputs: L-006, L-007). |
| `semanticHash` | moved | R-5 — to Rust unit DF-PLAN-INTROSPECT-1 (L-009; plan introspection is Rust-first). |

## Out of scope — moved to DF-PLAN-INTROSPECT-1

`inputFiles()` and `semanticHash()` were implemented in round 1 (`f20e94e7`, rebased
`5607d926`) as EXPLAIN-text readers. The critic found the approach unsound on real
inputs (L-006: the physical plan does not enumerate every scanned file on all readers;
L-007: `file_groups` parsing is reader-shape-dependent; L-009: plan text is not a
stable fingerprint source). Under R-5 and the owner's Rust-first rule, both names are
removed from this branch — bindings in `core.py`, bodies in `surface_a.py`, pins in
`test_df_surface_a_1.py`, freeze-table rows, map rows — and return in
`DF-PLAN-INTROSPECT-1`, a Rust unit that reads the plan natively. The round-1
`semanticHash`/`sameSemantics` relation note is preserved here: `sameSemantics`
answers handle identity (EX-DF-11) while `semanticHash` was to answer plan equality;
on the pinned shapes identity implied equal hash, not conversely — the Rust unit
re-derives this.

## Gates

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_df_surface_a_1.py -q` | 24 passed |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests/test_api_freeze.py -q` | 22 passed |
| `.venv/bin/python -m pytest python/repark/tests/test_dfcore_1_exports.py python/repark/tests/test_dfcore_4b_exports.py` | 11 passed |
| `python3 scripts/check_lib_py.py` | clean — `core.py` exact baseline 4041 → 4035 in both files; `session_core.py` held at 2304 (packed dict line) |
| `python3 scripts/check_example_coverage.py` | clean — inventory 932 → 939 (excl. plumbing), dataframe family 153 → 160, backlog baseline unchanged |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests/test_ex_0_example_coverage.py -q` | 26 passed |
| `python3 scripts/check_ledger_grammar.py` | clean except the missing `COVERAGE_ATTESTATION` block (orchestrator-owned, per the brief) |
| Full facade suite / full parity suite / `uvx ruff@0.15.22` check + format / `typos` | recorded in the final commit's handback gates table |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.
