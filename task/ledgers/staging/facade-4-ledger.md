# Unit ledger — FACADE-4 · type conversions in Rust — step 0

**Date:** 2026-09-14 · **Branch:** `perf/facade-4-s0` (C-001..C-008) · **Base:** `2bebc9da`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-4 (audit §8): Arrow schema ↔ Spark types ↔ DDL move into one
Rust table; `spark/types.py` keeps the Python classes (F1 frozen except `VariantType`)
and thins to checks plus conversion calls; K1 keeps DDL spellings byte-identical.
Step 0 is measurement and pins only — the release baseline cells (conversion isolated
per schema, plus conversion's share of end-to-end walls), the three-table agreement
census (facade `types.py` conversions vs `_csv_smart` rungs vs the reader lattice and
Rust `spark_ddl_type_name` / `arrow_type_key`), the DDL/Arrow goldens with a mutation
proof, and the step-1 target list. No product code under `python/repark/src/` or
`crates/` changes in this step.

**Not in this step:** `STATUS.md`, anything under `python/repark/src/` or `crates/`,
and (owned by another session tonight) `dataframe/core.py`, `dataframe/eager.py`,
`dataframe/cache_handle.py`, `spark/catalog.py`, `spark/functions_collections.py`,
`crates/repark-core/src/session/temp_views.rs`. No JVM, no parity-live legs.
Step 1 is a later round.

## PROPOSITION LEDGER — FACADE-4 step 0 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Step 0 lands with zero product change: no file under `python/repark/src/` or `crates/` in the branch diff; the native in `.venv` is a release build for every timing cell. | `git diff origin/main -- python/repark/src crates/` empty; `repark._native.__debug_assertions__ is False`. | **OPEN** | Pending gates. |
| C-002 | DDL goldens cover every F1 type class: for each of the 25 public type classes plus StructField/StructType shapes — `simpleString`, `typeName`, `json`, `repr`, `fromDDL` round-trip (parse → simpleString → parse), `StructType.toDDL` re-parse where the base tree permits, and `json` → `fromJson`/`_parse_datatype_json_value` round-trip — recorded from base `2bebc9da` as bytes. | `test_facade_4_ddl_round_trip.py` + committed `facade_4_type_goldens.json`; record mode `REPARK_FACADE_4_RECORD_GOLDENS=1` refused under `CI`/`GITHUB_ACTIONS`. | **OPEN** | Pending. |
| C-003 | Arrow answers are golden for the schema set (flat 7-type, 50-column wide mixed, `struct<array<map>>` depth 3, decimal (10,2)/(38,18)/(38,0), timestamp tz/ntz/session-tz, interval and char/varchar): `repark_type_to_arrow` Arrow spellings and `struct_type_from_arrow` class+simpleString answers; an `isinstance` pin asserts every conversion answer is a public `repark.spark.types` class. | Same test file + golden; isinstance pin. | **OPEN** | Pending. |
| C-004 | Mutation proof: a one-line change to a conversion answer turns the golden red; restore leaves it green. | Scratch edit, red output pasted, `git checkout` restore. | **OPEN** | Pending. |
| C-005 | Release baseline: warmup + 5 reps, medians, idle-box wait (no cargo/rustc/maturin) before each cell, `systemd-run --user --scope -p MemoryMax=8G` with `OPENBLAS_NUM_THREADS=8`. Cells: (a) `repark_type_to_arrow` per schema with per-call µs plus a spy count of calls per `createDataFrame`/`collect`/`to_arrow`/`show` of a 1e5 frame; (b) `struct_type_from_arrow` round-trip; (c) DDL parse (`fromDDL`/`_parse_datatype_string`) and DDL write (`simpleString`/DDL token) walls; (d) cProfile cumulative share of conversion in `df.schema`, `createDataFrame(pandas)`, `spark.read.csv(inferSchema)` at 1e5. Plain statement whether any cell is a wall (≥5 % of an end-to-end wall, or ≥1 ms per user call). | `docs/perf/facade-4-types-baseline-2026-09-14.md` + committed runner under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **OPEN** | Pending. |
| C-006 | Three-table agreement census measured by calling each table: for timestamps (tz, ntz, session-tz non-UTC), decimals (precision/scale edges, 38 overflow), nested nullability (struct field, array element `containsNull`, map `valueContainsNull`), date, binary, char/varchar, intervals — what (i) the facade `types.py` conversions, (ii) `_csv_smart` rungs, (iii) the reader lattice + Rust `spark_ddl_type_name`/`arrow_type_key` each answer. Every disagreement is a row with the concrete input and the three answers; none fixed. | Census section below; probe script under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **OPEN** | Pending. |
| C-007 | Step-1 target list: either (A) a measured wall and the Rust move that removes it, or (B) "no wall: ship as the correctness consolidation" naming the exact conversion entry points step 1 routes through one Rust table, plus the census disagreements needing an owner ruling (each a question with the three answers and a lean). | Step-1 section below. | **OPEN** | Pending. |
| C-008 | Gates green: the card's named pins unedited (`test_types_1.py`, `test_types_simple_string.py`, `test_types_x2_census.py`, `test_cast_failure_parity.py`, `test_a3_cast_vocab.py`), the FACADE-2 guard (`test_facade_2_column_display_goldens.py`, `test_facade_2_group2_no_python_assembly.py`), the new pins, `make verify`, `test_production_file_size.py`. | Commands and counts in Evidence. | **OPEN** | Pending. |

## Census — the three conversion tables (measured, step 0)

Pending — filled by C-006.

## Step-1 target list

Pending — filled by C-007.

## Evidence

Pending — filled as clauses close.
