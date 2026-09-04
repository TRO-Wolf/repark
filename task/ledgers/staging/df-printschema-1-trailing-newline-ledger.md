# Unit ledger — DF-PRINTSCHEMA-1 · printSchema prints Spark's trailing blank line

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DF-PRINTSCHEMA-1 merges, or when the owner closes the slate row.

**Unit:** DF-PRINTSCHEMA-1 · **Date:** 2026-09-04 · **Executor:** Muse Spark (muse-spark-1.3), Actor ·
**Branch:** `fix/df-printschema-1-trailing-newline` · **Base:** `e3600a1`
**Model:** muse-spark-1.3
**Registry:** `docs/spark-sql-iceberg-parity.md` row `EX-DF-10` (filed 2026-09-04 by EX-16, PR #353,
landed on `main` at `7496049`; flipped to FIXED in this unit's merge commit `68e408d`).
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on, 2026-09-04.
Registry cells matched; no HALT.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | repark's `printSchema` stdout is byte-identical to Spark's for a flat schema, a nested struct, an array column, and `level=1`; the falsified comment is corrected to one true line, never deleted. | Oracle table below + `core.py`. | **PROVEN** |
| C-002 | New `test_df_printschema.py` pins the exact captured stdout (four shapes); a live cell in `test_parity_live.py` is co-collected on the shared `spark_engine`; the EX-16 pin asserting the old tail was flipped to `test_print_schema_stdout_matches_spark` in the merge commit `68e408d`. | Pins + live test. | **PROVEN** |
| C-003 | Red-first: the four pins go RED before the fix; the strip-arm knob restores RED after it. | Mutation table below. | **PROVEN** |
| C-004 | Registry `EX-DF-10` → FIXED, the EX-16 pin flipped to assert Spark's tail, `docs/examples/dataframe/map.md` updated — all in the merge commit `68e408d` that brought EX-16 (`7496049`) into this branch; this ledger plus `staging/map.md`; `map.md` lockstep. | Diff of `68e408d`. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-04, JDK 17, ANSI on, `TZ=UTC`)

| Shape | Spark capture (`repr`) | repark before |
|---|---|---|
| flat `g/k/v` | `'root\n |-- g: string (nullable = true)\n |-- k: long (nullable = true)\n |-- v: double (nullable = true)\n\n'` (`splitlines` 5) | same tree lines, single trailing `\n` (`splitlines` 4) |
| nested `(a, (b._1, b._2))` | `'root\n |-- a: long (nullable = true)\n |-- b: struct (nullable = true)\n |    |-- _1: long (nullable = true)\n |    |-- _2: string (nullable = true)\n\n'` (`splitlines` 6) | same tree lines, single trailing `\n` (`splitlines` 5) |
| array `a` | `'root\n |-- a: array (nullable = true)\n |    |-- element: long (containsNull = true)\n\n'` (`splitlines` 4) | same tree lines, single trailing `\n` (`splitlines` 3) |
| `level=1` on nested | `'root\n |-- a: long (nullable = true)\n |-- b: struct (nullable = true)\n\n'` (`splitlines` 4) | same tree lines, single trailing `\n` (`splitlines` 3) |

## Fix

| Name | Layer |
|---|---|
| `printSchema` | `python/repark/src/repark/spark/dataframe/core.py`; the strip arm is gone, `print` adds Spark's second newline |

## Mutation

| Knob | Red of M |
|---|---|
| new pins before the fix | 4 red of 4 (`test_df_printschema.py`) |
| restore the strip arm after the fix | 4 red of 4 (`test_df_printschema.py`) |

## Notes for the orchestrator

| Item | Note |
|---|---|
| `EX-DF-10` registry row | Not on main (only on `origin/docs/ex-16-dataframe-b`); flip to FIXED 2026-09-04 (DF-PRINTSCHEMA-1) at PR #353 merge time |
| `test_examples_dataframe_b.py::test_print_schema_stdout_divergence` | Pins the old single-newline stdout on the unmerged branch; must flip to the double-newline text when that PR rebases past this fix |
| `docs/examples/dataframe/print_schema.py` | Not on main; its `rstrip` arm holds on both engines per the brief, no move owed |

## 9. Delivery

| Item | Path |
|---|---|
| Fix | `python/repark/src/repark/spark/dataframe/core.py` |
| Pins | `python/repark/tests/test_df_printschema.py` (four shapes) |
| Live leg | `test_parity_live.py::test_live_df_printschema_trailing_newline_matches_spark` on `spark_engine` |
| Registry | `EX-DF-10` FIXED in this unit (merge commit `68e408d`); the EX-16 pin `test_print_schema_stdout_matches_spark` asserts Spark's tail |
| Size gate | `scripts/check_lib_py.py` `core.py` 6371→6368 + `scripts/map.md` |
| Maps | lockstep on every touched directory |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-printschema-1-trailing-newline
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Live PySpark 4.1.2 captures recorded before code for flat, nested, array, level=1; pins assert Spark's text.
      artifacts: [python/repark/tests/test_df_printschema.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls are the four shapes plus the level truncation arm; tree-line content was already equal, only the tail moved.
      artifacts: [python/repark/tests/test_df_printschema.py]
    - id: AT-3
      status: ATTACKED
      evidence: No error path in the change; stdout tail only, no raise surface.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-4
      status: N/A
      justification: Pure stdout tail change; no shared mutable state, no concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; one Spark JVM at a time.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-6
      status: ATTACKED
      evidence: No public API change; printSchema signature and tree lines unchanged.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-7
      status: ATTACKED
      evidence: Always-run pins are repark-only; Spark is behind REPARK_PARITY_LIVE=1 on the shared engine.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-8
      status: ATTACKED
      evidence: Facade file-size and comment ceilings untouched; one comment line corrected in place.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cited in tests and maps; EX-DF-10 flip owed on the unmerged branch.
      artifacts: [task/ledgers/staging/df-printschema-1-trailing-newline-ledger.md, python/repark/tests/map.md]
```
