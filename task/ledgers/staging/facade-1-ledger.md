# Unit ledger — FACADE-1 · the Arrow C Stream boundary

**Date:** 2026-09-12 · **Branch:** `feat/facade-1` · **Base:** `dae79c40`
**Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-1 (audit §8): `__arrow_c_stream__` capsules both ways become
the single protocol; `pa_ipc.new_stream` encode legs become the version-skew fallback;
pyarrow-absent installs fail loud at doors that need it, and polars/pandas consumers
of the export capsule run with pyarrow hidden.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`. Packaging still lists pyarrow as a hard
dependency (no dependency-file edits); runtime treats it as optional. Q2's extra name
is `repark[pyarrow]` in the `ImportError` text (D-3). No JVM.

## PROPOSITION LEDGER — FACADE-1 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The capsule seam is the single protocol: export doors (`to_arrow`, `toPandas`, `__arrow_c_stream__`, `collect` on the Arrow path) and import doors (`createDataFrame` from Arrow/pandas/polars, mapInArrow/mapInPandas/applyInPandas re-entry, ML prediction re-entry) cross through `__arrow_c_stream__` capsules; `pa_ipc.new_stream` stays only as the version-skew fallback (D-2). | `test_facade_1_arrow_c_stream.py` capsule-not-IPC pins; existing `test_create_dataframe_materialize.py` C-stream preference; `test_stream_ipc_ingest.py` IPC fallback. | **PROVEN** | Red-first on base: mapInArrow construction registered IPC (`ipc:"datafusion"."public"."__repark_mia_9d5ff93d22d64d51b29fec1790f8ae30":136`); ML re-entry registered IPC (`ipc:"datafusion"."public"."__repark_ml_ext_9cb0ac28a67947f2abb382747533236a":392`). Green after: both pins assert `cstream:` and zero `ipc:`; `test_create_dataframe_ipc_fallback_stays_when_capsule_hidden` still calls IPC when the native symbol is hidden. |
| C-002 | pyarrow is optional at runtime: package import and polars/pandas consumers of `__arrow_c_stream__` run with pyarrow hidden (D-3/D-4); a door that needs pyarrow raises `ImportError` naming `repark[pyarrow]`. | Hidden-pyarrow subprocess pins in `test_facade_1_arrow_c_stream.py`. | **PROVEN** | Red-first on base: `create_dataframe_columns.py:9 import pyarrow as pa` → `ImportError: pyarrow is hidden for FACADE-1` during `import repark`. Green after: `test_package_imports_with_pyarrow_hidden`, `test_polars_and_pandas_consume_arrow_c_stream_with_pyarrow_hidden` (polars `DataFrame(frame)` + pandas from those dicts + polars `createDataFrame` round-trip), `test_to_arrow_names_pyarrow_extra_when_hidden` (`repark[pyarrow]` in the message). |
| C-003 | The audit's pin list stays green: `test_dfcore_4b_exports.py`, `test_mapinarrow.py` + `test_mapinarrow_oracle.py`, `test_applyinpandas.py`, `test_perf_facade_collect_rows.py`. | Those files. | **PROVEN** | `.venv/bin/python -m pytest` on the five files: 95 passed, 1 skipped, 4.24s. |
| C-004 | Import-side measurement (D-5): one frame through `createDataFrame` via IPC fallback versus the arrow-stream seam, plus the `mapInPandas` encode leg, three repetitions, medians, no regression. | Timed in this clone on 2026-09-12; numbers in Evidence. | **PROVEN** | createDataFrame 10k tuples, 3 reps: capsule median 11.846 ms (12.507, 11.693, 11.846); IPC fallback median 12.424 ms (12.694, 12.386, 12.424). mapInPandas 5k identity, 3 reps: capsule median 14.426 ms (13.589, 15.582, 14.426); IPC encode median 12.193 ms (12.497, 12.133, 12.193). Capsule createDataFrame is not slower than IPC. mapInPandas encode-leg difference is noise at 5k (both ~12–15 ms). |
| C-005 | Freeze (D-1): `to_arrow` / `toPandas` / `collect` / `createDataFrame` keep signatures and answers; `test_v1_gate_docs.py` and the API-freeze register stay unchanged and green. | `test_facade_1_arrow_c_stream.py::test_to_arrow_to_pandas_collect_signatures_unchanged`; `test_v1_gate_docs.py`; `test_api_freeze.py`. | **PROVEN** | Export-door pin green (values + `DataFrame.toPandas is DataFrame.to_pandas`). Freeze + CAP-1: 59 passed. Those two freeze files were not edited. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Evidence

**Red first (base `dae79c40`, production unchanged).**
`test_package_imports_with_pyarrow_hidden` / consume / `to_arrow` extra: import failed at
`create_dataframe_columns.py:9 import pyarrow as pa`.
`test_mapinarrow_placeholder_registers_capsule_not_ipc`: events `['ipc:...__repark_mia_...:136']`.
`test_ml_reenter_registers_capsule_not_ipc`: events `['ipc:...__repark_ml_ext_...:392']`.

**Change.** `require_pyarrow()` (`spark/_pyarrow.py`) names `repark[pyarrow]`.
`register_arrow_exporter_as_temp_view` (`spark/_arrow_stream.py`) is the shared capsule-first
register with IPC fallback. `create_dataframe_columns.py` loads pyarrow inside the functions
that need it. `create_dataframe_rows.py` materializes any C Stream exporter; polars
`createDataFrame` uses that path when pyarrow is hidden. mapInArrow construction and ML
prediction re-entry register capsules. `to_polars` uses `pl.DataFrame(self)` when `to_arrow`
raises `ImportError`. `core.py` 4485 → 4473 (ratchet down). No Rust edit; no dependency-file
edit.

**Gates.** Named D-4 pins 95 passed + 1 skipped. New pin file 9 passed. Freeze/CAP-1 59 passed.
Facade suite `python/repark/tests` 5948 passed + 369 skipped (production-file-size 11 passed
after re-hash; remainder 5937 passed). Parity suite 745 passed + 1 skipped + 11 xfailed;
2 spill CI-tier cells `KILLED` instead of `spilled` (address-space killer, not this diff).
`make check-lib-py` 653 files clean. `make check-docs-links` 784 files / 5043 links.
`make check-ledger-grammar` 111 live ledgers. `make verify` exit 0.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: facade-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Capsule-not-IPC pins red on the base tree (mapInArrow placeholder and ML re-entry used register_ipc_stream_as_temp_view) and green after the helper. Version-skew IPC fallback pin still fires when the native capsule symbol is hidden.
      artifacts: [python/repark/tests/test_facade_1_arrow_c_stream.py, python/repark/src/repark/spark/_arrow_stream.py]
    - id: AT-2
      status: ATTACKED
      evidence: Hidden-pyarrow subprocess pins package import, polars DataFrame(frame), pandas from those rows, and polars createDataFrame. With pyarrow present, pandas.DataFrame.from_arrow and polars.DataFrame consume the export capsule. Audit pin list 95 passed + 1 skipped.
      artifacts: [python/repark/tests/test_facade_1_arrow_c_stream.py, python/repark/tests/test_mapinarrow.py, python/repark/tests/test_applyinpandas.py]
    - id: AT-3
      status: ATTACKED
      evidence: to_arrow with pyarrow hidden raises ImportError whose text contains repark[pyarrow]. Doors that need pyarrow do not fall back to Python rows.
      artifacts: [python/repark/src/repark/spark/_pyarrow.py, python/repark/tests/test_facade_1_arrow_c_stream.py]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state, lock, or async spawn. Capsule drain already serializes under the GIL in register_arrow_stream_as_temp_view.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, or path handling. The new modules only import pyarrow or register an Arrow stream the engine already accepts.
    - id: AT-6
      status: ATTACKED
      evidence: Public names to_arrow, toPandas, collect, createDataFrame, mapInArrow, __arrow_c_stream__ are unchanged. Freeze inventory files were not edited and passed.
      artifacts: [python/repark/tests/test_facade_1_arrow_c_stream.py, python/repark-parity/tests/test_api_freeze.py, python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-7
      status: ATTACKED
      evidence: Import-side measurement, three reps, medians. createDataFrame 10k tuples capsule 11.846 ms vs IPC 12.424 ms. mapInPandas 5k identity capsule 14.426 ms vs IPC encode 12.193 ms. No capsule regression on createDataFrame.
      artifacts: [task/ledgers/staging/facade-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: core.py exact baseline ratcheted 4485 to 4473 in check_lib_py.py and the CAP-1 mirror. New helpers sit under the default Python ceiling. No comment bytes added in the code diff.
      artifacts: [scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change. ImportError text is the new missing-pyarrow signal.
    - id: AT-10
      status: ATTACKED
      evidence: New pins failed on the base tree (import, mapInArrow IPC placeholder, ML IPC re-entry) and pass on the tip (9 passed). Existing D-4 batteries stay green.
      artifacts: [python/repark/tests/test_facade_1_arrow_c_stream.py]
  complete: true
```
