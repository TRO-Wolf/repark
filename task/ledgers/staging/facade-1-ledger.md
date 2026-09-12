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
| C-002 | pyarrow is optional at runtime: package import and polars/pandas consumers of `__arrow_c_stream__` run with pyarrow hidden (D-3/D-4); a door that needs pyarrow raises `ImportError` naming `repark[pyarrow]`. | Hidden-pyarrow subprocess pins in `test_facade_1_arrow_c_stream.py`. | **PROVEN** | Red-first on base: `create_dataframe_columns.py:9 import pyarrow as pa` → `ImportError: pyarrow is hidden for FACADE-1` during `import repark`. Green after: `test_package_imports_with_pyarrow_hidden`, `test_polars_and_pandas_consume_arrow_c_stream_with_pyarrow_hidden` (polars `DataFrame(frame)` + pandas from those dicts + polars `createDataFrame` round-trip), `test_to_arrow_names_pyarrow_extra_when_hidden` (`repark[pyarrow]` in the message). Round 3: wheel-smoke CI run 34710119289 failed `test_package_imports_with_pyarrow_hidden` with `ModuleNotFoundError: repark._native` because `_hidden_pyarrow_env` put `python/repark/src` on the child's `PYTHONPATH` and `cwd` was the repo root, so the child imported the source package. Fix: child uses the parent `sys.executable` and env unchanged, `cwd` a scratch dir, no src injection; pin asserts `repark.__file__` matches the parent install and is not under `python/repark/src` when the parent is not. Hand proof from `/tmp/facade1-wheelproof-*` with empty PYTHONPATH: child loaded `repark.pth` → `_native.abi3.so`, `sys.path[0]` empty. |
| C-003 | The audit's pin list stays green: `test_dfcore_4b_exports.py`, `test_mapinarrow.py` + `test_mapinarrow_oracle.py`, `test_applyinpandas.py`, `test_perf_facade_collect_rows.py`. | Those files. | **PROVEN** | `.venv/bin/python -m pytest` on the five files: 95 passed, 1 skipped, 4.24s. |
| C-004 | Import-side measurement (D-5): one frame through `createDataFrame` via IPC fallback versus the arrow-stream seam, plus the `mapInPandas` encode leg, three repetitions, medians. | Timed in this clone 2026-09-12; review `/tmp/oc-worker/grok-rev-facade1/report.md` (S2-21). | **PROVEN** | Actor 5k mapInPandas identity, 3 reps: capsule 14.426 ms (13.589, 15.582, 14.426) vs IPC encode 12.193 ms (12.497, 12.133, 12.193) — a 2 ms wobble on a ~13 ms door, not a door moving. createDataFrame 10k tuples: capsule 11.846 ms vs IPC 12.424 ms. Reviewer, stage-isolated encode on the same machine class: capsule vs IPC 4.913 vs 5.145 ms at 5k (capsule 4.6 % faster) and 10.556 vs 13.728 ms at 500k (capsule 23 % faster); `Table.from_batches` wrap 0.002 / 0.004 ms. Interleaved 7-rep full 5k round-trip (review pass 3): capsule 13.78 vs forced-IPC 14.03 ms. Capsule is the faster encode leg; the 18 % figure does not hold. |
| C-005 | Freeze (D-1): `to_arrow` / `toPandas` / `collect` / `createDataFrame` keep signatures and answers; `test_v1_gate_docs.py` and the API-freeze register stay unchanged and green. | `test_facade_1_arrow_c_stream.py::test_to_arrow_to_pandas_collect_signatures_unchanged`; `test_v1_gate_docs.py`; `test_api_freeze.py`. | **PROVEN** | Export-door pin green (values + `DataFrame.toPandas is DataFrame.to_pandas`). Freeze + CAP-1: 59 passed. Those two freeze files were not edited. |
| C-006 | `to_polars` uses one capsule import (`pl.DataFrame(self)`); it does not call `to_arrow`. Duplicate display names stay suffixed (`aa, b, a, b__1`). | `test_to_polars_does_not_call_to_arrow` (red-first: `assert 1 == 0`); `test_h1_to_polars_uses_display_names`. | **PROVEN** | Spy pin red on d0045175 (`calls["n"] == 1`), green after (`n == 0` on a 1M `range` frame). Duplicate-name pin green. 1M `to_polars` median 24.479 ms (24.955, 24.462, 24.479) vs reviewer's 38.14 ms through `to_arrow` and 24.67 / 25.16 ms hidden / `pl.DataFrame(frame)`. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Evidence

**Red first (base `dae79c40`, production unchanged).**
`test_package_imports_with_pyarrow_hidden` / consume / `to_arrow` extra: import failed at
`create_dataframe_columns.py:9 import pyarrow as pa`.
`test_mapinarrow_placeholder_registers_capsule_not_ipc`: events `['ipc:...__repark_mia_...:136']`.
`test_ml_reenter_registers_capsule_not_ipc`: events `['ipc:...__repark_ml_ext_...:392']`.

**O-1 (remediation).** Spill CI cells `KILLED` with `OpenBLAS error: Memory allocation still failed after 10 retries` because pyarrow became lazy and the first `to_arrow` inside the cap imported numpy/OpenBLAS. Measured both outs (2026-09-12): `OPENBLAS_NUM_THREADS=1` with no pre-import, CI golden 2 passed in 44.51 s; `load_spill_worker_libraries()` before the cap, CI golden 2 passed in 33.04 s (`sort`/`hash_aggregate` SPILLED, `hash_join` refused). Kept pre-import: the cap follows the library loads, matching the harness map's VmSize facts.

**Change.** `require_pyarrow()` (`spark/_pyarrow.py`) names `repark[pyarrow]`.
`register_arrow_exporter_as_temp_view` (`spark/_arrow_stream.py`) is the shared capsule-first
register with IPC fallback. `create_dataframe_columns.py` loads pyarrow inside the functions
that need it. `create_dataframe_rows.py` materializes any C Stream exporter; polars
`createDataFrame` uses that path when pyarrow is hidden. mapInArrow construction and ML
prediction re-entry register capsules. `to_polars` always uses `pl.DataFrame(self)` and suffixes duplicate display names on
the polars columns. Spill worker pre-imports numpy/pyarrow before `RLIMIT_AS`.
`core.py` 4485 → 4473 → 4470 (ratchet down). No Rust edit; no dependency-file edit.

**Gates.** Named D-4 pins 95 passed + 1 skipped. New pin file 9 passed. Freeze/CAP-1 59 passed.
Remediation: spill CI golden 2 passed (pre-import). Parity suite 747 passed + 1 skipped + 11 xfailed.
Facade suite 5949 passed + 369 skipped. `make check-lib-py` 653 files clean.
`make check-docs-links` 784 files / 5043 links. `make check-ledger-grammar` 111 live ledgers
(711 clauses). `make verify` exit 0.

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
      evidence: Actor 5k mapInPandas identity capsule 14.426 vs IPC 12.193 ms is a 2 ms wobble (samples 13.59/15.58/14.43). Reviewer stage-isolated encode (report /tmp/oc-worker/grok-rev-facade1/report.md): capsule 4.913 vs IPC 5.145 ms at 5k and 10.556 vs 13.728 ms at 500k; Table.from_batches 0.004 ms at 500k. Interleaved 5k round-trip 13.78 vs 14.03 ms. Capsule is faster.
      artifacts: [task/ledgers/staging/facade-1-ledger.md, python/repark/tests/test_facade_1_arrow_c_stream.py]
    - id: AT-8
      status: ATTACKED
      evidence: core.py exact baseline ratcheted 4485 to 4473 to 4470 in check_lib_py.py and the CAP-1 mirror. New helpers sit under the default Python ceiling. No comment bytes added in the code diff.
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
