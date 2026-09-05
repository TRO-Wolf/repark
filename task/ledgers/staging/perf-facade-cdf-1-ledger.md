# Unit ledger — PERF-FACADE-CDF-1 · `createDataFrame(list of tuples)` goes column-wise

**Date:** 2026-09-05 · **Branch:** `perf/facade-cdf-1` · **Base:** `origin/main` `6eaccd5e` ·
**Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `PERF-FACADE-CDF-1` (filed FIXED at departure).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ANALYSIS-1 candidate 2: `createDataFrame(list of tuples)` normalizes every
cell in Python across five passes (`_normalize_create_dataframe_cell`, `_prepare_nested_cell`,
the long/double merge-refusal walk, the per-row schema check, the Arrow conversion) — 1,717 ms
at 1e5 where the same rows from pandas cost 3.2 ms. The tracked runner carries the cells as
controls (`create/100000/tuples_count` 1,699.00 ms in the PERF-FACADE-1 baseline); this unit turns
the controls into a before/after pair.

**Not in this unit:** any engine or binding change (pure-Python facade work); any public API
change — `createDataFrame(data, schema=None)` keeps its signature, and `verifySchema` /
`samplingRatio` are not in repark's signature at all (no such parameter exists in
`session_core.py`, so there is nothing to preserve beyond the freeze itself); the pandas/polars
native paths (already columnar through `pa.Table.from_pandas` / `.to_arrow()`); explicit-schema
(`StructType` / DDL) inputs stay on the legacy path by design — same code, not a second
implementation; the projection collapse (`PERF-FACADE-CHAIN-2`); every Iceberg candidate.

**Writable paths:**
`python/repark/src/repark/spark/session/create_dataframe_rows.py` (dispatch + the legacy path
kept callable),
NEW `python/repark/src/repark/spark/session/create_dataframe_columns.py` (column-wise
census/inference/conversion),
`python/repark/src/repark/spark/session/create_dataframe_tuples.py` (three tiny extracted
helpers shared by the old and new paths — duplicate-name refuse, all-null CAST parse, Arrow
build wrap),
`python/repark/src/repark/spark/session/_funcs.py` (router bindings),
`python/repark/src/repark/spark/session/map.md` (design note + pins),
`python/repark-parity/bench/facade/{cells,measure}.py` + `map.md` (create old-vs-new pairs,
nested and explicit-schema cells),
`python/repark/tests/test_perf_facade_cdf_1.py` (new, ≤ 1000 lines) + `python/repark/tests/map.md`,
`docs/perf/facade-boundary-baseline.md` (appended CDF-1 section, earlier tables untouched),
`docs/spark-sql-iceberg-parity.md` (§7 `PERF-FACADE-CDF-1` row),
this ledger and its `staging/map.md` row.
Closed: `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.lock`, every dependency,
`create_dataframe_values.py`, `create_dataframe_inference.py`, `create_dataframe_schema.py`,
`create_dataframe_arrow.py` (reused unchanged), every other ledger.

## PROPOSITION LEDGER — PERF-FACADE-CDF-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Baseline first from the tracked runner on a release module, unchanged lane: the `create` cells plus floor, spread and load. | `run_facade.py --cells create`; §8. | OPEN | Baseline run lands before any product commit. |
| C-002 | The column-wise path answers exactly like the legacy path on a wide value matrix: schema and `collect()` equal by Arrow type AND value, on tuples, lists, namedtuples, dicts, Rows, scalars and ragged/empty frames. | `python/repark/tests/test_perf_facade_cdf_1.py`; the dispatcher swap. | OPEN | Equality pins old-vs-new in one process via the swapped dispatcher. |
| C-003 | Every refusal keeps its exact text: scalar merge kinds (`CANNOT_MERGE_TYPE`), decimal envelope, infinite floats, complex, `array.array` typecodes, duplicate names, ragged rows, Timedelta/Period/Interval. | The refusal pins; §6 mutations. | OPEN | Single-failure inputs byte-identical; multi-failure order documented in §7. |
| C-004 | Every input shape keeps its answer: name-list / `StructType` / DDL / bare-`DataType` schema, dict key-union, Row strict bind, scalar cells, empty input, all-null NaN/NaT witnesses, nested columns through the unchanged per-cell path. | The shape pins; the TY-4/TY-5 interchange pins stay green. | OPEN | Explicit-schema inputs stay on the legacy path by dispatch, not by reimplementation. |
| C-005 | The delivery gate is measured against the ≤ 100 ms target at 1e5 tuples and reported met or missed with the isolated residue honestly. | §8; the baseline note. | OPEN |  |
| C-006 | The runner measures the createDataFrame old-vs-new pair in one process on one release module, plus nested-column and explicit-schema cells covering the delegated paths. | `bench/facade/`; `bench/facade/map.md`. | OPEN | Old leg swaps the dispatcher back to the legacy path in a `finally`, as PERF-FACADE-1 did for collect. |
| C-007 | The scalar matrix agrees with live PySpark 4.1.2 `createDataFrame` (schema and rows), and the disclosure leg still co-collects beside the new live leg. | The live leg; `test_parity_live.py`. | OPEN | JVM run once, beside at most one other JVM, then stopped. |
| C-008 | Docs and gates: the registry row filed FIXED with before/after, the baseline's CDF-1 section re-measured, every touched `map.md` in lockstep, every gate exit 0. | §10; the gates table. | OPEN |  |
| C-009 | Red-first (the pins fail under a deliberately wrong inference before the implementation) and a mutation score over the four brief-named faults. | §6. | OPEN |  |

VERDICT: 9 clauses, 0 PROVEN, 9 OPEN, 0 REJECTED.

## 1. Environment — what the lane actually had (2026-09-05)

The brief's environment premise did not hold and was re-established rather than assumed:

- The lane venv resolved `repark` to the sibling checkout
  (the sibling live checkout), whose native module is a **debug** build
  (`__debug_assertions__ True`, 638,162,472 B, built 2026-09-04 20:23) that **predates
  PERF-FACADE-1** — `rows_from_record_batch`, `logical_column_names` and
  `register_arrow_stream_as_temp_view` are all absent, so the lane facade at `6eaccd5e` cannot
  run on it at all. The sibling's `target/release/lib_native.so` is older still (2026-08-24).
- So the lane builds its own release module: `maturin build --release` (not `develop` — with
  the venv `.pth` files pointing at the sibling, `develop` would have installed the extension
  into the sibling checkout, outside this workspace), and the one `.so` from the wheel was
  copied into the lane package tree (`*.so` is gitignored — the tree stays clean). Native
  `163,478,728 B`, `__debug_assertions__ False`, both PERF-FACADE-1 symbols present, and a
  two-row `createDataFrame` smoke test passes.
- The venv's two plain-path `.pth` files (`repark.pth`,
  `_editable_impl_repark_parity.pth`) were repointed at the lane sources (venv-local, inside
  the workspace). `repark.__file__` and `repark_parity.__file__` both resolve under
  `$HOME/repark-lanes/lanes/oc-cdf1/` with no `PYTHONPATH` needed; the brief's gate
  commands pass `PYTHONPATH` anyway, which is harmless redundancy.
- The bench bed lives under `~/repark-lanes/beds/oc-cdf1/` (the Makefile default
  `/tmp/oc-facade-bed` is one power outage away from a rebuild; the runner regenerates it
  either way).

## SLR log (D3 — one per state-changing step)

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-ENV
  agent: Actor
  action: establish the lane runtime (release build, .so install, .pth repoint)
  charter_trace: C-001
  preconditions:
    - warm cargo caches present: SATISFIED (~/.cargo/git + registry from sibling builds)
    - no write outside the workspace: SATISFIED (maturin build writes dist/ in-lane; sibling untouched)
  success_condition: lane-local import proves lane files + release native + smoke createDataFrame
  step_risks: [stale-sibling confusion: HANDLED(.pth repoint + __file__ proof)]
  contingencies: [build failure: EXECUTABLE(additive — diagnose, rebuild)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
