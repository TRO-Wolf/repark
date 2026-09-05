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
| C-002 | The column-wise path answers exactly like the legacy path on a wide value matrix: schema and `collect()` equal by Arrow type AND value, on tuples, lists, namedtuples, dicts, Rows, scalars and ragged/empty frames. | `python/repark/tests/test_perf_facade_cdf_1.py`; the dispatcher swap. | **PROVEN** | 52 pins green, 1 skipped (the JVM-gated live leg): Arrow field types, Arrow values by repr, and `collect()` by `(type name, repr)` all equal on both dispatchers. |
| C-003 | Every refusal keeps its exact text: scalar merge kinds (`CANNOT_MERGE_TYPE`), decimal envelope, infinite floats, complex, `array.array` typecodes, duplicate names, ragged rows, Timedelta/Period/Interval. | The refusal pins; §6 mutations. | **PROVEN** | 22 refusal pins assert same exception type and byte-identical text on both dispatchers. Multi-failure precedence pairs documented in §7. |
| C-004 | Every input shape keeps its answer: name-list / `StructType` / DDL / bare-`DataType` schema, dict key-union, Row strict bind, scalar cells, empty input, all-null NaN/NaT witnesses, nested columns through the unchanged per-cell path. | The shape pins; the TY-4/TY-5 interchange pins stay green. | **PROVEN** | Shape pins green on both dispatchers; `test_interchange_parity.py` (TY-4/TY-5) green unchanged. Explicit schemas dispatch to the legacy path, verified by the StructType/DDL/bare-DataType pins. |
| C-005 | The delivery gate is measured against the ≤ 100 ms target at 1e5 tuples and reported met or missed with the isolated residue honestly. | §8; the baseline note. | OPEN |  |
| C-006 | The runner measures the createDataFrame old-vs-new pair in one process on one release module, plus nested-column and explicit-schema cells covering the delegated paths. | `bench/facade/`; `bench/facade/map.md`. | **PROVEN** | Seven cells in one run: tuples/nested/explicit old-vs-new pairs plus the pandas control (§8). The old leg swaps the dispatcher in a `finally`. |
| C-007 | The scalar matrix agrees with live PySpark 4.1.2 `createDataFrame` (schema and rows), and the disclosure leg still co-collects beside the new live leg. | The live leg; `test_parity_live.py`. | OPEN | JVM run once, beside at most one other JVM, then stopped. |
| C-008 | Docs and gates: the registry row filed FIXED with before/after, the baseline's CDF-1 section re-measured, every touched `map.md` in lockstep, every gate exit 0. | §10; the gates table. | OPEN |  |
| C-009 | Red-first (the pins fail under a deliberately wrong inference before the implementation) and a mutation score over the four brief-named faults. | §6. | **PROVEN** | Collection-error red, then 10 stub reds; 4 of 4 mutations red (§6 table). |

VERDICT: 9 clauses, 5 PROVEN, 4 OPEN, 0 REJECTED.

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

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

**Red first, two layers.** The pin file was committed before the implementation: collection
errored (`ModuleNotFoundError: create_dataframe_columns`, 0 collected). Then, with the
implementation wired but deliberately wrong (the int arm returning `pa.float64()`), **13
failed, 39 passed, 1 skipped**: 10 from the stub — every int-bearing equality pin plus the
int64-overflow refusal pin (which stopped refusing) — and 3 from test bugs of mine (below).
The 39 passes are the cases that never reach the fault. The stub was reverted uncommitted;
the record is this section.

The red run caught three test bugs of mine, fixed before green: `Row.__eq__` cannot compare
NaN (the `(type, repr)` signature subsumes it, so the bare `==` went away), `to_pylist()`
equality likewise (repr-per-row now), and a 38-significant-digit Decimal that trips
`decimal.InvalidOperation` in the shared envelope validator under the default 28-digit context
— pre-existing behavior on both paths, shrunk to 26 digits here and noted in §11 rather than
pinned.

**Mutation.** Four brief-named faults, each run uncommitted against the green pins and
reverted (tree verified clean after each):

| # | Mutation | Result |
|---|---|---|
| M1 | `{int, float}` census builds `float64` instead of refusing (the merge refusal dropped) | **RED** — `test_long_double_merge_refuses_with_same_text` only (1 failed, 51 passed) |
| M2 | decimal arm infers `decimal128(38, 9)` (wrong scale) | **RED** — `test_decimal_column_at_several_scales` only (Arrow type string and repr both move) |
| M3 | fast build drops `None` cells before `pa.array` (the None mask skipped) | **RED** — 9 pins: every equality case with Nones in fast-arm columns (scalar matrix, whole-None, bytes, date/datetime, decimal, int64 extremes, dict key-union, single row, 1e4 rows) |
| M4 | `{list}` census takes the scalar path (nested treated as scalar) | **RED** — 4 pins: the list-element merge refusal plus the three list-column equality pins |

**Mutation score: 4 of 4 red.**

One guard is deliberately *not* claimed as mutation-detected: the row-major-first choice
across two violating decimal columns has no pin (multi-failure order, §7). It is the one
branch whose behavior differs from legacy by design rather than by accident, and it is named
here rather than left unexamined.

## 7. Design — what the column-wise path shares and what it proves

Identity by construction: every shared rule is the same function object on both legs — the
cell normalizer, the all-null witness, the tuple converter for slow columns, the decimal
validator, the timestamp default, the nested-cell preparer, and the three tuples.py helpers.
The fast path skips work only where the census proves it a no-op: a `{int}` census cannot
carry a second merge kind (so the refusal walk is skipped); pure-`None` is all-null (so
normalization is skipped); `type()` — never `isinstance` — draws the line because `bool`
subclasses `int` and `datetime` subclasses `date`. Floats refuse infinity through Arrow
`is_inf` with the byte-identical text; datetimes keep per-cell preparation; decimals keep the
envelope validator with row-major-first reporting across fast decimal columns; times are
stringified exactly as the preparer does.

Error precedence differs only when two independent failures coexist (every single-failure
input raises the byte-identical error, pinned): (1) a slow column validates its envelope
inside its own build while fast decimal columns report row-major-first, so violations in a
slow column and elsewhere can report a different value than legacy row-major; (2) a slow
column's inference error raises in the build phase, after the envelope phase and the
duplicate-name check, where legacy raises all inference errors first.

The identity-permutation hoist in the tuple loop sits upstream of the dispatcher and benefits
both legs equally (~60 ms at 1e5); the old/new pair isolates the inference win, and the
absolute before/after includes the hoist — §8 reports all three numbers, not one.

## 8. Measurement (C-001, C-005, C-006)

Tracked runner, one run, release module `163,478,728 B`, load **10.75 → 10.51** (one sibling
`rustc` still live), 5 iterations after 1 warm-up (create cells cap at 3), bed
`~/repark-lanes/beds/oc-cdf1/run-create-pairs.json`:

| cell | old | new | × |
|---|---:|---:|---:|
| `create/100000/tuples_count` | 1,656.62 | **70.30** | **23.56×** |
| `create/100000/pandas_count` (control) | — | 3.00 | — |
| `create/10000/nested_count` | 261.70 | 273.21 | 0.96× |
| `create/100000/explicit_count` | 1,280.50 | 1,273.94 | 1.00× |

The pre-unit baseline on the unchanged lane (same runner, load 14.35 → 14.65) read
`tuples_count` **1,756.67 ms** and `pandas_count` 3.19 ms. Three honest numbers: the
absolute before/after is 1,756.67 → 70.30 across two runs on two loads; the same-process
pair is 1,656.62 → 70.30, which isolates the inference win (the old leg shares the
permutation hoist). Target ≤ 100 ms **met**, 30% under the bar.

The nested pair reads 0.96×: the slow arm does the same conversion plus one transpose and
census (~11 ms at 1e4) — the measured delegation cost, not a regression. The explicit pair
reads 1.00×: both legs run the identical legacy path by dispatch, as designed; the
explicit-schema path at ~1.27 s is now the slowest createDataFrame shape and stays out of
this unit (brief: preserve, not rewrite).

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

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-IMPL
  agent: Actor
  action: commit the column-wise implementation with green pins and maps
  charter_trace: C-002, C-003, C-004
  preconditions:
    - pins red under the wrong stub first: SATISFIED (13 failed, §6)
    - pins green on the real implementation: SATISFIED (52 passed, 1 JVM-skipped)
    - neighbor CDF suites green: SATISFIED (259 passed)
    - lint/format/conventions/docstring/size gates green: SATISFIED (this run)
    - no new code comment: SATISFIED (self-check below)
  success_condition: the commit's pins, gates and maps all hold on this tip
  step_risks: [shared-helper behavior drift: HANDLED(helper bodies are moved code; suite + pins)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-RUNNER
  agent: Actor
  action: commit the runner pairs with the measured numbers and maps
  charter_trace: C-006
  preconditions:
    - seven cells measured in one process: SATISFIED (§8 table)
    - lint/format/size/map gates green: SATISFIED (this run)
    - no new code comment: SATISFIED (self-check below)
  success_condition: the pairs reproduce on a re-run and the map states the contract
  step_risks: [old leg unfaithful: HANDLED(it calls the kept legacy path itself)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-HOIST
  agent: Actor
  action: commit the identity-permutation hoist with the mutation record
  charter_trace: C-009
  preconditions:
    - 4 of 4 mutations red with the tree clean after each: SATISFIED (§6 table)
    - pins + neighbors green on the hoist: SATISFIED (119 passed)
    - lint/format/map gates green: SATISFIED (this run)
    - no new code comment: SATISFIED (self-check below)
  success_condition: the hoist commit keeps every pin green and the maps true
  step_risks: [permute-arm behavior change: HANDLED(namedtuple-reorder pin + suite)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
