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
| C-001 | Baseline first from the tracked runner on a release module, unchanged lane: the `create` cells plus floor, spread and load. | `run_facade.py --cells create`; §8. | **PROVEN** | 1,756.67 ms pre-unit (load 14.35 → 14.65) before any product commit; floor 2.45 ms re-measured on the full run. |
| C-002 | The column-wise path answers exactly like the legacy path on a wide value matrix: schema and `collect()` equal by Arrow type AND value, on tuples, lists, namedtuples, dicts, Rows, scalars and ragged/empty frames. | `python/repark/tests/test_perf_facade_cdf_1.py`; the dispatcher swap. | **PROVEN** | 52 pins green, 1 skipped (the JVM-gated live leg): Arrow field types, Arrow values by repr, and `collect()` by `(type name, repr)` all equal on both dispatchers. |
| C-003 | Every refusal keeps its exact text: scalar merge kinds (`CANNOT_MERGE_TYPE`), decimal envelope, infinite floats, complex, `array.array` typecodes, duplicate names, ragged rows, Timedelta/Period/Interval. | The refusal pins; §6 mutations. | **PROVEN** | 24 refusal pins assert same exception type and byte-identical text on both dispatchers. Multi-failure precedence pairs documented in §7. |
| C-004 | Every input shape keeps its answer: name-list / `StructType` / DDL / bare-`DataType` schema, dict key-union, Row strict bind, scalar cells, empty input, all-null NaN/NaT witnesses, nested columns through the unchanged per-cell path. | The shape pins; the TY-4/TY-5 interchange pins stay green. | **PROVEN** | Shape pins green on both dispatchers; `test_interchange_parity.py` (TY-4/TY-5) green unchanged. Explicit schemas dispatch to the legacy path, verified by the StructType/DDL/bare-DataType pins. |
| C-005 | The delivery gate is measured against the ≤ 100 ms target at 1e5 tuples and reported met or missed with the isolated residue honestly. | §8; the baseline note. | **PROVEN** | 70.30 ms same-process (66.65 ms second run) — met 30% under the bar; residue is the transpose + census + Arrow conversion (§8). |
| C-006 | The runner measures the createDataFrame old-vs-new pair in one process on one release module, plus nested-column and explicit-schema cells covering the delegated paths. | `bench/facade/`; `bench/facade/map.md`. | **PROVEN** | Seven cells in one run: tuples/nested/explicit old-vs-new pairs plus the pandas control (§8). The old leg swaps the dispatcher in a `finally`. |
| C-007 | The scalar matrix agrees with live PySpark 4.1.2 `createDataFrame` (schema and rows), and the disclosure leg still co-collects beside the new live leg. | The live leg; `test_parity_live.py`. | **PROVEN** | 172 passed in one live run (the scalar leg plus the disclosure leg co-collected); JVM slot empty before and after. |
| C-008 | Docs and gates: the registry row filed FIXED with before/after, the baseline's CDF-1 section re-measured, every touched `map.md` in lockstep, every gate exit 0. | §10; the gates table. | **PROVEN** | Registry FIXED, baseline §4, five maps in lockstep, every brief gate exit 0 (§10). |
| C-009 | Red-first (the pins fail under a deliberately wrong inference before the implementation) and a mutation score over the four brief-named faults. | §6. | **PROVEN** | Collection-error red, then 10 stub reds; 4 of 4 mutations red (§6 table). |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

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

Second run, full battery the same day (`run-full-cdf1.json`, load ~19–25): the tuples pair
reproduced at 1,620.75 → 66.65 ms, the floor re-measured at **2.45 ms** (five medians 73.69,
75.62, 73.17, 75.06, 73.29), and the §1/§2 cells confirmed unmoved (`collect/1000000` 940.84
against 939.85, `chain/100/build_only` 344.01 against 366.11 — load noise, no regression from
this unit, which touches neither path). 70.30 ms is 28.7× the floor; pandas at 3.00 ms is the
remaining structural gap (no transpose to pay).

## 10. Gates (C-008)

Every brief gate, observed on the final tree:

| Gate | Result |
|---|---|
| `make ci` | exit 0 |
| `make check-python-conventions` | exit 0 (239 files clean) |
| `pytest python/repark/tests --timeout 900 -x` | exit 0 — **4,853 passed**, 199 skipped |
| `pytest python/repark-parity/tests` | exit 0 — 574 passed |
| live `test_parity_live.py` + `test_perf_facade_cdf_1.py` | exit 0 — 172 passed, JVM slot empty after |
| `check-map-sync`, `check-ledger-grammar`, `check-ledgers`, `check-docs-compaction` | exit 0 |
| `ledger_lifecycle.py check --base origin/main` | exit 0 |
| `typos .` | exit 0 |

Two mid-unit reds, both closed with evidence rather than absorbed: (1) the split-contract
baseline red on the intended refactor — joined completely (new module listed, six bindings
hashed/owned/exported, 76 edges reviewed triple by triple); (2)
`test_sort_merge_join_spills_under_small_fair_pool` failed once under a self-inflicted
concurrent load (two suites plus cargo on a load-29 box) — the file has zero `createDataFrame`
calls, the test passes alone in 6.3 s, and the final full run is green.

## 11. Out of scope, observed (not claimed, not fixed)

- The envelope validator leaks raw `decimal.InvalidOperation` past 28 significant digits
  (default-context `quantize`) instead of a PySpark error — pre-existing, identical on both
  paths; the pins use 26 digits.
- The explicit-schema path (~1.27 s at 1e5) is now the slowest `createDataFrame` shape. It
  stays on the legacy path by design (brief: preserve); a census for declared types would be a
  separate unit with its own pins.
- The nested pair reads 0.96× — the measured transpose-plus-census delegation cost, accepted.
- The brief's environment premise (sibling release native) did not hold; §1 records what was
  established instead. The venv-local provisions (`pyspark==4.1.2`, `pytest-timeout==2.4.0`,
  lane `.pth` repoint, lane `.ivy2`) touch no lockfile or dependency.

```yaml
COVERAGE_ATTESTATION:
  unit: PERF-FACADE-CDF-1
  verdict: complete
  complete: true
  note: Actor self-attestation — no separate Critic ran in this lane; the equality pins,
    the four mutations and the full suites are the review evidence.
  categories:
    AT-1_api-facade-shape: ATTACKED
    AT-1-artifacts:
      - python/repark/tests/test_perf_facade_cdf_1.py::test_shipped_dispatcher_is_the_column_wise_path
      - python/repark/tests/test_production_file_size.py (76 cross-owner edges pin the router binding)
    AT-2_data-correctness: ATTACKED
    AT-2-artifacts:
      - python/repark/tests/test_perf_facade_cdf_1.py (27 equality pins: Arrow types, Arrow values, collect signatures)
      - python/repark/tests/test_perf_facade_cdf_1.py::test_ten_thousand_rows
    AT-3_divergence-proof: ATTACKED
    AT-3-artifacts:
      - python/repark/tests/test_perf_facade_cdf_1.py (24 refusal pins with byte-identical text)
      - ledger §6 mutations M1/M2/M4 (dropped refusal, wrong scale, nested-as-scalar all red)
    AT-4_performance-method: ATTACKED
    AT-4-artifacts:
      - python/repark-parity/bench/facade/ (seven cells, both legs one process, §8 table)
      - ledger §8 (three honest numbers: absolute, pair, load labels)
    AT-5_determinism-order: ATTACKED
    AT-5-artifacts:
      - python/repark/tests/test_perf_facade_cdf_1.py::test_namedtuple_rows_reorder_by_name
      - ledger §7 (multi-failure precedence pairs named, single-failure byte-identical)
    AT-6_errors: ATTACKED
    AT-6-artifacts:
      - python/repark/tests/test_perf_facade_cdf_1.py (refusal-type equality in every refusal pin)
      - python/repark/tests/test_f1_errorclass.py (suite green)
    AT-7_null-semantics: ATTACKED
    AT-7-artifacts:
      - python/repark/tests/test_perf_facade_cdf_1.py (None in every column, whole-None, NaN/NaT witnesses)
      - ledger §6 mutation M3 (dropped None mask reds 9 pins)
    AT-8_lifecycle-cleanup: ATTACKED
    AT-8-artifacts:
      - bench old leg and every pin swap the dispatcher in a finally/monkeypatch
      - live run: JVM slot verified empty before and after (C-007)
    AT-9_concurrency-parallelism: N/A
    AT-9-justification: single-threaded facade path; no thread, lock, or async primitive added
    AT-10_coverage-honesty: ATTACKED
    AT-10-artifacts:
      - ledger §6 (red-first record: collection error, then 10 stub reds + 3 test bugs)
      - ledger §6 (the unclaimed multi-violation guard named, not pinned)
```

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

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-CONF
  agent: Actor
  action: commit the conf-pin session-reuse fix with effect assertions
  charter_trace: C-002, C-004
  preconditions:
    - reuse proven by the live-run warning: SATISFIED (unapplied timestampType keys)
    - halves assert their own conf effect: SATISFIED (struct/map, first/merged, UTC/naive)
    - pins green and warning-free: SATISFIED (52 passed, -W error::UserWarning)
    - lint/format gates green: SATISFIED (this run)
    - no new code comment: SATISFIED (self-check below)
  success_condition: each conf half runs under its own session and proves it
  step_risks: [none new: HANDLED(test-only change; product untouched)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-DOCS
  agent: Actor
  action: commit the registry row, baseline section and map trues with numbers
  charter_trace: C-001, C-005
  preconditions:
    - pairs + floor measured on labeled runs: SATISFIED (§8, two runs)
    - earlier baseline tables untouched: SATISFIED (§4 appended; §3 bytes kept)
    - doc gates green: SATISFIED (this run)
  success_condition: the registry, baseline and maps state the same numbers
  step_risks: [stale-claim drift: HANDLED(the one stale bullet amended in place, dated)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-SPLITBASE
  agent: Actor
  action: commit the split-contract inventory join for the new module
  charter_trace: C-008
  preconditions:
    - the contract failure is the intended refactor: SATISFIED (2 moved bodies, 1 net edge)
    - inventory complete, not just quiet: SATISFIED (6 names in hashes+owners+runtime, file listed)
    - binding delta reviewed triple by triple: SATISFIED (76 = 74 - 1 + 1 + 2, §10 note)
    - baseline file green standalone: SATISFIED (11 passed)
    - lint/format/size gates green: SATISFIED (this run)
    - no new code comment: SATISFIED (self-check below)
  success_condition: the split contract pins the new surface; the full suite confirms
  step_risks: [hash computed from the wrong tree: HANDLED(hashes match the failure diff)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CDF1-CLOSE
  agent: Actor
  action: close the unit ledger with gates, attestation and all clauses PROVEN
  charter_trace: C-007, C-008
  preconditions:
    - every brief gate exit 0 on the final tree: SATISFIED (§10 table)
    - pin counts stated exactly: SATISFIED (27 equality, 24 refusal, counted)
    - no background pytest/JVM left: SATISFIED (slot verified empty)
    - attestation grammar valid: SATISFIED (this run)
  success_condition: the ledger departs complete with zero OPEN clauses
  step_risks: [none: HANDLED(ledger-only change; code untouched)]
  contingencies: [suite red: EXECUTABLE(additive — fix forward, no amends)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
