# Unit ledger — PERF-FACADE-1 · `collect()` rows in the binding, `withColumn` chains made linear

**Date:** 2026-09-04 · **Branch:** `perf/facade-1` · **Base:** `origin/main` `ef256d67` ·
**Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `PERF-FACADE-COLLECT-1` **FIXED**, `PERF-FACADE-WITHCOLUMN-1` **FIXED**,
`PERF-FACADE-CHAIN-2` filed BACKLOG.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ANALYSIS-1 ranked eleven measured candidates and queued these two first as
"the biggest absolute user-visible walls and pure facade/binding work" — candidate 1
(`collect()` 4,963 ms at 1e6 × 7 where `to_arrow()` is 24 ms, and Spark is 1.4× faster) and
candidate 3 (a depth-100 `withColumn` chain costing 2,376 ms to *build*, 3.2× slower than
Spark).

**Not in this unit:** `createDataFrame` (candidate 2, `FACADE-CDF-1`); every Iceberg candidate;
the `avg` groups accumulator; the window-frame charter; any public API change; the projection
collapse (measured and filed as `PERF-FACADE-CHAIN-2`, §7).

**Writable paths:** `crates/repark-python/src/{collect_rows.rs,logical_names.rs,lib.rs}`,
`crates/repark-python/map.md`, `python/repark/src/repark/spark/dataframe/{core.py,rows_export.py,map.md}`,
`python/repark/tests/{test_perf_facade_collect_rows.py,test_perf_facade_logical_names.py,map.md}`,
the `core.py` row of `scripts/check_lib_py.py` with its `scripts/map.md` entry,
`docs/perf/facade-boundary-baseline.md` + `docs/perf/map.md`,
`docs/spark-sql-iceberg-parity.md` §7, this ledger and its `staging/map.md` row.
Closed: `STATUS.md` (pinned `_Last updated`), `briefs/next-sequence.md`, `.github/`,
`Cargo.lock`, every dependency, every other ledger.

## PROPOSITION LEDGER — PERF-FACADE-1 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Baseline first: the analysis' own facade battery runs at `origin/main` `ef256d67` on a RELEASE module in this lane, and reproduces PERF-ANALYSIS-1 before any code changes; every cell records its 1-minute load. | The release proof; `scratch/f1/numbers-before.json`; §8. | **PROVEN** | Module `163,145,824 B`, `__debug_assertions__ False`, path under `/tmp/oc-perfa`. Load 7.85 → 6.95. Reproduces the analysis within 2 % on every headline cell: collect 1e6 **4,908.03** (analysis 4,963), collect 1e5 562.35 (531), chain build 10/50/100 **8.14 / 328.48 / 2,385.23** (8.1 / 327 / 2,389), `to_arrow` 25.57 (24.1), `createDataFrame` tuples 1,749.96 (1,720). |
| C-002 | Row materialization moves into `repark-python`, and the binding converts ONLY cell kinds whose `to_pylist` mapping is unambiguous; every other kind is converted by the unchanged Python path or declines the whole batch. A wide type matrix proves the two converters return objects equal by value AND by `repr`. | `crates/repark-python/src/collect_rows.rs`; `python/repark/tests/test_perf_facade_collect_rows.py`; the mutation runs. | **PROVEN** | Native set: null, boolean, int8..int64, uint8..uint64, `f32`, `f64`, `Utf8`/`LargeUtf8`/`Utf8View`, `Binary`/`LargeBinary`/`BinaryView`. Declined (measured, §7): `timestamp[ns]` is a **pandas `Timestamp`** under `to_pylist`, not a `datetime`, and decimals carry a scale that `==` cannot police — so decimals, dates, times, timestamps, durations, `float16` and every nested kind keep the Python path. 34 pins green; the matrix compares `(type name, repr)` per cell, so `Decimal('1.23')` vs `Decimal('1.230')` and int-as-float are red. |
| C-003 | The map → dict, tz-aware-timestamp and calendar-interval contracts are unchanged, and the collector suspension is restored on every exit. | The pins; the mutation runs. | **PROVEN** | Map and tz-aware columns are converted by `_arrow_cell_to_spark_python` in Python and handed to the binding as supplied columns. A calendar interval **anywhere** in the schema returns the whole batch to the Python converter — load-bearing, not defensive: an interval nested in a list is not `needs_convert` and would otherwise reach the binding unrefused (mutation M5, 3 red). `gc.enable()` in a `finally`, pinned (M4, 1 red). |
| C-004 | `DataFrame.columns` answers from the plan's logical schema without an analyzer pass, and the names are byte-equal to the analyzed names on a fixture with aliases, unaliased arithmetic, coercion, wildcards, joins, unions, windows, nested fields and case-preserved names. | `crates/repark-python/src/logical_names.rs`; `python/repark/tests/test_perf_facade_logical_names.py`. | **PROVEN** | Sound by the tree's own invariant: every rule in `repark_functions::analyzer_rules` (`SparkDecimalPrecision`, `SparkDecimalRewrite`, `SparkIntegerOverflow`, `SparkExprSemantics`, the cardinality rules, the LTZ cast rule) rewrites through `NamePreserver`, as do DataFusion's `TypeCoercion` and `ResolveGroupingFunction`; none adds, drops or reorders a projection expression. Pinned over 19 planned statements + a 12-deep chain + 8 DataFrame transforms; `column_names` stays analyzer-backed as the oracle (M2, 14 red). |
| C-005 | `with_columns` reads `self.columns` once per call instead of once per existing column, without changing the duplicate-name `[AMBIGUOUS_REFERENCE]` contract; `core.py` shrinks and its ceiling ratchets down in the same commit. | `core.py`; `scripts/check_lib_py.py`; the profile; the facade suite. | **PROVEN** | `column_names` calls during a depth-100 build **5,750 → 0**; `_iter_bound_columns` + `_bind_schema_column` cost 34 ms of the 445 ms profiled build. Duplicate names fall back to the resolving path, so the raise is unchanged. `core.py` 6,368 → **6,303**, ceiling ratcheted, `scripts/map.md` row filed. M7 (wrong canonical) reds 2 of this unit's pins and `test_acceptance_helpers.py` in the suite. |
| C-006 | Same battery after, three repeats, with the floor: `collect` at 1e6 × 7 ≤ 1,500 ms, and no facade-boundary control of report §7.3 regresses beyond its floor. The depth-100 chain target is reported against what was measured, not asserted. | §8; `docs/perf/facade-boundary-baseline.md`. | **PROVEN** | `collect` 1e6 **955.76 ms** (target ≤ 1,500 — met with 36 % margin), 5.14×; chain depth 100 **366.71 ms** (target < 150 — **NOT met**, 2.4× the bar; cause measured in §7 and filed as `PERF-FACADE-CHAIN-2`). 19 control cells, 17 inside their floor; the two above it are `to_arrow` at 1e6 (+1.77 ms) and its 2-column twin (+0.92 ms), on a path the diff does not touch and whose cross-run drift on identical code was already +6.1 % between the analysis run and this unit's baseline. |
| C-007 | The measurement is load-independent: both halves are re-measured old-vs-new inside one process on one release module, and the reconstructed old path reproduces the `origin/main` battery. | §8 "Matched A/B"; `scratch/f1/probe_matched_ab.py`. | **PROVEN** | Old converter 4,773.64 → new 955.27 ms (**5.00×**) at 1e6; 451.40 → 67.45 (6.69×) at 1e5. Chain 8.38 → 1.83, 337.73 → 42.30, 2,469.54 → 371.62 (4.57× / **7.98×** / **6.65×**). The reconstructed old chain shape is within 4 % of the `origin/main` battery on all three depths, which is what makes it an A/B and not a strawman. |
| C-008 | Docs and gates: registry rows FIXED with before/after numbers, a new `docs/perf` baseline with the machine/profile header and the commands, `map.md` lockstep for every directory touched, and every gate in the brief exit 0. | §9, §10; the gates table. | **PROVEN** | `PERF-FACADE-COLLECT-1` / `PERF-FACADE-WITHCOLUMN-1` FIXED, `PERF-FACADE-CHAIN-2` BACKLOG. `docs/perf/facade-boundary-baseline.md` + its `map.md` row. Four `map.md` files in lockstep. Gates table §10. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-facade-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief. Both delivery gates are measured, not paraphrased, and the one that was missed (depth-100 build < 150 ms) is reported as missed with the profiled cause and a bounding measurement of the only remaining option, rather than restated as met.
      artifacts: [docs/perf/facade-boundary-baseline.md, task/ledgers/staging/perf-facade-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Type-width extremes with a null in every column (int8/int16/int32/int64 minima, uint8..uint64 maxima including 2^64-1, f32 widening, positive and negative infinity), empty string, NUL inside a string, non-ASCII and astral-plane text across all three UTF-8 layouts, empty and high-byte bytes across all three binary layouts, an all-null column, zero rows, zero columns, duplicate display names, and a batch object with no __arrow_c_array__.
      artifacts: [python/repark/tests/test_perf_facade_collect_rows.py]
    - id: AT-3
      status: ATTACKED
      evidence: The binding declines by returning None rather than raising, so an unsupported type is a slow path and never a user-visible error; a downcast miss and a non-struct export are typed ValueErrors; the pyfunction is wrapped in the crate's fence so no panic crosses FFI; make rust-panic-ban exit 0. The collector suspension is released in a finally, pinned.
      artifacts: [crates/repark-python/src/collect_rows.rs, python/repark/src/repark/spark/dataframe/rows_export.py]
    - id: AT-4
      status: N/A
      justification: Both additions are synchronous, single-threaded and stateless — a per-batch Arrow conversion holding the GIL, and a read of an immutable plan schema. No shared mutable state, no ordering assumption, no async. The one process-global touched is the cyclic collector, restored in a finally.
    - id: AT-5
      status: ATTACKED
      evidence: The only unsafe in the diff is the Arrow C Data Interface import, and it takes ownership exactly as the PyCapsule protocol specifies — pointer_checked validates both capsule names and non-nullness before the pointers are read, the structs are swapped for empty ones so the capsule destructors become no-ops, and from_ffi consumes each exactly once. unsafe_code is allow only in this crate, at this boundary, as its Cargo.toml records. No deserialization, no path, no network, no authz surface.
      artifacts: [crates/repark-python/src/collect_rows.rs, crates/repark-python/map.md]
    - id: AT-6
      status: ATTACKED
      evidence: The pre-existing converter is kept callable as rows_from_arrow_table_python and is the pin's oracle, so equality is measured against the old code on the same batch rather than against an expectation typed by hand. Cells are compared by repr as well as by value, which is what makes a lost Decimal scale or an int returned as a float red. The whole facade suite runs green: 4633 passed, 191 skipped, 0 failed.
      artifacts: [python/repark/tests/test_perf_facade_collect_rows.py, python/repark/tests/test_perf_facade_logical_names.py]
    - id: AT-7
      status: ATTACKED
      evidence: This unit is the performance work. Before and after are the analysis' own battery on a release module; after is the median of three repeats with the spread as the floor; both halves are also measured old-vs-new inside one process to remove load; nineteen boundary controls are reported with their floors, including the two that moved and why neither is this change.
      artifacts: [docs/perf/facade-boundary-baseline.md]
    - id: AT-8
      status: ATTACKED
      evidence: Contracts read from the source before use, not assumed. pyarrow 25.0.0 to_pylist was measured per type before choosing the native set, which is how timestamp[ns] returning a pandas Timestamp was caught and excluded. DataFusion 54.1's Analyzer::execute_and_check was read and makes no name guarantee of its own, so the invariant was traced to NamePreserver in every repark rule and in TypeCoercion, and then pinned. pyo3 0.29 renamed downcast to cast and exposes capsule pointers only through pointer_checked.
      artifacts: [crates/repark-python/src/logical_names.rs, crates/repark-python/map.md, python/repark/tests/test_perf_facade_logical_names.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every binding error names the fast path and the offending Arrow type; the decline is silent by design because it is a performance decision, not a failure, and a pin asserts the fast path is actually taken for a scalar batch and actually declined for a nested one, so a silent permanent fallback cannot pass as success.
      artifacts: [python/repark/tests/test_perf_facade_collect_rows.py]
    - id: AT-10
      status: ATTACKED
      evidence: Eight mutations built and run, not reasoned — three needed their own release build. All eight red. M1 was isolated from M8 on a null-free decimal batch so the decimal claim does not rest on the null mutation. The mutation table is section 6.
      artifacts: [task/ledgers/staging/perf-facade-1-ledger.md, python/repark/tests/map.md]
  complete: true
```

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

**Red first.** Both pin files were written and run before the binding existed: **27 failed, 2
passed** against the `origin/main` module (the two that passed are the pure-Python fallback
cases, which by construction do not reach the new code).

**Mutation.** Eight faults, each built and run. M1, M2 and M8 required their own
`maturin develop --release`; the clean module was rebuilt afterwards and is byte-size identical
to the pre-mutation build (`163,171,800 B`).

| # | Mutation | Where | Result |
|---|---|---|---|
| M1 | decimals converted in the binding as `f64` (the brief's named fault) | `collect_rows.rs` + the Python predicate | **RED** — delegated-type matrix and the wide session frame. Isolated from M8 on a null-free decimal batch: new `[('float','1.23')]` vs old `[('Decimal',"Decimal('1.230')")]` |
| M2 | logical column names upper-cased | `logical_names.rs` | **RED** — 14 name pins |
| M3 | shared names tuple reversed | `rows_export.py` | **RED** — 4 collect pins |
| M4 | the collector is never re-enabled | `rows_export.py` | **RED** — 1 pin |
| M5 | the calendar-interval whole-batch delegation removed | `rows_export.py` | **RED** — 3 interval pins (top level, inside a list, inside a struct) |
| M6 | map / tz cells no longer run `_arrow_cell_to_spark_python` | `rows_export.py` | **RED** — 1 pin |
| M7 | `_iter_bound_columns` passes a wrong canonical name | `core.py` | **RED** — 2 of this unit's pins, and `test_acceptance_helpers.py` in the facade suite |
| M8 | the null mask is ignored in the binding | `collect_rows.rs` | **RED** — scalar matrix and map/tz pins |

**Mutation score: 8 of 8 red.**

One guard is deliberately *not* claimed as mutation-detected: removing the duplicate-name
fallback in `_iter_bound_columns` reds nothing, because engine field names on the fallback path
are unique in every fixture the suite builds. It is kept because it is the only thing that
preserves the `[AMBIGUOUS_REFERENCE]` raise if that ever stops being true, and it is named here
rather than left as an unexamined line.

## 7. Design, and the alternatives that were measured

**Why the binding emits tuples and not `Row`s.** A `Row` is a `__slots__` class whose
construction is nine bytecodes; building it from Rust would mean `object.__new__` plus three
slot writes per row across FFI, for a class whose semantics (duplicate display names, the
factory form, pickling) live in one Python file. Measurement said the win was not there anyway:
with the collector suspended, `Row.from_ordered_fields` costs 285 ms per million rows against
`object.__new__` alone at 101 ms. The binding therefore converts *cells* and the facade builds
every `Row`, so `Row` keeps exactly one implementation.

**Why the batch is imported back through FFI rather than read from the stream.** `collect()`
already receives pyarrow batches from `to_arrow_batches`, which applies display-name renaming
and the tighten-metadata strip in Python. Having the binding re-run the query would duplicate
that pipeline; re-importing the batch through `__arrow_c_array__` costs one export/import per
batch (20 at 1e6) and leaves the pipeline untouched.

**Why the native type set is small.** pyarrow 25.0.0's `to_pylist` was measured per type before
the set was chosen. `timestamp[s|ms|us]` gives `datetime.datetime` but `timestamp[ns]` gives a
**pandas `Timestamp`**; decimals carry a scale that `==` does not police (`Decimal('1.23') ==
Decimal('1.230')` is `True`), which is why the pin compares `repr`. Rather than reproduce those
by hand, every such kind stays on the Python path and is handed to the binding as a supplied
column, so a frame with one timestamp among six integers still gets the win on the six.

**Why `columns` may skip the analyzer.** Not by hope: DataFusion 54.1's
`Analyzer::execute_and_check` checks plan invariants and makes no name guarantee, so the
invariant was traced instead — every repark analyzer rule wraps its rewrite in `NamePreserver`
(`analyzer.rs:41`, `decimal_spark.rs:95`, `integer_spark.rs:104`, `decimal_precision.rs:36`),
DataFusion's own `TypeCoercion` does the same, and no rule in the list adds, drops or reorders a
projection expression. Then it was pinned.

**The projection collapse (analysis option (c)) — measured, not built.** The brief makes it
conditional on the depth-100 build staying above 150 ms, and it does (366.71 ms). The collapsed
*shape* was built directly and timed before deciding: 100 `select`s of 7+i expressions on the
7-column base total **140.46 ms** against the shipped stacked 364.34 ms. A perfect collapse
therefore buys 2.6× and lands *on* the bar with no margin — while collapsing by inlining is
exponential when a new column reads an earlier new column (`c[n] = f(c[n-1])` at depth 100), and
the safe re-parenting variant mutates the plan lineage `_origin_plan_id`, the
`MISSING_ATTRIBUTES` contract and the adjacent-window-layer merge all depend on. Shipping that
as a rider on a perf unit would be trading a measured 6.5× for an unmeasured correctness risk.
Filed as `PERF-FACADE-CHAIN-2` with the numbers and the design sketch.

## 8. Measurement (C-001, C-006, C-007)

Full tables, machine header and commands: `docs/perf/facade-boundary-baseline.md`. Headlines,
before = one battery at `origin/main` `ef256d67` (load 7.85 → 6.95), after = median of three
repeats (loads 4.83 → 4.84, 4.84 → 9.44, 9.44 → 8.18), floor = spread of those three.

| cell | before | after | floor | × | target |
|---|---:|---:|---:|---:|---|
| `facade/export/1000000/collect` | 4,908.03 | **955.76** | 6.54 | **5.14×** | ≤ 1,500 — **met** |
| `facade/export/100000/collect` | 562.35 | **93.74** | 1.93 | 6.00× | — |
| `facade/export/1000000/collect_2col` | 2,058.79 | 527.39 | 13.96 | 3.90× | — |
| `facade/export/100000/collect_2col` | 236.20 | 49.97 | 2.13 | 4.73× | — |
| `facade/chain/10/build_only` | 8.14 | **1.83** | 0.07 | 4.45× | — |
| `facade/chain/50/build_only` | 328.48 | **42.28** | 0.86 | **7.77×** | — |
| `facade/chain/100/build_only` | 2,385.23 | **366.71** | 10.23 | **6.50×** | < 150 — **not met** |
| `facade/chain/100/build_and_count` | 2,550.21 | 464.04 | 5.70 | 5.50× | — |

Against Spark 4.1.2 `local[8]` *(recorded by PERF-ANALYSIS-1, not re-run)*: `collect` 1e6
3,619 ms — repark 1.37× slower → **3.79× faster**; depth-100 chain build 747 ms — repark 3.19×
slower → **2.04× faster**.

Matched A/B, one process, one module (C-007): converter 4,773.64 → 955.27 (5.00×) at 1e6 and
451.40 → 67.45 (6.69×) at 1e5; chain 8.38 → 1.83, 337.73 → 42.30, 2,469.54 → 371.62.

Controls (C-006): 19 cells, 17 inside their floor. The two outside are `to_arrow` 1e6
(25.57 → 27.34, floor 0.59) and `to_arrow_2col` 1e6 (10.37 → 11.29, floor 0.75). Neither is
this change: the diff adds no line to the `to_arrow` / `to_arrow_batches` path, the same cell at
1e5 moved *down*, and this cell's cross-run drift on identical code was already +6.1 % between
PERF-ANALYSIS-1 (24.1 ms) and this unit's baseline (25.57 ms) — the same magnitude as the step
being explained. A confirmation pass on the shipped module at the night's quietest load (1-min
**3.2**) settles it: `to_arrow` **24.24 ms**, back to the analysis' original 24.1 and *below*
this unit's own baseline, while `collect` 1e6 reads 961.91 and `chain/100/build_only` 365.82 —
both within 1 % of the reported medians. The control tracks load; the results do not.

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: perf-facade-1
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: perf-facade-1
  artifacts_verified:
    ledger: PASS (C-001..C-008 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: N/A (STATUS.md pinned closed by the brief)
  verdict: PASS
  rejection_route: N/A
```

## 10. Gates

| Gate | Exit |
|---|---|
| `make ci` | 0 |
| `make verify` | 0 |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 |
| `pytest python/repark/tests` | 0 (4,633 passed, 191 skipped) |
| `REPARK_PARITY_LIVE=1 pytest python/repark/tests/test_parity_live.py` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `maturin develop --release` | 0 (`__debug_assertions__ False`, `163,171,800 B`) |
