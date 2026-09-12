# Unit ledger — PERF-FACADE-1 · `collect()` rows in the binding, `withColumn` chains made linear

**Date:** 2026-09-04 · **Branch:** `perf/facade-1` · **Base:** `origin/main` `ef256d67` ·
**Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `PERF-FACADE-COLLECT-1` **FIXED**, `PERF-FACADE-WITHCOLUMN-1` **FIXED**,
`PERF-FACADE-CHAIN-2` and `COLLECT-STRUCT-ROW-1` filed BACKLOG.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ANALYSIS-1 ranked eleven measured candidates and queued these two first as
"the biggest absolute user-visible walls and pure facade/binding work" — candidate 1
(`collect()` 4,963 ms at 1e6 × 7 where `to_arrow()` is 24 ms, and Spark is 1.4× faster) and
candidate 3 (a depth-100 `withColumn` chain costing 2,376 ms to *build*, 3.2× slower than
Spark).

**Not in this unit:** `createDataFrame` (candidate 2, `FACADE-CDF-1`); every Iceberg candidate;
the `avg` groups accumulator; the window-frame charter; any public API change; the projection
collapse (measured and filed as `PERF-FACADE-CHAIN-2`, §7).

**Writable paths (round 2 adds the bench harness):**
`python/repark-parity/bench/facade/`, the `facade-bench` target in `Makefile`,
`crates/repark-python/src/{collect_rows.rs,logical_names.rs,lib.rs}`,
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
| C-001 | Baseline first, and from a runner anyone can re-run: the pre-unit numbers come from a TRACKED harness on a RELEASE module, not from probe scripts in an untracked directory, and every cell records its 1-minute load. | `python/repark-parity/bench/facade/`; the release proof; §8. | **PROVEN** | Round 1 measured the analysis' own untracked `scratch/probes` battery at `origin/main` `ef256d67` (module `163,145,824 B`, load 7.85 → 6.95) and reproduced PERF-ANALYSIS-1 within 2 % on every headline cell. That run is **superseded and not carried forward** (review gap R2-2): a number whose runner is not in the tree cannot be re-derived. Every figure in this ledger and in the baseline note now comes from `run_facade.py`, one run, load 6.94 → 6.46, native `163,171,800 B`, `__debug_assertions__ False`, floor **1.64 ms**. |
| C-002 | Row materialization moves into `repark-python`, and the binding converts ONLY cell kinds whose `to_pylist` mapping is unambiguous; every other kind is converted by the unchanged Python path or declines the whole batch. A wide type matrix proves the two converters return objects equal by value AND by `repr`. | `crates/repark-python/src/collect_rows.rs`; `python/repark/tests/test_perf_facade_collect_rows.py`; the mutation runs. | **PROVEN** | Native set: null, boolean, int8..int64, uint8..uint64, `f32`, `f64`, `Utf8`/`LargeUtf8`/`Utf8View`, `Binary`/`LargeBinary`/`BinaryView`. Declined (measured, §7): `timestamp[ns]` is a **pandas `Timestamp`** under `to_pylist`, not a `datetime`, and decimals carry a scale that `==` cannot police — so decimals, dates, times, timestamps, durations, `float16` and every nested kind keep the Python path. 34 pins green; the matrix compares `(type name, repr)` per cell, so `Decimal('1.23')` vs `Decimal('1.230')` and int-as-float are red. |
| C-003 | The map → dict, tz-aware-timestamp and calendar-interval contracts are unchanged, and the collector suspension is restored on every exit. | The pins; the mutation runs. | **PROVEN** | Map and tz-aware columns are converted by `_arrow_cell_to_spark_python` in Python and handed to the binding as supplied columns. A calendar interval **anywhere** in the schema returns the whole batch to the Python converter — load-bearing, not defensive: an interval nested in a list is not `needs_convert` and would otherwise reach the binding unrefused (mutation M5, 3 red). `gc.enable()` in a `finally`, pinned (M4, 1 red). |
| C-004 | `DataFrame.columns` answers from the plan's logical schema without an analyzer pass, and the names are byte-equal to the analyzed names on a fixture with aliases, unaliased arithmetic, coercion, wildcards, joins, unions, windows, nested fields and case-preserved names. | `crates/repark-python/src/logical_names.rs`; `python/repark/tests/test_perf_facade_logical_names.py`. | **PROVEN** | Sound by the tree's own invariant: every rule in `repark_functions::analyzer_rules` (`SparkDecimalPrecision`, `SparkDecimalRewrite`, `SparkIntegerOverflow`, `SparkExprSemantics`, the cardinality rules, the LTZ cast rule) rewrites through `NamePreserver`, as do DataFusion's `TypeCoercion` and `ResolveGroupingFunction`; none adds, drops or reorders a projection expression. Pinned over 19 planned statements + a 12-deep chain + 8 DataFrame transforms; `column_names` stays analyzer-backed as the oracle (M2, 14 red). |
| C-005 | `with_columns` reads `self.columns` once per call instead of once per existing column, without changing the duplicate-name `[AMBIGUOUS_REFERENCE]` contract; `core.py` shrinks and its ceiling ratchets down in the same commit. | `core.py`; `scripts/check_lib_py.py`; the profile; the facade suite. | **PROVEN** | `column_names` calls during a depth-100 build **5,750 → 0**; `_iter_bound_columns` + `_bind_schema_column` cost 34 ms of the 445 ms profiled build. Duplicate names fall back to the resolving path, so the raise is unchanged. `core.py` 6,368 → **6,303**, ceiling ratcheted, `scripts/map.md` row filed. M7 (wrong canonical) reds 2 of this unit's pins and `test_acceptance_helpers.py` in the suite. |
| C-006 | The delivery gates are measured and each is reported against what was measured, met or missed: `collect` at 1e6 × 7 ≤ 1,500 ms, and the depth-100 chain build < 150 ms. | §8; `docs/perf/facade-boundary-baseline.md`. | **PROVEN** | `collect/1000000` **4,767.60 → 939.85 ms** (5.07×) — target ≤ 1,500 **met**, 37 % under the bar. `chain/100/build_only` **2,476.08 → 366.11 ms** (6.76×) — target < 150 **NOT met**, 2.4× the bar. The miss is reported as a miss here, in the commit message, in the registry row and in the baseline note; its cause is profiled (346 ms of 445 ms is DataFusion's own `LogicalPlanBuilder::project`) and the one remaining option is measured rather than argued (§7). Boundary controls carry no old/new pair because the diff adds no line to `to_arrow` / `to_arrow_batches` / `toPandas` / `count` / `createDataFrame`; they are recorded so the next unit at this boundary starts from a reproducible number. |
| C-007 | The measurement is load-independent by construction: every before/after pair runs old-vs-new inside ONE process on ONE release module, so the only variable is which code path runs. | §8; `bench/facade/cells.py`; `bench/facade/map.md` "The before/after contract". | **PROVEN** | Three reconstructed pairs, each restored in a `finally`: `collect_old` swaps `DataFrame._rows_from_arrow_table` back to the pure-Python converter (4,767.60 → 939.85 at 1e6; 525.24 → 90.88 at 1e5); `rows_old`/`rows_new` run both converters over one set of pre-collected batches (4,945.68 → 557.88; 490.68 → 59.27); `chain_old` swaps the pre-unit `columns` / `_iter_bound_columns` bodies (8.23 → 1.85, 334.00 → 42.44, 2,476.08 → 366.11). The old legs land within 4 % of round 1's independently measured `origin/main` battery on every chain depth, which is what makes them an A/B and not a strawman. |
| C-008 | Docs and gates: registry rows FIXED with before/after numbers, a `docs/perf` baseline with the machine/profile header and a reproduce block, `map.md` lockstep for every directory touched, and every gate exit 0. | §9, §10; the gates table. | **PROVEN** | `PERF-FACADE-COLLECT-1` / `PERF-FACADE-WITHCOLUMN-1` FIXED; `PERF-FACADE-CHAIN-2` and `COLLECT-STRUCT-ROW-1` BACKLOG. `docs/perf/facade-boundary-baseline.md` + its `map.md` row. Six `map.md` files in lockstep. Gates table §10, including the `python/repark-parity` suite the CI Python job runs and `make ci` does not. |
| C-009 | The baseline is reproducible by someone who does not have this lane: a tracked runner builds its own fixture, refuses a debug module, measures both legs of every pair, prints medians / spread / load, and is reachable from `make`. | `python/repark-parity/bench/facade/`; `make facade-bench`; the baseline's reproduce block. | **PROVEN** | `run_facade.py` (+ `fixture.py`, `cells.py`, `measure.py`, `map.md`) writes the seed-42 seven-column parquet under `/tmp/oc-facade-bed`, never the repo; `release_proof()` raises on a debug module; `--cells` selects any of `export,collect,rows,create,chain`; `--iterations` / `--floor-repeats` set the sample counts; the floor is re-measured every run as the spread of 5 repeated `collect/100000` medians (**1.64 ms** this run). `make facade-bench` runs it beside `dynflatten-bench`. Every number in the baseline note and the two registry rows is this runner's output. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

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
      evidence: This unit is the performance work. Every number comes from a tracked runner on a release module, with both legs of every before/after pair reconstructed inside one process so load cannot be the difference; the floor is re-measured each run; the boundary controls are recorded. Round 2 replaced the whole first baseline because its runner was untracked, and corrected the one deferral number that did not reproduce.
      artifacts: [docs/perf/facade-boundary-baseline.md, python/repark-parity/bench/facade/map.md]
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
      evidence: Eight mutations built and run, not reasoned — three needed their own release build. All eight red. M1 was isolated from M8 on a null-free decimal batch so the decimal claim does not rest on the null mutation. The mutation table is section 6; an independent critic on its own clone reproduced the unit and reds 7 of its own mutations (section 12).
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
conditional on the depth-100 build staying above 150 ms, and it does (366.11 ms). The collapsed
*shape* is a cell of the tracked runner (`chain_collapsed/D/build_only`) with an exact
definition in `bench/facade/map.md`: one expression list built once and appended to, each step
projecting the 7 base columns plus the `i+1` computed expressions directly onto the base frame
so the child schema stays 7 fields wide, and the whole loop timed exactly as the stacked cell
times the whole `withColumn` loop.

| depth | shipped stacked | collapsed shape | what a perfect collapse buys |
|---:|---:|---:|---:|
| 10 | 1.85 | 1.94 | nothing |
| 50 | 42.44 | 19.41 | 2.2× |
| 100 | 366.11 | **65.04** | **5.6×** |

**Round 1 got this wrong and the correction matters.** It reported the collapsed shape at
140.46 ms and concluded a perfect collapse "lands on the bar with no margin" — i.e. that the
prize was too small to be worth the risk. The 140.46 ms loop rebuilt the entire expression list
on every step, which is O(depth²) Python work a real collapse would never do; measured
properly the shape is **65.04 ms, 2.3× *under* the 150 ms bar**. The prize is real and large.

So the deferral no longer rests on the prize being small — it rests on correctness alone, which
is the only ground that was ever load-bearing. Collapsing by inlining duplicates an expression
subtree whenever a new column reads an earlier new column, which is exponential in the depth
(`c[n] = f(c[n-1])` at 100). The safe variant re-parents onto the previous projection's input
only when the new expression reads no computed column — it never duplicates, but it rewrites
plan lineage, and `_origin_plan_id`, the `MISSING_ATTRIBUTES` contract and the
adjacent-window-layer merge are all defined in terms of that lineage. Trading a measured 6.76×
that is proven correct for a further 5.6× that is not is a scope decision with its own audit,
not a rider on a perf unit. Filed as `PERF-FACADE-CHAIN-2` with these numbers.

## 8. Measurement (C-001, C-006, C-007, C-009)

Full tables, machine header and the reproduce block: `docs/perf/facade-boundary-baseline.md`.
Every figure below is one run of `python/repark-parity/bench/facade/run_facade.py` on the
shipped release module (native `163,171,800 B`, `__debug_assertions__ False`), load
**6.94 → 6.46**, 5 iterations after 1 warm-up (`collect`-family 3), floor **1.64 ms** as the
spread of 5 repeated `collect/100000` medians. Old and new legs run in the same process.

| cell | old | new | × | target |
|---|---:|---:|---:|---|
| `collect/1000000` (end to end) | 4,767.60 | **939.85** | **5.07×** | ≤ 1,500 — **met** |
| `collect/100000` (end to end) | 525.24 | **90.88** | 5.78× | — |
| `rows_*/1000000` (converter only) | 4,945.68 | **557.88** | **8.87×** | — |
| `rows_*/100000` (converter only) | 490.68 | **59.27** | 8.28× | — |
| `chain/10/build_only` | 8.23 | **1.85** | 4.45× | — |
| `chain/50/build_only` | 334.00 | **42.44** | **7.87×** | — |
| `chain/100/build_only` | 2,476.08 | **366.11** | **6.76×** | < 150 — **not met** |
| `chain/100/count` (execution control) | — | 93.81 | — | unchanged |

The 382 ms between the isolated converter (557.88) and `collect()` (939.85) at 1e6 is the cost
of **holding** a million live `Row` objects, which `collect()`'s contract requires and no
converter can remove; on the old path the same two cells are 4,945.68 and 4,767.60, i.e.
indistinguishable, because that path was GC-dominated either way. The remaining headroom in
`collect()` is live-object pressure, not conversion.

Boundary controls (`to_arrow`, `to_arrow_2col`, `toPandas`, `count`, `createDataFrame`,
`collect_2col`) are recorded in the baseline §3. They carry no old/new pair on purpose: the
diff adds no line to any of those paths, so there is no mechanism by which they could move, and
a cross-run difference between two hours would be measuring the box rather than the change.

Against Spark: no cell here starts a JVM, deliberately — the box allows one at a time and this
battery must not compete for it. PERF-ANALYSIS-1's recorded `local[8]` numbers on a bed of the
same schema (`collect` 1e6 3,619 ms, depth-100 build 747 ms) put the shipped 939.85 and 366.11
at roughly 3.9× and 2.0× faster, across two runs on two beds — indicative, not a same-run
comparison, and labelled that way in the baseline.

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: perf-facade-1
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: perf-facade-1
  artifacts_verified:
    ledger: PASS (C-001..C-009 PROVEN)
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
| `make verify` | 0 (ci + the whole Rust workspace suite) |
| `make check-python-conventions` | 0 (238 files clean, nested-def rows 0) |
| `make rust-panic-ban` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q` | 0 (**4,636 passed**, 191 skipped, on this tip) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (**572 passed**; the CI Python job runs this and `make ci` does **not** — round-2 gap R2-1) |
| `REPARK_PARITY_LIVE=1 .venv/bin/python -m pytest python/repark/tests/test_parity_live.py -q` | 0 (119 passed; needs `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` — the default JDK on this box is 11 and Spark 4.1.2 needs 17) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `maturin develop --release` | 0 (`__debug_assertions__ False`, `163,171,800 B`) |
| `PYTHON=.venv/bin/python make facade-bench` | 0 (the tracked baseline runner; 32 cells). Bare `make facade-bench` needs an activated venv — the target resolves `$(PYTHON)`, default `python`, and the runner imports `repark`/`pyarrow`/`numpy`. `dynflatten-bench` hard-codes bare `python` and carries the same requirement without the override. |

**Known load-flaky, not this unit:**
`python/repark-parity/tests/test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool`
fails under box contention — it asserts a spill that a small fair pool only reaches when the
machine is not otherwise busy. It passed on a quiet re-run here and is unrelated to this diff
(no spill, join or memory-pool code is touched). Flagged so the next reader does not spend the
round on it.

## 11. Out of scope, observed and filed

- **`COLLECT-STRUCT-ROW-1`** (BACKLOG, registry §7). A `StructType` cell comes back from
  `collect()` as a `dict`; live PySpark 4.1.2 returns a nested `Row`. Measured here:
  `SELECT named_struct('n', 1, 't', 'x') AS st` gives repark `{'n': 1, 't': 'x'}` (`dict`) and
  Spark `Row(n=1, t='x')` (`Row`), with Spark's `asDict()` keeping the nested `Row`. **Not
  introduced by this unit** — the pre-existing Python converter and the new binding path return
  the identical `dict`, which is exactly what the converter-equality pin asserts, so the unit's
  own gate is what proves it is pre-existing. The cause is `_arrow_cell_to_spark_python`'s
  struct arm, which has built a `dict` since the facade was written. Fixing it means deciding
  what `asDict(recursive=False)` and `Row.__eq__` do with nested rows, so it belongs to a
  `Row`-contract unit, not to a converter change. Current answer pinned in
  `test_perf_facade_collect_rows.py::test_struct_cell_is_a_dict_not_a_row_collect_struct_row_1`.
- **`PERF-FACADE-CHAIN-2`** (BACKLOG, registry §7). The remaining 366.11 ms of depth-100 chain
  build, and the measured 65.04 ms ceiling of the collapse that would close it. §7.
- **`FACADE-CDF-1`** — `createDataFrame` from tuples is 1,699.00 ms at 1e5 and is the analysis'
  candidate 2. Untouched; its cells appear here only as controls.

## 12. Review — round 2 (critic on its own clone, PASS with findings)

The critic rebuilt on a separate clone and reproduced the unit: `collect` 1e6 × 7
4,932 → 992 ms and chain-100 2,270 → 378 ms in its own matched A/B, 5,750 → 0 analyzer passes
exactly, zero mismatches on a 35-column type matrix old-vs-new and end to end, GC sound on
every axis, 7 mutations red, house rules clean. Six findings, all remediated here.

| # | Sev | Finding | Resolution |
|---|---|---|---|
| R2-1 | S2 | The CAP-1 mirror pin table in `python/repark-parity/tests/test_cap_1_source_file_line_cap.py` still carried `core.py` at 6368 after the ratchet to 6303. `make ci` does not run the `repark-parity` suite, so the local gate set never saw it. | Fixed by the coordinator in `c7ad6c70`. The `repark-parity` suite is now a named row in §10 so the next unit runs the gate that would have caught it, rather than trusting `make ci` to be the whole surface. |
| R2-2 | S2 | The baseline note was **not reproducible**: its Commands block named `scratch/probes/*.py` and `scratch/bed/synth_*.parquet`, which are untracked and local to this lane. | New tracked harness `python/repark-parity/bench/facade/` (fixture, cells, orchestrator, CLI, `map.md`) plus `make facade-bench`; the baseline's reproduce block now names it, and **every number in the note, the ledger and both registry rows was re-measured with it** on the shipped release module. The round-1 numbers were discarded, not carried forward. C-009. |
| R2-3 | S2 | The `PERF-FACADE-CHAIN-2` deferral rested on a number that did not reproduce: 140.46 ms for the collapsed shape, and the conclusion "lands on the bar with no margin". The critic measured the described shape at 46.89 ms one way and 654 ms the other. | The shape is now a runner cell with an exact definition in `bench/facade/map.md`, measured at **65.04 ms** at depth 100 — **2.3× under the bar**, so a perfect collapse would buy a further 5.6×. Round 1's 140.46 ms rebuilt the whole expression list every step (O(depth²) Python a real collapse never pays) and its conclusion was wrong. The registry row, §7 and the baseline now say the prize is large and the deferral rests on **correctness alone** (plan lineage, `_origin_plan_id`, `MISSING_ATTRIBUTES`, the window-layer merge). |
| R2-4 | S3 | §10 said 4,633 passed; the commit message and the critic's run said 4,635. | The 4,633 was written before round 1 parametrized the calendar-interval pin over three nestings (top level, inside a list, inside a struct), which added the two cases the commit message and the critic both saw. §10 now records the count measured on **this** tip, **4,636**, which is 4,635 plus the `COLLECT-STRUCT-ROW-1` answer pin R2-5 adds. Re-counted rather than copied from the critic's run, since that run predates this commit. |
| R2-5 | S3 | A pre-existing struct-cell divergence found during review and not filed anywhere. | Filed as `COLLECT-STRUCT-ROW-1` with the measured pair and a pin; §11. |
| R2-6 | S3 | `test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool` is load-flaky under box contention. | Noted in §10 under "Known load-flaky, not this unit"; no code touched. |

**Comment self-check, round 2.** `git diff --cached | grep -nE '^\+.*(//|#)'` over the
round-2 diff returns exactly two lines, neither of which is a comment: the shebang
`#!/usr/bin/env python3` on `run_facade.py` (an executable CLI; `run_dynflatten.py` carries the
identical line) and a markdown `#` heading inside an f-string in the report renderer. No Rust,
Python, shell, TOML or YAML comment was added in either round, and the `Makefile` target uses
only the `##` help annotation the other bench targets use.

**Process note.** A superseded background `pytest` of this unit's kept running for ~2.4 h after
round 1's hand-back and blocked another unit's JVM. Round 2 checks
`pgrep -af 'pytest|pyspark-shell' | grep oc-perfa` before hand-back and leaves nothing running;
the facade bench harness deliberately starts no JVM at all, so it cannot repeat the block.
