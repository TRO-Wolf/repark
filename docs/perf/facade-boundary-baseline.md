# Facade boundary baseline (PERF-FACADE-1)

Measured 2026-09-04 on this clone. Every number below comes from one run of the tracked runner
`python/repark-parity/bench/facade/run_facade.py` on a release module. Not an H-3b hard-gated
baseline: the reference-host choice is still open and this host was **not idle** — the run's
1-minute load is recorded, and so is every cell's.

pins: perf-facade-1/C-001, C-006, C-007, C-009

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| governor | schedutil |
| ram | 125.7 GiB |
| kernel | 6.8.0-138 |
| repark | 1.0.1 on `perf/facade-1` |
| native | `163,171,800 B` |
| release proof | `repark._native.__debug_assertions__ is False`; `cells.release_proof()` raises rather than measure a debug module |
| build | `cd python/repark && VIRTUAL_ENV=../../.venv maturin develop --release` |
| python / pyarrow | 3.12.3 / 25.0.0 |
| threads | `spark.sql.shuffle.partitions = 8` (`target_partitions = 8`) |
| fixture | `bench/facade/fixture.py`, seed 42, 7 columns (`id` int64, `ts` int64, `v` double, `vi` int32, `s` string, `cat` string, `part` int32), zstd, 100 k row groups |
| iterations | 5 timed after 1 warm-up; `collect`-family cells 3, because each materializes a million rows |
| run load1 | **6.94 → 6.46** |
| floor | **1.64 ms**, the spread of 5 repeated medians of `collect/100000` |

## How to reproduce

```bash
cd python/repark && maturin develop --release
cd ../.. && .venv/bin/python python/repark-parity/bench/facade/run_facade.py \
  --out /tmp/oc-facade-bed --json /tmp/oc-facade-bed/run.json
```

`PYTHON=.venv/bin/python make facade-bench` runs the same battery and writes its rendered
report under the bed (bare `make facade-bench` works inside an activated venv).
`--cells export,collect,rows,create,chain` selects groups; `--iterations` and `--floor-repeats`
set the sample counts. The runner raises rather than measure a debug native module.

## Why before and after are one run

There is no stale "before" column here. The pre-unit code path is **reconstructed inside the
same process on the same module** and timed beside the shipped one, so the only variable in
each pair is which code runs — not the hour, not the load, not the binary:

| pair | old leg | new leg |
|---|---|---|
| `collect_old/N` vs `collect/N` | `DataFrame._rows_from_arrow_table` swapped back to the pure-Python converter | shipped |
| `rows_old/N` vs `rows_new/N` | `rows_export.rows_from_arrow_table_python` over pre-collected batches | `rows_from_arrow_table` over the same batches |
| `chain_old/D` vs `chain/D` | the pre-unit `columns` / `_iter_bound_columns` bodies swapped onto `DataFrame` | shipped |

Each swap is restored in a `finally`. The reconstruction is only faithful while those bodies
still mirror what this unit replaced — `bench/facade/map.md` says so, and says what to do when
they stop.

## 1. `collect()` — the headline (FACADE-COLLECT-1)

End to end, exactly what a caller pays, result list held:

| cell | old | new | speed-up | new spread |
|---|---:|---:|---:|---:|
| `collect/1000000` (7 cols) | 4,767.60 | **939.85** | **5.07×** | 11.64 |
| `collect/100000` (7 cols) | 525.24 | **90.88** | **5.78×** | 3.10 |

Target was ≤ 1,500 ms at 1e6 × 7. Delivered **939.85 ms**, 37 % under the bar.

The converter in isolation, over pre-collected batches, releasing each batch's rows instead of
holding a million of them:

| cell | old | new | speed-up |
|---|---:|---:|---:|
| `rows_*/1000000` | 4,945.68 | **557.88** | **8.87×** |
| `rows_*/100000` | 490.68 | **59.27** | **8.28×** |

The gap between the two tables is the most useful thing in them. On the new path, isolated
conversion is 557.88 ms but `collect()` costs 939.85 — about **382 ms is the price of *holding*
a million `Row` objects**, which `collect()`'s contract requires and no converter can remove.
On the old path the same two cells are 4,945.68 and 4,767.60, i.e. indistinguishable: holding
the result cost nothing extra there because the path was already dominated by the cyclic
collector. The remaining headroom in `collect()` is therefore live-object pressure, not
conversion — a different unit's problem, and `toLocalIterator` already avoids it.

### Where the old 4.77 s went

cProfile on the pre-unit path at 1e6 × 7: `Array.to_pylist()` across the 7 columns 3,405.6 ms,
`zip(*columns)` 224.5 ms, `Row.from_ordered_fields` × 1e6 1,740.1 ms. Per column, `to_pylist`
measured int64 360.0 / 363.2, double 364.2, int32 345.8 / 316.1, string 834.5 / 799.9 ms.

`Row.from_ordered_fields` is nine bytecodes; its 1.74 s was the cyclic collector, because a
`Row` has object-valued slots and every generation-2 pass rescans the growing result. On a
synthetic million-row list:

| | collector on | collector off |
|---|---:|---:|
| `object.__new__(Row)` only | 480.9 | 100.6 |
| `object.__new__` + 3 slot writes | 731.6 | 291.0 |
| `Row.from_ordered_fields(tuple names)` | 741.9 | 285.1 |

So the fix is three things: cells convert in the binding, the names tuple is built once per
batch instead of once per `Row`, and the collector is suspended across the batch and restored
in a `finally`.

## 2. `withColumn` chain build (FACADE-WITHCOLUMN-1)

| cell | old | new | speed-up | collapsed shape | `count` after build |
|---|---:|---:|---:|---:|---:|
| depth 10 | 8.23 | **1.85** | 4.45× | 1.94 | 2.54 |
| depth 50 | 334.00 | **42.44** | **7.87×** | 19.41 | 24.40 |
| depth 100 | 2,476.08 | **366.11** | **6.76×** | **65.04** | 93.81 |

Execution is untouched — the `count` column is the same work before and after; only plan
building changed.

**The 150 ms target is not met** (366.11 ms, 2.4× the bar). What (a) + (b) removed and what is
left, from `profile_chain.py 100` (0.445 s under the profiler):

| | before | after |
|---|---:|---:|
| `column_names` calls during a depth-100 build | 5,750, each an analyzer pass on first touch | 0 |
| Python inside `with_columns` | — | 99 ms |
| `PyDataFrame.select` (DataFusion `project`) | — | **346 ms of 445 ms** |

The residue is inside DataFusion, not the facade: `LogicalPlanBuilder::project` runs
`normalize_col` and `columnize_expr` per projected expression and each scans the child schema,
so one `select` of N expressions over an N-field child is O(N²) and the chain is O(depth³). At
depth 100 that is 5,750 expressions resolved against schemas averaging 57 fields.

### What a projection collapse would actually buy

The `chain_collapsed/D/build_only` cell measures the analysis' option (c) as a *shape*: one
expression list built once and appended to, each step projecting the 7 base columns plus the
`i+1` computed expressions **directly onto the base frame** so the child schema stays 7 fields
wide, with the whole loop timed exactly as the stacked cell times the whole `withColumn` loop.
`bench/facade/map.md` fixes that definition; measuring a different loop gives a different
number, which is precisely what went wrong the first time this was reported.

| depth | shipped stacked | collapsed shape | what a perfect collapse would buy |
|---:|---:|---:|---:|
| 10 | 1.85 | 1.94 | nothing (already flat) |
| 50 | 42.44 | 19.41 | 2.2× |
| 100 | 366.11 | **65.04** | **5.6×** |

**A perfect collapse lands at 65.04 ms — 2.3× *under* the 150 ms target, not on it.** An
earlier draft of this note reported the collapsed shape at 140.46 ms and concluded it "lands on
the bar with no margin"; that measurement rebuilt the whole expression list on every step,
which is O(depth²) Python a real collapse would never pay, and the conclusion drawn from it was
wrong. The prize is real and roughly 5.6× at depth 100.

The deferral therefore rests on **correctness, not on the size of the prize**. Collapsing by
inlining duplicates an expression subtree every time a new column reads an earlier new column,
which is exponential in the chain depth (`c[n] = f(c[n-1])`). The safe variant — re-parenting
onto the previous projection's input only when the new expression reads no computed column —
never duplicates, but it rewrites plan lineage, and `_origin_plan_id`, the `MISSING_ATTRIBUTES`
contract and the adjacent-window-layer merge are all defined in terms of that lineage. Trading
a measured 6.76× that is proven correct for a further 5.6× that is not is a scope decision, not
a rider on a perf unit. Filed as `PERF-FACADE-CHAIN-2` with these numbers.

## 3. Facade boundary controls

These paths are untouched by the unit — the diff adds no line to `to_arrow`,
`to_arrow_batches`, `toPandas`, `count` or `createDataFrame` — so they carry no old/new pair.
They are recorded because the next unit at this boundary needs a reproducible starting point,
and because a future change that moves them should have to explain itself.

| cell | median | min | spread |
|---|---:|---:|---:|
| `export/1000000/to_arrow` | 30.61 | 22.53 | 10.44 |
| `export/1000000/to_arrow_2col` | 12.77 | 12.10 | 1.73 |
| `export/1000000/toPandas` | 55.60 | 52.41 | 12.77 |
| `export/1000000/count` | 0.49 | 0.48 | 0.03 |
| `export/100000/to_arrow` | 8.51 | 8.32 | 4.83 |
| `export/100000/to_arrow_2col` | 2.92 | 2.91 | 0.03 |
| `export/100000/toPandas` | 11.10 | 10.69 | 0.44 |
| `export/100000/count` | 0.29 | 0.28 | 0.05 |
| `collect_2col/1000000` | 540.53 | 519.80 | 40.69 |
| `collect_2col/100000` | 45.31 | 45.18 | 1.24 |
| `create/100000/tuples_count` | 1,699.00 | 1,676.30 | 39.24 |
| `create/100000/pandas_count` | 4.00 | 3.52 | 0.66 |

`to_arrow` at 1e6 carries a 10.44 ms spread across its own five samples — a third of its
median. Cells this small are dominated by whatever else the box is doing; read them as an order
of magnitude, not as a number to compare across runs.

## 4. `createDataFrame(list of tuples)` goes column-wise (PERF-FACADE-CDF-1, 2026-09-05)

The §3 create controls are now a before/after pair. The inferred list-of-rows path infers and
converts column by column — one `set(map(type, …))` census per column, single-kind scalar
columns straight to Arrow, mixed/exotic columns through the unchanged per-cell path — and the
tuple loop skips the per-row permutation rebuild when the permutation is the identity. The old
leg is not a reconstruction: it calls the kept legacy path itself, swapped in for the timed
region. Same runner, one run, release module `163478728` B, load **10.75 → 10.51**, create
cells capped at 3 timed iterations after 1 warm-up:

| cell | old (ms) | new (ms) | × |
|---|---:|---:|---:|
| `create/100000/tuples_count` | 1,656.62 | **70.30** | **23.56×** |
| `create/100000/pandas_count` (control) | — | 3.00 | — |
| `create/10000/nested_count` | 261.70 | 273.21 | 0.96× |
| `create/100000/explicit_count` | 1,280.50 | 1,273.94 | 1.00× |

Three honest numbers, not one: the pre-unit absolute on the unchanged lane was **1,756.67 ms**
(same runner, load 14.35 → 14.65); the same-process pair above is 1,656.62 → 70.30. The pair
isolates the inference win — both legs share the permutation hoist. The ≤ 100 ms target is met
30% under the bar, on a loaded box.

The 0.96× nested pair is the measured delegation cost (one transpose and census before the
identical conversion), and the 1.00× explicit pair is the design (both legs run the identical
legacy path by dispatch — the explicit-schema path at ~1.27 s is now the slowest
createDataFrame shape and stays out of this unit). A second full-battery run the same day
(load ~19–25, floor **2.45 ms**, same runner) reproduced the tuples pair at 1,620.75 → 66.65
ms and confirmed the §1/§2 cells unmoved (`collect/1000000` 940.84, `chain/100/build_only`
344.01). Numbers: the unit ledger §8.

## What this baseline is not

- **Not an idle-host baseline.** Sibling lanes were building Rust throughout. Loads are
  recorded per cell and for the run.
- **Not a Spark comparison.** No cell here runs a JVM, deliberately: the box allows one Spark
  JVM at a time and this battery must not compete for it. PERF-ANALYSIS-1 recorded PySpark
  4.1.2 `local[8]` on a bed of the same schema — `collect` 1e6 3,619 ms, depth-100 chain build
  747 ms — which puts the shipped 939.85 and 366.11 at roughly 3.9× and 2.0× faster than Spark.
  Those ratios are across two runs on two beds and are indicative, not a same-run measurement.
- **Not a `createDataFrame` result** (until 2026-09-05). That candidate was `FACADE-CDF-1`
  and untouched at this writing; its cells appeared only as controls. PERF-FACADE-CDF-1 has
  since landed — §4 is its before/after, measured with the same runner.
- **Not the first run of this unit.** The unit's original numbers came from probe scripts under
  an untracked `scratch/` directory. They agreed with these within a few percent, but a number
  whose runner is not in the tree cannot be re-derived and is not a baseline; they were
  discarded rather than carried forward. The review-gap block in the unit ledger records that.

## Pointers

- Up: [map.md](map.md)
- Runner: [../../python/repark-parity/bench/facade/map.md](../../python/repark-parity/bench/facade/map.md)
- Ledger: [../../task/ledgers/staging/perf-facade-1-ledger.md](../../task/ledgers/staging/perf-facade-1-ledger.md)
