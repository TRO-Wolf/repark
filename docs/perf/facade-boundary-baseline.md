# Facade boundary baseline (PERF-FACADE-1)

Measured 2026-09-04 on this clone, before and after PERF-FACADE-1 (`collect()` row
materialization, and `withColumn` chain build). Not an H-3b hard-gated baseline: the
reference-host choice is still open and this host was **not idle**, so every cell carries its
own 1-minute load and its own measured floor.

pins: perf-facade-1/C-001, C-005, C-006

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| governor | schedutil |
| ram | 125.7 GiB |
| kernel | 6.8.0-138 |
| repark | 1.0.1; before at `origin/main` `ef256d67`, after on `perf/facade-1` |
| native (before / after) | `163,145,824 B` / `163,171,800 B` |
| release proof | `repark._native.__debug_assertions__ is False`; the probe harness refuses a debug module and refuses a module outside this lane |
| build | `VIRTUAL_ENV=.venv CARGO_BUILD_JOBS=8 maturin develop --release` |
| python / pyarrow | 3.12.3 / 25.0.0 |
| threads | `spark.sql.shuffle.partitions = 8` (`target_partitions = 8`) |
| batch size | repark default 65536 |
| fixture | `scratch/bed/synth_{100000,1000000}.parquet`, seed 42, 7 columns (`id` int64, `ts` int64, `v` double, `vi` int32, `s` string_view, `cat` string_view, `part` int32) |
| iterations | 5 timed after 1 warm-up per cell (`collect` cells 3), median reported |
| Spark column | PySpark 4.1.2 `local[8]`, **recorded** by PERF-ANALYSIS-1 in the same lane on 2026-09-04; not re-run here |

## What the floor is here

The "before" column is one run of the analysis battery at `origin/main` (load 7.85 → 6.95).
The "after" column is the **median of three** full repeats of the identical battery (loads
4.83 → 4.84, 4.84 → 9.44, 9.44 → 8.18), and `floor` is the spread of those three medians —
the dispersion of the same measurement on the same tree. A cell whose before/after difference
is inside its floor moved for reasons other than this change.

Because the two columns come from different hours, section "Matched A/B" below repeats both
halves **inside one process on one module**, where the only variable is which code path runs.
That is the load-independent measurement; the battery tables are the end-to-end confirmation.

## Commands

```bash
cd /tmp/oc-perfa/python/repark && VIRTUAL_ENV=/tmp/oc-perfa/.venv CARGO_BUILD_JOBS=8 \
  /tmp/oc-perfa/.venv/bin/maturin develop --release
cd /tmp/oc-perfa && .venv/bin/python -c \
  "import repark; from repark import _native; print(repark.__file__, _native.__debug_assertions__)"
.venv/bin/python scratch/probes/probe_facade.py export --rows 1000000
.venv/bin/python scratch/probes/probe_facade.py export --rows 100000
.venv/bin/python scratch/probes/probe_facade.py chain --depth 10
.venv/bin/python scratch/probes/probe_facade.py chain --depth 50
.venv/bin/python scratch/probes/probe_facade.py chain --depth 100
.venv/bin/python scratch/probes/probe_facade.py create --rows 100000
.venv/bin/python scratch/probes/profile_collect.py
.venv/bin/python scratch/probes/profile_chain.py 100
```

## 1. `collect()` — the headline (FACADE-COLLECT-1)

| cell | before | after (median of 3) | floor | after / before | Spark local[8] *(recorded)* |
|---|---:|---:|---:|---:|---:|
| `export/1000000/collect` (7 cols) | 4,908.03 | **955.76** | 6.54 | **5.14× faster** | 3,619 |
| `export/1000000/collect_2col` | 2,058.79 | **527.39** | 13.96 | 3.90× | 2,484 |
| `export/100000/collect` (7 cols) | 562.35 | **93.74** | 1.93 | **6.00× faster** | 410 |
| `export/100000/collect_2col` | 236.20 | **49.97** | 2.13 | 4.73× | — |

Isolated cost is `collect − to_arrow` on the same frame, read against the analysis' 10.1 ms
1e5 facade floor, the same way PERF-ANALYSIS-1 read it:

| isolated row-materialization cost | before | after | × the 10.1 ms floor |
|---|---:|---:|---:|
| 1e6 × 7 | 4,882.46 | **928.42** | 483× → **92×** |
| 1e5 × 7 | 555.00 | **86.74** | 55× → **8.6×** |

Target was ≤ 1,500 ms at 1e6 × 7. Delivered **955.76 ms**, 36 % under the bar. repark was
1.37× *slower* than Spark on this cell and is now **3.79× faster**; at 1e5 it was 1.37× slower
and is now 4.37× faster.

### Where the 4.88 s went (cProfile, before)

| component | 1e6 × 7 |
|---|---:|
| `Array.to_pylist()` × 7 columns | 3,405.6 ms |
| `zip(*columns)` → row tuples | 224.5 ms |
| `Row.from_ordered_fields` × 1e6 | 1,740.1 ms |

`to_pylist` per column, measured separately: int64 360.0 / 363.2, double 364.2, int32 345.8 /
316.1, string_view 834.5 / 799.9 ms.

### Why `Row.from_ordered_fields` cost 1.74 s, and what fixed it

The constructor is nine bytecodes. The cost was the cyclic collector: a `Row` has
object-valued slots, so every one of the million is GC-tracked, and each generation-2 pass
rescans the whole growing result. Measured on a synthetic million-row list:

| | collector on | collector off |
|---|---:|---:|
| `object.__new__(Row)` only | 480.9 | 100.6 |
| `object.__new__` + 3 slot writes | 731.6 | 291.0 |
| `Row.from_ordered_fields(tuple names)` | 741.9 | 285.1 |

So the fix is three things, not one: the cells convert in the binding (replacing `to_pylist`),
the names tuple is built once per batch rather than once per `Row`, and the collector is
suspended across the batch and restored in a `finally`.

## 2. `withColumn` chain build (FACADE-WITHCOLUMN-1)

| cell | before | after (median of 3) | floor | after / before | Spark local[8] *(recorded)* |
|---|---:|---:|---:|---:|---:|
| `chain/10/build_only` | 8.14 | **1.83** | 0.07 | 4.45× | 61.6 |
| `chain/50/build_only` | 328.48 | **42.28** | 0.86 | **7.77×** | 367 |
| `chain/100/build_only` | 2,385.23 | **366.71** | 10.23 | **6.50×** | 747 |
| `chain/100/build_and_count` | 2,550.21 | **464.04** | 5.70 | 5.50× | — |

repark was 3.19× slower than Spark on the depth-100 build and is now **2.04× faster**.

**The 150 ms target is not met** (366.71 ms, 2.4× the bar). What (a) + (b) removed and what
remains, from `profile_chain.py 100` (0.445 s total under the profiler):

| | before | after |
|---|---:|---:|
| `column_names` calls during a depth-100 build | 5,750 (each an analyzer pass on first touch) | 0 |
| Python inside `with_columns` | — | 99 ms |
| `PyDataFrame.select` (DataFusion `project`) | — | **346 ms of 445 ms** |

The residue is inside DataFusion, not the facade: `LogicalPlanBuilder::project` calls
`normalize_col` and `columnize_expr` per projected expression, and each scans the child
schema, so one `select` of N expressions over an N-field child is O(N²) and the chain is
O(depth³). At depth 100 that is 5,750 expressions resolved against schemas averaging 57
fields.

### Why the projection collapse was measured and not built

The analysis' option (c) — collapse consecutive projections so the child schema stays the
7-field base — was measured before being chosen against, by building the collapsed *shape*
directly (`select` of 7+i expressions on the base frame, for i in 0..100):

| shape, depth 100 | median |
|---|---:|
| stacked chain (shipped) | 364.34 |
| collapsed shape, total of 100 selects on the 7-column base | **140.46** |
| one `select` of 107 expressions on the 7-column base | 1.14 |

A *perfect* collapse therefore lands at ~140 ms — on the 150 ms bar with no margin, a further
2.6×. It is not free: collapsing by inlining duplicates an expression subtree every time a new
column reads an earlier new column, which is exponential in the chain depth
(`c[n] = f(c[n-1])` at depth 100), and the safe variant — re-parenting onto the previous
projection's input only when the new expression reads no computed column — mutates plan
lineage that `_origin_plan_id`, the `MISSING_ATTRIBUTES` contract and the adjacent-window-layer
merge all depend on. That is a unit with its own scope audit, not a rider on this one.
Filed as `PERF-FACADE-CHAIN-2`.

## 3. Facade boundary controls — no regression

Every other cell of PERF-ANALYSIS-1 §7.3 that this battery covers. `Δ vs floor` is the
before/after difference divided by that cell's measured floor; a ratio at or below 1 is inside
the noise of the measurement.

| cell | before | after (median of 3) | floor | Δ vs floor |
|---|---:|---:|---:|---:|
| `export/1000000/to_arrow` | 25.57 | 27.34 | 0.59 | 3.0 |
| `export/1000000/to_arrow_2col` | 10.37 | 11.29 | 0.75 | 1.2 |
| `export/1000000/toPandas` | 48.78 | 46.76 | 13.14 | −0.2 |
| `export/1000000/count` | 0.31 | 0.33 | 0.01 | 2.0 |
| `export/100000/to_arrow` | 7.35 | 7.00 | 0.16 | −2.2 |
| `export/100000/to_arrow_2col` | 2.62 | 2.48 | 0.08 | −1.8 |
| `export/100000/toPandas` | 12.28 | 11.89 | 0.17 | −2.3 |
| `export/100000/count` | 0.30 | 0.30 | 0.01 | 0.0 |
| `create/100000/tuples_count` | 1,749.96 | 1,729.22 | 30.43 | −0.7 |
| `create/100000/tuples_lazy` | 1,746.01 | 1,748.61 | 51.61 | 0.1 |
| `create/100000/pandas_count` | 2.91 | 3.04 | 0.34 | 0.4 |
| `create/100000/second_count` | 0.30 | 0.31 | 0.01 | 1.0 |
| `create/100000/second_to_arrow` | 0.22 | 0.22 | 0.00 | 0.0 |
| `chain/10/count` | 2.53 | 2.57 | 0.04 | 1.0 |
| `chain/50/count` | 24.10 | 24.18 | 0.68 | 0.1 |
| `chain/100/count` | 94.28 | 93.92 | 0.81 | −0.4 |
| `chain/10/to_arrow_limit1` | 10.62 | 10.36 | 0.17 | −1.5 |
| `chain/50/to_arrow_limit1` | 60.86 | 61.93 | 1.92 | 0.6 |
| `chain/100/to_arrow_limit1` | 221.37 | 219.53 | 2.14 | −0.9 |

The two cells above their floor are `to_arrow` at 1e6 (+1.77 ms) and its two-column twin
(+0.92 ms), and neither is this change. Three things say so. No line of the `to_arrow` /
`to_arrow_batches` path is touched by the diff. The same cell at 1e5 moved *down*. And the
cell's cross-run drift on **identical** code is already this large: PERF-ANALYSIS-1 recorded
**24.1 ms** at `55652cae`, and the same code measured 25.57 ms here hours later (+6.1 %), the
same magnitude as the step being explained.

A confirmation pass on the shipped module at the quietest load of the night (1-minute load
**3.2**, against 4.8–9.4 for the three battery repeats) closes it:

| cell | analysis, `55652cae` | before, load 7.9 | after ×3, load 4.8–9.4 | after, load 3.2 |
|---|---:|---:|---:|---:|
| `export/1000000/to_arrow` | 24.1 | 25.57 | 27.34 | **24.24** |
| `export/1000000/collect` | 4,963 | 4,908.03 | 955.76 | **961.91** |
| `chain/100/build_only` | 2,389 | 2,385.23 | 366.71 | **365.82** |

`to_arrow` on the shipped tree returns to the analysis' original 24.1 ms — *below* this unit's
own `origin/main` baseline — while the two headline cells reproduce within 1 %. The control
tracks load; the results do not.

## 4. Matched A/B — one process, one module, one load

The load-independent measurement. Both converters are callable on the shipped tree
(`rows_export.rows_from_arrow_table_python` is the pre-existing path, kept as the pin's
oracle), and the chain is built twice with `DataFrame.columns` / `_iter_bound_columns`
restored to their pre-unit bodies. Same batches, same module, same minute.

| leg | old path | new path | speed-up |
|---|---:|---:|---:|
| rows from 20 batches, 1e6 × 7 | 4,773.64 | **955.27** | **5.00×** |
| rows from 2 batches, 1e5 × 7 | 451.40 | **67.45** | **6.69×** |
| chain build depth 10 | 8.38 | **1.83** | 4.57× |
| chain build depth 50 | 337.73 | **42.30** | **7.98×** |
| chain build depth 100 | 2,469.54 | **371.62** | **6.65×** |

Loads 6.7 → 5.1 across the whole probe. The reconstructed old shape reproduces the
`origin/main` battery within 4 % on every chain cell (8.38 vs 8.14, 337.73 vs 328.48, 2,469.54
vs 2,385.23), which is what makes it a faithful A/B rather than a strawman.

## What this baseline is not

- Not an idle-host baseline. Two sibling lanes were building Rust throughout; loads are
  recorded per cell and per run.
- Not a Spark comparison run tonight. The Spark column is PERF-ANALYSIS-1's recorded
  `local[8]` measurement from earlier the same day, and it includes a JVM→Python Arrow
  transfer that repark does not pay.
- Not a claim about `createDataFrame`. That candidate is `FACADE-CDF-1` and is untouched here;
  its cells appear only as controls.

## Pointers

- Up: [map.md](map.md)
- Analysis this unit builds from: PERF-ANALYSIS-1 (candidates 1 and 3, slate items 1 and 2)
- Ledger: [../../task/ledgers/staging/perf-facade-1-ledger.md](../../task/ledgers/staging/perf-facade-1-ledger.md)
