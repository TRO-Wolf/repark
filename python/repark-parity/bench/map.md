# map — python/repark-parity/bench

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

Reproducible **local** performance measurement scripts (R-PERF-MEASURE). No product code;
no AWS. Outputs feed `task/perf-report-*.md`.

## Contents

- [writepath/](writepath/map.md) — the tracked probes behind
  [docs/perf/iceberg-write-baseline.md](../../../docs/perf/iceberg-write-baseline.md): the write
  cells, the grouping refutation and the grouping-independent invariants
  (PERF-ICE-WRITEPATH-1 round 3).
  pins: perf-ice-writepath-1/C-010

- `bench_coalesce_chain.py` — progressive withColumns/sort/show wall times on a VALUES frame.
- `bench_mor_merge.py` — local memory-catalog MoR vs COW MERGE phase timings;
  `--seed parquet` (fast polars seed) + `--concurrency N` (`repark.write.max-concurrent-files`)
  + `--codec {zstd,uncompressed,…}` (`write.parquet.compression-codec` on CTAS; prints
  `warehouse_bytes`).
- `tpch/` — **R-TPCH-HARNESS** TPC-H scoreboard (DuckDB dbgen → parquet → repark vs DuckDB);
  see [tpch/map.md](tpch/map.md).
- `tpcds/` — **R-TPCDS-HARNESS** (D1) TPC-DS scoreboard (DuckDB dsdgen → parquet → repark vs
  DuckDB; 99 queries; parquet only); see [tpcds/map.md](tpcds/map.md).
- `fuzz/` — **R-SQL-FUZZER** seeded differential SQL fuzzer (RePark vs DuckDB);
  see [fuzz/map.md](fuzz/map.md).
- `write/` — **R-WRITE-BENCH (W1)** local-fs CTAS+append K × target-file-size matrix
  plus r22 extension MERGE (1M/10M × narrow/wide × K) and INSERT OVERWRITE peak RSS
  (OTH-004); measurement only; see [write/map.md](write/map.md).
- `mw7/` — **MW-7** Iceberg scale measurement: a partitioned v2 table, MOR and COW legs,
  N MERGEs with a per-checkpoint census (delete files, manifests, manifest-list bytes,
  `COUNT(*)`, scan p50/p99), then the five-procedure maintenance sequence and the same
  scans again. Measurement only; see [mw7/map.md](mw7/map.md).
- `ta/` — **P-2 (perf-wave-14)** Python TA pipeline baseline battery (§8.1–8.5,
  §8.7); measurement only; §8.6 is #116 (do not rebuild); see [ta/map.md](ta/map.md).
- `windows/` — **W-0** window-shape measurement: sliding frames per aggregate
  class, constant frame, unpartitioned `ORDER BY` at 1e7, `lead`/`lag` over an
  unsorted Iceberg scan, window over `memory_limit`; DuckDB 1.5.5 and PySpark
  4.1.2 oracles; see [windows/map.md](windows/map.md).
- `dynflatten/` — **PERF-DYNFLATTEN-1** `dynamicFlatten` measurement bed +
  isolated repark cells + Spark explode oracle; see [dynflatten/map.md](dynflatten/map.md).
  pins: perf-dynflatten-1-measure/C-001, C-002, C-003
- `facade/` — **PERF-FACADE-1** facade-boundary battery: `collect()` row materialization and
  `withColumn` chain building, with the pre-unit code path reconstructed in-process so both
  legs of every before/after pair share one module and one load. No JVM, by design — the box
  allows one Spark JVM at a time and this battery must not compete for it. See
  [facade/map.md](facade/map.md). pins: perf-facade-1/C-009
- `spill/` — **H3-SPILL-1** the Never-OOM truth table: every operator the engine can plan,
  under a bounded `FairSpillPool` at 1e6 and 1e7 rows, classified `ok` / `spilled` /
  `degraded` / `clean_error` / `abort` / `wrong`. One subprocess per cell under an
  address-space cap; peak RSS polled from `/proc`; the answer compared against the unbounded
  run. See [spill/map.md](spill/map.md). pins: h3-spill-1/C-001, C-002
- [icescan/](icescan/map.md) — **PERF-ICE-SCAN-1** read cells: bed generator plus the
  §7.4 before/after battery (`count_star`, `count_id`, `sum_all`, `string_len`, DV legs).
  See [icescan/map.md](icescan/map.md).
  pins: perf-ice-scan-1/C-009
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Reproduce the coalesce report numbers | `bench_coalesce_chain.py --rows N` |
| Reproduce local MERGE timings | `bench_mor_merge.py --rows N --source M` |
| Fast seed + concurrency sweep (local only) | `bench_mor_merge.py --seed parquet --concurrency 1\|4` |
| Compare zstd vs uncompressed local bytes/wall | `bench_mor_merge.py --seed parquet --codec zstd\|uncompressed` |
| Run TPC-H SF1 scoreboard | `tpch/run_tpch.py --sf 1 --report …` |
| Run TPC-DS SF1 scoreboard | `tpcds/run_tpcds.py --sf 1 --report …` |
| Read TPC-H findings | `../../../task/tpch-report-2026-07-31.md` |
| Read TPC-DS findings | `../../../task/tpcds-report-2026-07-31.md` |
| Run SQL fuzzer smoke (seed 42, 200q) | `fuzz/run_fuzz.py` or `python/repark/tests/test_fuzz_smoke.py` *(facade path — arrives with the facade package in the phase-3 facade PR)* |
| Long fuzz pass | `REPARK_FUZZ_N=5000 python …/fuzz/run_fuzz.py --out …` |
| Run write-path K×file-size bench (SF1) | `write/run_write_bench.py --mode ctas --sf 1 --report task/write-bench-report-….md` |
| Run the P-2 TA pipeline battery | `ta/bench_kernel_race.py` (and siblings); `--quick` for n=1e5 |
| Run the W-0 window-shape bench | `windows/run_w0.py --scale quick\|full --scratch <dir> --out <json>` |
| Read W-0 numbers | [../../../task/window-bench-report-2026-08-31.md](../../../task/window-bench-report-2026-08-31.md) |
| Run the dynamicFlatten measurement | `dynflatten/run_dynflatten.py --scale gate\|quick\|full --out /tmp/oc-dynflatten-bed` |
| Run the facade-boundary measurement | `facade/run_facade.py --out /tmp/oc-facade-bed` (or `make facade-bench`) |
| Run the spill matrix | `spill/measure.py --scratch <dir> --json-out <file>` |
| Read the spill matrix | [../../../docs/perf/spill-matrix-baseline.md](../../../docs/perf/spill-matrix-baseline.md) |
| Read dynamicFlatten numbers | [../../../docs/perf/dynamic-flatten-baseline.md](../../../docs/perf/dynamic-flatten-baseline.md) |
| Run the MW-7 scale measurement | `mw7/run_mw7.py --rows N --merges M --scratch <dir>` |
| Read MW-7's numbers | [../../../task/ledgers/completed/mw-7-scale-measurement-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-24-mw-7-scale-measurement-ledger.md) |
| Run r22 MERGE+OVERWRITE extension | `write/run_write_bench.py --mode extension --assert-release --report task/write-bench-report-r22-extension.md` |
| Read TPC-H findings | `../../../task/tpch-report-2026-07-31.md` |
| Read fuzzer long-pass census | `../../../task/d3-sql-fuzzer-ledger.md` |
| Read write-bench findings (CTAS) | `../../../task/write-bench-report-2026-08-06.md` |
| Read write-bench r22 extension | `../../../task/write-bench-report-r22-extension.md` |
| Read perf findings | `../../../task/perf-report-2026-07-29.md` |

## Debug

| Symptom | Check |
|---|---|
| Extremely slow even at 100k | VALUES path — each action re-runs giant createDataFrame SQL |
| MoR gate / UnsupportedOperationException | TBLPROPERTIES write.merge.mode=merge-on-read on a V2 table |

Combine-review lint pass (2026-07-29): typed `_time`, PySpark-convention `F` noqa, raw regex patterns in the interchange battery.

<!-- Phase-3 PR-4 (V2 port), declared: the `task/…-report-*.md` scoreboards and unit
     ledgers named above are port-source measurement artifacts and were NOT ported —
     they are historical evidence of runs made in the source repository. Re-running a
     bench here writes a fresh report under `task/`. The row text is kept verbatim so
     the invocation recipes stay accurate; only the report files are absent. -->
