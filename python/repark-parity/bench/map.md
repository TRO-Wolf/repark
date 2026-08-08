# map — python/repark-parity/bench

## Purpose

Reproducible **local** performance measurement scripts (R-PERF-MEASURE). No product code;
no AWS. Outputs feed `task/perf-report-*.md`.

## Contents

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
