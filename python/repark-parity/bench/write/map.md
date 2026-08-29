# map — python/repark-parity/bench/write

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

**R-WRITE-BENCH (W1)** measurement-only harness: large **append + CTAS**, plus r22
extension **MERGE** (1M/10M × narrow/wide × K) and **INSERT OVERWRITE peak RSS**
(OTH-004) into a **local-fs** memory-catalog Iceberg warehouse. Sweeps
`repark.write.max-concurrent-files` (K) × `write.target-file-size-bytes` on CTAS;
MERGE pins file-size and sweeps K; OVERWRITE pins K (collect-bound). No engine,
fork, or knob-default changes. Outputs feed `task/write-bench-report-*.md`.

## Contents

- `datagen.py` — re-export TPC-H parquet ensure (`bench/tpch/datagen.py`); source table =
  `lineitem` (largest SF1 fact) for CTAS mode.
- `schemas.py` — synthetic narrow/wide parquet generators + MERGE source-plan / row
  integrity helpers (1M/10M axes; no TPC-H dependency).
- `runner.py` — CTAS+append matrix: per-cell session with K, CTAS + INSERT append, wall +
  per-stage timings + peak RSS (`resource.ru_maxrss`), warehouse bytes / data-file count;
  row-count integrity (2× source after CTAS+append); release-build assertion probe;
  markdown + JSON report; **stall-or-not verdict** per the R-WRITE-BENCH seed decision tree
  (local-fs disclosure: upload-latency conclusions bounded — no AWS; K proven on CTAS path).
  **PYC-4:** matrix records are Pydantic `BaseModel`. `StageTiming` keeps a
  positional `__init__(name, seconds)` because production and unit tests construct
  that way.
- `merge_runner.py` — MERGE matrix (MoR + optional COW): rows × width × K; rule-10 knob
  pins (`spark.sql.shuffle.partitions=8`, target-file-size 256 MiB). **PYC-4:** cell/board
  records are `BaseModel`.
- `overwrite_runner.py` — INSERT OVERWRITE peak RSS vs source size (OTH-004 MemTable
  materialize path); rows × width; K pinned (not swept). **PYC-4:** cell/board records
  are `BaseModel`.
- `run_write_bench.py` — CLI entry (`--mode ctas|merge|overwrite|extension|all`, `--sf`,
  `--k`, `--file-sizes`, `--rows`, `--width`, `--report`, `--out`, `--warehouse`,
  `--repeats`, `--assert-release`, `--no-cow`).
- `__init__.py` — package exports for unit tests.
- `__main__.py` — package entry shim → `run_write_bench.main`.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run SF1 full K×file-size CTAS matrix | `maturin develop --release` then `python …/run_write_bench.py --mode ctas --sf 1 --assert-release --report task/write-bench-report-….md` |
| r22 MERGE + OVERWRITE extension | `…/run_write_bench.py --mode extension --assert-release --report task/write-bench-report-r22-extension.md` |
| MERGE only 1M/10M × K | `…/run_write_bench.py --mode merge --rows 1000000,10000000 --k 1,2,4,8 --assert-release` |
| OVERWRITE RSS only (OTH-004) | `…/run_write_bench.py --mode overwrite --rows 1000000,10000000 --assert-release` |
| Tiny smoke (SF0.01, K=1,4) | `…/run_write_bench.py --mode ctas --sf 0.01 --k 1,4 --file-sizes 64MiB` |
| Tiny MERGE smoke | `…/run_write_bench.py --mode merge --rows 5000 --k 1,2 --no-cow --width narrow` |
| Read human report (prior CTAS) | `../../../../task/write-bench-report-2026-08-06.md` |
| Read r22 extension report | `../../../../task/write-bench-report-r22-extension.md` |
| Unit pins (no SF1/1M wall) | `python/repark/tests/test_write_bench_unit.py` *(facade path — arrives with the facade package in the phase-3 facade PR)* |

## Debug

| Symptom | Check |
|---|---|
| Debug-wheel trap (walls absurdly high) | **must** `maturin develop --release` first; then `--assert-release` or `REPARK_WRITE_BENCH_RELEASE=1` so the report does not mark release UNVERIFIED |
| Report says release disclosed **False** | operator forgot assertion flag/env after maturin --release |
| `ModuleNotFoundError: duckdb` | root `dev` group; `uv sync --group dev --no-install-workspace` (CTAS/TPC-H only) |
| `ModuleNotFoundError: polars` | needed for synthetic MERGE/OW **seed writes**; install polars (`repark[polars]` / bench env). Width/rows validated **before** import so unit pins reject bad inputs without polars; write I/O pin uses `pytest.importorskip("polars")` |
| CTAS fails on empty warehouse | memory catalog + local warehouse path; never AWS envs |
| K has no effect on local FS CTAS | expected disclosure — encode-bound on CTAS; S3 upload overlap is follow-up |
| Append wall ignores K | expected — INSERT INTO is fork TableProvider passthrough; verdict uses CTAS |
| MERGE K flat on local FS | encode/join bound; does not prove S3 stall; prior CTAS NO_K may still stand |
| OVERWRITE RSS delta ~0 | process-lifetime `ru_maxrss` already high from prior cells; compare peak across 1M→10M or run overwrite-only |
| `row_count_mismatch` cell error | CTAS+append must yield 2× source; MERGE expected = \|target ∪ source\|; OW rows = source |
| wall_total excludes session build | intentional — stages only; K compare uses CTAS / merge_mor |
| RSS rises every cell | process-lifetime `ru_maxrss`; not independent per-cell samples |
| K sticky across cells | each cell `getOrCreate` after `stop()`; if stop fails (logged warning) reuse may ignore new K — inspect cell logs |
| Symlink cache refused | private `$XDG_CACHE_HOME/repark-tpch` (same as TPC-H harness) |
| Accidentally used target_partitions=128 | rule 10: extension pins `spark.sql.shuffle.partitions=8` (never T2 OOM pin value) |

## Constraints

- Measurement only — **no** product / fork / knob-default edits.
- Never commit warehouses or parquet cache.
- Never touch AWS / `REPARK_*` acceptance / `TABLE_BUCKET_ARN` / `.github/` / `Cargo.toml [patch]`.
- Local-fs stands in for S3; every report must disclose that upload-latency conclusions are bounded.
- Prior CTAS `NO_K_BENEFIT_ON_LOCAL_FS` stands unless a new CTAS matrix contradicts loudly.

<!-- Phase-3 PR-4 (V2 port), declared: the `task/…-report-*.md` scoreboards and unit
     ledgers named above are port-source measurement artifacts and were NOT ported —
     they are historical evidence of runs made in the source repository. Re-running a
     bench here writes a fresh report under `task/`. The row text is kept verbatim so
     the invocation recipes stay accurate; only the report files are absent. -->
## SQP-1 (cycle-2)

The write runners embed the `CREATE NAMESPACE … LOCATION '<path>'` path through
`sql_string_literal`, not a hand-rolled quote-double — a no-op on backslash-free Linux paths, kept in the one home.
