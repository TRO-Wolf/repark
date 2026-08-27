# map — python/repark-parity/bench/tpcds

## Purpose

TPC-DS scoreboard harness (**R-TPCDS-HARNESS** / D1). DuckDB `dsdgen` → parquet cache →
repark `spark.sql` over temp views (parquet only) vs DuckDB same parquet. Measurement-first;
no AWS; no product-engine fixes here. Mirrors `../tpch/` shape.

## Contents

- `datagen.py` — `ensure_parquet_sf(sf)` → `$XDG_CACHE_HOME/repark-tpcds/sf{N}/{table}.parquet`
  (private; not sticky /tmp) (24 TPC-DS tables via DuckDB `dsdgen`).
- `queries.py` — load 99 texts from DuckDB `tpcds_queries()` + optional dialect rewrite table;
  ORDER BY detection for ordered compare. Provenance = DuckDB extension (not TPC-spec text).
  **PYC-4:** `TpcdsQuery` is a Pydantic `BaseModel`.
- `compare.py` — sorted-row multiset **or** ordered compare; ints/integral exact; non-integral
  floats `1e-6` relative. **PYC-4:** comparison rows are `BaseModel`.
- `runner.py` — scoreboard matrix (OK / WRONG-RESULT / ERROR / TIMEOUT / DIED), 120s + one
  300s retry (Slow vs hung), SF1 disk/OOM gate → SKIP FINDING, gap census, MD report.
  **PYC-4:** scoreboard records are `BaseModel`; `_alarm_handler` stays a nested-def pragma.
  `QueryResult.status` is `str` (not Literal) so unknown labels still construct — the
  ledger/exit-code gates refuse a green board.
- `run_tpcds.py` — CLI entry (`--sf`, `--report`, `--ledger`, `--repeats`, `--timeout`,
  `--timeout-retry`, `--queries`, `--isolation`).
- `query_worker.py` — isolated child for one query (JSON config/result; optional subprocess).
- `sf1_status_ledger.json` — SF1 status map consumed by `test_tpcds_smoke.py` pins
  (D2 flipped Q5/Q80/Q84 ERROR→OK after `SparkConcat` Utf8 shim; **evidence scale SF0.01**
  DuckDB differential + curated smoke — full SF1 board not re-run for the flip; notes pin
  in `test_sf1_ledger_d2_notes_disclose_sf001_evidence`. Q58/Q59 WRONG-RESULT + Q72 DIED
  remain seeds).
- `__init__.py` — package exports for tests importing the harness helpers.
- `__main__.py` — package entry shim (delegates to `run_tpcds.main`).
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run SF1 scoreboard | `python python/repark-parity/bench/tpcds/run_tpcds.py --sf 1 --report task/tpcds-report-2026-07-31.md` |
| Tiny smoke datagen | `ensure_parquet_sf(0.01)` or CLI `--sf 0.01` |
| Read the human scoreboard | `../../../../task/tpcds-report-2026-07-31.md` |
| CI pin battery | `python/repark/tests/test_tpcds_smoke.py` + unit pins in `test_tpcds_compare_unit.py` *(facade path — arrives with the facade package in the phase-3 facade PR)* |
| Add a dialect-only rewrite | `queries.py` → `DIALECT_REWRITES` (must disclose in report) |

## Debug

- Argument validation in `run_scoreboard` (empty `query_filter`, bad scale factor) fires
  BEFORE `ensure_parquet_sf` — validation must never depend on cache state or duckdb being
  installed (CI wheel-smoke regression, 2026-08-01: cold cache turned a ValueError test into
  ModuleNotFoundError).

| Symptom | Check |
|---|---|
| `ModuleNotFoundError: duckdb` | root `dev` group; `uv sync`; pins use `importorskip` |
| `INSTALL tpcds` fails (offline) | network once → `~/.duckdb` cache; tests skip with named reason |
| WRONG-RESULT on aggregates | ints/integral exact; non-integral floats 1e-6 relative |
| Timeout mis-classifies finished query | SIGALRM mutable-box keep-result; mid-repeat falls to compare |
| Slow vs hung | TIMEOUT + error_class `Slow` completed on 300s retry; hung failed both |
| repark ERROR on ROLLUP/CUBE/CTE | gap census seed — do not massage results |
| Parquet missing after dsdgen | free space; `TABLES` list in `datagen.py` (24 tables) |
| SF1 skipped | free disk < 5 GiB **or** datagen OOM — FINDING, not a bug |
| DIED on subprocess query | child signal/OOM; scoreboard continues; exit 6 if only DIED |
| DIED with WorkerTimeout too eager | hard wall must be greylight budget: `(timeout*repeats+retry)*2+setup` (`subprocess_hard_timeout_s`); old min-clamp 570s mislabeled Slow as DIED |
| Gap census says EXCEPT wrongly | `classify_error`: bare `except` is word-bounded so `PySparkException` ≠ EXCEPT; pin in `test_tpcds_compare_unit` |
| CLI exit 0 with garbage status | `KNOWN_STATUSES` + `exit_code_for_board` / `query_result_from_dict` / `status_ledger` reject unknown labels |
| Ordered compare false positive | `sql_has_order_by` strips comments/string literals before `\border\s+by\b` |
| Partial ledger overwrites SF1 pin | CLI `--ledger` without `--queries` requires 99 rows (`status_ledger(expect_query_count=99)`) |
| t300 report ambiguous | Slow → `t300_wall=`; hung → `t300_budget=` |
| `--queries` garbage / empty | CLI exit 2; empty `query_filter` raises in `run_scoreboard` |

## Constraints

- Never commit cache dirs (`~/.cache/repark-tpcds/**` or custom `--data-root`).
- Never touch AWS / `REPARK_*` acceptance envs / `.github/` / `Cargo.toml [patch]`.
- Oracle = DuckDB; WRONG-RESULT is worse than ERROR — never weaken compare to get green.
- D1 measurement only — engine fixes are out of scope (D2 is census-driven).
- D1: **parquet only** — no Iceberg leg.

<!-- Phase-3 PR-4 (V2 port), declared: the `task/…-report-*.md` scoreboards and unit
     ledgers named above are port-source measurement artifacts and were NOT ported —
     they are historical evidence of runs made in the source repository. Re-running a
     bench here writes a fresh report under `task/`. The row text is kept verbatim so
     the invocation recipes stay accurate; only the report files are absent. -->
## SQP-1 (cycle-2)

`datagen.py` (DuckDB `dsdgen` COPY) and `runner.py` (`read_parquet` views) embed export paths through
`repark_parity.sql.escape_sql_single_quotes` keeps quote-only path escaping inside the standalone
parity package; the runner and datagen do not import the RePark product package at module load.
PR-245 revalidation keeps `runner.py` line-neutral while preserving that helper boundary.
