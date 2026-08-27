- **octo C2:** ruff E501/SIM105 clean on V3 harness.

# map — python/repark-parity/bench/tpch

## Purpose

TPC-H scoreboard harness (**R-TPCH-HARNESS** / V1 + **R-TPCH-V3** / W1 + **B1 R-SAIL-BENCH**).
DuckDB `dbgen` → parquet cache → repark `spark.sql` over temp views (parquet **or** local
memory-catalog Iceberg) vs DuckDB same parquet. Optional third leg: **Sail** via Spark Connect
loopback (`--engine sail|both`). Measurement-first; no AWS; no product-engine fixes here.
Sail is prior-art only — never a RePark product dependency (no pysail in uv.lock/pyproject).

## Contents

- **B1 R-SAIL-BENCH:** `--engine repark|sail|both`; 120s + one 300s TIMEOUT retry (Slow vs hung);
  three-way merge (`merge_three_way`); `sail_engine.py` (SparkConnectServer + remote session);
  CLI `--timeout-retry`, `--sail-python` / `REPARK_SAIL_PYTHON`; unit pins without pysail in CI.
- **octo B1-C1:** sail unavailable → `skipped=True`; `_run_sail_scoreboard` only falls through on
  `SailUnavailableError` (no double-run); compare `subject_label` for Sail WRONG messages;
  `_subprocess_run_kill_group` (start_new_session + killpg) for Sail board + query workers;
  SF10 sail hard wall uses 300s default; open-failure stops spark then server; merge keeps Sail
  timeout metadata + dedupes gRPC boilerplate findings.
- **octo B1-C2:** three-way per-engine census includes DIED; skipped empty Sail board →
  `SailBoardSkipped` (not per-query ERROR); Sail subject uses `original_sql` (not repark
  dialect rewrite); status_ledger keeps repark/sail status; `--sail-python` requires X_OK.
- **octo B1-C3:** CLI `--queries` non-int / empty → usage exit 2; Sail board subprocess
  malformed JSON / bad rows → skipped FINDING (no crash).
- **octo B1-C4:** `query_result_from_dict` coerces unknown status → ERROR/InvalidStatus;
  `worse_status` / exit_code treat unknown as ERROR (no green-exit from hostile Sail JSON).
- **octo B1-C5:** Sail board hard wall scales with `--queries` filter size; gRPC/connect
  errors classify as `SailGrpc` / `SailConnect`.
- **octo B1-C6:** three-way disclosure claims gRPC cost only when Sail actually ran
  (not when the Sail board was fully skipped); merge Findings match.
- **octo B1-C7:** default `engine=repark` unit-pinned (sail_engine not imported at load).
- **octo C1:** DIED exit outranks TIMEOUT; worker writes structured ERROR on exception.

- **V3 / W1:** SF10 disk gate (<30 GiB free → SKIP FINDING); SF10 default timeout 300s;
  subprocess-per-query isolation → **DIED** on OOM/signal; RSS peaks recorded (no auto-abort);
  Iceberg leg (`--storage iceberg`) CTAS 8 tables into local memory catalog; report column
  `iceberg_wall`; CLI `--report-append`, `--isolation`, `--warehouse`, `--min-free-gib`.
- `query_worker.py` — isolated child for one query (JSON config/result).
- `sail_engine.py` — Sail Spark Connect open/register/collect (optional import; bench-venv only).
- **extra-octo E7:** private default cache root + refuse dir-level symlinks.
- **extra-octo E3:** disclosure strings match integral-exact compare.
- **extra-octo E2:** ERROR outranks TIMEOUT after drain; exit_code_for_board unit-pinned.
- **extra-octo E1:** non-empty regular-file cache; continue-on-first-timeout; multi-payload compare; CLI exits 3/4/5.
- **octo C5:** `_timed_call` mutable box keep-result; repark timeout keep-and-compare unit-pinned.
- `datagen.py` — `ensure_parquet_sf(sf)` → `$XDG_CACHE_HOME/repark-tpch/sf{N}/{table}.parquet` (private; not sticky /tmp) (8 tables).
- `queries.py` — load 22 texts from DuckDB `tpch_queries()` + optional dialect rewrite table.
  **PYC-4:** `TpchQuery` is a Pydantic `BaseModel`.
- `compare.py` — sorted-row compare; ints/integral exact; non-integral floats `1e-6` relative.
  **PYC-4:** comparison rows are `BaseModel`.
- `runner.py` — scoreboard matrix (OK / WRONG-RESULT / ERROR / TIMEOUT / DIED), gap census, MD report;
  three-way repark/Sail/DuckDB walls when `engine=both`. **PYC-4:** scoreboard records are
  `BaseModel`; `_alarm_handler` stays a nested-def pragma (SIGALRM callback).
- `run_tpch.py` — CLI entry (`--sf`, `--report`, `--report-append`, `--ledger`, `--repeats`,
  `--timeout`, `--timeout-retry`, `--storage`, `--isolation`, `--engine`, `--sail-python`).
- `sf1_status_ledger.json` — frozen SF1 status map consumed by `test_tpch_smoke.py` pins.
  **Z-3 U1 (2026-08-13):** Q1 flipped OK → WRONG-RESULT — Spark-typed `avg(l_discount)`
  (`decimal128(19,6)`) no longer matches DuckDB float at 1e-6 relative.
  **W-2 U2 (2026-08-13):** SF1 DuckDB-diff re-run after `parse_float_as_decimal=true`.
  No additional query moved (21 OK + Q1 WRONG-RESULT). Q1 is still the avg-type
  mismatch, not a new literal effect.
- `baseline-ratios.json` — **r24 G10 / Q15** PROVISIONAL repark/DuckDB wall-ratio ceilings
  (22 queries; seeded from sail-bench-report SF1 `r_ratio`; morning finals = tip × 1.5).
- `check_baseline_ratios.py` — compare a scoreboard JSON against `baseline-ratios.json`
  (fail-closed: empty scoreboard, zero ceiling checks, missing baseline query nrs, or
  null/non-finite OK ratios → exit 1)
  (exit 1 on ratio exceed / non-OK status). Unit pins in `python/repark/tests/test_tpch_compare_unit.py`
  (facade path — arrives with the facade package in the phase-3 facade PR).
- `__init__.py` — package exports for tests importing the harness helpers.
- `__main__.py` — package entry shim (delegates to `run_tpch.main`).
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run SF1 scoreboard | `python python/repark-parity/bench/tpch/run_tpch.py --sf 1 --report task/tpch-report-2026-07-31.md` |
| Run SF10 (V3) | `…/run_tpch.py --sf 10 --repeats 1 --report-append task/tpch-report-2026-07-31.md` |
| Run Iceberg leg (V3) | `…/run_tpch.py --sf 1 --storage iceberg --report-append task/tpch-report-2026-07-31.md` |
| Run Sail leg (B1) | `sail-venv/bin/python …/run_tpch.py --sf 1 --engine sail --report task/sail-bench-report-….md` |
| Three-way repark+Sail+DuckDB | `…/run_tpch.py --sf 1 --engine both --sail-python /path/to/sail-venv/bin/python` |
| Tiny smoke datagen | `ensure_parquet_sf(0.01)` or CLI `--sf 0.01` |
| Read the human scoreboard | `../../../../task/tpch-report-2026-07-31.md` |
| CI pin battery | `python/repark/tests/test_tpch_smoke.py` + V3/B1 unit pins in `test_tpch_compare_unit.py` *(facade path — arrives with the facade package in the phase-3 facade PR)* |
| Ratio regression gate (schedule) | `baseline-ratios.json` + `check_baseline_ratios.py` + a scheduled TPC-H workflow *(no workflow is wired in this repository yet — the tier-2 CI PR owns that decision)* |
| Add a dialect-only rewrite | `queries.py` → `DIALECT_REWRITES` (must disclose in report) |

## Debug

| Symptom | Check |
|---|---|
| `ModuleNotFoundError: duckdb` | root `dev` group; `uv sync`; pins use `importorskip` |
| `INSTALL tpch` fails (offline) | network once → `~/.duckdb` cache; tests skip with named reason |
| WRONG-RESULT on aggregates | ints/integral exact; non-integral floats 1e-6 relative |
| Timeout mis-classifies finished query | SIGALRM mutable-box keep-result; mid-repeat falls to compare |
| Exception after successful collect | keep prior rows and compare (WRONG-RESULT outranks ERROR/TIMEOUT) |
| repark ERROR on interval/EXTRACT | gap census seed — do not massage results |
| Parquet missing after dbgen | free space; `TABLES` list in `datagen.py` |
| SF10 skipped | free disk < 30 GiB on cache filesystem — FINDING, not a bug |
| DIED on SF10 query | child signal/OOM; scoreboard continues; exit 6 if only DIED |
| Iceberg CTAS fails | local warehouse path; memory catalog config; never AWS |
| Sail unavailable | install pysail+pyspark-client in **separate** bench venv; pass `--sail-python` |
| Sail TIMEOUT vs Slow | TIMEOUT + `error_class=Slow` completed on 300s retry; hung failed both |
| Three-way empty Sail | check `findings` for board_error / skipped; REPARK_SAIL_PYTHON path |

## Constraints

- Never commit cache dirs (`~/.cache/repark-tpch/**` or custom `--data-root` / warehouses).
- Never touch AWS / `REPARK_*` acceptance envs / `.github/` / `Cargo.toml [patch]`.
- **Never** add `pysail` / `pyspark-client` to repo `pyproject.toml` / `uv.lock` (B1 hard line).
- Oracle = DuckDB; WRONG-RESULT is worse than ERROR — never weaken compare to get green.
- V3/B1 measurement only — engine fixes are out of scope for this harness unit.
- Sail is measurement prior-art only; not a RePark product dependency.

<!-- 2026-08-04 (r24 combine rider): ruff format pass on check_baseline_ratios.py (no logic change). -->

<!-- Phase-3 PR-4 (V2 port), declared: the `task/…-report-*.md` scoreboards and unit
     ledgers named above are port-source measurement artifacts and were NOT ported —
     they are historical evidence of runs made in the source repository. Re-running a
     bench here writes a fresh report under `task/`. The row text is kept verbatim so
     the invocation recipes stay accurate; only the report files are absent. -->
## SQP-1 (cycle-2)

`datagen.py` (DuckDB `dbgen` COPY) and `runner.py` (`read_parquet` views) embed export paths through
`repark_parity.sql.escape_sql_single_quotes` keeps quote-only path escaping inside the standalone
parity package; the runner and datagen do not import the RePark product package at module load.
PR-245 revalidation keeps `runner.py` smaller while preserving that helper boundary.
