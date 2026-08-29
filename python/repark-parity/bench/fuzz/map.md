# map — python/repark-parity/bench/fuzz

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

**R-SQL-FUZZER (D3)** — seeded differential correctness fuzzer. Generated queries over
generated data; RePark vs DuckDB oracle; divergences minimized and banked. Infrastructure
is the deliverable; engine product fixes mid-unit are **out of scope** (bank + pin only).

## Contents

- `generator.py` — pure-Python query AST generator (`random.Random(seed)` only).
  **PYC-4:** query-shape records are Pydantic `BaseModel`.
- `datagen.py` — seeded multi-table fixture (int32/int64/float64/decimal/utf8/date/timestamp/bool, NULL ≥10%).
  **PYC-4:** `FuzzTable` / `FuzzDatabase` are `BaseModel`.
- `compare.py` — TPC-H-class compare (ints exact; non-integral Decimals exact; non-integral floats 1e-6 rel; int≠fractional float; ORDER BY → order-sensitive). **TZ-4 PR-2:** tz-aware UTC datetimes normalize to naive UTC walls so DuckDB (naive) does not false-red LTZ export.
  **PYC-4:** comparison rows are `BaseModel`.
- `runner.py` — end-to-end run + census (`REPARK_FUZZ_SEED`, `REPARK_FUZZ_N`);
  statuses OK | WRONG-RESULT | ERROR (no SKIP). **PYC-4:** outcomes are `BaseModel`;
  `_execute_minimize_pair` is module-level.
- `minimizer.py` — greedy shrink (drop LIMIT/ORDER leftmost-first/WHERE/joins/columns/rows;
  scrub ORDER BY when SELECT/GROUP keys drop; clear WHERE on dropped join tables;
  clear LIMIT if ORDER empties). **PYC-4:** shrink records are `BaseModel`;
  `still_diverges` stays a nested-def pragma (callback case).
- `bank.py` — write `repros/<seed>-<n>.sql` (no overwrite; `next_bank_sequence` continues);
  corpus index scans full on-disk dir; header includes `has_order_by`;
  `load_minimized_database` for pin replay. **PYC-4:** `BankedRepro` is `BaseModel`;
  `_flush_parsed_table` is module-level.
- `run_fuzz.py` / `__main__.py` — CLI entry (path bootstrap for script invocation).
- `repros/` — banked minimal repros (may be empty — see [repros/map.md](repros/map.md)).
- `__init__.py` — package exports.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run smoke (200q, seed 42) | `python python/repark-parity/bench/fuzz/run_fuzz.py` |
| Long pass (≥5000) | `REPARK_FUZZ_N=5000 python …/run_fuzz.py --out /tmp/fuzz-long.json` |
| CI smoke pin | `python/repark/tests/test_fuzz_smoke.py` *(facade path — arrives with the facade package in the phase-3 facade PR)* |
| Read long-pass census | `../../../../task/d3-sql-fuzzer-ledger.md` |
| Understand exclusions | **EXCLUSIONS** section below |

## Determinism (HARD)

- Seed: CLI `--seed` / env `REPARK_FUZZ_SEED` / test default **fixed literal `42`**.
- Seed must be **≥ 0** (bank filenames `<seed>-<n>.sql`; negative rejected at resolve).
- Same seed → byte-identical query SQL set (unit-pinned).
- Seed recorded in every banked repro header.
- **No time-based seeding anywhere.**

## Generator scope v1

In: SELECT projections/arithmetic/CASE/casts; WHERE nested boolean + scalar subqueries;
GROUP BY + COUNT/SUM/AVG/MIN/MAX; ORDER BY NULLS FIRST/LAST; LIMIT; INNER/LEFT joins ≤3 tables;
aggregate ORDER/LIMIT always includes ``MIN(row_id) AS ord_tie`` total-order key.

Out (v2 seeds): window frames, set ops, laterals.

## EXCLUSIONS (generator-side — each has a reason)

| Exclusion | Reason |
|---|---|
| No `SUM`/`AVG` on `float64` columns | Float aggregation order / non-associativity is a known divergence class already pinned elsewhere; integral + decimal aggregates only. |
| No `CAST(float AS INT)` | DuckDB rounds half-up; Spark/DataFusion truncates toward zero — dialect, not a RePark bug. |
| No NaN / Inf float values | NaN ordering and equality differ across engines; data generator emits finite floats only. |
| No bare integer column divisors | Avoid engine-specific div-by-zero error shapes; division uses non-zero literal divisors + `CAST(… AS DOUBLE)`. |
| No `LIMIT` without `ORDER BY` | Without ORDER BY, LIMIT is engine-non-deterministic (which rows survive); generator always pairs LIMIT with ORDER BY. |
| ORDER BY always ends with `row_id` (+ join partners) | Non-unique ORDER BY keys + LIMIT still non-deterministic under ties; every table has a unique non-null `row_id` used as final ASC tiebreaker; joins append every partner `row_id`. |
| Aggregate ORDER BY/LIMIT ends with `MIN(row_id) AS ord_tie` | After GROUP BY, aliases alone can still tie; `ord_tie` is the total-order key for LIMIT survivors (octo C1-L-001). |
| ORDER BY always names NULLS FIRST/LAST | Spark vs DuckDB disagree on default NULLS placement for ASC/DESC; omitting NULLS is a false-positive factory. |
| No CAST(decimal/timestamp/bool/float → VARCHAR) | Engine-specific string formats (decimal padding, `T` vs space in timestamps, bool spelling). INT→VARCHAR and date→VARCHAR kept (ISO-stable). |
| Window / set ops / laterals | Out of scope v1 (charter). |

An exclusion without a documented reason is a slate demerit — keep this table current.

## Budget

- Smoke: **200 queries**, seed **42**, always-on, must run **<60s**.
- Long run: env-gated `REPARK_FUZZ_N=…`; unit records a ≥5000-query pass in the ledger.

## Debug

- ruff format lockstep (octo C8).


| Symptom | Check |
|---|---|
| Non-deterministic SQL | ensure no `random.random()` / `time` / `uuid` — only `random.Random(seed)` |
| Smoke >60s | row counts in `datagen.DEFAULT_ROWS_PER_TABLE`; avoid per-query session open in smoke path |
| Mass ERROR on joins | both engines see same Arrow-registered fixture? |
| WRONG-RESULT on CAST | confirm exclusion table — do not "fix" engine mid-D3 |
| Empty repros/ | valid — say so in ledger; do not pad |

## Constraints

- Never AWS / `Cargo.toml [patch]` / `.github/`.
- Never `--all-features`.
- Engine fixes out of scope — bank + xfail pin only.

- 2026-08-01: CLI bootstrap repaired — package-absolute imports (`bench.fuzz.*`); the fuzz
  dir itself must never be the import root (bank/runner are package-relative).
  (CLI shim keeps zero path manipulation — imports only.)

<!-- Phase-3 PR-4 (V2 port), declared: the `task/…-report-*.md` scoreboards and unit
     ledgers named above are port-source measurement artifacts and were NOT ported —
     they are historical evidence of runs made in the source repository. Re-running a
     bench here writes a fresh report under `task/`. The row text is kept verbatim so
     the invocation recipes stay accurate; only the report files are absent. -->
