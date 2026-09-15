# Charter ledger — FNP-11B · datetime format parsing, the TIME family, BL-13 and BL-14

**Date:** 2026-09-15 · **Branch:** `feat/fnp-11b-temporal-formats` · **Base:** `origin/main`
`bee2cde3` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `EX-FN-20`, `EX-FN-21`, `EX-FN-28`, `BL-13`, `BL-14` stay BACKLOG in
step 1; flips land in step 7 with pins.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Thirteen Spark temporal names refuse a format argument, answer where
Spark refuses, or are absent on the facade while live PySpark 4.1.2 answers them:
`to_date` / `to_timestamp` / `unix_timestamp` with Java datetime patterns,
`to_timestamp_ltz` / `to_timestamp_ntz`, `try_to_timestamp`, the TIME family
(`make_time`, `to_time`, `time_diff`, `time_trunc`, `current_time`), the
`to_char` / `to_varchar` / `to_number` / `to_binary` formatting family,
the all-optional `make_timestamp(date=, time=)` keyword form, `try_avg(INTERVAL)`
(BL-13) and `DATE +` sub-day `INTERVAL` (BL-14). The orchestrator recorded the
live oracle on 2026-09-14; this unit implements the names and pins both doors
against it. Run 16a step 1 is ledger, fixture filter and red-first pins only.

**Not in this unit:** any product code (steps 2–6 own the kernels, the facade
wrappers, the registry flips and the census-pin updates); re-recording the
oracle; edits to `dataframe/**`, `column.py`, `session/**`, `catalog.py`,
`types.py`, the SQL parser/dialect, `Cargo.toml` or `Cargo.lock` (D-3).

## Orchestrator rulings (G-2), copied from the card

- D-1 One Java-pattern parser in Rust (`crates/repark-functions/src/`), shared by
  `to_date` / `to_timestamp` / `unix_timestamp` / `to_timestamp_ltz` /
  `to_timestamp_ntz` / `try_to_timestamp`; reuse the existing
  `CANNOT_PARSE_TIMESTAMP` precedent in `timestamp_cast.rs` and the
  `try_to_date(format)` kernel if it already parses Java patterns. Registration
  in `register_all`; ADD arms only in
  `crates/repark-python/src/column/function_dispatch*`.
- D-2 Frozen signatures: `to_date`, `to_timestamp`, `unix_timestamp` are frozen
  1.0 names — keep every existing required parameter required
  (`build_api_freeze.py` reads source with `ast`; widening a required parameter
  trips rule J1). Adding an optional parameter is allowed.
- D-3 Never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`,
  `types.py`, the SQL parser/dialect, `Cargo.toml` or `Cargo.lock`.
- D-4 Census pins: implementing absent or stubbed names reds
  `test_functions_d.py` (deferred tuple: `to_timestamp_ltz`,
  `to_timestamp_ntz`), `test_fn_batch*.py` stub refusals,
  `test_functions_split_identity.py` (install tail and total length), the EX-0
  count and the example-coverage walk — update them in the unit, with an example
  under `docs/examples/functions/`.
- D-5 Registry: flip `EX-FN-20` (try_to_timestamp), `EX-FN-21` (unix_timestamp
  format), `BL-13`, `BL-14` (or file the planner seam) to FIXED with pins; add a
  section-7 row per residual.
- D-6 Pins: `python/repark/tests/test_fnp11b_temporal_formats.py`, both doors,
  both ANSI settings, both zones where the oracle has them, red first on the
  base tree with the red summary in the ledger; Rust unit tests beside each
  kernel. Cast each input column ONCE per batch (never a per-row whole-array
  `cast`), parse constant patterns and zones once per batch.
- D-7 Scope from the run-16a order adds `to_char`, `to_varchar`, `to_number`,
  `to_binary` (moved here from FNP-MATH-1; oracle cells in
  `/tmp/oc-worker/pa-math/o245_spark_oracle.json`, copy the cells by name into
  this unit's fixture) — share the existing `try_to_number` / `try_to_binary`
  kernels; `to_char` must stop resolving to DataFusion's implementation.
- D-8 (owner ruling Q-15a-3, 2026-09-15) Widen `make_timestamp` to PySpark
  4.1.2's all-optional signature `(years=None, months=None, days=None,
  hours=None, mins=None, secs=None, timezone=None, date=None, time=None)` in
  this PR as a freeze update: regenerate the API-freeze register with
  `scripts/build_api_freeze.py`, flip registry row EX-FN-28 to FIXED with pins
  for the `(date=, time=)` keyword form on the Python door, and name Q-15a-3 in
  the ledger.
- D-9 The TIME family follows Spark's per-function default (run 15a ruling 1):
  `current_time` answers; `make_time`, `to_time`, `time_diff`, `time_trunc`
  raise `[UNSUPPORTED_TIME_TYPE]` on both doors as dated refusals with registry
  rows.
- D-10 BL-14 is a planner seam if the interval unit is lost before the kernel:
  HALT is not required — pin the Python door where reachable, leave BL-14 OPEN
  with the exact seam named for run 16c, and continue.

## PROPOSITION LEDGER — FNP-11B — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every card name is present on `repark.spark.functions` with PySpark 4.1.2's parameter names, order and defaults (frozen names unchanged per D-2; `make_timestamp` all-optional per D-8). | `test_fnp11b_temporal_formats.py::test_facade_signatures_match_spark`, red on the base. | OPEN | Red 2026-09-15: ten names absent (`to_timestamp_ltz`, `to_timestamp_ntz`, `make_time`, `to_time`, `time_diff`, `time_trunc`, `current_time`, `to_char`, `to_varchar`, `to_number`, `to_binary` — `AttributeError`); `unix_timestamp` default and `make_timestamp` arity mismatch the recording. |
| C-002 | The Python-door fixture cells answer Spark-equal (values, types, nullability) on both ANSI settings and both zones. | `test_fnp11b_temporal_formats.py` python-door cells, red on the base. | OPEN | Red 2026-09-15: every Python-door format cell fails; 12 pass (8 no-format `unix_timestamp`/`to_timestamp` casts already answer, 4 `current_time` answers). §1. |
| C-003 | The SQL-door fixture cells answer Spark-equal on both ANSI settings and both zones. | `test_fnp11b_temporal_formats.py` sql-door cells, red on the base. | OPEN | Red 2026-09-15: every SQL-door format cell fails; 16 pass (14 no-format casts, `INTERVAL 0 DAY` staying date, bare `current_time` answers). §1. |
| C-004 | Error cells raise Spark's condition in brackets with Spark's message prefix on both doors under both ANSI settings. | The error-cell legs of the door tests. | OPEN | Red 2026-09-15: base raises the wrong condition (`Invalid function`, arity, `[FNP-11]`, `TypeError`) or answers where Spark raises (`to_time`, `hour(current_time())`). §1. |
| C-005 | TIME-family refusals are Spark-equal: `make_time`, `to_time`, `time_diff`, `time_trunc` and `hour(<TIME>)` raise `[UNSUPPORTED_TIME_TYPE]`; `current_time` answers `time(6)` with `current_time(7)` raising `[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]`. | The TIME-family cells plus the `typeof` translation legs. | OPEN | Red 2026-09-15: `make_time` answers `time32`, `to_time` answers `time64`, `hour(current_time())` answers `True`, `current_time(0/3)` refuse on arity, `current_time(7)` refuses on arity not range. §1. |
| C-006 | BL-13 `try_avg(INTERVAL)` answers Spark's average interval (DAY-TIME and YEAR-MONTH) with `[INTERVAL_ARITHMETIC_OVERFLOW]` on overflow; BL-14 promotes `DATE +` sub-day `INTERVAL` to `timestamp` (or names the run-16c planner seam per D-10). | The `try_avg`/`avg`/`date_plus_interval` cells. | OPEN | Red 2026-09-15: `try_avg(INTERVAL)` refuses `[FNP-11]` (10 cells), `avg(INTERVAL)` refuses coercion (2 cells), `DATE + INTERVAL 0/1 HOUR/MINUTE` stays `date` (10 cells). §1. |
| C-007 | No regression and census pins: the touched suites stay green; `test_functions_d.py`, `test_fn_batch3.py`, `test_functions_split_identity.py`, the EX-0 count and the example-coverage walk move with the new names plus a `docs/examples/functions/` example. | The gate runs in step 7. | OPEN | Step 7 work; step 1 touches none of those files. |
| C-008 | Registry rows `EX-FN-20`/`EX-FN-21`/`EX-FN-28`/`BL-13`/`BL-14` flip to FIXED with the new pin paths, one row per residual divergence is appended in §7, and every touched `map.md` is in lockstep. | `docs/spark-sql-iceberg-parity.md` diff; `make check-map-sync`. | OPEN | Step 7 work; step 1 adds only the ledger, fixture and pin rows. |

VERDICT: 8 clauses, 0 PROVEN, 8 OPEN, 0 REJECTED.

## 1. Red-first record (base `bee2cde3`, before any kernel edit)

`PYTHONPATH=python/repark/src .venv/bin/python -m pytest
python/repark/tests/test_fnp11b_temporal_formats.py -q` on the base tree:
**310 failed, 28 passed in 15.93s** — the signature pin plus 309 door cells red.
Per-name failures (parametrized cells): `to_date` 22, `to_timestamp` 30,
`to_timestamp_ltz` 28, `to_timestamp_ntz` 28, `unix_timestamp` 20,
`try_to_timestamp` 30, `make_time` 18, `to_time` 20, `time_diff` 20,
`time_trunc` 18, `current_time` 12, `make_timestamp` (date= keyword) 19,
`try_avg` 10, `avg` 2, `date_plus_interval` 10, `to_char` 6, `to_varchar` 6,
`to_number` 4, `to_binary` 6. Sampled failure modes: Python-door cells fail
with `AttributeError` (ten names absent) or the `try_to_timestamp` stub refusal
(`functions.try_to_timestamp is not supported yet`) or `TypeError` (the
`make_timestamp(date=, time=)` keywords); SQL-door cells fail with `Invalid
function` (`to_timestamp_ltz`, `to_timestamp_ntz`, `to_number`, `to_binary`,
`time_diff`, `time_trunc`), arity refusals (`to_date` with two arguments,
`unix_timestamp` with two arguments, `current_time(0/3/7)`), the `[FNP-11]`
deferral (`try_avg(INTERVAL)`), the `avg` interval coercion refusal, or a wrong
answer where Spark raises (`to_time` answers `time64`, `make_time` answers
`time32`, `hour(current_time())` answers `True`; `DATE + INTERVAL 0 HOUR`
stays `date32`).

The 28 passes are the already-working surface, kept as controls: 14
no-format `to_timestamp` casts (`ts_str`, `dt`, the `+02:00` literal, both
doors), 8 no-format `unix_timestamp(ts_str)` (both doors), `DATE +
INTERVAL 0 DAY` staying `date` (2 cells), and 4 bare-`current_time` answers
(the two `typeof` translation legs, `IS NOT NULL`, the bare value cell).

## 2. Work log

- 2026-09-15 step 1: ledger created; fixture filtered to
  `python/repark/tests/fnp11b_spark_oracle.json` (315 pa-11b cells with
  `time_type_enabled == spark_default` over the card names plus the
  `make_timestamp` Python `date=` cells, and the 22 `to_char`-family cells from
  pa-math); red pins in
  `python/repark/tests/test_fnp11b_temporal_formats.py` (337 parametrized
  cells plus the facade-signature pin). SQL `typeof(current_time…)` cells run
  translated to Arrow time64 assertions (no `typeof` on the SQL door, the
  FNP-11A R2 precedent); Python `tm`-column value cells run on the TIME frame,
  `UNRESOLVED_COLUMN` error cells on the plain frame.
