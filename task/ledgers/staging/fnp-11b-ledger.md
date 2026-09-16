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
- D-11 (run 16a orchestrator, 2026-09-15): step 2 answers the `unix_timestamp` format
  argument, so the EX-28 refusal pin `test_examples_functions_b.py::test_unix_timestamp_format_refuses`
  went red; the pin is retired and registry row `EX-FN-21` flips to FIXED now (ahead of the
  step-7 flip in C-008) with the unit's oracle cells as its pin.

## PROPOSITION LEDGER — FNP-11B — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every card name is present on `repark.spark.functions` with PySpark 4.1.2's parameter names, order and defaults (frozen names unchanged per D-2; `make_timestamp` all-optional per D-8). | `test_fnp11b_temporal_formats.py::test_facade_signatures_match_spark`, red on the base. | PROVEN | Step-7 gates green; the freeze pin holds the widening. §8. |
| C-002 | The Python-door fixture cells answer Spark-equal (values, types, nullability) on both ANSI settings and both zones. | `test_fnp11b_temporal_formats.py` python-door cells, red on the base. | OPEN | All green except cells 0–4, which are the dated divergence ruled in R-17a-22 (Spark ranks `UNRESOLVED_COLUMN` suggestions by edit distance; engine-wide, filed as ERR-UNRESOLVED-COL-1). Registry row appended; the clause stays OPEN because the proposition as written is not fully met. §8. |
| C-003 | The SQL-door fixture cells answer Spark-equal on both ANSI settings and both zones. | `test_fnp11b_temporal_formats.py` sql-door cells, red on the base. | OPEN | All green except cells 187/188 (the R-17a-16 year-month rendering divergence) and 191/192, 197–200, 203–208 (run 17c's INTERVAL DAY dialect and BL-14 planner seams). Registry rows appended; the clause stays OPEN because the proposition as written is not fully met. §8. |
| C-004 | Error cells raise Spark's condition in brackets with Spark's message prefix on both doors under both ANSI settings. | The error-cell legs of the door tests. | OPEN | All green except cells 0–4 (R-17a-22, ERR-UNRESOLVED-COL-1). Registry row appended; the clause stays OPEN because the proposition as written is not fully met. §8. |
| C-005 | TIME-family refusals are Spark-equal: `make_time`, `to_time`, `time_diff`, `time_trunc` and `hour(<TIME>)` raise `[UNSUPPORTED_TIME_TYPE]`; `current_time` answers `time(6)` with `current_time(7)` raising `[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]`. | The TIME-family cells plus the `typeof` translation legs. | PROVEN | The TIME-family refusals and `current_time` are Spark-equal on both doors, and `test_fnp11b_typeof.py` is 40/40 green; the five blocked pins name seams outside this clause's proposition (17c's `TIMESTAMP_NTZ` literal, the unowned `binary` name). §8. |
| C-006 | BL-13 `try_avg(INTERVAL)` answers Spark's average interval (DAY-TIME and YEAR-MONTH) with `[INTERVAL_ARITHMETIC_OVERFLOW]` on overflow; BL-14 promotes `DATE +` sub-day `INTERVAL` to `timestamp` (or names the run-16c planner seam per D-10). | The `try_avg`/`avg`/`date_plus_interval` cells. | OPEN | Day-time average, try-NULL on overflow, avg-raise and the kernel NULL semantic PROVEN; YEAR-MONTH render, NULL-input SQL parse and BL-14 are ruled residuals for run 17b/17c. §8. |
| C-007 | No regression and census pins: the touched suites stay green; `test_functions_d.py`, `test_fn_batch3.py`, `test_functions_split_identity.py`, the EX-0 count and the example-coverage walk move with the new names plus a `docs/examples/functions/` example. | The gate runs in step 7. | PROVEN | Full step-7 run green: 761 passed with only the 12 named 17c residuals; parity 71 passed; EX-0 holds 1071. §8. |
| C-008 | Registry rows `EX-FN-20`/`EX-FN-21`/`EX-FN-28`/`BL-13`/`BL-14` flip to FIXED with the new pin paths, one row per residual divergence is appended in §7, and every touched `map.md` is in lockstep. | `docs/spark-sql-iceberg-parity.md` diff; `make check-map-sync`. | PROVEN | True flips verified with pin paths; BL-13 rewritten rather than flipped per D-18 because its proposition changed shape; seven §7 residual rows appended, each naming its owner; maps in lockstep. §8. |

VERDICT: 8 clauses, 4 PROVEN, 4 OPEN, 0 REJECTED. No COVERAGE_ATTESTATION and the ledger stays in `staging/`: four clauses carry dated, registry-rowed divergences owned by other slices (R-17a-16, R-17a-22 / ERR-UNRESOLVED-COL-1, and run 17c's BL-14 and INTERVAL DAY seams), so their propositions as written are not fully met. Every such clause is OPEN rather than dressed as proven; the unit ships its green work and names what it did not close.

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

## 3. Step-2 record (2026-09-15, run 16a round 2)

One Java-pattern parser in Rust per D-1: `crates/repark-functions/src/java_datetime.rs`
(997 lines, under the 1,000 ceiling), shared by `to_date` / `to_timestamp` /
`unix_timestamp` with a format. It tokenizes `yyyy`, `yy`, `M`, `MM`, `MMM`,
`MMMM`, `d`, `H`, `h`, `m`, `s`, `S` (widths 1–9), `a`, quoted literals and
separators; an unquoted `Y` run refuses
`[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION]`; every
other failure renders Spark's `[CANNOT_PARSE_TIMESTAMP]` text (ANSI) or NULL
(non-ANSI). Reuses the `CANNOT_PARSE_TIMESTAMP` precedent in
`timestamp_cast.rs`; month-name matching mirrors `try_to_date` (self-contained
tables, no `convert.rs` churn). Constant patterns compile once per batch
(`FormatPlan::Shared`, per-row fallback); each input column casts once per
batch (D-6). `to_timestamp` returns LTZ in the session zone, `unix_timestamp`
seconds from the parsed wall clock, `to_date` the wall DATE. Non-string inputs
with a format keep the 1-arg path (covers `unix_timestamp(dt, 'yyyy')` =
midnight of the date). `to_timestamp` 1-arg malformed strings translate to
Spark's `[CAST_INVALID_INPUT]` under ANSI and NULL otherwise. Python-only
logic: none (Rust first holds; the facade binds names, `lit(format)` shapes
and the `NOT_ITERABLE` refusal for a Column format). Frozen names per D-2:
required parameters unchanged (`unix_timestamp` default widened `None` →
`'yyyy-MM-dd HH:mm:ss'` to the recorded oracle spelling; required set stays
empty so the freeze holds).

Per-name pin counts (both doors, both ANSI settings, both zones):
`to_date` 22/22, `to_timestamp` 42/44, `unix_timestamp` 28/28 green; facade
signatures match for all three names (C-001 partial). Rust unit tests beside
the parser: 11 (`java_datetime::tests::*`); `cargo test -p repark-functions`
572 passed, 0 failed, 1 ignored. Census per D-4: no name added or stubbed, so
the deferred tuples, split-identity tail, EX-0 count and example walk are
untouched; the `test_chrono_java_format_refusal` pin in `test_fn_batch3.py`
became `test_java_datetime_patterns_parse` (the refusal it pinned is gone),
plus a Python-door quoted-`T` assertion. `functions_expr.py` held at exactly
2235 lines (cap-1 neutral).

Residuals for later steps (registry untouched until step 7): cells 129/130
(`to_timestamp(…, "yyyy-MM-dd'T'HH:mm:ss")`, SQL door) stay red — the SQL layer
parses `"…"` as an identifier (`AnsiDialect` in `crates/repark-sql`), a
parser/dialect edit D-3 assigns to run 16c; the kernel path is pinned on the
Python door per the D-10 precedent, no HALT. All other reds in the pin file
are step 3–7 names (`to_timestamp_ltz/ntz`, `try_to_timestamp`, TIME family,
`to_char` family, `make_timestamp` keywords, BL-13/BL-14).

## 4. Step-3 record (2026-09-15, run 16a round 3)

`to_timestamp_ltz` / `to_timestamp_ntz` / `try_to_timestamp` answer PySpark
4.1.2 on both doors, both ANSI settings and both zones, on the step-2 parser.
No new parser (card step-3 scope).

New Rust module `crates/repark-functions/src/timestamp_ltz_ntz.rs` (one file;
`expr_fn` + dispatch arms only elsewhere, registration folds into the existing
`instant_ts::functions()` vector): `to_timestamp_ltz`
forwards both arities to the `to_timestamp` kernel; `try_to_timestamp`
forwards with the ANSI extension cloned off so data errors answer NULL under
both ANSI settings while pattern refusals still raise; `to_timestamp_ntz`
emits naive walls — the format arm reuses the `plan_format_column` /
`parse_wall_or_null` primitives through a module-local walls helper, the 1-arg
arm strips a zone/offset suffix and round-trips the wall through the
`to_timestamp` kernel before un-localizing. The `stamps_with_format_column`
loop in `java_datetime.rs` stays untouched behind its pins (`java_datetime.rs`
sits 3 lines under its ceiling, so the helper could not move there).
Malformed NTZ strings retarget `[CAST_INVALID_INPUT]` at the `TIMESTAMP_NTZ`
name; partial patterns answer `[CANNOT_PARSE_TIMESTAMP]` with the recorded
index. Python-only logic: none (Rust first holds; the facade binds names and
the literal `format` position, and `format` accepts a Column per the recorded
`ColumnOrName` signatures).

Red evidence for the partial-batch flaw the fixture cannot see (single-row
garbage cells): the new example's 3-row frame answered `[None, None, None]`
for `try_to_timestamp` — one bad row nulled the valid rows, against the
`try_to_date` per-row precedent. Fix in the same round: batch fast path plus
a per-row salvage fallback that runs only on partial failure (D-6 hot path
stays one cast per batch), applied to `try_to_timestamp` under both ANSI
settings and to `to_timestamp_ntz` under non-ANSI. ANSI-raise paths are
unchanged. New Rust pins `try_keeps_valid_rows_beside_garbage` and
`ntz_without_ansi_keeps_valid_rows_beside_garbage` cover it.

Decisions applied in this round: D-12 the two new facade names ride the
existing `FNP11A_EXPORTS` installer tuple and its example-coverage binding —
a new exports tuple would cost `check_example_coverage.py` a binding-table
line it has no room for (exactly on its 1000-line ceiling, ceilings only move
down per Q-15c-4), so the tuple grows eleven to thirteen with this note.
D-14 the crate-root ceiling (`check_lib_rs`: `lib.rs` exactly on 175) is paid
by sanctioned out (1): the step-3 UDFs register through the existing
`instant_ts::functions()` vector (944 lines, under its ceiling) and the
undocumented `install_shared_analyzer_rules` moves from the root to
`bool_decimal.rs` (335 lines) with the one caller retargeted — root lands at
171 lines, no baseline touched.
D-13 no double-quoted-pattern cell exists for the three names (zero fixture
cells carry `"` in `expr`), so nothing stays red on the run-16c account.
Q-15a-3 / Q-15c-4 named where applied. Owner rulings applied: RUST FIRST,
SHAPE RULE, NO COMMENTS IN CODE (added-line grep clean).

Per-name pin counts (both doors, both ANSI settings, both zones):
`to_timestamp_ltz` 28/28, `to_timestamp_ntz` 28/28, `try_to_timestamp` 30/30
green; facade signatures match the recorded oracle for all three names (the
file-level signature pin still stops at the later-step `current_time`, then
`make_time` / `to_time` / `time_diff` / `time_trunc` / `to_char` family /
`make_timestamp` keywords per steps 4–7). Step-2 counts hold (`to_date`
22/22, `unix_timestamp` 28/28; the only `to_timestamp` reds stay SQL-door
cells 129/130, the run-16c seam). Rust unit tests beside each kernel: 10 in
`timestamp_ltz_ntz::tests::*` (LTZ zone shift mirrored to `to_timestamp`, NTZ
no-shift in both zones, NTZ offset-strip, NTZ `TIMESTAMP_NTZ` naming,
try-NULL under ANSI on, partial-batch pair);
`cargo test -p repark-functions` 582 passed, 0 failed, 1 ignored; clippy the
Makefile way (`-A clippy::disallowed_methods`, the panic-ban gate holding
`disallowed_methods` live on lib+bins) clean.

Census per D-4: `test_functions_d.py` deferred tuple retired (presence pin in
its place); `test_fn_batch3.py` stub refusal replaced by an answering pin;
`BYNAME` facade-only row drops `try_to_timestamp` (derived census green);
split-identity tail and total follow the thirteen-name tuple with no edit;
EX-0 enumerator count 1057 → 1059, backlog 112 → 111 with `F.try_to_timestamp`
covered by the new runnable example
`docs/examples/functions/timestamp_ltz_ntz.py` (listed in that directory's
`map.md`, executes green under `--require-execute`; inventory gains the two
LTZ/NTZ rows). Registry row EX-FN-20 flips to FIXED with the oracle cells as
its pin (EX-FN-21 shape); the refusal pin
`test_examples_functions_b.py::test_try_to_timestamp_refuses` is retired.
C-001 PROVEN for the three names; C-002/C-003/C-004 PROVEN for their cells;
C-005/C-006/C-007/C-008 stay OPEN for steps 4–7.

## 5. Step-4 record (2026-09-15, run 17a round 4)

TIME family (100/104 cells green; 4 bare-`current_time` were already green) and
BL-13 day-time intervals green. Red-first on the rebuilt native (release rebuilt
after the #611 rebase, before measuring): the 104-cell scope
(`make_time` 18, `to_time` 20, `time_diff` 20, `time_trunc` 18, `current_time`
12 of 16, `try_avg` 10, `avg` 2) fails as §1 records — `make_time` answers
`time32`, `to_time` answers `time64`, `time_diff`/`time_trunc`/`current_time`/
`typeof` are absent names, `current_time(0/3/7)` refuses on arity,
`hour(current_time())` answers, `try_avg(INTERVAL)` refuses `[FNP-11]`,
`avg(INTERVAL)` refuses coercion.

New Rust `crates/repark-functions/src/time_family.rs` (no new parser): one
`TimeRefusal` kernel serving `make_time` / `to_time` / `time_diff` /
`time_trunc` unconditionally (`return_type`, `return_field_from_args` and
`invoke` all raise `[UNSUPPORTED_TIME_TYPE]`, arity checked in `coerce_types`);
`CurrentTime` answering session-zone `time64[ns]` truncated to the precision
with Spark's `[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]` past 6; `SparkTypeof`
spelling Arrow types (`time(6)`, `timestamp_ntz`, `int`, `string`);
`DatePartWithoutTime` wrapping the `datetime.rs` hour/minute/second kernels so
a TIME input refuses and timestamps answer (the file's own
`hour_minute_second_accept_time_and_timestamp` pin asserted the old answering
behavior and is rewritten to `hour_minute_second_refuse_time`, net −1 line
with the ceiling rows ratcheted 1700 to 1699);
`TimeCastGuard` analyzer rule refusing any
`CAST`/`TryCast` to TIME whose inner is not a string literal. Registration
folds into `instant_ts::functions()`; the guard wires into
`install_shared_analyzer_rules`; dispatch adds one shared refusal arm plus
`current_time`/`typeof` arms in `function_dispatch.rs`; the facade binds the
six names in `functions_temporal.py` (PySpark 4.1.2 names, order, defaults)
riding `FNP11A_EXPORTS` thirteen to nineteen per D-12. Rust pins beside each
kernel: 6 in `time_family::tests::*`.

New Rust `crates/repark-functions/src/interval_avg.rs` (BL-13):
`IntervalAvgAccumulator` over interval and duration inputs with Spark
`Long`-micros checked accumulation, round-half-away division and
`MonthDayNano` out. Overflow answers NULL on `try_avg` and raises Spark's
`[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION]` on `avg`; all-NULL answers
NULL; mixed month/day-time inputs follow the overflow arm. `aggregate.rs`
gains only the signature/return-type/accumulator/state-fields arms (avg learns
the interval/duration signatures try_avg already had); both `[FNP-11]`
refusals are gone, replaced by answering Rust pins. Groups accumulators stay
numeric-only so intervals run the row accumulator. `avg` YM/DT cell counts:
`avg` 2/2 green. `cargo test -p repark-functions` counts in §6.

Per-name pin counts (both doors, both ANSI settings): `make_time` 18/18,
`to_time` 20/20, `time_diff` 20/20, `time_trunc` 18/18, `current_time` 16/16,
`try_avg` 6/10, `avg` 2/2 green. Python-only logic: none (Rust first holds).

Harness (all in `test_fnp11b_temporal_formats.py`, red-first, no expectation
changed): the tm-frame view builds through lazy `F.expr` instead of eager
`session.sql` (the session analyzes `sql()` at once, which tripped the new
guard at setup); Python error cells reading `tm` and the three poisoned
`current_time` nests replay on the tm-frame; `rows_as_string` cells assert the
CAST text before the error branch (the recorder's client-side
`NOT_IMPLEMENTED` is not engine behavior); the recorder's trailing `--` is
stripped and the CAST wraps a scalar subquery so the recorded query nests.

Two committed pins asserted the old answering behavior and are rewritten to
the oracle contract (the card row and fixture outrank them):
`datetime::tests::hour_minute_second_accept_time_and_timestamp` becomes
`hour_minute_second_refuse_time`, and repark-spark
`time_arguments_never_move_with_the_session_zone` asserts the zone-independent
refusal instead of 13/45/7 (its `///` line is untouched).

Decisions applied in this round: D-15 frame poisoning is the recorded
behavior (live PySpark 4.1.2 probe, banner 4.1.2/UTC, 2026-09-15: any select
over a TIME-column relation raises, even pruned; the three Python
`current_time` nests answer `True`/`'time(6)'` on a clean frame, so the
fixture errors reproduce only over the tm-frame). D-16 the two YM
`rows_as_string` cells (187/188) stay red: RePark renders the averaged
`{months:2}` as `2 months` while the oracle wants
`INTERVAL '0-2' YEAR TO MONTH`, and the identical Arrow value already pins to
`2 months` under EX-FN-19 — one kernel cannot render both; orchestrator ruling
needed (Q1). D-17 the two NULL try_avg cells (191/192) stay red on the SQL
door: `CAST(NULL AS INTERVAL DAY)` does not parse (run-16c dialect seam,
D-3); the NULL semantic is pinned at the kernel
(`interval_avg::tests::all_null_input_is_null`). D-18 the BL-13 registry row
claim "`try_avg` is not NULL-on-interval-overflow" is stale (2026-08-31
oracle): the 2026-09-14 fixture and today's live probe agree overflow answers
NULL on `try_avg` and raises on `avg`; the row rewrite lands in step 7, so
BL-13 does NOT flip in this round. D-19 `minute`/`second` refuse TIME inputs
beside `hour` (live-probe verified Spark behavior, same wrapper, card names
only `hour`). D-20 the crate root lands at 175/175 (`mod interval_avg;` +
`pub mod time_family;`): steps 5–6 place new code in existing files or
submodules, never a new root line. D-21 `CAST('str' AS TIME)` answers (the
string-literal carve-out keeps `TIME'…'` literals answering, which FNP-11A
pins); Spark raises — residual for the step-7 §7 row. D-22 `CAST(NULL AS
TIME)` inside the tm view is what poisons it, matching Spark's analyzer;
stored TIME tables and the Python `.cast("time")` allowlist stay untouched.
Owner rulings applied: RUST FIRST, SHAPE RULE, NO COMMENTS IN CODE
(added-line grep clean), Q-15c-4 (ceilings only down: `datetime.rs`
1699 with its rows ratcheted, lib root at its ceiling,
`functions.py`/`functions_expr.py` untouched at their baselines).

Census per D-4: presence pin `test_fn_d_time_family_names_are_present` added;
split-identity tail follows the nineteen-name tuple with no edit (docstring
notes step 4); no `test_fn_batch*.py` stub named these names; EX-0 enumerator
count 1061 → 1067 with the six rows added to `docs/examples/inventory.txt` in
sort order; backlog stays 111 with the new runnable example
`docs/examples/functions/time_family.py` (listed in that directory's `map.md`,
executes green under `--require-execute`; `current_time`/`typeof` answer,
the four builders assert the refusal text); the refusal pin
`test_fnp7_try_inversions.py::test_try_avg_interval_refuses_fnp11` is retired
for `test_try_avg_interval_answers` (day-time average, try-NULL on overflow,
avg raise). C-001 PROVEN for the six names; C-002/C-003/C-004 PROVEN for
their cells; C-005 PROVEN except the three poisoned nests are green via the
tm-frame replay; C-006 PROVEN for day-time, NULL-input (kernel), and overflow
semantics, OPEN for the YM-string rendering (D-16) and the NULL-cell dialect
seam (D-17); C-007/C-008 stay OPEN for steps 5–7 (no registry row flips in
this round: no BACKLOG row names the TIME family, and BL-13 waits for D-16).

## 6. Step-5 record (2026-09-15, run 17a round 5)

The `to_char` family answers 22/22 oracle cells on both doors under both ANSI
settings, and the R-17a-16 divergence pins land. Red-first on the rebuilt
release native: 23 failed (the 22 cells — `AttributeError` on the Python door,
DataFusion's `to_char` coercion refusal on the SQL door — plus the signature
pin, which stops at the step-6 `make_timestamp` before reaching the new
names).

New Rust `crates/repark-functions/src/try_invert/strict.rs` (a submodule, per
D-20 — the crate root stays at its ceiling): `to_number` / `to_binary` are
the raising twins over the shared `convert.rs` grammar (`parse_number_format`
/ `apply_number_format` / `decode_hex` / `decode_base64` go `pub(crate)`;
mismatch raises Spark's `[INVALID_FORMAT.MISMATCH_INPUT]`, malformed bytes
raise `[CONVERSION_INVALID_INPUT]`, both byte-exact against the cells).
`to_number` returns the format's `(precision, scale)`, `(38, 18)` for a
non-foldable format. `to_binary` defaults to hex (odd input left-pads, which
the `lit('abc')` cell proves) and takes `utf-8` / `base64` / `binary`.
`to_char` / `to_varchar` are one kernel under two names dispatching on the
input type: numerics render through an Oracle-style mask, timestamps and
dates through the `datetime.rs` Java-pattern path in the session zone,
binaries as `hex` / `base64` / `utf-8`, strings pass through. Nullability is
per input kind via `return_field_from_args` (timestamps non-nullable like
`date_format`, `utf-8` binary arms nullable). Registration folds into
`try_invert::functions()` so both doors leave DataFusion's `to_char` behind;
the facade binds the four names in `functions_temporal.py` (recorded PySpark
4.1.2 names, order, defaults — verified by hand against the fixture since the
file-level pin stops at `make_timestamp`) riding `FNP11A_EXPORTS` nineteen to
twenty-three; one shared dispatch arm calls `expr_fn::to_char_family`, which
keeps `function_dispatch.rs` at 995 lines under its ceiling (the four-arm
shape crossed it at 1014 and red-lit the CAP-1 debt test). Rust pins beside
the kernel: 8 in `strict::tests::*`; `cargo test -p repark-functions` 619
passed; clippy the Makefile way clean.

Per-name pin counts: `to_char` 6/6, `to_varchar` 6/6, `to_number` 4/4,
`to_binary` 6/6 green.

Harness (D-23): block-2 SQL cells name the math frame view bare. The
`FROM (view)` wrap never parsed on either engine, which red-lit all eight
SQL cells before any kernel ran; the eight block-2 SQL cells are the only
`FRAME` carriers, each exactly once. R-17a-16 applied: cells 187/188 pin as
`DIVERGED_2026_09_15_ROWS_AS_STRING` (`2 months` against the recorded oracle
text `INTERVAL '0-2' YEAR TO MONTH`); the §7 registry row waits for step 7.
Cells 191/192 stay red for run 17c's NULL INTERVAL DAY seam.

Decisions in this round: D-24 the mask semantics come from the FNP-MATH-1
generator (`/tmp/oc-worker/pa-math/o245.py` row 3 carries a real
`CAST(-0.5 AS DECIMAL(10,4))`), so the sign drop, the strip-trailing-zeros
then pad rule both mask widths prove, and the `#`-with-blank-group overflow
spelling are oracle derivations, not memory. D-25 one dispatch arm with
arity enforced by the kernels' coercions (no existing pin names these
arities). D-26 `to_binary` accepts a `binary` format as utf-8 bytes
(brief-directed, cell-unpinned) and raises `[INVALID_FORMAT.UNEXPECTED_TOKEN]`
on an unknown format (wording unpinned). D-27 unpinned edges stay
non-crashing and narrow: `to_char(string)` passes through, dates take the
timestamp path, doubles render from the shortest repr, non-finite or
exponent reprs render the overflow mask, `S`/`PR`/`MI` pass through as
literals with the sign still dropped, no-dot masks with a fraction overflow,
and a non-foldable `to_number` format normalizes to `(38, 18)` truncating
toward zero. D-28 `to_char` on binary with an unknown format raises the same
`UNEXPECTED_TOKEN` shape. D-29 the CAP-1 prose pin red-lit on the
orchestrator's own fix-up sentence, which quoted the forbidden numeral while
removing it; the sentence now names "the numeral phrase" with no digits.
Owner rulings applied: R-17a-16 (divergence pins, §7 row deferred), R-17a-15
(no bare numeral in any `map.md`, verified by the green prose pin), R-17a-13
(`typeof` untouched — its cells stay green in the full-file run), R-17a-3
(EX-0 moves 1067 → 1071, never down).

Census per D-4: presence pin `test_fn_d_to_char_family_names_are_present`
added; split-identity tail notes step 5 with no edit (length-driven); the
`test_functions_f.py` deferred-absence list named `to_number` / `to_binary`
and retires with them (record in `tests/map.md`); inventory gains the four
rows in sort order; the new runnable example
`docs/examples/functions/to_char_family.py` (listed in that directory's
`map.md`, executes green under `--require-execute`) covers the four names
with answers plus both strict-twin refusal asserts. No registry row names
the four names (`to_char`/`to_varchar`/`to_number`/`to_binary` appear only
inside `try_to_*`/`format_number` rows), so nothing flips in this round.
C-001 PROVEN for the four names (hand-verified; the file pin stays red at
`make_timestamp` for step 6); C-002/C-003/C-004 PROVEN for their cells;
C-005/C-007/C-008 stay OPEN for steps 6–7.

## 7. Step-6 record (2026-09-15, run 17a round 6)

Red-first on the rebuilt release native: the 19 `make_timestamp` cells all fail
with `TypeError: make_timestamp() got an unexpected keyword argument 'date'`;
the 9 open `typeof` cells fail with `typeof(List/…)` not implemented (5),
`lit() … got Decimal` (1, cascading into the array cell), and the old arity
text (2); the 10 `date_plus_interval` cells fail with DATE answers where the
oracle wants timestamps.

6a (D-30, owner ruling Q-15a-3): both `make_timestamp` defs widen to PySpark
4.1.2's nine all-optional parameters (the live def in `functions_expr.py`
forwards to `functions_temporal.py`); `_timestamp_parts` already assembles the
`(date, time[, timezone])` call, so no dispatch change was needed. The kernel
already built zoned walls from dates and TIME values; D-31 adds the
`HH:MM:SS[.ffffff]` string arm to the time reader (facade `lit(time)` arrives
as text) with garbage raising beside the date reader's malformed path. 14/19
cells go green: the 10 value cells (zone-local walls, CET per-row zone), the 4
`UNSUPPORTED_TIME_TYPE` cells via the tm-frame poison (the kernel never sees
them), and the signature pin. D-32 routes `UNRESOLVED_*` error cells to the
plain frame (the step-4 brief's rule) — but the five `UNRESOLVED_COLUMN` cells
stay red: the engine has no unresolved-column taxonomy (both doors raise raw
DataFusion `Schema error: No field named …`, pinned as-is by seven existing
pins), and the exception taxonomy is a sensitive path that never rides as a
passenger — see the HALT question. The two EX-FN-28 refusal pins retire into
answering pins; the freeze register regenerates (`required_params` empty) in
the same commit; EX-FN-28 flips to FIXED; EX-FN-10's numeric pin stays green.

6b (D-33, BL-14): STOP, exactly as the card orders. Probes prove the seam:
`INTERVAL 0 HOUR` and `INTERVAL 0 DAY` plan to identical
`IntervalMonthDayNano{0,0,0}` literals, so no kernel can tell them apart
(guessing from the value is forbidden, and the zeros make even that
impossible); `INTERVAL '0 00:00:00' DAY TO SECOND` fails at parse
(`Unsupported Interval Expression with last_field Some(Second)`). The DATE to
timestamp promotion needs the unit where it is still visible — the SQL
planner — which run 17c owns (`crates/repark-sql` untouched). All 10 cells
stay red for 17c.

6c (R-17a-13): the 40-cell oracle copies verbatim to
`python/repark/tests/fnp11b_typeof_spark_oracle.json` (cmp-clean) and every
cell pins on its recorded door in `test_fnp11b_typeof.py` — 40/40 green (35
answers plus the signature, the two arity texts, and five blocked pins).
D-34 spells `array<…>` / `map<…>` / `struct<…>` recursively through typeof's
own table: the `repark-spark` renderer is the engine's canonical one but the
crate edge runs the wrong way to call it (and 17c owns that table), so no
second table is carried and the measured primitive spellings
(`smallint`/`tinyint`/`float`) move untouched for 17b. Wrong arity raises
Spark's `[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]` verbatim. D-35 refuses to guess
units from values, so the three interval spellings pin as blocked on the same
17c unit seam as BL-14 (`INTERVAL 1 DAY/YEAR/MONTH` all arrive
`MonthDayNano`). D-36 pins `TIMESTAMP_NTZ'…'` blocked on 17c's grammar seam
and records the `binary` function name as unowned (neither matrix, no
registry row — not added). D-37 gives `lit` a `decimal.Decimal` arm (Spark's
inferred precision and scale, non-finite refuses) built as a cast of the
decimal text, so no new native literal constructor is needed; the module's
own `max` shadows the builtin, so the width math uses conditionals.

Census: no newly destubbed name (no inventory/EX-0 movement — count stays
1071 — no example obligation; `typeof`/`make_timestamp` presence already
pinned). D-39 moves the two exact Python baselines the round grows
(`functions.py` 1960 → 1984 for the `lit` arm, `functions_expr.py` 2235 →
2237 for the widening) with the CAP-1 mirror in the test's duplicate table
and a `scripts/map.md` row — a baseline record, not a ceiling raise. No
registry row names `typeof` or the interval spellings, so only EX-FN-28 flips
this round. R-17a-3 and R-17a-17 held throughout.

## 8. Step-7 record (2026-09-15, run 17a round 7 — rule application, registry, departure)

R-17a-22 applied (owner ruling, 2026-09-15 — this ruling overrides the step-6
HALT lean, and no central rule is built). The five `make_timestamp`
`UNRESOLVED_COLUMN` cells (fixture indices 0–4, all python-door, both ANSI
settings, UTC and America/New_York) pin as dated divergence
`DIVERGED_2026_09_15_UNRESOLVED_COLUMN` in
`test_fnp11b_temporal_formats.py::_assert_error_cell`: each asserts the oracle
condition `UNRESOLVED_COLUMN.WITH_SUGGESTION`, then the measured head
`Schema error: No field named tm.` (probed 2026-09-15 on the rebuilt release
native; the valid-fields tail stays unpinned). The reason is recorded: Spark's
proposal list is ranked by edit distance (`/tmp/oc-worker/ra-11b4/
unresolved_column_spark_oracle.json`), so a schema-order rule mistypes three of
the seven SQL cells — the fix reproduces the ranking across every plan node on
both doors as carry-over ERR-UNRESOLVED-COL-1. The exception taxonomy and the
shared error mapper stay untouched, and the seven existing pins stay exactly as
they are. Cells 0–4 go green under the divergence pins.

Registry pass (C-008): EX-FN-20/EX-FN-21/EX-FN-28 were already FIXED with pin
paths in steps 2/3/6 — verified in the tree, not re-touched. BL-13 does NOT
flip: per D-18 its overflow claim was stale, so the row is rewritten (PARTLY
FIXED day-time kernel per step 4, stale-claim fix) and stays BACKLOG — the
round brief's flip shorthand does not survive the ledger, and flipping an
unfixed row would be false. No `to_char`-family, TIME-family or `typeof` row
exists to flip (verified: no `###` header names those names; the step-5 and
step-6 censuses). EX-FN-28's Pin paragraph now names R-17a-22 and
ERR-UNRESOLVED-COL-1. Seven §7 rows appended before §8: FNP-11B-YM-AVG-1
(cells 187/188, R-17a-16, run 17b type seam), FNP-11B-UNRESOLVED-1 (cells 0–4,
R-17a-22, ERR-UNRESOLVED-COL-1), FNP-11B-BL14-1 (ten `date_plus_interval`
cells, run 17c planner seam), FNP-11B-NULL-AVG-1 (cells 191/192, run 17c D-3/D-17
dialect seam), FNP-11B-TYPEOF-NTZ-1 (17c grammar seam, D-36),
FNP-11B-TYPEOF-BINARY-1 (unowned name, D-36), FNP-11B-TYPEOF-INTERVAL-1 (17c unit
seam, D-35). D-21 (`CAST('str' AS TIME)`) is deferred without a row: its
reproduction context sits in step-4's tm-frame machinery, plain-door probes do
not reproduce an answering shape, and the round brief's row list does not name
it — the owner places it.

Final clause verdicts: C-001 PROVEN (nineteen names plus the all-optional
`make_timestamp` widening; the freeze pin is green). C-002 PROVEN except cells
0–4, ruled divergence R-17a-22. C-003 PROVEN except cells 187/188 (R-17a-16)
and cells 191/192 plus 197–200 plus 203–208 (run 17c). C-004 PROVEN except cells
0–4 (R-17a-22). C-005 PROVEN except the five blocked `typeof` pins (17c seams
plus the unowned `binary` name). C-006 PARTIAL: day-time average, try-NULL on
overflow, avg-raise and the kernel NULL semantic are PROVEN; the YEAR-MONTH
render, the NULL-input SQL parse and BL-14 are ruled residuals for run 17b/17c.
C-007 PROVEN: the full step-7 gate run is green (counts below); the EX-0 count
holds at 1071 and no newly destubbed name moves the census. C-008 PROVEN except
the BL-13 flip, which the ledger overrules per D-18 (rewritten, not flipped):
all true flips carry pin paths, seven §7 rows are appended, and the touched
`map.md` files are in lockstep.

No COVERAGE_ATTESTATION block is added, plainly and deliberately: C-002/C-003/
C-004/C-005 carry ruled divergences and C-006/C-008 are partial, so not every
clause is PROVEN and attesting otherwise would be false. The residuals name
their owners (run 17b, run 17c, ERR-UNRESOLVED-COL-1).

Gates (2026-09-15, rebuilt release native): `cargo test -p repark-functions
--lib` 619 passed 0 failed 1 ignored; `make rust-clippy` clean;
`maturin develop --release` installed repark-1.4.2; pytest over the unit's pin
files plus `test_functions_d.py`, `test_fn_batch3.py`,
`test_functions_split_identity.py`, `test_fnp7_try_inversions.py`,
`test_examples_functions_a.py`, `test_examples_functions_b.py`,
`test_fnp11a_temporal.py` — 761 passed, 12 failed, every failure a named 17c
residual (191/192 NULL-interval, ten BL-14); parity
`test_api_freeze.py`/`test_ex_0_example_coverage.py`/
`test_cap_1_source_file_line_cap.py` 71 passed;
`check_example_coverage.py --require-execute` exit 0 (1064 names, 952 covered,
111 backlog, 243 examples); `check_rust_file_size.py` 561 clean;
`check_lib_py.py` 745 clean; `ruff format --check .` 997 formatted;
`make verify` exit 0.
