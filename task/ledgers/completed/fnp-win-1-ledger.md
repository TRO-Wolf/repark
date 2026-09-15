# Charter ledger — FNP-WIN-1 · window, window_time, session_window (run 15a)

**Date:** 2026-09-15 · **Branch:** `feat/fnp-win-1` · **Base:** `1aa95356`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Card FNP-WIN-1 (1.5 Spark-parity campaign, run 15a): `window`,
`window_time`, and `session_window` are absent on both doors. The live PySpark
4.1.2 oracle `/tmp/oc-worker/pa-win/o245_spark_oracle.json` (recorder
`/tmp/oc-worker/pa-win/o245.py`, three rows `ts` = 10:07:30 `k1`, 10:12:00
`k1`, 10:31:00 `k2`, session zone UTC) pins tumbling, sliding, `startTime`,
`window_time`, static and dynamic-gap sessions, and the `CANNOT_PARSE_INTERVAL`
/ `MISSING_AGGREGATION` errors on both doors.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, dependency files,
`dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py`
(orchestrator D-2 fence); `crates/repark-core` planning and the SQL parser
(run 15c owns that seam); steps 4–5 (session_window, registry) after run
16a delivers step 3 (`window_time`).

**Shape rule (owner, 2026-09-14):** a name is done when it is present on
`repark.spark.functions` AND answers PySpark 4.1.2 on BOTH doors for its
argument shapes, NULLs, result types and error classes, with pins — or raises
Spark's own error condition with a dated registry row where the engine cannot
carry it. Never a silent stub.

**Standing instruction (owner, 2026-09-14) — Rust first:** every new function
lands in Rust with the Python facade a thin wrapper; a Python-only
implementation is allowed only when the engine cannot carry it, and the ledger
states why in one line.

## Decisions (orchestrator rulings under G-2)

- D-1 `window` is a projection that can expand one row into several (sliding).
  Implement it as an analyzer/planner rewrite in the function layer (an
  `Expand`-style unnest of the overlapping bucket starts, as Spark's
  `TimeWindowing` rule does), not as a scalar UDF. `session_window` needs a
  grouping-aware rewrite (Spark's `SessionWindowing`). Put the rules in
  `crates/repark-functions/src/` beside the existing analyzer rules and
  register them through `analyzer_rules()`; facade wrappers in
  `python/repark/src/repark/spark/functions_window.py`. If the rewrite needs a
  change inside `crates/repark-core` planning or the SQL parser, STOP and hand
  back with the exact seam — that is run 15c's planner.
- D-2 Never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`,
  `types.py`. The DataFrame door's `groupBy(F.window(...))` must work through
  the existing GroupedData path: if GroupedData cannot carry a struct grouping
  key without an edit to `dataframe/**`, hand back with the seam (run 15b owns
  it) and pin the SQL door plus the plain `select`.
- D-3 Durations parse Spark's interval strings (`'10 minutes'`, `'1 hour'`,
  `'5 seconds'`, `'1 day'`), including the `CANNOT_PARSE_INTERVAL` error text
  from the oracle cell.
- D-4 Timestamps: the session zone is UTC in every cell; bucket boundaries are
  computed on the UTC epoch (Spark's rule), so they do not shift with the
  zone. Pin one extra America/New_York session cell as a residual row if
  RePark's boundaries differ.
- D-5 Registry: add rows for every residual (for example sliding windows over
  a DST boundary, or `window` on a `timestamp_ntz` column) with pins.
- D-6 Pins: `python/repark/tests/test_fnp_win_1.py`, both doors, over the
  oracle cells, red first on the base tree with the red summary in the ledger;
  Rust tests beside each rule, including a two-partition input for
  `session_window`.
- D-7 (audit F-1, run 16a): the comment ban covers every source file, so the
  `check_lib_rs.py` ceiling reads bare `182,` and the measurement note lives
  in the row's reason string.
- D-8 (owner ruling Q-15c-4, 2026-09-15): size baselines are ratchet-only; the
  repark-functions crate-root ceiling 175 → 182 is the one-time +N granted to
  this PR, not a standing allowance.
- D-9 (audit R-8, run 16a): the step-2 hook in
  `crates/repark-spark/src/spark_ast.rs` (one line) and the
  `crates/repark-spark/src/time_window.rs` staging stay — function
  registration for a grouping function that the planner cannot see after
  parsing. The hook stays one line; run 16c owns that seam and may move it.
  No further edits under `crates/repark-spark` beyond `time_window.rs`.
- D-10 (owner ruling Q-15a-5): `test_functions_split_identity.py` asserts
  `len(set(F.__all__)) == len(F.__all__)`; if a sibling PR adds the same
  assert, keep one.
- D-11 (audit F-2, run 16a): the comment ban covers `///` doc comments too,
  so the five `pins:` citations added beside the rule last round are deleted
  and each citation lives in the matching `map.md` row, which the
  ledger-grammar gate reads. The moved `/// Return analyzer rules` docstring
  in `registration.rs` is deleted with them; the docstring-presence gate is
  Ruff/Python-only, so no mechanical gate requires it.
- D-12 (audit F-3, run 16a): the no-JVM premise was wrong — PySpark 4.1.2
  runs on the orchestrator box, which recorded 39 residual cells live
  (`win_resid_spark_oracle.json` plus `oracle_win_resid.py`). They are
  appended to the oracle fixture with the run-15a portion intact, and every
  differential residual pin is replaced by cell-backed pins on both doors
  wherever the cell has both. Per cell: fix in Rust inside the fence
  (month/year legacy refusal text, startTime constraint, zero/negative gaps
  answer empty, DATE input, window null-drop); the session-month gap is
  pinned as an expected divergence with a `WIN-1` registry row.
- D-13 (audit F-4, run 16a-2): the three names reach `F.__all__` only at
  runtime through `install_into`, so the AST walk misses them and the
  example-coverage gate reds. `functions_window.py` joins
  `FUNCTIONS_INSTALLER_SOURCES` (its `INSTALL_NAMES` binding is already
  known). The added line is paid by rewrapping the `COVERS` paragraph from
  eight lines to seven, so the script stays at its ceiling and the Q-15c-4
  exception is not used. `docs/examples/functions/time_windows.py` covers
  all three names with inventory rows, and the EX-0 count moves to main's
  count plus three.
- D-14 (remediation L-001 REFUTED, run 16a): Spark merges two events exactly
  `gap` apart into one session, so the boundary is unchanged (`Gt` stays —
  a row splits only past the previous end). Pinned on both doors
  (`C-L001-exact-gap`) plus the near-gap cell, so the boundary cannot regress.
- D-15 (remediation L-002 P1): a NULL time in a plain `select` drops the row:
  sliding emits an empty list from `window_list_batch`, tumbling projections
  take the aggregate's `IsNotNull(time)` filter, and field-access-wrapped
  `window` calls rewrite through their wrappers (`rewrite_nested_windows`).
  Touched projections rebase to input qualifiers. The chained
  alias-then-`getField` select still hits a pre-existing engine-wide pushdown
  failure (`F.struct` reproduces it without any window code), so it is
  outside the D-2 fence and the pins use the single-select shape (§8).
- D-16 (remediation L-003 P1): `startTime` parses signed
  (`parse_window_offset`); `abs(start) >= slide` refuses with Spark's
  microsecond text rendering the absolute value, and `window`/`slide` stay
  strictly positive. One shared `check_window_spec` serves the analyzer rule
  and the scalar UDF.
- D-17 (remediation L-004 P1): a dynamic gap answers each row's calendar
  session end (`__repark_session_end__` returning months plus micros, one
  parse per distinct string per batch); NULL, zero, and negative gaps yield a
  NULL end that the drop filter removes, and chaining is `ts > previous end`.
  Civil month arithmetic reuses `spark_add_months`. Static literal month gaps
  still refuse — WIN-1 is rescoped to literals (§8), C-015 intact.
- D-18 (remediation L-005 P1): `slide > window` refuses with Spark's text on
  the Python door and in `select`; the SQL door keeps RePark's constraint
  refusal instead of imitating Spark's `UNRESOLVED_COLUMN` message, pinned as
  the WIN-2 divergence row.
- D-19 (remediation L-006 P2): the SQL staging recurses CTE bodies,
  derived-table subqueries, and both UNION branches inside `time_window.rs`
  only — R-8 still binds (`spark_ast.rs` untouched).
- D-20 (remediation L-007 P2): `session_window` accepts DATE through the same
  `Timestamp(ns)` coercion as `window` plus a cast wrap, so the struct stays
  `struct<start:timestamp,end:timestamp>`.
- D-21 (remediation L-008 P2): `window_time` refuses provably non-window
  structs (`windowed_argument` / `windowed_column`) with the oracle
  `_LEGACY_ERROR_TEMP_3101` text, including through temp views, which unfold
  to their defining projection. An opaque scan still answers — the WIN-3 row
  plus pin.
- D-22 (remediation L-009): the SQL sliding GROUP BY values and the Python
  sliding select values are pinned cell-for-cell.
- D-23 (remediation L-010): C-007 closes with this round's gates (§8); every
  residual carries a fix or a dated registry row (WIN-1 rescoped, WIN-2,
  WIN-3).
- D-24 (remediation S2-21 P2-1…P2-6): tumbling computes the single start
  directly, sliding streams starts into pre-sized typed builders with one
  batch downcast and no second scaled copy at microsecond unit, the bucket
  count is computed once per batch, session plans hash-partition on keys
  (single partition only without keys), and dynamic gaps parse once per
  distinct string per batch. Measured table in §8: sliding 0.65–0.72x,
  tumbling 0.70x, sessions ~1.0–1.2x on the 1-partition bench with the
  gather-to-one removed from the plan.
- D-25 (remediation P3-1…P3-4 Rust plus Python P3-1): ledger-only except
  P3-1, which the batched kernels fix as a side effect (struct and child
  validity is now the input null buffer, never a rebuilt vector). P3-2
  (analyzer walks) stands — the nested search only
  runs past a missed bare match. P3-3 (staging hot path) stands — `SELECT 1`
  is unchanged at 0.43 ms and the recursion only descends on GROUP BY,
  subquery, CTE, or set-operation nodes. P3-4 (`window_time` builder) stands —
  the rewrite shares the batched kernel. Python P3-1 stands — the lit-gap
  facade path keeps one UDF call per literal (6.9 µs, inside the `_scalar`
  band); a literal-folding rule is future work.
- D-26: a `select`-path refusal surfaces as `AnalysisException` or
  `PySparkException` depending on door transport (observed flapping on
  identical runs); the pins accept both and assert the condition text. The
  exception taxonomy is untouched (sensitive path).
- D-27: file-size baselines move exactly with the debt — new
  `time_window/mod.rs` 1268 and `spark_time_window.rs` 1125 rows, new
  `test_fnp_win_1.py` 1209 row, counts 36→38 and 31→32 — each with its reason
  and split seam in the gate tables. (`datetime.rs` is not among them: the
  audit round moves its helper out again, see D-28.)
- D-28 (audit S-2, owner ruling Q-15c-4): `datetime.rs` keeps its 1700
  baseline — the calendar month-end helper moves next to its only caller in
  `spark_session_window.rs` (708 lines, under the ceiling), reusing
  `spark_add_months` through a one-line `pub(crate)` widening, and the gate
  tables put `datetime.rs` back to 1700. The three new-file rows stand as
  this PR's one-time +N with their split seams.
- D-29 (audit citations): the pins use `getField` because the dotted struct
  path is registered at `COL-DOTTED-FIELD-1` (`col("st.a")` does not resolve;
  the nested spelling is `col("st").getField("a")`), and `createDataFrame`
  frames because SQL `TIMESTAMP'…'` literals plan as nanoseconds while the
  frames plan as microseconds (measured in §8: the session-end UDF tests
  assert `TimestampNanosecondType` for literal inputs). No new rows: the
  registry was searched for a VALUES-literals unit entry and none exists
  under any wording, so the frame choice rests on the measurement, not a
  citation.
- D-30 (audit P3): the `slide > window` / `abs(startTime)` refusals keep
  DataFusion's rule-name prefix — the analyzer loop wraps every rule error
  unconditionally, and no function planning hook (`coerce_types`,
  `return_type`, `return_field_from_args`) sees literal values, so the
  prefix cannot be raised from this unit's code. Recorded as a one-line note
  on WIN-2, not fixed; DataFusion and the session error mapping untouched.
- D-31 (verification round 2, L-003 P1): a dynamic-gap row joins while its
  timestamp is at or before the running maximum of the session's ends, not
  the previous row's end. The per-row leg replaces `lag(end)` with
  `max(end) OVER (PARTITION BY keys ORDER BY time ROWS BETWEEN UNBOUNDED
  PRECEDING AND 1 PRECEDING)`; the first row frames empty (NULL) and starts
  a session, and equality merges via the existing strict-`Gt` flag. The
  static leg is unchanged: sorted input plus a fixed gap makes the lag end
  equal the running maximum, so it already follows the same rule (static
  pins green untouched). Partitioning by the grouping keys is unchanged
  (S2-21). Rust pin
  `dynamic_gap_chains_on_the_running_end_across_batches` splits the
  discriminator across two batches in one partition; Python pins
  `test_crit2_dynamic_gap_chains_on_running_end` on both doors.
- D-32 (verification round 2, L-001 P1): `windowed_column` recurses unary
  nodes (Filter, Limit, Sort, Distinct, Repartition, Subquery,
  SubqueryAlias) into their input and Join into the single child owning the
  column (zero or two owners refuse), while Union requires every branch
  windowed. Aggregate / TableScan / Window stay answering (grouped window
  columns, the WIN-3 opaque scan); every other node refuses (fail closed).
  The grouped-window-behind-filter/WHERE answers and the WIN-3 residual are
  pinned unchanged; every `C2-L001-*` cell is pinned.
- D-33 (verification round 2, L-002 P2): pins only. Naive datetimes in
  `createDataFrame` land as session-zone walls in RePark (measured:
  naive 10:00 reads back 10:00 in a UTC session), while the recorder's
  America/New_York driver shifted its naive literals into UTC (January
  +5 h, September +4 h). The pins therefore build the shifted walls
  directly, reproducing Spark's instants rather than its literal text.
- D-34 (verification round 2, L-004 P3): two session specs refuse with
  Spark's `[_LEGACY_ERROR_TEMP_1039]` text on both doors. The analyzer and
  SQL-staging sites share one constant from `spark_session_window.rs`; the
  Python door checks `check_single_session_spec` in the binding's
  `aggregate` before plan build, because both markers alias to
  `session_window` and DataFusion's duplicate-name error fires first. The
  DataFusion rule-name / planning prefix may precede the text (same
  standing rule as D-30); the pins assert condition plus text. Identical
  twin specs are out of scope: they still hit the duplicate-name error,
  fail-loud either way.

## PROPOSITION LEDGER — FNP-WIN-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `window`, `window_time`, `session_window` are present on `repark.spark.functions` with the PySpark 4.1.2 signatures from the oracle. | `test_fnp_win_1.py` signature pins, red on the base. | **PROVEN** | Step 1 red (see §1). Step 2 green for `window`, step 3 green for `window_time`, step 4 green for `session_window`: `test_session_window_signature_matches_pyspark` passes on the rebuilt release native (see §5). pins: fnp-win-1/C-001 |
| C-002 | Tumbling, sliding, and `startTime` `window` cells answer Spark on both doors, with the struct column named `window` of type `struct<start:timestamp,end:timestamp>`, in `groupBy` and in a plain `select`. | `test_fnp_win_1.py` window pins, red on the base. | **PROVEN** | Step 2 green: 9/9 window pins pass on the rebuilt release native (see §3): signature, tumbling groupby + schema, sliding groupby, startTime groupby, plain select, bad-duration, SQL tumbling groupby, SQL sliding schema. Audit round adds the residual value pins (week, DATE input, null-drop, pre-epoch, non-dividing slide) under the same pins (see §7). Remediation green (see §8): null selects drop on both doors, signed/zero `startTime` answers, nested/CTE/UNION SQL stages, sliding values pinned cell-for-cell. pins: fnp-win-1/C-002 |
| C-003 | `window_time` answers end-minus-one-microsecond on the Python door with `timestamp` type, and stays absent-or-error-compatible on the SQL door until step 3. | `test_fnp_win_1.py` window_time pins, red on the base. | **PROVEN** | Step 1 red: 2 Python-door pins fail with `AttributeError: ... has no attribute 'window_time'` (see §1). Step 3 green: all 4 `window_time` pins pass (see §4) — values, schema, signature, and the SQL `MISSING_AGGREGATION` refusal. Remediation green (see §8): plain-struct refusal on both doors with the legacy text, grouped answers kept, opaque-scan residual pinned under WIN-3. pins: fnp-win-1/C-003 |
| C-004 | `session_window` answers the static-gap, wide-gap, dynamic-gap, and schema cells on the Python door and the grouped SQL cell. | `test_fnp_win_1.py` session_window pins, red on the base. | **PROVEN** | Step 1 red (see §1). Step 4 green: all 4 session pins pass on the rebuilt release native (see §5) — static gaps (5 and 30 minutes), schema, dynamic gap, SQL groupby — plus the signature pin under C-001. Audit round adds the residual value pins (NTZ sessions, DST sessions, null-drop) under the same pins (see §7). Remediation green (see §8): exact-gap merge pinned, dynamic null/zero/negative drops plus calendar-month answers, DATE sessions, nested-session SQL. pins: fnp-win-1/C-004 |
| C-005 | A malformed duration raises `[CANNOT_PARSE_INTERVAL]` and SQL `window_time(window)` outside the grouping raises `[MISSING_AGGREGATION]`, both as `AnalysisException`. | `test_fnp_win_1.py` error pins, red on the base. | **PROVEN** | Step 1 red: the bad-duration pin fails with `AttributeError` (never reaches analysis); the SQL pin fails with `Invalid function 'window_time'` against the `[MISSING_AGGREGATION]` regex (see §1). Step 2 green: `test_window_bad_duration_raises` passes with `[CANNOT_PARSE_INTERVAL]`; step 3 owns the `MISSING_AGGREGATION` green. Step 3 green: `test_window_time_sql_missing_aggregation` passes on both ANSI settings (see §4). Remediation green (see §8): negative/over-slide `startTime` and `slide > window` refuse with Spark's texts on the Python door and in `select`; the SQL `slide > window` shape is the WIN-2 divergence pin. pins: fnp-win-1/C-005 |
| C-006 | Sliding expansion and session grouping are partition-independent (two-partition Rust inputs). | Rust tests beside each rule. | **PROVEN** | Step 4 green: `static_gap_sessions_match_on_two_partitions` and `sliding_groupby_is_correct_on_two_partitions` pass (see §5). Remediation green (see §8): `dynamic_gap_matches_on_two_partitions` covers the new end-chaining under hash partitioning. pins: fnp-win-1/C-006 |
| C-007 | No regression: the touched suites stay green. | Step gates in §2. | **PROVEN** | Step-1 commit touches tests, fixture, and ledger only; `test_functions_c.py` still asserts the three names absent (they are). Step 2 green: `cargo test -p repark-functions` 484 passed / 1 ignored, focused pytest trio green except the owned step-3/4 reds, `make verify` exit 0 (see §3). Step 3 green: `cargo test -p repark-functions` 489 passed / 1 ignored, trio 28 passed with only the 5 owned step-4 reds, `make verify` exit 0 (see §4). Remediation green (see §8): `cargo test -p repark-functions` 590 passed / 1 ignored, `repark-spark --lib time_window` 16 passed, Python quartet 123 passed, parity trio 71 passed, example coverage executes, `ruff format --check` clean, `make verify` exit 0. |
| C-008 | Registry rows for every residual, `map.md` lockstep in every touched directory, ledger verdicts final. | Step 5 gates. | **PROVEN** | Step 5 green (see §6): no divergence found so no registry rows; the 8 touched maps are lockstep in the same commit; verdicts final with the attestation below. Remediation green (see §8): WIN-1 rescoped to static literals, WIN-2 (SQL `slide > window`) and WIN-3 (`window_time` over opaque structs) added with pins; `tests/map.md`, both `time_window` maps, and both file-size gate tables move in the same commit. pins: fnp-win-1/C-008 |
| C-009 | Zone cells answer on both doors: UTC and America/New_York tumbling buckets, including the NY wall rendering of epoch days. | `test_resid_zone_tumble` over the `R-zone-tumble-*` cells. | **PROVEN** | Audit round green (see §7): all 8 zone cells match; the differential predecessor pin is deleted. pins: fnp-win-1/C-009 |
| C-010 | DST sliding/grid cells answer on both doors wherever the cell has both, including the wall-clock skip rendering. | `test_resid_dst_sliding` over the `R-dst-*` cells. | **PROVEN** | Audit round green (see §7): all 6 DST cells match; the differential predecessor pin is deleted. pins: fnp-win-1/C-010 |
| C-011 | Month/year `window` durations and slides refuse with Spark's `[_LEGACY_ERROR_TEMP_3231]` condition and message. | `test_resid_month_refusal` over the `R-month-*` / `R-year-*` cells. | **PROVEN** | Audit round green (see §7): all 4 error cells match condition and echo; the refusal went red pre-fix and green post-fix. pins: fnp-win-1/C-011 |
| C-012 | `timestamp_ntz` inputs answer with non-null tz-naive-microsecond structs on both doors. | `test_resid_ntz_tumble` / `test_resid_ntz_schemas` over the `R-ntz-*` / `R-*-ntz-*` cells. | **PROVEN** | Audit round green (see §7): all 8 NTZ cells match, types asserted in Arrow. pins: fnp-win-1/C-012 |
| C-013 | Zero and negative `session_window` gaps answer an empty result, not a refusal. | `test_resid_session_empty_gaps` over `R-session-zero-gap` / `R-session-neg-gap`. | **PROVEN** | Audit round green (see §7): both cells match (schema plus zero rows); the refusal went red pre-fix and green post-fix. pins: fnp-win-1/C-013 |
| C-014 | A `startTime` with `abs(start)` at or above the slide refuses with Spark's `DATATYPE_MISMATCH` constraint text. | `test_resid_start_gt_slide` over `R-start-gt-slide`. | **PROVEN** | Audit round green (see §7): condition and micros rendering match; the answering shape went red pre-fix and green post-fix. pins: fnp-win-1/C-014 |
| C-015 | A month/year `session_window` gap is an expected divergence: RePark refuses where Spark answers a calendar-month session. | `test_resid_session_month_diverges` against `R-session-month-gap`. | **PROVEN** | Audit round green (see §7): the refusal is pinned against the answering cell with a `WIN-1` registry row naming the calendar-arithmetic seam. pins: fnp-win-1/C-015 |

## 1. Step-1 red run (2026-09-15, base `1aa95356`)

`.venv/bin/python -m pytest python/repark/tests/test_fnp_win_1.py -q -p no:cacheprovider`
on the base tree: **18 failed, 0 passed.** Distinct failures, each the honest
absent-name signal:

- `AttributeError: module 'repark.spark.functions' has no attribute 'window'`
  (7: signature pin plus all 6 Python-door window pins).
- `AttributeError: ... has no attribute 'window_time'` (2 Python-door pins).
- `AttributeError: ... has no attribute 'session_window'`
  (4: signature plus 3 Python-door pins).
- `AnalysisException: Schema error: No field named window.start` (SQL
  tumbling group-by — the bare `window` name resolves to nothing, so the
  D-1-adjacent SQL aliasing work is required, not just a UDF).
- `Error during planning: Invalid function 'window'` (SQL sliding schema).
- `Error during planning: Invalid function 'window_time'` (SQL
  `MISSING_AGGREGATION` pin — regex mismatch, red as required).
- `AnalysisException: Schema error: No field named session_window.start`
  (SQL session pin).

The fixture `python/repark/tests/fnp_win_1_spark_oracle.json` is a byte-equal
copy of `/tmp/oc-worker/pa-win/o245_spark_oracle.json` (Spark 4.1.2, 222
cells); the pins read their expected rows from it, so the copy is load-bearing.

## 2. Step gates

- Step 1: the red run above, plus `make check-map-sync check-ledgers
  check-ledger-grammar` at commit time.
- Step 2: `CARGO_BUILD_JOBS=12 cargo test -p repark-functions`, the release
  native build, `.venv/bin/python -m pytest
  python/repark/tests/test_fnp_win_1.py
  python/repark/tests/test_functions_split_identity.py -q -p no:cacheprovider`,
  `make py-lint py-format-check check-lib-py check-map-sync check-ledgers
  check-ledger-grammar spell-check`.

## 3. Step-2 green run (2026-09-15, run 16a, on `d711bf08` rebased to `11ae1595`)

The carry-over tree needed one release rebuild (`uvx maturin@1.14.1 develop
--release` in `python/repark`, exit 0) because the rebase had left a stale
native module in `.venv` (`PyColumnParts` lacked `field_join_sql`). After it:

- `.venv/bin/python -m pytest python/repark/tests/test_fnp_win_1.py -q
  -p no:cacheprovider`: **9 passed, 9 failed.** Every `window` cell is green
  (list in C-002); the 9 reds are the owned step-3 cells (`window_time`
  signature, values, schema, SQL `MISSING_AGGREGATION`) and step-4 cells
  (`session_window` signature, static gaps, schema, dynamic gap, SQL groupby).
- `cargo test -p repark-functions`: **484 passed, 0 failed, 1 ignored.**
- `.venv/bin/python -m pytest python/repark/tests/test_functions_split_identity.py
  python/repark/tests/test_functions_c.py -q -p no:cacheprovider`:
  **15 passed.**
- `make verify`: exit 0.
- Comment-ban grep over `git diff origin/main..HEAD`
  (`^\+\s*(//|#(?!\[|!\[| noqa))` on `*.rs *.py *.toml *.sh *.yml`): **0 hits.**

Rust-first note (one line): `window` lands as the `SparkTimeWindow`
analyzer rewrite plus the `window` / `__repark_window_starts__` UDFs in
`crates/repark-functions`, the SQL-door staging in `crates/repark-spark`, and
the `field` join renderer in `crates/repark-python`; the facade
`functions_window.py` only binds names and argument shapes.

## 4. Step-3 green run (2026-09-15, run 16a)

`window_time(windowColumn)` lands as the `window_time` scalar UDF in the new
`crates/repark-functions/src/spark_window_time.rs` (window `end` minus one
microsecond in the end field's timestamp type, NULL struct to NULL,
non-struct arguments refuse loud), registered through the existing
`spark_time_window::functions()` loop; the facade wrapper is one line through
`_scalar` plus a `dispatch_json` arm, and `INSTALL_NAMES` grows to
`("window", "window_time")`.

The SQL-door `MISSING_AGGREGATION` refusal needs two halves. DataFusion's
planner lifts the grouped `window_time(window)` call into an outer projection,
so by analyzer time it is indistinguishable from the legal post-aggregation
Python-door use — the refusal therefore fires at staging in
`crates/repark-spark/src/time_window.rs`, which still sees the grouped shape
and refuses a `window_time(...)` call in the projection / having / order-by
with the oracle text. A second analyzer rule (`SparkWindowTimeGrouping` in
`crates/repark-functions/src/analyzer/time_window.rs`) refuses the same call
placed directly in an aggregate expression, which only DataFrame-built plans
can still produce. Both quote the found call, so the pin text
`window_time(window)` renders verbatim.

- `.venv/bin/python -m pytest python/repark/tests/test_fnp_win_1.py
  python/repark/tests/test_functions_split_identity.py
  python/repark/tests/test_functions_c.py -q -p no:cacheprovider` on the
  final release native: **28 passed, 5 failed** — all 4 `window_time` pins
  green (signature, values, schema, SQL refusal on both ANSI settings); the 5
  reds are the owned step-4 `session_window` cells.
- `cargo test -p repark-functions`: **489 passed, 0 failed, 1 ignored**
  (5 new: end-minus-1µs incl. a two-partition run, NULL, non-struct refusal,
  in-aggregate refusal, in-projection answer).
- `cargo test -p repark-spark time_window`: 7 passed (3 new refusal pins).
- `make verify`: exit 0.
- Comment-ban grep over the staged diff: 0 hits.
- `check_lib_rs` repark-functions ceiling 180 → 182 via sanctioned out (2)
  with the stated reason in the row (`pub mod spark_window_time;` + one
  analyzer-rules push, measured 182); `lib-py` and `rust-file-size` clean.

Known boundary for step 5: the staging scan covers function arguments,
casts, comparisons, `IN`/`BETWEEN`/`LIKE`, `CASE`, and tuples — a
`window_time` call buried deeper (struct literal, lambda) would answer
instead of refusing and earns a registry row if Spark refuses it.

## 5. Step-4 green run (2026-09-15, this round)

Rust-first note (one line): `session_window` lands as the `SparkSessionWindow`
analyzer rewrite (lag / new-session flag / running sum / min-max sessionize)
plus the GROUP-BY-only `session_window` marker and the
`__repark_session_gap__` / `__repark_ts_micros__` /
`__repark_session_assemble__` UDFs in `crates/repark-functions`, the SQL-door
staging in `crates/repark-spark`, the `PyColumnParts.session_window` entry in
`crates/repark-python`, and the thin `session_window(timeColumn, gapDuration)`
wrapper in `functions_window.py` (string or Column gap, `INSTALL_NAMES` grows
to `("window", "window_time", "session_window")`).

Three analyzer fixes this round, each red-then-green against the pins:

- Marker discovery and stripping assumed a `SubqueryAlias` input, but the
  planner flattens `SELECT *` derived tables into a bare `Projection`, so the
  staged marker never fired and the trap refused it. Both now descend through
  `Projection` / `SubqueryAlias` chains (`staged_marker_shape_sessionizes`
  red with the trap text, green after).
- The staged marker's time column carries the base-table qualifier while the
  stripped input exposes the subquery qualifier (`No field named
  ...fnp_win_1_frame.ts` on the SQL pin). Session specs rebase their column
  references onto the input schema by unqualified name; ambiguous names are
  left untouched and refuse later and loud.
- Parent projections keep analyzer-resolved qualified `session_window`
  references after the aggregate rewrites (`No field named
  __repark_windowed.session_window`). A `Projection` arm remaps stale
  qualified session refs to the rewritten output, mirroring the window side's
  `remap_dangling_window_refs`, but only when the name is unambiguous in the
  input schema — a join of two real `session_window` columns keeps resolving
  qualified.

Size: the rule file reached 1317 lines against the 1000 default, so step 4
split it at the cohesive boundary into `analyzer/time_window/mod.rs` (window
rules, 913) and `analyzer/time_window/session_window.rs` (session rule, 555)
— sanctioned out (1). No baseline moved (`analyzer.rs` still 1142, `lib.rs`
ceiling 182 per D-8); `SparkSessionWindow` stays reachable at its old path
through one re-export, so `registration.rs` is untouched by the split.

Step-4 evidence (rebuilt release native, `uvx maturin@1.14.1 develop
--release` in `python/repark`, exit 0):

- `.venv/bin/python -m pytest python/repark/tests/test_fnp_win_1.py -q
  -p no:cacheprovider`: **18 passed** — every oracle cell, including the
  `session_window` signature pin and the SQL groupby pin.
- `cargo test -p repark-functions`: **501 passed, 0 failed, 1 ignored**
  (17 `analyzer::time_window` tests incl. the staged-marker shape and both
  two-partition inputs; step 5 adds the month-gap refusal and the
  join-ambiguity guard pins for 19).
- D-2 fence holds: the diff touches nothing under `dataframe/**`,
  `column.py`, `session/**`, `catalog.py`, `types.py`, `repark-core`, or the
  SQL parser.

## 6. Step-5 verdicts (2026-09-15, this round)

Residual measurements on the release native (scratch probes
`/tmp/residual_probe.py`, `/tmp/zone_probe.py` — not committed; no JVM on
this box meets Spark 4.x, so no new live-oracle cells exist and none were
fabricated):

- America/New_York vs UTC over the same instants: identical session buckets
  (`k1 [1704103650, 1704104220]`, `k2 [1704105060, 1704105360]` both zones).
  The first NY run was vacuous — `getOrCreate` reused the UTC session with an
  unapplied-timeZone warning — and was rerun with `stop()` between sessions.
  Same wall strings shift buckets by exactly the zone offset (correct
  zone-wall parsing). Pin: `test_session_window_zone_rules_match_utc`.
- Sliding `window(ts, '1 day')` over 2024-03-09..11 instants: identical
  UTC-day buckets under both zones (epoch grid, no DST distortion). Pin:
  `test_sliding_window_dst_grid_matches_utc`.
- `session_window(ts, '1 month')` refuses loud; Spark's exact refusal text
  is unconfirmed without a live oracle, so the pin is a contract pin, not a
  parity pin. Pin: `month_gap_session_refuses`.
- `timestamp_ntz`: answers directly, and every session pin already runs over
  tz-naive micros, so the C-004 static pins double as the NTZ evidence. Pin:
  shared with C-004.
- No divergence found, so no divergence-registry rows.

(Superseded by §7: the audit round replaced every differential pin above
with live-cell pins, matched the window month text exactly, and rowed the
session-month gap as `WIN-1`. The zone/DST measurements themselves held.)

Census: `INSTALL_NAMES` carries `session_window`;
`test_functions_c.py` drops it from the deferred-absent list;
`test_functions_split_identity.py` uniqueness holds (D-10).
Examples: `make check-example-coverage` green (112 backlog / 2 exceptions;
`functions_window.py` sits outside the walked closed set and widening that
set is an owner decision, so it stays untouched).
Maps (lockstep, same commit): `crates/repark-functions/src/analyzer/map.md`,
`crates/repark-functions/src/analyzer/time_window/map.md` (new, for the split
module), `crates/repark-functions/src/map.md`,
`crates/repark-spark/src/map.md`, `crates/repark-python/src/column/map.md`,
`python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`,
`task/ledgers/staging/map.md` (plus one `functions_byname.py` row in the
spark map).

Full-suite footnote: `make py-test-facade` ran **7018 passed, 373 skipped,
1 failed** — the failure is `test_fnp_misc_1_byname_allowlist_covers_facade`,
which is step-2 debt, not step-4 fallout: it fails identically on the clean
`f33f807f` tree (verified via `git stash`), because `window` never joined
`FACADE_ONLY_ROUTINE_NAMES`. Step 5 settles it by adding the two
facade-routed names the scalar dispatch does not carry (`window`,
`session_window`; `window_time` answers through `_scalar` and stays out):
`test_fnp_misc_1.py` is **64 passed** after, and the tuple's only other test
consumer (`test_functions_split_identity.py`) is green. No full re-run: the
tuple feeds one assertion plus render-only error text with no other test
coverage.

Final gates: `make verify` exit 0; `cargo test --workspace` all suites ok
(`repark-functions` 503 passed / 1 ignored); comment-ban grep over the diff
0 hits (only required `/// pins:` doc citations); `make check-map-sync
check-ledgers check-ledger-grammar` exit 0 (post-write rerun recorded in the
commit evidence).

## 7. Audit round (2026-09-15, run 16a findings F-2/F-3)

F-2: the five `/// pins:` citations beside the rule are deleted (the ban
covers `///`), each citation moved into the matching `map.md` row; the moved
`/// Return analyzer rules` docstring in `registration.rs` is deleted with
them. The pre-commit ban grep
(`git diff origin/main -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P
'^\+\s*(//|#(?!\[|!\[| noqa))'`) prints nothing; Python `"""pins:"""`
docstrings stay (declarative test metadata, not `#` comments).

F-3: the 39 run-16a residual cells (Spark 4.1.2, recorder
`oracle_win_resid.py`) are appended to the fixture (222 run-15a cells
intact, 261 total). Resid frames are built with `createDataFrame`, not the
recorder's SQL `TIMESTAMP` literals: SQL timestamp literals through temp
views hit an unrelated engine gap (`expected Timestamp(ns) but found
Timestamp(µs, "UTC")` on a plain `SELECT *`, no window involved — outside
the D-1 fence, left alone). The instants match the recorder's, which is the
pin-relevant property. Sessions are one fresh builder session per zone;
`conf.set` on a live session and `getOrCreate` reuse do not apply a zone,
which twice produced vacuous all-equal comparisons before the rerun.

Classification per cell (measured, then fixed or rowed):

- Match as-is (20 window zone/DST/NTZ cells): epoch buckets with zone-wall
  rendering, including the DST skip (`01:00 → 03:00`) and NTZ zone-ignorance.
- Fixed in Rust, red-then-green (5 pins): month/year window refusal now
  emits `[_LEGACY_ERROR_TEMP_3231] Intervals greater than a month is not
  supported (…)` with the duration echo; `startTime` with `abs(start)` at or
  above the slide refuses with the `DATATYPE_MISMATCH` constraint text and
  micros rendering; zero/negative session gaps answer an empty result via a
  false filter; DATE input is accepted through the UDF coercion (DataFusion
  inserts the DATE→timestamp cast); window grouping drops null timestamps
  with an `IsNotNull` filter on the grouping input (projection use still
  answers nulls through, unchanged). The red proof: all five pins fail on
  the pre-fix tree (debug build) and pass on the release build with the
  fixes; the session-month divergence pin stays green on both by design.
- Expected divergence (1 cell): `session_window` month gaps refuse where
  Spark answers a calendar-month session — `WIN-1` row in
  `docs/spark-sql-iceberg-parity.md` §7 names the calendar-arithmetic seam.

Evidence (rebuilt release native): `test_fnp_win_1.py` **31 passed** (18
oracle + 13 residual); `cargo test -p repark-functions --lib analyzer`
**61 passed**; the month-gap Rust contract test now asserts the session
refusal text. C-007/C-008 verdicts stand with the rebase-run numbers below.

## 8. Remediation round (2026-09-15, run 16a critic-logic + S2-21 perf)

The 34 run-16a critic cells (`C-L*`, recorder `oracle_win_crit.py`, Spark
4.1.2) are appended to the fixture (261 cells intact, 295 total), and 13 pins
cover them (12 critic pins plus the WIN-3 opaque-struct pin).

Red run on the unfixed tree (release native from `55683227`):
`.venv/bin/python -m pytest python/repark/tests/test_fnp_win_1.py -q
-p no:cacheprovider -k crit` → **8 failed, 4 passed.** The 4 greens pin
already-correct behavior (exact/near-gap merge, sliding select values,
grouped `window_time`). The 8 reds, each the honest missing-behavior signal:
null rows kept in plain selects (both doors, tumbling and sliding);
`'-2 minutes'` / `'0 seconds'` startTime refused with
`CANNOT_PARSE_INTERVAL`; dynamic null/zero/negative gap rows kept, the month
gap refused, and the null-cast gap crashed in Arrow on a null struct end;
`slide > window` answered silently; nested/CTE/UNION SQL failed with `No
field named`; DATE sessions failed coercion; `window_time(named_struct(...))`
did not raise.

Green run on the rebuilt release native: `test_fnp_win_1.py` **44 passed**;
`cargo test -p repark-functions` **590 passed / 1 ignored** (19 new: signed
offsets, both constraints, null projections, session end/month/drop UDF
cases, exact-gap merge, dynamic drop/month/two-partition sessions,
DATE sessions, plain-struct refusal; the `repark-spark` nesting pins are
counted under `repark-spark`);
`cargo test -p repark-spark --lib time_window` **16 passed** (5 new nesting
pins); Python quartet **123 passed**; parity trio **71 passed**; example
coverage executes; `ruff format --check .` clean; `make verify` exit 0.
Comment-ban grep over the staged diff: 0 hits.

Perf (release native, reviewer's bench, 2M TIMESTAMP rows / 1K keys, median
of 5 `.toArrow()`, before `55683227` vs after; the middle run was discarded
— box contention moved even the untouched `groupby_k` 5x, and the rerun
restored it to baseline):

| workload | before (s) | after (s) | ratio |
|---|---|---|---|
| df_window_slide_10m_1m | 0.4759 | 0.3256 | 0.68 |
| df_window_slide_10m_10s | 2.6249 | 1.8879 | 0.72 |
| sql_window_slide_10m_1m | 0.4957 | 0.3231 | 0.65 |
| sql_window_slide_10m_10s | 2.5969 | 1.7824 | 0.69 |
| df_window_tumbling_10m | 0.0344 | 0.0242 | 0.70 |
| sql_window_tumbling_10m | 0.0328 | 0.0392 | 1.20 |
| df_session_sorted | 0.1169 | 0.1392 | 1.19 |
| df_session_shuffled | 0.1115 | 0.1386 | 1.24 |
| sql_session | 0.1249 | 0.1287 | 1.03 |
| SELECT 1 (staging hot path) | 0.00044 | 0.00043 | 1.00 |

Sliding wins 28–35% from the streamed starts, pre-sized typed builders, and
the single batch downcast. Tumbling wins on the DF door; the SQL-door
tumbling 1.20x sits inside that 30 ms workload's noise (its min beats the
before median). Sessions cost ~1.0–1.2x on this 1-partition bench from the
hash step, while the plan drops the gather-to-one: before
`Repartition: RoundRobinBatch(1)` → `CoalescePartitionsExec`, after
`Repartition: Hash(k) partition_count=64` with no coalesce. RSS high-water is
within run-to-run noise on this box, so no memory claim is made beyond the
removed staging (no 16-byte `Option` slots, no second scaled copy at
microsecond unit), which holds by construction.

Out of scope observed, not fixed: the chained alias-then-`getField` select
over a projection-built struct fails DataFusion `push_down_leaf_projections`
for every struct source (`F.struct` reproduces it, no window involved) —
door-plan territory under the D-2 fence. The bench harness overwrote the
reviewer's `explain_*.txt` snapshots in `/tmp/oc-worker/pa-win/rustperf/`;
they regenerate with `bench.py` and no repo file is affected.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-win-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every charter clause was re-derived from runs, not read off prior rounds. D-1 holds (rules beside the analyzer, registered through analyzer_rules(), no repark-core or parser edits in the diff); D-2 holds (nothing under dataframe/**, column.py, session/**, catalog.py, types.py in the diff); the shape rule holds per name (F.session_window present, both doors answer, error classes match the oracle). C-001/C-004/C-006 closed only after their pins went green on the rebuilt native in this round.
      artifacts: [task/ledgers/staging/fnp-win-1-ledger.md, python/repark/tests/test_fnp_win_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Null timestamps (null_time_rows_leave_no_session plus the resid null-drop cells on both functions), malformed durations (oracle CANNOT_PARSE_INTERVAL pin), month/year window durations (C-011 legacy-condition pin), month/year session gaps (C-015 divergence pin), zero and negative session gaps (C-013 empty pins), startTime at or above the slide (C-014 constraint pin), DATE input (resid cells, both doors), pre-epoch rows, non-dividing slides, zone and DST grids (C-009/C-010 cells), a second gap specification per block (refusal pin), dynamic per-row gaps (oracle pin), two-partition inputs (C-006 pins), and both ANSI settings on every SQL pin were all exercised. Empty input needs no pin: zero rows produce zero sessions with no window state touched.
      artifacts: [crates/repark-functions/src/analyzer/time_window/mod.rs, python/repark/tests/test_fnp_win_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal path is pinned with its text: CANNOT_PARSE_INTERVAL and MISSING_AGGREGATION against the oracle, the month/year legacy condition and echo and the startTime constraint text against the resid error cells, one-spec-per-block and the session month-gap text as contract pins, and the GROUP-BY-only marker trap, which was observed firing (staged_marker_shape_sessionizes red pre-fix) rather than assumed present.
      artifacts: [crates/repark-functions/src/analyzer/time_window/session_window.rs, crates/repark-functions/src/analyzer/time_window/mod.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The rules are stateless (derive Default, no fields, no global state). The sessionize leg repartitions to one partition to make the lag/flag/sum ordering total; the two-partition pins drive exactly that assumption. The zone probes caught a session-reuse ordering bug (getOrCreate kept the UTC session and warned) and reran isolated with stop() between sessions.
      artifacts: [crates/repark-functions/src/analyzer/time_window/session_window.rs, python/repark/tests/test_fnp_win_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no credentials, no network, no secret handling. The staging rewrite moves parsed AST nodes and never interpolates values into SQL text; display strings are render-only.
    - id: AT-6
      status: ATTACKED
      evidence: The output shape struct<start:timestamp,end:timestamp> is type-pinned on both doors (Python schema pin plus to_arrow assertions, Rust session_bounds over struct arrays). Values are oracle-pinned to the microsecond. The fixture is the step-1 byte-equal oracle copy plus the 39 appended run-16a residual cells (run-15a portion intact). NTZ grouping columns are asserted in Arrow as non-null structs of non-null tz-naive micros (C-012), with values against the NTZ cells.
      artifacts: [python/repark/tests/test_fnp_win_1.py, python/repark/tests/fnp_win_1_spark_oracle.json]
    - id: AT-7
      status: N/A
      justification: Not system-breaking. The sessionize leg adds bounded window sorts over the grouped input plus eight internal columns projected away at the end; the single-partition repartition is a stated serialization cost of session semantics, and no input grows without bound.
    - id: AT-8
      status: ATTACKED
      evidence: Oracle error texts are quoted verbatim (C-005, C-011, C-014). The one DataFusion shape presumed early (SubqueryAlias-wrapped staging input) was falsified by plan-debug — the planner flattens SELECT * subqueries — and discovery plus stripping now handle both shapes, measured on the VALUES frame and the real-table SQL pin. The audit round corrected two more premises: SQL TIMESTAMP literals through temp views fail independent of window code (outside the D-1 fence; resid frames use createDataFrame with identical instants), and zone configs apply only at session build (shared-session comparisons were vacuous twice before the fresh-session rerun). The session-month refusal text is RePark's own, pinned as a divergence (C-015), not presumed equal.
      artifacts: [crates/repark-functions/src/analyzer/time_window/session_window.rs, task/ledgers/staging/fnp-win-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal names the offending input (the duration text, the second specification, the legal GROUP BY use in the trap text). Sections 5 and 6 paste the exact commands, counts, and probe outputs, so a red pin is diagnosable without rerunning the round.
      artifacts: [task/ledgers/staging/fnp-win-1-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Every branch the diff adds has a nameable pin: staged marker in a flattened projection, staged marker behind a subquery alias, qualified time columns, qualified parent references (plus the ambiguity guard, pinned by the two-column join), dynamic gaps, nulls, month gaps, zone grids, two partitions, legacy versus session month refusals, the startTime constraint, the non-positive-gap empty filter, DATE coercion, and the grouping null-drop. Mutation was observed, not staged: removing the strip re-fires the trap on the staged test, removing the rebase re-breaks the SQL pin, disabling the guard re-breaks the join pin, and reverting the five audit-round fixes re-breaks exactly the five resid fix pins (debug build red, release build green) — all observed in-round.
      artifacts: [python/repark/tests/test_fnp_win_1.py, crates/repark-functions/src/analyzer/time_window/mod.rs]
  complete: true
```

## 9. Verification round 2 (2026-09-15, run 16a critic-logic cycle 2)

Second Grok critic (`crit2/report.md` over the pre-rebase head) plus the
orchestrator's live re-recording (`win_crit2_spark_oracle.json`, recorder
`oracle_win_crit2.py`, 25 `C2-*` cells, PySpark 4.1.2) and dispositions
(`dispositions-r2.md`). Three finding commits plus this paper commit:

- L-003 P1 (oracle-upgraded): `C2-L003-discriminator` puts
  `(10:00,'60 minutes'), (10:10,'1 minute'), (10:20,'5 minutes')` in one
  session to 11:00 count 3; RePark answered two. The per-row leg now flags
  against `max(end)` over preceding rows per key (D-31). Red pre-fix
  (two sessions `[10:00,11:00)` c=2 + `[10:20,10:25)` c=1), green post-fix
  on both doors; the Rust two-batch pin fails with the `lag(end)` leg
  restored and passes with the running maximum.
- L-001 P1: `windowed_column` recursed only Projection / SubqueryAlias
  (D-32). Red pre-fix (the struct shapes answer instead of refusing from
  the filter shape on, `DID NOT RAISE`), green post-fix on every
  `C2-L001-*` cell; the grouped-window-behind-filter /
  WHERE answers and the WIN-3 residual stay pinned. The Rust unary-shape
  pin fails with the catch-all restored to `true`.
- L-002 P2: pins only — all ten month-end/leap cells green pre-fix and
  post-fix. Naive `createDataFrame` datetimes land as session-zone walls
  (measured 10:00 → 10:00 in a UTC session); the pins build the recorder's
  shifted UTC walls directly (January +5 h, September +4 h, D-33).
- L-004 P3: two specs died as duplicate-name (Python) / RePark-only text
  (SQL). Both doors now refuse with Spark's `[_LEGACY_ERROR_TEMP_1039]`
  text (D-34). Red pre-fix on both doors, green post-fix.

Gates (this round, release native rebuilt from the head tree): `cargo
test -p repark-functions time_window` 47 passed; `session_window` 8
passed; `cargo test -p repark-spark --lib time_window` 16 passed; the
pytest quintet (`test_fnp_win_1`, `test_functions_split_identity`,
`test_functions_c`, `test_fnp_misc_1`, `test_fnp_bitmap_facade_1`) 161
passed; the parity trio (example coverage, api freeze, CAP-1) 71 passed;
both size gates clean with no new exception row (38/32 counts hold);
`check_example_coverage --require-execute` green (1053 names, 940
covered, 112 backlog, 1 exception, 240 examples); `ruff format --check
.` clean; `make verify` exit 0. Follow-up inside this round: clippy
pedantic `missing_errors_doc` on the new `check_single_session_spec`
(one `# Errors` doc section, no behavior change) and `cargo fmt` /
`ruff format` reflows, all folded before the head commit.
