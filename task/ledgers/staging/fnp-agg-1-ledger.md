# Charter ledger — FNP-AGG-1 · the missing aggregates (1.5 Spark-parity campaign)

**Date:** 2026-09-15 · **Branch:** `feat/fnp-agg-1` · **Base:** `bee2cde3` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `EX-FN-9` flips to FIXED in a later step; one section-7 row per residual
divergence measured there.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign measured fourteen aggregate names against live
PySpark 4.1.2 (`/tmp/oc-worker/pa-agg/agg_misc_spark_oracle.json`, 2026-09-14): eleven are
absent from the facade on both doors and three (`kurtosis`, `skewness`, `mode`) are
engine-gap stubs. This unit builds the Rust kernels and facade names and pins every cell.

**Not in this round (run 16a, step 1 only).** No product code, no cargo: this round writes
the ledger, copies the fixture, and lands the red-first pins. Steps 2–5 (kernels, facade,
`count_min_sketch`, registry) are later rounds on build clones.

## Decisions (orchestrator rulings under G-2, copied from the card)

| Id | Ruling |
|---|---|
| D-1 | Kernels in `crates/repark-functions/src/` (new modules listed in its `map.md`), registered in `register_all` (`lib.rs`) for the SQL door. The facade reaches a new aggregate through the aggregate-kind match tables in `crates/repark-python/src/column/function_dispatch.rs` (`unary_aggregate_udaf`, `binary_aggregate_udaf`) — ADD arms only; run 15c converges existing rows there, so never edit or reorder existing arms. |
| D-2 | Never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py`, the SQL parser/dialect. `cube`/`rollup` live in GroupedData (run 15b's files): use them as they are. |
| D-3 | Registry: flip `EX-FN-9` (kurtosis/skewness/mode) to FIXED with the new pins; add one section-7 row per residual divergence measured, each with a pin. |
| D-4 | Pins: `python/repark/tests/test_fnp_agg_1.py`, both doors, parametrized over the oracle cells, red first on the base tree with the red summary pasted into the ledger; Rust unit tests beside each UDAF. Floats compare exactly (Spark's value); nullability is pinned except the VALUES-derived grouping key. |
| D-5 | Performance: each UDAF implements `merge_batch`/`state` (multi-partition safe); `retract_batch` is NOT required. No per-row allocation in `update_batch` beyond what the algorithm needs. |

## Step 2 (run 17a, 2026-09-16) — the round's seven names

Run-17a rulings applied: R-17a-18 (read `registration.rs`; the UDAFs register through
`aggregate::functions()`, which `register_all` already calls — no analyzer rule added or
moved); R-17a-19 with owner Q-15c-4 (ceilings only move down: five private `mod` lines plus
`pub use` re-exports through `aggregate.rs`, ADD-only dispatch arms, no `check_lib_py`
EXCEPTIONS row — `functions_expr.py`/`functions.py` baselines ratchet to their measured
counts, `check_example_coverage.py` pays its two registry lines by compacting its docstring
prose); R-17a-17 (no bare default-ceiling literal in any `map.md`); R-17a-3 (EX-0 is main's
1074 plus this round's own 4: `any_value`, `max_by`, `min_by`, `product`).
Reviewer lessons honored: every UDAF loops all rows of every state array in `merge_batch`
(the FNP-11B index-0 drop cannot recur), proven by a two-partition merge test per UDAF;
no accumulator carries a sticky flag except `mode`'s deterministic bit, which merges by OR
and is pinned carrying across partitions. The audit-repark-parity skill is loaded; the
recorded 4.1.2 fixture cells are the oracle (no live Spark in this clone).

What landed: `any_value.rs`, `max_min_by.rs` (plus the shared `OrdKey` ordering),
`moments.rs`, `mode.rs`, `product.rs` in `crates/repark-functions/src/` with Rust tests
beside each UDAF; `aggregate::functions()` registrations; ADD-only dispatch arms;
the SQL-door unknown-routine mapping to `[UNRESOLVED_ROUTINE]` in the binding's
`to_py_err`; facade module `functions_agg_1.py` plus the `kurtosis`/`skewness`/`mode`
destub in `functions_expr.py`; census updates (batch4 stubs, split-identity tail, facade-only
list, example `agg_misc.py` with inventory/backlog, EX-0 1078).

Per-name step-2 pins (both doors, oracle-exact): `any_value` python 6/6, SQL 0/2 —
BLOCKED on run 17c (below); `max_by` 12/12; `min_by` 10/10; `product` 12/12 (python 6,
SQL `UNRESOLVED_ROUTINE` 6); `kurtosis` 8/8; `skewness` 8/8; `mode` 10/10.
Signatures green for all seven. The other nine card names stay red for later steps.

Blocked seam (exact): the `any_value` SQL cell selects `any_value(v)` twice and Spark
names both outputs `any_value(v)` — the engine now renders both names correctly, but the
planner rejects the query (`Projections require unique expression names`). Duplicate
select-item names fail on main for any query (`SELECT 1, 1`, `SELECT g, g` both refuse),
so the fix is duplicate-projection support in run 17c's parser/planner file set, which
this round must not touch. The two cells stay OPEN on that seam; the Python door and the
1- and 2-arg SQL shapes are proven separately.

Contract readings recorded: `skewness`/`kurtosis` answer NULL when the count is below 2
or the variance is 0 (the card's "variance is 0 or one row", per the oracle — no fixture
cell has two rows); non-deterministic `mode` keeps the last value to reach the top count
(the fixture's tie answers 30 for the scan order 10, 20, 30); `mode(s, true)` and the
ordered-set form render Spark's `mode() WITHIN GROUP` display on both doors; the
`WITHIN GROUP` sort key arrives prepended to the UDAF args (measured, read as the
ordered-set contract: the first arg is the data column).

## Owner rulings applied in this round

| Id | Ruling | Applied |
|---|---|---|
| OWNER-2026-08-26 | No comments in code (`//`, `///`, `//!` in Rust, `#` lines in Python/TOML/YAML/shell beyond `# noqa`). | The pin file carries docstrings only; the comment-ban grep in §Gates. |
| OWNER-2026-09-14 | Rust first: every new function lands in Rust with the Python facade a thin wrapper. | Step scope: pins only, no product code either side. |
| OWNER-2026-09-14 | Shape rule: a name is done when it answers PySpark 4.1.2 on BOTH doors or raises Spark's own error class with a dated registry row. | The pins assert both doors plus error conditions; Spark-refused SQL names (`product`, `listagg_distinct`, `string_agg_distinct`, `sum_distinct`) are pinned as `UNRESOLVED_ROUTINE` refusals. |

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | The sixteen card names are present on `repark.spark.functions` with PySpark 4.1.2 signatures, including `sum_distinct` / `sumDistinct` with the `FutureWarning`. | OPEN | Red on the base: `test_facade_names_present_and_exported` fails (`AttributeError: ... has no attribute 'any_value'`); 14 of 16 signature pins fail — only `kurtosis`/`skewness` pass (already `(col)`); `test_sumDistinct_warns_spark_deprecation` fails (no such attribute). Step 2: the seven round names present with oracle signatures (pins green); the other nine names still absent. |
| C-002 | The Python-door value cells answer the fixture rows, types and nullability. | OPEN | Red on the base: 68 of 70 agg-fixture Python cells fail (only the 2 pre-existing `listagg` cells pass); all 12 alias-fixture `sum_distinct`/`sumDistinct` Python cells fail. Per name failed: any_value 6, max_by 8, min_by 6, percentile 6, product 6, listagg_distinct 6, string_agg_distinct 6, grouping_id 4, histogram_numeric 4, kurtosis 4, skewness 4, mode 6, count_min_sketch 2, sum_distinct 8, sumDistinct 4. Step 2: the round's Python cells green (any_value 6, max_by 8, min_by 6, product 6, kurtosis 4, skewness 4, mode 6). |
| C-003 | The SQL-door value cells answer, and Spark-refused SQL names stay refused. | OPEN | Red on the base: all 33 agg-fixture SQL value cells fail and all 4 alias `sum(DISTINCT)` SQL value cells fail. Per name failed: listagg 4, kurtosis 4, skewness 4, mode 4, percentile 3, any_value 2, max_by 2, min_by 2, grouping_id 2, histogram_numeric 2, count_min_sketch 4, sum_distinct 4. Step 2: the round's SQL cells green except the two `any_value` duplicate-name cells, which wait on the run-17c seam (§Step 2). |
| C-004 | The error cells fail with Spark's own error condition on their own door. | PROVEN (step 9: 29/29 green, incl. the 2 alias `sum_distinct` SQL refusals) | Red on the base: all 29 error pins fail (27 agg + 2 alias `sum_distinct`). Per condition failed: `UNRESOLVED_ROUTINE` 10 (product 6, listagg_distinct 2, string_agg_distinct 2), `DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE` 6 (percentile 4, histogram_numeric 2), `UNSUPPORTED_GROUPING_EXPRESSION` 4, `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` 4, `GROUPING_ID_COLUMN_MISMATCH` 2, `CAST_INVALID_INPUT` 1 (percentile over strings, ANSI on). The `UNRESOLVED_ROUTINE` cells fail because RePark does not refuse with Spark's condition today. Step 2: the round's error cells green — `product` SQL refuses 6/6 via the new unknown-routine mapping, `max_by`/`min_by` 3-arg SQL raises `[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]` 4/4. |
| C-005 | Multi-partition merge: a Rust test splits input across two partitions per UDAF. | PROVEN (step 9: kernel two-partition sketch merge pin green alongside the earlier UDAFs) | Step 2: merge tests land for the five new kernel files (`any_value`, `max_by`/`min_by`, `kurtosis`/`skewness`, `mode` unique-max plus deterministic-carry, `product`); each asserts the merged result equals the single-partition one. Remaining UDAFs (percentile, grouping_id, histogram_numeric, count-min-sketch) come with their steps. |
| C-006 | No regression: the touched suites stay green. | OPEN | Step 2: `cargo test -p repark-functions` 726 passed; `test_fnp5_aggregates`, `test_functions_a`, `test_functions_b`, `test_fnp_misc_1`, `test_functions_split_identity`, `test_fn_batch4` green; full parity suite and `make verify` in §Gates (run 17a, step 2). |
| C-007 | Registry (`EX-FN-9` → FIXED, section-7 rows) and every touched `map.md` in lockstep. | PROVEN (step 9: EX-FN-9 FIXED, sketch section-7 row both doors, FNP-AGG-1-18C/18D added, maps lockstep) | Step 2 (maps only; registry flips are card step 5): new modules listed in `crates/repark-functions/src/map.md`, `functions_agg_1.py` in `python/repark/src/repark/spark/map.md`, `agg_misc.py` in `docs/examples/functions/map.md`; inventory +4 rows, backlog −3 (`F.kurtosis`, `F.mode`, `F.skewness` now covered), `BACKLOG_BASELINE` 111→108, EX-0 1074→1078. |

VERDICT: 7 clauses, 3 PROVEN, 4 OPEN, 0 REJECTED (step-9 roll-call above; the only in-card OPEN driver is the R-18a-18 sum carry).

## Step 10 (run 18a, 2026-09-16) — full gates, own reds fixed, 8 reds observed

| Gate | Result |
|---|---|
| `cargo test --workspace` | exit 0 — 3664 passed, 0 failed over 56 suites |
| `cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` | red first (4 own lints), green after the fix below |
| `cargo fmt --check` | green (one import reorder applied via `cargo fmt`) |
| `uvx ruff@0.15.22 check .` / `format --check .` | green / 1018 files formatted |
| comment-ban grep over the slice diff | clean |
| whole facade suite `python/repark/tests` (Q-17a-4) | 9188 passed, 42 failed = 35 card reds (all carried/prior/Q3 per §Step 9) + 7 below |
| whole parity suite `python/repark-parity/tests` (Q-17a-4) | 756 passed, 1 failed (EX-0 below) |

Own reds fixed in this step (all in this slice's files): `count_min_sketch.rs`
test helper `format_collect` (fold plus `write!`) and `unreadable_literal`
(`100_000`); `grouping.rs` two `question_mark` let-else arms (now `?`).
Lib suite stays 792 green after the fix.

Not mine — 8 reds observed, none touched (Q4 asks their disposition):
`test_functions_c` deferred roster (6 names from steps 1–7 now present;
`count_min_sketch` is not in that roster); window-bench absents roster (14
names now resolve; window area, untouched by this slice); EX-0 enumerator
(1092 rows vs the R-18a-1 pinned 1090; this slice adds no examples);
`test_column_parity_1` struct-edit, `test_fnp11b_typeof` owner,
`test_fnp7_try_inversions`, `test_fnp_gen_1` schema_of_csv,
`test_functions_gt2` shuffle — five unrelated areas. C-006 stays OPEN:
the suites covering this slice are green-or-accounted, but the two shared
suites are not fully green.

## Gates (run 16a, step 1)

| gate | result |
|---|---|
| `PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_agg_1.py -q` | 162 failed, 4 passed (the 4: `kurtosis`/`skewness` signature pins and the 2 pre-existing `listagg` Python cells) |
| `uvx ruff@0.15.22 check python/repark/tests/test_fnp_agg_1.py` | exit 0, all checks passed |
| `uvx ruff@0.15.22 format --check python/repark/tests/test_fnp_agg_1.py` | exit 0, already formatted |
| comment-ban grep over the staged diff | prints nothing |

## Gates (run 17a, step 2)

| gate | result |
|---|---|
| `cargo test -p repark-functions` | 726 passed, 0 failed (22 new: 3 any_value, 5 max_min_by, 4 moments, 7 mode, 3 product — each file carries its two-partition merge test) |
| `cargo test --workspace` (`make test`) | all suites green (incl. repark-functions 726, repark-python 434) |
| release native (`maturin develop --release`) | rebuilt; step-2 pins re-verified on the final binary |
| `test_fnp_agg_1.py -k "any_value or max_by or min_by or product or kurtosis or skewness or mode"` | 73 passed, 2 failed — both are the `any_value` SQL duplicate-name cells blocked on the run-17c seam (§Step 2); the other nine card names stay red for later steps |
| `test_fnp5_aggregates.py test_functions_a.py test_functions_b.py test_fnp_misc_1.py test_functions_split_identity.py test_fn_batch4.py` | 124 passed |
| `test_ex_0_example_coverage.py test_api_freeze.py test_cap_1_source_file_line_cap.py` | green after ratchets (EX-0 1078; cap-1 mirror rows 1985/2233) |
| `python/repark-parity/tests` (whole suite) | first run 755 passed, 2 failed — both from a stale staging-map row for the completed REGISTRY-16B-1 unit (ledger already in `completed/`); the row is gone in the final tree per the map's own rule ("every other ledger leaves for `../completed/` in its unit's last commit"), after which `ledger-check` (1007 links), `map-sync` (276 maps) and `docs-links` (5679 links) pass, and the dl_4/dl_6 suites re-run 37/37 green. Pre-existing on `origin/main` (verified via `git show`/`ls-tree`, not by a main-tree test run). |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | exit 0 (245 examples incl. new `agg_misc.py`; inventory +4, backlog 108) |
| `uvx ruff@0.15.22 format --check .` / `ruff check .` | 1005 files formatted, all checks passed |
| `make verify` | exit 0 end-to-end on the final tree (was red at `check-ledgers` on the stale staging row above before its removal) |
| comment-ban grep over the staged diff | prints nothing |

## Step 1–2 (run 18a, 2026-09-16) — PYPERF-001: one kernel behind one name

Run-18a rulings applied: R-18a-1 (the three counts are measured in step 9, not here);
Q-17a-1 (this round opens with the two door-split P1s); Q-17a-2 (the literal check is a
kernel argument check in `any_value.rs`, reached identically by both doors; Python holds
only the bool-to-`lit` shape and the display/sql/join spellings); Q-17b-1 (this step is
its own commit). Owner Rust-first and the shape rule hold as in step 2.

What changed: `check_flag_literal` in `crates/repark-functions/src/any_value.rs` refuses a
non-literal second physical arg with Spark's `[_LEGACY_ERROR_TEMP_1210]` from
`accumulator()`; ADD-only `any_value` arms (unary, binary) in `function_dispatch.rs`;
`F.any_value` binds `aggregate("any_value")` / `aggregate_binary("any_value", flag)` with
the flag passed through (a Python bool becomes `lit(bool)` — argument shape only),
`_fold_flag` deleted with every Python branch on the flag's value; `sql_expr` /
`join_sql_expr` name `any_value`, never `first_value`; `window.rs` aliases a windowed
`any_value` UDAF call to Spark's display (`any_value(v) OVER (PARTITION BY …)`; the
default ordered frame renders as Spark prints it). Fixture
`python/repark/tests/fnp_agg_1_p1_spark_oracle.json` copied verbatim (30 cells);
pins in `python/repark/tests/test_fnp_agg_1_p1.py` (exact except the eight ungrouped
`any_value` value cells, which pin schema exactly and the value as membership in the
frame's non-null candidates — Spark's answers there, 5 and 0.0, are partition-scan
order, and RePark answers 10 / 1.5 on its single partition).

Red on the base tree: `test_fnp_agg_1_p1.py` 17 failed, 13 passed (every `any_value`
refusal, the `any_value` window name, and all seven `product` cells fail; the failures
are `python-any_value-{3,4,18,19}`, `sql-any_value-{12,14,27,29}`, `python-any_value-{5,20}`,
`python-product-{7,8,10,11,22,23,26}`).
Green after the fix: 23 passed, 7 failed — all fourteen `any_value` cells green, the
seven failures are exactly the `product` cells step 3 owns. Rust:
`cargo test -p repark-functions --lib any_value` 4 passed (new
`non_literal_flag_is_refused` pins the plan-time refusal for a column flag and for
`CAST(NULL AS BOOLEAN)`).

Rust-first roll-call (logic left in Python, one line each): bool-to-`lit` in
`any_value` (argument shape, Q-17a-2 allows); display/sql/join spellings (names only);
`_thread_origin(column)` (lineage plumbing, mirrors `first`).

Size gate: the two new arms took `function_dispatch.rs` to 1002 of 1000, so
`cast_unsigned_count_to_signed` moved verbatim to `expr_build.rs` (which already
hosts the aggregate builders) with a re-export keeping the `mod.rs` path stable;
the file is back at 993, `mod.rs` untouched at its exact 1014, arms untouched per
D-1. No EXCEPTIONS row, no baseline raised (Q-15c-4). The move's doc line stays
behind: the comment ban rejects any added `///` line, moved or not, so the reason
lives in `crates/repark-python/src/column/map.md`.
R-18a-1, first count, set early (the lib-py gate blocks the commit otherwise):
`functions_expr.py` baseline 2233 → measured 2211 in `scripts/check_lib_py.py` and
`test_cap_1_source_file_line_cap.py`; `functions.py` still measures its 1985.

## Step 3–4 (run 18a, 2026-09-16) — PYPERF-002: the engine name in the SQL spelling

Run-18a rulings applied: Q-17a-1, Q-17a-2, Q-17b-1 (own commit). What changed:
`sql_expr` / `join_sql_expr` render `__repark_product(...)` while display and projection
stay `product(...)`; SQL `product` stays refused. The kernel gains `product(v)` display
and a `UserDefined` signature whose `coerce_types` maps string input to `DOUBLE` — the
ANSI decision then lives in the engine's own `CAST` (verified: `CAST(s AS DOUBLE)`
raises Spark's `[CAST_INVALID_INPUT]` under ANSI and nulls otherwise), so the kernel
never branches on a value. A bare-context probe showed DataFusion's default cast is
strict, which is why the coercion must ride the session CAST rather than the kernel's
arrow cast. `window.rs` extends the Spark window alias to `__repark_product`.

Red on the step-2 tree: the seven `product` cells
(`python-product-{7,8,11,22,23,26}`, `python-product-10`). Green after the fix: 28
passed, 2 xfailed — cells 7, 8, 22, 23 and the ANSI refusal (cell 10) answer; cells 11
and 26 (the window with a trailing `.orderBy('g','k')` over an unprojected `k`) fail
in `DataFrame.orderBy`, which is run 18b's seam (proven kernel-independent: `F.sum`
fails the same shape), so they are `xfail(strict=True)` naming that seam with registry
row `FNP-AGG-1-18A`. Rust: `cargo test -p repark-functions --lib product` 4 passed
(new `numeric_strings_coerce_to_double` proves strings reach the kernel as doubles).

Error-class note: the ANSI refusal surfaces as `PySparkException` carrying
`[CAST_INVALID_INPUT]`, not Spark's `NumberFormatException` leaf — the binding
deliberately defines no such leaf (`exceptions.rs`, Group X), so the condition text is
the attainable contract and the pins assert exactly that.

Rust-first roll-call: the string-to-double decision sits in `coerce_types`
(kernel); Python holds the display and the two spellings only.

## Critic round and its dispositions (2026-09-16, run 17a)

Three Grok reviewers read the step-2 head. **critic-logic found no P1** — the two-partition merge
test on every UDAF held up under attack, which is the defect class that cost a sibling unit a
remediation round two nights earlier. **Rust perf found no P1** (four P2s, one P3).

- **ag-logic's one P2 is REFUTED by measurement.** It conjectured that `F.mode('v', True)` might
  answer the *maximum* on a frequency tie, because Spark renders that form's display name as
  `mode() WITHIN GROUP (ORDER BY v DESC)`, and said that if so it would be a P1 on a claimed shape.
  The unit's fixture had no tie cell, so the orchestrator recorded one live:
  `python/repark/tests/fnp_agg_1_mode_tie_spark_oracle.json`, seven cells. On a group where 10, 20
  and 30 each appear once, **`F.mode('v', True)` and `SELECT mode(v, true)` both answer 10, the
  smallest** — exactly what `mode.rs`'s `extremum(false)` does. The explicit orderings are a
  separate path and also match (`WITHIN GROUP (ORDER BY v DESC)` → 30, `ORDER BY v` → 10). The
  `DESC` in the 2-arg display name is cosmetic. The critic was right to flag the gap and right to
  label it a conjecture; the measurement closes it in the implementation's favour and converts
  `deterministic_ties_pick_smallest` from an author's assertion into an oracle-backed pin.
- **Two P1s from the Python perf read are real and are CARRY-OVER**, not closed tonight (the run's
  clock ran out before a remediation round plus a full gate plus CI could land):
  - **PYPERF-001** `F.any_value` binds DataFusion's `first_value` and folds `ignoreNulls` in Python,
    while the SQL door runs `SparkAnyValue` — **two kernels behind one Python name**, so an
    ungrouped select and a `groupBy().agg` take different paths. The fix is dispatch arms onto
    `any_value_udaf()` (unary and binary), passing the `ignoreNulls` Column through instead of
    folding it, and `join_sql_expr` naming `any_value` rather than `first_value`.
  - **PYPERF-002** `F.product`'s `sql_expr` leaks the internal `__repark_product` name.
  Both are the door-split class the campaign keeps finding: a decision taken in Python is a decision
  the SQL door never makes.

## Step 3a (run 18a, 2026-09-16) — percentile: exact Spark percentile on both doors

What changed: `percentile.rs` (linear interpolation; scalar, array and Column
percentages; literal and Column frequency; Spark range and negative-frequency
refusals); `aggregate::functions()` registration; `binary_aggregate_udaf` widened
to `nary_aggregate_udaf` with a `percentile` arm; `F.percentile` thin facade naming
`percentile` on both doors; example, inventory and maps in lockstep.

Red on the step-2 tree: the card's `percentile` cells fail (no such facade name).
Green after the fix: `pytest test_fnp_agg_1.py -k percentile` 15 passed;
`cargo test -p repark-functions --lib percentile` 20 passed (7 kernel tests incl.
the two-partition merge test). Rust-first roll-call: interpolation, resolution and
refusals sit in the kernel; Python holds the literal/list/Column shape and the
display/sql/join spellings only.

## Step 3b (run 18a, 2026-09-16) — listagg_distinct / string_agg_distinct

What changed: `string_distinct.rs` (one parameterized kernel behind the two
internal names; reverse-scan dedup; NULL delimiter concatenates bare; all-NULL
answers NULL; non-literal delimiter refused); `aggregate::functions()`
registrations; n-ary dispatch arms; thin facades (`str`, `bytes`, Column, None;
display names the Spark spelling, `sql_expr` names the internal kernel so raw
SQL stays `UNRESOLVED_ROUTINE`); example, inventory and maps in lockstep.

Ordering rule derived from the fixture: no total order fits (`{x,y}` answers
`xy` while `{a,b,c}` answers `c-b-a`), and the frequency rule fails (`b`
appears twice but does not lead). Reverse-scan dedup (descending
last-occurrence) fits both cells. Cross-partition merge appends unseen values
in arrival order (Spark is equally order-free across partitions); the merge
test merges second-half state first and equals the single-partition answer.

Green after the fix: `pytest test_fnp_agg_1.py -k "listagg_distinct or
string_agg_distinct"` 18 passed (12 value + 4 SQL-refusal + 2 signature cells);
`cargo test -p repark-functions --lib string_distinct` 6 passed (incl. the
reverse-order merge test). Rust-first roll-call: dedup, ordering, delimiter
resolution and refusals sit in the kernel; Python holds the delimiter shape
and the display/sql/join spellings only.

## Step 3c (run 18a, 2026-09-16) — histogram_numeric (kernel green; 4 value cells await a ruling)

What changed: `histogram_numeric.rs` (Spark `NumericHistogram`: closest-pair
merge with last-wins ties, which the fixture demands — `{10,20,30}` at 2 bins
answers `[{10,1},{25,2}]`; `x` keeps the input numeric type, `y` is double;
state holds raw values so merge is exact); registration under the real name
(the SQL door answers); n-ary dispatch arm; thin facade (int or Column bins);
example, inventory and maps in lockstep. Census in the same slice: EX-0
1082 → 1090 (four drift rows predate this run, verified present at `8d99217b`;
four are this unit's names, added-only) and `BACKLOG_BASELINE` 108 → 105 (the
file holds 105 named lines; the drop predates this run).

Green: `cargo test -p repark-functions --lib histogram_numeric` 4 passed
(incl. the two-partition merge test); the signature pin and both
`VALUE_OUT_OF_RANGE` error pins pass on the fresh native. Blocked: the 6
value cells (4 python, 2 SQL) fail ONLY on the nested-struct row shape —
names, Spark types, nullability and numeric values all match, but `collect`
materializes nested structs as plain dicts (the settled engine contract,
pinned by the nested-container suite) while the fixture rows hold Spark
`Row`s (`{"map": ...}` versus `{"row": ...}`). No file in this unit's scope
can close that gap; it is handed back as Q1. Rust-first roll-call: binning,
typing, bins resolution and the refusal sit in the kernel; Python holds the
bins shape and the display/sql/join spellings only.

## Step 3d (run 18a, 2026-09-16) — grouping_id: Rust bitmask and ResolveGroupingId on both doors

What changed: `grouping.rs` (new; `__repark_grouping` UDAF aliased `grouping`,
`grouping_id` UDAF, `ResolveGroupingId` analyzer rule: bitmask from the plan's
grouping sets, `Int8` / `Int64` out, canonical `grouping(g)` display, both
error classes in Spark wording; the rule normalizes the planner's qualified
projection alias and rebuilds the projection so the cached schema follows the
renamed exprs); `aggregate::functions()` registrations; n-ary dispatch arm and
`grouping_id_column` binding; thin `F.grouping_id` facade plus the by-name
name; pins, example inventory untouched (no new example), maps in lockstep.

Precedence: the planner resolves `grouping` to the custom UDAF (registered
after DataFusion's builtins), so `ResolveGroupingId` answers and DataFusion
54.1's builtin `grouping` UDAF plus its `ResolveGroupingFunction` never fire;
only the custom path gives Spark's `tinyint` with the unqualified display and
refuses outside grouping sets, while the builtin answers `Int32` qualified.
Pinned by a `GROUPING SETS` Rust test and a Python-cube Arrow-`int8` pin, both
asserting type and name. The in-place projection rewrite (mutated exprs, stale
cached schema) broke the optimizer's schema invariant; the fix rebuilds the
projection through `Projection::try_new`.

TYPES-1 flip: the `tinyint` answer intentionally moves the three
`types-1/C-004` grouping pins off the builtin's `Int32` (TY-8 anticipated this
layer); values and the plain-`GROUP BY` acceptance are unchanged, and the live
carve-out now pins the (`int8`, `int8`) pair. The plain-`GROUP BY` zip counted
one column too many and dropped the aggregate pairing; fixed in this slice.

Rulings applied: R-18a-9 (signature helper strips the `*args` annotation; the
oracle file is never edited), R-18a-10 (tie pins compare order-insensitively
within runs tied on every sort key — tie order is not a semantic contract),
R-18a-11 (SQL-door `tinyint` label strict-xfails to LOGICAL-WIDTH-1 with a
functions-section registry row; type table untouched). Rust-first roll-call:
bitmask, typing, display, both refusals and the merge-clean accumulator sit in
the kernel; Python holds the argument shape and the display/sql/join spellings
only.

Green: `cargo test -p repark-functions --lib grouping::` 8 passed (incl. the
`GROUPING SETS` precedence test and the merge-clean constant accumulator);
`pytest test_fnp_agg_1.py -k "grouping_id or tinyint or
reaches_rust_rule"` 14 passed, 1 xfailed (the LOGICAL-WIDTH-1 label pin);
`pytest test_types_1.py -k grouping` 2 passed, 1 skipped (live tier). Red in
this commit, all outside the slice: `count_min_sketch` 8 (kernel and facade
land next under R-18a-12), `sum_distinct` / `sumDistinct` 19 (R-18a-8(b)
next), `histogram_numeric` 6 (nested-struct Q1 open since step 3c),
`listagg` 7 (SQL `UNRESOLVED_ROUTINE` out of card scope; delimiter-default
signature and names red since the 16a baseline).

## Run-18a rulings recorded (2026-09-16, follow-up 12:27)

| Id | Ruling |
|---|---|
| R-18a-1 | EX-0 1090 and BACKLOG_BASELINE 105 accepted as measurements (recounts). |
| R-18a-7 | `grouping_id` next (brief step 8): Rust decides the bitmask and both error classes; GroupedData (`dataframe/**`) untouched; a cell needing it is strict-xfail naming the seam and run 18b, with a registry row. |
| R-18a-8 | This session also carries card step 4 and the `sum_distinct` row: (a) `count_min_sketch` byte-exact against the fixture hex with a two-partition merge test, else a dated DECLARED refusal joining registry `FNP-16-sketches`; (b) `sum_distinct` / `sumDistinct` via a DISTINCT flag on `PyColumn.aggregate`, `sum(DISTINCT v)` display, `FutureWarning`, SQL `sum(DISTINCT v)` answers while `sum_distinct(v)` stays refused; pins over the alias oracle and the unit fixture. |

## Run-18a rulings recorded (2026-09-16, follow-up 14:12)

| Id | Ruling |
|---|---|
| R-18a-9 | Signature helper strips the `: annotation` in the `*args` branch so the oracle's `(*cols: 'ColumnOrName')` compares by name `cols`; the oracle file is never edited. |
| R-18a-10 | Spark does not guarantee emission order of rows tied on every `orderBy` key: tie pins compare order-insensitively within runs of equal sort keys, order-sensitively across runs; values, types, names and nullability stay exact. |
| R-18a-11 | SQL-door `grouping` `tinyint` label waits on LOGICAL-WIDTH-1 (owner-scheduled, run 18b type-table slice): strict xfail on that assertion plus one functions-section registry row; type table untouched. |
| R-18a-12 | `count_min_sketch` ships as the byte-exact Rust UDAF with the two-partition merge test (element-wise table add, `totalCount` add, incompatible-sketch merge refusal pinned); kernel commit first, facade plus pins second. |

## Step 7 (run 18a, 2026-09-16) — count_min_sketch: byte-exact Rust kernel

What changed: `count_min_sketch.rs` (new; `count_min_sketch` UDAF over
`(value, eps, confidence, seed)` with Spark's `count_min_sketch(v, 0.5, 0.5,
1)` display and `Binary` out); `aggregate::functions()` registration; maps in
lockstep. The spec came from the 4.1.2 sketch-jar bytecode plus a live local
4.1.2 probe: `java.util.Random` seeds (`nextInt(MAX_VALUE)` draws),
multiplicative long buckets (`seed * value`, fold high word, mask, mod
width), string double hashing with per-tail-byte Murmur mixing
(`hash(bytes, 0)` / `hash(bytes, hash1)`, `abs((h1 + i*h2) % width)`),
big-endian V1 serialization, `Random(1)` first draw `1569548985`. Integral /
string / binary input routes to `addLong` / `addBinary`; NULLs skip; empty
input answers the empty sketch; merge checks depth, width, then seeds in
Spark's wording and adds tables plus `totalCount` element-wise.

Calibration: the Murmur-`hashLong` family is falsified (it maps
`{10, 20, 30}` to cells `{1, 1, 2}`; Spark answers `{0, 1, 3}` with `5` at
cell 2). The bytecode formulas reproduce 63 of 63 live single-row probes
(13 ints and 8 strings over widths 4, 100000, 99999, including `INT_MIN` /
`INT_MAX` sharing cell 9155), the depth-2 seed pair and both its buckets, and
a 20-point `(eps, confidence)` dimension grid. The fixture `k` sketch holds
four items (`k` is `'b'` twice), so `totalCount` 4 with table `[0, 1, 1, 2]`.

Known loud-failure boundaries (verified against live Spark where noted):
zero-width params (`eps` NaN or infinite) refuse at add time while Spark
throws `ArithmeticException`; gigantic tables (`eps` 1e-9, 16 GB) refuse via
`try_reserve` while Spark OOMs the executor; display floats past 1e15 render
in Rust shortest form rather than Java scientific notation (fixture values
unaffected). Rust-first roll-call: seeds, buckets, serialization, merge and
both refusals sit in the kernel; Python will hold the argument shape and the
display/sql spellings only.

Green: `cargo test -p repark-functions --lib count_min_sketch::` 10 passed
(incl. the three fixture-hex byte pins, the oracle-cell extreme-int pin, the
two-partition merge pin and the incompatible-merge refusal pin); full
`--lib` 792 passed; clippy clean for the file; `cargo fmt --check` clean;
`check_rust_file_size` and `check-map-sync` clean. Red in this commit, all
outside the slice: the `count_min_sketch` facade plus pins (R-18a-12 second
commit next), `sum_distinct` / `sumDistinct` (R-18a-8(b) next),
`histogram_numeric` (nested-struct Q1 open since step 3c), `listagg` (SQL
`UNRESOLVED_ROUTINE` out of card scope).

## Run-18a rulings recorded (2026-09-16, follow-up 16:04)

| Id | Ruling |
|---|---|
| R-18a-16 | Q1 DENIED as scoped: the `LogicalKey` Binary-to-string arm is the type table's, owned by the run-18b slice — do not touch it here. The 4 sketch type assertions stay strict xfail (2 Python, 2 SQL) and the registry gains row FNP-AGG-1-18C naming the seam and 18b. |
| R-18a-17 | Q2 OWNED BY 18c, answered for 18a: the hybrid `count_min_sketch(k, ...)` SQL Debug alias keeps its Debug form for now; the 4 name assertions stay strict xfail and the registry gains row FNP-AGG-1-18D. The facade commit `a26e86e7` stands. |
| R-18a-18 | `sum_distinct` / `sumDistinct` (names, 2 signatures, `FutureWarning`) carry to the follow-up unit: C-001/C-002/C-003 stay OPEN only for those two names; everything else in the card must be PROVEN or carried under a prior disposition. |

## Step 8 (run 18a, 2026-09-16) — count_min_sketch facade on both doors

Commit `a26e86e7`. Facade `F.count_min_sketch(col, eps, confidence, seed)`
with Spark's display and both SQL spellings; dispatch routes the 4-arg
aggregate to the kernel. Pins: 1 signature pin plus the oracle sketch cells
(2 Python binary, 2 SQL bare-binary) green; 4 type assertions strict-xfail
per R-18a-16; 4 name assertions strict-xfail per R-18a-17. Registry:
`EX-FN-9` retired to FIXED, sketch section-7 row flipped, rows
FNP-AGG-1-18C (type-table seam to 18b) and FNP-AGG-1-18D (Debug-alias
naming to 18c) added. Maps in lockstep. Red in this commit: the 2 SQL
`hex()` nullability cells (fail-before evidence kept; Q3 below) and the
carried sum family (R-18a-18).

## Step 9 (run 18a, 2026-09-16) — clause roll-call, flips, residuals

Full card this step: 132 passed, 35 failed, 9 xfailed over
`test_fnp_agg_1.py` (plus 2 pre-existing collection errors in
`test_examples_functions_a.py`, untouched by this card). The 35 fail =
16 sum alias-oracle cells (carried, R-18a-18) + 14 value cells + 3
signature cells (2 sum carried; `listagg` SQL prior refusal) + 1 names
cell (the absent sum names, carried) + 1 deprecated-warn cell (carried).
The 14 value reds: `any_value` SQL 2 (17c seam, prior), `histogram_numeric`
Python 4 + SQL 2 (Q1, prior), `listagg` SQL 4 (Q2 + refusal, prior),
`count_min_sketch` SQL `hex()` 2 (Q3, new — see below). Sketch slice
exact: 5 passed (1 signature, 2 Python binary, 2 SQL bare-binary) + 8
strict-xfailed (4 type under R-18a-16, 4 names under R-18a-17) + the 2 Q3
reds. Every non-sum, non-prior cell in the card passes on both doors.

Q3 (new, OPEN, no ruling yet): the 2 SQL `hex(count_min_sketch(...))`
cells fail only on nullability — Spark marks `hex()` output nullable,
RePark propagates NOT NULL from the sketch kernel. Byte values match.
Fail-before evidence is the two FAILED lines kept in this step's run.

Rust-first roll-call for the sketch facade slice (one line per
Python-side logic): the `(col, eps, confidence, seed)` signature and
type checks are argument shape; the display and both SQL spellings are
names; `seed=None` draws a random 31-bit int because Spark's 3-arg form
also draws a per-query seed neither door can verify; `lit()` wrapping of
the scalars is argument shape; the test-side split comparator and the
two dedicated strict-xfail tests are pins under R-18a-16/17. Seeds,
buckets, serialization, merge and both refusals sit in the kernel.

Registry flips: EX-FN-9 FIXED; sketch section-7 row both doors;
FNP-AGG-1-18C and 18D added. Residuals: Q1 histogram nested-struct
(step 3c), Q2 listagg SQL `UNRESOLVED_ROUTINE` (prior), Q3 hex
nullability (above), the sum family (R-18a-18), `listagg` SQL signature
(prior refusal).

Clause verdicts: C-001 OPEN only for the two carried sum names (the one
other sig red, `listagg` SQL, rides its prior out-of-card refusal);
C-002 OPEN only for the carried sum Python alias cells (the 4
histogram-Python reds ride prior Q1); C-003 OPEN for the carried sum SQL
cells plus the four carried residuals (17c dupes, Q1, Q2, Q3); C-004
PROVEN (29/29 error cells green); C-005 PROVEN (kernel two-partition
merge pin green); C-006 OPEN pending the step-10 gates; C-007 PROVEN
(registry flips above, every touched map in lockstep).

VERDICT: 7 clauses, 3 PROVEN, 4 OPEN, 0 REJECTED (step-9 evidence
above; the only in-card OPEN driver is the R-18a-18 sum carry).
