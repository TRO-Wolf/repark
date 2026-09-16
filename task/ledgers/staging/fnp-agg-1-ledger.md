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
| C-004 | The error cells fail with Spark's own error condition on their own door. | OPEN | Red on the base: all 29 error pins fail (27 agg + 2 alias `sum_distinct`). Per condition failed: `UNRESOLVED_ROUTINE` 10 (product 6, listagg_distinct 2, string_agg_distinct 2), `DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE` 6 (percentile 4, histogram_numeric 2), `UNSUPPORTED_GROUPING_EXPRESSION` 4, `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` 4, `GROUPING_ID_COLUMN_MISMATCH` 2, `CAST_INVALID_INPUT` 1 (percentile over strings, ANSI on). The `UNRESOLVED_ROUTINE` cells fail because RePark does not refuse with Spark's condition today. Step 2: the round's error cells green — `product` SQL refuses 6/6 via the new unknown-routine mapping, `max_by`/`min_by` 3-arg SQL raises `[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]` 4/4. |
| C-005 | Multi-partition merge: a Rust test splits input across two partitions per UDAF. | OPEN | Step 2: merge tests land for the five new kernel files (`any_value`, `max_by`/`min_by`, `kurtosis`/`skewness`, `mode` unique-max plus deterministic-carry, `product`); each asserts the merged result equals the single-partition one. Remaining UDAFs (percentile, grouping_id, histogram_numeric, count-min-sketch) come with their steps. |
| C-006 | No regression: the touched suites stay green. | OPEN | Step 2: `cargo test -p repark-functions` 726 passed; `test_fnp5_aggregates`, `test_functions_a`, `test_functions_b`, `test_fnp_misc_1`, `test_functions_split_identity`, `test_fn_batch4` green; full parity suite and `make verify` in §Gates (run 17a, step 2). |
| C-007 | Registry (`EX-FN-9` → FIXED, section-7 rows) and every touched `map.md` in lockstep. | OPEN | Step 2 (maps only; registry flips are card step 5): new modules listed in `crates/repark-functions/src/map.md`, `functions_agg_1.py` in `python/repark/src/repark/spark/map.md`, `agg_misc.py` in `docs/examples/functions/map.md`; inventory +4 rows, backlog −3 (`F.kurtosis`, `F.mode`, `F.skewness` now covered), `BACKLOG_BASELINE` 111→108, EX-0 1074→1078. |

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED (step-2 evidence above; the two `any_value` SQL cells wait on the run-17c duplicate-projection seam).

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
| `make verify` | green through `check-manifest`; was red at `check-ledgers` on the stale staging row above — green after the repoint (re-run at commit time) |
| comment-ban grep over the staged diff | prints nothing |
