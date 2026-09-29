# Unit ledger — POLARS-IS-DUPLICATED-1 · `Column.is_duplicated()` on both doors answers real polars

**Date:** 2026-09-28 · **Branch:** `feat/polars-is-duplicated-1` · **Base:** `origin/main`
· **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Owner ruling 2026-09-28 pulls one expression ahead of the v1.9
Polars parity release into v1.5.2: `frame.filter(rp.col("c").is_duplicated())`
and `df.filter(F.col("c").is_duplicated())`, on both doors, as a documented
RePark extension (PySpark has no such method). Both calls raise today (a
`TypeError` through `Column.__getattr__` item fabrication, measured).

**Rulings recorded at open.** Owner: both doors, target v1.5.2 (card
`polars-is-duplicated-1.md`). Rust-first: the mask, the float normalisation and
the filter/select lowering live in Rust; Python forwards one call. Polars is
the answer oracle; Spark cells only confirm the grouping design (null==null,
NaN==NaN, -0.0==0.0 — all three confirmed on live Spark 4.1.2, session zone
America/New_York, 2026-09-28, so no halt).

## PROPOSITION LEDGER — POLARS-IS-DUPLICATED-1 — 2026-09-28

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Both doors expose `Column.is_duplicated()` as a documented RePark extension whose docstring names polars semantics and PySpark's lack; `dir` gains exactly that name. | `test_dir_gains_only_is_duplicated`, example run. | PROVEN | `column_fields.is_duplicated` bound on the shared `Column`; `column.py` net-zero at 1529; `dir` minus the name equals the recorded base list. pins: polars-is-duplicated-1/C-001. §2. |
| C-002 | `filter` over the mask equals real polars 1.43.2 on every dtype frame including row order. | `test_filter_matches_polars_on_both_doors` vs the oracle fixture. | PROVEN | INT/BIGINT/DOUBLE×2/STRING/DATE/DECIMAL(10,2)/BOOLEAN/empty/one-row, both doors, order-exact. pins: polars-is-duplicated-1/C-002. §2. |
| C-003 | `select` and `withColumn`/`with_columns` equal polars including row order. | `test_select_matches_polars_on_both_doors`, `test_with_column_matches_polars_on_both_doors`, `test_masks_match_polars_on_both_doors`. | PROVEN | Same frames, both doors; the lowering sorts a carried row index back because a bare window projection reorders. pins: polars-is-duplicated-1/C-003. §2. |
| C-004 | Negation, conjunction, expression receivers and a prior `filter` equal polars. | `test_negation_and_conjunction_in_filter`, `test_expression_receiver_and_prefilter`. | PROVEN | `~mask`, `mask & (k > 1)`, `(a % 2)` receiver, `filter(c > 1).filter(mask)` — all polars-equal, both doors. pins: polars-is-duplicated-1/C-004. §2. |
| C-005 | The 10,000-row frame (3 repeated values) pins true-count 6, duplicated values [0,1,2] and the recorded mask sha256, filtering under 2 s on a debug build. | `test_big_frame_summary_and_timing`. | PROVEN | `75d7db49…9e9e16`, measured ~0.1 s. pins: polars-is-duplicated-1/C-005. §2. |
| C-006 | No existing PySpark-door answer changes: user-written windows in filter keep today's refusal, and every keep-cell answers as at base. | `test_window_in_filter_still_refuses`, `test_window_select_answers_unchanged`, `test_null_and_membership_cells_unchanged`, `test_row_number_and_drop_duplicates_unchanged`, `test_plain_rp_columns_unchanged`, 20 neighbours base-vs-head. | PROVEN | Refusal class and shape unchanged (only the session-random `__repark_cdf_<uuid>` differs); 18/20 neighbours byte-identical, 2 dropDuplicates order-only (proven inherent by a head-vs-head rerun). pins: polars-is-duplicated-1/C-006. §2. |
| C-007 | Float grouping matches polars (NaN==NaN, -0.0==0.0) while every other expression's float semantics stay unchanged. | Double-frame legs of C-002..C-004 plus the unchanged dropDuplicates double set. | PROVEN | The normaliser UDF runs only on the `is_duplicated` partition key; `dropDuplicates` still keeps `-0.0`/`0.0` apart as at base. pins: polars-is-duplicated-1/C-007. §2. |
| C-008 | Mutation 1 (`row_number() > 1` lowering) turns pins red; mutation 2 (dropped ±0.0 normalisation) turns the ±0.0 legs red; both revert clean. | Red runs + `git status` clean + green rerun. | PROVEN | M1: 7 failed, 6 passed. M2: the 4 mask/filter/select/withColumn tests fail at the 0.0/-0.0 legs. Reverted, tree clean, 13 passed. pins: polars-is-duplicated-1/C-008. §3. |
| C-009 | The example inventory carries the new public name: COVERS example, snapshot row, count pin 1086 → 1087. | `is_duplicated_ext.py` run + enumerator/snapshot checks. | PROVEN | Example green; `example_inventory` vs snapshot findings empty; count pin moved. pins: polars-is-duplicated-1/C-009. §2. |

## 1. Red-first record (base, 2026-09-28)

Probe `target/dup/step0.py` on the unmodified tree: all 76 duplicate cells
`TypeError: 'Column' object is not callable` (the `__getattr__` fabrication);
keep/design/neighbour answers recorded to `target/dup/repark_base`,
`target/dup/neighbours_base.json`. Polars oracle to `target/dup/polars`
(polars 1.43.2); Spark design cells to `target/dup/spark` + `spark_null.json`
(Spark 4.1.2). Measured engine facts that shape the fix: the window groups
nulls and NaN as polars does but splits `-0.0`/`0.0`; a window projection
reorders rows by partition key; a window in filter/orderBy refuses at physical
planning with `UnsupportedOperationException`.

## 2. Implementation record (2026-09-28)

New `crates/repark-python/src/is_duplicated.rs`: a boolean-identity marker
ScalarUDF plus the float-key normaliser UDF, both consumed as resolved `Arc`s
so no session registration and no SQL-door name change; `filter_frame` /
`select_frame` serve the two `PyDataFrame` choke points (`dataframe.rs` shrinks
976 → 969). Marker-free plans route byte-identical to before. With a marker:
stage a `row_number() OVER ()` index in scan order, project each innermost
marker as a helper (iterated, so nesting composes), substitute helper refs,
sort the index back, drop helpers. The constructor
(`expr_build::is_duplicated_expr`) reuses `count_aggregate` +
`build_over_expression`, so the unordered full-partition frame matches a
user-written window exactly. Python is one forwarder in `column_fields.py`
plus one bind line; `column.py` stays 1529 net-zero. Product ~490 Rust + ~30
Python lines; tests ~260 lines plus the 17 KB recorded fixture.

## 3. Mutation record (2026-09-28)

M1 replaced the count window with
`row_number() OVER (PARTITION BY key ORDER BY key) > 1`: 7 failed, 6 passed
(the keep/dir legs hold). M2 made the normaliser identity: the 4
mask/filter/select/withColumn tests fail, first diff at the double frame's
0.0/-0.0 legs. Both reverted via saved copies; `git status` clean of product
diffs; pins rerun 13 passed; the native module rebuilt after the revert.

## 3b. DIFF-PROBE fold (2026-09-29, BUG-1 + BUG-2: the mask never reorders rows)

Probe `target/diff-probe` on this head: 351 non-dup neighbours identical and
the 1M-row plain filter/select gate at 0.96–1.09×, but two silent order bugs
on the new surface, both rooted in the `row_number() OVER ()` order index.
BUG-1: a user sort before a mask is lost (the helper window's partition-key
sort insertion strips the user's sort below the index window, so the index
numbers scan order). BUG-2: a user sort after a mask loses keys touching the
mask or partition column (the bare-`row_number` uniqueness FD is misattributed
to the mask column when `optimize_projections` merges the stacked helper
projections, and `eliminate_duplicated_expr` then prunes the later sort keys).

Fix in `crates/repark-python/src/is_duplicated.rs`: the index window is now
`row_number() OVER (PARTITION BY 1 ORDER BY <input sort keys>)`. The constant
partition keeps the numbering identical while removing the uniqueness FD, so
no later sort is ever pruned; the walked input-sort keys (top Sort through
Filter/Projection/SubqueryAlias/Limit, used only when every key column
resolves in the staged schema and no key nests a window, aggregate or
subquery, else the previous unkeyed shape) make the window require the user's
order, so the final index re-sort reproduces it. Mask values, rewrite triggers
and every non-dup plan are unchanged.

Proof: 8 new pins in `test_polars_is_duplicated_1.py` (BUG-1 filter/select ×
F/rp × asc/desc, BUG-2 all three shapes, limit/desc-nulls-last/3-key/
sortWithinPartitions/repartition-set/groupagg); mutation restoring the pre-fix
index turns the 7 order pins red (the set-based pin holds by design); full
DIFF-PROBE rerun recorded in the hand-back. 0 existing pins changed.

## 4. Residues (dated 2026-09-28, all out of the brief's cell list)

- R-001: `F.lit(1).is_duplicated()` answers all-true over RePark's 7 broadcast
  rows where polars answers `[False]` over its 1-row literal frame. Same mask
  rule, different frame shape; not pinned.
- R-002: a mask in a join ON refuses `[UNRESOLVED_ROUTINE] is_duplicated` (the
  `sql_expr` text leaks into generated SQL) where a user window refuses a
  `ParseException`. Both loud; the query cannot exist at base.
- R-003: nested `is_duplicated(is_duplicated())` answers polars-equal through
  the innermost-first expansion; exercised by hand, not pinned.

## 5. Gates (2026-09-28)

- `cargo test -p repark-python --lib` → 90 passed (5 new `dup_key` kernel tests).
- Gate clippy (`-p repark-python -p repark-core`, `-D warnings) → clean.
- `cargo fmt --check`, `check_lib_rs.py`, ruff check + format on touched files,
  `check_python_conventions.py`, `check_docstring_presence.py` → clean.
- Pin file → 13 passed; example inventory snapshot findings empty.
- Full `gate.sh` run recorded in the hand-back.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: polars-is-duplicated-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every brief cell pinned against the recorded polars oracle on both doors; red at base (TypeError), green at head.
      artifacts: [python/repark/tests/test_polars_is_duplicated_1.py, python/repark/tests/polars_is_duplicated_1_polars_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: NaN/±0.0/null/decimal/empty/one-row/big-frame numeric edges pinned; mutation 2 proves the ±0.0 legs bite.
      artifacts: [python/repark/tests/test_polars_is_duplicated_1_polars_oracle.json]
    - id: AT-3
      status: ATTACKED
      evidence: Negation, conjunction, expression receivers, prefilters, nesting and literal receivers probed; join/sort/agg/groupBy positions refuse loud with user-window-matching classes.
      artifacts: [crates/repark-python/src/is_duplicated.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Row order pinned order-exact on every shape including the 10k frame; no shared state (expression-local rewrite).
      artifacts: [python/repark/tests/test_polars_is_duplicated_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, environment read or secret.
    - id: AT-6
      status: ATTACKED
      evidence: Marker-free plans route to the identical pre-change calls; 20 neighbours plus keep-cells show no answer change.
      artifacts: [target/dup/neighbours_base.json]
    - id: AT-7
      status: ATTACKED
      evidence: 10k-frame filter ~0.1 s against the 2 s budget on a debug build; helpers are two window passes plus one sort.
      artifacts: [python/repark/tests/test_polars_is_duplicated_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, workflow or ceiling edit; column.py and lib.rs net-zero/down, dataframe.rs shrinks 976 to 969.
      artifacts: [scripts/check_lib_rs.py]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals keep today's classes; the extension docstring and COVERS example name the polars semantics and the PySpark lack.
      artifacts: [docs/examples/column/is_duplicated_ext.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first step-0 runs, both mutation reds and the revert-then-green rerun are recorded above.
      artifacts: [task/ledgers/staging/polars-is-duplicated-1-ledger.md]
  reattested: []
  complete: true
```
