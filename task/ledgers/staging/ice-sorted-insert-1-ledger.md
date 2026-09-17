# Unit ledger — ICE-SORTED-INSERT-1 sort-on-INSERT end to end

**Unit:** `ice-sorted-insert-1` · **Date:** 2026-09-17 · **Branch:** `feat/ice-sorted-insert-1` · **Base:** `origin/main` at `225f68ee`
**Model:** muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.
**Oracle:** PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (see `_oracle_pins.py`), recorded at authoring time into `python/repark/tests/ice_sorted_insert_1_spark_oracle.json`, replayed live under `REPARK_PARITY_LIVE=1`.
**Fork fix:** fork PR #287 (F-SORTED-INSERT-1), fork main `4151b488`, consumed via RP-22 (PR #667). Local develop override: the five iceberg* lines of `Cargo.toml` point at `/tmp/jc-forksrc`; `Cargo.toml` and `Cargo.lock` are skip-worktree and never staged.

## 1. Scope and fence

This unit proves the fork's sort-on-INSERT end to end and fixes any RePark-side gap. The table-format sort lives in the fork (`IcebergTableProvider::insert_into` sorts each writer stream by the table's default sort order and stamps `sort_order_id`); RePark pins it on both doors and checks every path RePark owns itself (INSERT OVERWRITE, CTAS, MERGE, `sort_batches_by_default_order` in `crates/repark-iceberg/src/write/distribution.rs`). Touches: the ledger, one recorder script plus truth JSON, one pin test, the parity registry (sort-on-INSERT row, WRITE-ORDER-TRANSFORM-1), and maps in lockstep. No `[patch]` override reaches any commit; no `Cargo.toml` edit; no STATUS.md edit.

## 2. Oracle rule

Spark 4.1.2 semantics for `INSERT INTO` on a table with a declared default sort order: every committed data file is sorted on the order's keys and stamped with the order id. Cells: partitioned local order, unpartitioned DESC, two keys with NULL ordering, the DataFrame door, `LOCALLY ORDERED BY`, transform `bucket(4, id)`, plus this unit's additions: transform `days(ts), id` and a float-with-NaN cell. Recorded once into the truth JSON; the live tier re-derives every cell from live Spark.

## 3. Design

TBD — recorder cells, pin shapes per door, `sort_order_id` checks via `{t}.files` plus parquet bytes.

## 4. Risks

- The live tier shares the one JVM `SparkContext` (`conftest.py` guard): no test stops any session.
- `git stash` is unusable (skip-worktree Cargo files); reds on main's pin come from the run-19c rating result, quoted below.
- Fixture JSON must stay small: per-file records arrays are trimmed to heads.

## 5. Clauses

| Clause | Statement | Pins | Verdict |
|---|---|---|---|
| C-001 | Plain `INSERT INTO … SELECT` into a declared-order table writes sorted, stamped files, SQL door | `test_ice_sorted_insert_1.py` | OPEN |
| C-002 | Same, DataFrame door (`writeTo(t).append()`, `saveAsTable(mode="append")`, `insertInto`) | `test_ice_sorted_insert_1.py` | OPEN |
| C-003 | Paths RePark owns (INSERT OVERWRITE, CTAS, MERGE, `sort_batches_by_default_order`) write sorted, stamped files or file a registry row | `test_ice_sorted_insert_1.py` | OPEN |
| C-004 | Spark oracle recorded as fixture: recorder script plus truth JSON (sort cells + days-transform cell + float-NaN cell); live tier re-derives | recorder script, truth JSON, live legs | OPEN |
| C-005 | Registry sort-on-INSERT row FIXED 2026-09-17 (consumes fork #287 via RP-22); WRITE-ORDER-TRANSFORM-1 FIXED if pinned green else stays open with the measurement | registry rows, map edits | OPEN |

## 6. Evidence

### Rating row (prior behaviour, quoted)

Row V2-12, claim C-7 (2026-09-16): a plain `INSERT INTO` into a table with a declared sort order (`ALTER TABLE … WRITE ORDERED BY …`) wrote unsorted files with `sort_order_id` NULL; Spark writes each file sorted and stamped with the table's order id. Probe: `/tmp/oc-worker/ice-rating/scratch/probes/p_sort_overwrite.py`. Spark oracle: `/tmp/oc-worker/ic-build/write_fidelity_spark.json` (cells `sort_partitioned_local`, `sort_unpartitioned_desc`, `sort_two_keys_nulls`, `sort_dataframe_door`, `sort_distribution_none`, `sort_transform_bucket`), generator `/tmp/oc-worker/ic-build/write_fidelity_probe.py`.

### RePark reproduction on this tree

TBD.

### Red-first (main pin)

TBD — run-19c rating result quoted here.

### Green (override)

TBD.

## 7. Gates

TBD.
