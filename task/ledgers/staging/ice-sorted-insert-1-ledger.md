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

### Oracle cells (recorded 2026-09-17, Spark 4.1.2, `local[4]`, UTC)

`python/repark/tests/_record_ice_sorted_insert_1_oracle.py --rewrite` wrote
`python/repark/tests/ice_sorted_insert_1_spark_oracle.json` (8 cells) and the
`fixtures/ice_sorted_insert_1/days` warehouse (84 KB). Per-file
`(records, sort_order_id, sorted)`:

| cell | files |
|---|---|
| sort_partitioned_local | (1000, 1, True, p=0), (1000, 1, True, p=1) |
| sort_unpartitioned_desc | (2000, 1, True) |
| sort_two_keys_nulls | (2000, 1, True), heads `(0, NULL)` |
| sort_dataframe_door | (1000, 1, True, p=0), (1000, 1, True, p=1) |
| sort_distribution_none | (1000, 1, True, p=0), (1000, 1, True, p=1) |
| sort_transform_bucket | (2000, 1, False) — one file, id-major heads `(0,1,2)`, unsorted by id alone (bucket-major) |
| sort_transform_days | (2000, 1, True), day-major heads |
| sort_float_nan | (2000, 1, True), NULLS FIRST heads, NaN largest |

The float cell confirms the checker: ASC NULLs first, NaN above every finite
value. The days cell confirms `WRITE ORDERED BY days(ts), id` sorts day-major.
`sparkenv` carries no pyarrow, so the recorder reads per-file rows through
Spark (`input_file_name()`) joined to `{t}.files` on the file basename.

### RePark reproduction on this tree

TBD — pins run once the release native lands.

### Red-first (main pin)

Run-19c rating row V2-12 / claim C-7 (2026-09-16): plain `INSERT INTO` into a
declared-order table wrote unsorted files with `sort_order_id` NULL; Spark
writes each file sorted and stamped. That is the red for C-001/C-002 on main's
pin. For C-003 the red is measured in-test on the release native before the
fix (`test_repark_owned_paths_write_sorted_stamped_files`):

```
assert entry["sort_order_id"] == order_id, entry
AssertionError: {'file_path': '.../owned/data/p=0/951c62c9-....parquet',
 'record_count': 1000, 'sort_order_id': None}
assert None == 1
```

The same run had C-001 (5 SQL cells) and C-002 (3 DataFrame doors) green: the
fork's `insert_into` sorts and stamps, while RePark-owned writers (`append.rs`,
`merge/row_lineage.rs`, `merge/mod.rs`) never call the fork's
`with_sort_order_id`. The fix stamps `default_sort_order_id` at those three
sites, reusing the fork's builder without re-implementing any sort.

### Green (override)

`test_ice_sorted_insert_1.py -k "not live"`: **10 passed** on the release
native with the stamp fix. C-001 (5 SQL cells) and C-002 (3 DataFrame doors):
every file sorted on the order keys, stamped 1, full 2,000-row set. C-003:
INSERT OVERWRITE and MERGE sort and stamp 1; the CTAS replace resets the
default to 0 and stamps 0 over the unsorted hash layout (the C-010 shape, so
the CTAS leg asserts stamp + row set, not sortedness); plain INSERT into the
adopted `days(ts), id` table sorts day-major and stamps 1, while INSERT
OVERWRITE keeps the `only identity sort fields are supported` refusal.

| Clause | Statement | Pins | Verdict |
|---|---|---|---|
| C-001 | Plain `INSERT INTO … SELECT` into a declared-order table writes sorted, stamped files, SQL door | `test_ice_sorted_insert_1.py` SQL cells | PROVEN |
| C-002 | Same, DataFrame door (`writeTo(t).append()`, `saveAsTable(mode="append")`, `insertInto`) | `test_ice_sorted_insert_1.py` door legs | PROVEN |
| C-003 | Paths RePark owns (INSERT OVERWRITE, CTAS, MERGE, `sort_batches_by_default_order`) write sorted, stamped files or file a registry row | `test_ice_sorted_insert_1.py` owned legs + days leg | PROVEN |
| C-004 | Spark oracle recorded as fixture: recorder script plus truth JSON (sort cells + days-transform cell + float-NaN cell); live tier re-derives | recorder script, truth JSON, live legs | OPEN |
| C-005 | Registry sort-on-INSERT row FIXED 2026-09-17 (consumes fork #287 via RP-22); WRITE-ORDER-TRANSFORM-1 FIXED if pinned green else stays open with the measurement | registry rows, map edits | OPEN |

## 7. Gates

TBD.

## 8. Round 2 (2026-09-17) — rebase onto main at RP-22

The orchestrator rebased this branch onto main (`444323f2`, RP-22 PR #667,
fork pin `96fc9f1f`, carries F-SORTED-INSERT-1 #287) and removed the
`/tmp/jc-forksrc` override; `Cargo.toml` / `Cargo.lock` are tracked again.
The unit's Rust fix uses only long-standing fork APIs (`DataFileWriterBuilder`,
`with_sort_order_id`, `default_sort_order`), so no code change was needed for
the rebase. No cell asserts file counts, so the 4151b488-only target-file-size
change (fork #288, RP-23 #671) touches no pin: no `xfail` was needed.

### Rebuild + rerun on main's pin

Release native rebuilt on the RP-22 tree (`maturin develop --release`,
installed); `test_ice_sorted_insert_1.py -k "not live"`: **10 passed, 8
deselected**. The owned-paths stamp assertions (red as `None == 1` before the
fix) pass, so the installed native carries both the fix and fork #287. No
`xfail` needed: nothing asserts file counts.

### Registry

`WRITE-ORDER-SORTED-INSERT-1` FIXED 2026-09-17 (consumes fork #287 via RP-22
#667); `WRITE-ORDER-TRANSFORM-1` stays open with the 2026-09-17 measurement
(plain INSERT into the adopted days table sorts and stamps via the fork;
RePark-owned paths keep the loud refusal). C-005 PROVEN on write; live tier
still to run.
