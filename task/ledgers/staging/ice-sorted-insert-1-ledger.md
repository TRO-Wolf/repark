# Unit ledger — ICE-SORTED-INSERT-1 sort-on-INSERT end to end

**Unit:** `ice-sorted-insert-1` · **Date:** 2026-09-17 · **Branch:** `feat/ice-sorted-insert-1` · **Base:** `origin/main` at `225f68ee`
**Model:** muse-spark-1.3-contributor (rounds 1–2)
**Model:** claude-opus-5 (round 3, 2026-09-17 — remediation of the logic critic's L-01…L-05)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.
**Oracle:** PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (see `_oracle_pins.py`), recorded at authoring time into `python/repark/tests/ice_sorted_insert_1_spark_oracle.json`, replayed live under `REPARK_PARITY_LIVE=1`.
**Fork fix:** fork PR #287 (F-SORTED-INSERT-1), fork main `4151b488`, consumed via RP-22 (PR #667). Round-1 local develop override: the five iceberg* lines of `Cargo.toml` pointed at a local fork checkout; `Cargo.toml` and `Cargo.lock` were skip-worktree and never staged (removed in round 2).

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

| Clause | Statement | Evidence | Verdict |
|---|---|---|---|
| C-001 | Plain `INSERT INTO … SELECT` into a declared-order table writes sorted, stamped files, SQL door | `test_ice_sorted_insert_1.py::test_sql_insert_writes_sorted_stamped_files` (5 cells); red on main = rating row V2-12 (§6) | PROVEN |
| C-002 | Same, DataFrame door (`writeTo(t).append()`, `saveAsTable(mode="append")`, `insertInto`) | `test_ice_sorted_insert_1.py::test_dataframe_doors_write_sorted_stamped_files` (3 doors) | PROVEN |
| C-003 | Paths RePark owns (INSERT OVERWRITE, CTAS, `sort_batches_by_default_order`) write sorted, stamped files or file a registry row | `test_ice_sorted_insert_1.py::test_repark_owned_paths_write_sorted_stamped_files` + days leg; red `None == 1` before the stamp (§6). Round 3: the MERGE leg this row once claimed wrote zero files (L-02) and is removed; MERGE is C-006 | PROVEN |
| C-004 | Spark oracle recorded as fixture: recorder script plus truth JSON (sort cells + days-transform cell + float-NaN cell); live tier re-derives | `_record_ice_sorted_insert_1_oracle.py`, `ice_sorted_insert_1_spark_oracle.json`, live legs 8/8 (§8) | PROVEN |
| C-005 | Registry sort-on-INSERT row FIXED 2026-09-17 (consumes fork #287 via RP-22); WRITE-ORDER-TRANSFORM-1 FIXED if pinned green else stays open with the measurement | registry `WRITE-ORDER-SORTED-INSERT-1`, `WRITE-ORDER-TRANSFORM-1` (still OPEN) | PROVEN |
| C-006 | v3 and v2 partitioned INSERT OVERWRITE and MERGE rewrites (matched UPDATE, NOT MATCHED INSERT of shuffled keys) commit files sorted by the default order and stamped with its id; no RePark writer stamps bytes it did not sort (L-01, L-02) | `test_ice_sorted_insert_2.py::test_partitioned_rewrites_sort_and_stamp[m3,m2]`; red-first §9.3 (v3 file `[5380, 5152, 5040, …]` stamped 1); mutations B, D §9.4 | PROVEN |
| C-007 | Every stamp site has a revert-red pin: the unpartitioned INSERT OVERWRITE stamps the default order id (L-05), and after a second `WRITE ORDERED BY (id DESC)` files carry the CURRENT order id 2 in the new direction | `test_unpartitioned_overwrite_sorts_and_stamps`, `test_reordered_table_stamps_the_current_order`; mutations A, B, C §9.4 | PROVEN |
| C-008 | The owned-path sort canonicalises NaN for float keys as the fork's INSERT path does: NULLS FIRST, values ascending, NaN one solid tail block, one file stamped 1; a negative NaN joins that block (L-03) | `test_owned_float_overwrite_places_nan_like_spark`, `test_owned_float_overwrite_canonicalises_negative_nan`; red-first §9.3 (`[None, nan, 3.0, …]`); mutation F §9.4 | PROVEN |
| C-009 | The rewrite shapes that stay on the fork are filed, rowed and pinned strict-xfail: binpack `rewrite_data_files` (`F-RDF-SORT-STAMP-1`, L-04) and the fork's COW UPDATE exec (`F-COW-UPDATE-STAMP-1`, found by C-006's measured cells); the 15-cell oracle is a replayable fixture with a guarded live leg | `docs/fork-sync.md` "Open fork asks", registry `WRITE-ORDER-RDF-1` + `WRITE-ORDER-COW-UPDATE-1`, 3 strict xfails failing on `sort_order_id None == 1` (§9.3), `_record_ice_sorted_insert_2_oracle.py` + `ice_sorted_insert_2_spark_oracle.json` | PROVEN |
| C-010 | Row lineage survives the new sort: across a v3 partitioned matched UPDATE the id → `_row_id` map is unchanged and no `_row_id` is NULL | `test_v3_lineage_survives_the_default_order_sort`; mutation E §9.4 | PROVEN |

## 6. Evidence

### Rating row (prior behaviour, quoted)

Row V2-12, claim C-7 (2026-09-16): a plain `INSERT INTO` into a table with a declared sort order (`ALTER TABLE … WRITE ORDERED BY …`) wrote unsorted files with `sort_order_id` NULL; Spark writes each file sorted and stamped with the table's order id. Probe: the rating run's `p_sort_overwrite.py`. Spark oracle: the rating run's `write_fidelity_spark.json` (cells `sort_partitioned_local`, `sort_unpartitioned_desc`, `sort_two_keys_nulls`, `sort_dataframe_door`, `sort_distribution_none`, `sort_transform_bucket`), generator `write_fidelity_probe.py` (orchestrator scratch, not committed).

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

Verdicts are recorded once, in §5.

## 7. Gates

- `test_ice_sorted_insert_1.py -k "not live"`: 10 passed (release native, RP-22 pin).
- `test_live_cells_match_fixture` (8 cells) + recorder re-run: green, Spark 4.1.2.
- `test_writer_v2.py`: 34 passed.
- `test_sql_harden_cutover.py` + `test_insert_store_assign.py`: 59 passed, 15 skipped.
- `sort_order_id` in `python/repark/tests/*.py`: only the unit's own recorder + pin test; no other file asserts it.
- `cargo test -p repark-iceberg --lib`: 434 passed, 0 failed.
- `make rust-clippy`: clean.

VERDICT (2026-09-17): 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## 8. Round 2 (2026-09-17) — rebase onto main at RP-22

The orchestrator rebased this branch onto main (`444323f2`, RP-22 PR #667,
fork pin `96fc9f1f`, carries F-SORTED-INSERT-1 #287) and removed the
local fork-checkout override; `Cargo.toml` / `Cargo.lock` are tracked again.
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
RePark-owned paths keep the loud refusal). C-004 PROVEN: the recorder
re-run exits 0 (`oracle matches the checked-in truth`) and
`test_live_cells_match_fixture` passes 8/8 on live Spark 4.1.2 — every cell
rebuilt from the recorded DDL. The venv carries no pyspark; the live tier runs
under the venv python with sparkenv's site-packages bridged in
(the Spark env's `site-packages` on `PYTHONPATH`, both Python 3.12.3).

## 9. Round 3 (2026-09-17) — logic-critic remediation

A Grok logic critic returned NEEDS_REMEDIATION on `96d784ba` with two P1s (L-01, L-02) and three
P2s (L-03, L-04, L-05). This round closes all five: two by code, one by pins, and one by a fork
ask. It also files a second fork ask that the new measured cells turned up.

### 9.1 Rulings and assumptions

- **Q-21c-2 (orchestrator ruling):** the critic's L-04 premise ("Spark's binpack does not re-sort
  by the table default; it must not claim the default id") is overturned by measurement. Spark
  4.1.2's binpack `rewrite_data_files` re-sorts each output file and stamps `sort_order_id = 1`
  (cell `binpack_after`). The pin asserts the measured answer.
- **A-1, the second fork ask.** The measured `v*_partitioned_update` cells showed that
  `UPDATE t SET id = id WHERE p = 0` writes `sort_order_id` NULL where Spark writes 1, at both
  format versions. The snapshot carries no `engine.operation-id` and the file name is a UUIDv7,
  so the write is the fork's: `predicate_dml::try_allowed_update_in` accepts only literal
  assignments, DataFusion hands a column-expression UPDATE to the provider's `update`, and the
  fork's `IcebergUpdateExec` → `physical_plan/delete.rs::copy_on_write_update` writes with no
  default-order sort and no `with_sort_order_id`. That is table-format behaviour (rule 3), so it
  becomes `F-COW-UPDATE-STAMP-1` next to `F-RDF-SORT-STAMP-1` and is pinned strict-xfail. It is
  not routed through RePark's identity path in this unit, because widening
  `try_allowed_update_in` to expression assignments is a feature change outside this unit.
- **A-2, the fork-ask home.** `docs/fork-sync.md` had no section for pending asks (its table is
  the append-only pin history, one row per bump PR). This round adds "Open fork asks" before
  "Debug" and does not add a pin-history row.
- **A-3, the RePark binpack leg's input shape.** RePark's `CALL rewrite_data_files` refuses an
  `options` map in v1 (measured: `options map is not supported in v1`), and the fork's binpack
  default is `min_input_files = 5`. So the strict-xfail pin writes 6 sorted files per partition
  and calls with no options. Spark's cell used 3 inputs plus `rewrite-all`. The expected stamp and
  sortedness come from the fixture's `binpack_after` cell.
- **A-4, NaN canonicalisation lives in RePark.** `iceberg-datafusion`'s `CanonicalFloatExpr` is a
  private `struct` at the pinned rev (`96fc9f1f`, `physical_plan/sort.rs`), so it cannot be
  reused. `write/distribution/canonical_float.rs` reimplements it with the same mapping (every
  NaN → the canonical positive NaN for `Float32` / `Float64`; −0.0 is left alone, as in the
  fork). The negative-NaN pin rests on that fork behaviour, which is the Spark `Float.compare`
  order the brief cites; no new Spark behaviour was guessed.
- **A-5, fixture normalisation.** The committed fixture keeps the recorded per-file values
  (stamp, record count, partition, the key head of the first six values) with their key names
  made uniform (`key_head` / `key_sorted`). Files are ordered by (partition, record count, head),
  not by UUID file path, so a live re-check can compare them. Two fields are derived and not read
  from the recorded JSON: the float cell's `layout` block (108 NULL, 154 NaN, NULLs-first block,
  NaN tail block, middle strictly ascending) is the orchestrator's measured statement of the
  bytes, and its counts follow from the recorded SQL; `binpack_before.ops` is the three appends
  that the plan runs. The recorder computes both from the bytes on a live run.
- **A-6, cost.** The lineage fanout now buffers the rewrite stream before it sorts, the same
  cost the v2 arm (`fanout_sorted_*`) and the unpartitioned arm (`drive_unpartitioned`) already
  pay. `stamp` still runs once per writer construction, never per batch.

### 9.2 Findings → disposition

| Finding | Disposition | Where |
|---|---|---|
| L-01 (P1) v3 partitioned MERGE/UPDATE/DELETE stamped unsorted bytes | FIXED by sorting. `sorted_lineage_batches` sorts by the default order before the lineage fanout, and the writer is built after the sort. Transform orders still refuse loud. Lineage columns ride the sort | `merge/row_lineage.rs`; C-006, C-010 |
| L-02 (P1) the C-003 MERGE pin wrote zero files | Removed. Replaced with pins that write: MATCHED UPDATE and NOT-MATCHED INSERT of 400 shuffled keys, at v3 and v2, per-file bytes + stamp | `test_ice_sorted_insert_2.py`; C-006 |
| L-03 (P2) the owned sort did not canonicalise NaN | FIXED: `CanonicalFloatExpr::wrap` on float sort keys | `write/distribution/canonical_float.rs`; C-008 |
| L-04 (P2) `rewrite_data_files` output bypasses sort + stamp | Fork ask `F-RDF-SORT-STAMP-1`, registry `WRITE-ORDER-RDF-1` OPEN / BLOCKED-ON-FORK, strict xfail (Q-21c-2) | C-009 |
| L-05 (P2) the unpartitioned stamp site had no revert-red pin | Pinned: unpartitioned INSERT OVERWRITE stamp 1. Mutation C reds it | C-007 |
| (new) the fork's COW UPDATE exec stamps NULL | Fork ask `F-COW-UPDATE-STAMP-1`, registry `WRITE-ORDER-COW-UPDATE-1`, strict xfail (A-1) | C-009 |

### 9.3 Red first (pins run on the round-2 native, before the fix)

`test_ice_sorted_insert_2.py --runxfail` on the `96d784ba` native: 4 failed, 4 passed, 1 skipped.
The binpack leg's first failure was the `options` refusal, so A-3 reshaped it. The failures that
carry each claim:

```
test_partitioned_rewrites_sort_and_stamp[m3]
E  AssertionError: ('v3_partitioned_merge_not_matched_insert', {... 'record_count': 200,
   'sort_order_id': 1}, [5380, 5152, 5040, 5612, 5874, 5732, ...])
E  assert False  where False = _sorted_under([5380, 5152, 5040, 5612, 5874, 5732, ...], False)

test_partitioned_rewrites_sort_and_stamp[m2]
E  AssertionError: ('v2_partitioned_update', {... 'record_count': 1200, 'sort_order_id': None})
E  assert None == 1

test_owned_float_overwrite_canonicalises_negative_nan
E  AssertionError: [None, nan, 3.0, 4.0, 5.0, 6.0, ...]
E  assert False  where False = _sorted_under([None, nan, 3.0, 4.0, 5.0, 6.0, ...], descending=False)
```

The v3 line is L-01 exactly: a lineage-fanout file stamped 1 whose bytes are in arrival order.
The v2 line is the fork's UPDATE exec (A-1). The float line is L-03: arrow's total order places
the negative NaN directly after the NULL, ahead of every value.

After the fix, the three strict xfails still fail, each for the reason it claims
(`--runxfail`):

```
test_predicate_update_sorts_and_stamps_like_spark[m3]
E  AssertionError: ('v3_partitioned_update', {... 'record_count': 1200, 'sort_order_id': None})
test_predicate_update_sorts_and_stamps_like_spark[m2]
E  AssertionError: ('v2_partitioned_update', {... 'record_count': 1200, 'sort_order_id': None})
test_binpack_rewrite_sorts_and_stamps_like_spark
E  AssertionError: {... '.../p=1/compacted-00000-....parquet', 'record_count': 600, 'sort_order_id': None}
```

A separate probe of the fork's binpack output on the same shape gave 2 files × 600 rows, stamp
`None`, `sorted: False`, heads `[4, 10, 16, 60, 66, 72, 116, 122]` and
`[36, 42, 48, 92, 98, 142, 148, 154]`: unsorted and unstamped, as `WRITE-ORDER-RDF-1` states.

### 9.4 Mutation proof

Each change was reverted alone in the working tree. The release native was rebuilt, and
`test_ice_sorted_insert_1.py` + `test_ice_sorted_insert_2.py` were run. The source was then
restored. The transcripts (site → red tests → counts):

| Mutation (one change reverted) | Red tests | Counts |
|---|---|---|
| A — `append.rs:277` `stamp(…)` removed | `test_ice_sorted_insert_1.py::test_repark_owned_paths_write_sorted_stamped_files`; `test_ice_sorted_insert_2.py::test_partitioned_rewrites_sort_and_stamp[m3]`, `[m2]`, `::test_reordered_table_stamps_the_current_order`, `::test_v3_lineage_survives_the_default_order_sort` | 5 failed, 12 passed, 9 skipped, 3 xfailed |
| B — `merge/row_lineage.rs` `stamp(…)` removed | `test_partitioned_rewrites_sort_and_stamp[m3]`, `test_v3_lineage_survives_the_default_order_sort` | 2 failed, 15 passed, 9 skipped, 3 xfailed |
| C — `merge/mod.rs:1600` `stamp(…)` removed | `test_unpartitioned_overwrite_sorts_and_stamps`, `test_owned_float_overwrite_places_nan_like_spark` | 2 failed, 15 passed, 9 skipped, 3 xfailed |
| D — the lineage default-order sort removed (`sorted_lineage_batches` returns arrival order, stamp kept) | `test_partitioned_rewrites_sort_and_stamp[m3]` | 1 failed, 16 passed, 9 skipped, 3 xfailed |
| E — the sort detaches `_row_id` from its row (the `_row_id` column reversed within each sorted batch) | `test_partitioned_rewrites_sort_and_stamp[m3]`, `test_v3_lineage_survives_the_default_order_sort` | 2 failed, 15 passed, 9 skipped, 3 xfailed |
| F — `CanonicalFloatExpr::wrap` removed from the sort keys | `test_owned_float_overwrite_canonicalises_negative_nan` | 1 failed, 16 passed, 9 skipped, 3 xfailed |
| restored tree (rebuilt) | none | 17 passed, 9 skipped, 3 xfailed |

A through D are the four sites the brief requires, each red on at least one pin. E proves the
C-010 lineage pin is live: a sort that separated `_row_id` from its row would go red. F is the
revert of the L-03 fix. The 9 skips are the two files' live legs (`REPARK_PARITY_LIVE` unset);
the 3 xfails are the strict fork-blocked legs, and none of them XPASSed under any mutation.

### 9.5 Gates (round 3 head)

| Gate | Result |
|---|---|
| `cargo test -p repark-iceberg` | 436 passed, 0 failed |
| `cargo test -p repark-spark` | 1120 passed, 0 failed, 4 ignored |
| `cargo test -p repark-core` | 621 passed, 0 failed, 2 ignored |
| `make verify` (fmt, clippy, guards, ledger grammar, workspace Rust tests) | exit 0; 3714 Rust tests passed, 0 failed, 7 ignored. The first run caught two `from_iter_instead_of_collect` in `canonical_float.rs`; they were rewritten as `.collect()`, a non-semantic change, and the native was rebuilt from the final source |
| `test_ice_sorted_insert_1.py` + `test_write_order_dist_1.py` | 23 passed, 11 skipped |
| the same + `test_ice_sorted_insert_2.py`, final native | 30 passed, 12 skipped, 3 xfailed (the 3 strict fork-blocked legs) |
| whole facade suite `python/repark/tests -n 8` | 9359 passed, 3 failed, 384 skipped, 31 xfailed. All 3 failures are environmental and in files this round does not touch. `test_csv_infer_perf_1.py::test_infer_schema_true_stays_under_half_second` is a wall-clock budget under machine contention (it passes alone: 1 passed). `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren[ansi-off,ansi-on]` compares the engine's UTC `current_date` (2026-09-18) with Python's local `date.today()` (2026-09-17 EDT), so it fails after 20:00 EDT |
| parity suite `make py-test` | 756 passed, 3 skipped, 12 xfailed, exit 0 |
| comment ban (`comment_ban.py` on the head) | `comment-ban hits=0`, exit 0 against `origin/main` |


```
COVERAGE_ATTESTATION:
  pr_unit: ice-sorted-insert-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against bytes, not metadata. Each round-3 clause names the pin, the red it showed before the fix and the mutation that reds it. C-003's MERGE claim was found vacuous (L-02) and moved to C-006, not re-asserted.
      artifacts: [task/ledgers/staging/ice-sorted-insert-1-ledger.md, python/repark/tests/test_ice_sorted_insert_2.py, python/repark/tests/ice_sorted_insert_2_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Edges pinned are NULL keys first, a solid NaN block, negative NaN, a DESC order after an ALTER (current id 2), an unpartitioned table, v2 and v3, an empty-batch skip in the lineage drain, and a transform order, which still refuses loud and writes nothing.
      artifacts: [python/repark/tests/test_ice_sorted_insert_2.py, crates/repark-iceberg/src/write/distribution/canonical_float.rs]
    - id: AT-3
      status: ATTACKED
      evidence: No stamp without a sort. The lineage writer is built after the sort returns, so a sort error aborts the write before any file exists. The shapes RePark cannot fix (the fork's binpack and COW UPDATE) are strict xfails whose failures were read with --runxfail and match the claim.
      artifacts: [crates/repark-iceberg/src/write/merge/row_lineage.rs, python/repark/tests/test_ice_sorted_insert_2.py]
    - id: AT-4
      status: N/A
      justification: No concurrency surface changed. The sort runs inside one writer call before the fanout exists, and commit and OCC paths are untouched.
    - id: AT-5
      status: N/A
      justification: No auth, secrets, network, dependency, Cargo or workflow change. The recorder reads its warehouse from an argument and its Ivy cache from REPARK_ORACLE_IVY.
    - id: AT-6
      status: ATTACKED
      evidence: Every expected value is Spark 4.1.2's, read from the committed 15-cell fixture (stamp, sortedness, live row count, float layout). Q-21c-2 follows the measurement over the critic's premise.
      artifacts: [python/repark/tests/ice_sorted_insert_2_spark_oracle.json, python/repark/tests/_record_ice_sorted_insert_2_oracle.py]
    - id: AT-7
      status: ATTACKED
      evidence: The lineage arm now buffers before sorting, the cost the v2 and unpartitioned arms already pay (A-6). The float wrap runs once per sort key per batch evaluation. The stamp stays once per writer construction.
      artifacts: [crates/repark-iceberg/src/write/merge/row_lineage.rs, crates/repark-iceberg/src/write/distribution.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Table-format gaps go to the fork, not to local patches (rule 3), as two asks with measurements. The fork's private CanonicalFloatExpr is mirrored with identical mapping and cited, not redefined.
      artifacts: [docs/fork-sync.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: Strict xfails XPASS and red once either fork PR lands, forcing their un-marking. The registry rows state OPEN / BLOCKED-ON-FORK with the measurement, not a paraphrase.
      artifacts: [python/repark/tests/test_ice_sorted_insert_2.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Six single-change mutations (three stamp sites, the lineage sort, lineage detachment, NaN canonicalisation) each red a named pin and nothing is left unpinned (§9.4).
      artifacts: [crates/repark-iceberg/src/write/append.rs, crates/repark-iceberg/src/write/merge/row_lineage.rs, crates/repark-iceberg/src/write/merge/mod.rs, crates/repark-iceberg/src/write/distribution.rs]
  complete: true
```
