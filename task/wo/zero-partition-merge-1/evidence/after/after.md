# ZERO-PARTITION-MERGE-1: the fix, its pins and its gates

Model `claude-opus-5-5`, 2026-11-13. The fork commit is `bc5a9211`, on fork main `8b6ff921`. The RePark branch
`fix/zero-partition-merge-1` has code commit `9955baa2`, merged with main `69aba5df` (#1325). The step-0 inventory
critic returned GAPS; the order's author ruled the seats, doors and defaults, and the owner ruled Q1–Q3, recorded in
the [card](../../../../roadmap/mid-term/zero-partition-merge-1-card-2026-11-13.md).

## Seats and files

| File | Change | Pinned by |
|---|---|---|
| fork `crates/iceberg/src/spec/values/struct_value.rs` | `Struct`'s `PartialEq`/`Eq`/`Hash` are hand-written. FLOAT and DOUBLE leaves, top level, in lists and in nested structs, compare and hash by bits after NaN canonicalisation; every other leaf keeps the derived rule | its tests `signed_zeros_are_two_partition_keys`, `nested_float_leaves_follow_the_same_rule` (red on `8b6ff921`), `every_nan_is_one_partition_key`, `other_leaves_keep_their_equality` (controls, green on both) |
| fork `crates/integrations/datafusion/src/physical_plan/sort.rs` | `canonical_partition_child`, the leading `_partition` key of a write order, canonicalises NaN only (it added `+ 0.0`) | `partition_sort_key_keeps_signed_zeros_apart_and_folds_nan` (red on `8b6ff921`) |
| RePark `crates/repark-iceberg/src/write/summary_collision.rs` | `replaced_data_files` matches live files by `(spec id, Struct)`, the fork's replace-partitions key | `test_overwrite_partitions_summary_counts_only_the_replaced_zeros_files`, added in the delta round. The two cells first cited here, `dynamic_overwrite_cross` and `overwrite_partitions_cross`, pass no snapshot property and never reach it (verifier 1, B1) |
| RePark `Cargo.toml`, `Cargo.lock` | the fork pin `8b6ff921` → `bc5a9211`. It resolves locally only until the fork branch is pushed | — |

**Fork tests that pinned the merge, changed:**
- `crates/iceberg/src/arrow/record_batch_partition_splitter.rs` `test_float_partition_values_stay_on_the_row_wise_split`:
  it expected 2 parts for `[0.0, -0.0, 1.0, 0.0]`. It now expects 3, with each part's partition bits and stored
  bits.
- `crates/integrations/datafusion/tests/sorted_insert.rs` `float_partition_signed_zero_stays_sorted_by_id`, renamed
  `float_partition_signed_zeros_write_two_partitions_sorted_by_id`: it expected one file for INSERT and for COW
  UPDATE. It now expects the two zero partitions, `[2]` and `[1, 3]`, each in id order, with the order id stamped.

**Not changed (ruling 2):** the comparators that answer `Equal` for the two zeros: four found here, and a fifth,
fork `ascending_partition_order`, found by verifier 2 (F2). The card lists all five, with lines re-checked in the
delta round, for a follow-up. No pin depends on their order.

## Facade pin `python/repark/tests/iceberg/test_zero_partition_merge_1.py`

The test runs 18 doors x `PARTITIONED BY (d)` / `(f)` x `spark.sql.shuffle.partitions` 1 and 16.

- **Doors:**
  - insert, on Parquet, ORC and Avro;
  - CTAS, `writeTo().create()` (v3), `saveAsTable` overwrite;
  - v3 MERGE insert, v3 merge-on-read UPDATE, v3 copy-on-write DELETE;
  - `rewrite_data_files`;
  - dynamic `INSERT OVERWRITE` and `overwritePartitions`, each over an existing `-0.0` partition;
  - a branch INSERT;
  - partition-granularity deletes, `rewrite_position_delete_files`, `run_maintenance`;
  - a write-ordered table with fan-out on, and the same table with fan-out off.
- **What each cell asserts:** each row reads its own zero, the data files hold exactly the zero partitions the rows
  need, and, where the cell writes deletes, the delete files hold both zero partitions.

| Build | 1 CPU (`taskset` 1) | 10 CPUs |
|---|---|---|
| main `1d9d2808` (`pkg-base`) | 60 failed, 12 passed | 60 failed, 12 passed |
| this fix | 72 passed | 72 passed |
| live tier (`REPARK_PARITY_LIVE=1`, Spark 4.1.2 reads every table, plus Spark's own refusal on the write-ordered cell): main | 120 failed, 26 passed | — |
| live tier: this fix | 146 passed | — |

- **Red on main:** 60 of 72 fail; the 12 that pass are cells whose zeros main already kept apart at that writer count. The live tier turns
  the hidden ones red too: Spark returns the deleted `-0.0` row from main's partition-granularity and
  `run_maintenance` tables.
- **Writer counts:** the 1 and 16 writers come from the test's own parameter, so the pin is red on main on any host.
- **Declared divergence:** `test_spark_refuses_interleaved_zeros_on_a_write_ordered_table` pins Spark's refusal,
  parity row ZPM-1.

## The step-0 matrix on the fixed build

The fixed build ran on 1 CPU and on 10 CPUs with 16 partitions ([raw/](raw/map.md)).
- All 288 Spark-door tables are `ok` against Spark's answer. Step 0 found 237 non-ok on 1 CPU: 189 merged, 24 lost, 18 mixed, 6 with a merged delete file.
- The two overwrite spellings still refuse, as on main; they are outside this card.
- Stored bits: 832 Parquet/ORC data files read, the same count as Spark's own run; 0 rows changed; 0 files hold both
  zeros.

## Gates

| Gate | Exit |
|---|---|
| fork `cargo test -p iceberg` (5,197 passed) | 0 |
| fork `cargo test -p iceberg-datafusion` (803 passed; `sorted_insert` re-run after the size trim, 17 passed) | 0 |
| fork `cargo fmt --all -- --check` | 0 |
| fork `cargo clippy -p iceberg --all-targets -- -D warnings` | 0 |
| fork `cargo clippy -p iceberg-datafusion --all-targets -- -D warnings` | 0 |
| fork `scripts/check_rust_file_size.sh`, `check_comment_blocks.sh`, `check_agent_artifacts.sh`, `check_matrix_anchors.sh` | 0 |
| fork comment gate (`comment_ban.py`) | 0 hits |
| `cargo fmt --all -- --check` | 0 |
| `cargo test --locked -p repark-iceberg` | 0 |
| `make rust-clippy` | 0 |
| `make rust-panic-ban` | 0 |
| `make py-test-facade FACADE_WORKERS=6` (28,548 passed, 812 skipped, 152 xfailed) | 0 |
| `make py-test-facade FACADE_PROFILE=release FACADE_WORKERS=6` (28,553 passed, 807 skipped, 152 xfailed) | 0 |
| map sync, map.md, crate DAG, manifest, docs compaction, map size, ledger grammar, docs links | 0 |
| typos, `ruff check .`, `ruff format --check .` | 0 |
| `python/repark-parity/tests/test_v1_gate_docs.py` (14 passed) | 0 |
| comment gate (`comment_ban.py`, both clones) | 0 hits |

## Delta round (2026-11-14), after both verifiers' FAIL

Both verifiers failed #1332 on class (b) findings only: verifier 1's B1 and B2, verifier 2's F1 and F2. The delta
changes the pin file and the records, and no product file. The builds are the same: head `pkg-1332` (`1bf566ee`)
and base `pkg-base` (main `1d9d2808`), run on 16 CPUs.

- **B1, the pin.** `test_overwrite_partitions_summary_counts_only_the_replaced_zeros_files` seeds two `-0.0` files,
  one `0.0` file and one `1.5` file, then runs `overwritePartitions` with `snapshot-property.deleted-data-files`,
  once with a `0.0` row and once with a `-0.0` row. It asserts Spark's count in the refusal (`1` and `2`) and the
  unchanged rows. Base answers `3` in all four cells, DOUBLE and FLOAT.
- **B2, ZPM-1's second half.** Door `spark_fanout_off` (no write order, `write.spark.fanout.enabled = false`, the
  six base rows) joins the facade cells, and `test_spark_refuses_interleaved_zeros_with_its_fanout_off` pins Spark's
  refusal.
- **The pin file:**

  | Build | Result |
  |---|---|
  | base | 66 failed, 14 passed, 80 skipped: the 60 of the first round, `spark_fanout_off` at 1 writer (2) and the four summary cells |
  | head | 80 passed, 80 skipped |
  | head, live tier, `-k fanout_off`, run once | 10 passed: RePark's four cells, Spark's four reads of RePark's tables, and Spark's refusal on DOUBLE and FLOAT (`IllegalStateException` from `ClusteredWriter.java:96`) |

- **Control probe** (Spark 4.1.2 with Iceberg 1.11.0, `local[2]`, one table per cell): with
  `write.spark.fanout.enabled = false` and no write order, `1.5` and `2.0` rows interleaved like the zeros are
  written as two files, and the zeros are refused. Without the property, both are written as two files.
- **Records:** ZPM-1 widened and ZPM-2 added, both PROPOSED with the owner's word owed; the card's Q1 text, its five
  comparators and F3; SIGNED-ZERO-WRITE-METADATA-1's `.partitions` half marked fixed; the new card
  SIGNED-ZERO-PARTITION-FILTER-1.
