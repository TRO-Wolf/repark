# Charter ledger — U7-WRITE-DF-2 · WO U7 PR2: DataFrame writer semantics

**Date:** 2026-09-24 (round 2: 2026-09-25) · **Branch:** `feat/u7-write-df-2` (slice 1 as `feat/u7-write-df-2a`, repark#835) · **Base:** `9e3bf2dd` (`origin/main`, U7 PR1 merged) · **Model:** Claude Opus (`claude-opus-5-5`), per each commit's `Authored-By` trailer · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** `docs/spark-sql-iceberg-parity.md` — rows `ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE` and `EX-W2-3` FIXED, row `EX-W2-5` added (BACKLOG, residue R-1), the ICE-OVERWRITE-MODE-1 "Before/After" note on the two `saveAsTable(overwrite)` shapes dated. Round 2 (critic r1 V-001..V-005): the SAVEAS-OVERWRITE row states the by-name field ids and residues R-9/R-10; EX-W2-3 names the doors slice 1 pins.

**Retires:** in flight.

**Scope:** four scoreboard cells (`/tmp/oc-worker/scoreboard/2026-09-24`, Spark answers in
`out/spark-core.json`, bodies in `cells_write.py`): `W-DF-SAVEASTABLE-OVERWRITE`,
`W-DF-V2-OPTION-BRANCH`, `W-DF-V2-OVERWRITE-COND-PART`, `W-DF-V2-OVERWRITE-COND-ROWS`.
Slice 1 (this ledger's C-001..C-005) covers the first two:
- `crates/repark-iceberg/src/write/writer_plan.rs`: every `saveAsTable` overwrite plans
  `rtas` (Spark's `ReplaceTableAsSelect(orCreate = true)`).
- `python/repark/src/repark/spark/dataframe/writer_readwriter.py`: `DataFrameWriterV2.option`
  no longer refuses a `branch` or `tag` key.
- Round 2: `crates/repark-iceberg/src/write/replace_schema.rs` (`replacement_schema`, Java's
  by-name `assignFreshIds` through the fork's `assign_fresh_ids_with_base`), called by
  `crates/repark-spark/src/ctas.rs` before the replace's partition spec is built.

The fork, every `Cargo.toml`, `Cargo.lock` and `STATUS.md` are untouched, and no code comment
is added.

## Measurements (decide-then-build evidence)

**M-1 — the record.** On Spark 4.1.2 + Iceberg 1.11.0 (`dfw` seeds three rows with one
`INSERT … VALUES`, then writes `[(7,'g','x'),(8,'h','w')]`):
- `W-DF-SAVEASTABLE-OVERWRITE`: rows 7,8; snapshots `append` (3 records) then `overwrite`
  with `added-records=2`, `total-records=2` and no `deleted-*` counter; `main → S1`.
- `W-DF-V2-OPTION-BRANCH`: main holds rows 1,2,3,7,8, `branch_b1` holds 1,2,3; refs
  `b1 → S0`, `main → S1`. The write lands on main; the brief's caution holds.

RePark before this unit: the first cell answered the rows with an in-place `INSERT OVERWRITE`
(`deleted-records=3`, `deleted-data-files=1`); the second refused
`UnsupportedOperationException` ("writing to an Iceberg branch is not supported").

**M-2 — step-1 probes on Spark 4.1.2** (`target/probe-u7-pr2/cells_pr2.py` through a copy of
the scoreboard harness, and `target/probe-u7-pr2/record.py`, under `jvm-lock.sh`, JDK 17,
InMemoryCatalog `sc`). The recorder's shapes are committed under `measured` in
`python/repark/tests/ice_write_df_1_spark_oracle.json`. Findings:

`saveAsTable(mode="overwrite")`:
- It is an RTAS on every table: the uuid stays; properties stay (`k1`, and
  `write.distribution-mode=range` left by `WRITE ORDERED BY`); other refs stay (`b1 → S0`).
- The frame's schema replaces the table's; the `partitionBy` spec replaces the table's (none
  without `partitionBy`); the sort order becomes empty; the format version stays (3 stays 3).
- The new snapshot is an `overwrite` with no parent: `.history` shows the old snapshots as
  not current ancestors. An empty frame writes a `delete` snapshot with `total-records=0`.
- A missing table is created by the same statement (`overwrite`, not CTAS's `append`).
- The session `partitionOverwriteMode=dynamic` and the `overwrite-mode=dynamic` option change
  nothing (the option measured and pinned in round 2), and a `snapshot-property.k` option
  lands in the summary.
- Without `format(...)` the table gets `write.format.default=parquet` on the CTAS and on the
  RTAS, even over an existing `orc` (residue R-1).

The `branch` option:
- `branch` and `tag` keys are ignored on `writeTo` append, `overwritePartitions`,
  `overwrite(condition)` and `create`, on `options(BRANCH=…)`, on a branch that does not
  exist, and on `DataFrameWriter` append, overwrite and `insertInto`. The write lands on main;
  the named ref keeps its snapshot. (Spark's answers; in slice 1 RePark's
  `overwrite(condition)` still refuses, so C-005 covers the other doors.)

RePark after slice 1 (`target/probe-u7-pr2/oracle-repark-s1.json`): 21 of the 23 slice-1
shapes equal Spark's on every observation; the two others are residue R-1.

**M-3 — round 2 (critic r1), Spark 4.1.2 + Iceberg 1.11.0** (`target/probe-u7-pr2a-r1fix/
record_ids.py` under `jvm-lock.sh`, results `spark.json` / `repark.json`): the replace keeps
field ids by name. A reordered frame `(cat, data, id)` keeps ids 3, 2, 1, and `branch_b1`
reads the seed rows (RePark before: ids 1, 2, 3 by position; `branch_b1` read `[null, 'a',
null]`). A renamed or added column takes a fresh id above `last-column-id` (4). A dropped
column's id is not reused by a later re-add (5). Nested struct fields keep their ids by dotted
name. A type-changed kept name keeps its id. Identifier fields keep id 1. A reordered
`partitionBy('cat')` keeps source id 3. RePark after the fix: 34 of the 37 recorded shapes
equal Spark's, field ids included (the raw JSON of `ids_nested_reorder_branch` differs only in
how the recorder renders a struct value, a Spark `Row` list against a RePark dict; the pin
normalizes both). The three others: R-1 twice (no-format property), and R-9
(Spark's type-changed branch read fails with `ClassCastException`, RePark reads NULL). The
metadata JSON's `partition-specs` order is hash order on RePark and ascending on Spark (R-10,
not compared).

## Clauses

| ID | Proposition | Evidence required | Status | Evidence |
|---|---|---|---|---|
| C-001 | `format("iceberg").mode("overwrite").saveAsTable(t)` on an existing table replaces it exactly as cell `W-DF-SAVEASTABLE-OVERWRITE` records (rows, `overwrite` summary without `deleted-*`, refs). | `test_save_as_table_overwrite_replaces_like_the_recorded_cell`; replay. | **PROVEN** | Replay EQUAL on every observation. |
| C-002 | Every `saveAsTable` overwrite is Spark's RTAS: uuid, properties and other refs kept; schema, `partitionBy` spec and sort order replaced; no parent on the replace snapshot; a missing table created with an `overwrite` snapshot; the session `partitionOverwriteMode` and the `overwrite-mode=dynamic` option ignored; an empty frame writes `delete`. | `test_save_as_table_overwrite_shapes_match_spark` (16 shapes, `sat_overwrite_dynamic_option` among them, each also comparing field ids, schema ids and spec ids); Rust `save_as_table_statements_follow_spark_save_modes`; `test_ice_overwrite_mode_1.py::test_save_as_table_history_matches_spark` passing plainly; `test_ice_hadoop_vn_1.py::test_stale_overwrite_doors_raise` (the stale replace, full message). | **PROVEN** | M1 red. The ICE-OVERWRITE-MODE-1 strict xfail on the RTAS history retired. |
| C-003 | Residue R-1: a format-less `saveAsTable` (CTAS or RTAS) stores no `write.format.default`, where Spark stores `parquet`; every other observation equals Spark's. | `test_save_as_table_without_a_format_writes_no_format_property_divergence`; registry `EX-W2-5`. | **PROVEN** | Divergence pin, BACKLOG row. |
| C-004 | `writeTo(t).option("branch", "b1").append()` writes main and leaves `b1` at S0, as cell `W-DF-V2-OPTION-BRANCH` records. | `test_writer_v2_branch_option_writes_main_like_the_recorded_cell`, `test_examples_window_catalog.py::test_writerv2_option_branch_writes_the_default_branch`; replay. | **PROVEN** | M2 red. |
| C-005 | A `branch` or `tag` key, in any case, is ignored on `writeTo` `append`, `overwritePartitions` and `create`, and on `DataFrameWriter` `saveAsTable` append, `saveAsTable` overwrite and `insertInto` overwrite, even for a missing ref: the write lands on main and the ref keeps its snapshot. | `test_branch_and_tag_options_are_ignored_like_spark` (9 shapes, one per door plus tag, upper case and missing ref), `test_time_travel.py::test_write_to_branch_option_writes_main`, `::test_write_to_tag_option_writes_main`. | **PROVEN** | M2 red on six of them. Narrowed 2026-09-25 (critic V-004): `overwrite(condition)` is not a slice-1 door. |
| C-011 | A replace keeps each column's field id by name (Java `TypeUtil.assignFreshIds(schema, base, nextId)`): reordered, swapped and renamed columns, added and re-added ones (fresh ids above `last-column-id`, a dropped id never reused), nested struct fields by dotted name, identifier fields and a reordered `partitionBy` source; a branch on a pre-replace snapshot reads its rows. | `test_save_as_table_overwrite_keeps_field_ids_by_name_like_spark` (9 shapes); Rust `tests/replace_schema.rs` (3). | **PROVEN** | M3 and M4 red. |
| C-012 | Residue R-9: after a replace that changes a kept column's type, the old branch reads NULL for it where Spark fails the read (`ClassCastException`); every other observation, the kept id included, equals Spark's. | `test_a_type_change_on_a_kept_name_reads_the_old_branch_as_null_divergence`. | **PROVEN** | Divergence pin; registry SAVEAS-OVERWRITE Residual. |

## Pre-existing pins changed (slice 1)

- `test_ice_v3_write_default_1.py::test_saveastable_overwrite_is_insert_overwrite_not_replace`
  flipped red as its row promised; it is now
  `test_saveastable_overwrite_replaces_the_table_like_spark` against the same recorded cell
  and schema.
- `test_examples_window_catalog.py::test_writerv2_option_branch_refuses` is now
  `test_writerv2_option_branch_writes_the_default_branch` (EX-W2-3's measured Spark answer).
- `test_ice_hadoop_vn_1.py::test_stale_overwrite_doors_raise`: a stale-pointer
  `saveAsTable(overwrite)` now fails on the replace door, `CatalogCommitConflicts => Cannot
  stage replace to …`, as the SQL `CREATE OR REPLACE` does in
  `test_stale_replace_raises_conflict`. Round 2 (critic V-002): `_assert_stale_write_raises`
  takes the message start as a parameter, so this door keeps every other observation (exact
  class, `version file already exists`, `v3.metadata.json`, the metadata names, v3's bytes),
  and the full message is pinned with `==`.
- `test_time_travel.py::test_write_to_branch_unsupported` / `test_write_to_tag_unsupported`
  (the V2 refusal slice 1 removed) are `test_write_to_branch_option_writes_main` /
  `test_write_to_tag_option_writes_main` (round 2; the round-1 gate list missed this file).
- `test_ice_overwrite_mode_1.py::test_save_as_table_history_matches_spark` loses its strict
  xfail.

## Mutation (step 6, `target/probe-u7-pr2/mutation-*.txt`)

Each load-bearing line was broken, its named test run, and the source restored.
- **M1:** `plan_save_as_table` answers `("overwrite", true) => Overwrite` again.
  `save_as_table_statements_follow_spark_save_modes` goes red: `left: ("overwrite", false)
  right: ("rtas", false)`.
- **M2:** `DataFrameWriterV2.option` raises on a `branch`/`tag` key again.
  `test_writer_v2_branch_option_writes_main_like_the_recorded_cell` and six
  `test_branch_and_tag_options_are_ignored_like_spark` shapes go red.
- **M3** (round 2, `target/probe-u7-pr2a-r1fix/mutation-M3.txt`): `replacement_schema`
  returns the frame's schema unchanged. All three `tests/replace_schema.rs` tests go red
  (`left: [(1, "id"), (2, "new")] right: [(1, "id"), (4, "new")]`).
- **M4** (`mutation-M4.txt`, rebuilt wheel): `ctas.rs` skips `replacement_schema`. Six
  `test_save_as_table_overwrite_keeps_field_ids_by_name_like_spark` shapes go red (reorder,
  swap, rename, nested, reordered `partitionBy`, re-add).

## Out of scope (observed, not worked)

- Residue R-1 (C-003, registry `EX-W2-5`): the format-less provider property on CTAS and RTAS.
- Residue R-9 (C-012): a type-changed kept column reads NULL on a pre-replace branch; Spark
  fails the read.
- Residue R-10 (dated 2026-09-25, critic V-005): the fork's `TableMetadata` serializer writes
  `schemas`, `partition-specs` and `sort-orders` in `HashMap` order. After
  `partitionBy('data')` over a `cat`-partitioned table, RePark wrote `[spec 1, spec 0]` and
  Spark `[spec 0, spec 1]`. Ids, fields, `default-spec-id` and `last-partition-id` are equal.
  The order varies run to run and SQL cannot observe it, so the pins compare the sorted ids.
  This is a fork change (hand-back Q1).
- The fork's `StagedTableTransaction::begin_replace` does not run Java's `assignFreshIds`
  itself; RePark calls the fork's port before staging (hand-back Q1).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u7-write-df-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The recorded cells are pinned from the scoreboard record and every measured
        slice-1 shape is compared on rows, operations, summary counters, `.files` specs,
        partitioning, schema, refs, history, selected properties, format version, sort
        fields, the uuid, field ids by name, identifier ids, last-column-id, schema ids and
        spec source/field ids; residues R-1 and R-9 are divergence pins.
      artifacts: [python/repark/tests/test_ice_write_df_2.py, python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: The oracle is live Spark 4.1.2 + Iceberg 1.11.0 (scoreboard record plus
        `target/probe-u7-pr2/record.py` under jvm-lock.sh); no expected value is derived from
        RePark output or typed in beside the oracle.
      artifacts: [python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-3
      status: ATTACKED
      evidence: The stale-pointer replace keeps the metadata files unchanged; no refusal is
        added in slice 1.
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added.
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temp dirs; the JVM ran only for the probes under the lock;
        no network or credentials.
      artifacts: [task/ledgers/staging/u7-write-df-2-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: One mutation per new path (M1..M4), each red on its named test (section Mutation).
      artifacts: [crates/repark-iceberg/src/tests/writer_plan.rs, crates/repark-iceberg/src/tests/replace_schema.rs, python/repark/tests/test_ice_write_df_2.py]
    - id: AT-7
      status: N/A
      justification: No performance claim.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml, lockfile, fork or workflow change; `writer_readwriter.py`
        ratchets 1039 to 1033 with its CAP-1 mirror row.
      artifacts: [scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: ATTACKED
      evidence: Registry rows ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE and EX-W2-3 FIXED, EX-W2-5
        added; the touched map.md files in lockstep.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The saveAsTable-overwrite and writer suites stay green; the pre-existing pins
        changed are listed with their reason.
      artifacts: [python/repark/tests/test_ice_overwrite_mode_1.py, python/repark/tests/test_ice_v3_write_default_1.py]
  complete: true
```
