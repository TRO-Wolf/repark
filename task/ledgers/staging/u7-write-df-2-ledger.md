# Charter ledger — U7-WRITE-DF-2 · WO U7 PR2: DataFrame writer semantics

**Date:** 2026-09-24 · **Branch:** `feat/u7-write-df-2` · **Base:** `9e3bf2dd` (`origin/main`, U7 PR1 merged) · **Model:** Claude Opus (`claude-opus-5-5`), per each commit's `Authored-By` trailer · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** `docs/spark-sql-iceberg-parity.md` — rows `ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE` and `EX-W2-3` FIXED, row `EX-W2-5` added (BACKLOG, residue R-1), the ICE-OVERWRITE-MODE-1 "Before/After" note on the two `saveAsTable(overwrite)` shapes dated.

**Retires:** in flight.

**Scope:** four scoreboard cells (`/tmp/oc-worker/scoreboard/2026-09-24`, Spark answers in
`out/spark-core.json`, bodies in `cells_write.py`): `W-DF-SAVEASTABLE-OVERWRITE`,
`W-DF-V2-OPTION-BRANCH`, `W-DF-V2-OVERWRITE-COND-PART`, `W-DF-V2-OVERWRITE-COND-ROWS`.
Slice 1 (this ledger's C-001..C-005) covers the first two:
- `crates/repark-iceberg/src/write/writer_plan.rs`: every `saveAsTable` overwrite plans
  `rtas` (Spark's `ReplaceTableAsSelect(orCreate = true)`).
- `python/repark/src/repark/spark/dataframe/writer_readwriter.py`: `DataFrameWriterV2.option`
  no longer refuses a `branch` or `tag` key.

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
  nothing, and a `snapshot-property.k` option lands in the summary.
- Without `format(...)` the table gets `write.format.default=parquet` on the CTAS and on the
  RTAS, even over an existing `orc` (residue R-1).

The `branch` option:
- `branch` and `tag` keys are ignored on `writeTo` append, `overwritePartitions`,
  `overwrite(condition)` and `create`, on `options(BRANCH=…)`, on a branch that does not
  exist, and on `DataFrameWriter` append, overwrite and `insertInto`. The write lands on main;
  the named ref keeps its snapshot.

RePark after slice 1 (`target/probe-u7-pr2/oracle-repark-s1.json`): 21 of the 23 slice-1
shapes equal Spark's on every observation; the two others are residue R-1.

## Clauses

| ID | Proposition | Evidence required | Status | Evidence |
|---|---|---|---|---|
| C-001 | `format("iceberg").mode("overwrite").saveAsTable(t)` on an existing table replaces it exactly as cell `W-DF-SAVEASTABLE-OVERWRITE` records (rows, `overwrite` summary without `deleted-*`, refs). | `test_save_as_table_overwrite_replaces_like_the_recorded_cell`; replay. | **PROVEN** | Replay EQUAL on every observation. |
| C-002 | Every `saveAsTable` overwrite is Spark's RTAS: uuid, properties and other refs kept; schema, `partitionBy` spec and sort order replaced; no parent on the replace snapshot; a missing table created with an `overwrite` snapshot; dynamic modes ignored; an empty frame writes `delete`. | `test_save_as_table_overwrite_shapes_match_spark` (14 shapes); Rust `save_as_table_statements_follow_spark_save_modes`; `test_ice_overwrite_mode_1.py::test_save_as_table_history_matches_spark` passing plainly. | **PROVEN** | M1 red. The ICE-OVERWRITE-MODE-1 strict xfail on the RTAS history retired. |
| C-003 | Residue R-1: a format-less `saveAsTable` (CTAS or RTAS) stores no `write.format.default`, where Spark stores `parquet`; every other observation equals Spark's. | `test_save_as_table_without_a_format_writes_no_format_property_divergence`; registry `EX-W2-5`. | **PROVEN** | Divergence pin, BACKLOG row. |
| C-004 | `writeTo(t).option("branch", "b1").append()` writes main and leaves `b1` at S0, as cell `W-DF-V2-OPTION-BRANCH` records. | `test_writer_v2_branch_option_writes_main_like_the_recorded_cell`, `test_examples_window_catalog.py::test_writerv2_option_branch_writes_the_default_branch`; replay. | **PROVEN** | M2 red. |
| C-005 | A `branch` or `tag` key, in any case and on any writer, is ignored, even for a missing ref: the write lands on main. | `test_branch_and_tag_options_are_ignored_like_spark` (7 shapes). | **PROVEN** | M2 red on six of them. |

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
  `test_stale_replace_raises_conflict`; the metadata files stay unchanged.
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

## Out of scope (observed, not worked)

- Residue R-1 (C-003, registry `EX-W2-5`): the format-less provider property on CTAS and RTAS.

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
        fields and the uuid; residue R-1 is a divergence pin.
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
      evidence: One mutation per new path, each red on its named test (section Mutation).
      artifacts: [crates/repark-iceberg/src/tests/writer_plan.rs, python/repark/tests/test_ice_write_df_2.py]
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
