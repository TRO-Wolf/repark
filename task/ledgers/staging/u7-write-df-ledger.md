# Charter ledger — U7-WRITE-DF · WO U7 PR1: DataFrame writer `save(name)`, `bucketBy`, `output-spec-id`

**Date:** 2026-09-24 · **Branch:** `feat/u7-write-df` · **Base:** `0b9dfb3c` (`origin/main`) · **Model:** Claude Opus (`claude-opus-5-5`), per each commit's `Authored-By` trailer · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** `docs/spark-sql-iceberg-parity.md` — row `IO-BUCKET-1` narrowed (the R-1 saveAsTable refusal retired, one residual kept), row `ICE-WRITE-DF-SAVE-1` added (table names FIXED, path create modes DECLARED), row `ICE-WRITE-OPTIONS-1` gains a U7 PR1 bullet (`output-spec-id` honoured). Row `EX-IO-5` stays true unchanged: a default-format `save()` keeps its refusal.

**Retires:** in flight.

**Scope:** four scoreboard cells (`/tmp/oc-worker/scoreboard/2026-09-24`, Spark answers in
`out/spark-core.json`, bodies in `cells_write.py`), all four EQUAL after this unit:
`W-DF-SAVE-NAME`, `W-DF-SAVE-OVERWRITE-NAME`, `W-DF-V1-BUCKETBY-ERR`, `W-DF-OPT-OUTPUT-SPEC-ID`.
The decisions are Rust kernels:
- `crates/repark-iceberg/src/write/output_spec.rs`: parse, validate and apply the staging view for `output-spec-id`.
- `crates/repark-iceberg/src/write/writer_partitioning.rs`: the layout comparison and the `save()` target decision.
- `crates/repark-spark/src/normalize/clustered_by.rs`: the `CLUSTERED BY` merge and refusals.
- `crates/repark-spark/src/normalize.rs`: case-aware partition columns.

They reach Python through `crates/repark-core/src/session/writer_layout.rs` and
`crates/repark-python/src/writer_layout.rs`. The facade stays SQL composition in the new
`python/repark/src/repark/spark/dataframe/writer_save.py`. The fork, every `Cargo.toml`,
`Cargo.lock` and `STATUS.md` are untouched, and no code comment is added.

## Measurements (decide-then-build evidence)

**M-1 — the record.** The four cells above as recorded on Spark 4.1.2 + Iceberg 1.11.0
(`dfw` seeds `t (id BIGINT, data STRING, cat STRING)` with rows 1,2,3 and writes
`[(7,'g','x'),(8,'h','w')]`):
- `W-DF-SAVE-NAME`: rows 1,2,3,7,8; `[append, append]`.
- `W-DF-SAVE-OVERWRITE-NAME`: rows 7,8; `[append, overwrite]` with `deleted-records=3`.
- `W-DF-V1-BUCKETBY-ERR`: spec `[["id_bucket","bucket[4]","id"]]`; one `append` of 2 rows.
- `W-DF-OPT-OUTPUT-SPEC-ID`: `specs [[0, 2]]`.

RePark before this unit:
- both `save` cells: the `requires format(...)` refusal;
- the bucketBy cell: `NOT_IMPLEMENTED`;
- the output-spec cell: `specs [[0,1],[1,2]]`.

**M-2 — step-1 probes on Spark 4.1.2** (`target/probe-u7-pr1/probe.py`, `probe2.py`,
`probe3.py` under `jvm-lock.sh`, JDK 17, InMemoryCatalog `sc`). All 61 cells are
committed as `measured` in `python/repark/tests/ice_write_df_1_spark_oracle.json`.
Findings the record left open:

`save(name)`:
- On a missing table, append and overwrite raise `TABLE_OR_VIEW_NOT_FOUND`
  (`relationName u7.missing_a`, unquoted, no catalog).
- On a missing table, the default and ignore modes create it, and `partitionBy` is honoured.
- On an existing table, the error modes raise `TABLE_OR_VIEW_ALREADY_EXISTS`
  `` `u7`.`t` ``, and ignore is a no-op.
- A `partitionBy` that differs from the table raises `IllegalArgumentException`
  `requirement failed: … - provided: identity(cat)\n - table: `.

`save('/path')`:
- Append and overwrite raise `TABLE_OR_VIEW_NOT_FOUND` `` `/parent`.leaf ``.
- The create modes resolve through the session catalog and fail
  `Failed to connect to Hive Metastore` in the probe session.

`bucketBy`:
- On an existing unbucketed table, append refuses the layout mismatch.
- Overwrite replaces the table (spec id 1).
- Error raises `TABLE_OR_VIEW_ALREADY_EXISTS`.
- Ignore is a no-op.
- A matching `bucket(4, id)` table appends.
- `bucket(8, id)` and `bucket(4, ID)` mismatch; the comparison is case-sensitive.
- `sortBy` and multi-column refuse `Cannot convert transform with more than one column
  reference: sorted_bucket(id, 4, data)` / `bucket(4, id, data)`.
- A missing column raises `_LEGACY_ERROR_TEMP_3060` (new table) or the mismatch (existing
  table).
- `ID` on a new table resolves to `id_bucket`.
- On a new table, overwrite commits an `overwrite` snapshot (RTAS).

`output-spec-id`:
- Unknown or negative ids raise `Output spec id 7 is not a valid spec id for table`.
- `x` raises `NumberFormatException` `For input string: "x"`.
- The current id writes under spec 1.
- Spec 0 is honoured by `insertInto`, `writeTo.append`, `save(name)`, overwrite and
  `overwritePartitions`.

SQL twins (all measured on Spark):
- `CLUSTERED BY` merges into `PARTITIONED BY` last.
- Multi-column and `SORTED BY` refuse with the texts above, for CTAS and column-def CREATE alike.
- `PARTITIONED BY (CAT)` and `bucket(4, ID)` resolve case-insensitively for CTAS and column-def CREATE.

**M-3 — RePark after** (`target/probe-u7-pr1/repark_edges.py`, debug wheel). Every M-2 shape
answers as Spark except:
- the path create modes (declared);
- the missing-bucket-column class (Q1);
- `saveAsTable` error mode on an existing table, which keeps RePark's pre-existing text with
  the same condition.

The replay of the four cells is EQUAL on every observation (`target/u7-pr1-replay.json`).

## Clauses

| Clause | Proposition | Evidence | Verdict | Notes |
|---|---|---|---|---|
| C-001 | `format("iceberg").mode("append").save("sc.u7.t")` appends by name exactly as cell `W-DF-SAVE-NAME` records. | `test_save_name_appends_like_the_recorded_cell`; replay. | **PROVEN** | Replay: data, md.spec, md.refs, md.snapshots, layout.snapshots all EQUAL. M8 red. |
| C-002 | `mode("overwrite").save(name)` is a whole-table overwrite with `deleted-records=3` (cell `W-DF-SAVE-OVERWRITE-NAME`). | `test_save_name_overwrites_like_the_recorded_cell`; replay. | **PROVEN** | Static by-name `INSERT OVERWRITE`, as Spark's `OverwriteByExpression(true)`. |
| C-003 | On a missing table, append and overwrite refuse `TABLE_OR_VIEW_NOT_FOUND` with Spark's relation text; error and ignore create the table (with `partitionBy`). | `test_save_name_on_a_missing_table_refuses_in_write_modes`, `test_save_name_creates_a_missing_table_in_create_modes`. | **PROVEN** | Kernel `decide_save_target`; text from `repark_common::spark_error`. |
| C-004 | On an existing table, the error modes refuse `TABLE_OR_VIEW_ALREADY_EXISTS` `` `u7`.`t` ``, ignore is a no-op, and `option("path", name).save()` routes the same way. | `test_save_name_on_an_existing_table_refuses_in_error_mode`, `test_save_reads_the_name_from_the_path_option`. | **PROVEN** | Messages equal Spark's byte for byte. |
| C-005 | A `partitionBy` (and `bucketBy`) layout on an existing table must equal the table's partitioning as Spark's transform list, else `IllegalArgumentException` `requirement failed: …`. | `test_save_name_refuses_a_partitioning_that_differs_from_the_table`, `test_save_name_with_a_matching_partitioning_appends`; Rust `tests/writer_partitioning.rs`. | **PROVEN** | M2 red. |
| C-006 | A path target: append and overwrite refuse `TABLE_OR_VIEW_NOT_FOUND` `` `<parent>`.<leaf> ``; create modes and a default-format `save()` keep the declared refusal. | `test_save_to_a_real_path_refuses_with_spark_path_relation`, `test_save_to_a_real_path_in_create_mode_is_a_declared_refusal`, `test_examples_io_session.py::test_save_default_format_refuses` (unchanged, green). | **PROVEN** | Registry `ICE-WRITE-DF-SAVE-1` (DECLARED part). |
| C-007 | `bucketBy(4, "id").saveAsTable` on a new table creates spec `id_bucket bucket[4] id` and writes one append; with `partitionBy` the bucket is last; `ID` resolves; overwrite is RTAS (`overwrite` snapshot). | `test_bucket_by_creates_a_bucket_partitioned_table`, `test_bucket_by_new_table_shapes`; replay. | **PROVEN** | M9 red. |
| C-008 | `sortBy` or a multi-column bucket refuses with Spark's `Cannot convert transform…` text and creates nothing. | `test_bucket_by_shapes_iceberg_cannot_convert_refuse`, `test_io_bucket_cluster_1.py::test_bucket_by_sort_by_save_as_table_refuses_like_iceberg`. | **PROVEN** | M5 red. R-1 pin retired. |
| C-009 | Bucketed saveAsTable on an existing table: append checks the layout, overwrite replaces (RTAS, next spec id), error and ignore keep their answers. | `test_bucket_by_existing_unbucketed_table`, `test_bucket_by_existing_bucketed_table`, `test_bucket_by_existing_partitioned_table`, `test_bucket_by_existing_table_overwrite_with_partition_by`. | **PROVEN** | M3 red on the save-target arm. |
| C-010 | `output-spec-id=<old id>` stages data files under that spec on every option-carrying writer; `.files` reports them there (cell `W-DF-OPT-OUTPUT-SPEC-ID`). | `test_output_spec_id_writes_under_the_old_spec`, `test_output_spec_id_reaches_every_writer`, `test_output_spec_id_writes_partitioned_files_under_an_old_partitioned_spec`; Rust `tests/output_spec.rs`. | **PROVEN** | M1 and M7 red. |
| C-011 | An unknown or negative id refuses `Output spec id <n> is not a valid spec id for table`, and a non-integer refuses `For input string: "<v>"`, both before any commit; the current id writes normally. | `test_output_spec_id_refusals`, `test_output_spec_id_current_and_absent_land_under_the_current_spec`. | **PROVEN** | Class `IllegalArgumentException` (Spark's `NumberFormatException` is its subclass). |
| C-012 | Near misses keep their paths: a parquet or csv `save(path)`, a `saveAsTable` without `bucketBy`, and a write without the option (current spec). | `test_near_misses_keep_their_paths`, `test_output_spec_id_current_and_absent_land_under_the_current_spec`; the 2883-test writer sweep. | **PROVEN** | The only pre-existing pin changed is R-1 (C-008). |
| C-013 | SQL twins: `CLUSTERED BY` joins `PARTITIONED BY` last, multi-column and sorted runs refuse with Spark's text, and partition columns fold case only under a case-insensitive session. | `test_sql_door_bucket_clauses`, `test_sql_door_bucket_refusals`, `test_sql_door_create_resolves_partition_columns_case_free`; `clustered_by` and `normalize::tests` units. | **PROVEN** | M4, M5 and M6 red. |
| C-014 | The Rust kernels carry in-crate pins: staging view, parse and validate refusals, layout rendering and comparison, save-target matrix. | `crates/repark-iceberg/src/tests/output_spec.rs` (5), `tests/writer_partitioning.rs` (7). | **PROVEN** | `cargo test -p repark-iceberg --lib -- tests::output_spec tests::writer_partitioning`: 12 passed. |

## Mutation (step 6, `target/probe-u7-pr1/mutate.py`, `mutation.txt`)

Each load-bearing line was broken, its named test run, and the source restored.
- **M1:** `output_spec.rs` staging never swaps. `append_with_an_old_output_spec_commits_files_under_that_spec` goes red: `left: [1, 1] right: [0]`.
- **M2:** `writer_partitioning.rs` comparison inverted. `a_mismatched_layout_refuses_with_spark_requirement_text` goes red (`expect_err` panicked).
- **M3:** the append arm returns Overwrite. `save_target_maps_mode_and_existence_like_spark_save` goes red: `left: Overwrite right: Append`.
- **M4:** the `PARTITIONED BY` merge is skipped. `the_bucket_joins_an_existing_partitioned_by_list_last` goes red, with a second `PARTITIONED BY` clause emitted.
- **M5:** the refusal is only for multi-column AND sorted. `multi_column_and_sorted_buckets_refuse_with_spark_text` goes red.
- **M6:** the fold is inverted. `partition_columns_fold_case_only_when_the_session_does` goes red.
- **M7:** the option key is renamed. `output_spec_id_parses_and_reaches_staging` goes red: `left: None right: Some(0)`.
- **M8:** `writer_save.py` overwrite routing is flipped. `test_save_name_appends_like_the_recorded_cell` goes red, showing rows 7,8 against 1,2,3,7,8.
- **M9:** `bucket_clause` returns `""`. `test_bucket_by_creates_a_bucket_partitioned_table` goes red, with spec `[]` against `id_bucket`.

## Out of scope (observed, not worked)

- `saveAsTable` error mode on an existing table keeps RePark's text
  `table '<q>' already exists; use mode(...)`, where Spark says `Cannot create table or view
  `u7`.`t` because it already exists…` (same condition and SQLSTATE).
- A non-bucketed `saveAsTable` overwrite on a missing table records `append` (CTAS), where
  Spark records `overwrite` (RTAS). The same holds for a non-bucketed overwrite of an
  existing table: Spark replaces the table, RePark runs `INSERT OVERWRITE`. Both were left
  unchanged as brief near misses.
- `PARTITIONED BY (bucket(4, id, data))` keeps V3-MULTIARG-1's declared RePark text; Spark
  says `Cannot convert transform with more than one column reference: bucket(4, id, data)`.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u7-write-df
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every Spark-visible answer is pinned against the committed oracle (four recorded
        cells verbatim plus 61 measured shapes); the four cells replay EQUAL on every observation.
      artifacts: [python/repark/tests/test_ice_write_df_1.py, python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: The oracle is live Spark 4.1.2 + Iceberg 1.11.0 (scoreboard record plus step-1
        probes under jvm-lock.sh); no expected value is derived from RePark output.
      artifacts: [python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal (not found, already exists, layout mismatch, cannot convert, spec id,
        parse) is pinned with its class and full text, and no table or commit appears after it.
      artifacts: [python/repark/tests/test_ice_write_df_1.py, crates/repark-iceberg/src/tests/writer_partitioning.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added; the staging view is a per-write read-only
        Table built from cloned metadata.
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temp dirs; the JVM ran only for the probes under the lock;
        no network or credentials.
      artifacts: [task/ledgers/staging/u7-write-df-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Nine mutations, each red on its named test (section Mutation).
      artifacts: [crates/repark-iceberg/src/tests/output_spec.rs, crates/repark-spark/src/normalize/clustered_by.rs]
    - id: AT-7
      status: N/A
      justification: No performance claim; the staging view adds one metadata clone per
        option-carrying write.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml, lockfile, fork or workflow change; file-size, lib-rs and lib-py
        ceilings held (writer_readwriter.py ratchets 1077 to 1073).
      artifacts: [scripts/check_lib_py.py]
    - id: AT-9
      status: ATTACKED
      evidence: Registry rows IO-BUCKET-1, ICE-WRITE-DF-SAVE-1 and ICE-WRITE-OPTIONS-1 updated;
        eleven map.md files in lockstep.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The writer-touching facade sweep (2883 passed) and the touched-crate Rust suites
        stay green; the only pre-existing pin changed is the retired R-1 refusal.
      artifacts: [python/repark/tests/test_io_bucket_cluster_1.py]
  complete: true
```
