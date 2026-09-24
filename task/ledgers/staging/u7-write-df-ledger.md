# Charter ledger — U7-WRITE-DF · WO U7 PR1: DataFrame writer `save(name)`, `bucketBy`, `output-spec-id`

**Date:** 2026-09-24 · **Branch:** `feat/u7-write-df` · **Base:** `6cf215bd` (`origin/main`) · **Model:** Claude Opus (`claude-opus-5-5`), per each commit's `Authored-By` trailer · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** `docs/spark-sql-iceberg-parity.md` — row `IO-BUCKET-1` narrowed (the R-1 saveAsTable refusal retired, one residual kept), row `ICE-WRITE-DF-SAVE-1` added (table names FIXED, path create modes DECLARED), row `ICE-WRITE-OPTIONS-1` gains a U7 PR1 bullet (`output-spec-id` honoured); round 2 (critic r1 V-001..V-012) applies Ruling Q1 and leaves IO-BUCKET-1 one residual (the `SORTED BY … DESC` parse text). Row `EX-IO-5` stays true unchanged: a default-format `save()` keeps its refusal.

**Retires:** in flight.

**Scope:** four scoreboard cells (`/tmp/oc-worker/scoreboard/2026-09-24`, Spark answers in
`out/spark-core.json`, bodies in `cells_write.py`), all four EQUAL after this unit:
`W-DF-SAVE-NAME`, `W-DF-SAVE-OVERWRITE-NAME`, `W-DF-V1-BUCKETBY-ERR`, `W-DF-OPT-OUTPUT-SPEC-ID`.
The decisions are Rust kernels:
- `crates/repark-iceberg/src/write/output_spec.rs`: parse, validate and apply the staging view for `output-spec-id`, and say whether the staged spec is partitioned.
- `crates/repark-iceberg/src/write/writer_plan.rs`: the statement (`ctas`, `rtas`, `append`, `overwrite`, `skip`) for `saveAsTable` and `save()`, the missing bucket column and the already-exists refusal.
- `crates/repark-iceberg/src/write/writer_partitioning.rs`: the layout comparison, the path relation and the `save()` target decision.
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
answered as Spark except the path create modes (declared), the missing-bucket-column class
(Q1) and the `saveAsTable` error-mode text; round 2 closed the last two (M-4).

The replay of the four cells is EQUAL on every observation (`target/u7-pr1-replay.json`).

**M-4 — round 2** (critic r1 V-001..V-012; `target/probe-u7-r1fix/sprobe.py`, `sprobe2.py`
on the same Spark under `jvm-lock.sh`, `rprobe.py` on the rebuilt wheel). The 41 Spark shapes
are committed in the oracle; 40 answer equal on RePark, and `clustered_sorted_desc` is the
residual below. Spark's answers:
- A missing bucket column raises `_LEGACY_ERROR_TEMP_3060` `Couldn't find column nope
  in:\nroot\n |-- id: long (nullable = true)\n …` on every create-or-replace arm, including an
  existing table in error or ignore mode; append onto an existing table raises the layout
  mismatch `provided: bucket(4, nope)`. `INVALID_BUCKET_COUNT` comes first.
- `saveAsTable` error mode on an existing table raises `Cannot create table or view
  `u7`.`t` because it already exists.…`.
- `output-spec-id`: `x` and ` 1` raise `NumberFormatException`; `+0` parses. An unknown id
  refuses on an empty frame too. Bucketed RTAS and `createOrReplace()` with id 0 write one
  spec-0 file into the partitioned replacement; `replace()` with id 1 writes two spec-1 files.
  `overwritePartitions()` with id 0 partitioned by `cat` under an unpartitioned current spec
  keeps row 2 (`specs [[0, 3]]`).
- `save()` paths split at the last `/`: `` `file:///…/a`.b ``, `` `/…/a/b`.`` ``,
  `` `/tmp//x/`.y ``, ``` ``.x ```, `a.b`.
- A non-bucketed `saveAsTable` `partitionBy('cat')` append onto an unpartitioned table
  refuses `requirement failed: … - provided: identity(cat)\n - table: `.
- `CLUSTERED BY (clustered)` on a column named `clustered` creates `bucket(4, clustered)`;
  `SORTED BY (x ASC)` refuses `sorted_bucket(id, 4, x)`; `SORTED BY (x DESC)` raises
  `ParseException` `_LEGACY_ERROR_TEMP_0035`.
- The identity partition columns of `ctas_part_upper` and `create_part_upper` sit under
  DESCRIBE's `# Partition Information` (`partition_info ["cat"]`), not in `Part N` rows.

## Clauses

| Clause | Proposition | Evidence | Verdict | Notes |
|---|---|---|---|---|
| C-001 | `format("iceberg").mode("append").save("sc.u7.t")` appends by name exactly as cell `W-DF-SAVE-NAME` records. | `test_save_name_appends_like_the_recorded_cell`; replay. | **PROVEN** | Replay: data, md.spec, md.refs, md.snapshots, layout.snapshots all EQUAL. M8 red. |
| C-002 | `mode("overwrite").save(name)` is a whole-table overwrite with `deleted-records=3` (cell `W-DF-SAVE-OVERWRITE-NAME`). | `test_save_name_overwrites_like_the_recorded_cell`; replay. | **PROVEN** | Static by-name `INSERT OVERWRITE`, as Spark's `OverwriteByExpression(true)`. |
| C-003 | On a missing table, append and overwrite refuse `TABLE_OR_VIEW_NOT_FOUND` with Spark's relation text; error and ignore create the table (with `partitionBy`). | `test_save_name_on_a_missing_table_refuses_in_write_modes`, `test_save_name_creates_a_missing_table_in_create_modes`. | **PROVEN** | Kernel `decide_save_target`; text from `repark_common::spark_error`. |
| C-004 | On an existing table, the error modes of `save(name)` and `saveAsTable` refuse `TABLE_OR_VIEW_ALREADY_EXISTS` `` `u7`.`t` ``, ignore is a no-op, and `option("path", name).save()` routes the same way. | `test_save_name_on_an_existing_table_refuses_in_error_mode`, `test_save_reads_the_name_from_the_path_option`, `test_error_mode_and_bucketed_save_answer_spark_text`. | **PROVEN** | Messages equal Spark's byte for byte (round 2 added `saveAsTable`). |
| C-005 | On an existing table, a `save(name)` append or overwrite and a `saveAsTable` append (bucketed or not) check the `partitionBy`/`bucketBy` layout against the table's partitioning as Spark's transform list, else `IllegalArgumentException` `requirement failed: …`; no layout means no check. | `test_save_name_refuses_a_partitioning_that_differs_from_the_table`, `test_save_name_with_a_matching_partitioning_appends`, `test_save_as_table_append_checks_a_partition_by_layout`, `test_save_as_table_append_refuses_a_different_partition_by`; Rust `tests/writer_partitioning.rs`, `tests/writer_plan.rs`. | **PROVEN** | M2 red. A `saveAsTable` overwrite without `bucketBy` is the listed RTAS residue. |
| C-006 | A path target: append and overwrite refuse `TABLE_OR_VIEW_NOT_FOUND` naming the path split at its last `/` (every other character kept, an empty name rendered ``` `` ```); create modes and a default-format `save()` keep the declared refusal. | `test_save_to_a_real_path_refuses_with_spark_path_relation`, `test_save_path_relations_split_at_the_last_slash`, `test_save_to_a_real_path_in_create_mode_is_a_declared_refusal`, `test_examples_io_session.py::test_save_default_format_refuses` (unchanged, green); Rust `path_relations_split_at_the_last_slash_like_iceberg_path_identifier`. | **PROVEN** | Registry `ICE-WRITE-DF-SAVE-1` (DECLARED part). |
| C-007 | `bucketBy(4, "id").saveAsTable` on a new table creates spec `id_bucket bucket[4] id` and writes one append; with `partitionBy` the bucket is last; `ID` resolves; overwrite is RTAS (`overwrite` snapshot). | `test_bucket_by_creates_a_bucket_partitioned_table`, `test_bucket_by_new_table_shapes`; replay. | **PROVEN** | M9 red. |
| C-008 | `sortBy` or a multi-column bucket refuses with Spark's `Cannot convert transform…` text and creates or replaces nothing. | `test_bucket_by_shapes_iceberg_cannot_convert_refuse`, `test_bucketed_overwrite_of_two_columns_leaves_the_table`, `test_io_bucket_cluster_1.py::test_bucket_by_sort_by_save_as_table_refuses_like_iceberg`. | **PROVEN** | M5 red. R-1 pin retired. |
| C-009 | Bucketed saveAsTable on an existing table: append checks the layout, overwrite replaces (RTAS, next spec id), error refuses with Spark's text and ignore skips; a bucketed `save()` refuses `_LEGACY_ERROR_TEMP_1312`. | `test_bucket_by_existing_unbucketed_table`, `test_bucket_by_existing_bucketed_table`, `test_bucket_by_existing_partitioned_table`, `test_bucket_by_existing_table_overwrite_with_partition_by`, `test_error_mode_and_bucketed_save_answer_spark_text`, `test_layout_mismatch_renders_every_table_transform`. | **PROVEN** | M3 red on the save-target arm. |
| C-010 | `output-spec-id=<old id>` stages data files under that spec on every option-carrying writer; `.files` reports them there (cell `W-DF-OPT-OUTPUT-SPEC-ID`). | `test_output_spec_id_writes_under_the_old_spec`, `test_output_spec_id_reaches_every_writer`, `test_output_spec_id_writes_partitioned_files_under_an_old_partitioned_spec`; Rust `tests/output_spec.rs`. | **PROVEN** | M1 and M7 red. |
| C-011 | An unknown or negative id refuses `IllegalArgumentException` `Output spec id <n> is not a valid spec id for table`, even when the frame is empty; a non-integer or padded value refuses `NumberFormatException` `For input string: "<v>"`; `+0` parses; nothing is committed on a refusal and the current id writes normally. | `test_output_spec_id_refusals`, `test_output_spec_id_parses_like_java_integer`, `test_an_unknown_output_spec_id_refuses_a_write_that_stages_nothing`, `test_output_spec_id_current_and_absent_land_under_the_current_spec`. | **PROVEN** | `NumberFormatException` is a native leaf under `IllegalArgumentException`, as in PySpark. |
| C-012 | Near misses keep their paths: a parquet or csv `save(path)`, a `saveAsTable` without `bucketBy`, and a write without the option (current spec). | `test_near_misses_keep_their_paths`, `test_output_spec_id_current_and_absent_land_under_the_current_spec`; the 2883-test writer sweep. | **PROVEN** | The only pre-existing pin changed is R-1 (C-008). |
| C-013 | SQL twins: `CLUSTERED BY` joins `PARTITIONED BY` last, multi-column and sorted runs refuse with Spark's text, and partition columns fold case only under a case-insensitive session. | `test_sql_door_bucket_clauses`, `test_sql_door_bucket_refusals`, `test_sql_door_create_resolves_partition_columns_case_free`; `clustered_by` and `normalize::tests` units. | **PROVEN** | M4, M5 and M6 red. |
| C-014 | The Rust kernels carry in-crate pins: staging view, parse and validate refusals, layout rendering and comparison, save-target matrix, path relation, staged-spec dispatch. | `crates/repark-iceberg/src/tests/output_spec.rs` (7), `tests/writer_partitioning.rs` (9). | **PROVEN** | `cargo test -p repark-iceberg --lib -- tests::output_spec tests::writer_partitioning tests::writer_plan`: 22 passed. |
| C-015 | A bucket column absent from the frame raises `_LEGACY_ERROR_TEMP_3060` `Couldn't find column <c> in:\n<printSchema tree>` on every create-or-replace arm of `saveAsTable` (after `INVALID_BUCKET_COUNT`, before the existence check); an append onto an existing table answers the layout mismatch. | `test_a_missing_bucket_column_on_a_new_table_is_legacy_3060`, `test_a_missing_bucket_column_on_an_existing_table`, `test_bucket_count_precedes_the_missing_column`, `test_bucket_by_existing_partitioned_table`, `test_io_bucket_cluster_1.py::test_bucket_by_missing_column_refused`; Rust `tests/writer_plan.rs`. | **PROVEN** | Ruling Q1. The tree is `repark_spark::spark_tree_string` over the frame's analyzed Arrow schema. |
| C-016 | With `output-spec-id` the writer follows the staged spec: a replacing write resolves the id in the replacement metadata (bucketed RTAS, `createOrReplace()`, `replace()`), a CTAS in its own, and a dynamic overwrite replaces the partitions of the staged spec (everything when that spec is unpartitioned). | `test_output_spec_id_on_a_replacing_write_resolves_in_the_replacement`, `test_output_spec_id_on_a_create_and_an_unknown_replacement_spec`, `test_dynamic_overwrite_replaces_partitions_of_the_staged_spec`, `test_dynamic_overwrite_of_an_unpartitioned_staged_spec_replaces_everything`; Rust `a_partitioned_writer_follows_an_unpartitioned_output_spec`, `the_staged_spec_decides_whether_a_dynamic_overwrite_replaces_partitions`. | **PROVEN** | V-002 (row 2 kept) and V-003 (no `DataInvalid` text). |
| C-017 | The `CLUSTERED BY` rewrite tries every unquoted `CLUSTERED` before the `AS` boundary, so a column named `clustered` keeps the clause; `SORTED BY (c ASC)` refuses like an unqualified sort. | `test_a_column_named_clustered_keeps_the_bucket_clause`, `test_sorted_by_ordering_in_a_clustered_clause`; Rust `a_column_named_clustered_does_not_hide_the_bucket_clause`, `an_ascending_sort_refuses_like_an_unqualified_one`. | **PROVEN** | `SORTED BY (c DESC)` is the listed parse-text residue. |
| C-018 | The statement decision is one Rust kernel for `saveAsTable` and `save()`; the facade composes the SQL for the kernel's answer and decides no statement from the mode, the bucket state or the target spelling. | `crates/repark-iceberg/src/tests/writer_plan.rs` (6); `writer_save.py` calls `writer_plan` / `writer_target_is_table` only. | **PROVEN** | V-007. |

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

Round 2 (`target/probe-u7-r1fix/mutation.txt`), one per [S1] fix:
- **M10 (V-001):** `plan_writer`'s missing-bucket-column check disabled. `a_missing_bucket_column_refuses_on_every_create_or_replace_arm` goes red (a missing table planned `ctas`), and so does `a_case_sensitive_session_keeps_the_bucket_column_case`.
- **M11 (V-003):** `stage_partitioned_stream_with_overrides` no longer dispatches on the staged spec. `a_partitioned_writer_follows_an_unpartitioned_output_spec` goes red: `DataInvalid => Cannot create partition calculator for unpartitioned table`.
- **M12 (V-004):** only the first `CLUSTERED` word is tried. `a_column_named_clustered_does_not_hide_the_bucket_clause` goes red, the statement left unrewritten.
- **M13 (V-005):** the path is collapsed and trimmed before the split. `path_relations_split_at_the_last_slash_like_iceberg_path_identifier` goes red: `` `file://tmp/probe/a`.b ``.
- **M14 (V-006):** the parse refusal is an `IllegalArgumentMarker` again. `output_spec_id_parses_like_java_integer_parse_int` goes red: `expected a NumberFormatMarker, got IllegalArgumentMarker`.
- **M15 (V-002):** the dynamic scope is keyed on the table's current spec again (rebuilt wheel). `test_dynamic_overwrite_replaces_partitions_of_the_staged_spec` goes red: rows `[[7,g,x],[8,h,w]]` against `[[2,b,y],[7,g,x],[8,h,w]]`.

## Out of scope (observed, not worked)

- A non-bucketed `saveAsTable` overwrite on a missing table records `append` (CTAS), where
  Spark records `overwrite` (RTAS). The same holds for a non-bucketed overwrite of an
  existing table: Spark replaces the table, RePark runs `INSERT OVERWRITE`. Both were left
  unchanged as brief near misses. Measured in round 2 (`rtas_sql_spec0_on_partitioned`): on a
  table partitioned by `cat`, Spark's replacement is unpartitioned (`# Partition Information`
  empty) while RePark keeps `cat`; rows, operations and `.files` specs agree.
- `CLUSTERED BY (c) SORTED BY (x DESC) INTO n BUCKETS` raises a `ParseException` with the
  engine's parser text (`SQL error: ParserError("Expected: end of statement, found: USING …")`);
  Spark raises `ParseException` `_LEGACY_ERROR_TEMP_0035` with the message `\nOperation not
  allowed: Column ordering must be ASC, was 'DESC'.\n== SQL (line 1, position 58) ==\n…`
  (its SQL context block and caret line). No Spark-shaped parse context renderer exists.
- With `spark_catalog` current, Spark sends a one- or two-part `save()` name to its Hive
  session catalog (a metastore error in the probe session); RePark resolves it against its own
  current catalog, like `saveAsTable`. Unmeasured beyond that probe.
- `save('s3://b/k/t')` names `` `s3://b/k`.t `` by the path rule; the probe session had no S3
  filesystem, so Spark raised `RuntimeIOException: Failed to get file system for path:
  s3://b/k/t/metadata/version-hint.text`. Not measured against a live S3 filesystem.
- `PARTITIONED BY (bucket(4, id, data))` keeps V3-MULTIARG-1's declared RePark text; Spark
  says `Cannot convert transform with more than one column reference: bucket(4, id, data)`.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u7-write-df
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The four recorded cells are pinned verbatim and replay EQUAL on every observation;
        98 of the 100 measured oracle shapes are cited by a test and compared by class, text
        and state (the two `SORTED BY … DESC` shapes by class only); the two uncited ones are
        the Out-of-scope evidence `rtas_sql_spec0_on_partitioned` (non-bucketed RTAS) and
        `save_s3` (no S3 filesystem in the probe).
      artifacts: [python/repark/tests/test_ice_write_df_1.py, python/repark/tests/test_ice_write_df_1_edges.py, python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: The oracle is live Spark 4.1.2 + Iceberg 1.11.0 (scoreboard record plus step-1
        probes under jvm-lock.sh); no expected value is derived from RePark output.
      artifacts: [python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal (not found, already exists, layout mismatch, cannot convert, spec id,
        number format, missing column) is pinned with its class and full text through
        `_assert_error`, and no table or commit appears after it; the `SORTED BY … DESC` parse
        answer is pinned by class only and listed as a residue.
      artifacts: [python/repark/tests/test_ice_write_df_1.py, python/repark/tests/test_ice_write_df_1_edges.py, crates/repark-iceberg/src/tests/writer_partitioning.rs]
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
      evidence: Nine step-6 mutations and six round-2 mutations, each red on its named test
        (section Mutation).
      artifacts: [crates/repark-iceberg/src/tests/output_spec.rs, crates/repark-spark/src/normalize/clustered_by.rs]
    - id: AT-7
      status: N/A
      justification: No performance claim; the staging view adds one metadata clone per
        option-carrying write.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml, lockfile, fork or workflow change; file-size, lib-rs and lib-py
        ceilings held (writer_readwriter.py ratchets 1077 to 1073, then down again in round 2).
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
