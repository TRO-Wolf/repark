# Unit ledger — U8 WRITE-SQL PR1 · REPLACE WHERE, the INSERT PARTITION clause, bucketed INSERT

**Date:** 2026-09-24 · **Branch:** `feat/u8-write-sql` · **Base:** `fb41309f`
(`origin/main`) **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Three scoreboard cells of the 2026-09-24 run answered wrong on the Spark door:
`W-INSERT-OVERWRITE-WHERE` did not parse, `W-INSERT-PARTITION-CLAUSE` answered DataFusion's
`Partitioned inserts not yet supported`, and `W-INSERT-BUCKETED` leaked DataFusion's
`Projections require unique expression names`. The third is not a bucket problem: a cast keeps
its input's name in DataFusion, so `SELECT id, CAST(id AS STRING)` repeats `id` in any source.

**What Spark does (measured 2026-09-24, `target/probe-u8-pr1/spark.json`, `spark2.json`,
`spark3.json`, `spark4.json`, 128 probes).** `INSERT INTO t REPLACE WHERE p q` is
`OverwriteByExpression`, which Iceberg runs as `OverwriteFiles.overwriteByRowFilter(p)` with
the query's files added and no check that they match `p`. A file whose rows match `p` only in
part refuses `ValidationException: Cannot delete file where some, but not all, rows match
filter …`. An empty query commits `delete`, even when nothing matches; a literal `false`
predicate commits `append`. The predicate converts by Spark's V2-filter rules or refuses
`IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: …`.
`REPLACE WHERE` is only legal right after `INSERT INTO [TABLE] <name>`. The `PARTITION`
clause on `INSERT INTO` fills each static key with its value cast from its string form and
takes a dynamic key from the query; only identity partition columns are accepted. A bucketed
`INSERT … FROM range(20)` writes 20 rows in four files, one per bucket.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `INSERT INTO t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'` on a `cat`-partitioned table leaves `[[2,b,y],[9,z,x]]` and commits `overwrite` with added-records 1, deleted-records 2, total-records 2, added-data-files 1, deleted-data-files 1, changed-partition-count 1. | Rows, summary and file layout on the facade and the Rust door; the scoreboard cell. | PROVEN | `test_replace_where_replaces_the_matching_partition`; Rust `replace_where_on_a_partition_replaces_its_rows_with_the_query`. Cell `W-INSERT-OVERWRITE-WHERE` replays EQUAL on the whole observation. |
| C-002 | The spellings Spark accepts write the same rows: `INSERT INTO TABLE`, lower case, a comment inside `REPLACE WHERE`, `VALUES` (one or two rows), a parenthesized query, a CTE query, a cast column, `!=`, `NOT`, `NOT IN`, `LIKE 'x%'`, `<=>`, a literal on the left, a query that reads the target, and query rows outside the predicate (written unchecked). `IS NOT NULL`, `BETWEEN`, `>=` and `IN` over every row replace the whole table. | One row pin per spelling. | PROVEN | `test_replace_where_accepts_the_spellings_spark_accepts`, `test_replace_where_reads_its_source_and_writes_it_unchecked`; Rust `replace_where_accepts_the_spellings_spark_accepts`, `replace_where_reads_the_target_before_it_replaces_it`, `replace_where_writes_rows_outside_the_predicate_unchecked`. Probes RW-*, RW2-*, RW4-values-multi. |
| C-003 | The snapshot follows what the commit changes: no match → `overwrite` add-only; empty query → `delete` (also when nothing matches, `changed-partition-count` 0, and on a table with no snapshot); `true` → `overwrite` of every row; `false` → `append`; an unpartitioned `id >= 1` → `overwrite` of the one file. | Operation and counts per case. | PROVEN | `test_replace_where_snapshot_follows_what_the_commit_changes`; Rust `replace_where_snapshot_operations_follow_what_the_commit_changes`, `replace_where_on_an_unpartitioned_table_replaces_whole_files_only`; iceberg `an_empty_source_commits_a_delete_even_when_nothing_matches`, `an_empty_source_on_a_table_without_snapshots_commits_a_delete`. |
| C-004 | Refusals commit nothing and carry Spark's condition, SQLSTATE and text: `UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY` / `0A000`; `INVALID_NON_DETERMINISTIC_EXPRESSIONS` / `42K0E` (`"(rand() < 2)"`); `IllegalArgumentException` `Cannot convert Spark predicate to Iceberg expression: UPPER(cat) = 'X'` and `…: null` for `cat = NULL`; `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` / `21S01`; `TABLE_OR_VIEW_NOT_FOUND` / `42P01`; `ParseException` `PARSE_SYNTAX_ERROR` / `42601` near `'REPLACE'` after `OVERWRITE`, a column list, `BY NAME` or `PARTITION`, and near `'BY'` after the predicate; and the partial-file message `Cannot delete file where some, but not all, rows match filter …` (class and filter spelling are R-1). | Exact class + condition + SQLSTATE + message pins, table unchanged. | PROVEN | `test_replace_where_refusals_match_spark_and_commit_nothing`, `test_replace_where_that_splits_a_file_refuses_like_iceberg`; Rust `replace_where_refusals_carry_spark_text_and_commit_nothing`, `misplaced_replace_where_is_a_spark_parse_error`; iceberg `untranslatable_predicates_refuse_with_the_spark_message`, `a_reference_in_another_case_fails_the_iceberg_field_lookup`, `a_filter_that_splits_a_file_refuses_before_any_commit`. |
| C-005 | `INSERT INTO t.branch_b1 REPLACE WHERE …` commits on the branch and leaves `main` unchanged. | Rows of `main` and of the branch. | PROVEN | `test_replace_where_on_a_branch_leaves_main_alone`; Rust `replace_where_writes_to_a_named_branch_only`. Probe RW-branch. |
| C-006 | `INSERT INTO t PARTITION (cat = 'q') SELECT 9, 'z'` appends `[9, z, q]` with one `append` snapshot (added-records 1, added-data-files 1, changed-partition-count 1), and so do `VALUES`, `TABLE`, an upper-case key, the dynamic `PARTITION (cat)` forms, both column-list orders and `BY NAME`. A two-key spec takes static and dynamic keys in any order. | Rows and summary per spelling. | PROVEN | `test_a_static_partition_value_fills_the_missing_column`, `test_two_key_specs_take_static_and_dynamic_values_in_any_order`; Rust `a_static_partition_value_fills_the_missing_column`, `two_key_specs_take_static_and_dynamic_values_in_any_order`. Cell `W-INSERT-PARTITION-CLAUSE` replays EQUAL. |
| C-007 | A static value is its Spark string form cast to the column type: `id = '7'` and `id = 8` → BIGINT, `cat = 5` → `'5'`, `cat = true` → `'true'`, `cat = NULL` → NULL, `d = '2024-01-05'` → DATE. | Rows per value. | PROVEN | `test_static_values_follow_spark_string_casts`; Rust `static_values_follow_spark_string_casts`. Probes PC-str-into-bigint, PC-int-into-*, PC-null, PC2-bool-into-string, PC2-date-part, PC4-date. |
| C-008 | Clause refusals commit nothing: `NON_PARTITION_COLUMN` / `42000` for a non-partition column, an unknown name, an unpartitioned table and a `bucket` source; `ParseException` `DUPLICATE_KEY` / `23505` on `INSERT INTO` and `INSERT OVERWRITE`; `CAST_INVALID_INPUT` / `22018` (`'abc'`, `'7.5'` into BIGINT, on append and overwrite; class R-3); `INSERT_COLUMN_ARITY_MISMATCH` naming the static column at its table position (`9`, `cat` and `9`, `z`, `cat`, `q`) and, for a dynamic-only clause, the query's columns; `STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST` / `42713`. | Exact class + condition + SQLSTATE + message pins. | PROVEN | `test_partition_clause_refusals_match_spark_and_commit_nothing`; Rust `partition_clause_refusals_carry_spark_text_and_commit_nothing`, `a_bad_static_value_refuses_as_an_illegal_argument`, `a_dynamic_only_clause_keeps_the_spark_arity_refusal`. Probes PC-*, PC2-dyn-short, PC4-static-in-list. |
| C-009 | A `PARTITION` insert into `t.branch_b1` commits on the branch only; `INSERT OVERWRITE` and `INSERT INTO` with an invalid DATE value refuse `CAST_INVALID_INPUT` (Spark's text, byte-equal); a static `INSERT OVERWRITE … PARTITION (cat = 'x')` still replaces that partition. Supersedes the refusal text of ice-overwrite-mode-1/C-013 (noted in that ledger). | Branch rows; exact refusal; overwrite rows. | PROVEN | `test_partition_inserts_reach_branches_and_validate_overwrite_values`, `test_invalid_static_date_refuses_with_spark_cast_invalid_input`; Rust `a_partition_insert_writes_to_a_named_branch_only`, `static_value_is_cast_to_a_date_partition_and_an_invalid_value_refuses` (re-pinned). Probes PC2-branch, PC3-*. |
| C-010 | On a `write.spark.accept-any-schema` table a `PARTITION` append resolves by name through U6's door: `SELECT 9, 'z'` refuses `Field 9 not found in source schema`, `VALUES (9, 'z')` refuses `Field col1 …`, an extra alias refuses `Field extra …`, and `SELECT 9 AS id, 'z' AS data` writes `[9, z, x]`. A positional `VALUES` keeps U6's `Field col1` refusal. This retires part (b) of u6-write-refusals residue R-3 for appends. | Class + message pins; rows. | PROVEN | `test_partition_inserts_on_accept_any_tables_resolve_by_name`; Rust `a_partition_insert_on_an_accept_any_table_resolves_by_name`. Probes PC2-accept-any, PC3-accept-any-alias; U6 probes e1–e10. |
| C-011 | A positional INSERT source whose later item repeats an earlier output name (a cast keeps its input's name) plans and writes: `INSERT INTO t SELECT id, CAST(id AS STRING), 'k' FROM range(20)` into `bucket(4, id)` appends 20 rows in four files (`append`, added-data-files 4, changed-partition-count 4) with Spark's per-bucket record counts; aliased sources, `INSERT OVERWRITE` (static and dynamic), identity, truncate and unpartitioned tables answer as Spark does. `BY NAME` keeps its names. | Rows, summaries and file layout. | PROVEN | `test_a_bucketed_insert_from_range_matches_spark_layout`, `test_bucketed_overwrites_and_other_layouts_from_range`; Rust `a_cast_that_keeps_its_input_name_no_longer_collides_in_a_positional_source`, `a_bucketed_insert_writes_one_file_per_bucket`, `the_source_rename_leaves_by_name_inserts_alone`. Cell `W-INSERT-BUCKETED` replays EQUAL. |
| C-012 | Near misses keep their answers: a `WHERE` inside the query (`… FROM t AS replace WHERE cat = 'y'`) is a plain append; plain `INSERT … VALUES` and `INSERT OVERWRITE … VALUES` are unchanged. | Rows and operations. | PROVEN | `test_near_misses_keep_their_answers`; Rust `a_where_inside_the_query_is_not_replace_where`. |
| C-013 | The overwrite-by-filter kernel lives in `repark-iceberg`: `spark_overwrite_filter` (sqlparser predicate → Iceberg filter by Spark's optimizer and V2 rules; the null semantics are C-015) and `commit_overwrite_by_filter_with_summary` (row filter, no added-file validation, isolation and branch as the sibling commits; an empty source still commits `delete`). | Kernel unit pins. | PROVEN | iceberg `spark_translatable_predicates_convert_to_iceberg_filters`, `not_pushes_down_through_comparisons_and_connectives_as_spark_does`, `in_lists_follow_spark_null_semantics`, `a_whole_partition_filter_replaces_its_rows_as_an_overwrite`, `added_rows_outside_the_filter_are_not_validated`. |
| C-014 | The facade's bare-name expansion leaves a `REPLACE WHERE` predicate alone and expands only the query, so a CTE in the query keeps its scope. | The CTE spelling through `spark.sql`. | PROVEN | `test_replace_where_accepts_the_spellings_spark_accepts` (CTE case); before the fix it answered `table 'hc.default.s' not found` under a non-default current catalog. |
| C-015 | *Added round 2 (2026-09-25, critic r1 V-001, V-002).* A NULL partition key is kept or replaced as Spark keeps or replaces it. `<` and `<=` convert to `notNull AND lt` / `notNull AND ltEq` (Java Iceberg's `lt` never matches a NULL; the fork's evaluator orders NULL first), `>` and `>=` stay bare. `NOT` pushes down as Spark's optimizer pushes it: a comparison flips (`NOT cat < 'y'` → `cat >= 'y'`), `AND`/`OR` swap, `IS NULL` ↔ `IS NOT NULL`, `NOT BETWEEN` → `< low OR > high`. An `IN` list is deduplicated with its NULL counted, and a one-element list folds to `=` / `<>` (Spark's OptimizeIn), so `NOT IN ('y')` and `NOT IN ('y', 'y')` delete the NULL row. Longer `NOT IN` lists keep `notNull AND notIn` over the non-NULL values and keep the NULL row. Measured over 49 predicates on a NULL `cat` key and 7 on a NULL `id` key. | Rows per predicate on the facade and the Rust door, generated from Spark's rows; kernel renderings. | PROVEN | `test_a_null_partition_key_is_kept_or_replaced_as_spark_does`, `test_a_null_long_key_is_kept_under_a_range_predicate`; Rust `a_null_partition_key_is_kept_or_replaced_as_spark_does` (49 cases), `a_null_identity_key_on_a_long_partition_follows_spark` (7); iceberg `spark_translatable_predicates_convert_to_iceberg_filters`, `not_pushes_down_through_comparisons_and_connectives_as_spark_does`, `in_lists_follow_spark_null_semantics` (re-pinned: the one-element `NOT IN` now folds; the `notNull AND notIn` shape is pinned on `NOT IN ('y', 'q')`). Probes `target/probe-u8-r1fix/spark_r1.json` S1-*, S3-*; critic XN-le, XN-id-lt-nullid, XR-lt-null, XR-not-between, XR-notin-null, XN-notin2, XN-not-lt. |
| C-016 | *Added round 2 (critic r1 V-003).* A `REPLACE WHERE` source whose output names repeat writes as Spark does: the width check plans the deduplicated source, so `SELECT id, CAST(id AS STRING), 'x' FROM t WHERE cat = 'x'` leaves `[[1,'1',x],[2,b,y],[3,'3',x]]`, `SELECT 9 AS a, 'z' AS a, 'x'` leaves `[[2,b,y],[9,z,x]]`, and its `UNION ALL` form adds `[10,w,x]`. | Rows on the facade and the Rust door. | PROVEN | `test_a_replace_where_source_with_repeated_names_writes`; Rust `a_replace_where_source_with_repeated_names_writes_as_spark_does`. Probes S5-*; critic XN-rw-cast-dup, XN-rw-union-dup, XR-rw-alias-dup. |
| C-017 | *Added round 2 (critic r1 V-005 part 2).* An integer literal outside an `INT`/`BIGINT` column's range folds as Spark's unwrap-cast folds it: a comparison that no value satisfies refuses `Cannot convert Spark predicate to Iceberg expression: null` (`=`, `>`, `>=` above the range; `<`, `<=` below; `IN` of only such literals), one that every non-NULL value satisfies refuses `…: (i IS NOT NULL) OR (null)` (`<`, `<>` above), and an `IN` list drops the literal (`i IN (1, 3000000000)` replaces `i = 1`). | Exact text and rows. | PROVEN | `test_an_out_of_range_integer_literal_refuses_as_spark_folds_it`; iceberg `an_out_of_range_integer_literal_folds_as_spark_unwraps_the_cast`, `in_lists_follow_spark_null_semantics`. Probes S4-*; critic XR-pk-int-overflow-pred. |
| C-018 | *Added round 2.* A `REPLACE WHERE` into a table whose namespace does not exist answers `TABLE_OR_VIEW_NOT_FOUND` (it answered the D7 load text before). | Rust door pin. | PROVEN | Rust `a_replace_where_into_a_missing_namespace_answers_table_or_view_not_found`. |

VERDICT (2026-09-25, critic r1 remediation): 18 clauses, 18 PROVEN, 0 OPEN, 0 REJECTED.

## Claims line for U7 (overwrite by condition)

`repark_iceberg::write::{spark_overwrite_filter, commit_overwrite_by_filter_with_summary}`
(`crates/repark-iceberg/src/write/overwrite_filter.rs`) are Spark's `OverwriteByFilter`:
the DataFrame `writeTo(t).overwrite(condition)` can render its condition to Spark SQL, parse it
with the Databricks dialect, convert it with `spark_overwrite_filter` and commit its staged files
with `commit_overwrite_by_filter_with_summary`. The Spark door's executor,
`router::insert_positional::replace_where::execute_replace_where`, shows the staging and the `false` → append arm.

## Mutation record (2026-09-24)

Each line was broken, the named tests ran, and the file was restored
(`target/probe-u8-pr1/mut/M*.txt`; round 2 in `target/probe-u8-r1fix/mut/`).

| # | Mutation | Red |
|---|---|---|
| M1 | drop `.allow_empty_commit()` from the overwrite-by-filter commit | none: the fork counts a set row filter as a change, so the call was dead and is removed; `an_empty_source_commits_a_delete_even_when_nothing_matches` and `an_empty_source_on_a_table_without_snapshots_commits_a_delete` pin the behaviour |
| M2 | validate added files against the filter | `added_rows_outside_the_filter_are_not_validated`, `a_filter_that_splits_a_file_refuses_before_any_commit` |
| M3 | `NOT IN` without the `notNull` conjunct | `in_lists_follow_spark_null_semantics` |
| M4 | a comparison with `NULL` is not refused | `untranslatable_predicates_refuse_with_the_spark_message` |
| M5 | the router never sees `REPLACE WHERE` | 9 of 10 `tests::replace_where` pins |
| M6 | a `false` predicate overwrites instead of appending | `replace_where_snapshot_operations_follow_what_the_commit_changes` |
| M7 | the positional-source rename never runs | `a_cast_that_keeps_its_input_name_no_longer_collides_in_a_positional_source`, `a_bucketed_insert_writes_one_file_per_bucket` |
| M8 | every static value goes to the last column | `static_values_follow_spark_string_casts`, `two_key_specs_take_static_and_dynamic_values_in_any_order` |
| M9 | the static-value cast check never refuses | `a_bad_static_value_refuses_as_an_illegal_argument`, `partition_clause_refusals_carry_spark_text_and_commit_nothing`, `static_value_is_cast_to_a_date_partition_and_an_invalid_value_refuses` |
| M10 | `REPLACE WHERE` is not an owned write head | `replace_where_writes_to_a_named_branch_only` |
| M11 | the router ignores `owned_append` | `a_partition_insert_writes_to_a_named_branch_only` |
| M12 | `INSERT INTO … PARTITION` is not an owned write head | `a_partition_insert_writes_to_a_named_branch_only` |
| M13 | U6's by-name routing excludes every `PARTITION` insert again | `a_partition_insert_on_an_accept_any_table_resolves_by_name` |
| M14 | a repeated clause key is not refused | `partition_clause_refusals_carry_spark_text_and_commit_nothing` |
| M15 | the static-clause arity check never refuses | `partition_clause_refusals_carry_spark_text_and_commit_nothing` |
| M16 | the dynamic-only clause skips the arity check | `a_dynamic_only_clause_keeps_the_spark_arity_refusal` |
| M17 | a subquery predicate is not refused | `replace_where_refusals_carry_spark_text_and_commit_nothing` |
| M18 | a non-deterministic predicate is not refused | `replace_where_refusals_carry_spark_text_and_commit_nothing` |
| M19 | the `REPLACE WHERE` arity check always passes | `replace_where_refusals_carry_spark_text_and_commit_nothing` |
| M20 | the facade expands the whole `REPLACE WHERE` body | `test_replace_where_accepts_the_spellings_spark_accepts[… WITH s AS …]` |
| M21 | *round 2 (2026-09-25):* `<` converts to a bare `lt` again | Rust `a_null_partition_key_is_kept_or_replaced_as_spark_does`, `a_null_identity_key_on_a_long_partition_follows_spark` |
| M22 | a one-element `IN` list does not fold | iceberg `in_lists_follow_spark_null_semantics`, `not_pushes_down_through_comparisons_and_connectives_as_spark_does` |
| M23 | `NOT <` does not flip to `>=` | iceberg `not_pushes_down_through_comparisons_and_connectives_as_spark_does` |
| M24 | the out-of-range fold text is never used | iceberg `an_out_of_range_integer_literal_folds_as_spark_unwraps_the_cast`, `untranslatable_predicates_refuse_with_the_spark_message` |
| M25 | the width check plans the raw `REPLACE WHERE` source (V-003) | Rust `a_replace_where_source_with_repeated_names_writes_as_spark_does` |
| M26 | a missing namespace is not a missing table | Rust `a_replace_where_into_a_missing_namespace_answers_table_or_view_not_found` |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u8-write-sql
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause comes from the 128-probe Spark measurement or the scoreboard record. The three cells replay EQUAL on the whole observation. The probe matrix answers 115 of 128 probes as Spark does (rows, error condition, snapshot summaries); the other 13 are R-1 (11 class-only), R-2 (RW2-ts) and R-4 (RW-table). Before this unit 11 of the first 122 probes matched (`repark.json`, `repark2.json`, pre-change wheel).
      artifacts: [python/repark/tests/test_ice_write_sql_1.py, crates/repark-spark/src/tests/replace_where.rs, crates/repark-spark/src/tests/partition_append.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Predicates cover every operator Spark converts plus the untranslatable, null, subquery, non-deterministic and mis-cased forms; sources cover SELECT, VALUES, CTE, parenthesized and self-reading queries; targets cover partitioned, unpartitioned, bucketed, unseeded, branch and accept-any tables. Misplaced REPLACE WHERE and a WHERE inside the query are pinned.
      artifacts: [crates/repark-iceberg/src/write/overwrite_filter/tests.rs, crates/repark-spark/src/tests/replace_where.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal asserts the snapshot count is unchanged. The partial-file refusal comes from the fork before any commit. The static-value cast is checked before any source runs.
      artifacts: [crates/repark-spark/src/tests/partition_append.rs, python/repark/tests/test_ice_write_sql_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The overwrite commit takes the table's isolation level and validate_from_snapshot like its sibling commits; REPLACE WHERE and PARTITION inserts are owned write heads, so the branch sniff rewrites them to the branch selector instead of a temp view.
      artifacts: [crates/repark-iceberg/src/write/overwrite_filter.rs, crates/repark-spark/src/write_to_branch.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling. Rewritten SQL re-parses through the door's own parser; static values render through sqlparser's literal display.
    - id: AT-6
      status: ATTACKED
      evidence: Rows, snapshot summaries and file layouts are pinned; the INSERT-adjacent Rust modules (976 tests) and facade suites stay green apart from the re-pinned C-013 text of ice-overwrite-mode-1.
      artifacts: [crates/repark-spark/src/tests/overwrite_mode.rs, python/repark/tests/test_ice_overwrite_mode_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: The two new recognizers return on a substring miss before tokenizing, so statements without `replace` or `partition` pay one lower-case scan. A PARTITION append adds one table load and one source plan.
      artifacts: [crates/repark-spark/src/router/insert_positional/replace_where.rs, crates/repark-spark/src/router/insert_positional/partition_append.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The converter and the commit are one kernel in repark-iceberg; the door reuses the owned append, the stage-then-commit helpers and U6's naming. No new dependency or crate edge, no code comments, router.rs at 997 lines, session_core.py on its exact baseline.
      artifacts: [crates/repark-iceberg/src/write/overwrite_filter.rs, crates/repark-spark/src/router.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The registry gains DML-7 and DML-8, DML-1's cast sentence and ID-3's positional exemption; the superseded ice-overwrite-mode-1/C-013 text is marked in its ledger.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/ice-overwrite-mode-1-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Each load-bearing line was broken and its named test ran red (table above).
      artifacts: [crates/repark-spark/src/tests/replace_where.rs, crates/repark-iceberg/src/write/overwrite_filter/tests.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Iceberg validation refusals keep RePark's class: the partial-file refusal is `PySparkException` `DataInvalid => Cannot delete file where some, but not all, rows match filter id = 2: …`, where Spark raises `Py4JJavaError` wrapping `ValidationException` and spells the filter `ref(name="id") == 2`. A mis-cased predicate column refuses Spark's `Cannot find field 'CAT' in struct: …` text as `AnalysisException`. The same class residue as u6-write-refusals R-7. Round 2 (2026-09-25): a `<` / `<=` filter now renders its `notNull` conjunct (`(data IS NOT NULL) AND (data < "b")`, Spark `ref(name="data") < "b"`); the rows match (sweep S2-*, eight probes). |
| R-2 | The untranslatable-predicate refusal quotes the statement's spelling: `(id + 1) = 3` reads `id + 1 = 3`, `CAST(id AS string)` reads `CAST(id AS STRING)`. A `TIMESTAMP` literal in the predicate refuses as untranslatable (probe RW2-ts); Spark converts it in the session zone. A `DATE` literal or date string converts. |
| R-3 | `CAST_INVALID_INPUT` raises `IllegalArgumentException`; Spark raises its subclass `NumberFormatException`, which the facade does not define. |
| R-4 | `INSERT INTO t REPLACE WHERE … TABLE a.b.c` fails the door's pre-existing parse of a three-part `TABLE` query (plain `INSERT INTO t TABLE a.b.c` fails the same way). |
| R-5 | Pre-existing and unchanged: a plain positional `INSERT … SELECT` of the wrong width still answers `Column count doesn't match insert query!` (u6-write-refusals R-4). The Spark text is produced only on the REPLACE WHERE and PARTITION paths, which already load the table. |
| R-6 | `REPLACE WHERE` has no native-door spelling; the ANSI door answers its parse error (the DML-6 shape). A native steer is a follow-up. |
| R-7 | `REPLACE WHERE` under `spark.wap.id` is unmeasured. Narrowed 2026-09-25: under `spark.wap.branch` the critic's XR-rw-wap matches Spark (`main` unchanged). |
| R-8 | Dated 2026-09-25 (critic r1 V-004, probe S1-notlike, XR-not-like). `REPLACE WHERE cat NOT LIKE 'y%'` on a table with a NULL `cat` key: Spark 4.1.2 fails `SparkException: [INTERNAL_ERROR] Eagerly executed overwrite failed. You hit a bug in Spark or the Spark plugins you use. … SQLSTATE: XX000` and commits nothing; RePark commits the overwrite as `not(startsWith)`, which also deletes the NULL-key row (rows `[[2,b,y],[9,z,x]]`). Spark's answer is a Spark bug, so it is not emulated. `NOT LIKE` without a wildcard folds to `<>` and matches Spark (S1-notlike-exact). |
| R-9 | Dated 2026-09-25 (critic r1 V-005 part 1, probes S6-*). A `REPLACE WHERE` target that is not an Iceberg table: Spark answers ``[_LEGACY_ERROR_TEMP_1011] Writing into a view is not allowed. View: `tv`.; line 1 pos 0`` for a temporary view and ``[_LEGACY_ERROR_TEMP_1012] Cannot write into v1 table: `spark_catalog`.`default`.`default_pq`.; line 1 pos 0`` for a `USING parquet` table. Through the facade a bare temp-view name is qualified to the current catalog first, so RePark answers `TABLE_OR_VIEW_NOT_FOUND` for ``hc.default.tv`` (a plain `INSERT INTO tv` answers `table 'hc.default.tv' not found`, the same pre-existing gap). A DataFusion session table reached on the Rust door answers RePark's own ``INSERT INTO … REPLACE WHERE requires an Iceberg table, got `<name>` ``. |
