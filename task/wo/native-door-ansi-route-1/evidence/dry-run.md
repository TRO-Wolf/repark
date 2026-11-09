# Dry run: the native door on AnsiDialect

Measured 2026-11-08 by the Claude Haiku 5.5 measurement clerk, as the sizing evidence for NATIVE-DOOR-ANSI-ROUTE-1 under the owner's 2026-11-08 ruling F. No product code changed. The prototype is [prototype.patch](prototype.patch) (three added lines), applied to a scratch clone and never pushed. Run details and the exact commands are in [provenance.md](provenance.md).

Base is `origin/main` at `03f925c5`. The prototype is the same commit plus the patch. Both runs use the same 58 collected test files, the same command and the same venv provisioning.

## Headline

- The prototype compiles. Its native-door sessions build with no duplicate-registration error.
- Each run collects 1816 test items. 1667 pass in both runs. 14 tests change, in 4 files: 6 new refusals and 8 changed answers. The 130 skips and 5 xfails are identical in both runs and sit outside the six buckets.
- Five of the six new refusals have one cause. The ANSI route refuses `CREATE TABLE ... AS SELECT` into an unqualified target (`is not a qualified Iceberg table name`), which the native door answers today. The sixth is a parse refusal of bracketed `[ice].[ns].[t]` names.
- The changed answers are of three kinds. Six subquery statements that base refuses are answered by the prototype. An empty DELETE commits a snapshot where base does not. One registry row disagrees.

## Counts

### Tests (1816 items in each run)

| bucket | tests |
|---|---:|
| PASS->PASS | 1667 |
| PASS->FAIL new refusal | 6 |
| PASS->FAIL different answer | 8 |
| FAIL->PASS | 0 |
| FAIL->FAIL | 0 |
| ERROR (collection or setup) | 0 |
| outside the six buckets: SKIP | 130 |
| outside the six buckets: XFAIL | 5 |

The brief's six buckets have no skip row, so skips and xfails are reported beside them and are not counted as PASS. The base run had 0 failures, so FAIL->PASS and FAIL->FAIL are zero by construction. No collection or setup error occurred in either run.

### Call sites (`repark.sql(` and `PyReparkSession.native(` occurrences)

Counted by file over the 58 collected `test_*.py` files. A file takes the worst bucket of any of its tests, which is the "approximated by file" rule in the brief. So a file with one failing test counts entirely as failing.

| bucket | files | call sites |
|---|---:|---:|
| PASS->FAIL new refusal | 2 | 21 |
| PASS->FAIL different answer | 2 | 3 |
| PASS->PASS | 52 | 183 |
| SKIP (file has only skipped tests) | 2 | 8 |
| total | 58 | 215 |

The four files with a changed test hold 24 call sites: `dataframe/test_offset_nested_sort_1.py` (3), `timestamps/test_ice_tsns_type_planner_1.py` (1), `writes/test_empty_dml_shared_guard_1.py` (2) and `writes/test_repark_sql_update_subquery_where_1.py` (18). The five `_*.py` helper files that pytest does not collect hold 18 more call sites. The brief's figure of 236 was not reproduced: this count gives 215 occurrences in the collected files and 233 with the helpers. [provenance.md](provenance.md) gives the exact counting commands.

## The 14 changed tests

One row per test that fails in the prototype and passes in base. The excerpt is the JUnit message, scrubbed and cut to 240 characters.

| # | file | test | bucket | error class | excerpt (scrubbed, first 240 chars) |
|---|---|---|---|---|---|
| 1 | `dataframe/test_offset_nested_sort_1.py` | `test_native_sql_door_answers_spark_rows[16]` | PASS->FAIL new refusal | AnalysisException | repark.errors.AnalysisException: Error during planning: CREATE TABLE AS SELECT target `offset_nested_sort_1_native` is not a qualified Iceberg table name. Write `<catalog>.<schema>.<table>`, where <catalog> is a registered Iceberg catalog ( |
| 2 | `dataframe/test_offset_nested_sort_1.py` | `test_native_sql_door_answers_spark_rows[1]` | PASS->FAIL new refusal | AnalysisException | repark.errors.AnalysisException: Error during planning: CREATE TABLE AS SELECT target `offset_nested_sort_1_native` is not a qualified Iceberg table name. Write `<catalog>.<schema>.<table>`, where <catalog> is a registered Iceberg catalog ( |
| 3 | `dataframe/test_offset_nested_sort_1.py` | `test_native_sql_door_answers_spark_rows[2]` | PASS->FAIL new refusal | AnalysisException | repark.errors.AnalysisException: Error during planning: CREATE TABLE AS SELECT target `offset_nested_sort_1_native` is not a qualified Iceberg table name. Write `<catalog>.<schema>.<table>`, where <catalog> is a registered Iceberg catalog ( |
| 4 | `timestamps/test_ice_tsns_type_planner_1.py` | `test_refusals_microsecond_pins_and_registry_rows[doors|UTC]` | PASS->FAIL different answer | AssertionError | AssertionError: 1 of 19 cells red in doors\|UTC assert {'r013\|ansi_k...000000000]]}'} == {} Left contains 1 more item: {'r013\|ansi_kernel_not_a_routine': "the kernel resolves by name: {'types': " "['timestamp[ns]'], 'rows': " '[[176732304500 |
| 5 | `writes/test_empty_dml_shared_guard_1.py` | `test_an_empty_dml_writes_and_commits_nothing[native-u-required-is-null-DELETE]` | PASS->FAIL different answer | AssertionError | AssertionError: v2-cow DELETE FROM ice.ns.t_u_required_is_null_delete_v2_cow WHERE id IS NULL: {'rows': 'unchanged', 'snapshot_changed': True, 'files_changed': False, 'counts': []} != {'rows': 'unchanged', 'snapshot_changed': False, 'files_ |
| 6 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_non_default_parser_dialect_still_refuses` | PASS->FAIL new refusal | ParseException (sqlparser ParserError) | AssertionError: SQL error: ParserError("Expected: identifier, found: [ at Line: 1, Column: 13") assert "DELETE with a subquery in its WHERE clause is refused on `[ice].[ns].[t]`: subquery predicates are silently mis-execu...dapter's proven  |
| 7 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_shape_main_answered_by_coincidence_refuses[P-C1-delete_in_self]` | PASS->FAIL different answer | DID NOT RAISE | Failed: DID NOT RAISE AnalysisException |
| 8 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_iceberg_table[P-D1-in]` | PASS->FAIL different answer | DID NOT RAISE | Failed: DID NOT RAISE AnalysisException |
| 9 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_iceberg_table[P-D3-exists_correlated]` | PASS->FAIL different answer | DID NOT RAISE | Failed: DID NOT RAISE AnalysisException |
| 10 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_iceberg_table[P-D5-not_in]` | PASS->FAIL different answer | DID NOT RAISE | Failed: DID NOT RAISE AnalysisException |
| 11 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_iceberg_table[P-D6-exists_no_match]` | PASS->FAIL different answer | DID NOT RAISE | Failed: DID NOT RAISE AnalysisException |
| 12 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_iceberg_table[P-U1-in]` | PASS->FAIL different answer | DID NOT RAISE | Failed: DID NOT RAISE AnalysisException |
| 13 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_in_memory_table[P-D10-in_memory]` | PASS->FAIL new refusal | AnalysisException | repark.errors.AnalysisException: Error during planning: CREATE TABLE AS SELECT target `mem3` is not a qualified Iceberg table name. Write `<catalog>.<schema>.<table>`, where <catalog> is a registered Iceberg catalog (registered here: `ice`) |
| 14 | `writes/test_repark_sql_update_subquery_where_1.py` | `test_a_subquery_predicate_refuses_on_an_in_memory_table[P-U10-in_memory]` | PASS->FAIL new refusal | AnalysisException | repark.errors.AnalysisException: Error during planning: CREATE TABLE AS SELECT target `mem3` is not a qualified Iceberg table name. Write `<catalog>.<schema>.<table>`, where <catalog> is a registered Iceberg catalog (registered here: `ice`) |

## Reading the rows

- Rows 1 to 3 and 13 to 14 are the unqualified-target refusal. Rows 1 to 3 are `test_native_sql_door_answers_spark_rows` in `dataframe/test_offset_nested_sort_1.py`. The Spark-door twin of that test passes in both runs, so on the native door the statement now refuses where the Spark door answers. Rows 13 and 14 fail at the seed statement `repark.sql(MEM_SEED)` (`CREATE TABLE mem3 AS SELECT ...`, line 273 of `writes/test_repark_sql_update_subquery_where_1.py`), before the refusal under test runs. They are the only two in-memory cells in that file.
- Row 6 is the one reason change. The test expects the G3-E8 subquery refusal text (`refuse_and_hold`, line 216). The prototype's ANSI parse rejects `[ice].[ns].[t]` with a sqlparser ParserError first. The test sets the MsSQL parser dialect, which does not change that parse on this route in this run. The cause of the parse behaviour was not established here.
- Rows 7 to 12 are `DID NOT RAISE`: the test expects an `AnalysisException` and the prototype returns an answer. Five are subquery statements on an Iceberg table (`P-U1-in`, `P-D1-in`, `P-D3-exists_correlated`, `P-D5-not_in`, `P-D6-exists_no_match`). The sixth is the `P-C1-delete_in_self` shape, which the test calls answered by coincidence.
- Row 5 is the empty-DML contract. `DELETE FROM ... WHERE id IS NULL` on a v2 copy-on-write table, with no matching row, reports `snapshot_changed: True` on the prototype where base reports `False`. Rows and files are unchanged.
- Row 4 is a registry check: `1 of 19 cells red in doors|UTC`. The prototype has one extra item, `r013|ansi_kernel_not_a_routine`.

How the buckets were assigned: a prototype failure is "different answer" when its failure is an `AssertionError` or a `DID NOT RAISE`, which means the prototype returned an answer or a different value or state where base refused or answered otherwise. It is "new refusal" when the prototype raised a repark exception that base did not raise on that line. Row 6 raises the same class of refusal as base with different text, so it is a new refusal with a changed reason. That assignment is the one manual override in the classifier. The traceback lines for rows 13 and 14 were checked by hand.

## Top error classes

- New refusals (6): `AnalysisException` 5 (the unqualified CTAS target), `ParseException` 1 (sqlparser ParserError on bracketed names).
- Different answers (8): `DID NOT RAISE` 6 (the prototype answers statements that base refuses), `AssertionError` 2 (one registry row, one empty DELETE that commits a snapshot).

## Prototype build

- Compiled first time: yes. The patched tree built once with no compiler errors. An earlier build had started on the unpatched source before the edits landed; I stopped it before patching. That was not a compile failure.
- Duplicate registration: none. `native()` builds with `with_sql_dialect(AnsiDialect)`, `with_local_fs_guard()`, `NativeDoorTypes` and `register_native_door_functions` all kept, and the native sessions in the 1667 passing tests build.
- Stale text: the comment above `repark-spark` in `crates/repark-python/Cargo.toml` still lists `repark-sql` as a deliberate non-edge. The prototype leaves it, because the prototype is throwaway and the brief bans code comments.

## Not measured

- `io/test_udf_oracle.py` (1 call site) is skipped at collection because `pyspark` is not in the venv. The `record` extra was not synced.
- `iceberg/test_v3_live_file_order.py` (7 call sites) has one test, which is live-gated by `REPARK_PARITY_LIVE`. The live tier is unarmed, as in `make ci`.
- The 130 skips are live-tier gates and fork-ledger skips. Skipped tests in files bucketed PASS->PASS are not separated, so their call sites count as PASS->PASS although those tests did not run.
- The live Spark oracle was not run. Answers were checked against the pinned goldens and the Spark door's twin tests.
- The five `_*.py` helpers are not collected. They run only through the tests that import them.
