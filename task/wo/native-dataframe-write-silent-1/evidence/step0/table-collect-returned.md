# NATIVE-DATAFRAME-WRITE-SILENT-1 step 0, the returned frame collected (diagnostic)

### Door `native` (612 cells)

| Target | Verb | Verdict (all four frames unless split) | Before → after (sql frame) | Refusal (sql frame) |
|---|---|---|---|---|
| ice_existing | sat_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | sat_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 41") |
| ice_existing | sat_error | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e9` because it already exists. |
| ice_existing | sat_errorifexists | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e13` because it already exists. |
| ice_existing | sat_ignore | stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 |  |
| ice_existing | ins_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | ins_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| ice_existing | v2_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | v2_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: REPLACE at Line: 1, Column: … |
| ice_existing | v2_overwrite_partitions | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| ice_existing | v2_create | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'ice.ns.e41' because it already exists. Choose a different … |
| ice_existing | v2_create_or_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| ice_existing | v2_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| ice_existing | save_name | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | save_option | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: Error during planning: write options are not supported on this SQL door (1 option(s) refused) |
| hd_existing | sat_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | sat_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 41") |
| hd_existing | sat_error | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e69` because it already exists. |
| hd_existing | sat_errorifexists | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e73` because it already exists. |
| hd_existing | sat_ignore | stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 |  |
| hd_existing | ins_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | ins_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: REPLACE at Line: 1, Column: … |
| hd_existing | v2_overwrite_partitions | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_create | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'hd.ns.e101' because it already exists. Choose a different … |
| hd_existing | v2_create_or_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_existing | v2_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_existing | save_name | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | save_option | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: Error during planning: write options are not supported on this SQL door (1 option(s) refused) |
| ice_new | sat_append | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | sat_overwrite | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_new | sat_error | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | sat_errorifexists | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | sat_ignore | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | ins_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'ice.ns.n141' not found |
| ice_new | ins_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'ice.ns.n145' not found |
| ice_new | v2_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n149' because it does not exist. Use create() or … |
| ice_new | v2_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n153' because it does not exist. Use create() or … |
| ice_new | v2_overwrite_partitions | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n157' because it does not exist. Use create() or … |
| ice_new | v2_create | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | v2_create_or_replace | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_new | v2_replace | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'ice.ns.n169' because it does not exist. Use create() or … |
| ice_new | save_name | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n173 cannot be found. Verify the spelling and correctness of the schema … |
| ice_new | save_option | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n177 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | sat_append | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | sat_overwrite | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_new | sat_error | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | sat_errorifexists | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | sat_ignore | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | ins_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n201' not found |
| hd_new | ins_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n205' not found |
| hd_new | v2_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n209' because it does not exist. Use create() or … |
| hd_new | v2_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n213' because it does not exist. Use create() or … |
| hd_new | v2_overwrite_partitions | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n217' because it does not exist. Use create() or … |
| hd_new | v2_create | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | v2_create_or_replace | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_new | v2_replace | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'hd.ns.n229' because it does not exist. Use create() or … |
| hd_new | save_name | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n233 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | save_option | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n237 cannot be found. Verify the spelling and correctness of the schema … |
| ice_memory_schema | sat_append | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | sat_overwrite | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_memory_schema | sat_error | refused | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | sat_errorifexists | refused | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | sat_ignore | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | ins_append | stores | rows 2 → rows 4 |  |
| ice_memory_schema | ins_overwrite | refused-model-stores | rows 2 → rows 2 | UnsupportedOperationException: This feature is not implemented: Insert Overwrite not implemented for MemoryTable yet |
| ice_memory_schema | v2_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m269' because it does not exist. Use create() or … |
| ice_memory_schema | v2_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m273' because it does not exist. Use create() or … |
| ice_memory_schema | v2_overwrite_partitions | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m277' because it does not exist. Use create() or … |
| ice_memory_schema | v2_create | refused | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | v2_create_or_replace | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_memory_schema | v2_replace | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'ice.ms.m289' because it does not exist. Use create() or … |
| ice_memory_schema | save_name | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ms.m293 cannot be found. Verify the spelling and correctness of the schema … |
| ice_memory_schema | save_option | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ms.m297 cannot be found. Verify the spelling and correctness of the schema … |
| memory_qualified | sat_append | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | sat_overwrite | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | sat_error | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | sat_errorifexists | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | sat_ignore | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | ins_append | stores | rows 2 → rows 4 |  |
| memory_qualified | ins_overwrite | refused-model-stores | rows 2 → rows 2 | UnsupportedOperationException: This feature is not implemented: Insert Overwrite not implemented for MemoryTable yet |
| memory_qualified | v2_append | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | v2_overwrite | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | v2_overwrite_partitions | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | v2_create | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | v2_create_or_replace | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 54") |
| memory_qualified | v2_replace | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | save_name | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | save_option | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_bare | sat_append | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_overwrite | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_error | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_errorifexists | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_ignore | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | ins_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: Error during planning: table 'spark_catalog.default.m381' not found |
| memory_bare | ins_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: Error during planning: table 'spark_catalog.default.m385' not found |
| memory_bare | v2_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m389`. |
| memory_bare | v2_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m393`. |
| memory_bare | v2_overwrite_partitions | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m397`. |
| memory_bare | v2_create | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | v2_create_or_replace | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 58") |
| memory_bare | v2_replace | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | save_name | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | save_option | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_append | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_overwrite | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_error | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_errorifexists | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_ignore | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | ins_append | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.v441' not found |
| view | ins_overwrite | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.v445' not found |
| view | v2_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v449`. |
| view | v2_overwrite | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v453`. |
| view | v2_overwrite_partitions | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v457`. |
| view | v2_create | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | v2_create_or_replace | refused-model-stores | rows absent → rows absent | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 58") |
| view | v2_replace | refused | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | save_name | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | save_option | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_append | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_overwrite | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_error | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_errorifexists | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_ignore | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | ins_append | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv501' not found |
| temp_view | ins_overwrite | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv505' not found |
| temp_view | v2_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv509`. |
| temp_view | v2_overwrite | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv513`. |
| temp_view | v2_overwrite_partitions | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv517`. |
| temp_view | v2_create | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | v2_create_or_replace | refused-model-stores | rows absent → rows absent | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 59") |
| temp_view | v2_replace | refused | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | save_name | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | save_option | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| path_new | save_path | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x541 cannot be found. Verify the spelling and … |
| path_new | parquet_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | text_append | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | text_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_existing | save_path | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x577 cannot be found. Verify the spelling and … |
| path_existing | parquet_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | parquet_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | csv_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | json_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | json_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | text_append | stores | rows 2, files 1 → rows 4, files 3 |  |
| path_existing | text_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |

Counts: refused 164, refused-model-stores 320, stores 128

#### Partitioned-table reach on this door

| Attempt | Outcome |
|---|---|
| native_ddl_ice | UnsupportedOperationException: This feature is not implemented: Hive distribution not supported: PARTITIONED { columns: [ColumnDef { name: Ident { value: "p", … |
| native_ddl_hd | UnsupportedOperationException: This feature is not implemented: Hive distribution not supported: PARTITIONED { columns: [ColumnDef { name: Ident { value: "p", … |
| native_read_spark_door_table | AnalysisException: Error during planning: table 'hd.ns.p0' not found |

#### Workarounds on this door (`repark.sql(...)` collected)

| Target | Statement | Outcome | Before → after |
|---|---|---|---|
| ice_existing | insert_into | returned | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |
| ice_existing | insert_overwrite | returned | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |
| hd_existing | insert_into | returned | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |
| hd_existing | insert_overwrite | returned | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |
| ice_memory_schema | insert_into | returned | rows 2 → rows 4 |
| ice_memory_schema | insert_overwrite | UnsupportedOperationException: This feature is not implemented: Insert Overwrite not implemented for MemoryTable yet | rows 2 → rows 2 |
| memory_qualified | insert_into | returned | rows 2 → rows 4 |
| memory_qualified | insert_overwrite | UnsupportedOperationException: This feature is not implemented: Insert Overwrite not implemented for MemoryTable yet | rows 2 → rows 2 |
| path_new | copy_parquet | AnalysisException: Error during planning: COPY TO to local filesystem path `<warehouse>/paths/x621/part-0.parquet` is refused: … | rows absent, files 0 → rows absent, files 0 |
| path_existing | copy_parquet | AnalysisException: Error during planning: COPY TO to local filesystem path `<warehouse>/paths/x622/part-0.parquet` is refused: … | rows 2, files 1 → rows 2, files 1 |

### Door `spark-door` (312 cells)

| Target | Verb | Verdict (all four frames unless split) | Before → after (sql frame) | Refusal (sql frame) |
|---|---|---|---|---|
| hd_existing | sat_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | sat_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | sat_error | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e9` because it already exists. |
| hd_existing | sat_errorifexists | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e13` because it already exists. |
| hd_existing | sat_ignore | stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 |  |
| hd_existing | ins_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | ins_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 2 | PySparkException: DataInvalid => Cannot delete file where some, but not all, rows match filter p = 1: … |
| hd_existing | v2_overwrite_partitions | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_create | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'hd.ns.e41' because it already exists. Choose a different … |
| hd_existing | v2_create_or_replace | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_replace | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | save_name | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | save_option | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_partitioned | sat_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | sat_overwrite | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 3 |  |
| hd_partitioned | sat_error | refused | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`p8` because it already exists. |
| hd_partitioned | sat_errorifexists | refused | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`p12` because it already exists. |
| hd_partitioned | sat_ignore | stores | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 |  |
| hd_partitioned | ins_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | ins_overwrite | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 4 |  |
| hd_partitioned | v2_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | v2_overwrite | stores | rows 2, snap 1, files 2 → rows 3, snap 2, files 4 |  |
| hd_partitioned | v2_overwrite_partitions | stores | rows 2, snap 1, files 2 → rows 3, snap 2, files 4 |  |
| hd_partitioned | v2_create | refused | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'hd.ns.p40' because it already exists. Choose a different … |
| hd_partitioned | v2_create_or_replace | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 3 |  |
| hd_partitioned | v2_replace | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 3 |  |
| hd_partitioned | save_name | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | save_option | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_new | sat_append | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_overwrite | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_error | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_errorifexists | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_ignore | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | ins_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n141' not found |
| hd_new | ins_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: INSERT OVERWRITE target ``hd`.`ns`.`n145`` could not be loaded as an Iceberg table (catalog … |
| hd_new | v2_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n149' because it does not exist. Use create() or … |
| hd_new | v2_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n153' because it does not exist. Use create() or … |
| hd_new | v2_overwrite_partitions | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n157' because it does not exist. Use create() or … |
| hd_new | v2_create | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | v2_create_or_replace | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | v2_replace | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'hd.ns.n169' because it does not exist. Use create() or … |
| hd_new | save_name | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n173 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | save_option | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n177 cannot be found. Verify the spelling and correctness of the schema … |
| temp_view | sat_append | stores | rows absent → rows 2 |  |
| temp_view | sat_overwrite | stores | rows absent → rows 2 |  |
| temp_view | sat_error | stores | rows absent → rows 2 |  |
| temp_view | sat_errorifexists | stores | rows absent → rows 2 |  |
| temp_view | sat_ignore | stores | rows absent → rows 2 |  |
| temp_view | ins_append | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv201' not found |
| temp_view | ins_overwrite | refused | rows absent → rows absent | AnalysisException: Error during planning: INSERT OVERWRITE target ``spark_catalog`.`default`.`tv205`` could not be loaded as an Iceberg … |
| temp_view | v2_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv209`. |
| temp_view | v2_overwrite | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv213`. |
| temp_view | v2_overwrite_partitions | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv217`. |
| temp_view | v2_create | stores | rows absent → rows 2 |  |
| temp_view | v2_create_or_replace | stores | rows absent → rows 2 |  |
| temp_view | v2_replace | refused | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'tv229' because it does not exist. Use create() or createOrReplace() to … |
| temp_view | save_name | refused-model-stores | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view default.tv233 cannot be found. Verify the spelling and correctness of the … |
| temp_view | save_option | refused-model-stores | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view default.tv237 cannot be found. Verify the spelling and correctness of the … |
| path_new | save_path | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x241 cannot be found. Verify the spelling and … |
| path_new | parquet_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | text_append | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | text_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_existing | save_path | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x277 cannot be found. Verify the spelling and … |
| path_existing | parquet_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | parquet_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | csv_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | json_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | json_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | text_append | stores | rows 2, files 1 → rows 4, files 3 |  |
| path_existing | text_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |

Counts: refused 80, refused-model-stores 20, stores 212
