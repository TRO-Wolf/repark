# NATIVE-DATAFRAME-WRITE-SILENT-1 step 0, the returned frame collected (diagnostic)

### Door `native` (1568 cells)

| Target | Verb | Verdict (all four frames unless split) | Before → after (sql frame) | Refusal (sql frame) |
|---|---|---|---|---|
| ice_existing | sat_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | sat_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 41") |
| ice_existing | sat_error | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e15` because it already exists. |
| ice_existing | sat_errorifexists | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e22` because it already exists. |
| ice_existing | sat_ignore | stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 |  |
| ice_existing | ins_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | ins_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| ice_existing | v2_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | v2_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: REPLACE at Line: 1, Column: … |
| ice_existing | v2_overwrite_partitions | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| ice_existing | v2_create | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'ice.ns.e71' because it already exists. Choose a different … |
| ice_existing | v2_create_or_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| ice_existing | v2_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| ice_existing | save_name | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | save_option | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: Error during planning: write options are not supported on this SQL door (1 option(s) refused) |
| ice_existing | save_name_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| ice_existing | ins_partition_by | WRONG | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | v2_property_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | v2_partitioned_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| ice_existing | merge_insert_all | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 186") |
| hd_existing | sat_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | sat_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_existing | sat_error | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e155` because it already exists. |
| hd_existing | sat_errorifexists | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e162` because it already exists. |
| hd_existing | sat_ignore | stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 |  |
| hd_existing | ins_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | ins_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: REPLACE at Line: 1, Column: … |
| hd_existing | v2_overwrite_partitions | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_create | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'hd.ns.e211' because it already exists. Choose a different … |
| hd_existing | v2_create_or_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_existing | v2_replace | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_existing | save_name | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | save_option | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: Error during planning: write options are not supported on this SQL door (1 option(s) refused) |
| hd_existing | save_name_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | ins_partition_by | WRONG | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_property_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_partitioned_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | merge_insert_all | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 185") |
| ice_new | sat_append | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | sat_overwrite | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_new | sat_error | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | sat_errorifexists | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | sat_ignore | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | ins_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'ice.ns.n316' not found |
| ice_new | ins_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'ice.ns.n323' not found |
| ice_new | v2_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n330' because it does not exist. Use create() or … |
| ice_new | v2_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n337' because it does not exist. Use create() or … |
| ice_new | v2_overwrite_partitions | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n344' because it does not exist. Use create() or … |
| ice_new | v2_create | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_new | v2_create_or_replace | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_new | v2_replace | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'ice.ns.n365' because it does not exist. Use create() or … |
| ice_new | save_name | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n372 cannot be found. Verify the spelling and correctness of the schema … |
| ice_new | save_option | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n379 cannot be found. Verify the spelling and correctness of the schema … |
| ice_new | save_name_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n386 cannot be found. Verify the spelling and correctness of the schema … |
| ice_new | ins_partition_by | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'ice.ns.n393' not found |
| ice_new | v2_property_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n400' because it does not exist. Use create() or … |
| ice_new | v2_partitioned_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ns.n407' because it does not exist. Use create() or … |
| ice_new | merge_insert_all | refused | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 186") |
| hd_new | sat_append | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | sat_overwrite | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_new | sat_error | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | sat_errorifexists | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | sat_ignore | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | ins_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n456' not found |
| hd_new | ins_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n463' not found |
| hd_new | v2_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n470' because it does not exist. Use create() or … |
| hd_new | v2_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n477' because it does not exist. Use create() or … |
| hd_new | v2_overwrite_partitions | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n484' because it does not exist. Use create() or … |
| hd_new | v2_create | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31") |
| hd_new | v2_create_or_replace | refused-model-stores | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 42") |
| hd_new | v2_replace | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'hd.ns.n505' because it does not exist. Use create() or … |
| hd_new | save_name | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n512 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | save_option | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n519 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | save_name_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n526 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | ins_partition_by | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n533' not found |
| hd_new | v2_property_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n540' because it does not exist. Use create() or … |
| hd_new | v2_partitioned_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n547' because it does not exist. Use create() or … |
| hd_new | merge_insert_all | refused | rows absent, files 0 → rows absent, files 0 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 185") |
| ice_memory_schema | sat_append | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | sat_overwrite | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_memory_schema | sat_error | refused | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | sat_errorifexists | refused | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | sat_ignore | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | ins_append | stores | rows 2 → rows 4 |  |
| ice_memory_schema | ins_overwrite | refused-model-stores | rows 2 → rows 2 | UnsupportedOperationException: This feature is not implemented: Insert Overwrite not implemented for MemoryTable yet |
| ice_memory_schema | v2_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m610' because it does not exist. Use create() or … |
| ice_memory_schema | v2_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m617' because it does not exist. Use create() or … |
| ice_memory_schema | v2_overwrite_partitions | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m624' because it does not exist. Use create() or … |
| ice_memory_schema | v2_create | refused | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 32") |
| ice_memory_schema | v2_create_or_replace | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 43") |
| ice_memory_schema | v2_replace | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'ice.ms.m645' because it does not exist. Use create() or … |
| ice_memory_schema | save_name | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ms.m652 cannot be found. Verify the spelling and correctness of the schema … |
| ice_memory_schema | save_option | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ms.m659 cannot be found. Verify the spelling and correctness of the schema … |
| ice_memory_schema | save_name_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ms.m666 cannot be found. Verify the spelling and correctness of the schema … |
| ice_memory_schema | ins_partition_by | WRONG | rows 2 → rows 4 |  |
| ice_memory_schema | v2_property_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m680' because it does not exist. Use create() or … |
| ice_memory_schema | v2_partitioned_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'ice.ms.m687' because it does not exist. Use create() or … |
| ice_memory_schema | merge_insert_all | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 186") |
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
| memory_qualified | save_name_overwrite | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | ins_partition_by | WRONG | rows 2 → rows 4 |  |
| memory_qualified | v2_property_append | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | v2_partitioned_append | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'datafusion' |
| memory_qualified | merge_insert_all | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 197") |
| memory_bare | sat_append | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_overwrite | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_error | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_errorifexists | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | sat_ignore | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | ins_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: Error during planning: table 'spark_catalog.default.m876' not found |
| memory_bare | ins_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: Error during planning: table 'spark_catalog.default.m883' not found |
| memory_bare | v2_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m890`. |
| memory_bare | v2_overwrite | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m897`. |
| memory_bare | v2_overwrite_partitions | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m904`. |
| memory_bare | v2_create | refused | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | v2_create_or_replace | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 58") |
| memory_bare | v2_replace | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | save_name | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | save_option | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | save_name_overwrite | refused-model-stores | rows 2 → rows 2 | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| memory_bare | ins_partition_by | refused | rows 2 → rows 2 | AnalysisException: Error during planning: table 'spark_catalog.default.m953' not found |
| memory_bare | v2_property_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m960`. |
| memory_bare | v2_partitioned_append | refused-model-stores | rows 2 → rows 2 | AnalysisException: Writing into a view is not allowed. View: `m967`. |
| memory_bare | merge_insert_all | refused-model-stores | rows 2 → rows 2 | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 201") |
| view | sat_append | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_overwrite | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_error | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_errorifexists | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | sat_ignore | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | ins_append | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.v1016' not found |
| view | ins_overwrite | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.v1023' not found |
| view | v2_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v1030`. |
| view | v2_overwrite | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v1037`. |
| view | v2_overwrite_partitions | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v1044`. |
| view | v2_create | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | v2_create_or_replace | refused-model-stores | rows absent → rows absent | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 59") |
| view | v2_replace | refused | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | save_name | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | save_option | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | save_name_overwrite | refused | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| view | ins_partition_by | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.v1093' not found |
| view | v2_property_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v1100`. |
| view | v2_partitioned_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `v1107`. |
| view | merge_insert_all | refused | rows absent → rows absent | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 204") |
| temp_view | sat_append | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_overwrite | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_error | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_errorifexists | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | sat_ignore | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | ins_append | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv1156' not found |
| temp_view | ins_overwrite | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv1163' not found |
| temp_view | v2_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv1170`. |
| temp_view | v2_overwrite | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv1177`. |
| temp_view | v2_overwrite_partitions | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv1184`. |
| temp_view | v2_create | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | v2_create_or_replace | refused-model-stores | rows absent → rows absent | ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 60") |
| temp_view | v2_replace | refused | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | save_name | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | save_option | refused-model-stores | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | save_name_overwrite | refused | rows absent → rows absent | PySparkException: datafusion engine error: unknown catalog 'spark_catalog' |
| temp_view | ins_partition_by | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv1233' not found |
| temp_view | v2_property_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv1240`. |
| temp_view | v2_partitioned_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv1247`. |
| temp_view | merge_insert_all | refused | rows absent → rows absent | ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, Column: 207") |
| path_new | save_path | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x1261 cannot be found. Verify the spelling and … |
| path_new | parquet_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | text_append | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | text_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | parquet_default | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_error | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_errorifexists | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_ignore | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_default | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_default | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_partitioned_append | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | parquet_partitioned_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | csv_partitioned_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | json_partitioned_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | parquet_format_save | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_save_kwargs | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_option_path_save | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_existing | save_path | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x1415 cannot be found. Verify the spelling and … |
| path_existing | parquet_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | parquet_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | csv_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | json_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | json_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | text_append | stores | rows 2, files 1 → rows 4, files 3 |  |
| path_existing | text_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | parquet_default | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x1478 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_error | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x1485 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_errorifexists | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x1492 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_ignore | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_default | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x1506 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | json_default | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x1513 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_partitioned_append | refused-model-stores | rows 2, files 1 → rows 2, files 1 | AnalysisException: cannot append DataFrame columns ['id', 'p'] to path parquet schema ['id', 'p']: column sets differ; missing from the … |
| path_existing | parquet_partitioned_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | csv_partitioned_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | json_partitioned_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | parquet_format_save | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_save_kwargs | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | parquet_option_path_save | stores | rows 2, files 1 → rows 2, files 1 |  |

Counts: WRONG 28, refused 469, refused-model-stores 665, stores 406

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
| path_new | copy_parquet | AnalysisException: Error during planning: COPY TO to local filesystem path `<warehouse>/paths/x1577/part-0.parquet` is refused: … | rows absent, files 0 → rows absent, files 0 |
| path_existing | copy_parquet | AnalysisException: Error during planning: COPY TO to local filesystem path `<warehouse>/paths/x1578/part-0.parquet` is refused: … | rows 2, files 1 → rows 2, files 1 |

### Door `spark-door` (868 cells)

| Target | Verb | Verdict (all four frames unless split) | Before → after (sql frame) | Refusal (sql frame) |
|---|---|---|---|---|
| hd_existing | sat_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | sat_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | sat_error | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e15` because it already exists. |
| hd_existing | sat_errorifexists | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e22` because it already exists. |
| hd_existing | sat_ignore | stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 |  |
| hd_existing | ins_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | ins_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_overwrite | refused-model-stores | rows 2, snap 1, files 1 → rows 2, snap 1, files 2 | PySparkException: DataInvalid => Cannot delete file where some, but not all, rows match filter p = 1: … |
| hd_existing | v2_overwrite_partitions | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_create | refused | rows 2, snap 1, files 1 → rows 2, snap 1, files 1 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'hd.ns.e71' because it already exists. Choose a different … |
| hd_existing | v2_create_or_replace | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | v2_replace | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | save_name | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | save_option | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | save_name_overwrite | stores | rows 2, snap 1, files 1 → rows 2, snap 2, files 2 |  |
| hd_existing | ins_partition_by | WRONG | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_property_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | v2_partitioned_append | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_existing | merge_insert_all | stores | rows 2, snap 1, files 1 → rows 4, snap 2, files 2 |  |
| hd_partitioned | sat_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | sat_overwrite | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 3 |  |
| hd_partitioned | sat_error | refused | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`p14` because it already exists. |
| hd_partitioned | sat_errorifexists | refused | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`p21` because it already exists. |
| hd_partitioned | sat_ignore | stores | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 |  |
| hd_partitioned | ins_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | ins_overwrite | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 4 |  |
| hd_partitioned | v2_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | v2_overwrite | stores | rows 2, snap 1, files 2 → rows 3, snap 2, files 4 |  |
| hd_partitioned | v2_overwrite_partitions | stores | rows 2, snap 1, files 2 → rows 3, snap 2, files 4 |  |
| hd_partitioned | v2_create | refused | rows 2, snap 1, files 2 → rows 2, snap 1, files 2 | AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'hd.ns.p70' because it already exists. Choose a different … |
| hd_partitioned | v2_create_or_replace | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 3 |  |
| hd_partitioned | v2_replace | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 3 |  |
| hd_partitioned | save_name | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | save_option | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | save_name_overwrite | stores | rows 2, snap 1, files 2 → rows 2, snap 2, files 4 |  |
| hd_partitioned | ins_partition_by | WRONG | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | v2_property_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | v2_partitioned_append | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_partitioned | merge_insert_all | stores | rows 2, snap 1, files 2 → rows 4, snap 2, files 4 |  |
| hd_new | sat_append | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_overwrite | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_error | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_errorifexists | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | sat_ignore | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | ins_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n316' not found |
| hd_new | ins_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: INSERT OVERWRITE target ``hd`.`ns`.`n323`` could not be loaded as an Iceberg table (catalog … |
| hd_new | v2_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n330' because it does not exist. Use create() or … |
| hd_new | v2_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n337' because it does not exist. Use create() or … |
| hd_new | v2_overwrite_partitions | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n344' because it does not exist. Use create() or … |
| hd_new | v2_create | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | v2_create_or_replace | stores | rows absent, files 0 → rows 2, snap 1, files 1 |  |
| hd_new | v2_replace | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'hd.ns.n365' because it does not exist. Use create() or … |
| hd_new | save_name | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n372 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | save_option | refused-model-stores | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n379 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | save_name_overwrite | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view ns.n386 cannot be found. Verify the spelling and correctness of the schema … |
| hd_new | ins_partition_by | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: Error during planning: table 'hd.ns.n393' not found |
| hd_new | v2_property_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n400' because it does not exist. Use create() or … |
| hd_new | v2_partitioned_append | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'hd.ns.n407' because it does not exist. Use create() or … |
| hd_new | merge_insert_all | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: TableNotFound => No such table: TableIdent { namespace: NamespaceIdent(["ns"]), name: "n414" } |
| temp_view | sat_append | stores | rows absent → rows 2 |  |
| temp_view | sat_overwrite | stores | rows absent → rows 2 |  |
| temp_view | sat_error | stores | rows absent → rows 2 |  |
| temp_view | sat_errorifexists | stores | rows absent → rows 2 |  |
| temp_view | sat_ignore | stores | rows absent → rows 2 |  |
| temp_view | ins_append | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv456' not found |
| temp_view | ins_overwrite | refused | rows absent → rows absent | AnalysisException: Error during planning: INSERT OVERWRITE target ``spark_catalog`.`default`.`tv463`` could not be loaded as an Iceberg … |
| temp_view | v2_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv470`. |
| temp_view | v2_overwrite | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv477`. |
| temp_view | v2_overwrite_partitions | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv484`. |
| temp_view | v2_create | stores | rows absent → rows 2 |  |
| temp_view | v2_create_or_replace | stores | rows absent → rows 2 |  |
| temp_view | v2_replace | refused | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'tv505' because it does not exist. Use create() or createOrReplace() to … |
| temp_view | save_name | refused-model-stores | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view default.tv512 cannot be found. Verify the spelling and correctness of the … |
| temp_view | save_option | refused-model-stores | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view default.tv519 cannot be found. Verify the spelling and correctness of the … |
| temp_view | save_name_overwrite | refused | rows absent → rows absent | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view default.tv526 cannot be found. Verify the spelling and correctness of the … |
| temp_view | ins_partition_by | refused | rows absent → rows absent | AnalysisException: Error during planning: table 'spark_catalog.default.tv533' not found |
| temp_view | v2_property_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv540`. |
| temp_view | v2_partitioned_append | refused | rows absent → rows absent | AnalysisException: Writing into a view is not allowed. View: `tv547`. |
| temp_view | merge_insert_all | refused | rows absent → rows absent | AnalysisException: TableNotFound => No such table: TableIdent { namespace: NamespaceIdent(["default"]), name: "tv554" } |
| path_new | save_path | refused | rows absent, files 0 → rows absent, files 0 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x561 cannot be found. Verify the spelling and … |
| path_new | parquet_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_append | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_overwrite | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | text_append | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | text_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | parquet_default | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_error | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_errorifexists | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_ignore | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_default | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | json_default | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_partitioned_append | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | parquet_partitioned_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | csv_partitioned_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | json_partitioned_overwrite | stores | rows absent, files 0 → rows 2, files 2 |  |
| path_new | parquet_format_save | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | csv_save_kwargs | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_new | parquet_option_path_save | stores | rows absent, files 0 → rows 2, files 1 |  |
| path_existing | save_path | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<warehouse>/paths`.x715 cannot be found. Verify the spelling and … |
| path_existing | parquet_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | parquet_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | csv_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | json_append | stores | rows 2, files 1 → rows 4, files 2 |  |
| path_existing | json_overwrite | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | text_append | stores | rows 2, files 1 → rows 4, files 3 |  |
| path_existing | text_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | parquet_default | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x778 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_error | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x785 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_errorifexists | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x792 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_ignore | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_default | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x806 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | json_default | refused | rows 2, files 1 → rows 2, files 1 | AnalysisException: [PATH_ALREADY_EXISTS] Path <warehouse>/paths/x813 already exists. Set mode as "overwrite" to overwrite the existing … |
| path_existing | parquet_partitioned_append | refused-model-stores | rows 2, files 1 → rows 2, files 1 | AnalysisException: cannot append DataFrame columns ['id', 'p'] to path parquet schema ['id', 'p']: column sets differ; missing from the … |
| path_existing | parquet_partitioned_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | csv_partitioned_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | json_partitioned_overwrite | stores | rows 2, files 1 → rows 2, files 2 |  |
| path_existing | parquet_format_save | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | csv_save_kwargs | stores | rows 2, files 1 → rows 2, files 1 |  |
| path_existing | parquet_option_path_save | stores | rows 2, files 1 → rows 2, files 1 |  |

Counts: WRONG 14, refused 245, refused-model-stores 42, stores 567
