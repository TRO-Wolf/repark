# WRITE-BODY-CARRIERS-1 step 0: main's answer

Recorded 2026-10-14 on the main release module (repark 1.5.5, memory catalog) by `python/repark/tests/writes/_record_write_body_carriers_1_main.py`. Full texts and row values live in [main-cells.json](main-cells.json); these tables are a view over it. `s`/`r`/`f` are the snapshot count, row count and data-file count before `->` after.

## CTAS cells

| cell | outcome | first line | t | z |
|---|---|---|---|---|
| create_insert_z_new_v2 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r3/f2 | absent -> absent |
| create_insert_z_exists_v2 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_insert_z_exists_v2` because it already exists. Choose a  | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_overwrite_z_new_v2 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| create_overwrite_z_exists_v2 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_overwrite_z_exists_v2` because it already exists. Choose | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_update_z_new_v2 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r2/f1 | absent -> absent |
| create_update_z_exists_v2 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_update_z_exists_v2` because it already exists. Choose a  | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_delete_z_new_v2 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| create_delete_z_exists_v2 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_delete_z_exists_v2` because it already exists. Choose a  | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_merge_z_new_v2 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_create_merge_z_new_v2 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, | s1/r2/f1 -> s1/r2/f1 | absent -> absent |
| create_merge_z_exists_v2 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_merge_z_exists_v2` because it already exists. Choose a d | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_insert_z_new_v2 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r3/f2 | absent -> absent |
| or_replace_insert_z_exists_v2 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r3/f2 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_overwrite_z_new_v2 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| or_replace_overwrite_z_exists_v2 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r1/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_update_z_new_v2 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r2/f1 | absent -> absent |
| or_replace_update_z_exists_v2 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_delete_z_new_v2 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| or_replace_delete_z_exists_v2 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r1/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_merge_z_new_v2 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_or_replace_merge_z_new_v2 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS | s1/r2/f1 -> s1/r2/f1 | absent -> absent |
| or_replace_merge_z_exists_v2 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_or_replace_merge_z_exists_v2 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_insert_z_new_v3 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r3/f2 | absent -> absent |
| create_insert_z_exists_v3 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_insert_z_exists_v3` because it already exists. Choose a  | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_overwrite_z_new_v3 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| create_overwrite_z_exists_v3 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_overwrite_z_exists_v3` because it already exists. Choose | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_update_z_new_v3 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r2/f1 | absent -> absent |
| create_update_z_exists_v3 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_update_z_exists_v3` because it already exists. Choose a  | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_delete_z_new_v3 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| create_delete_z_exists_v3 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_delete_z_exists_v3` because it already exists. Choose a  | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| create_merge_z_new_v3 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_create_merge_z_new_v3 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, | s1/r2/f1 -> s1/r2/f1 | absent -> absent |
| create_merge_z_exists_v3 | AnalysisException | Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `wbc1`.`ns`.`z_create_merge_z_exists_v3` because it already exists. Choose a d | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_insert_z_new_v3 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r3/f2 | absent -> absent |
| or_replace_insert_z_exists_v3 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r3/f2 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_overwrite_z_new_v3 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| or_replace_overwrite_z_exists_v3 | PySparkException | DataInvalid => UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range. | s1/r2/f1 -> s2/r1/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_update_z_new_v3 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r2/f1 | absent -> absent |
| or_replace_update_z_exists_v3 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r2/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_delete_z_new_v3 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r1/f1 | absent -> absent |
| or_replace_delete_z_exists_v3 | PySparkException | datafusion engine error: Arrow error: Invalid argument error: must either specify a row count or at least one column | s1/r2/f1 -> s2/r1/f1 | s1/r1/f1 -> s1/r1/f1 |
| or_replace_merge_z_new_v3 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_or_replace_merge_z_new_v3 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS | s1/r2/f1 -> s1/r2/f1 | absent -> absent |
| or_replace_merge_z_exists_v3 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_or_replace_merge_z_exists_v3 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 | s1/r2/f1 -> s1/r2/f1 | s1/r1/f1 -> s1/r1/f1 |

## Wrapper cells

| cell | outcome | first line | t |
|---|---|---|---|
| explain_insert_v2 | answered |  | s1/r2/f1 -> s2/r3/f2 |
| explain_overwrite_v2 | answered |  | s1/r2/f1 -> s2/r1/f1 |
| explain_update_v2 | answered |  | s1/r2/f1 -> s2/r2/f1 |
| explain_delete_v2 | answered |  | s1/r2/f1 -> s2/r1/f1 |
| explain_merge_v2 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_explain_merge_v2 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, 'n'  | s1/r2/f1 -> s1/r2/f1 |
| explain_insert_v3 | answered |  | s1/r2/f1 -> s2/r3/f2 |
| explain_overwrite_v3 | answered |  | s1/r2/f1 -> s2/r1/f1 |
| explain_update_v3 | answered |  | s1/r2/f1 -> s2/r2/f1 |
| explain_delete_v3 | answered |  | s1/r2/f1 -> s2/r1/f1 |
| explain_merge_v3 | UnsupportedOperationException | This feature is not implemented: Unsupported SQL statement: MERGE INTO wbc1.ns.t_explain_merge_v3 USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, 'n'  | s1/r2/f1 -> s1/r2/f1 |
| prepare_insert_v2 | prepare=answered execute=answered |  | s1/r2/f1 -> s2/r3/f2 |
| prepare_overwrite_v2 | prepare=answered execute=answered |  | s1/r2/f1 -> s2/r1/f1 |
| prepare_insert_v3 | prepare=answered execute=answered |  | s1/r2/f1 -> s2/r3/f2 |
| prepare_overwrite_v3 | prepare=answered execute=answered |  | s1/r2/f1 -> s2/r1/f1 |

54 cells, 36 with a table that reads back different.
