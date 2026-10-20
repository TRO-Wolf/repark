# WRITE-BODY-CARRIERS-1 step 0: Spark's answer

Recorded 2026-10-14 on live Spark 4.1.2 with `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` by `python/repark/tests/writes/_record_write_body_carriers_1_spark.py`. Full texts and row values live in [spark-cells.json](spark-cells.json); this table is a view over it.

| cell | exception | error class | condition | SQLSTATE | first line | t | z |
|---|---|---|---|---|---|---|---|
| create_insert_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 46) | same | same |
| create_insert_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 49) | same | same |
| create_overwrite_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 49) | same | same |
| create_overwrite_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 52) | same | same |
| create_update_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'UPDATE'. SQLSTATE: 42601 (line 1, pos 46) | same | same |
| create_update_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'UPDATE'. SQLSTATE: 42601 (line 1, pos 49) | same | same |
| create_delete_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'DELETE'. SQLSTATE: 42601 (line 1, pos 46) | same | same |
| create_delete_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'DELETE'. SQLSTATE: 42601 (line 1, pos 49) | same | same |
| create_merge_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'MERGE'. SQLSTATE: 42601 (line 1, pos 45) | same | same |
| create_merge_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'MERGE'. SQLSTATE: 42601 (line 1, pos 48) | same | same |
| or_replace_insert_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 61) | same | same |
| or_replace_insert_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 64) | same | same |
| or_replace_overwrite_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 64) | same | same |
| or_replace_overwrite_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 67) | same | same |
| or_replace_update_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'UPDATE'. SQLSTATE: 42601 (line 1, pos 61) | same | same |
| or_replace_update_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'UPDATE'. SQLSTATE: 42601 (line 1, pos 64) | same | same |
| or_replace_delete_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'DELETE'. SQLSTATE: 42601 (line 1, pos 61) | same | same |
| or_replace_delete_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'DELETE'. SQLSTATE: 42601 (line 1, pos 64) | same | same |
| or_replace_merge_z_new | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'MERGE'. SQLSTATE: 42601 (line 1, pos 60) | same | same |
| or_replace_merge_z_exists | ParseException | PARSE_SYNTAX_ERROR | PARSE_SYNTAX_ERROR | 42601 | [PARSE_SYNTAX_ERROR] Syntax error at or near 'MERGE'. SQLSTATE: 42601 (line 1, pos 63) | same | same |

20 cells, 20 refused, 0 with a table that reads back different.
