# NAN-WRITE-CANONICAL-1 step 0 — stored bits per write door, main `1d9d2808` against Spark 4.1.2

Rendered by `render_step0.py --repark raw/repark-1d9d2808.jsonl --spark raw/spark-4.1.2.jsonl --label main`.
Every table holds the 14 rows of `write_doors_probe.py` `ROWS`: ids 1–8 are NaN (`sqrt(x)` of -1, `sqrt` of a
literal -1, `-x` of NaN, `x - x` of infinity, `CAST('NaN' …)`, a payload NaN `7ff8deadbeef0001`/`7fc00123`, the same
with the sign set, a canonical NaN column), ids 9–14 are the controls (`-0.0`, `0.0`, `+inf`, `-inf`, NULL, `1.5`).
`u_` tables are unpartitioned, `p_` tables are `PARTITIONED BY (d, f)`. "NaN bits" is the set of bit patterns the
Parquet data files hold for rows 1–8 (read from the files, not through an engine). "NaN partition value" is the set of
DOUBLE partition values the current manifests hold for NaN partitions, "NaN-partition files" their count. The `= NaN`
and `isnan` columns list the ids each engine's read of its own table returns. "NaN count" sums the manifests'
`nan_value_counts` for `d` over live data files.

## 1. Every door

| table | main NaN bits d / f | Spark NaN bits d / f | controls d, f equal | NaN partition value main / Spark | NaN-partition files main / Spark | `= NaN` d main / Spark | `isnan(d)` main / Spark | NaN count d main / Spark |
|---|---|---|---|---|---|---|---|---|
| n_ctas | refused: datafusion engine error: Arrow error: Invalid argument error: column types must match sche | | | | | | | |
| n_insert | - / - | - / - | yes | - / - | 0 / 0 | - / - | - / - | 0 / 0 |
| p_ctas | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 4 / 1 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_delete_cow | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_delete_mor | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_insert | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_insert_overwrite | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 3 / 1 | 5,6,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_insertinto | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_merge_insert | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 3 / 1 | 5,6,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_merge_update_cow | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 3 / 1 | 5,6,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_merge_update_mor | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 3 / 1 |  / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_path_append | refused: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<scratch>/wh-main/ns`.p_path_append cannot be | | | | | | | |
| p_rtas | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 4 / 1 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_save_name | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_saveastable_append | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_saveastable_overwrite | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 4 / 1 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_update_cow | 7ff8000000000000 fff8000000000000 fff8deadbeef0001 / 7fc00000 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 / 7ff8000000000000 | 6 / 5 | 5,7 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_update_mor | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 / 7ff8000000000000 | 8 / 7 | 5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 9 / 9 |
| p_writeto_append | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 3 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_create | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 4 / 1 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_create_or_replace | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 4 / 1 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_overwrite | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 3 / 1 | 5,6,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_overwrite_partitions | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 fff8000000000000 fff8deadbeef0001 / 7ff8000000000000 | 3 / 1 | 5,6,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| plain | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | - / - | - / - | 0 / 0 |
| u_ctas | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_delete_cow | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_delete_mor | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_insert | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_insert_overwrite | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_insertinto | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_merge_insert | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_merge_update_cow | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_merge_update_mor | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 4000000000000000 7ff8000000000000 / 40000000 7fc00000 | NO | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_path_append | refused: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<scratch>/wh-main/ns`.u_path_append cannot be | | | | | | | |
| u_rtas | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_save_name | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_saveastable_append | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_saveastable_overwrite | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_update_cow | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_update_mor | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 3ff8000000000000 7ff0000000000000 7ff8000000000000 bff0000000000000 / 3fc00000 7f800000 7fc00000 bf800000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 9 / 9 |
| u_writeto_append | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_create | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_create_or_replace | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_overwrite | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_overwrite_partitions | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 5,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |

| native table | main NaN bits d / f | controls d | outcome |
|---|---|---|---|
| u_native_ctas | - / - | {9: None, 10: None, 11: None, 12: None, 13: None, 14: None} | error repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table nat.ns.u_n |
| u_native_delete | 7ff8000000000000 fff8000000000000 / 7fc00000 ffc00000 | {9: '8000000000000000', 10: '0000000000000000', 11: '7ff0000000000000', 12: 'fff0000000000000', 13: None, 14: None} | ok  |
| u_native_insert | 7ff8000000000000 fff8000000000000 / 7fc00000 ffc00000 | {9: '8000000000000000', 10: '0000000000000000', 11: '7ff0000000000000', 12: 'fff0000000000000', 13: None, 14: '3ff8000000000000'} | ok  |
| u_native_insert_partitioned | - / - | {9: None, 10: None, 11: None, 12: None, 13: None, 14: None} | error This feature is not implemented: Hive distribution not supported: PARTITIONED { columns: [ |
| u_native_merge | - / - | {9: None, 10: None, 11: None, 12: None, 13: None, 14: None} | error This feature is not implemented: Unsupported SQL statement: MERGE INTO nat.ns.u_native_mer |
| u_native_update | 7ff8000000000000 fff8000000000000 / 7fc00000 ffc00000 | {9: '8000000000000000', 10: '0000000000000000', 11: '7ff0000000000000', 12: 'fff0000000000000', 13: None, 14: '3ff8000000000000'} | ok  |

Reading the table:

- **Stored bits.** Every RePark Iceberg door stores the bits the row arrived with: `fff8…`/`ffc00000` for the
  in-query producers and both payloads unchanged. Spark stores `7ff8000000000000`/`7fc00000` for all eight rows on
  every door, partitioned or not. The native door (no payload rows; its source is a stored table) stores `fff8…`.
- **Controls** (`-0.0`, `0.0`, `±inf`, NULL, `1.5`) are equal on every door. `u_update_mor` and `u_merge_update_mor`
  list Spark's pre-update values as well because merge-on-read keeps the old rows in live files under a delete file;
  the probe reads every live data file.
- **Partition values.** CTAS, RTAS, `writeTo` `create`/`createOrReplace` and `saveAsTable` overwrite store four NaN
  partition values (the raw bits of each pattern); INSERT OVERWRITE, `writeTo` `overwrite`/`overwritePartitions`,
  MERGE and UPDATE store two or three; INSERT, `insertInto`, `writeTo().append()`, `saveAsTable` append and
  `save(<name>)` store only `7ff8…` (their file list crosses the JSON step of NAN-PARTITION-VALUE-1, which reads
  `"NaN"` back as the canonical NaN). Spark stores one, `7ff8…`.
- **File split.** RePark writes the NaN partition as 3–4 files on the insert-shaped doors; Spark writes 1 (the
  update doors' counts include files from earlier statements on both engines).
- **RePark's own read.** On every door whose partition values hold a non-canonical NaN, `= CAST('NaN' AS DOUBLE)`
  misses rows (for example `p_ctas` returns `5,8` where Spark returns `1…8`); unpartitioned tables return `5,8` on
  every door (the sign-set and payload rows compare unequal by bits). `isnan` finds all eight on both engines.
- **NaN counts** are equal (8 / 8) on every door: the statistics already count every NaN pattern.

## 2. Nested leaves (`n_insert`, `n_ctas`)

| table | RePark main, row 3 (`-x`) and row 7 (sign-set payload) | Spark |
|---|---|---|
| `n_insert` | `s.a`, `a[0]`, `m['k']`: `fff8000000000000` / `fff8deadbeef0001`; `s.b`, `af[0].e`: `ffc00000` / `ffc00123` | every leaf `7ff8000000000000` / `7fc00000` |
| `n_ctas` | refused: `column types must match schema types, expected Map(… "keys" …)` (a map CTAS fails on main, outside this unit) | every leaf canonical |

Row 9 (`-0.0`) is `8000000000000000` / `80000000` in every leaf on both engines.

## 3. The rewrite cell (rule 4)

Source: `u_merge_insert` (one data file holding all 14 rows) and `p_insert`, written by main's build
(`raw/*-rewrite.jsonl`, phases `before`/`after`). Each engine runs `UPDATE … SET label = 'touched'` on one row
(`id = 14`, `id = 8`) and the new data files are read back. Spark reads the RePark tables through its Hadoop
catalog; RePark reads them through `register_table`.

| table, row updated | stored before (rows 3, 6, 7) | Spark's rewrite | main's rewrite |
|---|---|---|---|
| `u_merge_insert`, 14 | `fff8000000000000`, `7ff8deadbeef0001`, `fff8deadbeef0001` | all three `7ff8000000000000` in the new file; `-0.0` stays `8000000000000000` | all three unchanged in the new file |
| `p_insert`, 8 | row 6 shares row 8's file; rows 3, 7 in other files | row 6 `7ff8000000000000`; files not rewritten keep their bits | row 6 `7ff8000000000000`, consistent with the reader filling the identity column from the file's partition value (this door stored `7ff8…`); not traced |

So Spark's copy-on-write UPDATE canonicalises every NaN of every row it rewrites, changed or not, and leaves files
it does not rewrite alone. A writer-side seat matches this.

## 4. Plain Parquet path write (not Iceberg)

`df.write.parquet(<dir>)` (`plain`): RePark stores the raw bits, Spark (parquet-java's `doubleToLongBits`) stores the
canonical NaN. This writer is DataFusion's, outside the Iceberg write; it is recorded, not this unit's.

## 5. The head, `e100717f` (fork `d6592644`), after the fix

Re-run 2026-11-13 on the head's release build (`maturin develop --profile release`, module sha256
`dd1009157ad8375a8e55ab15cae499f1d322e7de8d430a8faaac8289a4a4d2f4`), same legs, scripts and locks as §1; Spark's legs
are §1's. Rendered by `render_step0.py --repark raw/repark-e100717f.jsonl --spark raw/spark-4.1.2.jsonl --label head`.

**Per partitioned door** (from §1 and the render below). "RAW-BITS-1" is NAN-PARTITION-RAW-BITS-1, "FILE-SPLIT-1" is
NAN-PARTITION-FILE-SPLIT-1; "canonical on main" means that door already stored one canonical NaN partition value.
The update doors' file counts include files from earlier statements on both engines.

| door | NaN partition values main → head (Spark) | NaN-partition files main → head (Spark) | `= NaN` d head | RAW-BITS-1 | FILE-SPLIT-1 |
|---|---|---|---|---|---|
| `ctas` | 4 → 1 (1) | 4 → 1 (1) | 1–8 | closed | closed |
| `delete_cow` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `delete_mor` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `insert` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `insert_overwrite` | 3 → 1 (1) | 3 → 1 (1) | 1–8 | closed | closed |
| `insertinto` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `merge_insert` | 3 → 1 (1) | 3 → 1 (1) | 1–8 | closed | closed |
| `merge_update_cow` | 3 → 1 (1) | 3 → 1 (1) | 1–8 | closed | closed |
| `merge_update_mor` | 3 → 1 (1) | 3 → 1 (1) | 1–8 | closed | closed |
| `rtas` | 4 → 1 (1) | 4 → 1 (1) | 1–8 | closed | closed |
| `save_name` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `saveastable_append` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `saveastable_overwrite` | 4 → 1 (1) | 4 → 1 (1) | 1–8 | closed | closed |
| `update_cow` | 2 → 1 (1) | 6 → 5 (5) | 1–8 | closed | closed |
| `update_mor` | 2 → 1 (1) | 8 → 7 (7) | 1–8 | closed | closed |
| `writeto_append` | 1 → 1 (1) | 3 → 1 (1) | 1–8 | canonical on main | closed |
| `writeto_create` | 4 → 1 (1) | 4 → 1 (1) | 1–8 | closed | closed |
| `writeto_create_or_replace` | 4 → 1 (1) | 4 → 1 (1) | 1–8 | closed | closed |
| `writeto_overwrite` | 3 → 1 (1) | 3 → 1 (1) | 1–8 | closed | closed |
| `writeto_overwrite_partitions` | 3 → 1 (1) | 3 → 1 (1) | 1–8 | closed | closed |

**Every door, rendered:**

| table | head NaN bits d / f | Spark NaN bits d / f | controls d, f equal | NaN partition value head / Spark | NaN-partition files head / Spark | `= NaN` d head / Spark | `isnan(d)` head / Spark | NaN count d head / Spark |
|---|---|---|---|---|---|---|---|---|
| n_ctas | refused: datafusion engine error: Arrow error: Invalid argument error: column types must match sche | | | | | | | |
| n_insert | - / - | - / - | yes | - / - | 0 / 0 | - / - | - / - | 0 / 0 |
| p_ctas | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_delete_cow | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_delete_mor | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_insert | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_insert_overwrite | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_insertinto | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_merge_insert | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_merge_update_cow | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_merge_update_mor | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_path_append | refused: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<scratch>/whh/doors/ns`.p_path_append cannot  | | | | | | | |
| p_rtas | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_save_name | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_saveastable_append | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_saveastable_overwrite | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_update_cow | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 5 / 5 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_update_mor | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 7 / 7 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 9 / 9 |
| p_writeto_append | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_create | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_create_or_replace | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_overwrite | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| p_writeto_overwrite_partitions | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | 7ff8000000000000 / 7ff8000000000000 | 1 / 1 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| plain | 7ff8000000000000 7ff8deadbeef0001 fff8000000000000 fff8deadbeef0001 / 7fc00000 7fc00123 ffc00000 ffc00123 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | - / - | - / - | 0 / 0 |
| u_ctas | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_delete_cow | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_delete_mor | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_insert | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_insert_overwrite | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_insertinto | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_merge_insert | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_merge_update_cow | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_merge_update_mor | 7ff8000000000000 / 7fc00000 | 4000000000000000 7ff8000000000000 / 40000000 7fc00000 | NO | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_path_append | refused: [TABLE_OR_VIEW_NOT_FOUND] The table or view `<scratch>/whh/doors/ns`.u_path_append cannot  | | | | | | | |
| u_rtas | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_save_name | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_saveastable_append | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_saveastable_overwrite | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_update_cow | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_update_mor | 7ff8000000000000 / 7fc00000 | 3ff8000000000000 7ff0000000000000 7ff8000000000000 bff0000000000000 / 3fc00000 7f800000 7fc00000 bf800000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 9 / 9 |
| u_writeto_append | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_create | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_create_or_replace | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_overwrite | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |
| u_writeto_overwrite_partitions | 7ff8000000000000 / 7fc00000 | 7ff8000000000000 / 7fc00000 | yes | - / - | 0 / 0 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 1,2,3,4,5,6,7,8 / 1,2,3,4,5,6,7,8 | 8 / 8 |

| native table | head NaN bits d / f | controls d | outcome |
|---|---|---|---|
| u_native_ctas | - / - | {9: None, 10: None, 11: None, 12: None, 13: None, 14: None} | error repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table nat.ns.u_n |
| u_native_delete | 7ff8000000000000 / 7fc00000 | {9: '8000000000000000', 10: '0000000000000000', 11: '7ff0000000000000', 12: 'fff0000000000000', 13: None, 14: None} | ok  |
| u_native_insert | 7ff8000000000000 / 7fc00000 | {9: '8000000000000000', 10: '0000000000000000', 11: '7ff0000000000000', 12: 'fff0000000000000', 13: None, 14: '3ff8000000000000'} | ok  |
| u_native_insert_partitioned | - / - | {9: None, 10: None, 11: None, 12: None, 13: None, 14: None} | error This feature is not implemented: Hive distribution not supported: PARTITIONED { columns: [ |
| u_native_merge | - / - | {9: None, 10: None, 11: None, 12: None, 13: None, 14: None} | error This feature is not implemented: Unsupported SQL statement: MERGE INTO nat.ns.u_native_mer |
| u_native_update | 7ff8000000000000 / 7fc00000 | {9: '8000000000000000', 10: '0000000000000000', 11: '7ff0000000000000', 12: 'fff0000000000000', 13: None, 14: '3ff8000000000000'} | ok  |

- **Stored bits.** Every Iceberg door stores `7ff8000000000000`/`7fc00000` for all eight NaN rows, as Spark does,
  partitioned or not, and so does the native door's INSERT, UPDATE and DELETE. `= CAST('NaN' …)`, `isnan` and the NaN
  counts equal Spark's on every door.
- **No non-NaN value changed.** Rendering the head against main (`--spark raw/repark-1d9d2808.jsonl`) gives
  "controls equal: yes" on all 45 tables that wrote; the native controls are identical. `u_merge_update_mor`'s "NO"
  against Spark is §1's merge-on-read note, unchanged from main.
- **Refusals** are main's: `n_ctas` (the map CTAS), `u_path_append`/`p_path_append`, native CTAS, partitioned
  native create and native MERGE.
- **Nested leaves** (`n_insert`): every leaf `7ff8000000000000`/`7fc00000`, row 9 `8000000000000000`/`80000000`, as
  Spark.
- **Rewrite cell** (`raw/repark-e100717f-rewrite.jsonl`, sources written by main's build): the head's copy-on-write
  `UPDATE` of `u_merge_insert` row 14 writes one file whose NaN rows are all canonical, `-0.0` kept; on `p_insert`
  the rewritten file is canonical and files not rewritten keep their bits. That is Spark's answer in §3.
- **Plain Parquet path write** (§4) still stores the raw bits; it is DataFusion's writer, outside this unit.
