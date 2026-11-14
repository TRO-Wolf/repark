# ZERO-PARTITION-MERGE-1 step 0: the rendered cells

Rendered by `probe/render_step0.py` from [raw/](raw/map.md). Statuses: see [step0.md](step0.md) section 1.2. A count is over the 12 variants (v2/v3 x Parquet/ORC/Avro x `d`/`f`) of a door.

## 1. Status per door and run

| door | repark-main-c1 | repark-main-c2 | repark-main-c10 | repark-main-p16 | repark-main-extra-c1 | repark-main-extra-c2 | repark-1325-c1 | repark-1325-p16 | spark-c1 | spark-c2 | spark-c16 | spark-extra-c2 | spark-reads-repark-main-c1 | repark-main-reads-spark-c2 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| insert_values | MERGED 12 | MERGED 12 | ok 12 | ok 12 | — | — | MERGED 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| insert_values_pos_first | MERGED 12 | MERGED 12 | ok 12 | ok 12 | — | — | MERGED 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| insert_select | MERGED 12 | MERGED 12 | ok 12 | ok 12 | — | — | MERGED 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| insert_overwrite_static | MERGED 6 ok 6 | MERGED 6 ok 6 | MERGED 6 ok 6 | MERGED 6 ok 6 | — | — | MERGED 6 ok 6 | MERGED 6 ok 6 | ok 12 | ok 12 | ok 12 | — | MERGED 6 ok 6 | ok 12 |
| insert_overwrite_dynamic_cross | LOST 12 | LOST 12 | LOST 12 | LOST 12 | — | — | LOST 12 | LOST 12 | ok 12 | ok 12 | ok 12 | — | LOST 12 | ok 12 |
| ctas | MERGED 12 | MERGED 12 | MERGED 12 | MERGED 12 | — | — | MERGED 12 | MERGED 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| rtas | MERGED 12 | MERGED 12 | MERGED 12 | MERGED 12 | — | — | MERGED 12 | MERGED 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| wt_append | MERGED 12 | MERGED 12 | ok 12 | ok 12 | — | — | MERGED 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| wt_create | MERGED 12 | MERGED 12 | MERGED 12 | MERGED 12 | — | — | MERGED 12 | MERGED 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| wt_create_or_replace | MERGED 12 | MERGED 12 | MERGED 12 | MERGED 12 | — | — | MERGED 12 | MERGED 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| wt_overwrite_partitions | MERGED 6 ok 6 | MERGED 6 ok 6 | MERGED 6 ok 6 | MERGED 6 ok 6 | — | — | MERGED 6 ok 6 | MERGED 6 ok 6 | ok 12 | ok 12 | ok 12 | — | MERGED 6 ok 6 | ok 12 |
| wt_overwrite_partitions_cross | LOST 12 | LOST 12 | LOST 12 | LOST 12 | — | — | LOST 12 | LOST 12 | ok 12 | ok 12 | ok 12 | — | LOST 12 | ok 12 |
| sat_append | MERGED 12 | MERGED 12 | ok 12 | ok 12 | — | — | MERGED 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| sat_overwrite | MERGED 6 ok 6 | MERGED 6 ok 6 | MERGED 6 ok 6 | MERGED 6 ok 6 | — | — | MERGED 6 ok 6 | MERGED 6 ok 6 | ok 12 | ok 12 | ok 12 | — | MERGED 6 ok 6 | ok 12 |
| merge_insert | MERGED 9 ok 3 | MERGED 9 ok 3 | MERGED 9 ok 3 | MERGED 9 ok 3 | — | — | MERGED 9 ok 3 | MERGED 9 ok 3 | ok 12 | ok 12 | ok 12 | — | MERGED 9 ok 3 | ok 12 |
| merge_update_cow | MERGED 9 ok 3 | MERGED 9 ok 3 | MERGED 9 ok 3 | MERGED 9 ok 3 | — | — | MERGED 9 ok 3 | MERGED 9 ok 3 | ok 12 | ok 12 | ok 12 | — | MERGED 9 ok 3 | ok 12 |
| merge_update_mor | MIXED 9 ok 3 | MIXED 9 ok 3 | MIXED 9 ok 3 | MIXED 9 ok 3 | — | — | MIXED 9 ok 3 | MIXED 9 ok 3 | ok 12 | ok 12 | ok 12 | — | MIXED 9 ok 3 | ok 12 |
| update_cow | MERGED 12 | MERGED 3 error 9 | MERGED 3 error 9 | MERGED 4 error 8 | — | — | MERGED 12 | MERGED 5 error 7 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| update_mor | MIXED 9 ok 3 | MIXED 9 ok 3 | MIXED 9 ok 3 | MIXED 9 ok 3 | — | — | MIXED 9 ok 3 | MIXED 9 ok 3 | ok 12 | ok 12 | ok 12 | — | MIXED 9 ok 3 | ok 12 |
| delete_cow | MERGED 9 ok 3 | MERGED 9 ok 3 | MERGED 9 ok 3 | MERGED 9 ok 3 | — | — | MERGED 9 ok 3 | MERGED 9 ok 3 | ok 12 | ok 12 | ok 12 | — | MERGED 9 ok 3 | ok 12 |
| delete_mor | ok 12 | ok 12 | ok 12 | ok 12 | — | — | ok 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | ok 12 | ok 12 |
| delete_mor_partition_granularity | DEL-MERGED 6 ok 6 | DEL-MERGED 6 ok 6 | DEL-MERGED 6 ok 6 | DEL-MERGED 6 ok 6 | — | — | DEL-MERGED 6 ok 6 | DEL-MERGED 6 ok 6 | ok 12 | ok 12 | ok 12 | — | MIXED 6 ok 6 | ok 12 |
| insert_overwrite_static_partition_cross | — | — | — | — | error 12 | error 12 | error 12 | error 12 | — | — | ok 12 | ok 12 | — | — |
| wt_overwrite_filter_cross | — | — | — | — | error 12 | error 12 | error 12 | error 12 | — | — | ok 12 | ok 12 | — | — |
| rewrite_data_files | MERGED 12 | MERGED 12 | MERGED 12 | MERGED 12 | — | — | MERGED 12 | MERGED 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| branch_insert | MERGED 12 | MERGED 12 | ok 12 | ok 12 | — | — | MERGED 12 | ok 12 | ok 12 | ok 12 | ok 12 | — | MERGED 12 | ok 12 |
| native_insert | error 12 | error 12 | — | — | — | — | — | — | — | — | — | — | — | — |

## 2. Every cell that is not ok

| run | table | status | zero layout (data, deletes); rows read wrong (id:d/f bits) |
|---|---|---|---|
| repark-main-c1 | insert_values_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_values_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_values_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_values_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_values_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_values_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_values_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_values_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_values_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_values_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_values_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_values_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_values_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_values_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_values_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_values_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_values_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_values_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_values_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_values_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_values_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_values_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_values_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_values_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c1 | insert_values_pos_first_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c1 | insert_values_pos_first_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c1 | insert_values_pos_first_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c1 | insert_values_pos_first_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c1 | insert_values_pos_first_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c1 | insert_values_pos_first_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c2 | insert_values_pos_first_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c2 | insert_values_pos_first_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c2 | insert_values_pos_first_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c2 | insert_values_pos_first_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c2 | insert_values_pos_first_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c2 | insert_values_pos_first_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c2 | insert_values_pos_first_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c2 | insert_values_pos_first_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c2 | insert_values_pos_first_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c2 | insert_values_pos_first_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c2 | insert_values_pos_first_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-main-c2 | insert_values_pos_first_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_pos_first_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_pos_first_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_pos_first_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_pos_first_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_pos_first_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_pos_first_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_pos_first_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_pos_first_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_pos_first_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_pos_first_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-1325-c1 | insert_values_pos_first_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| repark-1325-c1 | insert_values_pos_first_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v2_orc_d | MERGED | +0:4; 1:0000000000000000/00000000, 6:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v2_orc_f | MERGED | +0:4; 1:0000000000000000/00000000, 6:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v3_orc_d | MERGED | +0:4; 1:0000000000000000/00000000, 6:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v3_orc_f | MERGED | +0:4; 1:0000000000000000/00000000, 6:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 6:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_values_pos_first_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 6:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_select_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_select_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_select_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_select_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_select_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | insert_select_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_select_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_select_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_select_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_select_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_select_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_select_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_select_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_select_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_select_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_select_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_select_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | insert_select_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_select_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_select_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_select_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_select_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_select_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_select_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_select_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_select_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_select_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_select_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_select_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | insert_select_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_select_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_select_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_select_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_select_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | insert_select_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_overwrite_static_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_overwrite_static_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_overwrite_static_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_overwrite_static_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_overwrite_static_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | insert_overwrite_static_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c1 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c2 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c10 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-p16 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v2_avro_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v2_avro_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v2_orc_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v2_orc_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v3_avro_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v3_avro_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v3_orc_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v3_orc_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | insert_overwrite_dynamic_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c1 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | ctas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | ctas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | ctas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | ctas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | ctas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | ctas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | ctas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | ctas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | ctas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | ctas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | ctas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | ctas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | ctas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | ctas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | ctas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | ctas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | ctas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | rtas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | rtas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | rtas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | rtas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | rtas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | rtas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | rtas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | rtas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | rtas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | rtas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | rtas_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | rtas_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rtas_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rtas_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rtas_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | rtas_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rtas_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_append_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_append_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_append_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_append_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_append_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_append_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | wt_create_or_replace_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c1 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c2 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-c10 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-main-p16 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-c1 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| repark-1325-p16 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v2_avro_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v2_avro_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v2_orc_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v2_orc_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v2_parquet_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v2_parquet_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v3_avro_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v3_avro_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v3_orc_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v3_orc_f | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v3_parquet_d | LOST | +0:1; 10:missing |
| spark-reads-repark-main-c1 | wt_overwrite_partitions_cross_v3_parquet_f | LOST | +0:1; 10:missing |
| repark-main-c1 | sat_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | sat_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | sat_append_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | sat_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | sat_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | sat_append_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | sat_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | sat_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | sat_append_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | sat_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | sat_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | sat_append_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | sat_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | sat_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | sat_append_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | sat_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | sat_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | sat_append_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | sat_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | sat_append_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | sat_append_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | sat_append_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_append_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | sat_append_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_overwrite_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_overwrite_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_overwrite_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_overwrite_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_overwrite_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | sat_overwrite_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | merge_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | merge_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | merge_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | merge_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | merge_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c10 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c10 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | merge_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | merge_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-p16 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-p16 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | merge_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | merge_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | merge_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | merge_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-p16 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-p16 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | merge_update_cow_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_cow_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_cow_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c1 | merge_update_cow_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_cow_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c1 | merge_update_cow_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_cow_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c1 | merge_update_cow_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c2 | merge_update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_cow_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_cow_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | merge_update_cow_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c2 | merge_update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | merge_update_cow_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_cow_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | merge_update_cow_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c10 | merge_update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c10 | merge_update_cow_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c10 | merge_update_cow_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | merge_update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | merge_update_cow_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | merge_update_cow_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | merge_update_cow_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | merge_update_cow_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_cow_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-p16 | merge_update_cow_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_cow_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_cow_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_cow_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-c1 | merge_update_cow_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_cow_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-c1 | merge_update_cow_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_cow_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-c1 | merge_update_cow_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_cow_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_cow_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_cow_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_cow_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_cow_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_cow_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_cow_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 8:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 8:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 8:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_update_cow_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_mor_v2_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_mor_v2_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c1 | merge_update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_mor_v3_orc_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c1 | merge_update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c1 | merge_update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c1 | merge_update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c2 | merge_update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c2 | merge_update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c2 | merge_update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | merge_update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c2 | merge_update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c2 | merge_update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c2 | merge_update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c10 | merge_update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c10 | merge_update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | merge_update_mor_v3_orc_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c10 | merge_update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c10 | merge_update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c10 | merge_update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v2_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v2_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-p16 | merge_update_mor_v3_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-p16 | merge_update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-p16 | merge_update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_mor_v2_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_mor_v2_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-1325-c1 | merge_update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_mor_v3_orc_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-1325-c1 | merge_update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | merge_update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-1325-c1 | merge_update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_mor_v3_orc_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-1325-p16 | merge_update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-p16 | merge_update_mor_v3_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v2_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v2_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v3_orc_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | merge_update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c1 | update_cow_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | update_cow_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | update_cow_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | update_cow_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c1 | update_cow_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | update_cow_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | update_cow_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | update_cow_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | update_cow_v2_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v2_avro_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | update_cow_v2_orc_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v2_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | update_cow_v3_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v3_avro_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | update_cow_v3_orc_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v3_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c2 | update_cow_v3_parquet_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v2_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v2_avro_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-main-c10 | update_cow_v2_orc_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v2_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v2_parquet_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v3_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v3_avro_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | update_cow_v3_orc_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v3_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-c10 | update_cow_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | update_cow_v2_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v2_avro_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | update_cow_v2_orc_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | update_cow_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-p16 | update_cow_v3_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | update_cow_v3_orc_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v3_orc_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v3_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-main-p16 | update_cow_v3_parquet_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-c1 | update_cow_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | update_cow_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | update_cow_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | update_cow_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-c1 | update_cow_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | update_cow_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-c1 | update_cow_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | update_cow_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-p16 | update_cow_v2_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-p16 | update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | update_cow_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-p16 | update_cow_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | update_cow_v2_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-p16 | update_cow_v2_parquet_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-p16 | update_cow_v3_avro_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-p16 | update_cow_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | update_cow_v3_orc_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-p16 | update_cow_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | update_cow_v3_parquet_d | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| repark-1325-p16 | update_cow_v3_parquet_f | error | AnalysisException: Error during planning: Mismatch between schema and batches |
| spark-reads-repark-main-c1 | update_cow_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | update_cow_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v2_orc_d | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v2_orc_f | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | update_cow_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v3_orc_d | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v3_orc_f | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_cow_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | update_cow_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c1 | update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c1 | update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c1 | update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c1 | update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c1 | update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c1 | update_mor_v3_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c1 | update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c1 | update_mor_v3_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c2 | update_mor_v3_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c2 | update_mor_v3_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | update_mor_v2_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c10 | update_mor_v3_avro_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c10 | update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c10 | update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-c10 | update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-c10 | update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-c10 | update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | update_mor_v2_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-p16 | update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-main-p16 | update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | update_mor_v3_orc_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-p16 | update_mor_v3_orc_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-main-p16 | update_mor_v3_parquet_d | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:8000000000000000/00000000 |
| repark-main-p16 | update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-c1 | update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-c1 | update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-c1 | update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-c1 | update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-c1 | update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-c1 | update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-c1 | update_mor_v3_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-c1 | update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-c1 | update_mor_v3_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-p16 | update_mor_v2_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-p16 | update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-p16 | update_mor_v2_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-p16 | update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-p16 | update_mor_v3_avro_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| repark-1325-p16 | update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-p16 | update_mor_v3_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-1325-p16 | update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| repark-1325-p16 | update_mor_v3_parquet_f | MIXED | -0:4 +0:2 del -0:1 +0:1; 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | update_mor_v2_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_mor_v2_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_mor_v2_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_mor_v3_avro_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | update_mor_v3_avro_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_mor_v3_orc_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_mor_v3_orc_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | update_mor_v3_parquet_d | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | update_mor_v3_parquet_f | MIXED | -0:2 +0:4 del -0:1 +0:1; 7:8000000000000000/00000000 |
| repark-main-c1 | delete_cow_v2_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c1 | delete_cow_v2_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c1 | delete_cow_v2_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c1 | delete_cow_v3_avro_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c1 | delete_cow_v3_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c1 | delete_cow_v3_orc_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c1 | delete_cow_v3_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c1 | delete_cow_v3_parquet_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c1 | delete_cow_v3_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c2 | delete_cow_v2_avro_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-c2 | delete_cow_v2_orc_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-c2 | delete_cow_v2_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c2 | delete_cow_v3_avro_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-main-c2 | delete_cow_v3_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c2 | delete_cow_v3_orc_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c2 | delete_cow_v3_orc_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-c2 | delete_cow_v3_parquet_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c2 | delete_cow_v3_parquet_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-c10 | delete_cow_v2_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c10 | delete_cow_v2_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c10 | delete_cow_v2_parquet_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-c10 | delete_cow_v3_avro_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c10 | delete_cow_v3_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c10 | delete_cow_v3_orc_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-main-c10 | delete_cow_v3_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c10 | delete_cow_v3_parquet_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-main-c10 | delete_cow_v3_parquet_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-p16 | delete_cow_v2_avro_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-p16 | delete_cow_v2_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-p16 | delete_cow_v2_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-p16 | delete_cow_v3_avro_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-main-p16 | delete_cow_v3_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-p16 | delete_cow_v3_orc_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-main-p16 | delete_cow_v3_orc_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-main-p16 | delete_cow_v3_parquet_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-main-p16 | delete_cow_v3_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-c1 | delete_cow_v2_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-c1 | delete_cow_v2_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-c1 | delete_cow_v2_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-c1 | delete_cow_v3_avro_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-1325-c1 | delete_cow_v3_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-c1 | delete_cow_v3_orc_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-1325-c1 | delete_cow_v3_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-c1 | delete_cow_v3_parquet_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| repark-1325-c1 | delete_cow_v3_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-p16 | delete_cow_v2_avro_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-1325-p16 | delete_cow_v2_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-p16 | delete_cow_v2_parquet_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-1325-p16 | delete_cow_v3_avro_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-1325-p16 | delete_cow_v3_avro_f | MERGED | +0:2; 1:8000000000000000/00000000 |
| repark-1325-p16 | delete_cow_v3_orc_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-1325-p16 | delete_cow_v3_orc_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-1325-p16 | delete_cow_v3_parquet_d | MERGED | +0:2; 1:0000000000000000/80000000 |
| repark-1325-p16 | delete_cow_v3_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v2_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v2_orc_f | MERGED | -0:2; 2:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v2_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v3_avro_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | delete_cow_v3_avro_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v3_orc_d | MERGED | -0:2; 2:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v3_orc_f | MERGED | -0:2; 2:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | delete_cow_v3_parquet_d | MERGED | -0:2; 2:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | delete_cow_v3_parquet_f | MERGED | -0:2; 2:0000000000000000/80000000 |
| repark-main-c1 | delete_mor_partition_granularity_v2_avro_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c1 | delete_mor_partition_granularity_v2_avro_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c1 | delete_mor_partition_granularity_v2_orc_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c1 | delete_mor_partition_granularity_v2_orc_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c1 | delete_mor_partition_granularity_v2_parquet_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c1 | delete_mor_partition_granularity_v2_parquet_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c2 | delete_mor_partition_granularity_v2_avro_d | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-c2 | delete_mor_partition_granularity_v2_avro_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c2 | delete_mor_partition_granularity_v2_orc_d | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-c2 | delete_mor_partition_granularity_v2_orc_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c2 | delete_mor_partition_granularity_v2_parquet_d | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-c2 | delete_mor_partition_granularity_v2_parquet_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c10 | delete_mor_partition_granularity_v2_avro_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c10 | delete_mor_partition_granularity_v2_avro_f | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-c10 | delete_mor_partition_granularity_v2_orc_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c10 | delete_mor_partition_granularity_v2_orc_f | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-c10 | delete_mor_partition_granularity_v2_parquet_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-c10 | delete_mor_partition_granularity_v2_parquet_f | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-p16 | delete_mor_partition_granularity_v2_avro_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-p16 | delete_mor_partition_granularity_v2_avro_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-p16 | delete_mor_partition_granularity_v2_orc_d | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-p16 | delete_mor_partition_granularity_v2_orc_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-main-p16 | delete_mor_partition_granularity_v2_parquet_d | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-main-p16 | delete_mor_partition_granularity_v2_parquet_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-c1 | delete_mor_partition_granularity_v2_avro_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-c1 | delete_mor_partition_granularity_v2_avro_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-c1 | delete_mor_partition_granularity_v2_orc_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-c1 | delete_mor_partition_granularity_v2_orc_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-c1 | delete_mor_partition_granularity_v2_parquet_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-c1 | delete_mor_partition_granularity_v2_parquet_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-p16 | delete_mor_partition_granularity_v2_avro_d | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-1325-p16 | delete_mor_partition_granularity_v2_avro_f | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| repark-1325-p16 | delete_mor_partition_granularity_v2_orc_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-p16 | delete_mor_partition_granularity_v2_orc_f | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-p16 | delete_mor_partition_granularity_v2_parquet_d | DEL-MERGED | -0:2 +0:2 del +0:2; rows ok |
| repark-1325-p16 | delete_mor_partition_granularity_v2_parquet_f | DEL-MERGED | -0:2 +0:2 del -0:2; rows ok |
| spark-reads-repark-main-c1 | delete_mor_partition_granularity_v2_avro_d | MIXED | -0:2 +0:2 del +0:2; 7:extra |
| spark-reads-repark-main-c1 | delete_mor_partition_granularity_v2_avro_f | MIXED | -0:2 +0:2 del +0:2; 7:extra |
| spark-reads-repark-main-c1 | delete_mor_partition_granularity_v2_orc_d | MIXED | -0:2 +0:2 del +0:2; 7:extra |
| spark-reads-repark-main-c1 | delete_mor_partition_granularity_v2_orc_f | MIXED | -0:2 +0:2 del +0:2; 7:extra |
| spark-reads-repark-main-c1 | delete_mor_partition_granularity_v2_parquet_d | MIXED | -0:2 +0:2 del +0:2; 7:extra |
| spark-reads-repark-main-c1 | delete_mor_partition_granularity_v2_parquet_f | MIXED | -0:2 +0:2 del +0:2; 7:extra |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v2_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v2_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v2_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v2_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v2_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v2_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v3_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v3_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v3_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v3_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v3_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | insert_overwrite_static_partition_cross_v3_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v2_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v2_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v2_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v2_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v2_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v2_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v3_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v3_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v3_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v3_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v3_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c2 | insert_overwrite_static_partition_cross_v3_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v2_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v2_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v2_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v2_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v2_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v2_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v3_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v3_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v3_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v3_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v3_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-c1 | insert_overwrite_static_partition_cross_v3_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v2_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v2_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v2_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v2_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v2_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v2_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v3_avro_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v3_avro_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v3_orc_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v3_orc_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v3_parquet_d | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-1325-p16 | insert_overwrite_static_partition_cross_v3_parquet_f | error | AnalysisException: Error during planning: INSERT OVERWRITE PARTITION numeric literal `0.0` is not an integer |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v2_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v2_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v2_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v2_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v2_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v2_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v3_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v3_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v3_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v3_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v3_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c1 | wt_overwrite_filter_cross_v3_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v2_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v2_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v2_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v2_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v2_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v2_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v3_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v3_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v3_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v3_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v3_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-main-extra-c2 | wt_overwrite_filter_cross_v3_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-c1 | wt_overwrite_filter_cross_v2_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-c1 | wt_overwrite_filter_cross_v2_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-c1 | wt_overwrite_filter_cross_v2_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-c1 | wt_overwrite_filter_cross_v2_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-c1 | wt_overwrite_filter_cross_v2_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-c1 | wt_overwrite_filter_cross_v2_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-c1 | wt_overwrite_filter_cross_v3_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-c1 | wt_overwrite_filter_cross_v3_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-c1 | wt_overwrite_filter_cross_v3_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-c1 | wt_overwrite_filter_cross_v3_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-c1 | wt_overwrite_filter_cross_v3_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-c1 | wt_overwrite_filter_cross_v3_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-p16 | wt_overwrite_filter_cross_v2_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-p16 | wt_overwrite_filter_cross_v2_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-p16 | wt_overwrite_filter_cross_v2_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-p16 | wt_overwrite_filter_cross_v2_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-p16 | wt_overwrite_filter_cross_v2_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-p16 | wt_overwrite_filter_cross_v2_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-p16 | wt_overwrite_filter_cross_v3_avro_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-p16 | wt_overwrite_filter_cross_v3_avro_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-p16 | wt_overwrite_filter_cross_v3_orc_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-p16 | wt_overwrite_filter_cross_v3_orc_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-1325-p16 | wt_overwrite_filter_cross_v3_parquet_d | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `d` = CAST(0.0 AS DOUBLE) |
| repark-1325-p16 | wt_overwrite_filter_cross_v3_parquet_f | error | IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression: `f` = CAST(0.0 AS FLOAT) |
| repark-main-c1 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | rewrite_data_files_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | rewrite_data_files_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | rewrite_data_files_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | rewrite_data_files_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | rewrite_data_files_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | rewrite_data_files_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c1 | rewrite_data_files_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | rewrite_data_files_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | rewrite_data_files_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c2 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c2 | rewrite_data_files_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | rewrite_data_files_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | rewrite_data_files_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | rewrite_data_files_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | rewrite_data_files_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | rewrite_data_files_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | rewrite_data_files_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c10 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-c10 | rewrite_data_files_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | rewrite_data_files_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | rewrite_data_files_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-p16 | rewrite_data_files_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-main-p16 | rewrite_data_files_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | rewrite_data_files_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | rewrite_data_files_v2_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | rewrite_data_files_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | rewrite_data_files_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | rewrite_data_files_v3_orc_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | rewrite_data_files_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-c1 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-c1 | rewrite_data_files_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-p16 | rewrite_data_files_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-p16 | rewrite_data_files_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-p16 | rewrite_data_files_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| repark-1325-p16 | rewrite_data_files_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 8:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v3_orc_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-1325-p16 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| repark-1325-p16 | rewrite_data_files_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 8:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v2_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v2_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v2_orc_d | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v2_orc_f | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v2_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v2_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v3_avro_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v3_avro_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v3_orc_d | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v3_orc_f | MERGED | +0:4; 1:0000000000000000/00000000, 7:0000000000000000/00000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v3_parquet_d | MERGED | +0:4; 1:0000000000000000/80000000, 7:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | rewrite_data_files_v3_parquet_f | MERGED | +0:4; 1:8000000000000000/00000000, 7:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | branch_insert_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | branch_insert_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | branch_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | branch_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | branch_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c1 | branch_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | branch_insert_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | branch_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | branch_insert_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | branch_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | branch_insert_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | branch_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | branch_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | branch_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | branch_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | branch_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c2 | branch_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-main-c2 | branch_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | branch_insert_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | branch_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | branch_insert_v2_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | branch_insert_v2_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | branch_insert_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | branch_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | branch_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | branch_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | branch_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | branch_insert_v3_orc_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-1325-c1 | branch_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| repark-1325-c1 | branch_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v2_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | branch_insert_v2_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v2_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v2_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v2_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | branch_insert_v2_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v3_avro_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | branch_insert_v3_avro_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v3_orc_d | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v3_orc_f | MERGED | -0:4; 2:8000000000000000/80000000, 5:8000000000000000/80000000 |
| spark-reads-repark-main-c1 | branch_insert_v3_parquet_d | MERGED | -0:4; 2:8000000000000000/00000000, 5:8000000000000000/00000000 |
| spark-reads-repark-main-c1 | branch_insert_v3_parquet_f | MERGED | -0:4; 2:0000000000000000/80000000, 5:0000000000000000/80000000 |
| repark-main-c1 | native_insert_v2_avro_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_avro_d' not found |
| repark-main-c1 | native_insert_v2_avro_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_avro_f' not found |
| repark-main-c1 | native_insert_v2_orc_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_orc_d' not found |
| repark-main-c1 | native_insert_v2_orc_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_orc_f' not found |
| repark-main-c1 | native_insert_v2_parquet_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_parquet_d' not found |
| repark-main-c1 | native_insert_v2_parquet_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_parquet_f' not found |
| repark-main-c1 | native_insert_v3_avro_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_avro_d' not found |
| repark-main-c1 | native_insert_v3_avro_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_avro_f' not found |
| repark-main-c1 | native_insert_v3_orc_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_orc_d' not found |
| repark-main-c1 | native_insert_v3_orc_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_orc_f' not found |
| repark-main-c1 | native_insert_v3_parquet_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_parquet_d' not found |
| repark-main-c1 | native_insert_v3_parquet_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_parquet_f' not found |
| repark-main-c2 | native_insert_v2_avro_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_avro_d' not found |
| repark-main-c2 | native_insert_v2_avro_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_avro_f' not found |
| repark-main-c2 | native_insert_v2_orc_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_orc_d' not found |
| repark-main-c2 | native_insert_v2_orc_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_orc_f' not found |
| repark-main-c2 | native_insert_v2_parquet_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_parquet_d' not found |
| repark-main-c2 | native_insert_v2_parquet_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_parquet_f' not found |
| repark-main-c2 | native_insert_v3_avro_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_avro_d' not found |
| repark-main-c2 | native_insert_v3_avro_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_avro_f' not found |
| repark-main-c2 | native_insert_v3_orc_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_orc_d' not found |
| repark-main-c2 | native_insert_v3_orc_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_orc_f' not found |
| repark-main-c2 | native_insert_v3_parquet_d | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_parquet_d' not found |
| repark-main-c2 | native_insert_v3_parquet_f | error | AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_parquet_f' not found |

## 3. Layouts, v2 Parquet DOUBLE (`_v2_parquet_d`)

| door | run | files: content partition rows | rows read: id d f |
|---|---|---|---|
| insert_values | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values_pos_first | repark-main-c1 | data +0.0 4; data 1.5 1; data NULL 1 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 +0.0 -0.0f |
| insert_values_pos_first | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values_pos_first | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values_pos_first | spark-reads-repark-main-c1 | data +0.0 4; data 1.5 1; data NULL 1 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 +0.0 -0.0f |
| insert_select | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| insert_select | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_select | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_select | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | repark-main-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_dynamic_cross | repark-main-c1 | data +0.0 1; data 1.5 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| insert_overwrite_dynamic_cross | repark-main-p16 | data +0.0 1; data 1.5 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| insert_overwrite_dynamic_cross | spark-c1 | data +0.0 1; data 1.5 1; data -0.0 1 | 1 +0.0 +0.0f; 10 -0.0 -0.0f; 11 1.5 1.5f |
| insert_overwrite_dynamic_cross | spark-reads-repark-main-c1 | data +0.0 1; data 1.5 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| ctas | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| ctas | repark-main-p16 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| ctas | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| ctas | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| rtas | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| rtas | repark-main-p16 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| rtas | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| rtas | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_append | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_append | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_append | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_append | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create | repark-main-p16 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | repark-main-p16 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| wt_overwrite_partitions | repark-main-c1 | data +0.0 2; data 1.5 1; data 2.0 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions | repark-main-p16 | data +0.0 2; data 1.5 1; data 2.0 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions | spark-c1 | data +0.0 2; data 1.5 1; data 2.0 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 1; data 2.0 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions_cross | repark-main-c1 | data +0.0 1; data 1.5 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| wt_overwrite_partitions_cross | repark-main-p16 | data +0.0 1; data 1.5 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| wt_overwrite_partitions_cross | spark-c1 | data +0.0 1; data 1.5 1; data -0.0 1 | 1 +0.0 +0.0f; 10 -0.0 -0.0f; 11 1.5 1.5f |
| wt_overwrite_partitions_cross | spark-reads-repark-main-c1 | data +0.0 1; data 1.5 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| sat_append | repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| sat_append | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_append | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_append | spark-reads-repark-main-c1 | data 1.5 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f |
| sat_overwrite | repark-main-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_overwrite | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_overwrite | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_overwrite | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_insert | repark-main-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_insert | repark-main-p16 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_insert | spark-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_insert | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_update_cow | repark-main-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_cow | repark-main-p16 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_cow | spark-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_cow | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | repark-main-c1 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | repark-main-p16 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | spark-c1 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | spark-reads-repark-main-c1 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_cow | repark-main-c1 | data +0.0 4; data 1.5 2; data NULL 2 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 +0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_cow | repark-main-p16 | data +0.0 4; data 1.5 2; data NULL 2 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 +0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_cow | spark-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_cow | spark-reads-repark-main-c1 | data +0.0 4; data 1.5 2; data NULL 2 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 +0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | repark-main-c1 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | repark-main-p16 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | spark-c1 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | spark-reads-repark-main-c1 | data +0.0 1; data +0.0 2; data 1.5 1; data 1.5 2; data -0.0 1; data -0.0 2; data NULL 1; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| delete_cow | repark-main-c1 | data +0.0 1; data 1.5 1; data -0.0 1; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_cow | repark-main-p16 | data +0.0 1; data 1.5 1; data -0.0 1; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_cow | spark-c1 | data +0.0 1; data 1.5 1; data -0.0 1; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_cow | spark-reads-repark-main-c1 | data +0.0 1; data 1.5 1; data -0.0 1; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | repark-main-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | repark-main-p16 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | spark-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | repark-main-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 2; del 1.5 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | repark-main-p16 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del 1.5 1; del -0.0 2; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | spark-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 1; del 1.5 1; del -0.0 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | spark-reads-repark-main-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2; del +0.0 2; del 1.5 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f |
| rewrite_data_files | repark-main-c1 | data +0.0 4; data 1.5 2; data NULL 2 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 +0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| rewrite_data_files | repark-main-p16 | data +0.0 4; data 1.5 2; data NULL 2 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 +0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| rewrite_data_files | spark-c1 | data +0.0 2; data 1.5 2; data -0.0 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| rewrite_data_files | spark-reads-repark-main-c1 | data +0.0 4; data 1.5 2; data NULL 2 | 1 +0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 +0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| branch_insert | repark-main-c1 | data 1.5 1; data 2.0 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| branch_insert | repark-main-p16 | data +0.0 2; data 1.5 1; data 2.0 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| branch_insert | spark-c1 | data +0.0 2; data 1.5 1; data 2.0 1; data -0.0 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| branch_insert | spark-reads-repark-main-c1 | data 1.5 1; data 2.0 1; data -0.0 4; data NULL 1 | 1 -0.0 -0.0f; 2 -0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 -0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| native_insert | repark-main-c1 | error: AnalysisException: Error during planning: table 'hc.ns.native_insert_v2_parquet_d' not fou | |

## 4. Layouts, v3 Parquet FLOAT (`_v3_parquet_f`)

| door | run | files: content partition rows | rows read: id d f |
|---|---|---|---|
| insert_values | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| insert_values | repark-main-p16 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values_pos_first | repark-main-c1 | data +0.0f 4; data 1.5f 1; data NULL 1 | 1 -0.0 +0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 +0.0f |
| insert_values_pos_first | repark-main-p16 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_values_pos_first | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_select | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| insert_select | repark-main-p16 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_select | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| insert_overwrite_static | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| insert_overwrite_dynamic_cross | repark-main-c1 | data +0.0f 1; data 1.5f 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| insert_overwrite_dynamic_cross | repark-main-p16 | data +0.0f 1; data 1.5f 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| insert_overwrite_dynamic_cross | spark-c1 | data +0.0f 1; data 1.5f 1; data -0.0f 1 | 1 +0.0 +0.0f; 10 -0.0 -0.0f; 11 1.5 1.5f |
| ctas | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| ctas | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| ctas | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| rtas | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| rtas | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| rtas | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_append | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| wt_append | repark-main-p16 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_append | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| wt_create | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| wt_create | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| wt_create_or_replace | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| wt_overwrite_partitions | repark-main-c1 | data 1.5f 1; data 2.0f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions | repark-main-p16 | data 1.5f 1; data 2.0f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions | spark-c1 | data +0.0f 2; data 1.5f 1; data 2.0f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| wt_overwrite_partitions_cross | repark-main-c1 | data +0.0f 1; data 1.5f 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| wt_overwrite_partitions_cross | repark-main-p16 | data +0.0f 1; data 1.5f 1 | 1 +0.0 +0.0f; 11 1.5 1.5f |
| wt_overwrite_partitions_cross | spark-c1 | data +0.0f 1; data 1.5f 1; data -0.0f 1 | 1 +0.0 +0.0f; 10 -0.0 -0.0f; 11 1.5 1.5f |
| sat_append | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| sat_append | repark-main-p16 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_append | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| sat_overwrite | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| sat_overwrite | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| sat_overwrite | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_insert | repark-main-c1 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| merge_insert | repark-main-p16 | data 1.5f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f |
| merge_insert | spark-c1 | data +0.0f 2; data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f |
| merge_update_cow | repark-main-c1 | data 1.5f 2; data -0.0f 4; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 -0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_cow | repark-main-p16 | data 1.5f 2; data -0.0f 4; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 -0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_cow | spark-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | repark-main-c1 | data +0.0f 2; data 1.5f 1; data 1.5f 2; data -0.0f 2; data -0.0f 2; data NULL 1; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 -0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | repark-main-p16 | data +0.0f 2; data 1.5f 1; data 1.5f 2; data -0.0f 2; data -0.0f 2; data NULL 1; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 -0.0f; 9 1.5 1.5f; 10 NULL NULL |
| merge_update_mor | spark-c1 | data +0.0f 1; data +0.0f 2; data 1.5f 1; data 1.5f 2; data -0.0f 1; data -0.0f 2; data NULL 1; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_cow | repark-main-c1 | data +0.0f 4; data 1.5f 2; data NULL 2 | 1 -0.0 +0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 +0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_cow | repark-main-p16 | error: AnalysisException: Error during planning: Mismatch between schema and batches | |
| update_cow | spark-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | repark-main-c1 | data +0.0f 2; data +0.0f 2; data 1.5f 1; data 1.5f 2; data -0.0f 2; data NULL 1; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 +0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | repark-main-p16 | data +0.0f 2; data 1.5f 1; data 1.5f 2; data -0.0f 2; data -0.0f 2; data NULL 1; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 -0.0f; 9 1.5 1.5f; 10 NULL NULL |
| update_mor | spark-c1 | data +0.0f 1; data +0.0f 2; data 1.5f 1; data 1.5f 2; data -0.0f 1; data -0.0f 2; data NULL 1; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| delete_cow | repark-main-c1 | data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_cow | repark-main-p16 | data 1.5f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_cow | spark-c1 | data +0.0f 1; data 1.5f 1; data -0.0f 1; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | repark-main-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | repark-main-p16 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor | spark-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | repark-main-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | repark-main-p16 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| delete_mor_partition_granularity | spark-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2; del +0.0f 1; del 1.5f 1; del -0.0f 1; del NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL |
| rewrite_data_files | repark-main-c1 | data +0.0f 4; data 1.5f 2; data NULL 2 | 1 -0.0 +0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 +0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| rewrite_data_files | repark-main-p16 | data +0.0f 4; data 1.5f 2; data NULL 2 | 1 -0.0 +0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 +0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| rewrite_data_files | spark-c1 | data +0.0f 2; data 1.5f 2; data -0.0f 2; data NULL 2 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 7 -0.0 -0.0f; 8 +0.0 +0.0f; 9 1.5 1.5f; 10 NULL NULL |
| branch_insert | repark-main-c1 | data 1.5f 1; data 2.0f 1; data -0.0f 4; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 -0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 -0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| branch_insert | repark-main-p16 | data +0.0f 2; data 1.5f 1; data 2.0f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| branch_insert | spark-c1 | data +0.0f 2; data 1.5f 1; data 2.0f 1; data -0.0f 2; data NULL 1 | 1 -0.0 -0.0f; 2 +0.0 +0.0f; 3 1.5 1.5f; 4 NULL NULL; 5 +0.0 +0.0f; 6 -0.0 -0.0f; 99 2.0 2.0f |
| native_insert | repark-main-c1 | error: AnalysisException: Error during planning: table 'hc.ns.native_insert_v3_parquet_f' not fou | |

## 5. Stored bits

| run | files_read | files_not_read_avro | read_errors | rows_with_changed_stored_bits | files_holding_both_zeros | files_where_partition_value_differs_from_a_stored_zero |
|---|---|---|---|---|---|---|
| disk-repark-main-c1 | 678 | 339 | 0 | 0 | 138 | 138 |
| disk-repark-main-c2 | 663 | 327 | 0 | 0 | 133 | 133 |
| disk-repark-main-c10 | 711 | 351 | 0 | 0 | 85 | 85 |
| disk-repark-main-p16 | 711 | 354 | 0 | 0 | 85 | 85 |
| disk-spark-c1 | 832 | 416 | 0 | 0 | 0 | 0 |
| disk-spark-c2 | 832 | 416 | 0 | 0 | 0 | 0 |
| disk-spark-c16 | 880 | 440 | 0 | 0 | 0 | 0 |
