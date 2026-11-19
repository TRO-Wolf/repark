# Spark 4.1.2 on the revision's cells (critic round 1)

`local[1]` and `local[4]` × whole-stage codegen on (`cg tr`) and off (`cg fa`), 2 runs each, every cell steady. Written by [../r1_spark.py](../r1_spark.py); the last column is [../r1_exact.py](../r1_exact.py) (seat A as revised). `AO` = `ARITHMETIC_OVERFLOW`, `NVOOR` = `NUMERIC_VALUE_OUT_OF_RANGE`; `n:m` = `count(*)`, `count(a)`; `g:v` = group and answer. `m_d256` cells read `TABLE_OR_VIEW_NOT_FOUND` because Spark refuses the file (`view/m_d256`). The ungrouped `big/u/avg_objhash` cell is a probe slip (`big` has no `id` column).

| query | ANSI | local[1] cg tr | local[1] cg fa | local[4] cg tr | local[4] cg fa | exact (seat A) |
|---|---|---|---|---|---|---|
| `big/c/g/avg/count` | false | 3000:0 | 3000:0 | 3000:0 | 3000:0 |  |
| `big/c/u/avg/all` | false | 0.0167 | NULL | NULL | NULL |  |
| `big/g/avg_objhash` | false | 3000:0 | 3000:0 | 3000:0 | 3000:0 |  |
| `big/gsets/avg` | false | rows=3001 nulls=3001 | rows=3001 nulls=3001 | rows=3001 nulls=3001 | rows=3001 nulls=3001 | rows=3001 nulls=0 |
| `big/u/avg_objhash` | false | ERR UNRESOLVED_COLUMN | ERR UNRESOLVED_COLUMN | ERR UNRESOLVED_COLUMN | ERR UNRESOLVED_COLUMN |  |
| `m_d256/g/avg` | false | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | 1:NULL |
| `m_d256/g/try_avg` | false | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | 1:NULL |
| `m_d256/u/avg` | false | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | NULL |
| `m_d256/u/try_avg` | false | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | NULL |
| `m_d256/win/avg` | false | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | 1:NULL |
| `m_d32/g/avg` | false | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/g/avg_M` | false | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 |
| `m_d32/g/try_avg` | false | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/g/try_avg_M` | false | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 |
| `m_d32/type` | false | decimal(13,6) | decimal(13,6) | decimal(13,6) | decimal(13,6) |  |
| `m_d32/u/avg` | false | 0.625 | 0.625 | 0.625 | 0.625 | 0.625 |
| `m_d32/u/avg_M` | false | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 |
| `m_d32/u/try_avg` | false | 0.625 | 0.625 | 0.625 | 0.625 | 0.625 |
| `m_d32/u/try_avg_M` | false | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 |
| `m_d32/win/avg` | false | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/win/avg_M` | false | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 |
| `m_d64/g/avg` | false | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/g/avg_M` | false | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 |
| `m_d64/g/try_avg` | false | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/g/try_avg_M` | false | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 |
| `m_d64/type` | false | decimal(22,4) | decimal(22,4) | decimal(22,4) | decimal(22,4) |  |
| `m_d64/u/avg` | false | 0.2381 | 0.2381 | 0.2381 | 0.2381 | 0.2381 |
| `m_d64/u/avg_M` | false | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 |
| `m_d64/u/try_avg` | false | 0.2381 | 0.2381 | 0.2381 | 0.2381 | 0.2381 |
| `m_d64/u/try_avg_M` | false | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 |
| `m_d64/win/avg` | false | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/win/avg_M` | false | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 |
| `m_mix/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_mix/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_mix/u/avg` | false | NULL | NULL | 8000000000000000000000000000000000 | NULL | NULL |
| `m_mix/u/try_avg` | false | NULL | NULL | 8000000000000000000000000000000000 | NULL | NULL |
| `m_mix/win/avg` | false | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 |
| `m_n33/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_n33/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_n33/u/avg` | false | NULL | NULL | -3000000000000000000000000000000000 | NULL | NULL |
| `m_n33/u/try_avg` | false | NULL | NULL | -3000000000000000000000000000000000 | NULL | NULL |
| `m_n33/win/avg` | false | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 |
| `m_s2/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_s2/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_s2/u/avg` | false | NULL | NULL | 90000000000000000000000000000000 | NULL | NULL |
| `m_s2/u/try_avg` | false | NULL | NULL | 90000000000000000000000000000000 | NULL | NULL |
| `m_s2/win/avg` | false | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 |
| `m_slide/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL |  |
| `m_slide/g/avg_objhash` | false | 1:36 | 1:36 | 1:36 | 1:36 |  |
| `m_w34/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/g/sum` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL |  |
| `m_w34/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/u/avg` | false | NULL | NULL | 9000000000000000000000000000000000 | NULL | NULL |
| `m_w34/u/avg_objhash` | false | NULL:20000 | NULL:20000 | 9000000000000000000000000000000000:20000 | 9000000000000000000000000000000000:20000 |  |
| `m_w34/u/sum` | false | NULL | NULL | NULL | NULL |  |
| `m_w34/u/try_avg` | false | NULL | NULL | 9000000000000000000000000000000000 | NULL | NULL |
| `m_w34/win/avg` | false | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 |
| `m_w34/win/sum` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL |  |
| `m_w8/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w8/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w8/u/avg` | false | NULL | NULL | 9000000000000000000000000000000000 | NULL | NULL |
| `m_w8/u/try_avg` | false | NULL | NULL | 9000000000000000000000000000000000 | NULL | NULL |
| `m_w8/win/avg` | false | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 |
| `t/c38_0/win/avg/g2` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows |
| `t/c38_0/win/avg/g3` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows |
| `t/c38_0/win/avg/g5` | false | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows |
| `t/c38_0/win/sum/g2` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows |  |
| `t/c38_0/win/sum/g3` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows |  |
| `t/c38_0/win/sum/g5` | false | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows |  |
| `t4/corr/avg` | false | 21:NULL 22:NULL 23:1.6667 24:NULL 25:1.6667 | 21:NULL 22:NULL 23:1.6667 24:NULL 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 |
| `t4/g/avg` | false | 21:NULL 22:NULL 23:1.6667 24:NULL 25:1.6667 | 21:NULL 22:NULL 23:1.6667 24:NULL 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 |  |
| `t4/g/avg_objhash` | false | 21:3 22:2 23:3 24:5 25:3 | 21:3 22:2 23:3 24:5 25:3 | 21:3 22:2 23:3 24:5 25:3 | 21:3 22:2 23:3 24:5 25:3 |  |
| `view/m_d256` | false | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION |  |
| `big/c/g/avg/count` | true | AO | AO | AO | AO |  |
| `big/c/u/avg/all` | true | 0.0167 | AO | AO | AO |  |
| `big/g/avg_objhash` | true | AO | AO | AO | AO |  |
| `big/gsets/avg` | true | AO | AO | AO | AO | rows=3001 nulls=0 |
| `big/u/avg_objhash` | true | ERR UNRESOLVED_COLUMN | ERR UNRESOLVED_COLUMN | ERR UNRESOLVED_COLUMN | ERR UNRESOLVED_COLUMN |  |
| `m_d256/g/avg` | true | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | AO |
| `m_d256/g/try_avg` | true | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | 1:NULL |
| `m_d256/u/avg` | true | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | AO |
| `m_d256/u/try_avg` | true | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | NULL |
| `m_d256/win/avg` | true | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | ERR TABLE_OR_VIEW_NOT_FOUND | NVOOR |
| `m_d32/g/avg` | true | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/g/avg_M` | true | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 |
| `m_d32/g/try_avg` | true | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/g/try_avg_M` | true | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 |
| `m_d32/type` | true | decimal(13,6) | decimal(13,6) | decimal(13,6) | decimal(13,6) |  |
| `m_d32/u/avg` | true | 0.625 | 0.625 | 0.625 | 0.625 | 0.625 |
| `m_d32/u/avg_M` | true | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 |
| `m_d32/u/try_avg` | true | 0.625 | 0.625 | 0.625 | 0.625 | 0.625 |
| `m_d32/u/try_avg_M` | true | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 | 9999999.99 |
| `m_d32/win/avg` | true | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/win/avg_M` | true | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 | 1:9999999.99 |
| `m_d64/g/avg` | true | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/g/avg_M` | true | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 |
| `m_d64/g/try_avg` | true | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/g/try_avg_M` | true | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 |
| `m_d64/type` | true | decimal(22,4) | decimal(22,4) | decimal(22,4) | decimal(22,4) |  |
| `m_d64/u/avg` | true | 0.2381 | 0.2381 | 0.2381 | 0.2381 | 0.2381 |
| `m_d64/u/avg_M` | true | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 |
| `m_d64/u/try_avg` | true | 0.2381 | 0.2381 | 0.2381 | 0.2381 | 0.2381 |
| `m_d64/u/try_avg_M` | true | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 | 999999999999999999 |
| `m_d64/win/avg` | true | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/win/avg_M` | true | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 | 1:999999999999999999 |
| `m_mix/g/avg` | true | AO | AO | AO | AO | AO |
| `m_mix/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_mix/u/avg` | true | AO | AO | 8000000000000000000000000000000000 | AO | AO |
| `m_mix/u/try_avg` | true | NULL | NULL | 8000000000000000000000000000000000 | NULL | NULL |
| `m_mix/win/avg` | true | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 | 1:8000000000000000000000000000000000 |
| `m_n33/g/avg` | true | AO | AO | AO | AO | AO |
| `m_n33/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_n33/u/avg` | true | AO | AO | -3000000000000000000000000000000000 | AO | AO |
| `m_n33/u/try_avg` | true | NULL | NULL | -3000000000000000000000000000000000 | NULL | NULL |
| `m_n33/win/avg` | true | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 | 1:-3000000000000000000000000000000000 |
| `m_s2/g/avg` | true | AO | AO | AO | AO | AO |
| `m_s2/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_s2/u/avg` | true | AO | AO | 90000000000000000000000000000000 | AO | AO |
| `m_s2/u/try_avg` | true | NULL | NULL | 90000000000000000000000000000000 | NULL | NULL |
| `m_s2/win/avg` | true | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 | 1:90000000000000000000000000000000 |
| `m_slide/g/avg` | true | AO | AO | AO | AO |  |
| `m_slide/g/avg_objhash` | true | 1:36 | 1:36 | 1:36 | 1:36 |  |
| `m_w34/g/avg` | true | AO | AO | AO | AO | AO |
| `m_w34/g/sum` | true | AO | AO | AO | AO |  |
| `m_w34/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/u/avg` | true | AO | AO | 9000000000000000000000000000000000 | AO | AO |
| `m_w34/u/avg_objhash` | true | AO | AO | 9000000000000000000000000000000000:20000 | 9000000000000000000000000000000000:20000 |  |
| `m_w34/u/sum` | true | AO | AO | NVOOR | AO |  |
| `m_w34/u/try_avg` | true | NULL | NULL | 9000000000000000000000000000000000 | NULL | NULL |
| `m_w34/win/avg` | true | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 |
| `m_w34/win/sum` | true | NVOOR | NVOOR | NVOOR | NVOOR |  |
| `m_w8/g/avg` | true | AO | AO | AO | AO | AO |
| `m_w8/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w8/u/avg` | true | AO | AO | 9000000000000000000000000000000000 | AO | AO |
| `m_w8/u/try_avg` | true | NULL | NULL | 9000000000000000000000000000000000 | NULL | NULL |
| `m_w8/win/avg` | true | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 | 1:9000000000000000000000000000000000 |
| `t/c38_0/win/avg/g2` | true | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR |
| `t/c38_0/win/avg/g3` | true | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR |
| `t/c38_0/win/avg/g5` | true | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR |
| `t/c38_0/win/sum/g2` | true | NVOOR | NVOOR | NVOOR | NVOOR |  |
| `t/c38_0/win/sum/g3` | true | NVOOR | NVOOR | NVOOR | NVOOR |  |
| `t/c38_0/win/sum/g5` | true | NVOOR | NVOOR | NVOOR | NVOOR |  |
| `t4/corr/avg` | true | AO | AO | AO | AO | AO / NVOOR |
| `t4/g/avg` | true | AO | AO | AO | AO |  |
| `t4/g/avg_objhash` | true | NVOOR | NVOOR | AO | AO |  |
| `view/m_d256` | true | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | ERR DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION |  |

Up: [map.md](map.md).
