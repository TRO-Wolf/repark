# Main on the revision's cells (critic round 1)

`pkg-main-4c365814`, 20 runs per cell: SET 1, 2, 4, 8 at 10 cores (stealing on), `2c-on` (2 cores), `4c-off` (4 cores, stealing off). Written by [../r1_repark.py](../r1_repark.py); the last column is [../r1_exact.py](../r1_exact.py). `a xN / b xM` is a cell that varies.

| query | ANSI | set1 | set2 | set4 | set8 | 2c-on | 4c-off | exact (seat A) |
|---|---|---|---|---|---|---|---|---|
| `big/gsets/avg` | false | rows=3001 nulls=0 | rows=3001 nulls=1 x11 / rows=3001 nulls=0 x9 | rows=3001 nulls=399 x7 / rows=3001 nulls=584 x4 / rows=3001 nulls=646 x4 / rows=3001 nulls=619 x4 / rows=3001 nulls=353 x1 | rows=3001 nulls=438 x11 / rows=3001 nulls=221 x5 / rows=3001 nulls=370 x3 / rows=3001 nulls=220 x1 | rows=3001 nulls=1 x12 / rows=3001 nulls=0 x8 | rows=3001 nulls=399 | rows=3001 nulls=0 |
| `m_d256/g/avg` | false | 1:0.4167 | 1:NULL | 1:NULL | 1:NULL | 1:0.4167 | 1:NULL | 1:NULL |
| `m_d256/g/try_avg` | false | 1:0.4167 | 1:NULL | 1:NULL | 1:NULL | 1:0.4167 | 1:NULL | 1:NULL |
| `m_d256/u/avg` | false | 0.4167 | 0.4167 x17 / NULL x3 | 0.4167 x16 / NULL x4 | NULL x16 / 0.4167 x4 | 0.4167 x19 / NULL x1 | NULL | NULL |
| `m_d256/u/try_avg` | false | 0.4167 | 0.4167 x19 / NULL x1 | 0.4167 x15 / NULL x5 | NULL x15 / 0.4167 x5 | 0.4167 x18 / NULL x2 | NULL | NULL |
| `m_d256/win/avg` | false | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:NULL |
| `m_d32/g/avg` | false | 1:0.625 | 1:NULL | 1:NULL | 1:NULL | 1:0.625 | 1:NULL | 1:0.625 |
| `m_d32/g/avg_M` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:9999999.99 |
| `m_d32/g/try_avg` | false | 1:0.625 | 1:NULL | 1:NULL | 1:NULL | 1:0.625 | 1:NULL | 1:0.625 |
| `m_d32/g/try_avg_M` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:9999999.99 |
| `m_d32/type` | false | decimal(9,6) | decimal(9,6) | decimal(9,6) | decimal(9,6) | decimal(9,6) | decimal(9,6) |  |
| `m_d32/u/avg` | false | 0.625 | 0.625 x15 / NULL x5 | NULL | NULL | 0.625 | NULL | 0.625 |
| `m_d32/u/avg_M` | false | NULL | NULL | NULL | NULL | NULL | NULL | 9999999.99 |
| `m_d32/u/try_avg` | false | 0.625 | NULL | NULL | NULL | 0.625 | NULL | 0.625 |
| `m_d32/u/try_avg_M` | false | NULL | NULL | NULL | NULL | NULL | NULL | 9999999.99 |
| `m_d32/win/avg` | false | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/win/avg_M` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:9999999.99 |
| `m_d64/g/avg` | false | 1:0.2381 | 1:NULL | 1:NULL | 1:NULL | 1:0.2381 x19 / 1:NULL x1 | 1:NULL | 1:0.2381 |
| `m_d64/g/avg_M` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:999999999999999999 |
| `m_d64/g/try_avg` | false | 1:0.2381 | 1:NULL | 1:NULL | 1:NULL | 1:0.2381 x17 / 1:NULL x3 | 1:NULL | 1:0.2381 |
| `m_d64/g/try_avg_M` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:999999999999999999 |
| `m_d64/type` | false | decimal(18,4) | decimal(18,4) | decimal(18,4) | decimal(18,4) | decimal(18,4) | decimal(18,4) |  |
| `m_d64/u/avg` | false | 0.2381 | NULL x19 / 0.2381 x1 | NULL | NULL | 0.2381 x18 / NULL x2 | NULL | 0.2381 |
| `m_d64/u/avg_M` | false | NULL | NULL | NULL | NULL | NULL | NULL | 999999999999999999 |
| `m_d64/u/try_avg` | false | 0.2381 | NULL | NULL | NULL | 0.2381 x16 / NULL x4 | NULL | 0.2381 |
| `m_d64/u/try_avg_M` | false | NULL | NULL | NULL | NULL | NULL | NULL | 999999999999999999 |
| `m_d64/win/avg` | false | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/win/avg_M` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:999999999999999999 |
| `m_mix/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_mix/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_mix/u/avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_mix/u/try_avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_mix/win/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:8000000000000000000000000000000000 |
| `m_n33/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_n33/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_n33/u/avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_n33/u/try_avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_n33/win/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:-3000000000000000000000000000000000 |
| `m_s2/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_s2/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_s2/u/avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_s2/u/try_avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_s2/win/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:90000000000000000000000000000000 |
| `m_w34/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/u/avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_w34/u/try_avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_w34/win/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:9000000000000000000000000000000000 |
| `m_w8/g/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w8/g/try_avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w8/u/avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_w8/u/try_avg` | false | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_w8/win/avg` | false | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:9000000000000000000000000000000000 |
| `t/c38_0/g/avg/g6` | false | 6:0.2 | 6:NULL | 6:NULL | 6:NULL | 6:NULL | 6:NULL | 6:0.2 |
| `t/c38_0/g/try_avg/g6` | false | 6:0.2 | 6:NULL | 6:NULL | 6:NULL | 6:NULL | 6:NULL | 6:0.2 |
| `t/c38_0/u/avg/g6` | false | 0.2 | NULL | NULL | NULL | NULL | NULL | 0.2 |
| `t/c38_0/u/try_avg/g6` | false | 0.2 | NULL | NULL | NULL | NULL | NULL | 0.2 |
| `t/c38_0/win/avg/g2` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows |
| `t/c38_0/win/avg/g3` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows | NULL x3 rows |
| `t/c38_0/win/avg/g5` | false | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows | NULL x2 rows |
| `t4/c38_0/g/avg/g21` | false | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL |
| `t4/c38_0/g/try_avg/g21` | false | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL |
| `t4/corr/avg` | false | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 x11 / 21:NULL 22:NULL 23:NULL 24:1 25:NULL x9 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 x19 / 21:NULL 22:NULL 23:NULL 24:1 25:NULL x1 | 21:NULL 22:NULL 23:NULL 24:1 25:NULL x12 / 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 x8 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 |
| `big/gsets/avg` | true | rows=3001 nulls=0 | AO x18 / rows=3001 nulls=0 x2 | AO | AO | rows=3001 nulls=0 x14 / AO x6 | AO | rows=3001 nulls=0 |
| `m_d256/g/avg` | true | 1:0.4167 | AO x15 / 1:0.4167 x5 | AO | AO | 1:0.4167 x16 / AO x4 | AO | AO |
| `m_d256/g/try_avg` | true | 1:0.4167 | 1:NULL | 1:NULL | 1:NULL | 1:0.4167 x18 / 1:NULL x2 | 1:NULL | 1:NULL |
| `m_d256/u/avg` | true | 0.4167 | 0.4167 x12 / AO x8 | AO | AO x18 / 0.4167 x2 | 0.4167 x19 / AO x1 | AO | AO |
| `m_d256/u/try_avg` | true | 0.4167 | 0.4167 x13 / NULL x7 | NULL x10 / 0.4167 x10 | NULL x15 / 0.4167 x5 | 0.4167 x18 / NULL x2 | NULL | NULL |
| `m_d256/win/avg` | true | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:0.4167 | 1:0.4167 | NVOOR |
| `m_d32/g/avg` | true | 1:0.625 | AO | AO | AO | 1:0.625 x18 / AO x2 | AO | 1:0.625 |
| `m_d32/g/avg_M` | true | NVOOR | AO | AO | AO | AO | AO | 1:9999999.99 |
| `m_d32/g/try_avg` | true | 1:0.625 | 1:NULL | 1:NULL | 1:NULL | 1:0.625 x17 / 1:NULL x3 | 1:NULL | 1:0.625 |
| `m_d32/g/try_avg_M` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:9999999.99 |
| `m_d32/type` | true | decimal(9,6) | AO x13 / decimal(9,6) x7 | AO | AO | decimal(9,6) x16 / AO x4 | AO |  |
| `m_d32/u/avg` | true | 0.625 | 0.625 x18 / AO x2 | AO | AO | 0.625 x18 / AO x2 | AO | 0.625 |
| `m_d32/u/avg_M` | true | NVOOR | AO | AO | AO | AO | AO | 9999999.99 |
| `m_d32/u/try_avg` | true | 0.625 | 0.625 x16 / NULL x4 | NULL | NULL | 0.625 x17 / NULL x3 | NULL | 0.625 |
| `m_d32/u/try_avg_M` | true | NULL | NULL | NULL | NULL | NULL | NULL | 9999999.99 |
| `m_d32/win/avg` | true | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 | 1:0.625 |
| `m_d32/win/avg_M` | true | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR | 1:9999999.99 |
| `m_d64/g/avg` | true | 1:0.2381 | AO | AO | AO | 1:0.2381 x17 / AO x3 | AO | 1:0.2381 |
| `m_d64/g/avg_M` | true | NVOOR | AO | AO | AO | AO | AO | 1:999999999999999999 |
| `m_d64/g/try_avg` | true | 1:0.2381 | 1:NULL | 1:NULL | 1:NULL | 1:0.2381 x16 / 1:NULL x4 | 1:NULL | 1:0.2381 |
| `m_d64/g/try_avg_M` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:999999999999999999 |
| `m_d64/type` | true | decimal(18,4) | decimal(18,4) x15 / AO x5 | AO | AO x19 / decimal(18,4) x1 | decimal(18,4) x16 / AO x4 | AO |  |
| `m_d64/u/avg` | true | 0.2381 | 0.2381 x18 / AO x2 | AO | AO | 0.2381 x18 / AO x2 | AO | 0.2381 |
| `m_d64/u/avg_M` | true | NVOOR | AO | AO | AO | AO | AO | 999999999999999999 |
| `m_d64/u/try_avg` | true | 0.2381 | 0.2381 x18 / NULL x2 | NULL | NULL | 0.2381 x14 / NULL x6 | NULL | 0.2381 |
| `m_d64/u/try_avg_M` | true | NULL | NULL | NULL | NULL | NULL | NULL | 999999999999999999 |
| `m_d64/win/avg` | true | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 | 1:0.2381 |
| `m_d64/win/avg_M` | true | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR | 1:999999999999999999 |
| `m_mix/g/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_mix/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_mix/u/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_mix/u/try_avg` | true | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_mix/win/avg` | true | AO | AO | AO | AO | AO | AO | 1:8000000000000000000000000000000000 |
| `m_n33/g/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_n33/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_n33/u/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_n33/u/try_avg` | true | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_n33/win/avg` | true | AO | AO | AO | AO | AO | AO | 1:-3000000000000000000000000000000000 |
| `m_s2/g/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_s2/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_s2/u/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_s2/u/try_avg` | true | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_s2/win/avg` | true | AO | AO | AO | AO | AO | AO | 1:90000000000000000000000000000000 |
| `m_w34/g/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_w34/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/u/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_w34/u/try_avg` | true | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_w34/win/avg` | true | AO | AO | AO | AO | AO | AO | 1:9000000000000000000000000000000000 |
| `m_w8/g/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_w8/g/try_avg` | true | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL | 1:NULL |
| `m_w8/u/avg` | true | AO | AO | AO | AO | AO | AO | AO |
| `m_w8/u/try_avg` | true | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| `m_w8/win/avg` | true | AO | AO | AO | AO | AO | AO | 1:9000000000000000000000000000000000 |
| `t/c38_0/g/avg/g6` | true | 6:0.2 | AO | AO | AO | AO | AO | 6:0.2 |
| `t/c38_0/g/try_avg/g6` | true | 6:0.2 | 6:NULL | 6:NULL | 6:NULL | 6:NULL | 6:NULL | 6:0.2 |
| `t/c38_0/u/avg/g6` | true | 0.2 | AO | AO | AO | AO | AO | 0.2 |
| `t/c38_0/u/try_avg/g6` | true | 0.2 | NULL | NULL | NULL | NULL | NULL | 0.2 |
| `t/c38_0/win/avg/g2` | true | AO | AO | AO | AO | AO | AO | NVOOR |
| `t/c38_0/win/avg/g3` | true | AO | AO | AO | AO | AO | AO | NVOOR |
| `t/c38_0/win/avg/g5` | true | AO | AO | AO | AO | AO | AO | NVOOR |
| `t4/c38_0/g/avg/g21` | true | NVOOR | NVOOR | NVOOR | NVOOR | NVOOR x18 / AO x2 | NVOOR | NVOOR |
| `t4/c38_0/g/try_avg/g21` | true | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL | 21:NULL |
| `t4/corr/avg` | true | NVOOR | NVOOR x12 / AO x8 | AO x14 / NVOOR x6 | AO x11 / NVOOR x9 | AO x12 / NVOOR x8 | NVOOR x11 / AO x9 | AO / NVOOR |
