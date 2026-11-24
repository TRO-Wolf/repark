# The pins' cells on main, and #1360's sum windows (critic round 1)

Main `pkg-main-4c365814` unless named. Spill: one file of 200,000 groups (`M, M` then `-M, -M, 5`, every group `1`), `target_partitions` 1, 5 runs, pool `datafusion.runtime.memory_limit`. Skip-partial: `big` at SET 4, 20 runs, default against `skip_partial_aggregation_probe_rows_threshold = 1` and `probe_ratio_threshold = 0.0`. DataFrame door and Iceberg: [../r1_door.py](../r1_door.py), a RePark-written v2 table (`t4` plus group 26, then `DELETE` of one row: a position-delete file with columns `file_path,pos` in both modes), 20 runs. `sum`: #1360's `pkg-sum-845766fd`, 20 runs; Spark's `sum` cells are in [spark.md](spark.md).

## Spill

| query | ANSI | nopool | pool16M | exact (seat A) |
|---|---|---|---|---|
| `sp/g/avg/count` | false | 200000/200000 1..1 | 200000/0 NULL..NULL | 200000/200000 1..1 |
| `sp/g/try_avg/count` | false | 200000/200000 1..1 | 200000/0 NULL..NULL | 200000/200000 1..1 |
| `sp/g/avg/count` | true | 200000/200000 1..1 | AO x3 / ERR Resources exhausted: Failed to allocate addi x2 | 200000/200000 1..1 |
| `sp/g/try_avg/count` | true | 200000/200000 1..1 | ERR Resources exhausted: Failed to allocate addi x4 / ERR Resources exhausted: Failed to allocate addi x1 | 200000/200000 1..1 |

## Skip-partial (`convert_to_state`)

| query | ANSI | default | probe1-ratio0 | exact (seat A) |
|---|---|---|---|---|
| `big/g/avg/count` | false | 3000/2602 0.0167..0.0167 x13 / 3000/2417 0.0167..0.0167 x4 / 3000/2355 0.0167..0.0167 x2 / 3000/2382 0.0167..0.0167 x1 | 3000/3000 0.0167..0.0167 | 3000/3000 0.0167..0.0167 |
| `big/g/try_avg/count` | false | 3000/2602 0.0167..0.0167 x10 / 3000/2417 0.0167..0.0167 x4 / 3000/2382 0.0167..0.0167 x4 / 3000/2648 0.0167..0.0167 x2 | 3000/3000 0.0167..0.0167 | 3000/3000 0.0167..0.0167 |
| `big/g/avg/count` | true | AO | 3000/3000 0.0167..0.0167 | 3000/3000 0.0167..0.0167 |
| `big/g/try_avg/count` | true | 3000/2602 0.0167..0.0167 x11 / 3000/2417 0.0167..0.0167 x5 / 3000/2355 0.0167..0.0167 x2 / 3000/2382 0.0167..0.0167 x1 / 3000/2648 0.0167..0.0167 x1 | 3000/3000 0.0167..0.0167 | 3000/3000 0.0167..0.0167 |

## DataFrame door and Iceberg v2 with a position-delete file

| query | ANSI | SET 1 | SET 2 | SET 4 | SET 8 | exact (seat A) |
|---|---|---|---|---|---|---|
| `big/table/g/avg` | false | rows=3000 nulls=0 | rows=3000 nulls=0 | rows=3000 nulls=583 x8 / rows=3000 nulls=398 x6 / rows=3000 nulls=618 x3 / rows=3000 nulls=352 x2 / rows=3000 nulls=645 x1 | rows=3000 nulls=437 x12 / rows=3000 nulls=220 x5 / rows=3000 nulls=369 x2 / rows=3000 nulls=219 x1 | rows=3000 nulls=0 |
| `big/table/g/try_avg` | false | rows=3000 nulls=0 | rows=3000 nulls=0 | rows=3000 nulls=398 x9 / rows=3000 nulls=583 x5 / rows=3000 nulls=618 x3 / rows=3000 nulls=645 x1 / rows=3000 nulls=580 x1 / rows=3000 nulls=352 x1 | rows=3000 nulls=437 x14 / rows=3000 nulls=369 x4 / rows=3000 nulls=220 x2 | rows=3000 nulls=0 |
| `big/table/u/avg` | false | 0.0167 | 0.0167 x15 / NULL x5 | NULL | NULL | 0.0167 |
| `ice/sql/g/avg` | false | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:NULL 26:1.6667 x19 / 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 x1 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 |
| `ice/sql/g/try_avg` | false | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:NULL 26:1.6667 x19 / 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 x1 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 |
| `ice/sql/u26/avg` | false | 1.6667 | 1.6667 | 1.6667 | 1.6667 | 1.6667 |
| `ice/table/g/avg` | false | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:NULL 26:1.6667 x19 / 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 x1 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 |
| `t4/table/g/avg` | false | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:NULL x13 / 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 x7 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 |
| `big/table/g/avg` | true | rows=3000 nulls=0 | rows=3000 nulls=0 | AO | AO | rows=3000 nulls=0 |
| `big/table/g/try_avg` | true | rows=3000 nulls=0 | rows=3000 nulls=0 | rows=3000 nulls=398 x12 / rows=3000 nulls=618 x2 / rows=3000 nulls=583 x2 / rows=3000 nulls=645 x2 / rows=3000 nulls=352 x2 | rows=3000 nulls=437 x14 / rows=3000 nulls=220 x3 / rows=3000 nulls=369 x2 / rows=3000 nulls=219 x1 | rows=3000 nulls=0 |
| `big/table/u/avg` | true | 0.0167 | 0.0167 x16 / AO x4 | AO | AO | 0.0167 |
| `ice/sql/g/avg` | true | NVOOR | AO x12 / NVOOR x8 | NVOOR x11 / AO x9 | AO x13 / NVOOR x7 | AO / NVOOR |
| `ice/sql/g/try_avg` | true | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:NULL 26:1.6667 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 x19 / 21:NULL 22:NULL 23:NULL 24:1 25:NULL 26:1.6667 x1 | 21:NULL 22:NULL 23:NULL 24:1 25:1.6667 26:1.6667 | 21:NULL 22:NULL 23:1.6667 24:1 25:1.6667 26:1.6667 |
| `ice/sql/u26/avg` | true | 1.6667 | 1.6667 | 1.6667 | 1.6667 | 1.6667 |
| `ice/table/g/avg` | true | NVOOR | NVOOR x11 / AO x9 | AO x12 / NVOOR x8 | AO x10 / NVOOR x10 | AO / NVOOR |
| `t4/table/g/avg` | true | NVOOR | AO x13 / NVOOR x7 | AO x10 / NVOOR x10 | NVOOR x11 / AO x9 | AO / NVOOR |

## `sum` on #1360's head (windows and the `m_w34` twins)

| query | ANSI | set1 | set4 | 2c-on |
|---|---|---|---|---|
| `m_w34/g/sum` | false | 1:NULL | 1:NULL | 1:NULL |
| `m_w34/u/sum` | false | NULL | NULL | NULL |
| `m_w34/win/sum` | false | 1:NULL | 1:NULL | 1:NULL |
| `t/c38_0/win/sum/g2` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows |
| `t/c38_0/win/sum/g3` | false | NULL x3 rows | NULL x3 rows | NULL x3 rows |
| `t/c38_0/win/sum/g5` | false | NULL x2 rows | NULL x2 rows | NULL x2 rows |
| `m_w34/g/sum` | true | AO | AO | AO |
| `m_w34/u/sum` | true | AO | AO | AO |
| `m_w34/win/sum` | true | NVOOR | NVOOR | NVOOR |
| `t/c38_0/win/sum/g2` | true | NVOOR | NVOOR | NVOOR |
| `t/c38_0/win/sum/g3` | true | NVOOR | NVOOR | NVOOR |
| `t/c38_0/win/sum/g5` | true | NVOOR | NVOOR | NVOOR |
