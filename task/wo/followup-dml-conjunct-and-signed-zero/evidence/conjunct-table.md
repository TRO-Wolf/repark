# DML conjunct drop, one row per RePark cell

## Label totals (RePark cells)

- `ACCEPTED-WHERE-SPARK-REFUSED`: 30
- `EQUAL`: 285
- `FEWER-COMMIT`: 63
- `NATIVE-REFUSED`: 66
- `NOT-EXPRESSIBLE`: 24
- `REFUSED-WHERE-SPARK-ACCEPTED`: 36

Spark cells with a DML touching rows outside its own SELECT: 0

## Counts per door and seat (RePark cells)

| door | seat | cells | ACCEPTED-WHERE-SPARK-REFUSED | EQUAL | FEWER-COMMIT | NATIVE-REFUSED | NOT-EXPRESSIBLE | REFUSED-WHERE-SPARK-ACCEPTED |
|---|---|---|---|---|---|---|---|---|
| spark-sql | select | 36 | 2 | 34 | 0 | 0 | 0 | 0 |
| spark-sql | delete | 108 | 8 | 61 | 21 | 0 | 0 | 18 |
| spark-sql | update | 108 | 8 | 61 | 21 | 0 | 0 | 18 |
| spark-sql | merge | 108 | 12 | 75 | 21 | 0 | 0 | 0 |
| native | select | 36 | 0 | 30 | 0 | 0 | 6 | 0 |
| native | delete | 36 | 0 | 12 | 0 | 18 | 6 | 0 |
| native | update | 36 | 0 | 12 | 0 | 18 | 6 | 0 |
| native | merge | 36 | 0 | 0 | 0 | 30 | 6 | 0 |

## Predicates

- `x1f1`: `id = 1 AND st.y IS NULL`
- `x1f2`: `st.y IS NULL AND id = 1`
- `x1f3`: `id = 1 OR st.y IS NULL`
- `x1f4`: `NOT (st.y IS NULL) AND id = 1`
- `x1f5`: `id IN (1, 2) AND st.y IS NULL`
- `x1f6`: `(st.y IS NULL) = true`
- `x2f1`: `id = 1 AND st.x = 1`
- `x2f2`: `st.x = 1 AND id = 1`
- `x2f3`: `id = 1 OR st.x = 1`
- `x2f4`: `NOT (st.x = 1) AND id = 1`
- `x2f5`: `id IN (1, 2) AND st.x = 1`
- `x2f6`: `(st.x = 1) = true`
- `x3f1`: `id = 1 AND m['k'] = 1`
- `x3f2`: `m['k'] = 1 AND id = 1`
- `x3f3`: `id = 1 OR m['k'] = 1`
- `x3f4`: `NOT (m['k'] = 1) AND id = 1`
- `x3f5`: `id IN (1, 2) AND m['k'] = 1`
- `x3f6`: `(m['k'] = 1) = true`
- `x4f1`: `id = 1 AND CASE WHEN i = 1 THEN true END`
- `x4f2`: `CASE WHEN i = 1 THEN true END AND id = 1`
- `x4f3`: `id = 1 OR CASE WHEN i = 1 THEN true END`
- `x4f4`: `NOT (CASE WHEN i = 1 THEN true END) AND id = 1`
- `x4f5`: `id IN (1, 2) AND CASE WHEN i = 1 THEN true END`
- `x4f6`: `(CASE WHEN i = 1 THEN true END) = true`
- `x5f1`: `id = 1 AND element_at(arr, 1) = 1`
- `x5f2`: `element_at(arr, 1) = 1 AND id = 1`
- `x5f3`: `id = 1 OR element_at(arr, 1) = 1`
- `x5f4`: `NOT (element_at(arr, 1) = 1) AND id = 1`
- `x5f5`: `id IN (1, 2) AND element_at(arr, 1) = 1`
- `x5f6`: `(element_at(arr, 1) = 1) = true`
- `x6f1`: `id = 1 AND coalesce(i, 0) = 0`
- `x6f2`: `coalesce(i, 0) = 0 AND id = 1`
- `x6f3`: `id = 1 OR coalesce(i, 0) = 0`
- `x6f4`: `NOT (coalesce(i, 0) = 0) AND id = 1`
- `x6f5`: `id IN (1, 2) AND coalesce(i, 0) = 0`
- `x6f6`: `(coalesce(i, 0) = 0) = true`

## Finding cells (one line each)

- x1f1 `id = 1 AND st.y IS NULL` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x5f4 `NOT (element_at(arr, 1) = 1) AND id = 1` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | delete | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x5f4 `NOT (element_at(arr, 1) = 1) AND id = 1` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | delete | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x5f4 `NOT (element_at(arr, 1) = 1) AND id = 1` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | delete | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x5f4 `NOT (element_at(arr, 1) = 1) AND id = 1` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | update | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x5f4 `NOT (element_at(arr, 1) = 1) AND id = 1` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | update | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x5f4 `NOT (element_at(arr, 1) = 1) AND id = 1` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | update | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x3f4 `NOT (m['k'] = 1) AND id = 1` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | merge | spark-sql | v2-cow | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x3f4 `NOT (m['k'] = 1) AND id = 1` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | merge | spark-sql | v2-mor | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f1 `id = 1 AND st.y IS NULL` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x1f2 `st.y IS NULL AND id = 1` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x2f4 `NOT (st.x = 1) AND id = 1` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x3f4 `NOT (m['k'] = 1) AND id = 1` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x4f4 `NOT (CASE WHEN i = 1 THEN true END) AND id = 1` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f1 `id = 1 AND coalesce(i, 0) = 0` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}
- x6f2 `coalesce(i, 0) = 0 AND id = 1` | merge | spark-sql | v3 | FEWER-COMMIT | Spark: ok {} | RePark: ok {} | own select {} | outside own select {}

## Full table (one row per RePark cell)

| pid | door | seat | kind | RePark | Spark | RePark own select | outside own select | labels |
|---|---|---|---|---|---|---|---|---|
| x1f1 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x1f2 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x1f3 | spark-sql | select | v2-cow | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | - | EQUAL |
| x1f4 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x1f5 | spark-sql | select | v2-cow | ok {2} | ok {2} | {2} | - | EQUAL |
| x1f6 | spark-sql | select | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | - | EQUAL |
| x2f1 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x2f2 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x2f3 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | - | EQUAL |
| x2f4 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x2f5 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x2f6 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | - | EQUAL |
| x3f1 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x3f2 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x3f3 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | - | EQUAL |
| x3f4 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x3f5 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x3f6 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | - | EQUAL |
| x4f1 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x4f2 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x4f3 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | - | EQUAL |
| x4f4 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x4f5 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x4f6 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | - | EQUAL |
| x5f1 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x5f2 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x5f3 | spark-sql | select | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | - | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x5f5 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x5f6 | spark-sql | select | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | - | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x6f2 | spark-sql | select | v2-cow | ok {} | ok {} | {} | - | EQUAL |
| x6f3 | spark-sql | select | v2-cow | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | - | EQUAL |
| x6f4 | spark-sql | select | v2-cow | ok {1} | ok {1} | {1} | - | EQUAL |
| x6f5 | spark-sql | select | v2-cow | ok {2} | ok {2} | {2} | - | EQUAL |
| x6f6 | spark-sql | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | - | EQUAL |
| x1f1 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | delete | v2-cow | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | delete | v2-cow | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | delete | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_cow_x3f1' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f2 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_cow_x3f2' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f3 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_cow_x3f3' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f4 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_cow_x3f4' caused by | ok {} | {} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f5 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_cow_x3f5' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f6 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_cow_x3f6' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x4f1 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | delete | v2-cow | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | delete | v2-cow | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x5f5 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | delete | v2-cow | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | delete | v2-cow | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | delete | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | delete | v2-cow | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | delete | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | delete | v2-mor | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | delete | v2-mor | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | delete | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_mor_x3f1' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f2 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_mor_x3f2' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f3 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_mor_x3f3' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f4 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_mor_x3f4' caused by | ok {} | {} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f5 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_mor_x3f5' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f6 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v2_mor_x3f6' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x4f1 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f3 | spark-sql | delete | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x5f5 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | delete | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | delete | v2-mor | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | delete | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | delete | v2-mor | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | delete | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | delete | v3 | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | delete | v3 | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | delete | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v3_x3f1' caused by Schema | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f2 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v3_x3f2' caused by Schema | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f3 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v3_x3f3' caused by Schema | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f4 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v3_x3f4' caused by Schema | ok {} | {} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f5 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v3_x3f5' caused by Schema | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f6 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_spark_sql_delete_v3_x3f6' caused by Schema | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x4f1 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | delete | v3 | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | delete | v3 | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x5f5 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | delete | v3 | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | delete | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | delete | v3 | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | delete | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | delete | v3 | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | delete | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | update | v2-cow | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | update | v2-cow | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | update | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_cow_x3f1' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f2 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_cow_x3f2' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f3 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_cow_x3f3' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f4 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_cow_x3f4' caused by | ok {} | {} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f5 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_cow_x3f5' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f6 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_cow_x3f6' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x4f1 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | update | v2-cow | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | update | v2-cow | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x5f5 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | update | v2-cow | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | update | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | update | v2-cow | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | update | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | update | v2-cow | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | update | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | update | v2-mor | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | update | v2-mor | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | update | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_mor_x3f1' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f2 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_mor_x3f2' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f3 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_mor_x3f3' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f4 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_mor_x3f4' caused by | ok {} | {} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f5 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_mor_x3f5' caused by | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f6 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v2_mor_x3f6' caused by | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x4f1 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f3 | spark-sql | update | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x5f5 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | update | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | update | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | update | v2-mor | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | update | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | update | v2-mor | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | update | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | update | v3 | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | update | v3 | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | update | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v3_x3f1' caused by Schema | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f2 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v3_x3f2' caused by Schema | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f3 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v3_x3f3' caused by Schema | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f4 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v3_x3f4' caused by Schema | ok {} | {} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f5 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v3_x3f5' caused by Schema | ok {1} | {1} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x3f6 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_spark_sql_update_v3_x3f6' caused by Schema | ok {1,8} | {1,8} | - | REFUSED-WHERE-SPARK-ACCEPTED |
| x4f1 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | update | v3 | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | update | v3 | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x5f5 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | update | v3 | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | update | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | update | v3 | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | update | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | update | v3 | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | update | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | merge | v2-cow | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | merge | v2-cow | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | merge | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f2 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f3 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f4 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x3f5 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f6 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f1 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | merge | v2-cow | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | merge | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | merge | v2-cow | ok {} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f5 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | merge | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | merge | v2-cow | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | merge | v2-cow | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | merge | v2-cow | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | merge | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | merge | v2-mor | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | merge | v2-mor | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | merge | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f2 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f3 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f4 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x3f5 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f6 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f1 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | merge | v2-mor | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | merge | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | merge | v2-mor | ok {} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f5 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | merge | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | merge | v2-mor | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | merge | v2-mor | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | merge | v2-mor | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | merge | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f2 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x1f3 | spark-sql | merge | v3 | ok {1,2,3,4,5,6,7} | ok {1,2,3,4,5,6,7} | {1,2,3,4,5,6,7} | {} | EQUAL |
| x1f4 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x1f5 | spark-sql | merge | v3 | ok {2} | ok {2} | {2} | {} | EQUAL |
| x1f6 | spark-sql | merge | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | {} | EQUAL |
| x2f1 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f2 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f3 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x2f4 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x2f5 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x2f6 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f1 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f2 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f3 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x3f4 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x3f5 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x3f6 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f1 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f2 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f3 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x4f4 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x4f5 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x4f6 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | {} | EQUAL |
| x5f1 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f2 | spark-sql | merge | v3 | ok {1} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f3 | spark-sql | merge | v3 | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f4 | spark-sql | merge | v3 | ok {} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x5f5 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x5f6 | spark-sql | merge | v3 | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | {} | ACCEPTED-WHERE-SPARK-REFUSED |
| x6f1 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f2 | spark-sql | merge | v3 | ok {} | ok {} | {} | {} | FEWER-COMMIT |
| x6f3 | spark-sql | merge | v3 | ok {1,2,3,4,7} | ok {1,2,3,4,7} | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | spark-sql | merge | v3 | ok {1} | ok {1} | {1} | {} | EQUAL |
| x6f5 | spark-sql | merge | v3 | ok {2} | ok {2} | {2} | {} | EQUAL |
| x6f6 | spark-sql | merge | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | {} | EQUAL |
| x1f1 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x1f2 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x1f3 | native | select | v2-cow | ok {1,2,3,4,5,6,7} | - | {1,2,3,4,5,6,7} | - | EQUAL |
| x1f4 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x1f5 | native | select | v2-cow | ok {2} | - | {2} | - | EQUAL |
| x1f6 | native | select | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | - | EQUAL |
| x2f1 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x2f2 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x2f3 | native | select | v2-cow | ok {1,8} | - | {1,8} | - | EQUAL |
| x2f4 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x2f5 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x2f6 | native | select | v2-cow | ok {1,8} | - | {1,8} | - | EQUAL |
| x3f1 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f2 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f3 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f4 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f5 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f6 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x4f1 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x4f2 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x4f3 | native | select | v2-cow | ok {1,8} | - | {1,8} | - | EQUAL |
| x4f4 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x4f5 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x4f6 | native | select | v2-cow | ok {1,8} | - | {1,8} | - | EQUAL |
| x5f1 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x5f2 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x5f3 | native | select | v2-cow | ok {1,8} | - | {1,8} | - | EQUAL |
| x5f4 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x5f5 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x5f6 | native | select | v2-cow | ok {1,8} | - | {1,8} | - | EQUAL |
| x6f1 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x6f2 | native | select | v2-cow | ok {} | - | {} | - | EQUAL |
| x6f3 | native | select | v2-cow | ok {1,2,3,4,7} | - | {1,2,3,4,7} | - | EQUAL |
| x6f4 | native | select | v2-cow | ok {1} | - | {1} | - | EQUAL |
| x6f5 | native | select | v2-cow | ok {2} | - | {2} | - | EQUAL |
| x6f6 | native | select | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | - | EQUAL |
| x1f1 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x1f1' caused by | - | {} | - | NATIVE-REFUSED |
| x1f2 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x1f2' caused by | - | {} | - | NATIVE-REFUSED |
| x1f3 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x1f3' caused by | - | {1,2,3,4,5,6,7} | - | NATIVE-REFUSED |
| x1f4 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x1f4' caused by | - | {1} | - | NATIVE-REFUSED |
| x1f5 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x1f5' caused by | - | {2} | - | NATIVE-REFUSED |
| x1f6 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x1f6' caused by | - | {2,3,4,5,6,7} | - | NATIVE-REFUSED |
| x2f1 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x2f1' caused by | - | {1} | - | NATIVE-REFUSED |
| x2f2 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x2f2' caused by | - | {1} | - | NATIVE-REFUSED |
| x2f3 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x2f3' caused by | - | {1,8} | - | NATIVE-REFUSED |
| x2f4 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x2f4' caused by | - | {} | - | NATIVE-REFUSED |
| x2f5 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x2f5' caused by | - | {1} | - | NATIVE-REFUSED |
| x2f6 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x2f6' caused by | - | {1,8} | - | NATIVE-REFUSED |
| x3f1 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f2 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f3 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f4 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f5 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f6 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x4f1 | native | delete | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x4f2 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x4f2' caused by | - | {1} | - | NATIVE-REFUSED |
| x4f3 | native | delete | v2-cow | ok {1,8} | - | {1,8} | {} | EQUAL |
| x4f4 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x4f4' caused by | - | {} | - | NATIVE-REFUSED |
| x4f5 | native | delete | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x4f6 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x4f6' caused by | - | {1,8} | - | NATIVE-REFUSED |
| x5f1 | native | delete | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x5f2 | native | delete | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x5f3 | native | delete | v2-cow | ok {1,8} | - | {1,8} | {} | EQUAL |
| x5f4 | native | delete | v2-cow | ok {} | - | {} | {} | EQUAL |
| x5f5 | native | delete | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x5f6 | native | delete | v2-cow | ok {1,8} | - | {1,8} | {} | EQUAL |
| x6f1 | native | delete | v2-cow | ok {} | - | {} | {} | EQUAL |
| x6f2 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x6f2' caused by | - | {} | - | NATIVE-REFUSED |
| x6f3 | native | delete | v2-cow | ok {1,2,3,4,7} | - | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x6f4' caused by | - | {1} | - | NATIVE-REFUSED |
| x6f5 | native | delete | v2-cow | ok {2} | - | {2} | {} | EQUAL |
| x6f6 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.cj_native_delete_v2_cow_x6f6' caused by | - | {2,3,4,7} | - | NATIVE-REFUSED |
| x1f1 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x1f1' caused by | - | {} | - | NATIVE-REFUSED |
| x1f2 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x1f2' caused by | - | {} | - | NATIVE-REFUSED |
| x1f3 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x1f3' caused by | - | {1,2,3,4,5,6,7} | - | NATIVE-REFUSED |
| x1f4 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x1f4' caused by | - | {1} | - | NATIVE-REFUSED |
| x1f5 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x1f5' caused by | - | {2} | - | NATIVE-REFUSED |
| x1f6 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x1f6' caused by | - | {2,3,4,5,6,7} | - | NATIVE-REFUSED |
| x2f1 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x2f1' caused by | - | {1} | - | NATIVE-REFUSED |
| x2f2 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x2f2' caused by | - | {1} | - | NATIVE-REFUSED |
| x2f3 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x2f3' caused by | - | {1,8} | - | NATIVE-REFUSED |
| x2f4 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x2f4' caused by | - | {} | - | NATIVE-REFUSED |
| x2f5 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x2f5' caused by | - | {1} | - | NATIVE-REFUSED |
| x2f6 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x2f6' caused by | - | {1,8} | - | NATIVE-REFUSED |
| x3f1 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f2 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f3 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f4 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f5 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f6 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x4f1 | native | update | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x4f2 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x4f2' caused by | - | {1} | - | NATIVE-REFUSED |
| x4f3 | native | update | v2-cow | ok {1,8} | - | {1,8} | {} | EQUAL |
| x4f4 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x4f4' caused by | - | {} | - | NATIVE-REFUSED |
| x4f5 | native | update | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x4f6 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x4f6' caused by | - | {1,8} | - | NATIVE-REFUSED |
| x5f1 | native | update | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x5f2 | native | update | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x5f3 | native | update | v2-cow | ok {1,8} | - | {1,8} | {} | EQUAL |
| x5f4 | native | update | v2-cow | ok {} | - | {} | {} | EQUAL |
| x5f5 | native | update | v2-cow | ok {1} | - | {1} | {} | EQUAL |
| x5f6 | native | update | v2-cow | ok {1,8} | - | {1,8} | {} | EQUAL |
| x6f1 | native | update | v2-cow | ok {} | - | {} | {} | EQUAL |
| x6f2 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x6f2' caused by | - | {} | - | NATIVE-REFUSED |
| x6f3 | native | update | v2-cow | ok {1,2,3,4,7} | - | {1,2,3,4,7} | {} | EQUAL |
| x6f4 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x6f4' caused by | - | {1} | - | NATIVE-REFUSED |
| x6f5 | native | update | v2-cow | ok {2} | - | {2} | {} | EQUAL |
| x6f6 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.cj_native_update_v2_cow_x6f6' caused by | - | {2,3,4,7} | - | NATIVE-REFUSED |
| x1f1 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x1f2 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x1f3 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,2,3,4,5,6,7} | - | NATIVE-REFUSED |
| x1f4 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x1f5 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2} | - | NATIVE-REFUSED |
| x1f6 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,5,6,7} | - | NATIVE-REFUSED |
| x2f1 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x2f2 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x2f3 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | - | NATIVE-REFUSED |
| x2f4 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x2f5 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x2f6 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | - | NATIVE-REFUSED |
| x3f1 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f2 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f3 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f4 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f5 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x3f6 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| x4f1 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x4f2 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x4f3 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | - | NATIVE-REFUSED |
| x4f4 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x4f5 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x4f6 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | - | NATIVE-REFUSED |
| x5f1 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x5f2 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x5f3 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | - | NATIVE-REFUSED |
| x5f4 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x5f5 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x5f6 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | - | NATIVE-REFUSED |
| x6f1 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x6f2 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | - | NATIVE-REFUSED |
| x6f3 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,2,3,4,7} | - | NATIVE-REFUSED |
| x6f4 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1} | - | NATIVE-REFUSED |
| x6f5 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2} | - | NATIVE-REFUSED |
| x6f6 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,7} | - | NATIVE-REFUSED |
