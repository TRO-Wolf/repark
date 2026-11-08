# Envelope table, one row per cell

Predicate texts (Spark spelling):

- `p01`: `i = 1`
- `p02`: `i <> 1`
- `p03`: `i IN (1, NULL)`
- `p04`: `i NOT IN (1, NULL)`
- `p05`: `i IS NULL`
- `p06`: `i BETWEEN 1 AND 2`
- `p07`: `i <=> NULL`
- `p08`: `i + 1 > 0`
- `p09`: `CAST(i AS STRING) = '1'`
- `p10`: `l = 9223372036854775807`
- `p11`: `dec = 1.10`
- `p12`: `dec > 1.1`
- `p13`: `d = CAST('NaN' AS DOUBLE)`
- `p14`: `isnan(d)`
- `p15`: `d = 0.0`
- `p16`: `d > 0`
- `p17`: `s = ''`
- `p18`: `s = 'a '`
- `p19`: `s LIKE 'a%'`
- `p20`: `upper(s) = 'A'`
- `p21`: `length(s) = 0`
- `p22`: `b`
- `p23`: `b IS NOT TRUE`
- `p24`: `dt = DATE '1970-01-01'`
- `p25`: `ts > TIMESTAMP '2026-01-01 00:00:00'`
- `p26`: `ntz = TIMESTAMP_NTZ '2026-03-08 02:30:00'`
- `p27`: `st.x = 1`
- `p28`: `st IS NULL`
- `p29`: `st.y IS NULL`
- `p30`: `st = named_struct('x', 1, 'y', 'a')`
- `p31`: `array_contains(arr, 1)`
- `p32`: `size(arr) = 0`
- `p33`: `element_at(arr, 1) = 1`
- `p34`: `m['k'] = 1`
- `p35`: `coalesce(i, 0) = 0`
- `p36`: `CASE WHEN i = 1 THEN true END`

| pid | door | seat | kind | RePark | Spark | RePark own select | snapshots added | labels |
|---|---|---|---|---|---|---|---|---|
| p01 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | select | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | select | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | select | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | select | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | select | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | select | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-READ |
| p16 | spark-sql | select | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | select | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | select | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | select | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | select | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | select | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | select | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | select | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | select | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | delete | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | delete | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | delete | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | delete | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | delete | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | delete | v2-cow | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | delete | v2-cow | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | delete | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | delete | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | delete | v2-cow | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | delete | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | delete | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | delete | v2-cow | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | delete | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | delete | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | delete | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | delete | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | delete | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | delete | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | delete | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | delete | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | delete | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v2_cow_p29_t' caused by | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | spark-sql | delete | v2-cow | ok {1,8} | REFUSED IllegalArgumentException Cannot create expression literal from | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p31 | spark-sql | delete | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | delete | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | delete | v2-cow | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v2_cow_p34_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p35 | spark-sql | delete | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v2_cow_p36_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p01 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | delete | v2-mor | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | delete | v2-mor | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | delete | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | delete | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | delete | v2-mor | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | delete | v2-mor | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | delete | v2-mor | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | delete | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | delete | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | delete | v2-mor | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | delete | v2-mor | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | delete | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | delete | v2-mor | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | delete | v2-mor | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | delete | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | delete | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | delete | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | delete | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | delete | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | delete | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | delete | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | delete | v2-mor | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v2_mor_p29_t' caused by | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | spark-sql | delete | v2-mor | ok {1,8} | REFUSED IllegalArgumentException Cannot create expression literal from | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p31 | spark-sql | delete | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | delete | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | delete | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v2_mor_p34_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p35 | spark-sql | delete | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | delete | v2-mor | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v2_mor_p36_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p01 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | delete | v3 | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | delete | v3 | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | delete | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | delete | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | delete | v3 | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | delete | v3 | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | delete | v3 | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | delete | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | delete | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | delete | v3 | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | delete | v3 | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | delete | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | delete | v3 | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | delete | v3 | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | delete | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | delete | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | delete | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | delete | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | delete | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | delete | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | delete | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | delete | v3 | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v3_p29_t' caused by Schema | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | spark-sql | delete | v3 | ok {1,8} | REFUSED IllegalArgumentException Cannot create expression literal from | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p31 | spark-sql | delete | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | delete | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | delete | v3 | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v3_p34_t' caused by Schema | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p35 | spark-sql | delete | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | delete | v3 | REFUSED AnalysisException DELETE operation on table 'ice.ns.spark_sql_delete_v3_p36_t' caused by Schema | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p01 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | update | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | update | v2-cow | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | update | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | update | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | update | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | update | v2-cow | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | update | v2-cow | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | update | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | update | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | update | v2-cow | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | update | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | update | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | update | v2-cow | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | update | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | update | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | update | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | update | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | update | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | update | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | update | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | update | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v2_cow_p29_t' caused by | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | spark-sql | update | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p31 | spark-sql | update | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | update | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | update | v2-cow | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v2_cow_p34_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p35 | spark-sql | update | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v2_cow_p36_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p01 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | update | v2-mor | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | update | v2-mor | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | update | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | update | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | update | v2-mor | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | update | v2-mor | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | update | v2-mor | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | update | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | update | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | update | v2-mor | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | update | v2-mor | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | update | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | update | v2-mor | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | update | v2-mor | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | update | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | update | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | update | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | update | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | update | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | update | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | update | v2-mor | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v2_mor_p29_t' caused by | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | spark-sql | update | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p31 | spark-sql | update | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | update | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | update | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v2_mor_p34_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p35 | spark-sql | update | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | update | v2-mor | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v2_mor_p36_t' caused by | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p01 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | update | v3 | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | update | v3 | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | update | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | update | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | update | v3 | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | update | v3 | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | update | v3 | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | update | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | update | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | update | v3 | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | update | v3 | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | update | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | update | v3 | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | update | v3 | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | update | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | update | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | update | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | update | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | update | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | update | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | update | v3 | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v3_p29_t' caused by Schema | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | spark-sql | update | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p31 | spark-sql | update | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | update | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | update | v3 | ok {1,8} | REFUSED Py4JJavaError An error occurred while calling o47.sql. : org.apache.spark.SparkException: | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v3_p34_t' caused by Schema | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p35 | spark-sql | update | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | update | v3 | REFUSED AnalysisException UPDATE operation on table 'ice.ns.spark_sql_update_v3_p36_t' caused by Schema | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p01 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | insert | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | insert | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | insert | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | insert | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | insert | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | insert | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | spark-sql | insert | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | insert | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | insert | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | insert | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | insert | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | insert | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | insert | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | insert | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | insert | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | insert | v2-mor | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | insert | v2-mor | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | insert | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | insert | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | insert | v2-mor | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | insert | v2-mor | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | insert | v2-mor | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | insert | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | insert | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | insert | v2-mor | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | spark-sql | insert | v2-mor | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | insert | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | insert | v2-mor | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | insert | v2-mor | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | insert | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | insert | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | insert | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | insert | v2-mor | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | insert | v2-mor | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | insert | v2-mor | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | insert | v2-mor | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | insert | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | insert | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | insert | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | insert | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | insert | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | insert | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | insert | v3 | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | insert | v3 | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | spark-sql | insert | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | insert | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | insert | v3 | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | insert | v3 | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | insert | v3 | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | insert | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | insert | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | insert | v3 | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | spark-sql | insert | v3 | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | insert | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | insert | v3 | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | insert | v3 | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | insert | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | insert | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | insert | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | insert | v3 | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | insert | v3 | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | insert | v3 | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | insert | v3 | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | insert | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | insert | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | insert | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | insert | v3 | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | insert | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | insert | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | ctas | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | ctas | v2-cow | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | ctas | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | ctas | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | ctas | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | ctas | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | ctas | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | ctas | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | ctas | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | ctas | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | spark-sql | ctas | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | ctas | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | ctas | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | ctas | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | ctas | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | ctas | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | ctas | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | ctas | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | ctas | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | ctas | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | ctas | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | ctas | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | ctas | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | ctas | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | ctas | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | ctas | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | ctas | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | ctas | v2-mor | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | ctas | v2-mor | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | ctas | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | ctas | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | ctas | v2-mor | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | ctas | v2-mor | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | ctas | v2-mor | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | ctas | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | ctas | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | ctas | v2-mor | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | spark-sql | ctas | v2-mor | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | ctas | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | ctas | v2-mor | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | ctas | v2-mor | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | ctas | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | ctas | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | ctas | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | ctas | v2-mor | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | ctas | v2-mor | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | ctas | v2-mor | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | ctas | v2-mor | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | ctas | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | ctas | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | ctas | v2-mor | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | ctas | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | ctas | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | ctas | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | spark-sql | ctas | v3 | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | spark-sql | ctas | v3 | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | ctas | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | spark-sql | ctas | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | spark-sql | ctas | v3 | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | spark-sql | ctas | v3 | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | spark-sql | ctas | v3 | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | spark-sql | ctas | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | spark-sql | ctas | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | spark-sql | ctas | v3 | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | spark-sql | ctas | v3 | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | spark-sql | ctas | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | spark-sql | ctas | v3 | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | spark-sql | ctas | v3 | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | spark-sql | ctas | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | spark-sql | ctas | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | spark-sql | ctas | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | spark-sql | ctas | v3 | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | spark-sql | ctas | v3 | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | spark-sql | ctas | v3 | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | spark-sql | ctas | v3 | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | spark-sql | ctas | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | spark-sql | ctas | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | spark-sql | ctas | v3 | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | spark-sql | ctas | v3 | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | spark-sql | ctas | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | spark-sql | ctas | v3 | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | merge | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | merge | v2-cow | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | merge | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | merge | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | merge | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | merge | v2-cow | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | merge | v2-cow | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | merge | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | merge | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | merge | v2-cow | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | merge | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | merge | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | merge | v2-cow | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | merge | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | merge | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | merge | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | merge | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | merge | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | merge | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | merge | v2-cow | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | merge | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | merge | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p30 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p31 | spark-sql | merge | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | merge | v2-cow | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | merge | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p35 | spark-sql | merge | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | merge | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p01 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | merge | v2-mor | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | merge | v2-mor | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | merge | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | merge | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | merge | v2-mor | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | merge | v2-mor | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | merge | v2-mor | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | merge | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | merge | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | merge | v2-mor | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | merge | v2-mor | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | merge | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | merge | v2-mor | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | merge | v2-mor | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | merge | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | merge | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | merge | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | merge | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | merge | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | merge | v2-mor | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | merge | v2-mor | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | merge | v2-mor | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p30 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p31 | spark-sql | merge | v2-mor | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | merge | v2-mor | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | merge | v2-mor | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p35 | spark-sql | merge | v2-mor | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | merge | v2-mor | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p01 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p02 | spark-sql | merge | v3 | ok {5,6} | ok {5,6} | {5,6} | 1 | EQUAL |
| p03 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p04 | spark-sql | merge | v3 | ok {} | ok {} | {} | 0 | KNOWN-EMPTY-DML, FEWER-COMMIT |
| p05 | spark-sql | merge | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p06 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p07 | spark-sql | merge | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p08 | spark-sql | merge | v3 | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p10 | spark-sql | merge | v3 | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p11 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p12 | spark-sql | merge | v3 | ok {5} | ok {5} | {5} | 1 | EQUAL |
| p13 | spark-sql | merge | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p14 | spark-sql | merge | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p15 | spark-sql | merge | v3 | ok {6} | ok {4,6} | {6} | 1 | WRONG-WRITE |
| p16 | spark-sql | merge | v3 | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 1 | EQUAL |
| p17 | spark-sql | merge | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p18 | spark-sql | merge | v3 | ok {4} | ok {4} | {4} | 1 | EQUAL |
| p19 | spark-sql | merge | v3 | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 1 | EQUAL |
| p20 | spark-sql | merge | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p21 | spark-sql | merge | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p22 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p23 | spark-sql | merge | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | spark-sql | merge | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p25 | spark-sql | merge | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p26 | spark-sql | merge | v3 | ok {7} | ok {7} | {7} | 1 | EQUAL |
| p27 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p28 | spark-sql | merge | v3 | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 1 | EQUAL |
| p29 | spark-sql | merge | v3 | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 1 | EQUAL |
| p30 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p31 | spark-sql | merge | v3 | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 1 | EQUAL |
| p32 | spark-sql | merge | v3 | ok {3} | ok {3} | {3} | 1 | EQUAL |
| p33 | spark-sql | merge | v3 | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 1 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p35 | spark-sql | merge | v3 | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 1 | EQUAL |
| p36 | spark-sql | merge | v3 | ok {1,8} | ok {1,8} | {1,8} | 1 | EQUAL |
| p01 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | filter-str | select | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | filter-str | select | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | filter-str | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | filter-str | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | filter-str | select | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | filter-str | select | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | filter-str | select | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | filter-str | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | filter-str | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | filter-str | select | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-READ |
| p16 | filter-str | select | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | filter-str | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | filter-str | select | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | filter-str | select | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | filter-str | select | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | filter-str | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | filter-str | select | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | filter-str | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | filter-str | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | filter-str | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | filter-str | select | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | filter-str | select | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | filter-str | select | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | filter-str | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | filter-str | select | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | filter-str | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | filter-str | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | filter-str | insert | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | filter-str | insert | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | filter-str | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | filter-str | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | filter-str | insert | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | filter-str | insert | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | filter-str | insert | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | filter-str | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | filter-str | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | filter-str | insert | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | filter-str | insert | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | filter-str | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | filter-str | insert | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | filter-str | insert | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | filter-str | insert | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | filter-str | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | filter-str | insert | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | filter-str | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | filter-str | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | filter-str | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p28 | filter-str | insert | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | filter-str | insert | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | filter-str | insert | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | filter-str | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | filter-str | insert | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | filter-str | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | filter-str | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | f-expr | select | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | f-expr | select | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | f-expr | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | f-expr | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | f-expr | select | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | f-expr | select | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | f-expr | select | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | f-expr | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | f-expr | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | f-expr | select | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-READ |
| p16 | f-expr | select | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | f-expr | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | f-expr | select | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | f-expr | select | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | f-expr | select | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | f-expr | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | f-expr | select | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | f-expr | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | f-expr | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | f-expr | select | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | f-expr | select | v2-cow | REFUSED AnalysisException type_coercion caused by Schema error: No field named st.x. Valid fields are | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p28 | f-expr | select | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | f-expr | select | v2-cow | REFUSED AnalysisException Optimizer rule 'simplify_expressions' failed caused by Schema error: No field | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | f-expr | select | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | f-expr | select | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | f-expr | select | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | f-expr | select | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | f-expr | select | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p02 | f-expr | insert | v2-cow | ok {5,6} | ok {5,6} | {5,6} | 0 | EQUAL |
| p03 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p04 | f-expr | insert | v2-cow | ok {} | ok {} | {} | 0 | EQUAL |
| p05 | f-expr | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p06 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p07 | f-expr | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p08 | f-expr | insert | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | 0 | BOTH-REFUSED |
| p09 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p10 | f-expr | insert | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p11 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p12 | f-expr | insert | v2-cow | ok {5} | ok {5} | {5} | 0 | EQUAL |
| p13 | f-expr | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p14 | f-expr | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p15 | f-expr | insert | v2-cow | ok {6} | ok {4,6} | {6} | 0 | WRONG-WRITE |
| p16 | f-expr | insert | v2-cow | ok {1,3,5,8} | ok {1,3,5,8} | {1,3,5,8} | 0 | EQUAL |
| p17 | f-expr | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p18 | f-expr | insert | v2-cow | ok {4} | ok {4} | {4} | 0 | EQUAL |
| p19 | f-expr | insert | v2-cow | ok {1,4,8} | ok {1,4,8} | {1,4,8} | 0 | EQUAL |
| p20 | f-expr | insert | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p21 | f-expr | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p22 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p23 | f-expr | insert | v2-cow | ok {2,3,4,5,6,7} | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | f-expr | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p25 | f-expr | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p26 | f-expr | insert | v2-cow | ok {7} | ok {7} | {7} | 0 | EQUAL |
| p27 | f-expr | insert | v2-cow | REFUSED AnalysisException type_coercion caused by Schema error: No field named st.x. Valid fields are | ok {1,8} | {1,8} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p28 | f-expr | insert | v2-cow | ok {2,4,5,6,7} | ok {2,4,5,6,7} | {2,4,5,6,7} | 0 | EQUAL |
| p29 | f-expr | insert | v2-cow | REFUSED AnalysisException Optimizer rule 'simplify_expressions' failed caused by Schema error: No field | ok {2,3,4,5,6,7} | {2,3,4,5,6,7} | 0 | REFUSED-WHERE-SPARK-ACCEPTED |
| p30 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p31 | f-expr | insert | v2-cow | ok {1,6,8} | ok {1,6,8} | {1,6,8} | 0 | EQUAL |
| p32 | f-expr | insert | v2-cow | ok {3} | ok {3} | {3} | 0 | EQUAL |
| p33 | f-expr | insert | v2-cow | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has | {1,8} | 0 | ACCEPTED-WHERE-SPARK-REFUSED |
| p34 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p35 | f-expr | insert | v2-cow | ok {2,3,4,7} | ok {2,3,4,7} | {2,3,4,7} | 0 | EQUAL |
| p36 | f-expr | insert | v2-cow | ok {1,8} | ok {1,8} | {1,8} | 0 | EQUAL |
| p01 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p02 | native | select | v2-cow | ok {5,6} | - | {5,6} | 0 | EQUAL |
| p03 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p04 | native | select | v2-cow | ok {} | - | {} | 0 | EQUAL |
| p05 | native | select | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 0 | EQUAL |
| p06 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p07 | native | select | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 0 | EQUAL |
| p08 | native | select | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | - | 0 | NATIVE-REFUSED |
| p09 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p10 | native | select | v2-cow | ok {5} | - | {5} | 0 | EQUAL |
| p11 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p12 | native | select | v2-cow | ok {5} | - | {5} | 0 | EQUAL |
| p13 | native | select | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p14 | native | select | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p15 | native | select | v2-cow | ok {6} | - | {6} | 0 | EQUAL |
| p16 | native | select | v2-cow | ok {1,3,5,8} | - | {1,3,5,8} | 0 | EQUAL |
| p17 | native | select | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p18 | native | select | v2-cow | ok {4} | - | {4} | 0 | EQUAL |
| p19 | native | select | v2-cow | ok {1,4,8} | - | {1,4,8} | 0 | EQUAL |
| p20 | native | select | v2-cow | ok {1,6,8} | - | {1,6,8} | 0 | EQUAL |
| p21 | native | select | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p22 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p23 | native | select | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | native | select | v2-cow | ok {7} | - | {7} | 0 | EQUAL |
| p25 | native | select | v2-cow | ok {7} | - | {7} | 0 | EQUAL |
| p26 | native | select | v2-cow | ok {7} | - | {7} | 0 | EQUAL |
| p27 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p28 | native | select | v2-cow | ok {2,4,5,6,7} | - | {2,4,5,6,7} | 0 | EQUAL |
| p29 | native | select | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p31 | native | select | v2-cow | ok {1,6,8} | - | {1,6,8} | 0 | EQUAL |
| p32 | native | select | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p33 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p34 | native | select | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| p35 | native | select | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 0 | EQUAL |
| p36 | native | select | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p01 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p02 | native | delete | v2-cow | ok {5,6} | - | {5,6} | 1 | EQUAL |
| p03 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p04 | native | delete | v2-cow | ok {} | - | {} | 0 | EQUAL |
| p05 | native | delete | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 1 | EQUAL |
| p06 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p07 | native | delete | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 1 | EQUAL |
| p08 | native | delete | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | - | 0 | NATIVE-REFUSED |
| p09 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p10 | native | delete | v2-cow | ok {5} | - | {5} | 1 | EQUAL |
| p11 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p12 | native | delete | v2-cow | ok {5} | - | {5} | 1 | EQUAL |
| p13 | native | delete | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p14 | native | delete | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p15 | native | delete | v2-cow | ok {6} | - | {6} | 1 | EQUAL |
| p16 | native | delete | v2-cow | ok {1,3,5,8} | - | {1,3,5,8} | 1 | EQUAL |
| p17 | native | delete | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p18 | native | delete | v2-cow | ok {4} | - | {4} | 1 | EQUAL |
| p19 | native | delete | v2-cow | ok {1,4,8} | - | {1,4,8} | 1 | EQUAL |
| p20 | native | delete | v2-cow | ok {1,6,8} | - | {1,6,8} | 1 | EQUAL |
| p21 | native | delete | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p22 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p23 | native | delete | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | native | delete | v2-cow | ok {7} | - | {7} | 1 | EQUAL |
| p25 | native | delete | v2-cow | ok {7} | - | {7} | 1 | EQUAL |
| p26 | native | delete | v2-cow | ok {7} | - | {7} | 1 | EQUAL |
| p27 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.native_delete_v2_cow_p27_t' caused by Schema | - | {1,8} | 0 | NATIVE-REFUSED |
| p28 | native | delete | v2-cow | ok {2,4,5,6,7} | - | {2,4,5,6,7} | 1 | EQUAL |
| p29 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.native_delete_v2_cow_p29_t' caused by Schema | - | {2,3,4,5,6,7} | 0 | NATIVE-REFUSED |
| p30 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p31 | native | delete | v2-cow | ok {1,6,8} | - | {1,6,8} | 1 | EQUAL |
| p32 | native | delete | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p33 | native | delete | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p34 | native | delete | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| p35 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.native_delete_v2_cow_p35_t' caused by Schema | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p36 | native | delete | v2-cow | REFUSED AnalysisException DELETE operation on table 'ice.ns.native_delete_v2_cow_p36_t' caused by Schema | - | {1,8} | 0 | NATIVE-REFUSED |
| p01 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p02 | native | update | v2-cow | ok {5,6} | - | {5,6} | 1 | EQUAL |
| p03 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p04 | native | update | v2-cow | ok {} | - | {} | 0 | EQUAL |
| p05 | native | update | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 1 | EQUAL |
| p06 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p07 | native | update | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 1 | EQUAL |
| p08 | native | update | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | - | 0 | NATIVE-REFUSED |
| p09 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p10 | native | update | v2-cow | ok {5} | - | {5} | 1 | EQUAL |
| p11 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p12 | native | update | v2-cow | ok {5} | - | {5} | 1 | EQUAL |
| p13 | native | update | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p14 | native | update | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p15 | native | update | v2-cow | ok {6} | - | {6} | 1 | EQUAL |
| p16 | native | update | v2-cow | ok {1,3,5,8} | - | {1,3,5,8} | 1 | EQUAL |
| p17 | native | update | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p18 | native | update | v2-cow | ok {4} | - | {4} | 1 | EQUAL |
| p19 | native | update | v2-cow | ok {1,4,8} | - | {1,4,8} | 1 | EQUAL |
| p20 | native | update | v2-cow | ok {1,6,8} | - | {1,6,8} | 1 | EQUAL |
| p21 | native | update | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p22 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p23 | native | update | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | 1 | EQUAL |
| p24 | native | update | v2-cow | ok {7} | - | {7} | 1 | EQUAL |
| p25 | native | update | v2-cow | ok {7} | - | {7} | 1 | EQUAL |
| p26 | native | update | v2-cow | ok {7} | - | {7} | 1 | EQUAL |
| p27 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.native_update_v2_cow_p27_t' caused by Schema | - | {1,8} | 0 | NATIVE-REFUSED |
| p28 | native | update | v2-cow | ok {2,4,5,6,7} | - | {2,4,5,6,7} | 1 | EQUAL |
| p29 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.native_update_v2_cow_p29_t' caused by Schema | - | {2,3,4,5,6,7} | 0 | NATIVE-REFUSED |
| p30 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p31 | native | update | v2-cow | ok {1,6,8} | - | {1,6,8} | 1 | EQUAL |
| p32 | native | update | v2-cow | ok {3} | - | {3} | 1 | EQUAL |
| p33 | native | update | v2-cow | ok {1,8} | - | {1,8} | 1 | EQUAL |
| p34 | native | update | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| p35 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.native_update_v2_cow_p35_t' caused by Schema | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p36 | native | update | v2-cow | REFUSED AnalysisException UPDATE operation on table 'ice.ns.native_update_v2_cow_p36_t' caused by Schema | - | {1,8} | 0 | NATIVE-REFUSED |
| p01 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p02 | native | insert | v2-cow | ok {5,6} | - | {5,6} | 0 | EQUAL |
| p03 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p04 | native | insert | v2-cow | ok {} | - | {} | 0 | EQUAL |
| p05 | native | insert | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 0 | EQUAL |
| p06 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p07 | native | insert | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 0 | EQUAL |
| p08 | native | insert | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | - | 0 | NATIVE-REFUSED |
| p09 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p10 | native | insert | v2-cow | ok {5} | - | {5} | 0 | EQUAL |
| p11 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p12 | native | insert | v2-cow | ok {5} | - | {5} | 0 | EQUAL |
| p13 | native | insert | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p14 | native | insert | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p15 | native | insert | v2-cow | ok {6} | - | {6} | 0 | EQUAL |
| p16 | native | insert | v2-cow | ok {1,3,5,8} | - | {1,3,5,8} | 0 | EQUAL |
| p17 | native | insert | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p18 | native | insert | v2-cow | ok {4} | - | {4} | 0 | EQUAL |
| p19 | native | insert | v2-cow | ok {1,4,8} | - | {1,4,8} | 0 | EQUAL |
| p20 | native | insert | v2-cow | ok {1,6,8} | - | {1,6,8} | 0 | EQUAL |
| p21 | native | insert | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p22 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p23 | native | insert | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | 0 | EQUAL |
| p24 | native | insert | v2-cow | ok {7} | - | {7} | 0 | EQUAL |
| p25 | native | insert | v2-cow | ok {7} | - | {7} | 0 | EQUAL |
| p26 | native | insert | v2-cow | ok {7} | - | {7} | 0 | EQUAL |
| p27 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p28 | native | insert | v2-cow | ok {2,4,5,6,7} | - | {2,4,5,6,7} | 0 | EQUAL |
| p29 | native | insert | v2-cow | ok {2,3,4,5,6,7} | - | {2,3,4,5,6,7} | 0 | EQUAL |
| p30 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p31 | native | insert | v2-cow | ok {1,6,8} | - | {1,6,8} | 0 | EQUAL |
| p32 | native | insert | v2-cow | ok {3} | - | {3} | 0 | EQUAL |
| p33 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p34 | native | insert | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| p35 | native | insert | v2-cow | ok {2,3,4,7} | - | {2,3,4,7} | 0 | EQUAL |
| p36 | native | insert | v2-cow | ok {1,8} | - | {1,8} | 0 | EQUAL |
| p01 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p02 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {5,6} | 0 | NATIVE-REFUSED |
| p03 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p04 | native | ctas | v2-cow | ok {} | - | {} | 0 | EQUAL |
| p05 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p06 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p07 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p08 | native | ctas | v2-cow | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | - | - | 0 | NATIVE-REFUSED |
| p09 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p10 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {5} | 0 | NATIVE-REFUSED |
| p11 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p12 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {5} | 0 | NATIVE-REFUSED |
| p13 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {3} | 0 | NATIVE-REFUSED |
| p14 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {3} | 0 | NATIVE-REFUSED |
| p15 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {6} | 0 | NATIVE-REFUSED |
| p16 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,3,5,8} | 0 | NATIVE-REFUSED |
| p17 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {3} | 0 | NATIVE-REFUSED |
| p18 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {4} | 0 | NATIVE-REFUSED |
| p19 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,4,8} | 0 | NATIVE-REFUSED |
| p20 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,6,8} | 0 | NATIVE-REFUSED |
| p21 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {3} | 0 | NATIVE-REFUSED |
| p22 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p23 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {2,3,4,5,6,7} | 0 | NATIVE-REFUSED |
| p24 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {7} | 0 | NATIVE-REFUSED |
| p25 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {7} | 0 | NATIVE-REFUSED |
| p26 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {7} | 0 | NATIVE-REFUSED |
| p27 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p28 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {2,4,5,6,7} | 0 | NATIVE-REFUSED |
| p29 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {2,3,4,5,6,7} | 0 | NATIVE-REFUSED |
| p30 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p31 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,6,8} | 0 | NATIVE-REFUSED |
| p32 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {3} | 0 | NATIVE-REFUSED |
| p33 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p34 | native | ctas | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| p35 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p36 | native | ctas | v2-cow | REFUSED PySparkException Unexpected => register_table does not support tables with data. | - | {1,8} | 0 | NATIVE-REFUSED |
| p01 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p02 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {5,6} | 0 | NATIVE-REFUSED |
| p03 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p04 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {} | 0 | NATIVE-REFUSED |
| p05 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p06 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p07 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p08 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | - | 0 | NATIVE-REFUSED |
| p09 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p10 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {5} | 0 | NATIVE-REFUSED |
| p11 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p12 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {5} | 0 | NATIVE-REFUSED |
| p13 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {3} | 0 | NATIVE-REFUSED |
| p14 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {3} | 0 | NATIVE-REFUSED |
| p15 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {6} | 0 | NATIVE-REFUSED |
| p16 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,3,5,8} | 0 | NATIVE-REFUSED |
| p17 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {3} | 0 | NATIVE-REFUSED |
| p18 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {4} | 0 | NATIVE-REFUSED |
| p19 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,4,8} | 0 | NATIVE-REFUSED |
| p20 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,6,8} | 0 | NATIVE-REFUSED |
| p21 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {3} | 0 | NATIVE-REFUSED |
| p22 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p23 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,5,6,7} | 0 | NATIVE-REFUSED |
| p24 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {7} | 0 | NATIVE-REFUSED |
| p25 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {7} | 0 | NATIVE-REFUSED |
| p26 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {7} | 0 | NATIVE-REFUSED |
| p27 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p28 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,4,5,6,7} | 0 | NATIVE-REFUSED |
| p29 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,5,6,7} | 0 | NATIVE-REFUSED |
| p30 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p31 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,6,8} | 0 | NATIVE-REFUSED |
| p32 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {3} | 0 | NATIVE-REFUSED |
| p33 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
| p34 | native | merge | v2-cow | NOT-EXPRESSIBLE   | - | - | - | NOT-EXPRESSIBLE |
| p35 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {2,3,4,7} | 0 | NATIVE-REFUSED |
| p36 | native | merge | v2-cow | REFUSED UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO | - | {1,8} | 0 | NATIVE-REFUSED |
