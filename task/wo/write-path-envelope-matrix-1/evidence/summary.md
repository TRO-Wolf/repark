# Summary

RePark cells: 936; Spark cells: 720

## Label totals

- `ACCEPTED-WHERE-SPARK-REFUSED`: 23
- `BOTH-REFUSED`: 20
- `EQUAL`: 755
- `FEWER-COMMIT`: 9
- `KNOWN-EMPTY-DML`: 9
- `NATIVE-REFUSED`: 81
- `NOT-EXPRESSIBLE`: 6
- `REFUSED-WHERE-SPARK-ACCEPTED`: 22
- `WRONG-READ`: 3
- `WRONG-WRITE`: 17

## Counts per door and seat

| door | seat | cells | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | EQUAL | FEWER-COMMIT | KNOWN-EMPTY-DML | NATIVE-REFUSED | NOT-EXPRESSIBLE | REFUSED-WHERE-SPARK-ACCEPTED | WRONG-READ | WRONG-WRITE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-sql | select | 36 | 1 | 1 | 33 | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| spark-sql | delete | 108 | 6 | 3 | 87 | 0 | 0 | 0 | 0 | 9 | 0 | 3 |
| spark-sql | update | 108 | 3 | 3 | 87 | 3 | 3 | 0 | 0 | 9 | 0 | 3 |
| spark-sql | insert | 108 | 3 | 3 | 99 | 0 | 0 | 0 | 0 | 0 | 0 | 3 |
| spark-sql | ctas | 108 | 3 | 3 | 96 | 3 | 3 | 0 | 0 | 0 | 0 | 3 |
| spark-sql | merge | 108 | 3 | 3 | 96 | 3 | 3 | 0 | 0 | 0 | 0 | 3 |
| filter-str | select | 36 | 1 | 1 | 33 | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| filter-str | insert | 36 | 1 | 1 | 33 | 0 | 0 | 0 | 0 | 0 | 0 | 1 |
| f-expr | select | 36 | 1 | 1 | 31 | 0 | 0 | 0 | 0 | 2 | 1 | 0 |
| f-expr | insert | 36 | 1 | 1 | 31 | 0 | 0 | 0 | 0 | 2 | 0 | 1 |
| native | select | 36 | 0 | 0 | 34 | 0 | 0 | 1 | 1 | 0 | 0 | 0 |
| native | delete | 36 | 0 | 0 | 30 | 0 | 0 | 5 | 1 | 0 | 0 | 0 |
| native | update | 36 | 0 | 0 | 30 | 0 | 0 | 5 | 1 | 0 | 0 | 0 |
| native | insert | 36 | 0 | 0 | 34 | 0 | 0 | 1 | 1 | 0 | 0 | 0 |
| native | ctas | 36 | 0 | 0 | 1 | 0 | 0 | 34 | 1 | 0 | 0 | 0 |
| native | merge | 36 | 0 | 0 | 0 | 0 | 0 | 35 | 1 | 0 | 0 | 0 |

## Non-EQUAL labels per predicate

| pid | predicate | labels |
|---|---|---|
| p01 | `i = 1` | NATIVE-REFUSED 2 |
| p02 | `i <> 1` | NATIVE-REFUSED 2 |
| p03 | `i IN (1, NULL)` | NATIVE-REFUSED 2 |
| p04 | `i NOT IN (1, NULL)` | FEWER-COMMIT 9, KNOWN-EMPTY-DML 9, NATIVE-REFUSED 1 |
| p05 | `i IS NULL` | NATIVE-REFUSED 2 |
| p06 | `i BETWEEN 1 AND 2` | NATIVE-REFUSED 2 |
| p07 | `i <=> NULL` | NATIVE-REFUSED 2 |
| p08 | `i + 1 > 0` | BOTH-REFUSED 20, NATIVE-REFUSED 6 |
| p09 | `CAST(i AS STRING) = '1'` | NATIVE-REFUSED 2 |
| p10 | `l = 9223372036854775807` | NATIVE-REFUSED 2 |
| p11 | `dec = 1.10` | NATIVE-REFUSED 2 |
| p12 | `dec > 1.1` | NATIVE-REFUSED 2 |
| p13 | `d = CAST('NaN' AS DOUBLE)` | NATIVE-REFUSED 2 |
| p14 | `isnan(d)` | NATIVE-REFUSED 2 |
| p15 | `d = 0.0` | NATIVE-REFUSED 2, WRONG-READ 3, WRONG-WRITE 17 |
| p16 | `d > 0` | NATIVE-REFUSED 2 |
| p17 | `s = ''` | NATIVE-REFUSED 2 |
| p18 | `s = 'a '` | NATIVE-REFUSED 2 |
| p19 | `s LIKE 'a%'` | NATIVE-REFUSED 2 |
| p20 | `upper(s) = 'A'` | NATIVE-REFUSED 2 |
| p21 | `length(s) = 0` | NATIVE-REFUSED 2 |
| p22 | `b` | NATIVE-REFUSED 2 |
| p23 | `b IS NOT TRUE` | NATIVE-REFUSED 2 |
| p24 | `dt = DATE '1970-01-01'` | NATIVE-REFUSED 2 |
| p25 | `ts > TIMESTAMP '2026-01-01 00:00:00'` | NATIVE-REFUSED 2 |
| p26 | `ntz = TIMESTAMP_NTZ '2026-03-08 02:30:00'` | NATIVE-REFUSED 2 |
| p27 | `st.x = 1` | NATIVE-REFUSED 4, REFUSED-WHERE-SPARK-ACCEPTED 2 |
| p28 | `st IS NULL` | NATIVE-REFUSED 2 |
| p29 | `st.y IS NULL` | NATIVE-REFUSED 4, REFUSED-WHERE-SPARK-ACCEPTED 8 |
| p30 | `st = named_struct('x', 1, 'y', 'a')` | ACCEPTED-WHERE-SPARK-REFUSED 3, NATIVE-REFUSED 2 |
| p31 | `array_contains(arr, 1)` | NATIVE-REFUSED 2 |
| p32 | `size(arr) = 0` | NATIVE-REFUSED 2 |
| p33 | `element_at(arr, 1) = 1` | ACCEPTED-WHERE-SPARK-REFUSED 20, NATIVE-REFUSED 2 |
| p34 | `m['k'] = 1` | NOT-EXPRESSIBLE 6, REFUSED-WHERE-SPARK-ACCEPTED 6 |
| p35 | `coalesce(i, 0) = 0` | NATIVE-REFUSED 4 |
| p36 | `CASE WHEN i = 1 THEN true END` | NATIVE-REFUSED 4, REFUSED-WHERE-SPARK-ACCEPTED 6 |

## Predicates Spark folds to an empty relation

`p04`

## Native select against Spark select (information, not a label)

| pid | native spelling | native | Spark |
|---|---|---|---|
| p08 | `i + 1 > 0` | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and | REFUSED ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and |
| p15 | `d = 0.0` | ok {6} | ok {4,6} |
| p33 | `arr[1] = 1` | ok {1,8} | REFUSED ArrayIndexOutOfBoundsException [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 1 is out of bounds. The array has |
| p34 | `None` | NOT-EXPRESSIBLE   | ok {1,8} |
