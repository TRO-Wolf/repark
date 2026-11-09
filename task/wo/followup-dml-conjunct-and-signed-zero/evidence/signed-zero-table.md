# Signed zero, reads and writes against the Spark oracle

## Label totals (RePark cells)

- `ACCEPTED-WHERE-SPARK-REFUSED`: 6
- `EQUAL`: 30
- `ORDER-DIFFER`: 8
- `REFUSED-WHERE-SPARK-ACCEPTED`: 4
- `SIGN-ONLY`: 8
- `WRONG-READ`: 130
- `WRONG-WRITE`: 12

## Reads: Spark result against RePark result (v2-cow)

| op | variant | Spark spark-sql | RePark spark-sql | RePark native | Spark filter-str | RePark filter-str | Spark f-expr | RePark f-expr | labels |
|---|---|---|---|---|---|---|---|---|---|
| c1 | t | (4); (6) | (6) | (6) | (4); (6) | (6) | (4); (6) | (6) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c2 | t | (4); (6) | (6) | (4) | (4); (6) | (6) | (4); (6) | (4) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c3 | t | (4); (6) | REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(30, 15). | (6) | (4); (6) | REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(30, 15). | (4); (6) | (6) | spark-sql: REFUSED-WHERE-SPARK-ACCEPTED; native: WRONG-READ; filter-str: REFUSED-WHERE-SPARK-ACCEPTED; f-expr: WRONG-READ |
| c4 | t | (4); (6) | (6) | (6) | (4); (6) | (6) | (4); (6) | (6) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c5 | t | (1); (3); (5); (8) | (1); (3); (4); (5); (8) | (1); (3); (4); (5); (8) | (1); (3); (5); (8) | (1); (3); (4); (5); (8) | (1); (3); (5); (8) | (1); (3); (4); (5); (8) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c6 | t | none | (4) | (4) | none | (4) | none | (4) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c7 | t | (4); (6) | (4); (6) | (4); (6) | (4); (6) | (4); (6) | (4); (6) | (4); (6) | spark-sql: EQUAL; native: EQUAL; filter-str: EQUAL; f-expr: EQUAL |
| c1 | tf | (4); (6) | (6) | (6) | (4); (6) | (6) | (4); (6) | (6) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c2 | tf | (4); (6) | (6) | (4) | (4); (6) | (6) | (4); (6) | (4) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c3 | tf | (4); (6) | REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(14, 7). | (6) | (4); (6) | REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(14, 7). | (4); (6) | (6) | spark-sql: REFUSED-WHERE-SPARK-ACCEPTED; native: WRONG-READ; filter-str: REFUSED-WHERE-SPARK-ACCEPTED; f-expr: WRONG-READ |
| c4 | tf | (4); (6) | (6) | (6) | (4); (6) | (6) | (4); (6) | (6) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c5 | tf | (1); (3); (5); (8) | (1); (3); (4); (5); (8) | (1); (3); (4); (5); (8) | (1); (3); (5); (8) | (1); (3); (4); (5); (8) | (1); (3); (5); (8) | (1); (3); (4); (5); (8) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c6 | tf | none | (4) | (4) | none | (4) | none | (4) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c7 | tf | (4); (6) | (4); (6) | (4); (6) | (4); (6) | (4); (6) | (4); (6) | (4); (6) | spark-sql: EQUAL; native: EQUAL; filter-str: EQUAL; f-expr: EQUAL |
| c1 | u | (10); (11) | (10) | (10) | (10); (11) | (10) | (10); (11) | (10) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c2 | u | (10); (11) | (10) | (11) | (10); (11) | (10) | (10); (11) | (11) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c3 | u | (10); (11) | (10); (11) | (10) | (10); (11) | (10); (11) | (10); (11) | (10) | spark-sql: EQUAL; native: WRONG-READ; filter-str: EQUAL; f-expr: WRONG-READ |
| c4 | u | (10); (11) | (10) | (10) | (10); (11) | (10) | (10); (11) | (10) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c5 | u | none | (11) | (11) | none | (11) | none | (11) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c6 | u | none | (11) | (11) | none | (11) | none | (11) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c7 | u | (10); (11) | (10); (11) | (10); (11) | (10); (11) | (10); (11) | (10); (11) | (10); (11) | spark-sql: EQUAL; native: EQUAL; filter-str: EQUAL; f-expr: EQUAL |
| c1 | uf | (10); (11) | (10) | (10) | (10); (11) | (10) | (10); (11) | (10) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c2 | uf | (10); (11) | (10) | (11) | (10); (11) | (10) | (10); (11) | (11) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c3 | uf | (10); (11) | (10); (11) | (10) | (10); (11) | (10); (11) | (10); (11) | (10) | spark-sql: EQUAL; native: WRONG-READ; filter-str: EQUAL; f-expr: WRONG-READ |
| c4 | uf | (10); (11) | (10) | (10) | (10); (11) | (10) | (10); (11) | (10) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c5 | uf | none | (11) | (11) | none | (11) | none | (11) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c6 | uf | none | (11) | (11) | none | (11) | none | (11) | spark-sql: WRONG-READ; native: WRONG-READ; filter-str: WRONG-READ; f-expr: WRONG-READ |
| c7 | uf | (10); (11) | (10); (11) | (10); (11) | (10); (11) | (10); (11) | (10); (11) | (10); (11) | spark-sql: EQUAL; native: EQUAL; filter-str: EQUAL; f-expr: EQUAL |
| grp | t | (0.0,2); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| dst | t | (0.0); (1.5); (Infinity); (NULL); (NaN) | (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN) | (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| ord | t | (2,NULL); (7,NULL); (6,0.0); (4,-0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | (2,NULL); (7,NULL); (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN); (2,NULL); (7,NULL) | - | - | - | - | spark-sql: ORDER-DIFFER; native: ORDER-DIFFER |
| cst | t | (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,Infinity); (6,0.0); (7,NULL); (8,1.5) | (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,Infinity); (6,0.0); (7,NULL); (8,1.5) | (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,inf); (6,0.0); (7,NULL); (8,1.5) | - | - | - | - | spark-sql: EQUAL; native: WRONG-READ |
| mxmn | t | (-0.0,-0.0) | (0.0,-0.0) | (0.0,-0.0) | - | - | - | - | spark-sql: SIGN-ONLY, WRONG-READ; native: SIGN-ONLY, WRONG-READ |
| win | t | (1,2); (2,2); (3,1); (4,2); (5,1); (6,2); (7,2); (8,2) | (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2) | (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| grp | tf | (0.0,2); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| dst | tf | (0.0); (1.5); (Infinity); (NULL); (NaN) | (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN) | (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| ord | tf | (2,NULL); (7,NULL); (6,0.0); (4,-0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | (2,NULL); (7,NULL); (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN); (2,NULL); (7,NULL) | - | - | - | - | spark-sql: ORDER-DIFFER; native: ORDER-DIFFER |
| cst | tf | (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,Infinity); (6,0.0); (7,NULL); (8,1.5) | (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,Infinity); (6,0.0); (7,NULL); (8,1.5) | (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,inf); (6,0.0); (7,NULL); (8,1.5) | - | - | - | - | spark-sql: EQUAL; native: WRONG-READ |
| mxmn | tf | (-0.0,-0.0) | (0.0,-0.0) | (0.0,-0.0) | - | - | - | - | spark-sql: SIGN-ONLY, WRONG-READ; native: SIGN-ONLY, WRONG-READ |
| win | tf | (1,2); (2,2); (3,1); (4,2); (5,1); (6,2); (7,2); (8,2) | (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2) | (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| grp | u | (0.0,2) | (-0.0,1); (0.0,1) | (-0.0,1); (0.0,1) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| dst | u | (0.0) | (-0.0); (0.0) | (-0.0); (0.0) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| ord | u | (10,0.0); (11,-0.0) | (11,-0.0); (10,0.0) | (11,-0.0); (10,0.0) | - | - | - | - | spark-sql: ORDER-DIFFER; native: ORDER-DIFFER |
| cst | u | (10,0.0); (11,-0.0) | (10,0.0); (11,-0.0) | (10,0.0); (11,-0.0) | - | - | - | - | spark-sql: EQUAL; native: EQUAL |
| mxmn | u | (0.0,0.0) | (0.0,-0.0) | (0.0,-0.0) | - | - | - | - | spark-sql: SIGN-ONLY, WRONG-READ; native: SIGN-ONLY, WRONG-READ |
| win | u | (10,2); (11,2) | (10,1); (11,1) | (10,1); (11,1) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| grp | uf | (0.0,2) | (-0.0,1); (0.0,1) | (-0.0,1); (0.0,1) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| dst | uf | (0.0) | (-0.0); (0.0) | (-0.0); (0.0) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| ord | uf | (10,0.0); (11,-0.0) | (11,-0.0); (10,0.0) | (11,-0.0); (10,0.0) | - | - | - | - | spark-sql: ORDER-DIFFER; native: ORDER-DIFFER |
| cst | uf | (10,0.0); (11,-0.0) | (10,0.0); (11,-0.0) | (10,0.0); (11,-0.0) | - | - | - | - | spark-sql: EQUAL; native: EQUAL |
| mxmn | uf | (0.0,0.0) | (0.0,-0.0) | (0.0,-0.0) | - | - | - | - | spark-sql: SIGN-ONLY, WRONG-READ; native: SIGN-ONLY, WRONG-READ |
| win | uf | (10,2); (11,2) | (10,1); (11,1) | (10,1); (11,1) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| jn | t+u | (4,10); (4,11); (6,10); (6,11) | (4,11); (6,10) | (4,11); (6,10) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| isc | t+u | (0.0) | (-0.0); (0.0) | (-0.0); (0.0) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| exc | t+u | (1.5); (Infinity); (NULL); (NaN) | (1.5); (Infinity); (NULL); (NaN) | (1.5); (Infinity); (NULL); (NaN) | - | - | - | - | spark-sql: EQUAL; native: EQUAL |
| jn | tf+uf | (4,10); (4,11); (6,10); (6,11) | (4,11); (6,10) | (4,11); (6,10) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| isc | tf+uf | (0.0) | (-0.0); (0.0) | (-0.0); (0.0) | - | - | - | - | spark-sql: WRONG-READ; native: WRONG-READ |
| exc | tf+uf | (1.5); (Infinity); (NULL); (NaN) | (1.5); (Infinity); (NULL); (NaN) | (1.5); (Infinity); (NULL); (NaN) | - | - | - | - | spark-sql: EQUAL; native: EQUAL |

## Writes: spark-sql door, three kinds (post-state rows of t, ordered by id)

| op | variant | kind | Spark | RePark | labels |
|---|---|---|---|---|---|
| m1 | t+u | v2-cow | REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | ACCEPTED-WHERE-SPARK-REFUSED |
| m1 | tf+uf | v2-cow | REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | ACCEPTED-WHERE-SPARK-REFUSED |
| m2 | t | v2-cow | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m2 | tf | v2-cow | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m3 | t | v2-cow | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m3 | tf | v2-cow | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m1 | t+u | v2-mor | REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | ACCEPTED-WHERE-SPARK-REFUSED |
| m1 | tf+uf | v2-mor | REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | ACCEPTED-WHERE-SPARK-REFUSED |
| m2 | t | v2-mor | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m2 | tf | v2-mor | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m3 | t | v2-mor | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m3 | tf | v2-mor | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m1 | t+u | v3 | REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | ACCEPTED-WHERE-SPARK-REFUSED |
| m1 | tf+uf | v3 | REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | ACCEPTED-WHERE-SPARK-REFUSED |
| m2 | t | v3 | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m2 | tf | v3 | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m3 | t | v3 | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |
| m3 | tf | v3 | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | WRONG-WRITE |

## Finding lines (one line each)

- `spark-sql|c1|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `spark-sql|c2|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `spark-sql|c3|t|v2-cow` | REFUSED-WHERE-SPARK-ACCEPTED | Spark: (4); (6) | RePark: REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(30, 15).
- `spark-sql|c4|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `spark-sql|c5|t|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `spark-sql|c6|t|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `spark-sql|c1|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `spark-sql|c2|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `spark-sql|c3|tf|v2-cow` | REFUSED-WHERE-SPARK-ACCEPTED | Spark: (4); (6) | RePark: REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(14, 7).
- `spark-sql|c4|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `spark-sql|c5|tf|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `spark-sql|c6|tf|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `spark-sql|c1|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `spark-sql|c2|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `spark-sql|c4|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `spark-sql|c5|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `spark-sql|c6|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `spark-sql|c1|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `spark-sql|c2|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `spark-sql|c4|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `spark-sql|c5|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `spark-sql|c6|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `filter-str|c1|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `filter-str|c2|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `filter-str|c3|t|v2-cow` | REFUSED-WHERE-SPARK-ACCEPTED | Spark: (4); (6) | RePark: REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(30, 15).
- `filter-str|c4|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `filter-str|c5|t|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `filter-str|c6|t|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `filter-str|c1|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `filter-str|c2|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `filter-str|c3|tf|v2-cow` | REFUSED-WHERE-SPARK-ACCEPTED | Spark: (4); (6) | RePark: REFUSED PySparkException Arrow error: Cast error: Cannot cast to Decimal128(14, 7).
- `filter-str|c4|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `filter-str|c5|tf|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `filter-str|c6|tf|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `filter-str|c1|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `filter-str|c2|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `filter-str|c4|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `filter-str|c5|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `filter-str|c6|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `filter-str|c1|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `filter-str|c2|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `filter-str|c4|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `filter-str|c5|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `filter-str|c6|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `f-expr|c1|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `f-expr|c2|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (4)
- `f-expr|c3|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `f-expr|c4|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `f-expr|c5|t|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `f-expr|c6|t|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `f-expr|c1|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `f-expr|c2|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (4)
- `f-expr|c3|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `f-expr|c4|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `f-expr|c5|tf|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `f-expr|c6|tf|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `f-expr|c1|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `f-expr|c2|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (11)
- `f-expr|c3|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `f-expr|c4|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `f-expr|c5|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `f-expr|c6|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `f-expr|c1|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `f-expr|c2|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (11)
- `f-expr|c3|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `f-expr|c4|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `f-expr|c5|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `f-expr|c6|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `native|c1|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `native|c2|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (4)
- `native|c3|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `native|c4|t|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `native|c5|t|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `native|c6|t|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `native|c1|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `native|c2|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (4)
- `native|c3|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `native|c4|tf|v2-cow` | WRONG-READ | Spark: (4); (6) | RePark: (6)
- `native|c5|tf|v2-cow` | WRONG-READ | Spark: (1); (3); (5); (8) | RePark: (1); (3); (4); (5); (8)
- `native|c6|tf|v2-cow` | WRONG-READ | Spark: none | RePark: (4)
- `native|c1|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `native|c2|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (11)
- `native|c3|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `native|c4|u|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `native|c5|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `native|c6|u|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `native|c1|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `native|c2|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (11)
- `native|c3|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `native|c4|uf|v2-cow` | WRONG-READ | Spark: (10); (11) | RePark: (10)
- `native|c5|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `native|c6|uf|v2-cow` | WRONG-READ | Spark: none | RePark: (11)
- `spark-sql|grp|t|v2-cow` | WRONG-READ | Spark: (0.0,2); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | RePark: (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1)
- `spark-sql|dst|t|v2-cow` | WRONG-READ | Spark: (0.0); (1.5); (Infinity); (NULL); (NaN) | RePark: (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN)
- `spark-sql|ord|t|v2-cow` | ORDER-DIFFER | Spark: (2,NULL); (7,NULL); (6,0.0); (4,-0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | RePark: (2,NULL); (7,NULL); (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN)
- `spark-sql|mxmn|t|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (-0.0,-0.0) | RePark: (0.0,-0.0)
- `spark-sql|win|t|v2-cow` | WRONG-READ | Spark: (1,2); (2,2); (3,1); (4,2); (5,1); (6,2); (7,2); (8,2) | RePark: (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2)
- `spark-sql|grp|tf|v2-cow` | WRONG-READ | Spark: (0.0,2); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | RePark: (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1)
- `spark-sql|dst|tf|v2-cow` | WRONG-READ | Spark: (0.0); (1.5); (Infinity); (NULL); (NaN) | RePark: (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN)
- `spark-sql|ord|tf|v2-cow` | ORDER-DIFFER | Spark: (2,NULL); (7,NULL); (6,0.0); (4,-0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | RePark: (2,NULL); (7,NULL); (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN)
- `spark-sql|mxmn|tf|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (-0.0,-0.0) | RePark: (0.0,-0.0)
- `spark-sql|win|tf|v2-cow` | WRONG-READ | Spark: (1,2); (2,2); (3,1); (4,2); (5,1); (6,2); (7,2); (8,2) | RePark: (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2)
- `spark-sql|grp|u|v2-cow` | WRONG-READ | Spark: (0.0,2) | RePark: (-0.0,1); (0.0,1)
- `spark-sql|dst|u|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `spark-sql|ord|u|v2-cow` | ORDER-DIFFER | Spark: (10,0.0); (11,-0.0) | RePark: (11,-0.0); (10,0.0)
- `spark-sql|mxmn|u|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (0.0,0.0) | RePark: (0.0,-0.0)
- `spark-sql|win|u|v2-cow` | WRONG-READ | Spark: (10,2); (11,2) | RePark: (10,1); (11,1)
- `spark-sql|grp|uf|v2-cow` | WRONG-READ | Spark: (0.0,2) | RePark: (-0.0,1); (0.0,1)
- `spark-sql|dst|uf|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `spark-sql|ord|uf|v2-cow` | ORDER-DIFFER | Spark: (10,0.0); (11,-0.0) | RePark: (11,-0.0); (10,0.0)
- `spark-sql|mxmn|uf|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (0.0,0.0) | RePark: (0.0,-0.0)
- `spark-sql|win|uf|v2-cow` | WRONG-READ | Spark: (10,2); (11,2) | RePark: (10,1); (11,1)
- `spark-sql|jn|t+u|v2-cow` | WRONG-READ | Spark: (4,10); (4,11); (6,10); (6,11) | RePark: (4,11); (6,10)
- `spark-sql|isc|t+u|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `spark-sql|jn|tf+uf|v2-cow` | WRONG-READ | Spark: (4,10); (4,11); (6,10); (6,11) | RePark: (4,11); (6,10)
- `spark-sql|isc|tf+uf|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `native|grp|t|v2-cow` | WRONG-READ | Spark: (0.0,2); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | RePark: (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1)
- `native|dst|t|v2-cow` | WRONG-READ | Spark: (0.0); (1.5); (Infinity); (NULL); (NaN) | RePark: (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN)
- `native|ord|t|v2-cow` | ORDER-DIFFER | Spark: (2,NULL); (7,NULL); (6,0.0); (4,-0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | RePark: (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN); (2,NULL); (7,NULL)
- `native|cst|t|v2-cow` | WRONG-READ | Spark: (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,Infinity); (6,0.0); (7,NULL); (8,1.5) | RePark: (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,inf); (6,0.0); (7,NULL); (8,1.5)
- `native|mxmn|t|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (-0.0,-0.0) | RePark: (0.0,-0.0)
- `native|win|t|v2-cow` | WRONG-READ | Spark: (1,2); (2,2); (3,1); (4,2); (5,1); (6,2); (7,2); (8,2) | RePark: (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2)
- `native|grp|tf|v2-cow` | WRONG-READ | Spark: (0.0,2); (1.5,2); (Infinity,1); (NULL,2); (NaN,1) | RePark: (-0.0,1); (0.0,1); (1.5,2); (Infinity,1); (NULL,2); (NaN,1)
- `native|dst|tf|v2-cow` | WRONG-READ | Spark: (0.0); (1.5); (Infinity); (NULL); (NaN) | RePark: (-0.0); (0.0); (1.5); (Infinity); (NULL); (NaN)
- `native|ord|tf|v2-cow` | ORDER-DIFFER | Spark: (2,NULL); (7,NULL); (6,0.0); (4,-0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN) | RePark: (4,-0.0); (6,0.0); (1,1.5); (8,1.5); (5,Infinity); (3,NaN); (2,NULL); (7,NULL)
- `native|cst|tf|v2-cow` | WRONG-READ | Spark: (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,Infinity); (6,0.0); (7,NULL); (8,1.5) | RePark: (1,1.5); (2,NULL); (3,NaN); (4,-0.0); (5,inf); (6,0.0); (7,NULL); (8,1.5)
- `native|mxmn|tf|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (-0.0,-0.0) | RePark: (0.0,-0.0)
- `native|win|tf|v2-cow` | WRONG-READ | Spark: (1,2); (2,2); (3,1); (4,2); (5,1); (6,2); (7,2); (8,2) | RePark: (1,2); (2,2); (3,1); (4,1); (5,1); (6,1); (7,2); (8,2)
- `native|grp|u|v2-cow` | WRONG-READ | Spark: (0.0,2) | RePark: (-0.0,1); (0.0,1)
- `native|dst|u|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `native|ord|u|v2-cow` | ORDER-DIFFER | Spark: (10,0.0); (11,-0.0) | RePark: (11,-0.0); (10,0.0)
- `native|mxmn|u|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (0.0,0.0) | RePark: (0.0,-0.0)
- `native|win|u|v2-cow` | WRONG-READ | Spark: (10,2); (11,2) | RePark: (10,1); (11,1)
- `native|grp|uf|v2-cow` | WRONG-READ | Spark: (0.0,2) | RePark: (-0.0,1); (0.0,1)
- `native|dst|uf|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `native|ord|uf|v2-cow` | ORDER-DIFFER | Spark: (10,0.0); (11,-0.0) | RePark: (11,-0.0); (10,0.0)
- `native|mxmn|uf|v2-cow` | SIGN-ONLY, WRONG-READ | Spark: (0.0,0.0) | RePark: (0.0,-0.0)
- `native|win|uf|v2-cow` | WRONG-READ | Spark: (10,2); (11,2) | RePark: (10,1); (11,1)
- `native|jn|t+u|v2-cow` | WRONG-READ | Spark: (4,10); (4,11); (6,10); (6,11) | RePark: (4,11); (6,10)
- `native|isc|t+u|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `native|jn|tf+uf|v2-cow` | WRONG-READ | Spark: (4,10); (4,11); (6,10); (6,11) | RePark: (4,11); (6,10)
- `native|isc|tf+uf|v2-cow` | WRONG-READ | Spark: (0.0) | RePark: (-0.0); (0.0)
- `spark-sql|m1|t+u|v2-cow` | ACCEPTED-WHERE-SPARK-REFUSED | Spark: REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m1|tf+uf|v2-cow` | ACCEPTED-WHERE-SPARK-REFUSED | Spark: REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m2|t|v2-cow` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m2|tf|v2-cow` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m3|t|v2-cow` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m3|tf|v2-cow` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m1|t+u|v2-mor` | ACCEPTED-WHERE-SPARK-REFUSED | Spark: REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m1|tf+uf|v2-mor` | ACCEPTED-WHERE-SPARK-REFUSED | Spark: REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m2|t|v2-mor` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m2|tf|v2-mor` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m3|t|v2-mor` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m3|tf|v2-mor` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m1|t+u|v3` | ACCEPTED-WHERE-SPARK-REFUSED | Spark: REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m1|tf+uf|v3` | ACCEPTED-WHERE-SPARK-REFUSED | Spark: REFUSED SparkRuntimeException [MERGE_CARDINALITY_VIOLATION] The ON search condition of | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m2|t|v3` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m2|tf|v3` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m3|t|v3` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
- `spark-sql|m3|tf|v3` | WRONG-WRITE | Spark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,z); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8) | RePark: (1,1.5,f1); (2,NULL,f2); (3,NaN,f3); (4,-0.0,f4); (5,Infinity,f5); (6,0.0,z); (7,NULL,f7); (8,1.5,f8)
