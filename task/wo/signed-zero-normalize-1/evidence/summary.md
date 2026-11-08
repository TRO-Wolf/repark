# Signed zero domain table: summary (generated)

Counts by label, row group and route. RePark rows are the test layout on the release wheel; Spark rows are the
Spark-on-Iceberg answer on the test layout for the same route.

| group | route | engine | label | count |
|---|---|---|---|---|
| D1 | native | RePark | NOT-EXPRESSIBLE | 8 |
| D1 | native | RePark | PRUNED-WRONG | 3 |
| D1 | native | RePark | WRONG-WRITE | 1 |
| D1 | spark-sql | RePark | PRUNED-WRONG | 12 |
| D1 | spark-sql | Spark | PRUNED-WRONG | 24 |
| D10 | native | RePark | WRONG-READ | 6 |
| D10 | spark-sql | RePark | WRONG-READ | 6 |
| D10 | spark-sql | Spark | EQUAL | 12 |
| D11 | spark-sql | RePark | EQUAL | 15 |
| D11 | spark-sql | Spark | EQUAL | 15 |
| D12 | spark-sql | RePark | PRUNED-WRONG | 4 |
| D12 | spark-sql | RePark | WRONG-WRITE | 2 |
| D12 | spark-sql | Spark | EQUAL | 2 |
| D12 | spark-sql | Spark | PRUNED-WRONG | 4 |
| D13 | spark-sql | RePark | EQUAL | 2 |
| D13 | spark-sql | Spark | EQUAL | 2 |
| D13 | typed | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 2 |
| D13 | typed | Spark | EQUAL | 2 |
| D17 | spark-sql | RePark | EQUAL | 4 |
| D17 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 3 |
| D17 | spark-sql | RePark | WRONG-READ | 4 |
| D17 | spark-sql | Spark | EQUAL | 11 |
| D2 | f-expr | RePark | PRUNED-WRONG | 2 |
| D2 | f-expr | RePark | WRONG-READ | 3 |
| D2 | f-expr | Spark | EQUAL | 2 |
| D2 | f-expr | Spark | PRUNED-WRONG | 3 |
| D2 | filter-str | RePark | PRUNED-WRONG | 1 |
| D2 | filter-str | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 2 |
| D2 | filter-str | RePark | WRONG-READ | 2 |
| D2 | filter-str | Spark | EQUAL | 2 |
| D2 | filter-str | Spark | PRUNED-WRONG | 3 |
| D2 | native | RePark | PRUNED-WRONG | 2 |
| D2 | native | RePark | WRONG-READ | 3 |
| D2 | spark-sql | RePark | PRUNED-WRONG | 1 |
| D2 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 2 |
| D2 | spark-sql | RePark | WRONG-READ | 2 |
| D2 | spark-sql | Spark | EQUAL | 4 |
| D2 | spark-sql | Spark | PRUNED-WRONG | 6 |
| D2 | typed | RePark | PRUNED-WRONG | 2 |
| D2 | typed | RePark | WRONG-READ | 3 |
| D2 | typed | Spark | EQUAL | 2 |
| D2 | typed | Spark | PRUNED-WRONG | 3 |
| D3 | native | RePark | WRONG-READ | 3 |
| D3 | spark-sql | RePark | EQUAL | 1 |
| D3 | spark-sql | RePark | WRONG-READ | 2 |
| D3 | spark-sql | Spark | EQUAL | 6 |
| D4 | f-expr | RePark | EQUAL | 3 |
| D4 | f-expr | RePark | WRONG-READ | 7 |
| D4 | f-expr | Spark | EQUAL | 6 |
| D4 | f-expr | Spark | PRUNED-WRONG | 4 |
| D4 | filter-str | RePark | EQUAL | 3 |
| D4 | filter-str | RePark | PRUNED-WRONG | 2 |
| D4 | filter-str | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| D4 | filter-str | RePark | WRONG-READ | 4 |
| D4 | filter-str | Spark | EQUAL | 6 |
| D4 | filter-str | Spark | PRUNED-WRONG | 4 |
| D4 | native | RePark | EQUAL | 3 |
| D4 | native | RePark | WRONG-READ | 7 |
| D4 | spark-sql | RePark | EQUAL | 6 |
| D4 | spark-sql | RePark | PRUNED-WRONG | 4 |
| D4 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 3 |
| D4 | spark-sql | RePark | WRONG-READ | 9 |
| D4 | spark-sql | Spark | EQUAL | 18 |
| D4 | spark-sql | Spark | PRUNED-WRONG | 14 |
| D4 | typed | RePark | EQUAL | 2 |
| D4 | typed | RePark | PRUNED-WRONG | 4 |
| D4 | typed | RePark | WRONG-READ | 6 |
| D4 | typed | Spark | EQUAL | 5 |
| D4 | typed | Spark | PRUNED-WRONG | 7 |
| D5 | f-expr | RePark | EQUAL | 3 |
| D5 | f-expr | RePark | PRUNED-WRONG | 1 |
| D5 | f-expr | RePark | WRONG-READ | 10 |
| D5 | f-expr | Spark | EQUAL | 8 |
| D5 | f-expr | Spark | PRUNED-WRONG | 6 |
| D5 | filter-str | RePark | EQUAL | 3 |
| D5 | filter-str | RePark | PRUNED-WRONG | 2 |
| D5 | filter-str | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 3 |
| D5 | filter-str | RePark | WRONG-READ | 6 |
| D5 | filter-str | Spark | EQUAL | 8 |
| D5 | filter-str | Spark | PRUNED-WRONG | 6 |
| D5 | native | RePark | EQUAL | 3 |
| D5 | native | RePark | PRUNED-WRONG | 1 |
| D5 | native | RePark | WRONG-READ | 10 |
| D5 | spark-sql | RePark | EQUAL | 6 |
| D5 | spark-sql | RePark | PRUNED-WRONG | 4 |
| D5 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 5 |
| D5 | spark-sql | RePark | WRONG-READ | 11 |
| D5 | spark-sql | Spark | EQUAL | 22 |
| D5 | spark-sql | Spark | PRUNED-WRONG | 18 |
| D5 | typed | RePark | EQUAL | 2 |
| D5 | typed | RePark | PRUNED-WRONG | 5 |
| D5 | typed | RePark | WRONG-READ | 9 |
| D5 | typed | Spark | EQUAL | 7 |
| D5 | typed | Spark | PRUNED-WRONG | 9 |
| D6 | native | RePark | WRONG-READ | 6 |
| D6 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| D6 | spark-sql | RePark | WRONG-READ | 5 |
| D6 | spark-sql | Spark | EQUAL | 12 |
| D6 | typed | RePark | WRONG-READ | 2 |
| D6 | typed | Spark | EQUAL | 2 |
| D7 | native | RePark | EQUAL | 2 |
| D7 | native | RePark | NOT-EXPRESSIBLE | 36 |
| D7 | native | RePark | PRUNED-WRONG | 12 |
| D7 | native | RePark | WRONG-WRITE | 4 |
| D7 | spark-sql | RePark | EQUAL | 17 |
| D7 | spark-sql | RePark | PRUNED-WRONG | 47 |
| D7 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| D7 | spark-sql | RePark | WRONG-WRITE | 13 |
| D7 | spark-sql | Spark | EQUAL | 35 |
| D7 | spark-sql | Spark | PRUNED-WRONG | 97 |
| D8 | native | RePark | WRONG-READ | 10 |
| D8 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| D8 | spark-sql | RePark | WRONG-READ | 9 |
| D8 | spark-sql | Spark | EQUAL | 20 |
| D9 | native | RePark | BOTH-REFUSED | 1 |
| D9 | native | RePark | EQUAL | 4 |
| D9 | native | RePark | NATIVE-REFUSED | 2 |
| D9 | native | RePark | ORDER-DIFFER | 1 |
| D9 | native | RePark | SIGN-ONLY+WRONG-READ | 1 |
| D9 | native | RePark | WRONG-READ | 16 |
| D9 | spark-sql | RePark | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| D9 | spark-sql | RePark | EQUAL | 6 |
| D9 | spark-sql | RePark | ORDER-DIFFER | 1 |
| D9 | spark-sql | RePark | PRUNED-WRONG | 1 |
| D9 | spark-sql | RePark | REFUSED-WHERE-SPARK-ACCEPTED | 2 |
| D9 | spark-sql | RePark | SIGN-ONLY+WRONG-READ | 1 |
| D9 | spark-sql | RePark | WRONG-READ | 13 |
| D9 | spark-sql | Spark | BOTH-REFUSED | 2 |
| D9 | spark-sql | Spark | EQUAL | 46 |
| D9 | spark-sql | Spark | PRUNED-WRONG | 2 |

## Findings (WRONG-READ, WRONG-WRITE, PRUNED-WRONG)

- D1-1-cow spark-sql (v2-cow): PRUNED-WRONG
- D1-1-cow native (v2-cow): PRUNED-WRONG
- D1-2-cow spark-sql (v2-cow): PRUNED-WRONG
- D1-2-cow native (v2-cow): WRONG-WRITE
- D1-3-cow spark-sql (v2-cow): PRUNED-WRONG
- D1-3-cow native (v2-cow): PRUNED-WRONG
- D1-4-cow spark-sql (v2-cow): PRUNED-WRONG
- D1-4-cow native (v2-cow): PRUNED-WRONG
- D1-1-mor spark-sql (v2-mor): PRUNED-WRONG
- D1-2-mor spark-sql (v2-mor): PRUNED-WRONG
- D1-3-mor spark-sql (v2-mor): PRUNED-WRONG
- D1-4-mor spark-sql (v2-mor): PRUNED-WRONG
- D1-1-v3 spark-sql (v3): PRUNED-WRONG
- D1-2-v3 spark-sql (v3): PRUNED-WRONG
- D1-3-v3 spark-sql (v3): PRUNED-WRONG
- D1-4-v3 spark-sql (v3): PRUNED-WRONG
- D7-delete-1-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-1-cow native (v2-cow): PRUNED-WRONG
- D7-update-1-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-update-1-cow native (v2-cow): PRUNED-WRONG
- D7-delete-1-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-1-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-delete-1-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-1-v3 spark-sql (v3): PRUNED-WRONG
- D7-insert-1 spark-sql (v2-cow): PRUNED-WRONG
- D7-ctas-1 spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-2-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-2-cow native (v2-cow): WRONG-WRITE
- D7-update-2-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-update-2-cow native (v2-cow): WRONG-WRITE
- D7-delete-2-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-2-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-delete-2-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-2-v3 spark-sql (v3): PRUNED-WRONG
- D7-insert-2 spark-sql (v2-cow): PRUNED-WRONG
- D7-ctas-2 spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-3-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-3-cow native (v2-cow): PRUNED-WRONG
- D7-update-3-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-update-3-cow native (v2-cow): PRUNED-WRONG
- D7-delete-3-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-3-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-delete-3-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-3-v3 spark-sql (v3): PRUNED-WRONG
- D7-insert-3 spark-sql (v2-cow): PRUNED-WRONG
- D7-ctas-3 spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-4-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-4-cow native (v2-cow): PRUNED-WRONG
- D7-update-4-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-update-4-cow native (v2-cow): PRUNED-WRONG
- D7-delete-4-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-4-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-delete-4-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-4-v3 spark-sql (v3): PRUNED-WRONG
- D7-insert-4 spark-sql (v2-cow): PRUNED-WRONG
- D7-ctas-4 spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-5-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-5-cow native (v2-cow): PRUNED-WRONG
- D7-update-5-cow spark-sql (v2-cow): WRONG-WRITE
- D7-update-5-cow native (v2-cow): WRONG-WRITE
- D7-delete-5-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-5-mor spark-sql (v2-mor): WRONG-WRITE
- D7-delete-5-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-5-v3 spark-sql (v3): WRONG-WRITE
- D7-insert-5 spark-sql (v2-cow): WRONG-WRITE
- D7-ctas-5 spark-sql (v2-cow): WRONG-WRITE
- D7-delete-6-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-6-cow native (v2-cow): PRUNED-WRONG
- D7-update-6-cow spark-sql (v2-cow): WRONG-WRITE
- D7-update-6-cow native (v2-cow): WRONG-WRITE
- D7-delete-6-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-6-mor spark-sql (v2-mor): WRONG-WRITE
- D7-delete-6-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-6-v3 spark-sql (v3): WRONG-WRITE
- D7-insert-6 spark-sql (v2-cow): WRONG-WRITE
- D7-ctas-6 spark-sql (v2-cow): WRONG-WRITE
- D7-delete-7-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-7-cow native (v2-cow): PRUNED-WRONG
- D7-update-7-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-update-7-cow native (v2-cow): PRUNED-WRONG
- D7-delete-7-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-update-7-mor spark-sql (v2-mor): PRUNED-WRONG
- D7-delete-7-v3 spark-sql (v3): PRUNED-WRONG
- D7-update-7-v3 spark-sql (v3): PRUNED-WRONG
- D7-insert-7 spark-sql (v2-cow): PRUNED-WRONG
- D7-ctas-7 spark-sql (v2-cow): PRUNED-WRONG
- D7-delete-8-cow native (v2-cow): PRUNED-WRONG
- D7-update-8-cow native (v2-cow): PRUNED-WRONG
- D7-merge-Ma-cow spark-sql (v2-cow): WRONG-WRITE
- D7-merge-Mc-cow spark-sql (v2-cow): PRUNED-WRONG
- D7-merge-Ma-mor spark-sql (v2-mor): WRONG-WRITE
- D7-merge-Mc-mor spark-sql (v2-mor): WRONG-WRITE
- D12-select-cow spark-sql (v2-cow): PRUNED-WRONG
- D12-delete-cow spark-sql (v2-cow): PRUNED-WRONG
- D12-group-cow spark-sql (v2-cow): WRONG-WRITE
- D12-select-mor spark-sql (v2-mor): PRUNED-WRONG
- D12-delete-mor spark-sql (v2-mor): PRUNED-WRONG
- D12-group-mor spark-sql (v2-mor): WRONG-WRITE
- D3-1 spark-sql (v2-cow): WRONG-READ
- D3-1 native (v2-cow): WRONG-READ
- D3-2 spark-sql (v2-cow): WRONG-READ
- D3-2 native (v2-cow): WRONG-READ
- D3-3 native (v2-cow): WRONG-READ
- D2-1 f-expr (v2-cow): PRUNED-WRONG
- D2-1 typed (v2-cow): PRUNED-WRONG
- D2-1 native (v2-cow): PRUNED-WRONG
- D2-2 spark-sql (v2-cow): WRONG-READ
- D2-2 filter-str (v2-cow): WRONG-READ
- D2-2 f-expr (v2-cow): WRONG-READ
- D2-2 typed (v2-cow): WRONG-READ
- D2-2 native (v2-cow): WRONG-READ
- D2-3 f-expr (v2-cow): WRONG-READ
- D2-3 typed (v2-cow): WRONG-READ
- D2-3 native (v2-cow): WRONG-READ
- D2-4 spark-sql (v2-cow): PRUNED-WRONG
- D2-4 filter-str (v2-cow): PRUNED-WRONG
- D2-4 f-expr (v2-cow): PRUNED-WRONG
- D2-4 typed (v2-cow): PRUNED-WRONG
- D2-4 native (v2-cow): PRUNED-WRONG
- D2-5 spark-sql (v2-cow): WRONG-READ
- D2-5 filter-str (v2-cow): WRONG-READ
- D2-5 f-expr (v2-cow): WRONG-READ
- D2-5 typed (v2-cow): WRONG-READ
- D2-5 native (v2-cow): WRONG-READ
- D4-1 spark-sql (v2-cow): WRONG-READ
- D4-1 filter-str (v2-cow): WRONG-READ
- D4-1 f-expr (v2-cow): WRONG-READ
- D4-1 native (v2-cow): WRONG-READ
- D4-2 spark-sql (v2-cow): WRONG-READ
- D4-2 filter-str (v2-cow): WRONG-READ
- D4-2 f-expr (v2-cow): WRONG-READ
- D4-2 native (v2-cow): WRONG-READ
- D4-3 spark-sql (v2-cow): PRUNED-WRONG
- D4-3 filter-str (v2-cow): PRUNED-WRONG
- D4-3 f-expr (v2-cow): WRONG-READ
- D4-3 native (v2-cow): WRONG-READ
- D4-5 spark-sql (v2-cow): WRONG-READ
- D4-5 filter-str (v2-cow): WRONG-READ
- D4-5 f-expr (v2-cow): WRONG-READ
- D4-5 native (v2-cow): WRONG-READ
- D4-7 spark-sql (v2-cow): WRONG-READ
- D4-7 filter-str (v2-cow): WRONG-READ
- D4-7 f-expr (v2-cow): WRONG-READ
- D4-7 native (v2-cow): WRONG-READ
- D4-9 spark-sql (v2-cow): PRUNED-WRONG
- D4-9 filter-str (v2-cow): PRUNED-WRONG
- D4-9 f-expr (v2-cow): WRONG-READ
- D4-9 native (v2-cow): WRONG-READ
- D4-10 f-expr (v2-cow): WRONG-READ
- D4-10 native (v2-cow): WRONG-READ
- D4-t1 spark-sql (v2-cow): PRUNED-WRONG
- D4-t1 typed (v2-cow): PRUNED-WRONG
- D4-t2 spark-sql (v2-cow): WRONG-READ
- D4-t2 typed (v2-cow): WRONG-READ
- D4-t3 spark-sql (v2-cow): WRONG-READ
- D4-t4 typed (v2-cow): PRUNED-WRONG
- D4-t5 typed (v2-cow): WRONG-READ
- D4-t6 spark-sql (v2-cow): WRONG-READ
- D4-t6 typed (v2-cow): WRONG-READ
- D4-t7 typed (v2-cow): PRUNED-WRONG
- D4-t8 typed (v2-cow): WRONG-READ
- D4-t9 spark-sql (v2-cow): PRUNED-WRONG
- D4-t9 typed (v2-cow): PRUNED-WRONG
- D4-t10 spark-sql (v2-cow): WRONG-READ
- D4-t10 typed (v2-cow): WRONG-READ
- D4-t12 spark-sql (v2-cow): WRONG-READ
- D4-t12 typed (v2-cow): WRONG-READ
- D5-1 f-expr (v2-cow): PRUNED-WRONG
- D5-1 typed (v2-cow): PRUNED-WRONG
- D5-1 native (v2-cow): PRUNED-WRONG
- D5-2 spark-sql (v2-cow): WRONG-READ
- D5-2 filter-str (v2-cow): WRONG-READ
- D5-2 f-expr (v2-cow): WRONG-READ
- D5-2 typed (v2-cow): WRONG-READ
- D5-2 native (v2-cow): WRONG-READ
- D5-3 f-expr (v2-cow): WRONG-READ
- D5-3 typed (v2-cow): WRONG-READ
- D5-3 native (v2-cow): WRONG-READ
- D5-5 spark-sql (v2-cow): WRONG-READ
- D5-5 filter-str (v2-cow): WRONG-READ
- D5-5 f-expr (v2-cow): WRONG-READ
- D5-5 typed (v2-cow): WRONG-READ
- D5-5 native (v2-cow): WRONG-READ
- D5-d4-1 spark-sql (v2-cow): WRONG-READ
- D5-d4-1 filter-str (v2-cow): WRONG-READ
- D5-d4-1 f-expr (v2-cow): WRONG-READ
- D5-d4-1 native (v2-cow): WRONG-READ
- D5-d4-2 spark-sql (v2-cow): WRONG-READ
- D5-d4-2 filter-str (v2-cow): WRONG-READ
- D5-d4-2 f-expr (v2-cow): WRONG-READ
- D5-d4-2 native (v2-cow): WRONG-READ
- D5-d4-3 spark-sql (v2-cow): PRUNED-WRONG
- D5-d4-3 filter-str (v2-cow): PRUNED-WRONG
- D5-d4-3 f-expr (v2-cow): WRONG-READ
- D5-d4-3 native (v2-cow): WRONG-READ
- D5-d4-5 spark-sql (v2-cow): WRONG-READ
- D5-d4-5 filter-str (v2-cow): WRONG-READ
- D5-d4-5 f-expr (v2-cow): WRONG-READ
- D5-d4-5 native (v2-cow): WRONG-READ
- D5-d4-7 spark-sql (v2-cow): WRONG-READ
- D5-d4-7 filter-str (v2-cow): WRONG-READ
- D5-d4-7 f-expr (v2-cow): WRONG-READ
- D5-d4-7 native (v2-cow): WRONG-READ
- D5-d4-9 spark-sql (v2-cow): PRUNED-WRONG
- D5-d4-9 filter-str (v2-cow): PRUNED-WRONG
- D5-d4-9 f-expr (v2-cow): WRONG-READ
- D5-d4-9 native (v2-cow): WRONG-READ
- D5-d4-10 f-expr (v2-cow): WRONG-READ
- D5-d4-10 native (v2-cow): WRONG-READ
- D5-t1 spark-sql (v2-cow): PRUNED-WRONG
- D5-t1 typed (v2-cow): PRUNED-WRONG
- D5-t2 spark-sql (v2-cow): WRONG-READ
- D5-t2 typed (v2-cow): WRONG-READ
- D5-t3 spark-sql (v2-cow): WRONG-READ
- D5-t4 typed (v2-cow): PRUNED-WRONG
- D5-t5 typed (v2-cow): WRONG-READ
- D5-t6 spark-sql (v2-cow): WRONG-READ
- D5-t6 typed (v2-cow): WRONG-READ
- D5-t7 typed (v2-cow): PRUNED-WRONG
- D5-t8 typed (v2-cow): WRONG-READ
- D5-t9 spark-sql (v2-cow): PRUNED-WRONG
- D5-t9 typed (v2-cow): PRUNED-WRONG
- D5-t10 spark-sql (v2-cow): WRONG-READ
- D5-t10 typed (v2-cow): WRONG-READ
- D5-t12 spark-sql (v2-cow): WRONG-READ
- D5-t12 typed (v2-cow): WRONG-READ
- D6-1 spark-sql (v2-cow): WRONG-READ
- D6-1 typed (v2-cow): WRONG-READ
- D6-1 native (v2-cow): WRONG-READ
- D6-2 typed (v2-cow): WRONG-READ
- D6-2 native (v2-cow): WRONG-READ
- D6-3 spark-sql (v2-cow): WRONG-READ
- D6-3 native (v2-cow): WRONG-READ
- D6-4 spark-sql (v2-cow): WRONG-READ
- D6-4 native (v2-cow): WRONG-READ
- D6-5 spark-sql (v2-cow): WRONG-READ
- D6-5 native (v2-cow): WRONG-READ
- D6-6 spark-sql (v2-cow): WRONG-READ
- D6-6 native (v2-cow): WRONG-READ
- D8-1 spark-sql (v2-cow): WRONG-READ
- D8-1 native (v2-cow): WRONG-READ
- D8-2 spark-sql (v2-cow): WRONG-READ
- D8-2 native (v2-cow): WRONG-READ
- D8-3 spark-sql (v2-cow): WRONG-READ
- D8-3 native (v2-cow): WRONG-READ
- D8-4 spark-sql (v2-cow): WRONG-READ
- D8-4 native (v2-cow): WRONG-READ
- D8-5 spark-sql (v2-cow): WRONG-READ
- D8-5 native (v2-cow): WRONG-READ
- D8-6 spark-sql (v2-cow): WRONG-READ
- D8-6 native (v2-cow): WRONG-READ
- D8-7 spark-sql (v2-cow): WRONG-READ
- D8-7 native (v2-cow): WRONG-READ
- D8-8 native (v2-cow): WRONG-READ
- D8-9 spark-sql (v2-cow): WRONG-READ
- D8-9 native (v2-cow): WRONG-READ
- D8-10 spark-sql (v2-cow): WRONG-READ
- D8-10 native (v2-cow): WRONG-READ
- D9-01 spark-sql (v2-cow): WRONG-READ
- D9-01 native (v2-cow): WRONG-READ
- D9-02 spark-sql (v2-cow): SIGN-ONLY+WRONG-READ
- D9-02 native (v2-cow): SIGN-ONLY+WRONG-READ
- D9-04 spark-sql (v2-cow): WRONG-READ
- D9-04 native (v2-cow): WRONG-READ
- D9-05 spark-sql (v2-cow): WRONG-READ
- D9-05 native (v2-cow): WRONG-READ
- D9-06 spark-sql (v2-cow): WRONG-READ
- D9-06 native (v2-cow): WRONG-READ
- D9-09 spark-sql (v2-cow): WRONG-READ
- D9-09 native (v2-cow): WRONG-READ
- D9-10 spark-sql (v2-cow): WRONG-READ
- D9-10 native (v2-cow): WRONG-READ
- D9-11 spark-sql (v2-cow): WRONG-READ
- D9-11 native (v2-cow): WRONG-READ
- D9-12 spark-sql (v2-cow): WRONG-READ
- D9-12 native (v2-cow): WRONG-READ
- D9-13 spark-sql (v2-cow): WRONG-READ
- D9-13 native (v2-cow): WRONG-READ
- D9-15 spark-sql (v2-cow): WRONG-READ
- D9-15 native (v2-cow): WRONG-READ
- D9-16 native (v2-cow): WRONG-READ
- D9-17 spark-sql (v2-cow): WRONG-READ
- D9-17 native (v2-cow): WRONG-READ
- D9-18 native (v2-cow): WRONG-READ
- D9-19 spark-sql (v2-cow): WRONG-READ
- D9-19 native (v2-cow): WRONG-READ
- D9-21 native (v2-cow): WRONG-READ
- D9-24 spark-sql (v2-cow): WRONG-READ
- D9-24 native (v2-cow): WRONG-READ
- D9-25 spark-sql (v2-cow): PRUNED-WRONG
- D10-1 spark-sql (v2-cow): WRONG-READ
- D10-1 native (v2-cow): WRONG-READ
- D10-2 spark-sql (v2-cow): WRONG-READ
- D10-2 native (v2-cow): WRONG-READ
- D10-3 spark-sql (v2-cow): WRONG-READ
- D10-3 native (v2-cow): WRONG-READ
- D10-4 spark-sql (v2-cow): WRONG-READ
- D10-4 native (v2-cow): WRONG-READ
- D10-5 spark-sql (v2-cow): WRONG-READ
- D10-5 native (v2-cow): WRONG-READ
- D10-6 spark-sql (v2-cow): WRONG-READ
- D10-6 native (v2-cow): WRONG-READ
- D17-1 spark-sql (v2-cow): WRONG-READ
- D17-8 spark-sql (v2-cow): WRONG-READ
- D17-9 spark-sql (v2-cow): WRONG-READ
- D17-11 spark-sql (v2-cow): WRONG-READ

## Legs

- {"_file": "repark-main-signed-zero-dml.jsonl", "engine": "repark", "facade_memory_limit_gb": null, "facade_time_zone": "America/New_York", "label": "repark-main-fdc61f26", "leg": "dml", "load1": 10.572265625, "native_config": {"repark.sql.allowCreateFormatVersion3": "true"}, "native_memory_limit_gb": null, "pyarrow_version": "26.0.1", "python": "3.12.3", "repark_version": "1.5.5", "wheel_file": "repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl", "wheel_sha256": "07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479"}
- {"_file": "repark-main-signed-zero-reads.jsonl", "engine": "repark", "facade_memory_limit_gb": null, "facade_time_zone": "America/New_York", "label": "repark-main-fdc61f26", "leg": "reads", "load1": 10.62255859375, "native_config": {"repark.sql.allowCreateFormatVersion3": "true"}, "native_memory_limit_gb": null, "pyarrow_version": "26.0.1", "python": "3.12.3", "repark_version": "1.5.5", "wheel_file": "repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl", "wheel_sha256": "07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479"}
- {"_file": "repark-main-signed-zero-spill-facade.jsonl", "engine": "repark", "facade_memory_limit_gb": "1", "facade_time_zone": "America/New_York", "label": "repark-main-fdc61f26", "leg": "spill-facade", "load1": 10.95263671875, "native_config": {"repark.sql.allowCreateFormatVersion3": "true"}, "native_memory_limit_gb": null, "pyarrow_version": "26.0.1", "python": "3.12.3", "repark_version": "1.5.5", "wheel_file": "repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl", "wheel_sha256": "07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479"}
- {"_file": "repark-main-signed-zero-spill-native.jsonl", "engine": "repark", "facade_memory_limit_gb": null, "facade_time_zone": "America/New_York", "label": "repark-main-fdc61f26", "leg": "spill-native", "load1": 10.88623046875, "native_config": {"repark.sql.allowCreateFormatVersion3": "true"}, "native_memory_limit_gb": 1, "pyarrow_version": "26.0.1", "python": "3.12.3", "repark_version": "1.5.5", "wheel_file": "repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl", "wheel_sha256": "07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479"}
- {"_file": "repark-main-signed-zero-timing.jsonl", "engine": "repark", "facade_memory_limit_gb": null, "facade_time_zone": "America/New_York", "label": "repark-main-fdc61f26", "leg": "timing", "load1": 10.95263671875, "native_config": {"repark.sql.allowCreateFormatVersion3": "true"}, "native_memory_limit_gb": null, "pyarrow_version": "26.0.1", "python": "3.12.3", "repark_version": "1.5.5", "wheel_file": "repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl", "wheel_sha256": "07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479"}
- {"_file": "spark-signed-zero-dml.jsonl", "engine": "spark", "iceberg_jar": "org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar", "iceberg_jar_sha256": "d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242", "jdk_version": "17.0.15", "label": "spark-4.1.2", "leg": "dml", "load1": 5.31201171875, "memory_fraction": null, "python": "3.12.3", "session_time_zone": "America/New_York", "spark_version": "4.1.2"}
- {"_file": "spark-signed-zero-reads.jsonl", "engine": "spark", "iceberg_jar": "org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar", "iceberg_jar_sha256": "d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242", "jdk_version": "17.0.15", "label": "spark-4.1.2", "leg": "reads", "load1": 5.048828125, "memory_fraction": null, "python": "3.12.3", "session_time_zone": "America/New_York", "spark_version": "4.1.2"}
- {"_file": "spark-signed-zero-spill.jsonl", "engine": "spark", "iceberg_jar": "org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar", "iceberg_jar_sha256": "d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242", "jdk_version": "17.0.15", "label": "spark-4.1.2", "leg": "spill", "load1": 10.33251953125, "memory_fraction": "0.01", "python": "3.12.3", "session_time_zone": "America/New_York", "spark_version": "4.1.2"}

## Timing (RePark, pre-unit wheel only)

- D15-group-by-d: outcome ANSWERED, median 0.032515380007680506 s over 5 runs, load1 before [10.95, 10.95, 10.95, 10.95, 10.95], rows (686.0,10000); (826.0,10000); (902.0,10000); (23.0,10000); (315.0,10000); (546.0,10000); (670.0,10000); (874.0,10000); (339.0,10000); (374.0,10000); (479.0,10000); (482.0,10000)...
- D15-join-count: outcome ANSWERED, median 0.02142423299665097 s over 5 runs, load1 before [10.95, 10.95, 10.95, 10.95, 10.95], rows (5000000); IEEE expectation from the seed: 10000000 (5,000,000 zero rows, each matching two u rows)

## Spill (D16)

- repark-facade-1gb E8-group-by-d: answer (123.0,2000); (295.0,2000); (330.0,2000); (467.0,2000); (570.0,2000); (646.0,2000); (786.0,2000); (926.0,2000); (51.0,2000); (98.0,2000); (335.0,2000); (586.0,2000); (726.0,2000...; explain ANSWERED; spill lines 12; nonzero False
- repark-facade-1gb E8-join-zeros: answer (1000000); explain ANSWERED; spill lines 3; nonzero False
- repark-facade-1gb E8-self-join-d: answer (1000000000); explain ANSWERED; spill lines 9; nonzero False
- repark-native-1gb E8-group-by-d: answer (886.0,2000); (962.0,2000); (62.0,2000); (75.0,2000); (118.0,2000); (275.0,2000); (310.0,2000); (542.0,2000); (746.0,2000); (919.0,2000); (147.0,2000); (182.0,2000); (407.0,2000...; explain ANSWERED; spill lines 12; nonzero False
- repark-native-1gb E8-join-zeros: answer (1000000); explain ANSWERED; spill lines 3; nonzero False
- repark-native-1gb E8-self-join-d: answer (1000000000); explain ANSWERED; spill lines 9; nonzero False
- spark-4.1.2-spill E8-group-by-d: answer (6.0,2000); (7.0,2000); (55.0,2000); (58.0,2000); (63.0,2000); (79.0,2000); (87.0,2000); (99.0,2000); (106.0,2000); (111.0,2000); (122.0,2000); (183.0,2000); (194.0,2000); (210....; explain REFUSED; spill lines 2; nonzero False
- spark-4.1.2-spill E8-join-zeros: answer (2000000); explain REFUSED; spill lines 5; nonzero True
- spark-4.1.2-spill E8-self-join-d: answer (2000000000); explain REFUSED; spill lines 5; nonzero True
