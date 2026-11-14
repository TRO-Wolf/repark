# SIGNED-ZERO-CONST-COMPARE-1 — the step-0 table on the head build

The same probe (`../step0/probe_const.py`) on the release build of the fix, against the same Spark 4.1.2 cells
(`../step0/raw/spark-*.jsonl.gz`), measured 2026-11-14. Generated from `../step0/` by
`python3 -I compare.py ../after/raw repark-head ../after/raw/cells.jsonl.gz raw`.

## Moves from main (`d843d37e`) to head, cell by cell

| from | to | cells |
|---|---|---|
| WRONG | EQUAL (comparison operators: SELECT list) | 3,304 |
| WRONG | EQUAL (contexts: WHERE, HAVING, joins, `OR`) | 448 |
| WRONG | EQUAL (DML: DELETE, UPDATE, MERGE ON, MERGE WHEN MATCHED AND; CoW, MoR, native) | 344 |
| EQUAL | anything else | **0** |
| WRONG | WRONG, same answer (`nullif` 800, `array_contains` 720, `greatest`/`least` 800, NaN 2,496) | 4,816 |
| REFUSED, NOT-EXPRESSIBLE | unchanged | 364, 4,300 |

All 4,096 in-scope cells reach Spark's answer; no cell that matched Spark on main moves; every out-of-scope cell
keeps its main answer.

## Totals

| part | door | ANSI | cells | EQUAL | WRONG | REFUSED | PANIC | BOTH-REFUSED | SPARK-REFUSED | NOT-EXPRESSIBLE | NO-TRUTH |
|---|---|---|---|---|---|---|---|---|---|---|---|
| context | dataframe | false | 96 | 96 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | dataframe | true | 96 | 96 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | fexpr | false | 96 | 96 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | fexpr | true | 96 | 96 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | filter-str | false | 96 | 96 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | filter-str | true | 96 | 96 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | native | n/a | 480 | 400 | 0 | 0 | 0 | 0 | 0 | 80 | 0 |
| context | spark-sql | false | 480 | 480 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| context | spark-sql | true | 480 | 480 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| dml | native | n/a | 100 | 60 | 0 | 40 | 0 | 0 | 0 | 0 | 0 |
| dml | spark-sql | false | 200 | 200 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| dml | spark-sql | true | 200 | 200 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| mixed | native | n/a | 220 | 180 | 0 | 0 | 0 | 0 | 0 | 40 | 0 |
| mixed | spark-sql | false | 220 | 220 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| mixed | spark-sql | true | 220 | 220 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| nan | dataframe | false | 1344 | 0 | 0 | 0 | 0 | 0 | 0 | 1344 | 0 |
| nan | dataframe | true | 1344 | 0 | 0 | 0 | 0 | 0 | 0 | 1344 | 0 |
| nan | fexpr | false | 1344 | 836 | 508 | 0 | 0 | 0 | 0 | 0 | 0 |
| nan | fexpr | true | 1344 | 836 | 508 | 0 | 0 | 0 | 0 | 0 | 0 |
| nan | native | n/a | 1344 | 752 | 464 | 128 | 0 | 0 | 0 | 0 | 0 |
| nan | spark-sql | false | 1344 | 836 | 508 | 0 | 0 | 0 | 0 | 0 | 0 |
| nan | spark-sql | true | 1344 | 836 | 508 | 0 | 0 | 0 | 0 | 0 | 0 |
| select | dataframe | false | 2982 | 2338 | 360 | 0 | 0 | 0 | 0 | 284 | 0 |
| select | dataframe | true | 2982 | 2338 | 360 | 0 | 0 | 0 | 0 | 284 | 0 |
| select | fexpr | false | 2982 | 2622 | 360 | 0 | 0 | 0 | 0 | 0 | 0 |
| select | fexpr | true | 2982 | 2622 | 360 | 0 | 0 | 0 | 0 | 0 | 0 |
| select | native | n/a | 2982 | 1702 | 160 | 196 | 0 | 0 | 0 | 924 | 0 |
| select | spark-sql | false | 2982 | 2622 | 360 | 0 | 0 | 0 | 0 | 0 | 0 |
| select | spark-sql | true | 2982 | 2622 | 360 | 0 | 0 | 0 | 0 | 0 | 0 |
| all | | | 33458 | 23978 | 4816 | 364 | 0 | 0 | 0 | 4300 | 0 |

### select: WRONG / REFUSED cells by operator and door (ANSI true | false; native n/a)

| op | dataframe | fexpr | filter-str | native | spark-sql |
|---|---|---|---|---|---|
| eq | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| ne | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| nseq | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| lt | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| le | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| gt | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| ge | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| in1 | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| in2 | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| notin1 | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| notin2 | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| between | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| nbetween | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| case_simple | 0/0 \| 0/0 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| case_searched | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| if | 0/0 \| 0/0 | 0/142 \| 0/142 | - | 98/98 | 0/142 \| 0/142 |
| nullif | 120/142 \| 120/142 | 120/142 \| 120/142 | - | 80/98 | 120/142 \| 120/142 |
| greatest | 60/142 \| 60/142 | 60/142 \| 60/142 | - | 40/98 | 60/142 \| 60/142 |
| least | 60/142 \| 60/142 | 60/142 \| 60/142 | - | 40/98 | 60/142 \| 60/142 |
| coalesce | 0/142 \| 0/142 | 0/142 \| 0/142 | - | 0/98 | 0/142 \| 0/142 |
| array_contains | 120/142 \| 120/142 | 120/142 \| 120/142 | - | 98/98 | 120/142 \| 120/142 |

### nan: WRONG / REFUSED cells by operator and door (ANSI true | false; native n/a)

| op | dataframe | fexpr | filter-str | native | spark-sql |
|---|---|---|---|---|---|
| eq | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 30/64 | 30/64 \| 30/64 |
| ne | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 30/64 | 30/64 \| 30/64 |
| nseq | 0/0 \| 0/0 | 12/64 \| 12/64 | - | 12/64 | 12/64 \| 12/64 |
| lt | 0/0 \| 0/0 | 34/64 \| 34/64 | - | 34/64 | 34/64 \| 34/64 |
| le | 0/0 \| 0/0 | 34/64 \| 34/64 | - | 34/64 | 34/64 \| 34/64 |
| gt | 0/0 \| 0/0 | 34/64 \| 34/64 | - | 34/64 | 34/64 \| 34/64 |
| ge | 0/0 \| 0/0 | 34/64 \| 34/64 | - | 34/64 | 34/64 \| 34/64 |
| in1 | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 30/64 | 30/64 \| 30/64 |
| in2 | 0/0 \| 0/0 | 28/64 \| 28/64 | - | 28/64 | 28/64 \| 28/64 |
| notin1 | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 30/64 | 30/64 \| 30/64 |
| notin2 | 0/0 \| 0/0 | 28/64 \| 28/64 | - | 28/64 | 28/64 \| 28/64 |
| between | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 30/64 | 30/64 \| 30/64 |
| nbetween | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 30/64 | 30/64 \| 30/64 |
| case_simple | 0/0 \| 0/0 | 14/64 \| 14/64 | - | 14/64 | 14/64 \| 14/64 |
| case_searched | 0/0 \| 0/0 | 14/64 \| 14/64 | - | 14/64 | 14/64 \| 14/64 |
| if | 0/0 \| 0/0 | 14/64 \| 14/64 | - | 64/64 | 14/64 \| 14/64 |
| nullif | 0/0 \| 0/0 | 18/64 \| 18/64 | - | 18/64 | 18/64 \| 18/64 |
| greatest | 0/0 \| 0/0 | 10/64 \| 10/64 | - | 10/64 | 10/64 \| 10/64 |
| least | 0/0 \| 0/0 | 18/64 \| 18/64 | - | 18/64 | 18/64 \| 18/64 |
| coalesce | 0/0 \| 0/0 | 6/64 \| 6/64 | - | 6/64 | 6/64 \| 6/64 |
| array_contains | 0/0 \| 0/0 | 30/64 \| 30/64 | - | 64/64 | 30/64 \| 30/64 |

### context: WRONG / REFUSED cells by operator and door (ANSI true | false; native n/a)

| op | dataframe | fexpr | filter-str | native | spark-sql |
|---|---|---|---|---|---|
| eq | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| ne | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| nseq | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| lt | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| ge | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| in2 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| notin2 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |
| between | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/12 \| 0/12 | 0/50 | 0/60 \| 0/60 |

### mixed: WRONG / REFUSED cells by operator and door (ANSI true | false; native n/a)

| op | dataframe | fexpr | filter-str | native | spark-sql |
|---|---|---|---|---|---|
| eq | - | - | - | 0/18 | 0/22 \| 0/22 |
| ne | - | - | - | 0/18 | 0/22 \| 0/22 |
| nseq | - | - | - | 0/18 | 0/22 \| 0/22 |
| lt | - | - | - | 0/18 | 0/22 \| 0/22 |
| le | - | - | - | 0/18 | 0/22 \| 0/22 |
| gt | - | - | - | 0/18 | 0/22 \| 0/22 |
| ge | - | - | - | 0/18 | 0/22 \| 0/22 |
| in1 | - | - | - | 0/18 | 0/22 \| 0/22 |
| notin1 | - | - | - | 0/18 | 0/22 \| 0/22 |
| between | - | - | - | 0/18 | 0/22 \| 0/22 |

### dml: every non-EQUAL cell

| door | ANSI | kind | stmt | op | type | left | right | verdict | Spark | RePark |
|---|---|---|---|---|---|---|---|---|---|---|
| native | n/a | v2-cow | merge | eq | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | eq | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | ne | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | ne | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | nseq | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | nseq | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | lt | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | lt | DOUBLE | neg.str | pos.str | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | eq | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | eq | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | ne | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | ne | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | nseq | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | nseq | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | lt | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | lt | DOUBLE | neg.nested | pos.dec | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | eq | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | eq | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | ne | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | ne | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | nseq | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | nseq | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | lt | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | lt | DOUBLE | neg.arith | pos.cast | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | eq | FLOAT | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | eq | FLOAT | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | ne | FLOAT | neg.str | pos.str | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | ne | FLOAT | neg.str | pos.str | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | nseq | FLOAT | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | nseq | FLOAT | neg.str | pos.str | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | lt | FLOAT | neg.str | pos.str | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | lt | FLOAT | neg.str | pos.str | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | eq | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | eq | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | ne | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | ne | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | nseq | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | nseq | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge | lt | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'a'], [1, 'm'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |
| native | n/a | v2-cow | merge_clause | lt | DOUBLE | neg.arith | pos.arith | REFUSED | [[1, 'a'], [2, 'b'], [3, 'c']] | REFUSED: This feature is not implemented: Unsupported SQL statement:  |

