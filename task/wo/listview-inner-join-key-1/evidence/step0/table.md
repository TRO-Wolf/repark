# LISTVIEW-INNER-JOIN-KEY-1 step-0 table

Cells: spark 300, main 326. Spark meta: `{"leg": "spark", "python": "3.12.3", "pyarrow": "25.0.1", "pyspark": "4.1.2", "spark_version": "4.1.2", "ansi_enabled": null}`. Main meta: `{"leg": "main", "python": "3.12.3", "pyarrow": "25.0.1", "repark_file": "<FAM_MAIN_WT>/python/repark/src/repark/__init__.py", "debug_assertions": false, "ansi_enabled": null}`.

Labels are against the Spark leg for the same kind, form, join type and dataset. Native forms are compared with the Spark `sql_using` and `sql_on` cells. DML labels are on the persisted rows.

## Counts: label by key type

| key type | CROSS-PRODUCT | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | WRONG-ANSWER | WRONG-WRITE |
|---|---|---|---|---|---|
| FSL2_I64 | 0 | 21 | 2 | 0 | 0 |
| LLV_I64 | 4 | 15 | 3 | 1 | 0 |
| LLV_STRUCT | 4 | 15 | 3 | 1 | 0 |
| LLV_UTF8 | 4 | 15 | 3 | 1 | 0 |
| LL_I64 | 0 | 21 | 2 | 0 | 0 |
| LV_I64 | 4 | 15 | 3 | 1 | 0 |
| LV_STRUCT | 4 | 15 | 3 | 1 | 0 |
| LV_UTF8 | 4 | 15 | 3 | 1 | 0 |
| L_I64 | 0 | 21 | 2 | 0 | 0 |
| L_STRUCT | 0 | 21 | 2 | 0 | 0 |
| L_UTF8 | 0 | 21 | 2 | 0 | 0 |
| NEST_L | 0 | 21 | 2 | 0 | 0 |
| NEST_LV | 4 | 15 | 3 | 1 | 0 |

## Counts: label by join form

| join form | CROSS-PRODUCT | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | WRONG-ANSWER | WRONG-WRITE |
|---|---|---|---|---|---|
| df_expr | 0 | 13 | 0 | 0 | 0 |
| df_expr_and | 0 | 13 | 0 | 0 | 0 |
| df_list | 7 | 6 | 0 | 0 | 0 |
| df_str | 21 | 83 | 0 | 0 | 0 |
| df_two | 0 | 6 | 0 | 7 | 0 |
| native_on | 0 | 0 | 13 | 0 | 0 |
| native_using | 0 | 0 | 13 | 0 | 0 |
| sql_on | 0 | 104 | 0 | 0 | 0 |
| sql_using | 0 | 6 | 7 | 0 | 0 |

## Counts: label by join type

| join type | CROSS-PRODUCT | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | WRONG-ANSWER | WRONG-WRITE |
|---|---|---|---|---|---|
| anti | 0 | 26 | 0 | 0 | 0 |
| full | 0 | 26 | 0 | 0 | 0 |
| inner | 28 | 101 | 33 | 7 | 0 |
| left | 0 | 26 | 0 | 0 | 0 |
| right | 0 | 26 | 0 | 0 | 0 |
| semi | 0 | 26 | 0 | 0 | 0 |

## Counts: label by key type, join form and join type (datasets summed)

| label | key type | join form | join type | cells |
|---|---|---|---|---|
| CROSS-PRODUCT | LLV_I64 | df_list | inner | 1 |
| CROSS-PRODUCT | LLV_I64 | df_str | inner | 3 |
| CROSS-PRODUCT | LLV_STRUCT | df_list | inner | 1 |
| CROSS-PRODUCT | LLV_STRUCT | df_str | inner | 3 |
| CROSS-PRODUCT | LLV_UTF8 | df_list | inner | 1 |
| CROSS-PRODUCT | LLV_UTF8 | df_str | inner | 3 |
| CROSS-PRODUCT | LV_I64 | df_list | inner | 1 |
| CROSS-PRODUCT | LV_I64 | df_str | inner | 3 |
| CROSS-PRODUCT | LV_STRUCT | df_list | inner | 1 |
| CROSS-PRODUCT | LV_STRUCT | df_str | inner | 3 |
| CROSS-PRODUCT | LV_UTF8 | df_list | inner | 1 |
| CROSS-PRODUCT | LV_UTF8 | df_str | inner | 3 |
| CROSS-PRODUCT | NEST_LV | df_list | inner | 1 |
| CROSS-PRODUCT | NEST_LV | df_str | inner | 3 |
| EQUAL | FSL2_I64 | df_expr | inner | 1 |
| EQUAL | FSL2_I64 | df_expr_and | inner | 1 |
| EQUAL | FSL2_I64 | df_list | inner | 1 |
| EQUAL | FSL2_I64 | df_str | anti | 1 |
| EQUAL | FSL2_I64 | df_str | full | 1 |
| EQUAL | FSL2_I64 | df_str | inner | 3 |
| EQUAL | FSL2_I64 | df_str | left | 1 |
| EQUAL | FSL2_I64 | df_str | right | 1 |
| EQUAL | FSL2_I64 | df_str | semi | 1 |
| EQUAL | FSL2_I64 | df_two | inner | 1 |
| EQUAL | FSL2_I64 | sql_on | anti | 1 |
| EQUAL | FSL2_I64 | sql_on | full | 1 |
| EQUAL | FSL2_I64 | sql_on | inner | 3 |
| EQUAL | FSL2_I64 | sql_on | left | 1 |
| EQUAL | FSL2_I64 | sql_on | right | 1 |
| EQUAL | FSL2_I64 | sql_on | semi | 1 |
| EQUAL | FSL2_I64 | sql_using | inner | 1 |
| EQUAL | LLV_I64 | df_expr | inner | 1 |
| EQUAL | LLV_I64 | df_expr_and | inner | 1 |
| EQUAL | LLV_I64 | df_str | anti | 1 |
| EQUAL | LLV_I64 | df_str | full | 1 |
| EQUAL | LLV_I64 | df_str | left | 1 |
| EQUAL | LLV_I64 | df_str | right | 1 |
| EQUAL | LLV_I64 | df_str | semi | 1 |
| EQUAL | LLV_I64 | sql_on | anti | 1 |
| EQUAL | LLV_I64 | sql_on | full | 1 |
| EQUAL | LLV_I64 | sql_on | inner | 3 |
| EQUAL | LLV_I64 | sql_on | left | 1 |
| EQUAL | LLV_I64 | sql_on | right | 1 |
| EQUAL | LLV_I64 | sql_on | semi | 1 |
| EQUAL | LLV_STRUCT | df_expr | inner | 1 |
| EQUAL | LLV_STRUCT | df_expr_and | inner | 1 |
| EQUAL | LLV_STRUCT | df_str | anti | 1 |
| EQUAL | LLV_STRUCT | df_str | full | 1 |
| EQUAL | LLV_STRUCT | df_str | left | 1 |
| EQUAL | LLV_STRUCT | df_str | right | 1 |
| EQUAL | LLV_STRUCT | df_str | semi | 1 |
| EQUAL | LLV_STRUCT | sql_on | anti | 1 |
| EQUAL | LLV_STRUCT | sql_on | full | 1 |
| EQUAL | LLV_STRUCT | sql_on | inner | 3 |
| EQUAL | LLV_STRUCT | sql_on | left | 1 |
| EQUAL | LLV_STRUCT | sql_on | right | 1 |
| EQUAL | LLV_STRUCT | sql_on | semi | 1 |
| EQUAL | LLV_UTF8 | df_expr | inner | 1 |
| EQUAL | LLV_UTF8 | df_expr_and | inner | 1 |
| EQUAL | LLV_UTF8 | df_str | anti | 1 |
| EQUAL | LLV_UTF8 | df_str | full | 1 |
| EQUAL | LLV_UTF8 | df_str | left | 1 |
| EQUAL | LLV_UTF8 | df_str | right | 1 |
| EQUAL | LLV_UTF8 | df_str | semi | 1 |
| EQUAL | LLV_UTF8 | sql_on | anti | 1 |
| EQUAL | LLV_UTF8 | sql_on | full | 1 |
| EQUAL | LLV_UTF8 | sql_on | inner | 3 |
| EQUAL | LLV_UTF8 | sql_on | left | 1 |
| EQUAL | LLV_UTF8 | sql_on | right | 1 |
| EQUAL | LLV_UTF8 | sql_on | semi | 1 |
| EQUAL | LL_I64 | df_expr | inner | 1 |
| EQUAL | LL_I64 | df_expr_and | inner | 1 |
| EQUAL | LL_I64 | df_list | inner | 1 |
| EQUAL | LL_I64 | df_str | anti | 1 |
| EQUAL | LL_I64 | df_str | full | 1 |
| EQUAL | LL_I64 | df_str | inner | 3 |
| EQUAL | LL_I64 | df_str | left | 1 |
| EQUAL | LL_I64 | df_str | right | 1 |
| EQUAL | LL_I64 | df_str | semi | 1 |
| EQUAL | LL_I64 | df_two | inner | 1 |
| EQUAL | LL_I64 | sql_on | anti | 1 |
| EQUAL | LL_I64 | sql_on | full | 1 |
| EQUAL | LL_I64 | sql_on | inner | 3 |
| EQUAL | LL_I64 | sql_on | left | 1 |
| EQUAL | LL_I64 | sql_on | right | 1 |
| EQUAL | LL_I64 | sql_on | semi | 1 |
| EQUAL | LL_I64 | sql_using | inner | 1 |
| EQUAL | LV_I64 | df_expr | inner | 1 |
| EQUAL | LV_I64 | df_expr_and | inner | 1 |
| EQUAL | LV_I64 | df_str | anti | 1 |
| EQUAL | LV_I64 | df_str | full | 1 |
| EQUAL | LV_I64 | df_str | left | 1 |
| EQUAL | LV_I64 | df_str | right | 1 |
| EQUAL | LV_I64 | df_str | semi | 1 |
| EQUAL | LV_I64 | sql_on | anti | 1 |
| EQUAL | LV_I64 | sql_on | full | 1 |
| EQUAL | LV_I64 | sql_on | inner | 3 |
| EQUAL | LV_I64 | sql_on | left | 1 |
| EQUAL | LV_I64 | sql_on | right | 1 |
| EQUAL | LV_I64 | sql_on | semi | 1 |
| EQUAL | LV_STRUCT | df_expr | inner | 1 |
| EQUAL | LV_STRUCT | df_expr_and | inner | 1 |
| EQUAL | LV_STRUCT | df_str | anti | 1 |
| EQUAL | LV_STRUCT | df_str | full | 1 |
| EQUAL | LV_STRUCT | df_str | left | 1 |
| EQUAL | LV_STRUCT | df_str | right | 1 |
| EQUAL | LV_STRUCT | df_str | semi | 1 |
| EQUAL | LV_STRUCT | sql_on | anti | 1 |
| EQUAL | LV_STRUCT | sql_on | full | 1 |
| EQUAL | LV_STRUCT | sql_on | inner | 3 |
| EQUAL | LV_STRUCT | sql_on | left | 1 |
| EQUAL | LV_STRUCT | sql_on | right | 1 |
| EQUAL | LV_STRUCT | sql_on | semi | 1 |
| EQUAL | LV_UTF8 | df_expr | inner | 1 |
| EQUAL | LV_UTF8 | df_expr_and | inner | 1 |
| EQUAL | LV_UTF8 | df_str | anti | 1 |
| EQUAL | LV_UTF8 | df_str | full | 1 |
| EQUAL | LV_UTF8 | df_str | left | 1 |
| EQUAL | LV_UTF8 | df_str | right | 1 |
| EQUAL | LV_UTF8 | df_str | semi | 1 |
| EQUAL | LV_UTF8 | sql_on | anti | 1 |
| EQUAL | LV_UTF8 | sql_on | full | 1 |
| EQUAL | LV_UTF8 | sql_on | inner | 3 |
| EQUAL | LV_UTF8 | sql_on | left | 1 |
| EQUAL | LV_UTF8 | sql_on | right | 1 |
| EQUAL | LV_UTF8 | sql_on | semi | 1 |
| EQUAL | L_I64 | df_expr | inner | 1 |
| EQUAL | L_I64 | df_expr_and | inner | 1 |
| EQUAL | L_I64 | df_list | inner | 1 |
| EQUAL | L_I64 | df_str | anti | 1 |
| EQUAL | L_I64 | df_str | full | 1 |
| EQUAL | L_I64 | df_str | inner | 3 |
| EQUAL | L_I64 | df_str | left | 1 |
| EQUAL | L_I64 | df_str | right | 1 |
| EQUAL | L_I64 | df_str | semi | 1 |
| EQUAL | L_I64 | df_two | inner | 1 |
| EQUAL | L_I64 | sql_on | anti | 1 |
| EQUAL | L_I64 | sql_on | full | 1 |
| EQUAL | L_I64 | sql_on | inner | 3 |
| EQUAL | L_I64 | sql_on | left | 1 |
| EQUAL | L_I64 | sql_on | right | 1 |
| EQUAL | L_I64 | sql_on | semi | 1 |
| EQUAL | L_I64 | sql_using | inner | 1 |
| EQUAL | L_STRUCT | df_expr | inner | 1 |
| EQUAL | L_STRUCT | df_expr_and | inner | 1 |
| EQUAL | L_STRUCT | df_list | inner | 1 |
| EQUAL | L_STRUCT | df_str | anti | 1 |
| EQUAL | L_STRUCT | df_str | full | 1 |
| EQUAL | L_STRUCT | df_str | inner | 3 |
| EQUAL | L_STRUCT | df_str | left | 1 |
| EQUAL | L_STRUCT | df_str | right | 1 |
| EQUAL | L_STRUCT | df_str | semi | 1 |
| EQUAL | L_STRUCT | df_two | inner | 1 |
| EQUAL | L_STRUCT | sql_on | anti | 1 |
| EQUAL | L_STRUCT | sql_on | full | 1 |
| EQUAL | L_STRUCT | sql_on | inner | 3 |
| EQUAL | L_STRUCT | sql_on | left | 1 |
| EQUAL | L_STRUCT | sql_on | right | 1 |
| EQUAL | L_STRUCT | sql_on | semi | 1 |
| EQUAL | L_STRUCT | sql_using | inner | 1 |
| EQUAL | L_UTF8 | df_expr | inner | 1 |
| EQUAL | L_UTF8 | df_expr_and | inner | 1 |
| EQUAL | L_UTF8 | df_list | inner | 1 |
| EQUAL | L_UTF8 | df_str | anti | 1 |
| EQUAL | L_UTF8 | df_str | full | 1 |
| EQUAL | L_UTF8 | df_str | inner | 3 |
| EQUAL | L_UTF8 | df_str | left | 1 |
| EQUAL | L_UTF8 | df_str | right | 1 |
| EQUAL | L_UTF8 | df_str | semi | 1 |
| EQUAL | L_UTF8 | df_two | inner | 1 |
| EQUAL | L_UTF8 | sql_on | anti | 1 |
| EQUAL | L_UTF8 | sql_on | full | 1 |
| EQUAL | L_UTF8 | sql_on | inner | 3 |
| EQUAL | L_UTF8 | sql_on | left | 1 |
| EQUAL | L_UTF8 | sql_on | right | 1 |
| EQUAL | L_UTF8 | sql_on | semi | 1 |
| EQUAL | L_UTF8 | sql_using | inner | 1 |
| EQUAL | NEST_L | df_expr | inner | 1 |
| EQUAL | NEST_L | df_expr_and | inner | 1 |
| EQUAL | NEST_L | df_list | inner | 1 |
| EQUAL | NEST_L | df_str | anti | 1 |
| EQUAL | NEST_L | df_str | full | 1 |
| EQUAL | NEST_L | df_str | inner | 3 |
| EQUAL | NEST_L | df_str | left | 1 |
| EQUAL | NEST_L | df_str | right | 1 |
| EQUAL | NEST_L | df_str | semi | 1 |
| EQUAL | NEST_L | df_two | inner | 1 |
| EQUAL | NEST_L | sql_on | anti | 1 |
| EQUAL | NEST_L | sql_on | full | 1 |
| EQUAL | NEST_L | sql_on | inner | 3 |
| EQUAL | NEST_L | sql_on | left | 1 |
| EQUAL | NEST_L | sql_on | right | 1 |
| EQUAL | NEST_L | sql_on | semi | 1 |
| EQUAL | NEST_L | sql_using | inner | 1 |
| EQUAL | NEST_LV | df_expr | inner | 1 |
| EQUAL | NEST_LV | df_expr_and | inner | 1 |
| EQUAL | NEST_LV | df_str | anti | 1 |
| EQUAL | NEST_LV | df_str | full | 1 |
| EQUAL | NEST_LV | df_str | left | 1 |
| EQUAL | NEST_LV | df_str | right | 1 |
| EQUAL | NEST_LV | df_str | semi | 1 |
| EQUAL | NEST_LV | sql_on | anti | 1 |
| EQUAL | NEST_LV | sql_on | full | 1 |
| EQUAL | NEST_LV | sql_on | inner | 3 |
| EQUAL | NEST_LV | sql_on | left | 1 |
| EQUAL | NEST_LV | sql_on | right | 1 |
| EQUAL | NEST_LV | sql_on | semi | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | FSL2_I64 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | FSL2_I64 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_I64 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_I64 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_I64 | sql_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_STRUCT | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_STRUCT | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_STRUCT | sql_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_UTF8 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_UTF8 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LLV_UTF8 | sql_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LL_I64 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LL_I64 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_I64 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_I64 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_I64 | sql_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_STRUCT | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_STRUCT | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_STRUCT | sql_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_UTF8 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_UTF8 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | LV_UTF8 | sql_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | L_I64 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | L_I64 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | L_STRUCT | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | L_STRUCT | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | L_UTF8 | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | L_UTF8 | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | NEST_L | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | NEST_L | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | NEST_LV | native_on | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | NEST_LV | native_using | inner | 1 |
| REFUSED-WHERE-SPARK-ANSWERED | NEST_LV | sql_using | inner | 1 |
| WRONG-ANSWER | LLV_I64 | df_two | inner | 1 |
| WRONG-ANSWER | LLV_STRUCT | df_two | inner | 1 |
| WRONG-ANSWER | LLV_UTF8 | df_two | inner | 1 |
| WRONG-ANSWER | LV_I64 | df_two | inner | 1 |
| WRONG-ANSWER | LV_STRUCT | df_two | inner | 1 |
| WRONG-ANSWER | LV_UTF8 | df_two | inner | 1 |
| WRONG-ANSWER | NEST_LV | df_two | inner | 1 |

## Wrong list (WRONG-ANSWER or WRONG-WRITE, excluding cross products)

- `join|LV_I64|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=5 sha=5034a392ff8c453f
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|LV_I64` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|LV_I64` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=f858915ec289e06e
  - main sample: `['[56, 252]']` spark sample: `['[9, 36]']`
- `join|LLV_I64|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=5 sha=5034a392ff8c453f
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|LLV_I64` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|LLV_I64` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=f858915ec289e06e
  - main sample: `['[56, 252]']` spark sample: `['[9, 36]']`
- `join|LV_UTF8|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=5 sha=5034a392ff8c453f
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|LV_UTF8` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|LV_UTF8` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=f858915ec289e06e
  - main sample: `['[56, 252]']` spark sample: `['[9, 36]']`
- `join|LLV_UTF8|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=5 sha=5034a392ff8c453f
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|LLV_UTF8` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|LLV_UTF8` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=f858915ec289e06e
  - main sample: `['[56, 252]']` spark sample: `['[9, 36]']`
- `join|LV_STRUCT|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=5 sha=5034a392ff8c453f
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|LV_STRUCT` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|LV_STRUCT` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=f858915ec289e06e
  - main sample: `['[56, 252]']` spark sample: `['[9, 36]']`
- `join|LLV_STRUCT|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=5 sha=5034a392ff8c453f
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|LLV_STRUCT` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|LLV_STRUCT` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=f858915ec289e06e
  - main sample: `['[56, 252]']` spark sample: `['[9, 36]']`
- `join|NEST_LV|df_two|inner|base` WRONG-ANSWER main: ok n=24 sha=9e9914f4ab9e6a0e / spark: ok n=6 sha=5a484148f9734c37
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `dml|wt|NEST_LV` WRONG-WRITE main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=10 sha=d09cfe68f10f46cc
  - main sample: `['[1, 10]']` spark sample: `['[1, 10]']`
- `agg|NEST_LV` WRONG-ANSWER main: ok n=1 sha=df7d72e06bb04ce1 / spark: ok n=1 sha=aeb7ed64fd7ef730
  - main sample: `['[56, 252]']` spark sample: `['[10, 41]']`

## Cross-product list (row count equals left x right)

- `join|LV_I64|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LV_I64|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LV_I64|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|LV_I64|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=122408 sha=bc0deca67e41ae70
  - main sample: `['[1, 10001]']`
- `join|LLV_I64|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LLV_I64|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LLV_I64|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|LLV_I64|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=122408 sha=bc0deca67e41ae70
  - main sample: `['[1, 10001]']`
- `join|LV_UTF8|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LV_UTF8|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LV_UTF8|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|LV_UTF8|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=122408 sha=bc0deca67e41ae70
  - main sample: `['[1, 10001]']`
- `join|LLV_UTF8|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LLV_UTF8|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LLV_UTF8|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|LLV_UTF8|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=122408 sha=bc0deca67e41ae70
  - main sample: `['[1, 10001]']`
- `join|LV_STRUCT|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LV_STRUCT|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LV_STRUCT|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|LV_STRUCT|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=122408 sha=bc0deca67e41ae70
  - main sample: `['[1, 10001]']`
- `join|LLV_STRUCT|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LLV_STRUCT|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=9 sha=2de42b07ea3770a9
  - main sample: `['[1, 10]']`
- `join|LLV_STRUCT|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|LLV_STRUCT|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=122408 sha=bc0deca67e41ae70
  - main sample: `['[1, 10001]']`
- `join|NEST_LV|df_str|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=10 sha=d09cfe68f10f46cc
  - main sample: `['[1, 10]']`
- `join|NEST_LV|df_list|inner|base` main: ok n=56 sha=3c4f4b9b2315b7aa / spark: ok n=10 sha=d09cfe68f10f46cc
  - main sample: `['[1, 10]']`
- `join|NEST_LV|df_str|inner|one` main: ok n=1 sha=e2efbdac0b9ad4bd / spark: ok n=0 sha=e3b0c44298fc1c14
  - main sample: `['[1, 10]']`
- `join|NEST_LV|df_str|inner|big` main: ok n=1000000 sha=f45219edf5816eca / spark: ok n=142857 sha=ef8c73243aae27fd
  - main sample: `['[1, 10001]']`

## Cell table (main leg, labelled)

| cell | key left | key right | main outcome | spark outcome | label | kind invariance |
|---|---|---|---|---|---|---|
| join|LV_I64|df_str|inner|base | list_view<item: int64> | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_I64|df_list|inner|base | list_view<item: int64> | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_I64|df_expr|inner|base | list_view<item: int64> | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LV_I64|df_expr_and|inner|base | list_view<item: int64> | list_view<item: int64> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | INVARIANT |
| join|LV_I64|sql_using|inner|base | list_view<item: int64> | list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|LV_I64|sql_on|inner|base | list_view<item: int64> | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LV_I64|df_str|left|base | list_view<item: int64> | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LV_I64|df_str|right|base | list_view<item: int64> | list_view<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LV_I64|df_str|full|base | list_view<item: int64> | list_view<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LV_I64|df_str|semi|base | list_view<item: int64> | list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LV_I64|df_str|anti|base | list_view<item: int64> | list_view<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LV_I64|sql_on|left|base | list_view<item: int64> | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LV_I64|sql_on|right|base | list_view<item: int64> | list_view<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LV_I64|sql_on|full|base | list_view<item: int64> | list_view<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LV_I64|sql_on|semi|base | list_view<item: int64> | list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LV_I64|sql_on|anti|base | list_view<item: int64> | list_view<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LV_I64|df_two|inner|base | list_view<item: int64> | list_view<item: int64> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER | VARIANT-DIFFERS |
| join|LV_I64|native_using|inner|base | list_view<item: int64> | list_view<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|LV_I64|native_on|inner|base | list_view<item: int64> | list_view<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|LV_I64 | list_view<item: int64> | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | - |
| dml|wt|LV_I64 | list_view<item: int64> | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE | - |
| agg|LV_I64 | list_view<item: int64> | list_view<item: int64> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER | - |
| join|LV_I64|df_str|inner|one | list_view<item: int64> | list_view<item: int64> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_I64|sql_on|inner|one | list_view<item: int64> | list_view<item: int64> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|LV_I64|df_str|inner|big | list_view<item: int64> | list_view<item: int64> | ok n=1000000 sha=f45219edf5816eca | ok n=122408 sha=bc0deca67e41ae70 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_I64|sql_on|inner|big | list_view<item: int64> | list_view<item: int64> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | INVARIANT |
| join|LLV_I64|df_str|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_I64|df_list|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_I64|df_expr|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LLV_I64|df_expr_and|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | INVARIANT |
| join|LLV_I64|sql_using|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|LLV_I64|sql_on|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LLV_I64|df_str|left|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LLV_I64|df_str|right|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LLV_I64|df_str|full|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LLV_I64|df_str|semi|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LLV_I64|df_str|anti|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LLV_I64|sql_on|left|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LLV_I64|sql_on|right|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LLV_I64|sql_on|full|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LLV_I64|sql_on|semi|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LLV_I64|sql_on|anti|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LLV_I64|df_two|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER | VARIANT-DIFFERS |
| join|LLV_I64|native_using|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|LLV_I64|native_on|inner|base | large_list_view<item: int64> | large_list_view<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|LLV_I64 | large_list_view<item: int64> | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | - |
| dml|wt|LLV_I64 | large_list_view<item: int64> | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE | - |
| agg|LLV_I64 | large_list_view<item: int64> | large_list_view<item: int64> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER | - |
| join|LLV_I64|df_str|inner|one | large_list_view<item: int64> | large_list_view<item: int64> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_I64|sql_on|inner|one | large_list_view<item: int64> | large_list_view<item: int64> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|LLV_I64|df_str|inner|big | large_list_view<item: int64> | large_list_view<item: int64> | ok n=1000000 sha=f45219edf5816eca | ok n=122408 sha=bc0deca67e41ae70 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_I64|sql_on|inner|big | large_list_view<item: int64> | large_list_view<item: int64> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|inner|base | list_view<item: string> | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_UTF8|df_list|inner|base | list_view<item: string> | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_UTF8|df_expr|inner|base | list_view<item: string> | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LV_UTF8|df_expr_and|inner|base | list_view<item: string> | list_view<item: string> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | INVARIANT |
| join|LV_UTF8|sql_using|inner|base | list_view<item: string> | list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|LV_UTF8|sql_on|inner|base | list_view<item: string> | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|left|base | list_view<item: string> | list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|right|base | list_view<item: string> | list_view<item: string> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|full|base | list_view<item: string> | list_view<item: string> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|semi|base | list_view<item: string> | list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|anti|base | list_view<item: string> | list_view<item: string> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LV_UTF8|sql_on|left|base | list_view<item: string> | list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LV_UTF8|sql_on|right|base | list_view<item: string> | list_view<item: string> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LV_UTF8|sql_on|full|base | list_view<item: string> | list_view<item: string> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LV_UTF8|sql_on|semi|base | list_view<item: string> | list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LV_UTF8|sql_on|anti|base | list_view<item: string> | list_view<item: string> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LV_UTF8|df_two|inner|base | list_view<item: string> | list_view<item: string> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER | VARIANT-DIFFERS |
| join|LV_UTF8|native_using|inner|base | list_view<item: string> | list_view<item: string> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|LV_UTF8|native_on|inner|base | list_view<item: string> | list_view<item: string> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|LV_UTF8 | list_view<item: string> | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | - |
| dml|wt|LV_UTF8 | list_view<item: string> | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE | - |
| agg|LV_UTF8 | list_view<item: string> | list_view<item: string> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER | - |
| join|LV_UTF8|df_str|inner|one | list_view<item: string> | list_view<item: string> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_UTF8|sql_on|inner|one | list_view<item: string> | list_view<item: string> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|LV_UTF8|df_str|inner|big | list_view<item: string> | list_view<item: string> | ok n=1000000 sha=f45219edf5816eca | ok n=122408 sha=bc0deca67e41ae70 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_UTF8|sql_on|inner|big | list_view<item: string> | list_view<item: string> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|inner|base | large_list_view<item: string> | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_UTF8|df_list|inner|base | large_list_view<item: string> | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_UTF8|df_expr|inner|base | large_list_view<item: string> | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_expr_and|inner|base | large_list_view<item: string> | large_list_view<item: string> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | INVARIANT |
| join|LLV_UTF8|sql_using|inner|base | large_list_view<item: string> | large_list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|LLV_UTF8|sql_on|inner|base | large_list_view<item: string> | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|left|base | large_list_view<item: string> | large_list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|right|base | large_list_view<item: string> | large_list_view<item: string> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|full|base | large_list_view<item: string> | large_list_view<item: string> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|semi|base | large_list_view<item: string> | large_list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|anti|base | large_list_view<item: string> | large_list_view<item: string> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LLV_UTF8|sql_on|left|base | large_list_view<item: string> | large_list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LLV_UTF8|sql_on|right|base | large_list_view<item: string> | large_list_view<item: string> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LLV_UTF8|sql_on|full|base | large_list_view<item: string> | large_list_view<item: string> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LLV_UTF8|sql_on|semi|base | large_list_view<item: string> | large_list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LLV_UTF8|sql_on|anti|base | large_list_view<item: string> | large_list_view<item: string> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_two|inner|base | large_list_view<item: string> | large_list_view<item: string> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER | VARIANT-DIFFERS |
| join|LLV_UTF8|native_using|inner|base | large_list_view<item: string> | large_list_view<item: string> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|LLV_UTF8|native_on|inner|base | large_list_view<item: string> | large_list_view<item: string> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|LLV_UTF8 | large_list_view<item: string> | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | - |
| dml|wt|LLV_UTF8 | large_list_view<item: string> | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE | - |
| agg|LLV_UTF8 | large_list_view<item: string> | large_list_view<item: string> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER | - |
| join|LLV_UTF8|df_str|inner|one | large_list_view<item: string> | large_list_view<item: string> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_UTF8|sql_on|inner|one | large_list_view<item: string> | large_list_view<item: string> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|LLV_UTF8|df_str|inner|big | large_list_view<item: string> | large_list_view<item: string> | ok n=1000000 sha=f45219edf5816eca | ok n=122408 sha=bc0deca67e41ae70 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_UTF8|sql_on|inner|big | large_list_view<item: string> | large_list_view<item: string> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_STRUCT|df_list|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_STRUCT|df_expr|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_expr_and|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | INVARIANT |
| join|LV_STRUCT|sql_using|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|LV_STRUCT|sql_on|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|left|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|right|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|full|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|semi|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|anti|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LV_STRUCT|sql_on|left|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LV_STRUCT|sql_on|right|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LV_STRUCT|sql_on|full|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LV_STRUCT|sql_on|semi|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LV_STRUCT|sql_on|anti|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_two|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER | VARIANT-DIFFERS |
| join|LV_STRUCT|native_using|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|LV_STRUCT|native_on|inner|base | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|LV_STRUCT | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | - |
| dml|wt|LV_STRUCT | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE | - |
| agg|LV_STRUCT | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER | - |
| join|LV_STRUCT|df_str|inner|one | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_STRUCT|sql_on|inner|one | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|LV_STRUCT|df_str|inner|big | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=1000000 sha=f45219edf5816eca | ok n=122408 sha=bc0deca67e41ae70 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LV_STRUCT|sql_on|inner|big | list_view<item: struct<x: int64, y: string>> | list_view<item: struct<x: int64, y: string>> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_STRUCT|df_list|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_STRUCT|df_expr|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_expr_and|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | INVARIANT |
| join|LLV_STRUCT|sql_using|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|LLV_STRUCT|sql_on|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|left|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|right|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|full|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|semi|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|anti|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LLV_STRUCT|sql_on|left|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | INVARIANT |
| join|LLV_STRUCT|sql_on|right|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | INVARIANT |
| join|LLV_STRUCT|sql_on|full|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | INVARIANT |
| join|LLV_STRUCT|sql_on|semi|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | INVARIANT |
| join|LLV_STRUCT|sql_on|anti|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_two|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER | VARIANT-DIFFERS |
| join|LLV_STRUCT|native_using|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|LLV_STRUCT|native_on|inner|base | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|LLV_STRUCT | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | - |
| dml|wt|LLV_STRUCT | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE | - |
| agg|LLV_STRUCT | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER | - |
| join|LLV_STRUCT|df_str|inner|one | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_STRUCT|sql_on|inner|one | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|LLV_STRUCT|df_str|inner|big | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=1000000 sha=f45219edf5816eca | ok n=122408 sha=bc0deca67e41ae70 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|LLV_STRUCT|sql_on|inner|big | large_list_view<item: struct<x: int64, y: string>> | large_list_view<item: struct<x: int64, y: string>> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | INVARIANT |
| join|L_I64|df_str|inner|base | list<item: int64> | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_I64|df_list|inner|base | list<item: int64> | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_I64|df_expr|inner|base | list<item: int64> | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_I64|df_expr_and|inner|base | list<item: int64> | list<item: int64> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | NO-TWIN |
| join|L_I64|sql_using|inner|base | list<item: int64> | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_I64|sql_on|inner|base | list<item: int64> | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_I64|df_str|left|base | list<item: int64> | list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|L_I64|df_str|right|base | list<item: int64> | list<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|L_I64|df_str|full|base | list<item: int64> | list<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|L_I64|df_str|semi|base | list<item: int64> | list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|L_I64|df_str|anti|base | list<item: int64> | list<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|L_I64|sql_on|left|base | list<item: int64> | list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|L_I64|sql_on|right|base | list<item: int64> | list<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|L_I64|sql_on|full|base | list<item: int64> | list<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|L_I64|sql_on|semi|base | list<item: int64> | list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|L_I64|sql_on|anti|base | list<item: int64> | list<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|L_I64|df_two|inner|base | list<item: int64> | list<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL | NO-TWIN |
| join|L_I64|native_using|inner|base | list<item: int64> | list<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| join|L_I64|native_on|inner|base | list<item: int64> | list<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| agg|L_I64 | list<item: int64> | list<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL | - |
| join|L_I64|df_str|inner|one | list<item: int64> | list<item: int64> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|L_I64|sql_on|inner|one | list<item: int64> | list<item: int64> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|L_I64|df_str|inner|big | list<item: int64> | list<item: int64> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|L_I64|sql_on|inner|big | list<item: int64> | list<item: int64> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|LL_I64|df_str|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|LL_I64|df_list|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|LL_I64|df_expr|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|LL_I64|df_expr_and|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | NO-TWIN |
| join|LL_I64|sql_using|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|LL_I64|df_str|left|base | large_list<item: int64> | large_list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|LL_I64|df_str|right|base | large_list<item: int64> | large_list<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|LL_I64|df_str|full|base | large_list<item: int64> | large_list<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|LL_I64|df_str|semi|base | large_list<item: int64> | large_list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|LL_I64|df_str|anti|base | large_list<item: int64> | large_list<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|left|base | large_list<item: int64> | large_list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|right|base | large_list<item: int64> | large_list<item: int64> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|full|base | large_list<item: int64> | large_list<item: int64> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|semi|base | large_list<item: int64> | large_list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|anti|base | large_list<item: int64> | large_list<item: int64> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|LL_I64|df_two|inner|base | large_list<item: int64> | large_list<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL | NO-TWIN |
| join|LL_I64|native_using|inner|base | large_list<item: int64> | large_list<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| join|LL_I64|native_on|inner|base | large_list<item: int64> | large_list<item: int64> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| agg|LL_I64 | large_list<item: int64> | large_list<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL | - |
| join|LL_I64|df_str|inner|one | large_list<item: int64> | large_list<item: int64> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|inner|one | large_list<item: int64> | large_list<item: int64> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|LL_I64|df_str|inner|big | large_list<item: int64> | large_list<item: int64> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|LL_I64|sql_on|inner|big | large_list<item: int64> | large_list<item: int64> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_list|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_expr|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_expr_and|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_using|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|left|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|right|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|full|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|semi|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|anti|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|left|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|right|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|full|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|semi|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|anti|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_two|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL | NO-TWIN |
| join|FSL2_I64|native_using|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| join|FSL2_I64|native_on|inner|base | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| agg|FSL2_I64 | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL | - |
| join|FSL2_I64|df_str|inner|one | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|inner|one | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|FSL2_I64|df_str|inner|big | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|FSL2_I64|sql_on|inner|big | fixed_size_list<item: int64>[2] | fixed_size_list<item: int64>[2] | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|inner|base | list<item: string> | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_UTF8|df_list|inner|base | list<item: string> | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_UTF8|df_expr|inner|base | list<item: string> | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_UTF8|df_expr_and|inner|base | list<item: string> | list<item: string> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | NO-TWIN |
| join|L_UTF8|sql_using|inner|base | list<item: string> | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|inner|base | list<item: string> | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|left|base | list<item: string> | list<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|right|base | list<item: string> | list<item: string> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|full|base | list<item: string> | list<item: string> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|semi|base | list<item: string> | list<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|anti|base | list<item: string> | list<item: string> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|left|base | list<item: string> | list<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|right|base | list<item: string> | list<item: string> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|full|base | list<item: string> | list<item: string> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|semi|base | list<item: string> | list<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|anti|base | list<item: string> | list<item: string> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|L_UTF8|df_two|inner|base | list<item: string> | list<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL | NO-TWIN |
| join|L_UTF8|native_using|inner|base | list<item: string> | list<item: string> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| join|L_UTF8|native_on|inner|base | list<item: string> | list<item: string> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| agg|L_UTF8 | list<item: string> | list<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL | - |
| join|L_UTF8|df_str|inner|one | list<item: string> | list<item: string> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|inner|one | list<item: string> | list<item: string> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|L_UTF8|df_str|inner|big | list<item: string> | list<item: string> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|L_UTF8|sql_on|inner|big | list<item: string> | list<item: string> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_list|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_expr|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_expr_and|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=7 sha=2f8e5ac41e2d1a2c | ok n=7 sha=2f8e5ac41e2d1a2c | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_using|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|left|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|right|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|full|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|semi|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|anti|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|left|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|right|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=11 sha=fc5d886ea14d6fef | ok n=11 sha=fc5d886ea14d6fef | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|full|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=13 sha=e856210e1e957cdf | ok n=13 sha=e856210e1e957cdf | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|semi|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|anti|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=2 sha=294e0503a4956969 | ok n=2 sha=294e0503a4956969 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_two|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL | NO-TWIN |
| join|L_STRUCT|native_using|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| join|L_STRUCT|native_on|inner|base | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| agg|L_STRUCT | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL | - |
| join|L_STRUCT|df_str|inner|one | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|inner|one | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|L_STRUCT|df_str|inner|big | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|L_STRUCT|sql_on|inner|big | list<item: struct<x: int64, y: string>> | list<item: struct<x: int64, y: string>> | ok n=122408 sha=bc0deca67e41ae70 | ok n=122408 sha=bc0deca67e41ae70 | EQUAL | NO-TWIN |
| join|NEST_LV|df_str|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|NEST_LV|df_list|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|NEST_LV|df_expr|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | INVARIANT |
| join|NEST_LV|df_expr_and|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=8 sha=804103abac24bc34 | ok n=8 sha=804103abac24bc34 | EQUAL | INVARIANT |
| join|NEST_LV|sql_using|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED | VARIANT-DIFFERS |
| join|NEST_LV|sql_on|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | INVARIANT |
| join|NEST_LV|df_str|left|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL | INVARIANT |
| join|NEST_LV|df_str|right|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=11 sha=c35e708197e2843c | ok n=11 sha=c35e708197e2843c | EQUAL | INVARIANT |
| join|NEST_LV|df_str|full|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=12 sha=b58373b567f9aed3 | ok n=12 sha=b58373b567f9aed3 | EQUAL | INVARIANT |
| join|NEST_LV|df_str|semi|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL | INVARIANT |
| join|NEST_LV|df_str|anti|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=1 sha=589ffd3acf522ae8 | ok n=1 sha=589ffd3acf522ae8 | EQUAL | INVARIANT |
| join|NEST_LV|sql_on|left|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL | INVARIANT |
| join|NEST_LV|sql_on|right|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=11 sha=c35e708197e2843c | ok n=11 sha=c35e708197e2843c | EQUAL | INVARIANT |
| join|NEST_LV|sql_on|full|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=12 sha=b58373b567f9aed3 | ok n=12 sha=b58373b567f9aed3 | EQUAL | INVARIANT |
| join|NEST_LV|sql_on|semi|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL | INVARIANT |
| join|NEST_LV|sql_on|anti|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=1 sha=589ffd3acf522ae8 | ok n=1 sha=589ffd3acf522ae8 | EQUAL | INVARIANT |
| join|NEST_LV|df_two|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=6 sha=5a484148f9734c37 | WRONG-ANSWER | VARIANT-DIFFERS |
| join|NEST_LV|native_using|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| join|NEST_LV|native_on|inner|base | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED | INVARIANT |
| dml|ins|NEST_LV | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | - |
| dml|wt|NEST_LV | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | WRONG-WRITE | - |
| agg|NEST_LV | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=aeb7ed64fd7ef730 | WRONG-ANSWER | - |
| join|NEST_LV|df_str|inner|one | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=1 sha=e2efbdac0b9ad4bd | ok n=0 sha=e3b0c44298fc1c14 | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|NEST_LV|sql_on|inner|one | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | INVARIANT |
| join|NEST_LV|df_str|inner|big | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=1000000 sha=f45219edf5816eca | ok n=142857 sha=ef8c73243aae27fd | CROSS-PRODUCT | VARIANT-DIFFERS |
| join|NEST_LV|sql_on|inner|big | struct<a: list_view<item: int64>> | struct<a: list_view<item: int64>> | ok n=142857 sha=ef8c73243aae27fd | ok n=142857 sha=ef8c73243aae27fd | EQUAL | INVARIANT |
| join|NEST_L|df_str|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | NO-TWIN |
| join|NEST_L|df_list|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | NO-TWIN |
| join|NEST_L|df_expr|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | NO-TWIN |
| join|NEST_L|df_expr_and|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=8 sha=804103abac24bc34 | ok n=8 sha=804103abac24bc34 | EQUAL | NO-TWIN |
| join|NEST_L|sql_using|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL | NO-TWIN |
| join|NEST_L|df_str|left|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL | NO-TWIN |
| join|NEST_L|df_str|right|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=11 sha=c35e708197e2843c | ok n=11 sha=c35e708197e2843c | EQUAL | NO-TWIN |
| join|NEST_L|df_str|full|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=12 sha=b58373b567f9aed3 | ok n=12 sha=b58373b567f9aed3 | EQUAL | NO-TWIN |
| join|NEST_L|df_str|semi|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL | NO-TWIN |
| join|NEST_L|df_str|anti|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=1 sha=589ffd3acf522ae8 | ok n=1 sha=589ffd3acf522ae8 | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|left|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|right|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=11 sha=c35e708197e2843c | ok n=11 sha=c35e708197e2843c | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|full|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=12 sha=b58373b567f9aed3 | ok n=12 sha=b58373b567f9aed3 | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|semi|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|anti|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=1 sha=589ffd3acf522ae8 | ok n=1 sha=589ffd3acf522ae8 | EQUAL | NO-TWIN |
| join|NEST_L|df_two|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL | NO-TWIN |
| join|NEST_L|native_using|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| join|NEST_L|native_on|inner|base | struct<a: list<item: int64>> | struct<a: list<item: int64>> | AnalysisException: Error during planning: table 'datafusion.public.lv_l' not found | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED | NO-TWIN |
| agg|NEST_L | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL | - |
| join|NEST_L|df_str|inner|one | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|inner|one | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL | NO-TWIN |
| join|NEST_L|df_str|inner|big | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=142857 sha=ef8c73243aae27fd | ok n=142857 sha=ef8c73243aae27fd | EQUAL | NO-TWIN |
| join|NEST_L|sql_on|inner|big | struct<a: list<item: int64>> | struct<a: list<item: int64>> | ok n=142857 sha=ef8c73243aae27fd | ok n=142857 sha=ef8c73243aae27fd | EQUAL | NO-TWIN |

## Cell table (spark oracle leg)

| cell | key left | key right | spark outcome | label |
|---|---|---|---|---|
| join|LV_I64|df_str|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_I64|df_list|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_I64|df_expr|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_I64|df_expr_and|inner|base | array<bigint> | array<bigint> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LV_I64|sql_using|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_I64|sql_on|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_I64|df_str|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LV_I64|df_str|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LV_I64|df_str|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LV_I64|df_str|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LV_I64|df_str|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LV_I64|sql_on|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LV_I64|sql_on|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LV_I64|sql_on|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LV_I64|sql_on|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LV_I64|sql_on|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LV_I64|df_two|inner|base | array<bigint> | array<bigint> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| dml|ins|LV_I64 | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| dml|wt|LV_I64 | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| agg|LV_I64 | array<bigint> | array<bigint> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LV_I64|df_str|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LV_I64|sql_on|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LV_I64|df_str|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LV_I64|sql_on|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LLV_I64|df_str|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_I64|df_list|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_I64|df_expr|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_I64|df_expr_and|inner|base | array<bigint> | array<bigint> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LLV_I64|sql_using|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_I64|sql_on|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_I64|df_str|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LLV_I64|df_str|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LLV_I64|df_str|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LLV_I64|df_str|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LLV_I64|df_str|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LLV_I64|sql_on|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LLV_I64|sql_on|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LLV_I64|sql_on|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LLV_I64|sql_on|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LLV_I64|sql_on|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LLV_I64|df_two|inner|base | array<bigint> | array<bigint> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| dml|ins|LLV_I64 | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| dml|wt|LLV_I64 | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| agg|LLV_I64 | array<bigint> | array<bigint> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LLV_I64|df_str|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LLV_I64|sql_on|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LLV_I64|df_str|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LLV_I64|sql_on|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LV_UTF8|df_str|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_UTF8|df_list|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_UTF8|df_expr|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_UTF8|df_expr_and|inner|base | array<string> | array<string> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LV_UTF8|sql_using|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_UTF8|sql_on|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_UTF8|df_str|left|base | array<string> | array<string> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LV_UTF8|df_str|right|base | array<string> | array<string> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LV_UTF8|df_str|full|base | array<string> | array<string> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LV_UTF8|df_str|semi|base | array<string> | array<string> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LV_UTF8|df_str|anti|base | array<string> | array<string> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LV_UTF8|sql_on|left|base | array<string> | array<string> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LV_UTF8|sql_on|right|base | array<string> | array<string> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LV_UTF8|sql_on|full|base | array<string> | array<string> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LV_UTF8|sql_on|semi|base | array<string> | array<string> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LV_UTF8|sql_on|anti|base | array<string> | array<string> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LV_UTF8|df_two|inner|base | array<string> | array<string> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| dml|ins|LV_UTF8 | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| dml|wt|LV_UTF8 | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| agg|LV_UTF8 | array<string> | array<string> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LV_UTF8|df_str|inner|one | array<string> | array<string> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LV_UTF8|sql_on|inner|one | array<string> | array<string> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LV_UTF8|df_str|inner|big | array<string> | array<string> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LV_UTF8|sql_on|inner|big | array<string> | array<string> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LLV_UTF8|df_str|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_UTF8|df_list|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_UTF8|df_expr|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_UTF8|df_expr_and|inner|base | array<string> | array<string> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LLV_UTF8|sql_using|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_UTF8|sql_on|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_UTF8|df_str|left|base | array<string> | array<string> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LLV_UTF8|df_str|right|base | array<string> | array<string> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LLV_UTF8|df_str|full|base | array<string> | array<string> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LLV_UTF8|df_str|semi|base | array<string> | array<string> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LLV_UTF8|df_str|anti|base | array<string> | array<string> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LLV_UTF8|sql_on|left|base | array<string> | array<string> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LLV_UTF8|sql_on|right|base | array<string> | array<string> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LLV_UTF8|sql_on|full|base | array<string> | array<string> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LLV_UTF8|sql_on|semi|base | array<string> | array<string> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LLV_UTF8|sql_on|anti|base | array<string> | array<string> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LLV_UTF8|df_two|inner|base | array<string> | array<string> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| dml|ins|LLV_UTF8 | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| dml|wt|LLV_UTF8 | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| agg|LLV_UTF8 | array<string> | array<string> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LLV_UTF8|df_str|inner|one | array<string> | array<string> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LLV_UTF8|sql_on|inner|one | array<string> | array<string> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LLV_UTF8|df_str|inner|big | array<string> | array<string> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LLV_UTF8|sql_on|inner|big | array<string> | array<string> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LV_STRUCT|df_str|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_STRUCT|df_list|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_STRUCT|df_expr|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_STRUCT|df_expr_and|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LV_STRUCT|sql_using|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_STRUCT|sql_on|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LV_STRUCT|df_str|left|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LV_STRUCT|df_str|right|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LV_STRUCT|df_str|full|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LV_STRUCT|df_str|semi|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LV_STRUCT|df_str|anti|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LV_STRUCT|sql_on|left|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LV_STRUCT|sql_on|right|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LV_STRUCT|sql_on|full|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LV_STRUCT|sql_on|semi|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LV_STRUCT|sql_on|anti|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LV_STRUCT|df_two|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| dml|ins|LV_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| dml|wt|LV_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| agg|LV_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LV_STRUCT|df_str|inner|one | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LV_STRUCT|sql_on|inner|one | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LV_STRUCT|df_str|inner|big | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LV_STRUCT|sql_on|inner|big | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LLV_STRUCT|df_str|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_STRUCT|df_list|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_STRUCT|df_expr|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_STRUCT|df_expr_and|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LLV_STRUCT|sql_using|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_STRUCT|sql_on|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LLV_STRUCT|df_str|left|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LLV_STRUCT|df_str|right|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LLV_STRUCT|df_str|full|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LLV_STRUCT|df_str|semi|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LLV_STRUCT|df_str|anti|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LLV_STRUCT|sql_on|left|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LLV_STRUCT|sql_on|right|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LLV_STRUCT|sql_on|full|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LLV_STRUCT|sql_on|semi|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LLV_STRUCT|sql_on|anti|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LLV_STRUCT|df_two|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| dml|ins|LLV_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| dml|wt|LLV_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| agg|LLV_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LLV_STRUCT|df_str|inner|one | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LLV_STRUCT|sql_on|inner|one | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LLV_STRUCT|df_str|inner|big | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LLV_STRUCT|sql_on|inner|big | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|L_I64|df_str|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_I64|df_list|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_I64|df_expr|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_I64|df_expr_and|inner|base | array<bigint> | array<bigint> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|L_I64|sql_using|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_I64|sql_on|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_I64|df_str|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|L_I64|df_str|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|L_I64|df_str|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|L_I64|df_str|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|L_I64|df_str|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|L_I64|sql_on|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|L_I64|sql_on|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|L_I64|sql_on|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|L_I64|sql_on|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|L_I64|sql_on|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|L_I64|df_two|inner|base | array<bigint> | array<bigint> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| agg|L_I64 | array<bigint> | array<bigint> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|L_I64|df_str|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|L_I64|sql_on|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|L_I64|df_str|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|L_I64|sql_on|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LL_I64|df_str|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LL_I64|df_list|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LL_I64|df_expr|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LL_I64|df_expr_and|inner|base | array<bigint> | array<bigint> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|LL_I64|sql_using|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LL_I64|sql_on|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|LL_I64|df_str|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LL_I64|df_str|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LL_I64|df_str|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LL_I64|df_str|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LL_I64|df_str|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LL_I64|sql_on|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|LL_I64|sql_on|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|LL_I64|sql_on|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|LL_I64|sql_on|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|LL_I64|sql_on|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|LL_I64|df_two|inner|base | array<bigint> | array<bigint> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| agg|LL_I64 | array<bigint> | array<bigint> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|LL_I64|df_str|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LL_I64|sql_on|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|LL_I64|df_str|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|LL_I64|sql_on|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|FSL2_I64|df_str|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|FSL2_I64|df_list|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|FSL2_I64|df_expr|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|FSL2_I64|df_expr_and|inner|base | array<bigint> | array<bigint> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|FSL2_I64|sql_using|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|FSL2_I64|sql_on|inner|base | array<bigint> | array<bigint> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|FSL2_I64|df_str|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|FSL2_I64|df_str|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|FSL2_I64|df_str|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|FSL2_I64|df_str|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|FSL2_I64|df_str|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|FSL2_I64|sql_on|left|base | array<bigint> | array<bigint> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|FSL2_I64|sql_on|right|base | array<bigint> | array<bigint> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|FSL2_I64|sql_on|full|base | array<bigint> | array<bigint> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|FSL2_I64|sql_on|semi|base | array<bigint> | array<bigint> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|FSL2_I64|sql_on|anti|base | array<bigint> | array<bigint> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|FSL2_I64|df_two|inner|base | array<bigint> | array<bigint> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| agg|FSL2_I64 | array<bigint> | array<bigint> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|FSL2_I64|df_str|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|FSL2_I64|sql_on|inner|one | array<bigint> | array<bigint> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|FSL2_I64|df_str|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|FSL2_I64|sql_on|inner|big | array<bigint> | array<bigint> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|L_UTF8|df_str|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_UTF8|df_list|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_UTF8|df_expr|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_UTF8|df_expr_and|inner|base | array<string> | array<string> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|L_UTF8|sql_using|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_UTF8|sql_on|inner|base | array<string> | array<string> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_UTF8|df_str|left|base | array<string> | array<string> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|L_UTF8|df_str|right|base | array<string> | array<string> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|L_UTF8|df_str|full|base | array<string> | array<string> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|L_UTF8|df_str|semi|base | array<string> | array<string> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|L_UTF8|df_str|anti|base | array<string> | array<string> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|L_UTF8|sql_on|left|base | array<string> | array<string> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|L_UTF8|sql_on|right|base | array<string> | array<string> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|L_UTF8|sql_on|full|base | array<string> | array<string> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|L_UTF8|sql_on|semi|base | array<string> | array<string> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|L_UTF8|sql_on|anti|base | array<string> | array<string> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|L_UTF8|df_two|inner|base | array<string> | array<string> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| agg|L_UTF8 | array<string> | array<string> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|L_UTF8|df_str|inner|one | array<string> | array<string> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|L_UTF8|sql_on|inner|one | array<string> | array<string> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|L_UTF8|df_str|inner|big | array<string> | array<string> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|L_UTF8|sql_on|inner|big | array<string> | array<string> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|L_STRUCT|df_str|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_STRUCT|df_list|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_STRUCT|df_expr|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_STRUCT|df_expr_and|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=7 sha=2f8e5ac41e2d1a2c | ORACLE |
| join|L_STRUCT|sql_using|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_STRUCT|sql_on|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 | ORACLE |
| join|L_STRUCT|df_str|left|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|L_STRUCT|df_str|right|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|L_STRUCT|df_str|full|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|L_STRUCT|df_str|semi|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|L_STRUCT|df_str|anti|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|L_STRUCT|sql_on|left|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 | ORACLE |
| join|L_STRUCT|sql_on|right|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=11 sha=fc5d886ea14d6fef | ORACLE |
| join|L_STRUCT|sql_on|full|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=13 sha=e856210e1e957cdf | ORACLE |
| join|L_STRUCT|sql_on|semi|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 | ORACLE |
| join|L_STRUCT|sql_on|anti|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=2 sha=294e0503a4956969 | ORACLE |
| join|L_STRUCT|df_two|inner|base | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=5 sha=5034a392ff8c453f | ORACLE |
| agg|L_STRUCT | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=1 sha=f858915ec289e06e | ORACLE |
| join|L_STRUCT|df_str|inner|one | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|L_STRUCT|sql_on|inner|one | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|L_STRUCT|df_str|inner|big | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|L_STRUCT|sql_on|inner|big | array<struct<x:bigint,y:string>> | array<struct<x:bigint,y:string>> | ok n=122408 sha=bc0deca67e41ae70 | ORACLE |
| join|NEST_LV|df_str|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_LV|df_list|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_LV|df_expr|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_LV|df_expr_and|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=8 sha=804103abac24bc34 | ORACLE |
| join|NEST_LV|sql_using|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_LV|sql_on|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_LV|df_str|left|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=261f51236e4fbcf7 | ORACLE |
| join|NEST_LV|df_str|right|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=c35e708197e2843c | ORACLE |
| join|NEST_LV|df_str|full|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=12 sha=b58373b567f9aed3 | ORACLE |
| join|NEST_LV|df_str|semi|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=7 sha=ade26df2c1e3b7a2 | ORACLE |
| join|NEST_LV|df_str|anti|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=1 sha=589ffd3acf522ae8 | ORACLE |
| join|NEST_LV|sql_on|left|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=261f51236e4fbcf7 | ORACLE |
| join|NEST_LV|sql_on|right|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=c35e708197e2843c | ORACLE |
| join|NEST_LV|sql_on|full|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=12 sha=b58373b567f9aed3 | ORACLE |
| join|NEST_LV|sql_on|semi|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=7 sha=ade26df2c1e3b7a2 | ORACLE |
| join|NEST_LV|sql_on|anti|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=1 sha=589ffd3acf522ae8 | ORACLE |
| join|NEST_LV|df_two|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=6 sha=5a484148f9734c37 | ORACLE |
| dml|ins|NEST_LV | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| dml|wt|NEST_LV | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| agg|NEST_LV | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=1 sha=aeb7ed64fd7ef730 | ORACLE |
| join|NEST_LV|df_str|inner|one | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|NEST_LV|sql_on|inner|one | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|NEST_LV|df_str|inner|big | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=142857 sha=ef8c73243aae27fd | ORACLE |
| join|NEST_LV|sql_on|inner|big | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=142857 sha=ef8c73243aae27fd | ORACLE |
| join|NEST_L|df_str|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_L|df_list|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_L|df_expr|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_L|df_expr_and|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=8 sha=804103abac24bc34 | ORACLE |
| join|NEST_L|sql_using|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_L|sql_on|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc | ORACLE |
| join|NEST_L|df_str|left|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=261f51236e4fbcf7 | ORACLE |
| join|NEST_L|df_str|right|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=c35e708197e2843c | ORACLE |
| join|NEST_L|df_str|full|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=12 sha=b58373b567f9aed3 | ORACLE |
| join|NEST_L|df_str|semi|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=7 sha=ade26df2c1e3b7a2 | ORACLE |
| join|NEST_L|df_str|anti|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=1 sha=589ffd3acf522ae8 | ORACLE |
| join|NEST_L|sql_on|left|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=261f51236e4fbcf7 | ORACLE |
| join|NEST_L|sql_on|right|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=11 sha=c35e708197e2843c | ORACLE |
| join|NEST_L|sql_on|full|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=12 sha=b58373b567f9aed3 | ORACLE |
| join|NEST_L|sql_on|semi|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=7 sha=ade26df2c1e3b7a2 | ORACLE |
| join|NEST_L|sql_on|anti|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=1 sha=589ffd3acf522ae8 | ORACLE |
| join|NEST_L|df_two|inner|base | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=6 sha=5a484148f9734c37 | ORACLE |
| agg|NEST_L | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=1 sha=aeb7ed64fd7ef730 | ORACLE |
| join|NEST_L|df_str|inner|one | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|NEST_L|sql_on|inner|one | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=0 sha=e3b0c44298fc1c14 | ORACLE |
| join|NEST_L|df_str|inner|big | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=142857 sha=ef8c73243aae27fd | ORACLE |
| join|NEST_L|sql_on|inner|big | struct<a:array<bigint>> | struct<a:array<bigint>> | ok n=142857 sha=ef8c73243aae27fd | ORACLE |
