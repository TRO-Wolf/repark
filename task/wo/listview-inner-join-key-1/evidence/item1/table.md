# LISTVIEW-INNER-JOIN-KEY-1 item-1 table

Spark meta: `{"leg": "spark", "label": "spark", "python": "3.12.3", "pyarrow": "25.0.1", "experiment": "", "pyspark": "4.1.2", "spark_version": "4.1.2"}`.

# Leg `main`

Meta: `{"leg": "main", "label": "main", "python": "3.12.3", "pyarrow": "25.0.1", "experiment": "", "debug_assertions": false}`.

## Counts for `main`: label by section

| section | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | CROSS-PRODUCT | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | STRUCK-NO-ROUTE | WRONG-ANSWER | WRONG-WRITE | total |
|---|---|---|---|---|---|---|---|---|---|
| failing | 0 | 0 | 14 | 48 | 16 | 0 | 14 | 7 | 99 |
| lateral | 17 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 17 |
| mixed | 0 | 0 | 0 | 0 | 36 | 0 | 0 | 0 | 36 |
| native | 0 | 0 | 0 | 19 | 7 | 0 | 0 | 0 | 26 |
| route | 0 | 0 | 3 | 1 | 0 | 4 | 0 | 0 | 8 |
| using | 0 | 0 | 0 | 5 | 5 | 0 | 0 | 0 | 10 |
| variant | 0 | 5 | 14 | 14 | 0 | 15 | 0 | 7 | 55 |

## Cells for `main`

| cell | reached | main outcome | spark outcome | label |
|---|---|---|---|---|
| failing|LV_I64|df_str|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LV_I64|df_list|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LV_I64|df_two|inner | list_view<item: int64> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER |
| failing|LV_I64|agg|inner | list_view<item: int64> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER |
| failing|LV_I64|wt|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| failing|LV_I64|df_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_I64|df_str|left_semi | list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_I64|sql_on|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|sql_using|inner | list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|cross_where|inner | list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|sql_comma|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LLV_I64|df_list|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LLV_I64|df_two|inner | large_list_view<item: int64> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER |
| failing|LLV_I64|agg|inner | large_list_view<item: int64> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER |
| failing|LLV_I64|wt|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| failing|LLV_I64|df_str|left | large_list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_I64|df_str|left_semi | large_list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_I64|sql_on|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|sql_using|inner | large_list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|cross_where|inner | large_list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|sql_comma|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|inner | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LV_UTF8|df_list|inner | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LV_UTF8|df_two|inner | list_view<item: string> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER |
| failing|LV_UTF8|agg|inner | list_view<item: string> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER |
| failing|LV_UTF8|wt|inner | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| failing|LV_UTF8|df_str|left | list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_UTF8|df_str|left_semi | list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_UTF8|sql_on|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|sql_using|inner | list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|cross_where|inner | list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|sql_comma|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|inner | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LLV_UTF8|df_list|inner | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LLV_UTF8|df_two|inner | large_list_view<item: string> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER |
| failing|LLV_UTF8|agg|inner | large_list_view<item: string> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER |
| failing|LLV_UTF8|wt|inner | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| failing|LLV_UTF8|df_str|left | large_list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_UTF8|df_str|left_semi | large_list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_UTF8|sql_on|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|sql_using|inner | large_list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|cross_where|inner | large_list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|sql_comma|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LV_STRUCT|df_list|inner | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LV_STRUCT|df_two|inner | list_view<item: struct<x: int64, y: string>> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER |
| failing|LV_STRUCT|agg|inner | list_view<item: struct<x: int64, y: string>> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER |
| failing|LV_STRUCT|wt|inner | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| failing|LV_STRUCT|df_str|left | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_STRUCT|df_str|left_semi | list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_STRUCT|sql_on|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|sql_using|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|cross_where|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|sql_comma|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LLV_STRUCT|df_list|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| failing|LLV_STRUCT|df_two|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=5 sha=5034a392ff8c453f | WRONG-ANSWER |
| failing|LLV_STRUCT|agg|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=f858915ec289e06e | WRONG-ANSWER |
| failing|LLV_STRUCT|wt|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| failing|LLV_STRUCT|df_str|left | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_STRUCT|df_str|left_semi | large_list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_STRUCT|sql_on|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|sql_using|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|cross_where|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|sql_comma|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_LV|df_str|inner | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | CROSS-PRODUCT |
| failing|NEST_LV|df_list|inner | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | CROSS-PRODUCT |
| failing|NEST_LV|df_two|inner | struct<a: list_view<item: int64>> | ok n=24 sha=9e9914f4ab9e6a0e | ok n=6 sha=5a484148f9734c37 | WRONG-ANSWER |
| failing|NEST_LV|agg|inner | struct<a: list_view<item: int64>> | ok n=1 sha=df7d72e06bb04ce1 | ok n=1 sha=aeb7ed64fd7ef730 | WRONG-ANSWER |
| failing|NEST_LV|wt|inner | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | WRONG-WRITE |
| failing|NEST_LV|df_str|left | struct<a: list_view<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_LV|df_str|left_semi | struct<a: list_view<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_LV|sql_on|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|sql_using|inner | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|cross_where|inner | struct<a: list_view<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|sql_comma|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|L_I64|df_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_two|inner | list<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|L_I64|agg|inner | list<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|L_I64|wt|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|L_I64|df_str|left_semi | list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|L_I64|sql_on|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|sql_using|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|cross_where|inner | list<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|L_I64|sql_comma|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_L|df_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_list|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_two|inner | struct<a: list<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_L|agg|inner | struct<a: list<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_L|wt|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_str|left | struct<a: list<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_L|df_str|left_semi | struct<a: list<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_L|sql_on|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|sql_using|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|cross_where|inner | struct<a: list<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_L|sql_comma|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| lateral|LV_I64|lateral_str|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_I64|lateral_str|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_UTF8|lateral_str|inner | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_UTF8|lateral_str|inner | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_STRUCT|lateral_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_STRUCT|lateral_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LL_I64|lateral_str|inner | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|FSL2_I64|lateral_str|inner | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_UTF8|lateral_str|inner | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_STRUCT|lateral_str|inner | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_LV|lateral_str|inner | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_L|lateral_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_list|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| mixed|plainxview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LV_I64|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LV_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LLV_I64|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LLV_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LV_UTF8|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LV_UTF8|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LLV_UTF8|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LLV_UTF8|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LV_STRUCT|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LV_STRUCT|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LLV_STRUCT|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| native|LLV_STRUCT|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|L_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|L_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LL_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|LL_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|FSL2_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|FSL2_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|L_UTF8|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|L_UTF8|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|L_STRUCT|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|L_STRUCT|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| native|NEST_LV|native_using|inner |  | AnalysisException: Schema error: No field named lv_r.k. Valid fields are lv_l.lid, lv_l.k, lv_l.id, lv_l.j. | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| native|NEST_LV|native_on|inner |  | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| native|NEST_L|native_using|inner |  | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| native|NEST_L|native_on|inner |  | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| route|pandas:LV_I64|df_str|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| route|pandas:LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| route|pandas:REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| route|pandas:UNION_I64|df_str|inner | int64 | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:DICT_LV|df_str|inner |  | BuildError: ArrowNotImplementedError: Function 'dictionary_encode' has no kernel matching input types (list_view<item: int | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vl" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LLV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vL" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|arrow:LV_I64|df_str|inner |  | BuildError: PySparkTypeError: createDataFrame expects a list of rows, a pandas DataFrame, or a polars DataFrame, got Table | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| using|LV_I64|using_pair|inner |  | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| using|LV_I64|using_star|inner |  | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=3cd2e6cb9e84ac53 | REFUSED-WHERE-SPARK-ANSWERED |
| using|LV_I64|using_alias|inner |  | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| using|LV_I64|using_left|inner |  | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=11 sha=6f307aadd294b507 | REFUSED-WHERE-SPARK-ANSWERED |
| using|LV_I64|natural|inner |  | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| using|L_I64|using_pair|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| using|L_I64|using_star|inner |  | ok n=9 sha=3cd2e6cb9e84ac53 | ok n=9 sha=3cd2e6cb9e84ac53 | EQUAL |
| using|L_I64|using_alias|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| using|L_I64|using_left|inner |  | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| using|L_I64|natural|inner |  | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|REE_I64|df_list|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|REE_I64|wt|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| variant|REE_I64|sql_on|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_str|left | run_end_encoded<run_ends: int32, values: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|REE_UTF8|df_str|inner | run_end_encoded<run_ends: int32, values: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|REE_UTF8|df_list|inner | run_end_encoded<run_ends: int32, values: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|REE_UTF8|wt|inner | run_end_encoded<run_ends: int32, values: string> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| variant|REE_UTF8|sql_on|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_str|left | run_end_encoded<run_ends: int32, values: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|DICT_LV|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_list|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|wt|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|sql_on|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|left | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|L_LV|df_str|inner | list<item: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|L_LV|df_list|inner | list<item: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|L_LV|wt|inner | list<item: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| variant|L_LV|sql_on|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_str|left | list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|LL_LV|df_str|inner | large_list<item: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|LL_LV|df_list|inner | large_list<item: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|LL_LV|wt|inner | large_list<item: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| variant|LL_LV|sql_on|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_str|left | large_list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|FSL_LV|df_str|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|FSL_LV|df_list|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|FSL_LV|wt|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| variant|FSL_LV|sql_on|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_str|left | fixed_size_list<item: list_view<item: int64>>[2] | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|MAP_LV|df_str|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_list|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|wt|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|sql_on|inner | map<string, list_view<item: int64>> | AnalysisException: Error during planning: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mis | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_str|left | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|STRUCT_REE|df_str|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | CROSS-PRODUCT |
| variant|STRUCT_REE|df_list|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | CROSS-PRODUCT |
| variant|STRUCT_REE|wt|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=10 sha=d09cfe68f10f46cc | WRONG-WRITE |
| variant|STRUCT_REE|sql_on|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_str|left | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| variant|L_REE|df_str|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|L_REE|df_list|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | CROSS-PRODUCT |
| variant|L_REE|wt|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | ok n=9 sha=2de42b07ea3770a9 | WRONG-WRITE |
| variant|L_REE|sql_on|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_str|left | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |

# Leg `exp-norule`

Meta: `{"leg": "main", "label": "exp-norule", "python": "3.12.3", "pyarrow": "25.0.1", "experiment": "norule", "debug_assertions": false}`.

## Counts for `exp-norule`: label by section

| section | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | STRUCK-NO-ROUTE | total |
|---|---|---|---|---|---|---|
| failing | 0 | 0 | 83 | 16 | 0 | 99 |
| lateral | 17 | 0 | 0 | 0 | 0 | 17 |
| mixed | 0 | 0 | 0 | 36 | 0 | 36 |
| route | 0 | 0 | 4 | 0 | 4 | 8 |
| variant | 0 | 5 | 35 | 0 | 15 | 55 |

## Cells for `exp-norule`

| cell | reached | main outcome | spark outcome | label |
|---|---|---|---|---|
| failing|LV_I64|df_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_list|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_two|inner | list_view<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_I64|agg|inner | list_view<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_I64|wt|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_I64|df_str|left_semi | list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_I64|sql_on|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|sql_using|inner | list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|cross_where|inner | list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|sql_comma|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_list|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_two|inner | large_list_view<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_I64|agg|inner | large_list_view<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_I64|wt|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|left | large_list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_I64|df_str|left_semi | large_list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_I64|sql_on|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|sql_using|inner | large_list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|cross_where|inner | large_list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|sql_comma|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_list|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_two|inner | list_view<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_UTF8|agg|inner | list_view<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_UTF8|wt|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|left | list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_UTF8|df_str|left_semi | list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_UTF8|sql_on|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|sql_using|inner | list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|cross_where|inner | list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|sql_comma|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_list|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_two|inner | large_list_view<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_UTF8|agg|inner | large_list_view<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_UTF8|wt|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|left | large_list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_UTF8|df_str|left_semi | large_list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_UTF8|sql_on|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|sql_using|inner | large_list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|cross_where|inner | large_list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|sql_comma|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_list|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_two|inner | list_view<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_STRUCT|agg|inner | list_view<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_STRUCT|wt|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|left | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_STRUCT|df_str|left_semi | list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_STRUCT|sql_on|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|sql_using|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|cross_where|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|sql_comma|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_list|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_two|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_STRUCT|agg|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_STRUCT|wt|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|left | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_STRUCT|df_str|left_semi | large_list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_STRUCT|sql_on|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|sql_using|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|cross_where|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|sql_comma|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_LV|df_str|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_list|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_two|inner | struct<a: list_view<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_LV|agg|inner | struct<a: list_view<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_LV|wt|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_str|left | struct<a: list_view<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_LV|df_str|left_semi | struct<a: list_view<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_LV|sql_on|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|sql_using|inner | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|cross_where|inner | struct<a: list_view<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|sql_comma|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|L_I64|df_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_two|inner | list<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|L_I64|agg|inner | list<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|L_I64|wt|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|L_I64|df_str|left_semi | list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|L_I64|sql_on|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|sql_using|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|cross_where|inner | list<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|L_I64|sql_comma|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_L|df_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_list|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_two|inner | struct<a: list<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_L|agg|inner | struct<a: list<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_L|wt|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_str|left | struct<a: list<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_L|df_str|left_semi | struct<a: list<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_L|sql_on|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|sql_using|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|cross_where|inner | struct<a: list<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_L|sql_comma|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| lateral|LV_I64|lateral_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_I64|lateral_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_UTF8|lateral_str|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_UTF8|lateral_str|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_STRUCT|lateral_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_STRUCT|lateral_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LL_I64|lateral_str|inner | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|FSL2_I64|lateral_str|inner | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_UTF8|lateral_str|inner | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_STRUCT|lateral_str|inner | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_LV|lateral_str|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_L|lateral_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_list|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| mixed|plainxview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| route|pandas:LV_I64|df_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:UNION_I64|df_str|inner | int64 | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:DICT_LV|df_str|inner |  | BuildError: ArrowNotImplementedError: Function 'dictionary_encode' has no kernel matching input types (list_view<item: int | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vl" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LLV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vL" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|arrow:LV_I64|df_str|inner |  | BuildError: PySparkTypeError: createDataFrame expects a list of rows, a pandas DataFrame, or a polars DataFrame, got Table | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_list|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|wt|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|sql_on|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_str|left | run_end_encoded<run_ends: int32, values: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|REE_UTF8|df_str|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_list|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|wt|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|sql_on|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_str|left | run_end_encoded<run_ends: int32, values: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|DICT_LV|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_list|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|wt|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|sql_on|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|left | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|L_LV|df_str|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_list|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|wt|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|sql_on|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_str|left | list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|LL_LV|df_str|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_list|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|wt|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|sql_on|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_str|left | large_list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|FSL_LV|df_str|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_list|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|wt|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|sql_on|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_str|left | fixed_size_list<item: list_view<item: int64>>[2] | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|MAP_LV|df_str|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_list|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|wt|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|sql_on|inner | map<string, list_view<item: int64>> | AnalysisException: Error during planning: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mis | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_str|left | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|STRUCT_REE|df_str|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_list|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|wt|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|sql_on|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_str|left | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| variant|L_REE|df_str|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_list|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|wt|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|sql_on|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_str|left | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |

# Leg `exp-filter`

Meta: `{"leg": "main", "label": "exp-filter", "python": "3.12.3", "pyarrow": "25.0.1", "experiment": "filter", "debug_assertions": false}`.

## Counts for `exp-filter`: label by section

| section | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | STRUCK-NO-ROUTE | total |
|---|---|---|---|---|---|---|
| failing | 0 | 0 | 83 | 16 | 0 | 99 |
| lateral | 17 | 0 | 0 | 0 | 0 | 17 |
| mixed | 0 | 0 | 0 | 36 | 0 | 36 |
| route | 0 | 0 | 4 | 0 | 4 | 8 |
| variant | 0 | 5 | 35 | 0 | 15 | 55 |

## Cells for `exp-filter`

| cell | reached | main outcome | spark outcome | label |
|---|---|---|---|---|
| failing|LV_I64|df_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_list|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_two|inner | list_view<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_I64|agg|inner | list_view<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_I64|wt|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_I64|df_str|left_semi | list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_I64|sql_on|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|sql_using|inner | list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|cross_where|inner | list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|sql_comma|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_list|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_two|inner | large_list_view<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_I64|agg|inner | large_list_view<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_I64|wt|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|left | large_list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_I64|df_str|left_semi | large_list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_I64|sql_on|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|sql_using|inner | large_list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|cross_where|inner | large_list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|sql_comma|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_list|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_two|inner | list_view<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_UTF8|agg|inner | list_view<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_UTF8|wt|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|left | list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_UTF8|df_str|left_semi | list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_UTF8|sql_on|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|sql_using|inner | list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|cross_where|inner | list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|sql_comma|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_list|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_two|inner | large_list_view<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_UTF8|agg|inner | large_list_view<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_UTF8|wt|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|left | large_list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_UTF8|df_str|left_semi | large_list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_UTF8|sql_on|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|sql_using|inner | large_list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|cross_where|inner | large_list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|sql_comma|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_list|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_two|inner | list_view<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_STRUCT|agg|inner | list_view<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_STRUCT|wt|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|left | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_STRUCT|df_str|left_semi | list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_STRUCT|sql_on|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|sql_using|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|cross_where|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|sql_comma|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_list|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_two|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_STRUCT|agg|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_STRUCT|wt|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|left | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_STRUCT|df_str|left_semi | large_list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_STRUCT|sql_on|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|sql_using|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|cross_where|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|sql_comma|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_LV|df_str|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_list|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_two|inner | struct<a: list_view<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_LV|agg|inner | struct<a: list_view<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_LV|wt|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_str|left | struct<a: list_view<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_LV|df_str|left_semi | struct<a: list_view<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_LV|sql_on|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|sql_using|inner | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|cross_where|inner | struct<a: list_view<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|sql_comma|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|L_I64|df_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_two|inner | list<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|L_I64|agg|inner | list<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|L_I64|wt|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|L_I64|df_str|left_semi | list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|L_I64|sql_on|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|sql_using|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|cross_where|inner | list<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|L_I64|sql_comma|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_L|df_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_list|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_two|inner | struct<a: list<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_L|agg|inner | struct<a: list<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_L|wt|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_str|left | struct<a: list<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_L|df_str|left_semi | struct<a: list<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_L|sql_on|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|sql_using|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|cross_where|inner | struct<a: list<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_L|sql_comma|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| lateral|LV_I64|lateral_str|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_I64|lateral_str|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_UTF8|lateral_str|inner | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_UTF8|lateral_str|inner | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_STRUCT|lateral_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_STRUCT|lateral_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LL_I64|lateral_str|inner | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|FSL2_I64|lateral_str|inner | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_UTF8|lateral_str|inner | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_STRUCT|lateral_str|inner | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_LV|lateral_str|inner | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_L|lateral_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_list|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| mixed|plainxview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=1429fe05cab82e21 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=9f97b9b4f06b1b37 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|left |  | AnalysisException: type_coercion | ok n=2 sha=8a0a3c0ddb73e540 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| route|pandas:LV_I64|df_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:UNION_I64|df_str|inner | int64 | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:DICT_LV|df_str|inner |  | BuildError: ArrowNotImplementedError: Function 'dictionary_encode' has no kernel matching input types (list_view<item: int | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vl" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LLV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vL" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|arrow:LV_I64|df_str|inner |  | BuildError: PySparkTypeError: createDataFrame expects a list of rows, a pandas DataFrame, or a polars DataFrame, got Table | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_list|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|wt|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|sql_on|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_str|left | run_end_encoded<run_ends: int32, values: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|REE_UTF8|df_str|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_list|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|wt|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|sql_on|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_str|left | run_end_encoded<run_ends: int32, values: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|DICT_LV|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_list|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|wt|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|sql_on|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|left | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|L_LV|df_str|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_list|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|wt|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|sql_on|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_str|left | list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|LL_LV|df_str|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_list|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|wt|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|sql_on|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_str|left | large_list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|FSL_LV|df_str|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_list|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|wt|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|sql_on|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_str|left | fixed_size_list<item: list_view<item: int64>>[2] | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|MAP_LV|df_str|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_list|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|wt|inner | map<string, list_view<item: int64>> | AnalysisException: Error during planning: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mis | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|sql_on|inner | map<string, list_view<item: int64>> | AnalysisException: Error during planning: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mis | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_str|left | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|STRUCT_REE|df_str|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_list|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|wt|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|sql_on|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_str|left | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| variant|L_REE|df_str|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_list|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|wt|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|sql_on|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_str|left | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |

# Leg `exp-cast`

Meta: `{"leg": "main", "label": "exp-cast", "python": "3.12.3", "pyarrow": "25.0.1", "experiment": "cast", "debug_assertions": false}`.

## Counts for `exp-cast`: label by section

| section | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | STRUCK-NO-ROUTE | total |
|---|---|---|---|---|---|---|
| failing | 0 | 0 | 83 | 16 | 0 | 99 |
| lateral | 17 | 0 | 0 | 0 | 0 | 17 |
| mixed | 0 | 0 | 24 | 12 | 0 | 36 |
| route | 0 | 0 | 4 | 0 | 4 | 8 |
| variant | 0 | 5 | 35 | 0 | 15 | 55 |

## Cells for `exp-cast`

| cell | reached | main outcome | spark outcome | label |
|---|---|---|---|---|
| failing|LV_I64|df_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_list|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_two|inner | list_view<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_I64|agg|inner | list_view<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_I64|wt|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|df_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_I64|df_str|left_semi | list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_I64|sql_on|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_I64|sql_using|inner | list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|cross_where|inner | list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_I64|sql_comma|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_list|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_two|inner | large_list_view<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_I64|agg|inner | large_list_view<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_I64|wt|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|df_str|left | large_list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_I64|df_str|left_semi | large_list_view<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_I64|sql_on|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_I64|sql_using|inner | large_list_view<item: int64> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|cross_where|inner | large_list_view<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_I64|sql_comma|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_list|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_two|inner | list_view<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_UTF8|agg|inner | list_view<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_UTF8|wt|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|df_str|left | list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_UTF8|df_str|left_semi | list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_UTF8|sql_on|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_UTF8|sql_using|inner | list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|cross_where|inner | list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_UTF8|sql_comma|inner | list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_list|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_two|inner | large_list_view<item: string> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_UTF8|agg|inner | large_list_view<item: string> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_UTF8|wt|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|df_str|left | large_list_view<item: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_UTF8|df_str|left_semi | large_list_view<item: string> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_UTF8|sql_on|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_UTF8|sql_using|inner | large_list_view<item: string> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|cross_where|inner | large_list_view<item: string> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_UTF8|sql_comma|inner | large_list_view<item: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_list|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_two|inner | list_view<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LV_STRUCT|agg|inner | list_view<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LV_STRUCT|wt|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|df_str|left | list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LV_STRUCT|df_str|left_semi | list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LV_STRUCT|sql_on|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LV_STRUCT|sql_using|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|cross_where|inner | list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LV_STRUCT|sql_comma|inner | list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_list|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_two|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|LLV_STRUCT|agg|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|LLV_STRUCT|wt|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|df_str|left | large_list_view<item: struct<x: int64, y: string>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|LLV_STRUCT|df_str|left_semi | large_list_view<item: struct<x: int64, y: string>> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|LLV_STRUCT|sql_on|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|LLV_STRUCT|sql_using|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|cross_where|inner | large_list_view<item: struct<x: int64, y: string>> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|LLV_STRUCT|sql_comma|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_LV|df_str|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_list|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_two|inner | struct<a: list_view<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_LV|agg|inner | struct<a: list_view<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_LV|wt|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|df_str|left | struct<a: list_view<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_LV|df_str|left_semi | struct<a: list_view<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_LV|sql_on|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_LV|sql_using|inner | struct<a: list_view<item: int64>> | AnalysisException: Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|cross_where|inner | struct<a: list_view<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_LV|sql_comma|inner | struct<a: list_view<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|L_I64|df_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_two|inner | list<item: int64> | ok n=5 sha=5034a392ff8c453f | ok n=5 sha=5034a392ff8c453f | EQUAL |
| failing|L_I64|agg|inner | list<item: int64> | ok n=1 sha=f858915ec289e06e | ok n=1 sha=f858915ec289e06e | EQUAL |
| failing|L_I64|wt|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|df_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| failing|L_I64|df_str|left_semi | list<item: int64> | ok n=6 sha=96353b9f92e2bca2 | ok n=6 sha=96353b9f92e2bca2 | EQUAL |
| failing|L_I64|sql_on|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|sql_using|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|L_I64|cross_where|inner | list<item: int64> | AnalysisException: type_coercion | ok n=9 sha=2de42b07ea3770a9 | REFUSED-WHERE-SPARK-ANSWERED |
| failing|L_I64|sql_comma|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| failing|NEST_L|df_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_list|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_two|inner | struct<a: list<item: int64>> | ok n=6 sha=5a484148f9734c37 | ok n=6 sha=5a484148f9734c37 | EQUAL |
| failing|NEST_L|agg|inner | struct<a: list<item: int64>> | ok n=1 sha=aeb7ed64fd7ef730 | ok n=1 sha=aeb7ed64fd7ef730 | EQUAL |
| failing|NEST_L|wt|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|df_str|left | struct<a: list<item: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| failing|NEST_L|df_str|left_semi | struct<a: list<item: int64>> | ok n=7 sha=ade26df2c1e3b7a2 | ok n=7 sha=ade26df2c1e3b7a2 | EQUAL |
| failing|NEST_L|sql_on|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|sql_using|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| failing|NEST_L|cross_where|inner | struct<a: list<item: int64>> | AnalysisException: type_coercion | ok n=10 sha=d09cfe68f10f46cc | REFUSED-WHERE-SPARK-ANSWERED |
| failing|NEST_L|sql_comma|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| lateral|LV_I64|lateral_str|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_I64|lateral_str|inner | large_list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_UTF8|lateral_str|inner | list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_UTF8|lateral_str|inner | large_list_view<item: string> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_STRUCT|lateral_str|inner | list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LLV_STRUCT|lateral_str|inner | large_list_view<item: struct<x: int64, y: string>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LL_I64|lateral_str|inner | large_list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|FSL2_I64|lateral_str|inner | fixed_size_list<item: int64>[2] | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_UTF8|lateral_str|inner | list<item: string> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_STRUCT|lateral_str|inner | list<item: struct<x: int64, y: string>> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_LV|lateral_str|inner | struct<a: list_view<item: int64>> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|NEST_L|lateral_str|inner | struct<a: list<item: int64>> | ok n=10 sha=d09cfe68f10f46cc | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_list|inner | list_view<item: int64> | ok n=56 sha=3c4f4b9b2315b7aa | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|LV_I64|lateral_str|left | list_view<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_list|inner | list<item: int64> | ok n=9 sha=2de42b07ea3770a9 | AttributeError: 'list' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| lateral|L_I64|lateral_str|left | list<item: int64> | ok n=11 sha=6f307aadd294b507 | AttributeError: 'str' object has no attribute '_jc' | ACCEPTED-WHERE-SPARK-REFUSED |
| mixed|plainxview_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd | ok n=1 sha=e2efbdac0b9ad4bd | EQUAL |
| mixed|plainxview_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 | ok n=2 sha=1429fe05cab82e21 | EQUAL |
| mixed|plainxview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|plainxview_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 | ok n=1 sha=cb1fc0e151046ec2 | EQUAL |
| mixed|plainxview_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 | ok n=2 sha=9f97b9b4f06b1b37 | EQUAL |
| mixed|plainxview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd | ok n=1 sha=e2efbdac0b9ad4bd | EQUAL |
| mixed|largexview_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 | ok n=2 sha=1429fe05cab82e21 | EQUAL |
| mixed|largexview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|largexview_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 | ok n=1 sha=cb1fc0e151046ec2 | EQUAL |
| mixed|largexview_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 | ok n=2 sha=9f97b9b4f06b1b37 | EQUAL |
| mixed|largexview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_rev|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL |
| mixed|fsl2xview_rev|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 | ok n=2 sha=8a0a3c0ddb73e540 | EQUAL |
| mixed|fsl2xview_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|fsl2xview_same|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL |
| mixed|fsl2xview_same|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 | ok n=2 sha=8a0a3c0ddb73e540 | EQUAL |
| mixed|fsl2xview_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd | ok n=1 sha=e2efbdac0b9ad4bd | EQUAL |
| mixed|viewxplain_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 | ok n=2 sha=1429fe05cab82e21 | EQUAL |
| mixed|viewxplain_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxplain_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 | ok n=1 sha=cb1fc0e151046ec2 | EQUAL |
| mixed|viewxplain_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 | ok n=2 sha=9f97b9b4f06b1b37 | EQUAL |
| mixed|viewxplain_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd | ok n=1 sha=e2efbdac0b9ad4bd | EQUAL |
| mixed|viewxlarge_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 | ok n=2 sha=1429fe05cab82e21 | EQUAL |
| mixed|viewxlarge_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=e2efbdac0b9ad4bd | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxlarge_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 | ok n=1 sha=cb1fc0e151046ec2 | EQUAL |
| mixed|viewxlarge_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 | ok n=2 sha=9f97b9b4f06b1b37 | EQUAL |
| mixed|viewxlarge_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=1 sha=cb1fc0e151046ec2 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_rev|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL |
| mixed|viewxfsl2_rev|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 | ok n=2 sha=8a0a3c0ddb73e540 | EQUAL |
| mixed|viewxfsl2_rev|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| mixed|viewxfsl2_same|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 | ok n=0 sha=e3b0c44298fc1c14 | EQUAL |
| mixed|viewxfsl2_same|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 | ok n=2 sha=8a0a3c0ddb73e540 | EQUAL |
| mixed|viewxfsl2_same|df_expr|inner |  | AnalysisException: type_coercion | ok n=0 sha=e3b0c44298fc1c14 | REFUSED-WHERE-SPARK-ANSWERED |
| route|pandas:LV_I64|df_str|inner | list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:LLV_I64|df_str|inner | large_list_view<item: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:UNION_I64|df_str|inner | int64 | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| route|pandas:DICT_LV|df_str|inner |  | BuildError: ArrowNotImplementedError: Function 'dictionary_encode' has no kernel matching input types (list_view<item: int | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vl" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|polars:LLV_I64|df_str|inner |  | BuildError: ComputeError: The datatype "+vL" is still not supported in Rust implementation | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| route|arrow:LV_I64|df_str|inner |  | BuildError: PySparkTypeError: createDataFrame expects a list of rows, a pandas DataFrame, or a polars DataFrame, got Table | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|REE_I64|df_str|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_list|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|wt|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|sql_on|inner | run_end_encoded<run_ends: int32, values: int64> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_I64|df_str|left | run_end_encoded<run_ends: int32, values: int64> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|REE_UTF8|df_str|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_list|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|wt|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|sql_on|inner | run_end_encoded<run_ends: int32, values: string> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|REE_UTF8|df_str|left | run_end_encoded<run_ends: int32, values: string> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|DICT_LV|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_LV|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_list|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|wt|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|sql_on|inner | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|DICT_REE|df_str|left | ERR PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_list|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|wt|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|sql_on|inner | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=9 sha=2de42b07ea3770a9 | STRUCK-NO-ROUTE |
| variant|UNION_I64|df_str|left | ERR UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | UnsupportedOperationException: Optimizer rule 'simplify_expressions' failed | ok n=11 sha=6f307aadd294b507 | STRUCK-NO-ROUTE |
| variant|L_LV|df_str|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_list|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|wt|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|sql_on|inner | list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_LV|df_str|left | list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|LL_LV|df_str|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_list|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|wt|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|sql_on|inner | large_list<item: list_view<item: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|LL_LV|df_str|left | large_list<item: list_view<item: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|FSL_LV|df_str|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_list|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|wt|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|sql_on|inner | fixed_size_list<item: list_view<item: int64>>[2] | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|FSL_LV|df_str|left | fixed_size_list<item: list_view<item: int64>>[2] | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |
| variant|MAP_LV|df_str|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_list|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|wt|inner | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|sql_on|inner | map<string, list_view<item: int64>> | AnalysisException: Error during planning: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mis | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|MAP_LV|df_str|left | map<string, list_view<item: int64>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not | BOTH-REFUSED |
| variant|STRUCT_REE|df_str|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_list|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|wt|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|sql_on|inner | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=10 sha=d09cfe68f10f46cc | ok n=10 sha=d09cfe68f10f46cc | EQUAL |
| variant|STRUCT_REE|df_str|left | struct<a: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=261f51236e4fbcf7 | ok n=11 sha=261f51236e4fbcf7 | EQUAL |
| variant|L_REE|df_str|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_list|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|wt|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|sql_on|inner | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=9 sha=2de42b07ea3770a9 | ok n=9 sha=2de42b07ea3770a9 | EQUAL |
| variant|L_REE|df_str|left | list<item: run_end_encoded<run_ends: int32, values: int64>> | ok n=11 sha=6f307aadd294b507 | ok n=11 sha=6f307aadd294b507 | EQUAL |

# Spark oracle cells

| cell | reached | spark outcome |
|---|---|---|
| failing|LV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_I64|df_list|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_I64|df_two|inner | array<bigint> | ok n=5 sha=5034a392ff8c453f |
| failing|LV_I64|agg|inner | array<bigint> | ok n=1 sha=f858915ec289e06e |
| failing|LV_I64|wt|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_I64|df_str|left | array<bigint> | ok n=11 sha=6f307aadd294b507 |
| failing|LV_I64|df_str|left_semi | array<bigint> | ok n=6 sha=96353b9f92e2bca2 |
| failing|LV_I64|sql_on|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_I64|sql_using|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_I64|cross_where|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_I64|sql_comma|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|df_list|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|df_two|inner | array<bigint> | ok n=5 sha=5034a392ff8c453f |
| failing|LLV_I64|agg|inner | array<bigint> | ok n=1 sha=f858915ec289e06e |
| failing|LLV_I64|wt|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|df_str|left | array<bigint> | ok n=11 sha=6f307aadd294b507 |
| failing|LLV_I64|df_str|left_semi | array<bigint> | ok n=6 sha=96353b9f92e2bca2 |
| failing|LLV_I64|sql_on|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|sql_using|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|cross_where|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_I64|sql_comma|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|df_str|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|df_list|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|df_two|inner | array<string> | ok n=5 sha=5034a392ff8c453f |
| failing|LV_UTF8|agg|inner | array<string> | ok n=1 sha=f858915ec289e06e |
| failing|LV_UTF8|wt|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|df_str|left | array<string> | ok n=11 sha=6f307aadd294b507 |
| failing|LV_UTF8|df_str|left_semi | array<string> | ok n=6 sha=96353b9f92e2bca2 |
| failing|LV_UTF8|sql_on|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|sql_using|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|cross_where|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_UTF8|sql_comma|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|df_str|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|df_list|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|df_two|inner | array<string> | ok n=5 sha=5034a392ff8c453f |
| failing|LLV_UTF8|agg|inner | array<string> | ok n=1 sha=f858915ec289e06e |
| failing|LLV_UTF8|wt|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|df_str|left | array<string> | ok n=11 sha=6f307aadd294b507 |
| failing|LLV_UTF8|df_str|left_semi | array<string> | ok n=6 sha=96353b9f92e2bca2 |
| failing|LLV_UTF8|sql_on|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|sql_using|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|cross_where|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_UTF8|sql_comma|inner | array<string> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|df_str|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|df_list|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|df_two|inner | array<struct<x:bigint,y:string>> | ok n=5 sha=5034a392ff8c453f |
| failing|LV_STRUCT|agg|inner | array<struct<x:bigint,y:string>> | ok n=1 sha=f858915ec289e06e |
| failing|LV_STRUCT|wt|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|df_str|left | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 |
| failing|LV_STRUCT|df_str|left_semi | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 |
| failing|LV_STRUCT|sql_on|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|sql_using|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|cross_where|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LV_STRUCT|sql_comma|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|df_str|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|df_list|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|df_two|inner | array<struct<x:bigint,y:string>> | ok n=5 sha=5034a392ff8c453f |
| failing|LLV_STRUCT|agg|inner | array<struct<x:bigint,y:string>> | ok n=1 sha=f858915ec289e06e |
| failing|LLV_STRUCT|wt|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|df_str|left | array<struct<x:bigint,y:string>> | ok n=11 sha=6f307aadd294b507 |
| failing|LLV_STRUCT|df_str|left_semi | array<struct<x:bigint,y:string>> | ok n=6 sha=96353b9f92e2bca2 |
| failing|LLV_STRUCT|sql_on|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|sql_using|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|cross_where|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|LLV_STRUCT|sql_comma|inner | array<struct<x:bigint,y:string>> | ok n=9 sha=2de42b07ea3770a9 |
| failing|NEST_LV|df_str|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_LV|df_list|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_LV|df_two|inner | struct<a:array<bigint>> | ok n=6 sha=5a484148f9734c37 |
| failing|NEST_LV|agg|inner | struct<a:array<bigint>> | ok n=1 sha=aeb7ed64fd7ef730 |
| failing|NEST_LV|wt|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_LV|df_str|left | struct<a:array<bigint>> | ok n=11 sha=261f51236e4fbcf7 |
| failing|NEST_LV|df_str|left_semi | struct<a:array<bigint>> | ok n=7 sha=ade26df2c1e3b7a2 |
| failing|NEST_LV|sql_on|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_LV|sql_using|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_LV|cross_where|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_LV|sql_comma|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|L_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|L_I64|df_list|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|L_I64|df_two|inner | array<bigint> | ok n=5 sha=5034a392ff8c453f |
| failing|L_I64|agg|inner | array<bigint> | ok n=1 sha=f858915ec289e06e |
| failing|L_I64|wt|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|L_I64|df_str|left | array<bigint> | ok n=11 sha=6f307aadd294b507 |
| failing|L_I64|df_str|left_semi | array<bigint> | ok n=6 sha=96353b9f92e2bca2 |
| failing|L_I64|sql_on|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|L_I64|sql_using|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|L_I64|cross_where|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|L_I64|sql_comma|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| failing|NEST_L|df_str|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_L|df_list|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_L|df_two|inner | struct<a:array<bigint>> | ok n=6 sha=5a484148f9734c37 |
| failing|NEST_L|agg|inner | struct<a:array<bigint>> | ok n=1 sha=aeb7ed64fd7ef730 |
| failing|NEST_L|wt|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_L|df_str|left | struct<a:array<bigint>> | ok n=11 sha=261f51236e4fbcf7 |
| failing|NEST_L|df_str|left_semi | struct<a:array<bigint>> | ok n=7 sha=ade26df2c1e3b7a2 |
| failing|NEST_L|sql_on|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_L|sql_using|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_L|cross_where|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| failing|NEST_L|sql_comma|inner | struct<a:array<bigint>> | ok n=10 sha=d09cfe68f10f46cc |
| lateral|LV_I64|lateral_str|inner | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LLV_I64|lateral_str|inner | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LV_UTF8|lateral_str|inner | array<string> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LLV_UTF8|lateral_str|inner | array<string> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LV_STRUCT|lateral_str|inner | array<struct<x:bigint,y:string>> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LLV_STRUCT|lateral_str|inner | array<struct<x:bigint,y:string>> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|L_I64|lateral_str|inner | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LL_I64|lateral_str|inner | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|FSL2_I64|lateral_str|inner | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|L_UTF8|lateral_str|inner | array<string> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|L_STRUCT|lateral_str|inner | array<struct<x:bigint,y:string>> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|NEST_LV|lateral_str|inner | struct<a:array<bigint>> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|NEST_L|lateral_str|inner | struct<a:array<bigint>> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|LV_I64|lateral_list|inner | array<bigint> | AttributeError: 'list' object has no attribute '_jc' |
| lateral|LV_I64|lateral_str|left | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| lateral|L_I64|lateral_list|inner | array<bigint> | AttributeError: 'list' object has no attribute '_jc' |
| lateral|L_I64|lateral_str|left | array<bigint> | AttributeError: 'str' object has no attribute '_jc' |
| mixed|plainxview_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|plainxview_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 |
| mixed|plainxview_rev|df_expr|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|plainxview_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|plainxview_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 |
| mixed|plainxview_same|df_expr|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|largexview_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|largexview_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 |
| mixed|largexview_rev|df_expr|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|largexview_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|largexview_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 |
| mixed|largexview_same|df_expr|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|fsl2xview_rev|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|fsl2xview_rev|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 |
| mixed|fsl2xview_rev|df_expr|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|fsl2xview_same|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|fsl2xview_same|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 |
| mixed|fsl2xview_same|df_expr|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|viewxplain_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|viewxplain_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 |
| mixed|viewxplain_rev|df_expr|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|viewxplain_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|viewxplain_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 |
| mixed|viewxplain_same|df_expr|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|viewxlarge_rev|join_str|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|viewxlarge_rev|join_str|left |  | ok n=2 sha=1429fe05cab82e21 |
| mixed|viewxlarge_rev|df_expr|inner |  | ok n=1 sha=e2efbdac0b9ad4bd |
| mixed|viewxlarge_same|join_str|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|viewxlarge_same|join_str|left |  | ok n=2 sha=9f97b9b4f06b1b37 |
| mixed|viewxlarge_same|df_expr|inner |  | ok n=1 sha=cb1fc0e151046ec2 |
| mixed|viewxfsl2_rev|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|viewxfsl2_rev|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 |
| mixed|viewxfsl2_rev|df_expr|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|viewxfsl2_same|join_str|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| mixed|viewxfsl2_same|join_str|left |  | ok n=2 sha=8a0a3c0ddb73e540 |
| mixed|viewxfsl2_same|df_expr|inner |  | ok n=0 sha=e3b0c44298fc1c14 |
| native|LV_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LV_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LLV_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LLV_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LV_UTF8|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LV_UTF8|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LLV_UTF8|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LLV_UTF8|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LV_STRUCT|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LV_STRUCT|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LLV_STRUCT|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LLV_STRUCT|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|L_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|L_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LL_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|LL_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|FSL2_I64|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|FSL2_I64|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|L_UTF8|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|L_UTF8|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|L_STRUCT|native_using|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|L_STRUCT|native_on|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| native|NEST_LV|native_using|inner |  | ok n=10 sha=d09cfe68f10f46cc |
| native|NEST_LV|native_on|inner |  | ok n=10 sha=d09cfe68f10f46cc |
| native|NEST_L|native_using|inner |  | ok n=10 sha=d09cfe68f10f46cc |
| native|NEST_L|native_on|inner |  | ok n=10 sha=d09cfe68f10f46cc |
| route|pandas:LV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| route|pandas:LLV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| route|pandas:REE_I64|df_str|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| route|pandas:UNION_I64|df_str|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| route|pandas:DICT_LV|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| route|polars:LV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| route|polars:LLV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| route|arrow:LV_I64|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| using|LV_I64|using_pair|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| using|LV_I64|using_star|inner |  | ok n=9 sha=3cd2e6cb9e84ac53 |
| using|LV_I64|using_alias|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| using|LV_I64|using_left|inner |  | ok n=11 sha=6f307aadd294b507 |
| using|LV_I64|natural|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| using|L_I64|using_pair|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| using|L_I64|using_star|inner |  | ok n=9 sha=3cd2e6cb9e84ac53 |
| using|L_I64|using_alias|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| using|L_I64|using_left|inner |  | ok n=11 sha=6f307aadd294b507 |
| using|L_I64|natural|inner |  | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_I64|df_str|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_I64|df_list|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_I64|wt|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_I64|sql_on|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_I64|df_str|left | bigint | ok n=11 sha=6f307aadd294b507 |
| variant|REE_UTF8|df_str|inner | string | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_UTF8|df_list|inner | string | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_UTF8|wt|inner | string | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_UTF8|sql_on|inner | string | ok n=9 sha=2de42b07ea3770a9 |
| variant|REE_UTF8|df_str|left | string | ok n=11 sha=6f307aadd294b507 |
| variant|DICT_LV|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_LV|df_list|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_LV|wt|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_LV|sql_on|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_LV|df_str|left | array<bigint> | ok n=11 sha=6f307aadd294b507 |
| variant|DICT_REE|df_str|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_REE|df_list|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_REE|wt|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_REE|sql_on|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|DICT_REE|df_str|left | bigint | ok n=11 sha=6f307aadd294b507 |
| variant|UNION_I64|df_str|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|UNION_I64|df_list|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|UNION_I64|wt|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|UNION_I64|sql_on|inner | bigint | ok n=9 sha=2de42b07ea3770a9 |
| variant|UNION_I64|df_str|left | bigint | ok n=11 sha=6f307aadd294b507 |
| variant|L_LV|df_str|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_LV|df_list|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_LV|wt|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_LV|sql_on|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_LV|df_str|left | array<array<bigint>> | ok n=11 sha=6f307aadd294b507 |
| variant|LL_LV|df_str|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|LL_LV|df_list|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|LL_LV|wt|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|LL_LV|sql_on|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|LL_LV|df_str|left | array<array<bigint>> | ok n=11 sha=6f307aadd294b507 |
| variant|FSL_LV|df_str|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|FSL_LV|df_list|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|FSL_LV|wt|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|FSL_LV|sql_on|inner | array<array<bigint>> | ok n=9 sha=2de42b07ea3770a9 |
| variant|FSL_LV|df_str|left | array<array<bigint>> | ok n=11 sha=6f307aadd294b507 |
| variant|MAP_LV|df_str|inner | map<string,array<bigint>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not |
| variant|MAP_LV|df_list|inner | map<string,array<bigint>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not |
| variant|MAP_LV|wt|inner | map<string,array<bigint>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not |
| variant|MAP_LV|sql_on|inner | map<string,array<bigint>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not |
| variant|MAP_LV|df_str|left | map<string,array<bigint>> | AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(k = k)" due to data type mismatch: The `=` does not |
| variant|STRUCT_REE|df_str|inner | struct<a:bigint> | ok n=10 sha=d09cfe68f10f46cc |
| variant|STRUCT_REE|df_list|inner | struct<a:bigint> | ok n=10 sha=d09cfe68f10f46cc |
| variant|STRUCT_REE|wt|inner | struct<a:bigint> | ok n=10 sha=d09cfe68f10f46cc |
| variant|STRUCT_REE|sql_on|inner | struct<a:bigint> | ok n=10 sha=d09cfe68f10f46cc |
| variant|STRUCT_REE|df_str|left | struct<a:bigint> | ok n=11 sha=261f51236e4fbcf7 |
| variant|L_REE|df_str|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_REE|df_list|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_REE|wt|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_REE|sql_on|inner | array<bigint> | ok n=9 sha=2de42b07ea3770a9 |
| variant|L_REE|df_str|left | array<bigint> | ok n=11 sha=6f307aadd294b507 |
