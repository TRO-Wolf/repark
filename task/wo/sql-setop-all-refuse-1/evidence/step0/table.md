# Step 0 on main

| group | operator | shape | Spark 4.1.2 | Spark door | verdict | native door | verdict |
|---|---|---|---|---|---|---|---|
| refuse | `INTERSECT ALL` | top | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| refuse | `INTERSECT ALL` | cte | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| refuse | `INTERSECT ALL` | scalar | [3] | [5] | WRONG | [5] | WRONG |
| refuse | `INTERSECT ALL` | in | [1][2][2] | [1][2][2] | EQUAL | [1][2][2] | EQUAL |
| refuse | `INTERSECT ALL` | temp-view | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| refuse | `INTERSECT ALL` | persistent-view | [1][2][2] | [1][1][1][2][2] | WRONG | refused AnalysisException: Error during planning: table 'ice.ns.vp_6' not found; create refused PySparkException | REFUSED |
| refuse | `INTERSECT ALL` | iceberg-source | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| refuse | `INTERSECT ALL` | insert | []; stored [[1], [2], [2]]; snapshots 1 | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG |
| refuse | `INTERSECT ALL` | ctas | []; stored [[1], [2], [2]]; snapshots 1 | []; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG | refused PySparkException: Unexpected => register_table does not support tables with data.; stored AnalysisException; snapshots None (no table) | REFUSED |
| refuse | `INTERSECT ALL` | merge | []; stored [[1], [2], [2]]; snapshots 1 | []; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG | refused ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, ...; stored []; snapshots 0 | REFUSED |
| refuse | `INTERSECT ALL` | union-nested | [1][2][2][9] | [1][1][1][2][2][9] | WRONG | [1][1][1][2][2][9] | WRONG |
| refuse | `INTERSECT ALL` | explain | rows | rows | EQUAL | rows | EQUAL |
| refuse | `EXCEPT ALL` | top | [1][1][4] | [4] | WRONG | [4] | WRONG |
| refuse | `EXCEPT ALL` | cte | [1][1][4] | [4] | WRONG | [4] | WRONG |
| refuse | `EXCEPT ALL` | scalar | [3] | [1] | WRONG | [1] | WRONG |
| refuse | `EXCEPT ALL` | in | [1] | [] | WRONG | [] | WRONG |
| refuse | `EXCEPT ALL` | temp-view | [1][1][4] | [4] | WRONG | [4] | WRONG |
| refuse | `EXCEPT ALL` | persistent-view | [1][1][4] | [4] | WRONG | refused AnalysisException: Error during planning: table 'ice.ns.vp_18' not found; create refused PySparkException | REFUSED |
| refuse | `EXCEPT ALL` | iceberg-source | [1][1][4] | [4] | WRONG | [4] | WRONG |
| refuse | `EXCEPT ALL` | insert | []; stored [[1], [1], [4]]; snapshots 1 | [1]; stored [[4]]; snapshots 1 | WRONG | [1]; stored [[4]]; snapshots 1 | WRONG |
| refuse | `EXCEPT ALL` | ctas | []; stored [[1], [1], [4]]; snapshots 1 | []; stored [[4]]; snapshots 1 | WRONG | refused PySparkException: Unexpected => register_table does not support tables with data.; stored AnalysisException; snapshots None (no table) | REFUSED |
| refuse | `EXCEPT ALL` | merge | []; stored [[1], [1], [4]]; snapshots 1 | []; stored [[4]]; snapshots 1 | WRONG | refused ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, ...; stored []; snapshots 0 | REFUSED |
| refuse | `EXCEPT ALL` | union-nested | [1][1][4][9] | [4][9] | WRONG | [4][9] | WRONG |
| refuse | `EXCEPT ALL` | explain | rows | rows | EQUAL | rows | EQUAL |
| refuse | `MINUS ALL` | top | [1][1][4] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| refuse | `MINUS ALL` | cte | [1][1][4] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| refuse | `MINUS ALL` | scalar | [3] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| refuse | `MINUS ALL` | in | [1] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| refuse | `MINUS ALL` | temp-view | [1][1][4] | refused AnalysisException: Error during planning: table 'spark_catalog.default.vt_29' not found; create refused UnsupportedOperationException | REFUSED | refused AnalysisException: Error during planning: table 'datafusion.public.vt_29' not found; create refused UnsupportedOperationException | REFUSED |
| refuse | `MINUS ALL` | persistent-view | [1][1][4] | refused AnalysisException: Error during planning: table 'ice.ns.vp_30' not found; create refused UnsupportedOperationException | REFUSED | refused AnalysisException: Error during planning: table 'ice.ns.vp_30' not found; create refused UnsupportedOperationException | REFUSED |
| refuse | `MINUS ALL` | iceberg-source | [1][1][4] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| refuse | `MINUS ALL` | insert | []; stored [[1], [1], [4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED |
| refuse | `MINUS ALL` | ctas | []; stored [[1], [1], [4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored AnalysisException; snapshots None (no table) | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored AnalysisException; snapshots None (no table) | REFUSED |
| refuse | `MINUS ALL` | merge | []; stored [[1], [1], [4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED | refused ParseException: SQL error: ParserError("Expected: VALUES, found: * at Line: 1, ...; stored []; snapshots 0 | REFUSED |
| refuse | `MINUS ALL` | union-nested | [1][1][4][9] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| refuse | `MINUS ALL` | explain | rows | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| spelling | `intersect all` | top | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| spelling | `intersect all` | insert | []; stored [[1], [2], [2]]; snapshots 1 | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG |
| spelling | `Intersect All` | top | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| spelling | `Intersect All` | insert | []; stored [[1], [2], [2]]; snapshots 1 | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG |
| spelling | `except all` | top | [1][1][4] | [4] | WRONG | [4] | WRONG |
| spelling | `except all` | insert | []; stored [[1], [1], [4]]; snapshots 1 | [1]; stored [[4]]; snapshots 1 | WRONG | [1]; stored [[4]]; snapshots 1 | WRONG |
| spelling | `ExCePt AlL` | top | [1][1][4] | [4] | WRONG | [4] | WRONG |
| spelling | `ExCePt AlL` | insert | []; stored [[1], [1], [4]]; snapshots 1 | [1]; stored [[4]]; snapshots 1 | WRONG | [1]; stored [[4]]; snapshots 1 | WRONG |
| spelling | `minus all` | top | [1][1][4] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| spelling | `minus all` | insert | []; stored [[1], [1], [4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED |
| spelling | `MiNuS aLl` | top | [1][1][4] | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented | REFUSED |
| spelling | `MiNuS aLl` | insert | []; stored [[1], [1], [4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented; stored []; snapshots 0 | REFUSED |
| spelling | `INTERSECT /* gap */ ALL` | top | [1][2][2] | [1][1][1][2][2] | WRONG | [1][1][1][2][2] | WRONG |
| spelling | `INTERSECT /* gap */ ALL` | insert | []; stored [[1], [2], [2]]; snapshots 1 | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG | [5]; stored [[1], [1], [1], [2], [2]]; snapshots 1 | WRONG |
| spelling | `EXCEPT\n  ALL` | top | [1][1][4] | [4] | WRONG | [4] | WRONG |
| spelling | `EXCEPT\n  ALL` | insert | []; stored [[1], [1], [4]]; snapshots 1 | [1]; stored [[4]]; snapshots 1 | WRONG | [1]; stored [[4]]; snapshots 1 | WRONG |
| control | `UNION ALL` | top | [1][1][1][1][2][2][2][2][3][4] | [1][1][1][1][2][2][2][2][3][4] | EQUAL | [1][1][1][1][2][2][2][2][3][4] | EQUAL |
| control | `UNION ALL` | cte | [1][1][1][1][2][2][2][2][3][4] | [1][1][1][1][2][2][2][2][3][4] | EQUAL | [1][1][1][1][2][2][2][2][3][4] | EQUAL |
| control | `UNION ALL` | insert | []; stored [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]]; snapshots 1 | [10]; stored [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]]; snapshots 1 | EQUAL | [10]; stored [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]]; snapshots 1 | EQUAL |
| control | `UNION ALL` | ctas | []; stored [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]]; snapshots 1 | []; stored [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]]; snapshots 1 | EQUAL | refused PySparkException: Unexpected => register_table does not support tables with data.; stored AnalysisException; snapshots None (no table) | REFUSED |
| control | `UNION` | top | [1][2][3][4] | [1][2][3][4] | EQUAL | [1][2][3][4] | EQUAL |
| control | `UNION` | cte | [1][2][3][4] | [1][2][3][4] | EQUAL | [1][2][3][4] | EQUAL |
| control | `UNION` | insert | []; stored [[1], [2], [3], [4]]; snapshots 1 | [4]; stored [[1], [2], [3], [4]]; snapshots 1 | EQUAL | [4]; stored [[1], [2], [3], [4]]; snapshots 1 | EQUAL |
| control | `UNION` | ctas | []; stored [[1], [2], [3], [4]]; snapshots 1 | []; stored [[1], [2], [3], [4]]; snapshots 1 | EQUAL | []; stored []; snapshots 0 | WRONG |
| control | `INTERSECT` | top | [1][2] | [1][2] | EQUAL | [1][2] | EQUAL |
| control | `INTERSECT` | cte | [1][2] | [1][2] | EQUAL | [1][2] | EQUAL |
| control | `INTERSECT` | insert | []; stored [[1], [2]]; snapshots 1 | [2]; stored [[1], [2]]; snapshots 1 | EQUAL | [2]; stored [[1], [2]]; snapshots 1 | EQUAL |
| control | `INTERSECT` | ctas | []; stored [[1], [2]]; snapshots 1 | []; stored [[1], [2]]; snapshots 1 | EQUAL | []; stored []; snapshots 0 | WRONG |
| control | `EXCEPT` | top | [4] | [4] | EQUAL | [4] | EQUAL |
| control | `EXCEPT` | cte | [4] | [4] | EQUAL | [4] | EQUAL |
| control | `EXCEPT` | insert | []; stored [[4]]; snapshots 1 | [1]; stored [[4]]; snapshots 1 | EQUAL | [1]; stored [[4]]; snapshots 1 | EQUAL |
| control | `EXCEPT` | ctas | []; stored [[4]]; snapshots 1 | []; stored [[4]]; snapshots 1 | EQUAL | []; stored []; snapshots 0 | WRONG |
| control | `EXCEPT DISTINCT` | top | [4] | [4] | EQUAL | [4] | EQUAL |
| control | `EXCEPT DISTINCT` | cte | [4] | [4] | EQUAL | [4] | EQUAL |
| control | `EXCEPT DISTINCT` | insert | []; stored [[4]]; snapshots 1 | [1]; stored [[4]]; snapshots 1 | EQUAL | [1]; stored [[4]]; snapshots 1 | EQUAL |
| control | `EXCEPT DISTINCT` | ctas | []; stored [[4]]; snapshots 1 | []; stored [[4]]; snapshots 1 | EQUAL | []; stored []; snapshots 0 | WRONG |
| control | `INTERSECT DISTINCT` | top | [1][2] | [1][2] | EQUAL | [1][2] | EQUAL |
| control | `INTERSECT DISTINCT` | cte | [1][2] | [1][2] | EQUAL | [1][2] | EQUAL |
| control | `INTERSECT DISTINCT` | insert | []; stored [[1], [2]]; snapshots 1 | [2]; stored [[1], [2]]; snapshots 1 | EQUAL | [2]; stored [[1], [2]]; snapshots 1 | EQUAL |
| control | `INTERSECT DISTINCT` | ctas | []; stored [[1], [2]]; snapshots 1 | []; stored [[1], [2]]; snapshots 1 | EQUAL | []; stored []; snapshots 0 | WRONG |
| control | `MINUS` | top | [4] | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented | REFUSED |
| control | `MINUS` | cte | [4] | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented | REFUSED |
| control | `MINUS` | insert | []; stored [[4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented; stored []; snapshots 0 | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented; stored []; snapshots 0 | REFUSED |
| control | `MINUS` | ctas | []; stored [[4]]; snapshots 1 | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented; stored AnalysisException; snapshots None (no table) | REFUSED | refused UnsupportedOperationException: This feature is not implemented: MINUS not implemented; stored AnalysisException; snapshots None (no table) | REFUSED |
| text-control | `string-literal` | top | ['a INTERSECT ALL b'] | ['a INTERSECT ALL b'] | EQUAL | ['a INTERSECT ALL b'] | EQUAL |
| text-control | `block-comment` | top | [1] | [1] | EQUAL | [1] | EQUAL |
| text-control | `line-comment` | top | [1] | [1] | EQUAL | [1] | EQUAL |
| text-control | `quoted-ident` | top | [1] | [1] | EQUAL | [1] | EQUAL |
| text-control | `all-prefix-ident` | top | [1][2] | [1][2] | EQUAL | [1][2] | EQUAL |
| workaround | `numbered-intersect` | top | [1][2][2] | [1][2][2] | EQUAL | [1][2][2] | EQUAL |
| workaround | `numbered-except` | top | [1][1][4] | [1][1][4] | EQUAL | [1][1][4] | EQUAL |
| dataframe | `df-intersectAll` | api | [1][2][2] | refused UnsupportedOperationException: DataFrame.intersectAll multiset semantics are not Spark-correct on ... | REFUSED | n/a | n/a |
| dataframe | `df-exceptAll` | api | [1][1][4] | refused UnsupportedOperationException: DataFrame.exceptAll multiset semantics are not Spark-correct on this ... | REFUSED | n/a | n/a |
| dataframe | `df-intersect` | api | [1][2] | [1][2] | EQUAL | n/a | n/a |
| dataframe | `df-subtract` | api | [4] | [4] | EQUAL | n/a | n/a |
| dataframe | `fexpr-scalar` | api | [3] | [5] | WRONG | n/a | n/a |
| dataframe | `fexpr-filter-in` | api | [1] | [] | WRONG | n/a | n/a |
| dataframe | `fexpr-filter-col` | api | [1] | [] | WRONG | n/a | n/a |
| dataframe | `fexpr-control` | api | [2] | [2] | EQUAL | n/a | n/a |
| dataframe | `selectexpr-scalar` | api | [3][3][3][3] | [5][5][5][5] | WRONG | n/a | n/a |
