# FILE-EXTENSION-FILTER-1 local write paths beside hidden names

Rendered by `render_write_hidden.py` from `probe_write_hidden.py` on Spark 4.1.2,
main and head; each cell writes one row (`id` 20) into a fresh directory, then
reads it back with the same engine.

| cell | ANSI | Spark 4.1.2 | main | head | moved |
|---|---|---|---|---|---|
| `csv/append/underscore-other-header` | false | write ok; read [10, 20] | write refuses (AnalysisException); read [1, 10] | write ok; read [1, 10, 20] | moved, still unlike Spark |
| `json/append/dot-not-json` | false | write ok; read [10, 20] | write ok; read refuses | write ok; read refuses | no (unlike Spark) |
| `parquet/append/dot` | false | write ok; read [10, 20] | write ok; read [10, 12, 20] | write ok; read [10, 20] | **fixed** |
| `parquet/append/dot-not-parquet` | false | write ok; read [10, 20] | write refuses (AnalysisException); read refuses | write ok; read [10, 20] | **fixed** |
| `parquet/append/markers` | false | write ok; read [10, 20] | write ok; read [10, 20] | write ok; read [10, 20] | no (= Spark) |
| `parquet/append/only-hidden` | false | write ok; read [20] | write ok; read [11, 20] | write ok; read [20] | **fixed** |
| `parquet/append/plain` | false | write ok; read [10, 20] | write ok; read [10, 20] | write ok; read [10, 20] | no (= Spark) |
| `parquet/append/temporary-not-parquet` | false | write refuses (Py4JJavaError); read refuses | write refuses (AnalysisException); read [10] | write ok; read [10, 20] | moved, still unlike Spark |
| `parquet/append/underscore` | false | write ok; read [10, 20] | write ok; read [10, 11, 20] | write ok; read [10, 20] | **fixed** |
| `parquet/append/underscore-other-schema` | false | write ok; read [10, 20] | write refuses (AnalysisException); read [None, z] | write ok; read [10, 20] | **fixed** |
| `parquet/append/visible-other-schema` | false | write ok; read [10, 20, None] | write refuses (AnalysisException); read [10, None] | write refuses (AnalysisException); read [10, None] | no (unlike Spark) |
| `parquet/errorifexists/only-hidden` | false | write refuses (AnalysisException); read refuses | write refuses (AnalysisException); read refuses | write refuses (AnalysisException); read refuses | no (= Spark) |
| `parquet/ignore/only-hidden` | false | write ok; read refuses | write ok; read [11] | write ok; read refuses | **fixed** |
| `parquet/overwrite/hidden` | false | write ok; read [20] | write ok; read [20] | write ok; read [20] | no (= Spark) |
| `csv/append/underscore-other-header` | true | write ok; read [10, 20] | write refuses (AnalysisException); read [1, 10] | write ok; read [1, 10, 20] | moved, still unlike Spark |
| `json/append/dot-not-json` | true | write ok; read [10, 20] | write ok; read refuses | write ok; read refuses | no (unlike Spark) |
| `parquet/append/dot` | true | write ok; read [10, 20] | write ok; read [10, 12, 20] | write ok; read [10, 20] | **fixed** |
| `parquet/append/dot-not-parquet` | true | write ok; read [10, 20] | write refuses (AnalysisException); read refuses | write ok; read [10, 20] | **fixed** |
| `parquet/append/markers` | true | write ok; read [10, 20] | write ok; read [10, 20] | write ok; read [10, 20] | no (= Spark) |
| `parquet/append/only-hidden` | true | write ok; read [20] | write ok; read [11, 20] | write ok; read [20] | **fixed** |
| `parquet/append/plain` | true | write ok; read [10, 20] | write ok; read [10, 20] | write ok; read [10, 20] | no (= Spark) |
| `parquet/append/temporary-not-parquet` | true | write refuses (Py4JJavaError); read refuses | write refuses (AnalysisException); read [10] | write ok; read [10, 20] | moved, still unlike Spark |
| `parquet/append/underscore` | true | write ok; read [10, 20] | write ok; read [10, 11, 20] | write ok; read [10, 20] | **fixed** |
| `parquet/append/underscore-other-schema` | true | write ok; read [10, 20] | write refuses (AnalysisException); read [None, z] | write ok; read [10, 20] | **fixed** |
| `parquet/append/visible-other-schema` | true | write ok; read [10, 20, None] | write refuses (AnalysisException); read [10, None] | write refuses (AnalysisException); read [10, None] | no (unlike Spark) |
| `parquet/errorifexists/only-hidden` | true | write refuses (AnalysisException); read refuses | write refuses (AnalysisException); read refuses | write refuses (AnalysisException); read refuses | no (= Spark) |
| `parquet/ignore/only-hidden` | true | write ok; read refuses | write ok; read [11] | write ok; read refuses | **fixed** |
| `parquet/overwrite/hidden` | true | write ok; read [20] | write ok; read [20] | write ok; read [20] | no (= Spark) |
