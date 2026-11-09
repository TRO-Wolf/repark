# Unit C delta-2 acceptance table (measured, not committed)

Legs: spark = Spark 4.1.2 oracle; head = unit C head b8a703c3; base = RePark 1.5.6 from PyPI.
Label is against the Spark twin. Head and base cells without a twin are ORACLE-MISSING.

| id | leg | outcome | stage | label | result or refusal (first 200 chars) |
|---|---|---|---|---|---|
| 1.union-all.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union-all.sql.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union-all.sql.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.df.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union-all.df.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.df.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union-all.df.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union-all.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union.sql.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union.sql.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.df.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union.df.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.df.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 1.union.df.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 1.union.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 2.union-all.sql.read | spark | ANSWERED | - | (oracle) | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} |
| 2.union-all.sql.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.sql.insert | spark | ANSWERED | - | (oracle) | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} |
| 2.union-all.sql.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.df.read | spark | ANSWERED | - | (oracle) | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} |
| 2.union-all.df.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.df.insert | spark | ANSWERED | - | (oracle) | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} |
| 2.union-all.df.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 2.union-all.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} |
| 3.union-all.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 3.union-all.sql.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 3.union-all.sql.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.df.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 3.union-all.df.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.df.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 3.union-all.df.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 3.union-all.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 4.intersect.sql.read | spark | REFUSED | read | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not  |
| 4.intersect.sql.read | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.sql.read | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.sql.insert | spark | REFUSED | insert | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not  |
| 4.intersect.sql.insert | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.sql.insert | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.df.read | spark | REFUSED | read | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not  |
| 4.intersect.df.read | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.df.read | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.df.insert | spark | REFUSED | insert | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] INTERSECT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not  |
| 4.intersect.df.insert | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.df.insert | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | (no rows) |
| 4.intersect.native.read | head | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 4.intersect.native.read | base | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 4.intersect.native.insert | head | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 4.intersect.native.insert | base | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 4.except.sql.read | spark | REFUSED | read | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not com |
| 4.except.sql.read | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.sql.read | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.sql.insert | spark | REFUSED | insert | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not com |
| 4.except.sql.insert | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.sql.insert | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.df.read | spark | REFUSED | read | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not com |
| 4.except.df.read | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.df.read | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.df.insert | spark | REFUSED | insert | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] EXCEPT can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<b: INT NOT NULL, a: INT NOT NULL>" type which is not com |
| 4.except.df.insert | head | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.df.insert | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 4.except.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} |
| 5.union-all.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 5.union-all.sql.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 5.union-all.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 5.union-all.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 5.union-all.sql.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 5.union-all.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 5.union-all.df.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 5.union-all.df.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 5.union-all.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 5.union-all.df.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 5.union-all.df.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 5.union-all.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 5.union-all.native.read | head | REFUSED | read | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 5.union-all.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 5.union-all.native.insert | head | REFUSED | insert | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 5.union-all.native.insert | base | REFUSED | insert | ORACLE-MISSING | PySparkException: Internal error: cannot convert text Int64 into byte Int32. This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https |
| 6.ctl-null-middle.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-null-middle.sql.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-middle.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-middle.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-null-middle.sql.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-middle.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-middle.df.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-null-middle.df.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-middle.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-middle.df.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-null-middle.df.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-middle.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-middle.native.read | head | REFUSED | read | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-middle.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-middle.native.insert | head | REFUSED | insert | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-middle.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-nested.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-null-nested.sql.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-nested.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-nested.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-null-nested.sql.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-nested.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-nested.df.read | spark | REFUSED | read | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<a: INT NOT NULL, b: INT NOT NULL>" type which is not comp |
| 6.ctl-null-nested.df.read | head | REFUSED | read | BOTH-REFUSED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-nested.df.read | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-nested.df.insert | spark | REFUSED | insert | (oracle) | AnalysisException: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "STRUCT<a: INT NOT NULL, b: INT NOT NULL>" type which is not comp |
| 6.ctl-null-nested.df.insert | head | REFUSED | insert | BOTH-REFUSED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-nested.df.insert | base | ANSWERED | - | ACCEPTED-WHERE-SPARK-REFUSED | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-nested.native.read | head | REFUSED | read | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-nested.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-null-nested.native.insert | head | REFUSED | insert | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 6.ctl-null-nested.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 6.ctl-same-names.sql.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.sql.read | head | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.sql.read | base | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.sql.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.sql.insert | head | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.sql.insert | base | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.df.read | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.df.read | head | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.df.read | base | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.df.insert | spark | ANSWERED | - | (oracle) | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.df.insert | head | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.df.insert | base | ANSWERED | - | EQUAL | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 6.ctl-same-names.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 12.cast-reordered.sql.read | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 12.cast-reordered.sql.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.sql.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.sql.insert | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 12.cast-reordered.sql.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.sql.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.df.read | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 12.cast-reordered.df.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.df.insert | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 12.cast-reordered.df.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 12.cast-reordered.native.insert | head | REFUSED | insert | ORACLE-MISSING | PySparkException: Internal error: cannot convert text Int64 into byte Int32. This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https |
| 12.cast-reordered.native.insert | base | REFUSED | insert | ORACLE-MISSING | PySparkException: Internal error: cannot convert text Int64 into byte Int32. This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https |
| 10.F1-select.df.read | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.F1-select.df.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 10.F1-select.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"v": 4} |
| 10.F1-select.df.insert | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.F1-select.df.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 10.F1-select.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"v": 4} |
| 10.F1-withColumn.df.read | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.F1-withColumn.df.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 10.F1-withColumn.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"v": 4} |
| 10.F1-withColumn.df.insert | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.F1-withColumn.df.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 10.F1-withColumn.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"v": 4} |
| 10.F1-filter.df.read | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.F1-filter.df.read | head | REFUSED | read | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 10.F1-filter.df.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"v": 4} |
| 10.F1-filter.df.insert | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.F1-filter.df.insert | head | REFUSED | insert | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 10.F1-filter.df.insert | base | ANSWERED | - | ROWS-DIFFER | {"v": 4} |
| 10.int-select.df.read | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.int-select.df.read | head | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-select.df.read | base | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-select.df.insert | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.int-select.df.insert | head | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-select.df.insert | base | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-withColumn.df.read | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.int-withColumn.df.read | head | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-withColumn.df.read | base | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-withColumn.df.insert | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.int-withColumn.df.insert | head | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-withColumn.df.insert | base | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-filter.df.read | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.int-filter.df.read | head | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-filter.df.read | base | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-filter.df.insert | spark | ANSWERED | - | (oracle) | {"v": 3} |
| 10.int-filter.df.insert | head | ANSWERED | - | EQUAL | {"v": 3} |
| 10.int-filter.df.insert | base | ANSWERED | - | EQUAL | {"v": 3} |
| 10.same-select.df.read | spark | ANSWERED | - | (oracle) | {"v": 4} |
| 10.same-select.df.read | head | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-select.df.read | base | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-select.df.insert | spark | ANSWERED | - | (oracle) | {"v": 4} |
| 10.same-select.df.insert | head | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-select.df.insert | base | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-withColumn.df.read | spark | ANSWERED | - | (oracle) | {"v": 4} |
| 10.same-withColumn.df.read | head | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-withColumn.df.read | base | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-withColumn.df.insert | spark | ANSWERED | - | (oracle) | {"v": 4} |
| 10.same-withColumn.df.insert | head | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-withColumn.df.insert | base | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-filter.df.read | spark | ANSWERED | - | (oracle) | {"v": 4} |
| 10.same-filter.df.read | head | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-filter.df.read | base | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-filter.df.insert | spark | ANSWERED | - | (oracle) | {"v": 4} |
| 10.same-filter.df.insert | head | ANSWERED | - | EQUAL | {"v": 4} |
| 10.same-filter.df.insert | base | ANSWERED | - | EQUAL | {"v": 4} |
| 11.create-view.sql.read | head | REFUSED | create | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 11.create-view.sql.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 11.create-view.sql.insert | head | REFUSED | create | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 11.create-view.sql.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 11.create-temporary-view.sql.read | head | REFUSED | create | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 11.create-temporary-view.sql.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 11.create-temporary-view.sql.insert | head | REFUSED | create | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 11.create-temporary-view.sql.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 11.dataframe-temp-view.df.read | head | REFUSED | read | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 11.dataframe-temp-view.df.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 11.dataframe-temp-view.df.insert | head | REFUSED | insert | ORACLE-MISSING | AnalysisException: struct_setop_names caused by Error during planning: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The first column of the second table is "Struct("b":  |
| 11.dataframe-temp-view.df.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-create.native.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-create.native.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-create.native.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-create.native.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-replace.native.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-replace.native.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-replace.native.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-replace.native.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-intersect.native.read | head | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 7.view-intersect.native.read | base | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 7.view-intersect.native.insert | head | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 7.view-intersect.native.insert | base | ANSWERED | - | ORACLE-MISSING | (no rows) |
| 7.view-except.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} |
| 7.view-except.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} |
| 7.view-except.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} |
| 7.view-except.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} |
| 7.view-scalar.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} |
| 7.view-scalar.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} |
| 7.view-scalar.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 4, "b": 3}} |
| 7.view-scalar.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 4, "b": 3}} |
| 7.view-cte.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-cte.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-cte.native.insert | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 7.view-cte.native.insert | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 8.prepare-union.native.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 8.prepare-union.native.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 8.prepare-union.native.insert | head | REFUSED | prepare | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: Error during planning: PREPARE `p103` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works. |
| 8.prepare-union.native.insert | base | REFUSED | prepare | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: Error during planning: PREPARE `p103` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works. |
| 8.prepare-param.native.read | head | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 8.prepare-param.native.read | base | ANSWERED | - | ORACLE-MISSING | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} |
| 8.prepare-param.native.insert | head | REFUSED | prepare | ORACLE-MISSING | AnalysisException: Error during planning: PREPARE `p105` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works. |
| 8.prepare-param.native.insert | base | REFUSED | prepare | ORACLE-MISSING | AnalysisException: Error during planning: PREPARE `p105` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works. |
| 9.rec-reordered.native.read | head | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-reordered.native.read | base | ANSWERED | - | RESULT-ROWS-DIFFER | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-reordered.native.insert | head | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-reordered.native.insert | base | ANSWERED | - | ROWS-DIFFER | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-reordered.sql.read | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 11, "b": 12}} |
| 9.rec-reordered.sql.insert | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 11, "b": 12}} |
| 9.rec-same.native.read | head | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-same.native.read | base | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-same.native.insert | head | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-same.native.insert | base | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-same.sql.read | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 9.rec-same.sql.insert | spark | ANSWERED | - | (oracle) | {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} |
| 13.view-same-name.native.read | head | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 13.view-same-name.native.read | base | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 13.view-same-name.native.insert | head | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 13.view-same-name.native.insert | base | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 13.prepare-same-name.native.read | head | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 13.prepare-same-name.native.read | base | ANSWERED | - | EQUAL | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| 13.prepare-same-name.native.insert | head | REFUSED | prepare | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: Error during planning: PREPARE `p117` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works. |
| 13.prepare-same-name.native.insert | base | REFUSED | prepare | REFUSED-WHERE-SPARK-ACCEPTED | AnalysisException: Error during planning: PREPARE `p117` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works. |
| ref.lr.sql.read | spark | ANSWERED | - | (reference) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| ref.lr.sql.insert | spark | ANSWERED | - | (reference) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| ref.lrs.sql.read | spark | ANSWERED | - | (reference) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |
| ref.lrs.sql.insert | spark | ANSWERED | - | (reference) | {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} |

## Counts by label, row and leg

| row | leg | label | count |
|---|---|---|---|
| 1 | base | ORACLE-MISSING | 4 |
| 1 | base | RESULT-ROWS-DIFFER | 4 |
| 1 | base | ROWS-DIFFER | 4 |
| 1 | head | ORACLE-MISSING | 4 |
| 1 | head | RESULT-ROWS-DIFFER | 4 |
| 1 | head | ROWS-DIFFER | 4 |
| 10 | base | EQUAL | 12 |
| 10 | base | RESULT-ROWS-DIFFER | 3 |
| 10 | base | ROWS-DIFFER | 3 |
| 10 | head | EQUAL | 12 |
| 10 | head | REFUSED-WHERE-SPARK-ACCEPTED | 6 |
| 11 | base | ORACLE-MISSING | 6 |
| 11 | head | ORACLE-MISSING | 6 |
| 12 | base | ORACLE-MISSING | 2 |
| 12 | base | RESULT-ROWS-DIFFER | 2 |
| 12 | base | ROWS-DIFFER | 2 |
| 12 | head | ORACLE-MISSING | 2 |
| 12 | head | RESULT-ROWS-DIFFER | 2 |
| 12 | head | ROWS-DIFFER | 2 |
| 13 | base | EQUAL | 3 |
| 13 | base | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| 13 | head | EQUAL | 3 |
| 13 | head | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| 2 | base | ORACLE-MISSING | 2 |
| 2 | base | RESULT-ROWS-DIFFER | 2 |
| 2 | base | ROWS-DIFFER | 2 |
| 2 | head | ORACLE-MISSING | 2 |
| 2 | head | RESULT-ROWS-DIFFER | 2 |
| 2 | head | ROWS-DIFFER | 2 |
| 3 | base | ORACLE-MISSING | 2 |
| 3 | base | RESULT-ROWS-DIFFER | 2 |
| 3 | base | ROWS-DIFFER | 2 |
| 3 | head | ORACLE-MISSING | 2 |
| 3 | head | RESULT-ROWS-DIFFER | 2 |
| 3 | head | ROWS-DIFFER | 2 |
| 4 | base | ACCEPTED-WHERE-SPARK-REFUSED | 8 |
| 4 | base | ORACLE-MISSING | 4 |
| 4 | head | ACCEPTED-WHERE-SPARK-REFUSED | 8 |
| 4 | head | ORACLE-MISSING | 4 |
| 5 | base | ORACLE-MISSING | 2 |
| 5 | base | RESULT-ROWS-DIFFER | 2 |
| 5 | base | ROWS-DIFFER | 2 |
| 5 | head | ORACLE-MISSING | 2 |
| 5 | head | REFUSED-WHERE-SPARK-ACCEPTED | 4 |
| 6 | base | ACCEPTED-WHERE-SPARK-REFUSED | 2 |
| 6 | base | EQUAL | 4 |
| 6 | base | ORACLE-MISSING | 6 |
| 6 | base | RESULT-ROWS-DIFFER | 3 |
| 6 | base | ROWS-DIFFER | 3 |
| 6 | head | BOTH-REFUSED | 2 |
| 6 | head | EQUAL | 4 |
| 6 | head | ORACLE-MISSING | 6 |
| 6 | head | REFUSED-WHERE-SPARK-ACCEPTED | 6 |
| 7 | base | ORACLE-MISSING | 8 |
| 7 | base | RESULT-ROWS-DIFFER | 2 |
| 7 | base | ROWS-DIFFER | 2 |
| 7 | head | ORACLE-MISSING | 8 |
| 7 | head | RESULT-ROWS-DIFFER | 2 |
| 7 | head | ROWS-DIFFER | 2 |
| 8 | base | ORACLE-MISSING | 2 |
| 8 | base | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| 8 | base | RESULT-ROWS-DIFFER | 1 |
| 8 | head | ORACLE-MISSING | 2 |
| 8 | head | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| 8 | head | RESULT-ROWS-DIFFER | 1 |
| 9 | base | EQUAL | 2 |
| 9 | base | RESULT-ROWS-DIFFER | 1 |
| 9 | base | ROWS-DIFFER | 1 |
| 9 | head | EQUAL | 2 |
| 9 | head | RESULT-ROWS-DIFFER | 1 |
| 9 | head | ROWS-DIFFER | 1 |

## Per row and door (spark / base / head)

- row 1 sql: spark {'ANSWERED': 4} | head {'ANSWERED': 4} labels {'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2} | base {'ANSWERED': 4} labels {'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2}
- row 1 df: spark {'ANSWERED': 4} | head {'ANSWERED': 4} labels {'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2} | base {'ANSWERED': 4} labels {'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2}
- row 1 native: head {'ANSWERED': 4} labels {'ORACLE-MISSING': 4} | base {'ANSWERED': 4} labels {'ORACLE-MISSING': 4}
- row 2 sql: spark {'ANSWERED': 2} | head {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 2 df: spark {'ANSWERED': 2} | head {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 2 native: head {'ANSWERED': 2} labels {'ORACLE-MISSING': 2} | base {'ANSWERED': 2} labels {'ORACLE-MISSING': 2}
- row 3 sql: spark {'ANSWERED': 2} | head {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 3 df: spark {'ANSWERED': 2} | head {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 3 native: head {'ANSWERED': 2} labels {'ORACLE-MISSING': 2} | base {'ANSWERED': 2} labels {'ORACLE-MISSING': 2}
- row 4 sql: spark {'REFUSED@insert': 2, 'REFUSED@read': 2} | head {'ANSWERED': 4} labels {'ACCEPTED-WHERE-SPARK-REFUSED': 4} | base {'ANSWERED': 4} labels {'ACCEPTED-WHERE-SPARK-REFUSED': 4}
- row 4 df: spark {'REFUSED@insert': 2, 'REFUSED@read': 2} | head {'ANSWERED': 4} labels {'ACCEPTED-WHERE-SPARK-REFUSED': 4} | base {'ANSWERED': 4} labels {'ACCEPTED-WHERE-SPARK-REFUSED': 4}
- row 4 native: head {'ANSWERED': 4} labels {'ORACLE-MISSING': 4} | base {'ANSWERED': 4} labels {'ORACLE-MISSING': 4}
- row 5 sql: spark {'ANSWERED': 2} | head {'REFUSED@insert': 1, 'REFUSED@read': 1} labels {'REFUSED-WHERE-SPARK-ACCEPTED': 2} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 5 df: spark {'ANSWERED': 2} | head {'REFUSED@insert': 1, 'REFUSED@read': 1} labels {'REFUSED-WHERE-SPARK-ACCEPTED': 2} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 5 native: head {'REFUSED@insert': 1, 'REFUSED@read': 1} labels {'ORACLE-MISSING': 2} | base {'ANSWERED': 1, 'REFUSED@insert': 1} labels {'ORACLE-MISSING': 2}
- row 6 sql: spark {'ANSWERED': 6} | head {'ANSWERED': 2, 'REFUSED@insert': 2, 'REFUSED@read': 2} labels {'EQUAL': 2, 'REFUSED-WHERE-SPARK-ACCEPTED': 4} | base {'ANSWERED': 6} labels {'EQUAL': 2, 'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2}
- row 6 df: spark {'ANSWERED': 4, 'REFUSED@insert': 1, 'REFUSED@read': 1} | head {'ANSWERED': 2, 'REFUSED@insert': 2, 'REFUSED@read': 2} labels {'BOTH-REFUSED': 2, 'EQUAL': 2, 'REFUSED-WHERE-SPARK-ACCEPTED': 2} | base {'ANSWERED': 6} labels {'ACCEPTED-WHERE-SPARK-REFUSED': 2, 'EQUAL': 2, 'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 6 native: head {'ANSWERED': 2, 'REFUSED@insert': 2, 'REFUSED@read': 2} labels {'ORACLE-MISSING': 6} | base {'ANSWERED': 6} labels {'ORACLE-MISSING': 6}
- row 12 sql: spark {'ANSWERED': 2} | head {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 12 df: spark {'ANSWERED': 2} | head {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 2} labels {'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 12 native: head {'ANSWERED': 1, 'REFUSED@insert': 1} labels {'ORACLE-MISSING': 2} | base {'ANSWERED': 1, 'REFUSED@insert': 1} labels {'ORACLE-MISSING': 2}
- row 10 df: spark {'ANSWERED': 18} | head {'ANSWERED': 12, 'REFUSED@insert': 3, 'REFUSED@read': 3} labels {'EQUAL': 12, 'REFUSED-WHERE-SPARK-ACCEPTED': 6} | base {'ANSWERED': 18} labels {'EQUAL': 12, 'RESULT-ROWS-DIFFER': 3, 'ROWS-DIFFER': 3}
- row 11 sql: head {'REFUSED@create': 4} labels {'ORACLE-MISSING': 4} | base {'ANSWERED': 4} labels {'ORACLE-MISSING': 4}
- row 11 df: head {'REFUSED@insert': 1, 'REFUSED@read': 1} labels {'ORACLE-MISSING': 2} | base {'ANSWERED': 2} labels {'ORACLE-MISSING': 2}
- row 7 native: head {'ANSWERED': 12} labels {'ORACLE-MISSING': 8, 'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2} | base {'ANSWERED': 12} labels {'ORACLE-MISSING': 8, 'RESULT-ROWS-DIFFER': 2, 'ROWS-DIFFER': 2}
- row 8 native: head {'ANSWERED': 2, 'REFUSED@prepare': 2} labels {'ORACLE-MISSING': 2, 'REFUSED-WHERE-SPARK-ACCEPTED': 1, 'RESULT-ROWS-DIFFER': 1} | base {'ANSWERED': 2, 'REFUSED@prepare': 2} labels {'ORACLE-MISSING': 2, 'REFUSED-WHERE-SPARK-ACCEPTED': 1, 'RESULT-ROWS-DIFFER': 1}
- row 9 sql: spark {'ANSWERED': 4}
- row 9 native: head {'ANSWERED': 4} labels {'EQUAL': 2, 'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1} | base {'ANSWERED': 4} labels {'EQUAL': 2, 'RESULT-ROWS-DIFFER': 1, 'ROWS-DIFFER': 1}
- row 13 native: head {'ANSWERED': 3, 'REFUSED@prepare': 1} labels {'EQUAL': 3, 'REFUSED-WHERE-SPARK-ACCEPTED': 1} | base {'ANSWERED': 3, 'REFUSED@prepare': 1} labels {'EQUAL': 3, 'REFUSED-WHERE-SPARK-ACCEPTED': 1}
- row ref sql: spark {'ANSWERED': 4}

## Head answers (every cell, one line each)

- 1.union-all.sql.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 1.union-all.sql.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 1.union-all.df.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 1.union-all.df.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 1.union-all.native.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 1.union-all.native.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 1.union.sql.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 1.union.sql.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 1.union.df.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 1.union.df.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 1.union.native.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 1.union.native.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 2.union-all.sql.read: head {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} | spark ANSWERED {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} | RESULT-ROWS-DIFFER
- 2.union-all.sql.insert: head {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} | spark ANSWERED {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} | ROWS-DIFFER
- 2.union-all.df.read: head {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} | spark ANSWERED {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} | RESULT-ROWS-DIFFER
- 2.union-all.df.insert: head {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} | spark ANSWERED {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 3, "b": 4}}} | ROWS-DIFFER
- 2.union-all.native.read: head {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} | no spark twin | ORACLE-MISSING
- 2.union-all.native.insert: head {"s": {"x": null}} ; {"s": {"x": {"a": 1, "b": 2}}} ; {"s": {"x": {"a": 4, "b": 3}}} | no spark twin | ORACLE-MISSING
- 3.union-all.sql.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 3.union-all.sql.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 3.union-all.df.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 3.union-all.df.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 3.union-all.native.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 3.union-all.native.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 4.intersect.sql.read: head (no rows) | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.intersect.sql.insert: head (no rows) | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.intersect.df.read: head (no rows) | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.intersect.df.insert: head (no rows) | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.intersect.native.read: head (no rows) | no spark twin | ORACLE-MISSING
- 4.intersect.native.insert: head (no rows) | no spark twin | ORACLE-MISSING
- 4.except.sql.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.except.sql.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.except.df.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.except.df.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} | spark REFUSED | ACCEPTED-WHERE-SPARK-REFUSED
- 4.except.native.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} | no spark twin | ORACLE-MISSING
- 4.except.native.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} | no spark twin | ORACLE-MISSING
- 6.ctl-same-names.sql.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
- 6.ctl-same-names.sql.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
- 6.ctl-same-names.df.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
- 6.ctl-same-names.df.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
- 6.ctl-same-names.native.read: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | no spark twin | ORACLE-MISSING
- 6.ctl-same-names.native.insert: head {"s": null} ; {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | no spark twin | ORACLE-MISSING
- 12.cast-reordered.sql.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 12.cast-reordered.sql.insert: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 12.cast-reordered.df.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 12.cast-reordered.df.insert: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 12.cast-reordered.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 10.int-select.df.read: head {"v": 3} | spark ANSWERED {"v": 3} | EQUAL
- 10.int-select.df.insert: head {"v": 3} | spark ANSWERED {"v": 3} | EQUAL
- 10.int-withColumn.df.read: head {"v": 3} | spark ANSWERED {"v": 3} | EQUAL
- 10.int-withColumn.df.insert: head {"v": 3} | spark ANSWERED {"v": 3} | EQUAL
- 10.int-filter.df.read: head {"v": 3} | spark ANSWERED {"v": 3} | EQUAL
- 10.int-filter.df.insert: head {"v": 3} | spark ANSWERED {"v": 3} | EQUAL
- 10.same-select.df.read: head {"v": 4} | spark ANSWERED {"v": 4} | EQUAL
- 10.same-select.df.insert: head {"v": 4} | spark ANSWERED {"v": 4} | EQUAL
- 10.same-withColumn.df.read: head {"v": 4} | spark ANSWERED {"v": 4} | EQUAL
- 10.same-withColumn.df.insert: head {"v": 4} | spark ANSWERED {"v": 4} | EQUAL
- 10.same-filter.df.read: head {"v": 4} | spark ANSWERED {"v": 4} | EQUAL
- 10.same-filter.df.insert: head {"v": 4} | spark ANSWERED {"v": 4} | EQUAL
- 7.view-create.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 7.view-create.native.insert: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 7.view-replace.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 7.view-replace.native.insert: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | ROWS-DIFFER
- 7.view-intersect.native.read: head (no rows) | no spark twin | ORACLE-MISSING
- 7.view-intersect.native.insert: head (no rows) | no spark twin | ORACLE-MISSING
- 7.view-except.native.read: head {"s": {"a": 1, "b": 2}} | no spark twin | ORACLE-MISSING
- 7.view-except.native.insert: head {"s": {"a": 1, "b": 2}} | no spark twin | ORACLE-MISSING
- 7.view-scalar.native.read: head {"s": {"a": 1, "b": 2}} | no spark twin | ORACLE-MISSING
- 7.view-scalar.native.insert: head {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 7.view-cte.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 7.view-cte.native.insert: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 8.prepare-union.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | RESULT-ROWS-DIFFER
- 8.prepare-param.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 4, "b": 3}} | no spark twin | ORACLE-MISSING
- 9.rec-reordered.native.read: head {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} | spark ANSWERED {"s": {"a": 1, "b": 10}} ; {"s": {"a": 11, "b": 12}} | RESULT-ROWS-DIFFER
- 9.rec-reordered.native.insert: head {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} | spark ANSWERED {"s": {"a": 1, "b": 10}} ; {"s": {"a": 11, "b": 12}} | ROWS-DIFFER
- 9.rec-same.native.read: head {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} | spark ANSWERED {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} | EQUAL
- 9.rec-same.native.insert: head {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} | spark ANSWERED {"s": {"a": 1, "b": 10}} ; {"s": {"a": 12, "b": 11}} | EQUAL
- 13.view-same-name.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
- 13.view-same-name.native.insert: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
- 13.prepare-same-name.native.read: head {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | spark ANSWERED {"s": {"a": 1, "b": 2}} ; {"s": {"a": 3, "b": 4}} | EQUAL
