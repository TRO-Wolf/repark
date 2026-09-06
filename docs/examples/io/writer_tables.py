"""Persist Iceberg tables by name and insert rows by position, both spellings."""

from __future__ import annotations

from repark.errors import AnalysisException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriter.saveAsTable",
    "DataFrameWriter.save_as_table",
    "DataFrameWriter.insertInto",
    "DataFrameWriter.insert_into",
]


def main() -> None:
    repark = ReparkSession.builder.appName("ex26-writer-tables").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
        frame.write.mode("overwrite").saveAsTable("t_ex26")
        saved = repark.sql("SELECT id, name FROM t_ex26 ORDER BY id")
        saved_dtypes = [("id", "int"), ("name", "string")]
        if saved.dtypes != saved_dtypes:
            raise SystemExit(f"saveAsTable dtypes {saved.dtypes!r} != {saved_dtypes!r}")
        saved_rows = [tuple(row) for row in saved.collect()]
        saved_expected = [(1, "a"), (2, "b")]
        if saved_rows != saved_expected:
            raise SystemExit(f"saveAsTable rows {saved_rows!r} != {saved_expected!r}")
        frame.write.mode("overwrite").save_as_table("t_ex26_snake")
        snake_rows = [
            tuple(row)
            for row in repark.sql("SELECT id, name FROM t_ex26_snake ORDER BY id").collect()
        ]
        if snake_rows != saved_expected:
            raise SystemExit(f"save_as_table rows {snake_rows!r} != {saved_expected!r}")
        exists_did_raise = False
        try:
            frame.write.saveAsTable("t_ex26")
        except AnalysisException:
            exists_did_raise = True
        if not exists_did_raise:
            raise SystemExit("saveAsTable on an existing table did not raise AnalysisException")
        frame.write.mode("overwrite").saveAsTable("t_ex26_target")
        other = repark.createDataFrame([(9, "z")], "x INT, y STRING")
        other.write.insertInto("t_ex26_target")
        merged_rows = [
            tuple(row)
            for row in repark.sql("SELECT id, name FROM t_ex26_target ORDER BY id").collect()
        ]
        merged_expected = [(1, "a"), (2, "b"), (9, "z")]
        if merged_rows != merged_expected:
            raise SystemExit(f"insertInto rows {merged_rows!r} != {merged_expected!r}")
        frame.write.mode("overwrite").saveAsTable("t_ex26_snake_target")
        other.write.insert_into("t_ex26_snake_target")
        snake_merged = [
            tuple(row)
            for row in repark.sql("SELECT id, name FROM t_ex26_snake_target ORDER BY id").collect()
        ]
        if snake_merged != merged_expected:
            raise SystemExit(f"insert_into rows {snake_merged!r} != {merged_expected!r}")
        missing_did_raise = False
        try:
            other.write.insertInto("missing_t_ex26")
        except AnalysisException:
            missing_did_raise = True
        if not missing_did_raise:
            raise SystemExit("insertInto on a missing table did not raise AnalysisException")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
