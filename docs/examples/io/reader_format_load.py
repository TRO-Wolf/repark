"""Load CSV, JSON, and Parquet through format and options; read a temp view by name."""

from __future__ import annotations

from pathlib import Path

from repark.errors import AnalysisException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameReader.format",
    "DataFrameReader.load",
    "DataFrameReader.options",
    "DataFrameReader.table",
]


def main() -> None:
    repark = ReparkSession.builder.appName("ex26-reader-load").master("local[1]").getOrCreate()
    try:
        csv_path = Path("ex26_load.csv")
        csv_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
        loaded = repark.read.format("csv").option("header", "true").load(str(csv_path))
        loaded_dtypes = [("id", "string"), ("name", "string")]
        if loaded.dtypes != loaded_dtypes:
            raise SystemExit(f"format csv dtypes {loaded.dtypes!r} != {loaded_dtypes!r}")
        loaded_rows = [tuple(row) for row in loaded.collect()]
        loaded_expected = [("1", "a"), ("2", "b"), ("3", "c")]
        if loaded_rows != loaded_expected:
            raise SystemExit(f"format csv rows {loaded_rows!r} != {loaded_expected!r}")
        plural = repark.read.format("csv").options(header="true").load(str(csv_path))
        plural_rows = [tuple(row) for row in plural.collect()]
        if plural_rows != loaded_expected:
            raise SystemExit(f"options csv rows {plural_rows!r} != {loaded_expected!r}")
        by_path_option = (
            repark.read.format("csv").option("path", str(csv_path)).option("header", "true").load()
        )
        by_path_rows = [tuple(row) for row in by_path_option.collect()]
        if by_path_rows != loaded_expected:
            raise SystemExit(f"path option rows {by_path_rows!r} != {loaded_expected!r}")
        json_path = Path("ex26_load.json")
        json_path.write_text('{"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n', encoding="utf-8")
        parsed = repark.read.format("json").load(str(json_path))
        parsed_rows = [tuple(row) for row in parsed.collect()]
        parsed_expected = [(1, "a"), (2, "b")]
        if parsed_rows != parsed_expected:
            raise SystemExit(f"format json rows {parsed_rows!r} != {parsed_expected!r}")
        seed = repark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
        parquet_path = Path("ex26_load_pq")
        seed.write.mode("overwrite").parquet(str(parquet_path))
        restored = repark.read.format("parquet").load(str(parquet_path))
        restored_rows = sorted(tuple(row) for row in restored.collect())
        if restored_rows != parsed_expected:
            raise SystemExit(f"format parquet rows {restored_rows!r} != {parsed_expected!r}")
        seed.createOrReplaceTempView("tv_ex26")
        named = repark.read.table("tv_ex26")
        named_rows = sorted(tuple(row) for row in named.collect())
        if named_rows != parsed_expected:
            raise SystemExit(f"table rows {named_rows!r} != {parsed_expected!r}")
        did_raise = False
        try:
            repark.read.table("missing_tv_ex26").collect()
        except AnalysisException:
            did_raise = True
        if not did_raise:
            raise SystemExit("table on a missing name did not raise AnalysisException")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
