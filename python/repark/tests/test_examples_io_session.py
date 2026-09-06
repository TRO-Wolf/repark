"""Divergence pins for the EX-26 reader/writer/session batch.

Registry §7 rows EX-IO-1..10 and EX-SES-6.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkValueError, UnsupportedOperationException
from repark.spark.functions import UserDefinedFunction


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex26-io-session").getOrCreate()
    yield session
    session.stop()


def test_bare_load_refuses(spark: ReparkSession, tmp_path: Path) -> None:
    """load() with no format raises; Spark defaults to parquet (EX-IO-1)."""
    seed = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    parquet_dir = tmp_path / "seed.parquet"
    seed.write.mode("overwrite").parquet(str(parquet_dir))
    with pytest.raises(AnalysisException, match="empty"):
        spark.read.option("path", str(parquet_dir)).load()


def test_schema_on_parquet_refuses(spark: ReparkSession, tmp_path: Path) -> None:
    """schema() on a parquet read refuses; Spark projects it (EX-IO-2)."""
    seed = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    parquet_dir = tmp_path / "seed.parquet"
    seed.write.mode("overwrite").parquet(str(parquet_dir))
    with pytest.raises(AnalysisException, match="not applied on parquet"):
        spark.read.schema("name STRING").parquet(str(parquet_dir))


def test_csv_infer_schema_width(spark: ReparkSession, tmp_path: Path) -> None:
    """csv inferSchema widens ints to bigint; Spark answers int, rows agree (EX-IO-3)."""
    csv_path = tmp_path / "letters.csv"
    csv_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
    frame = spark.read.csv(str(csv_path), header=True, inferSchema=True)
    assert frame.dtypes == [("id", "bigint"), ("name", "string")]
    assert [tuple(row) for row in frame.collect()] == [(1, "a"), (2, "b"), (3, "c")]


def test_csv_header_default_true(spark: ReparkSession, tmp_path: Path) -> None:
    """csv writes carry a header by default; Spark writes none (EX-IO-4)."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    headed = tmp_path / "default_csv"
    frame.write.mode("overwrite").csv(str(headed))
    assert read_data_bytes(headed, ".csv") == ["id,name\n1,a\n2,b\n"]
    shaped = tmp_path / "default_save"
    frame.write.mode("overwrite").format("csv").save(str(shaped))
    assert read_data_bytes(shaped, ".csv") == ["id,name\n1,a\n2,b\n"]


def read_data_bytes(root: Path, suffix: str) -> list[str]:
    """Return the sorted data-file bytes under a writer output directory."""
    return sorted(
        path.read_text(encoding="utf-8") for path in root.rglob(f"*{suffix}") if path.is_file()
    )


def test_save_default_format_refuses(spark: ReparkSession, tmp_path: Path) -> None:
    """save() with no format raises; Spark writes parquet (EX-IO-5)."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    with pytest.raises(AnalysisException, match="requires format"):
        frame.write.mode("overwrite").save(str(tmp_path / "out"))


def test_saveas_table_non_iceberg_refuses(spark: ReparkSession) -> None:
    """saveAsTable outside iceberg refuses; Spark serves the format (EX-IO-6)."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    with pytest.raises(PySparkValueError, match="only format"):
        frame.write.mode("overwrite").format("csv").saveAsTable("t_ex26_fmt")


def test_excel_reader_refuses(spark: ReparkSession) -> None:
    """The excel readers refuse; the engine connector is deferred (EX-IO-7)."""
    with pytest.raises(UnsupportedOperationException, match="not available in this build"):
        spark.read.excel("never-opened.xlsx")
    with pytest.raises(UnsupportedOperationException, match="not available in this build"):
        spark.read_excel("never-opened.xlsx")


def test_excel_sheet_names_refuses(spark: ReparkSession) -> None:
    """Sheet-name discovery refuses; the engine connector is deferred (EX-IO-7)."""
    with pytest.raises(UnsupportedOperationException, match="not available in this build"):
        spark.read.sheet_names("never-opened.xlsx")
    with pytest.raises(UnsupportedOperationException, match="not available in this build"):
        spark.excel_sheet_names("never-opened.xlsx")


def test_missing_table_text(spark: ReparkSession) -> None:
    """Missing-table reads share the type with Spark but not the text (EX-IO-8)."""
    with pytest.raises(AnalysisException, match="Error during planning"):
        spark.read.table("missing_t_ex26").collect()
    with pytest.raises(AnalysisException, match="Error during planning"):
        spark.table("missing_t_ex26").collect()
    other = spark.createDataFrame([(9, "z")], "x INT, y STRING")
    with pytest.raises(AnalysisException, match="Error during planning"):
        other.write.insertInto("missing_t_ex26")


def test_insert_arity_text(spark: ReparkSession) -> None:
    """insertInto with the wrong width shares the type with Spark, not the text (EX-IO-8)."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    frame.write.mode("overwrite").saveAsTable("t_ex26_arity")
    wide = spark.createDataFrame([(9, "z", 1)], "x INT, y STRING, z INT")
    with pytest.raises(AnalysisException, match="Column count doesn't match"):
        wide.write.insertInto("t_ex26_arity")
    narrow = spark.createDataFrame([(9,)], "x INT")
    with pytest.raises(AnalysisException, match="Column count doesn't match"):
        narrow.write.insertInto("t_ex26_arity")


def test_writer_output_listing(spark: ReparkSession, tmp_path: Path) -> None:
    """csv/json writes emit one data file with no _SUCCESS marker (EX-IO-10)."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    csv_out = tmp_path / "listed_csv"
    frame.write.mode("overwrite").csv(str(csv_out), header=True)
    csv_names = sorted(path.name for path in csv_out.iterdir() if path.is_file())
    assert len(csv_names) == 1 and csv_names[0].endswith(".csv")
    assert "_SUCCESS" not in csv_names
    json_out = tmp_path / "listed_json"
    frame.write.mode("overwrite").json(str(json_out))
    json_names = sorted(path.name for path in json_out.iterdir() if path.is_file())
    assert len(json_names) == 1 and json_names[0].endswith(".json")
    part_out = tmp_path / "listed_part"
    frame.write.mode("overwrite").partitionBy("name").option("header", "false").csv(str(part_out))
    assert sorted(path.name for path in part_out.iterdir() if path.is_dir()) == [
        "name=a",
        "name=b",
    ]
    assert [path.name for path in part_out.iterdir() if path.is_file()] == []
    for leaf in ("name=a", "name=b"):
        leaf_names = sorted(path.name for path in (part_out / leaf).iterdir() if path.is_file())
        assert len(leaf_names) == 1 and leaf_names[0].endswith(".csv")


def test_saveas_table_exists_text(spark: ReparkSession) -> None:
    """saveAsTable on an existing table shares the type with Spark, not the text (EX-IO-9)."""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
    frame.write.mode("overwrite").saveAsTable("t_ex26_exists")
    with pytest.raises(AnalysisException, match="already exists"):
        frame.write.saveAsTable("t_ex26_exists")


def prefix_for_pin(value: object) -> str:
    """Prefix one value for the register-return pin below."""
    return f"u{value}"


def test_udf_register_returns_udf_object(spark: ReparkSession) -> None:
    """udf.register answers the UDF object; Spark answers a plain function (EX-SES-6)."""
    registered = spark.udf.register("fn_ex26_pin", prefix_for_pin)
    assert isinstance(registered, UserDefinedFunction)
    assert [tuple(row) for row in spark.sql("SELECT fn_ex26_pin(4)").collect()] == [("u4",)]
