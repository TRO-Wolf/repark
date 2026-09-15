"""DF-PLAN-INTROSPECT-1 — Rust plan-introspection pins driven by the live oracle."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812

_ORACLE = json.loads((Path(__file__).parent / "facade_dataframe_surface_oracle.json").read_text())


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE["cells"][name]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-df-plan-introspect-1").getOrCreate()
    yield session
    session.stop()


def _kv(spark: ReparkSession):
    """Build the oracle's two-row key/a/b frame."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def _write_parquet(spark: ReparkSession, path: str):
    """Write the kv frame to path as parquet."""
    _kv(spark).coalesce(1).write.parquet(path)


def test_inputfiles_parquet_lists_file_uri(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_parquet."""
    assert _cell("inputFiles_parquet")["result"]["kind"] == "list"
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    files = spark.read.parquet(target).inputFiles()
    assert isinstance(files, list)
    assert len(files) == 1
    assert all(isinstance(item, str) for item in files)
    assert files[0].startswith("file://")
    assert files[0].endswith(".parquet")
    assert Path(files[0].removeprefix("file://")).exists()


def test_inputfiles_local_frame_is_empty(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_local."""
    assert _kv(spark).inputFiles() == _cell("inputFiles_local")["result"]["items"]


def test_inputfiles_union_dedupes(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_union."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target)
    assert len(frame.union(frame).inputFiles()) == _cell("inputFiles_union")["result"]["value"]


def test_inputfiles_filtered_scan_keeps_file(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cell inputFiles_csv_filter."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target).filter("a > 100")
    assert len(frame.inputFiles()) == _cell("inputFiles_csv_filter")["result"]["value"]


def test_inputfiles_path_with_comma_and_brackets(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — comma/bracket path stays one URI."""
    target = str(tmp_path / "if, a")
    _write_parquet(spark, target)
    written = list(Path(target).iterdir())
    assert len(written) == 1
    renamed = Path(target) / "data [0], x.parquet"
    written[0].rename(renamed)
    files = spark.read.parquet(target).inputFiles()
    assert len(files) == 1
    assert ", " in files[0]
    assert "[" in files[0] and "]" in files[0]
    assert files[0].startswith("file://")
    assert Path(files[0].removeprefix("file://")).exists()


def test_inputfiles_join_of_overlapping_scans(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — join lists both sides, no explain crash."""
    left_path = str(tmp_path / "left")
    right_path = str(tmp_path / "right")
    _write_parquet(spark, left_path)
    _write_parquet(spark, right_path)
    left = spark.read.parquet(left_path)
    right = spark.read.parquet(right_path)
    joined = left.join(right, "key").inputFiles()
    assert sorted(joined) == sorted(set(left.inputFiles()) | set(right.inputFiles()))
    assert len(joined) == len(left.inputFiles()) + len(right.inputFiles())


def test_inputfiles_csv_and_json(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — csv and json scans list their files."""
    csv_file = tmp_path / "csv_single" / "part.csv"
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.write_text("key,a,b\nx,1,2\ny,3,4\n")
    csv_files = spark.read.csv(str(csv_file)).inputFiles()
    assert len(csv_files) == 1
    assert csv_files[0].startswith("file://")
    json_file = tmp_path / "json_single" / "part.json"
    json_file.parent.mkdir(parents=True, exist_ok=True)
    json_file.write_text('{"key": "x", "a": 1}\n')
    json_files = spark.read.json(str(json_file)).inputFiles()
    assert len(json_files) == 1
    assert json_files[0].startswith("file://")


def test_inputfiles_glob_lists_matches(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — a glob read lists every match."""
    first = str(tmp_path / "g1")
    second = str(tmp_path / "g2")
    _write_parquet(spark, first)
    _write_parquet(spark, second)
    first_files = spark.read.parquet(first).inputFiles()
    second_files = spark.read.parquet(second).inputFiles()
    assert first_files and second_files
    assert not set(first_files) & set(second_files)
    globbed = spark.read.parquet(str(tmp_path / "g*")).inputFiles()
    assert sorted(globbed) == sorted(first_files + second_files)


def test_inputfiles_cached_frame_keeps_source_files(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — cached frame still lists source files."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    frame = spark.read.parquet(target)
    before = frame.inputFiles()
    frame.cache()
    assert frame.collect()
    assert frame.inputFiles() == before


def test_inputfiles_sql_door_matches_python_door(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-001 — spark.sql frame lists the same files."""
    target = str(tmp_path / "pq")
    _write_parquet(spark, target)
    direct = spark.read.parquet(target)
    direct.createOrReplaceTempView("plan_intro_v")
    try:
        via_sql = spark.sql("SELECT * FROM plan_intro_v")
        assert via_sql.inputFiles() == direct.inputFiles()
    finally:
        spark.catalog.dropTempView("plan_intro_v")


def test_semantichash_answers_int(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_type."""
    value = _kv(spark).semanticHash()
    assert type(value).__name__ == _cell("semanticHash_type")["result"]["value"]
    assert -(2**31) <= value <= 2**31 - 1


def test_semantichash_equal_plans(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_equal_plans."""
    left = spark.range(3).filter("id > 1").semanticHash()
    right = spark.range(3).filter("id > 1").semanticHash()
    assert (left == right) == _cell("semanticHash_equal_plans")["result"]["value"]


def test_semantichash_alias_equal(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_alias_equal."""
    left = spark.range(3).select(F.col("id").alias("a")).semanticHash()
    right = spark.range(3).select(F.col("id").alias("b")).semanticHash()
    assert (left == right) == _cell("semanticHash_alias_equal")["result"]["value"]


def test_semantichash_diff(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_diff."""
    assert (spark.range(3).semanticHash() == spark.range(4).semanticHash()) == _cell(
        "semanticHash_diff"
    )["result"]["value"]


def test_semantichash_filter_order(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — cell semanticHash_filter_order."""
    first = spark.range(3).filter("id > 1").filter("id < 3").semanticHash()
    second = spark.range(3).filter("id < 3").filter("id > 1").semanticHash()
    assert (first == second) == _cell("semanticHash_filter_order")["result"]["value"]


def test_semantichash_literals_columns_limits_casts_paths(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — literals, order, limit, cast, path arms."""
    assert (
        spark.range(5).filter("id > 1").semanticHash()
        != spark.range(5).filter("id > 2").semanticHash()
    )
    assert (
        _kv(spark).select("key", "a").semanticHash() != _kv(spark).select("a", "key").semanticHash()
    )
    assert _kv(spark).limit(5).semanticHash() != _kv(spark).limit(10).semanticHash()
    assert (
        _kv(spark).select(F.col("a").cast("int").alias("v")).semanticHash()
        != _kv(spark).select(F.col("a").cast("bigint").alias("v")).semanticHash()
    )


def test_semantichash_file_paths_differ(spark: ReparkSession, tmp_path: Path) -> None:
    """pins: df-plan-introspect-1/C-002 — two file paths hash differently."""
    first = str(tmp_path / "h1")
    second = str(tmp_path / "h2")
    _write_parquet(spark, first)
    _write_parquet(spark, second)
    assert spark.read.parquet(first).semanticHash() != spark.read.parquet(second).semanticHash()


def test_semantichash_ignores_batch_size(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — batch_size never reaches the hash."""
    before = spark.range(3).filter("id > 1").semanticHash()
    spark.conf.set("datafusion.execution.batch_size", "1024")
    try:
        assert spark.range(3).filter("id > 1").semanticHash() == before
    finally:
        spark.conf.set("datafusion.execution.batch_size", "65536")


def test_semantichash_stable_and_sql_door(spark: ReparkSession) -> None:
    """pins: df-plan-introspect-1/C-002 — stable in process, equal over spark.sql."""
    frame = spark.range(3).filter("id > 1")
    assert frame.semanticHash() == frame.semanticHash()
    assert spark.sql("SELECT 1 AS a").semanticHash() == spark.sql("SELECT 1 AS b").semanticHash()
