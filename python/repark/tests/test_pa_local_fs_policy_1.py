from __future__ import annotations

import os
from pathlib import Path

import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException

_ALLOW_KEY = "repark.sql.allowLocalFilesystemDDL"


def _files(root: Path) -> list[Path]:
    return [path for path in root.rglob("*") if path.is_file()] if root.exists() else []


def _copy(location: str) -> str:
    return f"COPY (SELECT 1 AS a) TO '{location}' STORED AS PARQUET"


@pytest.fixture
def dirs(tmp_path: Path) -> dict[str, Path]:
    warehouse = tmp_path / "warehouse"
    outside = tmp_path / "outside"
    warehouse.mkdir()
    outside.mkdir()
    (warehouse / "link").symlink_to(outside)
    return {"warehouse": warehouse, "outside": outside}


@pytest.fixture
def spark(dirs: dict[str, Path]) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("test_pa_local_fs_policy_1")
        .config("repark.memory.limit.gb", "1")
        .getOrCreate()
    )
    session.register_memory_catalog("wh", str(dirs["warehouse"]))
    yield session
    session.stop()


@pytest.fixture
def opened(dirs: dict[str, Path]) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("test_pa_local_fs_policy_1_open")
        .config("repark.memory.limit.gb", "1")
        .config(_ALLOW_KEY, "true")
        .getOrCreate()
    )
    session.register_memory_catalog("wh", str(dirs["warehouse"]))
    yield session
    session.stop()


def _refuses(run, sql: str) -> None:
    with pytest.raises(AnalysisException, match=_ALLOW_KEY):
        run(sql).collect()


def test_pa02_explain_analyze_copy_outside_refuses_and_writes_nothing(spark, dirs) -> None:
    _refuses(spark.sql, f"EXPLAIN ANALYZE {_copy(dirs['outside'] / 'wrapped')}")
    _refuses(spark.sql, _copy(dirs["outside"] / "direct"))
    assert _files(dirs["outside"]) == []


def test_pa02_plain_explain_still_answers_without_writing(spark, dirs) -> None:
    rows = spark.sql(f"EXPLAIN {_copy(dirs['outside'] / 'explained')}").collect()
    assert rows
    assert _files(dirs["outside"]) == []


def test_pa02_warehouse_control_still_writes(spark, dirs) -> None:
    inside = dirs["warehouse"] / "wrapped_inside"
    spark.sql(f"EXPLAIN ANALYZE {_copy(inside)}").collect()
    assert _files(inside)


def test_pa02_opt_in_control_still_writes(opened, dirs) -> None:
    wrapped = dirs["outside"] / "wrapped_open"
    opened.sql(f"EXPLAIN ANALYZE {_copy(wrapped)}").collect()
    assert _files(wrapped)


@pytest.mark.parametrize("tail", ["link/new/export", "link/m1/m2/m3/export"])
def test_pa03_missing_parents_through_a_warehouse_symlink_refuse(spark, dirs, tail) -> None:
    _refuses(spark.sql, _copy(dirs["warehouse"] / tail))
    assert _files(dirs["outside"]) == []


def test_pa03_warehouse_control_still_writes(spark, dirs) -> None:
    real = dirs["warehouse"] / "real"
    real.mkdir()
    (dirs["warehouse"] / "inner").symlink_to(real)
    spark.sql(_copy(dirs["warehouse"] / "inner/m1/m2/m3/export")).collect()
    assert _files(real / "m1/m2/m3/export")


def test_pa03_opt_in_control_still_writes(opened, dirs) -> None:
    opened.sql(_copy(dirs["warehouse"] / "link/m1/m2/m3/export")).collect()
    assert _files(dirs["outside"] / "m1/m2/m3/export")


@pytest.mark.parametrize("location", ["aaaaé", "aaaaaaé", "file://aaaaaaaaé"])
def test_pa04_non_ascii_relative_locations_refuse_with_the_policy_error(
    spark, tmp_path, location
) -> None:
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        _refuses(spark.sql, _copy(location))
    finally:
        os.chdir(cwd)
    assert not (tmp_path / location.removeprefix("file://")).exists()


def test_pa04_file_localhost_forms(spark, dirs) -> None:
    _refuses(spark.sql, _copy(f"file://localhost{dirs['outside']}/aaé"))
    assert _files(dirs["outside"]) == []
    inside = dirs["warehouse"] / "aaé"
    spark.sql(_copy(f"file://localhost{inside}")).collect()
    assert _files(inside)


def test_pa04_opt_in_control_still_writes(opened, dirs) -> None:
    opened_dest = dirs["outside"] / "aaaaaaé"
    opened.sql(_copy(f"FILE://LOCALHOST{opened_dest}")).collect()
    assert _files(opened_dest)


def test_repark_sql_refuses_executing_writes_outside(dirs, tmp_path) -> None:
    _refuses(repark.sql, _copy(dirs["outside"] / "native_direct"))
    _refuses(repark.sql, f"EXPLAIN ANALYZE {_copy(dirs['outside'] / 'native_wrapped')}")
    _refuses(repark.sql, _copy(dirs["warehouse"] / "link/m1/m2/m3/export"))
    _refuses(
        repark.sql,
        f"CREATE EXTERNAL TABLE pa_native_ext (a INT) STORED AS PARQUET "
        f"LOCATION '{dirs['outside']}/ext/'",
    )
    assert _files(dirs["outside"]) == []
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        _refuses(repark.sql, _copy("aaaaaaé"))
    finally:
        os.chdir(cwd)
    assert not (tmp_path / "aaaaaaé").exists()


def test_repark_sql_plain_explain_still_answers(dirs) -> None:
    assert repark.sql(f"EXPLAIN {_copy(dirs['outside'] / 'native_explained')}").collect()
    assert _files(dirs["outside"]) == []
