from __future__ import annotations

import os
from pathlib import Path

import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException

_ALLOW_KEY = "repark.sql.allowLocalFilesystemDDL"
_NATIVE_REFUSAL = "repark.sql\\(\\) does not allow local-file DDL"


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


def _native_refuses(sql: str) -> None:
    with pytest.raises(AnalysisException, match=_NATIVE_REFUSAL) as raised:
        repark.sql(sql).collect()
    assert _ALLOW_KEY not in str(raised.value)


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
    _native_refuses(_copy(dirs["outside"] / "native_direct"))
    _native_refuses(f"EXPLAIN ANALYZE {_copy(dirs['outside'] / 'native_wrapped')}")
    _native_refuses(_copy(dirs["warehouse"] / "link/m1/m2/m3/export"))
    _native_refuses(
        f"CREATE EXTERNAL TABLE pa_native_ext (a INT) STORED AS PARQUET "
        f"LOCATION '{dirs['outside']}/ext/'",
    )
    assert _files(dirs["outside"]) == []
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        _native_refuses(_copy("aaaaaaé"))
    finally:
        os.chdir(cwd)
    assert not (tmp_path / "aaaaaaé").exists()


def test_repark_sql_plain_explain_still_answers(dirs) -> None:
    assert repark.sql(f"EXPLAIN {_copy(dirs['outside'] / 'native_explained')}").collect()
    assert _files(dirs["outside"]) == []


def _deep(dirs: dict[str, Path]) -> Path:
    real = dirs["warehouse"] / "a" / "b"
    real.mkdir(parents=True)
    (dirs["warehouse"] / "deep").symlink_to(real)
    return dirs["warehouse"] / "deep"


def test_v01_symlink_then_dot_dot_writes_outside_on_todays_head_and_now_refuses(
    spark, dirs
) -> None:
    deep = _deep(dirs)
    _refuses(spark.sql, _copy(f"{deep}/../../outside/bare"))
    _refuses(spark.sql, _copy(f"file://{deep}/../../outside/url"))
    _refuses(spark.sql, f"EXPLAIN ANALYZE {_copy(f'{deep}/../../outside/ea')}")
    assert _files(dirs["outside"]) == []


@pytest.mark.parametrize(
    "form",
    [
        "file://{wh}/%2e%2e/outside/p",
        "file://{wh}/%2E%2E/outside/p",
        "file://{wh}/.%2e/outside/p",
        "file:{wh}/%2e%2e/outside/p",
        "file://localhost{wh}/%2e%2e/outside/p",
    ],
)
def test_v02_percent_encoded_dot_segments_write_outside_on_todays_head_and_now_refuse(
    spark, dirs, form
) -> None:
    _refuses(spark.sql, _copy(form.format(wh=dirs["warehouse"])))
    assert _files(dirs["outside"]) == []


@pytest.mark.parametrize("prefix", ["file:", "file:./"])
def test_v03_file_relative_with_the_cwd_in_a_warehouse_writes_outside_on_todays_head(
    spark, dirs, prefix
) -> None:
    relative = str(dirs["outside"]).lstrip("/")
    cwd = Path.cwd()
    os.chdir(dirs["warehouse"])
    try:
        _refuses(spark.sql, _copy(f"{prefix}{relative}/fc"))
    finally:
        os.chdir(cwd)
    assert Path.cwd() == cwd
    assert _files(dirs["outside"]) == []


def test_a_warehouse_registered_through_a_symlink_still_writes_inside_it(tmp_path) -> None:
    real = tmp_path / "real"
    (real / "r2").mkdir(parents=True)
    linked = tmp_path / "whl"
    linked.symlink_to(real)
    (real / "inner").symlink_to(real / "r2")
    session = (
        ReparkSession.builder.appName("test_pa_local_fs_policy_1_linked_root")
        .config("repark.memory.limit.gb", "1")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("wh", str(linked))
        for location, landed in [
            (linked / "direct", real / "direct"),
            (linked / "inner/x", real / "r2/x"),
            (linked / "m1/m2/m3/x", real / "m1/m2/m3/x"),
        ]:
            session.sql(_copy(location)).collect()
            assert _files(landed), location
    finally:
        session.stop()


def test_repark_sql_names_no_opt_in_and_no_route_opens_it(spark, dirs) -> None:
    from repark import _native

    with pytest.raises(Exception, match="repark"):
        repark.sql(f"SET {_ALLOW_KEY} = true").collect()
    opened = _native.PyReparkSession.native(config={_ALLOW_KEY: "true"})
    with pytest.raises(Exception, match=_NATIVE_REFUSAL):
        opened.sql(_copy(dirs["outside"] / "configured"))
    assert _files(dirs["outside"]) == []
    existing = dirs["warehouse"] / "existing"
    spark.sql(_copy(existing)).collect()
    assert _files(existing)
    _native_refuses(
        f"CREATE EXTERNAL TABLE pa_native_read STORED AS PARQUET LOCATION '{existing}/'"
    )


def test_repark_sql_keeps_the_q6_wall_type_under_the_guard(dirs) -> None:
    table = repark.sql("SELECT CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP) AS v").to_arrow()
    assert str(table.schema.field("v").type) == "timestamp[us]"
    assert table.column("v").cast("int64").to_pylist() == [1_704_164_645_123_456]
    _native_refuses(_copy(dirs["outside"] / "q6_guard"))
    assert _files(dirs["outside"]) == []
