"""SQL-HARDEN-1 cutover shapes S1-S7 measured against live Spark.

pins: sql-harden-1-cutover-shapes/C-001, C-003, C-004
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _sql_harden_cutover_golden import REGISTRY, REPARK, SPARK, VERDICTS
from _sql_harden_cutover_programs import (
    _CATALOG,
    _NAMESPACE,
    _PROGRAMS,
    _Program,
    mor_properties,
    write_bronze_parquet,
)
from _sql_harden_cutover_run import (
    as_golden,
    delete_file_count,
    delete_file_kinds,
    make_names,
    program_sql_texts,
    run_program,
    sql_arrow,
)
from test_v3_live_oracle import _ALLOW_CREATE_V3_KEY, _LIVE, _LIVE_SKIP, _v37_iceberg_runtime_jar

_MERGE_PROGRAMS = tuple(program for program in _PROGRAMS if program.runner == "merge")
_SPARK_DELETE_FILE_FLOOR = 2
_WRITE_CONCURRENCY_KEY = "repark.write.max-concurrent-files"


def _agrees(left: Any, right: Any) -> bool:
    """Two engine cells agree when both refuse, or both answer the same value."""
    if left[0] != right[0]:
        return False
    return left[0] == "ERROR" or left[1] == right[1]


def _verdict(repark: dict[str, Any], spark: dict[str, Any]) -> str:
    """EQUAL, REFUSED (both engines refuse the statement) or DIVERGES for one program."""
    pairs = [
        *zip(repark["statements"], spark["statements"], strict=True),
        *zip(repark["probes"], spark["probes"], strict=True),
    ]
    if any(not _agrees(left, right) for left, right in pairs):
        return "DIVERGES"
    refused = any(cell[0] == "ERROR" for cell in repark["statements"])
    return "REFUSED" if refused else "EQUAL"


def repark_outcome(
    program: _Program,
    warehouse: Path,
    *,
    extra_config: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run one inventory program end to end on the repark facade."""
    from repark import ReparkSession, Window, functions
    from repark.spark import types

    warehouse.mkdir(parents=True, exist_ok=True)
    parquet = warehouse / "bronze.parquet"
    write_bronze_parquet(parquet)
    builder = ReparkSession.builder.appName("sql-harden-1").config(_ALLOW_CREATE_V3_KEY, "true")
    if extra_config is not None:
        for key, value in extra_config.items():
            builder = builder.config(key, value)
    repark = builder.getOrCreate()
    try:
        repark.register_memory_catalog("ice", warehouse)
        sql_arrow(repark, f"CREATE NAMESPACE ice.{_NAMESPACE} LOCATION '{warehouse / _NAMESPACE}'")
        return run_program(
            program,
            repark,
            warehouse,
            catalog="ice",
            functions=functions,
            types=types,
            window=Window,
            qualified_call=True,
            parquet=parquet,
        )
    finally:
        repark.stop()


def _live_session(warehouse: Path) -> tuple[Any, bool]:
    """Reuse the collection's live session when one is alive; otherwise build one."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    owned = session is None
    if owned:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("sql-harden-1-live")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        )
        jar = _v37_iceberg_runtime_jar()
        builder = (
            builder.config("spark.jars", jar)
            if jar is not None
            else builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        )
        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
    session.conf.set(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
    session.conf.set(f"spark.sql.catalog.{_CATALOG}.warehouse", str(warehouse))
    return session, owned


def spark_outcome(
    program: _Program, session: Any, warehouse: Path, parquet: Path
) -> dict[str, Any]:
    """Run one inventory program end to end on the live Spark oracle."""
    from pyspark.sql import Window, functions, types

    return run_program(
        program,
        session,
        warehouse,
        catalog=_CATALOG,
        functions=functions,
        types=types,
        window=Window,
        qualified_call=False,
        parquet=parquet,
    )


@pytest.fixture(scope="module")
def coverage_session() -> Iterator[Any]:
    """One live Spark session for the whole matrix, stopped only when this module created it."""
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    warehouse = Path(tempfile.mkdtemp(prefix="repark-sqlh1-live-"))
    parquet = warehouse / "bronze.parquet"
    write_bronze_parquet(parquet)
    session, owned = _live_session(warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
    try:
        yield session, warehouse, parquet
    finally:
        if owned:
            session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


@pytest.mark.parametrize("program", _PROGRAMS, ids=[program.name for program in _PROGRAMS])
def test_sql_harden_row_reproduces_the_measured_repark_answer(
    program: _Program, tmp_path: Path
) -> None:
    """Every inventory row still answers on repark exactly what this unit measured."""
    assert as_golden(repark_outcome(program, tmp_path)) == REPARK[program.name]


@pytest.mark.parametrize("program", _PROGRAMS, ids=[program.name for program in _PROGRAMS])
def test_sql_harden_row_matches_the_live_spark_oracle(
    program: _Program, coverage_session: Any
) -> None:
    """The same row on live Spark reproduces its measured half and keeps its measured verdict."""
    session, warehouse, parquet = coverage_session
    spark = as_golden(spark_outcome(program, session, warehouse, parquet))
    assert spark == SPARK[program.name]
    assert _verdict(REPARK[program.name], spark) == VERDICTS[program.name]


def test_sql_harden_verdicts_match_the_committed_halves() -> None:
    """Each committed verdict is the join of the two measured golden halves."""
    for program in _PROGRAMS:
        assert _verdict(REPARK[program.name], SPARK[program.name]) == VERDICTS[program.name]


def test_sql_harden_inventory_carries_every_program_once() -> None:
    """The golden, the verdict table and the program list are the same set of rows."""
    names = [program.name for program in _PROGRAMS]
    assert len(names) == len(set(names))
    assert set(names) == set(VERDICTS) == set(REPARK) == set(SPARK) == set(REGISTRY)


def test_every_diverging_row_names_a_registry_row_that_exists() -> None:
    """A DIVERGES verdict cites a registry heading that is in the parity document."""
    from pathlib import Path

    registry = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"
    text = registry.read_text(encoding="utf-8")
    for name, verdict in VERDICTS.items():
        if verdict != "DIVERGES":
            continue
        row = REGISTRY[name]
        assert row != "—", name
        assert f"{row} —" in text, (name, row)


def test_rendered_sql_uses_only_the_passed_namespace() -> None:
    """No program SQL interpolates a namespace other than the one passed to make_names."""
    catalog = "catx"
    namespace = "ns_pin_only"
    stem = "stemx"
    names = make_names(catalog, stem, True, namespace)
    token = f".{_NAMESPACE}."
    passed = f".{namespace}."
    for program in _PROGRAMS:
        blob = "\n".join(program_sql_texts(program, names, mor_properties(program.format_version)))
        assert token not in blob, (program.name, blob[:200])
        if program.runner != "dedup":
            assert passed in blob, program.name


def test_to_date_and_cast_as_date_answer_on_repark() -> None:
    """CUTOVER-DATE-1 control: to_date(ts) and CAST(ts AS DATE) work on repark."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("sqlh1-date-ok").getOrCreate()
    try:
        to_date = spark.sql("SELECT to_date(TIMESTAMP '2026-01-01 10:15:00') AS d").to_arrow()
        casted = spark.sql("SELECT CAST(TIMESTAMP '2026-01-01 10:15:00' AS DATE) AS d").to_arrow()
    finally:
        spark.stop()
    assert str(to_date.to_pylist()[0]["d"]) == "2026-01-01"
    assert str(casted.to_pylist()[0]["d"]) == "2026-01-01"


def test_date_and_unix_timestamp_functions_answer_on_repark() -> None:
    """CUTOVER-DATE-1 FIXED by DATE-FN-1: date(ts) and unix_timestamp(ts) both answer."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("sqlh1-date-answers").getOrCreate()
    try:
        dated = spark.sql("SELECT date(TIMESTAMP '2026-01-01 10:15:00') AS d").to_arrow()
        assert str(dated.to_pylist()[0]["d"]) == "2026-01-01"
        epoch = spark.sql("SELECT unix_timestamp(TIMESTAMP '2026-01-01 10:15:00') AS e").to_arrow()
        assert epoch.to_pylist()[0]["e"] == 1767262500
    finally:
        spark.stop()


@pytest.mark.parametrize(
    "program", _MERGE_PROGRAMS, ids=[program.name for program in _MERGE_PROGRAMS]
)
def test_merge_delete_file_count_meets_spark_floor(program: _Program, tmp_path: Path) -> None:
    """CUTOVER-MERGE-FILES-1: kinds stay in the golden; the count is at least Spark's 2."""
    outcome = repark_outcome(program, tmp_path)
    expected_kind = "PUFFIN" if program.format_version == 3 else "PARQUET"
    assert as_golden(outcome) == REPARK[program.name]
    assert delete_file_kinds(outcome) == [expected_kind]
    assert delete_file_count(outcome) >= _SPARK_DELETE_FILE_FLOOR


@pytest.mark.parametrize(
    "program", _MERGE_PROGRAMS, ids=[program.name for program in _MERGE_PROGRAMS]
)
def test_merge_delete_file_count_moves_with_write_concurrency(
    program: _Program, tmp_path: Path
) -> None:
    """Write-concurrency / shuffle cap moves the delete-file count; kinds golden stays green."""
    default_outcome = repark_outcome(program, tmp_path / "default")
    capped_outcome = repark_outcome(
        program,
        tmp_path / "capped",
        extra_config={
            _WRITE_CONCURRENCY_KEY: "1",
            "spark.sql.shuffle.partitions": "1",
        },
    )
    expected_kind = "PUFFIN" if program.format_version == 3 else "PARQUET"
    assert as_golden(default_outcome) == REPARK[program.name]
    assert as_golden(capped_outcome) == REPARK[program.name]
    assert delete_file_kinds(default_outcome) == [expected_kind]
    assert delete_file_kinds(capped_outcome) == [expected_kind]
    default_count = delete_file_count(default_outcome)
    capped_count = delete_file_count(capped_outcome)
    assert default_count >= _SPARK_DELETE_FILE_FLOOR
    assert capped_count >= _SPARK_DELETE_FILE_FLOOR
    assert capped_count <= default_count
    if default_count > _SPARK_DELETE_FILE_FLOOR:
        assert capped_count < default_count
