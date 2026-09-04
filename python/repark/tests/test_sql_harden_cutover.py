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
    write_bronze_parquet,
)
from _sql_harden_cutover_run import as_golden, run_program, sql_arrow
from test_v3_live_oracle import _ALLOW_CREATE_V3_KEY, _LIVE, _LIVE_SKIP, _v37_iceberg_runtime_jar


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


def repark_outcome(program: _Program, warehouse: Path) -> dict[str, Any]:
    """Run one inventory program end to end on the repark facade."""
    from repark import ReparkSession, Window, functions
    from repark.spark import types

    parquet = warehouse / "bronze.parquet"
    write_bronze_parquet(parquet)
    repark = (
        ReparkSession.builder.appName("sql-harden-1")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
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
