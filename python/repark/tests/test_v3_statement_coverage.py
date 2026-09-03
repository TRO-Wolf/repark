"""V3-COV — every served statement class and CALL procedure on a v3 table, repark against Spark.

pins: v3-cov-statement-coverage/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import json
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _v3_statement_coverage_golden import REPARK, SPARK, VERDICTS
from _v3_statement_coverage_programs import (
    _CATALOG,
    _NAMESPACE,
    _PART_SCHEMA,
    _PART_VALUES,
    _PROGRAMS,
    _SEEDS,
    _Program,
    _Seed,
)
from test_v3_live_oracle import (
    _ALLOW_CREATE_V3_KEY,
    _LIVE,
    _LIVE_SKIP,
    _v37_iceberg_runtime_jar,
)


def _cell(value: Any) -> Any:
    """One Arrow value rendered as a comparable Python scalar."""
    if isinstance(value, bool | int | float | str) or value is None:
        return value
    return str(value)


def _rows(table: pa.Table) -> list[tuple]:
    """Every row of an Arrow result as a sorted tuple list with comparable scalars."""
    names = table.column_names
    rows = [tuple(_cell(row[name]) for name in names) for row in table.to_pylist()]
    rows.sort(key=repr)
    return rows


def _excerpt(error: BaseException) -> str:
    """The first line of an engine error, trimmed to a length one source line holds."""
    return str(error).strip().splitlines()[0][:76]


def _statement(item: Any) -> tuple[str, tuple[str, ...]]:
    """A program entry split into its SQL text and the result columns to capture."""
    if isinstance(item, tuple):
        return item[0], item[1]
    return item, ()


def _run_program(run, program: _Program, names: dict[str, str]) -> dict[str, Any]:
    """Execute one program on an engine and return its statement verdicts and probe rows."""
    outcome: dict[str, Any] = {"statements": [], "probes": []}
    for item in program.statements:
        text, columns = _statement(item)
        try:
            table = run(text.format(**names))
        except Exception as error:
            outcome["statements"].append(("ERROR", _excerpt(error)))
            continue
        if columns:
            picked = table.select([name for name in columns if name in table.column_names])
            outcome["statements"].append(("OK", _rows(picked)))
        else:
            outcome["statements"].append(("OK", None))
    for probe in program.probes:
        try:
            outcome["probes"].append(("OK", _rows(run(probe.format(**names)))))
        except Exception as error:
            outcome["probes"].append(("ERROR", _excerpt(error)))
    return outcome


def _snapshot_marks(run, table: str) -> dict[str, str]:
    """The first snapshot id and a timestamp that resolves to it, for the time-travel rows."""
    marks = {"snapshot0": "0", "timestamp0": "1970-01-01 00:00:00"}
    try:
        rows = run(
            f"SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at"
        ).to_pylist()
    except Exception:
        return marks
    if not rows:
        return marks
    marks["snapshot0"] = str(rows[0]["snapshot_id"])
    marks["timestamp0"] = str(rows[0]["committed_at"])[:26]
    return marks


_SOURCE_DDL = "CREATE TABLE {s} (id INT) USING iceberg TBLPROPERTIES ('format-version' = '3')"
_SOURCE_VALUES = "(2)"
_SOURCE_ROWS = [(2,)]


def _seed_repark(repark, target: str, seed: _Seed) -> None:
    """Create and single-file seed one v3 table on the repark facade."""
    repark.sql(
        f"CREATE TABLE {target} ({seed.schema}) USING iceberg{seed.partition} "
        f"TBLPROPERTIES ({seed.properties})"
    ).to_arrow()
    repark.sql(f"INSERT INTO {target} VALUES {seed.values}").to_arrow()


def repark_outcome(program: _Program, warehouse: Path) -> dict[str, Any]:
    """Run one inventory program end to end on the repark facade."""
    from repark import ReparkSession

    repark = (
        ReparkSession.builder.appName("v3-cov").config(_ALLOW_CREATE_V3_KEY, "true").getOrCreate()
    )
    stem = program.name.replace("-", "_")
    target = f"ice.{_NAMESPACE}.{stem}"
    names = {
        "t": target,
        "s": f"ice.{_NAMESPACE}.{stem}_src",
        "c": "ice",
        "q": target,
        "metadata": "",
        "snapshot0": "0",
        "timestamp0": "1970-01-01 00:00:00",
    }
    try:
        repark.register_memory_catalog("ice", warehouse)
        repark.sql(
            f"CREATE NAMESPACE ice.{_NAMESPACE} LOCATION '{warehouse / _NAMESPACE}'"
        ).to_arrow()
        seed = _SEEDS.get(program.seed)
        if seed is not None:
            _seed_repark(repark, target, seed)
            if program.source:
                repark.sql(_SOURCE_DDL.format(s=names["s"])).to_arrow()
                repark.sql(f"INSERT INTO {names['s']} VALUES {_SOURCE_VALUES}").to_arrow()
            names.update(_snapshot_marks(lambda text: repark.sql(text).to_arrow(), target))
            names["metadata"] = _latest_metadata(warehouse, stem)
        return _run_program(lambda text: repark.sql(text).to_arrow(), program, names)
    finally:
        repark.stop()


def _latest_metadata(warehouse: Path, stem: str) -> str:
    """The newest metadata pointer of one table under a warehouse, for `register_table`."""
    candidates = [
        path for path in warehouse.rglob("*.metadata.json") if path.parent.parent.name == stem
    ]
    candidates.sort(key=lambda path: path.stat().st_mtime)
    return str(candidates[-1]) if candidates else ""


def _live_session(warehouse: Path):
    """Reuse the collection's live session when one is alive; otherwise build one."""
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    owned = session is None
    if owned:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("v3-cov-live")
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


def _seed_spark(session, target: str, seed: _Seed) -> None:
    """Create and single-file seed one v3 table on live Spark."""
    session.sql(
        f"CREATE TABLE {target} ({seed.schema}) USING iceberg{seed.partition} "
        f"TBLPROPERTIES ({seed.properties})"
    )
    session.createDataFrame(seed.rows, seed.schema).coalesce(1).writeTo(target).append()


def spark_outcome(program: _Program, session) -> dict[str, Any]:
    """Run one inventory program end to end on the live Spark oracle."""
    warehouse = Path(session.conf.get(f"spark.sql.catalog.{_CATALOG}.warehouse"))
    stem = program.name.replace("-", "_")
    target = f"{_CATALOG}.{_NAMESPACE}.{stem}"
    names = {
        "t": target,
        "s": f"{_CATALOG}.{_NAMESPACE}.{stem}_src",
        "c": _CATALOG,
        "q": f"{_NAMESPACE}.{stem}",
        "metadata": "",
        "snapshot0": "0",
        "timestamp0": "1970-01-01 00:00:00",
    }
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
    seed = _SEEDS.get(program.seed)
    if seed is not None:
        _seed_spark(session, target, seed)
        if program.source:
            session.sql(_SOURCE_DDL.format(s=names["s"]))
            session.createDataFrame(_SOURCE_ROWS, "id INT").coalesce(1).writeTo(names["s"]).append()
        names.update(_snapshot_marks(lambda text: session.sql(text).toArrow(), target))
        names["metadata"] = _latest_metadata(warehouse, stem)
    return _run_program(lambda text: session.sql(text).toArrow(), program, names)


def _agrees(left: Any, right: Any) -> bool:
    """Two engine outcomes agree when both refuse, or both succeed with the same rows."""
    if left[0] != right[0]:
        return False
    return left[0] != "OK" or left[1] == right[1]


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


def _as_golden(outcome: dict[str, Any]) -> dict[str, Any]:
    """One outcome in the golden's shape: every tuple flattened to the list the golden holds."""
    return json.loads(json.dumps(outcome))


@pytest.fixture(scope="module")
def coverage_session() -> Iterator[Any]:
    """One live Spark session for the whole matrix, stopped only when this module created it."""
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    warehouse = Path(tempfile.mkdtemp(prefix="repark-v3cov-live-"))
    session, owned = _live_session(warehouse)
    try:
        yield session
    finally:
        if owned:
            session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


@pytest.mark.parametrize("program", _PROGRAMS, ids=[program.name for program in _PROGRAMS])
def test_v3_statement_row_reproduces_the_measured_repark_answer(
    program: _Program, tmp_path: Path
) -> None:
    """Every inventory row still answers on repark exactly what 2026-09-03 measured."""
    assert _as_golden(repark_outcome(program, tmp_path)) == REPARK[program.name]


@pytest.mark.parametrize("program", _PROGRAMS, ids=[program.name for program in _PROGRAMS])
def test_v3_statement_row_matches_the_live_spark_oracle(
    program: _Program, tmp_path: Path, coverage_session
) -> None:
    """The same row on live Spark reproduces its measured half and keeps its measured verdict."""
    spark = _as_golden(spark_outcome(program, coverage_session))
    assert spark == SPARK[program.name]
    repark = _as_golden(repark_outcome(program, tmp_path))
    assert _verdict(repark, spark) == VERDICTS[program.name]


def test_v3_coverage_inventory_carries_every_program_once() -> None:
    """The golden, the verdict table and the program list are the same set of rows."""
    names = [program.name for program in _PROGRAMS]
    assert len(names) == len(set(names))
    assert set(names) == set(VERDICTS) == set(REPARK) == set(SPARK)


def _partitioned_row_id_mapping(warehouse: Path) -> tuple[tuple[int, int], ...]:
    """The id → `_row_id` mapping a single partitioned v3 INSERT leaves behind."""
    from repark import ReparkSession

    repark = (
        ReparkSession.builder.appName("v3-cov-rowid")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    target = "ice.cov.rowid"
    try:
        repark.register_memory_catalog("ice", warehouse)
        repark.sql(f"CREATE NAMESPACE ice.cov LOCATION '{warehouse / 'cov'}'").to_arrow()
        _seed_repark(repark, target, _SEEDS["pmor"])
        rows = repark.sql(f"SELECT id, _row_id FROM {target} ORDER BY id").to_arrow().to_pylist()
    finally:
        repark.stop()
    return tuple((int(row["id"]), int(row["_row_id"])) for row in rows)


_ROW_ID_SPARK_EQUAL = ((1, 0), (2, 1), (3, 2), (4, 3))
_ROW_ID_REVERSED = ((1, 2), (2, 3), (3, 0), (4, 1))


def test_v3_partitioned_insert_row_id_mapping_is_one_of_two_measured_orders(
    tmp_path: Path,
) -> None:
    """V3-COV-3: the delegated partitioned INSERT assigns `_row_id` by an unstable file order."""
    mapping = _partitioned_row_id_mapping(tmp_path)
    assert mapping in (_ROW_ID_SPARK_EQUAL, _ROW_ID_REVERSED)
    assert sorted(row_id for _, row_id in mapping) == [0, 1, 2, 3]


def test_v3_ctas_partitioned_row_id_mapping_is_stable_and_spark_ordered(tmp_path: Path) -> None:
    """Incidental control: the RePark-owned CTAS writer sorts partitions, so its mapping is not."""
    from repark import ReparkSession

    repark = (
        ReparkSession.builder.appName("v3-cov-ctas-rowid")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        repark.register_memory_catalog("ice", tmp_path)
        repark.sql(f"CREATE NAMESPACE ice.cov LOCATION '{tmp_path / 'cov'}'").to_arrow()
        repark.sql(
            f"CREATE TABLE ice.cov.src ({_PART_SCHEMA}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        ).to_arrow()
        repark.sql(f"INSERT INTO ice.cov.src VALUES {_PART_VALUES}").to_arrow()
        repark.sql(
            "CREATE TABLE ice.cov.ctas USING iceberg PARTITIONED BY (part) "
            "TBLPROPERTIES ('format-version' = '3') AS SELECT * FROM ice.cov.src"
        ).to_arrow()
        rows = repark.sql("SELECT id, _row_id FROM ice.cov.ctas ORDER BY id").to_arrow().to_pylist()
    finally:
        repark.stop()
    assert tuple((int(row["id"]), int(row["_row_id"])) for row in rows) == _ROW_ID_SPARK_EQUAL
