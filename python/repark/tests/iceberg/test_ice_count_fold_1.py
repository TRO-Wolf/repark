"""ICE-COUNT-FOLD-1 — count(*) folds from Iceberg statistics with names and types unchanged."""

from __future__ import annotations

import contextlib
import io
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.spark.dataframe.core import DataFrame

CATALOG = "count_fold"
TABLE = f"{CATALOG}.w.t"
FILES = 4
ROWS_PER_FILE = 3
TOTAL_ROWS = FILES * ROWS_PER_FILE
PHYSICAL_HEADER = "== Physical Plan =="
SCAN = "IcebergTableScan"


@pytest.fixture()
def engine(tmp_path: Path) -> Iterator[ReparkSession]:
    """A session with a four-file, twelve-row Iceberg table on a module-private catalog."""
    warehouse = tmp_path / "wh"
    session = ReparkSession.builder.appName("pytest-ice-count-fold-1").getOrCreate()
    session.register_memory_catalog(CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    session.sql(f"CREATE TABLE {TABLE} (id INT, name STRING) USING iceberg").collect()
    for index in range(FILES):
        base = index * ROWS_PER_FILE
        session.sql(
            f"INSERT INTO {TABLE} VALUES ({base + 1}, 'a'), ({base + 2}, 'b'), ({base + 3}, 'c')"
        ).collect()
    yield session
    session.stop()


def _sql_physical_plan(engine: ReparkSession, query: str) -> str:
    """The physical-plan text of a SQL query through EXPLAIN."""
    rows = engine.sql(f"EXPLAIN {query}").collect()
    physical = [row["plan"] for row in rows if row["plan_type"] == "physical_plan"]
    assert physical, f"EXPLAIN produced no physical plan: {rows}"
    return "\n".join(physical)


def _frame_physical_plan(frame: DataFrame) -> str:
    """The physical-plan section of DataFrame.explain() output."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain()
    output = buffer.getvalue()
    assert PHYSICAL_HEADER in output, output
    return output.split(PHYSICAL_HEADER, 1)[1]


def _single_long(frame: DataFrame) -> tuple[str, int]:
    """The only column's name and value, with the Arrow and Spark types checked as LongType."""
    table = frame.to_arrow()
    assert table.num_columns == 1, table.schema
    assert table.schema.field(0).type == pa.int64(), table.schema
    assert str(frame.schema.fields[0].dataType) == "LongType()", frame.schema
    return table.schema.field(0).name, int(table.column(0)[0].as_py())


@pytest.mark.parametrize(
    ("query", "name"),
    [
        (f"SELECT count(*) FROM {TABLE}", "count(*)"),
        (f"SELECT count(1) FROM {TABLE}", "count(Int64(1))"),
        (f"SELECT count(5) FROM {TABLE}", "count(Int64(5))"),
    ],
)
def test_sql_count_name_type_and_value_unchanged(
    engine: ReparkSession, query: str, name: str
) -> None:
    """C-004: the SQL door keeps main's column name, LongType and the row count."""
    assert _single_long(engine.sql(query)) == (name, TOTAL_ROWS)


def test_sql_count_star_folds(engine: ReparkSession) -> None:
    """C-003: SELECT count(*) and count(1) on a plain table plan no scan."""
    for query in (f"SELECT count(*) FROM {TABLE}", f"SELECT count(1) FROM {TABLE}"):
        plan = _sql_physical_plan(engine, query)
        assert SCAN not in plan, plan


def test_sql_count_with_filter_still_scans(engine: ReparkSession) -> None:
    """C-003: a residual keeps the scan, and the count is the matching rows."""
    query = f"SELECT count(*) FROM {TABLE} WHERE id < 5"
    assert SCAN in _sql_physical_plan(engine, query)
    assert _single_long(engine.sql(query))[1] == 4


def test_dataframe_count_value_unchanged(engine: ReparkSession) -> None:
    """C-004: df.count() answers the row count as a Python int."""
    counted = engine.table(TABLE).count()
    assert isinstance(counted, int)
    assert counted == TOTAL_ROWS


def test_group_by_count_name_type_value_and_fold(engine: ReparkSession) -> None:
    """C-004 / C-003: groupBy().count() keeps the Spark name count, LongType, and folds."""
    frame = engine.table(TABLE).groupBy().count()
    assert _single_long(frame) == ("count", TOTAL_ROWS)
    plan = _frame_physical_plan(frame)
    assert SCAN not in plan, plan


def test_functions_count_star_name_type_value_and_fold(engine: ReparkSession) -> None:
    """C-004 / C-003: agg(F.count('*')) keeps the Spark name count(1), LongType, and folds."""
    frame = engine.table(TABLE).agg(F.count("*"))
    assert _single_long(frame) == ("count(1)", TOTAL_ROWS)
    plan = _frame_physical_plan(frame)
    assert SCAN not in plan, plan
