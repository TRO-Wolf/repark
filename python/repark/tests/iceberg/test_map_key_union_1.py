"""MAP-KEY-ALTER-1 delta: a schema-evolving write never changes a map key, as Java refuses."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import PySparkException

TARGET = (
    "CREATE TABLE mem.ns.t (id INT, mk MAP<STRUCT<s: STRUCT<x: INT>, a: INT>, INT>) USING iceberg"
)
ACCEPT_ANY = " TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
WITH_Z = "mk MAP<STRUCT<s: STRUCT<x: INT, z: INT>, a: INT>, INT>"
WIDER_X = "mk MAP<STRUCT<s: STRUCT<x: BIGINT>, a: INT>, INT>"
ALTER_MK = (
    "Unsupported table change: Cannot alter map keys: "
    "map<struct<#: s: optional struct<#: x: optional int>, #: a: optional int>, int>"
)
BY_NAME = "INSERT INTO mem.ns.t BY NAME SELECT * FROM mem.ns.src"
MERGE = (
    "MERGE WITH SCHEMA EVOLUTION INTO mem.ns.t t USING mem.ns.src s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("pytest-map-key-union-1")
        .config("spark.sql.iceberg.merge-schema", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _table_files(root: Path) -> int:
    return sum(1 for path in (root / "ns" / "t").rglob("*") if path.is_file())


def _mk_type(spark: ReparkSession) -> str:
    rows = spark.sql("DESCRIBE TABLE mem.ns.t").collect()
    return next(str(row[1]) for row in rows if row[0] == "mk")


def _seed(spark: ReparkSession, accept_any: bool, source: str) -> None:
    spark.sql(TARGET + (ACCEPT_ANY if accept_any else "")).collect()
    spark.sql(f"CREATE TABLE mem.ns.src (id INT, {source}) USING iceberg").collect()
    spark.sql("INSERT INTO mem.ns.src (id) VALUES (1)").collect()


def _refuses(spark: ReparkSession, root: Path, write: Callable[[], object]) -> None:
    before = (_mk_type(spark), _table_files(root))
    with pytest.raises(PySparkException, match="Cannot alter map keys") as raised:
        write()
    assert ALTER_MK in re.sub(r"\b\d+: ", "#: ", str(raised.value))
    assert (_mk_type(spark), _table_files(root)) == before
    assert spark.sql("SELECT * FROM mem.ns.t").count() == 0


@pytest.mark.parametrize(
    ("accept_any", "source", "statement"),
    [(True, WITH_Z, BY_NAME), (True, WIDER_X, BY_NAME), (False, WITH_Z, MERGE)],
    ids=["by-name-adds-z", "by-name-widens-x", "merge-adds-z"],
)
def test_an_evolving_statement_refuses_a_map_key_change(
    spark: ReparkSession, tmp_path: Path, accept_any: bool, source: str, statement: str
) -> None:
    _seed(spark, accept_any, source)
    _refuses(spark, tmp_path, lambda: spark.sql(statement).collect())


def test_a_dataframe_append_with_an_extra_column_refuses_a_map_key_change(
    spark: ReparkSession, tmp_path: Path
) -> None:
    _seed(spark, True, f"{WITH_Z}, extra INT")
    frame = spark.sql("SELECT * FROM mem.ns.src")
    _refuses(spark, tmp_path, lambda: frame.writeTo("mem.ns.t").append())


def test_a_map_value_union_still_evolves(spark: ReparkSession) -> None:
    spark.sql(
        "CREATE TABLE mem.ns.t (id INT, mk MAP<INT, STRUCT<x: INT>>) USING iceberg" + ACCEPT_ANY
    ).collect()
    spark.sql(
        "CREATE TABLE mem.ns.src (id INT, mk MAP<INT, STRUCT<x: INT, z: INT>>) USING iceberg"
    ).collect()
    spark.sql("INSERT INTO mem.ns.src (id) VALUES (1)").collect()
    spark.sql(BY_NAME).collect()
    assert "z" in _mk_type(spark)
    assert spark.sql("SELECT * FROM mem.ns.t").count() == 1
