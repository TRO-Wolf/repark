"""ENC-1 — the DataFrame writers refuse a table carrying ``encryption.key-id``.

The facade door of the round-2 refusal: every V1 and V2 writer funnels into the SQL
seats (W-17..W-19 of the unit ledger), so each refuses ``UnsupportedOperationException``
with RePark's one-sentence text and leaves snapshots, rows and warehouse files
unchanged. Source of the expected shape: the live-Spark recording
``python/repark-parity/tests/live_spark/enc1_encryption_oracle.json`` (Spark runs every
cell in plaintext; RePark refuses by the owner's ES-3 ruling — a dated divergence).

pins: enc-1/C-005
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException

NS = "mem.ns"
KEY = "review-test-key"


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    """The memory catalog's warehouse directory."""
    return tmp_path / "wh"


@pytest.fixture
def spark(warehouse: Path) -> ReparkSession:
    """A session with the ``mem`` memory catalog and v3 creates on."""
    session = (
        ReparkSession.builder.appName("pytest-enc-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(warehouse))
    session.sql(f"CREATE NAMESPACE {NS}")
    return session


def _frame(spark: ReparkSession) -> Any:
    """A two-row frame matching the seed schema."""
    return spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )


def _seed(spark: ReparkSession, name: str, *, keyed: bool, part: str = "") -> str:
    """Create ``(id BIGINT, data STRING, cat STRING)`` with three rows, keyed on request."""
    table = f"{NS}.{name}"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part}")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    if keyed:
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')")
    return table


def _snapshots(spark: ReparkSession, table: str) -> list[int]:
    """Every snapshot id in commit order."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()
    return [int(row[0]) for row in rows]


def _rows(spark: ReparkSession, table: str) -> list[list[Any]]:
    """Every row as a list, ordered by id."""
    return [list(row) for row in spark.sql(f"SELECT * FROM {table} ORDER BY id").collect()]


def _listing(warehouse: Path) -> set[str]:
    """Every file under the warehouse, as relative posix paths."""
    return {
        path.relative_to(warehouse).as_posix() for path in warehouse.rglob("*") if path.is_file()
    }


def _state(
    spark: ReparkSession, warehouse: Path, table: str
) -> tuple[list[int], list[list[Any]], set[str]]:
    """Snapshot ids, rows and the warehouse file listing."""
    return (_snapshots(spark, table), _rows(spark, table), _listing(warehouse))


def _assert_refusal(error: BaseException, table: str) -> None:
    """The refusal names the table, the property and ENC-1, never the key value."""
    assert type(error) is UnsupportedOperationException
    message = str(error)
    short = table.rsplit(".", 1)[-1]
    assert message == (
        f"Table ns.{short} carries property 'encryption.key-id': RePark has no table "
        "encryption and refuses to write plaintext into a table that asks for it (ENC-1)."
    )
    assert KEY not in message, f"refusal must never echo the key value, got: {message}"


def test_write_to_append_refuses(spark: ReparkSession, warehouse: Path) -> None:
    """pins: enc-1/C-005"""
    table = _seed(spark, "v2append", keyed=True)
    before = _state(spark, warehouse, table)
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).writeTo(table).append()
    _assert_refusal(caught.value, table)
    assert _state(spark, warehouse, table) == before


def test_write_to_overwrite_partitions_refuses(spark: ReparkSession, warehouse: Path) -> None:
    """pins: enc-1/C-005"""
    table = _seed(spark, "v2part", keyed=True, part="PARTITIONED BY (cat)")
    before = _state(spark, warehouse, table)
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).writeTo(table).overwritePartitions()
    _assert_refusal(caught.value, table)
    assert _state(spark, warehouse, table) == before


def test_write_to_overwrite_condition_refuses(spark: ReparkSession, warehouse: Path) -> None:
    """pins: enc-1/C-005"""
    table = _seed(spark, "v2cond", keyed=True)
    before = _state(spark, warehouse, table)
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).writeTo(table).overwrite("cat = 'x'")
    _assert_refusal(caught.value, table)
    assert _state(spark, warehouse, table) == before


def test_write_to_create_with_key_refuses_without_leaving_a_table(
    spark: ReparkSession, warehouse: Path
) -> None:
    """pins: enc-1/C-005"""
    table = f"{NS}.v2create"
    before = _listing(warehouse)
    with pytest.raises(UnsupportedOperationException) as caught:
        (
            _frame(spark)
            .writeTo(table)
            .tableProperty("format-version", "3")
            .tableProperty("encryption.key-id", KEY)
            .create()
        )
    _assert_refusal(caught.value, table)
    assert not spark.catalog.table_exists(table)
    assert _listing(warehouse) == before


def test_write_to_create_or_replace_onto_keyed_table_refuses(
    spark: ReparkSession, warehouse: Path
) -> None:
    """pins: enc-1/C-005"""
    table = _seed(spark, "v2replace", keyed=True)
    before = _state(spark, warehouse, table)
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).writeTo(table).createOrReplace()
    _assert_refusal(caught.value, table)
    assert _state(spark, warehouse, table) == before


def test_v1_save_as_table_append_refuses(spark: ReparkSession, warehouse: Path) -> None:
    """pins: enc-1/C-005"""
    table = _seed(spark, "v1append", keyed=True)
    before = _state(spark, warehouse, table)
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).write.format("iceberg").mode("append").saveAsTable(table)
    _assert_refusal(caught.value, table)
    assert _state(spark, warehouse, table) == before


def test_v1_save_as_table_overwrite_refuses(spark: ReparkSession, warehouse: Path) -> None:
    """pins: enc-1/C-005"""
    table = _seed(spark, "v1overwrite", keyed=True)
    before = _state(spark, warehouse, table)
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).write.format("iceberg").mode("overwrite").saveAsTable(table)
    _assert_refusal(caught.value, table)
    assert _state(spark, warehouse, table) == before


def test_v1_insert_into_refuses(spark: ReparkSession, warehouse: Path) -> None:
    """pins: enc-1/C-005"""
    for name, overwrite in (("v1insert", None), ("v1insertoverw", True)):
        table = _seed(spark, name, keyed=True)
        before = _state(spark, warehouse, table)
        with pytest.raises(UnsupportedOperationException) as caught:
            if overwrite is None:
                _frame(spark).write.format("iceberg").insertInto(table)
            else:
                _frame(spark).write.format("iceberg").insertInto(table, overwrite=True)
        _assert_refusal(caught.value, table)
        assert _state(spark, warehouse, table) == before


def _refs(spark: ReparkSession, table: str) -> list[list[Any]]:
    """Every ref as ``[name, type, snapshot_id]``, ordered by name."""
    rows = spark.sql(f"SELECT name, type, snapshot_id FROM {table}.refs ORDER BY name").collect()
    return [list(row) for row in rows]


@pytest.mark.parametrize("version", ["2", "3"])
def test_write_to_branch_append_refuses(
    spark: ReparkSession, warehouse: Path, version: str
) -> None:
    """pins: enc-1/C-007"""
    table = f"{NS}.branch{version}"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '{version}')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x')")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')")
    before = (_state(spark, warehouse, table), _refs(spark, table))
    with pytest.raises(UnsupportedOperationException) as caught:
        _frame(spark).writeTo(f"{table}.branch_b1").append()
    _assert_refusal(caught.value, table)
    assert (_state(spark, warehouse, table), _refs(spark, table)) == before


@pytest.mark.parametrize("version", ["2", "3"])
@pytest.mark.parametrize("conf", ["spark.wap.branch", "spark.wap.id"])
def test_wap_session_writes_refuse(
    spark: ReparkSession, warehouse: Path, version: str, conf: str
) -> None:
    """pins: enc-1/C-007"""
    table = f"{NS}.wap{version}"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '{version}', 'write.wap.enabled' = 'true')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')")
    before = (_state(spark, warehouse, table), _refs(spark, table))
    spark.conf.set(conf, "w1")
    try:
        with pytest.raises(UnsupportedOperationException) as caught:
            spark.sql(f"INSERT INTO {table} VALUES (4, 'd', 'x')")
        _assert_refusal(caught.value, table)
        with pytest.raises(UnsupportedOperationException) as caught:
            _frame(spark).writeTo(table).append()
        _assert_refusal(caught.value, table)
    finally:
        spark.conf.unset(conf)
    assert (_state(spark, warehouse, table), _refs(spark, table)) == before


def test_writers_onto_unkeyed_and_lookalike_tables_run(
    spark: ReparkSession, warehouse: Path
) -> None:
    """pins: enc-1/C-005"""
    plain = _seed(spark, "plain", keyed=False)
    _frame(spark).writeTo(plain).append()
    _frame(spark).write.format("iceberg").mode("append").saveAsTable(plain)
    assert len(_rows(spark, plain)) == 7
    lookalike = _seed(spark, "look", keyed=False)
    spark.sql(f"ALTER TABLE {lookalike} SET TBLPROPERTIES ('encryption.key-id-x' = '{KEY}')")
    _frame(spark).writeTo(lookalike).append()
    assert len(_rows(spark, lookalike)) == 5
