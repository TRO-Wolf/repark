"""ICE-WRITE-OPTIONS-1 run 22b — write options across the paths main added under them.

The options map meets ICE-DYN-OVERWRITE-1's dynamic overwrite and typed static flag,
ICE-RTAS-OPS-2's replace commit, ICE-SORTED-INSERT-1's sort-order stamp and
ICE-V3-WRITE-DEFAULT-1's column-list writers. Row answers come from the recorded
Spark oracles of those units; the options half is the SNAP-03 / SNAP-06 rule that
Spark stamps ``snapshot-property.*`` on every commit its writer makes.

pins: ice-write-options-1/C-014, C-015, C-016, C-017, C-018
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow.parquet as pa_pq
import pytest
from test_ice_dyn_overwrite_1 import _KEY, _expect, _fixture, _rows, _seed
from test_ice_write_options_1 import _latest_summary, _snapshot_count

from repark import ReparkSession, _native
from repark.errors import AnalysisException

_CATALOG = "wo_rebase"
_NS = "ns"
_RUN_ID = "snapshot-property.run_id"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog and one namespace."""
    session = ReparkSession.builder.appName("pytest-ice-write-options-1-rebase").getOrCreate()
    session.register_memory_catalog(_CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NS}")
    return session


@pytest.fixture
def dynamic(spark: ReparkSession) -> Iterator[ReparkSession]:
    """The same session under ``partitionOverwriteMode=dynamic``, restored after."""
    spark.conf.set(_KEY, "dynamic")
    try:
        yield spark
    finally:
        spark.conf.unset(_KEY)


def _table(name: str) -> str:
    return f"{_CATALOG}.{_NS}.{name}"


def _channel(spark: ReparkSession, sql: str, options: dict[str, str]) -> None:
    _native.session_sql_with_write_options(spark._inner, sql, options)


def _operations(spark: ReparkSession, table: str) -> list[str]:
    rows = (
        spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at")
        .to_arrow()
        .to_pylist()
    )
    return [str(row["operation"]) for row in rows]


def _files(spark: ReparkSession, table: str) -> list[dict[str, Any]]:
    rows = spark.sql(f"SELECT file_path, sort_order_id FROM {table}.files").to_arrow().to_pylist()
    assert rows, f"no data files on {table}"
    return rows


def _local(path: str) -> str:
    return path[7:] if path.startswith("file://") else path


def test_dynamic_insert_into_honours_snapshot_property(dynamic: ReparkSession) -> None:
    """WO-DYN-01: dynamic ``insertInto`` overwrite keeps siblings and stamps the property."""
    table = _table("ii_dyn")
    _seed(dynamic, table)
    source = dynamic.sql("SELECT CAST(30 AS BIGINT) AS id, 'c' AS p")
    source.write.format("iceberg").option(_RUN_ID, "dyn-ii").mode("overwrite").insertInto(table)
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_insertInto"])
    summary = _latest_summary(dynamic, table)
    assert summary["run_id"] == "dyn-ii"
    assert summary["replace-partitions"] == "true"


def test_dynamic_insert_into_honours_writer_knob(dynamic: ReparkSession) -> None:
    """WO-DYN-02: the dynamic overwrite stages with the option codec (gzip footers)."""
    table = _table("ii_dyn_codec")
    _seed(dynamic, table)
    source = dynamic.sql("SELECT CAST(30 AS BIGINT) AS id, 'c' AS p")
    writer = source.write.format("iceberg").option("compression-codec", "gzip")
    writer.mode("overwrite").insertInto(table)
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_insertInto"])
    newest = [row for row in _files(dynamic, table) if "p=c" in str(row["file_path"])]
    footer = pa_pq.read_metadata(_local(str(newest[0]["file_path"])))
    assert footer.row_group(0).column(0).compression == "GZIP"


def test_save_as_table_static_pin_travels_with_options(dynamic: ReparkSession) -> None:
    """WO-DYN-03: ``saveAsTable`` overwrite stays whole-table under dynamic AND stamps."""
    table = _table("sat_dyn")
    _seed(dynamic, table)
    source = dynamic.sql("SELECT CAST(40 AS BIGINT) AS id, 'c' AS p")
    source.write.format("iceberg").option(_RUN_ID, "sat").mode("overwrite").saveAsTable(table)
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_saveAsTable"])
    summary = _latest_summary(dynamic, table)
    assert summary["run_id"] == "sat"
    assert "replace-partitions" not in summary


def test_dynamic_by_name_overwrite_honours_options(dynamic: ReparkSession) -> None:
    """WO-DYN-04: ``INSERT OVERWRITE … BY NAME`` under dynamic keeps siblings and stamps."""
    table = _table("byname_dyn")
    _seed(dynamic, table)
    _channel(
        dynamic,
        f"INSERT OVERWRITE {table} BY NAME SELECT 'b' AS p, CAST(20 AS BIGINT) AS id",
        {_RUN_ID: "byname-dyn"},
    )
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql"])
    assert _latest_summary(dynamic, table)["run_id"] == "byname-dyn"


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT OVERWRITE {t} SELECT CAST(9 AS BIGINT) AS id, 'z' AS p WHERE false",
        "INSERT OVERWRITE {t} BY NAME SELECT 'z' AS p, CAST(9 AS BIGINT) AS id WHERE false",
    ],
)
def test_dynamic_empty_overwrite_commits_nothing_with_options(
    dynamic: ReparkSession, statement: str
) -> None:
    """WO-DYN-05: an empty dynamic source commits no snapshot, options or not (Q-22b-WO-3)."""
    table = _table("empty_dyn")
    _seed(dynamic, table)
    before = _snapshot_count(dynamic, table)
    _channel(dynamic, statement.format(t=table), {_RUN_ID: "empty-dyn"})
    assert _snapshot_count(dynamic, table) == before
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql_empty"])


def test_static_empty_by_name_still_refuses_options(spark: ReparkSession) -> None:
    """WO-DYN-06: the static empty BY NAME wipe keeps its C-010 refusal; nothing commits."""
    table = _table("empty_static_byname")
    _seed(spark, table)
    before = _snapshot_count(spark, table)
    with pytest.raises(AnalysisException, match="empty projection does not support write"):
        _channel(
            spark,
            f"INSERT OVERWRITE {table} BY NAME SELECT 'z' AS p, CAST(9 AS BIGINT) AS id "
            "WHERE false",
            {_RUN_ID: "empty-static"},
        )
    assert _snapshot_count(spark, table) == before


def test_create_or_replace_existing_records_overwrite_with_options(
    spark: ReparkSession,
) -> None:
    """WO-RTAS-01: an option-carrying RTAS over a table answers [append, overwrite]."""
    table = _table("rtas_existing")
    spark.sql(f"CREATE TABLE {table} USING iceberg AS SELECT CAST(1 AS BIGINT) AS id")
    frame = spark.sql("SELECT CAST(2 AS BIGINT) AS id")
    frame.writeTo(table).option(_RUN_ID, "rtas-1").createOrReplace()
    assert _operations(spark, table) == ["append", "overwrite"]
    assert _latest_summary(spark, table)["run_id"] == "rtas-1"


@pytest.mark.parametrize(
    ("where", "operations"),
    [("", ["overwrite"]), (" WHERE false", ["delete"])],
)
def test_create_or_replace_new_table_operation_with_options(
    spark: ReparkSession, where: str, operations: list[str]
) -> None:
    """WO-RTAS-02: RTAS creating the table answers [overwrite] / empty [delete] and stamps."""
    table = _table("rtas_new")
    frame = spark.sql(f"SELECT CAST(2 AS BIGINT) AS id{where}")
    frame.writeTo(table).option(_RUN_ID, "rtas-new").createOrReplace()
    assert _operations(spark, table) == operations
    assert _latest_summary(spark, table)["run_id"] == "rtas-new"


@pytest.mark.parametrize("partitioned", [False, True])
def test_option_append_stamps_default_sort_order(spark: ReparkSession, partitioned: bool) -> None:
    """WO-SORT-01: the option-carrying list-free append sorts and stamps the order id."""
    table = _table(f"sorted_{int(partitioned)}")
    part = " PARTITIONED BY (p)" if partitioned else ""
    spark.sql(f"CREATE TABLE {table} (id BIGINT, p STRING) USING iceberg{part}")
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY (id)")
    values = ", ".join(f"({key}, 'a')" for key in (5, 3, 9, 1, 7))
    _channel(
        spark,
        f"INSERT INTO {table} SELECT * FROM (VALUES {values}) AS t(id, p)",
        {_RUN_ID: "sorted"},
    )
    assert _latest_summary(spark, table)["run_id"] == "sorted"
    for entry in _files(spark, table):
        assert entry["sort_order_id"] == 1, entry
        ids = pa_pq.read_table(_local(str(entry["file_path"])), columns=["id"])
        assert ids.column("id").to_pylist() == sorted(ids.column("id").to_pylist())


def test_option_append_honours_writer_column_list(spark: ReparkSession) -> None:
    """WO-APP-01: ``writeTo().append()`` renders a column list; the options append takes it."""
    table = _table("col_list")
    spark.sql(f"CREATE TABLE {table} (id BIGINT, name STRING, note STRING) USING iceberg")
    frame = spark.sql("SELECT 'x' AS name, CAST(4 AS BIGINT) AS id, 'n' AS note")
    frame.writeTo(table).option(_RUN_ID, "cols").append()
    rows = spark.sql(f"SELECT id, name, note FROM {table}").to_arrow().to_pylist()
    assert rows == [{"id": 4, "name": "x", "note": "n"}]
    assert _latest_summary(spark, table)["run_id"] == "cols"


def test_option_column_list_append_on_sql_door(spark: ReparkSession) -> None:
    """WO-APP-02: an explicit list maps by name and leaves an unlisted nullable column NULL."""
    table = _table("col_list_sql")
    spark.sql(f"CREATE TABLE {table} (id BIGINT, name STRING, note STRING) USING iceberg")
    _channel(
        spark,
        f"INSERT INTO {table} (name, id) SELECT 'y' AS name, CAST(5 AS BIGINT) AS id",
        {_RUN_ID: "cols-sql"},
    )
    rows = spark.sql(f"SELECT id, name, note FROM {table}").to_arrow().to_pylist()
    assert rows == [{"id": 5, "name": "y", "note": None}]
    assert _latest_summary(spark, table)["run_id"] == "cols-sql"
