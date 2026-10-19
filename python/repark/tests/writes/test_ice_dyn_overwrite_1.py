"""Dynamic ``partitionOverwriteMode`` pins (ICE-DYN-OVERWRITE-1).

Live Spark 4.1.2 + Iceberg 1.11.0 answers were recorded into
``python/repark-parity/fixtures/torture/data/ice_dyn_overwrite_1/spark_oracle.json``
(one JVM, ``/tmp/oc-worker/ja-dyn/record_spark_oracle.py`` plus the ``race4`` rerun for
the overwrite-vs-concurrent-append interleaves). Routine CI runs the JVM-free
repark-vs-fixture half; ``REPARK_PARITY_LIVE=1`` re-runs the Spark cells live and
asserts the triple repark == fixture == live Spark.

pins: ice-dyn-overwrite-1/C-001
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException

_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark oracle skipped (routine CI is JVM-free)"
_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "repark-parity"
    / "fixtures"
    / "torture"
    / "data"
    / "ice_dyn_overwrite_1"
    / "spark_oracle.json"
)
_KEY = "spark.sql.sources.partitionOverwriteMode"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_CATALOG = "dynow_cat"
_NS = "dynow_ns"


def _fixture() -> dict[str, Any]:
    """Load the recorded Spark oracle answers."""
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Fresh session with a memory Iceberg catalog and v3 creation allowed."""
    session = (
        ReparkSession.builder.appName("pytest-ice-dyn-overwrite-1")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NS}")
    return session


@pytest.fixture
def dynamic(spark: ReparkSession) -> Iterator[ReparkSession]:
    """Session with the dynamic overwrite conf set, restored to static after."""
    spark.conf.set(_KEY, "dynamic")
    try:
        yield spark
    finally:
        spark.conf.unset(_KEY)


def _seed(
    spark: ReparkSession,
    table: str,
    partitioned: bool = True,
    version: int = 2,
    extra_props: str = "",
) -> None:
    """Create ``(id BIGINT, p STRING)`` holding ``a, b, c``."""
    part = " PARTITIONED BY (p)" if partitioned else ""
    props = f", {extra_props}" if extra_props else ""
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, p STRING) USING iceberg{part} "
        f"TBLPROPERTIES ('format-version'='{version}'{props})"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")


def _rows(spark: ReparkSession, table: str) -> list[tuple[Any, Any]]:
    """Read ``id, p`` ordered by id on the Arrow export path."""
    arrow = spark.sql(f"SELECT id, p FROM {table} ORDER BY id").to_arrow()
    assert arrow.schema.field("id").type in (pa.int32(), pa.int64())
    assert arrow.schema.field("p").type in (pa.string(), pa.large_string())
    return [(row["id"], row["p"]) for row in arrow.to_pylist()]


def _expect(cell: dict[str, Any]) -> list[tuple[Any, Any]]:
    """Recorded Spark rows as tuples."""
    assert "rows" in cell, f"oracle cell has no rows: {cell}"
    return [(row[0], row[1]) for row in cell["rows"]]


def test_sql_dynamic_partition_less_matches_oracle(
    dynamic: ReparkSession,
) -> None:
    """Dynamic SQL overwrite replaces only the touched partition.

    pins: ice-dyn-overwrite-1/C-002
    """
    table = f"{_CATALOG}.{_NS}.sql_dyn"
    _seed(dynamic, table)
    dynamic.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql"])


def test_insert_into_dynamic_matches_oracle(dynamic: ReparkSession) -> None:
    """Dynamic ``insertInto`` overwrite replaces only the touched partition.

    pins: ice-dyn-overwrite-1/C-003
    """
    table = f"{_CATALOG}.{_NS}.ii_dyn"
    _seed(dynamic, table)
    source = dynamic.sql("SELECT CAST(30 AS BIGINT) AS id, 'c' AS p")
    source.write.mode("overwrite").insertInto(table)
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_insertInto"])


def test_sql_static_replaces_whole_table(spark: ReparkSession) -> None:
    """Static SQL overwrite replaces the whole table (Spark-equal control).

    pins: ice-dyn-overwrite-1/C-004
    """
    table = f"{_CATALOG}.{_NS}.sql_static"
    _seed(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _rows(spark, table) == _expect(_fixture()["cells"]["static_sql"])


def test_insert_into_static_replaces_whole_table(spark: ReparkSession) -> None:
    """Static ``insertInto`` overwrite replaces the whole table (Spark-equal control).

    pins: ice-dyn-overwrite-1/C-005
    """
    table = f"{_CATALOG}.{_NS}.ii_static"
    _seed(spark, table)
    spark.sql("SELECT CAST(30 AS BIGINT) AS id, 'c' AS p").write.mode("overwrite").insertInto(table)
    assert _rows(spark, table) == _expect(_fixture()["cells"]["static_insertInto"])


def test_overwrite_partitions_dynamic_matches_oracle(dynamic: ReparkSession) -> None:
    """``overwritePartitions`` stays partition-scoped under dynamic (already correct).

    pins: ice-dyn-overwrite-1/C-006
    """
    table = f"{_CATALOG}.{_NS}.owp_dyn"
    _seed(dynamic, table)
    dynamic.sql("SELECT CAST(10 AS BIGINT) AS id, 'a' AS p").writeTo(table).overwritePartitions()
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_overwritePartitions"])


def test_sql_dynamic_v3_matches_oracle(dynamic: ReparkSession) -> None:
    """Dynamic SQL overwrite on a v3 table replaces only the touched partition.

    pins: ice-dyn-overwrite-1/C-007
    """
    table = f"{_CATALOG}.{_NS}.sql_dyn_v3"
    _seed(dynamic, table, version=3)
    dynamic.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql_v3"])


def test_insert_into_dynamic_v3_matches_oracle(dynamic: ReparkSession) -> None:
    """Dynamic ``insertInto`` overwrite on a v3 table replaces only the touched partition.

    pins: ice-dyn-overwrite-1/C-008
    """
    table = f"{_CATALOG}.{_NS}.ii_dyn_v3"
    _seed(dynamic, table, version=3)
    source = dynamic.sql("SELECT CAST(30 AS BIGINT) AS id, 'c' AS p")
    source.write.mode("overwrite").insertInto(table)
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_insertInto_v3"])


def test_sql_dynamic_unpartitioned_replaces_whole_table(dynamic: ReparkSession) -> None:
    """Dynamic overwrite of an unpartitioned table replaces the whole table.

    pins: ice-dyn-overwrite-1/C-009
    """
    table = f"{_CATALOG}.{_NS}.sql_dyn_unpart"
    _seed(dynamic, table, partitioned=False)
    dynamic.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql_unpartitioned"])


def test_sql_dynamic_empty_leaves_table_unchanged(dynamic: ReparkSession) -> None:
    """Dynamic overwrite with an empty source touches no partition.

    pins: ice-dyn-overwrite-1/C-010
    """
    table = f"{_CATALOG}.{_NS}.sql_dyn_empty"
    _seed(dynamic, table)
    dynamic.sql(f"INSERT OVERWRITE {table} SELECT CAST(20 AS BIGINT) AS id, 'b' AS p WHERE false")
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql_empty"])


def test_sql_dynamic_empty_unpartitioned_leaves_table_unchanged(
    dynamic: ReparkSession,
) -> None:
    """Dynamic empty overwrite of an unpartitioned table is a no-op, not a wipe.

    pins: ice-dyn-overwrite-1/C-011
    """
    table = f"{_CATALOG}.{_NS}.sql_dyn_empty_unpart"
    _seed(dynamic, table, partitioned=False)
    dynamic.sql(f"INSERT OVERWRITE {table} SELECT CAST(20 AS BIGINT) AS id, 'b' AS p WHERE false")
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql_empty_unpartitioned"])


def test_sql_static_empty_wipes_table(spark: ReparkSession) -> None:
    """Static empty overwrite wipes the table (Spark-equal control).

    pins: ice-dyn-overwrite-1/C-012
    """
    table = f"{_CATALOG}.{_NS}.sql_static_empty"
    _seed(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} SELECT CAST(20 AS BIGINT) AS id, 'b' AS p WHERE false")
    assert _rows(spark, table) == _expect(_fixture()["cells"]["static_sql_empty"])


def test_sql_dynamic_evolved_spec_matches_oracle(dynamic: ReparkSession) -> None:
    """Dynamic overwrite after unpartitioned-to-identity evolution matches Spark.

    pins: ice-dyn-overwrite-1/C-013
    """
    table = f"{_CATALOG}.{_NS}.sql_dyn_evo"
    _seed(dynamic, table, partitioned=False)
    dynamic.sql(f"ALTER TABLE {table} ADD PARTITION FIELD p")
    dynamic.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_sql_evolved_spec"])


def test_save_as_table_overwrite_ignores_dynamic_conf(dynamic: ReparkSession) -> None:
    """``saveAsTable`` overwrite always replaces the whole table, even under dynamic.

    pins: ice-dyn-overwrite-1/C-014
    """
    table = f"{_CATALOG}.{_NS}.sat_dyn"
    _seed(dynamic, table)
    source = dynamic.sql("SELECT CAST(40 AS BIGINT) AS id, 'c' AS p")
    source.write.mode("overwrite").saveAsTable(table)
    assert _rows(dynamic, table) == _expect(_fixture()["cells"]["dynamic_saveAsTable"])


def test_conf_mixed_case_dynamic_honored(spark: ReparkSession) -> None:
    """The conf value matches case-insensitively like Spark.

    pins: ice-dyn-overwrite-1/C-015
    """
    spark.conf.set(_KEY, "DyNaMiC")
    try:
        assert spark.conf.get(_KEY) == "DyNaMiC"
        table = f"{_CATALOG}.{_NS}.mixed"
        _seed(spark, table)
        spark.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
        assert _rows(spark, table) == _cell_mixed_rows(_fixture()["cells"]["conf_case"])
    finally:
        spark.conf.unset(_KEY)


def _cell_mixed_rows(cell: dict[str, Any]) -> list[tuple[Any, Any]]:
    """Recorded Spark rows of the mixed-case conf cell."""
    return [(row[0], row[1]) for row in cell["mixed_rows"]]


def test_conf_bogus_refuses_with_spark_class(spark: ReparkSession) -> None:
    """An unknown mode refuses loud with Spark's error class and keeps the old value.

    pins: ice-dyn-overwrite-1/C-016
    """
    spark.conf.set(_KEY, "dynamic")
    try:
        with pytest.raises(IllegalArgumentException, match="OUT_OF_RANGE_OF_OPTIONS"):
            spark.conf.set(_KEY, "bogus")
        assert spark.conf.get(_KEY) == "dynamic"
    finally:
        spark.conf.unset(_KEY)


def test_builder_conf_dynamic_matches_oracle(tmp_path: Path) -> None:
    """Builder-supplied dynamic conf reaches the write path.

    pins: ice-dyn-overwrite-1/C-017
    """
    session = (
        ReparkSession.builder.appName("pytest-ice-dyn-builder")
        .config(_KEY, "dynamic")
        .getOrCreate()
    )
    try:
        assert session.conf.get(_KEY) == "dynamic"
        session.register_memory_catalog("dynow_builder", tmp_path)
        session.sql("CREATE NAMESPACE dynow_builder.ns")
        table = "dynow_builder.ns.builder_dyn"
        _seed(session, table)
        session.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
        assert _rows(session, table) == _cell_builder_rows(_fixture()["cells"]["builder_conf"])
    finally:
        session.stop()


def _cell_builder_rows(cell: dict[str, Any]) -> list[tuple[Any, Any]]:
    """Recorded Spark rows of the builder-conf cell."""
    return [(row[0], row[1]) for row in cell["rows"]]


def _live_fresh(session: Any, name: str, partitioned: bool = True) -> str:
    """Create and seed one live-tier table, returning its name."""
    table = f"dynow_live.ns.{name}"
    part = " PARTITIONED BY (p)" if partitioned else ""
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, p STRING) USING iceberg{part} "
        "TBLPROPERTIES ('format-version'='2')"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return table


def _live_rows(engine: Any, table: str) -> list[tuple[Any, Any]]:
    """Read ``id, p`` ordered by id from the live engine."""
    arrow = engine.arrow_of(engine.session.sql(f"SELECT id, p FROM {table} ORDER BY id"))
    return [(row["id"], row["p"]) for row in arrow.to_pylist()]


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_spark_matches_fixture(tmp_path: Path) -> None:
    """Live Spark replays the small cells and equals the recorded fixture.

    pins: ice-dyn-overwrite-1/C-018
    """
    import _live_parity as lp

    fixture = _fixture()
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

    assert fixture["meta"]["gav"] == ICEBERG_SPARK_RUNTIME_GAV
    warehouse = tmp_path / "spark-warehouse"
    engine = lp.build_spark_iceberg_engine(warehouse, catalog="dynow_live")
    session = engine.session
    session.sql("CREATE NAMESPACE IF NOT EXISTS dynow_live.ns")
    session.conf.set(_KEY, "dynamic")
    table = _live_fresh(session, "live1")
    session.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _live_rows(engine, table) == _expect(fixture["cells"]["dynamic_sql"])
    table = _live_fresh(session, "live2")
    source = session.sql("SELECT CAST(30 AS BIGINT) AS id, 'c' AS p")
    source.write.mode("overwrite").insertInto(table)
    assert _live_rows(engine, table) == _expect(fixture["cells"]["dynamic_insertInto"])
    table = _live_fresh(session, "live3")
    session.sql(f"INSERT OVERWRITE {table} SELECT CAST(20 AS BIGINT) AS id, 'b' AS p WHERE false")
    assert _live_rows(engine, table) == _expect(fixture["cells"]["dynamic_sql_empty"])
    table = _live_fresh(session, "live4", partitioned=False)
    session.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _live_rows(engine, table) == _expect(fixture["cells"]["dynamic_sql_unpartitioned"])
    table = _live_fresh(session, "live5")
    source = session.sql("SELECT CAST(40 AS BIGINT) AS id, 'c' AS p")
    source.write.mode("overwrite").saveAsTable(table)
    assert _live_rows(engine, table) == _expect(fixture["cells"]["dynamic_saveAsTable"])
    session.conf.set(_KEY, "static")
    table = _live_fresh(session, "live6")
    session.sql(f"INSERT OVERWRITE {table} SELECT 20 AS id, 'b' AS p")
    assert _live_rows(engine, table) == _expect(fixture["cells"]["static_sql"])
