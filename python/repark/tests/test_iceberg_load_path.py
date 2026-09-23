"""R-DF-LOAD-PATH / R-DF-LOAD-METADATA-JSON — ``format("iceberg").load(<path>)`` reads.

Spark's IcebergSource rule: a ``load`` argument containing ``/`` is a filesystem path —
a ``*.metadata.json`` path pins that metadata file's snapshot, and any other path is a
table location whose current metadata resolves through ``version-hint.text`` or the
highest-numbered metadata file under ``<location>/metadata``. Near-miss identifiers keep
the catalog route untouched. Arrow ``to_arrow`` pins carry value AND type (docs/testing.md).

pins: dfload-1/C-001, C-002, C-004, C-005, C-006
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException

TABLE = "mem.ns.events"
EXPECTED_CURRENT = [[2, "b", "y"], [3, "c", "x"]]
EXPECTED_PRE_DELETE = [[1, "a", "z"], [2, "b", "y"], [3, "c", "x"]]
PATH_OPTION_REFUSAL = (
    "format('iceberg').load(<path>) reads one pinned metadata snapshot and does not "
    "support time-travel or incremental options; got snapshot-id"
)
SNAPSHOT_ID_REFUSAL = (
    "Time travel option `snapshot-id` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with a ``mem`` memory catalog and the ``mem.ns`` namespace."""
    session = ReparkSession.builder.appName("pytest-iceberg-load-path").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


@pytest.fixture
def loaded(spark: ReparkSession, tmp_path: Path) -> dict[str, object]:
    """The fixture table (insert three rows, delete one) and its metadata file list."""
    spark.sql(
        f"CREATE TABLE {TABLE} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'a', 'z'), (2, 'b', 'y'), (3, 'c', 'x')")
    spark.sql(f"DELETE FROM {TABLE} WHERE id = 1")
    table_dir = _table_dir(tmp_path)
    metadata_files = sorted((table_dir / "metadata").glob("*.metadata.json"))
    assert len(metadata_files) >= 3, (
        f"expected create+insert+delete metadata files under {table_dir / 'metadata'}"
    )
    return {"table_dir": table_dir, "metadata_files": metadata_files}


def _table_dir(root: Path) -> Path:
    """Return the ``events`` table directory discovered under the warehouse root."""
    hits = [p.parent for p in root.rglob("metadata") if p.parent.name == "events"]
    assert hits, f"no events table directory under {root}"
    return hits[0]


def _pin_schema(table: pa.Table) -> None:
    """Pin the Arrow schema: BIGINT ``id``, STRING ``data`` / ``cat`` (value AND type)."""
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("data").type == pa.string()
    assert table.schema.field("cat").type == pa.string()


def _sorted_rows(table: pa.Table) -> list[list[object]]:
    """Sorted full-row multiset for the ``(id, data, cat)`` fixture table."""
    rows = table.select(["id", "data", "cat"]).to_pylist()
    return sorted([int(row["id"]), str(row["data"]), str(row["cat"])] for row in rows)


def _assert_current(frame: object) -> None:
    """Pin the current-snapshot rows and schema of a loaded DataFrame."""
    arrow = frame.to_arrow()
    _pin_schema(arrow)
    assert _sorted_rows(arrow) == EXPECTED_CURRENT


def test_load_table_location_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load(<table location>)`` answers the current snapshot like Spark.

    pins: dfload-1/C-001
    """
    frame = spark.read.format("iceberg").load(str(loaded["table_dir"]))
    _assert_current(frame)


def test_load_table_location_trailing_slash(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """One trailing ``/`` on the location resolves the same current snapshot.

    pins: dfload-1/C-001
    """
    frame = spark.read.format("iceberg").load(f"{loaded['table_dir']}/")
    _assert_current(frame)


def test_load_latest_metadata_file_reads_current_rows(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load(<latest metadata.json>)`` reads that file's snapshot (the current one).

    pins: dfload-1/C-002
    """
    latest = loaded["metadata_files"][-1]
    frame = spark.read.format("iceberg").load(str(latest))
    _assert_current(frame)


def test_load_older_metadata_file_reads_that_version(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load(<an older metadata.json>)`` answers the pre-delete three-row snapshot.

    pins: dfload-1/C-002
    """
    pre_delete = loaded["metadata_files"][-2]
    frame = spark.read.format("iceberg").load(str(pre_delete))
    arrow = frame.to_arrow()
    _pin_schema(arrow)
    assert _sorted_rows(arrow) == EXPECTED_PRE_DELETE


def test_load_path_with_time_travel_option_refuses(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """Time-travel or incremental options with a path refuse loud, never silently read.

    pins: dfload-1/C-004
    """
    with pytest.raises(AnalysisException, match=re.escape(PATH_OPTION_REFUSAL)):
        spark.read.format("iceberg").option("snapshot-id", "1").load(str(loaded["table_dir"]))


def test_load_missing_location_names_the_path(spark: ReparkSession, tmp_path: Path) -> None:
    """A location with no resolvable metadata raises AnalysisException naming the path.

    pins: dfload-1/C-005
    """
    missing = str(tmp_path / "no" / "such" / "dir")
    with pytest.raises(AnalysisException, match=re.escape(missing)):
        spark.read.format("iceberg").load(missing)


def test_load_two_part_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("ns.events")`` resolves under the current catalog, never the path branch.

    pins: dfload-1/C-006
    """
    frame = spark.read.format("iceberg").load("ns.events")
    _assert_current(frame)


def test_load_three_part_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("mem.ns.events")`` keeps the catalog route.

    pins: dfload-1/C-006
    """
    frame = spark.read.format("iceberg").load("mem.ns.events")
    _assert_current(frame)


def test_load_metadata_table_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("ns.events.snapshots")`` keeps the catalog route's measured refusal.

    pins: dfload-1/C-006
    """
    with pytest.raises(AnalysisException, match=r"table 'ns\.events\.snapshots' not found"):
        spark.read.format("iceberg").load("ns.events.snapshots")


def test_load_quoted_identifier_keeps_catalog_route(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``load("`ns`.`events`")`` parses as a quoted identifier, not a path.

    pins: dfload-1/C-006
    """
    frame = spark.read.format("iceberg").load("`ns`.`events`")
    _assert_current(frame)


def test_load_identifier_snapshot_id_keeps_spark_refusal(
    spark: ReparkSession, loaded: dict[str, object]
) -> None:
    """``snapshot-id`` on a catalog identifier keeps Spark's IllegalArgumentException.

    pins: dfload-1/C-006
    """
    with pytest.raises(IllegalArgumentException, match=re.escape(SNAPSHOT_ID_REFUSAL)):
        spark.read.format("iceberg").option("snapshot-id", "1").load("ns.events")
