"""ICE-PROCS-ROUTE-1 pins: four Iceberg maintenance procedures answer as Spark.

Replays the measured Spark oracle (``ice_procs_route_1_spark_oracle.json``,
PySpark 4.1.2 + Iceberg 1.11.0, re-derived by
``_record_ice_procs_route_1_oracle.py``) against RePark: ``ancestors_of``,
``compute_table_stats``, ``compute_partition_stats`` and
``rewrite_table_path`` — output schemas, rows with snapshot ids resolved to
commit-order positions, metadata entries, file contents, and every recorded
refusal shape. Snapshot ids are random per run, so the suite resolves them
against its own snapshot log. Every procedure cell runs on the facade door;
the native door has no CALL surface by design, so each cell also pins the
native refusal.

The live tier (``REPARK_PARITY_LIVE=1``) re-derives the oracle on live Spark
and fails naming the first cell that differs from the committed fixture.

Error-class map: Spark raises procedure-layer validations as
``IllegalArgumentException``. RePark surfaces the same validations as
``IllegalArgumentException`` with the same message; missing or wrong-typed
routine arguments surface as ``AnalysisException``.

pins: ice-procs-route-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: ice-procs-route-1/C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark.errors import IllegalArgumentException, UnsupportedOperationException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live re-derivation skipped (CI is JVM-free)"
FIXTURE: list[dict[str, Any]] = json.loads(
    Path(__file__).with_name("ice_procs_route_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in FIXTURE}
CATALOG = "sc"
NAMESPACE = "ns"
TABLE = "sc.ns.t"

_EXC: dict[str, type[BaseException]] = {
    "IllegalArgumentException": IllegalArgumentException,
}


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with memory catalog and namespace for one cell."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-procs-route-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def seed_three(session: Any) -> list[int]:
    """Create the shared partitioned seed, return its three snapshot ids."""
    session.sql(
        f"CREATE TABLE {TABLE} (id BIGINT, data STRING, cat STRING) "
        "USING iceberg PARTITIONED BY (cat)"
    )
    session.sql(f"INSERT INTO {TABLE} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')")
    session.sql(f"INSERT INTO {TABLE} VALUES (3, 'c', 'x'), (7, 'g', 'x')")
    session.sql(f"INSERT INTO {TABLE} VALUES (4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')")
    return snapshot_log(session)


def snapshot_log(session: Any) -> list[int]:
    """Return commit-ordered snapshot ids for the shared table."""
    arrow = session.sql(
        f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at, snapshot_id"
    ).to_arrow()
    return [int(value) for value in arrow.column("snapshot_id").to_pylist()]


def snapshot_parents(session: Any) -> dict[int, int | None]:
    """Map each snapshot id to its parent id for the shared table."""
    arrow = session.sql(f"SELECT snapshot_id, parent_id FROM {TABLE}.snapshots").to_arrow()
    ids = [int(value) for value in arrow.column("snapshot_id").to_pylist()]
    parents = [
        None if value is None else int(value) for value in arrow.column("parent_id").to_pylist()
    ]
    return dict(zip(ids, parents, strict=True))


def label_map(ids: list[int]) -> dict[int, str]:
    """Label commit-ordered snapshot ids S0, S1, ... like the oracle."""
    return {sid: f"S{pos}" for pos, sid in enumerate(ids)}


def ancestor_chain(parents: dict[int, int | None], head: int) -> list[int]:
    """Walk the parent chain from head down to the root snapshot."""
    chain = [head]
    while parents[chain[-1]] is not None:
        parent = parents[chain[-1]]
        assert parent is not None
        chain.append(parent)
    return chain


def table_dir(warehouse: Path, name: str) -> Path:
    """Locate the table directory holding metadata for the named table."""
    hits = [p.parent for p in warehouse.rglob("metadata") if p.is_dir() and p.parent.name == name]
    assert hits, f"no table dir for {name} under {warehouse}"
    return max(
        hits,
        key=lambda p: max(
            (q.stat().st_mtime_ns for q in (p / "metadata").glob("*.metadata.json")),
            default=0,
        ),
    )


def table_metadata(warehouse: Path, name: str) -> dict[str, Any]:
    """Parse the newest metadata.json for the named table."""
    directory = table_dir(warehouse, name)
    files = list((directory / "metadata").glob("*.metadata.json"))
    newest = max(files, key=lambda p: (p.stat().st_mtime_ns, p.name))
    return json.loads(newest.read_text(encoding="utf-8"))


def newest_metadata_name(warehouse: Path, name: str) -> str:
    """Return the newest metadata.json basename for the named table."""
    directory = table_dir(warehouse, name)
    files = list((directory / "metadata").glob("*.metadata.json"))
    return max(files, key=lambda p: (p.stat().st_mtime_ns, p.name)).name


def table_location(warehouse: Path, name: str) -> str:
    """Return the table location without a trailing slash."""
    return str(table_metadata(warehouse, name).get("location", "")).rstrip("/")


def snapshot_order(metadata: dict[str, Any]) -> dict[int, int]:
    """Map snapshot ids to commit-order positions like the oracle."""
    ordered = sorted(
        metadata.get("snapshots", []),
        key=lambda s: (s["timestamp-ms"], s["snapshot-id"]),
    )
    return {s["snapshot-id"]: i for i, s in enumerate(ordered)}


def blob_row(order: dict[int, int], blob: dict[str, Any]) -> list[Any]:
    """Normalize one statistics blob with snapshot positions like the oracle."""
    return [
        blob.get("type"),
        blob.get("fields", []),
        order.get(blob.get("snapshot-id")),
        blob.get("sequence-number"),
        [[key, value] for key, value in sorted((blob.get("properties") or {}).items())],
    ]


def statistics_entries(metadata: dict[str, Any]) -> list[list[list[Any]]]:
    """Normalize statistics entries with snapshot positions like the oracle."""
    order = snapshot_order(metadata)
    out = []
    for entry in metadata.get("statistics", []):
        fields = {
            "snapshot": order.get(entry.get("snapshot-id")),
            "blobs": sorted(
                [blob_row(order, b) for b in entry.get("blob-metadata", [])],
                key=repr,
            ),
            "size>0": entry.get("file-size-in-bytes", 0) > 0,
            "footer>0": entry.get("file-footer-size-in-bytes", 0) > 0,
        }
        out.append([[key, fields[key]] for key, _ in sorted(fields.items(), key=repr)])
    return sorted(out, key=lambda pairs: repr(dict(pairs).get("snapshot")))


_SPARK_TO_ARROW: dict[str, str] = {"bigint": "int64", "int": "int32", "string": "string"}
_CANON_UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
_STAGING_ID = re.compile(r"copy-table-staging-(?:<uuid>|[0-9a-fA-F-]+)")


def check_columns(arrow: Any, cell_id: str) -> None:
    """Assert the CALL result schema matches the oracle columns and types."""
    want = [[name, _SPARK_TO_ARROW[kind]] for name, kind in CELLS[cell_id]["obs"]["cols"]]
    assert [[f.name, str(f.type)] for f in arrow.schema] == want


def check_refusal(session: Any, sql: str, cell_id: str, needle: str | None = None) -> None:
    """Assert a CALL refuses with the recorded class and whole message."""
    error = CELLS[cell_id]["error"]
    assert error["type"] in _EXC, f"unmapped oracle exception {error['type']!r}"
    with pytest.raises(_EXC[error["type"]]) as caught:
        session.sql(sql).to_arrow()
    want = needle or str(CELLS[cell_id]["error"]["msg"])
    assert want in str(caught.value), f"{sql}: {want!r} not in {caught.value}"


def check_native_refused(sql: str) -> None:
    """Assert the native door refuses CALL with its documented message."""
    with pytest.raises(UnsupportedOperationException, match="Unsupported SQL statement"):
        repark.sql(sql).to_arrow()


def check_ancestors(session: Any, warehouse: Path, sql: str, cell: str, head: int) -> None:
    """Assert ancestors_of columns, newest-first symbolic rows, timestamps."""
    arrow = session.sql(sql).to_arrow()
    check_columns(arrow, cell)
    ids = snapshot_log(session)
    labels = label_map(ids)
    chain = ancestor_chain(snapshot_parents(session), ids[head])
    stamps = {
        s["snapshot-id"]: s["timestamp-ms"]
        for s in table_metadata(warehouse, "t").get("snapshots", [])
    }
    rows = arrow.to_pylist()
    assert [int(r["snapshot_id"]) for r in rows] == chain
    symbolic = [
        [labels[int(r["snapshot_id"])], bool(int(r["timestamp"]) == stamps[int(r["snapshot_id"])])]
        for r in rows
    ]
    assert symbolic == CELLS[cell]["obs"]["rows"]
    check_native_refused(sql)


def test_ancestors_default(spark: Any, tmp_path: Path) -> None:
    """ancestors_of(table) walks S2, S1, S0 newest first. pins: ice-procs-route-1/C-004"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.ancestors_of('ns.t')"
    check_ancestors(spark, tmp_path / "wh", sql, "QP-ANC-DEFAULT", 2)


def test_ancestors_snapshot_id(spark: Any, tmp_path: Path) -> None:
    """ancestors_of(table, snapshot_id) walks from S1. pins: ice-procs-route-1/C-004"""
    ids = seed_three(spark)
    sql = f"CALL {CATALOG}.system.ancestors_of(table => 'ns.t', snapshot_id => {ids[1]})"
    check_ancestors(spark, tmp_path / "wh", sql, "QP-ANC-ID", 1)


def test_ancestors_rollback(spark: Any, tmp_path: Path) -> None:
    """ancestors_of after rollback plus commit walks S3, S0. pins: ice-procs-route-1/C-004"""
    ids = seed_three(spark)
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('ns.t', {ids[0]})").to_arrow()
    spark.sql(f"INSERT INTO {TABLE} VALUES (9, 'i', 'y')").to_arrow()
    sql = f"CALL {CATALOG}.system.ancestors_of('ns.t')"
    check_ancestors(spark, tmp_path / "wh", sql, "QP-ANC-ROLLBACK", 3)


def test_ancestors_branch(spark: Any, tmp_path: Path) -> None:
    """ancestors_of a branch head walks its own chain. pins: ice-procs-route-1/C-004"""
    ids = seed_three(spark)
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH b AS OF VERSION {ids[0]}").to_arrow()
    spark.sql(f"INSERT INTO {TABLE}.branch_b VALUES (9, 'i', 'y')").to_arrow()
    head = spark.sql(f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'b'").to_arrow()
    branch_id = int(head.column("snapshot_id").to_pylist()[0])
    sql = f"CALL {CATALOG}.system.ancestors_of(table => 'ns.t', snapshot_id => {branch_id})"
    check_ancestors(spark, tmp_path / "wh", sql, "QP-ANC-BRANCH", 3)


def test_ancestors_empty(spark: Any) -> None:
    """ancestors_of with no snapshot raises not-found. pins: ice-procs-route-1/C-005"""
    spark.sql(f"CREATE TABLE {TABLE} (id BIGINT) USING iceberg").to_arrow()
    check_refusal(spark, f"CALL {CATALOG}.system.ancestors_of('ns.t')", "QP-ANC-EMPTY")


def test_ancestors_missing_id(spark: Any) -> None:
    """ancestors_of unknown id raises Cannot find snapshot: 12345. pins: ice-procs-route-1/C-005"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.ancestors_of(table => 'ns.t', snapshot_id => 12345)"
    check_refusal(spark, sql, "QP-ANC-MISSING-ID")


def test_ancestors_positional_id(spark: Any, tmp_path: Path) -> None:
    """ancestors_of positional snapshot id walks the full chain. pins: ice-procs-route-1/C-004"""
    ids = seed_three(spark)
    sql = f"CALL {CATALOG}.system.ancestors_of('ns.t', {ids[2]})"
    check_ancestors(spark, tmp_path / "wh", sql, "QP-ANC-POSITIONAL-ID", 2)


def statistics_order(metadata: dict[str, Any]) -> list[list[Any]]:
    """Return per-entry blob field ids in stored order with snapshot positions."""
    order = snapshot_order(metadata)
    out = []
    for entry in metadata.get("statistics", []):
        out.append(
            [
                order.get(entry.get("snapshot-id")),
                [blob.get("fields", []) for blob in entry.get("blob-metadata", [])],
            ]
        )
    return sorted(out, key=repr)


def stats_path_label(path: str, metadata: dict[str, Any]) -> str:
    """Normalize a statistics path with snapshot and uuid markers like the oracle."""
    ordered = sorted(
        metadata.get("snapshots", []), key=lambda s: (s["timestamp-ms"], s["snapshot-id"])
    )
    out = re.sub(r"^.*/metadata/", "metadata/", path)
    out = out.replace(str(ordered[0]["snapshot-id"]), "<s0>")
    out = out.replace(str(metadata.get("current-snapshot-id")), "<cur>")
    return _CANON_UUID.sub("<uuid>", out)


def check_table_stats(session: Any, warehouse: Path, sql: str, cell: str) -> None:
    """Assert compute_table_stats path registration and statistics entries."""
    arrow = session.sql(sql).to_arrow()
    check_columns(arrow, cell)
    rows = arrow.to_pylist()
    assert len(rows) == len(CELLS[cell]["obs"]["out-rel"]) == 1
    metadata = table_metadata(warehouse, "t")
    assert metadata.get("statistics", [])
    registered = metadata["statistics"][-1]["statistics-path"]
    assert rows[0]["statistics_file"] == registered
    assert registered.startswith(table_location(warehouse, "t") + "/metadata/")
    assert stats_path_label(registered, metadata) == CELLS[cell]["obs"]["out-rel"][0]
    assert statistics_entries(metadata) == CELLS[cell]["obs"]["statistics"]
    assert statistics_order(metadata) == CELLS[cell]["obs"]["statistics-order"]
    check_native_refused(sql)


def test_table_stats_default(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats(table) stats every top-level column. pins: ice-procs-route-1/C-006"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t')"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-DEFAULT")


def test_table_stats_snapshot(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats over S0 pins the first snapshot only. pins: ice-procs-route-1/C-006"""
    ids = seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', snapshot_id => {ids[0]})"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-SNAPSHOT")


def test_table_stats_columns(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats columns keeps only id. pins: ice-procs-route-1/C-007"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', columns => array('id'))"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-COLUMNS")


def test_table_stats_columns_two(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats reversed columns keep caller order. pins: ice-procs-route-1/C-007"""
    seed_three(spark)
    sql = (
        f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', "
        "columns => array('data', 'id'))"
    )
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-COLUMNS-TWO")


def test_table_stats_unknown_column(spark: Any) -> None:
    """compute_table_stats unknown column raises Can't find column. pins: ice-procs-route-1/C-008"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', columns => array('nope'))"
    check_refusal(spark, sql, "QP-CTS-COLUMNS-UNKNOWN")


def test_table_stats_empty_array(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats empty columns refuses. pins: ice-procs-route-1/C-008"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', columns => array())"
    check_refusal(spark, sql, "QP-CTS-EMPTY-ARRAY")
    assert table_metadata(tmp_path / "wh", "t").get("statistics", []) == []


def seed_nested(spark: Any) -> None:
    """Create the struct seed table for nested column shapes."""
    spark.sql(
        f"CREATE TABLE {TABLE} (id BIGINT, st STRUCT<a: INT, b: STRING>) USING iceberg"
    ).to_arrow()
    spark.sql(
        f"INSERT INTO {TABLE} VALUES (1, named_struct('a', 1, 'b', 'x')), "
        "(2, named_struct('a', 2, 'b', 'y'))"
    ).to_arrow()


@pytest.mark.xfail(
    strict=True, reason="fork ComputeTableStats has no nested scan projection; see ledger R-005"
)
def test_table_stats_nested_name(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats st.a resolves to the nested field id. pins: ice-procs-route-1/C-007"""
    seed_nested(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', columns => array('st.a'))"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-NESTED-NAME")


def test_table_stats_struct_arg(spark: Any) -> None:
    """compute_table_stats struct column refuses as non-primitive. pins: ice-procs-route-1/C-008"""
    seed_nested(spark)
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', columns => array('st'))"
    check_refusal(spark, sql, "QP-CTS-STRUCT-ARG")


def test_table_stats_duplicate(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats duplicate column dedupes to one blob. pins: ice-procs-route-1/C-007"""
    seed_three(spark)
    sql = (
        f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t', columns => array('id', 'id'))"
    )
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-DUP")


def test_table_stats_types(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats covers every primitive type, exact ndv. pins: ice-procs-route-1/C-009"""
    spark.sql(
        f"CREATE TABLE {TABLE} (i INT, l BIGINT, d DOUBLE, s STRING, dt DATE, "
        "ts TIMESTAMP, b BOOLEAN, dec DECIMAL(10,2)) USING iceberg"
    ).to_arrow()
    spark.sql(
        f"INSERT INTO {TABLE} VALUES (1, 1, 1.5, 'a', DATE'2024-01-01', "
        "TIMESTAMP'2024-01-01 00:00:00', true, 1.25), (2, 2, 2.5, 'b', "
        "DATE'2024-01-02', TIMESTAMP'2024-01-02 00:00:00', false, 2.50), "
        "(2, 3, NULL, NULL, NULL, NULL, NULL, NULL)"
    ).to_arrow()
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t')"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-TYPES")


def test_table_stats_empty(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats on an empty table answers zero rows. pins: ice-procs-route-1/C-010"""
    spark.sql(f"CREATE TABLE {TABLE} (id BIGINT) USING iceberg").to_arrow()
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t')"
    arrow = spark.sql(sql).to_arrow()
    check_columns(arrow, "QP-CTS-EMPTY")
    assert arrow.num_rows == 0
    assert table_metadata(tmp_path / "wh", "t").get("statistics", []) == []
    check_native_refused(sql)


def test_table_stats_nested(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats default skips struct columns, keeps id. pins: ice-procs-route-1/C-009"""
    spark.sql(
        f"CREATE TABLE {TABLE} (id BIGINT, st STRUCT<a: INT, b: STRING>) USING iceberg"
    ).to_arrow()
    spark.sql(
        f"INSERT INTO {TABLE} VALUES (1, named_struct('a', 1, 'b', 'x')), "
        "(2, named_struct('a', 2, 'b', 'y'))"
    ).to_arrow()
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t')"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-NESTED")


def test_table_stats_twice(spark: Any, tmp_path: Path) -> None:
    """compute_table_stats second run registers again. pins: ice-procs-route-1/C-006"""
    seed_three(spark)
    spark.sql(f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t')").to_arrow()
    sql = f"CALL {CATALOG}.system.compute_table_stats(table => 'ns.t')"
    check_table_stats(spark, tmp_path / "wh", sql, "QP-CTS-TWICE")


def norm_value(value: Any) -> Any:
    """Normalize nested structs to sorted pair lists like the harness."""
    if isinstance(value, dict):
        return sorted([[key, norm_value(item)] for key, item in value.items()], key=repr)
    if isinstance(value, list):
        return [norm_value(item) for item in value]
    return value


def partition_path_label(path: Any, metadata: dict[str, Any]) -> Any:
    """Normalize a partition-statistics path with snapshot and uuid markers."""
    if path is None:
        return None
    ordered = sorted(
        metadata.get("snapshots", []), key=lambda s: (s["timestamp-ms"], s["snapshot-id"])
    )
    out = re.sub(r"^.*/metadata/", "metadata/", str(path))
    out = _CANON_UUID.sub("<uuid>", out)
    out = out.replace(str(metadata.get("current-snapshot-id")), "<cur>")
    return out.replace(str(ordered[0]["snapshot-id"]), "<s0>")


def check_partition_stats(session: Any, warehouse: Path, sql: str, cell: str) -> None:
    """Assert compute_partition_stats file registration, entry, contents."""
    import pyarrow.parquet as parquet

    arrow = session.sql(sql).to_arrow()
    check_columns(arrow, cell)
    rows = arrow.to_pylist()
    assert len(rows) == len(CELLS[cell]["obs"]["out-rel"]) == 1
    metadata = table_metadata(warehouse, "t")
    entries = metadata.get("partition-statistics", [])
    assert len(entries) == 1
    assert rows[0]["partition_statistics_file"] == entries[0]["statistics-path"]
    assert (
        partition_path_label(entries[0]["statistics-path"], metadata)
        == CELLS[cell]["obs"]["out-rel"][0]
    )
    order = snapshot_order(metadata)
    assert CELLS[cell]["obs"]["partition-statistics"] == [[order[entries[0]["snapshot-id"]], True]]
    drop = {"last_updated_at", "last_updated_snapshot_id", "total_data_file_size_in_bytes"}
    table = parquet.read_table(entries[0]["statistics-path"].replace("file:", ""))
    dicts = sorted(
        [{k: v for k, v in r.items() if k not in drop} for r in table.to_pylist()],
        key=repr,
    )
    got = [table.schema.names, [norm_value(d) for d in dicts]]
    assert got == CELLS[cell]["obs"]["contents"][0]
    check_native_refused(sql)


def test_partition_stats_default(spark: Any, tmp_path: Path) -> None:
    """compute_partition_stats(table) writes per-partition counts. pins: ice-procs-route-1/C-011"""
    seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_partition_stats(table => 'ns.t')"
    check_partition_stats(spark, tmp_path / "wh", sql, "QP-CPS-DEFAULT")


def test_partition_stats_snapshot(spark: Any, tmp_path: Path) -> None:
    """compute_partition_stats over S0 counts the first snapshot. pins: ice-procs-route-1/C-011"""
    ids = seed_three(spark)
    sql = f"CALL {CATALOG}.system.compute_partition_stats(table => 'ns.t', snapshot_id => {ids[0]})"
    check_partition_stats(spark, tmp_path / "wh", sql, "QP-CPS-SNAPSHOT")


def test_partition_stats_unpartitioned(spark: Any) -> None:
    """compute_partition_stats on an unpartitioned table raises. pins: ice-procs-route-1/C-012"""
    spark.sql(f"CREATE TABLE {TABLE} (id BIGINT, data STRING, cat STRING) USING iceberg").to_arrow()
    spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'a', 'x')").to_arrow()
    sql = f"CALL {CATALOG}.system.compute_partition_stats(table => 'ns.t')"
    check_refusal(spark, sql, "QP-CPS-UNPARTITIONED")


def test_partition_stats_v3(spark: Any, tmp_path: Path) -> None:
    """compute_partition_stats on v3 adds dv_count. pins: ice-procs-route-1/C-011"""
    spark.sql(
        f"CREATE TABLE {TABLE} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')"
    ).to_arrow()
    spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')").to_arrow()
    spark.sql(f"INSERT INTO {TABLE} VALUES (3, 'c', 'x'), (7, 'g', 'x')").to_arrow()
    spark.sql(f"INSERT INTO {TABLE} VALUES (4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')").to_arrow()
    sql = f"CALL {CATALOG}.system.compute_partition_stats(table => 'ns.t')"
    check_partition_stats(spark, tmp_path / "wh", sql, "QP-CPS-V3")


def mark_rewrite_side(side: str, source: str, target: str, staging: str) -> str:
    """Replace rewrite path prefixes and the staging id with stable markers."""
    out = side
    for prefix, marker in ((source, "<src>"), (target, "<dst>"), (staging, "<stg>")):
        if prefix and (out == prefix or out.startswith(prefix + "/")):
            out = marker + out[len(prefix) :]
            break
    return _STAGING_ID.sub("copy-table-staging-<id>", out)


def rewrite_line_shape(line: str, source: str, target: str, staging: str) -> list[str]:
    """Reduce one file-list line to marked dirs plus the basename kind."""
    left, _, right = line.partition(",")
    marked = [mark_rewrite_side(side, source, target, staging) for side in (left, right)]
    name = left.rpartition("/")[2]
    staged = marked[0].startswith("<stg>/") or "copy-table-staging-<id>/" in marked[0]
    if name.endswith(".parquet"):
        kind = "deletes" if staged else "data"
    elif name.endswith("-m0.avro"):
        kind = "manifest"
    elif name.startswith("snap-") and name.endswith(".avro"):
        kind = "snap"
    elif re.fullmatch(r"v[0-9]+\.metadata\.json", name):
        kind = "version"
    elif name.endswith(".metadata.json"):
        kind = "rewritten"
    else:
        kind = "other:" + name
    return [marked[0].rpartition("/")[0], marked[1].rpartition("/")[0], kind]


def check_rewrite_path(
    session: Any, warehouse: Path, sql: str, cell: str, source: str, target: str, staging: str
) -> None:
    """Assert rewrite_table_path rows, file list, and staged metadata."""
    pre_newest = newest_metadata_name(warehouse, "t")
    arrow = session.sql(sql).to_arrow()
    check_columns(arrow, cell)
    rows = arrow.to_pylist()
    assert len(rows) == 1
    row = rows[0]
    want_rows = CELLS[cell]["obs"]["rows"][0]
    assert [
        row["rewritten_manifest_file_paths_count"],
        row["rewritten_delete_file_paths_count"],
    ] == want_rows[2:]
    assert row["latest_version"] == pre_newest
    assert re.fullmatch(r"[0-9]{5}-[0-9a-fA-F-]+\.metadata\.json", row["latest_version"]), (
        "the memory catalog names versions NNNNN-<uuid>.metadata.json where "
        f"the oracle Hadoop door names them {want_rows[0]}"
    )
    listed = row["file_list_location"]
    if listed == "N/A":
        assert want_rows[1] == "N/A"
        check_native_refused(sql)
        return
    assert listed.startswith(staging) and listed.endswith("/file-list")
    actual_staging = listed[: -len("/file-list")]
    file_list_token = mark_rewrite_side(listed, source, target, actual_staging)
    assert file_list_token == want_rows[1].replace("<uuid>", "<id>"), file_list_token
    lines = Path(listed.replace("file:", "")).read_text(encoding="utf-8").splitlines()
    assert lines, "file list is empty"
    staged_metadata: list[str] = []
    for line in lines:
        parts = line.split(",")
        assert len(parts) == 2, f"file-list line has no src,target pair: {line!r}"
        left, right = parts
        assert left.startswith(source) or (staging and left.startswith(staging)), line
        assert right.startswith(target), line
        assert Path(left.replace("file:", "")).name == Path(right.replace("file:", "")).name
        if left.endswith(".metadata.json"):
            staged_metadata.append(left)
    live_shapes = sorted(rewrite_line_shape(line, source, target, actual_staging) for line in lines)
    want_shapes = sorted(
        rewrite_line_shape(line, "<src>", "<dst>", "<stg>")
        for line in CELLS[cell]["obs"]["file-list"]
    )
    assert [s for s in live_shapes if s[2] not in ("rewritten", "version")] == [
        s for s in want_shapes if s[2] not in ("rewritten", "version")
    ]
    live_meta = [s for s in live_shapes if s[2] in ("rewritten", "version")]
    want_meta = [s for s in want_shapes if s[2] in ("rewritten", "version")]
    assert len(live_meta) == 1 and live_meta[0][2] == "rewritten", live_meta
    assert re.fullmatch(r"v[0-9]+\.metadata\.json", want_rows[0]) is not None, want_rows[0]
    assert len(want_meta) == int(want_rows[0][1:].split(".")[0]), (want_meta, want_rows[0])
    assert all(s[2] == "version" for s in want_meta), want_meta
    assert staged_metadata, "file list names no staged metadata.json"
    staged_doc = json.loads(
        Path(staged_metadata[-1].replace("file:", "")).read_text(encoding="utf-8")
    )
    assert staged_doc.get("location") == target
    assert (
        mark_rewrite_side(staged_doc.get("location"), source, target, actual_staging)
        == CELLS[cell]["obs"]["staged-metadata-location"]
    )
    check_native_refused(sql)


def test_rewrite_path_default(spark: Any, tmp_path: Path) -> None:
    """rewrite_table_path stages under metadata with a file list. pins: ice-procs-route-1/C-013"""
    warehouse = tmp_path / "wh"
    seed_three(spark)
    source = table_location(warehouse, "t")
    target = str(tmp_path / "dst")
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        f"source_prefix => '{source}', target_prefix => '{target}')"
    )
    check_rewrite_path(
        spark,
        warehouse,
        sql,
        "QP-RTP-DEFAULT",
        source,
        target,
        source + "/metadata/copy-table-staging-",
    )


def test_rewrite_path_staging(spark: Any, tmp_path: Path) -> None:
    """rewrite_table_path honors an explicit staging_location. pins: ice-procs-route-1/C-013"""
    warehouse = tmp_path / "wh"
    seed_three(spark)
    source = table_location(warehouse, "t")
    target = str(tmp_path / "dst")
    staging = str(tmp_path / "stg")
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        f"source_prefix => '{source}', target_prefix => '{target}', "
        f"staging_location => '{staging}')"
    )
    check_rewrite_path(spark, warehouse, sql, "QP-RTP-STAGING", source, target, staging)


def test_rewrite_path_no_file_list(spark: Any, tmp_path: Path) -> None:
    """rewrite_table_path create_file_list false answers N/A. pins: ice-procs-route-1/C-014"""
    warehouse = tmp_path / "wh"
    seed_three(spark)
    source = table_location(warehouse, "t")
    target = str(tmp_path / "dst")
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        f"source_prefix => '{source}', target_prefix => '{target}', create_file_list => false)"
    )
    check_rewrite_path(spark, warehouse, sql, "QP-RTP-NO-FILE-LIST", source, target, "")


def test_rewrite_path_missing_prefix(spark: Any) -> None:
    """rewrite_table_path wrong prefix raises not-start-with. pins: ice-procs-route-1/C-015"""
    seed_three(spark)
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        "source_prefix => '/nope', target_prefix => '/x')"
    )
    check_refusal(spark, sql, "QP-RTP-MISSING-PREFIX-ERR", "does not start with /nope/")


@pytest.mark.xfail(strict=True, reason="fork RewriteTablePath has no version range; see ledger")
def test_rewrite_path_end_version(spark: Any, tmp_path: Path) -> None:
    """rewrite_table_path end_version rewrites up to v3 only. pins: ice-procs-route-1/C-016"""
    warehouse = tmp_path / "wh"
    seed_three(spark)
    source = table_location(warehouse, "t")
    target = str(tmp_path / "dst")
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        f"source_prefix => '{source}', target_prefix => '{target}', "
        "end_version => 'v3.metadata.json')"
    )
    check_rewrite_path(spark, warehouse, sql, "QP-RTP-END-VERSION", source, target, source)


@pytest.mark.xfail(strict=True, reason="fork RewriteTablePath has no version range; see ledger")
def test_rewrite_path_start_version(spark: Any, tmp_path: Path) -> None:
    """rewrite_table_path start_version rewrites from v2 only. pins: ice-procs-route-1/C-016"""
    warehouse = tmp_path / "wh"
    seed_three(spark)
    source = table_location(warehouse, "t")
    target = str(tmp_path / "dst")
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        f"source_prefix => '{source}', target_prefix => '{target}', "
        "start_version => 'v2.metadata.json')"
    )
    check_rewrite_path(spark, warehouse, sql, "QP-RTP-START-VERSION", source, target, source)


def test_rewrite_path_mor_deletes(spark: Any, tmp_path: Path) -> None:
    """rewrite_table_path rewrites one position-delete path on MoR. pins: ice-procs-route-1/C-013"""
    warehouse = tmp_path / "wh"
    seed_three(spark)
    spark.sql(
        f"ALTER TABLE {TABLE} SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')"
    ).to_arrow()
    spark.sql(f"DELETE FROM {TABLE} WHERE id = 1").to_arrow()
    source = table_location(warehouse, "t")
    target = str(tmp_path / "dst")
    sql = (
        f"CALL {CATALOG}.system.rewrite_table_path(table => 'ns.t', "
        f"source_prefix => '{source}', target_prefix => '{target}')"
    )
    check_rewrite_path(spark, warehouse, sql, "QP-RTP-MOR-DELETES", source, target, source)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_matches_recorded(tmp_path: Path) -> None:
    """Live Spark re-derivation matches the committed oracle. pins: ice-procs-route-1/C-003"""
    pytest.importorskip("pyspark")
    from _record_ice_procs_route_1_oracle import check_oracle

    check_oracle(tmp_path / "live-wh", FIXTURE)
