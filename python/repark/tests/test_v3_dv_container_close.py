"""RP-7 — the shared-Puffin deletion-vector container close, repark against live Spark."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pyarrow as pa
import pytest
from test_v3_live_oracle import (
    _ALLOW_CREATE_V3_KEY,
    _LIVE,
    _LIVE_SKIP,
    _v37_iceberg_runtime_jar,
)

_DV_COLUMNS = (
    "SELECT referenced_data_file, file_path, content_offset, record_count FROM {table}.delete_files"
)
_SHARED_PUFFIN_ROWS = [(3, "c", 0), (4, "d", 1), (6, "f", 1)]
_SHARED_PUFFIN_SEED = [(1, "a", 0), (2, "b", 0), (3, "c", 0), (4, "d", 1), (5, "e", 1), (6, "f", 1)]
_SHARED_PUFFIN_MOR = "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'"


def _dv_entries(arrow: pa.Table) -> list[tuple[str, str, int, int]]:
    """Return ``(referenced, container, offset, records)`` per live DV, ordered by referenced."""
    referenced = arrow.column("referenced_data_file").to_pylist()
    container = arrow.column("file_path").to_pylist()
    offset = arrow.column("content_offset").to_pylist()
    records = arrow.column("record_count").to_pylist()
    rows = [
        (str(ref).rsplit("/", 1)[-1], str(path), int(off), int(count))
        for ref, path, off, count in zip(referenced, container, offset, records, strict=True)
    ]
    rows.sort()
    return rows


def _dv_close_shape(before: list, after: list, rows: list) -> dict:
    """Engine-independent shape of a shared-Puffin close: what moved, what stayed."""
    was = {entry[0]: entry[1:] for entry in before}
    now = {entry[0]: entry[1:] for entry in after}
    touched = [ref for ref, entry in now.items() if entry[2] == 2]
    sibling = [ref for ref, entry in now.items() if entry[2] == 1]
    assert len(touched) == 1 and len(sibling) == 1, (before, after)
    touched, sibling = touched[0], sibling[0]
    return {
        "before_containers": len({entry[1] for entry in before}),
        "after_containers": len({entry[1] for entry in after}),
        "touched_moved": now[touched][0] != was[touched][0],
        "touched_offset": now[touched][1],
        "touched_records": now[touched][2],
        "sibling_unchanged": now[sibling] == was[sibling],
        "sibling_records": now[sibling][2],
        "rows": rows,
    }


def test_v3_shared_puffin_container_close_live(tmp_path: Path) -> None:
    """A second DELETE rewrites only the touched blob and leaves the sibling entry put."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("rp-7-shared-puffin-live")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")
        session.sql(
            "CREATE TABLE ice.sales.partdv (id INT, name STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({_SHARED_PUFFIN_MOR})"
        )
        seed = ", ".join(f"({row[0]}, '{row[1]}', {row[2]})" for row in _SHARED_PUFFIN_SEED)
        session.sql(f"INSERT INTO ice.sales.partdv VALUES {seed}").collect()
        session.sql("DELETE FROM ice.sales.partdv WHERE id IN (2, 5)").collect()
        before = _dv_entries(session.sql(_DV_COLUMNS.format(table="ice.sales.partdv")).to_arrow())
        session.sql("DELETE FROM ice.sales.partdv WHERE id = 1").collect()
        after = _dv_entries(session.sql(_DV_COLUMNS.format(table="ice.sales.partdv")).to_arrow())
        rows = [
            (row["id"], row["name"], row["part"])
            for row in session.sql("SELECT id, name, part FROM ice.sales.partdv ORDER BY id")
            .to_arrow()
            .to_pylist()
        ]
        shape = _dv_close_shape(before, after, rows)
    finally:
        session.stop()
    assert shape["rows"] == _SHARED_PUFFIN_ROWS
    assert shape["after_containers"] == 2
    assert shape["touched_moved"]
    assert shape["sibling_unchanged"]
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    assert shape == _live_shared_puffin_close_shape()


def _live_shared_puffin_close_shape() -> dict:
    """Live Spark running the same two DELETE statements over the partitioned v3 MoR seed."""
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-rp7-shared-puffin-"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("rp-7-shared-puffin-live")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    jar = _v37_iceberg_runtime_jar()
    if jar is not None:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    target = f"{catalog}.sales.partdv"
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        session.sql(
            f"CREATE TABLE {target} (id INT, name STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({_SHARED_PUFFIN_MOR})"
        )
        values = ", ".join(f"({row[0]}, '{row[1]}', {row[2]})" for row in _SHARED_PUFFIN_SEED)
        session.sql(f"INSERT INTO {target} VALUES {values}")
        session.sql(f"DELETE FROM {target} WHERE id IN (2, 5)")
        before = _dv_entries(session.sql(_DV_COLUMNS.format(table=target)).toArrow())
        session.sql(f"DELETE FROM {target} WHERE id = 1")
        after = _dv_entries(session.sql(_DV_COLUMNS.format(table=target)).toArrow())
        rows = [
            (row["id"], row["name"], row["part"])
            for row in session.sql(f"SELECT id, name, part FROM {target} ORDER BY id")
            .toArrow()
            .to_pylist()
        ]
        return _dv_close_shape(before, after, rows)
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


_MATRIX_SEED = "(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')"
_MATRIX_SOURCES = (
    ("src", "id INT", "(2), (5)"),
    ("src2", "id INT", "(6)"),
    ("srcnull", "id INT", "(2), (CAST(NULL AS INT))"),
    ("srcbig", "id BIGINT", "(CAST(2 AS BIGINT)), (CAST(5 AS BIGINT))"),
    ("k", "id INT", "(2), (5)"),
)
_MATRIX_CELLS = (
    (
        "shadow_exists_alias",
        "DELETE FROM {t} s WHERE EXISTS (SELECT 1 FROM {ns}.src s WHERE s.id = s.id)",
        [],
    ),
    (
        "shadow_exists_bare",
        "DELETE FROM {t} k WHERE EXISTS (SELECT 1 FROM {ns}.k WHERE k.id = k.id)",
        [],
    ),
    (
        "filtered_in",
        "DELETE FROM {t} WHERE id IN (SELECT id FROM {ns}.src WHERE id > 4)",
        [1, 2, 3, 4, 6],
    ),
    (
        "empty_source",
        "DELETE FROM {t} WHERE id IN (SELECT id FROM {ns}.srcempty)",
        [1, 2, 3, 4, 5, 6],
    ),
    (
        "null_source_keys",
        "DELETE FROM {t} WHERE id IN (SELECT id FROM {ns}.srcnull)",
        [1, 3, 4, 5, 6],
    ),
    (
        "projection_alias",
        "DELETE FROM {t} WHERE id IN (SELECT id AS key FROM {ns}.src)",
        [1, 3, 4, 6],
    ),
    (
        "int_vs_bigint",
        "DELETE FROM {t} WHERE id IN (SELECT id FROM {ns}.srcbig)",
        [1, 3, 4, 6],
    ),
    (
        "correlated_exists",
        "DELETE FROM {t} t WHERE EXISTS (SELECT 1 FROM {ns}.src s WHERE s.id = t.id)",
        [1, 3, 4, 6],
    ),
    (
        "plus_one",
        "DELETE FROM {t} t WHERE EXISTS (SELECT 1 FROM {ns}.src s WHERE t.id = s.id + 1)",
        [1, 2, 4, 5],
    ),
    (
        "and_filter",
        "DELETE FROM {t} t WHERE EXISTS (SELECT 1 FROM {ns}.src s WHERE s.id = t.id AND s.id > 1)",
        [1, 3, 4, 6],
    ),
)


def _seed_matrix(run, namespace: str) -> None:
    """Create the five matrix source tables plus the empty one."""
    for name, columns, values in _MATRIX_SOURCES:
        run(f"CREATE TABLE {namespace}.{name} ({columns}) USING iceberg")
        run(f"INSERT INTO {namespace}.{name} VALUES {values}")
    run(f"CREATE TABLE {namespace}.srcempty (id INT) USING iceberg")


def _matrix_answers(run, ids_of, namespace: str) -> dict[str, list[int]]:
    """Run every matrix cell on a fresh v3 merge-on-read target and return the survivors."""
    answers = {}
    for cell, template, _expected in _MATRIX_CELLS:
        target = f"{namespace}.t_{cell}"
        run(
            f"CREATE TABLE {target} (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ({_SHARED_PUFFIN_MOR})"
        )
        run(f"INSERT INTO {target} VALUES {_MATRIX_SEED}")
        run(template.format(t=target, ns=namespace))
        answers[cell] = ids_of(target)
    return answers


def test_v3_subquery_dml_matrix_matches_spark(tmp_path: Path) -> None:
    """Owner resolution is exact: a shadowed alias pushes no residual and deletes every row."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("rp-7-subquery-matrix")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", tmp_path)
        session.sql("CREATE NAMESPACE ice.sales")

        def run(sql: str) -> None:
            session.sql(sql).collect()

        def ids_of(target: str) -> list[int]:
            return (
                session.sql(f"SELECT id FROM {target} ORDER BY id")
                .to_arrow()
                .column("id")
                .to_pylist()
            )

        _seed_matrix(run, "ice.sales")
        answers = _matrix_answers(run, ids_of, "ice.sales")
    finally:
        session.stop()
    for cell, _template, expected in _MATRIX_CELLS:
        assert answers[cell] == expected, cell
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    assert answers == _live_subquery_dml_matrix()


def _live_subquery_dml_matrix() -> dict[str, list[int]]:
    """Live Spark running the same twelve-cell matrix over the same seed."""
    import tempfile

    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    catalog = "local"
    warehouse = Path(tempfile.mkdtemp(prefix="repark-rp7-matrix-"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("rp-7-subquery-matrix")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    jar = _v37_iceberg_runtime_jar()
    if jar is not None:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    namespace = f"{catalog}.sales"
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        def run(sql: str) -> None:
            session.sql(sql)

        def ids_of(target: str) -> list[int]:
            return [
                row["id"] for row in session.sql(f"SELECT id FROM {target} ORDER BY id").collect()
            ]

        _seed_matrix(run, namespace)
        return _matrix_answers(run, ids_of, namespace)
    finally:
        session.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
