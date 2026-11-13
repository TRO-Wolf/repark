from __future__ import annotations

import math
import uuid
from pathlib import Path
from typing import Any

import pytest
from test_v3_live_oracle import _LIVE, _LIVE_SKIP, _v37_iceberg_runtime_jar

from repark import ReparkSession

_RP = "nanpart_rp"
_ADOPT = "nanpart_adopt"
_NS = "ns"
_TYPES = ("DOUBLE", "FLOAT")
_DOORS = ("insert", "ctas", "writeto", "saveastable")
_ROWS = (
    "(1, CAST(0.0 AS {ty})), (2, CAST(-0.0 AS {ty})), (3, CAST('NaN' AS {ty})), "
    "(4, CAST(NULL AS {ty})), (5, CAST(1.5 AS {ty}))"
)
_SOURCE = "SELECT CAST(id AS BIGINT) AS id, d FROM (VALUES {rows}) AS t(id, d)"
_INF_ROWS = "(6, CAST('Infinity' AS DOUBLE)), (7, CAST('-Infinity' AS DOUBLE))"
_CHECKS = {
    "rows": "SELECT id, d FROM {t} ORDER BY id",
    "isnan": "SELECT id FROM {t} WHERE isnan(d) ORDER BY id",
    "eq_nan": "SELECT id FROM {t} WHERE d = CAST('NaN' AS {ty}) ORDER BY id",
    "gt_one": "SELECT id FROM {t} WHERE d > 1.0 ORDER BY id",
    "is_null": "SELECT id FROM {t} WHERE d IS NULL ORDER BY id",
    "count": "SELECT count(*) FROM {t}",
}
_FILES = "SELECT partition, record_count FROM {t}.files"
_ROWS_ANSWER = [[1, 0.0], [2, 0.0], [3, "NaN"], [4, None], [5, 1.5]]
_FILES_ANSWER = sorted([[None, 1], [0.0, 2], [1.5, 1], ["NaN", 1]], key=repr)


def _norm(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, float) and math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if hasattr(value, "asDict"):
        return _norm(value.asDict())
    if isinstance(value, dict):
        return {key: _norm(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    return value


def _collect(session: Any, text: str) -> list[Any]:
    return [_norm(list(row)) for row in session.sql(text).collect()]


def _files(session: Any, table: str) -> list[Any]:
    rows = _collect(session, _FILES.format(t=table))
    return sorted(([partition["d"], count] for partition, count in rows), key=repr)


def _answers(session: Any, table: str, ty: str, partitioned: bool = True) -> dict[str, Any]:
    out = {name: _collect(session, text.format(t=table, ty=ty)) for name, text in _CHECKS.items()}
    if partitioned:
        out["files"] = _files(session, table)
    return out


def _table(catalog: str, ty: str, door: str) -> str:
    return f"{catalog}.{_NS}.p_{ty.lower()}_{door}"


def _write(session: ReparkSession, ty: str, door: str) -> None:
    name = _table(_RP, ty, door)
    source = _SOURCE.format(rows=_ROWS.format(ty=ty))
    if door == "ctas":
        session.sql(f"CREATE TABLE {name} USING iceberg PARTITIONED BY (d) AS {source}").collect()
        return
    session.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg PARTITIONED BY (d)")
    if door == "insert":
        session.sql(f"INSERT INTO {name} VALUES {_ROWS.format(ty=ty)}").collect()
    elif door == "writeto":
        session.sql(source).writeTo(name).append()
    else:
        session.sql(source).write.format("iceberg").mode("append").saveAsTable(name)


def _write_controls(session: ReparkSession) -> None:
    for ty, rows in (("INT", "(1, 7), (2, NULL)"), ("STRING", "(1, 'a'), (2, NULL)")):
        name = _table(_RP, ty, "control")
        session.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg PARTITIONED BY (d)")
        session.sql(f"INSERT INTO {name} VALUES {rows}").collect()
    flat = _table(_RP, "DOUBLE", "flat")
    session.sql(f"CREATE TABLE {flat} (id BIGINT, d DOUBLE) USING iceberg")
    session.sql(f"INSERT INTO {flat} VALUES {_ROWS.format(ty='DOUBLE')}").collect()
    inf = _table(_RP, "DOUBLE", "inf")
    session.sql(f"CREATE TABLE {inf} (id BIGINT, d DOUBLE) USING iceberg PARTITIONED BY (d)")
    session.sql(f"INSERT INTO {inf} VALUES {_INF_ROWS}").collect()


def _repark_session(root: Path) -> ReparkSession:
    return (
        ReparkSession.builder.appName("nan-partition-value-1")
        .config(f"spark.sql.catalog.{_RP}.type", "hadoop")
        .config(f"spark.sql.catalog.{_RP}.warehouse", str(root / "repark"))
        .config(f"spark.sql.catalog.{_ADOPT}.type", "hadoop")
        .config(f"spark.sql.catalog.{_ADOPT}.warehouse", str(root / "adopt"))
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def repark_side(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("nan_partition_value_1")
    session = _repark_session(root)
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_RP}.{_NS}").collect()
        own: dict[tuple[str, str], dict[str, Any]] = {}
        for ty in _TYPES:
            for door in _DOORS:
                _write(session, ty, door)
                own[(ty, door)] = _answers(session, _table(_RP, ty, door), ty)
        _write_controls(session)
        inf = _table(_RP, "DOUBLE", "inf")
        flat = _table(_RP, "DOUBLE", "flat")
        return {
            "root": root,
            "own": own,
            "inf_files": _files(session, inf),
            "inf_rows": _collect(session, f"SELECT id, d FROM {inf} ORDER BY id"),
            "int_files": _files(session, _table(_RP, "INT", "control")),
            "string_files": _files(session, _table(_RP, "STRING", "control")),
            "flat": _answers(session, flat, "DOUBLE", partitioned=False),
        }
    finally:
        session.stop()


@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("ty", _TYPES)
def test_repark_stores_and_reads_its_nan_partition(
    repark_side: dict[str, Any], ty: str, door: str
) -> None:
    got = repark_side["own"][(ty, door)]
    assert got["files"] == _FILES_ANSWER
    assert got["isnan"] == [[3]]
    assert got["eq_nan"] == [[3]]
    assert got["rows"] == _ROWS_ANSWER
    assert got["count"] == [[5]]


def test_repark_stores_infinite_partitions(repark_side: dict[str, Any]) -> None:
    assert repark_side["inf_files"] == sorted([["Infinity", 1], ["-Infinity", 1]], key=repr)
    assert repark_side["inf_rows"] == [[6, "Infinity"], [7, "-Infinity"]]


def test_controls_keep_their_partitions(repark_side: dict[str, Any]) -> None:
    assert repark_side["int_files"] == sorted([[7, 1], [None, 1]], key=repr)
    assert repark_side["string_files"] == sorted([["a", 1], [None, 1]], key=repr)
    assert repark_side["flat"]["isnan"] == [[3]]
    assert repark_side["flat"]["gt_one"] == [[3], [5]]


def _live_spark(spark_wh: Path, repark_wh: Path) -> tuple[Any, str, str]:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("nan-partition-value-1-live")
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
    suffix = uuid.uuid4().hex[:8]
    own, cross = f"nanpart_sp_{suffix}", f"nanpart_spx_{suffix}"
    for catalog, warehouse in ((own, spark_wh), (cross, repark_wh)):
        session.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        session.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
        session.conf.set(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    return session, own, cross


def _repark_reads_spark(root: Path, tables: dict[str, str]) -> dict[str, Any]:
    session = _repark_session(root)
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_ADOPT}.{_NS}").collect()
        out = {}
        for ty, table in tables.items():
            metadata = max(
                (root / "spark" / _NS / table / "metadata").glob("v*.metadata.json"),
                key=lambda path: int(path.name[1:].split(".")[0]),
            )
            session.sql(
                f"CALL {_ADOPT}.system.register_table("
                f"table => '{_NS}.{table}', metadata_file => '{metadata}')"
            ).collect()
            out[ty] = _answers(session, f"{_ADOPT}.{_NS}.{table}", ty)
        return out
    finally:
        session.stop()


@pytest.fixture(scope="module")
def spark_side(repark_side: dict[str, Any]) -> dict[str, Any]:
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    root = repark_side["root"]
    spark, own, cross = _live_spark(root / "spark", root / "repark")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {own}.{_NS}")
    answers: dict[str, Any] = {"own": {}, "cross": {}}
    for ty in _TYPES:
        name = _table(own, ty, "insert")
        spark.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg PARTITIONED BY (d)")
        spark.sql(f"INSERT INTO {name} VALUES {_ROWS.format(ty=ty)}")
        answers["own"][ty] = _answers(spark, name, ty)
        for door in _DOORS:
            answers["cross"][(ty, door)] = _answers(spark, _table(cross, ty, door), ty)
    inf = _table(own, "DOUBLE", "inf")
    spark.sql(f"CREATE TABLE {inf} (id BIGINT, d DOUBLE) USING iceberg PARTITIONED BY (d)")
    spark.sql(f"INSERT INTO {inf} VALUES {_INF_ROWS}")
    answers["own"]["inf"] = _files(spark, inf)
    answers["cross"]["inf"] = _files(spark, _table(cross, "DOUBLE", "inf"))
    answers["read"] = _repark_reads_spark(root, {ty: f"p_{ty.lower()}_insert" for ty in _TYPES})
    return answers


@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("ty", _TYPES)
def test_spark_reads_repark_nan_partition_as_its_own(
    spark_side: dict[str, Any], ty: str, door: str
) -> None:
    assert spark_side["cross"][(ty, door)] == spark_side["own"][ty]


def test_spark_reads_repark_infinite_partitions_as_its_own(spark_side: dict[str, Any]) -> None:
    assert spark_side["cross"]["inf"] == spark_side["own"]["inf"]


@pytest.mark.parametrize("ty", _TYPES)
def test_repark_reads_spark_nan_partition_as_spark(spark_side: dict[str, Any], ty: str) -> None:
    assert spark_side["read"][ty] == spark_side["own"][ty]
