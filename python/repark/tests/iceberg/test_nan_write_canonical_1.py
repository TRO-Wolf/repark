from __future__ import annotations

import struct
import uuid
from pathlib import Path
from typing import Any

import pytest
from test_v3_live_oracle import _LIVE, _LIVE_SKIP, _v37_iceberg_runtime_jar

from repark import ReparkSession

_RP = "nanwrite_rp"
_NS = "ns"
_CANONICAL = ("7ff8000000000000", "7fc00000")
_PAYLOADS = (
    ("payload", "7ff8deadbeef0001", "7ff8002460000000", "7fc00123"),
    ("negpayload", "fff8deadbeef0001", "fff8002460000000", "ffc00123"),
)
_VALUES = (
    ("-1.0", "-1.0"),
    ("NaN", "NaN"),
    ("Infinity", "Infinity"),
    ("-Infinity", "-Infinity"),
    ("-0.0", "-0.0"),
    ("0.0", "0.0"),
    ("1.5", "1.5"),
)
_ROWS = (
    (1, "sqrt(x)", "CAST(sqrt(y) AS FLOAT)", "-1.0"),
    (2, "-x", "-y", "NaN"),
    (3, "x - x", "y - y", "Infinity"),
    (4, "CAST('NaN' AS DOUBLE)", "CAST('NaN' AS FLOAT)", "1.5"),
    (5, "x", "y", "payload"),
    (6, "x", "y", "negpayload"),
    (7, "x", "y", "-0.0"),
    (8, "x", "y", "0.0"),
    (9, "x", "y", "Infinity"),
    (10, "x", "y", "-Infinity"),
    (11, "x", "y", "1.5"),
)
_ONE_ZERO_CELLS = {("avro", "insert")}
_ONE_ZERO_ROW = (8, "x + 2.5", "y", "0.0")
_ONE_ZERO_CONTROL = ("4004000000000000", "00000000")
_NAN_IDS = [1, 2, 3, 4, 5, 6]
_CONTROLS = {
    7: ("8000000000000000", "80000000"),
    8: ("0000000000000000", "00000000"),
    9: ("7ff0000000000000", "7f800000"),
    10: ("fff0000000000000", "ff800000"),
    11: ("3ff8000000000000", "3fc00000"),
}
_RAW = {
    1: ("fff8000000000000", "ffc00000"),
    2: ("fff8000000000000", "ffc00000"),
    3: ("fff8000000000000", "ffc00000"),
    4: _CANONICAL,
    5: ("7ff8deadbeef0001", "7fc00123"),
    6: ("fff8deadbeef0001", "ffc00123"),
}
_CELLS = (
    ("parquet", "insert"),
    ("parquet", "ctas"),
    ("parquet", "writeto"),
    ("parquet", "saveastable"),
    ("parquet", "merge"),
    ("parquet", "update"),
    ("orc", "insert"),
    ("orc", "ctas"),
    ("avro", "insert"),
    ("avro", "ctas"),
)


def _double(text: str) -> float:
    return struct.unpack(">d", bytes.fromhex(text))[0]


def _bits(value: float | None, width: int) -> str | None:
    if value is None:
        return None
    return struct.pack(">f" if width == 4 else ">d", value).hex()


def _source(session: Any) -> None:
    payload = [(key, _double(d), _double(f)) for key, d, f, _ in _PAYLOADS]
    session.createDataFrame(payload, schema="k STRING, x DOUBLE, y FLOAT").createOrReplaceTempView(
        "nanwrite_payload"
    )
    values = ", ".join(f"('{k}', CAST('{x}' AS DOUBLE), CAST('{x}' AS FLOAT))" for k, x in _VALUES)
    session.sql(
        f"SELECT k, x, y FROM (VALUES {values}) AS v(k, x, y) "
        "UNION ALL SELECT k, x, y FROM nanwrite_payload"
    ).createOrReplaceTempView("nanwrite_src")


def _rows(fmt: str, door: str) -> str:
    one_zero = (fmt, door) in _ONE_ZERO_CELLS
    rows = [_ONE_ZERO_ROW if one_zero and row[0] == _ONE_ZERO_ROW[0] else row for row in _ROWS]
    return " UNION ALL ".join(
        f"SELECT CAST({i} AS BIGINT) AS id, CAST({d} AS DOUBLE) AS d, CAST({f} AS FLOAT) AS f "
        f"FROM nanwrite_src WHERE k = '{k}'"
        for i, d, f, k in rows
    )


def _controls(fmt: str, door: str) -> dict[int, tuple[str, str]]:
    if (fmt, door) not in _ONE_ZERO_CELLS:
        return _CONTROLS
    return {**_CONTROLS, _ONE_ZERO_ROW[0]: _ONE_ZERO_CONTROL}


def _props(fmt: str, mode: str = "copy-on-write") -> str:
    return (
        f"TBLPROPERTIES ('format-version' = '2', 'write.format.default' = '{fmt}', "
        f"'write.update.mode' = '{mode}')"
    )


def _write(session: Any, table: str, fmt: str, door: str, rows: str) -> None:
    create = f"CREATE TABLE {table} (id BIGINT, d DOUBLE, f FLOAT) USING iceberg PARTITIONED BY (d)"
    if door == "ctas":
        head = create.split(" (")[0]
        session.sql(f"{head} USING iceberg PARTITIONED BY (d) {_props(fmt)} AS {rows}").collect()
        return
    session.sql(f"{create} {_props(fmt)}").collect()
    if door == "insert":
        session.sql(f"INSERT INTO {table} {rows}").collect()
    elif door == "writeto":
        session.sql(rows).writeTo(table).append()
    elif door == "saveastable":
        session.sql(rows).write.format("iceberg").mode("overwrite").partitionBy("d").saveAsTable(
            table
        )
    elif door == "merge":
        session.sql(
            f"MERGE INTO {table} t USING ({rows}) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
        ).collect()
    else:
        raw = rows.replace("CAST(sqrt(x) AS DOUBLE)", "CAST(x AS DOUBLE)")
        session.sql(f"INSERT INTO {table} {raw}").collect()
        session.sql(
            f"UPDATE {table} SET d = sqrt(d), f = CAST(sqrt(f) AS FLOAT) WHERE id = 1"
        ).collect()


def _file_bits(path: Path) -> dict[int, tuple[str | None, str | None]]:
    if path.suffix == ".orc":
        import pyarrow.orc as po

        table = po.ORCFile(str(path)).read()
    else:
        import pyarrow.parquet as pq

        table = pq.ParquetFile(path).read()
    ids = table.column("id").to_pylist()
    doubles = table.column("d").to_pylist()
    floats = table.column("f").to_pylist()
    return {i: (_bits(d, 8), _bits(f, 4)) for i, d, f in zip(ids, doubles, floats, strict=True)}


def _stored(root: Path, session: Any, table: str) -> dict[int, tuple[str | None, str | None]]:
    live = {
        Path(row[0]).name for row in session.sql(f"SELECT file_path FROM {table}.files").collect()
    }
    out: dict[int, tuple[str | None, str | None]] = {}
    for path in root.rglob("*"):
        if path.name in live and path.suffix in (".parquet", ".orc"):
            out.update(_file_bits(path))
    if not out:
        rows = session.sql(f"SELECT id, d, f FROM {table}").collect()
        out = {row[0]: (_bits(row[1], 8), _bits(row[2], 4)) for row in rows}
    return out


def _answers(session: Any, root: Path, table: str) -> dict[str, Any]:
    files = session.sql(f"SELECT partition, record_count FROM {table}.files").collect()
    nan_parts = [row for row in files if row[0]["d"] is not None and row[0]["d"] != row[0]["d"]]
    ids = "SELECT id FROM {t} WHERE {c} ORDER BY id"
    return {
        "stored": _stored(root, session, table),
        "nan_partition": sorted({_bits(row[0]["d"], 8) for row in nan_parts}),
        "nan_partition_records": sorted(row[1] for row in nan_parts),
        "eq_nan": [
            r[0] for r in session.sql(ids.format(t=table, c="d = CAST('NaN' AS DOUBLE)")).collect()
        ],
        "isnan": [r[0] for r in session.sql(ids.format(t=table, c="isnan(d)")).collect()],
    }


def _avro_f_answers(session: Any, table: str) -> dict[str, list[int]]:
    ids = "SELECT id FROM {t} WHERE {c} ORDER BY id"
    return {
        name: [r[0] for r in session.sql(ids.format(t=table, c=cond)).collect()]
        for name, cond in (
            ("eq_nan", "f = CAST('NaN' AS FLOAT)"),
            ("gt_inf", "f > CAST('Infinity' AS FLOAT)"),
        )
    }


def _name(catalog: str, fmt: str, door: str) -> str:
    return f"{catalog}.{_NS}.t_{fmt}_{door}"


@pytest.fixture(scope="module")
def repark_side(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("nan_write_canonical_1")
    session = (
        ReparkSession.builder.appName("nan-write-canonical-1")
        .config(f"spark.sql.catalog.{_RP}.type", "hadoop")
        .config(f"spark.sql.catalog.{_RP}.warehouse", str(root / "repark"))
        .getOrCreate()
    )
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_RP}.{_NS}").collect()
        _source(session)
        out: dict[Any, Any] = {"root": root}
        for fmt, door in _CELLS:
            table = _name(_RP, fmt, door)
            _write(session, table, fmt, door, _rows(fmt, door))
            out[(fmt, door)] = _answers(
                session, root / "repark" / _NS / table.split(".")[-1], table
            )
        out["avro_f"] = _avro_f_answers(session, _name(_RP, "avro", "insert"))
        return out
    finally:
        session.stop()


@pytest.mark.parametrize(("fmt", "door"), _CELLS)
def test_repark_stores_nan_as_spark_does(repark_side: dict[str, Any], fmt: str, door: str) -> None:
    got = repark_side[(fmt, door)]
    for row_id in _NAN_IDS:
        expected = (_CANONICAL[0], _RAW[row_id][1]) if fmt == "avro" else _CANONICAL
        assert got["stored"][row_id] == expected, (row_id, got["stored"][row_id])
    for row_id, bits in _controls(fmt, door).items():
        assert got["stored"][row_id] == bits
    assert got["nan_partition"] == [_CANONICAL[0]]
    assert got["nan_partition_records"] == ([1, 5] if door == "update" else [len(_NAN_IDS)])
    assert got["eq_nan"] == _NAN_IDS
    assert got["isnan"] == _NAN_IDS


@pytest.mark.xfail(
    strict=True,
    reason=(
        "NAN-COMPARE-SPARK-1: RePark compares a stored Avro NaN by its bits; "
        "Spark finds every NaN row"
    ),
)
def test_repark_filters_stored_avro_nan_as_spark_does(repark_side: dict[str, Any]) -> None:
    assert repark_side["avro_f"] == {"eq_nan": _NAN_IDS, "gt_inf": _NAN_IDS}


def _live_spark(warehouse: Path) -> tuple[Any, str]:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("nan-write-canonical-1-live")
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
    catalog = f"nanwrite_sp_{uuid.uuid4().hex[:8]}"
    session.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
    session.conf.set(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    return session, catalog


@pytest.fixture(scope="module")
def spark_side(repark_side: dict[str, Any]) -> dict[Any, Any]:
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    warehouse = repark_side["root"] / "spark"
    spark, catalog = _live_spark(warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{_NS}")
    _source(spark)
    out: dict[Any, Any] = {}
    for fmt, door in _CELLS:
        table = _name(catalog, fmt, door)
        _write(spark, table, fmt, door, _rows(fmt, door))
        out[(fmt, door)] = _answers(spark, warehouse / _NS / table.split(".")[-1], table)
    return out


@pytest.mark.parametrize(("fmt", "door"), _CELLS)
def test_repark_table_equals_spark_table(
    repark_side: dict[str, Any], spark_side: dict[Any, Any], fmt: str, door: str
) -> None:
    assert repark_side[(fmt, door)] == spark_side[(fmt, door)]
