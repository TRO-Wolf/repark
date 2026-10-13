"""ICE-TSNS-FRAME-DOOR-1: a pandas or polars nanosecond timestamp column enters as microseconds.

pins: ice-tsns-frame-door-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
pins: ice-tsns-frame-door-1/C-012, C-014
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _record_ice_tsns_frame_door_1 as recorder
import pandas as pd
import pyarrow as pa
import pytest

from repark import ReparkSession

SPARK: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_tsns_frame_door_1_spark.json").read_text(encoding="utf-8")
)["cells"]
SOURCES = (*recorder.AWARE_SOURCES, *recorder.NAIVE_SOURCES)
SCHEMAS = recorder.SCHEMAS
SEED_MICROS = recorder.SEED_MICROS
PRE_EPOCH_MICROS = list(recorder.PRE_EPOCH_MICROS)
NANOS_UTC = pa.timestamp("ns", tz="UTC")
MICROS_UTC = pa.timestamp("us", tz="UTC")
MAIN_UNTOUCHED: dict[str, dict[str, Any]] = {
    "us": {"arrow": "timestamp[us]", "nullable": True, "values": [SEED_MICROS, None]},
    "us_utc": {"arrow": "timestamp[us, tz=UTC]", "nullable": True, "values": [SEED_MICROS, None]},
    "ms": {"arrow": "timestamp[ms]", "nullable": True, "values": [1767323045123, None]},
    "s": {"arrow": "timestamp[s]", "nullable": True, "values": [1767323045, None]},
    "date": {"arrow": "date32[day]", "nullable": True, "values": [20455, None]},
    "int": {"arrow": "int64", "nullable": True, "values": [1, 2]},
    "string": {"arrow": "string", "nullable": True, "values": ["a", None]},
    "decimal": {
        "arrow": "decimal128(38, 18)",
        "nullable": True,
        "values": ["1.500000000000000000", None],
    },
    "list_us": {
        "arrow": "list<item: timestamp[us, tz=UTC]>",
        "nullable": True,
        "values": [[SEED_MICROS], None],
    },
}
MAIN_DECLARED_TIMESTAMP: dict[str, dict[str, list[Any]]] = {
    "UTC": {
        "pd_naive_ns": [1767323045123456, None],
        "pd_utc_ns": [1767323045123456, None],
        "pd_ny_ns": [1767323045123456, None],
        "pd_naive_ns_pre_epoch": [-1, -2, -1000000000],
        "pd_utc_ns_pre_epoch": [-1, -2, -1000000000],
        "pd_arrowdtype_ns": [1767323045123456, None],
        "pd_utc_ns_notnull": [1767323045123456],
    },
    "America/New_York": {
        "pd_naive_ns": [1767341045123456, None],
        "pd_utc_ns": [1767323045123456, None],
        "pd_ny_ns": [1767323045123456, None],
        "pd_naive_ns_pre_epoch": [17999999999, 17999999998, 17000000000],
        "pd_utc_ns_pre_epoch": [-1, -2, -1000000000],
        "pd_arrowdtype_ns": [1767341045123456, None],
        "pd_utc_ns_notnull": [1767323045123456],
    },
}
MAIN_NESTED: dict[str, dict[str, Any]] = {
    "list_ns": {
        "arrow": "list<item: timestamp[ns, tz=UTC]>",
        "nullable": True,
        "values": [[recorder.SEED_NANOS], None],
    },
    "struct_ns": {
        "arrow": "struct<x: timestamp[ns, tz=UTC]>",
        "nullable": True,
        "values": [{"x": recorder.SEED_NANOS}, None],
    },
}


@pytest.fixture(params=recorder.ZONES)
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    """An ANSI session in one of the two recorded zones over a fresh warehouse."""
    session = (
        ReparkSession.builder.appName("ice-tsns-frame-door-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", request.param)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path))
    session.sql("CREATE NAMESPACE ice.ns")
    try:
        yield session
    finally:
        session.stop()


def zone_of(spark: Any) -> str:
    """Return the session zone of ``spark``."""
    return spark.conf.get("spark.sql.session.timeZone")


def ticks(column: pa.ChunkedArray) -> list[Any]:
    """Return ``column`` with every timestamp, at any depth, as its int64 ticks."""
    array = column.combine_chunks()
    kind = array.type
    if pa.types.is_timestamp(kind) or pa.types.is_date(kind):
        width = pa.int32() if pa.types.is_date32(kind) else pa.int64()
        return array.cast(width).to_pylist()
    if pa.types.is_list(kind):
        return array.cast(pa.list_(pa.int64())).to_pylist()
    if pa.types.is_struct(kind):
        return array.cast(pa.struct([(field.name, pa.int64()) for field in kind])).to_pylist()
    if pa.types.is_decimal(kind):
        return [None if cell is None else str(cell) for cell in array.to_pylist()]
    return array.to_pylist()


def answer(spark: Any, data: Any, schema: str | None = None) -> dict[str, Any]:
    """Return the Arrow type, nullability and values ``createDataFrame`` answers, or its refusal."""
    try:
        frame = (
            spark.createDataFrame(data)
            if schema is None
            else spark.createDataFrame(data, schema=schema)
        )
        table = frame.to_arrow()
    except Exception as error:
        return {"error": type(error).__name__}
    field = table.schema.field("c")
    return {
        "arrow": str(field.type),
        "nullable": field.nullable,
        "values": ticks(table.column("c")),
    }


def microsecond_twin(frame: pd.DataFrame) -> pd.DataFrame:
    """Return ``frame`` with its nanosecond column floored to microseconds by pandas."""
    series = frame["c"]
    floored = series.dt.floor("us")
    if isinstance(series.dtype, pd.ArrowDtype):
        target: Any = pd.ArrowDtype(pa.timestamp("us", tz=series.dtype.pyarrow_dtype.tz))
    elif series.dt.tz is None:
        target = "datetime64[us]"
    else:
        target = pd.DatetimeTZDtype("us", series.dt.tz)
    return pd.DataFrame({"c": floored.astype(target)})


def arrow_frame(values: list[Any], arrow_type: pa.DataType) -> pd.DataFrame:
    """Return a one-column pandas frame backed by ``arrow_type``."""
    return pd.DataFrame(
        {"c": pd.Series(pa.array(values, type=arrow_type), dtype=pd.ArrowDtype(arrow_type))}
    )


def untouched_frames() -> dict[str, pd.DataFrame]:
    """Return the frames that hold no top-level nanosecond timestamp column."""
    micros = pd.Series(pd.to_datetime([recorder.SEED, None])).astype("datetime64[us]")
    return {
        "us": pd.DataFrame({"c": micros}),
        "us_utc": pd.DataFrame({"c": micros.dt.tz_localize("UTC")}),
        "ms": pd.DataFrame({"c": micros.astype("datetime64[ms]")}),
        "s": pd.DataFrame({"c": micros.astype("datetime64[s]")}),
        "date": pd.DataFrame({"c": [dt.date(2026, 1, 2), None]}),
        "int": pd.DataFrame({"c": pd.array([1, 2], dtype="int32")}),
        "string": pd.DataFrame({"c": ["a", None]}),
        "decimal": pd.DataFrame({"c": [decimal.Decimal("1.50"), None]}),
        "list_us": arrow_frame([[SEED_MICROS], None], pa.list_(MICROS_UTC)),
    }


def nested_frames() -> dict[str, pd.DataFrame]:
    """Return the ``ArrowDtype`` frames that hold nanoseconds below the top level."""
    return {
        "list_ns": arrow_frame([[recorder.SEED_NANOS], None], pa.list_(NANOS_UTC)),
        "struct_ns": arrow_frame([{"x": recorder.SEED_NANOS}, None], pa.struct([("x", NANOS_UTC)])),
    }


def iceberg_column_type(warehouse: Path, table: str) -> str:
    """Return the Iceberg type of column ``c`` from the latest metadata JSON of ``table``."""
    paths = [
        path for path in warehouse.rglob("*.metadata.json") if path.parent.parent.name == table
    ]
    latest = max(paths, key=lambda path: int(path.name.split("-", 1)[0]))
    metadata = json.loads(latest.read_text(encoding="utf-8"))
    current = metadata["current-schema-id"]
    schema = next(item for item in metadata["schemas"] if item["schema-id"] == current)
    return next(field["type"] for field in schema["fields"] if field["name"] == "c")


def create_by_ctas(spark: Any, frame: Any, table: str) -> None:
    """CREATE TABLE AS SELECT from a temporary view of ``frame``."""
    frame.createOrReplaceTempView("frame_door_source")
    spark.sql(f"CREATE TABLE {table} USING iceberg AS SELECT * FROM frame_door_source")


def create_by_write_to(spark: Any, frame: Any, table: str) -> None:
    """``writeTo(table).create()``."""
    frame.writeTo(table).using("iceberg").create()


def create_by_save_as_table(spark: Any, frame: Any, table: str) -> None:
    """``write.saveAsTable(table)``."""
    frame.write.format("iceberg").saveAsTable(table)


CREATE_DOORS = {
    "ctas": create_by_ctas,
    "write_to_create": create_by_write_to,
    "save_as_table": create_by_save_as_table,
}


@pytest.mark.parametrize("schema", SCHEMAS, ids=["none", "timestamp", "timestamp_ntz"])
@pytest.mark.parametrize("source", SOURCES)
def test_nanosecond_column_equals_its_microsecond_twin(
    spark: Any, source: str, schema: str | None
) -> None:
    """A nanosecond column answers what the same column floored to microseconds answers."""
    frame = recorder.sources()[source]
    twin = microsecond_twin(frame)
    assert "[us" in str(twin["c"].dtype)
    got = answer(spark, frame, schema)
    assert got == answer(spark, twin, schema)
    assert "error" not in got
    assert got["arrow"].startswith("timestamp[us")
    assert got["nullable"] is True
    if schema is None and source == "pd_ny_ns":
        assert got["arrow"] == "timestamp[us, tz=America/New_York]"
    if schema is None and source in recorder.NAIVE_SOURCES:
        assert got["arrow"] == "timestamp[us]"


@pytest.mark.parametrize("schema", SCHEMAS[:2], ids=["none", "timestamp"])
@pytest.mark.parametrize("source", recorder.AWARE_SOURCES)
def test_aware_nanosecond_values_equal_the_spark_oracle(
    spark: Any, source: str, schema: str | None
) -> None:
    """An aware nanosecond column holds the instants and nullability Spark recorded."""
    oracle = SPARK[recorder.cell_key(zone_of(spark), source, schema)]
    got = answer(spark, recorder.sources()[source], schema)
    assert oracle["status"] == "ok"
    assert oracle["warnings"] == []
    assert got["values"] == oracle["ints"]
    assert got["nullable"] == oracle["nullable"]
    assert got["arrow"].startswith("timestamp[us, tz=")


def test_pre_epoch_nanoseconds_floor_as_spark_floors(spark: Any) -> None:
    """-1 ns, -1001 ns and -999999999999 ns floor to -1, -2 and -1000000000 microseconds."""
    frames = recorder.sources()
    oracle = SPARK[recorder.cell_key(zone_of(spark), "pd_utc_ns_pre_epoch", None)]
    assert oracle["ints"] == PRE_EPOCH_MICROS
    aware = answer(spark, frames["pd_utc_ns_pre_epoch"])
    assert aware == {"arrow": oracle["arrow"], "nullable": True, "values": oracle["ints"]}
    naive = answer(spark, frames["pd_naive_ns_pre_epoch"])
    assert naive == {"arrow": "timestamp[us]", "nullable": True, "values": PRE_EPOCH_MICROS}
    assert naive == answer(spark, microsecond_twin(frames["pd_naive_ns_pre_epoch"]))


@pytest.mark.parametrize("source", recorder.NAIVE_SOURCES)
def test_declared_timestamp_ntz_over_nanoseconds_answers_microseconds(
    spark: Any, source: str
) -> None:
    """``schema="c timestamp_ntz"`` over a naive nanosecond column answers Spark's cell."""
    oracle = SPARK[recorder.cell_key(zone_of(spark), source, "c timestamp_ntz")]
    got = answer(spark, recorder.sources()[source], "c timestamp_ntz")
    assert oracle["status"] == "ok"
    assert got == {
        "arrow": oracle["arrow"],
        "nullable": oracle["nullable"],
        "values": oracle["ints"],
    }


@pytest.mark.parametrize("source", SOURCES)
def test_declared_timestamp_over_nanoseconds_is_unchanged(spark: Any, source: str) -> None:
    """``schema="c timestamp"`` over a nanosecond column answers what main answers."""
    got = answer(spark, recorder.sources()[source], "c timestamp")
    values = MAIN_DECLARED_TIMESTAMP[zone_of(spark)][source]
    assert got == {"arrow": "timestamp[us, tz=UTC]", "nullable": True, "values": values}


@pytest.mark.parametrize("source", recorder.MICROSECOND_SOURCES)
def test_declared_timestamp_over_an_aware_microsecond_column_answers_utc(
    spark: Any, source: str
) -> None:
    """``schema="c timestamp"`` over a New York microsecond column answers Spark's UTC type."""
    oracle = SPARK[recorder.cell_key(zone_of(spark), source, "c timestamp")]
    frame = recorder.sources()[source]
    assert oracle["arrow"] == "timestamp[us, tz=UTC]"
    assert answer(spark, frame, "c timestamp") == {
        "arrow": oracle["arrow"],
        "nullable": oracle["nullable"],
        "values": oracle["ints"],
    }
    assert answer(spark, frame) == {
        "arrow": "timestamp[us, tz=America/New_York]",
        "nullable": True,
        "values": oracle["ints"],
    }


@pytest.mark.parametrize("name", tuple(MAIN_UNTOUCHED))
def test_columns_that_are_not_nanosecond_timestamps_are_untouched(spark: Any, name: str) -> None:
    """A column that is not a nanosecond timestamp keeps main's Arrow type and values."""
    assert answer(spark, untouched_frames()[name]) == MAIN_UNTOUCHED[name]


@pytest.mark.parametrize("name", tuple(MAIN_NESTED))
def test_nested_arrowdtype_nanoseconds_are_still_carried(spark: Any, name: str) -> None:
    """A list or struct of nanoseconds keeps its nanosecond leaf, as on main."""
    assert answer(spark, nested_frames()[name]) == MAIN_NESTED[name]


@pytest.mark.parametrize("source", SOURCES)
def test_no_warning_is_raised_for_a_nanosecond_frame(spark: Any, source: str) -> None:
    """``createDataFrame`` of a nanosecond frame raises no warning, as Spark raises none."""
    assert SPARK[recorder.cell_key(zone_of(spark), source, None)]["warnings"] == []
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        warnings.simplefilter("ignore", ResourceWarning)
        spark.createDataFrame(recorder.sources()[source]).to_arrow()


@pytest.mark.parametrize("door", tuple(CREATE_DOORS))
@pytest.mark.parametrize(
    ("source", "iceberg_type", "arrow_type"),
    [
        ("pd_naive_ns", "timestamp", pa.timestamp("us")),
        ("pd_utc_ns", "timestamptz", MICROS_UTC),
    ],
)
def test_pandas_frame_create_doors_make_microsecond_tables(
    spark: Any,
    tmp_path: Path,
    door: str,
    source: str,
    iceberg_type: str,
    arrow_type: pa.DataType,
) -> None:
    """A table created from a pandas nanosecond frame holds a microsecond column."""
    table = f"ice.ns.{door}_{source}"
    CREATE_DOORS[door](spark, spark.createDataFrame(recorder.sources()[source]), table)
    assert iceberg_column_type(tmp_path, f"{door}_{source}") == iceberg_type
    stored = spark.table(table).to_arrow().column("c")
    assert stored.type == arrow_type
    assert sorted(ticks(stored), key=lambda value: value is None) == [SEED_MICROS, None]


@pytest.mark.parametrize(
    ("source", "arrow_type", "values"),
    [
        ("pd_naive_ns", "timestamp[us]", [SEED_MICROS, None]),
        ("pd_utc_ns", "timestamp[us, tz=UTC]", [SEED_MICROS, None]),
        ("pd_naive_ns_pre_epoch", "timestamp[us]", PRE_EPOCH_MICROS),
        ("pd_utc_ns_pre_epoch", "timestamp[us, tz=UTC]", PRE_EPOCH_MICROS),
    ],
)
def test_polars_nanosecond_column_arrives_as_microseconds(
    spark: Any, source: str, arrow_type: str, values: list[Any]
) -> None:
    """A polars ``Datetime("ns")`` column answers microseconds, floored, naive and UTC."""
    polars = pytest.importorskip("polars")
    pandas_frame = recorder.sources()[source]
    frame = polars.from_pandas(pandas_frame)
    twin = polars.from_pandas(microsecond_twin(pandas_frame))
    assert (frame.schema["c"].time_unit, twin.schema["c"].time_unit) == ("ns", "us")
    got = answer(spark, frame)
    assert got == {"arrow": arrow_type, "nullable": True, "values": values}
    assert got == answer(spark, twin)
