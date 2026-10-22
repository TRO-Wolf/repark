"""ZONE-SEAM-1: a container cast of a zoned timestamp stores Spark's session wall on every door."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import repark
from repark import ReparkSession, functions
from repark.spark.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

NEW_YORK = "America/New_York"
INSTANT = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
UTC_WALL = 1_767_323_045_123_456
NEW_YORK_WALL = 1_767_305_045_123_456
NEW_YORK_MIDNIGHT = 1_767_225_600_000_000
STRUCT = "STRUCT<v: TIMESTAMP_NTZ, n: INT>"
NTZ_STRUCT = StructType([StructField("v", TimestampNTZType()), StructField("n", IntegerType())])
CONTAINER_CAST = f"CAST(named_struct('v', x, 'n', 1) AS {STRUCT})"


def open_session(zone: str, warehouse: Path) -> Any:
    """Open a facade session in ``zone`` with ``ice.ns.src`` holding one instant and one wall."""
    spark = (
        ReparkSession.builder.appName("zone-seam-1")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ) USING iceberg "
        "TBLPROPERTIES ('format-version' = '2')"
    )
    spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {INSTANT}, {WALL})")
    return spark


def create_target(spark: Any, name: str, column_type: str) -> None:
    """Create ``ice.ns.<name> (id INT, st <column_type>, k INT)``."""
    spark.sql(
        f"CREATE TABLE ice.ns.{name} (id INT, st {column_type}, k INT) USING iceberg "
        "TBLPROPERTIES ('format-version' = '2')"
    )


def leaf_ticks(column: pa.ChunkedArray) -> list[int | None]:
    """Return the int64 ticks of the first leaf of a struct, list or bare column."""
    array = column.combine_chunks()
    if pa.types.is_struct(array.type):
        array = array.field(0)
    elif pa.types.is_list(array.type) or pa.types.is_large_list(array.type):
        array = array.flatten()
    return array.cast(pa.int64()).to_pylist()


def stored(spark: Any, name: str) -> list[int | None]:
    """Return the leaf ticks of ``ice.ns.<name>.st``, nulls kept."""
    return leaf_ticks(spark.sql(f"SELECT st FROM ice.ns.{name} ORDER BY id").toArrow().column(0))


def selected(spark: Any, sql: str) -> list[int | None]:
    """Return the leaf ticks of the first column ``sql`` selects."""
    return leaf_ticks(spark.sql(sql).toArrow().column(0))


PLAIN_SELECTS: dict[str, str] = {
    "array": f"SELECT CAST(array({INSTANT}) AS ARRAY<TIMESTAMP_NTZ>) AS v",
    "struct": f"SELECT CAST(named_struct('v', {INSTANT}, 'n', 1) AS {STRUCT}) AS v",
    "map": f"SELECT CAST(map('a', {INSTANT}) AS MAP<STRING, TIMESTAMP_NTZ>)['a'] AS v",
    "leaf control": f"SELECT CAST({INSTANT} AS TIMESTAMP_NTZ) AS v",
}


@pytest.mark.parametrize("zone", [NEW_YORK, "UTC"])
@pytest.mark.parametrize("shape", list(PLAIN_SELECTS))
def test_a_plain_select_of_a_container_cast_answers_the_session_wall(
    tmp_path: Path, shape: str, zone: str
) -> None:
    """The card's plain ``SELECT`` rows answer the session wall on the Spark SQL door."""
    spark = open_session(zone, tmp_path)
    try:
        wall = NEW_YORK_WALL if zone == NEW_YORK else UTC_WALL
        assert selected(spark, PLAIN_SELECTS[shape]) == [wall]
    finally:
        spark.stop()


def test_the_dataframe_door_casts_a_container_to_the_session_wall(tmp_path: Path) -> None:
    """``Column.cast`` of a map and ``expr`` casts of a struct and an array answer the wall."""
    spark = open_session(NEW_YORK, tmp_path)
    try:
        frame = spark.sql(
            f"SELECT named_struct('v', {INSTANT}, 'n', 1) AS s, array({INSTANT}) AS a, "
            f"map('k', {INSTANT}) AS m"
        )
        cast = frame.select(
            functions.col("m").cast(MapType(StringType(), TimestampNTZType())).alias("m"),
            functions.expr(f"CAST(s AS {STRUCT})").alias("s"),
            functions.expr("CAST(a AS ARRAY<TIMESTAMP_NTZ>)").alias("a"),
        ).toArrow()
        values = cast.column("m").combine_chunks().values.field(1)
        assert values.cast(pa.int64()).to_pylist() == [NEW_YORK_WALL]
        assert leaf_ticks(cast.column("s")) == [NEW_YORK_WALL]
        assert leaf_ticks(cast.column("a")) == [NEW_YORK_WALL]
    finally:
        spark.stop()


def test_a_pandas_instant_zoned_outside_utc_casts_in_a_struct_to_the_session_wall(
    tmp_path: Path,
) -> None:
    """A pandas column zoned ``America/New_York`` casts inside a struct to the session wall."""
    import pandas as pd

    spark = open_session(NEW_YORK, tmp_path)
    try:
        instant = pd.Timestamp("2026-01-02 03:04:05.123456", tz="UTC").tz_convert(NEW_YORK)
        spark.createDataFrame(pd.DataFrame({"x": [instant, instant]})).createOrReplaceTempView(
            "zoned"
        )
        cast = spark.sql(f"SELECT {CONTAINER_CAST} AS st FROM zoned").toArrow()
        assert leaf_ticks(cast.column(0)) == [NEW_YORK_WALL, NEW_YORK_WALL]
    finally:
        spark.stop()


def test_the_dataframe_door_refuses_a_struct_or_array_cast_type_as_on_main(tmp_path: Path) -> None:
    """``Column.cast`` to a struct or array type is not parsed by the facade, on main too."""
    spark = open_session(NEW_YORK, tmp_path)
    try:
        frame = spark.sql(
            f"SELECT named_struct('v', {INSTANT}, 'n', 1) AS s, array({INSTANT}) AS a"
        )
        for column, target in (("s", NTZ_STRUCT), ("a", ArrayType(TimestampNTZType()))):
            with pytest.raises(Exception, match="unknown cast type"):
                frame.select(functions.col(column).cast(target)).toArrow()
    finally:
        spark.stop()


THROUGH: dict[str, tuple[str, str, int | None]] = {
    "insert through string": (
        STRUCT,
        f"INSERT INTO ice.ns.{{t}} SELECT id, CAST(CAST(named_struct('v', x, 'n', 1) AS "
        f"STRUCT<v: STRING, n: INT>) AS {STRUCT}), 0 FROM ice.ns.src",
        NEW_YORK_WALL,
    ),
    "array through string": (
        "ARRAY<TIMESTAMP_NTZ>",
        "INSERT INTO ice.ns.{t} SELECT id, CAST(CAST(array(x) AS ARRAY<STRING>) AS "
        "ARRAY<TIMESTAMP_NTZ>), 0 FROM ice.ns.src",
        NEW_YORK_WALL,
    ),
    "leaf cast over a string the container made": (
        STRUCT,
        "INSERT INTO ice.ns.{t} SELECT id, named_struct('v', CAST(CAST(array(x) AS "
        "ARRAY<STRING>)[0] AS TIMESTAMP_NTZ), 'n', 1), 0 FROM ice.ns.src",
        NEW_YORK_WALL,
    ),
    "insert through date": (
        STRUCT,
        f"INSERT INTO ice.ns.{{t}} SELECT id, CAST(CAST(named_struct('v', x, 'n', 1) AS "
        f"STRUCT<v: DATE, n: INT>) AS {STRUCT}), 0 FROM ice.ns.src",
        NEW_YORK_MIDNIGHT,
    ),
    "from_json of to_json": (
        STRUCT,
        "INSERT INTO ice.ns.{t} SELECT id, from_json(to_json(named_struct('v', x, 'n', 1)), "
        "'v TIMESTAMP_NTZ, n INT'), 0 FROM ice.ns.src",
        None,
    ),
}


@pytest.mark.parametrize("case", list(THROUGH))
def test_a_value_through_another_type_stores_what_spark_stores(tmp_path: Path, case: str) -> None:
    """The card's "through another type" rows store Spark's value in New York."""
    column_type, write, expected = THROUGH[case]
    spark = open_session(NEW_YORK, tmp_path)
    try:
        create_target(spark, "t", column_type)
        spark.sql(write.format(t="t"))
        assert stored(spark, "t") == [expected]
    finally:
        spark.stop()


def test_a_merge_update_through_string_stores_the_session_wall(tmp_path: Path) -> None:
    """``MERGE … UPDATE SET`` of a struct cast through ``STRING`` stores the New York wall."""
    spark = open_session(NEW_YORK, tmp_path)
    try:
        create_target(spark, "t", STRUCT)
        spark.sql("INSERT INTO ice.ns.t (id, k) VALUES (1, 0)")
        spark.sql(
            "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN UPDATE "
            "SET t.st = CAST(CAST(named_struct('v', s.x, 'n', 1) AS STRUCT<v: STRING, n: INT>) "
            f"AS {STRUCT})"
        )
        assert stored(spark, "t") == [NEW_YORK_WALL]
    finally:
        spark.stop()


BIGINT_REFUSALS: dict[str, str] = {
    "UTC": r"DATATYPE_MISMATCH\.CAST_WITHOUT_SUGGESTION",
    NEW_YORK: r"INCOMPATIBLE_DATA_FOR_TABLE\.CANNOT_SAFELY_CAST",
}


@pytest.mark.parametrize("zone", list(BIGINT_REFUSALS))
def test_a_struct_through_bigint_is_refused_and_writes_nothing(tmp_path: Path, zone: str) -> None:
    """``BIGINT`` to ``TIMESTAMP_NTZ`` in a struct refuses; outside UTC R-011's gate says so."""
    spark = open_session(zone, tmp_path)
    try:
        create_target(spark, "t", STRUCT)
        with pytest.raises(Exception, match=BIGINT_REFUSALS[zone]):
            spark.sql(
                "INSERT INTO ice.ns.t SELECT id, CAST(CAST(named_struct('v', x, 'n', 1) AS "
                f"STRUCT<v: BIGINT, n: INT>) AS {STRUCT}), 0 FROM ice.ns.src"
            )
        with pytest.raises(Exception, match=r"DATATYPE_MISMATCH\.CAST_WITHOUT_SUGGESTION"):
            spark.sql(
                "SELECT CAST(CAST(named_struct('v', x, 'n', 1) AS STRUCT<v: BIGINT, n: INT>) "
                f"AS {STRUCT}) AS st FROM ice.ns.src"
            ).toArrow()
        assert stored(spark, "t") == []
    finally:
        spark.stop()


def create_as_select(spark: Any, name: str) -> None:
    """``CREATE TABLE … USING iceberg AS SELECT`` of the container cast."""
    spark.sql(
        f"CREATE TABLE ice.ns.{name} USING iceberg AS SELECT id, {CONTAINER_CAST} AS st "
        "FROM ice.ns.src"
    )


def create_or_replace_as_select(spark: Any, name: str) -> None:
    """``CREATE OR REPLACE TABLE … AS SELECT`` of the container cast."""
    spark.sql(
        f"CREATE OR REPLACE TABLE ice.ns.{name} USING iceberg AS SELECT id, {CONTAINER_CAST} "
        "AS st FROM ice.ns.src"
    )


def replace_as_select(spark: Any, name: str) -> None:
    """``REPLACE TABLE … AS SELECT`` over an existing table."""
    create_target(spark, name, STRUCT)
    spark.sql(
        f"REPLACE TABLE ice.ns.{name} USING iceberg AS SELECT id, {CONTAINER_CAST} AS st "
        "FROM ice.ns.src"
    )


def cast_frame(spark: Any) -> Any:
    """Return the frame ``SELECT id, <container cast> AS st FROM ice.ns.src``."""
    return spark.sql(f"SELECT id, {CONTAINER_CAST} AS st FROM ice.ns.src")


def save_as_table_overwrite(spark: Any, name: str) -> None:
    """``df.write.mode("overwrite").saveAsTable``."""
    cast_frame(spark).write.mode("overwrite").saveAsTable(f"ice.ns.{name}")


def write_to_create(spark: Any, name: str) -> None:
    """``df.writeTo(...).create()``."""
    cast_frame(spark).writeTo(f"ice.ns.{name}").create()


def write_to_replace(spark: Any, name: str) -> None:
    """``df.writeTo(...).replace()`` over an existing table."""
    create_target(spark, name, STRUCT)
    cast_frame(spark).writeTo(f"ice.ns.{name}").replace()


def write_to_create_or_replace(spark: Any, name: str) -> None:
    """``df.writeTo(...).createOrReplace()``."""
    cast_frame(spark).writeTo(f"ice.ns.{name}").createOrReplace()


def cached_frame_append(spark: Any, name: str) -> None:
    """A cached frame of the container cast written with ``writeTo().append()``."""
    create_target(spark, name, STRUCT)
    frame = spark.sql(f"SELECT id, {CONTAINER_CAST} AS st, 0 AS k FROM ice.ns.src").cache()
    frame.writeTo(f"ice.ns.{name}").append()


WRITERS: dict[str, Callable[[Any, str], None]] = {
    "create table as select": create_as_select,
    "create or replace table as select": create_or_replace_as_select,
    "replace table as select": replace_as_select,
    "saveAsTable overwrite": save_as_table_overwrite,
    "writeTo create": write_to_create,
    "writeTo replace": write_to_replace,
    "writeTo createOrReplace": write_to_create_or_replace,
    "cached frame append": cached_frame_append,
}


@pytest.mark.parametrize("zone", [NEW_YORK, "UTC"])
@pytest.mark.parametrize("writer", list(WRITERS))
def test_every_table_creating_writer_stores_the_session_wall(
    tmp_path: Path, writer: str, zone: str
) -> None:
    """The six table-creating writers and a cached frame store the session wall."""
    spark = open_session(zone, tmp_path)
    try:
        WRITERS[writer](spark, "w")
        assert stored(spark, "w") == [NEW_YORK_WALL if zone == NEW_YORK else UTC_WALL]
    finally:
        spark.stop()


NATIVE_CONTROLS: dict[str, tuple[str, Any]] = {
    "struct": (
        f"SELECT CAST(named_struct('v', {INSTANT}, 'n', 1) AS {STRUCT}) AS v",
        UTC_WALL,
    ),
    "array": (f"SELECT CAST(make_array({INSTANT}) AS ARRAY<TIMESTAMP_NTZ>) AS v", UTC_WALL),
    "leaf": (f"SELECT CAST({INSTANT} AS TIMESTAMP_NTZ) AS v", UTC_WALL),
}


@pytest.mark.parametrize("shape", list(NATIVE_CONTROLS))
def test_the_native_door_answers_as_on_main(shape: str) -> None:
    """``repark.sql()`` has no session-aware leaf cast; its container cast answers as on main."""
    sql, expected = NATIVE_CONTROLS[shape]
    assert leaf_ticks(repark.sql(sql).to_arrow().column(0)) == [expected]


def test_the_native_door_renders_a_struct_string_leaf_as_on_main() -> None:
    """A native-door struct cast to a ``STRING`` leaf keeps main's ISO text."""
    table = repark.sql(
        f"SELECT CAST(named_struct('v', {INSTANT}, 'n', 1) AS STRUCT<v: STRING, n: INT>) AS v"
    ).to_arrow()
    assert table.column(0).combine_chunks().field(0).to_pylist() == ["2026-01-02T03:04:05.123456"]


LTZ_STRUCT = "STRUCT<v: TIMESTAMP, n: INT>"
NTZ_INTO_LTZ_REFUSAL = (
    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the "
    'table `ice`.`ns`.`t`: Cannot safely cast `st`.`v` to "TIMESTAMP". The session time zone '
    "is not UTC, and a nested TIMESTAMP_NTZ value cannot yet be stored into a nested TIMESTAMP "
    "field outside UTC: set spark.sql.session.timeZone to UTC for the statement, or cast the "
    "leaf to TIMESTAMP before nesting it, as in named_struct('<field>', CAST(<value> AS "
    "TIMESTAMP), ...). SQLSTATE: KD000"
)
NTZ_FRAME = f"SELECT id, named_struct('v', {WALL}, 'n', 1) AS st, 0 AS k FROM ice.ns.src"
REFUSING_WRITERS: dict[str, Callable[[Any], None]] = {
    "insertInto overwrite": lambda frame: frame.write.insertInto("ice.ns.t", overwrite=True),
    "overwritePartitions": lambda frame: frame.writeTo("ice.ns.t").overwritePartitions(),
}
STORING_WRITERS: dict[str, Callable[[Any], None]] = {
    "writeTo append": lambda frame: frame.writeTo("ice.ns.t").append(),
    "insertInto": lambda frame: frame.write.insertInto("ice.ns.t"),
}


@pytest.mark.parametrize("writer", list(REFUSING_WRITERS))
def test_an_overwriting_dataframe_writer_refuses_a_nested_wall_into_a_timestamp_field(
    tmp_path: Path, writer: str
) -> None:
    """Outside UTC a nested wall into a nested ``TIMESTAMP`` field refuses (ruling ZS-9)."""
    spark = open_session(NEW_YORK, tmp_path)
    try:
        create_target(spark, "t", LTZ_STRUCT)
        with pytest.raises(Exception) as refused:
            REFUSING_WRITERS[writer](spark.sql(NTZ_FRAME))
        assert NTZ_INTO_LTZ_REFUSAL in str(refused.value)
        assert stored(spark, "t") == []
    finally:
        spark.stop()


STORING_CASES: list[tuple[str, str]] = [
    *((writer, NEW_YORK) for writer in STORING_WRITERS),
    *((writer, "UTC") for writer in (*STORING_WRITERS, *REFUSING_WRITERS)),
]


@pytest.mark.parametrize(("writer", "zone"), STORING_CASES)
def test_a_nested_wall_into_a_timestamp_field_stores_where_it_is_not_refused(
    tmp_path: Path, writer: str, zone: str
) -> None:
    """The appending writers read the wall in the session zone; in UTC every writer stores."""
    writers = {**STORING_WRITERS, **REFUSING_WRITERS}
    spark = open_session(zone, tmp_path)
    try:
        create_target(spark, "t", LTZ_STRUCT)
        writers[writer](spark.sql(NTZ_FRAME))
        expected = 1_767_341_045_123_456 if zone == NEW_YORK else UTC_WALL
        assert stored(spark, "t") == [expected]
    finally:
        spark.stop()
