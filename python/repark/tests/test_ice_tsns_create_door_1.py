"""ICE-TSNS-CREATE-DOOR-1: a new nanosecond column is created only when the statement asks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _record_ice_tsns_create_door_1_main as doors
import pytest

MAIN: dict[str, dict[str, Any]] = json.loads(doors.FIXTURE.read_text(encoding="utf-8"))
NOT_REQUESTED = (
    "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] {form} would create column "
    "`{name}` as {nano}, a type Spark cannot read, and the statement does not ask for it. "
    "Ask with CAST(<expression> AS {nano}) on the column (SQL or selectExpr), or set "
    "`repark.sql.allowImplicitTimestampNanos` = true on the session. For microseconds write "
    "CAST(<expression> AS {micro})."
)
NANO_ICEBERG = ("timestamp_ns", "timestamptz_ns")
EMPTY_REPLACED = {
    "format_version": 3,
    "schema": [["id", "int"], ["keepcol", "int"]],
    "arrow": ["int32", "int32"],
    "rows": [[], []],
}
EMPTY_EVOLVED = {"format_version": 3, "schema": [["id", "int"]], "arrow": ["int32"], "rows": [[]]}
SEED_ROWS = [[1, 2], [doors.SEED_NANOS, None]]


def not_requested(form: str, column: str) -> dict[str, Any]:
    """Return the refusal of an unasked nanosecond ``column`` through ``form``."""
    text = NOT_REQUESTED.format(
        form=form, name=column, nano=doors.NANO_TYPES[column], micro=doors.MICRO_TYPES[column]
    )
    return {
        "class": "AnalysisException",
        "condition": "TIMESTAMP_NANOS_NOT_REQUESTED",
        "text": text,
    }


def prepared(door: str) -> dict[str, Any] | None:
    """Return the table a door starts from, which a refusal must leave as it was."""
    if door in doors.REPLACED:
        return EMPTY_REPLACED
    if door in doors.EVOLVED or door == "merge_schema_evolution":
        return EMPTY_EVOLVED
    return None


def holds_nanoseconds(cell: dict[str, Any]) -> bool:
    """Return whether a recorded cell ends with a nanosecond Iceberg column."""
    table = cell["table"]
    return table is not None and any(kind in NANO_ICEBERG for _, kind in table["schema"])


def below_v3(cell: dict[str, Any], column: str) -> None:
    """Assert a cell is the refusal of a nanosecond type on a format v2 table."""
    assert cell["table"] is None
    assert cell["files_unchanged"] is True
    wanted = f"Invalid type for {column}: {doors.NANO_TYPES[column]} is not supported until v3"
    assert wanted in cell["error"]["text"]


def test_main_created_the_column_without_an_ask() -> None:
    """The fixture is main's: 44 unasked matrix cells and 28 pass 3 cells made nanoseconds."""
    matrix = MAIN["matrix"]
    unasked = [key for key in matrix if key.endswith("|none") and holds_nanoseconds(matrix[key])]
    asked = [key for key in matrix if key.endswith("|cast") and holds_nanoseconds(matrix[key])]
    assert (len(unasked), len(asked)) == (44, 44)
    for key in asked:
        assert matrix[key]["table"]["rows"] == SEED_ROWS, key
        assert matrix[key]["table"]["format_version"] == 3, key
    pass3 = MAIN["pass3"]
    created = [key for key in pass3 if holds_nanoseconds(pass3[key])]
    assert len(created) == 42
    assert len([key for key in created if "|scast|" not in key]) == 28


@pytest.mark.parametrize("column", doors.COLUMNS)
@pytest.mark.parametrize("door", doors.DOORS)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_an_unasked_nanosecond_column_is_refused_on_every_door(
    zone: str, door: str, column: str
) -> None:
    """The plain column of a nanosecond table creates nothing and leaves the table as it was."""
    cell = doors.in_session(zone, False, doors.matrix_cell, door, column, "none")
    assert cell["error"] == not_requested(doors.FORMS[door], column)
    assert cell["files_unchanged"] is True
    assert cell["table"] == prepared(door)


@pytest.mark.parametrize("column", doors.COLUMNS)
@pytest.mark.parametrize("door", doors.DOORS)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_a_written_cast_creates_what_main_created(zone: str, door: str, column: str) -> None:
    """A cast written in SQL or ``selectExpr`` is the ask: main's table, all nine digits."""
    cell = doors.in_session(zone, False, doors.matrix_cell, door, column, "cast")
    if door == "save_create":
        below_v3(cell, column)
        return
    assert cell == MAIN["matrix"][f"{zone}|{door}|{column}|cast"]
    assert cell["table"]["rows"] == SEED_ROWS


@pytest.mark.parametrize("column", doors.COLUMNS)
@pytest.mark.parametrize("door", doors.DOORS)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_the_session_setting_creates_what_the_written_cast_creates(
    zone: str, door: str, column: str
) -> None:
    """With the setting on, the plain column makes the table the written cast makes."""
    cell = doors.in_session(zone, True, doors.matrix_cell, door, column, "none")
    if door == "save_create":
        below_v3(cell, column)
        return
    assert cell == MAIN["matrix"][f"{zone}|{door}|{column}|cast"]


@pytest.mark.parametrize("door", doors.PASS3_DOORS)
@pytest.mark.parametrize("source", doors.PASS3_SOURCES)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_the_census_cells_refuse_unasked_and_create_when_cast(
    zone: str, source: str, door: str
) -> None:
    """The census's 28 unasked cells refuse naming ``c``; its 14 written-cast twins create."""
    cell = doors.in_session(zone, False, doors.pass3_cell, source, door)
    if source == "scast":
        assert cell == MAIN["pass3"][f"{zone}|{source}|{door}"]
        assert cell["table"]["rows"] == [[doors.SEED_NANOS], [doors.SEED_NANOS]]
        return
    form = "CTAS" if door == "temp_view" else doors.FORMS[door]
    assert cell["error"] == not_requested(form, "c")
    assert cell["files_unchanged"] is True
    assert cell["table"] == prepared(door)


@pytest.mark.parametrize("name", doors.PLAIN)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_statements_with_no_nanosecond_output_equal_main(zone: str, name: str) -> None:
    """A create, evolution or refusal over a microsecond source answers what main answered."""
    cell = doors.in_session(zone, False, doors.plain_cell, name)
    assert cell == MAIN["plain"][f"{zone}|{name}"]


def kept_cell(spark: Any, warehouse: Path, door: str) -> dict[str, Any]:
    """Run ``door`` over the plain columns into a table that already holds them as nanoseconds."""
    table = f"ice.ns.kept_{door}"
    properties = doors.EVOLVING if door in doors.EVOLVED else doors.V3
    spark.sql(
        f"CREATE TABLE {table} (id INT, c timestamp_ns, cz timestamptz_ns) "
        f"USING iceberg {properties}"
    )
    select = f"SELECT id, c, cz FROM {doors.SEED}"
    frame = spark.table(doors.SEED)
    return doors.measure(
        spark, warehouse, table, lambda: doors.run_door(spark, door, table, select, frame)
    )


@pytest.mark.parametrize(
    "door",
    [
        "create_or_replace",
        "replace",
        "write_to_create_or_replace",
        "write_to_replace",
        "save_overwrite",
        "save_overwrite_schema",
        "save_merge_schema",
        "write_to_append_merge_schema",
        "merge_schema_evolution",
    ],
)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_a_kept_nanosecond_column_survives_without_an_ask(zone: str, door: str) -> None:
    """A column the replaced or evolved table already holds, same name and type, is not new."""
    cell = doors.in_session(zone, False, kept_cell, door)
    assert "error" not in cell
    assert cell["table"] == {
        "format_version": 3,
        "schema": [["id", "int"], ["c", "timestamp_ns"], ["cz", "timestamptz_ns"]],
        "arrow": ["int32", "timestamp[ns]", "timestamp[ns, tz=UTC]"],
        "rows": [[1, 2], [doors.SEED_NANOS, None], [doors.SEED_NANOS, None]],
    }


def renamed_or_retyped_cell(spark: Any, warehouse: Path, select: str) -> dict[str, Any]:
    """Replace a table holding ``c timestamp_ns`` with the columns of ``select``."""
    table = "ice.ns.kept_other"
    spark.sql(f"CREATE TABLE {table} (id INT, c timestamp_ns) USING iceberg {doors.V3}")
    statement = f"CREATE OR REPLACE TABLE {table} USING iceberg AS {select}"
    return doors.measure(spark, warehouse, table, lambda: spark.sql(statement))


@pytest.mark.parametrize(
    ("select", "column", "name"),
    [
        (f"SELECT id, cz AS c FROM {doors.SEED}", "cz", "c"),
        (f"SELECT id, c AS d FROM {doors.SEED}", "c", "d"),
    ],
)
def test_a_kept_name_with_another_type_or_another_name_is_new(
    select: str, column: str, name: str
) -> None:
    """Changing the type under a kept name, or the name, makes the column new again."""
    cell = doors.in_session("UTC", False, renamed_or_retyped_cell, select)
    expected = not_requested("RTAS", column)
    expected["text"] = expected["text"].replace(f"`{column}`", f"`{name}`")
    assert cell["error"] == expected
    assert cell["files_unchanged"] is True
    assert cell["table"]["schema"] == [["id", "int"], ["c", "timestamp_ns"]]


def named_type_cell(spark: Any, warehouse: Path) -> dict[str, Any]:
    """Create a nanosecond table by DDL, then add and replace columns naming the type."""
    table = "ice.ns.named"
    spark.sql(f"CREATE TABLE {table} (id INT, c timestamp_ns) USING iceberg {doors.V3}")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN cz timestamptz_ns")
    return doors.measure(spark, warehouse, table, lambda: None)


@pytest.mark.parametrize("zone", doors.ZONES)
def test_ddl_and_add_column_naming_the_type_still_create_it(zone: str) -> None:
    """A statement that names the type needs no cast and no setting."""
    cell = doors.in_session(zone, False, named_type_cell)
    assert cell["table"]["format_version"] == 3
    assert cell["table"]["schema"] == [
        ["id", "int"],
        ["c", "timestamp_ns"],
        ["cz", "timestamptz_ns"],
    ]


def cached_cell(spark: Any, warehouse: Path) -> dict[str, Any]:
    """Create a table from a cached frame whose cast was written before the cache."""
    table = "ice.ns.cached"
    frame = doors.frame_of(spark, "cast", "c").cache()
    writer = frame.writeTo(table).tableProperty("format-version", "3")
    return doors.measure(spark, warehouse, table, writer.create)


def pandas_cell(spark: Any, warehouse: Path) -> dict[str, Any]:
    """Create a v3 table from a pandas nanosecond frame."""
    pandas = pytest.importorskip("pandas")
    table = "ice.ns.from_pandas"
    stamps = pandas.to_datetime([doors.SEED_TEXT, None]).as_unit("ns")
    frame = spark.createDataFrame(pandas.DataFrame({"id": [1, 2], "c": stamps}))
    writer = frame.writeTo(table).tableProperty("format-version", "3")
    return doors.measure(spark, warehouse, table, writer.create)


def test_a_cached_frame_is_refused_and_a_pandas_frame_is_microseconds() -> None:
    """A cache hides the written cast, so it is not an ask; pandas enters as microseconds."""
    cached = doors.in_session("UTC", False, cached_cell)
    assert cached["error"] == not_requested("CTAS", "c")
    assert cached["table"] is None
    from_pandas = doors.in_session("UTC", False, pandas_cell)
    assert "error" not in from_pandas
    assert from_pandas["table"]["schema"] == [["id", "long"], ["c", "timestamp"]]
    assert from_pandas["table"]["rows"][1] == [doors.SEED_NANOS // 1000, None]


def format_version_cell(spark: Any, warehouse: Path, source: str) -> dict[str, Any]:
    """Run a CTAS that asks for format v3 and nothing else over ``source``."""
    table = "ice.ns.only_v3"
    statement = f"CREATE TABLE {table} USING iceberg {doors.V3} AS SELECT id, c FROM {source}"
    return doors.measure(spark, warehouse, table, lambda: spark.sql(statement))


def test_format_version_3_alone_is_not_an_ask() -> None:
    """Asking for a v3 table does not ask for a nanosecond column in it."""
    refused = doors.in_session("UTC", False, format_version_cell, doors.SEED)
    assert refused["error"] == not_requested("CTAS", "c")
    assert refused["table"] is None
    created = doors.in_session("UTC", False, format_version_cell, doors.MICRO)
    assert created["table"]["format_version"] == 3
    assert created["table"]["schema"] == [["id", "int"], ["c", "timestamp"]]


def shapes_cell(spark: Any, warehouse: Path, select: str) -> dict[str, Any]:
    """Run a v3 CTAS over ``select``."""
    table = "ice.ns.shape"
    statement = f"CREATE TABLE {table} USING iceberg {doors.V3} AS {select}"
    return doors.measure(spark, warehouse, table, lambda: spark.sql(statement))


ASKED_SHAPES = {
    "aggregate": f"SELECT max(CAST(c AS timestamp_ns)) AS c FROM {doors.MICRO}",
    "subquery": (
        f"SELECT * FROM (SELECT id, CAST(c AS timestamp_ns) AS c FROM {doors.MICRO}) x WHERE id > 0"
    ),
    "union": (
        f"SELECT CAST(c AS timestamp_ns) AS c FROM {doors.MICRO} "
        f"UNION ALL SELECT CAST(cz AS timestamp_ns) FROM {doors.MICRO}"
    ),
    "values": (f"SELECT * FROM VALUES (CAST('{doors.SEED_TEXT}' AS timestamp_ns)), (NULL) AS v(c)"),
    "double_colon": f"SELECT c::timestamp_ns AS c FROM {doors.MICRO}",
}
UNASKED_SHAPES = {
    "union_with_a_table_column": (
        f"SELECT CAST(c AS timestamp_ns) AS c FROM {doors.MICRO} "
        f"UNION ALL SELECT c FROM {doors.SEED}"
    ),
    "coalesce_over_a_table_column": (
        f"SELECT coalesce(c, CAST('{doors.SEED_TEXT}' AS timestamp_ns)) AS c FROM {doors.SEED}"
    ),
    "array": f"SELECT array(c) AS c FROM {doors.SEED}",
    "struct": f"SELECT named_struct('x', c) AS c FROM {doors.SEED}",
    "window": f"SELECT max(c) OVER (PARTITION BY id) AS c FROM {doors.SEED}",
    "join": f"SELECT s.c FROM {doors.MICRO} m JOIN {doors.SEED} s ON m.id = s.id",
}


@pytest.mark.parametrize("shape", list(ASKED_SHAPES))
def test_a_column_whose_every_nanosecond_origin_is_a_written_cast_is_asked(shape: str) -> None:
    """An aggregate, subquery, union or VALUES over written casts creates the column."""
    cell = doors.in_session("UTC", False, shapes_cell, ASKED_SHAPES[shape])
    assert "error" not in cell
    assert cell["table"]["schema"][-1] == ["c", "timestamp_ns"]


@pytest.mark.parametrize("shape", list(UNASKED_SHAPES))
def test_a_column_with_a_table_origin_is_not_asked(shape: str) -> None:
    """One origin that is a table column, at any depth, makes the column unasked."""
    cell = doors.in_session("UTC", False, shapes_cell, UNASKED_SHAPES[shape])
    assert cell["error"] == not_requested("CTAS", "c")
    assert cell["table"] is None
