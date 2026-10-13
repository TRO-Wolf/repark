"""ICE-TSNS-CREATE-DOOR-1 fold 1: every remedy a refusal names works on the seat that raised it."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_create_door_1_texts as texts
import _record_ice_tsns_create_door_1_main as doors
import pytest

MAIN: dict[str, dict[str, Any]] = json.loads(doors.FIXTURE.read_text(encoding="utf-8"))
SEED = doors.SEED
V3 = doors.V3
TABLE = "ice.ns.t"
STAR = "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
SEATS = {
    "ctas": ("CTAS", "create"),
    "create_or_replace": ("RTAS", "create"),
    "sql_temp_view": ("CTAS", "create"),
    "write_to_create": ("CTAS", "create"),
    "write_to_create_or_replace": ("RTAS", "create"),
    "save_create": ("CTAS", "create"),
    "replace": ("RTAS", "replace"),
    "write_to_replace": ("RTAS", "replace"),
    "save_overwrite": ("RTAS", "replace"),
    "save_overwrite_schema": ("RTAS", "replace"),
    "save_merge_schema": ("mergeSchema append", "evolve"),
    "write_to_append_merge_schema": ("mergeSchema append", "evolve"),
    "merge_schema_evolution": (texts.MERGE_FORM, "evolve"),
}
WAYS_TO_V3 = {"create": texts.CREATE_V3, "replace": texts.REPLACE_V3, "evolve": texts.UPGRADE_V3}
MICRO_ICEBERG = {"c": "timestamp", "cz": "timestamptz"}
CELLS = [
    (seat, column, target)
    for seat in SEATS
    for column in doors.COLUMNS
    for target in ("v3", "below")
    if not (seat == "save_create" and target == "v3")
]


def properties(seat: str, target: str) -> str:
    """Return the table properties a seat's prepared or created table carries."""
    wanted = []
    if target == "v3":
        wanted.append("'format-version' = '3'")
    if seat in ("save_merge_schema", "write_to_append_merge_schema"):
        wanted.append("'write.spark.accept-any-schema' = 'true'")
    return f"TBLPROPERTIES ({', '.join(wanted)})" if wanted else ""


def prepare(spark: Any, seat: str, target: str) -> None:
    """Create the table a replacing or evolving seat starts from."""
    kind = SEATS[seat][1]
    if kind == "replace":
        spark.sql(
            f"CREATE TABLE {TABLE} (id INT, keepcol INT) USING iceberg {V3 * (target == 'v3')}"
        )
    elif kind == "evolve":
        spark.sql(f"CREATE TABLE {TABLE} (id INT) USING iceberg {properties(seat, target)}")


def act(spark: Any, seat: str, target: str, expression: str) -> None:
    """Run one seat over ``expression`` of the seed table."""
    select = f"SELECT id, {expression} FROM {SEED}"
    frame = spark.table(SEED).selectExpr("id", expression)
    asked_v3 = V3 if target == "v3" else ""
    if seat == "ctas":
        spark.sql(f"CREATE TABLE {TABLE} USING iceberg {asked_v3} AS {select}")
    elif seat == "create_or_replace":
        spark.sql(f"CREATE OR REPLACE TABLE {TABLE} USING iceberg {asked_v3} AS {select}")
    elif seat == "sql_temp_view":
        spark.sql(f"CREATE OR REPLACE TEMP VIEW fold_view AS {select}")
        spark.sql(f"CREATE TABLE {TABLE} USING iceberg {asked_v3} AS SELECT * FROM fold_view")
    elif seat in ("write_to_create", "write_to_create_or_replace"):
        writer = frame.writeTo(TABLE)
        if target == "v3":
            writer = writer.tableProperty("format-version", "3")
        if seat == "write_to_create":
            writer.create()
        else:
            writer.createOrReplace()
    elif seat == "merge_schema_evolution":
        spark.sql(
            f"MERGE WITH SCHEMA EVOLUTION INTO {TABLE} t USING ({select}) s ON t.id = s.id {STAR}"
        )
    else:
        doors.run_door(spark, seat, TABLE, select, frame)


def follow_sql_property(spark: Any, seat: str, expression: str) -> None:
    """Follow ``TBLPROPERTIES ('format-version'='3') in SQL`` for the seat's kind."""
    verb = "REPLACE TABLE" if SEATS[seat][1] == "replace" else "CREATE TABLE"
    spark.sql(f"{verb} {TABLE} USING iceberg {V3} AS SELECT id, {expression} FROM {SEED}")


def follow_write_to_property(spark: Any, seat: str, expression: str) -> None:
    """Follow ``writeTo(...).tableProperty('format-version', '3')`` for the seat's kind."""
    writer = spark.table(SEED).selectExpr("id", expression).writeTo(TABLE)
    writer = writer.tableProperty("format-version", "3")
    if SEATS[seat][1] == "replace":
        writer.replace()
    else:
        writer.create()


def follow_upgrade(spark: Any, seat: str, expression: str) -> None:
    """Follow the ``ALTER TABLE … SET TBLPROPERTIES`` upgrade, then run the seat again."""
    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES ('format-version'='3')")
    act(spark, seat, "below", expression)


FOLLOWED = {
    "TBLPROPERTIES ('format-version'='3') in SQL": follow_sql_property,
    "`writeTo(...).tableProperty('format-version', '3')`": follow_write_to_property,
    "ALTER TABLE … SET TBLPROPERTIES ('format-version'='3')": follow_upgrade,
}


def outcome(
    spark: Any, warehouse: Path, seat: str, target: str, expression: str, follow: str = ""
) -> dict[str, Any]:
    """Prepare the seat, run it or the named way to a v3 table, and return the measured cell."""
    prepare(spark, seat, target)
    if follow:
        action = FOLLOWED[follow]
        return doors.measure(spark, warehouse, TABLE, lambda: action(spark, seat, expression))
    return doors.measure(spark, warehouse, TABLE, lambda: act(spark, seat, target, expression))


def stored_type(cell: dict[str, Any], column: str) -> str | None:
    """Return the Iceberg type the cell's table holds for ``column``, or its error text."""
    if "error" in cell:
        return cell["error"]["text"]
    return dict(map(tuple, cell["table"]["schema"])).get(column)


def spelled(template: str, column: str) -> str:
    """Return a remedy's spelling written out for ``column``."""
    return f"{template.replace('<expression>', column)} AS {column}"


@pytest.mark.parametrize(("seat", "column", "target"), CELLS)
def test_every_remedy_a_refusal_names_works_when_followed(
    seat: str, column: str, target: str
) -> None:
    """Follow the cast, the session setting, the microsecond spelling and each way to format v3."""
    form, kind = SEATS[seat]
    nano = doors.NANO_TYPES[column]
    below = WAYS_TO_V3[kind] if target == "below" else None
    refused = doors.in_session("UTC", False, outcome, seat, target, column)
    expected = texts.not_requested(form, column, below=below)
    assert refused["error"] == expected
    assert refused["files_unchanged"] is True
    text = expected["text"]
    assert f"CAST(<expression> AS {nano}) on the column" in text
    assert texts.SETTING in text
    micro = text.rsplit("For microseconds write ", 1)[1].removesuffix(".")
    microseconds = doors.in_session("UTC", False, outcome, seat, target, spelled(micro, column))
    assert stored_type(microseconds, column) == MICRO_ICEBERG[column]
    cast = spelled(f"CAST(<expression> AS {nano})", column)
    asked = doors.in_session("UTC", False, outcome, seat, target, cast)
    with_setting = doors.in_session("UTC", True, outcome, seat, target, column)
    if below is None:
        assert stored_type(asked, column) == nano
        assert stored_type(with_setting, column) == nano
        assert not any(name in text for name in FOLLOWED)
        return
    needs_v3 = texts.requires_v3(column, nano, below)
    assert asked["error"] == needs_v3
    assert with_setting["error"] == needs_v3
    assert asked["files_unchanged"] is True
    named = [name for name in FOLLOWED if name in below]
    assert named, below
    assert all(name in text for name in named)
    assert ("Create the table" in below) == (kind == "create")
    for name in named:
        followed = doors.in_session("UTC", False, outcome, seat, target, cast, name)
        assert stored_type(followed, column) == nano, (name, followed.get("error"))
        with_both = doors.in_session("UTC", True, outcome, seat, target, column, name)
        assert stored_type(with_both, column) == nano, (name, with_both.get("error"))


def conf_cell(spark: Any, warehouse: Path, how: str) -> dict[str, Any]:
    """Turn the setting on at run time through ``how``, then run a plain-column CTAS."""
    if how == "conf.set":
        spark.conf.set(texts.SETTING_KEY, "true")
    else:
        spark.sql(f"SET {texts.SETTING_KEY} = true")
    statement = f"CREATE TABLE {TABLE} USING iceberg {V3} AS SELECT id, c FROM {SEED}"
    return doors.measure(spark, warehouse, TABLE, lambda: spark.sql(statement))


@pytest.mark.parametrize("how", ["conf.set", "SET"])
def test_the_setting_is_read_when_the_session_is_built_and_the_hint_says_so(how: str) -> None:
    """A run-time ``conf.set`` or ``SET`` is still ignored; the text names the builder only."""
    cell = doors.in_session("UTC", False, conf_cell, how)
    assert cell["error"] == texts.not_requested("CTAS", "c")
    assert "ReparkSession.builder.config" in cell["error"]["text"]
    assert "when the session is built" in cell["error"]["text"]
    assert " on the session" not in cell["error"]["text"]


@pytest.mark.parametrize("key", sorted(MAIN["typechange"]))
def test_an_evolving_seat_answers_a_same_name_column_of_another_type_as_main_did(key: str) -> None:
    """Only a column the evolution would add is checked; a type change is main's error."""
    zone, door, held, source = key.split("|")
    cell = doors.in_session(zone, False, doors.typechange_cell, door, held, source)
    assert cell == MAIN["typechange"][key]
    if "error" in cell:
        assert "TIMESTAMP_NANOS" not in cell["error"]["text"]


def shape_cell(spark: Any, warehouse: Path, select: str, *views: str) -> dict[str, Any]:
    """Create the views, then run a v3 CTAS over ``select``."""
    for view in views:
        spark.sql(view)
    statement = f"CREATE TABLE {TABLE} USING iceberg {V3} AS {select}"
    return doors.measure(spark, warehouse, TABLE, lambda: spark.sql(statement))


WRITTEN = "CAST(c AS timestamp_ns)"
CAST_VIEW = f"CREATE TEMP VIEW written AS SELECT id, {WRITTEN} AS c FROM {SEED}"
FOLLOWED_SHAPES = {
    "sql_temp_view": ("SELECT * FROM written", CAST_VIEW),
    "sql_view_of_a_view": (
        "SELECT * FROM over_written",
        CAST_VIEW,
        "CREATE TEMP VIEW over_written AS SELECT * FROM written WHERE id > 0",
    ),
    "except": (
        f"SELECT id, {WRITTEN} AS c FROM {SEED} "
        f"EXCEPT SELECT id, CAST('2026-01-01' AS timestamp_ns) FROM {SEED}",
    ),
    "except_of_a_plain_right_branch": (
        f"SELECT id, {WRITTEN} AS c FROM {SEED} EXCEPT SELECT id, c FROM {SEED} WHERE id > 5",
    ),
    "intersect": (
        f"SELECT id, {WRITTEN} AS c FROM {SEED} INTERSECT SELECT id, {WRITTEN} FROM {SEED}",
    ),
    "left_semi_join": (
        f"SELECT * FROM (SELECT id, {WRITTEN} AS c FROM {SEED}) a "
        f"LEFT SEMI JOIN {doors.MICRO} b ON a.id = b.id",
    ),
    "left_anti_join": (
        f"SELECT * FROM (SELECT id, {WRITTEN} AS c FROM {SEED}) a "
        f"LEFT ANTI JOIN {doors.MICRO} b ON a.id = b.id + 100",
    ),
}
UNFOLLOWED_SHAPES = {
    "sql_temp_view_of_the_plain_column": (
        "SELECT * FROM plain",
        f"CREATE TEMP VIEW plain AS SELECT id, c FROM {SEED}",
    ),
    "rollup": (f"SELECT {WRITTEN} AS c, count(*) AS n FROM {SEED} GROUP BY ROLLUP({WRITTEN})",),
    "scalar_subquery": (f"SELECT id, (SELECT max({WRITTEN}) FROM {SEED}) AS c FROM {SEED}",),
    "window_ordered_by_the_plain_column": (
        f"SELECT id, max({WRITTEN}) OVER (ORDER BY c) AS c FROM {SEED}",
    ),
    "lambda": (f"SELECT id, transform(array(id), x -> {WRITTEN})[0] AS c FROM {SEED}",),
}
OUTERMOST = {
    "rollup": (
        f"SELECT CAST(c AS timestamp_ns) AS c, n FROM (SELECT {WRITTEN} AS c, count(*) AS n "
        f"FROM {SEED} GROUP BY ROLLUP({WRITTEN})) q"
    ),
    "scalar_subquery": (
        f"SELECT id, CAST((SELECT max({WRITTEN}) FROM {SEED}) AS timestamp_ns) AS c FROM {SEED}"
    ),
    "window_ordered_by_the_plain_column": (
        f"SELECT id, CAST(max({WRITTEN}) OVER (ORDER BY c) AS timestamp_ns) AS c FROM {SEED}"
    ),
    "lambda": (
        f"SELECT id, CAST(transform(array(id), x -> {WRITTEN})[0] AS timestamp_ns) AS c FROM {SEED}"
    ),
}


@pytest.mark.parametrize("shape", list(FOLLOWED_SHAPES))
def test_a_written_cast_is_followed_through_views_set_operations_and_semi_joins(
    shape: str,
) -> None:
    """A SQL temp view, EXCEPT, INTERSECT and a semi or anti join carry the kept side's cast."""
    cell = doors.in_session("UTC", False, shape_cell, *FOLLOWED_SHAPES[shape])
    assert "error" not in cell, cell.get("error")
    assert cell["table"]["schema"][-1] == ["c", "timestamp_ns"]


@pytest.mark.parametrize("shape", list(UNFOLLOWED_SHAPES))
def test_a_written_cast_that_is_not_followed_is_refused_and_works_written_outermost(
    shape: str,
) -> None:
    """Grouping sets, a scalar subquery, a lambda and a window ordered by a plain column refuse."""
    cell = doors.in_session("UTC", False, shape_cell, *UNFOLLOWED_SHAPES[shape])
    assert cell["error"] == texts.not_requested("CTAS", "c")
    assert cell["table"] is None
    if shape in OUTERMOST:
        outer = doors.in_session("UTC", False, shape_cell, OUTERMOST[shape])
        assert "error" not in outer, outer.get("error")
        assert ["c", "timestamp_ns"] in outer["table"]["schema"]


def materialized_cell(spark: Any, warehouse: Path, method: str, outer: str) -> dict[str, Any]:
    """Create a v3 table from a frame whose cast was written before ``method`` hid its plan."""
    frame = getattr(spark.sql(f"SELECT id, {WRITTEN} AS c FROM {SEED}"), method)()
    if outer:
        frame = frame.selectExpr("id", outer)
    writer = frame.writeTo(TABLE).tableProperty("format-version", "3")
    return doors.measure(spark, warehouse, TABLE, writer.create)


@pytest.mark.parametrize("method", ["cache", "persist", "localCheckpoint"])
def test_a_cast_written_before_a_cache_is_hidden_and_one_written_after_it_is_an_ask(
    method: str,
) -> None:
    """``cache``, ``persist`` and ``localCheckpoint`` replace the plan with stored rows."""
    hidden = doors.in_session("UTC", False, materialized_cell, method, "")
    assert hidden["error"] == texts.not_requested("CTAS", "c")
    after = doors.in_session("UTC", False, materialized_cell, method, f"{WRITTEN} AS c")
    assert stored_type(after, "c") == "timestamp_ns"


DEEP = {
    "array_of_struct": "array(named_struct('a', {column}, 'n', 1))",
    "struct_of_struct": "named_struct('o', named_struct('i', {column}))",
    "struct_of_array_of_struct": "named_struct('s', array(named_struct('a', {column})))",
    "array_of_struct_of_array": "array(named_struct('a', array({column})))",
    "map_value_struct": "map('k', named_struct('a', {column}))",
    "map_of_map": "map('k', map('j', {column}))",
}


@pytest.mark.parametrize("column", doors.COLUMNS)
@pytest.mark.parametrize("shape", list(DEEP))
@pytest.mark.parametrize("seat", ["ctas", "write_to_append_merge_schema", "merge_schema_evolution"])
def test_a_nanosecond_field_two_and_three_levels_down_is_refused(
    seat: str, shape: str, column: str
) -> None:
    """The door looks at every depth of a list, struct and map, on create and evolving seats."""
    expression = f"{DEEP[shape].format(column=column)} AS n"
    cell = doors.in_session("UTC", False, outcome, seat, "v3", expression)
    expected = texts.not_requested(SEATS[seat][0], "n", doors.NANO_TYPES[column])
    assert cell["error"] == expected
    assert cell["files_unchanged"] is True


MERGE_SOURCES = {
    "timestamp_literal": ("TIMESTAMP '2026-01-02 03:04:05'", "timestamp_ns"),
    "cast_of_a_string": ("CAST('2026-01-02 03:04:05' AS TIMESTAMP)", "timestamp_ns"),
    "cast_of_the_naive_column": ("CAST(c AS TIMESTAMP)", "timestamp_ns"),
    "cast_of_the_zoned_column": ("CAST(cz AS TIMESTAMP)", "timestamp_ns"),
}


@pytest.mark.parametrize("source", list(MERGE_SOURCES))
def test_a_plain_timestamp_in_a_schema_evolving_merge_source_is_refused(source: str) -> None:
    """Inside a MERGE source ``TIMESTAMP`` is nanoseconds; the hint's own spelling stores micros."""
    expression, nano = MERGE_SOURCES[source]
    seat = "merge_schema_evolution"
    refused = doors.in_session("UTC", False, outcome, seat, "v3", f"{expression} AS x")
    expected = texts.not_requested(texts.MERGE_FORM, "x", nano)
    assert refused["error"] == expected
    assert refused["files_unchanged"] is True
    micro = expected["text"].rsplit("For microseconds write ", 1)[1].removesuffix(".")
    written = f"{micro.replace('<expression>', expression)} AS x"
    followed = doors.in_session("UTC", False, outcome, seat, "v3", written)
    assert stored_type(followed, "x") == "timestamp"
    as_a_frame = doors.in_session("UTC", False, outcome, "ctas", "v3", f"{expression} AS x")
    assert stored_type(as_a_frame, "x") in ("timestamp", "timestamptz")
