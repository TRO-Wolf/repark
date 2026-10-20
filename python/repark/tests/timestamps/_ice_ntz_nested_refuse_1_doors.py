"""ICE-NTZ-NESTED-REFUSE-1: the nested TIMESTAMP_NTZ door cells shared by Spark and RePark.

The shapes are PR #1037's measured matrix (``task/wo/ice-ntz-nested-refuse-1/evidence``), cut
to twelve doors. One function runs a cell on either engine; one function states the ruled
RePark answer from the Spark answer.

pins: ice-ntz-nested-refuse-1/C-010, C-011
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

INSTANT = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
ZONES = ("UTC", "America/New_York", "Asia/Kolkata")
INSTANT_SOURCES = ("ts_literal", "ts_column")
SOURCES = (*INSTANT_SOURCES, "ntz_literal", "ntz_cast", "ntz_cast_column")
COLUMN_SOURCES = ("ts_column", "ntz_cast_column")
DOORS = (
    "insert_values",
    "insert_select",
    "insert_by_name",
    "insert_overwrite",
    "merge_insert",
    "merge_insert_star",
    "merge_update_whole",
    "merge_update_field",
    "update_field_where",
    "update_whole_where",
    "df_append",
    "df_insert_into",
)
SEEDED_DOORS = (
    "insert_overwrite",
    "merge_update_whole",
    "merge_update_field",
    "update_field_where",
    "update_whole_where",
)
CAST_REFUSED_DOORS = ("update_whole_where",)
REFUSAL = (
    "A TIMESTAMP value is not written into a nested TIMESTAMP_NTZ field while the session time "
    "zone is not UTC"
)


def leaf_sql(source: str, place: str) -> str:
    """Return the SQL of the value for ``st.v``; ``place`` is ``s``, ``src`` or ``bare``."""
    column = {"s": "s.l", "src": "l", "bare": "(SELECT l FROM {catalog}.ns.src)"}[place]
    spelled = {
        "ts_literal": INSTANT,
        "ts_column": column,
        "ntz_literal": WALL,
        "ntz_cast": f"CAST({INSTANT} AS TIMESTAMP_NTZ)",
        "ntz_cast_column": f"CAST({column} AS TIMESTAMP_NTZ)",
    }
    return spelled[source]


def struct_sql(source: str, place: str) -> str:
    """Return ``named_struct('v', <value>, 'n', 1)``."""
    return f"named_struct('v', {leaf_sql(source, place)}, 'n', 1)"


def projection(source: str) -> str:
    """Return a one-row SELECT shaped as the target's columns."""
    value = struct_sql(source, "src")
    if source in COLUMN_SOURCES:
        return f"SELECT id, {value} AS st, 0 AS k FROM {{catalog}}.ns.src"
    return f"SELECT 1 AS id, {value} AS st, 0 AS k"


def door_sql(door: str, source: str) -> str:
    """Return the statement of one SQL door, with ``{t}`` and ``{catalog}`` left open."""
    place = "s" if source in COLUMN_SOURCES else "bare"
    using = "{catalog}.ns.src s" if source in COLUMN_SOURCES else "(SELECT 1 AS id) s"
    statements = {
        "insert_values": f"INSERT INTO {{t}} VALUES (1, {struct_sql(source, 'bare')}, 0)",
        "insert_select": f"INSERT INTO {{t}} {projection(source)}",
        "insert_by_name": f"INSERT INTO {{t}} BY NAME {projection(source)}",
        "insert_overwrite": f"INSERT OVERWRITE {{t}} {projection(source)}",
        "merge_insert": (
            f"MERGE INTO {{t}} t USING {using} ON t.id = s.id WHEN NOT MATCHED THEN "
            f"INSERT (id, st, k) VALUES (s.id, {struct_sql(source, place)}, 0)"
        ),
        "merge_insert_star": (
            f"MERGE INTO {{t}} t USING ({projection(source)}) s ON t.id = s.id "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
        "merge_update_whole": (
            f"MERGE INTO {{t}} t USING {using} ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET t.st = {struct_sql(source, place)}"
        ),
        "merge_update_field": (
            f"MERGE INTO {{t}} t USING {using} ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET t.st.v = {leaf_sql(source, place)}"
        ),
        "update_field_where": f"UPDATE {{t}} SET st.v = {leaf_sql(source, 'bare')} WHERE id >= 0",
        "update_whole_where": f"UPDATE {{t}} SET st = {struct_sql(source, 'bare')} WHERE id >= 0",
    }
    return statements[door]


def seed(spark: Any, catalog: str) -> None:
    """Create ``<catalog>.ns.src`` holding the instant."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    spark.sql(
        f"CREATE TABLE {catalog}.ns.src (id INT, l TIMESTAMP) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {catalog}.ns.src VALUES (1, {INSTANT})")


def run_cell(spark: Any, catalog: str, door: str, source: str, table: str) -> dict[str, Any]:
    """Run one door into a fresh ``table`` and return its stored ticks or its first error line."""
    spark.sql(
        f"CREATE TABLE {table} (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    if door in SEEDED_DOORS:
        spark.sql(
            f"INSERT INTO {table} VALUES "
            "(1, named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0)"
        )
    try:
        if door == "df_append":
            spark.sql(projection(source).format(catalog=catalog)).writeTo(table).append()
        elif door == "df_insert_into":
            spark.sql(projection(source).format(catalog=catalog)).write.insertInto(table)
        else:
            spark.sql(door_sql(door, source).format(t=table, catalog=catalog))
    except Exception as error:
        lines = [line.strip() for line in str(error).splitlines() if line.strip()]
        return {"error": lines[0] if lines else type(error).__name__}
    stored = spark.sql(f"SELECT st.v AS v FROM {table} WHERE st.v IS NOT NULL").toArrow()
    ticks = stored.column("v").combine_chunks().cast(pa.int64()).to_pylist()
    return {"stored": ticks}


def cells() -> list[tuple[str, str]]:
    """Return every ``(door, source)`` cell.

    ``INSERT … VALUES`` with a column source is a scalar subquery inside ``VALUES``, which both
    engines refuse with their own text; it is not a cell of this unit.
    """
    return [
        (door, source)
        for door in DOORS
        for source in SOURCES
        if not (door == "insert_values" and source in COLUMN_SOURCES)
    ]


def ruled_answer(door: str, source: str, zone: str, spark_answer: dict[str, Any]) -> str:
    """Return what RePark must answer: ``refused`` or Spark's own answer.

    Outside UTC an instant source refuses on every door, and the hint's ``CAST`` spelling
    refuses on the plain ``UPDATE`` that assigns the whole struct. Every other cell is Spark's.
    """
    if zone != "UTC" and source in INSTANT_SOURCES:
        return "refused"
    if zone != "UTC" and source.startswith("ntz_cast") and door in CAST_REFUSED_DOORS:
        return "refused"
    return str(spark_answer.get("stored"))


def repark_answer(answer: dict[str, Any]) -> str:
    """Return a RePark cell as ``refused`` (this unit's text) or its stored ticks."""
    if "error" in answer:
        return "refused" if REFUSAL in answer["error"] else f"error: {answer['error']}"
    return str(answer["stored"])
