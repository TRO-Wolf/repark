"""NS-TRUTHUP-1: UPDATE and DELETE lower the nanosecond cast spellings.

ICE-TSNS-SQL-1-R-010, re-measured 2026-10-26: ``CAST(... AS timestamp_ns)`` and
``CAST(... AS timestamptz_ns)`` inside ``UPDATE ... SET``, ``UPDATE ... WHERE`` and
``DELETE ... WHERE`` store and match. The 2026-10-09 claim refused them.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

VALUE = "2026-01-02 03:04:05.123456789"
OTHER = "2020-05-06 07:08:09.5"
V3 = "TBLPROPERTIES ('format-version' = '3')"
TS_TICKS = 1767323045123456789
TZ_TICKS = {"UTC": 1767323045123456789, "America/New_York": 1767341045123456789}


def open_session(warehouse: Path, zone: str) -> Any:
    """Open a v3-enabled ANSI session in zone over warehouse with namespace ns."""
    session = (
        ReparkSession.builder.appName("ns-truthup-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(warehouse))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    return session


def stored_ticks(session: Any, table: str, column: str, where: str) -> list[int | None]:
    """Read column of table at where as int64 nanoseconds ordered by id."""
    arrow = session.sql(f"SELECT {column} FROM {table} WHERE {where} ORDER BY id").to_arrow()
    return list(arrow.column(0).cast(pa.int64()).to_pylist())


def stored_ids(session: Any, table: str) -> list[int]:
    """Read the ordered id column of table."""
    arrow = session.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    return list(arrow.column(0).to_pylist())


@pytest.mark.parametrize("zone", ["UTC", "America/New_York"])
@pytest.mark.parametrize("target", ["timestamp_ns", "timestamptz_ns"])
@pytest.mark.parametrize("position", ["set", "where", "delete"])
def test_update_and_delete_lower_the_nanosecond_cast_spellings(
    tmp_path: Path, zone: str, target: str, position: str
) -> None:
    """R-010 stores the cast value in SET and matches it in WHERE and DELETE."""
    column = "ts" if target == "timestamp_ns" else "tz"
    session = open_session(tmp_path, zone)
    try:
        session.sql(
            f"CREATE TABLE ice.ns.t (id INT, ts timestamp_ns, tz timestamptz_ns) USING iceberg {V3}"
        ).collect()
        session.sql(
            "INSERT INTO ice.ns.t VALUES "
            f"(1, CAST('{VALUE}' AS timestamp_ns), CAST('{VALUE}' AS timestamptz_ns)), "
            f"(2, CAST('{OTHER}' AS timestamp_ns), CAST('{OTHER}' AS timestamptz_ns))"
        ).collect()
        if position == "set":
            session.sql(
                f"UPDATE ice.ns.t SET {column} = CAST('{VALUE}' AS {target}) WHERE id = 2"
            ).collect()
            expected = TS_TICKS if column == "ts" else TZ_TICKS[zone]
            assert stored_ticks(session, "ice.ns.t", column, "id = 2") == [expected]
        elif position == "where":
            session.sql(
                f"UPDATE ice.ns.t SET id = 99 WHERE {column} = CAST('{VALUE}' AS {target})"
            ).collect()
            assert stored_ids(session, "ice.ns.t") == [2, 99]
        else:
            session.sql(
                f"DELETE FROM ice.ns.t WHERE {column} = CAST('{VALUE}' AS {target})"
            ).collect()
            assert stored_ids(session, "ice.ns.t") == [2]
    finally:
        session.stop()


def described_types(session: Any, statement: str) -> dict[str, str]:
    """Return the col_name to data_type rows of a DESCRIBE statement for ts and tz."""
    rows = session.sql(statement).to_arrow().to_pylist()
    return {
        str(row["col_name"]): str(row["data_type"])
        for row in rows
        if row["col_name"] in ("ts", "tz")
    }


@pytest.mark.parametrize(
    "spelling", ["DESCRIBE TABLE ice.ns.t", "DESCRIBE TABLE EXTENDED ice.ns.t"]
)
def test_describe_shows_the_spec_names_for_nanosecond_columns(
    tmp_path: Path, spelling: str
) -> None:
    """R-001 shows timestamp_ns and timestamptz_ns; the v2 control keeps its names."""
    session = open_session(tmp_path, "UTC")
    try:
        session.sql(
            f"CREATE TABLE ice.ns.t (ts timestamp_ns, tz timestamptz_ns) USING iceberg {V3}"
        ).collect()
        assert described_types(session, spelling) == {
            "ts": "timestamp_ns",
            "tz": "timestamptz_ns",
        }
        session.sql(
            "CREATE TABLE ice.ns.c (ts timestamp_ntz, tz timestamp) USING iceberg "
            "TBLPROPERTIES ('format-version' = '2')"
        ).collect()
        control = spelling.replace("ice.ns.t", "ice.ns.c")
        assert described_types(session, control) == {
            "ts": "timestamp_ntz",
            "tz": "timestamp",
        }
    finally:
        session.stop()
