"""C-4 step 2 live cells: Postgres INSERT routing on the Spark door and the writer.

Rows land through ``spark.sql`` and ``df.write.jdbc``; the take-report hook says
which path each write took and why a bulk request took rows.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from repark import ReparkSession

repark = pytest.importorskip("repark")
errors = pytest.importorskip("repark.errors")
facade_session = pytest.importorskip("repark.spark.session")
io_declared = pytest.importorskip("repark.spark.dataframe.io_declared")

TYPED_COLUMNS = (
    "id int8 PRIMARY KEY, i2 int2, i4 int4, b bool, f8 float8, n numeric(10,2), nu numeric, "
    "t text, d date, tz timestamptz, ts timestamp, u uuid, j jsonb, iv interval"
)
TYPED_COLUMNS_NO_INTERVAL = (
    "id int8 PRIMARY KEY, i2 int2, i4 int4, b bool, f8 float8, n numeric(10,2), nu numeric, "
    "t text, d date, tz timestamptz, ts timestamp, u uuid, j jsonb"
)
TYPED_ROWS = (
    "(1, 7, -70000, true, 1.5, 12345.67, 0.5, 'héllo', '2024-02-29', "
    "'2024-03-01 12:00:00+00', '2024-03-01 12:00:00', "
    "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\": [1, 2]}', '1 day 2 hours'), "
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
)
TYPED_ROWS_NO_INTERVAL = (
    "(1, 7, -70000, true, 1.5, 12345.67, 0.5, 'héllo', '2024-02-29', "
    "'2024-03-01 12:00:00+00', '2024-03-01 12:00:00', "
    "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\": [1, 2]}'), "
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
)
SEND_COLUMNS = (
    ("id", "int8send"),
    ("i2", "int2send"),
    ("i4", "int4send"),
    ("b", "boolsend"),
    ("f8", "float8send"),
    ("n", "numeric_send"),
    ("nu", "numeric_send"),
    ("t", "textsend"),
    ("d", "date_send"),
    ("tz", "timestamptz_send"),
    ("ts", "timestamp_send"),
    ("u", "uuid_send"),
    ("j", "jsonb_send"),
)

VIEW_FALLBACK = "the target is a view, which COPY cannot write"
RULE_FALLBACK = "the target has an INSERT rule, which COPY does not fire"
COLUMN_FALLBACK = "a written column's type has no COPY BINARY form here"


def _url() -> str:
    """Return the C-0 container URL the fixture already required."""
    return os.environ["REPARK_PG_URL"]


def _write_config(tmp_path: Path, *sources: str) -> Path:
    """Write a repark.toml mounting the container once per `[...]` source header given."""
    lines: list[str] = []
    for header in sources:
        lines += [header, f'url = "{_url()}"', 'sslmode = "disable"']
    path = tmp_path / "repark.toml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.fixture
def spark(tmp_path: Path, pg_live: tuple[Any, dict[str, str]]) -> Iterator[ReparkSession]:
    """A UTC session mounting the container as `pg`, after the live cell skips or opens."""
    assert pg_live
    facade_session._reset_active_session_for_tests()
    path = _write_config(tmp_path, "[default.database.postgres.pg]")
    session = (
        repark.ReparkSession.builder.configFile(str(path))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()
        facade_session._reset_active_session_for_tests()


def _take(spark: ReparkSession) -> tuple[str, int, str | None] | None:
    """Read back the last write report once through the C-4 test hook."""
    return io_declared.take_postgres_write_report(spark)


def _send_hex(
    conn: Any,
    table: str,
    extra: tuple[tuple[str, str], ...] = (),
    skip: tuple[str, ...] = (),
) -> list[tuple[Any, ...]]:
    """Read each kept column's `*_send` bytes as hex, in id order, through psycopg."""
    columns = [column for column in SEND_COLUMNS + extra if column[0] not in skip]
    selects = ", ".join(f"encode({send}({name}), 'hex')" for name, send in columns)
    return conn.execute(f"SELECT {selects} FROM {table} ORDER BY id").fetchall()


def _seed(conn: Any, schema: str, table: str, columns: str, rows: str) -> str:
    """Create and fill one table; return its quoted name."""
    quoted = f'"{schema}".{table}'
    conn.execute(f"CREATE TABLE {quoted} ({columns})")
    conn.execute(f"INSERT INTO {quoted} VALUES {rows}")
    return quoted


def test_rows_land_and_the_statement_returns_an_empty_frame(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """Append INSERT statements land through VALUES and SELECT; one empty row returns."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    conn.execute(f'CREATE TABLE "{schema}".u (id int8 PRIMARY KEY, name text)')
    returned = spark.sql(f"INSERT INTO pg.\"{schema}\".t (id, name) VALUES (1, 'a'), (2, 'b')")
    assert [tuple(row) for row in returned.collect()] == [()]
    assert _take(spark) == ("bulk", 2, None)
    spark.sql(f'INSERT INTO pg."{schema}".u SELECT * FROM pg."{schema}".t').collect()
    assert _take(spark) == ("bulk", 2, None)
    assert conn.execute(f'SELECT id, name FROM "{schema}".t ORDER BY id').fetchall() == [
        (1, "a"),
        (2, "b"),
    ]
    assert conn.execute(f'SELECT id, name FROM "{schema}".u ORDER BY id').fetchall() == [
        (1, "a"),
        (2, "b"),
    ]


def test_types_round_trip_through_the_read_path(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """Every declared type written through the door reads back byte-identical.

    The unconstrained `nu` compares by value: the seed's `0.5` carries dscale 1
    while the door writes the Arrow scale, the same value in different bytes.
    """
    conn, names = pg_live
    schema = names["schema"]
    source = _seed(conn, schema, "a", TYPED_COLUMNS, TYPED_ROWS)
    target = f'"{schema}".b'
    conn.execute(f"CREATE TABLE {target} ({TYPED_COLUMNS})")
    spark.sql(f'INSERT INTO pg."{schema}".b SELECT * FROM pg."{schema}".a').collect()
    assert _take(spark) == ("row", 2, COLUMN_FALLBACK)
    assert _send_hex(conn, target, (("iv", "interval_send"),), ("nu",)) == _send_hex(
        conn, source, (("iv", "interval_send"),), ("nu",)
    )
    assert (
        conn.execute(f"SELECT nu FROM {target} ORDER BY id").fetchall()
        == conn.execute(f"SELECT nu FROM {source} ORDER BY id").fetchall()
    )
    read_source = spark.sql(f'SELECT * FROM pg."{schema}".a ORDER BY id').collect()
    read_target = spark.sql(f'SELECT * FROM pg."{schema}".b ORDER BY id').collect()
    assert [tuple(row) for row in read_target] == [tuple(row) for row in read_source]


def test_column_list_and_column_order_forms(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """A reordered column list writes by name; unlisted columns take their defaults."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (a int8, b text, c int8 DEFAULT 7)')
    spark.sql(f"INSERT INTO pg.\"{schema}\".t (b, a) VALUES ('x', 1)").collect()
    assert _take(spark) == ("bulk", 1, None)
    spark.sql(f'INSERT INTO pg."{schema}".t (a) VALUES (2)').collect()
    assert _take(spark) == ("bulk", 1, None)
    assert conn.execute(f'SELECT a, b, c FROM "{schema}".t ORDER BY a').fetchall() == [
        (1, "x", 7),
        (2, None, 7),
    ]


def test_a_failed_write_stores_nothing(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """A duplicate key refuses the whole write; the table stays as it was, no report."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    spark.sql(f"INSERT INTO pg.\"{schema}\".t (id, name) VALUES (1, 'a')").collect()
    assert _take(spark) == ("bulk", 1, None)
    with pytest.raises(Exception) as excinfo:
        spark.sql(f"INSERT INTO pg.\"{schema}\".t (id, name) VALUES (2, 'b'), (1, 'c')").collect()
    assert "23505" in str(excinfo.value)
    assert _take(spark) is None
    assert conn.execute(f'SELECT id, name FROM "{schema}".t ORDER BY id').fetchall() == [(1, "a")]


def test_session_zone_timestamp_round_trips_as_the_wall_clock(
    tmp_path: Path, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """Under a DST zone, a read timestamp writes back as the wall clock it came from."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t1 (id int8 PRIMARY KEY, ts timestamp, tz timestamptz)')
    conn.execute(f'CREATE TABLE "{schema}".t2 (id int8 PRIMARY KEY, ts timestamp, tz timestamptz)')
    conn.execute(
        f"""INSERT INTO "{schema}".t1 VALUES
        (1, '2024-01-15 12:00:00', '2024-01-15 12:00:00+00'),
        (2, '2024-07-15 12:00:00.123456', '2024-07-15 12:00:00.5+00'),
        (3, NULL, NULL)"""
    )
    facade_session._reset_active_session_for_tests()
    path = _write_config(tmp_path, "[default.database.postgres.pg]")
    session = (
        repark.ReparkSession.builder.configFile(str(path))
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    try:
        session.sql(f'INSERT INTO pg."{schema}".t2 SELECT * FROM pg."{schema}".t1').collect()
        assert _take(session) == ("bulk", 3, None)
    finally:
        session.stop()
        facade_session._reset_active_session_for_tests()
    rows = conn.execute(f'SELECT id, ts::text, tz::text FROM "{schema}".t2 ORDER BY id').fetchall()
    assert rows == [
        (1, "2024-01-15 12:00:00", "2024-01-15 12:00:00+00"),
        (2, "2024-07-15 12:00:00.123456", "2024-07-15 12:00:00.5+00"),
        (3, None, None),
    ]


def test_view_target_takes_the_row_path_and_says_why(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """Writes through a view reach its base table by rows, saying why, on both doors."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    conn.execute(f'CREATE VIEW "{schema}".v AS SELECT * FROM "{schema}".t')
    spark.sql(f"INSERT INTO pg.\"{schema}\".v (id, name) VALUES (1, 'a')").collect()
    assert _take(spark) == ("row", 1, VIEW_FALLBACK)
    frame = spark.createDataFrame([(2, "b")], "id long, name string")
    frame.write.jdbc(_url(), f"{schema}.v", mode="append", properties={"sslmode": "disable"})
    assert _take(spark) == ("row", 1, VIEW_FALLBACK)
    assert conn.execute(f'SELECT id, name FROM "{schema}".t ORDER BY id').fetchall() == [
        (1, "a"),
        (2, "b"),
    ]


def test_insert_rule_target_takes_the_row_path_and_says_why(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """A DO INSTEAD NOTHING rule fires on both doors; nothing is stored either way."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    conn.execute(f'CREATE RULE no_rows AS ON INSERT TO "{schema}".t DO INSTEAD NOTHING')
    spark.sql(f"INSERT INTO pg.\"{schema}\".t (id, name) VALUES (1, 'a')").collect()
    assert _take(spark) == ("row", 0, RULE_FALLBACK)
    frame = spark.createDataFrame([(2, "b")], "id long, name string")
    frame.write.jdbc(_url(), f"{schema}.t", mode="append", properties={"sslmode": "disable"})
    assert _take(spark) == ("row", 0, RULE_FALLBACK)
    assert conn.execute(f'SELECT count(*) FROM "{schema}".t').fetchone() == (0,)


def test_named_identity_column_refuses_with_the_core_refusal(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """A named GENERATED ALWAYS identity column refuses with the core text, both doors.

    The class is the door's `External` flattening (`PySparkException`), as for every
    connect error through a door; the sentence is the core's, naming the column.
    """
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(
        f'CREATE TABLE "{schema}".g (id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text)'
    )
    with pytest.raises(errors.PySparkException) as excinfo:
        spark.sql(f"INSERT INTO pg.\"{schema}\".g (id, name) VALUES (1, 'a')").collect()
    assert type(excinfo.value).__name__ == "PySparkException"
    message = str(excinfo.value)
    assert "GENERATED ALWAYS" in message
    assert "`id`" in message
    assert _take(spark) is None
    frame = spark.createDataFrame([(1, "a")], "id long, name string")
    with pytest.raises(errors.PySparkException) as excinfo:
        frame.write.jdbc(_url(), f"{schema}.g", mode="append", properties={"sslmode": "disable"})
    assert type(excinfo.value).__name__ == "PySparkException"
    message = str(excinfo.value)
    assert "GENERATED ALWAYS" in message
    assert "`id`" in message
    assert _take(spark) is None
    assert conn.execute(f'SELECT count(*) FROM "{schema}".g').fetchone() == (0,)


def test_bulk_and_row_leave_byte_identical_tables(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """The default and write.path=row writer writes agree byte for byte on every type."""
    conn, names = pg_live
    schema = names["schema"]
    source = _seed(conn, schema, "a", TYPED_COLUMNS_NO_INTERVAL, TYPED_ROWS_NO_INTERVAL)
    bulk_table = f'"{schema}".b'
    row_table = f'"{schema}".c'
    conn.execute(f"CREATE TABLE {bulk_table} ({TYPED_COLUMNS_NO_INTERVAL})")
    conn.execute(f"CREATE TABLE {row_table} ({TYPED_COLUMNS_NO_INTERVAL})")
    frame = spark.sql(f'SELECT * FROM pg."{schema}".a ORDER BY id')
    frame.write.jdbc(_url(), f"{schema}.b", mode="append", properties={"sslmode": "disable"})
    assert _take(spark) == ("bulk", 2, None)
    frame.write.option("write.path", "row").jdbc(
        _url(), f"{schema}.c", mode="append", properties={"sslmode": "disable"}
    )
    assert _take(spark) == ("row", 2, None)
    assert _send_hex(conn, bulk_table) == _send_hex(conn, row_table)
    assert _send_hex(conn, bulk_table, (), ("nu",)) == _send_hex(conn, source, (), ("nu",))
    assert (
        conn.execute(f"SELECT nu FROM {bulk_table} ORDER BY id").fetchall()
        == conn.execute(f"SELECT nu FROM {source} ORDER BY id").fetchall()
    )


def test_writer_door_appends_and_reports(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """df.write.jdbc and format('jdbc').save append; each write leaves its report."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    frame = spark.createDataFrame([(1, "a")], "id long, name string")
    returned = frame.write.jdbc(
        _url(), f"{schema}.t", mode="append", properties={"sslmode": "disable"}
    )
    assert returned is None
    assert _take(spark) == ("bulk", 1, None)
    writer = spark.createDataFrame([(2, "b")], "id long, name string").write
    writer.format("jdbc").option("url", _url()).option("dbtable", f"{schema}.t").option(
        "sslmode", "disable"
    ).mode("append").save()
    assert _take(spark) == ("bulk", 1, None)
    assert conn.execute(f'SELECT id, name FROM "{schema}".t ORDER BY id').fetchall() == [
        (1, "a"),
        (2, "b"),
    ]


def test_refused_modes_name_the_row_on_every_door(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """Non-append modes refuse under CONNECT-DECL-pg-write-modes on SQL and the writer."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    with pytest.raises(errors.UnsupportedOperationException) as excinfo:
        spark.sql(f"INSERT OVERWRITE pg.\"{schema}\".t SELECT 1, 'a'").collect()
    assert "CONNECT-DECL-pg-write-modes" in str(excinfo.value)
    frame = spark.createDataFrame([(1, "a")], "id long, name string")
    for mode in ("overwrite", "ignore", "error"):
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            frame.write.jdbc(_url(), f"{schema}.t", mode=mode, properties={"sslmode": "disable"})
        assert "CONNECT-DECL-pg-write-modes" in str(excinfo.value), mode
    assert conn.execute(f'SELECT count(*) FROM "{schema}".t').fetchone() == (0,)


def test_row_changing_statements_refuse_on_both_doors(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    """UPDATE names the upsert row; REPLACE parses nowhere here; MERGE/CTAS keep pg-ddl."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int8 PRIMARY KEY, name text)')
    with pytest.raises(errors.UnsupportedOperationException) as excinfo:
        spark.sql(f"UPDATE pg.\"{schema}\".t SET name = 'x'").collect()
    assert "CONNECT-DECL-pg-write-upsert" in str(excinfo.value)
    with pytest.raises(errors.ParseException) as excinfo:
        spark.sql(f"REPLACE INTO pg.\"{schema}\".t (id, name) VALUES (1, 'x')").collect()
    assert "Unsupported statement REPLACE" in str(excinfo.value)
    merge = (
        f'MERGE INTO pg."{schema}".t USING (SELECT 1) AS s ON true '
        "WHEN MATCHED THEN UPDATE SET name = 'x'"
    )
    for statement in (merge, f'CREATE TABLE pg."{schema}".fresh AS SELECT 1'):
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            spark.sql(statement).collect()
        assert "CONNECT-DECL-pg-ddl" in str(excinfo.value), statement
    assert conn.execute(f'SELECT count(*) FROM "{schema}".t').fetchone() == (0,)
