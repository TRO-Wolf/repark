"""C-2d live cells: the mounted Postgres source and the read_postgres door, against C-0."""

from __future__ import annotations

import datetime as dt
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from repark import ReparkSession

repark = pytest.importorskip("repark")
errors = pytest.importorskip("repark.errors")
facade_session = pytest.importorskip("repark.spark.session")

TYPED_COLUMNS = (
    "id int8 PRIMARY KEY, i2 int2, i4 int4, b bool, f8 float8, n numeric(10,2), nu numeric, "
    "t text, d date, tz timestamptz, ts timestamp, u uuid, j jsonb, iv interval"
)
TYPED_ROWS = (
    "(1, 7, -70000, true, 1.5, 12345.67, 0.5, 'héllo', '2024-02-29', "
    "'2024-03-01 12:00:00+00', '2024-03-01 12:00:00', "
    "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\": [1, 2]}', '1 day 2 hours'), "
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
)
EXPECTED_SQL = (
    "SELECT id, i2, i4, b, f8, n, nu, t, d, tz AT TIME ZONE 'UTC', ts, u::text, j::text, "
    "iv::text FROM {table} ORDER BY id"
)


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


def _typed_table(conn: Any, names: dict[str, str]) -> str:
    """Create and fill the every-mapped-type table; return its quoted name."""
    table = f'"{names["schema"]}".typed'
    conn.execute(f"CREATE TABLE {table} ({TYPED_COLUMNS})")
    conn.execute(f"INSERT INTO {table} VALUES {TYPED_ROWS}")
    return table


def _expected(conn: Any, table: str) -> list[tuple[Any, ...]]:
    """Read the rows through psycopg, the independent reference."""
    rows = conn.execute(EXPECTED_SQL.format(table=table)).fetchall()
    return [tuple(row) for row in rows]


def _actual(rows: list[Any]) -> list[tuple[Any, ...]]:
    """Flatten collected Rows into tuples."""
    return [tuple(row) for row in rows]


def _busy_backends(conn: Any, application_name: str) -> int:
    """Count the cell's backends that are not idle, waiting up to three seconds for zero."""
    busy = -1
    for _ in range(30):
        row = conn.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = %s AND state <> 'idle'",
            (application_name,),
        ).fetchone()
        busy = row[0]
        if busy == 0:
            break
        time.sleep(0.1)
    return busy


def test_a_mounted_source_reads_every_mapped_type(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    table = _typed_table(conn, names)
    frame = spark.sql(f"SELECT * FROM pg.{names['schema']}.typed ORDER BY id")
    assert _actual(frame.collect()) == _expected(conn, table)
    types = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
    assert types["n"] == "decimal(10,2)"
    assert types["nu"] == "decimal(38,18)"
    assert types["tz"] == "timestamp"
    assert types["ts"] == "timestamp"
    assert types["iv"] == "string"
    assert frame.schema["id"].nullable is False


def test_read_jdbc_and_format_postgres_match_the_mount(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    table = _typed_table(conn, names)
    expected = _expected(conn, table)
    jdbc_url = "jdbc:" + _url()
    by_jdbc = spark.read.jdbc(
        jdbc_url, f"{names['schema']}.typed", properties={"sslmode": "disable"}
    )
    by_query = (
        spark.read.format("postgres")
        .option("url", _url())
        .option("sslmode", "disable")
        .option("query", f"SELECT * FROM {table}")
        .load()
    )
    for frame in (by_jdbc, by_query):
        assert _actual(frame.orderBy("id").collect()) == expected
    pushed = by_jdbc.filter("i4 < 0").select("id").collect()
    assert _actual(pushed) == [(1,)]


def test_a_partitioned_read_refuses_naming_its_row(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    _typed_table(conn, names)
    target = f"{names['schema']}.typed"
    props = {"sslmode": "disable"}
    attempts = [
        lambda: spark.read.jdbc(
            _url(),
            target,
            column="id",
            lowerBound=0,
            upperBound=10,
            numPartitions=2,
            properties=props,
        ),
        lambda: spark.read.jdbc(_url(), target, predicates=["id < 2"], properties=props),
        lambda: (
            spark.read.format("postgres")
            .option("url", _url())
            .option("dbtable", target)
            .option("partitionColumn", "id")
            .option("lowerBound", "0")
            .option("upperBound", "10")
            .option("numPartitions", "2")
            .load()
        ),
    ]
    for attempt in attempts:
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            attempt()
        message = str(excinfo.value)
        assert "CONNECT-DECL-pg-partitioned-read" in message
        assert "postgres:repark@" not in message


def test_ddl_and_dml_refuse_through_both_doors(
    spark: ReparkSession,
    pg_live: tuple[Any, dict[str, str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, names = pg_live
    table = _typed_table(conn, names)
    qualified = f"pg.{names['schema']}.typed"
    for statement in (
        f"CREATE TABLE pg.{names['schema']}.fresh (a INT)",
        f"DROP TABLE {qualified}",
    ):
        with pytest.raises(Exception) as excinfo:
            spark.sql(statement).collect()
        assert "read-only" in str(excinfo.value), statement
    for statement in (
        f"INSERT INTO {qualified} (id) VALUES (3)",
        f"UPDATE {qualified} SET i4 = 0",
        f"DELETE FROM {qualified}",
    ):
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            spark.sql(statement).collect()
        assert "not implemented" in str(excinfo.value).lower(), statement
    monkeypatch.setenv("REPARK_CONFIG", str(tmp_path / "repark.toml"))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", None)
    for statement in (f"DROP TABLE {qualified}", f"DROP SCHEMA pg.{names['schema']}"):
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            repark.sql(statement).collect()
        message = str(excinfo.value)
        assert "database source `default.database.postgres.pg` is read-only" in message
        assert "CONNECT-DECL-pg-ddl" in message
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (2,)
    assert conn.execute(
        "SELECT count(*) FROM pg_tables WHERE schemaname = %s", (names["schema"],)
    ).fetchone() == (1,)


def test_ping_reaches_the_server_and_names_the_source_on_failure(
    tmp_path: Path, pg_live: tuple[Any, dict[str, str]]
) -> None:
    facade_session._reset_active_session_for_tests()
    path = _write_config(tmp_path, "[default.database.postgres.pg]")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            "[default.database.postgres.closed]\n"
            'host = "127.0.0.1"\nport = "1"\nuser = "postgres"\nsslmode = "disable"\n'
            'connect_timeout_ms = "2000"\n'
        )
    session = repark.ReparkSession.builder.configFile(str(path)).getOrCreate()
    try:
        assert session.source("pg").ping() is None
        with pytest.raises(Exception) as excinfo:
            session.source("closed").ping()
    finally:
        session.stop()
        facade_session._reset_active_session_for_tests()
    assert "database source `default.database.postgres.closed`" in str(excinfo.value)
    assert "unreachable" in str(excinfo.value)


def test_timestamp_is_placed_in_the_session_zone_or_kept_as_the_wall_clock(
    tmp_path: Path, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    conn.execute(f'CREATE TABLE "{names["schema"]}".clock (id int4, ts timestamp)')
    conn.execute(
        f'INSERT INTO "{names["schema"]}".clock VALUES '
        "(1, '2024-01-15 12:00:00'), (2, '2024-07-15 12:00:00'), (3, '2024-03-10 02:30:00')"
    )
    facade_session._reset_active_session_for_tests()
    path = _write_config(
        tmp_path, "[default.database.postgres.pg]", "[default.database.postgres.ntz]"
    )
    with path.open("a", encoding="utf-8") as handle:
        handle.write('prefer_timestamp_ntz = "true"\n')
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace(
            'sslmode = "disable"\n',
            f'sslmode = "disable"\napplication_name = "{names["schema"]}"\n',
            1,
        ),
        encoding="utf-8",
    )
    session = (
        repark.ReparkSession.builder.configFile(str(path))
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    try:
        placed = session.sql(
            f"SELECT id, CAST(ts AS STRING) AS wall, unix_timestamp(ts) AS epoch "
            f"FROM pg.{names['schema']}.clock WHERE id < 3 ORDER BY id"
        ).collect()
        wall = session.sql(f"SELECT ts FROM ntz.{names['schema']}.clock ORDER BY id")
        ntz_type = wall.schema["ts"].dataType.simpleString()
        ntz_rows = [row[0] for row in wall.collect()]
        with pytest.raises(errors.PySparkException) as excinfo:
            session.sql(f"SELECT ts FROM pg.{names['schema']}.clock WHERE id = 3").collect()
        lingering = _busy_backends(conn, names["schema"])
    finally:
        session.stop()
        facade_session._reset_active_session_for_tests()
    utc = dt.UTC
    assert [(row[0], row[1]) for row in placed] == [
        (1, "2024-01-15 12:00:00"),
        (2, "2024-07-15 12:00:00"),
    ]
    assert [row[2] for row in placed] == [
        int(dt.datetime(2024, 1, 15, 17, tzinfo=utc).timestamp()),
        int(dt.datetime(2024, 7, 15, 16, tzinfo=utc).timestamp()),
    ]
    assert ntz_type == "timestamp_ntz"
    assert ntz_rows[2] == dt.datetime(2024, 3, 10, 2, 30)
    assert "CONNECT-DIV-pg-timestamp-zone" in str(excinfo.value)
    assert "prefer_timestamp_ntz" in str(excinfo.value)
    assert lingering == 0


JAVA_FORM_ZONES = [
    ("Z", "2024-07-15 12:00:00"),
    ("UT", "2024-07-15 12:00:00"),
    ("GMT+8", "2024-07-15 04:00:00"),
    ("UTC+05:30", "2024-07-15 06:30:00"),
    ("-8", "2024-07-15 20:00:00"),
    ("+3", "2024-07-15 09:00:00"),
]


@pytest.mark.parametrize(("zone", "utc_wall"), JAVA_FORM_ZONES)
def test_a_java_form_session_zone_places_the_wall_clock_at_its_offset(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]], zone: str, utc_wall: str
) -> None:
    conn, names = pg_live
    conn.execute(f'CREATE TABLE "{names["schema"]}".c (id int4, ts timestamp)')
    conn.execute(f"INSERT INTO \"{names['schema']}\".c VALUES (1, '2024-07-15 12:00:00')")
    spark.conf.set("spark.sql.session.timeZone", zone)
    rows = spark.sql(
        f"SELECT CAST(ts AS STRING), unix_micros(ts), "
        f"unix_micros(ts) = unix_micros(TIMESTAMP '2024-07-15 12:00:00') "
        f"FROM pg.{names['schema']}.c"
    ).collect()
    instant = dt.datetime.fromisoformat(utc_wall).replace(tzinfo=dt.UTC)
    micros = int(instant.timestamp()) * 1_000_000
    assert [tuple(row) for row in rows] == [("2024-07-15 12:00:00", micros, True)]


def test_explain_shows_the_boundary_through_both_doors(
    spark: ReparkSession,
    pg_live: tuple[Any, dict[str, str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, names = pg_live
    _typed_table(conn, names)
    query = (
        f"SELECT id FROM pg.{names['schema']}.typed "
        "WHERE n > CAST(100 AS DECIMAL(10,2)) AND lower(t) = 'x'"
    )
    pushed = "pushed_filters=[n > Decimal128(Some(10000),10,2)]"
    residual = 'residual_filters=[lower(t) = Utf8("x")]'
    by_sql = "\n".join(row["plan"] for row in spark.sql(f"EXPLAIN {query}").collect())
    by_frame = spark.sql(query)._explain_text()
    monkeypatch.setenv("REPARK_CONFIG", str(tmp_path / "repark.toml"))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", None)
    verbose = "\n".join(row["plan"] for row in repark.sql(f"EXPLAIN VERBOSE {query}").collect())
    for text in (by_sql, by_frame, verbose):
        assert "PostgresScanExec: source=pg" in text
        assert pushed in text and residual in text
        assert "FilterExec: lower(t@" in text
        port = _url().rsplit(":", 1)[1].split("/")[0]
        for secret in ("127.0.0.1", "postgres:repark", f":{port}", "sslmode", "user="):
            assert secret not in text
    assert "current_setting('repark.p0')" in verbose
    assert "bound_values=1" in verbose
    assert "remote_sql" not in by_sql
    assert "100" not in verbose.split("remote_sql=")[1].split("bound_values")[0]


def test_unknown_keys_and_a_bad_url_refuse_before_any_connection(spark: ReparkSession) -> None:
    with pytest.raises(errors.IllegalArgumentException) as excinfo:
        spark.read.format("postgres").option("url", _url()).option("dbtable", "t").option(
            "bogusKey", "sentinel-value"
        ).load()
    assert "bogusKey" in str(excinfo.value)
    assert "sentinel-value" not in str(excinfo.value)
    assert "postgres:repark@" not in str(excinfo.value)
