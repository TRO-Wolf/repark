"""C-3 live cells: partitioned Postgres reads through the three doors, against C-0."""

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from repark import ReparkSession

repark = pytest.importorskip("repark")
errors = pytest.importorskip("repark.errors")
facade_session = pytest.importorskip("repark.spark.session")

ROWS = 2000
ROW = "CONNECT-DECL-pg-partitioned-read"
ALL_OR_NONE = (
    "When reading JDBC data sources, users need to specify all or none for the following "
    "options: 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'"
)
WIDE = (
    "CREATE TABLE {table} (id int8 PRIMARY KEY, k int4, small int2, n numeric(12,2), "
    "nu numeric, f8 float8, t text, d date, tz timestamptz, ts timestamp, u uuid, j jsonb, "
    "iv interval, b bool)"
)
FILL = (
    "INSERT INTO {table} SELECT g, "
    "CASE WHEN g % 97 = 0 THEN NULL WHEN g % 89 = 0 THEN -100000 - g "
    "WHEN g % 83 = 0 THEN 100000 + g ELSE (g * 7919) % 1000 END, "
    "(g % 300)::int2 - 150, g * 1.25, g / 8.0, g * 0.5, 'n' || g, "
    "DATE '2000-01-01' + (g % 9000)::int, "
    "TIMESTAMPTZ '2001-01-01 00:00:00+00' + g * INTERVAL '1 second', "
    "TIMESTAMP '2001-01-01' + g * INTERVAL '1 minute', md5(g::text)::uuid, "
    "jsonb_build_object('g', g), g * INTERVAL '1 hour', g % 2 = 0 "
    "FROM generate_series(1, {rows}) g"
)
EXPECTED_SQL = (
    "SELECT id, k, small, n, nu, f8, t, d, tz AT TIME ZONE 'UTC', ts, u::text, j::text, "
    "iv::text, b FROM {table} ORDER BY id"
)
PLAIN = {"sslmode": "disable"}


def _url() -> str:
    """Return the C-0 container URL the fixture already required."""
    return os.environ["REPARK_PG_URL"]


@pytest.fixture
def spark(pg_live: tuple[Any, dict[str, str]]) -> Iterator[ReparkSession]:
    """A UTC session with no mounted source: every read goes through the read door."""
    assert pg_live
    facade_session._reset_active_session_for_tests()
    session = repark.ReparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        facade_session._reset_active_session_for_tests()


def _wide(conn: Any, names: dict[str, str], rows: int = ROWS) -> tuple[str, str]:
    """Create and fill the wide table; return its quoted name and its `dbtable` spelling."""
    table = f'"{names["schema"]}".wide'
    conn.execute(WIDE.format(table=table))
    conn.execute(FILL.format(table=table, rows=rows))
    return table, f"{names['schema']}.wide"


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    """Collect a frame ordered by id into tuples."""
    return [tuple(row) for row in frame.orderBy("id").collect()]


def _types(frame: Any) -> list[tuple[str, str]]:
    """Name and Spark type of every field; a subquery's columns are all nullable."""
    return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]


def _jdbc(spark: ReparkSession, target: str, column: str, bounds: tuple[int, int, int]) -> Any:
    """Read `target` through `spark.read.jdbc` partitioned on `column`."""
    lower, upper, count = bounds
    return spark.read.jdbc(
        _url(),
        target,
        column=column,
        lowerBound=lower,
        upperBound=upper,
        numPartitions=count,
        properties=PLAIN,
    )


def _options(spark: ReparkSession, **options: str) -> Any:
    """Build a `format("postgres")` reader over the container with the given options."""
    reader = spark.read.format("postgres").option("url", _url()).option("sslmode", "disable")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def test_the_three_doors_partition_and_equal_the_unpartitioned_read(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    table, target = _wide(conn, names)
    expected = [tuple(row) for row in conn.execute(EXPECTED_SQL.format(table=table)).fetchall()]
    assert len(expected) == ROWS
    plain = spark.read.jdbc(_url(), target, properties=PLAIN)
    assert _rows(plain) == expected
    spelled = {
        "partitionColumn": "k",
        "lowerBound": "0",
        "upperBound": "1000",
        "numPartitions": "4",
    }
    subquery = f"(SELECT * FROM {table}) AS sub"
    doors = {
        "jdbc": _jdbc(spark, target, "k", (0, 1000, 4)),
        "format": _options(spark, dbtable=target, **spelled).load(),
        "properties": spark.read.jdbc(_url(), target, properties={**PLAIN, **spelled}),
        "read_postgres": spark.read_postgres(
            _url(),
            dbtable=target,
            properties=PLAIN,
            partition_column="K",
            lower_bound=0,
            upper_bound=1000,
            num_partitions=4,
        ),
        "subquery": _options(spark, dbtable=subquery, **spelled).load(),
        "int8": _jdbc(spark, target, "id", (1, ROWS, 8)),
        "int2": _jdbc(spark, target, "small", (-150, 150, 3)),
        "sixteen": _jdbc(spark, target, "k", (0, 1000, 16)),
        "outside": _jdbc(spark, target, "k", (5_000_000, 6_000_000, 4)),
    }
    strides = {"int8": 8, "int2": 3, "sixteen": 16}
    for door, frame in doors.items():
        assert _types(frame) == _types(plain), door
        assert _rows(frame) == expected, door
        plan = frame._explain_text()
        assert f"strides={strides.get(door, 4)}, max_connections=" in plan, door
    assert "max_connections=4" in doors["sixteen"]._explain_text()
    assert "partition_column=k" in doors["read_postgres"]._explain_text()


def test_null_and_out_of_bounds_rows_arrive_exactly_once(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    table, target = _wide(conn, names)
    frame = _jdbc(spark, target, "k", (0, 1000, 4))
    ids = sorted(row[0] for row in frame.select("id").collect())
    assert ids == list(range(1, ROWS + 1))
    nulls = conn.execute(f"SELECT count(*) FROM {table} WHERE k IS NULL").fetchone()[0]
    outside = conn.execute(f"SELECT count(*) FROM {table} WHERE k < 0 OR k > 1000").fetchone()[0]
    assert nulls > 15
    assert outside > 40
    assert frame.filter("k IS NULL").count() == nulls
    assert frame.filter("k < 0 OR k > 1000").count() == outside


def test_spark_refusals_keep_sparks_class_and_words(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    _, target = _wide(conn, names, rows=10)
    full = {
        "partitionColumn": "k",
        "lowerBound": "0",
        "upperBound": "9",
        "numPartitions": "4",
    }
    reversed_bounds = {**full, "lowerBound": "200", "upperBound": "0"}
    cases: list[tuple[Callable[[], Any], type[Exception], str]] = [
        (
            lambda: spark.read.jdbc(_url(), target, properties={**PLAIN, "partitionColumn": "k"}),
            errors.IllegalArgumentException,
            ALL_OR_NONE,
        ),
        (
            lambda: _options(spark, dbtable=target, **reversed_bounds).load(),
            errors.IllegalArgumentException,
            "Operation not allowed: the lower bound of partitioning column is larger than the "
            "upper bound. Lower bound: 200; Upper bound: 0",
        ),
        (
            lambda: _options(spark, query=f"SELECT id, k FROM {target}", **full).load(),
            errors.IllegalArgumentException,
            "Options 'query' and 'partitionColumn' can not be specified together",
        ),
        (
            lambda: _jdbc(spark, target, "nope", (0, 9, 4)),
            errors.AnalysisException,
            "User-defined partition column nope not found in the JDBC relation: id, k, small",
        ),
        (
            lambda: _jdbc(spark, target, "t", (0, 9, 4)),
            errors.AnalysisException,
            "Partition column type should be numeric, date, or timestamp, but string found.",
        ),
        (
            lambda: _jdbc(spark, target, "b", (0, 1, 2)),
            errors.AnalysisException,
            "Partition column type should be numeric, date, or timestamp, but boolean found.",
        ),
        (
            lambda: _options(spark, dbtable=target, **{**full, "lowerBound": "abc"}).load(),
            errors.NumberFormatException,
            "lowerBound",
        ),
        (
            lambda: spark.read.jdbc(
                _url(), target, properties={**PLAIN, **full, "upperBound": "1.5"}
            ),
            errors.NumberFormatException,
            "`upperBound` must be a 64-bit integer",
        ),
        (
            lambda: _jdbc(spark, target, "k", (0, 2**63, 4)),
            errors.NumberFormatException,
            "upperBound",
        ),
        (
            lambda: _options(spark, dbtable=target, partitionColumn="k", numPartitions="4").load(),
            errors.IllegalArgumentException,
            "together",
        ),
    ]
    for attempt, expected, words in cases:
        with pytest.raises(expected) as excinfo:
            attempt()
        message = str(excinfo.value)
        assert words in message
        assert "postgres:repark@" not in message


def test_one_stride_cases_read_unpartitioned_as_spark_does(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    table, target = _wide(conn, names, rows=50)
    expected = [tuple(row) for row in conn.execute(EXPECTED_SQL.format(table=table)).fetchall()]
    frames = {
        "count alone": _options(spark, dbtable=target, numPartitions="4").load(),
        "count one": _jdbc(spark, target, "k", (0, 1000, 1)),
        "count zero": _jdbc(spark, target, "k", (0, 1000, 0)),
        "count negative": _jdbc(spark, target, "k", (0, 1000, -1)),
        "equal bounds": _jdbc(spark, target, "k", (50, 50, 4)),
        "a span of one": _jdbc(spark, target, "k", (7, 8, 31)),
        "reversed at one": _jdbc(spark, target, "k", (9, 0, 1)),
    }
    for case, frame in frames.items():
        assert _rows(frame) == expected, case
        assert "strides=" not in frame._explain_text(), case
    with pytest.raises(errors.AnalysisException):
        _jdbc(spark, target, "nope", (0, 1000, 1))


def test_what_stays_declared_refuses_naming_its_row(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    _, target = _wide(conn, names, rows=10)
    attempts: list[Callable[[], Any]] = [
        lambda: spark.read.jdbc(_url(), target, predicates=["id < 2"], properties=PLAIN),
        lambda: spark.read.jdbc(_url(), target, properties={**PLAIN, "predicates": "id < 2"}),
    ]
    for column in ("d", "tz", "ts", "n", "nu", "f8"):
        attempts.append(lambda column=column: _jdbc(spark, target, column, (0, 9, 4)))
    sparks_spellings = {
        "d": ("2024-01-01", "2024-02-01"),
        "ts": ("2024-01-01 00:00:00", "2024-01-02 00:00:00"),
        "tz": ("2024-01-01", "2024-01-02"),
    }
    for column, (lower, upper) in sparks_spellings.items():
        spelled = {
            "partitionColumn": column,
            "lowerBound": lower,
            "upperBound": upper,
            "numPartitions": "4",
        }
        attempts.append(lambda spelled=spelled: _options(spark, dbtable=target, **spelled).load())
        attempts.append(
            lambda spelled=spelled: spark.read.jdbc(_url(), target, properties={**PLAIN, **spelled})
        )
    for attempt in attempts:
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            attempt()
        message = str(excinfo.value)
        assert ROW in message
        assert "postgres:repark@" not in message
        assert "id < 2" not in message
        assert "2024-01-0" not in message
    with pytest.raises(errors.NumberFormatException, match="`lowerBound` must be"):
        spark.read.jdbc(
            _url(),
            target,
            properties={
                **PLAIN,
                "partitionColumn": "k",
                "lowerBound": "2024-01-01",
                "upperBound": "9",
                "numPartitions": "4",
            },
        )


def test_num_partitions_is_sparks_int_and_strides_have_a_ceiling(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    _, target = _wide(conn, names, rows=50)
    past_int: list[Callable[[], Any]] = [
        lambda: _jdbc(spark, target, "k", (0, 1000, 3_000_000_000)),
        lambda: _options(
            spark,
            dbtable=target,
            partitionColumn="k",
            lowerBound="0",
            upperBound="1000",
            numPartitions="3000000000",
        ).load(),
    ]
    for attempt in past_int:
        with pytest.raises(errors.NumberFormatException, match="`numPartitions` must be a 32-bit"):
            attempt()
    shrunk = _jdbc(spark, target, "k", (0, 3, 2_147_483_647))
    assert "strides=3, " in shrunk._explain_text()
    assert shrunk.count() == 50
    at_the_ceiling = _jdbc(spark, target, "id", (0, 10_000_000, 10_000))
    assert "strides=10000, " in at_the_ceiling._explain_text()
    for count, bounds in ((10_001, (0, 10_000_000)), (2_147_483_647, (0, 10_001))):
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            _jdbc(spark, target, "id", (*bounds, count))
        message = str(excinfo.value)
        assert ROW in message
        assert "10001 strides" in message
        assert "at most 10000 strides" in message


def test_filter_projection_and_limit_compose_with_the_strides(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    _, target = _wide(conn, names)
    plain = spark.read.jdbc(_url(), target, properties=PLAIN)
    parts = _jdbc(spark, target, "k", (0, 1000, 4))
    unpushed = spark.read.jdbc(
        _url(),
        target,
        column="k",
        lowerBound=0,
        upperBound=1000,
        numPartitions=4,
        properties={**PLAIN, "pushDownPredicate": "false"},
    )
    for condition in (
        "k > 100 AND k < 700",
        "k IS NULL OR small = 7",
        "n > 1000 AND lower(t) LIKE 'n1%'",
        "d >= DATE '2003-01-01' AND id % 3 = 0",
        "k > 5000000",
    ):
        expected = _rows(plain.filter(condition).select("t", "id", "n"))
        assert _rows(parts.filter(condition).select("t", "id", "n")) == expected, condition
        assert _rows(unpushed.filter(condition).select("t", "id", "n")) == expected, condition
    assert parts.count() == ROWS
    limited = parts.select("id").limit(7)
    plan = limited._explain_text()
    assert "pushed_limit=7" in plan
    assert "strides=4" in plan
    seven = [row[0] for row in limited.collect()]
    assert len(seven) == 7
    assert len(set(seven)) == 7
    assert all(1 <= value <= ROWS for value in seven)
    assert parts.filter("k < 300").limit(9).count() == 9
    assert parts.filter("lower(t) LIKE 'n4%'").limit(5).count() == 5
    assert parts.limit(0).count() == 0
    assert parts.limit(ROWS * 10).count() == ROWS


def test_num_partitions_above_the_pool_runs_on_the_pool(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    _, target = _wide(conn, names)
    application = f"c3_{names['schema']}"
    props = {**PLAIN, "pool_max_size": "2", "ApplicationName": application}
    frame = spark.read.jdbc(
        _url(),
        target,
        column="id",
        lowerBound=1,
        upperBound=ROWS,
        numPartitions=16,
        properties=props,
    )
    assert "strides=16, max_connections=2" in frame._explain_text()
    for _ in range(3):
        assert sorted(row[0] for row in frame.select("id").collect()) == list(range(1, ROWS + 1))
        opened = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE state <> 'idle') FROM pg_stat_activity "
            "WHERE application_name = %s",
            (application,),
        ).fetchone()
        assert opened[0] <= 2
        assert opened[1] == 0


def _move_rows(psycopg: Any, table: str, stop: threading.Event, moved: list[int]) -> None:
    """Move two thirds of the rows across stride boundaries until told to stop."""
    with psycopg.connect(_url(), autocommit=True) as writer:
        while not stop.is_set():
            writer.execute(f"UPDATE {table} SET k = (COALESCE(k, 0) + 500) % 1000 WHERE id % 3 = 0")
            writer.execute(f"UPDATE {table} SET k = (COALESCE(k, 0) + 250) % 1000 WHERE id % 3 = 1")
            moved.append(1)


def test_a_writer_moving_rows_across_strides_never_duplicates_or_drops(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    table, target = _wide(conn, names, rows=20_000)
    frame = _jdbc(spark, target, "k", (0, 1000, 4)).select("id")
    stop = threading.Event()
    moved: list[int] = []
    psycopg = pytest.importorskip("psycopg")
    writer = threading.Thread(target=_move_rows, args=(psycopg, table, stop, moved))
    writer.start()
    try:
        for _ in range(12):
            ids = [row[0] for row in frame.collect()]
            assert len(ids) == 20_000
            assert len(set(ids)) == 20_000
    finally:
        stop.set()
        writer.join(timeout=60)
    assert moved, "the writer committed while the reads ran"
