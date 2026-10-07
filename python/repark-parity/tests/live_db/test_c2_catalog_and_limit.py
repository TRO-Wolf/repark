"""C-2d fold 1 live cells: catalog APIs on a mounted source, limits over refused values, plan names."""

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

READ_ONLY = "database source `default.database.postgres.pg` is read-only"
NAN_ROW = 150_000
BIG_ROWS = 200_000


def _url() -> str:
    """Return the C-0 container URL the fixture already required."""
    return os.environ["REPARK_PG_URL"]


@pytest.fixture
def spark(tmp_path: Path, pg_live: tuple[Any, dict[str, str]]) -> Iterator[ReparkSession]:
    """A New York session mounting the container as `pg`, with Spark's display style."""
    assert pg_live
    facade_session._reset_active_session_for_tests()
    path = tmp_path / "repark.toml"
    path.write_text(
        f'[default.database.postgres.pg]\nurl = "{_url()}"\nsslmode = "disable"\n'
        f'[default.database.postgres.unpushed]\nurl = "{_url()}"\nsslmode = "disable"\n'
        'pushdown_limit = "false"\n',
        encoding="utf-8",
    )
    session = (
        repark.ReparkSession.builder.configFile(str(path))
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("repark.display.style", "spark")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()
        facade_session._reset_active_session_for_tests()


def _reader(spark: ReparkSession, table: str, *, push_limit: bool) -> Any:
    """A `format("postgres")` frame over `table`, with limit pushdown on or off."""
    return (
        spark.read.format("postgres")
        .option("url", _url())
        .option("sslmode", "disable")
        .option("dbtable", table)
        .option("pushDownLimit", "true" if push_limit else "false")
        .load()
    )


def _shown_rows(output: str) -> int:
    """Count the data rows of a Spark-style `show()` table."""
    lines = [line for line in output.splitlines() if line.startswith("|")]
    return len(lines) - 1


def test_catalog_apis_answer_for_a_mounted_source(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".t (id int4)')
    assert spark.catalog.tableExists(f"pg.{schema}.t") is True
    assert spark.catalog.tableExists(f"pg.{schema}.absent") is False
    assert spark.sql(f"SHOW TABLES IN pg.{schema}").collect() == []
    assert spark.sql("SHOW SCHEMAS IN pg").collect() == []
    writes = (
        lambda: spark.range(2).writeTo(f"pg.{schema}.wt").create(),
        lambda: spark.range(2).write.saveAsTable(f"pg.{schema}.sat"),
    )
    for write in writes:
        with pytest.raises(errors.UnsupportedOperationException) as excinfo:
            write()
        message = str(excinfo.value)
        assert READ_ONLY in message
        assert "CONNECT-DECL-pg-ddl" in message
        assert "unknown catalog" not in message
    created = conn.execute(
        "SELECT count(*) FROM pg_tables WHERE schemaname = %s", (schema,)
    ).fetchone()
    assert created == (1,)


def test_a_limit_never_reaches_a_refused_value_past_it(
    spark: ReparkSession,
    pg_live: tuple[Any, dict[str, str]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".big (id int4, n numeric(10,2), s text)')
    conn.execute(
        f'INSERT INTO "{schema}".big SELECT g, '
        f"CASE WHEN g = {NAN_ROW} THEN 'NaN'::numeric ELSE g END, g::text "
        f"FROM generate_series(1, {BIG_ROWS}) g"
    )
    limited = spark.sql(f"SELECT * FROM pg.{schema}.big LIMIT 5")
    assert len(limited.collect()) == 5
    assert "pushed_limit=5" in limited._explain_text()
    for push_limit in (True, False):
        frame = _reader(spark, f"{schema}.big", push_limit=push_limit)
        frame.show(3)
        assert _shown_rows(capsys.readouterr().out) == 3
        assert len(frame.limit(5).collect()) == 5
    with pytest.raises(errors.PySparkException) as excinfo:
        spark.sql(f"SELECT count(n) FROM pg.{schema}.big").collect()
    assert "CONNECT-DECL-pg-numeric-special" in str(excinfo.value)


def test_a_refused_value_fails_only_a_read_that_reaches_its_row(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".odd (id int4, n numeric, ts timestamp)')
    conn.execute(
        f'INSERT INTO "{schema}".odd VALUES '
        "(1, 1, '2024-07-01 12:00'), (2, 2, '2024-07-01 12:00'), (3, 3, '2024-07-01 12:00'), "
        "(4, 'NaN', '2024-07-01 12:00'), (5, 5, '2024-03-10 02:30')"
    )
    numbers = _reader(spark, f"{schema}.odd", push_limit=False).select("id", "n")
    assert [row[0] for row in numbers.limit(3).collect()] == [1, 2, 3]
    clocks = spark.table(f"unpushed.{schema}.odd").select("id", "ts")
    assert [row[0] for row in clocks.limit(4).collect()] == [1, 2, 3, 4]
    with pytest.raises(errors.PySparkException) as excinfo:
        numbers.collect()
    assert "CONNECT-DECL-pg-numeric-special" in str(excinfo.value)
    with pytest.raises(errors.PySparkException) as excinfo:
        clocks.collect()
    assert "CONNECT-DIV-pg-timestamp-zone" in str(excinfo.value)


def test_a_read_postgres_frame_names_its_relation_in_the_plan(
    spark: ReparkSession, pg_live: tuple[Any, dict[str, str]]
) -> None:
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(f'CREATE TABLE "{schema}".vals (id int4, note text)')
    by_jdbc = spark.read.jdbc(
        "jdbc:" + _url(), f"{schema}.vals", properties={"sslmode": "disable"}
    ).filter("id > 1")
    by_query = (
        spark.read.format("postgres")
        .option("url", _url())
        .option("sslmode", "disable")
        .option("query", f'SELECT id FROM "{schema}".vals')
        .load()
    )
    relation = by_jdbc._explain_text("extended")
    query = by_query._explain_text("extended")
    for text in (relation, query):
        assert "?table?" not in text
    assert f"TableScan: {schema}.vals" in relation
    assert f"{schema}.vals.id > Int32(1)" in relation
    assert "TableScan: jdbc" in query
