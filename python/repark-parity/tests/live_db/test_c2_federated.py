"""C-2d live cells: one statement joining an Iceberg table and a Postgres table (sketch §5.4)."""

from __future__ import annotations

import datetime as dt
import os
import re
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions
from repark.spark.session import _reset_active_session_for_tests

UTC = dt.UTC
CUSTOMERS = [
    (10, "ann", dt.datetime(2024, 1, 1, 10, tzinfo=UTC)),
    (11, "bob", dt.datetime(2024, 2, 1, 10, tzinfo=UTC)),
    (12, "cy", dt.datetime(2024, 3, 1, 9, tzinfo=UTC)),
]
ORDERS = (
    "(1, 10, 5.50, '2024-01-01 10:00+00', 'B'), "
    "(2, 11, 150.00, '2024-02-01 10:00+00', 'B'), "
    "(3, 11, 250.00, '2024-02-01 11:00+00', 'b'), "
    "(4, 12, 300.00, '2024-03-01 09:00+00', 'x'), "
    "(5, 12, 400.00, '2024-03-01 04:00-05', 'B'), "
    "(6, 13, 500.00, '2024-04-01 00:00+00', 'b')"
)
JOIN_SQL = (
    "SELECT o.id, c.name, o.amount FROM pg.{schema}.orders o "
    "JOIN ice.db.customers c ON o.cust = c.id AND o.placed = c.since "
    "WHERE o.amount > CAST(100 AS DECIMAL(10,2)) AND lower(o.note) = 'b'"
)
EXPECTED_PLAN = """\
HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@N, cust@N), (since@N, placed@N)], \
projection=[id@N, name@N, amount@N]
  CooperativeExec
    IcebergTableScan projection:[id,name,since] predicate:[] N=1
  FilterExec: lower(note@N) = b, projection=[id@N, cust@N, amount@N, placed@N]
    RepartitionExec: partitioning=RoundRobinBatch(N), input_partitions=1
      PostgresScanExec: source=pg, relation="<schema>"."orders", \
projection=[id, cust, amount, placed, note], \
pushed_filters=[amount > Decimal128(Some(10000),10,2)], \
residual_filters=[lower(note) = Utf8("b")], pushed_limit=None"""


@pytest.fixture
def federated(
    tmp_path: Path, pg_live: tuple[Any, dict[str, str]]
) -> Iterator[tuple[ReparkSession, Any, str]]:
    """A New York session mounting `pg` beside a private memory catalog `ice`, both seeded."""
    conn, names = pg_live
    schema = names["schema"]
    conn.execute(
        f'CREATE TABLE "{schema}".orders '
        "(id int8 PRIMARY KEY, cust int4, amount numeric(10,2), placed timestamptz, note text)"
    )
    conn.execute(f'INSERT INTO "{schema}".orders VALUES {ORDERS}')
    path = tmp_path / "repark.toml"
    path.write_text(
        "[default.catalog.ice]\n"
        'type = "memory"\n'
        f'warehouse = "{(tmp_path / "wh").as_posix()}"\n'
        "[default.database.postgres.pg]\n"
        f'url = "{os.environ["REPARK_PG_URL"]}"\n'
        'sslmode = "disable"\n',
        encoding="utf-8",
    )
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.configFile(str(path))
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
        session.sql("CREATE TABLE ice.db.customers (id INT, name STRING, since TIMESTAMP)")
        values = ", ".join(
            f"({key}, '{name}', TIMESTAMP '{since:%Y-%m-%d %H:%M:%S} UTC')"
            for key, name, since in CUSTOMERS
        )
        session.sql(f"INSERT INTO ice.db.customers VALUES {values}").collect()
        yield session, conn, schema
    finally:
        session.stop()
        _reset_active_session_for_tests()


def _expected(conn: Any, schema: str) -> list[tuple[int, str, Decimal]]:
    """Join psycopg's filtered orders with the seeded customers in pyarrow, independently."""
    rows = conn.execute(
        f'SELECT id, cust, amount, placed FROM "{schema}".orders '
        "WHERE amount > 100 AND lower(note) = 'b'"
    ).fetchall()
    orders = pa.table(
        {
            "id": pa.array([row[0] for row in rows], pa.int64()),
            "cust": pa.array([row[1] for row in rows], pa.int32()),
            "amount": pa.array([row[2] for row in rows], pa.decimal128(10, 2)),
            "placed": pa.array([row[3] for row in rows], pa.timestamp("us", tz="UTC")),
        }
    )
    customers = pa.table(
        {
            "cid": pa.array([row[0] for row in CUSTOMERS], pa.int32()),
            "name": pa.array([row[1] for row in CUSTOMERS], pa.string()),
            "since": pa.array([row[2] for row in CUSTOMERS], pa.timestamp("us", tz="UTC")),
        }
    )
    joined = orders.join(
        customers, keys=["cust", "placed"], right_keys=["cid", "since"], join_type="inner"
    )
    result = joined.select(["id", "name", "amount"]).sort_by("id").to_pylist()
    return [(row["id"], row["name"], row["amount"]) for row in result]


def _normalised(plan: str, schema: str) -> str:
    """Strip attribute ids, the partition fan-out and the cell tag from a physical plan."""
    plan = re.sub(r"@\d+", "@N", plan)
    plan = re.sub(r"RoundRobinBatch\(\d+\)", "RoundRobinBatch(N)", plan)
    return plan.replace(schema, "<schema>").strip()


def test_federated_iceberg_postgres_join(federated: tuple[ReparkSession, Any, str]) -> None:
    spark, conn, schema = federated
    expected = _expected(conn, schema)
    assert [row[0] for row in expected] == [2, 5]
    by_sql = spark.sql(JOIN_SQL.format(schema=schema))
    orders = (
        spark.table(f"pg.{schema}.orders")
        .filter(functions.col("amount") > functions.lit(Decimal("100.00")))
        .filter(functions.lower(functions.col("note")) == "b")
        .alias("o")
    )
    customers = spark.table("ice.db.customers").alias("c")
    condition = (functions.col("o.cust") == functions.col("c.id")) & (
        functions.col("o.placed") == functions.col("c.since")
    )
    by_frame = orders.join(customers, condition).select("o.id", "c.name", "o.amount")
    for frame in (by_sql, by_frame):
        rows = sorted(tuple(row) for row in frame.collect())
        assert rows == expected
        plan = frame._explain_text()
        assert plan.count("PostgresScanExec") == 1
        assert plan.count("IcebergTableScan") == 1
        assert "pushed_filters=[amount > Decimal128(Some(10000),10,2)]" in plan
        assert 'residual_filters=[lower(note) = Utf8("b")]' in plan
        join_line = next(line for line in plan.splitlines() if "HashJoinExec" in line)
        assert "cust@" in join_line and "placed@" in join_line
        assert plan.index("HashJoinExec") < plan.index("PostgresScanExec")
        assert plan.index("HashJoinExec") < plan.index("IcebergTableScan")
        scan_line = next(line for line in plan.splitlines() if "PostgresScanExec" in line)
        assert "cust" not in scan_line.split("pushed_filters=")[1]


def test_federated_join_explain_boundary(federated: tuple[ReparkSession, Any, str]) -> None:
    spark, _, schema = federated
    rows = spark.sql("EXPLAIN " + JOIN_SQL.format(schema=schema)).collect()
    physical = next(row["plan"] for row in rows if row["plan_type"] == "physical_plan")
    assert _normalised(physical, schema) == EXPECTED_PLAN


def test_federated_join_sees_one_clock_across_zones(
    federated: tuple[ReparkSession, Any, str],
) -> None:
    spark, _, schema = federated
    rows = spark.sql(
        f"SELECT o.id, CAST(o.placed AS STRING) AS pg, CAST(c.since AS STRING) AS ice "
        f"FROM pg.{schema}.orders o JOIN ice.db.customers c "
        "ON o.placed = c.since AND o.cust = c.id ORDER BY o.id"
    ).collect()
    assert [tuple(row) for row in rows] == [
        (1, "2024-01-01 05:00:00", "2024-01-01 05:00:00"),
        (2, "2024-02-01 05:00:00", "2024-02-01 05:00:00"),
        (4, "2024-03-01 04:00:00", "2024-03-01 04:00:00"),
        (5, "2024-03-01 04:00:00", "2024-03-01 04:00:00"),
    ]
