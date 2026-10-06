from __future__ import annotations

import secrets
from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import PySparkException

HOST = "db.example.com"


@pytest.fixture
def password() -> str:
    return f"Pw{secrets.token_hex(12)}"


@pytest.fixture
def spark(tmp_path: Path, password: str) -> Iterator[ReparkSession]:
    lines = [
        "[default.conf]",
        f'"spark.repark.test.jdbc_url" = "jdbc:postgresql://alice:{password}@{HOST}/sales"',
        "[default.catalog.mem]",
        'type = "memory"',
        f'warehouse = "{(tmp_path / "wh").as_posix()}"',
        f'uri = "jdbc:postgresql://alice:{password}@{HOST}:5432/sales"',
        "[default.database.postgres.acme]",
        f'url = "postgresql://alice:{password}@{HOST}:5432/sales"',
        f'query_url = "postgresql://{HOST}:5432/sales?user=alice&password={password}"',
        f'dsn = "host={HOST} dbname=sales user=alice password={password}"',
        f'odbc = "Server={HOST};Database=sales;Uid=alice;Pwd={password};"',
        'user = "alice"',
    ]
    path = tmp_path / "repark.toml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    session = ReparkSession.builder.configFile(str(path)).getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def test_sources_rows_never_carry_the_password(spark: ReparkSession, password: str) -> None:
    rows = spark.sources()
    rendered = f"{rows!r} {rows!s}"
    assert password not in rendered
    assert HOST in rendered
    properties = rows[0].properties
    assert properties["url"] == f"postgresql://alice:***@{HOST}:5432/sales"
    assert properties["query_url"].endswith("password=***")
    assert properties["dsn"].endswith("password=***")
    assert properties["odbc"].endswith("Pwd=***;")
    assert properties["user"] == "alice"


def test_config_dump_never_carries_the_password(spark: ReparkSession, password: str) -> None:
    dump = spark.conf.getAll
    rendered = repr(dump)
    assert password not in rendered
    assert dump["spark.repark.test.jdbc_url"] == f"jdbc:postgresql://alice:***@{HOST}/sales"
    catalog_uri = [value for key, value in dump.items() if key.endswith("catalog.mem.uri")]
    assert catalog_uri == [f"jdbc:postgresql://alice:***@{HOST}:5432/sales"]
    assert HOST in rendered


def test_set_listings_never_carry_the_password(spark: ReparkSession, password: str) -> None:
    spark.conf.set("spark.repark.test.runtime_url", f"mysql://bob:{password}@{HOST}/sales")
    for statement in ("SET", "SET -v", "SET spark.repark.test.runtime_url"):
        rendered = repr(spark.sql(statement).collect())
        assert password not in rendered, statement
        assert HOST in rendered, statement


def test_ping_refusal_never_carries_the_password(spark: ReparkSession, password: str) -> None:
    with pytest.raises(PySparkException) as excinfo:
        spark.source("acme").ping()
    assert password not in str(excinfo.value)
    assert "acme" in str(excinfo.value)
