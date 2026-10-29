from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession

EXPECT_TABLE_NOT_VIEW = "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]"
TABLE_OPERATION = "[UNSUPPORTED_FEATURE.TABLE_OPERATION]"
COPY_ON_WRITE = "TBLPROPERTIES ('format-version'='2')"
MERGE_ON_READ = (
    "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', "
    "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
)

ROUTES = [
    (
        "DELETE FROM {} WHERE id = 2",
        EXPECT_TABLE_NOT_VIEW,
        "'DELETE' expects a table but `t` is a view",
        [1, 3],
    ),
    (
        "UPDATE {} SET id = 10 WHERE id = 1",
        TABLE_OPERATION,
        "does not support UPDATE TABLE",
        [2, 3, 10],
    ),
    (
        "MERGE INTO {} AS x USING (SELECT 2 AS id) AS s ON x.id = s.id WHEN MATCHED THEN DELETE",
        TABLE_OPERATION,
        "does not support MERGE INTO TABLE",
        [1, 3],
    ),
    (
        "INSERT INTO {} VALUES (40)",
        EXPECT_TABLE_NOT_VIEW,
        "'INSERT' expects a table but `t` is a view",
        [1, 2, 3, 40],
    ),
    (
        "INSERT INTO {} BY NAME SELECT 42 AS id",
        EXPECT_TABLE_NOT_VIEW,
        "'INSERT' expects a table but `t` is a view",
        [1, 2, 3, 42],
    ),
    (
        "INSERT OVERWRITE {} VALUES (50)",
        EXPECT_TABLE_NOT_VIEW,
        "'INSERT' expects a table but `t` is a view",
        [50],
    ),
    (
        "TRUNCATE TABLE {}",
        EXPECT_TABLE_NOT_VIEW,
        "'TRUNCATE TABLE' expects a table but `t` is a view",
        [],
    ),
]
ROW_LEVEL = ROUTES[:3]
ROUTE_IDS = [
    "delete",
    "update",
    "merge",
    "insert",
    "insert-by-name",
    "insert-overwrite",
    "truncate",
]


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    spark = (
        ReparkSession.builder.appName("temp-view-write-target")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(tmp_path))
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


def _seed(spark: ReparkSession, properties: str, view: str | None) -> None:
    statements = [
        "CREATE NAMESPACE hd.ns",
        f"CREATE TABLE hd.ns.t (id INT) USING iceberg {properties}",
        "INSERT INTO hd.ns.t VALUES (1),(2),(3)",
        "USE hd.ns",
    ]
    if view is not None:
        statements.append(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id")
    for statement in statements:
        spark.sql(statement).collect()


def _ids(spark: ReparkSession, query: str) -> list[int]:
    return sorted(row["id"] for row in spark.sql(query).to_arrow().to_pylist())


def test_minimal_reproduction_truncate_through_a_view_name_refuses(
    session: ReparkSession,
) -> None:
    _seed(session, COPY_ON_WRITE, "t")
    with pytest.raises(Exception, match=r"EXPECT_TABLE_NOT_VIEW\.NO_ALTERNATIVE"):
        session.sql("TRUNCATE TABLE t").collect()
    assert _ids(session, "SELECT id FROM t") == [99]
    assert _ids(session, "SELECT id FROM hd.ns.t") == [1, 2, 3]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROUTES, ids=ROUTE_IDS)
def test_every_write_through_a_view_name_refuses_and_holds_the_table(
    session: ReparkSession, template: str, token: str, shape: str, written: list[int]
) -> None:
    _seed(session, COPY_ON_WRITE, "t")
    with pytest.raises(Exception, match=re.escape(token)) as raised:
        session.sql(template.format("t")).collect()
    assert shape in str(raised.value)
    assert _ids(session, "SELECT id FROM hd.ns.t") == [1, 2, 3]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROW_LEVEL, ids=ROUTE_IDS[:3])
def test_row_level_writes_through_a_view_name_refuse_on_merge_on_read(
    session: ReparkSession, template: str, token: str, shape: str, written: list[int]
) -> None:
    _seed(session, MERGE_ON_READ, "t")
    with pytest.raises(Exception, match=re.escape(token)) as raised:
        session.sql(template.format("t")).collect()
    assert shape in str(raised.value)
    assert _ids(session, "SELECT id FROM hd.ns.t") == [1, 2, 3]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROUTES, ids=ROUTE_IDS)
@pytest.mark.parametrize("view", [None, "w"], ids=["no-view", "other-view"])
def test_a_bare_table_name_writes_when_no_view_holds_it(
    session: ReparkSession,
    view: str | None,
    template: str,
    token: str,
    shape: str,
    written: list[int],
) -> None:
    _seed(session, COPY_ON_WRITE, view)
    session.sql(template.format("t")).collect()
    assert _ids(session, "SELECT id FROM hd.ns.t") == written
    if view is not None:
        assert _ids(session, f"SELECT id FROM {view}") == [99]
