from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.functions import lit

EXPECT_TABLE_NOT_VIEW = "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]"
TABLE_OPERATION = "[UNSUPPORTED_FEATURE.TABLE_OPERATION]"
VIEW_SPELLINGS = ["`T`", "T", "t", "`t`"]
NAME_SPELLINGS = ["t", "T", "`T`", "`t`"]
VIEW_IDS = ["view-quoted-upper", "view-upper", "view-lower", "view-quoted-lower"]
NAME_IDS = ["name-lower", "name-upper", "name-quoted-upper", "name-quoted-lower"]

ROUTES = [
    ("DELETE FROM {} WHERE id = 2", EXPECT_TABLE_NOT_VIEW, "'DELETE' expects a table", [1, 3]),
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
        "'INSERT' expects a table",
        [1, 2, 3, 40],
    ),
    ("INSERT OVERWRITE {} VALUES (50)", EXPECT_TABLE_NOT_VIEW, "'INSERT' expects a table", [50]),
    ("TRUNCATE TABLE {}", EXPECT_TABLE_NOT_VIEW, "'TRUNCATE TABLE' expects a table", []),
]
ROUTE_IDS = ["delete", "update", "merge", "insert", "insert-overwrite", "truncate"]
MERGE_WITH_SCHEMA_EVOLUTION = (
    "MERGE WITH SCHEMA EVOLUTION INTO {} AS x USING (SELECT 2 AS id) AS s "
    "ON x.id = s.id WHEN MATCHED THEN DELETE"
)

Writer = Callable[..., None]

REFUSED_WRITERS: list[tuple[str, Writer]] = [
    ("append", lambda frame, name: frame.writeTo(name).append()),
    ("overwrite-partitions", lambda frame, name: frame.writeTo(name).overwritePartitions()),
    ("overwrite", lambda frame, name: frame.writeTo(name).overwrite(lit(True))),
    (
        "option-append",
        lambda frame, name: frame.writeTo(name).option("snapshot-property.vt", "x").append(),
    ),
]
V2_WRITTEN = {
    "append": [1, 2, 3, 60],
    "overwrite-partitions": [60],
    "overwrite": [60],
    "option-append": [1, 2, 3, 60],
}
TABLE_WRITERS: list[tuple[str, Writer, list[int]]] = [
    ("insert-into", lambda frame, name: frame.write.insertInto(name), [1, 2, 3, 60]),
    (
        "insert-into-overwrite",
        lambda frame, name: frame.write.insertInto(name, overwrite=True),
        [60],
    ),
    (
        "mode-overwrite-insert-into",
        lambda frame, name: frame.write.mode("overwrite").insertInto(name),
        [60],
    ),
    (
        "mode-append-save-as-table",
        lambda frame, name: frame.write.mode("append").saveAsTable(name),
        [1, 2, 3, 60],
    ),
    (
        "mode-overwrite-save-as-table",
        lambda frame, name: frame.write.mode("overwrite").saveAsTable(name),
        [60],
    ),
    ("create-or-replace", lambda frame, name: frame.writeTo(name).createOrReplace(), [60]),
]


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    spark = (
        ReparkSession.builder.appName("temp-view-name-case")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(tmp_path))
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


def _seed(spark: ReparkSession, view: str | None, *, case_sensitive: bool = False) -> None:
    spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
    statements = [
        "CREATE NAMESPACE hd.ns",
        "CREATE TABLE hd.ns.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='2')",
        "INSERT INTO hd.ns.t VALUES (1),(2),(3)",
        "USE hd.ns",
    ]
    if view is not None:
        statements.append(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id")
    for statement in statements:
        spark.sql(statement).collect()


def _ids(spark: ReparkSession, query: str) -> list[int]:
    return sorted(row["id"] for row in spark.sql(query).to_arrow().to_pylist())


def _table(spark: ReparkSession) -> list[int]:
    return _ids(spark, "SELECT id FROM hd.ns.t")


@pytest.mark.parametrize("name", NAME_SPELLINGS, ids=NAME_IDS)
@pytest.mark.parametrize("view", VIEW_SPELLINGS, ids=VIEW_IDS)
def test_every_spelling_reads_the_view_under_the_default_rule(
    session: ReparkSession, view: str, name: str
) -> None:
    _seed(session, view)
    assert _ids(session, f"SELECT id FROM {name}") == [99]
    assert _table(session) == [1, 2, 3]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROUTES, ids=ROUTE_IDS)
@pytest.mark.parametrize("name", NAME_SPELLINGS, ids=NAME_IDS)
@pytest.mark.parametrize("view", VIEW_SPELLINGS, ids=VIEW_IDS)
def test_every_spelling_of_a_view_name_refuses_every_write_under_the_default_rule(
    session: ReparkSession,
    view: str,
    name: str,
    template: str,
    token: str,
    shape: str,
    written: list[int],
) -> None:
    _seed(session, view)
    with pytest.raises(Exception, match=re.escape(token)) as raised:
        session.sql(template.format(name)).collect()
    assert shape in str(raised.value)
    assert _table(session) == [1, 2, 3]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize("name", NAME_SPELLINGS, ids=NAME_IDS)
@pytest.mark.parametrize("view", VIEW_SPELLINGS, ids=VIEW_IDS)
def test_merge_with_schema_evolution_through_a_view_name_refuses_as_a_merge(
    session: ReparkSession, view: str, name: str
) -> None:
    _seed(session, view)
    with pytest.raises(Exception, match=re.escape(TABLE_OPERATION)) as raised:
        session.sql(MERGE_WITH_SCHEMA_EVOLUTION.format(name)).collect()
    assert "does not support MERGE INTO TABLE" in str(raised.value)
    assert _table(session) == [1, 2, 3]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize("view", ["T", "`T`"], ids=["view-upper", "view-quoted-upper"])
def test_under_case_sensitive_names_an_upper_case_view_reads_only_by_its_exact_name(
    session: ReparkSession, view: str
) -> None:
    _seed(session, view, case_sensitive=True)
    for name in ["T", "`T`"]:
        assert _ids(session, f"SELECT id FROM {name}") == [99]
    for name in ["t", "`t`"]:
        assert _ids(session, f"SELECT id FROM {name}") == [1, 2, 3]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROUTES, ids=ROUTE_IDS)
@pytest.mark.parametrize("name", ["t", "`t`"], ids=["name-lower", "name-quoted-lower"])
@pytest.mark.parametrize("view", ["T", "`T`"], ids=["view-upper", "view-quoted-upper"])
def test_under_case_sensitive_names_a_lower_case_write_reaches_the_table(
    session: ReparkSession,
    view: str,
    name: str,
    template: str,
    token: str,
    shape: str,
    written: list[int],
) -> None:
    _seed(session, view, case_sensitive=True)
    session.sql(template.format(name)).collect()
    assert _table(session) == written
    assert _ids(session, "SELECT id FROM T") == [99]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROUTES, ids=ROUTE_IDS)
@pytest.mark.parametrize("name", ["T", "`T`"], ids=["name-upper", "name-quoted-upper"])
@pytest.mark.parametrize("view", ["T", "`T`"], ids=["view-upper", "view-quoted-upper"])
def test_under_case_sensitive_names_the_exact_view_name_refuses_every_write(
    session: ReparkSession,
    view: str,
    name: str,
    template: str,
    token: str,
    shape: str,
    written: list[int],
) -> None:
    _seed(session, view, case_sensitive=True)
    with pytest.raises(Exception, match=re.escape(token)) as raised:
        session.sql(template.format(name)).collect()
    assert shape in str(raised.value)
    assert _table(session) == [1, 2, 3]
    assert _ids(session, "SELECT id FROM T") == [99]


@pytest.mark.parametrize(("label", "writer"), REFUSED_WRITERS, ids=[w[0] for w in REFUSED_WRITERS])
@pytest.mark.parametrize("name", ["t", "T", "`t`"], ids=["name-lower", "name-upper", "name-quoted"])
def test_write_to_a_view_name_refuses_before_any_write(
    session: ReparkSession, name: str, label: str, writer: Writer
) -> None:
    _seed(session, "t")
    frame = session.createDataFrame([(60,)], "id INT")
    with pytest.raises(AnalysisException) as raised:
        writer(frame, name)
    assert raised.value.getCondition() == "_LEGACY_ERROR_TEMP_1011"
    assert "Writing into a view is not allowed. View: `t`." in str(raised.value)
    assert _table(session) == [1, 2, 3]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(("label", "writer"), REFUSED_WRITERS, ids=[w[0] for w in REFUSED_WRITERS])
@pytest.mark.parametrize(
    ("view", "name"),
    [(None, "t"), ("t", "ns.t"), ("t", "hd.ns.t"), ("w", "t")],
    ids=["no-view", "two-part", "three-part", "other-view"],
)
def test_write_to_a_table_name_writes_the_table(
    session: ReparkSession, view: str | None, name: str, label: str, writer: Writer
) -> None:
    _seed(session, view)
    frame = session.createDataFrame([(60,)], "id INT")
    writer(frame, name)
    assert _table(session) == V2_WRITTEN[label]
    if view is not None:
        assert _ids(session, f"SELECT id FROM {view}") == [99]


@pytest.mark.parametrize(
    ("label", "writer", "written"), TABLE_WRITERS, ids=[w[0] for w in TABLE_WRITERS]
)
def test_insert_into_save_as_table_and_create_or_replace_write_the_table_past_a_view_name(
    session: ReparkSession, label: str, writer: Writer, written: list[int]
) -> None:
    _seed(session, "t")
    frame = session.createDataFrame([(60,)], "id INT")
    writer(frame, "t")
    assert _table(session) == written
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(("template", "token", "shape", "written"), ROUTES, ids=ROUTE_IDS)
def test_a_two_part_table_name_writes_the_table_with_the_view_in_scope(
    session: ReparkSession, template: str, token: str, shape: str, written: list[int]
) -> None:
    _seed(session, "t")
    session.sql(template.format("ns.t")).collect()
    assert _table(session) == written
    assert _ids(session, "SELECT id FROM t") == [99]
