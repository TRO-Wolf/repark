from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

EXPECT_TABLE_NOT_VIEW = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE"
TABLE_OPERATION = "UNSUPPORTED_FEATURE.TABLE_OPERATION"

ROUTES = [
    ("DELETE FROM {} WHERE id = 2", EXPECT_TABLE_NOT_VIEW, [1, 3]),
    ("UPDATE {} SET id = 10 WHERE id = 1", TABLE_OPERATION, [2, 3, 10]),
    (
        "MERGE INTO {} AS x USING (SELECT 2 AS id) AS s ON x.id = s.id WHEN MATCHED THEN DELETE",
        TABLE_OPERATION,
        [1, 3],
    ),
    ("INSERT INTO {} VALUES (40)", EXPECT_TABLE_NOT_VIEW, [1, 2, 3, 40]),
    ("INSERT OVERWRITE {} VALUES (50)", EXPECT_TABLE_NOT_VIEW, [50]),
    ("TRUNCATE TABLE {}", EXPECT_TABLE_NOT_VIEW, []),
]
ROUTE_IDS = ["delete", "update", "merge", "insert", "insert-overwrite", "truncate"]
SWITCHES = [(False, True), (True, False)]
SWITCH_IDS = ["false-then-true", "true-then-false"]
ASCII_VIEWS = ["`T`", "T", "t"]
ASCII_NAMES = ["t", "T", "`T`", "`t`"]
LETTER_TABLES = ["ä", "Ä"]
LETTER_VIEWS = ["`Ä`", "`ä`"]
LETTER_NAMES = ["`Ä`", "`ä`"]


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    spark = (
        ReparkSession.builder.appName("temp-view-name-fold")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(tmp_path))
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


def _segment(name: str) -> str:
    return name[1:-1] if name.startswith("`") else name


def _key(name: str, case_sensitive: bool) -> str:
    segment = _segment(name)
    return segment if case_sensitive else segment.lower()


def _holds(view: str, created: bool, name: str, looked_up: bool) -> bool:
    return _key(view, created) == _key(name, looked_up)


def _set_case(spark: ReparkSession, case_sensitive: bool) -> None:
    spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())


def _seed(spark: ReparkSession, table: str, view: str, created: bool, looked_up: bool) -> None:
    _set_case(spark, created)
    for statement in [
        "CREATE NAMESPACE hd.ns",
        f"CREATE TABLE hd.ns.`{table}` (id INT) USING iceberg TBLPROPERTIES ('format-version'='2')",
        f"INSERT INTO hd.ns.`{table}` VALUES (1),(2),(3)",
        "USE hd.ns",
        f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id",
    ]:
        spark.sql(statement).collect()
    _set_case(spark, looked_up)


def _ids(spark: ReparkSession, query: str) -> list[int]:
    return sorted(row["id"] for row in spark.sql(query).to_arrow().to_pylist())


def _table(spark: ReparkSession, table: str) -> list[int]:
    return _ids(spark, f"SELECT id FROM hd.ns.`{table}`")


def _assert_read(spark: ReparkSession, query: str, expected: list[int] | None) -> None:
    if expected is None:
        with pytest.raises(Exception):  # noqa: B017
            spark.sql(query).collect()
    else:
        assert _ids(spark, query) == expected


def _assert_write(
    spark: ReparkSession,
    sql: str,
    table: str,
    *,
    holds: bool,
    reaches_table: bool,
    token: str,
    written: list[int],
) -> None:
    if holds:
        with pytest.raises(Exception) as raised:
            spark.sql(sql).collect()
        assert token in str(raised.value)
        assert _table(spark, table) == [1, 2, 3]
    elif reaches_table:
        spark.sql(sql).collect()
        assert _table(spark, table) == written
    else:
        with pytest.raises(Exception):  # noqa: B017
            spark.sql(sql).collect()
        assert _table(spark, table) == [1, 2, 3]


def _assert_write_to(
    spark: ReparkSession, name: str, table: str, *, holds: bool, reaches_table: bool
) -> None:
    frame = spark.createDataFrame([(60,)], "id INT")
    if holds:
        with pytest.raises(AnalysisException) as raised:
            frame.writeTo(name).append()
        assert raised.value.getCondition() == "_LEGACY_ERROR_TEMP_1011"
        assert _table(spark, table) == [1, 2, 3]
    elif reaches_table:
        frame.writeTo(name).append()
        assert _table(spark, table) == [1, 2, 3, 60]
    else:
        with pytest.raises(Exception):  # noqa: B017
            frame.writeTo(name).append()
        assert _table(spark, table) == [1, 2, 3]


@pytest.mark.parametrize("name", ASCII_NAMES)
@pytest.mark.parametrize("view", ASCII_VIEWS)
@pytest.mark.parametrize(("created", "looked_up"), SWITCHES, ids=SWITCH_IDS)
def test_a_switched_case_rule_reads_the_name_stored_at_creation(
    session: ReparkSession, created: bool, looked_up: bool, view: str, name: str
) -> None:
    _seed(session, "t", view, created, looked_up)
    holds = _holds(view, created, name, looked_up)
    reaches_table = _segment(name) == "t"
    expected = [99] if holds else ([1, 2, 3] if reaches_table else None)
    _assert_read(session, f"SELECT id FROM {name}", expected)
    assert _table(session, "t") == [1, 2, 3]


@pytest.mark.parametrize(("template", "token", "written"), ROUTES, ids=ROUTE_IDS)
@pytest.mark.parametrize("name", ASCII_NAMES)
@pytest.mark.parametrize("view", ASCII_VIEWS)
@pytest.mark.parametrize(("created", "looked_up"), SWITCHES, ids=SWITCH_IDS)
def test_a_switched_case_rule_writes_through_the_name_stored_at_creation(
    session: ReparkSession,
    created: bool,
    looked_up: bool,
    view: str,
    name: str,
    template: str,
    token: str,
    written: list[int],
) -> None:
    _seed(session, "t", view, created, looked_up)
    _assert_write(
        session,
        template.format(name),
        "t",
        holds=_holds(view, created, name, looked_up),
        reaches_table=_segment(name) == "t",
        token=token,
        written=written,
    )


@pytest.mark.parametrize("name", ASCII_NAMES)
@pytest.mark.parametrize("view", ASCII_VIEWS)
@pytest.mark.parametrize(("created", "looked_up"), SWITCHES, ids=SWITCH_IDS)
def test_a_switched_case_rule_write_to_follows_the_name_stored_at_creation(
    session: ReparkSession, created: bool, looked_up: bool, view: str, name: str
) -> None:
    _seed(session, "t", view, created, looked_up)
    _assert_write_to(
        session,
        name,
        "t",
        holds=_holds(view, created, name, looked_up),
        reaches_table=_segment(name) == "t",
    )


@pytest.mark.parametrize("name", LETTER_NAMES)
@pytest.mark.parametrize("view", LETTER_VIEWS)
@pytest.mark.parametrize("table", LETTER_TABLES)
@pytest.mark.parametrize("case_sensitive", [False, True], ids=["default", "case-sensitive"])
def test_a_non_ascii_view_name_reads_under_unicode_folding(
    session: ReparkSession, case_sensitive: bool, table: str, view: str, name: str
) -> None:
    _seed(session, table, view, case_sensitive, case_sensitive)
    holds = _holds(view, case_sensitive, name, case_sensitive)
    reaches_table = _segment(name) == table
    expected = [99] if holds else ([1, 2, 3] if reaches_table else None)
    _assert_read(session, f"SELECT id FROM {name}", expected)
    assert _table(session, table) == [1, 2, 3]


@pytest.mark.parametrize(("template", "token", "written"), ROUTES, ids=ROUTE_IDS)
@pytest.mark.parametrize("name", LETTER_NAMES)
@pytest.mark.parametrize("view", LETTER_VIEWS)
@pytest.mark.parametrize("table", LETTER_TABLES)
@pytest.mark.parametrize("case_sensitive", [False, True], ids=["default", "case-sensitive"])
def test_a_non_ascii_view_name_holds_its_writes_under_unicode_folding(
    session: ReparkSession,
    case_sensitive: bool,
    table: str,
    view: str,
    name: str,
    template: str,
    token: str,
    written: list[int],
) -> None:
    _seed(session, table, view, case_sensitive, case_sensitive)
    _assert_write(
        session,
        template.format(name),
        table,
        holds=_holds(view, case_sensitive, name, case_sensitive),
        reaches_table=_segment(name) == table,
        token=token,
        written=written,
    )


@pytest.mark.parametrize("name", LETTER_NAMES)
@pytest.mark.parametrize("view", LETTER_VIEWS)
@pytest.mark.parametrize("table", LETTER_TABLES)
@pytest.mark.parametrize("case_sensitive", [False, True], ids=["default", "case-sensitive"])
def test_a_non_ascii_view_name_holds_write_to_and_the_catalog_lookups(
    session: ReparkSession, case_sensitive: bool, table: str, view: str, name: str
) -> None:
    _seed(session, table, view, case_sensitive, case_sensitive)
    holds = _holds(view, case_sensitive, name, case_sensitive)
    reaches_table = _segment(name) == table
    assert session.catalog.tableExists(name) is (holds or reaches_table)
    for reader in (session.table, session.read.table):
        if holds or reaches_table:
            frame = reader(name)
            assert sorted(row["id"] for row in frame.collect()) == ([99] if holds else [1, 2, 3])
        else:
            with pytest.raises(Exception):  # noqa: B017
                reader(name).collect()
    _assert_write_to(session, name, table, holds=holds, reaches_table=reaches_table)


@pytest.mark.parametrize("name", ["`Ä`", "`ä`", "Ä", "ä"])
@pytest.mark.parametrize("view", LETTER_VIEWS)
@pytest.mark.parametrize("case_sensitive", [False, True], ids=["default", "case-sensitive"])
def test_drop_temp_view_matches_the_literal_name_under_the_rule(
    session: ReparkSession, case_sensitive: bool, view: str, name: str
) -> None:
    _seed(session, "ä", view, case_sensitive, case_sensitive)
    dropped = _key(view, case_sensitive) == (name if case_sensitive else name.lower())
    assert session.catalog.dropTempView(name) is dropped
    after = ([1, 2, 3] if _segment(view) == "ä" else None) if dropped else [99]
    _assert_read(session, f"SELECT id FROM {view}", after)
    assert _table(session, "ä") == [1, 2, 3]


@pytest.mark.parametrize("name", ["Ä", "ä", "my view", "a-b"])
def test_create_or_replace_temp_view_refuses_a_name_spark_cannot_parse(
    session: ReparkSession, name: str
) -> None:
    frame = session.createDataFrame([(99,)], "id INT")
    with pytest.raises(AnalysisException) as raised:
        frame.createOrReplaceTempView(name)
    assert raised.value.getCondition() == "_LEGACY_ERROR_TEMP_1321"
    assert f"Invalid view name: {name}." in str(raised.value)
    assert not session.catalog.tableExists(f"`{name}`")


@pytest.mark.parametrize(
    ("name", "reads"),
    [("`Ä`", ["`ä`", "`Ä`"]), ("1a", ["`1a`"]), ("T", ["t", "`T`"]), ("`T`", ["t", "T"])],
    ids=["quoted-letter", "leading-digit", "upper", "quoted-upper"],
)
def test_create_or_replace_temp_view_registers_a_name_spark_accepts(
    session: ReparkSession, name: str, reads: list[str]
) -> None:
    session.createDataFrame([(99,)], "id INT").createOrReplaceTempView(name)
    for read in reads:
        assert _ids(session, f"SELECT id FROM {read}") == [99]
