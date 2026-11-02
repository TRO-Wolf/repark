from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import BaseModel

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session.catalog_resolution import resolve_table_name
from repark.spark.session.sql_relations import (
    _parse_table_identifier_segments,
    _scan_sql_table_identifier_end,
)

CATALOGS = ["mem", "hd"]
SEED_ROWS = ["{'id': 1, 'v': 'a'}", "{'id': 2, 'v': 'b'}", "{'id': 3, 'v': 'c'}"]
BACKTICK_NAMES = ["a`b", "x`"]
BACKTICK_IDS = ["inner-backtick", "trailing-backtick"]
BACKTICK_WRITES = [
    ("INSERT INTO {r} VALUES (5, 'q')", "'INSERT' expects a table but {r} is a view"),
    ("INSERT OVERWRITE {r} SELECT 6, 'w'", "'INSERT' expects a table but {r} is a view"),
    ("DELETE FROM {r} WHERE id = 1", "'DELETE' expects a table but {r} is a view"),
    ("UPDATE {r} SET v = 'u'", "Table {r} does not support UPDATE TABLE"),
    (
        "MERGE INTO {r} USING (SELECT 1 AS id) s ON {r}.id = s.id WHEN MATCHED THEN DELETE",
        "Table {r} does not support MERGE INTO TABLE",
    ),
]
BACKTICK_WRITE_IDS = ["insert-into", "insert-overwrite", "delete", "update", "merge"]
LITERAL_TOKENS = ["123", "1d", "1L", "1e5", "1Y", "1S", "1BD"]
QUOTED_DROPS = [
    ("DROP TABLE 't'", "''t''"),
    ("DROP VIEW 't'", "''t''"),
    ("DROP TABLE IF EXISTS 't'", "''t''"),
    ("DROP VIEW IF EXISTS 't'", "''t''"),
    ("TRUNCATE TABLE 't'", "''t''"),
    ('DROP TABLE "t""x"', '\'"t""x"\''),
    ('DROP TABLE "t"', "'\"t\"'"),
    ('DROP TABLE IF EXISTS "t"', "'\"t\"'"),
    ('DROP VIEW "t"', "'\"t\"'"),
    ('DROP VIEW IF EXISTS "t"', "'\"t\"'"),
    ('TRUNCATE TABLE "t"', "'\"t\"'"),
]
QUOTED_DROP_IDS = [
    "drop-table-single",
    "drop-view-single",
    "drop-table-if-exists-single",
    "drop-view-if-exists-single",
    "truncate-single",
    "drop-table-doubled-quote",
    "drop-table-double",
    "drop-table-if-exists-double",
    "drop-view-double",
    "drop-view-if-exists-double",
    "truncate-double",
]
VIEW_KINDS = ["sql", "data-frame", "none"]
SEEDED_VIEW = {"sql": "t", "data-frame": "data-frame", "none": None}
QUOTED_RENAMES = [
    ('ALTER TABLE t RENAME TO "t2"', "'\"t2\"'"),
    ("ALTER TABLE t RENAME TO 't2'", "''t2''"),
    ('ALTER TABLE t RENAME TO "t2";', "'\"t2\"'"),
    ('ALTER TABLE `t` RENAME TO "t2"', "'\"t2\"'"),
    ('ALTER TABLE t RENAME TO ns."t2"', "'\"t2\"'"),
]
QUOTED_RENAME_IDS = ["double", "single", "double-semicolon", "backquoted-source", "qualified"]
VIEW_RENAMES = [
    ('ALTER VIEW t RENAME TO "t2"', "'\"t2\"'"),
    ("ALTER VIEW t RENAME TO 't2'", "''t2''"),
]
MULTI_DROPS = [
    ('DROP TABLE a, "t"', "','"),
    ('DROP TABLE IF EXISTS a,"t"', "','"),
    ('DROP TABLE t,"t"', "','"),
    ("DROP TABLE a, t", "','"),
    ("DROP TABLE IF EXISTS t, a", "','"),
    ("DROP VIEW t, a", "','"),
    ('DROP TABLE "t",a', "'\"t\"'"),
    ('DROP TABLE "a","t"', "'\"a\"'"),
]
MULTI_DROP_IDS = [
    "plain-then-double",
    "if-exists-plain-then-double",
    "same-name-twice",
    "two-plain",
    "if-exists-two-plain",
    "drop-view-two-plain",
    "double-then-plain",
    "two-double",
]


class TableState(BaseModel):
    describe: list[dict[str, str | None]]
    rows: list[str]
    properties: list[dict[str, str]]


def _ref(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _parse_refusal(near: str) -> str:
    return f"[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"


def _open(warehouse: Path) -> ReparkSession:
    spark = (
        ReparkSession.builder.appName("temp-view-ddl-target-2")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(warehouse / "hd"))
        .getOrCreate()
    )
    spark.register_memory_catalog("mem", str(warehouse / "mem"))
    return spark


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    spark = _open(tmp_path)
    try:
        yield spark
    finally:
        spark.stop()


def _seed(spark: ReparkSession, catalog: str, tables: list[str], view: str | None) -> None:
    spark.sql(f"CREATE NAMESPACE {catalog}.ns").collect()
    for table in tables:
        qualified = f"{catalog}.ns.{_ref(table)}"
        spark.sql(
            f"CREATE TABLE {qualified} (id INT, v STRING) USING iceberg "
            "TBLPROPERTIES ('format-version'='2', 'k0'='x')"
        ).collect()
        spark.sql(f"INSERT INTO {qualified} VALUES (1, 'a'), (2, 'b'), (3, 'c')").collect()
    spark.sql(f"USE {catalog}.ns").collect()
    if view == "data-frame":
        spark.createDataFrame([(99,)], "id INT").createOrReplaceTempView(tables[0])
    elif view is not None:
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {_ref(view)} AS SELECT 99 AS id").collect()


def _rows(spark: ReparkSession, query: str) -> list[str]:
    return sorted(repr(row) for row in spark.sql(query).to_arrow().to_pylist())


def _ids(spark: ReparkSession, query: str) -> list[int]:
    return sorted(row["id"] for row in spark.sql(query).to_arrow().to_pylist())


def _views(spark: ReparkSession) -> list[str]:
    return sorted(row["viewName"] for row in spark.sql("SHOW VIEWS").to_arrow().to_pylist())


def _state(spark: ReparkSession, catalog: str, table: str) -> TableState:
    qualified = f"{catalog}.ns.{_ref(table)}"
    return TableState(
        describe=spark.sql(f"DESCRIBE TABLE {qualified}").to_arrow().to_pylist(),
        rows=_rows(spark, f"SELECT * FROM {qualified}"),
        properties=spark.sql(f"SHOW TBLPROPERTIES {qualified}").to_arrow().to_pylist(),
    )


def _files(warehouse: Path, catalog: str, table: str) -> dict[str, bytes]:
    root = warehouse / catalog / "ns" / table
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _metadata_version(path: Path) -> int:
    match = re.match(r"v?(\d+)", path.name)
    return int(match.group(1)) if match is not None else -1


def _reopened_state(warehouse: Path, catalog: str, table: str) -> TableState:
    metadata = max(
        (warehouse / catalog / "ns" / table / "metadata").glob("*.metadata.json"),
        key=_metadata_version,
    )
    fresh = _open(warehouse)
    try:
        fresh.sql(f"CREATE NAMESPACE {catalog}.ns").collect()
        fresh.sql(
            f"CALL {catalog}.system.register_table("
            f"table => 'ns.{table}', metadata_file => '{metadata}')"
        ).collect()
        return _state(fresh, catalog, table)
    finally:
        fresh.stop()


def _snapshot(
    spark: ReparkSession, warehouse: Path, catalog: str, tables: list[str]
) -> dict[str, tuple[TableState, dict[str, bytes]]]:
    return {
        table: (_state(spark, catalog, table), _files(warehouse, catalog, table))
        for table in tables
    }


def _assert_refused(spark: ReparkSession, statement: str, expected: str) -> None:
    with pytest.raises(Exception) as raised:
        spark.sql(statement).collect()
    assert expected in str(raised.value)


@pytest.mark.parametrize(("template", "shape"), BACKTICK_WRITES, ids=BACKTICK_WRITE_IDS)
@pytest.mark.parametrize("name", BACKTICK_NAMES, ids=BACKTICK_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_view_name_with_an_embedded_backtick_refuses_every_write_head(
    session: ReparkSession, tmp_path: Path, catalog: str, name: str, template: str, shape: str
) -> None:
    _seed(session, catalog, [name], name)
    reference = _ref(name)
    held = _snapshot(session, tmp_path, catalog, [name])
    _assert_refused(session, template.format(r=reference), shape.format(r=reference))
    assert _snapshot(session, tmp_path, catalog, [name]) == held
    assert _ids(session, f"SELECT id FROM {reference}") == [99]
    assert _views(session) == [name]
    session.stop()
    assert _reopened_state(tmp_path, catalog, name) == held[name][0]


@pytest.mark.parametrize("name", BACKTICK_NAMES, ids=BACKTICK_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_read_through_a_view_name_with_an_embedded_backtick_reads_the_view(
    session: ReparkSession, tmp_path: Path, catalog: str, name: str
) -> None:
    _seed(session, catalog, [name], name)
    held = _snapshot(session, tmp_path, catalog, [name])
    assert _rows(session, f"SELECT * FROM {_ref(name)}") == ["{'id': 99}"]
    assert _snapshot(session, tmp_path, catalog, [name]) == held


@pytest.mark.parametrize("name", BACKTICK_NAMES, ids=BACKTICK_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_table_through_a_view_name_with_an_embedded_backtick_drops_the_view(
    session: ReparkSession, tmp_path: Path, catalog: str, name: str
) -> None:
    _seed(session, catalog, [name], name)
    held = _snapshot(session, tmp_path, catalog, [name])
    session.sql(f"DROP TABLE {_ref(name)}").collect()
    assert _views(session) == []
    assert _rows(session, f"SELECT * FROM {_ref(name)}") == SEED_ROWS
    assert _snapshot(session, tmp_path, catalog, [name]) == held


@pytest.mark.parametrize("view", [True, False], ids=["view", "no-view"])
@pytest.mark.parametrize("token", [*LITERAL_TOKENS, "1t$"])
@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_table_on_a_token_spark_cannot_parse_refuses_and_holds_both(
    session: ReparkSession, tmp_path: Path, catalog: str, token: str, view: bool
) -> None:
    _seed(session, catalog, [token], token if view else None)
    held = _snapshot(session, tmp_path, catalog, [token])
    near = "'$': extra input '$'" if token == "1t$" else f"'{token}'"
    _assert_refused(session, f"DROP TABLE {token}", _parse_refusal(near))
    assert _snapshot(session, tmp_path, catalog, [token]) == held
    if view:
        assert _views(session) == [token]
        assert _ids(session, f"SELECT id FROM {_ref(token)}") == [99]


@pytest.mark.parametrize("token", ["0x1", "1_t"])
@pytest.mark.parametrize("catalog", CATALOGS)
def test_drop_table_on_a_digit_leading_identifier_spark_parses_drops_the_view(
    session: ReparkSession, tmp_path: Path, catalog: str, token: str
) -> None:
    _seed(session, catalog, [token], token)
    held = _snapshot(session, tmp_path, catalog, [token])
    session.sql(f"DROP TABLE {token}").collect()
    assert _views(session) == []
    assert _snapshot(session, tmp_path, catalog, [token]) == held


@pytest.mark.parametrize(("statement", "near"), QUOTED_DROPS, ids=QUOTED_DROP_IDS)
@pytest.mark.parametrize("view", VIEW_KINDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_quoted_drop_or_truncate_target_refuses_with_the_parse_error_and_holds_both(
    session: ReparkSession, tmp_path: Path, catalog: str, view: str, statement: str, near: str
) -> None:
    _seed(session, catalog, ["t"], SEEDED_VIEW[view])
    held = _snapshot(session, tmp_path, catalog, ["t"])
    _assert_refused(session, statement, _parse_refusal(near))
    assert _snapshot(session, tmp_path, catalog, ["t"]) == held
    if view != "none":
        assert _views(session) == ["t"]
        assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(("statement", "near"), QUOTED_RENAMES, ids=QUOTED_RENAME_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_quoted_rename_destination_refuses_and_never_renames_the_view(
    session: ReparkSession, tmp_path: Path, catalog: str, statement: str, near: str
) -> None:
    _seed(session, catalog, ["t"], "t")
    held = _snapshot(session, tmp_path, catalog, ["t"])
    _assert_refused(session, statement, _parse_refusal(near))
    assert _snapshot(session, tmp_path, catalog, ["t"]) == held
    assert _views(session) == ["t"]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(("statement", "near"), VIEW_RENAMES, ids=["double", "single"])
@pytest.mark.parametrize("view", [True, False], ids=["view", "no-view"])
@pytest.mark.parametrize("catalog", CATALOGS)
def test_alter_view_rename_to_a_quoted_destination_refuses_with_the_parse_error(
    session: ReparkSession, tmp_path: Path, catalog: str, view: bool, statement: str, near: str
) -> None:
    _seed(session, catalog, ["t"], "t" if view else None)
    held = _snapshot(session, tmp_path, catalog, ["t"])
    _assert_refused(session, statement, _parse_refusal(near))
    assert _snapshot(session, tmp_path, catalog, ["t"]) == held
    assert _views(session) == (["t"] if view else [])


@pytest.mark.parametrize(("statement", "near"), MULTI_DROPS, ids=MULTI_DROP_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_drop_with_more_than_one_target_refuses_and_drops_nothing(
    session: ReparkSession, tmp_path: Path, catalog: str, statement: str, near: str
) -> None:
    _seed(session, catalog, ["t", "a"], "t")
    held = _snapshot(session, tmp_path, catalog, ["t", "a"])
    _assert_refused(session, statement, _parse_refusal(near))
    assert _snapshot(session, tmp_path, catalog, ["t", "a"]) == held
    assert _views(session) == ["t"]
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize(
    ("text", "start", "end"),
    [
        ("INSERT INTO `a``b` VALUES (1)", 12, 18),
        ("INSERT INTO `x``` VALUES (1)", 12, 17),
        ("INSERT INTO cat.`a``b`.t VALUES (1)", 12, 24),
        ('INSERT INTO "a""b" VALUES (1)', 12, 18),
        ("INSERT INTO `a` VALUES (1)", 12, 15),
    ],
    ids=["inner-backtick", "trailing-backtick", "qualified", "doubled-double-quote", "plain"],
)
def test_the_scanner_reads_a_doubled_backtick_as_part_of_the_name(
    text: str, start: int, end: int
) -> None:
    assert _scan_sql_table_identifier_end(text, start) == end


@pytest.mark.parametrize(
    ("name", "segments"),
    [
        ("`a``b`", ["a`b"]),
        ("`x```", ["x`"]),
        ("cat.`a``b`.t", ["cat", "a`b", "t"]),
        ("```x`", ["`x"]),
        ('"a""b"', ['a"b']),
    ],
    ids=["inner", "trailing", "qualified", "leading", "doubled-double-quote"],
)
def test_the_segment_parser_reads_a_doubled_backtick_as_one_segment(
    name: str, segments: list[str]
) -> None:
    assert _parse_table_identifier_segments(name) == segments


@pytest.mark.parametrize("token", [*LITERAL_TOKENS, "1t$", "'t'", "1e5f", "2bd"])
def test_a_token_spark_cannot_parse_never_reaches_the_view_lookup(token: str) -> None:
    looked_up: list[str] = []

    def probe(name: str) -> list[str]:
        looked_up.append(name)
        return ["datafusion", "public", name]

    with pytest.raises(AnalysisException, match="invalid table identifier"):
        resolve_table_name(
            token,
            current_catalog="mem",
            current_database="ns",
            prefer_temp_view=True,
            temp_view_home_ref=probe,
        )
    assert looked_up == []


@pytest.mark.parametrize("token", ["1t", "9x_1", "0x1", "1_t", "1e", "1e5x"])
def test_a_digit_leading_identifier_spark_parses_reaches_the_view_lookup(token: str) -> None:
    resolved = resolve_table_name(
        token,
        current_catalog="mem",
        current_database="ns",
        prefer_temp_view=True,
        temp_view_home_ref=lambda name: ["datafusion", "public", name],
    )
    assert resolved == f"datafusion.public.`{token}`"
