from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import BaseModel

from repark import ReparkSession

NO_ALTERNATIVE = "NO_ALTERNATIVE"
USE_ALTER_VIEW = "USE_ALTER_VIEW"
CATALOGS = ["mem", "hd"]
SEED_ROWS = ["{'id': 1, 'v': 'a'}", "{'id': 2, 'v': 'b'}", "{'id': 3, 'v': 'c'}"]

REFUSED = [
    ("ALTER TABLE {} ADD COLUMN z INT", "ADD COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ADD COLUMNS (z INT)", "ADD COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} ADD COLUMNS (z INT, y STRING)", "ADD COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} ADD COLUMN s.x INT", "ADD COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} DROP COLUMN v", "DROP COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} DROP COLUMNS (v)", "DROP COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} DROP COLUMN IF EXISTS v", "DROP COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} DROP COLUMNS IF EXISTS (v, zz)", "DROP COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} RENAME COLUMN v TO v2", "RENAME COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER COLUMN id TYPE BIGINT", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER COLUMN v COMMENT 'c'", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER COLUMN v FIRST", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER COLUMN id AFTER v", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER COLUMN v DROP NOT NULL", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER COLUMN v SET DEFAULT 'q'", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} ALTER v COMMENT 'c'", "ALTER COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} CHANGE COLUMN v COMMENT 'c'", "CHANGE COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} CHANGE COLUMN v v STRING COMMENT 'c'", "CHANGE COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} CHANGE v v STRING COMMENT 'c'", "CHANGE COLUMN", NO_ALTERNATIVE),
    ("ALTER TABLE {} REPLACE COLUMNS (id INT)", "REPLACE COLUMNS", NO_ALTERNATIVE),
    ("ALTER TABLE {} SET TBLPROPERTIES ('k1'='x')", "SET TBLPROPERTIES", USE_ALTER_VIEW),
    (
        "ALTER TABLE {} SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')",
        "SET TBLPROPERTIES",
        USE_ALTER_VIEW,
    ),
    ("ALTER TABLE {} UNSET TBLPROPERTIES ('k0')", "UNSET TBLPROPERTIES", USE_ALTER_VIEW),
    (
        "ALTER TABLE {} UNSET TBLPROPERTIES IF EXISTS ('k9')",
        "UNSET TBLPROPERTIES",
        USE_ALTER_VIEW,
    ),
    (
        "ALTER TABLE {} SET SERDEPROPERTIES ('a'='b')",
        "SET [SERDE|SERDEPROPERTIES]",
        USE_ALTER_VIEW,
    ),
    ("ALTER TABLE {} SET SERDE 'x'", "SET [SERDE|SERDEPROPERTIES]", USE_ALTER_VIEW),
    ("ALTER TABLE {} SET LOCATION 'relocated'", "SET LOCATION ...", NO_ALTERNATIVE),
    (
        "ALTER TABLE {} PARTITION (id=1) SET LOCATION 'relocated'",
        "SET LOCATION ...",
        NO_ALTERNATIVE,
    ),
    ("ALTER TABLE {} ADD PARTITION (id=1)", "ADD PARTITION ...", NO_ALTERNATIVE),
    ("ALTER TABLE {} DROP PARTITION (id=1)", "DROP PARTITION ...", NO_ALTERNATIVE),
    (
        "ALTER TABLE {} PARTITION (id=1) RENAME TO PARTITION (id=2)",
        "RENAME TO PARTITION",
        NO_ALTERNATIVE,
    ),
    ("ALTER TABLE {} CLUSTER BY (id)", "CLUSTER BY", NO_ALTERNATIVE),
    ("ALTER TABLE {} RECOVER PARTITIONS", "RECOVER PARTITIONS", NO_ALTERNATIVE),
    ("ALTER TABLE {} DROP CONSTRAINT c1", "DROP CONSTRAINT", NO_ALTERNATIVE),
]
REFUSED_IDS = [
    "add-column",
    "add-columns",
    "add-columns-two",
    "add-nested-column",
    "drop-column",
    "drop-columns",
    "drop-column-if-exists",
    "drop-columns-if-exists",
    "rename-column",
    "alter-column-type",
    "alter-column-comment",
    "alter-column-first",
    "alter-column-after",
    "alter-column-drop-not-null",
    "alter-column-set-default",
    "alter-without-column-keyword",
    "change-column-comment",
    "change-column-full",
    "change-without-column-keyword",
    "replace-columns",
    "set-tblproperties",
    "set-tblproperties-write-mode",
    "unset-tblproperties",
    "unset-tblproperties-if-exists",
    "set-serdeproperties",
    "set-serde",
    "set-location",
    "partition-set-location",
    "add-partition",
    "drop-partition",
    "rename-partition",
    "cluster-by",
    "recover-partitions",
    "drop-constraint",
]
REPARK_SPELLINGS = [
    ("ALTER TABLE {} ADD z INT", "ADD COLUMN"),
    ("ALTER TABLE {} DROP v", "DROP COLUMNS"),
    ("ALTER TABLE {} RENAME v TO v2", "RENAME COLUMN"),
    ("ALTER TABLE IF EXISTS {} DROP COLUMN v", "DROP COLUMNS"),
    ("ALTER TABLE {} ADD COLUMN IF NOT EXISTS z INT", "ADD COLUMN"),
    ("ALTER TABLE {} ADD COLUMN z INT, DROP COLUMN v", "ADD COLUMN"),
]
REPARK_SPELLING_IDS = [
    "add-without-column-keyword",
    "drop-without-column-keyword",
    "rename-without-column-keyword",
    "alter-table-if-exists",
    "add-column-if-not-exists",
    "two-operations",
]
APPLIED = [
    "ALTER TABLE {} ADD COLUMN z INT",
    "ALTER TABLE {} ADD COLUMNS (z INT)",
    "ALTER TABLE {} DROP COLUMN v",
    "ALTER TABLE {} DROP COLUMNS (v)",
    "ALTER TABLE {} DROP COLUMN IF EXISTS v",
    "ALTER TABLE {} RENAME COLUMN v TO v2",
    "ALTER TABLE {} ALTER COLUMN id TYPE BIGINT",
    "ALTER TABLE {} ALTER COLUMN v COMMENT 'c'",
    "ALTER TABLE {} ALTER COLUMN v FIRST",
    "ALTER TABLE {} CHANGE COLUMN v COMMENT 'c'",
    "ALTER TABLE {} REPLACE COLUMNS (id INT)",
    "ALTER TABLE {} SET TBLPROPERTIES ('k1'='x')",
    "ALTER TABLE {} UNSET TBLPROPERTIES ('k0')",
]
APPLIED_IDS = [
    "add-column",
    "add-columns",
    "drop-column",
    "drop-columns",
    "drop-column-if-exists",
    "rename-column",
    "alter-column-type",
    "alter-column-comment",
    "alter-column-first",
    "change-column-comment",
    "replace-columns",
    "set-tblproperties",
    "unset-tblproperties",
]
EXTENSION_CHANGES = ["ALTER TABLE {} ADD PARTITION FIELD id", "ALTER TABLE {} WRITE ORDERED BY id"]
IDENTIFIER_FIELDS_REFUSAL = "Cannot add field id as an identifier field: not a required field"
ALREADY_EXISTS = (
    "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `{}` because it already exists.\n"
    "Choose a different name, drop or replace the existing object, or add the IF NOT EXISTS "
    "clause to tolerate pre-existing objects. SQLSTATE: 42P07"
)


class TableState(BaseModel):
    describe: list[dict[str, str | None]]
    rows: list[str]
    properties: list[dict[str, str]]


def _open(warehouse: Path) -> ReparkSession:
    spark = (
        ReparkSession.builder.appName("temp-view-ddl-target")
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


def _seed(
    spark: ReparkSession, catalog: str, view: str | None, *, case_sensitive: bool = False
) -> None:
    spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
    statements = [
        f"CREATE NAMESPACE {catalog}.ns",
        f"CREATE TABLE {catalog}.ns.t (id INT, v STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2', 'k0'='x')",
        f"INSERT INTO {catalog}.ns.t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        f"USE {catalog}.ns",
    ]
    if view is not None:
        statements.append(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id")
    for statement in statements:
        spark.sql(statement).collect()


def _rows(spark: ReparkSession, query: str) -> list[str]:
    return sorted(repr(row) for row in spark.sql(query).to_arrow().to_pylist())


def _state(spark: ReparkSession, catalog: str) -> TableState:
    table = f"{catalog}.ns.t"
    return TableState(
        describe=spark.sql(f"DESCRIBE TABLE {table}").to_arrow().to_pylist(),
        rows=_rows(spark, f"SELECT * FROM {table}"),
        properties=spark.sql(f"SHOW TBLPROPERTIES {table}").to_arrow().to_pylist(),
    )


def _files(warehouse: Path, catalog: str) -> dict[str, bytes]:
    root = warehouse / catalog / "ns" / "t"
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _metadata_version(path: Path) -> int:
    match = re.match(r"v?(\d+)", path.name)
    return int(match.group(1)) if match is not None else -1


def _reopened_state(warehouse: Path, catalog: str) -> TableState:
    metadata = max(
        (warehouse / catalog / "ns" / "t" / "metadata").glob("*.metadata.json"),
        key=_metadata_version,
    )
    fresh = _open(warehouse)
    try:
        fresh.sql(f"CREATE NAMESPACE {catalog}.ns").collect()
        fresh.sql(
            f"CALL {catalog}.system.register_table(table => 'ns.t', metadata_file => '{metadata}')"
        ).collect()
        return _state(fresh, catalog)
    finally:
        fresh.stop()


def _ids(spark: ReparkSession, query: str) -> list[int]:
    return sorted(row["id"] for row in spark.sql(query).to_arrow().to_pylist())


def _views(spark: ReparkSession) -> list[str]:
    return sorted(row["viewName"] for row in spark.sql("SHOW VIEWS").to_arrow().to_pylist())


def _refusal(operation: str, alternative: str, shown: str) -> str:
    sentence = (
        f"[EXPECT_TABLE_NOT_VIEW.{alternative}] 'ALTER TABLE ... {operation}' expects a table "
        f"but `{shown}` is a view."
    )
    if alternative == USE_ALTER_VIEW:
        return f"{sentence} Please use ALTER VIEW instead. SQLSTATE: 42809"
    return f"{sentence}  SQLSTATE: 42809"


def _assert_refused_and_held(
    spark: ReparkSession, warehouse: Path, catalog: str, name: str, statement: str, expected: str
) -> None:
    state = _state(spark, catalog)
    files = _files(warehouse, catalog)
    with pytest.raises(Exception, match=r"EXPECT_TABLE_NOT_VIEW") as raised:
        spark.sql(statement).collect()
    assert expected in str(raised.value)
    assert _state(spark, catalog) == state
    assert _files(warehouse, catalog) == files
    assert _ids(spark, f"SELECT id FROM {name}") == [99]
    spark.stop()
    assert _reopened_state(warehouse, catalog) == state


def test_minimal_reproduction_drop_column_through_a_view_name_refuses(
    session: ReparkSession,
) -> None:
    _seed(session, "mem", "t")
    with pytest.raises(Exception, match=r"EXPECT_TABLE_NOT_VIEW\.NO_ALTERNATIVE"):
        session.sql("ALTER TABLE t DROP COLUMN v").collect()
    assert _state(session, "mem").rows == SEED_ROWS
    assert _ids(session, "SELECT * FROM t") == [99]


@pytest.mark.parametrize(("template", "operation", "alternative"), REFUSED, ids=REFUSED_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_every_alter_through_a_view_name_refuses_and_holds_the_table(
    session: ReparkSession,
    tmp_path: Path,
    catalog: str,
    template: str,
    operation: str,
    alternative: str,
) -> None:
    _seed(session, catalog, "t")
    expected = _refusal(operation, alternative, "t")
    _assert_refused_and_held(session, tmp_path, catalog, "t", template.format("t"), expected)


@pytest.mark.parametrize(("template", "operation"), REPARK_SPELLINGS, ids=REPARK_SPELLING_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_repark_only_alter_spellings_through_a_view_name_refuse_and_hold_the_table(
    session: ReparkSession, tmp_path: Path, catalog: str, template: str, operation: str
) -> None:
    _seed(session, catalog, "t")
    expected = _refusal(operation, NO_ALTERNATIVE, "t")
    _assert_refused_and_held(session, tmp_path, catalog, "t", template.format("t"), expected)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_rename_to_renames_the_view_and_leaves_the_table(
    session: ReparkSession, tmp_path: Path, catalog: str
) -> None:
    _seed(session, catalog, "t")
    state = _state(session, catalog)
    files = _files(tmp_path, catalog)
    session.sql("ALTER TABLE t RENAME TO t2").collect()
    assert _ids(session, "SELECT id FROM t2") == [99]
    assert _views(session) == ["t2"]
    assert _rows(session, "SELECT * FROM t") == SEED_ROWS
    assert _state(session, catalog) == state
    assert _files(tmp_path, catalog) == files
    session.stop()
    assert _reopened_state(tmp_path, catalog) == state


@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_renamed_view_serves_new_views_and_describe(session: ReparkSession, catalog: str) -> None:
    _seed(session, catalog, "t")
    session.sql("ALTER TABLE t RENAME TO t2").collect()
    session.sql("CREATE TEMPORARY VIEW w AS SELECT id + 1 AS id FROM t2").collect()
    assert _ids(session, "SELECT id FROM w") == [100]
    rows = session.sql("DESCRIBE TABLE t2").to_arrow().to_pylist()
    assert [(row["col_name"], row["data_type"]) for row in rows] == [("id", "int")]


@pytest.mark.parametrize("catalog", CATALOGS)
def test_rename_to_renames_a_data_frame_view(session: ReparkSession, catalog: str) -> None:
    _seed(session, catalog, None)
    session.createDataFrame([(99,)], "id INT").createOrReplaceTempView("t")
    state = _state(session, catalog)
    session.sql("ALTER TABLE t RENAME TO t2").collect()
    assert _ids(session, "SELECT id FROM t2") == [99]
    assert _views(session) == ["t2"]
    assert _state(session, catalog) == state


@pytest.mark.parametrize(
    ("destination", "shown"), [("t2", "t2"), ("t", "t"), ("T", "T")], ids=["other", "same", "upper"]
)
def test_rename_to_an_existing_view_name_refuses(
    session: ReparkSession, destination: str, shown: str
) -> None:
    _seed(session, "mem", "t")
    session.sql("CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT 7 AS id").collect()
    state = _state(session, "mem")
    with pytest.raises(Exception, match=r"TABLE_OR_VIEW_ALREADY_EXISTS") as raised:
        session.sql(f"ALTER TABLE t RENAME TO {destination}").collect()
    assert ALREADY_EXISTS.format(shown) in str(raised.value)
    assert _ids(session, "SELECT id FROM t") == [99]
    assert _ids(session, "SELECT id FROM t2") == [7]
    assert _state(session, "mem") == state


@pytest.mark.parametrize(
    ("destination", "expected"),
    [
        (
            "a.b.t2",
            "[IDENTIFIER_TOO_MANY_NAME_PARTS] `a`.`b`.`t2` is not a valid identifier as it has "
            "more than 2 name parts. SQLSTATE: 42601",
        ),
        (
            "ns.t2",
            "RENAME TABLE source and destination databases do not match: 'default' != 'ns'.",
        ),
        (
            "default.t2",
            "RENAME TEMPORARY VIEW from '`t`' to '`default`.`t2`': cannot specify database "
            "name 'default' in the destination table.",
        ),
    ],
    ids=["three-part", "two-part", "two-part-default"],
)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_rename_to_a_qualified_name_refuses_and_holds_the_view_and_the_table(
    session: ReparkSession, catalog: str, destination: str, expected: str
) -> None:
    _seed(session, catalog, "t")
    state = _state(session, catalog)
    with pytest.raises(Exception) as raised:
        session.sql(f"ALTER TABLE t RENAME TO {destination}").collect()
    assert expected in str(raised.value)
    assert _ids(session, "SELECT id FROM t") == [99]
    assert _views(session) == ["t"]
    assert _state(session, catalog) == state


def test_rename_to_a_quoted_name_renames_the_view(session: ReparkSession) -> None:
    _seed(session, "mem", "t")
    session.sql("ALTER TABLE t RENAME TO `t 2`").collect()
    assert _ids(session, "SELECT id FROM `t 2`") == [99]
    assert _views(session) == ["t 2"]


@pytest.mark.parametrize("template", APPLIED, ids=APPLIED_IDS)
@pytest.mark.parametrize("view", [None, "w"], ids=["no-view", "other-view"])
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_bare_table_name_alters_the_table_when_no_view_holds_it(
    session: ReparkSession, catalog: str, view: str | None, template: str
) -> None:
    _seed(session, catalog, view)
    state = _state(session, catalog)
    session.sql(template.format("t")).collect()
    assert _state(session, catalog) != state
    if view is not None:
        assert _ids(session, f"SELECT id FROM {view}") == [99]


@pytest.mark.parametrize("template", APPLIED, ids=APPLIED_IDS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_qualified_table_name_alters_the_table_with_the_view_in_scope(
    session: ReparkSession, catalog: str, template: str
) -> None:
    _seed(session, catalog, "t")
    state = _state(session, catalog)
    session.sql(template.format(f"{catalog}.ns.t")).collect()
    assert _state(session, catalog) != state
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize("template", EXTENSION_CHANGES, ids=["add-partition-field", "write-order"])
@pytest.mark.parametrize("catalog", CATALOGS)
def test_iceberg_extension_alters_reach_the_table_with_the_view_in_scope(
    session: ReparkSession, tmp_path: Path, catalog: str, template: str
) -> None:
    _seed(session, catalog, "t")
    files = _files(tmp_path, catalog)
    session.sql(template.format("t")).collect()
    assert _files(tmp_path, catalog) != files
    assert _ids(session, "SELECT id FROM t") == [99]


@pytest.mark.parametrize("catalog", CATALOGS)
def test_set_identifier_fields_through_a_view_name_refuses_as_on_the_table(
    session: ReparkSession, tmp_path: Path, catalog: str
) -> None:
    _seed(session, catalog, "t")
    files = _files(tmp_path, catalog)
    with pytest.raises(Exception, match=re.escape(IDENTIFIER_FIELDS_REFUSAL)):
        session.sql("ALTER TABLE t SET IDENTIFIER FIELDS id").collect()
    assert _files(tmp_path, catalog) == files


@pytest.mark.parametrize("catalog", CATALOGS)
def test_rename_to_with_no_view_reaches_the_catalog(session: ReparkSession, catalog: str) -> None:
    _seed(session, catalog, None)
    if catalog == "hd":
        with pytest.raises(Exception, match=r"Cannot rename Hadoop tables"):
            session.sql("ALTER TABLE t RENAME TO t2").collect()
        return
    session.sql("ALTER TABLE t RENAME TO t2").collect()
    tables = session.sql("SHOW TABLES IN mem.ns").to_arrow().to_pylist()
    assert [row["tableName"] for row in tables] == ["t2"]


@pytest.mark.parametrize(
    ("view", "name", "shown"),
    [("T", "t", "t"), ("t", "T", "T"), ("T", "`T`", "T"), ("`t`", "t", "t")],
    ids=["view-upper", "name-upper", "name-quoted-upper", "view-quoted"],
)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_every_spelling_of_a_view_name_refuses_under_the_default_rule(
    session: ReparkSession, tmp_path: Path, catalog: str, view: str, name: str, shown: str
) -> None:
    _seed(session, catalog, view)
    expected = _refusal("DROP COLUMNS", NO_ALTERNATIVE, shown)
    _assert_refused_and_held(
        session, tmp_path, catalog, name, f"ALTER TABLE {name} DROP COLUMN v", expected
    )


@pytest.mark.parametrize("catalog", CATALOGS)
def test_under_case_sensitive_names_an_upper_case_view_does_not_hold_the_lower_case_table(
    session: ReparkSession, catalog: str
) -> None:
    _seed(session, catalog, "T", case_sensitive=True)
    session.sql("ALTER TABLE t DROP COLUMN v").collect()
    assert _state(session, catalog).rows == ["{'id': 1}", "{'id': 2}", "{'id': 3}"]
    assert _ids(session, "SELECT id FROM T") == [99]


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("ALTER TABLE T DROP COLUMN v", _refusal("DROP COLUMNS", NO_ALTERNATIVE, "T")),
        (
            "ALTER TABLE T SET TBLPROPERTIES ('k1'='x')",
            _refusal("SET TBLPROPERTIES", USE_ALTER_VIEW, "T"),
        ),
    ],
    ids=["drop-column", "set-tblproperties"],
)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_under_case_sensitive_names_the_exact_view_name_refuses(
    session: ReparkSession, tmp_path: Path, catalog: str, statement: str, expected: str
) -> None:
    _seed(session, catalog, "T", case_sensitive=True)
    _assert_refused_and_held(session, tmp_path, catalog, "T", statement, expected)


@pytest.mark.parametrize("catalog", CATALOGS)
def test_under_case_sensitive_names_rename_to_renames_the_exact_view(
    session: ReparkSession, catalog: str
) -> None:
    _seed(session, catalog, "T", case_sensitive=True)
    state = _state(session, catalog)
    session.sql("ALTER TABLE T RENAME TO t2").collect()
    assert _views(session) == ["t2"]
    assert _ids(session, "SELECT id FROM t2") == [99]
    assert _state(session, catalog) == state


@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_switched_case_rule_alter_follows_the_name_stored_at_creation(
    session: ReparkSession, tmp_path: Path, catalog: str
) -> None:
    _seed(session, catalog, "T")
    session.conf.set("spark.sql.caseSensitive", "true")
    expected = _refusal("DROP COLUMNS", NO_ALTERNATIVE, "t")
    _assert_refused_and_held(
        session, tmp_path, catalog, "t", "ALTER TABLE t DROP COLUMN v", expected
    )


@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_view_stored_under_the_exact_rule_does_not_hold_the_folded_name(
    session: ReparkSession, catalog: str
) -> None:
    _seed(session, catalog, "T", case_sensitive=True)
    session.conf.set("spark.sql.caseSensitive", "false")
    session.sql("ALTER TABLE t DROP COLUMN v").collect()
    assert _state(session, catalog).rows == ["{'id': 1}", "{'id': 2}", "{'id': 3}"]
