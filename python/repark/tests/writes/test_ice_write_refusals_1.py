"""U6 WRITE-REFUSALS — refusal parity on `write.spark.accept-any-schema` tables.

Oracle: live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, measured on 2026-09-24 by
``target/probe-u6/probe_spark.py`` (probes a1..g9 and f1..f13) and the scoreboard records of the
cells ``W-ACCEPT-ANY-INSERT-VALUES``, ``W-MERGE-SCHEMA-EVOLUTION*``, ``W-DF-OPT-MERGE-SCHEMA*``,
``TP-ACCEPT-ANY-SCHEMA-DF`` and ``W-INSERT-MERGE-SCHEMA-CONF``.

Every expected value below is Spark's recorded answer, not a RePark derivation.

The round-2 pins (C-010..C-014) come from ``target/probe-u6-r1fix/spark_probe.py`` and
``spark_probe2.py`` (probes a1..a10, b1..b2, c1..c4, d1..d4, f1..f9, g1..g12, r1..r2).

The round-3 pins (C-015..C-018) come from ``target/probe-u6-r2fix/spark.out`` and
``spark2.out`` (probes n00..n70, s1..s13, m00..m41, w1..w8) and the critic's h1..h9.

The round-4 pins come from ``target/probe-u6-r3fix/spark.out`` (probes y1..y18) and the
critic's x1..x15.

pins: u6-write-refusals/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-010, C-011,
C-012, C-013, C-014, C-015, C-016, C-017, C-018
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    IllegalArgumentException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)

NS = "mem.ns"
MERGE_SCHEMA_CONF = "spark.sql.iceberg.merge-schema"
SEED = "INSERT INTO {t} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"
NAMED_SEED = "INSERT INTO {t} SELECT 1 AS id, 'a' AS data, 'x' AS cat"
EVOLVING_MERGE = (
    "MERGE WITH SCHEMA EVOLUTION INTO {t} t USING {v} s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)
BASE_SCHEMA = [("id", "int64"), ("data", "string"), ("cat", "string")]
HSRC = "CREATE OR REPLACE TEMP VIEW hsrc AS SELECT CAST(1 AS BIGINT) AS id, 'a' AS data"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog and one namespace."""
    session = ReparkSession.builder.appName("pytest-ice-write-refusals-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _create(spark: ReparkSession, name: str, *, accept_any: bool) -> str:
    """Create ``(id BIGINT, data STRING, cat STRING)``, optionally with accept-any-schema."""
    table = f"{NS}.{name}"
    props = "'format-version' = '2'"
    if accept_any:
        props += ", 'write.spark.accept-any-schema' = 'true'"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ({props})"
    )
    return table


def _schema(spark: ReparkSession, table: str) -> list[tuple[str, str]]:
    """Return the table's Arrow column names and types."""
    arrow = spark.sql(f"SELECT * FROM {table} LIMIT 0").to_arrow().schema
    return [(field.name, str(field.type)) for field in arrow]


def _rows(spark: ReparkSession, table: str) -> list[list[Any]]:
    """Return every row as a list, sorted by its repr."""
    arrow = spark.sql(f"SELECT * FROM {table}").to_arrow()
    names = arrow.schema.names
    return sorted(([row[name] for name in names] for row in arrow.to_pylist()), key=repr)


def _snapshots(spark: ReparkSession, table: str) -> int:
    """Return the table's snapshot count."""
    return spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").to_arrow().to_pylist()[0]["n"]


def _assert_illegal_argument(action: Callable[[], object], message: str) -> None:
    """Run ``action`` and pin Spark's condition-less ``IllegalArgumentException`` exactly."""
    with pytest.raises(IllegalArgumentException) as caught:
        action()
    assert str(caught.value) == message
    assert caught.value.getCondition() is None
    assert caught.value.getErrorClass() is None
    assert caught.value.getSqlState() is None


def test_positional_values_seed_refuses_with_the_first_values_column(
    spark: ReparkSession,
) -> None:
    """pins: u6-write-refusals/C-001"""
    table = _create(spark, "seed", accept_any=True)
    for statement in (
        SEED,
        "INSERT INTO {t} VALUES (1, 'a', 'x'), (2, 'b', 'y')",
        "INSERT INTO {t} VALUES (9, 'z')",
        "INSERT INTO TABLE {t} VALUES (9, 'z', 'q')",
        "INSERT OVERWRITE {t} VALUES (9, 'z', 'q')",
    ):
        _assert_illegal_argument(
            lambda sql=statement: spark.sql(sql.format(t=table)),
            "Field col1 not found in source schema",
        )
    assert _rows(spark, table) == []
    assert _schema(spark, table) == BASE_SCHEMA


def test_positional_select_refuses_with_the_first_unknown_output_name(
    spark: ReparkSession,
) -> None:
    """pins: u6-write-refusals/C-002"""
    table = _create(spark, "literal", accept_any=True)
    cases = [
        ("INSERT INTO {t} SELECT 9, 'z', 'q'", "Field 9 not found in source schema"),
        ("INSERT INTO {t} SELECT 9", "Field 9 not found in source schema"),
        ("INSERT INTO {t} SELECT 9 AS id, 'z', 'q' AS cat", "Field z not found in source schema"),
        ("INSERT INTO {t} SELECT 9 AS a, 'z' AS b", "Field a not found in source schema"),
        (
            "INSERT INTO {t} SELECT 9 AS id, 'z' AS data, 'q' AS cat, 1 AS extra",
            "Field extra not found in source schema",
        ),
    ]
    for statement, message in cases:
        _assert_illegal_argument(lambda sql=statement: spark.sql(sql.format(t=table)), message)
    assert _rows(spark, table) == []


def test_dataframe_writes_resolve_by_name_on_accept_any(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-003"""
    table = _create(spark, "frames", accept_any=True)
    renamed = spark.createDataFrame([(7, "g", "x")], "a BIGINT, b STRING, c STRING")
    _assert_illegal_argument(
        lambda: renamed.write.insertInto(table), "Field a not found in source schema"
    )
    other = spark.createDataFrame([(7, "g", "x")], "id BIGINT, d STRING, cat STRING")
    _assert_illegal_argument(
        lambda: other.write.format("iceberg").mode("append").saveAsTable(table),
        "Field d not found in source schema",
    )
    matching = spark.createDataFrame([(7, "g", "x")], "id BIGINT, data STRING, cat STRING")
    matching.write.format("iceberg").mode("append").saveAsTable(table)
    short = spark.createDataFrame([(8, "h")], "id BIGINT, data STRING")
    short.write.format("iceberg").mode("append").saveAsTable(table)
    assert _rows(spark, table) == [[7, "g", "x"], [8, "h", None]]
    assert _schema(spark, table) == BASE_SCHEMA


def test_matching_names_and_property_less_tables_keep_writing(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-004"""
    table = _create(spark, "named", accept_any=True)
    spark.sql(f"INSERT INTO {table} SELECT 9 AS id, 'z' AS data, 'q' AS cat")
    spark.sql(f"INSERT INTO {table} SELECT 8 AS ID, 'y' AS Data")
    spark.sql(f"INSERT INTO {table} (id, data, cat) VALUES (7, 'x', 'w')")
    spark.sql(f"INSERT INTO {table} SELECT * FROM VALUES (6, 'v', 'u') AS v(id, data, cat)")
    assert _rows(spark, table) == [
        [6, "v", "u"],
        [7, "x", "w"],
        [8, "y", None],
        [9, "z", "q"],
    ]
    plain = _create(spark, "plain", accept_any=False)
    spark.sql(SEED.format(t=plain))
    spark.sql(f"INSERT INTO {plain} SELECT 9, 'z', 'q'")
    spark.sql(f"INSERT OVERWRITE {plain} VALUES (5, 'e', 'f')")
    assert _rows(spark, plain) == [[5, "e", "f"]]
    assert _schema(spark, plain) == BASE_SCHEMA


def test_merge_schema_conf_adds_the_values_columns(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-005"""
    table = _create(spark, "conf_values", accept_any=True)
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        spark.sql(SEED.format(t=table))
        spark.sql(f"INSERT INTO {table} VALUES (9, 'z', 'q')")
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)
    assert _schema(spark, table) == [
        *BASE_SCHEMA,
        ("col1", "int32"),
        ("col2", "string"),
        ("col3", "string"),
    ]
    assert _rows(spark, table) == [
        [None, None, None, 1, "a", "x"],
        [None, None, None, 2, "b", "y"],
        [None, None, None, 3, "c", "x"],
        [None, None, None, 9, "z", "q"],
    ]
    assert _snapshots(spark, table) == 2


def test_merge_schema_conf_widens_overwrites_and_refuses_like_spark(
    spark: ReparkSession,
) -> None:
    """pins: u6-write-refusals/C-006"""
    widen = _create(spark, "conf_widen", accept_any=True)
    partial = _create(spark, "conf_partial", accept_any=True)
    overwrite = _create(spark, "conf_overwrite", accept_any=True)
    conflict = _create(spark, "conf_conflict", accept_any=True)
    narrower = _create(spark, "conf_narrower", accept_any=True)
    spark.sql(NAMED_SEED.format(t=overwrite))
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        spark.sql(f"INSERT INTO {widen} VALUES (1, 'a', 'x')")
        spark.sql(f"INSERT INTO {widen} SELECT CAST(5 AS BIGINT) AS col1, 'b' AS col2, 'c' AS col3")
        spark.sql(f"INSERT INTO {partial} SELECT 9 AS id, 'z' AS newc")
        spark.sql(f"INSERT OVERWRITE {overwrite} VALUES (9, 'z', 'q')")
        spark.sql(f"INSERT INTO {narrower} SELECT CAST(9 AS INT) AS id, 'z' AS data, 'q' AS cat")
        _assert_illegal_argument(
            lambda: spark.sql(
                f"INSERT INTO {conflict} SELECT 'nine' AS id, 'z' AS data, 'q' AS cat"
            ),
            "Cannot change column type: id: long -> string",
        )
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)
    values_columns = [("col1", "int64"), ("col2", "string"), ("col3", "string")]
    assert _schema(spark, widen) == [*BASE_SCHEMA, *values_columns]
    assert _rows(spark, widen) == [[None, None, None, 1, "a", "x"], [None, None, None, 5, "b", "c"]]
    assert _schema(spark, partial) == [*BASE_SCHEMA, ("newc", "string")]
    assert _rows(spark, partial) == [[9, None, None, "z"]]
    assert _schema(spark, overwrite) == [*BASE_SCHEMA, ("col1", "int32"), *values_columns[1:]]
    assert _rows(spark, overwrite) == [[None, None, None, 9, "z", "q"]]
    assert _schema(spark, narrower) == BASE_SCHEMA
    assert _rows(spark, narrower) == [[9, "z", "q"]]
    assert _schema(spark, conflict) == BASE_SCHEMA
    assert _rows(spark, conflict) == []


def _merge_source(spark: ReparkSession, name: str, query: str) -> str:
    """Register ``query`` as a temporary view and return its name."""
    spark.sql(query).createOrReplaceTempView(name)
    return name


def test_merge_schema_evolution_refuses_an_incompatible_type_change(
    spark: ReparkSession,
) -> None:
    """pins: u6-write-refusals/C-007"""
    cases = [
        (
            "int_id",
            "SELECT * FROM VALUES (2, 'B', 'y', 7), (4, 'D', 'z', 8) AS s(id, data, cat, extra)",
            EVOLVING_MERGE,
            "Cannot change column type: id: long -> int",
        ),
        (
            "int_id_set",
            "SELECT * FROM VALUES (2, 'B', 'y', 7), (4, 'D', 'z', 8) AS s(id, data, cat, extra)",
            "MERGE WITH SCHEMA EVOLUTION INTO {t} t USING {v} s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.data = s.data",
            "Cannot change column type: id: long -> int",
        ),
        (
            "int_data",
            "SELECT CAST(2 AS BIGINT) AS id, 5 AS data, 6 AS cat",
            EVOLVING_MERGE,
            "Cannot change column type: data: string -> int",
        ),
    ]
    for name, source, merge, message in cases:
        table = _create(spark, name, accept_any=False)
        spark.sql(NAMED_SEED.format(t=table))
        view = _merge_source(spark, f"v_{name}", source)
        _assert_illegal_argument(
            lambda sql=merge, t=table, v=view: spark.sql(sql.format(t=t, v=v)), message
        )
        assert _schema(spark, table) == BASE_SCHEMA
        assert _rows(spark, table) == [[1, "a", "x"]]


def test_merge_schema_evolution_near_misses_still_succeed(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-008"""
    table = _create(spark, "bigint_src", accept_any=False)
    spark.sql(NAMED_SEED.format(t=table))
    view = _merge_source(
        spark,
        "v_bigint",
        "SELECT CAST(2 AS BIGINT) AS id, 'B' AS data, 'y' AS cat, 7 AS extra "
        "UNION ALL SELECT CAST(4 AS BIGINT), 'D', 'z', 8",
    )
    spark.sql(EVOLVING_MERGE.format(t=table, v=view))
    assert _schema(spark, table) == [*BASE_SCHEMA, ("extra", "int32")]
    assert _rows(spark, table) == [[1, "a", "x", None], [2, "B", "y", 7], [4, "D", "z", 8]]

    widened = f"{NS}.int_target"
    spark.sql(f"CREATE TABLE {widened} (id INT, data STRING, cat STRING) USING iceberg")
    spark.sql(NAMED_SEED.format(t=widened))
    view = _merge_source(
        spark,
        "v_widen",
        "SELECT CAST(2 AS BIGINT) AS id, 'B' AS data, 'y' AS cat "
        "UNION ALL SELECT CAST(4 AS BIGINT), 'D', 'z'",
    )
    spark.sql(EVOLVING_MERGE.format(t=widened, v=view))
    assert _schema(spark, widened) == BASE_SCHEMA
    assert _rows(spark, widened) == [[1, "a", "x"], [2, "B", "y"], [4, "D", "z"]]

    delete_only = _create(spark, "delete_only", accept_any=False)
    spark.sql(NAMED_SEED.format(t=delete_only))
    view = _merge_source(
        spark,
        "v_delete",
        "SELECT * FROM VALUES (2, 'B', 'y', 7), (4, 'D', 'z', 8) AS s(id, data, cat, extra)",
    )
    spark.sql(
        f"MERGE WITH SCHEMA EVOLUTION INTO {delete_only} t USING {view} s ON t.id = s.id "
        "WHEN MATCHED THEN DELETE"
    )
    assert _schema(spark, delete_only) == BASE_SCHEMA
    assert _rows(spark, delete_only) == [[1, "a", "x"]]

    plain_merge = _create(spark, "plain_merge", accept_any=False)
    spark.sql(NAMED_SEED.format(t=plain_merge))
    view = _merge_source(
        spark,
        "v_plain",
        "SELECT * FROM VALUES (2, 'B', 'y', 7), (4, 'D', 'z', 8) AS s(id, data, cat, extra)",
    )
    spark.sql(EVOLVING_MERGE.replace(" WITH SCHEMA EVOLUTION", "").format(t=plain_merge, v=view))
    assert _schema(spark, plain_merge) == BASE_SCHEMA
    assert _rows(spark, plain_merge) == [[1, "a", "x"], [2, "B", "y"], [4, "D", "z"]]


def _with_merge_schema(spark: ReparkSession, statements: list[str]) -> None:
    """Run ``statements`` with the merge-schema conf set, then unset it."""
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        for statement in statements:
            spark.sql(statement)
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)


def test_merge_schema_conf_adds_a_new_column_in_the_source_spelling(
    spark: ReparkSession,
) -> None:
    """pins: u6-write-refusals/C-010"""
    spark.sql("CREATE OR REPLACE TEMP VIEW vnc AS SELECT 9 AS id, 'z' AS newc")
    new_c = [(9, None, None, "z")]
    cases = [
        ("pos", "INSERT INTO {t} SELECT 9 AS id, 'z' AS NewC", ("NewC", "string"), new_c),
        (
            "by_name",
            "INSERT INTO {t} BY NAME SELECT 9 AS id, 'z' AS data, 1 AS NewC",
            ("NewC", "int32"),
            [(9, "z", None, 1)],
        ),
        (
            "by_name_case",
            "INSERT INTO {t} BY NAME SELECT 9 AS ID, 'q' AS Cat, 1 AS NewC",
            ("NewC", "int32"),
            [(9, None, "q", 1)],
        ),
        ("quoted", "INSERT INTO {t} SELECT 9 AS id, 'z' AS `NewC`", ("NewC", "string"), new_c),
        (
            "union",
            "INSERT INTO {t} SELECT 9 AS id, 'z' AS NewC UNION ALL SELECT 8, 'y'",
            ("NewC", "string"),
            [(8, None, None, "y"), *new_c],
        ),
        ("view_ref", "INSERT INTO {t} SELECT id, NEWC FROM vnc", ("NEWC", "string"), new_c),
        (
            "overwrite",
            "INSERT OVERWRITE {t} SELECT 9 AS id, 'z' AS NewC",
            ("NewC", "string"),
            new_c,
        ),
        (
            "overwrite_by_name",
            "INSERT OVERWRITE {t} BY NAME SELECT 9 AS id, 'z' AS NewC",
            ("NewC", "string"),
            new_c,
        ),
    ]
    for name, statement, added, rows in cases:
        table = _create(spark, f"spelling_{name}", accept_any=True)
        spark.sql(NAMED_SEED.format(t=table))
        _with_merge_schema(spark, [statement.format(t=table)])
        assert _schema(spark, table) == [*BASE_SCHEMA, added], name
        expected = [list(row) for row in rows]
        if not name.startswith("overwrite"):
            expected = sorted([[1, "a", "x", None], *expected], key=repr)
        assert _rows(spark, table) == expected, name
    matched = _create(spark, "spelling_matched", accept_any=True)
    _with_merge_schema(spark, [f"INSERT INTO {matched} SELECT 9 AS ID, 'z' AS Data"])
    assert _schema(spark, matched) == BASE_SCHEMA
    assert _rows(spark, matched) == [[9, "z", None]]


def test_dataframe_writes_add_a_new_column_in_the_frame_spelling(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-010"""
    frame = spark.createDataFrame(
        [(9, "z", "q", 1)], "id BIGINT, data STRING, cat STRING, NewC INT"
    )
    write_to = _create(spark, "df_write_to", accept_any=True)
    frame.writeTo(write_to).option("mergeSchema", "true").append()
    save_as = _create(spark, "df_save_as", accept_any=True)
    frame.write.format("iceberg").option("mergeSchema", "true").mode("append").saveAsTable(save_as)
    insert_into = _create(spark, "df_insert_into", accept_any=True)
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        frame.write.insertInto(insert_into)
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)
    for table in (write_to, save_as, insert_into):
        assert _schema(spark, table) == [*BASE_SCHEMA, ("NewC", "int32")], table
        assert _rows(spark, table) == [[9, "z", "q", 1]], table
    upper = _create(spark, "df_upper", accept_any=True)
    spark.createDataFrame([(9, "z")], "ID BIGINT, Data STRING").writeTo(upper).option(
        "mergeSchema", "true"
    ).append()
    assert _schema(spark, upper) == BASE_SCHEMA
    assert _rows(spark, upper) == [[9, "z", None]]


def test_overwrite_by_name_with_an_extra_column_follows_the_conf(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-011"""
    statement = "INSERT OVERWRITE {t} BY NAME SELECT 9 AS id, 'z' AS data, 'q' AS cat, 1 AS extra"
    refused = _create(spark, "overwrite_extra", accept_any=True)
    spark.sql(NAMED_SEED.format(t=refused))
    _assert_illegal_argument(
        lambda: spark.sql(statement.format(t=refused)), "Field extra not found in source schema"
    )
    assert _schema(spark, refused) == BASE_SCHEMA
    assert _rows(spark, refused) == [[1, "a", "x"]]
    evolved = _create(spark, "overwrite_extra_conf", accept_any=True)
    spark.sql(NAMED_SEED.format(t=evolved))
    _with_merge_schema(spark, [statement.format(t=evolved)])
    assert _schema(spark, evolved) == [*BASE_SCHEMA, ("extra", "int32")]
    assert _rows(spark, evolved) == [[9, "z", "q", 1]]


def test_an_unknown_upper_case_alias_keeps_its_spelling(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-012"""
    table = _create(spark, "upper_extra", accept_any=True)
    spark.sql(NAMED_SEED.format(t=table))
    for verb in ("INSERT INTO", "INSERT OVERWRITE"):
        for by_name in ("BY NAME ", ""):
            statement = (
                f"{verb} {table} {by_name}SELECT 9 AS id, 'z' AS data, 'q' AS cat, 1 AS EXTRA"
            )
            _assert_illegal_argument(
                lambda sql=statement: spark.sql(sql), "Field EXTRA not found in source schema"
            )
    assert _rows(spark, table) == [[1, "a", "x"]]


def test_repeated_source_names_refuse_like_iceberg(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-013"""
    spark.sql(HSRC)
    for merge_schema in (True, False):
        table = _create(spark, f"dup_{merge_schema}", accept_any=True)
        spark.sql(NAMED_SEED.format(t=table))
        if merge_schema:
            spark.conf.set(MERGE_SCHEMA_CONF, "true")
        try:
            for statement, core in (
                (
                    "INSERT INTO {t} BY NAME SELECT 9 AS id, 1 AS id",
                    "Invalid schema: multiple fields for name id: 0 and 1",
                ),
                (
                    "INSERT OVERWRITE {t} SELECT 9 AS id, 1 AS id",
                    "Invalid schema: multiple fields for name id: 0 and 1",
                ),
                (
                    "INSERT INTO {t} SELECT 9 AS id, 'z' AS data, 'q' AS cat, 1 AS data",
                    "Invalid schema: multiple fields for name data: 1 and 3",
                ),
                (
                    "INSERT INTO {t} SELECT *, * FROM hsrc",
                    "Invalid schema: multiple fields for name id: 0 and 2",
                ),
                (
                    "INSERT INTO {t} BY NAME SELECT *, * FROM hsrc",
                    "Invalid schema: multiple fields for name id: 0 and 2",
                ),
                (
                    "INSERT OVERWRITE {t} SELECT *, * FROM hsrc",
                    "Invalid schema: multiple fields for name id: 0 and 2",
                ),
            ):
                with pytest.raises(PySparkException) as caught:
                    spark.sql(statement.format(t=table))
                assert str(caught.value).endswith(core)
            for statement, message in (
                (
                    "INSERT INTO {t} BY NAME SELECT 9 AS id, 1 AS ID",
                    "Multiple entries with same key: 1=ID and 1=id",
                ),
                (
                    "INSERT INTO {t} BY NAME SELECT 9 AS id, 'a' AS data, 'b' AS DATA",
                    "Multiple entries with same key: 2=DATA and 2=data",
                ),
                (
                    "INSERT INTO {t} BY NAME SELECT 9 AS id, 1 AS n, 2 AS N",
                    "Cannot build lower case index: n and N collide"
                    if merge_schema
                    else "Field n not found in source schema",
                ),
            ):
                _assert_illegal_argument(
                    lambda sql=statement, t=table: spark.sql(sql.format(t=t)), message
                )
        finally:
            spark.conf.unset(MERGE_SCHEMA_CONF)
        assert _schema(spark, table) == BASE_SCHEMA
        assert _rows(spark, table) == [[1, "a", "x"]]


def test_replace_into_an_accept_any_table_refuses_at_the_parser(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-014"""
    table = _create(spark, "replace_into", accept_any=True)
    for merge_schema in (False, True):
        if merge_schema:
            spark.conf.set(MERGE_SCHEMA_CONF, "true")
        try:
            with pytest.raises(ParseException):
                spark.sql(f"REPLACE INTO {table} VALUES (9, 'z', 'q')")
        finally:
            spark.conf.unset(MERGE_SCHEMA_CONF)
    assert _rows(spark, table) == []


def test_an_added_column_is_named_per_item_like_spark(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-015"""
    spark.sql(HSRC)
    cases = [
        (
            "star_alias",
            "INSERT INTO {t} SELECT *, 'z' AS NewC FROM hsrc",
            [("NewC", "string")],
            [[1, "a", None, "z"]],
        ),
        (
            "fn_alias",
            "INSERT INTO {t} SELECT id, upper(data), 'z' AS NewC FROM hsrc",
            [("upper(data)", "string"), ("NewC", "string")],
            [[1, None, None, "A", "z"]],
        ),
        (
            "by_name_fn",
            "INSERT INTO {t} BY NAME SELECT id, upper(data) FROM hsrc",
            [("upper(data)", "string")],
            [[1, None, None, "A"]],
        ),
        (
            "derived_star",
            "INSERT INTO {t} SELECT * FROM (SELECT 9 AS id, 'z' AS NewC)",
            [("NewC", "string")],
            [[9, None, None, "z"]],
        ),
        (
            "cte_columns",
            "INSERT INTO {t} WITH c(id, NewC) AS (SELECT 9, 'z') SELECT * FROM c",
            [("NewC", "string")],
            [[9, None, None, "z"]],
        ),
        (
            "derived_columns",
            "INSERT INTO {t} SELECT * FROM (SELECT 9, 'z') AS v(id, NewC)",
            [("NewC", "string")],
            [[9, None, None, "z"]],
        ),
        (
            "values_columns",
            "INSERT INTO {t} SELECT * FROM VALUES (9, 'z') AS v(id, NewC)",
            [("NewC", "string")],
            [[9, None, None, "z"]],
        ),
        (
            "cte_reference_columns",
            "INSERT INTO {t} WITH c AS (SELECT 9 AS id, 'z' AS NewC) "
            "SELECT * FROM c AS x(id, Other)",
            [("Other", "string")],
            [[9, None, None, "z"]],
        ),
    ]
    for name, statement, added, rows in cases:
        table = _create(spark, f"per_item_{name}", accept_any=True)
        _with_merge_schema(spark, [statement.format(t=table)])
        assert _schema(spark, table) == [*BASE_SCHEMA, *added], name
        assert _rows(spark, table) == rows, name


def test_the_refusal_names_an_unaliased_expression_like_spark(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-016"""
    spark.sql(HSRC)
    table = _create(spark, "expr_names", accept_any=True)
    for source, name in (
        ("SELECT id, upper(data) FROM hsrc", "upper(data)"),
        ("SELECT -1, 'z'", "-1"),
        ("VALUES (9, 'z') UNION ALL SELECT 8, 'y'", "col1"),
        ("SELECT id, id + 1 FROM hsrc", "(id + 1)"),
        ("SELECT id, id <> 1 FROM hsrc", "(NOT (id = 1))"),
        ("SELECT id, data || 'x' FROM hsrc", "concat(data, x)"),
        ("WITH c(id, NewC) AS (SELECT 9, 'z') SELECT * FROM c", "NewC"),
        ("SELECT * FROM (SELECT 9, 'z') AS v(id, NewC)", "NewC"),
        ("SELECT * FROM VALUES (9, 'z') AS v(id, NewC)", "NewC"),
        ("WITH c AS (SELECT 9 AS id, 'z' AS NewC) SELECT * FROM c AS x(id, Other)", "Other"),
    ):
        _assert_illegal_argument(
            lambda sql=f"INSERT INTO {table} {source}": spark.sql(sql),
            f"Field {name} not found in source schema",
        )
    assert _rows(spark, table) == []
    assert _schema(spark, table) == BASE_SCHEMA


def test_an_underivable_name_refuses_before_any_evolution(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-017"""
    spark.sql(HSRC)
    table = _create(spark, "underived", accept_any=True)
    for statement in (
        "INSERT INTO {t} SELECT id, CASE WHEN id > 0 THEN 1 END FROM hsrc",
        "INSERT INTO {t} SELECT id, ceil(1.2) FROM hsrc",
        "INSERT INTO {t} SELECT id, 9L FROM hsrc",
        "INSERT INTO {t} SELECT id, 1.5D FROM hsrc",
        "INSERT INTO {t} SELECT id, 2BD FROM hsrc",
    ):
        with pytest.raises(UnsupportedOperationException, match="as Spark names it") as caught:
            _with_merge_schema(spark, [statement.format(t=table)])
        assert "__repark" not in str(caught.value)
    assert _rows(spark, table) == []
    assert _schema(spark, table) == BASE_SCHEMA


def test_an_empty_overwrite_wipes_the_table(spark: ReparkSession) -> None:
    """pins: u6-write-refusals/C-018"""
    wiped = _create(spark, "empty_overwrite", accept_any=True)
    spark.sql(NAMED_SEED.format(t=wiped))
    spark.sql(f"INSERT OVERWRITE {wiped} SELECT 9 AS id WHERE false")
    assert _rows(spark, wiped) == []
    assert _schema(spark, wiped) == BASE_SCHEMA
    evolved = _create(spark, "empty_overwrite_conf", accept_any=True)
    spark.sql(NAMED_SEED.format(t=evolved))
    _with_merge_schema(
        spark, [f"INSERT OVERWRITE {evolved} SELECT 9 AS id, 'z' AS NewC WHERE false"]
    )
    assert _rows(spark, evolved) == []
    assert _schema(spark, evolved) == [*BASE_SCHEMA, ("NewC", "string")]
    for name, statement in (
        ("missing", "INSERT OVERWRITE {t} BY NAME SELECT 9 AS id WHERE false"),
        (
            "null",
            "INSERT OVERWRITE {t} BY NAME SELECT 9 AS id, NULL AS data, 'c' AS cat WHERE false",
        ),
    ):
        plain = _create(spark, f"empty_overwrite_plain_{name}", accept_any=False)
        spark.sql(NAMED_SEED.format(t=plain))
        spark.sql(statement.format(t=plain))
        assert _rows(spark, plain) == [], name
        assert _schema(spark, plain) == BASE_SCHEMA, name
