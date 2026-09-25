"""U8 WRITE-SQL PR1 and PR2 — REPLACE WHERE, INSERT PARTITION, bucketed INSERT, nested SET.

Oracle: live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, measured on 2026-09-24
(``pr1/RW-*``, ``pr1/PC-*``, ``pr1/BK-*``, ``pr1/RW2-*``, ``pr1/PC2-*``..``pr1/PC4-*`` in
``u8_write_sql_spark_oracle.json``) plus the scoreboard record of the cells
``W-INSERT-OVERWRITE-WHERE``, ``W-INSERT-PARTITION-CLAUSE`` and ``W-INSERT-BUCKETED``.

Every expected value below is Spark's recorded answer, not a RePark derivation. Where the
exception class or the rendering differs, the pin says so and the ledger names the residue.

Every measurement is committed in ``u8_write_sql_spark_oracle.json`` beside this file (its
``provenance`` block names the probes; keys ``pr1/…`` hold the 2026-09-24 probes cited below).
The rounds-2 and -3 pins (C-015..C-017, C-019..C-021) replay that file case by case:
``r1/…`` (NULL ``cat`` / ``id`` keys, INT keys, repeated source names), ``r2/…`` and
``r2b/…`` (the critic's r2 probes) and ``r2fix/…`` (BIGINT and fractional literals,
``<=>``, the conjunct-split refusal texts, repeated-name arity texts, the WAP branch). Round 4
(C-022..C-024) adds the critic's ``r3/…`` .. ``r3e/…`` and ``r3fix/…``; round 5 (C-019) adds
``r4/…`` (fractional comparisons rendered in refusal texts); a case whose answer is a named
residue (R-2, R-8, R-11, R-12) is held to RePark's recorded answer.

PR2 (C-025..C-031, 2026-09-25) pins nested struct-field assignment in UPDATE and MERGE. The
cells ``W-UPDATE-NESTED-FIELD`` and ``W-MERGE-NESTED`` have their own tests. The 93 ``pr2/…``
measurements in ``u8_write_sql_nested_spark_oracle.json`` replay case by case, and a residue
key (R-13..R-17) is held to RePark's recorded answer.

pins: u8-write-sql/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
C-011, C-012, C-014, C-015, C-016, C-017, C-019, C-020, C-021, C-022, C-023, C-024, C-025,
C-026, C-027, C-028, C-029, C-030, C-031
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkException,
)

NS = "mem.ns"
SEED = "INSERT INTO {t} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"
SEEDED = [[1, "a", "x"], [2, "b", "y"], [3, "c", "x"]]
REPLACED = [[2, "b", "y"], [9, "z", "x"]]
ADDED = [[1, "a", "x"], [2, "b", "y"], [3, "c", "x"], [9, "z", "q"]]
SUMMARY_KEYS = (
    "operation",
    "added-records",
    "deleted-records",
    "total-records",
    "added-data-files",
    "deleted-data-files",
    "changed-partition-count",
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog and one namespace."""
    session = ReparkSession.builder.appName("pytest-ice-write-sql-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _create(
    spark: ReparkSession,
    name: str,
    partitioned_by: str = "PARTITIONED BY (cat)",
    *,
    seeded: bool = True,
    columns: str = "id BIGINT, data STRING, cat STRING",
) -> str:
    """Create ``mem.ns.<name>`` (format version 2) and seed Spark's three rows."""
    table = f"{NS}.{name}"
    spark.sql(
        f"CREATE TABLE {table} ({columns}) USING iceberg {partitioned_by} "
        "TBLPROPERTIES ('format-version' = '2')"
    )
    if seeded:
        spark.sql(SEED.format(t=table))
    return table


def _rows(spark: ReparkSession, table: str) -> list[list[Any]]:
    """Return every row as a list, sorted by its repr."""
    arrow = spark.sql(f"SELECT * FROM {table}").to_arrow()
    names = arrow.schema.names
    return sorted(([row[name] for name in names] for row in arrow.to_pylist()), key=repr)


def _summaries(spark: ReparkSession, table: str) -> list[dict[str, str]]:
    """Return each snapshot's operation and measured summary counts, oldest first."""
    rows = (
        spark.sql(f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at")
        .to_arrow()
        .to_pylist()
    )
    out = []
    for row in rows:
        summary = dict(row["summary"])
        summary["operation"] = row["operation"]
        out.append({key: summary[key] for key in SUMMARY_KEYS if key in summary})
    return out


def _files(spark: ReparkSession, table: str) -> list[tuple[str, int]]:
    """Return each live data file's partition and record count, sorted."""
    arrow = spark.sql(f"SELECT partition, record_count FROM {table}.files").to_arrow()
    return sorted((repr(row["partition"]), row["record_count"]) for row in arrow.to_pylist())


def _raises(
    action: Callable[[], object],
    kind: type[Exception],
    message: str,
    condition: str | None,
    state: str | None,
) -> None:
    """Run ``action`` and pin its exact class, message, condition and SQLSTATE."""
    with pytest.raises(kind) as caught:
        action()
    assert type(caught.value) is kind
    assert str(caught.value) == message
    assert caught.value.getCondition() == condition
    assert caught.value.getSqlState() == state


def test_replace_where_replaces_the_matching_partition(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-001"""
    table = _create(spark, "rw")
    spark.sql(f"INSERT INTO {table} REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'")
    assert _rows(spark, table) == REPLACED
    assert _summaries(spark, table)[1] == {
        "operation": "overwrite",
        "added-records": "1",
        "deleted-records": "2",
        "total-records": "2",
        "added-data-files": "1",
        "deleted-data-files": "1",
        "changed-partition-count": "1",
    }
    assert _files(spark, table) == [("{'cat': 'x'}", 1), ("{'cat': 'y'}", 1)]


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO TABLE {t} REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
        "insert into {t} replace where cat = 'x' select 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE /* c */ WHERE cat = 'x' SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE cat = 'x' VALUES (9, 'z', 'x')",
        "INSERT INTO {t} REPLACE WHERE cat = 'x' (SELECT 9, 'z', 'x')",
        "INSERT INTO {t} REPLACE WHERE (cat = 'x') SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE cat = 'x' "
        "WITH s AS (SELECT 9 AS a, 'z' AS b, 'x' AS c) SELECT * FROM s",
        "INSERT INTO {t} REPLACE WHERE cat = 'x' SELECT CAST(9 AS INT), 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE cat != 'y' SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE NOT cat = 'y' SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE cat NOT IN ('y') SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE cat LIKE 'x%' SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE cat <=> 'x' SELECT 9, 'z', 'x'",
        "INSERT INTO {t} REPLACE WHERE 'x' = cat SELECT 9, 'z', 'x'",
    ],
)
def test_replace_where_accepts_the_spellings_spark_accepts(
    spark: ReparkSession, statement: str
) -> None:
    """pins: u8-write-sql/C-002, C-014"""
    table = _create(spark, "spell")
    spark.sql(statement.format(t=table))
    assert _rows(spark, table) == REPLACED


def test_replace_where_reads_its_source_and_writes_it_unchecked(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-002"""
    table = _create(spark, "self")
    spark.sql(
        f"INSERT INTO {table} REPLACE WHERE cat = 'x' "
        f"SELECT id + 10, data, cat FROM {table} WHERE cat = 'x'"
    )
    assert _rows(spark, table) == [[11, "a", "x"], [13, "c", "x"], [2, "b", "y"]]
    outside = _create(spark, "outside")
    spark.sql(f"INSERT INTO {outside} REPLACE WHERE cat = 'x' SELECT 9, 'z', 'y'")
    assert _rows(spark, outside) == [[2, "b", "y"], [9, "z", "y"]]
    wide = _create(spark, "wide")
    spark.sql(f"INSERT INTO {wide} REPLACE WHERE cat = 'x' VALUES (9, 'z', 'x'), (10, 'w', 'x')")
    assert _rows(spark, wide) == [[10, "w", "x"], [2, "b", "y"], [9, "z", "x"]]
    for index, predicate in enumerate(
        ("cat IS NOT NULL", "id BETWEEN 1 AND 3", "cat >= 'x'", "cat IN ('x', 'y')")
    ):
        table = _create(spark, f"all_{index}")
        spark.sql(f"INSERT INTO {table} REPLACE WHERE {predicate} SELECT 9, 'z', 'x'")
        assert _rows(spark, table) == [[9, "z", "x"]], predicate


@pytest.mark.parametrize(
    ("partitioned_by", "seeded", "statement", "summary"),
    [
        (
            "PARTITIONED BY (cat)",
            True,
            "REPLACE WHERE cat = 'nope' SELECT 9, 'z', 'nope'",
            {"operation": "overwrite", "added-records": "1", "total-records": "4"},
        ),
        (
            "PARTITIONED BY (cat)",
            True,
            "REPLACE WHERE cat IS NULL SELECT 9, 'z', 'x'",
            {"operation": "overwrite", "added-records": "1", "total-records": "4"},
        ),
        (
            "PARTITIONED BY (cat)",
            True,
            "REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x' WHERE false",
            {"operation": "delete", "deleted-records": "2", "total-records": "1"},
        ),
        (
            "PARTITIONED BY (cat)",
            True,
            "REPLACE WHERE cat = 'nope' SELECT 9, 'z', 'x' WHERE false",
            {"operation": "delete", "total-records": "3", "changed-partition-count": "0"},
        ),
        (
            "PARTITIONED BY (cat)",
            True,
            "REPLACE WHERE true SELECT 9, 'z', 'x'",
            {"operation": "overwrite", "added-records": "1", "deleted-records": "3"},
        ),
        (
            "PARTITIONED BY (cat)",
            True,
            "REPLACE WHERE false SELECT 9, 'z', 'x'",
            {"operation": "append", "added-records": "1", "total-records": "4"},
        ),
        (
            "",
            True,
            "REPLACE WHERE id >= 1 SELECT 9, 'z', 'x'",
            {"operation": "overwrite", "added-records": "1", "deleted-records": "3"},
        ),
        (
            "PARTITIONED BY (cat)",
            False,
            "REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            {"operation": "overwrite", "added-records": "1", "total-records": "1"},
        ),
        (
            "PARTITIONED BY (cat)",
            False,
            "REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x' WHERE false",
            {"operation": "delete", "total-records": "0"},
        ),
    ],
)
def test_replace_where_snapshot_follows_what_the_commit_changes(
    spark: ReparkSession,
    partitioned_by: str,
    seeded: bool,
    statement: str,
    summary: dict[str, str],
) -> None:
    """pins: u8-write-sql/C-003"""
    table = _create(spark, "ops", partitioned_by, seeded=seeded)
    spark.sql(f"INSERT INTO {table} {statement}")
    latest = _summaries(spark, table)[-1]
    assert {key: latest.get(key) for key in summary} == summary
    assert len(_summaries(spark, table)) == (2 if seeded else 1)


@pytest.mark.parametrize(
    ("statement", "kind", "message", "condition", "state"),
    [
        (
            "INSERT INTO {t} REPLACE WHERE UPPER(cat) = 'X' SELECT 9, 'z', 'x'",
            IllegalArgumentException,
            "Cannot convert Spark predicate to Iceberg expression: UPPER(cat) = 'X'",
            None,
            None,
        ),
        (
            "INSERT INTO {t} REPLACE WHERE cat = NULL SELECT 9, 'z', 'x'",
            IllegalArgumentException,
            "Cannot convert Spark predicate to Iceberg expression: null",
            None,
            None,
        ),
        (
            "INSERT INTO {t} REPLACE WHERE id IN (SELECT 1) SELECT 9, 'z', 'x'",
            AnalysisException,
            "Error during planning: [UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY] The feature is "
            "not supported: INSERT OVERWRITE with a subquery condition. SQLSTATE: 0A000",
            "UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY",
            "0A000",
        ),
        (
            "INSERT INTO {t} REPLACE WHERE rand() < 2 SELECT 9, 'z', 'x'",
            AnalysisException,
            "Error during planning: [INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects "
            'a deterministic expression, but the actual expression is "(rand() < 2)". '
            "SQLSTATE: 42K0E",
            "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
            "42K0E",
        ),
        (
            "INSERT INTO {t} REPLACE WHERE cat = 'x' SELECT 9, 'z'",
            AnalysisException,
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot "
            "write to `mem`.`ns`.`refuse`, the reason is not enough data columns:\n"
            "Table columns: `id`, `data`, `cat`.\nData columns: `9`, `z`. SQLSTATE: 21S01",
            "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
            "21S01",
        ),
        (
            "INSERT INTO {t}_missing REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            AnalysisException,
            "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
            "`mem`.`ns`.`refuse_missing` cannot be found. Verify the spelling and correctness of "
            "the schema and catalog. If you did not qualify the name with a schema, verify the "
            "current_schema() output, or qualify the name with the correct schema and catalog. "
            "To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. "
            "SQLSTATE: 42P01",
            "TABLE_OR_VIEW_NOT_FOUND",
            "42P01",
        ),
        (
            "INSERT OVERWRITE {t} REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'REPLACE'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
        ),
        (
            "INSERT INTO {t} (id, data, cat) REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'REPLACE'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
        ),
        (
            "INSERT INTO {t} BY NAME REPLACE WHERE cat = 'x' SELECT 9 AS id",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'REPLACE'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
        ),
        (
            "INSERT INTO {t} PARTITION (cat = 'x') REPLACE WHERE cat = 'x' SELECT 9, 'z'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'REPLACE'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
        ),
        (
            "INSERT INTO {t} REPLACE WHERE cat = 'x' BY NAME SELECT 9 AS id",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'BY'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
        ),
    ],
)
def test_replace_where_refusals_match_spark_and_commit_nothing(
    spark: ReparkSession,
    statement: str,
    kind: type[Exception],
    message: str,
    condition: str | None,
    state: str | None,
) -> None:
    """pins: u8-write-sql/C-004"""
    table = _create(spark, "refuse")
    _raises(lambda: spark.sql(statement.format(t=table)), kind, message, condition, state)
    assert _rows(spark, table) == SEEDED
    assert len(_summaries(spark, table)) == 1


def test_replace_where_that_splits_a_file_refuses_like_iceberg(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-004

    Spark raises ``Py4JJavaError`` wrapping Iceberg's ``ValidationException`` and renders the
    filter as ``ref(name="id") == 2``; the class and the filter spelling are residue R-1.
    """
    for partitioned_by, predicate, rendered in (
        ("PARTITIONED BY (cat)", "id = 2", "id = 2"),
        ("PARTITIONED BY (cat)", "id = '2'", "id = 2"),
        ("", "cat = 'x'", 'cat = "x"'),
        ("PARTITIONED BY (bucket(4, id))", "id = 2", "id = 2"),
    ):
        table = _create(spark, f"split_{len(partitioned_by)}_{len(predicate)}", partitioned_by)
        with pytest.raises(PySparkException) as caught:
            spark.sql(f"INSERT INTO {table} REPLACE WHERE {predicate} SELECT 9, 'z', 'y'")
        assert f"Cannot delete file where some, but not all, rows match filter {rendered}: " in str(
            caught.value
        )
        assert _rows(spark, table) == SEEDED
        assert len(_summaries(spark, table)) == 1


def test_replace_where_on_a_branch_leaves_main_alone(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-005"""
    table = _create(spark, "branch")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")
    spark.sql(f"INSERT INTO {table}.branch_b1 REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'")
    assert _rows(spark, table) == SEEDED
    assert _rows(spark, f"{table}.branch_b1") == REPLACED


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO {t} PARTITION (cat = 'q') SELECT 9, 'z'",
        "INSERT INTO {t} PARTITION (cat = 'q') VALUES (9, 'z')",
        "INSERT INTO TABLE {t} PARTITION (cat = 'q') SELECT 9, 'z'",
        "INSERT INTO {t} PARTITION (CAT = 'q') SELECT 9, 'z'",
        "INSERT INTO {t} PARTITION (cat) SELECT 9, 'z', 'q'",
        "INSERT INTO {t} PARTITION (cat) VALUES (9, 'z', 'q')",
        "INSERT INTO {t} PARTITION (cat = 'q') (id, data) SELECT 9, 'z'",
        "INSERT INTO {t} PARTITION (cat = 'q') (data, id) SELECT 'z', 9",
        "INSERT INTO {t} PARTITION (cat = 'q') BY NAME SELECT 9 AS id, 'z' AS data",
    ],
)
def test_a_static_partition_value_fills_the_missing_column(
    spark: ReparkSession, statement: str
) -> None:
    """pins: u8-write-sql/C-006"""
    table = _create(spark, "static")
    spark.sql(statement.format(t=table))
    assert _rows(spark, table) == ADDED
    assert _summaries(spark, table)[1] == {
        "operation": "append",
        "added-records": "1",
        "total-records": "4",
        "added-data-files": "1",
        "changed-partition-count": "1",
    }


def test_two_key_specs_take_static_and_dynamic_values_in_any_order(
    spark: ReparkSession,
) -> None:
    """pins: u8-write-sql/C-006"""
    for index, clause in enumerate(
        (
            "(cat = 'q', data = 'w') SELECT 9",
            "(data = 'w', cat = 'q') SELECT 9",
            "(cat = 'q', data) SELECT 9, 'w'",
            "(cat, data = 'w') SELECT 9, 'q'",
            "(data = 'w', cat) SELECT 9, 'q'",
        )
    ):
        table = _create(spark, f"two_{index}", "PARTITIONED BY (cat, data)")
        spark.sql(f"INSERT INTO {table} PARTITION {clause}")
        assert _rows(spark, table) == [
            [1, "a", "x"],
            [2, "b", "y"],
            [3, "c", "x"],
            [9, "w", "q"],
        ], clause


def test_static_values_follow_spark_string_casts(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-007"""
    by_id = _create(spark, "by_id", "PARTITIONED BY (id)")
    spark.sql(f"INSERT INTO {by_id} PARTITION (id = '7') SELECT 'z', 'q'")
    spark.sql(f"INSERT INTO {by_id} PARTITION (id = 8) SELECT 'y', 'q'")
    assert _rows(spark, by_id) == sorted([*SEEDED, [7, "z", "q"], [8, "y", "q"]], key=repr)
    by_cat = _create(spark, "by_cat")
    spark.sql(f"INSERT INTO {by_cat} PARTITION (cat = 5) SELECT 9, 'z'")
    spark.sql(f"INSERT INTO {by_cat} PARTITION (cat = true) SELECT 10, 'w'")
    spark.sql(f"INSERT INTO {by_cat} PARTITION (cat = NULL) SELECT 11, 'v'")
    assert _rows(spark, by_cat) == sorted(
        [*SEEDED, [10, "w", "true"], [11, "v", None], [9, "z", "5"]], key=repr
    )
    by_day = _create(
        spark,
        "by_day",
        "PARTITIONED BY (d)",
        seeded=False,
        columns="id BIGINT, data STRING, cat STRING, d DATE",
    )
    spark.sql(f"INSERT INTO {by_day} PARTITION (d = '2024-01-05') SELECT 9, 'z', 'q'")
    assert str(_rows(spark, by_day)[0][3]) == "2024-01-05"


def _non_partition(name: str) -> str:
    """Spark's NON_PARTITION_COLUMN text for ``name``."""
    return (
        "Error during planning: [NON_PARTITION_COLUMN] PARTITION clause cannot contain the "
        f"non-partition column: `{name}`. SQLSTATE: 42000"
    )


def _cast_invalid(value: str, target: str) -> str:
    """Spark's CAST_INVALID_INPUT text for a STRING ``value`` cast to ``target``."""
    return (
        f"[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to "
        f'"{target}" because it is malformed. Correct the value as per the syntax, or change its '
        "target type. Use `try_cast` to tolerate malformed input and return NULL instead. "
        "SQLSTATE: 22018"
    )


@pytest.mark.parametrize(
    ("partitioned_by", "statement", "kind", "message", "condition", "state"),
    [
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (data = 'w') SELECT 9, 'q'",
            AnalysisException,
            _non_partition("data"),
            "NON_PARTITION_COLUMN",
            "42000",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (nope = 'w') SELECT 9, 'z'",
            AnalysisException,
            _non_partition("nope"),
            "NON_PARTITION_COLUMN",
            "42000",
        ),
        (
            "",
            "INSERT INTO {t} PARTITION (cat = 'q') SELECT 9, 'z'",
            AnalysisException,
            _non_partition("cat"),
            "NON_PARTITION_COLUMN",
            "42000",
        ),
        (
            "PARTITIONED BY (bucket(4, id))",
            "INSERT INTO {t} PARTITION (id = 5) SELECT 'z', 'q'",
            AnalysisException,
            _non_partition("id"),
            "NON_PARTITION_COLUMN",
            "42000",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (cat = 'q', cat = 'r') SELECT 9, 'z'",
            ParseException,
            "[DUPLICATE_KEY] Found duplicate keys `cat`. SQLSTATE: 23505",
            "DUPLICATE_KEY",
            "23505",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT OVERWRITE {t} PARTITION (cat = 'q', cat = 'r') SELECT 9, 'z'",
            ParseException,
            "[DUPLICATE_KEY] Found duplicate keys `cat`. SQLSTATE: 23505",
            "DUPLICATE_KEY",
            "23505",
        ),
        (
            "PARTITIONED BY (id)",
            "INSERT INTO {t} PARTITION (id = 'abc') SELECT 'z', 'q'",
            IllegalArgumentException,
            _cast_invalid("abc", "BIGINT"),
            "CAST_INVALID_INPUT",
            "22018",
        ),
        (
            "PARTITIONED BY (id)",
            "INSERT INTO {t} PARTITION (id = 7.5) SELECT 'z', 'q'",
            IllegalArgumentException,
            _cast_invalid("7.5", "BIGINT"),
            "CAST_INVALID_INPUT",
            "22018",
        ),
        (
            "PARTITIONED BY (id)",
            "INSERT OVERWRITE {t} PARTITION (id = 'abc') SELECT 'z', 'q'",
            IllegalArgumentException,
            _cast_invalid("abc", "BIGINT"),
            "CAST_INVALID_INPUT",
            "22018",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (cat = 'q') SELECT 9",
            AnalysisException,
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot "
            "write to `mem`.`ns`.`clause`, the reason is not enough data columns:\n"
            "Table columns: `id`, `data`, `cat`.\nData columns: `9`, `cat`. SQLSTATE: 21S01",
            "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
            "21S01",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (cat = 'q') SELECT 9, 'z', 'q'",
            AnalysisException,
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot "
            "write to `mem`.`ns`.`clause`, the reason is too many data columns:\n"
            "Table columns: `id`, `data`, `cat`.\nData columns: `9`, `z`, `cat`, `q`. "
            "SQLSTATE: 21S01",
            "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
            "21S01",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (cat) SELECT 9, 'z'",
            AnalysisException,
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot "
            "write to `mem`.`ns`.`clause`, the reason is not enough data columns:\n"
            "Table columns: `id`, `data`, `cat`.\nData columns: `9`, `z`. SQLSTATE: 21S01",
            "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
            "21S01",
        ),
        (
            "PARTITIONED BY (cat)",
            "INSERT INTO {t} PARTITION (cat = 'q') (id, cat) SELECT 9, 'z'",
            AnalysisException,
            "Error during planning: [STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST] Static "
            "partition column cat is also specified in the column list. SQLSTATE: 42713",
            "STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST",
            "42713",
        ),
    ],
)
def test_partition_clause_refusals_match_spark_and_commit_nothing(
    spark: ReparkSession,
    partitioned_by: str,
    statement: str,
    kind: type[Exception],
    message: str,
    condition: str,
    state: str,
) -> None:
    """pins: u8-write-sql/C-008

    Spark raises ``NumberFormatException`` (an ``IllegalArgumentException``) for
    CAST_INVALID_INPUT; the narrower class is residue R-3.
    """
    table = _create(spark, "clause", partitioned_by)
    _raises(lambda: spark.sql(statement.format(t=table)), kind, message, condition, state)
    assert _rows(spark, table) == SEEDED
    assert len(_summaries(spark, table)) == 1


def test_partition_inserts_reach_branches_and_validate_overwrite_values(
    spark: ReparkSession,
) -> None:
    """pins: u8-write-sql/C-009"""
    table = _create(spark, "pbranch")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")
    spark.sql(f"INSERT INTO {table}.branch_b1 PARTITION (cat = 'q') SELECT 9, 'z'")
    assert _rows(spark, table) == SEEDED
    assert _rows(spark, f"{table}.branch_b1") == ADDED
    dated = _create(
        spark,
        "dated",
        "PARTITIONED BY (d)",
        seeded=False,
        columns="id INT, name STRING, d DATE",
    )
    for verb in ("INSERT OVERWRITE", "INSERT INTO"):
        _raises(
            lambda verb=verb: spark.sql(
                f"{verb} {dated} PARTITION (d = '2024-13-45') SELECT 9, 'z'"
            ),
            IllegalArgumentException,
            _cast_invalid("2024-13-45", "DATE"),
            "CAST_INVALID_INPUT",
            "22018",
        )
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (cat = 'x') SELECT 9, 'z'")
    assert _rows(spark, table) == REPLACED


def test_partition_inserts_on_accept_any_tables_resolve_by_name(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-010"""
    table = _create(spark, "anyschema")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
    for statement, message in (
        ("INSERT INTO {t} PARTITION (cat = 'x') SELECT 9, 'z'", "Field 9 not found"),
        ("INSERT INTO {t} PARTITION (cat = 'x') VALUES (9, 'z')", "Field col1 not found"),
        ("INSERT INTO {t} VALUES (9, 'z', 'q')", "Field col1 not found"),
    ):
        _raises(
            lambda sql=statement: spark.sql(sql.format(t=table)),
            IllegalArgumentException,
            f"{message} in source schema",
            None,
            None,
        )
    spark.sql(f"INSERT INTO {table} PARTITION (cat = 'x') SELECT 9 AS id, 'z' AS data")
    assert _rows(spark, table) == [*SEEDED, [9, "z", "x"]]


BUCKETED = "PARTITIONED BY (bucket(4, id))"
BUCKET_FILES = [
    ("{'id_bucket': 0}", 2),
    ("{'id_bucket': 0}", 7),
    ("{'id_bucket': 1}", 3),
    ("{'id_bucket': 2}", 3),
    ("{'id_bucket': 3}", 1),
    ("{'id_bucket': 3}", 7),
]


@pytest.mark.parametrize(
    "select",
    [
        "SELECT id, CAST(id AS STRING), 'k' FROM range(20)",
        "SELECT r.id AS id, CAST(r.id AS STRING) AS data, 'k' AS cat FROM range(20) r",
    ],
)
def test_a_bucketed_insert_from_range_matches_spark_layout(
    spark: ReparkSession, select: str
) -> None:
    """pins: u8-write-sql/C-011"""
    table = _create(spark, "bucketed", BUCKETED)
    spark.sql(f"INSERT INTO {table} {select}")
    assert _rows(spark, table) == sorted(
        SEEDED + [[index, str(index), "k"] for index in range(20)], key=repr
    )
    assert _summaries(spark, table)[1] == {
        "operation": "append",
        "added-records": "20",
        "total-records": "23",
        "added-data-files": "4",
        "changed-partition-count": "4",
    }
    assert _files(spark, table) == BUCKET_FILES


def test_bucketed_overwrites_and_other_layouts_from_range(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-011"""
    source = "SELECT id, CAST(id AS STRING), 'k' FROM range(20)"
    replaced = _create(spark, "b_over", BUCKETED)
    spark.sql(f"INSERT OVERWRITE {replaced} {source}")
    assert _summaries(spark, replaced)[1] == {
        "operation": "overwrite",
        "added-records": "20",
        "deleted-records": "3",
        "total-records": "20",
        "added-data-files": "4",
        "deleted-data-files": "2",
        "changed-partition-count": "4",
    }
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        dynamic = _create(spark, "b_dyn", BUCKETED)
        spark.sql(f"INSERT OVERWRITE {dynamic} SELECT id, CAST(id AS STRING), 'k' FROM range(2)")
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")
    assert _rows(spark, dynamic) == [[0, "0", "k"], [1, "1", "k"], [3, "c", "x"]]
    assert _files(spark, dynamic) == [("{'id_bucket': 0}", 2), ("{'id_bucket': 3}", 1)]
    for partitioned_by, files in (("", "1"), ("PARTITIONED BY (cat)", "1")):
        table = _create(spark, f"b_other_{len(partitioned_by)}", partitioned_by)
        spark.sql(f"INSERT INTO {table} {source}")
        assert _summaries(spark, table)[1]["added-data-files"] == files
        assert len(_rows(spark, table)) == 23


def test_near_misses_keep_their_answers(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-012"""
    table = _create(spark, "near")
    spark.sql(
        f"INSERT INTO {table} SELECT id + 10, data, cat FROM {table} AS replace WHERE cat = 'y'"
    )
    spark.sql(f"INSERT INTO {table} VALUES (9, 'z', 'q')")
    assert _rows(spark, table) == sorted([*SEEDED, [12, "b", "y"], [9, "z", "q"]], key=repr)
    assert [row["operation"] for row in _summaries(spark, table)] == ["append"] * 3
    spark.sql(f"INSERT OVERWRITE {table} VALUES (9, 'z', 'x')")
    assert _rows(spark, table) == [[9, "z", "x"]]


ORACLE = json.loads(
    (Path(__file__).resolve().parent / "u8_write_sql_spark_oracle.json").read_text()
)["cases"]
REPLAYED = (
    "r1/S1-",
    "r1/S2-",
    "r1/S3-",
    "r1/S4-",
    "r1/S5-",
    "r2/C-",
    "r2/I-",
    "r2/O-",
    "r2/A-",
    "r2b/O-",
    "r2fix/L-",
    "r2fix/IN-",
    "r2fix/SM-",
    "r2fix/IO-",
    "r2fix/QI-",
    "r2fix/Q-",
    "r2fix/CN-",
    "r2fix/P-",
    "r2fix/W-",
    "r2fix/A-",
    "r3/",
    "r3b/",
    "r3c/",
    "r3d/",
    "r3e/",
    "r3fix/",
    "r4/",
)
TYPES = {
    "IllegalArgumentException": IllegalArgumentException,
    "AnalysisException": AnalysisException,
    "ParseException": ParseException,
    "PySparkException": PySparkException,
}


def _replayed_keys() -> list[str]:
    """Return the oracle cases this file replays, in fixture order."""
    return [key for key in ORACLE if key.startswith(REPLAYED)]


def _cell(value: Any) -> Any:
    """Spell a DECIMAL or DATE cell as the oracle records it."""
    return str(value) if isinstance(value, (Decimal, date)) else value


def _normalized(message: str, table: str) -> str:
    """Drop the planner prefix and spell the target as the oracle does."""
    ticked = ".".join(f"`{part}`" for part in table.split("."))
    return message.removeprefix("Error during planning: ").replace(ticked, "`sc`.`ns`.`<t>`")


def _assert_refusal(key: str, error: Exception, expected: dict[str, Any], table: str) -> None:
    """Compare one RePark refusal with the answer the oracle holds it to for ``key``."""
    message = _normalized(str(error), table)
    if expected["type"] == "Py4JJavaError":
        assert type(error) is PySparkException, (key, type(error))
        assert "Cannot delete file where some, but not all, rows match filter" in message, (
            key,
            message,
        )
        return
    assert type(error) is TYPES[expected["type"]], (key, type(error))
    assert error.getCondition() == expected["condition"], key
    assert error.getSqlState() == expected["sqlstate"], key
    assert message == expected["message"], key


@pytest.mark.parametrize("key", _replayed_keys())
def test_the_replace_where_measurements_replay_as_spark_answered(
    spark: ReparkSession, key: str
) -> None:
    """pins: u8-write-sql/C-015, C-016, C-017, C-019, C-020, C-021, C-022, C-023, C-024"""
    case = ORACLE[key]
    residue = case.get("residue")
    expected = residue["repark"] if residue else {"step": case["steps"][-1], "rows": case["rows"]}
    table = f"{NS}.oracle"
    spark.sql(
        f"CREATE TABLE {table} ({case['columns']}) USING iceberg {case['partitioned_by'] or ''} "
        "TBLPROPERTIES ('format-version' = '2')"
    )
    if case["default_seed"]:
        spark.sql(SEED.format(t=table))
    *setup, last = [statement.replace("{T}", table) for statement in case["statements"]]
    for statement in setup:
        spark.sql(statement)
    if expected["step"] == "ok":
        spark.sql(last)
    else:
        with pytest.raises(PySparkException) as caught:
            spark.sql(last)
        _assert_refusal(key, caught.value, expected["step"], table)
    rows = sorted(([_cell(value) for value in row] for row in _rows(spark, table)), key=repr)
    assert rows == sorted(expected["rows"], key=repr), key
    if residue and residue["id"] == "R-2":
        assert rows == sorted(case["rows"], key=repr), key
    if "branch_rows" in case:
        assert _rows(spark, f"{table}.branch_wb") == sorted(case["branch_rows"], key=repr), key


NESTED = json.loads(
    (Path(__file__).resolve().parent / "u8_write_sql_nested_spark_oracle.json").read_text()
)["cases"]
STRUCT_COLUMNS = "id BIGINT, st STRUCT<a: INT, b: STRING>"
NESTED_WRITE = {
    "operation": "overwrite",
    "added-records": "1",
    "deleted-records": "1",
    "added-data-files": "1",
    "deleted-data-files": "1",
    "changed-partition-count": "1",
}


def _plain(value: Any) -> Any:
    """Spell a struct, map or array cell as the nested oracle records it."""
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list) and value and all(isinstance(item, tuple) for item in value):
        return {str(key): _plain(item) for key, item in value}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _nested_message(error: Exception, table: str) -> str:
    """Drop the planner prefix and spell the target as the nested oracle does."""
    ticked = ".".join(f"`{part}`" for part in table.split("."))
    last = table.rsplit(".", 1)[-1]
    message = str(error).removeprefix("Error during planning: ")
    return message.replace(ticked, "`<T>`").replace(table, "<T>").replace(f"`{last}`", "`<t>`")


def test_update_sets_one_struct_field_as_spark_does(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-025"""
    table = _create(spark, "upd_nested", "", seeded=False, columns=STRUCT_COLUMNS)
    spark.sql(f"INSERT INTO {table} VALUES (1, named_struct('a', 1, 'b', 'p'))")
    spark.sql(f"UPDATE {table} SET st.a = 99 WHERE id = 1")
    assert _rows(spark, table) == [[1, {"a": 99, "b": "p"}]]
    assert spark.table(table).schema.simpleString() == "struct<id:bigint,st:struct<a:int,b:string>>"
    assert _summaries(spark, table)[-1] == {**NESTED_WRITE, "total-records": "1"}


def test_merge_sets_one_struct_field_as_spark_does(spark: ReparkSession) -> None:
    """pins: u8-write-sql/C-026"""
    table = _create(spark, "mrg_nested", "", seeded=False, columns=STRUCT_COLUMNS)
    spark.sql(
        f"INSERT INTO {table} VALUES (1, named_struct('a', 1, 'b', 'p')), "
        "(2, named_struct('a', 2, 'b', 'q'))"
    )
    spark.sql(
        f"MERGE INTO {table} t USING (SELECT 2 AS id, 20 AS na) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.st.a = s.na"
    )
    assert _rows(spark, table) == [[1, {"a": 1, "b": "p"}], [2, {"a": 20, "b": "q"}]]
    assert spark.table(table).schema.simpleString() == "struct<id:bigint,st:struct<a:int,b:string>>"
    assert _summaries(spark, table)[-1] == {
        **NESTED_WRITE,
        "added-records": "2",
        "deleted-records": "2",
        "total-records": "2",
    }


@pytest.mark.parametrize("key", list(NESTED))
def test_the_nested_assignment_measurements_replay_as_spark_answered(
    spark: ReparkSession, key: str
) -> None:
    """pins: u8-write-sql/C-025, C-026, C-027, C-028, C-029, C-030, C-031"""
    case = NESTED[key]
    residue = case.get("residue")
    expected = residue["repark"] if residue else {"step": case["steps"][-1], "rows": case["rows"]}
    table = f"{NS}.nested"
    spark.sql(f"CREATE TABLE {table} ({case['columns']}) USING iceberg {case['props']}")
    seed = f"INSERT INTO {table} VALUES {case['seed']}"
    statements = [statement.replace("{T}", table) for statement in case["statements"]]
    last = seed if expected.get("at") == "seed" else statements[-1]
    if last != seed:
        spark.sql(seed)
    step = expected["step"]
    if step == "ok":
        spark.sql(last)
    else:
        with pytest.raises(PySparkException) as caught:
            spark.sql(last)
        assert type(caught.value).__name__ == step["type"], key
        assert caught.value.getCondition() == step["condition"], key
        assert caught.value.getSqlState() == step["sqlstate"], key
        assert _nested_message(caught.value, table) == step["message"], key
    if expected["rows"] is not None:
        rows = sorted(([_plain(value) for value in row] for row in _rows(spark, table)), key=repr)
        assert rows == sorted(expected["rows"], key=repr), key
