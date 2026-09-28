"""ICE-CASE-TWIN-1 — the RP-56 verifier fold pins against the p9/p9b Spark oracle.

pins: rp-56/C-002, C-003

Spark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 answers recorded 2026-09-28
by ``cs_probe9.py`` (54 cells, twin table ``(id, ID, s<x,X,z>, b, ts)`` plus a
branch) and ``cs_probe9b.py`` (quoted twin-star shapes). The twin-star refusal
belongs to the scan of a catalog table only: derived tables, CTEs, joins and
DataFrame temp views answer, as do version/snapshot/timestamp time-travel
reads; branch-name reads refuse. Under ``caseSensitive=true`` nested DROP,
ADD, DROP IF EXISTS and ``rewrite_data_files(where)`` answer.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

_CATALOG = "twf"
_CASE_SENSITIVE_KEY = "spark.sql.caseSensitive"
_TWIN_DDL = "(id INT, ID INT, s STRUCT<x: INT, X: INT, z: INT>, b INT)"
_TWIN_ROW = "(1, 2, named_struct('x', 10, 'X', 20, 'z', 30), 5)"
_REFUSAL = (
    "[COLUMN_ALREADY_EXISTS] The column `id` already exists. "
    "Choose another name or rename the existing column. SQLSTATE: 42711"
)
_COLLISION = "Cannot build lower case index: id and ID collide"


@pytest.fixture
def twin_session(tmp_path: Path) -> Iterator[ReparkSession]:
    """A fresh session with an empty memory-catalog namespace."""
    spark = ReparkSession.builder.appName("pytest-ice-case-twin-1").getOrCreate()
    spark.register_memory_catalog(_CATALOG, str(tmp_path / "wh"))
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns").collect()
    yield spark
    spark.stop()


def _describe_map(spark: ReparkSession, table: str) -> dict[str, str]:
    """DESCRIBE TABLE as col_name -> data_type."""
    described = spark.sql(f"DESCRIBE TABLE {table}").to_arrow().to_pylist()
    return {str(row["col_name"]): str(row["data_type"]) for row in described}


def _make_twin(spark: ReparkSession, table: str, ddl: str = _TWIN_DDL) -> None:
    """Create a one-row twin table under ``caseSensitive=true``."""
    spark.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        spark.sql(f"CREATE TABLE {_CATALOG}.ns.{table} {ddl} USING iceberg").collect()
        spark.sql(f"INSERT INTO {_CATALOG}.ns.{table} VALUES {_TWIN_ROW}").collect()
    finally:
        spark.conf.set(_CASE_SENSITIVE_KEY, "false")


def test_derived_cte_and_join_twin_stars_answer(
    twin_session: ReparkSession,
) -> None:
    """VR-3: twin-named derived, CTE and joined stars answer like Spark (p9b)."""
    for sql, rows in [
        ("SELECT * FROM (SELECT 1 AS a, 2 AS `A`) s", [[1, 2]]),
        ("SELECT s.* FROM (SELECT 1 AS a, 2 AS `A`) s", [[1, 2]]),
        ("WITH c AS (SELECT 1 AS a, 2 AS `A`) SELECT * FROM c", [[1, 2]]),
        (
            "SELECT * FROM (SELECT 1 AS a) x JOIN (SELECT 2 AS `A`) y ON x.a = y.A",
            [],
        ),
    ]:
        table = twin_session.sql(sql).to_arrow()
        assert table.column_names == ["a", "A"]
        assert sorted(([*row.values()] for row in table.to_pylist()), key=repr) == rows


def test_nested_drop_if_exists_drops_and_add_answers_under_true(
    twin_session: ReparkSession,
) -> None:
    """VR-1/VR-4: nested DROP IF EXISTS drops, ADD answers (p9 t-cells)."""
    _make_twin(twin_session, "tw9")
    twin_session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        twin_session.sql(f"ALTER TABLE {_CATALOG}.ns.tw9 DROP COLUMN IF EXISTS s.x").collect()
        assert _describe_map(twin_session, f"{_CATALOG}.ns.tw9")["s"] == "struct<X:int,z:int>"
        twin_session.sql(f"ALTER TABLE {_CATALOG}.ns.tw9 ADD COLUMN s.y INT").collect()
        assert _describe_map(twin_session, f"{_CATALOG}.ns.tw9")["s"] == "struct<X:int,z:int,y:int>"
    finally:
        twin_session.conf.set(_CASE_SENSITIVE_KEY, "false")
    with pytest.raises(PySparkException, match=_COLLISION):
        twin_session.sql(f"ALTER TABLE {_CATALOG}.ns.tw9 DROP COLUMN IF EXISTS s.z").collect()


def test_branch_read_refuses_and_time_travel_answers(
    twin_session: ReparkSession,
) -> None:
    """VR-2: branch reads refuse 42711; pinned reads answer (p9 f_branch_table)."""
    _make_twin(twin_session, "twbr")
    twin_session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        twin_session.sql(f"ALTER TABLE {_CATALOG}.ns.twbr CREATE BRANCH br").collect()
        answered = twin_session.sql(f"SELECT * FROM {_CATALOG}.ns.twbr.branch_br").to_arrow()
        assert answered.column_names == ["id", "ID", "s", "b"]
    finally:
        twin_session.conf.set(_CASE_SENSITIVE_KEY, "false")
    with pytest.raises(AnalysisException) as caught:
        twin_session.sql(f"SELECT * FROM {_CATALOG}.ns.twbr.branch_br").collect()
    assert _REFUSAL in str(caught.value)
    traveled = twin_session.sql(
        f"SELECT * FROM {_CATALOG}.ns.twbr TIMESTAMP AS OF '2999-01-01 00:00:00'"
    ).to_arrow()
    assert traveled.column_names == ["id", "ID", "s", "b"]
    assert sorted(([*row.values()] for row in traveled.to_pylist()), key=repr)[0][:2] == [1, 2]


def test_group_by_window_refuses_on_twin_table(twin_session: ReparkSession) -> None:
    """VR-5: GROUP BY window over a twin table refuses 42711 (p9 f_window)."""
    twin_session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        twin_session.sql(
            f"CREATE TABLE {_CATALOG}.ns.tw9w (id INT, ID INT, b INT, ts TIMESTAMP) USING iceberg"
        ).collect()
        twin_session.sql(
            f"INSERT INTO {_CATALOG}.ns.tw9w VALUES (1, 2, 5, TIMESTAMP '2024-01-01 00:05:00')"
        ).collect()
    finally:
        twin_session.conf.set(_CASE_SENSITIVE_KEY, "false")
    with pytest.raises(AnalysisException) as caught:
        twin_session.sql(
            f"SELECT window(ts, '10 minutes') AS w, count(*) AS n FROM {_CATALOG}.ns.tw9w "
            "GROUP BY window(ts, '10 minutes')"
        ).collect()
    assert _REFUSAL in str(caught.value)


def test_exists_star_refuses_on_twin_table(twin_session: ReparkSession) -> None:
    """VR-5: EXISTS (SELECT * FROM twin) refuses 42711 (p9 f_exists)."""
    _make_twin(twin_session, "tw9e")
    twin_session.sql(f"CREATE TABLE {_CATALOG}.ns.plain9e (p INT) USING iceberg").collect()
    twin_session.sql(f"INSERT INTO {_CATALOG}.ns.plain9e VALUES (1)").collect()
    with pytest.raises(AnalysisException) as caught:
        twin_session.sql(
            f"SELECT * FROM {_CATALOG}.ns.plain9e WHERE EXISTS (SELECT * FROM {_CATALOG}.ns.tw9e)"
        ).collect()
    assert _REFUSAL in str(caught.value)


def test_rewrite_where_refuses_under_false_and_answers_under_true(
    twin_session: ReparkSession,
) -> None:
    """VR-4: rewrite where refuses loud under false, answers under true (p9)."""
    twin_session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        twin_session.sql(
            f"CREATE TABLE {_CATALOG}.ns.tw9r (id INT, ID INT, b INT) USING iceberg"
        ).collect()
        twin_session.sql(f"INSERT INTO {_CATALOG}.ns.tw9r VALUES (1, 2, 5)").collect()
    finally:
        twin_session.conf.set(_CASE_SENSITIVE_KEY, "false")
    with pytest.raises(AnalysisException, match=_COLLISION):
        twin_session.sql(
            f"CALL {_CATALOG}.system.rewrite_data_files(table => 'ns.tw9r', where => 'b > 0')"
        ).collect()
    twin_session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        rewritten = twin_session.sql(
            f"CALL {_CATALOG}.system.rewrite_data_files(table => 'ns.tw9r', where => 'b > 0')"
        ).to_arrow()
        assert rewritten.column_names == [
            "rewritten_data_files_count",
            "added_data_files_count",
            "rewritten_bytes_count",
            "failed_data_files_count",
            "removed_delete_files_count",
        ]
    finally:
        twin_session.conf.set(_CASE_SENSITIVE_KEY, "false")
