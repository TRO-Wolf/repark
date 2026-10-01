from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("store_ts_doors_2_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("store-ts-doors-2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )
    for statement in _ORACLE["setup"]:
        session.sql(statement).collect()
    return session


def _norm(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _write(session: ReparkSession, cell: dict[str, Any]) -> dict[str, Any]:
    try:
        session.sql(cell["sql"]).collect()
    except Exception as error:
        condition = getattr(error, "getCondition", None)
        sql_state = getattr(error, "getSqlState", None)
        return {
            "refused": True,
            "condition": condition() if callable(condition) else None,
            "sql_state": sql_state() if callable(sql_state) else None,
            "message": str(error).splitlines()[0],
        }
    return {"refused": False}


def _mismatch(session: ReparkSession, key: str) -> str | None:
    cell = _CELLS[key]
    got = _write(session, cell)
    rows = sorted(
        [_norm(list(row)) for row in session.sql(cell["readback"]).collect()],
        key=repr,
    )
    if got["refused"] != cell["refused"]:
        return f"{key}: {got} but Spark refused={cell['refused']}"
    if cell["refused"] and (
        got["condition"] != cell["condition"]
        or got["sql_state"] != cell["sql_state"]
        or not got["message"].endswith(cell["message"])
    ):
        return f"{key}: {got} != {cell['message']}"
    if rows != cell["rows"]:
        return f"{key}: rows {rows} != {cell['rows']}"
    return None


def test_store_assignment_cells_replay_spark(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        misses = [miss for key in _CELLS if (miss := _mismatch(session, key)) is not None]
    finally:
        session.stop()
    assert misses == []


def test_view_over_values_keeps_base_refusal(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.sql(
            "CREATE OR REPLACE TEMP VIEW doors2_vv AS "
            "SELECT * FROM (VALUES (41, TIMESTAMP'2020-01-01 10:00:00')) AS v(a, b)"
        ).collect()
        with pytest.raises(Exception, match="cannot store-assign column `c`") as excinfo:
            session.sql("INSERT INTO sc.ns.d2_bigint SELECT * FROM doors2_vv").collect()
        assert "repark_insert_store_assignment" in str(excinfo.value)
        rows = sorted(
            [
                _norm(list(row))
                for row in session.sql(
                    "SELECT id, CAST(c AS STRING) AS c, typeof(c) AS t "
                    "FROM sc.ns.d2_bigint WHERE id IN (0, 41)"
                ).collect()
            ],
            key=repr,
        )
        assert rows == [[0, "7", "bigint"]]
    finally:
        session.stop()


def test_string_source_still_stores(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.sql(
            "INSERT INTO sc.ns.d2_bigint SELECT * FROM (VALUES (42, '1')) AS v(a, b)"
        ).collect()
        rows = sorted(
            [
                _norm(list(row))
                for row in session.sql(
                    "SELECT id, CAST(c AS STRING) AS c, typeof(c) AS t "
                    "FROM sc.ns.d2_bigint WHERE id IN (0, 42)"
                ).collect()
            ],
            key=repr,
        )
        assert rows == [[0, "7", "bigint"], [42, "1", "bigint"]]
    finally:
        session.stop()


def _fold_rows(session: ReparkSession, table: str, low: int) -> list[list[Any]]:
    return sorted(
        [
            _norm(list(row))
            for row in session.sql(
                "SELECT id, CAST(c AS STRING) AS c, typeof(c) AS t "
                f"FROM {table} WHERE id >= {low} AND id < {low + 10}"
            ).collect()
        ],
        key=repr,
    )


def test_fold_string_beside_bigint_or_bool_keeps_string_refusal(tmp_path: Path) -> None:
    cases = [
        (
            "INSERT INTO sc.ns.t_ts SELECT * FROM (VALUES (8221, 'abc')) AS v(a, b) "
            "UNION ALL SELECT * FROM (VALUES (8222, 8L)) AS w(a, b)",
            8221,
        ),
        (
            "INSERT INTO sc.ns.t_ts SELECT * FROM "
            "(VALUES (8231, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT * FROM (VALUES (8232, 8L)) AS w(a, b)",
            8231,
        ),
        (
            "INSERT INTO sc.ns.t_ts SELECT * FROM (VALUES (8241, 'abc')) AS v(a, b) "
            "UNION ALL SELECT * FROM (VALUES (8242, TRUE)) AS w(a, b)",
            8241,
        ),
        (
            "INSERT INTO sc.ns.t_ts SELECT * FROM "
            "(VALUES (8251, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT * FROM (VALUES (8252, TRUE)) AS w(a, b)",
            8251,
        ),
    ]
    session = _open(tmp_path)
    try:
        for sql, low in cases:
            got = _write(session, {"sql": sql})
            assert got["refused"] is True, sql
            assert got["condition"] == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST", sql
            assert got["sql_state"] == "KD000", sql
            assert 'Cannot safely cast `c` "STRING" to "TIMESTAMP"' in got["message"], sql
            assert _fold_rows(session, "sc.ns.t_ts", low) == [], sql
    finally:
        session.stop()


def test_fold_setop_datetime_clash_into_bigint_refuses(tmp_path: Path) -> None:
    cases = [
        (
            "INSERT INTO sc.ns.t_bigint SELECT * FROM (VALUES (8261, 7)) AS v(a, b) "
            "EXCEPT SELECT * FROM (VALUES (8262, TIMESTAMP'2020-01-01 10:00:00')) AS w(a, b)",
            8261,
        ),
        (
            "INSERT INTO sc.ns.t_bigint SELECT * FROM (VALUES (8271, 7)) AS v(a, b) "
            "INTERSECT SELECT * FROM (VALUES (8272, TIMESTAMP'2020-01-01 10:00:00')) AS w(a, b)",
            8271,
        ),
        (
            "INSERT INTO sc.ns.t_bigint SELECT * FROM "
            "(VALUES (8291, TIMESTAMP'2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT 8292, 7L",
            8291,
        ),
    ]
    session = _open(tmp_path)
    try:
        for sql, low in cases:
            got = _write(session, {"sql": sql})
            assert got["refused"] is True, sql
            assert got["condition"] == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST", sql
            assert got["sql_state"] == "KD000", sql
            assert 'Cannot safely cast `c` "TIMESTAMP" to "BIGINT"' in got["message"], sql
            assert _fold_rows(session, "sc.ns.t_bigint", low) == [], sql
    finally:
        session.stop()


def _fold_parses_as_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False
    return True


def test_fold_values_current_timestamp_sibling_stores(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.t_ts SELECT * FROM "
                "(VALUES (8571, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL "
                "SELECT * FROM (VALUES (8572, current_timestamp())) AS w(a, b)"
            },
        )
        assert got["refused"] is False, got
        rows = _fold_rows(session, "sc.ns.t_ts", 8571)
        assert rows[0] == [8571, "2020-01-01 10:00:00", "timestamp"], rows
        assert len(rows) == 2, rows
        assert rows[1][0] == 8572, rows
        assert rows[1][2] == "timestamp", rows
        assert _fold_parses_as_timestamp(rows[1][1]), rows
    finally:
        session.stop()


def test_fold_current_timestamp_in_from_sibling_stores(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.t_ts SELECT * FROM "
                "(VALUES (8581, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL "
                "SELECT id + 8581, current_timestamp() FROM sc.ns.src"
            },
        )
        assert got["refused"] is False, got
        rows = _fold_rows(session, "sc.ns.t_ts", 8581)
        assert rows[0] == [8581, "2020-01-01 10:00:00", "timestamp"], rows
        assert len(rows) == 3, rows
        for row in rows[1:]:
            assert row[2] == "timestamp", rows
            assert _fold_parses_as_timestamp(row[1]), rows
        assert sorted(row[0] for row in rows[1:]) == [8582, 8583], rows
    finally:
        session.stop()


def test_fold_date_sibling_time_string_stores_with_midnight_divergence(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path)
    try:
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.t_ts SELECT * FROM "
                "(VALUES (8591, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL "
                "SELECT id + 8591, date(tsc) FROM sc.ns.src"
            },
        )
        assert got["refused"] is False, got
        assert _fold_rows(session, "sc.ns.t_ts", 8591) == [
            [8591, "2020-01-01 10:00:00", "timestamp"],
            [8592, "2020-01-01 00:00:00", "timestamp"],
            [8593, "2021-06-15 00:00:00", "timestamp"],
        ]
    finally:
        session.stop()


def test_fold_ambiguous_sibling_raises_ambiguous_reference(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns2").collect()
        session.sql(
            "CREATE TABLE sc.ns2.doors2_src2 (id INT, tsc TIMESTAMP) USING iceberg"
        ).collect()
        session.sql(
            "INSERT INTO sc.ns2.doors2_src2 VALUES (1, TIMESTAMP'2020-08-08 08:08:08')"
        ).collect()
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.t_ts SELECT * FROM "
                "(VALUES (8601, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL "
                "SELECT a.id + 8601, tsc FROM sc.ns.src a "
                "JOIN sc.ns2.doors2_src2 b ON a.id = b.id"
            },
        )
        assert got["refused"] is True, got
        assert got["condition"] == "AMBIGUOUS_REFERENCE", got
        assert got["sql_state"] == "42704", got
        assert "Reference `tsc` is ambiguous" in got["message"], got
        assert _fold_rows(session, "sc.ns.t_ts", 8601) == []
    finally:
        session.stop()


def test_fold_string_beside_timestamp_into_bigint_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        sql = (
            "INSERT INTO sc.ns.t_bigint SELECT * FROM (VALUES (8281, '2020-01-01 10:00:00')) "
            "AS v(a, b) UNION ALL SELECT id + 8281, tsc FROM sc.ns.src"
        )
        try:
            session.sql(sql).collect()
            stored = True
        except Exception as error:
            stored = False
            condition = getattr(error, "getCondition", None)
            assert (condition() if callable(condition) else None) is None
            assert str(error).splitlines()[0] == "type_coercion"
            assert "Incompatible inputs for Union" in str(error)
        assert stored is False
        assert _fold_rows(session, "sc.ns.t_bigint", 8281) == []
    finally:
        session.stop()


def _fold3_setup(session: ReparkSession) -> None:
    session.sql("CREATE TABLE sc.ns.strtab (id INT, c STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.strtab VALUES (2, '2020-07-07 07:07:07')").collect()
    session.sql("CREATE TABLE sc.ns.bigtab (id INT, c BIGINT) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.bigtab VALUES (2, 7L)").collect()
    session.sql("CREATE TABLE sc.ns.ambtab (id INT, ambiguous BIGINT) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.ambtab VALUES (2, 7L)").collect()
    session.sql("CREATE TABLE sc.ns.f3_xt (id INT, c TIMESTAMP) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.f3_xt VALUES (2, TIMESTAMP'2020-08-08 08:08:08')").collect()
    session.sql("CREATE TEMPORARY VIEW xv AS SELECT id, tsc AS c FROM sc.ns.src").collect()


def _fold3_target(session: ReparkSession, table: str) -> None:
    session.sql(f"CREATE TABLE {table} (id INT, c TIMESTAMP) USING iceberg").collect()


def _fold3_rows(session: ReparkSession, table: str) -> list[list[Any]]:
    return sorted(
        [
            _norm(list(row))
            for row in session.sql(f"SELECT id, CAST(c AS STRING) AS c FROM {table}").collect()
        ],
        key=repr,
    )


def _fold3_refused_cast(session: ReparkSession, sql: str) -> dict[str, Any]:
    got = _write(session, {"sql": sql})
    assert got["refused"] is True, sql
    assert got["condition"] == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST", sql
    assert got["sql_state"] == "KD000", sql
    assert 'Cannot safely cast `c` "STRING" to "TIMESTAMP"' in got["message"], sql
    return got


def test_fold3_cte_view_shadow_inline_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g1")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g1 WITH xv AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT * FROM xv",
        )
        assert _fold3_rows(session, "sc.ns.f3_g1") == []
    finally:
        session.stop()


def test_fold3_cte_view_shadow_arm_with_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g2")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g2 SELECT * FROM "
            "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL "
            "(WITH xv AS (SELECT * FROM sc.ns.strtab) SELECT * FROM xv)",
        )
        assert _fold3_rows(session, "sc.ns.f3_g2") == []
    finally:
        session.stop()


def test_fold3_cte_view_shadow_dynpart_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        session.sql(
            "CREATE TABLE sc.ns.f3_pt (id INT, c TIMESTAMP, p STRING) "
            "USING iceberg PARTITIONED BY (p)"
        ).collect()
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_pt WITH xv AS "
            "(SELECT id, c, 'p1' AS p FROM (SELECT * FROM sc.ns.strtab)) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00', 'p1')) AS v(a, b, p) "
            "UNION ALL SELECT * FROM xv",
        )
        assert _fold3_rows(session, "sc.ns.f3_pt") == []
    finally:
        session.stop()


def test_fold3_cte_view_shadow_cols_expr_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g3")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g3 WITH xv AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT id, coalesce(c, c) FROM xv",
        )
        assert _fold3_rows(session, "sc.ns.f3_g3") == []
    finally:
        session.stop()


def test_fold3_cte_view_shadow_join_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g4")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g4 WITH xv AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT x.id, x.c FROM xv x "
            "JOIN sc.ns.bigtab y ON x.id = y.id",
        )
        assert _fold3_rows(session, "sc.ns.f3_g4") == []
    finally:
        session.stop()


def test_fold3_collist_cte_shadow_carried_precedence_card(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g5")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_g5 (id, c) WITH xv AS "
                "(SELECT * FROM sc.ns.strtab) SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT * FROM xv"
            },
        )
        assert got["refused"] is False, got
        assert _fold3_rows(session, "sc.ns.f3_g5") == [
            [1, "2020-01-01 10:00:00"],
            [1, "2020-01-01 10:00:00"],
            [2, "2021-06-15 12:30:00"],
        ]
    finally:
        session.stop()


def test_fold3_nested_with_shadow_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g6")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g6 WITH o AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL (WITH xv AS (SELECT * FROM o) SELECT * FROM xv)",
        )
        assert _fold3_rows(session, "sc.ns.f3_g6") == []
    finally:
        session.stop()


def test_fold3_table_shadow_star_and_cols_refuse(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        session.sql(
            "CREATE TABLE spark_catalog.default.pv (id INT, c TIMESTAMP) USING parquet"
        ).collect()
        session.sql(
            "INSERT INTO spark_catalog.default.pv VALUES (3, TIMESTAMP'2020-08-08 08:08:08')"
        ).collect()
        _fold3_target(session, "sc.ns.f3_g7")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g7 WITH pv AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT * FROM pv",
        )
        assert _fold3_rows(session, "sc.ns.f3_g7") == []
        _fold3_target(session, "sc.ns.f3_g8")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g8 WITH pv AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT id, c FROM pv",
        )
        assert _fold3_rows(session, "sc.ns.f3_g8") == []
    finally:
        session.stop()


def test_fold3_qualified_star_view_shadow_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_g9")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_g9 WITH xv AS (SELECT * FROM sc.ns.strtab) "
            "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT x.* FROM xv x",
        )
        assert _fold3_rows(session, "sc.ns.f3_g9") == []
    finally:
        session.stop()


def test_fold3_timestamp_cte_and_view_siblings_still_store(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_s1")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_s1 WITH xv AS "
                "(SELECT * FROM sc.ns.f3_xt) SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT * FROM xv"
            },
        )
        assert got["refused"] is False, got
        assert _fold3_rows(session, "sc.ns.f3_s1") == [
            [1, "2020-01-01 10:00:00"],
            [2, "2020-08-08 08:08:08"],
        ]
        _fold3_target(session, "sc.ns.f3_s2")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_s2 SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT * FROM xv"
            },
        )
        assert got["refused"] is False, got
        assert _fold3_rows(session, "sc.ns.f3_s2") == [
            [1, "2020-01-01 10:00:00"],
            [1, "2020-01-01 10:00:00"],
            [2, "2021-06-15 12:30:00"],
        ]
    finally:
        session.stop()


def test_fold3_cte_named_ambiguous_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_a1")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_a1 WITH ambiguous AS "
            "(SELECT * FROM sc.ns.strtab) SELECT * FROM "
            "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT * FROM ambiguous",
        )
        assert _fold3_rows(session, "sc.ns.f3_a1") == []
    finally:
        session.stop()


def test_fold3_view_named_ambiguous_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        session.sql("CREATE TEMPORARY VIEW ambiguous AS SELECT * FROM sc.ns.strtab").collect()
        _fold3_target(session, "sc.ns.f3_a2")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_a2 SELECT * FROM "
            "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT * FROM ambiguous",
        )
        assert _fold3_rows(session, "sc.ns.f3_a2") == []
    finally:
        session.stop()


def test_fold3_column_named_ambiguous_refuses(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_a3")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_a3 SELECT * FROM "
            "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
            "UNION ALL SELECT id, ambiguous FROM sc.ns.ambtab",
        )
        assert _fold3_rows(session, "sc.ns.f3_a3") == []
    finally:
        session.stop()


def test_fold3_missing_ambiguous_surfaces_not_found(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_a4")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_a4 SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT * FROM ambiguous"
            },
        )
        assert got["refused"] is True, got
        assert "not found" in got["message"].lower(), got
        assert _fold3_rows(session, "sc.ns.f3_a4") == []
    finally:
        session.stop()


def test_fold3_unresolved_column_surfaces(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_u1")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_u1 SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT id, tsc + nosuch FROM sc.ns.src"
            },
        )
        assert got["refused"] is True, got
        assert "UNRESOLVED_COLUMN" in got["message"] or "nosuch" in got["message"], got
        assert _fold3_rows(session, "sc.ns.f3_u1") == []
    finally:
        session.stop()


def test_fold3_unresolved_routine_surfaces(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_u2")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_u2 SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT id, nosuchfn(tsc) FROM sc.ns.src"
            },
        )
        assert got["refused"] is True, got
        assert "UNRESOLVED_ROUTINE" in got["message"] or "nosuchfn" in got["message"], got
        assert _fold3_rows(session, "sc.ns.f3_u2") == []
    finally:
        session.stop()


def test_fold3_missing_table_surfaces(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_u3")
        got = _write(
            session,
            {
                "sql": "INSERT INTO sc.ns.f3_u3 SELECT * FROM "
                "(VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) "
                "UNION ALL SELECT * FROM sc.ns.nosuchtable"
            },
        )
        assert got["refused"] is True, got
        assert "TABLE_OR_VIEW_NOT_FOUND" in got["message"] or "nosuchtable" in got["message"], got
        assert _fold3_rows(session, "sc.ns.f3_u3") == []
    finally:
        session.stop()


def test_fold3_quoted_case_siblings_keep_refusal(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        _fold3_setup(session)
        _fold3_target(session, "sc.ns.f3_q1")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_q1 SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) "
            'AS v(a, b) UNION ALL SELECT id, "S" FROM sc.ns.src',
        )
        assert _fold3_rows(session, "sc.ns.f3_q1") == []
        _fold3_target(session, "sc.ns.f3_q2")
        _fold3_refused_cast(
            session,
            "INSERT INTO sc.ns.f3_q2 SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) "
            'AS v(a, b) UNION ALL SELECT id, "TSC" FROM sc.ns.src',
        )
        assert _fold3_rows(session, "sc.ns.f3_q2") == []
    finally:
        session.stop()
