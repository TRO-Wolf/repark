from __future__ import annotations

from pathlib import Path
from typing import Any

from repark import ReparkSession

_FOLD6_VALUES = "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b)"
_FOLD6_VALUES_PART = "SELECT * FROM (VALUES (1, '2020-01-01 10:00:00', 'p1')) AS v(a, b, p)"

_FOLD6_ARMS = [
    "SELECT id, coalesce(localtimestamp, localtimestamp) FROM sc.ns.nsrc",
    "SELECT id, coalesce(localtimestamp, localtimestamp) FROM sc.ns.nsrc WHERE now IS NOT NULL",
    "SELECT id, CASE WHEN id > 0 THEN localtimestamp END FROM sc.ns.nsrc",
    "SELECT id, x FROM (SELECT id, coalesce(localtimestamp, localtimestamp) AS x "
    "FROM sc.ns.nsrc) q",
    "SELECT id, (SELECT max(localtimestamp) FROM sc.ns.nsrc) FROM sc.ns.tsrc",
    "SELECT id, coalesce(LOCALTIMESTAMP, LOCALTIMESTAMP) FROM sc.ns.nsrc",
]

_FOLD6_CONTROLS = [
    "SELECT id, localtimestamp FROM sc.ns.nsrc",
    "SELECT id, coalesce(`localtimestamp`, `localtimestamp`) FROM sc.ns.nsrc",
    "SELECT id, coalesce(n.localtimestamp, n.localtimestamp) FROM sc.ns.nsrc n",
]


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("store-ts-doors-2-fold6")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.tsrc (id INT, c STRING, t TIMESTAMP) USING iceberg").collect()
    session.sql(
        "INSERT INTO sc.ns.tsrc VALUES (2, '2020-07-07 07:07:07', TIMESTAMP'2020-08-08 08:08:08')"
    ).collect()
    session.sql(
        "CREATE TABLE sc.ns.nsrc (id INT, now STRING, localtimestamp STRING, "
        "current_timezone STRING, window STRING) USING iceberg"
    ).collect()
    session.sql(
        "INSERT INTO sc.ns.nsrc VALUES (2, '2020-07-07 07:07:07', "
        "'2020-06-06 06:06:06', '2020-05-05 05:05:05', '2020-04-04 04:04:04')"
    ).collect()
    return session


def _norm(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _write(session: ReparkSession, sql: str) -> dict[str, Any]:
    try:
        session.sql(sql).collect()
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


def _target(session: ReparkSession, table: str, partitioned: bool = False) -> None:
    if partitioned:
        session.sql(
            f"CREATE TABLE {table} (id INT, c TIMESTAMP, p STRING) USING iceberg PARTITIONED BY (p)"
        ).collect()
    else:
        session.sql(f"CREATE TABLE {table} (id INT, c TIMESTAMP) USING iceberg").collect()


def _door_sql(table: str, door: str, arm: str) -> str:
    if door == "dynpart":
        widened = arm.replace(" FROM ", ", 'p1' AS p FROM ", 1)
        return f"INSERT INTO {table} {_FOLD6_VALUES_PART} UNION ALL {widened}"
    if door == "collist":
        return f"INSERT INTO {table} (id, c) {_FOLD6_VALUES} UNION ALL {arm}"
    return f"INSERT INTO {table} {_FOLD6_VALUES} UNION ALL {arm}"


def _rows(session: ReparkSession, table: str) -> list[list[Any]]:
    return sorted(
        [
            _norm(list(row))
            for row in session.sql(f"SELECT id, CAST(c AS STRING) AS c FROM {table}").collect()
        ],
        key=repr,
    )


def _refused_cast(session: ReparkSession, sql: str) -> None:
    got = _write(session, sql)
    assert got["refused"] is True, sql
    assert got["condition"] == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST", sql
    assert got["sql_state"] == "KD000", sql
    assert 'Cannot safely cast `c` "STRING" to "TIMESTAMP"' in got["message"], sql


def test_fold6_nullary_columns_refuse_on_all_doors(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        index = 0
        for door in ("into", "dynpart", "collist"):
            for arm in _FOLD6_ARMS:
                table = f"sc.ns.f6_g{index}"
                index += 1
                _target(session, table, partitioned=door == "dynpart")
                sql = _door_sql(table, door, arm)
                _refused_cast(session, sql)
                assert _rows(session, table) == [], sql
    finally:
        session.stop()


def test_fold6_nullary_columns_refuse_case_sensitive(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.sql("SET spark.sql.caseSensitive=true").collect()
        for index, arm in enumerate(_FOLD6_ARMS):
            table = f"sc.ns.f6_c{index}"
            _target(session, table)
            sql = _door_sql(table, "into", arm)
            if arm == _FOLD6_ARMS[5]:
                got = _write(session, sql)
                assert got["refused"] is True, sql
                assert got["condition"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION", got
                assert got["sql_state"] == "42703", got
            else:
                _refused_cast(session, sql)
            assert _rows(session, table) == [], sql
    finally:
        session.stop()


def test_fold6_nullary_controls_keep_refusing(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        index = 0
        for mode in ("ci", "cs"):
            if mode == "cs":
                session.sql("SET spark.sql.caseSensitive=true").collect()
            for arm in _FOLD6_CONTROLS:
                table = f"sc.ns.f6_k{index}"
                index += 1
                _target(session, table)
                sql = _door_sql(table, "into", arm)
                _refused_cast(session, sql)
                assert _rows(session, table) == [], sql
    finally:
        session.stop()


def test_fold6_timestamp_coalesce_controls_still_store(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        arms = [
            (
                "SELECT id, coalesce(CAST(now AS TIMESTAMP), CAST(now AS TIMESTAMP)) "
                "FROM sc.ns.nsrc",
                [[1, "2020-01-01 10:00:00"], [2, "2020-07-07 07:07:07"]],
            ),
            (
                "SELECT id, coalesce(t, t) FROM (SELECT id, t, count(*) OVER (ORDER BY t "
                "RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW) AS n "
                "FROM sc.ns.tsrc) q",
                [[1, "2020-01-01 10:00:00"], [2, "2020-08-08 08:08:08"]],
            ),
        ]
        for index, (arm, want) in enumerate(arms):
            table = f"sc.ns.f6_s{index}"
            _target(session, table)
            sql = _door_sql(table, "into", arm)
            got = _write(session, sql)
            assert got["refused"] is False, (sql, got)
            assert _rows(session, table) == want, sql
    finally:
        session.stop()
