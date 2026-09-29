from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("cast_overflow_insert_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]
_FRAMES: dict[str, dict[str, Any]] = _ORACLE["dataframe"]
_GROUPS: tuple[str, ...] = tuple(dict.fromkeys(key.rsplit("/", 1)[0] for key in _CELLS))
_COV_READ: str = "SELECT id, v FROM sc.ns.cov ORDER BY id"
_KEEP_MARKERS: dict[str, tuple[str, ...]] = {
    "controls/str": (),
    "controls/cast-str": (),
    "controls/div0-double": ("DIVIDE_BY_ZERO",),
    "controls/sel-div0": ("DIVIDE_BY_ZERO",),
    "insel/cte": ("not implemented",),
    "insel/arith-expr": ("ARITHMETIC_OVERFLOW",),
}


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("cast-overflow-insert-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )
    for statement in _ORACLE["setup"]:
        session.sql(statement).collect()
    return session


def _reset(session: ReparkSession) -> None:
    for statement in _ORACLE["setup"]:
        session.sql(statement).collect()


def _norm(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: _norm(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _rows(frame: Any) -> list[Any]:
    return sorted([_norm(list(row)) for row in frame.collect()], key=repr)


def _cols(frame: Any) -> list[list[str]]:
    return [[field.name, field.dataType.simpleString()] for field in frame.schema.fields]


def _attempt(run: Any) -> dict[str, Any]:
    try:
        run()
    except Exception as error:
        sql_state = getattr(error, "getSqlState", None)
        return {
            "err": type(error).__name__,
            "message": str(error),
            "sql_state": sql_state() if callable(sql_state) else None,
        }
    return {"answered": True}


def _refusal(session: ReparkSession, sql: str) -> dict[str, Any]:
    return _attempt(lambda: session.sql(sql).collect())


def _frame_run(session: ReparkSession, cell: dict[str, Any]) -> dict[str, Any]:
    frame = session.sql(cell["query"])
    if "limit" in cell:
        frame = frame.limit(cell["limit"])
    if "offset" in cell:
        frame = frame.offset(cell["offset"])
    if "select" in cell:
        frame = frame.selectExpr(*cell["select"])
    if cell["mode"] == "writeto":
        return _attempt(lambda: frame.writeTo("sc.ns.cov").append())
    return _attempt(lambda: frame.write.insertInto("sc.ns.cov"))


def _read_back(session: ReparkSession, read: str | None) -> list[Any] | None:
    if read is None:
        return None
    try:
        return _rows(session.sql(read))
    except Exception:
        return None


def _check_refusal(
    key: str, cell: dict[str, Any], got: dict[str, Any], read_rows: list[Any] | None
) -> str | None:
    if "answered" in got:
        return f"{key}: answered instead of refusing"
    want_head = cell.get("repark_head", cell["message_head"])
    want_err = cell.get("repark_err", cell["err"])
    want_state = cell.get("repark_state", cell["sql_state"])
    if want_head not in got.get("message", ""):
        return f"{key}: head {got}"
    if got.get("err") != want_err:
        return f"{key}: err {got}"
    if got.get("sql_state") != want_state:
        return f"{key}: sql_state {got}"
    if "read_rows" in cell:
        want_rows = sorted(cell["read_rows"], key=repr)
        if (read_rows if read_rows is not None else []) != want_rows:
            return f"{key}: read-back {read_rows} != {want_rows}"
    return None


def _check_store(key: str, cell: dict[str, Any], got_cols: Any, got_rows: Any) -> str | None:
    want = {"cols": cell["cols"], "rows": sorted(cell["rows"], key=repr)}
    seen = {"cols": got_cols, "rows": sorted(got_rows, key=repr)}
    return None if seen == want else f"{key}: {seen} != {want}"


def _check_keep(key: str, got: dict[str, Any]) -> str | None:
    if "answered" in got:
        return f"{key}: answered instead of refusing"
    for marker in _KEEP_MARKERS[key]:
        if marker not in got.get("message", ""):
            return f"{key}: marker {marker!r} missing from {got}"
    if "CAST_OVERFLOW_IN_TABLE_INSERT" in got.get("message", ""):
        return f"{key}: overflow class leaked into {got}"
    return None


def _mismatch(session: ReparkSession, key: str) -> str | None:
    cell = _CELLS[key]
    _reset(session)
    if key in _KEEP_MARKERS:
        return _check_keep(key, _refusal(session, cell["sql"]))
    if "condition" in cell:
        got = _refusal(session, cell["sql"])
        read_rows = _read_back(session, cell.get("read"))
        return _check_refusal(key, cell, got, read_rows)
    frame = session.sql(cell["sql"])
    try:
        got_cols, got_rows = _cols(frame), _rows(frame)
    except Exception as error:
        return f"{key}: store raised {type(error).__name__}: {str(error)[:200]}"
    if cell.get("read"):
        try:
            got_rows = _rows(session.sql(cell["read"]))
            got_cols = _cols(session.sql(cell["read"]))
        except Exception as error:
            return f"{key}: read raised {type(error).__name__}: {str(error)[:200]}"
    return _check_store(key, cell, got_cols, got_rows)


def _frame_mismatch(session: ReparkSession, key: str) -> str | None:
    cell = _FRAMES[key]
    _reset(session)
    if "condition" in cell:
        got = _frame_run(session, cell)
        read_rows = _read_back(session, _COV_READ)
        return _check_refusal(key, cell, got, read_rows)
    got = _frame_run(session, cell)
    if "answered" not in got:
        return f"{key}: store raised {got}"
    want_rows = sorted(cell["rows"], key=repr)
    seen_rows = _rows(session.sql(_COV_READ))
    return None if seen_rows == want_rows else f"{key}: {seen_rows} != {want_rows}"


@pytest.mark.parametrize("group", _GROUPS)
def test_sql_doors_replay_spark(group: str, tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        keys = [key for key in _CELLS if key.rsplit("/", 1)[0] == group]
        misses = [miss for key in keys if (miss := _mismatch(session, key)) is not None]
    finally:
        session.stop()
    assert misses == []


def test_dataframe_doors_replay_spark(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        misses = [miss for key in _FRAMES if (miss := _frame_mismatch(session, key)) is not None]
    finally:
        session.stop()
    assert misses == []


_VO3_TS: str = "TIMESTAMP'2024-01-02 03:04:05'"
_VO3_OVF: str = "1e19D"
_VO3_DYNAMIC_KEY: str = "spark.sql.sources.partitionOverwriteMode"


def _open_vo3(warehouse: Path) -> ReparkSession:
    """Open a session for the VO3-1 gate-before-overflow pins."""
    return (
        ReparkSession.builder.appName("cast-overflow-insert-vo3-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _seed_vo3(session: ReparkSession, table: str, ddl: str, seed: str) -> None:
    """Create the pin table and write one seed row through it."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(f"DROP TABLE IF EXISTS {table}").collect()
    session.sql(f"CREATE TABLE {table} {ddl} USING iceberg").collect()
    session.sql(seed.replace("@T", table)).collect()


def _assert_vo3_gate_refusal(
    session: ReparkSession,
    table: str,
    run: Any,
    read: str,
    want: list[Any],
    op: str,
) -> None:
    """Assert the run refuses with the store-assignment text and writes nothing."""
    got = _attempt(run)
    assert "answered" not in got
    message = got.get("message", "")
    assert got.get("err") == "AnalysisException"
    assert f"{op} cannot store-assign column `b`" in message
    assert "not ANSI-store-assignable" in message
    assert "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST" in message
    assert "CAST_OVERFLOW" not in message
    assert _rows(session.sql(read.replace("@T", table))) == want


def test_vo3_1_byname_gate_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/gate-first/byname: the gate judges before the wrap."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        _seed_vo3(
            session, table, "(id BIGINT, b BIGINT, a INT)", "INSERT INTO @T SELECT 1, NULL, NULL"
        )
        sql = (
            f"INSERT INTO {table} BY NAME SELECT {_VO3_TS} AS b, {_VO3_OVF} AS a, "
            "CAST(1 AS BIGINT) AS id"
        )
        _assert_vo3_gate_refusal(
            session,
            table,
            lambda: session.sql(sql).collect(),
            "SELECT * FROM @T",
            [[1, None, None]],
            "append",
        )
    finally:
        session.stop()


def test_vo3_1_byname_ovf_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/ovf-first/byname: order never lets the overflow win."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        _seed_vo3(
            session, table, "(id BIGINT, a INT, b BIGINT)", "INSERT INTO @T SELECT 1, NULL, NULL"
        )
        sql = (
            f"INSERT INTO {table} BY NAME SELECT {_VO3_TS} AS b, {_VO3_OVF} AS a, "
            "CAST(1 AS BIGINT) AS id"
        )
        _assert_vo3_gate_refusal(
            session,
            table,
            lambda: session.sql(sql).collect(),
            "SELECT * FROM @T",
            [[1, None, None]],
            "append",
        )
    finally:
        session.stop()


def test_vo3_1_overwrite_gate_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/gate-first/ovw: the gate judges before the wrap."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        _seed_vo3(
            session, table, "(id BIGINT, b BIGINT, a INT)", "INSERT INTO @T SELECT 1, NULL, NULL"
        )
        sql = f"INSERT OVERWRITE {table} SELECT 1, {_VO3_TS}, {_VO3_OVF}"
        _assert_vo3_gate_refusal(
            session,
            table,
            lambda: session.sql(sql).collect(),
            "SELECT * FROM @T",
            [[1, None, None]],
            "INSERT OVERWRITE",
        )
    finally:
        session.stop()


def test_vo3_1_overwrite_ovf_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/ovf-first/ovw: order never lets the overflow win."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        _seed_vo3(
            session, table, "(id BIGINT, a INT, b BIGINT)", "INSERT INTO @T SELECT 1, NULL, NULL"
        )
        sql = f"INSERT OVERWRITE {table} SELECT 1, {_VO3_OVF}, {_VO3_TS}"
        _assert_vo3_gate_refusal(
            session,
            table,
            lambda: session.sql(sql).collect(),
            "SELECT * FROM @T",
            [[1, None, None]],
            "INSERT OVERWRITE",
        )
    finally:
        session.stop()


def test_vo3_1_overwrite_dynamic_gate_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/gate-first/ovwdyn: the gate judges before the wrap."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        ddl = "(id BIGINT, b BIGINT, a INT, p INT) PARTITIONED BY (p)"
        _seed_vo3(session, table, ddl, "INSERT INTO @T SELECT 1, NULL, NULL, 1")
        session.conf.set(_VO3_DYNAMIC_KEY, "dynamic")
        try:
            sql = f"INSERT OVERWRITE {table} SELECT 1, {_VO3_TS}, {_VO3_OVF}, 1"
            _assert_vo3_gate_refusal(
                session,
                table,
                lambda: session.sql(sql).collect(),
                "SELECT * FROM @T",
                [[1, None, None, 1]],
                "INSERT OVERWRITE",
            )
        finally:
            session.conf.set(_VO3_DYNAMIC_KEY, "static")
    finally:
        session.stop()


def test_vo3_1_overwrite_dynamic_ovf_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/ovf-first/ovwdyn: order never lets the overflow win."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        ddl = "(id BIGINT, a INT, b BIGINT, p INT) PARTITIONED BY (p)"
        _seed_vo3(session, table, ddl, "INSERT INTO @T SELECT 1, NULL, NULL, 1")
        session.conf.set(_VO3_DYNAMIC_KEY, "dynamic")
        try:
            sql = f"INSERT OVERWRITE {table} SELECT 1, {_VO3_OVF}, {_VO3_TS}, 1"
            _assert_vo3_gate_refusal(
                session,
                table,
                lambda: session.sql(sql).collect(),
                "SELECT * FROM @T",
                [[1, None, None, 1]],
                "INSERT OVERWRITE",
            )
        finally:
            session.conf.set(_VO3_DYNAMIC_KEY, "static")
    finally:
        session.stop()


def test_vo3_1_insert_into_overwrite_gate_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/gate-first/dfinsertovw: the gate judges before the wrap."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        _seed_vo3(
            session, table, "(id BIGINT, b BIGINT, a INT)", "INSERT INTO @T SELECT 1, NULL, NULL"
        )
        query = f"SELECT CAST(1 AS BIGINT) AS id, {_VO3_TS} AS b, {_VO3_OVF} AS a"
        frame = session.sql(query)
        _assert_vo3_gate_refusal(
            session,
            table,
            lambda: frame.write.insertInto(table, overwrite=True),
            "SELECT * FROM @T",
            [[1, None, None]],
            "INSERT OVERWRITE",
        )
    finally:
        session.stop()


def test_vo3_1_insert_into_overwrite_ovf_first_reports_store_refusal(tmp_path: Path) -> None:
    """VO3-1 pin c/ts2bigint/lit/ovf-first/dfinsertovw: order never lets the overflow win."""
    session = _open_vo3(tmp_path)
    try:
        table = "sc.ns.vo3"
        _seed_vo3(
            session, table, "(id BIGINT, a INT, b BIGINT)", "INSERT INTO @T SELECT 1, NULL, NULL"
        )
        query = f"SELECT CAST(1 AS BIGINT) AS id, {_VO3_OVF} AS a, {_VO3_TS} AS b"
        frame = session.sql(query)
        _assert_vo3_gate_refusal(
            session,
            table,
            lambda: frame.write.insertInto(table, overwrite=True),
            "SELECT * FROM @T",
            [[1, None, None]],
            "INSERT OVERWRITE",
        )
    finally:
        session.stop()
