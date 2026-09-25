"""WO U9-TYPES-1: TIMESTAMP_LTZ and MAP columns end to end, replayed against Spark 4.1.2.

Every step of ``u9_types_1_spark_oracle.json`` runs in order through the facade, one session per
group, and its observation must equal Spark's measured answer. A step whose RePark answer is a
dated residue carries ``residue = {id, repark}``; the replay holds RePark to that recorded answer,
so a residue that moves reds. The generator is ``target/probe-u9-types-1/build_oracle.py``.

pins: u9-types-1/C-001, C-002, C-003, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import contextlib
import datetime
import decimal
import io
import json
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

ORACLE_PATH = Path(__file__).with_name("u9_types_1_spark_oracle.json")
ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
GROUPS: list[str] = sorted({step["group"] for step in ORACLE["steps"]})


def normalize(value: Any) -> Any:
    """Return a JSON-comparable form of one collected value, as the scoreboard harness does."""
    if isinstance(value, float):
        return "NaN" if value != value else round(value, 9)
    if isinstance(value, decimal.Decimal):
        return str(value.normalize())
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "0x" + bytes(value).hex()
    if isinstance(value, dict):
        pairs = [[normalize(key), normalize(item)] for key, item in value.items()]
        return sorted(pairs, key=repr)
    if hasattr(value, "asDict") and callable(value.asDict):
        return {key: normalize(item) for key, item in value.asDict(recursive=False).items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    return value


def error_observation(error: BaseException, warehouse: Path) -> dict[str, Any]:
    """Return the class, condition, SQLSTATE and message of a refused step."""
    condition = None
    state = None
    for name in ("getCondition", "getErrorClass"):
        method = getattr(error, name, None)
        if callable(method) and condition is None:
            condition = method()
    method = getattr(error, "getSqlState", None)
    if callable(method):
        state = method()
    message = str(error).strip()
    message = re.sub(r"\n\s*(JVM stacktrace|at |\tat ).*", "", message, flags=re.S)
    message = message.replace(str(warehouse), "<wh>")
    return {"error": type(error).__name__, "cond": condition, "state": state, "msg": message}


def decode(value: Any) -> Any:
    """Decode one JSON-encoded DataFrame input value (``{"datetime": iso}`` is a datetime)."""
    if isinstance(value, dict) and set(value) == {"datetime"}:
        return datetime.datetime.fromisoformat(value["datetime"])
    if isinstance(value, dict) and set(value) == {"map"}:
        return dict(value["map"])
    if isinstance(value, list):
        return [decode(item) for item in value]
    return value


def latest_metadata(warehouse: Path, table: str) -> dict[str, Any]:
    """Read the newest metadata JSON of ``table`` under the warehouse."""
    name = table.split(".")[-1]
    files = [
        path
        for path in warehouse.rglob("*.metadata.json")
        if path.parent.name == "metadata" and path.parent.parent.name.split("-")[0] == name
    ]
    newest = max(files, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return json.loads(newest.read_text(encoding="utf-8"))


def metadata_observation(metadata: dict[str, Any]) -> dict[str, Any]:
    """Return the current schema fields and default spec fields of one metadata document."""
    schema = next(
        entry
        for entry in metadata["schemas"]
        if entry["schema-id"] == metadata["current-schema-id"]
    )
    spec = next(
        entry
        for entry in metadata["partition-specs"]
        if entry["spec-id"] == metadata["default-spec-id"]
    )
    return {"fields": schema["fields"], "spec": spec["fields"]}


def collected_rows(session: Any, sql: str, ordered: bool) -> list[Any]:
    """Collect ``sql`` and normalize its rows, sorted unless the step is ordered."""
    rows = [normalize(list(row)) for row in session.sql(sql).collect()]
    return rows if ordered else sorted(rows, key=repr)


def printed_schema(session: Any, sql: str) -> str:
    """Capture ``printSchema`` of ``sql``."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        session.sql(sql).printSchema()
    return buffer.getvalue()


def observe_query(session: Any, step: dict[str, Any]) -> Any:
    """Answer the query-shaped step kinds."""
    kind = step["do"]
    sql = step["sql"]
    if kind in ("rows", "ordered"):
        return collected_rows(session, sql, kind == "ordered")
    if kind == "cols":
        return [[field.name, field.dataType.simpleString()] for field in session.sql(sql).schema]
    if kind == "describe":
        return [[row[0], row[1]] for row in session.sql(f"DESCRIBE TABLE {sql}").collect()]
    if kind == "dtypes":
        return [list(pair) for pair in session.sql(sql).dtypes]
    if kind == "printschema":
        return printed_schema(session, sql)
    if kind == "pytypes":
        return [[type(value).__name__ for value in row] for row in session.sql(sql).collect()]
    if kind == "tzinfo":
        rows = session.sql(sql).collect()
        return [[str(getattr(value, "tzinfo", "n/a")) for value in row] for row in rows]
    raise AssertionError(f"unknown step kind {kind}")


def run_step(session: Any, step: dict[str, Any], warehouse: Path, engine: str) -> Any:
    """Run one oracle step and return its observation (an error is an observation too)."""
    try:
        kind = step["do"]
        if kind == "sql":
            session.sql(step["sql"]).collect()
            return "ok"
        if kind == "conf":
            session.conf.set(step["setting"], step["value"])
            return "ok"
        if kind == "append":
            rows = [tuple(decode(value) for value in row) for row in step["rows"]]
            session.createDataFrame(rows, step["schema"]).writeTo(step["table"]).append()
            return "ok"
        if kind == "add_uuid":
            return add_uuid_column(session, step["table"], engine)
        if kind == "md":
            return metadata_observation(latest_metadata(warehouse, step["table"]))
        return observe_query(session, step)
    except Exception as error:
        return error_observation(error, warehouse)


def add_uuid_column(session: Any, table: str, engine: str) -> str:
    """Add ``u uuid`` to ``table``: Spark through the Iceberg API, RePark through its SQL door."""
    if engine == "spark":
        jvm = session._jvm
        loaded = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            session._jsparkSession, table
        )
        uuid_type = jvm.org.apache.iceberg.types.Types.UUIDType.get()
        loaded.updateSchema().addColumn("u", uuid_type).commit()
        session.sql(f"REFRESH TABLE {table}")
    else:
        session.sql(f"ALTER TABLE {table} ADD COLUMN u UUID").collect()
    return "ok"


def open_session(warehouse: Path) -> Any:
    """Open the scoreboard-shaped facade session: UTC, v3 creates allowed, catalog ``sc``."""
    session = (
        ReparkSession.builder.appName("u9-types-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    return session


@pytest.mark.parametrize("group", GROUPS)
def test_every_step_answers_as_spark_measured_or_as_its_dated_residue(
    group: str, tmp_path: Path
) -> None:
    """Replay one group of the oracle in order and compare every observation."""
    session = open_session(tmp_path)
    mismatches: list[str] = []
    try:
        for step in (entry for entry in ORACLE["steps"] if entry["group"] == group):
            answer = normalize(run_step(session, step, tmp_path, "repark"))
            residue = step.get("residue")
            expected = residue["repark"] if residue else step["spark"]
            if answer != expected:
                mismatches.append(f"{step['key']}: expected {expected!r}, got {answer!r}")
            if residue is None and answer != step["spark"]:
                mismatches.append(f"{step['key']}: EQUAL step diverges from Spark")
    finally:
        session.stop()
    assert not mismatches, "\n".join(mismatches)


def test_every_residue_names_a_ledger_residue_and_differs_from_spark() -> None:
    """A residue must differ from Spark's answer and cite a residue id of the unit ledger."""
    ledger = (
        Path(__file__).resolve().parents[3]
        / "task"
        / "ledgers"
        / "staging"
        / "u9-types-1-ledger.md"
    ).read_text(encoding="utf-8")
    for step in ORACLE["steps"]:
        residue = step.get("residue")
        if residue is None:
            continue
        assert residue["repark"] != step["spark"], step["key"]
        assert f"| {residue['id']} |" in ledger, residue["id"]
