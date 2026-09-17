"""ICE-PROMOTE-READ-1 — reads and DML after a legal Iceberg type promotion answer Spark.

pins: ice-promote-read-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: ice-promote-read-1/C-009, C-010, C-012, C-015
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from decimal import Decimal
from pathlib import Path
from typing import Any

import _record_ice_promote_read_1 as recorder
import pyarrow as pa
import pytest

_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "ice_promote_read_1"
_LIVE_CATALOG = "ice_promote_read_1_live"
_LIVE_ADOPTED_CATALOG = "ice_promote_read_1_live_adopted"
_SPARK_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
_TRUTH = json.loads(recorder.TRUTH_FILE.read_text(encoding="utf-8"))
_CASES = recorder.build_cases()
_ANSWERS = {answer["id"]: answer for answer in _TRUTH["answers"]}
_PROMOTED_COLUMNS = ("id", "f", "d", "p")
_TABLE_CASES = [case for case in _CASES if case["group"] != "adopted"]
_DATAFRAME_CASES = [
    case for case in _TABLE_CASES if any(step["dataframe"] is not None for step in case["steps"])
]
_ADOPTED_CASES = [case for case in _CASES if case["group"] == "adopted"]


class _DirLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(
                        f"fixture lock {path} held for 2 minutes (no steal)"
                    ) from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialized_adopted_table(table: str) -> Iterator[Path]:
    root = recorder.ADOPTED_WAREHOUSE / recorder.ADOPTED_NAMESPACE / table
    lock = _DirLock(Path(str(recorder.ADOPTED_WAREHOUSE) + ".lock"))
    try:
        if root.exists():
            shutil.rmtree(root)
        root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(recorder.FIXTURE_DIR / table, root, copy_function=shutil.copy)
        yield root
    finally:
        with suppress(OSError):
            if recorder.ADOPTED_WAREHOUSE.exists():
                shutil.rmtree(recorder.ADOPTED_WAREHOUSE)
        lock.close()


def _session(warehouse: Path) -> Any:
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("ice-promote-read-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.ns")
    return session


def _normalized(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value


def _answer_of(table: pa.Table) -> dict[str, Any]:
    rows = [[_normalized(row[name]) for name in table.column_names] for row in table.to_pylist()]
    rows.sort(key=repr)
    return {
        "columns": list(table.column_names),
        "types": {field.name: str(field.type) for field in table.schema},
        "rows": rows,
    }


def _promoted_types(answer: dict[str, Any]) -> dict[str, str]:
    return {name: kind for name, kind in answer["types"].items() if name in _PROMOTED_COLUMNS}


def _assert_query_matches(label: str, got: dict[str, Any], expected: dict[str, Any]) -> None:
    assert "error" not in expected, (label, expected)
    assert got["rows"] == expected["rows"], (label, got["rows"], expected["rows"])
    assert got["columns"] == expected["columns"], (label, got["columns"], expected["columns"])
    assert _promoted_types(got) == _promoted_types(expected), (label, got["types"])


def _column_filter(functions: Any, spec: dict[str, Any]) -> Any:
    column = functions.col(spec["column"])
    operations = {
        "eq": lambda literal: column == literal,
        "ne": lambda literal: column != literal,
        "lt": lambda literal: column < literal,
        "le": lambda literal: column <= literal,
        "gt": lambda literal: column > literal,
        "ge": lambda literal: column >= literal,
    }
    if spec["op"] == "in":
        return column.isin(*spec["value"])
    if spec["op"] == "not_in":
        return ~column.isin(*spec["value"])
    if spec["op"] == "between":
        return column.between(*spec["value"])
    if "decimal" in spec:
        return operations[spec["op"]](functions.lit(Decimal(spec["decimal"])))
    return operations[spec["op"]](functions.lit(spec["value"]))


def _dataframe_query(session: Any, table: str, spec: dict[str, Any]) -> dict[str, Any]:
    from repark.spark import functions

    frame = session.table(table)
    if "filter" in spec:
        frame = frame.filter(_column_filter(functions, spec["filter"]))
    if spec.get("count"):
        return {"columns": ["c"], "types": {}, "rows": [[frame.count()]]}
    return _answer_of(frame.select(*spec["select"]).to_arrow())


def _dataframe_merge(session: Any, table: str, spec: dict[str, Any]) -> None:
    from repark.spark import functions

    condition = functions.col(f"target.{spec['key']}") == functions.col(f"source.{spec['key']}")
    writer = session.sql(spec["source"]).mergeInto(table, condition)
    writer = writer.whenMatched().update(
        {name: functions.col(f"source.{name}") for name in spec["update"]}
    )
    if spec.get("insert_all"):
        writer = writer.whenNotMatched().insertAll()
    if spec.get("insert"):
        writer = writer.whenNotMatched().insert(
            {name: functions.col(f"source.{name}") for name in spec["insert"]}
        )
    writer.merge()


def _run_setup(session: Any, table: str, case: dict[str, Any]) -> None:
    for statement in case["setup"]:
        session.sql(statement.format(t=table)).collect()


def _run_statement_step(session: Any, table: str, step: dict[str, Any], door: str) -> None:
    for key, value in step.get("conf", {}).items():
        session.conf.set(key, value)
    spec = step["dataframe"] if door == "dataframe" else None
    if spec is not None and "merge" in spec:
        _dataframe_merge(session, table, spec["merge"])
    elif spec is not None and "overwrite_partitions" in spec:
        session.sql(spec["overwrite_partitions"]).writeTo(table).overwritePartitions()
    else:
        session.sql(step["sql"].format(t=table)).collect()


def _spark_step_answers(case_id: str, door: str) -> dict[str, dict[str, Any]]:
    answer = _ANSWERS[case_id]
    if door == "dataframe" and "dataframe_steps" in answer:
        return {step["id"]: {"sql": step["dataframe"]} for step in answer["dataframe_steps"]}
    return {step["id"]: step for step in answer["steps"]}


def _expected_query_answer(
    spark_step: dict[str, Any], step: dict[str, Any], door: str
) -> dict[str, Any]:
    if door != "dataframe" or step["dataframe"] is None or "dataframe" not in spark_step:
        return spark_step["sql"]
    stored = spark_step["dataframe"]
    if "count" in stored:
        return {"columns": ["c"], "types": {}, "rows": [[stored["count"]]]}
    return stored


def _replay(session: Any, table: str, case: dict[str, Any], door: str) -> None:
    spark_steps = _spark_step_answers(case["id"], door)
    for step in case["steps"]:
        spark_step = spark_steps[step["id"]]
        label = f"{case['id']} [{door}] {step['id']}"
        if step["kind"] == "statement":
            assert spark_step["sql"] == {"ok": True}, (label, spark_step)
            _run_statement_step(session, table, step, door)
            continue
        if door == "dataframe" and step["dataframe"] is not None:
            got = _dataframe_query(session, table, step["dataframe"])
        else:
            got = _answer_of(session.sql(step["sql"].format(t=table)).to_arrow())
        _assert_query_matches(label, got, _expected_query_answer(spark_step, step, door))


def test_recorded_oracle_covers_the_driver_catalog() -> None:
    assert _TRUTH["catalog_sha256"] == recorder.catalog_digest(_CASES)
    assert sorted(_ANSWERS) == sorted(case["id"] for case in _CASES)
    assert _TRUTH["oracle"]["pyspark"] == "4.1.2"
    assert _TRUTH["oracle"]["iceberg_runtime"].endswith("iceberg-spark-runtime-4.1_2.13:1.11.0")
    assert _TRUTH["oracle"]["ansi"] == "true"
    for answer in _TRUTH["answers"]:
        assert "setup_error" not in answer, answer["id"]
    groups = {case["group"] for case in _CASES}
    assert groups == {
        "read",
        "read_partition",
        "merge_key",
        "dml_range",
        "dml_single",
        "dml_partition",
        "adopted",
        "inspect",
    }
    assert {case["format_version"] for case in _CASES} == {"2", "3"}


@pytest.mark.parametrize("case", _TABLE_CASES, ids=[case["id"] for case in _TABLE_CASES])
def test_sql_door_matches_spark(case: dict[str, Any], tmp_path: Path) -> None:
    session = _session(tmp_path / "warehouse")
    try:
        table = f"{_CATALOG}.ns.t"
        _run_setup(session, table, case)
        _replay(session, table, case, "sql")
    finally:
        session.stop()


@pytest.mark.parametrize("case", _DATAFRAME_CASES, ids=[case["id"] for case in _DATAFRAME_CASES])
def test_dataframe_door_matches_spark(case: dict[str, Any], tmp_path: Path) -> None:
    session = _session(tmp_path / "warehouse")
    try:
        table = f"{_CATALOG}.ns.t"
        _run_setup(session, table, case)
        _replay(session, table, case, "dataframe")
    finally:
        session.stop()


@pytest.mark.parametrize("door", ["sql", "dataframe"])
@pytest.mark.parametrize("case", _ADOPTED_CASES, ids=[case["id"] for case in _ADOPTED_CASES])
def test_adopted_spark_table_matches_spark(case: dict[str, Any], door: str, tmp_path: Path) -> None:
    frozen = _ANSWERS[case["id"]]["frozen"]
    session = _session(tmp_path / "warehouse")
    try:
        with _materialized_adopted_table(case["table"]) as root:
            assert str(root) == frozen["table_root"]
            metadata_file = root / "metadata" / frozen["metadata_file"]
            session.sql(
                f"CALL {_CATALOG}.system.register_table(table => 'ns.{case['table']}', "
                f"metadata_file => '{metadata_file}')"
            )
            _replay(session, f"{_CATALOG}.ns.{case['table']}", case, door)
    finally:
        session.stop()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_spark_rederives_every_recorded_answer(tmp_path: Path) -> None:
    import _live_parity as lp
    from pyspark.sql import functions

    spark = lp.build_spark_iceberg_engine(tmp_path / "live", catalog=_LIVE_CATALOG).session
    spark.conf.set(f"spark.sql.catalog.{_LIVE_ADOPTED_CATALOG}", _SPARK_CATALOG_IMPL)
    spark.conf.set(f"spark.sql.catalog.{_LIVE_ADOPTED_CATALOG}.type", "hadoop")
    spark.conf.set(
        f"spark.sql.catalog.{_LIVE_ADOPTED_CATALOG}.warehouse", str(tmp_path / "adopted")
    )
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_LIVE_CATALOG}.ns")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_LIVE_ADOPTED_CATALOG}.ns")
    drift = []
    for index, case in enumerate(_CASES):
        catalog = _LIVE_ADOPTED_CATALOG if case["group"] == "adopted" else _LIVE_CATALOG
        table = f"{catalog}.ns.live{index}"
        for statement in case["setup"]:
            spark.sql(statement.format(t=table)).collect()
        recorded = _ANSWERS[case["id"]]
        if recorder.run_case_steps(spark, functions, table, case) != recorded["steps"]:
            drift.append(case["id"])
        if "dataframe_steps" in recorded:
            twin = recorder.run_overwrite_partitions_twin(
                spark, functions, case, index, _LIVE_CATALOG
            )
            if twin != recorded["dataframe_steps"]:
                drift.append(f"{case['id']} overwritePartitions")
    assert drift == []
