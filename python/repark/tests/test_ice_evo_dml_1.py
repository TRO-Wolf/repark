"""ICE-EVO-DML-1 — DML after ADD / RENAME COLUMN with no write since answers Spark.

pins: ice-evo-dml-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-012
pins: ice-evo-dml-1/C-016, C-017
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import _record_ice_evo_dml_1 as recorder
import pyarrow as pa
import pytest

from repark.errors import (
    AnalysisException,
    ParseException,
    UnsupportedOperationException,
)

_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "ice_evo_dml_1"
_LIVE_CATALOG = "ice_evo_dml_1_live"
_TRUTH = json.loads(recorder.TRUTH_FILE.read_text(encoding="utf-8"))
_CASES = recorder.build_cases()
_ANSWERS = {answer["id"]: answer for answer in _TRUTH["answers"]}
_TABLE_CASES = [case for case in _CASES if case["group"] not in {"adopted", "lineage_read"}]
_LINEAGE_CASES = [case for case in _CASES if case["group"] == "lineage_read"]
_DATAFRAME_CASES = [case for case in _TABLE_CASES if case["steps"][0]["dataframe"] is not None]
_EX_W2_4 = (
    "EX-W2-4 (OPEN, fix unit WRITERV2-OVERWRITE-UNPART-1): overwritePartitions() on an "
    "unpartitioned table renders PARTITION () and leaks a ParseException"
)
_ERROR_TYPES = {
    "AnalysisException": AnalysisException,
    "ParseException": ParseException,
    "UnsupportedOperationException": UnsupportedOperationException,
}
_DATAFRAME_PARAMS = [
    pytest.param(
        case,
        id=case["id"],
        marks=pytest.mark.xfail(strict=True, raises=ParseException, reason=_EX_W2_4)
        if "overwrite_partitions" in case["steps"][0]["dataframe"]
        else (),
    )
    for case in _DATAFRAME_CASES
]
_ADOPTED_DOORS = [
    (case, door)
    for case in _CASES
    if case["group"] == "adopted"
    for door in ("sql", "dataframe")
    if door == "sql" or case["steps"][0]["dataframe"] is not None
]


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
def _materialized_adopted_table() -> Iterator[Path]:
    root = recorder.adopted_root()
    lock = _DirLock(Path(str(recorder.ADOPTED_WAREHOUSE) + ".lock"))
    try:
        if root.exists():
            shutil.rmtree(root)
        root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(
            recorder.FIXTURE_DIR / recorder.ADOPTED_TABLE, root, copy_function=shutil.copy
        )
        yield root
    finally:
        with suppress(OSError):
            if recorder.ADOPTED_WAREHOUSE.exists():
                shutil.rmtree(recorder.ADOPTED_WAREHOUSE)
        lock.close()


def _session(warehouse: Path) -> Any:
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("ice-evo-dml-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.ns")
    return session


def _answer_of(table: pa.Table) -> dict[str, Any]:
    rows = [[row[name] for name in table.column_names] for row in table.to_pylist()]
    rows.sort(key=repr)
    return {"columns": list(table.column_names), "rows": rows}


def _dataframe_merge(session: Any, table: str, spec: dict[str, Any]) -> None:
    from repark.spark import functions

    condition = functions.col(f"target.{spec['key']}") == functions.col(f"source.{spec['key']}")
    writer = session.sql(spec["source"]).mergeInto(table, condition)
    if spec.get("update_all"):
        writer = writer.whenMatched().updateAll()
    if spec.get("update"):
        writer = writer.whenMatched().update(
            {name: functions.col(f"source.{name}") for name in spec["update"]}
        )
    if spec.get("insert_all"):
        writer = writer.whenNotMatched().insertAll()
    if spec.get("insert"):
        writer = writer.whenNotMatched().insert(
            {name: functions.col(f"source.{name}") for name in spec["insert"]}
        )
    if spec.get("by_source_update"):
        writer = writer.whenNotMatchedBySource().update(
            {name: functions.lit(value) for name, value in spec["by_source_update"].items()}
        )
    writer.merge()


def _dataframe_statement(session: Any, table: str, spec: dict[str, Any]) -> None:
    if "merge" in spec:
        _dataframe_merge(session, table, spec["merge"])
    elif "append" in spec:
        session.sql(spec["append"]).writeTo(table).append()
    else:
        session.sql(spec["overwrite_partitions"]).writeTo(table).overwritePartitions()


def _expected(case_id: str, door: str) -> tuple[dict[str, Any], dict[str, Any]]:
    answer = _ANSWERS[case_id]
    if door == "dataframe":
        statement, check = answer["dataframe_steps"]
        return statement["dataframe"], check["dataframe"]
    statement, check = answer["steps"]
    return statement["sql"], check["sql"]


def _replay(session: Any, table: str, case: dict[str, Any], door: str) -> None:
    statement, check = case["steps"]
    expected_statement, expected_check = _expected(case["id"], door)
    label = f"{case['id']} [{door}]"
    if statement["kind"] == "statement_error":
        expected_type = expected_statement["error"]["type"]
        assert expected_type in _ERROR_TYPES, (label, expected_statement)
        with pytest.raises(_ERROR_TYPES[expected_type]):
            session.sql(recorder.render(statement["sql"], table)).collect()
    else:
        assert expected_statement == {"ok": True}, (label, expected_statement)
        if door == "dataframe":
            _dataframe_statement(session, table, statement["dataframe"])
        else:
            session.sql(recorder.render(statement["sql"], table)).collect()
    got = _answer_of(session.sql(recorder.render(check["sql"], table)).to_arrow())
    assert got["columns"] == expected_check["columns"], (label, got["columns"])
    assert got["rows"] == expected_check["rows"], (label, got["rows"], expected_check["rows"])


def _run_setup(session: Any, table: str, case: dict[str, Any]) -> None:
    for statement in case["setup"]:
        session.sql(recorder.render(statement, table)).collect()


def test_recorded_oracle_covers_the_driver_catalog() -> None:
    assert _TRUTH["catalog_sha256"] == recorder.catalog_digest(_CASES)
    assert sorted(_ANSWERS) == sorted(case["id"] for case in _CASES)
    assert _TRUTH["oracle"]["pyspark"] == "4.1.2"
    assert _TRUTH["oracle"]["iceberg_runtime"].endswith("iceberg-spark-runtime-4.1_2.13:1.11.0")
    assert _TRUTH["oracle"]["ansi"] == "true"
    assert {case["group"] for case in _CASES} == {
        "grid",
        "emptied",
        "rewrite",
        "merge_star_source",
        "adopted",
        "lineage_read",
        "drop_add",
        "rename_onto_dropped",
        "add_default",
        "new_col_predicate",
    }
    grid = [case for case in _CASES if case["group"] == "grid"]
    assert len(grid) == 4 * 8 * 2 * 2
    l02 = [case for case in _CASES if case["group"] in {"drop_add", "rename_onto_dropped"}]
    assert len(l02) == 2 * 3 * 2 * 2
    assert len([case for case in _CASES if case["group"] == "add_default"]) == 2
    assert len([case for case in _CASES if case["group"] == "new_col_predicate"]) == 3 * 2 * 2
    assert {case["format_version"] for case in grid} == {"2", "3"}
    assert {case["mode"] for case in grid} == set(recorder.MODES)
    for case in _DATAFRAME_CASES:
        assert "dataframe_steps" in _ANSWERS[case["id"]], case["id"]


@pytest.mark.parametrize("case", _TABLE_CASES, ids=[case["id"] for case in _TABLE_CASES])
def test_sql_door_matches_spark(case: dict[str, Any], tmp_path: Path) -> None:
    session = _session(tmp_path / "warehouse")
    try:
        table = f"{_CATALOG}.ns.t"
        _run_setup(session, table, case)
        _replay(session, table, case, "sql")
    finally:
        session.stop()


@pytest.mark.parametrize("case", _LINEAGE_CASES, ids=[case["id"] for case in _LINEAGE_CASES])
def test_lineage_read_matches_spark(case: dict[str, Any], tmp_path: Path) -> None:
    session = _session(tmp_path / "warehouse")
    try:
        table = f"{_CATALOG}.ns.t"
        _run_setup(session, table, case)
        for step, recorded in zip(case["steps"], _ANSWERS[case["id"]]["steps"], strict=True):
            expected = recorded["sql"]
            got = _answer_of(session.sql(recorder.render(step["sql"], table)).to_arrow())
            label = f"{case['id']} {step['id']}"
            assert got["columns"] == expected["columns"], (label, got["columns"])
            assert got["rows"] == expected["rows"], (label, got["rows"], expected["rows"])
    finally:
        session.stop()


@pytest.mark.parametrize("case", _DATAFRAME_PARAMS)
def test_dataframe_door_matches_spark(case: dict[str, Any], tmp_path: Path) -> None:
    session = _session(tmp_path / "warehouse")
    try:
        table = f"{_CATALOG}.ns.t"
        _run_setup(session, table, case)
        _replay(session, table, case, "dataframe")
    finally:
        session.stop()


@pytest.mark.parametrize(
    ("case", "door"),
    _ADOPTED_DOORS,
    ids=[f"{case['id']}-{door}" for case, door in _ADOPTED_DOORS],
)
def test_adopted_spark_table_matches_spark(case: dict[str, Any], door: str, tmp_path: Path) -> None:
    frozen = _ANSWERS[case["id"]]["frozen"]
    session = _session(tmp_path / "warehouse")
    try:
        with _materialized_adopted_table() as root:
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
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_LIVE_CATALOG}.ns")
    drift = []
    for index, case in enumerate(_CASES):
        table = f"{_LIVE_CATALOG}.ns.live{index}"
        for statement in case["setup"]:
            spark.sql(recorder.render(statement, table)).collect()
        recorded = _ANSWERS[case["id"]]
        if recorder.run_case_steps(spark, table, case) != recorded["steps"]:
            drift.append(case["id"])
        if "dataframe_steps" in recorded:
            twin = f"{_LIVE_CATALOG}.ns.twin{index}"
            for statement in case["setup"]:
                spark.sql(recorder.render(statement, twin)).collect()
            if (
                recorder.run_dataframe_steps(spark, functions, twin, case)
                != recorded["dataframe_steps"]
            ):
                drift.append(f"{case['id']} dataframe")
    assert drift == []
