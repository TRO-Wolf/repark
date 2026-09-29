from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.functions import col

ORACLE_PATH: Path = Path(__file__).with_name("ntz_store_doors_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))


def _open(zone: str, warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("ntz-store-doors-1")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    return session


def _plain(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: _plain(item) for key, item in value.asDict().items()}
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if value is None or isinstance(value, (int, float, str)):
        return value
    return repr(value)


def _run_step(session: ReparkSession, step: dict[str, Any]) -> list[list[Any]]:
    door = step.get("df")
    if door == "df_owp":
        session.sql(step["sql"]).writeTo(step["table"]).overwritePartitions()
        return []
    if door == "df_ow_cond":
        session.sql(step["sql"]).writeTo(step["table"]).overwrite(col("id") == step["id"])
        return []
    return [[_plain(item) for item in row] for row in session.sql(step["sql"]).collect()]


@pytest.mark.parametrize("cell", sorted(_ORACLE["cells"]))
def test_write_door_stores_sparks_value(cell: str, tmp_path: Path) -> None:
    spec = _ORACLE["cells"][cell]
    session = _open(spec["zone"], tmp_path)
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
        for step in spec["steps"]:
            if "conf" in step:
                session.conf.set(*step["conf"])
                continue
            if "spark_error" in step:
                with pytest.raises(Exception) as exc_info:
                    _run_step(session, step)
                assert step["repark_error"] in str(exc_info.value), step["key"]
                continue
            rows = _run_step(session, step)
            if "rows" in step:
                assert sorted(rows, key=repr) == step["rows"], step["key"]
    finally:
        session.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        session.stop()


@pytest.mark.parametrize("cell", sorted(_ORACLE["partitions"]))
def test_partition_transforms_follow_the_stored_value(cell: str, tmp_path: Path) -> None:
    spec = _ORACLE["partitions"][cell]
    session = _open(spec["zone"], tmp_path)
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
        for step in spec["steps"]:
            rows = _run_step(session, step)
            if "rows" in step:
                assert sorted(rows, key=repr) == step["rows"], step["key"]
    finally:
        session.stop()
