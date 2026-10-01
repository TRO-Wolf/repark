"""WO NTZ-1 re-verify fold: VALUES cells computed in LTZ store the session wall.

The part-2 retarget wrapped bare expressions only on the DML projection, so a
VALUES cell holding ``from_utc_timestamp``, ``to_utc_timestamp`` or
``date_trunc`` reached the writer as a UTC instant and failed with an Arrow
type error. VALUES cells at naive-microsecond VALUES-schema fields now wrap
as well, including VALUES nested under pass-through subqueries. This test
replays the recorded New York write sequence against
``ntz_9_verify_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0, measured
2026-09-28): every write must run and every read replays Spark's rows and
dtypes.

pins: ntz-1/C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("ntz_9_verify_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))


def _open(zone: str, warehouse: Path) -> ReparkSession:
    """Open a facade session at the zone with a memory catalog."""
    session = (
        ReparkSession.builder.appName("ntz-9-verify")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame into plain rows."""
    return [list(row) for row in frame.collect()]


def _dtypes(frame: Any) -> list[str]:
    """Read a frame's dtypes without the column names."""
    return [dtype for _, dtype in frame.dtypes]


def test_values_cells_store_sparks_session_wall(tmp_path: Path) -> None:
    """The recorded write sequence replays Spark's rows, dtypes and refusals."""
    session = _open("America/New_York", tmp_path)
    try:
        for step in _ORACLE["steps"]:
            if "repark_error_contains" in step:
                with pytest.raises(Exception) as exc_info:
                    session.sql(step["sql"]).collect()
                assert step["repark_error_contains"] in str(exc_info.value), step["key"]
                assert step["repark_error_contains"] not in step["spark_error"], step["key"]
                continue
            frame = session.sql(step["sql"])
            if "rows" in step:
                assert _rows(frame) == step["rows"], step["key"]
                assert _dtypes(frame) == step["dtypes"], step["key"]
                if "spark_rows" in step:
                    assert step["spark_rows"] != step["rows"], step["key"]
            else:
                frame.collect()
    finally:
        session.stop()
