"""WO NTZ-1 verifier fold part 2: zone-shift calls inside DML keep Spark's instant.

The part-1 DML gate still rewrote casts nested inside function arguments of a
DML plan, so ``from_utc_timestamp`` and ``to_utc_timestamp`` over strings
computed a wall-shifted instant on INSERT ... SELECT — observably wrong at DST
boundaries. The retarget now touches only the store projection (top-level
casts of the projection under the DML node, VALUES rows, and bare
expressions at TIMESTAMP_NTZ positions). This test replays the recorded
New York write sequence against ``ntz_8_verify_spark_oracle.json`` (Spark
4.1.2 + Iceberg 1.11.0, measured 2026-09-28): every write must run and every
read replays Spark's rows and dtypes. The two DST INSERT reads were red on
0a61bc02 (off by the DST hour); the fixed-offset cells guard the mechanism.

pins: ntz-1/C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("ntz_8_verify_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))


def _open(zone: str, warehouse: Path) -> ReparkSession:
    """Open a facade session at the zone with a memory catalog."""
    session = (
        ReparkSession.builder.appName("ntz-8-verify")
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


def test_zone_shift_calls_inside_dml_store_sparks_answer(tmp_path: Path) -> None:
    """The recorded write sequence replays Spark's rows and dtypes."""
    session = _open("America/New_York", tmp_path)
    try:
        for step in _ORACLE["steps"]:
            frame = session.sql(step["sql"])
            if "rows" in step:
                assert _rows(frame) == step["rows"], step["key"]
                assert _dtypes(frame) == step["dtypes"], step["key"]
            else:
                frame.collect()
    finally:
        session.stop()
