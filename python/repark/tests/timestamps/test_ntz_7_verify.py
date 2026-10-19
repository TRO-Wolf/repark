"""WO NTZ-1 verifier fold: from_utc_timestamp and to_utc_timestamp keep Spark's TIMESTAMP.

Slice 2's store-cast retarget rewrote every analyzer-visible cast to a naive
timestamp, including the implicit string coercion inside ``from_utc_timestamp``
and ``to_utc_timestamp`` — plain SELECT statements answered TIMESTAMP_NTZ walls with the
wrong value and type. The retarget now fires only inside a DML plan, so these
cells answer Spark's recorded values again. The oracle is
``ntz_7_verify_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0, measured
2026-09-28 in an America/New_York session). Every cell compares rows and
dtypes; unaliased column names stay out of the comparison per the R2 naming
class (RePark renders the embedded call where Spark renders its function text).

pins: ntz-1/C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("ntz_7_verify_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))


def _open(zone: str, warehouse: Path) -> ReparkSession:
    """Open a facade session at the zone with a memory catalog."""
    session = (
        ReparkSession.builder.appName("ntz-7-verify")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame into plain rows."""
    return [list(row) for row in frame.collect()]


def _dtypes(frame: Any) -> list[str]:
    """Read a frame's dtypes without the column names."""
    return [dtype for _, dtype in frame.dtypes]


def test_zone_shift_functions_keep_sparks_timestamp(tmp_path: Path) -> None:
    """The recorded from/to_utc_timestamp cells replay value and dtype."""
    session = _open("America/New_York", tmp_path)
    try:
        for statement in _ORACLE["setup"]:
            session.sql(statement).collect()
        for key in sorted(_ORACLE["cells"]):
            cell = _ORACLE["cells"][key]
            frame = session.sql(cell["sql"])
            assert _rows(frame) == cell["rows"], key
            assert _dtypes(frame) == cell["dtypes"], key
    finally:
        session.stop()
