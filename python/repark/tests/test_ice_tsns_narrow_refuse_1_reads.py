"""ICE-TSNS-NARROW-REFUSE-1, fold 3: a statement that writes nothing answers as the base does.

The unit marks a narrowing during analysis, so a read is analyzed by its code too. These pins
hold every read of ``_record_ice_tsns_narrow_refuse_1_reads.READS`` against
``ice_tsns_narrow_refuse_1_reads_base.json``, recorded on main ``2c33f289``: the Arrow schema with
its field metadata, the rows, and the logical rows of the plain ``EXPLAIN``. ``EXPLAIN VERBOSE``
prints no function of the unit, and an ``EXPLAIN`` frame is the statement frame it is on main.

pins: ice-tsns-narrow-refuse-1/C-040, C-041
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _record_ice_tsns_narrow_refuse_1_reads as recorder

HIDDEN = (
    "__repark_narrowed_beside",
    "__repark_written_timestamp_cast__",
    "repark.untyped_null",
    "repark.written",
)


def differing(spark: Any, base: dict[str, Any]) -> list[str]:
    """Return the statements whose answer on this build is not the base's."""
    return [
        statement
        for statement in recorder.READS
        if recorder.answered(spark, statement.replace("{s}", recorder.SRC)) != base[statement]
    ]


def printed(spark: Any) -> list[tuple[str, str, str]]:
    """Return each hidden name a form of ``EXPLAIN`` prints, with the form and the read."""
    found = []
    for statement in recorder.READS:
        read = statement.replace("{s}", recorder.SRC)
        for form in ("EXPLAIN VERBOSE", "EXPLAIN EXTENDED", "EXPLAIN FORMATTED"):
            try:
                rows = spark.sql(f"{form} {read}").toArrow().to_pylist()
            except Exception:
                continue
            shown = "\n".join(str(row) for row in rows)
            found.extend((name, form, read) for name in HIDDEN if name in shown)
    return found


def test_a_read_answers_as_the_base_does(tmp_path: Path) -> None:
    """Schema with field metadata, rows and the plain ``EXPLAIN`` equal the base's."""
    base = json.loads(recorder.FIXTURE.read_text())["reads"]
    assert sorted(base) == sorted(recorder.READS)
    spark = recorder.open_reads_session(tmp_path)
    try:
        assert differing(spark, base) == []
    finally:
        spark.stop()


def test_explain_prints_no_function_of_the_unit(tmp_path: Path) -> None:
    """No mark, no kept cast, no tag and no sign is printed by any form of ``EXPLAIN``."""
    spark = recorder.open_reads_session(tmp_path)
    try:
        assert printed(spark) == []
    finally:
        spark.stop()


def test_an_explain_frame_is_a_statement_frame(tmp_path: Path) -> None:
    """An ``EXPLAIN`` frame binds without an attribute id and collects its rows."""
    spark = recorder.open_reads_session(tmp_path)
    try:
        explained = spark.sql(f"EXPLAIN SELECT id, coalesce(ns, ts) AS v FROM {recorder.SRC}")
        assert explained.columns == ["plan_type", "plan"]
        assert explained["plan"]._attr_id is None
        assert explained.collect()
    finally:
        spark.stop()
