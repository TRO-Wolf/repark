"""LRS-4 — per-door divergence pins (C-012): SQL door and facade resolve different kernels.

LOG-1 pins (SEM-1, 2026-08-31) now assert Spark's answers. UNIX-1 still
codifies today's behavior; the unit that fixes it turns that pin red on purpose.
"""

from __future__ import annotations

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs4-door-domain").getOrCreate()


def _frame():
    frame = _session().createDataFrame([(1,)], "i int")
    frame.createOrReplaceTempView("lrs4_probe")
    return frame


def test_log1_sql_door_log_is_natural() -> None:
    """pins: sem-1-spark-answer-parity/C-004, C-008

    Spark returns 2.0794415416798357 for ``log(8)`` — the natural log. Both doors match.
    """
    frame = _frame()
    door = _session().sql("SELECT log(8) AS r FROM lrs4_probe").collect()[0][0]
    facade = frame.select(F.log(F.lit(8.0)).alias("r")).collect()[0][0]
    assert facade == 2.0794415416798357
    assert door == 2.0794415416798357


def test_log1_the_two_argument_form_agrees_on_positive_operands() -> None:
    """The two-argument form agrees on positive operands."""
    _frame()
    assert _session().sql("SELECT log(2, 8) AS r FROM lrs4_probe").collect()[0][0] == 3.0


def test_log1_both_arities_null_on_non_positive_operands() -> None:
    """pins: sem-1-spark-answer-parity/C-004, C-008

    Spark returns NULL for every one of these (live PySpark 4.1.2, 2026-08-31).
    """
    _frame()
    session = _session()

    def door(text: str) -> object:
        return session.sql(f"SELECT {text} AS r FROM lrs4_probe").collect()[0][0]

    assert door("log(0, 8)") is None
    assert door("log(-2, 8)") is None
    assert door("log(10, 0)") is None
    assert door("log(10, -1)") is None
    assert door("log(0)") is None
    assert door("log(-1)") is None


def test_unix1_sql_door_from_unixtime_is_a_timestamp() -> None:
    """Spark's ``from_unixtime`` returns a STRING (``struct<r:string>``); the facade agrees, the
    SQL door returns a timestamp.
    """
    frame = _frame()
    door = _session().sql("SELECT from_unixtime(0) AS r FROM lrs4_probe").toArrow()
    facade = frame.select(F.from_unixtime(F.lit(0)).alias("r")).toArrow()
    assert str(facade.schema.field("r").type) == "string", "the facade already matches Spark"
    assert str(door.schema.field("r").type).startswith("timestamp"), (
        "the SQL door no longer returns a timestamp — close UNIX-1 and retire the row"
    )
