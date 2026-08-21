"""LRS-4 — what the widened C-012 guard found.

The Rust guard's domain went from 20 hand-listed names to the session registry's own 341. Four
names resolve a different kernel per door; two of them are user-visible enough to be registry rows,
and these are their pins. Both **codify today's behavior**, so the unit that fixes either turns its
pin red on purpose.

Ledger: ``task/lrs-4-door-domain-ledger.md``.
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


def test_log1_sql_door_log_is_base_ten() -> None:
    """**Spark returns 2.0794415416798357** for ``log(8)`` — the natural log.

    The facade is right; the SQL door returns DataFusion's base-10 answer. A silently wrong number
    on a common function: it is off by a constant factor and looks perfectly plausible.
    """
    frame = _frame()
    door = _session().sql("SELECT log(8) AS r FROM lrs4_probe").collect()[0][0]
    facade = frame.select(F.log(F.lit(8.0)).alias("r")).collect()[0][0]
    assert facade == 2.0794415416798357, "the facade already matches Spark"
    assert door == 0.9030899869919434, "the SQL door is base 10 — close LOG-1 and this goes red"


def test_log1_the_two_argument_form_agrees_on_positive_operands() -> None:
    """The happy path agrees, which is what made it look like only the one-argument form was
    broken. It is not — see the pin below.
    """
    _frame()
    assert _session().sql("SELECT log(2, 8) AS r FROM lrs4_probe").collect()[0][0] == 3.0


def test_log1_the_two_argument_form_diverges_on_non_positive_operands() -> None:
    """**Spark returns NULL for every one of these** (``Logarithm.nullSafeEval``).

    DataFusion's ``LogFunc`` has no null-guard, so it hands back IEEE junk instead. This pin exists
    because the first version of registry row ``LOG-1`` claimed only the one-argument form
    diverged — it was written from ``log(2, 8) == 3.0``, which is true and proves nothing about
    the operand domain. A fix that redirects the one-argument form to ``ln`` and leaves
    DataFusion's two-argument formula alone would close half the row and leave this half silently
    open.
    """
    _frame()
    session = _session()

    def door(text: str):
        return session.sql(f"SELECT {text} AS r FROM lrs4_probe").collect()[0][0]

    assert door("log(0, 8)") == 0.0  # -0.0; Spark: NULL
    assert str(door("log(-2, 8)")) == "nan"  # Spark: NULL
    assert door("log(10, 0)") == float("-inf")  # Spark: NULL
    assert str(door("log(10, -1)")) == "nan"  # Spark: NULL
    assert door("log(0)") == float("-inf")  # Spark: NULL
    assert str(door("log(-1)")) == "nan"  # Spark: NULL


def test_unix1_sql_door_from_unixtime_is_a_timestamp() -> None:
    """Spark's ``from_unixtime`` returns a STRING (``struct<r:string>``). The facade agrees; the
    SQL door hands back a timestamp, so the same call written two ways lands two different types
    in Parquet.
    """
    frame = _frame()
    door = _session().sql("SELECT from_unixtime(0) AS r FROM lrs4_probe").toArrow()
    facade = frame.select(F.from_unixtime(F.lit(0)).alias("r")).toArrow()
    assert str(facade.schema.field("r").type) == "string", "the facade already matches Spark"
    assert str(door.schema.field("r").type).startswith("timestamp"), (
        "the SQL door no longer returns a timestamp — close UNIX-1 and retire the row"
    )
