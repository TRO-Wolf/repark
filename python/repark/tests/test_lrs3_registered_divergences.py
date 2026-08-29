"""LRS-3 — pins for registered divergences, plus the door alias it added.

The registry's rule is that a row lands with its pin or it does not land
(``docs/spark-sql-iceberg-parity.md`` §6). These are those pins: ``RAND-1`` (repark caps a
``randstr`` length Spark accepts) and ``BL-8`` (the SQL door hands back ``UInt64`` where Spark and
the facade give ``bigint``) — the latter pinned in ``test_fnp5_aggregates.py`` as a ratchet, and
reached here through the door's own spelling.

Ledger: ``task/lrs-3-registered-divergences-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs3-registered").getOrCreate()


# ---- RAND-1 — a deliberate limit, not a parity claim ------------------------------------------


def test_randstr_refuses_a_length_spark_accepts() -> None:
    """Spark has no cap: ``SELECT length(randstr(5000000, 1))`` returns 5000000 there (oracle).

    repark caps at 1,000,000 because the failure mode WITHOUT a cap is not an error —
    ``String::with_capacity(n)`` per row aborts the process, SIGABRT, session gone. Pinned so the
    limit is a stated decision rather than something a user discovers.
    """
    with pytest.raises(PySparkException, match="between 0 and 1000000"):
        _session().range(1).select(F.randstr(2_000_000, F.lit(1))).toArrow()


def test_randstr_refuses_a_batch_that_would_overflow_string_offsets() -> None:
    """A LEGAL length times a large batch overflows the i32 offsets of an Arrow ``StringArray``;
    without the gate this panics inside arrow-rs (or aborts). The refusal is a stated contract
    naming both numbers.
    """
    with pytest.raises(PySparkException, match="past the 2147483647 byte limit"):
        _session().range(2_500).select(F.randstr(1_000_000, F.lit(1))).toArrow()


def test_a_randstr_within_both_bounds_is_untouched() -> None:
    """The bounds must not have narrowed the working call."""
    got = _session().range(1).select(F.randstr(10, F.lit(1)).alias("r")).toArrow()
    assert len(got.column("r").to_pylist()[0]) == 10


# ---- the door alias --------------------------------------------------------------------------


def test_the_sql_door_knows_sparks_approx_count_distinct_spelling() -> None:
    """Spark SQL has ``approx_count_distinct``; DataFusion has ``approx_distinct``. The facade
    resolved both from its own dispatch table, so the door needs the Spark spelling registered too.
    """
    session = _session()
    frame = session.createDataFrame([(1,), (2,), (1,)], "g int")
    frame.createOrReplaceTempView("lrs3_alias")
    through_door = session.sql("SELECT approx_count_distinct(g) AS r FROM lrs3_alias").collect()
    through_facade = frame.agg(F.approx_count_distinct("g").alias("r")).collect()
    assert through_door[0]["r"] == through_facade[0]["r"] == 2


def test_the_datafusion_spelling_still_resolves_too() -> None:
    """The alias is added, not swapped — a query written against the engine's own name keeps
    working.
    """
    session = _session()
    session.createDataFrame([(1,), (2,), (1,)], "g int").createOrReplaceTempView("lrs3_alias2")
    assert session.sql("SELECT approx_distinct(g) AS r FROM lrs3_alias2").collect()[0]["r"] == 2


# ---- BL-8 — reached through the door's own spelling ------------------------------


def test_bl8_the_door_still_returns_unsigned_where_the_facade_returns_bigint() -> None:
    """A ratchet, and it is meant to go RED when the door is fixed.

    Spark gives ``bigint`` on both doors. The facade agrees with Spark; the door does not, and the
    cost of that gap is on disk — a ``UInt64`` column written to Parquet is read back by Spark as
    ``decimal(20,0)``.
    """
    session = _session()
    frame = session.createDataFrame([(1,), (2,), (1,)], "g int")
    frame.createOrReplaceTempView("lrs3_bl8")
    door = session.sql("SELECT approx_count_distinct(g) AS r FROM lrs3_bl8").toArrow()
    facade = frame.agg(F.approx_count_distinct("g").alias("r")).toArrow()
    assert str(facade.schema.field("r").type) == "int64"
    assert str(door.schema.field("r").type) == "uint64", (
        "the SQL door no longer returns unsigned — retire registry row BL-8"
    )
