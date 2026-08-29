"""Charter clause C-012 — the facade and the SQL door must resolve the same kernel.

The facade embeds a UDF instance by hand (a Column has no session to resolve names against); the
SQL door resolves the same spelling out of the session registry. Nothing forces the two to agree,
and a divergence returns different answers for the same input with no error anywhere. The Rust
guard ``crates/repark-python/src/column/door_parity_tests.rs`` proves the two paths hold the same
*function*, not that a user sees the same *answer*. These rows are that evidence, on the Arrow
path, value AND type, per the entry-point matrix in ``docs/testing.md``.

Ledger: ``task/fnp-1-two-door-asymmetry-ledger.md``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom

# Names reachable from both doors, with a recipe per door. The column recipe is a callable so the
# expression is built inside the test, not at import time. Both doors must agree on type AND value.
TWO_DOOR_ROWS: list[tuple[str, object, str]] = [
    ("to_timestamp", lambda: F.to_timestamp(F.col("s")), "to_timestamp(s)"),
    ("to_date", lambda: F.to_date(F.col("s")), "to_date(s)"),
    ("bit_length", lambda: F.bit_length(F.col("s")), "bit_length(s)"),
    ("octet_length", lambda: F.octet_length(F.col("s")), "octet_length(s)"),
]


def _session():
    """A facade session of this module's own, per the suite's build-your-own convention."""
    from repark.spark import SparkSession

    return SparkSession.builder.appName("two-door-kernel-parity").getOrCreate()


@pytest.mark.parametrize(
    ("name", "column", "sql"), TWO_DOOR_ROWS, ids=[row[0] for row in TWO_DOOR_ROWS]
)
def test_both_doors_agree_on_type_and_value(name: str, column, sql: str) -> None:
    spark = _session()
    frame = spark.createDataFrame([("2026-03-01 12:34:56",)], ["s"])
    frame.createOrReplaceTempView("two_door_v")

    facade: pa.Table = frame.select(column().alias("out")).toArrow()
    door: pa.Table = spark.sql(f"SELECT {sql} AS out FROM two_door_v").toArrow()

    assert facade.schema.field("out").type == door.schema.field("out").type, (
        f"{name}: the facade and the SQL door disagree on the RESULT TYPE, so the two doors are "
        f"resolving different kernels (charter clause C-012)"
    )
    assert facade.column("out").to_pylist() == door.column("out").to_pylist(), (
        f"{name}: the facade and the SQL door disagree on the VALUE for the same input"
    )


def test_to_timestamp_is_the_ltz_instant_on_the_facade_path() -> None:
    """The specific regression FNP-1 closed: DataFusion-core returns ``timestamp[ns]``, zoneless.

    Pins the concrete type, not only cross-door equality — the two doors would also "agree" if a
    future change broke both of them the same way.
    """
    spark = _session()
    frame = spark.createDataFrame([("2026-03-01 12:34:56",)], ["s"])

    out = frame.select(F.to_timestamp(F.col("s")).alias("t")).toArrow()
    assert out.schema.field("t").type == pa.timestamp("us", tz="UTC"), (
        "F.to_timestamp must produce the TZ-4 LTZ wire type; timestamp[ns] with no zone means the "
        "facade is back on DataFusion-core's kernel"
    )


def test_avg_agrees_across_doors_including_nulls() -> None:
    """``F.avg`` must be ``SparkAvgWithRetract``, the kernel the SQL door resolves (FLOAT-AGG-2)."""
    spark = _session()
    frame = spark.createDataFrame([(1.0,), (2.0,), (None,)], ["x"])
    frame.createOrReplaceTempView("two_door_avg_v")

    facade = frame.select(F.avg("x").alias("a")).toArrow()
    door = spark.sql("SELECT avg(x) AS a FROM two_door_avg_v").toArrow()

    assert facade.schema.field("a").type == door.schema.field("a").type
    assert facade.column("a").to_pylist() == door.column("a").to_pylist()
