"""Live-Spark-oracle parity pins for the Float64 sliding-avg shim (R-RETRACT-SHIM rider).

X2 shipped with mathematically-derived Rust pins only; this rider adds the entry-point pins
docs/testing.md requires for the parity claim: oracle-recorded values (live PySpark 4.1.2,
zulu-17, 2026-07-29 — verbatim block in the X2 ledger) on the Arrow path, value AND type,
including the NULL-in-frame cases where a hand-rolled ``retract_batch`` typically breaks
(NULL retraction must not decrement the count; an all-NULL frame yields NULL avg / 0 count).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession

# One column with NULLs so frames cover: partial-NULL, all-NULL, and NULL-retract transitions.
_SQL = (
    "SELECT id, "
    "avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a, "
    "count(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS c "
    "FROM (VALUES (1, CAST(1.0 AS DOUBLE)), (2, CAST(NULL AS DOUBLE)), "
    "(3, CAST(3.0 AS DOUBLE)), "
    "(4, CAST(NULL AS DOUBLE)), (5, CAST(NULL AS DOUBLE)), "
    "(6, CAST(6.0 AS DOUBLE))) t(id, v) "
    "ORDER BY id"
)

# Live PySpark 4.1.2 output, recorded 2026-07-29 (all values exactly representable in f64).
_ORACLE = [
    (1, 1.0, 1),
    (2, 1.0, 1),
    (3, 3.0, 1),
    (4, 3.0, 1),
    (5, None, 0),
    (6, 6.0, 1),
]


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("sliding-avg-parity").getOrCreate()
    yield session
    session.stop()


def test_sliding_avg_null_frames_match_live_oracle(spark: ReparkSession) -> None:
    rows = spark.sql(_SQL).collect()
    got = [(row["id"], row["a"], row["c"]) for row in rows]
    assert got == _ORACLE


def test_sliding_avg_arrow_types_match_live_oracle(spark: ReparkSession) -> None:
    table = spark.sql(_SQL).to_arrow()
    assert table.schema.field("a").type == pa.float64()  # oracle: double
    assert table.schema.field("c").type == pa.int64()  # oracle: bigint
