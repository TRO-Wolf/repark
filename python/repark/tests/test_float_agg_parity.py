"""Float aggregation differential corpus (H-2 gap G7) — catastrophic-cancellation fixture.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``). One SQL string per row runs on BOTH engines, so the recipe
under test and the recipe the oracle ran are the same string — nothing here is hand-computed.

**Fixture.** The same catastrophic-cancellation vector the Rust pins use
(``crates/repark-spark/src/tests/float_agg.rs``): large ±1e16 interleaved with small addends.
Exact element bit patterns are fixed there; this module reuses the same VALUES recipe.

**Why a row may be a DISCLOSURE.** When repark and Spark agree on value AND Arrow type AND
nullability the row is a plain equality (``repark is None``). When they honestly disagree in
last-ulp bits (or type/nullability), the row pins BOTH halves and asserts the divergence still
holds — or declares an in-module tolerance. A silent CONVERGENCE goes red. Never fudge a bit
pattern. Live-tier DISCLOSURE implications are §6 paste-true handoff only (conductor A4: this
lane never edits ``_live_parity.py``).

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``) through the parity
comparator — value AND type AND nullability; never ``show``.

**Re-deriving the goldens (record mode).** The driver that recorded every ``spark`` half is
committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_float_agg_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe. Needs a JVM + ``pyspark``
(``uv sync --extra record``); never collected by pytest. CI stays JVM-free. Hold
``/tmp/grok-jvm-record.lock`` (conductor B4).

**Entry points.** Facade ``sql()`` door only for these two rows. The Rust ``f64::to_bits`` pins
at three ``target_partitions`` counts live in ``float_agg.rs`` (G7 Rust half).

**In-flight fix named by every disclosure** so a red row points at what flips it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Named so every disclosure's note can cite the same future work without inventing per-row fix IDs.
FIX_G7 = (
    "the float aggregation determinism / accumulation-order fix "
    "(briefs/v2-engine-hardening.md, gap G7; DECLARE candidacy if the ruling is disclosure-only)"
)

# Budget floors/ceilings pinned by test_float_agg_row_set_covers_g7_budget (not incidental).
G7_BUDGET_MIN = 2
G7_BUDGET_MAX = 2

# Same VALUES recipe as the Rust fixture (order matters for accumulation).
# 1e16, 1.0, -1e16, 2.0, 1e16, 0.5, -1e16, 0.25
FIXTURE_VALUES_SQL = (
    "SELECT * FROM (VALUES "
    "(CAST(1.0e16 AS DOUBLE)), "
    "(CAST(1.0 AS DOUBLE)), "
    "(CAST(-1.0e16 AS DOUBLE)), "
    "(CAST(2.0 AS DOUBLE)), "
    "(CAST(1.0e16 AS DOUBLE)), "
    "(CAST(0.5 AS DOUBLE)), "
    "(CAST(-1.0e16 AS DOUBLE)), "
    "(CAST(0.25 AS DOUBLE))"
    ") AS t(v)"
)

SUM_SQL = f"SELECT sum(v) AS s FROM ({FIXTURE_VALUES_SQL}) src"
AVG_SQL = f"SELECT avg(v) AS a FROM ({FIXTURE_VALUES_SQL}) src"


# ==================================================================================================
# Arrow helpers
# ==================================================================================================


def _one_row_f64(name: str, value: float, *, nullable: bool) -> pa.Table:
    """One-column float64 table with exact value and nullability."""
    schema = pa.schema([pa.field(name, pa.float64(), nullable=nullable)])
    return pa.table({name: pa.array([value], type=pa.float64())}, schema=schema)


# ==================================================================================================
# Row shape
# ==================================================================================================


@dataclass(frozen=True)
class FloatAggRow:
    """One differential float-agg row: SQL recipe + recorded Spark half + optional repark half.

    ``repark is None`` and ``spark is not None`` → plain EQUALITY (``repark == Spark``).

    ``repark is not None`` and ``spark is not None`` → DISCLOSURE: repark's actual output is pinned
    and a convergence onto the recorded Spark output is detected and reported as one.

    Optional ``max_ulps``: when set, the row is a **declared-tolerance** equality — repark and
    Spark must share schema (name/type/nullability) and the float cell must be within ``max_ulps``
    ULP of the recorded Spark value. Use only when bit-equality fails honestly; never invent a
    tolerance to hide a type/nullability split.
    """

    name: str
    sql: str
    spark: pa.Table | None
    repark: pa.Table | None
    note: str
    max_ulps: int | None = None

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (no repark pin, no tolerance)."""
        return self.repark is None and self.spark is not None and self.max_ulps is None

    def is_disclosure(self) -> bool:
        """True when the row pins a known divergence (table disclosure)."""
        return self.repark is not None and self.spark is not None

    def is_tolerance(self) -> bool:
        """True when the row is a declared-ULP equality against the Spark golden."""
        return self.max_ulps is not None and self.spark is not None and self.repark is None


# ==================================================================================================
# Dual-engine recipe (shared with the record driver — one SSOT)
# ==================================================================================================


def run_row(session: object, row: FloatAggRow) -> pa.Table:
    """Execute the row's SQL and return the Arrow result (facade or Spark)."""
    frame = session.sql(row.sql)  # type: ignore[attr-defined]
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


def _ulp_distance(left: float, right: float) -> int:
    """IEEE-754 ULP distance between two finite floats (ordered sign-magnitude bit distance)."""
    import struct

    left_int = int.from_bytes(struct.pack(">d", left), "big")
    right_int = int.from_bytes(struct.pack(">d", right), "big")

    def _ordered(bits: int) -> int:
        if bits & (1 << 63):
            return -(bits & ((1 << 63) - 1))
        return bits

    return abs(_ordered(left_int) - _ordered(right_int))


def _single_f64(table: pa.Table) -> float:
    """Extract the sole non-null float64 cell."""
    assert table.num_rows == 1 and table.num_columns == 1
    column = table.column(0)
    assert column[0].is_valid
    return float(column[0].as_py())


# ==================================================================================================
# Rows — spark halves filled by the record driver; repark halves measured on the facade
# ==================================================================================================

# Recorded 2026-08-11 against live PySpark 4.1.2 (zulu-17, local[2], ANSI on, shuffle=2).
# Repark measured on the facade with spark.sql.shuffle.partitions=2 (→ target_partitions=2).
# Both engines answer float64 nullable; VALUES differ (Spark left-to-right loses small addends
# → 2.25; repark's accumulator on this single-partition VALUES source keeps 3.75). Honest
# disclosure — not a fudged equality. ULP distance is large (exact bit patterns differ by
# orders of magnitude of ULP); no declared-tolerance equality is honest here.
ROWS: list[FloatAggRow] = [
    FloatAggRow(
        name="sum_catastrophic_cancellation_fixture",
        sql=SUM_SQL,
        spark=_one_row_f64("s", 2.25, nullable=True),  # bits 0x4002000000000000
        repark=_one_row_f64("s", 3.75, nullable=True),  # bits 0x400e000000000000
        note=(
            "sum of the catastrophic-cancellation fixture: large ±1e16 interleaved with small "
            "addends. Spark (local[2], shuffle=2) lands 2.25; repark (shuffle.partitions=2) "
            "lands 3.75. Same Arrow type (float64 nullable); value diverges because "
            "accumulation order differs. The Rust half further discloses repark's own "
            f"cross-count spread at target_partitions=8. Flipped by {FIX_G7}."
        ),
    ),
    FloatAggRow(
        name="avg_catastrophic_cancellation_fixture",
        sql=AVG_SQL,
        spark=_one_row_f64("a", 0.28125, nullable=True),  # bits 0x3fd2000000000000
        repark=_one_row_f64("a", 0.46875, nullable=True),  # bits 0x3fde000000000000
        note=(
            "avg of the same fixture (sum/8). Spark 0.28125 vs repark 0.46875 — follows the "
            f"sum divergence bit-for-bit. Flipped by {FIX_G7}."
        ),
    ),
]


# ==================================================================================================
# Session + tests
# ==================================================================================================


def _repark_session() -> ReparkSession:
    """Facade session with target_partitions=2 (via spark.sql.shuffle.partitions)."""
    import repark

    return (
        repark.ReparkSession.builder.appName("float-agg-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def repark() -> ReparkSession:
    """Repark session for the facade door."""
    return _repark_session()


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_float_agg_parity_row(row: FloatAggRow, repark: ReparkSession) -> None:
    """Every recorded row on the Arrow path (value AND type AND nullability).

    Equality rows assert ``repark == Spark`` bit-exactly.

    Disclosure rows assert repark's pinned actual output — and when that assertion fails, the
    failure is CLASSIFIED (CONVERGED vs regression).

    Tolerance rows assert schema equality + ULP distance ≤ ``max_ulps`` against the Spark golden.
    """
    assert row.spark is not None
    actual = run_row(repark, row)

    if row.is_tolerance():
        assert row.max_ulps is not None
        actual_signature = [
            (field.name, str(field.type), field.nullable) for field in actual.schema
        ]
        spark_signature = [
            (field.name, str(field.type), field.nullable) for field in row.spark.schema
        ]
        assert actual_signature == spark_signature, (
            f"{row.name}: schema must match under declared tolerance "
            f"(actual={actual_signature} spark={spark_signature}). {row.note}"
        )
        distance = _ulp_distance(_single_f64(actual), _single_f64(row.spark))
        assert distance <= row.max_ulps, (
            f"{row.name}: ULP distance {distance} exceeds declared tolerance "
            f"max_ulps={row.max_ulps}. {row.note}"
        )
        return

    if row.repark is None:
        assert_frames_equal(actual, row.spark)
        return

    try:
        assert_frames_equal(actual, row.repark)
    except FrameMismatchError as mismatch:
        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED - repark now produces the RECORDED "
                f"SPARK output, so this disclosure is stale. Do not delete the row: flip it to an "
                f"equality row (repark=None) and record the convergence. {row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the recorded "
            f"Spark golden either - this is a regression, not a convergence. Re-derive both "
            f"halves in record mode (see this module's docstring) before touching the pin. "
            f"{row.note}"
        ) from mismatch

    assert _frames_differ(row.repark, row.spark), (
        f"{row.name}: disclosure halves are identical - flip to equality (repark=None). {row.note}"
    )


def test_float_agg_row_set_covers_g7_budget() -> None:
    """Budget + fixture coverage pins so the corpus cannot silently shrink or lose the recipe."""
    assert G7_BUDGET_MIN <= len(ROWS) <= G7_BUDGET_MAX, (
        f"G7 budget {G7_BUDGET_MIN}-{G7_BUDGET_MAX} differential rows (got {len(ROWS)})"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"
    names = {row.name for row in ROWS}
    assert any("sum" in name for name in names), "G7 must pin sum of the fixture"
    assert any("avg" in name for name in names), "G7 must pin avg of the fixture"
    for row in ROWS:
        assert "1.0e16" in row.sql or "1e16" in row.sql.lower() or "1.0E16" in row.sql, (
            f"{row.name}: recipe must carry the catastrophic-cancellation large magnitude"
        )
        assert row.spark is not None, f"{row.name}: spark half must be recorded"
        assert row.is_equality() or row.is_disclosure() or row.is_tolerance(), (
            f"{row.name}: row must be equality, disclosure, or declared-tolerance"
        )
