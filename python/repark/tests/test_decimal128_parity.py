"""Decimal128 differential corpus (gap G2) + expression overflow (gap G13) - G-7 Python half.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``) on 2026-08-10. One SQL string per row runs on BOTH engines, so
the recipe under test and the recipe the oracle ran are the same string - nothing here is
hand-computed.

**Why some rows are DISCLOSURES, not equalities.** Money and quantity columns are unpinned: Spark's
DECIMAL result precision/scale rules, the 38-digit clamp, decimal literal inference, and
``avg``/overflow semantics have never been differentially compared. Where repark and Spark already
agree (``+ - *`` result ``(p,s)`` on many operand pairs, ``sum`` of money, null propagation) the
row is a plain equality. Where they diverge the row pins BOTH halves:

* ``repark`` - repark's actual output today (value AND exact Arrow ``decimal128(p,s)`` or
  ``double``), and
* ``spark`` - the recorded live-Spark output it differs from,

and the row asserts that the two still differ. A row that silently CONVERGES goes RED and forces
the disclosure to be revisited rather than laundered into "parity" - the same discipline
``docs/testing.md`` puts on the live tier's disclosures. When a G2 fix lands, each divergent row
flips to ``repark=None`` (equality) and that flip is the fix's revert-red evidence.

**Raise-class rows (G13 overflow / ANSI divide-by-zero).** Some recipes RAISE on ANSI Spark and
return a value (or a plan error) on repark. Those rows set ``spark_raises`` / ``repark_raises`` to
the exception *class name substring* the record driver and the suite re-check; the table half on
the raising side is ``None``.

**Rows assert on the Arrow path** (``to_arrow``) through the parity comparator, so schema name,
Arrow type (including exact ``decimal128(p,s)``) and nullability are part of every assertion  -
never ``show``.

**CTAS write-back (Q1 ruling).** Three rows prove decimal type preservation through repark's own
write path (CTAS -> memory-catalog Iceberg -> read back). Spark is the arithmetic/type oracle on the
SELECT half only - exactly ``test_ctas_division_writeback.py``'s shape. No Iceberg-on-Spark.

**Re-deriving the goldens (record mode).** The driver that recorded every ``spark`` half is
committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_decimal128_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe, so the recorded golden and the
asserted recipe cannot drift apart. Needs a JVM + ``pyspark`` (``uv sync --extra record``); never
collected by pytest.

**Entry points.** Every differential row goes through the facade ``sql()`` door. The Rust bit-exact
``Decimal128`` fixture pins and the 2 cross-door rows are **G-7b** (deferred - collide with G-4's
``repark-spark/src/tests.rs`` ban / the ANSI door); declared in the unit ledger, not silent.

**In-flight fix named by every disclosure** so a red row points at what flips it.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.session import ReparkSession

# Named so every disclosure's note can cite the same future work without inventing per-row fix IDs.
FIX_G2 = (
    "the decimal128 result-type / literal-inference / avg-type fix "
    "(briefs/v2-engine-hardening.md, gap G2; DECLARE candidacy if the ruling is disclosure-only)"
)
FIX_G13 = (
    "the expression-level arithmetic overflow / ANSI divide-by-zero fix "
    "(briefs/v2-engine-hardening.md, gap G13; folds into G2's follow-on unit)"
)

# Budget floors/ceilings pinned by test_decimal128_row_set_covers_gap_budgets (not incidental).
G2_BUDGET_MIN = 20
G2_BUDGET_MAX = 26
G13_BUDGET_MIN = 6
G13_BUDGET_MAX = 8
CTAS_BUDGET = 3
# Corpus cannot degenerate to all-disclosures: at least this many plain equalities, and at most
# this many disclosures among the differential ROWS (CTAS counted separately).
MIN_EQUALITY_ROWS = 8
MAX_DISCLOSURE_ROWS = 20


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


def _one_row(fields: list[tuple[str, pa.DataType, bool]], values: dict[str, object]) -> pa.Table:
    """Build the single-row Arrow table a recorded golden describes."""
    return _table(fields, {name: [values[name]] for name, _, _ in fields})


def _dec(precision: int, scale: int, value: Decimal | None, *, nullable: bool = False) -> pa.Table:
    """One-column ``v`` table with exact ``decimal128(precision, scale)`` and the given value."""
    return _one_row(
        [("v", pa.decimal128(precision, scale), nullable)],
        {"v": value},
    )


def _f64(value: float, *, nullable: bool = False) -> pa.Table:
    """One-column ``v`` table with ``float64`` (repark's bare decimal-literal inference today)."""
    return _one_row([("v", pa.float64(), nullable)], {"v": value})


@dataclass(frozen=True)
class DecimalRow:
    """One differential row: a SQL string, the recorded live-Spark half, and repark's own.

    ``repark is None`` and ``spark is not None`` and no raise flags -> plain EQUALITY
    (``repark == Spark``).

    ``repark is not None`` and ``spark is not None`` -> DISCLOSURE: repark's actual output is pinned
    and a convergence onto the recorded Spark output is detected and reported as one.

    ``spark_raises`` / ``repark_raises`` mark ANSI raise-class rows (G13 overflow / divide-by-zero).
    The raising side's table is ``None``; the non-raising side pins its Arrow half.
    """

    name: str
    gap: str
    sql: str
    spark: pa.Table | None
    repark: pa.Table | None
    note: str
    spark_raises: str | None = None
    repark_raises: str | None = None

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (no raise flags, no repark pin)."""
        return (
            self.repark is None
            and self.spark is not None
            and self.spark_raises is None
            and self.repark_raises is None
        )

    def is_disclosure(self) -> bool:
        """True when the row pins a known divergence (table disclosure or raise-class split)."""
        return not self.is_equality()


@dataclass(frozen=True)
class CtasRow:
    """One CTAS write-back row: SELECT half + expected Arrow table after Iceberg round-trip."""

    name: str
    select_sql: str
    expected: pa.Table
    note: str
    # When set, the SELECT half is also a Spark equality oracle (value+type before write).
    # When None, the SELECT half is a repark-only type pin (Spark diverges on the SELECT).
    spark_select: pa.Table | None = None


# ==================================================================================================
# Gap G2 - decimal128 arithmetic bit-exactness (value AND exact decimal128(p,s))
# ==================================================================================================

G2_ROWS: list[DecimalRow] = [
    # ----- control equalities: + - * result (p,s) repark already matches Spark --------------------
    DecimalRow(
        "cast_decimal_identity",
        "G2",
        "SELECT CAST(1.23 AS DECIMAL(10,2)) AS v",
        _dec(10, 2, Decimal("1.23")),
        None,
        "control: an explicit CAST lands as decimal128(10,2) with the value intact on both "
        "engines.",
    ),
    DecimalRow(
        "add_same_precision_scale",
        "G2",
        "SELECT CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2)) AS v",
        _dec(11, 2, Decimal("5.79")),
        None,
        "Spark add rule p=max(p1-s1,p2-s2)+max(s1,s2)+1, s=max(s1,s2) -> (11,2); repark "
        "agrees today.",
    ),
    DecimalRow(
        "sub_same_precision_scale",
        "G2",
        "SELECT CAST(1.23 AS DECIMAL(10,2)) - CAST(4.56 AS DECIMAL(10,2)) AS v",
        _dec(11, 2, Decimal("-3.33")),
        None,
        "subtraction uses the same (p,s) rule as addition; value and type agree.",
    ),
    DecimalRow(
        "mul_same_precision_scale",
        "G2",
        "SELECT CAST(1.23 AS DECIMAL(10,2)) * CAST(4.56 AS DECIMAL(10,2)) AS v",
        _dec(21, 4, Decimal("5.6088")),
        None,
        "Spark mul rule p=p1+p2+1, s=s1+s2 -> (21,4); repark agrees on value and type.",
    ),
    DecimalRow(
        "add_carry_widens_precision",
        "G2",
        "SELECT CAST(99.99 AS DECIMAL(4,2)) + CAST(0.01 AS DECIMAL(4,2)) AS v",
        _dec(5, 2, Decimal("100.00")),
        None,
        "carry out of the integer digits widens p by one (4,2)+(4,2)->(5,2); money-safe control.",
    ),
    DecimalRow(
        "mul_mixed_scales",
        "G2",
        "SELECT CAST(1.2 AS DECIMAL(5,1)) * CAST(3.45 AS DECIMAL(5,2)) AS v",
        _dec(11, 3, Decimal("4.140")),
        None,
        "mixed-scale mul sums the scales; (5,1)*(5,2)->(11,3).",
    ),
    DecimalRow(
        "mul_money_by_quantity",
        "G2",
        "SELECT CAST(19.99 AS DECIMAL(10,2)) * CAST(3 AS DECIMAL(10,0)) AS v",
        _dec(21, 2, Decimal("59.97")),
        None,
        "classic money x qty: (10,2)*(10,0)->(21,2) with exact 59.97 - a control a shopping cart "
        "depends on.",
    ),
    DecimalRow(
        "mul_money_by_tax_rate",
        "G2",
        "SELECT CAST(100.00 AS DECIMAL(10,2)) * CAST(0.0825 AS DECIMAL(6,4)) AS v",
        _dec(17, 6, Decimal("8.250000")),
        None,
        "money x tax rate: (10,2)*(6,4)->(17,6); fractional cents stay exact.",
    ),
    DecimalRow(
        "mul_38_0_identity",
        "G2",
        "SELECT CAST(1 AS DECIMAL(38,0)) * CAST(1 AS DECIMAL(38,0)) AS v",
        _dec(38, 0, Decimal("1")),
        None,
        "full-width integer multiply that stays inside the 38-digit clamp without scale loss.",
    ),
    DecimalRow(
        "sum_two_money_values",
        "G2",
        "SELECT sum(x) AS v FROM (SELECT CAST(1.10 AS DECIMAL(10,2)) AS x "
        "UNION ALL SELECT CAST(2.20 AS DECIMAL(10,2))) t",
        _dec(20, 2, Decimal("3.30"), nullable=True),
        None,
        "sum of DECIMAL(10,2) promotes to decimal128(20,2) nullable (Spark aggregate nullability) "
        "with exact 3.30 on both engines.",
    ),
    DecimalRow(
        "null_plus_money_propagates",
        "G2",
        "SELECT CAST(NULL AS DECIMAL(10,2)) + CAST(1.00 AS DECIMAL(10,2)) AS v",
        _dec(11, 2, None, nullable=True),
        None,
        "NULL + money -> NULL at the add result type (11,2); null propagation control.",
    ),
    DecimalRow(
        "mul_negative_money",
        "G2",
        "SELECT CAST(-1.23 AS DECIMAL(10,2)) * CAST(4.56 AS DECIMAL(10,2)) AS v",
        _dec(21, 4, Decimal("-5.6088")),
        None,
        "signed multiply keeps the (21,4) result type; sign handling is not the gap.",
    ),
    # ----- disclosures: literal inference ---------------------------------------------------------
    DecimalRow(
        "literal_1_23_infers_decimal_in_spark_double_in_repark",
        "G2",
        "SELECT 1.23 AS v",
        _dec(3, 2, Decimal("1.23")),
        _f64(1.23),
        "bare decimal literal: Spark infers DECIMAL(3,2) -> decimal128(3,2); repark infers double. "
        f"A money column written from a literal is the wrong Arrow type. Flipped by {FIX_G2}.",
    ),
    DecimalRow(
        "literal_0_1_infers_decimal_in_spark_double_in_repark",
        "G2",
        "SELECT 0.1 AS v",
        _dec(1, 1, Decimal("0.1")),
        _f64(0.1),
        "the classic binary-float landmine as a type divergence: Spark keeps DECIMAL(1,1); repark "
        f"answers float64 0.1. Flipped by {FIX_G2}.",
    ),
    DecimalRow(
        "literal_123_456_infers_decimal_in_spark_double_in_repark",
        "G2",
        "SELECT 123.456 AS v",
        _dec(6, 3, Decimal("123.456")),
        _f64(123.456),
        "three fractional digits of literal inference: Spark DECIMAL(6,3); repark double. "
        f"Flipped by {FIX_G2}.",
    ),
    # ----- disclosures: division result (p,s) -----------------------------------------------------
    DecimalRow(
        "div_same_precision_scale",
        "G2",
        "SELECT CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2)) AS v",
        _dec(23, 13, Decimal("0.2697368421053"), nullable=True),
        _dec(16, 6, Decimal("0.269736"), nullable=True),
        "Spark division (p,s) is far wider (23,13) and keeps more fractional digits; repark lands "
        f"(16,6) with a rounded 0.269736. Value AND type diverge. Flipped by {FIX_G2}.",
    ),
    DecimalRow(
        "div_repeating_money",
        "G2",
        "SELECT CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS v",
        _dec(23, 13, Decimal("3.3333333333333"), nullable=True),
        _dec(16, 6, Decimal("3.333333"), nullable=True),
        "repeating money division: Spark keeps 13 fractional digits at (23,13); repark six at "
        f"(16,6). A unit-price split is silently short. Flipped by {FIX_G2}.",
    ),
    DecimalRow(
        "div_integer_scales",
        "G2",
        "SELECT CAST(1 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)) AS v",
        _dec(21, 11, Decimal("0.33333333333"), nullable=True),
        _dec(14, 4, Decimal("0.3333"), nullable=True),
        "integer-scale division still produces a fractional result type; Spark (21,11) vs repark "
        f"(14,4). Flipped by {FIX_G2}.",
    ),
    DecimalRow(
        "div_exact_half_type_only",
        "G2",
        "SELECT CAST(5.00 AS DECIMAL(10,2)) / CAST(2.00 AS DECIMAL(10,2)) AS v",
        _dec(23, 13, Decimal("2.5000000000000"), nullable=True),
        _dec(16, 6, Decimal("2.500000"), nullable=True),
        "exact half: the VALUE is 2.5 on both engines but the result type still diverges "
        f"((23,13) vs (16,6)), so a schema-sensitive consumer is wrong. Flipped by {FIX_G2}.",
    ),
    # ----- disclosures: 38-digit clamp ------------------------------------------------------------
    DecimalRow(
        "mul_38_10_clamps_scale_in_spark",
        "G2",
        "SELECT CAST(1 AS DECIMAL(38,10)) * CAST(1 AS DECIMAL(38,10)) AS v",
        _dec(38, 6, Decimal("1.000000")),
        _dec(38, 20, Decimal("1.00000000000000000000")),
        "38-digit clamp on multiply: Spark reduces scale to keep p<=38 -> decimal128(38,6); repark "
        f"keeps s1+s2=20 -> decimal128(38,20). A high-scale product is the wrong width. {FIX_G2}.",
    ),
    DecimalRow(
        "add_38_18_clamps_scale_in_spark",
        "G2",
        "SELECT CAST(1 AS DECIMAL(38,18)) + CAST(1 AS DECIMAL(38,18)) AS v",
        _dec(38, 17, Decimal("2.00000000000000000")),
        _dec(38, 18, Decimal("2.000000000000000000")),
        "38-digit clamp on add: Spark drops one scale digit -> (38,17); repark keeps (38,18). "
        f"Flipped by {FIX_G2}.",
    ),
    DecimalRow(
        "add_38_10_clamps_scale_in_spark",
        "G2",
        "SELECT CAST(1 AS DECIMAL(38,10)) + CAST(1 AS DECIMAL(38,10)) AS v",
        _dec(38, 9, Decimal("2.000000000")),
        _dec(38, 10, Decimal("2.0000000000")),
        f"same clamp class at scale 10: Spark (38,9) vs repark (38,10). Flipped by {FIX_G2}.",
    ),
    # ----- disclosures: avg type + int/decimal promotion ------------------------------------------
    DecimalRow(
        "avg_money_stays_decimal_in_spark_double_in_repark",
        "G2",
        "SELECT avg(x) AS v FROM (SELECT CAST(1.10 AS DECIMAL(10,2)) AS x "
        "UNION ALL SELECT CAST(2.20 AS DECIMAL(10,2))) t",
        _dec(14, 6, Decimal("1.650000"), nullable=True),
        None,
        "avg of DECIMAL(10,2): both engines keep decimal128(14,6) exact 1.650000 nullable "
        "(Spark Average +4; Z-3 U1 / DEC-5 stopped the facade Float64 overwrite). Name kept "
        "so existing registry citations still resolve.",
    ),
    DecimalRow(
        "int_times_decimal_promotes_wider_in_repark",
        "G2",
        "SELECT 5 * CAST(1.50 AS DECIMAL(10,2)) AS v",
        _dec(12, 2, Decimal("7.50"), nullable=True),
        _dec(31, 2, Decimal("7.50")),
        "INT * DECIMAL: value agrees (7.50) but Spark lands decimal128(12,2) nullable while repark "
        f"lands decimal128(31,2) non-null - a schema-level money divergence. Flipped by "
        f"{FIX_G2}.",
    ),
]


# ==================================================================================================
# Gap G13 - expression-level arithmetic overflow (folded into this corpus)
# ==================================================================================================

G13_ROWS: list[DecimalRow] = [
    DecimalRow(
        "overflow_max_decimal38_plus_one_raises_in_spark",
        "G13",
        "SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) "
        "+ CAST(1 AS DECIMAL(38,0)) AS v",
        None,
        # repark currently returns a corrupted integer (float-path residue of the big literal).
        _dec(38, 0, Decimal("99999999999999997748809823456034029569")),
        "ANSI Spark raises NUMERIC_VALUE_OUT_OF_RANGE for max DECIMAL(38,0)+1; repark returns a "
        f"wrong 38-digit value (no raise). Flipped by {FIX_G13}.",
        spark_raises="ArithmeticException",
    ),
    DecimalRow(
        "div_by_zero_decimal38_raises_in_spark_null_in_repark",
        "G13",
        "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(0 AS DECIMAL(38,0)) AS v",
        None,
        _dec(38, 4, None, nullable=True),
        "ANSI Spark raises DIVIDE_BY_ZERO; repark returns NULL at decimal128(38,4). Flipped by "
        f"{FIX_G13}.",
        spark_raises="ArithmeticException",
    ),
    DecimalRow(
        "div_by_zero_small_decimal_raises_in_spark_null_in_repark",
        "G13",
        "SELECT CAST(10 AS DECIMAL(2,0)) / CAST(0 AS DECIMAL(2,0)) AS v",
        None,
        _dec(6, 4, None, nullable=True),
        "same ANSI divide-by-zero class at small precision; repark NULL at (6,4). Flipped by "
        f"{FIX_G13}.",
        spark_raises="ArithmeticException",
    ),
    DecimalRow(
        "mul_38_20_plans_in_spark_refuses_in_repark",
        "G13",
        "SELECT CAST(1 AS DECIMAL(38,20)) * CAST(1 AS DECIMAL(38,20)) AS v",
        _dec(38, 6, Decimal("1.000000")),
        None,
        "Spark clamps the product to decimal128(38,6) and succeeds; repark refuses at plan time "
        f"(Cannot get result type for decimal operation … 38,20 * 38,20). Flipped by {FIX_G13}.",
        repark_raises="AnalysisException",
    ),
    DecimalRow(
        "mul_single_digit_nullability_differs",
        "G13",
        "SELECT CAST(9 AS DECIMAL(1,0)) * CAST(9 AS DECIMAL(1,0)) AS v",
        _dec(3, 0, Decimal("81"), nullable=True),
        _dec(3, 0, Decimal("81")),
        "value 81 and type (3,0) agree, but Spark marks the result nullable (overflow-capable "
        f"binary arithmetic) while repark marks it non-null. A nullability-only pin. {FIX_G13}.",
    ),
    DecimalRow(
        "add_single_digit_nullability_differs",
        "G13",
        "SELECT CAST(9 AS DECIMAL(1,0)) + CAST(9 AS DECIMAL(1,0)) AS v",
        _dec(2, 0, Decimal("18"), nullable=True),
        _dec(2, 0, Decimal("18")),
        "same nullability class on add: value 18 at (2,0) agrees; Spark nullable, repark not. "
        f"{FIX_G13}.",
    ),
    DecimalRow(
        "mul_three_digit_capacity_nullability_differs",
        "G13",
        "SELECT CAST(999 AS DECIMAL(3,0)) * CAST(999 AS DECIMAL(3,0)) AS v",
        _dec(7, 0, Decimal("998001"), nullable=True),
        _dec(7, 0, Decimal("998001")),
        "near-capacity multiply: value 998001 at (7,0) agrees; nullability still diverges. "
        f"{FIX_G13}.",
    ),
]


ROWS: list[DecimalRow] = [*G2_ROWS, *G13_ROWS]


# ==================================================================================================
# CTAS write-back (Q1: repark-only write path; Spark is SELECT oracle when equality holds)
# ==================================================================================================

CTAS_NAMESPACE = "glue_catalog.decimal_ns"

CTAS_ROWS: list[CtasRow] = [
    CtasRow(
        "ctas_add_money_preserves_decimal128",
        "SELECT CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2)) AS q",
        _one_row([("q", pa.decimal128(11, 2), False)], {"q": Decimal("5.79")}),
        "CTAS of an equality-class add: decimal128(11,2) value 5.79 survives Iceberg write+read.",
        spark_select=_one_row([("q", pa.decimal128(11, 2), False)], {"q": Decimal("5.79")}),
    ),
    CtasRow(
        "ctas_mul_money_qty_preserves_decimal128",
        "SELECT CAST(19.99 AS DECIMAL(10,2)) * CAST(3 AS DECIMAL(10,0)) AS q",
        _one_row([("q", pa.decimal128(21, 2), False)], {"q": Decimal("59.97")}),
        "CTAS of money x qty: decimal128(21,2) 59.97 preserved end to end.",
        spark_select=_one_row([("q", pa.decimal128(21, 2), False)], {"q": Decimal("59.97")}),
    ),
    CtasRow(
        "ctas_div_preserves_repark_result_type",
        "SELECT CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS q",
        _one_row(
            [("q", pa.decimal128(16, 6), True)],
            {"q": Decimal("3.333333")},
        ),
        "CTAS of a division DISCLOSURE path: repark's (16,6) result type is what is written and "
        "read back - type preservation through Iceberg, even while the SELECT half still "
        f"diverges from Spark's (23,13). Flipped to Spark equality by {FIX_G2}.",
        # Spark SELECT half would be (23,13); deliberately not required equal here (Q1).
        spark_select=None,
    ),
]


# ==================================================================================================
# Helpers
# ==================================================================================================


def _session() -> ReparkSession:
    """A plain repark session for decimal SQL (no zone knob - decimals are zone-independent)."""
    import repark

    return repark.ReparkSession.builder.appName("decimal128-parity").getOrCreate()


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


def _exception_name(exc: BaseException) -> str:
    """Stable class-name string for raise-class matching (Spark wraps; match on type name)."""
    return type(exc).__name__


def _matches_raise(exc: BaseException, expected_substring: str) -> bool:
    """True when the exception class name or its MRO contains the recorded raise token."""
    names = [type(exc).__name__, *[base.__name__ for base in type(exc).mro()]]
    # PySpark surfaces JVM ArithmeticException as pyspark.errors.ArithmeticException; the string
    # "ArithmeticException" is the token the corpus pins. Also match message for wrapped forms.
    if any(expected_substring in name for name in names):
        return True
    return expected_substring in str(exc)


def run_row(row: DecimalRow, session: object) -> pa.Table:
    """Run one row's recipe on a session (either engine) and return its Arrow output.

    Shared with the record driver so the recipe the oracle ran and the recipe asserted here are
    the same code, not two copies of one string. Callers that expect a raise must catch around
    this helper (raise-class rows do not return a table).
    """
    frame = session.sql(row.sql)  # type: ignore[attr-defined]
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


# ==================================================================================================
# The differential rows
# ==================================================================================================


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_decimal128_row_matches_spark_or_still_diverges(row: DecimalRow) -> None:
    """Every recorded row, on the Arrow path (value AND exact Arrow type AND nullability).

    Equality rows assert ``repark == Spark``.

    Disclosure rows assert repark's pinned actual output - and when that assertion fails, the
    failure is CLASSIFIED before it is raised: CONVERGED (flip-don't-delete) vs regression
    (re-derive both halves). Raise-class rows assert the raising side still raises the recorded
    exception class and the non-raising side still matches its pinned table.
    """
    session = _session()

    # ----- raise-class rows (G13 overflow / ANSI divide-by-zero) ---------------------------------
    if row.repark_raises is not None:
        with pytest.raises(Exception) as excinfo:
            run_row(row, session)
        assert _matches_raise(excinfo.value, row.repark_raises), (
            f"{row.name}: repark was expected to raise matching {row.repark_raises!r}, got "
            f"{_exception_name(excinfo.value)}: {excinfo.value!s:.200}. {row.note}"
        )
        # Spark half is a successful table for the plan-refuse row; well-formedness only.
        if row.spark is not None and row.repark is not None:
            assert _frames_differ(row.repark, row.spark), (
                f"{row.name}: both halves identical on a raise-class disclosure - re-record. "
                f"{row.note}"
            )
        return

    if row.spark_raises is not None:
        # Spark raises; repark must still match its pinned table (and NOT silently start raising
        # the same way without the pin being flipped - a new raise is a convergence of kind, so
        # classify it).
        try:
            actual = run_row(row, session)
        except Exception as exc:
            if _matches_raise(exc, row.spark_raises):
                raise AssertionError(
                    f"{row.name}: repark and Spark have CONVERGED on the RAISE - repark now "
                    f"raises {row.spark_raises} too. Flip this disclosure to a shared-raise "
                    f"equality (or drop the repark pin) and record the convergence. {row.note}"
                ) from exc
            raise AssertionError(
                f"{row.name}: repark raised {_exception_name(exc)} instead of producing its "
                f"pinned table (and the raise is not the Spark {row.spark_raises} either) - "
                f"regression. Re-derive. {row.note}"
            ) from exc

        assert row.repark is not None, f"{row.name}: spark_raises row must pin a repark table"
        try:
            assert_frames_equal(actual, row.repark)
        except FrameMismatchError as mismatch:
            raise AssertionError(
                f"{row.name}: repark moved OFF its pinned disclosure for a spark_raises row - "
                f"regression. Re-derive both halves in record mode. {row.note}"
            ) from mismatch
        return

    # ----- table rows (equality or disclosure) ---------------------------------------------------
    actual = run_row(row, session)

    if row.repark is None:
        assert row.spark is not None, f"{row.name}: equality row needs a spark golden"
        assert_frames_equal(actual, row.spark)
        return

    assert row.spark is not None, f"{row.name}: table disclosure needs both halves"
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
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure at "
        f"all - either it converged and was half-edited, or the Spark half was pasted over the "
        f"repark half. Flip it to an equality row (repark=None) or re-record it. {row.note}"
    )


def test_decimal128_row_set_covers_gap_budgets() -> None:
    """The pin budget is part of the unit, so the corpus size and shape are pinned, not incidental.

    G2's budget is 20-26 differential rows; G13's is 6-8. At least :data:`MIN_EQUALITY_ROWS`
    plain equalities keep the corpus from degenerating into all-disclosures, and at most
    :data:`MAX_DISCLOSURE_ROWS` disclosures keep a future edit from turning every control red into
    a silent disclosure. CTAS is a separate budget of exactly 3.
    """
    g2 = [row for row in ROWS if row.gap == "G2"]
    g13 = [row for row in ROWS if row.gap == "G13"]
    assert G2_BUDGET_MIN <= len(g2) <= G2_BUDGET_MAX, (
        f"G2 budget {G2_BUDGET_MIN}-{G2_BUDGET_MAX}, got {len(g2)}"
    )
    assert G13_BUDGET_MIN <= len(g13) <= G13_BUDGET_MAX, (
        f"G13 budget {G13_BUDGET_MIN}-{G13_BUDGET_MAX}, got {len(g13)}"
    )
    assert len(CTAS_ROWS) == CTAS_BUDGET, f"CTAS budget {CTAS_BUDGET}, got {len(CTAS_ROWS)}"
    assert len({row.name for row in ROWS}) == len(ROWS), "differential row names are unique"
    assert len({row.name for row in CTAS_ROWS}) == len(CTAS_ROWS), "CTAS row names are unique"
    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure()]
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"at least {MIN_EQUALITY_ROWS} control equality rows required so the corpus cannot "
        f"degenerate to all-disclosures; got {len(equalities)}"
    )
    assert len(disclosures) <= MAX_DISCLOSURE_ROWS, (
        f"at most {MAX_DISCLOSURE_ROWS} disclosures so the corpus cannot silently absorb every "
        f"regression as a new disclosure; got {len(disclosures)}"
    )
    # Class coverage pins - the brief names these surfaces explicitly.
    assert any(
        row.sql.strip().startswith("SELECT 1.23") or "SELECT 0.1" in row.sql for row in g2
    ), "G2 must pin bare decimal literal inference"
    assert any("/" in row.sql and "CAST" in row.sql for row in g2), (
        "G2 must pin division result (p,s)"
    )
    # Name-gated clamp family: a DECIMAL(38,...) equality control (e.g. mul_38_0_identity)
    # must NOT satisfy this pin — deleting the clamp disclosures has to go red.
    clamp_family = [row for row in g2 if "clamps_scale_in_spark" in row.name]
    assert len(clamp_family) >= 3, (
        "G2 must keep the 38-digit clamp disclosure family "
        f"(>=3 rows named *clamps_scale_in_spark); got {len(clamp_family)}. "
        "A DECIMAL(38,...) equality control alone does not satisfy this."
    )
    assert any("avg(" in row.sql.lower() for row in g2), "G2 must pin avg result type"
    assert any("sum(" in row.sql.lower() for row in g2), "G2 must pin sum result type"
    assert any(row.spark_raises for row in g13), "G13 must pin at least one ANSI raise"
    assert any(row.repark_raises for row in g13), "G13 must pin at least one repark plan refuse"
    assert any(row.spark_select is not None for row in CTAS_ROWS), (
        "at least one CTAS row must carry a Spark SELECT oracle (equality-path write-back)"
    )
    assert any(row.spark_select is None for row in CTAS_ROWS), (
        "at least one CTAS row must be repark-only (disclosure-path type preservation, Q1)"
    )
    # Row-shape well-formedness: raise flags and table halves cannot contradict each other.
    for row in ROWS:
        assert not (row.spark_raises and row.repark_raises), (
            f"{row.name}: a row cannot pin both engines raising "
            f"(no shared-raise equality shape yet)"
        )
        if row.spark_raises is not None:
            assert row.spark is None, f"{row.name}: spark_raises forbids a spark table half"
            assert row.repark is not None, f"{row.name}: spark_raises requires a repark table pin"
        if row.repark_raises is not None:
            assert row.repark is None, f"{row.name}: repark_raises forbids a repark table half"
            assert row.spark is not None, f"{row.name}: repark_raises requires a spark table pin"
        if row.is_equality():
            assert row.spark is not None and row.repark is None


# ==================================================================================================
# CTAS write-back
# ==================================================================================================


@pytest.fixture
def ctas_session(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog + namespace (local, AWS-free)."""
    import repark

    session = repark.ReparkSession.builder.appName("pytest-ctas-decimal").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql(f"CREATE NAMESPACE {CTAS_NAMESPACE}")
    return session


def _ctas_writeback(session: ReparkSession, table: str, select_sql: str) -> pa.Table:
    """CTAS ``select_sql`` into ``table`` and read column ``q`` back on the Arrow path."""
    session.sql(f"CREATE TABLE {CTAS_NAMESPACE}.{table} AS {select_sql}")
    return session.sql(f"SELECT q FROM {CTAS_NAMESPACE}.{table}").to_arrow()


@pytest.mark.parametrize("row", CTAS_ROWS, ids=[row.name for row in CTAS_ROWS])
def test_ctas_decimal_type_preserved(row: CtasRow, ctas_session: ReparkSession) -> None:
    """CTAS -> memory Iceberg -> read back: decimal128(p,s) intact on the Arrow path.

    When ``spark_select`` is set, it is the recorded Spark SELECT oracle and MUST equal
    ``expected`` (well-formedness: the write-back pin and the Spark oracle cannot drift apart
    in the module constants). The live repark SELECT is then asserted equal to that shared
    table before and after the Iceberg round-trip. When ``spark_select`` is None, the SELECT
    half is a disclosure-class expression and only repark's own write-path preservation is
    asserted (Q1 ruling: no Iceberg-on-Spark).
    """
    if row.spark_select is not None:
        # Well-formedness: the CTAS expected table and the Spark SELECT oracle are one pin.
        assert_frames_equal(row.expected, row.spark_select)

    # Pre-write SELECT half (repark).
    select_frame = ctas_session.sql(row.select_sql)
    select_arrow = select_frame.to_arrow()
    # The expected table is the post-read shape; SELECT and post-read must agree on type+value.
    assert_frames_equal(select_arrow, row.expected)

    written = _ctas_writeback(ctas_session, row.name, row.select_sql)
    assert_frames_equal(written, row.expected)
