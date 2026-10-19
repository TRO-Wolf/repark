"""Cast-failure semantics differential corpus (H-2 gap G6) vs live Spark 4.1.2 ANSI ON.

Every Spark half / error needle was recorded in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``, ``spark.sql.session.timeZone=UTC``). One recipe per
row runs on BOTH engines — nothing here is hand-computed.

Premise (binding): under ANSI ON, Spark raises on the same malformed / overflow casts
repark raises on, so the corpus is mostly shared-raise error and try_cast NULL
equalities, with a small number of true divergences. The budget test bounds
disclosures so regressions cannot be absorbed as new ones.

Rows assert on the Arrow path (``to_arrow`` / Spark ``toArrow``) — value AND Arrow type
AND nullability, never ``show``. Entry points: facade ``sql()`` primary, plus a
DataFrame-API ``Column.cast(...)`` error row (CP-11). Row-kind contract: ``CastRow``.

Re-deriving the goldens (record mode)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_cast_failure_goldens.py

Hold ``/tmp/grok-jvm-record.lock`` around the process (B4). Needs a JVM + ``pyspark``
(``uv sync --extra record``); never collected by pytest; CI stays JVM-free.
"""

from __future__ import annotations

import contextlib
import datetime
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Budget floors/ceilings — pinned by test_cast_failure_row_set_covers_g6_budget.

G6_BUDGET_MIN = 8
# Ceiling 15 (was 10): the DATE-INT gate closed five doors at once — the pinned CAST(DATE AS
# INT) row, four measured doors, and the reverse (G63-DATE-INT-DESIGN.md §4.2 / §1.3).
# Ratchet back down if any door is retired.
G6_BUDGET_MAX = 15
MIN_EQUALITY_ROWS = 3  # content equalities + shared-raise error rows count as equality-class
MIN_ERROR_ROWS = 3  # shared-raise error equalities
MIN_TRY_CAST_ROWS = 2  # try_cast twin of ≥2 failing casts (name-gated *try_cast*)
MIN_DF_API_ROWS = 1  # CP-11 Column.cast door
# Name-gated families so a control cannot satisfy them (CP-2 tautological-pin lesson).
MIN_MALFORMED_NUMERIC = 1  # *malformed_string*to_int* / *malformed_string_to_int*
MIN_MALFORMED_TEMPORAL = 1  # *malformed_string_to_date* / *timestamp*
MIN_NUMERIC_OVERFLOW = 1  # *overflow* / *decimal_narrowing_overflow*
# True content/split disclosures found under ANSI ON — do not invent more (brief A3 / item 6).
MAX_DISCLOSURE_OR_SPLIT_ROWS = 4

# The Spark class both engines raise for every DATE ↔ INT cast door, recorded verbatim
# against live PySpark 4.1.2 ANSI; repark emits the same class, so one needle pins both halves.
_DATE_INT_NEEDLE = "DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION"

FIX_G6 = (
    "the cast-failure semantics fix "
    "(briefs/v2-engine-hardening.md, gap G6; registry BL-1 moves with the unit that lands it)"
)


# Arrow helpers


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


def _one_row(fields: list[tuple[str, pa.DataType, bool]], values: dict[str, object]) -> pa.Table:
    """Build the single-row Arrow table a recorded golden describes."""
    return _table(fields, {name: [values[name]] for name, _, _ in fields})


_I32 = pa.int32()
_I8 = pa.int8()


# Row shape


@dataclass(frozen=True)
class CastRow:
    """One differential cast row: a recipe + recorded Spark half + optional repark half.

    ``kind="content"``: Arrow result; ``repark is None`` → equality, else disclosure.
    ``kind="error"``: both engines refuse; pins each engine's error needle.
    ``kind="split"``: one engine succeeds, the other refuses; ``which_raises`` picks which
    side pins the golden vs the needle. A refuse side that starts succeeding is classified
    CONVERGED vs regression.
    ``entry="sql"`` runs ``session.sql(sql)``; ``df_cast`` / ``df_try_cast`` run the CP-11
    ``Column.cast`` / ``Column.try_cast`` door.
    """

    name: str
    kind: Literal["content", "error", "split"]
    entry: Literal["sql", "df_cast", "df_try_cast"]
    family: str
    note: str
    # SQL path
    sql: str | None = None
    # DF path
    df_rows: list[tuple[object, ...]] | None = None
    df_columns: list[str] | None = None
    cast_column: str | None = None
    cast_type: str | None = None
    result_alias: str = "n"
    # Recorded halves
    spark: pa.Table | None = None
    repark: pa.Table | None = None
    spark_error_needle: str | None = None
    repark_error_needle: str | None = None
    which_raises: Literal["spark", "repark"] | None = None

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (content equality or shared-raise)."""
        if self.kind == "error":
            return True
        return self.kind == "content" and self.repark is None and self.spark is not None

    def is_disclosure_or_split(self) -> bool:
        """True when the row pins a known divergence (table disclosure or split)."""
        if self.kind == "split":
            return True
        return self.kind == "content" and self.repark is not None


# Lifecycle helpers (recipe SSOT the record driver imports)


def _functions_for(session: Any) -> Any:
    """Return the Column-functions module matching the session engine (Spark or repark)."""
    module_name = type(session).__module__
    if module_name.startswith("pyspark"):
        from pyspark.sql import functions as spark_functions

        return spark_functions
    from repark.spark.sql import functions as repark_functions

    return repark_functions


def run_cast_content(session: Any, row: CastRow) -> pa.Table:
    """Execute the row's recipe and return the Arrow result (facade or Spark)."""
    if row.entry == "sql":
        assert row.sql is not None
        frame = session.sql(row.sql)
        to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
        return to_arrow()  # type: ignore[no-any-return]

    assert row.df_rows is not None and row.df_columns is not None
    assert row.cast_column is not None and row.cast_type is not None
    functions = _functions_for(session)
    frame = session.createDataFrame(row.df_rows, row.df_columns)
    column = functions.col(row.cast_column)
    if row.entry == "df_cast":
        selected = frame.select(column.cast(row.cast_type).alias(row.result_alias))
    else:
        selected = frame.select(column.try_cast(row.cast_type).alias(row.result_alias))
    to_arrow = getattr(selected, "to_arrow", None) or selected.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def run_cast_expect_error(session: Any, row: CastRow) -> str:
    """Run the recipe expecting a raise; return the error message text."""
    try:
        _ = run_cast_content(session, row)
    except Exception as exc:  # both engines' error types; message is the pin
        return str(exc)
    raise AssertionError(f"{row.name}: expected recipe to raise, but it returned a table")


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


def _needle_in(message: str, needle: str) -> bool:
    """True when the recorded error token appears in the exception message (substring match)."""
    return needle in message


# The corpus (gap G6)

ROWS: list[CastRow] = [
    # shared-raise error equalities (ANSI ON: both engines raise)
    CastRow(
        name="malformed_string_to_int_both_raise",
        kind="error",
        entry="sql",
        family="malformed_string_numeric",
        sql="SELECT CAST(a AS INT) AS n FROM (VALUES ('abc')) AS t(a)",
        spark_error_needle="CAST_INVALID_INPUT",
        repark_error_needle="Cast error",
        note=(
            "ANSI ON equality: malformed string→INT raises on BOTH engines. Spark surface is "
            "CAST_INVALID_INPUT / NumberFormatException; repark surfaces Arrow Cast error via "
            f"PySparkException. Under non-ANSI Spark this used to be NULL (BL-1). {FIX_G6}."
        ),
    ),
    CastRow(
        name="malformed_string_to_date_both_raise",
        kind="error",
        entry="sql",
        family="malformed_string_temporal",
        sql="SELECT CAST(a AS DATE) AS n FROM (VALUES ('not-a-date')) AS t(a)",
        spark_error_needle="CAST_INVALID_INPUT",
        repark_error_needle="Cast error",
        note=(
            "ANSI ON equality: malformed string→DATE raises on both engines "
            f"(Spark DateTimeException / CAST_INVALID_INPUT; repark Arrow Cast error). {FIX_G6}."
        ),
    ),
    CastRow(
        name="overflow_int_to_tinyint_both_raise",
        kind="error",
        entry="sql",
        family="numeric_overflow",
        sql="SELECT CAST(a AS TINYINT) AS n FROM (VALUES (200)) AS t(a)",
        spark_error_needle="CAST_OVERFLOW",
        repark_error_needle="Cast error",
        note=(
            "ANSI ON equality: INT 200 → TINYINT overflows on both engines (Spark CAST_OVERFLOW / "
            f"ArithmeticException; repark Arrow Cast error). {FIX_G6}."
        ),
    ),
    CastRow(
        name="decimal_narrowing_overflow_both_raise",
        kind="error",
        entry="sql",
        family="numeric_overflow",
        sql="SELECT CAST(a AS DECIMAL(3,2)) AS n FROM (VALUES (123.45)) AS t(a)",
        spark_error_needle="NUMERIC_VALUE_OUT_OF_RANGE",
        repark_error_needle="too large to store",
        note=(
            "ANSI ON equality: decimal narrowing that cannot fit DECIMAL(3,2) raises on both "
            "(Spark NUMERIC_VALUE_OUT_OF_RANGE; repark Arrow 'too large to store'). Distinct from "
            f"scale-only rounding which succeeds on both. {FIX_G6}."
        ),
    ),
    # try_cast twins of failing casts (NULL equality)
    CastRow(
        name="try_cast_malformed_string_to_int_null",
        kind="content",
        entry="sql",
        family="try_cast",
        sql="SELECT try_cast(a AS INT) AS n FROM (VALUES ('abc')) AS t(a)",
        spark=_one_row([("n", _I32, True)], {"n": None}),
        repark=None,
        note=(
            "try_cast twin of malformed_string_to_int: both engines yield NULL at int32 nullable. "
            "The soft-cast door is the honest migration path off ANSI raise-on-bad-cast."
        ),
    ),
    CastRow(
        name="try_cast_overflow_tinyint_null",
        kind="content",
        entry="sql",
        family="try_cast",
        sql="SELECT try_cast(a AS TINYINT) AS n FROM (VALUES (200)) AS t(a)",
        spark=_one_row([("n", _I8, True)], {"n": None}),
        repark=None,
        note=(
            "try_cast twin of overflow_int_to_tinyint: both engines yield NULL at int8 nullable "
            "instead of CAST_OVERFLOW."
        ),
    ),
    # control equality
    CastRow(
        name="valid_string_to_int_control",
        kind="content",
        entry="sql",
        family="control",
        sql="SELECT CAST(a AS INT) AS n FROM (VALUES ('42')) AS t(a)",
        spark=_one_row([("n", _I32, True)], {"n": 42}),
        repark=None,
        note=(
            "control equality: well-formed string→INT succeeds on both engines with value 42 and "
            "Arrow int32 (VALUES path nullability matches)."
        ),
    ),
    # DataFrame-API door (CP-11)
    CastRow(
        name="df_cast_malformed_string_to_int_both_raise",
        kind="error",
        entry="df_cast",
        family="df_api",
        df_rows=[("abc",)],
        df_columns=["v"],
        cast_column="v",
        cast_type="int",
        spark_error_needle="CAST_INVALID_INPUT",
        repark_error_needle="Cast error",
        note=(
            "CP-11 DataFrame-API twin of malformed_string_to_int: Column.cast('int') on 'abc' "
            "raises on both engines under ANSI ON (same class as the SQL door). Pins the DF "
            f"entry point so a SQL-only green cannot claim the cast surface. {FIX_G6}."
        ),
    ),
    # true divergences under ANSI ON (do not invent more)
    CastRow(
        name="date_to_int_spark_refuses_repark_days",
        kind="error",
        entry="sql",
        family="date_to_int",
        sql="SELECT CAST(DATE '2020-01-01' AS INT) AS n",
        spark_error_needle=_DATE_INT_NEEDLE,
        repark_error_needle=_DATE_INT_NEEDLE,
        note=(
            "CONVERGED 2026-08-15 (was a spark-raises SPLIT): repark answered days-since-epoch "
            "18262 as non-null int32 because arrow-rs 58.4 reinterprets the Date32 backing value; "
            "Spark 4.1.2 ANSI refuses the type pair at analysis (Cast.checkInputDataTypes). The "
            "G6-3 gate in repark-functions' SparkExprSemantics now refuses with Spark's own class "
            "and Spark's own remedy, so this is a shared-raise equality. The name predates the "
            f"flip; the rename ships alone per relocation discipline. Flipped by {FIX_G6}."
        ),
    ),
    # the four DATE ↔ INT doors beside the pinned one
    CastRow(
        name="date_to_bigint_both_refuse",
        kind="error",
        entry="sql",
        family="date_to_int",
        sql="SELECT CAST(DATE '2020-01-01' AS BIGINT) AS n",
        spark_error_needle=_DATE_INT_NEEDLE,
        repark_error_needle=_DATE_INT_NEEDLE,
        note=(
            "The BIGINT twin of the headline row. arrow-rs permits Date32→Int64 as the SAME "
            "days-since-epoch reinterpretation, so repark answered 18262 as int64 while Spark "
            "refused with the identical class (design §1.2 door 2, recorded). Unpinned until "
            f"this unit. Closed by {FIX_G6}."
        ),
    ),
    CastRow(
        name="try_cast_date_to_int_both_refuse",
        kind="error",
        entry="sql",
        family="date_to_int",
        sql="SELECT try_cast(DATE '2020-01-01' AS INT) AS n",
        spark_error_needle=_DATE_INT_NEEDLE,
        repark_error_needle=_DATE_INT_NEEDLE,
        note=(
            "try_cast is total over VALUES, never over TYPES. Spark 4's TryCast is "
            "Cast(evalMode = TRY), so the legality check in Cast.checkInputDataTypes fires "
            "identically and the message spells TRY_CAST (design §1.2 door 3 — recorded "
            "fresh, settling the corpus's open question). repark's Expr::TryCast arm did not "
            f"exist before this unit, so this door was silently wrong. Closed by {FIX_G6}."
        ),
    ),
    CastRow(
        name="df_cast_date_to_int_both_refuse",
        kind="error",
        entry="df_cast",
        family="date_to_int",
        df_rows=[(datetime.date(2020, 1, 1),)],
        df_columns=["d"],
        cast_column="d",
        cast_type="int",
        spark_error_needle=_DATE_INT_NEEDLE,
        repark_error_needle=_DATE_INT_NEEDLE,
        note=(
            "CP-11 DataFrame-API door: Column.cast('int') on a DATE column. Spark refuses at "
            "analysis exactly as on the SQL door because the check is on the TYPE PAIR, not the "
            "spelling (design §1.2 door 4). A SQL-only green could not have claimed this "
            f"surface. Closed by {FIX_G6}."
        ),
    ),
    CastRow(
        name="df_try_cast_date_to_int_both_refuse",
        kind="error",
        entry="df_try_cast",
        family="date_to_int",
        df_rows=[(datetime.date(2020, 1, 1),)],
        df_columns=["d"],
        cast_column="d",
        cast_type="int",
        spark_error_needle=_DATE_INT_NEEDLE,
        repark_error_needle=_DATE_INT_NEEDLE,
        note=(
            "The fourth door: Column.try_cast('int') on a DATE column (design §1.2 door 5). "
            "Together with the SQL try_cast row this pins that the gate is on legality and the "
            f"eval mode is irrelevant to it. Closed by {FIX_G6}."
        ),
    ),
    CastRow(
        name="int_to_date_both_refuse",
        kind="error",
        entry="sql",
        family="int_to_date",
        sql="SELECT CAST(CAST(18262 AS INT) AS DATE) AS d",
        spark_error_needle=_DATE_INT_NEEDLE,
        repark_error_needle=_DATE_INT_NEEDLE,
        note=(
            "G6-5, the same class in the REVERSE direction (design §1.3, recorded): Spark "
            "refuses INT→DATE and names DATE_FROM_UNIX_DATE as the remedy, while repark "
            "answered date32 2020-01-01 non-null. It was unpinned anywhere — not in this "
            "corpus, not in the registry — and closing G6-3 without it would leave the class "
            "half-shut, which §6 discipline forbids. The inner CAST narrows the Int64 "
            "literal so the message names Spark's INT rather than DataFusion's BIGINT. Closed by "
            f"{FIX_G6}."
        ),
    ),
    CastRow(
        name="timestamp_to_int_nullability",
        kind="content",
        entry="sql",
        family="timestamp_to_int",
        sql="SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS n",
        spark=_one_row([("n", _I32, True)], {"n": 1577836800}),
        repark=None,
        note=(
            "The TZ-5 cast unit fixed the timestamp→numeric scaling and un-refused the INT "
            "path; NULLABILITY-2 (2026-09-05) closed the nullability residual — unix seconds "
            "1577836800 as nullable int32 on both engines. The name predates the "
            "flip; the rename ships alone per relocation discipline."
        ),
    ),
]


# Session


def _repark_session() -> ReparkSession:
    """A plain repark session for cast SQL / DF (no catalog, no zone knob beyond engine default)."""
    from repark import ReparkSession

    return ReparkSession.builder.appName("cast-failure-parity").getOrCreate()


@pytest.fixture
def repark() -> Iterator[ReparkSession]:
    """Repark session for the facade door."""
    session = _repark_session()
    try:
        yield session
    finally:
        with contextlib.suppress(Exception):
            session.stop()


# The rows


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_cast_failure_row(row: CastRow, repark: ReparkSession) -> None:
    """Every recorded row on the Arrow path (value AND type AND nullability) or error needle.

    Disclosures and splits classify a mismatch: CONVERGED (flip, never delete) vs regression.
    Error rows pin the repark needle live; the record driver pins the Spark needle.
    """
    if row.kind == "error":
        assert row.repark_error_needle is not None
        # Drive the real lifecycle so an engine that starts succeeding is CLASSIFIED (CP-1).
        try:
            actual = run_cast_content(repark, row)
        except Exception as exc:  # both engines' error types; message is the pin
            message = str(exc)
            assert _needle_in(message, row.repark_error_needle), (
                f"{row.name}: repark error missing {row.repark_error_needle!r}: "
                f"{message!r}. {row.note}"
            )
            return
        raise AssertionError(
            f"{row.name}: repark and Spark have CONVERGED on SUCCESS - repark no longer raises "
            f"on this shared-raise error row (got a table with schema "
            f"{[(field.name, str(field.type), field.nullable) for field in actual.schema]}). "
            f"Do not delete the row: flip it to a content equality (kind='content', pin the "
            f"Spark half from record mode, clear the error needles) and record the "
            f"convergence. {row.note}"
        )

    if row.kind == "split":
        assert row.which_raises is not None
        if row.which_raises == "repark":
            assert row.repark_error_needle is not None
            assert row.spark is not None
            try:
                actual = run_cast_content(repark, row)
            except Exception as exc:  # both engines' error types; message is the pin
                message = str(exc)
                assert _needle_in(message, row.repark_error_needle), (
                    f"{row.name}: repark was expected to refuse with {row.repark_error_needle!r}, "
                    f"got: {message!r}. {row.note}"
                )
                assert row.spark.num_rows >= 1, f"{row.name}: spark golden is empty - re-record"
                return

            if not _frames_differ(actual, row.spark):
                raise AssertionError(
                    f"{row.name}: repark and Spark have CONVERGED - repark now succeeds with the "
                    f"RECORDED SPARK output, so this split disclosure is stale. Do not delete the "
                    f"row: flip it to a content equality row (kind='content', repark=None, clear "
                    f"the error needle) and record the convergence. {row.note}"
                )
            raise AssertionError(
                f"{row.name}: repark no longer refuses (recipe committed) but the result does NOT "
                f"match the recorded Spark golden - this is a regression/partial change, not a "
                f"clean convergence. Re-derive both halves in record mode (see this module's "
                f"docstring) before flipping the pin. {row.note}"
            )

        assert row.spark_error_needle is not None
        assert row.repark is not None
        try:
            actual = run_cast_content(repark, row)
        except Exception as exc:
            message = str(exc)
            if row.spark_error_needle is not None and _needle_in(message, row.spark_error_needle):
                raise AssertionError(
                    f"{row.name}: repark and Spark have CONVERGED on the RAISE - repark now "
                    f"refuses with {row.spark_error_needle!r} too. Flip this split to a shared-"
                    f"raise error equality (kind='error', clear the repark table) and record the "
                    f"convergence. {row.note}"
                ) from exc
            raise AssertionError(
                f"{row.name}: repark raised unexpectedly "
                f"({type(exc).__name__}: {message!s:.200}) instead of producing its pinned "
                f"table - regression. Re-derive. {row.note}"
            ) from exc

        try:
            assert_frames_equal(actual, row.repark)
        except FrameMismatchError as mismatch:
            raise AssertionError(
                f"{row.name}: repark moved OFF its pinned disclosure for a spark-raises split - "
                f"regression. Re-derive both halves in record mode. {row.note}"
            ) from mismatch
        return

    assert row.spark is not None
    actual = run_cast_content(repark, row)

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
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure at "
        f"all - flip it to an equality row (repark=None) or re-record it. {row.note}"
    )


def test_cast_failure_row_set_covers_g6_budget() -> None:
    """Corpus size and class coverage are pinned; family floors are name-gated (CP-2, CP-11)."""
    assert G6_BUDGET_MIN <= len(ROWS) <= G6_BUDGET_MAX, (
        f"G6 budget {G6_BUDGET_MIN}-{G6_BUDGET_MAX} differential rows (got {len(ROWS)})"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure_or_split()]
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"at least {MIN_EQUALITY_ROWS} equality-class rows (content equality + shared-raise "
        f"error) required; got {len(equalities)}"
    )
    assert len(disclosures) <= MAX_DISCLOSURE_OR_SPLIT_ROWS, (
        f"at most {MAX_DISCLOSURE_OR_SPLIT_ROWS} disclosures/splits so the corpus cannot silently "
        f"absorb every regression as a new disclosure; got {len(disclosures)}"
    )

    names = {row.name for row in ROWS}

    malformed_numeric = [
        name
        for name in names
        if "malformed_string" in name and ("int" in name or "double" in name or "numeric" in name)
    ]
    assert len(malformed_numeric) >= MIN_MALFORMED_NUMERIC, (
        f"need >={MIN_MALFORMED_NUMERIC} malformed string→numeric rows; got {malformed_numeric}"
    )

    malformed_temporal = [
        name
        for name in names
        if "malformed_string" in name and ("date" in name or "timestamp" in name)
    ]
    assert len(malformed_temporal) >= MIN_MALFORMED_TEMPORAL, (
        f"need >={MIN_MALFORMED_TEMPORAL} malformed string→date/timestamp rows; "
        f"got {malformed_temporal}"
    )

    overflow_rows = [
        name for name in names if "overflow" in name and not name.startswith("try_cast_")
    ]
    assert len(overflow_rows) >= MIN_NUMERIC_OVERFLOW, (
        f"need >={MIN_NUMERIC_OVERFLOW} strict-cast overflow rows; got {overflow_rows}"
    )

    try_cast_rows = [name for name in names if name.startswith("try_cast_")]
    assert len(try_cast_rows) >= MIN_TRY_CAST_ROWS, (
        f"need >={MIN_TRY_CAST_ROWS} try_cast_* twins; got {try_cast_rows}"
    )

    assert any(row.family == "control" and row.is_equality() for row in ROWS), (
        "must keep at least one well-formed cast control equality"
    )

    df_rows = [row for row in ROWS if row.entry in ("df_cast", "df_try_cast")]
    assert len(df_rows) >= MIN_DF_API_ROWS, (
        f"need >={MIN_DF_API_ROWS} DataFrame-API cast rows (CP-11); got {len(df_rows)}"
    )
    assert any(row.entry == "sql" for row in ROWS), "sql() door must remain primary"

    error_rows = [row for row in ROWS if row.kind == "error"]
    assert len(error_rows) >= MIN_ERROR_ROWS, (
        f"need >={MIN_ERROR_ROWS} shared-raise error rows under ANSI ON; got {len(error_rows)}"
    )

    for row in ROWS:
        if row.kind == "content":
            assert row.spark is not None, f"{row.name}: content needs spark golden"
            if row.entry == "sql":
                assert row.sql is not None
            else:
                assert row.df_rows is not None and row.cast_type is not None
        if row.kind == "error":
            assert row.spark_error_needle is not None
            assert row.repark_error_needle is not None
            assert row.spark is None and row.repark is None
            assert row.which_raises is None
        if row.kind == "split":
            assert row.which_raises in ("spark", "repark")
            if row.which_raises == "repark":
                assert row.spark is not None
                assert row.repark_error_needle is not None
                assert row.repark is None
            else:
                assert row.repark is not None
                assert row.spark_error_needle is not None
                assert row.spark is None


# Classifier reachability (CP-1)


# Synthetic exemplar: the classifier arm must stay proven even with no real row exercising it;
# it never joins ROWS so the budget pins see only real rows.
_SYNTHETIC_REPARK_RAISES_SPLIT = CastRow(
    name="synthetic_repark_raises_split_exemplar",
    kind="split",
    entry="sql",
    family="synthetic",
    sql="SELECT 1 AS n",  # never executed — run_cast_content is monkeypatched in both arms
    which_raises="repark",
    spark=_one_row([("n", _I32, True)], {"n": 1577836800}),
    repark_error_needle="Cast error",
    note="synthetic CP-1 exemplar for the repark-raises split classifier arms; not in ROWS.",
)


def test_split_repark_raises_classifier_converged_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: repark-raises split matching the Spark golden → CONVERGED flip guidance."""
    import test_cast_failure_parity as cast_mod

    split_row = _SYNTHETIC_REPARK_RAISES_SPLIT
    assert split_row.spark is not None
    golden = split_row.spark

    def _fake_success(_session: Any, _row: CastRow) -> pa.Table:
        return golden

    monkeypatch.setattr(cast_mod, "run_cast_content", _fake_success)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_cast_failure_row(split_row, repark)
    message = str(excinfo.value)
    assert "flip it to a content equality" in message
    assert "Do not delete" in message


def test_split_repark_raises_classifier_regression_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: repark-raises split commits a non-Spark result → regression guidance."""
    import test_cast_failure_parity as cast_mod

    split_row = _SYNTHETIC_REPARK_RAISES_SPLIT
    wrong = _one_row([("n", _I32, True)], {"n": 99})

    def _fake_wrong(_session: Any, _row: CastRow) -> pa.Table:
        return wrong

    monkeypatch.setattr(cast_mod, "run_cast_content", _fake_wrong)

    with pytest.raises(AssertionError, match="regression") as excinfo:
        test_cast_failure_row(split_row, repark)
    message = str(excinfo.value)
    assert "Re-derive" in message
    assert "not a clean convergence" in message


def test_content_disclosure_classifier_converged_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: content disclosure landing ON the recorded Spark output → CONVERGED guidance."""
    import test_cast_failure_parity as cast_mod

    row = _SYNTHETIC_CONTENT_DISCLOSURE
    assert row.kind == "content" and row.repark is not None and row.spark is not None
    golden = row.spark

    def _fake_spark_output(_session: Any, _row: CastRow) -> pa.Table:
        return golden

    monkeypatch.setattr(cast_mod, "run_cast_content", _fake_spark_output)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_cast_failure_row(row, repark)
    message = str(excinfo.value)
    assert "flip it to an equality row" in message
    assert "Do not delete" in message


def test_content_disclosure_classifier_regression_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: content disclosure landing on NEITHER half → regression guidance."""
    import test_cast_failure_parity as cast_mod

    row = _SYNTHETIC_CONTENT_DISCLOSURE
    wrong = _one_row([("n", _I32, False)], {"n": 99})

    def _fake_wrong(_session: Any, _row: CastRow) -> pa.Table:
        return wrong

    monkeypatch.setattr(cast_mod, "run_cast_content", _fake_wrong)

    with pytest.raises(AssertionError, match="regression") as excinfo:
        test_cast_failure_row(row, repark)
    message = str(excinfo.value)
    assert "moved OFF its pinned disclosure" in message
    assert "Re-derive" in message


# Synthetic exemplar: the classifier arm must stay proven even with no real row exercising it;
# it never joins ROWS so the budget pins see only real rows.
_SYNTHETIC_CONTENT_DISCLOSURE = CastRow(
    name="synthetic_content_disclosure_exemplar",
    kind="content",
    entry="sql",
    family="synthetic",
    sql="SELECT 1 AS n",
    spark=_one_row([("n", _I32, True)], {"n": 1577836800}),
    repark=_one_row([("n", _I32, False)], {"n": 1577836800}),
    note="synthetic CP-1 exemplar for the content classifier arms; not in ROWS.",
)
_SYNTHETIC_SPARK_RAISES_SPLIT = CastRow(
    name="synthetic_spark_raises_split_exemplar",
    kind="split",
    entry="sql",
    family="synthetic",
    sql="SELECT 1 AS n",  # never executed — run_cast_content is monkeypatched
    which_raises="spark",
    spark_error_needle="DATATYPE_MISMATCH",
    repark=_one_row([("n", _I32, False)], {"n": 18262}),
    note="synthetic CP-1 exemplar for the spark-raises split classifier arm; not in ROWS.",
)


def test_split_spark_raises_classifier_converged_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: spark-raises split where repark starts raising Spark's needle → CONVERGED."""
    import test_cast_failure_parity as cast_mod

    split_row = _SYNTHETIC_SPARK_RAISES_SPLIT
    assert split_row.spark_error_needle is not None
    needle = split_row.spark_error_needle

    def _fake_raise(_session: Any, _row: CastRow) -> pa.Table:
        raise RuntimeError(f"simulated spark refuse [{needle}] DATE cannot cast to INT")

    monkeypatch.setattr(cast_mod, "run_cast_content", _fake_raise)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_cast_failure_row(split_row, repark)
    message = str(excinfo.value)
    assert "shared-raise error equality" in message or "Flip this split" in message


def test_error_row_classifier_success_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: shared-raise error row that starts succeeding → CONVERGED flip guidance."""
    import test_cast_failure_parity as cast_mod

    error_row = next(row for row in ROWS if row.name == "malformed_string_to_int_both_raise")
    success = _one_row([("n", _I32, True)], {"n": None})

    def _fake_null(_session: Any, _row: CastRow) -> pa.Table:
        return success

    monkeypatch.setattr(cast_mod, "run_cast_content", _fake_null)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_cast_failure_row(error_row, repark)
    message = str(excinfo.value)
    assert "flip it to a content equality" in message
    assert "Do not delete" in message
