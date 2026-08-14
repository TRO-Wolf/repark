"""Three-valued logic differential corpus (H-2 gap G12) — value AND Arrow type AND nullability.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``) on 2026-08-11. One recipe per row runs on BOTH engines, so the
recipe under test and the recipe the oracle ran are the same code — nothing here is hand-computed.

**Why some rows may be DISCLOSURES.** When the engines agree on value AND Arrow type AND
nullability the row is a plain equality (``repark is None``). When they honestly disagree the row
pins BOTH halves and asserts the divergence still holds. A silent CONVERGENCE goes red and forces
the disclosure to be revisited rather than laundered into "parity". When a G12 fix lands, each
divergent row flips to ``repark=None`` (equality) and that flip is the fix's revert-red evidence.

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``) through the parity
comparator, so schema name, Arrow type and nullability are part of every content assertion —
never ``show``. Nullability is load-bearing here (boolean columns produced by 3VL expressions).

**Re-deriving the goldens (record mode).** The driver that recorded every ``spark`` half is
committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_tvl_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe, so the recorded golden and the
asserted recipe cannot drift apart. Needs a JVM + ``pyspark`` (``uv sync --extra record``); never
collected by pytest. ``--emit`` prints paste-ready table constructors. Serialize with other JVM
recorders via ``/tmp/grok-jvm-record.lock``.

**Entry points (CP-11).** Facade ``sql()`` is primary (``entry="sql"``). At least two DataFrame-API
rows (``entry="df"``) pin ``filter`` / ``select`` column expressions — name-gated so a SQL control
cannot satisfy either family. A class claim is scoped to the entry it names.

**Six load-bearing AND/OR/NOT combos (not all 9x2).** The traps that distinguish UNKNOWN from
FALSE: ``TRUE AND NULL→NULL``, ``FALSE AND NULL→FALSE``, ``TRUE OR NULL→TRUE``,
``FALSE OR NULL→NULL``, ``NOT NULL→NULL``, ``NULL AND NULL→NULL``. Boolean-core pairs
(``TRUE AND TRUE``, …) are not 3VL-load-bearing and are omitted by design (named in the ledger).

**Out of scope (named, not silent):** fixing any divergence found; the registry file; DML-level
``NOT IN`` with NULL (that family is the G3-E8 corpus — **PR #54 in flight** at kickoff; do not
duplicate). One SELECT-level ``IN (…, NULL)`` row is enough for the SELECT surface.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Named so every disclosure's note can cite the same future work without inventing per-row fix IDs.
FIX_G12 = (
    "the SQL three-valued-logic parity fix "
    "(briefs/v2-engine-hardening.md, gap G12; DECLARE candidacy if the ruling is disclosure-only)"
)

# Budget floors/ceilings pinned by test_tvl_row_set_covers_g12_budget (not incidental).
G12_BUDGET_MIN = 10
G12_BUDGET_MAX = 12
MIN_EQUALITY_ROWS = 6
MAX_DISCLOSURE_ROWS = 6
MIN_DF_API_ROWS = 2
# Name-gated family floors so a control cannot green the pin (CP-2).
MIN_TRUTH_TABLE_ROWS = 6  # and_* / or_* / not_* load-bearing combos
MIN_NULL_COMPARE_ROWS = 1  # *null_eq* / *null_safe*
MIN_IS_NULL_ROWS = 1  # *is_null*
MIN_CASE_WHEN_ROWS = 1  # *case_when*
MIN_IN_LIST_ROWS = 1  # *in_list*


# ==================================================================================================
# Arrow helpers
# ==================================================================================================


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


def _one_row(fields: list[tuple[str, pa.DataType, bool]], values: dict[str, object]) -> pa.Table:
    """Build the single-row Arrow table a recorded golden describes."""
    return _table(fields, {name: [values[name]] for name, _, _ in fields})


_BOOL = pa.bool_()
_I32 = pa.int32()


# ==================================================================================================
# Row shape
# ==================================================================================================


@dataclass(frozen=True)
class TvlRow:
    """One differential 3VL row: a recipe + recorded Spark half + optional repark half.

    ``kind="content"`` — result set on the Arrow path.
      * ``repark is None`` means the engines AGREE — plain equality against ``spark``.
      * ``repark is not None`` means DISCLOSURE: repark's actual output is pinned, and a
        convergence onto the recorded Spark output is detected and reported as one.

    ``entry="sql"`` runs ``session.sql(sql)``. ``entry="df"`` runs the named DataFrame recipe
    (``df_recipe``) so the CP-11 door is a real ``filter``/``select`` expression path, not a
    string-inspected SQL twin.
    """

    name: str
    kind: Literal["content"]
    entry: Literal["sql", "df"]
    family: str
    note: str
    sql: str | None = None
    df_recipe: str | None = None
    spark: pa.Table | None = None
    repark: pa.Table | None = None

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (content, no repark pin)."""
        return self.kind == "content" and self.repark is None and self.spark is not None

    def is_disclosure(self) -> bool:
        """True when the row pins a known divergence (table disclosure)."""
        return self.kind == "content" and self.repark is not None


# ==================================================================================================
# Lifecycle helpers — one recipe SSOT the record driver imports
# ==================================================================================================


def _run_df_recipe(session: Any, recipe: str) -> pa.Table:
    """Execute a named DataFrame-API recipe on repark or Spark; return Arrow."""
    if recipe == "eq_null_safe_select":
        # NULL <=> NULL is TRUE; NULL <=> 1 is FALSE; 1 <=> 1 is TRUE.
        frame = session.createDataFrame(
            [(1, 1), (None, None), (1, None), (None, 1)],
            ["a", "b"],
        )
        out = frame.select(frame.a.eqNullSafe(frame.b).alias("nse"))
    elif recipe == "select_and_true_null":
        # Column AND/OR/NOT over boolean columns carrying NULL (3VL on the DF door).
        frame = session.createDataFrame(
            [(True, None), (False, None), (True, True)],
            ["left_flag", "right_flag"],
        )
        out = frame.select(
            (frame.left_flag & frame.right_flag).alias("and_v"),
            (frame.left_flag | frame.right_flag).alias("or_v"),
            (~frame.left_flag).alias("not_left"),
        )
    else:
        raise AssertionError(f"unknown df_recipe: {recipe!r}")

    to_arrow = getattr(out, "to_arrow", None) or out.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def run_tvl_content(session: Any, row: TvlRow) -> pa.Table:
    """Execute the row's recipe and return the Arrow result (facade or Spark)."""
    if row.entry == "sql":
        assert row.sql is not None
        frame = session.sql(row.sql)
        to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
        return to_arrow()  # type: ignore[no-any-return]

    assert row.df_recipe is not None
    return _run_df_recipe(session, row.df_recipe)


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


# ==================================================================================================
# The corpus (gap G12: budget 10-12)
# ==================================================================================================

ROWS: list[TvlRow] = [
    # ----- 1-6. Truth-table floor: six load-bearing AND/OR/NOT combos ---------------------------
    # Spark halves recorded 2026-08-11 against PySpark 4.1.2 (see module docstring / record driver).
    TvlRow(
        name="and_true_null_is_null",
        kind="content",
        entry="sql",
        family="truth_table",
        sql="SELECT (TRUE AND CAST(NULL AS BOOLEAN)) AS v",
        spark=_one_row([("v", _BOOL, True)], {"v": None}),
        repark=None,
        note=(
            "3VL: TRUE AND NULL → NULL (not FALSE). The classic trap that distinguishes "
            "UNKNOWN from boolean-false. Nullability of the boolean result is load-bearing."
        ),
    ),
    TvlRow(
        name="and_false_null_is_false",
        kind="content",
        entry="sql",
        family="truth_table",
        sql="SELECT (FALSE AND CAST(NULL AS BOOLEAN)) AS v",
        spark=_one_row([("v", _BOOL, True)], {"v": False}),
        repark=None,
        note="3VL: FALSE AND NULL → FALSE (AND short-circuits on FALSE; null is irrelevant).",
    ),
    TvlRow(
        name="or_true_null_is_true",
        kind="content",
        entry="sql",
        family="truth_table",
        sql="SELECT (TRUE OR CAST(NULL AS BOOLEAN)) AS v",
        spark=_one_row([("v", _BOOL, True)], {"v": True}),
        repark=None,
        note="3VL: TRUE OR NULL → TRUE (OR short-circuits on TRUE).",
    ),
    TvlRow(
        name="or_false_null_is_null",
        kind="content",
        entry="sql",
        family="truth_table",
        sql="SELECT (FALSE OR CAST(NULL AS BOOLEAN)) AS v",
        spark=_one_row([("v", _BOOL, True)], {"v": None}),
        repark=None,
        note="3VL: FALSE OR NULL → NULL (not TRUE).",
    ),
    TvlRow(
        name="not_null_is_null",
        kind="content",
        entry="sql",
        family="truth_table",
        sql="SELECT (NOT CAST(NULL AS BOOLEAN)) AS v",
        spark=_one_row([("v", _BOOL, True)], {"v": None}),
        repark=None,
        note="3VL: NOT NULL → NULL.",
    ),
    TvlRow(
        name="and_null_null_is_null",
        kind="content",
        entry="sql",
        family="truth_table",
        sql="SELECT (CAST(NULL AS BOOLEAN) AND CAST(NULL AS BOOLEAN)) AS v",
        spark=_one_row([("v", _BOOL, True)], {"v": None}),
        repark=None,
        note=(
            "3VL: NULL AND NULL → NULL. Sixth load-bearing combo — both operands unknown. "
            "Boolean-core pairs (TRUE AND TRUE, …) are deliberately omitted."
        ),
    ),
    # ----- 7. NULL = NULL vs NULL <=> NULL -------------------------------------------------------
    TvlRow(
        name="null_eq_vs_null_safe_eq",
        kind="content",
        entry="sql",
        family="null_compare",
        sql=(
            "SELECT "
            "(CAST(NULL AS INT) = CAST(NULL AS INT)) AS eq, "
            "(CAST(NULL AS INT) <=> CAST(NULL AS INT)) AS nse"
        ),
        spark=_one_row(
            [("eq", _BOOL, True), ("nse", _BOOL, False)],
            {"eq": None, "nse": True},
        ),
        # VALUE agrees (eq=NULL, nse=TRUE); nullability of nse diverges — Spark marks
        # null-safe equal non-nullable, repark's Arrow bool is nullable. Flipped by FIX_G12.
        repark=_one_row(
            [("eq", _BOOL, True), ("nse", _BOOL, True)],
            {"eq": None, "nse": True},
        ),
        note=(
            "NULL = NULL is UNKNOWN (NULL); NULL <=> NULL (Spark null-safe equal / "
            "IS NOT DISTINCT FROM) is TRUE. VALUE matches; DISCLOSURE on nse nullability "
            f"(Spark non-null bool vs repark nullable bool). Flipped by {FIX_G12}."
        ),
    ),
    # ----- 8. IS [NOT] NULL vs = NULL ------------------------------------------------------------
    TvlRow(
        name="is_null_vs_eq_null",
        kind="content",
        entry="sql",
        family="is_null",
        sql=(
            "SELECT v, "
            "(v IS NULL) AS is_null, "
            "(v IS NOT NULL) AS is_not_null, "
            "(v = CAST(NULL AS INT)) AS eq_null "
            "FROM ("
            "  SELECT CAST(1 AS INT) AS v "
            "  UNION ALL SELECT CAST(NULL AS INT)"
            ") t "
            "ORDER BY v NULLS LAST"
        ),
        spark=_table(
            [
                ("v", _I32, True),
                ("is_null", _BOOL, False),
                ("is_not_null", _BOOL, False),
                ("eq_null", _BOOL, True),
            ],
            {
                "v": [1, None],
                "is_null": [False, True],
                "is_not_null": [True, False],
                "eq_null": [None, None],
            },
        ),
        repark=None,
        note=(
            "IS NULL is a two-valued predicate (TRUE/FALSE); = NULL is three-valued (UNKNOWN for "
            "every left-hand side, including NULL). Pins value AND nullability of all three "
            "boolean columns over a non-null and a null payload row."
        ),
    ),
    # ----- 9. CASE WHEN <null-predicate> ---------------------------------------------------------
    TvlRow(
        name="case_when_null_predicate",
        kind="content",
        entry="sql",
        family="case_when",
        sql=(
            "SELECT CASE "
            "  WHEN CAST(NULL AS BOOLEAN) THEN CAST(1 AS INT) "
            "  WHEN TRUE THEN CAST(2 AS INT) "
            "  ELSE CAST(3 AS INT) "
            "END AS v"
        ),
        spark=_one_row([("v", _I32, False)], {"v": 2}),
        repark=None,
        note=(
            "CASE WHEN <null-predicate>: UNKNOWN does not match, so control falls through to the "
            "next WHEN TRUE branch → 2 (not the ELSE). Classic 3VL CASE trap."
        ),
    ),
    # ----- 10. SELECT-level IN (…, NULL) — do not duplicate DML NOT-IN (PR #54 in flight) --------
    TvlRow(
        name="in_list_with_null_select",
        kind="content",
        entry="sql",
        family="in_list",
        sql=(
            "SELECT "
            "(CAST(1 AS INT) IN (CAST(1 AS INT), CAST(2 AS INT), CAST(NULL AS INT))) AS hit, "
            "(CAST(3 AS INT) IN (CAST(1 AS INT), CAST(2 AS INT), CAST(NULL AS INT))) AS miss, "
            "(CAST(NULL AS INT) IN (CAST(1 AS INT), CAST(2 AS INT))) AS null_lhs"
        ),
        spark=_one_row(
            [("hit", _BOOL, True), ("miss", _BOOL, True), ("null_lhs", _BOOL, True)],
            {"hit": True, "miss": None, "null_lhs": None},
        ),
        repark=None,
        note=(
            "SELECT-level IN (…, NULL): hit with a present member is TRUE; miss with a NULL "
            "sibling is UNKNOWN; NULL LHS is UNKNOWN. DML-level NOT IN with NULL is the G3-E8 "
            "corpus (**PR #54 in flight** at kickoff) — not duplicated here."
        ),
    ),
    # ----- 11-12. DataFrame-API door (CP-11) >=2 rows -------------------------------------------
    TvlRow(
        name="df_eq_null_safe_select",
        kind="content",
        entry="df",
        family="df_api",
        df_recipe="eq_null_safe_select",
        spark=_table(
            [("nse", _BOOL, False)],
            {"nse": [True, True, False, False]},
        ),
        # VALUE agrees; nullability of nse diverges (same family as SQL <=> disclosure).
        repark=_table(
            [("nse", _BOOL, True)],
            {"nse": [True, True, False, False]},
        ),
        note=(
            "CP-11 DataFrame door: ``Column.eqNullSafe`` select — NULL <=> NULL is TRUE, "
            "NULL <=> 1 is FALSE. VALUE matches; DISCLOSURE on nse nullability "
            f"(Spark non-null bool vs repark nullable bool). Flipped by {FIX_G12}. "
            "Distinct from the SQL ``<=>`` spelling in null_eq_vs_null_safe_eq."
        ),
    ),
    TvlRow(
        name="df_select_and_or_not_nulls",
        kind="content",
        entry="df",
        family="df_api",
        df_recipe="select_and_true_null",
        spark=_table(
            [("and_v", _BOOL, True), ("or_v", _BOOL, True), ("not_left", _BOOL, True)],
            {
                "and_v": [None, False, True],
                "or_v": [True, None, True],
                "not_left": [False, True, False],
            },
        ),
        repark=None,
        note=(
            "CP-11 DataFrame door: ``&`` / ``|`` / ``~`` over boolean columns carrying NULL. "
            "TRUE AND NULL → NULL; FALSE OR NULL → NULL; NOT TRUE → FALSE. Pins the 3VL "
            "truth table on the filter/select expression path (not sql())."
        ),
    ),
]


# ==================================================================================================
# Session builders
# ==================================================================================================


def _repark_session() -> ReparkSession:
    """A repark facade session (no catalog required — pure SQL / createDataFrame)."""
    from repark import ReparkSession

    return ReparkSession.builder.appName("tvl-parity").getOrCreate()


@pytest.fixture
def repark() -> Iterator[ReparkSession]:
    """Repark session for the facade door. Yields then stops."""
    session = _repark_session()
    try:
        yield session
    finally:
        with contextlib.suppress(Exception):
            session.stop()


# ==================================================================================================
# The rows
# ==================================================================================================


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_tvl_parity_row(row: TvlRow, repark: ReparkSession) -> None:
    """Every recorded row on the Arrow path (value AND type AND nullability).

    Content equality rows assert ``repark == Spark``.

    Content disclosure rows assert repark's pinned actual output — and when that assertion fails,
    the failure is CLASSIFIED (CONVERGED vs regression).
    """
    assert row.spark is not None, (
        f"{row.name}: spark golden is missing — run "
        f"python/repark/tests/_record_tvl_goldens.py --emit and paste. {row.note}"
    )
    actual = run_tvl_content(repark, row)

    if row.repark is None:
        assert_frames_equal(actual, row.spark)
        return

    try:
        assert_frames_equal(actual, row.repark)
    except FrameMismatchError as mismatch:
        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED — repark now produces the RECORDED "
                f"SPARK output, so this disclosure is stale. Do not delete the row: flip it to an "
                f"equality row (repark=None) and record the convergence. Flipped by {FIX_G12}. "
                f"{row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the recorded "
            f"Spark golden either — this is a regression, not a convergence. Re-derive both "
            f"halves in record mode (see this module's docstring) before touching the pin. "
            f"{row.note}"
        ) from mismatch

    assert _frames_differ(row.repark, row.spark), (
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure at "
        f"all — flip it to an equality row (repark=None) or re-record it. {row.note}"
    )


def test_tvl_row_set_covers_g12_budget() -> None:
    """The pin budget is part of the unit — corpus size and class coverage are pinned.

    Family coverage pins are **name-gated** so a control row cannot satisfy them (CP-2).
    Entry-point coverage pins the DF door (CP-11) by ``entry="df"``, not by SQL that happens
    to mention AND.
    """
    assert G12_BUDGET_MIN <= len(ROWS) <= G12_BUDGET_MAX, (
        f"G12 budget {G12_BUDGET_MIN}-{G12_BUDGET_MAX} differential rows (got {len(ROWS)})"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure()]
    assert all(row.spark is not None for row in ROWS), (
        "every row must carry a recorded Spark golden (run _record_tvl_goldens.py --emit)"
    )
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"at least {MIN_EQUALITY_ROWS} control equality rows required so the corpus cannot "
        f"degenerate to all-disclosures; got {len(equalities)}"
    )
    assert len(disclosures) <= MAX_DISCLOSURE_ROWS, (
        f"at most {MAX_DISCLOSURE_ROWS} disclosures so the corpus cannot silently absorb every "
        f"regression as a new disclosure; got {len(disclosures)}"
    )

    names = {row.name for row in ROWS}

    # 1. Truth-table floor — name-gated and_*/or_*/not_* (six load-bearing).
    truth_rows = [
        name
        for name in names
        if name.startswith("and_") or name.startswith("or_") or name.startswith("not_")
    ]
    assert len(truth_rows) >= MIN_TRUTH_TABLE_ROWS, (
        f"need ≥{MIN_TRUTH_TABLE_ROWS} truth-table rows named and_*/or_*/not_*; got {truth_rows}"
    )
    for required in (
        "and_true_null_is_null",
        "and_false_null_is_false",
        "or_true_null_is_true",
        "or_false_null_is_null",
        "not_null_is_null",
        "and_null_null_is_null",
    ):
        assert required in names, f"load-bearing truth-table row missing: {required}"

    # 2. NULL = NULL vs null-safe equal — name-gated.
    null_compare = [name for name in names if "null_eq" in name or "null_safe" in name]
    assert len(null_compare) >= MIN_NULL_COMPARE_ROWS, (
        f"need ≥{MIN_NULL_COMPARE_ROWS} *null_eq*/*null_safe* rows; got {null_compare}"
    )

    # 3. IS [NOT] NULL vs = NULL — name-gated.
    is_null_rows = [name for name in names if "is_null" in name]
    assert len(is_null_rows) >= MIN_IS_NULL_ROWS, (
        f"need ≥{MIN_IS_NULL_ROWS} *is_null* rows; got {is_null_rows}"
    )

    # 4. CASE WHEN null-predicate — name-gated.
    case_rows = [name for name in names if "case_when" in name]
    assert len(case_rows) >= MIN_CASE_WHEN_ROWS, (
        f"need ≥{MIN_CASE_WHEN_ROWS} *case_when* rows; got {case_rows}"
    )

    # 5. SELECT-level IN (…, NULL) — name-gated; DML NOT-IN is PR #54, not here.
    in_rows = [name for name in names if "in_list" in name]
    assert len(in_rows) >= MIN_IN_LIST_ROWS, (
        f"need ≥{MIN_IN_LIST_ROWS} *in_list* SELECT-level rows; got {in_rows}"
    )
    assert not any("not_in" in name for name in names), (
        "do not duplicate the DML NOT-IN family (G3-E8 / PR #54 in flight); cite it in the ledger"
    )

    # 6. Entry points — CP-11 DF door (entry field, not name substring alone).
    df_rows = [row for row in ROWS if row.entry == "df"]
    assert len(df_rows) >= MIN_DF_API_ROWS, (
        f"need ≥{MIN_DF_API_ROWS} DataFrame-API rows (CP-11); got {len(df_rows)}"
    )
    assert all(row.df_recipe is not None for row in df_rows)
    assert any(row.entry == "sql" for row in ROWS), "sql() door must remain primary"
    # Name gate: df_* prefix so a SQL row cannot be re-tagged to green the DF pin by family alone.
    assert all(row.name.startswith("df_") for row in df_rows), (
        "DF rows must be name-gated df_* so SQL controls cannot satisfy CP-11"
    )

    # Well-formedness.
    for row in ROWS:
        if row.entry == "sql":
            assert row.sql is not None and row.df_recipe is None
        else:
            assert row.df_recipe is not None and row.sql is None
        assert row.kind == "content"


# ==================================================================================================
# Classifier reachability (CP-1) — both arms proven by monkeypatch
# ==================================================================================================


def test_disclosure_classifier_converged_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: disclosure actual matching the Spark golden → CONVERGED flip guidance."""
    import test_three_valued_logic_parity as tvl_mod

    # Build a synthetic disclosure row so the arm is reachable even when the corpus is all-equality.
    base = next(row for row in ROWS if row.name == "and_true_null_is_null")
    spark_half = base.spark or _one_row([("v", _BOOL, True)], {"v": None})
    wrong_repark = _one_row([("v", _BOOL, True)], {"v": False})
    disclosure = TvlRow(
        name=base.name,
        kind="content",
        entry=base.entry,
        family=base.family,
        note=base.note,
        sql=base.sql,
        spark=spark_half,
        repark=wrong_repark,
    )

    def _fake_match(_session: Any, _row: TvlRow) -> pa.Table:
        return spark_half

    monkeypatch.setattr(tvl_mod, "run_tvl_content", _fake_match)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_tvl_parity_row(disclosure, repark)
    message = str(excinfo.value)
    assert "flip it to an equality" in message
    assert "Do not delete" in message


def test_disclosure_classifier_regression_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: disclosure actual matching neither half → regression guidance."""
    import test_three_valued_logic_parity as tvl_mod

    base = next(row for row in ROWS if row.name == "and_true_null_is_null")
    spark_half = base.spark or _one_row([("v", _BOOL, True)], {"v": None})
    repark_half = _one_row([("v", _BOOL, True)], {"v": False})
    disclosure = TvlRow(
        name=base.name,
        kind="content",
        entry=base.entry,
        family=base.family,
        note=base.note,
        sql=base.sql,
        spark=spark_half,
        repark=repark_half,
    )
    third = _one_row([("v", _BOOL, True)], {"v": True})

    def _fake_third(_session: Any, _row: TvlRow) -> pa.Table:
        return third

    monkeypatch.setattr(tvl_mod, "run_tvl_content", _fake_third)

    with pytest.raises(AssertionError, match="regression") as excinfo:
        test_tvl_parity_row(disclosure, repark)
    message = str(excinfo.value)
    assert "Re-derive" in message
