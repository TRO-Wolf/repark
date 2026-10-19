"""Joins differential corpus (H-2 gap G4) - value AND Arrow type AND nullability vs live Spark.

**Oracle.** Every ``spark`` table / error needle below was RECORDED in record mode against live
PySpark 4.1.2 (zulu-17, ``master("local[2]")``, ANSI on, ``spark.sql.shuffle.partitions=2``).
One recipe per row runs on BOTH engines, so the recipe under test and the recipe the oracle ran
are the same code path - nothing here is hand-computed.

**Disclosures / splits.** When one engine refuses a surface the other runs, the row is a
**split** pinning the recorded Spark success half and repark's refuse needle. A silent
CONVERGENCE goes red and forces the disclosure to be revisited, not laundered into "parity".

Former refuse splits ``df_left_semi_on_name`` / ``df_left_anti_on_name`` are now content
equalities; their names stay byte-identical (a pin's name is part of the pin; the
``_unsupported`` suffix retires in a declared-rename unit). The corpus currently holds no
splits; the classifier's arms stay proven by ``_CLASSIFIER_PROBE_SPLIT``.

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``) through the parity
comparator - schema name, Arrow type and nullability are part of every content assertion, never
``show``. The comparator is order-insensitive by default. Error rows pin the error *token*.

**Re-deriving the goldens (record mode).** The driver that recorded every Spark half is committed
beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_join_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe, so the recorded golden and
the asserted recipe cannot drift apart. Needs a JVM + ``pyspark`` (``uv sync --extra record``);
never collected by pytest.

**Entry points (CP-11).** Facade ``sql()`` is the primary door; the DataFrame-API door carries
its own content rows including the whole semi family across every ``on`` shape.

**Out of scope (named, not silent):** fixing any divergence found (rows document), the registry
file, window functions (W-4).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Budget floors/ceilings - pinned by test_join_row_set_covers_g4_budget

G4_BUDGET_MIN = 20
# The budget is a sprawl guard: growing it is a reviewed act with a named driver, never a
# silent bump.
G4_BUDGET_MAX = 30
MIN_EQUALITY_ROWS = 14
MAX_DISCLOSURE_OR_SPLIT_ROWS = 8
MIN_DF_API_ROWS = 6  # floor keeps the DF door from quietly shrinking back
MIN_NULL_KEY_ROWS = 4  # every join type: inner/left/right/full (name-gated *null_keys_*)
MIN_DUPLICATE_KEY_ROWS = 2
MIN_TYPE_MISMATCH_ROWS = 2
MIN_NULLABILITY_ROWS = 2  # outer-join schema nullability flips (name-gated *nullable*)


# Arrow helpers


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


_I64 = pa.int64()
_STR = pa.string()
_DEC_10_2 = pa.decimal128(10, 2)


# Row shape


@dataclass(frozen=True)
class JoinRow:
    """One differential join row: a recipe + recorded Spark half + optional repark half.

    ``kind="content"`` - result set on the Arrow path. ``repark is None``: the engines AGREE,
    plain equality against ``spark``. ``repark is not None``: DISCLOSURE - repark's actual
    output is pinned, and convergence onto the Spark output is detected and reported.

    ``kind="error"`` - both engines refuse; pins the error *token* each raises.

    ``kind="split"`` - one engine succeeds, the other refuses; pins both halves. If the refuse
    side starts succeeding, the harness classifies CONVERGED (matches the success golden ->
    flip to content equality) vs regression.

    ``entry="sql"`` runs ``session.sql(sql)``. ``entry="df"`` runs ``createDataFrame`` +
    ``DataFrame.join`` (CP-11 door).
    """

    name: str
    kind: Literal["content", "error", "split"]
    entry: Literal["sql", "df"]
    family: str
    note: str
    # SQL path
    sql: str | None = None
    # DF path
    left_rows: list[tuple[object, ...]] | None = None
    right_rows: list[tuple[object, ...]] | None = None
    left_columns: list[str] | None = None
    right_columns: list[str] | None = None
    on: str | None = None
    how: str = "inner"
    # ``name`` = ``on="k"``; ``name_list`` = ``on=["k"]``; ``condition`` = ``left.k == right.k``;
    # ``eq_null_safe`` = ``left.k.eqNullSafe(right.k)``; ``none`` = no ``on`` at all. The shapes
    # are separate engine paths (name equi-join vs the H1 condition rewrite), so a claim proven
    # on one says nothing about the others.
    on_mode: Literal["name", "name_list", "condition", "eq_null_safe", "none"] = "name"
    # When True, post-join select aliases left.k->lk / right.k->rk to avoid duplicate names.
    select_eq_null_safe: bool = False
    spark: pa.Table | None = None
    repark: pa.Table | None = None
    spark_error_needle: str | None = None
    repark_error_needle: str | None = None

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (content, no repark pin)."""
        return self.kind == "content" and self.repark is None and self.spark is not None

    def is_disclosure_or_split(self) -> bool:
        """True when the row pins a known divergence (table disclosure, split, or error split)."""
        if self.kind in ("split", "error"):
            return True
        return self.kind == "content" and self.repark is not None


# Lifecycle helpers - one recipe SSOT the record driver imports


def run_join_content(session: Any, row: JoinRow) -> pa.Table:
    """Execute the row's recipe and return the Arrow result (facade or Spark)."""
    if row.entry == "sql":
        assert row.sql is not None
        frame = session.sql(row.sql)
        to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
        return to_arrow()  # type: ignore[no-any-return]

    assert row.left_rows is not None and row.right_rows is not None
    assert row.left_columns is not None and row.right_columns is not None
    left = session.createDataFrame(row.left_rows, row.left_columns)
    right = session.createDataFrame(row.right_rows, row.right_columns)

    if row.on_mode == "eq_null_safe":
        assert row.on is not None
        joined = left.join(right, left[row.on].eqNullSafe(right[row.on]), row.how)
        if row.select_eq_null_safe:
            # Condition joins keep both key copies; alias for a stable Arrow schema.
            frame = joined.select(
                left[row.on].alias("lk"),
                left[row.left_columns[1]],
                right[row.on].alias("rk"),
                right[row.right_columns[1]],
            )
        else:
            frame = joined
    elif row.on_mode == "condition":
        assert row.on is not None
        frame = left.join(right, left[row.on] == right[row.on], row.how)
    elif row.on_mode == "name_list":
        assert row.on is not None
        frame = left.join(right, [row.on], row.how)
    elif row.on_mode == "none":
        frame = left.join(right, how=row.how)
    else:
        frame = left.join(right, on=row.on, how=row.how)

    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def run_join_expect_error(session: Any, row: JoinRow) -> str:
    """Run the recipe expecting a raise; return the error message text."""
    try:
        _ = run_join_content(session, row)
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


# The corpus (gap G4)

ROWS: list[JoinRow] = [
    # ----- controls ------------------------------------------------------------------------------
    JoinRow(
        name="control_inner_equality",
        kind="content",
        entry="sql",
        family="control",
        sql=(
            "SELECT l.k, l.a, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'b') l "
            "INNER JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "            UNION ALL SELECT CAST(3 AS BIGINT), 'z') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False), ("b", _STR, False)],
            {"k": [1], "a": ["a"], "b": ["x"]},
        ),
        repark=None,
        note="control equality: basic equi-join INNER; both engines agree on value AND type.",
    ),
    JoinRow(
        name="control_left_equality",
        kind="content",
        entry="sql",
        family="control",
        sql=(
            "SELECT l.k, l.a, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'b') l "
            "LEFT JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False), ("b", _STR, True)],
            {"k": [1, 2], "a": ["a", "b"], "b": ["x", None]},
        ),
        repark=None,
        note="control equality: LEFT JOIN preserves left-unmatched row; right payload nullable.",
    ),
    # ----- 1. NULL join keys - every join type; NULL never matches NULL --------------------------
    JoinRow(
        name="null_keys_inner_no_match",
        kind="content",
        entry="sql",
        family="null_keys",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l "
            "INNER JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "            UNION ALL SELECT CAST(NULL AS BIGINT), 'y') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, False),
                ("rk", _I64, True),
                ("b", _STR, False),
            ],
            {"lk": [1], "a": ["a"], "rk": [1], "b": ["x"]},
        ),
        repark=None,
        note=(
            "SQL three-valued logic: NULL = NULL is unknown, so NULL keys never match on INNER. "
            "Only the non-null key pair survives."
        ),
    ),
    JoinRow(
        name="null_keys_left_outer_fate",
        kind="content",
        entry="sql",
        family="null_keys",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l "
            "LEFT JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "           UNION ALL SELECT CAST(NULL AS BIGINT), 'y') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, False),
                ("rk", _I64, True),
                ("b", _STR, True),
            ],
            {"lk": [1, None], "a": ["a", "n"], "rk": [1, None], "b": ["x", None]},
        ),
        repark=None,
        note=(
            "LEFT OUTER: left NULL-key row is preserved with right-side NULLs (no match). "
            "Right NULL-key row is not injected."
        ),
    ),
    JoinRow(
        name="null_keys_right_outer_fate",
        kind="content",
        entry="sql",
        family="null_keys",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l "
            "RIGHT JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "            UNION ALL SELECT CAST(NULL AS BIGINT), 'y') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, True),
                ("rk", _I64, True),
                ("b", _STR, False),
            ],
            {"lk": [1, None], "a": ["a", None], "rk": [1, None], "b": ["x", "y"]},
        ),
        repark=None,
        note=(
            "RIGHT OUTER: right NULL-key row is preserved with left-side NULLs. "
            "Left NULL-key row is not injected."
        ),
    ),
    JoinRow(
        name="null_keys_full_outer_fate",
        kind="content",
        entry="sql",
        family="null_keys",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l "
            "FULL OUTER JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "                 UNION ALL SELECT CAST(NULL AS BIGINT), 'y') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, True),
                ("rk", _I64, True),
                ("b", _STR, True),
            ],
            {
                "lk": [1, None, None],
                "a": ["a", "n", None],
                "rk": [1, None, None],
                "b": ["x", None, "y"],
            },
        ),
        repark=None,
        note=(
            "FULL OUTER: both NULL-key rows survive unmatched (NULL never equals NULL). "
            "Three rows total: the match on k=1 plus each side's NULL-key orphan."
        ),
    ),
    JoinRow(
        name="null_safe_equal_matches_nulls",
        kind="content",
        entry="sql",
        family="null_keys",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l "
            "INNER JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "            UNION ALL SELECT CAST(NULL AS BIGINT), 'y') r "
            "ON l.k <=> r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, False),
                ("rk", _I64, True),
                ("b", _STR, False),
            ],
            {"lk": [1, None], "a": ["a", "n"], "rk": [1, None], "b": ["x", "y"]},
        ),
        repark=None,
        note=(
            "Spark null-safe equal (``<=>`` / IS NOT DISTINCT FROM): NULL matches NULL. "
            "Both key pairs survive the INNER join."
        ),
    ),
    # ----- 2. Duplicate keys - mxn fan-out, order-insensitive ------------------------------------
    JoinRow(
        name="duplicate_keys_inner_2x2_fanout",
        kind="content",
        entry="sql",
        family="duplicate_keys",
        sql=(
            "SELECT l.k, l.a, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a1' AS a "
            "      UNION ALL SELECT CAST(1 AS BIGINT), 'a2') l "
            "INNER JOIN (SELECT CAST(1 AS BIGINT) AS k, 'b1' AS b "
            "            UNION ALL SELECT CAST(1 AS BIGINT), 'b2') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False), ("b", _STR, False)],
            {
                "k": [1, 1, 1, 1],
                "a": ["a1", "a1", "a2", "a2"],
                "b": ["b1", "b2", "b1", "b2"],
            },
        ),
        repark=None,
        note=(
            "2x2 fan-out: both sides duplicate the join key -> 4 result rows. "
            "Order-insensitive compare (parity comparator default)."
        ),
    ),
    JoinRow(
        name="duplicate_keys_left_with_unmatched",
        kind="content",
        entry="sql",
        family="duplicate_keys",
        sql=(
            "SELECT l.k, l.a, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a1' AS a "
            "      UNION ALL SELECT CAST(1 AS BIGINT), 'a2' "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'solo') l "
            "LEFT JOIN (SELECT CAST(1 AS BIGINT) AS k, 'b1' AS b "
            "           UNION ALL SELECT CAST(1 AS BIGINT), 'b2') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False), ("b", _STR, True)],
            {
                "k": [1, 1, 1, 1, 2],
                "a": ["a1", "a1", "a2", "a2", "solo"],
                "b": ["b1", "b2", "b1", "b2", None],
            },
        ),
        repark=None,
        note=(
            "LEFT fan-out with unmatched left key: 2x2 matches on k=1 plus one NULL-padded "
            "solo row on k=2."
        ),
    ),
    # ----- 3. Missing types - semi / anti / cross -----------------------------------------------
    JoinRow(
        name="cross_join_sql",
        kind="content",
        entry="sql",
        family="missing_type",
        sql=(
            "SELECT l.a, r.b "
            "FROM (SELECT 'a' AS a UNION ALL SELECT 'b') l "
            "CROSS JOIN (SELECT 'x' AS b UNION ALL SELECT 'y') r"
        ),
        spark=_table(
            [("a", _STR, False), ("b", _STR, False)],
            {"a": ["a", "a", "b", "b"], "b": ["x", "y", "x", "y"]},
        ),
        repark=None,
        note="CROSS JOIN (SQL door): 2x2 Cartesian product. Both engines agree.",
    ),
    JoinRow(
        name="left_semi_sql",
        kind="content",
        entry="sql",
        family="missing_type",
        sql=(
            "SELECT l.k, l.a "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'b') l "
            "LEFT SEMI JOIN (SELECT CAST(1 AS BIGINT) AS k) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False)],
            {"k": [1], "a": ["a"]},
        ),
        repark=None,
        note="LEFT SEMI (SQL door): keeps left rows with a match; right columns dropped.",
    ),
    JoinRow(
        name="left_anti_sql",
        kind="content",
        entry="sql",
        family="missing_type",
        sql=(
            "SELECT l.k, l.a "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'b') l "
            "LEFT ANTI JOIN (SELECT CAST(1 AS BIGINT) AS k) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False)],
            {"k": [2], "a": ["b"]},
        ),
        repark=None,
        note="LEFT ANTI (SQL door): keeps left rows with no match.",
    ),
    JoinRow(
        name="left_semi_null_keys_no_match",
        kind="content",
        entry="sql",
        family="missing_type",
        sql=(
            "SELECT l.k, l.a "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(NULL AS BIGINT), 'n') l "
            "LEFT SEMI JOIN (SELECT CAST(NULL AS BIGINT) AS k) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, True), ("a", _STR, False)],
            {"k": [], "a": []},
        ),
        repark=None,
        note=(
            "LEFT SEMI + NULL keys: NULL = NULL is unknown -> no semi match; empty result. "
            "Pins the three-valued-logic edge on the semi surface."
        ),
    ),
    # ----- 4. Type-mismatched keys ---------------------------------------------------------------
    JoinRow(
        name="type_mismatch_int_string_key",
        kind="content",
        entry="sql",
        family="type_mismatch",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a) l "
            "INNER JOIN (SELECT '1' AS k, 'x' AS b) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, False),
                ("a", _STR, False),
                ("rk", _STR, False),
                ("b", _STR, False),
            ],
            {"lk": [1], "a": ["a"], "rk": ["1"], "b": ["x"]},
        ),
        repark=None,
        note="int64 key vs string '1': implicit cast yields a match; both engines agree.",
    ),
    JoinRow(
        name="type_mismatch_int_decimal_key",
        kind="content",
        entry="sql",
        family="type_mismatch",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a) l "
            "INNER JOIN (SELECT CAST(1.0 AS DECIMAL(10,2)) AS k, 'x' AS b) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, False),
                ("a", _STR, False),
                ("rk", _DEC_10_2, False),
                ("b", _STR, False),
            ],
            {"lk": [1], "a": ["a"], "rk": [Decimal("1.00")], "b": ["x"]},
        ),
        repark=None,
        note="int64 key vs DECIMAL(10,2) 1.00: match; right key type preserved in projection.",
    ),
    JoinRow(
        name="type_mismatch_string_decimal_key",
        kind="content",
        entry="sql",
        family="type_mismatch",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT '1.00' AS k, 'a' AS a) l "
            "INNER JOIN (SELECT CAST(1.0 AS DECIMAL(10,2)) AS k, 'x' AS b) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _STR, False),
                ("a", _STR, False),
                ("rk", _DEC_10_2, False),
                ("b", _STR, False),
            ],
            {"lk": ["1.00"], "a": ["a"], "rk": [Decimal("1.00")], "b": ["x"]},
        ),
        repark=None,
        note="string '1.00' vs DECIMAL(10,2): match via implicit cast; both engines agree.",
    ),
    JoinRow(
        name="type_mismatch_string_decimal_malformed_raises",
        kind="error",
        entry="sql",
        family="type_mismatch",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT 'abc' AS k, 'a' AS a) l "
            "INNER JOIN (SELECT CAST(1.0 AS DECIMAL(10,2)) AS k, 'x' AS b) r "
            "ON l.k = r.k"
        ),
        spark=None,
        repark=None,
        spark_error_needle="CAST_INVALID_INPUT",
        repark_error_needle="Cast error",
        note=(
            "malformed string key vs decimal: BOTH engines refuse the cast. Tokens differ "
            "(Spark ANSI CAST_INVALID_INPUT; repark Arrow Cast error) - honest class compare, "
            "not invented support."
        ),
    ),
    # ----- 5. Outer-join schema nullability flips ------------------------------------------------
    JoinRow(
        name="left_outer_right_cols_nullable",
        kind="content",
        entry="sql",
        family="nullability",
        sql=(
            "SELECT l.k, l.a, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'b') l "
            "LEFT JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b) r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("k", _I64, False), ("a", _STR, False), ("b", _STR, True)],
            {"k": [1, 2], "a": ["a", "b"], "b": ["x", None]},
        ),
        repark=None,
        note=(
            "nullability-only class: LEFT JOIN flips the non-preserved (right) payload column "
            "to nullable=True while left non-null literals stay non-nullable."
        ),
    ),
    JoinRow(
        name="right_outer_left_cols_nullable",
        kind="content",
        entry="sql",
        family="nullability",
        sql=(
            "SELECT l.a, r.k, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a) l "
            "RIGHT JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "            UNION ALL SELECT CAST(2 AS BIGINT), 'y') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [("a", _STR, True), ("k", _I64, False), ("b", _STR, False)],
            {"a": ["a", None], "k": [1, 2], "b": ["x", "y"]},
        ),
        repark=None,
        note=(
            "RIGHT JOIN flips the non-preserved (left) payload ``a`` to nullable=True; "
            "right-side non-null literals stay non-nullable."
        ),
    ),
    JoinRow(
        name="full_outer_both_sides_nullable",
        kind="content",
        entry="sql",
        family="nullability",
        sql=(
            "SELECT l.k AS lk, l.a, r.k AS rk, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k, 'a' AS a "
            "      UNION ALL SELECT CAST(2 AS BIGINT), 'b') l "
            "FULL OUTER JOIN (SELECT CAST(1 AS BIGINT) AS k, 'x' AS b "
            "                 UNION ALL SELECT CAST(3 AS BIGINT), 'z') r "
            "ON l.k = r.k"
        ),
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, True),
                ("rk", _I64, True),
                ("b", _STR, True),
            ],
            {
                "lk": [1, 2, None],
                "a": ["a", "b", None],
                "rk": [1, None, 3],
                "b": ["x", None, "z"],
            },
        ),
        repark=None,
        note=(
            "FULL OUTER makes both sides' columns nullable (either side may be absent). "
            "Pins the schema-nullability class independently of the value fan-out."
        ),
    ),
    # multi-key control (still equality)
    JoinRow(
        name="multi_key_inner_equality",
        kind="content",
        entry="sql",
        family="control",
        sql=(
            "SELECT l.k1, l.k2, l.a, r.b "
            "FROM (SELECT CAST(1 AS BIGINT) AS k1, 'x' AS k2, 'a' AS a "
            "      UNION ALL SELECT CAST(1 AS BIGINT), 'y', 'b') l "
            "INNER JOIN (SELECT CAST(1 AS BIGINT) AS k1, 'x' AS k2, 'p' AS b "
            "            UNION ALL SELECT CAST(1 AS BIGINT), 'z', 'q') r "
            "ON l.k1 = r.k1 AND l.k2 = r.k2"
        ),
        spark=_table(
            [
                ("k1", _I64, False),
                ("k2", _STR, False),
                ("a", _STR, False),
                ("b", _STR, False),
            ],
            {"k1": [1], "k2": ["x"], "a": ["a"], "b": ["p"]},
        ),
        repark=None,
        note="composite equi-join on (k1, k2): only the matching pair survives.",
    ),
    # ----- 6. DataFrame-API door (CP-11) ---------------------------------------------------------
    JoinRow(
        name="df_join_inner_on_name",
        kind="content",
        entry="df",
        family="df_api",
        left_rows=[(1, "a"), (2, "b")],
        right_rows=[(1, "x"), (3, "z")],
        left_columns=["k", "a"],
        right_columns=["k", "b"],
        on="k",
        how="inner",
        on_mode="name",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True), ("b", _STR, True)],
            {"k": [1], "a": ["a"], "b": ["x"]},
        ),
        repark=None,
        note=(
            "CP-11 DataFrame door: ``df.join(other, on='k', how='inner')``. "
            "createDataFrame columns are nullable; key is merged (Spark-style)."
        ),
    ),
    JoinRow(
        name="df_join_left_outer_on_name",
        kind="content",
        entry="df",
        family="df_api",
        left_rows=[(1, "a"), (2, "b")],
        right_rows=[(1, "x")],
        left_columns=["k", "a"],
        right_columns=["k", "b"],
        on="k",
        how="left",
        on_mode="name",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True), ("b", _STR, True)],
            {"k": [1, 2], "a": ["a", "b"], "b": ["x", None]},
        ),
        repark=None,
        note="CP-11 DataFrame door: left outer join on shared name; unmatched left row preserved.",
    ),
    JoinRow(
        name="df_join_eq_null_safe",
        kind="content",
        entry="df",
        family="df_api",
        left_rows=[(1, "a"), (None, "n")],
        right_rows=[(1, "x"), (None, "y")],
        left_columns=["k", "a"],
        right_columns=["k", "b"],
        on="k",
        how="inner",
        on_mode="eq_null_safe",
        select_eq_null_safe=True,
        spark=_table(
            [
                ("lk", _I64, True),
                ("a", _STR, True),
                ("rk", _I64, True),
                ("b", _STR, True),
            ],
            {"lk": [1, None], "a": ["a", "n"], "rk": [1, None], "b": ["x", "y"]},
        ),
        repark=None,
        note=(
            "CP-11 DataFrame door: ``left.k.eqNullSafe(right.k)`` condition join matches NULLs. "
            "Post-join select aliases keys to avoid duplicate column names in Arrow."
        ),
    ),
    # ----- 7. DataFrame-door semi family ---------------------------------------------------------
    JoinRow(
        name="df_left_semi_on_name",
        kind="content",
        entry="df",
        family="missing_type",
        left_rows=[(1, "a"), (2, "b")],
        right_rows=[(1,)],
        left_columns=["k", "a"],
        right_columns=["k"],
        on="k",
        how="leftsemi",
        on_mode="name",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True)],
            {"k": [1], "a": ["a"]},
        ),
        repark=None,
        note=(
            "G4b CONVERGED (was a refuse split): DataFrame ``how='leftsemi'`` on a name key. "
            "Output is the LEFT side's columns only - no key merge, no right-hand column. The "
            "recorded Spark half is UNCHANGED from the split; only repark's side moved, and the "
            "harness's own split classifier reported CONVERGED against this exact golden. "
            "The row NAME is kept byte-identical on purpose - a pin's name is part of the pin, "
            "so the (now historical) '_unsupported' suffix is retired by a declared-rename unit, "
            "never smuggled into a behaviour change."
        ),
    ),
    JoinRow(
        name="df_left_anti_on_name",
        kind="content",
        entry="df",
        family="missing_type",
        left_rows=[(1, "a"), (2, "b")],
        right_rows=[(1,)],
        left_columns=["k", "a"],
        right_columns=["k"],
        on="k",
        how="leftanti",
        on_mode="name",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True)],
            {"k": [2], "a": ["b"]},
        ),
        repark=None,
        note=(
            "G4b CONVERGED (was a refuse split): DataFrame ``how='leftanti'`` on a name key - "
            "the exact complement of the semi row above on the same inputs, so neither can be "
            "satisfied by an empty-result bug. Name kept byte-identical (see the semi row)."
        ),
    ),
    JoinRow(
        name="df_left_semi_on_condition",
        kind="content",
        entry="df",
        family="missing_type",
        left_rows=[(1, "a"), (2, "b")],
        right_rows=[(1,)],
        left_columns=["k", "a"],
        right_columns=["k"],
        on="k",
        how="leftsemi",
        on_mode="condition",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True)],
            {"k": [1], "a": ["a"]},
        ),
        repark=None,
        note=(
            "G4b: ``leftsemi`` through the H1 Column-condition path (``left.k == right.k``), a "
            "DIFFERENT engine path from the name-key row (SQL rewrite, not ``join_on_names``). "
            "Spark keeps the left schema for a condition semi join too - the condition join's "
            "usual all-columns-from-both-sides rule does not apply to the semi family."
        ),
    ),
    JoinRow(
        name="df_left_anti_on_condition",
        kind="content",
        entry="df",
        family="missing_type",
        left_rows=[(1, "a"), (2, "b")],
        right_rows=[(1,)],
        left_columns=["k", "a"],
        right_columns=["k"],
        on="k",
        how="leftanti",
        on_mode="condition",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True)],
            {"k": [2], "a": ["b"]},
        ),
        repark=None,
        note=(
            "G4b: ``leftanti`` through the H1 Column-condition path; left schema, complement rows."
        ),
    ),
    JoinRow(
        name="df_left_semi_null_keys_no_match",
        kind="content",
        entry="df",
        family="missing_type",
        left_rows=[(1, "a"), (None, "n")],
        right_rows=[(9,), (None,)],
        left_columns=["k", "a"],
        right_columns=["k"],
        on="k",
        how="leftsemi",
        on_mode="name_list",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True)],
            {"k": [], "a": []},
        ),
        repark=None,
        note=(
            "G4b: DF door mirror of ``left_semi_null_keys_no_match`` (SQL door). NULL = NULL is "
            "unknown, so the NULL-keyed left row does NOT match the right side's NULL key, and "
            "k=1 has no partner among the right's real keys - empty result, left schema "
            "preserved. The right side carries a non-matching REAL key (9) beside the NULL: an "
            "all-NULL column is not inferable by Spark's ``createDataFrame`` (CANNOT_DETERMINE_"
            "TYPE), and it also proves the emptiness comes from the NULL logic rather than from "
            "an empty right side. Uses the ``on=['k']`` LIST shape so the list entry point is "
            "pinned too, not only ``on='k'``."
        ),
    ),
    JoinRow(
        name="df_left_anti_null_keys_keeps_row",
        kind="content",
        entry="df",
        family="missing_type",
        left_rows=[(1, "a"), (None, "n")],
        right_rows=[(9,), (None,)],
        left_columns=["k", "a"],
        right_columns=["k"],
        on="k",
        how="leftanti",
        on_mode="name_list",
        spark=_table(
            [("k", _I64, True), ("a", _STR, True)],
            {"k": [1, None], "a": ["a", "n"]},
        ),
        repark=None,
        note=(
            "G4b: the anti side of the same NULL-key edge - because NULL never matches, the "
            "NULL-keyed left row is KEPT (three-valued logic, not 'NULL is dropped'). Complement "
            "of the semi row on identical inputs, so an all-empty or all-pass bug reds one of "
            "the pair. ``on=['k']`` LIST shape."
        ),
    ),
]


# Session builders


def _repark_session() -> ReparkSession:
    """A repark facade session (no catalog required - pure SQL / createDataFrame)."""
    from repark import ReparkSession

    return ReparkSession.builder.appName("join-parity").getOrCreate()


# The rows


@pytest.fixture
def repark() -> Iterator[ReparkSession]:
    """Repark session for the facade door."""
    session = _repark_session()
    try:
        yield session
    finally:
        with contextlib.suppress(Exception):
            session.stop()


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_join_parity_row(row: JoinRow, repark: ReparkSession) -> None:
    """Every recorded row on the Arrow path (value AND type AND nullability) or error class.

    Content equality rows assert ``repark == Spark``. Content disclosure rows assert repark's
    pinned output, with failures CLASSIFIED (CONVERGED vs regression). Error rows assert both
    engines' needles appear in the raised message. Split rows assert repark still refuses with
    its needle AND the recorded Spark half is well-formed; if repark starts succeeding, the
    failure is CLASSIFIED (CONVERGED -> flip to content equality vs regression).
    """
    if row.kind == "error":
        assert row.repark_error_needle is not None
        message = run_join_expect_error(repark, row)
        assert row.repark_error_needle in message, (
            f"{row.name}: repark error missing {row.repark_error_needle!r}: {message!r}. {row.note}"
        )
        return

    if row.kind == "split":
        # Drive the real lifecycle so a future accepting engine is CLASSIFIED (CONVERGED vs
        # regression).
        assert row.repark_error_needle is not None
        assert row.spark is not None
        try:
            actual = run_join_content(repark, row)
        except Exception as exc:  # both engines' error types; message is the pin
            message = str(exc)
            assert row.repark_error_needle in message, (
                f"{row.name}: repark was expected to refuse with {row.repark_error_needle!r}, "
                f"got: {message!r}. {row.note}"
            )
            assert row.spark.num_rows >= 1, f"{row.name}: spark golden is empty - re-record"
            return

        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED - repark now succeeds with the "
                f"RECORDED SPARK output, so this split disclosure is stale. Do not delete the "
                f"row: flip it to a content equality row (kind='content', repark=None, clear the "
                f"error needle) and record the convergence. {row.note}"
            )
        raise AssertionError(
            f"{row.name}: repark no longer refuses (recipe committed) but the result does NOT "
            f"match the recorded Spark golden - this is a regression/partial change, not a clean "
            f"convergence. Re-derive both halves in record mode (see this module's docstring) "
            f"before flipping the pin. {row.note}"
        )

    # kind == "content"
    assert row.spark is not None
    actual = run_join_content(repark, row)

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


def test_join_row_set_covers_g4_budget() -> None:
    """The pin budget is part of the unit - corpus size and class coverage are pinned.

    Family coverage pins are name-gated so a control row cannot satisfy them (CP-2);
    entry-point coverage pins the DF door (CP-11).
    """
    assert G4_BUDGET_MIN <= len(ROWS) <= G4_BUDGET_MAX, (
        f"G4 budget {G4_BUDGET_MIN}-{G4_BUDGET_MAX} differential rows (got {len(ROWS)})"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure_or_split()]
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"at least {MIN_EQUALITY_ROWS} control equality rows required so the corpus cannot "
        f"degenerate to all-disclosures; got {len(equalities)}"
    )
    assert len(disclosures) <= MAX_DISCLOSURE_OR_SPLIT_ROWS, (
        f"at most {MAX_DISCLOSURE_OR_SPLIT_ROWS} disclosures/splits so the corpus cannot silently "
        f"absorb every regression as a new disclosure; got {len(disclosures)}"
    )

    names = {row.name for row in ROWS}

    # 1. NULL join keys - name-gated prefix null_keys_* covers every join type (not a control;
    # not left_semi_null_keys_* which is a semi-family edge, CP-2).
    null_key_rows = [name for name in names if name.startswith("null_keys_")]
    assert len(null_key_rows) >= MIN_NULL_KEY_ROWS, (
        f"need >={MIN_NULL_KEY_ROWS} rows named null_keys_* (every join type); got {null_key_rows}"
    )
    for join_type in ("inner", "left", "right", "full"):
        assert any(name.startswith(f"null_keys_{join_type}") for name in null_key_rows), (
            f"null_keys family missing join type {join_type!r} in {null_key_rows}"
        )
    assert any("null_safe" in name for name in names), (
        "must pin null-safe equal (<=>) where supported"
    )

    # 2. Duplicate keys - name-gated.
    dup_rows = [name for name in names if "duplicate_keys_" in name]
    assert len(dup_rows) >= MIN_DUPLICATE_KEY_ROWS, (
        f"need >={MIN_DUPLICATE_KEY_ROWS} *duplicate_keys_* rows; got {dup_rows}"
    )

    # 3. Missing types - semi / anti / cross present.
    assert any("cross" in name for name in names), "must pin CROSS JOIN"
    assert any("semi" in name for name in names), "must pin LEFT SEMI"
    assert any("anti" in name for name in names), "must pin LEFT ANTI"
    # The two former DF refuse splits are now content equalities; a re-refuse of the surface
    # reds here as well as on the row itself.
    for flipped_name in ("df_left_semi_on_name", "df_left_anti_on_name"):
        flipped = next(row for row in ROWS if row.name == flipped_name)
        assert flipped.kind == "content", (
            f"{flipped_name} is a G4b content equality; a split here means the DataFrame semi "
            "surface stopped working (do not re-record the refuse needle to make it green)"
        )
        assert flipped.repark is None, f"{flipped_name} asserts repark == Spark, not a pin"
        assert flipped.repark_error_needle is None, f"{flipped_name} must not keep a refuse needle"
        assert flipped.spark is not None
    # Entry-point matrix: the semi family is pinned on the DF door across both `on` shapes and
    # the NULL-key edge - name-gated so no single control row can satisfy the family (CP-2).
    df_semi_family = [row for row in ROWS if row.entry == "df" and row.family == "missing_type"]
    df_semi_modes = {(row.how, row.on_mode) for row in df_semi_family}
    for how in ("leftsemi", "leftanti"):
        assert (how, "condition") in df_semi_modes, (
            f"DF door must pin how={how!r} through the Column-condition path; got {df_semi_modes}"
        )
        assert {(how, "name"), (how, "name_list")} & df_semi_modes, (
            f"DF door must pin how={how!r} through a name/list key; got {df_semi_modes}"
        )
    for null_edge in ("df_left_semi_null_keys_no_match", "df_left_anti_null_keys_keeps_row"):
        assert null_edge in names, f"DF door must pin the semi-family NULL-key edge: {null_edge}"
    assert all(row.kind == "content" and row.repark is None for row in df_semi_family), (
        "every DF semi-family row is a plain repark == Spark equality after G4b"
    )

    # 4. Type-mismatched keys - name-gated.
    mismatch_rows = [name for name in names if "type_mismatch_" in name]
    assert len(mismatch_rows) >= MIN_TYPE_MISMATCH_ROWS, (
        f"need >={MIN_TYPE_MISMATCH_ROWS} *type_mismatch_* rows; got {mismatch_rows}"
    )

    # 5. Outer-join nullability - name-gated *nullable* (control_left does NOT satisfy).
    nullable_rows = [name for name in names if "nullable" in name]
    assert len(nullable_rows) >= MIN_NULLABILITY_ROWS, (
        f"need >={MIN_NULLABILITY_ROWS} *nullable* schema rows; got {nullable_rows}. "
        "A LEFT JOIN control equality alone does not satisfy this."
    )

    # 6. Entry points - CP-11 DF door.
    df_rows = [row for row in ROWS if row.entry == "df"]
    assert len(df_rows) >= MIN_DF_API_ROWS, (
        f"need >={MIN_DF_API_ROWS} DataFrame-API rows (CP-11); got {len(df_rows)}"
    )
    df_content = [row for row in df_rows if row.kind == "content"]
    assert len(df_content) >= MIN_DF_API_ROWS, (
        f"need >={MIN_DF_API_ROWS} DF content rows, not only refuse splits; got {len(df_content)}"
    )
    assert any(row.entry == "sql" for row in ROWS), "sql() door must remain primary"

    # Well-formedness.
    for row in ROWS:
        if row.kind == "content":
            assert row.spark is not None, f"{row.name}: content needs spark golden"
            if row.entry == "sql":
                assert row.sql is not None
            else:
                assert row.left_rows is not None and row.right_rows is not None
        if row.kind == "error":
            assert row.spark_error_needle is not None
            assert row.repark_error_needle is not None
            assert row.spark is None and row.repark is None
        if row.kind == "split":
            assert row.spark is not None
            assert row.repark_error_needle is not None
            assert row.repark is None


# Classifier reachability (CP-1) - both arms proven by monkeypatch

# The split classifier has no live split row in today's corpus; the classifier is HARNESS
# machinery, not a corpus pin, so its coverage must not die with the corpus. This probe row
# keeps both arms provable; it is deliberately NOT in ROWS (unrecorded golden, budget count).
_CLASSIFIER_PROBE_SPLIT = JoinRow(
    name="_classifier_probe_split",
    kind="split",
    entry="df",
    family="harness_probe",
    left_rows=[(1, "a"), (2, "b")],
    right_rows=[(1,)],
    left_columns=["k", "a"],
    right_columns=["k"],
    on="k",
    how="leftsemi",
    on_mode="name",
    spark=_table([("k", _I64, True), ("a", _STR, True)], {"k": [1], "a": ["a"]}),
    repark=None,
    repark_error_needle="Unsupported join type",
    note="Harness probe for the split classifier's two arms - not a recorded corpus row.",
)


def test_split_classifier_converged_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: split refuse side matching the Spark golden -> CONVERGED flip guidance."""
    import test_join_parity as join_mod

    split_row = _CLASSIFIER_PROBE_SPLIT
    assert split_row.spark is not None
    golden = split_row.spark

    def _fake_success(_session: Any, _row: JoinRow) -> pa.Table:
        return golden

    monkeypatch.setattr(join_mod, "run_join_content", _fake_success)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_join_parity_row(split_row, repark)
    message = str(excinfo.value)
    assert "flip it to a content equality" in message
    assert "Do not delete" in message


def test_split_classifier_regression_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: split commits a non-Spark result -> regression guidance (not bare assert)."""
    import test_join_parity as join_mod

    split_row = _CLASSIFIER_PROBE_SPLIT
    wrong = _table(
        [("k", _I64, True), ("a", _STR, True)],
        {"k": [99], "a": ["WRONG"]},
    )

    def _fake_wrong(_session: Any, _row: JoinRow) -> pa.Table:
        return wrong

    monkeypatch.setattr(join_mod, "run_join_content", _fake_wrong)

    with pytest.raises(AssertionError, match="regression") as excinfo:
        test_join_parity_row(split_row, repark)
    message = str(excinfo.value)
    assert "Re-derive" in message
    assert "not a clean convergence" in message
