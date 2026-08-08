"""The SQL-string filter-predicate rewriter (``DataFrame._quote_filter_sql_identifiers``).

``filter(str)`` / ``where(str)`` rewrite schema-bound identifiers to double-quoted canonical form
so DataFusion's unquoted lowercase fold cannot lose a mixed-case field. Three behaviours of that
rewriter are pinned here, per user entry point (**both** ``.filter`` and ``.where``) and on the
Arrow export path (``to_arrow`` — value AND type), never ``show``:

* **casefold collisions refuse at the REFERENCE** (audit G2). A frame carrying both ``id`` and
  ``ID`` is legal; Spark under ``spark.sql.caseSensitive=false`` raises ``AMBIGUOUS_REFERENCE`` only
  when the predicate names the colliding column. Filtering an unrelated column of that frame still
  runs. The two failure modes this sits between are both pinned: whole-frame refusal (the
  over-refusal) and last-write-wins (P4C5-Q-001 — silently binding the ident to the wrong column).
* **a token followed by ``(`` is a function call, not a column** (P5C5-Q-001): ``year(ts)`` stays a
  function even when a column named ``year`` exists — while bare ``year`` on the SAME frame is still
  rewritten.
* **SQL literal keywords are never bound to a same-named column**: ``true`` / ``false`` / ``null``
  keep their grammar meaning even on a frame with a column literally named ``true`` / ``false`` /
  ``null``. **All three** members of ``_SQL_LITERAL_KEYWORDS`` are pinned against a frame that
  actually carries a column of that name — dropping any one member reds this module.

Each keyword/lookahead pin carries its discriminator — the *rewritten* form the skip suppresses
(``"true"`` / ``"false"`` as a predicate, ``b IS NOT "null"``) is asserted to fail, so removing the
skip turns the test red rather than merely changing an unobserved plan.

**Oracle basis.** Every golden below was derived from **live PySpark 4.1.2** (``local[2]``, ANSI on,
``spark.sql.caseSensitive=false``, ``timeZone=UTC``) during audit G2, not hand-guessed. Two of the
recipes carry a standing live leg in ``_live_parity.py``
(``filter_unambiguous_on_case_colliding_frame``, ``filter_keyword_literal_false_column``) and the
three disclosed divergences below carry live ``DISCLOSURES`` entries, so the whole family is
re-derived from real Spark by the nightly ``make parity-live`` tier rather than pinned once and
forgotten. The remaining goldens here (function-call skip, ``null`` keyword, mixed-case survival)
are **hand-derived from that same oracle session with no standing live leg** — a JVM-free pin whose
drift detector is the two scenarios above plus the disclosures.

**Disclosed divergences from live Spark, characterized here so they cannot drift unobserved:**

* the :class:`Column` entry point (``df.filter(df["id"] > 0)``) does NOT refuse — it resolves
  exact-case-first and returns rows; live Spark raises ``AMBIGUOUS_REFERENCE``;
* an explicitly double-quoted ``'"ID" > 1'`` does NOT refuse — DataFusion resolves it
  case-sensitively; Spark reads ``"ID"`` as a string *literal* and raises ``CAST_INVALID_INPUT``;
* backtick-quoted identifiers are **not** a protected span — ``filter("`x` > 0")`` is corrupted
  into ``No field named \"\"\"x\"\"\"``; live Spark filters normally. Pre-existing (main had no
  backtick handling either); the fix belongs in a follow-up unit.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.dataframe import DataFrame
from repark.errors import AnalysisException, ParseException


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-filter-rewrite").getOrCreate()


def _collides(spark: ReparkSession) -> DataFrame:
    """A legal frame whose ``id``/``ID`` columns collide only by case; ``other`` does not."""
    return spark.createDataFrame([(1, 2, 3)], ["id", "ID", "other"])


# ---- casefold collisions refuse at the reference, not for the whole frame (audit G2) ----------


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_unambiguous_column_still_filters_on_a_case_colliding_frame(
    spark: ReparkSession, entry_point: str
) -> None:
    """The over-refusal regression: a collision elsewhere in the frame must not block a predicate
    that never references it. Live PySpark 4.1.2 (caseSensitive=false) runs this.
    """
    df = _collides(spark)
    kept = getattr(df, entry_point)("other > 0").to_arrow()
    dropped = getattr(df, entry_point)("other > 99").to_arrow()

    assert kept.to_pylist() == [{"id": 1, "ID": 2, "other": 3}]
    assert kept.schema.field("other").type == pa.int64()
    assert kept.schema.field("id").type == pa.int64()
    assert kept.schema.field("ID").type == pa.int64()
    # The predicate is really evaluated (not a pass-through that only looks green).
    assert dropped.to_pylist() == []
    assert dropped.schema.field("other").type == pa.int64()


@pytest.mark.parametrize("entry_point", ["filter", "where"])
@pytest.mark.parametrize("spelling", ["id", "ID", "Id"])
def test_ambiguous_reference_raises_analysis_exception(
    spark: ReparkSession, entry_point: str, spelling: str
) -> None:
    """Referencing the colliding name — in ANY spelling — is a loud refusal, never last-write-wins
    (P4C5-Q-001: the ident used to silently bind to whichever column was seen last).
    """
    df = _collides(spark)
    with pytest.raises(AnalysisException, match="ambiguous"):
        getattr(df, entry_point)(f"{spelling} > 0").to_arrow()


def test_ambiguous_reference_error_uses_the_spark_message_shape(spark: ReparkSession) -> None:
    """The refusal carries Spark's ``[AMBIGUOUS_REFERENCE]`` condition tag and sentence, verbatim.

    Live PySpark 4.1.2 on this exact frame emits::

        [AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`id`, `id`]. SQLSTATE: 42704

    Two recorded, deliberate differences (see ``_quote_filter_sql_identifiers``): repark lists the
    ACTUAL colliding columns where Spark echoes the reference spelling once per candidate, and
    repark omits the ``SQLSTATE`` suffix (no repark error carries one).
    """
    with pytest.raises(AnalysisException) as excinfo:
        _collides(spark).filter("id > 0")
    message = str(excinfo.value)

    assert message == "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`id`, `ID`]."
    # The recorded differences are asserted, not merely commented, so a later "parity" edit that
    # adds SQLSTATE or copies Spark's duplicated-spelling candidate list has to update this pin.
    assert "SQLSTATE" not in message


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_colliding_name_inside_a_string_literal_does_not_refuse(
    spark: ReparkSession, entry_point: str
) -> None:
    """Only real references count: ``'ID'`` inside a single-quoted literal is data, not an ident."""
    df = spark.createDataFrame([(1, 2, "ID"), (3, 4, "other")], ["id", "ID", "b"])
    got = getattr(df, entry_point)("b = 'ID'").to_arrow()

    assert got.to_pylist() == [{"id": 1, "ID": 2, "b": "ID"}]
    assert got.schema.field("b").type == pa.string()
    assert got.schema.field("id").type == pa.int64()


# ---- disclosed bypasses of the refusal + the unprotected backtick span (audit G2 review) ------


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_column_entry_point_bypasses_the_ambiguity_refusal(
    spark: ReparkSession, entry_point: str
) -> None:
    """DISCLOSED DIVERGENCE (not a fix): the ``Column`` form never reaches the rewriter, so it does
    not refuse — it resolves **exact-case-first**. Live PySpark 4.1.2 raises ``AMBIGUOUS_REFERENCE``
    for ``df.filter(df["id"] > 0)``. Characterized so a later ``_resolve_getitem_column_name``
    refactor cannot flip it in either direction unobserved (live leg:
    ``filter_case_collision_bypasses``).
    """
    df = _collides(spark)
    lower = getattr(df, entry_point)(df["id"] > 1).to_arrow()
    upper = getattr(df, entry_point)(df["ID"] > 1).to_arrow()

    # `id` is 1 and `ID` is 2 — the predicate discriminates WHICH column each spelling bound to.
    assert lower.to_pylist() == []
    assert upper.to_pylist() == [{"id": 1, "ID": 2, "other": 3}]
    assert upper.schema.field("id").type == pa.int64()
    assert upper.schema.field("ID").type == pa.int64()


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_explicitly_double_quoted_ident_bypasses_the_ambiguity_refusal(
    spark: ReparkSession, entry_point: str
) -> None:
    """DISCLOSED DIVERGENCE (not a fix): an already-double-quoted span is a protected span, so it
    is passed through and DataFusion resolves it case-**sensitively** rather than refusing. Spark
    does not even read ``"ID"`` as an identifier — it is a string literal there, and live PySpark
    4.1.2 raises ``CAST_INVALID_INPUT`` on ``'"ID" > 1'``.
    """
    df = _collides(spark)
    upper = getattr(df, entry_point)('"ID" > 1').to_arrow()
    lower = getattr(df, entry_point)('"id" > 1').to_arrow()

    assert upper.to_pylist() == [{"id": 1, "ID": 2, "other": 3}]
    assert upper.schema.field("ID").type == pa.int64()
    assert lower.to_pylist() == []
    assert lower.schema.field("id").type == pa.int64()


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_backtick_quoted_identifier_is_not_a_protected_span(
    spark: ReparkSession, entry_point: str
) -> None:
    """DISCLOSED HOLE (pre-existing, not a G2 regression): backticks — Spark's own quoting
    spelling — are NOT protected, so the token inside them is rewritten and DataFusion re-quotes
    the result. The user sees a field spelling they never wrote. Live PySpark 4.1.2 filters
    normally. Pinned with the observed text so the follow-up fix has to update this test.
    """
    df = spark.createDataFrame([(1, 2)], ["x", "b"])
    with pytest.raises(AnalysisException) as excinfo:
        getattr(df, entry_point)("`x` > 0").to_arrow()

    assert 'No field named """x"""' in str(excinfo.value)


# ---- a token followed by `(` is a function call, not a column (P5C5-Q-001) --------------------


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_function_call_is_not_rewritten_when_a_same_named_column_exists(
    spark: ReparkSession, entry_point: str
) -> None:
    """``year(ts)`` stays the date function on a frame that also has a column named ``year``."""
    df = spark.sql(
        "SELECT 1 AS year, TIMESTAMP '2020-05-01 00:00:00' AS ts "
        "UNION ALL SELECT 2, TIMESTAMP '2021-05-01 00:00:00'"
    )
    got = getattr(df, entry_point)("year(ts) = 2020").to_arrow()

    assert got.num_rows == 1
    assert got.column("year").to_pylist() == [1]
    assert got.schema.field("year").type == pa.int64()
    assert pa.types.is_timestamp(got.schema.field("ts").type)


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_function_call_survives_a_case_differing_same_named_column(
    spark: ReparkSession, entry_point: str
) -> None:
    """The discriminating shape for the call-site skip: the column is ``YEAR`` and the predicate
    calls ``year(ts)``. Rewriting the token would emit ``"YEAR"(ts)`` — DataFusion resolves
    function names case-SENSITIVELY, so that is ``Invalid function 'YEAR'`` (P5C5-Q-001). The
    lowercase-column case above cannot catch this: ``"year"(ts)`` still resolves.
    """
    df = spark.sql(
        "SELECT 1 AS \"YEAR\", TIMESTAMP '2020-05-01 00:00:00' AS ts "
        "UNION ALL SELECT 2, TIMESTAMP '2021-05-01 00:00:00'"
    )
    got = getattr(df, entry_point)("year(ts) = 2020").to_arrow()

    assert got.column("YEAR").to_pylist() == [1]
    assert got.schema.field("YEAR").type == pa.int64()
    assert pa.types.is_timestamp(got.schema.field("ts").type)
    # …and the bare column reference on that same frame is still rewritten, in either spelling.
    assert getattr(df, entry_point)("YEAR > 1").to_arrow().column("YEAR").to_pylist() == [2]
    assert getattr(df, entry_point)("year > 1").to_arrow().column("YEAR").to_pylist() == [2]


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_bare_column_of_a_function_name_is_still_rewritten(
    spark: ReparkSession, entry_point: str
) -> None:
    """The lookahead is scoped to call sites: bare ``year`` on the SAME frame still binds to the
    column (this is the discriminator — a blanket function-name skip would drop the rewrite).
    """
    df = spark.sql(
        "SELECT 1 AS year, TIMESTAMP '2020-05-01 00:00:00' AS ts "
        "UNION ALL SELECT 2, TIMESTAMP '2021-05-01 00:00:00'"
    )
    got = getattr(df, entry_point)("year > 1").to_arrow()

    assert got.column("year").to_pylist() == [2]
    assert got.schema.field("year").type == pa.int64()


def test_mixed_case_column_survives_the_rewrite(spark: ReparkSession) -> None:
    """The reason the rewriter exists: after a requested-spelling projection the field is ``X``,
    and an unquoted ``X`` would fold to ``x`` and fail to resolve.
    """
    df = spark.createDataFrame([(1,), (5,)], ["x"]).select("X")
    got = df.filter("X > 1").to_arrow()

    assert got.to_pylist() == [{"X": 5}]
    assert got.schema.field("X").type == pa.int64()
    assert df.where("x > 1").to_arrow().column("X").to_pylist() == [5]


# ---- SQL literal keywords are never bound to a same-named column ------------------------------


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_boolean_literals_are_not_bound_to_a_column_named_true(
    spark: ReparkSession, entry_point: str
) -> None:
    """A column literally named ``true`` is reachable through ``createDataFrame``; the predicate
    ``true`` must stay the boolean literal (Spark's grammar takes the keyword).
    """
    df = spark.createDataFrame([(1, 2), (3, 4)], ["true", "b"])
    kept = getattr(df, entry_point)("true").to_arrow()
    dropped = getattr(df, entry_point)("false").to_arrow()

    assert kept.to_pylist() == [{"true": 1, "b": 2}, {"true": 3, "b": 4}]
    assert kept.schema.field("true").type == pa.int64()
    assert kept.schema.field("b").type == pa.int64()
    assert dropped.to_pylist() == []
    assert dropped.schema.field("true").type == pa.int64()


def test_bound_true_column_predicate_is_the_discriminator(spark: ReparkSession) -> None:
    """Removing the keyword skip would produce exactly this predicate — and it does not plan, so
    the pin above is load-bearing rather than incidentally green.
    """
    df = spark.createDataFrame([(1, 2), (3, 4)], ["true", "b"])
    with pytest.raises(AnalysisException, match="non-boolean predicate"):
        df.filter('"true"').to_arrow()


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_false_keyword_is_not_bound_to_a_column_named_false(
    spark: ReparkSession, entry_point: str
) -> None:
    """The third member of ``_SQL_LITERAL_KEYWORDS`` needs its own frame: on a ``["true", "b"]``
    frame the token ``false`` matches no column, so the skip is never consulted for it and the
    member is unpinned (audit G2 review G2-C-001 — mutation-proved: dropping ``"false"`` left the
    whole suite green). Here the column IS named ``false``, so the keyword and the column collide.

    Live PySpark 4.1.2 on this exact frame: ``filter("false")`` → 0 rows, ``filter("true")`` → 2.
    """
    df = spark.createDataFrame([(1, 2), (3, 4)], ["false", "b"])
    dropped = getattr(df, entry_point)("false").to_arrow()
    kept = getattr(df, entry_point)("true").to_arrow()

    assert dropped.to_pylist() == []
    assert dropped.schema.field("false").type == pa.int64()
    assert dropped.schema.field("b").type == pa.int64()
    assert kept.to_pylist() == [{"false": 1, "b": 2}, {"false": 3, "b": 4}]
    assert kept.schema.field("false").type == pa.int64()
    assert kept.schema.field("b").type == pa.int64()
    # The keyword also survives inside a compound predicate (it is a token skip, not a whole-
    # predicate special case): `false` stays the literal while `b` is still rewritten.
    combined = getattr(df, entry_point)("b > 3 OR false").to_arrow()
    assert combined.to_pylist() == [{"false": 3, "b": 4}]
    assert combined.schema.field("b").type == pa.int64()


def test_bound_false_column_predicate_is_the_discriminator(spark: ReparkSession) -> None:
    """The rewrite the ``false`` skip suppresses: binding the token to the ``false`` column yields
    an Int64 predicate, which does not plan. Removing ``"false"`` from ``_SQL_LITERAL_KEYWORDS``
    turns the pin above into exactly this error.
    """
    df = spark.createDataFrame([(1, 2), (3, 4)], ["false", "b"])
    with pytest.raises(AnalysisException, match="non-boolean predicate"):
        df.filter('"false"').to_arrow()


@pytest.mark.parametrize("entry_point", ["filter", "where"])
def test_null_keyword_is_not_bound_to_a_column_named_null(
    spark: ReparkSession, entry_point: str
) -> None:
    """``IS NOT NULL`` stays the null test on a frame carrying a column named ``null``."""
    df = spark.createDataFrame([(1, 2), (5, None)], ["null", "b"])
    got = getattr(df, entry_point)("b IS NOT NULL").to_arrow()

    assert got.to_pylist() == [{"null": 1, "b": 2}]
    assert got.schema.field("b").type == pa.int64()
    assert got.schema.field("null").type == pa.int64()


def test_bound_null_column_predicate_is_the_discriminator(spark: ReparkSession) -> None:
    """The rewritten form the keyword skip suppresses is not even parseable."""
    df = spark.createDataFrame([(1, 2), (5, None)], ["null", "b"])
    with pytest.raises(ParseException):
        df.filter('b IS NOT "null"').to_arrow()


# ---- the upstream guard the writer helper's exact-duplicate branch depends on -----------------


def test_exact_duplicate_column_names_are_rejected_at_frame_construction(
    spark: ReparkSession,
) -> None:
    """``_by_name_casefold_map``'s exact-duplicate refusal is defensive: DataFusion rejects
    duplicate output names before any frame carrying them exists, on both construction paths. If
    this ever goes green-with-duplicates, that branch becomes live and needs a facade-level pin.

    DISCLOSED DIVERGENCE: live PySpark 4.1.2 **accepts** both of these (``Row(id=1, id=2)``) and
    only raises ``AMBIGUOUS_REFERENCE`` when the duplicate name is referenced. This is a
    construction-time divergence inherited from DataFusion, out of charter for audit G2 — recorded
    here so the branch's "unreachable" justification is not mistaken for Spark parity.
    """
    with pytest.raises(AnalysisException, match="unique expression names"):
        spark.createDataFrame([(1, 2)], ["id", "id"])
    with pytest.raises(AnalysisException, match="unique expression names"):
        spark.sql("SELECT 1 AS id, 2 AS id")
