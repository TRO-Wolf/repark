"""R-EXPLODE-REWRITE: explode / explode_outer via guarded DataFusion unnest."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.errors import (
    AnalysisException,
    ParseException,
    UnsupportedOperationException,
)


def _is_arrow_string(data_type: pa.DataType) -> bool:
    """True for utf8 / large_string / string_view (DataFusion may emit any of these)."""
    return (
        pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_string_view(data_type)
    )


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("explode").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> object:
    return spark.sql(
        """
        SELECT 1 AS id, make_array(10, 20) AS a
        UNION ALL SELECT 2, CAST(NULL AS BIGINT[])
        UNION ALL SELECT 3, make_array()
        UNION ALL SELECT 4, make_array(CAST(NULL AS BIGINT), 5)
        """
    )


def test_explode_drops_null_and_empty(frame: object) -> None:
    out = frame.select(frame.id, F.explode(frame.a).alias("e")).orderBy("id", "e")
    table = out.to_arrow()
    rows = table.to_pylist()
    # id=1 → 10,20; id=4 → null,5; id=2/3 dropped
    assert [(r["id"], r["e"]) for r in rows] == [
        (1, 10),
        (1, 20),
        (4, None),
        (4, 5),
    ]
    assert table.schema.field("e").type == pa.int64()


def test_explode_outer_keeps_null_and_empty(frame: object) -> None:
    out = frame.select(frame.id, F.explode_outer(frame.a).alias("e")).orderBy("id", "e")
    rows = out.to_arrow().to_pylist()
    by_id: dict[int, list] = {}
    for row in rows:
        by_id.setdefault(row["id"], []).append(row["e"])
    assert sorted(by_id[1]) == [10, 20]
    assert by_id[2] == [None]
    assert by_id[3] == [None]
    assert None in by_id[4] and 5 in by_id[4]


def test_two_generators_rejected(frame: object) -> None:
    with pytest.raises(AnalysisException, match=r"(?i)only one generator"):
        frame.select(F.explode(frame.a), F.explode_outer(frame.a)).collect()


def test_posexplode_stops_loud(frame: object) -> None:
    with pytest.raises(UnsupportedOperationException, match="posexplode") as raised:
        F.posexplode(frame.a)
    # r24 A3 octo C1-Q-001: message must not embed a rotting DataFusion major.
    assert "DataFusion 52" not in str(raised.value)
    assert "52.x" not in str(raised.value)


def test_posexplode_outer_stops_loud(frame: object) -> None:
    """posexplode_outer must STOP loud (not a silent stub) — octo C1-Q-006."""
    with pytest.raises(UnsupportedOperationException, match="posexplode_outer"):
        F.posexplode_outer(frame.a)


def test_explode_alone_select(frame: object) -> None:
    out = frame.select(F.explode(frame.a).alias("e")).to_arrow()
    values = sorted(v for v in out.column("e").to_pylist() if v is not None)
    assert values == [5, 10, 20]


def test_explode_str_column_name_not_literal(frame: object) -> None:
    """F.explode(\"a\") is ColumnOrName → col, never lit (octo C1-Q-001)."""
    out = frame.select(frame.id, F.explode("a").alias("e")).orderBy("id", "e")
    table = out.to_arrow()
    rows = [(r["id"], r["e"]) for r in table.to_pylist()]
    assert rows == [(1, 10), (1, 20), (4, None), (4, 5)]
    assert table.schema.field("e").type == pa.int64()


def test_explode_outer_str_column_name_not_literal(frame: object) -> None:
    """F.explode_outer(\"a\") binds the column, not a string literal (octo C1-Q-001)."""
    out = frame.select(frame.id, F.explode_outer("a").alias("e")).orderBy("id", "e")
    by_id: dict[int, list] = {}
    for row in out.to_arrow().to_pylist():
        by_id.setdefault(row["id"], []).append(row["e"])
    assert sorted(by_id[1]) == [10, 20]
    assert by_id[2] == [None]
    assert by_id[3] == [None]


def test_explode_cast_stays_generator(frame: object) -> None:
    """explode(...).cast(...) must still unnest (sticky _generator) — octo C1-Q-003."""
    out = frame.select(frame.id, F.explode(frame.a).cast("string").alias("e")).orderBy("id", "e")
    table = out.to_arrow()
    rows = [(r["id"], r["e"]) for r in table.to_pylist()]
    assert rows == [(1, "10"), (1, "20"), (4, None), (4, "5")]
    # Arrow path: value AND type (utf8 / large_string / string_view)
    assert _is_arrow_string(table.schema.field("e").type)


def test_with_column_explode_multiplies_rows(frame: object) -> None:
    """withColumn(generator) must unnest, not project the array (octo C1-Q-004 / C1-L-001)."""
    out = frame.withColumn("e", F.explode(frame.a)).orderBy("id", "e")
    table = out.to_arrow()
    rows = [(r["id"], r["e"]) for r in table.to_pylist()]
    assert rows == [(1, 10), (1, 20), (4, None), (4, 5)]
    assert table.schema.field("e").type == pa.int64()
    # Mutation-proof: row count multiplies (non-empty arrays), not 4 placeholder rows.
    assert table.num_rows == 4


def test_explode_prealiased_array_strips_as(frame: object) -> None:
    """F.explode(col.alias(...)) must not embed AS into unnest SQL (octo C1-Q-005)."""
    out = frame.select(frame.id, F.explode(frame.a.alias("renamed")).alias("e")).orderBy("id", "e")
    rows = [(r["id"], r["e"]) for r in out.to_arrow().to_pylist()]
    assert rows == [(1, 10), (1, 20), (4, None), (4, 5)]


def test_explode_outer_multi_array_exact_type_bind(spark: ReparkSession) -> None:
    """Sibling list cols must not steal CASE element type via substring match (C1-Q-002).

    Column ``a`` is a substring of ``data``; old ``name in display`` bound BIGINT for
    explode_outer(data) and broke string null/empty guards. Exact field bind only.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id,
               CAST(NULL AS BIGINT[]) AS a,
               CAST(NULL AS VARCHAR[]) AS data
        UNION ALL SELECT 2, make_array(1), make_array()
        UNION ALL SELECT 3, make_array(2), make_array('p', 'q')
        """
    )
    out = frame.select(frame.id, F.explode_outer(frame.data).alias("e")).orderBy("id", "e")
    table = out.to_arrow()
    by_id: dict[int, list] = {}
    for row in table.to_pylist():
        by_id.setdefault(row["id"], []).append(row["e"])
    assert by_id[1] == [None]
    assert by_id[2] == [None]
    assert sorted(v for v in by_id[3] if v is not None) == ["p", "q"]
    assert _is_arrow_string(table.schema.field("e").type)


def test_explode_outer_field_named_explode_exact_bind(spark: ReparkSession) -> None:
    """A list column literally named ``explode`` must not hijack type resolution (C1-L-002)."""
    frame = spark.sql(
        """
        SELECT 1 AS id,
               make_array('s') AS explode,
               CAST(NULL AS VARCHAR[]) AS a
        """
    )
    out = frame.select(frame.id, F.explode_outer(frame.a).alias("e")).to_arrow()
    rows = out.to_pylist()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["e"] is None
    assert _is_arrow_string(out.schema.field("e").type)


def test_explode_prealiased_sibling_no_double_as(frame: object) -> None:
    """select(id.alias(...), explode(...)) must not emit double AS (octo C2-Q-001)."""
    out = frame.select(frame.id.alias("x"), F.explode(frame.a).alias("e")).orderBy("x", "e")
    table = out.to_arrow()
    rows = [(r["x"], r["e"]) for r in table.to_pylist()]
    assert rows == [(1, 10), (1, 20), (4, None), (4, 5)]
    assert table.schema.field("x").type == pa.int64()
    assert table.schema.field("e").type == pa.int64()


def test_explode_outer_timestamp_element_type(spark: ReparkSession) -> None:
    """explode_outer NULL/empty guard must use TIMESTAMP not BIGINT fail-open (C2-Q-003/L-001)."""
    frame = spark.sql(
        """
        SELECT 1 AS id, CAST(NULL AS TIMESTAMP[]) AS a
        UNION ALL SELECT 2, make_array()
        UNION ALL SELECT 3, make_array(CAST('2020-01-01 00:00:00' AS TIMESTAMP))
        """
    )
    out = frame.select(frame.id, F.explode_outer(frame.a).alias("e")).orderBy("id")
    table = out.to_arrow()
    by_id: dict[int, list] = {}
    for row in table.to_pylist():
        by_id.setdefault(row["id"], []).append(row["e"])
    assert by_id[1] == [None]
    assert by_id[2] == [None]
    assert by_id[3][0] is not None
    assert pa.types.is_timestamp(table.schema.field("e").type)


def test_explode_reserved_and_mixed_case_array_ident(spark: ReparkSession) -> None:
    """Array/sibling idents must be quoted (reserved + mixed-case) — C2-Q-002 / C2-L-002."""
    frame = spark.sql(
        """
        SELECT 1 AS id,
               make_array(10, 20) AS "order",
               make_array(1) AS "MyArr"
        """
    )
    reserved = frame.select(frame.id, F.explode(frame["order"]).alias("e")).orderBy("id", "e")
    reserved_table = reserved.to_arrow()
    assert [(r["id"], r["e"]) for r in reserved_table.to_pylist()] == [(1, 10), (1, 20)]
    assert reserved_table.schema.field("e").type == pa.int64()

    mixed = frame.select(frame.id, F.explode(frame["MyArr"]).alias("e")).to_arrow()
    assert [(r["id"], r["e"]) for r in mixed.to_pylist()] == [(1, 1)]
    assert mixed.schema.field("e").type == pa.int64()


def test_explode_hostile_column_name_is_identifier_not_injection(frame: object) -> None:
    """Hostile ColumnOrName must not reshape FROM/SELECT (octo C2-SEC-001).

    Quoting turns the token into one identifier; analysis fails on missing field —
    not a successful alternate FROM clause.
    """
    hostile = "a) FROM (SELECT 1) t --"
    with pytest.raises((AnalysisException, Exception)) as raised:
        frame.select(F.explode(hostile).alias("e")).collect()
    message = str(raised.value).casefold()
    # Must not silently return rows from an injected subquery.
    assert "field" in message or "schema" in message or "error" in message or "failed" in message


def test_explode_sibling_hostile_alias_quoted(frame: object) -> None:
    """Sibling alias names with SQL metacharacters stay one identifier (C2-SEC-002)."""
    evil = 'x", 1 AS pwn --'
    out = frame.select(frame.id.alias(evil), F.explode(frame.a).alias("e")).to_arrow()
    # Projection name is the evil string (quoted in SQL); one extra column only.
    names = out.column_names
    assert evil in names
    assert "pwn" not in names
    assert "e" in names
    assert out.num_rows == 4  # explode keeps non-empty only: id1 x2 + id4 x2


def test_explode_asc_desc_keeps_generator(frame: object) -> None:
    """asc/desc must not drop _generator (octo C2-Q-005)."""
    gen_asc = F.explode(frame.a).asc()
    assert gen_asc._generator == "explode"
    gen_desc = F.explode_outer(frame.a).desc()
    assert gen_desc._generator == "explode_outer"
    # End-to-end: order markers on the generator still unnest when selected.
    out = frame.select(frame.id, F.explode(frame.a).alias("e").asc()).orderBy("id", "e")
    rows = [(r["id"], r["e"]) for r in out.to_arrow().to_pylist()]
    assert rows == [(1, 10), (1, 20), (4, None), (4, 5)]


def test_explode_outer_alone_select_null_empty_typed(frame: object) -> None:
    """Alone-select explode_outer keeps null/empty rows (value pin; C2-L-004 cheap)."""
    out = frame.select(F.explode_outer(frame.a).alias("e")).to_arrow()
    values = out.column("e").to_pylist()
    # Non-empty elements + one null from id=2 + one null from id=3 + null element from id=4.
    assert values.count(None) >= 3
    assert 10 in values and 20 in values and 5 in values
    assert out.schema.field("e").type == pa.int64()


def test_explode_compound_mixed_case_sibling(spark: ReparkSession) -> None:
    """Compound siblings with mixed-case idents must not re-embed unquoted SQL (C3-Q-001).

    Two-phase rewrite: native project ``MyId + 0`` first, then unnest by quoted names.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id, make_array(10, 20) AS a, 7 AS "MyId"
        """
    )
    out = frame.select((frame["MyId"] + 0).alias("x"), F.explode(frame.a).alias("e")).orderBy(
        "x", "e"
    )
    table = out.to_arrow()
    assert [(r["x"], r["e"]) for r in table.to_pylist()] == [(7, 10), (7, 20)]
    assert table.schema.field("x").type == pa.int64()
    assert table.schema.field("e").type == pa.int64()


def test_explode_outer_nested_list_element_type(spark: ReparkSession) -> None:
    """Nested list element type must be ``BIGINT[]`` not BIGINT fail-open (C3-Q-002 / C2 nested).

    Mutation-proof: removing the List( branch in ``_arrow_debug_type_to_sql`` raises or
    yields a scalar null type instead of list<item: int64>.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id, CAST(NULL AS BIGINT[][]) AS a
        UNION ALL SELECT 2, make_array()
        UNION ALL SELECT 3, make_array(make_array(1, 2))
        """
    )
    out = frame.select(frame.id, F.explode_outer(frame.a).alias("e")).orderBy("id")
    table = out.to_arrow()
    by_id: dict[int, list] = {}
    for row in table.to_pylist():
        by_id.setdefault(row["id"], []).append(row["e"])
    assert by_id[1] == [None]
    assert by_id[2] == [None]
    assert by_id[3] == [[1, 2]]
    # Value AND type on Arrow path — outer element is list<item: int64>, not int64.
    assert pa.types.is_list(table.schema.field("e").type)
    assert table.schema.field("e").type.value_type == pa.int64()


def test_explode_hostile_fn_call_column_name_not_sql(frame: object) -> None:
    """ColumnOrName shaped like a fn-call must not execute as free SQL (C3-SEC-001).

    Pre-fix injected ``make_array(1,2,3)`` rows; two-phase binds a field name only.
    """
    hostile = "make_array(1,2,3)"
    with pytest.raises(AnalysisException, match=r"(?i)field") as raised:
        frame.select(F.explode(hostile).alias("e")).collect()
    # Error names the hostile token as a missing field — not a successful unnest of 1,2,3.
    assert "make_array(1,2,3)" in str(raised.value)


def test_explode_hostile_subquery_column_name_not_sql(frame: object) -> None:
    """Leading-paren ColumnOrName must not run as a subquery (C3-SEC-001)."""
    hostile = "(SELECT make_array(9))"
    with pytest.raises(AnalysisException, match=r"(?i)field|schema|column"):
        frame.select(F.explode(hostile).alias("e")).collect()


def test_explode_array_of_struct_allowed(spark: ReparkSession) -> None:
    """Plain explode must not require outer element-type resolution (C3-L-001).

    Pre-fix raised explode_outer-shaped AnalysisException on array<struct>.
    """
    frame = spark.sql("SELECT 1 AS id, [{x: 10}, {x: 20}] AS a")
    out = frame.select(F.explode(frame.a).alias("e")).to_arrow()
    rows = out.to_pylist()
    assert rows == [{"e": {"x": 10}}, {"e": {"x": 20}}]
    assert pa.types.is_struct(out.schema.field("e").type)
    assert out.schema.field("e").type.field("x").type == pa.int64()


def test_explode_outer_coalesce_preserves_element_type(spark: ReparkSession) -> None:
    """explode_outer on compound array expr must not fail-open CASE to BIGINT (C3-L-002)."""
    frame = spark.sql(
        """
        SELECT 1 AS id, CAST(NULL AS VARCHAR[]) AS a
        UNION ALL SELECT 2, make_array()
        UNION ALL SELECT 3, make_array('p', 'q')
        """
    )
    out = frame.select(frame.id, F.explode_outer(F.coalesce(frame.a, frame.a)).alias("e")).orderBy(
        "id", "e"
    )
    table = out.to_arrow()
    by_id: dict[int, list] = {}
    for row in table.to_pylist():
        by_id.setdefault(row["id"], []).append(row["e"])
    assert by_id[1] == [None]
    assert by_id[2] == [None]
    assert sorted(v for v in by_id[3] if v is not None) == ["p", "q"]
    assert _is_arrow_string(table.schema.field("e").type)


def test_explode_size_sibling_uses_engine_cardinality(frame: object) -> None:
    """Scalar siblings (size) must use native plan, not Spark pretty name in SQL (C3-L-003).

    Pre-fix: ``size(a)`` re-embedded as free SQL → Invalid function 'size'.
    """
    out = frame.select(F.size(frame.a).alias("s"), F.explode(frame.a).alias("e"))
    table = out.to_arrow()
    rows = {(r["s"], r["e"]) for r in table.to_pylist()}
    # id=1 size 2 → (2,10),(2,20); id=4 size 2 → (2,None),(2,5); null/empty dropped.
    assert rows == {(2, 10), (2, 20), (2, None), (2, 5)}
    assert table.num_rows == 4
    # cardinality may surface as int64 or uint64 depending on DF version.
    assert pa.types.is_integer(table.schema.field("s").type)
    assert table.schema.field("e").type == pa.int64()


def test_explode_on_sql_functions_export_and_posexplode_stop() -> None:
    """``repark.sql.functions`` must re-export explode* (sed-swap) — octo C4-Q-001.

    Pre-fix: names lived only as module attributes; ``__all__`` omission made
    ``from repark.sql import functions as F; F.explode`` raise a false 'not implemented'
    AttributeError via sql.functions.__getattr__.
    """
    import repark.functions as canonical
    from repark.sql import functions as sql_functions

    for name in ("explode", "explode_outer", "posexplode", "posexplode_outer"):
        assert name in canonical.__all__, name
        assert name in sql_functions.__all__, name
        assert getattr(sql_functions, name) is getattr(canonical, name), name

    # posexplode STOP must fire on the sed-swap import path too.
    with pytest.raises(UnsupportedOperationException, match="posexplode"):
        sql_functions.posexplode("a")
    with pytest.raises(UnsupportedOperationException, match="posexplode_outer"):
        sql_functions.posexplode_outer("a")


def test_explode_nested_ops_refuse_loud(frame: object) -> None:
    """Nested ops on generators must not silently drop unnest (octo C4-L-001).

    Pre-fix: ``explode(a).isNotNull()`` cleared ``_generator``; select projected array
    null-checks without unnest (wrong row count/values). Spark: UNSUPPORTED_GENERATOR.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(frame.a).isNotNull()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode_outer(frame.a).isNull()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(frame.a) > 0
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = ~F.explode(frame.a)
    # End-to-end: select must refuse, never return non-unnested booleans.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.select(F.explode(frame.a).isNotNull().alias("b")).collect()


def test_explode_hostile_cast_rejected(frame: object) -> None:
    """Hostile ``.cast`` type text must not reshape generator unnest SQL (C4-SEC-001 / C4-L-002).

    Pre-fix: fail-open ``_spark_cast_type_name`` + ``CAST(unnest AS {type})`` accepted
    suffixes like ``decimal(10,4)),1`` and comment-out fragments.

    The refusal class is ``ParseException`` (an ``AnalysisException``) to match live
    PySpark on unparsable cast text — r24 morning rider. The allowlist, not the
    exception class, is the injection control.
    """
    with pytest.raises(ParseException, match=r"unknown cast type"):
        _ = F.explode(frame.a).cast("decimal(10,4)),1")
    with pytest.raises(ParseException, match=r"unknown cast type"):
        _ = F.explode(frame.a).cast("int)--")
    with pytest.raises(ParseException, match=r"unknown cast type"):
        _ = F.explode(frame.a).cast("string),unnest(make_array(1))")
    with pytest.raises(ParseException, match=r"unknown cast type"):
        _ = F.explode(frame.a).cast("notatype")
    # Allowlisted cast still unnests (regression pin for sticky generator + cast).
    out = frame.select(frame.id, F.explode(frame.a).cast("string").alias("e")).orderBy("id", "e")
    table = out.to_arrow()
    assert [(r["id"], r["e"]) for r in table.to_pylist()] == [
        (1, "10"),
        (1, "20"),
        (4, None),
        (4, "5"),
    ]
    assert _is_arrow_string(table.schema.field("e").type)


def test_explode_function_wrappers_refuse_generator(frame: object) -> None:
    """F.size/coalesce/when must not strip ``_generator`` (octo C5-Q-001 / C5-L-001).

    Pre-fix: ``select(F.size(F.explode(a)))`` returned per-row array cardinality without
    unnest (Spark ``UNSUPPORTED_GENERATOR``). Same hole for coalesce/concat/when + .str.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.size(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.coalesce(F.explode(frame.a), frame.a)
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.concat(F.explode(frame.a).cast("string"), F.lit("x"))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.when(F.explode(frame.a).isNotNull(), 1)  # isNotNull already refuses
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        # when(condition, value) with generator as value
        _ = F.when(frame.id > 0, F.explode(frame.a))
    # End-to-end select must refuse, never return non-unnested cardinalities.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.select(F.size(F.explode(frame.a)).alias("s")).collect()
    # Polars-style .str path lowers via _scalar — same refuse (C5-Q-001 pin).
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(frame.a).cast("string").str.to_uppercase()


def test_nested_explode_refuses_kind_overwrite(frame: object) -> None:
    """``explode(explode_outer(...))`` must refuse, not rewrite kind (octo C5-L-002).

    Pre-fix: nested call set kind=explode and dropped null/empty silently.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(F.explode_outer(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode_outer(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.select(F.explode(F.explode_outer(frame.a)).alias("e")).collect()


def test_explode_chained_cast_composes(frame: object) -> None:
    """``.cast().cast()`` must apply *every* cast, not only the last (octo C5-L-003).

    Pre-fix: second cast overwrote ``_generator_cast`` so only final CAST(unnest AS T).
    float→int truncates then stringifies the truncated value.
    """
    # 10, 20, null, 5 as doubles → cast int → cast string
    out = frame.select(
        frame.id,
        F.explode(frame.a).cast("double").cast("int").cast("string").alias("e"),
    ).orderBy("id", "e")
    table = out.to_arrow()
    rows = [(r["id"], r["e"]) for r in table.to_pylist()]
    assert rows == [(1, "10"), (1, "20"), (4, None), (4, "5")]
    assert _is_arrow_string(table.schema.field("e").type)
    # Mutation-proof: cast chain is stored as ordered tokens (not last-only).
    gen = F.explode(frame.a).cast("double").cast("int").cast("string")
    assert gen._generator == "explode"
    assert gen._generator_cast == ("DOUBLE", "INT", "STRING")


def test_explode_generator_select_duplicate_name_preflight(frame: object) -> None:
    """Generator select must share duplicate-name preflight (octo C5 residual S2)."""
    with pytest.raises(AnalysisException, match=r"(?i)duplicate"):
        frame.select(F.explode(frame.a).alias("e"), frame.id.alias("e")).collect()


def test_explode_aggregate_wrappers_refuse_generator(frame: object) -> None:
    """F.count/sum/avg/… must not strip ``_generator`` (octo C6-Q-001).

    Pre-fix: ``F.count(F.explode(a))`` built a count over the array placeholder (or
    failed engine-side) instead of refusing like Spark ``UNSUPPORTED_GENERATOR``. Same
    hole for sum/avg/min/max/collect_list via ``_aggregate_argument``.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.count(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.sum(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.avg(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.min(F.explode_outer(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.max(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.collect_list(F.explode(frame.a))
    # End-to-end select/agg must refuse, never count non-null arrays as elements.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.select(F.count(F.explode(frame.a)).alias("c")).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.agg(F.sum(F.explode(frame.a))).collect()


def test_explode_filter_orderby_groupby_refuse_generator(frame: object) -> None:
    """filter/orderBy/groupBy/agg must refuse generators (octo C6-Q-002).

    Pre-fix: orderBy(F.explode(a).asc()) sorted by array; groupBy(F.explode(a))
    grouped by array; filter accepted the generator Column into native filter.
    Select-path unnest is the only supported generator surface.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.filter(F.explode(frame.a)).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.where(F.explode_outer(frame.a)).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.orderBy(F.explode(frame.a).asc()).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.sort(F.explode_outer(frame.a).desc()).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.groupBy(F.explode(frame.a)).count().collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.groupBy(frame.id).agg(F.explode(frame.a)).collect()


def test_explode_nested_array_top_level_length_not_cardinality(spark: ReparkSession) -> None:
    """Empty guards use array_length, not multi-dim cardinality (octo C6-L-001).

    Pre-fix: DataFusion ``cardinality`` is a nested product (empty→NULL); ``[[]]`` and
    ``[[],[1,2]]`` had product 0 and were treated as empty — silent drop (explode) or
    null rewrite (outer). Top-level length keeps them and unnests outer elements.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id, make_array(make_array()) AS a
        UNION ALL SELECT 2, make_array(make_array(1, 2))
        UNION ALL SELECT 3, make_array(make_array(), make_array(3))
        UNION ALL SELECT 4, make_array()
        UNION ALL SELECT 5, CAST(NULL AS BIGINT[][])
        """
    )
    exploded = frame.select(frame.id, F.explode(frame.a).alias("e")).orderBy("id")
    table = exploded.to_arrow()
    by_id: dict[int, list] = {}
    for row in table.to_pylist():
        by_id.setdefault(row["id"], []).append(row["e"])
    # id=1 [[]] → one row with empty list element (not dropped as empty).
    assert by_id[1] == [[]]
    assert by_id[2] == [[1, 2]]
    # id=3 [[], [3]] → two outer elements (cardinality product was 0 pre-fix).
    assert len(by_id[3]) == 2
    assert [] in by_id[3] and [3] in by_id[3]
    # Flat empty / null still dropped.
    assert 4 not in by_id
    assert 5 not in by_id
    assert pa.types.is_list(table.schema.field("e").type)
    assert table.schema.field("e").type.value_type == pa.int64()

    outer = frame.select(frame.id, F.explode_outer(frame.a).alias("e")).orderBy("id")
    outer_table = outer.to_arrow()
    outer_by_id: dict[int, list] = {}
    for row in outer_table.to_pylist():
        outer_by_id.setdefault(row["id"], []).append(row["e"])
    # Nested non-empty outer length: keep elements ([[]] → [] element, not scalar NULL).
    assert outer_by_id[1] == [[]]
    assert outer_by_id[2] == [[1, 2]]
    assert len(outer_by_id[3]) == 2
    assert [] in outer_by_id[3] and [3] in outer_by_id[3]
    # True empty / null → one null *element* row (scalar NULL of list type).
    assert outer_by_id[4] == [None]
    assert outer_by_id[5] == [None]
    assert pa.types.is_list(outer_table.schema.field("e").type)


def test_explode_date_wrappers_refuse_generator(frame: object) -> None:
    """F.year/date_* and Column.dt must not strip ``_generator`` (octo C7-Q-001).

    Pre-fix: ``_date_fn`` / add_months / date_add / date_format / trunc / date_trunc
    built plain Columns over the array placeholder (no UNSUPPORTED_GENERATOR). The
    Polars ``.dt`` namespace lowers through the same helpers.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.year(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.month(F.explode_outer(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.dayofmonth(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.add_months(F.explode(frame.a), 1)
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.date_add(F.explode(frame.a), 1)
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.date_sub(F.explode(frame.a), 1)
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.date_format(F.explode(frame.a), "yyyy")
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.trunc(F.explode(frame.a), "year")
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.date_trunc("year", F.explode(frame.a))
    # End-to-end select must refuse, never project year-of-array.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.select(F.year(F.explode(frame.a)).alias("y")).collect()
    # Polars-style .dt path lowers via _date_fn / date_trunc — same refuse.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(frame.a).dt.year()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode_outer(frame.a).dt.month()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = F.explode(frame.a).dt.truncate("year")


def test_explode_window_partition_order_refuse_generator(frame: object) -> None:
    """Window.partitionBy/orderBy must refuse generators (octo C7-Q-002).

    Pre-fix: ``Window.partitionBy(F.explode(a))`` / ``orderBy`` accepted the generator
    and windowed over the array placeholder instead of Spark UNSUPPORTED_GENERATOR.
    """
    from repark import Window

    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = Window.partitionBy(F.explode(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = Window.orderBy(F.explode_outer(frame.a))
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = Window.partitionBy(frame.id).orderBy(F.explode(frame.a).asc())
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        _ = Window.orderBy(frame.id).partitionBy(F.explode(frame.a))
    # End-to-end over() must refuse at WindowSpec construction, not window arrays.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.select(
            F.row_number().over(Window.partitionBy(F.explode(frame.a)).orderBy(frame.id))
        ).collect()


def test_explode_cube_rollup_grouping_sets_refuse_generator(frame: object) -> None:
    """cube/rollup/groupingSets + SQL agg path refuse generators (octo C7-Q-003 / C7-L-001/002).

    Pre-fix: ``_grouping_sets_grouped`` skipped the groupBy generator refuse, so
    cube/rollup/groupingSets grouped by array placeholders; ``_agg_via_sql_group``
    early-returned before GroupedData.agg's refuse and embedded bare explode SQL.
    """
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.cube(F.explode(frame.a)).count().collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.rollup(F.explode_outer(frame.a)).count().collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.groupingSets(F.explode(frame.a)).count().collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.grouping_sets(F.explode(frame.a)).count().collect()
    # Mixed group key + generator key must still refuse.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.cube(frame.id, F.explode(frame.a)).count().collect()
    # SQL GroupedData.agg path: bare explode under cube must refuse (not project arrays).
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.cube(frame.id).agg(F.explode(frame.a)).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.rollup(frame.id).agg(F.explode_outer(frame.a).alias("e")).collect()
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.groupingSets(frame.id).agg(F.explode(frame.a)).collect()


def test_explode_plus_sticky_aggregate_missing_group_by(frame: object) -> None:
    """select(explode, sum/count) must raise ``[MISSING_GROUP_BY]`` (combine octo C1-Q-002).

    Pre-fix: generator short-circuit ran before F1 sticky-aggregate classification, so
    mixed explode+sum mid-projected aggregates as unnest siblings instead of refusing.
    Mutation-proof: bare sticky flags on sum/count still present when refuse fires.
    """
    assert F.sum(frame.id)._is_aggregate is True
    assert F.count("*")._is_aggregate is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_sum:
        frame.select(F.explode(frame.a).alias("e"), F.sum(frame.id)).collect()
    assert "GROUP BY" in str(caught_sum.value)
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_count:
        frame.select(F.explode_outer(frame.a).alias("e"), F.count("*")).collect()
    assert "GROUP BY" in str(caught_count.value)
    # Composed sticky aggregate + generator still refuses (not pure-global mis-route).
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(F.explode(frame.a).alias("e"), (F.sum(frame.id) + 1).alias("s")).collect()
    # Generator-only and generator+free attr still unnest (regression guard).
    alone = frame.select(F.explode(frame.a).alias("e")).to_arrow()
    assert alone.num_rows == 4
    with_id = frame.select(frame.id, F.explode(frame.a).alias("e")).orderBy("id", "e")
    assert [(r["id"], r["e"]) for r in with_id.to_arrow().to_pylist()] == [
        (1, 10),
        (1, 20),
        (4, None),
        (4, 5),
    ]


def test_explode_nested_aggregate_argument_missing_group_by(frame: object) -> None:
    """explode(collect_list/array_repeat(sum)) refuses sticky AF args (combine C4-Q-001).

    Sibling ``select(explode, sum)`` is already pinned (C1); nested explode(agg) previously
    stripped ``_is_aggregate`` at the builder and entered unnest mid-project. Mutation that
    drops the sticky-agg refuse (or select generator+agg early gate) fails here.
    """
    collect = F.collect_list(frame.id)
    assert collect._is_aggregate is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_list:
        F.explode(collect)
    assert "GROUP BY" in str(caught_list.value)
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        F.explode_outer(F.collect_list("id"))
    # array_repeat(sum) keeps sticky aggregate via _scalar OR-propagation.
    repeated = F.array_repeat(F.sum(frame.id), 1)
    assert repeated._is_aggregate is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        F.explode(repeated)
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        F.explode_outer(F.array_repeat(F.sum("id"), 1))
    # select boundary: nested form must not reach unnest even if builder were bypassed
    # (select-level generator+aggregate gate — synthetic sticky Column).
    from repark.column import Column

    synth = Column(
        collect._inner,
        spark_display="explode(collect_list(id))",
        sql_expr=collect.sql_expr_part(),
        projection_name="col",
        generator="explode",
        is_aggregate=True,
    )
    assert synth._is_aggregate is True
    assert synth._generator == "explode"
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(synth).collect()
    # Non-aggregate explode still works (regression guard vs over-broad refuse).
    alone = frame.select(F.explode(frame.a).alias("e")).to_arrow()
    assert alone.num_rows == 4


def test_generator_alias_cast_keeps_sticky_aggregate(frame: object) -> None:
    """Generator ``.alias`` / ``.cast`` keep sticky aggregate bits (combine C5-Q-002).

    Pre-fix: generator branches of alias/cast omitted ``_is_aggregate`` (and free /
    ungroupable / AF), so ``select(synth.alias('e'))`` / ``.cast`` bypassed the C4
    generator+agg ``[MISSING_GROUP_BY]`` gate that bare ``select(synth)`` raised.
    Mutation that drops sticky copy fails these pins; ordinary explode still unnests.
    """
    from repark.column import Column

    collect = F.collect_list(frame.id)
    synth = Column(
        collect._inner,
        spark_display="explode(collect_list(id))",
        sql_expr=collect.sql_expr_part(),
        projection_name="col",
        generator="explode",
        is_aggregate=True,
        is_aggregate_function=True,
        has_free_attribute=True,
        has_ungroupable=True,
    )
    assert synth._is_aggregate is True
    aliased = synth.alias("e")
    assert aliased._generator == "explode"
    assert aliased._is_aggregate is True
    assert aliased._is_aggregate_function is True
    assert aliased._has_free_attribute is True
    assert aliased._has_ungroupable is True
    assert aliased._projection_name == "e"
    casted = synth.cast("int")
    assert casted._generator == "explode"
    assert casted._is_aggregate is True
    assert casted._has_free_attribute is True
    assert casted._has_ungroupable is True
    # Cast clears AF purity (SQL path) — same as non-generator cast.
    assert casted._is_aggregate_function is False
    # Select gate: aliased / cast sticky generator+agg still refuse (not unnest).
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(aliased).collect()
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(casted).collect()
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(synth.alias("e").cast("string")).collect()
    # Non-aggregate explode.alias / cast still unnest (regression guard).
    alone = frame.select(F.explode(frame.a).alias("e").cast("int")).to_arrow()
    assert alone.num_rows == 4
