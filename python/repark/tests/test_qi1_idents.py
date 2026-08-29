"""QI1 — quoting / path-escape single-source pins (CQ-006/007).

Pins:
* always-quote vs quote-if-needed call-site classes (behavior-preserving migration)
* injection-probe battery (Spark dialect) at every SSOT entry point
* path-escape needles lockstep with Rust ``repark_write::idents::probes``
* re-exports from session / dataframe / catalog / column remain the same objects

No live Spark oracle (Q6): current behavior is pinned, not divergence-fixed.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkValueError
from repark.spark._idents import (
    INJECTION_PROBES,
    PATH_ESCAPE_PROBES,
    PATH_ESCAPE_SAFE,
    assert_spark_injection_probe_is_single_token,
    is_plain_ident,
    path_escape_kind,
    quote_column_sql_expr,
    quote_ident,
    quote_ident_if_needed,
    quote_multipart,
    reject_path_escape_segment,
)

# Behavior pins — call-site classes


def test_always_quote_class_doubles_embedded_quotes() -> None:
    """Session / dataframe / column / ML class: always double-quote."""
    assert quote_ident("plain") == '"plain"'
    assert quote_ident('na"me') == '"na""me"'
    assert quote_ident("") == '""'
    assert quote_ident("order") == '"order"'


def test_quote_if_needed_class_leaves_plain_bare() -> None:
    """Catalog / assign-target class: plain bare stays unquoted."""
    assert quote_ident_if_needed("plain") == "plain"
    assert quote_ident_if_needed("order") == "order"  # bare reserved word stays bare
    assert quote_ident_if_needed("a b") == '"a b"'
    assert quote_ident_if_needed('na"me') == '"na""me"'
    assert quote_ident_if_needed("a.b") == '"a.b"'


def test_quote_column_sql_expr_quotes_per_segment() -> None:
    assert quote_column_sql_expr("x") == '"x"'
    assert quote_column_sql_expr("source.name") == '"source"."name"'
    assert quote_column_sql_expr('a"b.c"d') == '"a""b"."c""d"'


def test_quote_multipart_catalog_vs_always() -> None:
    assert quote_multipart(["cat", "db", "t"], always=False) == "cat.db.t"
    assert quote_multipart(["cat", "my-db", "t"], always=False) == 'cat."my-db".t'
    assert quote_multipart(["cat", "db", "t"], always=True) == '"cat"."db"."t"'


# Injection-probe battery (Spark dialect) — every SSOT surface


@pytest.mark.parametrize("probe", INJECTION_PROBES)
def test_injection_probe_is_single_token_via_ssot(probe: str) -> None:
    quoted = assert_spark_injection_probe_is_single_token(probe)
    # Independent oracle — undouble-only false-passes under-escape (octo C1-Q-002).
    expected = '"' + probe.replace('"', '""') + '"'
    assert quoted == expected
    assert '"' not in quoted[1:-1].replace('""', "")


def test_injection_under_escape_oracle_would_reject() -> None:
    """Mutation-proof: forget-doubling is not a single token (C1-SEC-001)."""
    probe = 'id"; evil'
    under = '"' + probe + '"'
    correct = '"' + probe.replace('"', '""') + '"'
    assert under != correct
    assert '"' in under[1:-1].replace('""', "")


def test_probe_tables_lockstep_frozen_with_rust_ssot() -> None:
    """Cross-lang lockstep (C1-Q-003): frozen literals match repark_write::idents::probes."""
    assert INJECTION_PROBES == (
        r'"; DROP TABLE x; --',
        r'id"; DROP TABLE x; --',
        'na"me',
        "order",
        "a b",
        "a.b",
        "",
    )
    assert PATH_ESCAPE_PROBES == (
        (".", "traversal"),
        ("..", "traversal"),
        ("foo..bar", "traversal"),
        ("a/b", "separator"),
        (r"a\b", "separator"),
        ("../etc", "traversal"),
    )
    assert PATH_ESCAPE_SAFE == ("ok_table", "my_table", "t0", "Order")


def test_injection_probe_battery_at_reexport_sites() -> None:
    """Migrated re-exports must be the same function object (nothing dual-sourced)."""
    from repark import catalog as catalog_mod
    from repark import column as column_mod
    from repark import dataframe as dataframe_mod
    from repark import session as session_mod

    # Always-quote class
    assert session_mod._quote_ident is quote_ident
    assert dataframe_mod._quote_ident_sql is quote_ident
    assert column_mod._quote_sql_field_ident is quote_ident

    # Quote-if-needed class
    assert catalog_mod._quote_ident is quote_ident_if_needed

    for probe in INJECTION_PROBES:
        assert session_mod._quote_ident(probe) == quote_ident(probe)
        assert dataframe_mod._quote_ident_sql(probe) == quote_ident(probe)


def test_functions_column_sql_expr_uses_ssot() -> None:
    from repark import functions as functions_mod

    assert functions_mod._quote_column_sql_expr is quote_column_sql_expr
    assert functions_mod._quote_column_sql_expr("s.x") == '"s"."x"'


def test_merge_assign_target_is_quote_if_needed() -> None:
    from repark import merge as merge_mod

    assert merge_mod._quote_assign_target is quote_ident_if_needed
    assert merge_mod._quote_assign_target("id") == "id"
    assert merge_mod._quote_assign_target("a b") == '"a b"'


# Path-escape — lockstep with Rust probes table


@pytest.mark.parametrize(("segment", "expected"), PATH_ESCAPE_PROBES)
def test_path_escape_probes_reject(segment: str, expected: str) -> None:
    assert path_escape_kind(segment) == expected
    with pytest.raises(PySparkValueError) as info:
        reject_path_escape_segment(segment)
    text = str(info.value)
    if expected == "traversal":
        assert "path traversal" in text or ".." in text
    else:
        assert "path separators" in text or "/" in text or "\\" in text


@pytest.mark.parametrize("segment", PATH_ESCAPE_SAFE)
def test_path_escape_safe_segments_pass(segment: str) -> None:
    assert path_escape_kind(segment) is None
    reject_path_escape_segment(segment)


def test_session_path_escape_reexport() -> None:
    from repark import session as session_mod

    assert session_mod._reject_path_escape_segment is reject_path_escape_segment
    with pytest.raises(PySparkValueError):
        session_mod._reject_path_escape_segment("..")


def test_is_plain_ident() -> None:
    assert is_plain_ident("abc")
    assert is_plain_ident("_x1")
    assert not is_plain_ident("a b")
    assert not is_plain_ident("1a")
    assert not is_plain_ident("")


def test_polars_join_quote_uses_ssot_for_bare() -> None:
    """Q5: polars nested quote_ident builds SQL identifiers — bare-only + always-quote SSOT."""
    from repark.spark._idents import is_plain_ident
    from repark.spark._idents import quote_ident as ssot

    # Mirror the nested helper contract without spinning a full join.
    assert is_plain_ident("id")
    assert ssot("id") == '"id"'
    assert not is_plain_ident("a b")
