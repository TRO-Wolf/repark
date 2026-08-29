"""SQP-1 record pin (charter C-011) — the disposition is documented and single-homed.

A tree pin over repository files: fixed entries left ``STATUS.md``, the three out-of-scope
divergences are ``§7`` registry rows each with a pin, the GT1 residual comments are updated,
and the new module carries the rule table with its oracle provenance. Files only — no engine,
no JVM.

pins: sqp-1-spark-string-literals/C-011
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]


def _read(relative: str) -> str:
    """Return the UTF-8 text of a repo-relative file."""
    return (_REPO / relative).read_text(encoding="utf-8")


def test_status_no_longer_carries_the_fixed_issues() -> None:
    """The two entries this unit fixed are removed from STATUS (§6: a fixed defect leaves STATUS,
    it is not moved to the registry)."""
    status = _read("STATUS.md")
    assert "do not process backslash escapes" not in status
    assert "CAST(x AS BINARY) unimplemented" not in status


def test_registry_has_the_three_backlog_rows_each_pinned() -> None:
    """The three measured, not-closed divergences are §7 rows, each naming its pin (§6)."""
    registry = _read("docs/spark-sql-iceberg-parity.md")
    assert "### BL-9 — a double-quoted string literal is an identifier" in registry
    assert "### BL-10 — `spark.sql.parser.escapedStringLiterals=true` has no carrier" in registry
    assert "### BL-11 — numeric → `BINARY` under `spark.sql.ansi.enabled=false`" in registry
    for pin in (
        "test_sqp_1_string_literals.py::test_double_quoted_literal_is_an_identifier",
        "test_sqp_1_string_literals.py::test_escaped_string_literals_flag_has_no_carrier",
        "test_sqp_1_string_literals.py::test_numeric_to_binary_refuses",
    ):
        assert pin in registry, f"registry row must name its pin: {pin}"
    # The oracle is scrubbed in the committed doc, never a private path.
    assert "<pyspark-4.1.2-oracle>" in registry


def test_gt1_residual_comments_are_updated() -> None:
    """The two GT1 disclosure comments that described the residual are rewritten — the residual
    closed with this unit."""
    gt1 = _read("python/repark/tests/test_functions_gt1.py")
    assert "coincidence of double-escaping" not in gt1
    assert "SQL literals do not process" not in gt1
    assert "SQP-1 landed" in gt1
    assert "its own escape pins are in" in gt1


def test_module_doc_records_the_rule_table() -> None:
    """The new module doc is where a future reader learns the escape rules and their provenance."""
    module = _read("crates/repark-spark/src/spark_literals.rs")
    assert "The rules (Spark 4.1.2" in module
    assert "<pyspark-4.1.2-oracle>" in module
    assert "backslash KEPT" in module
    assert "one astral char" in module
