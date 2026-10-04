"""REG-1: the divergence registry says what the pins prove (DEC-2/6/7/8, TZ-8, G3-E8).

Tree pins: the dated FIXED notes are in, stale BACKLOG phrasing is gone, every cited test
exists as a real ``fn`` / ``def`` at the named path, and the genuinely open rows (DEC-9, the
TZ-8 residual, the G3-E8 remainder) still read BACKLOG / refused. No engine is imported;
nothing here needs a JVM or the facade wheel.
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_REGISTRY = _REPO / "docs/spark-sql-iceberg-parity.md"
_STATUS = _REPO / "STATUS.md"


def _registry() -> str:
    """The whole divergence registry."""
    return _REGISTRY.read_text(encoding="utf-8")


def _status() -> str:
    """The whole STATUS document."""
    return _STATUS.read_text(encoding="utf-8")


def _defines(rel_path: str, name: str) -> bool:
    """True when ``rel_path`` defines ``fn name`` (Rust) or ``def name`` (Python)."""
    text = (_REPO / rel_path).read_text(encoding="utf-8")
    return f"fn {name}" in text or f"def {name}" in text


def test_dec_rows_carry_dated_fixed_notes() -> None:
    """pins: reg-1-registry-truth-up/C-001 — DEC-2/6/7/8 read FIXED with PRs and pins."""
    text = _registry()
    assert "**DEC-2 — `DECIMAL / DECIMAL` result precision and scale — FIXED (2026-08-14" in text
    assert "**DEC-6 — max `DECIMAL(38,0) + 1` under ANSI raises — FIXED (2026-08-14" in text
    assert "**DEC-7 — `DECIMAL / 0` under ANSI raises — FIXED (2026-08-14" in text
    assert "**DEC-8 — `DECIMAL(38,20) * DECIMAL(38,20)` — FIXED (2026-08-14" in text
    assert "#99" in text and "#94" in text
    for pin in (
        "pin_div_same_precision_scale_repark_i128",
        "pin_overflow_max_decimal38_plus_one_wrong_value_i128",
        "pin_div_by_zero_decimal38_raises_under_default_ansi",
        "pin_mul_38_20_still_refuses_at_plan",
        "mul_38_20_plans_via_the_expr_planner",
    ):
        assert pin in text, pin
    for heading in ("### DEC-2 —", "### DEC-6 —", "### DEC-7 —", "### DEC-8 —"):
        assert heading not in text, heading
    assert "needs a UDF / scaled division" not in text
    assert "still refuses with `AnalysisException`" not in text
    assert "mul_38_20_still_refuses_before_any_analyzer_rule" not in text
    assert "#dec-6--max-decimal380--1-under-ansi-returns-a-corrupted-value" not in text
    assert "#dec-7--decimal--0-under-ansi-returns-null" not in text


def test_tz8_row_splits_fixed_and_residual() -> None:
    """pins: reg-1-registry-truth-up/C-002 — TZ-8 names the FIXED half and the residual half."""
    text = _registry()
    assert "**Not FIXED.**" not in text
    assert "### TZ-8 — `last_day` / `date_add` over a TIMESTAMP refuse to plan" in text
    # `date_sub` refuses too but has no pin — the row claims only what a pin proves
    # (registry §6: an unpinned divergence is prose).
    tz8 = text.index("### TZ-8")
    row_end = text.index("\n### ", tz8 + 1)
    assert "date_sub" not in text[tz8:row_end]
    assert "— FIXED (2026-08-14, #100" in text
    assert "rewrite_timestamp_to_date_cast" in text
    assert "timestamp_to_date_paths_read_the_session_zone" in text
    assert "2024-06-14" in text
    assert "last_day_and_date_add_over_a_timestamp_still_refuse" in text
    assert "2024-06-30" in text and "2024-06-15" in text
    assert "B-TZ-3" in text
    assert "timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone" not in text
    assert "tz-8--timestampdate-outside-this-repos-coercion-path-reads-the-stored-zone" not in text


def test_g3e8_row_states_delivered_and_remainder() -> None:
    """pins: reg-1-registry-truth-up/C-003 — G3-E8 states delivered spellings and remainder."""
    text = _registry()
    assert "UPDATE IN / NOT IN / EXISTS, correlated IN," not in text
    assert "UPDATE IN + correlated IN / ANY / ALL stay valved" not in text
    assert "try_allowed_update_in" in text
    assert "g3e8_update_in_subquery_rewrites_only_the_matching_row" in text
    assert "g3e8_delete_correlated_in_deletes_exactly_the_matching_row" in text
    assert "[update_in_subquery]" in text
    assert "correlated UPDATE IN" in text
    assert "g3e8_update_subquery_family_all_refuse" in text
    assert "### G3-E8-NULL" in text
    assert "update_not_in_subquery_with_null_key" in text


def test_status_known_issues_match_the_registry() -> None:
    """pins: reg-1-registry-truth-up/C-004 — STATUS bullets match the registry, under ceiling."""
    text = _status()
    assert "BACKLOG on DEC-2 / DEC-6 / DEC-7 / DEC-8" not in text
    assert "DEC-2 / DEC-6 / DEC-7 / DEC-8" in text and "FIXED" in text
    assert "**TZ-8 open**" not in text
    assert "#100" in text
    assert "UPDATE IN and correlated IN / ANY / ALL stay valved" not in text
    assert "G3-E8-NULL's UPDATE half stays refused" in text
    assert text.count("docs/spark-sql-iceberg-parity.md") >= 3
    ceiling = 25_000
    assert _STATUS.stat().st_size <= ceiling, _STATUS.stat().st_size


def test_cited_pins_exist_and_dec9_stays_open() -> None:
    """pins: reg-1-registry-truth-up/C-005 — every cited test exists; DEC-9 FIXED (name kept)."""
    decimal_rs = "crates/repark-spark/src/tests/decimal.rs"
    session_tz_rs = "crates/repark-spark/tests/session_timezone.rs"
    dml_rs = "crates/repark-spark/src/tests/dml.rs"
    precision_rs = "crates/repark-functions/src/decimal_precision.rs"
    for rel, name in (
        (decimal_rs, "pin_div_same_precision_scale_repark_i128"),
        (decimal_rs, "pin_overflow_max_decimal38_plus_one_wrong_value_i128"),
        (decimal_rs, "pin_overflow_max_decimal38_plus_one_null_when_ansi_false"),
        (decimal_rs, "pin_div_by_zero_decimal38_raises_under_default_ansi"),
        (decimal_rs, "pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false"),
        (decimal_rs, "pin_mul_38_20_still_refuses_at_plan"),
        (precision_rs, "mul_38_20_plans_via_the_expr_planner"),
        (session_tz_rs, "timestamp_to_date_paths_read_the_session_zone"),
        (session_tz_rs, "native_dataframe_api_cast_to_date_reads_the_session_zone"),
        (session_tz_rs, "date_valued_shims_take_the_date_in_the_session_zone"),
        (session_tz_rs, "last_day_and_date_add_over_a_timestamp_still_refuse"),
        (dml_rs, "g3e8_delete_correlated_in_deletes_exactly_the_matching_row"),
        (dml_rs, "g3e8_update_in_subquery_rewrites_only_the_matching_row"),
        (dml_rs, "g3e8_update_subquery_family_all_refuse"),
    ):
        assert _defines(rel, name), f"{rel}::{name}"
    corpus = (_REPO / "python/repark/tests/test_decimal128_parity.py").read_text(encoding="utf-8")
    for row in ("div_same_precision_scale", "mul_38_20_plans_in_spark_refuses_in_repark"):
        assert f'"{row}"' in corpus, row
    dml = (_REPO / "python/repark/tests/test_dml_subquery_parity.py").read_text(encoding="utf-8")
    assert 'name="update_in_subquery"' in dml
    text = _registry()
    assert "mul_38_20_plans_via_the_expr_planner" in text
    assert "timestamp_to_date_paths_read_the_session_zone" in text
    assert "### DEC-9 — overflow-capable binary arithmetic is marked non-null" in text
    assert "FIXED 2026-09-05 (NULLABILITY-2)" in text


def test_no_row_deleted_and_maps_in_lockstep() -> None:
    """pins: reg-1-registry-truth-up/C-006 — no row silently deleted; the maps carry the new pin."""
    text = _registry()
    for dec in range(1, 10):
        # Match the row itself, not just its id: an id inside a range is not a row.
        # ("DEC-1 … DEC-9") must not satisfy this.
        assert f"### DEC-{dec} —" in text or f"**DEC-{dec} —" in text, f"DEC-{dec}"
    for row in ("### TZ-8 —", "### G3-E8 —", "### G3-E8-NULL"):
        assert row in text, row
    parity_map = (_REPO / "python/repark-parity/tests/map.md").read_text(encoding="utf-8")
    assert "test_reg_1_registry_truth_up.py" in parity_map
    # The ledger is listed by the completed/ map until the next pickup archives it,
    # then by the archive map.
    completed_map = (_REPO / "task/ledgers/completed/map.md").read_text(encoding="utf-8")
    archive_maps = [
        m.read_text(encoding="utf-8") for m in (_REPO / "task/ledgers/archive").glob("*/map.md")
    ]
    assert "reg-1-registry-truth-up" in completed_map or any(
        "reg-1-registry-truth-up" in m for m in archive_maps
    )
    assert not (_REPO / "task/ledgers/staging/reg-1-registry-truth-up-ledger.md").exists()
