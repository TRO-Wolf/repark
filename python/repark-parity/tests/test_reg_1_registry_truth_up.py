"""REG-1: the divergence registry says what the pins prove (DEC-2/6/7/8, TZ-8, G3-E8).

Tree pins, in the style of ``test_dl_5_contract_compaction.py``: they read the registry and
STATUS text and assert the dated FIXED notes are in, the stale BACKLOG phrasing is gone, every
test the new notes cite exists as a real ``fn`` / ``def`` at the named path, and the rows that are
genuinely open (DEC-9, the TZ-8 residual, the G3-E8 remainder) still read BACKLOG / refused. No
engine is imported; nothing here needs a JVM or the facade wheel.
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
    # Each row is now a dated FIXED note in the DEC-3 / DEC-4 house form.
    assert "**DEC-2 — `DECIMAL / DECIMAL` result precision and scale — FIXED (2026-08-14" in text
    assert "**DEC-6 — max `DECIMAL(38,0) + 1` under ANSI raises — FIXED (2026-08-14" in text
    assert "**DEC-7 — `DECIMAL / 0` under ANSI raises — FIXED (2026-08-14" in text
    assert "**DEC-8 — `DECIMAL(38,20) * DECIMAL(38,20)` — FIXED (2026-08-14" in text
    # The landing PRs are named: #99 (U4b / DEC-8 planner / DEC-6 exec) and #94 (ANSI default TRUE).
    assert "#99" in text and "#94" in text
    # The equality / shared-raise pins that prove each note.
    for pin in (
        "pin_div_same_precision_scale_repark_i128",
        "pin_overflow_max_decimal38_plus_one_wrong_value_i128",
        "pin_div_by_zero_decimal38_raises_under_default_ansi",
        "pin_mul_38_20_still_refuses_at_plan",
        "mul_38_20_plans_via_the_expr_planner",
    ):
        assert pin in text, pin
    # The four-field BACKLOG rows are gone (converted to notes) — no heading, no stale intent.
    for heading in ("### DEC-2 —", "### DEC-6 —", "### DEC-7 —", "### DEC-8 —"):
        assert heading not in text, heading
    assert "needs a UDF / scaled division" not in text
    assert "still refuses with `AnalysisException`" not in text
    # The DEC-8 four-field row cited a Rust test that the fix deleted; the dead citation is gone.
    assert "mul_38_20_still_refuses_before_any_analyzer_rule" not in text
    # The old inbound anchors to the DEC-6 / DEC-7 headings are de-linked (no such headings now).
    assert "#dec-6--max-decimal380--1-under-ansi-returns-a-corrupted-value" not in text
    assert "#dec-7--decimal--0-under-ansi-returns-null" not in text


def test_tz8_row_splits_fixed_and_residual() -> None:
    """pins: reg-1-registry-truth-up/C-002 — TZ-8 names the FIXED half and the residual half."""
    text = _registry()
    assert "**Not FIXED.**" not in text
    # The row is now titled by what actually remains open.
    assert "### TZ-8 — `last_day` / `date_add` over a TIMESTAMP refuse to plan" in text
    # `date_sub` was measured to refuse too, but no pin exercises it — the row claims only
    # what a pin proves (registry §6: an unpinned divergence is prose).
    tz8 = text.index("### TZ-8")
    row_end = text.index("\n### ", tz8 + 1)
    assert "date_sub" not in text[tz8:row_end]
    # The FIXED half: the analyzer rewrite, its pin, and Spark's session-zone answer.
    assert "— FIXED (2026-08-14, #100" in text
    assert "rewrite_timestamp_to_date_cast" in text
    assert "timestamp_to_date_paths_read_the_session_zone" in text
    assert "2024-06-14" in text
    # The residual half: the red-on-purpose pin, its recorded live NY values, and B-TZ-3.
    assert "last_day_and_date_add_over_a_timestamp_still_refuse" in text
    assert "2024-06-30" in text and "2024-06-15" in text
    assert "B-TZ-3" in text
    # The stale pin name and the stale "stored zone" title/anchor are gone.
    assert "timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone" not in text
    assert "tz-8--timestampdate-outside-this-repos-coercion-path-reads-the-stored-zone" not in text


def test_g3e8_row_states_delivered_and_remainder() -> None:
    """pins: reg-1-registry-truth-up/C-003 — G3-E8 states delivered spellings and remainder."""
    text = _registry()
    # The stale "UPDATE IN … remain refused" phrasing is gone.
    assert "UPDATE IN / NOT IN / EXISTS, correlated IN," not in text
    assert "UPDATE IN + correlated IN / ANY / ALL stay valved" not in text
    # Delivered now includes correlated DELETE IN and uncorrelated identity UPDATE IN.
    assert "try_allowed_update_in" in text
    assert "g3e8_update_in_subquery_rewrites_only_the_matching_row" in text
    assert "g3e8_delete_correlated_in_deletes_exactly_the_matching_row" in text
    assert "[update_in_subquery]" in text
    # The true remainder is named as remainder, each side pinned.
    assert "correlated UPDATE IN" in text
    assert "g3e8_update_subquery_family_all_refuse" in text
    # G3-E8-NULL's UPDATE half stays refused.
    assert "### G3-E8-NULL" in text
    assert "update_not_in_subquery_with_null_key" in text


def test_status_known_issues_match_the_registry() -> None:
    """pins: reg-1-registry-truth-up/C-004 — STATUS bullets match the registry, under ceiling."""
    text = _status()
    # decimal128: DEC-2/6/7/8 now FIXED; only DEC-9 (and DEC-5 nullability) stay BACKLOG.
    assert "BACKLOG on DEC-2 / DEC-6 / DEC-7 / DEC-8" not in text
    assert "DEC-2 / DEC-6 / DEC-7 / DEC-8" in text and "FIXED" in text
    # session-timezone: TZ-8 is no longer wholesale "open".
    assert "**TZ-8 open**" not in text
    assert "#100" in text
    # DELETE/UPDATE: uncorrelated UPDATE IN executes now; the stale valve line is gone.
    assert "UPDATE IN and correlated IN / ANY / ALL stay valved" not in text
    assert "G3-E8-NULL's UPDATE half stays refused" in text
    # Each of the three bullets still links the registry, and STATUS stays under its ceiling.
    assert text.count("docs/spark-sql-iceberg-parity.md") >= 3
    ceiling = 25_000
    assert _STATUS.stat().st_size <= ceiling, _STATUS.stat().st_size


def test_cited_pins_exist_and_dec9_stays_open() -> None:
    """pins: reg-1-registry-truth-up/C-005 — every cited test exists; DEC-9 still reads BACKLOG."""
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
    # The Python corpus rows the notes cite are real parametrization ids.
    corpus = (_REPO / "python/repark/tests/test_decimal128_parity.py").read_text(encoding="utf-8")
    for row in ("div_same_precision_scale", "mul_38_20_plans_in_spark_refuses_in_repark"):
        assert f'"{row}"' in corpus, row
    dml = (_REPO / "python/repark/tests/test_dml_subquery_parity.py").read_text(encoding="utf-8")
    assert 'name="update_in_subquery"' in dml
    # The new notes replaced the DEC-8 / TZ-8 citations the fixes had renamed — the registry now
    # cites the live names, so a citation the notes write resolves to a real test.
    text = _registry()
    assert "mul_38_20_plans_via_the_expr_planner" in text
    assert "timestamp_to_date_paths_read_the_session_zone" in text
    # DEC-9 is genuinely open and must still read BACKLOG.
    assert "### DEC-9 — overflow-capable binary arithmetic is marked non-null" in text
    assert "BACKLOG, intent to FIX (gap G13). Nullability-only pin" in text


def test_no_row_deleted_and_maps_in_lockstep() -> None:
    """pins: reg-1-registry-truth-up/C-006 — no row silently deleted; the maps carry the new pin."""
    text = _registry()
    # Every DEC row id survives (as a FIXED note or a still-open row) — never a silent deletion.
    for dec in range(1, 10):
        # The row itself, not just its id: an open row keeps its heading, a landed one its
        # FIXED-note opener — an id surviving inside a range ("DEC-1 … DEC-9") is not a row.
        assert f"### DEC-{dec} —" in text or f"**DEC-{dec} —" in text, f"DEC-{dec}"
    for row in ("### TZ-8 —", "### G3-E8 —", "### G3-E8-NULL"):
        assert row in text, row
    # The maps carry this unit's pin file and ledger in lockstep.
    parity_map = (_REPO / "python/repark-parity/tests/map.md").read_text(encoding="utf-8")
    assert "test_reg_1_registry_truth_up.py" in parity_map
    staging_map = (_REPO / "task/ledgers/staging/map.md").read_text(encoding="utf-8")
    assert "reg-1-registry-truth-up" in staging_map
