"""V3-COV: the coverage document, the registry and the discharge lines all hold one matrix.

pins: v3-cov-statement-coverage/C-001, C-004, C-005
MUTATION: change a §1 total, drop a matrix row, drop a registry row filed by this unit, unpin a
diverging row, or soften a discharge line → this REDs.
"""

from __future__ import annotations

import re
from functools import cache
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_DOC = "docs/design/v3-statement-coverage.md"
_REGISTRY = "docs/spark-sql-iceberg-parity.md"
_NORTH_STAR = "task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md"
_TRACK = "docs/design/format-v3-track.md"
_HARNESS = "python/repark/tests/_v3_statement_coverage_programs.py"
_GOLDEN = "python/repark/tests/_v3_statement_coverage_repark.py"
_SPARK_GOLDEN = "python/repark/tests/_v3_statement_coverage_spark.py"
_FILED = (
    "V3-COV-1",
    "V3-COV-2",
    "V3-COV-3",
    "V3-COV-4",
    "V3-COV-5",
    "V3-COV-6",
    "V3-COV-7",
    "V3-COV-8",
)
_CITED = ("DML-1", "G3-E8", "B-MOR-3")
_TOTALS = {
    "Statement programs measured": 81,
    "Comparison cells (statements + probes)": 267,
    "**EQUAL** — repark and Spark agree on every cell": 71,
    "**REFUSED** — both engines refuse the statement": 1,
    "**DIVERGES** — a registry row": 9,
}


@cache
def _read(relative: str) -> str:
    """The whole document at ``relative``, read once per session."""
    return (_REPO / relative).read_text(encoding="utf-8")


@cache
def _matrix_rows() -> list[list[str]]:
    """Every §3 matrix row of the coverage document, split into its cells; parsed once."""
    rows = []
    for line in _read(_DOC).splitlines():
        match = re.match(r"^\| `([a-z0-9-]+)` \|", line)
        if match:
            rows.append([cell.strip() for cell in line.strip().strip("|").split("|")])
    return rows


def test_the_matrix_row_count_and_verdicts_match_the_stated_totals() -> None:
    """C-001: §1's totals are counted from §3, not asserted beside it."""
    rows = _matrix_rows()
    verdicts = [row[7] for row in rows]
    assert len(rows) == _TOTALS["Statement programs measured"]
    assert (
        verdicts.count("**EQUAL**") == _TOTALS["**EQUAL** — repark and Spark agree on every cell"]
    )
    assert verdicts.count("**REFUSED**") == 1
    assert verdicts.count("**DIVERGES**") == _TOTALS["**DIVERGES** — a registry row"]
    assert set(verdicts) == {"**EQUAL**", "**REFUSED**", "**DIVERGES**"}


def test_every_stated_total_appears_in_the_totals_table() -> None:
    """C-001: each §1 count is written where a reader looks for it."""
    doc = _read(_DOC)
    for label, count in _TOTALS.items():
        assert f"| {label} | **{count}** |" in doc or f"| {label} | {count} |" in doc, label


def test_every_diverging_row_names_a_registry_row_that_exists() -> None:
    """C-004: a DIVERGES verdict is never prose — it cites a row the registry actually holds."""
    registry = _read(_REGISTRY)
    diverging = [row for row in _matrix_rows() if row[7] == "**DIVERGES**"]
    assert len(diverging) == _TOTALS["**DIVERGES** — a registry row"]
    for row in diverging:
        cited = row[8].strip("`").strip()
        assert cited and cited != "—", row[0]
        assert re.search(rf"^#+ {re.escape(cited)} —", registry, re.MULTILINE), cited


def test_the_rows_this_unit_filed_carry_a_class_a_date_and_a_pin() -> None:
    """C-004: every new registry row is dated 2026-09-03 and names its pinning test."""
    registry = _read(_REGISTRY)
    for row in _FILED:
        start = registry.index(f"{row} — ")
        body = registry[start : start + 3000]
        end = body.find("\n### ")
        body = body[: end if end > 0 else len(body)]
        assert "2026-09-03" in body, row
        assert any(word in body for word in ("FIXED", "DECLARED", "BACKLOG")), row
        assert "test_v3_statement_coverage.py" in body, row


def test_the_fork_routed_rows_name_a_trigger() -> None:
    """C-004: a row the fork owns says what retires it, so it cannot sit as a permanent gap."""
    registry = _read(_REGISTRY)
    for row in ("V3-COV-3", "V3-COV-6"):
        start = registry.index(f"{row} — ")
        body = registry[start : start + 3000]
        assert "TRIGGER:" in body, row


def test_the_north_star_carries_the_measured_discharge() -> None:
    """C-005: §2 pillar 4 is discharged with its totals, and no longer reads as owed."""
    north_star = _read(_NORTH_STAR)
    assert "§2 pillar 4 — discharged (V3-COV, 2026-09-03)" in north_star
    assert "Statement coverage measured 2026-09-03" in north_star
    assert "Nothing in §2 pillar 4 is now owed." in north_star
    assert "Recorded as owed, not claimed." not in north_star
    assert "no engineering item remains on this gate" in north_star


def test_the_v3_track_step_6_carries_the_v3_cov_state_line() -> None:
    """C-005: Step 6 dates the coverage run and stops listing it as work to run."""
    track = _read(_TRACK)
    assert "*Step 6 state, dated 2026-09-03 (V3-COV).*" in track
    assert "Step 6 now owes **no engineering item**" in track
    assert "Full v3 statement coverage is the one that is not done" not in track
    programs = _TOTALS["Statement programs measured"]
    assert f"{programs} statement programs" in " ".join(track.split())


def test_the_harness_and_the_golden_carry_every_matrix_row() -> None:
    """C-002: each documented row is a real program name in the committed harness and golden."""
    harness = _read(_HARNESS)
    golden = _read(_GOLDEN)
    spark = _read(_SPARK_GOLDEN)
    for row in _matrix_rows():
        name = row[0].strip("`")
        assert f'"{name}"' in harness or f"'{name}'" in harness, name
        assert f'"{name}"' in golden, name
        assert f'"{name}"' in spark, name


def test_the_rows_an_existing_registry_row_covers_are_cited_not_refiled() -> None:
    """C-004: a divergence an older row already owns cites that row instead of minting a new one."""
    doc = _read(_DOC)
    for row in _CITED:
        assert f"`{row}`" in doc, row
    assert "V3-COV-9" not in _read(_REGISTRY)
