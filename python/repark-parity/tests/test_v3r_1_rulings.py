"""V3R-1: the 2026-08-25 owner rulings are recorded where the gate reads them.

pins: v3r-1-rulings/C-007, C-009, C-010, C-011, C-013
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_RULING_DATE = "2026-08-25"


def _read(relative: str) -> str:
    return (_REPO / relative).read_text(encoding="utf-8")


def _registry() -> str:
    return _read("docs/spark-sql-iceberg-parity.md")


def _north_star() -> str:
    return _read("task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md")


def _matrix_row(text: str, label: str) -> str:
    for line in text.splitlines():
        if line.startswith(f"| {label}"):
            return line
    raise AssertionError(f"north-star §3 has no row starting with `| {label}`")


def test_v3_cow_1_is_a_refusal_row_dated_by_the_ruling() -> None:
    """C-007: the row keeps the ruling's refusals, BACKLOG, dated and pinned.

    RP-2 lifted the DV-free first DELETE. RP-3 lifted live-DV DELETE merge. Remaining
    refusals — UPDATE, MERGE, sequential COW after overwrite — stay BACKLOG.
    """
    registry = _registry()
    heading = (
        "### V3-COW-1 — v3 row-DML: measured DELETE lifts; UPDATE, MERGE, "
        "and sequential COW after overwrite refuse"
    )
    assert heading in registry
    row = registry[registry.index(heading) : registry.index("### Surfaced, awaiting pins")]
    assert f"owner ruling {_RULING_DATE}" in row
    assert "BACKLOG" in row
    assert "adopted_v3_cow_delete_carries_survivor_row_lineage" in row
    assert "adopted_v3_mor_second_delete_merges_into_the_live_deletion_vector" in row
    assert "adopted_v3_cow_second_delete_refuses_before_lineage_diverges" in row
    assert "live-DV" in row
    assert "commits and reassigns row lineage" not in registry.split("## 7.")[1].split(heading)[0]


def test_v3_geo_1_is_declared_and_shredded_variant_is_queued_not_rowed() -> None:
    """C-009: geometry/geography is a DECLARED row; shredded variant waits for V3-6's pin."""
    registry = _registry()
    assert "### V3-GEO-1 — the v3 `geometry` / `geography` types are not supported" in registry
    geo = registry[registry.index("### V3-GEO-1") : registry.index("## 5. Facade drop-in")]
    assert "DECLARED" in geo and _RULING_DATE in geo
    assert "v3_type_columns_geometry_geography_variant_refuse_naming_the_type" in geo
    queue = registry[registry.index("### Surfaced, awaiting pins") : registry.index("## 8.")]
    assert "**V3-VARIANT-SHRED-1**" in queue and _RULING_DATE in queue
    assert "### V3-VARIANT-SHRED-1" not in registry, "no row without a pin"


def test_north_star_matrix_carries_the_three_engine_rulings() -> None:
    """C-007 / C-009 / C-010: the gate's own rows say what was ruled, and when."""
    north_star = _north_star()
    cow = _matrix_row(north_star, "Write: COW DML on an adopted v3 table")
    assert "V3-COW-1" in cow and _RULING_DATE in cow and cow.count("🚫") == 1
    types = _matrix_row(north_star, "Read/write: v3 types + default values")
    assert "V3-GEO-1" in types and "V3-VARIANT-SHRED-1" in types and _RULING_DATE in types
    upgrade = _matrix_row(north_star, "Upgrade: v2 → v3 in place")
    assert "allowCreateFormatVersion3" in upgrade and _RULING_DATE in upgrade
    assert "or DECLARED" not in upgrade, "ruling 5 chose build, not declare"


def test_od_3b_is_ruled_in_and_the_runbook_carries_the_scoped_statement() -> None:
    """C-011: OD-3b in; the tier-2 runbook names the s3tables actions, scope, and the unknown."""
    north_star = _north_star()
    od_3b = north_star[north_star.index("- **OD-3b**") : north_star.index("- **Sequencing vs")]
    assert f"ruled {_RULING_DATE}" in od_3b and "docs/tier2-aws.md" in od_3b
    runbook = _read("docs/tier2-aws.md")
    for needle in (
        "s3tables:PutTableData",
        "s3tables:namespace",
        "testing_repark_acceptance",
        "Still no `s3tables:DeleteTable`",
        "a denial is a stop, not a design",
    ):
        assert needle in runbook, needle
    assert "No delete actions" not in runbook


def test_the_unit_leaves_no_obituary() -> None:
    """C-013: no departure line for V3R-1 anywhere the live documents are read."""
    for relative in ("STATUS.md", "briefs/next-sequence.md"):
        text = _read(relative)
        assert not re.search(r"V3R-1[^\n]*\b(merged|landed|departed)\b", text), relative
    assert "<!-- ws id=v3r" not in _read("STATUS.md")
