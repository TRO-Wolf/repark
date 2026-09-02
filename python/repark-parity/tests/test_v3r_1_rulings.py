"""V3R-1: the 2026-08-25 owner rulings are recorded where the gate reads them.

pins: v3r-1-rulings/C-007, C-009, C-010, C-011, C-013
pins: v3-3-dml/C-003
pins: v3-7-merge-lineage/C-003
pins: v3-8-subquery-where-lineage/C-003
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


def test_v3_cow_1_is_fixed_and_discharges_the_ruling() -> None:
    """C-007: V3-8 closes the row; the ruling is discharged and every earlier pin is kept.

    RP-6 lifted plain-WHERE, V3-7 MERGE, V3-8 subquery-WHERE.
    """
    registry = _registry()
    heading = (
        "### V3-COW-1 — FIXED (V3-8, 2026-09-02): v3 row-DML keeps row lineage "
        "on every served shape"
    )
    assert heading in registry
    row = registry[registry.index(heading) : registry.index("### BL-9")]
    assert f"owner ruling {_RULING_DATE}" in row and "discharged" in row
    assert "stays BACKLOG" not in row and "BACKLOG for that shape" not in row
    assert "FIXED (V3-8, 2026-09-02)" in row
    assert "adopted_v3_cow_delete_carries_survivor_row_lineage" in row
    assert "adopted_v3_cow_second_delete_keeps_survivor_row_id" in row
    assert "adopted_v3_cow_update_keeps_row_id_and_bumps_matched_seq" in row
    assert "adopted_v3_cow_merge_matched_update_keeps_row_id" in row
    assert "v3_subquery_dml.rs" in row
    assert "F-rp3-c7" in row and "F-v3-8-update-files" in row
    assert "row_lineage_guard.rs" in row, "the deleted refusal seat is named"
    assert "ride V3-3" not in row


def test_v3_geo_1_is_declared_and_shredded_variant_is_rowed_with_v3_6_pins() -> None:
    """C-009 + V3-6: geometry/geography stay DECLARED; shredded variant is a DECLARED row
    citing the binary-vs-shredded pins V3-6 measured."""
    registry = _registry()
    assert "### V3-GEO-1 — the v3 `geometry` / `geography` types are not supported" in registry
    geo = registry[registry.index("### V3-GEO-1") : registry.index("### V3-VARIANT-SHRED-1")]
    assert "DECLARED" in geo and _RULING_DATE in geo
    assert "v3_type_columns_geometry_geography_variant_refuse_naming_the_type" in geo
    shred = registry[
        registry.index("### V3-VARIANT-SHRED-1") : registry.index("## 5. Facade drop-in")
    ]
    assert "DECLARED" in shred and _RULING_DATE in shred
    assert "fork_variant_arrow_maps_and_parquet_write_refuses" in shred
    assert "fork_variant_scan_refuses_naming_the_type" in shred
    assert "R88" in shred


def test_north_star_matrix_carries_the_three_engine_rulings() -> None:
    """C-007 / C-009 / C-010: the gate's own rows say what was ruled, and when.

    pins: v3-9-mor-predicate-dml-dv/C-005
    """
    north_star = _north_star()
    cow = _matrix_row(north_star, "Write: COW DML on an adopted v3 table")
    assert "V3-COW-1" in cow and _RULING_DATE in cow and cow.count("🚫") == 0
    assert "FIXED" in cow and "V3-8" in cow
    assert "V3-7" in cow and "F-rp3-c7" in cow and "F-v3-8-update-files" in cow
    mor = _matrix_row(north_star, "Write: MOR DML via deletion vectors")
    assert "V3-9" in mor and "V3-MOR-1" in mor and mor.count("🚫") == 0
    assert "V3-7" in mor and "RP-6" in mor
    status = _read("STATUS.md")
    assert "V3-7 / V3-8 (2026-09-02" in status
    assert "**V3-9 (2026-09-02):**" in status
    assert "F-rp3-c7\n    consumed" in status
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
