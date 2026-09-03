"""V1-GATE: the v1.0 north-star gate audit is written, dated, and every row it claims is real.

pins: v1-gate-audit/C-001, C-002, C-003, C-004, C-005, C-006
MUTATION: soften a §3.1 glyph, drop the dated gate line, drop the SCALE-v3 or V1-GATE line from
STATUS, drop a fork row from the §3.1 fork table, or unfile the status board → this REDs.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_NORTH_STAR = "task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md"
_REGISTRY = "docs/spark-sql-iceberg-parity.md"
_BOARD = "docs/artifacts/v1-0-gate-closing-2026-09-02.html"
_GATE_OPENING = (
    "**Audit result (V1-GATE, 2026-09-03; engineering item discharged by V3-COV, 2026-09-03).** "
    "§3.1 audits all twenty rows and the fork rows they lean on: every row ✅ or dated DECLARED "
    "as of 2026-09-03."
)
_SURFACE_RESIDUALS = {
    "12 · `rewrite_data_files`": ("RDF-1", "F-16 residue 2"),
    "14 · expiry / orphans": ("ORPHAN-1", "ORPHAN-2"),
    "15 · `rewrite_manifests`": ("MANIFEST-1", "MANIFEST-3"),
    "6 · Write: create v3": ("V3-COV-7", "V3-COV-8"),
    "9 · Write: MoR DML via deletion vectors": ("V3-COV-4",),
    "13 · Maintain: DV / delete-file maintenance": ("B-MOR-3-FLOOR-1",),
    "no §3 row · sort-order evolution": ("V3-COV-5", "RDF-SORT-1"),
}
_AUDIT_ROWS = 20
_RESIDUAL_ROWS = {
    3: ("V3-ROWID-2", "DECLARED", "2026-08-31"),
    4: ("V3-GEO-1", "DECLARED", "2026-08-25"),
    5: ("ENC-1", "DECLARED exclusion", "2026-08-24"),
    7: ("V3-UPGRADE-V4-1", "DECLARED", "2026-09-02"),
    9: ("V3-FILEORDER-1", "DECLARED", "2026-09-02"),
    17: ("S3T-1", "DECLARED service gap", "2026-08-27"),
}
_RESIDUAL_JUSTIFICATIONS = {
    "6 · Write: create v3": 'the cell reads "stays opt-in until V3-3; default remains v2"',
    "9 · Write: MoR DML via deletion vectors": (
        'the cell reads "full DML including UPDATE/MERGE, round-tripped"'
    ),
}
_FORK_ROWS = ("R88", "R91", "R114", "R126", "R167")


def _read(relative: str) -> str:
    """The whole document at ``relative``."""
    return (_REPO / relative).read_text(encoding="utf-8")


def _normalized(text: str) -> str:
    """``text`` with every run of whitespace collapsed to one space."""
    return " ".join(text.split())


def _audit_section() -> str:
    """The north star's §3.1 audit section, up to the gate paragraph."""
    north_star = _read(_NORTH_STAR)
    start = north_star.index("### 3.1 The gate audit")
    return north_star[start : north_star.index("**The gate.**", start)]


def _audit_row(number: int) -> str:
    """The §3.1 audit row whose first cell starts with ``| <number> · ``."""
    for line in _audit_section().splitlines():
        if line.startswith(f"| {number} · "):
            return line
    raise AssertionError(f"§3.1 has no audit row numbered {number}")


def _matrix_row(label: str) -> str:
    """The north-star §3 matrix row that starts with ``| <label>``."""
    for line in _read(_NORTH_STAR).splitlines():
        if line.startswith(f"| {label}"):
            return line
    raise AssertionError(f"north-star §3 has no row starting with `| {label}`")


def test_every_audited_row_is_green_and_none_is_backlog_blocked() -> None:
    """C-001: all twenty rows are ✅, and a BACKLOG residual needs its out-of-requires clause."""
    for number in range(1, _AUDIT_ROWS + 1):
        row = _audit_row(number)
        glyph = row.split("|")[2].strip()
        assert glyph == "✅", f"row {number} glyph is {glyph!r}"
        if "BACKLOG" in row:
            assert "outside the requires cell" in row, f"row {number} BACKLOG is unscoped"
    assert f"| {_AUDIT_ROWS + 1} · " not in _audit_section()


def _surface_residual_table() -> list[str]:
    """Only the rows of §3.1's surface-residuals table — never §3's own audit rows."""
    lines = _audit_section().splitlines()
    start = next(
        index
        for index, line in enumerate(lines)
        if "Surface residuals outside the requires cells" in line
    )
    rows = []
    for line in lines[start:]:
        if (
            line.startswith("| ")
            and not line.startswith("| Row |")
            and set(line) != {"|", "-", " "}
        ):
            rows.append(" ".join(line.split()))
        elif rows and not line.startswith("|"):
            break
    return rows


def test_the_audit_is_scoped_to_the_v1_0_requires_cells() -> None:
    """C-001: the preamble scopes the audit, and the surface residuals are listed with a class."""
    section = _normalized(_audit_section())
    assert "**v1.0 requires** cell" in section
    assert "a residual inside one whose class is BACKLOG blocks the gate, and none is" in section
    assert "Surface residuals outside the requires cells — recorded, not gating." in section
    residual_table = _surface_residual_table()
    for row, residuals in _SURFACE_RESIDUALS.items():
        matching = [line for line in residual_table if line.startswith(f"| {row} |")]
        assert len(matching) == 1, row
        for residual in residuals:
            pattern = rf"(?<![\w-]){re.escape(residual)}(?![\w-])"
            assert re.search(pattern, matching[0]), (row, residual)
    for row, quoted in _RESIDUAL_JUSTIFICATIONS.items():
        line = next(line for line in residual_table if line.startswith(f"| {row} |"))
        assert quoted in _normalized(line), (row, quoted)
    assert "open residue on a FIXED row, fork-owned" in section
    assert "DECLARED, owner decision OD-2 (ruled 2026-08-21)" in section
    assert "BACKLOG, both v2-measured" in section
    assert "BACKLOG, both measured 2026-09-03 by V3-COV" in section
    assert "BACKLOG, measured 2026-09-03 by V3-COV" in section


def test_the_unrowed_v1_0_requirement_is_discharged_with_its_measured_totals() -> None:
    """C-002 + V3-COV: §2 pillar 4 carries the measured matrix, not the search that found it."""
    section = _normalized(_audit_section())
    assert "§2 pillar 4 — discharged (V3-COV, 2026-09-03)" in section
    assert "full statement-coverage comparison against PySpark" in _normalized(_read(_NORTH_STAR))
    assert "ten cells over two fixtures" in section
    assert "no statement-coverage harness at any format version" in section
    assert "Statement coverage measured 2026-09-03" in section
    assert "81 statement programs" in section
    assert "267 comparison cells" in section
    assert "72 EQUAL" in section
    assert "audited in the surface-residuals table above" in section
    assert "Nothing in §2 pillar 4 is now owed." in section
    assert "docs/design/v3-statement-coverage.md" in section


def test_each_residual_names_its_registry_row_class_and_date() -> None:
    """C-001: a row with a residual cites the registry row, its class word and its date."""
    for number, (registry_row, klass, date) in _RESIDUAL_ROWS.items():
        row = _audit_row(number)
        assert registry_row in row, f"row {number} does not name {registry_row}"
        assert klass in row, f"row {number} does not carry the class {klass!r}"
        assert date in row, f"row {number} does not carry the date {date}"
        assert registry_row in _read(_REGISTRY), registry_row


def test_every_audit_row_names_a_pin() -> None:
    """C-001: the pin cell of every row names a real tracked path or the ledger that holds it."""
    for number in range(1, _AUDIT_ROWS + 1):
        pin_cell = _audit_row(number).rstrip().rstrip("|").rsplit("|", 1)[1]
        assert pin_cell.strip(), f"row {number} has an empty pin cell"
        cited = [
            token.strip("`,;")
            for token in pin_cell.split()
            if token.startswith("`crates/") or token.startswith("`python/")
        ]
        for path in cited:
            assert (_REPO / path.split("::")[0]).exists(), f"row {number}: {path}"


def test_the_three_softened_glyphs_are_now_green_with_their_dated_clause() -> None:
    """C-001: types, encryption and DV maintenance read ✅ by dated DECLARED residual."""
    types = _matrix_row("Read/write: v3 types + default values")
    assert types.startswith("| Read/write: v3 types + default values | ✅ by dated DECLARED")
    assert "V3-GEO-1" in types and "V3-VARIANT-SHRED-1" in types
    encryption = _matrix_row("Table encryption keys")
    assert "✅ by dated DECLARED exclusion" in encryption and "❌" not in encryption
    assert "ENC-1" in encryption and "2026-08-24" in encryption
    maintenance = _matrix_row("Maintain: DV / delete-file maintenance")
    assert "✅ B-MOR-3 FIXED" in maintenance and "⚠" not in maintenance
    assert "B-MOR-3-FLOOR-1" in maintenance


def test_the_rewrite_manifests_row_records_its_v3_exercise() -> None:
    """C-001: `rewrite_manifests` is exercised on v3, with the SCALE-v3 counts on the row."""
    row = _matrix_row("Maintain: `rewrite_manifests`")
    assert "exercised on v3 by SCALE-v3 (2026-09-02)" in row
    assert "59 manifests → 1" in row and "10 → 1" in row
    assert "MANIFEST-1/2/3" in row


def test_the_gate_carries_one_dated_audit_line_and_claims_no_tag() -> None:
    """C-002: one dated result line names what is still owed and never claims the tag."""
    north_star = _normalized(_read(_NORTH_STAR))
    assert _normalized(_GATE_OPENING) in north_star
    assert north_star.count("**Audit result (V1-GATE") == 1
    assert "ruled 2026-09-03 BUILD on `B-MOR-3`" in north_star
    assert "The v1.0 tag is what remains." in north_star
    assert "**V3-COV**" in north_star
    assert "no engineering item remains on this gate" in north_star
    assert "v1.0 is tagged" not in north_star
    assert "the API review (owner) is the remaining gate item" not in north_star
    assert "no gate item remains on the review" in north_star


def test_the_two_softest_class_cells_say_exactly_what_the_registry_holds() -> None:
    """C-001: row 13's OD-2 analogy, row 3's queue entry and row 17's undated gap are explicit."""
    thirteen = _audit_row(13)
    assert "B-MOR-3" in thirteen and "FIXED 2026-09-03" in thirteen
    assert "owner ruling: build" in thirteen
    three = _audit_row(3)
    assert 'queue** entry under §7 "Surfaced, awaiting pins"' in three
    assert "not a §7 row" in three
    seventeen = _audit_row(17)
    assert "**undated on `S3T-1`**" in seventeen
    assert "dated 2026-08-27 on fork R126 (c)" in seventeen
    registry = _read(_REGISTRY)
    assert "### Surfaced, awaiting pins — not yet rows" in registry


def test_step_6_and_the_slate_carry_the_same_disposition() -> None:
    """C-002: the v3 track dates the answered API review and queues V3-COV where the gate says."""
    track = _normalized(_read("docs/design/format-v3-track.md"))
    assert "*Step 6 state, dated 2026-09-03 (V1-GATE).*" in track
    assert "*Step 6 state, dated 2026-09-03 (V3-COV).*" in track
    assert "the v1.0 API review, answered 2026-09-02" in track
    assert "V3-COV measured the statement" in track
    assert "Step 6 now owes **no engineering item**" in track


def test_the_fork_side_rows_are_listed_and_dated_at_the_pin() -> None:
    """C-004: every 🟡 fork row the gate leans on is named with a dated cell at `594bdbe5`."""
    section = _normalized(_audit_section())
    assert "at the consumed pin `594bdbe5`" in section
    for row in _FORK_ROWS:
        assert f"| {row} ·" in section or f"{row} ·" in section, row
    assert "R89" in section and "R130" in section and "R136" in section
    cargo = _read("Cargo.toml")
    assert "594bdbe5f257455d77ac49f1a2d50794a1aea6fd" in cargo


def test_status_carries_the_scale_line_the_gate_line_and_its_ceiling() -> None:
    """C-003: STATUS records SCALE-v3 and the audit, and stays under the compaction ceiling."""
    status = _normalized(_read("STATUS.md"))
    assert "**SCALE-v3 (2026-09-02):**" in status
    assert "96 delete files against v2's 400" in status
    assert "zero delete files and zero delete records" in status
    assert "**The gate is audited (V1-GATE, 2026-09-03) and §2 pillar 4 is discharged" in status
    assert "**V3-COV**" in status
    assert "Result at acceptance" in status
    assert "**V3-10 (2026-09-02):**" in status
    assert "**RDF-1 (2026-09-02):**" in status
    assert "**LOG1P-1 (2026-09-02):**" in status
    assert "_Last updated: 2026-09-03._" in status
    assert (_REPO / "STATUS.md").stat().st_size <= 25_000


def test_the_status_board_is_filed_and_mapped() -> None:
    """C-006: the published gate board is in the tree and its map row names its sources."""
    board = _read(_BOARD)
    assert "<title>v1.0 Gate Closing</title>" in board
    assert "v1.0 Gate Closing" in board
    mapped = _read("docs/artifacts/map.md")
    assert "[v1-0-gate-closing-2026-09-02.html](v1-0-gate-closing-2026-09-02.html)" in mapped
    assert "Sources:" in mapped.split("v1-0-gate-closing-2026-09-02.html")[-1]


def test_the_audit_is_dated_once_and_the_unit_leaves_no_obituary() -> None:
    """C-005: the audit carries one date, and no live document narrates V1-GATE's departure."""
    section = _normalized(_audit_section())
    assert "2026-09-03 (V1-GATE)" in section
    assert section.count("2026-09-03 (V1-GATE)") == 1
    for relative in ("STATUS.md", "briefs/next-sequence.md"):
        text = _read(relative)
        assert "V1-GATE merged" not in text, relative
        assert "V1-GATE landed" not in text, relative
