"""V1-GATE: the v1.0 north-star gate audit is written, dated, and every row it claims is real.

pins: v1-gate-audit/C-001, C-002, C-003, C-004, C-005, C-006
MUTATION: soften a §3.1 glyph, drop the dated gate line, drop the SCALE-v3 or V1-GATE line from
STATUS, drop a fork row from the §3.1 fork table, or unfile the status board → this REDs.
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_NORTH_STAR = "task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md"
_REGISTRY = "docs/spark-sql-iceberg-parity.md"
_BOARD = "docs/artifacts/v1-0-gate-closing-2026-09-02.html"
_GATE_LINE = (
    "**Audit result (V1-GATE, 2026-09-03).** §3.1 audits all twenty rows and the fork rows they "
    "lean on: every row ✅ or dated DECLARED as of 2026-09-03; the v1.0 tag is the owner's "
    "remaining step."
)
_AUDIT_ROWS = 20
_RESIDUAL_ROWS = {
    3: ("V3-ROWID-2", "DECLARED", "2026-08-31"),
    4: ("V3-GEO-1", "DECLARED", "2026-08-25"),
    5: ("ENC-1", "DECLARED exclusion", "2026-08-24"),
    7: ("V3-UPGRADE-V4-1", "DECLARED", "2026-09-02"),
    9: ("V3-FILEORDER-1", "DECLARED", "2026-09-02"),
    13: ("B-MOR-3", "OD-2", "2026-08-21"),
    17: ("S3T-1", "DECLARED service gap", "2026-08-27"),
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
    """C-001: all twenty rows carry ✅ in the glyph cell and no residual is BACKLOG."""
    for number in range(1, _AUDIT_ROWS + 1):
        row = _audit_row(number)
        glyph = row.split("|")[2].strip()
        assert glyph == "✅", f"row {number} glyph is {glyph!r}"
        assert "BACKLOG" not in row, f"row {number} names a BACKLOG residual"
    assert f"| {_AUDIT_ROWS + 1} · " not in _audit_section()


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
    assert "✅ by dated DECLARED residual" in maintenance and "⚠" not in maintenance
    assert "B-MOR-3" in maintenance and "OD-2" in maintenance


def test_the_rewrite_manifests_row_records_its_v3_exercise() -> None:
    """C-001: `rewrite_manifests` is exercised on v3, with the SCALE-v3 counts on the row."""
    row = _matrix_row("Maintain: `rewrite_manifests`")
    assert "exercised on v3 by SCALE-v3 (2026-09-02)" in row
    assert "59 manifests → 1" in row and "10 → 1" in row
    assert "MANIFEST-1/2/3" in row


def test_the_gate_carries_one_dated_audit_line_and_claims_no_tag() -> None:
    """C-002: the gate paragraph gains exactly one dated result line and never claims the tag."""
    north_star = _normalized(_read(_NORTH_STAR))
    assert _normalized(_GATE_LINE) in north_star
    assert north_star.count("**Audit result (V1-GATE") == 1
    assert "the v1.0 tag is the owner's remaining step" in north_star
    assert "v1.0 is tagged" not in north_star
    assert "the API review (owner) is the remaining gate item" not in north_star
    assert "no gate item remains on the review" in north_star


def test_the_fork_side_rows_are_listed_and_dated_at_the_pin() -> None:
    """C-004: every 🟡 fork row the gate leans on is named with a dated cell at `ff4764d3`."""
    section = _normalized(_audit_section())
    assert "at the consumed pin `ff4764d3`" in section
    for row in _FORK_ROWS:
        assert f"| {row} ·" in section or f"{row} ·" in section, row
    assert "R89" in section and "R130" in section and "R136" in section
    cargo = _read("Cargo.toml")
    assert "ff4764d3eba037ecfa185be5de5f639cbffef80b" in cargo


def test_status_carries_the_scale_line_the_gate_line_and_its_ceiling() -> None:
    """C-003: STATUS records SCALE-v3 and the audit, and stays under the compaction ceiling."""
    status = _normalized(_read("STATUS.md"))
    assert "**SCALE-v3 (2026-09-02):**" in status
    assert "96 delete files against v2's 400" in status
    assert "zero delete files and zero delete records" in status
    assert "**The gate is audited (V1-GATE, 2026-09-03):**" in status
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
