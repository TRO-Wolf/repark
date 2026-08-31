"""Pin the owner-approved North Star and FNP planning contracts.

pins: plan-1-northstar-fnp-sequence/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _read(relative_path: str) -> str:
    """Read one UTF-8 repository file."""
    return (_REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")


def _assert_in_order(text: str, tokens: tuple[str, ...]) -> None:
    """Assert that each token occurs after the preceding token."""
    position = -1
    for token in tokens:
        next_position = text.find(token, position + 1)
        assert next_position >= 0, token
        position = next_position


def _normalize_whitespace(text: str) -> str:
    """Collapse Markdown line wrapping for semantic assertions."""
    return " ".join(text.split())


def test_north_star_sequence_keeps_the_guard_before_the_fork_fix() -> None:
    """C-001: RP-2 stays guarded, F-17 repairs the invariant, and RP-3 consumes it."""
    design = _read("docs/design/format-v3-track.md")
    _assert_in_order(
        design,
        (
            "Step 1 — land the narrowed RP-2 increment",
            "Step 2 — repair shared-Puffin DV closure in the fork",
            "Step 3 — charter RP-3 against one post-fix fork SHA",
            "Step 4 — deliver V3-3 and the guarded upgrade",
            "Step 5 — run the remaining product units on their real dependencies",
            "Step 6 — close the v1.0 gate",
        ),
    )
    assert "V3-6 may run in parallel with V3-3 or V3-4" in _normalize_whitespace(design)
    north_star = _read("task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md")
    _assert_in_order(north_star, ("narrowed, guarded RP-2", "fork F-17", "RP-3", "V3-3"))


def test_live_slate_retires_v3e_5_and_queues_the_safe_work() -> None:
    """C-002: the rolling slate starts with fork-independent work; FNP fills the gaps.

    pins: v3-3-dml/C-003
    pins: v3-4-serve-lineage-columns/C-010
    """
    slate = _read("briefs/next-sequence.md")
    assert "<!-- unit id=f-y10-1" not in slate
    assert "<!-- unit id=fnp-15-16" not in slate
    assert "<!-- unit id=mw-10" not in slate
    assert "<!-- unit id=v3e-5" not in slate
    assert "<!-- unit id=rp-2" not in slate
    assert "<!-- unit id=rp-3" not in slate
    flat = _normalize_whitespace(slate)
    assert "V3-4 and the engine units after it" in flat
    assert "V3-3 delivered 2026-08-30" in flat
    status = _read("STATUS.md")
    assert "V3E-5 added the nightly v3 live-oracle leg" in status
    assert "**Next:** V3-5 / F-7" in status


def test_fork_handoff_records_the_shared_puffin_failure_and_acceptance() -> None:
    """C-003: F-17 carries the measured failure, mechanism, reuse point, and cross-engine pin."""
    handoff = _read("task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md")
    f17 = _normalize_whitespace(
        handoff[handoff.index("### F-17") : handoff.index("## 4. Not fork work")]
    )
    for token in (
        "different partitions",
        "The expected live set is `{3,4,6}`; the measured set is `{3,4,5,6}`",
        "removes the old delete manifest entry by Puffin path",
        "rewrite_data_files_dv.rs",
        "DELETE or UPDATE",
        "Recompute and publish correct offsets, lengths, and file size",
        "Java reads the exact survivor rows",
        "A sabotage variant",
        "RP-2 keeps its broad live-DV refusal",
        "RP-3 retargets those pins",
    ):
        assert token in f17, token


def test_fnp_documents_share_one_remaining_order_and_delivery_shape() -> None:
    """C-004: the design, brief, and status use per-unit PRs and the same remaining order."""
    order = (
        "FNP-15/16",
        "FNP-4c",
        "FNP-7a/7b",
        "FNP-9/10",
        "FNP-8",
        "FNP-11/12",
        "FNP-Z",
    )
    sections = {
        "docs/design/spark-function-parity.md": (
            "The remaining order is:",
            "Four units are deferred",
        ),
        "briefs/spark-function-parity.md": ("The remaining order is:", "FNP-4b, FNP-6d"),
        "STATUS.md": ("**Next, in order (revised 2026-08-30):**", "<!-- /ws -->"),
    }
    for relative_path, (start_marker, end_marker) in sections.items():
        text = _read(relative_path)
        sequence = text[text.index(start_marker) : text.index(end_marker, text.index(start_marker))]
        _assert_in_order(sequence, order)
        assert "one coherent" in text
    assert "Twenty units, one branch, one PR" not in _read("briefs/spark-function-parity.md")


def test_fnp_retirement_and_fork_independence_are_explicit() -> None:
    """C-005: FNP closes at FNP-Z and remains independent of the North Star fork blocker."""
    ledger = _read("task/ledgers/staging/fnp-0-charter-ledger.md")
    assert "campaign closes (FNP-Z merges" in ledger
    assert "remaining campaign ships one coherent PR per unit or tightly coupled pair" in ledger
    design = _read("docs/design/format-v3-track.md")
    assert "FNP, TA performance, dbt, and the general correctness backlog may run" in design
    assert "The FNP and TA performance campaigns consume none of these fork surfaces" in design


def test_navigation_describes_the_revised_authoritative_documents() -> None:
    """C-006: each touched planning directory maps the revised contract."""
    assertions = {
        "briefs/map.md": ("**FNP-15/16**", "per remaining unit or coupled pair"),
        "docs/design/map.md": ("2026-08-28 per-unit delivery order",),
        "task/roadmap/epic-term/map.md": ("F-17 shared-Puffin closure",),
        "task/roadmap/mid-term/map.md": ("**F-17 added 2026-08-28 from RP-2:**",),
        "task/ledgers/completed/map.md": ("**V3-3 (2026-08-30)", "F-rp3-c7"),
        "task/ledgers/archive/2026-08/map.md": (
            "salvaged 2026-08-28",
            "became fork F-17",
            "**RP-3 (2026-08-28)",
            "opt-in for callers",
        ),
    }
    for relative_path, tokens in assertions.items():
        text = _read(relative_path)
        for token in tokens:
            assert token in text, f"{relative_path}: {token}"
