"""DL-5: compact the live STATUS remainder and the contributor contract."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_REPO = Path(__file__).resolve().parents[3]
_SCRIPTS = _REPO / "scripts"
_METHOD = _REPO / ".agents/skills/engineering-method/SKILL.md"
_AGENTS = _REPO / "AGENTS.md"
_STATUS = _REPO / "STATUS.md"


def _load(name: str) -> ModuleType:
    """Load a scripts/ module by file name."""
    specification = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[f"dl5_{name}"] = module
    specification.loader.exec_module(module)
    return module


def _milestone() -> str:
    """The Current milestone section body."""
    text = _STATUS.read_text(encoding="utf-8")
    start = text.index("## Current milestone\n")
    rest = text[start + len("## Current milestone\n") :]
    next_heading = rest.find("\n## ")
    return rest if next_heading < 0 else rest[:next_heading]


def test_current_milestone_keeps_the_forward_path() -> None:
    """pins: dl-5-contract-compaction/C-001 — live path stays; the manifest still matches."""
    section = _milestone().lower()
    assert "milestone one is complete" in section
    assert "bugfix-only" in section
    assert "single-writer-per-table" in section


def test_status_no_longer_pastes_the_h2_wave_diary() -> None:
    """pins: dl-5-contract-compaction/C-002 — the diary already has archived homes."""
    text = _STATUS.read_text(encoding="utf-8")
    assert "Y-wave PRs" not in text
    assert "Z-wave PRs" not in text
    assert "W-wave PRs" not in text
    archive = _REPO / "task/ledgers/archive/2026-08"
    for name in (
        "2026-08-13-z5-landing-increment-ledger.md",
        "2026-08-13-w5-z-landing-ledger.md",
        "2026-08-13-v5-w-landing-ledger.md",
        "2026-08-13-s5-v-landing-ledger.md",
    ):
        assert (archive / name).is_file(), name


def test_status_ceiling_ratcheted_down() -> None:
    """pins: dl-5-contract-compaction/C-003 — the STATUS ratchet moved with the trim."""
    gate = _load("check_docs_compaction")
    status_ceiling = gate.CEILINGS["STATUS.md"]
    assert status_ceiling < 31_000
    assert _STATUS.stat().st_size <= status_ceiling


def test_engineering_method_points_at_agents_for_invariants() -> None:
    """pins: dl-5-contract-compaction/C-004 — the skill is not a second contract."""
    text = _METHOD.read_text(encoding="utf-8")
    language = text[text.index("## Language-Specific Rules") : text.index("## Function Length")]
    navigation = text[text.index("## Navigation:") : text.index("## Naming Conventions")]
    assert "AGENTS.md" in language
    assert "AGENTS.md" in navigation
    assert "Panic-driven control flow is forbidden" not in text


def test_engineering_method_keeps_the_method() -> None:
    """pins: dl-5-contract-compaction/C-005 — compaction does not delete the working method."""
    text = _METHOD.read_text(encoding="utf-8")
    for marker in (
        "<risk_first>",
        "<verification_gate>",
        "<scope_boundaries>",
        "## Mode Handling",
        "## Naming Conventions",
    ):
        assert marker in text, marker


def test_agents_md_keeps_the_universal_invariants() -> None:
    """pins: dl-5-contract-compaction/C-006 — obligations stay explicit in the contract."""
    text = _AGENTS.read_text(encoding="utf-8")
    for needle in (
        "iceberg-rust is forked",
        "never vendored",
        "Two honest SQL doors",
        "Tests in the same commit as code",
        "`map.md` in every directory",
        'unsafe_code = "forbid"',
        "cargo test --workspace",
        "Never drop or delete a Glue table",
        "The live `Cargo.toml` is the SSOT",
    ):
        assert needle in text, needle


def test_no_role_packet_directory() -> None:
    """pins: dl-5-contract-compaction/C-007 — no fourth copy of the rules."""
    assert not (_REPO / ".agents/roles").exists()


def test_ceilings_cover_the_contract_files() -> None:
    """pins: dl-5-contract-compaction/C-008 — (d) bites every CEILINGS key."""
    gate = _load("check_docs_compaction")
    assert "AGENTS.md" in gate.CEILINGS
    assert ".agents/skills/engineering-method/SKILL.md" in gate.CEILINGS
    tight = {**gate.CEILINGS, "AGENTS.md": _AGENTS.stat().st_size - 1}
    findings = gate.findings(_REPO, tight)
    assert any(line.startswith("AGENTS.md:") and "exceeds its ceiling" in line for line in findings)
    assert not any(
        "closed campaign" in line or "outside any `ws` block" in line for line in findings
    )
    assert gate.findings(_REPO, gate.CEILINGS) == []


def test_dl_4_rule_text_still_holds() -> None:
    """pins: dl-5-contract-compaction/C-009 — this unit does not break DL-4 C-008."""
    agents = _AGENTS.read_text(encoding="utf-8")
    assert agents.count("**A live document carries no obituary.**") == 1
    assert "make check-docs-compaction" in agents


def test_pyc_5_load_bearing_tokens_remain_in_agents() -> None:
    """pins: dl-5-contract-compaction/C-010 — PYC-5 tree pins stay green after compaction."""
    agents = _AGENTS.read_text(encoding="utf-8")
    assert "**Not** on the pre-commit hook as of PYC-5" in agents
    assert "sub-second budget" in agents


def test_method_keeps_how_to_with_no_agents_home() -> None:
    """pins: dl-5-contract-compaction/C-011 — method keeps how-to with no AGENTS.md home."""
    method = _METHOD.read_text(encoding="utf-8")
    assert "Prefer iterators over manual indexing" in method
    assert "Validate Python-to-Rust conversions at the FFI boundary" in method


def test_dl_5_left_the_slate_at_departure() -> None:
    """pins: dl-5-contract-compaction/C-012 — the row DL-5 held is gone with its departure.

    C-012 pinned DL-5 at slate row 2 while the unit was in flight. Its departure
    (run at PROC-1's pickup, 2026-08-25, because #243 merged without it) files the
    ledger under ``completed/`` and compacts the row and its reasoning block off
    the slate; the pin turns over with it so the slate cannot be a queue with a
    merged unit still on it (the DL-4 gate rule).
    """
    slate = (_REPO / "briefs/next-sequence.md").read_text(encoding="utf-8")
    assert "**DL-5**" not in slate
    assert "<!-- unit id=dl-5" not in slate
    assert not (_REPO / "task/ledgers/staging/dl-5-contract-compaction-ledger.md").exists()
    filed = _REPO / "task/ledgers/completed/dl-5-contract-compaction-ledger.md"
    archived = list((_REPO / "task/ledgers/archive").glob("*/*dl-5-contract-compaction-ledger.md"))
    assert filed.exists() or archived
