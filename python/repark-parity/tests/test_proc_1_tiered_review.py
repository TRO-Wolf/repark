"""PROC-1: review effort by tier, the MW-6 evidence home, two runbook truth-ups.

Tree pins for the PROC-1 charter clauses. Each test reads a document relative to
the repository root and asserts the load-bearing tokens the clause makes true. The
comment on each test names what silently regresses if the pin is removed — the
process rule these documents carry has no other mechanical guard.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType

_REPO = Path(__file__).resolve().parents[3]
_SCRIPTS = _REPO / "scripts"
_MANIFEST = _REPO / ".agents/skills/sepmo/binding-manifest.md"
_RUNBOOK = _REPO / ".agents/skills/sepmo/unit-runbook.md"
_SEPMO_MAP = _REPO / ".agents/skills/sepmo/map.md"
_SKILLS_MAP = _REPO / ".agents/skills/map.md"
_CLAUDE = _REPO / "CLAUDE.md"
_CCC = _REPO / ".agents/skills/critic-critic-critic/SKILL.md"
_LESSONS = _REPO / "task/lessons.md"
_EVIDENCE = _REPO / "task/mw-6-critic-evidence"
_DISK = _REPO / ".agents/skills/check-disk-headroom/SKILL.md"
_DISK_MAP = _REPO / ".agents/skills/check-disk-headroom/map.md"
_HANDOFF = _REPO / "task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md"


def _load(name: str) -> ModuleType:
    """Load a scripts/ module by file name."""
    specification = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[f"proc1_{name}"] = module
    specification.loader.exec_module(module)
    return module


def _row(text: str, key: str) -> str:
    """The single tunables-table row whose first cell is `key` (backticked)."""
    marker = f"| `{key}` |"
    start = text.index(marker)
    return text[start : text.index("\n", start)]


def test_manifest_carries_the_review_profile_table() -> None:
    """pins: proc-1-tiered-review/C-001 — the tunable that tiers review effort exists.

    WHY: without the row, a unit has no bound rule for how much Critic effort its
    risk warrants, and STANDARD silently reverts to full CCC (the bloat this removes).
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "review_profile")
    for tier in ("**LIGHT**", "**STANDARD**", "**HIGH**"):
        assert tier in row, tier
    assert "risk-tier auto-detect" in row
    assert "riskiest touched path" in row
    # LIGHT is the spine's single in-line AC cycle with a filed attestation (PROC-1 cycle 2) —
    # not "no Critic stage" / "no attestation", which canon forbids (ref 05 constraint 2;
    # SKILL.md Proportionality). The pin reddens on the old wording.
    assert "in-line AC cycle" in row
    assert "no Critic stage" not in row
    assert "no attestation" not in row
    # STANDARD's single-pass checklist is the manifest's lighter review under the spine's own
    # Critic stage — not CCC merging its phases; CCC's absolute rule 1 governs only HIGH.
    assert "not CCC merging its phases" in row
    assert "absolute rule 1" in row


def test_light_thresholds_name_the_prose_only_class() -> None:
    """pins: proc-1-tiered-review/C-002 — a docs-only unit is LIGHT at any line count.

    WHY: if the prose-only class drops, a large but code-free documentation unit is
    forced onto the STANDARD path against proportionality; if the code default drops,
    a code change escapes the line/file caps.
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "light_thresholds")
    for token in ("prose", "map.md", "ledger", "recorded evidence", "tests"):
        assert token in row, token
    assert "whatever its line count" in row
    assert "six spine criteria" in row
    assert "changes code keeps the spine defaults" in row
    assert "150" in row and "5 files" in row


def test_critic_engine_row_binds_ccc_at_high() -> None:
    """pins: proc-1-tiered-review/C-003 — CCC binds at HIGH; STANDARD runs one pass.

    WHY: a silent revert to "CCC for STANDARD-and-above" re-imposes the four-phase
    fan-out on every STANDARD unit; losing the AT mapping or scratch-clone rule loses
    the engine's contract with the spine.
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "critic_engine")
    assert "HIGH" in row
    assert "STANDARD" in row
    assert "2026-08-25" in row  # the ruling date (given 2026-08-25, tonight)
    assert "2026-08-26" not in row  # the tiering ruling was 2026-08-25, not 08-26 (PROC-1 cycle 2)
    assert "scratch" in row
    assert "AT-1..AT-10" in row
    # LIGHT runs the spine's single in-line AC cycle, not "no Critic stage" (ref 05 constraint 2).
    assert "in-line AC cycle" in row
    assert "runs no Critic stage" not in row


def test_standard_pass_states_its_two_obligations() -> None:
    """pins: proc-1-tiered-review/C-004 — the STANDARD pass keeps the two hard duties.

    WHY: drop obligation (a) and a silently-wrong-results change ships with no fresh
    execution through the public door; drop (b) and a pin that never went red is
    accepted as proof. The bar-unchanged list is what a tier may never relax.
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "review_profile")
    assert "novel" in row
    assert "freshly execute" in row or "freshly executed" in row
    assert "public entry point" in row
    assert "s0_fresh_execution" in row
    assert "red before the fix and green after" in row
    assert "mutation probe" in row
    assert "per new guard seat" in row
    for bar in ("green workspace", "every clause pinned", "S1", "R7"):
        assert bar in row, bar


def test_unit_runbook_is_small_and_pointer_only() -> None:
    """pins: proc-1-tiered-review/C-005 — the runbook is small and links, never restates.

    WHY: a runbook that grows past its ceiling or starts restating rules becomes a
    second spine that drifts from the manifest — the exact failure this unit prevents.
    """
    text = _RUNBOOK.read_text(encoding="utf-8")
    assert _RUNBOOK.stat().st_size <= 5_000
    # Pointer-only: every obligation names the home the rule lives in.
    for home in (
        "binding-manifest.md",
        "s0_fresh_execution",
        "check_ledger_grammar",
        "unit-runbook",  # self-name in the pickup/departure pointers is fine
    ):
        assert home in text, home
    # The sections a LIGHT/STANDARD unit reads first.
    for section in ("pickup", "tier", "STANDARD", "departure"):
        assert section in text, section
    # It points at the spine's rules by id rather than restating them.
    assert "R7" in text and "R2" in text
    # Names-and-links, not "restates none of them" (PROC-1 cycle 2 — three lines glossed a
    # threshold/obligation, trimmed; the claim is softened to name-and-link).
    assert "the home is authoritative" in text
    assert "restates none of them" not in text


def test_ceilings_cover_the_runbook() -> None:
    """pins: proc-1-tiered-review/C-005 — the CEILINGS gate holds the runbook at 5,000 B.

    WHY: without the ceiling key the runbook can regrow silently; the gate is what
    makes regrowth into a second spine a red build.
    """
    gate = _load("check_docs_compaction")
    key = ".agents/skills/sepmo/unit-runbook.md"
    assert gate.CEILINGS.get(key) == 5_000
    assert _RUNBOOK.stat().st_size <= gate.CEILINGS[key]
    assert gate.findings(_REPO, gate.CEILINGS) == []


def test_routing_points_at_the_runbook_once_each() -> None:
    """pins: proc-1-tiered-review/C-006 — the three routing homes land on the runbook.

    WHY: a LIGHT/STANDARD unit finds the per-tier checklist first only if each entry
    map points at it; and the profile table stays single-homed only if no router
    restates it.
    """
    assert "unit-runbook.md" in _SEPMO_MAP.read_text(encoding="utf-8")
    assert "run a unit" in _SEPMO_MAP.read_text(encoding="utf-8")
    assert "unit-runbook.md" in _SKILLS_MAP.read_text(encoding="utf-8")
    assert "unit-runbook.md" in _CLAUDE.read_text(encoding="utf-8")
    # The tier table is single-homed in the manifest: no router restates it.
    distinctive = "exactly one Critic pass"
    assert distinctive in _MANIFEST.read_text(encoding="utf-8")
    for other in (_RUNBOOK, _SEPMO_MAP, _SKILLS_MAP, _CLAUDE):
        assert distinctive not in other.read_text(encoding="utf-8"), other.name


def test_ccc_changes_only_its_binding_sentence() -> None:
    """pins: proc-1-tiered-review/C-007 — CCC gains one sentence; the rest is intact.

    WHY: this unit re-times WHEN CCC runs, never what it does. If an absolute rule,
    a risk tier, or a taxonomy changed here, the bound engine's contract drifted.
    """
    text = _CCC.read_text(encoding="utf-8")
    # The one added sentence: bound at HIGH; STANDARD walks the taxonomies as a checklist.
    assert "bound at HIGH" in text
    assert "single-pass checklist" in text
    # The absolute rules, tier table and taxonomies are byte-identical anchors.
    for anchor in (
        "1. **Distinct Critic phases**",
        "13. **Spawn contract**",
        "| **High** |",
        "| **Standard** (default) |",
        "| 4 | **Critic-4 (Claims / Record)**",
    ):
        assert anchor in text, anchor


def test_lessons_record_the_ruling() -> None:
    """pins: proc-1-tiered-review/C-008 — the tiering ruling and its measured reason.

    WHY: a rule with no recorded measurement gets re-litigated; the entry records
    which instruments caught defects and which produced only record.
    """
    text = _LESSONS.read_text(encoding="utf-8")
    # The ruling was given 2026-08-25 (tonight), not 08-26 (PROC-1 cycle 2).
    assert "## 2026-08-25 — PROC-1" in text
    assert "## 2026-08-26" not in text
    section = text[text.index("## 2026-08-25 — PROC-1") :]
    assert "V3R-1" in section  # the fresh-execution catches
    assert "DL-5" in section  # the gate catches
    assert "190 kB" in section  # the STANDARD read had grown to ~190 kB
    assert "**DO" in section  # the DO / DO-NOT form


def test_mw6_evidence_is_home_and_excluded_from_lint() -> None:
    """pins: proc-1-tiered-review/C-009 — the cited evidence lives here, un-linted.

    WHY: the archived MW-6 ledger cites these files by path; if they are absent or
    a linter rewrites them, the ledger's evidence is lost or no longer verbatim.
    """
    for name in (
        "test_critic_shapes.py",
        "test_critic_shapes2.py",
        "test_critic_bytes.py",
        "oracle_critic.py",
        "oracle_critic.log",
        "oracle_k2.py",
        "oracle_k2.log",
        "oracle_r2.py",
        "oracle_r2.log",
        "jar/rmsa.txt",
        "jar/rmp.txt",
    ):
        assert (_EVIDENCE / name).is_file(), name
    assert (_EVIDENCE / "map.md").is_file()
    assert (_EVIDENCE / "jar/map.md").is_file()
    # No local identifiers anywhere under the evidence home (PROC-1 cycle 2 scrub of the Spark
    # start-up banner + the home path): hostname, both IPs, the interface, the home dir, the user.
    # Identifying CLASSES, not literals: the tree must never carry the owner's
    # hostname, a LAN address, an interface name, a real home path or an e-mail —
    # the classes briefs/map.md "Import gate" forbids and the owner-local pre-push
    # hook scans for. Patterns keep the literals themselves out of this file.
    forbidden = (
        re.compile(r"Your hostname, (?!<host>)"),  # Spark's boot banner naming a real host
        re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"),  # any IPv4 address
        re.compile(r"on interface (?!<iface>)[a-z0-9]+"),  # a real NIC name
        re.compile(r"/home/(?!<user>)[A-Za-z0-9_-]+"),  # a real home path
        re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}"),  # an e-mail
    )
    for path in sorted(_EVIDENCE.rglob("*")):
        if not path.is_file():
            continue
        body = path.read_text(encoding="utf-8", errors="replace")
        for pattern in forbidden:
            hit = pattern.search(body)
            assert hit is None, f"{path.relative_to(_EVIDENCE)}: {pattern.pattern}"
    # Excluded from ruff and typos by the literal entry line (not the neighbouring comment alone —
    # deleting the entry must redden this pin), with the reason recorded next to the exclusion.
    pyproject = (_REPO / "pyproject.toml").read_text(encoding="utf-8")
    typos = (_REPO / ".typos.toml").read_text(encoding="utf-8")
    assert 'extend-exclude = ["task/mw-6-critic-evidence"]' in pyproject
    assert '"task/mw-6-critic-evidence/",' in typos
    assert "mw-6-critic-evidence" in (_REPO / "task/map.md").read_text(encoding="utf-8")
    # The archived ledger it explains is untouched (a known citation line still reads).
    archived = _REPO / "task/ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md"
    assert archived.is_file()


def test_disk_runbook_carries_the_2026_08_25_block() -> None:
    """pins: proc-1-tiered-review/C-010 — the disk skill records the 2026-08-25 sweep.

    WHY: a scratch directory once held the only copy of ledger-cited evidence; the
    refute-before-rm rule and the merged-unit reclaim order are what keep the next
    sweep from deleting evidence or expecting a non-owner to run sudo reclaim.
    """
    text = _DISK.read_text(encoding="utf-8")
    assert "2026-08-25" in text
    for token in ("timeshift", "840 G", "207 G", "coredump", "kernel"):
        assert token in text, token
    assert "merged-unit" in text.lower()  # the first reclaim step in §3
    assert "refute" in text  # the scratch-directory Gotcha
    assert "owner-run" in text  # the sudo-tier Gotcha
    # The home directory is neutralised, never the literal path or user name.
    # No real home path and no e-mail in the runbook (classes, not literals — see C-009).
    assert re.search(r"/home/(?!<user>)[A-Za-z0-9_-]+", text) is None
    assert re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", text) is None
    assert "2026-08-25" in _DISK_MAP.read_text(encoding="utf-8")


def test_handoff_f7_records_the_unit_3_ruling() -> None:
    """pins: proc-1-tiered-review/C-011 — F-7 carries the B-MOR-3 / V3-DANGLE-1 ruling.

    WHY: the addendum is the fork lane's record of the 2026-08-25 decision — extend
    R136 to v3, no DV-specific action, the retire-at-repin acceptance. Without it the
    engine's refusal pin has no named successor.
    """
    text = _HANDOFF.read_text(encoding="utf-8")
    section = text[text.index("### F-7") : text.index("### F-8")]
    assert "Addendum 2026-08-25" in section
    assert "B-MOR-3" in section
    assert "R136" in section
    assert "truthful zero" in section
    assert "one DV per data file" in section
    assert "R137" in section
    assert "V3-DANGLE-1" in section
    assert "V3-LINEAGE-1" in section
    assert "F-13" in section
    assert "call_rewrite_position_delete_files_refuses_spark_written_puffin_vectors" in section
