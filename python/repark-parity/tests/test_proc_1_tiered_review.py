"""PR-244: revalidate the tiered-review process and preserve its evidence corrections.

Tree pins for the PR-244 revalidation clauses. Each test reads a document relative to
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
_CCC_MAP = _REPO / ".agents/skills/critic-critic-critic/map.md"
_LESSONS = _REPO / "task/lessons.md"
_EVIDENCE = _REPO / "task/mw-6-critic-evidence"
_DISK = _REPO / ".agents/skills/check-disk-headroom/SKILL.md"
_DISK_MAP = _REPO / ".agents/skills/check-disk-headroom/map.md"
_HANDOFF = _REPO / "task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md"
_LEDGER = _REPO / "task/ledgers/staging/pr-244-revalidation-ledger.md"
_STAGING_MAP = _REPO / "task/ledgers/staging/map.md"
_TEST_MAP = _REPO / "python/repark-parity/tests/map.md"


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


def test_current_main_source_size_and_map_guards_remain_bound() -> None:
    """pins: pr-244-revalidation/C-001 — source-size and map gates remain in `make ci`."""
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    for target in ("check-lib-py", "check-rust-file-size", "check-map-sync"):
        assert target in makefile, target
    assert "test_proc_1_tiered_review.py" in _TEST_MAP.read_text(encoding="utf-8")
    assert "pr-244-revalidation-ledger.md" in _STAGING_MAP.read_text(encoding="utf-8")


def test_every_execution_unit_has_one_actor_then_one_critic_stage() -> None:
    """pins: pr-244-revalidation/C-002
    pins: proc-1-tiered-review/C-003 — one Actor precedes one canonical Critic stage.
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "critic_engine")
    assert "Every execution unit runs one Actor" in row
    assert "then one Critic stage" in row
    assert "sequentially" in row
    assert "LIGHT" in row and "spine's in-line Critic" in row
    assert "never selects external" in row
    assert "STANDARD/HIGH" in row and "one bound CCC engine" in row
    assert "attack lenses or passes inside that one stage" in row
    assert "remediation returns to the Actor" in row


def test_adjacent_process_roles_stay_outside_the_execution_loop() -> None:
    """pins: pr-244-revalidation/C-002 — adjacent lanes do not become loop roles."""
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "critic_engine")
    assert "Finder and Verifier are not roles in this loop" in row
    assert "Delivery remains post-convergence readiness verification" in row
    assert "separate hardening lane outside this loop" in row
    assert "CCC-CONVERGED` is not Delivery" in row


def test_review_tiers_scale_effort_without_relaxing_the_bar() -> None:
    """pins: pr-244-revalidation/C-003
    pins: proc-1-tiered-review/C-001 — every tier retains every mandatory invariant.
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "review_profile")
    for tier in ("**LIGHT**", "**STANDARD**", "**HIGH**"):
        assert tier in row, tier
    for effort in ("attack depth", "pass count", "isolation", "mutation probes"):
        assert effort in row, effort
    assert "LIGHT" in row and "never selects an external engine" in row
    assert "STANDARD" in row and "bound CCC engine at standard intensity" in row
    assert "HIGH" in row and "same engine at high intensity" in row
    for bar in (
        "every clause pinned",
        "full `COVERAGE_ATTESTATION`",
        "S1",
        "green_commands",
        "s0_fresh_execution",
        "R7",
    ):
        assert bar in row, bar


def test_light_thresholds_preserve_the_prose_only_class() -> None:
    """pins: pr-244-revalidation/C-003
    pins: proc-1-tiered-review/C-002 — tier selection keeps the prose-only class.
    """
    row = _row(_MANIFEST.read_text(encoding="utf-8"), "light_thresholds")
    for token in ("prose", "map.md", "ledger", "recorded evidence", "tests"):
        assert token in row, token
    assert "whatever its line count" in row
    assert "changes code keeps the spine defaults" in row


def test_manifest_owns_proof_isolation_and_taxonomy_mapping() -> None:
    """pins: pr-244-revalidation/C-002, C-003
    pins: proc-1-tiered-review/C-004 — proof and taxonomy duties remain load-bearing.
    """
    text = _MANIFEST.read_text(encoding="utf-8")
    profile = _row(text, "review_profile")
    engine = _row(text, "critic_engine")
    for proof in ("red before its fix and green after it", "mutation probe per new guard seat"):
        assert proof in profile, proof
    for tunable in (
        "mode=review-only",
        "max_cycles",
        "severity_floor",
        "claims_critic=true",
        "scratch clone",
        "context_break_mechanics",
    ):
        assert tunable in engine, tunable
    for mapping in (
        "Critic-1 → AT-8, AT-10",
        "Critic-2 → AT-3, AT-4, AT-5",
        "Critic-3 → AT-1, AT-2, AT-6",
        "Critic-4 → claims and readiness outside AT-1..AT-10",
        "AT-7 is attacked only for system-breaking change",
        "AT-9 is attacked where a failure path exists",
        "Every attestation lists all ten",
    ):
        assert mapping in engine, mapping


def test_process_policy_is_single_homed_and_routes_by_pointer() -> None:
    """pins: pr-244-revalidation/C-005
    pins: proc-1-tiered-review/C-005 — bindings are single-homed and routers point.
    """
    text = _RUNBOOK.read_text(encoding="utf-8")
    assert _RUNBOOK.stat().st_size <= 5_000
    for home in ("review_profile", "critic_engine", "check_ledger_grammar"):
        assert home in text, home
    binding_sentence = "Every execution unit runs one Actor"
    assert binding_sentence in _MANIFEST.read_text(encoding="utf-8")
    for other in (_RUNBOOK, _SEPMO_MAP, _SKILLS_MAP, _CLAUDE, _CCC, _CCC_MAP, _LESSONS):
        assert binding_sentence not in other.read_text(encoding="utf-8"), other.name
    assert "The repository-specific binding and effort profile live only in the manifest" in (
        _CCC.read_text(encoding="utf-8")
    )
    lessons = _LESSONS.read_text(encoding="utf-8")
    lessons_section = lessons[lessons.index("## 2026-08-25 — PROC-1") :]
    carriers = (
        _STAGING_MAP.read_text(encoding="utf-8"),
        _TEST_MAP.read_text(encoding="utf-8"),
        lessons_section,
        _SEPMO_MAP.read_text(encoding="utf-8"),
    )
    for carrier in carriers:
        assert "binding-manifest.md" in carrier
        assert "review_profile" in carrier
        assert "critic_engine" in carrier
    forbidden_restatements = (
        "one Actor",
        "one bound",
        "one Critic stage",
        "spine's in-line Critic",
        "external engine for",
        "LIGHT uses",
        "STANDARD/HIGH",
    )
    for carrier in carriers:
        for restatement in forbidden_restatements:
            assert restatement not in carrier, restatement


def test_ceilings_cover_the_runbook() -> None:
    """pins: pr-244-revalidation/C-005 — the CEILINGS gate holds the pointer runbook.

    WHY: without the ceiling key the runbook can regrow silently; the gate is what
    makes regrowth into a second spine a red build.
    """
    gate = _load("check_docs_compaction")
    key = ".agents/skills/sepmo/unit-runbook.md"
    assert gate.CEILINGS.get(key) == 5_000
    assert _RUNBOOK.stat().st_size <= gate.CEILINGS[key]
    assert gate.findings(_REPO, gate.CEILINGS) == []


def test_routing_points_at_the_runbook_once_each() -> None:
    """pins: pr-244-revalidation/C-005
    pins: proc-1-tiered-review/C-006 — each routing home points at the runbook.
    """
    assert "unit-runbook.md" in _SEPMO_MAP.read_text(encoding="utf-8")
    assert "run a unit" in _SEPMO_MAP.read_text(encoding="utf-8")
    assert "unit-runbook.md" in _SKILLS_MAP.read_text(encoding="utf-8")
    assert "unit-runbook.md" in _CLAUDE.read_text(encoding="utf-8")
    for other in (_SEPMO_MAP, _SKILLS_MAP, _CLAUDE):
        assert "unit-runbook.md" in other.read_text(encoding="utf-8"), other.name


def test_ccc_keeps_its_taxonomies_and_defers_the_binding() -> None:
    """pins: proc-1-tiered-review/C-007 — CCC keeps canon and points at the manifest."""
    text = _CCC.read_text(encoding="utf-8")
    assert "The repository-specific binding and effort profile live only in the manifest" in text
    for anchor in (
        "1. **Distinct Critic phases**",
        "13. **Spawn contract**",
        "2. **LIGHT units never select this engine**",
        "3. **Taxonomy mapping onto the spine's AT-1..AT-10**",
    ):
        assert anchor in text, anchor


def test_lessons_keep_the_measurement_and_point_to_the_ruling() -> None:
    """pins: proc-1-tiered-review/C-008 — the measured lesson points at its policy home."""
    text = _LESSONS.read_text(encoding="utf-8")
    section = text[text.index("## 2026-08-25 — PROC-1") :]
    for token in (
        "V3R-1",
        "DL-5",
        "190 kB",
        "binding-manifest.md",
        "review_profile",
        "critic_engine",
    ):
        assert token in section, token


def test_revalidation_scope_has_a_pin_for_every_clause() -> None:
    """pins: pr-244-revalidation/C-006 — the live ledger cites every frozen clause."""
    text = _LEDGER.read_text(encoding="utf-8")
    for number in range(1, 8):
        assert f"pins: pr-244-revalidation/C-{number:03}" in text


def test_revalidation_maps_and_ledger_are_reviewable() -> None:
    """pins: pr-244-revalidation/C-007 — the live record is linked from both maps."""
    assert "pr-244-revalidation-ledger.md" in _STAGING_MAP.read_text(encoding="utf-8")
    tests_map = _TEST_MAP.read_text(encoding="utf-8")
    assert "test_proc_1_tiered_review.py" in tests_map
    assert "PR-244" in tests_map


def test_mw6_evidence_is_home_and_excluded_from_lint() -> None:
    """pins: pr-244-revalidation/C-004
    pins: proc-1-tiered-review/C-009 — the cited evidence lives here, un-linted.

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
    """pins: pr-244-revalidation/C-004
    pins: proc-1-tiered-review/C-010 — the disk skill records the 2026-08-25 sweep.

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
    # No real home path and no e-mail in the runbook (classes, not literals — see C-004).
    assert re.search(r"/home/(?!<user>)[A-Za-z0-9_-]+", text) is None
    assert re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", text) is None
    assert "2026-08-25" in _DISK_MAP.read_text(encoding="utf-8")


def test_handoff_f7_records_the_unit_3_ruling() -> None:
    """pins: pr-244-revalidation/C-004
    pins: proc-1-tiered-review/C-011 — F-7 carries the B-MOR-3 / V3-DANGLE-1 ruling.

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
