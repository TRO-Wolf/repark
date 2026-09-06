"""SEPMO compact worker packet pins.

pins: sepmo-e2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import sys
from pathlib import Path
from types import ModuleType

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "sepmo_packet.py"
_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sepmo_packets"
_SCHEMA = _REPO / "docs" / "sepmo" / "packets" / "packet.schema.json"
_FORMAT = _REPO / "docs" / "sepmo" / "packets" / "packet-format.md"
_BASELINE = _REPO / "docs" / "sepmo" / "packets" / "baseline.md"
_ADOPTION = _REPO / "docs" / "sepmo" / "packets" / "adoption.md"
_UNITS: tuple[tuple[str, str], ...] = (
    ("ex25", "bc7c76cc"),
    ("cdf1", "e8344e8e"),
    ("icescan", "8f40ce46"),
)
_PREFIX_BRIEFS: tuple[tuple[str, str], ...] = (
    ("ex25", "bc7c76cc"),
    ("cdf1", "e8344e8e"),
    ("icescan", "8f40ce46"),
    ("ex26", "aaaaaaaa"),
    ("types1", "1a0c9f4f"),
)
_RULE_MARKERS: tuple[str, ...] = (
    "No code comments anywhere",
    "Rust `//` `///` `//!`",
    "TOML/YAML/shell `#`",
    "`/// # Errors`",
    "`deny(missing_docs)`",
    "Never delete a pre-existing comment",
    "`pins: <unit>/C-NNN`",
    "git diff --cached | grep",
    "TRO-Wolf",
    "Never push, never `gh`, never `aws`, never `--no-verify`",
    "never touch `.github/`",
    "$HOME/.claude/",
    "No home paths in the tree",
    "Never change dependencies or `Cargo.lock`",
    "make bump-fork-pin",
    "Three cargo builders",
    "maturin develop --release",
    "__debug_assertions__",
    "Live PySpark",
    "--extra record",
    "PYSPARK_SUBMIT_ARGS",
    "JAVA_HOME",
    "ANSI on",
    "Size ceilings only ratchet down",
    "test_cap_1_source_file_line_cap.py",
    "1000 lines",
    "Do not change what any gate requires",
    "`map.md` in every directory",
    "No amends",
    "frozen",
)
_E0_USAGE: dict[str, tuple[int, int, int]] = {
    "ex25": (269827, 24171581, 95642),
    "cdf1": (689231, 75010034, 194648),
    "icescan": (1106400, 122053590, 302942),
}
_HOME_DIR = re.compile(r"/(?:home|Users)/[A-Za-z0-9._-]+")


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("sepmo_packet", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _packet_paths(unit: str) -> tuple[Path, Path, Path]:
    return (
        _FIXTURES / f"brief-{unit}.md",
        _FIXTURES / f"{unit}-actor.md",
        _FIXTURES / f"{unit}-actor.json",
    )


def test_schema_file_lists_eight_groups_and_source_identity() -> None:
    """The schema names packet_version, eight groups, and source identity.

    pins: sepmo-e2/C-001
    """
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    required = set(schema["required"])
    expected = {
        "packet_version",
        "identity",
        "source_identity",
        "authority",
        "scope",
        "implementation_context",
        "verification",
        "permissions_and_resources",
        "handoff",
        "stable_prefix",
        "dynamic_markdown",
    }
    assert required == expected
    assert schema["properties"]["packet_version"]["const"] == "1"
    source = schema["properties"]["source_identity"]["required"]
    assert "repository" in source
    assert "base_revision" in source
    assert "brief_hash" in source
    identity = schema["properties"]["identity"]["properties"]
    assert identity["role"]["enum"] == ["actor", "critic"]
    assert identity["adapter"]["enum"] == ["muse", "grok", "glm", "opus"]
    format_text = _FORMAT.read_text(encoding="utf-8")
    assert "packet_version" in format_text
    assert "Stable prefix" in format_text
    for group in (
        "Identity",
        "Source identity",
        "Authority",
        "Scope",
        "Implementation context",
        "Verification",
        "Permissions and resources",
        "Handoff",
    ):
        assert group in format_text


def test_fixture_packets_validate_against_schema() -> None:
    """Each checked-in JSON sidecar passes the schema and check.

    pins: sepmo-e2/C-001, C-002
    """
    module = _load()
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    for unit, _base in _UNITS:
        _brief, markdown_path, json_path = _packet_paths(unit)
        packet = json.loads(json_path.read_text(encoding="utf-8"))
        markdown = markdown_path.read_text(encoding="utf-8")
        findings = module.validate_schema(packet, schema)
        assert findings == [], findings
        check = module.check_packet(packet, markdown, brief_text=_brief.read_text())
        assert check == [], check
        assert markdown.startswith(module.STABLE_PREFIX.rstrip("\n"))
        assert "\n## Dynamic\n" in markdown


def test_prefix_is_byte_identical_across_three_units() -> None:
    """The three converted packets share one stable prefix.

    pins: sepmo-e2/C-003
    """
    module = _load()
    prefixes = []
    for unit, _base in _UNITS:
        markdown = (_FIXTURES / f"{unit}-actor.md").read_text(encoding="utf-8")
        prefix = module.prefix_of(markdown)
        prefixes.append(prefix)
        assert prefix == module.STABLE_PREFIX
        for marker in _RULE_MARKERS:
            assert marker in prefix, marker
        for rule in module.STABLE_RULES:
            assert rule in prefix
    assert prefixes[0] == prefixes[1] == prefixes[2]


def test_prefix_is_byte_identical_across_five_briefs() -> None:
    """Building five campaign briefs yields one prefix.

    pins: sepmo-e2/C-003
    """
    module = _load()
    prefixes = []
    for unit, base in _PREFIX_BRIEFS:
        brief = (_FIXTURES / f"brief-{unit}.md").read_text(encoding="utf-8")
        packet = module.build_packet(unit, "actor", base, brief, "muse")
        markdown = module.render_markdown(packet)
        prefix = module.prefix_of(markdown)
        prefixes.append(prefix)
        assert prefix == module.STABLE_PREFIX
    assert len(set(prefixes)) == 1


def test_dropping_a_stable_rule_fails_check(tmp_path: Path) -> None:
    """A mutation that drops a stable rule is detected.

    pins: sepmo-e2/C-003
    """
    module = _load()
    source_md = _FIXTURES / "ex25-actor.md"
    source_json = _FIXTURES / "ex25-actor.json"
    mutated_md = tmp_path / "ex25-actor.md"
    mutated_json = tmp_path / "ex25-actor.json"
    dropped = "Never push, never `gh`, never `aws`, never `--no-verify`"
    text = source_md.read_text(encoding="utf-8").replace(dropped, "never `gh`")
    mutated_md.write_text(text, encoding="utf-8")
    shutil.copy(source_json, mutated_json)
    packet, markdown = module.load_packet_files(mutated_md)
    findings = module.check_packet(packet, markdown)
    assert findings, "dropped rule must be detected"
    joined = " ".join(findings)
    assert "stable prefix" in joined or "missing stable rule" in joined
    assert module.main(["check", str(mutated_md)]) == 1


def test_diff_shows_only_the_dynamic_delta() -> None:
    """diff of two packets names units and does not rewrite the prefix.

    pins: sepmo-e2/C-004
    """
    module = _load()
    packet_a, _md_a = module.load_packet_files(_FIXTURES / "ex25-actor.md")
    packet_b, _md_b = module.load_packet_files(_FIXTURES / "cdf1-actor.md")
    rendered = module.diff_packets(packet_a, packet_b)
    assert rendered.startswith("--- ex25 dynamic")
    assert "+++ cdf1 dynamic" in rendered
    assert "-- unit: ex25" in rendered
    assert "+- unit: cdf1" in rendered
    assert "Never push, never `gh`" not in rendered
    assert "## Stable prefix" not in rendered
    diff_args = [
        "diff",
        str(_FIXTURES / "ex25-actor.md"),
        str(_FIXTURES / "cdf1-actor.md"),
    ]
    assert module.main(diff_args) == 0


def test_fixtures_have_no_home_paths() -> None:
    """Converted briefs and packets contain no /home/ or /Users/ paths.

    pins: sepmo-e2/C-005
    """
    for path in sorted(_FIXTURES.iterdir()):
        if not path.is_file() or path.name == "map.md":
            continue
        text = path.read_text(encoding="utf-8")
        match = _HOME_DIR.search(text)
        assert match is None, f"{path.name} contains {match.group(0) if match else ''}"


def test_rebuild_matches_checked_in_packets(tmp_path: Path) -> None:
    """build from the fixture briefs reproduces the checked-in packets.

    pins: sepmo-e2/C-002, C-005
    """
    module = _load()
    for unit, base in _UNITS:
        brief, markdown_path, json_path = _packet_paths(unit)
        packet = module.build_packet(
            unit=unit,
            role="actor",
            base_revision=base,
            brief_text=brief.read_text(encoding="utf-8"),
            adapter="muse",
        )
        markdown = module.render_markdown(packet)
        assert markdown == markdown_path.read_text(encoding="utf-8")
        assert module.dump_json(packet) == json_path.read_text(encoding="utf-8")
        out_dir = tmp_path / unit
        assert (
            module.main(
                [
                    "build",
                    "--unit",
                    unit,
                    "--role",
                    "actor",
                    "--base",
                    base,
                    "--brief",
                    str(brief),
                    "--adapter",
                    "muse",
                    "--out-dir",
                    str(out_dir),
                ]
            )
            == 0
        )
        assert (out_dir / f"{unit}-actor.md").read_text(
            encoding="utf-8"
        ) == markdown_path.read_text(encoding="utf-8")


def test_critic_packet_keeps_prefix_and_excludes_actor_narrative() -> None:
    """A critic packet shares the prefix and excludes the Actor self-review.

    pins: sepmo-e2/C-002, C-003
    """
    module = _load()
    brief = (_FIXTURES / "brief-ex25.md").read_text(encoding="utf-8")
    actor = module.build_packet("ex25", "actor", "bc7c76cc", brief, "muse")
    critic = module.build_packet("ex25", "critic", "bc7c76cc", brief, "muse")
    assert (
        module.render_markdown(actor).split("\n## Dynamic\n", 1)[0]
        == (module.render_markdown(critic).split("\n## Dynamic\n", 1)[0])
    )
    assert "Actor Self Logic Review" in critic["scope"]["exclusions"]
    assert "Actor narrative" in critic["scope"]["exclusions"]
    assert "findings" in critic["handoff"]["expected_output_fields"]
    assert "commits" in actor["handoff"]["expected_output_fields"]
    assert "This is a Critic packet" in critic["dynamic_markdown"]
    assert "This is a Critic packet" not in actor["dynamic_markdown"]


def test_brief_hash_refresh_is_detected(tmp_path: Path) -> None:
    """check --brief fails when the brief bytes no longer match brief_hash.

    pins: sepmo-e2/C-008
    """
    module = _load()
    brief = _FIXTURES / "brief-ex25.md"
    changed = tmp_path / "brief-ex25.md"
    changed.write_text(brief.read_text(encoding="utf-8") + "\nextra\n", encoding="utf-8")
    packet, markdown = module.load_packet_files(_FIXTURES / "ex25-actor.md")
    findings = module.check_packet(packet, markdown, brief_text=changed.read_text(encoding="utf-8"))
    assert any("brief hash mismatch" in item for item in findings)
    assert module.main(["check", str(_FIXTURES / "ex25-actor.md"), "--brief", str(changed)]) == 1
    assert module.main(["check", str(_FIXTURES / "ex25-actor.md"), "--brief", str(brief)]) == 0


def test_baseline_table_matches_fixture_sizes_and_e0_ratios() -> None:
    """Baseline sizes match the fixtures; E-0 ratios are recorded; no savings claim.

    pins: sepmo-e2/C-006
    """
    module = _load()
    baseline = _BASELINE.read_text(encoding="utf-8")
    assert "does **not** claim token savings" in baseline
    assert "token savings" in baseline
    assert "E-4" in baseline
    for unit, _base in _UNITS:
        brief = (_FIXTURES / f"brief-{unit}.md").read_text(encoding="utf-8")
        markdown = (_FIXTURES / f"{unit}-actor.md").read_text(encoding="utf-8")
        sizes = module.packet_sizes(markdown)
        orig_bytes = len(brief.encode("utf-8"))
        orig_words = len(brief.split())
        assert str(orig_bytes) in baseline
        assert str(orig_words) in baseline
        assert str(sizes["prefix_bytes"]) in baseline
        assert str(sizes["dynamic_bytes"]) in baseline
        tokens_in, tokens_cached, tokens_out = _E0_USAGE[unit]
        assert str(tokens_in) in baseline
        assert str(tokens_cached) in baseline
        assert str(tokens_out) in baseline
        ratio = f"{tokens_cached / tokens_in:.1f}"
        assert ratio in baseline
    assert sizes["prefix_bytes"] == 2756
    assert "10007936" in baseline


def test_adoption_names_each_adapter_prompt_file() -> None:
    """Adoption names --brief/--followup and matches wrapper text when present.

    pins: sepmo-e2/C-007
    """
    text = _ADOPTION.read_text(encoding="utf-8")
    assert "--brief" in text
    assert "--followup" in text
    assert "muse exec" in text
    assert "opencode run" in text
    assert "Claude sub-agents" in text
    assert "does not edit wrappers" in text.lower() or "does not edit wrappers" in text
    assert "persona" in text.lower()
    grok_wrapper = _skill_script("grok-worker", "grok-worker.sh")
    if grok_wrapper is not None:
        wrapper = grok_wrapper.read_text(encoding="utf-8")
        assert "--brief FILE" in wrapper
        assert "--followup FILE" in wrapper
        assert "prompt=${followup:-$brief}" in wrapper
        assert "--brief" in text
        assert "archive copy" in text.lower() or "not the input" in text.lower()
    muse_wrapper = _skill_script("muse-worker", "muse-worker.sh")
    if muse_wrapper is not None:
        wrapper = muse_wrapper.read_text(encoding="utf-8")
        assert "--brief FILE" in wrapper
        assert 'cat "$persona"' in wrapper or "persona" in wrapper
        assert "GENERATES" in text or "generates" in text.lower()


@pytest.mark.parametrize(
    "raw",
    (
        "https://example.invalid/brief.md",
        "s3://bucket/brief.md",
        "file:///etc/passwd",
    ),
)
def test_remote_url_is_rejected(raw: str) -> None:
    """build and check refuse a remote path.

    pins: sepmo-e2/C-002
    """
    module = _load()
    with pytest.raises(module.PacketError, match="refusing a remote path"):
        module._reject_remote(raw)
    assert module.main(["check", raw]) == 1


def test_malformed_sidecar_fails_loudly(tmp_path: Path) -> None:
    """A truncated JSON sidecar is a check failure, not a partial packet.

    pins: sepmo-e2/C-002
    """
    module = _load()
    markdown = tmp_path / "broken.md"
    sidecar = tmp_path / "broken.json"
    markdown.write_text((_FIXTURES / "ex25-actor.md").read_text(encoding="utf-8"))
    sidecar.write_text("{not json", encoding="utf-8")
    with pytest.raises(module.PacketError, match="malformed JSON sidecar"):
        module.load_packet_files(markdown)
    assert module.main(["check", str(markdown)]) == 1


def _skill_script(skill: str, script: str) -> Path | None:
    roots = [
        Path.home() / ".agents" / "skills",
        Path.home() / ".grok" / "skills",
        Path.home() / ".claude" / "skills",
    ]
    for root in roots:
        path = root / skill / script
        if path.is_file():
            return path
    return None


def test_wrong_trailer_fails_check(tmp_path: Path) -> None:
    """Rewriting the rendered trailer to a co-authorship trailer fails check.

    pins: sepmo-e2/C-002
    """
    module = _load()
    source_md = _FIXTURES / "ex25-actor.md"
    source_json = _FIXTURES / "ex25-actor.json"
    mutated_md = tmp_path / "ex25-actor.md"
    mutated_json = tmp_path / "ex25-actor.json"
    expected = module.AUTHORED_BY["muse"]
    forged = "Co-" + "Authored-By: Claude <noreply@example.invalid>"
    mutated_md.write_text(
        source_md.read_text(encoding="utf-8").replace(expected, forged),
        encoding="utf-8",
    )
    mutated_json.write_text(
        source_json.read_text(encoding="utf-8").replace(expected, forged),
        encoding="utf-8",
    )
    packet, markdown = module.load_packet_files(mutated_md)
    findings = module.check_packet(packet, markdown)
    joined = " ".join(findings)
    assert findings, "forged trailer must be detected"
    assert "trailer" in joined.lower() or ("Co-" + "Authored-By") in joined
    assert module.main(["check", str(mutated_md)]) == 1


def test_sidecar_base_revision_mismatch_fails_check(tmp_path: Path) -> None:
    """A JSON-only base_revision rewrite fails the re-render check.

    pins: sepmo-e2/C-002
    """
    module = _load()
    source_md = _FIXTURES / "ex25-actor.md"
    source_json = _FIXTURES / "ex25-actor.json"
    mutated_md = tmp_path / "ex25-actor.md"
    mutated_json = tmp_path / "ex25-actor.json"
    shutil.copy(source_md, mutated_md)
    payload = json.loads(source_json.read_text(encoding="utf-8"))
    payload["source_identity"]["base_revision"] = "0000000"
    mutated_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    packet, markdown = module.load_packet_files(mutated_md)
    findings = module.check_packet(packet, markdown)
    joined = " ".join(findings)
    assert findings, "json-only base_revision rewrite must be detected"
    assert "re-render" in joined or "dynamic" in joined
    assert module.main(["check", str(mutated_md)]) == 1


def test_verification_commands_are_shell_and_not_prose() -> None:
    """Every commands[] entry is a shell command: bash -n, no backticks.

    pins: sepmo-e2/C-002
    """
    module = _load()
    for unit, _base in _UNITS:
        packet, _markdown = module.load_packet_files(_FIXTURES / f"{unit}-actor.md")
        commands = packet["verification"]["commands"]
        assert commands, unit
        for command in commands:
            assert "`" not in command, command
            assert module.looks_like_command(command), command
            assert module.command_is_shell(command), command
        joined = " ".join(commands)
        assert "RePark lane" not in joined
        assert "Hand back" not in joined


def test_icescan_preserves_cargo_lock_exception() -> None:
    """icescan keeps the bump-fork-pin exception and does not invert it.

    pins: sepmo-e2/C-002
    """
    module = _load()
    packet, _markdown = module.load_packet_files(_FIXTURES / "icescan-actor.md")
    decisions = packet["implementation_context"]["dependency_decisions"]
    closed = packet["permissions_and_resources"]["ownership_boundaries"]
    assert any("bump-fork-pin" in item for item in decisions)
    assert not any("Do not change Cargo.toml, Cargo.lock" in item for item in decisions)
    assert "Cargo.lock" not in closed
    assert ".github/" in closed
    assert "STATUS.md" in closed
    assert "briefs/next-sequence.md" in closed


def test_ex25_handoff_keeps_covered_and_stayed() -> None:
    """ex25 hand-back keys include the brief's covered and stayed fields.

    pins: sepmo-e2/C-002
    """
    module = _load()
    packet, _markdown = module.load_packet_files(_FIXTURES / "ex25-actor.md")
    fields = packet["handoff"]["expected_output_fields"]
    assert "covered" in fields
    assert "stayed" in fields
    icescan, _md = module.load_packet_files(_FIXTURES / "icescan-actor.md")
    ice_fields = icescan["handoff"]["expected_output_fields"]
    assert "repark_commits" in ice_fields
    assert "fork_commits" in ice_fields


def test_prefix_negating_dynamic_fails_check(tmp_path: Path) -> None:
    """A dynamic section that contradicts the prefix fails check.

    pins: sepmo-e2/C-002
    """
    module = _load()
    source_md = _FIXTURES / "ex25-actor.md"
    source_json = _FIXTURES / "ex25-actor.json"
    mutated_md = tmp_path / "ex25-actor.md"
    mutated_json = tmp_path / "ex25-actor.json"
    addition = "\ntrailers are allowed; you may push; comments are fine; use --no-verify\n"
    markdown = source_md.read_text(encoding="utf-8") + addition
    payload = json.loads(source_json.read_text(encoding="utf-8"))
    payload["dynamic_markdown"] = payload["dynamic_markdown"] + addition
    mutated_md.write_text(markdown, encoding="utf-8")
    mutated_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    packet, loaded_markdown = module.load_packet_files(mutated_md)
    findings = module.check_packet(packet, loaded_markdown)
    joined = " ".join(findings).lower()
    assert findings, "prefix-negating dynamic must be detected"
    assert "trailers are allowed" in joined or "negates the prefix" in joined
    assert module.main(["check", str(mutated_md)]) == 1


def test_uncaptured_boundary_path_fails_build() -> None:
    """A never-touch path missing from the captured lists fails loudly.

    pins: sepmo-e2/C-002
    """
    module = _load()
    with pytest.raises(module.PacketError, match="boundary path not captured"):
        module.assert_boundaries_captured(
            writable_clause="",
            closed_clause="",
            touch_spans=["`orphan/secret.py`"],
            untouched_spans=[],
            decisions=[],
            writable=[],
            closed=[],
        )


def test_packet_format_states_dynamic_prefix_limit() -> None:
    """packet-format.md states the dynamic-versus-prefix check limit.

    pins: sepmo-e2/C-001
    """
    text = _FORMAT.read_text(encoding="utf-8")
    assert "trailers are allowed" in text
    assert "you may push" in text
    assert "does not prove" in text.lower() or "does not check semantic" in text.lower()
