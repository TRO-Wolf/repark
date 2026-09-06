#!/usr/bin/env python3
"""Assemble, check, and diff SEPMO compact worker packets."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from difflib import unified_diff
from pathlib import Path
from types import ModuleType
from typing import Any

PACKET_VERSION: str = "1"
ADAPTERS: tuple[str, ...] = ("muse", "grok", "glm", "opus")
ROLES: tuple[str, ...] = ("actor", "critic")
REPOSITORY: str = "repark"
BINDING_VERSION: str = "v2.3"
CONTRACT: str = "AGENTS.md"
SOURCE_REFERENCES: tuple[str, ...] = (
    "AGENTS.md",
    "docs/testing.md",
    ".agents/skills/sepmo/binding-manifest.md",
    ".agents/skills/sepmo/unit-runbook.md",
)
AUTHORED_BY: dict[str, str] = {
    "muse": "Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>",
    "grok": "Authored-By: Grok (grok-4.6) <noreply@x.ai>",
    "glm": "Authored-By: GLM (glm-5.3-flash)",
    "opus": "Authored-By: Claude Opus",
}
REMOTE_SCHEME: re.Pattern[str] = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]+:/")
UNIT_SAFE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._-]+$")
SHA_SAFE: re.Pattern[str] = re.compile(r"^[0-9a-f]{7,40}$")
DYNAMIC_SPLIT: str = "\n## Dynamic\n"
SCHEMA_RELATIVE: str = "docs/sepmo/packets/packet.schema.json"

STABLE_RULES: tuple[str, ...] = (
    "No code comments anywhere: Python `#` beyond `# noqa`; Rust `//` `///` "
    "`//!`; TOML/YAML/shell `#`; docstrings one line where "
    "`check-docstring-presence` demands.",
    "The only forced exception is one `/// # Errors` line clippy demands on a "
    "`pub fn` returning `Result`.",
    "On a new fork file the ASF header plus one `///` where the fork's "
    "`deny(missing_docs)` forces it on a new `pub` item.",
    "Never delete a pre-existing comment.",
    "Reasons live in `map.md`s (`pins: <unit>/C-NNN`) and the ledger.",
    "Self-check before every commit: `git diff --cached | grep -nE "
    "'^\\+.*(//|#)' -- '*.rs' '*.py' '*.toml' | grep -v '#\\[' | grep -v noqa` "
    "prints nothing beyond the forced lines.",
    'Identity `git -c user.name="TRO-Wolf" -c '
    "user.email=64240326+TRO-Wolf@users.noreply.github.com`.",
    "The LAST line of every commit message is the adapter Authored-By trailer "
    "named in the packet identity; no other trailer.",
    "Never push, never `gh`, never `aws`, never `--no-verify`, never touch `.github/`.",
    "Never edit `$HOME/.claude/`. Wrapper patches live under "
    "`docs/sepmo/telemetry/wrapper-patches/`.",
    "No home paths in the tree.",
    "Never change dependencies or `Cargo.lock` unless the packet names a "
    "sanctioned writer (`make bump-fork-pin`).",
    "Three cargo builders is the box cap. One cargo invocation at a time, in "
    "one lane at a time, unless the packet names a different bound.",
    "Numbers and native-module checks need `maturin develop --release`. Prove "
    "`__debug_assertions__` is False and the path is under the lane. Record "
    "load; keep the box quiet.",
    "Live PySpark is the oracle when a unit asserts Spark values. Provision "
    "with `--extra record` as `make parity-live` does.",
    "One JVM beside at most one other. Wait for an empty box (60 s poll, up to 15 min).",
    "Redirect ivy into the lane (`cp -a ~/.ivy2.5.2 <lane>/.ivy2` + "
    '`PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=<lane>/.ivy2 pyspark-shell"`). '
    "`JAVA_HOME` is Java 17. ANSI on; UTC session zone unless the case is "
    "about zones. Kill what you start.",
    "Size ceilings only ratchet down. A new Python file stays under 1000 lines.",
    "Every ratchet in `scripts/check_lib_py.py` is mirrored in "
    "`python/repark-parity/tests/test_cap_1_source_file_line_cap.py` in the "
    "same commit.",
    "Do not raise a gate baseline without owner approval. Do not change what any gate requires.",
    "`map.md` in every directory, updated in the same change. A new directory "
    "gets a new `map.md`. No amends.",
    "A ledger in `completed/` or `archive/` is frozen. Do not amend it.",
)

STABLE_PREFIX: str = (
    "# SEPMO compact worker packet\n"
    "\n"
    "packet_version: 1\n"
    "\n"
    "## Stable prefix\n"
    "\n"
    "These rules never change between units. A packet that omits any of them "
    "is invalid.\n"
    "\n"
    "### Comment ban\n"
    "No code comments anywhere: Python `#` beyond `# noqa`; Rust `//` `///` "
    "`//!`; TOML/YAML/shell `#`; docstrings one line where "
    "`check-docstring-presence` demands. The only forced exception is one "
    "`/// # Errors` line clippy demands on a `pub fn` returning `Result`. On "
    "a new fork file the ASF header plus one `///` where the fork's "
    "`deny(missing_docs)` forces it on a new `pub` item. Never delete a "
    "pre-existing comment. Reasons live in `map.md`s (`pins: <unit>/C-NNN`) "
    "and the ledger. Self-check before every commit: `git diff --cached | grep "
    "-nE '^\\+.*(//|#)' -- '*.rs' '*.py' '*.toml' | grep -v '#\\[' | grep -v "
    "noqa` prints nothing beyond the forced lines.\n"
    "\n"
    "### Identity and trailer\n"
    'Identity `git -c user.name="TRO-Wolf" -c '
    "user.email=64240326+TRO-Wolf@users.noreply.github.com`. The LAST line of "
    "every commit message is the adapter Authored-By trailer named in the "
    "packet identity; no other trailer.\n"
    "\n"
    "### Prohibitions\n"
    "Never push, never `gh`, never `aws`, never `--no-verify`, never touch "
    "`.github/`. Never edit `$HOME/.claude/`. Wrapper patches live under "
    "`docs/sepmo/telemetry/wrapper-patches/`. No home paths in the tree. Never "
    "change dependencies or `Cargo.lock` unless the packet names a sanctioned "
    "writer (`make bump-fork-pin`).\n"
    "\n"
    "### Cargo cap\n"
    "Three cargo builders is the box cap. One cargo invocation at a time, in "
    "one lane at a time, unless the packet names a different bound.\n"
    "\n"
    "### Release module\n"
    "Numbers and native-module checks need `maturin develop --release`. Prove "
    "`__debug_assertions__` is False and the path is under the lane. Record "
    "load; keep the box quiet.\n"
    "\n"
    "### Live-oracle provisioning\n"
    "Live PySpark is the oracle when a unit asserts Spark values. Provision "
    "with `--extra record` as `make parity-live` does. One JVM beside at most "
    "one other. Wait for an empty box (60 s poll, up to 15 min). Redirect ivy "
    "into the lane (`cp -a ~/.ivy2.5.2 <lane>/.ivy2` + "
    '`PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=<lane>/.ivy2 pyspark-shell"`). '
    "`JAVA_HOME` is Java 17. ANSI on; UTC session zone unless the case is "
    "about zones. Kill what you start.\n"
    "\n"
    "### Size ceilings\n"
    "Size ceilings only ratchet down. A new Python file stays under 1000 "
    "lines. Every ratchet in `scripts/check_lib_py.py` is mirrored in "
    "`python/repark-parity/tests/test_cap_1_source_file_line_cap.py` in the "
    "same commit. Do not raise a gate baseline without owner approval. Do not "
    "change what any gate requires.\n"
    "\n"
    "### Map lockstep\n"
    "`map.md` in every directory, updated in the same change. A new directory "
    "gets a new `map.md`. No amends.\n"
    "\n"
    "### Frozen ledgers\n"
    "A ledger in `completed/` or `archive/` is frozen. Do not amend it.\n"
)


def _load_extract() -> ModuleType:
    """Load the sibling field-extractor module next to this file."""
    path: Path = Path(__file__).resolve().with_name("sepmo_packet_extract.py")
    name: str = "sepmo_packet_extract"
    loaded: ModuleType | None = sys.modules.get(name)
    if loaded is not None and Path(getattr(loaded, "__file__", "")).resolve() == path:
        return loaded
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_extract = _load_extract()
PacketError = _extract.PacketError
HOME_DIR = _extract.HOME_DIR
parse_brief = _extract.parse_brief
section_named = _extract.section_named
extract_gate_commands = _extract.extract_gate_commands
extract_boundaries = _extract.extract_boundaries
assert_boundaries_captured = _extract.assert_boundaries_captured
looks_like_command = _extract.looks_like_command
command_is_shell = _extract.command_is_shell


def build_packet(
    unit: str,
    role: str,
    base_revision: str,
    brief_text: str,
    adapter: str,
    attempt: int = 1,
    working_diff_identity: str = "",
    untracked_inputs: list[str] | None = None,
) -> dict[str, Any]:
    """Assemble one packet object from a sanitized brief."""
    _validate_identity(unit, role, adapter, base_revision, attempt)
    sanitized: str = sanitize_text(brief_text)
    title, sections = _extract.parse_brief(sanitized)
    boundaries: dict[str, list[str]] = _extract.extract_boundaries(sections, sanitized)
    packet: dict[str, Any] = {
        "packet_version": PACKET_VERSION,
        "identity": {
            "unit": unit,
            "role": role,
            "attempt": attempt,
            "packet_format_version": PACKET_VERSION,
            "task_reference": title or unit,
            "adapter": adapter,
        },
        "source_identity": {
            "repository": REPOSITORY,
            "base_revision": base_revision,
            "working_diff_identity": working_diff_identity,
            "untracked_inputs": list(untracked_inputs or []),
            "brief_hash": sha256_text(sanitized),
        },
        "authority": {
            "contract": CONTRACT,
            "binding_version": BINDING_VERSION,
            "source_references": list(SOURCE_REFERENCES),
            "constraints": list(STABLE_RULES),
        },
        "scope": _extract.scope_group(unit, role, title, sections, boundaries["closed"]),
        "implementation_context": _extract.context_group(
            sanitized, sections, boundaries["dependency_decisions"]
        ),
        "verification": _extract.verification_group(sanitized, sections),
        "permissions_and_resources": _extract.permissions_group(
            role, boundaries["writable"], boundaries["closed"]
        ),
        "handoff": _extract.handoff_group(role, sanitized),
        "stable_prefix": STABLE_PREFIX,
        "dynamic_markdown": "",
    }
    packet["dynamic_markdown"] = render_dynamic(packet)
    return packet


def render_markdown(packet: dict[str, Any]) -> str:
    """Render the worker Markdown with the stable prefix first."""
    dynamic: str = str(packet.get("dynamic_markdown") or "").lstrip("\n")
    return STABLE_PREFIX.rstrip("\n") + DYNAMIC_SPLIT + "\n" + dynamic


def prefix_of(markdown: str) -> str:
    """Return the markdown bytes before the dynamic split."""
    if DYNAMIC_SPLIT not in markdown:
        raise PacketError("packet markdown has no ## Dynamic split")
    return markdown.split(DYNAMIC_SPLIT, 1)[0] + "\n"


def dynamic_of(markdown: str) -> str:
    """Return the markdown bytes after the dynamic split."""
    if DYNAMIC_SPLIT not in markdown:
        raise PacketError("packet markdown has no ## Dynamic split")
    return markdown.split(DYNAMIC_SPLIT, 1)[1]


def check_packet(
    packet: dict[str, Any],
    markdown: str,
    brief_text: str | None = None,
    schema: dict[str, Any] | None = None,
) -> list[str]:
    """Return findings for one packet; empty means valid."""
    findings: list[str] = []
    loaded_schema: dict[str, Any] = schema if schema is not None else load_schema()
    findings.extend(validate_schema(packet, loaded_schema))
    try:
        prefix: str = prefix_of(markdown)
    except PacketError as error:
        findings.append(str(error))
        return findings
    if prefix != STABLE_PREFIX:
        findings.append("stable prefix is not byte-identical to the assembler prefix")
    stored_prefix: Any = packet.get("stable_prefix")
    if stored_prefix != STABLE_PREFIX:
        findings.append("JSON stable_prefix is not the assembler prefix")
    if str(packet.get("dynamic_markdown") or "") != dynamic_of(markdown).lstrip("\n"):
        findings.append("JSON dynamic_markdown does not match the markdown dynamic section")
    findings.extend(check_constraints(prefix))
    if sha256_text(prefix) != sha256_text(STABLE_PREFIX):
        findings.append("prefix hash mismatch")
    findings.extend(_check_rerender(packet, markdown))
    adapter: str = str(packet.get("identity", {}).get("adapter") or "")
    if adapter in AUTHORED_BY:
        findings.extend(_extract.trailer_findings(AUTHORED_BY[adapter], markdown, packet))
    findings.extend(_extract.prefix_negation_findings(dynamic_of(markdown)))
    commands: Any = packet.get("verification", {}).get("commands") or []
    if isinstance(commands, list):
        findings.extend(_extract.validate_commands([str(item) for item in commands]))
    if HOME_DIR.search(markdown) or HOME_DIR.search(json.dumps(packet)):
        findings.append("packet contains a home directory path")
    if brief_text is not None:
        expected: str = sha256_text(sanitize_text(brief_text))
        actual: Any = packet.get("source_identity", {}).get("brief_hash")
        if actual != expected:
            findings.append(
                f"brief hash mismatch: packet has {actual!r}, brief hashes to {expected!r}"
            )
    return findings


def check_constraints(prefix: str) -> list[str]:
    """Return a finding for every stable rule missing from the prefix."""
    findings: list[str] = []
    for rule in STABLE_RULES:
        if rule not in prefix:
            findings.append(f"missing stable rule: {rule}")
    return findings


def _check_rerender(packet: dict[str, Any], markdown: str) -> list[str]:
    """Re-render the dynamic section from sidecar fields and compare."""
    findings: list[str] = []
    try:
        rendered: str = render_dynamic(packet)
        markdown_dynamic: str = dynamic_of(markdown).lstrip("\n")
    except (KeyError, TypeError, PacketError) as error:
        findings.append(f"cannot re-render dynamic section: {error}")
        return findings
    stored: str = str(packet.get("dynamic_markdown") or "")
    if rendered != markdown_dynamic:
        findings.append("re-rendered dynamic section does not match the markdown dynamic section")
    if rendered != stored:
        findings.append("JSON dynamic_markdown does not match a re-render from sidecar fields")
    if stored != markdown_dynamic:
        findings.append("JSON dynamic_markdown does not match the markdown dynamic section")
    return findings


def diff_packets(packet_a: dict[str, Any], packet_b: dict[str, Any]) -> str:
    """Return a unified diff of the two dynamic sections only."""
    unit_a: str = str(packet_a.get("identity", {}).get("unit") or "a")
    unit_b: str = str(packet_b.get("identity", {}).get("unit") or "b")
    left: list[str] = str(packet_a.get("dynamic_markdown") or "").splitlines(keepends=True)
    right: list[str] = str(packet_b.get("dynamic_markdown") or "").splitlines(keepends=True)
    if left and not left[-1].endswith("\n"):
        left[-1] = left[-1] + "\n"
    if right and not right[-1].endswith("\n"):
        right[-1] = right[-1] + "\n"
    lines = unified_diff(
        left,
        right,
        fromfile=f"{unit_a} dynamic",
        tofile=f"{unit_b} dynamic",
        lineterm="\n",
    )
    return "".join(lines)


def packet_sizes(markdown: str) -> dict[str, int]:
    """Count bytes and words of the prefix, the dynamic section, and the whole packet."""
    prefix: str = prefix_of(markdown)
    dynamic: str = dynamic_of(markdown)
    return {
        "prefix_bytes": _byte_count(prefix),
        "prefix_words": _word_count(prefix),
        "dynamic_bytes": _byte_count(dynamic),
        "dynamic_words": _word_count(dynamic),
        "packet_bytes": _byte_count(markdown),
        "packet_words": _word_count(markdown),
    }


def sanitize_text(text: str) -> str:
    """Replace absolute home directories with $HOME."""
    return HOME_DIR.sub("$HOME", text)


def sha256_text(text: str) -> str:
    """Return the SHA-256 hex digest of UTF-8 text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_schema() -> dict[str, Any]:
    """Load the checked-in packet schema from the repository."""
    schema_path: Path = _repo_root() / SCHEMA_RELATIVE
    if not schema_path.is_file():
        raise PacketError(f"missing schema: {schema_path}")
    try:
        payload: Any = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise PacketError(f"malformed schema: {schema_path}") from error
    if not isinstance(payload, dict):
        raise PacketError(f"schema must be an object: {schema_path}")
    return payload


def validate_schema(instance: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Return JSON Schema findings for this packet schema dialect."""
    findings: list[str] = []
    type_spec: Any = schema.get("type")
    if type_spec is not None:
        allowed: list[str] = type_spec if isinstance(type_spec, list) else [type_spec]
        if not _json_type_matches(instance, allowed):
            findings.append(f"{path}: expected {allowed}, got {type(instance).__name__}")
            return findings
    if "enum" in schema and instance not in schema["enum"]:
        findings.append(f"{path}: {instance!r} is not in enum {schema['enum']}")
    if "const" in schema and instance != schema["const"]:
        findings.append(f"{path}: expected const {schema['const']!r}")
    if (
        "pattern" in schema
        and isinstance(instance, str)
        and (re.fullmatch(str(schema["pattern"]), instance) is None)
    ):
        findings.append(f"{path}: does not match {schema['pattern']}")
    if (
        "minLength" in schema
        and isinstance(instance, str)
        and (len(instance) < int(schema["minLength"]))
    ):
        findings.append(f"{path}: shorter than minLength")
    if (
        "minItems" in schema
        and isinstance(instance, list)
        and (len(instance) < int(schema["minItems"]))
    ):
        findings.append(f"{path}: fewer than minItems")
    if (
        "minimum" in schema
        and isinstance(instance, (int, float))
        and type(instance) is not bool
        and instance < schema["minimum"]
    ):
        findings.append(f"{path}: below minimum")
    findings.extend(_validate_object(instance, schema, path))
    findings.extend(_validate_array(instance, schema, path))
    return findings


def render_dynamic(packet: dict[str, Any]) -> str:
    """Render the unit-specific markdown from a packet object."""
    identity: dict[str, Any] = packet["identity"]
    source: dict[str, Any] = packet["source_identity"]
    adapter: str = str(identity["adapter"])
    role: str = str(identity["role"])
    lines: list[str] = [
        "### Identity",
        f"- unit: {identity['unit']}",
        f"- role: {role}",
        f"- attempt: {identity['attempt']}",
        f"- adapter: {adapter}",
        f"- packet_format_version: {identity['packet_format_version']}",
        f"- task_reference: {identity['task_reference']}",
        f"- authored_by_trailer: {AUTHORED_BY[adapter]}",
        "",
        "### Source identity",
        f"- repository: {source['repository']}",
        f"- base_revision: {source['base_revision']}",
        f"- working_diff_identity: {source['working_diff_identity'] or '(empty)'}",
        f"- untracked_inputs: {_slash_list(source['untracked_inputs'])}",
        f"- brief_hash: {source['brief_hash']}",
        "",
        "### Authority",
        (
            f"Contract: {packet['authority']['contract']}. "
            f"Binding: SEPMO {packet['authority']['binding_version']}. "
            "Constraints: the stable prefix above. "
            f"Sources: {_slash_list(packet['authority']['source_references'])}."
        ),
        "",
        "### Scope",
        f"**Objective.** {packet['scope']['objective']}",
        "",
        f"**Requirement ids.** {_slash_list(packet['scope']['requirement_ids'])}",
        "",
        "**Acceptance.**",
        packet["scope"]["acceptance_criteria"] or "(none)",
        "",
        f"**Exclusions.** {_slash_list(packet['scope']['exclusions'])}",
        "",
        "### Implementation context",
        f"- relevant_files: {_slash_list(packet['implementation_context']['relevant_files'])}",
        f"- callers: {_slash_list(packet['implementation_context']['callers'])}",
        f"- interfaces: {_slash_list(packet['implementation_context']['interfaces'])}",
        (
            "- dependency_decisions: "
            f"{_slash_list(packet['implementation_context']['dependency_decisions'])}"
        ),
        f"- known_traps: {_slash_list(packet['implementation_context']['known_traps'])}",
        "",
        "### Verification",
        "**Commands.**",
        *_bullet_lines(packet["verification"]["commands"]),
        "",
        "**Behavioral cases.**",
        packet["verification"]["behavioral_cases"] or "(none)",
        "",
        (f"**Oracle requirements.** {_slash_list(packet['verification']['oracle_requirements'])}"),
        (
            "**Evidence destinations.** "
            f"{_slash_list(packet['verification']['evidence_destinations'])}"
        ),
        "",
        "### Permissions and resources",
        (
            "- authorized_actions: "
            f"{_slash_list(packet['permissions_and_resources']['authorized_actions'])}"
        ),
        (
            "- ownership_boundaries: "
            f"{_slash_list(packet['permissions_and_resources']['ownership_boundaries'])}"
        ),
        (
            "- resource_limits: "
            f"{_slash_list(packet['permissions_and_resources']['resource_limits'])}"
        ),
        (
            "- escalation_conditions: "
            f"{_slash_list(packet['permissions_and_resources']['escalation_conditions'])}"
        ),
        "",
        "### Handoff",
        (f"- expected_output_fields: {_slash_list(packet['handoff']['expected_output_fields'])}"),
        (f"- unresolved_decisions: {_slash_list(packet['handoff']['unresolved_decisions'])}"),
        (f"- dependency_consumers: {_slash_list(packet['handoff']['dependency_consumers'])}"),
        "",
        f"Use this trailer as the last line of every commit: {AUTHORED_BY[adapter]}",
    ]
    if role == "critic":
        lines.extend(
            [
                "",
                "This is a Critic packet. Inputs are the unit clauses, the diff "
                "and artifacts, test results, and the attack taxonomy. The Actor "
                "Self Logic Review and Actor narrative are excluded.",
            ]
        )
    return "\n".join(lines) + "\n"


def dump_json(packet: dict[str, Any]) -> str:
    """Serialize a packet sidecar with stable key order."""
    return json.dumps(packet, indent=2, sort_keys=True) + "\n"


def load_packet_files(path: Path) -> tuple[dict[str, Any], str]:
    """Load a packet JSON sidecar and its sibling markdown."""
    markdown_path, json_path = _packet_paths(path)
    if not markdown_path.is_file():
        raise PacketError(f"missing markdown packet: {markdown_path}")
    if not json_path.is_file():
        raise PacketError(f"missing JSON sidecar: {json_path}")
    try:
        payload: Any = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise PacketError(f"malformed JSON sidecar: {json_path}") from error
    if not isinstance(payload, dict):
        raise PacketError(f"JSON sidecar must be an object: {json_path}")
    markdown: str = markdown_path.read_text(encoding="utf-8")
    return payload, markdown


def main(argv: list[str] | None = None) -> int:
    """Run build, check, or diff."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Assemble, check, and diff SEPMO compact worker packets."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    build_parser = subparsers.add_parser("build", help="render markdown and JSON")
    build_parser.add_argument("--unit", required=True)
    build_parser.add_argument("--role", required=True, choices=ROLES)
    build_parser.add_argument("--base", required=True)
    build_parser.add_argument("--brief", required=True)
    build_parser.add_argument("--adapter", default="muse", choices=ADAPTERS)
    build_parser.add_argument("--attempt", type=int, default=1)
    build_parser.add_argument("--working-diff", default="")
    build_parser.add_argument("--out-dir", required=True)
    check_parser = subparsers.add_parser("check", help="validate a packet")
    check_parser.add_argument("packet")
    check_parser.add_argument("--brief")
    diff_parser = subparsers.add_parser("diff", help="dynamic-only delta")
    diff_parser.add_argument("packet_a")
    diff_parser.add_argument("packet_b")
    arguments: argparse.Namespace = parser.parse_args(argv)
    try:
        if arguments.command == "build":
            return _run_build(arguments)
        if arguments.command == "check":
            return _run_check(arguments)
        return _run_diff(arguments)
    except PacketError as error:
        print(f"sepmo-packet: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"sepmo-packet: {error}", file=sys.stderr)
        return 1


def _run_build(arguments: argparse.Namespace) -> int:
    _reject_remote(arguments.brief)
    _reject_remote(arguments.out_dir)
    brief_path: Path = Path(arguments.brief)
    if not brief_path.is_file():
        raise PacketError(f"brief is not a file: {brief_path}")
    packet: dict[str, Any] = build_packet(
        unit=arguments.unit,
        role=arguments.role,
        base_revision=arguments.base,
        brief_text=brief_path.read_text(encoding="utf-8"),
        adapter=arguments.adapter,
        attempt=arguments.attempt,
        working_diff_identity=arguments.working_diff,
    )
    markdown: str = render_markdown(packet)
    findings: list[str] = check_packet(packet, markdown)
    if findings:
        joined: str = "; ".join(findings)
        raise PacketError(f"assembled packet failed check: {joined}")
    out_dir: Path = Path(arguments.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem: str = f"{arguments.unit}-{arguments.role}"
    markdown_path: Path = out_dir / f"{stem}.md"
    json_path: Path = out_dir / f"{stem}.json"
    markdown_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(dump_json(packet), encoding="utf-8")
    sys.stdout.write(f"{markdown_path}\n{json_path}\n")
    return 0


def _run_check(arguments: argparse.Namespace) -> int:
    _reject_remote(arguments.packet)
    packet, markdown = load_packet_files(Path(arguments.packet))
    brief_text: str | None = None
    if arguments.brief:
        _reject_remote(arguments.brief)
        brief_path: Path = Path(arguments.brief)
        if not brief_path.is_file():
            raise PacketError(f"brief is not a file: {brief_path}")
        brief_text = brief_path.read_text(encoding="utf-8")
    findings: list[str] = check_packet(packet, markdown, brief_text=brief_text)
    if findings:
        for finding in findings:
            print(f"sepmo-packet: {finding}", file=sys.stderr)
        return 1
    sys.stdout.write("ok\n")
    return 0


def _run_diff(arguments: argparse.Namespace) -> int:
    _reject_remote(arguments.packet_a)
    _reject_remote(arguments.packet_b)
    packet_a, _markdown_a = load_packet_files(Path(arguments.packet_a))
    packet_b, _markdown_b = load_packet_files(Path(arguments.packet_b))
    rendered: str = diff_packets(packet_a, packet_b)
    sys.stdout.write(rendered)
    if rendered and not rendered.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def _validate_identity(
    unit: str, role: str, adapter: str, base_revision: str, attempt: int
) -> None:
    if not UNIT_SAFE.match(unit):
        raise PacketError(f"unit id is not a safe token: {unit!r}")
    if role not in ROLES:
        raise PacketError(f"role must be actor or critic: {role!r}")
    if adapter not in ADAPTERS:
        raise PacketError(f"adapter is not known: {adapter!r}")
    if not SHA_SAFE.match(base_revision):
        raise PacketError(f"base revision is not a git sha: {base_revision!r}")
    if attempt < 1:
        raise PacketError("attempt must be >= 1")


def _slash_list(values: list[Any]) -> str:
    if not values:
        return "(none)"
    return ", ".join(str(item) for item in values)


def _bullet_lines(values: list[Any]) -> list[str]:
    if not values:
        return ["- (none)"]
    return [f"- {item}" for item in values]


def _byte_count(text: str) -> int:
    return len(text.encode("utf-8"))


def _word_count(text: str) -> int:
    return len(text.split())


def _json_type_matches(instance: Any, allowed: list[str]) -> bool:
    mapping: dict[str, type | tuple[type, ...]] = {
        "object": dict,
        "array": list,
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "null": type(None),
    }
    for name in allowed:
        expected = mapping.get(name)
        if expected is None:
            return False
        if name == "integer" and type(instance) is bool:
            continue
        if name == "number" and type(instance) is bool:
            continue
        if isinstance(instance, expected):
            if name == "integer" and type(instance) is int:
                return True
            if name != "integer":
                return True
    return False


def _validate_object(instance: Any, schema: dict[str, Any], path: str) -> list[str]:
    if "properties" not in schema and schema.get("type") != "object":
        return []
    if not isinstance(instance, dict):
        return []
    findings: list[str] = []
    required: list[str] = list(schema.get("required") or [])
    for key in required:
        if key not in instance:
            findings.append(f"{path}: missing required {key}")
    properties: dict[str, Any] = dict(schema.get("properties") or {})
    additional: Any = schema.get("additionalProperties", True)
    for key, value in instance.items():
        if key in properties:
            findings.extend(validate_schema(value, properties[key], f"{path}.{key}"))
        elif additional is False:
            findings.append(f"{path}: additional property {key}")
        elif isinstance(additional, dict):
            findings.extend(validate_schema(value, additional, f"{path}.{key}"))
    return findings


def _validate_array(instance: Any, schema: dict[str, Any], path: str) -> list[str]:
    if "items" not in schema and schema.get("type") != "array":
        return []
    if not isinstance(instance, list):
        return []
    item_schema: Any = schema.get("items")
    if not isinstance(item_schema, dict):
        return []
    findings: list[str] = []
    for index, item in enumerate(instance):
        findings.extend(validate_schema(item, item_schema, f"{path}[{index}]"))
    return findings


def _packet_paths(path: Path) -> tuple[Path, Path]:
    if path.suffix == ".json":
        return path.with_suffix(".md"), path
    if path.suffix == ".md":
        return path, path.with_suffix(".json")
    raise PacketError("packet path must be .md or .json")


def _reject_remote(path: Path | str) -> None:
    text: str = str(path).strip().replace("\\", "/")
    if "://" in text or REMOTE_SCHEME.match(text):
        raise PacketError("refusing a remote path; assembler is local-only")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


if __name__ == "__main__":
    raise SystemExit(main())
