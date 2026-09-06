#!/usr/bin/env python3
"""Extract packet field groups from a sanitized campaign brief."""

from __future__ import annotations

import json
import re
import subprocess
from typing import Any

PATH_TOKEN: re.Pattern[str] = re.compile(r"(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+\.[A-Za-z0-9]+")
HOME_DIR: re.Pattern[str] = re.compile(r"/(?:home|Users)/[A-Za-z0-9._-]+")
FENCE_BLOCK: re.Pattern[str] = re.compile(r"```(?:[a-zA-Z0-9_-]+)?\n(.*?)```", re.S)
INLINE_CODE: re.Pattern[str] = re.compile(r"`([^`]+)`")
BACKTICK: re.Pattern[str] = re.compile(r"`([^`]+)`")
HANDBACK_KEY: re.Pattern[str] = re.compile(r'"([A-Za-z][A-Za-z0-9_]*)"\s*:')
ONLY_THROUGH: re.Pattern[str] = re.compile(
    r"(?i)(`[^`]+`|\b[\w./-]+\b)\s+may change ONLY through\s+(`[^`]+`|\S+)"
)
GATE_SPLIT: re.Pattern[str] = re.compile(r"\s*&&\s*|\n")
PLACEHOLDER: re.Pattern[str] = re.compile(r"<[^<>]+>")
ENV_LEAD: re.Pattern[str] = re.compile(r"^(?:[A-Za-z_][A-Za-z0-9_]*=\S*\s+)+")
COMMAND_VERB: re.Pattern[str] = re.compile(
    r"^(?:make|cargo|python3?|uv|\.venv/|pytest|typos|git|ruff|maturin|cd|"
    r"for|while|if|pgrep|cp|kill|bash|sh|VIRTUAL_ENV=|CARGO_|RUST_|PYTHONPATH=|"
    r"REPARK_|JAVA_HOME=|SPARK_)"
)
BARE_PATH: re.Pattern[str] = re.compile(
    r"\b(?:STATUS\.md|Cargo\.lock|Cargo\.toml|pyproject\.toml|"
    r"briefs/next-sequence\.md|BACKLOG_BASELINE|\.github/)\b"
)
WRITABLE_LABEL: re.Pattern[str] = re.compile(r"(?i)\bwritable\s*:")
CLOSED_LABEL: re.Pattern[str] = re.compile(r"(?i)\bclosed\s*:")
NEVER_TOUCH: re.Pattern[str] = re.compile(r"(?i)never touch\s+")
UNTOUCHED_TAIL: re.Pattern[str] = re.compile(
    r"(?i)((?:`[^`]+`(?:\s+and\s+`[^`]+`)+)|(?:`[^`]+`))\s+untouched"
)
NEVER_CHANGE_DEP: re.Pattern[str] = re.compile(
    r"(?i)never change dependencies|do not change dependencies"
)
TOUCH_CUT: re.Pattern[str] = re.compile(
    r";|\bmay change\b|never change dependencies|do not change dependencies",
    re.IGNORECASE,
)
NEGATING_PHRASES: tuple[str, ...] = (
    "trailers are allowed",
    "you may push",
    "comments are fine",
)
FORBIDDEN_TRAILER: tuple[str, ...] = (
    "Co-" + "Authored-By:",
    "Co-" + "authored-by:",
    "Signed-off-by:",
)
ACTOR_HANDOFF: tuple[str, ...] = ("status", "commits", "gates", "notes", "questions")
CRITIC_HANDOFF: tuple[str, ...] = (
    "findings",
    "coverage_attestation",
    "dispositions",
    "evidence",
)
KNOWN_PATH_NAMES: frozenset[str] = frozenset(
    {
        "Cargo.lock",
        "Cargo.toml",
        "pyproject.toml",
        "STATUS.md",
        "BACKLOG_BASELINE",
        ".github/",
        "map.md",
    }
)


class PacketError(Exception):
    """A build, check, or diff input that must fail loudly."""


def parse_brief(text: str) -> tuple[str, dict[str, str]]:
    """Split a brief into its title and `##` sections."""
    lines: list[str] = text.splitlines()
    title: str = ""
    if lines and lines[0].startswith("# "):
        title = lines[0][2:].strip()
    sections: dict[str, str] = {}
    current: str = "_lead"
    buffer: list[str] = []
    for line in lines:
        if line.startswith("## "):
            sections[current] = "\n".join(buffer).strip()
            current = line[3:].strip()
            buffer = []
            continue
        buffer.append(line)
    sections[current] = "\n".join(buffer).strip()
    return title, sections


def section_named(sections: dict[str, str], *names: str) -> str:
    """Return the first section whose heading starts with one of the names."""
    lowered: dict[str, str] = {key.lower(): value for key, value in sections.items()}
    for name in names:
        needle: str = name.lower()
        for key, value in lowered.items():
            if key == needle or key.startswith(needle):
                return value
    return ""


def is_rules_heading(heading: str) -> bool:
    """Return whether a heading is the repeated non-negotiable rules block."""
    lowered: str = heading.lower()
    return lowered.startswith("rules") or "non-negotiable" in lowered


def rules_text(sections: dict[str, str]) -> str:
    """Return the non-negotiable rules body, or empty."""
    for heading, body in sections.items():
        if is_rules_heading(heading):
            return body
    return ""


def extract_gate_commands(gates_text: str) -> list[str]:
    """Parse shell commands from fenced and inline code, then split on `&&`."""
    chunks: list[str] = []
    for fence in FENCE_BLOCK.findall(gates_text):
        chunks.append(fence)
    remainder: str = FENCE_BLOCK.sub(" ", gates_text)
    for span in INLINE_CODE.findall(remainder):
        chunks.append(span)
    commands: list[str] = []
    seen: set[str] = set()
    for chunk in chunks:
        for piece in GATE_SPLIT.split(chunk):
            command: str = piece.strip().strip("`").strip()
            if not command or command in seen:
                continue
            if not looks_like_command(command):
                continue
            seen.add(command)
            commands.append(command)
    return commands


def looks_like_command(command: str) -> bool:
    """Return whether a string is a shell command rather than prose."""
    stripped: str = command.strip()
    if not stripped or "`" in stripped:
        return False
    if stripped.startswith("{") or stripped.startswith("["):
        return False
    without_env: str = ENV_LEAD.sub("", stripped)
    return COMMAND_VERB.search(without_env) is not None


def command_is_shell(command: str) -> bool:
    """Return whether `bash -n` accepts the command (placeholders replaced)."""
    checkable: str = PLACEHOLDER.sub("PLACEHOLDER", command)
    try:
        completed = subprocess.run(
            ["bash", "-n", "-c", checkable],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise PacketError(f"bash -n is unavailable: {error}") from error
    return completed.returncode == 0


def validate_commands(commands: list[str]) -> list[str]:
    """Return findings for commands that are prose, backticks, or invalid shell."""
    findings: list[str] = []
    for command in commands:
        if "`" in command:
            findings.append(f"verification command contains backticks: {command!r}")
            continue
        if not looks_like_command(command):
            findings.append(f"verification command is prose, not a shell command: {command!r}")
            continue
        if not command_is_shell(command):
            findings.append(f"verification command fails bash -n: {command!r}")
    return findings


def extract_boundaries(sections: dict[str, str], brief_text: str = "") -> dict[str, list[str]]:
    """Carry writable, closed, and dependency clauses from the brief."""
    body: str = rules_text(sections)
    scan: str = brief_text or body
    writable_clause: str = _clause_after(scan, WRITABLE_LABEL)
    closed_clause: str = _clause_after(scan, CLOSED_LABEL)
    writable: list[str] = _unique(_paths_in(writable_clause))
    closed: list[str] = _unique(_paths_in(closed_clause))
    decisions: list[str] = []
    touch_spans: list[str] = []
    untouched_spans: list[str] = []
    for sentence in _sentences(scan):
        if NEVER_TOUCH.search(sentence):
            after_touch: str = _after_match(sentence, NEVER_TOUCH)
            touch_span: str = TOUCH_CUT.split(after_touch, maxsplit=1)[0]
            touch_spans.append(touch_span)
            closed.extend(_paths_in(touch_span))
        for match in UNTOUCHED_TAIL.finditer(sentence):
            untouched_spans.append(match.group(1))
            closed.extend(_paths_in(match.group(1)))
        for match in ONLY_THROUGH.finditer(sentence):
            decisions.append(match.group(0).strip().rstrip("."))
        dep_match = NEVER_CHANGE_DEP.search(sentence)
        if dep_match is not None:
            decisions.append(sentence[dep_match.start() :].strip().rstrip("."))
    closed = _unique(closed)
    writable = _unique(writable)
    closed = [item for item in closed if item not in set(writable)]
    decisions = _unique(decisions)
    assert_boundaries_captured(
        writable_clause,
        closed_clause,
        touch_spans,
        untouched_spans,
        decisions,
        writable,
        closed,
    )
    return {
        "writable": writable,
        "closed": closed,
        "dependency_decisions": decisions,
    }


def assert_boundaries_captured(
    writable_clause: str,
    closed_clause: str,
    touch_spans: list[str],
    untouched_spans: list[str],
    decisions: list[str],
    writable: list[str],
    closed: list[str],
) -> None:
    """Fail when a boundary clause names a path the lists did not keep."""
    checks: list[tuple[str, list[str]]] = [
        (writable_clause, writable),
        (closed_clause, closed),
    ]
    for span in touch_spans:
        checks.append((span, closed + writable))
    for span in untouched_spans:
        checks.append((span, closed))
    for clause in decisions:
        checks.append((clause, decisions + writable + closed))
    for span, bucket in checks:
        for path in _paths_in(span):
            if not _is_captured(path, bucket):
                raise PacketError(f"boundary path not captured: {path!r} in {span!r}")


def extract_handoff_keys(role: str, brief_text: str) -> list[str]:
    """Return role defaults plus keys declared in a brief hand-back object."""
    fields: list[str] = list(CRITIC_HANDOFF if role == "critic" else ACTOR_HANDOFF)
    lowered: str = brief_text.lower()
    marker: int = lowered.find("handback.json")
    if marker < 0:
        marker = lowered.find("hand-back")
    if marker < 0:
        return fields
    start: int = brief_text.find("{", marker)
    if start < 0:
        return fields
    blob: str = _balanced_object(brief_text, start)
    if not blob:
        return fields
    seen: set[str] = set(fields)
    for key in _top_level_keys(blob):
        if key in seen:
            continue
        seen.add(key)
        fields.append(key)
    return fields


def prefix_negation_findings(dynamic: str) -> list[str]:
    """Return findings when the dynamic section contradicts the prefix."""
    findings: list[str] = []
    lowered: str = dynamic.lower()
    for phrase in NEGATING_PHRASES:
        if phrase in lowered:
            findings.append(f"dynamic section negates the prefix: {phrase}")
    for match in re.finditer(r"--no-verify", dynamic):
        start: int = max(0, match.start() - 24)
        window: str = dynamic[start : match.end()].lower()
        if "never" not in window:
            findings.append("dynamic section negates the prefix: --no-verify")
    return findings


def trailer_findings(adapter_trailer: str, markdown: str, packet: dict[str, Any]) -> list[str]:
    """Return findings when the rendered trailer is not the adapter AUTHORED_BY."""
    findings: list[str] = []
    if adapter_trailer not in markdown:
        findings.append(f"rendered trailer is not the adapter AUTHORED_BY: {adapter_trailer}")
    authored_line: str = f"authored_by_trailer: {adapter_trailer}"
    if authored_line not in markdown:
        findings.append("authored_by_trailer does not match adapter AUTHORED_BY")
    closing: str = f"Use this trailer as the last line of every commit: {adapter_trailer}"
    if closing not in markdown:
        findings.append("closing trailer line does not match adapter AUTHORED_BY")
    combined: str = markdown + "\n" + json.dumps(packet)
    for form in FORBIDDEN_TRAILER:
        if form in combined:
            findings.append(f"forbidden trailer form: {form.rstrip(':')}")
    for match in re.finditer(r"Authored-By: [^\n<]+(?:<[^>]+>)?", combined):
        line: str = match.group(0).strip()
        if line != adapter_trailer and adapter_trailer not in line:
            findings.append(f"other Authored-By trailer: {line}")
    return findings


def scope_group(
    unit: str, role: str, title: str, sections: dict[str, str], closed: list[str]
) -> dict[str, Any]:
    """Build the scope field group."""
    deliverable: str = section_named(
        sections, "Deliverable", "Deliverables", "The measured problem", "The two halves"
    )
    roster: str = section_named(sections, "Roster")
    acceptance_parts: list[str] = [part for part in (roster, deliverable) if part]
    exclusions: list[str] = list(closed)
    if role == "critic":
        exclusions.append("Actor Self Logic Review")
        exclusions.append("Actor narrative")
    requirement_ids: list[str] = [unit]
    if roster:
        requirement_ids.append("roster")
    return {
        "objective": title or unit,
        "requirement_ids": requirement_ids,
        "acceptance_criteria": "\n\n".join(acceptance_parts),
        "exclusions": exclusions,
    }


def context_group(
    brief_text: str, sections: dict[str, str], decisions: list[str]
) -> dict[str, Any]:
    """Build the implementation-context field group."""
    files: list[str] = []
    seen: set[str] = set()
    for match in PATH_TOKEN.finditer(brief_text):
        token: str = match.group(0)
        if token in seen:
            continue
        seen.add(token)
        files.append(token)
    traps: list[str] = []
    if "HALT" in brief_text:
        traps.append("HALT with evidence rather than inventing a missing decision")
    if section_named(sections, "Durability"):
        traps.append("Commit early; lanes live under $HOME/repark-lanes/lanes/")
    return {
        "relevant_files": files[:40],
        "callers": [],
        "interfaces": [],
        "dependency_decisions": list(decisions),
        "known_traps": traps,
    }


def verification_group(brief_text: str, sections: dict[str, str]) -> dict[str, Any]:
    """Build the verification field group from fenced and inline gate commands."""
    gates: str = section_named(sections, "Gates before hand-back", "Gates")
    commands: list[str] = extract_gate_commands(gates)
    if gates and not commands:
        raise PacketError("gates section produced no shell commands")
    command_findings: list[str] = validate_commands(commands)
    if command_findings:
        raise PacketError("; ".join(command_findings))
    oracles: list[str] = []
    if "PySpark" in brief_text or "live oracle" in brief_text.lower():
        oracles.append("live PySpark 4.1.2")
    destinations: list[str] = ["task/ledgers/staging/", "handback.json"]
    if "ledger" in brief_text.lower():
        destinations.append("unit ledger")
    return {
        "commands": commands,
        "behavioral_cases": section_named(sections, "Red-first", "Red first"),
        "oracle_requirements": oracles,
        "evidence_destinations": destinations,
    }


def permissions_group(role: str, writable: list[str], closed: list[str]) -> dict[str, Any]:
    """Build the permissions-and-resources field group."""
    authorized: list[str] = [
        "Read the named sources",
        "Edit only the writable paths in this packet",
        "Run the listed verification commands",
    ]
    if writable:
        authorized.append("Writable: " + ", ".join(writable))
    if role == "actor":
        authorized.append("Commit with the bound identity and trailer")
    else:
        authorized.append("File findings and a coverage attestation")
    return {
        "authorized_actions": authorized,
        "ownership_boundaries": list(closed),
        "resource_limits": [
            "Three cargo builders is the box cap",
            "One Spark JVM beside at most one other",
        ],
        "escalation_conditions": [
            "Ambiguity that changes the outcome is a HALT",
            "A red gate is not worked around",
        ],
    }


def handoff_group(role: str, brief_text: str) -> dict[str, Any]:
    """Build the handoff field group, keeping brief-declared keys."""
    return {
        "expected_output_fields": extract_handoff_keys(role, brief_text),
        "unresolved_decisions": [],
        "dependency_consumers": ["orchestrator launch wrapper"],
    }


def _clause_after(text: str, label: re.Pattern[str]) -> str:
    match = label.search(text)
    if match is None:
        return ""
    rest: str = text[match.end() :]
    for sentence in _sentences(rest):
        if sentence:
            return sentence
    return rest.strip()


def _after_match(text: str, pattern: re.Pattern[str]) -> str:
    match = pattern.search(text)
    if match is None:
        return ""
    return text[match.end() :]


def _is_captured(path: str, captured: list[str]) -> bool:
    return any(path == item or path in item or item in path for item in captured)


def _paths_in(text: str) -> list[str]:
    found: list[str] = []
    for match in BACKTICK.findall(text):
        token: str = match.strip()
        if _is_pathish(token):
            found.append(token)
    for match in BARE_PATH.findall(text):
        found.append(match)
    return _unique(found)


def _is_pathish(token: str) -> bool:
    stripped: str = token.strip()
    if not stripped or stripped.startswith("make "):
        return False
    if stripped in KNOWN_PATH_NAMES:
        return True
    if stripped.endswith("/") and re.match(r"^[A-Za-z0-9_.*/-]+/$", stripped):
        return True
    if "/" in stripped:
        return True
    if stripped.startswith(".") and "/" in stripped + "/":
        return True
    return stripped.endswith((".md", ".py", ".toml", ".lock", ".json", ".rs", ".sh", ".txt"))


def _sentences(text: str) -> list[str]:
    pieces: list[str] = []
    current: list[str] = []
    in_tick: bool = False
    for character in text:
        if character == "`":
            in_tick = not in_tick
            current.append(character)
            continue
        if character == "." and not in_tick:
            current.append(character)
            piece: str = "".join(current).strip()
            if piece:
                pieces.append(piece)
            current = []
            continue
        current.append(character)
    tail: str = "".join(current).strip()
    if tail:
        pieces.append(tail)
    return pieces


def _balanced_object(text: str, start: int) -> str:
    depth: int = 0
    for index, character in enumerate(text[start:], start):
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return ""


def _top_level_keys(blob: str) -> list[str]:
    keys: list[str] = []
    depth: int = 0
    index: int = 0
    length: int = len(blob)
    while index < length:
        character: str = blob[index]
        if character == "{":
            depth += 1
            index += 1
            continue
        if character == "}":
            depth -= 1
            index += 1
            continue
        if character == "[":
            depth += 1
            index += 1
            continue
        if character == "]":
            depth -= 1
            index += 1
            continue
        if depth == 1 and character == '"':
            match = HANDBACK_KEY.match(blob[index:])
            if match:
                keys.append(match.group(1))
                index += match.end()
                continue
        index += 1
    return keys


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        cleaned: str = item.strip().rstrip(".")
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result
