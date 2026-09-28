#!/usr/bin/env python3
"""Check staged maps when the optional Rust tool cannot build."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote, unquote_to_bytes

import sync_map_md

START = "<!-- repo-tool:contents:start -->"
END = "<!-- repo-tool:contents:end -->"
REFERENCE_START = re.compile(r"^\s*(?:(?:>\s*)|(?:[-*+]\s+)|(?:\d+[.)]\s+))*\[")


def staged_entries(repo: Path) -> list[tuple[str, str, str]]:
    """Read names, modes, and object IDs from the active Git index."""
    output = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--stage", "-z"],
        capture_output=True,
        check=True,
    ).stdout
    entries: list[tuple[str, str, str]] = []
    for record in output.split(b"\0"):
        if not record:
            continue
        metadata, name = record.split(b"\t", 1)
        mode, object_id, stage = metadata.decode("ascii").split()
        if stage != "0":
            raise ValueError("unmerged Git index entry")
        path = name.decode("utf-8")
        if path.startswith("/") or any(part in ("", ".", "..") for part in path.split("/")):
            raise ValueError(f"unsafe staged path: {path}")
        entries.append((path, mode, object_id))
    return entries


def blob(repo: Path, object_id: str) -> bytes:
    """Read one blob from the repository object database."""
    return subprocess.run(
        ["git", "-C", str(repo), "cat-file", "blob", object_id],
        capture_output=True,
        check=True,
    ).stdout


def materialize(repo: Path, shadow: Path, entries: list[tuple[str, str, str]]) -> None:
    """Build the staged map view with placeholders for non-map targets."""
    symlinks: list[tuple[Path, str]] = []
    for name, mode, object_id in entries:
        destination = shadow / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if mode == "160000":
            destination.mkdir(exist_ok=True)
        elif mode == "120000":
            symlinks.append((destination, blob(repo, object_id).decode("utf-8")))
        elif destination.name == "map.md":
            destination.write_bytes(blob(repo, object_id))
        else:
            destination.touch()
    for destination, target in symlinks:
        if Path(target).is_absolute():
            raise ValueError(f"absolute staged symlink: {destination}")
        resolved = (destination.parent / target).resolve(strict=False)
        if not resolved.is_relative_to(shadow):
            raise ValueError(f"staged symlink escapes repository: {destination}")
        destination.symlink_to(target)


def inventory(paths: list[str], map_path: str) -> str:
    """Render the compiler's managed file inventory for one map."""
    parent = str(Path(map_path).parent)
    prefix = "" if parent == "." else f"{parent}/"
    names = set(paths)
    entries: set[str] = set()
    for path in paths:
        if not path.startswith(prefix):
            continue
        rest = path[len(prefix) :]
        if rest == "map.md":
            continue
        if "/" in rest:
            child = rest.split("/", 1)[0]
            if not child.startswith(".") and f"{prefix}{child}/map.md" in names:
                entries.add(f"{child}/map.md")
        elif not rest.startswith(".") and rest not in ("Cargo.lock", "uv.lock"):
            if Path(rest).suffix in (".rs", ".py", ".sh", ".md", ".toml"):
                entries.add(rest)
    lines = []
    for entry in sorted(entries):
        label = entry.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")
        label = label.replace("\n", " ").replace("\r", " ")
        lines.append(f"- [{label}]({quote(entry, safe='/._-')})\n")
    return "".join(lines)


def managed_findings(map_path: str, text: str, paths: list[str], required: bool) -> list[str]:
    """Reject malformed or stale managed blocks in the staged map."""
    fence: tuple[str, int] | None = None
    for line in text.splitlines():
        trimmed = line.lstrip(" ")
        indent = len(line) - len(trimmed)
        character = trimmed[:1]
        run = len(trimmed) - len(trimmed.lstrip(character)) if character in ("`", "~") else 0
        delimiter = indent <= 3 and run >= 3
        if fence is not None:
            if START in line or END in line:
                return [f"{map_path}: contents markers inside a code fence need the Rust checker"]
            closing = delimiter and character == fence[0] and run >= fence[1]
            if closing and not trimmed[run:].strip():
                fence = None
        elif delimiter:
            fence = (character, run)
    starts = text.count(START)
    ends = text.count(END)
    if starts == 0 and ends == 0:
        return [f"{map_path}: managed contents block required"] if required else []
    if starts != 1 or ends != 1:
        return [f"{map_path}: malformed contents markers"]
    before, remainder = text.split(START, 1)
    between, after = remainder.split(END, 1)
    if (before and not before.endswith("\n")) or (after and not after.startswith("\n")):
        return [f"{map_path}: contents marker must occupy its own line"]
    if between != f"\n{inventory(paths, map_path)}":
        return [f"{map_path}: managed contents differ from staged inventory"]
    return []


def reference_findings(map_path: str, text: str) -> list[str]:
    """Refuse defined reference links that the retained scanner does not check."""
    findings: list[str] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        start = REFERENCE_START.match(line)
        if start is None:
            continue
        label = "\n".join(lines[index:])[:1200]
        escaped = False
        for offset, character in enumerate(label[start.end() :], start.end()):
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == "]":
                if label[offset + 1 :].startswith(":"):
                    findings.append(
                        f"{map_path}:{index + 1}: reference links require the Rust checker"
                    )
                break
    return findings


def boundary_findings(map_path: str, text: str) -> list[str]:
    """Reject staged inline links that leave the repository after URL decoding."""
    findings: list[str] = []
    parent = list(Path(map_path).parts[:-1])
    for number, line in enumerate(text.splitlines(), 1):
        visible = sync_map_md.CODE_SPAN_PATTERN.sub("", line)
        matches = list(sync_map_md.LINK_PATTERN.finditer(visible))
        recognized = sum(match.group().count("](") for match in matches)
        if visible.count("](") > recognized:
            findings.append(f"{map_path}:{number}: link syntax requires the Rust checker")
        for target in (match.group(1) for match in matches):
            if sync_map_md._is_external(target):
                continue
            local = sync_map_md._local_target(target)
            if "\\" in local or "&" in local:
                findings.append(f"{map_path}:{number}: link escapes need the Rust checker")
                continue
            try:
                decoded = unquote_to_bytes(local).decode("utf-8")
            except UnicodeError:
                findings.append(f"{map_path}:{number}: invalid UTF-8 link: {local}")
                continue
            if decoded.startswith("/"):
                findings.append(f"{map_path}:{number}: absolute link: {local}")
                continue
            components = parent.copy()
            for part in decoded.split("/"):
                if part == "..":
                    if not components:
                        findings.append(f"{map_path}:{number}: link escapes repository: {local}")
                        break
                    components.pop()
                elif part not in ("", "."):
                    components.append(part)
    return findings


def check(repo: Path, directory: str | None, require_managed: bool, strict: bool) -> int:
    """Run the legacy map checks over a shadow of the active index."""
    entries = staged_entries(repo)
    modes = {name: mode for name, mode, _ in entries}
    paths = sorted(name for name, _, _ in entries)
    maps = [name for name in paths if Path(name).name == "map.md"]
    if directory is not None:
        unsafe_component = any(part in ("", ".", "..") for part in directory.split("/"))
        if Path(directory).is_absolute() or (unsafe_component and directory != "."):
            raise ValueError("unsafe directory selection")
        selected = "map.md" if directory == "." else f"{directory}/map.md"
        maps = [selected]
    if not maps:
        raise ValueError("no staged map.md found")
    findings: list[str] = []
    with tempfile.TemporaryDirectory(prefix="repark-map-index-") as temporary:
        shadow = Path(temporary)
        materialize(repo, shadow, entries)
        for map_path in maps:
            source = shadow / map_path
            if modes.get(map_path) not in ("100644", "100755") or not source.is_file():
                findings.append(f"{map_path}: staged map is missing or is not a regular file")
                continue
            text = source.read_text(encoding="utf-8")
            findings.extend(sync_map_md.check_map(source, paths, map_path, strict))
            findings.extend(managed_findings(map_path, text, paths, require_managed))
            findings.extend(reference_findings(map_path, text))
            findings.extend(boundary_findings(map_path, text))
    for finding in findings:
        print(finding, file=sys.stderr)
    if findings:
        print(f"map-index-fallback: FAIL — {len(findings)} finding(s)", file=sys.stderr)
        return 1
    print(f"map-index-fallback: {len(maps)} staged maps clean")
    return 0


def main() -> int:
    """Accept only the read-only staged-map command used by hooks."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--snapshot", choices=["index"], required=True)
    parser.add_argument("command", choices=["maps"])
    parser.add_argument("--check", action="store_true", required=True)
    parser.add_argument("--require-managed", action="store_true")
    parser.add_argument("--path")
    parser.add_argument("--strict", action="store_true")
    arguments = parser.parse_args()
    try:
        return check(arguments.repo, arguments.path, arguments.require_managed, arguments.strict)
    except (OSError, UnicodeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"map-index-fallback: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
