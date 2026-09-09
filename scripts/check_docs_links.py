from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ALLOWLIST_PATH = "scripts/docs_links_allowlist.txt"
LEDGERS_PREFIX = "task/ledgers/"
EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "ftp://")
DESCRIPTION = (
    "Check every tracked markdown file: each relative link resolves to a tracked file, an"
    " anchor matches the target's GitHub-style heading slug, and docs: evidence cells under"
    " task/ledgers follow the same rule. Exit 0 clean, 1 findings, 2 environment error."
)
MISSING = "does not exist"
UNTRACKED = "exists but is not tracked"
ESCAPED = "resolves outside the repository"

LINK_PATTERN = re.compile(
    r"\[[^\]]*\]\(\s*(<[^<>]*>|(?:[^()\s]|\([^()]*\))*)(?:\s+\"[^\"]*\")?\s*\)"
)
CODE_SPAN_PATTERN = re.compile(r"`[^`]*`")
FENCE_PATTERN = re.compile(r"^\s*(?:```|~~~)")
HEADING_PATTERN = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$")
DOCS_CELL_PATTERN = re.compile(r"(?<![\w./-])docs:\s*([^\s|`]+)")
ALLOWLIST_ENTRY = re.compile(r"[^\s:]+:\d+:\S+")


def tracked_paths(repo: Path) -> list[str]:
    """Every path git tracks, repo-relative posix, sorted."""
    completed = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"], capture_output=True, check=True, text=True
    )
    return sorted(entry for entry in completed.stdout.split("\0") if entry)


def load_allowlist(repo: Path) -> frozenset[str]:
    """The seeded path:line:link entries; blank lines and #-prefixed header lines are ignored."""
    path = repo / ALLOWLIST_PATH
    if not path.exists():
        return frozenset()
    entries: set[str] = set()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        if ALLOWLIST_ENTRY.fullmatch(entry) is None:
            raise ValueError(f"{ALLOWLIST_PATH}:{number}: malformed allowlist entry {entry!r}")
        entries.add(entry)
    return frozenset(entries)


def slug_text(text: str) -> str:
    """The GitHub heading slug: lower-case, spaces to hyphens, punctuation dropped."""
    parts: list[str] = []
    for char in text.lower():
        if char.isspace():
            parts.append("-")
        elif char.isalnum() or char in "-_":
            parts.append(char)
    return "".join(parts)


def heading_anchors(path: Path) -> frozenset[str]:
    """Every GitHub anchor of the file's ATX headings outside fenced blocks, duplicates suffixed."""
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    in_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if FENCE_PATTERN.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = HEADING_PATTERN.match(line)
        if match is None:
            continue
        slug = slug_text(match.group(1))
        seen = counts.get(slug, 0)
        counts[slug] = seen + 1
        anchors.add(slug if seen == 0 else f"{slug}-{seen}")
    return frozenset(anchors)


def resolve_target(repo: Path, base: Path, local: str) -> tuple[str | None, bool, bool]:
    """The target's repo-relative posix path, whether it exists, whether it is a directory."""
    resolved = (base / local).resolve()
    try:
        relative = resolved.relative_to(repo.resolve()).as_posix()
    except ValueError:
        return None, False, False
    return relative, resolved.exists(), resolved.is_dir()


def local_target(target: str) -> str:
    """The on-disk part of a link target: angle brackets and title stripped."""
    cleaned = target.strip()
    if cleaned.startswith("<") and cleaned.endswith(">"):
        cleaned = cleaned[1:-1].strip()
    else:
        cleaned = cleaned.split(" ", 1)[0]
    return cleaned


def _is_tracked_target(
    relative: str, is_directory: bool, tracked: frozenset[str], tracked_list: list[str]
) -> bool:
    """Whether git tracks the target: a file by name, a directory by anything beneath it."""
    if not is_directory:
        return relative in tracked
    prefix = f"{relative}/"
    return any(entry.startswith(prefix) for entry in tracked_list)


def scan_file(
    repo: Path,
    relative: str,
    tracked: frozenset[str],
    tracked_list: list[str],
    anchors: dict[str, frozenset[str]],
) -> tuple[list[tuple[int, str, str]], int]:
    """(line, raw target, reason) findings for one tracked markdown file, and links checked."""
    findings: list[tuple[int, str, str]] = []
    checked = 0
    source = repo / relative
    in_fence = False
    for number, line in enumerate(source.read_text(encoding="utf-8").splitlines(), 1):
        if FENCE_PATTERN.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        stripped = CODE_SPAN_PATTERN.sub("", line)
        for raw in LINK_PATTERN.findall(stripped):
            local = local_target(raw)
            fragment = ""
            if "#" in local:
                local, fragment = local.split("#", 1)
            if not local:
                continue
            if local.lower().startswith(EXTERNAL_PREFIXES):
                continue
            checked += 1
            posix, exists, is_directory = resolve_target(repo, source.parent, local)
            if posix is None:
                findings.append((number, raw, ESCAPED))
                continue
            if not exists:
                findings.append((number, raw, MISSING))
                continue
            if not _is_tracked_target(posix, is_directory, tracked, tracked_list):
                findings.append((number, raw, UNTRACKED))
                continue
            if fragment and posix.endswith(".md"):
                if posix not in anchors:
                    anchors[posix] = heading_anchors(repo / posix)
                if fragment not in anchors[posix]:
                    findings.append((number, raw, f"anchor #{fragment} does not match a heading"))
        if relative.startswith(LEDGERS_PREFIX):
            for match in DOCS_CELL_PATTERN.finditer(stripped):
                token = match.group(1).rstrip(".,;:!?")
                if "<" in token or ">" in token:
                    continue
                checked += 1
                local, fragment = token.partition("#")[0], token.partition("#")[2]
                posix, exists, is_directory = resolve_target(repo, repo, local)
                if posix is None:
                    findings.append((number, token, ESCAPED))
                    continue
                if not exists:
                    findings.append((number, token, MISSING))
                    continue
                if not _is_tracked_target(posix, is_directory, tracked, tracked_list):
                    findings.append((number, token, UNTRACKED))
                    continue
                if fragment and posix.endswith(".md"):
                    if posix not in anchors:
                        anchors[posix] = heading_anchors(repo / posix)
                    if fragment not in anchors[posix]:
                        findings.append(
                            (number, token, f"anchor #{fragment} does not match a heading")
                        )
    return findings, checked


def run(repo: Path) -> int:
    """Exit 0 with the counts of files and links checked, 1 with one line per broken link."""
    try:
        allowlist = load_allowlist(repo)
        tracked_list = tracked_paths(repo)
        tracked = frozenset(tracked_list)
        documents = [path for path in tracked_list if path.endswith(".md")]
        anchors: dict[str, frozenset[str]] = {}
        broken: list[str] = []
        checked = 0
        for relative in documents:
            findings, links = scan_file(repo, relative, tracked, tracked_list, anchors)
            checked += links
            for number, raw, reason in findings:
                if f"{relative}:{number}:{raw}" in allowlist:
                    continue
                broken.append(f"{relative}:{number}: {raw} -> {reason}")
    except (subprocess.CalledProcessError, OSError, UnicodeDecodeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    if broken:
        for line in broken:
            print(line, file=sys.stderr)
        print(f"docs-links: FAIL — {len(broken)} broken link(s)", file=sys.stderr)
        return 1
    print(f"docs-links: {len(documents)} files, {checked} links checked — clean")
    return 0


def main() -> int:
    """CLI entry point; --repo is a hidden override the provocation tests use."""
    parser = argparse.ArgumentParser(prog="check_docs_links.py", description=DESCRIPTION)
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent, help=argparse.SUPPRESS
    )
    arguments = parser.parse_args()
    return run(arguments.repo.resolve())


if __name__ == "__main__":
    sys.exit(main())
