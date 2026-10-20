#!/usr/bin/env python3
"""Preserve the owner rulings (no code comments; Opus 5.0 banned) and the enforcement boundary."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

EXPECTED_RULING = (
    "# OWNER RULING (2026-08-26) — NO CODE COMMENTS FROM ANTHROPIC MODELS\n\n"
    "**EFFECTIVE IMMEDIATELY. ALL ANTHROPIC MODELS ARE HEREBY BANNED FROM "
    "MAKING COMMENTS IN THE CODE. THIS INCLUDES, FABLE, OPUS, SONNET AND HAIKU**\n\n"
    "**AGAIN, IF YOU ARE FABLE, OPUS, SONNET OR HAIKU, DO NOT WRITE ANY COMMENTS**<br>\n"
    "**IF YOU ARE FABLE, DO NOT WRITE ANY COMMENTS**<br>\n"
    "**IF YOU ARE OPUS, DO NOT WRITE ANY COMMENTS**<br>\n"
    "**IF YOU ARE SONNET, DO NOT WRITE ANY COMMENTS**<br>\n"
    "**IF YOU ARE HAIKU, DO NOT WRITE ANY COMMENTS**<br>\n\n"
    "**THIS INCLUDES ANY MODEL VERSION, EXAMPLE BEING OPUS 4.8 OR OPUS 5, EITHER ONE IS "
    "BANNED, IT DOESN'T MATTER**\n\n"
    "*Adjustment (owner, 2026-08-26, same day):* the ban is on comments **in code** — Rust, "
    "Python, shell,\n"
    "TOML, YAML and every other source file. **Markdown files may carry comments and explanatory "
    "prose**;\n"
    "that is where a reason, a design note or a `pins: <unit>/C-NNN` citation now lives — the\n"
    "directory's `map.md` (the ledger-grammar gate reads every tracked file under `crates/`,\n"
    "`python/`, `scripts/`, so a citation in a `map.md` there counts). Condensation is "
    "held by review,\n"
    "not by a gate: the `check-comment-density` ratchet was dropped before #247 merged; the "
    "file-size\n"
    "ratchets are the mechanical gates, and a new file carries no comments."
)

EXPECTED_BOUNDARY = (
    "Authorship is undetectable; review holds this rule. The gate preserves bytes. Required\n"
    "docstrings, Rust banners, and invariant comments remain. No sweep."
)

EXPECTED_RULING_OPUS = (
    "# OWNER RULING (2026-10-17) — OPUS 5.0 IS BANNED FOREVER; OPUS MEANS OPUS 5.5\n"
    "\n"
    "**EFFECTIVE IMMEDIATELY AND PERMANENTLY. NO AGENT, LANE, EXECUTOR, VERIFIER, SKETCHER "
    "OR CLERK RUNS ON OPUS 5.0. EVERY OPUS DISPATCH IS CLAUDE OPUS 5.5 (`claude-opus-5-5`) "
    "AND NOTHING ELSE.**\n"
    "\n"
    "*Mechanics (owner, 2026-10-17, same night):* a brief, trailer, workflow, launcher or "
    "skill that names an\n"
    "Opus model names `claude-opus-5-5`; the bare alias `opus` is allowed only where the "
    "tool resolves it to\n"
    "that id, and the resolution is verified from the agent transcript, never from the "
    "brief. Two gates hold\n"
    "it: `make check-model-ban` fails any tracked file that names a 5.0 Opus id, and `make "
    "check-owner-ruling`\n"
    "preserves this block byte-for-byte in `AGENTS.md` and `CLAUDE.md`."
)

DOCUMENT_TITLES = {
    "AGENTS.md": "# AGENTS.md — the authoritative contributor contract",
    "CLAUDE.md": "# CLAUDE.md — the Claude adapter (not authoritative)",
}


def expected_prefix(title: str) -> str:
    """Return the exact document prefix through the owner ruling."""
    return f"{title}\n\n{EXPECTED_RULING}\n\n"


def findings(repo: Path) -> list[str]:
    """Return every owner-ruling preservation or compatibility finding."""
    found: list[str] = []
    for relative_path, title in DOCUMENT_TITLES.items():
        path = repo / relative_path
        if path.is_symlink():
            found.append(f"{relative_path}: must be a regular file, not a symlink")
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            found.append(f"{relative_path}: cannot read: {error}")
            continue
        if not text.startswith(expected_prefix(title)):
            found.append(f"{relative_path}: owner ruling is not byte-exact at the document start")
        elif text.count(EXPECTED_RULING) != 1:
            found.append(f"{relative_path}: owner ruling must appear exactly once")
        if relative_path == "CLAUDE.md":
            opus_prefix = f"{expected_prefix(title)}{EXPECTED_RULING_OPUS}\n\n"
            if text.startswith(expected_prefix(title)) and not text.startswith(opus_prefix):
                found.append(
                    "CLAUDE.md: Opus 5.0 ban ruling is missing or changed after the comment ruling"
                )
            elif text.count(EXPECTED_RULING_OPUS) != 1:
                found.append("CLAUDE.md: Opus 5.0 ban ruling must appear exactly once")
    agents_path = repo / "AGENTS.md"
    if agents_path.is_file():
        agents_text = agents_path.read_text(encoding="utf-8")
        ruling_prefix = expected_prefix(DOCUMENT_TITLES["AGENTS.md"])
        boundary_prefix = f"{ruling_prefix}{EXPECTED_BOUNDARY}\n\n"
        if agents_text.startswith(ruling_prefix) and not agents_text.startswith(boundary_prefix):
            found.append("AGENTS.md: enforcement boundary is missing or changed")
        elif agents_text.count(EXPECTED_BOUNDARY) != 1:
            found.append("AGENTS.md: enforcement boundary must appear exactly once")
        opus_prefix = f"{boundary_prefix}{EXPECTED_RULING_OPUS}\n\n"
        if agents_text.startswith(boundary_prefix) and not agents_text.startswith(opus_prefix):
            found.append(
                "AGENTS.md: Opus 5.0 ban ruling is missing or changed after the enforcement "
                "boundary"
            )
        elif agents_text.count(EXPECTED_RULING_OPUS) != 1:
            found.append("AGENTS.md: Opus 5.0 ban ruling must appear exactly once")
    return found


def main() -> int:
    """Check the repository and return a process exit status."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    arguments = parser.parse_args()
    found = findings(arguments.repo.resolve())
    if found:
        for finding in found:
            print(finding, file=sys.stderr)
        print(f"owner-ruling: FAIL — {len(found)} finding(s)", file=sys.stderr)
        return 1
    print("owner-ruling: exact rulings and enforcement boundary present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
