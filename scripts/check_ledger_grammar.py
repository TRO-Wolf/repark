#!/usr/bin/env python3
"""Check the shape of live ledgers so the Scope Auditor and the Critic do not have to.

Scope: every tracked `*-ledger.md` under `task/ledgers/staging/` (the archive is
immutable by the DL-1 rule, so there is never a retrofit debt). Three rules,
chartered by DL-2 (2026-08-23) on top of the SEPMO skill's meanings:

A. **Clause rows.** A clause table is any markdown table whose data rows begin
   with `| C-NNN |`. Each row: the id is unique in the ledger; exactly one cell
   is a verdict — `PROVEN`, `OPEN` or `REJECTED`, bold allowed, optionally
   followed by a parenthetical note; at least one further cell is non-empty
   (the evidence or proof obligation). Whether an `OPEN` row really carries the
   question that would close it, and whether a quantified clause is enumerated,
   are the Scope Auditor's readings — measured on the live charters and left to
   the skill, not faked by a regex.

B. **Pin binding.** A test cites a clause with `pins: <unit>/C-NNN[, C-MMM...]`
   where `<unit>` is the ledger's filename without `-ledger.md` (and, in the
   archive, without its `yyyy-mm-dd-` prefix). Every `PROVEN` clause in a
   staging ledger must be cited at least once — the measured floor is seeded
   per ledger in EXCEPTIONS and only ratchets down — and every citation must
   resolve to a clause that exists in `staging/` or the archive.

C. **Attestation form.** A `COVERAGE_ATTESTATION:` block (ref 05's shape, in a
   fenced block) lists `AT-1`..`AT-10` exactly once each; `ATTACKED` needs a
   non-empty `artifacts:` list, `N/A` a `justification:`; `complete:` is `true`
   iff every category satisfies that. A staging ledger not listed in EXCEPTIONS
   must carry a clause table (the SEPMO gate rule: scope is a ledger of
   propositions before any work) and, once none of its clauses is `OPEN`, the
   attestation — it is the Critic's artifact, filed after the Actor's work. A
   `FINDING:` record, where present, carries
   `id`, a severity in S0..S3, a category in AT-1..AT-10, a clause list and a
   disposition from ref 05's enumeration.

The meanings stay in `skills/sepmo/` (SKILL.md "The gate is a ledger, not a
score"; references/05-critic.md); this script owns only the shape. Exit 0 clean,
1 findings, 2 usage or environment error.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

STAGING = "task/ledgers/staging"
ARCHIVE = "task/ledgers/archive"
LEDGER_SUFFIX = "-ledger.md"
# Where tests live; `pins:` citations are read from every tracked file here.
CITATION_ROOTS: tuple[str, ...] = ("crates/", "python/", "scripts/")

# Ledger -> (unpinned PROVEN clauses allowed, attestation block required).
# Seeded 2026-08-23 from the measured floor (DL-2 ledger §1): the three live
# charters predate the pin convention and the checked attestation. Ceilings
# ratchet DOWN only; a row is deleted when it reaches zero and the block is
# filed. A ledger not listed allows zero and must file its attestation.
EXCEPTIONS: dict[str, tuple[int, bool]] = {
    "fnp-0-charter-ledger.md": (12, False),
    "mw-0-charter-ledger.md": (10, False),
    "sem-0-charter-ledger.md": (9, False),
    "v3-0-charter-ledger.md": (0, False),
}

VERDICTS: frozenset[str] = frozenset({"PROVEN", "OPEN", "REJECTED"})
CLAUSE_ROW = re.compile(r"^\|\s*(C-\d{3})\s*\|(.*)$")
VERDICT_CELL = re.compile(r"^\**(PROVEN|OPEN|REJECTED)\**(?:\s*\(.*\))?$")
CITATION = re.compile(r"pins:\s*([a-z0-9][a-z0-9.-]*)/(C-\d{3}(?:\s*,\s*C-\d{3})*)")
ARCHIVE_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}-")
FENCE = re.compile(r"^\s*(```|~~~)")
CATEGORIES: tuple[str, ...] = tuple(f"AT-{n}" for n in range(1, 11))
SEVERITIES: frozenset[str] = frozenset({"S0", "S1", "S2", "S3"})
DISPOSITIONS: tuple[str, ...] = ("OPEN", "REMEDIATED", "ACCEPTED_FLAGGED", "DISPUTED")


def tracked_paths(repo: Path) -> list[str]:
    """Every path git tracks, repo-relative posix, sorted."""
    completed = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"], capture_output=True, check=True, text=True
    )
    return sorted(entry for entry in completed.stdout.split("\0") if entry)


def unit_of(path: str) -> str:
    """The citation key of a ledger: its basename without suffix and archive date."""
    name = Path(path).name[: -len(LEDGER_SUFFIX)]
    if path.startswith(ARCHIVE + "/"):
        name = ARCHIVE_PREFIX.sub("", name)
    return name


def _cells(rest: str) -> list[str]:
    """Cells after the id cell, stripped; the empty cell from the closing pipe dropped."""
    cells = [cell.strip() for cell in rest.split("|")]
    if cells and cells[-1] == "":
        cells.pop()
    return cells


def clause_rows(text: str) -> list[tuple[int, str, list[str]]]:
    """(line number, id, other cells) for every clause row outside fenced blocks."""
    rows: list[tuple[int, str, list[str]]] = []
    in_fence = False
    for number, line in enumerate(text.splitlines(), start=1):
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = CLAUSE_ROW.match(line)
        if match:
            rows.append((number, match.group(1), _cells(match.group(2))))
    return rows


def check_rows(
    path: str, rows: list[tuple[int, str, list[str]]]
) -> tuple[list[str], dict[str, str]]:
    """Rule A over one ledger: findings, and id -> verdict for the rows that have one."""
    findings: list[str] = []
    verdicts: dict[str, str] = {}
    for number, clause_id, cells in rows:
        where = f"{path}:{number}: {clause_id}"
        if clause_id in verdicts:
            findings.append(f"{where} duplicate clause id")
        matches = [VERDICT_CELL.match(cell) for cell in cells]
        verdict_cells = [match.group(1) for match in matches if match]
        if len(verdict_cells) != 1:
            allowed = "/".join(sorted(VERDICTS))
            findings.append(
                f"{where} needs exactly one verdict cell ({allowed}), found {len(verdict_cells)}"
            )
            continue
        verdict = verdict_cells[0]
        verdicts[clause_id] = verdict
        others = [cell for cell in cells if not VERDICT_CELL.match(cell) and cell]
        if len(others) < 2:
            findings.append(f"{where} needs the clause text and at least one evidence cell")
    return findings, verdicts


def citations(repo: Path, tracked: list[str]) -> dict[tuple[str, str], list[str]]:
    """(unit, clause id) -> the files citing it, from every tracked file under CITATION_ROOTS."""
    found: dict[tuple[str, str], list[str]] = {}
    for path in tracked:
        if not path.startswith(CITATION_ROOTS):
            continue
        try:
            text = (repo / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for match in CITATION.finditer(text):
            unit = match.group(1)
            for clause_id in re.findall(r"C-\d{3}", match.group(2)):
                found.setdefault((unit, clause_id), []).append(path)
    return found


def _block_lines(text: str, marker: str) -> list[list[str]]:
    """The lines of every fenced block that starts with `marker`."""
    blocks: list[list[str]] = []
    current: list[str] | None = None
    for line in text.splitlines():
        if FENCE.match(line):
            if current is not None:
                blocks.append(current)
                current = None
            else:
                current = []
            continue
        if current is not None:
            current.append(line)
    return [block for block in blocks if block and block[0].strip().startswith(marker)]


def check_attestation(path: str, text: str, required: bool) -> list[str]:
    """Rule C over one ledger."""
    blocks = _block_lines(text, "COVERAGE_ATTESTATION:")
    findings: list[str] = []
    if not blocks:
        if required:
            findings.append(
                f"{path}: no COVERAGE_ATTESTATION block (ref 05 shape, in a fenced block)"
            )
        return findings
    if len(blocks) > 1:
        findings.append(f"{path}: {len(blocks)} COVERAGE_ATTESTATION blocks; file one")
    block = blocks[0]
    entries: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    complete_claim: str | None = None
    for raw in block:
        line = raw.strip()
        if line.startswith("- id:"):
            current = {"id": line.split(":", 1)[1].strip()}
            entries.setdefault(current["id"], current)
            continue
        if line.startswith("complete:"):
            complete_claim = line.split(":", 1)[1].split("#", 1)[0].strip()
            current = None
            continue
        if current is not None and ":" in line and not line.startswith("-"):
            key, value = line.split(":", 1)
            current[key.strip()] = value.split("#", 1)[0].strip()
    satisfied = True
    for category in CATEGORIES:
        entry = entries.get(category)
        if entry is None:
            findings.append(f"{path}: attestation lacks {category}")
            satisfied = False
            continue
        status = entry.get("status", "")
        if status == "ATTACKED":
            if entry.get("artifacts", "") in ("", "[]"):
                findings.append(f"{path}: {category} ATTACKED without artifacts")
                satisfied = False
        elif status == "N/A":
            if not entry.get("justification"):
                findings.append(f"{path}: {category} N/A without justification")
                satisfied = False
        else:
            findings.append(f"{path}: {category} status must be ATTACKED or N/A, found {status!r}")
            satisfied = False
    for extra in sorted(set(entries) - set(CATEGORIES)):
        findings.append(f"{path}: attestation names unknown category {extra}")
    if complete_claim is None:
        findings.append(f"{path}: attestation has no `complete:` line")
    elif (complete_claim == "true") != satisfied:
        findings.append(
            f"{path}: attestation says complete: {complete_claim} "
            f"but the categories say {str(satisfied).lower()}"
        )
    return findings


def check_findings(path: str, text: str) -> list[str]:
    """Rule C's second half: every FINDING record has the ref 05 fields."""
    findings: list[str] = []
    for block in _block_lines(text, "FINDING:"):
        fields: dict[str, str] = {}
        for raw in block[1:]:
            line = raw.strip()
            if ":" in line and not line.startswith("-") and not line.startswith("|"):
                key, value = line.split(":", 1)
                fields[key.strip()] = value.split("#", 1)[0].strip()
        ident = fields.get("id", "<no id>")
        where = f"{path}: FINDING {ident}"
        if "id" not in fields:
            findings.append(f"{where} has no id")
        if fields.get("severity") not in SEVERITIES:
            findings.append(f"{where} severity must be S0..S3")
        if fields.get("category") not in CATEGORIES:
            findings.append(f"{where} category must be AT-1..AT-10")
        if not re.search(r"C-\d{3}", fields.get("clause", "")):
            findings.append(f"{where} names no charter clause (orphan work under D5)")
        disposition = fields.get("disposition", "")
        if not disposition.startswith(DISPOSITIONS):
            findings.append(f"{where} disposition must start with one of {', '.join(DISPOSITIONS)}")
    return findings


def run(repo: Path) -> int:
    """All three rules over the staging ledgers; the summary line; the exit code."""
    try:
        tracked = tracked_paths(repo)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"ERROR: cannot list tracked files ({error})", file=sys.stderr)
        return 2
    staging = [p for p in tracked if p.startswith(STAGING + "/") and p.endswith(LEDGER_SUFFIX)]
    archived = [p for p in tracked if p.startswith(ARCHIVE + "/") and p.endswith(LEDGER_SUFFIX)]
    if not staging and not archived:
        print("ERROR: no ledgers found under task/ledgers — refuse to pass closed", file=sys.stderr)
        return 2

    findings: list[str] = []
    known: set[tuple[str, str]] = set()
    proven: dict[str, list[str]] = {}
    clauses = 0
    for path in staging + archived:
        text = (repo / path).read_text(encoding="utf-8")
        rows = clause_rows(text)
        unit = unit_of(path)
        if path in staging:
            row_findings, verdicts = check_rows(path, rows)
            findings.extend(row_findings)
            clauses += len(rows)
            proven[path] = [cid for cid, verdict in verdicts.items() if verdict == "PROVEN"]
            name = Path(path).name
            ceiling, governed = EXCEPTIONS.get(name, (0, True))
            if governed and not rows:
                findings.append(f"{path}: no clause table (a ledger is propositions first)")
            required = governed and bool(rows) and "OPEN" not in verdicts.values()
            findings.extend(check_attestation(path, text, required))
            findings.extend(check_findings(path, text))
        else:
            verdicts = {cid: "" for _n, cid, _c in rows}
        known.update((unit, cid) for cid in verdicts)

    cited = citations(repo, tracked)
    for (unit, clause_id), files in sorted(cited.items()):
        if (unit, clause_id) not in known:
            findings.append(f"{files[0]}: `pins: {unit}/{clause_id}` names no clause in any ledger")
    for path, proven_ids in sorted(proven.items()):
        unit = unit_of(path)
        unpinned = [cid for cid in proven_ids if (unit, cid) not in cited]
        ceiling = EXCEPTIONS.get(Path(path).name, (0, True))[0]
        if len(unpinned) > ceiling:
            findings.append(
                f"{path}: {len(unpinned)} PROVEN clause(s) with no `pins: {unit}/C-NNN` citation "
                f"(ceiling {ceiling}): {', '.join(unpinned[: max(1, len(unpinned) - ceiling)])}"
            )
        elif ceiling and len(unpinned) < ceiling:
            findings.append(
                f"{path}: ceiling {ceiling} is above the measured {len(unpinned)} — "
                "ratchet it down in EXCEPTIONS"
            )
    for name in sorted(EXCEPTIONS):
        if f"{STAGING}/{name}" not in staging:
            findings.append(f"EXCEPTIONS names {name}, which is not in {STAGING}/ — delete the row")

    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        print(f"ledger-grammar: FAIL — {len(findings)} finding(s)", file=sys.stderr)
        return 1
    print(
        f"ledger-grammar: {len(staging)} staging ledgers clean ({clauses} clauses, "
        f"{len(cited)} pinned clause ids, {len(EXCEPTIONS)} exception rows)"
    )
    return 0


def main() -> int:
    """CLI: `--repo` only (hidden), for the provocation tests."""
    parser = argparse.ArgumentParser(
        prog="check_ledger_grammar.py", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent, help=argparse.SUPPRESS
    )
    arguments = parser.parse_args()
    return run(arguments.repo.resolve())


if __name__ == "__main__":
    sys.exit(main())
