#!/usr/bin/env python3
"""Check the shape of live ledgers so the Scope Auditor and the Critic do not have to.

Scope: every tracked `*-ledger.md` under `task/ledgers/staging/` and `completed/`; the archive's
ledgers are read for their clause ids and its sidecars for their bindings. Three rules, on top of
the SEPMO skill's meanings:

A. **Clause rows.** A clause table is any markdown table whose data rows begin with
   `| C-NNN |`. Each row: the id is unique in the ledger; exactly one cell is a verdict —
   `PROVEN`, `OPEN` or `REJECTED`, bold allowed, optionally followed by a parenthetical note;
   at least one further cell is non-empty (the evidence or proof obligation). Whether an `OPEN`
   row really carries the closing question, and whether a quantified clause is enumerated, are
   the Scope Auditor's readings, not faked by a regex.

B. **Pin binding.** A clause is bound by a line `C-NNN: <entry>[, <entry>...]` in its unit's
   sidecar `<unit>-pins.md`, which counts only beside its own ledger (same bin, same name; in
   the archive with the ledger's `yyyy-mm-dd-` prefix). `<unit>` is the ledger's filename
   without `-ledger.md` and without the archive prefix. An entry is `<path>` or
   `<path>::<name>`: the path is a tracked `.rs`, `.py` or `.sh` file that is a script under
   `scripts/` or a test by place or name (under a `tests/` directory, `tests.rs`, `*_tests.rs`,
   `test_*.py`, `*_test.py`) that carries a test (a Python `test_*` function, or a
   `pytest.fixture` / `fixture` / `pytest.mark.parametrize` / `parametrize` decorator; a Rust
   `#[test]`, `#[tokio::test]` or `#[rstest]`), never a fixture, a helper, oracle or recorder
   (`_record*`, or a test-placed file with no test), a map, a ledger, a sidecar, a licence or a
   readme; the name is a definition the file makes, read by parsing (Python: module- and
   class-level functions, classes and the names an assignment binds, never a subscript or
   attribute target; Rust: line-start fn, struct, enum, trait, type, const, static, mod, union
   and macro_rules with comments (nested block comments included) and literals blanked; shell:
   functions outside heredocs, whatever their delimiter quoting, and quoted strings, which may
   span lines). The line `C-NNN: none (records)` binds a clause that no test pins; the gate
   counts and lists those clauses. A `pins: <unit>/C-NNN[, C-MMM...]` citation also binds when
   it sits in a tracked test file or script under CITATION_ROOTS; one in a map or in production
   source does not. Every `PROVEN` clause in a staging or completed ledger must be bound at
   least once — the measured floor is seeded per ledger in EXCEPTIONS and only ratchets down — a
   sidecar line counts only for the ledger beside it and must name a clause of that ledger, and
   a source citation must resolve to a clause that exists in any bin (`staging/`, `completed/`,
   the archive). A staging or completed ledger whose first 40 lines carry a `**Path:**` field
   valued `READING` outside a code span is a reading unit: rule B does not apply to its clauses,
   rules A and C still do.

C. **Attestation form.** A `COVERAGE_ATTESTATION:` block (ref 05's shape, in a fenced block)
   lists `AT-1`..`AT-10` exactly once each; `ATTACKED` needs a non-empty `artifacts:` list,
   `N/A` a `justification:`; `complete:` is `true` iff every category satisfies that. A
   staging or completed ledger not listed in EXCEPTIONS must carry a clause table and, once no
   clause is `OPEN`, the attestation. A `FINDING:` record, where present, carries `id`, a
   severity in S0..S3, a category in AT-1..AT-10, a clause list and a disposition from ref 05's
   enumeration.

The meanings stay in `.agents/skills/sepmo/` (SKILL.md "The gate is a ledger, not a score";
references/05-critic.md); this script owns only the shape. `--report row-named-records`
also lists the records lines whose row backticks a test that bindable files define. Exit 0
clean, 1 findings, 2 usage or environment error.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

STAGING = "task/ledgers/staging"
COMPLETED = "task/ledgers/completed"
ARCHIVE = "task/ledgers/archive"
LEDGER_SUFFIX = "-ledger.md"
PINS_SUFFIX = "-pins.md"
MAP_NAME = "map.md"
RECORDS = "none (records)"
TEST_NAMES = re.compile(r"^(?:tests\.rs|.+_tests\.rs|test_.+\.py|.+_test\.py)$")
CODE_SUFFIXES: frozenset[str] = frozenset({".rs", ".py", ".sh"})
RUST_COMMENT_MARK = re.compile(r"/\*|\*/")
RUST_MACRO_BODY = re.compile(r"macro_rules![ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t\n]*[{(\[]")
RUST_CFG_NEVER = re.compile(r"#\[cfg\(any\(\)\)\]")
RUST_TOKEN = re.compile(
    r"//[^\n]*|/\*|\*/"
    r'|(?<![A-Za-z0-9_])[bc]?r(#*)".*?"\1'
    r'|(?<![A-Za-z0-9_])[bc]?"(?:\\.|[^"\\])*"'
    r"|(?<![A-Za-z0-9_])b?'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]{1,6}\}|.)|[^\\'\n])'",
    re.DOTALL,
)
RUST_DEFINITION = re.compile(
    r"^[ \t]*(?:pub(?:\([^)\n]*\))?[ \t]+)?"
    r"(?:(?:async|const|unsafe|default|extern(?:[ \t]+\"[^\"\n]*\")?)[ \t]+)*"
    r"(?:fn|struct|enum|trait|type|const|static|mod|union)[ \t]+([A-Za-z_][A-Za-z0-9_]*)"
    r"|^[ \t]*macro_rules![ \t]*([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)
SHELL_DEFINITION = re.compile(
    r"^[ \t]*function[ \t]+([A-Za-z_][A-Za-z0-9_-]*)|^[ \t]*([A-Za-z_][A-Za-z0-9_-]*)[ \t]*\(\)",
    re.MULTILINE,
)
SHELL_HEREDOC = re.compile(r"<<(?!<)-?[ \t]*\\?[\"']?([A-Za-z0-9_][A-Za-z0-9_-]*)[\"']?")
RUST_TEST_ATTRIBUTE = re.compile(r"#\[(?:test|tokio::test|rstest)\b")
PYTHON_TEST_MARKERS = frozenset(
    {"pytest.fixture", "fixture", "pytest.mark.parametrize", "parametrize"}
)
_DEFINITIONS: dict[tuple[str, int], frozenset[str]] = {}
_TEXTS: dict[str, str] = {}
_TEST_NAMES: dict[tuple[str, int], frozenset[str]] = {}
RUST_TEST_FUNCTION = re.compile(
    r"#\[(?:test|tokio::test|rstest)\b[^\]]*\](?:\s*#\[[^\]]*\])*\s*"
    r"(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)"
)
_CARRIES: dict[tuple[str, int], bool] = {}
_STRIPPED: dict[int, str] = {}
UNBINDABLE = re.compile(
    r"^(?:map\.md|.+-ledger\.md|.+-pins\.md|LICEN[CS]E.*|README.*|_record.*)$", re.IGNORECASE
)
CITATION_ROOTS: tuple[str, ...] = ("crates/", "python/", "scripts/")

# Ledger -> (unpinned PROVEN clauses allowed, attestation block required). The listed charters
# predate the pin convention and the checked attestation. Ceilings ratchet DOWN only; a row is
# deleted when it reaches zero and the block is filed. A ledger not listed allows zero and
# must file its attestation.
EXCEPTIONS: dict[str, tuple[int, bool]] = {
    "fnp-0-charter-ledger.md": (12, False),
    "v3-0-charter-ledger.md": (0, False),
}

VERDICTS: frozenset[str] = frozenset({"PROVEN", "OPEN", "REJECTED"})
CLAUSE_ROW = re.compile(r"^\|\s*(C-\d{3})\s*\|(.*)$")
VERDICT_CELL = re.compile(r"^\**(PROVEN|OPEN|REJECTED)\**(?:\s*\(.*\))?$")
CITATION = re.compile(r"pins:\s*([a-z0-9][a-z0-9.-]*)/(C-\d{3}(?:\s*,\s*C-\d{3})*)")
ARCHIVE_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}-")
FENCE = re.compile(r"^\s*(```|~~~)")
READING_FIELD = re.compile(r"\*\*Path:\*\*\s*([A-Za-z0-9_-]+)")
CODE_SPAN = re.compile(r"`[^`]*`")
BINDING_LINE = re.compile(r"^(C-\d{3}): (\S(?:.*\S)?)$")
BINDING_ENTRY = re.compile(r"^([^\s:,]+)(?:::([A-Za-z_][A-Za-z0-9_]*))?$")
HEADER_LINES = 40
CATEGORIES: tuple[str, ...] = tuple(f"AT-{n}" for n in range(1, 11))
SEVERITIES: frozenset[str] = frozenset({"S0", "S1", "S2", "S3"})
DISPOSITIONS: tuple[str, ...] = ("OPEN", "REMEDIATED", "ACCEPTED_FLAGGED", "DISPUTED")


def load_text() -> ModuleType:
    """The map-text helpers beside this script, for the shared code-span reader."""
    location = Path(__file__).resolve().parent / "migrate_pins_text.py"
    spec = importlib.util.spec_from_file_location("migrate_pins_text", location)
    if spec is None or spec.loader is None:
        message = f"cannot load {location}"
        raise RuntimeError(message)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TEXT = load_text()


def tracked_paths(repo: Path) -> list[str]:
    """Every path git tracks, repo-relative posix, sorted."""
    completed = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"], capture_output=True, check=True, text=True
    )
    return sorted(entry for entry in completed.stdout.split("\0") if entry)


def unit_of(path: str, suffix: str = LEDGER_SUFFIX) -> str:
    """The citation key of a ledger or sidecar: its basename without suffix and archive date."""
    name = Path(path).name[: -len(suffix)]
    if path.startswith(ARCHIVE + "/"):
        name = ARCHIVE_PREFIX.sub("", name)
    return name


def is_reading(text: str) -> bool:
    """Whether the header window marks the ledger as a reading unit, exempt from rule B."""
    for line in text.splitlines()[:HEADER_LINES]:
        match = READING_FIELD.search(CODE_SPAN.sub("", line))
        if match is not None and match.group(1) == "READING":
            return True
    return False


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
    """(unit, clause id) -> the files citing it, from every tracked test file or script under
    CITATION_ROOTS (see `bindable`); a map or a production source never counts."""
    found: dict[tuple[str, str], list[str]] = {}
    for path in tracked:
        if not path.startswith(CITATION_ROOTS):
            continue
        try:
            text = (repo / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if CITATION.search(text) is None or not bindable(path, text):
            continue
        for match in CITATION.finditer(text):
            unit = match.group(1)
            for clause_id in re.findall(r"C-\d{3}", match.group(2)):
                found.setdefault((unit, clause_id), []).append(path)
    return found


class SidecarUnreadableError(Exception):
    """A tracked sidecar that is missing from disk or is not UTF-8."""


def strip_rust(text: str) -> str:
    """Rust source with comments and literals blanked, cached per text (see `_strip_rust`)."""
    key = hash(text)
    if key not in _STRIPPED:
        _STRIPPED[key] = _strip_rust(text)
    return _STRIPPED[key]


def _strip_rust(text: str) -> str:
    """Rust source with comments (block comments nested, and everything inside them, quotes and
    `//` included), string, raw-string, byte, C-string and char literals blanked; newlines kept."""
    out: list[str] = []
    cursor = 0
    index = 0
    while True:
        token = RUST_TOKEN.search(text, index)
        if token is None:
            out.append(text[cursor:])
            return "".join(out)
        out.append(text[cursor : token.start()])
        lexeme = token.group(0)
        if lexeme == "*/":
            out.append(lexeme)
            cursor = index = token.end()
            continue
        if lexeme != "/*":
            out.append("\n" * lexeme.count("\n"))
            cursor = index = token.end()
            continue
        depth, scan = 1, token.end()
        while depth:
            mark = RUST_COMMENT_MARK.search(text, scan)
            if mark is None:
                scan = len(text)
                break
            depth += 1 if mark.group(0) == "/*" else -1
            scan = mark.end()
        out.append("\n" * text.count("\n", token.start(), scan))
        cursor = index = scan


def _balanced_end(text: str, start: int) -> int:
    """The index just past the bracket group that opens at `start`, or the end of the text."""
    pairs = {"{": "}", "(": ")", "[": "]"}
    closing = pairs[text[start]]
    depth = 0
    for index in range(start, len(text)):
        if text[index] == text[start]:
            depth += 1
        elif text[index] == closing:
            depth -= 1
            if not depth:
                return index + 1
    return len(text)


def without_inert_rust(stripped: str) -> str:
    """Stripped Rust with the bodies of `macro_rules!` and the items under `#[cfg(any())]`
    blanked (newlines kept): a `fn` there is a template or compiled out, not a definition."""
    spans: list[tuple[int, int]] = []
    for macro in RUST_MACRO_BODY.finditer(stripped):
        spans.append((macro.end() - 1, _balanced_end(stripped, macro.end() - 1)))
    for attribute in RUST_CFG_NEVER.finditer(stripped):
        brace = stripped.find("{", attribute.end())
        semicolon = stripped.find(";", attribute.end())
        if brace != -1 and (semicolon == -1 or brace < semicolon):
            spans.append((attribute.start(), _balanced_end(stripped, brace)))
        elif semicolon != -1:
            spans.append((attribute.start(), semicolon + 1))
    if not spans:
        return stripped
    out: list[str] = []
    cursor = 0
    for start, end in sorted(spans):
        if start < cursor:
            continue
        out.append(stripped[cursor:start])
        out.append("\n" * stripped.count("\n", start, end))
        cursor = end
    out.append(stripped[cursor:])
    return "".join(out)


def _bound_names(target: ast.expr) -> set[str]:
    """The names an assignment target binds: a Name, or the Names inside a Tuple, List or
    Starred; a subscript or attribute target binds none."""
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        return {name for element in target.elts for name in _bound_names(element)}
    return set()


def strip_shell(text: str) -> str:
    """Shell source with heredoc bodies (any delimiter quoting), quoted strings (which may span
    lines), `$'...'` strings and comments blanked; newlines kept. `$((...))` and `((...))` are
    arithmetic, not heredocs."""
    out: list[str] = []
    quote: str | None = None
    closing: str | None = None
    pending: str | None = None
    for line in text.split("\n"):
        if closing is not None:
            out.append("")
            if line.lstrip("\t") == closing:
                closing = None
            continue
        kept: list[str] = []
        index = 0
        while index < len(line):
            char = line[index]
            if quote is not None:
                if quote == '"' and char == "\\":
                    index += 2
                    continue
                if char == quote:
                    quote = None
                index += 1
                continue
            if char == "\\":
                kept.append(" ")
                index += 2
                continue
            if char in "\"'":
                quote = char
                index += 1
                continue
            if char == "#" and (index == 0 or line[index - 1] in " \t;"):
                break
            arithmetic = 3 if line.startswith("$((", index) else 0
            arithmetic = arithmetic or (2 if line.startswith("((", index) else 0)
            if arithmetic:
                stop = line.find("))", index + arithmetic)
                index = len(line) if stop == -1 else stop + 2
                continue
            if line.startswith("$'", index):
                cursor = index + 2
                while cursor < len(line) and line[cursor] != "'":
                    cursor += 2 if line[cursor] == "\\" else 1
                index = cursor + 1
                continue
            if line.startswith("<<<", index):
                index += 3
                continue
            heredoc = SHELL_HEREDOC.match(line, index)
            if heredoc is not None:
                pending = heredoc.group(1)
                index = heredoc.end()
                continue
            kept.append(char)
            index += 1
        out.append("".join(kept))
        if pending is not None and quote is None:
            closing, pending = pending, None
    return "\n".join(out)


def _python_definitions(text: str) -> set[str]:
    """Module-level and class-level functions, classes and assigned names of a Python file."""
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return set()
    names: set[str] = set()
    bodies = [tree.body]
    bodies.extend(node.body for node in tree.body if isinstance(node, ast.ClassDef))
    for body in bodies:
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                for assigned in node.targets:
                    names.update(_bound_names(assigned))
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
    return names


def definitions(path: str, text: str) -> frozenset[str]:
    """Every name a bindable file defines: Python by parsing, Rust by line-start definitions over
    the source with comments and literals blanked, shell by function definitions."""
    key = (path, hash(text))
    cached = _DEFINITIONS.get(key)
    if cached is not None:
        return cached
    names: set[str] = set()
    if path.endswith(".py"):
        names = _python_definitions(text)
    elif path.endswith(".rs"):
        names = {
            match.group(1) or match.group(2)
            for match in RUST_DEFINITION.finditer(without_inert_rust(strip_rust(text)))
        }
    elif path.endswith(".sh"):
        names = {
            match.group(1) or match.group(2)
            for match in SHELL_DEFINITION.finditer(strip_shell(text))
        }
    result = frozenset(names)
    _DEFINITIONS[key] = result
    return result


def defines(path: str, text: str, name: str) -> bool:
    """Whether a bindable file defines `name`."""
    return name in definitions(path, text)


def carries_tests(path: str, text: str) -> bool:
    """Whether a test-located source holds a test, cached per file text (see `_carries_tests`)."""
    key = (path, hash(text))
    if key not in _CARRIES:
        _CARRIES[key] = _carries_tests(path, text)
    return _CARRIES[key]


def _carries_tests(path: str, text: str) -> bool:
    """Whether a test-located source holds a test: a Python `test_*` function at module or class
    level, or a pytest fixture or parametrize; a Rust `#[test]`, `#[tokio::test]` or
    `#[rstest]` outside comments and literals. A shell file counts as it stands."""
    if path.endswith(".rs"):
        return RUST_TEST_ATTRIBUTE.search(strip_rust(text)) is not None
    if not path.endswith(".py"):
        return True
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    bodies = [tree.body]
    bodies.extend(node.body for node in tree.body if isinstance(node, ast.ClassDef))
    for body in bodies:
        for node in body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name.startswith("test_"):
                return True
            for decorator in node.decorator_list:
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                if ast.unparse(target) in PYTHON_TEST_MARKERS:
                    return True
    return False


def bindable(path: str, text: str) -> bool:
    """Whether a sidecar entry may name `path`: a `.rs`, `.py` or `.sh` script under `scripts/`,
    or a test file (by place or name) that carries a test (`carries_tests`)."""
    name = Path(path).name
    if UNBINDABLE.match(name) or Path(path).suffix not in CODE_SUFFIXES:
        return False
    if path.startswith("scripts/"):
        return True
    located = "tests" in Path(path).parts[:-1] or TEST_NAMES.match(name) is not None
    return located and carries_tests(path, text)


def test_names(path: str, text: str) -> frozenset[str]:
    """The test functions a bindable file defines: Python module- or class-level `test_*`
    functions; Rust functions under `#[test]`, `#[tokio::test]` or `#[rstest]`."""
    key = (path, hash(text))
    cached = _TEST_NAMES.get(key)
    if cached is not None:
        return cached
    names: set[str] = set()
    if path.endswith(".rs"):
        names = {
            match.group(1)
            for match in RUST_TEST_FUNCTION.finditer(without_inert_rust(strip_rust(text)))
        }
    elif path.endswith(".py"):
        names = {name for name in _python_definitions(text) if name.startswith("test_")}
    result = frozenset(names)
    _TEST_NAMES[key] = result
    return result


def test_index(repo: Path, tracked: list[str]) -> dict[str, list[str]]:
    """Test function name -> the bindable files that define it, over the whole tree."""
    index: dict[str, list[str]] = {}
    for path in tracked:
        if Path(path).suffix not in (".rs", ".py"):
            continue
        text = read_tracked(repo, path)
        if not text or not bindable(path, text):
            continue
        for name in test_names(path, text):
            index.setdefault(name, []).append(path)
    return index


def backticked_names(text: str) -> list[str]:
    """The identifiers a row backticks, read by the shared CommonMark code-span reader: the last
    segment of each qualified or `path::name` span, a parametrize id dropped."""
    names: list[str] = []
    for span in TEXT.code_spans(text):
        name = TEXT.span_name(span)
        if name is not None and name not in names:
            names.append(name)
    return names


def bound_files(repo: Path, tracked: list[str]) -> dict[tuple[str, str], set[str]]:
    """(ledger beside the sidecar, clause id) -> the files its sidecar line names."""
    files: dict[tuple[str, str], set[str]] = {}
    tracked_set = set(tracked)
    for path in tracked:
        if not path.endswith(PINS_SUFFIX):
            continue
        ledger = path[: -len(PINS_SUFFIX)] + LEDGER_SUFFIX
        if ledger not in tracked_set:
            continue
        for line in read_tracked(repo, path).splitlines():
            match = BINDING_LINE.match(line)
            if match is None:
                continue
            files.setdefault((ledger, match.group(1)), set()).update(
                entry.split("::", 1)[0] for entry in match.group(2).split(", ") if entry != RECORDS
            )
    return files


def intent_findings(
    repo: Path,
    tracked: list[str],
    proven_rows: dict[tuple[str, str], str],
    cited: dict[tuple[str, str], list[str]],
) -> list[str]:
    """A PROVEN clause whose own row backticks a test that exactly one bindable file defines
    must bind that file, by its sidecar line or by a citation in that file."""
    index = test_index(repo, tracked)
    files = bound_files(repo, tracked)
    findings: list[str] = []
    for (ledger, clause_id), row in sorted(proven_rows.items()):
        unit = unit_of(ledger)
        have = files.get((ledger, clause_id), set()) | set(cited.get((unit, clause_id), []))
        for name in backticked_names(row):
            defining = index.get(name, [])
            if len(defining) == 1 and defining[0] not in have:
                findings.append(
                    f"{ledger}: {clause_id} row names `{name}`, which only {defining[0]} "
                    "defines, and no binding names that file"
                )
    return findings


def report_row_named_records(
    repo: Path,
    tracked: list[str],
    records: list[tuple[str, str, str]],
    row_texts: dict[tuple[str, str], str],
) -> None:
    """Records lines whose clause row backticks a test that bindable files define: one file is
    a missed binding, more than one is ambiguous and needs a hand line."""
    index = test_index(repo, tracked)
    found: list[str] = []
    ambiguous = 0
    for unit, clause_id, ledger in records:
        for name in backticked_names(row_texts.get((ledger, clause_id), "")):
            files = index.get(name, [])
            if files:
                ambiguous += len(files) > 1
                found.append(f"  {unit}/{clause_id}\t{name}\t{', '.join(sorted(files))}")
    print(f"row-named records: {len(found)} ({ambiguous} ambiguous)")
    for line in found:
        print(line)


def read_tracked(repo: Path, path: str) -> str:
    """A tracked file's text, cached for the run; an unreadable file reads as empty."""
    key = f"{repo}/{path}"
    if key not in _TEXTS:
        try:
            _TEXTS[key] = (repo / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            _TEXTS[key] = ""
    return _TEXTS[key]


def _entry_findings(
    repo: Path, tracked: set[str], where: str, clause_id: str, entries: list[str]
) -> list[str]:
    """Every defect of one binding line's entries: shape, kind, tracked path, defined name."""
    findings: list[str] = []
    if RECORDS in entries and len(entries) > 1:
        findings.append(f"{where}: {clause_id} `{RECORDS}` must be the only entry")
    if len(set(entries)) != len(entries):
        findings.append(f"{where}: {clause_id} repeats an entry")
    for entry in entries:
        if entry == RECORDS:
            continue
        match = BINDING_ENTRY.match(entry)
        if match is None:
            findings.append(f"{where}: {clause_id} entry `{entry}` is not <path> or <path>::<name>")
            continue
        path, name = match.group(1), match.group(2)
        if path not in tracked:
            findings.append(f"{where}: {clause_id} binds {path}, which is not a tracked file")
            continue
        text = read_tracked(repo, path)
        if not bindable(path, text):
            findings.append(
                f"{where}: {clause_id} binds {path}, which is not a test file or a script"
            )
            continue
        if name is None:
            continue
        if not defines(path, text, name):
            findings.append(f"{where}: {clause_id} names `{name}`, which {path} does not define")
    return findings


def _sidecar_text(repo: Path, path: str) -> str:
    """A tracked sidecar's text, or SidecarUnreadableError."""
    try:
        return (repo / path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        message = f"{path}: tracked sidecar is unreadable ({error})"
        raise SidecarUnreadableError(message) from error


def bindings(
    repo: Path, tracked: list[str]
) -> tuple[
    dict[tuple[str, str], list[str]],
    list[tuple[str, str, str]],
    list[str],
    list[tuple[str, str, str]],
]:
    """Sidecar bindings: (ledger beside the sidecar, clause id) -> sidecar lines, every line as
    (where, ledger, clause id) for the clause check, the shape findings, and the clauses bound to
    `none (records)` as (unit, clause id, ledger)."""
    found: dict[tuple[str, str], list[str]] = {}
    lines: list[tuple[str, str, str]] = []
    findings: list[str] = []
    records: list[tuple[str, str, str]] = []
    tracked_set = set(tracked)
    bins = (STAGING + "/", COMPLETED + "/", ARCHIVE + "/")
    for path in tracked:
        if not path.startswith(bins) or not path.endswith(PINS_SUFFIX):
            continue
        ledger = path[: -len(PINS_SUFFIX)] + LEDGER_SUFFIX
        if ledger not in tracked_set:
            findings.append(f"{path}: no ledger {Path(ledger).name} beside it; it binds nothing")
            continue
        unit = unit_of(path, PINS_SUFFIX)
        seen: set[str] = set()
        text = _sidecar_text(repo, path)
        for number, line in enumerate(text.splitlines(), start=1):
            if not line.strip() or line.startswith("#"):
                continue
            where = f"{path}:{number}"
            match = BINDING_LINE.match(line)
            if match is None:
                findings.append(f"{where}: not a binding line (C-NNN: <path>[::<name>], ...)")
                continue
            clause_id = match.group(1)
            if clause_id in seen:
                findings.append(f"{where}: {clause_id} is bound twice in this sidecar")
            seen.add(clause_id)
            entries = match.group(2).split(", ")
            findings.extend(_entry_findings(repo, tracked_set, where, clause_id, entries))
            if entries == [RECORDS]:
                records.append((unit, clause_id, ledger))
            found.setdefault((ledger, clause_id), []).append(where)
            lines.append((where, ledger, clause_id))
    return found, lines, findings, sorted(records)


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


def run(repo: Path, row_named_report: bool = False) -> int:
    """All three rules over the live ledgers; the summary, the records list; the exit code."""
    try:
        tracked = tracked_paths(repo)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"ERROR: cannot list tracked files ({error})", file=sys.stderr)
        return 2
    live = [
        p
        for p in tracked
        if p.startswith((STAGING + "/", COMPLETED + "/")) and p.endswith(LEDGER_SUFFIX)
    ]
    archived = [p for p in tracked if p.startswith(ARCHIVE + "/") and p.endswith(LEDGER_SUFFIX)]
    if not live and not archived:
        print("ERROR: no ledgers found under task/ledgers — refuse to pass closed", file=sys.stderr)
        return 2

    findings: list[str] = []
    known: set[tuple[str, str]] = set()
    clauses_of: dict[str, set[str]] = {}
    row_texts: dict[tuple[str, str], str] = {}
    proven_rows: dict[tuple[str, str], str] = {}
    proven: dict[str, list[str]] = {}
    clauses = 0
    for path in live + archived:
        text = (repo / path).read_text(encoding="utf-8")
        rows = clause_rows(text)
        unit = unit_of(path)
        if path in live:
            row_findings, verdicts = check_rows(path, rows)
            findings.extend(row_findings)
            clauses += len(rows)
            if not is_reading(text):
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
        clauses_of[path] = {cid for _n, cid, _c in rows}
        row_texts.update(((path, cid), " | ".join(cells)) for _n, cid, cells in rows)
        for _n, cid, cells in rows:
            marks = [VERDICT_CELL.match(cell) for cell in cells]
            if any(mark is not None and mark.group(1) == "PROVEN" for mark in marks):
                proven_rows[(path, cid)] = " | ".join(cells)

    cited = citations(repo, tracked)
    for (unit, clause_id), files in sorted(cited.items()):
        if (unit, clause_id) not in known:
            findings.append(f"{files[0]}: `pins: {unit}/{clause_id}` names no clause in any ledger")
    try:
        bound, binding_lines, binding_findings, records = bindings(repo, tracked)
    except SidecarUnreadableError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    findings.extend(binding_findings)
    for where, ledger, clause_id in binding_lines:
        if clause_id not in clauses_of.get(ledger, set()):
            findings.append(
                f"{where}: {clause_id} names no clause of {unit_of(ledger)} in the ledger beside it"
            )
    pinned = set(cited) | {(unit_of(ledger), cid) for ledger, cid in bound}
    findings.extend(intent_findings(repo, tracked, proven_rows, cited))
    for path, proven_ids in sorted(proven.items()):
        unit = unit_of(path)
        unpinned = [
            cid for cid in proven_ids if (path, cid) not in bound and (unit, cid) not in cited
        ]
        ceiling = EXCEPTIONS.get(Path(path).name, (0, True))[0]
        if len(unpinned) > ceiling:
            findings.append(
                f"{path}: {len(unpinned)} PROVEN clause(s) with no binding (ceiling {ceiling}): "
                f"{', '.join(unpinned[: max(1, len(unpinned) - ceiling)])} — no sidecar line binds "
                f"them; add `C-NNN: <test path>[::<name>]` or `C-NNN: {RECORDS}` to "
                f"{unit}{PINS_SUFFIX} beside the ledger"
            )
        elif ceiling and len(unpinned) < ceiling:
            findings.append(
                f"{path}: ceiling {ceiling} is above the measured {len(unpinned)} — "
                "ratchet it down in EXCEPTIONS"
            )
    for name in sorted(EXCEPTIONS):
        if f"{STAGING}/{name}" not in live and f"{COMPLETED}/{name}" not in live:
            findings.append(f"EXCEPTIONS names {name}, which is in no live bin — delete the row")

    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        print(f"ledger-grammar: FAIL — {len(findings)} finding(s)", file=sys.stderr)
        if row_named_report:
            report_row_named_records(repo, tracked, records, row_texts)
        return 1
    print(
        f"ledger-grammar: {len(live)} live ledgers clean ({clauses} clauses, "
        f"{len(pinned)} pinned clause ids, {len(EXCEPTIONS)} exception rows)"
    )
    print(f"ledger-grammar: {len(records)} clause(s) bound to `{RECORDS}`, no test pins them")
    by_unit: dict[str, list[str]] = {}
    for unit, clause_id, _ledger in records:
        by_unit.setdefault(unit, []).append(clause_id)
    for unit in sorted(by_unit):
        print(f"  {unit}: {', '.join(by_unit[unit])}")
    if row_named_report:
        report_row_named_records(repo, tracked, records, row_texts)
    return 0


def main() -> int:
    """CLI: `--report row-named-records`, and the hidden `--repo` for the provocation tests."""
    parser = argparse.ArgumentParser(
        prog="check_ledger_grammar.py", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--report",
        choices=["row-named-records"],
        help="list records lines whose ledger row names a test one bindable file defines",
    )
    arguments = parser.parse_args()
    return run(arguments.repo.resolve(), arguments.report == "row-named-records")


if __name__ == "__main__":
    sys.exit(main())
