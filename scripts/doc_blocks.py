#!/usr/bin/env python3
"""The block grammar of the two live documents, and the two transforms that keep them live.

`STATUS.md` "Active workstreams" and `briefs/next-sequence.md` carry HTML-comment
markers (DL-4, 2026-08-25) so that a merged unit can leave the slate and a closed
campaign can leave STATUS mechanically — at pickup, with no judgement and no
obituary. The markers render as nothing.

    <!-- ws id=mw ledgers=mw- state=closed closed=2026-08-23 by=#224 history=docs/history/mw -->
    - **Iceberg maintenance wave (MW)** ...
    <!-- /ws -->

    | 1 | **V3E-4** — ... | M <!-- unit id=v3e-4 --> |      (a table row: inline form)
    <!-- unit id=v3e-4 -->                                 (prose: block form)
    ...the reasoning...
    <!-- /unit -->

`ws` blocks wrap every top-level bullet under "Active workstreams"; `state` is
`open`, `held` or `closed`, and a closed block must name `closed=` (the date),
`by=` (the closing PR) and `history=` (its `docs/history/` directory). Closure is
declared by the departure edit under an owner ruling; nothing here infers it.
`unit` markers name a unit id; the unit's ledger is any `<id>-*-ledger.md`
(override with `ledger=<prefix>`), and the unit's rows and blocks leave when that
ledger sits in `task/ledgers/completed/` or the archive.

Pure text functions; the lifecycle script applies them and the gate reads them.
"""

from __future__ import annotations

import re

STATUS_PATH = "STATUS.md"
SLATE_PATH = "briefs/next-sequence.md"
ACTIVE_HEADING = "## Active workstreams"
CLOSED_LIST_MARKER = "<!-- closed-campaigns -->"
STATES: frozenset[str] = frozenset({"open", "held", "closed"})
CLOSED_KEYS: tuple[str, ...] = ("closed", "by", "history")

MARKER = re.compile(r"<!--\s*(/?)(ws|unit)\b([^>]*?)-->")
ATTRIBUTE = re.compile(r"([a-z]+)=(\S+)")
BULLET = re.compile(r"^- ")
TABLE_ROW = re.compile(r"^\|")
HEADING = re.compile(r"^## ")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PR = re.compile(r"^#\d+$")
LEDGER_SUFFIX = "-ledger.md"
ARCHIVE_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}-")


class Block:
    """One marked region: `kind` is `ws` or `unit`; lines are 0-based, `end` exclusive.

    An inline unit marker (a table row) is a one-line block with `inline=True`;
    its `start`/`end` cover the row and no marker line exists on its own.
    (A plain class, not a model: the scripts run under the system interpreter
    from the pre-commit hook and carry no dependencies.)
    """

    __slots__ = ("attrs", "end", "inline", "kind", "start")

    def __init__(
        self, kind: str, attrs: dict[str, str], start: int, end: int, inline: bool = False
    ) -> None:
        self.kind = kind
        self.attrs = attrs
        self.start = start
        self.end = end
        self.inline = inline

    @property
    def id(self) -> str:
        """The block's id attribute."""
        return self.attrs["id"]


class Parsed:
    """The parse of one document: its blocks and every grammar finding."""

    __slots__ = ("blocks", "findings")

    def __init__(self) -> None:
        self.blocks: list[Block] = []
        self.findings: list[str] = []


def _attrs(raw: str) -> dict[str, str]:
    """`key=value` pairs of a marker body, in order."""
    return dict(ATTRIBUTE.findall(raw))


def _validate(kind: str, attrs: dict[str, str], where: str) -> list[str]:
    """Grammar findings for one opening marker."""
    findings: list[str] = []
    if "id" not in attrs:
        findings.append(f"{where}: `{kind}` marker has no id")
    if kind == "ws":
        state = attrs.get("state")
        if state not in STATES:
            findings.append(f"{where}: ws state must be one of {sorted(STATES)}, got {state!r}")
        if state == "closed":
            for key in CLOSED_KEYS:
                if key not in attrs:
                    findings.append(f"{where}: closed ws block needs `{key}=`")
            if "closed" in attrs and not DATE.match(attrs["closed"]):
                findings.append(f"{where}: `closed=` must be yyyy-mm-dd")
            if "by" in attrs and not PR.match(attrs["by"]):
                findings.append(f"{where}: `by=` must be a PR number like #224")
    return findings


def parse(text: str, path: str) -> Parsed:
    """Parse the markers of one document; every grammar violation is a finding."""
    parsed = Parsed()
    lines = text.splitlines()
    open_block: Block | None = None
    for index, line in enumerate(lines):
        where = f"{path}:{index + 1}"
        matches = list(MARKER.finditer(line))
        if not matches:
            continue
        for match in matches:
            closing, kind, raw = match.group(1) == "/", match.group(2), match.group(3)
            if closing:
                if open_block is None or open_block.kind != kind:
                    parsed.findings.append(f"{where}: `/{kind}` closes nothing")
                    continue
                open_block.end = index + 1
                parsed.blocks.append(open_block)
                open_block = None
                continue
            attrs = _attrs(raw)
            parsed.findings.extend(_validate(kind, attrs, where))
            inline = kind == "unit" and TABLE_ROW.match(line) is not None
            if inline:
                parsed.blocks.append(Block(kind, attrs, index, index + 1, inline=True))
                continue
            if open_block is not None:
                parsed.findings.append(
                    f"{where}: `{kind}` opens inside `{open_block.kind}` "
                    f"{open_block.id} (no nesting)"
                )
                continue
            open_block = Block(kind, attrs, index, index + 1)
    if open_block is not None:
        parsed.findings.append(
            f"{path}:{open_block.start + 1}: `{open_block.kind}` {open_block.id} never closes"
        )
    return parsed


def uncovered_bullets(text: str, parsed: Parsed) -> list[int]:
    """1-based lines of top-level bullets under "Active workstreams" outside any ws block.

    The one-line records under the `<!-- closed-campaigns -->` marker are the
    list `compact` writes, not workstreams, and are exempt.
    """
    lines = text.splitlines()
    covered: set[int] = set()
    for block in parsed.blocks:
        if block.kind == "ws":
            covered.update(range(block.start, block.end))
    inside = False
    in_closed_list = False
    found: list[int] = []
    for index, line in enumerate(lines):
        if HEADING.match(line):
            inside = line.strip() == ACTIVE_HEADING
            in_closed_list = False
            continue
        if line.strip() == CLOSED_LIST_MARKER:
            in_closed_list = True
            continue
        if in_closed_list and not BULLET.match(line):
            in_closed_list = False
        if inside and BULLET.match(line) and index not in covered and not in_closed_list:
            found.append(index + 1)
    return found


def unit_prefix(block: Block) -> str:
    """The ledger-name prefix a unit marker binds: `ledger=` or `<id>-`."""
    return block.attrs.get("ledger", block.id + "-")


def ledger_name(path: str) -> str:
    """A ledger's name without its archive date prefix, or "" for a non-ledger."""
    name = path.rsplit("/", 1)[-1]
    if not name.endswith(LEDGER_SUFFIX):
        return ""
    return ARCHIVE_PREFIX.sub("", name)


def departed_units(parsed: Parsed, retired_ledgers: list[str]) -> list[str]:
    """Ids of the unit blocks whose ledger is among `retired_ledgers` (completed or archived)."""
    names = [ledger_name(path) for path in retired_ledgers]
    departed: list[str] = []
    for block in parsed.blocks:
        if block.kind != "unit" or block.id in departed:
            continue
        prefix = unit_prefix(block)
        if any(name.startswith(prefix) for name in names):
            departed.append(block.id)
    return departed


def _collapse(lines: list[str]) -> list[str]:
    """Never two blank lines in a row; no leading blank line."""
    kept: list[str] = []
    for line in lines:
        if not line.strip() and (not kept or not kept[-1].strip()):
            continue
        kept.append(line)
    return kept


def _renumber(lines: list[str]) -> list[str]:
    """Tables whose first header cell is `#` count their rows 1..n again."""
    out: list[str] = []
    counter = 0
    in_numbered = False
    for line in lines:
        cells = [cell.strip() for cell in line.split("|")] if TABLE_ROW.match(line) else []
        first = cells[1] if len(cells) > 2 else None
        if first == "#":
            in_numbered, counter = True, 0
        elif in_numbered and first is not None and first.isdigit():
            counter += 1
            line = f"| {counter} |" + line.split("|", 2)[2]
        elif first is None:
            in_numbered = False
        out.append(line)
    return out


def remove_units(text: str, ids: list[str]) -> tuple[str, int]:
    """Delete every row and block of the given unit ids, whole; renumber `#` tables."""
    parsed = parse(text, SLATE_PATH)
    lines = text.splitlines()
    drop: set[int] = set()
    removed = 0
    for block in parsed.blocks:
        if block.kind == "unit" and block.id in ids:
            drop.update(range(block.start, block.end))
            removed += 1
    kept = [line for index, line in enumerate(lines) if index not in drop]
    return "\n".join(_renumber(_collapse(kept))) + "\n", removed


def _title(block_text: str) -> str:
    """The bold name at the head of a workstream bullet, or its first line."""
    match = re.search(r"\*\*(.+?)\*\*", block_text)
    return match.group(1) if match else block_text.splitlines()[0][:60]


def cut_closed(text: str) -> tuple[str, list[tuple[Block, str]]]:
    """Remove every `state=closed` ws block (markers included); return the text and the cuts.

    Each cut's text is the block's content without its marker lines. The
    one-line record STATUS keeps is added by `record_closed`, once the caller
    knows where the content landed.
    """
    parsed = parse(text, STATUS_PATH)
    lines = text.splitlines()
    cuts: list[tuple[Block, str]] = []
    drop: set[int] = set()
    for block in parsed.blocks:
        if block.kind == "ws" and block.attrs.get("state") == "closed":
            content = "\n".join(lines[block.start + 1 : block.end - 1]) + "\n"
            cuts.append((block, content))
            drop.update(range(block.start, block.end))
    kept = [line for index, line in enumerate(lines) if index not in drop]
    return "\n".join(_collapse(kept)) + "\n", cuts


def record_closed(text: str, block: Block, content: str, record_path: str) -> str:
    """Append the one-line record of a cut campaign to the closed-campaigns list."""
    lines = text.splitlines()
    try:
        marker = next(i for i, line in enumerate(lines) if line.strip() == CLOSED_LIST_MARKER)
    except StopIteration as error:
        raise ValueError(f"{STATUS_PATH} has no `{CLOSED_LIST_MARKER}` line") from error
    end = marker + 1
    while end < len(lines) and BULLET.match(lines[end]):
        end += 1
    row = (
        f"- **{_title(content)}** — closed {block.attrs['closed']} by {block.attrs['by']}; "
        f"record: [{record_path}]({record_path})"
    )
    lines.insert(end, row)
    return "\n".join(lines) + "\n"
