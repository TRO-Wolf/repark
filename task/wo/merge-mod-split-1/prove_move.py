"""Prove that the MERGE module split is a pure move.

Usage: prove_move.py [--head-ref <ref>]

The base side is `origin/main:crates/repark-iceberg/src/write/merge/mod.rs`. The head side is the
four files that hold its items after the split, read from the working tree, or from `<ref>` when
`--head-ref` is given. Each item is compared after comments, visibility keywords and whitespace
outside string literals are removed. A comma before a closing delimiter is removed too, because
rustfmt adds one when a widened signature wraps. Exit 0 when every item matches and the counts
agree.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_MERGE = "crates/repark-iceberg/src/write/merge"
_BASE_REF = "origin/main"
_OLD_FILE = "mod.rs"
_NEW_FILES = ("mod.rs", "merge_sql.rs", "copy_on_write.rs", "merge_on_read.rs")
_LITERAL = re.compile(
    r"""
    (?P<line>//[^\n]*)
    |(?P<block>/\*.*?\*/)
    |(?P<raw>r(?P<hashes>\#*)".*?"(?P=hashes))
    |(?P<string>"(?:\\.|[^"\\])*")
    |(?P<char>'(?:\\.|[^'\\])')
    """,
    re.DOTALL | re.VERBOSE,
)
_VISIBILITY = re.compile(r"\bpub(?:\s*\([^)]*\))?\s+")
_TRAILING_COMMA = re.compile(r",(?=[)\]}])")
_ATTRIBUTE = re.compile(r"#!?\[[^\]]*\]")
_NAMED = re.compile(
    r"^(?:(?:async|const|unsafe)\s+)*"
    r"(?P<kind>fn|struct|enum|trait|const|static|mod|type)\s+(?P<name>\w+)"
)
_IMPL = re.compile(r"^impl\b(?P<head>[^{]*)\{")
_MACRO = re.compile(r"^(?P<path>[\w:]+)!")
_SKIPPED_KINDS = ("use", "mod")
_PLACEHOLDER = "\x00"


def read_source(ref: str | None, name: str) -> str:
    """Read one merge source file from a git ref, or from the working tree when ref is None."""
    relative = f"{_MERGE}/{name}"
    if ref is None:
        return (_REPO / relative).read_text(encoding="utf-8")
    shown = subprocess.run(
        ["git", "-C", str(_REPO), "show", f"{ref}:{relative}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return shown.stdout


def mask_literals(source: str) -> tuple[str, list[str]]:
    """Drop comments and swap every string or char literal for a placeholder byte."""
    literals: list[str] = []
    pieces: list[str] = []
    cursor = 0
    for match in _LITERAL.finditer(source):
        pieces.append(source[cursor : match.start()])
        cursor = match.end()
        if match.lastgroup in ("line", "block"):
            pieces.append(" ")
            continue
        literals.append(match.group(0))
        pieces.append(_PLACEHOLDER)
    pieces.append(source[cursor:])
    return "".join(pieces), literals


def split_items(masked: str) -> list[str]:
    """Split masked source into items that end at depth zero on a brace or a semicolon."""
    items: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(masked):
        if char in "{([":
            depth += 1
        elif char in "})]":
            depth -= 1
        closes_block = char == "}" and depth == 0 and not is_use(masked[start:index])
        closes_statement = char == ";" and depth == 0
        if closes_block or closes_statement:
            items.append(masked[start : index + 1].strip())
            start = index + 1
    tail = masked[start:].strip()
    if tail:
        items.append(tail)
    return items


def bare_head(item: str) -> str:
    """Return an item with its attributes and visibility removed, for classification."""
    without_attributes = _ATTRIBUTE.sub("", item)
    return _VISIBILITY.sub("", without_attributes, count=1).strip()


def is_use(item: str) -> bool:
    """Tell whether an item, possibly unfinished, is a `use` declaration."""
    return bare_head(item).startswith("use ")


def describe(item: str) -> tuple[str, str]:
    """Return the kind and the name of one item."""
    head = bare_head(item)
    if head.startswith("use "):
        return "use", " ".join(head.split())
    named = _NAMED.match(head)
    if named:
        return named.group("kind"), named.group("name")
    implementation = _IMPL.match(head)
    if implementation:
        return "impl", " ".join(implementation.group("head").split())
    macro = _MACRO.match(head)
    if macro:
        return "macro", macro.group("path")
    return "other", " ".join(head.split())[:60]


def normalise(item: str, literals: list[str]) -> str:
    """Remove visibility, whitespace and wrap commas from code, then restore the literals."""
    code = _TRAILING_COMMA.sub("", re.sub(r"\s+", "", _VISIBILITY.sub("", item)))
    restored: list[str] = []
    for char in code:
        restored.append(literals.pop(0) if char == _PLACEHOLDER else char)
    return "".join(restored)


def impl_body(item: str) -> str:
    """Return the text between the outer braces of an impl item."""
    return item[item.index("{") + 1 : item.rindex("}")]


def catalogue(source: str) -> dict[str, str]:
    """Map each item and each impl method to its normalised text, in a stable key form."""
    masked, literals = mask_literals(source)
    seen: Counter[str] = Counter()
    entries: dict[str, str] = {}
    for item in split_items(masked):
        kind, name = describe(item)
        if kind in _SKIPPED_KINDS:
            normalise(item, literals)
            continue
        if kind != "impl":
            add_entry(entries, seen, f"{kind} {name}", normalise(item, literals))
            continue
        open_brace = item.index("{")
        normalise(item[: open_brace + 1], literals)
        for method in split_items(impl_body(item)):
            method_kind, method_name = describe(method)
            key = f"{method_kind} <impl {name}>::{method_name}"
            add_entry(entries, seen, key, normalise(method, literals))
        add_entry(entries, seen, f"impl {name}", "")
    return entries


def add_entry(entries: dict[str, str], seen: Counter[str], key: str, text: str) -> None:
    """Record one entry, numbering a repeated key so cfg twins stay distinct."""
    seen[key] += 1
    unique = key if seen[key] == 1 else f"{key} #{seen[key]}"
    entries[unique] = text


def compare(old: dict[str, str], new: dict[str, dict[str, str]]) -> list[str]:
    """Write one report line per old item and return the failures."""
    failures: list[str] = []
    for key, old_text in old.items():
        homes = [name for name, entries in new.items() if key in entries]
        if len(homes) != 1:
            failures.append(f"{key}: found in {homes or 'no file'}")
            print(f"MISSING  {key}")
            continue
        home = homes[0]
        same = new[home][key] == old_text
        moved = "moved" if home != _OLD_FILE else "stays"
        print(f"{'SAME' if same else 'DIFF'}  {moved}  {home:<17} {key}")
        if not same:
            failures.append(f"{key}: body differs in {home}")
    for name, entries in new.items():
        for key in entries:
            if key not in old:
                failures.append(f"{key}: new in {name}")
                print(f"EXTRA    {name:<17} {key}")
    return failures


def main() -> int:
    """Compare the base module with the split files and print the proof."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--head-ref", default=None)
    arguments = parser.parse_args()
    old = catalogue(read_source(_BASE_REF, _OLD_FILE))
    new = {name: catalogue(read_source(arguments.head_ref, name)) for name in _NEW_FILES}
    print(f"base: {_BASE_REF}:{_MERGE}/{_OLD_FILE}")
    print("compared after removing comments, visibility keywords and whitespace outside literals,")
    print("and the comma rustfmt adds before a closing delimiter when a signature wraps")
    print("use and mod declarations are not items here; an impl counts once, plus its methods")
    print()
    failures = compare(old, new)
    print()
    total = 0
    for name, entries in new.items():
        moved = sum(1 for key in entries if key in old)
        total += len(entries)
        print(f"items in {name}: {len(entries)} ({moved} from the base file)")
    print(f"items in the base file: {len(old)}")
    print(f"items across the split files: {total}")
    if len(old) != total:
        failures.append(f"item count {len(old)} != {total}")
    moved_count = sum(len(entries) for name, entries in new.items() if name != _OLD_FILE)
    print(f"moved items: {moved_count}")
    print(f"result: {'PURE MOVE' if not failures else 'NOT A PURE MOVE'}")
    for failure in failures:
        print(f"failure: {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
