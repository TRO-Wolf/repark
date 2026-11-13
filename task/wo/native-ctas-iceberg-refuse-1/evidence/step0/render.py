from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

TEXT_LIMIT = 110


def cut(text: str) -> str:
    if len(text) <= TEXT_LIMIT:
        return text
    head = text[:TEXT_LIMIT]
    return head[: head.rfind(" ")] + " …"


def answer(cell: dict[str, Any], suffix: str = "") -> str:
    kind = cell.get(f"kind{suffix}")
    if kind is None:
        return ""
    if kind == "refused":
        return f"refused {cell[f'class{suffix}']}: {cut(cell[f'text{suffix}'])}"
    return "answered"


def landed(cell: dict[str, Any]) -> str:
    after = cell["after"]
    if not after["readable"]:
        files = after.get("metadata_files")
        return "no table" if not files else f"unreadable, {files} metadata file(s)"
    parts = [f"rows {after['rows']}"]
    if "snapshots" in after:
        parts.append(f"{after['snapshots']} snap")
        parts.append(after["metadata_latest"][:12])
    return ", ".join(parts)


def before(cell: dict[str, Any]) -> str:
    state = cell["before"]
    if not state["readable"]:
        return "-"
    parts = [f"rows {state['rows']}"]
    if "snapshots" in state:
        parts.append(f"{state['snapshots']} snap")
        parts.append(state["metadata_latest"][:12])
    return ", ".join(parts)


def main() -> None:
    title = sys.argv[-1]
    print(f"# {title}\n")
    for path in sys.argv[1:-1]:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        cells = [json.loads(line) for line in lines]
        print(f"## `{Path(path).name}` ({len(cells)} cells)\n")
        print("| group | target | form | shape | outcome | second statement | before | after |")
        print("|---|---|---|---|---|---|---|---|")
        for cell in cells:
            row = [
                cell["group"],
                cell["target"],
                cell["form"],
                cell["shape"],
                answer(cell),
                answer(cell, "_1"),
                before(cell),
                landed(cell),
            ]
            print("| " + " | ".join(item.replace("|", "\\|") for item in row) + " |")
        print()


if __name__ == "__main__":
    main()
