import json
import sys
from collections import defaultdict
from pathlib import Path

LETTER = {"EQUAL": "E", "WRONG": "W", "RAISE": "R"}
cells: dict[tuple[str, str, str, str], dict[str, str]] = defaultdict(dict)
for line in Path(sys.argv[1]).read_text().splitlines():
    record = json.loads(line)
    group, hold, write, action, ansi = record["key"].split("|")
    mark = LETTER[record["v_main"]]
    if record["spark_raises"] and mark == "W":
        mark = "w"
    if record["stored"]:
        mark = "S"
    cells[(group, hold, write, action)][ansi] = mark


def mark(key: tuple[str, str, str, str]) -> str:
    modes = cells.get(key, {})
    if not modes:
        return "-"
    on, off = modes.get("true", "?"), modes.get("false", "?")
    return on if on == off else on + "/" + off


def table(group: str, rows: list[str], cols: list[tuple[str, str]], row_is_hold: bool) -> None:
    print("| " + group + " | " + " | ".join(a + ":" + b if b else a for a, b in cols) + " |")
    print("|---" * (len(cols) + 1) + "|")
    for row in rows:
        marks = []
        for first, second in cols:
            if row_is_hold:
                marks.append(mark((group, row, first, second)))
            else:
                marks.append(mark((group, "table", row, first)))
        print("| `" + row + "` | " + " | ".join(marks) + " |")
    print()


keys = list(cells)
groups = {key[0] for key in keys}


def ordered(index: int, group: str) -> list[str]:
    seen: list[str] = []
    for key in keys:
        if key[0] == group and key[index] not in seen:
            seen.append(key[index])
    return seen


if "HW" in groups:
    table("HW", ordered(1, "HW"), [(w, "collect") for w in ordered(2, "HW")], True)
if "WA" in groups:
    table("WA", ordered(2, "WA"), [(a, "") for a in ordered(3, "WA")], False)
if "HA" in groups:
    cols = [(w, a) for w in ordered(2, "HA") for a in ordered(3, "HA")]
    table("HA", ordered(1, "HA"), cols, True)
if "P" in groups:
    cols = [(w, a) for w in ordered(2, "P") for a in ordered(3, "P")]
    table("P", ordered(1, "P"), cols, True)
if "B" in groups:
    actions = ordered(3, "B")
    print("| hold | write | " + " | ".join("`" + a + "`" for a in actions) + " |")
    print("|---" * (len(actions) + 2) + "|")
    for hold in ordered(1, "B"):
        for write in ordered(2, "B"):
            marks = [mark(("B", hold, write, a)) for a in actions]
            print("| `" + hold + "` | `" + write + "` | " + " | ".join(marks) + " |")
    print()
for group in ("S", "C", "Q", "X", "M"):
    for key in keys:
        if key[0] == group:
            print(group, key[1], mark(key))
