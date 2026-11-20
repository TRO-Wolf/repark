import json
import sys
from collections import Counter
from pathlib import Path

READS = {"S": "user schema", "N": "no options", "F": "inferSchema=false", "T": "inferSchema=true"}


def grid(counter: Counter, rows: list[str], cols: list[str], head: str) -> list[str]:
    lines = [f"| {head} | " + " | ".join(cols) + " | total |", "|---" * (len(cols) + 2) + "|"]
    for row in rows:
        values = [counter[(row, col)] for col in cols]
        if sum(values):
            lines.append(f"| {row} | " + " | ".join(map(str, values)) + f" | {sum(values)} |")
    return lines


def main() -> None:
    cells = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
    out: list[str] = []
    wrong = [cell for cell in cells if cell["main"] == "WRONG"]
    raise_ = [cell for cell in cells if cell["main"] == "RAISE"]
    sub = Counter((cell["tag"][0], cell["main_sub"]) for cell in wrong)
    out += ["### WRONG by subclass (silent: every WRONG cell answers without an error)"]
    out += grid(sub, list("SNFTL"), ["value", "type", "answers"], "part")
    read_door = Counter()
    for cell in wrong + raise_:
        part, _, door = cell["tag"].split("/")[:3]
        if part in READS:
            read_door[(READS[part], door + ":" + cell["main"])] += 1
    cols = [
        f"{door}:{kind}" for door in ("csv", "load", "view", "sql") for kind in ("WRONG", "RAISE")
    ]
    out += ["", "### WRONG and RAISE by read and door"]
    out += grid(read_door, list(READS.values()), cols, "read")
    shapes: list[str] = []
    for cell in cells:
        shape = cell["tag"].split("/")[6]
        if shape not in shapes:
            shapes.append(shape)
    by_shape = Counter()
    for cell in wrong:
        by_shape[(cell["tag"].split("/")[6], cell["tag"][0])] += 1
    out += ["", "### WRONG by shape and read (all doors, both ANSI modes)"]
    out += grid(by_shape, shapes, list("SNFTL"), "shape")
    types: list[str] = []
    s_cells = [cell for cell in cells if cell["tag"].startswith("S/")]
    for cell in s_cells:
        kind = cell["tag"].split("/")[5]
        if kind not in types:
            types.append(kind)
    by_type = Counter()
    for cell in s_cells:
        parts = cell["tag"].split("/")
        by_type[(parts[5], parts[4] + ":" + cell["main"])] += 1
    cols = [
        f"{comp}:{kind}" for comp in ("alone", "beside") for kind in ("EQUAL", "WRONG", "RAISE")
    ]
    out += ["", "### The user-schema read by declared type, alone and beside a `TIMESTAMP` field"]
    out += grid(by_type, types, cols, "type")
    ansi = Counter(
        (cell["tag"][0], cell["tag"].split("/")[1] + ":" + cell["main"]) for cell in cells
    )
    cols = [f"{mode}:{kind}" for mode in ("true", "false") for kind in ("WRONG", "RAISE")]
    out += ["", "### WRONG and RAISE by ANSI mode"]
    out += grid(ansi, list("SNFTLM"), cols, "part")
    string = Counter()
    for cell in s_cells:
        parts = cell["tag"].split("/")
        if parts[5] == "STRING":
            string[(parts[6], parts[4] + ":" + cell["main"])] += 1
    cols = [f"{comp}:{kind}" for comp in ("alone", "beside") for kind in ("EQUAL", "WRONG")]
    out += [
        "",
        "### A `STRING` field, alone and beside a `TIMESTAMP` field (3 doors, 2 headers, 2 modes)",
    ]
    out += grid(string, shapes, cols, "shape")
    errors = Counter(cell["main_err"][:70].rsplit(" ", 1)[0] for cell in raise_)
    out += ["", "### main's raise texts (first 70 characters)"]
    out += [f"- {count}: `{text}`" for text, count in errors.most_common(12)]
    print("\n".join(out))


if __name__ == "__main__":
    main()
