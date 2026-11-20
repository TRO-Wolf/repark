import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

PATH = re.compile(r"/?tmp/\S*?/csv-string-out/work/(data|wh)")
BUILDS = tuple(sys.argv[3:]) or ("main", "unit1")


def norm(record: dict[str, Any]) -> dict[str, Any]:
    text = json.dumps(record)
    return json.loads(PATH.sub("<work>", text))


def load(raw: Path, engine: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(raw.glob(f"{engine}-*.jsonl")):
        for line in path.read_text().splitlines():
            record = norm(json.loads(line))
            out[record["tag"]] = record
    return out


def verdict(got: dict[str, Any], want: dict[str, Any] | None) -> str:
    if want is None:
        return "NOSPARK"
    got_err = "error" in got or "create_error" in got
    want_err = "error" in want or "create_error" in want
    if got_err and want_err:
        return "BOTHERR"
    if got_err:
        return "RAISE"
    if want_err:
        return "WRONG"
    if got["rows"] == want["rows"] and got["types"] == want["types"]:
        return "EQUAL"
    return "WRONG"


def subclass(got: dict[str, Any], want: dict[str, Any] | None) -> str:
    if want is None or "rows" not in got:
        return ""
    if "rows" not in want:
        return "answers"
    if got["rows"] != want["rows"]:
        return "value"
    if got["types"] != want["types"]:
        return "type"
    return ""


def names_differ(got: dict[str, Any], want: dict[str, Any] | None) -> bool:
    if want is None or "names" not in got or "names" not in want:
        return False
    return got["names"] != want["names"]


def first_x(record: dict[str, Any]) -> str:
    if "rows" not in record:
        return "ERR"
    if not record["rows"]:
        return "no rows"
    return ";".join(str(cols[0]) for cols in record["cols"])


def table(counter: Counter, rows: list[str], cols: list[str], title: str) -> list[str]:
    lines = [title, "| | " + " | ".join(cols) + " |", "|---" * (len(cols) + 1) + "|"]
    for row in rows:
        lines.append(f"| {row} | " + " | ".join(str(counter[(row, col)]) for col in cols) + " |")
    return lines


def summarise(cells: list[dict[str, Any]], spark: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    parts = sorted({cell["tag"].split("/")[0] for cell in cells})
    kinds = ["EQUAL", "WRONG", "RAISE", "BOTHERR", "NOSPARK"]
    for build in BUILDS:
        counts: Counter = Counter()
        for cell in cells:
            counts[(cell["tag"].split("/")[0], cell[build])] += 1
        lines += table(counts, parts, kinds, f"## verdicts on {build} by part")
        lines.append("")
    base = BUILDS[0]
    for other in BUILDS[1:]:
        moves = Counter((cell[base], cell[other]) for cell in cells if cell[base] != cell[other])
        differ = sum(1 for cell in cells if cell["differ_" + other])
        lines.append(
            f"## {base} against {other}: {sum(moves.values())} verdicts move, {differ} differ"
        )
        lines += [f"- {a} -> {b}: {n}" for (a, b), n in sorted(moves.items())]
        lost = [cell["tag"] for cell in cells if cell[base] == "EQUAL" and cell[other] != "EQUAL"]
        lines += [f"- lost EQUAL {tag}" for tag in lost[:30]]
        lines.append("")
    dims = {"ansi": 1, "door": 2, "hdr": 3, "comp": 4, "type": 5, "shape": 6}
    for dim, index in dims.items():
        counts = Counter()
        values: list[str] = []
        for cell in cells:
            value = cell["tag"].split("/")[index]
            if value not in values:
                values.append(value)
            if cell[BUILDS[0]] in {"WRONG", "RAISE"}:
                counts[(value, cell["tag"].split("/")[0] + ":" + cell[BUILDS[0]])] += 1
        cols = [f"{part}:{kind}" for part in parts for kind in ("WRONG", "RAISE")]
        lines += table(counts, values, cols, f"## {BUILDS[0]}'s WRONG and RAISE by {dim}")
        lines.append("")
    lines.append("## Spark's x by type and shape (S, csv door, header off, alone; ANSI on | off)")
    shapes = sorted({cell["tag"].split("/")[6] for cell in cells if cell["tag"].startswith("S/")})
    types = []
    for cell in cells:
        kind = cell["tag"].split("/")[5]
        if cell["tag"].startswith("S/") and kind not in types:
            types.append(kind)
    lines.append("| shape | " + " | ".join(types) + " |")
    lines.append("|---" * (len(types) + 1) + "|")
    for shape in shapes:
        row = []
        for kind in types:
            on = spark.get(f"S/true/csv/off/alone/{kind}/{shape}")
            off = spark.get(f"S/false/csv/off/alone/{kind}/{shape}")
            if on is None or off is None:
                row.append("")
                continue
            a, b = first_x(on), first_x(off)
            row.append(a if a == b else f"{a} / {b}")
        lines.append(f"| {shape} | " + " | ".join(row) + " |")
    return lines


def main() -> None:
    raw, out = Path(sys.argv[1]), Path(sys.argv[2])
    spark = load(raw, "spark")
    builds = {build: load(raw, build) for build in BUILDS}
    cells: list[dict[str, Any]] = []
    for tag, got in builds[BUILDS[0]].items():
        if tag.startswith("E/") or tag.endswith("/drop"):
            continue
        want = spark.get(tag)
        others = {b: builds[b].get(tag, {"tag": tag, "error": "missing"}) for b in BUILDS}
        cells.append(
            {
                "tag": tag,
                **{b: verdict(others[b], want) for b in BUILDS},
                **{"differ_" + b: others[b] != got for b in BUILDS[1:]},
                "main_sub": subclass(got, want),
                "spark": first_x(want) if want else "",
                "main_x": first_x(got),
                "main_err": got.get("error", ""),
                "names_differ": names_differ(got, want),
            }
        )
    by_part: dict[str, int] = defaultdict(int)
    for cell in cells:
        by_part[cell["tag"].split("/")[0]] += 1
    with (out / "verdicts.jsonl").open("w") as handle:
        for cell in cells:
            handle.write(json.dumps(cell) + "\n")
    lines = [f"cells: {len(cells)} ({dict(by_part)})", ""]
    lines += summarise(cells, spark)
    (out / "summary.txt").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
