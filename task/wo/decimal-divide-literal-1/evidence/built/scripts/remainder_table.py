import json
import re
import sys
from pathlib import Path

out_dir, tag, main, head = sys.argv[1:5]
CLASS = re.compile(r"ERR:[A-Za-z]+:([A-Z_.]+|None):")
BRACKET = re.compile(r"\[([A-Z_]+)")


def load(who: str, ansi: str) -> dict:
    found = {}
    for line in (Path(out_dir) / f"{tag}-{who}-{ansi}.jsonl").open():
        r = json.loads(line)
        if r["key"] != "provenance":
            found[r["key"]] = r["result"]
    return found


def short(result: object) -> str:
    if isinstance(result, str):
        m = BRACKET.search(result)
        return "RAISE " + (m.group(1) if m else result[:60])
    rows = ["/".join(row[1:]) if len(row) > 1 else row[0] for row in result["rows"]]
    return f"{result['types'][-1]} " + ",".join(rows)


def verdict(found: object, want: object) -> str:
    if isinstance(want, str) and isinstance(found, str):
        a, b = BRACKET.search(found), BRACKET.search(want)
        same = a and b and a.group(1) == b.group(1)
        return "BOTH_RAISE" if same else "BOTH_RAISE_CLASS"
    if isinstance(want, str):
        return "SPARK_RAISES"
    if isinstance(found, str):
        return "REPARK_RAISES"
    if found["types"] != want["types"]:
        return "TYPE" if found["rows"] == want["rows"] else "TYPE+VALUE"
    return "EQUAL" if found["rows"] == want["rows"] else "VALUE"


print("| cell | ANSI | Spark | main | head | main vs Spark | head vs Spark |")
print("|---|---|---|---|---|---|---|")
for ansi in ("true", "false"):
    sp, mn, hd = load("spark", ansi), load(main, ansi), load(head, ansi)
    for key in sp:
        print(
            f"| `{key}` | {ansi} | {short(sp[key])} | {short(mn[key])} | {short(hd[key])} | "
            f"{verdict(mn[key], sp[key])} | {verdict(hd[key], sp[key])} |"
        )
