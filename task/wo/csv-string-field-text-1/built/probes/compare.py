import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sp = {(r["ansi"], r["tag"]): r for r in map(json.loads, Path(sys.argv[1]).read_text().splitlines())}
got_all = {
    (r["ansi"], r["tag"]): r for r in map(json.loads, Path(sys.argv[2]).read_text().splitlines())
}
verbose = len(sys.argv) > 3


def strip(r: dict[str, Any]) -> dict[str, Any]:
    return {x: y for x, y in r.items() if x not in ("ansi", "tag")}


c = Counter()
for k, want in sp.items():
    got = got_all.get(k, {"error": "missing"})
    w, g = strip(want), strip(got)
    if "error" in w and "error" in g:
        v = "BOTHERR"
    elif "error" in g:
        v = "RAISE"
    elif "error" in w:
        v = "WRONG"
    else:
        v = "EQUAL" if w == g else "WRONG"
    c[v] += 1
    if verbose or (v != "EQUAL" and k[0] == "true"):
        print(v, k, json.dumps(g)[:220])
print(dict(c))
