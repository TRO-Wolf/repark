import json
import sys
from pathlib import Path

import cells

raw = Path(sys.argv[1])
CODE = {
    "agree": "=",
    "both_raise": "B",
    "wrong_answer": "W",
    "wrong_type": "T",
    "wrong_write": "S",
    "over_raise": "R",
    "missed_raise": "M",
}
verdicts = [json.loads(line) for line in (raw / "verdicts.jsonl").open()]
index = {(v["engine"], v["ansi"], v["key"]): v for v in verdicts}
pairs = [p[0] for p in cells.PAIRS]
positions = list(cells.EXPRS) + list(cells.PLANS)
out = []
for engine in ("repark", "native"):
    for table in ("t", "tx"):
        out.append(f"### {engine}, table `{table}` (ANSI off; `x/y` where ANSI on differs)")
        out.append("")
        out.append("| position | " + " | ".join(pairs) + " |")
        out.append("|---|" + "---|" * len(pairs))
        for pos in positions:
            row = []
            for p in pairs:
                off = index.get((engine, "false", f"pair/{p}/{table}/{pos}"))
                on = index.get((engine, "true", f"pair/{p}/{table}/{pos}"))
                if off is None:
                    row.append("")
                    continue
                a, b = CODE[off["verdict"]], CODE[on["verdict"]] if on else "?"
                row.append(a if a == b else f"{a}/{b}")
            if any(row):
                out.append(f"| `{pos}` | " + " | ".join(row) + " |")
        out.append("")
for group in ("three", "agg", "values", "literal", "special", "const", "dfconst", "df", "ice"):
    out.append(f"### group `{group}` (RePark Spark door; ANSI off, then on)")
    out.append("")
    out.append("| cell | off | on | native off |")
    out.append("|---|---|---|---|")
    keys = sorted(
        {
            k
            for (e, a, k) in index
            if e == "repark" and a == "false" and k.split("/")[0].startswith(group)
        }
    )
    for k in keys:
        off, on = index[("repark", "false", k)], index.get(("repark", "true", k))
        nat = index.get(("native", "false", k))
        out.append(
            f"| `{k}` | {CODE[off['verdict']]} | {CODE[on['verdict']] if on else ''} | "
            f"{CODE[nat['verdict']] if nat else ''} |"
        )
    out.append("")
(raw / "grids.md").write_text("# Grids (written by grids.py)\n\n" + "\n".join(out) + "\n")
print(len(out))
