import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

CLASSES = {"E": "EQUAL", "W": "WRONG", "X": "WRONG", "R": "RAISE", "C": "RAISE", "V": "VARIES"}


def read(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def family(query: str) -> tuple[str, str]:
    table, column, fam, fn, _ = query.split("/")
    name = table if table in ("big", "t4") else ("over" if table == "over" else "t")
    return f"{name} {fam} {fn}", column if name == "t" else name


def counts(rows: list[dict[str, str]], field: str) -> dict[tuple[str, str, str], Counter[str]]:
    out: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        fam, column = family(row["query"])
        for code in row[field].split():
            out[(fam, column, row["ansi"])][CLASSES[code[0]] + ("*" if code == "X" else "")] += 1
    return out


def render_counts(rows: list[dict[str, str]]) -> list[str]:
    one, four = counts(rows, "vs_local1"), counts(rows, "vs_local4")
    lines = [
        "| family | column | ANSI | cells | vs `local[1]` | vs `local[4]` |",
        "|---|---|---|---|---|---|",
    ]
    for key in sorted(one):
        if set(one[key]) == {"EQUAL"} and set(four[key]) == {"EQUAL"}:
            continue
        total = sum(one[key].values())
        a = ", ".join(f"{k} {v}" for k, v in sorted(one[key].items()))
        b = ", ".join(f"{k} {v}" for k, v in sorted(four[key].items()))
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {total} | {a} | {b} |")
    return lines


def render_varies(rows: list[dict[str, str]]) -> list[str]:
    lines = [
        "| query | ANSI | configurations that vary (answers) | answers seen |",
        "|---|---|---|---|",
    ]
    for row in rows:
        names = row["configs"].split(",")
        codes = row["vs_local1"].split()
        varied = [f"{n} ({c[1:]})" for n, c in zip(names, codes, strict=True) if c.startswith("V")]
        if varied:
            lines.append(
                f"| `{row['query']}` | {row['ansi']} | {', '.join(varied)} | {row['answers']} |"
            )
    return lines


def render_inexact(rows: list[dict[str, str]]) -> list[str]:
    lines = [
        "| query | ANSI | steady configurations not exact | answers seen |",
        "|---|---|---|---|",
    ]
    for row in rows:
        names = row["configs"].split(",")
        bad = [n for n, e in zip(names, row["exact"], strict=True) if e == "n"]
        if bad:
            shown = "all 11" if len(bad) == len(names) else ", ".join(bad)
            lines.append(f"| `{row['query']}` | {row['ansi']} | {shown} | {row['answers']} |")
    return lines


def render_model(rows: list[dict[str, str]]) -> list[str]:
    tally: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        fam, column = family(row["query"])
        for master, field in (("local[1]", "model_vs_local1"), ("local[4]", "model_vs_local4")):
            code = row[field]
            if code != "E" and code != "-":
                tally[(fam, column, row["ansi"])][f"{master} {CLASSES[code[0]]}"] += 1
    lines = ["| family | column | ANSI | exact answer differs from Spark |", "|---|---|---|---|"]
    for key in sorted(tally):
        listed = ", ".join(f"{k} {v}" for k, v in sorted(tally[key].items()))
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {listed} |")
    return lines


def kind(model: str, spark: str) -> str:
    m_err = model.startswith(("ERR", "stmt ERR"))
    s_err = spark.startswith(("ERR", "stmt ERR"))
    if m_err and s_err:
        return "BOTH_RAISE_OTHER_CONDITION"
    if s_err:
        return "EXACT_ANSWERS_SPARK_RAISES"
    if m_err:
        return "EXACT_RAISES_SPARK_ANSWERS"
    if nulls(spark) > nulls(model):
        return "EXACT_VALUE_SPARK_NULL"
    if nulls(model) > nulls(spark):
        return "EXACT_NULL_SPARK_VALUE"
    return "EXACT_VALUE_SPARK_OTHER"


def nulls(text: str) -> int:
    found = re.search(r"nulls=(\d+)", text)
    if found:
        return int(found.group(1))
    return text.count("NULL") + text.count("null")


def render_classes(rows: list[dict[str, str]]) -> list[str]:
    lines = [
        "| query | ANSI | exact (seat A) | Spark `local[1]` | class vs `local[1]` "
        "| Spark `local[4]` | class vs `local[4]` |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        fn = row["query"].split("/")[3]
        if row["query"].startswith("over/") or fn not in ("avg", "try_avg"):
            continue
        model, one, four = row["model"], row["local1"], row["local4"]
        if one == model and four == model:
            continue
        k1 = "EQUAL" if one == model else kind(model, one)
        k4 = "EQUAL" if four == model else kind(model, four)
        cells = [f"`{row['query']}`", row["ansi"], model, one, k1, four, k4]
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def main() -> None:
    which, path = sys.argv[1], Path(sys.argv[2])
    rows = read(path)
    render = {
        "counts": render_counts,
        "varies": render_varies,
        "inexact": render_inexact,
        "model": render_model,
        "classes": render_classes,
    }[which]
    print("\n".join(render(rows)))


if __name__ == "__main__":
    main()
