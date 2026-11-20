import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from revision import ORACLE_TYPES, TEXTS, ZONE_TEXTS, ZONE_TYPES, ZONES

PATH = re.compile(r"/?tmp/\S*?/csv-string-out/work/data2")


def load(raw: Path, engine: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(raw.glob(f"{engine}-*.jsonl")):
        mode = path.stem.rsplit("-", 1)[-1] if "-oracle-" in path.name else ""
        for line in path.read_text().splitlines():
            record = json.loads(PATH.sub("<data>", line))
            if mode:
                record["tag"] = record["tag"].replace("O/", f"O/{mode}/", 1)
            out[record["tag"]] = record
    return out


def x_of(record: dict[str, Any] | None) -> str:
    if record is None:
        return "?"
    if "error" in record or "create_error" in record:
        return "RAISE " + record.get("error", record.get("create_error", ""))[:40].split(" (")[0]
    if not record["rows"]:
        return "no rows"
    return ";".join(str(cols[0]) for cols in record["cols"]).replace("|", "/")


def whole(record: dict[str, Any] | None) -> str:
    if record is None:
        return "?"
    if "error" in record or "create_error" in record:
        return "RAISE " + record.get("error", record.get("create_error", ""))[:60]
    return f"{record['names']} {record['cols']}".replace("|", "/")


def same(a: dict[str, Any] | None, b: dict[str, Any] | None) -> bool:
    if a is None or b is None:
        return False
    a_err = "error" in a or "create_error" in a
    b_err = "error" in b or "create_error" in b
    if a_err or b_err:
        return a_err and b_err
    return a["rows"] == b["rows"] and a["types"] == b["types"]


def show(text: str) -> str:
    return "`" + text.replace("\t", "\\t").replace("|", "/") + "`" if text.strip() else repr(text)


def oracle(spark: dict, unit1: dict, out: list[str]) -> None:
    pairs = Counter()
    for ansi in ("true", "false"):
        for kind in ORACLE_TYPES:
            for index in range(len(TEXTS)):
                want = spark.get(f"O/{ansi}/spark/{kind}/{index}")
                for variant in ("base", "text_try"):
                    got = unit1.get(f"O/{ansi}/{variant}/{kind}/{index}")
                    pairs[(variant, ansi, same(got, want))] += 1
    ansi_diff = sum(
        1
        for kind in ORACLE_TYPES
        for index in range(len(TEXTS))
        if not same(
            spark.get(f"O/true/spark/{kind}/{index}"), spark.get(f"O/false/spark/{kind}/{index}")
        )
    )
    out.append(f"Spark cells differing between ANSI on and off: {ansi_diff}")
    for variant in ("base", "text_try"):
        for ansi in ("true", "false"):
            hit, miss = pairs[(variant, ansi, True)], pairs[(variant, ansi, False)]
            out.append(f"- unit 1 {variant}, ANSI {ansi}: {hit} EQUAL, {miss} not")
    out.append("")
    short = [kind.replace("INTERVAL ", "").replace(" TO ", "-") for kind in ORACLE_TYPES]
    out.append("| text | " + " | ".join(short) + " |")
    out.append("|---" * (len(short) + 1) + "|")
    for index, text in enumerate(TEXTS):
        row = []
        for kind in ORACLE_TYPES:
            want = spark.get(f"O/true/spark/{kind}/{index}")
            value = x_of(want)
            base = same(unit1.get(f"O/true/base/{kind}/{index}"), want)
            trial = same(unit1.get(f"O/true/text_try/{kind}/{index}"), want)
            mark = ("" if base else "†") + ("" if trial else "‡")
            row.append(value + mark)
        out.append(f"| {show(text)} | " + " | ".join(row) + " |")


def zones(spark: dict, unit1: dict, out: list[str]) -> None:
    counts = Counter()
    lines = []
    for zone in ZONES:
        for comp in ("alone", "beside"):
            for kind in ZONE_TYPES:
                for index, text in enumerate(ZONE_TEXTS):
                    key = f"{zone}/{comp}/{kind}/{index}"
                    want = spark.get(f"Z/spark/{key}")
                    base = unit1.get(f"Z/base/{key}")
                    trial = unit1.get(f"Z/text_try/{key}")
                    counts[("base", same(base, want))] += 1
                    counts[("text_try", same(trial, want))] += 1
                    if not same(base, want) or not same(trial, want):
                        lines.append(
                            f"| {zone.split('/')[-1]} | {comp} | {kind} | {show(text)} | "
                            f"{x_of(want)} | {x_of(base)} | {x_of(trial)} |"
                        )
    total = counts[("base", True)] + counts[("base", False)]
    hits = counts[("base", True)], counts[("text_try", True)]
    out.append(f"base EQUAL {hits[0]} of {total}; all-text + TRY_CAST EQUAL {hits[1]}")
    out.append("")
    out.append("| zone | companion | type | text | Spark | unit 1 | all-text + `TRY_CAST` |")
    out.append("|---|---|---|---|---|---|---|")
    out.extend(lines)


def listing(
    spark: dict, unit1: dict, prefix: str, variants: tuple[str, ...], out: list[str]
) -> None:
    lead = prefix + "spark/"
    keys = sorted(tag[len(lead) :] for tag in spark if tag.startswith(lead) and "/using" not in tag)
    head = "| cell | Spark | " + " | ".join(variants) + " |"
    out += [head, "|---" * (len(variants) + 2) + "|"]
    for key in keys:
        want = spark.get(f"{prefix}spark/{key}")
        row = [whole(want)]
        for variant in variants:
            got = unit1.get(f"{prefix}{variant}/{key}")
            row.append("EQUAL" if same(got, want) else "**" + whole(got) + "**")
        out.append(f"| {key} | " + " | ".join(row) + " |")


def main() -> None:
    raw = Path(sys.argv[1])
    spark, main_build, unit1 = load(raw, "spark"), load(raw, "main"), load(raw, "unit1")
    differ = sum(1 for tag, record in main_build.items() if unit1.get(tag) != record)
    out = [f"main against unit 1: {differ} of {len(main_build)} records differ", ""]
    out += ["## oracle"]
    oracle(spark, unit1, out)
    out += ["", "## zones"]
    zones(spark, unit1, out)
    out += ["", "## topology"]
    listing(spark, unit1, "P/", ("base", "scan0"), out)
    for ansi in ("true", "false"):
        out += ["", f"## pairing, ANSI {ansi}"]
        listing(spark, unit1, f"Q/{ansi}/", ("base", "text_cast"), out)
    out += ["", "## extra"]
    listing(spark, unit1, "X/", ("base", "text_cast"), out)
    out += ["", "## USING csv"]
    for key in ("using", "using_cols"):
        want, got = whole(spark.get("X/spark/" + key)), whole(unit1.get("X/repark/" + key))
        out.append(f"- {key}: Spark {want}; unit 1 {got}")
    print("\n".join(out))


if __name__ == "__main__":
    main()
