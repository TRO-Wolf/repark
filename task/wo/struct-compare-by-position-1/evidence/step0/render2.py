from __future__ import annotations

import collections
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import drive_cmp as d  # noqa

CLASH_SAME_ORDER = ("int_string|same_order", "int_boolean|same_order")
LABELS = [
    "EQUAL",
    "WRONG-ANSWER",
    "WRONG-WRITE",
    "ACCEPTED-WHERE-SPARK-REFUSED",
    "REFUSED-WHERE-SPARK-ANSWERED",
    "BOTH-REFUSED",
    "ORACLE-MISSING",
    "SNAPSHOT-DIFF",
    "PANIC",
]
DERIVED = ["DERIVED-EQUAL", "DERIVED-WRONG-WRITE", "DERIVED-REFUSED", "DERIVED-SPARK-REFUSES"]


def load(path: Path) -> tuple[dict, dict]:
    main, derived = {}, {}
    for line in path.read_text().splitlines():
        if not line:
            continue
        record = json.loads(line)
        if "derived" in record:
            derived[record["key"]] = record
        else:
            main[record["key"]] = record
    return main, derived


def derived_label(main_record: dict, derived_record: dict) -> str:
    body = derived_record["derived"]
    if body["twin"] == "error":
        return "DERIVED-SPARK-REFUSES"
    if main_record["kind"] != "ok":
        return "DERIVED-REFUSED"
    if main_record["post_rows"] == body["expected_rows"]:
        return "DERIVED-EQUAL"
    return "DERIVED-WRONG-WRITE"


def label_for(cell: dict, spark: dict, main: dict) -> str:
    return d.label_of(cell, spark, main)


def main() -> None:
    cells = [json.loads(line) for line in (HERE / "cells.jsonl").read_text().splitlines() if line]
    spark, spark_derived = load(HERE / "raw" / "spark.jsonl")
    main_raw, _ = load(HERE / "raw" / "main.jsonl")
    counts = collections.Counter()
    seat_door = collections.defaultdict(collections.Counter)
    derived_seat_shape = collections.defaultdict(collections.Counter)
    derived_counts = collections.Counter()
    oracle_derived = {}
    controls: dict[str, list[str]] = collections.defaultdict(list)
    groups: dict[tuple, int] = collections.Counter()
    rows_md = []
    for cell in cells:
        cell = dict(cell)
        if cell["types"] in CLASH_SAME_ORDER and cell["family"] == "typed":
            cell["shape"] = "clash_same_order"
            cell["control"] = ""
        key = cell["key"]
        label = label_for(cell, spark[key], main_raw[key])
        counts[label] += 1
        seat_door[(cell["seat"], cell["door"])][label] += 1
        derived_text = ""
        if label == "ORACLE-MISSING" and key in spark_derived:
            derived_text = derived_label(main_raw[key], spark_derived[key])
            derived_counts[derived_text] += 1
            derived_seat_shape[(cell["seat"], cell["shape"])][derived_text] += 1
            oracle_derived[key] = (cell["seat"], cell["shape"], derived_text)
        if label in ("WRONG-ANSWER", "WRONG-WRITE", "PANIC", "ORACLE-MISSING", "SNAPSHOT-DIFF"):
            groups[(label, cell["seat"], cell["door"], cell["shape"])] += 1
        name = cell["control"]
        if cell["seat"] == "control" or cell["family"] == "brief":
            name = (
                cell["shape"]
                if cell["seat"] in ("control",)
                else cell["shape"]
                if cell["family"] == "brief"
                else name
            )
        if name == "control":
            name = "same_order_int" if cell["types"] == "int_int|same_order" else "same_order_typed"
        if name:
            controls[name].append(f"{key}:{label}")
        outcome_s = d.outcome_text(spark[key]).replace("|", "/")
        outcome_m = d.outcome_text(main_raw[key]).replace("|", "/")
        stmt = cell["stmt"].replace("|", "\\|")
        rows_md.append(
            f"| {key} | {cell['family']} | {cell['shape']} | {cell['types']} | {cell['op']} | "
            f"{cell['seat']} | "
            f"{cell['door']} | {cell['case']} | {cell['ansi']} | `{stmt}` | {outcome_s} | "
            f"{outcome_m} | {label} | {derived_text} |"
        )
    lines = [
        "# Struct comparison step 0 table",
        "",
        f"Cells: {len(cells)} (pass 1: 4389; pass 2 appended: {len(cells) - 4389})",
        "",
        "## Label counts",
        "",
    ]
    lines += [f"- {label}: {counts[label]}" for label in LABELS if counts[label]]
    lines += [
        "",
        "## Labels by seat x door",
        "",
        "| seat | door | label | n |",
        "|---|---|---|---|",
    ]
    for (seat, door), labels in sorted(seat_door.items()):
        for label in LABELS:
            if labels[label]:
                lines.append(f"| {seat} | {door} | {label} | {labels[label]} |")
    lines += [
        "",
        "## Derived oracle (ORACLE-MISSING DELETE cells), by seat x shape",
        "",
        "| seat | shape | label | n |",
        "|---|---|---|---|",
    ]
    for (seat, shape), labels in sorted(derived_seat_shape.items()):
        for label, n in sorted(labels.items()):
            lines.append(f"| {seat} | {shape} | {label} | {n} |")
    lines += ["", f"Derived totals: {dict(derived_counts)}", ""]
    lines += ["## Controls", ""]
    for name in sorted(controls):
        labels = collections.Counter(item.split(":", 1)[1] for item in controls[name])
        keys = ", ".join(item.split(":", 1)[0] for item in controls[name][:12])
        lines.append(
            f"- {name}: {dict(labels)} (n={len(controls[name])}; keys "
            f"{keys}{' ...' if len(controls[name]) > 12 else ''})"
        )
    lines += ["", "## Wrong and oracle-missing groups (label, seat x door x shape, cells)", ""]
    for (label, seat, door, shape), n in sorted(groups.items()):
        lines.append(f"- {label} {seat} x {door} x {shape}: {n}")
    lines += [
        "",
        "## Cells",
        "",
        "| key | family | shape | types | op | seat | door | case | ansi | statement | spark | "
        "main | label | derived |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    lines += rows_md
    (HERE / "table.md").write_text("\n".join(lines) + "\n")
    group_lines = [
        f"{label} | {seat} x {door} x {shape} | {n}"
        for (label, seat, door, shape), n in sorted(groups.items())
    ]
    (HERE / "groups.txt").write_text("\n".join(group_lines) + "\n")
    print(json.dumps({"counts": counts, "derived": derived_counts}, sort_keys=True))


if __name__ == "__main__":
    main()
