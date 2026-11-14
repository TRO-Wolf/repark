from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

NEG = {"d": "8000000000000000", "f": "80000000"}
POS = {"d": "0000000000000000", "f": "00000000"}
ONE_FIVE = {"d": "3ff8000000000000", "f": "3fc00000"}
TWO = {"d": "4000000000000000", "f": "40000000"}

BASE = {1: "neg", 2: "pos", 3: "one_five", 4: None, 5: "pos", 6: "neg"}
PAIRS = {1: "neg", 2: "pos", 3: "one_five", 4: None, 7: "neg", 8: "pos", 9: "one_five", 10: None}
CROSS = {10: "neg", 11: "one_five", 1: "pos"}
SURVIVORS = {1: "neg", 2: "pos", 3: "one_five", 4: None}

EXPECTED = {
    "insert_values": BASE,
    "insert_values_pos_first": BASE,
    "insert_select": BASE,
    "insert_overwrite_static": BASE,
    "insert_overwrite_dynamic_cross": CROSS,
    "ctas": BASE,
    "rtas": BASE,
    "wt_append": BASE,
    "wt_create": BASE,
    "wt_create_or_replace": BASE,
    "wt_overwrite_partitions": {**BASE, 99: "two"},
    "wt_overwrite_partitions_cross": CROSS,
    "sat_append": BASE,
    "sat_overwrite": BASE,
    "merge_insert": BASE,
    "merge_update_cow": PAIRS,
    "merge_update_mor": PAIRS,
    "update_cow": PAIRS,
    "update_mor": PAIRS,
    "delete_cow": SURVIVORS,
    "delete_mor": SURVIVORS,
    "delete_mor_partition_granularity": SURVIVORS,
    "insert_overwrite_static_partition_cross": CROSS,
    "wt_overwrite_filter_cross": CROSS,
    "rewrite_data_files": PAIRS,
    "branch_insert": {**BASE, 99: "two"},
    "native_insert": BASE,
}


DELETING = {"update_mor", "merge_update_mor", "delete_mor", "delete_mor_partition_granularity"}


def expected_bits(kind: str | None, col: str) -> str | None:
    if kind is None:
        return None
    return {"neg": NEG, "pos": POS, "one_five": ONE_FIVE, "two": TWO}[kind][col]


def load(path: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    header: dict[str, Any] = {}
    cells: dict[str, dict[str, Any]] = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record.get("leg") == "setup":
            header = record
        elif record.get("leg") in ("doors", "cross_read"):
            cells[record["table"]] = record
    return header, cells


def zero_layout(files: list[list[Any]], col: str) -> str:
    counts: dict[tuple[int, str], int] = defaultdict(int)
    for item in files:
        content, partition, count = item[0], item[1], item[2]
        if partition in (NEG[col], POS[col]):
            counts[(content or 0, "-0" if partition == NEG[col] else "+0")] += count
    data = " ".join(f"{sign}:{counts[(0, sign)]}" for sign in ("-0", "+0") if (0, sign) in counts)
    deletes = " ".join(
        f"{sign}:{counts[(1, sign)]}" for sign in ("-0", "+0") if (1, sign) in counts
    )
    return data + (f" del {deletes}" if deletes else "")


def verdict(record: dict[str, Any] | None, door: str, col: str) -> tuple[str, str]:
    if record is None:
        return "absent", ""
    if record.get("outcome") != "ok":
        return "error", record.get("error", "")[:120]
    checks = record.get("checks", {})
    read = checks.get("read", {})
    files = checks.get("files", [])
    if not isinstance(read, dict) or "error" in read or not isinstance(files, list):
        return "unread", json.dumps(read)[:120]
    expected = EXPECTED[door]
    wrong = []
    for ident, kind in expected.items():
        got = read.get(str(ident))
        if got is None:
            wrong.append(f"{ident}:missing")
            continue
        want_d, want_f = expected_bits(kind, "d"), expected_bits(kind, "f")
        if got[0] != want_d or got[1] != want_f:
            wrong.append(f"{ident}:{got[0]}/{got[1]}")
    extra = sorted(set(read) - {str(i) for i in expected})
    wrong += [f"{ident}:extra" for ident in extra]
    layout = zero_layout(files, col)
    data_signs = {item[1] for item in files if (item[0] or 0) == 0}
    delete_signs = {item[1] for item in files if item[0] == 1}
    want_signs = {expected_bits(kind, col) for kind in expected.values() if kind in ("neg", "pos")}
    want_delete_signs = {NEG[col], POS[col]} if door in DELETING else set()
    if any(item.endswith(":missing") for item in wrong):
        status = "LOST"
    elif not want_signs <= data_signs:
        status = "MERGED"
    elif wrong:
        status = "MIXED"
    elif not want_delete_signs <= delete_signs:
        status = "DEL-MERGED"
    else:
        status = "ok"
    return status, f"{layout}; {', '.join(wrong) if wrong else 'rows ok'}"


def short(bits_text: Any) -> str:
    names = {
        "8000000000000000": "-0.0",
        "0000000000000000": "+0.0",
        "80000000": "-0.0f",
        "00000000": "+0.0f",
        "3ff8000000000000": "1.5",
        "3fc00000": "1.5f",
        "4000000000000000": "2.0",
        "40000000": "2.0f",
        None: "NULL",
    }
    return names.get(bits_text, str(bits_text))


def layouts(runs: list[Path], variant: str) -> None:
    loaded = [(path.stem, load(path)[1]) for path in runs]
    print("| door | run | files: content partition rows | rows read: id d f |")
    print("|---|---|---|---|")
    for door in EXPECTED:
        name = f"{door}_{variant}"
        for stem, cells in loaded:
            record = cells.get(name)
            if record is None:
                continue
            if record.get("outcome") != "ok":
                print(f"| {door} | {stem} | error: {record.get('error', '')[:90]} | |")
                continue
            checks = record["checks"]
            files = checks.get("files")
            file_text = (
                "; ".join(
                    f"{'del' if item[0] == 1 else 'data'} {short(item[1])} {item[2]}"
                    for item in files
                )
                if isinstance(files, list)
                else str(files)[:90]
            )
            read = checks.get("read", {})
            read_text = (
                "; ".join(
                    f"{ident} {short(value[0])} {short(value[1])}"
                    for ident, value in sorted(read.items(), key=lambda kv: int(kv[0]))
                )
                if isinstance(read, dict) and "error" not in read
                else str(read)[:90]
            )
            print(f"| {door} | {stem} | {file_text} | {read_text} |")


def main() -> int:
    if sys.argv[1] == "--layouts":
        layouts([Path(arg) for arg in sys.argv[3:]], sys.argv[2])
        return 0
    runs = [Path(arg) for arg in sys.argv[1:]]
    loaded = [(path.stem, *load(path)) for path in runs]
    doors = list(EXPECTED)
    lines = [
        "| door | " + " | ".join(stem for stem, _, _ in loaded) + " |",
        "|---" * (len(loaded) + 1) + "|",
    ]
    detail: list[str] = []
    for door in doors:
        summary = []
        for stem, _, cells in loaded:
            mine = {
                name: rec
                for name, rec in cells.items()
                if rec.get("door") == door or name.startswith(door + "_v")
            }
            mine = {name: rec for name, rec in mine.items() if name.rsplit("_", 3)[0] == door}
            if not mine:
                summary.append("—")
                continue
            tally: dict[str, int] = defaultdict(int)
            for name, rec in sorted(mine.items()):
                col = name.rsplit("_", 1)[1]
                status, note = verdict(rec, door, col)
                tally[status] += 1
                if status != "ok":
                    detail.append(f"| {stem} | {name} | {status} | {note} |")
            summary.append(" ".join(f"{k} {v}" for k, v in sorted(tally.items())))
        lines.append(f"| {door} | " + " | ".join(summary) + " |")
    print("\n".join(lines))
    print()
    print("| run | table | status | zero layout (data, deletes); rows read wrong (id:d/f bits) |")
    print("|---|---|---|---|")
    print("\n".join(detail))
    return 0


if __name__ == "__main__":
    sys.exit(main())
