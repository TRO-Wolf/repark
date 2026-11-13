from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path
from typing import Any

RENAMED_SEATS = {"matrix": "bare"}


def read(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def cell_key(record: dict[str, Any], rename: bool) -> tuple[str, str, str]:
    cid = record["cid"]
    if rename and record.get("group") == "floatkeys":
        for old, new in RENAMED_SEATS.items():
            if cid.endswith("|" + old):
                cid = cid[: -len(old)] + new
    return cid, record["route"], record["layout"]


def cells(paths: list[Path], rename: bool) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in paths:
        for record in read(path):
            if record.get("record") == "cell":
                out[cell_key(record, rename)] = record
    return out


def labels(path: Path, rename: bool) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    for row in read(path):
        if row["engine"] != "repark":
            continue
        fake = {"cid": row["cid"], "route": row["route"], "layout": "", "group": row["group"]}
        out[cell_key(fake, rename)[:2]] = row["label"]
    return out


def canon(rows: list[Any]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


def error_text(record: dict[str, Any]) -> str:
    error = record.get("error") or {}
    return str(error.get("message", ""))[:160] if isinstance(error, dict) else str(error)[:160]


def compare(old: dict, new: dict, old_labels: dict, new_labels: dict) -> list[dict[str, Any]]:
    found = []
    for key in sorted(set(old) | set(new)):
        before, after = old.get(key), new.get(key)
        label_before = old_labels.get(key[:2])
        label_after = new_labels.get(key[:2])
        if before is None or after is None:
            found.append(
                {
                    "key": key,
                    "kind": "missing-before" if before is None else "missing-after",
                    "label_before": label_before,
                    "label_after": label_after,
                }
            )
            continue
        kind = None
        if before["outcome"] != after["outcome"]:
            kind = "outcome"
        elif before["outcome"] != "ANSWERED":
            if error_text(before) != error_text(after):
                kind = "error-text"
        elif before.get("cols") != after.get("cols"):
            kind = "columns"
        elif before.get("rows") != after.get("rows"):
            kind = "row-order-only" if canon(before["rows"]) == canon(after["rows"]) else "rows"
        if label_before != label_after:
            kind = "label" if kind is None else kind + "+label"
        if kind is not None:
            found.append(
                {
                    "key": key,
                    "kind": kind,
                    "label_before": label_before,
                    "label_after": label_after,
                    "before": (
                        before.get("rows")
                        if before["outcome"] == "ANSWERED"
                        else error_text(before)
                    ),
                    "after": (
                        after.get("rows") if after["outcome"] == "ANSWERED" else error_text(after)
                    ),
                }
            )
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old", action="append", type=Path, required=True)
    parser.add_argument("--new", action="append", type=Path, required=True)
    parser.add_argument("--old-labels", type=Path, required=True)
    parser.add_argument("--new-labels", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    old = cells(arguments.old, rename=True)
    new = cells(arguments.new, rename=False)
    found = compare(
        old, new, labels(arguments.old_labels, True), labels(arguments.new_labels, False)
    )
    arguments.out.write_text(
        "".join(json.dumps(x, sort_keys=True, default=str) + "\n" for x in found), encoding="utf-8"
    )
    counts = collections.Counter(x["kind"] for x in found)
    print(
        f"old cells {len(old)}; new cells {len(new)}; differences {len(found)}: "
        f"{dict(sorted(counts.items()))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
