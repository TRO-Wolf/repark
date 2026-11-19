import json
import sys
from collections import Counter
from pathlib import Path

raw = Path(sys.argv[1])
builds = sys.argv[2:]


def load(name: str) -> dict:
    found = {}
    for line in (raw / (name + "-X.jsonl")).read_text().splitlines():
        record = json.loads(line)
        found[record["tag"]] = record
    return found


def answer(record: dict | None) -> str:
    if record is None:
        return "MISSING"
    if "plan" in record:
        return record["plan"]
    if "error" in record or "create_error" in record:
        return "RAISE " + (record.get("error") or record.get("create_error"))[:90]
    return " ".join(record["rows"])


def verdict(want: dict | None, got: dict | None) -> str:
    if want is None or got is None:
        return "MISSING"
    if "plan" in want:
        return "PLAN"
    want_error = "error" in want or "create_error" in want
    got_error = "error" in got or "create_error" in got
    if want_error:
        return "EQUAL" if got_error else "WRONG"
    if got_error:
        return "RAISE"
    return "EQUAL" if want["rows"] == got["rows"] else "WRONG"


spark = load("spark")
data = {name: load(name) for name in builds}
counts = {name: Counter() for name in builds}
for tag in sorted(spark):
    if tag.startswith("E/"):
        continue
    verdicts = {name: verdict(spark[tag], data[name].get(tag)) for name in builds}
    for name in builds:
        counts[name][verdicts[name]] += 1
    moved = len({answer(data[name].get(tag)) for name in builds}) > 1
    print(
        ("MOVED " if moved else "      ") + tag,
        "| spark:",
        answer(spark[tag])[:80],
        *[
            "| " + name + " " + verdicts[name] + ": " + answer(data[name].get(tag))[:80]
            for name in builds
        ],
    )
for name in builds:
    print(name, dict(counts[name]))
