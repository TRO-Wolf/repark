from __future__ import annotations

import json
import sys
import textwrap
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def clip(text: str, width: int) -> str:
    return textwrap.shorten(text, width, placeholder=" …")


FRAMES = ("sql", "filter", "select", "union")


def load(path: str) -> list[dict[str, Any]]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def short_text(record: dict[str, Any]) -> str:
    outcome = record["outcome"]
    if outcome["kind"] != "refused":
        return ""
    first = outcome["text"].splitlines()[0]
    return f"{outcome['class']}: {clip(first, 120)}"


def state_text(state: dict[str, Any]) -> str:
    rows = state.get("rows")
    shown = "absent" if rows is None else str(len(rows))
    parts = [f"rows {shown}"]
    if state.get("snapshots") is not None:
        parts.append(f"snap {state['snapshots']}")
    if "data_files" in state:
        parts.append(f"files {state['data_files']}")
    return ", ".join(parts)


def cell_text(records: dict[str, dict[str, Any]]) -> str:
    verdicts = {frame: records[frame]["verdict"] for frame in FRAMES if frame in records}
    if len(set(verdicts.values())) == 1:
        return next(iter(verdicts.values()))
    return "; ".join(f"{frame}: {value}" for frame, value in verdicts.items())


def reach_lines(records: list[dict[str, Any]]) -> list[str]:
    lines = ["#### Partitioned-table reach on this door", "", "| Attempt | Outcome |", "|---|---|"]
    for record in records:
        outcome = record["outcome"]
        if outcome["kind"] == "refused":
            text = f"{outcome['class']}: {clip(outcome['text'].splitlines()[0], 140)}"
        else:
            text = f"returned {outcome['rows']}"
        lines.append(f"| {record['reach']} | {text.replace('|', chr(92) + '|')} |")
    lines.append("")
    return lines


def workaround_lines(records: list[dict[str, Any]]) -> list[str]:
    lines = [
        "#### Workarounds on this door (`repark.sql(...)` collected)",
        "",
        "| Target | Statement | Outcome | Before → after |",
        "|---|---|---|---|",
    ]
    for record in records:
        outcome = record["outcome"]
        text = "returned"
        if outcome["kind"] == "refused":
            text = f"{outcome['class']}: {clip(outcome['text'].splitlines()[0], 120)}"
        change = f"{state_text(record['before'])} → {state_text(record['after'])}"
        text = text.replace("|", chr(92) + "|")
        lines.append(f"| {record['target_kind']} | {record['workaround']} | {text} | {change} |")
    lines.append("")
    return lines


def table(all_records: list[dict[str, Any]], title: str) -> list[str]:
    records = [record for record in all_records if "verdict" in record]
    reach = [record for record in all_records if "reach" in record]
    workarounds = [record for record in all_records if "workaround" in record]
    grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for record in records:
        grouped[(record["target_kind"], record["verb"])][record["frame"]] = record
    lines = [
        f"### {title}",
        "",
        "| Target | Verb | Verdict (all four frames unless split) | Before → after (sql frame) "
        "| Refusal (sql frame) |",
        "|---|---|---|---|---|",
    ]
    for (kind, verb), frames in grouped.items():
        sample = frames["sql"]
        change = f"{state_text(sample['before'])} → {state_text(sample['after'])}"
        text = short_text(sample).replace("|", "\\|")
        lines.append(f"| {kind} | {verb} | {cell_text(frames)} | {change} | {text} |")
    counts = Counter(record["verdict"] for record in records)
    lines.append("")
    lines.append(
        "Counts: " + ", ".join(f"{name} {count}" for name, count in sorted(counts.items()))
    )
    lines.append("")
    if reach:
        lines.extend(reach_lines(reach))
    if workarounds:
        lines.extend(workaround_lines(workarounds))
    return lines


def main() -> None:
    title = sys.argv[1]
    out = [f"# {title}", ""]
    for path in sys.argv[2:]:
        records = load(path)
        cells = sum(1 for record in records if "verdict" in record)
        out.extend(table(records, f"Door `{records[0]['door']}` ({cells} cells)"))
    sys.stdout.write("\n".join(out).rstrip("\n") + "\n")


if __name__ == "__main__":
    main()
