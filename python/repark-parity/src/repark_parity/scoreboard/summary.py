from __future__ import annotations

import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any

from repark_parity.scoreboard.classify import CLASSES
from repark_parity.scoreboard.encoding import read_records

FORMAT = "spark-door-scoreboard/1"
REPORT_FORMAT = "spark-door-scoreboard-comparison/1"
FAMILY_FIELDS = ("rule_id", "card", "record_ids", "variant")
ANSWER_FIELDS = ("outcome", "value", "schema", "error_class", "sqlstate", "physical_write")
UNCOUNTED = ("RECORDED-EXCLUSION", "FLAKY")
NOT_COMPARABLE = ("EXCLUDED", *UNCOUNTED)


def write_json(path: Path, value: Any) -> None:
    """Write JSON, gzipped with a fixed header time when the name ends in .gz."""
    text = json.dumps(value, indent=1, sort_keys=True) + "\n"
    if path.suffix == ".gz":
        with gzip.GzipFile(path, "wb", mtime=0) as stream:
            stream.write(text.encode("utf-8"))
    else:
        path.write_text(text, encoding="utf-8")


def read_json(path: Path) -> Any:
    """Read JSON, gzipped when the name ends in .gz."""
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return json.load(stream)
    return json.loads(path.read_text(encoding="utf-8"))


def families_of(cell: dict) -> list[str]:
    """Return the family labels a cell definition carries, as field:value."""
    labels = []
    for field in FAMILY_FIELDS:
        value = cell.get(field)
        values = value if isinstance(value, list) else [value] if value else []
        labels.extend(f"{field}:{item}" for item in values)
    return labels


def _ordered_counts(counter: Counter) -> dict[str, int]:
    order = [*CLASSES, *UNCOUNTED]
    return {name: counter[name] for name in sorted(counter, key=order.index)}


def _native_sha(folder: Path) -> str | None:
    answers = folder / "repark.jsonl"
    if not answers.exists():
        return None
    for answer in read_records(answers).values():
        binary = answer.get("binary") or {}
        return binary.get("sha256")
    return None


def cell_classes(
    replay_dir: Path, exclusions: dict[str, str], flaky: set[str]
) -> dict[str, dict[str, Any]]:
    """Return every replayed cell with its lane, class, ANSI mode and families."""
    cells: dict[str, dict[str, Any]] = {}
    for folder in sorted(path.parent for path in replay_dir.glob("*/diff.jsonl")):
        cases = read_records(folder / "cases.jsonl")
        for key, verdict in read_records(folder / "diff.jsonl").items():
            label = verdict["class"]
            if key in exclusions:
                label = "RECORDED-EXCLUSION"
            elif key in flaky:
                label = "FLAKY"
            cells[key] = {
                "lane": folder.name,
                "class": label,
                "ansi": cases[key]["ansi"],
                "families": families_of(cases[key]),
            }
    return cells


def tally(cells: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Count classes overall, by lane, by ANSI mode and by family."""
    totals: Counter = Counter()
    lanes: dict[str, Counter] = {}
    ansi = {"on": Counter(), "off": Counter()}
    families: dict[str, Counter] = {}
    for cell in cells.values():
        label = cell["class"]
        totals[label] += 1
        lanes.setdefault(cell["lane"], Counter())[label] += 1
        if label in NOT_COMPARABLE:
            continue
        ansi["on" if cell["ansi"] else "off"][label] += 1
        for family in cell["families"]:
            families.setdefault(family, Counter())[label] += 1
    comparable = sum(count for label, count in totals.items() if label not in NOT_COMPARABLE)
    lane_rows = {}
    for lane, counter in sorted(lanes.items()):
        lane_comparable = sum(n for label, n in counter.items() if label not in NOT_COMPARABLE)
        lane_rows[lane] = {
            "comparable": lane_comparable,
            "equal": counter["EQUAL"],
            "classes": _ordered_counts(counter),
        }
    return {
        "totals": {
            "definitions": len(cells),
            "comparable": comparable,
            "equal": totals["EQUAL"],
            "equal_share": round(totals["EQUAL"] / comparable, 4) if comparable else None,
            "recorded_exclusions": totals["RECORDED-EXCLUSION"],
            "excluded_at_run": totals["EXCLUDED"],
            "flaky": totals["FLAKY"],
            "classes": _ordered_counts(
                Counter({k: v for k, v in totals.items() if k not in NOT_COMPARABLE})
            ),
        },
        "ansi": {mode: _ordered_counts(counter) for mode, counter in ansi.items()},
        "lanes": lane_rows,
        "families": {name: _ordered_counts(counter) for name, counter in sorted(families.items())},
    }


def build_scoreboard(
    replay_dir: Path,
    exclusions: dict[str, str],
    flaky: set[str],
    meta: dict[str, Any],
) -> dict[str, Any]:
    """Build one scoreboard from a replay directory of per-lane diff files."""
    cells = cell_classes(replay_dir, exclusions, flaky)
    shas = sorted({sha for folder in replay_dir.glob("*/") if (sha := _native_sha(folder))})
    compact = {
        key: [cell["lane"], cell["class"], cell["ansi"]] for key, cell in sorted(cells.items())
    }
    return {
        "format": FORMAT,
        "meta": {**meta, "native_sha256": shas},
        **tally(cells),
        "cells": compact,
    }


def _counted(label: str) -> bool:
    return label not in UNCOUNTED


def compare_scoreboards(base: dict[str, Any], head: dict[str, Any]) -> dict[str, Any]:
    """List cells moved onto Spark, moved off it, and changed but still unlike, by lane."""
    lanes: dict[str, dict[str, list]] = {}
    counts: Counter = Counter()
    shared = sorted(set(base["cells"]) & set(head["cells"]))
    for key in shared:
        lane, before, _ = base["cells"][key]
        after = head["cells"][key][1]
        if before == after:
            continue
        if not (_counted(before) and _counted(after)):
            bucket, entry = "uncounted", {"id": key, "from": before, "to": after}
        elif before == "EQUAL" and after != "EQUAL":
            bucket, entry = "moved_off", {"id": key, "to": after}
        elif before != "EQUAL" and after == "EQUAL":
            bucket, entry = "moved_on", {"id": key, "from": before}
        else:
            bucket, entry = "changed_unlike", {"id": key, "from": before, "to": after}
        counts[bucket] += 1
        lanes.setdefault(lane, {}).setdefault(bucket, []).append(entry)
    return {
        "format": REPORT_FORMAT,
        "base": base["meta"],
        "head": head["meta"],
        "totals": {
            "base": base["totals"],
            "head": head["totals"],
            "shared_cells": len(shared),
            "only_base": len(set(base["cells"]) - set(head["cells"])),
            "only_head": len(set(head["cells"]) - set(base["cells"])),
            "moved_on": counts["moved_on"],
            "moved_off": counts["moved_off"],
            "changed_unlike": counts["changed_unlike"],
            "uncounted": counts["uncounted"],
        },
        "lanes": {lane: lanes[lane] for lane in sorted(lanes)},
    }


def answer_fields(answer: dict) -> dict:
    """Return the answer fields a repeat run must reproduce."""
    return {field: answer.get(field) for field in ANSWER_FIELDS}


def flaky_cells(run_dirs: list[Path]) -> dict[str, list[str]]:
    """Return each cell whose RePark answer or class differs between runs on one package."""
    seen: dict[str, list[tuple[dict, str]]] = {}
    for run in run_dirs:
        for folder in sorted(path.parent for path in run.glob("*/diff.jsonl")):
            answers = read_records(folder / "repark.jsonl")
            for key, verdict in read_records(folder / "diff.jsonl").items():
                seen.setdefault(key, []).append((answer_fields(answers[key]), verdict["class"]))
    flaky = {}
    for key, observed in sorted(seen.items()):
        if len(observed) < 2:
            continue
        first_fields, first_class = observed[0]
        changed = sorted(
            {
                field
                for fields, _ in observed[1:]
                for field in ANSWER_FIELDS
                if fields[field] != first_fields[field]
            }
        )
        if changed or any(label != first_class for _, label in observed[1:]):
            flaky[key] = changed or ["class"]
    return flaky


def _share(totals: dict[str, Any]) -> str:
    share = totals["equal_share"]
    return "n/a" if share is None else f"{share:.2%}"


def render_scoreboard(board: dict[str, Any]) -> str:
    """Render the short Markdown table of one scoreboard."""
    totals = board["totals"]
    meta = board["meta"]
    lines = [
        f"# Spark-door scoreboard: {meta.get('label')}",
        "",
        f"Revision `{meta.get('revision')}`, Spark {meta.get('spark')}, "
        f"measured {meta.get('measured')}.",
        f"Comparable {totals['comparable']:,}; EQUAL {totals['equal']:,} ({_share(totals)}); "
        f"recorded exclusions {totals['recorded_exclusions']:,}; excluded at run "
        f"{totals['excluded_at_run']:,}; flaky {totals['flaky']:,}.",
        "",
        "| Class | All | ANSI on | ANSI off |",
        "| --- | ---: | ---: | ---: |",
    ]
    for label, count in totals["classes"].items():
        on = board["ansi"]["on"].get(label, 0)
        off = board["ansi"]["off"].get(label, 0)
        lines.append(f"| {label} | {count:,} | {on:,} | {off:,} |")
    lines += ["", "| Lane | Comparable | EQUAL | Share |", "| --- | ---: | ---: | ---: |"]
    for lane, row in board["lanes"].items():
        share = f"{row['equal'] / row['comparable']:.1%}" if row["comparable"] else "n/a"
        lines.append(f"| {lane} | {row['comparable']:,} | {row['equal']:,} | {share} |")
    return "\n".join(lines) + "\n"


def render_comparison(report: dict[str, Any]) -> str:
    """Render the short Markdown table of one comparison."""
    totals = report["totals"]
    base, head = report["base"], report["head"]
    lines = [
        f"# Scoreboard comparison: {base.get('label')} to {head.get('label')}",
        "",
        f"Base `{base.get('revision')}`: EQUAL {totals['base']['equal']:,} of "
        f"{totals['base']['comparable']:,} ({_share(totals['base'])}). "
        f"Head `{head.get('revision')}`: EQUAL {totals['head']['equal']:,} of "
        f"{totals['head']['comparable']:,} ({_share(totals['head'])}).",
        f"Moved onto Spark {totals['moved_on']:,}; moved off Spark {totals['moved_off']:,}; "
        f"changed but still unlike {totals['changed_unlike']:,}; uncounted on one side "
        f"{totals['uncounted']:,}; shared cells {totals['shared_cells']:,}.",
        "",
        "| Lane | Moved on | Moved off | Changed, still unlike |",
        "| --- | ---: | ---: | ---: |",
    ]
    for lane, buckets in report["lanes"].items():
        row = [len(buckets.get(name, [])) for name in ("moved_on", "moved_off", "changed_unlike")]
        if any(row):
            lines.append(f"| {lane} | {row[0]:,} | {row[1]:,} | {row[2]:,} |")
    return "\n".join(lines) + "\n"
