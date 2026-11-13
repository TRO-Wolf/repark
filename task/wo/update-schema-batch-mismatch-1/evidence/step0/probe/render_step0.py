from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path
from typing import Any

HEADER = (
    "| Door | Statement | Layout | Kind | Attempts | Outcomes | Path | Batches per attempt "
    "| Order (count, outcome) | Predicted / refused |"
)


def cell_text(text: str) -> str:
    return text.replace("|", "/").replace(chr(10), " ")


def nullable_set(batch: dict[str, Any]) -> list[str]:
    return [field["name"] for field in batch["fields"] if field["nullable"]]


def signature(batches: list[dict[str, Any]]) -> str:
    if not batches:
        return "none"
    return " then ".join("{" + ",".join(nullable_set(b)) + "}" for b in batches)


def predicts_refusal(batches: list[dict[str, Any]]) -> bool:
    if len(batches) < 2:
        return False
    first = {f["name"]: f["nullable"] for f in batches[0]["fields"]}
    return any(
        f["nullable"] and not first.get(f["name"], False) for b in batches[1:] for f in b["fields"]
    )


def attempts(path: Path) -> tuple[str, list[dict[str, Any]], dict[str, str], collections.Counter]:
    module = ""
    setup: collections.Counter = collections.Counter()
    current: dict[str, Any] | None = None
    done: list[dict[str, Any]] = []
    plans: dict[str, str] = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        event = record["event"]
        if event == "module":
            module = record["module"]
        elif event == "setup-failed":
            setup[(record["cell"], record["error"][:200])] += 1
        elif event == "attempt":
            current = {
                "cell": record["cell"],
                "batches": [],
                "register": None,
                "plan": None,
                "identity": False,
            }
        elif current is None:
            continue
        elif event == "plan":
            current["plan"] = record["physical"]
            current["identity"] = True
        elif event == "batch":
            current["batches"].append(record)
            current["identity"] = True
        elif event == "done":
            current["identity"] = True
        elif event == "register":
            current["register"] = record
        elif event == "outcome":
            current["outcome"] = record["outcome"]
            current["error"] = record["error"]
            done.append(current)
            stem = "|".join(current["cell"].split("|")[:2])
            if current["plan"] is not None and stem not in plans:
                plans[stem] = current["plan"]
            current = None
    return module, done, plans, setup


def render(path: Path) -> str:
    module, done, plans, setup = attempts(path)
    cells: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for item in done:
        cells[item["cell"]].append(item)
    lines = [
        f"Rendered by `probe/render_step0.py` from `raw/{path.name}`; module `{module}`.",
        "",
        "Path: `identity` = the batches reached `collect_identity_update_rows`; "
        "`fork` = no identity event (the statement went to the fork's `TableProvider::update`). "
        "Batches: new-value batches in arrival order, each shown as its set of nullable fields. "
        "Predicted: refusals that `MemTable::try_new`'s `contains` check implies from the "
        "printed nullability (copy-on-write only).",
        "",
        HEADER,
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    totals = collections.Counter()
    for cell in sorted(cells):
        items = cells[cell]
        door, sname, lname, kind = cell.split("|")
        outcomes = collections.Counter(i["outcome"] for i in items)
        paths = collections.Counter("identity" if i["identity"] else "fork" for i in items)
        counts = collections.Counter(len(i["batches"]) for i in items)
        orders = collections.Counter((signature(i["batches"]), i["outcome"]) for i in items)
        cow = kind != "v2-mor"
        predicted = sum(
            1 for i in items if cow and i["identity"] and predicts_refusal(i["batches"])
        )
        refused = outcomes.get("refused-mismatch", 0)
        mismatch = sum(
            1
            for i in items
            if cow
            and i["identity"]
            and predicts_refusal(i["batches"]) != (i["outcome"] == "refused-mismatch")
        )
        totals["attempts"] += len(items)
        totals["disagree"] += mismatch
        totals["refused"] += refused
        totals["predicted"] += predicted
        lines.append(
            f"| {door} | {sname} | {lname} | {kind} | {len(items)} | "
            + ", ".join(f"{k} {v}" for k, v in sorted(outcomes.items()))
            + " | "
            + ", ".join(f"{k} {v}" for k, v in sorted(paths.items()))
            + " | "
            + ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
            + " | "
            + "; ".join(f"`{s}` ({v}, {o})" for (s, o), v in sorted(orders.items()))
            + f" | {predicted} / {refused} |"
        )
    lines += [
        "",
        f"Totals: {totals['attempts']} attempts, "
        f"{totals['refused']} refused with the mismatch text, "
        f"{totals['predicted']} predicted from the printed batches, "
        f"{totals['disagree']} attempts where the prediction and the outcome disagree.",
        "",
    ]
    errors = collections.Counter(
        (i["cell"].split("|")[1], i["outcome"], (i.get("error") or "")[:200])
        for i in done
        if i["outcome"] != "answered"
    )
    lines += [
        "## Refusal texts",
        "",
        "| Statement | Outcome | Text (first 200 characters) | Count |",
        "|---|---|---|---|",
    ]
    for (sname, outcome, text), count in sorted(errors.items()):
        lines.append(f"| {sname} | {outcome} | `{cell_text(text)}` | {count} |")
    lines += [
        "",
        "## Tables the door could not create",
        "",
        "| Cell | Text (first 200 characters) | Count |",
        "|---|---|---|",
    ]
    for (cell, text), count in sorted(setup.items()):
        lines.append(f"| {cell} | `{cell_text(text)}` | {count} |")
    lines += ["", "## One physical plan per door and statement (identity path only)", ""]
    for stem in sorted(plans):
        lines += [f"### {stem}", "", "```text", plans[stem].rstrip(), "```", ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw")
    parser.add_argument("--out", required=True)
    a = parser.parse_args()
    Path(a.out).write_text(render(Path(a.raw)))


if __name__ == "__main__":
    main()
