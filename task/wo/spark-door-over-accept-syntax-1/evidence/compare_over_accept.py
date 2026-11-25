from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def summarize(result: dict[str, Any] | None) -> str:
    if result is None:
        return "-"
    if result["kind"] == "error":
        text = f"ERR {result['class'] or result['type']}"
    else:
        columns = ",".join(
            f"{name}:{kind}{'' if nullable else '!'}" for name, kind, nullable in result["fields"]
        )
        rows = json.dumps(result["rows"], separators=(",", ":"))
        text = f"{columns}={rows}" if columns else f"ok={rows}"
    after = result.get("after")
    if after is not None:
        text += f" / after {summarize(after)}"
    return text


def error_base(result: dict[str, Any]) -> str:
    return (result["class"] or result["type"]).split(".")[0]


def verdict(spark: dict[str, Any] | None, other: dict[str, Any] | None) -> str:
    if spark is None or other is None:
        return "n/a"
    if spark["kind"] != other["kind"]:
        if spark["kind"] == "error":
            return "OVER-ACCEPT"
        return "OVER-REFUSE"
    if spark["kind"] == "error":
        base = "EQ" if error_base(spark) == error_base(other) else "REFUSE~CLASS"
    elif spark.get("after") is not None:
        base = "EQ"
    elif spark["rows"] != other["rows"]:
        base = "VALUE"
    elif [field[1] for field in spark["fields"]] != [field[1] for field in other["fields"]]:
        base = "TYPE"
    elif [field[0] for field in spark["fields"]] != [field[0] for field in other["fields"]]:
        base = "NAME"
    elif [field[2] for field in spark["fields"]] != [field[2] for field in other["fields"]]:
        base = "NULLABILITY"
    else:
        base = "EQ"
    spark_after = spark.get("after")
    other_after = other.get("after")
    if (
        spark_after is not None
        and other_after is not None
        and verdict(spark_after, other_after) != "EQ"
    ):
        return f"{base}+AFTER"
    return base


def load(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text())


def build_rows(
    spark: dict[str, Any], main: dict[str, Any], head: dict[str, Any] | None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cell_id, cell in spark["cells"].items():
        for door, spark_result in cell["doors"].items():
            main_result = main["cells"].get(cell_id, {}).get("doors", {}).get(door)
            head_result = (
                head["cells"].get(cell_id, {}).get("doors", {}).get(door) if head else None
            )
            rows.append(
                {
                    "id": cell_id,
                    "role": cell["role"],
                    "door": door,
                    "text": cell["text"]
                    if door in {"sql", "view", "dml"}
                    else (cell["pred"] if door == "filter" else cell["item"]),
                    "spark": summarize(spark_result),
                    "main": summarize(main_result),
                    "head": summarize(head_result) if head else "-",
                    "main_verdict": verdict(spark_result, main_result),
                    "head_verdict": verdict(spark_result, head_result) if head else "-",
                }
            )
    return rows


def escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_compact(rows_by_mode: dict[str, list[dict[str, Any]]]) -> str:
    lines = []
    for mode, rows in rows_by_mode.items():
        for row in rows:
            if row["main_verdict"] != "EQ" or row["head_verdict"] not in {"EQ", "-"}:
                lines.append(
                    f"{row['id']} {row['door']} {mode} {row['main_verdict']}->{row['head_verdict']}"
                    f" | S: {row['spark'][:70]} | M: {row['main'][:70]} | H: {row['head'][:70]}"
                )
    return "\n".join(sorted(lines))


def render(rows_by_mode: dict[str, list[dict[str, Any]]]) -> str:
    lines = [
        "| id | role | door | ANSI | statement | Spark 4.1.2 | main | main vs Spark | head "
        "| head vs Spark |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    keyed: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for mode, rows in rows_by_mode.items():
        for row in rows:
            keyed.setdefault((row["id"], row["door"]), {})[mode] = row
    for (cell_id, door), modes in keyed.items():
        for mode, row in modes.items():
            lines.append(
                f"| {cell_id} | {row['role']} | {door} | {mode} | `{escape(row['text'])}` "
                f"| {escape(row['spark'])} | {escape(row['main'])} | {row['main_verdict']} "
                f"| {escape(row['head'])} | {row['head_verdict']} |"
            )
    return "\n".join(lines)


def render_native(main: dict[str, Any], head: dict[str, Any] | None, mode: str) -> list[str]:
    lines = []
    for sql, result in main.get("native", {}).items():
        head_result = head.get("native", {}).get(sql) if head else None
        same = "-" if head is None else ("same" if result == head_result else "MOVED")
        lines.append(
            f"| native | {mode} | `{escape(sql)}` | {escape(summarize(result))} "
            f"| {escape(summarize(head_result)) if head else '-'} | {same} |"
        )
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--head", default="")
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    rows_by_mode: dict[str, list[dict[str, Any]]] = {}
    native_lines = [
        "| door | ANSI | statement | main | head | head vs main |",
        "|---|---|---|---|---|---|",
    ]
    tally: dict[str, int] = {}
    for mode in ("true", "false"):
        spark = load(arguments.dir / f"spark-{mode}.json")
        main_report = load(arguments.dir / f"main-{mode}.json")
        head = load(arguments.dir / f"{arguments.head}-{mode}.json") if arguments.head else None
        if spark is None or main_report is None:
            continue
        rows_by_mode[mode] = build_rows(spark, main_report, head)
        native_lines.extend(render_native(main_report, head, mode))
        for row in rows_by_mode[mode]:
            key = f"{row['role']}:{row['main_verdict']}->{row['head_verdict']}"
            tally[key] = tally.get(key, 0) + 1
    tally_lines = ["| role: main -> head | cells |", "|---|---|"]
    tally_lines.extend(f"| {key} | {count} |" for key, count in sorted(tally.items()))
    arguments.out.with_suffix(".compact.txt").write_text(render_compact(rows_by_mode) + "\n")
    arguments.out.write_text(
        render(rows_by_mode)
        + "\n\n"
        + "\n".join(native_lines)
        + "\n\n"
        + "\n".join(tally_lines)
        + "\n"
    )


if __name__ == "__main__":
    main()
