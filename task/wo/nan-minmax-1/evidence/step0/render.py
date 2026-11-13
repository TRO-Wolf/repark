from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

Key = tuple[str, str, str, str]


def load(paths: list[Path]) -> dict[Key, dict[str, Any]]:
    cells: dict[Key, dict[str, Any]] = {}
    for path in paths:
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if record["record"] == "cell":
                cells[(record["group"], record["source"], record["door"], record["cell"])] = record
    return cells


def answer(record: dict[str, Any] | None) -> Any:
    if record is None:
        return None
    if not record["ok"]:
        return "ERR " + record["error"].split(":")[0]
    return record.get("rows")


def show(value: Any) -> str:
    if value is None:
        return "."
    if isinstance(value, str):
        return value
    if value and all(isinstance(r, list) and len(r) == 3 for r in value) and len(value) > 1:
        d_nan = sum(1 for r in value if r[1] == "NaN")
        f_nan = sum(1 for r in value if r[2] == "NaN")
        return f"{len(value)} rows, NaN d {d_nan} f {f_nan}"
    return json.dumps(value)


def spark_key(key: Key, spark: dict[Key, dict[str, Any]]) -> Key:
    group, source, door, cell = key
    if group == "path":
        own = ("path", source, "sql", cell)
        return own if own in spark else ("path", source, "sql", "read_parquet")
    if group == "read":
        return ("read", source, "sql", cell)
    if group in ("rt_by_spark", "rt_by_repark"):
        return ("roundtrip", source.split(":", 1)[1], "sql", "read_back")
    if group == "ingest_by_spark":
        return ("ingest", source.split(":", 1)[1], "sql", "read_back")
    return (group, source, door, cell)


def main() -> int:
    spark = load([Path(p) for p in sys.argv[1].split(",")])
    builds = []
    for spec in sys.argv[2:]:
        tag, files = spec.split("=", 1)
        builds.append((tag, load([Path(p) for p in files.split(",")])))
    keys: list[Key] = []
    for _, cells in builds:
        for key in cells:
            if key not in keys and not (key[3] == "write" and cells[key].get("ok")):
                keys.append(key)
    for key in spark:
        if key[0] in ("rt_by_spark", "ingest_by_spark") and key not in keys:
            keys.append(key)
    tags = [tag for tag, _ in builds]
    print("| group | source | door | cell | Spark | " + " | ".join(tags) + " |")
    print("|---|---|---|---|---|" + "---|" * len(tags))
    wrong = dict.fromkeys(tags, 0)
    for key in keys:
        expected = answer(spark.get(spark_key(key, spark)))
        row = [*key, show(expected)]
        for tag, cells in builds:
            source = key[1]
            if key[0] == "rt_by_spark" or key[0] == "ingest_by_spark":
                got = answer(spark.get(key)) if source.startswith(f"{tag}:") else None
            else:
                got = answer(cells.get(key))
            if got is None:
                row.append(".")
            elif got == expected:
                row.append("=")
            else:
                wrong[tag] += 1
                row.append(show(got))
        print("| " + " | ".join(row) + " |")
    print()
    print("Unequal cells: " + ", ".join(f"{tag} {count}" for tag, count in wrong.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
