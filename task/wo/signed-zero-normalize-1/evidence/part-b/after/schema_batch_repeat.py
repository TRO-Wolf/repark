from __future__ import annotations

import argparse
import collections
import json
import tempfile
from pathlib import Path
from typing import Any

COLUMNS = "id BIGINT, i INT, d DOUBLE, tf FLOAT, e DOUBLE, s STRING, flag STRING"
ROW_4 = "(4, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, 'a ', 'f4')"
ROW_6 = "(6, -1, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, 'A', 'f6')"
KINDS = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
STATEMENTS = {
    "literal": "UPDATE {table} SET flag = 'upd' WHERE d <=> 0.0",
    "key": "UPDATE {table} SET flag = 'upd' WHERE d IN (SELECT d FROM ice.ns.src)",
}
TRUTH = [(4, "upd"), (6, "upd")]


def props(version: str, mode: str) -> str:
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def attempt(spark: Any, statement: str, table: str) -> tuple[str, str | None]:
    try:
        spark.sql(statement.format(table=table)).collect()
        state = sorted(
            (row[0], row[1]) for row in spark.sql(f"SELECT id, flag FROM {table}").collect()
        )
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            raise
        text = str(error)
        if "Mismatch between schema and batches" in text:
            return "refused-mismatch", None
        return "refused-other", text[:200]
    return ("answered" if state == TRUTH else f"wrong:{state}"), None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=50)
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    from repark import ReparkSession

    root = Path(tempfile.mkdtemp(prefix="szb-repeat-"))
    spark = (
        ReparkSession.builder.appName("szb-schema-batch-repeat")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(root))
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    spark.sql("CREATE TABLE ice.ns.src (d DOUBLE) USING iceberg").collect()
    spark.sql("INSERT INTO ice.ns.src VALUES (CAST('0.0' AS DOUBLE))").collect()
    counts: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    other: list[dict[str, Any]] = []
    for repeat in range(arguments.repeats):
        for name, statement in STATEMENTS.items():
            for kind, (version, mode) in KINDS.items():
                table = f"ice.ns.r_{name}_{kind.replace('-', '_')}_{repeat}"
                spark.sql(
                    f"CREATE TABLE {table} ({COLUMNS}) USING iceberg "
                    f"TBLPROPERTIES ({props(version, mode)})"
                ).collect()
                spark.sql(f"INSERT INTO {table} VALUES {ROW_4}").collect()
                spark.sql(f"INSERT INTO {table} VALUES {ROW_6}").collect()
                outcome, error = attempt(spark, statement, table)
                counts[f"{name}:{kind}"][outcome] += 1
                if error is not None:
                    other.append({"cell": f"{name}:{kind}", "repeat": repeat, "error": error})
    summary = {
        "label": arguments.label,
        "repeats": arguments.repeats,
        "counts": {key: dict(value) for key, value in sorted(counts.items())},
        "other": other[:5],
    }
    Path(arguments.out).write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n")
    print(json.dumps(summary["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
