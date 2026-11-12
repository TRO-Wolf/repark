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


def props(version: str, mode: str) -> str:
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=50)
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    from repark import ReparkSession

    root = Path(tempfile.mkdtemp(prefix="sz-repeat-"))
    spark = (
        ReparkSession.builder.appName("schema-batch-repeat")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(root))
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    counts: dict[str, collections.Counter[str]] = {kind: collections.Counter() for kind in KINDS}
    records: list[dict[str, Any]] = []
    for repeat in range(arguments.repeats):
        for kind, (version, mode) in KINDS.items():
            table = f"ice.ns.r_{kind.replace('-', '_')}_{repeat}"
            spark.sql(
                f"CREATE TABLE {table} ({COLUMNS}) USING iceberg "
                f"TBLPROPERTIES ({props(version, mode)})"
            ).collect()
            spark.sql(f"INSERT INTO {table} VALUES {ROW_4}").collect()
            spark.sql(f"INSERT INTO {table} VALUES {ROW_6}").collect()
            try:
                spark.sql(f"UPDATE {table} SET flag = 'upd' WHERE d <=> 0.0").collect()
                state = sorted(
                    (row[0], row[1]) for row in spark.sql(f"SELECT id, flag FROM {table}").collect()
                )
                outcome = "answered" if state == [(4, "upd"), (6, "upd")] else f"wrong:{state}"
            except BaseException as error:
                if isinstance(error, KeyboardInterrupt):
                    raise
                text = str(error)
                outcome = (
                    "refused-mismatch"
                    if "Mismatch between schema and batches" in text
                    else "refused-other"
                )
                if outcome == "refused-other":
                    records.append({"kind": kind, "repeat": repeat, "error": text[:200]})
            counts[kind][outcome] += 1
    summary = {
        "label": arguments.label,
        "repeats": arguments.repeats,
        "counts": counts,
        "other": records[:5],
    }
    Path(arguments.out).write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n")
    print(json.dumps({kind: dict(value) for kind, value in counts.items()}, sort_keys=True))


if __name__ == "__main__":
    main()
