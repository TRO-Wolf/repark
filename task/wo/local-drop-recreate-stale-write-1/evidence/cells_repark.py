"""LOCAL-DROP-RECREATE-STALE-WRITE-1 matrix probe: one RePark session per cell.

Runs every row of the unit's matrix on a fresh Hadoop warehouse under ``--root``, cache on
unless ``--cache off``, and writes one JSON line per cell to ``--out``: the session rows, the
path-read rows, the truth, the current metadata file and whether its uuid is the recreated
table's. Takes every location as an argument so no machine path is committed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from repark import ReparkSession

TABLE = "hd.ns.t"
STEPS = {
    "C": "CREATE TABLE {t} (id BIGINT, n INT) USING iceberg",
    "CT": "CREATE TABLE {t} AS SELECT CAST(9 AS BIGINT) AS id, 1 AS n",
    "I7": "INSERT INTO {t} VALUES (7, 1)",
    "I8": "INSERT INTO {t} VALUES (8, 1)",
    "I8Z": "INSERT INTO {t} VALUES (8, 1, 0)",
    "R": "CREATE OR REPLACE TABLE {t} AS SELECT CAST(id AS BIGINT) AS id, 1 AS n FROM range(2, 5)",
    "RC": "CREATE OR REPLACE TABLE {t} (id BIGINT, n INT) USING iceberg",
    "S": "SELECT * FROM {t}",
    "M": "MERGE INTO {t} USING (SELECT CAST(9 AS BIGINT) AS id, 1 AS n) s ON {t}.id = s.id "
    "WHEN NOT MATCHED THEN INSERT *",
    "M3": "MERGE INTO {t} USING (SELECT CAST(3 AS BIGINT) AS id, 5 AS n) s ON {t}.id = s.id "
    "WHEN MATCHED THEN UPDATE SET n = s.n WHEN NOT MATCHED THEN INSERT *",
    "U": "UPDATE {t} SET n = 5 WHERE id >= 0",
    "DEL": "DELETE FROM {t} WHERE id = 7",
    "A": "ALTER TABLE {t} ADD COLUMN z INT",
    "D": "DROP TABLE {t}",
    "DP": "DROP TABLE {t} PURGE",
    **{
        f"R{k}": f"CREATE OR REPLACE TABLE {{t}} AS SELECT CAST({k} AS BIGINT) AS id, 1 AS n"
        for k in range(1, 6)
    },
}
STALE = "C,I7,R,D,C,R"
CELLS = [
    ("wrong-read", STALE),
    ("wrong-read", f"{STALE},S"),
    ("wrong-read", "C,I7,R,D,C,RC"),
    ("wrong-read", "C,I7,R,D,CT,R"),
    *[("wrong-write", f"{STALE},{tail}") for tail in ("I8", "M", "M3", "U", "DEL", "A,I8Z")],
    ("replace-only", "R1,R2,R3,D,R4,R5"),
    ("purge", "C,I7,R,DP,C,R"),
    ("control", "C,I7,D,C,R"),
    ("control", "C,I7,I7,D,C,R"),
    ("control", "C,I7,S,D,C,R"),
    ("control", "C,R,D,C,R"),
    ("control", "C,I7,R,D,C,I8"),
    ("control", "C,I7,R,D,C,M"),
    ("control", "R1,R2,D,R3,R4"),
]


def truth(sequence: str) -> list[list[int]]:
    """Fold the step model over the sequence and return the sorted rows."""
    rows: list[tuple[int, int]] = []
    for step in sequence.split(","):
        if step in ("C", "RC"):
            rows = []
        elif step == "CT":
            rows = [(9, 1)]
        elif step in ("I7", "I8", "I8Z"):
            rows = [*rows, (int(step[1]), 1)]
        elif step == "R":
            rows = [(2, 1), (3, 1), (4, 1)]
        elif step.startswith("R") and step[1:].isdigit():
            rows = [(int(step[1:]), 1)]
        elif step == "M" and all(r[0] != 9 for r in rows):
            rows = [*rows, (9, 1)]
        elif step == "M3":
            matched = any(r[0] == 3 for r in rows)
            rows = [(i, 5 if i == 3 else n) for i, n in rows] if matched else [*rows, (3, 5)]
        elif step == "U":
            rows = [(i, 5) for i, _ in rows]
        elif step == "DEL":
            rows = [r for r in rows if r[0] != 7]
    return [list(r) for r in sorted(rows)]


def read_rows(frame: Any) -> list[list[int]] | str:
    """Sorted (id, n) rows of a frame, or the error class and head of its message."""
    try:
        rows = frame().select("id", "n").to_arrow().to_pylist()
    except Exception as exc:
        return f"ERR {type(exc).__name__}: {str(exc)[:120]}"
    return sorted([int(r["id"]), int(r["n"])] for r in rows)


def run_cell(root: Path, index: int, sequence: str, cache: bool) -> dict[str, Any]:
    """Run one cell in its own session and warehouse and return its record."""
    warehouse = root / f"cell-{index:02d}"
    builder = (
        ReparkSession.builder.appName("local-drop-recreate-probe")
        .config("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(warehouse))
    )
    if not cache:
        builder = builder.config("repark.iceberg.metadataCache", "false")
    session = builder.getOrCreate()
    meta = warehouse / "ns" / "t" / "metadata"
    errors: list[str] = []
    incarnations: list[str] = []
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS hd.ns").collect()
        present = False
        for step in sequence.split(","):
            try:
                session.sql(STEPS[step].format(t=TABLE)).collect()
            except Exception as exc:
                errors.append(f"{step}: {type(exc).__name__}: {str(exc)[:120]}")
            dropped = step in ("D", "DP")
            if not present and not dropped:
                doc = json.loads((meta / "v1.metadata.json").read_text(encoding="utf-8"))
                incarnations.append(doc["table-uuid"])
            present = not dropped
        session_rows = read_rows(lambda: session.sql(f"SELECT * FROM {TABLE}"))
        path_rows = read_rows(lambda: session.read.format("iceberg").load(str(meta.parent)))
        hint = (meta / "version-hint.text").read_text(encoding="utf-8").strip()
        current = f"v{hint}.metadata.json"
        current_uuid = json.loads((meta / current).read_text(encoding="utf-8"))["table-uuid"]
    finally:
        session.stop()
    return {
        "sequence": sequence,
        "cache": "on" if cache else "off",
        "truth": truth(sequence),
        "session_rows": session_rows,
        "path_rows": path_rows,
        "current": current,
        "current_is_recreated": current_uuid == incarnations[-1],
        "errors": errors,
        "table_dir": str(meta.parent),
    }


def main() -> int:
    """Run every cell and write the JSON lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cache", choices=("on", "off"), default="on")
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as out:
        for index, (kind, sequence) in enumerate(CELLS):
            record = {"kind": kind, **run_cell(args.root, index, sequence, args.cache == "on")}
            out.write(json.dumps(record) + "\n")
            sys.stdout.write(f"{kind:12s} {sequence:24s} session={record['session_rows']}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
