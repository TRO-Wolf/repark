from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

COLUMNS = "id BIGINT, i INT, d DOUBLE, tf FLOAT, e DOUBLE, s STRING, flag STRING"
LAYOUTS = {
    "mixed-nulls": (
        "(4, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, 'a ', 'f4')",
        "(6, -1, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, 'A', 'f6')",
    ),
    "no-nulls": (
        "(4, 7, CAST('2.5' AS DOUBLE), CAST('2.5' AS FLOAT), CAST('1.0' AS DOUBLE), 'a ', 'f4')",
        "(6, -1, CAST('3.5' AS DOUBLE), CAST('3.5' AS FLOAT), CAST('2.0' AS DOUBLE), 'A', 'f6')",
    ),
}
KINDS = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
STATEMENTS = {
    "float-literal": ("UPDATE {table} SET flag = 'upd' WHERE d <=> 0.0", {"mixed-nulls"}),
    "int-both": ("UPDATE {table} SET flag = 'upd' WHERE id IN (4, 6)", None),
    "int-col": ("UPDATE {table} SET flag = 'upd' WHERE i IS NULL OR i <= 7", None),
    "string": ("UPDATE {table} SET flag = 'upd' WHERE flag IN ('f4', 'f6')", None),
    "int-one": ("UPDATE {table} SET flag = 'upd' WHERE id = 4", None),
}
DOORS = ("spark", "native")
REQUIRED_COLUMNS = "id BIGINT, i INT, r INT NOT NULL, flag STRING"
REQUIRED_ROWS = ("(4, NULL, 1, 'f4')", "(6, -1, 2, 'f6')")
REQUIRED_STATEMENT = "UPDATE {table} SET r = NULL WHERE i IS NULL OR i <= 7"


def props(version: str, mode: str) -> str:
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def log(path: Path, record: dict[str, Any]) -> None:
    with path.open("a") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def run(sessions: dict[str, Any], door: str, statement: str) -> list[tuple[Any, ...]]:
    if door == "spark":
        return [tuple(row) for row in sessions["spark"].sql(statement).collect()]
    import repark

    repark._ANSI_NATIVE = sessions["native"]
    table = repark.sql(statement).to_arrow()
    return list(zip(*(table.column(i).to_pylist() for i in range(table.num_columns)), strict=True))


def attempt(
    sessions: dict[str, Any], door: str, statement: str, table: str, truth: list | None
) -> tuple[str, str | None]:
    try:
        run(sessions, door, statement.format(table=table))
        state = sorted(run(sessions, door, f"SELECT id, flag FROM {table}"))
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            raise
        text = str(error)
        if "Mismatch between schema and batches" in text:
            return "refused-mismatch", text[:400]
        return "refused-other", text[:400]
    if truth is None:
        return "answered", None
    return ("answered" if state == truth else f"wrong:{state}"), None


def make_table(
    sessions: dict[str, Any],
    door: str,
    table: str,
    columns: str,
    version: str,
    mode: str,
    rows: tuple[str, str],
) -> None:
    if door == "native" and (version, mode) == ("2", "copy-on-write"):
        run(sessions, door, f"CREATE TABLE {table} ({columns})")
    else:
        run(
            sessions,
            door,
            f"CREATE TABLE {table} ({columns}) USING iceberg "
            f"TBLPROPERTIES ({props(version, mode)})",
        )
    for row in rows:
        run(sessions, door, f"INSERT INTO {table} VALUES {row}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--required-repeats", type=int, default=3)
    parser.add_argument("--out", required=True)
    a = parser.parse_args()
    out = Path(a.out)
    out.unlink(missing_ok=True)
    os.environ["REPARK_STEP0_BATCH_LOG"] = str(out)
    import repark
    from repark import ReparkSession

    root = Path(tempfile.mkdtemp(prefix="upd-step0-"))
    spark = (
        ReparkSession.builder.appName("upd-step0-print-batches")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(root / "spark"))
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    from repark._native import PyReparkSession

    native = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    native.register_memory_catalog("ice", str(root / "native"))
    native.create_namespace("ice", "ns", str(root / "native" / "ns"))
    sessions = {"spark": spark, "native": native}
    log(out, {"event": "module", "module": Path(repark._native.__file__).name})
    n = 0
    for repeat in range(a.repeats):
        for door in DOORS:
            for sname, (statement, only) in STATEMENTS.items():
                for lname, rows in LAYOUTS.items():
                    if only is not None and lname not in only:
                        continue
                    truth = (
                        [(4, "upd"), (6, "f6")] if sname == "int-one" else [(4, "upd"), (6, "upd")]
                    )
                    for kind, (version, mode) in KINDS.items():
                        n += 1
                        table = f"ice.ns.t{n}"
                        cell = f"{door}|{sname}|{lname}|{kind}"
                        try:
                            make_table(sessions, door, table, COLUMNS, version, mode, rows)
                        except Exception as error:
                            log(
                                out,
                                {"event": "setup-failed", "cell": cell, "error": str(error)[:400]},
                            )
                            continue
                        log(out, {"event": "attempt", "cell": cell, "repeat": repeat})
                        outcome, error = attempt(sessions, door, statement, table, truth)
                        log(
                            out,
                            {"event": "outcome", "cell": cell, "outcome": outcome, "error": error},
                        )
    for repeat in range(a.required_repeats):
        for door in DOORS:
            for kind, (version, mode) in KINDS.items():
                n += 1
                table = f"ice.ns.t{n}"
                cell = f"{door}|required-set-null|required|{kind}"
                try:
                    make_table(
                        sessions, door, table, REQUIRED_COLUMNS, version, mode, REQUIRED_ROWS
                    )
                except Exception as error:
                    log(out, {"event": "setup-failed", "cell": cell, "error": str(error)[:400]})
                    continue
                log(out, {"event": "attempt", "cell": cell, "repeat": repeat})
                outcome, error = attempt(sessions, door, REQUIRED_STATEMENT, table, None)
                log(out, {"event": "outcome", "cell": cell, "outcome": outcome, "error": error})
    spark.stop()


if __name__ == "__main__":
    main()
