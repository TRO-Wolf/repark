from __future__ import annotations

import argparse
import collections
import datetime
import decimal
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")
DML = ("DELETE", "UPDATE", "MERGE", "INSERT")
ICEBERG_JAR = os.environ.get("STRUCT_CMP_IVY_JAR", "<ivy-jar>")
CREATE_R = (
    "CREATE TABLE {t} (id INT, v INT, p STRUCT<a: INT, b: INT>, q STRUCT<b: INT, a: INT>, "
    "n STRUCT<a: INT, b: STRUCT<c: INT, d: INT>>, nq STRUCT<b: STRUCT<d: INT, c: INT>, a: INT>, "
    "ap ARRAY<STRUCT<a: INT, b: INT>>, aq ARRAY<STRUCT<b: INT, a: INT>>, "
    "mp MAP<STRING, STRUCT<a: INT, b: INT>>) USING iceberg TBLPROPERTIES ('format-version'='2')"
)
SEED_R = (
    "INSERT INTO {t} VALUES "
    "(1, 1, named_struct('a', 3, 'b', 4), named_struct('b', 4, 'a', 3), "
    "named_struct('a', 3, 'b', named_struct('c', 1, 'd', 2)), "
    "named_struct('b', named_struct('d', 2, 'c', 1), 'a', 3), "
    "array(named_struct('a', 3, 'b', 4)), array(named_struct('b', 4, 'a', 3)), "
    "map('k', named_struct('a', 3, 'b', 4))), "
    "(2, 2, named_struct('a', 4, 'b', 3), named_struct('b', 3, 'a', 4), "
    "named_struct('a', 4, 'b', named_struct('c', 5, 'd', 6)), "
    "named_struct('b', named_struct('d', 6, 'c', 5), 'a', 4), "
    "array(named_struct('a', 4, 'b', 3)), array(named_struct('b', 3, 'a', 4)), "
    "map('k', named_struct('a', 4, 'b', 3)))"
)
READBACK = "SELECT id, v, p, q, n, nq, ap, aq, mp FROM {t}"
SNAPSHOTS = "SELECT count(*) AS n FROM {t}.snapshots"


class SetupError(Exception):
    pass


def value_of(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: value_of(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [value_of(item) for item in value]
    if isinstance(value, dict):
        return {str(key): value_of(item) for key, item in sorted(value.items())}
    if isinstance(value, (datetime.date, datetime.datetime, decimal.Decimal)):
        return str(value)
    return value


def rows_of(frame: Any) -> list[list[Any]]:
    rows = [[value_of(item) for item in row] for row in frame.collect()]
    return sorted(rows, key=json.dumps)


def error_record(error: BaseException) -> dict[str, str]:
    text = str(error)
    if type(error).__name__ == "PanicException":
        return {"class": "PANIC", "head": text[:200]}
    found = CLASS.search(text)
    error_class = found.group(1) if found else type(error).__name__
    return {"class": error_class, "head": text[:200]}


class Runner:
    def __init__(self, spark: Any, functions: Any, namespace: str) -> None:
        self.spark = spark
        self.functions = functions
        self.namespace = namespace
        self.count = 0
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}").collect()
        self.wide = self.fresh()

    def fresh(self) -> str:
        self.count += 1
        name = f"{self.namespace}.t{self.count}"
        try:
            self.spark.sql(CREATE_R.replace("{t}", name)).collect()
            self.spark.sql(SEED_R.replace("{t}", name)).collect()
        except Exception as error:
            raise SetupError(str(error)[:200]) from error
        return name

    def snapshots(self, table: str) -> Any:
        try:
            return rows_of(self.spark.sql(SNAPSHOTS.replace("{t}", table)))[0][0]
        except Exception as error:
            return "ERR " + error_record(error)["class"]

    def readback(self, table: str) -> Any:
        return rows_of(self.spark.sql(READBACK.replace("{t}", table)))

    def execute(self, cell: dict[str, Any], stmt: str) -> dict[str, Any]:
        seat, door = cell["seat"], cell["door"]
        try:
            if door == "df_typed" or door == "df_string":
                scope = {
                    "F": self.functions,
                    "w": self.spark.table(self.wide),
                    "spark": self.spark,
                }
                frame = eval(stmt, scope)
                return self.answer_frame(frame)
            if seat == "df_temp_view":
                self.spark.sql(stmt).createOrReplaceTempView("dv")
                return self.answer_frame(self.spark.sql("SELECT * FROM dv"))
            if seat == "view":
                self.spark.sql(stmt).collect()
                return self.answer_frame(self.spark.sql("SELECT * FROM sdv"))
            if stmt.startswith(DML):
                self.spark.sql(stmt).collect()
                return {"kind": "ok", "answer": "ok"}
            if stmt.startswith("PREPARE"):
                self.spark.sql(stmt).collect()
                return {"kind": "ok", "answer": "ok"}
            return self.answer_frame(self.spark.sql(stmt))
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            return {"kind": "error", "answer": error_record(error)}

    def answer_frame(self, frame: Any) -> dict[str, Any]:
        return {
            "kind": "rows",
            "answer": {"schema": frame.schema.simpleString(), "rows": rows_of(frame)},
        }

    def run(self, cell: dict[str, Any]) -> dict[str, Any]:
        stmt = cell["stmt"].replace("{W}", self.wide)
        record: dict[str, Any] = {"key": cell["key"]}
        target = None
        if "{T}" in stmt:
            try:
                target = self.fresh()
            except SetupError as error:
                return {**record, "kind": "setup", "answer": {"class": "SETUP", "head": str(error)}}
            stmt = stmt.replace("{T}", target)
            record["pre_rows"] = self.readback(target)
            record["pre_snap"] = self.snapshots(target)
        if cell["ansi"] == "off":
            self.spark.conf.set("spark.sql.ansi.enabled", "false")
        if cell["case"] == "sensitive":
            self.spark.conf.set("spark.sql.caseSensitive", "true")
        try:
            result = self.execute(cell, stmt)
        finally:
            self.spark.conf.set("spark.sql.ansi.enabled", "true")
            self.spark.conf.set("spark.sql.caseSensitive", "false")
        record.update({"kind": result["kind"], "answer": result["answer"], "stmt_run": stmt})
        if target is not None:
            record["post_rows"] = self.readback(target)
            record["post_snap"] = self.snapshots(target)
        return record


def spark_session(warehouse: str) -> Any:
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("struct-cmp-step0-spark")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", ICEBERG_JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session.conf.set("spark.sql.ansi.enabled", "true")
    return session


def repark_session(warehouse: str) -> Any:
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("struct-cmp-step0-main")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_leg(leg: str, warehouse: str, cells_path: Path, out: Path, limit: int | None) -> None:
    cells = [json.loads(line) for line in cells_path.read_text().splitlines() if line]
    if limit is not None:
        cells = cells[:limit]
    if leg == "spark":
        spark = spark_session(warehouse)
        from pyspark.sql import functions
    else:
        spark = repark_session(warehouse)
        from repark.spark import functions
    spark.conf.set("spark.sql.ansi.enabled", "true")
    runner = Runner(spark, functions, "hd.sd")
    lines = []
    for index, cell in enumerate(cells, start=1):
        lines.append(json.dumps(runner.run(cell), sort_keys=True))
        if index % 250 == 0:
            print(f"{leg}: {index}/{len(cells)}", file=sys.stderr, flush=True)
    out.write_text("\n".join(lines) + "\n")
    print(f"{leg}: wrote {len(lines)} records to {out}", file=sys.stderr, flush=True)
    if leg == "spark":
        spark.stop()


def load_raw(path: Path) -> dict[str, dict[str, Any]]:
    return {
        record["key"]: record for record in map(json.loads, path.read_text().splitlines()) if record
    }


def label_of(cell: dict[str, Any], spark: dict[str, Any], main: dict[str, Any]) -> str:
    if spark["kind"] == "setup":
        return "ORACLE-MISSING"
    if main["kind"] == "setup":
        return "MAIN-SETUP-ERROR"
    if main["kind"] == "error" and main["answer"]["class"] == "PANIC":
        return "PANIC"
    if spark["kind"] == "error" and spark["answer"]["class"] == "IllegalArgumentException":
        return "ORACLE-MISSING"
    if spark["kind"] == "error" and main["kind"] == "error":
        if CLASS.search(main["answer"]["head"]) is None:
            return "BOTH-REFUSED"
        if spark["answer"]["class"] == main["answer"]["class"]:
            return "BOTH-REFUSED"
        return "REFUSED-DIFFERENT-CLASS"
    if spark["kind"] != "error" and main["kind"] == "error":
        return "REFUSED-WHERE-SPARK-ANSWERED"
    if spark["kind"] == "error" and main["kind"] != "error":
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if "pre_rows" in spark:
        if spark["post_rows"] != main["post_rows"]:
            return "WRONG-WRITE"
        snaps = (spark["pre_snap"], spark["post_snap"], main["pre_snap"], main["post_snap"])
        if all(isinstance(x, int) for x in snaps) and (snaps[0], snaps[1]) != (snaps[2], snaps[3]):
            return "SNAPSHOT-DIFF"
        return "EQUAL"
    if spark["answer"] != main["answer"]:
        return "WRONG-ANSWER"
    return "EQUAL"


def outcome_text(record: dict[str, Any]) -> str:
    if record["kind"] == "setup":
        return "SETUP " + record["answer"]["class"]
    if record["kind"] == "error":
        return "ERR " + record["answer"]["class"]
    if record["kind"] == "ok":
        text = "ok"
    else:
        rows = record["answer"]["rows"]
        text = f"rows={len(rows)} " + json.dumps(rows, sort_keys=True)[:120]
    if "post_rows" in record:
        changed = record["post_rows"] != record["pre_rows"]
        text += f" | readback_changed={changed} snap {record['pre_snap']}->{record['post_snap']}"
    return text


def render(scratch: Path, cells_path: Path) -> None:
    cells = [json.loads(line) for line in cells_path.read_text().splitlines() if line]
    spark = load_raw(scratch / "raw" / "spark.jsonl")
    main = load_raw(scratch / "raw" / "main.jsonl")
    counts = collections.Counter()
    by_seat_door = collections.Counter()
    by_door_label = collections.Counter()
    by_shape_label = collections.Counter()
    seat_label = collections.Counter()
    groups: dict[tuple[str, str, str, str], collections.Counter] = collections.defaultdict(
        collections.Counter
    )
    rows_md = []
    controls: dict[str, list[str]] = collections.defaultdict(list)
    for cell in cells:
        key = cell["key"]
        s, m = spark[key], main[key]
        label = label_of(cell, s, m)
        counts[label] += 1
        seat_label[(cell["seat"], label)] += 1
        by_seat_door[(cell["seat"], cell["door"], label)] += 1
        by_door_label[(cell["door"], label)] += 1
        by_shape_label[(cell["family"], label)] += 1
        if label in (
            "WRONG-ANSWER",
            "WRONG-WRITE",
            "PANIC",
            "ORACLE-MISSING",
            "MAIN-SETUP-ERROR",
            "SNAPSHOT-DIFF",
        ):
            groups[(cell["seat"], cell["door"], cell["shape"], label)][key] += 1
        control_name = cell["control"]
        if cell["seat"] == "control":
            control_name = cell["shape"]
        if control_name == "control":
            control_name = (
                "same_order_int" if cell["types"] == "int_int|same_order" else "same_order_typed"
            )
        if control_name:
            controls[control_name].append(f"{key}:{label}")
        stmt = cell["stmt"].replace("|", "\\|")
        rows_md.append(
            f"| {key} | {cell['family']} | {cell['shape']} | {cell['types']} | {cell['op']} | "
            f"{cell['seat']} | "
            f"{cell['door']} | {cell['case']} | {cell['ansi']} | `{stmt}` | "
            f"{outcome_text(s).replace('|', '/')} | "
            f"{outcome_text(m).replace('|', '/')} | {label} |"
        )
    lines = [
        "# Struct comparison step 0 table",
        "",
        f"Cells: {len(cells)}",
        "",
        "## Label counts",
        "",
    ]
    lines += [f"- {label}: {n}" for label, n in counts.most_common()]
    lines += [
        "",
        "## Labels by seat x door",
        "",
        "| seat | door | label | n |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| {seat} | {door} | {label} | {n} |"
        for (seat, door, label), n in sorted(by_seat_door.items())
    ]
    lines += ["", "## Labels by door", "", "| door | label | n |", "|---|---|---|"]
    lines += [f"| {door} | {label} | {n} |" for (door, label), n in sorted(by_door_label.items())]
    lines += ["", "## Labels by shape family", "", "| family | label | n |", "|---|---|---|"]
    lines += [
        f"| {family} | {label} | {n} |" for (family, label), n in sorted(by_shape_label.items())
    ]
    lines += ["", "## Controls", ""]
    for name in sorted(controls):
        labels = collections.Counter(item.split(":", 1)[1] for item in controls[name])
        keys = ", ".join(item.split(":", 1)[0] for item in controls[name][:40])
        more = " ..." if len(controls[name]) > 40 else ""
        lines.append(f"- {name}: {dict(labels)} (n={len(controls[name])}; keys {keys}{more})")
    lines += ["", "## Wrong and unmeasured groups (seat x door x shape)", ""]
    for (seat, door, shape, label), keys in sorted(groups.items()):
        lines.append(f"- {label} {seat} x {door} x {shape}: {sum(keys.values())} cells")
    lines += [
        "",
        "## Cells",
        "",
        "| key | family | shape | types | op | seat | door | case | ansi | statement | spark | "
        "main | label |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    lines += rows_md
    (scratch / "table.md").write_text("\n".join(lines) + "\n")
    print(json.dumps({"counts": counts, "groups": len(groups)}, sort_keys=True, default=str))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=["spark", "main"])
    parser.add_argument("--warehouse")
    parser.add_argument("--cells", default=str(HERE / "cells.jsonl"))
    parser.add_argument("--out")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--render", action="store_true")
    args = parser.parse_args()
    if args.render:
        render(HERE, Path(args.cells))
        return
    run_leg(args.leg, args.warehouse, Path(args.cells), Path(args.out), args.limit)


if __name__ == "__main__":
    main()
