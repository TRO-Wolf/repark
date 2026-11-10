from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import drive_cmp as d  # noqa

TABLE_DDL = "USING iceberg TBLPROPERTIES ('format-version'='2')"
FIXTURES = {
    "W": (d.CREATE_R, d.SEED_R, d.READBACK),
    "T1": (
        "CREATE TABLE {t} (id INT, s STRUCT<a: INT, b: INT>) " + TABLE_DDL,
        "INSERT INTO {t} VALUES (1, named_struct('a', 3, 'b', 4)), (2, named_struct('a', 4, "
        "'b', 3))",
        "SELECT id, s FROM {t}",
    ),
    "L": (
        "CREATE TABLE {t} (id INT, s STRUCT<a: INT, b: INT>) " + TABLE_DDL,
        "INSERT INTO {t} VALUES (1, named_struct('a', 3, 'b', 4)), (2, named_struct('a', 4, "
        "'b', 3))",
        "SELECT id, s FROM {t}",
    ),
    "R": (
        "CREATE TABLE {t} (id INT, s STRUCT<b: INT, a: INT>) " + TABLE_DDL,
        "INSERT INTO {t} VALUES (1, named_struct('b', 4, 'a', 3)), (2, named_struct('b', 3, "
        "'a', 4))",
        "SELECT id, s FROM {t}",
    ),
    "U": (
        "CREATE TABLE {t} (k STRUCT<b: INT, a: INT>) " + TABLE_DDL,
        "INSERT INTO {t} VALUES (named_struct('b', 3, 'a', 4))",
        "SELECT k FROM {t}",
    ),
}
NEW_CELLS = (
    {
        "key": "P2-16",
        "family": "brief",
        "shape": "brief_cell16_in_subquery",
        "types": "struct_ab_vs_ba|reordered",
        "op": "in_sub",
        "door": "sql",
        "seat": "brief",
        "case": "insensitive",
        "ansi": "on",
        "control": "",
        "pass": 2,
        "stmt": "SELECT l.id FROM {L} l WHERE l.s IN (SELECT r.s FROM {R} r)",
    },
    {
        "key": "P2-22",
        "family": "brief",
        "shape": "brief_cell22_join_on",
        "types": "struct_ab_vs_ba|reordered",
        "op": "join",
        "door": "sql",
        "seat": "brief",
        "case": "insensitive",
        "ansi": "on",
        "control": "",
        "pass": 2,
        "stmt": "SELECT l.id AS lid, r.id AS rid FROM {L} l JOIN {R} r ON l.s = r.s",
    },
    {
        "key": "P2-39",
        "family": "brief",
        "shape": "brief_cell39_merge_delete",
        "types": "int_int|reordered",
        "op": "eq",
        "door": "sql",
        "seat": "merge_delete",
        "case": "insensitive",
        "ansi": "on",
        "control": "",
        "pass": 2,
        "stmt": "MERGE INTO {T1} t USING (SELECT named_struct('b', 3, 'a', 4) AS k) u ON t.s = "
        "u.k WHEN MATCHED THEN DELETE",
    },
    {
        "key": "P2-39T",
        "family": "brief",
        "shape": "brief_cell39_merge_delete_column_twin",
        "types": "int_int|reordered",
        "op": "eq",
        "door": "sql",
        "seat": "merge_delete",
        "case": "insensitive",
        "ansi": "on",
        "control": "",
        "pass": 2,
        "stmt": "MERGE INTO {T1} t USING (SELECT k FROM {U}) u ON t.s = u.k WHEN MATCHED THEN "
        "DELETE",
    },
)


class Runner(d.Runner):
    def __init__(self, spark: Any, functions: Any, namespace: str) -> None:
        self.spark = spark
        self.functions = functions
        self.namespace = namespace
        self.count = 0
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}").collect()

    def fresh(self, name: str = "W") -> str:
        self.count += 1
        table = f"{self.namespace}.p{self.count}"
        ddl, seed, _ = FIXTURES[name]
        try:
            self.spark.sql(ddl.replace("{t}", table)).collect()
            self.spark.sql(seed.replace("{t}", table)).collect()
        except Exception as error:
            raise d.SetupError(str(error)[:200]) from error
        return table

    def readback(self, table: str, name: str = "W") -> Any:
        return d.rows_of(self.spark.sql(FIXTURES[name][2].replace("{t}", table)))

    def run_cell(self, cell: dict[str, Any]) -> dict[str, Any]:
        stmt = cell["stmt"]
        record: dict[str, Any] = {"key": cell["key"], "pass": 2}
        tables = {}
        try:
            for name in FIXTURES:
                if "{" + name + "}" in stmt:
                    tables[name] = self.fresh(name)
        except d.SetupError as error:
            return {**record, "kind": "setup", "answer": {"class": "SETUP", "head": str(error)}}
        for name, table in tables.items():
            stmt = stmt.replace("{" + name + "}", table)
        target = tables.get("T1") if stmt.startswith(d.DML) else None
        if target is not None:
            record["pre_rows"] = self.readback(target, "T1")
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
            record["post_rows"] = self.readback(target, "T1")
            record["post_snap"] = self.snapshots(target)
        return record

    def derive(self, cell: dict[str, Any]) -> dict[str, Any]:
        record: dict[str, Any] = {"key": cell["key"], "pass": 2}
        table = self.fresh("W")
        pred = cell["stmt"].split(" WHERE ", 1)[1].replace("{T}", table)
        pre = self.readback(table, "W")
        twin = f"SELECT id, v, p, q, n, nq, ap, aq, mp FROM {table} WHERE {pred}"
        if cell["case"] == "sensitive":
            self.spark.conf.set("spark.sql.caseSensitive", "true")
        try:
            selected = d.rows_of(self.spark.sql(twin))
        except Exception as error:
            return {**record, "derived": {"twin": "error", "error": d.error_record(error)}}
        finally:
            self.spark.conf.set("spark.sql.caseSensitive", "false")
        remaining = collections.Counter(json.dumps(row, sort_keys=True) for row in pre)
        remaining.subtract(collections.Counter(json.dumps(row, sort_keys=True) for row in selected))
        expected = sorted(
            (json.loads(text) for text, count in remaining.items() for _ in range(count)),
            key=json.dumps,
        )
        return {
            **record,
            "derived": {"twin": "rows", "selected": len(selected), "expected_rows": expected},
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=["spark", "main"], required=True)
    parser.add_argument("--mode", choices=["cells", "derive"], required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--cells")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.mode == "cells":
        cells = list(NEW_CELLS)
    else:
        cells = [json.loads(line) for line in Path(args.cells).read_text().splitlines() if line]
    if args.leg == "spark":
        spark = d.spark_session(args.warehouse)
        from pyspark.sql import functions
    else:
        spark = d.repark_session(args.warehouse)
        from repark.spark import functions
    spark.conf.set("spark.sql.ansi.enabled", "true")
    runner = Runner(spark, functions, "hd.p2" if args.mode == "cells" else "hd.p2d")
    lines = []
    for cell in cells:
        record = runner.run_cell(cell) if args.mode == "cells" else runner.derive(cell)
        lines.append(json.dumps(record, sort_keys=True))
    Path(args.out).write_text("\n".join(lines) + "\n")
    print(f"{args.leg} {args.mode}: {len(lines)} records", file=sys.stderr)
    if args.leg == "spark":
        spark.stop()


if __name__ == "__main__":
    main()
