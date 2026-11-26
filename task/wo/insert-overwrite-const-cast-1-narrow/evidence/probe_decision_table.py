from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

TYPES = [
    "DOUBLE",
    "FLOAT",
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "DECIMAL(10,2)",
    "DECIMAL(38,0)",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
]

LETTERS = [
    "x",
    "X",
    "abc",
    "foo",
    "bad",
    "z",
    "Z",
    "e",
    "E",
    "d",
    "D",
    "f",
    "F",
    "l",
    "L",
    "t",
    "T",
    "y",
    "Y",
    "n",
    "N",
    "i",
    "I",
    "yes",
    "YES",
    "no",
    "NO",
    "true",
    "TRUE",
    "True",
    "false",
    "FALSE",
    "on",
    "ON",
    "off",
    "OFF",
    "ye",
    "true"[:3],
    "false"[:4],
    "yess",
    "nan",
    "NaN",
    "NAN",
    "nAn",
    "inf",
    "Inf",
    "INF",
    "infinity",
    "Infinity",
    "INFINITY",
    "iNfInItY",
    "nand",
    "NaN" + "d",
    "infd",
    "Infinityd",
    "infinityf",
    "NaNf",
    "infinite",
    "nana",
    "epoch",
    "now",
    "today",
    "yesterday",
    "tomorrow",
    "UTC",
    "GMT",
    "PST",
    "CET",
    "TZ",
    "Tz",
    "Tx",
    "TT",
    "null",
    "NULL",
    "none",
    "AM",
    "PM",
    "Jan",
    "January",
    "Mon",
    "xyz",
    "abcdefghijklmnopqrstuvwxyz",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
]

OUTSIDE = [
    "",
    " ",
    " x ",
    "x ",
    "1x",
    "x1",
    "1.5",
    "+1",
    "-1",
    "1",
    "0",
    "1e5",
    "0x1p0",
    "\u0661",
    "\uff58",
    "\u00e9",
    "x\t",
    "\tx",
    "x\x7f",
    "2024-01-01",
    "2024",
    "T00:00:00",
    "-x",
    "+x",
    "x-y",
    "x.y",
]


def build_session(engine: str) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("iocnarrow_decision")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("OFF")
        return spark
    import repark._native

    print(f"REPARK_NATIVE={repark._native.__file__}", file=sys.stderr)
    from repark.spark.sql import SparkSession

    return (
        SparkSession.builder.appName("iocnarrow_decision")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def err_class(message: str) -> str:
    found = re.search(r"\[([A-Z][A-Z0-9_.]*)\]", message)
    return found.group(1) if found else "GENERIC:" + message.splitlines()[0][:80]


def sql_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def one(spark: Any, sql: str) -> dict[str, Any]:
    try:
        rows = spark.sql(sql).collect()
        return {"ok": True, "value": repr(rows[0][0]) if rows else None}
    except Exception as exc:
        return {"ok": False, "cls": err_class(str(exc))}


def main() -> int:
    engine = sys.argv[1]
    out_path = Path(sys.argv[2])
    spark = build_session(engine)
    out: dict[str, Any] = {}
    for ansi in ("true", "false"):
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        for group, values in (("letters", LETTERS), ("outside", OUTSIDE)):
            for value in values:
                for target in TYPES:
                    key = f"{ansi}|{group}|{target}|{value!r}"
                    literal = sql_literal(value)
                    sub = f"SELECT CAST(s AS {target}) AS v FROM (SELECT {literal} AS s) src"
                    out[key] = {"subquery": one(spark, sub)}
                    if ansi == "true":
                        lit = f"SELECT CAST({sql_literal(value)} AS {target}) AS v"
                        out[key]["literal"] = one(spark, lit)
    out_path.write_text(json.dumps(out, indent=1, sort_keys=True), encoding="utf-8")
    print(f"{engine}: {len(out)} cells -> {out_path}")
    if engine == "spark":
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
