from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

FIXTURE = Path(__file__).resolve().parent / "unix_timestamp_truncate_1_spark_oracle.json"
ZONES: tuple[str, ...] = ("UTC", "America/Los_Angeles", "Asia/Kolkata")
ANSI_MODES: tuple[str, ...] = ("false", "true")
SEATS: tuple[str, ...] = ("sql-literal", "sql-column", "expr", "typed")
FRACTION_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS"
LITERALS: dict[str, str] = {
    "ts": "TIMESTAMP'{v}'",
    "ntz": "TIMESTAMP_NTZ'{v}'",
    "d": "DATE'{v}'",
    "s": "'{v}'",
}
CASTS: dict[str, str] = {"ts": "timestamp", "ntz": "timestamp_ntz", "d": "date", "s": "string"}
PRE_EPOCH: tuple[str, ...] = (
    "1969-12-31 23:59:59.5",
    "1969-12-31 23:59:59.999999",
    "1969-12-31 23:59:59.000001",
    "1960-06-01 00:00:00.25",
    "1969-12-31 15:59:59.5",
    "1970-01-01 05:29:59.5",
    "1900-01-01 00:00:00.5",
    "0001-01-01 00:00:00.000001",
)
WHOLE_AND_POST_EPOCH: tuple[str, ...] = (
    "1969-12-31 23:59:59",
    "1960-06-01 00:00:00",
    "2025-01-15 12:00:59",
    "2025-01-15 12:00:59.5",
    "2025-01-15 12:00:59.999999",
)
NTZ_WALLS: tuple[str, ...] = PRE_EPOCH[:-1] + WHOLE_AND_POST_EPOCH
FAR: tuple[str, ...] = ("2999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999")
TIMESTAMPS: tuple[str, ...] = PRE_EPOCH + WHOLE_AND_POST_EPOCH + FAR
SIX_DIGIT_TEXTS: tuple[str, ...] = (
    "1969-12-31 23:59:59.999999",
    "1969-12-31 23:59:59.000001",
    "1960-06-01 00:00:00.250000",
    "0001-01-01 00:00:00.000001",
    "2025-01-15 12:00:59.999999",
    "1969-12-31 23:59:59.000000",
)
DATES: tuple[str, ...] = ("1960-06-01", "1969-12-31", "1970-01-01", "2025-01-15")
TEXTS: tuple[str, ...] = (
    " 2025-01-15 12:00:00",
    "\t2025-01-15 12:00:00",
    "2025-01-15  12:00:00",
    "2025-03-09 02:30:00",
    "2025-01-15 12:00:00",
    "2025-01-15 12:00:00 ",
    "2025-11-02 01:30:00",
    "2025-01-15T12:00:00",
    "1969-12-31 23:59:59",
    "2025-01-15",
)
NTZ_REFUSED: tuple[str, ...] = (
    "1969-12-31 23:59:59.5",
    "1960-06-01 00:00:00.25",
    "2025-01-15 12:00:59.5",
)
NTZ_LITERAL_GAP: frozenset[str] = frozenset(
    {
        "1969-12-31 23:59:59.5",
        "1969-12-31 23:59:59.999999",
        "1969-12-31 23:59:59.000001",
        "1969-12-31 23:59:59",
    }
)
FUNCTIONS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("unix_timestamp", "ts", "unix_timestamp({x})", TIMESTAMPS),
    ("to_unix_timestamp", "ts", "to_unix_timestamp({x})", TIMESTAMPS),
    ("unix_timestamp_fmt", "ts", "unix_timestamp({x}, '" + FRACTION_PATTERN + "')", TIMESTAMPS),
    ("unix_timestamp", "ntz", "unix_timestamp({x})", NTZ_WALLS),
    ("to_unix_timestamp", "ntz", "to_unix_timestamp({x})", NTZ_WALLS),
    ("unix_timestamp", "d", "unix_timestamp({x})", DATES),
    ("to_unix_timestamp", "d", "to_unix_timestamp({x})", DATES),
    ("unix_timestamp", "s", "unix_timestamp({x})", TEXTS),
    ("to_unix_timestamp", "s", "to_unix_timestamp({x})", TEXTS),
    ("unix_timestamp_fmt", "s", "unix_timestamp({x}, '" + FRACTION_PATTERN + "')", SIX_DIGIT_TEXTS),
    ("unix_seconds", "ts", "unix_seconds({x})", TIMESTAMPS),
    ("unix_millis", "ts", "unix_millis({x})", TIMESTAMPS),
    ("unix_seconds", "ntz", "unix_seconds({x})", NTZ_REFUSED),
    ("unix_millis", "ntz", "unix_millis({x})", NTZ_REFUSED),
    ("unix_micros", "ts", "unix_micros({x})", TIMESTAMPS),
    ("cast_bigint", "ts", "CAST({x} AS BIGINT)", TIMESTAMPS),
    ("unix_date", "d", "unix_date({x})", DATES),
)
STORED_PAIRS: tuple[tuple[str, str], ...] = (
    ("1969-12-31 23:59:00", "1969-12-31 23:59:59.5"),
    ("1969-12-31 23:58:59.5", "1970-01-01 00:00:30"),
    ("1960-06-01 00:00:00.25", "1960-06-01 00:10:00"),
    ("1969-12-31 15:58:00", "1969-12-31 15:59:59.5"),
    ("1970-01-01 05:28:00", "1970-01-01 05:29:59.5"),
    ("2025-01-15 12:00:00", "2025-01-15 12:59:59.5"),
    ("2025-01-15 12:00:00", "2025-01-15 13:00:00"),
)
EPOCH_VECTORS: tuple[str | None, ...] = (
    "1969-12-31 23:59:59.999999",
    "1969-12-31 23:59:59.4996",
    "1969-12-31 23:59:58.999999",
    "1969-12-31 23:59:59.000001",
    "1969-12-31 23:59:59",
    "1970-01-01 00:00:00",
    "1970-01-01 00:00:00.000001",
    "1970-01-01 00:00:00.999999",
    "1970-01-01 00:00:01",
    None,
)
EPOCH_FUNCTIONS: tuple[str, ...] = (
    "unix_seconds",
    "unix_millis",
    "unix_timestamp",
    "to_unix_timestamp",
    "unix_micros",
)
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?)\]")


def cell_key(function: str, kind: str, seat: str, zone: str, ansi: str, value: str) -> str:
    return "|".join((function, kind, seat, zone, ansi, value))


def cells() -> list[tuple[str, str, str, str, str, str, str]]:
    rows = []
    for zone in ZONES:
        for ansi in ANSI_MODES:
            for function, kind, template, values in FUNCTIONS:
                for seat in SEATS:
                    for value in values:
                        if kind == "ntz" and seat == "sql-literal" and value in NTZ_LITERAL_GAP:
                            continue
                        rows.append((function, kind, template, seat, zone, ansi, value))
    return rows


def typed(functions: Any, function: str, column: Any) -> Any:
    if function == "unix_timestamp_fmt":
        return functions.unix_timestamp(column, FRACTION_PATTERN)
    if function == "cast_bigint":
        return column.cast("bigint")
    return getattr(functions, function)(column)


def outcome(error: BaseException) -> dict[str, str]:
    text = str(error)
    found = _CONDITION.search(text)
    first = next((line for line in text.splitlines() if line.strip()), "")
    return {
        "error": found.group(1) if found else "NONE:" + type(error).__name__,
        "message": first.strip()[:400],
    }


def answer(
    session: Any, functions: Any, function: str, kind: str, template: str, seat: str, value: str
) -> dict[str, Any]:
    try:
        if seat == "sql-literal":
            literal = LITERALS[kind].format(v=value)
            got = session.sql("SELECT " + template.format(x=literal) + " AS r").collect()
        else:
            frame = session.createDataFrame([(value,)], "s string").select(
                functions.col("s").cast(CASTS[kind]).alias(kind)
            )
            if seat == "sql-column":
                frame.createOrReplaceTempView("unix_truncate_cells")
                got = session.sql(
                    "SELECT " + template.format(x=kind) + " AS r FROM unix_truncate_cells"
                ).collect()
            elif seat == "expr":
                got = frame.select(functions.expr(template.format(x=kind)).alias("r")).collect()
            else:
                got = frame.select(
                    typed(functions, function, functions.col(kind)).alias("r")
                ).collect()
        return {"value": got[0][0]}
    except Exception as error:
        return outcome(error)


def stored_minutes(session: Any, functions: Any, path: str) -> list[Any]:
    frame = session.createDataFrame(
        [(index, start, end) for index, (start, end) in enumerate(STORED_PAIRS)],
        "id int, start_text string, end_text string",
    ).select(
        functions.col("id"),
        functions.col("start_text").cast("timestamp").alias("start_at"),
        functions.col("end_text").cast("timestamp").alias("end_at"),
    )
    frame.createOrReplaceTempView("unix_truncate_spans")
    minutes = session.sql(
        "SELECT id, CAST((unix_timestamp(end_at) - unix_timestamp(start_at)) / 60 AS BIGINT)"
        " AS minutes FROM unix_truncate_spans"
    )
    minutes.write.mode("overwrite").parquet(path)
    read = session.read.parquet(path).orderBy("id").collect()
    return [row["minutes"] for row in read]


def six_digit_text(value: str | None) -> str | None:
    if value is None:
        return None
    whole, _, fraction = value.partition(".")
    return whole + "." + fraction.ljust(6, "0")


def epoch_vectors(session: Any, functions: Any, path: str) -> dict[str, dict[str, list[Any]]]:
    rows = [(index, value, six_digit_text(value)) for index, value in enumerate(EPOCH_VECTORS)]
    frame = session.createDataFrame(rows, "id int, s string, s6 string").select(
        "id", "s6", functions.col("s").cast("timestamp").alias("ts")
    )
    frame.write.mode("overwrite").parquet(path)
    cold = session.read.parquet(path)
    cold.createOrReplaceTempView("unix_truncate_epoch")
    vectors: dict[str, dict[str, list[Any]]] = {}
    for name in EPOCH_FUNCTIONS:
        sql = session.sql(
            f"SELECT id, {name}(ts) AS r, typeof({name}(ts)) AS t"
            " FROM unix_truncate_epoch ORDER BY id"
        ).collect()
        typed = cold.select("id", getattr(functions, name)(functions.col("ts")).alias("r"))
        expr = cold.select("id", functions.expr(f"{name}(ts)").alias("r"))
        vectors[name] = {
            "sql": [row["r"] for row in sql],
            "type": sorted({row["t"] for row in sql}),
            "typed": [row["r"] for row in typed.orderBy("id").collect()],
            "expr": [row["r"] for row in expr.orderBy("id").collect()],
        }
        if name in ("unix_timestamp", "to_unix_timestamp"):
            pattern = session.sql(
                f"SELECT id, {name}(s6, '{FRACTION_PATTERN}') AS r"
                " FROM unix_truncate_epoch ORDER BY id"
            ).collect()
            vectors[name]["pattern"] = [row["r"] for row in pattern]
    cast = session.sql("SELECT id, CAST(ts AS BIGINT) AS r FROM unix_truncate_epoch ORDER BY id")
    vectors["cast_bigint"] = {"sql": [row["r"] for row in cast.collect()]}
    return vectors


def record(path: Path, scratch: Path) -> None:
    from pyspark.sql import SparkSession, functions

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    recorded: dict[str, Any] = {}
    stored: dict[str, Any] = {}
    epochs: dict[str, Any] = {}
    for zone in ZONES:
        session.conf.set("spark.sql.session.timeZone", zone)
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.ansi.enabled", ansi)
            for function, kind, template, seat, cell_zone, cell_ansi, value in cells():
                if (cell_zone, cell_ansi) != (zone, ansi):
                    continue
                recorded[cell_key(function, kind, seat, zone, ansi, value)] = answer(
                    session, functions, function, kind, template, seat, value
                )
            target = scratch / f"stored-{zone.replace('/', '_')}-{ansi}"
            stored[f"{zone}|{ansi}"] = stored_minutes(session, functions, str(target))
            epochs[f"{zone}|{ansi}"] = epoch_vectors(session, functions, str(target) + "-epoch")
    document = {
        "spark": session.version,
        "recorded": "2026-11-22",
        "cells": recorded,
        "stored_minutes": stored,
        "epoch_vectors": epochs,
    }
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=FIXTURE)
    parser.add_argument("--scratch", type=Path, required=True)
    arguments = parser.parse_args(argv)
    record(arguments.out, arguments.scratch)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
