from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

FIXTURE = Path(__file__).resolve().parent / "string_cast_lenient_int_1_spark_oracle.json"
ANSI_MODES: tuple[str, ...] = ("false", "true")
TARGETS: tuple[str, ...] = ("tinyint", "smallint", "int", "bigint")
SEATS: tuple[str, ...] = (
    "sql-literal",
    "sql-values",
    "sql-column",
    "dataframe",
    "expr",
    "selectexpr",
    "try-cast",
)
LIMITS: dict[str, tuple[int, int]] = {
    "tinyint": (-128, 127),
    "smallint": (-32768, 32767),
    "int": (-2147483648, 2147483647),
    "bigint": (-9223372036854775808, 9223372036854775807),
}
SHARED_TEXTS: dict[str, tuple[str, ...]] = {
    "valid": ("42", "007", "+5", "-0", "+0", "-007", "0000000000000000000042"),
    "whitespace": (
        " 42 ",
        "\t7\n",
        " 1",
        "1 ",
        "\n5",
        "\r9\r",
        "\x0b3",
        "\x0c3",
        "\x003",
        "\x7f3",
        "  -12  ",
        " +8\t",
        "\x1f6\x1f",
    ),
    "unicode-space": ("\u00a03", "\u20033", "3\u3000", "\u00853"),
    "malformed": (
        "abc",
        "4 2",
        "1a",
        "a1",
        "0x10",
        "1,000",
        "1_000",
        "++1",
        "+-1",
        "--1",
        "+",
        "-",
        "- 1",
        "\uff11\uff12",
        "\u0661\u0662",
    ),
    "fraction": (
        "1.0",
        "1.5",
        "-1.9",
        "1.",
        ".5",
        "-.5",
        ".",
        "+.",
        "-.",
        " 2.5 ",
        "1.5a",
        "1..2",
        "1.2.3",
        "1.-2",
        "1.+2",
        ". 5",
    ),
    "exponent": ("1e3", "1E2", "1e-1", "2e0"),
    "special": ("NaN", "Infinity", "-Infinity", "inf", "nan"),
    "empty": ("",),
    "blank": (" ", "   ", "\t", "\n\t "),
}
ANSWERING_ON: tuple[str, ...] = ("valid", "whitespace", "limit", "limit-padded")
REFUSAL_TEXTS: tuple[str, ...] = ("abc", "1.0", "")
IMPLICIT_TEXTS: tuple[str, ...] = ("42", " 42 ", "abc")
WRITE_KINDS: tuple[str, ...] = ("insert", "merge", "writeto")
HUNDRED_BAD: tuple[str, ...] = ("abc", " 57 ", "1.5", "")
HUNDRED_SEATS: tuple[str, ...] = ("sql", "dataframe")
BAD_ROW = 57
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
_STATE = re.compile(r"SQLSTATE: ([0-9A-Z]{5})")


def target_texts(target: str) -> dict[str, tuple[str, ...]]:
    low, high = LIMITS[target]
    texts = dict(SHARED_TEXTS)
    texts["limit"] = (str(low), str(high))
    texts["limit-padded"] = (f" {low}", f"{high}\t")
    texts["limit-fraction"] = (f"{high}.9", f"{low}.9")
    texts["overflow"] = (
        str(high + 1),
        str(low - 1),
        "99999999999999999999",
        "-99999999999999999999",
        f"{high + 1}.0",
        f" {high + 1} ",
    )
    return texts


def text_class(target: str, text: str | None) -> str:
    if text is None:
        return "null"
    for name, values in target_texts(target).items():
        if text in values:
            return name
    return "unknown"


def cell_texts(target: str) -> list[str | None]:
    found: list[str | None] = [None]
    for values in target_texts(target).values():
        found.extend(value for value in values if value not in found)
    return found


def cell_key(seat: str, target: str, ansi: str, text: str | None) -> str:
    return "|".join((seat, target, ansi, json.dumps(text)))


def cells() -> list[tuple[str, str, str, str | None]]:
    return [
        (seat, target, ansi, text)
        for ansi in ANSI_MODES
        for target in TARGETS
        for seat in SEATS
        for text in cell_texts(target)
    ]


def literal(text: str | None) -> str:
    if text is None:
        return "NULL"
    return "'" + text + "'"


def outcome(error: BaseException) -> dict[str, Any]:
    text = str(error)
    found = _CONDITION.search(text)
    state = _STATE.search(text)
    if found and state and state.start() > found.start():
        message = text[found.start() : state.end()]
    else:
        message = text.strip().splitlines()[0][:300] if text.strip() else ""
    return {
        "error": found.group(1) if found else "NONE:" + type(error).__name__,
        "sqlstate": state.group(1) if state else None,
        "message": message,
    }


def frame_for(session: Any, functions: Any, seat: str, target: str, text: str | None) -> Any:
    if seat == "sql-literal":
        return session.sql(f"SELECT CAST({literal(text)} AS {target}) AS r")
    if seat == "sql-values":
        return session.sql(
            f"SELECT CAST(a AS {target}) AS r FROM (VALUES ({literal(text)})) AS t(a)"
        )
    source = session.createDataFrame([(text,)], "s string")
    if seat in ("sql-column", "try-cast"):
        source.createOrReplaceTempView("lenient_int_cells")
        keyword = "TRY_CAST" if seat == "try-cast" else "CAST"
        return session.sql(f"SELECT {keyword}(s AS {target}) FROM lenient_int_cells")
    if seat == "dataframe":
        return source.select(functions.col("s").cast(target))
    if seat == "expr":
        return source.select(functions.expr(f"CAST(s AS {target})"))
    return source.selectExpr(f"CAST(s AS {target})")


def answer(
    session: Any, functions: Any, seat: str, target: str, text: str | None
) -> dict[str, Any]:
    try:
        frame = frame_for(session, functions, seat, target, text)
        field = frame.schema.fields[0]
        rows = frame.collect()
        return {
            "value": rows[0][0],
            "type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
            "name": field.name,
        }
    except Exception as error:
        return outcome(error)


def hundred_rows(bad: str) -> list[tuple[int, str]]:
    return [(index, bad if index == BAD_ROW else str(index)) for index in range(100)]


def hundred(session: Any, functions: Any, seat: str, target: str, bad: str) -> dict[str, Any]:
    source = session.createDataFrame(hundred_rows(bad), "id int, s string")
    try:
        if seat == "sql":
            source.createOrReplaceTempView("lenient_int_hundred")
            frame = session.sql(
                f"SELECT id, CAST(s AS {target}) AS c FROM lenient_int_hundred ORDER BY id"
            )
        else:
            frame = source.select("id", functions.col("s").cast(target).alias("c")).orderBy("id")
        return {"values": [row["c"] for row in frame.collect()]}
    except Exception as error:
        return outcome(error)


def write_texts(target: str, ansi: str, batch: str) -> list[str | None]:
    if batch == "all":
        return cell_texts(target)
    if batch == "answering":
        grouped = target_texts(target)
        return [text for name in ANSWERING_ON for text in grouped[name]]
    return [batch]


def write_batches(ansi: str) -> list[str]:
    if ansi == "false":
        return ["all"]
    return ["answering", *REFUSAL_TEXTS]


def write_cells() -> list[tuple[str, str, str, str]]:
    return [
        (kind, target, ansi, batch)
        for ansi in ANSI_MODES
        for target in TARGETS
        for kind in WRITE_KINDS
        for batch in write_batches(ansi)
    ]


def write_key(kind: str, target: str, ansi: str, batch: str) -> str:
    return "|".join((kind, target, ansi, json.dumps(batch)))


def table_names(catalog: str, kind: str, target: str, ansi: str, slot: int) -> tuple[str, str]:
    stem = f"{catalog}.ns.{kind}_{target}_{ansi}_{slot}"
    return stem + "_t", stem + "_s"


def seed(session: Any, target: str, table: str, source: str, rows: list[Any]) -> None:
    session.sql(f"CREATE TABLE {source} (id INT, s STRING) USING iceberg").collect()
    session.sql(f"CREATE TABLE {table} (id INT, c {target}) USING iceberg").collect()
    values = ", ".join(f"({index}, {literal(text)})" for index, text in rows)
    session.sql(f"INSERT INTO {source} VALUES {values}").collect()


def run_write(
    session: Any, functions: Any, kind: str, target: str, table: str, source: str, cast: bool
) -> dict[str, Any]:
    value = f"CAST(u.s AS {target})" if cast else "u.s"
    try:
        if kind == "insert":
            session.sql(f"INSERT INTO {table} SELECT u.id, {value} FROM {source} AS u").collect()
        elif kind == "merge":
            session.sql(f"INSERT INTO {table} SELECT id, NULL FROM {source}").collect()
            session.sql(
                f"MERGE INTO {table} AS t USING {source} AS u ON t.id = u.id "
                f"WHEN MATCHED THEN UPDATE SET c = {value}"
            ).collect()
        else:
            column = functions.col("s").cast(target) if cast else functions.col("s")
            session.table(source).select("id", column.alias("c")).writeTo(table).append()
    except Exception as error:
        return {"refused": True, **outcome(error)}
    return {"refused": False}


def write_cell(
    session: Any, functions: Any, catalog: str, cell: tuple[str, str, str, str], slot: int
) -> tuple[dict[str, Any], str]:
    kind, target, ansi, batch = cell
    table, source = table_names(catalog, kind, target, ansi, slot)
    seed(session, target, table, source, list(enumerate(write_texts(target, ansi, batch))))
    return run_write(session, functions, kind, target, table, source, True), table


def implicit_cells() -> list[tuple[str, str, str, str]]:
    return [
        (kind, target, ansi, text)
        for ansi in ANSI_MODES
        for target in TARGETS
        for kind in WRITE_KINDS
        for text in IMPLICIT_TEXTS
    ]


def implicit_cell(
    session: Any, functions: Any, catalog: str, cell: tuple[str, str, str, str], slot: int
) -> tuple[dict[str, Any], str]:
    kind, target, ansi, text = cell
    table, source = table_names(catalog, kind, target, ansi, slot)
    seed(session, target, table, source, [(0, text)])
    return run_write(session, functions, kind, target, table, source, False), table


def stored(session: Any, table: str) -> list[list[Any]]:
    return [
        [row[0], row[1]] for row in session.sql(f"SELECT id, c FROM {table} ORDER BY id").collect()
    ]


def comparisons(target: str) -> dict[str, str]:
    return {
        "eq-padded": f"SELECT n FROM lenient_int_cmp WHERE CAST(n AS {target}) = ' 1 '",
        "eq-fraction": f"SELECT n FROM lenient_int_cmp WHERE CAST(n AS {target}) = '1.25'",
        "in-padded": f"SELECT n FROM lenient_int_cmp WHERE CAST(n AS {target}) IN (' 2', '3 ')",
        "lt-junk": f"SELECT n FROM lenient_int_cmp WHERE CAST(n AS {target}) < 'abc'",
    }


def comparison(session: Any, target: str, name: str) -> dict[str, Any]:
    session.createDataFrame([(1,), (2,), (3,)], "n int").createOrReplaceTempView("lenient_int_cmp")
    try:
        rows = session.sql(comparisons(target)[name]).collect()
        return {"values": sorted(row[0] for row in rows)}
    except Exception as error:
        return outcome(error)


def record_writes(session: Any, functions: Any, catalog: str) -> dict[str, Any]:
    writes: dict[str, Any] = {}
    slot = 0
    for kind, target, ansi, batch in write_cells():
        session.conf.set("spark.sql.ansi.enabled", ansi)
        slot += 1
        got, table = write_cell(session, functions, catalog, (kind, target, ansi, batch), slot)
        writes[write_key(kind, target, ansi, batch)] = {**got, "rows": stored(session, table)}
    for kind, target, ansi, text in implicit_cells():
        session.conf.set("spark.sql.ansi.enabled", ansi)
        slot += 1
        got, table = implicit_cell(session, functions, catalog, (kind, target, ansi, text), slot)
        writes["implicit|" + write_key(kind, target, ansi, text)] = {
            **got,
            "rows": stored(session, table),
        }
    return writes


def record(path: Path, scratch: Path, runtime_jar: str) -> None:
    from pyspark.sql import SparkSession, functions

    shutil.rmtree(scratch, ignore_errors=True)
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars", runtime_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lenient", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lenient.type", "hadoop")
        .config("spark.sql.catalog.lenient.warehouse", str(scratch / "warehouse"))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    recorded: dict[str, Any] = {}
    hundreds: dict[str, Any] = {}
    compared: dict[str, Any] = {}
    for ansi in ANSI_MODES:
        session.conf.set("spark.sql.ansi.enabled", ansi)
        for seat, target, cell_ansi, text in cells():
            if cell_ansi == ansi:
                recorded[cell_key(seat, target, ansi, text)] = answer(
                    session, functions, seat, target, text
                )
        for target in TARGETS:
            for seat in HUNDRED_SEATS:
                for bad in HUNDRED_BAD:
                    hundreds[cell_key(seat, target, ansi, bad)] = hundred(
                        session, functions, seat, target, bad
                    )
            for name in comparisons(target):
                compared[cell_key(name, target, ansi, None)] = comparison(session, target, name)
    session.sql("CREATE NAMESPACE IF NOT EXISTS lenient.ns").collect()
    writes = record_writes(session, functions, "lenient")
    document = {
        "spark": session.version,
        "recorded": "2026-11-22",
        "cells": recorded,
        "hundred": hundreds,
        "comparisons": compared,
        "writes": writes,
    }
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=FIXTURE)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--runtime-jar", required=True)
    arguments = parser.parse_args(argv)
    record(arguments.out, arguments.scratch, arguments.runtime_jar)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
