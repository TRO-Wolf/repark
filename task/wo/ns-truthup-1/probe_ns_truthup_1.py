"""Measure the four NS-TRUTHUP-1 registry rows on this head, one object per cell."""

from __future__ import annotations

import argparse
import io
import json
import re
import tempfile
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from repark import ReparkSession

VALUE = "2026-01-02 03:04:05.123456789"
OTHER = "2020-05-06 07:08:09.5"
ZONES = ("UTC", "America/New_York")
ZONE_TAG = {"UTC": "utc", "America/New_York": "ny"}
V3 = "TBLPROPERTIES ('format-version' = '3')"
LITERAL = "TIMESTAMP '2026-01-02 03:04:05'"
CLASS_PATTERN = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
REFUSAL_MARKERS = ("refus", "unsupported", "not supported", "cannot safely", "not writable")
CONTAINERS = {
    "arr": ("array(" + LITERAL + ")", "(id INT, arr ARRAY<timestamp_ns>)", "arr"),
    "struct": (
        "named_struct('v', " + LITERAL + ")",
        "(id INT, st STRUCT<v: timestamp_ns>)",
        "st",
    ),
    "map": ("map('k', " + LITERAL + ")", "(id INT, m MAP<STRING, timestamp_ns>)", "m"),
}


def open_session(warehouse: Path, zone: str) -> Any:
    """Open a v3-enabled ANSI session in zone over warehouse with namespace ns."""
    session = (
        ReparkSession.builder.appName("ns-truthup-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(warehouse))
    session.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
    return session


def first_line(text: str) -> str:
    """Return the first line of text with whitespace folded to single spaces."""
    return " ".join(text.splitlines()[0].split()) if text.strip() else ""


def error_class(text: str) -> str | None:
    """Return the first bracketed Spark class in text, or None when absent."""
    found = CLASS_PATTERN.search(text)
    return found.group(1) if found else None


def is_refusal(text: str) -> bool:
    """Return whether an error text reads as a deliberate named refusal."""
    lowered = text.lower()
    return any(marker in lowered for marker in REFUSAL_MARKERS)


def ns_readback(session: Any, table: str, column: str, where: str) -> str:
    """Read column of table at where as int64 nanoseconds plus its string render."""
    arrow = session.sql(f"SELECT {column} FROM {table} WHERE {where}").to_arrow()
    ticks = arrow.column(0).cast(pa.int64()).to_pylist()
    rendered = (
        session.sql(f"SELECT CAST({column} AS STRING) AS v FROM {table} WHERE {where}")
        .to_arrow()
        .column(0)
        .to_pylist()
    )
    return "; ".join(f"{text} ({tick})" for text, tick in zip(rendered, ticks, strict=True))


def ids_readback(session: Any, table: str) -> str:
    """Read the ordered id column of table as a bracketed list."""
    arrow = session.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    return "[" + ", ".join(str(v) for v in arrow.column(0).to_pylist()) + "]"


def run_cell(
    warehouse: Path,
    row: str,
    cell: str,
    statement: str,
    zone: str,
    fmt: str,
    kind: str,
    setup: list[str],
    action: str,
    verify: Any = None,
    target: str = "",
) -> dict[str, Any]:
    """Run one cell in its own session and return the recorded object."""
    record: dict[str, Any] = {
        "row": row,
        "cell": cell,
        "statement": statement,
        "ansi": "true",
        "zone": zone,
        "format": fmt,
        "outcome": "error",
        "class": None,
        "text": "",
        "value": "",
    }
    session = open_session(warehouse, zone)
    try:
        for sql in setup:
            session.sql(sql).collect()
        if action == "sql":
            frame = session.sql(statement)
            rows = frame.collect()
            record["outcome"] = "stores" if kind == "write" else "answers"
            record["value"] = "; ".join(str(r) for r in rows)
            record["text"] = first_line(record["value"])
        elif action == "schema":
            record["value"] = str(session.table(target or statement).schema)
            record["outcome"] = "answers"
            record["text"] = first_line(record["value"])
        elif action == "dtypes":
            record["value"] = repr(session.table(target or statement).dtypes)
            record["outcome"] = "answers"
            record["text"] = first_line(record["value"])
        elif action == "print_schema":
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                returned = session.table(target or statement).printSchema()
            record["value"] = buffer.getvalue() or str(returned)
            record["outcome"] = "answers"
            record["text"] = first_line(record["value"])
        if verify is not None:
            try:
                record["value"] = verify(session, record["value"])
            except BaseException as failed:
                record["value"] = f"VERIFY-FAILED {type(failed).__name__}: {failed}"
            record["text"] = first_line(record["value"])
        return record
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        text = f"{type(error).__name__}: {' '.join(str(error).split())}"
        record["outcome"] = "refuses" if is_refusal(text) else "error"
        record["class"] = error_class(text)
        record["text"] = first_line(text)
        return record
    finally:
        session.stop()


def r010_cells(warehouse: Path) -> list[dict[str, Any]]:
    """Build the twelve UPDATE and DELETE nanosecond-cast cells of R-010."""
    cells: list[dict[str, Any]] = []
    for zone in ZONES:
        tag = ZONE_TAG[zone]
        for target, column in (("timestamp_ns", "ts"), ("timestamptz_ns", "tz")):
            short = "ns" if target == "timestamp_ns" else "tzns"
            seed = (
                f"(1, CAST('{VALUE}' AS timestamp_ns), CAST('{VALUE}' AS timestamptz_ns)), "
                f"(2, CAST('{OTHER}' AS timestamp_ns), CAST('{OTHER}' AS timestamptz_ns))"
            )
            specs = (
                ("set", f"UPDATE {{t}} SET {column} = CAST('{VALUE}' AS {target}) WHERE id = 2"),
                ("where", f"UPDATE {{t}} SET id = 99 WHERE {column} = CAST('{VALUE}' AS {target})"),
                ("delete", f"DELETE FROM {{t}} WHERE {column} = CAST('{VALUE}' AS {target})"),
            )
            for position, template in specs:
                name = f"r010_{position}_{short}_{tag}"
                table = f"ice.ns.{name}"
                setup = [
                    "CREATE TABLE " + table + " (id INT, ts timestamp_ns, tz timestamptz_ns) "
                    "USING iceberg " + V3,
                    f"INSERT INTO {table} VALUES {seed}",
                ]
                statement = template.format(t=table)
                cells.append(
                    run_cell(
                        warehouse,
                        "ICE-TSNS-SQL-1-R-010",
                        f"r010.{position}.{short}.{tag}",
                        statement,
                        zone,
                        "v3",
                        "write",
                        setup,
                        "sql",
                        R010Verify(table, position, column),
                    )
                )
    return cells


class R010Verify:
    """Read back the row an R-010 write touched, in the cell session."""

    def __init__(self, table: str, position: str, column: str) -> None:
        """Keep the table, the position and the column to read back."""
        self.table = table
        self.position = position
        self.column = column

    def __call__(self, session: Any, current: str) -> str:
        """Return the stored value or the surviving ids, ignoring the write echo."""
        if self.position == "set":
            return self.column + "=" + ns_readback(session, self.table, self.column, "id = 2")
        return "ids=" + ids_readback(session, self.table)


def r013_cells(warehouse: Path) -> list[dict[str, Any]]:
    """Build the three kernel-resolution and listing cells of R-013."""
    literal = "SELECT __repark_cast_timestamp_ns__('" + VALUE + "')"
    select = run_cell(
        warehouse,
        "ICE-TSNS-SQL-1-R-013",
        "r013.select",
        literal,
        "UTC",
        "n/a",
        "read",
        [],
        "sql",
        R013Arrow(literal),
    )
    routines = run_cell(
        warehouse,
        "ICE-TSNS-SQL-1-R-013",
        "r013.routines",
        "SELECT routine_name FROM information_schema.routines ORDER BY 1",
        "UTC",
        "n/a",
        "read",
        [],
        "sql",
        R013Routines(),
    )
    show = run_cell(
        warehouse,
        "ICE-TSNS-SQL-1-R-013",
        "r013.show",
        "SHOW FUNCTIONS",
        "UTC",
        "n/a",
        "read",
        [],
        "sql",
    )
    return [select, routines, show]


class R013Arrow:
    """Append the Arrow type of the R-013 kernel SELECT to the collected row."""

    def __init__(self, literal: str) -> None:
        """Keep the kernel SELECT whose Arrow type is read back."""
        self.literal = literal

    def __call__(self, session: Any, current: str) -> str:
        """Return the collected row plus the Arrow type in brackets."""
        arrow = session.sql(self.literal).to_arrow()
        return current + " [" + str(arrow.schema.field(0).type) + "]"


class R013Routines:
    """Replace the routines listing with the reserved-prefix names plus the count."""

    def __call__(self, session: Any, current: str) -> str:
        """List every routines name with the reserved prefix plus the total count."""
        arrow = session.sql("SELECT routine_name FROM information_schema.routines").to_arrow()
        names = sorted(str(v) for v in arrow.column(0).to_pylist() if v is not None)
        kept = [name for name in names if name.startswith("__repark_")]
        return f"total={len(names)} repark={kept}"


def r018_cells(warehouse: Path) -> list[dict[str, Any]]:
    """Build the eighteen container VALUES, INSERT and OVERWRITE cells of R-018."""
    cells: list[dict[str, Any]] = []
    for zone in ZONES:
        tag = ZONE_TAG[zone]
        for shape, (expr, ddl, column) in CONTAINERS.items():
            bare = run_cell(
                warehouse,
                "ICE-TSNS-SQL-1-R-018",
                f"r018.bare.{shape}.{tag}",
                f"VALUES (1, {expr})",
                zone,
                "n/a",
                "read",
                [],
                "sql",
            )
            cells.append(bare)
            for spelling, verb in (("insert", "INSERT INTO"), ("overwrite", "INSERT OVERWRITE")):
                name = f"r018_{spelling}_{shape}_{tag}"
                table = f"ice.ns.{name}"
                setup = ["CREATE TABLE " + table + " " + ddl + " USING iceberg " + V3]
                statement = f"{verb} {table} VALUES (1, {expr})"
                cells.append(
                    run_cell(
                        warehouse,
                        "ICE-TSNS-SQL-1-R-018",
                        f"r018.{spelling}.{shape}.{tag}",
                        statement,
                        zone,
                        "v3",
                        "write",
                        setup,
                        "sql",
                        R018Stored(table, column),
                    )
                )
    return cells


class R018Stored:
    """Read back the stored container column of an R-018 table in the cell session."""

    def __init__(self, table: str, column: str) -> None:
        """Keep the table and the container column to read back."""
        self.table = table
        self.column = column

    def __call__(self, session: Any, current: str) -> str:
        """Return the stored container values, ignoring the write echo."""
        arrow = session.sql(f"SELECT {self.column} FROM {self.table}").to_arrow()
        return str(arrow.column(0).to_pylist())


def r001_cells(warehouse: Path) -> list[dict[str, Any]]:
    """Build the six DESCRIBE and schema-surface cells of R-001."""
    cells: list[dict[str, Any]] = []
    specs = (
        ("describe", "DESCRIBE TABLE {t}", "sql"),
        ("describe_extended", "DESCRIBE TABLE EXTENDED {t}", "sql"),
        ("show_columns", "SHOW COLUMNS FROM {t}", "sql"),
        ("schema", 'spark.table("{t}").schema', "schema"),
        ("print_schema", 'spark.table("{t}").printSchema()', "print_schema"),
        ("dtypes", 'spark.table("{t}").dtypes', "dtypes"),
    )
    for surface, template, action in specs:
        name = f"r001_{surface}"
        table = f"ice.ns.{name}"
        setup = [
            "CREATE TABLE " + table + " (ts timestamp_ns, tz timestamptz_ns) USING iceberg " + V3
        ]
        statement = template.format(t=table)
        cells.append(
            run_cell(
                warehouse,
                "ICE-TSNS-SQL-1-R-001",
                f"r001.{surface}",
                statement,
                "UTC",
                "v3",
                "read",
                setup,
                action,
                None,
                table,
            )
        )
    return cells


def md_escape(text: str) -> str:
    """Fold a cell value to one markdown table cell without pipes or newlines."""
    return " ".join(text.replace("|", "/").split())


def write_markdown(path: Path, cells: list[dict[str, Any]]) -> str:
    """Write one markdown table line per cell and return the document text."""
    lines = ["| cell | outcome | class | value |", "|---|---|---|---|"]
    for cell in cells:
        shown = cell["value"] if cell["outcome"] in ("stores", "answers") else cell["text"]
        lines.append(
            f"| {cell['cell']} | {cell['outcome']} | {cell['class'] or ''} "
            f"| {md_escape(str(shown))[:300]} |"
        )
    text = "\n".join(lines) + "\n"
    path.write_text(text, encoding="utf-8")
    return text


def main(argv: list[str] | None = None) -> int:
    """Run every NS-TRUTHUP-1 cell and write the JSON record plus the cell table."""
    parser = argparse.ArgumentParser(description="Measure the NS-TRUTHUP-1 registry rows.")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--md", type=Path, required=True)
    parser.add_argument("--warehouse", type=Path, default=None)
    arguments = parser.parse_args(argv)
    root = arguments.warehouse or Path(tempfile.mkdtemp(prefix="ns-truthup-1-"))
    root.mkdir(parents=True, exist_ok=True)
    cells = r010_cells(root) + r013_cells(root) + r018_cells(root) + r001_cells(root)
    document = {
        "provenance": {
            "recorded": date.today().isoformat(),
            "order": "NS-TRUTHUP-1 step 0",
            "cells": len(cells),
            "recorder": "task/wo/ns-truthup-1/probe_ns_truthup_1.py",
        },
        "cells": cells,
    }
    arguments.out.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    write_markdown(arguments.md, cells)
    counts: dict[str, int] = {}
    for cell in cells:
        counts[cell["outcome"]] = counts.get(cell["outcome"], 0) + 1
    print(f"cells={len(cells)} counts={counts} warehouse={root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
