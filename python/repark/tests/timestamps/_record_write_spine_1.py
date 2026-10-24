"""Record the WRITE-SPINE-1 step-0 door-by-shape census on Spark 4.1.2 and on RePark main.

NOT a ``test_`` module: pytest never collects it. Every cell of ``_write_spine_1_shapes.py``
creates a fresh Iceberg table with the target schema, runs the spelling's seed and template,
reads the table back and drops it. The stored value is rendered as the ZONE-ORACLE-1 recorder
renders values (``_record_zone_oracle_1.py``): microseconds for timestamps, ISO text for
dates, lists for arrays, objects for structs and maps. A failing step records the step, the
error class and the first line of the error. Run it as a script in its own interpreter::

    JAVA_HOME=... SPARK_LOCAL_IP=127.0.0.1 python _record_write_spine_1.py --spark
    python _record_write_spine_1.py --main --base <sha>

``--spark`` records Spark 4.1.2 with Iceberg 1.11.0: a Hadoop catalog in a temp directory,
``local[1]``, ANSI on, one session per zone in its own child process, and the process zone
set to UTC so PySpark converts timestamps without a local shift. It writes
``write_spine_1_spark_oracle.json``.

``--main --base <sha>`` records the installed RePark build the same way, one child process per
zone: the ``spark_sql`` and ``dataframe`` spellings through a ``ReparkSession`` facade session,
the ``native_sql`` spellings through ``repark.sql()`` bound to a native session of that zone,
each with a memory catalog in a temp directory. The native door has no temp views, so its source
relation is a table made by the same ``SELECT``; its target DDL is the native dialect
(``TIMESTAMP_LTZ`` for an instant column, a ``WITH`` clause for partitioning and merge-on-read).
Each cell is classified against the Spark fixture and the document is written to
``write_spine_1_main_<sha>.json``.

``--census --base <sha>`` prints the census tables and the red set from the two fixtures.
``--only TEXT`` runs the cells whose id contains ``TEXT`` and prints them; it never writes a
fixture.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import _write_spine_1_shapes as shapes
from _record_zone_oracle_1 import error_class, render_arrow, render_value
from _write_spine_1_spellings import DATAFRAME, NATIVE_SQL, SESSION_CODEC, WRITE_OPTION

HERE = Path(__file__).resolve().parent
ORACLE_FILE = HERE / "write_spine_1_spark_oracle.json"
RECORDER = "python/repark/tests/timestamps/_record_write_spine_1.py"
SESSION_ZONE_KEY = "spark.sql.session.timeZone"
CATALOG = "ice"
NAMESPACE = "ns"
TABLE_DIGITS = 12
READ_SQL = "SELECT v FROM {table} ORDER BY id"
MOR_KEYS: tuple[str, ...] = ("write.delete.mode", "write.update.mode", "write.merge.mode")
MOR_PROPERTIES = ", ".join(f"'{key}'='merge-on-read'" for key in MOR_KEYS)
NATIVE_INSTANT_TYPE = re.compile(r"\bTIMESTAMP\b(?!_)")
NATIVE_SOURCE_SQL = shapes.VIEW_SQL.replace("CREATE OR REPLACE TEMP VIEW", "CREATE TABLE")
ERROR_TEXT_LIMIT = 320
EQUAL = "EQUAL"
DIFFERENT = "DIFFERENT"
REFUSED = "REFUSED"
ERROR = "ERROR"
UNRECORDED = "UNRECORDED"
STATUSES: tuple[str, ...] = (EQUAL, DIFFERENT, REFUSED, ERROR, UNRECORDED)
REFUSAL_MARKERS: tuple[str, ...] = (
    "refus",
    "unsupported",
    "not supported",
    "is not written",
    "cannot be written",
    "cannot store-assign",
)
SPARK_CONDITION = re.compile(r"\[[A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*\]")
ICEBERG_GAV = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
ICEBERG_VERSION = "1.11.0"


class StepError(Exception):
    """A cell step failed; carries the step name and the original error."""

    def __init__(self, step: str, error: BaseException) -> None:
        """Keep the failed step and the error it raised."""
        super().__init__(step)
        self.step = step
        self.error = error


def first_line(error: BaseException) -> str:
    """Return the first non-empty line of the error text, cut at a word boundary."""
    lines = [line.strip() for line in str(error).splitlines() if line.strip()]
    text = lines[0] if lines else type(error).__name__
    if len(text) <= ERROR_TEXT_LIMIT:
        return text
    return text[:ERROR_TEXT_LIMIT].rsplit(" ", 1)[0]


def error_record(step: str, error: BaseException) -> dict[str, Any]:
    """Return the recorded shape of a failed step: the step, the class and the first line."""
    return {"step": step, "error_class": error_class(error), "error": first_line(error)}


def table_name(cell: shapes.Cell) -> str:
    """Return the cell's target table; the name depends only on the cell id, so texts repeat.

    The name is decimal digits, so no hexadecimal fragment of it reads as a misspelled word.
    """
    digest = int(hashlib.sha256(cell.cell_id.encode("utf-8")).hexdigest(), 16)
    return f"{CATALOG}.{NAMESPACE}.t_{digest % 10**TABLE_DIGITS:0{TABLE_DIGITS}d}"


def create_sql(cell: shapes.Cell, table: str) -> str:
    """Return the Spark-dialect CREATE TABLE statement of the cell's target table."""
    properties = "'format-version'='2'"
    if cell.mode == "mor":
        properties += f", {MOR_PROPERTIES}"
    partition = " PARTITIONED BY (k)" if cell.spelling.partitioned else ""
    return (
        f"CREATE TABLE {table} ({shapes.table_columns(cell)}) USING iceberg{partition} "
        f"TBLPROPERTIES ({properties})"
    )


def native_type(spark_type: str) -> str:
    """Return the ``repark.sql()`` type spelling that creates the same Iceberg type."""
    return NATIVE_INSTANT_TYPE.sub("TIMESTAMP_LTZ", spark_type)


def native_create_sql(cell: shapes.Cell, table: str) -> str:
    """Return the native-dialect CREATE TABLE statement of the cell's target table.

    The native door spells table properties and partitioning in a ``WITH`` clause and an
    instant column as ``TIMESTAMP_LTZ``; its ``TIMESTAMP`` is a wall clock.
    """
    columns = native_type(shapes.table_columns(cell))
    options: list[str] = []
    if cell.spelling.partitioned:
        options.append("partitioning = ARRAY['k']")
    if cell.mode == "mor":
        keys = ", ".join(f"'{key}'" for key in MOR_KEYS)
        values = ", ".join("'merge-on-read'" for _ in MOR_KEYS)
        options.append(f"extra_properties = MAP(ARRAY[{keys}], ARRAY[{values}])")
    clause = f" WITH ({', '.join(options)})" if options else ""
    return f"CREATE TABLE {table} ({columns}){clause}"


class Door:
    """One engine door: runs statements and writers and reads a table back."""

    def run(self, statement: str) -> None:
        """Run one statement to completion."""
        raise NotImplementedError

    def write_frame(self, cell: shapes.Cell, table: str) -> None:
        """Run the cell's DataFrame writer call."""
        raise NotImplementedError

    def read(self, table: str) -> dict[str, Any]:
        """Return the stored values of ``v`` and their type."""
        raise NotImplementedError

    def drop(self, table: str) -> None:
        """Drop the table; a missing table is fine."""
        try:
            self.run(f"DROP TABLE IF EXISTS {table}")
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return

    def create(self, cell: shapes.Cell, table: str) -> None:
        """Create the cell's target table in this door's dialect."""
        self.run(create_sql(cell, table))

    def set_conf(self, pairs: tuple[tuple[str, str], ...]) -> None:
        """Set the spelling's session conf pairs."""

    def unset_conf(self, pairs: tuple[tuple[str, str], ...]) -> None:
        """Unset the spelling's session conf pairs."""


def frame_writer(frame: Any, cell: shapes.Cell, table: str, condition: Any) -> None:
    """Run the DataFrame writer the cell's spelling names on ``frame``."""
    name = cell.spelling.name
    if name == "df_append":
        frame.writeTo(table).append()
    elif name == "df_overwrite_partitions":
        frame.writeTo(table).overwritePartitions()
    elif name == "df_overwrite_condition":
        frame.writeTo(table).overwrite(condition)
    elif name == "df_insert_into":
        frame.write.insertInto(table)
    elif name == "df_insert_into_overwrite":
        frame.write.insertInto(table, overwrite=True)
    elif name == "df_save_as_table":
        frame.write.mode("append").saveAsTable(table)
    elif name == "insert_options":
        frame.writeTo(table).option(*WRITE_OPTION).append()
    else:
        raise KeyError(f"no DataFrame writer for {name}")


class SparkDoor(Door):
    """The live Spark 4.1.2 session with the Hadoop Iceberg catalog."""

    def __init__(self, session: Any) -> None:
        """Bind the Spark session."""
        self.session = session

    def run(self, statement: str) -> None:
        """Run one statement; Spark executes commands eagerly."""
        self.session.sql(statement).collect()

    def write_frame(self, cell: shapes.Cell, table: str) -> None:
        """Run the writer call on ``spark.sql({select})``."""
        from pyspark.sql.functions import col

        frame = self.session.sql(cell.holes(table)["select"])
        frame_writer(frame, cell, table, col("id") >= 0)

    def read(self, table: str) -> dict[str, Any]:
        """Collect ``v`` ordered by ``id``."""
        frame = self.session.sql(READ_SQL.format(table=table))
        rows = frame.collect()
        return {
            "type": frame.schema[0].dataType.simpleString(),
            "values": [render_value(row[0]) for row in rows],
        }

    def set_conf(self, pairs: tuple[tuple[str, str], ...]) -> None:
        """Set each pair on the live session."""
        for key, value in pairs:
            self.session.conf.set(key, value)

    def unset_conf(self, pairs: tuple[tuple[str, str], ...]) -> None:
        """Unset each pair on the live session."""
        for key, _ in pairs:
            self.session.conf.unset(key)


class FacadeDoor(Door):
    """A RePark ``ReparkSession`` facade session with a memory catalog."""

    def __init__(self, session: Any) -> None:
        """Bind the facade session."""
        self.session = session

    def run(self, statement: str) -> None:
        """Run one statement and drain its result."""
        self.session.sql(statement).collect()

    def write_frame(self, cell: shapes.Cell, table: str) -> None:
        """Run the writer call on ``spark.sql({select})``."""
        from repark import functions

        frame = self.session.sql(cell.holes(table)["select"])
        frame_writer(frame, cell, table, functions.col("id") >= 0)

    def read(self, table: str) -> dict[str, Any]:
        """Read ``v`` ordered by ``id`` through the Arrow path."""
        frame = self.session.sql(READ_SQL.format(table=table))
        arrow = frame.to_arrow()
        field_type = arrow.schema.field(0).type
        return {
            "type": frame.schema[0].dataType.simpleString(),
            "values": [render_arrow(value, field_type) for value in arrow.column(0).to_pylist()],
        }


class NativeDoor(FacadeDoor):
    """``repark.sql()`` bound to one native session with a memory catalog."""

    def __init__(self, native: Any) -> None:
        """Bind the native session behind ``repark.sql()``."""
        self.native = native
        self.session = native

    def bind(self) -> None:
        """Point ``repark.sql()`` at this native session."""
        import repark

        repark._ANSI_NATIVE = self.native

    def run(self, statement: str) -> None:
        """Run one statement through ``repark.sql()`` and drain its result."""
        import repark

        self.bind()
        repark.sql(statement).collect()

    def create(self, cell: shapes.Cell, table: str) -> None:
        """Create the cell's target table in the native dialect."""
        self.run(native_create_sql(cell, table))

    def read(self, table: str) -> dict[str, Any]:
        """Read ``v`` ordered by ``id`` through ``repark.sql()``."""
        import repark

        self.bind()
        frame = repark.sql(READ_SQL.format(table=table))
        arrow = frame.to_arrow()
        field_type = arrow.schema.field(0).type
        return {
            "type": frame.schema[0].dataType.simpleString(),
            "values": [render_arrow(value, field_type) for value in arrow.column(0).to_pylist()],
        }


def run_step(step: str, action: Any, *arguments: Any) -> None:
    """Run one cell step and wrap any failure in :class:`StepError`."""
    try:
        action(*arguments)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        raise StepError(step, error) from error


def measure(door: Door, cell: shapes.Cell, table: str) -> dict[str, Any]:
    """Create, seed, write, read and drop one cell's table; return the recorded answer."""
    spelling = cell.spelling
    door.set_conf(spelling.session_conf)
    try:
        if not spelling.creates_table:
            run_step("create", door.create, cell, table)
        if spelling.seed:
            run_step("seed", door.run, cell.statement(spelling.seed, table))
        if spelling.reach == DATAFRAME:
            run_step("write", door.write_frame, cell, table)
        else:
            run_step("write", door.run, cell.statement(spelling.template, table))
        try:
            answer = door.read(table)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            raise StepError("read", error) from error
        return {"step": "read", **answer}
    except StepError as failure:
        return error_record(failure.step, failure.error)
    finally:
        door.unset_conf(spelling.session_conf)
        door.drop(table)


def spark_session(zone: str, warehouse: Path) -> Any:
    """Build the recorder's own ``local[1]`` Spark session with a Hadoop Iceberg catalog.

    Raises:
        RuntimeError: the session runs on a master other than ``local[1]``.
    """
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("record-write-spine-1")
        .config("spark.sql.ansi.enabled", "true")
        .config(SESSION_ZONE_KEY, zone)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", ICEBERG_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )
    master = session.sparkContext.master
    if master != "local[1]":
        raise RuntimeError(f"the recorder needs its own local[1] SparkContext, got {master!r}")
    session.sparkContext.setLogLevel("OFF")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    session.sql(shapes.VIEW_SQL)
    return session


def repark_doors(zone: str, warehouse: Path) -> dict[str, Door]:
    """Build the facade session and the two native sessions of one zone."""
    os.environ["TZ"] = "UTC"
    time.tzset()
    from repark import ReparkSession
    from repark._native import PyReparkSession

    facade = (
        ReparkSession.builder.appName("record-write-spine-1-main")
        .config("spark.sql.ansi.enabled", "true")
        .config(SESSION_ZONE_KEY, zone)
        .getOrCreate()
    )
    facade.register_memory_catalog(CATALOG, str(warehouse / "facade"))
    facade.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}").collect()
    facade.sql(shapes.VIEW_SQL).collect()
    doors: dict[str, Door] = {"facade": FacadeDoor(facade)}
    for name, extra in (("native", ()), ("native_conf", SESSION_CODEC)):
        config = {SESSION_ZONE_KEY: zone, "spark.sql.ansi.enabled": "true", **dict(extra)}
        native = PyReparkSession.native(config=config)
        place = warehouse / name
        native.register_memory_catalog(CATALOG, str(place))
        native.create_namespace(CATALOG, NAMESPACE, str(place / NAMESPACE))
        door = NativeDoor(native)
        door.run(NATIVE_SOURCE_SQL)
        doors[name] = door
    return doors


def repark_door_of(doors: dict[str, Door], cell: shapes.Cell) -> Door:
    """Return the RePark door a cell's spelling enters."""
    if cell.spelling.reach != NATIVE_SQL:
        return doors["facade"]
    if cell.spelling.session_conf:
        return doors["native_conf"]
    return doors["native"]


def zone_cells(zone: str, only: str) -> list[shapes.Cell]:
    """Return the cells of one zone, narrowed to ids containing ``only`` when it is set."""
    return [
        cell for cell in shapes.cells() if cell.zone == zone and (not only or only in cell.cell_id)
    ]


def child(engine: str, zone: str, part: Path, only: str) -> int:
    """Record every cell of one zone on one engine, appending one JSON line per cell."""
    done: set[str] = set()
    if part.exists():
        for line in part.read_text(encoding="utf-8").splitlines():
            done.add(json.loads(line)["cell_id"])
    todo = [cell for cell in zone_cells(zone, only) if cell.cell_id not in done]
    warehouse = Path(tempfile.mkdtemp(prefix="write-spine-1-"))
    try:
        if engine == "spark":
            session = spark_session(zone, warehouse)
            door: Door = SparkDoor(session)
            doors = {"facade": door, "native": door, "native_conf": door}
            version = session.version
        else:
            doors = repark_doors(zone, warehouse)
            version = ""
        with part.open("a", encoding="utf-8") as handle:
            for index, cell in enumerate(todo):
                table = table_name(cell)
                answer = measure(repark_door_of(doors, cell), cell, table)
                handle.write(json.dumps({"cell_id": cell.cell_id, **answer}) + "\n")
                handle.flush()
                if index % 100 == 0:
                    print(f"{engine} {zone} {index}/{len(todo)}", file=sys.stderr, flush=True)
        if version:
            part.with_suffix(".version").write_text(version, encoding="utf-8")
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)
    return 0


def run_children(engine: str, parts: Path, only: str) -> dict[str, dict[str, Any]]:
    """Run one child process per zone in parallel and return every cell's answer."""
    processes = []
    for zone in shapes.ZONES:
        part = parts / f"{engine}-{shapes.ZONE_TAGS[zone]}.jsonl"
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            engine,
            "--zone",
            zone,
            "--part",
            str(part),
            "--only",
            only,
        ]
        processes.append(subprocess.Popen(command))
    codes = [process.wait() for process in processes]
    if any(codes):
        raise SystemExit(f"a zone child failed: {codes}")
    answers: dict[str, dict[str, Any]] = {}
    for zone in shapes.ZONES:
        part = parts / f"{engine}-{shapes.ZONE_TAGS[zone]}.jsonl"
        for line in part.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            answers[row.pop("cell_id")] = row
    return answers


def ordered(answers: dict[str, dict[str, Any]], only: str) -> dict[str, dict[str, Any]]:
    """Return the answers in corpus order; a cell with no answer is marked ``unrecorded``."""
    return {
        cell.cell_id: answers.get(cell.cell_id, {"step": "unrecorded"})
        for cell in shapes.cells()
        if not only or only in cell.cell_id
    }


def dump_document(document: dict[str, Any]) -> str:
    """Return the fixture text: the provenance indented, then one line per cell."""
    lines = json.dumps(document["provenance"], indent=1).splitlines()
    head = "\n ".join(lines)
    rows = [
        f"  {json.dumps(key)}: {json.dumps(answer, separators=(',', ':'))}"
        for key, answer in document["cells"].items()
    ]
    return '{\n "provenance": ' + head + ',\n "cells": {\n' + ",\n".join(rows) + "\n }\n}\n"


def is_refusal(answer: dict[str, Any]) -> bool:
    """Return whether a RePark error reads as a deliberate refusal.

    A refusal names a Spark error condition in brackets, or says it refuses, does not support
    or cannot store-assign the write. Anything else, an engine or Arrow error included, is not.
    """
    text = f"{answer.get('error_class', '')} {answer.get('error', '')}"
    if SPARK_CONDITION.search(text):
        return True
    lowered = text.lower()
    return any(marker in lowered for marker in REFUSAL_MARKERS)


def classify(expected: dict[str, Any] | None, measured: dict[str, Any]) -> str:
    """Classify one RePark answer against the recorded Spark cell.

    ``EQUAL``: the same type and values, or both raise and RePark's class or text holds
    Spark's error class. ``DIFFERENT``: both answer differently, Spark raises and RePark
    answers, or both raise with another class. ``REFUSED``: Spark answers and RePark raises a
    deliberate refusal. ``ERROR``: Spark answers and RePark raises anything else.
    ``UNRECORDED``: the Spark fixture holds no answer for the cell.
    """
    if expected is None or expected.get("step") == "unrecorded":
        return UNRECORDED
    if "error" in expected:
        if "error" in measured:
            spark_class = str(expected["error_class"])
            text = f"{measured['error_class']} {measured['error']}"
            return EQUAL if spark_class in text else DIFFERENT
        return DIFFERENT
    if "error" in measured:
        return REFUSED if is_refusal(measured) else ERROR
    same = measured["type"] == expected["type"] and measured["values"] == expected["values"]
    return EQUAL if same else DIFFERENT


def spark_mode(arguments: argparse.Namespace) -> int:
    """Record every cell on Spark and write the oracle fixture."""
    with tempfile.TemporaryDirectory(prefix="write-spine-1-parts-") as parts:
        answers = run_children("spark", Path(parts), arguments.only)
        version_file = Path(parts) / f"spark-{shapes.ZONE_TAGS[shapes.ZONES[0]]}.version"
        version = version_file.read_text(encoding="utf-8") if version_file.exists() else ""
    cells = ordered(answers, arguments.only)
    if arguments.only:
        print(json.dumps(cells, indent=1))
        return 0
    document = {
        "provenance": {
            "spark": version,
            "iceberg": ICEBERG_VERSION,
            "catalog": "hadoop",
            "master": "local[1]",
            "ansi": "true",
            "recorded": datetime.date.today().isoformat(),
            "generator_sha256": shapes.corpus_hash(),
            "cells": len(cells),
            "recorder": f"{RECORDER} --spark",
        },
        "cells": cells,
    }
    arguments.out.write_text(dump_document(document), encoding="utf-8")
    print(f"write-spine-1 spark cells={len(cells)} -> {arguments.out}")
    return 0


RED_STATUSES: tuple[str, ...] = (DIFFERENT, REFUSED, ERROR)
DOOR_ORDER: tuple[str, ...] = (
    "D1",
    "D2",
    "D3",
    "D4",
    "D5",
    "D6",
    "D7",
    "D8",
    "D9",
    "D10",
    "D14",
    "D15",
    "D16",
    "D17",
    "D18",
    "D19",
    "D20",
    "D21",
)
LINE_LIMIT = 96
TABLE_NAME = re.compile(r"`?ice`?\.`?ns`?\.`?t_[0-9]+`?")
AXES: dict[str, shapes.Cell] = {cell.cell_id: cell for cell in shapes.cells()}


def answer_text(answer: dict[str, Any]) -> str:
    """Return a short form of one answer: ``answers`` or ``raises at <step>``."""
    if "error" in answer:
        return f"raises at {answer['step']}"
    return "answers"


def tally(answers: list[dict[str, Any]]) -> str:
    """Return ``<answers> / <raises>`` for a group of recorded answers."""
    raised = sum(1 for answer in answers if "error" in answer)
    return f"{len(answers) - raised} / {raised}"


def census_rows(spark: dict[str, Any], main: dict[str, Any]) -> list[str]:
    """Return the door x family x zone rows of the census table."""
    rows = [
        "| door | family | zone | cells | Spark answers / raises | main answers / raises | "
        "EQUAL | DIFFERENT | REFUSED | ERROR | UNRECORDED |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for door in DOOR_ORDER:
        for family in shapes.CONTAINERS:
            for zone in shapes.ZONES:
                keys = [
                    key
                    for key in main
                    if AXES[key].spelling.door == door
                    and AXES[key].target.container == family
                    and AXES[key].zone == zone
                ]
                if not keys:
                    continue
                counts = [sum(1 for key in keys if main[key]["status"] == s) for s in STATUSES]
                spark_tally = tally([spark[key] for key in keys if key in spark])
                main_tally = tally([main[key] for key in keys])
                rows.append(
                    f"| {door} | {family} | {zone} | {len(keys)} | {spark_tally} | {main_tally} | "
                    + " | ".join(str(count) for count in counts)
                    + " |"
                )
    return rows


def different_rows(spark: dict[str, Any], main: dict[str, Any]) -> list[str]:
    """Return, per door, the three kinds of ``DIFFERENT`` cell."""
    rows = [
        "| door | both answer, another value or type | both raise, another class | "
        "Spark raises, main answers |",
        "|---|---|---|---|",
    ]
    for door in DOOR_ORDER:
        kinds = [0, 0, 0]
        for key, cell in main.items():
            if AXES[key].spelling.door != door or cell["status"] != DIFFERENT:
                continue
            spark_raises = "error" in spark[key]
            main_raises = "error" in cell
            if spark_raises and main_raises:
                kinds[1] += 1
            elif spark_raises:
                kinds[2] += 1
            else:
                kinds[0] += 1
        if any(kinds):
            rows.append(f"| {door} | " + " | ".join(str(kind) for kind in kinds) + " |")
    return rows


def error_rows(main: dict[str, Any]) -> list[str]:
    """Return main's non-EQUAL answers grouped by door, status and first error line."""
    groups: dict[tuple[str, str, str], int] = {}
    for key, cell in main.items():
        if cell["status"] not in RED_STATUSES:
            continue
        if "error" in cell:
            error = TABLE_NAME.sub("<table>", cell["error"])
            text = f"{cell['step']}: {error[:LINE_LIMIT]}"
        else:
            text = "answers another value or type"
        text = text.replace("|", "\\|")
        group = (AXES[key].spelling.door, cell["status"], text)
        groups[group] = groups.get(group, 0) + 1
    rows = ["| door | status | main's answer | cells |", "|---|---|---|---|"]
    for door in DOOR_ORDER:
        for (group_door, status, text), count in sorted(groups.items()):
            if group_door == door:
                rows.append(f"| {door} | {status} | {text} | {count} |")
    return rows


def red_set(main: dict[str, Any]) -> list[str]:
    """Return every cell id main answers differently from Spark, grouped by door.

    One line per ``<spelling>.<mode>.<target>`` prefix lists its ``<source>.<zone>`` suffixes,
    so each id is the prefix, a dot and one suffix.
    """
    lines: list[str] = []
    for door in DOOR_ORDER:
        keys = [key for key in main if AXES[key].spelling.door == door]
        if not keys:
            continue
        red = [key for key in keys if main[key]["status"] in RED_STATUSES]
        lines.extend(["", f"### {door} ({len(red)} of {len(keys)} cells)", ""])
        if not red:
            lines.append("None.")
            continue
        prefixes: dict[str, list[str]] = {}
        for key in red:
            cell = AXES[key]
            prefix = f"{cell.spelling.name}.{cell.mode}.{cell.target.tag}"
            prefixes.setdefault(prefix, []).append(key.removeprefix(prefix + "."))
        lines.append("```")
        lines.extend(f"{prefix}: {' '.join(suffixes)}" for prefix, suffixes in prefixes.items())
        lines.append("```")
    return lines


def census_mode(arguments: argparse.Namespace) -> int:
    """Print the census tables and the red set from the two fixtures."""
    spark = json.loads(ORACLE_FILE.read_text(encoding="utf-8"))["cells"]
    path = arguments.out or HERE / f"write_spine_1_main_{arguments.base}.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    main = document["cells"]
    lines = [
        f"Counts: {document['provenance']['counts']}.",
        "",
        "## Census table",
        "",
        *census_rows(spark, main),
        "",
        "## The DIFFERENT cells by kind",
        "",
        *different_rows(spark, main),
        "",
        "## Main's answers that are not EQUAL, by first line",
        "",
        *error_rows(main),
        "",
        "## The red set: every cell id where main differs from Spark, by door",
        *red_set(main),
    ]
    unrecorded = [key for key, cell in main.items() if cell["status"] == UNRECORDED]
    lines.extend(["", f"UNRECORDED cells: {len(unrecorded)}.", *unrecorded, ""])
    print("\n".join(lines))
    return 0


def main_mode(arguments: argparse.Namespace) -> int:
    """Record every cell on the installed RePark build and write the main fixture."""
    import repark

    oracle = json.loads(ORACLE_FILE.read_text(encoding="utf-8"))
    if oracle["provenance"]["generator_sha256"] != shapes.corpus_hash():
        raise SystemExit("the Spark fixture was recorded from another corpus hash")
    with tempfile.TemporaryDirectory(prefix="write-spine-1-parts-") as parts:
        answers = run_children("main", Path(parts), arguments.only)
    cells = ordered(answers, arguments.only)
    for key, answer in cells.items():
        answer["status"] = classify(oracle["cells"].get(key), answer)
    if arguments.only:
        print(json.dumps(cells, indent=1))
        return 0
    counts = {
        status: sum(1 for answer in cells.values() if answer["status"] == status)
        for status in STATUSES
    }
    document = {
        "provenance": {
            "engine": f"repark {repark.__version__}",
            "main": arguments.base,
            "build": "maturin develop --profile ci",
            "catalog": "memory",
            "recorded": datetime.date.today().isoformat(),
            "generator_sha256": shapes.corpus_hash(),
            "cells": len(cells),
            "counts": counts,
            "recorder": f"{RECORDER} --main --base {arguments.base}",
        },
        "cells": cells,
    }
    out = arguments.out or HERE / f"write_spine_1_main_{arguments.base}.json"
    out.write_text(dump_document(document), encoding="utf-8")
    print(f"write-spine-1 main counts={counts} -> {out}")
    return 0


def main(argv: list[str]) -> int:
    """Record the Spark fixture with ``--spark`` or the main fixture with ``--main``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spark", action="store_true")
    parser.add_argument("--main", action="store_true")
    parser.add_argument("--base", default="unknown")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--only", default="")
    parser.add_argument("--child", choices=("spark", "main"), default=None)
    parser.add_argument("--zone", default="")
    parser.add_argument("--part", type=Path, default=None)
    parser.add_argument("--census", action="store_true")
    arguments = parser.parse_args(argv)
    if arguments.census:
        return census_mode(arguments)
    if arguments.child:
        return child(arguments.child, arguments.zone, arguments.part, arguments.only)
    if arguments.spark:
        arguments.out = arguments.out or ORACLE_FILE
        return spark_mode(arguments)
    if arguments.main:
        return main_mode(arguments)
    parser.error("choose --spark or --main")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
