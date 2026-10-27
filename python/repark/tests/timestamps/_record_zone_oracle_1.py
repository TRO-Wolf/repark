"""Record Spark 4.1.2's answers for ZONE-ORACLE-1 into ``zone_oracle_1_spark_oracle.json``.

NOT a ``test_`` module: pytest never collects it. It runs every cell of
``_zone_oracle_1_corpus.py`` as a plain ``SELECT`` on one live PySpark session set to the
cell's zone, with ``spark.sql.ansi.enabled`` true (Spark 4's default). Each cell records the
statement, the zone, the result type (``simpleString`` of the column's data type) and the
value as ``collect()`` renders it: microseconds for timestamps, ISO text for dates, the text
for strings, lists for arrays, objects for structs and maps. A failing cell records the error
condition instead. The provenance header carries the Spark version, the recording date and the
corpus hash, so a fixture older than its generator fails the replay.

The recorder refuses any SparkContext whose master is not ``local[1]`` and sets its own
process zone to UTC before it builds the session, so PySpark converts timestamp values
without a local-zone shift. Run it as a script in its own interpreter::

    JAVA_HOME=... SPARK_LOCAL_IP=127.0.0.1 python _record_zone_oracle_1.py
    JAVA_HOME=... SPARK_LOCAL_IP=127.0.0.1 python _record_zone_oracle_1.py --check
    JAVA_HOME=... SPARK_LOCAL_IP=127.0.0.1 python _record_zone_oracle_1.py --check --sample 20

The first form rewrites the fixture. ``--check`` re-derives the cells and exits non-zero on
drift; ``--sample N`` re-derives only the N cells chosen by hash.

``--main`` is the second recording: it needs no PySpark. It replays every cell on the
installed RePark build through ``ReparkSession.sql`` in a session set to the cell's zone,
classifies each cell against the Spark fixture (``EQUAL``, ``DIFFERENT``, ``REFUSED`` or
``ERROR``), and writes the cells that are not ``EQUAL``, with the measured RePark answer, to
``zone_oracle_1_main_difference.json``. ``--main --base <sha>`` records the main commit the
build came from; ``--main --check`` compares a fresh replay with the committed list. RePark's
Arrow path supplies the value, because ``collect()`` renders a ``TIMESTAMP`` at the session
wall clock, and the facade schema supplies the Spark type name.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from _zone_oracle_1_corpus import (
    VIEW_SQL,
    ZONES,
    Cell,
    cells,
    corpus_hash,
    sample_ids,
)

ORACLE_FILE = Path(__file__).with_name("zone_oracle_1_spark_oracle.json")
MAIN_FILE = Path(__file__).with_name("zone_oracle_1_main_difference.json")
EQUAL = "EQUAL"
DIFFERENT = "DIFFERENT"
REFUSED = "REFUSED"
ERROR = "ERROR"
STATUSES: tuple[str, ...] = (EQUAL, DIFFERENT, REFUSED, ERROR)
REFUSAL_MARKERS: tuple[str, ...] = ("refus", "unsupported", "not supported")
ERROR_TEXT_LIMIT = 320
SESSION_ZONE_KEY = "spark.sql.session.timeZone"
EPOCH_AWARE = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
EPOCH_NAIVE = datetime.datetime(1970, 1, 1)
ONE_MICROSECOND = datetime.timedelta(microseconds=1)


def micros(value: datetime.datetime) -> int:
    """Return a datetime as whole microseconds since the epoch, reading a naive one as UTC."""
    if value.tzinfo is None:
        return (value - EPOCH_NAIVE) // ONE_MICROSECOND
    return (value - EPOCH_AWARE) // ONE_MICROSECOND


def render_key(key: Any) -> Any:
    """Render a map key as JSON: timestamps in microseconds, anything else as text."""
    if isinstance(key, datetime.datetime):
        return micros(key)
    if isinstance(key, datetime.date):
        return key.isoformat()
    return str(key)


def render_value(value: Any) -> Any:
    """Render a collected value as JSON: timestamps in microseconds, dates in ISO text."""
    if isinstance(value, datetime.datetime):
        return micros(value)
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {render_key(key): render_value(item) for key, item in value.items()}
    if hasattr(value, "asDict"):
        return {key: render_value(item) for key, item in value.asDict(recursive=False).items()}
    if isinstance(value, (list, tuple)):
        return [render_value(item) for item in value]
    return value


def error_class(error: BaseException) -> str:
    """Return Spark's error condition, or the exception class name when it has none."""
    getter = getattr(error, "getCondition", None)
    condition = getter() if callable(getter) else None
    return str(condition) if condition else type(error).__name__


def render_arrow(value: Any, arrow_type: Any) -> Any:
    """Render an Arrow ``to_pylist`` value like :func:`render_value`, reading maps as objects."""
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        return {render_key(key): render_arrow(item, arrow_type.item_type) for key, item in value}
    if pa.types.is_struct(arrow_type):
        return {field.name: render_arrow(value[field.name], field.type) for field in arrow_type}
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return [render_arrow(item, arrow_type.value_type) for item in value]
    return render_value(value)


def error_head(error: BaseException) -> str:
    """Return the exception class and its message on one line, cut at a word boundary."""
    text = f"{type(error).__name__}: {' '.join(str(error).split())}"
    if len(text) <= ERROR_TEXT_LIMIT:
        return text
    return text[:ERROR_TEXT_LIMIT].rsplit(" ", 1)[0]


def repark_cell(session: Any, cell: Cell) -> dict[str, Any]:
    """Run one cell on a RePark session already set to its zone and return the measured answer.

    A Rust panic caught at the Python boundary surfaces as an exception that is not always an
    ``Exception`` subclass, so everything but an interpreter exit is recorded as an error.
    """
    try:
        frame = session.sql(cell.sql)
        table = frame.to_arrow()
        values = table.column(0).to_pylist()
        if len(values) != 1:
            raise RuntimeError(f"{cell.cell_id} returned {len(values)} rows")
        return {
            "type": frame.schema[0].dataType.simpleString(),
            "value": render_arrow(values[0], table.schema.field(0).type),
        }
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        return {"error": error_head(error)}


def replay(session: Any, selected: list[Cell]) -> dict[str, dict[str, Any]]:
    """Replay the selected cells on a RePark session, zone by zone, in corpus order."""
    session.conf.set("spark.sql.ansi.enabled", "true")
    answers: dict[str, dict[str, Any]] = {}
    for zone in ZONES:
        in_zone = [cell for cell in selected if cell.zone == zone]
        if not in_zone:
            continue
        session.conf.set(SESSION_ZONE_KEY, zone)
        session.sql(VIEW_SQL)
        for cell in in_zone:
            answers[cell.cell_id] = repark_cell(session, cell)
    return {cell.cell_id: answers[cell.cell_id] for cell in selected}


def is_refusal(error: str) -> bool:
    """Return whether a RePark error text reads as a deliberate refusal."""
    lowered = error.lower()
    return any(marker in lowered for marker in REFUSAL_MARKERS)


def classify(expected: dict[str, Any], measured: dict[str, Any]) -> str:
    """Classify one RePark answer against the recorded Spark cell.

    ``EQUAL``: the same type and value, or both raise and RePark's text names Spark's error
    condition. ``DIFFERENT``: both answer with another type or value, Spark raises and RePark
    answers, or both raise with another condition. ``REFUSED``: Spark answers and RePark raises
    a deliberate refusal. ``ERROR``: Spark answers and RePark raises anything else.
    """
    if "error" in expected:
        if "error" in measured:
            return EQUAL if expected["error"] in measured["error"] else DIFFERENT
        return DIFFERENT
    if "error" in measured:
        return REFUSED if is_refusal(measured["error"]) else ERROR
    same = measured["type"] == expected["type"] and measured["value"] == expected["value"]
    return EQUAL if same else DIFFERENT


def classify_all(
    recorded: dict[str, dict[str, Any]], measured: dict[str, dict[str, Any]]
) -> dict[str, str]:
    """Classify every measured cell against its recorded Spark answer."""
    return {key: classify(recorded[key], answer) for key, answer in measured.items()}


def run_cell(session: Any, cell: Cell) -> dict[str, Any]:
    """Run one cell on a session already set to its zone and return the recorded shape."""
    answer: dict[str, Any] = {"sql": cell.sql, "zone": cell.zone}
    try:
        frame = session.sql(cell.sql)
        rows = frame.collect()
        if len(rows) != 1:
            raise RuntimeError(f"{cell.cell_id} returned {len(rows)} rows")
        answer["type"] = frame.schema[0].dataType.simpleString()
        answer["value"] = render_value(rows[0][0])
    except Exception as error:
        answer = {"sql": cell.sql, "zone": cell.zone, "error": error_class(error)}
    return answer


def _spark() -> Any:
    """Build the recorder's own ``local[1]`` session, or fail on a borrowed one.

    ``getOrCreate`` keeps the master of a SparkContext already live in the process, and a
    borrowed context would also keep its process zone, so the recording would not be the
    reproducible one.

    Raises:
        RuntimeError: the session runs on a master other than ``local[1]``.
    """
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("record-zone-oracle-1")
        .config("spark.sql.ansi.enabled", "true")
        .config(SESSION_ZONE_KEY, "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    master = session.sparkContext.master
    if master != "local[1]":
        raise RuntimeError(
            f"the zone-oracle recorder needs its own local[1] SparkContext, got {master!r}: "
            "run it as a script in its own interpreter"
        )
    session.sparkContext.setLogLevel("OFF")
    return session


def record(session: Any, selected: list[Cell]) -> dict[str, dict[str, Any]]:
    """Run the selected cells zone by zone and return the answers in corpus order."""
    answers: dict[str, dict[str, Any]] = {}
    for zone in ZONES:
        in_zone = [cell for cell in selected if cell.zone == zone]
        if not in_zone:
            continue
        session.conf.set(SESSION_ZONE_KEY, zone)
        session.sql(VIEW_SQL)
        for cell in in_zone:
            answers[cell.cell_id] = run_cell(session, cell)
    return {cell.cell_id: answers[cell.cell_id] for cell in selected}


def load_document() -> dict[str, Any]:
    """Return the committed fixture document."""
    return json.loads(ORACLE_FILE.read_text(encoding="utf-8"))


def selected_cells(sample: int) -> list[Cell]:
    """Return every cell, or the ``sample`` cells chosen by hash when ``sample`` is positive."""
    everything = cells()
    if sample <= 0:
        return everything
    chosen = set(sample_ids(sample))
    return [cell for cell in everything if cell.cell_id in chosen]


def build_document(session: Any, answers: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Wrap recorded answers in the provenance header."""
    return {
        "provenance": {
            "spark": session.version,
            "recorded": datetime.date.today().isoformat(),
            "master": session.sparkContext.master,
            "ansi": session.conf.get("spark.sql.ansi.enabled"),
            "generator_sha256": corpus_hash(),
            "cells": len(answers),
            "recorder": "python/repark/tests/timestamps/_record_zone_oracle_1.py",
        },
        "cells": answers,
    }


def check(answers: dict[str, dict[str, Any]]) -> int:
    """Compare fresh answers with the committed fixture and print every drift."""
    committed = load_document()
    stale = committed["provenance"]["generator_sha256"] != corpus_hash()
    if stale:
        print("DRIFT the fixture was recorded from another corpus hash")
    drift = [key for key in answers if answers[key] != committed["cells"].get(key)]
    for key in drift:
        fresh = json.dumps(answers[key])
        recorded = json.dumps(committed["cells"].get(key))
        print(f"DRIFT {key}: fresh={fresh} recorded={recorded}")
    print(f"zone-oracle cells checked={len(answers)} drift={len(drift)}")
    return 1 if drift or stale else 0


def build_main_document(
    statuses: dict[str, str], measured: dict[str, dict[str, Any]], base: str
) -> dict[str, Any]:
    """Build the main-difference document: every cell that is not ``EQUAL``, as measured."""
    counts = {
        status: sum(1 for value in statuses.values() if value == status) for status in STATUSES
    }
    return {
        "provenance": {
            "recorded": datetime.date.today().isoformat(),
            "main": base,
            "build": "maturin develop --profile ci",
            "generator_sha256": corpus_hash(),
            "cells": len(statuses),
            "counts": counts,
            "recorder": "python/repark/tests/timestamps/_record_zone_oracle_1.py --main",
        },
        "cells": {
            key: {"status": status, **measured[key]}
            for key, status in statuses.items()
            if status != EQUAL
        },
    }


def main_mode(arguments: argparse.Namespace) -> int:
    """Replay the corpus on the installed RePark build and write or check the difference list."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("record-zone-oracle-1-main").getOrCreate()
    recorded = load_document()["cells"]
    measured = replay(session, cells())
    statuses = classify_all(recorded, measured)
    document = build_main_document(statuses, measured, arguments.base)
    counts = document["provenance"]["counts"]
    if arguments.check:
        committed = json.loads(MAIN_FILE.read_text(encoding="utf-8"))["cells"]
        drift = sorted(set(committed) ^ set(document["cells"]))
        drift += sorted(
            key
            for key in set(committed) & set(document["cells"])
            if committed[key] != document["cells"][key]
        )
        for key in drift:
            print(f"DRIFT {key}")
        print(f"zone-oracle main counts={counts} drift={len(drift)}")
        return 1 if drift else 0
    MAIN_FILE.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"zone-oracle main counts={counts} -> {MAIN_FILE}")
    return 0


def main(argv: list[str]) -> int:
    """Rewrite the Spark fixture, or check a fresh recording; ``--main`` records RePark."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--out", type=Path, default=ORACLE_FILE)
    parser.add_argument("--main", action="store_true")
    parser.add_argument("--base", default="unknown")
    arguments = parser.parse_args(argv)
    if arguments.main:
        return main_mode(arguments)
    session = _spark()
    answers = record(session, selected_cells(arguments.sample))
    if arguments.check:
        return check(answers)
    if arguments.sample > 0:
        raise SystemExit("--sample re-derives a subset; it never rewrites the fixture")
    document = build_document(session, answers)
    arguments.out.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"zone-oracle cells recorded={len(answers)} -> {arguments.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
