from __future__ import annotations

import contextlib
import json
import queue
import re
import statistics
import subprocess
import threading
import time
import uuid
from collections.abc import Callable
from functools import partial
from itertools import pairwise
from pathlib import Path
from typing import Any

COST_CALLS = 10
COST_TAG = "ICE-HELD-COST"
CELL_TAG = "ICE-HELD-CELL"
WRITER_TAG = "ICE-HELD-WRITER"
WRITER_LOG_TAG = "ICE-HELD-WRITER-LOG"
COST_FIELDS = ("label", "catalog", "n", "held_median_ms", "fresh_median_ms", "ratio")
FILTER = "n >= 0"
SEED = ["(1, 'a')", "(2, 'b')"]
GROWN = [*SEED, "(9, 'z')"]
KEPT = ["(2, 'b')"]
SPARK_ROWS: dict[str, list[Any]] = {
    "insert": [GROWN, GROWN],
    "delete": [1, 1],
    "copy": [KEPT, KEPT],
}
WRITER = Path(__file__).with_name("_acceptance_held_frame_writer.py")
WRITER_START_S = 900.0
WRITER_STEP_S = 300.0
EXTERNAL_ROW = "(9, 'z')"
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
TABLE_SHAPES = ("bare", "alias", "cache_table")
QUERY_SHAPES = ("sql_star", "filter", "agg", "df_view")
SHAPES = (*TABLE_SHAPES, *QUERY_SHAPES)
STEPS = ("after_writer", "after_props", "after_refresh")
READS = ("held", "fresh", "new", "held_again")
OPEN = "open: fresh-read-while-table-cached"
PENDING = "pending: #1394"
_HELD_AFTER_COMMIT = (
    ("after_props", "held"),
    ("after_props", "held_again"),
    ("after_refresh", "held"),
    ("after_refresh", "held_again"),
)
PENDING_1394: frozenset[tuple[str, str, str]] = frozenset(
    {("alias", step, read) for step, read in _HELD_AFTER_COMMIT[2:]}
    | {
        (shape, step, read)
        for shape in ("bare", *QUERY_SHAPES)
        for step, read in (("after_writer", "held_again"), *_HELD_AFTER_COMMIT)
    }
)
_PINNED_BEFORE_REFRESH = {"held": 2, "fresh": OPEN, "new": OPEN, "held_again": 2}
_TABLE_RULE = {
    "after_writer": _PINNED_BEFORE_REFRESH,
    "after_props": _PINNED_BEFORE_REFRESH,
    "after_refresh": dict.fromkeys(READS, 3),
}
_QUERY_RULE = {
    "after_writer": {"held": (2, 3), "fresh": None, "new": "fresh", "held_again": "fresh"},
    "after_props": dict.fromkeys(READS, 3),
    "after_refresh": dict.fromkeys(READS, 3),
}
_CACHE_TABLE_RULE = dict.fromkeys(STEPS, dict.fromkeys(READS, OPEN))
_SPARK_TABLE_RULE = {
    "after_writer": dict.fromkeys(READS, 2),
    "after_props": dict.fromkeys(READS, 2),
    "after_refresh": dict.fromkeys(READS, 3),
}
_SPARK_QUERY_RULE = {
    "after_writer": dict.fromkeys(READS),
    "after_props": dict.fromkeys(READS),
    "after_refresh": dict.fromkeys(READS, 3),
}
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
_ARN = re.compile(r"arn:[A-Za-z0-9:/_.+=,@-]+")
_ACCOUNT = re.compile(r"\b\d{12}\b")


def scrub(text: str) -> str:
    return _ACCOUNT.sub("<ACCOUNT>", _ARN.sub("<ARN>", text))


def condition_text(kind: str, message: str) -> str:
    text = scrub(" ".join(message.split()))
    found = _CONDITION.search(text)
    if found:
        return f"ERROR {found.group(1)}"
    return f"ERROR {kind}: {text[:120]}"


def condition(error: BaseException) -> str:
    return condition_text(type(error).__name__, str(error))


def rows(frame: Any) -> list[str]:
    return sorted(repr(tuple(row)) for row in frame.collect())


def answer(body: Callable[[], Any]) -> Any:
    try:
        return body()
    except Exception as error:
        return condition(error)


def cost_row(label: str, held_ms: list[float], fresh_ms: list[float]) -> dict[str, Any]:
    held = statistics.median(held_ms)
    fresh = statistics.median(fresh_ms)
    return {"label": label, "n": len(held_ms), "held": held, "fresh": fresh, "ratio": held / fresh}


def cost_line(catalog: str, row: dict[str, Any]) -> str:
    return (
        f"{COST_TAG} label={row['label']} catalog={catalog} n={row['n']} "
        f"held_median_ms={row['held']:.3f} fresh_median_ms={row['fresh']:.3f} "
        f"ratio={row['ratio']:.3f}"
    )


def parse_cost_line(line: str) -> dict[str, str]:
    tag, *fields = line.split()
    if tag != COST_TAG:
        raise ValueError(f"not a cost line: {line!r}")
    parsed = dict(field.split("=", 1) for field in fields)
    if tuple(parsed) != COST_FIELDS:
        raise ValueError(f"cost line fields {tuple(parsed)} are not {COST_FIELDS}")
    return parsed


def cell_line(catalog: str, label: str, found: Any, spark: Any) -> str:
    record = json.dumps({"answer": found, "spark": spark}, sort_keys=True)
    return f"{CELL_TAG} label={label} catalog={catalog} {record}"


def shape_line(
    catalog: str, leg: str, shape: str, found: dict[str, Any], graded: dict[str, Any]
) -> str:
    record = json.dumps({"answer": found, **graded}, sort_keys=True, default=str)
    return f"{CELL_TAG} label=shape_{shape} leg={leg} catalog={catalog} {record}"


def rule(shape: str, leg: str) -> dict[str, dict[str, Any]]:
    pinned = shape in TABLE_SHAPES
    if leg == "spark":
        return _SPARK_TABLE_RULE if pinned else _SPARK_QUERY_RULE
    if shape == "cache_table":
        return _CACHE_TABLE_RULE
    return _TABLE_RULE if pinned else _QUERY_RULE


def _wanted(want: Any, reads: dict[str, Any]) -> Any:
    return reads.get("fresh") if want == "fresh" else want


def _holds(got: Any, want: Any) -> bool:
    return got in want if isinstance(want, tuple) else got == want


def _validity(found: dict[str, Any]) -> dict[str, str]:
    checks = {
        "error": (found.get("error"), None),
        "before": (found.get("before"), 2),
        "writer": (found.get("writer"), {"count": 3}),
        "set_props": (found.get("set_props"), None),
        "refresh": (found.get("refresh"), None),
        "validity.fresh_after_refresh": ((found.get("after_refresh") or {}).get("fresh"), 3),
    }
    return {
        name: f"{got!r}, expected {want!r}" for name, (got, want) in checks.items() if got != want
    }


def grade(
    shape: str,
    found: dict[str, Any],
    leg: str,
    pending: frozenset[tuple[str, str, str]] = PENDING_1394,
) -> dict[str, Any]:
    graded: dict[str, Any] = {"failed": _validity(found), OPEN: {}, PENDING: {}, "recorded": {}}
    exempt = pending if leg == "repark" else frozenset()
    for step, wants in rule(shape, leg).items():
        reads = found.get(step) or {}
        for read, want in wants.items():
            name = f"{step}.{read}"
            got = reads.get(read)
            if want == OPEN:
                graded[OPEN][name] = got
            elif (shape, step, read) in exempt:
                graded[PENDING][name] = got
            elif want is None:
                graded["recorded"][name] = got
            elif not _holds(got, _wanted(want, reads)):
                graded["failed"][name] = f"{got!r}, expected {_wanted(want, reads)!r}"
    return graded


def spark_conf(catalog_config: dict[str, str], app: str, jars: dict[str, str]) -> dict[str, str]:
    return {
        "spark.master": "local[2]",
        "spark.app.name": app,
        "spark.ui.enabled": "false",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.sql.ansi.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.extensions": EXTENSIONS,
        **jars,
        **catalog_config,
    }


def spark_session(conf: dict[str, str]) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


class SparkProcessWriter:
    def __init__(
        self,
        python: str,
        conf: dict[str, str],
        script: Path = WRITER,
        start_s: float = WRITER_START_S,
        step_s: float = WRITER_STEP_S,
    ) -> None:
        self.step_s = step_s
        self.replies: queue.Queue[dict[str, Any]] = queue.Queue()
        self.process = subprocess.Popen(
            [python, str(script)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.pump = threading.Thread(target=self._drain, daemon=True)
        self.pump.start()
        self._send({"conf": conf})
        ready = self._reply(start_s)
        if ready != {"ready": True}:
            self.close()
            raise RuntimeError(f"the writer process did not start: {ready}")

    def __enter__(self) -> SparkProcessWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __call__(self, table: str, values: str) -> dict[str, Any]:
        self._send({"table": table, "values": values})
        found = self._reply(self.step_s)
        if "count" in found:
            return {"count": found["count"]}
        return {"error": condition_text(str(found.get("kind")), str(found.get("message")))}

    def close(self) -> None:
        if self.process.stdin is not None:
            with contextlib.suppress(OSError):
                self.process.stdin.close()
        try:
            self.process.wait(timeout=self.step_s)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.pump.join(timeout=self.step_s)

    def _send(self, request: dict[str, Any]) -> None:
        if self.process.stdin is None:
            raise RuntimeError("the writer process has no stdin")
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()

    def _reply(self, timeout: float) -> dict[str, Any]:
        try:
            found = self.replies.get(timeout=timeout)
        except queue.Empty as empty:
            self.process.kill()
            raise TimeoutError(f"the writer process gave no reply in {timeout} s") from empty
        if "exit" in found:
            raise RuntimeError(f"the writer process exited with {found['exit']}")
        return found

    def _drain(self) -> None:
        if self.process.stdout is not None:
            for line in self.process.stdout:
                text = scrub(line.rstrip("\n"))
                print(f"{WRITER_LOG_TAG} {text}", flush=True)
                if text.startswith(f"{WRITER_TAG} "):
                    self.replies.put(json.loads(text[len(WRITER_TAG) + 1 :]))
        self.replies.put({"exit": self.process.wait()})


def _view_of(session: Any, t: str) -> Any:
    name = f"v_{t.rsplit('.', 1)[1]}"
    session.table(t).createOrReplaceTempView(name)
    return session.table(name)


BUILDERS: dict[str, Callable[[Any, str], Any]] = {
    "bare": lambda s, t: s.table(t),
    "alias": lambda s, t: s.table(t).alias("a"),
    "cache_table": lambda s, t: s.table(t),
    "sql_star": lambda s, t: s.sql(f"SELECT * FROM {t}"),
    "filter": lambda s, t: s.table(t).filter("n > 0"),
    "agg": lambda s, t: s.table(t).groupBy("c").count(),
    "df_view": _view_of,
}


class _Accesses:
    def __init__(self, cells: HeldFrameCells, label: str) -> None:
        self.cells = cells
        self.label = label
        self.spans: list[tuple[float, float]] = []

    def __call__(self, body: Callable[[], Any]) -> Any:
        self.cells.wait(self.label, len(self.spans))
        start = time.perf_counter()
        value = body()
        self.spans.append((start, time.perf_counter()))
        return value

    def idle(self) -> list[float]:
        return [round(after[0] - before[1], 3) for before, after in pairwise(self.spans)]


class HeldFrameCells:
    def __init__(
        self,
        session: Any,
        catalog: str,
        namespace: str,
        prefix: str,
        properties: str,
        writer: Callable[[str, str], dict[str, Any]] | None = None,
        calls: int = COST_CALLS,
    ) -> None:
        self.s = session
        self.catalog = catalog
        self.namespace = namespace
        self.prefix = prefix
        self.properties = properties
        self.writer = writer
        self.calls = calls

    def wait(self, label: str, access: int) -> None:
        pass

    def sql(self, statement: str) -> None:
        self.s.sql(statement).collect()

    def make(self, tag: str) -> str:
        name = f"{self.prefix}held_{tag}_{uuid.uuid4().hex[:8]}"
        table = f"{self.catalog}.{self.namespace}.{name}"
        self.sql(
            f"CREATE TABLE {table} (n INT, c STRING) USING iceberg "
            f"TBLPROPERTIES ({self.properties})"
        )
        self.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
        return table

    def row_insert(self) -> list[Any]:
        t = self.make("insert")
        held = self.s.table(t)
        self.sql(f"INSERT INTO {t} VALUES (9, 'z')")
        return [rows(held), rows(self.s.table(t))]

    def row_delete(self) -> list[Any]:
        t = self.make("delete")
        held = self.s.table(t)
        self.sql(f"DELETE FROM {t} WHERE n = 1")
        return [held.count(), self.s.table(t).count()]

    def row_copy(self) -> list[Any]:
        t = self.make("copy")
        held = self.s.table(t)
        self.sql(f"DELETE FROM {t} WHERE n = 1")
        held.writeTo(f"{t}_c").create()
        self.s.table(t).writeTo(f"{t}_f").create()
        return [rows(self.s.table(f"{t}_c")), rows(self.s.table(f"{t}_f"))]

    def rows(self) -> dict[str, Any]:
        bodies = {"insert": self.row_insert, "delete": self.row_delete, "copy": self.row_copy}
        return {label: answer(body) for label, body in bodies.items()}

    def fresh_count(self, t: str) -> Any:
        return self.s.sql(f"SELECT count(*) FROM {t}").collect()[0][0]

    def new_count(self, shape: str, t: str) -> Any:
        return BUILDERS[shape](self.s, t).count()

    def reads(self, shape: str, t: str, held: Any, access: _Accesses) -> dict[str, Any]:
        return {
            "held": access(partial(answer, held.count)),
            "fresh": access(partial(answer, partial(self.fresh_count, t))),
            "new": access(partial(answer, partial(self.new_count, shape, t))),
            "held_again": access(partial(answer, held.count)),
        }

    def shape_steps(self, shape: str, t: str, found: dict[str, Any], access: _Accesses) -> None:
        if self.writer is None:
            raise ValueError("the shape cells need the other writer")
        if shape == "cache_table":
            self.sql(f"CACHE TABLE {t}")
            held = BUILDERS[shape](self.s, t)
        else:
            held = BUILDERS[shape](self.s, t).cache()
        found["before"] = access(partial(answer, held.count))
        found["writer"] = self.writer(t, EXTERNAL_ROW)
        found["after_writer"] = self.reads(shape, t, held, access)
        alter = f"ALTER TABLE {t} SET TBLPROPERTIES ('k' = 'v')"
        found["set_props"] = answer(partial(self.sql, alter))
        found["after_props"] = self.reads(shape, t, held, access)
        found["refresh"] = answer(partial(self.sql, f"REFRESH TABLE {t}"))
        found["after_refresh"] = self.reads(shape, t, held, access)

    def shape(self, shape: str) -> dict[str, Any]:
        found: dict[str, Any] = {}
        access = _Accesses(self, shape)
        try:
            self.shape_steps(shape, self.make(shape), found, access)
        except Exception as error:
            found["error"] = condition(error)
        found["idle_s"] = access.idle()
        answer(self.s.catalog.clearCache)
        return found

    def timed(self, body: Callable[[], Any]) -> float:
        start = time.perf_counter()
        body()
        return (time.perf_counter() - start) * 1000.0

    def cost_held_read(self) -> dict[str, Any]:
        t = self.make("cost_read")
        rows(self.s.table(t))
        held_ms: list[float] = []
        fresh_ms: list[float] = []
        for _ in range(self.calls):
            held = self.s.table(t)
            held_ms.append(self.timed(partial(rows, held)))
            fresh_ms.append(self.timed(lambda: rows(self.s.table(t))))
        return cost_row("held_read", held_ms, fresh_ms)

    def cost_held_filter_count(self) -> dict[str, Any]:
        t = self.make("cost_filter")
        held_ms: list[float] = []
        fresh_ms: list[float] = []
        for index in range(self.calls):
            held = self.s.table(t).filter(FILTER)
            self.sql(f"INSERT INTO {t} VALUES ({-2 * index - 1}, 'w')")
            held_ms.append(self.timed(held.count))
            self.sql(f"INSERT INTO {t} VALUES ({-2 * index - 2}, 'w')")
            fresh_ms.append(self.timed(lambda: self.s.table(t).filter(FILTER).count()))
        return cost_row("held_filter_count", held_ms, fresh_ms)
