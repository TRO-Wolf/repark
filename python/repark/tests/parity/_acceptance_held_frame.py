from __future__ import annotations

import json
import re
import statistics
import time
import uuid
from collections.abc import Callable
from functools import partial
from itertools import pairwise
from typing import Any

SPARK_CACHE_INTERVAL_S = 30.0
COST_CALLS = 10
COST_TAG = "ICE-HELD-COST"
CELL_TAG = "ICE-HELD-CELL"
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
SPARK_EXTERNAL: dict[str, dict[str, Any]] = {
    "control": {
        "stale": [2, 2, SEED, 2, 4, 4, 4, 4],
        "expired": [2, 3, GROWN, 3, 4, 4, 4, 4],
        "decisive": {1: 1, 2: 2, 3: 3},
    },
    "refresh": {
        "stale": [2, 2, 3, 3],
        "expired": [3, 3, 3, 3],
        "decisive": {0: 0, 1: 1},
    },
}
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
_ARN = re.compile(r"arn:[A-Za-z0-9:/_.+=,@-]+")
_ACCOUNT = re.compile(r"\b\d{12}\b")


def condition(error: BaseException) -> str:
    text = _ACCOUNT.sub("<ACCOUNT>", _ARN.sub("<ARN>", " ".join(str(error).split())))
    found = _CONDITION.search(text)
    if found:
        return f"ERROR {found.group(1)}"
    return f"ERROR {type(error).__name__}: {text[:120]}"


def rows(frame: Any) -> list[str]:
    return sorted(repr(tuple(row)) for row in frame.collect())


def answer(body: Callable[[], Any]) -> Any:
    try:
        return body()
    except Exception as error:
        return condition(error)


def spark_answer(
    label: str, idle_s: list[float], interval: float = SPARK_CACHE_INTERVAL_S
) -> list[Any]:
    known = SPARK_EXTERNAL[label]
    expired = False
    found = []
    for index in range(len(known["stale"])):
        gap = known["decisive"].get(index)
        if gap is not None and gap < len(idle_s) and idle_s[gap] > interval:
            expired = True
        found.append(known["expired" if expired else "stale"][index])
    return found


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


def _session_write(session: Any, statement: str) -> None:
    session.sql(statement).collect()


def session_writer(session: Any) -> Callable[[str], None]:
    return partial(_session_write, session)


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
        writer: Callable[[str], None],
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

    def control(self, found: list[Any], access: _Accesses) -> None:
        t = self.make("control")
        held = access(partial(self.s.table, t))
        found.append(access(held.count))
        self.writer(f"INSERT INTO {t} VALUES (9, 'z')")
        found.append(access(held.count))
        found.append(access(partial(rows, held)))
        found.append(access(lambda: self.s.table(t).count()))
        self.sql(f"INSERT INTO {t} VALUES (8, 'q')")
        found.extend([held.count(), self.s.table(t).count()])
        self.s.catalog.refreshTable(t)
        found.extend([held.count(), self.s.table(t).count()])

    def refresh(self, found: list[Any], access: _Accesses) -> None:
        t = self.make("refresh")
        held = access(partial(self.s.table, t))
        self.writer(f"INSERT INTO {t} VALUES (9, 'z')")
        found.append(access(held.count))
        found.append(access(lambda: self.s.table(t).count()))
        self.sql(f"REFRESH TABLE {t}")
        found.extend([held.count(), self.s.table(t).count()])

    def external(self, label: str) -> dict[str, Any]:
        found: list[Any] = []
        access = _Accesses(self, label)
        body = self.control if label == "control" else self.refresh
        try:
            body(found, access)
        except Exception as error:
            found.append(condition(error))
        return {"answer": found, "idle_s": access.idle()}

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
