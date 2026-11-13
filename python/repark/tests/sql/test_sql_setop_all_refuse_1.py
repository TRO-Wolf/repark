from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark._native import PyReparkSession
from repark.spark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

LEFT = (1, 1, 1, 2, 2, 4)
RIGHT = (1, 2, 2, 3)
SPARK_ROWS = {
    "INTERSECT ALL": [1, 2, 2],
    "EXCEPT ALL": [1, 1, 4],
    "MINUS ALL": [1, 1, 4],
    "UNION ALL": [1, 1, 1, 1, 2, 2, 2, 2, 3, 4],
    "UNION": [1, 2, 3, 4],
    "INTERSECT": [1, 2],
    "EXCEPT": [4],
    "EXCEPT DISTINCT": [4],
    "INTERSECT DISTINCT": [1, 2],
    "MINUS": [4],
}
REFUSED_OPS = ("INTERSECT ALL", "EXCEPT ALL", "MINUS ALL")
CONTROL_OPS = (
    "UNION ALL",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "EXCEPT DISTINCT",
    "INTERSECT DISTINCT",
    "MINUS",
)
SPELLINGS = {
    "intersect all": "INTERSECT ALL",
    "Intersect All": "INTERSECT ALL",
    "except all": "EXCEPT ALL",
    "ExCePt AlL": "EXCEPT ALL",
    "minus all": "MINUS ALL",
    "MiNuS aLl": "MINUS ALL",
    "INTERSECT /* gap */ ALL": "INTERSECT ALL",
    "EXCEPT\n  ALL": "EXCEPT ALL",
}
SHAPES = (
    "top",
    "cte",
    "scalar",
    "in",
    "temp-view",
    "persistent-view",
    "iceberg-source",
    "insert",
    "ctas",
    "merge",
    "union-nested",
    "explain",
)
CONTROL_SHAPES = ("top", "cte", "insert", "ctas")
DOORS = ("spark", "native")
DIVERGES_ON_MAIN_OUTSIDE_THIS_UNIT = (
    {("spark", "MINUS", shape) for shape in CONTROL_SHAPES}
    | {("native", "MINUS", shape) for shape in CONTROL_SHAPES}
    | {("native", op, "ctas") for op in CONTROL_OPS}
)
DATAFRAME_REFUSALS = {
    "df-intersectAll": "DataFrame.intersectAll multiset semantics are not Spark-correct on this "
    "engine yet; use intersect() for distinct bags (octo C1-L-005)",
    "df-exceptAll": "DataFrame.exceptAll multiset semantics are not Spark-correct on this "
    "engine yet; use subtract() for distinct bags (octo C1-L-006)",
}
FRAGMENT_CELLS = {
    "fexpr-scalar": "INTERSECT ALL",
    "fexpr-filter-in": "EXCEPT ALL",
    "fexpr-filter-col": "EXCEPT ALL",
    "selectexpr-scalar": "INTERSECT ALL",
}
TEXT_CONTROLS = {
    "string-literal": ("SELECT 'a INTERSECT ALL b' AS s", [("a INTERSECT ALL b",)]),
    "block-comment": ("SELECT 1 AS a /* EXCEPT ALL */", [(1,)]),
    "line-comment": ("SELECT 1 AS a -- MINUS ALL\n", [(1,)]),
    "all-prefix-ident": ("SELECT a AS all_rows FROM l INTERSECT SELECT a FROM r", [(1,), (2,)]),
}


def refusal_text(op: str) -> str:
    distinct = "INTERSECT" if op == "INTERSECT ALL" else "EXCEPT"
    return (
        f"SQL {op} multiset semantics are not Spark-correct on this engine yet; use {distinct} "
        "for distinct bags, or number each side's duplicates with row_number() and "
        f"{distinct} the numbered rows (SQL-SETOP-ALL-MULTISET-1)"
    )


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def table_state(root: Path, table: str) -> tuple[bool, int | None]:
    files = sorted((root / "ns" / table / "metadata").glob("*.metadata.json"), key=metadata_order)
    if not files:
        return ((root / "ns" / table).exists(), None)
    document = json.loads(files[-1].read_text(encoding="utf-8"))
    return (True, len(document.get("snapshots") or []))


def outcome(run: Callable[[], list[tuple[Any, ...]]]) -> dict[str, Any]:
    try:
        rows = run()
    except Exception as error:
        return {"kind": "refused", "class": type(error).__name__, "text": str(error)}
    return {"kind": "rows", "rows": sorted(rows)}


class Door:
    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = root
        self.facade: ReparkSession | None = None

    def run(self, sql: str) -> list[tuple[Any, ...]]:
        if self.facade is None:
            return [tuple(row.values()) for row in repark.sql(sql).to_arrow().to_pylist()]
        return [tuple(row.values()) for row in self.facade.sql(sql).toArrow().to_pylist()]

    def values(self, items: tuple[int, ...]) -> str:
        body = ", ".join(f"({item})" for item in items)
        if self.facade is None:
            return f"SELECT * FROM (VALUES {body}) AS t(a)"
        return f"SELECT * FROM VALUES {body} AS t(a)"

    def using(self) -> str:
        return "" if self.facade is None else " USING iceberg"

    def temp_view(self) -> str:
        return "VIEW" if self.facade is None else "TEMPORARY VIEW"

    def seed(self) -> None:
        self.run(f"CREATE OR REPLACE {self.temp_view()} l AS {self.values(LEFT)}")
        self.run(f"CREATE OR REPLACE {self.temp_view()} r AS {self.values(RIGHT)}")
        for name, items in (("pl", LEFT), ("pr", RIGHT)):
            self.run(f"CREATE TABLE ice.ns.{name} (a INT){self.using()}")
            body = ", ".join(f"({item})" for item in items)
            self.run(f"INSERT INTO ice.ns.{name} VALUES {body}")

    def stored(self, table: str) -> list[int]:
        return sorted(row[0] for row in self.run(f"SELECT a FROM ice.ns.{table}"))


def query(op: str) -> str:
    return f"SELECT a FROM l {op} SELECT a FROM r"


def table_query(op: str) -> str:
    return f"SELECT a FROM ice.ns.pl {op} SELECT a FROM ice.ns.pr"


def write_cell(door: Door, table: str, statement: str, create: bool) -> dict[str, Any]:
    if create:
        door.run(f"CREATE TABLE ice.ns.{table} (a INT){door.using()}")
    before = table_state(door.root, table)
    result = outcome(lambda: door.run(statement))
    result["before"] = before
    result["after"] = table_state(door.root, table)
    if result["after"][0]:
        result["stored"] = door.stored(table)
    return result


def shape_cell(door: Door, shape: str, op: str, slot: int) -> dict[str, Any]:
    q = query(op)
    if shape == "top":
        return outcome(lambda: door.run(q))
    if shape == "cte":
        return outcome(lambda: door.run(f"WITH x AS ({q}) SELECT a FROM x"))
    if shape == "scalar":
        return outcome(lambda: door.run(f"SELECT (SELECT count(*) FROM ({q}) x) AS c"))
    if shape == "in":
        return outcome(lambda: door.run(f"SELECT a FROM r WHERE a IN ({q})"))
    if shape == "temp-view":
        return outcome(lambda: door.run(f"CREATE OR REPLACE {door.temp_view()} vt_{slot} AS {q}"))
    if shape == "persistent-view":
        return outcome(lambda: door.run(f"CREATE VIEW ice.ns.vp_{slot} AS {table_query(op)}"))
    if shape == "iceberg-source":
        return outcome(lambda: door.run(table_query(op)))
    if shape == "insert":
        return write_cell(door, f"t_{slot}", f"INSERT INTO ice.ns.t_{slot} {q}", True)
    if shape == "ctas":
        statement = f"CREATE TABLE ice.ns.c_{slot}{door.using()} AS {q}"
        return write_cell(door, f"c_{slot}", statement, False)
    if shape == "merge":
        statement = (
            f"MERGE INTO ice.ns.m_{slot} t USING ({q}) s ON t.a = s.a "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        return write_cell(door, f"m_{slot}", statement, True)
    if shape == "union-nested":
        return outcome(lambda: door.run(f"SELECT a FROM (SELECT 9 AS a UNION ALL ({q})) x"))
    return outcome(lambda: door.run(f"EXPLAIN {q}"))


def numbered(side: str) -> str:
    return f"SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) AS n FROM {side}"


WORKAROUNDS = {
    "numbered-intersect": (
        f"SELECT a FROM ({numbered('l')} INTERSECT {numbered('r')}) x",
        "INTERSECT ALL",
    ),
    "numbered-except": (f"SELECT a FROM ({numbered('l')} EXCEPT {numbered('r')}) x", "EXCEPT ALL"),
}


def dataframe_cells(session: ReparkSession) -> dict[str, dict[str, Any]]:
    left = session.sql("SELECT a FROM l")
    right = session.sql("SELECT a FROM r")
    inline_left = "SELECT a FROM VALUES (1), (1), (1), (2), (2), (4) AS t(a)"
    inline_right = "SELECT a FROM VALUES (1), (2), (2), (3) AS t(a)"
    fragment = f"(SELECT count(*) FROM ({inline_left} INTERSECT ALL {inline_right}) x)"
    in_filter = f"a IN ({inline_left} EXCEPT ALL {inline_right})"
    control = f"(SELECT count(*) FROM ({inline_left} INTERSECT {inline_right}) x)"
    one = session.sql("SELECT 1 AS one")

    def collect(frame: Any) -> list[tuple[Any, ...]]:
        return [tuple(row) for row in frame.collect()]

    return {
        "df-intersectAll": outcome(lambda: collect(left.intersectAll(right))),
        "df-exceptAll": outcome(lambda: collect(left.exceptAll(right))),
        "df-intersect": outcome(lambda: collect(left.intersect(right))),
        "df-subtract": outcome(lambda: collect(left.subtract(right))),
        "fexpr-scalar": outcome(lambda: collect(one.select(sf.expr(fragment)))),
        "fexpr-filter-in": outcome(lambda: collect(right.filter(in_filter))),
        "fexpr-filter-col": outcome(lambda: collect(right.filter(sf.expr(in_filter)))),
        "selectexpr-scalar": outcome(lambda: collect(right.selectExpr(fragment))),
        "fexpr-control": outcome(lambda: collect(one.select(sf.expr(control)))),
    }


def stored_view_cell(door: Door) -> dict[str, Any]:
    door.run(f"CREATE VIEW ice.ns.foreign_v AS {table_query('INTERSECT')}")
    plain = outcome(lambda: door.run("SELECT a FROM ice.ns.foreign_v"))
    for path in (door.root / "ns" / "foreign_v" / "metadata").glob("*.metadata.json"):
        text = path.read_text(encoding="utf-8")
        path.write_text(text.replace(" INTERSECT SELECT", " INTERSECT ALL SELECT"), "utf-8")
    after = outcome(lambda: door.run("SELECT a FROM ice.ns.foreign_v"))
    return {"plain": plain, **after}


def measure(door: Door) -> dict[str, dict[str, Any]]:
    door.seed()
    cells: dict[str, dict[str, Any]] = {}
    slot = 0
    for op in REFUSED_OPS:
        for shape in SHAPES:
            slot += 1
            cells[f"refuse|{op}|{shape}"] = shape_cell(door, shape, op, slot)
    for spelled in SPELLINGS:
        for shape in ("top", "insert"):
            slot += 1
            cells[f"spelling|{spelled}|{shape}"] = shape_cell(door, shape, spelled, slot)
    for op in CONTROL_OPS:
        for shape in CONTROL_SHAPES:
            slot += 1
            cells[f"control|{op}|{shape}"] = shape_cell(door, shape, op, slot)
    for name, (sql, _) in TEXT_CONTROLS.items():
        cells[f"text|{name}"] = outcome(lambda sql=sql: door.run(sql))
    for name, (sql, _) in WORKAROUNDS.items():
        cells[f"workaround|{name}"] = outcome(lambda sql=sql: door.run(sql))
    if door.facade is not None:
        for name, result in dataframe_cells(door.facade).items():
            cells[f"dataframe|{name}"] = result
        cells["stored-view"] = stored_view_cell(door)
    return cells


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    _reset_active_session_for_tests()
    spark_root = tmp_path_factory.mktemp("spark-door")
    native_root = tmp_path_factory.mktemp("native-door")
    session = ReparkSession.builder.appName("sql-setop-all-refuse-1").getOrCreate()
    session.register_memory_catalog("ice", str(spark_root))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    spark_door = Door("spark", spark_root)
    spark_door.facade = session
    with pytest.MonkeyPatch.context() as patch:
        native = PyReparkSession.native()
        native.register_memory_catalog("ice", str(native_root))
        native.create_namespace("ice", "ns", str(native_root / "ns"))
        patch.setattr(repark, "_ANSI_NATIVE", native)
        results = {"spark": measure(spark_door), "native": measure(Door("native", native_root))}
        yield results
    session.stop()
    _reset_active_session_for_tests()


def assert_refused(cell: dict[str, Any], op: str) -> None:
    assert cell["kind"] == "refused", cell
    assert cell["class"] == "UnsupportedOperationException", cell
    assert cell["text"] == refusal_text(op), cell
    if "before" in cell:
        assert cell["after"] == cell["before"], cell
        if cell["before"][0]:
            assert cell["stored"] == [], cell


def refused_params() -> list[pytest.ParamSet]:
    return [
        pytest.param(door, op, shape, id=f"{door}-{op.replace(' ', '-')}-{shape}")
        for door in DOORS
        for op in REFUSED_OPS
        for shape in SHAPES
    ]


@pytest.mark.parametrize(("door", "op", "shape"), refused_params())
def test_every_shape_refuses_before_any_write(
    measured: dict[str, Any], door: str, op: str, shape: str
) -> None:
    assert_refused(measured[door][f"refuse|{op}|{shape}"], op)


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("shape", ["top", "insert"])
@pytest.mark.parametrize("spelled", list(SPELLINGS), ids=lambda text: text.replace("\n", "nl"))
def test_every_spelling_refuses(
    measured: dict[str, Any], door: str, shape: str, spelled: str
) -> None:
    assert_refused(measured[door][f"spelling|{spelled}|{shape}"], SPELLINGS[spelled])


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("shape", CONTROL_SHAPES)
@pytest.mark.parametrize("op", CONTROL_OPS)
def test_controls_keep_answering(measured: dict[str, Any], door: str, shape: str, op: str) -> None:
    cell = measured[door][f"control|{op}|{shape}"]
    if cell["kind"] == "refused":
        assert "SQL-SETOP-ALL-MULTISET-1" not in cell["text"], cell
    if (door, op, shape) in DIVERGES_ON_MAIN_OUTSIDE_THIS_UNIT:
        return
    assert cell["kind"] == "rows", cell
    if "stored" in cell:
        assert cell["stored"] == SPARK_ROWS[op], cell
        assert cell["after"][1] == (cell["before"][1] or 0) + 1, cell
    else:
        assert [row[0] for row in cell["rows"]] == SPARK_ROWS[op], cell


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("name", list(TEXT_CONTROLS))
def test_text_that_only_mentions_the_operator_answers(
    measured: dict[str, Any], door: str, name: str
) -> None:
    assert measured[door][f"text|{name}"] == {"kind": "rows", "rows": TEXT_CONTROLS[name][1]}


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("name", list(WORKAROUNDS))
def test_the_hinted_workaround_answers_spark_rows(
    measured: dict[str, Any], door: str, name: str
) -> None:
    cell = measured[door][f"workaround|{name}"]
    assert cell["kind"] == "rows", cell
    assert [row[0] for row in cell["rows"]] == SPARK_ROWS[WORKAROUNDS[name][1]]


@pytest.mark.parametrize("name", list(DATAFRAME_REFUSALS))
def test_the_dataframe_refusal_is_unchanged(measured: dict[str, Any], name: str) -> None:
    cell = measured["spark"][f"dataframe|{name}"]
    assert cell == {
        "kind": "refused",
        "class": "UnsupportedOperationException",
        "text": DATAFRAME_REFUSALS[name],
    }


@pytest.mark.parametrize("name", list(FRAGMENT_CELLS))
def test_sql_fragments_refuse_the_same_way(measured: dict[str, Any], name: str) -> None:
    assert_refused(measured["spark"][f"dataframe|{name}"], FRAGMENT_CELLS[name])


def test_dataframe_and_fragment_controls_answer_spark_rows(measured: dict[str, Any]) -> None:
    cells = measured["spark"]
    assert cells["dataframe|df-intersect"] == {"kind": "rows", "rows": [(1,), (2,)]}
    assert cells["dataframe|df-subtract"] == {"kind": "rows", "rows": [(4,)]}
    assert cells["dataframe|fexpr-control"] == {"kind": "rows", "rows": [(2,)]}


def test_a_stored_view_body_naming_the_operator_refuses_at_read(measured: dict[str, Any]) -> None:
    cell = measured["spark"]["stored-view"]
    assert cell["plain"] == {"kind": "rows", "rows": [(1,), (2,)]}
    assert_refused(cell, "INTERSECT ALL")
