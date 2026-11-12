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
from repark.spark.session import _reset_active_session_for_tests

CARD = "NATIVE-CTAS-ICEBERG-REFUSE-1"
CATALOGS = ("ice", "hd")
SHAPES = {
    "literal": ("SELECT 1 AS id", "id INT", [(1,)]),
    "plain": ("SELECT id FROM {c}.ns.src", "id INT", [(1,), (1,), (2,), (3,)]),
    "union": (
        "SELECT id FROM {c}.ns.src UNION SELECT id FROM {c}.ns.src2",
        "id INT",
        [(1,), (2,), (3,), (4,)],
    ),
    "union_all": (
        "SELECT id FROM {c}.ns.src UNION ALL SELECT id FROM {c}.ns.src2",
        "id INT",
        [(1,), (1,), (2,), (2,), (3,), (3,), (4,)],
    ),
    "distinct": ("SELECT DISTINCT id FROM {c}.ns.src", "id INT", [(1,), (2,), (3,)]),
    "intersect": (
        "SELECT id FROM {c}.ns.src INTERSECT SELECT id FROM {c}.ns.src2",
        "id INT",
        [(2,), (3,)],
    ),
    "except": ("SELECT id FROM {c}.ns.src EXCEPT SELECT id FROM {c}.ns.src2", "id INT", [(1,)]),
    "values": ("VALUES (1), (2)", "id INT", [(1,), (2,)]),
    "aggregate": (
        "SELECT id, count(*) AS n FROM {c}.ns.src GROUP BY id",
        "id INT, n BIGINT",
        [(1, 2), (2, 1), (3, 1)],
    ),
    "join": (
        "SELECT a.id FROM {c}.ns.src a JOIN {c}.ns.src2 b ON a.id = b.id",
        "id INT",
        [(2,), (3,)],
    ),
}
FORM_SHAPES = ("literal", "union", "distinct")
REFUSED_FORMS = {
    "ctas_lower": "create table {t} as {q}",
    "ctas_mixed": "Create Table {t} As {q}",
    "if_not_exists": "CREATE TABLE IF NOT EXISTS {t} AS {q}",
    "or_replace": "CREATE OR REPLACE TABLE {t} AS {q}",
    "columns": "CREATE TABLE {t} ({cols}) AS {q}",
    "explain": "EXPLAIN CREATE TABLE {t} AS {q}",
    "explain_analyze": "EXPLAIN ANALYZE CREATE TABLE {t} AS {q}",
    "prepare": "PREPARE {p} AS CREATE TABLE {t} AS {q}",
}
EARLIER_REFUSALS = {
    "replace": (
        "REPLACE TABLE {t} AS SELECT 1 AS id",
        "ParseException",
        'SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, '
        'found: AS at Line: 1, Column: {col}")',
    ),
    "temporary": (
        "CREATE TEMPORARY TABLE {t} AS SELECT 1 AS id",
        "UnsupportedOperationException",
        "This feature is not implemented: Temporary tables not supported",
    ),
    "multi_statement": (
        "CREATE TABLE {t} AS SELECT 1 AS id; SELECT 1",
        "UnsupportedOperationException",
        "This feature is not implemented: The context currently only supports a single SQL "
        "statement",
    ),
}
EXISTING = {
    "or_replace_failing": (
        "CREATE OR REPLACE TABLE {t} AS SELECT 10 / (id - id) AS id FROM {c}.ns.src"
    ),
    "or_replace_succeeding": "CREATE OR REPLACE TABLE {t} AS SELECT id FROM {c}.ns.src",
    "if_not_exists": "CREATE TABLE IF NOT EXISTS {t} AS SELECT id FROM {c}.ns.src",
    "ctas": "CREATE TABLE {t} AS SELECT id FROM {c}.ns.src",
}
MEMORY_FORMS = {
    "ctas": "CREATE TABLE {t} AS {q}",
    "if_not_exists": "CREATE TABLE IF NOT EXISTS {t} AS {q}",
    "or_replace": "CREATE OR REPLACE TABLE {t} AS {q}",
    "columns": "CREATE TABLE {t} ({cols}) AS {q}",
}
SPARK_DOOR_FORMS = {
    "ctas": "CREATE TABLE {t} AS {q}",
    "if_not_exists": "CREATE TABLE IF NOT EXISTS {t} AS {q}",
    "or_replace": "CREATE OR REPLACE TABLE {t} AS {q}",
}
ICEBERG_VIEW_REFUSAL = "Unexpected => register_table does not support tables with data."


def refusal_text(target: str) -> str:
    return (
        "repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table "
        f"{target}: create the table with its columns, CREATE TABLE {target} (<columns>), then "
        f"INSERT INTO {target} SELECT ...; or run the same statement through spark.sql ({CARD})"
    )


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def outcome(run: Callable[[], list[tuple[Any, ...]]]) -> dict[str, Any]:
    try:
        rows = run()
    except Exception as error:
        return {"kind": "refused", "class": type(error).__name__, "text": str(error)}
    return {"kind": "rows", "rows": sorted(rows)}


class Door:
    def __init__(self, root: Path, facade: ReparkSession | None = None) -> None:
        self.root = root
        self.facade = facade
        self.slot = 0

    def name(self, prefix: str) -> str:
        self.slot += 1
        return f"{prefix}{self.slot}"

    def run(self, sql: str) -> list[tuple[Any, ...]]:
        if self.facade is None:
            return [tuple(row.values()) for row in repark.sql(sql).to_arrow().to_pylist()]
        return [tuple(row.values()) for row in self.facade.sql(sql).toArrow().to_pylist()]

    def seed(self, catalog: str) -> None:
        for table, items in (("src", (1, 1, 2, 3)), ("src2", (2, 3, 4))):
            self.run(f"CREATE TABLE {catalog}.ns.{table} (id INT)")
            body = ", ".join(f"({item})" for item in items)
            self.run(f"INSERT INTO {catalog}.ns.{table} VALUES {body}")

    def state(self, target: str) -> dict[str, Any]:
        read = outcome(lambda: self.run(f"SELECT * FROM {target}"))
        state: dict[str, Any] = {"rows": read.get("rows")}
        parts = target.lower().split(".")
        if len(parts) == 3 and parts[0] in CATALOGS:
            metadata = self.root / parts[0] / parts[1] / parts[2] / "metadata"
            files = sorted(metadata.glob("*.metadata.json"), key=metadata_order)
            state["metadata"] = [path.name for path in files]
            if files:
                document = json.loads(files[-1].read_text(encoding="utf-8"))
                state["snapshots"] = [
                    snap["snapshot-id"] for snap in document.get("snapshots") or []
                ]
        return state

    def cell(self, target: str, statements: list[str]) -> dict[str, Any]:
        before = self.state(target)
        results = [outcome(lambda sql=sql: self.run(sql)) for sql in statements]
        return {"results": results, "before": before, "after": self.state(target)}


def query(shape: str, catalog: str) -> tuple[str, str]:
    text, cols, _ = SHAPES[shape]
    return text.format(c=catalog), cols


def native_cells(door: Door) -> dict[str, dict[str, Any]]:
    cells: dict[str, dict[str, Any]] = {}
    for catalog in CATALOGS:
        for shape in SHAPES:
            target = f"{catalog}.ns.{door.name('t')}"
            text, _ = query(shape, catalog)
            cell = door.cell(target, [f"CREATE TABLE {target} AS {text}"])
            cells[f"refuse|{catalog}|ctas|{shape}"] = {"target": target, **cell}
        for form, template in REFUSED_FORMS.items():
            for shape in FORM_SHAPES:
                name = door.name("t")
                target = f"{catalog}.ns.{name}"
                text, cols = query(shape, catalog)
                statements = [template.format(t=target, q=text, cols=cols, p=f"p_{name}")]
                if form == "prepare":
                    statements.append(f"EXECUTE p_{name}")
                cell = door.cell(target, statements)
                cells[f"refuse|{catalog}|{form}|{shape}"] = {"target": target, **cell}
        for shape in FORM_SHAPES:
            name = door.name("t")
            target = f"{catalog.upper()}.Ns.{name}"
            text, _ = query(shape, catalog)
            cell = door.cell(target, [f"CREATE TABLE {target} AS {text}"])
            cells[f"refuse|{catalog}|mixed_case_target|{shape}"] = {
                "target": f"{catalog}.ns.{name}",
                **cell,
            }
        for form, (template, _, _) in EARLIER_REFUSALS.items():
            target = f"{catalog}.ns.{door.name('t')}"
            cells[f"earlier|{catalog}|{form}"] = {
                "target": target,
                **door.cell(target, [template.format(t=target)]),
            }
        for case, template in EXISTING.items():
            target = f"{catalog}.ns.{door.name('e')}"
            door.run(f"CREATE TABLE {target} (id INT)")
            door.run(f"INSERT INTO {target} VALUES (7), (8)")
            cell = door.cell(target, [template.format(t=target, c=catalog)])
            cells[f"existing|{catalog}|{case}"] = {"target": target, **cell}
        for shape in SHAPES:
            target = f"{catalog}.ns.{door.name('w')}"
            text, cols = query(shape, catalog)
            statements = [f"CREATE TABLE {target} ({cols})", f"INSERT INTO {target} {text}"]
            cells[f"workaround|{catalog}|{shape}"] = {
                "target": target,
                **door.cell(target, statements),
            }
        target = f"{catalog}.ns.{door.name('c')}"
        cells[f"control|{catalog}|create_no_query"] = {
            "target": target,
            **door.cell(target, [f"CREATE TABLE {target} (id INT)"]),
        }
        target = f"{catalog}.ns.{door.name('v')}"
        cells[f"control|{catalog}|create_view"] = {
            "target": target,
            **door.cell(
                target, [f"CREATE VIEW {target} AS SELECT DISTINCT id FROM {catalog}.ns.src"]
            ),
        }
    for prefix in ("", "datafusion.public."):
        for form, template in MEMORY_FORMS.items():
            shapes = SHAPES if form == "ctas" else FORM_SHAPES
            for shape in shapes:
                target = f"{prefix}{door.name('m')}"
                text, cols = query(shape, "ice")
                cell = door.cell(target, [template.format(t=target, q=text, cols=cols)])
                cells[f"memory|{prefix or 'bare'}|{form}|{shape}"] = {"target": target, **cell}
    target = door.name("mv")
    cells["control|memory|create_view"] = {
        "target": target,
        **door.cell(target, [f"CREATE VIEW {target} AS SELECT DISTINCT id FROM ice.ns.src"]),
    }
    return cells


def default_catalog_cells(root: Path) -> dict[str, dict[str, Any]]:
    session = PyReparkSession.native()
    session.register_memory_catalog("ice", str(root / "ice"))
    session.create_namespace("ice", "ns", str(root / "ice" / "ns"))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(repark, "_ANSI_NATIVE", session)
        door = Door(root)
        door.seed("ice")
        door.run("SET datafusion.catalog.default_catalog = 'ice'")
        door.run("SET datafusion.catalog.default_schema = 'ns'")
        cells = {}
        for shape in FORM_SHAPES:
            name = door.name("d")
            text, _ = query(shape, "ice")
            cell = door.cell(f"ice.ns.{name}", [f"CREATE TABLE {name} AS {text}"])
            cells[f"refuse|ice|default_catalog|{shape}"] = {"target": f"ice.ns.{name}", **cell}
        return cells


def spark_door_cells(door: Door) -> dict[str, dict[str, Any]]:
    cells: dict[str, dict[str, Any]] = {}
    for form, template in SPARK_DOOR_FORMS.items():
        shapes = SHAPES if form == "ctas" else FORM_SHAPES
        for shape in shapes:
            target = f"ice.ns.{door.name('s')}"
            text, cols = query(shape, "ice")
            cell = door.cell(target, [template.format(t=target, q=text, cols=cols)])
            cells[f"spark|{form}|{shape}"] = {"target": target, **cell}
    return cells


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, dict[str, Any]]]:
    _reset_active_session_for_tests()
    spark_root = tmp_path_factory.mktemp("spark-door")
    native_root = tmp_path_factory.mktemp("native-door")
    session = ReparkSession.builder.appName("native-ctas-iceberg-refuse-1").getOrCreate()
    session.register_memory_catalog("ice", str(spark_root / "ice"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    spark_door = Door(spark_root, session)
    spark_door.seed("ice")
    hadoop = {
        "spark.sql.catalog.hd": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hd.type": "hadoop",
        "spark.sql.catalog.hd.warehouse": str(native_root / "hd"),
    }
    with pytest.MonkeyPatch.context() as patch:
        native = PyReparkSession.native(config=hadoop)
        native.register_memory_catalog("ice", str(native_root / "ice"))
        native.create_namespace("ice", "ns", str(native_root / "ice" / "ns"))
        native.create_namespace("hd", "ns", None)
        patch.setattr(repark, "_ANSI_NATIVE", native)
        door = Door(native_root)
        for catalog in CATALOGS:
            door.seed(catalog)
        cells = native_cells(door)
        cells["insert|ice|union"] = door.cell(
            "ice.ns.c_insert",
            [
                "CREATE TABLE ice.ns.c_insert (id INT)",
                "INSERT INTO ice.ns.c_insert SELECT id FROM ice.ns.src UNION SELECT id FROM "
                "ice.ns.src2",
            ],
        )
    cells |= default_catalog_cells(tmp_path_factory.mktemp("default-catalog"))
    cells |= spark_door_cells(spark_door)
    yield cells
    session.stop()
    _reset_active_session_for_tests()


def assert_refused_and_absent(cell: dict[str, Any]) -> None:
    first = cell["results"][0]
    assert first == {
        "kind": "refused",
        "class": "UnsupportedOperationException",
        "text": refusal_text(cell["target"]),
    }, cell
    assert cell["after"]["rows"] is None, cell
    assert cell["after"].get("metadata", []) == [], cell


def refused_keys() -> list[pytest.ParamSet]:
    keys = [f"{catalog}|ctas|{shape}" for catalog in CATALOGS for shape in SHAPES]
    keys += [
        f"{catalog}|{form}|{shape}"
        for catalog in CATALOGS
        for form in (*REFUSED_FORMS, "mixed_case_target")
        for shape in FORM_SHAPES
    ]
    keys += [f"ice|default_catalog|{shape}" for shape in FORM_SHAPES]
    return [pytest.param(key, id=key.replace("|", "-")) for key in keys]


@pytest.mark.parametrize("key", refused_keys())
def test_native_ctas_into_an_iceberg_table_refuses_and_creates_nothing(
    measured: dict[str, dict[str, Any]], key: str
) -> None:
    cell = measured[f"refuse|{key}"]
    assert_refused_and_absent(cell)
    if "|prepare|" in key:
        name = cell["target"].rsplit(".", 1)[1]
        assert cell["results"][1]["kind"] == "refused", cell
        assert cell["results"][1]["text"] == (
            f"datafusion engine error: Execution error: Prepared statement 'p_{name}' does not "
            "exist"
        ), cell


@pytest.mark.parametrize("catalog", CATALOGS)
@pytest.mark.parametrize("form", list(EARLIER_REFUSALS))
def test_forms_the_parser_or_planner_refuses_first_keep_their_refusal(
    measured: dict[str, dict[str, Any]], catalog: str, form: str
) -> None:
    cell = measured[f"earlier|{catalog}|{form}"]
    _, error_class, text = EARLIER_REFUSALS[form]
    column = len(f"REPLACE TABLE {cell['target']} ") + 1
    assert cell["results"][0] == {
        "kind": "refused",
        "class": error_class,
        "text": text.format(col=column),
    }, cell
    assert cell["after"]["rows"] is None, cell
    assert cell["after"]["metadata"] == [], cell


@pytest.mark.parametrize("catalog", CATALOGS)
@pytest.mark.parametrize("case", list(EXISTING))
def test_a_refused_ctas_leaves_an_existing_table_untouched(
    measured: dict[str, dict[str, Any]], catalog: str, case: str
) -> None:
    cell = measured[f"existing|{catalog}|{case}"]
    assert cell["results"][0] == {
        "kind": "refused",
        "class": "UnsupportedOperationException",
        "text": refusal_text(cell["target"]),
    }, cell
    assert cell["before"]["rows"] == [(7,), (8,)], cell
    assert cell["after"] == cell["before"], cell


@pytest.mark.parametrize("catalog", CATALOGS)
@pytest.mark.parametrize("shape", list(SHAPES))
def test_the_hinted_workaround_stores_spark_rows(
    measured: dict[str, dict[str, Any]], catalog: str, shape: str
) -> None:
    cell = measured[f"workaround|{catalog}|{shape}"]
    assert [result["kind"] for result in cell["results"]] == ["rows", "rows"], cell
    assert cell["after"]["rows"] == SHAPES[shape][2], cell
    assert len(cell["after"]["snapshots"]) == 1, cell


def memory_keys() -> list[pytest.ParamSet]:
    keys = [
        f"{prefix}|{form}|{shape}"
        for prefix in ("bare", "datafusion.public.")
        for form in MEMORY_FORMS
        for shape in (SHAPES if form == "ctas" else FORM_SHAPES)
    ]
    return [pytest.param(key, id=key.replace("|", "-")) for key in keys]


@pytest.mark.parametrize("key", memory_keys())
def test_native_ctas_into_the_in_memory_catalog_still_stores_the_rows(
    measured: dict[str, dict[str, Any]], key: str
) -> None:
    cell = measured[f"memory|{key}"]
    shape = key.rsplit("|", 1)[1]
    assert cell["results"] == [{"kind": "rows", "rows": []}], cell
    assert cell["after"]["rows"] == SHAPES[shape][2], cell


def spark_door_keys() -> list[pytest.ParamSet]:
    keys = [
        f"{form}|{shape}"
        for form in SPARK_DOOR_FORMS
        for shape in (SHAPES if form == "ctas" else FORM_SHAPES)
    ]
    return [pytest.param(key, id=key.replace("|", "-")) for key in keys]


@pytest.mark.parametrize("key", spark_door_keys())
def test_the_spark_door_runs_the_same_ctas_with_spark_rows(
    measured: dict[str, dict[str, Any]], key: str
) -> None:
    cell = measured[f"spark|{key}"]
    shape = key.rsplit("|", 1)[1]
    assert cell["results"][0]["kind"] == "rows", cell
    assert cell["after"]["rows"] == SHAPES[shape][2], cell
    assert len(cell["after"]["snapshots"]) == 1, cell


@pytest.mark.parametrize("catalog", CATALOGS)
def test_a_create_table_without_a_query_still_creates_the_iceberg_table(
    measured: dict[str, dict[str, Any]], catalog: str
) -> None:
    cell = measured[f"control|{catalog}|create_no_query"]
    assert cell["results"] == [{"kind": "rows", "rows": []}], cell
    assert cell["after"]["rows"] == [], cell
    assert len(cell["after"]["metadata"]) == 1, cell


@pytest.mark.parametrize("catalog", CATALOGS)
def test_create_view_keeps_its_own_answer(
    measured: dict[str, dict[str, Any]], catalog: str
) -> None:
    cell = measured[f"control|{catalog}|create_view"]
    assert cell["results"] == [
        {"kind": "refused", "class": "PySparkException", "text": ICEBERG_VIEW_REFUSAL}
    ], cell
    assert cell["after"]["rows"] is None, cell
    memory = measured["control|memory|create_view"]
    assert memory["results"] == [{"kind": "rows", "rows": []}], memory
    assert memory["after"]["rows"] == [(1,), (2,), (3,)], memory


def test_native_insert_select_into_an_iceberg_table_still_writes(
    measured: dict[str, dict[str, Any]],
) -> None:
    cell = measured["insert|ice|union"]
    assert [result["kind"] for result in cell["results"]] == ["rows", "rows"], cell
    assert cell["after"]["rows"] == [(1,), (2,), (3,), (4,)], cell
