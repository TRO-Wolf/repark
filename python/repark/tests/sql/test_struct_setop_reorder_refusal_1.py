"""STRUCT-SETOP-REORDER-REFUSAL-1: set operations refuse reordered struct operands."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.session import _reset_active_session_for_tests

A1B2 = "named_struct('a', 1, 'b', 2)"
A3B4 = "named_struct('a', 3, 'b', 4)"
STRUCT2 = "STRUCT<a: INT, b: INT>"
UNION_12_34 = ("answer", "[1,2]|[3,4]", "", "[1,2]")

SHAPES = [
    {
        "id": "same-order",
        "left": A1B2,
        "right_union": A3B4,
        "right_set": A1B2,
        "ddl": STRUCT2,
        "expect": ("answer", "[1,2]|[3,4]", "[1,2]", ""),
    },
    {
        "id": "reordered",
        "left": A1B2,
        "right_union": "named_struct('b', 3, 'a', 4)",
        "right_set": "named_struct('b', 2, 'a', 1)",
        "ddl": STRUCT2,
        "expect": ("refuse", "s", "b, a", "a, b"),
    },
    {
        "id": "reordered-typed",
        "left": "named_struct('a', 1, 'b', 'x')",
        "right_union": "named_struct('b', 'y', 'a', 2)",
        "right_set": "named_struct('b', 'x', 'a', 1)",
        "ddl": "STRUCT<a: INT, b: STRING>",
        "expect": ("refuse", "s", "b, a", "a, b"),
    },
    {
        "id": "disjoint-names",
        "left": A1B2,
        "right_union": "named_struct('c', 3, 'd', 4)",
        "right_set": "named_struct('c', 1, 'd', 2)",
        "ddl": STRUCT2,
        "expect": ("refuse", "s", "c, d", "a, b"),
    },
    {
        "id": "partial-overlap",
        "left": A1B2,
        "right_union": "named_struct('a', 3, 'c', 4)",
        "right_set": "named_struct('a', 1, 'c', 2)",
        "ddl": STRUCT2,
        "expect": ("refuse", "s", "a, c", "a, b"),
    },
    {
        "id": "case-reordered",
        "left": A1B2,
        "right_union": "named_struct('B', 3, 'A', 4)",
        "right_set": "named_struct('B', 2, 'A', 1)",
        "ddl": STRUCT2,
        "expect": ("refuse", "s", "B, A", "a, b"),
    },
    {
        "id": "nested-reordered",
        "left": "named_struct('x', named_struct('a', 1, 'b', 2))",
        "right_union": "named_struct('x', named_struct('b', 3, 'a', 4))",
        "right_set": "named_struct('x', named_struct('b', 2, 'a', 1))",
        "ddl": "STRUCT<x: STRUCT<a: INT, b: INT>>",
        "expect": ("refuse", "s.x", "b, a", "a, b"),
    },
    {
        "id": "array-reordered",
        "left": "array(named_struct('a', 1, 'b', 2))",
        "right_union": "array(named_struct('b', 3, 'a', 4))",
        "right_set": "array(named_struct('b', 2, 'a', 1))",
        "ddl": "ARRAY<STRUCT<a: INT, b: INT>>",
        "expect": ("refuse", "s[]", "b, a", "a, b"),
    },
    {
        "id": "map-value-reordered",
        "left": "map('k', named_struct('a', 1, 'b', 2))",
        "right_union": "map('k', named_struct('b', 3, 'a', 4))",
        "right_set": "map('k', named_struct('b', 2, 'a', 1))",
        "ddl": "MAP<STRING, STRUCT<a: INT, b: INT>>",
        "expect": ("map-refuse", "s{value}", "b, a", "a, b"),
    },
    {
        "id": "three-way",
        "left": "named_struct('a', 1, 'b', 2, 'c', 3)",
        "right_union": "named_struct('c', 4, 'a', 5, 'b', 6)",
        "right_set": "named_struct('c', 3, 'a', 1, 'b', 2)",
        "ddl": "STRUCT<a: INT, b: INT, c: INT>",
        "expect": ("refuse", "s", "c, a, b", "a, b, c"),
    },
    {
        "id": "ctl-same-order",
        "left": A1B2,
        "right_union": A3B4,
        "right_set": A3B4,
        "ddl": STRUCT2,
        "expect": UNION_12_34,
    },
    {
        "id": "ctl-reordered",
        "left": A1B2,
        "right_union": A3B4,
        "right_set": A3B4,
        "ddl": STRUCT2,
        "expect": UNION_12_34,
    },
    {
        "id": "ctl-reordered-typed",
        "left": "named_struct('a', 1, 'b', 'x')",
        "right_union": "named_struct('a', 3, 'b', 'y')",
        "right_set": "named_struct('a', 3, 'b', 'y')",
        "ddl": "STRUCT<a: INT, b: STRING>",
        "expect": ("answer", '[1,"x"]|[3,"y"]', "", '[1,"x"]'),
    },
    {
        "id": "ctl-disjoint-names",
        "left": A1B2,
        "right_union": A3B4,
        "right_set": A3B4,
        "ddl": STRUCT2,
        "expect": UNION_12_34,
    },
    {
        "id": "ctl-partial-overlap",
        "left": A1B2,
        "right_union": A3B4,
        "right_set": A3B4,
        "ddl": STRUCT2,
        "expect": UNION_12_34,
    },
    {
        "id": "ctl-case-reordered",
        "left": A1B2,
        "right_union": A3B4,
        "right_set": A3B4,
        "ddl": STRUCT2,
        "expect": UNION_12_34,
    },
    {
        "id": "ctl-nested-reordered",
        "left": "named_struct('x', named_struct('a', 1, 'b', 2))",
        "right_union": "named_struct('x', named_struct('a', 3, 'b', 4))",
        "right_set": "named_struct('x', named_struct('a', 3, 'b', 4))",
        "ddl": "STRUCT<x: STRUCT<a: INT, b: INT>>",
        "expect": ("answer", "[[1,2]]|[[3,4]]", "", "[[1,2]]"),
    },
    {
        "id": "ctl-array-reordered",
        "left": "array(named_struct('a', 1, 'b', 2))",
        "right_union": "array(named_struct('a', 3, 'b', 4))",
        "right_set": "array(named_struct('a', 3, 'b', 4))",
        "ddl": "ARRAY<STRUCT<a: INT, b: INT>>",
        "expect": ("answer", "[[1,2]]|[[3,4]]", "", "[[1,2]]"),
    },
    {
        "id": "ctl-map-value-reordered",
        "left": "map('k', named_struct('a', 1, 'b', 2))",
        "right_union": "map('k', named_struct('a', 3, 'b', 4))",
        "right_set": "map('k', named_struct('a', 3, 'b', 4))",
        "ddl": "MAP<STRING, STRUCT<a: INT, b: INT>>",
        "expect": ("map-answer", '[["k",[1,2]]]|[["k",[3,4]]]', "", '[["k",[1,2]]]'),
    },
    {
        "id": "ctl-three-way",
        "left": "named_struct('a', 1, 'b', 2, 'c', 3)",
        "right_union": "named_struct('a', 3, 'b', 4, 'c', 5)",
        "right_set": "named_struct('a', 3, 'b', 4, 'c', 5)",
        "ddl": "STRUCT<a: INT, b: INT, c: INT>",
        "expect": ("answer", "[1,2,3]|[3,4,5]", "", "[1,2,3]"),
    },
]
BY_ID = {shape["id"]: shape for shape in SHAPES}

SQL_OPS = [
    "UNION ALL",
    "UNION",
    "UNION DISTINCT",
    "INTERSECT",
    "EXCEPT",
    "INTERSECT ALL",
    "EXCEPT ALL",
]
DF_OPS = ["union", "unionByName", "intersect", "subtract"]
INTERSECT_FAMILY = {"INTERSECT", "INTERSECT ALL", "intersect"}
EXCEPT_FAMILY = {"EXCEPT", "EXCEPT ALL", "subtract"}
UNION_WORD = {
    "UNION ALL": "UNION",
    "UNION": "UNION",
    "UNION DISTINCT": "UNION",
    "union": "UNION",
    "unionByName": "UNION",
}
OVERREFUSAL = {"reordered", "reordered-typed", "nested-reordered", "array-reordered", "three-way"}
UNMEASURED = {"INTERSECT ALL", "EXCEPT ALL", "intersect", "subtract"}


def op_slug(op: str) -> str:
    slug = op.replace("UNION DISTINCT", "union-distinct").replace(" ", "-")
    return slug.replace("ByName", "-by-name").lower()


def case_id(kind: str, op: str, shape_id: str, form: str) -> str:
    prefix = "df-" if kind == "df" else ""
    text = f"spark-{prefix}{op_slug(op)}-{shape_id}-{form}"
    if op == "unionByName" and shape_id in OVERREFUSAL:
        text += "-STRUCT-SETOP-UNIONBYNAME-OVERREFUSAL-1"
    if op in UNMEASURED:
        text += "-unmeasured-fail-closed"
    return text


def sql_cases() -> list[pytest.ParamSet]:
    return [
        pytest.param(op, shape["id"], form, id=case_id("sql", op, shape["id"], form))
        for op in SQL_OPS
        for shape in SHAPES
        for form in ("read", "insert")
    ]


def df_cases() -> list[pytest.ParamSet]:
    return [
        pytest.param(op, shape["id"], form, id=case_id("df", op, shape["id"], form))
        for op in DF_OPS
        for shape in SHAPES
        for form in ("read", "insert")
    ]


def render(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, dict):
        return "[" + ",".join(render(item) for item in value.values()) + "]"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(render(item) for item in value) + "]"
    return str(value)


def render_rows(pylist: list[dict[str, Any]]) -> str:
    return "|".join(sorted(render(row["s"]) for row in pylist))


def expected_rows(shape: dict[str, Any], op: str) -> str:
    _, union, intersect, excepted = shape["expect"]
    if op in INTERSECT_FAMILY:
        return intersect
    if op in EXCEPT_FAMILY:
        return excepted
    return union


def is_answer(shape: dict[str, Any], op: str) -> bool:
    kind = shape["expect"][0]
    if kind == "answer":
        return True
    if kind == "map-answer":
        return op in ("UNION ALL", "union", "unionByName", "INTERSECT ALL", "EXCEPT ALL")
    return False


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def snapshot_of(root: Path, table: str) -> tuple[int, int | None]:
    metadata = sorted(
        (root / "ns" / table / "metadata").glob("*.metadata.json"), key=metadata_order
    )
    document = json.loads(metadata[-1].read_text(encoding="utf-8"))
    return (len(document.get("snapshots") or []), document.get("current-snapshot-id"))


def op_word(op: str) -> str:
    if op in UNION_WORD:
        return "UNION"
    if op == "INTERSECT ALL":
        return "INTERSECT ALL"
    if op in INTERSECT_FAMILY:
        return "INTERSECT"
    if op == "EXCEPT ALL":
        return "EXCEPT ALL"
    return "EXCEPT"


def check_refusal_text(error: str, word: str, path: str, right: str, left: str) -> None:
    assert "[INCOMPATIBLE_COLUMN_TYPE]" in error, error
    assert f"{word} can only be performed" in error, error
    marker = "Struct field names differ at "
    assert marker in error, error
    rest = error.split(marker, 1)[1].split(";", 1)[0]
    found_path, _, found_lists = rest.partition(": ")
    assert found_path == path, error
    assert found_lists == f"[{right}] versus [{left}]", error


def check_refusal(shape: dict[str, Any], op: str, error: str) -> None:
    kind = shape["expect"][0]
    if kind == "map-answer":
        assert "Struct field names differ" not in error, error
        assert "Struct fields cannot be paired" not in error, error
    elif kind == "refuse" or op in ("UNION ALL", "union", "unionByName"):
        _, path, right, left = shape["expect"]
        check_refusal_text(error, op_word(op), path, right, left)


def table_name(kind: str, shape_id: str, op: str) -> str:
    slug = op_slug(op).replace("-", "_")
    return f"t_{kind}_{shape_id.replace('-', '_')}_{slug}_insert"


def create_table(spark: ReparkSession, table: str, ddl: str) -> None:
    spark.sql(
        f"CREATE TABLE ice.ns.{table} (s {ddl}) USING iceberg TBLPROPERTIES "
        "('format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
        "'write.update.mode' = 'copy-on-write')"
    ).collect()


def rows_of(spark: ReparkSession, table: str) -> str:
    return render_rows(spark.sql(f"SELECT s FROM ice.ns.{table}").toArrow().to_pylist())


def right_operand(shape: dict[str, Any], op: str) -> str:
    if op in INTERSECT_FAMILY or op in EXCEPT_FAMILY:
        return shape["right_set"]
    return shape["right_union"]


def run_sql(spark: ReparkSession, sql: str) -> str:
    return render_rows(spark.sql(sql).toArrow().to_pylist())


def combine(spark: ReparkSession, shape: dict[str, Any], op: str) -> Any:
    left = spark.sql(f"SELECT {shape['left']} AS s")
    right = spark.sql(f"SELECT {right_operand(shape, op)} AS s")
    if op == "union":
        return left.union(right)
    if op == "unionByName":
        return left.unionByName(right)
    if op == "intersect":
        return left.intersect(right)
    return left.subtract(right)


EXPR_FRAGMENT = (
    "(SELECT sum(x.s.a) FROM (SELECT named_struct('a', 1, 'b', 2) AS s "
    "UNION ALL SELECT named_struct('b', 3, 'a', 4) AS s) x)"
)
EXPR_INT_FRAGMENT = "(SELECT sum(x.v) FROM (SELECT 1 AS v UNION ALL SELECT 2 AS v) x)"
EXPR_STRUCT_FRAGMENT = (
    "(SELECT sum(x.s.a) FROM (SELECT named_struct('a', 1, 'b', 2) AS s "
    "UNION ALL SELECT named_struct('a', 3, 'b', 4) AS s) x)"
)
EXPR_CASES = ["f1-select", "f1-withcolumn", "f1-filter", "f1-control-int", "f1-control-struct"]


def run_expr_case(spark: ReparkSession, case: str) -> dict[str, Any]:
    base = spark.sql("SELECT 1 AS one")
    try:
        if case == "f1-select":
            rows = base.select(F.expr(EXPR_FRAGMENT).alias("total")).toArrow().to_pylist()
        elif case == "f1-withcolumn":
            rows = base.withColumn("total", F.expr(EXPR_FRAGMENT)).toArrow().to_pylist()
        elif case == "f1-filter":
            rows = base.filter(F.expr(f"{EXPR_FRAGMENT} > 0")).toArrow().to_pylist()
        elif case == "f1-control-int":
            rows = base.select(F.expr(EXPR_INT_FRAGMENT).alias("total")).toArrow().to_pylist()
        else:
            rows = base.select(F.expr(EXPR_STRUCT_FRAGMENT).alias("total")).toArrow().to_pylist()
    except Exception as error:
        return {"kind": "refused", "class": type(error).__name__, "text": str(error)}
    return {"kind": "rows", "rows": rows}


CASES = [
    ("sql", op, shape["id"], form)
    for op in SQL_OPS
    for shape in SHAPES
    for form in ("read", "insert")
] + [
    ("df", op, shape["id"], form)
    for op in DF_OPS
    for shape in SHAPES
    for form in ("read", "insert")
]


def run_sql_case(
    spark: ReparkSession, root: Path, op: str, shape: dict[str, Any], form: str
) -> dict[str, Any]:
    statement = f"SELECT {shape['left']} AS s {op} SELECT {right_operand(shape, op)} AS s"
    if form == "read":
        try:
            return {"kind": "rows", "rows": run_sql(spark, statement)}
        except Exception as error:
            return {"kind": "refused", "class": type(error).__name__, "text": str(error)}
    table = table_name("sql", shape["id"], op)
    create_table(spark, table, shape["ddl"])
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        spark.sql(f"INSERT INTO ice.ns.{table} {statement}").collect()
    except Exception as error:
        return {
            "kind": "refused",
            "class": type(error).__name__,
            "text": str(error),
            "rows": rows_of(spark, table),
            "snapshot": snapshot_of(root, table),
            "before": before,
        }
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def run_df_case(
    spark: ReparkSession, root: Path, op: str, shape: dict[str, Any], form: str
) -> dict[str, Any]:
    if form == "read":
        try:
            rows = render_rows(combine(spark, shape, op).toArrow().to_pylist())
        except Exception as error:
            return {"kind": "refused", "class": type(error).__name__, "text": str(error)}
        return {"kind": "rows", "rows": rows}
    table = table_name("df", shape["id"], op)
    create_table(spark, table, shape["ddl"])
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        combine(spark, shape, op).write.insertInto(f"ice.ns.{table}")
    except Exception as error:
        return {
            "kind": "refused",
            "class": type(error).__name__,
            "text": str(error),
            "rows": rows_of(spark, table),
            "snapshot": snapshot_of(root, table),
            "before": before,
        }
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, dict[str, Any]]]:
    _reset_active_session_for_tests()
    root = tmp_path_factory.mktemp("warehouse")
    spark = ReparkSession.builder.appName("struct-setop-reorder-refusal-1").getOrCreate()
    spark.register_memory_catalog("ice", str(root))
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    outcomes = {}
    for case in EXPR_CASES:
        outcomes[f"spark-{case}"] = run_expr_case(spark, case)
    for kind, op, shape_id, form in CASES:
        shape = BY_ID[shape_id]
        if kind == "sql":
            key = case_id("sql", op, shape_id, form)
            outcomes[key] = run_sql_case(spark, root, op, shape, form)
        else:
            key = case_id("df", op, shape_id, form)
            outcomes[key] = run_df_case(spark, root, op, shape, form)
    for rowid, select, ddl, _ in D2_SQL_ROWS:
        for form in ("read", "insert"):
            outcomes[f"spark-{rowid}-{form}"] = run_d2_sql_case(
                spark, root, rowid, select, ddl, form
            )
    for rowid, tree, ddl, _ in D2_DF_ROWS:
        for form in ("read", "insert"):
            outcomes[f"spark-{rowid}-{form}"] = run_d2_df_case(spark, root, rowid, tree, ddl, form)
    for case, shape, fragment, _ in D2_EXPR_CASES:
        for form in ("read", "insert"):
            outcomes[f"spark-{case}-{form}"] = run_d2_expr_case(
                spark, root, case, shape, fragment, form
            )
    for case, kind, _ in D2_VIEW_CASES:
        for form in ("read", "insert"):
            outcomes[f"spark-{case}-{form}"] = run_d2_view_case(spark, root, case, kind, form)
    yield outcomes
    spark.stop()
    _reset_active_session_for_tests()


def check_outcome(outcome: dict[str, Any], shape: dict[str, Any], op: str) -> None:
    label = f"{shape['id']} {op}"
    if outcome["kind"] == "rows":
        assert is_answer(shape, op), f"{label}: answered {outcome['rows']}"
        assert outcome["rows"] == expected_rows(shape, op), f"{label}: {outcome['rows']}"
        if "before" in outcome:
            assert outcome["snapshot"] != outcome["before"], label
    else:
        assert outcome["class"] == "AnalysisException", f"{label}: {outcome['class']}"
        assert not is_answer(shape, op), f"{label}: refused {outcome['text']}"
        check_refusal(shape, op, outcome["text"])
        if "before" in outcome:
            assert outcome["rows"] == "", label
            assert outcome["snapshot"] == outcome["before"], label


@pytest.mark.parametrize(("op", "shape_id", "form"), sql_cases())
def test_spark_sql_cells(
    measured: dict[str, dict[str, Any]], op: str, shape_id: str, form: str
) -> None:
    check_outcome(measured[case_id("sql", op, shape_id, form)], BY_ID[shape_id], op)


@pytest.mark.parametrize(("op", "shape_id", "form"), df_cases())
def test_dataframe_cells(
    measured: dict[str, dict[str, Any]], op: str, shape_id: str, form: str
) -> None:
    check_outcome(measured[case_id("df", op, shape_id, form)], BY_ID[shape_id], op)


@pytest.mark.parametrize("case", EXPR_CASES, ids=[f"spark-{case}" for case in EXPR_CASES])
def test_f_expr_fragment_cells(measured: dict[str, dict[str, Any]], case: str) -> None:
    outcome = measured[f"spark-{case}"]
    if case in ("f1-select", "f1-withcolumn", "f1-filter"):
        assert outcome["kind"] == "refused", outcome
        assert outcome["class"] == "AnalysisException", outcome
        assert "[INCOMPATIBLE_COLUMN_TYPE]" in outcome["text"], outcome["text"]
    else:
        expected = 3 if case == "f1-control-int" else 4
        assert outcome == {"kind": "rows", "rows": [{"total": expected}]}, outcome


D2_L = "named_struct('a', 1, 'b', 2)"
D2_R = "named_struct('b', 3, 'a', 4)"
D2_RS = "named_struct('a', 3, 'b', 4)"
D2_NESTED_DDL = "STRUCT<x: STRUCT<a: INT, b: INT>>"
D2_REFUSE_UNION = ("refuse", "UNION", "s", "b, a", "a, b")
D2_UNION = f"SELECT {D2_L} AS s UNION ALL SELECT {D2_R} AS s"

D2_SQL_ROWS = [
    ("d2-row1-union-all", f"SELECT NULL AS s UNION ALL {D2_UNION}", STRUCT2, D2_REFUSE_UNION),
    (
        "d2-row1-union",
        f"SELECT NULL AS s UNION SELECT {D2_L} AS s UNION SELECT {D2_R} AS s",
        STRUCT2,
        D2_REFUSE_UNION,
    ),
    (
        "d2-row2-union-all",
        "SELECT named_struct('x', NULL) AS s UNION ALL "
        "SELECT named_struct('x', named_struct('a', 1, 'b', 2)) AS s UNION ALL "
        "SELECT named_struct('x', named_struct('b', 3, 'a', 4)) AS s",
        D2_NESTED_DDL,
        ("refuse", "UNION", "s.x", "b, a", "a, b"),
    ),
    (
        "d2-row3-union-all",
        f"SELECT CASE WHEN true THEN NULL END AS s UNION ALL {D2_UNION}",
        STRUCT2,
        D2_REFUSE_UNION,
    ),
    (
        "d2-row4-intersect",
        f"(SELECT NULL AS s UNION ALL SELECT {D2_L} AS s) INTERSECT SELECT {D2_R} AS s",
        STRUCT2,
        ("refuse", "INTERSECT", "s", "b, a", "a, b"),
    ),
    (
        "d2-row4-except",
        f"(SELECT NULL AS s UNION ALL SELECT {D2_L} AS s) EXCEPT SELECT {D2_R} AS s",
        STRUCT2,
        ("refuse", "EXCEPT", "s", "b, a", "a, b"),
    ),
    (
        "d2-row5-union-all",
        f"SELECT CAST(NULL AS STRUCT<a: INT, b: INT>) AS s UNION ALL {D2_UNION}",
        STRUCT2,
        D2_REFUSE_UNION,
    ),
    (
        "d2-row6-ctl-null-middle",
        f"SELECT {D2_L} AS s UNION ALL SELECT NULL AS s UNION ALL SELECT {D2_R} AS s",
        STRUCT2,
        D2_REFUSE_UNION,
    ),
    (
        "d2-row6-ctl-null-nested",
        f"SELECT NULL AS s UNION ALL (SELECT {D2_L} AS s UNION ALL SELECT {D2_R} AS s)",
        STRUCT2,
        D2_REFUSE_UNION,
    ),
    (
        "d2-row6-ctl-same-names",
        f"SELECT NULL AS s UNION ALL SELECT {D2_L} AS s UNION ALL SELECT {D2_RS} AS s",
        STRUCT2,
        ("answer", "[1,2]|[3,4]|null"),
    ),
    (
        "d2-row12-cast-STRUCT-SETOP-USER-CAST-OVERREFUSAL-1",
        f"SELECT CAST({D2_R} AS STRUCT<a: INT, b: INT>) AS s UNION ALL SELECT {D2_L} AS s",
        STRUCT2,
        D2_REFUSE_UNION,
    ),
]
D2_SQL_BY_ID = {row[0]: row for row in D2_SQL_ROWS}

D2_N0 = ("S", "SELECT NULL AS s")
D2_NL = ("S", f"SELECT {D2_L} AS s")
D2_NR = ("S", f"SELECT {D2_R} AS s")
D2_NRS = ("S", f"SELECT {D2_RS} AS s")
D2_X0 = ("S", "SELECT named_struct('x', NULL) AS s")
D2_X1 = ("S", "SELECT named_struct('x', named_struct('a', 1, 'b', 2)) AS s")
D2_X2 = ("S", "SELECT named_struct('x', named_struct('b', 3, 'a', 4)) AS s")
D2_C0 = ("S", "SELECT CASE WHEN true THEN NULL END AS s")
D2_K0 = ("S", "SELECT CAST(NULL AS STRUCT<a: INT, b: INT>) AS s")
D2_CAST = ("S", f"SELECT CAST({D2_R} AS STRUCT<a: INT, b: INT>) AS s")
D2_DF_ROWS = [
    ("d2-df-row1-union-all", ("U", ("U", D2_N0, D2_NL), D2_NR), STRUCT2, D2_REFUSE_UNION),
    ("d2-df-row1-union", ("UD", ("UD", D2_N0, D2_NL), D2_NR), STRUCT2, D2_REFUSE_UNION),
    (
        "d2-df-row2-union-all",
        ("U", ("U", D2_X0, D2_X1), D2_X2),
        D2_NESTED_DDL,
        ("refuse", "UNION", "s.x", "b, a", "a, b"),
    ),
    ("d2-df-row3-union-all", ("U", ("U", D2_C0, D2_NL), D2_NR), STRUCT2, D2_REFUSE_UNION),
    (
        "d2-df-row4-intersect",
        ("I", ("U", D2_N0, D2_NL), D2_NR),
        STRUCT2,
        ("refuse", "INTERSECT", "s", "b, a", "a, b"),
    ),
    (
        "d2-df-row4-except",
        ("E", ("U", D2_N0, D2_NL), D2_NR),
        STRUCT2,
        ("refuse", "EXCEPT", "s", "b, a", "a, b"),
    ),
    ("d2-df-row5-union-all", ("U", ("U", D2_K0, D2_NL), D2_NR), STRUCT2, D2_REFUSE_UNION),
    ("d2-df-row6-ctl-null-middle", ("U", ("U", D2_NL, D2_N0), D2_NR), STRUCT2, D2_REFUSE_UNION),
    ("d2-df-row6-ctl-null-nested", ("U", D2_N0, ("U", D2_NL, D2_NR)), STRUCT2, D2_REFUSE_UNION),
    (
        "d2-df-row6-ctl-same-names",
        ("U", ("U", D2_N0, D2_NL), D2_NRS),
        STRUCT2,
        ("answer", "[1,2]|[3,4]|null"),
    ),
    (
        "d2-df-row12-cast-STRUCT-SETOP-USER-CAST-OVERREFUSAL-1",
        ("U", D2_CAST, D2_NL),
        STRUCT2,
        D2_REFUSE_UNION,
    ),
]
D2_DF_BY_ID = {row[0]: row for row in D2_DF_ROWS}

D2_EXPR_MAX = (
    "(SELECT max(s.a) FROM (SELECT named_struct('a', 1, 'b', 2) AS s "
    "UNION ALL SELECT named_struct('b', 3, 'a', 4) AS s))"
)
D2_EXPR_MAX_INT = "(SELECT max(x) FROM (SELECT 3 AS x UNION ALL SELECT 2 AS x))"
D2_EXPR_MAX_STRUCT = (
    "(SELECT max(s.a) FROM (SELECT named_struct('a', 1, 'b', 2) AS s "
    "UNION ALL SELECT named_struct('a', 4, 'b', 5) AS s))"
)
D2_EXPR_CASES = [
    ("d2-expr-max-select", "select", D2_EXPR_MAX, D2_REFUSE_UNION),
    ("d2-expr-max-withcolumn", "withcolumn", D2_EXPR_MAX, D2_REFUSE_UNION),
    ("d2-expr-max-filter", "filter", D2_EXPR_MAX, D2_REFUSE_UNION),
    ("d2-expr-max-control-int", "select", D2_EXPR_MAX_INT, ("answer", 3)),
    ("d2-expr-max-control-struct", "select", D2_EXPR_MAX_STRUCT, ("answer", 4)),
]
D2_EXPR_BY_ID = {row[0]: row for row in D2_EXPR_CASES}

D2_VIEW_CASES = [
    ("d2-row11-create-view", "create", D2_REFUSE_UNION),
    ("d2-row11-create-temp-view", "create-temp", D2_REFUSE_UNION),
    ("d2-row11-dataframe-temp-view", "df", D2_REFUSE_UNION),
]
D2_VIEW_BY_ID = {row[0]: row for row in D2_VIEW_CASES}


def d2_build(spark: ReparkSession, tree: tuple[Any, ...]) -> Any:
    if tree[0] == "S":
        return spark.sql(tree[1])
    left = d2_build(spark, tree[1])
    right = d2_build(spark, tree[2])
    if tree[0] == "U":
        return left.union(right)
    if tree[0] == "UD":
        return left.union(right).distinct()
    if tree[0] == "I":
        return left.intersect(right)
    return left.subtract(right)


def d2_table(rowid: str) -> str:
    return f"d2t_{rowid.replace('-', '_').lower()}_insert"


def d2_create_v_table(spark: ReparkSession, table: str) -> None:
    spark.sql(
        f"CREATE TABLE ice.ns.{table} (v INT) USING iceberg TBLPROPERTIES "
        "('format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
        "'write.update.mode' = 'copy-on-write')"
    ).collect()


def d2_rows_of_v(spark: ReparkSession, table: str) -> str:
    pylist = spark.sql(f"SELECT v FROM ice.ns.{table}").toArrow().to_pylist()
    return "|".join(sorted(render(row["v"]) for row in pylist))


def d2_expr_frame(spark: ReparkSession, shape: str, fragment: str) -> Any:
    base = spark.sql("SELECT 1 AS one")
    if shape == "select":
        return base.select(F.expr(fragment).alias("v"))
    if shape == "withcolumn":
        return base.withColumn("v", F.expr(fragment)).select("v")
    return base.filter(F.expr(f"{fragment} IS NOT NULL")).select(F.expr(fragment).alias("v"))


def d2_refused(error: Exception, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    outcome: dict[str, Any] = {"kind": "refused", "class": type(error).__name__, "text": str(error)}
    if extra:
        outcome.update(extra)
    return outcome


def run_d2_sql_case(
    spark: ReparkSession, root: Path, rowid: str, select: str, ddl: str, form: str
) -> dict[str, Any]:
    if form == "read":
        try:
            return {"kind": "rows", "rows": run_sql(spark, select)}
        except Exception as error:
            return d2_refused(error)
    table = d2_table(rowid)
    create_table(spark, table, ddl)
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        spark.sql(f"INSERT INTO ice.ns.{table} {select}").collect()
    except Exception as error:
        return d2_refused(
            error,
            {"rows": rows_of(spark, table), "snapshot": snapshot_of(root, table), "before": before},
        )
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def run_d2_df_case(
    spark: ReparkSession, root: Path, rowid: str, tree: tuple[Any, ...], ddl: str, form: str
) -> dict[str, Any]:
    if form == "read":
        try:
            rows = render_rows(d2_build(spark, tree).toArrow().to_pylist())
        except Exception as error:
            return d2_refused(error)
        return {"kind": "rows", "rows": rows}
    table = d2_table(rowid)
    create_table(spark, table, ddl)
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        d2_build(spark, tree).write.insertInto(f"ice.ns.{table}")
    except Exception as error:
        return d2_refused(
            error,
            {"rows": rows_of(spark, table), "snapshot": snapshot_of(root, table), "before": before},
        )
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def run_d2_expr_case(
    spark: ReparkSession, root: Path, case: str, shape: str, fragment: str, form: str
) -> dict[str, Any]:
    if form == "read":
        try:
            rows = d2_expr_frame(spark, shape, fragment).toArrow().to_pylist()
        except Exception as error:
            return d2_refused(error)
        return {"kind": "rows", "rows": rows}
    table = d2_table(case)
    d2_create_v_table(spark, table)
    assert d2_rows_of_v(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        frame = d2_expr_frame(spark, shape, fragment)
        frame.write.insertInto(f"ice.ns.{table}")
    except Exception as error:
        return d2_refused(
            error,
            {
                "rows": d2_rows_of_v(spark, table),
                "snapshot": snapshot_of(root, table),
                "before": before,
            },
        )
    return {
        "kind": "rows",
        "rows": d2_rows_of_v(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def run_d2_view_case(
    spark: ReparkSession, root: Path, case: str, kind: str, form: str
) -> dict[str, Any]:
    view = f"d2v_{case.replace('-', '_')}"
    if kind in ("create", "create-temp"):
        verb = "CREATE VIEW" if kind == "create" else "CREATE TEMPORARY VIEW"
        try:
            spark.sql(f"{verb} {view} AS {D2_UNION}").collect()
        except Exception as error:
            return d2_refused(error)
        return {"kind": "rows", "rows": "created"}
    df = spark.sql(f"SELECT {D2_L} AS s").union(spark.sql(f"SELECT {D2_R} AS s"))
    try:
        df.createOrReplaceTempView(view)
    except Exception as error:
        return d2_refused(error, {"stage": "create"})
    if form == "read":
        try:
            return {"kind": "rows", "rows": run_sql(spark, f"SELECT * FROM {view}")}
        except Exception as error:
            return d2_refused(error)
    table = d2_table(case)
    create_table(spark, table, STRUCT2)
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        spark.sql(f"INSERT INTO ice.ns.{table} SELECT * FROM {view}").collect()
    except Exception as error:
        return d2_refused(
            error,
            {"rows": rows_of(spark, table), "snapshot": snapshot_of(root, table), "before": before},
        )
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def check_d2_refusal(outcome: dict[str, Any], expect: tuple[Any, ...]) -> None:
    assert outcome["kind"] == "refused", outcome
    assert outcome.get("stage", "read") != "create", outcome
    assert outcome["class"] == "AnalysisException", outcome
    _, word, path, right, left = expect
    check_refusal_text(outcome["text"], word, path, right, left)


def check_d2_outcome(outcome: dict[str, Any], expect: tuple[Any, ...], form: str) -> None:
    if expect[0] == "answer":
        assert outcome["kind"] == "rows", outcome
        assert outcome["rows"] == expect[1], outcome
        if form == "insert":
            assert outcome["snapshot"] != outcome["before"], outcome
        return
    check_d2_refusal(outcome, expect)
    if form == "insert" and "before" in outcome:
        assert outcome["rows"] == "", outcome
        assert outcome["snapshot"] == outcome["before"], outcome


def check_d2_expr_outcome(outcome: dict[str, Any], expect: tuple[Any, ...], form: str) -> None:
    if expect[0] == "answer":
        if form == "read":
            assert outcome == {"kind": "rows", "rows": [{"v": expect[1]}]}, outcome
        else:
            assert outcome["kind"] == "rows", outcome
            assert outcome["rows"] == str(expect[1]), outcome
            assert outcome["snapshot"] != outcome["before"], outcome
        return
    check_d2_refusal(outcome, expect)
    if form == "insert":
        assert outcome["rows"] == "", outcome
        assert outcome["snapshot"] == outcome["before"], outcome


def d2_params(rows: list[tuple[Any, ...]]) -> list[pytest.ParamSet]:
    return [
        pytest.param(row[0], form, id=f"spark-{row[0]}-{form}")
        for row in rows
        for form in ("read", "insert")
    ]


@pytest.mark.parametrize(("rowid", "form"), d2_params(D2_SQL_ROWS))
def test_d2_sql_cells(measured: dict[str, dict[str, Any]], rowid: str, form: str) -> None:
    check_d2_outcome(measured[f"spark-{rowid}-{form}"], D2_SQL_BY_ID[rowid][3], form)


@pytest.mark.parametrize(("rowid", "form"), d2_params(D2_DF_ROWS))
def test_d2_dataframe_cells(measured: dict[str, dict[str, Any]], rowid: str, form: str) -> None:
    check_d2_outcome(measured[f"spark-{rowid}-{form}"], D2_DF_BY_ID[rowid][3], form)


@pytest.mark.parametrize(("case", "form"), d2_params(D2_EXPR_CASES))
def test_d2_expr_cells(measured: dict[str, dict[str, Any]], case: str, form: str) -> None:
    check_d2_expr_outcome(measured[f"spark-{case}-{form}"], D2_EXPR_BY_ID[case][3], form)


@pytest.mark.parametrize(("case", "form"), d2_params(D2_VIEW_CASES))
def test_d2_view_cells(measured: dict[str, dict[str, Any]], case: str, form: str) -> None:
    check_d2_outcome(measured[f"spark-{case}-{form}"], D2_VIEW_BY_ID[case][2], form)
