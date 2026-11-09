"""STRUCT-SETOP-TYPE-FAMILY-1: the set-operation name check is kind-invariant."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas
import polars
import pyarrow as pa
import pytest

import repark
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

VALS_L = {"a": 1, "b": 2}
VALS_R = {"a": 4, "b": 3}

REPS = [
    ("list", "list"),
    ("large", "list"),
    ("list", "fsl1"),
    ("fsl1", "fsl2"),
    ("largeview", "largeview"),
    ("view", "list"),
]

DF_OPS = ("union", "subtract", "intersect")
FORMS = ("read", "insert")
OP_SQL = {"union": "UNION ALL", "subtract": "EXCEPT", "intersect": "INTERSECT"}
OP_WORD = {"union": "UNION", "subtract": "EXCEPT", "intersect": "INTERSECT"}

DDL_TOP = "ARRAY<STRUCT<a: BIGINT, b: BIGINT>>"
DDL_STRUCT = "STRUCT<x: ARRAY<STRUCT<a: BIGINT, b: BIGINT>>>"
DDL_LIST = "ARRAY<ARRAY<STRUCT<a: BIGINT, b: BIGINT>>>"
DDL_MAP = "MAP<STRING, ARRAY<STRUCT<a: BIGINT, b: BIGINT>>>"

MAP_ROWS = '[["k",[[1,2]]]]|[["k",[[4,3]]]]'

FK_NULL_LAST = (
    "SELECT named_struct('a', 1, 'b', 2) AS s UNION ALL "
    "SELECT CAST(NULL AS STRUCT<b: INT, a: INT>) AS s"
)
FK_NULL_FIRST = (
    "SELECT CAST(NULL AS STRUCT<b: INT, a: INT>) AS s UNION ALL "
    "SELECT named_struct('a', 1, 'b', 2) AS s"
)
FK_SQL = {"null_last": FK_NULL_LAST, "null_first": FK_NULL_FIRST}

ICE_LEFT_INSERT = "SELECT array(named_struct('a', CAST(1 AS BIGINT), 'b', CAST(2 AS BIGINT))) AS s"


def ordered(order: str) -> list[str]:
    return ["a", "b"] if order == "AB" else ["b", "a"]


def width(kind: str) -> int:
    return 2 if kind == "fsl2" else 1


def side(kind: str, enc: str = "plain", order: str = "AB", names: str = "std") -> dict[str, str]:
    return {"kind": kind, "enc": enc, "order": order, "names": names}


def arrow_struct(order: str) -> pa.StructType:
    return pa.struct([(name, pa.int64()) for name in ordered(order)])


def arrow_elem(payload: str, order: str) -> pa.DataType:
    struct = arrow_struct(order)
    return struct if payload == "struct" else pa.list_(struct)


def arrow_elem_value(payload: str, values: dict[str, int], order: str) -> Any:
    row = {name: values[name] for name in ordered(order)}
    return row if payload == "struct" else [row]


def kind_arrow(kind: str, elem: pa.DataType) -> pa.DataType:
    if kind == "list":
        return pa.list_(elem)
    if kind == "large":
        return pa.large_list(elem)
    if kind == "fsl1":
        return pa.list_(elem, 1)
    if kind == "fsl2":
        return pa.list_(elem, 2)
    if kind == "view":
        return pa.list_view(elem)
    return pa.large_list_view(elem)


def side_array(spec: dict[str, str], payload: str, shape: str, values: dict[str, int]) -> pa.Array:
    elem = arrow_elem(payload, spec["order"])
    list_type = kind_arrow(spec["kind"], elem)
    element = arrow_elem_value(payload, values, spec["order"])
    row = [element] * width(spec["kind"])
    if shape == "top":
        plain = pa.array([row], type=list_type)
        if spec["enc"] == "plain":
            return plain
        if spec["enc"] == "ree":
            return pa.RunEndEncodedArray.from_arrays(pa.array([1], type=pa.int32()), plain)
        return pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int32()), plain)
    if shape == "struct":
        return pa.array([{"x": row}], type=pa.struct([("x", list_type)]))
    if shape == "list":
        return pa.array([[row]], type=pa.list_(list_type))
    if spec["names"] == "std":
        return pa.array([[("k", row)]], type=pa.map_(pa.string(), list_type))
    key_field = pa.field("k", pa.string(), nullable=False)
    return pa.array([[("k", row)]], type=pa.map_(key_field, pa.field("v", list_type)))


def to_pandas(array: pa.Array) -> pandas.DataFrame:
    return pa.table({"s": array}).to_pandas(types_mapper=pandas.ArrowDtype)


def polars_side(
    spec: dict[str, str], payload: str, source: str, values: dict[str, int]
) -> polars.DataFrame:
    inner = polars.Struct([polars.Field(name, polars.Int64) for name in ordered(spec["order"])])
    elem = inner if payload == "struct" else polars.List(inner)
    element: Any = {name: values[name] for name in ordered(spec["order"])}
    if payload != "struct":
        element = [element]
    if source == "polars_array" and spec["kind"] in ("fsl1", "fsl2"):
        dtype = polars.Array(elem, width(spec["kind"]))
        row = [element] * width(spec["kind"])
    else:
        dtype = polars.List(elem)
        row = [element]
    return polars.DataFrame({"s": [row]}, schema={"s": dtype})


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


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def snapshot_of(root: Path, table: str) -> tuple[int, int | None]:
    metadata = sorted(
        (root / "ns" / table / "metadata").glob("*.metadata.json"), key=metadata_order
    )
    document = json.loads(metadata[-1].read_text(encoding="utf-8"))
    return (len(document.get("snapshots") or []), document.get("current-snapshot-id"))


def check_refusal_text(error: str, word: str, path: str, right: str, left: str) -> None:
    assert "[INCOMPATIBLE_COLUMN_TYPE]" in error, error
    assert f"{word} can only be performed" in error, error
    marker = "Struct field names differ at "
    assert marker in error, error
    rest = error.split(marker, 1)[1].split(";", 1)[0]
    found_path, _, found_lists = rest.partition(": ")
    assert found_path == path, error
    assert found_lists == f"[{right}] versus [{left}]", error


def refuse_for(op: str, path: str, orient: str) -> tuple[str, ...]:
    right, left = ("a, b", "b, a") if orient == "rev" else ("b, a", "a, b")
    return ("refuse", OP_WORD[op], path, right, left)


def ddl_for_shape(shape: str, payload: str) -> str:
    if shape == "struct":
        return DDL_STRUCT
    if shape == "list" or payload == "lstruct":
        return DDL_LIST
    if shape == "map":
        return DDL_MAP
    return DDL_TOP


def pandas_top_cells() -> list[dict[str, Any]]:
    cells = []
    for left_kind, right_kind in REPS:
        for orient in ("fwd", "rev"):
            if orient == "fwd":
                left = side(left_kind, "plain", "AB")
                right = side(right_kind, "plain", "BA")
            else:
                left = side(right_kind, "plain", "BA")
                right = side(left_kind, "plain", "AB")
            for op in DF_OPS:
                for form in FORMS:
                    expect: tuple[str, ...] = (
                        ("refuse-other",)
                        if "view" in (left_kind, right_kind)
                        else refuse_for(op, "s[]", orient)
                    )
                    pair = f"{left_kind}-plain-{right_kind}-plain"
                    cells.append(
                        {
                            "id": f"tf1-pandas-{pair}-{orient}-{op}-{form}",
                            "source": "pandas",
                            "left": left,
                            "right": right,
                            "payload": "struct",
                            "shape": "top",
                            "op": op,
                            "form": form,
                            "orient": orient,
                            "ddl": DDL_TOP,
                            "expect": expect,
                        }
                    )
    return cells


def pandas_ree_cells() -> list[dict[str, Any]]:
    cells = []
    for left_kind, right_kind in REPS:
        for op in DF_OPS:
            for form in FORMS:
                expect = (
                    ("refuse-other",)
                    if "view" in (left_kind, right_kind)
                    else refuse_for(op, "s[]", "fwd")
                )
                cells.append(
                    {
                        "id": f"tf1-pandas-{left_kind}-ree-{right_kind}-ree-fwd-{op}-{form}",
                        "source": "pandas",
                        "left": side(left_kind, "ree", "AB"),
                        "right": side(right_kind, "ree", "BA"),
                        "payload": "struct",
                        "shape": "top",
                        "op": op,
                        "form": form,
                        "orient": "fwd",
                        "ddl": DDL_TOP,
                        "expect": expect,
                    }
                )
    for left_enc, right_enc in (("ree", "plain"), ("plain", "ree")):
        for form in FORMS:
            cells.append(
                {
                    "id": f"tf1-pandas-list-{left_enc}-list-{right_enc}-fwd-union-{form}",
                    "source": "pandas",
                    "left": side("list", left_enc, "AB"),
                    "right": side("list", right_enc, "BA"),
                    "payload": "struct",
                    "shape": "top",
                    "op": "union",
                    "form": form,
                    "orient": "fwd",
                    "ddl": DDL_TOP,
                    "expect": refuse_for("union", "s[]", "fwd"),
                }
            )
    return cells


def pandas_depth_cells() -> list[dict[str, Any]]:
    depths = [
        ("struct", "struct", "s.x[]", ("AB", "std"), ("BA", "std"), "refuse"),
        ("list", "list", "s[][]", ("AB", "std"), ("BA", "std"), "refuse"),
        ("map_std_reordered", "map", "s{value}[]", ("AB", "std"), ("BA", "std"), "refuse"),
        ("map_ren_reordered", "map", "s{value}[]", ("AB", "std"), ("BA", "ren"), "refuse"),
        ("map_std_same", "map", "", ("AB", "std"), ("AB", "std"), "answer"),
        ("map_ren_same", "map", "", ("AB", "std"), ("AB", "ren"), "answer"),
    ]
    pairs = {
        "struct": [("list", "list"), ("large", "list")],
        "list": [("list", "list")],
        "map_std_reordered": [("list", "list")],
        "map_ren_reordered": [("list", "list")],
        "map_std_same": [
            ("list", "list"),
            ("large", "list"),
            ("list", "fsl1"),
            ("largeview", "largeview"),
        ],
        "map_ren_same": [("list", "list"), ("large", "list")],
    }
    cells = []
    for name, shape, path, left_cfg, right_cfg, kind in depths:
        for left_kind, right_kind in pairs[name]:
            for form in FORMS:
                if kind == "answer":
                    expect: tuple[str, ...] = ("answer", MAP_ROWS)
                else:
                    expect = refuse_for("union", path, "fwd")
                pair = f"{left_kind}-plain-{right_kind}-plain"
                cells.append(
                    {
                        "id": f"tf1-pandas-{name}-{pair}-fwd-union-{form}",
                        "source": "pandas",
                        "left": side(left_kind, "plain", left_cfg[0], left_cfg[1]),
                        "right": side(right_kind, "plain", right_cfg[0], right_cfg[1]),
                        "payload": "struct",
                        "shape": shape,
                        "op": "union",
                        "form": form,
                        "orient": "fwd",
                        "ddl": ddl_for_shape(shape, "struct"),
                        "expect": expect,
                    }
                )
    for left_kind, right_kind in (("list", "list"), ("large", "list")):
        for form in FORMS:
            pair = f"{left_kind}-plain-{right_kind}-plain"
            cells.append(
                {
                    "id": f"tf1-pandas-lstruct-{pair}-fwd-union-{form}",
                    "source": "pandas",
                    "left": side(left_kind, "plain", "AB"),
                    "right": side(right_kind, "plain", "BA"),
                    "payload": "lstruct",
                    "shape": "top",
                    "op": "union",
                    "form": form,
                    "orient": "fwd",
                    "ddl": ddl_for_shape("top", "lstruct"),
                    "expect": refuse_for("union", "s[][]", "fwd"),
                }
            )
    return cells


def polars_cells() -> list[dict[str, Any]]:
    cells = []
    for left_kind, right_kind in REPS:
        for form in FORMS:
            cells.append(
                {
                    "id": f"tf1-polars_df-{left_kind}-{right_kind}-fwd-union-{form}",
                    "source": "polars_df",
                    "left": side(left_kind, "plain", "AB"),
                    "right": side(right_kind, "plain", "BA"),
                    "payload": "struct",
                    "shape": "top",
                    "op": "union",
                    "form": form,
                    "orient": "fwd",
                    "ddl": DDL_TOP,
                    "expect": refuse_for("union", "s[]", "fwd"),
                }
            )
    for op in ("subtract", "intersect"):
        for form in FORMS:
            cells.append(
                {
                    "id": f"tf1-polars_df-list-list-fwd-{op}-{form}",
                    "source": "polars_df",
                    "left": side("list", "plain", "AB"),
                    "right": side("list", "plain", "BA"),
                    "payload": "struct",
                    "shape": "top",
                    "op": op,
                    "form": form,
                    "orient": "fwd",
                    "ddl": DDL_TOP,
                    "expect": refuse_for(op, "s[]", "fwd"),
                }
            )
    for op in DF_OPS:
        for form in FORMS:
            cells.append(
                {
                    "id": f"tf1-polars_array-fsl1-fsl2-fwd-{op}-{form}",
                    "source": "polars_array",
                    "left": side("fsl1", "plain", "AB"),
                    "right": side("fsl2", "plain", "BA"),
                    "payload": "struct",
                    "shape": "top",
                    "op": op,
                    "form": form,
                    "orient": "fwd",
                    "ddl": DDL_TOP,
                    "expect": refuse_for(op, "s[]", "fwd"),
                }
            )
    for form in FORMS:
        cells.append(
            {
                "id": f"tf1-polars_array-list-list-fwd-union-{form}",
                "source": "polars_array",
                "left": side("list", "plain", "AB"),
                "right": side("list", "plain", "BA"),
                "payload": "struct",
                "shape": "top",
                "op": "union",
                "form": form,
                "orient": "fwd",
                "ddl": DDL_TOP,
                "expect": refuse_for("union", "s[]", "fwd"),
            }
        )
    return cells


def iceberg_cells() -> list[dict[str, Any]]:
    cells = []
    for op in DF_OPS:
        for form in FORMS:
            cells.append(
                {
                    "id": f"tf1-ice_polars-list-large-fwd-{op}-{form}",
                    "source": "ice_polars",
                    "left": side("list", "plain", "AB"),
                    "right": side("large", "plain", "BA"),
                    "payload": "struct",
                    "shape": "top",
                    "op": op,
                    "form": form,
                    "orient": "fwd",
                    "ddl": DDL_TOP,
                    "expect": refuse_for(op, "s[]", "fwd"),
                }
            )
    for form in FORMS:
        cells.append(
            {
                "id": f"tf1-ice_polars_sql-list-large-fwd-union-{form}",
                "source": "ice_polars_sql",
                "left": side("list", "plain", "AB"),
                "right": side("large", "plain", "BA"),
                "payload": "struct",
                "shape": "top",
                "op": "union",
                "form": form,
                "orient": "fwd",
                "ddl": DDL_TOP,
                "expect": refuse_for("union", "s[]", "fwd"),
            }
        )
    return cells


def tempview_cells() -> list[dict[str, Any]]:
    cells = []
    for left_kind, right_kind in REPS:
        for op in DF_OPS:
            for form in FORMS:
                expect: tuple[str, ...] = (
                    ("refuse-other",)
                    if "view" in (left_kind, right_kind)
                    else refuse_for(op, "s[]", "fwd")
                )
                cells.append(
                    {
                        "id": f"tf1-tempview-{left_kind}-{right_kind}-fwd-{op}-{form}",
                        "source": "tempview",
                        "left": side(left_kind, "plain", "AB"),
                        "right": side(right_kind, "plain", "BA"),
                        "payload": "struct",
                        "shape": "top",
                        "op": op,
                        "form": form,
                        "orient": "fwd",
                        "ddl": DDL_TOP,
                        "expect": expect,
                    }
                )
    return cells


TF1_CELLS = (
    pandas_top_cells()
    + pandas_ree_cells()
    + pandas_depth_cells()
    + polars_cells()
    + iceberg_cells()
    + tempview_cells()
)
TF1_BY_ID = {cell["id"]: cell for cell in TF1_CELLS}


def table_name(cell_id: str) -> str:
    return "t_" + cell_id.replace("-", "_")


def create_table(spark: ReparkSession, table: str, ddl: str) -> None:
    spark.sql(
        f"CREATE TABLE ice.ns.{table} (s {ddl}) USING iceberg TBLPROPERTIES "
        "('format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
        "'write.update.mode' = 'copy-on-write')"
    ).collect()


def rows_of(spark: ReparkSession, table: str) -> str:
    return render_rows(spark.sql(f"SELECT s FROM ice.ns.{table}").toArrow().to_pylist())


def build_pandas_frames(spark: ReparkSession, cell: dict[str, Any]) -> tuple[Any, Any]:
    left = spark.createDataFrame(
        to_pandas(side_array(cell["left"], cell["payload"], cell["shape"], VALS_L))
    )
    right = spark.createDataFrame(
        to_pandas(side_array(cell["right"], cell["payload"], cell["shape"], VALS_R))
    )
    return left, right


def build_polars_frames(spark: ReparkSession, cell: dict[str, Any]) -> tuple[Any, Any]:
    left = spark.createDataFrame(polars_side(cell["left"], cell["payload"], cell["source"], VALS_L))
    right = spark.createDataFrame(
        polars_side(cell["right"], cell["payload"], cell["source"], VALS_R)
    )
    return left, right


def build_ice_frames(spark: ReparkSession, cell: dict[str, Any]) -> tuple[Any, Any]:
    table = "src_" + cell["id"].replace("-", "_")
    create_table(spark, table, DDL_TOP)
    spark.sql(f"INSERT INTO ice.ns.{table} {ICE_LEFT_INSERT}").collect()
    left = spark.sql(f"SELECT s FROM ice.ns.{table}")
    right = spark.createDataFrame(polars_side(cell["right"], "struct", "polars_df", VALS_R))
    return left, right


def frames_for(spark: ReparkSession, cell: dict[str, Any]) -> tuple[Any, Any]:
    if cell["source"] == "pandas":
        return build_pandas_frames(spark, cell)
    if cell["source"] in ("polars_df", "polars_array"):
        return build_polars_frames(spark, cell)
    return build_ice_frames(spark, cell)


def combine_frames(left: Any, right: Any, op: str) -> Any:
    if op == "union":
        return left.union(right)
    if op == "subtract":
        return left.subtract(right)
    return left.intersect(right)


def view_frames(spark: ReparkSession, cell: dict[str, Any]) -> tuple[Any, Any]:
    if cell["source"] == "tempview":
        return build_pandas_frames(spark, cell)
    return build_ice_frames(spark, cell)


def view_statement(left: Any, right: Any, cell: dict[str, Any]) -> str:
    stem = cell["id"].replace("-", "_")
    left_view = f"tf1v_{stem}_l"
    right_view = f"tf1v_{stem}_r"
    left.createOrReplaceTempView(left_view)
    right.createOrReplaceTempView(right_view)
    return f"SELECT s FROM {left_view} {OP_SQL[cell['op']]} SELECT s FROM {right_view}"


def refused(error: Exception) -> dict[str, Any]:
    return {"kind": "refused", "class": type(error).__name__, "text": str(error)}


def setup_refused(error: Exception) -> dict[str, Any]:
    return {"kind": "setup-refused", "class": type(error).__name__, "text": str(error)}


def run_df_cell(spark: ReparkSession, root: Path, cell: dict[str, Any]) -> dict[str, Any]:
    try:
        left, right = frames_for(spark, cell)
    except Exception as error:
        return setup_refused(error)
    if cell["form"] == "read":
        try:
            frame = combine_frames(left, right, cell["op"])
            rows = render_rows(frame.toArrow().to_pylist())
        except Exception as error:
            return refused(error)
        return {"kind": "rows", "rows": rows}
    table = table_name(cell["id"])
    create_table(spark, table, cell["ddl"])
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        frame = combine_frames(left, right, cell["op"])
        frame.write.insertInto(f"ice.ns.{table}")
    except Exception as error:
        outcome = refused(error)
        outcome.update(
            {"rows": rows_of(spark, table), "snapshot": snapshot_of(root, table), "before": before}
        )
        return outcome
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def run_view_cell(spark: ReparkSession, root: Path, cell: dict[str, Any]) -> dict[str, Any]:
    try:
        left, right = view_frames(spark, cell)
    except Exception as error:
        return setup_refused(error)
    statement = view_statement(left, right, cell)
    if cell["form"] == "read":
        try:
            rows = render_rows(spark.sql(statement).toArrow().to_pylist())
        except Exception as error:
            return refused(error)
        return {"kind": "rows", "rows": rows}
    table = table_name(cell["id"])
    create_table(spark, table, cell["ddl"])
    assert rows_of(spark, table) == ""
    before = snapshot_of(root, table)
    try:
        spark.sql(f"INSERT INTO ice.ns.{table} {statement}").collect()
    except Exception as error:
        outcome = refused(error)
        outcome.update(
            {"rows": rows_of(spark, table), "snapshot": snapshot_of(root, table), "before": before}
        )
        return outcome
    return {
        "kind": "rows",
        "rows": rows_of(spark, table),
        "snapshot": snapshot_of(root, table),
        "before": before,
    }


def run_fk_cell(spark: ReparkSession, door: str, fk: str) -> dict[str, Any]:
    run = spark.sql if door == "facade" else repark.sql
    try:
        rows = render_rows(run(FK_SQL[fk]).toArrow().to_pylist())
    except Exception as error:
        return refused(error)
    return {"kind": "rows", "rows": rows}


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, dict[str, Any]]]:
    _reset_active_session_for_tests()
    root = tmp_path_factory.mktemp("warehouse")
    spark = ReparkSession.builder.appName("struct-setop-type-family-1").getOrCreate()
    spark.register_memory_catalog("ice", str(root))
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    outcomes = {}
    for cell in TF1_CELLS:
        if cell["source"] in ("tempview", "ice_polars_sql"):
            outcomes[cell["id"]] = run_view_cell(spark, root, cell)
        else:
            outcomes[cell["id"]] = run_df_cell(spark, root, cell)
    for fk in ("null_last", "null_first"):
        for door in ("facade", "native"):
            outcomes[f"tf1-fk-{fk}-{door}"] = run_fk_cell(spark, door, fk)
    yield outcomes
    spark.stop()
    _reset_active_session_for_tests()


def check_tf1_outcome(outcome: dict[str, Any], cell: dict[str, Any]) -> None:
    label = cell["id"]
    assert outcome["kind"] != "setup-refused", f"{label}: {outcome}"
    expect = cell["expect"]
    if expect[0] == "answer":
        assert outcome["kind"] == "rows", f"{label}: {outcome}"
        assert outcome["rows"] == expect[1], f"{label}: {outcome['rows']}"
        if cell["form"] == "insert":
            assert outcome["snapshot"] != outcome["before"], label
        return
    assert outcome["kind"] == "refused", f"{label}: {outcome}"
    assert outcome["class"] == "AnalysisException", f"{label}: {outcome}"
    if expect[0] == "refuse":
        _, word, path, right, left = expect
        check_refusal_text(outcome["text"], word, path, right, left)
    else:
        assert "[INCOMPATIBLE_COLUMN_TYPE]" not in outcome["text"], outcome["text"]
    if cell["form"] == "insert":
        assert outcome["rows"] == "", label
        assert outcome["snapshot"] == outcome["before"], label


def check_fk_outcome(outcome: dict[str, Any], fk: str) -> None:
    if fk == "null_last":
        assert outcome["kind"] == "rows", outcome
        assert outcome["rows"] == "[1,2]|null", outcome
        return
    assert outcome["kind"] == "refused", outcome
    assert outcome["class"] == "AnalysisException", outcome
    check_refusal_text(outcome["text"], "UNION", "s", "a, b", "b, a")


def tf1_params() -> list[pytest.ParamSet]:
    return [pytest.param(cell["id"], id=cell["id"]) for cell in TF1_CELLS]


def fk_params() -> list[pytest.ParamSet]:
    return [
        pytest.param(fk, door, id=f"tf1-fk-{fk}-{door}")
        for fk in ("null_last", "null_first")
        for door in ("facade", "native")
    ]


@pytest.mark.parametrize("cell_id", tf1_params())
def test_variant_cells(measured: dict[str, dict[str, Any]], cell_id: str) -> None:
    check_tf1_outcome(measured[cell_id], TF1_BY_ID[cell_id])


@pytest.mark.parametrize(("fk", "door"), fk_params())
def test_fk_cells(measured: dict[str, dict[str, Any]], fk: str, door: str) -> None:
    check_fk_outcome(measured[f"tf1-fk-{fk}-{door}"], fk)
