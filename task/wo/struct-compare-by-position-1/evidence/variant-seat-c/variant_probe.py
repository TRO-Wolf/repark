import argparse
import json
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa

OUT = Path(__file__).resolve().parent
RAW = OUT / "raw"
WORK = OUT / "work"
JAR = os.environ.get("VARIANT_TABLE_IVY_JAR", "<ivy-jar>")
LEGS = ["spark", "base", "head"]

KINDS = ["list", "large", "fsl1", "fsl2", "view", "largeview"]
PAIRS = [(a, b) for a in KINDS for b in KINDS]
REPS = [
    ("list", "list"),
    ("large", "list"),
    ("list", "fsl1"),
    ("fsl1", "fsl2"),
    ("largeview", "largeview"),
    ("view", "list"),
]
ENCODED = {
    "ree_ee": ("ree", "ree"),
    "ree_ep": ("ree", "plain"),
    "ree_pe": ("plain", "ree"),
    "dict_ee": ("dict", "dict"),
    "dict_ep": ("dict", "plain"),
    "dict_pe": ("plain", "dict"),
}
MAP_VARIANTS = {
    "map_std_same": ("AB", "std"),
    "map_std_reord": ("BA", "std"),
    "map_ren_same": ("AB", "ren"),
    "map_ren_reord": ("BA", "ren"),
}
NONTOP_DEPTHS = ["struct", "list", *list(MAP_VARIANTS)]
OP_SQL = {
    "union_all": "UNION ALL",
    "union": "UNION",
    "intersect": "INTERSECT",
    "intersect_all": "INTERSECT ALL",
    "except": "EXCEPT",
    "except_all": "EXCEPT ALL",
}
NON_UNION_ALL = ["union", "intersect", "intersect_all", "except", "except_all", "union_by_name"]
RBA = "named_struct('b', CAST(2 AS BIGINT), 'a', CAST(1 AS BIGINT))"
RAB = "named_struct('a', CAST(1 AS BIGINT), 'b', CAST(2 AS BIGINT))"
PATH_STMTS = {
    "update_reordered": "UPDATE {t} SET id = 9 WHERE s = array(" + RBA + ")",
    "delete_reordered": "DELETE FROM {t} WHERE s = array(" + RBA + ")",
    "merge_reordered": "MERGE INTO {t} AS t USING (SELECT array("
    + RBA
    + ") AS s) AS u ON t.s = u.s WHEN MATCHED THEN DELETE",
    "delete_same_order": "DELETE FROM {t} WHERE s = array(" + RAB + ")",
    "merge_same_order": "MERGE INTO {t} AS t USING (SELECT array("
    + RAB
    + ") AS s) AS u ON t.s = u.s WHEN MATCHED THEN DELETE",
}
PATH_INSERT = (
    "INSERT INTO {t} VALUES (1, array("
    + RAB
    + ")), (2, array(named_struct('a', CAST(3 AS BIGINT), 'b', CAST(4 AS BIGINT))))"
)
FK_SQL = {
    "null_last": (
        "SELECT named_struct('a', 1, 'b', 2) AS s UNION ALL "
        "SELECT CAST(NULL AS STRUCT<b: INT, a: INT>) AS s"
    ),
    "null_first": (
        "SELECT CAST(NULL AS STRUCT<b: INT, a: INT>) AS s UNION ALL "
        "SELECT named_struct('a', 1, 'b', 2) AS s"
    ),
}
SEATC_SQL = {
    "eq": "SELECT l.s = r.s AS x FROM l CROSS JOIN r",
    "in": "SELECT l.s IN (r.s) AS x FROM l CROSS JOIN r",
    "join": "SELECT l.s AS x FROM l JOIN r ON l.s = r.s",
}
DML_TYPE = {
    "struct": "ARRAY<STRUCT<a: BIGINT, b: BIGINT>>",
    "lstruct": "ARRAY<ARRAY<STRUCT<a: BIGINT, b: BIGINT>>>",
}
DML_LIT = {
    "struct": (
        "array(" + RAB + ")",
        "array(" + RBA + ")",
        "array(named_struct('a', CAST(3 AS BIGINT), 'b', CAST(4 AS BIGINT)))",
    ),
    "lstruct": (
        "array(array(" + RAB + "))",
        "array(array(" + RBA + "))",
        "array(array(named_struct('a', CAST(3 AS BIGINT), 'b', CAST(4 AS BIGINT))))",
    ),
}
VALS = {"L": {"a": 1, "b": 2}, "R": {"a": 4, "b": 3}}
MEMO_TABLE = [
    ("list", "list", "refused", "refused"),
    ("large", "list", "answered", "answered"),
    ("list", "large", "refused", "answered"),
    ("fsl1", "list", "refused", "answered"),
    ("list", "fsl1", "answered", "answered"),
    ("fsl1", "fsl2", "answered", "refused"),
    ("large", "fsl1", "answered", "answered"),
    ("view", "view", "refused", "refused"),
    ("largeview", "largeview", "answered", "answered"),
    ("view", "list", "refused", "refused"),
    ("largeview", "large", "refused", "refused"),
]

CELLS = []


def side(kind: str, enc: str = "plain", order: str = "AB", names: str = "std") -> dict[str, str]:
    return {"kind": kind, "enc": enc, "order": order, "names": names}


def base_l(kind: str) -> dict[str, str]:
    return side(kind, "plain", "AB")


def base_r(kind: str) -> dict[str, str]:
    return side(kind, "plain", "BA")


def add(plan: str, legs: list[str], **fields: Any) -> dict[str, Any]:
    for leg in legs:
        cell = {
            "plan": plan,
            "leg": leg,
            "seat": "S",
            "payload": "struct",
            "shape": "top",
            "op": "union_all",
            "door": "df",
            "source": "pandas",
            "effect": "read",
            "orient": "fwd",
            "stmt": None,
            "fk": None,
            "cop": None,
        }
        cell.update(fields)
        cell["id"] = f"{plan}-{len(CELLS) + 1:05d}"
        CELLS.append(cell)


def depth_sides(a: str, b: str, shape: str) -> dict[str, dict[str, str]]:
    if shape in MAP_VARIANTS:
        order, names = MAP_VARIANTS[shape]
        return side(a, "plain", "AB", "std"), side(b, "plain", order, names), "map"
    return side(a, "plain", "AB"), side(b, "plain", "BA"), shape


def generate() -> list[dict[str, Any]]:
    CELLS.clear()
    for a, b in PAIRS:
        add("P1", LEGS, L=base_l(a), R=base_r(b))
    for enc, (el, er) in ENCODED.items():
        door, source = ("df", "pandas") if enc.startswith("ree") else ("native", "arrow_cast")
        for a, b in PAIRS:
            add("P2", LEGS, L=side(a, el, "AB"), R=side(b, er, "BA"), door=door, source=source)
    for a, b in PAIRS:
        add("P3", LEGS, payload="lstruct", L=base_l(a), R=base_r(b))
    for a, b in REPS:
        for shape in NONTOP_DEPTHS:
            left, right, key_shape = depth_sides(a, b, shape)
            add("P4", LEGS, shape=key_shape, L=left, R=right)
    for a, b in REPS:
        for op in NON_UNION_ALL:
            add("P5", LEGS, op=op, L=base_l(a), R=base_r(b))
    for a, b in PAIRS:
        add("P6", LEGS, L=base_r(b), R=base_l(a), orient="rev")
    for a, b in REPS:
        for source in ["polars_df", "polars_array", "iceberg_polars_df", "iceberg_polars_sql"]:
            door = "sql_view" if source == "iceberg_polars_sql" else "df"
            add("P7", LEGS, L=base_l(a), R=base_r(b), source=source, door=door)
    for a, b in REPS:
        add("P8", ["base", "head"], L=base_l(a), R=base_r(b), door="sql_view", source="pandas")
        add("P8", ["base", "head"], L=base_l(a), R=base_r(b), door="native", source="arrow_cast")
    add(
        "P8b",
        LEGS,
        L=base_l("list"),
        R=base_r("list"),
        op="rcte",
        door="native",
        source="arrow_cast",
    )
    for a, b in PAIRS:
        add("P9", LEGS, L=base_l(a), R=base_r(b), effect="insert")
    for a, b in PAIRS:
        for cop in ["eq", "in", "join"]:
            for payload in ["struct", "lstruct"]:
                add(
                    "P10",
                    ["spark", "base"],
                    seat="C",
                    cop=cop,
                    op=cop,
                    payload=payload,
                    L=base_l(a),
                    R=base_r(b),
                    door="sql_view",
                    source="pandas",
                )
    for payload in ["struct", "lstruct"]:
        add(
            "P10dml",
            ["spark", "base"],
            seat="C",
            cop="dml",
            op="dml",
            payload=payload,
            L=base_l("list"),
            R=base_r("list"),
            door="sql_view",
            source="pandas",
        )
    for stmt in PATH_STMTS:
        add(
            "P11",
            ["spark", "base"],
            seat="PATH",
            stmt=stmt,
            op=stmt,
            door="sql_view",
            source="none",
            L=base_l("list"),
            R=base_r("list"),
        )
    for fk in FK_SQL:
        add(
            "P12",
            ["spark"],
            seat="FK",
            fk=fk,
            op=fk,
            door="spark_sql",
            source="none",
            L=base_l("list"),
            R=base_r("list"),
        )
        add(
            "P12",
            ["base", "head"],
            seat="FK",
            fk=fk,
            op=fk,
            door="facade_sql",
            source="none",
            L=base_l("list"),
            R=base_r("list"),
        )
        add(
            "P12",
            ["base", "head"],
            seat="FK",
            fk=fk,
            op=fk,
            door="native_sql",
            source="none",
            L=base_l("list"),
            R=base_r("list"),
        )
    for a, b in PAIRS:
        add("P13", LEGS, op="except", L=base_l(a), R=base_r(b), orient="fwd")
        add("P13", LEGS, op="except", L=base_r(b), R=base_l(a), orient="rev")
    for enc, (el, er) in ENCODED.items():
        door, source = ("df", "pandas") if enc.startswith("ree") else ("native", "arrow_cast")
        for a, b in REPS:
            add(
                "P14",
                LEGS,
                op="except",
                L=side(a, el, "AB"),
                R=side(b, er, "BA"),
                door=door,
                source=source,
            )
    add(
        "P15",
        ["base", "head"],
        L=side("list", "dict", "AB"),
        R=side("list", "dict", "BA"),
        door="df",
        source="pandas",
    )
    return CELLS


def ordered(order: str) -> list[str]:
    return ["a", "b"] if order == "AB" else ["b", "a"]


def width(kind: str) -> int:
    return 2 if kind == "fsl2" else 1


def skey(c: dict[str, Any]) -> str:
    if c["seat"] == "S":
        return (
            f"S|{c['payload']}|{c['shape']}|{c['op']}|"
            f"{c['L']['order']}|{c['R']['order']}|{c['effect']}"
        )
    if c["seat"] == "C":
        if c["cop"] == "dml":
            return f"CDML|{c['payload']}"
        return f"C|{c['cop']}|{c['payload']}"
    if c["seat"] == "PATH":
        return f"PATH|{c['stmt']}"
    return f"FK|{c['fk']}"


def tkey(c: dict[str, Any]) -> str:
    def strip(s: dict[str, str]) -> str:
        out = dict(s)
        out["kind"] = "list"
        return out

    return json.dumps(
        {
            "leg": c["leg"],
            "seat": c["seat"],
            "payload": c["payload"],
            "shape": c["shape"],
            "op": c["op"],
            "effect": c["effect"],
            "door": c["door"],
            "source": c["source"],
            "stmt": c["stmt"],
            "fk": c["fk"],
            "cop": c["cop"],
            "L": strip(c["L"]),
            "R": strip(c["R"]),
        },
        sort_keys=True,
    )


def canon_py(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): canon_py(v) for k, v in value.items()}
    if isinstance(value, list):
        if value and all(isinstance(item, tuple) for item in value):
            return {str(k): canon_py(v) for k, v in value}
        return [canon_py(item) for item in value]
    return value


def plain_spark(value: Any) -> Any:
    from pyspark.sql import Row

    if isinstance(value, Row):
        fields = zip(value.__fields__, value, strict=False)
        return {name: plain_spark(item) for name, item in fields}
    if isinstance(value, dict):
        return {str(k): plain_spark(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain_spark(item) for item in value]
    return value


def rowset(rows: list[str]) -> str:
    return sorted(json.dumps(canon_py(row), sort_keys=True, default=str) for row in rows)


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob("*") if item.is_file())


def step(phase: str, call: Callable[[], Any]) -> tuple[Any, str | None, str | None]:
    try:
        return call(), None
    except BaseException as error:
        return None, {
            "phase": phase,
            "cls": type(error).__name__,
            "message": str(error).replace("\n", " ")[:200],
        }


def answered(
    rows: list[str],
    types: dict[str, str] | None = None,
    persisted: list[str] | None = None,
    files: int | None = None,
) -> dict[str, Any]:
    return {
        "answered": True,
        "rows": rowset(rows),
        "types": types or {},
        "persisted": rowset(persisted) if persisted is not None else None,
        "files": files,
        "error": None,
    }


def refused(
    err: tuple[str, str, str] | Exception,
    types: dict[str, str] | None = None,
    persisted: list[str] | None = None,
    files: int | None = None,
) -> dict[str, Any]:
    return {
        "answered": False,
        "rows": None,
        "types": types or {},
        "persisted": rowset(persisted) if persisted is not None else None,
        "files": files,
        "error": err,
    }


def to_pandas(arr: pa.Array) -> Any:
    import pandas

    return pa.table({"s": arr}).to_pandas(types_mapper=pandas.ArrowDtype)


def arrow_struct(order: str) -> pa.StructType:
    return pa.struct([(n, pa.int64()) for n in ordered(order)])


def arrow_elem(payload: str, order: str) -> pa.DataType:
    st = arrow_struct(order)
    return st if payload == "struct" else pa.list_(st)


def arrow_elem_value(payload: str, values: dict[str, int], order: str) -> Any:
    row = {n: values[n] for n in ordered(order)}
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


def side_array(s: dict[str, str], payload: str, shape: str, values: dict[str, int]) -> pa.Array:
    elem = arrow_elem(payload, s["order"])
    kt = kind_arrow(s["kind"], elem)
    ev = arrow_elem_value(payload, values, s["order"])
    row = [ev] * width(s["kind"])
    if shape == "top":
        plain = pa.array([row], type=kt)
        if s["enc"] == "plain":
            return plain
        if s["enc"] == "ree":
            return pa.RunEndEncodedArray.from_arrays(pa.array([1], type=pa.int32()), plain)
        return pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int32()), plain)
    if shape == "struct":
        return pa.array([{"x": row}], type=pa.struct([("x", kt)]))
    if shape == "list":
        return pa.array([[row]], type=pa.list_(kt))
    if s["names"] == "std":
        return pa.array([[("k", row)]], type=pa.map_(pa.string(), kt))
    return pa.array(
        [[("k", row)]], type=pa.map_(pa.field("k", pa.string(), nullable=False), pa.field("v", kt))
    )


def polars_side(s: dict[str, str], payload: str, source: str, values: dict[str, int]) -> Any:
    import polars as pl

    inner = pl.Struct([pl.Field(n, pl.Int64) for n in ordered(s["order"])])
    elem = inner if payload == "struct" else pl.List(inner)
    ev = {n: values[n] for n in ordered(s["order"])}
    if payload != "struct":
        ev = [ev]
    if source == "polars_array" and s["kind"] in ("fsl1", "fsl2"):
        dtype = pl.Array(elem, width(s["kind"]))
        row = [ev] * width(s["kind"])
    else:
        dtype = pl.List(elem)
        row = [ev]
    return pl.DataFrame({"s": [row]}, schema={"s": dtype})


def struct_sql(order: str) -> str:
    return "Struct(" + ", ".join(f'"{n}": Int64' for n in ordered(order)) + ")"


def kind_sql(kind: str, elem: str) -> str:
    if kind == "list":
        return f"List({elem})"
    if kind == "large":
        return f"LargeList({elem})"
    if kind == "fsl1":
        return f"FixedSizeList(1, {elem})"
    if kind == "fsl2":
        return f"FixedSizeList(2, {elem})"
    if kind == "view":
        return f"ListView({elem})"
    return f"LargeListView({elem})"


def named_struct_sql(order: str, values: dict[str, int]) -> str:
    parts = [f"'{n}', {values[n]}" for n in ordered(order)]
    return "named_struct(" + ", ".join(parts) + ")"


def native_sql(s: dict[str, str], payload: str, values: dict[str, int]) -> str:
    struct_expr = named_struct_sql(s["order"], values)
    inner = struct_expr if payload == "struct" else f"make_array({struct_expr})"
    rowexpr = f"make_array({inner})" if width(s["kind"]) == 1 else f"make_array({inner}, {inner})"
    elem = struct_sql(s["order"]) if payload == "struct" else f"List({struct_sql(s['order'])})"
    kt = kind_sql(s["kind"], elem)
    final = f"Dictionary(Int32, {kt})" if s["enc"] == "dict" else kt
    return f"SELECT arrow_cast({rowexpr}, '{final}') AS s"


def apply_df(op: str, a: Any, b: Any) -> Any:
    if op == "union_all":
        return a.union(b)
    if op == "union":
        return a.union(b).distinct()
    if op == "intersect":
        return a.intersect(b)
    if op == "intersect_all":
        return a.intersectAll(b)
    if op == "except":
        return a.subtract(b)
    if op == "except_all":
        return a.exceptAll(b)
    return a.unionByName(b)


class RepEnv:
    def __init__(self, leg: str) -> None:
        import repark
        from repark import ReparkSession
        from repark._native import PyReparkSession

        self.repark = repark
        self.facade = ReparkSession.builder.appName("variant-" + leg).getOrCreate()
        repark._ANSI_NATIVE = PyReparkSession.native(config={})
        self.root = WORK / leg
        self.root.mkdir(parents=True, exist_ok=True)
        self.serial = 0

    def next_serial(self) -> int:
        self.serial += 1
        return self.serial

    def catalog(self) -> tuple[str, Path]:
        n = self.next_serial()
        cat = f"ice{n}"
        root = self.root / f"c{n}"
        self.facade.register_memory_catalog(cat, str(root))
        self.facade.sql(f"CREATE NAMESPACE {cat}.ns LOCATION '{root / 'ns'}'").collect()
        return cat, root

    def facade_frames(self, c: dict[str, Any]) -> tuple[Any, Any]:
        left_side, right_side = c["L"], c["R"]
        if c["source"] == "pandas":
            left = self.facade.createDataFrame(
                to_pandas(side_array(left_side, c["payload"], c["shape"], VALS["L"]))
            )
            right = self.facade.createDataFrame(
                to_pandas(side_array(right_side, c["payload"], c["shape"], VALS["R"]))
            )
            return left, right
        if c["source"] in ("polars_df", "polars_array"):
            left = self.facade.createDataFrame(
                polars_side(left_side, c["payload"], c["source"], VALS["L"])
            )
            right = self.facade.createDataFrame(
                polars_side(right_side, c["payload"], c["source"], VALS["R"])
            )
            return left, right
        cat, _ = self.catalog()
        self.facade.sql(
            f"CREATE TABLE {cat}.ns.t (s ARRAY<STRUCT<a: BIGINT, b: BIGINT>>) USING iceberg"
        ).collect()
        self.facade.sql(f"INSERT INTO {cat}.ns.t SELECT array({RAB}) AS s").collect()
        left = self.facade.sql(f"SELECT s FROM {cat}.ns.t")
        right = self.facade.createDataFrame(
            polars_side(right_side, c["payload"], "polars_df", VALS["R"])
        )
        return left, right


def repark_set(c: dict[str, Any], env: RepEnv) -> dict[str, Any]:
    rp = env.repark
    types = {}
    if c["door"] == "native":
        lsql = native_sql(c["L"], c["payload"], VALS["L"])
        rsql = native_sql(c["R"], c["payload"], VALS["R"])
        for name, sql in (("L", lsql), ("R", rsql)):
            value, err = step("ingest:" + name, lambda sql=sql: rp.sql(sql).toArrow())
            if err:
                return refused(err, types)
            types[name] = str(value.schema.field("s").type)
        if c["op"] == "rcte":
            final = (
                f"WITH RECURSIVE r(s) AS ({lsql} UNION ALL SELECT s FROM r WHERE FALSE) "
                f"SELECT s FROM r UNION ALL {rsql}"
            )
        else:
            final = f"{lsql} {OP_SQL[c['op']]} {rsql}"
        out, err = step("op", lambda: rp.sql(final).toArrow())
        if err:
            return refused(err, types)
        return answered(out.to_pylist(), types)
    frames, err = step("ingest", lambda: env.facade_frames(c))
    if err:
        return refused(err, types)
    left, right = frames
    for name, frame in (("L", left), ("R", right)):
        value, err = step("ingest:" + name, lambda frame=frame: frame.toArrow())
        if err:
            return refused(err, types)
        types[name] = str(value.schema.field("s").type)
    if c["door"] == "sql_view":
        left.createOrReplaceTempView("l")
        right.createOrReplaceTempView("r")
        frame, err = step(
            "plan", lambda: env.facade.sql(f"SELECT s FROM l {OP_SQL[c['op']]} SELECT s FROM r")
        )
    else:
        frame, err = step("plan", lambda: apply_df(c["op"], left, right))
    if err:
        return refused(err, types)
    if c["effect"] == "insert":
        cat, root = env.catalog()
        _, err = step(
            "ddl",
            lambda: env.facade.sql(
                f"CREATE TABLE {cat}.ns.t (s ARRAY<STRUCT<a: BIGINT, b: BIGINT>>) USING iceberg"
            ).collect(),
        )
        if err:
            return refused(err, types)
        before = count_files(root)
        _, err = step("op", lambda: frame.write.insertInto(f"{cat}.ns.t"))
        after = count_files(root)
        if err:
            return refused(err, types, files=[before, after])
        persisted, err = step(
            "read", lambda: env.facade.sql(f"SELECT s FROM {cat}.ns.t").toArrow().to_pylist()
        )
        if err:
            return refused(err, types, files=[before, after])
        return answered(persisted, types, persisted=persisted, files=[before, after])
    out, err = step("op", lambda: frame.toArrow().to_pylist())
    if err:
        return refused(err, types)
    return answered(out, types)


def repark_seatc(c: dict[str, Any], env: RepEnv) -> dict[str, Any]:
    left, err = step(
        "ingest:L",
        lambda: env.facade.createDataFrame(
            to_pandas(side_array(c["L"], c["payload"], "top", VALS["L"]))
        ),
    )
    if err:
        return refused(err)
    right, err = step(
        "ingest:R",
        lambda: env.facade.createDataFrame(
            to_pandas(side_array(c["R"], c["payload"], "top", VALS["L"]))
        ),
    )
    if err:
        return refused(err)
    left.createOrReplaceTempView("l")
    right.createOrReplaceTempView("r")
    out, err = step("op", lambda: env.facade.sql(SEATC_SQL[c["cop"]]).toArrow().to_pylist())
    if err:
        return refused(err)
    return answered(out)


def repark_dml(c: dict[str, Any], env: RepEnv) -> dict[str, Any]:
    cat, root = env.catalog()
    table = f"{cat}.ns.d"
    ty = DML_TYPE[c["payload"]]
    first, pred, second = DML_LIT[c["payload"]]
    _, err = step(
        "ddl",
        lambda: env.facade.sql(f"CREATE TABLE {table} (id INT, s {ty}) USING iceberg").collect(),
    )
    if err:
        return refused(err)
    _, err = step(
        "ddl",
        lambda: env.facade.sql(f"INSERT INTO {table} VALUES (1, {first}), (2, {second})").collect(),
    )
    if err:
        return refused(err)
    before = count_files(root)
    _, err = step("op", lambda: env.facade.sql(f"DELETE FROM {table} WHERE s = {pred}").collect())
    after = count_files(root)
    persisted, perr = step(
        "read", lambda: env.facade.sql(f"SELECT id FROM {table} ORDER BY id").toArrow().to_pylist()
    )
    if err:
        return refused(err, persisted=persisted, files=[before, after])
    if perr:
        return refused(perr, files=[before, after])
    return answered(persisted, persisted=persisted, files=[before, after])


def repark_path(c: dict[str, Any], env: RepEnv) -> dict[str, Any]:
    n = env.next_serial()
    location = env.root / f"path{n}"
    table = f"p{n}"
    _, err = step(
        "ddl",
        lambda: env.facade.sql(
            f"CREATE TABLE {table} (id INT, s ARRAY<STRUCT<a: BIGINT, b: BIGINT>>) "
            f"USING parquet LOCATION '{location}'"
        ).collect(),
    )
    if err:
        return refused(err)
    _, err = step("ddl", lambda: env.facade.sql(PATH_INSERT.format(t=table)).collect())
    if err:
        return refused(err)
    stmt = PATH_STMTS[c["stmt"]].format(t=table)
    _, serr = step("op", lambda: env.facade.sql(stmt).collect())
    persisted, perr = step(
        "read",
        lambda: env.facade.sql(f"SELECT id, s FROM {table} ORDER BY id").toArrow().to_pylist(),
    )
    if serr:
        return refused(serr, persisted=persisted)
    if perr:
        return refused(perr)
    return answered(persisted, persisted=persisted)


def repark_fk(c: dict[str, Any], env: RepEnv) -> dict[str, Any]:
    sql = FK_SQL[c["fk"]]
    if c["door"] == "native_sql":
        out, err = step("op", lambda: env.repark.sql(sql).toArrow().to_pylist())
    else:
        out, err = step("op", lambda: env.facade.sql(sql).toArrow().to_pylist())
    if err:
        return refused(err)
    return answered(out)


def repark_cell(c: dict[str, Any], env: RepEnv) -> dict[str, Any]:
    if c["seat"] == "S":
        return repark_set(c, env)
    if c["seat"] == "C":
        return repark_dml(c, env) if c["cop"] == "dml" else repark_seatc(c, env)
    if c["seat"] == "PATH":
        return repark_path(c, env)
    return repark_fk(c, env)


class SparkEnv:
    def __init__(self) -> None:
        from pyspark.sql import SparkSession

        self.spark = (
            SparkSession.builder.master("local[1]")
            .appName("variant-spark")
            .config("spark.jars", JAR)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(WORK / "spark" / "wh"))
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns")
        self.root = WORK / "spark"
        self.root.mkdir(parents=True, exist_ok=True)
        self.serial = 0

    def next_serial(self) -> int:
        self.serial += 1
        return self.serial


def spark_frame(
    spark: Any, s: dict[str, str], payload: str, shape: str, values: dict[str, int]
) -> Any:
    from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

    st = StructType([StructField(n, LongType(), True) for n in ordered(s["order"])])
    elem_t = st if payload == "struct" else ArrayType(st)
    tup = tuple(values[n] for n in ordered(s["order"]))
    ev = tup if payload == "struct" else [tup]
    if shape == "top":
        typ, val = ArrayType(elem_t), [ev]
    elif shape == "struct":
        typ, val = StructType([StructField("x", ArrayType(elem_t), True)]), ([ev],)
    elif shape == "list":
        typ, val = ArrayType(ArrayType(elem_t)), [[ev]]
    else:
        typ, val = MapType(StringType(), ArrayType(elem_t)), {"k": [ev]}
    return spark.createDataFrame([(val,)], StructType([StructField("s", typ, True)]))


def spark_set(c: dict[str, Any], env: SparkEnv) -> dict[str, Any]:
    sp = env.spark
    if c["op"] == "rcte":
        lit_l = named_struct_sql(c["L"]["order"], VALS["L"])
        lit_r = named_struct_sql(c["R"]["order"], VALS["R"])
        sql = (
            f"WITH RECURSIVE r(s) AS (SELECT array({lit_l}) AS s UNION ALL "
            f"SELECT s FROM r WHERE FALSE) SELECT s FROM r UNION ALL SELECT array({lit_r}) AS s"
        )
        out, err = step("op", lambda: [plain_spark(row) for row in sp.sql(sql).collect()])
        if err:
            return refused(err)
        return answered(out)
    types = {}
    left, err = step(
        "ingest:L", lambda: spark_frame(sp, c["L"], c["payload"], c["shape"], VALS["L"])
    )
    if err:
        return refused(err)
    right, err = step(
        "ingest:R", lambda: spark_frame(sp, c["R"], c["payload"], c["shape"], VALS["R"])
    )
    if err:
        return refused(err)
    types["L"] = left.schema["s"].dataType.simpleString()
    types["R"] = right.schema["s"].dataType.simpleString()
    frame, err = step("plan", lambda: apply_df(c["op"], left, right))
    if err:
        return refused(err, types)
    if c["effect"] == "insert":
        n = env.next_serial()
        name = f"t{n}"
        root = env.root / "wh" / "ns" / name
        _, err = step(
            "ddl",
            lambda: sp.sql(
                f"CREATE TABLE ice.ns.{name} (s ARRAY<STRUCT<a: BIGINT, b: BIGINT>>) USING iceberg"
            ),
        )
        if err:
            return refused(err, types)
        before = count_files(root)
        frame.createOrReplaceTempView("u")
        _, err = step("op", lambda: sp.sql(f"INSERT INTO ice.ns.{name} SELECT s FROM u"))
        after = count_files(root)
        if err:
            return refused(err, types, files=[before, after])
        persisted, err = step(
            "read",
            lambda: [plain_spark(row) for row in sp.sql(f"SELECT s FROM ice.ns.{name}").collect()],
        )
        if err:
            return refused(err, types, files=[before, after])
        return answered(persisted, types, persisted=persisted, files=[before, after])
    out, err = step("op", lambda: [plain_spark(row) for row in frame.collect()])
    if err:
        return refused(err, types)
    return answered(out, types)


def spark_seatc(c: dict[str, Any], env: SparkEnv) -> dict[str, Any]:
    sp = env.spark
    left, err = step("ingest:L", lambda: spark_frame(sp, c["L"], c["payload"], "top", VALS["L"]))
    if err:
        return refused(err)
    right, err = step("ingest:R", lambda: spark_frame(sp, c["R"], c["payload"], "top", VALS["L"]))
    if err:
        return refused(err)
    left.createOrReplaceTempView("l")
    right.createOrReplaceTempView("r")
    out, err = step(
        "op", lambda: [plain_spark(row) for row in sp.sql(SEATC_SQL[c["cop"]]).collect()]
    )
    if err:
        return refused(err)
    return answered(out)


def spark_dml(c: dict[str, Any], env: SparkEnv) -> dict[str, Any]:
    sp = env.spark
    n = env.next_serial()
    table = f"ice.ns.d{n}"
    root = env.root / "wh" / "ns" / f"d{n}"
    ty = DML_TYPE[c["payload"]]
    first, pred, second = DML_LIT[c["payload"]]
    _, err = step("ddl", lambda: sp.sql(f"CREATE TABLE {table} (id INT, s {ty}) USING iceberg"))
    if err:
        return refused(err)
    _, err = step("ddl", lambda: sp.sql(f"INSERT INTO {table} VALUES (1, {first}), (2, {second})"))
    if err:
        return refused(err)
    before = count_files(root)
    _, err = step("op", lambda: sp.sql(f"DELETE FROM {table} WHERE s = {pred}"))
    after = count_files(root)
    persisted, perr = step(
        "read",
        lambda: [
            plain_spark(row) for row in sp.sql(f"SELECT id FROM {table} ORDER BY id").collect()
        ],
    )
    if err:
        return refused(err, persisted=persisted, files=[before, after])
    if perr:
        return refused(perr, files=[before, after])
    return answered(persisted, persisted=persisted, files=[before, after])


def spark_path(c: dict[str, Any], env: SparkEnv) -> dict[str, Any]:
    sp = env.spark
    n = env.next_serial()
    location = env.root / "paths" / f"path{n}"
    table = f"p{n}"
    _, err = step(
        "ddl",
        lambda: sp.sql(
            f"CREATE TABLE {table} (id INT, s ARRAY<STRUCT<a: BIGINT, b: BIGINT>>) "
            f"USING parquet LOCATION '{location}'"
        ),
    )
    if err:
        return refused(err)
    _, err = step("ddl", lambda: sp.sql(PATH_INSERT.format(t=table)))
    if err:
        return refused(err)
    stmt = PATH_STMTS[c["stmt"]].format(t=table)
    _, serr = step("op", lambda: sp.sql(stmt).collect())
    persisted, perr = step(
        "read",
        lambda: [
            plain_spark(row) for row in sp.sql(f"SELECT id, s FROM {table} ORDER BY id").collect()
        ],
    )
    if serr:
        return refused(serr, persisted=persisted)
    if perr:
        return refused(perr)
    return answered(persisted, persisted=persisted)


def spark_fk(c: dict[str, Any], env: SparkEnv) -> dict[str, Any]:
    out, err = step(
        "op", lambda: [plain_spark(row) for row in env.spark.sql(FK_SQL[c["fk"]]).collect()]
    )
    if err:
        return refused(err)
    return answered(out)


def spark_cell(c: dict[str, Any], env: SparkEnv) -> dict[str, Any]:
    if c["seat"] == "S":
        return spark_set(c, env)
    if c["seat"] == "C":
        return spark_dml(c, env) if c["cop"] == "dml" else spark_seatc(c, env)
    if c["seat"] == "PATH":
        return spark_path(c, env)
    return spark_fk(c, env)


def record(c: dict[str, Any], res: dict[str, Any], reuse: bool, seconds: float) -> dict[str, Any]:
    out = {
        "cell": c["id"],
        "plan": c["plan"],
        "leg": c["leg"],
        "seat": c["seat"],
        "payload": c["payload"],
        "shape": c["shape"],
        "op": c["op"],
        "door": c["door"],
        "source": c["source"],
        "effect": c["effect"],
        "orient": c["orient"],
        "stmt": c["stmt"],
        "fk": c["fk"],
        "cop": c["cop"],
        "L": c["L"],
        "R": c["R"],
        "skey": skey(c),
        "tkey": tkey(c),
        "reuse": reuse,
        "seconds": round(seconds, 3),
    }
    out.update(res)
    return out


def run_leg(leg: str, cells: list[dict[str, Any]]) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)
    env = SparkEnv() if leg == "spark" else RepEnv(leg)
    cache = {}
    path = RAW / f"{leg}.jsonl"
    tally = {}
    with path.open("w", encoding="utf-8") as handle:
        for c in cells:
            start = time.time()
            reuse = False
            try:
                if leg == "spark":
                    key = skey(c)
                    if key in cache:
                        res = cache[key]
                        reuse = True
                    else:
                        res = spark_cell(c, env)
                        cache[key] = res
                else:
                    res = repark_cell(c, env)
            except BaseException as error:
                res = refused(
                    {
                        "phase": "HARNESS",
                        "cls": type(error).__name__,
                        "message": str(error).replace("\n", " ")[:200],
                    }
                )
            handle.write(json.dumps(record(c, res, reuse, time.time() - start), default=str) + "\n")
            handle.flush()
            bucket = tally.setdefault(c["plan"], [0, 0])
            bucket[0 if res["answered"] else 1] += 1
    for plan, (ok, bad) in sorted(tally.items()):
        print(f"{leg} {plan} answered={ok} refused={bad}")
    print(f"{leg} total={sum(a + b for a, b in tally.values())}")


def load(leg: str) -> list[dict[str, Any]]:
    path = RAW / f"{leg}.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def compare(a: dict[str, Any], b: dict[str, Any]) -> str:
    if a["answered"] and b["answered"]:
        if a["rows"] == b["rows"]:
            return "EQUAL"
        if len(a["rows"]) != len(b["rows"]):
            return "ROWS-DIFFER"
        return "RESULT-ROWS-DIFFER"
    if a["answered"]:
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if b["answered"]:
        return "REFUSED-WHERE-SPARK-ACCEPTED"
    return "BOTH-REFUSED"


def twin_compare(a: dict[str, Any], t: dict[str, Any] | None) -> str:
    if a["answered"] and t["answered"]:
        return "INVARIANT" if a["rows"] == t["rows"] else "VARIANT-DIFFERS"
    if not a["answered"] and not t["answered"]:
        return "INVARIANT"
    return "VARIANT-DIFFERS"


def kind_label(s: dict[str, str]) -> str:
    return f"{s['kind']}/{s['enc']}/{s['order']}/{s['names']}"


def outcome_text(r: dict[str, Any]) -> str:
    if r["answered"]:
        return f"ANSWERED rows={len(r['rows'])}"
    err = r["error"]
    return f"REFUSED {err['phase']} {err['cls']}: {err['message']}"


def render() -> None:
    legs = {leg: load(leg) for leg in LEGS}
    spark_by_key = {}
    for r in legs["spark"]:
        spark_by_key.setdefault(r["skey"], r)
    by_tkey = {}
    for leg in LEGS:
        for r in legs[leg]:
            by_tkey.setdefault(r["tkey"], r)
    rows = []
    for leg in LEGS:
        for r in legs[leg]:
            if leg == "spark":
                label = "SELF"
            else:
                s = spark_by_key.get(r["skey"])
                label = "ORACLE-MISSING" if s is None else compare(r, s)
            twin = by_tkey.get(r["tkey"])
            if twin is None:
                tlabel = "NO-TWIN"
            elif twin is r:
                tlabel = "INVARIANT"
            else:
                tlabel = twin_compare(r, twin)
            rows.append((r, label, tlabel))
    out = []
    out.append("# Variant table: measured results\n")
    out.append(
        "Generated by `variant_probe.py --render` from `raw/<leg>.jsonl`. Labels: see notes.md.\n"
    )
    out.append("## Cells per leg and plan\n")
    out.append("| plan | spark | base | head |")
    out.append("|---|---|---|---|")
    plans = sorted({r["plan"] for r, _, _ in rows})
    for p in plans:
        cnt = {
            leg: sum(1 for r, _, _ in rows if r["plan"] == p and r["leg"] == leg) for leg in LEGS
        }
        out.append(f"| {p} | {cnt['spark']} | {cnt['base']} | {cnt['head']} |")
    out.append(
        f"| total | {sum(1 for r, _, _ in rows if r['leg'] == 'spark')} | "
        f"{sum(1 for r, _, _ in rows if r['leg'] == 'base')} | "
        f"{sum(1 for r, _, _ in rows if r['leg'] == 'head')} |\n"
    )
    out.append("## Label counts per seat and leg (label vs Spark twin)\n")
    seat_labels = {}
    for r, label, _ in rows:
        seat_labels.setdefault((r["seat"], r["leg"]), {}).setdefault(label, 0)
        seat_labels[(r["seat"], r["leg"])][label] += 1
    out.append("| seat | leg | label | count |")
    out.append("|---|---|---|---|")
    for key in sorted(seat_labels):
        for label, n in sorted(seat_labels[key].items()):
            out.append(f"| {key[0]} | {key[1]} | {label} | {n} |")
    out.append("")
    out.append(
        "## Kind-invariance labels per seat and leg (cell vs its List twin, same leg and door)\n"
    )
    inv = {}
    for r, _, tlabel in rows:
        inv.setdefault((r["seat"], r["leg"]), {}).setdefault(tlabel, 0)
        inv[(r["seat"], r["leg"])][tlabel] += 1
    out.append("| seat | leg | label | count |")
    out.append("|---|---|---|---|")
    for key in sorted(inv):
        for label, n in sorted(inv[key].items()):
            out.append(f"| {key[0]} | {key[1]} | {label} | {n} |")
    out.append("")
    out.append("## Seat S: label x kind pair x operation x door x leg (Spark label)\n")
    group = {}
    for r, label, _ in rows:
        if r["seat"] != "S" or r["leg"] == "spark":
            continue
        key = (r["leg"], r["door"], r["op"], r["L"]["kind"], r["R"]["kind"])
        group.setdefault(key, {}).setdefault(label, 0)
        group[key][label] += 1
    out.append("| leg | door | op | left kind | right kind | labels |")
    out.append("|---|---|---|---|---|---|")
    for key in sorted(group):
        tally = "; ".join(f"{k} {v}" for k, v in sorted(group[key].items()))
        out.append(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {tally} |")
    out.append("")
    out.append("## Hidden on head (seat S): answered where Spark differs or refused\n")
    hidden = {}
    for r, label, _ in rows:
        if r["leg"] != "head" or r["seat"] != "S":
            continue
        if not r["answered"]:
            continue
        if label not in ("RESULT-ROWS-DIFFER", "ROWS-DIFFER", "ACCEPTED-WHERE-SPARK-REFUSED"):
            continue
        sig = (
            label,
            r["op"],
            r["door"],
            r["source"],
            r["effect"],
            r["payload"],
            r["shape"],
            kind_label(r["L"]),
            kind_label(r["R"]),
        )
        hidden.setdefault(sig, []).append(r["cell"])
    out.append(f"Hidden groups: {len(hidden)}, cells: {sum(len(v) for v in hidden.values())}\n")
    out.append("| label | op | door | source | effect | payload | shape | left | right | cells |")
    out.append("|---|---|---|---|---|---|---|---|---|---|")
    for sig in sorted(hidden):
        ids = hidden[sig]
        shown = f"{len(ids)}: {', '.join(ids[:4])}{' ...' if len(ids) > 4 else ''}"
        out.append(
            f"| {sig[0]} | {sig[1]} | {sig[2]} | {sig[3]} | {sig[4]} | "
            f"{sig[5]} | {sig[6]} | {sig[7]} | {sig[8]} | {shown} |"
        )
    out.append("")
    out.append("## Seat C (comparisons): non-EQUAL and VARIANT-DIFFERS groups\n")
    seatc = {}
    for r, label, tlabel in rows:
        if r["seat"] != "C":
            continue
        if r["leg"] == "base" and (label != "EQUAL" or tlabel != "INVARIANT"):
            key = (r["leg"], r["cop"], r["payload"], label, tlabel)
            seatc.setdefault(key, []).append(f"{r['L']['kind']}-{r['R']['kind']}")
    out.append("| leg | op | payload | label | twin label | count | kind pairs |")
    out.append("|---|---|---|---|---|---|---|")
    for key in sorted(seatc):
        pairs = seatc[key]
        shown = f"{', '.join(pairs[:12])}{' ...' if len(pairs) > 12 else ''}"
        out.append(
            f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {len(pairs)} | {shown} |"
        )
    out.append("")
    out.append("## Path-table DML control rows and F-K rows\n")
    out.append("| cell | leg | id | label | outcome | persisted |")
    out.append("|---|---|---|---|---|---|")
    for r, label, _tlabel in rows:
        if r["seat"] in ("PATH", "FK") or (r["seat"] == "C" and r["cop"] == "dml"):
            outcome = outcome_text(r).replace("|", "/")
            persisted = (r["persisted"] or [])[:4]
            out.append(
                f"| {r['cell']} | {r['leg']} | {r['stmt'] or r['fk'] or r['cop']} | "
                f"{label} | {outcome} | {persisted} |"
            )
    out.append("")
    out.append(
        "## Memo section 1.2 cross-check (seat S, head; union = union_all, subtract = except)\n"
    )
    out.append("| left | right | memo union | head union_all | memo subtract | head except |")
    out.append("|---|---|---|---|---|---|")
    head_u = {}
    head_e = {}
    for r in legs["head"]:
        if (
            r["seat"] != "S"
            or r["plan"] not in ("P1", "P13")
            or r["door"] != "df"
            or r["source"] != "pandas"
        ):
            continue
        if (
            r["payload"] != "struct"
            or r["shape"] != "top"
            or r["effect"] != "read"
            or r["orient"] != "fwd"
        ):
            continue
        if r["L"]["enc"] != "plain" or r["R"]["enc"] != "plain":
            continue
        key = (r["L"]["kind"], r["R"]["kind"])
        if r["op"] == "union_all":
            head_u[key] = r
        if r["op"] == "except":
            head_e[key] = r
    memo_ok = []
    for left, right, mu, ms in MEMO_TABLE:
        hu = head_u.get((left, right))
        he = head_e.get((left, right))
        hu_text = "missing" if hu is None else ("answered" if hu["answered"] else "refused")
        he_text = "missing" if he is None else ("answered" if he["answered"] else "refused")
        out.append(f"| {left} | {right} | {mu} | {hu_text} | {ms} | {he_text} |")
        memo_ok.append((left, right, mu, hu_text, ms, he_text))
    out.append("")
    out.append(
        'Memo claim "hidden" is answered; "coercion error" in the memo\'s rows '
        "is recorded as refused here.\n"
    )
    out.append("## Per-cell rows\n")
    out.append(
        "| cell | plan | seat | leg | door | source | op | effect | payload | shape | "
        "left | right | outcome | label vs Spark | kind-invariance label | types L | types R |"
    )
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r, label, tlabel in rows:
        left = kind_label(r["L"]) if r["seat"] == "S" else "-"
        right = kind_label(r["R"]) if r["seat"] == "S" else "-"
        types = r.get("types") or {}
        outcome = outcome_text(r).replace("|", "/")
        type_l = str(types.get("L", "-")).replace("|", "/")
        type_r = str(types.get("R", "-")).replace("|", "/")
        out.append(
            f"| {r['cell']} | {r['plan']} | {r['seat']} | {r['leg']} | {r['door']} | "
            f"{r['source']} | {r['op']} | {r['effect']} | {r['payload']} | {r['shape']} | "
            f"{left} | {right} | {outcome} | {label} | {tlabel} | {type_l} | {type_r} |"
        )
    (OUT / "table.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"rendered {len(rows)} cells to {OUT / 'table.md'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=LEGS)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only", default="")
    args = parser.parse_args()
    generate()
    if args.render:
        render()
        return 0
    if not args.leg:
        print(f"planned cells: {len(CELLS)}")
        for leg in LEGS:
            print(leg, sum(1 for c in CELLS if c["leg"] == leg))
        return 0
    cells = [c for c in CELLS if c["leg"] == args.leg]
    if args.only:
        keep = set(args.only.split(","))
        cells = [c for c in cells if c["plan"] in keep]
    if args.limit:
        cells = cells[: args.limit]
    run_leg(args.leg, cells)
    return 0


if __name__ == "__main__":
    sys.exit(main())
