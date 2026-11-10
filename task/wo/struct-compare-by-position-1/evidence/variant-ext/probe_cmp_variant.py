import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import pyarrow as pa

OUT = Path(__file__).resolve().parent
RAW = OUT / "raw"
WORK = OUT / "work"
JAR = os.environ.get("STRUCT_CMP_IVY_JAR", "<ivy-jar>")
LEGS = ["spark", "base"]
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
COPS = ["eq", "in", "join"]
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
DEPTHS = ["struct", "map_std_reord", "map_ren_reord"]
RBA = "named_struct('b', CAST(2 AS BIGINT), 'a', CAST(1 AS BIGINT))"
RAB = "named_struct('a', CAST(1 AS BIGINT), 'b', CAST(2 AS BIGINT))"
VAL = {"a": 1, "b": 2}


def seat_sql(cop: Any, value_path: Any = "") -> Any:
    lhs = "l.s" + value_path
    rhs = "r.s" + value_path
    if cop == "eq":
        return f"SELECT {lhs} = {rhs} AS x FROM l CROSS JOIN r"
    if cop == "in":
        return f"SELECT {lhs} IN ({rhs}) AS x FROM l CROSS JOIN r"
    return f"SELECT {lhs} AS x FROM l JOIN r ON {lhs} = {rhs}"


def map_path(c: Any) -> Any:
    return "['k']" if c["shape"] == "map" else ""


SEATC_NATIVE = {
    "eq": "SELECT l.s = r.s AS x FROM l CROSS JOIN r",
    "in": "SELECT EXISTS (SELECT 1 FROM r WHERE r.s = l.s) AS x FROM l",
    "join": "SELECT l.s AS x FROM l JOIN r ON l.s = r.s",
}
DML_STMTS = {
    "update": "UPDATE {t} SET id = 9 WHERE s = {pred}",
    "delete": "DELETE FROM {t} WHERE s = {pred}",
}
DML_CREATE = (
    "CREATE TABLE {t} (id INT, s ARRAY<STRUCT<a: BIGINT, b: BIGINT>>) USING iceberg "
    "TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write')"
)
DML_FIXTURE = (
    "INSERT INTO {t} VALUES (1, array(" + RAB + ")), "
    "(2, array(named_struct('a', CAST(3 AS BIGINT), 'b', CAST(4 AS BIGINT))))"
)
FIXTURE_IDS = [1, 2]
EXPR_LITERAL_MARK = "Cannot create expression literal"

CELLS = []


def side(kind: Any, enc: Any = "plain", order: Any = "AB", names: Any = "std") -> Any:
    return {"kind": kind, "enc": enc, "order": order, "names": names}


def add(plan: Any, legs: Any, **fields: Any) -> Any:
    for leg in legs:
        cell = {
            "plan": plan,
            "leg": leg,
            "seat": "C",
            "cop": None,
            "payload": "struct",
            "shape": "top",
            "depth": "top",
            "op": None,
            "door": "sql_view",
            "source": "pandas",
            "effect": "read",
            "orient": "fwd",
            "stmt": None,
            "pred": None,
            "enc": None,
        }
        cell.update(fields)
        cell["id"] = f"{plan}-{len(CELLS) + 1:05d}"
        CELLS.append(cell)


def add_c(plan: Any, legs: Any, cop: Any, left: Any, right: Any, **extra: Any) -> Any:
    add(plan, legs, seat="C", cop=cop, op=cop, L=left, R=right, **extra)


def depth_sides(a: Any, b: Any, depth: Any) -> Any:
    if depth in MAP_VARIANTS:
        order, names = MAP_VARIANTS[depth]
        return side(a, "plain", "AB", "std"), side(b, "plain", order, names), "map"
    return side(a, "plain", "AB"), side(b, "plain", "BA"), "struct"


def generate() -> Any:
    CELLS.clear()
    for cop in COPS:
        add_c("P30", LEGS, cop, side("list", "plain", "AB"), side("list", "plain", "BA"))
    for a, b in PAIRS:
        for cop in COPS:
            add_c("P31", LEGS, cop, side(a, "plain", "BA"), side(b, "plain", "AB"), orient="rev")
    for enc, (el, er) in ENCODED.items():
        for cop in COPS:
            add_c("P32", LEGS, cop, side("list", el, "AB"), side("list", er, "BA"), enc=enc)
    for a, b in REPS:
        for depth in DEPTHS:
            left, right, key = depth_sides(a, b, depth)
            for cop in COPS:
                add_c("P33", LEGS, cop, left, right, shape=key, depth=depth)
    for a, b in REPS:
        for cop in COPS:
            add_c("P34", LEGS, cop, side(a, "plain", "AB"), side(b, "plain", "AB"))
    for a, b in REPS:
        for cop in COPS:
            add_c("P35", LEGS, cop, side(a, "plain", "AB"), side(b, "plain", "BA"), door="df")
    for a, b in REPS:
        for cop in COPS:
            add_c(
                "P36",
                ["base"],
                cop,
                side(a, "plain", "AB"),
                side(b, "plain", "BA"),
                door="native",
                source="arrow_cast",
            )
    add_c(
        "P37",
        ["base"],
        "eq",
        side("list", "dict", "AB"),
        side("list", "dict", "BA"),
        door="native",
        source="arrow_cast",
        enc="dict_ee",
    )
    for stmt in DML_STMTS:
        for kind in ["list", "large"]:
            for pred in ["reord", "same"]:
                for leg in LEGS:
                    add(
                        "P38",
                        [leg],
                        seat="DML",
                        cop="dml",
                        op=stmt,
                        stmt=stmt,
                        pred=pred,
                        door="dml",
                        source="literal",
                        effect="dml",
                        L=side("list", "plain", "AB"),
                        R=side(kind, "plain", "BA" if pred == "reord" else "AB"),
                    )
    return CELLS


def ordered(order: Any) -> Any:
    return ["a", "b"] if order == "AB" else ["b", "a"]


def width(kind: Any) -> Any:
    return 2 if kind == "fsl2" else 1


def skey(c: Any) -> Any:
    if c["seat"] == "DML":
        return f"DML|{c['stmt']}|{c['pred']}"
    door = "df" if c["door"] == "df" else "sql"
    return f"C|{c['cop']}|{c['payload']}|{c['shape']}|{c['L']['order']}|{c['R']['order']}|{door}"


def tkey(c: Any) -> Any:
    def strip(s: Any) -> Any:
        out = dict(s)
        out["kind"] = "list"
        out["enc"] = "plain"
        return out

    return json.dumps(
        {
            "leg": c["leg"],
            "seat": c["seat"],
            "payload": c["payload"],
            "shape": c["shape"],
            "depth": c["depth"],
            "orient": c["orient"],
            "op": c["op"],
            "effect": c["effect"],
            "door": c["door"],
            "source": c["source"],
            "stmt": c["stmt"],
            "pred": c["pred"],
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
        return {
            name: plain_spark(item) for name, item in zip(value.__fields__, value, strict=False)
        }
    if isinstance(value, dict):
        return {str(k): plain_spark(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain_spark(item) for item in value]
    return value


def rowset(rows: Any) -> Any:
    return sorted(json.dumps(canon_py(row), sort_keys=True, default=str) for row in rows)


def step(phase: Any, call: Any) -> Any:
    try:
        return call(), None
    except BaseException as error:
        return None, {
            "phase": phase,
            "cls": type(error).__name__,
            "message": str(error).replace("\n", " ")[:800],
        }


def answered_result(rows: Any, types: Any = None) -> Any:
    return {
        "answered": True,
        "rows": rowset(rows),
        "types": types or {},
        "persisted": None,
        "files": None,
        "error": None,
        "derived": None,
    }


def refused_result(err: Any, types: Any = None) -> Any:
    return {
        "answered": False,
        "rows": None,
        "types": types or {},
        "persisted": None,
        "files": None,
        "error": err,
        "derived": None,
    }


def dml_result(answered: Any, err: Any, persisted: Any, files: Any, derived: Any = None) -> Any:
    return {
        "answered": answered,
        "rows": None,
        "types": {},
        "persisted": persisted,
        "files": files,
        "error": err,
        "derived": derived,
    }


def to_pandas(arr: Any) -> Any:
    import pandas

    return pa.table({"s": arr}).to_pandas(types_mapper=pandas.ArrowDtype)


def arrow_struct(order: Any) -> Any:
    return pa.struct([(n, pa.int64()) for n in ordered(order)])


def arrow_elem(payload: Any, order: Any) -> Any:
    st = arrow_struct(order)
    return st if payload == "struct" else pa.list_(st)


def arrow_elem_value(payload: Any, values: Any, order: Any) -> Any:
    row = {n: values[n] for n in ordered(order)}
    return row if payload == "struct" else [row]


def kind_arrow(kind: Any, elem: Any) -> Any:
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


def side_array(s: Any, payload: Any, shape: Any, values: Any) -> Any:
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
    if s["names"] == "std":
        return pa.array([[("k", row)]], type=pa.map_(pa.string(), kt))
    return pa.array(
        [[("k", row)]], type=pa.map_(pa.field("k", pa.string(), nullable=False), pa.field("v", kt))
    )


def struct_sql(order: Any) -> Any:
    return "Struct(" + ", ".join(f'"{n}": Int64' for n in ordered(order)) + ")"


def kind_sql(kind: Any, elem: Any) -> Any:
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


def named_struct_sql(order: Any, values: Any) -> Any:
    parts = [f"'{n}', {values[n]}" for n in ordered(order)]
    return "named_struct(" + ", ".join(parts) + ")"


def native_sql(s: Any, payload: Any, values: Any) -> Any:
    struct_expr = named_struct_sql(s["order"], values)
    inner = struct_expr if payload == "struct" else f"make_array({struct_expr})"
    rowexpr = f"make_array({inner})" if width(s["kind"]) == 1 else f"make_array({inner}, {inner})"
    elem = struct_sql(s["order"]) if payload == "struct" else f"List({struct_sql(s['order'])})"
    kt = kind_sql(s["kind"], elem)
    final = f"Dictionary(Int32, {kt})" if s["enc"] == "dict" else kt
    return f"SELECT arrow_cast({rowexpr}, '{final}') AS s"


def dml_pred(c: Any, leg: Any) -> Any:
    order = c["R"]["order"]
    lit = RBA if order == "BA" else RAB
    if leg == "spark" or c["R"]["kind"] == "list":
        return "array(" + lit + ")"
    return f"arrow_cast(array({lit}), 'LargeList({struct_sql(order)})')"


def expected_ids(stmt: Any, matched: Any) -> Any:
    if stmt == "delete":
        return sorted(i for i in FIXTURE_IDS if i not in matched)
    return sorted(9 if i in matched else i for i in FIXTURE_IDS)


def seat_df(cop: Any, left: Any, right: Any, fns: Any) -> Any:
    lf = left.select(fns.col("s").alias("ls"))
    rf = right.select(fns.col("s").alias("rs"))
    if cop == "eq":
        return lf.crossJoin(rf).select((fns.col("ls") == fns.col("rs")).alias("x"))
    if cop == "in":
        return lf.crossJoin(rf).select(fns.col("ls").isin(fns.col("rs")).alias("x"))
    return lf.join(rf, fns.col("ls") == fns.col("rs")).select(fns.col("ls").alias("x"))


def repark_functions() -> Any:
    from repark.spark import functions

    return functions


class RepEnv:
    def __init__(self, leg: Any) -> None:
        import repark
        from repark import ReparkSession
        from repark._native import PyReparkSession

        self.repark = repark
        self.facade = ReparkSession.builder.appName("cmpvariant-" + leg).getOrCreate()
        repark._ANSI_NATIVE = PyReparkSession.native(config={})
        self.root = WORK / leg
        self.root.mkdir(parents=True, exist_ok=True)
        self.serial = 0

    def next_serial(self) -> Any:
        self.serial += 1
        return self.serial

    def catalog(self) -> Any:
        n = self.next_serial()
        cat = f"ice{n}"
        root = self.root / f"c{n}"
        self.facade.register_memory_catalog(cat, str(root))
        self.facade.sql(f"CREATE NAMESPACE {cat}.ns LOCATION '{root / 'ns'}'").collect()
        return cat


def repark_seatc(c: Any, env: Any) -> Any:
    rp = env.repark
    types = {}
    payload, shape = c["payload"], c["shape"]
    if c["door"] == "native":
        lsql = native_sql(c["L"], payload, VAL)
        rsql = native_sql(c["R"], payload, VAL)
        for name, sql in (("L", lsql), ("R", rsql)):
            value, err = step("ingest:" + name, lambda sql=sql: rp.sql(sql).toArrow())
            if err:
                return refused_result(err, types)
            types[name] = str(value.schema.field("s").type)
        sql = f"WITH l AS ({lsql}), r AS ({rsql}) " + SEATC_NATIVE[c["cop"]]
        out, err = step("op", lambda: rp.sql(sql).toArrow().to_pylist())
        if err:
            return refused_result(err, types)
        return answered_result(out, types)
    left, err = step(
        "ingest:L",
        lambda: env.facade.createDataFrame(to_pandas(side_array(c["L"], payload, shape, VAL))),
    )
    if err:
        return refused_result(err, types)
    right, err = step(
        "ingest:R",
        lambda: env.facade.createDataFrame(to_pandas(side_array(c["R"], payload, shape, VAL))),
    )
    if err:
        return refused_result(err, types)
    for name, frame in (("L", left), ("R", right)):
        value, err = step("ingest:" + name, lambda frame=frame: frame.toArrow())
        if err:
            return refused_result(err, types)
        types[name] = str(value.schema.field("s").type)
    if c["door"] == "df":
        frame, err = step("plan", lambda: seat_df(c["cop"], left, right, repark_functions()))
        if err:
            return refused_result(err, types)
        out, err = step("op", lambda: frame.toArrow().to_pylist())
    else:
        left.createOrReplaceTempView("l")
        right.createOrReplaceTempView("r")
        sql = seat_sql(c["cop"], map_path(c))
        out, err = step("op", lambda: env.facade.sql(sql).toArrow().to_pylist())
    if err:
        return refused_result(err, types)
    return answered_result(out, types)


def repark_snapshots(facade: Any, table: Any) -> Any:
    out, err = step(
        "snap",
        lambda: facade.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").toArrow().to_pylist(),
    )
    if err:
        return None
    return int(out[0]["n"])


def repark_dml(c: Any, env: Any) -> Any:
    cat = env.catalog()
    table = f"{cat}.ns.d"
    pred = dml_pred(c, "base")
    _, err = step("ddl", lambda: env.facade.sql(DML_CREATE.format(t=table)).collect())
    if err:
        return dml_result(False, err, None, None)
    _, err = step("ddl", lambda: env.facade.sql(DML_FIXTURE.format(t=table)).collect())
    if err:
        return dml_result(False, err, None, None)
    before = repark_snapshots(env.facade, table)
    _, err = step(
        "op", lambda: env.facade.sql(DML_STMTS[c["stmt"]].format(t=table, pred=pred)).collect()
    )
    after = repark_snapshots(env.facade, table)
    persisted, perr = step(
        "read",
        lambda: [
            row["id"]
            for row in env.facade.sql(f"SELECT id FROM {table} ORDER BY id").toArrow().to_pylist()
        ],
    )
    error = err or perr
    return dml_result(error is None, error, persisted, [before, after])


def repark_cell(c: Any, env: Any) -> Any:
    if c["seat"] == "DML":
        return repark_dml(c, env)
    return repark_seatc(c, env)


class SparkEnv:
    def __init__(self) -> None:
        from pyspark.sql import SparkSession

        self.spark = (
            SparkSession.builder.master("local[1]")
            .appName("cmpvariant-spark")
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
        self.serial = 0

    def next_serial(self) -> Any:
        self.serial += 1
        return self.serial


def spark_frame(spark: Any, s: Any, payload: Any, shape: Any, values: Any) -> Any:
    from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

    st = StructType([StructField(n, LongType(), True) for n in ordered(s["order"])])
    elem_t = st if payload == "struct" else ArrayType(st)
    tup = tuple(values[n] for n in ordered(s["order"]))
    ev = tup if payload == "struct" else [tup]
    if shape == "top":
        typ, val = ArrayType(elem_t), [ev]
    elif shape == "struct":
        typ, val = StructType([StructField("x", ArrayType(elem_t), True)]), ([ev],)
    else:
        typ, val = MapType(StringType(), ArrayType(elem_t)), {"k": [ev]}
    return spark.createDataFrame([(val,)], StructType([StructField("s", typ, True)]))


def spark_seatc(c: Any, env: Any) -> Any:
    sp = env.spark
    types = {}
    left, err = step("ingest:L", lambda: spark_frame(sp, c["L"], c["payload"], c["shape"], VAL))
    if err:
        return refused_result(err)
    right, err = step("ingest:R", lambda: spark_frame(sp, c["R"], c["payload"], c["shape"], VAL))
    if err:
        return refused_result(err)
    types["L"] = left.schema["s"].dataType.simpleString()
    types["R"] = right.schema["s"].dataType.simpleString()
    if c["door"] == "df":
        from pyspark.sql import functions

        frame, err = step("plan", lambda: seat_df(c["cop"], left, right, functions))
        if err:
            return refused_result(err, types)
        out, err = step("op", lambda: [plain_spark(row) for row in frame.collect()])
    else:
        left.createOrReplaceTempView("l")
        right.createOrReplaceTempView("r")
        sql = seat_sql(c["cop"], map_path(c))
        out, err = step("op", lambda: [plain_spark(row) for row in sp.sql(sql).collect()])
    if err:
        return refused_result(err, types)
    return answered_result(out, types)


def spark_snapshots(sp: Any, table: Any) -> Any:
    out, err = step(
        "snap", lambda: sp.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").collect()[0]["n"]
    )
    if err:
        return None
    return int(out)


def spark_fixture(env: Any) -> Any:
    sp = env.spark
    n = env.next_serial()
    table = f"ice.ns.m{n}"
    _, err = step("ddl", lambda: sp.sql(DML_CREATE.format(t=table)).collect())
    if err:
        return table, err
    _, err = step("ddl", lambda: sp.sql(DML_FIXTURE.format(t=table)).collect())
    return table, err


def spark_dml(c: Any, env: Any) -> Any:
    sp = env.spark
    pred = dml_pred(c, "spark")
    table, err = spark_fixture(env)
    if err:
        return dml_result(False, err, None, None)
    before = spark_snapshots(sp, table)
    _, err = step("op", lambda: sp.sql(DML_STMTS[c["stmt"]].format(t=table, pred=pred)).collect())
    after = spark_snapshots(sp, table)
    persisted, perr = step(
        "read",
        lambda: [row["id"] for row in sp.sql(f"SELECT id FROM {table} ORDER BY id").collect()],
    )
    error = err or perr
    derived = None
    if err and EXPR_LITERAL_MARK in err["message"]:
        oracle_table, oerr = spark_fixture(env)
        matched, merr = step(
            "derive",
            lambda: [
                row["id"]
                for row in sp.sql(
                    f"SELECT id FROM {oracle_table} WHERE s = {pred} ORDER BY id"
                ).collect()
            ],
        )
        derived = {
            "matched": matched,
            "expected": expected_ids(c["stmt"], matched or []),
            "error": oerr or merr,
        }
    return dml_result(error is None, error, persisted, [before, after], derived)


def spark_cell(c: Any, env: Any) -> Any:
    if c["seat"] == "DML":
        return spark_dml(c, env)
    return spark_seatc(c, env)


def record(c: Any, res: Any, reuse: Any, seconds: Any) -> Any:
    out = {
        "cell": c["id"],
        "plan": c["plan"],
        "leg": c["leg"],
        "seat": c["seat"],
        "cop": c["cop"],
        "payload": c["payload"],
        "shape": c["shape"],
        "depth": c["depth"],
        "op": c["op"],
        "door": c["door"],
        "source": c["source"],
        "effect": c["effect"],
        "orient": c["orient"],
        "stmt": c["stmt"],
        "pred": c["pred"],
        "enc": c["enc"],
        "L": c["L"],
        "R": c["R"],
        "skey": skey(c),
        "tkey": tkey(c),
        "reuse": reuse,
        "seconds": round(seconds, 3),
    }
    out.update(res)
    return out


def run_leg(leg: Any, cells: Any) -> Any:
    RAW.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)
    root = WORK / leg
    if root.exists():
        shutil.rmtree(root)
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
                res = refused_result(
                    {
                        "phase": "HARNESS",
                        "cls": type(error).__name__,
                        "message": str(error).replace("\n", " ")[:800],
                    }
                )
            handle.write(json.dumps(record(c, res, reuse, time.time() - start), default=str) + "\n")
            handle.flush()
            bucket = tally.setdefault(c["plan"], [0, 0])
            bucket[0 if res["answered"] else 1] += 1
    for plan, (ok, bad) in sorted(tally.items()):
        print(f"{leg} {plan} answered={ok} refused={bad}")
    print(f"{leg} total={sum(a + b for a, b in tally.values())}")


def load(leg: Any) -> Any:
    path = RAW / f"{leg}.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def label_comparison(r: Any, s: Any) -> Any:
    if s is None:
        return "ORACLE-MISSING"
    if r["answered"] and s["answered"]:
        return "EQUAL" if r["rows"] == s["rows"] else "WRONG-ANSWER"
    if r["answered"]:
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if s["answered"]:
        return "REFUSED-WHERE-SPARK-ANSWERED"
    return "BOTH-REFUSED"


def delta(files: Any) -> Any:
    if files and isinstance(files[0], int) and isinstance(files[1], int):
        return files[1] - files[0]
    return None


def label_dml(r: Any, s: Any) -> Any:
    if s is None:
        return "ORACLE-MISSING"
    if s.get("derived") is not None:
        if r["persisted"] == s["derived"]["expected"]:
            return "DERIVED-EQUAL"
        if not r["answered"]:
            return "DERIVED-REFUSED"
        return "DERIVED-WRONG-WRITE"
    if not s["answered"]:
        return "ORACLE-MISSING"
    if r["persisted"] != s["persisted"]:
        return "WRONG-WRITE" if r["answered"] else "REFUSED-WHERE-SPARK-ANSWERED"
    if not r["answered"]:
        return "REFUSED-WHERE-SPARK-ANSWERED"
    if (
        delta(r["files"]) is not None
        and delta(s["files"]) is not None
        and delta(r["files"]) != delta(s["files"])
    ):
        return "SNAPSHOT-DIFF"
    return "EQUAL"


def value_of(x: Any) -> Any:
    return x["persisted"] if x["seat"] == "DML" else x["rows"]


def twin_compare(r: Any, t: Any) -> Any:
    if r["answered"] and t["answered"]:
        return "INVARIANT" if value_of(r) == value_of(t) else "VARIANT-DIFFERS"
    if not r["answered"] and not t["answered"]:
        return "INVARIANT"
    return "VARIANT-DIFFERS"


def kind_label(s: Any) -> Any:
    return f"{s['kind']}/{s['enc']}/{s['order']}/{s['names']}"


def outcome_text(r: Any) -> Any:
    if r["answered"]:
        if r["seat"] == "DML":
            return f"WROTE persisted={r['persisted']}"
        return f"ANSWERED rows={len(r['rows'])}"
    err = r["error"]
    return f"REFUSED {err['phase']} {err['cls']}: {err['message'][:160]}"


def build_rows() -> Any:
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
            s = spark_by_key.get(r["skey"])
            if leg == "spark":
                if r["seat"] == "DML" and r.get("derived") is not None:
                    label = "ORACLE-MISSING"
                else:
                    label = "SELF"
            elif r["seat"] == "DML":
                label = label_dml(r, s)
            else:
                label = label_comparison(r, s)
            twin = by_tkey.get(r["tkey"])
            if twin is None:
                tlabel = "NO-TWIN"
            elif twin is r:
                tlabel = "INVARIANT"
            else:
                tlabel = twin_compare(r, twin)
            rows.append((r, label, tlabel))
    return rows


def render() -> Any:
    rows = build_rows()
    out = [
        "# Comparison variant extension: measured results\n",
        "Generated by `probe_cmp_variant.py --render` from `raw/<leg>.jsonl`. Labels: see "
        "notes.md.\n",
        "## Cells per leg and plan\n",
        "| plan | spark | base |",
        "|---|---|---|",
    ]
    for p in sorted({r["plan"] for r, _, _ in rows}):
        cnt = {
            leg: sum(1 for r, _, _ in rows if r["plan"] == p and r["leg"] == leg) for leg in LEGS
        }
        out.append(f"| {p} | {cnt['spark']} | {cnt['base']} |")
    out.append(
        f"| total | {sum(1 for r, _, _ in rows if r['leg'] == 'spark')} | "
        f"{sum(1 for r, _, _ in rows if r['leg'] == 'base')} |\n"
    )

    out.append(
        "## Labels per seat and leg (base against the Spark twin; spark rows are SELF or "
        "ORACLE-MISSING)\n"
    )
    out.append("| seat | leg | label | count |")
    out.append("|---|---|---|---|")
    tally = {}
    for r, label, _ in rows:
        tally[(r["seat"], r["leg"], label)] = tally.get((r["seat"], r["leg"], label), 0) + 1
    for key in sorted(tally):
        out.append(f"| {key[0]} | {key[1]} | {key[2]} | {tally[key]} |")
    out.append("")

    out.append(
        "## Kind-invariance labels per plan and leg (cell against its plain-List twin, same leg "
        "and door)\n"
    )
    out.append("| plan | leg | label | count |")
    out.append("|---|---|---|---|")
    inv = {}
    for r, _, tlabel in rows:
        inv[(r["plan"], r["leg"], tlabel)] = inv.get((r["plan"], r["leg"], tlabel), 0) + 1
    for key in sorted(inv):
        out.append(f"| {key[0]} | {key[1]} | {key[2]} | {inv[key]} |")
    out.append("")

    out.append("## Counts by label x seat x kind pair x door (base leg, comparisons)\n")
    counts = {}
    for r, label, _ in rows:
        if r["leg"] != "base" or r["seat"] != "C":
            continue
        key = (r["plan"], r["cop"], r["door"], r["L"]["kind"], r["R"]["kind"])
        counts.setdefault(key, {})
        counts[key][label] = counts[key].get(label, 0) + 1
    out.append("| plan | op | door | left kind | right kind | labels |")
    out.append("|---|---|---|---|---|---|")
    for key in sorted(counts):
        tally_text = "; ".join(f"{k} {v}" for k, v in sorted(counts[key].items()))
        out.append(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {tally_text} |")
    out.append("")

    out.append("## Wrong on base: every WRONG-ANSWER and WRONG-WRITE group\n")
    wrong = {}
    for r, label, _ in rows:
        if r["leg"] != "base" or label not in ("WRONG-ANSWER", "WRONG-WRITE"):
            continue
        key = (
            label,
            r["plan"],
            r["op"] if r["seat"] == "DML" else r["cop"],
            r["door"],
            r["orient"],
            r["depth"],
            kind_label(r["L"]),
            kind_label(r["R"]),
        )
        wrong.setdefault(key, []).append(r["cell"])
    out.append(f"Groups: {len(wrong)}, cells: {sum(len(v) for v in wrong.values())}\n")
    out.append("| label | plan | op | door | orient | depth | left | right | cells |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for key in sorted(wrong):
        ids = wrong[key]
        out.append(
            f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {key[5]} | {key[6]} | "
            f"{key[7]} | "
            f"{len(ids)}: {', '.join(ids[:4])}{' ...' if len(ids) > 4 else ''} |"
        )
    out.append("")

    out.append("## Variant-differs: every cell whose plain-List twin gives a different answer\n")
    vdiff = {}
    for r, label, tlabel in rows:
        if tlabel != "VARIANT-DIFFERS":
            continue
        key = (
            r["leg"],
            r["plan"],
            r["op"] if r["seat"] == "DML" else r["cop"],
            r["door"],
            r["depth"],
            kind_label(r["L"]),
            kind_label(r["R"]),
            label,
        )
        vdiff.setdefault(key, []).append(r["cell"])
    out.append(f"Groups: {len(vdiff)}, cells: {sum(len(v) for v in vdiff.values())}\n")
    out.append("| leg | plan | op | door | depth | left | right | label vs Spark | cells |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for key in sorted(vdiff):
        ids = vdiff[key]
        out.append(
            f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {key[5]} | {key[6]} | "
            f"{key[7]} | "
            f"{len(ids)}: {', '.join(ids[:4])}{' ...' if len(ids) > 4 else ''} |"
        )
    out.append("")

    out.append("## DML rows: outcome, read-back, snapshot delta and derived oracle\n")
    out.append(
        "| cell | leg | stmt | predicate order | kind | label | outcome | read-back ids | "
        "snapshot delta | derived matched | derived expected |"
    )
    out.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r, label, _ in rows:
        if r["seat"] != "DML":
            continue
        derived = r.get("derived") or {}
        out.append(
            f"| {r['cell']} | {r['leg']} | {r['stmt']} | {r['R']['order']} | {r['R']['kind']} | "
            f"{label} | "
            f"{outcome_text(r).replace('|', '/')} | {r['persisted']} | {delta(r['files'])} | "
            f"{derived.get('matched', '-')} | {derived.get('expected', '-')} |"
        )
    out.append("")

    out.append("## Per-cell rows\n")
    out.append(
        "| cell | plan | leg | seat | op | door | source | orient | depth | enc | left | right "
        "| outcome | label vs Spark | kind-invariance label | types L | types R |"
    )
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r, label, tlabel in rows:
        types = r.get("types") or {}
        left = kind_label(r["L"]) if r["seat"] == "C" else "-"
        right = kind_label(r["R"]) if r["seat"] == "C" else "-"
        out.append(
            f"| {r['cell']} | {r['plan']} | {r['leg']} | {r['seat']} | {r['op']} | {r['door']} "
            f"| {r['source']} | "
            f"{r['orient']} | {r['depth']} | {r['enc'] or 'plain'} | {left} | {right} | "
            f"{outcome_text(r).replace('|', '/')} | {label} | {tlabel} | "
            f"{str(types.get('L', '-')).replace('|', '/')} | "
            f"{str(types.get('R', '-')).replace('|', '/')} |"
        )
    (OUT / "table.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"rendered {len(rows)} cells to {OUT / 'table.md'}")


def main() -> Any:
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
