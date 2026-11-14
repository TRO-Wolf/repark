from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

TYPES = ("DOUBLE", "FLOAT")
OTHER = {"DOUBLE": "FLOAT", "FLOAT": "DOUBLE"}
SUFFIX = {"DOUBLE": "D", "FLOAT": "F"}
CATALOG = {"spark": "probe_ice", "repark": "ice"}


def neg_forms(t: str) -> dict[str, str]:
    u = OTHER[t]
    return {
        "dlit": f"-0.0{SUFFIX[t]}",
        "neg": f"-CAST(0 AS {t})",
        "str": f"CAST('-0.0' AS {t})",
        "arith": f"(CAST(0 AS {t}) * -1)",
        "nested": f"CAST(CAST('-0.0' AS {u}) AS {t})",
    }


def pos_forms(t: str) -> dict[str, str]:
    u = OTHER[t]
    return {
        "dlit": f"0.0{SUFFIX[t]}",
        "cast": f"CAST(0 AS {t})",
        "str": f"CAST('0.0' AS {t})",
        "arith": f"(-CAST(0 AS {t}) * -1)",
        "dec": f"CAST(CAST(0 AS DECIMAL(10,2)) AS {t})",
        "nested": f"CAST(CAST('0.0' AS {u}) AS {t})",
    }


def nan_forms(t: str) -> dict[str, str]:
    return {
        "nan": f"CAST('NaN' AS {t})",
        "sqrt": f"CAST(sqrt(CAST(-1 AS DOUBLE)) AS {t})",
        "ln": f"CAST(ln(CAST(-1 AS DOUBLE)) AS {t})",
        "negnan": f"(CAST('NaN' AS {t}) * -1)",
        "one": f"CAST(1 AS {t})",
        "inf": f"CAST('Infinity' AS {t})",
    }


def frame_form(functions: Any, t: str, sign: str, form: str) -> Any:
    fn = functions
    ft = t.lower()
    fu = OTHER[t].lower()
    if form == "dlit":
        value = fn.lit(-0.0 if sign == "neg" else 0.0)
        return value if t == "DOUBLE" else value.cast(ft)
    zero = fn.lit(0).cast(ft)
    table = {
        ("neg", "neg"): -zero,
        ("neg", "str"): fn.lit("-0.0").cast(ft),
        ("neg", "arith"): zero * -1,
        ("neg", "nested"): fn.lit("-0.0").cast(fu).cast(ft),
        ("pos", "cast"): zero,
        ("pos", "str"): fn.lit("0.0").cast(ft),
        ("pos", "arith"): (-zero) * -1,
        ("pos", "dec"): fn.lit(0).cast("decimal(10,2)").cast(ft),
        ("pos", "nested"): fn.lit("0.0").cast(fu).cast(ft),
    }
    return table[(sign, form)]


OPS = {
    "eq": "{l} = {r}",
    "ne": "{l} <> {r}",
    "nseq": "{l} <=> {r}",
    "lt": "{l} < {r}",
    "le": "{l} <= {r}",
    "gt": "{l} > {r}",
    "ge": "{l} >= {r}",
    "in1": "{l} IN ({r})",
    "in2": "{l} IN ({r}, CAST(1 AS {t}))",
    "notin1": "{l} NOT IN ({r})",
    "notin2": "{l} NOT IN ({r}, CAST(1 AS {t}))",
    "between": "{l} BETWEEN {r} AND {r}",
    "nbetween": "{l} NOT BETWEEN {r} AND {r}",
    "case_simple": "CASE {l} WHEN {r} THEN 1 ELSE 0 END",
    "case_searched": "CASE WHEN {l} = {r} THEN 1 ELSE 0 END",
    "if": "IF({l} = {r}, 1, 0)",
    "nullif": "nullif({l}, {r}) IS NULL",
    "greatest": "greatest({l}, {r})",
    "least": "least({l}, {r})",
    "coalesce": "coalesce({l}, {r})",
    "array_contains": "array_contains(array({l}), {r})",
}


def frame_op(functions: Any, op: str, left: Any, right: Any, t: str) -> Any:
    fn = functions
    one = fn.lit(1).cast(t.lower())
    if op == "eq":
        return left == right
    if op == "ne":
        return left != right
    if op == "nseq":
        return left.eqNullSafe(right)
    if op == "lt":
        return left < right
    if op == "le":
        return left <= right
    if op == "gt":
        return left > right
    if op == "ge":
        return left >= right
    if op == "in1":
        return left.isin(right)
    if op == "in2":
        return left.isin(right, one)
    if op == "notin1":
        return ~left.isin(right)
    if op == "notin2":
        return ~left.isin(right, one)
    if op == "between":
        return left.between(right, right)
    if op == "nbetween":
        return ~left.between(right, right)
    if op == "case_searched":
        return fn.when(left == right, 1).otherwise(0)
    if op == "nullif":
        return fn.nullif(left, right).isNull()
    if op == "greatest":
        return fn.greatest(left, right)
    if op == "least":
        return fn.least(left, right)
    if op == "coalesce":
        return fn.coalesce(left, right)
    if op == "array_contains":
        return fn.array_contains(fn.array(left), right)
    return None


def zero_pairs() -> list[dict[str, str]]:
    pairs = []
    for t in TYPES:
        neg, pos = neg_forms(t), pos_forms(t)
        for nk in neg:
            for pk in pos:
                pairs.append({"t": t, "l": f"neg.{nk}", "r": f"pos.{pk}"})
                pairs.append({"t": t, "l": f"pos.{pk}", "r": f"neg.{nk}"})
        for nk in neg:
            pairs.append({"t": t, "l": f"neg.{nk}", "r": f"neg.{nk}"})
        for pk in pos:
            pairs.append({"t": t, "l": f"pos.{pk}", "r": f"pos.{pk}"})
    return pairs


def nan_pairs() -> list[dict[str, str]]:
    pairs = []
    for t in TYPES:
        nans = ["nan", "sqrt", "ln", "negnan"]
        for a in nans:
            for b in nans:
                pairs.append({"t": t, "l": f"nan.{a}", "r": f"nan.{b}"})
            for c in ("one", "inf"):
                pairs.append({"t": t, "l": f"nan.{a}", "r": f"nan.{c}"})
                pairs.append({"t": t, "l": f"nan.{c}", "r": f"nan.{a}"})
    return pairs


def form_sql(t: str, key: str) -> str:
    family, name = key.split(".")
    table = {"neg": neg_forms, "pos": pos_forms, "nan": nan_forms}[family](t)
    return table[name]


CONTEXT_PAIRS = [
    ("DOUBLE", "neg.str", "pos.str"),
    ("DOUBLE", "neg.neg", "pos.dec"),
    ("DOUBLE", "neg.arith", "pos.cast"),
    ("DOUBLE", "neg.nested", "pos.str"),
    ("DOUBLE", "neg.dlit", "pos.dlit"),
    ("DOUBLE", "pos.str", "pos.str"),
    ("FLOAT", "neg.str", "pos.str"),
    ("FLOAT", "neg.neg", "pos.dec"),
    ("FLOAT", "neg.arith", "pos.cast"),
    ("FLOAT", "neg.nested", "pos.str"),
    ("FLOAT", "neg.dlit", "pos.dlit"),
    ("FLOAT", "pos.str", "pos.str"),
]
CONTEXT_OPS = ("eq", "ne", "nseq", "lt", "ge", "in2", "notin2", "between")
CONTEXTS = {
    "where": "SELECT count(*) AS r FROM (VALUES (1), (2)) AS t(x) WHERE {p}",
    "having": (
        "SELECT count(*) AS r FROM (SELECT x FROM (VALUES (1), (2)) AS t(x) "
        "GROUP BY x HAVING {p}) AS h"
    ),
    "join": (
        "SELECT count(*) AS r FROM (VALUES (1), (2)) AS a(x) "
        "JOIN (VALUES (1), (2)) AS b(y) ON a.x = b.y AND {p}"
    ),
    "left_join": (
        "SELECT count(b.y) AS r FROM (VALUES (1), (2)) AS a(x) "
        "LEFT JOIN (VALUES (1), (2)) AS b(y) ON a.x = b.y AND {p}"
    ),
    "where_or": "SELECT count(*) AS r FROM (VALUES (1), (2)) AS t(x) WHERE x = 1 OR {p}",
}
MIXED_OPS = ("eq", "ne", "nseq", "lt", "le", "gt", "ge", "in1", "notin1", "between")
MIXED_TABLE = (
    "SELECT count(*) AS r FROM (VALUES (CAST('-0.0' AS {t})), (CAST('0.0' AS {t})), "
    "(CAST('1.5' AS {t})), (CAST(NULL AS {t}))) AS m(d) WHERE {p}"
)
DML_PAIRS = [
    ("DOUBLE", "neg.str", "pos.str"),
    ("DOUBLE", "neg.nested", "pos.dec"),
    ("DOUBLE", "neg.arith", "pos.cast"),
    ("FLOAT", "neg.str", "pos.str"),
    ("DOUBLE", "neg.arith", "pos.arith"),
]
DML_OPS = ("eq", "ne", "nseq", "lt")
DML_STMTS = {
    "delete_all": "DELETE FROM {tbl} WHERE {p}",
    "delete_id": "DELETE FROM {tbl} WHERE id <= 2 AND {p}",
    "update": "UPDATE {tbl} SET v = 'u' WHERE id <= 2 AND {p}",
    "merge": (
        "MERGE INTO {tbl} AS t USING (SELECT CAST(1 AS BIGINT) AS id, 'm' AS v) AS s "
        "ON t.id = s.id AND {p} WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    ),
    "merge_clause": (
        "MERGE INTO {tbl} AS t USING (SELECT CAST(1 AS BIGINT) AS id, 'm' AS v) AS s "
        "ON t.id = s.id WHEN MATCHED AND {p} THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    ),
}
KINDS = {"cow": "copy-on-write", "mor": "merge-on-read"}


def norm(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if value == 0.0:
            return "-0.0" if math.copysign(1.0, value) < 0 else "0.0"
        return repr(value)
    if isinstance(value, Decimal):
        return str(value)
    return value


def rows_of(result: Any) -> list[list[Any]]:
    if hasattr(result, "to_arrow") and not hasattr(result, "collect"):
        table = result.to_arrow()
        return [[norm(v) for v in row.values()] for row in table.to_pylist()]
    if hasattr(result, "collect"):
        try:
            return [[norm(v) for v in row] for row in result.collect()]
        except TypeError:
            pass
    table = result.to_arrow()
    return [[norm(v) for v in row.values()] for row in table.to_pylist()]


def attempt(call: Any) -> dict[str, Any]:
    try:
        return {"outcome": "ANSWERED", "rows": rows_of(call())}
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        text = str(error)
        panic = type(error).__name__ == "PanicException" or "panicked at" in text
        return {
            "outcome": "PANIC" if panic else "REFUSED",
            "error": {"class": type(error).__name__, "message": text[:300]},
        }


def build_spark(ansi: str, warehouse: Path, jar: str) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as fn

    cat = CATALOG["spark"]
    session = (
        SparkSession.builder.master("local[1]")
        .appName("signed-zero-const-compare-1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(warehouse / "spark-local"))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "hadoop")
        .config(f"spark.sql.catalog.{cat}.warehouse", str(warehouse / "spark"))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.ns").collect()
    return {
        "engine": "spark",
        "catalog": cat,
        "session": session,
        "functions": fn,
        "sql": session.sql,
        "native": None,
        "version": session.version,
        "close": session.stop,
    }


def build_repark(ansi: str, warehouse: Path) -> dict[str, Any]:
    import importlib.metadata

    import repark
    from repark import ReparkSession
    from repark import functions as fn
    from repark._native import PyReparkSession

    cat = CATALOG["repark"]
    native_root = warehouse / "native"
    facade_root = warehouse / "facade"
    native_root.mkdir(parents=True, exist_ok=True)
    facade_root.mkdir(parents=True, exist_ok=True)
    native = PyReparkSession.native(config={})
    native.register_memory_catalog(cat, str(native_root))
    native.create_namespace(cat, "ns", str(native_root / "ns"))
    repark._ANSI_NATIVE = native
    session = (
        ReparkSession.builder.appName("signed-zero-const-compare-1")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog(cat, str(facade_root))
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.ns").collect()
    return {
        "engine": "repark",
        "catalog": cat,
        "session": session,
        "functions": fn,
        "sql": session.sql,
        "native": repark.sql,
        "version": importlib.metadata.version("repark"),
        "built_from": os.environ.get("REPARK_BUILT_FROM", ""),
        "close": lambda: None,
    }


def doors(ctx: dict[str, Any]) -> list[str]:
    names = ["spark-sql", "dataframe", "fexpr"]
    if ctx["native"] is not None:
        names.append("native")
    return names


def select_cells(ctx: dict[str, Any], part: str, emit: Any) -> None:
    pairs = zero_pairs() if part == "select" else nan_pairs()
    session, fn = ctx["session"], ctx["functions"]
    one_row = session.sql("SELECT 1 AS k")
    for door in doors(ctx):
        for op, template in OPS.items():
            for pair in pairs:
                t = pair["t"]
                left, right = form_sql(t, pair["l"]), form_sql(t, pair["r"])
                expr = template.format(l=left, r=right, t=t)
                cell = {"part": part, "door": door, "op": op, **pair, "expr": expr}
                if door == "native" and ("dlit" in pair["l"] or "dlit" in pair["r"]):
                    emit({**cell, "outcome": "NOT-EXPRESSIBLE", "why": "D/F suffix"})
                    continue
                if door == "spark-sql":
                    result = attempt(lambda e=expr: ctx["sql"](f"SELECT {e} AS r"))
                elif door == "native":
                    result = attempt(lambda e=expr: ctx["native"](f"SELECT {e} AS r"))
                elif door == "fexpr":
                    result = attempt(lambda e=expr: one_row.select(fn.expr(e).alias("r")))
                else:
                    if part != "select" or op in ("case_simple", "if"):
                        emit({**cell, "outcome": "NOT-EXPRESSIBLE", "why": "no frame form"})
                        continue
                    ls, lk = pair["l"].split(".")
                    rs, rk = pair["r"].split(".")

                    def frame_call(
                        ls: str = ls,
                        lk: str = lk,
                        rs: str = rs,
                        rk: str = rk,
                        op: str = op,
                        t: str = t,
                    ) -> Any:
                        lc = frame_form(fn, t, ls, lk)
                        rc = frame_form(fn, t, rs, rk)
                        return one_row.select(frame_op(fn, op, lc, rc, t).alias("r"))

                    result = attempt(frame_call)
                emit({**cell, **result})


def context_cells(ctx: dict[str, Any], emit: Any) -> None:
    session, fn = ctx["session"], ctx["functions"]
    for t, lk, rk in CONTEXT_PAIRS:
        left, right = form_sql(t, lk), form_sql(t, rk)
        for op in CONTEXT_OPS:
            pred = OPS[op].format(l=left, r=right, t=t)
            for name, template in CONTEXTS.items():
                text = template.format(p=pred)
                for door in ("spark-sql", "native"):
                    if door == "native" and ctx["native"] is None:
                        continue
                    cell = {
                        "part": "context",
                        "context": name,
                        "door": door,
                        "op": op,
                        "t": t,
                        "l": lk,
                        "r": rk,
                        "expr": pred,
                        "sql": text,
                    }
                    if door == "native" and "dlit" in lk + rk:
                        emit({**cell, "outcome": "NOT-EXPRESSIBLE", "why": "D/F suffix"})
                        continue
                    run = ctx["sql"] if door == "spark-sql" else ctx["native"]
                    emit({**cell, **attempt(lambda s=text, run=run: run(s))})
            base = session.sql("SELECT x FROM (VALUES (1), (2)) AS t(x)")
            cell = {
                "part": "context",
                "context": "where",
                "door": "fexpr",
                "op": op,
                "t": t,
                "l": lk,
                "r": rk,
                "expr": pred,
            }
            emit(
                {
                    **cell,
                    **attempt(lambda p=pred, base=base: base.filter(fn.expr(p)).groupBy().count()),
                }
            )
            cell = {**cell, "door": "filter-str"}
            emit({**cell, **attempt(lambda p=pred, base=base: base.filter(p).groupBy().count())})
            ls, lname = lk.split(".")
            rs, rname = rk.split(".")
            if op in ("eq", "ne", "nseq", "lt", "ge", "in2", "notin2", "between"):

                def frame_where(
                    ls: str = ls,
                    lname: str = lname,
                    rs: str = rs,
                    rname: str = rname,
                    op: str = op,
                    t: str = t,
                    base: Any = base,
                ) -> Any:
                    lc = frame_form(fn, t, ls, lname)
                    rc = frame_form(fn, t, rs, rname)
                    return base.filter(frame_op(fn, op, lc, rc, t)).groupBy().count()

                cell = {**cell, "door": "dataframe"}
                emit({**cell, **attempt(frame_where)})
    for t in TYPES:
        forms = {
            **{f"neg.{k}": v for k, v in neg_forms(t).items()},
            **{f"pos.{k}": v for k, v in pos_forms(t).items()},
        }
        for key, form in forms.items():
            for op in MIXED_OPS:
                pred = OPS[op].format(l=form, r="d", t=t)
                text = MIXED_TABLE.format(t=t, p=pred)
                for door in ("spark-sql", "native"):
                    if door == "native" and ctx["native"] is None:
                        continue
                    cell = {
                        "part": "mixed",
                        "context": "where-column",
                        "door": door,
                        "op": op,
                        "t": t,
                        "l": key,
                        "r": "column d",
                        "expr": pred,
                        "sql": text,
                    }
                    if door == "native" and "dlit" in key:
                        emit({**cell, "outcome": "NOT-EXPRESSIBLE", "why": "D/F suffix"})
                        continue
                    run = ctx["sql"] if door == "spark-sql" else ctx["native"]
                    emit({**cell, **attempt(lambda s=text, run=run: run(s))})


def dml_cells(ctx: dict[str, Any], emit: Any) -> None:
    cat = ctx["catalog"]
    counter = 0
    door_list = ["spark-sql"] + (["native"] if ctx["native"] is not None else [])
    for door in door_list:
        run = ctx["sql"] if door == "spark-sql" else ctx["native"]
        kinds = ("cow", "mor") if door == "spark-sql" else ("cow",)
        for kind in kinds:
            for t, lk, rk in DML_PAIRS:
                pred = None
                for op in DML_OPS:
                    pred = OPS[op].format(l=form_sql(t, lk), r=form_sql(t, rk), t=t)
                    for stmt, template in DML_STMTS.items():
                        counter += 1
                        tbl = f"{cat}.ns.d{counter}_{door.replace('-', '')}_{kind}"
                        if door == "native":
                            create = f"CREATE TABLE {tbl} (id BIGINT, v STRING)"
                        else:
                            mode = KINDS[kind]
                            create = (
                                f"CREATE TABLE {tbl} (id BIGINT, v STRING) USING iceberg "
                                f"TBLPROPERTIES ('format-version' = '2', "
                                f"'write.delete.mode' = '{mode}', 'write.update.mode' = '{mode}', "
                                f"'write.merge.mode' = '{mode}')"
                            )
                        text = template.format(tbl=tbl, p=pred)
                        cell = {
                            "part": "dml",
                            "door": door,
                            "kind": f"v2-{kind}",
                            "stmt": stmt,
                            "op": op,
                            "t": t,
                            "l": lk,
                            "r": rk,
                            "expr": pred,
                            "sql": text,
                        }
                        setup = attempt(
                            lambda c=create, tbl=tbl, run=run: (
                                rows_of(run(c)),
                                rows_of(
                                    run(f"INSERT INTO {tbl} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
                                ),
                                run(f"SELECT count(*) FROM {tbl}"),
                            )[2]
                        )
                        if setup["outcome"] != "ANSWERED" or setup["rows"] != [[3]]:
                            emit({**cell, "outcome": "SETUP-REFUSED", "error": setup.get("error")})
                            continue
                        effect = attempt(lambda s=text, run=run: run(s))
                        state = attempt(
                            lambda tbl=tbl, run=run: run(f"SELECT id, v FROM {tbl} ORDER BY id, v")
                        )
                        emit(
                            {
                                **cell,
                                "outcome": effect["outcome"],
                                "error": effect.get("error"),
                                "rows": state.get("rows"),
                            }
                        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--ansi", choices=("true", "false"), required=True)
    parser.add_argument("--part", choices=("select", "nan", "context", "dml"), required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--jar", default="")
    arguments = parser.parse_args()
    warehouse = Path(arguments.warehouse)
    warehouse.mkdir(parents=True, exist_ok=True)
    if arguments.engine == "spark":
        ctx = build_spark(arguments.ansi, warehouse, arguments.jar)
    else:
        ctx = build_repark(arguments.ansi, warehouse)
        if arguments.ansi == "false":
            ctx["native"] = None
    out = Path(arguments.out)
    started = time.time()
    with out.open("w") as sink:
        header = {
            "record": "leg",
            "engine": ctx["engine"],
            "version": ctx["version"],
            "ansi": arguments.ansi,
            "part": arguments.part,
            "built_from": ctx.get("built_from", ""),
            "python": sys.version.split()[0],
        }
        sink.write(json.dumps(header) + "\n")

        def emit(cell: dict[str, Any]) -> None:
            sink.write(
                json.dumps({"record": "cell", "ansi": arguments.ansi, **cell}, default=str) + "\n"
            )

        if arguments.part in ("select", "nan"):
            select_cells(ctx, arguments.part, emit)
        elif arguments.part == "context":
            context_cells(ctx, emit)
        else:
            dml_cells(ctx, emit)
        sink.write(json.dumps({"record": "end", "secs": round(time.time() - started, 1)}) + "\n")
    ctx["close"]()


if __name__ == "__main__":
    main()
