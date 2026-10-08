from __future__ import annotations

import json
import re
import sys
import tempfile
from functools import partial
from pathlib import Path
from typing import Any

ENGINE = sys.argv[1]
OUT = sys.argv[2]

if ENGINE == "spark":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as sf

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
else:
    from repark import ReparkSession
    from repark.spark import functions as sf

    wh = tempfile.mkdtemp(prefix="grid-")
    session = (
        ReparkSession.builder.appName("grid")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", wh + "/sc")
        .getOrCreate()
    )

HOWS = ["inner", "left", "right", "full", "left_semi", "left_anti"]
SQL_HOW = {
    "inner": "INNER",
    "left": "LEFT",
    "right": "RIGHT",
    "full": "FULL",
    "left_semi": "LEFT SEMI",
    "left_anti": "LEFT ANTI",
}
cells: dict[str, object] = {}


def condition(error: Exception) -> str:
    for getter in ("getCondition", "getErrorClass"):
        fn = getattr(error, getter, None)
        if callable(fn):
            try:
                value = fn()
            except Exception:
                value = None
            if value:
                return str(value)
    return ""


def scrub(text: str) -> str:
    text = re.sub(r"calling o\d+\.", "calling o<N>.", text)
    text = re.sub(r"_repark_j([lr])_[0-9a-f]{12}", r"_repark_j\1_<hex>", text)
    return re.sub(r"__repark_([a-z]+)_[0-9a-f]{12}_", r"__repark_\1_<hex>_", text)


def clip(text: str) -> str:
    if len(text) <= 150:
        return text
    return text[:150].rsplit(" ", 1)[0]


def run(key: str, action: Any, ordered: bool = False) -> None:
    try:
        frame = action()
        rows = [list(row) for row in frame.collect()]
        if not ordered:
            rows = sorted(rows, key=lambda row: [str(v) for v in row])
        cells[key] = {"cols": list(frame.columns), "rows": rows}
    except Exception as error:
        text = clip(scrub(str(error).split("\n")[0]))
        cells[key] = {"error": type(error).__name__, "cond": condition(error), "msg": text}


def frames() -> tuple[Any, Any, Any]:
    left = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, s STRING")
    right = session.createDataFrame([(2, "x"), (3, "y"), (4, "z")], "id INT, t STRING")
    other = session.createDataFrame([(2, "p"), (3, "q"), (4, "r")], "id INT, u STRING")
    return left, right, other


def as_is(frame: Any) -> Any:
    return frame


def text_ref(name: str) -> str:
    return name


def col_ref(name: str) -> Any:
    return sf.col(name)


def item_ref(frame: Any, name: str) -> Any:
    return frame[name]


def do_select(frame: Any, make: Any) -> Any:
    return frame.select(make())


def do_filter(frame: Any, make: Any) -> Any:
    ref = make()
    if isinstance(ref, str):
        return frame.filter(f"{ref} > 2")
    return frame.filter(ref > 2)


def do_sort(frame: Any, make: Any) -> Any:
    return frame.sort(make())


def do_join(frame: Any, other: Any, make: Any) -> Any:
    ref = make()
    if isinstance(ref, str):
        ref = sf.col(ref)
    return frame.join(other, ref == sf.col("q.id"))


def schema_of(frame: Any) -> str:
    try:
        return str(frame.schema.simpleString())
    except Exception as error:
        return type(error).__name__


def df_cells(how: str, aliased: bool) -> None:
    left, right, other = frames()
    lf = left.alias("l") if aliased else left
    rf = right.alias("r") if aliased else right
    q = other.alias("q")
    frame = lf.join(rf, "id", how)
    tag = f"df|{how}|{'alias' if aliased else 'plain'}"
    cells[f"{tag}|columns"] = list(frame.columns)
    cells[f"{tag}|schema"] = schema_of(frame)
    run(f"{tag}|show", partial(as_is, frame))
    run(f"{tag}|select|star", partial(frame.select, "*"))
    run(f"{tag}|select|l.id,r.id", partial(frame.select, "l.id", "r.id"))
    run(f"{tag}|select|l.*", partial(frame.select, "l.*"))
    run(f"{tag}|select|r.*", partial(frame.select, "r.*"))
    run(f"{tag}|selectExpr|r.id+0", partial(frame.selectExpr, "r.id + 0"))
    run(f"{tag}|selectExpr|l.id+0", partial(frame.selectExpr, "l.id + 0"))
    run(f"{tag}|select|col(r.id)+1", partial(do_select, frame, lambda: sf.col("r.id") + 1))
    run(f"{tag}|select|col(l.id)+1", partial(do_select, frame, lambda: sf.col("l.id") + 1))
    refs = {
        "id": partial(text_ref, "id"),
        "l.id": partial(text_ref, "l.id"),
        "r.id": partial(text_ref, "r.id"),
        "col(id)": partial(col_ref, "id"),
        "col(l.id)": partial(col_ref, "l.id"),
        "col(r.id)": partial(col_ref, "r.id"),
        "L[id]": partial(item_ref, lf, "id"),
        "R[id]": partial(item_ref, rf, "id"),
        "frame[id]": partial(item_ref, frame, "id"),
        "frame[l.id]": partial(item_ref, frame, "l.id"),
        "frame[r.id]": partial(item_ref, frame, "r.id"),
        "stale-left[id]": partial(item_ref, left, "id"),
        "stale-right[id]": partial(item_ref, right, "id"),
    }
    for name, make in refs.items():
        run(f"{tag}|select|{name}", partial(do_select, frame, make))
        run(f"{tag}|filter|{name}", partial(do_filter, frame, make))
        run(f"{tag}|sort|{name}", partial(do_sort, frame, make), ordered=True)
        run(f"{tag}|joincond|{name}", partial(do_join, frame, q, make))


def sql_cells(how: str, aliased: bool) -> None:
    lq, rq = ("l", "r") if aliased else ("tl", "tr")
    src = (
        f"tl l {SQL_HOW[how]} JOIN tr r USING (id)"
        if aliased
        else f"tl {SQL_HOW[how]} JOIN tr USING (id)"
    )
    tag = f"sql|{how}|{'alias' if aliased else 'plain'}"
    run(f"{tag}|select|star", partial(session.sql, f"SELECT * FROM {src}"))
    run(f"{tag}|select|l.*", partial(session.sql, f"SELECT {lq}.* FROM {src}"))
    run(f"{tag}|select|r.*", partial(session.sql, f"SELECT {rq}.* FROM {src}"))
    run(f"{tag}|select|l.id,r.id", partial(session.sql, f"SELECT {lq}.id, {rq}.id FROM {src}"))
    run(
        f"{tag}|select|id,l.id,r.id",
        partial(session.sql, f"SELECT id, {lq}.id, {rq}.id FROM {src}"),
    )
    for name, ref in (("id", "id"), ("l.id", f"{lq}.id"), ("r.id", f"{rq}.id")):
        run(f"{tag}|select|{name}", partial(session.sql, f"SELECT {ref} FROM {src}"))
        run(f"{tag}|select|{name}+1", partial(session.sql, f"SELECT {ref} + 1 FROM {src}"))
        run(
            f"{tag}|filter|{name}",
            partial(session.sql, f"SELECT * FROM {src} WHERE {ref} > 2"),
        )
        run(
            f"{tag}|sort|{name}",
            partial(session.sql, f"SELECT * FROM {src} ORDER BY {ref}"),
            ordered=True,
        )
        run(
            f"{tag}|joincond|{name}",
            partial(session.sql, f"SELECT * FROM {src} JOIN tq q ON {ref} = q.id"),
        )
        run(
            f"{tag}|groupby|{name}",
            partial(session.sql, f"SELECT {ref}, count(*) FROM {src} GROUP BY {ref}"),
        )


def mixed_cells(how: str) -> None:
    left = session.createDataFrame([(1,), (2,)], "id INT")
    right = session.createDataFrame([("2",), ("3",)], "id STRING")
    frame = left.alias("l").join(right.alias("r"), "id", how)
    cells[f"mixed|{how}|schema"] = schema_of(frame)
    run(f"mixed|{how}|star", partial(frame.select, "*"))
    run(f"mixed|{how}|l.id", partial(frame.select, "l.id"))
    run(f"mixed|{how}|r.id", partial(frame.select, "r.id"))


def two_key_cells(how: str) -> None:
    two_l = session.createDataFrame([(1, 1, "a"), (2, 2, "b")], "a INT, b INT, s STRING")
    two_r = session.createDataFrame([(2, 2, "x"), (3, 3, "y")], "a INT, b INT, t STRING")
    frame = two_l.alias("l").join(two_r.alias("r"), ["a", "b"], how)
    cells[f"two|{how}|columns"] = list(frame.columns)
    run(f"two|{how}|star", partial(frame.select, "*"))
    run(f"two|{how}|l.a,r.b", partial(frame.select, "l.a", "r.b"))


def control_cells(how: str) -> None:
    base_l, base_r, _ = frames()
    frame = base_l.alias("l").join(base_r.alias("r"), sf.col("l.id") == sf.col("r.id"), how)
    cells[f"control-on|{how}|columns"] = list(frame.columns)
    run(f"control-on|{how}|show", partial(as_is, frame))
    run(f"control-on|{how}|l.id,r.id", partial(frame.select, "l.id", "r.id"))
    renamed = base_r.withColumnRenamed("id", "rid")
    named = base_l.join(renamed, sf.col("id") == sf.col("rid"), how)
    run(f"control-expr|{how}|show", partial(as_is, named))


def then_side_key(narrowed: Any) -> Any:
    return narrowed().select("r.id")


def chain_cells(how: str) -> None:
    left, right, other = frames()
    frame = left.alias("l").join(right.alias("r"), "id", how)
    run(f"chain|{how}|withColumn", partial(frame.withColumn, "k", sf.col("id") + 1))
    run(f"chain|{how}|drop-id", partial(frame.drop, "id"))
    run(f"chain|{how}|groupBy-id", partial(frame.groupBy("id").count))
    run(f"chain|{how}|using-again", partial(frame.join, other.alias("q"), "id", "full"))
    run(
        f"chain|{how}|select-then-r.id",
        partial(then_side_key, partial(frame.select, "id", "s")),
    )
    run(
        f"chain|{how}|filter-then-r.id",
        partial(then_side_key, partial(frame.filter, "id > 0")),
    )
    run(f"chain|{how}|union", partial(frame.union, frame))
    run(f"chain|{how}|distinct", partial(frame.distinct))
    run(f"chain|{how}|alias-x.id", partial(frame.alias("x").select, "x.id"))
    run(f"chain|{how}|alias-x-r.id", partial(frame.alias("x").select, "r.id"))


def natural_cells(how: str) -> None:
    kind = how.upper()
    run(
        f"natural|{how}|star",
        partial(session.sql, f"SELECT * FROM tl l NATURAL {kind} JOIN tr r"),
    )
    run(
        f"natural|{how}|l.id,r.id",
        partial(session.sql, f"SELECT l.id, r.id FROM tl l NATURAL {kind} JOIN tr r"),
    )


def main() -> None:
    for how in HOWS:
        for aliased in (True, False):
            df_cells(how, aliased)
    left, right, other = frames()
    left.createOrReplaceTempView("tl")
    right.createOrReplaceTempView("tr")
    other.createOrReplaceTempView("tq")
    for how in HOWS:
        for aliased in (True, False):
            sql_cells(how, aliased)
    for how in ("inner", "left", "right", "full"):
        mixed_cells(how)
    for how in ("right", "full"):
        two_key_cells(how)
    for how in ("left", "right", "full"):
        control_cells(how)
    for how in ("right", "full"):
        chain_cells(how)
    for how in ("right", "full"):
        natural_cells(how)
    lines = ",\n".join(
        f"{json.dumps(key)}: {json.dumps(cells[key], sort_keys=True, default=str)}"
        for key in sorted(cells)
    )
    Path(OUT).write_text("{\n" + lines + "\n}\n")
    print(len(cells), "cells")
    session.stop()


main()
