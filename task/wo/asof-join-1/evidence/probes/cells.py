import contextlib
import io
import sys
from collections.abc import Callable
from typing import Any

import asof
from common import TYPES, Rec, modules, session

DIRS = ("backward", "forward", "nearest")
ONS: dict[str, Any] = {"none": None, "k1": "k1", "k1k2": ["k1", "k2"]}
KEYS = {"none": [], "k1": ["k1"], "k1k2": ["k1", "k2"]}
MAX64 = 9223372036854775807
MAX32 = 2147483647


def surfaces(
    f: Any,
    win: Any,
    left: Any,
    right: Any,
    on: str,
    how: str,
    tol: Any,
    exact: bool,
    direction: str,
) -> dict[str, Callable[[], Any]]:
    keys = KEYS[on]
    return {
        "D": lambda: asof.direct(left, right, ONS[on], how, tol, exact, direction),
        "W1": lambda: asof.w1(f, win, left, right, keys, how, tol, exact, direction),
        "W2": lambda: asof.w2(f, win, left, right, keys, how, tol, exact, direction),
    }


def grid(rec: Rec, s: Any, f: Any, win: Any, ansi: str, only: str | None) -> None:
    for typ in TYPES:
        if only and typ != only:
            continue
        left, right = asof.frames(s, f, typ, typ)
        for direction in DIRS:
            for exact in (True, False):
                for tn in (None, 1, 2):
                    tol = asof.tol_lit(f, typ, tn)
                    for how in ("inner", "left"):
                        for on in ONS:
                            key = {
                                "g": "G",
                                "ansi": ansi,
                                "typ": typ,
                                "dir": direction,
                                "exact": exact,
                                "tol": tn,
                                "how": how,
                                "on": on,
                            }
                            for surf, build in surfaces(
                                f, win, left, right, on, how, tol, exact, direction
                            ).items():
                                rec.run({**key, "surf": surf}, build)


def dcall(left: Any, right: Any, **kw: Any) -> Any:
    args: dict[str, Any] = {
        "leftAsOfColumn": "t",
        "rightAsOfColumn": "t",
        "on": "k1",
        "how": "left",
        "tolerance": None,
        "allowExactMatches": True,
        "direction": "backward",
    }
    args.update(kw)
    return left._joinAsOf(right, **args)


def errors(rec: Rec, s: Any, f: Any, ansi: str) -> None:
    left, right = asof.frames(s, f, "BIGINT", "BIGINT")
    cases: dict[str, dict[str, Any]] = {
        "missing_left_col": {"leftAsOfColumn": "nope"},
        "missing_right_col": {"rightAsOfColumn": "nope"},
        "missing_on": {"on": "nope"},
        "dir_sideways": {"direction": "sideways"},
        "dir_upper": {"direction": "BACKWARD"},
        "dir_mixed_case": {"direction": "Nearest"},
        "dir_none": {"direction": None},
        "tol_negative": {"tolerance": f.lit(-1)},
        "tol_zero": {"tolerance": f.lit(0)},
        "tol_null": {"tolerance": f.lit(None)},
        "tol_null_int": {"tolerance": f.lit(None).cast("int")},
        "tol_column": {"tolerance": f.col("lid")},
        "tol_string": {"tolerance": f.lit("2")},
        "tol_double_on_bigint": {"tolerance": f.lit(1.5)},
        "tol_interval_on_bigint": {"tolerance": f.expr("INTERVAL 2 SECONDS")},
        "tol_python_int": {"tolerance": 2},
        "exact_string": {"allowExactMatches": "yes"},
        "on_empty_list": {"on": []},
    }
    for how in (
        "right",
        "full",
        "outer",
        "cross",
        "semi",
        "anti",
        "bogus",
        "LEFT",
        "left_outer",
        "leftouter",
        "INNER",
        "inner",
    ):
        cases["how_" + how] = {"how": how}
    for name, kw in cases.items():
        rec.run(
            {"g": "E", "ansi": ansi, "case": name, "surf": "D"},
            lambda kw=kw: dcall(left, right, **kw),
        )
    typed = {
        "string_asof": ("STRING", "STRING", None),
        "string_asof_tol": ("STRING", "STRING", f.lit(2)),
        "ts_tol_int": ("TIMESTAMP", "TIMESTAMP", f.lit(2)),
        "ts_tol_day_interval": ("TIMESTAMP", "TIMESTAMP", f.expr("INTERVAL 2 DAYS")),
        "ts_tol_month_interval": ("TIMESTAMP", "TIMESTAMP", f.expr("INTERVAL 1 MONTH")),
        "ts_tol_negative_interval": ("TIMESTAMP", "TIMESTAMP", f.expr("INTERVAL -2 SECONDS")),
        "ntz_tol_int": ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ", f.lit(2)),
        "date_tol_day_interval": ("DATE", "DATE", f.expr("INTERVAL 2 DAYS")),
        "date_tol_second_interval": ("DATE", "DATE", f.expr("INTERVAL 2 SECONDS")),
        "date_tol_double": ("DATE", "DATE", f.lit(1.5)),
        "decimal_tol_double": ("DECIMAL(10,2)", "DECIMAL(10,2)", f.lit(1.5)),
        "double_tol_decimal": ("DOUBLE", "DOUBLE", f.expr("CAST(1.5 AS DECIMAL(3,1))")),
        "boolean_asof": ("BOOLEAN", "BOOLEAN", None),
    }
    for name, (lt, rt, tol) in typed.items():
        lf, rf = asof.frames(s, f, lt, rt)
        for direction in DIRS:
            key = {"g": "E", "ansi": ansi, "case": name + ":" + direction, "surf": "D"}
            rec.run(
                key,
                lambda lf=lf, rf=rf, tol=tol, d=direction: dcall(
                    lf, rf, tolerance=tol, direction=d
                ),
            )


MIXED = (
    ("BIGINT", "INT"),
    ("INT", "BIGINT"),
    ("INT", "DOUBLE"),
    ("BIGINT", "DECIMAL(10,2)"),
    ("DOUBLE", "DECIMAL(10,2)"),
    ("DATE", "TIMESTAMP"),
    ("TIMESTAMP", "DATE"),
    ("TIMESTAMP", "TIMESTAMP_NTZ"),
    ("TIMESTAMP_NTZ", "TIMESTAMP"),
    ("BIGINT", "STRING"),
    ("STRING", "BIGINT"),
    ("DATE", "BIGINT"),
)


def mixed(rec: Rec, s: Any, f: Any, win: Any, ansi: str) -> None:
    for lt, rt in MIXED:
        left, right = asof.frames(s, f, lt, rt)
        for direction in DIRS:
            for tn in (None, 2):
                tol = None if tn is None else asof.tol_lit(f, lt, tn)
                key = {
                    "g": "M",
                    "ansi": ansi,
                    "typ": lt + ">" + rt,
                    "dir": direction,
                    "exact": True,
                    "tol": tn,
                    "how": "left",
                    "on": "k1",
                }
                built = surfaces(f, win, left, right, "k1", "left", tol, True, direction)
                for surf in ("D", "W1"):
                    rec.run({**key, "surf": surf}, built[surf])


def overflow(rec: Rec, s: Any, f: Any, win: Any, ansi: str) -> None:
    for typ, top in (("BIGINT", MAX64), ("INT", MAX32)):
        lrows = [(1, "a", 1, top), (2, "a", 1, -top - 1), (3, "a", 1, 0)]
        rrows = [
            (1, "a", 1, -top - 1, "rmin"),
            (2, "a", 1, top - 1, "rtop1"),
            (3, "a", 1, top, "rtop"),
            (4, "a", 1, -top, "rmin1"),
        ]
        left, right = asof.frames(s, f, typ, typ, lrows, rrows)
        for direction in DIRS:
            for exact in (True, False):
                for tn in (None, 2):
                    tol = asof.tol_lit(f, typ, tn)
                    key = {
                        "g": "O",
                        "ansi": ansi,
                        "typ": typ,
                        "dir": direction,
                        "exact": exact,
                        "tol": tn,
                        "how": "left",
                        "on": "k1",
                    }
                    for surf, build in surfaces(
                        f, win, left, right, "k1", "left", tol, exact, direction
                    ).items():
                        rec.run({**key, "surf": surf}, build)


def special(rec: Rec, s: Any, f: Any, win: Any, ansi: str) -> None:
    lrows = [
        (1, "a", 1, "NaN"),
        (2, "a", 1, "0.0"),
        (3, "a", 1, "-0.0"),
        (4, "a", 1, "1.0"),
        (5, "a", 1, "Infinity"),
    ]
    rrows = [
        (1, "a", 1, "NaN", "rnan"),
        (2, "a", 1, "-0.0", "rnz"),
        (3, "a", 1, "0.5", "rhalf"),
        (4, "a", 1, "-Infinity", "rninf"),
    ]
    left, right = asof.frames(s, f, "DOUBLE", "DOUBLE", lrows, rrows)
    for direction in DIRS:
        for exact in (True, False):
            for tn in (None, 1):
                tol = asof.tol_lit(f, "DOUBLE", tn)
                key = {
                    "g": "N",
                    "ansi": ansi,
                    "typ": "DOUBLE",
                    "dir": direction,
                    "exact": exact,
                    "tol": tn,
                    "how": "left",
                    "on": "k1",
                }
                for surf, build in surfaces(
                    f, win, left, right, "k1", "left", tol, exact, direction
                ).items():
                    rec.run({**key, "surf": surf}, build)


def ties(rec: Rec, s: Any, f: Any, win: Any, ansi: str) -> None:
    lrows = [(1, "a", 1, 10), (2, "a", 1, 20)]
    base = [
        (1, "a", 1, 10, "x1"),
        (2, "a", 1, 10, "x2"),
        (3, "a", 1, 10, "x3"),
        (4, "a", 1, 18, "y18"),
        (5, "a", 1, 22, "z22"),
    ]
    orders = {"asis": base, "reversed": base[::-1], "rotated": base[2:] + base[:2]}
    for shuffle in ("1", "4"):
        s.conf.set("spark.sql.shuffle.partitions", shuffle)
        for oname, rrows in orders.items():
            left, right0 = asof.frames(s, f, "BIGINT", "BIGINT", lrows, rrows)
            for part in ("none", "repart3"):
                right = right0.repartition(3) if part == "repart3" else right0
                for direction in DIRS:
                    for rep in (0, 1):
                        key = {
                            "g": "T",
                            "ansi": ansi,
                            "typ": "BIGINT",
                            "dir": direction,
                            "exact": True,
                            "tol": None,
                            "how": "left",
                            "on": "k1",
                            "order": oname,
                            "part": part,
                            "shuffle": shuffle,
                            "rep": rep,
                        }
                        for surf, build in surfaces(
                            f, win, left, right, "k1", "left", None, True, direction
                        ).items():
                            rec.run({**key, "surf": surf}, build)
    s.conf.set("spark.sql.shuffle.partitions", "1")


def forms(rec: Rec, s: Any, f: Any, ansi: str) -> None:
    left, right = asof.frames(s, f, "BIGINT", "BIGINT")
    renamed = right.withColumnRenamed("t", "rt")
    cases: dict[str, Callable[[], Any]] = {
        "on_column": lambda: dcall(left, right, on=left.k1 == right.k1),
        "on_columns": lambda: dcall(left, right, on=[left.k1 == right.k1, left.k2 == right.k2]),
        "on_list_one": lambda: dcall(left, right, on=["k1"]),
        "on_non_equi": lambda: dcall(left, right, on=left.k1 != right.k1),
        "on_null_safe": lambda: dcall(left, right, on=left.k1.eqNullSafe(right.k1)),
        "asof_columns": lambda: dcall(left, right, leftAsOfColumn=left.t, rightAsOfColumn=right.t),
        "asof_expression": lambda: dcall(left, right, leftAsOfColumn=left.t + 1),
        "asof_renamed_right": lambda: dcall(left, renamed, rightAsOfColumn="rt"),
        "self_join": lambda: dcall(left, left),
        "select_k1": lambda: dcall(left, right).select("k1"),
        "select_left_k1": lambda: dcall(left, right).select(left.k1, right.k1),
        "select_star": lambda: dcall(left, right).select("*"),
        "inner_no_on": lambda: dcall(left, right, on=None, how="inner"),
    }
    for name, build in cases.items():
        rec.run({"g": "C", "ansi": ansi, "case": name, "surf": "D"}, build)


def grammar(rec: Rec, s: Any, f: Any, ansi: str) -> None:
    left, right = asof.frames(s, f, "BIGINT", "BIGINT")
    left.createOrReplaceTempView("l")
    right.createOrReplaceTempView("r")
    texts = {
        "asof_as_alias": "SELECT ASOF.lid, r.rid FROM l ASOF JOIN r "
        "ON ASOF.k1 = r.k1 AND ASOF.t = r.t",
        "snowflake_match_condition": "SELECT l.lid, r.rid FROM l ASOF JOIN r "
        "MATCH_CONDITION (l.t >= r.t) ON l.k1 = r.k1",
        "left_asof": "SELECT l.lid, r.rid FROM l LEFT ASOF JOIN r ON l.k1 = r.k1",
        "min_by_sql": "SELECT min_by(rv, t) AS m FROM r",
        "max_by_sql": "SELECT max_by(rv, t) AS m FROM r",
        "min_by_struct_grouped": "SELECT k1, min_by(struct(rid, rv), t) AS m FROM r GROUP BY k1",
        "correlated_non_equi_scalar": "SELECT l.lid, (SELECT max(r.t) FROM r "
        "WHERE r.k1 = l.k1 AND r.t <= l.t) AS m FROM l",
    }
    for name, query in texts.items():
        rec.run({"g": "S", "ansi": ansi, "case": name, "surf": "SQL"}, lambda q=query: s.sql(q))
    rec.run(
        {"g": "S", "ansi": ansi, "case": "min_by_expr", "surf": "F.expr"},
        lambda: right.groupBy("k1").agg(f.expr("min_by(rv, t)").alias("m")),
    )
    rec.run(
        {"g": "S", "ansi": ansi, "case": "monotonically_increasing_id", "surf": "F"},
        lambda: left.select(f.monotonically_increasing_id().alias("i")),
    )
    rec.run(
        {"g": "S", "ansi": ansi, "case": "select_list", "surf": "DataFrame"},
        lambda: left.select([f.col("lid")]),
    )


def plans(rec: Rec, s: Any, f: Any, win: Any, ansi: str, engine: str) -> None:
    left, right = asof.frames(s, f, "BIGINT", "BIGINT")
    tol = f.lit(2)
    for direction in DIRS:
        for on in ("k1", "none"):
            built = surfaces(f, win, left, right, on, "left", tol, True, direction)
            for surf, build in built.items():
                key = {"g": "X", "ansi": ansi, "dir": direction, "on": on, "surf": surf}
                try:
                    frame = build()
                    if engine == "spark":
                        text = frame._jdf.queryExecution().toString()
                    else:
                        buffer = io.StringIO()
                        with contextlib.redirect_stdout(buffer):
                            frame.explain(True)
                        text = buffer.getvalue()
                    rec.emit({**key, "plan": text})
                except Exception as error:
                    rec.emit(
                        {
                            **key,
                            "plan": "ERROR "
                            + type(error).__name__
                            + " "
                            + " ".join(str(error).split())[:300],
                        }
                    )


def main() -> None:
    engine, out, ansi, groups = sys.argv[1:5]
    only = sys.argv[5] if len(sys.argv) > 5 else None
    s = session(engine, ansi)
    f, win = modules(engine)
    rec = Rec(out)
    for group in groups.split(","):
        if group == "G":
            grid(rec, s, f, win, ansi, only)
        elif group == "E":
            errors(rec, s, f, ansi)
        elif group == "M":
            mixed(rec, s, f, win, ansi)
        elif group == "O":
            overflow(rec, s, f, win, ansi)
        elif group == "N":
            special(rec, s, f, win, ansi)
        elif group == "T":
            ties(rec, s, f, win, ansi)
        elif group == "C":
            forms(rec, s, f, ansi)
        elif group == "S":
            grammar(rec, s, f, ansi)
        elif group == "X":
            plans(rec, s, f, win, ansi, engine)
        print("DONE", group, flush=True)


if __name__ == "__main__":
    main()
