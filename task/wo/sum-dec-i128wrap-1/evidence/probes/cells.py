from decimal import Decimal, getcontext

getcontext().prec = 120

COLS = [
    (10, 0),
    (10, 2),
    (18, 0),
    (18, 4),
    (19, 0),
    (19, 9),
    (28, 0),
    (28, 2),
    (34, 4),
    (38, 0),
    (38, 10),
    (38, 38),
]
WIDE = [(38, 0), (38, 10), (38, 38)]
S4_COLS = [(10, 2), (28, 2), (38, 0), (38, 10), (38, 38)]
TRY_COLS = [(10, 2), (34, 4), (38, 0), (38, 10), (38, 38)]
BESIDE_COLS = [(10, 2), (38, 0), (38, 10), (38, 38)]
P2_COLS = [(10, 2), (38, 0), (38, 38)]
BIG_COLS = [(34, 0), (34, 4)]
ICE_COLS = [(10, 2), (38, 0), (38, 38)]

SHAPES = {
    1: [105, 255, -10],
    2: [100, None, 200, None],
    3: [None, None],
    4: ["M"],
    5: ["M", 1],
    6: ["M", "M"],
    7: ["M", "M", "M"],
    8: ["-M", -1],
    9: ["-M", "-M"],
    10: ["M", "M", "-M", "-M", 5],
    11: ["M", 1, -1],
    12: ["M", "-M", "M", "-M", 7],
    13: ["M", -1],
    14: [1],
}
SHAPE_NAMES = {
    1: "fits",
    2: "fits_nulls",
    3: "all_null",
    4: "max",
    5: "max_plus_unit",
    6: "two_max",
    7: "three_max",
    8: "neg_max_minus_unit",
    9: "two_neg_max",
    10: "pass_back",
    11: "pass_back_unit",
    12: "cancel",
    13: "max_minus_unit",
    14: "unit",
}
WIN_GROUPS = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
DF_GROUPS = [1, 5, 6, 7, 10, 11]
FIT_GROUPS = [1, 2, 3, 4, 12, 13, 14]

P2_SHAPES = {
    21: (["M", "M"], ["-M", "-M", 5]),
    22: (["M"], [1]),
    23: (["M", "-M"], ["M", "-M", 5]),
    24: (["M", "M", "-M"], ["-M"]),
}
P2_NAMES = {
    21: "split_pass_back",
    22: "split_total_past",
    23: "split_fits",
    24: "split_back_inside",
}

BIG_SHAPES = {
    31: [("M", 10001)],
    32: [("M", 17100)],
    33: [("M", 10001), ("-M", 10001), (5, 1)],
}
BIG_NAMES = {31: "x10001_max", 32: "x17100_max_wraps", 33: "x10001_back"}

MAXL = 9223372036854775807
MINL = -9223372036854775808
MAXI = 2147483647
K_SHAPES = {
    41: [1, 2, -3, 10],
    42: [MAXL, 1],
    43: [MINL, -1],
    44: [MAXL, 1, -1],
    45: [None, None],
    46: [MAXI, MAXI],
    47: ["F", "F"],
}
K_NAMES = {
    41: "fits",
    42: "bigint_over",
    43: "bigint_under",
    44: "bigint_pass_back",
    45: "all_null",
    46: "int_max_pair",
    47: "double_over",
}

WIN_FORMS = {
    "win_part": "PARTITION BY g",
    "win_rows2": "PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
    "win_running": "PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "win_range": "PARTITION BY g ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW",
    "win_default": "PARTITION BY g ORDER BY id",
}


def col_name(p: int, s: int) -> str:
    return f"c{p}_{s}"


def unscaled(token: object, p: int) -> int | None:
    if token is None:
        return None
    if token == "M":
        return 10**p - 1
    if token == "-M":
        return -(10**p) + 1
    return int(token)


def to_text(value: int | None, s: int) -> str | None:
    if value is None:
        return None
    return format(Decimal(value).scaleb(-s), "f")


def s_rows() -> tuple[list, list]:
    names = ["g", "id", "f"] + [col_name(p, s) for p, s in COLS]
    rows = []
    rid = 0
    for g, shape in SHAPES.items():
        for token in shape:
            rid += 1
            row = [str(g), str(rid), str(rid % 4)]
            for p, s in COLS:
                row.append(to_text(unscaled(token, p), s))
            rows.append(row)
    return names, rows


def s_types() -> dict:
    t = {"g": "INT", "id": "INT", "f": "INT"}
    for p, s in COLS:
        t[col_name(p, s)] = f"DECIMAL({p},{s})"
    return t


def p2_rows() -> tuple[list, list]:
    names = ["g", "id", "f"] + [col_name(p, s) for p, s in P2_COLS]
    rows = []
    rid = 100
    for g, files in P2_SHAPES.items():
        for f, shape in enumerate(files):
            for token in shape:
                rid += 1
                row = [str(g), str(rid), str(f)]
                for p, s in P2_COLS:
                    row.append(to_text(unscaled(token, p), s))
                rows.append(row)
    return names, rows


def p2_types() -> dict:
    t = {"g": "INT", "id": "INT", "f": "INT"}
    for p, s in P2_COLS:
        t[col_name(p, s)] = f"DECIMAL({p},{s})"
    return t


def big_rows() -> tuple[list, list]:
    names = ["g", "id"] + [col_name(p, s) for p, s in BIG_COLS]
    rows = []
    rid = 1000
    for g, runs in BIG_SHAPES.items():
        for token, count in runs:
            for _ in range(count):
                rid += 1
                row = [str(g), str(rid)]
                for p, s in BIG_COLS:
                    row.append(to_text(unscaled(token, p), s))
                rows.append(row)
    return names, rows


def big_types() -> dict:
    t = {"g": "INT", "id": "INT"}
    for p, s in BIG_COLS:
        t[col_name(p, s)] = f"DECIMAL({p},{s})"
    return t


def k_rows() -> tuple[list, list]:
    names = ["g", "id", "i", "b", "d"]
    rows = []
    rid = 500
    for g, shape in K_SHAPES.items():
        for v in shape:
            rid += 1
            if v == "F":
                rows.append([str(g), str(rid), None, None, "1e308"])
                continue
            iv = None if v is None or abs(v) > MAXI else str(v)
            bv = None if v is None else str(v)
            dv = None if v is None else str(float(v))
            rows.append([str(g), str(rid), iv, bv, dv])
    return names, rows


def k_types() -> dict:
    return {"g": "INT", "id": "INT", "i": "INT", "b": "BIGINT", "d": "DOUBLE"}


def exact_sum(table: str, g: int, p: int) -> int | None:
    if table in ("s", "s4"):
        vals = [unscaled(t, p) for t in SHAPES[g]]
    elif table == "p2":
        vals = [unscaled(t, p) for part in P2_SHAPES[g] for t in part]
    elif table == "big":
        vals = [unscaled(t, p) * n for t, n in BIG_SHAPES[g]]
    else:
        return None
    vals = [v for v in vals if v is not None]
    return sum(vals) if vals else None


def cols_all() -> list:
    return [col_name(p, s) for p, s in COLS]


def names_of(cols: list) -> list:
    return [col_name(p, s) for p, s in cols]


def sum_sql_cells() -> list:
    out = []
    for g in SHAPES:
        for c in cols_all():
            out.append(
                (
                    f"sum/ungrouped/g{g}/{c}",
                    "sum",
                    "ungrouped",
                    c,
                    g,
                    "s",
                    f"SELECT sum({c}) AS m FROM s WHERE g = {g}",
                )
            )
            out.append(
                (
                    f"sum/grouped/g{g}/{c}",
                    "sum",
                    "grouped",
                    c,
                    g,
                    "s",
                    f"SELECT g, sum({c}) AS m FROM s WHERE g = {g} GROUP BY g",
                )
            )
    for c in cols_all():
        out.append(
            (
                f"sum/grouped_all/{c}",
                "sum",
                "grouped_all",
                c,
                None,
                "s",
                f"SELECT g, sum({c}) AS m FROM s GROUP BY g",
            )
        )
        out.append(
            (
                f"sum/empty_ungrouped/{c}",
                "sum",
                "empty_ungrouped",
                c,
                None,
                "s",
                f"SELECT sum({c}) AS m FROM s WHERE g < 0",
            )
        )
        out.append(
            (
                f"sum/empty_grouped/{c}",
                "sum",
                "empty_grouped",
                c,
                None,
                "s",
                f"SELECT g, sum({c}) AS m FROM s WHERE g < 0 GROUP BY g",
            )
        )
        out.append(
            (
                f"try/empty_ungrouped/{c}",
                "try_sum",
                "try_empty",
                c,
                None,
                "s",
                f"SELECT try_sum({c}) AS m FROM s WHERE g < 0",
            )
        )
        out.append(
            (
                f"sum/distinct_all/{c}",
                "sum",
                "distinct_all",
                c,
                None,
                "s",
                f"SELECT g, sum(DISTINCT {c}) AS m FROM s GROUP BY g",
            )
        )
        out.append(
            (
                f"try/grouped_all/{c}",
                "try_sum",
                "try_grouped_all",
                c,
                None,
                "s",
                f"SELECT g, try_sum({c}) AS m FROM s GROUP BY g",
            )
        )
        out.append(
            (
                f"sum/having_all/{c}",
                "sum",
                "having_all",
                c,
                None,
                "s",
                f"SELECT g FROM s GROUP BY g HAVING sum({c}) > 0",
            )
        )
        for form, spec in WIN_FORMS.items():
            out.append(
                (
                    f"sum/{form}_all/{c}",
                    "sum",
                    f"{form}_all",
                    c,
                    None,
                    "s",
                    f"SELECT g, id, sum({c}) OVER ({spec}) AS m FROM s",
                )
            )
    for g in SHAPES:
        for c in names_of(S4_COLS):
            out.append(
                (
                    f"sum/grouped_s4/g{g}/{c}",
                    "sum",
                    "grouped_s4",
                    c,
                    g,
                    "s4",
                    f"SELECT g, sum({c}) AS m FROM s4 WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"sum/ungrouped_s4/g{g}/{c}",
                    "sum",
                    "ungrouped_s4",
                    c,
                    g,
                    "s4",
                    f"SELECT sum({c}) AS m FROM s4 WHERE g = {g}",
                )
            )
        for c in names_of(WIDE):
            out.append(
                (
                    f"sum/distinct/g{g}/{c}",
                    "sum",
                    "distinct",
                    c,
                    g,
                    "s",
                    f"SELECT g, sum(DISTINCT {c}) AS m FROM s WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"sum/distinct_ungrouped/g{g}/{c}",
                    "sum",
                    "distinct_ungrouped",
                    c,
                    g,
                    "s",
                    f"SELECT sum(DISTINCT {c}) AS m FROM s WHERE g = {g}",
                )
            )
            out.append(
                (
                    f"sum/having/g{g}/{c}",
                    "sum",
                    "having",
                    c,
                    g,
                    "s",
                    f"SELECT g FROM s WHERE g = {g} GROUP BY g HAVING sum({c}) > 0",
                )
            )
            out.append(
                (
                    f"sum/as_string/g{g}/{c}",
                    "sum",
                    "as_string",
                    c,
                    g,
                    "s",
                    f"SELECT g, CAST(sum({c}) AS STRING) AS m FROM s WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"try/s4/g{g}/{c}",
                    "try_sum",
                    "try_grouped_s4",
                    c,
                    g,
                    "s4",
                    f"SELECT g, try_sum({c}) AS m FROM s4 WHERE g = {g} GROUP BY g",
                )
            )
        for c in names_of(TRY_COLS):
            out.append(
                (
                    f"try/grouped/g{g}/{c}",
                    "try_sum",
                    "try_grouped",
                    c,
                    g,
                    "s",
                    f"SELECT g, try_sum({c}) AS m FROM s WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"try/ungrouped/g{g}/{c}",
                    "try_sum",
                    "try_ungrouped",
                    c,
                    g,
                    "s",
                    f"SELECT try_sum({c}) AS m FROM s WHERE g = {g}",
                )
            )
        for c in names_of(BESIDE_COLS):
            out.append(
                (
                    f"sum/beside/g{g}/{c}",
                    "sum",
                    "beside",
                    c,
                    g,
                    "s",
                    f"SELECT g, sum({c}) AS m, count({c}) AS n, avg({c}) AS a, max({c}) AS x "
                    f"FROM s WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"sum/plus1/g{g}/{c}",
                    "sum",
                    "plus1",
                    c,
                    g,
                    "s",
                    f"SELECT g, sum({c}) + 1 AS m FROM s WHERE g = {g} GROUP BY g",
                )
            )
        for c in ("c10_2", "c38_0"):
            out.append(
                (
                    f"sum/linear/g{g}/{c}",
                    "sum",
                    "linear",
                    c,
                    g,
                    "s",
                    f"SELECT g, sum({c} + 1) AS a, sum({c} + 2) AS b "
                    f"FROM s WHERE g = {g} GROUP BY g",
                )
            )
    for g in WIN_GROUPS:
        for c in names_of(WIDE):
            for form, spec in WIN_FORMS.items():
                out.append(
                    (
                        f"sum/{form}/g{g}/{c}",
                        "sum",
                        form,
                        c,
                        g,
                        "s",
                        f"SELECT g, id, sum({c}) OVER ({spec}) AS m FROM s WHERE g = {g}",
                    )
                )
            for form in ("win_part", "win_rows2"):
                out.append(
                    (
                        f"try/{form}/g{g}/{c}",
                        "try_sum",
                        f"try_{form}",
                        c,
                        g,
                        "s",
                        f"SELECT g, id, try_sum({c}) OVER ({WIN_FORMS[form]}) AS m "
                        f"FROM s WHERE g = {g}",
                    )
                )
        out.append(
            (
                f"sum/win_parallel/g{g}/c38_0",
                "sum",
                "win_parallel",
                "c38_0",
                g,
                "s",
                "SELECT id, sum(c38_0) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
                "AS a, "
                "sum(c10_2) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS b "
                f"FROM s WHERE g = {g}",
            )
        )
    for g in (1, 5, 6, 10):
        out.append(
            (
                f"sum/cte/g{g}/c38_0",
                "sum",
                "cte",
                "c38_0",
                g,
                "s",
                "WITH x AS (SELECT g, sum(c38_0) AS m FROM s GROUP BY g) "
                f"SELECT g, m FROM x WHERE g = {g}",
            )
        )
        out.append(
            (
                f"sum/scalar_sub/g{g}/c38_0",
                "sum",
                "scalar_sub",
                "c38_0",
                g,
                "s",
                f"SELECT (SELECT sum(c38_0) FROM s WHERE g = {g}) AS m",
            )
        )
        out.append(
            (
                f"sum/temp_view/g{g}/c38_0",
                "sum",
                "temp_view",
                "c38_0",
                g,
                "s",
                f"SELECT g, m FROM sv_sum WHERE g = {g}",
            )
        )
    for g in P2_SHAPES:
        for c in names_of(P2_COLS):
            out.append(
                (
                    f"sum/grouped_p2/g{g}/{c}",
                    "sum",
                    "grouped_p2",
                    c,
                    g,
                    "p2",
                    f"SELECT g, sum({c}) AS m FROM p2 WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"sum/ungrouped_p2/g{g}/{c}",
                    "sum",
                    "ungrouped_p2",
                    c,
                    g,
                    "p2",
                    f"SELECT sum({c}) AS m FROM p2 WHERE g = {g}",
                )
            )
            out.append(
                (
                    f"try/grouped_p2/g{g}/{c}",
                    "try_sum",
                    "try_grouped_p2",
                    c,
                    g,
                    "p2",
                    f"SELECT g, try_sum({c}) AS m FROM p2 WHERE g = {g} GROUP BY g",
                )
            )
            out.append(
                (
                    f"try/ungrouped_p2/g{g}/{c}",
                    "try_sum",
                    "try_ungrouped_p2",
                    c,
                    g,
                    "p2",
                    f"SELECT try_sum({c}) AS m FROM p2 WHERE g = {g}",
                )
            )
            out.append(
                (
                    f"avg/grouped_p2/g{g}/{c}",
                    "avg",
                    "avg_grouped_p2",
                    c,
                    g,
                    "p2",
                    f"SELECT g, avg({c}) AS m FROM p2 WHERE g = {g} GROUP BY g",
                )
            )
    for g in BIG_SHAPES:
        for c in names_of(BIG_COLS):
            for pos, fn in (("big", "sum"), ("try_big", "try_sum"), ("avg_big", "avg")):
                fam = {"sum": "sum", "try_sum": "try_sum", "avg": "avg"}[fn]
                out.append(
                    (
                        f"{fam}/grouped_{pos}/g{g}/{c}",
                        fam,
                        f"grouped_{pos}",
                        c,
                        g,
                        "big",
                        f"SELECT g, {fn}({c}) AS m FROM big WHERE g = {g} GROUP BY g",
                    )
                )
                out.append(
                    (
                        f"{fam}/ungrouped_{pos}/g{g}/{c}",
                        fam,
                        f"ungrouped_{pos}",
                        c,
                        g,
                        "big",
                        f"SELECT {fn}({c}) AS m FROM big WHERE g = {g}",
                    )
                )
    for g in SHAPES:
        for c in cols_all():
            out.append(
                (
                    f"avg/grouped/g{g}/{c}",
                    "avg",
                    "avg_grouped",
                    c,
                    g,
                    "s",
                    f"SELECT g, avg({c}) AS m FROM s WHERE g = {g} GROUP BY g",
                )
            )
        for c in names_of(WIDE):
            out.append(
                (
                    f"avg/ungrouped/g{g}/{c}",
                    "avg",
                    "avg_ungrouped",
                    c,
                    g,
                    "s",
                    f"SELECT avg({c}) AS m FROM s WHERE g = {g}",
                )
            )
            out.append(
                (
                    f"avg/grouped_s4/g{g}/{c}",
                    "avg",
                    "avg_grouped_s4",
                    c,
                    g,
                    "s4",
                    f"SELECT g, avg({c}) AS m FROM s4 WHERE g = {g} GROUP BY g",
                )
            )
    for g in K_SHAPES:
        for c in ("i", "b", "d"):
            forms = {
                "ungrouped": f"SELECT sum({c}) AS m FROM k WHERE g = {g}",
                "grouped": f"SELECT g, sum({c}) AS m FROM k WHERE g = {g} GROUP BY g",
                "try": f"SELECT g, try_sum({c}) AS m FROM k WHERE g = {g} GROUP BY g",
                "distinct": f"SELECT g, sum(DISTINCT {c}) AS m FROM k WHERE g = {g} GROUP BY g",
                "win_rows2": f"SELECT g, id, sum({c}) OVER ({WIN_FORMS['win_rows2']}) AS m "
                f"FROM k WHERE g = {g}",
                "win_running": f"SELECT g, id, sum({c}) OVER ({WIN_FORMS['win_running']}) AS m "
                f"FROM k WHERE g = {g}",
                "avg": f"SELECT g, avg({c}) AS m FROM k WHERE g = {g} GROUP BY g",
            }
            for pos, sql in forms.items():
                out.append((f"int/{pos}/g{g}/{c}", "control", f"int_{pos}", c, g, "k", sql))
    return out


DF_FORMS = (
    "F.sum",
    "GroupedData.sum",
    "agg_dict",
    "F.expr",
    "selectExpr",
    "select_F.sum",
    "agg_F.sum",
    "F.try_sum",
    "window_rows2",
    "window_part",
    "repartition",
    "F.sum_distinct",
)

EXPLAIN_CELLS = {
    "ungrouped_s": "SELECT sum(c38_0) AS m FROM s WHERE g = 10",
    "grouped_s": "SELECT g, sum(c38_0) AS m FROM s WHERE g = 10 GROUP BY g",
    "grouped_p2": "SELECT g, sum(c38_0) AS m FROM p2 WHERE g = 21 GROUP BY g",
    "ungrouped_p2": "SELECT sum(c38_0) AS m FROM p2 WHERE g = 21",
    "win_rows2": f"SELECT g, id, sum(c38_0) OVER ({WIN_FORMS['win_rows2']}) AS m "
    "FROM s WHERE g = 10",
    "distinct": "SELECT g, sum(DISTINCT c38_0) AS m FROM s WHERE g = 5 GROUP BY g",
    "linear": "SELECT g, sum(c38_0 + 1) AS a, sum(c38_0 + 2) AS b FROM s WHERE g = 4 GROUP BY g",
    "try_grouped": "SELECT g, try_sum(c38_0) AS m FROM s WHERE g = 10 GROUP BY g",
}
