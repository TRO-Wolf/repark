from decimal import ROUND_HALF_UP, Decimal, getcontext

getcontext().prec = 120

AVG_COLS = [
    (10, 0),
    (10, 2),
    (10, 6),
    (28, 0),
    (28, 2),
    (28, 10),
    (34, 0),
    (34, 4),
    (34, 30),
    (38, 0),
    (38, 2),
    (38, 18),
    (38, 36),
]

NORMAL = {
    1: [1, 2],
    2: [1, 1, 0],
    3: [1, 0, 0],
    4: [-1, -1, 0],
    5: [1] + [0] * 31,
    6: [-1] + [0] * 31,
    7: [1, None, 2, None, 2],
    8: [None, None],
    9: [251, 0, 0],
    10: [-7, 2, -1],
    11: [-251, 0, 0],
    12: [6] + [0] * 10,
    13: [1, 2, 5],
    14: [-1, -2, -5],
}

BIG = (20, 21, 22, 23, 24)


def big_pattern(g: int, p: int) -> list:
    if g == 20:
        m = 10**p - 1
        return [m, m]
    if g == 21:
        v = 10 ** (min(p, 34) - 1)
        return [v, v, v + 1]
    if g == 22:
        w = 9 * 10 ** (min(p, 34) - 1)
        return [w, w]
    if g == 23:
        v = 10 ** (min(p, 34) - 1)
        return [-v, -v, -v - 1]
    if g == 24:
        h = 5 * 10 ** (p - 1)
        return [h, h]
    return []


def pattern(g: int, p: int) -> list:
    if g in NORMAL:
        return NORMAL[g]
    return big_pattern(g, p)


def col_name(p: int, s: int) -> str:
    return f"c{p}_{s}"


def to_text(unscaled: int | None, s: int) -> str | None:
    if unscaled is None:
        return None
    return format(Decimal(unscaled).scaleb(-s), "f")


def avg_rows() -> tuple[list, list]:
    names = ["g", "id"] + [col_name(p, s) for p, s in AVG_COLS] + ["i", "b", "x"]
    rows = []
    rid = 0
    for g in list(NORMAL) + list(BIG):
        width = max(len(pattern(g, p)) for p, _ in AVG_COLS)
        for k in range(width):
            rid += 1
            row = [str(g), str(rid)]
            for p, s in AVG_COLS:
                pat = pattern(g, p)
                v = pat[k] if k < len(pat) else None
                if v is not None and abs(v) >= 10**p:
                    v = None
                row.append(to_text(v, s))
            base = NORMAL.get(g)
            iv = base[k] if base is not None and k < len(base) else None
            row += [None if iv is None else str(iv)] * 2
            row.append(None if iv is None else str(float(iv)))
            rows.append(row)
    return names, rows


def avg_types() -> dict:
    t = {"g": "INT", "id": "INT", "i": "INT", "b": "BIGINT", "x": "DOUBLE"}
    for p, s in AVG_COLS:
        t[col_name(p, s)] = f"DECIMAL({p},{s})"
    return t


def spark_avg_expect(g: int, p: int, s: int) -> str | None:
    vals = [v for v in pattern(g, p) if v is not None and abs(v) < 10**p]
    if not vals:
        return None
    q = Decimal(sum(vals)).scaleb(-s) / Decimal(len(vals))
    ts = min(s + 4, 38)
    return format(q.quantize(Decimal(1).scaleb(-ts), rounding=ROUND_HALF_UP), "f")


DIV_COLS = [
    (10, 0),
    (10, 2),
    (18, 4),
    (20, 2),
    (28, 2),
    (28, 10),
    (34, 4),
    (38, 0),
    (38, 2),
    (38, 10),
]
DIV_UNSCALED = [30, 100, 251, -251, 1, 1000, None, 777, 6, "max"]


def div_rows() -> tuple[list, list]:
    names = ["id"] + [f"d{p}_{s}" for p, s in DIV_COLS] + ["bi", "bl", "bs", "by"]
    rows = []
    for k, u in enumerate(DIV_UNSCALED):
        row = [str(k + 1)]
        for p, s in DIV_COLS:
            v = 10**p - 1 if u == "max" else u
            row.append(to_text(v, s))
        row += ["3", "3", "3", "3"]
        rows.append(row)
    return names, rows


def div_types() -> dict:
    t = {"id": "INT", "bi": "INT", "bl": "BIGINT", "bs": "SMALLINT", "by": "TINYINT"}
    for p, s in DIV_COLS:
        t[f"d{p}_{s}"] = f"DECIMAL({p},{s})"
    return t


LITERALS = {
    "i3": "3",
    "s3": "3S",
    "y3": "3Y",
    "l3": "3L",
    "i100": "100",
    "neg3": "-3",
    "big3e9": "3000000000",
    "dec3e19": "30000000000000000000",
    "i11": "11",
    "dec3_0": "3.0",
}
TYPED = {
    "cast_int": "CAST(3 AS INT)",
    "cast_bigint": "CAST(3 AS BIGINT)",
    "col_int": "bi",
    "col_bigint": "bl",
    "col_smallint": "bs",
    "col_tinyint": "by",
}
CONTROL_OPS = {"mod": "%", "mul": "*", "add": "+", "sub": "-"}

AVG_SQL_FORMS = {
    "grouped": "SELECT g, avg({c}) AS m FROM {t} WHERE g IN ({normal}) GROUP BY g",
    "mean": "SELECT g, mean({c}) AS m FROM {t} WHERE g IN ({normal}) GROUP BY g",
    "distinct": "SELECT g, avg(DISTINCT {c}) AS m FROM {t} WHERE g IN ({normal}) GROUP BY g",
    "try_avg": "SELECT g, try_avg({c}) AS m FROM {t} WHERE g IN ({normal}) GROUP BY g",
    "win_part": "SELECT g, id, avg({c}) OVER (PARTITION BY g) AS m FROM {t} WHERE g IN ({normal})",
    "win_rows": (
        "SELECT g, id, avg({c}) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND "
        "CURRENT ROW) AS m FROM {t} WHERE g IN ({normal})"
    ),
    "win_rows3": (
        "SELECT g, id, avg({c}) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND "
        "CURRENT ROW) AS m FROM {t} WHERE g IN ({normal})"
    ),
    "win_running": (
        "SELECT g, id, avg({c}) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED "
        "PRECEDING AND CURRENT ROW) AS m FROM {t} WHERE g IN ({normal})"
    ),
    "win_range": (
        "SELECT g, id, avg({c}) OVER (PARTITION BY g ORDER BY id RANGE BETWEEN 2 PRECEDING AND "
        "CURRENT ROW) AS m FROM {t} WHERE g IN ({normal})"
    ),
    "having_gt": (
        "SELECT g FROM {t} WHERE g IN ({normal}) GROUP BY g HAVING avg({c}) * count({c}) > sum({c})"
    ),
    "having_eq": "SELECT g FROM {t} WHERE g IN ({normal}) GROUP BY g HAVING avg({c}) = {case}",
    "beside": (
        "SELECT g, avg({c}) + max({c}) AS a, avg({c}) - 1 AS b2, avg({c}) * 2 AS c2 FROM {t} "
        "WHERE g IN ({normal}) GROUP BY g"
    ),
    "sum_div_count": (
        "SELECT g, sum({c}) / count({c}) AS m FROM {t} WHERE g IN ({normal}) GROUP BY g"
    ),
    "beside_const": (
        "SELECT g, avg({c}) AS m, CAST('1.5' AS DECIMAL(10,2)) AS k FROM {t} "
        "WHERE g IN ({normal}) GROUP BY g"
    ),
}


def normal_list() -> str:
    return ", ".join(str(g) for g in NORMAL)


def having_case(p: int, s: int) -> str:
    ty = f"DECIMAL({min(p + 4, 38)},{min(s + 4, 38)})"
    arms = []
    for g in NORMAL:
        v = spark_avg_expect(g, p, s)
        if v is not None:
            arms.append(f"WHEN {g} THEN CAST('{v}' AS {ty})")
    return "CASE g " + " ".join(arms) + " END"


def avg_sql_cells(t: str) -> list:
    out = []
    for p, s in AVG_COLS:
        c = col_name(p, s)
        for g in list(NORMAL) + list(BIG):
            out.append(
                (
                    f"avg/ungrouped/{c}/g{g}",
                    "avg",
                    "ungrouped",
                    c,
                    f"SELECT avg({c}) AS m FROM {t} WHERE g = {g}",
                )
            )
        for form, tmpl in AVG_SQL_FORMS.items():
            sql = tmpl.format(c=c, t=t, normal=normal_list(), case=having_case(p, s))
            out.append((f"avg/{form}/{c}", "avg", form, c, sql))
    for c in ("i", "b", "x"):
        for form in ("grouped", "win_rows", "distinct"):
            sql = AVG_SQL_FORMS[form].format(c=c, t=t, normal=normal_list(), case="")
            out.append((f"avgctl/{form}/{c}", "avgctl", form, c, sql))
    return out


def div_sql_cells(t: str) -> list:
    out = []
    for p, s in DIV_COLS:
        d = f"d{p}_{s}"
        for lk, lit in LITERALS.items():
            out.append(
                (
                    f"div/lit_right/{d}/{lk}",
                    "div",
                    "lit_right",
                    d,
                    f"SELECT id, {d} / {lit} AS q FROM {t}",
                )
            )
            out.append(
                (
                    f"div/lit_left/{d}/{lk}",
                    "div",
                    "lit_left",
                    d,
                    f"SELECT id, {lit} / {d} AS q FROM {t}",
                )
            )
        for tk, ex in TYPED.items():
            out.append(
                (f"div/typed/{d}/{tk}", "div", "typed", d, f"SELECT id, {d} / {ex} AS q FROM {t}")
            )
        for ok, op in CONTROL_OPS.items():
            out.append(
                (f"divctl/{ok}/{d}/i3", "divctl", ok, d, f"SELECT id, {d} {op} 3 AS q FROM {t}")
            )
        out.append(
            (f"divctl/mul/{d}/neg3", "divctl", "mul", d, f"SELECT id, {d} * -3 AS q FROM {t}")
        )
        out.append(
            (
                f"div/try_divide/{d}/i3",
                "div",
                "try_divide",
                d,
                f"SELECT id, try_divide({d}, 3) AS q FROM {t}",
            )
        )
        out.append(
            (f"divctl/mod/{d}/col_int", "divctl", "mod", d, f"SELECT id, {d} % bi AS q FROM {t}")
        )
        out.append(
            (
                f"div/where_round/{d}",
                "div",
                "where_round",
                d,
                f"SELECT id FROM {t} WHERE {d} / 3 = round({d} / 3, 6)",
            )
        )
        out.append(
            (
                f"div/case_round/{d}",
                "div",
                "case_round",
                d,
                f"SELECT id, CASE WHEN {d} / 3 = round({d} / 3, 6) THEN 'six' ELSE 'long' END "
                f"AS r FROM {t}",
            )
        )
    out.append(
        (
            "div/where_lit/d10_2",
            "div",
            "where_lit",
            "d10_2",
            f"SELECT id FROM {t} WHERE d10_2 / 3 = 0.836667",
        )
    )
    out.append(
        (
            "div/case_lit/d10_2",
            "div",
            "case_lit",
            "d10_2",
            f"SELECT id, CASE WHEN d10_2 / 3 > 0.8366667 THEN 'hi' ELSE 'lo' END AS r FROM {t} "
            "WHERE id = 3",
        )
    )
    out.append(("div/literal_only", "div", "literal_only", "lit", "SELECT 1.0 / 3 AS q"))
    return out


H_SCOPES = {
    "derived": "SELECT id, h / 2 AS v FROM (SELECT id, d10_2 / 2 AS h FROM {t}) s",
    "cte": "WITH s AS (SELECT id, d10_2 / 2 AS h FROM {t}) SELECT id, h / 2 AS v FROM s",
    "tempview": "SELECT id, h / 2 AS v FROM tv_h",
    "sessview": "SELECT id, h / 2 AS v FROM default.sv_h",
    "flat": "SELECT id, d10_2 / 2 / 2 AS v FROM {t}",
}

CONF_KEYS = {
    "apl_false": ("spark.sql.decimalOperations.allowPrecisionLoss", "false"),
    "pmp_false": ("spark.sql.legacy.literal.pickMinimumPrecision", "false"),
}
