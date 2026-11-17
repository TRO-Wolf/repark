from decimal import ROUND_DOWN, Decimal

import cells

SHAPE_COLS = ("c10_2", "c28_2", "c38_18")
DIV_SAMPLE = ("d10_2", "d28_2", "d38_2")
TRY = {
    "try_add": "try_add({d}, 1)",
    "try_subtract": "try_subtract({d}, 1)",
    "try_multiply": "try_multiply({d}, 3)",
    "try_divide": "try_divide({d}, 3)",
    "try_mod": "try_mod({d}, 3)",
}
MODS = {"mod": "mod({d}, 3)", "pmod": "pmod({d}, 3)", "pct": "{d} % 3", "pmod_neg": "pmod({d}, -3)"}
MAX38 = "9" * 38
S04 = [MAX38, MAX38, "-" + MAX38, "-" + MAX38, "5"]


def trunc_avg(g: int, p: int, s: int) -> str | None:
    vals = [v for v in cells.pattern(g, p) if v is not None and abs(v) < 10**p]
    if not vals:
        return None
    q = Decimal(sum(vals)).scaleb(-s) / Decimal(len(vals))
    ts = min(s + 4, 38)
    return format(q.quantize(Decimal(1).scaleb(-ts), rounding=ROUND_DOWN), "f")


def trunc_case(p: int, s: int) -> str:
    ty = f"DECIMAL({min(p + 4, 38)},{min(s + 4, 38)})"
    arms = [
        f"WHEN {g} THEN CAST('{v}' AS {ty})"
        for g in cells.NORMAL
        if (v := trunc_avg(g, p, s)) is not None
    ]
    return "CASE g " + " ".join(arms) + " END"


def mean_cells(t: str) -> list:
    out = []
    normal = cells.normal_list()
    for p, s in cells.AVG_COLS:
        c = cells.col_name(p, s)
        for g in list(cells.NORMAL) + list(cells.BIG):
            out.append(
                (
                    f"mean/ungrouped/{c}/g{g}",
                    "mean",
                    "ungrouped",
                    c,
                    f"SELECT mean({c}) AS m FROM {t} WHERE g = {g}",
                )
            )
        for form, tmpl in cells.AVG_SQL_FORMS.items():
            if form in ("mean", "try_avg", "sum_div_count"):
                continue
            sql = tmpl.format(c=c, t=t, normal=normal, case=cells.having_case(p, s))
            out.append((f"mean/{form}/{c}", "mean", form, c, sql.replace("avg(", "mean(")))
        for fn in ("avg", "mean"):
            out.append(
                (
                    f"mixed/{fn}_distinct_count/{c}",
                    "mixed",
                    f"{fn}_distinct_count",
                    c,
                    f"SELECT g, {fn}(DISTINCT {c}) AS m, count({c}) AS n FROM {t} "
                    f"WHERE g IN ({normal}) GROUP BY g",
                )
            )
    return out


def shape_cells(t: str, v: str) -> list:
    out = []
    normal = cells.normal_list()
    for c in SHAPE_COLS:
        p, s = (int(x) for x in c[1:].split("_"))
        base = f"FROM {t} WHERE g IN ({normal})"
        for name, grp in (
            ("grouping_sets", "GROUPING SETS ((g), ())"),
            ("rollup", "ROLLUP(g)"),
            ("cube", "CUBE(g)"),
        ):
            out.append(
                (
                    f"shape/{name}/{c}",
                    "shape",
                    name,
                    c,
                    f"SELECT g, avg({c}) AS m {base} GROUP BY {grp}",
                )
            )
        out.append(
            (
                f"shape/union_all/{c}",
                "shape",
                "union_all",
                c,
                f"SELECT 2 AS k, avg({c}) AS m FROM {t} WHERE g = 2 UNION ALL "
                f"SELECT 9 AS k, avg({c}) AS m FROM {t} WHERE g = 9",
            )
        )
        out.append(
            (
                f"shape/having_trunc/{c}",
                "shape",
                "having_trunc",
                c,
                f"SELECT g {base} GROUP BY g HAVING avg({c}) = {trunc_case(p, s)}",
            )
        )
    for p, s in cells.DIV_COLS:
        d = f"d{p}_{s}"
        out.append(
            (f"shape/avg_div/{d}", "shape", "avg_div", d, f"SELECT avg({d} / 3) AS m FROM {v}")
        )
        out.append(
            (f"shape/mean_div/{d}", "shape", "mean_div", d, f"SELECT mean({d} / 3) AS m FROM {v}")
        )
        out.append(
            (f"shape/avg_mul/{d}", "shape", "avg_mul", d, f"SELECT avg({d} * 2) AS m FROM {v}")
        )
        out.append(
            (
                f"shape/fold_div/{d}",
                "shape",
                "fold_div",
                d,
                f"SELECT id, {d} / (1 + 2) AS q FROM {v}",
            )
        )
        out.append(
            (
                f"round/round_div/{d}",
                "round",
                "round_div",
                d,
                f"SELECT id, round({d} / 3, 2) AS q FROM {v}",
            )
        )
        out.append(
            (
                f"round/round_col/{d}",
                "round",
                "round_col",
                d,
                f"SELECT id, round({d}, 1) AS q FROM {v}",
            )
        )
        for k, tmpl in MODS.items():
            out.append(
                (f"mod/{k}/{d}", "mod", k, d, f"SELECT id, {tmpl.format(d=d)} AS q FROM {v}")
            )
        for k, tmpl in TRY.items():
            out.append(
                (f"try/{k}/{d}", "try", k, d, f"SELECT id, {tmpl.format(d=d)} AS q FROM {v}")
            )
            out.append(
                (
                    f"try/{k}_rows1to9/{d}",
                    "try",
                    f"{k}_rows1to9",
                    d,
                    f"SELECT id, {tmpl.format(d=d)} AS q FROM {v} WHERE id <> 10",
                )
            )
    return out
