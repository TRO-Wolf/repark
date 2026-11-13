from __future__ import annotations

from typing import Any


def lit(value: str, sql_type: str) -> str:
    return f"CAST('{value}' AS {sql_type})"


def sql_ops(t: str) -> list[tuple[str, str, str]]:
    nan = lit("NaN", t)
    inf = lit("Infinity", t)
    return [
        ("agg_max", "agg", "max({c})"),
        ("agg_min", "agg", "min({c})"),
        ("win_max_all", "proj", "max({c}) OVER ()"),
        (
            "win_max_run",
            "proj",
            "max({c}) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        ),
        (
            "win_min_run",
            "proj",
            "min({c}) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        ),
        (
            "win_max_slide",
            "proj",
            "max({c}) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
        ),
        (
            "win_min_slide",
            "proj",
            "min({c}) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
        ),
        ("greatest", "proj", f"greatest({{c}}, CAST(100 AS {t}))"),
        ("least", "proj", f"least({{c}}, CAST(-100 AS {t}))"),
        ("ord_asc", "sort", "{c} ASC, id"),
        ("ord_desc", "sort", "{c} DESC, id"),
        ("ord_asc_nl", "sort", "{c} ASC NULLS LAST, id"),
        ("ord_desc_nf", "sort", "{c} DESC NULLS FIRST, id"),
        ("topk_asc", "sort", "{c} ASC NULLS LAST, id LIMIT 3"),
        ("topk_desc", "sort", "{c} DESC NULLS LAST, id LIMIT 3"),
        ("win_rank", "proj", "rank() OVER (ORDER BY {c} DESC NULLS LAST)"),
        ("f_eq_nan", "filter", f"{{c}} = {nan}"),
        ("f_ne_nan", "filter", f"{{c}} <> {nan}"),
        ("f_lt_nan", "filter", f"{{c}} < {nan}"),
        ("f_le_nan", "filter", f"{{c}} <= {nan}"),
        ("f_gt_nan", "filter", f"{{c}} > {nan}"),
        ("f_ge_nan", "filter", f"{{c}} >= {nan}"),
        ("f_nseq_nan", "filter", f"{{c}} <=> {nan}"),
        ("f_in_nan", "filter", f"{{c}} IN ({nan}, CAST(1.5 AS {t}))"),
        ("f_notin_nan", "filter", f"{{c}} NOT IN ({nan})"),
        ("f_between", "filter", f"{{c}} BETWEEN CAST(0 AS {t}) AND {nan}"),
        ("f_gt_inf", "filter", f"{{c}} > {inf}"),
        ("f_ge_inf", "filter", f"{{c}} >= {inf}"),
        ("f_gt_half", "filter", f"{{c}} > CAST(0.5 AS {t})"),
        ("f_lt_zero", "filter", f"{{c}} < CAST(0 AS {t})"),
        ("f_le_neginf", "filter", f"{{c}} <= {lit('-Infinity', t)}"),
        ("f_isnan", "filter", "isnan({c})"),
        ("f_eq_col", "filter", "{c} = {k}"),
        ("p_eq_nan", "proj", f"{{c}} = {nan}"),
        ("p_gt_inf", "proj", f"{{c}} > {inf}"),
        ("p_lt_nan", "proj", f"{{c}} < {nan}"),
        ("p_eq_col", "proj", "{c} = {k}"),
        ("p_lt_col", "proj", "{c} < {k}"),
        ("case_gt", "proj", f"CASE WHEN {{c}} > CAST(1 AS {t}) THEN 'hi' ELSE 'lo' END"),
        ("case_eq", "proj", f"CASE {{c}} WHEN {nan} THEN 'nan' ELSE 'other' END"),
        ("arr_sort", "agg", "array_sort(collect_list({c}))"),
        ("arr_min", "agg", "array_min(collect_list({c}))"),
        ("arr_max", "agg", "array_max(collect_list({c}))"),
        ("sort_arr_asc", "agg", "sort_array(collect_list({c}))"),
        ("sort_arr_desc", "agg", "sort_array(collect_list({c}), false)"),
        ("cnt_distinct", "agg", "count(DISTINCT {c})"),
        ("approx_distinct", "agg", "approx_count_distinct({c})"),
        (
            "grp_max",
            "special",
            "SELECT id % 2 AS g, max({c}) AS r FROM {t} GROUP BY id % 2 ORDER BY g",
        ),
        (
            "grp_min",
            "special",
            "SELECT id % 2 AS g, min({c}) AS r FROM {t} GROUP BY id % 2 ORDER BY g",
        ),
        ("grp_max_one", "special", "SELECT max({c}) AS r FROM {t} GROUP BY id > 0"),
        ("grp_min_one", "special", "SELECT min({c}) AS r FROM {t} GROUP BY id > 0"),
        ("group_by", "special", "SELECT {c} AS g, count(*) AS n FROM {t} GROUP BY {c}"),
        (
            "join_col",
            "special",
            "SELECT a.id, b.id AS bid FROM {t} a JOIN {t} b ON a.{c} = b.{k} ORDER BY 1, 2",
        ),
    ]


FLOAT_OPS = {
    "agg_max",
    "agg_min",
    "greatest",
    "least",
    "ord_asc",
    "ord_desc",
    "f_eq_nan",
    "f_gt_inf",
    "f_gt_half",
    "p_eq_nan",
    "arr_max",
    "cnt_distinct",
}
NEEDS_KEY = {"f_eq_col", "p_eq_col", "p_lt_col", "join_col"}


def sql_text(kind: str, frag: str, rel: str) -> str:
    if kind == "agg":
        return f"SELECT {frag} AS r FROM {rel}"
    if kind == "proj":
        return f"SELECT id, {frag} AS r FROM {rel} ORDER BY id"
    if kind == "filter":
        return f"SELECT id FROM {rel} WHERE {frag} ORDER BY id"
    if kind == "sort":
        return f"SELECT id FROM {rel} ORDER BY {frag}"
    return frag


def mix_ops(rel: str, cols: tuple[str, ...]) -> list[tuple[str, str]]:
    union = " UNION ALL ".join(f"SELECT id, '{c}' AS src, {c} AS x FROM {rel}" for c in cols)
    return [
        ("mix_distinct", f"SELECT count(DISTINCT x) AS r FROM ({union}) u"),
        ("mix_max", f"SELECT max(x) AS r, min(x) AS m FROM ({union}) u"),
        ("mix_group", f"SELECT x AS g, count(*) AS n FROM ({union}) u GROUP BY x"),
        ("mix_order", f"SELECT id, src FROM ({union}) u ORDER BY x DESC NULLS LAST, id, src"),
        (
            "mix_eq_nan",
            f"SELECT id, src FROM ({union}) u WHERE x = CAST('NaN' AS DOUBLE) ORDER BY id, src",
        ),
    ]


def frame_ops(fn: Any, win: Any, t: str) -> dict[str, Any]:
    nan = fn.lit(float("nan")).cast(t)
    inf = fn.lit(float("inf")).cast(t)

    def run(c: str) -> Any:
        return win.orderBy("id").rowsBetween(win.unboundedPreceding, win.currentRow)

    return {
        "agg_max": lambda s, c, k: s.agg(fn.max(c).alias("r")),
        "agg_min": lambda s, c, k: s.agg(fn.min(c).alias("r")),
        "win_max_run": lambda s, c, k: s.select("id", fn.max(c).over(run(c)).alias("r")).orderBy(
            "id"
        ),
        "greatest": lambda s, c, k: s.select(
            "id", fn.greatest(fn.col(c), fn.lit(100).cast(t)).alias("r")
        ).orderBy("id"),
        "least": lambda s, c, k: s.select(
            "id", fn.least(fn.col(c), fn.lit(-100).cast(t)).alias("r")
        ).orderBy("id"),
        "ord_asc": lambda s, c, k: s.orderBy(fn.col(c).asc(), "id").select("id"),
        "ord_desc": lambda s, c, k: s.orderBy(fn.col(c).desc(), "id").select("id"),
        "ord_asc_nl": lambda s, c, k: s.orderBy(fn.col(c).asc_nulls_last(), "id").select("id"),
        "ord_desc_nf": lambda s, c, k: s.orderBy(fn.col(c).desc_nulls_first(), "id").select("id"),
        "topk_desc": lambda s, c, k: (
            s.orderBy(fn.col(c).desc_nulls_last(), "id").limit(3).select("id")
        ),
        "f_eq_nan": lambda s, c, k: s.filter(fn.col(c) == nan).select("id").orderBy("id"),
        "f_lt_nan": lambda s, c, k: s.filter(fn.col(c) < nan).select("id").orderBy("id"),
        "f_nseq_nan": lambda s, c, k: (
            s.filter(fn.col(c).eqNullSafe(nan)).select("id").orderBy("id")
        ),
        "f_in_nan": lambda s, c, k: (
            s.filter(fn.col(c).isin(float("nan"), 1.5)).select("id").orderBy("id")
        ),
        "f_between": lambda s, c, k: (
            s.filter(fn.col(c).between(fn.lit(0).cast(t), nan)).select("id").orderBy("id")
        ),
        "f_gt_inf": lambda s, c, k: s.filter(fn.col(c) > inf).select("id").orderBy("id"),
        "f_gt_half": lambda s, c, k: (
            s.filter(fn.col(c) > fn.lit(0.5).cast(t)).select("id").orderBy("id")
        ),
        "f_eq_col": lambda s, c, k: s.filter(fn.col(c) == fn.col(k)).select("id").orderBy("id"),
        "p_eq_nan": lambda s, c, k: s.select("id", (fn.col(c) == nan).alias("r")).orderBy("id"),
        "arr_sort": lambda s, c, k: s.agg(fn.array_sort(fn.collect_list(c)).alias("r")),
        "arr_max": lambda s, c, k: s.agg(fn.array_max(fn.collect_list(c)).alias("r")),
        "sort_arr_desc": lambda s, c, k: s.agg(
            fn.sort_array(fn.collect_list(c), asc=False).alias("r")
        ),
        "cnt_distinct": lambda s, c, k: s.agg(fn.countDistinct(c).alias("r")),
        "approx_distinct": lambda s, c, k: s.agg(fn.approx_count_distinct(c).alias("r")),
    }


def fexpr_frame(fn: Any, kind: str, frag: str) -> Any:
    if kind == "agg":
        return lambda s: s.agg(fn.expr(frag).alias("r"))
    if kind == "proj" and "OVER" not in frag:
        return lambda s: s.select("id", fn.expr(frag).alias("r")).orderBy("id")
    if kind == "filter":
        return lambda s: s.filter(fn.expr(frag)).select("id").orderBy("id")
    return None


PRODUCERS = (
    ("cast_nan", "CAST('NaN' AS DOUBLE)", "n"),
    ("neg_nan", "-CAST('NaN' AS DOUBLE)", "-n"),
    ("div00", "CAST(0.0 AS DOUBLE) / CAST(0.0 AS DOUBLE)", "z / z"),
    ("sqrt_m1", "sqrt(CAST(-1.0 AS DOUBLE))", "sqrt(m)"),
    ("ln_m1", "ln(CAST(-1.0 AS DOUBLE))", "ln(m)"),
    ("log10_m1", "log10(CAST(-1.0 AS DOUBLE))", "log10(m)"),
    ("inf_sub", "CAST('Infinity' AS DOUBLE) - CAST('Infinity' AS DOUBLE)", "i - i"),
    ("inf_mul0", "CAST('Infinity' AS DOUBLE) * CAST(0.0 AS DOUBLE)", "i * z"),
    ("inf_div", "CAST('Infinity' AS DOUBLE) / CAST('Infinity' AS DOUBLE)", "i / i"),
    ("inf_mod", "CAST('Infinity' AS DOUBLE) % CAST(1.0 AS DOUBLE)", "i % CAST(1.0 AS DOUBLE)"),
)
PRODUCER_SRC = (
    "(VALUES (CAST('NaN' AS DOUBLE), CAST(0.0 AS DOUBLE), CAST(-1.0 AS DOUBLE), "
    "CAST('Infinity' AS DOUBLE))) AS p(n, z, m, i)"
)
