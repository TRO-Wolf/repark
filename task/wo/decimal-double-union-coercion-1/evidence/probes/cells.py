LEFT = {
    "dec_0.1": "0.1",
    "dec_1.5": "1.5",
    "dec_18_9": "123456789.123456789",
    "dec_0.001": "0.001",
    "dec_38_18": "CAST(0.5 AS DECIMAL(38,18))",
    "dec_col": "dc",
    "int": "1",
    "bigint": "CAST(2 AS BIGINT)",
    "null": "NULL",
    "str_digit": "'3'",
}
RIGHT = {
    "dbl_1e-20": "CAST(1e-20 AS DOUBLE)",
    "dbl_1e30": "CAST(1e30 AS DOUBLE)",
    "dbl_neg0": "CAST('-0.0' AS DOUBLE)",
    "dbl_nan": "CAST('NaN' AS DOUBLE)",
    "dbl_inf": "CAST('Infinity' AS DOUBLE)",
    "dbl_col": "dd",
    "flt_1.5": "CAST(1.5 AS FLOAT)",
    "flt_1e-20": "CAST(1e-20 AS FLOAT)",
}
COLUMN_CLASSES = {"dec_col", "dbl_col"}
CONSTRUCTS = {
    "union_all": "SELECT {L} AS c FROM src UNION ALL SELECT {R} FROM src",
    "union_all_rev": "SELECT {R} AS c FROM src UNION ALL SELECT {L} FROM src",
    "union": "SELECT {L} AS c FROM src UNION SELECT {R} FROM src",
    "intersect": "SELECT {L} AS c FROM src INTERSECT SELECT {R} FROM src",
    "except": "SELECT {L} AS c FROM src EXCEPT SELECT {R} FROM src",
    "values": "SELECT c FROM VALUES ({L}), ({R}) AS v(c)",
    "values_rev": "SELECT c FROM VALUES ({R}), ({L}) AS v(c)",
    "case": "SELECT CASE WHEN id = 2 THEN {L} ELSE {R} END AS c FROM src",
    "if": "SELECT IF(id = 2, {L}, {R}) AS c FROM src",
    "coalesce": "SELECT coalesce({R}, {L}) AS c FROM src",
    "nvl": "SELECT nvl({R}, {L}) AS c FROM src",
    "ifnull": "SELECT ifnull({R}, {L}) AS c FROM src",
    "nvl2": "SELECT nvl2(id, {R}, {L}) AS c FROM src",
    "nullif_rl": "SELECT nullif({R}, {L}) AS c FROM src",
    "nullif_lr": "SELECT nullif({L}, {R}) AS c FROM src",
    "greatest": "SELECT greatest({L}, {R}) AS c FROM src",
    "least": "SELECT least({L}, {R}) AS c FROM src",
    "in_rl": "SELECT {R} IN ({L}) AS c FROM src",
    "in_lr": "SELECT {L} IN ({R}) AS c FROM src",
    "insub_rl": "SELECT {R} IN (SELECT {L} FROM src) AS c FROM src",
    "insub_lr": "SELECT {L} IN (SELECT {R} FROM src) AS c FROM src",
    "array": "SELECT array({L}, {R}) AS c FROM src",
    "map_value": "SELECT map('a', {L}, 'b', {R}) AS c FROM src",
    "ctl_eq": "SELECT {R} = {L} AS c FROM src",
    "ctl_add": "SELECT {R} + {L} AS c FROM src",
}
RECURSIVE = {
    "rcte_dec_anchor": (
        "WITH RECURSIVE r(n, c) AS (SELECT 1, 0.5 UNION ALL SELECT n + 1, CAST(1e-20 AS "
        "DOUBLE) FROM r WHERE n < 2) SELECT c FROM r"
    ),
    "rcte_dbl_anchor": (
        "WITH RECURSIVE r(n, c) AS (SELECT 1, CAST(1e-20 AS DOUBLE) UNION ALL SELECT n + 1, "
        "0.5 FROM r WHERE n < 2) SELECT c FROM r"
    ),
    "rcte_dec_anchor_big": (
        "WITH RECURSIVE r(n, c) AS (SELECT 1, 0.5 UNION ALL SELECT n + 1, CAST(1e30 AS "
        "DOUBLE) FROM r WHERE n < 2) SELECT c FROM r"
    ),
    "cte_union": (
        "WITH a AS (SELECT 0.5 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)) SELECT c FROM a"
    ),
    "view_union": "SELECT c FROM vu",
    "subquery_union_filter": (
        "SELECT c FROM (SELECT 0.0 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)) WHERE c > 0"
    ),
    "union_three_way": "SELECT 0.1 AS c UNION ALL SELECT 1 UNION ALL SELECT CAST(1e-20 AS DOUBLE)",
    "union_nested_cast": (
        "SELECT CAST(c AS STRING) AS c FROM (SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS "
        "DOUBLE))"
    ),
    "union_sum": "SELECT sum(c) AS c FROM (SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE))",
    "values_three_rows": "SELECT c FROM VALUES (0.1), (1), (CAST(1e-20 AS DOUBLE)) AS v(c)",
    "values_two_cols": "SELECT c FROM VALUES (1, 0.1), (2, CAST(1e-20 AS DOUBLE)) AS v(id, c)",
    "values_nan": "SELECT c FROM VALUES (CAST('NaN' AS DOUBLE)), (1.5) AS v(c)",
    "coalesce_nan_card": (
        "SELECT coalesce(x, -1.0) AS c FROM VALUES (CAST('NaN' AS DOUBLE)), (1.5) AS t(x)"
    ),
    "case_simple_when": "SELECT CASE dd WHEN 0.0 THEN 'zero' ELSE 'other' END AS c FROM src",
    "where_eq_literal": "SELECT count(*) AS c FROM src WHERE dd = 0.0",
    "where_in_literal": "SELECT count(*) AS c FROM src WHERE dd IN (0.0, 0.5)",
    "where_dec_in_dbl": "SELECT count(*) AS c FROM src WHERE 0.0 IN (dd)",
    "join_on_mixed": (
        "SELECT count(*) AS c FROM (SELECT 0.0 AS k) a JOIN (SELECT dd AS k FROM src) b ON "
        "a.k = b.k"
    ),
    "sort_union": (
        "SELECT c FROM (SELECT 0.0 AS c UNION ALL SELECT CAST(-1e-20 AS DOUBLE)) ORDER BY c"
    ),
}
SETUP = [
    "CREATE OR REPLACE TEMP VIEW vu AS SELECT 0.5 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)",
]
SRC_SCHEMA = (
    "id INT, dc DECIMAL(10,2), dd DOUBLE, dbig DOUBLE, ff FLOAT, ii INT, bb BIGINT, ss STRING"
)


def src_rows() -> list:
    from decimal import Decimal

    return [(1, Decimal("1.25"), 1e-20, 1e30, 1.5, 7, 8, "3")]


def select_cells() -> list:
    out = []
    for cname, template in CONSTRUCTS.items():
        for lname, ltext in LEFT.items():
            for rname, rtext in RIGHT.items():
                if cname.startswith("values") and (
                    lname in COLUMN_CLASSES or rname in COLUMN_CLASSES
                ):
                    continue
                key = f"{cname}/{lname}/{rname}"
                out.append((key, template.format(L=ltext, R=rtext)))
    for key, sql in RECURSIVE.items():
        out.append((f"extra/{key}", sql))
    return out
