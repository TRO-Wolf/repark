from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Decimal
from typing import Any

DECIMAL_TEXT = re.compile(r"-?\d+\.\d+")

COLUMNS = [
    ("id", "INT", ["1", "2", "3", "4", "5", "6", "7", "8"]),
    ("g", "INT", ["1", "0", "1", "0", "1", "0", "1", "0"]),
    ("a", "DECIMAL(38,0)", ["1", "2", "-2", "1", "0", None, "9" * 38, "-" + "9" * 38]),
    (
        "c",
        "DECIMAL(10,2)",
        ["1.25", "1.50", "-1.50", "0.50", "0.49", "1.45", "99999999.99", "-99999999.99"],
    ),
    ("d", "DECIMAL(38,1)", ["1.5", "2.5", "-2.5", "0.5", "-0.5", "1.4", "9" * 37 + ".9", "-0.5"]),
    (
        "b",
        "DECIMAL(38,38)",
        [
            "0.5",
            "-0.5",
            "0.4" + "9" * 37,
            "0.5" + "0" * 36 + "1",
            "0.12345678901234567890123456789012345678",
            None,
            "0." + "9" * 38,
            "-0." + "9" * 38,
        ],
    ),
    ("e", "DECIMAL(20,0)", ["1", "1", "0", "0", "1", None, "9" * 20, "-" + "9" * 20]),
    (
        "f",
        "DECIMAL(20,20)",
        [
            "0.5",
            "0.99999999999999999999",
            "-0.00000000000000000005",
            "0.12345678901234567895",
            "0.99999999999999999950",
            None,
            "0.00000000000000000049",
            "-0.5",
        ],
    ),
    (
        "gg",
        "DECIMAL(38,10)",
        ["1.0000000001", "2.5", "-1.0000000001", "0", "1", None, "9" * 28 + "." + "9" * 10, "-1"],
    ),
    (
        "h",
        "DECIMAL(38,20)",
        [
            "1.00000000005000000000",
            "2.5",
            "-1.00000000005000000000",
            "0.00000000004999999999",
            "1.00000000000000000049",
            None,
            "9" * 18 + "." + "9" * 20,
            "0.00000000000000000050",
        ],
    ),
    (
        "i64",
        "BIGINT",
        ["1", "2", "-1", "0", "1", None, "9223372036854775807", "-9223372036854775808"],
    ),
    ("i32", "INT", ["1", "2", "-1", "0", "1", None, "2147483647", "-2147483648"]),
    ("i16", "SMALLINT", ["1", "2", "-1", "0", "1", None, "32767", "-32768"]),
    ("i8", "TINYINT", ["1", "2", "-1", "0", "1", None, "127", "-128"]),
    (
        "w30",
        "DECIMAL(38,30)",
        [
            "1.000000000000000000000000000049",
            "2.5",
            "-1.000000000000000000000000000050",
            "0",
            "0.999999999999999999999999999950",
            None,
            "99999999." + "9" * 30,
            "1",
        ],
    ),
    (
        "w34",
        "DECIMAL(38,34)",
        [
            "1.0000000000000000000000000000000004",
            "2.5",
            "-1.0000000000000000000000000000000005",
            "0",
            "0.9999999999999999999999999999999995",
            None,
            "9999." + "9" * 34,
            "1",
        ],
    ),
    (
        "w36",
        "DECIMAL(38,36)",
        [
            "1.000000000000000000000000000000000004",
            "2.5",
            "-1.000000000000000000000000000000000005",
            "0",
            "0.999999999999999999999999999999999995",
            None,
            "99." + "9" * 36,
            "1",
        ],
    ),
    ("c0", "DECIMAL(10,0)", ["1", "2", "-2", "1", "0", None, "9999999999", "-9999999999"]),
    (
        "k28",
        "DECIMAL(28,2)",
        ["1.00", "1.00", "0.50", "1.00", "1.01", None, "9" * 26 + ".99", "1.00"],
    ),
    (
        "k4",
        "DECIMAL(10,4)",
        ["2.5050", "1.9950", "0.0001", "0.0000", None, "1.0000", "999999.9999", "0.0005"],
    ),
]

PAIRS = {
    "ab": ("a", "b"),
    "ac": ("a", "c"),
    "ad": ("a", "d"),
    "bc": ("b", "c"),
    "ef": ("e", "f"),
    "gh": ("gg", "h"),
    "i64h": ("i64", "h"),
    "i32w30": ("i32", "w30"),
    "i16w34": ("i16", "w34"),
    "i8w36": ("i8", "w36"),
    "c0c": ("c0", "c"),
    "i32c": ("i32", "c"),
    "i64c": ("i64", "c"),
    "k28k4": ("k28", "k4"),
}
CONTROL_PAIRS = ("c0c", "i32c", "i64c", "k28k4")
OTHER_CAUSES = ("except", "intersect", "nullif")
EXPRS = {
    "eq": "{x} = {y}",
    "eq_rev": "{y} = {x}",
    "ne": "{x} <> {y}",
    "lt": "{x} < {y}",
    "le": "{x} <= {y}",
    "gt": "{x} > {y}",
    "nseq": "{x} <=> {y}",
    "between": "{x} BETWEEN {y} AND {y}",
    "between_rev": "{y} BETWEEN {x} AND {x}",
    "in1": "{x} IN ({y})",
    "in_rev": "{y} IN ({x})",
    "not_in": "{x} NOT IN ({y})",
    "case": "CASE WHEN id % 2 = 0 THEN {x} ELSE {y} END",
    "case_simple": "CASE {x} WHEN {y} THEN 'm' ELSE 'n' END",
    "if": "if(id % 2 = 0, {x}, {y})",
    "coalesce": "coalesce({y}, {x})",
    "nvl2": "nvl2(id, {x}, {y})",
    "nullif": "nullif({x}, {y})",
    "greatest": "greatest({x}, {y})",
    "least": "least({x}, {y})",
    "array": "array({x}, {y})",
    "map": "map(1, {x}, 2, {y})",
    "array_contains": "array_contains(array({x}), {y})",
    "map_concat": "map_concat(map(1, {x}), map(2, {y}))",
}
PLANS = {
    "union_all": "SELECT {x} AS v FROM {t} UNION ALL SELECT {y} FROM {t}",
    "union_all_rev": "SELECT {y} AS v FROM {t} UNION ALL SELECT {x} FROM {t}",
    "union": "SELECT {x} AS v FROM {t} UNION SELECT {y} FROM {t}",
    "union3": (
        "SELECT {x} AS v FROM {t} UNION ALL SELECT {y} FROM {t} UNION ALL SELECT {x} FROM {t}"
    ),
    "except": "SELECT {x} AS v FROM {t} EXCEPT SELECT {y} FROM {t}",
    "intersect": "SELECT {x} AS v FROM {t} INTERSECT SELECT {y} FROM {t}",
    "where_eq": "SELECT id FROM {t} WHERE {x} = {y}",
    "where_eq_rev": "SELECT id FROM {t} WHERE {y} = {x}",
    "where_lt": "SELECT id FROM {t} WHERE {x} < {y}",
    "where_in": "SELECT id FROM {t} WHERE {x} IN ({y})",
    "where_between": "SELECT id FROM {t} WHERE {x} BETWEEN {y} AND {y}",
    "join_eq": "SELECT l.id AS li, r.id AS ri FROM {t} l JOIN {t} r ON l.{x} = r.{y}",
    "join_nseq": "SELECT l.id AS li, r.id AS ri FROM {t} l JOIN {t} r ON l.{x} <=> r.{y}",
    "in_subquery": "SELECT id FROM {t} WHERE {x} IN (SELECT {y} FROM {t})",
    "not_in_subquery": (
        "SELECT id FROM {t} WHERE {x} NOT IN (SELECT {y} FROM {t} WHERE {y} IS NOT NULL)"
    ),
    "scalar_subquery": "SELECT id FROM {t} WHERE {x} = (SELECT max({y}) FROM {t} WHERE id = 1)",
    "exists_corr": "SELECT id FROM {t} o WHERE EXISTS (SELECT 1 FROM {t} i WHERE i.{y} = o.{x})",
}
CORE = [
    "union_all",
    "union_all_rev",
    "eq",
    "eq_rev",
    "lt",
    "coalesce",
    "case",
    "in1",
    "where_eq",
    "join_eq",
]
FULL_PAIRS = ("ac", "ab", "gh", "i64h", "i32w30", "c0c")
TX_PAIRS = ("ac", "bc", "gh", "i64h", "i32w30")
CONSTS = {
    "a_eq_c125": "a = CAST('1.25' AS DECIMAL(10,2))",
    "c125_eq_a": "CAST('1.25' AS DECIMAL(10,2)) = a",
    "c_eq_a1": "c = CAST('1' AS DECIMAL(38,0))",
    "a1_eq_c": "CAST('1' AS DECIMAL(38,0)) = c",
    "c_lt_a1": "c < CAST('1' AS DECIMAL(38,0))",
    "c_ge_a1": "c >= CAST('1' AS DECIMAL(38,0))",
    "c_nseq_a1": "c <=> CAST('1' AS DECIMAL(38,0))",
    "c_in_a12": "c IN (CAST('1' AS DECIMAL(38,0)), CAST('2' AS DECIMAL(38,0)))",
    "c_notin_a12": "c NOT IN (CAST('1' AS DECIMAL(38,0)), CAST('2' AS DECIMAL(38,0)))",
    "c_between_a1": "c BETWEEN CAST('1' AS DECIMAL(38,0)) AND CAST('1' AS DECIMAL(38,0))",
    "a1_between_c": "CAST('1' AS DECIMAL(38,0)) BETWEEN c AND c",
    "a_in_c": "a IN (CAST('1.25' AS DECIMAL(10,2)), CAST('2.5' AS DECIMAL(10,2)))",
    "b_eq_a1": "b = CAST('1' AS DECIMAL(38,0))",
    "a_eq_b05": "a = CAST('0.5' AS DECIMAL(38,38))",
    "a_eq_lit125": "a = 1.25",
    "lit125_eq_a": "1.25 = a",
    "a_eq_lit15": "a = 1.5",
    "a_gt_lit05": "a > 0.5",
    "a_in_lit": "a IN (1.25, 2.5)",
    "d_eq_lit145": "d = 1.45",
    "h_eq_int1": "h = 1",
    "h_eq_big1": "h = CAST(1 AS BIGINT)",
    "i64_eq_hlit": "i64 = CAST('1.00000000000000000049' AS DECIMAL(38,20))",
    "w30_eq_int1": "w30 = 1",
    "i32_eq_w30lit": "i32 = CAST('1.000000000000000000000000000049' AS DECIMAL(38,30))",
    "c_eq_big1": "c = CAST(1 AS BIGINT)",
    "gg_eq_hlit": "gg = CAST('1.00000000005' AS DECIMAL(38,20))",
    "h_eq_glit": "h = CAST('1.0000000001' AS DECIMAL(38,10))",
    "fold_rounded_literal": "a = CAST(1.5 AS DECIMAL(10,2))",
}
W30 = "CAST('1.000000000000000000000000000049' AS DECIMAL(38,30))"
SHAPES = {
    "u3_flat": "SELECT c AS v FROM t UNION ALL SELECT d FROM t UNION ALL SELECT a FROM t",
    "u3_left": "(SELECT c AS v FROM t UNION ALL SELECT d FROM t) UNION ALL SELECT a FROM t",
    "u3_right": "SELECT a AS v FROM t UNION ALL (SELECT c FROM t UNION ALL SELECT d FROM t)",
    "u3_a_c_d": "SELECT a AS v FROM t UNION ALL SELECT c FROM t UNION ALL SELECT d FROM t",
    "u3_sub": (
        "SELECT v FROM (SELECT c AS v FROM t UNION ALL SELECT d FROM t) s UNION ALL SELECT a FROM t"
    ),
    "coalesce3": "SELECT id, coalesce(c, d, a) AS v FROM t",
    "coalesce_nested": "SELECT id, coalesce(coalesce(c, d), a) AS v FROM t",
    "greatest3": "SELECT id, greatest(c, d, a) AS v FROM t",
    "least3": "SELECT id, least(c, d, a) AS v FROM t",
    "in3": "SELECT id, c IN (d, a) AS v FROM t",
    "case3": "SELECT id, CASE id % 3 WHEN 0 THEN c WHEN 1 THEN d ELSE a END AS v FROM t",
    "array3": "SELECT id, array(c, d, a) AS v FROM t",
    "u3_int": "SELECT i64 AS v FROM t UNION ALL SELECT h FROM t UNION ALL SELECT c FROM t",
    "having_sum_const": (
        "SELECT g, sum(k28) AS s FROM t GROUP BY g HAVING sum(k28) = CAST('2.505' AS DECIMAL(10,4))"
    ),
    "having_sum_max": "SELECT g, sum(k28) AS s FROM t GROUP BY g HAVING sum(k28) = max(k4)",
    "sum_eq_select": "SELECT g, sum(k28) = max(k4) AS s FROM t GROUP BY g",
    "sum_union": "SELECT sum(k28) AS v FROM t GROUP BY g UNION ALL SELECT k4 FROM t",
    "sum_coalesce": "SELECT g, coalesce(sum(k28), max(k4)) AS s FROM t GROUP BY g",
    "max_case": "SELECT max(CASE WHEN id > 1 THEN a ELSE c END) AS r FROM t",
    "min_case": "SELECT min(CASE WHEN id > 1 THEN a ELSE c END) AS r FROM t",
    "sum_case": "SELECT sum(CASE WHEN id > 1 THEN a ELSE c END) AS r FROM t",
    "sum_of_union": "SELECT sum(v) AS r FROM (SELECT a AS v FROM t UNION ALL SELECT c AS v FROM t)",
    "gsets_coalesce": (
        "SELECT coalesce(a, c) AS k, count(*) AS n FROM t GROUP BY GROUPING SETS ((coalesce(a, "
        "c)), ())"
    ),
    "rollup_ac": (
        "SELECT coalesce(c, a) AS k, count(*) AS n FROM t GROUP BY ROLLUP (coalesce(c, a))"
    ),
    "gsets_ac_control": "SELECT a, c, count(*) AS n FROM t GROUP BY GROUPING SETS ((a), (c))",
    "v_ac": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0))), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)"
    ),
    "v_ca": (
        "SELECT * FROM VALUES (CAST('1.25' AS DECIMAL(10,2))), (CAST('1' AS DECIMAL(38,0))) AS v(x)"
    ),
    "v_int_c": "SELECT * FROM VALUES (1), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)",
    "v_int_lit": "SELECT * FROM VALUES (1), (1.25) AS v(x)",
    "v_big_h": (
        "SELECT * FROM VALUES (CAST(1 AS BIGINT)), (CAST('1.00000000000000000049' AS "
        "DECIMAL(38,20))) AS v(x)"
    ),
    "v_bc": (
        "SELECT * FROM VALUES (CAST('0.5' AS DECIMAL(38,38))), (CAST('1.25' AS DECIMAL(10,2))) AS "
        "v(x)"
    ),
    "v_three": (
        "SELECT * FROM VALUES (CAST('1.45' AS DECIMAL(10,2))), (CAST('1.5' AS DECIMAL(38,1))), "
        "(CAST('1' AS DECIMAL(38,0))) AS v(x)"
    ),
    "v_null": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0))), (NULL), (CAST('1.25' AS "
        "DECIMAL(10,2))) AS v(x)"
    ),
    "v_lit": "SELECT * FROM VALUES (1.25), (CAST('1' AS DECIMAL(38,0))) AS v(x)",
    "v_union": (
        "SELECT * FROM VALUES (CAST('1.25' AS DECIMAL(10,2))) AS v(x) UNION ALL SELECT a FROM t"
    ),
    "v_two_col": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0)), 1), (CAST('1.25' AS DECIMAL(10,2)), 2) "
        "AS v(x, n)"
    ),
    "v_control": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(10,0))), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)"
    ),
    "u_const_ab": (
        "SELECT CAST('1' AS DECIMAL(38,0)) AS x UNION ALL SELECT CAST('0.5' AS DECIMAL(38,38))"
    ),
    "u_const_ac": (
        "SELECT CAST('1' AS DECIMAL(38,0)) AS x UNION ALL SELECT CAST('1.25' AS DECIMAL(10,2))"
    ),
    "u_col_const": "SELECT a AS x FROM t UNION ALL SELECT CAST('1.25' AS DECIMAL(10,2))",
    "u_null": "SELECT a AS x FROM t UNION ALL SELECT NULL UNION ALL SELECT c FROM t",
    "u_arith_control": "SELECT a + c AS x FROM t",
    "u_struct": (
        "SELECT named_struct('f', a) AS x FROM t UNION ALL SELECT named_struct('f', c) FROM t"
    ),
    "struct_pos": (
        "SELECT named_struct('f', a, 'g', c) AS r FROM t WHERE id = 1 "
        "UNION ALL SELECT named_struct('f', c, 'g', a) FROM t WHERE id = 2"
    ),
    "struct_cmp": "SELECT id FROM t WHERE named_struct('f', a) = named_struct('f', c)",
    "map_concat_keys_id1": "SELECT map_concat(map(a, 'x'), map(c, 'y')) AS r FROM t WHERE id = 1",
    "create_map_keys_id1": "SELECT map(a, 'x', c, 'y') AS r FROM t WHERE id = 1",
    "map_concat_same_type_control": (
        "SELECT id, map_concat(map(a, 'x'), map(a + 5, 'y')) AS r FROM t WHERE id <= 5"
    ),
    "map_lit": "SELECT map('k', a, 'j', c) AS r FROM t WHERE id = 1",
    "map_vals": "SELECT map_concat(map('x', a), map('y', c)) AS r FROM t WHERE id = 1",
    "using_inner": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) l JOIN (SELECT id AS ri, c AS k "
        "FROM t) r USING (k)"
    ),
    "using_left": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) l LEFT JOIN (SELECT id AS ri, c AS "
        "k FROM t) r USING (k)"
    ),
    "using_full": (
        "SELECT k, li, ri FROM (SELECT id AS li, a AS k FROM t) l FULL OUTER JOIN (SELECT id AS "
        "ri, c AS k FROM t) r USING (k)"
    ),
    "using_full_dup_names": (
        "SELECT k FROM (SELECT a AS k FROM t) l FULL OUTER JOIN (SELECT c AS k FROM t) r USING (k)"
    ),
    "using_inner_same_type_control": (
        "SELECT k, li, ri FROM (SELECT id AS li, c AS k FROM t) l JOIN (SELECT id AS ri, c AS k "
        "FROM t) r USING (k)"
    ),
    "using_inner_h_i64": (
        "SELECT k, li, ri FROM (SELECT id AS li, i64 AS k FROM t) l JOIN (SELECT id AS ri, h AS k "
        "FROM t) r USING (k)"
    ),
    "lit_in_int_lit": f"SELECT id, {W30} IN (1) AS r FROM t",
    "lit_in_id": f"SELECT id, {W30} IN (id) AS r FROM t",
    "lit_eq_id": f"SELECT id, {W30} = id AS r FROM t",
    "w30_in_1": "SELECT id, w30 IN (1) AS r FROM t",
    "w30_in_1_where": "SELECT id FROM t WHERE w30 IN (1)",
    "w30_in_1_2": "SELECT id FROM t WHERE w30 IN (1, 2)",
    "w30_not_in_1": "SELECT id FROM t WHERE w30 NOT IN (1)",
    "w30_eq_1": "SELECT id FROM t WHERE w30 = 1",
    "w30_in_big1": "SELECT id FROM t WHERE w30 IN (CAST(1 AS BIGINT))",
    "w30_in_i32": "SELECT id FROM t WHERE w30 IN (i32)",
    "w30_in_subq_i32": "SELECT id FROM t WHERE w30 IN (SELECT i32 FROM t)",
    "w36_in_1": "SELECT id FROM t WHERE w36 IN (1)",
    "w36_eq_1": "SELECT id FROM t WHERE w36 = 1",
    "h_in_1": "SELECT id FROM t WHERE h IN (1)",
    "w30_case_when_1": "SELECT id, CASE w30 WHEN 1 THEN 'm' ELSE 'n' END AS r FROM t",
    "w30_between_1_1": "SELECT id FROM t WHERE w30 BETWEEN 1 AND 1",
    "intersect_distinct_ac": "SELECT a AS v FROM t INTERSECT DISTINCT SELECT c FROM t",
    "except_distinct_ac": "SELECT a AS v FROM t EXCEPT DISTINCT SELECT c FROM t",
    "intersect_ca": "SELECT c AS v FROM t INTERSECT SELECT a FROM t",
    "except_ca": "SELECT c AS v FROM t EXCEPT SELECT a FROM t",
    "union_distinct_ac": "SELECT a AS v FROM t UNION DISTINCT SELECT c FROM t",
    "intersect_i64h": "SELECT i64 AS v FROM t INTERSECT SELECT h FROM t",
}
LEGACY_KEY = "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate"
LEGACY = {
    "union_all_ac": "SELECT a AS v FROM t UNION ALL SELECT c FROM t",
    "eq_ac": "SELECT id FROM t WHERE a = c",
    "coalesce_ac": "SELECT id, coalesce(c, a) AS v FROM t",
    "union_all_gh": "SELECT gg AS v FROM t UNION ALL SELECT h FROM t",
    "eq_i64h": "SELECT id FROM t WHERE i64 = h",
    "coalesce_ef": "SELECT id, coalesce(f, e) AS v FROM t",
    "v_ac": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0))), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)"
    ),
    "a_eq_lit125": "SELECT id FROM t WHERE a = 1.25",
    "c_eq_a1": "SELECT id FROM t WHERE c = CAST('1' AS DECIMAL(38,0))",
    "u3_flat": "SELECT c AS v FROM t UNION ALL SELECT d FROM t UNION ALL SELECT a FROM t",
}
ICE_SRC = "SRC"
ICE_WRITES = {
    "insert_union": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} SELECT a FROM {s} UNION ALL SELECT c FROM {s}",
    ),
    "insert_union_h": (
        "CREATE TABLE {n} (x DECIMAL(38,20)) USING iceberg",
        "INSERT INTO {n} SELECT i64 FROM {s} UNION ALL SELECT h FROM {s}",
    ),
    "insert_union_gh": (
        "CREATE TABLE {n} (x DECIMAL(38,20)) USING iceberg",
        "INSERT INTO {n} SELECT gg FROM {s} UNION ALL SELECT h FROM {s}",
    ),
    "insert_union_bc": (
        "CREATE TABLE {n} (x DECIMAL(38,38)) USING iceberg",
        (
            "INSERT INTO {n} SELECT b FROM {s} WHERE id <> 3 UNION ALL SELECT c FROM {s} WHERE id "
            "IN (4, 5)"
        ),
    ),
    "insert_coalesce": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} SELECT coalesce(CASE WHEN id % 2 = 0 THEN NULL ELSE c END, a) FROM {s}",
    ),
    "insert_case": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} SELECT CASE WHEN id % 2 = 0 THEN a ELSE c END FROM {s}",
    ),
    "insert_values": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} VALUES (CAST('1' AS DECIMAL(38,0))), (CAST('1.25' AS DECIMAL(10,2)))",
    ),
    "insert_where": (
        "CREATE TABLE {n} (id INT) USING iceberg",
        "INSERT INTO {n} SELECT id FROM {s} WHERE a = c",
    ),
    "ctas_union": (
        None,
        "CREATE TABLE {n} USING iceberg AS SELECT a AS x FROM {s} UNION ALL SELECT c FROM {s}",
    ),
    "ctas_coalesce": (
        None,
        "CREATE TABLE {n} USING iceberg AS SELECT id, coalesce(c, a) AS x FROM {s}",
    ),
    "ctas_union_h": (
        None,
        "CREATE TABLE {n} USING iceberg AS SELECT i64 AS x FROM {s} UNION ALL SELECT h FROM {s}",
    ),
    "rtas_union": (
        "CREATE TABLE {n} (x INT) USING iceberg",
        (
            "CREATE OR REPLACE TABLE {n} USING iceberg AS SELECT a AS x FROM {s} UNION ALL SELECT "
            "c FROM {s}"
        ),
    ),
}
MODES = {
    "cow": "'format-version'='2', 'write.delete.mode'='copy-on-write', "
    "'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'",
    "mor": "'format-version'='2', 'write.delete.mode'='merge-on-read', "
    "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'",
}
DML_COLUMNS = (
    "id INT, a DECIMAL(38,0), c DECIMAL(10,2), i64 BIGINT, h DECIMAL(38,20), a2 DECIMAL(38,2), tag "
    "STRING"
)
ICE_DML = {
    "delete_a_c125": "DELETE FROM {m} WHERE a = CAST('1.25' AS DECIMAL(10,2))",
    "delete_c_a1": "DELETE FROM {m} WHERE c = CAST('1' AS DECIMAL(38,0))",
    "delete_a_eq_c": "DELETE FROM {m} WHERE a = c",
    "delete_a_lit": "DELETE FROM {m} WHERE a = 1.25",
    "delete_in_subq": "DELETE FROM {m} WHERE a IN (SELECT c FROM {s})",
    "update_c_a1": "UPDATE {m} SET tag = 'u' WHERE c = CAST('1' AS DECIMAL(38,0))",
    "update_c_in": (
        "UPDATE {m} SET tag = 'u' WHERE c IN (CAST('1' AS DECIMAL(38,0)), CAST('2' AS "
        "DECIMAL(38,0)))"
    ),
    "update_h_i64": "UPDATE {m} SET tag = 'u' WHERE i64 = h",
    "merge_on_a_c": (
        "MERGE INTO {m} t USING (SELECT c FROM {s} WHERE id IN (1, 2)) s ON t.a = s.c "
        "WHEN MATCHED THEN UPDATE SET tag = 'm'"
    ),
    "merge_on_c_a": (
        "MERGE INTO {m} t USING (SELECT a FROM {s} WHERE id = 1) s ON t.c = s.a "
        "WHEN MATCHED THEN UPDATE SET tag = 'm'"
    ),
    "merge_insert_union": (
        "MERGE INTO {m} t USING (SELECT 100 AS id, a AS v FROM {s} WHERE id = 1 UNION ALL "
        "SELECT 101, c FROM {s} WHERE id = 1) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT "
        "(id, a, c, i64, h, a2, tag) VALUES (s.id, NULL, NULL, NULL, NULL, s.v, 'i')"
    ),
}
PART_DML = {
    "part_delete_c_a1": ("c", "DELETE FROM {q} WHERE c = CAST('1' AS DECIMAL(38,0))"),
    "part_delete_a_c125": ("a", "DELETE FROM {q} WHERE a = CAST('1.25' AS DECIMAL(10,2))"),
}
PART_READS = {
    "part_c_a1": ("c", "SELECT id FROM {q} WHERE c = CAST('1' AS DECIMAL(38,0))"),
    "part_c_in": (
        "c",
        "SELECT id FROM {q} WHERE c IN (CAST('1' AS DECIMAL(38,0)), CAST('2' AS DECIMAL(38,0)))",
    ),
    "part_a_c125": ("a", "SELECT id FROM {q} WHERE a = CAST('1.25' AS DECIMAL(10,2))"),
    "part_a_lit": ("a", "SELECT id FROM {q} WHERE a = 1.25"),
    "part_h_big": ("h", "SELECT id FROM {q} WHERE h = CAST(1 AS BIGINT)"),
}
PART_COLUMNS = "id INT, a DECIMAL(38,0), c DECIMAL(10,2), h DECIMAL(38,20)"


def setup_exprs() -> list[str]:
    items = []
    for name, sql_type, values in COLUMNS:
        whens = " ".join(
            f"WHEN {i} THEN {'NULL' if values[i] is None else repr(values[i])}" for i in range(8)
        )
        items.append(f"CAST(CASE id {whens} END AS {sql_type}) AS {name}")
    return items


def sql_cells() -> dict[str, str]:
    cells: dict[str, str] = {}
    for key, (x, y) in PAIRS.items():
        positions = list(EXPRS) + list(PLANS) if key in FULL_PAIRS else CORE
        tables = ("t", "tx") if key in TX_PAIRS else ("t",)
        for table in tables:
            for pos in positions:
                if table == "tx" and (pos not in CORE or pos == "exists_corr"):
                    continue
                if key in CONTROL_PAIRS and pos in OTHER_CAUSES:
                    continue
                template = PLANS.get(pos) or "SELECT id, " + EXPRS[pos] + " AS r FROM {t}"
                cells[f"pair/{key}/{table}/{pos}"] = template.format(x=x, y=y, t=table)
    for key, predicate in CONSTS.items():
        cells[f"const/{key}/select"] = f"SELECT id, {predicate} AS r FROM t"
        cells[f"const/{key}/where"] = f"SELECT id FROM t WHERE {predicate}"
    for key, sql in SHAPES.items():
        cells[f"shape/{key}"] = sql
    return cells


def frame_cells(sf: Any) -> dict[str, Callable[[Any], Any]]:
    cells: dict[str, Callable[[Any], Any]] = {}
    for key in ("ac", "ab", "gh", "i64h", "i32w30", "c0c"):
        x, y = PAIRS[key]
        cx, cy = sf.col(x), sf.col(y)
        cells.update(
            {
                f"df/{key}/union": lambda t, x=x, y=y: t.select(sf.col(x).alias("v")).union(
                    t.select(sf.col(y).alias("v"))
                ),
                f"df/{key}/unionByName": lambda t, x=x, y=y: t.select(
                    sf.col(y).alias("v")
                ).unionByName(t.select(sf.col(x).alias("v"))),
                f"df/{key}/when": lambda t, cx=cx, cy=cy: t.select(
                    "id", sf.when(sf.col("id") % 2 == 0, cx).otherwise(cy).alias("r")
                ),
                f"df/{key}/isin": lambda t, cx=cx, cy=cy: t.select("id", cy.isin(cx).alias("r")),
                f"df/{key}/between": lambda t, cx=cx, cy=cy: t.select(
                    "id", cx.between(cy, cy).alias("r")
                ),
                f"df/{key}/filter_eq": lambda t, cx=cx, cy=cy: t.filter(cx == cy).select("id"),
                f"df/{key}/coalesce": lambda t, cx=cx, cy=cy: t.select(
                    "id", sf.coalesce(cy, cx).alias("r")
                ),
                f"df/{key}/greatest": lambda t, cx=cx, cy=cy: t.select(
                    "id", sf.greatest(cx, cy).alias("r")
                ),
                f"df/{key}/join": lambda t, x=x, y=y: (
                    t.select(sf.col("id").alias("li"), sf.col(x).alias("lx"))
                    .join(
                        t.select(sf.col("id").alias("ri"), sf.col(y).alias("ry")),
                        sf.col("lx") == sf.col("ry"),
                    )
                    .select("li", "ri")
                ),
                f"fexpr/{key}/eq": lambda t, x=x, y=y: t.select(
                    "id", sf.expr(f"{x} = {y}").alias("r")
                ),
                f"fexpr/{key}/coalesce": lambda t, x=x, y=y: t.select(
                    "id", sf.expr(f"coalesce({y}, {x})").alias("r")
                ),
                f"fexpr/{key}/filter": lambda t, x=x, y=y: t.filter(sf.expr(f"{x} = {y}")).select(
                    "id"
                ),
            }
        )
    lit_a1 = sf.lit("1").cast("decimal(38,0)")
    lit_c125 = sf.lit("1.25").cast("decimal(10,2)")
    cells.update(
        {
            "dfconst/c_eq_a1": lambda t: t.filter(sf.col("c") == lit_a1).select("id"),
            "dfconst/a1_eq_c": lambda t: t.filter(lit_a1 == sf.col("c")).select("id"),
            "dfconst/a_eq_c125": lambda t: t.filter(sf.col("a") == lit_c125).select("id"),
            "dfconst/c_isin_a1": lambda t: t.filter(sf.col("c").isin(lit_a1)).select("id"),
            "dfconst/a_eq_pydec": lambda t: t.filter(sf.col("a") == Decimal("1.25")).select("id"),
            "dfconst/a_isin_pydec": lambda t: t.filter(
                sf.col("a").isin(Decimal("1.25"), Decimal("2.5"))
            ).select("id"),
            "dfconst/c_between_a1": lambda t: t.filter(sf.col("c").between(lit_a1, lit_a1)).select(
                "id"
            ),
            "dfconst/fexpr_c_eq_a1": lambda t: t.filter(
                sf.expr("c = CAST('1' AS DECIMAL(38,0))")
            ).select("id"),
            "dfconst/w30_isin_1": lambda t: t.filter(sf.col("w30").isin(1)).select("id"),
            "dfshape/union_chain_c_d_a": lambda t: (
                t.select(sf.col("c").alias("v"))
                .union(t.select(sf.col("d").alias("v")))
                .union(t.select(sf.col("a").alias("v")))
            ),
            "dfshape/union_chain_a_c_d": lambda t: (
                t.select(sf.col("a").alias("v"))
                .union(t.select(sf.col("c").alias("v")))
                .union(t.select(sf.col("d").alias("v")))
            ),
            "dfshape/by_name_missing": lambda t: t.select(sf.col("c").alias("v")).unionByName(
                t.select(sf.col("a").alias("v")).withColumn("extra", sf.lit(1)),
                allowMissingColumns=True,
            ),
            "dfshape/join_on_name": lambda t: (
                t.select(sf.col("id").alias("li"), sf.col("a").alias("k"))
                .join(t.select(sf.col("id").alias("ri"), sf.col("c").alias("k")), "k")
                .select("k", "li", "ri")
            ),
            "dfshape/intersect": lambda t: t.select(sf.col("a").alias("v")).intersect(
                t.select(sf.col("c").alias("v"))
            ),
            "dfshape/subtract": lambda t: t.select(sf.col("a").alias("v")).subtract(
                t.select(sf.col("c").alias("v"))
            ),
        }
    )
    return cells


def norm(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        return "{" + ",".join(f"{norm(k)}->{norm(v)}" for k, v in items) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(norm(item) for item in value) + "]"
    return str(value)


def typed_norm(value: object, data_type: Any) -> str:
    fields = getattr(data_type, "fields", None)
    if value is not None and fields is not None and data_type.simpleString().startswith("struct<"):
        items = list(value.values()) if isinstance(value, dict) else list(value)
        return (
            "["
            + ",".join(typed_norm(v, f.dataType) for v, f in zip(items, fields, strict=False))
            + "]"
        )
    return norm(value)


def answer_of(frame: Any) -> dict[str, list[str]]:
    rows = frame.collect()
    fields = frame.schema.fields
    types = [field.dataType.simpleString() for field in fields]
    rendered = (
        "|".join(typed_norm(v, f.dataType) for v, f in zip(row, fields, strict=True))
        for row in rows
    )
    return {"rows": sorted(rendered), "types": types}


def trim_decimal(match: re.Match[str]) -> str:
    text = match.group(0).rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def trimmed(rows: list[str]) -> list[str]:
    return sorted(DECIMAL_TEXT.sub(trim_decimal, row) for row in rows)


def same_answer(found: Any, want: Any) -> bool:
    if found == want:
        return True
    if not (isinstance(found, dict) and isinstance(want, dict)):
        return False
    if found.get("types") != want.get("types") or "rows" not in found or "rows" not in want:
        return False
    return trimmed(found["rows"]) == trimmed(want["rows"])


def error_class(error: Exception) -> str:
    for attr in ("getCondition", "getErrorClass"):
        reader = getattr(error, attr, None)
        if reader is None:
            continue
        try:
            found = reader()
        except Exception:
            found = None
        if found:
            return str(found)
    return type(error).__name__


def attempt(run: Callable[[], Any]) -> Any:
    try:
        return run()
    except Exception as error:
        return "ERR:" + error_class(error) + ":" + " ".join(str(error).split())[:200]


def make_tables(spark: Any, root: str) -> None:
    base = spark.range(8).selectExpr(*setup_exprs())
    for name, condition in (("t", "id <= 6"), ("tx", "id >= 7")):
        path = f"{root}/pq_{name}"
        base.filter(condition).write.parquet(path)
        spark.read.parquet(path).createOrReplaceTempView(name)


def run_sql(spark: Any, sql: str) -> Any:
    return attempt(lambda: answer_of(spark.sql(sql)))


def make_source(spark: Any, catalog: str) -> str:
    source = f"{catalog}.ns.src"
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type, _ in COLUMNS)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {source} ({columns}) USING iceberg").collect()
    if not spark.table(source).limit(1).collect():
        spark.sql(f"INSERT INTO {source} SELECT * FROM t").collect()
    return source


def statement_and_rows(spark: Any, sql: str, read: str) -> dict[str, Any]:
    done = attempt(lambda: spark.sql(sql).collect())
    return {
        "statement": done if isinstance(done, str) else "ok",
        "rows": attempt(lambda: answer_of(spark.sql(read))),
    }


def run_write(spark: Any, catalog: str, key: str) -> dict[str, Any]:
    source = make_source(spark, catalog)
    create, statement = ICE_WRITES[key]
    name = f"{catalog}.ns.w_{key}"
    if create:
        spark.sql(create.format(n=name)).collect()
    return statement_and_rows(spark, statement.format(n=name, s=source), f"SELECT * FROM {name}")


def run_dml(spark: Any, catalog: str, mode: str, key: str) -> dict[str, Any]:
    source = make_source(spark, catalog)
    name = f"{catalog}.ns.m_{mode}_{key}"
    spark.sql(
        f"CREATE TABLE {name} ({DML_COLUMNS}) USING iceberg TBLPROPERTIES ({MODES[mode]})"
    ).collect()
    spark.sql(
        f"INSERT INTO {name} SELECT id, a, c, i64, h, CAST(NULL AS DECIMAL(38,2)), 'o' FROM t"
    ).collect()
    statement = ICE_DML[key].format(m=name, s=source)
    return statement_and_rows(spark, statement, f"SELECT id, a2, tag FROM {name}")


def run_part_dml(spark: Any, catalog: str, mode: str, key: str) -> dict[str, Any]:
    column, statement = PART_DML[key]
    name = f"{catalog}.ns.pd_{mode}_{key}"
    spark.sql(
        f"CREATE TABLE {name} ({PART_COLUMNS}) USING iceberg PARTITIONED BY ({column}) "
        f"TBLPROPERTIES ({MODES[mode]})"
    ).collect()
    spark.sql(f"INSERT INTO {name} SELECT id, a, c, h FROM t").collect()
    return statement_and_rows(spark, statement.format(q=name), f"SELECT id FROM {name}")


def run_part_read(spark: Any, catalog: str, key: str) -> Any:
    column, statement = PART_READS[key]
    name = f"{catalog}.ns.p_{key}"
    spark.sql(
        f"CREATE TABLE {name} ({PART_COLUMNS}) USING iceberg PARTITIONED BY ({column})"
    ).collect()
    spark.sql(f"INSERT INTO {name} SELECT id, a, c, h FROM t").collect()
    return run_sql(spark, statement.format(q=name))


def run_legacy(spark: Any) -> dict[str, Any]:
    spark.sql(f"SET {LEGACY_KEY}=true").collect()
    try:
        found = {key: run_sql(spark, sql) for key, sql in LEGACY.items()}
    finally:
        spark.sql(f"RESET {LEGACY_KEY}").collect()
    found["after_reset/eq_ac"] = run_sql(spark, LEGACY["eq_ac"])
    found["after_reset/union_all_ac"] = run_sql(spark, LEGACY["union_all_ac"])
    return found


def measure_all(spark: Any, sf: Any, catalog: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, sql in sql_cells().items():
        found[key] = run_sql(spark, sql)
    for key, build in frame_cells(sf).items():
        found[key] = attempt(lambda build=build: answer_of(build(spark.table("t"))))
    for key in ICE_WRITES:
        found[f"ice/{key}"] = attempt(lambda key=key: run_write(spark, catalog, key))
    for mode in MODES:
        for key in ICE_DML:
            found[f"icedml/{mode}/{key}"] = attempt(
                lambda key=key, mode=mode: run_dml(spark, catalog, mode, key)
            )
        for key in PART_DML:
            found[f"icepart/{mode}/{key}"] = attempt(
                lambda key=key, mode=mode: run_part_dml(spark, catalog, mode, key)
            )
    for key in PART_READS:
        found[f"icepart/read/{key}"] = attempt(lambda key=key: run_part_read(spark, catalog, key))
    for key, value in run_legacy(spark).items():
        found[f"legacy/{key}"] = value
    return found
