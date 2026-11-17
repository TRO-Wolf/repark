COLUMNS = [
    ("id", "INT", ["1", "2", "3", "4", "5", "6", "7", "8"]),
    ("g", "INT", ["1", "0", "1", "0", "1", "0", "1", "0"]),
    (
        "a",
        "DECIMAL(38,0)",
        ["1", "2", "-2", "1", "0", None, "9" * 38, "-" + "9" * 38],
    ),
    (
        "c",
        "DECIMAL(10,2)",
        ["1.25", "1.50", "-1.50", "0.50", "0.49", "1.45", "99999999.99", "-99999999.99"],
    ),
    (
        "d",
        "DECIMAL(38,1)",
        ["1.5", "2.5", "-2.5", "0.5", "-0.5", "1.4", "9" * 37 + ".9", "-0.5"],
    ),
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

TYPES = {name: sql_type for name, sql_type, _ in COLUMNS}

PAIRS = [
    ("ab", "a", "b", "past38", "decimal(38,0)"),
    ("ac", "a", "c", "past38", "decimal(38,0)"),
    ("ad", "a", "d", "past38", "decimal(38,0)"),
    ("bc", "b", "c", "past38", "decimal(38,30)"),
    ("ef", "e", "f", "past38", "decimal(38,18)"),
    ("gh", "gg", "h", "past38", "decimal(38,10)"),
    ("i64h", "i64", "h", "past38-int", "decimal(38,18)"),
    ("i32w30", "i32", "w30", "past38-int", "decimal(38,28)"),
    ("i16w34", "i16", "w34", "past38-int", "decimal(38,33)"),
    ("i8w36", "i8", "w36", "past38-int", "decimal(38,35)"),
    ("c0c", "c0", "c", "control", "decimal(12,2)"),
    ("i32c", "i32", "c", "control-int", "decimal(12,2)"),
    ("i64c", "i64", "c", "control-int", "decimal(22,2)"),
    ("k28k4", "k28", "k4", "control", "decimal(30,4)"),
]

FULL_PAIRS = {"ac", "ab", "i64h", "gh", "i32w30", "c0c"}

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
    "nvl": "nvl({y}, {x})",
    "nvl2": "nvl2(id, {x}, {y})",
    "nullif": "nullif({x}, {y})",
    "greatest": "greatest({x}, {y})",
    "least": "least({x}, {y})",
    "array": "array({x}, {y})",
    "map": "map(1, {x}, 2, {y})",
    "array_contains": "array_contains(array({x}), {y})",
    "array_union": "array_union(array({x}), array({y}))",
    "map_concat": "map_concat(map(1, {x}), map(2, {y}))",
    "lag": "lag({x}, 1, {y}) OVER (ORDER BY id)",
    "lead": "lead({x}, 1, {y}) OVER (ORDER BY id)",
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
    "join_full_using": (
        "SELECT k FROM (SELECT {x} AS k FROM {t}) FULL OUTER JOIN (SELECT {y} AS k FROM {t}) "
        "USING (k)"
    ),
    "in_subquery": "SELECT id FROM {t} WHERE {x} IN (SELECT {y} FROM {t})",
    "not_in_subquery": (
        "SELECT id FROM {t} WHERE {x} NOT IN (SELECT {y} FROM {t} WHERE {y} IS NOT NULL)"
    ),
    "scalar_subquery": "SELECT id FROM {t} WHERE {x} = (SELECT max({y}) FROM {t} WHERE id = 1)",
    "exists_corr": "SELECT id FROM {t} o WHERE EXISTS (SELECT 1 FROM {t} i WHERE i.{y} = o.{x})",
}

T_ONLY = {"exists_corr"}

CONSTS = [
    ("a_eq_c125", "a = CAST('1.25' AS DECIMAL(10,2))"),
    ("c125_eq_a", "CAST('1.25' AS DECIMAL(10,2)) = a"),
    ("c_eq_a1", "c = CAST('1' AS DECIMAL(38,0))"),
    ("a1_eq_c", "CAST('1' AS DECIMAL(38,0)) = c"),
    ("c_lt_a1", "c < CAST('1' AS DECIMAL(38,0))"),
    ("c_ge_a1", "c >= CAST('1' AS DECIMAL(38,0))"),
    ("c_nseq_a1", "c <=> CAST('1' AS DECIMAL(38,0))"),
    ("c_in_a12", "c IN (CAST('1' AS DECIMAL(38,0)), CAST('2' AS DECIMAL(38,0)))"),
    ("c_notin_a12", "c NOT IN (CAST('1' AS DECIMAL(38,0)), CAST('2' AS DECIMAL(38,0)))"),
    ("c_between_a1", "c BETWEEN CAST('1' AS DECIMAL(38,0)) AND CAST('1' AS DECIMAL(38,0))"),
    ("a1_between_c", "CAST('1' AS DECIMAL(38,0)) BETWEEN c AND c"),
    ("a_in_c", "a IN (CAST('1.25' AS DECIMAL(10,2)), CAST('2.5' AS DECIMAL(10,2)))"),
    ("b_eq_a1", "b = CAST('1' AS DECIMAL(38,0))"),
    ("a_eq_b05", "a = CAST('0.5' AS DECIMAL(38,38))"),
    ("a_eq_lit125", "a = 1.25"),
    ("lit125_eq_a", "1.25 = a"),
    ("a_eq_lit15", "a = 1.5"),
    ("a_gt_lit05", "a > 0.5"),
    ("a_in_lit", "a IN (1.25, 2.5)"),
    ("d_eq_lit145", "d = 1.45"),
    ("h_eq_int1", "h = 1"),
    ("h_eq_big1", "h = CAST(1 AS BIGINT)"),
    ("i64_eq_hlit", "i64 = CAST('1.00000000000000000049' AS DECIMAL(38,20))"),
    ("w30_eq_int1", "w30 = 1"),
    ("i32_eq_w30lit", "i32 = CAST('1.000000000000000000000000000049' AS DECIMAL(38,30))"),
    ("c_eq_big1", "c = CAST(1 AS BIGINT)"),
    ("gg_eq_hlit", "gg = CAST('1.00000000005' AS DECIMAL(38,20))"),
    ("h_eq_glit", "h = CAST('1.0000000001' AS DECIMAL(38,10))"),
]

THREE = {
    "u3_flat": "SELECT c AS v FROM {t} UNION ALL SELECT d FROM {t} UNION ALL SELECT a FROM {t}",
    "u3_left": "(SELECT c AS v FROM {t} UNION ALL SELECT d FROM {t}) UNION ALL SELECT a FROM {t}",
    "u3_right": "SELECT a AS v FROM {t} UNION ALL (SELECT c FROM {t} UNION ALL SELECT d FROM {t})",
    "u3_sub": (
        "SELECT v FROM (SELECT c AS v FROM {t} UNION ALL SELECT d FROM {t}) s UNION ALL "
        "SELECT a FROM {t}"
    ),
    "coalesce3": "SELECT id, coalesce(c, d, a) AS v FROM {t}",
    "coalesce_nested": "SELECT id, coalesce(coalesce(c, d), a) AS v FROM {t}",
    "greatest3": "SELECT id, greatest(c, d, a) AS v FROM {t}",
    "least3": "SELECT id, least(c, d, a) AS v FROM {t}",
    "in3": "SELECT id, c IN (d, a) AS v FROM {t}",
    "case3": "SELECT id, CASE id % 3 WHEN 0 THEN c WHEN 1 THEN d ELSE a END AS v FROM {t}",
    "array3": "SELECT id, array(c, d, a) AS v FROM {t}",
    "u3_int": "SELECT i64 AS v FROM {t} UNION ALL SELECT h FROM {t} UNION ALL SELECT c FROM {t}",
}

AGGS = {
    "having_sum_const": (
        "SELECT g, sum(k28) AS s FROM {t} GROUP BY g HAVING sum(k28) = CAST('2.505' AS "
        "DECIMAL(10,4))"
    ),
    "having_sum_max": "SELECT g, sum(k28) AS s FROM {t} GROUP BY g HAVING sum(k28) = max(k4)",
    "having_avg_max": "SELECT g, avg(k28) AS s FROM {t} GROUP BY g HAVING avg(k28) = max(a)",
    "sum_eq_select": "SELECT g, sum(k28) = max(k4) AS s FROM {t} GROUP BY g",
    "sum_union": "SELECT sum(k28) AS v FROM {t} GROUP BY g UNION ALL SELECT k4 FROM {t}",
    "sum_coalesce": "SELECT g, coalesce(sum(k28), max(k4)) AS s FROM {t} GROUP BY g",
    "sum_type": "SELECT g, sum(k28) AS s, avg(k28) AS m FROM {t} GROUP BY g",
}

VALUES = {
    "v_ac": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0))), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)"
    ),
    "v_ca": (
        "SELECT * FROM VALUES (CAST('1.25' AS DECIMAL(10,2))), (CAST('1' AS DECIMAL(38,0))) AS v(x)"
    ),
    "v_int_c": "SELECT * FROM VALUES (1), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)",
    "v_big_h": (
        "SELECT * FROM VALUES (CAST(1 AS BIGINT)), (CAST('1.00000000000000000049' AS "
        "DECIMAL(38,20))) AS v(x)"
    ),
    "v_bc": (
        "SELECT * FROM VALUES (CAST('0.5' AS DECIMAL(38,38))), (CAST('1.25' AS "
        "DECIMAL(10,2))) AS v(x)"
    ),
    "v_three": (
        "SELECT * FROM VALUES (CAST('1.45' AS DECIMAL(10,2))), (CAST('1.5' AS "
        "DECIMAL(38,1))), (CAST('1' AS DECIMAL(38,0))) AS v(x)"
    ),
    "v_null": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0))), (NULL), (CAST('1.25' AS "
        "DECIMAL(10,2))) AS v(x)"
    ),
    "v_lit": "SELECT * FROM VALUES (1.25), (CAST('1' AS DECIMAL(38,0))) AS v(x)",
    "v_union": (
        "SELECT * FROM VALUES (CAST('1.25' AS DECIMAL(10,2))) AS v(x) UNION ALL SELECT a FROM {t}"
    ),
    "v_two_col": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(38,0)), 1), (CAST('1.25' AS "
        "DECIMAL(10,2)), 2) AS v(x, n)"
    ),
    "v_control": (
        "SELECT * FROM VALUES (CAST('1' AS DECIMAL(10,0))), (CAST('1.25' AS DECIMAL(10,2))) AS v(x)"
    ),
}

LITERALS = {
    "u_const_ab": (
        "SELECT CAST('1' AS DECIMAL(38,0)) AS x UNION ALL SELECT CAST('0.5' AS DECIMAL(38,38))"
    ),
    "u_const_ac": (
        "SELECT CAST('1' AS DECIMAL(38,0)) AS x UNION ALL SELECT CAST('1.25' AS DECIMAL(10,2))"
    ),
    "u_col_const": "SELECT a AS x FROM {t} UNION ALL SELECT CAST('1.25' AS DECIMAL(10,2))",
    "u_overflow_lit": "SELECT CAST('1.5' AS DECIMAL(38,38)) AS x UNION ALL SELECT a FROM {t}",
    "where_overflow_lit": "SELECT id FROM {t} WHERE a = CAST('1.5' AS DECIMAL(38,38))",
    "u_overflow39": (
        "SELECT CAST('123456789012345678901234567890123456789' AS DECIMAL(38,0)) AS x UNION "
        "ALL SELECT c FROM {t}"
    ),
    "coalesce_overflow_lit": "SELECT id, coalesce(c, CAST('1.5' AS DECIMAL(38,38))) AS x FROM {t}",
    "u_null": "SELECT a AS x FROM {t} UNION ALL SELECT NULL UNION ALL SELECT c FROM {t}",
    "u_arith": "SELECT a + c AS x FROM {t}",
    "u_struct": (
        "SELECT named_struct('f', a) AS x FROM {t} UNION ALL SELECT named_struct('f', c) FROM {t}"
    ),
}

SPECIAL = {
    "w_range_a_dec": (
        "SELECT id, count(*) OVER (ORDER BY a RANGE BETWEEN CAST('0.5' AS DECIMAL(10,2)) "
        "PRECEDING AND CURRENT ROW) AS n FROM {t}"
    ),
    "w_range_a_lit": (
        "SELECT id, count(*) OVER (ORDER BY a RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) "
        "AS n FROM {t}"
    ),
    "w_range_c_dec": (
        "SELECT id, count(*) OVER (ORDER BY c RANGE BETWEEN CAST('1' AS DECIMAL(38,0)) "
        "PRECEDING AND CURRENT ROW) AS n FROM {t}"
    ),
    "w_range_c_lit": (
        "SELECT id, count(*) OVER (ORDER BY c RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) "
        "AS n FROM {t}"
    ),
    "w_range_h_dec": (
        "SELECT id, count(*) OVER (ORDER BY h RANGE BETWEEN CAST('1' AS DECIMAL(38,0)) "
        "PRECEDING AND CURRENT ROW) AS n FROM {t}"
    ),
    "pivot_a": (
        "SELECT * FROM (SELECT g, a FROM {t}) PIVOT (count(*) FOR a IN (CAST('1.25' AS "
        "DECIMAL(10,2)) AS k1, CAST('2' AS DECIMAL(10,2)) AS k2))"
    ),
    "pivot_c": (
        "SELECT * FROM (SELECT g, c FROM {t}) PIVOT (count(*) FOR c IN (CAST('1' AS "
        "DECIMAL(38,0)) AS k1, CAST('2' AS DECIMAL(38,0)) AS k2))"
    ),
    "gsets_ac": "SELECT a, c, count(*) AS n FROM {t} GROUP BY GROUPING SETS ((a), (c))",
    "gsets_coalesce": (
        "SELECT coalesce(a, c) AS k, count(*) AS n FROM {t} GROUP BY GROUPING SETS "
        "((coalesce(a, c)), ())"
    ),
    "rollup_ac": (
        "SELECT coalesce(c, a) AS k, count(*) AS n FROM {t} GROUP BY ROLLUP (coalesce(c, a))"
    ),
}

ICE_SRC = "lc.ns.src"

ICE_READS = {
    "ice_insert_union": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} SELECT a FROM {s} UNION ALL SELECT c FROM {s}",
    ),
    "ice_insert_union_h": (
        "CREATE TABLE {n} (x DECIMAL(38,20)) USING iceberg",
        "INSERT INTO {n} SELECT i64 FROM {s} UNION ALL SELECT h FROM {s}",
    ),
    "ice_insert_union_gh": (
        "CREATE TABLE {n} (x DECIMAL(38,20)) USING iceberg",
        "INSERT INTO {n} SELECT gg FROM {s} UNION ALL SELECT h FROM {s}",
    ),
    "ice_insert_union_bc": (
        "CREATE TABLE {n} (x DECIMAL(38,38)) USING iceberg",
        (
            "INSERT INTO {n} SELECT b FROM {s} WHERE id <> 3 UNION ALL SELECT c FROM {s} "
            "WHERE id IN (4, 5)"
        ),
    ),
    "ice_insert_coalesce": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} SELECT coalesce(CASE WHEN id % 2 = 0 THEN NULL ELSE c END, a) FROM {s}",
    ),
    "ice_insert_case": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} SELECT CASE WHEN id % 2 = 0 THEN a ELSE c END FROM {s}",
    ),
    "ice_insert_values": (
        "CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg",
        "INSERT INTO {n} VALUES (CAST('1' AS DECIMAL(38,0))), (CAST('1.25' AS DECIMAL(10,2)))",
    ),
    "ice_insert_where": (
        "CREATE TABLE {n} (id INT) USING iceberg",
        "INSERT INTO {n} SELECT id FROM {s} WHERE a = c",
    ),
    "ice_ctas_union": (
        None,
        "CREATE TABLE {n} USING iceberg AS SELECT a AS x FROM {s} UNION ALL SELECT c FROM {s}",
    ),
    "ice_ctas_coalesce": (
        None,
        "CREATE TABLE {n} USING iceberg AS SELECT id, coalesce(c, a) AS x FROM {s}",
    ),
    "ice_ctas_union_h": (
        None,
        "CREATE TABLE {n} USING iceberg AS SELECT i64 AS x FROM {s} UNION ALL SELECT h FROM {s}",
    ),
    "ice_rtas_union": (
        "CREATE TABLE {n} (x INT) USING iceberg",
        (
            "CREATE OR REPLACE TABLE {n} USING iceberg AS SELECT a AS x FROM {s} UNION ALL "
            "SELECT c FROM {s}"
        ),
    ),
}

MODES = {
    "cow": "'format-version'='2', 'write.delete.mode'='copy-on-write', "
    "'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'",
    "mor": "'format-version'='2', 'write.delete.mode'='merge-on-read', "
    "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'",
}

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
    "update_set_coalesce": (
        "UPDATE {m} SET a2 = coalesce(CASE WHEN id % 2 = 0 THEN NULL ELSE c END, a)"
    ),
    "merge_on_a_c": (
        "MERGE INTO {m} t USING (SELECT c FROM {s} WHERE id IN (1, 2)) s ON t.a = s.c WHEN "
        "MATCHED THEN UPDATE SET tag = 'm'"
    ),
    "merge_on_c_a": (
        "MERGE INTO {m} t USING (SELECT a FROM {s} WHERE id = 1) s ON t.c = s.a WHEN MATCHED "
        "THEN UPDATE SET tag = 'm'"
    ),
    "merge_insert_union": (
        "MERGE INTO {m} t USING (SELECT 100 AS id, a AS v FROM {s} WHERE id = 1 UNION ALL "
        "SELECT 101, c FROM {s} WHERE id = 1) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT "
        "(id, a, c, i64, h, a2, tag) VALUES (s.id, NULL, NULL, NULL, NULL, s.v, 'i')"
    ),
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

PART_DML = {
    "part_delete_c_a1": ("c", "DELETE FROM {q} WHERE c = CAST('1' AS DECIMAL(38,0))"),
    "part_delete_a_c125": ("a", "DELETE FROM {q} WHERE a = CAST('1.25' AS DECIMAL(10,2))"),
}

LEGACY_POS = ["union_all", "eq", "coalesce"]
LEGACY_EXTRA = ["v_ac", "v_bc", "u_const_ab", "a_eq_lit125", "c_eq_a1", "having_sum_max", "u3_flat"]

LEGACY_KEY = "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate"


def setup_exprs() -> list:
    rows = range(8)
    items = []
    for name, sql_type, values in COLUMNS:
        whens = " ".join(
            f"WHEN {i} THEN {'NULL' if values[i] is None else repr(values[i])}" for i in rows
        )
        items.append(f"CAST(CASE id {whens} END AS {sql_type}) AS {name}")
    return items


def pair(key: str) -> tuple:
    for p in PAIRS:
        if p[0] == key:
            return p
    raise KeyError(key)


def pair_cells() -> list:
    out = []
    for key, x, y, family, _ in PAIRS:
        positions = list(EXPRS) + list(PLANS) if key in FULL_PAIRS else CORE
        for pos in positions:
            tmpl = "SELECT id, " + EXPRS[pos] + " AS r FROM {t}" if pos in EXPRS else PLANS[pos]
            out.append((key, family, pos, tmpl.replace("{x}", x).replace("{y}", y)))
    return out
