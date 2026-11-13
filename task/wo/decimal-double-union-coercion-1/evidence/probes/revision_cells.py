U_TINY = "SELECT 1 AS id, 0.1 AS v UNION ALL SELECT 2, CAST(1e-20 AS DOUBLE)"
V_TINY = "VALUES (1, 0.1), (2, CAST(1e-20 AS DOUBLE))"
THREE = "id INT, v DOUBLE, d DECIMAL(10,2)"
THREE_SEED = (
    "INSERT INTO {t} VALUES (1, CAST(1e-20 AS DOUBLE), CAST(0 AS DECIMAL(10,2))), "
    "(2, CAST(0.5 AS DOUBLE), CAST(0.5 AS DECIMAL(10,2)))"
)
ZERO_SEED = "INSERT INTO {t} VALUES (1, CAST('-0.0' AS DOUBLE), CAST(0 AS DECIMAL(10,2)))"
MOR = (
    " TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')"
)
K0 = "(SELECT CAST(0 AS DECIMAL(10,2)) AS k)"
WRITES = [
    ("ow_values", "id INT, v DOUBLE", "seed", "INSERT OVERWRITE {t} " + V_TINY),
    ("ow_union", "id INT, v DOUBLE", "seed", "INSERT OVERWRITE {t} " + U_TINY),
    (
        "ow_select_values",
        "id INT, v DOUBLE",
        "seed",
        "INSERT OVERWRITE {t} SELECT * FROM " + V_TINY + " AS s(id, v)",
    ),
    (
        "byname_values",
        "id INT, v DOUBLE",
        None,
        "INSERT INTO {t} BY NAME SELECT * FROM " + V_TINY + " AS s(id, v)",
    ),
    ("byname_union", "id INT, v DOUBLE", None, "INSERT INTO {t} BY NAME " + U_TINY),
    (
        "values_string",
        "id INT, v STRING",
        None,
        "INSERT INTO {t} VALUES (1, 1.50), (2, CAST(2 AS DOUBLE))",
    ),
    (
        "select_values_string",
        "id INT, v STRING",
        None,
        "INSERT INTO {t} SELECT id, v FROM VALUES (1, 1.50), (2, CAST(2 AS DOUBLE)) AS s(id, v)",
    ),
    (
        "values_dec38",
        "id INT, v DECIMAL(38,18)",
        None,
        "INSERT INTO {t} VALUES (1, 123456789.123456789), (2, CAST(1 AS DOUBLE))",
    ),
    ("values_float", "id INT, v FLOAT", None, "INSERT INTO {t} " + V_TINY),
    (
        "values_bigint",
        "id INT, v BIGINT",
        None,
        "INSERT INTO {t} VALUES (1, 1.5), (2, CAST(2.5 AS DOUBLE))",
    ),
    (
        "rtas_values",
        "rtas",
        None,
        "CREATE OR REPLACE TABLE {t} USING iceberg AS SELECT * FROM " + V_TINY + " AS s(id, v)",
    ),
    ("rtas_union", "rtas", None, "CREATE OR REPLACE TABLE {t} USING iceberg AS " + U_TINY),
    ("delete_eq_cols", THREE, "three", "DELETE FROM {t} WHERE v = d"),
    ("update_where_cols", THREE, "three", "UPDATE {t} SET id = id + 10 WHERE v = d"),
    (
        "merge_on_cols",
        THREE,
        "three",
        "MERGE INTO {t} t USING " + K0 + " s ON t.v = s.k WHEN MATCHED THEN UPDATE SET id = 99",
    ),
    ("delete_eq_cols_mor", THREE + "|mor", "three", "DELETE FROM {t} WHERE v = d"),
    ("update_where_cols_mor", THREE + "|mor", "three", "UPDATE {t} SET id = id + 10 WHERE v = d"),
    (
        "merge_on_cols_mor",
        THREE + "|mor",
        "three",
        "MERGE INTO {t} t USING " + K0 + " s ON t.v = s.k WHEN MATCHED THEN UPDATE SET id = 99",
    ),
    (
        "merge_on_zero_today",
        THREE,
        "zero",
        "MERGE INTO {t} t USING " + K0 + " s ON t.v = s.k WHEN MATCHED THEN UPDATE SET id = 99",
    ),
    (
        "merge_on_zero_widened",
        THREE,
        "zero",
        "MERGE INTO {t} t USING "
        + K0
        + " s ON t.v = CAST(s.k AS DOUBLE) WHEN MATCHED THEN UPDATE SET id = 99",
    ),
    ("delete_zero_today", THREE, "zero", "DELETE FROM {t} WHERE v = d"),
    ("delete_zero_widened", THREE, "zero", "DELETE FROM {t} WHERE v = CAST(d AS DOUBLE)"),
    ("df_insertinto_union", "id INT, v DOUBLE", None, "DF-INSERTINTO"),
    ("df_overwritepartitions_union", "id INT, v DOUBLE", "seed", "DF-OVERWRITEPARTITIONS"),
]
ZSRC = "(SELECT CAST(0 AS DECIMAL(10,2)) AS d, CAST('-0.0' AS DOUBLE) AS v)"
ZD = "CAST(0 AS DECIMAL(10,2))"
ZW = "CAST(CAST(0 AS DECIMAL(10,2)) AS DOUBLE)"
NZ = "CAST('-0.0' AS DOUBLE)"
SELECTS = [
    (
        "struct_union",
        "SELECT named_struct('a', 0.1) AS c UNION ALL SELECT named_struct('a', "
        "CAST(1e-20 AS DOUBLE))",
    ),
    ("array_union", "SELECT array(0.1) AS c UNION ALL SELECT array(CAST(1e-20 AS DOUBLE))"),
    ("map_union", "SELECT map('k', 0.1) AS c UNION ALL SELECT map('k', CAST(1e-20 AS DOUBLE))"),
    (
        "struct_case",
        "SELECT CASE WHEN id = 2 THEN named_struct('a', 0.1) ELSE named_struct('a', "
        "CAST(1e-20 AS DOUBLE)) END AS c "
        "FROM (SELECT 1 AS id)",
    ),
    (
        "array_case",
        "SELECT CASE WHEN id = 2 THEN array(0.1) ELSE array(CAST(1e-20 AS DOUBLE)) END "
        "AS c FROM (SELECT 1 AS id)",
    ),
    (
        "map_case",
        "SELECT CASE WHEN id = 2 THEN map('k', 0.1) ELSE map('k', CAST(1e-20 AS DOUBLE)) "
        "END AS c FROM (SELECT 1 AS id)",
    ),
    (
        "values_A_planner_cast",
        "SELECT c FROM VALUES (CAST(1e-20 AS DOUBLE)), (CAST(2.5 AS DECIMAL(30,15))) AS v(c)",
    ),
    (
        "values_B_user_cast",
        "SELECT c FROM VALUES (CAST(CAST(1e-20 AS DOUBLE) AS DECIMAL(30,15))), (CAST(2.5 "
        "AS DECIMAL(30,15))) AS v(c)",
    ),
    (
        "values_C_user_cast_vs_double",
        "SELECT c FROM VALUES (CAST(CAST(1e-20 AS DOUBLE) AS DECIMAL(30,15))), (CAST(2 "
        "AS DOUBLE)) AS v(c)",
    ),
    (
        "semi_join_left_type",
        "SELECT dc AS c FROM (SELECT CAST(1.25 AS DECIMAL(10,2)) AS dc) a "
        "LEFT SEMI JOIN (SELECT CAST(1.25 AS DOUBLE) AS dd) b ON a.dc = b.dd",
    ),
    (
        "anti_join_left_type",
        "SELECT dc AS c FROM (SELECT CAST(1.25 AS DECIMAL(10,2)) AS dc) a "
        "LEFT ANTI JOIN (SELECT CAST(9.5 AS DOUBLE) AS dd) b ON a.dc = b.dd",
    ),
    (
        "rcte_dec_anchor",
        "WITH RECURSIVE r(n, c) AS (SELECT 1, 0.5 UNION ALL SELECT n + 1, CAST(1e-20 AS "
        "DOUBLE) FROM r WHERE n < 2) "
        "SELECT c FROM r",
    ),
    (
        "rcte_dbl_anchor",
        "WITH RECURSIVE r(n, c) AS (SELECT 1, CAST(1e-20 AS DOUBLE) UNION ALL SELECT n + "
        "1, 0.5 FROM r WHERE n < 2) "
        "SELECT c FROM r",
    ),
    (
        "rcte_dbl_anchor_one_col",
        "WITH RECURSIVE r(c) AS (SELECT CAST(1e-20 AS DOUBLE) UNION ALL SELECT 0.5 FROM "
        "r WHERE c < 0.1) SELECT c FROM r",
    ),
    ("zero_eq_today", f"SELECT v = d AS c FROM {ZSRC}"),
    ("zero_eq_widened", f"SELECT CAST(d AS DOUBLE) = v AS c FROM {ZSRC}"),
    (
        "zero_join_today",
        f"SELECT count(*) AS c FROM (SELECT {ZD} AS k) a JOIN (SELECT {NZ} AS k) b ON a.k = b.k",
    ),
    (
        "zero_join_widened",
        f"SELECT count(*) AS c FROM (SELECT {ZW} AS k) a JOIN (SELECT {NZ} AS k) b ON a.k = b.k",
    ),
    ("zero_intersect_today", f"SELECT count(*) AS c FROM (SELECT {ZD} AS k INTERSECT SELECT {NZ})"),
    (
        "zero_intersect_widened",
        f"SELECT count(*) AS c FROM (SELECT {ZW} AS k INTERSECT SELECT {NZ})",
    ),
    ("zero_except_today", f"SELECT count(*) AS c FROM (SELECT {ZD} AS k EXCEPT SELECT {NZ})"),
    ("zero_except_widened", f"SELECT count(*) AS c FROM (SELECT {ZW} AS k EXCEPT SELECT {NZ})"),
    (
        "zero_union_distinct_today",
        f"SELECT count(*) AS c FROM (SELECT {ZD} AS k UNION SELECT {NZ})",
    ),
    (
        "zero_union_distinct_widened",
        f"SELECT count(*) AS c FROM (SELECT {ZW} AS k UNION SELECT {NZ})",
    ),
    (
        "zero_group_by_today",
        f"SELECT count(*) AS c FROM (SELECT k FROM (SELECT {ZD} AS k UNION ALL SELECT "
        f"{NZ}) GROUP BY k)",
    ),
    (
        "zero_group_by_widened",
        f"SELECT count(*) AS c FROM (SELECT k FROM (SELECT {ZW} AS k UNION ALL SELECT "
        f"{NZ}) GROUP BY k)",
    ),
    (
        "zero_in_subquery_today",
        f"SELECT count(*) AS c FROM (SELECT {NZ} AS v) WHERE v IN (SELECT {ZD})",
    ),
    (
        "zero_in_subquery_widened",
        f"SELECT count(*) AS c FROM (SELECT {NZ} AS v) WHERE v IN (SELECT {ZW})",
    ),
]
PRUNE_SEED = [
    "INSERT INTO {t} VALUES (1, CAST(0.25 AS DECIMAL(10,2)), CAST(0.25 AS DOUBLE))",
    "INSERT INTO {t} VALUES (2, CAST(0.5 AS DECIMAL(10,2)), CAST(0.5 AS DOUBLE))",
    "INSERT INTO {t} VALUES (3, CAST(0.75 AS DECIMAL(10,2)), CAST(0.75 AS DOUBLE))",
]
PRUNE = [
    ("prune_decimal_literal", "SELECT id FROM {t} WHERE d = 0.5"),
    ("prune_today_double_literal", "SELECT id FROM {t} WHERE d = CAST(0.5 AS DOUBLE)"),
    (
        "prune_widened_double_literal",
        "SELECT id FROM {t} WHERE CAST(d AS DOUBLE) = CAST(0.5 AS DOUBLE)",
    ),
    ("prune_double_column_control", "SELECT id FROM {t} WHERE v = CAST(0.5 AS DOUBLE)"),
]
