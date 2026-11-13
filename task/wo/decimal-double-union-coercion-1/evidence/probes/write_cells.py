U_TINY = "SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e-20 AS DOUBLE)"
MERGE_TAIL = (
    " ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT "
    "(id, v) VALUES (s.id, s.v)"
)
WRITES = [
    ("ins_union_tiny", "DOUBLE", False, "INSERT INTO {t} " + U_TINY),
    (
        "ins_union_big",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e30 AS DOUBLE)",
    ),
    (
        "ins_union_neg0",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST('-0.0' AS DOUBLE)",
    ),
    (
        "ins_union_nan",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST('NaN' AS DOUBLE)",
    ),
    (
        "ins_union_rev_tiny",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 1, CAST(1e-20 AS DOUBLE) UNION ALL SELECT 2, 0.1",
    ),
    (
        "ins_union_distinct_tiny",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION SELECT 2, CAST(1e-20 AS DOUBLE)",
    ),
    (
        "ins_values_tiny",
        "DOUBLE",
        False,
        "INSERT INTO {t} VALUES (1, 0.1), (2, CAST(1e-20 AS DOUBLE))",
    ),
    (
        "ins_values_big",
        "DOUBLE",
        False,
        "INSERT INTO {t} VALUES (1, 0.1), (2, CAST(1e30 AS DOUBLE))",
    ),
    (
        "ins_select_values_tiny",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT id, v FROM VALUES (1, 0.1), (2, CAST(1e-20 AS DOUBLE)) AS s(id, v)",
    ),
    (
        "ins_case_tiny",
        "DOUBLE",
        False,
        (
            "INSERT INTO {t} SELECT id, CASE WHEN id = 1 THEN 0.1 ELSE CAST(1e-20 AS DOUBLE) END "
            "FROM VALUES (1), (2) AS s(id)"
        ),
    ),
    (
        "ins_coalesce_tiny",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 2, coalesce(CAST(1e-20 AS DOUBLE), 0.1)",
    ),
    (
        "ins_greatest_big",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 2, greatest(0.1, CAST(1e30 AS DOUBLE))",
    ),
    (
        "ins_array_elem_tiny",
        "DOUBLE",
        False,
        "INSERT INTO {t} SELECT 2, array(0.1, CAST(1e-20 AS DOUBLE))[1]",
    ),
    (
        "ins_union_float_tiny",
        "FLOAT",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e-20 AS FLOAT)",
    ),
    (
        "ins_union_float_big",
        "FLOAT",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e30 AS FLOAT)",
    ),
    (
        "ins_union_dec_round",
        "DECIMAL(10,2)",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1.255 AS DOUBLE)",
    ),
    ("ins_union_dec_tiny", "DECIMAL(10,2)", False, "INSERT INTO {t} " + U_TINY),
    (
        "ins_union_dec_big",
        "DECIMAL(10,2)",
        False,
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e30 AS DOUBLE)",
    ),
    (
        "ins_values_dec_round",
        "DECIMAL(10,2)",
        False,
        "INSERT INTO {t} VALUES (1, 0.1), (2, CAST(1.255 AS DOUBLE))",
    ),
    (
        "upd_case_tiny",
        "DOUBLE",
        True,
        "UPDATE {t} SET v = CASE WHEN id = 1 THEN 0.1 ELSE CAST(1e-20 AS DOUBLE) END",
    ),
    (
        "upd_coalesce_tiny",
        "DOUBLE",
        True,
        "UPDATE {t} SET v = coalesce(CAST(1e-20 AS DOUBLE), 0.1) WHERE id = 2",
    ),
    ("upd_if_big", "DOUBLE", True, "UPDATE {t} SET v = IF(id = 1, 0.1, CAST(1e30 AS DOUBLE))"),
    (
        "upd_where_in_union",
        "DOUBLE",
        True,
        (
            "UPDATE {t} SET v = 1.0 WHERE CAST(id AS DOUBLE) * 1e-20 IN (SELECT 0.0 UNION ALL "
            "SELECT CAST(1e-20 AS DOUBLE))"
        ),
    ),
    (
        "merge_union_src",
        "DOUBLE",
        True,
        (
            "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS v UNION ALL SELECT 2, CAST(1e-20 AS "
            "DOUBLE) UNION ALL SELECT 3, CAST(-1e-20 AS DOUBLE)) s"
        )
        + MERGE_TAIL,
    ),
    (
        "merge_union_big",
        "DOUBLE",
        True,
        (
            "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS v UNION ALL SELECT 2, CAST(1e30 AS "
            "DOUBLE) UNION ALL SELECT 3, CAST(-1e30 AS DOUBLE)) s"
        )
        + MERGE_TAIL,
    ),
    (
        "merge_values_src",
        "DOUBLE",
        True,
        (
            "MERGE INTO {t} t USING (SELECT * FROM VALUES (1, 0.1), (2, CAST(1e-20 AS DOUBLE)), "
            "(3, CAST(-1e-20 AS DOUBLE)) AS s0(id, v)) s"
        )
        + MERGE_TAIL,
    ),
    (
        "merge_set_case",
        "DOUBLE",
        True,
        (
            "MERGE INTO {t} t USING (SELECT 2 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE "
            "SET v = CASE WHEN s.id = 1 THEN 0.1 ELSE CAST(1e-20 AS DOUBLE) END"
        ),
    ),
    (
        "merge_insert_coalesce",
        "DOUBLE",
        True,
        (
            "MERGE INTO {t} t USING (SELECT 3 AS id) s ON t.id = s.id WHEN NOT MATCHED THEN "
            "INSERT (id, v) VALUES (s.id, coalesce(CAST(1e-20 AS DOUBLE), 0.1))"
        ),
    ),
    (
        "ctas_union_tiny",
        None,
        False,
        (
            "CREATE TABLE {t} USING iceberg AS SELECT 1 AS id, 0.1 AS v UNION ALL SELECT 2, "
            "CAST(1e-20 AS DOUBLE)"
        ),
    ),
    ("df_union_append", "DOUBLE", False, "DF"),
]
