from __future__ import annotations

from typing import Any

ANSI_MODES: tuple[str, ...] = ("false", "true")

SPARK_SQL_BUG_CELLS: tuple[tuple[str, str, list[list[Any]]], ...] = (
    ("tbl_isnull_select", "SELECT (SELECT b FROM ice.ns.empty_nn) IS NULL AS r", [[True]]),
    (
        "tbl_isnotnull_select",
        "SELECT (SELECT b FROM ice.ns.empty_nn) IS NOT NULL AS r",
        [[False]],
    ),
    (
        "tbl_isnull_where",
        "SELECT id FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "tbl_isnotnull_where",
        "SELECT id FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NOT NULL ORDER BY id",
        [],
    ),
    (
        "tbl_coalesce_select",
        "SELECT id, coalesce((SELECT b FROM ice.ns.empty_nn), 42) AS c "
        "FROM ice.ns.l_nn ORDER BY id",
        [[1, 42], [2, 42]],
    ),
    (
        "tbl_ifnull_select",
        "SELECT ifnull((SELECT b FROM ice.ns.empty_nn), 42) AS r",
        [[42]],
    ),
    (
        "tbl_nvl_select",
        "SELECT nvl((SELECT b FROM ice.ns.empty_nn), 42) AS r",
        [[42]],
    ),
    (
        "tbl_nvl2_select",
        "SELECT nvl2((SELECT b FROM ice.ns.empty_nn), 1, 2) AS r",
        [[2]],
    ),
    (
        "tbl_case_isnull",
        "SELECT CASE WHEN (SELECT b FROM ice.ns.empty_nn) IS NULL THEN 1 ELSE 2 END AS r",
        [[1]],
    ),
    (
        "tbl_bare_select",
        "SELECT (SELECT b FROM ice.ns.empty_nn) AS r",
        [[None]],
    ),
    (
        "inline_bare_select",
        "SELECT (SELECT 1 WHERE false) AS r",
        [[None]],
    ),
    (
        "tbl_self_eq",
        "SELECT ((SELECT b FROM ice.ns.empty_nn) = (SELECT b FROM ice.ns.empty_nn)) AS r",
        [[None]],
    ),
    (
        "tbl_like_percent",
        "SELECT ((SELECT s FROM ice.ns.empty_nn) LIKE '%') AS r",
        [[None]],
    ),
    (
        "tbl_mod_one",
        "SELECT ((SELECT b FROM ice.ns.empty_nn) % 1) AS r",
        [[None]],
    ),
    (
        "tbl_bitand_zero",
        "SELECT ((SELECT b FROM ice.ns.empty_nn) & 0) AS r",
        [[None]],
    ),
    (
        "tbl_bool_and_true",
        "SELECT ((SELECT flag FROM ice.ns.empty_nn) AND true) AS r",
        [[None]],
    ),
    (
        "tbl_bool_or_false",
        "SELECT ((SELECT flag FROM ice.ns.empty_nn) OR false) AS r",
        [[None]],
    ),
    (
        "tbl_limit1_isnull",
        "SELECT (SELECT b FROM ice.ns.empty_nn LIMIT 1) IS NULL AS r",
        [[True]],
    ),
    (
        "tbl_nonempty_where_false_isnull",
        "SELECT (SELECT b FROM ice.ns.r_nn WHERE b < 0) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_lit_where_false_isnull",
        "SELECT (SELECT 1 WHERE false) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_lit_where_false_coalesce",
        "SELECT coalesce((SELECT 1 WHERE false), 42) AS r",
        [[42]],
    ),
    (
        "inline_lit_limit0_isnull",
        "SELECT (SELECT 1 LIMIT 0) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_lit_offset1_isnull",
        "SELECT (SELECT 1 LIMIT 1 OFFSET 1) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_utf8view_subq",
        "SELECT (SELECT 'hello' WHERE false) AS r",
        [[None]],
    ),
    (
        "agg_count_having_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn HAVING COUNT(*) > 0) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_count_having_coalesce",
        "SELECT coalesce((SELECT COUNT(*) FROM ice.ns.empty_nn HAVING COUNT(*) > 0), 42) AS r",
        [[42]],
    ),
    (
        "agg_count_groupby_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn GROUP BY b) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_count_groupby_where",
        "SELECT id FROM ice.ns.l_nn "
        "WHERE (SELECT COUNT(*) FROM ice.ns.empty_nn GROUP BY b) IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "agg_count_limit0_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn LIMIT 0) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_count_offset1_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn LIMIT 1 OFFSET 1) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_coalesce_max_having_isnull",
        "SELECT (SELECT coalesce(MAX(b), 0) FROM ice.ns.empty_nn HAVING COUNT(*) > 0) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_groupby_b_rollup_b_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn GROUP BY b, ROLLUP(b)) IS NULL AS r",
        [[True]],
    ),
    (
        "corr_nonagg_limit1_where_isnull",
        "SELECT l.id FROM ice.ns.l_nn l "
        "WHERE (SELECT r.b FROM ice.ns.r_nn r WHERE r.id = l.id LIMIT 1) IS NULL "
        "ORDER BY l.id",
        [[1]],
    ),
    (
        "corr_nonagg_limit1_select_isnull",
        "SELECT l.id, "
        "(SELECT r.b FROM ice.ns.r_nn r WHERE r.id = l.id LIMIT 1) IS NULL AS n "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, True], [2, False]],
    ),
    (
        "corr_nonagg_limit1_coalesce",
        "SELECT l.id, "
        "coalesce((SELECT r.b FROM ice.ns.r_nn r WHERE r.id = l.id LIMIT 1), 42) AS c "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, 42], [2, 20]],
    ),
    (
        "corr_count_having_where_isnull",
        "SELECT l.id FROM ice.ns.l_nn l "
        "WHERE (SELECT COUNT(*) FROM ice.ns.r_nn r "
        "WHERE r.id = l.id HAVING COUNT(*) > 0) IS NULL ORDER BY l.id",
        [[1]],
    ),
    (
        "corr_count_having_coalesce",
        "SELECT l.id, coalesce((SELECT COUNT(*) FROM ice.ns.r_nn r "
        "WHERE r.id = l.id HAVING COUNT(*) > 0), 42) AS c "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, 42], [2, 1]],
    ),
    (
        "corr_count_having_eq0_where_isnull",
        "SELECT l.id FROM ice.ns.l_nn l "
        "WHERE (SELECT COUNT(*) FROM ice.ns.r_nn r "
        "WHERE r.id = l.id HAVING COUNT(*) = 0) IS NULL ORDER BY l.id",
        [[2]],
    ),
    (
        "corr_count_groupby_where_isnull",
        "SELECT l.id FROM ice.ns.l_nn l "
        "WHERE (SELECT COUNT(*) FROM ice.ns.r_nn r "
        "WHERE r.id = l.id GROUP BY r.b) IS NULL ORDER BY l.id",
        [[1]],
    ),
    (
        "corr_coalesce_max_having_isnull",
        "SELECT l.id, (SELECT coalesce(MAX(r.b), 0) FROM ice.ns.r_nn r "
        "WHERE r.id = l.id HAVING COUNT(*) > 0) IS NULL AS n "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, True], [2, False]],
    ),
    (
        "cte_with_scalar_subq_isnull",
        "WITH c AS (SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn) "
        "SELECT id, r IS NULL AS n, coalesce(r, 42) AS cv FROM c ORDER BY id",
        [[1, True, 42], [2, True, 42]],
    ),
    (
        "cte_with_scalar_subq_where_isnull",
        "WITH c AS (SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn) "
        "SELECT id FROM c WHERE r IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "cte_empty_in_scalar_subq_isnull",
        "WITH e AS (SELECT b FROM ice.ns.empty_nn) SELECT (SELECT b FROM e) IS NULL AS r",
        [[True]],
    ),
    (
        "cte_nested_scalar_subq_isnull",
        "WITH c1 AS (SELECT b FROM ice.ns.empty_nn), "
        "c2 AS (SELECT (SELECT b FROM c1) AS x) SELECT x IS NULL AS r FROM c2",
        [[True]],
    ),
    (
        "cast_date_subq_to_ts_isnull",
        "SELECT CAST((SELECT d FROM ice.ns.empty_nn) AS TIMESTAMP) IS NULL AS r",
        [[True]],
    ),
    (
        "cast_date_subq_to_ts_val",
        "SELECT CAST((SELECT d FROM ice.ns.empty_nn) AS TIMESTAMP) AS r",
        [[None]],
    ),
    (
        "nested_scalar_subq_isnull",
        "SELECT (SELECT (SELECT b FROM ice.ns.empty_nn) FROM ice.ns.l_nn LIMIT 1) IS NULL AS r",
        [[True]],
    ),
    (
        "nested_scalar_subq_outer_empty",
        "SELECT (SELECT (SELECT 1) FROM ice.ns.empty_nn) IS NULL AS r",
        [[True]],
    ),
    (
        "scalar_subq_in_having",
        "SELECT id FROM ice.ns.l_nn GROUP BY id "
        "HAVING (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "scalar_subq_in_join_on",
        "SELECT l.id FROM ice.ns.l_nn l INNER JOIN ice.ns.l_nn r "
        "ON l.id = r.id AND (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY l.id",
        [[1], [2]],
    ),
    (
        "union_with_empty_subq",
        "SELECT r FROM (SELECT (SELECT b FROM ice.ns.empty_nn) AS r "
        "UNION ALL SELECT 1 AS r) t ORDER BY r NULLS FIRST",
        [[None], [1]],
    ),
)

SQL_CONTROL_CELLS: tuple[tuple[str, str, list[list[Any]]], ...] = (
    ("agg_count_isnull", "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn) IS NULL AS r", [[False]]),
    (
        "agg_count_coalesce",
        "SELECT coalesce((SELECT COUNT(*) FROM ice.ns.empty_nn), 42) AS r",
        [[0]],
    ),
    (
        "agg_coalesce_max_nohaving",
        "SELECT (SELECT coalesce(MAX(b), 0) FROM ice.ns.empty_nn) IS NULL AS r",
        [[False]],
    ),
    ("lit_1_isnull", "SELECT (SELECT 1) IS NULL AS r", [[False]]),
    ("values_1_isnull", "SELECT (SELECT c FROM (VALUES (1)) AS t(c)) IS NULL AS r", [[False]]),
    (
        "tbl_null_safe_eq_null",
        "SELECT ((SELECT b FROM ice.ns.empty_nn) <=> CAST(NULL AS INT)) AS r",
        [[True]],
    ),
    (
        "tbl_null_safe_eq_val",
        "SELECT ((SELECT b FROM ice.ns.empty_nn) <=> 1) AS r",
        [[False]],
    ),
    (
        "corr_count_select_val",
        "SELECT l.id, (SELECT COUNT(*) FROM ice.ns.r_nn r WHERE r.id = l.id) AS c "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, 0], [2, 1]],
    ),
    (
        "corr_coalesce_max_nohaving",
        "SELECT l.id, "
        "(SELECT coalesce(MAX(r.b), 0) FROM ice.ns.r_nn r WHERE r.id = l.id) AS c "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, 0], [2, 20]],
    ),
)

NATIVE_SQL_BUG_CELLS: tuple[tuple[str, str, list[list[Any]]], ...] = (
    ("tbl_isnull_select", "SELECT (SELECT b FROM ice.ns.empty_nn) IS NULL AS r", [[True]]),
    (
        "tbl_isnotnull_select",
        "SELECT (SELECT b FROM ice.ns.empty_nn) IS NOT NULL AS r",
        [[False]],
    ),
    (
        "tbl_isnull_where",
        "SELECT id FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "tbl_coalesce_select",
        "SELECT id, coalesce((SELECT b FROM ice.ns.empty_nn), 42) AS c "
        "FROM ice.ns.l_nn ORDER BY id",
        [[1, 42], [2, 42]],
    ),
    (
        "tbl_case_isnull",
        "SELECT CASE WHEN (SELECT b FROM ice.ns.empty_nn) IS NULL THEN 1 ELSE 2 END AS r",
        [[1]],
    ),
    (
        "tbl_self_eq",
        "SELECT ((SELECT b FROM ice.ns.empty_nn) = (SELECT b FROM ice.ns.empty_nn)) AS r",
        [[None]],
    ),
    (
        "tbl_limit1_isnull",
        "SELECT (SELECT b FROM ice.ns.empty_nn LIMIT 1) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_lit_where_false_isnull",
        "SELECT (SELECT 1 WHERE false) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_lit_limit0_isnull",
        "SELECT (SELECT 1 LIMIT 0) IS NULL AS r",
        [[True]],
    ),
    (
        "inline_lit_offset1_isnull",
        "SELECT (SELECT 1 LIMIT 1 OFFSET 1) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_count_having_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn HAVING COUNT(*) > 0) IS NULL AS r",
        [[True]],
    ),
    (
        "agg_count_groupby_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn GROUP BY b) IS NULL AS r",
        [[True]],
    ),
    (
        "corr_nonagg_limit1_where_isnull",
        "SELECT l.id FROM ice.ns.l_nn l "
        "WHERE (SELECT r.b FROM ice.ns.r_nn r WHERE r.id = l.id LIMIT 1) IS NULL "
        "ORDER BY l.id",
        [[1]],
    ),
    (
        "corr_count_having_coalesce",
        "SELECT l.id, coalesce((SELECT COUNT(*) FROM ice.ns.r_nn r "
        "WHERE r.id = l.id HAVING COUNT(*) > 0), 42) AS c "
        "FROM ice.ns.l_nn l ORDER BY l.id",
        [[1, 42], [2, 1]],
    ),
    (
        "cte_with_scalar_subq_isnull",
        "WITH c AS (SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn) "
        "SELECT id, r IS NULL AS n, coalesce(r, 42) AS cv FROM c ORDER BY id",
        [[1, True, 42], [2, True, 42]],
    ),
    (
        "cte_empty_in_scalar_subq_isnull",
        "WITH e AS (SELECT b FROM ice.ns.empty_nn) SELECT (SELECT b FROM e) IS NULL AS r",
        [[True]],
    ),
    (
        "cast_date_subq_to_ts_isnull",
        "SELECT CAST((SELECT d FROM ice.ns.empty_nn) AS TIMESTAMP) IS NULL AS r",
        [[True]],
    ),
    (
        "nested_scalar_subq_outer_empty",
        "SELECT (SELECT (SELECT 1) FROM ice.ns.empty_nn) IS NULL AS r",
        [[True]],
    ),
    (
        "scalar_subq_in_having",
        "SELECT id FROM ice.ns.l_nn GROUP BY id "
        "HAVING (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "scalar_subq_in_join_on",
        "SELECT l.id FROM ice.ns.l_nn l INNER JOIN ice.ns.l_nn r "
        "ON l.id = r.id AND (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY l.id",
        [[1], [2]],
    ),
    (
        "union_with_empty_subq",
        "SELECT r FROM (SELECT (SELECT b FROM ice.ns.empty_nn) AS r "
        "UNION ALL SELECT 1 AS r) t ORDER BY r NULLS FIRST",
        [[None], [1]],
    ),
)

PHYS_UNCORR_CELLS: tuple[tuple[str, str, list[list[Any]]], ...] = (
    ("phys_isnull_select", "SELECT (SELECT b FROM ice.ns.empty_nn) IS NULL AS r", [[True]]),
    (
        "phys_isnull_where",
        "SELECT id FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id",
        [[1], [2]],
    ),
    (
        "phys_coalesce",
        "SELECT id, coalesce((SELECT b FROM ice.ns.empty_nn), 42) AS c "
        "FROM ice.ns.l_nn ORDER BY id",
        [[1, 42], [2, 42]],
    ),
    (
        "phys_count_having_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn HAVING COUNT(*) > 0) IS NULL AS r",
        [[True]],
    ),
    (
        "phys_count_groupby_isnull",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn GROUP BY b) IS NULL AS r",
        [[True]],
    ),
    (
        "phys_count_control",
        "SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn) IS NULL AS r",
        [[False]],
    ),
)

DF_EXPECTED: dict[str, list[list[Any]]] = {
    "fexpr_withcol_isnull": [[1, True], [2, True]],
    "fexpr_withcol_isnotnull": [[1, False], [2, False]],
    "fexpr_withcol_coalesce": [[1, 42], [2, 42]],
    "fexpr_withcol_coalesce_id": [[1, 1], [2, 2]],
    "fexpr_withcol_bare": [[1, None], [2, None]],
    "fexpr_withcol_cast_date_ts": [[1, None], [2, None]],
    "fexpr_filter_col_isnull": [[1], [2]],
    "fexpr_filter_col_isnotnull": [],
    "filter_str_inline_isnull": [[1], [2]],
    "filter_str_inline_isnotnull": [],
    "selectexpr_tbl_isnull": [[1, True], [2, True]],
    "selectexpr_tbl_coalesce": [[1, 42], [2, 42]],
    "withcol_scalar_isnull": [[1, True], [2, True]],
    "withcol_scalar_coalesce": [[1, 42], [2, 42]],
    "filter_scalar_isnull": [[1], [2]],
    "filter_scalar_isnotnull": [],
}

STORED_EXPECTED: dict[str, list[list[Any]]] = {
    "spark_tv_sql_select": [[1, None, True, 42], [2, None, True, 42]],
    "spark_tv_sql_where": [[1], [2]],
    "spark_tv_df_where": [[1], [2]],
    "spark_tv_df_ansi_flip": [[1, True, 42], [2, True, 42]],
    "spark_cached_where": [[1, None], [2, None]],
    "spark_prep_select": [[True]],
    "spark_prep_param": [[1, 42], [2, 42]],
    "native_view_where": [[1], [2]],
    "native_prep_param": [[1, 42], [2, 42]],
}
