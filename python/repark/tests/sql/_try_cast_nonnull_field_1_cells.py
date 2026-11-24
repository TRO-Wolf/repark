from __future__ import annotations

from typing import Any

ANSI_MODES: tuple[str, ...] = ("false", "true")

SWEEP_TRY_CAST_CELLS: tuple[tuple[str, str, list[list[Any]], list[str], list[bool]], ...] = (
    (
        "dfbug/cast_wide_neg_try_abs",
        "SELECT id, abs(CAST(-TRY_CAST(s AS INT) AS BIGINT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "bigint"],
        [False, True],
    ),
    (
        "dfbug/cast_wide_try_abs",
        "SELECT id, abs(CAST(TRY_CAST(s AS INT) AS BIGINT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "bigint"],
        [False, True],
    ),
    (
        "neg_try_dbl|cast/to_string",
        "SELECT id AS id, CAST(-TRY_CAST(s AS DOUBLE) AS STRING) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "-7.0"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "neg_try_dbl|math/abs",
        "SELECT id AS id, abs(-TRY_CAST(s AS DOUBLE)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7.0]],
        ["int", "double"],
        [False, True],
    ),
    (
        "neg_try_dbl|math/floor",
        "SELECT id AS id, floor(-TRY_CAST(s AS DOUBLE)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -7]],
        ["int", "bigint"],
        [False, True],
    ),
    (
        "neg_try_dbl|str/cast_len",
        (
            "SELECT id AS id, length(CAST(-TRY_CAST(s AS DOUBLE) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 4]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|bit/bit_count",
        "SELECT id AS id, bit_count(-TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 62]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|bit/bit_get",
        "SELECT id AS id, bit_get(-TRY_CAST(s AS INT), 0) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 1]],
        ["int", "tinyint"],
        [False, True],
    ),
    (
        "neg_try_int|bit/shiftleft",
        "SELECT id AS id, shiftleft(-TRY_CAST(s AS INT), 1) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -14]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|bit/shiftright",
        "SELECT id AS id, shiftright(-TRY_CAST(s AS INT), 1) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -4]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|coll/map_get",
        "SELECT id AS id, map('k', -TRY_CAST(s AS INT))['k'] AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|coll/named_struct_field",
        "SELECT id AS id, named_struct('a', -TRY_CAST(s AS INT)).a AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|coll/slice",
        (
            "SELECT id AS id, slice(array(1, 2, 3), -TRY_CAST(s AS INT), 1) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, []]],
        ["int", "array<int>"],
        [False, True],
    ),
    (
        "neg_try_int|math/abs",
        "SELECT id AS id, abs(-TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|math/add_1",
        "SELECT id AS id, -TRY_CAST(s AS INT) + 1 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -6]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|math/bin",
        "SELECT id AS id, bin(-TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "1111111111111111111111111111111111111111111111111111111111111001"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "neg_try_int|math/floor",
        "SELECT id AS id, floor(-TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -7]],
        ["int", "bigint"],
        [False, True],
    ),
    (
        "neg_try_int|math/mul_2",
        "SELECT id AS id, -TRY_CAST(s AS INT) * 2 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -14]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|math/sub_1",
        "SELECT id AS id, -TRY_CAST(s AS INT) - 1 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, -8]],
        ["int", "int"],
        [False, True],
    ),
    (
        "neg_try_int|str/chr",
        "SELECT id AS id, chr(-TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, ""]],
        ["int", "string"],
        [False, True],
    ),
    (
        "not_try_bool|bool/bit_count",
        (
            "SELECT id AS id, bit_count(NOT TRY_CAST(s_bool AS BOOLEAN)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 0]],
        ["int", "int"],
        [False, True],
    ),
    (
        "not_try_bool|bool/cast_int_abs",
        (
            "SELECT id AS id, abs(CAST(NOT TRY_CAST(s_bool AS BOOLEAN) AS INT)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 0]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_bool|bool/bit_count",
        "SELECT id AS id, bit_count(TRY_CAST(s_bool AS BOOLEAN)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 1]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_bool|bool/cast_int_abs",
        (
            "SELECT id AS id, abs(CAST(TRY_CAST(s_bool AS BOOLEAN) AS INT)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 1]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_dbl|cast/to_string",
        "SELECT id AS id, CAST(TRY_CAST(s AS DOUBLE) AS STRING) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "7.0"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_dbl|math/abs",
        "SELECT id AS id, abs(TRY_CAST(s AS DOUBLE)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7.0]],
        ["int", "double"],
        [False, True],
    ),
    (
        "try_dbl|math/floor",
        "SELECT id AS id, floor(TRY_CAST(s AS DOUBLE)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "bigint"],
        [False, True],
    ),
    (
        "try_dbl|str/cast_len",
        (
            "SELECT id AS id, length(CAST(TRY_CAST(s AS DOUBLE) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 3]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_dt_str|dt/date_add",
        "SELECT id AS id, date_add(TRY_CAST(s_dt AS DATE), 1) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "2024-01-16"]],
        ["int", "date"],
        [False, True],
    ),
    (
        "try_dt_str|dt/date_part_year",
        (
            "SELECT id AS id, date_part('year', TRY_CAST(s_dt AS DATE)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 2024]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_dt_str|dt/date_sub",
        "SELECT id AS id, date_sub(TRY_CAST(s_dt AS DATE), 1) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "2024-01-14"]],
        ["int", "date"],
        [False, True],
    ),
    (
        "try_dt_str|dt/extract_year",
        (
            "SELECT id AS id, extract(YEAR FROM TRY_CAST(s_dt AS DATE)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 2024]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_dt_str|dt/last_day",
        "SELECT id AS id, last_day(TRY_CAST(s_dt AS DATE)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "2024-01-31"]],
        ["int", "date"],
        [False, True],
    ),
    (
        "try_int|bit/bit_count",
        "SELECT id AS id, bit_count(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 3]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|bit/bit_get",
        "SELECT id AS id, bit_get(TRY_CAST(s AS INT), 0) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 1]],
        ["int", "tinyint"],
        [False, True],
    ),
    (
        "try_int|bit/shiftleft",
        "SELECT id AS id, shiftleft(TRY_CAST(s AS INT), 1) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 14]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|bit/shiftright",
        "SELECT id AS id, shiftright(TRY_CAST(s AS INT), 1) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 3]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|coll/map_get",
        "SELECT id AS id, map('k', TRY_CAST(s AS INT))['k'] AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|coll/named_struct_field",
        "SELECT id AS id, named_struct('a', TRY_CAST(s AS INT)).a AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|coll/slice",
        (
            "SELECT id AS id, slice(array(1, 2, 3), TRY_CAST(s AS INT), 1) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, []]],
        ["int", "array<int>"],
        [False, True],
    ),
    (
        "try_int|math/abs",
        "SELECT id AS id, abs(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|math/add_1",
        "SELECT id AS id, TRY_CAST(s AS INT) + 1 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 8]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|math/bin",
        "SELECT id AS id, bin(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "111"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_int|math/floor",
        "SELECT id AS id, floor(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "bigint"],
        [False, True],
    ),
    (
        "try_int|math/mul_2",
        "SELECT id AS id, TRY_CAST(s AS INT) * 2 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 14]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|math/sub_1",
        "SELECT id AS id, TRY_CAST(s AS INT) - 1 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, 6]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_int|str/chr",
        "SELECT id AS id, chr(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, "\x07"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|coll/str_to_map",
        (
            "SELECT id AS id, str_to_map(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, {"7": None}]],
        ["int", "map<string,string>"],
        [False, True],
    ),
    (
        "try_str|str/ascii",
        (
            "SELECT id AS id, ascii(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 55]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_str|str/base64",
        (
            "SELECT id AS id, base64(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, "Nw=="]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|str/bit_length",
        (
            "SELECT id AS id, bit_length(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 8]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_str|str/concat",
        (
            "SELECT id AS id, concat(TRY_CAST(TRY_CAST(s AS INT) AS STRING), 'z') AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, "7z"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|str/format_string",
        (
            "SELECT id AS id, format_string(TRY_CAST(TRY_CAST(s AS INT) AS STRING), 1) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, "7"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|str/initcap",
        (
            "SELECT id AS id, initcap(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, "7"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|str/instr",
        (
            "SELECT id AS id, instr(TRY_CAST(TRY_CAST(s AS INT) AS STRING), '7') AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 1]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_str|str/length",
        (
            "SELECT id AS id, length(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 1]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_str|str/octet_length",
        (
            "SELECT id AS id, octet_length(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 1]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_str|str/reverse",
        (
            "SELECT id AS id, reverse(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, "7"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|str/split_part",
        (
            "SELECT id AS id, split_part(TRY_CAST(TRY_CAST(s AS INT) AS STRING), ':', 1) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, "7"]],
        ["int", "string"],
        [False, True],
    ),
    (
        "try_str|str/startswith",
        (
            "SELECT id AS id, startswith(TRY_CAST(TRY_CAST(s AS INT) AS STRING), '7') AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, True]],
        ["int", "boolean"],
        [False, True],
    ),
    (
        "try_ts_int|dt/date_part_hour",
        (
            "SELECT id AS id, date_part('hour', TRY_CAST(TRY_CAST(s AS INT) AS TIMESTAMP)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 19]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_ts_int|dt/date_part_year",
        (
            "SELECT id AS id, date_part('year', TRY_CAST(TRY_CAST(s AS INT) AS TIMESTAMP)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 1969]],
        ["int", "int"],
        [False, True],
    ),
    (
        "try_ts_int|dt/extract_year",
        (
            "SELECT id AS id, extract(YEAR FROM TRY_CAST(TRY_CAST(s AS INT) AS TIMESTAMP)) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, None], [2, 1969]],
        ["int", "int"],
        [False, True],
    ),
)

DOOR_SQL_CASES: tuple[tuple[str, str, list[list[Any]], list[str], list[bool]], ...] = (
    (
        "inline_union_abs",
        "SELECT abs(TRY_CAST(s AS INT)) AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')",
        [[None], [7]],
        ["int"],
        [True],
    ),
    (
        "inline_union_add1",
        "SELECT TRY_CAST(s AS INT) + 1 AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')",
        [[None], [8]],
        ["int"],
        [True],
    ),
    (
        "cast_shield_eq",
        "SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, True]],
        ["int", "boolean"],
        [False, True],
    ),
    (
        "hof_transform_abs",
        (
            "SELECT id, transform(array(1, 2), e -> abs(TRY_CAST(s AS INT)) + e) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        [[1, [None, None]], [2, [8, 9]]],
        ["int", "array<int>"],
        [False, False],
    ),
    (
        "join_on_abs_try",
        (
            "SELECT a.id, a.r FROM (SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn) a "
            "LEFT JOIN (SELECT 7 AS k) b ON a.r = b.k ORDER BY a.id"
        ),
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "union_abs_try",
        (
            "SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn "
            "UNION ALL SELECT 3 AS id, 9 AS r ORDER BY id"
        ),
        [[1, None], [2, 7], [3, 9]],
        ["int", "int"],
        [False, True],
    ),
    (
        "merged_proj_map_limit",
        (
            "SELECT id, map('k', x)['k'] AS r "
            "FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM ice.sw.nn LIMIT 10) q ORDER BY id"
        ),
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "merged_proj_map_where",
        (
            "SELECT id, map('k', x)['k'] AS r "
            "FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM ice.sw.nn) q WHERE id > 0 ORDER BY id"
        ),
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "parquet_coalesce_abs",
        "SELECT id, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS r FROM pq_nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [True, True],
    ),
    (
        "mem_coalesce_abs",
        "SELECT id, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS r FROM mem_nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [True, True],
    ),
)

PARQUET_PUSHDOWN_A1_CASES: tuple[tuple[str, str, list[list[Any]], list[str], list[bool]], ...] = (
    (
        "cse_map_abs",
        (
            "SELECT id FROM pq_a1 "
            "WHERE map('k', TRY_CAST(coalesce(s, 'x') AS INT))['k'] > 0 "
            "AND abs(TRY_CAST(coalesce(s, 'x') AS INT)) > 5 ORDER BY id"
        ),
        [[2]],
        ["int"],
        [True],
    ),
    (
        "cse_two_maps",
        (
            "SELECT id FROM pq_a1 "
            "WHERE map('k', TRY_CAST(coalesce(s, 'x') AS INT))['k'] > 0 "
            "AND map('m', TRY_CAST(coalesce(s, 'x') AS INT))['m'] < 10 ORDER BY id"
        ),
        [[2]],
        ["int"],
        [True],
    ),
    (
        "cse_map_or",
        (
            "SELECT id FROM pq_a1 "
            "WHERE map('k', TRY_CAST(coalesce(s, 'x') AS INT))['k'] > 0 "
            "OR abs(TRY_CAST(coalesce(s, 'x') AS INT)) = 5 ORDER BY id"
        ),
        [[2], [3]],
        ["int"],
        [True],
    ),
    (
        "cse_map_and_proj",
        (
            "SELECT id, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS a FROM pq_a1 "
            "WHERE map('k', TRY_CAST(coalesce(s, 'x') AS INT))['k'] > 0 "
            "AND abs(TRY_CAST(coalesce(s, 'x') AS INT)) > 5 ORDER BY id"
        ),
        [[2, 7]],
        ["int", "int"],
        [True, True],
    ),
)

NATIVE_CASES: tuple[tuple[str, str, list[list[Any]], list[bool]], ...] = (
    (
        "mem_round_try_dbl",
        "SELECT id, round(TRY_CAST(s AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, 7.0]],
        [False, True],
    ),
    (
        "mem_round_neg_try_dbl",
        "SELECT id, round(-TRY_CAST(s AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, -7.0]],
        [False, True],
    ),
    (
        "mem_date_part_try_dt",
        "SELECT id, date_part('year', TRY_CAST(s_dt AS DATE)) AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, 2024]],
        [False, True],
    ),
    (
        "mem_strpos_try_str",
        (
            "SELECT id, strpos(TRY_CAST(TRY_CAST(s AS INT) AS VARCHAR), '7') AS r "
            "FROM nn_mem ORDER BY id"
        ),
        [[1, None], [2, 1]],
        [False, True],
    ),
    (
        "mem_cast_shield_eq",
        "SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, True]],
        [False, True],
    ),
    (
        "pq_coalesce_round",
        "SELECT id, round(TRY_CAST(coalesce(s, 'x') AS DOUBLE), 1) AS r FROM pq_nat ORDER BY id",
        [[1, None], [2, 7.0]],
        [True, True],
    ),
    (
        "pq_coalesce_date_part",
        (
            "SELECT id, date_part('year', TRY_CAST(coalesce(s_dt, 'x') AS DATE)) AS r "
            "FROM pq_nat ORDER BY id"
        ),
        [[1, None], [2, 2024]],
        [True, True],
    ),
)

NATIVE_PARQUET_PUSHDOWN_A1_CASES: tuple[tuple[str, str, list[list[Any]], list[bool]], ...] = (
    (
        "pq_a1_cse_map_abs",
        (
            "SELECT id FROM pq_a1_nat "
            "WHERE map('k', TRY_CAST(coalesce(s, 'x') AS INT))['k'] > 0 "
            "AND abs(TRY_CAST(coalesce(s, 'x') AS INT)) > 5 ORDER BY id"
        ),
        [[2]],
        [True],
    ),
)
