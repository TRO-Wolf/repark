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
