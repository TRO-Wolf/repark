TEXTS = [
    "1.255",
    "1.005",
    "2.675",
    "0.125",
    "0.375",
    "2.5",
    "-1.255",
    "-0.125",
    "-2.5",
    "1e-20",
    "123456789.125",
    "-0.0",
    "0.30000000000000004",
    "9.995",
    "99.95",
    "1e10",
    "2.82879384806159E17",
    "1e23",
    "NaN",
    "Infinity",
    "-Infinity",
]
TARGETS = [(10, 2), (5, 4), (38, 18), (38, 0), (3, 1)]
SOURCES = ["DOUBLE", "FLOAT"]
READ_DOORS = ["sel_cast_col", "sel_cast_lit", "df_cast"]
WRITE_DOORS = [
    "ins_values",
    "ins_select",
    "ins_union",
    "ctas",
    "update",
    "merge_update",
    "merge_insert",
    "df_writeto",
    "df_saveastable",
]
EXTRA_DOORS = ["ins_overwrite", "ins_overwrite_part", "df_insertinto", "df_overwrite"]
CARD = [
    ("ins_values_dec_round", "INSERT INTO {t} VALUES (1, 0.1), (2, CAST(1.255 AS DOUBLE))"),
    (
        "ins_union_dec_round",
        "INSERT INTO {t} SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1.255 AS DOUBLE)",
    ),
    ("ins_values_double_lit", "INSERT INTO {t} VALUES (1, 1.255D), (2, 1.005D)"),
    ("ins_select_double_lit", "INSERT INTO {t} SELECT 1, 1.255D UNION ALL SELECT 2, 1.005D"),
]


def lit(text: str, src: str) -> str:
    return f"CAST('{text}' AS {src})"


def ids() -> range:
    return range(len(TEXTS))


D1005 = "CAST('1.005' AS DOUBLE)"
D1255 = "CAST('1.255' AS DOUBLE)"
K = "CAST({} AS DECIMAL(5,2))"
SELECTS = [
    (
        "card_cast",
        f"SELECT CAST({D1005} AS DECIMAL(5,2)) AS a, CAST(1.005D AS DECIMAL(5,2)) AS b",
    ),
    (
        "subscript_struct",
        f"SELECT map(named_struct('k', {K.format('1.00')}, 'j', 2), 7, "
        f"named_struct('k', {K.format('1.01')}, 'j', 2), 8)[named_struct('k', {D1005}, 'j', 2)]",
    ),
    (
        "subscript_array",
        f"SELECT map(array({K.format('1.00')}), 7, array({K.format('1.01')}), 8)[array({D1005})]",
    ),
    ("cast_array", f"SELECT CAST(array({D1255}, {D1005}) AS ARRAY<DECIMAL(10,2)>)"),
    ("cast_struct", f"SELECT CAST(named_struct('a', {D1255}) AS STRUCT<a: DECIMAL(10,2)>)"),
    ("cast_map_value", f"SELECT CAST(map('k', {D1255}) AS MAP<STRING, DECIMAL(10,2)>)"),
    ("cast_map_key", f"SELECT CAST(map({D1255}, 1) AS MAP<DECIMAL(10,2), INT>)"),
    ("try_cast_round", f"SELECT TRY_CAST({D1255} AS DECIMAL(10,2))"),
    ("try_cast_overflow", "SELECT TRY_CAST(CAST('1e10' AS DOUBLE) AS DECIMAL(10,2))"),
    ("try_cast_nan", "SELECT TRY_CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(10,2))"),
    ("float_literal", "SELECT CAST(CAST('1.255' AS FLOAT) AS DECIMAL(10,2))"),
    (
        "double_literal_d",
        "SELECT CAST(1.255D AS DECIMAL(10,2)) AS a, CAST(1.255E0 AS DECIMAL(10,2)) AS b",
    ),
]
