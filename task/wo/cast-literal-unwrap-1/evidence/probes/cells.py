from dataclasses import dataclass, field

OPS = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">=", "ns": "<=>"}
SAMPLED_OPS = ("eq", "lt", "ge", "ns")


@dataclass(frozen=True)
class Kind:
    name: str
    family: str
    col_type: str
    target: str
    values: tuple
    consts: tuple
    in_list: tuple
    between: tuple
    lossless: bool = False
    dml: bool = False
    pylit: object = None
    pyin: tuple = field(default_factory=tuple)
    pybetween: tuple = field(default_factory=tuple)
    df_type: str = ""


def dec(v: str, t: str = "DECIMAL(10,2)") -> str:
    return f"CAST('{v}' AS {t})"


def typed(v: str, t: str) -> str:
    return f"CAST('{v}' AS {t})"


D102 = "DECIMAL(10,2)"


def with_null(items: object) -> tuple:
    return (*items, "NULL")


DEC_VALUES = with_null(
    dec(v)
    for v in (
        "0.50",
        "1.00",
        "1.25",
        "1.50",
        "1.99",
        "-0.50",
        "-1.25",
        "-1.50",
        "2.50",
        "0.00",
        "99999999.99",
        "-99999999.99",
    )
)
DBL_VALUES = with_null(
    f"CAST('{v}' AS DOUBLE)" for v in ("0.5", "1.0", "1.25", "1.5", "1.99", "-0.5", "-1.5", "2.5")
)
FLT_VALUES = with_null(
    f"CAST('{v}' AS FLOAT)" for v in ("0.5", "1.0", "1.25", "1.5", "1.99", "-0.5", "-1.5", "2.5")
)
TS_TEXTS = (
    "2024-01-01 00:00:00",
    "2024-01-01 10:30:00",
    "2024-01-01 23:59:59.999999",
    "2024-01-02 00:00:00",
    "2023-12-31 23:59:59",
)


def strs(*texts: str) -> tuple:
    return tuple("NULL" if t is None else "'" + t + "'" for t in texts)


KINDS = (
    Kind(
        "dec_int",
        "decimal-to-integer",
        D102,
        "INT",
        DEC_VALUES,
        ("1", "0", "-1", "2", "CAST(1 AS INT)", "(3 - 2)", "'1'", "1.0"),
        ("1", "-1"),
        ("1", "2"),
        dml=True,
        pylit=1,
        pyin=(1, -1),
        pybetween=(1, 2),
        df_type="int",
    ),
    Kind(
        "dec_tinyint",
        "decimal-to-integer",
        D102,
        "TINYINT",
        with_null(DEC_VALUES[:10]),
        ("1", "-1", "CAST(1 AS TINYINT)"),
        ("1", "2"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 2),
        pybetween=(0, 1),
        df_type="tinyint",
    ),
    Kind(
        "dec_smallint",
        "decimal-to-integer",
        D102,
        "SMALLINT",
        with_null(DEC_VALUES[:10]),
        ("1", "-1", "CAST(1 AS SMALLINT)"),
        ("1", "2"),
        ("-1", "1"),
        pylit=1,
        pyin=(1, 2),
        pybetween=(-1, 1),
        df_type="smallint",
    ),
    Kind(
        "dec_bigint",
        "decimal-to-integer",
        D102,
        "BIGINT",
        DEC_VALUES,
        ("1", "-1", "CAST(1 AS BIGINT)", "1.0"),
        ("1", "2"),
        ("1", "2"),
        pylit=1,
        pyin=(1, 2),
        pybetween=(1, 2),
        df_type="bigint",
    ),
    Kind(
        "dec_tinyint_overflow",
        "decimal-overflow",
        D102,
        "TINYINT",
        with_null(dec(v) for v in ("1.25", "127.00", "127.50", "128.00", "-129.00", "384.00")),
        ("1", "127", "0"),
        ("1", "127"),
        ("0", "127"),
        pylit=127,
        pyin=(1, 127),
        pybetween=(0, 127),
        df_type="tinyint",
    ),
    Kind(
        "dec38_int_overflow",
        "decimal-overflow",
        "DECIMAL(38,0)",
        "INT",
        with_null(
            dec(v, "DECIMAL(38,0)")
            for v in ("1", "-1", "3000000000", "-3000000000", "4294967297", "9" * 38)
        ),
        ("1", "0", "-1"),
        ("1", "0"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 0),
        pybetween=(0, 1),
        df_type="int",
    ),
    Kind(
        "dec_dec0",
        "decimal-to-smaller-scale",
        D102,
        "DECIMAL(10,0)",
        DEC_VALUES,
        ("1", "0", "-1", "2", "CAST(1 AS DECIMAL(10,0))", "1.0", "(3 - 2)"),
        ("1", "3"),
        ("1", "2"),
        dml=True,
        pylit=1,
        pyin=(1, 3),
        pybetween=(1, 2),
        df_type="decimal(10,0)",
    ),
    Kind(
        "dec_dec1",
        "decimal-to-smaller-scale",
        D102,
        "DECIMAL(10,1)",
        DEC_VALUES,
        ("1.3", "-1.3", "2", "1.5", "CAST(1.3 AS DECIMAL(10,1))"),
        ("1.3", "2.0"),
        ("1.3", "1.5"),
        dml=True,
        pylit="1.3",
        pyin=("1.3", "2.0"),
        pybetween=("1.3", "1.5"),
        df_type="decimal(10,1)",
    ),
    Kind(
        "dec_prec_overflow",
        "decimal-overflow",
        D102,
        "DECIMAL(3,2)",
        with_null(dec(v) for v in ("0.50", "1.25", "9.99", "10.00", "-10.00", "123.45")),
        ("1.25", "5", "9.99", "CAST(1.25 AS DECIMAL(3,2))"),
        ("1.25", "9.99"),
        ("1", "9.99"),
        pylit="1.25",
        pyin=("1.25", "9.99"),
        pybetween=("1", "9.99"),
        df_type="decimal(3,2)",
    ),
    Kind(
        "dec_round_overflow",
        "decimal-overflow",
        "DECIMAL(10,3)",
        "DECIMAL(3,2)",
        with_null(dec(v, "DECIMAL(10,3)") for v in ("1.000", "9.994", "9.995", "-9.995")),
        ("9.99", "5", "1"),
        ("9.99", "1"),
        ("5", "9.99"),
        pylit="9.99",
        pyin=("9.99", "1"),
        pybetween=("5", "9.99"),
        df_type="decimal(3,2)",
    ),
    Kind(
        "bigint_int",
        "integer-overflow",
        "BIGINT",
        "INT",
        with_null(
            f"CAST({v} AS BIGINT)"
            for v in (
                "1",
                "2",
                "-1",
                "0",
                "2147483647",
                "2147483648",
                "4294967297",
                "-2147483649",
                "-9223372036854775808",
            )
        ),
        ("1", "-1", "2147483647", "0", "CAST(1 AS INT)"),
        ("1", "2147483647"),
        ("0", "1"),
        dml=True,
        pylit=1,
        pyin=(1, 2147483647),
        pybetween=(0, 1),
        df_type="int",
    ),
    Kind(
        "int_smallint",
        "integer-overflow",
        "INT",
        "SMALLINT",
        with_null(f"CAST({v} AS INT)" for v in ("1", "2", "32767", "32768", "65537", "-32769")),
        ("1", "32767", "-1"),
        ("1", "32767"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 32767),
        pybetween=(0, 1),
        df_type="smallint",
    ),
    Kind(
        "double_int",
        "float-to-integer",
        "DOUBLE",
        "INT",
        DBL_VALUES,
        ("1", "0", "-1", "CAST(1 AS INT)"),
        ("1", "2"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 2),
        pybetween=(0, 1),
        df_type="int",
    ),
    Kind(
        "double_int_edge",
        "float-to-integer",
        "DOUBLE",
        "INT",
        with_null(
            f"CAST('{v}' AS DOUBLE)"
            for v in ("1.0", "NaN", "Infinity", "-Infinity", "1e20", "-1e20")
        ),
        ("0", "2147483647", "1"),
        ("0", "2147483647"),
        ("0", "1"),
        pylit=2147483647,
        pyin=(0, 2147483647),
        pybetween=(0, 1),
        df_type="int",
    ),
    Kind(
        "double_bigint",
        "float-to-integer",
        "DOUBLE",
        "BIGINT",
        DBL_VALUES,
        ("1", "-1"),
        ("1", "2"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 2),
        pybetween=(0, 1),
        df_type="bigint",
    ),
    Kind(
        "float_int",
        "float-to-integer",
        "FLOAT",
        "INT",
        FLT_VALUES,
        ("1", "0"),
        ("1", "2"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 2),
        pybetween=(0, 1),
        df_type="int",
    ),
    Kind(
        "double_float",
        "double-to-float",
        "DOUBLE",
        "FLOAT",
        with_null(
            f"CAST('{v}' AS DOUBLE)"
            for v in ("0.1", "0.10000000149011612", "1.0", "1e39", "3.4028235e38", "-1e39")
        ),
        ("CAST(0.1 AS FLOAT)", "CAST('Infinity' AS FLOAT)", "1.0", "1"),
        ("CAST(0.1 AS FLOAT)", "CAST(1 AS FLOAT)"),
        ("CAST(0.1 AS FLOAT)", "CAST(1 AS FLOAT)"),
        df_type="float",
    ),
    Kind(
        "double_dec",
        "double-to-decimal",
        "DOUBLE",
        "DECIMAL(10,1)",
        with_null(
            f"CAST('{v}' AS DOUBLE)" for v in ("1.25", "1.35", "1.0", "0.05", "-1.25", "1.3")
        ),
        ("1.3", "1.4", "1", "CAST(1.3 AS DECIMAL(10,1))"),
        ("1.3", "1.4"),
        ("1.3", "1.4"),
        pylit="1.3",
        pyin=("1.3", "1.4"),
        pybetween=("1.3", "1.4"),
        df_type="decimal(10,1)",
    ),
    Kind(
        "double_dec_overflow",
        "double-to-decimal",
        "DOUBLE",
        "DECIMAL(3,1)",
        with_null(f"CAST('{v}' AS DOUBLE)" for v in ("1.25", "99.9", "99.96", "1000.0")),
        ("1.3", "99.9", "50"),
        ("1.3", "99.9"),
        ("1", "99.9"),
        pylit="1.3",
        pyin=("1.3", "99.9"),
        pybetween=("1", "99.9"),
        df_type="decimal(3,1)",
    ),
    Kind(
        "str_int",
        "string",
        "STRING",
        "INT",
        strs("1", "01", " 1", "1 ", "+1", "1.0", "1.9", "-1", "2", None),
        ("1", "2", "-1", "'1'", "CAST(1 AS INT)"),
        ("1", "2"),
        ("1", "2"),
        dml=True,
        pylit=1,
        pyin=(1, 2),
        pybetween=(1, 2),
        df_type="int",
    ),
    Kind(
        "str_int_invalid",
        "string",
        "STRING",
        "INT",
        strs("1", "1e0", "abc", "", "2147483648", None),
        ("1", "0"),
        ("1", "0"),
        ("0", "1"),
        pylit=1,
        pyin=(1, 0),
        pybetween=(0, 1),
        df_type="int",
    ),
    Kind(
        "str_dec",
        "string",
        "STRING",
        D102,
        strs("1", "01", " 1", "1.0", "+1", "1e0", "1.005", "1.25", "1.254", None),
        ("1", "1.25", "1.01"),
        ("1", "1.25"),
        ("1", "1.01"),
        pylit="1.25",
        pyin=("1", "1.25"),
        pybetween=("1", "1.01"),
        df_type="decimal(10,2)",
    ),
    Kind(
        "str_double",
        "string",
        "STRING",
        "DOUBLE",
        strs("1", "01", " 1", "1.0", "+1", "1e0", "1.5", "NaN", "Infinity", "1d", None),
        ("1", "1.5", "CAST('NaN' AS DOUBLE)"),
        ("1", "1.5"),
        ("1", "1.5"),
        pylit=1.0,
        pyin=(1.0, 1.5),
        pybetween=(1.0, 1.5),
        df_type="double",
    ),
    Kind(
        "str_date",
        "string",
        "STRING",
        "DATE",
        strs(
            "2024-01-01",
            "2024-1-1",
            " 2024-01-01",
            "2024-01-01 10:00:00",
            "2024-01-01T10:00",
            "2024-01",
            "2024",
            "2024-01-02",
            None,
        ),
        ("DATE'2024-01-01'", "'2024-01-01'", "DATE'2024-01-02'"),
        ("DATE'2024-01-01'", "DATE'2024-01-02'"),
        ("DATE'2024-01-01'", "DATE'2024-01-02'"),
        dml=True,
        pylit="date:2024-01-01",
        pyin=("date:2024-01-01", "date:2024-01-02"),
        pybetween=("date:2024-01-01", "date:2024-01-02"),
        df_type="date",
    ),
    Kind(
        "ts_date",
        "temporal",
        "TIMESTAMP",
        "DATE",
        with_null(typed(t, "TIMESTAMP") for t in TS_TEXTS),
        ("DATE'2024-01-01'", "'2024-01-01'", "CAST('2024-01-01' AS DATE)", "DATE'2024-01-02'"),
        ("DATE'2024-01-01'", "DATE'2023-12-31'"),
        ("DATE'2024-01-01'", "DATE'2024-01-02'"),
        dml=True,
        pylit="date:2024-01-01",
        pyin=("date:2024-01-01", "date:2023-12-31"),
        pybetween=("date:2024-01-01", "date:2024-01-02"),
        df_type="date",
    ),
    Kind(
        "ntz_date",
        "temporal",
        "TIMESTAMP_NTZ",
        "DATE",
        with_null(typed(t, "TIMESTAMP_NTZ") for t in TS_TEXTS),
        ("DATE'2024-01-01'", "'2024-01-01'", "DATE'2024-01-02'"),
        ("DATE'2024-01-01'", "DATE'2023-12-31'"),
        ("DATE'2024-01-01'", "DATE'2024-01-02'"),
        pylit="date:2024-01-01",
        pyin=("date:2024-01-01", "date:2023-12-31"),
        pybetween=("date:2024-01-01", "date:2024-01-02"),
        df_type="date",
    ),
    Kind(
        "int_bigint",
        "control",
        "INT",
        "BIGINT",
        with_null(f"CAST({v} AS INT)" for v in ("1", "2", "-1", "2147483647", "-2147483648")),
        ("1", "CAST(1 AS BIGINT)", "2147483648", "-1"),
        ("1", "2147483647"),
        ("0", "2"),
        lossless=True,
        pylit=1,
        pyin=(1, 2147483647),
        pybetween=(0, 2),
        df_type="bigint",
    ),
    Kind(
        "dec_wide",
        "control",
        D102,
        "DECIMAL(20,4)",
        DEC_VALUES,
        ("1.25", "1", "1.2500", "1.255"),
        ("1.25", "1.5"),
        ("1", "1.5"),
        lossless=True,
        pylit="1.25",
        pyin=("1.25", "1.5"),
        pybetween=("1", "1.5"),
        df_type="decimal(20,4)",
    ),
    Kind(
        "date_ts",
        "control",
        "DATE",
        "TIMESTAMP",
        with_null(typed(t, "DATE") for t in ("2024-01-01", "2024-01-02", "2023-12-31")),
        ("TIMESTAMP'2024-01-01 00:00:00'", "TIMESTAMP'2024-01-01 10:00:00'", "'2024-01-01'"),
        ("TIMESTAMP'2024-01-01 00:00:00'", "TIMESTAMP'2024-01-02 00:00:00'"),
        ("TIMESTAMP'2024-01-01 00:00:00'", "TIMESTAMP'2024-01-01 12:00:00'"),
        lossless=True,
        pylit="ts:2024-01-01 00:00:00",
        pyin=("ts:2024-01-01 00:00:00", "ts:2024-01-02 00:00:00"),
        pybetween=("ts:2024-01-01 00:00:00", "ts:2024-01-01 12:00:00"),
        df_type="timestamp",
    ),
)


def values_sql(kind: Kind) -> str:
    rows = ", ".join(f"({i + 1}, {v})" for i, v in enumerate(kind.values))
    return rows


def select_sql(kind: Kind) -> str:
    items = ", ".join(f"CAST({v} AS {kind.col_type})" for v in kind.values)
    n = len(kind.values)
    return (
        f"SELECT CAST(id AS INT) AS id, element_at(array({items}), CAST(id AS INT)) AS c "
        f"FROM range(1, {n + 1})"
    )


def cast_expr(kind: Kind, column: str = "c") -> str:
    return f"CAST({column} AS {kind.target})"


def predicate(kind: Kind, op: str, const: str, side: str, column: str = "c") -> str:
    cast = cast_expr(kind, column)
    return f"{cast} {OPS[op]} {const}" if side == "r" else f"{const} {OPS[op]} {cast}"


def where_cells(kind: Kind, dense: bool) -> list:
    out = []
    consts = kind.consts if dense else kind.consts[:2]
    for ci, const in enumerate(consts):
        for op in OPS:
            sides = ("r", "l") if dense or op == "eq" else ("r",)
            if not dense and op not in SAMPLED_OPS:
                continue
            for side in sides:
                out.append((f"c{ci}/{op}/{side}", predicate(kind, op, const, side), const))
    cast = cast_expr(kind)
    a, b = kind.in_list
    lo, hi = kind.between
    out.append(("in", f"{cast} IN ({a}, {b})", f"{a},{b}"))
    out.append(("notin", f"{cast} NOT IN ({a}, {b})", f"{a},{b}"))
    out.append(("between", f"{cast} BETWEEN {lo} AND {hi}", f"{lo},{hi}"))
    if dense:
        out.append(("notbetween", f"{cast} NOT BETWEEN {lo} AND {hi}", f"{lo},{hi}"))
    return out


def position_cells(kind: Kind, view: str, other: str) -> list:
    const = kind.consts[0]
    eq = predicate(kind, "eq", const, "r")
    lt = predicate(kind, "lt", const, "r")
    ja = predicate(kind, "eq", const, "r", "a.c")
    return [
        ("select/eq", f"SELECT id, {eq} AS x FROM {view}"),
        ("select/lt", f"SELECT id, {lt} AS x FROM {view}"),
        ("case/eq", f"SELECT id, CASE WHEN {eq} THEN 'y' ELSE 'n' END AS x FROM {view}"),
        ("having/eq", f"SELECT id, count(*) AS x FROM {view} GROUP BY id, c HAVING {eq}"),
        (
            "join/eq",
            f"SELECT a.id, b.id AS x FROM {view} a JOIN {other} b ON a.id = b.id AND {ja}",
        ),
    ]
