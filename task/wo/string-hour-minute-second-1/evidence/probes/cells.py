from collections.abc import Callable
from typing import Any

Build = Callable[[Any, Any], Any]

SHAPES: dict[str, str | None] = {
    "date": "2024-01-01",
    "full": "2024-01-01 03:04:05",
    "tsep": "2024-06-01T23:30:00",
    "frac": "2024-01-01 03:04:05.123456",
    "off": "2024-01-01T03:04:05+05:30",
    "zulu": "2024-01-01T03:04:05Z",
    "lead": "  2024-01-01 03:04:05",
    "trail": "2024-01-01 03:04:05  ",
    "empty": "",
    "null": None,
    "bad": "not a time",
    "nosec": "2024-01-01 03:04",
    "tonly": "03:04:05",
    "badtime": "2024-01-01 25:00:00",
    "gap": "2024-03-10 02:30:00",
    "yonly": "2024",
}

G_SHAPES = [
    "date",
    "full",
    "tsep",
    "frac",
    "off",
    "zulu",
    "lead",
    "trail",
    "null",
    "nosec",
    "gap",
    "yonly",
]

CONFIGS: dict[str, tuple[str, str, str | None]] = {
    "a0-utc": ("false", "UTC", None),
    "a0-la": ("false", "America/Los_Angeles", None),
    "a1-utc": ("true", "UTC", None),
    "a1-la": ("true", "America/Los_Angeles", None),
    "a1-utc-ntz": ("true", "UTC", "TIMESTAMP_NTZ"),
    "a1-la-ntz": ("true", "America/Los_Angeles", "TIMESTAMP_NTZ"),
}

FIELDS = [
    "YEAR",
    "YEAROFWEEK",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "DAYOFWEEK",
    "DAYOFWEEK_ISO",
    "DOY",
    "HOUR",
    "MINUTE",
    "SECOND",
]

TRUNCS = [
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MILLISECOND",
    "MICROSECOND",
]

NAMED = [
    "hour",
    "minute",
    "second",
    "year",
    "quarter",
    "month",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "weekofyear",
    "weekday",
]

ALIASES = [
    ("extract", "H"),
    ("extract", "HRS"),
    ("date_part", "min"),
    ("date_part", "S"),
    ("extract", "SECONDS"),
]


def named(name: str) -> Build:
    return lambda f, c: getattr(f, name)(c)


def extract(field: str) -> Build:
    return lambda f, c: f.extract(f.lit(field), c)


def date_part(field: str) -> Build:
    return lambda f, c: f.date_part(f.lit(field), c)


def trunc(unit: str) -> Build:
    return lambda f, c: f.date_trunc(unit, c).cast("string")


def cast_field(name: str) -> Build:
    return lambda f, c: getattr(f, name)(c.cast("timestamp"))


def build_funcs() -> list[tuple[str, str, Build]]:
    out: list[tuple[str, str, Build]] = [(n, n + "({c})", named(n)) for n in NAMED]
    for field in FIELDS:
        out.append(("extract_" + field, "extract(" + field + " FROM {c})", extract(field)))
    for field in FIELDS:
        out.append(("date_part_" + field, "date_part('" + field + "', {c})", date_part(field)))
    for kind, field in ALIASES:
        if kind == "extract":
            out.append(("extract_" + field, "extract(" + field + " FROM {c})", extract(field)))
        else:
            out.append(("date_part_" + field, "date_part('" + field + "', {c})", date_part(field)))
    for unit in TRUNCS:
        sql = "CAST(date_trunc('" + unit + "', {c}) AS STRING)"
        out.append(("date_trunc_" + unit, sql, trunc(unit)))
    out.append(("unix_timestamp", "unix_timestamp({c})", named("unix_timestamp")))
    out.append(("date_format_HH", "date_format({c}, 'HH')", lambda f, c: f.date_format(c, "HH")))
    full = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    out.append(
        (
            "date_format_full",
            "date_format({c}, '" + full + "')",
            lambda f, c: f.date_format(c, full),
        )
    )
    for name in ("hour", "minute", "second"):
        out.append(("cast_" + name, name + "(CAST({c} AS TIMESTAMP))", cast_field(name)))
    out.append(
        (
            "cast_ts",
            "CAST(CAST({c} AS TIMESTAMP) AS STRING)",
            lambda f, c: c.cast("timestamp").cast("string"),
        )
    )
    out.append(
        (
            "cast_date",
            "CAST(CAST({c} AS DATE) AS STRING)",
            lambda f, c: c.cast("date").cast("string"),
        )
    )
    return out


FUNCS = build_funcs()

G_FUNCS: dict[str, tuple[str, Build]] = {
    "hour": ("3", lambda f: f.lit(3)),
    "minute": ("4", lambda f: f.lit(4)),
    "second": ("5", lambda f: f.lit(5)),
    "year": ("2024", lambda f: f.lit(2024)),
    "dayofmonth": ("1", lambda f: f.lit(1)),
    "extract_HOUR": ("3", lambda f: f.lit(3)),
    "date_part_MINUTE": ("4", lambda f: f.lit(4)),
    "extract_SECOND": ("5", lambda f: f.lit(5)),
    "date_trunc_HOUR": ("'2024-01-01 03:00:00'", lambda f: f.lit("2024-01-01 03:00:00")),
    "date_format_HH": ("'03'", lambda f: f.lit("03")),
    "unix_timestamp": (
        "unix_timestamp('2024-01-01 03:04:05')",
        lambda f: f.unix_timestamp(f.lit("2024-01-01 03:04:05")),
    ),
}

G_DOORS = ["where_sql", "group_sql", "order_sql", "where_f", "group_f", "order_f"]
P_DOORS = ["sql", "fcol", "sexpr", "fexpr"]
INPUTS = ["L", "I", "P", "V"]


def sql_text(text: str | None) -> str:
    if text is None:
        return "CAST(NULL AS STRING)"
    return "'" + text + "'"


def func_map() -> dict[str, tuple[str, Build]]:
    return {fid: (sql, build) for fid, sql, build in FUNCS}
