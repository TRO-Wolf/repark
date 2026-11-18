from __future__ import annotations

from collections.abc import Callable
from typing import Any

ANSI = "spark.sql.ansi.enabled"
TZ = "spark.sql.session.timeZone"
DEC = "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate"
LA = "America/Los_Angeles"
COUNT = "SELECT count(*) AS m FROM {V}"
VALUES = "SELECT m FROM {V}"

FIXTURES = {
    "swt": "SELECT TIMESTAMP '2024-01-01 03:00:00' AS ts, DATE '2024-01-01' AS d, "
    "CAST(1 AS INT) AS n UNION ALL "
    "SELECT TIMESTAMP '2024-01-01 12:00:00', DATE '2024-01-02', CAST(2 AS INT)",
    "swa": "SELECT '1' AS s, CAST(1 AS INT) AS n, DATE '2024-01-01' AS d "
    "UNION ALL SELECT 'x', CAST(2 AS INT), DATE '2024-01-02'",
}

ORDERS = {
    "zone": (TZ, [("UTC", LA), (LA, "UTC")]),
    "ansi": (ANSI, [("true", "false"), ("false", "true")]),
    "dec": (DEC, [("false", "true"), ("true", "false")]),
}
SHORT = {"UTC": "utc", LA: "la", "true": "on", "false": "off"}

Frame = Callable[[Any], Any]


def where(table: str, text: str) -> Frame:
    return lambda s: s.table(table).filter(text)


def select(table: str, text: str) -> Frame:
    return lambda s: s.table(table).selectExpr(text)


def column_ge(s: Any) -> Any:
    frame = s.table("swt")
    return frame.filter(frame["ts"] >= "2024-01-01 00:00:00")


def decimal_union(s: Any) -> Any:
    frame = s.table("fdec")
    return frame.selectExpr("x0 AS m").union(frame.selectExpr("x10 AS m"))


SHAPES: dict[str, tuple[str, Frame, str]] = {
    "ts_ge": ("zone", where("swt", "ts >= '2024-01-01 00:00:00'"), COUNT),
    "ts_ge_column": ("zone", column_ge, COUNT),
    "ts_eq": ("zone", where("swt", "ts = '2024-01-01 03:00:00'"), COUNT),
    "ts_in": ("zone", where("swt", "ts IN ('2024-01-01 03:00:00', '2024-01-01 12:00:00')"), COUNT),
    "ts_between": (
        "zone",
        where("swt", "ts BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 13:00:00'"),
        COUNT,
    ),
    "date_ge": ("zone", where("swt", "d >= '2024-01-02'"), COUNT),
    "date_eq": ("zone", where("swt", "d = '2024-01-01'"), COUNT),
    "date_in": ("zone", where("swt", "d IN ('2024-01-01', '2024-01-05')"), COUNT),
    "date_between": ("zone", where("swt", "d BETWEEN '2023-12-31' AND '2024-01-01'"), COUNT),
    "date_ts": ("zone", where("swt", "d < ts"), COUNT),
    "date_add": ("zone", select("swt", "date_add(ts, 0) AS m"), VALUES),
    "date_add_where": ("zone", where("swt", "date_add(ts, 0) = DATE '2024-01-01'"), COUNT),
    "hour_where": ("zone", where("swt", "hour(ts) = 3"), COUNT),
    "hour_text": ("zone", select("swt", "hour('2024-01-01T03:00:00Z') AS m"), VALUES),
    "case_zone": (
        "zone",
        select(
            "swt", "CAST(CASE WHEN n = 1 THEN ts ELSE '2024-01-01 00:00:00' END AS STRING) AS m"
        ),
        VALUES,
    ),
    "sql_frame": (
        "zone",
        lambda s: s.sql("SELECT ts FROM swt WHERE ts >= '2024-01-01 00:00:00'"),
        COUNT,
    ),
    "str_eq_int": ("ansi", where("swa", "s = 1"), COUNT),
    "str_plus": ("ansi", select("swa", "s + n AS m"), VALUES),
    "lit_plus": ("ansi", select("swa", "'1' + n AS m"), VALUES),
    "str_cast_where": ("ansi", where("swa", "CAST(s AS INT) > 0"), COUNT),
    "date_bad": ("ansi", where("swa", "d = 'x'"), COUNT),
    "case_mixed": ("ansi", select("swa", "CASE WHEN n = 1 THEN s ELSE n END AS m"), VALUES),
    "add_date": ("ansi", select("swa", "date_add(d, '1') AS m"), VALUES),
    "dec_eq": ("dec", where("fdec", "x0 = x10"), COUNT),
    "dec_case": ("dec", select("fdec", "CASE WHEN k = 0 THEN x0 ELSE x10 END AS m"), VALUES),
    "dec_union": ("dec", decimal_union, VALUES),
}
TWO_LEVELS = {
    "two_levels_zone": ("zone", "ts_ge"),
    "two_levels_ansi": ("ansi", "str_eq_int"),
    "two_levels_dec": ("dec", "dec_eq"),
}


class Sweep:
    def __init__(self, cells: Any) -> None:
        self.c = cells

    def fixtures(self) -> None:
        for name, text in FIXTURES.items():
            path = self.c.root + "/fx-" + name
            self.c.s.sql(text).write.mode("overwrite").parquet(path)
            self.c.s.read.parquet(path).createOrReplaceTempView(name)

    def framed(self, frame: Frame) -> str:
        name = self.c.name("d")
        frame(self.c.s).createOrReplaceTempView(name)
        return name

    def two_levels(self, inner: Frame) -> str:
        first = self.framed(inner)
        return self.framed(lambda s: s.table(first).select("*"))

    def cell(
        self, key: str, first: str, second: str, read: str, build: Callable[[], str], reader: str
    ) -> dict:
        self.c.set(key, first)
        frame = build()
        self.c.set(key, second)
        name = self.c.view("SELECT * FROM " + frame)
        self.c.set(key, read)
        return self.c.read(reader.format(V=name))

    def run(self) -> None:
        self.fixtures()
        shapes = {
            name: (family, frame, reader, False) for name, (family, frame, reader) in SHAPES.items()
        }
        for name, (family, inner) in TWO_LEVELS.items():
            _, frame, reader = SHAPES[inner]
            shapes[name] = (family, frame, reader, True)
        for name, (family, frame, reader, nested) in shapes.items():
            key, orders = ORDERS[family]
            for first, second in orders:
                tag = SHORT.get(first, first) + ">" + SHORT.get(second, second)
                for read in (first, second):
                    build = (
                        (lambda f=frame: self.two_levels(f))
                        if nested
                        else (lambda f=frame: self.framed(f))
                    )
                    self.c.put(
                        "sweep/"
                        + family
                        + "/"
                        + name
                        + "/"
                        + tag
                        + "/read_"
                        + SHORT.get(read, read),
                        lambda k=key, a=first, b=second, r=read, bd=build, q=reader: self.cell(
                            k, a, b, r, bd, q
                        ),
                    )
