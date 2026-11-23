from __future__ import annotations

import argparse
import datetime
import decimal
import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

FIXTURE = Path(__file__).resolve().parent / "extract_field_aliases_1_spark_oracle.json"
ZONES: tuple[str, ...] = ("UTC", "America/New_York")
ANSI_MODES: tuple[str, ...] = ("false", "true")
CARD_FIELDS: tuple[str, ...] = ("YEAROFWEEK", "DAYOFWEEK", "DAYOFWEEK_ISO", "H", "HRS")
FIELDS: tuple[str, ...] = (
    "YEAR",
    "Y",
    "YEARS",
    "YR",
    "YRS",
    "YEAROFWEEK",
    "QUARTER",
    "QTR",
    "MONTH",
    "MON",
    "MONS",
    "MONTHS",
    "WEEK",
    "W",
    "WEEKS",
    "DAY",
    "D",
    "DAYS",
    "DAYOFWEEK",
    "DOW",
    "DAYOFWEEK_ISO",
    "DOW_ISO",
    "DOY",
    "HOUR",
    "H",
    "HOURS",
    "HR",
    "HRS",
    "MINUTE",
    "M",
    "MIN",
    "MINS",
    "MINUTES",
    "SECOND",
    "S",
    "SEC",
    "SECONDS",
    "SECS",
)
CAST_FIELDS: tuple[str, ...] = (*CARD_FIELDS, "DOW", "DOW_ISO", "WEEK")
KINDS: dict[str, str] = {"date": "DATE", "ts": "TIMESTAMP", "ntz": "TIMESTAMP_NTZ"}
VALUES: tuple[str | None, ...] = (
    "1969-12-31 13:45:30",
    "1950-06-15 04:05:06",
    "1582-10-15 01:02:03",
    "1900-02-28 23:59:59",
    "2019-12-30 18:00:00",
    "2020-12-31 13:45:30",
    "2021-01-03 07:08:09",
    "2021-01-04 00:00:00",
    "2024-02-29 12:00:00",
    "2025-12-31 23:00:00",
    "2026-01-01 00:30:00",
    "2027-01-03 09:10:11",
    None,
)
FORMS: dict[str, str] = {
    "extract": "extract({f} FROM x)",
    "date_part": "date_part('{f}', x)",
    "smallint": "CAST(extract({f} FROM x) AS SMALLINT)",
}
NTZ_FLOOR_YEAR = 1677
OBSERVED: dict[str, str] = {
    "unknown-field": "SELECT extract(FOO FROM DATE '2020-12-31') AS r",
    "unknown-part": "SELECT date_part('FOO', DATE '2020-12-31') AS r",
    "ntz-1582": "SELECT CAST('1582-10-15 01:02:03' AS TIMESTAMP_NTZ) AS r",
    "interval-hour": "SELECT extract(HOUR FROM INTERVAL '1 02:03:04' DAY TO SECOND) AS r",
    "interval-h": "SELECT extract(H FROM INTERVAL '1 02:03:04' DAY TO SECOND) AS r",
    "string-extract": "SELECT extract(YEAROFWEEK FROM '2020-12-31') AS r",
    "string-part": "SELECT date_part('YEAROFWEEK', '2020-12-31') AS r",
    "epoch": "SELECT extract(EPOCH FROM TIMESTAMP '2020-12-31 00:00:00') AS r",
    "isodow": "SELECT extract(ISODOW FROM DATE '2020-12-31') AS r",
    "unaliased": "SELECT extract(YEAROFWEEK FROM DATE '2020-12-31')",
    "unaliased-year": "SELECT extract(YEAR FROM DATE '2020-12-31')",
}
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?)\]")


def mode_key(zone: str, ansi: str) -> str:
    return f"{zone}|{ansi}"


def view_name(kind: str) -> str:
    return f"extract_cells_{kind}"


def source_sql(kind: str) -> str:
    rows = []
    for index, value in enumerate(VALUES):
        if kind == "ntz" and value is not None and int(value[:4]) < NTZ_FLOOR_YEAR:
            continue
        if value is None:
            rows.append(f"({index}, CAST(NULL AS STRING), CAST(NULL AS STRING))")
        else:
            rows.append(f"({index}, '{value}', '{value[:10]}')")
    column = "dv" if kind == "date" else "v"
    return (
        f"SELECT CAST(id AS INT) AS id, CAST({column} AS {KINDS[kind]}) AS x"
        f" FROM (VALUES {', '.join(rows)}) AS t(id, v, dv)"
    )


def cells() -> list[tuple[str, str, str]]:
    found = []
    for kind in KINDS:
        for field in FIELDS:
            for form in FORMS:
                if form == "smallint" and field not in CAST_FIELDS:
                    continue
                found.append((kind, field, form))
    return found


def cell_key(kind: str, field: str, form: str, seat: str) -> str:
    return "|".join((kind, field, form, seat))


def plain(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return "dec:" + str(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return "time:" + value.isoformat()
    return value


def outcome(error: BaseException) -> dict[str, str]:
    text = str(error)
    found = _CONDITION.search(text)
    first = next((line for line in text.splitlines() if line.strip()), "")
    return {
        "error": found.group(1) if found else "NONE:" + type(error).__name__,
        "message": first.strip()[:300],
    }


def rows_of(frame: Any) -> dict[str, Any]:
    collected = frame.orderBy("id").collect()
    typed_rows = "t" in frame.columns
    return {
        "values": [plain(row["r"]) for row in collected],
        "types": sorted({row["t"] for row in collected}) if typed_rows else [],
    }


def observed_answers(session: Any) -> dict[str, dict[str, Any]]:
    answers: dict[str, dict[str, Any]] = {}
    for name, text in OBSERVED.items():
        try:
            frame = session.sql(text)
            answers[name] = {
                "columns": list(frame.columns),
                "values": [plain(value) for value in frame.collect()[0]],
            }
        except Exception as error:
            answers[name] = outcome(error)
    return answers


def guarded(build: Callable[[], Any]) -> dict[str, Any]:
    try:
        return rows_of(build())
    except Exception as error:
        return outcome(error)


def typed(functions: Any, form: str, field: str, column: Any) -> Any:
    if form == "extract":
        return functions.extract(functions.lit(field), column)
    if form == "date_part":
        return functions.date_part(functions.lit(field), column)
    return functions.extract(functions.lit(field), column).cast("smallint")


def seed(session: Any) -> None:
    for kind in KINDS:
        session.sql(source_sql(kind)).createOrReplaceTempView(view_name(kind))


def seat_answers(session: Any, functions: Any) -> dict[str, dict[str, Any]]:
    answers: dict[str, dict[str, Any]] = {}
    for kind, field, form in cells():
        text = FORMS[form].format(f=field)
        view = view_name(kind)
        answers[cell_key(kind, field, form, "sql")] = guarded(
            lambda text=text, view=view: session.sql(
                f"SELECT id, {text} AS r, typeof({text}) AS t FROM {view}"
            )
        )
        answers[cell_key(kind, field, form, "expr")] = guarded(
            lambda text=text, view=view: session.table(view).select(
                "id", functions.expr(text).alias("r"), functions.expr(f"typeof({text})").alias("t")
            )
        )
        answers[cell_key(kind, field, form, "select-expr")] = guarded(
            lambda text=text, view=view: session.table(view).selectExpr(
                "id", f"{text} AS r", f"typeof({text}) AS t"
            )
        )
        answers[cell_key(kind, field, form, "typed")] = guarded(
            lambda kind=kind, field=field, form=form, view=view: session.table(view).select(
                "id",
                typed(functions, form, field, functions.col("x")).alias("r"),
                functions.typeof(typed(functions, form, field, functions.col("x"))).alias("t"),
            )
        )
    return answers


def native_answers(run: Callable[[str], Any]) -> dict[str, dict[str, Any]]:
    answers: dict[str, dict[str, Any]] = {}
    for kind, field, form in cells():
        text = FORMS[form].format(f=field)
        source = source_sql(kind)
        answers[cell_key(kind, field, form, "native")] = guarded(
            lambda text=text, source=source: run(f"SELECT id, {text} AS r FROM ({source}) AS s")
        )
    return answers


def record(path: Path) -> None:
    from pyspark.sql import SparkSession, functions

    session = (
        SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    modes: dict[str, Any] = {}
    for zone in ZONES:
        session.conf.set("spark.sql.session.timeZone", zone)
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.ansi.enabled", ansi)
            seed(session)
            answers = seat_answers(session, functions)
            answers.update(
                {f"{name}|observed": cell for name, cell in observed_answers(session).items()}
            )
            modes[mode_key(zone, ansi)] = answers
    document = {"spark": session.version, "recorded": "2026-11-23", "modes": modes}
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=FIXTURE)
    arguments = parser.parse_args(argv)
    record(arguments.out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
