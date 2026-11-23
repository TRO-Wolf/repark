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

FIXTURE = Path(__file__).resolve().parent / "sql_explode_1_spark_oracle.json"
ZONES: tuple[str, ...] = ("UTC", "America/New_York")
ANSI_MODES: tuple[str, ...] = ("false", "true")
VIEW = "explode_cells"
FRAME_SCHEMA = "id int, tag string, arr array<int>, nest array<array<int>>, m map<string,int>"
FRAME_ROWS: tuple[tuple[Any, ...], ...] = (
    (1, "a", [1, 2], [[1, 2], [3]], {"k1": 1, "k2": 2}),
    (2, "b", [], [], {}),
    (3, "c", None, None, None),
    (4, "d", [None, 5], [None, [None]], {"k3": None}),
)
EXPR_SOURCES: frozenset[str] = frozenset({"none", "view", "table"})


def _sequence(start: str, stop: str) -> str:
    return f"explode(sequence(DATE '{start}', DATE '{stop}', INTERVAL 1 DAY)) AS d"


CELLS: dict[str, tuple[str, tuple[str, ...]]] = {
    "bare": ("none", ("explode(array(1, 2))",)),
    "alias": ("none", ("explode(array(1, 2)) AS x",)),
    "outer": ("none", ("explode_outer(array(1, 2)) AS x",)),
    "outer-bare": ("none", ("explode_outer(array(3, 4))",)),
    "upper-case": ("none", ("EXPLODE(array(1, 2)) AS x",)),
    "empty": ("none", ("explode(CAST(array() AS ARRAY<INT>)) AS x",)),
    "empty-outer": ("none", ("explode_outer(CAST(array() AS ARRAY<INT>)) AS x",)),
    "null": ("none", ("explode(CAST(NULL AS ARRAY<INT>)) AS x",)),
    "null-outer": ("none", ("explode_outer(CAST(NULL AS ARRAY<INT>)) AS x",)),
    "null-elements": ("none", ("explode(array(1, NULL, 3)) AS x",)),
    "nested": ("none", ("explode(array(array(1, 2), array(3))) AS x",)),
    "strings": ("none", ("explode(array('x', NULL, 'y')) AS s",)),
    "beside": ("none", ("7 AS k", "explode(array('x', 'y')) AS s", "'z' AS t")),
    "seq-leap": ("none", (_sequence("2024-02-27", "2024-03-02"),)),
    "seq-year-end": ("none", (_sequence("2019-12-29", "2020-01-03"),)),
    "seq-dst-spring": ("none", (_sequence("2025-03-08", "2025-03-11"),)),
    "seq-dst-fall": ("none", (_sequence("2025-11-01", "2025-11-04"),)),
    "seq-week53-2015": ("none", (_sequence("2015-12-26", "2016-01-04"),)),
    "seq-week53-2020": ("none", (_sequence("2020-12-26", "2021-01-04"),)),
    "seq-week53-2026": ("none", (_sequence("2026-12-26", "2027-01-04"),)),
    "seq-no-step": ("none", ("explode(sequence(DATE '2024-02-28', DATE '2024-03-01')) AS d",)),
    "seq-pre-1970": ("none", (_sequence("1969-12-30", "1970-01-02"),)),
    "view": ("view", ("id", "explode(arr) AS x")),
    "view-bare": ("view", ("id", "explode(arr)")),
    "view-outer": ("view", ("id", "explode_outer(arr) AS x", "tag")),
    "view-nested": ("view", ("id", "explode(nest) AS inner_list")),
    "view-nested-outer": ("view", ("id", "explode_outer(nest) AS inner_list")),
    "table": ("table", ("id", "explode(arr) AS x")),
    "table-outer": ("table", ("tag", "explode_outer(arr) AS x")),
    "cte": ("cte", ("id", "explode(arr) AS x")),
    "cte-outer": ("cte", ("id", "explode_outer(arr) AS x")),
    "subquery": ("subquery", ("id", "explode(arr) AS x")),
    "subquery-outer": ("subquery", ("id", "explode_outer(arr) AS x")),
    "projected": ("projected", ("x + 1 AS y",)),
    "projected-dates": ("projected-dates", ("d", "dayofmonth(d) AS dom")),
    "map": ("none", ("explode(map('a', 1, 'b', 2))",)),
    "map-outer-null": ("none", ("explode_outer(CAST(NULL AS MAP<STRING,INT>))",)),
    "map-view": ("view", ("id", "explode(m)")),
    "map-view-outer": ("view", ("id", "explode_outer(m)")),
}
OBSERVED: dict[str, str] = {
    "lateral-view": f"SELECT id, x FROM {VIEW} LATERAL VIEW explode(arr) g AS x",
    "lateral-view-outer": f"SELECT id, x FROM {VIEW} LATERAL VIEW OUTER explode(arr) g AS x",
    "map-two-aliases": "SELECT explode(map('a', 1)) AS (k, v)",
    "map-one-alias": "SELECT explode(map('a', 1)) AS k",
    "posexplode": "SELECT posexplode(array(10, 20))",
    "two-generators": "SELECT explode(array(1)), explode(array(2))",
    "nested-generator": "SELECT explode(explode(array(array(1))))",
    "in-expression": "SELECT explode(array(1, 2)) + 1",
    "non-array": "SELECT explode(1)",
    "ts-sequence-dst": (
        "SELECT CAST(t AS STRING) AS t FROM (SELECT explode(sequence(TIMESTAMP "
        "'2025-03-08 12:00:00', TIMESTAMP '2025-03-10 12:00:00', INTERVAL 1 DAY)) AS t) AS g"
    ),
}
WRITES: dict[str, tuple[str | None, str]] = {
    "ctas": (None, f"CREATE TABLE {{t}} AS SELECT id, explode(arr) AS x FROM {VIEW}"),
    "insert-outer": (
        "CREATE TABLE {t} (id INT, x INT)",
        f"INSERT INTO {{t}} SELECT id, explode_outer(arr) AS x FROM {VIEW}",
    ),
    "ctas-calendar": (
        None,
        "CREATE TABLE {t} AS SELECT d, CAST(year(d) AS SMALLINT) AS y,"
        " CAST(extract(YEAROFWEEK FROM d) AS SMALLINT) AS yow,"
        " CAST(extract(DAYOFWEEK FROM d) AS SMALLINT) AS dow, weekofyear(d) AS woy"
        " FROM (SELECT " + _sequence("2020-12-26", "2021-01-04") + ") AS g",
    ),
    "insert-calendar": (
        "CREATE TABLE {t} (d DATE, yow SMALLINT, dow_iso SMALLINT)",
        "INSERT INTO {t} SELECT d, CAST(extract(YEAROFWEEK FROM d) AS SMALLINT),"
        " CAST(extract(DAYOFWEEK_ISO FROM d) AS SMALLINT)"
        " FROM (SELECT " + _sequence("2026-12-26", "2027-01-04") + ") AS g",
    ),
}
_CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?)\]")


def mode_key(zone: str, ansi: str) -> str:
    return f"{zone}|{ansi}"


def table_name(catalog: str, name: str, zone: str, ansi: str) -> str:
    tag = re.sub(r"[^a-z0-9]+", "_", f"{name}_{zone}_{ansi}".lower())
    return f"{catalog}.ns.{tag}"


def source_sql(source: str, items: tuple[str, ...], table: str) -> str:
    listed = ", ".join(items)
    if source == "none":
        return f"SELECT {listed}"
    if source == "view":
        return f"SELECT {listed} FROM {VIEW}"
    if source == "table":
        return f"SELECT {listed} FROM {table}"
    if source == "cte":
        return f"WITH c AS (SELECT * FROM {VIEW}) SELECT {listed} FROM c"
    if source == "subquery":
        return f"SELECT {listed} FROM (SELECT * FROM {VIEW} WHERE id < 4) AS s"
    if source == "projected":
        return f"SELECT {listed} FROM (SELECT explode(arr) AS x FROM {VIEW}) AS g"
    return f"SELECT {listed} FROM (SELECT {_sequence('2024-02-27', '2024-03-02')}) AS g"


def plain(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return "ts:" + value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return "dec:" + str(value)
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "asDict"):
        return plain(list(value))
    return value


def outcome(error: BaseException) -> dict[str, str]:
    text = str(error)
    found = _CONDITION.search(text)
    first = next((line for line in text.splitlines() if line.strip()), "")
    return {
        "error": found.group(1) if found else "NONE:" + type(error).__name__,
        "message": first.strip()[:300],
    }


def frame_answer(frame: Any) -> dict[str, Any]:
    fields = [
        [field.name, field.dataType.simpleString(), bool(field.nullable)]
        for field in frame.schema.fields
    ]
    rows = [plain(list(row)) for row in frame.collect()]
    return {"columns": list(frame.columns), "schema": fields, "rows": rows}


def guarded(build: Callable[[], Any]) -> dict[str, Any]:
    try:
        return frame_answer(build())
    except Exception as error:
        return outcome(error)


def expr_column(functions: Any, item: str) -> Any:
    body, marker, name = item.rpartition(" AS ")
    if not marker:
        return functions.expr(item)
    return functions.expr(body).alias(name)


def typed_frames(session: Any, functions: Any) -> dict[str, Callable[[], Any]]:
    return {
        "view": lambda: session.table(VIEW).select("id", functions.explode("arr").alias("x")),
        "view-outer": lambda: session.table(VIEW).select(
            "id", functions.explode_outer("arr").alias("x"), "tag"
        ),
        "view-nested": lambda: session.table(VIEW).select("id", functions.explode("nest")),
        "map-view": lambda: session.table(VIEW).select("id", functions.explode("m")),
        "map-view-outer": lambda: session.table(VIEW).select("id", functions.explode_outer("m")),
        "bare": lambda: session.range(1).select(
            functions.explode(functions.array(functions.lit(1), functions.lit(2)))
        ),
    }


def seed(session: Any, catalog: str, zone: str, ansi: str) -> str:
    frame = session.createDataFrame(list(FRAME_ROWS), FRAME_SCHEMA)
    frame.createOrReplaceTempView(VIEW)
    table = table_name(catalog, "src", zone, ansi)
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(f"CREATE TABLE {table} AS SELECT * FROM {VIEW}")
    return table


def seat_answers(session: Any, functions: Any, table: str) -> dict[str, dict[str, Any]]:
    answers: dict[str, dict[str, Any]] = {}
    for name, (source, items) in CELLS.items():
        text = source_sql(source, items, table)
        answers[f"{name}|sql"] = guarded(lambda text=text: session.sql(text))
        if source not in EXPR_SOURCES:
            continue
        base = {"none": lambda: session.range(1), "view": lambda: session.table(VIEW)}.get(
            source, lambda: session.table(table)
        )
        answers[f"{name}|expr"] = guarded(
            lambda base=base, items=items: base().select(
                *[expr_column(functions, i) for i in items]
            )
        )
        answers[f"{name}|select-expr"] = guarded(
            lambda base=base, items=items: base().selectExpr(*items)
        )
    for name, text in OBSERVED.items():
        answers[f"{name}|sql"] = guarded(lambda text=text: session.sql(text))
    for name, build in typed_frames(session, functions).items():
        answers[f"{name}|typed"] = guarded(build)
    answers["inline-alias|expr"] = guarded(
        lambda: session.range(1).select(functions.expr("explode(array(1, 2)) AS x"))
    )
    return answers


def native_answers(run: Callable[[str], Any]) -> dict[str, dict[str, Any]]:
    answers: dict[str, dict[str, Any]] = {}
    for name, (source, items) in CELLS.items():
        if source != "none":
            continue
        text = source_sql(source, items, "")
        answers[f"{name}|native"] = guarded(lambda text=text: run(text))
    return answers


def write_answers(
    session: Any, catalog: str, zone: str, ansi: str, reopen: Callable[[], Any]
) -> dict[str, dict[str, Any]]:
    written: dict[str, str] = {}
    answers: dict[str, dict[str, Any]] = {}
    for name, (create, statement) in WRITES.items():
        table = table_name(catalog, name, zone, ansi)
        try:
            session.sql(f"DROP TABLE IF EXISTS {table}")
            if create is not None:
                session.sql(create.format(t=table)).collect()
            session.sql(statement.format(t=table)).collect()
            written[name] = table
        except Exception as error:
            answers[f"{name}|write"] = outcome(error)
    reader = reopen()
    for name, table in written.items():
        answers[f"{name}|write"] = guarded(lambda table=table: reader.sql(f"SELECT * FROM {table}"))
    return answers


def record(path: Path, warehouse: Path, iceberg_jar: str) -> None:
    from pyspark.sql import SparkSession, functions

    catalog = "loc"
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars", iceberg_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    modes: dict[str, Any] = {}
    for zone in ZONES:
        session.conf.set("spark.sql.session.timeZone", zone)
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.ansi.enabled", ansi)
            table = seed(session, catalog, zone, ansi)
            answers = seat_answers(session, functions, table)
            answers.update(write_answers(session, catalog, zone, ansi, lambda: session))
            modes[mode_key(zone, ansi)] = answers
    document = {"spark": session.version, "recorded": "2026-11-23", "modes": modes}
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=FIXTURE)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--iceberg-jar", required=True)
    arguments = parser.parse_args(argv)
    record(arguments.out, arguments.warehouse, arguments.iceberg_jar)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
