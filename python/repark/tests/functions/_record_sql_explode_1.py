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
    "seq-date-outer": (
        "none",
        ("explode_outer(sequence(DATE '2024-02-28', DATE '2024-03-01', INTERVAL 1 DAY)) AS d",),
    ),
    "seq-int-outer": ("none", ("explode_outer(sequence(1, 3)) AS n",)),
    "seq-ts-outer": ("projected-ts-outer", ("CAST(t AS STRING) AS t",)),
    "split-outer": ("none", ("explode_outer(split('a,b', ',')) AS s",)),
    "split-null-outer": ("none", ("explode_outer(split(CAST(NULL AS STRING), ',')) AS s",)),
    "posexplode-outer-seq": ("none", ("posexplode_outer(sequence(1, 3))",)),
    "posexplode-outer-split-null": (
        "none",
        ("posexplode_outer(split(CAST(NULL AS STRING), ','))",),
    ),
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
    "subq-star": ("subq-unaliased", ("*",)),
    "subq-star-lit": ("subq-unaliased-lit", ("*",)),
    "subq-col": ("subq-unaliased", ("col + 1 AS y",)),
    "subq-where": ("subq-unaliased-where", ("col",)),
    "subq-groupby": ("subq-unaliased-group", ("col", "COUNT(*) AS cnt")),
    "orderby-col": ("table-orderby-col", ("id", "explode(arr)")),
    "alias-explode-custom": ("table-id1", ("explode(arr) AS `explode(custom)`",)),
    "alias-explode-custom-orderby": ("table-id1-orderby", ("explode(arr) AS `explode(custom)`",)),
    "alias-explode-arr": ("table-id1", ("explode(arr) AS `explode(arr)`",)),
    "alias-explode-outer-custom": ("table-id1", ("explode_outer(arr) AS `explode_outer(custom)`",)),
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
    "map-alias-col": "SELECT explode(map('a', 1)) AS col",
    "posexplode": "SELECT posexplode(array(10, 20))",
    "posexplode-alias-custom": f"SELECT posexplode(arr) AS `posexplode(custom)` FROM {VIEW}",
    "two-generators": "SELECT explode(array(1)), explode(array(2))",
    "nested-generator": "SELECT explode(explode(array(array(1))))",
    "in-expression": "SELECT explode(array(1, 2)) + 1",
    "non-array": "SELECT explode(1)",
    "ts-sequence-dst": (
        "SELECT CAST(t AS STRING) AS t FROM (SELECT explode(sequence(TIMESTAMP "
        "'2025-03-08 12:00:00', TIMESTAMP '2025-03-10 12:00:00', INTERVAL 1 DAY)) AS t) AS g"
    ),
}
_COL_SOURCE = "(SELECT tag AS col, m FROM {t}) AS s"
NAMED: dict[str, str] = {
    "r1-posexplode-case": "SELECT id, posexplode(CASE WHEN id = 1 THEN arr ELSE NULL END) FROM {t}",
    "r1-posexplode-outer-case": (
        "SELECT id, posexplode_outer(CASE WHEN id = 2 THEN CAST(NULL AS ARRAY<INT>) ELSE arr END) "
        "FROM {t}"
    ),
    "r1-posexplode-filter": "SELECT id, posexplode(filter(arr, x -> x > 1)) FROM {t}",
    "r1-posexplode-transform": "SELECT id, posexplode(transform(arr, x -> x + 1)) FROM {t}",
    "r1-posexplode-map-case": "SELECT id, posexplode(CASE WHEN id = 1 THEN m END) FROM {t}",
    "r1-inline-case": (
        "SELECT id, inline(CASE WHEN id = 1 THEN array(named_struct('a', id, 'b', tag)) "
        "ELSE NULL END) FROM {t}"
    ),
    "r1-inline-outer-case": (
        "SELECT id, inline_outer(CASE WHEN id = 1 THEN array(named_struct('a', id, 'b', tag)) "
        "ELSE NULL END) FROM {t}"
    ),
    "r1-inline-transform": (
        "SELECT inline(transform(array(1, 2), x -> named_struct('a', x, 'b', 'ok')))"
    ),
    "r1-json-tuple-case": (
        'SELECT id, json_tuple(CASE WHEN id = 1 THEN \'{{"a":1,"b":"x"}}\' ELSE NULL END, '
        "'a', 'b') FROM {t}"
    ),
    "r1-explode-case-alias": (
        "SELECT explode(CASE WHEN id = 1 THEN arr END) AS `explode(custom)` FROM {t}"
    ),
    "r1-posexplode-call-alias": "SELECT posexplode(arr) AS `posexplode(arr)` FROM {t}",
    "r2-col-explode-map": "SELECT col, explode(m) FROM " + _COL_SOURCE,
    "r2-col-explode-outer-map": "SELECT col, explode_outer(m) FROM " + _COL_SOURCE,
    "r2-explode-map-col-after": "SELECT explode(m), col FROM " + _COL_SOURCE,
    "r2-qualified-col-explode-map": "SELECT s.col, explode(s.m) FROM " + _COL_SOURCE,
    "r2-alias-col-explode-map": "SELECT tag AS col, explode(m) FROM {t}",
    "r4-array-repeat-explode-outer": "SELECT explode_outer(array_repeat(sequence(1, 2), 0)) AS a",
    "r4-array-repeat-alone": "SELECT array_repeat(array(1, 2), 2) AS a",
}
WRITES: dict[str, tuple[str | None, str]] = {
    "ctas": (None, f"CREATE TABLE {{t}} AS SELECT id, explode(arr) AS x FROM {VIEW}"),
    "ctas-unaliased": (None, f"CREATE TABLE {{t}} AS SELECT explode(arr) FROM {VIEW}"),
    "insert-outer": (
        "CREATE TABLE {t} (id INT, x INT)",
        f"INSERT INTO {{t}} SELECT id, explode_outer(arr) AS x FROM {VIEW}",
    ),
    "insert-unaliased": (
        "CREATE TABLE {t} (x INT)",
        f"INSERT INTO {{t}} SELECT explode(arr) FROM {VIEW}",
    ),
    "insert-overwrite-unaliased": (
        "CREATE TABLE {t} (x INT)",
        f"INSERT OVERWRITE {{t}} SELECT explode(arr) FROM {VIEW}",
    ),
    "insert-seq-outer": (
        "CREATE TABLE {t} (d DATE)",
        "INSERT INTO {t} SELECT explode_outer("
        "sequence(DATE '2024-02-28', DATE '2024-03-01', INTERVAL 1 DAY)) AS d",
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
_TRAILING_ALIAS = re.compile(r"^(.*)\s+AS\s+(?:`([^`]+)`|([A-Za-z_][A-Za-z0-9_]*))\s*$")


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
    if source == "projected-ts-outer":
        return (
            f"SELECT {listed} FROM (SELECT explode_outer(sequence(TIMESTAMP "
            "'2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00', INTERVAL 1 HOUR)) AS t) AS g"
        )
    if source == "subq-unaliased":
        return f"SELECT {listed} FROM (SELECT explode(arr) FROM {table}) AS g"
    if source == "subq-unaliased-lit":
        return f"SELECT {listed} FROM (SELECT explode(array(1, 2))) AS g"
    if source == "subq-unaliased-where":
        return f"SELECT {listed} FROM (SELECT explode(arr) FROM {table}) AS g WHERE col > 1"
    if source == "subq-unaliased-group":
        return (
            f"SELECT {listed} FROM (SELECT explode(arr) FROM {table}) AS g "
            "GROUP BY col ORDER BY col"
        )
    if source == "table-orderby-col":
        return f"SELECT {listed} FROM {table} ORDER BY id, col"
    if source == "table-id1":
        return f"SELECT {listed} FROM {table} WHERE id = 1"
    if source == "table-id1-orderby":
        return f"SELECT {listed} FROM {table} WHERE id = 1 ORDER BY `explode(custom)`"
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
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        return outcome(error)


def expr_column(functions: Any, item: str) -> Any:
    matched = _TRAILING_ALIAS.match(item)
    if matched is None:
        return functions.expr(item)
    body = matched.group(1)
    name = matched.group(2) if matched.group(2) is not None else matched.group(3)
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


def frame_ops(session: Any, functions: Any, table: str) -> dict[str, Callable[[], Any]]:
    def bare(item: str) -> Any:
        return session.table(table).select("id", functions.expr(item))

    def self_join() -> Any:
        side = session.table(table).where("id = 1").select("id", functions.expr("explode(arr)"))
        return (
            side.alias("a")
            .join(side.alias("b"), functions.col("a.id") == functions.col("b.id"))
            .select(functions.col("a.col").alias("c1"), functions.col("b.col").alias("c2"))
        )

    return {
        "r3-select-col": lambda: bare("explode(arr)").select("id", "col"),
        "r3-with-column": lambda: bare("explode(arr)").withColumn("y", functions.col("col") + 1),
        "r3-self-join": self_join,
        "r3-outer-select-col": lambda: bare("explode_outer(arr)").select("col", "id"),
        "r3-case-select-col": lambda: bare("explode(CASE WHEN id = 1 THEN arr END)").select("col"),
        "r3-group-col": lambda: (
            session.table(table).select(functions.expr("explode(arr)")).groupBy("col").count()
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
    for name, text in NAMED.items():
        answers[f"{name}|named"] = guarded(lambda text=text: session.sql(text.format(t=table)))
    for name, build in frame_ops(session, functions, table).items():
        answers[f"{name}|frame"] = guarded(build)
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
    session: Any, catalog: str, zone: str, ansi: str, cold: Callable[[], Callable[[str], Any]]
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
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            answers[f"{name}|write"] = outcome(error)
    read_back = cold()
    for name, table in written.items():
        answers[f"{name}|write"] = guarded(lambda table=table: read_back(table))
    return answers


def spark_read(session: Any) -> Callable[[str], Any]:
    return lambda table: session.sql(f"SELECT * FROM {table}")


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
            answers.update(write_answers(session, catalog, zone, ansi, lambda: spark_read(session)))
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
