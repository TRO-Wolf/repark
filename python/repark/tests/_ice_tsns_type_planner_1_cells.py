"""Cells of the ICE-TSNS-TYPE-PLANNER-1 fixture: census, cache, reads, consumers, repros.

Each family returns groups; a group opens one session and returns, per named
cell, an empty string (green) or the reason it is red. The expectation of every
cell comes from ``_ice_tsns_type_planner_1_rule``.

pins: ice-tsns-type-planner-1/C-006
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _ice_tsns_type_planner_1_rule as rule
import pandas as pd

from repark import functions

TESTS = Path(__file__).resolve().parent
TWINS = TESTS / "ice_tsns_coercion_1_twins.json"
READS = TESTS / "ice_tsns_type_planner_1_reads.json"
CENSUS_FORMS = ("coalesce", "array", "case", "if", "union")
CENSUS_SIBLING = "untyped_null"
CACHE_ZONES = (
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Asia/Kathmandu",
    "Australia/Lord_Howe",
)
CACHE_NULL_SPELLINGS = {
    "coalesce": "coalesce({c}, {N})",
    "nvl": "nvl({c}, {N})",
    "case": "CASE WHEN id > 0 THEN {c} ELSE {N} END",
    "if": "if(id > 0, {c}, {N})",
    "elt": "array({c}, {N})[0]",
    "case_null_first": "CASE WHEN id < 0 THEN {N} ELSE {c} END",
    "coalesce_null_first": "coalesce({N}, {c})",
    "ifnull": "ifnull({c}, {N})",
    "nvl2": "nvl2(id, {c}, {N})",
    "element_at": "element_at(array({N}, {c}), 2)",
    "lambda": "transform(array({c}), x -> coalesce(x, {N}))[0]",
    "greatest": "greatest({c}, {N})",
    "nullif": "nullif({c}, {N})",
    "struct_field": "named_struct('f', coalesce({c}, {N})).f",
    "case_three": (
        "CASE WHEN id = 1 THEN {c} "
        "WHEN id = 2 THEN <<TIMESTAMP '2026-01-02 03:04:05'>> ELSE {N} END"
    ),
}
CACHE_VALUE_SPELLINGS = {
    "typed_ts_null": "coalesce({c}, <<CAST(NULL AS TIMESTAMP)>>)",
    "typed_ntz_null": "coalesce({c}, <<CAST(NULL AS TIMESTAMP_NTZ)>>)",
    "beside_literal": ("CASE WHEN id = 1 THEN {c} ELSE <<TIMESTAMP '2026-01-02 03:04:05'>> END"),
    "beside_value": "coalesce({c}, <<CAST({c} AS TIMESTAMP)>>)",
    "greatest_literal": "greatest({c}, <<TIMESTAMP '2026-01-02 03:04:05'>>)",
    "array_literal": "array({c}, <<TIMESTAMP '2026-01-02 03:04:05'>>)[0]",
}
CACHE_ZONED = frozenset(
    {
        "case_three",
        "typed_ts_null",
        "beside_literal",
        "beside_value",
        "greatest_literal",
        "array_literal",
    }
)
CACHE_TARGETS = {"ts_ns": "timestamp_ns", "tz_ns": "timestamptz_ns"}
CACHE_SOURCES = ("ns", "tzns")
CONSUMER_ZONES = ("UTC", "America/New_York")
CONSUMER_EXTRAS = ("k", "big", "dec", "s")


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _census_run(zone: str, target: str, source: str) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        fixture = _load(TWINS)
        door_names = fixture["doors"]
        spark = doors.open_session(zone, scratch)
        out: dict[str, str] = {}
        try:
            for form in CENSUS_FORMS:
                key = f"{zone}|{target}|{source}|{form}|{CENSUS_SIBLING}"
                recorded = fixture["twin"][key]
                for index, door in enumerate(door_names):
                    args = (spark, scratch, target, source, form, CENSUS_SIBLING)
                    statement = doors.measure(*args, "statement", door)
                    twin = doors.measure(*args, "twin", door)
                    reason = rule.differ(statement, twin)
                    main_twin = fixture["outcomes"][recorded[index]]
                    if not reason and rule.answered(main_twin) and not rule.answered(statement):
                        reason = f"main's twin answered, head errors: {statement}"[:300]
                    out[f"census|{key}|{door}"] = reason
                    rule.note(f"census|{key}|{door}", statement)
        finally:
            spark.stop()
        return out

    return run


def census_groups() -> list[rule.Group]:
    """Return the 9,600 census cells as one group per zone, target and source."""
    fixture = _load(TWINS)
    groups: list[rule.Group] = []
    for key in sorted(fixture["plain"]):
        zone, target, source = key.split("|")
        groups.append(
            rule.Group(
                name=f"census|{key}",
                family="census",
                zone=zone,
                run=_census_run(zone, target, source),
                cells=len(CENSUS_FORMS) * len(fixture["doors"]),
            )
        )
    return groups


def _append(
    spark: Any, scratch: Path, target: str, source: str, value: str, cached: bool
) -> dict[str, Any]:
    table = doors.fresh_table()
    result: dict[str, Any] = {}
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, v {CACHE_TARGETS[target]}, k INT, "
            f"c {rule.NS_NAMES[source]}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        )
        try:
            frame = spark.sql(
                f"SELECT id, {value} AS v, 0 AS k, "
                f"CAST(NULL AS {rule.NS_NAMES[source]}) AS c FROM {rule.SRC}"
            )
            if cached:
                frame = frame.cache()
            frame.writeTo(table).append()
        except Exception as error:
            result["error"] = doors.error_text(error)
            result["error_type"] = type(error).__name__
        pairs, parquet_type = doors.stored(scratch, table)
        result["stored"] = pairs
        if parquet_type:
            result["parquet_type"] = parquet_type
    finally:
        doors.cleanup(spark, scratch, table)
    return result


def _cache_run(zone: str) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        spark = doors.open_session(zone, scratch)
        out: dict[str, str] = {}
        try:
            for name, text in {**CACHE_NULL_SPELLINGS, **CACHE_VALUE_SPELLINGS}.items():
                for target in CACHE_TARGETS:
                    for source in CACHE_SOURCES:
                        zoned = source == "tzns" or name in CACHE_ZONED
                        statement = rule.statement_of(text, source)
                        twin = rule.twin_of(text, source, zoned)
                        cached = _append(spark, scratch, target, source, statement, True)
                        plain = _append(spark, scratch, target, source, statement, False)
                        typed = _append(spark, scratch, target, source, twin, False)
                        reason = rule.differ(cached, plain) or rule.differ(cached, typed)
                        out[f"cache|{zone}|{target}|{source}|{name}"] = reason
                        rule.note(f"cache|{zone}|{target}|{source}|{name}", cached)
        finally:
            spark.stop()
        return out

    return run


def cache_groups() -> list[rule.Group]:
    """Return the 420 cache cells: twenty-one spellings, two targets, two sources, five zones."""
    count = (
        (len(CACHE_NULL_SPELLINGS) + len(CACHE_VALUE_SPELLINGS))
        * len(CACHE_TARGETS)
        * len(CACHE_SOURCES)
    )
    return [
        rule.Group(
            name=f"cache|{zone}", family="cache", zone=zone, run=_cache_run(zone), cells=count
        )
        for zone in CACHE_ZONES
    ]


def _reads_run(
    zone: str, ansi: str, wants: list[tuple[str, str]]
) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        cases = _load(READS)["cases"]
        spark = rule.open_session(zone, scratch, ansi)
        out: dict[str, str] = {}
        try:
            for key, column in wants:
                case = cases[key]
                kind = "sql" if "sql" in case else "py"
                runner = rule.run_text if kind == "sql" else rule.run_code
                statement = rule.capture(runner(spark, rule.statement_of(case[kind], column)))
                twin = rule.capture(runner(spark, rule.twin_of(case[kind], column)))
                cell = f"reads|{key}|{column}|{zone}|ansi{ansi}"
                out[cell] = rule.differ(statement, twin)
                rule.note(cell, statement)
        finally:
            spark.stop()
        return out

    return run


def reads_groups() -> list[rule.Group]:
    """Return the 117 read cells, one group per zone and ANSI mode."""
    grouped: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for key, column, zone, ansi in _load(READS)["cells"]:
        grouped.setdefault((zone, ansi), []).append((key, column))
    return [
        rule.Group(
            name=f"reads|{zone}|ansi{ansi}",
            family="reads",
            zone=zone,
            ansi=ansi,
            run=_reads_run(zone, ansi, wants),
            cells=len(wants),
        )
        for (zone, ansi), wants in sorted(grouped.items())
    ]


def _consumer_table(spark: Any) -> Any:
    spark.sql(
        "CREATE TABLE ice.ns.dn (id INT, k INT, big BIGINT, dec DECIMAL(10,2), d DATE, "
        "s STRING, ok BOOLEAN, us TIMESTAMP, untz TIMESTAMP_NTZ, ns timestamp_ns, "
        "tzns timestamptz_ns) USING iceberg TBLPROPERTIES ('format-version' = '3')"
    ).toArrow()
    spark.sql(
        "INSERT INTO ice.ns.dn SELECT 1, 10, 100, CAST(1.50 AS DECIMAL(10,2)), "
        "DATE '2026-01-02', 'a', true, TIMESTAMP '2026-01-02 03:04:05.123456', "
        "CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ), "
        "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), "
        "CAST('2026-01-02 03:04:05.123456789+00:00' AS timestamptz_ns)"
    ).toArrow()
    spark.sql(
        "INSERT INTO ice.ns.dn SELECT 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
        "CAST('1969-12-31 23:59:59.999999999' AS timestamp_ns), "
        "CAST('1969-12-31 23:59:59.999999999+00:00' AS timestamptz_ns)"
    ).toArrow()
    spark.sql(
        "INSERT INTO ice.ns.dn SELECT 2, 20, 200, CAST(2.25 AS DECIMAL(10,2)), "
        "DATE '1969-12-31', 'b', false, TIMESTAMP '1969-12-31 23:59:59.999999', "
        "CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP_NTZ), NULL, NULL"
    ).toArrow()
    return spark.table("ice.ns.dn")


def _value(column: str, site: bool) -> Any:
    if site:
        return functions.coalesce(functions.col(column), functions.lit(None))
    return functions.col(column)


def consumer_cases(table: Any, column: str, site: bool) -> dict[str, Callable[[], Any]]:
    """Return the consumer doors over the site, or over the plain column."""
    cases: dict[str, Callable[[], Any]] = {}

    def frame(extra: str = "") -> Any:
        columns = ["id", _value(column, site).alias("v")] + ([extra] if extra else [])
        return table.select(*columns)

    def declared() -> str:
        return frame().schema.fields[1].dataType.simpleString()

    for extra in CONSUMER_EXTRAS:
        cases[f"+{extra}|aip"] = lambda extra=extra: (
            frame(extra)
            .groupBy("id")
            .applyInPandas(lambda pdf: pdf, frame(extra).schema)
            .orderBy("id", "v")
        )
        cases[f"+{extra}|cog"] = lambda extra=extra: (
            frame(extra)
            .groupBy("id")
            .cogroup(frame(extra).groupBy("id"))
            .applyInPandas(lambda left, right: left, frame(extra).schema)
            .orderBy("id", "v")
        )
    cases["aip_agg_len"] = lambda: (
        frame()
        .groupBy("id")
        .applyInPandas(
            lambda pdf: pd.DataFrame({"id": [pdf.id.iloc[0]], "v": [pdf.v.max()], "n": [len(pdf)]}),
            "id int, v " + declared() + ", n int",
        )
        .orderBy("id")
    )
    cases["aip_id_int64"] = lambda: (
        frame()
        .groupBy("id")
        .applyInPandas(lambda pdf: pdf.assign(id=pdf.id.astype("int64")), frame().schema)
        .orderBy("id", "v")
    )
    cases["aip_reordered"] = lambda: (
        frame()
        .groupBy("id")
        .applyInPandas(lambda pdf: pdf[["v", "id"]], frame().schema)
        .orderBy("id", "v")
    )
    cases["aip_add_float"] = lambda: (
        frame()
        .groupBy("id")
        .applyInPandas(lambda pdf: pdf.assign(x=1), "id int, v " + declared() + ", x double")
        .orderBy("id", "v")
    )
    cases["aip_add_str_none"] = lambda: (
        frame()
        .groupBy("id")
        .applyInPandas(lambda pdf: pdf.assign(x=None), "id int, v " + declared() + ", x string")
        .orderBy("id", "v")
    )
    for decl in ("timestamp", "timestamp_ntz"):
        cases[f"mia_ddl|{decl}"] = lambda decl=decl: (
            frame().mapInArrow(lambda batches: batches, f"id int, v {decl}").orderBy("id", "v")
        )
        cases[f"mip_ddl|{decl}"] = lambda decl=decl: (
            frame().mapInPandas(lambda frames: frames, f"id int, v {decl}").orderBy("id", "v")
        )
        cases[f"aip_ddl|{decl}"] = lambda decl=decl: (
            frame()
            .groupBy("id")
            .applyInPandas(lambda pdf: pdf, f"id int, v {decl}")
            .orderBy("id", "v")
        )
        cases[f"cog_ddl|{decl}"] = lambda decl=decl: (
            frame()
            .groupBy("id")
            .cogroup(frame().groupBy("id"))
            .applyInPandas(lambda left, right: left, f"id int, v {decl}")
            .orderBy("id", "v")
        )

    def nested() -> Any:
        return table.select("id", functions.struct(_value(column, site).alias("a")).alias("v"))

    cases["aip_struct"] = lambda: (
        nested().groupBy("id").applyInPandas(lambda pdf: pdf, nested().schema).orderBy("id")
    )
    cases["mia_struct"] = lambda: (
        nested().mapInArrow(lambda batches: batches, nested().schema).orderBy("id")
    )
    value = _value(column, site)
    cases["udf|pandas_udf_ret_ntz"] = lambda: table.select(
        "id", functions.pandas_udf(lambda series: series, "timestamp_ntz")(value).alias("v")
    ).orderBy("id", "v")
    cases["udf|pandas_udf_ret_ts"] = lambda: table.select(
        "id", functions.pandas_udf(lambda series: series, "timestamp")(value).alias("v")
    ).orderBy("id", "v")
    cases["udf|arrow_udf_ret_ntz"] = lambda: table.select(
        "id", functions.arrow_udf(lambda array: array, "timestamp_ntz")(value).alias("v")
    ).orderBy("id", "v")
    cases["udf|arrow_udf_ret_ts"] = lambda: table.select(
        "id", functions.arrow_udf(lambda array: array, "timestamp")(value).alias("v")
    ).orderBy("id", "v")
    cases["udf|apply_in_arrow"] = lambda: (
        frame().groupBy("id").applyInArrow(lambda batch: batch, frame().schema).orderBy("id", "v")
    )
    return cases


CONSUMER_SKIPPED = {
    "ns": (
        "mia_ddl|timestamp_ntz",
        "mip_ddl|timestamp_ntz",
        "aip_ddl|timestamp_ntz",
        "cog_ddl|timestamp_ntz",
    ),
    "tzns": (
        "mia_ddl|timestamp",
        "mip_ddl|timestamp",
        "aip_ddl|timestamp",
        "cog_ddl|timestamp",
        "mip_ddl|timestamp_ntz",
        "mia_ddl|timestamp_ntz",
    ),
}


def consumer_names(column: str) -> list[str]:
    """Return the names of the consumer cells kept for ``column``."""
    names: list[str] = []
    for extra in CONSUMER_EXTRAS:
        names += [f"+{extra}|aip", f"+{extra}|cog"]
    names += ["aip_agg_len", "aip_id_int64", "aip_reordered", "aip_add_float", "aip_add_str_none"]
    for decl in ("timestamp", "timestamp_ntz"):
        names += [f"mia_ddl|{decl}", f"mip_ddl|{decl}", f"aip_ddl|{decl}", f"cog_ddl|{decl}"]
    names += ["aip_struct", "mia_struct"]
    names += [
        "udf|pandas_udf_ret_ntz",
        "udf|pandas_udf_ret_ts",
        "udf|arrow_udf_ret_ntz",
        "udf|arrow_udf_ret_ts",
        "udf|apply_in_arrow",
    ]
    return [name for name in names if name not in CONSUMER_SKIPPED[column]]


def _consumer_run(zone: str) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        spark = doors.open_session(zone, scratch)
        out: dict[str, str] = {}
        try:
            table = _consumer_table(spark)
            for column in ("ns", "tzns"):
                site = consumer_cases(table, column, True)
                plain = consumer_cases(table, column, False)
                for name in consumer_names(column):
                    left = rule.capture(site[name])
                    right = rule.capture(plain[name])
                    reason = rule.differ(left, right, ordered=False)
                    out[f"consumer|{column}|{name}|{zone}"] = reason
                    rule.note(f"consumer|{column}|{name}|{zone}", left)
        finally:
            spark.stop()
        return out

    return run


def consumer_groups() -> list[rule.Group]:
    """Return the consumer-door cells: a site against the plain column, two zones."""
    count = len(consumer_names("ns")) + len(consumer_names("tzns"))
    return [
        rule.Group(
            name=f"consumer|{zone}",
            family="consumer",
            zone=zone,
            run=_consumer_run(zone),
            cells=count,
        )
        for zone in CONSUMER_ZONES
    ]
