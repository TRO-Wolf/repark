"""Amended pins of ICE-TSNS-TYPE-PLANNER-1: the lattice, positions, refusals, doors.

The twenty mixed-unit cells run on both SQL doors in three session zones; every
position of the lattice table runs beside an untyped NULL and beside each
microsecond kind; the refusals, the out-of-range raise in both ANSI modes, the microsecond pins of
the two spellings the planner does not reach, the inferred CSV unit, and the R-010, R-013 and R-018
statements are single named cells.

pins: ice-tsns-type-planner-1/C-006
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _ice_tsns_type_planner_1_rule as rule
import pyarrow as pa

import repark
from repark import functions

ZONES = ("UTC", "America/New_York", "Asia/Kolkata")
WALL = "2026-01-02 03:04:05.123456"
OPERANDS = {
    "us_tz": (f"TIMESTAMP '{WALL}'", "us", True),
    "us": (f"TIMESTAMP_NTZ '{WALL}'", "us", False),
    "ns": (f"CAST('{WALL}789' AS timestamp_ns)", "ns", False),
    "ns_tz": (f"CAST('{WALL}789+00:00' AS timestamptz_ns)", "ns", True),
}
FIRST_FORMS = {
    "coalesce": "SELECT coalesce({l}, {r}) AS v",
    "union": "SELECT {l} AS v UNION ALL SELECT {r} AS v",
    "case": "SELECT CASE WHEN 1 > 0 THEN {l} ELSE {r} END AS v",
}
ARRAY_FORM = {"spark": "SELECT array({l}, {r}) AS v", "ansi": "SELECT make_array({l}, {r}) AS v"}
SIBLINGS = {"null": "{N}", "ts": "<<ts>>", "ntz": "<<ntz>>"}
POSITIONS: tuple[tuple[str, str], ...] = (
    ("coalesce", "SELECT id, coalesce({c}, @X) AS v FROM {S}"),
    ("nvl", "SELECT id, nvl({c}, @X) AS v FROM {S}"),
    ("ifnull", "SELECT id, ifnull({c}, @X) AS v FROM {S}"),
    ("nvl2", "SELECT id, nvl2(id, {c}, @X) AS v FROM {S}"),
    ("greatest", "SELECT id, greatest({c}, @X) AS v FROM {S}"),
    ("least", "SELECT id, least({c}, @X) AS v FROM {S}"),
    ("array", "SELECT id, array({c}, @X) AS v FROM {S}"),
    ("array_append", "SELECT id, array_append(array({c}), @X) AS v FROM {S}"),
    ("array_prepend", "SELECT id, array_prepend(array({c}), @X) AS v FROM {S}"),
    ("array_concat", "SELECT id, concat(array({c}), array(@X)) AS v FROM {S}"),
    ("array_contains", "SELECT id, array_contains(array({c}), @X) AS v FROM {S}"),
    ("array_position", "SELECT id, array_position(array({c}), @X) AS v FROM {S}"),
    ("array_remove", "SELECT id, array_remove(array({c}), @X) AS v FROM {S}"),
    ("element_at", "SELECT id, element_at(array({c}, @X), 2) AS v FROM {S}"),
    ("case_searched", "SELECT id, CASE WHEN id > 1 THEN {c} ELSE @X END AS v FROM {S}"),
    ("case_simple", "SELECT id, CASE {c} WHEN @X THEN 1 ELSE 0 END AS v FROM {S}"),
    ("if", "SELECT id, if(id > 1, {c}, @X) AS v FROM {S}"),
    ("union", "SELECT {c} AS v FROM {S} UNION SELECT @X FROM {S}"),
    ("union_all", "SELECT {c} AS v FROM {S} UNION ALL SELECT @X FROM {S}"),
    ("intersect", "SELECT {c} AS v FROM {S} INTERSECT SELECT @X FROM {S}"),
    ("except", "SELECT {c} AS v FROM {S} EXCEPT SELECT @X FROM {S}"),
    ("eq", "SELECT id, {c} = @X AS v FROM {S}"),
    ("ne", "SELECT id, {c} <> @X AS v FROM {S}"),
    ("lt", "SELECT id, {c} < @X AS v FROM {S}"),
    ("le", "SELECT id, {c} <= @X AS v FROM {S}"),
    ("gt", "SELECT id, {c} > @X AS v FROM {S}"),
    ("ge", "SELECT id, {c} >= @X AS v FROM {S}"),
    ("between", "SELECT id, {c} BETWEEN @X AND @X AS v FROM {S}"),
    ("in_list", "SELECT id, {c} IN (@X) AS v FROM {S}"),
    ("null_safe_eq", "SELECT id, {c} <=> @X AS v FROM {S}"),
    ("join_equi", "SELECT a.id FROM {S} a JOIN {S} b ON a.{c} = @Xb"),
    ("join_non_equi", "SELECT a.id FROM {S} a JOIN {S} b ON a.{c} >= @Xb"),
)
REFUSED = ("range", "generate_series")
OUT_OF_RANGE = {"us_first": "coalesce(far, ns)", "ns_first": "greatest(ns, far)"}
Q6_CELL = "q6|repark_sql|timestamp_spellings"
SERIES = "TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-03 00:00:00', INTERVAL '1' DAY"


def _native(text: str) -> dict[str, Any]:
    return rule.capture_native(lambda: repark.sql(text))


def _lattice_cells(door: str) -> list[tuple[str, str, str, str]]:
    cells: list[tuple[str, str, str, str]] = []
    for form, text in FIRST_FORMS.items():
        for left in ("us_tz", "us"):
            for right in ("ns", "ns_tz"):
                cells.append((form, left, right, text))
    for left, (_, left_unit, _) in OPERANDS.items():
        for right, (_, right_unit, _) in OPERANDS.items():
            if left_unit != right_unit:
                cells.append(("array", left, right, ARRAY_FORM[door]))
    return cells


def _lattice_check(run: Callable[[str], dict[str, Any]], door: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for form, left, right, text in _lattice_cells(door):
        left_sql, left_unit, left_zoned = OPERANDS[left]
        right_sql, right_unit, right_zoned = OPERANDS[right]
        want_unit = rule.finest([left_unit, right_unit])
        name = rule.site_type("ns", left_zoned or right_zoned)
        statement = run(text.format(l=left_sql, r=right_sql))
        wide_left = left_sql if left_unit == want_unit else rule.typed(left_sql, name)
        wide_right = right_sql if right_unit == want_unit else rule.typed(right_sql, name)
        twin = run(text.format(l=wide_left, r=wide_right))
        reason = rule.differ(statement, twin, ordered=False)
        if not reason and rule.answered(statement):
            got = statement["types"][0]
            if f"[{want_unit}" not in got:
                reason = f"type {got}, the finest unit is {want_unit}"
        if not reason and not rule.answered(statement):
            reason = repr(statement)[:260]
        out[f"{form}|{left}|{right}"] = reason
        rule.note(f"lattice|{door}|{rule.ZONE[0]}|{form}|{left}|{right}", statement)
    return out


def _lattice_run(zone: str) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        out: dict[str, str] = {}
        rule.ZONE[0] = zone
        spark = doors.open_session(zone, scratch)
        try:
            cells = _lattice_check(lambda text: rule.capture(rule.run_text(spark, text)), "spark")
            for name, reason in cells.items():
                out[f"lattice|spark|{zone}|{name}"] = reason
        finally:
            spark.stop()
        repark.sql(f"SET TIME ZONE = '{zone}'").to_arrow()
        try:
            for name, reason in _lattice_check(_native, "ansi").items():
                out[f"lattice|ansi|{zone}|{name}"] = reason
        finally:
            repark.sql("SET TIME ZONE = 'UTC'").to_arrow()
        return out

    return run


def lattice_groups() -> list[rule.Group]:
    """Return the twenty mixed-unit cells on both doors, one group per zone."""
    return [
        rule.Group(
            name=f"lattice|{zone}",
            family="lattice",
            zone=zone,
            run=_lattice_run(zone),
            cells=2 * len(_lattice_cells("spark")),
        )
        for zone in ZONES
    ]


def _position_text(text: str, sibling: str) -> str:
    marker = SIBLINGS[sibling]
    joined = "{N}" if sibling == "null" else f"<<b.{sibling}>>"
    return text.replace("@Xb", joined).replace("@X", marker)


def _positions_run(zone: str) -> Callable[[Path], dict[str, str]]:
    def run(scratch: Path) -> dict[str, str]:
        spark = doors.open_session(zone, scratch)
        out: dict[str, str] = {}
        try:
            for column in ("ns", "tzns"):
                for position, text in POSITIONS:
                    for sibling in SIBLINGS:
                        marked = _position_text(text, sibling)
                        zoned = column != "ns" or sibling == "ts"
                        statement = rule.capture(
                            rule.run_text(spark, rule.statement_of(marked, column))
                        )
                        twin = rule.capture(
                            rule.run_text(spark, rule.twin_of(marked, column, zoned))
                        )
                        reason = rule.differ(statement, twin, ordered=False)
                        if not reason and not rule.answered(statement):
                            reason = repr(statement)[:260]
                        cell = f"position|{position}|{sibling}|{column}|{zone}"
                        out[cell] = reason
                        rule.note(cell, statement)
        finally:
            spark.stop()
        return out

    return run


def position_groups() -> list[rule.Group]:
    """Return every lattice position beside a NULL and each microsecond kind."""
    return [
        rule.Group(
            name=f"position|{zone}",
            family="position",
            zone=zone,
            run=_positions_run(zone),
            cells=len(POSITIONS) * len(SIBLINGS) * 2,
        )
        for zone in ZONES
    ]


def _refusal(outcome: dict[str, Any], name: str) -> str:
    if rule.answered(outcome):
        return f"{name} over timestamps answered: {outcome}"[:260]
    text = outcome.get("error", "")
    if "sequence" not in text or name not in text:
        return f"the refusal names neither `{name}` nor `sequence`: {text}"[:260]
    return ""


def _overflow(outcome: dict[str, Any]) -> str:
    if rule.answered(outcome):
        return f"a microsecond value outside the nanosecond range answered: {outcome}"[:260]
    text = outcome.get("error", "")
    return "" if "OVERFLOW" in text else f"not the overflow class: {text}"[:260]


def _unit(outcome: dict[str, Any], want: str, index: int = 0) -> str:
    if not rule.answered(outcome):
        return repr(outcome)[:260]
    got = outcome["types"][index]
    return "" if f"[{want}" in got else f"type {got}, wanted unit {want}"


def _live_pairs(spark: Any, table: str) -> list[list[Any]]:
    rows = spark.sql(f"SELECT id, v FROM {table} WHERE v IS NOT NULL").toArrow()
    ticks = rule.ticks_of(rows.column(1))
    return sorted(
        ([row, tick] for row, tick in zip(rows.column(0).to_pylist(), ticks, strict=True)),
        key=repr,
    )


def _equal_statements(spark: Any, scratch: Path, name: str, left: str, right: str) -> str:
    seeded = "INSERT INTO {T} (id, k) VALUES (1, 0), (2, 0)"
    outcomes = []
    for statement in (left, right):
        table = doors.fresh_table()
        result: dict[str, Any] = {}
        try:
            spark.sql(
                f"CREATE TABLE {table} (id INT, v {name}, k INT) USING iceberg "
                "TBLPROPERTIES ('format-version' = '3')"
            )
            spark.sql(seeded.replace("{T}", table)).toArrow()
            try:
                for part in statement.split(";"):
                    spark.sql(part.replace("{T}", table)).toArrow()
            except Exception as error:
                result["error"] = doors.error_text(error)
                result["error_type"] = type(error).__name__
            _, parquet_type = doors.stored(scratch, table)
            result["stored"] = _live_pairs(spark, table)
            result["parquet_type"] = parquet_type
        finally:
            doors.cleanup(spark, scratch, table)
        outcomes.append(result)
    if not rule.answered(outcomes[0]):
        return repr(outcomes[0])[:260]
    return rule.differ(outcomes[0], outcomes[1])


def _csv_cell(spark: Any, scratch: Path, reader: str) -> str:
    path = scratch / "p15.csv"
    path.write_text("ts\n2024-01-02 03:04:05\n2024-06-07 08:09:10\n", encoding="utf-8")
    options = {"header": "true", "inferSchema": "true"}
    try:
        if reader == "read_csv":
            table = spark.read_csv(str(path), options).toArrow()
        else:
            table = spark.read.options(**options).csv(str(path)).toArrow()
    except Exception as error:
        return f"{type(error).__name__}: {error}"[:260]
    kind = table.schema.types[0]
    if not pa.types.is_timestamp(kind) or kind.unit != "us":
        return f"inferred {kind}, Spark infers microseconds"
    want = [
        int(datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC).timestamp()) * 1_000_000,
        int(datetime(2024, 6, 7, 8, 9, 10, tzinfo=UTC).timestamp()) * 1_000_000,
    ]
    got = sorted(table.column(0).cast(pa.int64()).to_pylist())
    return "" if got == want else f"ticks {got}, the seconds widen to {want}"


def _door_run(scratch: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    spark = doors.open_session("UTC", scratch)
    try:
        for name in REFUSED:
            outcome = rule.capture(rule.run_text(spark, f"SELECT {name}({SERIES})"))
            out[f"h1|spark|{name}"] = _refusal(outcome, name)
            out[f"h1|ansi|{name}"] = _refusal(_native(f"SELECT {name}({SERIES})"), name)
        integers = rule.capture(rule.run_text(spark, "SELECT * FROM range(3)"))
        out["h1|spark|integer_range_relation"] = (
            ""
            if integers.get("rows") == [[repr(value) for value in range(3)]]
            else repr(integers)[:260]
        )
        spark.sql("CREATE TABLE ice.ns.h2 (c TIMESTAMP) USING iceberg").toArrow()
        out["h2|ddl_timestamp_column"] = _unit(
            rule.capture(rule.run_text(spark, "SELECT c FROM ice.ns.h2")), "us"
        )
        frame = spark.createDataFrame([(1,)], "x int")
        out["h2|column_cast_timestamp"] = _unit(
            rule.capture(lambda: frame.select(functions.col("x").cast("timestamp"))), "us"
        )
        for reader in ("read_csv", "reader_csv"):
            out[f"p15|{reader}|inferred_seconds"] = _csv_cell(spark, scratch, reader)
        literal = "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns)"
        out["r010|update_set_literal"] = _equal_statements(
            spark,
            scratch,
            "timestamp_ns",
            f"UPDATE {{T}} SET v = {literal} WHERE id = 1",
            f"INSERT INTO {{T}} (id, v, k) SELECT 1, {literal}, 0;"
            "DELETE FROM {T} WHERE v IS NULL AND id = 1",
        )
        out["r010|delete_where_literal"] = _equal_statements(
            spark,
            scratch,
            "timestamp_ns",
            f"INSERT INTO {{T}} (id, v, k) SELECT 3, {literal}, 0;"
            f"DELETE FROM {{T}} WHERE v = {literal}",
            "INSERT INTO {T} (id, v, k) SELECT 3, NULL, 0;DELETE FROM {T} WHERE id = 3",
        )
        named = _native("SELECT __repark_cast_timestamp_ns__('2026-01-02 03:04:05')")
        out["r013|ansi_kernel_not_a_routine"] = (
            "" if not rule.answered(named) else f"the kernel resolves by name: {named}"[:260]
        )
        for shape, text in (
            ("array", "array(TIMESTAMP '2026-01-02 03:04:05')"),
            ("named_struct", "named_struct('a', TIMESTAMP '2026-01-02 03:04:05')"),
            ("map", "map('a', TIMESTAMP '2026-01-02 03:04:05')"),
        ):
            values = rule.capture(
                rule.run_text(spark, f"SELECT * FROM (VALUES (1, {text})) AS q(a, b)")
            )
            select = rule.capture(rule.run_text(spark, f"SELECT 1 AS a, {text} AS b"))
            reason = rule.differ(values, select)
            if not reason and not rule.answered(values):
                reason = repr(values)[:260]
            out[f"r018|values_{shape}_literal"] = reason
        for ansi in ("true", "false"):
            spark.conf.set("spark.sql.ansi.enabled", ansi)
            for name, text in OUT_OF_RANGE.items():
                outcome = rule.capture(rule.run_text(spark, f"SELECT {text} AS v FROM {rule.SRC}"))
                out[f"range|{name}|ansi{ansi}"] = _overflow(outcome)
    finally:
        spark.stop()
    return out


def door_groups() -> list[rule.Group]:
    """Return the refusal, microsecond-pin, CSV and registry-row cells."""
    return [rule.Group(name="doors|UTC", family="doors", zone="UTC", run=_door_run, cells=19)]


def _q6_run(_scratch: Path) -> dict[str, str]:
    wall = "2024-01-02 03:04:05.123456789"
    micros = [[1_704_164_645_123_456]]
    want = {
        f"SELECT CAST('{wall}' AS TIMESTAMP) AS v": ("timestamp[us]", micros),
        f"SELECT TIMESTAMP '{wall}' AS v": ("timestamp[us]", micros),
        f"SELECT CAST('{wall}' AS TIMESTAMPTZ) AS v": ("timestamp[us, tz=UTC]", micros),
    }
    reasons = []
    outcomes = []
    for text, (kind, ticks) in want.items():
        outcome = _native(text)
        outcomes.append(outcome)
        if not rule.answered(outcome):
            reasons.append(f"{text}: {outcome}")
        elif outcome["types"] != [kind] or outcome["rows"] != ticks:
            reasons.append(f"{text}: {outcome['types']} {outcome['rows']}, ruled {kind} {ticks}")
    failed = [outcome for outcome in outcomes if not rule.answered(outcome)]
    rule.note(Q6_CELL, failed[0] if failed else outcomes[0])
    return {Q6_CELL: "; ".join(reasons)[:300]}


def q6_groups() -> list[rule.Group]:
    """Return the ruling-94 cell on the Python ``repark.sql()`` callable."""
    return [rule.Group(name="q6|repark_sql", family="q6", zone="UTC", run=_q6_run, cells=1)]


def _postgres_run(scratch: Path) -> dict[str, str]:
    url = os.environ.get("REPARK_PG_URL", "").strip()
    out: dict[str, str] = {}
    if not url:
        return {"postgres|timestamp": "skip", "postgres|timestamptz": "skip"}
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=disable"
    spark = doors.open_session("UTC", scratch)
    try:
        query = f"SELECT TIMESTAMP '{WALL}' AS ts, TIMESTAMPTZ '{WALL}+00' AS tz"
        outcome = rule.capture(lambda: spark.read_postgres(url, query=query))
        rule.note("postgres|timestamp", outcome)
        rule.note("postgres|timestamptz", outcome)
        want = int(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC).timestamp()) * 1_000_000 + 123456
        for index, name in enumerate(("timestamp", "timestamptz")):
            reason = _unit(outcome, "us", index)
            if not reason and outcome["rows"][index] != [want]:
                reason = f"ticks {outcome['rows'][index]}, wanted {[want]}"
            out[f"postgres|{name}"] = reason
    finally:
        spark.stop()
    return out


def postgres_groups() -> list[rule.Group]:
    """Return the two Postgres read cells; they skip without ``REPARK_PG_URL``."""
    return [
        rule.Group(name="postgres|UTC", family="postgres", zone="UTC", run=_postgres_run, cells=2)
    ]
