"""Record and check the ICE-TSNS-SQL-1 oracle: the Iceberg spec plus a PyIceberg 0.12.0 read-back.

Spark 4.1.2 cannot read or write ``timestamp_ns``, so the oracle is the Iceberg spec and a second
engine. The recorder runs in two processes because the repository venv carries RePark and no
PyIceberg, and the PyIceberg interpreter carries no RePark:

1. ``<repo venv python> _record_ice_tsns_sql_1_oracle.py write --warehouse W`` writes the
   DataFrame-door control tables and every SQL-door table into ``W`` and leaves RePark's own
   answers in ``W/repark_answers.json``. On a tree without the ICE-TSNS-SQL-1 product change
   the SQL-door writes fail; the failures are recorded, not raised.
2. ``<pyiceberg python> _record_ice_tsns_sql_1_oracle.py record --warehouse W`` reads the control
   tables with ``StaticTable``, derives the SQL-door expectations from them, and prints the
   fixture JSON (``--output`` also writes it).
3. ``<pyiceberg python> _record_ice_tsns_sql_1_oracle.py check --warehouse W`` reads every
   SQL-door table with ``StaticTable`` and exits non-zero on any difference from the fixture.

The PyIceberg interpreter needs ``pyiceberg==0.12.0`` and ``pyarrow``; its path comes from the
caller and is never written into this file.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_FIXTURE = REPO_ROOT / "python" / "repark" / "tests" / "ice_tsns_sql_1_oracle.json"
V3 = "TBLPROPERTIES ('format-version' = '3')"
NS_PER_SECOND = 1_000_000_000
SESSION_ZONE = "UTC"
FOREIGN_ZONE = "America/Los_Angeles"
HOURS_DERIVATION = (
    "spec: hour = floor(tz_ns / 3_600_000_000_000); "
    "the DataFrame-door control is refused by the fork"
)

LITERALS: dict[int, str] = {
    1: "2026-01-02 03:04:05.123456789",
    2: "2026-01-02 23:59:59.999999999",
    3: "2026-01-03 00:00:00.000000001",
    4: "2026-01-03 23:59:59.999999",
    5: "2026-01-04 00:00:00.000000001",
    6: "2026-01-02 03:04:05.5",
    7: "2026-01-02 03:04:05",
    8: "2026-01-02 03:04:05.123456788",
}
MICRO_LITERAL_IDS = (4,)
STRING_LITERAL_IDS = (5,)
HOURS_IDS = (1, 2, 3, 8)
OVERWRITE_IDS = (4, 6, 7)
MERGE_INSERT_IDS = (2, 3)
MERGE_UPDATE_ID = 1
MERGE_UPDATE_SOURCE_ID = 8
CTAS_IDS = (1, 3)

SQL_TABLES: dict[str, str] = {
    "sql_days": "INSERT … VALUES: ns casts, a TIMESTAMP literal and a string",
    "sql_hours": "INSERT … SELECT with CAST(… AS timestamptz_ns) into hours(tz)",
    "sql_overwrite": "INSERT OVERWRITE … VALUES with TIMESTAMP literals and ns casts",
    "sql_merge": "MERGE INTO: insert and update with ns casts and a microsecond TIMESTAMP source",
    "sql_ctas": "CTAS carrying CAST(… AS timestamp_ns) and CAST(… AS timestamptz_ns)",
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the subcommand, the warehouse, and the fixture path."""
    parser = argparse.ArgumentParser(description="Record or check the ICE-TSNS-SQL-1 oracle.")
    parser.add_argument("command", choices=("write", "record", "check"))
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--fixture", default=str(DEFAULT_FIXTURE))
    parser.add_argument("--output", default="")
    return parser.parse_args(argv)


def wall_ns(text: str) -> int:
    """Return the naive wall of a literal as int64 nanoseconds since the epoch, exactly."""
    whole, _, fraction = text.partition(".")
    seconds = dt.datetime.strptime(whole, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.UTC)
    digits = (fraction + "000000000")[:9]
    return int(seconds.timestamp()) * NS_PER_SECOND + int(digits)


def zoned_instant_ns(text: str, zone: str) -> int:
    """Return the instant of a zoneless literal read as a wall in ``zone``, in nanoseconds."""
    wall = wall_ns(text)
    naive = dt.datetime.fromtimestamp(wall // NS_PER_SECOND, dt.UTC).replace(tzinfo=None)
    offset = naive.replace(tzinfo=ZoneInfo(zone)).utcoffset()
    assert offset is not None
    return wall - int(offset.total_seconds()) * NS_PER_SECOND


def render_ns(value: int) -> str:
    """Render int64 nanoseconds the way the contract's clause 4 trims a fraction."""
    seconds, fraction = divmod(value, NS_PER_SECOND)
    stamp = dt.datetime.fromtimestamp(seconds, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
    if fraction == 0:
        return stamp
    return f"{stamp}.{fraction:09d}".rstrip("0")


def start_repark(warehouse: Path) -> Any:
    """Start a v3-enabled RePark session with a memory catalog rooted at ``warehouse``."""
    from repark import ReparkSession

    shutil.rmtree(warehouse, ignore_errors=True)
    warehouse.mkdir(parents=True)
    spark = (
        ReparkSession.builder.appName("ice-tsns-sql-1-record")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


def ns_cast(identifier: int) -> str:
    """Return the SQL-door expression that writes literal ``identifier`` as ``timestamp_ns``."""
    text = LITERALS[identifier]
    if identifier in MICRO_LITERAL_IDS:
        return f"TIMESTAMP '{text}'"
    if identifier in STRING_LITERAL_IDS:
        return f"'{text}'"
    return f"CAST('{text}' AS timestamp_ns)"


def tz_cast(identifier: int) -> str:
    """Return the SQL-door expression that writes literal ``identifier`` as ``timestamptz_ns``."""
    text = LITERALS[identifier]
    if identifier in MICRO_LITERAL_IDS:
        return f"TIMESTAMP '{text}'"
    if identifier in STRING_LITERAL_IDS:
        return f"'{text}'"
    return f"CAST('{text}+00:00' AS timestamptz_ns)"


def sql_statements() -> dict[str, list[str]]:
    """Return the SQL-door statements that build each table, in execution order."""
    days_rows = ", ".join(f"({i}, {ns_cast(i)}, {tz_cast(i)})" for i in sorted(LITERALS))
    hours_rows = " UNION ALL ".join(
        f"SELECT {i} AS id, CAST('{LITERALS[i]}' AS timestamptz_ns) AS tz" for i in HOURS_IDS
    )
    overwrite_rows = ", ".join(f"({i}, {ns_cast(i)}, {tz_cast(i)})" for i in OVERWRITE_IDS)
    merge_source = " UNION ALL ".join(
        f"SELECT {i} AS id, CAST('{LITERALS[i]}' AS timestamp_ns) AS ts, "
        f"CAST('{LITERALS[i]}' AS timestamptz_ns) AS tz"
        for i in (*MERGE_INSERT_IDS, MERGE_UPDATE_SOURCE_ID)
    )
    ctas_rows = " UNION ALL ".join(
        f"SELECT {i} AS id, CAST('{LITERALS[i]}' AS timestamp_ns) AS ts, "
        f"CAST('{LITERALS[i]}' AS timestamptz_ns) AS tz"
        for i in CTAS_IDS
    )
    columns = "(id INT, ts timestamp_ns, tz timestamptz_ns)"
    return {
        "sql_days": [
            f"CREATE TABLE ice.ns.sql_days {columns} USING iceberg PARTITIONED BY (days(ts)) {V3}",
            f"INSERT INTO ice.ns.sql_days VALUES {days_rows}",
        ],
        "sql_hours": [
            f"CREATE TABLE ice.ns.sql_hours (id INT, tz timestamptz_ns) USING iceberg "
            f"PARTITIONED BY (hours(tz)) {V3}",
            f"INSERT INTO ice.ns.sql_hours {hours_rows}",
        ],
        "sql_overwrite": [
            f"CREATE TABLE ice.ns.sql_overwrite {columns} USING iceberg {V3}",
            f"INSERT OVERWRITE ice.ns.sql_overwrite VALUES {overwrite_rows}",
        ],
        "sql_merge": [
            f"CREATE TABLE ice.ns.sql_merge {columns} USING iceberg {V3}",
            f"INSERT INTO ice.ns.sql_merge VALUES ({MERGE_UPDATE_ID}, "
            f"TIMESTAMP '{LITERALS[4]}', TIMESTAMP '{LITERALS[4]}')",
            f"MERGE INTO ice.ns.sql_merge t USING ({merge_source}) s "
            f"ON t.id = s.id - {MERGE_UPDATE_SOURCE_ID - MERGE_UPDATE_ID} "
            f"WHEN MATCHED THEN UPDATE SET t.ts = s.ts, t.tz = s.tz "
            f"WHEN NOT MATCHED THEN INSERT (id, ts, tz) VALUES (s.id, s.ts, s.tz)",
        ],
        "sql_ctas": [
            f"CREATE TABLE ice.ns.sql_ctas USING iceberg {V3} AS {ctas_rows}",
        ],
    }


def write_controls(spark: Any) -> None:
    """Write the DataFrame-door control table from pandas ``datetime64[ns]`` columns."""
    import pandas as pd

    ids = sorted(LITERALS)
    texts = [LITERALS[i] for i in ids]
    spark.sql(
        "CREATE TABLE ice.ns.ctl_days (id INT, ts timestamp_ns, tz timestamptz_ns) USING iceberg "
        f"PARTITIONED BY (days(ts)) {V3}"
    ).collect()
    days = pd.DataFrame(
        {
            "id": pd.array(ids, dtype="int32"),
            "ts": pd.to_datetime(texts, format="ISO8601"),
            "tz": pd.to_datetime(texts, format="ISO8601").tz_localize("UTC"),
        }
    )
    spark.createDataFrame(days).writeTo("ice.ns.ctl_days").append()


def hours_control_attempt(spark: Any) -> dict[str, Any]:
    """Try the DataFrame-door ``hours(tz)`` control and record RePark's answer."""
    import pandas as pd

    spark.sql(
        f"CREATE TABLE ice.ns.ctl_hours (id INT, tz timestamptz_ns) USING iceberg "
        f"PARTITIONED BY (hours(tz)) {V3}"
    ).collect()
    hours = pd.DataFrame(
        {
            "id": pd.array(list(HOURS_IDS), dtype="int32"),
            "tz": pd.to_datetime([LITERALS[i] for i in HOURS_IDS], format="ISO8601").tz_localize(
                "UTC"
            ),
        }
    )
    try:
        spark.createDataFrame(hours).writeTo("ice.ns.ctl_hours").append()
    except Exception as exc:
        return {"ok": False, "type": type(exc).__name__, "message": str(exc)[:400]}
    return {"ok": True}


def json_safe(value: Any) -> Any:
    """Render a collected cell JSON-safe: structs field by field, dates and instants as text."""
    if isinstance(value, dict):
        return {key: json_safe(inner) for key, inner in value.items()}
    if value is None or isinstance(value, (int, str)):
        return value
    return str(value)


def repark_rows(spark: Any, sql: str) -> list[dict[str, Any]]:
    """Collect ``sql`` as JSON-safe rows."""
    return [
        {key: json_safe(value) for key, value in row.items()}
        for row in spark.sql(sql).to_arrow().to_pylist()
    ]


def run_statement(spark: Any, sql: str) -> dict[str, Any]:
    """Run one SQL-door statement and record success or the exception class and text."""
    try:
        spark.sql(sql).collect()
    except Exception as exc:
        return {"sql": sql, "ok": False, "type": type(exc).__name__, "message": str(exc)[:400]}
    return {"sql": sql, "ok": True}


def command_write(warehouse: Path) -> int:
    """RePark half: write controls and SQL-door tables, then save RePark's answers."""
    spark = start_repark(warehouse)
    try:
        write_controls(spark)
        answers: dict[str, Any] = {
            "control_partitions": {
                "ctl_days": repark_rows(
                    spark,
                    "SELECT partition, record_count FROM ice.ns.ctl_days.partitions "
                    "ORDER BY partition",
                ),
            },
            "micro_render_anchor": repark_rows(
                spark,
                "SELECT "
                + ", ".join(
                    f"CAST(TIMESTAMP '{LITERALS[i]}' AS STRING) AS l{i}" for i in (4, 6, 7)
                ),
            )[0],
            "hours_control": hours_control_attempt(spark),
            "sql_door": {},
        }
        for table, statements in sql_statements().items():
            answers["sql_door"][table] = [run_statement(spark, sql) for sql in statements]
    finally:
        spark.stop()
    (warehouse / "repark_answers.json").write_text(json.dumps(answers, indent=2) + "\n")
    print(json.dumps(answers["sql_door"], indent=2))
    return 0


def latest_metadata(warehouse: Path, table: str) -> str:
    """Return the newest ``*.metadata.json`` path of ``table`` under the warehouse."""
    candidates = [
        path for path in warehouse.rglob("*.metadata.json") if path.parent.parent.name == table
    ]
    if not candidates:
        raise FileNotFoundError(f"no metadata for {table} under the warehouse")
    return str(max(candidates, key=lambda path: int(path.name.split("-", 1)[0])))


def read_back(warehouse: Path, table: str) -> dict[str, Any]:
    """Read one table with PyIceberg ``StaticTable``: types, spec, int64-ns values, partitions."""
    import pyarrow as pa
    from pyiceberg.table import StaticTable

    static = StaticTable.from_metadata(latest_metadata(warehouse, table))
    schema = static.schema()
    arrow = static.scan().to_arrow().sort_by("id")
    rows: list[dict[str, Any]] = []
    columns = {
        name: arrow.column(name).cast(pa.int64()).to_pylist()
        for name in arrow.column_names
        if name != "id"
    }
    for index, identifier in enumerate(arrow.column("id").to_pylist()):
        row: dict[str, Any] = {"id": identifier}
        for name, values in columns.items():
            row[name] = values[index]
        rows.append(row)
    partitions = sorted(
        (
            {
                "partition": {key: str(value) for key, value in part["partition"].items()},
                "record_count": part["record_count"],
            }
            for part in static.inspect.partitions().to_pylist()
        ),
        key=lambda part: json.dumps(part["partition"], sort_keys=True),
    )
    return {
        "format_version": static.metadata.format_version,
        "schema": {field.name: str(field.field_type) for field in schema.fields},
        "spec": [
            {
                "name": field.name,
                "source": schema.find_column_name(field.source_id),
                "transform": str(field.transform),
            }
            for field in static.spec().fields
        ],
        "rows": rows,
        "partitions": partitions,
    }


def subset(
    read: dict[str, Any], ids: tuple[int, ...], columns: tuple[str, ...]
) -> list[dict[str, Any]]:
    """Return the control rows for ``ids`` projected to ``id`` plus ``columns``."""
    by_id = {row["id"]: row for row in read["rows"]}
    return [{"id": i, **{name: by_id[i][name] for name in columns}} for i in ids]


def spec_hours(days: dict[str, Any]) -> dict[str, Any]:
    """Derive the ``hours(tz)`` read-back from the control values and the spec's hour transform."""
    rows = subset(days, HOURS_IDS, ("tz",))
    counts: dict[int, int] = {}
    for row in rows:
        hour = row["tz"] // (3600 * NS_PER_SECOND)
        counts[hour] = counts.get(hour, 0) + 1
    return {
        "schema": {"id": "int", "tz": "timestamptz_ns"},
        "spec": [{"name": "tz_hour", "source": "tz", "transform": "hour"}],
        "rows": rows,
        "partitions": [
            {"partition": {"tz_hour": str(hour)}, "record_count": count}
            for hour, count in sorted(counts.items())
        ],
    }


def expected_sql_tables(days: dict[str, Any]) -> dict[str, Any]:
    """Derive every SQL-door table's expected read-back from the DataFrame-door control."""
    both = ("ts", "tz")
    merged = subset(days, (MERGE_UPDATE_ID, *MERGE_INSERT_IDS), both)
    update = subset(days, (MERGE_UPDATE_SOURCE_ID,), both)[0]
    merged[0] = {"id": MERGE_UPDATE_ID, "ts": update["ts"], "tz": update["tz"]}
    types = {"id": "int", "ts": "timestamp_ns", "tz": "timestamptz_ns"}
    return {
        "sql_days": {key: days[key] for key in ("schema", "spec", "rows", "partitions")},
        "sql_hours": spec_hours(days),
        "sql_overwrite": {"schema": types, "spec": [], "rows": subset(days, OVERWRITE_IDS, both)},
        "sql_merge": {"schema": types, "spec": [], "rows": merged},
        "sql_ctas": {"schema": types, "spec": [], "rows": subset(days, CTAS_IDS, both)},
    }


def build_fixture(warehouse: Path) -> dict[str, Any]:
    """Assemble the fixture from the PyIceberg control read-back and the derived expectations."""
    import pyiceberg

    answers = json.loads((warehouse / "repark_answers.json").read_text())
    days = read_back(warehouse, "ctl_days")
    literal_rows = {
        str(i): {
            "text": text,
            "wall_ns": wall_ns(text),
            "render": render_ns(wall_ns(text)),
            "foreign_zone_instant_ns": zoned_instant_ns(text, FOREIGN_ZONE),
        }
        for i, text in LITERALS.items()
    }
    for identifier, row in literal_rows.items():
        control = next(r for r in days["rows"] if r["id"] == int(identifier))
        assert control["ts"] == row["wall_ns"] == control["tz"], (identifier, control, row)
    return {
        "unit": "ICE-TSNS-SQL-1",
        "ruling": "Q-21c-6",
        "oracle": {
            "spec": "Iceberg table spec v3: timestamp_ns / timestamptz_ns are int64 nanoseconds",
            "second_engine": f"pyiceberg {pyiceberg.__version__} StaticTable read-back",
            "spark": "Spark 4.1.2 cannot read or write timestamp_ns",
        },
        "session_zone": SESSION_ZONE,
        "foreign_zone": FOREIGN_ZONE,
        "literals": literal_rows,
        "micro_render_anchor": answers["micro_render_anchor"],
        "control": {"ctl_days": days},
        "hours_control": answers["hours_control"],
        "hours_derivation": HOURS_DERIVATION,
        "control_repark_partitions": answers["control_partitions"],
        "sql_tables": expected_sql_tables(days),
        "sql_statements": sql_statements(),
    }


def command_record(warehouse: Path, output: str) -> int:
    """PyIceberg half: print the fixture JSON and optionally write it to ``output``."""
    text = json.dumps(build_fixture(warehouse), indent=2, sort_keys=True) + "\n"
    if output:
        Path(output).write_text(text)
    sys.stdout.write(text)
    return 0


def command_check(warehouse: Path, fixture_path: str) -> int:
    """PyIceberg half: read every SQL-door table and compare it with the fixture."""
    fixture = json.loads(Path(fixture_path).read_text())
    failures = 0
    for table, expected in fixture["sql_tables"].items():
        try:
            actual = read_back(warehouse, table)
        except Exception as exc:
            print(f"FAIL {table}: unreadable ({type(exc).__name__}: {exc})")
            failures += 1
            continue
        for key, value in expected.items():
            if actual[key] != value:
                print(f"FAIL {table}.{key}: expected {value!r} got {actual[key]!r}")
                failures += 1
            else:
                print(f"ok   {table}.{key}")
        if actual["format_version"] != 3:
            print(f"FAIL {table}.format_version: {actual['format_version']}")
            failures += 1
    print(f"{failures} failure(s)")
    return 1 if failures else 0


def main(argv: list[str]) -> int:
    """Dispatch the subcommand."""
    args = parse_args(argv)
    warehouse = Path(args.warehouse)
    if args.command == "write":
        return command_write(warehouse)
    if args.command == "record":
        return command_record(warehouse, args.output)
    return command_check(warehouse, args.fixture)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
