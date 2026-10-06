"""D-M2 JDBC oracle: Spark 4.1.2 reads of Postgres types recorded verbatim."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

SCHEMA = "dm2"
DRIVER_CLASS = "org.postgresql.Driver"
TZ_NY = "America/New_York"
TZ_UTC = "UTC"
URL_ENV = "DM2_PG_URL"
JAR_ENV = "DM2_PGJDBC_JAR"
WAREHOUSE_ENV = "DM2_WAREHOUSE"
OUT_ENV = "DM2_OUT"
JARS_DIR = Path("/tmp/oc-worker/direct/wo/connect-1-6/d-m2/jars")
EXPECTED_CELLS = 58
SETUP_PREAMBLE = ("DROP SCHEMA IF EXISTS dm2 CASCADE", "CREATE SCHEMA dm2")
T11_LITERAL = "'2024-06-15 12:34:56.123456'"
V07_LITERAL = "'2024-03-10 02:30:00 America/New_York'"
V09_LITERAL = "'2024-11-03 01:30:00 America/New_York'"

SINGLE_TYPES: tuple[tuple[str, str, str], ...] = (
    ("T02", "numeric", "'123456789.987654321'"),
    ("T03", "numeric(10,2)", "'12345678.90'"),
    ("T04", "numeric(38,10)", "'1234567890123456789012345678.1234567890'"),
    ("T05", "numeric(39,1)", "'12345678901234567890123456789012345678.5'"),
    ("T06", "numeric(50,10)", "'12345678901234567890123456789012345678.12'"),
    ("T07", "numeric(50,45)", "'1.123456789012345678901234567890123456789012345'"),
    ("T08", "numeric(1000,40)", "'1.5'"),
    ("T09", "numeric(3,5)", "'0.00123'"),
    ("T10", "numeric(5,-2)", "'12300'"),
    ("T15", "uuid", "'123e4567-e89b-12d3-a456-426614174000'"),
    ("T16", "json", '\'{"a":1,"b":[true,null],"s":"x"}\''),
    ("T17", "jsonb", '\'{"a":1,"b":[true,null],"s":"x"}\''),
    ("T20", "int2", "1234"),
    ("T21", "int4", "123456"),
    ("T22", "int8", "123456789012"),
    ("T23", "float4", "1.5"),
    ("T24", "float8", "2.5"),
    ("T25", "bool", "true"),
    ("T26", "text", "'hello'"),
    ("T27", "varchar(5)", "'abcde'"),
    ("T28", "bpchar(5)", "'ab   '"),
    ("T29", "bytea", "'\\xdeadbeef'"),
)

DOUBLE_TYPES: tuple[tuple[str, str, str], ...] = (
    ("T01", "time", "'12:34:56.123456'"),
    ("T11", "timestamp", T11_LITERAL),
    ("T13", "timestamptz", "'2024-06-15 12:34:56.123456+02:00'"),
    ("T14", "interval", "'1 year 2 months 3 days 04:05:06.789'"),
    ("T30", "date", "'2024-06-15'"),
)


class Pg:
    __slots__ = ("dsn", "jdbc_url", "password", "user")

    def __init__(self, dsn: str, jdbc_url: str, user: str, password: str) -> None:
        self.dsn = dsn
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password


class CellSpec:
    __slots__ = ("code", "creates", "dbtable", "limit", "options", "setup", "tz")

    def __init__(
        self,
        code: str,
        tz: str,
        setup: tuple[str, ...],
        dbtable: str,
        options: tuple[tuple[str, str], ...],
        limit: int | None,
        creates: bool,
    ) -> None:
        self.code = code
        self.tz = tz
        self.setup = setup
        self.dbtable = dbtable
        self.options = options
        self.limit = limit
        self.creates = creates


class Ctx:
    __slots__ = ("pg", "scrubs", "spark", "warehouse")

    def __init__(self, spark: SparkSession, pg: Pg, warehouse: Path) -> None:
        self.spark = spark
        self.pg = pg
        self.warehouse = warehouse
        pairs = (
            (pg.jdbc_url, "<pg-url>"),
            (pg.password, "<pg-password>"),
            (str(warehouse), "$WAREHOUSE"),
            (str(Path(__file__).resolve()), "$RECORDER"),
        )
        self.scrubs = tuple(pair for pair in pairs if pair[0])


def table_name(code: str) -> str:
    return SCHEMA + ".t_" + code.lower()


def one_table(code: str, column_type: str, literals: tuple[str, ...]) -> tuple[str, ...]:
    table = table_name(code)
    values = ", ".join("(" + literal + ")" for literal in literals)
    return (
        "CREATE TABLE " + table + " (v " + column_type + ")",
        "INSERT INTO " + table + " (v) VALUES " + values,
    )


def simple_cells(
    code: str,
    column_type: str,
    literals: tuple[str, ...],
    zones: tuple[str, ...],
) -> tuple[CellSpec, ...]:
    setup = one_table(code, column_type, literals)
    dbtable = table_name(code)
    return tuple(
        CellSpec(code, zone, setup, dbtable, (), None, index == 0)
        for index, zone in enumerate(zones)
    )


def define_cells() -> tuple[CellSpec, ...]:
    ny = (TZ_NY,)
    both = (TZ_NY, TZ_UTC)
    cells: list[CellSpec] = []
    for code, column_type, literal in SINGLE_TYPES:
        cells.extend(simple_cells(code, column_type, (literal,), ny))
    for code, column_type, literal in DOUBLE_TYPES:
        cells.extend(simple_cells(code, column_type, (literal,), both))
    t11 = one_table("T11", "timestamp", (T11_LITERAL,))
    ntz = (("preferTimestampNTZ", "true"),)
    cells.append(CellSpec("T12", TZ_NY, t11, table_name("T11"), ntz, None, False))
    cells.append(CellSpec("T12", TZ_UTC, t11, table_name("T11"), ntz, None, False))
    mood = (
        "CREATE TYPE dm2.mood AS ENUM ('sad', 'ok')",
        "CREATE TABLE dm2.t_t18 (v dm2.mood)",
        "INSERT INTO dm2.t_t18 (v) VALUES ('ok')",
    )
    cells.append(CellSpec("T18", TZ_NY, mood, "dm2.t_t18", (), None, True))
    pos = (
        "CREATE DOMAIN dm2.pos AS int4 CHECK (VALUE > 0)",
        "CREATE TABLE dm2.t_t19 (v dm2.pos)",
        "INSERT INTO dm2.t_t19 (v) VALUES (7)",
    )
    cells.append(CellSpec("T19", TZ_NY, pos, "dm2.t_t19", (), None, True))
    infinities = ("'infinity'", "'-infinity'")
    cells.extend(simple_cells("V01", "date", infinities, both))
    cells.extend(simple_cells("V02", "timestamp", infinities, both))
    cells.extend(simple_cells("V03", "timestamptz", infinities, both))
    cells.extend(simple_cells("V04", "numeric", ("'NaN'",), ny))
    cells.extend(simple_cells("V05", "numeric", ("'Infinity'", "'-Infinity'"), ny))
    cells.extend(simple_cells("V06", "timestamp", ("'2024-03-10 02:30:00'",), both))
    cells.extend(simple_cells("V07", "timestamptz", (V07_LITERAL,), both))
    cells.extend(simple_cells("V08", "timestamp", ("'2024-11-03 01:30:00'",), both))
    cells.extend(simple_cells("V09", "timestamptz", (V09_LITERAL,), both))
    cells.extend(simple_cells("V10", "time", ("'24:00:00'",), both))
    mixed = (
        'CREATE TABLE dm2."MixedCase" (v int4)',
        'INSERT INTO dm2."MixedCase" (v) VALUES (42)',
    )
    cells.append(CellSpec("S01", TZ_NY, mixed, 'dm2."MixedCase"', (), None, True))
    cells.append(CellSpec("S02", TZ_NY, mixed, "dm2.MixedCase", (), None, False))
    limit_setup = (
        "CREATE TABLE dm2.t_limit100 (v int4)",
        "INSERT INTO dm2.t_limit100 (v) SELECT g FROM generate_series(1, 100) g",
    )
    cells.append(CellSpec("S03", TZ_NY, limit_setup, "dm2.t_limit100", (), 3, True))
    unknown_setup = one_table("S04", "int4", ("1",))
    odd_option = (("noSuchOption", "x"),)
    cells.append(CellSpec("S04", TZ_NY, unknown_setup, table_name("S04"), odd_option, None, True))
    return tuple(cells)


def run_psql(dsn: str, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", dsn, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", script],
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )


def run_setup(dsn: str, setup: tuple[str, ...]) -> str | None:
    script = ";\n".join(setup) + ";"
    try:
        done = run_psql(dsn, script)
    except Exception as exc:
        return type(exc).__name__ + ": " + str(exc)
    if done.returncode != 0:
        return (done.stderr or done.stdout or "psql failed").strip()
    return None


def pg_version(dsn: str) -> str | None:
    try:
        done = subprocess.run(
            ["psql", dsn, "-X", "-q", "-t", "-A", "-c", "SHOW server_version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
    except Exception:
        return None
    if done.returncode != 0:
        return None
    return done.stdout.strip() or None


def build_session(warehouse: Path, jar: Path) -> SparkSession:
    from pyspark.sql import SparkSession

    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    return (
        SparkSession.builder.master("local[2]")
        .appName("c2-jdbc-oracle")
        .config("spark.jars", str(jar))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", TZ_NY)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_frame(ctx: Ctx, spec: CellSpec) -> DataFrame:
    reader = (
        ctx.spark.read.format("jdbc")
        .option("url", ctx.pg.jdbc_url)
        .option("user", ctx.pg.user)
        .option("password", ctx.pg.password)
        .option("driver", DRIVER_CLASS)
        .option("dbtable", spec.dbtable)
    )
    for key, value in spec.options:
        reader = reader.option(key, value)
    return reader.load()


def call_or_none(target: Any, name: str) -> Any:
    call = getattr(target, name, None)
    if call is None:
        return None
    try:
        return call()
    except Exception:
        return None


def error_of(exc: Exception) -> dict[str, Any]:
    java_exc = getattr(exc, "java_exception", None)
    targets = (exc,) if java_exc is None else (exc, java_exc)
    error_class: Any = None
    sqlstate: Any = None
    for target in targets:
        if error_class is None:
            error_class = call_or_none(target, "getErrorClass")
        if error_class is None:
            error_class = call_or_none(target, "getCondition")
        if sqlstate is None:
            sqlstate = call_or_none(target, "getSqlState")
    return {
        "class": type(exc).__module__ + "." + type(exc).__name__,
        "error_class": error_class,
        "message": str(exc)[:400],
        "sqlstate": sqlstate,
    }


def read_cell(ctx: Ctx, spec: CellSpec) -> tuple[dict[str, Any], DataFrame | None]:
    ctx.spark.conf.set("spark.sql.session.timeZone", spec.tz)
    try:
        frame = load_frame(ctx, spec)
        if spec.limit is not None:
            frame = frame.limit(spec.limit)
    except Exception as exc:
        return {"error": error_of(exc), "rows": None, "schema": None}, None
    try:
        schema = frame.schema.simpleString()
    except Exception as exc:
        return {"error": error_of(exc), "rows": None, "schema": None}, None
    try:
        rows = [repr(row) for row in frame.collect()]
    except Exception as exc:
        return {"error": error_of(exc), "rows": None, "schema": schema}, None
    return {"error": None, "rows": rows, "schema": schema}, frame


def explain_of(ctx: Ctx, frame: DataFrame) -> str | None:
    try:
        utils = ctx.spark._jvm.PythonSQLUtils
        return str(utils.explainString(frame._jdf.queryExecution(), "formatted"))
    except Exception:
        return None


def pushdown_of(ctx: Ctx, spec: CellSpec, explain: str | None) -> dict[str, Any]:
    pushed: bool | None = None
    if explain is not None:
        for line in explain.splitlines():
            if line.strip().startswith("External engine query:"):
                pushed = re.search(r"\blimit\b", line, re.IGNORECASE) is not None
    value: bool | None = None
    reflection_error: str | None = None
    try:
        base = load_frame(ctx, spec)
        relation = base._jdf.queryExecution().analyzed().relation()
        value = bool(relation.jdbcOptions().pushDownLimit())
    except Exception as exc:
        reflection_error = str(exc)[:400]
    return {
        "jdbc_options_value": value,
        "plan_shows_pushed_limit": pushed,
        "reflection_error": reflection_error,
    }


def scrub(text: str, ctx: Ctx) -> str:
    for secret, token in ctx.scrubs:
        text = text.replace(secret, token)
    return text


def record_cell(ctx: Ctx, spec: CellSpec, created: set[str], cell_id: str) -> dict[str, Any]:
    read: dict[str, Any] = {
        "dbtable": spec.dbtable,
        "driver": DRIVER_CLASS,
        "url": "<pg-url>",
    }
    if spec.options:
        read["options"] = dict(spec.options)
    if spec.limit is not None:
        read["limit"] = spec.limit
    entry: dict[str, Any] = {
        "error": None,
        "id": cell_id,
        "read": read,
        "rows": None,
        "schema": None,
        "sql_setup": list(spec.setup),
        "tz": spec.tz,
    }
    if spec.creates and spec.dbtable not in created:
        failure = run_setup(ctx.pg.dsn, spec.setup)
        if failure is not None:
            entry["error"] = {
                "class": "SetupError",
                "error_class": None,
                "message": failure[:400],
                "sqlstate": None,
            }
            return entry
        created.add(spec.dbtable)
    answer, frame = read_cell(ctx, spec)
    entry["error"] = answer["error"]
    entry["rows"] = answer["rows"]
    entry["schema"] = answer["schema"]
    if spec.code == "S03":
        explain = explain_of(ctx, frame) if frame is not None else None
        entry["explain"] = explain
        entry["push_down_limit"] = pushdown_of(ctx, spec, explain)
    return entry


def record_all(ctx: Ctx) -> dict[str, dict[str, Any]]:
    created: set[str] = set()
    recorded: dict[str, dict[str, Any]] = {}
    for spec in define_cells():
        cell_id = "DM2-" + spec.code + "@" + spec.tz
        entry = record_cell(ctx, spec, created, cell_id)
        recorded[cell_id] = json.loads(scrub(json.dumps(entry, sort_keys=True), ctx))
        print(json.dumps(recorded[cell_id], sort_keys=True), flush=True)
    return recorded


def resolve_jar() -> Path | None:
    override = os.environ.get(JAR_ENV, "")
    if override:
        candidate = Path(override)
        return candidate if candidate.is_file() else None
    if not JARS_DIR.is_dir():
        return None
    jars = sorted(JARS_DIR.glob("postgresql-*.jar"))
    if len(jars) != 1:
        return None
    return jars[0]


def jar_info(jar: Path) -> tuple[str, str]:
    version = jar.name.removeprefix("postgresql-").removesuffix(".jar")
    return version, hashlib.sha256(jar.read_bytes()).hexdigest()


def fresh_dir(variable: str, prefix: str) -> Path:
    configured = os.environ.get(variable)
    path = Path(configured) if configured else Path(tempfile.mkdtemp(prefix="dm2-" + prefix + "-"))
    path.mkdir(parents=True, exist_ok=True)
    if any(path.iterdir()):
        raise SystemExit(variable + "=" + str(path) + " must be empty")
    return path.resolve()


def skip(reason: str) -> int:
    print("SKIP c2_jdbc_oracle: " + reason, file=sys.stderr, flush=True)
    return 0


def main() -> int:
    dsn = os.environ.get(URL_ENV, "")
    if not dsn:
        return skip(URL_ENV + " is unset")
    if shutil.which("psql") is None:
        return skip("psql is not on PATH")
    jar = resolve_jar()
    if jar is None:
        return skip("pgjdbc jar is missing (set " + JAR_ENV + ")")
    if find_spec("pyspark") is None:
        return skip("pyspark is not installed")
    warehouse = fresh_dir(WAREHOUSE_ENV, "warehouse")
    out = Path(os.environ.get(OUT_ENV) or Path(__file__).with_suffix(".json"))
    server = pg_version(dsn)
    if server is None:
        return skip("cannot reach Postgres through " + URL_ENV)
    schema_error = run_setup(dsn, SETUP_PREAMBLE)
    if schema_error is not None:
        return skip("cannot prepare schema dm2: " + schema_error[:200])
    parts = urlparse(dsn)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or 5432
    jdbc_url = "jdbc:postgresql://" + host + ":" + str(port) + parts.path
    pg = Pg(dsn, jdbc_url, parts.username or "", parts.password or "")
    spark = build_session(warehouse, jar)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ctx = Ctx(spark, pg, warehouse)
        recorded = record_all(ctx)
        version, sha256 = jar_info(jar)
        document = {
            "cells": list(recorded.values()),
            "date": datetime.now(UTC).date().isoformat(),
            "jvm_timezone": str(spark._jvm.java.util.TimeZone.getDefault().getID()),
            "pgjdbc_sha256": sha256,
            "pgjdbc_version": version,
            "postgres": server,
            "setup_preamble": list(SETUP_PREAMBLE),
            "spark": spark.version,
        }
    finally:
        spark.stop()
    out.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return 0 if len(document["cells"]) == EXPECTED_CELLS else 1


if __name__ == "__main__":
    sys.exit(main())
