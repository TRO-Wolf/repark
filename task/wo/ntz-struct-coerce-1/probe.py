"""Measure mixed struct timestamp coercion on one engine.

The inventory is closed: three session zones, a wall struct and an instant struct
plus their top-level twins, four mixing shapes, and six uses. One JSON object
per cell goes to stdout's destination path. Paths and the Iceberg jar come from
the environment or the process temporary directory, never from a literal here.

Run ``python probe.py repark <out.jsonl>`` or ``python probe.py spark <out.jsonl>``.
``python probe.py summarize <repark.jsonl> <spark.jsonl> <summary.md>`` writes
the comparison table.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ZONES: tuple[str, ...] = ("UTC", "America/New_York", "Asia/Kolkata")
WALL: str = "2026-01-02 03:04:05.123456"
INSTANT: str = "2026-07-04 11:22:33.654321"
SHAPES: tuple[str, ...] = ("case", "coalesce", "union", "cast")
USES: tuple[str, ...] = ("read", "compare", "order", "group", "join", "write")
LEVELS: tuple[str, ...] = ("struct", "top")


def epoch_micros(value: datetime) -> int:
    """Return UTC epoch microseconds for an aware or naive datetime.

    A naive value is read as a UTC wall, which is the tick a naive timestamp
    stores. An aware value is converted to UTC first.
    """
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    utc = aware.astimezone(timezone.utc)
    delta = utc - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86_400 * 1_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def localized_wall_micros(zone: str) -> int:
    """Return the session-zone instant of the inventory wall, in microseconds."""
    local = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=ZoneInfo(zone))
    return epoch_micros(local)


def wall_as_utc_micros() -> int:
    """Return the inventory wall read as UTC, in microseconds."""
    return epoch_micros(datetime(2026, 1, 2, 3, 4, 5, 123456))


def instant_micros() -> int:
    """Return the inventory instant, in microseconds."""
    return epoch_micros(datetime(2026, 7, 4, 11, 22, 33, 654321, tzinfo=timezone.utc))


def redact(text: str) -> str:
    """Drop absolute temporary and home paths from a recorded error line."""
    roots = (tempfile.gettempdir(), str(Path.home()))
    cleaned: list[str] = []
    for token in text.split():
        if any(root and (token == root or token.startswith(root + os.sep)) for root in roots):
            cleaned.append("<path>")
        else:
            cleaned.append(token)
    return " ".join(cleaned)


def error_record(exc: BaseException) -> tuple[str, str]:
    """Return an error class and the redacted first line."""
    message = redact(str(exc).strip().splitlines()[0] if str(exc).strip() else exc.__class__.__name__)
    if len(message) > 400:
        message = message[:400]
    bracket = ""
    start = message.find("[")
    end = message.find("]")
    if start != -1 and end > start:
        bracket = message[start : end + 1]
    kind = bracket if bracket else exc.__class__.__name__
    return kind, message


def wall_sql() -> str:
    """Return the wall literal spelling."""
    return f"TIMESTAMP_NTZ '{WALL}'"


def instant_sql() -> str:
    """Return the instant literal spelling."""
    return f"TIMESTAMP '{INSTANT}+00:00'"


def fixed_struct_sql() -> str:
    """Return a struct literal of the inventory instant and n = 7."""
    return f"named_struct('v', {instant_sql()}, 'n', CAST(7 AS INT))"


def shape_sql(level: str, shape: str, names: dict[str, str]) -> str:
    """Return the mixing query for one level and shape."""
    if level == "struct" and shape == "case":
        return (
            f"SELECT a.id AS id, CASE WHEN a.id = 2 THEN b.st ELSE a.st END AS mixed "
            f"FROM {names['a']} a JOIN {names['b']} b ON a.id = b.id"
        )
    if level == "struct" and shape == "coalesce":
        return (
            f"SELECT a.id AS id, COALESCE(a.st, b.st) AS mixed "
            f"FROM {names['a']} a JOIN {names['b']} b ON a.id = b.id"
        )
    if level == "struct" and shape == "union":
        return (
            f"SELECT st AS mixed FROM {names['a']} "
            f"UNION ALL SELECT st AS mixed FROM {names['b']}"
        )
    if level == "struct" and shape == "cast":
        return (
            f"SELECT id, CAST(st AS STRUCT<v: TIMESTAMP, n: INT>) AS mixed FROM {names['a']}"
        )
    if level == "top" and shape == "case":
        return (
            f"SELECT a.id AS id, CASE WHEN a.id = 2 THEN b.v ELSE a.v END AS mixed "
            f"FROM {names['at']} a JOIN {names['bt']} b ON a.id = b.id"
        )
    if level == "top" and shape == "coalesce":
        return (
            f"SELECT a.id AS id, COALESCE(a.v, b.v) AS mixed "
            f"FROM {names['at']} a JOIN {names['bt']} b ON a.id = b.id"
        )
    if level == "top" and shape == "union":
        return (
            f"SELECT v AS mixed FROM {names['at']} "
            f"UNION ALL SELECT v AS mixed FROM {names['bt']}"
        )
    return f"SELECT id, CAST(v AS TIMESTAMP) AS mixed FROM {names['at']}"


def has_id(shape: str) -> bool:
    """Return whether the shape projects an id column."""
    return shape != "union"


def use_sql(level: str, shape: str, use: str, names: dict[str, str], dest: str) -> list[str]:
    """Return the statements for one use. The last statement is the read."""
    inner = shape_sql(level, shape, names)
    ident = "id, " if has_id(shape) else ""
    order_id = " ORDER BY id" if has_id(shape) else ""
    if use == "read":
        if level == "struct":
            return [f"SELECT {ident}mixed.v AS v FROM ({inner}) s{order_id}"]
        return [f"SELECT {ident}mixed AS v FROM ({inner}) s{order_id}"]
    if use == "compare":
        target = fixed_struct_sql() if level == "struct" else instant_sql()
        return [f"SELECT {ident}(mixed = {target}) AS same FROM ({inner}) s{order_id}"]
    if use == "order":
        if level == "struct":
            return [f"SELECT mixed.v AS v FROM ({inner}) s ORDER BY mixed.v"]
        return [f"SELECT mixed AS v FROM ({inner}) s ORDER BY mixed"]
    if use == "group":
        return [f"SELECT mixed AS g, count(*) AS c FROM ({inner}) s GROUP BY mixed"]
    if use == "join":
        if level == "struct":
            return [
                f"SELECT s.mixed.v AS v, b.id AS bid FROM ({inner}) s "
                f"JOIN {names['b']} b ON s.mixed = b.st ORDER BY v, bid"
            ]
        return [
            f"SELECT s.mixed AS v, b.id AS bid FROM ({inner}) s "
            f"JOIN {names['bt']} b ON s.mixed = b.v ORDER BY v, bid"
        ]
    value_type = "STRUCT<v: TIMESTAMP, n: INT>" if level == "struct" else "TIMESTAMP"
    value_name = "st" if level == "struct" else "v"
    if has_id(shape):
        create = (
            f"CREATE TABLE {dest} (id INT, {value_name} {value_type}) USING iceberg"
        )
        insert = f"INSERT INTO {dest} SELECT id, mixed FROM ({inner}) s"
        read_col = "st.v" if level == "struct" else "v"
        read = f"SELECT id, {read_col} AS v FROM {dest} ORDER BY id"
    else:
        create = f"CREATE TABLE {dest} ({value_name} {value_type}) USING iceberg"
        insert = f"INSERT INTO {dest} SELECT mixed FROM ({inner}) s"
        read_col = "st.v" if level == "struct" else "v"
        read = f"SELECT {read_col} AS v FROM {dest}"
    return [f"DROP TABLE IF EXISTS {dest}", create, insert, read]


def normalize_python(value: Any, naive_zone: str | None) -> Any:
    """Reduce one collected value to ticks, nulls, bools, ints, or text.

    Spark returns a ``TIMESTAMP`` as a naive datetime in the JVM zone, not
    UTC. ``naive_zone`` is that zone, and the tick is the UTC instant.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, datetime):
        if value.tzinfo is None and naive_zone is not None:
            local = value.replace(tzinfo=ZoneInfo(naive_zone))
            return {"tick": epoch_micros(local), "iso": value.isoformat(), "read_as": naive_zone}
        return {
            "tick": epoch_micros(value),
            "iso": value.isoformat(),
            "tz": None if value.tzinfo is None else str(value.tzinfo),
        }
    if isinstance(value, dict):
        return {str(key): normalize_python(item, naive_zone) for key, item in value.items()}
    if hasattr(value, "asDict"):
        return normalize_python(value.asDict(recursive=True), naive_zone)
    if isinstance(value, (list, tuple)):
        return [normalize_python(item, naive_zone) for item in value]
    return redact(repr(value))


def arrow_rows(table: Any) -> tuple[str, list[dict[str, Any]]]:
    """Return the Arrow type string of the value column and one dict per row."""
    import pyarrow as pa

    names = list(table.schema.names)
    type_text = ", ".join(f"{field.name}:{field.type}" for field in table.schema)
    columns: dict[str, list[Any]] = {}
    for name in names:
        array = table.column(name).combine_chunks()
        field = table.schema.field(name)
        if pa.types.is_timestamp(field.type):
            columns[name] = [
                None if item is None else int(item)
                for item in array.cast(pa.int64()).to_pylist()
            ]
        elif pa.types.is_struct(field.type) and field.type.get_field_index("v") >= 0:
            child = array.field("v")
            ticks = child.cast(pa.int64()).to_pylist() if pa.types.is_timestamp(child.type) else None
            structs = array.to_pylist()
            packed: list[Any] = []
            for index, item in enumerate(structs):
                if item is None:
                    packed.append(None)
                    continue
                entry = {str(key): normalize_python(val, None) for key, val in item.items()}
                if ticks is not None:
                    entry["v_tick"] = None if ticks[index] is None else int(ticks[index])
                packed.append(entry)
            columns[name] = packed
        else:
            columns[name] = [normalize_python(item, None) for item in array.to_pylist()]
    rows: list[dict[str, Any]] = []
    width = len(columns[names[0]]) if names else 0
    for index in range(width):
        rows.append({name: columns[name][index] for name in names})
    return type_text, rows


def spark_rows(frame: Any, naive_zone: str) -> tuple[str, list[dict[str, Any]]]:
    """Return the Spark type string and rows whose ticks are UTC instants."""
    schema = frame.schema
    type_text = ", ".join(f"{field.name}:{field.dataType.simpleString()}" for field in schema.fields)
    rows: list[dict[str, Any]] = []
    for collected in frame.collect():
        mapped = collected.asDict(recursive=True)
        rows.append({str(key): normalize_python(val, naive_zone) for key, val in mapped.items()})
    return type_text, rows


def jvm_zone(session: Any) -> str:
    """Return the JVM default zone Spark uses when it builds Python datetimes."""
    return str(session.sparkContext._jvm.java.time.ZoneId.systemDefault().toString())


def collect_query(
    engine: str, session: Any, sql: str, naive_zone: str | None
) -> tuple[str, list[dict[str, Any]]]:
    """Run one SELECT and return its type string and rows."""
    if engine == "repark":
        return arrow_rows(session.sql(sql).to_arrow())
    return spark_rows(session.sql(sql), naive_zone or "UTC")


def exec_sql(session: Any, sql: str) -> None:
    """Run one statement and discard its rows."""
    session.sql(sql).collect()


def setup_sql(names: dict[str, str]) -> list[str]:
    """Return the statements that create and fill the four source tables."""
    struct_wall = "STRUCT<v: TIMESTAMP_NTZ, n: INT>"
    struct_instant = "STRUCT<v: TIMESTAMP, n: INT>"
    statements = [
        f"CREATE NAMESPACE IF NOT EXISTS {names['ns']}",
    ]
    for key in ("a", "b", "at", "bt"):
        statements.append(f"DROP TABLE IF EXISTS {names[key]}")
    statements.extend(
        [
            f"CREATE TABLE {names['a']} (id INT, st {struct_wall}) USING iceberg",
            f"CREATE TABLE {names['b']} (id INT, st {struct_instant}) USING iceberg",
            f"CREATE TABLE {names['at']} (id INT, v TIMESTAMP_NTZ) USING iceberg",
            f"CREATE TABLE {names['bt']} (id INT, v TIMESTAMP) USING iceberg",
            (
                f"INSERT INTO {names['a']} SELECT 1, named_struct('v', {wall_sql()}, "
                f"'n', CAST(7 AS INT))"
            ),
            f"INSERT INTO {names['a']} SELECT 2, NULL",
            (
                f"INSERT INTO {names['b']} SELECT 1, named_struct('v', {instant_sql()}, "
                f"'n', CAST(7 AS INT))"
            ),
            (
                f"INSERT INTO {names['b']} SELECT 2, named_struct('v', {instant_sql()}, "
                f"'n', CAST(7 AS INT))"
            ),
            f"INSERT INTO {names['at']} SELECT 1, {wall_sql()}",
            f"INSERT INTO {names['at']} SELECT 2, CAST(NULL AS TIMESTAMP_NTZ)",
            f"INSERT INTO {names['bt']} SELECT 1, {instant_sql()}",
            f"INSERT INTO {names['bt']} SELECT 2, {instant_sql()}",
        ]
    )
    return statements


def names_for(catalog: str) -> dict[str, str]:
    """Return qualified table names for one catalog."""
    namespace = f"{catalog}.ns"
    return {
        "ns": namespace,
        "a": f"{namespace}.a",
        "b": f"{namespace}.b",
        "at": f"{namespace}.at",
        "bt": f"{namespace}.bt",
    }


def open_repark(zone: str, warehouse: Path) -> Any:
    """Open one facade session in ``zone`` with a memory catalog."""
    from repark.spark import ReparkSession
    from repark.spark.session.session_state import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("ntz-struct-coerce-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(warehouse))
    return session


def close_repark(session: Any) -> None:
    """Stop a facade session and clear the process-wide active handle."""
    from repark.spark.session.session_state import _reset_active_session_for_tests

    session.stop()
    _reset_active_session_for_tests()


def open_spark(zone: str) -> Any:
    """Open the live Spark 4.1.2 session with an in-memory Iceberg catalog."""
    from pyspark.sql import SparkSession

    jar = os.environ["NTZ_ICEBERG_JAR"]
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ntz-struct-coerce-1")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.mc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.mc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def cell_of(
    engine: str,
    session: Any,
    level: str,
    shape: str,
    use: str,
    zone: str,
    names: dict[str, str],
    naive_zone: str | None,
) -> dict[str, Any]:
    """Measure one inventory cell."""
    dest = f"{names['ns']}.w_{level}_{shape}"
    statements = use_sql(level, shape, use, names, dest)
    record: dict[str, Any] = {
        "level": level,
        "shape": shape,
        "use": use,
        "zone": zone,
        "ok": True,
        "type": "",
        "rows": [],
        "error_class": None,
        "error_line": None,
        "sql": statements[-1],
    }
    try:
        for statement in statements[:-1]:
            exec_sql(session, statement)
        type_text, rows = collect_query(engine, session, statements[-1], naive_zone)
        record["type"] = type_text
        record["rows"] = rows
    except Exception as exc:
        kind, line = error_record(exc)
        record["ok"] = False
        record["error_class"] = kind
        record["error_line"] = line
    return record


def explain_record(
    engine: str, session: Any, label: str, sql: str, naive_zone: str | None
) -> dict[str, Any]:
    """Capture every EXPLAIN row for one statement."""
    try:
        _type_text, rows = collect_query(engine, session, f"EXPLAIN {sql}", naive_zone)
        plans = []
        for row in rows:
            plan_type = str(row.get("plan_type", ""))
            plan = redact(str(row.get("plan", row)))
            plans.append({"plan_type": plan_type, "plan": plan})
        return {"label": label, "ok": True, "sql": sql, "plans": plans}
    except Exception as exc:
        kind, line = error_record(exc)
        return {"label": label, "ok": False, "sql": sql, "error_class": kind, "error_line": line}


def measure_zone(
    engine: str, session: Any, zone: str, names: dict[str, str], naive_zone: str | None
) -> list[dict[str, Any]]:
    """Create the tables and measure every cell of one zone."""
    for statement in setup_sql(names):
        try:
            exec_sql(session, statement)
        except Exception as exc:
            kind, line = error_record(exc)
            raise RuntimeError(f"setup failed {kind}: {line} :: {statement}") from exc
    records: list[dict[str, Any]] = []
    for level in LEVELS:
        for shape in SHAPES:
            for use in USES:
                records.append(cell_of(engine, session, level, shape, use, zone, names, naive_zone))
    return records


def seat_records(
    engine: str, session: Any, names: dict[str, str], naive_zone: str | None
) -> list[dict[str, Any]]:
    """Explain the coercion case, the user cast, and the top-level control."""
    case_sql = f"SELECT mixed FROM ({shape_sql('struct', 'case', names)}) s"
    cast_sql = shape_sql("struct", "cast", names)
    top_sql = f"SELECT mixed FROM ({shape_sql('top', 'case', names)}) s"
    return [
        explain_record(engine, session, "struct-case", case_sql, naive_zone),
        explain_record(engine, session, "struct-cast", cast_sql, naive_zone),
        explain_record(engine, session, "top-case", top_sql, naive_zone),
    ]


def run_repark(out_path: Path, seat_path: Path) -> None:
    """Measure RePark in each zone and write JSONL plus the seat explains."""
    cells: list[dict[str, Any]] = []
    seats: list[dict[str, Any]] = []
    for zone in ZONES:
        warehouse = Path(tempfile.mkdtemp(prefix="ntz-struct-"))
        session = open_repark(zone, warehouse)
        try:
            names = names_for("ice")
            cells.extend(measure_zone("repark", session, zone, names, None))
            if zone == "America/New_York":
                seats.extend(seat_records("repark", session, names, None))
        finally:
            close_repark(session)
    write_jsonl(out_path, cells)
    seat_path.write_text(json.dumps(seats, indent=1) + "\n", encoding="utf-8")


def run_spark(out_path: Path) -> None:
    """Measure Spark 4.1.2 in each zone on one session."""
    session = open_spark(ZONES[0])
    cells: list[dict[str, Any]] = []
    try:
        names = names_for("mc")
        naive = jvm_zone(session)
        for zone in ZONES:
            session.conf.set("spark.sql.session.timeZone", zone)
            measured = measure_zone("spark", session, zone, names, naive)
            for record in measured:
                record["jvm_zone"] = naive
            cells.extend(measured)
    finally:
        session.stop()
    write_jsonl(out_path, cells)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write one JSON object per line."""
    text = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(text, encoding="utf-8")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read JSONL cells."""
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def canon_value(value: Any) -> Any:
    """Collapse a Spark datetime object to its UTC tick."""
    if isinstance(value, dict) and "tick" in value and ("iso" in value or "read_as" in value):
        return value["tick"]
    if isinstance(value, dict) and "v_tick" in value:
        reduced = {
            key: canon_value(item)
            for key, item in value.items()
            if key not in ("v", "v_tick")
        }
        reduced["v"] = value["v_tick"]
        return reduced
    if isinstance(value, dict):
        return {str(key): canon_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [canon_value(item) for item in value]
    return value


def value_key(row: dict[str, Any]) -> Any:
    """Return the comparable payload of one cell, ignoring type spelling."""
    if not row["ok"]:
        return ("err", row["error_class"], row["error_line"])
    payload = canon_value(row["rows"])
    if row["use"] not in ("order", "join"):
        payload = sorted(payload, key=lambda item: json.dumps(item, sort_keys=True))
    return ("ok", json.dumps(payload, sort_keys=True))


def short_cell(row: dict[str, Any]) -> str:
    """Return one table cell of ticks or the error class."""
    if not row["ok"]:
        return f"ERR {row['error_class']}: {row['error_line']}"
    body = json.dumps(canon_value(row["rows"]), sort_keys=True, separators=(",", ":"))
    if len(body) > 180:
        body = body[:180] + "..."
    return f"{row['type']} {body}"


def summarize(repark_path: Path, spark_path: Path, summary_path: Path) -> None:
    """Write the comparison table and the wall-leaf reading."""
    repark = {
        (row["level"], row["shape"], row["use"], row["zone"]): row for row in read_jsonl(repark_path)
    }
    spark = {
        (row["level"], row["shape"], row["use"], row["zone"]): row for row in read_jsonl(spark_path)
    }
    keys = sorted(set(repark) | set(spark))
    differs = 0
    lines = [
        "# NTZ-STRUCT-COERCE-1 step 0",
        "",
        f"Wall as UTC ticks: `{wall_as_utc_micros()}`. Instant ticks: `{instant_micros()}`.",
        "",
        "| shape | use | zone | RePark | Spark | same or differs |",
        "|---|---|---|---|---|---|",
    ]
    for key in keys:
        level, shape, use, zone = key
        left = repark.get(key)
        right = spark.get(key)
        if left is None or right is None:
            verdict = "differs"
            left_text = "MISSING" if left is None else short_cell(left)
            right_text = "MISSING" if right is None else short_cell(right)
        else:
            verdict = "same" if value_key(left) == value_key(right) else "differs"
            left_text = short_cell(left).replace("|", "/")
            right_text = short_cell(right).replace("|", "/")
        if verdict == "differs":
            differs += 1
        lines.append(
            f"| {level}/{shape} | {use} | {zone} | {left_text} | {right_text} | {verdict} |"
        )
    lines.extend(["", f"Differing cells: {differs} of {len(keys)}.", ""])
    lines.append("## Wall leaf on struct case read and top case read")
    lines.append("")
    lines.append("| zone | localized | struct case id 1 | top case id 1 | struct cast id 1 |")
    lines.append("|---|---|---|---|---|")
    for zone in ZONES:
        localized = localized_wall_micros(zone)
        struct_tick = tick_from(repark.get(("struct", "case", "read", zone)), spark.get(("struct", "case", "read", zone)))
        top_tick = tick_from(repark.get(("top", "case", "read", zone)), spark.get(("top", "case", "read", zone)))
        cast_tick = tick_from(repark.get(("struct", "cast", "read", zone)), spark.get(("struct", "cast", "read", zone)))
        lines.append(
            f"| {zone} | {localized} | {struct_tick} | {top_tick} | {cast_tick} |"
        )
    lines.append("")
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"differing {differs} of {len(keys)}")


def tick_from(repark_row: dict[str, Any] | None, spark_row: dict[str, Any] | None) -> str:
    """Return id=1 ticks from a read cell pair, for the wall-leaf section."""
    return f"repark={id_tick(repark_row)} spark={id_tick(spark_row)}"


def id_tick(row: dict[str, Any] | None) -> str:
    """Return the tick of the id=1 row, or the cell's error class."""
    if row is None:
        return "missing"
    if not row["ok"]:
        return f"ERR {row['error_class']}"
    chosen = row["rows"][0] if row["rows"] else None
    for item in row["rows"]:
        if item.get("id") == 1:
            chosen = item
            break
    if not isinstance(chosen, dict):
        return "empty"
    return str(canon_value(chosen.get("v")))


def main() -> int:
    """Dispatch a measurement or the summary."""
    if len(sys.argv) < 3:
        print("usage: probe.py repark|spark <out.jsonl> | summarize <repark> <spark> <summary>", file=sys.stderr)
        return 2
    command = sys.argv[1]
    try:
        if command == "summarize":
            summarize(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
            return 0
        out_path = Path(sys.argv[2])
        if command == "repark":
            seat_path = Path(sys.argv[3]) if len(sys.argv) > 3 else out_path.with_name("seat-explain.json")
            run_repark(out_path, seat_path)
            return 0
        if command == "spark":
            run_spark(out_path)
            return 0
    except Exception:
        traceback.print_exc()
        return 1
    print(f"unknown command {command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
