from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import importlib
import io
import json
import os
import re
import struct
import tempfile
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from repark_parity.scoreboard.encoding import canonical, cell_digest, encode_line, read_records

TICKS_NS = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1000, "ns": 1}
MONTH_INTERVAL_TYPE_ID = 21
SPARK_VERSION = "4.1.2"
PACKAGE_ENV = "SCOREBOARD_PACKAGE"
TIME_TYPE = re.compile(r'"timestamp(?:_ntz)?"')
RENDER_ONLY_TYPE = re.compile(r'interval year|interval month|"interval"|"void-variant"|"variant"')
CODE_PATTERNS = (
    ("error_class", "getCondition", r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]"),
    ("sqlstate", "getSqlState", r"SQLSTATE\s*[:=]?\s*([A-Z0-9]{5})"),
)


def arrow_value(item: pa.Scalar) -> Any:
    """Encode one Arrow scalar: positional structs, timestamp ticks in ns, sorted map entries."""
    if not item.is_valid:
        return None
    kind = item.type
    if pa.types.is_timestamp(kind):
        return {"timestamp_ns": item.value * TICKS_NS[kind.unit]}
    if pa.types.is_date(kind):
        days = item.value if pa.types.is_date32(kind) else item.value // 86_400_000
        try:
            return {"iso": (dt.date(1970, 1, 1) + dt.timedelta(days=days)).isoformat()}
        except OverflowError:
            return {"date_days": days}
    if pa.types.is_time(kind):
        return {"wall_time_ns": item.value * TICKS_NS[kind.unit]}
    if pa.types.is_map(kind):
        pairs = [[arrow_value(pair[0]), arrow_value(pair[1])] for pair in item.values]
        return {"map": sorted(pairs, key=encode_line)}
    if pa.types.is_struct(kind):
        return [arrow_value(item[index]) for index in range(len(kind))]
    if pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind):
        return [arrow_value(child) for child in item.values]
    return canonical(item.as_py())


def _month_intervals(chunk: pa.Array) -> list:
    valid, data = chunk.buffers()
    values = []
    for index in range(chunk.offset, chunk.offset + len(chunk)):
        present = valid is None or (valid[index // 8] & (1 << (index % 8)))
        months = struct.unpack_from("<i", data, index * 4)[0]
        values.append({"interval_months": months} if present else None)
    return values


def table_rows(table: pa.Table) -> list:
    """Return positional rows; decode the legacy month interval from its buffers."""
    columns = []
    for column in table.columns:
        values = []
        for chunk in column.chunks:
            if chunk.type.id == MONTH_INTERVAL_TYPE_ID:
                values.extend(_month_intervals(chunk))
            else:
                values.extend(arrow_value(item) for item in chunk)
        columns.append(values)
    if not columns:
        return [[] for _ in range(table.num_rows)]
    return [list(row) for row in zip(*columns, strict=True)]


def fixture(session: Any, work: Path, table: pa.Table) -> Any:
    """Read a table through a content-named Parquet file so no optimizer folds its columns."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    key = hashlib.sha256(sink.getvalue()).hexdigest()
    path = work / (key + ".parquet")
    if not path.exists():
        pq.write_table(table, path)
    return session.read.parquet(str(path))


def inspect_write(path: Path) -> dict:
    """Cold-read every committed Parquet part with PyArrow, independent of the engine."""
    files = sorted(
        part
        for part in path.rglob("*.parquet")
        if not any(name.startswith(("_", ".")) for name in part.relative_to(path).parts)
    )
    rows = []
    physical = []
    for file in files:
        table = pq.ParquetFile(file, coerce_int96_timestamp_unit="us").read()
        rows.extend(table_rows(table))
        physical.append(str(table.schema.remove_metadata()))
    return {
        "part_files": len(files),
        "rows": sorted(rows, key=encode_line),
        "schemas": sorted(set(physical)),
    }


def _quoted(funcs: Any, name: str) -> Any:
    return funcs.col("`" + name.replace("`", "``") + "`").cast("string")


def observe_frame(frame: Any, funcs: Any, ordered: bool) -> dict:
    """Record the public schema and the Arrow rows, plus SQL text for top-level timestamps."""
    schema = frame.schema.jsonValue()
    fields = schema["fields"]
    names = [field["name"] for field in fields]
    time_names = [
        name
        for name, field in zip(names, fields, strict=True)
        if TIME_TYPE.search(json.dumps(field["type"])) and names.count(name) == 1
    ]
    render_only = any(RENDER_ONLY_TYPE.search(json.dumps(field["type"])) for field in fields)
    original = frame
    render_error = None
    projection = render_only or bool(time_names)
    try:
        if render_only and len(set(names)) == len(names):
            frame = frame.select(*[_quoted(funcs, name).alias(name) for name in names])
        elif time_names:
            rendered = [
                _quoted(funcs, name).alias(f"_audit_render_{index}")
                for index, name in enumerate(time_names)
            ]
            frame = frame.select("*", *rendered)
        rows = table_rows(frame.toArrow())
    except Exception as exc:
        if not projection:
            raise
        rows = table_rows(original.toArrow())
        render_error = failure(exc)["message"]
        time_names = []
        render_only = False
    return {
        "outcome": "answered",
        "schema": schema,
        "value": rows if ordered else sorted(rows, key=encode_line),
        "rendered_columns": names if render_only else time_names,
        "render_error": render_error,
        "ordered": ordered,
        "observation": "SQL text with original schema" if render_only else "Arrow",
    }


def failure(exc: BaseException) -> dict:
    """Record a refusal: Python type, message head, error class and SQLSTATE with their source."""
    message = str(exc)
    pytype = type(exc).__name__
    fields: dict[str, Any] = {}
    for key, method, pattern in CODE_PATTERNS:
        value = None
        with contextlib.suppress(Exception):
            value = getattr(exc, method)()
        match = re.search(pattern, message) if not value else None
        fields[key] = value or (match.group(1) if match else None)
        fields[key + "_source"] = "api" if value else "text" if match else None
    return {
        "outcome": "panicked" if "PanicException" in pytype else "refused",
        "python_error": pytype,
        "message": message[:800],
        **fields,
    }


def show_text(frame: Any, *args: Any, **kwargs: Any) -> str:
    """Capture what the public show call prints."""
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink):
        frame.show(*args, **kwargs)
    return sink.getvalue()


def _from_import(root: str, name: str) -> Any:
    module = importlib.import_module(root)
    if hasattr(module, name):
        return getattr(module, name)
    return importlib.import_module(f"{root}.{name}")


def _repark_binary() -> dict:
    native_module = importlib.import_module("repark._native")
    native = Path(native_module.__file__).resolve()
    print(f"repark._native.__file__={native}", flush=True)
    requested = os.environ.get(PACKAGE_ENV)
    expected = [] if not requested else list((Path(requested) / "repark").glob("_native*.so"))
    if requested and native not in [path.resolve() for path in expected]:
        raise RuntimeError(f"unexpected binary {native} for package {requested}")
    with native.open("rb") as stream:
        return {"file": str(native), "sha256": hashlib.file_digest(stream, "sha256").hexdigest()}


def start(engine: str, work: Path) -> tuple:
    """Create one session with a private warehouse and return it with the case scope."""
    binary = _repark_binary() if engine == "repark" else None
    root = "pyspark.sql" if engine == "spark" else "repark.spark.sql"
    names = ("SparkSession", "Window", "Row", "functions", "types")
    scope = {name: _from_import(root, name) for name in names}
    builder = scope["SparkSession"].builder
    if engine == "spark":
        builder = (
            builder.master("local[1]")
            .appName("spark-door-audit")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.warehouse.dir", str(work / "warehouse"))
        )
    else:
        builder = builder.appName("spark-door-audit")
    session = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    if engine == "spark":
        session.sparkContext.setLogLevel("ERROR")
        if session.version != SPARK_VERSION:
            raise ValueError(f"wrong Spark version: {session.version}")
    zone = session.conf.get("spark.sql.session.timeZone")
    banner = {"engine": engine, "version": session.version, "native": binary, "zone": zone}
    print(encode_line(banner), flush=True)
    env = {
        "s": session,
        "F": scope["functions"],
        "T": scope["types"],
        "Window": scope["Window"],
        "Row": scope["Row"],
        "Path": Path,
        "Decimal": Decimal,
        "dt": dt,
        "pa": pa,
        "seeded": set(),
        "show_text": show_text,
        "work": work,
        "fixture": partial(fixture, session, work),
    }
    return session, env, binary


def _apply_settings(cell: dict, session: Any, previous: dict) -> str | None:
    session.conf.set("spark.sql.ansi.enabled", str(cell["ansi"]).lower())
    session.conf.set("spark.sql.session.timeZone", cell["zone"])
    for key, target in cell.get("settings", {}).items():
        try:
            previous[key] = session.conf.get(key)
        except Exception:
            previous[key] = None
        session.conf.set(key, target)
    expected = [
        ("spark.sql.ansi.enabled", str(cell["ansi"]).lower()),
        ("spark.sql.session.timeZone", cell["zone"]),
    ]
    for key, value in expected:
        if str(session.conf.get(key)) != value:
            return f"setting readback failed: {key}"
    return None


def _is_observation_failure(exc: BaseException, detail: dict) -> bool:
    return (
        type(exc).__module__.startswith("pyarrow")
        or isinstance(exc, OverflowError)
        or (isinstance(exc, KeyError) and exc.args == (MONTH_INTERVAL_TYPE_ID,))
        or (isinstance(exc, NotImplementedError) and "Wrapping scalar" in str(exc))
        or detail.get("error_class") == "UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION"
    )


def _refusal(exc: BaseException, cell: dict, sandbox: dict, stage: str) -> dict:
    connection = (ConnectionRefusedError, ConnectionResetError, BrokenPipeError)
    if isinstance(exc, connection) or type(exc).__name__ == "Py4JNetworkError":
        raise exc
    detail = failure(exc)
    if stage == "materialize" and _is_observation_failure(exc, detail):
        detail.update(
            outcome="excluded", reason="observation conversion: " + detail["message"][:200]
        )
    if stage in {"settings", "setup"} or isinstance(
        exc, (NameError, SyntaxError, FileNotFoundError)
    ):
        detail.update(outcome="excluded", reason=f"{stage}: {detail['message'][:240]}")
    if cell.get("store"):
        detail["physical_write"] = inspect_write(Path(sandbox["p"]))
    return {**detail, "stage": stage}


def _run_cell(cell: dict, env: dict, sandbox: dict, previous: dict, state: dict) -> dict:
    session = env["s"]
    state["stage"] = "settings"
    unread = _apply_settings(cell, session, previous)
    if unread:
        return {"outcome": "excluded", "reason": unread}
    state["stage"] = "setup"
    for code in cell.get("once", []):
        if code not in env["seeded"]:
            exec(code, sandbox)
            env["seeded"].add(code)
    for code in cell.get("setup", []):
        exec(code, sandbox)
    state["stage"] = "operation"
    result = session.sql(cell["sql"]) if "sql" in cell else eval(cell["py"], sandbox)
    before = None
    if cell.get("store"):
        state["stage"] = "write"
        before = result.schema.jsonValue()
        result.write.mode("overwrite").parquet(sandbox["p"])
        state["stage"] = "readback"
        result = session.read.parquet(sandbox["p"])
    if hasattr(type(result), "toArrow"):
        state["stage"] = "materialize"
        answer = observe_frame(result, sandbox["F"], cell.get("ordered", False))
        if cell.get("store"):
            answer["prewrite_schema"] = before
            answer["physical_write"] = inspect_write(Path(sandbox["p"]))
        return answer
    return {
        "outcome": "answered",
        "schema": {"python": type(result).__name__},
        "value": canonical(result),
    }


def execute(cell: dict, env: dict, directory: Path) -> dict:
    """Run one cell; setup failures are exclusions and tested settings are restored after."""
    if cell.get("exclude"):
        return {"outcome": "excluded", "reason": cell["exclude"]}
    session = env["s"]
    sandbox = {**env, "p": str(directory / "data"), "folder": directory}
    directory.mkdir()
    previous: dict[str, Any] = {}
    state = {"stage": "settings"}
    try:
        return _run_cell(cell, env, sandbox, previous, state)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:
        if previous and state["stage"] == "settings":
            state["stage"] = "tested-setting"
        return _refusal(exc, cell, sandbox, state["stage"])
    finally:
        for key, value in previous.items():
            if value is None:
                session.conf.unset(key)
            else:
                session.conf.set(key, value)


def main() -> None:
    """Answer every cell of one case file in one session and publish the complete file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=["spark", "repark"])
    parser.add_argument("cases", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    cases = read_records(args.cases)
    work = Path(tempfile.mkdtemp(prefix=args.engine + "-", dir=os.environ["TMPDIR"]))
    session, env, binary = start(args.engine, work)
    path = args.output.with_name(args.output.name + ".partial")
    with path.open("w", buffering=1) as out:
        for index, cell in enumerate(cases.values()):
            row = execute(cell, env, work / str(index))
            record = {
                "id": cell["id"],
                "input_sha256": cell_digest(cell),
                "engine": args.engine,
                "binary": binary,
                "version": session.version,
                "zone": cell["zone"],
                "ansi": cell["ansi"],
                **row,
            }
            out.write(encode_line(record) + "\n")
    session.stop()
    path.replace(args.output)


if __name__ == "__main__":
    main()
