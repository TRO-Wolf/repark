from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import math
import os
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any

SPECIALS = [
    "a/b",
    "a=b",
    "a%b",
    "a#b",
    "a:b",
    "a b",
    "é",
    "日本",
    "a'b",
    'a"b',
    "a*b",
    "a?b",
    "a\\b",
    "a{b}",
    "a[b]",
    "a^b",
    "a~b",
    "a|b",
    "a<b>",
    "a`b",
    "a\tb",
    "a\u007fb",
    "a+b",
    "a,b",
    "a;b",
    "a&b",
    "a$b",
    "a!b",
    "a@b",
    "..",
    ".",
    "%41",
    "x%2Fy",
]

ZONES = ["UTC", "America/New_York"]
TS_VALUES = [None, "2024-03-01 10:30:00", "2024-03-01 10:30:00.123456", "1969-12-31 23:59:59"]


def typed_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []

    def add(cid: str, ptype: str, values: list[Any], cast: str | None = None, **extra: Any) -> None:
        rows = [(index + 1, value) for index, value in enumerate(values)]
        source_type = "string" if cast else ptype
        cells.append(
            {
                "id": cid,
                "schema": f"id int, p {source_type}",
                "rows": rows,
                "cast": {"p": cast} if cast else {},
                "parts": ["p"],
                **extra,
            }
        )

    add("tinyint", "tinyint", [None, 0, 1, -1])
    add("smallint", "smallint", [None, 0, 7, -7])
    add("int", "int", [None, 0, 5, -7])
    add("bigint", "bigint", [None, 0, 5_000_000_000, -7])
    add("decimal", "decimal(10,2)", [None, Decimal("1.50"), Decimal("-0.25"), Decimal("0.00")])
    floats = [None, "1.0", "1.5", "NaN", "-0.0", "0.0", "Infinity", "-Infinity", "1e20"]
    add("float", "float", floats, cast="float")
    add("double", "double", [*floats, "0.1"], cast="double")
    add("boolean", "boolean", [None, True, False])
    add("date", "date", [None, dt.date(2024, 3, 1), dt.date(1970, 1, 1)])
    for zone in ZONES:
        tag = "utc" if zone == "UTC" else "ny"
        add(f"timestamp_{tag}", "timestamp", TS_VALUES, cast="timestamp", zone=zone)
        add(f"timestamp_ntz_{tag}", "timestamp_ntz", TS_VALUES, cast="timestamp_ntz", zone=zone)
    add("string_null", "string", [None, "a"])
    add("string_empty", "string", ["", "a"])
    add("string_sentinel", "string", ["__HIVE_DEFAULT_PARTITION__", "a"])
    add("string_specials", "string", SPECIALS)
    add("binary", "binary", [None, bytearray(b"ab")])
    add("int_nonull", "int", [0, 5, -7])
    add("string_nonull", "string", ["a", "b c"])
    add("int_allnull", "int", [None, None])
    add("string_allnull", "string", [None, None])
    add("date_allnull", "date", [None, None])
    cells.append(
        {
            "id": "two_level",
            "schema": "id int, p int, q string",
            "rows": [(1, None, "x"), (2, 5, None), (3, None, None), (4, 5, "x"), (5, 6, "")],
            "cast": {},
            "parts": ["p", "q"],
        }
    )
    cells.append(
        {
            "id": "two_level_nonull",
            "schema": "id int, p int, q string",
            "rows": [(1, 1, "x"), (2, 5, "y")],
            "cast": {},
            "parts": ["p", "q"],
        }
    )
    cells.append(
        {
            "id": "two_level_date_string",
            "schema": "id int, d date, s string",
            "rows": [(1, None, "a:b"), (2, dt.date(2024, 3, 1), None)],
            "cast": {},
            "parts": ["d", "s"],
        }
    )
    return cells


def all_cells() -> list[dict[str, Any]]:
    base = typed_cells()
    out: list[dict[str, Any]] = []
    for cell in base:
        out.append({**cell, "fmt": "parquet", "door": "method", "mode": "errorifexists"})
    text_ids = {
        "int",
        "string_null",
        "string_empty",
        "string_sentinel",
        "string_specials",
        "date",
        "two_level",
        "int_nonull",
        "int_allnull",
        "double",
        "boolean",
        "timestamp_ny",
        "decimal",
    }
    for cell in base:
        if cell["id"] not in text_ids:
            continue
        for fmt in ("csv", "json", "text"):
            out.append({**cell, "fmt": fmt, "door": "method", "mode": "errorifexists"})
    for cell in base:
        if cell["id"] not in {"int", "string_null", "string_empty", "date", "two_level"}:
            continue
        for fmt in ("parquet", "csv", "json", "text"):
            out.append({**cell, "fmt": fmt, "door": "save", "mode": "errorifexists"})
    int_cell = next(cell for cell in base if cell["id"] == "int")
    for fmt in ("parquet", "csv", "json"):
        out.append(
            {
                **int_cell,
                "id": "mode_overwrite",
                "fmt": fmt,
                "door": "method",
                "mode": "overwrite",
                "rows": [(1, None), (2, 5)],
                "pre": [(9, 9), (8, None)],
            }
        )
        out.append(
            {
                **int_cell,
                "id": "mode_append",
                "fmt": fmt,
                "door": "method",
                "mode": "append",
                "rows": [(2, None), (3, 5)],
                "pre": [(1, None)],
            }
        )
        out.append(
            {
                **int_cell,
                "id": "mode_error_exists",
                "fmt": fmt,
                "door": "method",
                "mode": "errorifexists",
                "rows": [(2, None)],
                "pre": [(1, None)],
            }
        )
        out.append(
            {
                **int_cell,
                "id": "mode_dynamic_null",
                "fmt": fmt,
                "door": "method",
                "mode": "overwrite",
                "rows": [(3, None)],
                "pre": [(1, None), (2, 5)],
                "conf": {"spark.sql.sources.partitionOverwriteMode": "dynamic"},
            }
        )
        out.append(
            {
                **int_cell,
                "id": "mode_dynamic_nonnull",
                "fmt": fmt,
                "door": "method",
                "mode": "overwrite",
                "rows": [(3, 5)],
                "pre": [(1, None), (2, 5)],
                "conf": {"spark.sql.sources.partitionOverwriteMode": "dynamic"},
            }
        )
    text_cell = {
        **int_cell,
        "id": "mode_append",
        "fmt": "text",
        "door": "method",
        "mode": "append",
        "rows": [(2, None), (3, 5)],
        "pre": [(1, None)],
    }
    out.append(text_cell)
    out.append({**text_cell, "id": "mode_overwrite", "mode": "overwrite", "pre": [(9, 9)]})
    for fmt in ("parquet", "csv", "json"):
        out.append(
            {
                "id": "roundtrip_copy",
                "fmt": fmt,
                "door": "roundtrip",
                "mode": "errorifexists",
                "parts": ["p"],
                "source": f"int/{fmt}/method/errorifexists",
            }
        )
    out.append(
        {
            "id": "roundtrip_copy_two_level",
            "fmt": "parquet",
            "door": "roundtrip",
            "mode": "errorifexists",
            "parts": ["p", "q"],
            "source": "two_level/parquet/method/errorifexists",
        }
    )
    out.append(
        {
            **int_cell,
            "id": "create_df_headline",
            "fmt": "parquet",
            "door": "method",
            "mode": "errorifexists",
            "rows": [(1, None), (2, 5)],
        }
    )
    for fmt in ("parquet", "csv", "json"):
        out.append({**int_cell, "id": "int", "fmt": fmt, "door": "sql_iod", "mode": "overwrite"})
        out.append(
            {**int_cell, "id": "int", "fmt": fmt, "door": "sql_iod_part", "mode": "overwrite"}
        )
    out.append({**int_cell, "id": "int", "fmt": "parquet", "door": "sql_ctas", "mode": "error"})
    out.append(
        {**int_cell, "id": "int", "fmt": "parquet", "door": "save_as_table", "mode": "error"}
    )
    return out


def cell_key(cell: dict[str, Any]) -> str:
    return f"{cell['id']}/{cell['fmt']}/{cell['door']}/{cell['mode']}"


def cell_dir_name(cell: dict[str, Any]) -> str:
    return cell_key(cell).replace("/", "__")


def normalize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Inf" if value > 0 else "-Inf"
        if value == 0.0 and math.copysign(1.0, value) < 0:
            return "-0.0"
        return repr(value)
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, (bytes, bytearray)):
        return f"bin({bytes(value).hex()})"
    if isinstance(value, dt.datetime):
        return f"ts({value.isoformat()})"
    if isinstance(value, dt.date):
        return f"date({value.isoformat()})"
    return str(value)


def first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    if not text:
        return type(exc).__name__
    for line in text.splitlines():
        if line.strip():
            return line.strip()[:300]
    return type(exc).__name__


def data_files(root: Path) -> list[Path]:
    found: list[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        name = path.name
        if name.startswith(".") or name.startswith("_") or name.endswith(".crc"):
            continue
        found.append(path)
    return found


def listing(root: Path) -> dict[str, Any]:
    if not root.exists():
        return {"exists": False}
    files = data_files(root)
    dirs = sorted({str(path.parent.relative_to(root)) for path in files})
    raw_dirs = sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_dir())
    return {"exists": True, "dirs": dirs, "all_dirs": raw_dirs, "files": len(files)}


def file_contents(root: Path, fmt: str) -> dict[str, Any]:
    if not root.exists():
        return {}
    out: dict[str, Any] = {}
    for path in data_files(root):
        rel = str(path.parent.relative_to(root))
        if fmt == "parquet":
            try:
                import pyarrow.parquet as pq

                table = pq.read_table(str(path))
                schema = [f"{field.name}:{field.type}" for field in table.schema]
                rows = [
                    [normalize_value(cell) for cell in row.values()] for row in table.to_pylist()
                ]
                entry = out.setdefault(rel, {"schema": schema, "rows": []})
                entry["rows"].extend(rows)
            except Exception as exc:
                out.setdefault(rel, {"error": first_line(exc)})
        else:
            raw = path.read_bytes()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            lines = [line for line in raw.decode("utf-8", "replace").splitlines() if line]
            out.setdefault(rel, {"rows": []})["rows"].extend(lines)
    for entry in out.values():
        if "rows" in entry:
            entry["rows"] = sorted(entry["rows"], key=json.dumps)
    return out


def read_back(spark: Any, path: Path, fmt: str) -> dict[str, Any]:
    if not path.exists():
        return {"ok": False, "first": "missing"}
    try:
        reader = spark.read
        frame = reader.text(str(path)) if fmt == "text" else reader.format(fmt).load(str(path))
        schema = [[field.name, field.dataType.simpleString()] for field in frame.schema.fields]
        rows = [[normalize_value(value) for value in row] for row in frame.collect()]
        rows = sorted(rows, key=json.dumps)
        return {"ok": True, "schema": schema, "rows": rows}
    except Exception as exc:
        return {"ok": False, "error_type": type(exc).__name__, "first": first_line(exc)}


def split_schema(schema: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current = ""
    for char in schema:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    parts.append(current)
    return parts


def build_frame(spark: Any, cell: dict[str, Any], rows: list[Any]) -> Any:
    schema = cell["schema"]
    if cell["fmt"] == "text":
        fields = [part.strip().split(" ", 1) for part in split_schema(schema)]
        schema = ", ".join(f"{name} {'string' if name == 'id' else kind}" for name, kind in fields)
        rows = [(str(row[0]), *row[1:]) for row in rows]
    frame = spark.createDataFrame(rows, schema)
    if cell.get("cast"):
        exprs = []
        for name in frame.columns:
            target = cell["cast"].get(name)
            exprs.append(f"CAST({name} AS {target}) AS {name}" if target else name)
        frame = frame.selectExpr(*exprs)
    return frame


def write_frame(frame: Any, cell: dict[str, Any], target: Path, mode: str) -> None:
    fmt = cell["fmt"]
    door = cell["door"]
    if door == "save":
        frame.write.format(fmt).partitionBy(*cell["parts"]).mode(mode).save(str(target))
        return
    writer = frame.write.partitionBy(*cell["parts"]).mode(mode)
    getattr(writer, fmt)(str(target))


def run_sql_door(spark: Any, cell: dict[str, Any], target: Path, seq: int) -> None:
    frame = build_frame(spark, cell, cell["rows"])
    view = f"partnull_src_{seq}"
    frame.createOrReplaceTempView(view)
    fmt = cell["fmt"]
    if cell["door"] == "sql_iod":
        spark.sql(f"INSERT OVERWRITE DIRECTORY '{target}' USING {fmt} SELECT * FROM {view}")
    elif cell["door"] == "sql_iod_part":
        spark.sql(
            f"INSERT OVERWRITE DIRECTORY '{target}' USING {fmt} PARTITIONED BY (p) "
            f"SELECT * FROM {view}"
        )
    elif cell["door"] == "sql_ctas":
        spark.sql(
            f"CREATE TABLE partnull_ctas_{seq} USING parquet PARTITIONED BY (p) "
            f"LOCATION '{target}' AS SELECT * FROM {view}"
        )
    else:
        frame.write.format("parquet").partitionBy("p").saveAsTable(f"partnull_sat_{seq}")


def run_cell(
    spark: Any, cell: dict[str, Any], root: Path, source_root: Path, seq: int
) -> dict[str, Any]:
    target = root / cell_dir_name(cell)
    if target.exists():
        shutil.rmtree(target)
    conf = dict(cell.get("conf", {}))
    zone = cell.get("zone", "UTC")
    spark.conf.set("spark.sql.session.timeZone", zone)
    result: dict[str, Any] = {"zone": zone, "conf": conf}
    try:
        if cell.get("pre") is not None:
            pre = build_frame(spark, cell, cell["pre"])
            write_frame(pre, cell, target, "errorifexists")
            result["pre_listing"] = listing(target)
        for key, value in conf.items():
            spark.conf.set(key, value)
        try:
            if cell["door"] == "roundtrip":
                source = source_root / cell["source"].replace("/", "__")
                result["source"] = str(source)
                result["source_listing"] = listing(source)
                reader = spark.read
                frame = (
                    reader.text(str(source))
                    if cell["fmt"] == "text"
                    else reader.format(cell["fmt"]).load(str(source))
                )
                result["source_schema"] = [
                    [field.name, field.dataType.simpleString()] for field in frame.schema.fields
                ]
                writer = frame.write.partitionBy(*cell["parts"]).mode("errorifexists")
                getattr(writer, cell["fmt"])(str(target))
            elif cell["door"].startswith("sql_") or cell["door"] == "save_as_table":
                run_sql_door(spark, cell, target, seq)
            else:
                frame = build_frame(spark, cell, cell["rows"])
                write_frame(frame, cell, target, cell["mode"])
            result["write"] = {"ok": True}
        finally:
            for key in conf:
                spark.conf.unset(key)
    except Exception as exc:
        result["write"] = {"ok": False, "error_type": type(exc).__name__, "first": first_line(exc)}
    result["listing"] = listing(target)
    result["contents"] = file_contents(target, cell["fmt"])
    result["read_own"] = read_back(spark, target, cell["fmt"])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return result


def make_session(engine: str, ansi: str, warehouse: Path) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"partnull-{ansi}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .config("spark.sql.warehouse.dir", str(warehouse))
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    from repark import ReparkSession
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    return (
        ReparkSession.builder.appName(f"partnull-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )


def run_writes(engine: str, label: str, work: Path, spark_label: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"engine": engine, "label": label, "modes": {}}
    if engine == "repark":
        import repark

        payload["native_file"] = repark._native.__file__
    for ansi in ("false", "true"):
        root = work / label / f"ansi_{ansi}"
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True)
        source_root = work / spark_label / f"ansi_{ansi}"
        spark = make_session(engine, ansi, root / "_warehouse")
        cells: dict[str, Any] = {}
        try:
            for seq, cell in enumerate(all_cells()):
                key = cell_key(cell)
                result = run_cell(spark, cell, root, source_root, seq)
                if cell["door"] == "save_as_table":
                    result["listing"] = listing(root / "_warehouse" / f"partnull_sat_{seq}")
                    result["contents"] = file_contents(
                        root / "_warehouse" / f"partnull_sat_{seq}", cell["fmt"]
                    )
                if engine == "repark" and label != spark_label:
                    result["read_spark_dir"] = read_back(
                        spark, source_root / cell_dir_name(cell), cell["fmt"]
                    )
                cells[key] = result
                print(f"{ansi} {key} write={result['write']['ok']}", flush=True)
        finally:
            spark.stop()
        payload["modes"][ansi] = cells
    return payload


def run_cross(work: Path, labels: list[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {"engine": "spark", "cross": {}}
    for ansi in ("false", "true"):
        spark = make_session("spark", ansi, work / "_cross_wh")
        try:
            for label in labels:
                root = work / label / f"ansi_{ansi}"
                out = payload["cross"].setdefault(label, {}).setdefault(ansi, {})
                for cell in all_cells():
                    out[cell_key(cell)] = read_back(spark, root / cell_dir_name(cell), cell["fmt"])
        finally:
            spark.stop()
    return payload


SKIP_TYPES = {
    "decimal",
    "float",
    "double",
    "binary",
    "timestamp_utc",
    "timestamp_ny",
    "timestamp_ntz_utc",
    "timestamp_ntz_ny",
}
SKIP_IDS = {"mode_dynamic_null", "mode_dynamic_nonnull"}


def ids_of(lines: list[Any], fmt: str) -> list[str]:
    out: list[str] = []
    for line in lines:
        if fmt == "parquet":
            out.append(str(line[0]))
        elif fmt == "json":
            out.append(str(json.loads(line)["id"]))
        elif fmt == "csv":
            if line.lstrip("-").isdigit():
                out.append(line)
        else:
            out.append(line)
    return sorted(out)


def expected(result: dict[str, Any], fmt: str) -> dict[str, Any]:
    if not result["write"]["ok"]:
        first = result["write"]["first"]
        return {"refuses": first.split("]")[0].lstrip("[") if first.startswith("[") else first}
    return {
        "dirs": {
            directory: ids_of(entry["rows"], fmt)
            for directory, entry in sorted(result["contents"].items())
        }
    }


def build_pin_table(spark_json: Path, out: Path) -> int:
    spark = json.loads(spark_json.read_text())
    cells_false = spark["modes"]["false"]
    cells_true = spark["modes"]["true"]
    table: list[dict[str, Any]] = []
    for cell in all_cells():
        if cell["id"] in SKIP_TYPES or cell["id"] in SKIP_IDS:
            continue
        if cell["door"] not in {"method", "save"}:
            continue
        key = cell_key(cell)
        recorded = expected(cells_false[key], cell["fmt"])
        if recorded != expected(cells_true[key], cell["fmt"]):
            raise SystemExit(f"ANSI modes differ on {key}")
        table.append(
            {
                "key": key,
                "fmt": cell["fmt"],
                "door": cell["door"],
                "mode": cell["mode"],
                "schema": cell["schema"],
                "rows": [list(row) for row in cell["rows"]],
                "pre": [list(row) for row in cell["pre"]] if cell.get("pre") else None,
                "parts": cell["parts"],
                **recorded,
            }
        )
    body = ",\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in table)
    out.write_text("[\n" + body + "\n]\n", encoding="utf-8")
    return len(table)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--engine", choices=["spark", "repark", "cross", "pin-table"], required=True
    )
    parser.add_argument("--label", required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--spark-label", default="spark")
    parser.add_argument("--cross-labels", default="")
    parser.add_argument("--spark-json", type=Path)
    args = parser.parse_args()
    os.environ.setdefault("TZ", "UTC")
    if args.engine == "pin-table":
        count = build_pin_table(args.spark_json, args.out)
        print(f"{count} cells -> {args.out}", flush=True)
        return
    if args.engine == "cross":
        payload = run_cross(args.work, [item for item in args.cross_labels.split(",") if item])
    else:
        payload = run_writes(args.engine, args.label, args.work, args.spark_label)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True, ensure_ascii=False))
    print(f"WROTE {args.out}", flush=True)


if __name__ == "__main__":
    main()
