from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

ANSWERED = "ANSWERED"
REFUSED = "REFUSED"
PANIC = "PANIC"
SETUP_REFUSED = "SETUP-REFUSED"
ORACLE_MISSING = "ORACLE-MISSING"
SPARK_CATALOG = "probe_ice"
REPARK_CATALOG = "ice"
NAMESPACE = "ns"
SQL_OPS = ("UNION ALL", "UNION", "INTERSECT", "EXCEPT")
DF_OPS = ("union", "unionByName")
SET_RIGHT_OPS = frozenset({"INTERSECT", "EXCEPT"})
FORMS = ("read", "insert")
DOORS = ("sql", "native", "df")
POSITION = "position"
NAME = "name"
PAIRING_TEXT = {POSITION: "by-position", NAME: "by-name"}
PREDICTION_KEY = {POSITION: "by_position", NAME: "by_name"}
NATIVE_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
ORACLE_REASON = "the native door has no Spark reference (its reference is Trino, not Spark)"
TABLE_PROPERTIES = (
    "'format-version' = '2', "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write'"
)


class NoPairingError(Exception):
    pass


def struct(*fields: tuple[str, Any]) -> tuple[str, list[tuple[str, Any]]]:
    return ("s", list(fields))


def array(child: Any) -> tuple[str, list[Any]]:
    return ("a", [child])


def mapping(key: str, child: Any) -> tuple[str, list[tuple[str, Any]]]:
    return ("m", [(key, child)])


SHAPES: tuple[tuple[str, Any, Any, Any], ...] = (
    (
        "same-order",
        struct(("a", 1), ("b", 2)),
        struct(("a", 3), ("b", 4)),
        struct(("a", 1), ("b", 2)),
    ),
    (
        "reordered",
        struct(("a", 1), ("b", 2)),
        struct(("b", 3), ("a", 4)),
        struct(("b", 2), ("a", 1)),
    ),
    (
        "reordered-typed",
        struct(("a", 1), ("b", "x")),
        struct(("b", "y"), ("a", 2)),
        struct(("b", "x"), ("a", 1)),
    ),
    (
        "disjoint-names",
        struct(("a", 1), ("b", 2)),
        struct(("c", 3), ("d", 4)),
        struct(("c", 1), ("d", 2)),
    ),
    (
        "partial-overlap",
        struct(("a", 1), ("b", 2)),
        struct(("a", 3), ("c", 4)),
        struct(("a", 1), ("c", 2)),
    ),
    (
        "case-reordered",
        struct(("a", 1), ("b", 2)),
        struct(("B", 3), ("A", 4)),
        struct(("B", 2), ("A", 1)),
    ),
    (
        "nested-reordered",
        struct(("x", struct(("a", 1), ("b", 2)))),
        struct(("x", struct(("b", 3), ("a", 4)))),
        struct(("x", struct(("b", 2), ("a", 1)))),
    ),
    (
        "array-reordered",
        array(struct(("a", 1), ("b", 2))),
        array(struct(("b", 3), ("a", 4))),
        array(struct(("b", 2), ("a", 1))),
    ),
    (
        "map-value-reordered",
        mapping("k", struct(("a", 1), ("b", 2))),
        mapping("k", struct(("b", 3), ("a", 4))),
        mapping("k", struct(("b", 2), ("a", 1))),
    ),
    (
        "three-way",
        struct(("a", 1), ("b", 2), ("c", 3)),
        struct(("c", 4), ("a", 5), ("b", 6)),
        struct(("c", 3), ("a", 1), ("b", 2)),
    ),
)


def sanitize(text: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", text.lower())


def scrub(text: str, warehouse_root: str) -> str:
    text = text.replace(warehouse_root, "<warehouse>")
    temporary = re.escape(tempfile.gettempdir())
    text = re.sub(temporary + r"/[^\s'\"),\]\\]*", "<tmp>", text)
    home = re.escape(str(Path.home()))
    return re.sub(home + r"(?:/[^\s'\"),\]\\]*)?", "<home>", text)


def canon(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def sort_rows(rows: list[Any]) -> list[Any]:
    return sorted(rows, key=canon)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def spell(node: Any, native: bool) -> str:
    if isinstance(node, int):
        return str(node)
    if isinstance(node, str):
        return f"'{node}'"
    tag, body = node
    if tag == "s":
        fields = ", ".join(f"'{name}', {spell(child, native)}" for name, child in body)
        return f"named_struct({fields})"
    if tag == "a":
        inner = spell(body[0], native)
        return f"ARRAY[{inner}]" if native else f"array({inner})"
    key, child = body[0]
    if native:
        return f"MAP(ARRAY['{key}'], ARRAY[{spell(child, native)}])"
    return f"map('{key}', {spell(child, native)})"


def ddl_type(node: Any) -> str:
    if isinstance(node, int):
        return "INT"
    if isinstance(node, str):
        return "STRING"
    tag, body = node
    if tag == "s":
        inner = ", ".join(f"{name}: {ddl_type(child)}" for name, child in body)
        return f"STRUCT<{inner}>"
    if tag == "a":
        return f"ARRAY<{ddl_type(body[0])}>"
    return f"MAP<STRING, {ddl_type(body[0][1])}>"


def reading(left: Any, right: Any, mode: str) -> Any:
    if not isinstance(left, tuple):
        return right
    tag, body = left
    if tag == "s":
        right_fields = right[1]
        if mode == POSITION:
            pairs = zip(body, right_fields, strict=True)
            return (
                "s",
                [(name, reading(child, rchild, mode)) for (name, child), (_, rchild) in pairs],
            )
        lookup = {rname.lower(): rchild for rname, rchild in right_fields}
        if not any(name.lower() in lookup for name, _ in body):
            raise NoPairingError(left)
        fields = []
        for name, child in body:
            if name.lower() in lookup:
                fields.append((name, reading(child, lookup[name.lower()], mode)))
            else:
                fields.append((name, None))
        return ("s", fields)
    if tag == "a":
        return ("a", [reading(body[0], right[1][0], mode)])
    key, child = body[0]
    return ("m", [(key, reading(child, right[1][0][1], mode))])


def flat(node: Any) -> Any:
    if not isinstance(node, tuple):
        return node
    tag, body = node
    if tag == "s":
        return [flat(child) for _, child in body]
    if tag == "a":
        return [flat(child) for child in body]
    return [[key, flat(child)] for key, child in body]


def observed(value: Any) -> Any:
    if isinstance(value, dict):
        return [observed(item) for item in value.values()]
    if isinstance(value, list):
        if value and isinstance(value[0], tuple):
            return [[key, observed(item)] for key, item in value]
        return [observed(item) for item in value]
    return value


def loose(value: Any) -> Any:
    if isinstance(value, list):
        return [loose(item) for item in value]
    return str(value)


def strict_key(rows: list[Any]) -> list[str]:
    return sorted(canon(row) for row in rows)


def loose_key(rows: list[Any]) -> list[str]:
    return sorted(canon(loose(row)) for row in rows)


def predicted(op: str, left: Any, right: Any, mode: str) -> list[list[Any]] | None:
    try:
        mapped = reading(left, right, mode)
    except NoPairingError:
        return None
    left_row = [flat(left)]
    mapped_row = [flat(mapped)]
    if op in ("UNION ALL", "union", "unionByName"):
        return [left_row, mapped_row]
    matched = loose_key([left_row]) == loose_key([mapped_row])
    if op == "UNION":
        return [left_row] if matched else [left_row, mapped_row]
    if op == "INTERSECT":
        return [left_row] if matched else []
    return [] if matched else [left_row]


def prediction_text(rows: list[list[Any]] | None) -> str:
    if rows is None:
        return "REFUSED (no field name matches)"
    return json.dumps(rows, separators=(",", ":"))


def make_prediction(op: str, left: Any, right: Any) -> dict[str, Any]:
    by_position = predicted(op, left, right, POSITION)
    by_name = predicted(op, left, right, NAME)
    text = f"by-position {prediction_text(by_position)}; by-name {prediction_text(by_name)}"
    return {"text": text, "by_position": by_position, "by_name": by_name}


def make_spec(shape: str, left: Any, right: Any, op: str, form: str, door: str) -> dict[str, Any]:
    table = sanitize(f"t_{shape}_{op}_{door}") if form == "insert" else ""
    return {
        "shape": shape,
        "op": op,
        "form": form,
        "door": door,
        "join": f"{shape}|{op}|{form}",
        "table": table,
        "left": left,
        "right": right,
        "prediction": make_prediction(op, left, right),
    }


def plan() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for shape, left, right, set_right in SHAPES:
        for door in ("sql", "native"):
            for op in SQL_OPS:
                operand = set_right if op in SET_RIGHT_OPS else right
                for form in FORMS:
                    specs.append(make_spec(shape, left, operand, op, form, door))
        for op in DF_OPS:
            for form in FORMS:
                specs.append(make_spec(shape, left, right, op, form, "df"))
    return specs


def type_text(data_type: Any) -> str:
    import pyarrow.types as kinds

    if kinds.is_struct(data_type):
        fields = [data_type[index] for index in range(data_type.num_fields)]
        inner = ", ".join(f"{field.name}: {type_text(field.type)}" for field in fields)
        return f"struct<{inner}>"
    if kinds.is_map(data_type):
        return f"map<{type_text(data_type.key_type)}, {type_text(data_type.item_type)}>"
    if kinds.is_list(data_type):
        return f"list<{type_text(data_type.value_type)}>"
    return str(data_type)


def schema_signature(schema: Any) -> str:
    pairs = zip(schema.names, schema.types, strict=True)
    return ", ".join(f"{name}: {type_text(kind)}" for name, kind in pairs)


def collect(frame: Any) -> dict[str, Any]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    rows = [[observed(value) for value in row.values()] for row in table.to_pylist()]
    return {
        "schema": str(table.schema),
        "signature": schema_signature(table.schema),
        "rows": sort_rows(rows),
    }


def excerpt(text: str, limit: int = 300) -> str:
    if len(text) <= limit:
        return text
    head = text[:limit]
    boundary = head.rfind(" ")
    return head[:boundary] if boundary > 0 else head


def error_record(ctx: dict[str, Any], error: BaseException, stage: str) -> dict[str, Any]:
    text = scrub(str(error), ctx["scrub"])
    panic = type(error).__name__ == "PanicException" or "panicked at" in text
    return {
        "class": type(error).__name__,
        "stage": stage,
        "message": excerpt(text),
        "panic": panic,
    }


def attempt(ctx: dict[str, Any], call: Any, stage: str) -> tuple[Any, dict[str, Any] | None]:
    try:
        return call(), None
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        return None, error_record(ctx, error, stage)


def outcome_of(error: dict[str, Any] | None) -> str:
    if error is None:
        return ANSWERED
    return PANIC if error["panic"] else REFUSED


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def read_metadata(table_dir: Path) -> dict[str, Any]:
    candidates = sorted((table_dir / "metadata").glob("*.metadata.json"), key=metadata_order)
    if not candidates:
        return {"snapshot_count": 0, "current_snapshot_id": None}
    document = json.loads(candidates[-1].read_text(encoding="utf-8"))
    return {
        "snapshot_count": len(document.get("snapshots") or []),
        "current_snapshot_id": document.get("current-snapshot-id"),
    }


def list_data_files(table_dir: Path) -> list[str]:
    data = table_dir / "data"
    if not data.is_dir():
        return []
    return sorted(str(path.relative_to(data)) for path in data.rglob("*") if path.is_file())


def observe(
    ctx: dict[str, Any], door: dict[str, Any], table_dir: Path, select_sql: str
) -> dict[str, Any]:
    state = read_metadata(table_dir)
    rows, error = attempt(ctx, lambda: collect(door["sql"](select_sql))["rows"], "observe")
    state["persisted"] = rows
    state["persist_error"] = error
    state["files"] = list_data_files(table_dir)
    return state


def facts_of(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "persisted_changed": before["persisted"] != after["persisted"],
        "snapshot_changed": (before["snapshot_count"], before["current_snapshot_id"])
        != (after["snapshot_count"], after["current_snapshot_id"]),
        "files_changed": sorted(before["files"]) != sorted(after["files"]),
        "files_added": len(set(after["files"]) - set(before["files"])),
    }


def create_sql(door: dict[str, Any], qualified: str, columns: str) -> str:
    using = " USING iceberg" if door["using_iceberg"] else ""
    clause = f" TBLPROPERTIES ({TABLE_PROPERTIES})" if door["accepts_properties"] else ""
    return f"CREATE TABLE {qualified} ({columns}){using}{clause}"


def base_record(ctx: dict[str, Any], spec: dict[str, Any], qualified: str) -> dict[str, Any]:
    return {
        "record": "cell",
        "engine": ctx["engine"],
        "leg": ctx["label"],
        "matrix": "setop",
        "shape": spec["shape"],
        "operation": spec["op"],
        "form": spec["form"],
        "door": spec["door"],
        "join": spec["join"],
        "table": qualified,
        "column_type": ddl_type(spec["left"]),
        "ddl": None,
        "statement": None,
        "outcome": None,
        "error": None,
        "result": None,
        "persisted_before": None,
        "persisted_after": None,
        "snapshot_before": None,
        "snapshot_after": None,
        "files_before": None,
        "files_after": None,
        "facts": None,
        "prediction": spec["prediction"],
    }


def finish_setup(record: dict[str, Any], error: dict[str, Any]) -> dict[str, Any]:
    record.update(outcome=SETUP_REFUSED, error=error)
    return record


def finish_oracle(record: dict[str, Any], reason: str) -> dict[str, Any]:
    record.update(
        outcome=ORACLE_MISSING,
        error={"class": None, "stage": None, "message": reason, "panic": False},
    )
    return record


def df_frame(door: dict[str, Any], spec: dict[str, Any], left: str, right: str) -> Any:
    first = door["sql"](f"SELECT {left} AS s")
    second = door["sql"](f"SELECT {right} AS s")
    return getattr(first, spec["op"])(second)


def df_statement(spec: dict[str, Any], left: str, right: str) -> str:
    return f"SELECT {left} AS s; SELECT {right} AS s; DataFrame.{spec['op']}(second)"


def run_read(
    ctx: dict[str, Any],
    door: dict[str, Any],
    spec: dict[str, Any],
    record: dict[str, Any],
    left: str,
    right: str,
) -> dict[str, Any]:
    if spec["door"] == "df":
        record["statement"] = df_statement(spec, left, right)
        frame, error = attempt(ctx, lambda: df_frame(door, spec, left, right), "combine")
        result = None
        if error is None:
            result, error = attempt(ctx, lambda: collect(frame), "collect")
    else:
        statement = f"SELECT {left} AS s {spec['op']} SELECT {right} AS s"
        record["statement"] = statement
        result, error = attempt(ctx, lambda: collect(door["sql"](statement)), "sql")
    record.update(outcome=outcome_of(error), error=error, result=result)
    return record


def run_insert(
    ctx: dict[str, Any],
    door: dict[str, Any],
    spec: dict[str, Any],
    record: dict[str, Any],
    qualified: str,
    left: str,
    right: str,
) -> dict[str, Any]:
    table_dir = door["root"] / NAMESPACE / spec["table"]
    ddl = create_sql(door, qualified, f"s {ddl_type(spec['left'])}")
    record["ddl"] = ddl
    _, error = attempt(ctx, lambda: collect(door["sql"](ddl)), "create")
    if error is not None:
        return finish_setup(record, error)
    select_sql = f"SELECT s FROM {qualified}"
    before = observe(ctx, door, table_dir, select_sql)
    if spec["door"] == "df":
        record["statement"] = f"{df_statement(spec, left, right)}; insertInto({qualified})"
        frame, error = attempt(ctx, lambda: df_frame(door, spec, left, right), "combine")
        if error is None:
            _, error = attempt(ctx, lambda: frame.write.insertInto(qualified), "write")
    else:
        statement = f"INSERT INTO {qualified} SELECT {left} AS s {spec['op']} SELECT {right} AS s"
        record["statement"] = statement
        _, error = attempt(ctx, lambda: collect(door["sql"](statement)), "sql")
    after = observe(ctx, door, table_dir, select_sql)
    record.update(
        outcome=outcome_of(error),
        error=error,
        persisted_before=before["persisted"],
        persisted_after=after["persisted"],
        snapshot_before={
            "count": before["snapshot_count"],
            "current": before["current_snapshot_id"],
        },
        snapshot_after={"count": after["snapshot_count"], "current": after["current_snapshot_id"]},
        files_before=before["files"],
        files_after=after["files"],
        facts=facts_of(before, after),
    )
    return record


def run_spec(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    if ctx["engine"] == "spark" and spec["door"] == "native":
        return finish_oracle(base_record(ctx, spec, ""), ORACLE_REASON)
    door = ctx["doors"][spec["door"]]
    native = spec["door"] == "native"
    qualified = ""
    if spec["form"] == "insert":
        qualified = f"{door['catalog']}.{NAMESPACE}.{spec['table']}"
    record = base_record(ctx, spec, qualified)
    left = spell(spec["left"], native)
    right = spell(spec["right"], native)
    if spec["form"] == "insert":
        return run_insert(ctx, door, spec, record, qualified, left, right)
    return run_read(ctx, door, spec, record, left, right)


def build_spark_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as spark_functions

    root = warehouse / "spark"
    session = (
        SparkSession.builder.master("local[1]")
        .appName("probe-setop")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(warehouse / "spark-local"))
        .config("spark.jars", arguments.ivy_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(root))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    door = {
        "name": "sql",
        "sql": session.sql,
        "catalog": SPARK_CATALOG,
        "root": root,
        "session": session,
        "functions": spark_functions,
        "accepts_properties": True,
        "using_iceberg": True,
    }
    collect(session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{NAMESPACE}"))
    jar = Path(arguments.ivy_jar)
    header = {
        "record": "leg",
        "engine": "spark",
        "label": arguments.label,
        "spark_version": session.version,
        "jdk_version": session.sparkContext._jvm.System.getProperty("java.version"),
        "iceberg_jar": jar.name,
        "iceberg_jar_sha256": sha256_of(jar),
        "ansi_enabled": session.conf.get("spark.sql.ansi.enabled"),
        "case_sensitive": session.conf.get("spark.sql.caseSensitive"),
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    return {
        "engine": "spark",
        "label": arguments.label,
        "scrub": str(warehouse),
        "doors": {"sql": door, "df": door},
        "header": header,
        "close": session.stop,
    }


def build_repark_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    import pyarrow

    import repark
    from repark import ReparkSession
    from repark import functions as repark_functions
    from repark._native import PyReparkSession

    native_root = warehouse / "native"
    facade_root = warehouse / "facade"
    native_root.mkdir(parents=True)
    facade_root.mkdir(parents=True)
    native = PyReparkSession.native(config=NATIVE_CONFIG)
    native.register_memory_catalog(REPARK_CATALOG, str(native_root))
    native.create_namespace(REPARK_CATALOG, NAMESPACE, str(native_root / NAMESPACE))
    repark._ANSI_NATIVE = native
    facade = (
        ReparkSession.builder.appName("probe-setop")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    facade.register_memory_catalog(REPARK_CATALOG, str(facade_root))
    native_door = {
        "name": "native",
        "sql": repark.sql,
        "catalog": REPARK_CATALOG,
        "root": native_root,
        "accepts_properties": False,
        "using_iceberg": False,
    }
    facade_door = {
        "name": "facade",
        "sql": facade.sql,
        "catalog": REPARK_CATALOG,
        "root": facade_root,
        "session": facade,
        "functions": repark_functions,
        "accepts_properties": True,
        "using_iceberg": True,
    }
    wheel = Path(arguments.wheel)
    header = {
        "record": "leg",
        "engine": "repark",
        "label": arguments.label,
        "wheel_file": wheel.name,
        "wheel_sha256": sha256_of(wheel),
        "repark_file": repark.__file__,
        "repark_version": importlib.metadata.version("repark"),
        "pyarrow_version": pyarrow.__version__,
        "native_config": NATIVE_CONFIG,
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    facade_location = facade_root / NAMESPACE
    collect(
        facade.sql(f"CREATE NAMESPACE {REPARK_CATALOG}.{NAMESPACE} LOCATION '{facade_location}'")
    )
    return {
        "engine": "repark",
        "label": arguments.label,
        "scrub": str(warehouse),
        "doors": {"native": native_door, "sql": facade_door, "df": facade_door},
        "header": header,
        "close": lambda: None,
    }


def build_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    if arguments.engine == "spark":
        return build_spark_context(arguments, warehouse)
    return build_repark_context(arguments, warehouse)


def write_line(handle: Any, record: dict[str, Any], warehouse_root: str) -> None:
    text = json.dumps(record, sort_keys=True, default=str)
    handle.write(scrub(text, warehouse_root) + "\n")


def load_cells(path: str) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("record") == "cell":
            cells.append(record)
    return cells


def index_cells(cells: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(cell["join"], cell["door"]): cell for cell in cells}


def answered_rows(record: dict[str, Any]) -> list[Any]:
    if record["form"] == "read":
        return record["result"]["rows"]
    return record["persisted_after"]


def pairing_of(record: dict[str, Any] | None) -> str:
    if record is None or record["outcome"] != ANSWERED:
        return "n/a"
    observed_key = loose_key(answered_rows(record))
    hits = []
    for mode, key in PREDICTION_KEY.items():
        rows = record["prediction"][key]
        if rows is not None and loose_key(rows) == observed_key:
            hits.append(mode)
    return PAIRING_TEXT[hits[0]] if len(hits) == 1 else "n/a"


def compare_answers(form: str, spark: dict[str, Any], repark: dict[str, Any]) -> list[str]:
    if form == "insert":
        if strict_key(repark["persisted_after"]) != strict_key(spark["persisted_after"]):
            return ["ROWS-DIFFER"]
        if repark["facts"]["snapshot_changed"] != spark["facts"]["snapshot_changed"]:
            return ["SNAPSHOT-DIFFERS"]
        return ["EQUAL"]
    repark_rows = repark["result"]["rows"]
    spark_rows = spark["result"]["rows"]
    strict_equal = strict_key(repark_rows) == strict_key(spark_rows)
    same_schema = repark["result"]["signature"] == spark["result"]["signature"]
    if strict_equal and same_schema:
        return ["EQUAL"]
    if strict_equal or loose_key(repark_rows) == loose_key(spark_rows):
        return ["SCHEMA-DIFFERS"]
    return ["RESULT-ROWS-DIFFER"]


def cell_labels(spark: dict[str, Any] | None, repark: dict[str, Any]) -> list[str]:
    if repark["outcome"] == PANIC or (spark is not None and spark["outcome"] == PANIC):
        return ["PANIC"]
    if spark is None or spark["outcome"] == ORACLE_MISSING:
        return ["ORACLE-MISSING"]
    repark_ok = repark["outcome"] == ANSWERED
    spark_ok = spark["outcome"] == ANSWERED
    if repark_ok and not spark_ok:
        return ["ACCEPTED-WHERE-SPARK-REFUSED"]
    if spark_ok and not repark_ok:
        return ["REFUSED-WHERE-SPARK-ACCEPTED"]
    if not repark_ok and not spark_ok:
        return ["BOTH-REFUSED"]
    return compare_answers(repark["form"], spark, repark)


def measured_text(record: dict[str, Any] | None) -> str:
    if record is None:
        return "-"
    outcome = record["outcome"]
    if outcome == ORACLE_MISSING:
        return ORACLE_MISSING
    if outcome != ANSWERED:
        error = record["error"]
        return f"{outcome} ({error['stage']}): {error['class']}: {error['message'][:120]}"
    rows = json.dumps(answered_rows(record), separators=(",", ":"))
    if record["form"] == "read":
        return f"ANSWERED rows {rows}; schema {record['result']['signature']}"
    changed = "changed" if record["facts"]["snapshot_changed"] else "same"
    return f"ANSWERED persisted {rows}; snapshot {changed}"


def markdown_cell(text: str) -> str:
    return text.replace("|", "/").replace("\n", " ")


def markdown_table(rows: list[list[str]]) -> list[str]:
    header = [
        "| shape | operation | door | form | predicted (left field order) | Spark measured | "
        "RePark measured | pairing Spark | pairing RePark | labels for RePark |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    body = ["| " + " | ".join(markdown_cell(cell) for cell in row) + " |" for row in rows]
    return header + body


def render(arguments: argparse.Namespace) -> int:
    spark_index = index_cells(load_cells(arguments.spark))
    repark_cells = load_cells(arguments.main)
    table_rows: list[list[str]] = []
    counts: Counter[str] = Counter()
    non_equal: list[str] = []
    per_door: dict[str, Counter[str]] = {door: Counter() for door in DOORS}
    for cell in repark_cells:
        key = (cell["join"], cell["door"])
        spark = spark_index.get(key)
        labels = cell_labels(spark, cell)
        counts.update(labels)
        per_door[cell["door"]].update(labels)
        if labels != ["EQUAL"]:
            non_equal.append(
                f"{cell['shape']} | {cell['operation']} | {cell['door']} | {cell['form']} | "
                f"{', '.join(labels)}"
            )
        table_rows.append(
            [
                cell["shape"],
                cell["operation"],
                cell["door"],
                cell["form"],
                cell["prediction"]["text"],
                measured_text(spark),
                measured_text(cell),
                pairing_of(spark),
                pairing_of(cell),
                ", ".join(labels),
            ]
        )
    lines = [
        "# Set-operation domain table (STRUCT-SETOP-REORDER-REFUSAL-1, measurement only)",
        "",
        "Generated by `probe_setop.py --render` from `raw/spark.jsonl` and "
        "`raw/repark-main-fdc61f26.jsonl`; not edited by hand. One row per shape, operation, "
        "door and form. Struct values are listed in the left operand's field order, so `[1,2]` "
        "is `(a=1, b=2)` for shape `reordered`; a map is listed as `[key, value]` pairs. "
        "The two readings: `by-position` pairs the i-th right field with the i-th left field; "
        "`by-name` pairs fields by name, ignoring case as Spark does by default, puts NULL where "
        "the right operand has no field of that name, and refuses when no name matches. The "
        "right operand of INTERSECT and "
        "EXCEPT is the set-right variant (see provenance.md). `by-position` and `by-name` in the "
        "pairing columns name the reading whose predicted rows equal the measured rows; `n/a` "
        "means no reading matched, both matched, or the cell refused or was not measured. A "
        "native CREATE that refuses is shown in the measured column as SETUP-REFUSED and is "
        "labelled ORACLE-MISSING, because the native door has no Spark cell.",
        "",
        *markdown_table(table_rows),
        "",
        "## Label counts (RePark cells against the Spark cell)",
        "",
        "| label | all cells | sql door | native door | df door |",
        "| --- | --- | --- | --- | --- |",
    ]
    for label in sorted(counts):
        cells = [str(counts[label])] + [str(per_door[door][label]) for door in DOORS]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines.append("")
    Path(arguments.table).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("label counts: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print(f"cells: {len(repark_cells)} non-EQUAL: {len(non_equal)}")
    for line in non_equal:
        print(line)
    return 0


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set-operation domain probe over reordered struct fields (measurement only)."
    )
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--spark")
    parser.add_argument("--main")
    parser.add_argument("--table")
    parser.add_argument("--engine", choices=("spark", "repark"))
    parser.add_argument("--label")
    parser.add_argument("--warehouse")
    parser.add_argument("--out")
    parser.add_argument("--ivy-jar")
    parser.add_argument("--wheel")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    if arguments.render:
        return render(arguments)
    specs = plan()
    if arguments.plan_only:
        print(f"cells planned: {len(specs)}")
        return 0
    missing = [
        name for name in ("engine", "label", "warehouse", "out") if getattr(arguments, name) is None
    ]
    if arguments.engine == "spark" and arguments.ivy_jar is None:
        missing.append("ivy-jar")
    if arguments.engine == "repark" and arguments.wheel is None:
        missing.append("wheel")
    if missing:
        print(f"missing arguments: {', '.join(missing)}", file=sys.stderr)
        return 2
    warehouse = Path(arguments.warehouse)
    if warehouse.exists() and any(warehouse.iterdir()):
        print("warehouse must be empty or absent", file=sys.stderr)
        return 2
    warehouse.mkdir(parents=True)
    context = build_context(arguments, warehouse)
    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    outcomes: Counter[str] = Counter()
    with out.open("w", encoding="utf-8") as handle:
        write_line(handle, context["header"], context["scrub"])
        for spec in specs:
            record = run_spec(context, spec)
            outcomes[record["outcome"]] += 1
            write_line(handle, record, context["scrub"])
            handle.flush()
    context["close"]()
    print(f"cells: {len(specs)} outcomes: {dict(sorted(outcomes.items()))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
