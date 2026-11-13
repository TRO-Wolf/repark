from __future__ import annotations

import argparse
import importlib.util
import json
import struct
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("avro_bits", HERE / "avro_bits.py")
AVRO = importlib.util.module_from_spec(SPEC)
sys.modules["avro_bits"] = AVRO
SPEC.loader.exec_module(AVRO)

CATALOG = "sc"
NS = "ns"
SOURCE_VIEW = "nwc_src"
COLUMNS = "id BIGINT, label STRING, d DOUBLE, f FLOAT"
D_ID = 3
F_ID = 4


def from_bits(text: str) -> float:
    return struct.unpack(">d", bytes.fromhex(text))[0]


SOURCE = (
    ("-1.0", -1.0, -1.0),
    ("NaN", float("nan"), float("nan")),
    ("Infinity", float("inf"), float("inf")),
    ("-Infinity", float("-inf"), float("-inf")),
    ("payload", from_bits("7ff8deadbeef0001"), from_bits("7ff8002460000000")),
    ("negpayload", from_bits("fff8deadbeef0001"), from_bits("fff8002460000000")),
    ("-0.0", -0.0, -0.0),
    ("0.0", 0.0, 0.0),
    ("1.5", 1.5, 1.5),
    ("null", None, None),
)

ROWS = (
    (1, "sqrt-col", "sqrt(x)", "CAST(sqrt(y) AS FLOAT)", "-1.0"),
    (
        2,
        "sqrt-lit",
        "sqrt(CAST(-1.0 AS DOUBLE))",
        "CAST(sqrt(CAST(-1.0 AS DOUBLE)) AS FLOAT)",
        "-1.0",
    ),
    (3, "neg-nan-col", "-x", "-y", "NaN"),
    (4, "inf-minus-inf-col", "x - x", "y - y", "Infinity"),
    (5, "cast-nan", "CAST('NaN' AS DOUBLE)", "CAST('NaN' AS FLOAT)", "1.5"),
    (6, "payload", "x", "y", "payload"),
    (7, "neg-payload", "x", "y", "negpayload"),
    (8, "nan-col", "x", "y", "NaN"),
    (9, "neg-zero", "x", "y", "-0.0"),
    (10, "zero", "x", "y", "0.0"),
    (11, "inf", "x", "y", "Infinity"),
    (12, "neg-inf", "x", "y", "-Infinity"),
    (13, "null", "x", "y", "null"),
    (14, "one-five", "x", "y", "1.5"),
)

NATIVE_ROWS = tuple(row for row in ROWS if row[4] not in ("payload", "negpayload"))

UPDATES = (
    ("sqrt-col", "sqrt(d)", "CAST(sqrt(f) AS FLOAT)"),
    ("sqrt-lit", "sqrt(CAST(-1.0 AS DOUBLE))", "CAST(sqrt(CAST(-1.0 AS DOUBLE)) AS FLOAT)"),
    ("neg-nan-col", "-d", "-f"),
    ("inf-minus-inf-col", "d - d", "f - f"),
    ("cast-nan", "CAST('NaN' AS DOUBLE)", "CAST('NaN' AS FLOAT)"),
)

MOR = (
    "TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read', "
    "'write.delete.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')"
)
COW = (
    "TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'copy-on-write', "
    "'write.delete.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write')"
)


def rows_sql(rows: tuple[tuple[int, str, str, str, str], ...], source: str) -> str:
    parts = [
        f"SELECT CAST({row_id} AS BIGINT) AS id, '{label}' AS label, CAST({d} AS DOUBLE) AS d, "
        f"CAST({f} AS FLOAT) AS f FROM {source} WHERE k = '{key}'"
        for row_id, label, d, f, key in rows
    ]
    return " UNION ALL ".join(parts)


def raw_rows_sql(rows: tuple[tuple[int, str, str, str, str], ...], source: str) -> str:
    parts = [
        f"SELECT CAST({row_id} AS BIGINT) AS id, '{label}' AS label, x AS d, y AS f "
        f"FROM {source} WHERE k = '{key}'"
        for row_id, label, _, _, key in rows
    ]
    return " UNION ALL ".join(parts)


def bits_of(value: Any, width: int) -> Any:
    if value is None:
        return None
    if width == 4:
        return struct.pack(">f", value).hex()
    return struct.pack(">d", value).hex()


def leaf_bits(array: Any) -> Any:
    import pyarrow as pa

    kind = array.type
    if pa.types.is_floating(kind):
        width = 8 if pa.types.is_float64(kind) else 4
        data = array.buffers()[1].to_pybytes()
        start = array.offset * width
        raw = [
            int.from_bytes(data[start + i * width : start + (i + 1) * width], "little")
            for i in range(len(array))
        ]
        nulls = array.is_null().to_pylist()
        return [None if nulls[i] else format(raw[i], f"0{width * 2}x") for i in range(len(raw))]
    if pa.types.is_struct(kind):
        return {kind.field(i).name: leaf_bits(child) for i, child in enumerate(array.flatten())}
    if pa.types.is_map(kind):
        return leaf_bits(array.items)
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        return leaf_bits(array.flatten())
    return None


def parquet_rows(path: Path) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    table = pq.ParquetFile(path).read().combine_chunks()
    ids = table.column("id").to_pylist()
    columns = {
        name: leaf_bits(table.column(name).chunk(0))
        for name in table.column_names
        if name not in ("id", "label")
    }
    out = []
    for index, row_id in enumerate(ids):
        item: dict[str, Any] = {"id": row_id}
        for name, bits in columns.items():
            if isinstance(bits, list) and len(bits) == len(ids):
                item[name] = bits[index]
        out.append(item)
    if any(not isinstance(bits, list) or len(bits) != len(ids) for bits in columns.values()):
        out.append({"nested": columns})
    return out


def disk_files(table_root: Path) -> list[dict[str, Any]]:
    entries = AVRO.current_entries(table_root)
    by_name = {path.name: path for path in table_root.rglob("*.parquet")}
    for entry in entries:
        path = by_name.get(entry["file"])
        if entry["content"] == 0 and path is not None:
            entry["rows"] = parquet_rows(path)
    return entries


def attempt(action: Callable[[], Any]) -> dict[str, Any]:
    try:
        return {"outcome": "ok", "value": action()}
    except Exception as error:
        lines = str(error).strip().splitlines() or [type(error).__name__]
        return {
            "outcome": "error",
            "error_type": type(error).__name__,
            "error": lines[0][:400],
            "where": traceback.format_exc().strip().splitlines()[-1][:200],
        }


def collect_ids(session: Any, text: str) -> list[Any]:
    return [next(iter(row)) for row in session.sql(text).collect()]


def read_checks(session: Any, table: str, nested: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if nested:
        out["rows"] = attempt(
            lambda: [
                repr(list(row))
                for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()
            ]
        )
        return out
    out["rows"] = attempt(
        lambda: [
            [row[0], bits_of(row[1], 8), bits_of(row[2], 4)]
            for row in session.sql(f"SELECT id, d, f FROM {table} ORDER BY id").collect()
        ]
    )
    for column, kind in (("d", "DOUBLE"), ("f", "FLOAT")):
        out[f"isnan_{column}"] = attempt(
            lambda c=column: collect_ids(
                session, f"SELECT id FROM {table} WHERE isnan({c}) ORDER BY id"
            )
        )
        out[f"eq_nan_{column}"] = attempt(
            lambda c=column, k=kind: collect_ids(
                session, f"SELECT id FROM {table} WHERE {c} = CAST('NaN' AS {k}) ORDER BY id"
            )
        )
    out["files"] = attempt(
        lambda: sorted(
            [repr(_norm(row[0])), row[1]]
            for row in session.sql(f"SELECT partition, record_count FROM {table}.files").collect()
        )
    )
    return out


def _norm(value: Any) -> Any:
    if hasattr(value, "asDict"):
        value = value.asDict()
    if isinstance(value, dict):
        return {key: _norm(item) for key, item in value.items()}
    if isinstance(value, float):
        return bits_of(value, 8)
    return value


def create_sql(table: str, partitioned: bool, props: str = "") -> str:
    clause = " PARTITIONED BY (d, f)" if partitioned else ""
    return f"CREATE TABLE {table} ({COLUMNS}) USING iceberg{clause} {props}".strip()


def run(session: Any, statements: list[str]) -> None:
    for text in statements:
        session.sql(text).collect()


def door_actions(session: Any, functions: Any) -> dict[str, Callable[[str, bool], None]]:
    source = rows_sql(ROWS, SOURCE_VIEW)

    def frame() -> Any:
        return session.sql(source)

    def partition_columns() -> list[Any]:
        return [functions.col("d"), functions.col("f")]

    def insert(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned), f"INSERT INTO {table} {source}"])

    def insert_overwrite(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned), f"INSERT OVERWRITE {table} {source}"])

    def ctas(table: str, partitioned: bool) -> None:
        clause = " PARTITIONED BY (d, f)" if partitioned else ""
        run(session, [f"CREATE TABLE {table} USING iceberg{clause} AS {source}"])

    def rtas(table: str, partitioned: bool) -> None:
        clause = " PARTITIONED BY (d, f)" if partitioned else ""
        run(
            session,
            [
                create_sql(table, partitioned),
                f"INSERT INTO {table} SELECT CAST(99 AS BIGINT), 'seed', "
                "CAST(2.0 AS DOUBLE), CAST(2.0 AS FLOAT)",
                f"CREATE OR REPLACE TABLE {table} USING iceberg{clause} AS {source}",
            ],
        )

    def writeto_create(table: str, partitioned: bool) -> None:
        writer = frame().writeTo(table).using("iceberg")
        if partitioned:
            writer = writer.partitionedBy(*partition_columns())
        writer.create()

    def writeto_append(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        frame().writeTo(table).append()

    def writeto_overwrite(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        frame().writeTo(table).overwrite(functions.lit(True))

    def writeto_overwrite_partitions(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        frame().writeTo(table).overwritePartitions()

    def writeto_create_or_replace(table: str, partitioned: bool) -> None:
        writer = frame().writeTo(table).using("iceberg")
        if partitioned:
            writer = writer.partitionedBy(*partition_columns())
        writer.createOrReplace()

    def saveastable_append(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        frame().write.format("iceberg").mode("append").saveAsTable(table)

    def saveastable_overwrite(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        writer = frame().write.format("iceberg").mode("overwrite")
        if partitioned:
            writer = writer.partitionBy("d", "f")
        writer.saveAsTable(table)

    def insertinto(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        frame().write.insertInto(table)

    def merge_insert(table: str, partitioned: bool) -> None:
        run(
            session,
            [
                create_sql(table, partitioned),
                f"MERGE INTO {table} t USING ({source}) s ON t.id = s.id "
                "WHEN NOT MATCHED THEN INSERT *",
            ],
        )

    def merge_update(mode: str) -> Callable[[str, bool], None]:
        def action(table: str, partitioned: bool) -> None:
            run(
                session,
                [
                    create_sql(table, partitioned, MOR if mode == "mor" else COW),
                    f"INSERT INTO {table} SELECT id, label, CAST(2.0 AS DOUBLE), "
                    f"CAST(2.0 AS FLOAT) FROM ({source})",
                    f"MERGE INTO {table} t USING ({source}) s ON t.id = s.id "
                    "WHEN MATCHED THEN UPDATE SET d = s.d, f = s.f",
                ],
            )

        return action

    def update(mode: str) -> Callable[[str, bool], None]:
        def action(table: str, partitioned: bool) -> None:
            statements = [
                create_sql(table, partitioned, MOR if mode == "mor" else COW),
                f"INSERT INTO {table} {raw_rows_sql(ROWS, SOURCE_VIEW)}",
            ]
            statements += [
                f"UPDATE {table} SET d = {d}, f = {f} WHERE label = '{label}'"
                for label, d, f in UPDATES
            ]
            run(session, statements)

        return action

    def delete(mode: str) -> Callable[[str, bool], None]:
        def action(table: str, partitioned: bool) -> None:
            run(
                session,
                [
                    create_sql(table, partitioned, MOR if mode == "mor" else COW),
                    f"INSERT INTO {table} {source}",
                    f"DELETE FROM {table} WHERE label = 'one-five'",
                ],
            )

        return action

    def save_name(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        frame().write.format("iceberg").mode("append").save(table)

    def path_append(table: str, partitioned: bool) -> None:
        run(session, [create_sql(table, partitioned)])
        location = session.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        root = next(row[1] for row in location if row[0] == "Location")
        frame().write.format("iceberg").mode("append").save(root)

    return {
        "insert": insert,
        "insert_overwrite": insert_overwrite,
        "ctas": ctas,
        "rtas": rtas,
        "writeto_create": writeto_create,
        "writeto_append": writeto_append,
        "writeto_overwrite": writeto_overwrite,
        "writeto_overwrite_partitions": writeto_overwrite_partitions,
        "writeto_create_or_replace": writeto_create_or_replace,
        "saveastable_append": saveastable_append,
        "saveastable_overwrite": saveastable_overwrite,
        "insertinto": insertinto,
        "merge_insert": merge_insert,
        "merge_update_cow": merge_update("cow"),
        "merge_update_mor": merge_update("mor"),
        "update_cow": update("cow"),
        "update_mor": update("mor"),
        "delete_cow": delete("cow"),
        "delete_mor": delete("mor"),
        "save_name": save_name,
        "path_append": path_append,
    }


def open_session(engine: str, warehouse: Path, jar: str) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession, functions

        builder = (
            SparkSession.builder.master("local[2]")
            .appName("nan-write-canonical-1")
            .config("spark.jars", jar)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        )
    else:
        from repark import ReparkSession, functions

        builder = ReparkSession.builder.appName("nan-write-canonical-1")
    session = (
        builder.config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )
    return session, functions


def source_view(session: Any) -> dict[str, Any]:
    payload = [row for row in SOURCE if row[0] in ("payload", "negpayload")]
    session.createDataFrame(payload, schema="k STRING, x DOUBLE, y FLOAT").createOrReplaceTempView(
        f"{SOURCE_VIEW}_payload"
    )
    values = ", ".join(
        f"('{key}', CAST({_lit(x)} AS DOUBLE), CAST({_lit(y)} AS FLOAT))"
        for key, x, y in SOURCE
        if key not in ("payload", "negpayload")
    )
    session.sql(
        f"SELECT k, x, y FROM (VALUES {values}) AS v(k, x, y) "
        f"UNION ALL SELECT k, x, y FROM {SOURCE_VIEW}_payload"
    ).createOrReplaceTempView(SOURCE_VIEW)
    return attempt(
        lambda: [
            [row[0], bits_of(row[1], 8), bits_of(row[2], 4)]
            for row in session.sql(f"SELECT k, x, y FROM {SOURCE_VIEW}").collect()
        ]
    )


def emit(sink: Any, record: dict[str, Any]) -> None:
    sink.write(json.dumps(record, sort_keys=True) + "\n")
    sink.flush()


def table_root(warehouse: Path, name: str) -> Path:
    return warehouse / NS / name


def leg_doors(
    session: Any, functions: Any, warehouse: Path, engine: str, sink: Any, only: set[str]
) -> None:
    actions = door_actions(session, functions)
    for door, action in actions.items():
        if only and door not in only:
            continue
        for partitioned in (False, True):
            name = f"{'p' if partitioned else 'u'}_{door}"
            table = f"{CATALOG}.{NS}.{name}"
            result = attempt(lambda a=action, t=table, p=partitioned: a(t, p))
            record = {
                "record": "door",
                "engine": engine,
                "door": door,
                "table": name,
                "partitioned": partitioned,
            }
            emit(sink, {**record, **{k: v for k, v in result.items() if k != "value"}})
            if result["outcome"] != "ok":
                continue
            emit(sink, {**record, "record": "checks", **read_checks(session, table)})
            for item in disk_files(table_root(warehouse, name)):
                emit(sink, {**record, "record": "file", **item})


def leg_nested(session: Any, warehouse: Path, engine: str, sink: Any) -> None:
    source = rows_sql(ROWS, SOURCE_VIEW)
    select = (
        f"SELECT id, named_struct('a', d, 'b', f) AS s, array(d, CAST(1.5 AS DOUBLE)) AS a, "
        f"map('k', d) AS m, array(named_struct('e', f)) AS af FROM ({source})"
    )
    for door in ("ctas", "insert"):
        name = f"n_{door}"
        table = f"{CATALOG}.{NS}.{name}"
        if door == "ctas":
            statements = [f"CREATE TABLE {table} USING iceberg AS {select}"]
        else:
            statements = [
                f"CREATE TABLE {table} (id BIGINT, s STRUCT<a: DOUBLE, b: FLOAT>, a ARRAY<DOUBLE>, "
                "m MAP<STRING, DOUBLE>, af ARRAY<STRUCT<e: FLOAT>>) USING iceberg",
                f"INSERT INTO {table} {select}",
            ]
        result = attempt(lambda s=statements: run(session, s))
        record = {
            "record": "door",
            "engine": engine,
            "door": f"nested_{door}",
            "table": name,
            "partitioned": False,
        }
        emit(sink, {**record, **{k: v for k, v in result.items() if k != "value"}})
        if result["outcome"] != "ok":
            continue
        for item in disk_files(table_root(warehouse, name)):
            emit(sink, {**record, "record": "file", **item})


def leg_plain(session: Any, warehouse: Path, engine: str, sink: Any) -> None:
    target = warehouse / "plain_parquet"
    result = attempt(
        lambda: (
            session.sql(rows_sql(ROWS, SOURCE_VIEW)).write.mode("overwrite").parquet(str(target))
        )
    )
    record = {
        "record": "door",
        "engine": engine,
        "door": "plain_parquet_path",
        "table": "plain",
        "partitioned": False,
    }
    emit(sink, {**record, **{k: v for k, v in result.items() if k != "value"}})
    for path in sorted(target.rglob("*.parquet")):
        emit(sink, {**record, "record": "file", "file": path.name, "rows": parquet_rows(path)})


def leg_rewrite(session: Any, warehouse: Path, engine: str, sink: Any, source: str) -> None:
    run(session, [f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}"])
    for name, row_id in (("u_merge_insert", 14), ("p_insert", 8)):
        table = f"{CATALOG}.{NS}.{name}"
        if source:
            warehouse = Path(source)
            metadata = max(
                (warehouse / NS / name / "metadata").glob("v*.metadata.json"),
                key=lambda path: int(path.name[1:].split(".")[0]),
            )
            run(
                session,
                [
                    f"CALL {CATALOG}.system.register_table(table => '{NS}.{name}', "
                    f"metadata_file => '{metadata}')"
                ],
            )
        record = {"record": "rewrite", "engine": engine, "table": name, "row": row_id}
        emit(sink, {**record, "phase": "before", "files": disk_files(table_root(warehouse, name))})
        result = attempt(
            lambda t=table, r=row_id: run(
                session, [f"UPDATE {t} SET label = 'touched' WHERE id = {r}"]
            )
        )
        emit(
            sink, {**record, "phase": "update", **{k: v for k, v in result.items() if k != "value"}}
        )
        emit(sink, {**record, "phase": "after", "files": disk_files(table_root(warehouse, name))})
        emit(sink, {**record, "phase": "checks", **read_checks(session, table)})


def leg_native(warehouse: Path, sink: Any) -> None:
    import repark
    from repark._native import PyReparkSession

    native = PyReparkSession.native()
    warehouse.mkdir(parents=True, exist_ok=True)
    native.register_memory_catalog("nat", str(warehouse))
    native.create_namespace("nat", NS, str(warehouse / NS))
    repark._ANSI_NATIVE = native
    values = ", ".join(
        f"('{key}', CAST({_lit(x)} AS DOUBLE), CAST({_lit(y)} AS FLOAT))"
        for key, x, y in SOURCE
        if key not in ("payload", "negpayload")
    )
    setup = attempt(
        lambda: [
            repark.sql(text).to_arrow()
            for text in (
                f"CREATE TABLE nat.{NS}.src (k STRING, x DOUBLE, y FLOAT)",
                f"INSERT INTO nat.{NS}.src VALUES {values}",
            )
        ]
    )
    emit(sink, {"record": "native_setup", **{k: v for k, v in setup.items() if k != "value"}})
    source = rows_sql(NATIVE_ROWS, f"nat.{NS}.src")
    cells = {
        "native_ctas": [f"CREATE TABLE nat.{NS}.{{t}} AS {source}"],
        "native_insert": [
            f"CREATE TABLE nat.{NS}.{{t}} (id BIGINT, label STRING, d DOUBLE, f FLOAT)",
            f"INSERT INTO nat.{NS}.{{t}} {source}",
        ],
        "native_insert_partitioned": [
            f"CREATE TABLE nat.{NS}.{{t}} (id BIGINT, label STRING, d DOUBLE, f FLOAT) "
            "PARTITIONED BY (d, f)",
            f"INSERT INTO nat.{NS}.{{t}} {source}",
        ],
        "native_merge": [
            f"CREATE TABLE nat.{NS}.{{t}} (id BIGINT, label STRING, d DOUBLE, f FLOAT)",
            f"MERGE INTO nat.{NS}.{{t}} t USING ({source}) s ON t.id = s.id WHEN NOT MATCHED THEN "
            "INSERT (id, label, d, f) VALUES (s.id, s.label, s.d, s.f)",
        ],
        "native_update": [
            f"CREATE TABLE nat.{NS}.{{t}} (id BIGINT, label STRING, d DOUBLE, f FLOAT)",
            f"INSERT INTO nat.{NS}.{{t}} {raw_rows_sql(NATIVE_ROWS, f'nat.{NS}.src')}",
        ]
        + [
            f"UPDATE nat.{NS}.{{t}} SET d = {d}, f = {f} WHERE label = '{label}'"
            for label, d, f in UPDATES
        ],
        "native_delete": [
            f"CREATE TABLE nat.{NS}.{{t}} (id BIGINT, label STRING, d DOUBLE, f FLOAT)",
            f"INSERT INTO nat.{NS}.{{t}} {source}",
            f"DELETE FROM nat.{NS}.{{t}} WHERE label = 'one-five'",
        ],
    }
    for door, statements in cells.items():
        name = f"u_{door}"
        result = attempt(
            lambda s=statements, n=name: [repark.sql(text.format(t=n)).to_arrow() for text in s]
        )
        record = {
            "record": "door",
            "engine": "repark-native",
            "door": door,
            "table": name,
            "partitioned": False,
        }
        emit(sink, {**record, **{k: v for k, v in result.items() if k != "value"}})
        if result["outcome"] != "ok":
            continue
        rows = attempt(
            lambda n=name: [
                [r["id"], bits_of(r["d"], 8), bits_of(r["f"], 4)]
                for r in repark.sql(f"SELECT id, d, f FROM nat.{NS}.{n} ORDER BY id")
                .to_arrow()
                .to_pylist()
            ]
        )
        emit(sink, {**record, "record": "checks", "rows": rows})
        roots = [path for path in (warehouse / NS).glob(f"{name}*") if path.is_dir()]
        for root in roots:
            for item in disk_files(root):
                emit(sink, {**record, "record": "file", **item})


def _lit(value: Any) -> str:
    if value is None:
        return "NULL"
    if value != value:
        return "'NaN'"
    if value in (float("inf"), float("-inf")):
        return "'Infinity'" if value > 0 else "'-Infinity'"
    return f"'{value!r}'"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("repark", "spark"), required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ivy-jar", default="")
    parser.add_argument("--legs", default="doors,nested,plain,native")
    parser.add_argument("--only", default="")
    parser.add_argument("--source", default="")
    arguments = parser.parse_args()
    warehouse = Path(arguments.warehouse)
    legs = set(arguments.legs.split(","))
    if "rewrite" not in legs or arguments.source:
        warehouse.mkdir(parents=True, exist_ok=False)
    session, functions = open_session(arguments.engine, warehouse, arguments.ivy_jar)
    only = {item for item in arguments.only.split(",") if item}
    with Path(arguments.out).open("w", encoding="utf-8") as sink:
        emit(
            sink,
            {
                "record": "header",
                "engine": arguments.engine,
                "version": session.version,
                "legs": sorted(legs),
            },
        )
        if "rewrite" in legs:
            leg_rewrite(session, warehouse, arguments.engine, sink, arguments.source)
            return 0
        run(session, [f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}"])
        emit(sink, {"record": "source", **source_view(session)})
        if "doors" in legs:
            leg_doors(session, functions, warehouse, arguments.engine, sink, only)
        if "nested" in legs:
            leg_nested(session, warehouse, arguments.engine, sink)
        if "plain" in legs:
            leg_plain(session, warehouse, arguments.engine, sink)
        if "native" in legs and arguments.engine == "repark":
            leg_native(warehouse / "native", sink)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
