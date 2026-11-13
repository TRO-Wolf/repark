from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("write_doors_probe", HERE / "write_doors_probe.py")
DOORS = importlib.util.module_from_spec(SPEC)
sys.modules["write_doors_probe"] = DOORS
SPEC.loader.exec_module(DOORS)
AVRO = DOORS.AVRO

CATALOG = DOORS.CATALOG
NS = DOORS.NS
FORMATS = ("parquet", "orc", "avro")
NESTED = "id BIGINT, s STRUCT<a: DOUBLE, b: FLOAT>, a ARRAY<DOUBLE>, m MAP<STRING, DOUBLE>"


def props(fmt: str) -> str:
    return f"TBLPROPERTIES ('format-version' = '2', 'write.format.default' = '{fmt}')"


def data_rows(path: Path) -> list[Any]:
    if path.suffix == ".avro":
        return AVRO.read_avro(path)
    if path.suffix == ".orc":
        import pyarrow.orc as po

        table = po.ORCFile(str(path)).read().combine_chunks()
    else:
        import pyarrow.parquet as pq

        table = pq.ParquetFile(path).read().combine_chunks()
    columns = {name: DOORS.leaf_bits(table.column(name).chunk(0)) for name in table.column_names}
    return [{"columns": columns, "ids": table.column("id").to_pylist()}]


def files(root: Path) -> list[dict[str, Any]]:
    entries = AVRO.current_entries(root)
    by_name = {path.name: path for path in root.rglob("*") if path.is_file()}
    for entry in entries:
        path = by_name.get(entry["file"])
        if entry["content"] == 0 and path is not None:
            entry["data"] = data_rows(path)
    return entries


def emit(sink: Any, record: dict[str, Any]) -> None:
    sink.write(json.dumps(record, sort_keys=True) + "\n")
    sink.flush()


ONLY: set[str] = set()


def cell(session: Any, warehouse: Path, sink: Any, name: str, statements: list[str]) -> None:
    if ONLY and name.split("_")[0] not in ONLY:
        return
    result = DOORS.attempt(lambda: DOORS.run(session, statements))
    record = {"record": "cell", "table": name, **{k: v for k, v in result.items() if k != "value"}}
    if result["outcome"] == "ok":
        record["files"] = files(warehouse / NS / name)
    emit(sink, record)


def leg_formats(session: Any, warehouse: Path, sink: Any) -> None:
    source = DOORS.rows_sql(DOORS.ROWS, DOORS.SOURCE_VIEW)
    nested = (
        "SELECT id, named_struct('a', d, 'b', f) AS s, array(d, CAST(1.5 AS DOUBLE)) AS a, "
        f"map('k', d) AS m FROM ({source})"
    )
    for fmt in FORMATS:
        table = f"{CATALOG}.{NS}"
        cell(
            session,
            warehouse,
            sink,
            f"u_{fmt}",
            [
                f"CREATE TABLE {table}.u_{fmt} ({DOORS.COLUMNS}) USING iceberg {props(fmt)}",
                f"INSERT INTO {table}.u_{fmt} {source}",
            ],
        )
        cell(
            session,
            warehouse,
            sink,
            f"p_{fmt}",
            [
                f"CREATE TABLE {table}.p_{fmt} ({DOORS.COLUMNS}) USING iceberg "
                f"PARTITIONED BY (d, f) {props(fmt)}",
                f"INSERT INTO {table}.p_{fmt} {source}",
            ],
        )
        cell(
            session,
            warehouse,
            sink,
            f"c_{fmt}",
            [
                f"CREATE TABLE {table}.c_{fmt} USING iceberg PARTITIONED BY (d, f) {props(fmt)} "
                f"AS {source}"
            ],
        )
        cell(
            session,
            warehouse,
            sink,
            f"n_{fmt}",
            [
                f"CREATE TABLE {table}.n_{fmt} ({NESTED}) USING iceberg {props(fmt)}",
                f"INSERT INTO {table}.n_{fmt} {nested}",
            ],
        )
        payloads = (
            f"SELECT a.x AS x1, b.x AS x2 FROM {DOORS.SOURCE_VIEW} a, {DOORS.SOURCE_VIEW} b "
            "WHERE a.k = 'payload' AND b.k = 'negpayload'"
        )
        cell(
            session,
            warehouse,
            sink,
            f"k1_{fmt}",
            [
                f"CREATE TABLE {table}.k1_{fmt} (id BIGINT, mk MAP<DOUBLE, INT>) "
                f"USING iceberg {props(fmt)}",
                f"INSERT INTO {table}.k1_{fmt} SELECT CAST(1 AS BIGINT), map(x2, 1) "
                f"FROM ({payloads})",
            ],
        )
        cell(
            session,
            warehouse,
            sink,
            f"k2_{fmt}",
            [
                f"CREATE TABLE {table}.k2_{fmt} (id BIGINT, mk MAP<DOUBLE, INT>) "
                f"USING iceberg {props(fmt)}",
                f"INSERT INTO {table}.k2_{fmt} SELECT CAST(1 AS BIGINT), map(x1, 1, x2, 2) "
                f"FROM ({payloads})",
            ],
        )


def leg_rewrite(session: Any, warehouse: Path, sink: Any, source: str) -> None:
    for fmt in FORMATS:
        name = f"u_{fmt}"
        if source:
            metadata = max(
                (Path(source) / NS / name / "metadata").glob("v*.metadata.json"),
                key=lambda path: int(path.name[1:].split(".")[0]),
            )
            DOORS.run(
                session,
                [
                    f"CALL {CATALOG}.system.register_table(table => '{NS}.{name}', "
                    f"metadata_file => '{metadata}')"
                ],
            )
            root = Path(source) / NS / name
        else:
            root = warehouse / NS / name
        emit(sink, {"record": "rewrite", "phase": "before", "table": name, "files": files(root)})
        result = DOORS.attempt(
            lambda n=name: DOORS.run(
                session,
                [
                    f"CALL {CATALOG}.system.rewrite_data_files(table => '{NS}.{n}', "
                    "options => map('rewrite-all', 'true'))"
                ],
            )
        )
        emit(
            sink,
            {
                "record": "rewrite",
                "phase": "call",
                "table": name,
                **{k: v for k, v in result.items() if k != "value"},
            },
        )
        emit(sink, {"record": "rewrite", "phase": "after", "table": name, "files": files(root)})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("repark", "spark"), required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ivy-jar", default="")
    parser.add_argument("--legs", default="formats")
    parser.add_argument("--source", default="")
    parser.add_argument("--tables", default="")
    arguments = parser.parse_args()
    ONLY.update(item for item in arguments.tables.split(",") if item)
    warehouse = Path(arguments.warehouse)
    in_place = arguments.legs == "rewrite" and not arguments.source
    warehouse.mkdir(parents=True, exist_ok=in_place)
    session, _ = DOORS.open_session(arguments.engine, warehouse, arguments.ivy_jar)
    legs = set(arguments.legs.split(","))
    with Path(arguments.out).open("w", encoding="utf-8") as sink:
        emit(sink, {"record": "header", "engine": arguments.engine, "version": session.version})
        DOORS.run(session, [f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}"])
        emit(sink, {"record": "source", **DOORS.source_view(session)})
        if "formats" in legs:
            leg_formats(session, warehouse, sink)
        if "rewrite" in legs:
            leg_rewrite(session, warehouse, sink, arguments.source)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
