from __future__ import annotations

import json
import math
import os
import shutil
import sys
from pathlib import Path
from typing import Any

JAR = os.environ.get("ICEBERG_SPARK_JAR", "iceberg-spark-runtime-4.1_2.13-1.11.0.jar")
D_BITS = {"c": 0x7FF8000000000000, "s": 0xFFF8000000000000, "p": 0x7FF8DEADBEEF0001}
F_BITS = {"c": 0x7FC00000, "s": 0xFFC00000, "p": 0x7FC00123}
SHAPES = ("c", "s", "p")
LAYOUTS = {"one": (1, 4, 4), "rg": (1, 6, 2), "many": (4, 2, 2)}
SOURCES = [f"{layout}_{shape}" for layout in LAYOUTS for shape in SHAPES] + ["ctl"]
PATH_SOURCE = "one_c"
INGEST_FORMATS = {"one_c": ("parquet", "avro")}
TABLE_DDL = "id BIGINT, d DOUBLE, f FLOAT"


def nan_column(count: int, shape: str | None, wide: bool) -> Any:
    import numpy as np
    import pyarrow as pa

    if wide:
        values = np.full(count, 1.0, dtype=np.float64)
        if shape is not None:
            values[0::2] = np.array([D_BITS[shape]], dtype=np.uint64).view(np.float64)[0]
        return pa.array(values, pa.float64())
    values32 = np.full(count, 1.0, dtype=np.float32)
    if shape is not None:
        values32[0::2] = np.array([F_BITS[shape]], dtype=np.uint32).view(np.float32)[0]
    return pa.array(values32, pa.float32())


def write_files(root: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if root.exists():
        shutil.rmtree(root)
    next_id = 1
    for layout, (files, rows, group) in LAYOUTS.items():
        for shape in SHAPES:
            folder = root / "src" / f"{layout}_{shape}"
            folder.mkdir(parents=True)
            for index in range(files):
                ids = pa.array(range(next_id, next_id + rows), pa.int64())
                next_id += rows
                table = pa.table(
                    {
                        "id": ids,
                        "d": nan_column(rows, shape, True),
                        "f": nan_column(rows, shape, False),
                    }
                )
                pq.write_table(table, folder / f"part-{index}.parquet", row_group_size=group)
    folder = root / "src" / "ctl"
    folder.mkdir(parents=True)
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "d": nan_column(2, None, True),
            "f": nan_column(2, None, False),
        }
    )
    pq.write_table(table, folder / "part-0.parquet")


def token(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return repr(value)
    return value


def nan_bits(array: Any) -> list[str]:
    import pyarrow as pa

    if pa.types.is_float64(array.type):
        words, width = array.view(pa.uint64()).to_pylist(), 16
    elif pa.types.is_float32(array.type):
        words, width = array.view(pa.uint32()).to_pylist(), 8
    else:
        return []
    values = array.to_pylist()
    return [
        f"{w:0{width}x}" for v, w in zip(values, words, strict=True) if v is not None and v != v
    ]


def from_arrow(table: Any) -> dict[str, Any]:
    table = table.combine_chunks()
    rows = [[token(v) for v in row.values()] for row in table.to_pylist()]
    bits: list[str] = []
    for column in table.columns:
        for chunk in column.chunks:
            bits.extend(nan_bits(chunk))
    return {"rows": rows, "nan_bits": sorted(set(bits))}


def collect(frame: Any) -> dict[str, Any]:
    if hasattr(frame, "toArrow"):
        return from_arrow(frame.toArrow())
    return from_arrow(frame.to_arrow())


def attempt(action: Any) -> dict[str, Any]:
    try:
        return {"ok": True, **action()}
    except Exception as error:
        text = f"{type(error).__name__}: {error!s}"
        return {"ok": False, "error": text.splitlines()[0][:240]}


def stored(folder: Path) -> dict[str, Any]:
    import pyarrow.parquet as pq

    files = sorted(p for p in folder.rglob("*.parquet") if not p.name.startswith("."))
    if not files:
        return {"ok": False, "error": "no parquet files"}
    tables = [pq.read_table(p) for p in files]
    import pyarrow as pa

    merged = pa.concat_tables(tables).sort_by("id")
    return {"ok": True, **from_arrow(merged.select(["id", "d", "f"]))}


def queries(view: str) -> list[tuple[str, str]]:
    return [
        ("rows", f"SELECT id, d, f FROM {view} ORDER BY id"),
        ("count", f"SELECT count(*) FROM {view}"),
        ("isnan_d", f"SELECT count(*) FROM {view} WHERE isnan(d)"),
        ("isnan_f", f"SELECT count(*) FROM {view} WHERE isnan(f)"),
        ("minmax", f"SELECT max(d), min(d), max(f), min(f) FROM {view}"),
        ("eq_nan_d", f"SELECT count(*) FROM {view} WHERE d = CAST('NaN' AS DOUBLE)"),
    ]


class Leg:
    def __init__(self, engine: str, root: Path, tag: str, sink: Any) -> None:
        self.engine = engine
        self.root = root
        self.tag = tag
        self.sink = sink
        self.native: Any = None
        self.native_sql: Any = None
        warehouse = root / f"wh-{tag}"
        warehouse.mkdir(parents=True, exist_ok=True)
        if engine == "spark":
            from pyspark.sql import SparkSession

            builder = (
                SparkSession.builder.master("local[1]")
                .appName("nan-minmax-step0")
                .config("spark.jars", JAR)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC")
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.sc.type", "hadoop")
                .config("spark.sql.catalog.sc.warehouse", str(warehouse))
            )
            for other in sorted(root.glob("wh-*")):
                name = other.name[3:].replace("-", "_")
                if name == tag.replace("-", "_"):
                    continue
                builder = (
                    builder.config(
                        f"spark.sql.catalog.r_{name}", "org.apache.iceberg.spark.SparkCatalog"
                    )
                    .config(f"spark.sql.catalog.r_{name}.type", "hadoop")
                    .config(f"spark.sql.catalog.r_{name}.warehouse", str(other))
                )
            self.session = builder.getOrCreate()
            self.session.sparkContext.setLogLevel("ERROR")
        else:
            import repark
            from repark import ReparkSession
            from repark._native import PyReparkSession

            self.native = PyReparkSession.native(
                config={"repark.sql.allowLocalFilesystemDDL": "true"}
            )
            repark._ANSI_NATIVE = self.native
            self.native_sql = repark.sql
            self.session = (
                ReparkSession.builder.appName("nan-minmax-step0")
                .config("spark.sql.catalog.sc.type", "hadoop")
                .config("spark.sql.catalog.sc.warehouse", str(warehouse))
                .config("repark.sql.allowLocalFilesystemDDL", "true")
                .getOrCreate()
            )
        self.session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
        self.emit({"record": "leg", "engine": engine, "tag": tag, "python": sys.version.split()[0]})

    def emit(self, record: dict[str, Any]) -> None:
        self.sink.write(json.dumps(record, sort_keys=True) + "\n")
        self.sink.flush()

    def cell(self, group: str, source: str, door: str, name: str, result: dict[str, Any]) -> None:
        self.emit(
            {
                "record": "cell",
                "group": group,
                "source": source,
                "door": door,
                "cell": name,
                **result,
            }
        )

    def doors(self) -> list[str]:
        return ["sql", "native"] if self.engine == "repark" else ["sql"]

    def sql(self, door: str, text: str) -> Any:
        return self.native_sql(text) if door == "native" else self.session.sql(text)

    def view(self, door: str, name: str, folder: Path) -> None:
        if door == "native":
            frame = self.native.read_parquet(str(folder))
            self.native.create_or_replace_temp_view(name, frame)
        else:
            self.session.read.parquet(str(folder)).createOrReplaceTempView(name)

    def reads(self) -> None:
        for source in SOURCES:
            folder = self.root / "src" / source
            for door in self.doors():
                view = f"v_{source}"
                made = attempt(lambda d=door, v=view, f=folder: (self.view(d, v, f), {})[1])
                if not made["ok"]:
                    self.cell("read", source, door, "view", made)
                    continue
                for name, text in queries(view):
                    self.cell(
                        "read",
                        source,
                        door,
                        name,
                        attempt(lambda d=door, t=text: collect(self.sql(d, t))),
                    )

    def read_paths(self) -> None:
        folder = self.root / "src" / PATH_SOURCE
        text = "SELECT id, d, f FROM {} ORDER BY id"
        reader = self.session.read
        self.cell(
            "path",
            PATH_SOURCE,
            "sql",
            "read_parquet",
            attempt(lambda: collect(reader.parquet(str(folder)).orderBy("id"))),
        )
        self.cell(
            "path",
            PATH_SOURCE,
            "sql",
            "format_load",
            attempt(
                lambda: collect(self.session.read.format("parquet").load(str(folder)).orderBy("id"))
            ),
        )
        self.cell(
            "path",
            PATH_SOURCE,
            "sql",
            "sql_parquet_path",
            attempt(lambda: collect(self.session.sql(text.format(f"parquet.`{folder}`")))),
        )
        self.cell(
            "path", PATH_SOURCE, "sql", "create_using_parquet", attempt(lambda: self.using_table())
        )
        if self.engine == "repark":
            self.cell(
                "path",
                PATH_SOURCE,
                "sql",
                "create_external_stored_parquet",
                attempt(lambda: self.external(self.session.sql, "t_ext_sql")),
            )
            self.cell(
                "path",
                PATH_SOURCE,
                "native",
                "read_parquet",
                attempt(lambda: self.native_view(folder)),
            )
            self.cell(
                "path",
                PATH_SOURCE,
                "native",
                "create_external_stored_parquet",
                attempt(lambda: self.external(self.native_sql, "t_ext_native")),
            )
            self.native.register_memory_catalog("extwh", str(self.root))
            self.cell(
                "path",
                PATH_SOURCE,
                "native",
                "create_external_under_warehouse",
                attempt(lambda: self.external(self.native_sql, "t_ext_native_wh")),
            )

    def using_table(self) -> dict[str, Any]:
        folder = self.root / "src" / PATH_SOURCE
        self.session.sql("DROP TABLE IF EXISTS t_using").collect()
        self.session.sql(f"CREATE TABLE t_using USING parquet LOCATION '{folder}'").collect()
        return collect(self.session.sql("SELECT id, d, f FROM t_using ORDER BY id"))

    def external(self, run: Any, name: str) -> dict[str, Any]:
        folder = self.root / "src" / PATH_SOURCE
        run(f"CREATE EXTERNAL TABLE {name} STORED AS PARQUET LOCATION '{folder}/'").collect()
        return collect(run(f"SELECT id, d, f FROM {name} ORDER BY id"))

    def native_view(self, folder: Path) -> dict[str, Any]:
        self.native.create_or_replace_temp_view("v_path", self.native.read_parquet(str(folder)))
        return collect(self.native_sql("SELECT id, d, f FROM v_path ORDER BY id"))

    def round_trip(self) -> None:
        for source in SOURCES:
            out = self.root / "rt" / self.tag / source
            if out.exists():
                shutil.rmtree(out)
            frame = self.session.read.parquet(str(self.root / "src" / source))
            wrote = attempt(lambda f=frame, o=out: (f.coalesce(1).write.parquet(str(o)), {})[1])
            self.cell("roundtrip", source, "sql", "write", wrote)
            if not wrote["ok"]:
                continue
            self.cell("roundtrip", source, "file", "stored", attempt(lambda o=out: stored(o)))
            self.cell(
                "roundtrip",
                source,
                "sql",
                "read_back",
                attempt(lambda o=out: collect(self.session.read.parquet(str(o)).orderBy("id"))),
            )

    def ingest(self) -> None:
        for source in SOURCES:
            for fmt in INGEST_FORMATS.get(source, ("parquet",)):
                name = f"sc.ns.ing_{source}_{fmt}"
                props = f"TBLPROPERTIES ('format-version' = '2', 'write.format.default' = '{fmt}')"

                def go(n: str = name, p: str = props, s: str = source) -> dict[str, Any]:
                    self.session.sql(f"DROP TABLE IF EXISTS {n}").collect()
                    self.session.sql(f"CREATE TABLE {n} ({TABLE_DDL}) USING iceberg {p}").collect()
                    self.session.read.parquet(str(self.root / "src" / s)).writeTo(n).append()
                    return {}

                wrote = attempt(go)
                self.cell("ingest", f"{source}_{fmt}", "sql", "write", wrote)
                if not wrote["ok"]:
                    continue
                self.cell(
                    "ingest",
                    f"{source}_{fmt}",
                    "sql",
                    "read_back",
                    attempt(
                        lambda n=name: collect(
                            self.session.sql(f"SELECT id, d, f FROM {n} ORDER BY id")
                        )
                    ),
                )
                if fmt == "parquet":
                    data = self.root / f"wh-{self.tag}" / "ns" / f"ing_{source}_{fmt}" / "data"
                    self.cell(
                        "ingest",
                        f"{source}_{fmt}",
                        "file",
                        "stored",
                        attempt(lambda d=data: stored(d)),
                    )

    def cross_reads(self) -> None:
        for other in sorted((self.root / "rt").glob("*")):
            if other.name == self.tag:
                continue
            for source in SOURCES:
                folder = other / source
                if folder.exists():
                    self.cell(
                        f"rt_by_{self.engine}",
                        f"{other.name}:{source}",
                        "sql",
                        "read",
                        attempt(
                            lambda f=folder: collect(
                                self.session.read.parquet(str(f)).orderBy("id")
                            )
                        ),
                    )
        if self.engine != "spark":
            return
        for other in sorted(self.root.glob("wh-*")):
            name = other.name[3:].replace("-", "_")
            if other.name == f"wh-{self.tag}":
                continue
            for source in SOURCES:
                for fmt in INGEST_FORMATS.get(source, ("parquet",)):
                    table = f"r_{name}.ns.ing_{source}_{fmt}"
                    if not (other / "ns" / f"ing_{source}_{fmt}").exists():
                        continue
                    self.cell(
                        "ingest_by_spark",
                        f"{other.name[3:]}:{source}_{fmt}",
                        "sql",
                        "read",
                        attempt(
                            lambda t=table: collect(
                                self.session.sql(f"SELECT id, d, f FROM {t} ORDER BY id")
                            )
                        ),
                    )


def main() -> int:
    mode = sys.argv[1]
    root = Path(sys.argv[2])
    if mode == "files":
        write_files(root)
        return 0
    tag, out = sys.argv[3], Path(sys.argv[4])
    legs = (
        sys.argv[5].split(",")
        if len(sys.argv) > 5
        else ["reads", "paths", "roundtrip", "ingest", "cross"]
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as sink:
        leg = Leg(mode, root, tag, sink)
        steps = {
            "reads": leg.reads,
            "paths": leg.read_paths,
            "roundtrip": leg.round_trip,
            "ingest": leg.ingest,
            "cross": leg.cross_reads,
        }
        for name in legs:
            steps[name]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
