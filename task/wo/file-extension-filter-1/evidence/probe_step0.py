from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.orc as po
import pyarrow.parquet as pq

PARQUET_NAMES = [
    "b.parquet",
    "b.PARQUET",
    "b.snappy.parquet",
    "b.parq",
    "b",
    "b.bin",
    "b.x.y.z",
    "_b.parquet",
    ".b.parquet",
    "_k=1.parquet",
    "b.parquet._COPYING_",
]

MARKER_FILES: dict[str, tuple[str, ...]] = {
    "_SUCCESS": ("empty",),
    "_committed_123": ("bytes", '{"added":["a.parquet"],"removed":[]}'),
    "_started_123": ("empty",),
    ".z.parquet.crc": ("bytes", "crc-bytes-not-parquet"),
    ".part-00000.parquet.crc": ("bytes", "crc-bytes-not-parquet"),
    "_metadata": ("meta",),
    "_common_metadata": ("meta",),
    "README.txt": ("bytes", "a text file with an ordinary name\n"),
    "c.parquet": ("empty",),
    "c": ("empty",),
}

PARQUET_DOORS = ["reader", "format_load", "default_load", "sql", "view", "native"]


def write_parquet(path: Path, ident: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": pa.array([ident], pa.int64())}), str(path))


def write_spec(path: Path, spec: tuple[Any, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    kind = spec[0]
    if kind == "pq":
        write_parquet(path, spec[1])
    elif kind == "empty":
        path.write_bytes(b"")
    elif kind == "bytes":
        path.write_bytes(spec[1].encode())
    elif kind == "meta":
        pq.write_metadata(pa.schema([("id", pa.int64())]), str(path))
    elif kind == "orc":
        po.write_table(pa.table({"id": pa.array([spec[1]], pa.int64())}), str(path))
    else:
        raise ValueError(kind)


def parquet_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for name in PARQUET_NAMES:
        files = {"a.parquet": ("pq", 10), name: ("pq", 11)}
        cells.append({"id": f"name/{name}", "files": files, "target": "", "doors": PARQUET_DOORS})
    for name, spec in MARKER_FILES.items():
        files = {"a.parquet": ("pq", 10), name: spec}
        cells.append(
            {
                "id": f"marker/{name}",
                "files": files,
                "target": "",
                "doors": PARQUET_DOORS,
            }
        )
    spark_like = {
        "part-00000-x.snappy.parquet": ("pq", 10),
        "part-00001-x.snappy.parquet": ("pq", 11),
        "_SUCCESS": ("empty",),
        ".part-00000-y.snappy.parquet.crc": ("bytes", "crc"),
        ".part-00001-y.snappy.parquet.crc": ("bytes", "crc"),
        "._SUCCESS.crc": ("bytes", "crc"),
        "_committed_9": ("bytes", "{}"),
        "_started_9": ("empty",),
    }
    cells.append(
        {
            "id": "marker/all-writer-markers",
            "files": spark_like,
            "target": "",
            "doors": PARQUET_DOORS,
        }
    )
    partitioned = {
        "p=1/part-00000": ("pq", 1),
        "p=2/part-00000.parquet": ("pq", 2),
        "p=3/part-00000.snappy.parquet": ("pq", 3),
        "p=4/data.bin": ("pq", 4),
    }
    layouts: dict[str, tuple[dict[str, tuple[Any, ...]], Any]] = {
        "layout/flat-no-ext": ({"a.parquet": ("pq", 10), "b": ("pq", 11)}, ""),
        "layout/nested-plain": (
            {"sub1/a.parquet": ("pq", 10), "sub2/b": ("pq", 11)},
            "",
        ),
        "layout/nested-mixed": (
            {"a.parquet": ("pq", 10), "sub/b.parquet": ("pq", 11)},
            "",
        ),
        "layout/partitioned-card": (partitioned, ""),
        "layout/partitioned-markers": (
            {
                "p=1/a.parquet": ("pq", 1),
                "p=1/b": ("pq", 2),
                "p=1/_SUCCESS": ("empty",),
                "p=1/.z.parquet.crc": ("bytes", "crc"),
                "_SUCCESS": ("empty",),
            },
            "",
        ),
        "layout/hidden-dirs": (
            {
                "a.parquet": ("pq", 10),
                "_temporary/0/x.parquet": ("pq", 11),
                ".hidden/y.parquet": ("pq", 12),
                "_tmp/z": ("pq", 13),
            },
            "",
        ),
        "layout/hidden-partition-dirs": (
            {
                "p=1/a.parquet": ("pq", 1),
                "_p=2/b.parquet": ("pq", 2),
                ".p=3/c.parquet": ("pq", 3),
            },
            "",
        ),
        "glob/star-flat": (
            {
                "a.parquet": ("pq", 10),
                "b": ("pq", 11),
                "_SUCCESS": ("empty",),
                ".z.crc": ("bytes", "x"),
            },
            "*",
        ),
        "glob/star-parquet": (
            {"a.parquet": ("pq", 10), "b": ("pq", 11), "c.PARQUET": ("pq", 12)},
            "*.parquet",
        ),
        "glob/star-hidden-explicit": (
            {"a.parquet": ("pq", 10), "_b.parquet": ("pq", 11)},
            "_*",
        ),
        "glob/p-star": (partitioned, "p=*"),
        "list/file-and-no-ext": (
            {"a.parquet": ("pq", 10), "b": ("pq", 11)},
            ["a.parquet", "b"],
        ),
        "list/two-dirs": (
            {"d1/a.parquet": ("pq", 10), "d2/b": ("pq", 11), "d2/_SUCCESS": ("empty",)},
            ["d1", "d2"],
        ),
        "single/no-ext": ({"a.parquet": ("pq", 10), "b": ("pq", 11)}, "b"),
        "single/bin": ({"b.bin": ("pq", 11)}, "b.bin"),
        "single/upper": ({"b.PARQUET": ("pq", 11)}, "b.PARQUET"),
        "single/hidden-underscore": ({"_b.parquet": ("pq", 11)}, "_b.parquet"),
        "single/hidden-dot": ({".b.parquet": ("pq", 11)}, ".b.parquet"),
        "single/copying": ({"b.parquet._COPYING_": ("pq", 11)}, "b.parquet._COPYING_"),
        "root/underscore-dir": (
            {"_root/a.parquet": ("pq", 10), "_root/b": ("pq", 11)},
            "_root",
        ),
        "root/dot-dir": (
            {".root/a.parquet": ("pq", 10), ".root/b": ("pq", 11)},
            ".root",
        ),
    }
    for ident, (files, target) in layouts.items():
        doors = PARQUET_DOORS if not isinstance(target, list) else ["reader", "format_load"]
        cells.append({"id": ident, "files": files, "target": target, "doors": doors})
    options = {
        "option/pathGlobFilter": {"pathGlobFilter": "*.parquet"},
        "option/recursiveFileLookup": {"recursiveFileLookup": "true"},
        "option/ignoreCorruptFiles": {"ignoreCorruptFiles": "true"},
        "option/ignoreMissingFiles": {"ignoreMissingFiles": "true"},
        "option/mergeSchema": {"mergeSchema": "true"},
    }
    for ident, opts in options.items():
        files: dict[str, tuple[Any, ...]] = {"a.parquet": ("pq", 10), "b": ("pq", 11)}
        if ident == "option/ignoreCorruptFiles":
            files["README.txt"] = ("bytes", "text\n")
        cells.append(
            {
                "id": ident,
                "files": files,
                "target": "",
                "doors": ["reader"],
                "options": opts,
            }
        )
    return cells


def neighbour_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for fmt, ext in (("csv", "csv"), ("json", "json"), ("text", "txt"), ("orc", "orc")):
        names = [
            "b",
            f"b.{ext.upper()}",
            "b.bin",
            f"_b.{ext}",
            f".b.{ext}",
            f"b.{ext}._COPYING_",
        ]
        for name in names:
            files = {f"a.{ext}": content_for(fmt, 10), name: content_for(fmt, 11)}
            cells.append(
                {
                    "id": f"neighbour/{fmt}/{name}",
                    "files": files,
                    "target": "",
                    "fmt": fmt,
                }
            )
        markers = {
            f"a.{ext}": content_for(fmt, 10),
            "_SUCCESS": ("empty",),
            f".z.{ext}.crc": ("bytes", "crc"),
            "_committed_1": ("bytes", "{}"),
        }
        cells.append(
            {
                "id": f"neighbour/{fmt}/markers",
                "files": markers,
                "target": "",
                "fmt": fmt,
            }
        )
    return cells


def content_for(fmt: str, ident: int) -> tuple[Any, ...]:
    if fmt == "csv":
        return ("bytes", f"{ident}\n")
    if fmt == "json":
        return ("bytes", json.dumps({"id": ident}) + "\n")
    if fmt == "text":
        return ("bytes", f"{ident}\n")
    return ("orc", ident)


def norm(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def err_info(exc: BaseException) -> dict[str, Any]:
    cond = None
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(exc, attr, None)
        if callable(fn):
            try:
                cond = fn()
            except Exception:
                cond = None
            if cond:
                break
    text = str(exc).strip().splitlines()
    return {
        "type": type(exc).__name__,
        "cond": cond,
        "first": (text[0] if text else "")[:220],
    }


def rel_files(files: list[str], cell_dir: Path) -> list[str]:
    out = []
    base = str(cell_dir.resolve())
    for item in files:
        text = item
        for prefix in ("file://", "file:"):
            if text.startswith(prefix):
                text = text[len(prefix) :]
                break
        text = text.replace("%3D", "=")
        if text.startswith(base):
            text = text[len(base) :].lstrip("/")
        out.append(text)
    return sorted(out)


def capture(make: Any, cell_dir: Path) -> dict[str, Any]:
    try:
        df = make()
    except Exception as exc:
        return {"err": err_info(exc), "at": "read"}
    out: dict[str, Any] = {}
    try:
        out["schema"] = df.schema.simpleString()
    except Exception as exc:
        out["schema_err"] = err_info(exc)
    try:
        rows = [[norm(v) for v in row] for row in df.collect()]
        out["rows"] = sorted(rows, key=json.dumps)
    except Exception as exc:
        out["rows_err"] = err_info(exc)
    try:
        out["count"] = df.count()
    except Exception as exc:
        out["count_err"] = err_info(exc)
    try:
        out["files"] = rel_files(list(df.inputFiles()), cell_dir)
    except Exception as exc:
        out["files_err"] = err_info(exc)
    return out


def capture_native(session: Any, native_mod: Any, path: str, cell_dir: Path) -> dict[str, Any]:
    try:
        frame = session.read_parquet(path)
    except Exception as exc:
        return {"err": err_info(exc), "at": "read"}
    out: dict[str, Any] = {}
    try:
        session.create_or_replace_temp_view("fx_native", frame)
        table = pa.table(session.sql("SELECT * FROM fx_native"))
        out["schema"] = ",".join(f"{f.name}:{f.type}" for f in table.schema)
        rows = [list(r.values()) for r in table.to_pylist()]
        out["rows"] = sorted([[norm(v) for v in r] for r in rows], key=json.dumps)
        out["count"] = table.num_rows
    except Exception as exc:
        out["rows_err"] = err_info(exc)
    try:
        out["files"] = rel_files(list(native_mod.input_files(frame)), cell_dir)
    except Exception as exc:
        out["files_err"] = err_info(exc)
    return out


def build(cell_dir: Path, files: dict[str, tuple[Any, ...]]) -> None:
    if cell_dir.exists():
        shutil.rmtree(cell_dir)
    cell_dir.mkdir(parents=True)
    for rel, spec in files.items():
        write_spec(cell_dir / rel, spec)


def target_of(cell_dir: Path, target: Any) -> Any:
    if isinstance(target, list):
        return [str(cell_dir / item) for item in target]
    if target == "":
        return str(cell_dir)
    return str(cell_dir / target)


def run_parquet_door(spark: Any, door: str, target: Any, options: dict[str, str]) -> Any:
    if door == "reader":
        reader = spark.read
        for key, value in options.items():
            reader = reader.option(key, value)
        if isinstance(target, list):
            return lambda: reader.parquet(*target)
        return lambda: reader.parquet(target)
    if door == "format_load":
        return lambda: spark.read.format("parquet").load(target)
    if door == "default_load":
        return lambda: spark.read.load(target)
    if door == "sql":
        return lambda: spark.sql(f"SELECT * FROM parquet.`{target}`")
    if door == "view":

        def make() -> Any:
            spark.sql(
                f"CREATE OR REPLACE TEMPORARY VIEW fx_view USING parquet OPTIONS (path '{target}')"
            )
            return spark.table("fx_view")

        return make
    raise ValueError(door)


def run_neighbour(spark: Any, fmt: str, target: str) -> Any:
    reader = spark.read
    if fmt == "csv":
        return lambda: reader.csv(target)
    if fmt == "json":
        return lambda: reader.json(target)
    if fmt == "text":
        return lambda: reader.text(target)
    return lambda: reader.orc(target)


def make_session(engine: str, warehouse: Path) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("fileext-step0")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "America/New_York")
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
            .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("OFF")
        return spark
    from repark import ReparkSession
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    return (
        ReparkSession.builder.appName("fileext-step0")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )


def write_cells(spark: Any, root: Path, results: dict[str, Any]) -> None:
    for mode in ("append", "overwrite"):
        cell_dir = root / f"write-{mode}"
        build(cell_dir, {"a.parquet": ("pq", 10), "b": ("pq", 11)})
        df = spark.createDataFrame([(20,)], "id bigint")
        try:
            df.write.mode(mode).parquet(str(cell_dir))
            results[f"write/{mode}/self"] = capture(
                lambda target=str(cell_dir): spark.read.parquet(target), cell_dir
            )
        except Exception as exc:
            results[f"write/{mode}/self"] = {"err": err_info(exc), "at": "write"}
        listing = sorted(str(p.relative_to(cell_dir)) for p in cell_dir.rglob("*") if p.is_file())
        results[f"write/{mode}/listing"] = {"files_on_disk": listing}
    written = root / "written-flat"
    if written.exists():
        shutil.rmtree(written)
    spark.createDataFrame([(1, 1), (2, 2), (3, 1)], "id bigint, p int").write.parquet(str(written))
    part = root / "written-partitioned"
    if part.exists():
        shutil.rmtree(part)
    spark.createDataFrame([(1, 1), (2, 2), (3, 1)], "id bigint, p int").write.partitionBy(
        "p"
    ).parquet(str(part))


def cross_cells(spark: Any, peer: Path, results: dict[str, Any]) -> None:
    for mode in ("append", "overwrite"):
        cell_dir = peer / f"write-{mode}"
        if cell_dir.exists():
            results[f"cross/write-{mode}"] = capture(
                lambda target=str(cell_dir): spark.read.parquet(target), cell_dir
            )
    for name in ("written-flat", "written-partitioned"):
        cell_dir = peer / name
        if cell_dir.exists():
            results[f"cross/{name}"] = capture(
                lambda target=str(cell_dir): spark.read.parquet(target), cell_dir
            )
            listing = sorted(
                str(p.relative_to(cell_dir)) for p in cell_dir.rglob("*") if p.is_file()
            )
            results[f"cross/{name}/listing"] = {"files_on_disk": listing}


def iceberg_cells(spark: Any, results: dict[str, Any]) -> None:
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.fx")
        spark.sql("DROP TABLE IF EXISTS ice.fx.t")
        spark.sql("CREATE TABLE ice.fx.t (id bigint) USING iceberg")
        spark.sql("INSERT INTO ice.fx.t VALUES (1), (2)")
        results["iceberg/control"] = capture(
            lambda: spark.sql("SELECT * FROM ice.fx.t"), Path("/nonexistent")
        )
    except Exception as exc:
        results["iceberg/control"] = {"err": err_info(exc), "at": "setup"}


def run_mode(engine: str, spark: Any, root: Path, ansi: str, native: Any) -> dict[str, Any]:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    results: dict[str, Any] = {}
    base = root / f"ansi-{ansi}"
    for cell in parquet_cells():
        for door in cell["doors"]:
            key = f"{cell['id']}|{door}"
            cell_dir = base / key.replace("/", "__").replace("|", "--")
            build(cell_dir, cell["files"])
            target = target_of(cell_dir, cell["target"])
            if door == "native":
                if engine == "spark" or isinstance(target, list):
                    continue
                results[key] = capture_native(native[0], native[1], target, cell_dir)
                continue
            make = run_parquet_door(spark, door, target, cell.get("options", {}))
            results[key] = capture(make, cell_dir)
    for cell in neighbour_cells():
        key = f"{cell['id']}|reader"
        cell_dir = base / key.replace("/", "__").replace("|", "--")
        build(cell_dir, cell["files"])
        results[key] = capture(run_neighbour(spark, cell["fmt"], str(cell_dir)), cell_dir)
    write_cells(spark, base, results)
    iceberg_cells(spark, results)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark", "repark"], required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--peer", default="")
    parser.add_argument("--cross-only", action="store_true")
    args = parser.parse_args()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    spark = make_session(args.engine, root / "wh")
    payload: dict[str, Any] = {"engine": args.engine, "modes": {}}
    native: Any = None
    if args.engine == "repark":
        import repark
        from repark import _native
        from repark._native import PyReparkSession

        payload["native_file"] = repark._native.__file__
        native = (PyReparkSession.native(), _native)
    for ansi in ("false", "true"):
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        if args.cross_only:
            results: dict[str, Any] = {}
        else:
            results = run_mode(args.engine, spark, root, ansi, native)
        if args.peer:
            cross_cells(spark, Path(args.peer) / f"ansi-{ansi}", results)
        payload["modes"][ansi] = results
    Path(args.out).write_text(json.dumps(payload, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
