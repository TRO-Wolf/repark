from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

JUNK = b"not a parquet, csv or json file"


def ids_table(ident: int) -> pa.Table:
    return pa.table({"id": pa.array([ident], pa.int64())})


def other_table() -> pa.Table:
    return pa.table({"y": pa.array(["z"], pa.string())})


CELLS: dict[str, tuple[str, str, dict[str, Any]]] = {
    "parquet/append/plain": ("parquet", "append", {"a.parquet": 10}),
    "parquet/append/underscore": ("parquet", "append", {"a.parquet": 10, "_b.parquet": 11}),
    "parquet/append/dot": ("parquet", "append", {"a.parquet": 10, ".c.parquet": 12}),
    "parquet/append/underscore-other-schema": (
        "parquet",
        "append",
        {"a.parquet": 10, "_b.parquet": "other"},
    ),
    "parquet/append/dot-not-parquet": ("parquet", "append", {"a.parquet": 10, ".c.parquet": JUNK}),
    "parquet/append/temporary-not-parquet": (
        "parquet",
        "append",
        {"a.parquet": 10, "_temporary/0/part-9.parquet": JUNK},
    ),
    "parquet/append/markers": (
        "parquet",
        "append",
        {"a.parquet": 10, "_SUCCESS": b"", ".z.parquet.crc": b"crc"},
    ),
    "parquet/append/only-hidden": ("parquet", "append", {"_SUCCESS": b"", "_b.parquet": 11}),
    "parquet/append/visible-other-schema": (
        "parquet",
        "append",
        {"a.parquet": 10, "d.parquet": "other"},
    ),
    "parquet/overwrite/hidden": (
        "parquet",
        "overwrite",
        {"a.parquet": 10, "_b.parquet": 11, "_SUCCESS": b""},
    ),
    "parquet/errorifexists/only-hidden": ("parquet", "errorifexists", {"_SUCCESS": b""}),
    "parquet/ignore/only-hidden": ("parquet", "ignore", {"_SUCCESS": b"", "_b.parquet": 11}),
    "csv/append/underscore-other-header": (
        "csv",
        "append",
        {"a.csv": b"id\n10\n", "_b.csv": b"zzz\n1\n"},
    ),
    "json/append/dot-not-json": ("json", "append", {"a.json": b'{"id": 10}\n', ".c.json": JUNK}),
}


def build(root: Path, files: dict[str, Any]) -> None:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    for rel, spec in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(spec, int):
            pq.write_table(ids_table(spec), str(path))
        elif spec == "other":
            pq.write_table(other_table(), str(path))
        else:
            path.write_bytes(spec)


def err_text(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    return f"{type(exc).__name__}: {(lines[0] if lines else '')[:160]}"


def read_back(spark: Any, fmt: str, root: Path) -> Any:
    try:
        if fmt == "parquet":
            frame = spark.read.parquet(str(root))
        elif fmt == "csv":
            frame = spark.read.option("header", "true").csv(str(root))
        else:
            frame = spark.read.json(str(root))
        return sorted(str(row[0]) for row in frame.collect())
    except Exception as exc:
        return "refuses: " + err_text(exc)


def run_cell(spark: Any, root: Path, key: str) -> dict[str, Any]:
    fmt, mode, files = CELLS[key]
    build(root, files)
    frame = spark.createDataFrame([(20,)], "id bigint")
    writer = frame.write.mode(mode)
    try:
        if fmt == "parquet":
            writer.parquet(str(root))
        elif fmt == "csv":
            writer.option("header", "true").csv(str(root))
        else:
            writer.json(str(root))
        write = "ok"
    except Exception as exc:
        write = "refuses: " + err_text(exc)
    disk = sorted(
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and not p.name.startswith(("part-", ".part-"))
    )
    return {"write": write, "read": read_back(spark, fmt, root), "disk": disk}


def make_session(engine: str) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("fileext-write-hidden")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("OFF")
        return spark
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("fileext-write-hidden")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark", "repark"], required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    spark = make_session(args.engine)
    payload: dict[str, Any] = {"engine": args.engine, "modes": {}}
    if args.engine == "repark":
        from repark import _native

        payload["native_file"] = _native.__file__
    for ansi in ("false", "true"):
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        results = {}
        for index, key in enumerate(CELLS):
            results[key] = run_cell(spark, Path(args.root) / f"ansi-{ansi}" / f"c{index}", key)
        payload["modes"][ansi] = results
    Path(args.out).write_text(json.dumps(payload, indent=1, sort_keys=True) + os.linesep)


if __name__ == "__main__":
    main()
