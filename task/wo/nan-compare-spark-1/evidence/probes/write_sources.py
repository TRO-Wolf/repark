from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
import common as cm

FORMATS = ("parquet", "orc", "avro")
WIDE_SCHEMA = cm.WIDE_DDL
PART_SCHEMA = cm.PART_DDL


def session_for(engine: str, warehouse: Path) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .appName("nan-compare-write")
            .config("spark.jars", cm.JAR)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{cm.CAT}", "org.apache.iceberg.spark.SparkCatalog")
        )
    else:
        from repark import ReparkSession

        builder = ReparkSession.builder.appName("nan-compare-write")
    session = (
        builder.config(f"spark.sql.catalog.{cm.CAT}.type", "hadoop")
        .config(f"spark.sql.catalog.{cm.CAT}.warehouse", str(warehouse))
        .getOrCreate()
    )
    if engine == "spark":
        session.sparkContext.setLogLevel("ERROR")
    return session


def props(fmt: str) -> str:
    return f"TBLPROPERTIES ('format-version' = '2', 'write.format.default' = '{fmt}')"


def group_frame(session: Any, root: Path, layout: str, index: int) -> Any:
    return session.read.parquet(str(root / f"pa_{layout}" / f"part-{index}.parquet"))


def write_iceberg(session: Any, tag: str, root: Path, log: Any) -> None:
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cm.CAT}.{cm.NS}").collect()
    for fmt in FORMATS:
        for layout, groups in cm.LAYOUTS.items():
            name = f"{cm.CAT}.{cm.NS}.{tag}_{fmt}_{layout}"
            session.sql(f"CREATE TABLE {name} ({WIDE_SCHEMA}) USING iceberg {props(fmt)}").collect()
            for index in range(len(groups)):
                frame = group_frame(session, root, layout, index)
                frame.coalesce(1).writeTo(name).append()
            cm.dump(log, {"record": "table", "name": name})
        name = f"{cm.CAT}.{cm.NS}.{tag}_{fmt}_part"
        session.sql(
            f"CREATE TABLE {name} ({PART_SCHEMA}) USING iceberg PARTITIONED BY (pk) {props(fmt)}"
        ).collect()
        frame = session.read.parquet(str(root / "pa_part" / "part-0.parquet"))
        frame.coalesce(1).writeTo(name).append()
        cm.dump(log, {"record": "table", "name": name})


def write_parquet(session: Any, tag: str, root: Path, log: Any) -> None:
    for layout, groups in cm.LAYOUTS.items():
        target = root / f"{tag}_{layout}"
        for index in range(len(groups)):
            frame = group_frame(session, root, layout, index)
            frame.coalesce(1).write.mode("append").parquet(str(target))
        cm.dump(log, {"record": "parquet", "path": target.name})


def write_pyarrow(root: Path, log: Any) -> None:
    import pyarrow.parquet as pq

    for layout, groups in cm.LAYOUTS.items():
        target = root / f"pa_{layout}"
        target.mkdir(parents=True, exist_ok=True)
        for index, group in enumerate(groups):
            pq.write_table(cm.arrow_wide(group), target / f"part-{index}.parquet")
        cm.dump(log, {"record": "parquet", "path": target.name})
    target = root / "pa_part"
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(cm.arrow_part(), target / "part-0.parquet")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=("spark", "repark"))
    parser.add_argument("warehouse")
    parser.add_argument("parquet_root")
    parser.add_argument("out")
    args = parser.parse_args()
    warehouse = Path(args.warehouse)
    root = Path(args.parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    session = session_for(args.engine, warehouse)
    tag = "sp" if args.engine == "spark" else "rb"
    with Path(args.out).open("w") as log:
        cm.dump(log, cm.header(args.engine))
        if args.engine == "repark":
            write_pyarrow(root, log)
        for step in (
            lambda: write_iceberg(session, tag, root, log),
            lambda: write_parquet(session, tag, root, log),
        ):
            result = cm.attempt(lambda s=step: (s(), {})[1])
            if not result["ok"]:
                cm.dump(log, {"record": "error", **result})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
