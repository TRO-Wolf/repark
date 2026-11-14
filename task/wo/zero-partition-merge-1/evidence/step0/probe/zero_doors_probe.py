from __future__ import annotations

import argparse
import json
import os
import struct
import sys
import time
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

CAT = "c"
NS = "ns"
JAR = os.environ.get("ICEBERG_SPARK_JAR", "iceberg-spark-runtime-4.1_2.13-1.11.0.jar")

ROWS = [
    (1, "-0.0", 1),
    (2, "0.0", 2),
    (3, "1.5", 3),
    (4, None, 4),
    (5, "0.0", 5),
    (6, "-0.0", 6),
]
PAIRS = [*ROWS[:4], (7, "-0.0", 7), (8, "0.0", 8), (9, "1.5", 9), (10, None, 10)]
POS_FIRST = [ROWS[1], ROWS[0], ROWS[2], ROWS[3], ROWS[5], ROWS[4]]
CROSS_SEED = [(10, "-0.0", 10), (11, "1.5", 11)]
CROSS_NEW = [(1, "0.0", 1)]
FILLER = [(99, "2.0", 99)]
FORMATS = ("parquet", "orc", "avro")
VERSIONS = (2, 3)
COLUMNS = ("d", "f")
COLUMN_TYPES = "id BIGINT, d DOUBLE, f FLOAT, v INT"


def value_sql(text: str | None, kind: str) -> str:
    if text is None:
        return f"CAST(NULL AS {kind})"
    return f"CAST('{text}' AS {kind})"


def row_sql(row: tuple[int, str | None, int]) -> str:
    ident, text, payload = row
    return (
        f"(CAST({ident} AS BIGINT), {value_sql(text, 'DOUBLE')}, {value_sql(text, 'FLOAT')}, "
        f"CAST({payload} AS INT))"
    )


def values_sql(rows: list[tuple[int, str | None, int]]) -> str:
    return ", ".join(row_sql(row) for row in rows)


def select_sql(rows: list[tuple[int, str | None, int]]) -> str:
    return f"SELECT id, d, f, v FROM (VALUES {values_sql(rows)}) AS q(id, d, f, v)"


def bits(value: Any, width: int) -> str | None:
    if value is None:
        return None
    return struct.pack(">d" if width == 8 else ">f", value).hex()


def emit(sink: Any, record: dict[str, Any]) -> None:
    sink.write(json.dumps(record, default=str) + "\n")
    sink.flush()


def attempt(action: Callable[[], Any]) -> dict[str, Any]:
    try:
        action()
        return {"outcome": "ok"}
    except Exception as error:
        text = f"{type(error).__name__}: {error}"
        return {"outcome": "error", "error": text[:600]}


def session_for(engine: str, warehouse: Path, cores: str, shuffle: int | None) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession, functions

        builder = (
            SparkSession.builder.master(f"local[{cores}]")
            .appName("zero-partition-step0")
            .config("spark.jars", JAR)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", str(shuffle or 4))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{CAT}", "org.apache.iceberg.spark.SparkCatalog")
        )
    else:
        import repark
        from repark import ReparkSession, functions

        print("native module:", repark._native.__file__, file=sys.stderr)
        builder = ReparkSession.builder.appName("zero-partition-step0").config(
            "repark.sql.allowCreateFormatVersion3", "true"
        )
        if shuffle is not None:
            builder = builder.config("spark.sql.shuffle.partitions", str(shuffle))
    session = (
        builder.config(f"spark.sql.catalog.{CAT}.type", "hadoop")
        .config(f"spark.sql.catalog.{CAT}.warehouse", str(warehouse))
        .getOrCreate()
    )
    if engine == "spark":
        session.sparkContext.setLogLevel("ERROR")
    return session, functions


def props(version: int, fmt: str, mode: str = "copy-on-write", extra: str = "") -> str:
    return (
        f"TBLPROPERTIES ('format-version' = '{version}', 'write.format.default' = '{fmt}', "
        f"'write.update.mode' = '{mode}', 'write.delete.mode' = '{mode}', "
        f"'write.merge.mode' = '{mode}'{extra})"
    )


class Cells:
    def __init__(self, session: Any, functions: Any) -> None:
        self.session = session
        self.F = functions

    def sql(self, text: str) -> None:
        self.session.sql(text).collect()

    def create(
        self, t: str, col: str, version: int, fmt: str, mode: str = "copy-on-write", extra: str = ""
    ) -> None:
        self.sql(
            f"CREATE TABLE {t} ({COLUMN_TYPES}) USING iceberg PARTITIONED BY ({col}) "
            f"{props(version, fmt, mode, extra)}"
        )

    def seed_each(self, t: str, rows: list[tuple[int, str | None, int]]) -> None:
        for row in rows:
            self.sql(f"INSERT INTO {t} {select_sql([row])}")

    def seed_pairs(self, t: str) -> None:
        for first, second in ((1, 7), (2, 8), (3, 9), (4, 10)):
            chosen = [row for row in PAIRS if row[0] in (first, second)]
            self.sql(f"INSERT INTO {t} {select_sql(chosen)}")

    def frame(self, rows: list[tuple[int, str | None, int]]) -> Any:
        return self.session.sql(select_sql(rows))

    def doors(self) -> dict[str, Callable[[str, str, int, str], None]]:
        return {
            "insert_values": self.insert_values,
            "insert_values_pos_first": self.insert_values_pos_first,
            "insert_select": self.insert_select,
            "insert_overwrite_static": self.insert_overwrite_static,
            "insert_overwrite_dynamic_cross": self.insert_overwrite_dynamic_cross,
            "ctas": self.ctas,
            "rtas": self.rtas,
            "wt_append": self.wt_append,
            "wt_create": self.wt_create,
            "wt_create_or_replace": self.wt_create_or_replace,
            "wt_overwrite_partitions": self.wt_overwrite_partitions,
            "wt_overwrite_partitions_cross": self.wt_overwrite_partitions_cross,
            "sat_append": self.sat_append,
            "sat_overwrite": self.sat_overwrite,
            "merge_insert": self.merge_insert,
            "merge_update_cow": self.merge_update("copy-on-write"),
            "merge_update_mor": self.merge_update("merge-on-read"),
            "update_cow": self.update("copy-on-write"),
            "update_mor": self.update("merge-on-read"),
            "delete_cow": self.delete("copy-on-write", ""),
            "delete_mor": self.delete("merge-on-read", ""),
            "delete_mor_partition_granularity": self.delete(
                "merge-on-read", ", 'write.delete.granularity' = 'partition'"
            ),
            "insert_overwrite_static_partition_cross": self.insert_overwrite_static_partition_cross,
            "wt_overwrite_filter_cross": self.wt_overwrite_filter_cross,
            "rewrite_data_files": self.rewrite,
            "branch_insert": self.branch_insert,
        }

    def insert_values(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.sql(f"INSERT INTO {t} VALUES {values_sql(ROWS)}")

    def insert_values_pos_first(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.sql(f"INSERT INTO {t} VALUES {values_sql(POS_FIRST)}")

    def insert_select(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.sql(f"INSERT INTO {t} {select_sql(ROWS)}")

    def insert_overwrite_static(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, FILLER)
        self.sql(f"INSERT OVERWRITE {t} {select_sql(ROWS)}")

    def insert_overwrite_dynamic_cross(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, CROSS_SEED)
        self.session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try:
            self.sql(f"INSERT OVERWRITE {t} {select_sql(CROSS_NEW)}")
        finally:
            self.session.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    def insert_overwrite_static_partition_cross(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, CROSS_SEED)
        rest = ", ".join(name for name in ("id", "d", "f", "v") if name != col)
        self.sql(
            f"INSERT OVERWRITE {t} PARTITION ({col} = 0.0) SELECT {rest} FROM "
            f"({select_sql(CROSS_NEW)}) q"
        )

    def wt_overwrite_filter_cross(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, CROSS_SEED)
        kind = "DOUBLE" if col == "d" else "FLOAT"
        condition = self.F.col(col) == self.F.lit(0.0).cast(kind)
        self.frame(CROSS_NEW).writeTo(t).overwrite(condition)

    def ctas(self, t: str, col: str, v: int, fmt: str) -> None:
        self.sql(
            f"CREATE TABLE {t} USING iceberg PARTITIONED BY ({col}) {props(v, fmt)} AS "
            f"{select_sql(ROWS)}"
        )

    def rtas(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, FILLER)
        self.sql(
            f"REPLACE TABLE {t} USING iceberg PARTITIONED BY ({col}) {props(v, fmt)} AS "
            f"{select_sql(ROWS)}"
        )

    def wt_append(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.frame(ROWS).writeTo(t).append()

    def wt_builder(self, t: str, col: str, v: int, fmt: str) -> Any:
        return (
            self.frame(ROWS)
            .writeTo(t)
            .using("iceberg")
            .tableProperty("format-version", str(v))
            .tableProperty("write.format.default", fmt)
            .partitionedBy(self.F.col(col))
        )

    def wt_create(self, t: str, col: str, v: int, fmt: str) -> None:
        self.wt_builder(t, col, v, fmt).create()

    def wt_create_or_replace(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, FILLER)
        self.wt_builder(t, col, v, fmt).createOrReplace()

    def wt_overwrite_partitions(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, FILLER)
        self.frame(ROWS).writeTo(t).overwritePartitions()

    def wt_overwrite_partitions_cross(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, CROSS_SEED)
        self.frame(CROSS_NEW).writeTo(t).overwritePartitions()

    def sat_append(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.frame(ROWS).write.format("iceberg").mode("append").saveAsTable(t)

    def sat_overwrite(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, FILLER)
        (
            self.frame(ROWS)
            .write.format("iceberg")
            .mode("overwrite")
            .option("format-version", str(v))
            .option("write.format.default", fmt)
            .partitionBy(col)
            .saveAsTable(t)
        )

    def merge_insert(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.sql(
            f"MERGE INTO {t} t USING ({select_sql(ROWS)}) s ON t.id = s.id "
            "WHEN NOT MATCHED THEN INSERT *"
        )

    def merge_update(self, mode: str) -> Callable[[str, str, int, str], None]:
        def run(t: str, col: str, v: int, fmt: str) -> None:
            self.create(t, col, v, fmt, mode)
            self.seed_pairs(t)
            keys = [row for row in PAIRS if row[0] >= 7]
            self.sql(
                f"MERGE INTO {t} t USING ({select_sql(keys)}) s ON t.id = s.id "
                "WHEN MATCHED THEN UPDATE SET v = t.v + 100"
            )

        return run

    def update(self, mode: str) -> Callable[[str, str, int, str], None]:
        def run(t: str, col: str, v: int, fmt: str) -> None:
            self.create(t, col, v, fmt, mode)
            self.seed_pairs(t)
            self.sql(f"UPDATE {t} SET v = v + 100 WHERE id >= 7")

        return run

    def delete(self, mode: str, extra: str) -> Callable[[str, str, int, str], None]:
        def run(t: str, col: str, v: int, fmt: str) -> None:
            self.create(t, col, v, fmt, mode, extra)
            self.seed_pairs(t)
            self.sql(f"DELETE FROM {t} WHERE id >= 7")

        return run

    def rewrite(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_pairs(t)
        name = t.split(".", 1)[1]
        self.sql(
            f"CALL {CAT}.system.rewrite_data_files(table => '{name}', "
            "options => map('rewrite-all', 'true'))"
        )

    def branch_insert(self, t: str, col: str, v: int, fmt: str) -> None:
        self.create(t, col, v, fmt)
        self.seed_each(t, FILLER)
        self.sql(f"ALTER TABLE {t} CREATE BRANCH b")
        self.sql(f"INSERT INTO {t}.branch_b {select_sql(ROWS)}")


def partition_bits(partition: Any, col: str) -> Any:
    if partition is None:
        return "no-partition"
    try:
        value = partition[col]
    except Exception:
        try:
            value = partition.asDict()[col]
        except Exception:
            return f"unreadable:{partition!r}"
    if value is None:
        return None
    return bits(value, 8 if col == "d" else 4)


def checks(session: Any, table: str, col: str, branch: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {}
    source = f"{table}.branch_b" if branch else table
    try:
        out["read"] = {
            str(r[0]): [bits(r[1], 8), bits(r[2], 4), r[3]]
            for r in session.sql(f"SELECT id, d, f, v FROM {source}").collect()
        }
    except Exception as error:
        out["read"] = {"error": str(error)[:300]}
    suffix = " VERSION AS OF 'b'" if branch else ""
    files: Any
    try:
        files = sorted(
            [
                [r[0], partition_bits(r[1], col), r[2], r[3]]
                for r in session.sql(
                    f"SELECT content, partition, record_count, file_path FROM {table}.files{suffix}"
                ).collect()
            ],
            key=lambda item: json.dumps(item),
        )
    except Exception as error:
        try:
            files = sorted(
                [
                    [None, partition_bits(r[0], col), r[1]]
                    for r in session.sql(
                        f"SELECT partition, record_count FROM {table}.files{suffix}"
                    ).collect()
                ],
                key=lambda item: json.dumps(item),
            )
        except Exception as inner:
            files = {"error": f"{str(error)[:200]} | {str(inner)[:200]}"}
    out["files"] = files
    return out


def leg_doors(
    session: Any, functions: Any, sink: Any, only: set[str] | None, formats: tuple[str, ...]
) -> None:
    cells = Cells(session, functions)
    for door, run in cells.doors().items():
        if only and door not in only:
            continue
        for version in VERSIONS:
            for fmt in formats:
                for col in COLUMNS:
                    name = f"{door}_v{version}_{fmt}_{col}"
                    table = f"{CAT}.{NS}.{name}"
                    started = time.monotonic()
                    result = attempt(lambda r=run, t=table, c=col, v=version, f=fmt: r(t, c, v, f))
                    record = {
                        "leg": "doors",
                        "table": name,
                        "door": door,
                        "version": version,
                        "fmt": fmt,
                        "col": col,
                        **result,
                        "seconds": round(time.monotonic() - started, 2),
                    }
                    if result["outcome"] == "ok":
                        record["checks"] = checks(session, table, col, door == "branch_insert")
                    emit(sink, record)


def leg_transforms(session: Any, sink: Any) -> None:
    for col in COLUMNS:
        for transform in (f"bucket(4, {col})", f"truncate(2, {col})"):
            tag = transform.split("(")[0]
            name = f"tf_{tag}_{col}"
            table = f"{CAT}.{NS}.{name}"
            result = attempt(
                lambda t=table, x=transform: session.sql(
                    f"CREATE TABLE {t} ({COLUMN_TYPES}) USING iceberg PARTITIONED BY ({x})"
                ).collect()
            )
            emit(sink, {"leg": "transforms", "table": name, "transform": transform, **result})


def leg_native_insert(session: Any, root: Path, sink: Any, formats: tuple[str, ...]) -> None:
    import repark
    from repark._native import PyReparkSession

    cells = Cells(session, None)
    names = []
    for version in VERSIONS:
        for fmt in formats:
            for col in COLUMNS:
                name = f"native_insert_v{version}_{fmt}_{col}"
                result = attempt(
                    lambda n=name, c=col, v=version, f=fmt: cells.create(f"{CAT}.{NS}.{n}", c, v, f)
                )
                names.append((name, col, version, fmt, result))
    native = PyReparkSession.native()
    registered = attempt(
        lambda: native.register_late_catalogs(
            {
                "spark.sql.catalog.hc.type": "hadoop",
                "spark.sql.catalog.hc.warehouse": str(root),
            }
        )
    )
    emit(sink, {"leg": "native_setup", **registered})
    repark._ANSI_NATIVE = native
    for name, col, version, fmt, created in names:
        if created["outcome"] != "ok":
            emit(
                sink,
                {
                    "leg": "doors",
                    "table": name,
                    "door": "native_insert",
                    "version": version,
                    "fmt": fmt,
                    "col": col,
                    **created,
                },
            )
            continue
        result = attempt(
            lambda n=name: repark.sql(
                f"INSERT INTO hc.{NS}.{n} VALUES {values_sql(ROWS)}"
            ).to_arrow()
        )
        record = {
            "leg": "doors",
            "table": name,
            "door": "native_insert",
            "version": version,
            "fmt": fmt,
            "col": col,
            **result,
        }
        if result["outcome"] == "ok":
            session.sql(f"REFRESH TABLE {CAT}.{NS}.{name}").collect()
            record["checks"] = checks(session, f"{CAT}.{NS}.{name}", col)
        emit(sink, record)


def leg_native(warehouse: Path, sink: Any) -> None:
    import repark
    from repark._native import PyReparkSession

    native = PyReparkSession.native()
    warehouse.mkdir(parents=True, exist_ok=True)
    native.register_memory_catalog("nat", str(warehouse))
    native.create_namespace("nat", NS, str(warehouse / NS))
    repark._ANSI_NATIVE = native
    statements = {
        "nat_partitioned_by": [
            f"CREATE TABLE nat.{NS}.nat_partitioned_by (id BIGINT, d DOUBLE, f FLOAT, v INT) "
            "PARTITIONED BY (d)",
        ],
        "nat_unpartitioned_control": [
            f"CREATE TABLE nat.{NS}.nat_unpartitioned_control ({COLUMN_TYPES})",
            f"INSERT INTO nat.{NS}.nat_unpartitioned_control VALUES {values_sql(ROWS)}",
        ],
    }
    for name, batch in statements.items():
        result = attempt(lambda s=batch: [repark.sql(x).to_arrow() for x in s])
        record = {"leg": "native", "table": name, **result}
        if result["outcome"] == "ok" and name != "nat_partitioned_by":
            try:
                got = repark.sql(f"SELECT id, d, f, v FROM nat.{NS}.{name}").to_arrow().to_pylist()
                record["read"] = {
                    str(r["id"]): [bits(r["d"], 8), bits(r["f"], 4), r["v"]] for r in got
                }
            except Exception as error:
                record["read"] = {"error": str(error)[:300]}
        emit(sink, record)


def register(session: Any, root: Path, name: str) -> None:
    meta = root / NS / name / "metadata"
    version = (meta / "version-hint.text").read_text().strip()
    session.sql(
        f"CALL {CAT}.system.register_table(table => '{NS}.{name}', "
        f"metadata_file => '{meta / f'v{version}.metadata.json'}')"
    ).collect()


def leg_cross_read(
    session: Any, sink: Any, engine: str, other_root: Path, names: list[tuple[str, str, bool]]
) -> None:
    for name, col, branch in names:
        table = f"{CAT}.{NS}.{name}"
        if engine == "repark":
            reg = attempt(lambda n=name: register(session, other_root, n))
            if reg["outcome"] != "ok":
                emit(sink, {"leg": "cross_read", "table": name, **reg})
                continue
        emit(
            sink,
            {
                "leg": "cross_read",
                "table": name,
                "outcome": "ok",
                "checks": checks(session, table, col, branch),
            },
        )


def tables_from(path: Path) -> list[tuple[str, str, bool]]:
    names = []
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record.get("leg") == "doors" and record.get("outcome") == "ok":
            names.append((record["table"], record["col"], record["door"] == "branch_insert"))
    return names


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=("spark", "repark"))
    parser.add_argument("warehouse", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--legs", default="doors,transforms")
    parser.add_argument("--cores", default="2")
    parser.add_argument("--shuffle", type=int, default=None)
    parser.add_argument("--only", default="")
    parser.add_argument("--formats", default=",".join(FORMATS))
    parser.add_argument("--other-root", type=Path, default=None)
    parser.add_argument("--other-records", type=Path, default=None)
    args = parser.parse_args()
    legs = args.legs.split(",")
    only = set(filter(None, args.only.split(","))) or None
    formats = tuple(filter(None, args.formats.split(",")))
    args.warehouse.mkdir(parents=True, exist_ok=True)
    with args.out.open("a") as sink:
        session, functions = session_for(
            args.engine, args.warehouse / "hadoop", args.cores, args.shuffle
        )
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.{NS}").collect()
        header = {
            "leg": "setup",
            "engine": args.engine,
            "legs": legs,
            "cores": args.cores,
            "shuffle": args.shuffle,
        }
        if args.engine == "repark":
            import repark

            header["native_module"] = repark._native.__file__
        else:
            import pyspark

            header["pyspark"] = pyspark.__version__
            header["iceberg_jar"] = Path(JAR).name
        emit(sink, header)
        try:
            if "doors" in legs:
                leg_doors(session, functions, sink, only, formats)
            if "transforms" in legs:
                leg_transforms(session, sink)
            if "cross_read" in legs and args.other_records is not None:
                leg_cross_read(
                    session, sink, args.engine, args.other_root, tables_from(args.other_records)
                )
            if "native" in legs and args.engine == "repark":
                leg_native(args.warehouse / "native", sink)
            if "native_insert" in legs and args.engine == "repark":
                leg_native_insert(session, args.warehouse / "hadoop", sink, formats)
        except Exception:
            emit(sink, {"leg": "fatal", "trace": traceback.format_exc()})
        finally:
            session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
