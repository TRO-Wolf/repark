from __future__ import annotations

import importlib.metadata
import json
import re
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

import repark
from repark import ReparkSession

REFUSED_EXIT = 3
METADATA_VERSION = re.compile(r"^v?(\d+)[-.]")
COLUMNS = (
    "id",
    "concat('n', CAST(id AS STRING)) AS name",
    "CAST(id * 1.25 AS DECIMAL(10,2)) AS amount",
    "TIMESTAMP '2026-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0) AS ts",
    "TIMESTAMP_NTZ '2026-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, CAST(id AS INT), 0) AS ntz",
    "date_add(DATE '2026-01-01', CAST(id % 365 AS INT)) AS dt",
    "id % 2 = 0 AS flag",
    "array(CAST(id AS INT), CAST(id + 1 AS INT)) AS arr",
    "named_struct('a', CAST(id AS INT), 'b', concat('s', CAST(id AS STRING))) AS st",
    "map(concat('k', CAST(id % 3 AS STRING)), CAST(id AS INT)) AS mp",
)
MERGE_ON_READ = (
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)


def selected(start: int, stop: int) -> str:
    return f"SELECT {', '.join(COLUMNS)} FROM range({start}, {stop})"


def build_origin() -> str:
    direct_url = importlib.metadata.distribution("repark").read_text("direct_url.json")
    return "index-wheel" if direct_url is None else "local-build"


def refuse_wrong_release(spec: dict[str, Any]) -> None:
    identity = {"version": repark.__version__, "build": build_origin()}
    expected = {"version": spec["expect_version"], "build": spec["expect_build"]}
    if identity != expected:
        report = {"refused": True, "role": spec["role"], **identity, "expected": expected}
        print(json.dumps(report))
        sys.exit(REFUSED_EXIT)


def session(spec: dict[str, Any]) -> ReparkSession:
    builder = (
        ReparkSession.builder.appName(f"xrel-{spec['role']}-{spec['op']}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("repark.sql.allowCreateFormatVersion3", "true")
    )
    for key, value in spec["catalog_config"].items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def metadata_version(path: Path) -> int:
    match = METADATA_VERSION.match(path.name)
    if match is None:
        msg = f"not a versioned metadata file: {path.name}"
        raise ValueError(msg)
    return int(match.group(1))


def newest_metadata(table_dir: str) -> str:
    files = list((Path(table_dir) / "metadata").glob("*.metadata.json"))
    return str(max(files, key=metadata_version))


def table_name(spec: dict[str, Any]) -> str:
    return f"{spec['catalog']}.{spec['namespace']}.{spec['table']}"


def adopt(spark: ReparkSession, spec: dict[str, Any]) -> None:
    table_dir = spec.get("adopt_dir")
    if table_dir is None:
        return
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {spec['catalog']}.{spec['namespace']}")
    if spec["op"] == "create" and not (Path(table_dir) / "metadata").exists():
        return
    spark.sql(
        f"CALL {spec['catalog']}.system.register_table("
        f"table => '{spec['namespace']}.{spec['table']}', "
        f"metadata_file => '{newest_metadata(table_dir)}')"
    )


def create(spark: ReparkSession, spec: dict[str, Any]) -> None:
    table = table_name(spec)
    kind = spec["kind"]
    properties = f"'format-version' = '{spec['format']}'"
    if kind == "deletes":
        properties += MERGE_ON_READ
    if kind == "ns":
        spark.sql(
            f"CREATE OR REPLACE TABLE {table} (id BIGINT, tns timestamp_ns, tzns timestamptz_ns) "
            f"USING iceberg TBLPROPERTIES ({properties})"
        )
        spark.sql(
            f"INSERT INTO {table} VALUES "
            "(1, CAST('2026-01-01 00:00:00.123456789' AS timestamp_ns), "
            "CAST('2026-01-01 00:00:00.123456789' AS timestamptz_ns))"
        )
        return
    partitioned = " PARTITIONED BY (months(ts), bucket(4, id))" if kind == "partitioned" else ""
    spark.sql(
        f"CREATE OR REPLACE TABLE {table} USING iceberg{partitioned} "
        f"TBLPROPERTIES ({properties}) AS {selected(0, 1000)}"
    )
    if kind == "deletes":
        spark.sql(f"DELETE FROM {table} WHERE id % 10 = 0")
        spark.sql(
            f"MERGE INTO {table} AS t USING (SELECT id FROM range(0, 100) WHERE id % 2 = 1) AS s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = concat('m', CAST(t.id AS STRING))"
        )
    if kind == "evolved":
        spark.sql(f"ALTER TABLE {table} ADD COLUMN extra STRING")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN name TO label")
        spark.sql(
            f"INSERT INTO {table} SELECT {', '.join(COLUMNS)}, concat('e', CAST(id AS STRING)) "
            "FROM range(1000, 1010)"
        )


def append(spark: ReparkSession, spec: dict[str, Any]) -> None:
    spark.sql(f"INSERT INTO {table_name(spec)} {selected(5000, 5003)}")


def current_summary(spark: ReparkSession, table: str) -> tuple[int, int]:
    history = spark.sql(
        f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
    ).to_arrow()
    snapshot_id = int(history.column(0)[0].as_py())
    summary = spark.sql(
        f"SELECT summary['total-records'] FROM {table}.snapshots WHERE snapshot_id = {snapshot_id}"
    ).to_arrow()
    return snapshot_id, int(summary.column(0)[0].as_py())


def delete_files(spark: ReparkSession, table: str) -> list[str]:
    arrow = spark.sql(f"SELECT file_path FROM {table}.delete_files").to_arrow()
    return sorted(str(path) for path in arrow.column(0).to_pylist())


def read(spark: ReparkSession, spec: dict[str, Any]) -> dict[str, Any]:
    table = table_name(spec)
    arrow = spark.sql(f"SELECT * FROM {table} ORDER BY id").to_arrow()
    with pa.OSFile(spec["out"], "wb") as sink, ipc.new_file(sink, arrow.schema) as writer:
        writer.write_table(arrow)
    snapshot_id, total_records = current_summary(spark, table)
    return {
        "rows": arrow.num_rows,
        "snapshot_id": snapshot_id,
        "total_records": total_records,
        "delete_files": delete_files(spark, table),
    }


def main(argv: list[str]) -> int:
    spec = json.loads(Path(argv[1]).read_text(encoding="utf-8"))
    refuse_wrong_release(spec)
    spark = session(spec)
    try:
        adopt(spark, spec)
        if spec["op"] == "create":
            create(spark, spec)
        elif spec["op"] == "append":
            append(spark, spec)
        report = read(spark, spec)
    finally:
        spark.stop()
    report.update(
        {
            "refused": False,
            "role": spec["role"],
            "version": repark.__version__,
            "build": build_origin(),
        }
    )
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
