from __future__ import annotations

import datetime as dt
import importlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
TRUTH = HERE / "parquet_int96_as_ntz_1_oracle.json"
FIXTURES = HERE.parent / "fixtures" / "parquet_int96_as_ntz_1"
sys.path.insert(0, str(HERE.parent))
PINNED = importlib.import_module("_oracle_pins")
ICEBERG_RUNTIME_VERSION = PINNED.ICEBERG_RUNTIME_VERSION
RUNTIME = f"iceberg-spark-runtime-4.1_{PINNED.ICEBERG_SPARK_SCALA_BINARY}"
JAR = (
    REPO
    / ".ivy2"
    / "cache"
    / "org.apache.iceberg"
    / RUNTIME
    / "jars"
    / f"{RUNTIME}-{ICEBERG_RUNTIME_VERSION}.jar"
)
ZONES = ("UTC", "America/New_York", "Asia/Kolkata")
MODES = ("true", "false")
BASE_ROWS = (
    "SELECT * FROM VALUES "
    "(1, TIMESTAMP'2025-01-15 01:00:00', TIMESTAMP_NTZ'2025-01-15 01:00:00'), "
    "(2, TIMESTAMP'2025-07-01 12:00:00', TIMESTAMP_NTZ'2025-07-01 12:00:00'), "
    "(3, CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP_NTZ)), "
    "(4, TIMESTAMP'1960-03-01 05:30:00.25', TIMESTAMP_NTZ'1960-03-01 05:30:00.25'), "
    "(5, TIMESTAMP'2999-12-31 22:00:00.123456', TIMESTAMP_NTZ'2999-12-31 22:00:00.123456'), "
    "(6, TIMESTAMP'2025-01-15 03:00:00', TIMESTAMP_NTZ'2025-01-15 03:00:00'), "
    "(7, TIMESTAMP'2025-01-14 20:00:00', TIMESTAMP_NTZ'2025-01-14 20:00:00') "
    "AS t(id, ts, ntz)"
)
NESTED = (
    "SELECT id, ts, ntz, named_struct('t', ts, 'k', id) s, "
    "CASE WHEN ts IS NULL THEN NULL ELSE array(ts, ts + INTERVAL 1 HOUR) END a, "
    "CASE WHEN ts IS NULL THEN NULL ELSE map('k', ts) END m FROM b"
)
WALLS = (
    dt.datetime(2025, 1, 15, 1, 0, 0),
    dt.datetime(2025, 7, 1, 12, 0, 0),
    None,
    dt.datetime(1960, 3, 1, 5, 30, 0, 250000),
    dt.datetime(2999, 12, 31, 22, 0, 0, 123456),
    dt.datetime(2025, 1, 15, 3, 0, 0),
    dt.datetime(2025, 1, 14, 20, 0, 0),
)
VIEWS = {
    "t": "spark_int96",
    "mic": "spark_micros",
    "mix": "spark_mixdir",
    "pa_adj": "pyarrow_adjusted",
    "pa_naive": "pyarrow_naive",
    "pa_int96": "pyarrow_int96",
    "pa_int96_naive": "pyarrow_int96_naive",
}
FLAT = "spark_int96_flat"
APPEND_ZONES = ("UTC", "America/New_York")
APPENDS = {
    "a1_read_then_append": "",
    "a2_literal_append": "SELECT 2 AS id, TIMESTAMP'2025-01-16 00:00:00' AS ts",
}
APPEND_READ = "SELECT id, CAST(ts AS STRING) FROM w ORDER BY id, 2"
GAP_PINS = {
    "pyarrow_int96_naive_hint": (
        "SELECT id, CAST(ts AS STRING), typeof(ts) FROM pa_int96_naive ORDER BY id"
    ),
}
ICEBERG_ADD_FILES_READ = "SELECT id, CAST(ts AS STRING) FROM lc.db.added ORDER BY id"
ROUTES = ("parquet", "format", "sqlpath", "view")
PINS = {
    "str": "SELECT id, CAST(ts AS STRING) FROM t ORDER BY id",
    "hour": "SELECT id, hour(ts) FROM t ORDER BY id",
    "to_date": "SELECT id, CAST(to_date(ts) AS STRING) FROM t ORDER BY id",
    "date_filter": "SELECT id FROM t WHERE CAST(ts AS DATE) = DATE'2025-01-14' ORDER BY id",
    "range_filter": (
        "SELECT id FROM t WHERE ts >= TIMESTAMP'2025-01-15 00:00:00' "
        "AND ts < TIMESTAMP'2025-01-16 00:00:00' ORDER BY id"
    ),
    "max_min": "SELECT CAST(max(ts) AS STRING), CAST(min(ts) AS STRING), count(ts) FROM t",
    "group_by_date": "SELECT CAST(to_date(ts) AS STRING) d, count(*) FROM t GROUP BY 1 ORDER BY 1",
    "ts_eq_ntz": "SELECT id, ts = ntz FROM t ORDER BY id",
    "typeof": "SELECT typeof(ts), typeof(ntz), typeof(s), typeof(a), typeof(m) FROM t LIMIT 1",
    "nested_struct": "SELECT id, CAST(s.t AS STRING) FROM t ORDER BY id",
    "nested_array": "SELECT id, CAST(a[1] AS STRING) FROM t ORDER BY id",
    "nested_map": "SELECT id, CAST(m['k'] AS STRING) FROM t ORDER BY id",
    "join_micros": "SELECT a.id, b.id FROM t a JOIN mic b ON a.ts = b.ts ORDER BY 1",
    "join_pyarrow_adjusted": "SELECT a.id, b.id FROM t a JOIN pa_adj b ON a.ts = b.ts ORDER BY 1",
    "join_pyarrow_naive": "SELECT a.id, b.id FROM t a JOIN pa_naive b ON a.ts = b.ts ORDER BY 1",
    "unix_timestamp": "SELECT id, unix_timestamp(ts) FROM t ORDER BY id",
    "mixed_directory": "SELECT id, CAST(ts AS STRING), typeof(ts) FROM mix ORDER BY id",
}
CONTROLS = {
    "self_join": "SELECT a.id, b.id FROM t a JOIN t b ON a.ts = b.ts ORDER BY 1",
    "ntz_column": "SELECT id, CAST(ntz AS STRING), typeof(ntz) FROM t ORDER BY id",
    "spark_micros": "SELECT id, CAST(ts AS STRING), typeof(ts) FROM mic ORDER BY id",
    "pyarrow_adjusted": "SELECT id, CAST(ts AS STRING), typeof(ts) FROM pa_adj ORDER BY id",
    "pyarrow_naive": "SELECT id, CAST(ts AS STRING), typeof(ts) FROM pa_naive ORDER BY id",
    "pyarrow_int96": "SELECT id, CAST(ts AS STRING), typeof(ts) FROM pa_int96 ORDER BY id",
}
GAPS = {
    "iceberg_add_files": {
        "card": "task/roadmap/mid-term/iceberg-int96-null-1-card-2026-11-22.md",
        "cell": "iceberg_add_files/America/New_York",
        "repark": [[1, None], [2, None]],
    },
}
WRITE_SOURCE = "SELECT id, ts, s FROM t"
WRITE_READ = "SELECT id, CAST(ts AS STRING), CAST(s.t AS STRING) FROM w ORDER BY id"
ICEBERG_SOURCE = "SELECT id, ts FROM t"
ICEBERG_READ = "SELECT id, CAST(ts AS STRING) FROM {table} ORDER BY id"


def spark_session(warehouse: Path) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars", str(JAR))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_spark_fixtures(spark: Any, root: Path) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(BASE_ROWS).createOrReplaceTempView("b")
    nested = spark.sql(NESTED)
    nested.coalesce(1).write.mode("overwrite").parquet(str(root / "spark_int96"))
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    nested.coalesce(1).write.mode("overwrite").parquet(str(root / "spark_micros"))
    spark.conf.unset("spark.sql.parquet.outputTimestampType")
    flat = spark.sql("SELECT id, ts FROM b")
    flat.where("id <= 2").coalesce(1).write.mode("overwrite").parquet(str(root / FLAT))
    flat.where("id <= 3").coalesce(1).write.mode("overwrite").parquet(str(root / "spark_mixdir"))
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    flat.where("id > 3").coalesce(1).write.mode("append").parquet(str(root / "spark_mixdir"))
    spark.conf.unset("spark.sql.parquet.outputTimestampType")


def write_pyarrow_fixtures(root: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    ids = pa.array(range(1, 8), pa.int32())
    aware = [None if wall is None else wall.replace(tzinfo=dt.UTC) for wall in WALLS]
    shapes = {
        "pyarrow_adjusted": (pa.array(aware, pa.timestamp("us", tz="UTC")), False),
        "pyarrow_naive": (pa.array(list(WALLS), pa.timestamp("us")), False),
        "pyarrow_int96": (pa.array(aware, pa.timestamp("us", tz="UTC")), True),
        "pyarrow_int96_naive": (pa.array(list(WALLS), pa.timestamp("us")), True),
    }
    for name, (values, int96) in shapes.items():
        (root / name).mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"id": ids, "ts": values}),
            str(root / name / "part-00000.parquet"),
            use_deprecated_int96_timestamps=int96,
        )


def copy_fixtures(root: Path, names: list[str]) -> None:
    for name in names:
        target = FIXTURES / name
        shutil.rmtree(target, ignore_errors=True)
        target.mkdir(parents=True)
        for source in sorted((root / name).iterdir()):
            if source.name.endswith(".crc"):
                continue
            shutil.copyfile(source, target / source.name)


def rows_of(spark: Any, query: str) -> Any:
    try:
        return [list(row) for row in spark.sql(query).collect()]
    except Exception as error:
        return f"ERR {type(error).__name__}"


def route_frame(spark: Any, route: str, path: str) -> Any:
    if route == "parquet":
        return spark.read.parquet(path)
    if route == "format":
        return spark.read.format("parquet").load(path)
    if route == "sqlpath":
        return spark.sql(f"SELECT * FROM parquet.`{path}`")
    spark.read.parquet(path).createOrReplaceTempView("route_view")
    return spark.sql("SELECT * FROM route_view")


def schema_of(spark: Any, route: str, path: str) -> str:
    try:
        return route_frame(spark, route, path).schema.simpleString()
    except Exception as error:
        return f"ERR {type(error).__name__}"


def register_views(spark: Any, fixtures: Path) -> None:
    for view, name in VIEWS.items():
        spark.read.parquet(str(fixtures / name)).createOrReplaceTempView(view)


def zone_tag(zone: str) -> str:
    return zone.replace("/", "_")


def measure_reads(spark: Any, fixtures: Path, zone: str, found: dict[str, Any]) -> None:
    spark.conf.set("spark.sql.session.timeZone", zone)
    register_views(spark, fixtures)
    for route in ROUTES:
        found[f"schema/{route}/{zone}"] = schema_of(spark, route, str(fixtures / "spark_int96"))
    for name, query in {**PINS, **CONTROLS, **GAP_PINS}.items():
        found[f"rows/{name}/{zone}"] = rows_of(spark, query)


def measure_writes(spark: Any, fixtures: Path, scratch: Path, mode: str, found: dict) -> None:
    for written in ZONES:
        spark.conf.set("spark.sql.session.timeZone", written)
        register_views(spark, fixtures)
        out = scratch / f"w_{zone_tag(written)}_{mode}"
        spark.sql(WRITE_SOURCE).write.mode("overwrite").parquet(str(out))
        table = f"lc.db.w_{zone_tag(written).lower()}_{mode}"
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lc.db")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"CREATE TABLE {table} USING iceberg AS {ICEBERG_SOURCE}")
        for read in ZONES:
            spark.conf.set("spark.sql.session.timeZone", read)
            frame = spark.read.parquet(str(out))
            frame.createOrReplaceTempView("w")
            found[f"write_parquet/{written}/{read}"] = [
                frame.schema.simpleString(),
                rows_of(spark, WRITE_READ),
            ]
            found[f"write_iceberg/{written}/{read}"] = [
                spark.table(table).schema.simpleString(),
                rows_of(spark, ICEBERG_READ.format(table=table)),
            ]


def measure_appends(spark: Any, fixtures: Path, scratch: Path, mode: str, found: dict) -> None:
    for zone in APPEND_ZONES:
        spark.conf.set("spark.sql.session.timeZone", zone)
        for case, query in APPENDS.items():
            out = scratch / f"append_{case}_{zone_tag(zone)}_{mode}"
            shutil.rmtree(out, ignore_errors=True)
            shutil.copytree(fixtures / FLAT, out)
            source = spark.sql(query) if query else spark.read.parquet(str(fixtures / FLAT))
            source.write.mode("append").parquet(str(out))
            frame = spark.read.parquet(str(out))
            frame.createOrReplaceTempView("w")
            found[f"append/{case}/{zone}"] = [
                frame.schema.simpleString(),
                rows_of(spark, APPEND_READ),
            ]


def measure_iceberg_add_files(spark: Any, fixtures: Path, found: dict) -> None:
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lc.db")
    spark.sql("DROP TABLE IF EXISTS lc.db.added")
    spark.sql("CREATE TABLE lc.db.added (id INT, ts TIMESTAMP) USING iceberg")
    source = f"`parquet`.`{fixtures / FLAT}`"
    spark.sql(f"CALL lc.system.add_files(table => 'db.added', source_table => '{source}')")
    found["iceberg_add_files/America/New_York"] = rows_of(spark, ICEBERG_ADD_FILES_READ)


def measure(spark: Any, fixtures: Path, scratch: Path) -> dict[str, dict[str, Any]]:
    truth: dict[str, dict[str, Any]] = {}
    for mode in MODES:
        spark.conf.set("spark.sql.ansi.enabled", mode)
        found: dict[str, Any] = {}
        for zone in ZONES:
            measure_reads(spark, fixtures, zone, found)
        measure_writes(spark, fixtures, scratch, mode, found)
        measure_appends(spark, fixtures, scratch, mode, found)
        measure_iceberg_add_files(spark, fixtures, found)
        truth[mode] = found
    return truth


def document(truth: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "basis": "PySpark 4.1.2 local[1], " + RUNTIME + ":" + ICEBERG_RUNTIME_VERSION,
        "zones": list(ZONES),
        "views": VIEWS,
        "routes": list(ROUTES),
        "pins": PINS,
        "controls": CONTROLS,
        "write_source": WRITE_SOURCE,
        "write_read": WRITE_READ,
        "iceberg_source": ICEBERG_SOURCE,
        "iceberg_read": ICEBERG_READ,
        "flat": FLAT,
        "append_zones": list(APPEND_ZONES),
        "appends": APPENDS,
        "append_read": APPEND_READ,
        "gap_pins": GAP_PINS,
        "gaps": GAPS,
        "truth": truth,
    }


def main(argv: list[str]) -> int:
    rewrite = "--rewrite" in argv
    with tempfile.TemporaryDirectory(prefix="parquet-int96-") as scratch_name:
        scratch = Path(scratch_name)
        spark = spark_session(scratch / "wh")
        if "--fixtures" in argv:
            written = scratch / "fixtures"
            write_spark_fixtures(spark, written)
            write_pyarrow_fixtures(written)
            copy_fixtures(written, sorted(set(VIEWS.values()) | {FLAT}))
        truth = measure(spark, FIXTURES, scratch / "out")
        spark.stop()
    recorded = document(truth)
    if rewrite:
        TRUTH.write_text(json.dumps(recorded, indent=1, sort_keys=True) + "\n")
        return 0
    stored = json.loads(TRUTH.read_text())
    drift = sorted(
        f"{mode}:{cell}"
        for mode in MODES
        for cell in set(stored["truth"][mode]) | set(truth[mode])
        if stored["truth"][mode].get(cell) != truth[mode].get(cell)
    )
    if stored != json.loads(json.dumps(recorded)):
        print("drift:", drift or "cell definitions")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
