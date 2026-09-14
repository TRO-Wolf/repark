"""ICE-SPARK-TABLE-1 — RePark writes into a Spark-created Iceberg v2 CoW table.

pins: ice-spark-table-1/C-001, C-002, C-003, C-004, C-005, C-006
pins: ice-spark-table-1/C-007, C-008, C-009, C-010, C-011
"""

from __future__ import annotations

import json
import os
import re
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE_SRC = _REPO_ROOT / "python/repark-parity/fixtures/torture/data/ice_spark_table_1"
_TABLE_ROOT = Path("/tmp/repark-ice-spark-table-1/ns/facts")
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "ice_spark_table_1"
_RESIDUAL_CATALOG = "ice_spark_table_1_resid"
_NAMESPACE = "ns"
_TABLE = "facts"
_FQ_TABLE = f"{_CATALOG}.{_NAMESPACE}.{_TABLE}"
_TABLE_ARG = f"{_NAMESPACE}.{_TABLE}"
_DDL = (
    "(id BIGINT, name STRING, amount DECIMAL(12,2), "
    "ingestion_timestamp TIMESTAMP, ds STRING) USING iceberg PARTITIONED BY (ds)"
)
_TBLPROPERTIES = (
    "'format-version' = 2, "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write', "
    "'write.target-file-size-bytes' = '268435456'"
)
_SPARK_ONLY_PROPERTY_KEYS = ("owner", "write.parquet.compression-codec")
_REPARK_ONLY_SUMMARY_KEY = "engine.operation-id"
_SPARK_ONLY_SUMMARY_KEYS = (
    "app-id",
    "app-name",
    "engine-name",
    "engine-version",
    "iceberg-version",
    "manifests-created",
    "manifests-kept",
    "manifests-replaced",
    "spark.app.id",
)
_REPARK_METADATA_NAME = re.compile(r"^\d{5}-[0-9a-f-]{36}\.metadata\.json$")
_SPARK_METADATA_NAME = re.compile(r"^v\d+\.metadata\.json$")
_TRANSFORM_REFUSAL = (
    "sorting by the table's default sort order uses transform `bucket[4]` on source id 1, "
    "only identity sort fields are supported"
)
_MERGE_SQL = (
    f"MERGE INTO {_FQ_TABLE} AS Target USING ice1_staging AS Source "
    "ON Target.id = Source.id "
    "WHEN MATCHED THEN UPDATE SET * "
    "WHEN NOT MATCHED THEN INSERT *"
)
_MAINTENANCE_CALLS = (
    (
        "expire_snapshots",
        f"CALL {_CATALOG}.system.expire_snapshots(table => '{_TABLE_ARG}', "
        "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 3)",
    ),
    (
        "rewrite_manifests",
        f"CALL {_CATALOG}.system.rewrite_manifests(table => '{_TABLE_ARG}')",
    ),
    (
        "rewrite_data_files",
        f"CALL {_CATALOG}.system.rewrite_data_files(table => '{_TABLE_ARG}', "
        "strategy => 'binpack')",
    ),
    (
        "remove_orphan_files",
        f"CALL {_CATALOG}.system.remove_orphan_files(table => '{_TABLE_ARG}', "
        "older_than => TIMESTAMP '2020-01-01 00:00:00', dry_run => false)",
    ),
    (
        "rewrite_position_delete_files",
        f"CALL {_CATALOG}.system.rewrite_position_delete_files(table => '{_TABLE_ARG}')",
    ),
)


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(
                        f"fixture lock {path} held for 2 minutes (no steal)"
                    ) from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize() -> Iterator[Path]:
    """Copy the checked-in Spark-written table to its baked-in canonical location."""
    lock = _DirLock(Path(str(_TABLE_ROOT) + ".lock"))
    try:
        if _TABLE_ROOT.exists():
            shutil.rmtree(_TABLE_ROOT)
        _TABLE_ROOT.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(_FIXTURE_SRC, _TABLE_ROOT, copy_function=shutil.copy)
        yield _TABLE_ROOT
    finally:
        lock.close()


def _newest_metadata_file(table_root: Path) -> Path:
    """The highest-versioned vN.metadata.json under the table's metadata directory."""
    versions = sorted(
        (table_root / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    if not versions:
        raise ValueError(f"no vN metadata files under {table_root}/metadata")
    return versions[-1]


def _seed_rows() -> list[tuple[int, str, Decimal, datetime, str]]:
    """The twenty rows Spark seeded into the fixture and the live table."""
    rows: list[tuple[int, str, Decimal, datetime, str]] = []
    base = datetime(2026, 9, 13, 10, 0, 0)
    for index in range(1, 11):
        rows.append(
            (
                index,
                f"seed-{index}",
                Decimal(f"{index}.25"),
                base + timedelta(minutes=index),
                "2026-09-13",
            )
        )
    for index in range(11, 21):
        rows.append(
            (
                index,
                f"seed-{index}",
                Decimal(f"{index}.25"),
                base + timedelta(hours=24, minutes=index),
                "2026-09-14",
            )
        )
    return rows


def _merge_source_one() -> list[tuple[int, str, Decimal, datetime, str]]:
    """First staging batch: an older duplicate that dedup drops, updates, inserts."""
    base = datetime(2026, 9, 15, 8, 0, 0)
    rows = [(5, "dup-old-5", Decimal("5.55"), base - timedelta(hours=1), "2026-09-15")]
    for index in range(5, 11):
        rows.append(
            (
                index,
                f"m1-{index}",
                Decimal(f"{index}.99"),
                base + timedelta(minutes=index),
                "2026-09-15",
            )
        )
    for index in range(21, 31):
        rows.append(
            (
                index,
                f"new-{index}",
                Decimal(f"{index}.50"),
                base + timedelta(minutes=index),
                "2026-09-15",
            )
        )
    return rows


def _merge_source_two() -> list[tuple[int, str, Decimal, datetime, str]]:
    """Second staging batch: overlapping updates and new ids on a fourth partition."""
    base = datetime(2026, 9, 16, 8, 0, 0)
    rows: list[tuple[int, str, Decimal, datetime, str]] = []
    for index in range(21, 26):
        rows.append(
            (
                index,
                f"m2-{index}",
                Decimal(f"{index}.77"),
                base + timedelta(minutes=index),
                "2026-09-16",
            )
        )
    for index in range(31, 36):
        rows.append(
            (
                index,
                f"new-{index}",
                Decimal(f"{index}.60"),
                base + timedelta(minutes=index),
                "2026-09-16",
            )
        )
    return rows


def _expected_final_rows() -> list[tuple[int, str, str]]:
    """``(id, name, ds)`` after both production MERGEs, in id order."""
    rows = [(index, f"seed-{index}", "2026-09-13") for index in range(1, 5)]
    rows += [(index, f"m1-{index}", "2026-09-15") for index in range(5, 11)]
    rows += [(index, f"seed-{index}", "2026-09-14") for index in range(11, 21)]
    rows += [(index, f"m2-{index}", "2026-09-16") for index in range(21, 26)]
    rows += [(index, f"new-{index}", "2026-09-15") for index in range(26, 31)]
    rows += [(index, f"new-{index}", "2026-09-16") for index in range(31, 36)]
    return rows


def _values_sql(rows: list[tuple[int, str, Decimal, datetime, str]]) -> str:
    """A Spark VALUES list with typed decimal and timestamp literals."""
    literals = []
    for row_id, name, amount, stamp, ds in rows:
        literals.append(
            f"({row_id}, '{name}', CAST('{amount}' AS DECIMAL(12,2)), "
            f"TIMESTAMP '{stamp:%Y-%m-%d %H:%M:%S}', '{ds}')"
        )
    return ", ".join(literals)


def _write_merge_source(path: Path, rows: list[tuple[int, str, Decimal, datetime, str]]) -> None:
    """Write a staging parquet with the table's exact Arrow schema."""
    table = pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.int64()),
            "name": pa.array([row[1] for row in rows], type=pa.string()),
            "amount": pa.array([row[2] for row in rows], type=pa.decimal128(12, 2)),
            "ingestion_timestamp": pa.array(
                [row[3] for row in rows], type=pa.timestamp("us", tz="UTC")
            ),
            "ds": pa.array([row[4] for row in rows], type=pa.string()),
        }
    )
    pq.write_table(table, str(path))


def _id_name_ds_rows(table: pa.Table) -> list[tuple[int, str, str]]:
    """``(id, name, ds)`` rows from an Arrow table, sorted by id."""
    ids = table.column("id").to_pylist()
    names = table.column("name").to_pylist()
    partitions = table.column("ds").to_pylist()
    rows = list(zip(ids, names, partitions, strict=True))
    rows.sort(key=lambda row: row[0])
    return [(int(one), str(two), str(three)) for one, two, three in rows]


def _schema_field_key(field: dict[str, Any]) -> tuple[Any, ...]:
    """A metadata-schema field reduced to ``(id, name, type, required)``."""
    return (
        field["id"],
        field["name"],
        str(field["type"]).replace(" ", ""),
        field["required"],
    )


def _assert_table_schema(table: pa.Table) -> None:
    """Fail unless the Arrow schema is the Spark-written five-column shape."""
    assert table.schema.field("id").type == pa.int64(), table.schema
    assert table.schema.field("name").type == pa.string(), table.schema
    assert table.schema.field("amount").type == pa.decimal128(12, 2), table.schema
    stamp = table.schema.field("ingestion_timestamp").type
    assert stamp == pa.timestamp("us", tz="UTC"), table.schema
    assert table.schema.field("ds").type == pa.string(), table.schema


def _stage_and_merge(session: Any, source: Path, staging_sql: str) -> None:
    """The production staging leg: parquet source, row_number dedup, temp view, MERGE."""
    from repark import Window
    from repark import functions as F  # noqa: N812 — PySpark idiom

    frame = session.read.parquet(str(source))
    window = Window.partitionBy("id").orderBy(F.col("ingestion_timestamp").desc())
    deduped = (
        frame.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    deduped.createOrReplaceTempView("ice1_staging")
    session.sql(staging_sql).collect()


def _repark_write_phase(
    session: Any, src_dir: Path, merge_sql: str = _MERGE_SQL
) -> dict[str, pa.Table]:
    """Both production MERGEs and the weekly maintenance CALLs; returns CALL outputs."""
    src_dir.mkdir(parents=True, exist_ok=True)
    for index, rows in enumerate((_merge_source_one(), _merge_source_two()), start=1):
        source = src_dir / f"merge{index}.parquet"
        _write_merge_source(source, rows)
        _stage_and_merge(session, source, merge_sql)
    outputs: dict[str, pa.Table] = {}
    for name, statement in _MAINTENANCE_CALLS:
        outputs[name] = session.sql(statement).to_arrow()
    return outputs


def _assert_maintenance_outputs(outputs: dict[str, pa.Table]) -> None:
    """The measured CALL results on the Spark-created two-partition seed."""
    expire = outputs["expire_snapshots"].to_pylist()
    assert expire == [
        {
            "deleted_data_files_count": 0,
            "deleted_position_delete_files_count": 0,
            "deleted_equality_delete_files_count": 0,
            "deleted_manifest_files_count": 0,
            "deleted_manifest_lists_count": 1,
            "deleted_statistics_files_count": 0,
        }
    ], expire
    manifests = outputs["rewrite_manifests"].to_pylist()
    assert manifests == [{"rewritten_manifests_count": 3, "added_manifests_count": 1}], manifests
    data_files = outputs["rewrite_data_files"].to_pylist()
    assert data_files == [
        {
            "rewritten_data_files_count": 0,
            "added_data_files_count": 0,
            "rewritten_bytes_count": 0,
            "failed_data_files_count": 0,
            "removed_delete_files_count": 0,
        }
    ], data_files
    assert outputs["remove_orphan_files"].num_rows == 0
    deletes = outputs["rewrite_position_delete_files"].to_pylist()
    assert deletes == [
        {
            "rewritten_delete_files_count": 0,
            "added_delete_files_count": 0,
            "rewritten_bytes_count": 0,
            "added_bytes_count": 0,
        }
    ], deletes


def _assert_final_state(session: Any, table_root: Path) -> pa.Table:
    """Row set, snapshot log, file naming and version-hint after the write phase."""
    answer = session.sql(
        f"SELECT id, name, amount, ingestion_timestamp, ds FROM {_FQ_TABLE} ORDER BY id"
    ).to_arrow()
    _assert_table_schema(answer)
    assert _id_name_ds_rows(answer) == _expected_final_rows()
    counts = session.sql(
        f"SELECT ds, COUNT(*) AS c FROM {_FQ_TABLE} GROUP BY ds ORDER BY ds"
    ).to_arrow()
    by_ds = dict(zip(counts.column("ds").to_pylist(), counts.column("c").to_pylist(), strict=True))
    assert by_ds == {
        "2026-09-13": 4,
        "2026-09-14": 10,
        "2026-09-15": 11,
        "2026-09-16": 10,
    }, by_ds
    deletes = session.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}.delete_files").to_arrow()
    assert deletes.column("c")[0].as_py() == 0
    snapshots = session.sql(
        f"SELECT operation FROM {_FQ_TABLE}.snapshots ORDER BY committed_at"
    ).to_arrow()
    assert snapshots.column("operation").to_pylist() == [
        "append",
        "overwrite",
        "overwrite",
        "replace",
    ]
    metadata_names = sorted(path.name for path in (table_root / "metadata").glob("*.metadata.json"))
    assert metadata_names == [f"v{index}.metadata.json" for index in range(1, 8)]
    hint = (table_root / "metadata" / "version-hint.text").read_text(encoding="utf-8")
    assert hint.strip() == "3"
    return answer


def _register_spark_table(session: Any, metadata_file: Path) -> None:
    """Adopt the Spark-written metadata file into the memory catalog."""
    session.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => '{_TABLE_ARG}', metadata_file => '{metadata_file}')"
    )


def test_spark_created_fixture_adopted_merged_and_maintained(tmp_path: Path) -> None:
    """The checked-in Spark-written fixture: register, MERGE twice, five CALLs."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("ice-spark-table-1-fixture").getOrCreate()
    try:
        spark.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        spark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        with _materialize():
            _register_spark_table(spark, _TABLE_ROOT / "metadata" / "v3.metadata.json")
            seed = spark.sql(
                f"SELECT id, name, amount, ingestion_timestamp, ds FROM {_FQ_TABLE} ORDER BY id"
            ).to_arrow()
            _assert_table_schema(seed)
            assert _id_name_ds_rows(seed) == [(row[0], row[1], row[4]) for row in _seed_rows()]
            amounts = seed.column("amount").to_pylist()
            assert amounts[:3] == [
                Decimal("1.25"),
                Decimal("2.25"),
                Decimal("3.25"),
            ], amounts
            outputs = _repark_write_phase(spark, tmp_path / "staging")
            _assert_maintenance_outputs(outputs)
            _assert_final_state(spark, _TABLE_ROOT)
    finally:
        spark.stop()


def test_spark_created_and_repark_created_metadata_shapes(tmp_path: Path) -> None:
    """The property, summary, naming and hint deltas against a RePark-created twin."""
    from repark import ReparkSession

    spark_doc = json.loads(
        (_FIXTURE_SRC / "metadata" / "v3.metadata.json").read_text(encoding="utf-8")
    )
    spark = ReparkSession.builder.appName("ice-spark-table-1-twin").getOrCreate()
    try:
        spark.register_memory_catalog(_CATALOG, tmp_path / "twin-warehouse")
        spark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        spark.sql(f"CREATE TABLE {_FQ_TABLE} {_DDL} TBLPROPERTIES ({_TBLPROPERTIES})")
        rows = _seed_rows()
        spark.sql(f"INSERT INTO {_FQ_TABLE} VALUES {_values_sql(rows[:10])}")
        spark.sql(f"INSERT INTO {_FQ_TABLE} VALUES {_values_sql(rows[10:])}")
        twin_root = tmp_path / "twin-warehouse" / "repark_ctas" / _CATALOG / _NAMESPACE / _TABLE
        metadata_names = sorted(
            path.name for path in (twin_root / "metadata").glob("*.metadata.json")
        )
        assert len(metadata_names) == 3, metadata_names
        assert all(_REPARK_METADATA_NAME.match(name) for name in metadata_names)
        twin_doc = json.loads(
            (twin_root / "metadata" / metadata_names[-1]).read_text(encoding="utf-8")
        )
        assert not (twin_root / "metadata" / "version-hint.text").exists()
    finally:
        spark.stop()
    spark_names = sorted(path.name for path in (_FIXTURE_SRC / "metadata").glob("*.metadata.json"))
    assert spark_names == ["v1.metadata.json", "v2.metadata.json", "v3.metadata.json"]
    assert (_FIXTURE_SRC / "metadata" / "version-hint.text").read_text(
        encoding="utf-8"
    ).strip() == "3"
    spark_props = spark_doc["properties"]
    twin_props = twin_doc["properties"]
    assert twin_props == {
        "write.merge.mode": "copy-on-write",
        "write.delete.mode": "copy-on-write",
        "write.update.mode": "copy-on-write",
        "write.target-file-size-bytes": "268435456",
    }, twin_props
    assert spark_props == {
        **twin_props,
        "owner": spark_props["owner"],
        "write.parquet.compression-codec": "zstd",
    }, spark_props
    assert isinstance(spark_props["owner"], str) and spark_props["owner"]
    assert spark_doc["format-version"] == twin_doc["format-version"] == 2
    assert spark_doc["partition-specs"] == twin_doc["partition-specs"]
    assert spark_doc["sort-orders"] == twin_doc["sort-orders"]
    assert spark_doc["default-sort-order-id"] == twin_doc["default-sort-order-id"] == 0

    assert [_schema_field_key(f) for f in spark_doc["schemas"][0]["fields"]] == [
        _schema_field_key(f) for f in twin_doc["schemas"][0]["fields"]
    ]
    spark_keys = set(spark_doc["snapshots"][0]["summary"])
    twin_keys = set(twin_doc["snapshots"][0]["summary"])
    assert set(_SPARK_ONLY_SUMMARY_KEYS) <= spark_keys - twin_keys
    assert _REPARK_ONLY_SUMMARY_KEY in twin_keys - spark_keys


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_spark_created_table_roundtrip(tmp_path: Path) -> None:
    """Spark creates and seeds; RePark writes and maintains; Spark reads it all back."""
    import _live_parity as lp
    from pyspark.sql import SparkSession

    from repark import ReparkSession

    prior = SparkSession.getActiveSession()
    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog=_CATALOG).session
    assert SparkSession.getActiveSession() is spark
    if prior is not None:
        assert spark is prior
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.{_NAMESPACE}")
    spark.sql(f"CREATE TABLE {_FQ_TABLE} {_DDL} TBLPROPERTIES ({_TBLPROPERTIES})")
    rows = _seed_rows()
    spark.sql(f"INSERT INTO {_FQ_TABLE} VALUES {_values_sql(rows[:10])}")
    spark.sql(f"INSERT INTO {_FQ_TABLE} VALUES {_values_sql(rows[10:])}")
    seeded = spark.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}").toArrow()
    assert seeded.column("c")[0].as_py() == 20
    table_root = warehouse / _NAMESPACE / _TABLE
    spark_meta = _newest_metadata_file(table_root)
    assert spark_meta.name == "v3.metadata.json"
    spark_doc = json.loads(spark_meta.read_text(encoding="utf-8"))
    props = spark_doc["properties"]
    for key in _SPARK_ONLY_PROPERTY_KEYS:
        assert props[key], props
    assert spark_doc["default-sort-order-id"] == 0
    assert (table_root / "metadata" / "version-hint.text").exists()

    repark = ReparkSession.builder.appName("ice-spark-table-1-live").getOrCreate()
    try:
        repark.register_memory_catalog(_CATALOG, tmp_path / "repark-warehouse")
        repark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        _register_spark_table(repark, spark_meta)
        adopted = repark.sql(f"SELECT id, name, ds FROM {_FQ_TABLE} ORDER BY id").to_arrow()
        assert _id_name_ds_rows(adopted) == [(row[0], row[1], row[4]) for row in _seed_rows()]
        outputs = _repark_write_phase(repark, tmp_path / "staging")
        _assert_maintenance_outputs(outputs)
        answer = _assert_final_state(repark, table_root)
        answer_path = tmp_path / "repark-answer.parquet"
        pq.write_table(answer, str(answer_path))
    finally:
        repark.stop()

    stale = spark.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}").toArrow()
    assert stale.column("c")[0].as_py() == 20
    spark.catalog.refreshTable(_FQ_TABLE)
    refreshed = spark.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}").toArrow()
    assert refreshed.column("c")[0].as_py() == 35
    newest = _newest_metadata_file(table_root)
    assert newest.name == "v7.metadata.json"
    spark.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => 'ns.facts_repark', metadata_file => '{newest}')"
    )
    spark.read.parquet(str(answer_path)).createOrReplaceTempView("repark_answer")
    columns = "id, name, amount, ingestion_timestamp, ds"
    forward = spark.sql(
        f"SELECT {columns} FROM {_CATALOG}.ns.facts_repark "
        f"EXCEPT ALL SELECT {columns} FROM repark_answer"
    ).toArrow()
    assert forward.num_rows == 0
    backward = spark.sql(
        f"SELECT {columns} FROM repark_answer "
        f"EXCEPT ALL SELECT {columns} FROM {_CATALOG}.ns.facts_repark"
    ).toArrow()
    assert backward.num_rows == 0
    counted = spark.sql(f"SELECT COUNT(*) AS c FROM {_CATALOG}.ns.facts_repark").toArrow()
    assert counted.column("c")[0].as_py() == 35
    partition_counts = spark.sql(
        f"SELECT ds, COUNT(*) AS c FROM {_CATALOG}.ns.facts_repark GROUP BY ds ORDER BY ds"
    ).toArrow()
    by_ds = dict(
        zip(
            partition_counts.column("ds").to_pylist(),
            partition_counts.column("c").to_pylist(),
            strict=True,
        )
    )
    assert by_ds == {
        "2026-09-13": 4,
        "2026-09-14": 10,
        "2026-09-15": 11,
        "2026-09-16": 10,
    }, by_ds
    readback = f"{_CATALOG}.ns.facts_repark"
    for meta_table in ("snapshots", "history", "files"):
        frame = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}.{meta_table}").toArrow()
        assert frame.column("c")[0].as_py() == 4, meta_table
    deletes = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}.delete_files").toArrow()
    assert deletes.column("c")[0].as_py() == 0


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_transform_sort_residual(tmp_path: Path) -> None:
    """The bucket-transform sort order refuses loudly and commits no snapshot."""
    import _live_parity as lp
    from pyspark.sql import SparkSession

    from repark import ReparkSession

    prior = SparkSession.getActiveSession()
    warehouse = tmp_path / "residual-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog=_RESIDUAL_CATALOG).session
    assert SparkSession.getActiveSession() is spark
    if prior is not None:
        assert spark is prior
    fq_sorted = f"{_RESIDUAL_CATALOG}.ns.sorted"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_RESIDUAL_CATALOG}.ns")
    spark.sql(f"CREATE TABLE {fq_sorted} {_DDL} TBLPROPERTIES ({_TBLPROPERTIES})")
    spark.sql(
        f"INSERT INTO {fq_sorted} VALUES "
        "(1, 'a', CAST('1.00' AS DECIMAL(12,2)), "
        "TIMESTAMP '2026-09-13 08:00:00', '2026-09-13'), "
        "(2, 'b', CAST('2.00' AS DECIMAL(12,2)), "
        "TIMESTAMP '2026-09-13 08:01:00', '2026-09-13'), "
        "(3, 'c', CAST('3.00' AS DECIMAL(12,2)), "
        "TIMESTAMP '2026-09-13 08:02:00', '2026-09-13')"
    )
    spark.sql(f"ALTER TABLE {fq_sorted} WRITE ORDERED BY (bucket(4, id))")
    snapshots_before = spark.sql(
        f"SELECT snapshot_id FROM {fq_sorted}.snapshots ORDER BY committed_at"
    ).toArrow()
    ids_before = [int(v) for v in snapshots_before.column("snapshot_id").to_pylist()]
    table_root = warehouse / "ns" / "sorted"
    doc = json.loads(_newest_metadata_file(table_root).read_text(encoding="utf-8"))
    assert doc["properties"]["write.distribution-mode"] == "range"
    assert doc["sort-orders"][1]["fields"][0]["transform"] == "bucket[4]"

    repark = ReparkSession.builder.appName("ice-spark-table-1-residual").getOrCreate()
    try:
        repark.register_memory_catalog(_RESIDUAL_CATALOG, tmp_path / "repark-residual-warehouse")
        repark.sql(f"CREATE NAMESPACE {_RESIDUAL_CATALOG}.ns")
        repark.sql(
            f"CALL {_RESIDUAL_CATALOG}.system.register_table("
            f"table => 'ns.sorted', metadata_file => "
            f"'{_newest_metadata_file(table_root)}')"
        )
        seed = repark.sql(f"SELECT id, name FROM {fq_sorted} ORDER BY id").to_arrow()
        assert seed.to_pylist() == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3, "name": "c"},
        ]
        source = tmp_path / "sorted-merge.parquet"
        _write_merge_source(
            source,
            [
                (
                    2,
                    "zz",
                    Decimal("2.00"),
                    datetime(2026, 9, 17, 8, 0, 0),
                    "2026-09-17",
                ),
                (
                    9,
                    "n9",
                    Decimal("9.00"),
                    datetime(2026, 9, 17, 8, 0, 0),
                    "2026-09-17",
                ),
            ],
        )
        merge_sql = _MERGE_SQL.replace(_FQ_TABLE, fq_sorted)
        with pytest.raises(Exception, match=re.escape(_TRANSFORM_REFUSAL)):
            _stage_and_merge(repark, source, merge_sql)
        still = repark.sql(f"SELECT COUNT(*) AS c FROM {fq_sorted}").to_arrow()
        assert still.column("c")[0].as_py() == 3
    finally:
        repark.stop()
    snapshots_after = spark.sql(
        f"SELECT snapshot_id FROM {fq_sorted}.snapshots ORDER BY committed_at"
    ).toArrow()
    ids_after = [int(v) for v in snapshots_after.column("snapshot_id").to_pylist()]
    assert ids_after == ids_before
