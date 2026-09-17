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
from datetime import UTC, datetime, timedelta
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
_TWIN_TBLPROPERTIES = _TBLPROPERTIES + ", 'write.distribution-mode' = 'hash'"
_SPARK_ONLY_PROPERTY_KEYS = ("owner",)
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
_SPARK_OWNED_ROWS = {
    19: (19, "spark-19", Decimal("119.01"), datetime(2026, 9, 14, 7, 0, 0), "2026-09-14"),
    20: (20, "spark-20", Decimal("120.01"), datetime(2026, 9, 14, 7, 0, 1), "2026-09-14"),
}
_ADOPTED_METADATA = "v5.metadata.json"
_FINAL_METADATA_COUNT = 9
_ADOPTED_HINT = "5"
_EXPECTED_SNAPSHOT_OPS = ["append", "overwrite", "overwrite", "overwrite", "replace"]
_EXPECTED_PARTITION_COUNTS = {
    "2026-09-13": 4,
    "2026-09-14": 10,
    "2026-09-15": 11,
    "2026-09-16": 10,
}


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
        with suppress(OSError):
            if _TABLE_ROOT.exists():
                shutil.rmtree(_TABLE_ROOT)
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


def _spark_merge_sql() -> str:
    """Spark's own CoW MERGE before adoption: updates ids 19 and 20 only."""
    selects = " UNION ALL ".join(
        f"SELECT {row[0]} AS id, '{row[1]}' AS name, "
        f"CAST('{row[2]}' AS DECIMAL(12,2)) AS amount, "
        f"TIMESTAMP '{row[3]:%Y-%m-%d %H:%M:%S}' AS ingestion_timestamp, "
        f"'{row[4]}' AS ds"
        for row in _SPARK_OWNED_ROWS.values()
    )
    return (
        f"MERGE INTO {_FQ_TABLE} AS Target USING ({selects}) AS Source "
        "ON Target.id = Source.id WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def _hash_distribution_sql() -> str:
    """Stamp the production distribution mode before adoption."""
    return f"ALTER TABLE {_FQ_TABLE} SET TBLPROPERTIES ('write.distribution-mode'='hash')"


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


def _dedup_latest(
    rows: list[tuple[int, str, Decimal, datetime, str]],
) -> list[tuple[int, str, Decimal, datetime, str]]:
    """The row_number-over-ingestion_timestamp dedup applied to a staging batch."""
    best: dict[int, tuple[int, str, Decimal, datetime, str]] = {}
    for row in rows:
        if row[0] not in best or row[3] > best[row[0]][3]:
            best[row[0]] = row
    return list(best.values())


def _merged_rows(
    base: list[tuple[int, str, Decimal, datetime, str]],
    source: list[tuple[int, str, Decimal, datetime, str]],
) -> list[tuple[int, str, Decimal, datetime, str]]:
    """The UPDATE SET * / INSERT * outcome of one deduped staging batch."""
    merged = {row[0]: row for row in base}
    merged.update({row[0]: row for row in _dedup_latest(source)})
    return [merged[key] for key in sorted(merged)]


def _expected_seed_rows() -> list[tuple[int, str, Decimal, datetime, str]]:
    """The adopted state: Spark's seed plus its own CoW MERGE updates."""
    return _merged_rows(_seed_rows(), list(_SPARK_OWNED_ROWS.values()))


def _expected_merge1_rows() -> list[tuple[int, str, Decimal, datetime, str]]:
    """The independently derived 30-row state after the first production MERGE."""
    return _merged_rows(_expected_seed_rows(), _merge_source_one())


def _expected_final_rows() -> list[tuple[int, str, Decimal, datetime, str]]:
    """The independently derived 35-row state after the second production MERGE."""
    return _merged_rows(_expected_merge1_rows(), _merge_source_two())


def _truth_seed_rows() -> list[tuple[int, str, Decimal, datetime, str]]:
    """The checked-in fixture's seed oracle, parsed from truth.json."""
    truth = json.loads((_FIXTURE_SRC / "truth.json").read_text(encoding="utf-8"))
    rows: list[tuple[int, str, Decimal, datetime, str]] = []
    for row_id, name, amount, stamp, ds in truth["seed_rows"]:
        rows.append(
            (
                int(row_id),
                str(name),
                Decimal(str(amount)),
                datetime.fromisoformat(str(stamp)),
                str(ds),
            )
        )
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


def _write_rows_parquet(path: Path, rows: list[tuple[int, str, Decimal, datetime, str]]) -> None:
    """Write rows as a parquet with the table's exact Arrow schema."""
    table = pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.int64()),
            "name": pa.array([row[1] for row in rows], type=pa.string()),
            "amount": pa.array([row[2] for row in rows], type=pa.decimal128(12, 2)),
            "ingestion_timestamp": pa.array(
                [row[3].replace(tzinfo=UTC) for row in rows],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ds": pa.array([row[4] for row in rows], type=pa.string()),
        }
    )
    pq.write_table(table, str(path))


def _full_rows(table: pa.Table) -> list[tuple[int, str, Decimal, datetime, str]]:
    """All five columns of an Arrow table as tuples, sorted by id."""
    rows = sorted(table.to_pylist(), key=lambda row: row["id"])
    return [
        (
            int(row["id"]),
            str(row["name"]),
            row["amount"],
            row["ingestion_timestamp"],
            str(row["ds"]),
        )
        for row in rows
    ]


def _expected_typed(
    rows: list[tuple[int, str, Decimal, datetime, str]],
) -> list[tuple[int, str, Decimal, datetime, str]]:
    """Expected rows with the UTC tz the Arrow read-back carries."""
    return [(row[0], row[1], row[2], row[3].replace(tzinfo=UTC), row[4]) for row in rows]


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


def _assert_table_rows(
    table: pa.Table, expected: list[tuple[int, str, Decimal, datetime, str]]
) -> None:
    """Fail unless the Arrow value AND type state equals the derived rows."""
    _assert_table_schema(table)
    assert _full_rows(table) == _expected_typed(expected)


def _select_all(session: Any) -> pa.Table:
    """The current table contents in id order."""
    return session.sql(
        f"SELECT id, name, amount, ingestion_timestamp, ds FROM {_FQ_TABLE} ORDER BY id"
    ).to_arrow()


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


def _stage_merge_batch(
    session: Any,
    src_dir: Path,
    index: int,
    rows: list[tuple[int, str, Decimal, datetime, str]],
    merge_sql: str = _MERGE_SQL,
) -> None:
    """Stage one parquet batch and run the production MERGE shape against it."""
    src_dir.mkdir(parents=True, exist_ok=True)
    source = src_dir / f"merge{index}.parquet"
    _write_rows_parquet(source, rows)
    _stage_and_merge(session, source, merge_sql)


def _plant_orphan(table_root: Path) -> Path:
    """An unreferenced pre-dated file only remove_orphan_files may sweep."""
    planted = table_root / "data" / "ds=2026-09-13" / "orphan-000.parquet"
    planted.parent.mkdir(parents=True, exist_ok=True)
    planted.write_bytes(b"orphan")
    stale = datetime(2020, 1, 1).timestamp()
    os.utime(planted, (stale, stale))
    return planted


def _maintenance_calls(session: Any, table_root: Path) -> tuple[dict[str, pa.Table], Path]:
    """The weekly CALLs in production order, then the extra CoW no-op CALL."""
    snaps = session.sql(
        f"SELECT committed_at FROM {_FQ_TABLE}.snapshots ORDER BY committed_at"
    ).to_arrow()
    first, second = snaps.column("committed_at").to_pylist()[:2]
    midpoint = first + (second - first) / 2
    orphan_cutoff = datetime.now() - timedelta(hours=48)
    planted = _plant_orphan(table_root)
    statements = (
        (
            "expire_snapshots",
            f"CALL {_CATALOG}.system.expire_snapshots(table => '{_TABLE_ARG}', "
            f"older_than => TIMESTAMP '{midpoint:%Y-%m-%d %H:%M:%S.%f}', "
            "retain_last => 1)",
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
            f"older_than => TIMESTAMP '{orphan_cutoff:%Y-%m-%d %H:%M:%S}', "
            "dry_run => false)",
        ),
        (
            "rewrite_position_delete_files",
            f"CALL {_CATALOG}.system.rewrite_position_delete_files(table => '{_TABLE_ARG}')",
        ),
    )
    outputs: dict[str, pa.Table] = {}
    for name, statement in statements:
        outputs[name] = session.sql(statement).to_arrow()
    return outputs, planted


def _assert_maintenance_outputs(outputs: dict[str, pa.Table], planted: Path) -> None:
    """The measured CALL results on the Spark-created adopted table."""
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
    orphans = outputs["remove_orphan_files"].to_pylist()
    assert orphans == [{"orphan_file_location": str(planted)}], orphans
    assert not planted.exists()
    deletes = outputs["rewrite_position_delete_files"].to_pylist()
    assert deletes == [
        {
            "rewritten_delete_files_count": 0,
            "added_delete_files_count": 0,
            "rewritten_bytes_count": 0,
            "added_bytes_count": 0,
        }
    ], deletes


def _repark_data_files(table_root: Path) -> list[Path]:
    """Data files RePark wrote: uuid-named, unlike Spark's task-prefixed names."""
    files = []
    for path in sorted((table_root / "data").rglob("*.parquet")):
        if not path.name.startswith("00000-"):
            files.append(path)
    return files


def _assert_final_state(session: Any, table_root: Path, adopted_props: dict[str, str]) -> pa.Table:
    """Rows, snapshot log, file naming, hint, properties and codec after writes."""
    answer = _select_all(session)
    _assert_table_rows(answer, _expected_final_rows())
    counts = session.sql(
        f"SELECT ds, COUNT(*) AS c FROM {_FQ_TABLE} GROUP BY ds ORDER BY ds"
    ).to_arrow()
    by_ds = dict(zip(counts.column("ds").to_pylist(), counts.column("c").to_pylist(), strict=True))
    assert by_ds == _EXPECTED_PARTITION_COUNTS, by_ds
    deletes = session.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}.delete_files").to_arrow()
    assert deletes.column("c")[0].as_py() == 0
    snapshots = session.sql(
        f"SELECT operation FROM {_FQ_TABLE}.snapshots ORDER BY committed_at"
    ).to_arrow()
    assert snapshots.column("operation").to_pylist() == _EXPECTED_SNAPSHOT_OPS
    metadata_names = sorted(path.name for path in (table_root / "metadata").glob("*.metadata.json"))
    assert metadata_names == [
        f"v{index}.metadata.json" for index in range(1, _FINAL_METADATA_COUNT + 1)
    ]
    hint = (table_root / "metadata" / "version-hint.text").read_text(encoding="utf-8")
    assert hint.strip() == _ADOPTED_HINT
    final_doc = json.loads(
        (table_root / "metadata" / metadata_names[-1]).read_text(encoding="utf-8")
    )
    assert final_doc["properties"] == adopted_props
    codecs = {
        pq.ParquetFile(path).metadata.row_group(0).column(0).compression
        for path in _repark_data_files(table_root)
    }
    assert codecs == {"ZSTD"}, codecs
    return answer


def _register_spark_table(session: Any, metadata_file: Path) -> None:
    """Adopt the Spark-written metadata file into the memory catalog."""
    session.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => '{_TABLE_ARG}', metadata_file => '{metadata_file}')"
    )


def test_spark_created_fixture_adopted_merged_and_maintained(tmp_path: Path) -> None:
    """The checked-in Spark-written fixture: register, MERGE twice, weekly CALLs."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("ice-spark-table-1-fixture").getOrCreate()
    try:
        spark.register_memory_catalog(_CATALOG, tmp_path / "warehouse")
        spark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        with _materialize():
            adopted_meta = _TABLE_ROOT / "metadata" / _ADOPTED_METADATA
            adopted_props = json.loads(adopted_meta.read_text(encoding="utf-8"))["properties"]
            _register_spark_table(spark, adopted_meta)
            seed = _select_all(spark)
            _assert_table_rows(seed, _truth_seed_rows())
            assert _full_rows(seed) == _expected_typed(_expected_seed_rows())
            _stage_merge_batch(spark, tmp_path / "staging", 1, _merge_source_one())
            _assert_table_rows(_select_all(spark), _expected_merge1_rows())
            _stage_merge_batch(spark, tmp_path / "staging", 2, _merge_source_two())
            _assert_table_rows(_select_all(spark), _expected_final_rows())
            outputs, planted = _maintenance_calls(spark, _TABLE_ROOT)
            _assert_maintenance_outputs(outputs, planted)
            _assert_final_state(spark, _TABLE_ROOT, adopted_props)
    finally:
        spark.stop()


def test_spark_created_and_repark_created_metadata_shapes(tmp_path: Path) -> None:
    """The property, summary, naming and hint deltas against a RePark-created twin."""
    from repark import ReparkSession

    spark_doc = json.loads(
        (_FIXTURE_SRC / "metadata" / _ADOPTED_METADATA).read_text(encoding="utf-8")
    )
    spark = ReparkSession.builder.appName("ice-spark-table-1-twin").getOrCreate()
    try:
        spark.register_memory_catalog(_CATALOG, tmp_path / "twin-warehouse")
        spark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        spark.sql(f"CREATE TABLE {_FQ_TABLE} {_DDL} TBLPROPERTIES ({_TWIN_TBLPROPERTIES})")
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
    assert spark_names == [f"v{index}.metadata.json" for index in range(1, int(_ADOPTED_HINT) + 1)]
    assert (_FIXTURE_SRC / "metadata" / "version-hint.text").read_text(
        encoding="utf-8"
    ).strip() == _ADOPTED_HINT
    spark_props = spark_doc["properties"]
    twin_props = twin_doc["properties"]
    assert twin_props == {
        "write.merge.mode": "copy-on-write",
        "write.delete.mode": "copy-on-write",
        "write.update.mode": "copy-on-write",
        "write.parquet.compression-codec": "zstd",
        "write.target-file-size-bytes": "268435456",
        "write.distribution-mode": "hash",
    }, twin_props
    assert spark_props == {
        **twin_props,
        "owner": spark_props["owner"],
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
    spark.sql(_spark_merge_sql())
    spark.sql(_hash_distribution_sql())
    seeded = spark.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}").toArrow()
    assert seeded.column("c")[0].as_py() == 20
    table_root = warehouse / _NAMESPACE / _TABLE
    spark_meta = _newest_metadata_file(table_root)
    assert spark_meta.name == _ADOPTED_METADATA
    spark_doc = json.loads(spark_meta.read_text(encoding="utf-8"))
    props = spark_doc["properties"]
    for key in _SPARK_ONLY_PROPERTY_KEYS:
        assert props[key], props
    assert props["write.distribution-mode"] == "hash"
    assert [s["summary"]["operation"] for s in spark_doc["snapshots"]] == [
        "append",
        "append",
        "overwrite",
    ]
    assert spark_doc["default-sort-order-id"] == 0
    assert (table_root / "metadata" / "version-hint.text").exists()

    repark = ReparkSession.builder.appName("ice-spark-table-1-live").getOrCreate()
    try:
        repark.register_memory_catalog(_CATALOG, tmp_path / "repark-warehouse")
        repark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
        _register_spark_table(repark, spark_meta)
        _assert_table_rows(_select_all(repark), _expected_seed_rows())
        _stage_merge_batch(repark, tmp_path / "staging", 1, _merge_source_one())
        _assert_table_rows(_select_all(repark), _expected_merge1_rows())
        _stage_merge_batch(repark, tmp_path / "staging", 2, _merge_source_two())
        _assert_table_rows(_select_all(repark), _expected_final_rows())
        outputs, planted = _maintenance_calls(repark, table_root)
        _assert_maintenance_outputs(outputs, planted)
        answer = _assert_final_state(repark, table_root, props)
        answer_path = tmp_path / "repark-answer.parquet"
        pq.write_table(answer, str(answer_path))
        expected_path = tmp_path / "expected-answer.parquet"
        _write_rows_parquet(expected_path, _expected_final_rows())
        _assert_live_extra_legs(repark, spark, warehouse, tmp_path)
    finally:
        repark.stop()

    stale = spark.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}").toArrow()
    assert stale.column("c")[0].as_py() == 20
    spark.sql(
        f"INSERT INTO {_FQ_TABLE} VALUES (99, 'stale-write', "
        "CAST('99.99' AS DECIMAL(12,2)), TIMESTAMP '2026-09-17 00:00:00', '2026-09-17')"
    )
    assert (table_root / "metadata" / "v10.metadata.json").exists()
    spark.catalog.refreshTable(_FQ_TABLE)
    refreshed = spark.sql(f"SELECT COUNT(*) AS c FROM {_FQ_TABLE}").toArrow()
    assert refreshed.column("c")[0].as_py() == 36
    spark.sql(
        f"INSERT INTO {_FQ_TABLE} VALUES (100, 'post-refresh', "
        "CAST('100.00' AS DECIMAL(12,2)), TIMESTAMP '2026-09-17 00:00:00', '2026-09-17')"
    )
    assert (table_root / "metadata" / "v11.metadata.json").exists()
    repark_meta = table_root / "metadata" / "v9.metadata.json"
    spark.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => 'ns.facts_repark', metadata_file => '{repark_meta}')"
    )
    spark.read.parquet(str(answer_path)).createOrReplaceTempView("repark_answer")
    spark.read.parquet(str(expected_path)).createOrReplaceTempView("expected_answer")
    columns = "id, name, amount, ingestion_timestamp, ds"
    readback = f"{_CATALOG}.ns.facts_repark"
    for other in ("repark_answer", "expected_answer"):
        forward = spark.sql(
            f"SELECT {columns} FROM {readback} EXCEPT ALL SELECT {columns} FROM {other}"
        ).toArrow()
        assert forward.num_rows == 0, other
        backward = spark.sql(
            f"SELECT {columns} FROM {other} EXCEPT ALL SELECT {columns} FROM {readback}"
        ).toArrow()
        assert backward.num_rows == 0, other
    counted = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}").toArrow()
    assert counted.column("c")[0].as_py() == 35
    partition_counts = spark.sql(
        f"SELECT ds, COUNT(*) AS c FROM {readback} GROUP BY ds ORDER BY ds"
    ).toArrow()
    by_ds = dict(
        zip(
            partition_counts.column("ds").to_pylist(),
            partition_counts.column("c").to_pylist(),
            strict=True,
        )
    )
    assert by_ds == _EXPECTED_PARTITION_COUNTS, by_ds
    snaps = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}.snapshots").toArrow()
    assert snaps.column("c")[0].as_py() == 5
    history = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}.history").toArrow()
    assert history.column("c")[0].as_py() == 5
    live_files = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}.files").toArrow()
    assert live_files.column("c")[0].as_py() == 4
    deletes = spark.sql(f"SELECT COUNT(*) AS c FROM {readback}.delete_files").toArrow()
    assert deletes.column("c")[0].as_py() == 0


def _assert_live_extra_legs(repark: Any, spark: Any, warehouse: Path, tmp_path: Path) -> None:
    """The snappy-codec and spec-evolution legs against fresh Spark tables."""
    snappy_fq = f"{_CATALOG}.ns.snappy"
    spark.sql(
        f"CREATE TABLE {snappy_fq} {_DDL} TBLPROPERTIES ({_TBLPROPERTIES}, "
        "'write.parquet.compression-codec'='snappy')"
    )
    spark.sql(
        f"INSERT INTO {snappy_fq} VALUES (1, 's1', CAST('1.99' AS DECIMAL(12,2)), "
        "TIMESTAMP '2026-09-13 08:00:00', '2026-09-13')"
    )
    repark.sql(
        f"CALL {_CATALOG}.system.register_table(table => 'ns.snappy', "
        f"metadata_file => '{_newest_metadata_file(warehouse / 'ns' / 'snappy')}')"
    )
    _stage_merge_batch(
        repark,
        tmp_path / "staging-snappy",
        1,
        [(2, "s2", Decimal("2.22"), datetime(2026, 9, 15, 8, 0, 0), "2026-09-15")],
        (
            f"MERGE INTO {snappy_fq} AS Target USING ice1_staging AS Source "
            "ON Target.id = Source.id WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
    )
    repark_files = _repark_data_files(warehouse / "ns" / "snappy")
    assert repark_files, "no repark-written data file on the snappy table"
    for path in repark_files:
        codec = pq.ParquetFile(path).metadata.row_group(0).column(0).compression
        assert codec == "SNAPPY", (path, codec)

    evo_fq = f"{_CATALOG}.ns.evo"
    spark.sql(f"CREATE TABLE {evo_fq} {_DDL} TBLPROPERTIES ({_TBLPROPERTIES})")
    spark.sql(
        f"INSERT INTO {evo_fq} VALUES (1, 'e1', CAST('1.11' AS DECIMAL(12,2)), "
        "TIMESTAMP '2026-09-13 08:00:00', '2026-09-13')"
    )
    spark.sql(f"ALTER TABLE {evo_fq} ADD PARTITION FIELD days(ingestion_timestamp)")
    evo_meta = _newest_metadata_file(warehouse / "ns" / "evo")
    evo_doc = json.loads(evo_meta.read_text(encoding="utf-8"))
    assert evo_doc["default-spec-id"] == 1
    assert len(evo_doc["partition-specs"]) == 2
    repark.sql(
        f"CALL {_CATALOG}.system.register_table(table => 'ns.evo', metadata_file => '{evo_meta}')"
    )
    _stage_merge_batch(
        repark,
        tmp_path / "staging-evo",
        1,
        [(2, "e2", Decimal("2.22"), datetime(2026, 9, 14, 8, 0, 0), "2026-09-14")],
        (
            f"MERGE INTO {evo_fq} AS Target USING ice1_staging AS Source "
            "ON Target.id = Source.id WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ),
    )
    evo_rows = repark.sql(f"SELECT * FROM {evo_fq} ORDER BY id").to_arrow().to_pylist()
    assert [(r["id"], r["name"]) for r in evo_rows] == [(1, "e1"), (2, "e2")]
    evo_files = repark.sql(f"SELECT file_path, spec_id FROM {evo_fq}.files").to_arrow().to_pylist()
    assert sorted(row["spec_id"] for row in evo_files) == [0, 1], evo_files
    spark.catalog.refreshTable(evo_fq)
    evo_count = spark.sql(f"SELECT COUNT(*) AS c FROM {evo_fq}").toArrow()
    assert evo_count.column("c")[0].as_py() == 2


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
        with pytest.raises(Exception, match=re.escape(_TRANSFORM_REFUSAL)):
            _stage_and_merge(
                repark,
                _residual_source(tmp_path),
                f"MERGE INTO {fq_sorted} AS Target USING ice1_staging AS Source "
                "ON Target.id = Source.id WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *",
            )
    finally:
        repark.stop()

    snapshots_after = spark.sql(
        f"SELECT snapshot_id FROM {fq_sorted}.snapshots ORDER BY committed_at"
    ).toArrow()
    assert [int(v) for v in snapshots_after.column("snapshot_id").to_pylist()] == ids_before


def _residual_source(tmp_path: Path) -> Path:
    """A one-row staging parquet for the refusal leg."""
    source = tmp_path / "residual-src" / "src.parquet"
    source.parent.mkdir(parents=True, exist_ok=True)
    _write_rows_parquet(
        source,
        [(4, "d", Decimal("4.00"), datetime(2026, 9, 13, 8, 3, 0), "2026-09-13")],
    )
    return source
