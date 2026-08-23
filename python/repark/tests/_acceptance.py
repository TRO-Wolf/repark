"""Pure, AWS-free helpers for the real-AWS acceptance harness.

Kept in a non-``test_`` module (pytest does not collect it) so both the gated harness
(``test_aws_acceptance.py``) and its always-run unit tests (``test_acceptance_helpers.py``) share
one definition. Nothing here touches AWS or constructs a session — just constants and pure
builders, plus the ``deduplicate`` transform (which operates on an already-constructed DataFrame).

These mirror the shape of the source publish job.
"""

from __future__ import annotations

import os
import time
from typing import NamedTuple

import pyarrow as pa

from repark import Window
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import PySparkException, UnsupportedOperationException
from repark.spark.dataframe import DataFrame

# ==============================================================================================
# Constants — mirrored from the real source publish job's config block
# ==============================================================================================
# Bronze reads use the s3a scheme; the Glue warehouse uses s3. Both must resolve (WG3).
#
# The bucket + warehouse are RUNTIME-OVERRIDABLE, defaulting to synthetic placeholders. This is
# load-bearing security, not convenience: the committed defaults are `example-*` placeholders the
# maintainer does not own, so a real-AWS run MUST supply the operator's own buckets via
# `REPARK_ACCEPT_BRONZE_BUCKET` / `REPARK_ACCEPT_WAREHOUSE` (repository VARIABLES in
# `aws-acceptance.yml`; docs/tier2-aws.md §4). Without them the credentialed job would issue
# SigV4-signed requests against globally-squattable placeholder names — so the harness fails loud
# (below) when `REPARK_AWS_ACCEPTANCE=1` is set but the buckets are still the placeholders.
BRONZE_BUCKET = os.environ.get("REPARK_ACCEPT_BRONZE_BUCKET", "example-bronze-bucket-v1")
BRONZE_PREFIX = "bronze"

# The real script's config block names the catalog ``glue_alt`` but publishes via ``glue_catalog``
# (the cluster spark-defaults supply that name on Glue/EMR). The harness configures the name it
# actually uses for the publish path.
SILVER_CATALOG = "glue_catalog"
GLUE_WAREHOUSE = os.environ.get("REPARK_ACCEPT_WAREHOUSE", "s3://example-warehouse/")

# The placeholder values the committed defaults carry — a real-AWS run must not use these.
_PLACEHOLDER_BRONZE_BUCKET = "example-bronze-bucket-v1"
_PLACEHOLDER_WAREHOUSE = "s3://example-warehouse/"


def assert_real_buckets_configured() -> None:
    """Fail loud if a real-AWS run still targets the synthetic placeholder buckets.

    Called by the gated harness once ``REPARK_AWS_ACCEPTANCE=1``. A signed request to a
    placeholder bucket discloses the assumed-role ARN + account id to whoever owns that global
    name (and could feed attacker-controlled Parquet into the reader), so this is a hard refusal,
    never a skip.
    """
    unset = []
    if BRONZE_BUCKET == _PLACEHOLDER_BRONZE_BUCKET:
        unset.append("REPARK_ACCEPT_BRONZE_BUCKET")
    if GLUE_WAREHOUSE == _PLACEHOLDER_WAREHOUSE:
        unset.append("REPARK_ACCEPT_WAREHOUSE")
    if unset:
        raise RuntimeError(
            "real-AWS acceptance targets synthetic placeholder buckets; set "
            + " and ".join(unset)
            + " to buckets you own (docs/tier2-aws.md §4). Refusing to issue signed requests "
            "against squattable placeholder names."
        )


# S3 Tables (A2 second bullet). A NON-secret catalog name only; the table-bucket ARN is an
# account-specific value passed at RUNTIME from the `TABLE_BUCKET_ARN` env var — NEVER hardcoded
# here or committed (both repos are public-bound).
S3TABLES_CATALOG = "s3tables_catalog"

# Scratch namespace ONLY. Never the production silver namespace. Both the namespace and every
# table the harness creates carry a `testing_` prefix so they read as disposable at a glance.
ACCEPTANCE_NAMESPACE = "testing_repark_acceptance"
ACCEPTANCE_TABLE_PREFIX = "testing_"
PRODUCTION_NAMESPACE = "example_silver"  # named here solely to assert we never touch it

TEMP_VIEW = "staging_view"

# The real TBLPROPERTIES block: format-version 2, copy-on-write for every write mode, target file
# size. (The script carries a trailing ``-- 256 MiB`` inline comment; dropped here as cosmetic.)
TARGET_FILE_SIZE_BYTES = "268435456"
ICEBERG_TABLE_PROPERTIES = (
    "'format-version' = 2, "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write', "
    f"'write.target-file-size-bytes' = '{TARGET_FILE_SIZE_BYTES}'"
)

# MW-4: merge-on-read sibling of ICEBERG_TABLE_PROPERTIES. The COW block above is the
# publish-job mirror and must not change (LRS). This block is a new table, never a rewrite
# of the existing silver entity.
MOR_ICEBERG_TABLE_PROPERTIES = (
    "'format-version' = 2, "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    f"'write.target-file-size-bytes' = '{TARGET_FILE_SIZE_BYTES}'"
)

# Iceberg FileContent::PositionDeletes. Used to count live delete files via ``table.files``.
POSITION_DELETE_CONTENT = 1
MOR_SEED_ROW_COUNT = 20
MOR_UPDATED_ID_COUNT = 3
MW4_TEMP_VIEW = "mw4_staging_view"
# Compact is a no-op on a single delete file (MW-2). The sequence writes this many MERGEs so
# rewrite_position_delete_files has something to fold.
MOR_MIN_POSITION_DELETE_FILES = 2
# Far-future older_than so expire is driven by retain_last, not file age (same pattern as
# test_maintenance_call.py).
EXPIRE_OLDER_THAN_FUTURE_MS = 86_400_000


# ==============================================================================================
# Pure builders
# ==============================================================================================
def bronze_path(entity: str, ds: str) -> str:
    """The s3a bronze Parquet path for ``entity``/``ds`` (mirrors ``utils.get_bronze_path``)."""
    return f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}/{entity}/{ds}.parquet"


def fq_table(catalog: str, namespace: str, entity: str) -> str:
    """The three-part fully-qualified table name."""
    return f"{catalog}.{namespace}.{entity}"


def acceptance_namespace_location(warehouse: str) -> str:
    """The scratch namespace's warehouse ``location`` (``<warehouse>/<namespace>``).

    A namespace on a Glue (RequireExplicitLocation) catalog must carry a ``location``, or a CTAS
    into it fails loud (no path to write to). SQL ``CREATE NAMESPACE … LOCATION`` (WG-5) or the
    harness creates the namespace programmatically with this path (ADV-1).
    """
    return f"{warehouse.rstrip('/')}/{ACCEPTANCE_NAMESPACE}"


def normalize_location_uri(location: str) -> str:
    """Slash-normalize a warehouse/namespace location for equality comparison.

    Strips trailing slashes only. S3 paths are case-sensitive — no other rewrite.
    """
    return location.rstrip("/")


def location_from_describe_rows(
    rows: list[tuple[str, str | None]],
) -> str | None:
    """Extract the ``Location`` value from ``DESCRIBE NAMESPACE`` ``(info_name, info_value)`` rows.

    Returns ``None`` when the row is absent (a property-less / bare namespace). An empty string
    is treated as present-but-empty and returned as-is so the comparison guard can fail loud.
    """
    for name, value in rows:
        if name == "Location":
            return value
    return None


def assert_namespace_location_matches(*, actual: str | None, expected: str) -> None:
    """Fail loud when an adopted namespace's location does not match the intended path.

    Comparison is exact equality after :func:`normalize_location_uri` (trailing-slash only).
    A missing location (``None``) is the catalog-has-no-location edge and also fails loud —
    never silently steers table writes to a different warehouse (docs/tier2-aws.md §5).

    The error names both values and the operator fix (delete the stale namespace, or change
    the target warehouse). IAM remains defence in depth, not the design.
    """
    expected_norm = normalize_location_uri(expected)
    if actual is None:
        raise RuntimeError(
            "acceptance namespace has no Location (catalog-has-no-location); expected "
            f"{expected_norm!r}. Delete the stale namespace or recreate it with the intended "
            "warehouse location (docs/tier2-aws.md §5)."
        )
    actual_norm = normalize_location_uri(actual)
    if actual_norm != expected_norm:
        raise RuntimeError(
            "acceptance namespace Location mismatch: "
            f"actual={actual_norm!r} expected={expected_norm!r}. "
            "Delete the stale namespace (owner credentials) or change REPARK_ACCEPT_WAREHOUSE "
            "to match (docs/tier2-aws.md §5). Refusing to adopt pre-existing cloud state that "
            "would silently steer table writes to a different warehouse."
        )


def probe_namespace_location_via_describe(
    spark: object,
    catalog: str,
    namespace: str,
) -> str | None:
    """Read namespace Location via SQL ``DESCRIBE NAMESPACE`` (unit-test helper).

    Retired as the sanctioned live-read path: :meth:`spark.catalog.getDatabase` now returns
    a real ``locationUri`` (Y-3). Kept so DESCRIBE-row extraction stays unit-testable without
    a session. ``listDatabases`` still returns ``locationUri=None`` (FA-2).

    ``spark`` is duck-typed (``spark.sql(...).to_arrow()``).
    """
    sql = f"DESCRIBE NAMESPACE {catalog}.{namespace}"
    table = spark.sql(sql).to_arrow()  # type: ignore[attr-defined]
    names = table.column("info_name").to_pylist()
    values = table.column("info_value").to_pylist()
    rows = list(zip(names, values, strict=True))
    return location_from_describe_rows(rows)


def assert_glue_scratch_namespace_location(spark: object, warehouse: str) -> None:
    """After ensure-namespace on the Glue leg: verify Location matches the intended path.

    Glue-only. S3 Tables namespaces carry no location by design — nothing to compare; that leg
    must not call this guard. Reads ``locationUri`` from the public
    ``spark.catalog.getDatabase`` API (Y-3; this was the helper's retirement condition).
    """
    expected = acceptance_namespace_location(warehouse)
    db = spark.catalog.getDatabase(f"{SILVER_CATALOG}.{ACCEPTANCE_NAMESPACE}")  # type: ignore[attr-defined]
    actual = db.locationUri
    assert_namespace_location_matches(actual=actual, expected=expected)


def glue_catalog_config(catalog_name: str, warehouse: str) -> dict[str, str]:
    """The ``spark.sql.catalog.<name>.*`` block for a Glue catalog (source publish job shape).

    Includes ``io-impl`` verbatim from the real script — the WG2 mapping recognises and **drops**
    it (iceberg-rust FileIO is not pluggable by classname), so it is carried for fidelity.
    """
    prefix = f"spark.sql.catalog.{catalog_name}"
    return {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        f"{prefix}.warehouse": warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def s3tables_catalog_config(catalog_name: str, table_bucket_arn: str) -> dict[str, str]:
    """The ``spark.sql.catalog.<name>.*`` block for an **S3 Tables** catalog (publish job shape).

    S3 Tables addresses its virtual bucket by **ARN**, passed as the ``warehouse`` — RePark's
    ``catalog_config`` carries an S3 Tables block's ``warehouse`` into the ``table_bucket_arn`` the
    ``repark-catalog`` builder requires (an explicit ``table_bucket_arn`` would win). ``io-impl`` is
    carried verbatim for fidelity (recognised and dropped, exactly like the Glue block).

    ``table_bucket_arn`` is a RUNTIME argument (from ``TABLE_BUCKET_ARN``) — never a committed
    literal. Region is taken from the caller's AWS environment (the ARN is region-qualified;
    the runbook sets ``AWS_REGION=us-east-2``).
    """
    prefix = f"spark.sql.catalog.{catalog_name}"
    return {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.aws.s3tables.S3TablesCatalog",
        f"{prefix}.warehouse": table_bucket_arn,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def ctas_sql(table: str, source_view: str) -> str:
    """The ``ensure_silver_table_exists`` CTAS statement (CREATE IF NOT EXISTS + TBLPROPERTIES)."""
    return (
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"TBLPROPERTIES ({ICEBERG_TABLE_PROPERTIES}) AS SELECT * FROM {source_view}"
    )


def merge_sql(table: str, source_view: str, id_col: str) -> str:
    """The ``upsert_silver_df`` MERGE statement, keyed on ``id_col`` (UPDATE SET * / INSERT *)."""
    return (
        f"MERGE INTO {table} AS Target USING {source_view} AS Source "
        f"ON Target.{id_col} = Source.{id_col} "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def deduplicate(
    df: DataFrame,
    id_col: str,
    timestamp_col: str = "ingestion_timestamp",
) -> DataFrame:
    """Keep the newest row per ``id_col`` (mirrors the source publish job's dedup step).

    ``row_number()`` over ``partitionBy(id_col).orderBy(timestamp_col DESC)`` → keep ``rn == 1`` →
    drop the helper column.
    """
    window = Window.partitionBy(id_col).orderBy(F.col(timestamp_col).desc())
    return (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


# ==============================================================================================
# MW-4 — merge-on-read compact + expire (Glue live + memory analog share this path)
# ==============================================================================================
class MorMaintenanceOutcome(NamedTuple):
    """Arrow row set and delete-file counts from :func:`run_mor_merge_compact_expire`."""

    table: str
    rows: object
    position_deletes_before: int
    position_deletes_after: int
    first_snapshot_id: int


def mor_ctas_sql(table: str, source_view: str) -> str:
    """CREATE TABLE AS SELECT with merge-on-read write modes. No IF NOT EXISTS: a name
    collision with a leftover scratch table must fail loud, not adopt the leftover."""
    return (
        f"CREATE TABLE {table} USING iceberg "
        f"TBLPROPERTIES ({MOR_ICEBERG_TABLE_PROPERTIES}) AS SELECT * FROM {source_view}"
    )


def mor_seed_select_sql() -> str:
    """Twenty ``(id, name)`` rows as a VALUES select. Small on purpose: the live job is
    operational proof, not a publish-job mirror."""
    value_sql: str = ", ".join(
        f"({index}, 'n{index}')" for index in range(1, MOR_SEED_ROW_COUNT + 1)
    )
    return f"SELECT * FROM (VALUES {value_sql}) AS t(id, name)"


def mor_acceptance_expected_rows() -> list[dict[str, object]]:
    """Post-MERGE oracle: ids 1..3 renamed ``mN``, the rest keep ``nN``."""
    rows: list[dict[str, object]] = []
    for index in range(1, MOR_SEED_ROW_COUNT + 1):
        name = f"m{index}" if index <= MOR_UPDATED_ID_COUNT else f"n{index}"
        rows.append({"id": index, "name": name})
    return rows


def maintenance_call_sql(
    catalog: str,
    procedure: str,
    table_arg: str,
    extra: str = "",
) -> str:
    """``CALL catalog.system.procedure(table => 'ns.tbl'[, extra])``."""
    arguments = f"table => '{table_arg}'"
    if extra:
        arguments = f"{arguments}, {extra}"
    return f"CALL {catalog}.system.{procedure}({arguments})"


def position_delete_file_count(spark: object, table: str) -> int:
    """Live position-delete files (``files.content = 1``) on ``table``."""
    files = spark.sql(f"SELECT content FROM {table}.files").to_arrow()  # type: ignore[attr-defined]
    contents = files.column("content").to_pylist()
    return sum(
        1 for value in contents if value is not None and int(value) == POSITION_DELETE_CONTENT
    )


def snapshot_ids_oldest_first(spark: object, table: str) -> list[int]:
    """Snapshot ids in commit order from the public ``table.snapshots`` metadata table."""
    snaps = spark.sql(  # type: ignore[attr-defined]
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at"
    ).to_arrow()
    return [int(value) for value in snaps.column("snapshot_id").to_pylist() if value is not None]


def drop_temp_view(spark: object, view: str) -> None:
    """Drop a session-local view. Never an AWS object."""
    spark.catalog.dropTempView(view)  # type: ignore[attr-defined]


def merge_named_updates(
    spark: object,
    table: str,
    view: str,
    id_col: str,
    updates: list[tuple[int, str]],
) -> None:
    """One MERGE per ``(id, name)`` so each write strands its own position-delete file."""
    for row_id, name in updates:
        spark.sql(  # type: ignore[attr-defined]
            f"SELECT {row_id} AS {id_col}, '{name}' AS name"
        ).createOrReplaceTempView(view)
        spark.sql(merge_sql(table, view, id_col))  # type: ignore[attr-defined]
        drop_temp_view(spark, view)


def run_mor_merge_compact_expire(
    spark: object,
    catalog: str,
    namespace: str,
    table_name: str,
    id_col: str = "id",
) -> MorMaintenanceOutcome:
    """CTAS merge-on-read → three MERGEs → identical MERGE → compact deletes → expire.

    Shared by the always-run memory analog and the Glue live leg. Does not drop the table.
    """
    table = fq_table(catalog, namespace, table_name)
    table_arg = f"{namespace}.{table_name}"
    view = MW4_TEMP_VIEW

    spark.sql(mor_seed_select_sql()).createOrReplaceTempView(view)  # type: ignore[attr-defined]
    spark.sql(mor_ctas_sql(table, view))  # type: ignore[attr-defined]
    drop_temp_view(spark, view)

    snapshots_after_ctas = snapshot_ids_oldest_first(spark, table)
    first_snapshot_id = snapshots_after_ctas[0]

    updates: list[tuple[int, str]] = [
        (index, f"m{index}") for index in range(1, MOR_UPDATED_ID_COUNT + 1)
    ]
    merge_named_updates(spark, table, view, id_col, updates)
    # Identical replay of the last MERGE: row-set idempotency, not file-count idempotency.
    merge_named_updates(spark, table, view, id_col, [updates[-1]])

    deletes_before = position_delete_file_count(spark, table)
    if deletes_before < MOR_MIN_POSITION_DELETE_FILES:
        raise AssertionError(
            f"MOR MERGE must leave ≥{MOR_MIN_POSITION_DELETE_FILES} position-delete files; "
            f"got {deletes_before} on {table}"
        )

    rows_before = spark.sql(  # type: ignore[attr-defined]
        f"SELECT {id_col}, name FROM {table} ORDER BY {id_col}"
    ).to_arrow()

    spark.sql(  # type: ignore[attr-defined]
        maintenance_call_sql(catalog, "rewrite_position_delete_files", table_arg)
    ).to_arrow()
    deletes_after = position_delete_file_count(spark, table)
    if deletes_after >= deletes_before:
        raise AssertionError(
            f"rewrite_position_delete_files must compact deletes: "
            f"{deletes_before} → {deletes_after} on {table}"
        )

    spark.sql(  # type: ignore[attr-defined]
        maintenance_call_sql(catalog, "rewrite_data_files", table_arg)
    ).to_arrow()

    older_than_ms = int(time.time() * 1000) + EXPIRE_OLDER_THAN_FUTURE_MS
    expire_extra = f"older_than => {older_than_ms}, retain_last => 1"
    spark.sql(  # type: ignore[attr-defined]
        maintenance_call_sql(catalog, "expire_snapshots", table_arg, extra=expire_extra)
    ).to_arrow()

    rows_after = spark.sql(  # type: ignore[attr-defined]
        f"SELECT {id_col}, name FROM {table} ORDER BY {id_col}"
    ).to_arrow()
    if rows_after.to_pylist() != rows_before.to_pylist():
        raise AssertionError(
            f"compact+expire changed the live row set on {table}: "
            f"before={rows_before.to_pylist()!r} after={rows_after.to_pylist()!r}"
        )

    return MorMaintenanceOutcome(
        table=table,
        rows=rows_after,
        position_deletes_before=deletes_before,
        position_deletes_after=deletes_after,
        first_snapshot_id=first_snapshot_id,
    )


def assert_mor_maintenance_outcome(spark: object, outcome: MorMaintenanceOutcome) -> None:
    """Pin compact, Arrow value+type, and expire mutation-proof on a finished outcome."""
    if outcome.position_deletes_before < MOR_MIN_POSITION_DELETE_FILES:
        raise AssertionError(
            f"expected ≥{MOR_MIN_POSITION_DELETE_FILES} position-delete files before compact; "
            f"got {outcome.position_deletes_before}"
        )
    if outcome.position_deletes_after >= outcome.position_deletes_before:
        raise AssertionError(
            f"compact did not reduce delete files: "
            f"{outcome.position_deletes_before} → {outcome.position_deletes_after}"
        )

    rows = outcome.rows
    id_field = rows.schema.field("id")
    name_field = rows.schema.field("name")
    if id_field.type != pa.int64():
        raise AssertionError(f"id type {id_field.type} != int64")
    if name_field.type != pa.string():
        raise AssertionError(f"name type {name_field.type} != string")

    got = [{"id": int(row["id"]), "name": row["name"]} for row in rows.to_pylist()]
    expected = mor_acceptance_expected_rows()
    if got != expected:
        raise AssertionError(f"row set {got!r} != {expected!r}")

    expired = False
    try:
        spark.sql(  # type: ignore[attr-defined]
            f"SELECT id FROM {outcome.table} VERSION AS OF {outcome.first_snapshot_id}"
        ).to_arrow()
    except (UnsupportedOperationException, PySparkException):
        expired = True
    if not expired:
        raise AssertionError(
            f"VERSION AS OF {outcome.first_snapshot_id} still resolved after expire on "
            f"{outcome.table}; expire was a no-op"
        )

    current = spark.sql(  # type: ignore[attr-defined]
        f"SELECT id FROM {outcome.table} ORDER BY id"
    ).to_arrow()
    current_ids = [int(value) for value in current.column("id").to_pylist()]
    expected_ids = list(range(1, MOR_SEED_ROW_COUNT + 1))
    if current_ids != expected_ids:
        raise AssertionError(f"current ids {current_ids!r} != {expected_ids!r}")
