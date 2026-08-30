"""Shared helpers for the real-AWS acceptance harness.

Non-``test_`` module so pytest does not collect it. The publish-job constants, SQL builders, and
``deduplicate`` transform do not construct a session; the maintenance flow drives an
already-built session (memory analog or Glue live) through CTAS / MERGE / CALL.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable
from functools import partial
from typing import NamedTuple

import pyarrow as pa

from repark import Window
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import AnalysisException
from repark.spark.dataframe import DataFrame

# Constants — mirrored from the real source publish job's config block
# Bronze reads use s3a; the Glue warehouse uses s3.
# Bucket + warehouse must be overridden for a real-AWS run (REPARK_ACCEPT_BRONZE_BUCKET /
# REPARK_ACCEPT_WAREHOUSE): the committed defaults are `example-*` placeholders the maintainer
# does not own, and signed requests against squattable names disclose the assumed-role identity —
# hence the fail-loud check below.
BRONZE_BUCKET = os.environ.get("REPARK_ACCEPT_BRONZE_BUCKET", "example-bronze-bucket-v1")
BRONZE_PREFIX = "bronze"

# The real script's config block names the catalog ``glue_alt`` but publishes via ``glue_catalog``;
# the harness configures the name it actually uses for the publish path.
SILVER_CATALOG = "glue_catalog"
GLUE_WAREHOUSE = os.environ.get("REPARK_ACCEPT_WAREHOUSE", "s3://example-warehouse/")

_PLACEHOLDER_BRONZE_BUCKET = "example-bronze-bucket-v1"
_PLACEHOLDER_WAREHOUSE = "s3://example-warehouse/"


def assert_real_buckets_configured() -> None:
    """Fail loud if a real-AWS run still targets the synthetic placeholder buckets.

    Called by the gated harness once ``REPARK_AWS_ACCEPTANCE=1``. A signed request to a squattable
    placeholder name discloses the assumed-role ARN + account id; hard refusal, never a skip.
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


# NON-secret catalog name. The table-bucket ARN is account-specific, passed at RUNTIME from the
# `TABLE_BUCKET_ARN` env var — never hardcoded or committed (both repos are public-bound).
S3TABLES_CATALOG = "s3tables_catalog"

# Scratch namespace ONLY, never production; the `testing_` prefix marks every created name as
# disposable at a glance.
ACCEPTANCE_NAMESPACE = "testing_repark_acceptance"
ACCEPTANCE_TABLE_PREFIX = "testing_"
PRODUCTION_NAMESPACE = "example_silver"  # named here solely to assert we never touch it

TEMP_VIEW = "staging_view"

# The real TBLPROPERTIES block: format-version 2, copy-on-write for every write mode, target
# file size.
TARGET_FILE_SIZE_BYTES = "268435456"
ICEBERG_TABLE_PROPERTIES = (
    "'format-version' = 2, "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write', "
    f"'write.target-file-size-bytes' = '{TARGET_FILE_SIZE_BYTES}'"
)

# Merge-on-read sibling of ICEBERG_TABLE_PROPERTIES: a new scratch table, never a rewrite of the
# existing silver entity.
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
# Five distinct MERGE commits (plus the idempotent replay of the last) so the live position-delete
# group meets MOR_MIN_POSITION_DELETE_FILES.
MOR_UPDATED_ID_COUNT = 5
MW4_TEMP_VIEW = "mw4_staging_view"
# Compact is a no-op below Spark's min-input-files floor of 5; the MERGEs above must exceed it.
# pins: rp-1-fork-repin/C-008
MOR_MIN_POSITION_DELETE_FILES = 5
# Far-future older_than: expire is driven by retain_last, not file age.
EXPIRE_OLDER_THAN_FUTURE_MS = 86_400_000
COMMIT_CONFLICT_RETRY_ATTEMPTS = 3
_ACCOUNT_ID = re.compile(r"\b\d{12}\b")
_DENIAL_ACTION = re.compile(r"perform:\s*(\S+)", re.IGNORECASE)
_DENIAL_RESOURCE = re.compile(r"resource:\s*(\S+)", re.IGNORECASE)


# Pure builders
def bronze_path(entity: str, ds: str) -> str:
    """The s3a bronze Parquet path for ``entity``/``ds`` (mirrors ``utils.get_bronze_path``)."""
    return f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}/{entity}/{ds}.parquet"


def fq_table(catalog: str, namespace: str, entity: str) -> str:
    """The three-part fully-qualified table name."""
    return f"{catalog}.{namespace}.{entity}"


def acceptance_namespace_location(warehouse: str) -> str:
    """The scratch namespace's warehouse ``location`` (``<warehouse>/<namespace>``).

    A Glue (RequireExplicitLocation) namespace must carry a ``location``, or a CTAS into it fails
    loud (no path to write to).
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

    Returns ``None`` when absent. An empty string is returned as-is so the comparison guard fails
    loud.
    """
    for name, value in rows:
        if name == "Location":
            return value
    return None


def assert_namespace_location_matches(*, actual: str | None, expected: str) -> None:
    """Fail loud when an adopted namespace's location does not match the intended path.

    Exact equality after :func:`normalize_location_uri`; a missing location also fails loud.
    Adoption must never silently steer table writes to a different warehouse. The error names both
    values and the operator fix.
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

    Kept so DESCRIBE-row extraction stays unit-testable without a session. ``spark`` is
    duck-typed (``spark.sql(...).to_arrow()``).
    """
    sql = f"DESCRIBE NAMESPACE {catalog}.{namespace}"
    table = spark.sql(sql).to_arrow()  # type: ignore[attr-defined]
    names = table.column("info_name").to_pylist()
    values = table.column("info_value").to_pylist()
    rows = list(zip(names, values, strict=True))
    return location_from_describe_rows(rows)


def assert_glue_scratch_namespace_location(spark: object, warehouse: str) -> None:
    """After ensure-namespace on the Glue leg: verify Location matches the intended path.

    Glue-only: S3 Tables namespaces carry no location by design, so that leg must not call this
    guard.
    """
    expected = acceptance_namespace_location(warehouse)
    db = spark.catalog.getDatabase(f"{SILVER_CATALOG}.{ACCEPTANCE_NAMESPACE}")  # type: ignore[attr-defined]
    actual = db.locationUri
    assert_namespace_location_matches(actual=actual, expected=expected)


def glue_catalog_config(catalog_name: str, warehouse: str) -> dict[str, str]:
    """The ``spark.sql.catalog.<name>.*`` block for a Glue catalog (source publish job shape).

    ``io-impl`` is carried verbatim for fidelity; the mapping recognises and **drops** it
    (iceberg-rust FileIO is not pluggable by classname).
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

    S3 Tables addresses its virtual bucket by **ARN**, passed as the ``warehouse``;
    ``catalog_config`` maps it into the ``table_bucket_arn`` the ``repark-catalog`` builder
    requires. ``io-impl`` is carried verbatim for fidelity (recognised and dropped).
    ``table_bucket_arn`` comes from ``TABLE_BUCKET_ARN`` at RUNTIME — never a committed literal.
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
    """Keep the newest row per ``id_col`` (mirrors the source publish job's dedup step)."""
    window = Window.partitionBy(id_col).orderBy(F.col(timestamp_col).desc())
    return (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


# Merge-on-read compact + expire (shared by the Glue live leg and the memory analog)
class MorMaintenanceOutcome(NamedTuple):
    """Arrow row set, delete-file counts, and C-003/C-004 records from the MOR helper."""

    table: str
    rows: object
    position_deletes_before: int
    position_deletes_after: int
    first_snapshot_id: int
    retry_count: int
    service_commits: int
    snapshot_log_before_expire: list[tuple[int, str]]
    snapshot_log_after_expire: list[tuple[int, str]]
    current_snapshot_matches_engine: bool


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
    """Post-MERGE oracle: ids 1..MOR_UPDATED_ID_COUNT renamed ``mN``, the rest keep ``nN``."""
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


def snapshot_log_oldest_first(spark: object, table: str) -> list[tuple[int, str]]:
    """``(snapshot_id, operation)`` pairs in commit order from ``table.snapshots``."""
    snaps = spark.sql(  # type: ignore[attr-defined]
        f"SELECT snapshot_id, operation FROM {table}.snapshots ORDER BY committed_at"
    ).to_arrow()
    snapshot_ids = snaps.column("snapshot_id").to_pylist()
    operations = snaps.column("operation").to_pylist()
    log: list[tuple[int, str]] = []
    for snapshot_id, operation in zip(snapshot_ids, operations, strict=True):
        if snapshot_id is None:
            continue
        log.append((int(snapshot_id), str(operation)))
    return log


def current_snapshot_id(spark: object, table: str) -> int:
    """Newest snapshot id on ``table`` (commit order)."""
    snapshot_ids = snapshot_ids_oldest_first(spark, table)
    if not snapshot_ids:
        raise AssertionError(f"no snapshots on {table}")
    return snapshot_ids[-1]


def note_engine_snapshot(spark: object, table: str, engine_snapshot_ids: set[int]) -> int:
    """Record the current snapshot as one this run's engine steps wrote."""
    snapshot_id = current_snapshot_id(spark, table)
    engine_snapshot_ids.add(snapshot_id)
    return snapshot_id


def is_commit_conflict_error(error: BaseException) -> bool:
    """True when ``error`` text matches a retryable commit-conflict signature."""
    text = str(error)
    if "CatalogCommitConflicts" in text:
        return True
    if "validate_data_files_exist" in text:
        return True
    lowered = text.lower()
    return "commitfailed" in lowered and "requirement" in lowered


def retry_on_commit_conflict[Result](
    operation: Callable[[], Result],
    attempts: int = COMMIT_CONFLICT_RETRY_ATTEMPTS,
) -> tuple[Result, int]:
    """Call ``operation`` up to ``attempts`` times, retrying commit conflicts only.

    Returns the result and the number of retries consumed. A non-conflict error is
    re-raised on the first call. An exhausted budget raises with the attempt count
    and the last error (a failure, never a skip).
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            return operation(), attempt
        except Exception as error:
            if not is_commit_conflict_error(error):
                raise
            last_error = error
    raise RuntimeError(
        f"commit conflict retry exhausted after {attempts} attempts: {last_error}"
    ) from last_error


def mask_account_ids(text: str) -> str:
    """Replace 12-digit AWS account ids with ``<ACCOUNT>``."""
    return _ACCOUNT_ID.sub("<ACCOUNT>", text)


def is_storage_delete_denial(error: BaseException) -> bool:
    """True when ``error`` looks like an IAM / object-API denial, not a commit conflict."""
    if is_commit_conflict_error(error):
        return False
    lowered = str(error).lower()
    return (
        "accessdenied" in lowered
        or "access denied" in lowered
        or "not authorized" in lowered
        or "explicit deny" in lowered
    )


def format_denial_failure(error: BaseException) -> str:
    """Loud-fail text for a table-storage delete denial, with account ids masked."""
    masked = mask_account_ids(str(error))
    action_match = _DENIAL_ACTION.search(masked)
    resource_match = _DENIAL_RESOURCE.search(masked)
    action = action_match.group(1) if action_match else "<unknown>"
    resource = resource_match.group(1) if resource_match else "<unknown>"
    return (
        f"S3 Tables table-storage delete denied: action={action!r} "
        f"resource={resource!r} message={masked}"
    )


def _sql(spark: object, statement: str) -> object:
    """Run ``spark.sql(statement)`` (MERGE is eager; no Arrow collect)."""
    return spark.sql(statement)  # type: ignore[attr-defined]


def _sql_arrow(spark: object, statement: str) -> object:
    """Run ``spark.sql(statement).to_arrow()`` (CALL procedures need the action)."""
    return spark.sql(statement).to_arrow()  # type: ignore[attr-defined]


def drop_temp_view(spark: object, view: str) -> None:
    """Drop a session-local view. Never an AWS object."""
    spark.catalog.dropTempView(view)  # type: ignore[attr-defined]


def merge_named_updates(
    spark: object,
    table: str,
    view: str,
    id_col: str,
    updates: list[tuple[int, str]],
    engine_snapshot_ids: set[int] | None = None,
    attempts: int = COMMIT_CONFLICT_RETRY_ATTEMPTS,
) -> int:
    """One MERGE per ``(id, name)`` so each write strands its own position-delete file.

    Each MERGE is retried on a commit conflict. Returns the retries consumed.
    """
    retry_total = 0
    owned_ids = engine_snapshot_ids if engine_snapshot_ids is not None else set()
    for row_id, name in updates:
        spark.sql(  # type: ignore[attr-defined]
            f"SELECT {row_id} AS {id_col}, '{name}' AS name"
        ).createOrReplaceTempView(view)
        _, retries = retry_on_commit_conflict(
            partial(_sql, spark, merge_sql(table, view, id_col)),
            attempts=attempts,
        )
        retry_total += retries
        note_engine_snapshot(spark, table, owned_ids)
        drop_temp_view(spark, view)
    return retry_total


def run_mor_merge_compact_expire(
    spark: object,
    catalog: str,
    namespace: str,
    table_name: str,
    id_col: str = "id",
    attempts: int = COMMIT_CONFLICT_RETRY_ATTEMPTS,
) -> MorMaintenanceOutcome:
    """CTAS merge-on-read → five MERGEs → identical MERGE → compact deletes → expire.

    Shared by the always-run memory analog and the Glue / S3 Tables live legs. Does not drop
    the table. MERGE and each maintenance CALL retry commit conflicts up to ``attempts``.
    """
    table = fq_table(catalog, namespace, table_name)
    table_arg = f"{namespace}.{table_name}"
    view = MW4_TEMP_VIEW
    engine_snapshot_ids: set[int] = set()
    retry_count = 0

    spark.sql(mor_seed_select_sql()).createOrReplaceTempView(view)  # type: ignore[attr-defined]
    spark.sql(mor_ctas_sql(table, view))  # type: ignore[attr-defined]
    drop_temp_view(spark, view)
    note_engine_snapshot(spark, table, engine_snapshot_ids)

    snapshots_after_ctas = snapshot_ids_oldest_first(spark, table)
    first_snapshot_id = snapshots_after_ctas[0]
    require_snapshot_readable(spark, table, first_snapshot_id, id_col)

    updates: list[tuple[int, str]] = [
        (index, f"m{index}") for index in range(1, MOR_UPDATED_ID_COUNT + 1)
    ]
    retry_count += merge_named_updates(
        spark, table, view, id_col, updates, engine_snapshot_ids, attempts
    )
    rows_after_updates = ordered_id_name_rows(spark, table, id_col)
    retry_count += merge_named_updates(
        spark, table, view, id_col, [updates[-1]], engine_snapshot_ids, attempts
    )
    rows_after_replay = ordered_id_name_rows(spark, table, id_col)
    if rows_after_replay != rows_after_updates:
        raise AssertionError(
            f"identical MERGE changed the live row set on {table}: "
            f"before={rows_after_updates!r} after={rows_after_replay!r}"
        )

    deletes_before = position_delete_file_count(spark, table)
    if deletes_before < MOR_MIN_POSITION_DELETE_FILES:
        raise AssertionError(
            f"MOR MERGE must leave ≥{MOR_MIN_POSITION_DELETE_FILES} position-delete files; "
            f"got {deletes_before} on {table}"
        )

    rows_before = spark.sql(  # type: ignore[attr-defined]
        f"SELECT {id_col}, name FROM {table} ORDER BY {id_col}"
    ).to_arrow()

    rewrite_deletes = maintenance_call_sql(catalog, "rewrite_position_delete_files", table_arg)
    _, retries = retry_on_commit_conflict(
        partial(_sql_arrow, spark, rewrite_deletes), attempts=attempts
    )
    retry_count += retries
    note_engine_snapshot(spark, table, engine_snapshot_ids)
    deletes_after = position_delete_file_count(spark, table)
    if deletes_after >= deletes_before:
        raise AssertionError(
            f"rewrite_position_delete_files must compact deletes: "
            f"{deletes_before} → {deletes_after} on {table}"
        )

    rewrite_data = maintenance_call_sql(catalog, "rewrite_data_files", table_arg)
    _, retries = retry_on_commit_conflict(
        partial(_sql_arrow, spark, rewrite_data), attempts=attempts
    )
    retry_count += retries
    note_engine_snapshot(spark, table, engine_snapshot_ids)

    snapshot_log_before_expire = snapshot_log_oldest_first(spark, table)
    older_than_ms = int(time.time() * 1000) + EXPIRE_OLDER_THAN_FUTURE_MS
    expire_extra = f"older_than => {older_than_ms}, retain_last => 1"
    expire_sql = maintenance_call_sql(catalog, "expire_snapshots", table_arg, extra=expire_extra)
    _, retries = retry_on_commit_conflict(partial(_sql_arrow, spark, expire_sql), attempts=attempts)
    retry_count += retries
    expected_current = note_engine_snapshot(spark, table, engine_snapshot_ids)
    snapshot_log_after_expire = snapshot_log_oldest_first(spark, table)
    actual_current = current_snapshot_id(spark, table)
    current_snapshot_matches_engine = actual_current == expected_current

    rows_after = spark.sql(  # type: ignore[attr-defined]
        f"SELECT {id_col}, name FROM {table} ORDER BY {id_col}"
    ).to_arrow()
    if rows_after.to_pylist() != rows_before.to_pylist():
        raise AssertionError(
            f"compact+expire changed the live row set on {table}: "
            f"before={rows_before.to_pylist()!r} after={rows_after.to_pylist()!r}"
        )

    final_ids = set(snapshot_ids_oldest_first(spark, table))
    service_commits = len(final_ids - engine_snapshot_ids)

    return MorMaintenanceOutcome(
        table=table,
        rows=rows_after,
        position_deletes_before=deletes_before,
        position_deletes_after=deletes_after,
        first_snapshot_id=first_snapshot_id,
        retry_count=retry_count,
        service_commits=service_commits,
        snapshot_log_before_expire=snapshot_log_before_expire,
        snapshot_log_after_expire=snapshot_log_after_expire,
        current_snapshot_matches_engine=current_snapshot_matches_engine,
    )


def ordered_id_name_rows(spark: object, table: str, id_col: str) -> list[dict[str, object]]:
    """Live ``(id, name)`` rows in id order as Python dicts."""
    arrow = spark.sql(  # type: ignore[attr-defined]
        f"SELECT {id_col}, name FROM {table} ORDER BY {id_col}"
    ).to_arrow()
    return [{"id": int(row[id_col]), "name": row["name"]} for row in arrow.to_pylist()]


def require_snapshot_readable(
    spark: object,
    table: str,
    snapshot_id: int,
    id_col: str,
    expected_rows: int = MOR_SEED_ROW_COUNT,
) -> None:
    """Fail if ``VERSION AS OF snapshot_id`` does not return ``expected_rows``.

    Dual probe with :func:`require_snapshot_expired`: the snapshot must be readable before expire.
    """
    arrow = spark.sql(  # type: ignore[attr-defined]
        f"SELECT {id_col} FROM {table} VERSION AS OF {snapshot_id}"
    ).to_arrow()
    if arrow.num_rows != expected_rows:
        raise AssertionError(
            f"VERSION AS OF {snapshot_id} returned {arrow.num_rows} rows; "
            f"expected {expected_rows} on {table}"
        )


def require_snapshot_expired(spark: object, table: str, snapshot_id: int) -> None:
    """Fail unless ``VERSION AS OF`` is the unknown-snapshot analysis error.

    Needle is the engine string in ``time_travel.rs`` (``unknown Iceberg snapshot id {id}: not
    found in table metadata``); a generic ``AnalysisException`` is not expire proof.
    """
    try:
        spark.sql(  # type: ignore[attr-defined]
            f"SELECT id FROM {table} VERSION AS OF {snapshot_id}"
        ).to_arrow()
    except AnalysisException as error:
        needle = f"unknown Iceberg snapshot id {snapshot_id}: not found in table metadata"
        if needle in str(error):
            return
        raise
    raise AssertionError(
        f"VERSION AS OF {snapshot_id} still resolved after expire on {table}; expire was a no-op"
    )


def assert_snapshot_log_shape(log: object, label: str) -> None:
    """Fail unless ``log`` is a list of ``(snapshot_id: int, operation: str)`` pairs."""
    if not isinstance(log, list):
        raise AssertionError(f"{label} is not a list: {type(log)!r}")
    for index, entry in enumerate(log):
        if not (isinstance(entry, tuple) and len(entry) == 2):
            raise AssertionError(
                f"{label}[{index}] is not a (snapshot_id, operation) pair: {entry!r}"
            )
        snapshot_id, operation = entry
        if not isinstance(snapshot_id, int):
            raise AssertionError(f"{label}[{index}] snapshot_id is not int: {snapshot_id!r}")
        if not isinstance(operation, str):
            raise AssertionError(f"{label}[{index}] operation is not str: {operation!r}")


def assert_mor_maintenance_outcome(
    spark: object,
    outcome: MorMaintenanceOutcome,
    attempts: int = COMMIT_CONFLICT_RETRY_ATTEMPTS,
) -> None:
    """Pin compact, Arrow value+type, expire mutation-proof, and C-003/C-004 records."""
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

    require_snapshot_expired(spark, outcome.table, outcome.first_snapshot_id)

    current = spark.sql(  # type: ignore[attr-defined]
        f"SELECT id FROM {outcome.table} ORDER BY id"
    ).to_arrow()
    current_ids = [int(value) for value in current.column("id").to_pylist()]
    expected_ids = list(range(1, MOR_SEED_ROW_COUNT + 1))
    if current_ids != expected_ids:
        raise AssertionError(f"current ids {current_ids!r} != {expected_ids!r}")

    if not isinstance(outcome.retry_count, int) or outcome.retry_count < 0:
        raise AssertionError(f"retry_count must be a non-negative int: {outcome.retry_count!r}")
    if outcome.retry_count > attempts:
        raise AssertionError(f"retry_count {outcome.retry_count} exceeds attempts {attempts}")
    if not isinstance(outcome.service_commits, int) or outcome.service_commits < 0:
        raise AssertionError(
            f"service_commits must be a non-negative int: {outcome.service_commits!r}"
        )
    assert_snapshot_log_shape(outcome.snapshot_log_before_expire, "snapshot_log_before_expire")
    assert_snapshot_log_shape(outcome.snapshot_log_after_expire, "snapshot_log_after_expire")
    if not outcome.snapshot_log_before_expire:
        raise AssertionError("snapshot_log_before_expire is empty")
    if not isinstance(outcome.current_snapshot_matches_engine, bool):
        raise AssertionError(
            "current_snapshot_matches_engine must be bool: "
            f"{outcome.current_snapshot_matches_engine!r}"
        )
