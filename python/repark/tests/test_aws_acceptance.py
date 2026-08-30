"""Env-gated real-AWS acceptance harness — the source publish job's path against Glue.

Mirrors the source publish job: a session configured with a Glue ``catalog-impl`` + an
``s3://`` warehouse, a bronze ``s3a://`` Parquet read, the ``row_number`` dedup transform,
then the ``tableExists`` → CTAS → MERGE publish loop into a **scratch** namespace. The
whole module is gated by a module-level ``pytest.mark.skipif`` on ``REPARK_AWS_ACCEPTANCE``:
it is collected and skipped by default so CI stays AWS-free, and opens a network
connection to AWS only when the gate is explicitly set to ``"1"``. This is the single
sanctioned real-AWS execution (owned by the Fable audit) — do not run it during routine
development. The pure helpers (path/config/SQL builders, the ``deduplicate`` transform)
live in the non-collected ``_acceptance`` module and are unit-tested AWS-free in
``test_acceptance_helpers.py``.

CLEANUP IS THE USER'S MANUAL CALL.
This harness NEVER drops or deletes any Glue table or namespace. It creates (namespace /
table) and upserts into the scratch namespace ``testing_repark_acceptance`` only (tables
carry a ``testing_`` prefix too); it never touches production ``example_silver``. There
is no table-teardown against AWS; if the scratch tables should be removed after a run,
the user does that by hand. ``dropTempView`` drops only a session-local view, never an
AWS object. MW-4 ``CALL expire_snapshots`` / ``rewrite_*`` may remove expired snapshot
*files* under the warehouse scratch prefix (OD-3 scoped object-delete); Glue tables
still accumulate.
"""

from __future__ import annotations

import os
import uuid

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    ACCEPTANCE_TABLE_PREFIX,
    GLUE_WAREHOUSE,
    S3TABLES_CATALOG,
    SILVER_CATALOG,
    TEMP_VIEW,
    acceptance_namespace_location,
    assert_glue_scratch_namespace_location,
    assert_mor_maintenance_outcome,
    assert_real_buckets_configured,
    bronze_path,
    ctas_sql,
    deduplicate,
    format_denial_failure,
    fq_table,
    glue_catalog_config,
    is_storage_delete_denial,
    merge_sql,
    run_mor_merge_compact_expire,
    s3tables_catalog_config,
)

from repark import ReparkSession

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason=(
        "real-AWS acceptance harness: set REPARK_AWS_ACCEPTANCE=1 to run "
        "(owned by the Fable audit — the single sanctioned real-AWS execution)"
    ),
)


def _require_env(name: str) -> str:
    """Return env var ``name`` or fail loudly (only reached when the gate is on)."""
    value = os.environ.get(name)
    if not value:
        pytest.fail(
            f"{name} is required when REPARK_AWS_ACCEPTANCE=1 "
            "(the acceptance harness needs a real bronze entity/ds + id column)."
        )
    return value


def _table_row_count(spark: ReparkSession, table: str) -> int:
    return spark.sql(f"SELECT * FROM {table}").count()


def _bronze_dedup_publish_idempotent(
    spark: ReparkSession, table: str, entity: str, ds: str, id_col: str
) -> None:
    """The catalog-agnostic publish path + oracles (shared by Glue and S3 Tables).

    Bronze (s3a) read → dedup → CTAS-if-fresh / MERGE-if-exists → a second identical MERGE
    that must be idempotent. Only session/namespace setup differs between catalogs (done by
    the caller); the publish semantics are identical.
    """
    bronze = spark.read_parquet(bronze_path(entity, ds))
    raw_count = bronze.count()
    assert raw_count > 0, "bronze read returned zero rows"

    deduped = deduplicate(bronze, id_col=id_col)
    deduped_count = deduped.count()
    assert 0 < deduped_count <= raw_count

    # publish pass 1: CTAS when the table is fresh, MERGE when it already exists
    was_fresh = not spark.catalog.tableExists(table)
    deduped.createOrReplaceTempView(TEMP_VIEW)
    if was_fresh:
        spark.sql(ctas_sql(table, TEMP_VIEW))
    else:
        spark.sql(merge_sql(table, TEMP_VIEW, id_col))
    spark.catalog.clearCache()
    spark.catalog.dropTempView(TEMP_VIEW)

    count_after_first = _table_row_count(spark, table)
    if was_fresh:
        assert count_after_first == deduped_count  # CTAS wrote exactly the deduped set
    else:
        assert count_after_first >= deduped_count  # MERGE upserted into a prior run's table

    # publish pass 2: the identical MERGE must be idempotent
    deduped.createOrReplaceTempView(TEMP_VIEW)
    spark.sql(merge_sql(table, TEMP_VIEW, id_col))
    spark.catalog.clearCache()
    spark.catalog.dropTempView(TEMP_VIEW)

    count_after_second = _table_row_count(spark, table)
    assert count_after_second == count_after_first, "second publish pass was not idempotent"


def test_process_silver_acceptance_against_glue() -> None:
    """Mirror the source publish job end to end against real Glue + S3, into the scratch namespace.

    entity/ds/id-column come from env (``REPARK_ACCEPT_ENTITY`` / ``REPARK_ACCEPT_DS`` /
    ``REPARK_ACCEPT_ID_COL``). Oracles: bronze rows > 0; the published table holds the
    deduped set; a second publish pass is idempotent (row count unchanged).
    """
    assert_real_buckets_configured()
    entity = _require_env("REPARK_ACCEPT_ENTITY")
    ds = _require_env("REPARK_ACCEPT_DS")
    id_col = _require_env("REPARK_ACCEPT_ID_COL")

    builder = ReparkSession.builder.appName("process-silver-acceptance")
    for key, value in glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

    # Scratch namespace only — create if missing, NEVER touch production silver. Programmatic
    # create_namespace (ADV-1) sets the `location`: on real Glue (RequireExplicitLocation) a
    # location-less namespace makes the CTAS below fail loud (N5); SQL `CREATE NAMESPACE …
    # LOCATION` (WG-5) can also set it. Idempotent across runs — an "already exists" error from
    # a prior run is expected. After ensure: fail loud if the adopted Location does not match
    # the intended warehouse path (docs/tier2-aws.md §5 / G-6). Read path is DESCRIBE NAMESPACE
    # (bounded probe).
    try:
        spark.create_namespace(
            SILVER_CATALOG,
            ACCEPTANCE_NAMESPACE,
            location=acceptance_namespace_location(GLUE_WAREHOUSE),
        )
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise
    assert_glue_scratch_namespace_location(spark, GLUE_WAREHOUSE)
    table = fq_table(SILVER_CATALOG, ACCEPTANCE_NAMESPACE, f"{ACCEPTANCE_TABLE_PREFIX}{entity}")
    _bronze_dedup_publish_idempotent(spark, table, entity, ds, id_col)


def test_process_silver_acceptance_against_s3tables() -> None:
    """Mirror the source publish job end to end against real **S3 Tables** + S3, scratch only.

    Additionally gated on the ``TABLE_BUCKET_ARN`` env var (a us-east-2 table-bucket ARN, set
    by the user) — SKIPPED, not failed, when absent, so a Glue-only acceptance run is
    unaffected. The ARN is read from the environment and never logged. entity/ds/id-column
    come from the same ``REPARK_ACCEPT_*`` env.

    S3 Tables difference vs Glue: the table bucket **is** the storage, so the namespace is
    created WITHOUT an explicit ``location``. The publish semantics (bronze → dedup →
    CTAS/MERGE → idempotent) are identical and shared.
    """
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip(
            "S3 Tables acceptance needs TABLE_BUCKET_ARN (a us-east-2 table-bucket ARN); "
            "absent → skip so the Glue bullet is unaffected"
        )
    assert_real_buckets_configured()
    entity = _require_env("REPARK_ACCEPT_ENTITY")
    ds = _require_env("REPARK_ACCEPT_DS")
    id_col = _require_env("REPARK_ACCEPT_ID_COL")

    builder = ReparkSession.builder.appName("process-silver-acceptance-s3tables")
    for key, value in s3tables_catalog_config(S3TABLES_CATALOG, arn).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

    # Scratch namespace only — no `location`: the table bucket is the storage. Idempotent
    # across runs — "already exists" from a prior run is expected.
    # S3 Tables namespaces carry no location by design — nothing to compare — so the Glue-leg
    # location-mismatch guard is intentionally not called here.
    try:
        spark.create_namespace(S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE)
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise
    table = fq_table(S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE, f"{ACCEPTANCE_TABLE_PREFIX}{entity}")
    _bronze_dedup_publish_idempotent(spark, table, entity, ds, id_col)


def test_mor_merge_compact_expire_against_glue() -> None:
    """MW-4: merge-on-read CTAS → MERGE → compact + expire on real Glue + S3.

    A new scratch MOR table each run (never-teardown: tables accumulate). OD-3 scoped
    object-delete on the warehouse scratch prefix is what lets expire remove snapshot files.
    S3 Tables MOR is ``test_mor_merge_compact_expire_against_s3tables`` (MW-10 / OD-3b).
    """
    assert_real_buckets_configured()

    builder = ReparkSession.builder.appName("mw4-mor-acceptance")
    for key, value in glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

    try:
        spark.create_namespace(
            SILVER_CATALOG,
            ACCEPTANCE_NAMESPACE,
            location=acceptance_namespace_location(GLUE_WAREHOUSE),
        )
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise
    assert_glue_scratch_namespace_location(spark, GLUE_WAREHOUSE)

    table_name = f"{ACCEPTANCE_TABLE_PREFIX}mw4_mor_{uuid.uuid4().hex[:12]}"
    outcome = run_mor_merge_compact_expire(spark, SILVER_CATALOG, ACCEPTANCE_NAMESPACE, table_name)
    assert_mor_maintenance_outcome(spark, outcome)


def test_mor_merge_compact_expire_against_s3tables() -> None:
    """MW-10: merge-on-read CTAS → MERGE → compact + expire on real S3 Tables.

    OD-3b: whether ``s3tables:PutTableData`` authorizes ``expire_snapshots`` file removal
    on table storage. A denial fails loud (action, resource, account masked as
    ``<ACCOUNT>``) and stops; the harness never widens IAM.

    pins: mw-10-s3tables-mor/C-001, C-002
    """
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip(
            "S3 Tables acceptance needs TABLE_BUCKET_ARN (a us-east-2 table-bucket ARN); "
            "absent → skip so the Glue bullet is unaffected"
        )
    assert_real_buckets_configured()

    builder = ReparkSession.builder.appName("mw10-mor-acceptance-s3tables")
    for key, value in s3tables_catalog_config(S3TABLES_CATALOG, arn).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

    try:
        spark.create_namespace(S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE)
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise
    table_name = f"{ACCEPTANCE_TABLE_PREFIX}mw10_mor_{uuid.uuid4().hex[:12]}"
    try:
        outcome = run_mor_merge_compact_expire(
            spark, S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE, table_name
        )
        assert_mor_maintenance_outcome(spark, outcome)
    except Exception as error:
        if is_storage_delete_denial(error):
            pytest.fail(format_denial_failure(error))
        raise
