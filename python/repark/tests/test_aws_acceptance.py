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
import tempfile
import uuid
import warnings
from pathlib import Path

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
from _acceptance_v3 import (
    S3T_V3_REFUSED_AT_CREATE,
    V3_ALLOW_CREATE_KEY,
    assert_v3_acceptance_outcome,
    classify_v3_create_outcome,
    format_v3_refusal_record,
    run_v3_acceptance,
)
from _sql_harden_cutover_golden import REPARK
from _sql_harden_cutover_programs import _NAMESPACE, _PROGRAMS
from _sql_harden_cutover_run import (
    GOLD_CREATED_AT_FCT,
    GOLD_CREATED_BEFORE_FCT,
    as_golden,
    data_file_count,
    delete_file_count,
    run_program,
    without_meta,
)

from repark import ReparkSession, Window, functions
from repark.spark import types

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


def test_v3_dv_dml_maintenance_against_glue() -> None:
    """LIVE-v3 Glue leg: v3 MoR CTAS → DV DELETE → MERGE → rewrite → expire → register_table.

    Twin of the MW-4 leg: same scratch namespace, same never-teardown naming, a fresh
    `testing_v3_dv_<uuid>` table each run. Glue implements `register_table`, so the adopt step
    runs on a second session here and is skipped on S3 Tables (registry `S3T-1` / fork R126).

    pins: live-v3-aws-legs/C-001, C-003
    """
    assert_real_buckets_configured()

    builder = ReparkSession.builder.appName("live-v3-glue").config(V3_ALLOW_CREATE_KEY, "true")
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

    table_name = f"{ACCEPTANCE_TABLE_PREFIX}v3_dv_{uuid.uuid4().hex[:12]}"
    outcome = run_v3_acceptance(
        spark,
        SILVER_CATALOG,
        ACCEPTANCE_NAMESPACE,
        table_name,
        adopt_with=spark.newSession,
    )
    assert_v3_acceptance_outcome(outcome)


def test_v3_dv_dml_maintenance_against_s3tables() -> None:
    """LIVE-v3 S3 Tables leg: the same sequence, or a recorded `S3T-V3-1` v3 CREATE refusal.

    Decision table — supported ⇒ the full leg with `exact_commit_counts=False` (the service
    commits on its own); refused at CREATE ⇒ the refusal text is recorded and the leg passes,
    having asserted no table was left behind; any other error ⇒ raised. The adopt step is not
    run: `register_table` on S3 Tables is the dated gap `S3T-1` / fork R126.

    pins: live-v3-aws-legs/C-001, C-003
    """
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip(
            "S3 Tables acceptance needs TABLE_BUCKET_ARN (a us-east-2 table-bucket ARN); "
            "absent → skip so the Glue bullet is unaffected"
        )
    assert_real_buckets_configured()

    builder = ReparkSession.builder.appName("live-v3-s3tables").config(V3_ALLOW_CREATE_KEY, "true")
    for key, value in s3tables_catalog_config(S3TABLES_CATALOG, arn).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

    try:
        spark.create_namespace(S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE)
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise

    table_name = f"{ACCEPTANCE_TABLE_PREFIX}v3_dv_{uuid.uuid4().hex[:12]}"
    table = fq_table(S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE, table_name)
    try:
        outcome = run_v3_acceptance(spark, S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE, table_name)
    except Exception as error:
        if is_storage_delete_denial(error):
            pytest.fail(format_denial_failure(error))
        if classify_v3_create_outcome(error) != S3T_V3_REFUSED_AT_CREATE:
            raise
        assert not spark.catalog.tableExists(table), (
            f"{S3T_V3_REFUSED_AT_CREATE} classified but {table} exists: the refusal was not "
            "at CREATE"
        )
        warnings.warn(format_v3_refusal_record(error), RuntimeWarning, stacklevel=2)
        return
    assert_v3_acceptance_outcome(outcome, exact_commit_counts=False)


def _run_sql_harden_on_catalog(
    spark: ReparkSession, catalog: str
) -> tuple[dict[str, object], dict[str, str]]:
    """Run every S1-S7 program into the scratch namespace and return outcomes plus stems."""
    from _sql_harden_cutover_programs import write_bronze_parquet

    results: dict[str, object] = {}
    stems: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="sqlh1-aws-") as raw:
        warehouse = Path(raw)
        parquet = warehouse / "bronze.parquet"
        write_bronze_parquet(parquet)
        for program in _PROGRAMS:
            short = program.name.replace("-", "")[:12]
            stem = f"{ACCEPTANCE_TABLE_PREFIX}h1{short}{uuid.uuid4().hex[:6]}"
            stems[program.name] = stem
            results[program.name] = without_meta(
                as_golden(
                    run_program(
                        program,
                        spark,
                        warehouse,
                        catalog=catalog,
                        functions=functions,
                        types=types,
                        window=Window,
                        qualified_call=True,
                        parquet=parquet,
                        stem=stem,
                        namespace=ACCEPTANCE_NAMESPACE,
                    )
                )
            )
    return results, stems


def _assert_s6_aws_namespace(spark: ReparkSession, catalog: str, stem: str) -> None:
    for suffix in GOLD_CREATED_BEFORE_FCT + GOLD_CREATED_AT_FCT:
        present = f"{catalog}.{ACCEPTANCE_NAMESPACE}.{stem}_{suffix}"
        leaked = f"{catalog}.{_NAMESPACE}.{stem}_{suffix}"
        assert spark.catalog.tableExists(present), present
        assert not spark.catalog.tableExists(leaked), leaked


def _assert_aws_cutover_core(
    spark: ReparkSession,
    catalog: str,
    got: dict[str, object],
    stems: dict[str, str],
) -> None:
    """Row probes that memory measured OK must match; S6 pins the DATE() refusal."""
    for program in _PROGRAMS:
        name = program.name
        actual = got[name]
        memory = without_meta(REPARK[name])
        assert isinstance(actual, dict)
        if name == "s6-gold-incremental":
            _assert_s6_aws_namespace(spark, catalog, stems[name])
        assert actual["statements"][0][0] == "OK", (name, actual["statements"][0])
        if memory["probes"][0][0] == "OK":
            assert actual["probes"][0] == memory["probes"][0], name
        if program.write_mode == "copy-on-write" and program.runner == "merge":
            assert delete_file_count(actual) == 0, name
            assert data_file_count(actual) == data_file_count(memory), name


def test_sql_harden_cutover_against_glue() -> None:
    """S1-S7 cutover shapes on Glue into testing_repark_acceptance.

    pins: sql-harden-1-cutover-shapes/C-002
    """
    assert_real_buckets_configured()
    builder = ReparkSession.builder.appName("sql-harden-1-glue").config(V3_ALLOW_CREATE_KEY, "true")
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
    got, stems = _run_sql_harden_on_catalog(spark, SILVER_CATALOG)
    _assert_aws_cutover_core(spark, SILVER_CATALOG, got, stems)


def test_sql_harden_cutover_against_s3tables() -> None:
    """S1-S7 cutover shapes on S3 Tables into testing_repark_acceptance.

    pins: sql-harden-1-cutover-shapes/C-002
    """
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip(
            "S3 Tables acceptance needs TABLE_BUCKET_ARN (a us-east-2 table-bucket ARN); "
            "absent → skip so the Glue bullet is unaffected"
        )
    assert_real_buckets_configured()
    builder = ReparkSession.builder.appName("sql-harden-1-s3tables").config(
        V3_ALLOW_CREATE_KEY, "true"
    )
    for key, value in s3tables_catalog_config(S3TABLES_CATALOG, arn).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        spark.create_namespace(S3TABLES_CATALOG, ACCEPTANCE_NAMESPACE)
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise
    got, stems = _run_sql_harden_on_catalog(spark, S3TABLES_CATALOG)
    _assert_aws_cutover_core(spark, S3TABLES_CATALOG, got, stems)
