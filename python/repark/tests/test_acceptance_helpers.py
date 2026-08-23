"""AWS-free unit tests for the acceptance-harness helpers — these run EVERYWHERE (no gate).

They cover the publish-job builders + ``deduplicate`` in ``_acceptance``, the MW-4
merge-on-read compact+expire helper (memory analog), and assert the gated harness plus
``_acceptance.py`` carry no DROP/DELETE SQL against AWS. The gated real-AWS test itself lives
in ``test_aws_acceptance.py`` behind the ``REPARK_AWS_ACCEPTANCE`` gate.
"""

from __future__ import annotations

import pathlib

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    ACCEPTANCE_TABLE_PREFIX,
    GLUE_WAREHOUSE,
    ICEBERG_TABLE_PROPERTIES,
    MOR_ICEBERG_TABLE_PROPERTIES,
    MOR_SEED_ROW_COUNT,
    PRODUCTION_NAMESPACE,
    TARGET_FILE_SIZE_BYTES,
    acceptance_namespace_location,
    assert_mor_maintenance_outcome,
    assert_namespace_location_matches,
    bronze_path,
    ctas_sql,
    deduplicate,
    fq_table,
    glue_catalog_config,
    location_from_describe_rows,
    maintenance_call_sql,
    merge_sql,
    mor_acceptance_expected_rows,
    mor_ctas_sql,
    normalize_location_uri,
    run_mor_merge_compact_expire,
    s3tables_catalog_config,
)

from repark import ReparkSession

_TESTS_DIR = pathlib.Path(__file__).resolve().parent


def test_bronze_path_is_the_s3a_parquet_path() -> None:
    assert (
        bronze_path("entity_a", "2026-07-01")
        == "s3a://example-bronze-bucket-v1/bronze/entity_a/2026-07-01.parquet"
    )


def test_bronze_path_uses_the_s3a_scheme_not_s3() -> None:
    # The script reads bronze via s3a (Hadoop FS), distinct from the s3 warehouse scheme.
    assert bronze_path("entity_b", "2026-01-01").startswith("s3a://")


def test_fq_table_is_three_part() -> None:
    assert fq_table("glue_catalog", "testing_repark_acceptance", "entity_a") == (
        "glue_catalog.testing_repark_acceptance.entity_a"
    )


def test_acceptance_namespace_location_is_under_the_glue_warehouse() -> None:
    # ADV-1: the harness creates the scratch namespace WITH this `location` (SQL `LOCATION`
    # also works since WG-5), so
    # a CTAS on the RequireExplicitLocation Glue catalog does not hit N5. The warehouse's trailing
    # slash must not double up.
    location = acceptance_namespace_location(GLUE_WAREHOUSE)
    assert location == "s3://example-warehouse/testing_repark_acceptance"
    assert location.startswith("s3://")
    assert "//testing_" not in location  # trailing-slash warehouse must not double the separator


def test_acceptance_namespace_location_appends_when_warehouse_has_no_trailing_slash() -> None:
    assert (
        acceptance_namespace_location("s3://bucket/warehouse")
        == "s3://bucket/warehouse/testing_repark_acceptance"
    )


def test_glue_catalog_config_matches_the_measured_block() -> None:
    cfg = glue_catalog_config("glue_catalog", GLUE_WAREHOUSE)
    assert cfg == {
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": ("org.apache.iceberg.aws.glue.GlueCatalog"),
        "spark.sql.catalog.glue_catalog.warehouse": "s3://example-warehouse/",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def test_glue_catalog_config_carries_the_glue_impl_and_s3_warehouse() -> None:
    cfg = glue_catalog_config("glue_catalog", GLUE_WAREHOUSE)
    assert cfg["spark.sql.catalog.glue_catalog.catalog-impl"].endswith("GlueCatalog")
    assert cfg["spark.sql.catalog.glue_catalog.warehouse"].startswith("s3://")


def test_s3tables_catalog_config_carries_the_arn_as_warehouse() -> None:
    # AWS-free: a DUMMY ARN (never a real one) proves the config shape. The S3 Tables catalog-impl
    # is used and the ARN is passed as `warehouse` (RePark carries it into `table_bucket_arn`).
    dummy_arn = "arn:aws:s3tables:us-east-2:000000000000:bucket/dummy-acceptance-bucket"
    cfg = s3tables_catalog_config("s3tables_catalog", dummy_arn)
    assert cfg == {
        "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables_catalog.catalog-impl": (
            "org.apache.iceberg.aws.s3tables.S3TablesCatalog"
        ),
        "spark.sql.catalog.s3tables_catalog.warehouse": dummy_arn,
        "spark.sql.catalog.s3tables_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def test_iceberg_table_properties_carry_the_real_block() -> None:
    # format-version 2, copy-on-write for every write mode, and a target file size.
    assert "'format-version' = 2" in ICEBERG_TABLE_PROPERTIES
    assert ICEBERG_TABLE_PROPERTIES.count("'copy-on-write'") == 3
    assert "'write.delete.mode' = 'copy-on-write'" in ICEBERG_TABLE_PROPERTIES
    assert "'write.update.mode' = 'copy-on-write'" in ICEBERG_TABLE_PROPERTIES
    assert "'write.merge.mode' = 'copy-on-write'" in ICEBERG_TABLE_PROPERTIES
    assert f"'write.target-file-size-bytes' = '{TARGET_FILE_SIZE_BYTES}'" in (
        ICEBERG_TABLE_PROPERTIES
    )


def test_ctas_sql_shape() -> None:
    table = "glue_catalog.testing_repark_acceptance.testing_entity_a"
    sql = ctas_sql(table, "staging_view")
    assert sql.startswith(f"CREATE TABLE IF NOT EXISTS {table}")
    assert "USING iceberg" in sql
    assert "AS SELECT * FROM staging_view" in sql
    assert "'format-version' = 2" in sql


def test_merge_sql_shape_keys_on_the_id_column() -> None:
    table = "glue_catalog.testing_repark_acceptance.testing_entity_a"
    sql = merge_sql(table, "staging_view", "entity_a_id")
    assert sql.startswith(f"MERGE INTO {table} AS Target")
    assert "ON Target.entity_a_id = Source.entity_a_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_scratch_namespace_is_never_the_production_namespace() -> None:
    # A guard against a copy-paste regression aiming the harness at production silver. Namespace
    # AND created tables carry the testing_ prefix so they read as disposable in the Glue console.
    assert ACCEPTANCE_NAMESPACE == "testing_repark_acceptance"
    assert ACCEPTANCE_NAMESPACE != PRODUCTION_NAMESPACE
    assert ACCEPTANCE_TABLE_PREFIX == "testing_"


def test_the_gated_harness_has_no_drop_or_delete_against_aws() -> None:
    # Structural guard: the harness must NEVER emit DROP TABLE / DELETE FROM / DROP NAMESPACE.
    # CALL expire_snapshots / rewrite_* may remove expired snapshot *files* under the scratch
    # prefix (OD-3); that is not table teardown. Tables still accumulate.
    # MW-4 live SQL lives in `_acceptance.py` as well as the gated module (octo C1-Q-002).
    for filename in ("test_aws_acceptance.py", "_acceptance.py"):
        source = (_TESTS_DIR / filename).read_text(encoding="utf-8")
        upper = source.upper()
        assert "DROP TABLE" not in upper, filename
        assert "DELETE FROM" not in upper, filename
        assert "DROP NAMESPACE" not in upper, filename
        assert "DROP_TABLE" not in upper, filename


def test_deduplicate_keeps_the_newest_row_per_id() -> None:
    # AWS-free: a memory session proves the dedup transform keeps the latest ingestion_timestamp.
    spark = ReparkSession.builder.appName("pytest-dedup").getOrCreate()
    df = spark.sql(
        "SELECT 1 AS id, 'old' AS name, TIMESTAMP '2020-01-01 00:00:00' AS ingestion_timestamp "
        "UNION ALL SELECT 1, 'new', TIMESTAMP '2020-01-02 00:00:00' "
        "UNION ALL SELECT 2, 'only', TIMESTAMP '2020-01-01 00:00:00'"
    )
    out = deduplicate(df, id_col="id")
    got = {r["id"]: r["name"] for r in out.to_arrow().to_pylist()}
    assert got == {1: "new", 2: "only"}


def test_placeholder_buckets_refuse_a_real_aws_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The committed defaults are placeholders; `assert_real_buckets_configured` must refuse them
    by name so a credentialed job never issues signed requests against squattable global names."""
    import _acceptance

    monkeypatch.setattr(_acceptance, "BRONZE_BUCKET", _acceptance._PLACEHOLDER_BRONZE_BUCKET)
    monkeypatch.setattr(_acceptance, "GLUE_WAREHOUSE", _acceptance._PLACEHOLDER_WAREHOUSE)
    with pytest.raises(RuntimeError) as excinfo:
        _acceptance.assert_real_buckets_configured()
    message = str(excinfo.value)
    assert "REPARK_ACCEPT_BRONZE_BUCKET" in message
    assert "REPARK_ACCEPT_WAREHOUSE" in message


def test_operator_buckets_pass_the_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real operator-supplied buckets (non-placeholder) satisfy the guard."""
    import _acceptance

    monkeypatch.setattr(_acceptance, "BRONZE_BUCKET", "acme-bronze-real")
    monkeypatch.setattr(_acceptance, "GLUE_WAREHOUSE", "s3://acme-warehouse-real/")
    _acceptance.assert_real_buckets_configured()  # must not raise


# ==============================================================================================
# Namespace location-mismatch guard (G-6) — pure comparison, no AWS
# ==============================================================================================
def test_normalize_location_uri_strips_trailing_slashes_only() -> None:
    assert normalize_location_uri("s3://bucket/ns/") == "s3://bucket/ns"
    assert normalize_location_uri("s3://bucket/ns///") == "s3://bucket/ns"
    # S3 paths are case-sensitive — no rewrite beyond trailing slashes.
    assert normalize_location_uri("s3://Bucket/NS") == "s3://Bucket/NS"


def test_location_guard_match_passes() -> None:
    expected = acceptance_namespace_location("s3://acme-warehouse/")
    assert_namespace_location_matches(actual=expected, expected=expected)
    # Trailing-slash-only difference is still a match.
    assert_namespace_location_matches(actual=expected + "/", expected=expected)


def test_location_guard_mismatch_fails_loud_naming_both_values() -> None:
    expected = acceptance_namespace_location("s3://acme-warehouse/")
    stale = "s3://old-hand-test-bucket/testing_repark_acceptance"
    with pytest.raises(RuntimeError) as excinfo:
        assert_namespace_location_matches(actual=stale, expected=expected)
    message = str(excinfo.value)
    assert "mismatch" in message.lower()
    assert stale.rstrip("/") in message
    assert expected.rstrip("/") in message
    assert "stale namespace" in message.lower()


def test_location_guard_no_location_fails_loud() -> None:
    expected = acceptance_namespace_location("s3://acme-warehouse/")
    with pytest.raises(RuntimeError) as excinfo:
        assert_namespace_location_matches(actual=None, expected=expected)
    message = str(excinfo.value)
    assert "no Location" in message or "catalog-has-no-location" in message
    assert expected.rstrip("/") in message


def test_location_from_describe_rows_extracts_location() -> None:
    rows = [
        ("Catalog Name", "glue_catalog"),
        ("Namespace Name", "testing_repark_acceptance"),
        ("Location", "s3://acme-warehouse/testing_repark_acceptance"),
    ]
    assert location_from_describe_rows(rows) == "s3://acme-warehouse/testing_repark_acceptance"


def test_location_from_describe_rows_absent_is_none() -> None:
    rows = [
        ("Catalog Name", "glue_catalog"),
        ("Namespace Name", "bare"),
        ("Properties", ""),
    ]
    assert location_from_describe_rows(rows) is None


def test_probe_namespace_location_via_describe_reads_location_row() -> None:
    """AWS-free stub: probe drives DESCRIBE SQL + extracts Location from Arrow columns."""
    import pyarrow as pa
    from _acceptance import probe_namespace_location_via_describe

    class _FakeFrame:
        def to_arrow(self) -> pa.Table:
            return pa.table(
                {
                    "info_name": ["Catalog Name", "Namespace Name", "Location"],
                    "info_value": [
                        "glue_catalog",
                        "testing_repark_acceptance",
                        "s3://acme-warehouse/testing_repark_acceptance",
                    ],
                }
            )

    class _FakeSpark:
        def __init__(self) -> None:
            self.last_sql: str | None = None

        def sql(self, statement: str) -> _FakeFrame:
            self.last_sql = statement
            return _FakeFrame()

    spark = _FakeSpark()
    actual = probe_namespace_location_via_describe(
        spark, "glue_catalog", "testing_repark_acceptance"
    )
    assert actual == "s3://acme-warehouse/testing_repark_acceptance"
    assert spark.last_sql == "DESCRIBE NAMESPACE glue_catalog.testing_repark_acceptance"


def test_assert_glue_scratch_namespace_location_composes_getdatabase_and_compare() -> None:
    """AWS-free: Glue wrapper reads ``getDatabase.locationUri`` and compares to warehouse intent."""
    from types import SimpleNamespace

    from _acceptance import (
        ACCEPTANCE_NAMESPACE,
        SILVER_CATALOG,
        assert_glue_scratch_namespace_location,
    )

    class _FakeCatalog:
        def __init__(self, location: str) -> None:
            self._location = location
            self.last_name: str | None = None

        def getDatabase(self, db_name: str) -> SimpleNamespace:  # noqa: N802 — PySpark camelCase
            self.last_name = db_name
            return SimpleNamespace(locationUri=self._location)

    class _FakeSpark:
        def __init__(self, location: str) -> None:
            self.catalog = _FakeCatalog(location)

    expected = acceptance_namespace_location("s3://acme-warehouse/")
    spark_ok = _FakeSpark(expected)
    assert_glue_scratch_namespace_location(spark_ok, "s3://acme-warehouse/")
    assert spark_ok.catalog.last_name == f"{SILVER_CATALOG}.{ACCEPTANCE_NAMESPACE}"
    with pytest.raises(RuntimeError, match="mismatch"):
        assert_glue_scratch_namespace_location(
            _FakeSpark("s3://stale-bucket/testing_repark_acceptance"),
            "s3://acme-warehouse/",
        )


def test_glue_harness_calls_location_guard_and_s3tables_does_not() -> None:
    """Structural pin: Glue leg *invokes* the guard; S3 Tables body must not.

    AST-based (comments/strings do not count): the Glue test function must contain a real
    ``Call`` to ``assert_glue_scratch_namespace_location`` after a real ``create_namespace``
    call. Commenting the call out must turn this red.
    """
    import ast

    source = (_TESTS_DIR / "test_aws_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    def _function(name: str) -> ast.FunctionDef:
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        raise AssertionError(f"function {name} not found")

    def _call_names(fn: ast.FunctionDef) -> list[tuple[str, int]]:
        out: list[tuple[str, int]] = []
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    out.append((func.id, node.lineno))
                elif isinstance(func, ast.Attribute):
                    out.append((func.attr, node.lineno))
        return out

    glue = _function("test_process_silver_acceptance_against_glue")
    s3 = _function("test_process_silver_acceptance_against_s3tables")
    glue_calls = _call_names(glue)
    create_lines = [line for name, line in glue_calls if name == "create_namespace"]
    guard_lines = [
        line for name, line in glue_calls if name == "assert_glue_scratch_namespace_location"
    ]
    assert create_lines, "Glue leg must call create_namespace"
    assert guard_lines, "Glue leg must call assert_glue_scratch_namespace_location"
    assert min(create_lines) < min(guard_lines), "guard must run after ensure-namespace"
    s3_names = {name for name, _ in _call_names(s3)}
    assert "assert_glue_scratch_namespace_location" not in s3_names
    assert "run_mor_merge_compact_expire" not in s3_names
    s3_source = ast.get_source_segment(source, s3) or ""
    assert "S3 Tables namespaces carry no location by design" in s3_source

    mor = _function("test_mor_merge_compact_expire_against_glue")
    mor_calls = _call_names(mor)
    mor_create = [line for name, line in mor_calls if name == "create_namespace"]
    mor_guard = [
        line for name, line in mor_calls if name == "assert_glue_scratch_namespace_location"
    ]
    mor_names = {name for name, _ in mor_calls}
    assert mor_create, "MW-4 Glue leg must call create_namespace"
    assert mor_guard, "MW-4 Glue leg must call assert_glue_scratch_namespace_location"
    assert min(mor_create) < min(mor_guard), "MW-4 guard must run after ensure-namespace"
    assert "run_mor_merge_compact_expire" in mor_names
    assert "uuid4" in mor_names
    mor_source = ast.get_source_segment(source, mor) or ""
    assert "testing_mw4_mor_" in mor_source


def test_glue_location_guard_calls_get_database() -> None:
    """Y-3 activation: the Glue wrapper's live read is ``catalog.getDatabase``, not DESCRIBE."""
    import ast

    source = (_TESTS_DIR / "_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    helper = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == (
            "assert_glue_scratch_namespace_location"
        ):
            helper = node
            break
    assert helper is not None
    call_names = []
    for node in ast.walk(helper):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            call_names.append(node.func.attr)
    assert "getDatabase" in call_names
    assert "probe_namespace_location_via_describe" not in call_names
    assert "sql" not in call_names


def test_mor_ctas_sql_is_merge_on_read_not_copy_on_write() -> None:
    sql = mor_ctas_sql("glue_catalog.testing_repark_acceptance.testing_mw4_mor", "src")
    assert "merge-on-read" in sql
    assert "copy-on-write" not in sql
    assert "CREATE TABLE IF NOT EXISTS" not in sql
    assert MOR_ICEBERG_TABLE_PROPERTIES in sql
    assert ICEBERG_TABLE_PROPERTIES not in sql
    assert TARGET_FILE_SIZE_BYTES in sql


def test_mor_acceptance_expected_rows_renames_the_first_three() -> None:
    rows = mor_acceptance_expected_rows()
    assert len(rows) == MOR_SEED_ROW_COUNT
    assert rows[0] == {"id": 1, "name": "m1"}
    assert rows[2] == {"id": 3, "name": "m3"}
    assert rows[3] == {"id": 4, "name": "n4"}
    assert rows[-1] == {"id": MOR_SEED_ROW_COUNT, "name": f"n{MOR_SEED_ROW_COUNT}"}


def test_mor_helper_replays_the_last_merge() -> None:
    """C1-Q-004: the identical MERGE is a second ``merge_named_updates`` of ``[updates[-1]]``.

    Removing that replay must turn this pin red; a row-set compare around a deleted call
    would stay green.
    """
    import ast

    source = (_TESTS_DIR / "_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    helper = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "run_mor_merge_compact_expire":
            helper = node
            break
    assert helper is not None
    merge_calls = 0
    for node in ast.walk(helper):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "merge_named_updates"
        ):
            merge_calls += 1
    assert merge_calls >= 2
    helper_source = ast.get_source_segment(source, helper)
    assert helper_source is not None
    assert "[updates[-1]]" in helper_source


def test_maintenance_call_sql_is_catalog_dot_system() -> None:
    sql = maintenance_call_sql(
        "glue_catalog",
        "expire_snapshots",
        "testing_repark_acceptance.testing_mw4",
        extra="retain_last => 1",
    )
    assert sql.startswith("CALL glue_catalog.system.expire_snapshots(")
    assert "table => 'testing_repark_acceptance.testing_mw4'" in sql
    assert "retain_last => 1" in sql


def test_mor_merge_compact_expire_on_memory_catalog(tmp_path: pathlib.Path) -> None:
    """Always-run analog of the Glue MW-4 leg. Same helper, local warehouse."""
    spark = ReparkSession.builder.appName("pytest-mw4-mor").getOrCreate()
    spark.register_memory_catalog("mem", tmp_path)
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.ns LOCATION '{owned}'")

    outcome = run_mor_merge_compact_expire(spark, "mem", "ns", "mw4mor")
    assert_mor_maintenance_outcome(spark, outcome)
