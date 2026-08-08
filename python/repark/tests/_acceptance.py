"""Pure, AWS-free helpers for the real-AWS acceptance harness.

Kept in a non-``test_`` module (pytest does not collect it) so both the gated harness
(``test_aws_acceptance.py``) and its always-run unit tests (``test_acceptance_helpers.py``) share
one definition. Nothing here touches AWS or constructs a session — just constants and pure
builders, plus the ``deduplicate`` transform (which operates on an already-constructed DataFrame).

These mirror the shape of ``process_silver.py``.
"""

from __future__ import annotations

from repark import Window
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.dataframe import DataFrame

# ==============================================================================================
# Constants — mirrored from the real process_silver.py config block
# ==============================================================================================
# Bronze reads use the s3a scheme; the Glue warehouse uses s3. Both must resolve (WG3).
BRONZE_BUCKET = "example-bronze-bucket-v1"
BRONZE_PREFIX = "bronze"

# The real script's config block names the catalog ``glue_alt`` but publishes via ``glue_catalog``
# (the cluster spark-defaults supply that name on Glue/EMR). The harness configures the name it
# actually uses for the publish path.
SILVER_CATALOG = "glue_catalog"
GLUE_WAREHOUSE = "s3://example-warehouse/"

# S3 Tables (A2 second bullet). A NON-secret catalog name only; the table-bucket ARN is an
# account-specific value passed at RUNTIME from the `TABLE_BUCKET_ARN` env var — NEVER hardcoded
# here or committed (both repos are public-bound).
S3TABLES_CATALOG = "s3tables_catalog"

# Scratch namespace ONLY. Never the production silver namespace. Both the namespace and every
# table the harness creates carry a `testing_` prefix so they read as disposable at a glance.
ACCEPTANCE_NAMESPACE = "testing_repark_acceptance"
ACCEPTANCE_TABLE_PREFIX = "testing_"
PRODUCTION_NAMESPACE = "example_silver"  # named here solely to assert we never touch it

TEMP_VIEW = "iv_temp_data"

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


def glue_catalog_config(catalog_name: str, warehouse: str) -> dict[str, str]:
    """The ``spark.sql.catalog.<name>.*`` block for a Glue catalog (process_silver.py shape).

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
    """The ``spark.sql.catalog.<name>.*`` block for an **S3 Tables** catalog (process_silver shape).

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
    """Keep the newest row per ``id_col`` (mirrors ``process_silver.deduplicate_silver_df``).

    ``row_number()`` over ``partitionBy(id_col).orderBy(timestamp_col DESC)`` → keep ``rn == 1`` →
    drop the helper column.
    """
    window = Window.partitionBy(id_col).orderBy(F.col(timestamp_col).desc())
    return (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
