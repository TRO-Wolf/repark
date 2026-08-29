"""SEC-04 — RuntimeConfig.getAll redacts secret-shaped values; get() does not."""

from __future__ import annotations

import pytest

from repark.spark._secrets import prop_key_is_secret
from repark.spark.session import ReparkSession

# Needle inventory mirrored from catalog_config.rs:126 prop_key_is_secret (do not edit Rust).
# Conformance pin: every Rust arm has ≥1 positive key; `bucket`/`arn`
# inside `_key` stay non-secret.
_SECRET_KEYS: tuple[str, ...] = (
    "aws_secret_access_key",
    "s3.secret-access-key",
    "fs.s3a.secret.key",
    "password",
    "session_token",
    "access_key_id",
    "s3.access-key-id",
    "token",
    "credential",
    "accessKey",
    "apikey",
    "privateKey",
    "bearer",
    "Authorization-Bearer",
    "basic.auth.user.info",
    "userinfo",
    "connection_string",
    "jdbc.connection-string",
    "key",  # exact-match arm
    "foo.key",  # becomes foo_key → _key arm
    "my_service_key",
    "spark.sql.catalog.mem.s3.secret-access-key",
    # Audit SEC-04: Hadoop-prefixed S3A spellings.
    "spark.hadoop.fs.s3a.secret.key",
    "spark.hadoop.fs.s3a.access.key",
)

_NON_SECRET_KEYS: tuple[str, ...] = (
    "spark.sql.shuffle.partitions",
    "warehouse",
    "s3.bucket",
    "table_bucket_arn",
    # `_key` arm exclusions: contains bucket/arn → must NOT redact (Rust parity).
    "bucket_key",
    "my_arn_key",
    "table_bucket_arn_key",
)


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test so conf mutations do not leak."""
    session = (
        ReparkSession.builder.master("local[1]").appName("test_a3_secrets_redaction").getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.parametrize("key", _SECRET_KEYS)
def test_prop_key_is_secret_needles(key: str) -> None:
    """Python mirror matches the Rust needle set for known secret key spellings."""
    assert prop_key_is_secret(key) is True, key


@pytest.mark.parametrize("key", _NON_SECRET_KEYS)
def test_prop_key_is_secret_non_secrets(key: str) -> None:
    """Non-secret keys stay unredacted."""
    assert prop_key_is_secret(key) is False, key


def test_get_all_redacts_secret_values(spark: ReparkSession) -> None:
    """getAll replaces secret values with *** while preserving keys."""
    secret = "SUPER_SECRET_VALUE_do_not_leak"
    spark.conf.set("s3.secret-access-key", secret)
    spark.conf.set("fs.s3a.secret.key", secret)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    all_conf = spark.conf.getAll
    assert all_conf["s3.secret-access-key"] == "***"
    assert all_conf["fs.s3a.secret.key"] == "***"
    assert secret not in all_conf.values()
    assert all_conf["spark.sql.shuffle.partitions"] == "8"
    assert "s3.secret-access-key" in all_conf
    assert "fs.s3a.secret.key" in all_conf


def test_get_explicit_secret_key_unchanged(spark: ReparkSession) -> None:
    """get(explicit secret key) returns the real value — SEC-04 both-ways rec.

    getAll redacts; get of a named key is intentional lookup and stays plaintext.
    """
    secret = "EXPLICIT_GET_SECRET_XYZ"
    spark.conf.set("s3.secret-access-key", secret)
    assert spark.conf.get("s3.secret-access-key") == secret
    assert spark.conf.getAll["s3.secret-access-key"] == "***"


def test_get_all_returned_dict_is_isolated(spark: ReparkSession) -> None:
    """Mutating the getAll mapping must not poison the conf store."""
    secret = "ISOLATION_SECRET_VALUE_XYZ"
    spark.conf.set("password", secret)
    dump = spark.conf.getAll
    assert dump["password"] == "***"
    dump["password"] = "LEAKED_VIA_MUTATION"
    assert spark.conf.getAll["password"] == "***"
    assert spark.conf.get("password") == secret
