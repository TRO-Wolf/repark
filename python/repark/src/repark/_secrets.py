"""Mirror of Rust ``prop_key_is_secret`` for Python conf redaction (r24 SEC-04).

Source of truth for the needle set is
``crates/repark-session/src/catalog_config.rs`` ``prop_key_is_secret`` (line ~126).
This module is a **read-only mirror** — do not edit the Rust twin from this track.
Used by :meth:`repark.session.RuntimeConfig.getAll` to redact secret-shaped values
before users paste conf dumps into bug reports.

Postgres has its own redaction path (``repark-postgres``); leave it alone.
"""

from __future__ import annotations


def prop_key_is_secret(key: str) -> bool:
    """Whether a configuration **value** should be redacted for the given key name.

    Matches the Rust ``prop_key_is_secret`` needles case-insensitively after folding
    hyphens and dots to underscores, then a compact form with underscores stripped
    for camelCase / one-word spellings.
    """
    # Hyphens and dots → underscore so `basic.auth.user.info` / `s3.access-key-id` share needles
    # with snake_case (C2-SEC-002 / O4-C3-SEC-001).
    lower = key.lower().replace("-", "_").replace(".", "_")
    # Underscores stripped so camelCase `accessKey` / `privateKey` / one-word `apikey` share
    # needles with snake_case (O2-C2-SEC-001 / O4-C1-SEC-001 residual of C1-SEC-002 / C2-SEC-002).
    compact = lower.replace("_", "")
    # Substring match covers `aws_secret_access_key`, `s3.access-key-id`, `session_token`, etc.
    # Hyphen/dot fold lets OpenDAL / Spark spellings share one needle set (C2-SEC-002).
    return (
        "aws_secret" in lower
        or "secret" in lower
        or "password" in lower
        or "token" in lower
        or "credential" in lower
        or "connection_string" in lower
        or lower.endswith("access_key_id")
        or lower.endswith("access_key")
        or "accesskey" in compact
        or "apikey" in compact
        or "privatekey" in compact
        or compact == "bearer"
        or compact.endswith("bearer")
        # Kafka / Spark JDBC often embed `user:password` under this key (O4-C3-SEC-001).
        or "user_info" in lower
        or "userinfo" in compact
        or lower == "key"
        # `.key` needle is unreachable after the dot→underscore fold above; `foo.key` → `foo_key`
        # is caught by the `_key` arm (review 2026-07-23).
        or (lower.endswith("_key") and "bucket" not in lower and "arn" not in lower)
    )


__all__ = ["prop_key_is_secret"]
