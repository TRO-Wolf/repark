"""Secret-property detection for facade configuration redaction.

The needle set mirrors the native catalog configuration classifier in
``crates/repark-core/src/catalog_config.rs``. Runtime configuration
listings redact matching values; explicit value reads remain unchanged.
"""

from __future__ import annotations


def prop_key_is_secret(key: str) -> bool:
    """Whether a configuration **value** should be redacted for the given key name.

    Matches the Rust ``prop_key_is_secret`` needles case-insensitively after folding
    hyphens and dots to underscores, then a compact form with underscores stripped
    for camelCase / one-word spellings.
    """
    # Hyphens and dots → underscore so `basic.auth.user.info` / `s3.access-key-id` share needles
    lower = key.lower().replace("-", "_").replace(".", "_")
    # Underscores stripped so camelCase `accessKey` / `privateKey` / one-word `apikey` share
    compact = lower.replace("_", "")
    # Substring match covers `aws_secret_access_key`, `s3.access-key-id`, `session_token`, etc.
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
        or "user_info" in lower
        or "userinfo" in compact
        or lower == "key"
        # `.key` needle is unreachable after the dot→underscore fold above; `foo.key` → `foo_key`
        # The `_key` arm catches this form.
        or (lower.endswith("_key") and "bucket" not in lower and "arn" not in lower)
    )


__all__ = ["prop_key_is_secret"]
