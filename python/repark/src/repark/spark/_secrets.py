"""Credential masking for facade refusal text, delegated to ``repark_common::redaction``."""

from __future__ import annotations


def mask_credentials(value: object) -> object:
    if not isinstance(value, str):
        return value
    from repark import _native

    return _native.mask_value_credentials(value)


__all__ = ["mask_credentials"]
