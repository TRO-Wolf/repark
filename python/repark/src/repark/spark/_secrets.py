"""Credential masking for facade refusal text, delegated to ``repark_common::redaction``."""

from __future__ import annotations


def mask_credentials(value: object) -> object:
    if not isinstance(value, str):
        return value
    from repark import _native

    return _native.mask_value_credentials(value)


def register_config_value(value: object) -> None:
    if not isinstance(value, str):
        return
    from repark import _native

    _native.register_config_value(value)


__all__ = ["mask_credentials", "register_config_value"]
