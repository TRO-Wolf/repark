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


def scrub_exception(error: BaseException) -> BaseException:
    """Replace credential text in an exception chain's args with masked text."""
    from repark import _native

    seen: set[int] = set()
    pending: list[BaseException] = [error]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        current.args = tuple(
            _native.mask_value_credentials(item) if isinstance(item, str) else item
            for item in current.args
        )
        for link in (current.__cause__, current.__context__):
            if isinstance(link, BaseException):
                pending.append(link)
    return error


__all__ = ["mask_credentials", "register_config_value", "scrub_exception"]
