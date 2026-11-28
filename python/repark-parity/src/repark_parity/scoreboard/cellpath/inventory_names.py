from __future__ import annotations

import contextlib
import importlib
import inspect
import re
from typing import Any

EXCLUDED_BY_INSTRUCTION = frozenset({"aes_encrypt", "aes_decrypt", "try_aes_decrypt"})


def normalized_default(value: Any) -> str | None:
    """Render a parameter default without object addresses."""
    if value is inspect.Parameter.empty:
        return None
    return re.sub(r"0x[0-9a-fA-F]+", "<address>", repr(value))


def describe(obj: Any) -> dict:
    """Return the signature text and the parameter names, kinds and defaults of a callable."""
    try:
        signature = inspect.signature(obj)
    except (TypeError, ValueError):
        return {"signature": None, "parameters": None}
    return {
        "signature": str(signature),
        "parameters": [
            [parameter.name, parameter.kind.name, normalized_default(parameter.default)]
            for parameter in signature.parameters.values()
        ],
    }


def _surfaces(s: Any, funcs: Any, types: Any, window: Any, row: Any) -> dict[str, Any]:
    spark = type(s).__module__.startswith("pyspark")
    frame = s.range(1)
    return {
        "SparkSession": type(s),
        "SparkSession.Builder": type(s.builder),
        "RuntimeConfig": type(s.conf),
        "DataFrame": type(frame),
        "Column": type(frame.id),
        "GroupedData": type(frame.groupBy()),
        "DataFrameNaFunctions": type(frame.na),
        "DataFrameStatFunctions": type(frame.stat),
        "Row": row,
        "Window": window,
        "WindowSpec": type(window.orderBy("x")),
        "Catalog": type(s.catalog),
        "DataFrameReader": type(s.read),
        "DataFrameWriter": type(frame.write),
        "DataFrameWriterV2": type(frame.writeTo("audit_unused")),
        "F": funcs,
        "types": types,
        "errors": importlib.import_module("pyspark.errors" if spark else "repark.errors"),
    }


def _member_row(prefix: str, owner: Any, name: str) -> list | None:
    if name in EXCLUDED_BY_INSTRUCTION:
        return [prefix, name, "excluded-by-instruction", None, None, None]
    try:
        member = inspect.getattr_static(owner, name)
    except AttributeError:
        with contextlib.suppress(AttributeError):
            member = getattr(owner, name)
            info = describe(member)
            module = getattr(member, "__module__", None)
            return [prefix, name, "dynamic", info["signature"], info["parameters"], module]
        return None
    if isinstance(member, (staticmethod, classmethod)):
        member = member.__func__
    if inspect.ismodule(member):
        return None
    info = describe(member)
    module = getattr(member, "__module__", None)
    return [prefix, name, type(member).__name__, info["signature"], info["parameters"], module]


def survey(s: Any, funcs: Any, types: Any, window: Any, row: Any) -> list:
    """List the public names and signatures of each facade surface without calling them."""
    output = []
    for prefix, owner in _surfaces(s, funcs, types, window, row).items():
        symbols = getattr(owner, "__all__", None) if inspect.ismodule(owner) else None
        for name in sorted(set(symbols if symbols is not None else dir(owner))):
            if name.startswith("_"):
                continue
            member_row = _member_row(prefix, owner, name)
            if member_row is not None:
                output.append(member_row)
    return output
