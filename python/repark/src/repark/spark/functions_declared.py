"""Declared-absent Spark function refusals (FNP-15 / FNP-16).

Each public name raises ``UnsupportedOperationException`` with the registry
reason. pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-016
"""

from __future__ import annotations

from typing import Any, NoReturn

from repark.errors import UnsupportedOperationException
from repark.spark.column import Column

FNP15_MESSAGES: dict[str, str] = {
    "java_method": (
        "java_method is unreachable: it loads a Java class by name and invokes a static "
        "method by reflection, which needs a live JVM. repark has no JVM. See "
        "docs/spark-sql-iceberg-parity.md (FNP-15 java_method)."
    ),
    "reflect": (
        "reflect is unreachable: it is Spark's CallMethodViaReflection spelling of "
        "java_method, which needs a live JVM. repark has no JVM. See "
        "docs/spark-sql-iceberg-parity.md (FNP-15 reflect)."
    ),
    "try_reflect": (
        "try_reflect is unreachable: it is reflect with exception-to-NULL, and still "
        "needs a live JVM. repark has no JVM. See docs/spark-sql-iceberg-parity.md "
        "(FNP-15 try_reflect)."
    ),
    "unwrap_udt": (
        "unwrap_udt is unreachable: Spark UserDefinedType unwrap walks the JVM UDT "
        "registry; with no JVM there is no UDT system to unwrap from. See "
        "docs/spark-sql-iceberg-parity.md (FNP-15 unwrap_udt)."
    ),
    "input_file_block_start": (
        "input_file_block_start is unreachable: it reads Spark's InputFileBlockHolder "
        "thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a "
        "task. DataFusion has no equivalent surface, and repark's input_file_name is "
        "itself still a stub. See docs/spark-sql-iceberg-parity.md "
        "(FNP-15 input_file_block_start)."
    ),
    "input_file_block_length": (
        "input_file_block_length is unreachable: it reads Spark's InputFileBlockHolder "
        "thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a "
        "task. DataFusion has no equivalent surface, and repark's input_file_name is "
        "itself still a stub. See docs/spark-sql-iceberg-parity.md "
        "(FNP-15 input_file_block_length)."
    ),
}

DECLARED_REFUSE_NAMES: tuple[str, ...] = tuple(FNP15_MESSAGES)


def _refuse(name: str) -> NoReturn:
    raise UnsupportedOperationException(FNP15_MESSAGES[name])


def java_method(*args: object, **kwargs: object) -> Column:
    """Unreachable JVM class-load reflection. pins: fnp-15-16/C-002"""
    _refuse("java_method")


def reflect(*args: object, **kwargs: object) -> Column:
    """Unreachable CallMethodViaReflection spelling. pins: fnp-15-16/C-003"""
    _refuse("reflect")


def try_reflect(*args: object, **kwargs: object) -> Column:
    """Unreachable reflect with exception-to-NULL. pins: fnp-15-16/C-004"""
    _refuse("try_reflect")


def unwrap_udt(*args: object, **kwargs: object) -> Column:
    """Unreachable Spark UserDefinedType unwrap. pins: fnp-15-16/C-005"""
    _refuse("unwrap_udt")


def input_file_block_start(*args: object, **kwargs: object) -> Column:
    """Unreachable InputFileBlockHolder start. pins: fnp-15-16/C-006"""
    _refuse("input_file_block_start")


def input_file_block_length(*args: object, **kwargs: object) -> Column:
    """Unreachable InputFileBlockHolder length. pins: fnp-15-16/C-007"""
    _refuse("input_file_block_length")


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy declared refusals onto the canonical functions module."""
    for name in DECLARED_REFUSE_NAMES:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
