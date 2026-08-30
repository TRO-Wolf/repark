"""Declared-absent Spark function refusals (FNP-15 / FNP-16).

Each public name raises ``UnsupportedOperationException`` with the registry
reason. pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
C-016
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


class _DeferredFamilyRefusal:
    """Callable stub for an FNP-16 deferred-by-cost name."""

    def __init__(self, name: str, message: str) -> None:
        """Bind the Spark function name and its registry refusal message."""
        self.__name__ = name
        self.__qualname__ = name
        self.__doc__ = message
        self._message = message

    def __call__(self, *args: object, **kwargs: object) -> Column:
        """Raise the deferred-by-cost refusal for this name."""
        raise UnsupportedOperationException(self._message)


SKETCH_NAMES: tuple[str, ...] = (
    "hll_sketch_agg",
    "hll_sketch_estimate",
    "hll_union",
    "hll_union_agg",
    "kll_merge_agg_bigint",
    "kll_merge_agg_double",
    "kll_merge_agg_float",
    "kll_sketch_agg_bigint",
    "kll_sketch_agg_double",
    "kll_sketch_agg_float",
    "kll_sketch_get_n_bigint",
    "kll_sketch_get_n_double",
    "kll_sketch_get_n_float",
    "kll_sketch_get_quantile_bigint",
    "kll_sketch_get_quantile_double",
    "kll_sketch_get_quantile_float",
    "kll_sketch_get_rank_bigint",
    "kll_sketch_get_rank_double",
    "kll_sketch_get_rank_float",
    "kll_sketch_merge_bigint",
    "kll_sketch_merge_double",
    "kll_sketch_merge_float",
    "kll_sketch_to_string_bigint",
    "kll_sketch_to_string_double",
    "kll_sketch_to_string_float",
    "theta_difference",
    "theta_intersection",
    "theta_intersection_agg",
    "theta_sketch_agg",
    "theta_sketch_estimate",
    "theta_union",
    "theta_union_agg",
)

_SKETCH_REASON = (
    "is reachable without a JVM and is deferred by cost: Spark sketch columns are "
    "Apache DataSketches binary blobs, and DataFusion's hyperloglog.rs is a different "
    "format that cannot serve the blob. See docs/spark-sql-iceberg-parity.md "
    "(FNP-16 sketches)."
)


def _bind_family(names: tuple[str, ...], reason: str) -> None:
    for name in names:
        globals()[name] = _DeferredFamilyRefusal(name, f"{name} {reason}")


_bind_family(SKETCH_NAMES, _SKETCH_REASON)

CSV_XML_XPATH_NAMES: tuple[str, ...] = (
    "to_csv",
    "to_xml",
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_number",
    "xpath_short",
    "xpath_string",
)
_CSV_XML_XPATH_REASON = (
    "is reachable without a JVM and is deferred by cost: the xpath family needs an "
    "XPath 1.0 engine matching javax.xml.xpath, and datafusion-spark's csv and xml "
    "modules are empty. See docs/spark-sql-iceberg-parity.md (FNP-16 CSV/XML/XPath)."
)
_bind_family(CSV_XML_XPATH_NAMES, _CSV_XML_XPATH_REASON)

VARIANT_NAMES: tuple[str, ...] = (
    "is_variant_null",
    "parse_json",
    "schema_of_variant",
    "schema_of_variant_agg",
    "to_variant_object",
    "try_parse_json",
    "try_variant_get",
    "variant_get",
)
_VARIANT_REASON = (
    "is reachable without a JVM and is deferred by cost: Spark VARIANT is a specific "
    "value/metadata binary encoding; repark's VariantType is a shell with nothing behind "
    "it. See docs/spark-sql-iceberg-parity.md (FNP-16 VARIANT)."
)
_bind_family(VARIANT_NAMES, _VARIANT_REASON)

DECLARED_REFUSE_NAMES: tuple[str, ...] = (
    tuple(FNP15_MESSAGES) + SKETCH_NAMES + CSV_XML_XPATH_NAMES + VARIANT_NAMES
)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy declared refusals onto the canonical functions module."""
    for name in DECLARED_REFUSE_NAMES:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
