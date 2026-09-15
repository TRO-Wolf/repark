"""The Connect-only session names and the engine-less surfaces refuse loud.

pins: session-surface-1/C-003, C-004
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from repark.errors import PySparkNotImplementedError, PySparkRuntimeError
from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.client",
    "SparkSession.copyFromLocalToFs",
    "SparkSession.registerProgressHandler",
    "SparkSession.removeProgressHandler",
    "SparkSession.clearProgressHandlers",
    "SparkSession.readStream",
    "SparkSession.streams",
    "SparkSession.dataSource",
]


def _expect_refusal(call: Callable[[], Any], error_type: type[Exception], token: str) -> None:
    """Raise ``SystemExit`` unless ``call`` raises ``error_type`` carrying ``token``."""
    try:
        call()
    except error_type as error:
        if token not in str(error):
            raise SystemExit(f"refusal {error!r} lacks {token!r}") from error
        return
    raise SystemExit(f"{call} returned instead of refusing")


def main() -> None:
    """Run every refused name and check its error class token."""
    repark = ReparkSession.builder.appName("ex-ses-refusals").master("local[1]").getOrCreate()
    try:
        _expect_refusal(
            lambda: repark.client, PySparkRuntimeError, "ONLY_SUPPORTED_WITH_SPARK_CONNECT"
        )
        _expect_refusal(
            lambda: repark.copyFromLocalToFs("a", "b"),
            PySparkRuntimeError,
            "ONLY_SUPPORTED_WITH_SPARK_CONNECT",
        )
        handler = lambda *args: None  # noqa: E731
        _expect_refusal(
            lambda: repark.registerProgressHandler(handler),
            PySparkRuntimeError,
            "ONLY_SUPPORTED_WITH_SPARK_CONNECT",
        )
        _expect_refusal(
            lambda: repark.removeProgressHandler(handler),
            PySparkRuntimeError,
            "ONLY_SUPPORTED_WITH_SPARK_CONNECT",
        )
        _expect_refusal(
            lambda: repark.clearProgressHandlers(),
            PySparkRuntimeError,
            "ONLY_SUPPORTED_WITH_SPARK_CONNECT",
        )
        _expect_refusal(lambda: repark.readStream, PySparkNotImplementedError, "readStream")
        _expect_refusal(lambda: repark.streams, PySparkNotImplementedError, "streams")
        _expect_refusal(lambda: repark.dataSource, PySparkNotImplementedError, "dataSource")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
