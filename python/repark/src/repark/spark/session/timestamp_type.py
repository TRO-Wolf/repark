"""The session default-timestamp conf — ``spark.sql.timestampType``.

The key is spelled **exactly once** on each side of the boundary: :data:`TIMESTAMP_TYPE_KEY`
here and ``repark_functions::timestamp_type::SPARK_SQL_TIMESTAMP_TYPE_KEY`` in the engine.
There is no alternate spelling.

**One truth at build, store-only at runtime (ansi.enabled precedent).** The engine
resolves the value once in ``SparkExtension.configure``. ``spark.conf.get`` /
``spark.conf.set`` round-trip on the facade store; a runtime set does **not** re-resolve
the analyzer / DDL mapping. NTZ opt-in is ``ReparkSession.builder.config(KEY,
"TIMESTAMP_NTZ")``. An invalid value is refused loud, naming both legal tokens.

**Default ``TIMESTAMP_LTZ``.** Bare ``TIMESTAMP`` stays today's instant (µs+UTC).
``TIMESTAMP_NTZ`` makes literals / ``CAST(… AS TIMESTAMP)`` / DDL ``TIMESTAMP`` resolve
as naive µs. Explicit ``TimestampType`` / ``TimestampNTZType`` / ``TIMESTAMP_NTZ`` /
``TIMESTAMP WITH TIME ZONE`` are not this knob.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    import pyarrow as pa

    from repark.spark.types import DataType

TIMESTAMP_TYPE_KEY = "spark.sql.timestampType"
DEFAULT_TIMESTAMP_TYPE = "TIMESTAMP_LTZ"
TIMESTAMP_LTZ_VALUE = "TIMESTAMP_LTZ"
TIMESTAMP_NTZ_VALUE = "TIMESTAMP_NTZ"
TIMESTAMP_TYPE_KEYS: tuple[str, ...] = (TIMESTAMP_TYPE_KEY,)


def parse_timestamp_type(raw: str) -> str:
    """Return the canonical token, or raise naming both legal values."""
    from repark.errors import IllegalArgumentException

    trimmed = raw.strip()
    if trimmed in {TIMESTAMP_LTZ_VALUE, TIMESTAMP_NTZ_VALUE}:
        return trimmed
    raise IllegalArgumentException(
        f"The value '{raw}' in the config \"{TIMESTAMP_TYPE_KEY}\" is invalid. "
        f"{TIMESTAMP_TYPE_KEY} should be one of {TIMESTAMP_LTZ_VALUE}, {TIMESTAMP_NTZ_VALUE}"
    )


def normalize_timestamp_type_config(config: MutableMapping[str, str | None]) -> None:
    """Strip surrounding whitespace from the builder value, in place.

    Validity stays the engine's at ``getOrCreate`` (and :func:`parse_timestamp_type` at
    runtime ``conf.set``). A value that trims to empty is left empty so the engine refuses.
    """
    raw = config.get(TIMESTAMP_TYPE_KEY)
    if isinstance(raw, str):
        config[TIMESTAMP_TYPE_KEY] = raw.strip()


def active_timestamp_type() -> str:
    """The live session's default, or :data:`DEFAULT_TIMESTAMP_TYPE` if none is active."""
    try:
        from repark.spark.session.session_core import ReparkSession

        session = ReparkSession.getActiveSession()
        if session is None:
            return DEFAULT_TIMESTAMP_TYPE
        value = session.conf.get(TIMESTAMP_TYPE_KEY)
        if isinstance(value, str) and value.strip():
            return parse_timestamp_type(value)
    except Exception:
        return DEFAULT_TIMESTAMP_TYPE
    return DEFAULT_TIMESTAMP_TYPE


def is_default_timestamp_ntz() -> bool:
    """True when the live session's default SQL ``TIMESTAMP`` is NTZ."""
    return active_timestamp_type() == TIMESTAMP_NTZ_VALUE


def default_timestamp_arrow_type() -> pa.DataType:
    """Arrow type for an inferred bare ``TIMESTAMP`` column."""
    import pyarrow as pa

    if is_default_timestamp_ntz():
        return pa.timestamp("us")
    return pa.timestamp("us", tz="UTC")


def default_timestamp_data_type() -> DataType:
    """Spark type class for an inferred bare ``TIMESTAMP`` column."""
    from repark.spark.types import TimestampNTZType, TimestampType

    if is_default_timestamp_ntz():
        return TimestampNTZType()
    return TimestampType()
