"""Target_partitions emit / session contract.

Primary cells omit the session knob (DataFusion ``num_cpus`` default) and emit
``target_partitions=default``; isolation cells set ``target_partitions=1`` and emit
``isolation=single_core``. No spaces in TA_PIPELINE kv values.
"""

from __future__ import annotations

DEFAULT_TARGET_PARTITIONS_LABEL = "default"
ISOLATION_ROLE = "single_core"


def session_target_partitions(*, isolation: bool) -> int | None:
    """Value for ``make_session(target_partitions=...)``; ``None`` omits the knob."""
    return 1 if isolation else None


def emit_target_partition_fields(*, isolation: bool) -> dict[str, int | str]:
    """Keyword fields for ``emit_line`` (A3 tokens; no spaces)."""
    if isolation:
        return {"target_partitions": 1, "isolation": ISOLATION_ROLE}
    return {"target_partitions": DEFAULT_TARGET_PARTITIONS_LABEL}
