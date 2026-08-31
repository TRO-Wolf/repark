"""Classify engine outcomes for W-0 cells. No engine imports."""

from __future__ import annotations

OUTCOME_OK = "ok"
OUTCOME_REFUSE = "refuse"
OUTCOME_ABSENT = "absent"
OUTCOME_OOM = "oom"
OUTCOME_SPILL = "spill"
OUTCOME_ERROR = "error"
OUTCOME_CRASH = "crash"
OUTCOME_SKIP = "skip"

OUTCOME_CLASSES: tuple[str, ...] = (
    OUTCOME_OK,
    OUTCOME_REFUSE,
    OUTCOME_ABSENT,
    OUTCOME_OOM,
    OUTCOME_SPILL,
    OUTCOME_ERROR,
    OUTCOME_CRASH,
    OUTCOME_SKIP,
)

_SLIDING_NEEDLES: tuple[str, ...] = (
    "sliding window",
    "sliding accumulator",
    "create_sliding_accumulator",
    "does not support retract",
    "retract_batch",
)

_ABSENT_NEEDLES: tuple[str, ...] = (
    "invalid function",
    "does not exist",
    "not found",
    "unsupportedoperation",
    "error_unrecognized_function",
    "no function",
    "unknown function",
    "not implemented",
    "not yet implemented",
)

_OOM_NEEDLES: tuple[str, ...] = (
    "outofmemory",
    "out of memory",
    "memory limit",
    "resourcesexhausted",
    "resources exhausted",
    "failed to allocate",
)

_SPILL_NEEDLES: tuple[str, ...] = (
    "spill",
    "spilling",
)


def classify_exception_text(text: str) -> str:
    """Map an exception string to one outcome class.

    Sliding-window DataFusion refusals win over a generic ``not implemented``
    so a retract gap is never filed as mere absence.

    Args:
        text: ``str(exception)`` plus the type name, lowercased by the caller
            or not — this function lowercases.

    Returns:
        One of :data:`OUTCOME_CLASSES` other than ``ok`` / ``skip`` / ``crash``.
    """
    lower = text.lower()
    if any(needle in lower for needle in _SLIDING_NEEDLES):
        return OUTCOME_REFUSE
    if any(needle in lower for needle in _OOM_NEEDLES):
        return OUTCOME_OOM
    if any(needle in lower for needle in _SPILL_NEEDLES):
        return OUTCOME_SPILL
    if any(needle in lower for needle in _ABSENT_NEEDLES):
        return OUTCOME_ABSENT
    return OUTCOME_ERROR


def registry_heading(name: str) -> str:
    """Registry section heading for one sliding-frame refusal.

    Args:
        name: roster aggregate name.

    Returns:
        ``### WIN-SLIDE-<name> —`` prefix used in the divergence registry.
    """
    return f"### WIN-SLIDE-{name} —"
