"""Mid-stream Arrow export failure mapping for the DataFrame facade."""

from __future__ import annotations

from repark.errors import PySparkException

_EXPORT_MEMORY_ERROR_MARKERS: tuple[str, ...] = (
    "resources exhausted",
    "externalsorter",
    "externalsortermerge",
    "sortpreservingmergeexec",
    "sortpreservingmerge",
    "not enough memory to continue external sort",
    "memory pool",
    "failed to allocate additional",
    "datafusion.runtime.memory_limit",
)
_PYARROW_DYNAMIC_SOURCE_NOISE = "dynamically evaluated source"


def _export_error_message_is_noise(message: str) -> bool:
    """True when ``message`` is pyarrow capsule noise that hides the engine payload."""
    lower = message.lower()
    return _PYARROW_DYNAMIC_SOURCE_NOISE in lower and not any(
        marker in lower for marker in _EXPORT_MEMORY_ERROR_MARKERS
    )


def _export_error_message(error: BaseException) -> str:
    """Extract the best human message from a mid-stream Arrow/engine export failure.

    Prefers the DataFusion payload over pyarrow's "Could not get source, probably due
    dynamically evaluated source code" wrapper. Walks ``__cause__`` / ``__context__`` and
    ``args`` so the operator sees the ExternalSorter / pool text, not the capsule noise.
    """
    candidates: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    depth = 0
    while current is not None and depth < 12:
        identity = id(current)
        if identity in seen:
            break
        seen.add(identity)
        text = str(current).strip()
        if text:
            candidates.append(text)
        for argument in getattr(current, "args", ()) or ():
            if isinstance(argument, str) and argument.strip():
                candidates.append(argument.strip())
            elif isinstance(argument, BaseException):
                nested = str(argument).strip()
                if nested:
                    candidates.append(nested)
        nxt: BaseException | None = current.__cause__
        if nxt is None and current.__context__ is not None:
            nxt = current.__context__
        current = nxt
        depth += 1

    if not candidates:
        return repr(error)

    useful = [message for message in candidates if not _export_error_message_is_noise(message)]
    if not useful:
        useful = candidates
    chosen = max(useful, key=len)
    if chosen.startswith("External error: "):
        chosen = chosen[len("External error: ") :]
    return chosen


def _export_engine_error(error: BaseException) -> PySparkException:
    """Map a mid-stream Arrow export failure to ``PySparkException`` with useful context."""
    message = _export_error_message(error)
    lower = message.lower()
    is_memory = any(marker in lower for marker in _EXPORT_MEMORY_ERROR_MARKERS)
    if is_memory and "repark.memory.limit.gb" not in lower:
        message = (
            f"{message.rstrip()}\n"
            "REPARK: raise the FairSpillPool via "
            "SparkSession.builder.config('repark.memory.limit.gb', N).getOrCreate() "
            "(build-time; RAM-relative, cap 8 GiB; 0 = unbounded) or "
            "spark.conf.set('datafusion.runtime.memory_limit', 'NG') "
            "(runtime; same pool — one truth, not two knobs)."
        )
    return PySparkException(message)
