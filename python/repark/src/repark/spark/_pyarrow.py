"""Optional pyarrow import that names the extra on failure."""

from __future__ import annotations

from typing import Any

_PYARROW_EXTRA_HINT = "this operation requires pyarrow (pip install 'repark[pyarrow]')"


def require_pyarrow() -> Any:
    """Import ``pyarrow`` or raise ``ImportError`` naming ``repark[pyarrow]``."""
    try:
        import pyarrow as pa
    except ImportError as error:
        raise ImportError(_PYARROW_EXTRA_HINT) from error
    return pa
