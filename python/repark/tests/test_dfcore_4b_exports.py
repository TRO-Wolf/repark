"""DFCORE-4b ownership pin: the ten display bodies live in ``display.py``.

Sibling of ``test_dfcore_1_exports.py`` (which holds the surface snapshots):
this file would push that pin past the default source ceiling, so the gate's
sanctioned out is a split at the ownership boundary. The leaf import stays
function-local so the pin never binds the moved home at collection time.
"""

from __future__ import annotations

import repark.spark.dataframe as dataframe_package
import repark.spark.dataframe.core as dataframe_core
from repark.spark.dataframe import DataFrame

MOVED_DISPLAY_HELPERS: tuple[str, ...] = (
    "_show",
    "_repr",
    "_repr_html",
    "_eager_eval_enabled",
    "_eager_eval_limits",
    "_conf_lookup",
    "_normalize_show_args",
    "_resolve_display_style",
    "_preview_tail_rows",
    "_render_styled_show",
)

MOVED_DISPLAY_LEAVERS: tuple[str, ...] = (
    "_conf_lookup",
    "_eager_eval_enabled",
    "_eager_eval_limits",
    "_normalize_show_args",
    "_render_styled_show",
    "_resolve_display_style",
)

MOVED_DISPLAY_WRAPPERS: tuple[str, ...] = (
    "show",
    "__repr__",
    "_repr_html_",
    "_preview_tail_rows",
)


def test_moved_display_helpers_live_in_new_home() -> None:
    """Each moved display body is ``display``'s own frame-first module function.

    The six leavers are gone from the class; ``show``, ``__repr__``,
    ``_repr_html_``, and ``_preview_tail_rows`` stay as one-line wrappers (the
    styled-show pins call the tail preview on the instance).
    """
    import inspect

    import repark.spark.dataframe.display as display

    for name in MOVED_DISPLAY_HELPERS:
        helper = getattr(display, name)
        assert inspect.isfunction(helper), name
        assert helper.__module__ == display.__name__, name
        assert next(iter(inspect.signature(helper).parameters)) == "frame", name
    for name in MOVED_DISPLAY_LEAVERS:
        assert name not in vars(DataFrame), name
    for name in MOVED_DISPLAY_WRAPPERS:
        assert name in vars(DataFrame), name
    assert "display" in dir(dataframe_core)
    assert "display" in dir(dataframe_package)
