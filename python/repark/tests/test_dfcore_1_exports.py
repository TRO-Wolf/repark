"""DFCORE-1 export snapshot pin: package, core, and DataFrame identity frozen pre-slice.

The expected sets below were recorded on the pre-slice tree; the move-only
relocation must leave every one of them byte-identical.

DFCORE-2 (2026-09-07) declared deltas, extended before the production edit: the
four ``_select_with_*`` helpers leave the class, so ``EXPECTED_DATAFRAME_DIR``
loses exactly those four names; ``core`` and the package each gain exactly the
two new module names ``udf_projection`` and ``udf_window_projection``.

DFCORE-3 (2026-09-07) declared deltas, extended before the production edit: the
seven statistics bodies leave for ``statistics.py`` as private frame-first
module functions, but the public methods (``approxQuantile``, ``corr``,
``cov``, ``crosstab``, ``summary``, ``describe`` on ``DataFrame``;
``freqItems`` on ``DataFrameStatFunctions``) stay as one-line wrappers, so
``EXPECTED_DATAFRAME_DIR`` is unchanged; ``core`` and the package each gain
exactly the one new module name ``statistics``.

DFCORE-4a (2026-09-07) declared deltas, extended before the production edit: the
three sampling bodies (``sample``, ``randomSplit``, ``sampleBy``) leave for
``sampling.py`` as private frame-first module functions, but the public methods
stay as one-line wrappers; the ``_prepare_sample_args`` static method leaves the
class with no wrapper, so ``EXPECTED_DATAFRAME_DIR`` loses exactly that one
name; the module-level ``_coerce_sample_seed`` moves to ``sampling.py`` and is
re-imported by ``core`` so both surfaces keep it; ``core`` and the package each
gain exactly the one new module name ``sampling``.

DFCORE-4b (2026-09-07) declared deltas, extended before the production edit: the
ten display bodies (``show``, ``__repr__``, ``_repr_html_``,
``_preview_tail_rows`` plus the six private helpers) leave for ``display.py``
as private frame-first module functions; ``show``, ``__repr__``,
``_repr_html_``, and ``_preview_tail_rows`` (called on the instance by the
styled-show pins) stay as one-line wrappers, so ``EXPECTED_DATAFRAME_DIR``
loses exactly the six leavers (``_conf_lookup``, ``_eager_eval_enabled``,
``_eager_eval_limits``, ``_normalize_show_args``, ``_render_styled_show``,
``_resolve_display_style``); ``core`` and the package each gain exactly the one
new module name ``display``. The ownership test lives in the sibling
``test_dfcore_4b_exports.py``: this file is at the default ceiling and the
gate's sanctioned out is a split, not an exception row.

DF-EXPLAIN-1 (2026-09-08): the explain rendering support splits out and
``_explain_text`` joins the class (same ``(extended, mode)`` shape beside
``explain``), so ``EXPECTED_DATAFRAME_DIR`` gains exactly ``_explain_text``;
the package gains exactly the one new module name ``explain``, while ``core``
gains the two private imports in its frozen surface and no module binding.
DF-EAGER-1 step 2 (2026-09-09): eager/lazy/compute join the class; guard trio to eager.py.
DF-COLREGEX-1 (2026-09-11): the package gains exactly ``colregex``, imported below.
EAGER-OWN-1 step 1 (2026-09-13): the cache-view ownership handle lands in the new
``cache_handle`` module; ``core`` binds it (``from repark.spark.dataframe import
cache_handle``), so ``EXPECTED_NEW_CORE_SUBMODULES`` and
``EXPECTED_NEW_PACKAGE_SUBMODULES`` each gain exactly ``cache_handle``.
``DataFrame`` gains two slots (``_cache_view_owned_handle``, ``_handles``), so
``EXPECTED_DATAFRAME_SLOTS`` and ``EXPECTED_DATAFRAME_DIR`` each gain exactly
those two names. ``_warn_storage_level_cosmetic_once`` moves to
``cache_handle.py`` unchanged and is re-imported by ``core``, so the frozen
core/package surfaces keep it. The same commit splits the ``EXPECTED_*`` tables
into ``_dfcore_1_expected.py`` (imported below) — the file sat exactly at the
default source ceiling and the new names crossed it; the tables are the
cohesive seam.
REPLACE-LINEAR-1 step 1 (2026-09-14): the ``DataFrame.replace`` body moves to
``replace_expr.py`` (validation and the flat searched-CASE build); ``replace``
stays a one-line wrapper so ``EXPECTED_DATAFRAME_DIR`` is unchanged; ``core``
and the package each gain exactly the one new module name ``replace_expr``.
DF-STREAM-BATCH-1 (2026-09-14): the streaming-named members bind on the class
from ``streaming_batch.py`` — ``EXPECTED_DATAFRAME_DIR`` gains exactly
``dropDuplicatesWithinWatermark``, ``drop_duplicates_within_watermark``,
``pandas_api``, ``plot``, ``rdd``, ``withWatermark``, ``with_watermark``, and
``writeStream``; ``EXPECTED_DATAFRAME_ALIASES`` gains the two camelCase pairs;
``core`` and the package each gain exactly the one new module name
``streaming_batch``.
IO-DECLARED-1 (2026-09-14): the package gains exactly the one new module name
``io_declared`` (bound by ``writer_readwriter`` importing it); no
``DataFrame`` member, slot, alias, or core-surface name changes.
IO-TEXT-1 (2026-09-14): the ``DataFrameWriter.text`` body moves to
``writer_text.py`` behind the one-line class attribute (the API freeze parses
``def`` signatures, and an attribute bind keeps the surface); the package gains
exactly the one new module name ``writer_text``, imported below.
DF-SURFACE-A-1 step 1 (2026-09-14): the ``localCheckpoint`` and ``isStreaming``
bodies move to ``surface_a.py`` behind signature-keeping def wrappers (the API
freeze parses ``def`` signatures), and the seven surface-a names bind one-line
each (``to``, ``withMetadata``, ``registerTempTable``, ``checkpoint``,
``isLocal`` as functions; ``sparkSession`` and ``executionInfo`` as
properties), so ``EXPECTED_DATAFRAME_DIR`` gains exactly those seven names;
``core`` and the package each gain exactly the one new module name
``surface_a``. Critic round 1 (rulings R-5/R-6, 2026-09-14): ``inputFiles``
and ``semanticHash`` leave the branch for a Rust plan-introspection unit and
the ``_schema_override`` sticker is deleted — ``schema`` reports whatever the
engine's plan reports (the narrow-width divergence is registry
LOGICAL-WIDTH-1).
DF-SURFACE-B-1 (2026-09-14): ``foreach``, ``foreachPartition``, and ``observe``
bind on the class from ``surface_b.py`` — ``EXPECTED_DATAFRAME_DIR`` gains those
three names and ``_observations``; ``core`` and the package each gain exactly
the one new module name ``surface_b``.
pins: df-surface-b-1/C-006
GROUPED-SURFACE-1 (2026-09-14): the six new ``GroupedData`` names bind from
``grouped_arrow.py`` and ``cogroup.py`` inside ``joins_columns.py``, so the
class surface only — no ``EXPECTED_DATAFRAME_DIR`` change; the package gains
exactly the two new module names ``grouped_arrow`` and ``cogroup`` while
``core`` binds neither.
COLUMN-PARITY-1 step 1 (2026-09-14): ``DataFrame`` gains one slot
(``_field_metadata``, the ``name(..., metadata=)`` StructField overlay), so
``EXPECTED_DATAFRAME_SLOTS`` and ``EXPECTED_DATAFRAME_DIR`` each gain exactly
that name; ``core`` binds ``column_fields`` (the deferred struct-edit /
method-body module), so ``EXPECTED_NEW_CORE_SUBMODULES`` and
``EXPECTED_NEW_PACKAGE_SUBMODULES`` each gain exactly ``_column_fields``;
``_column_window_spec`` now imports from ``column_fields`` unchanged.
DF-PLAN-INTROSPECT-1 (2026-09-14): ``inputFiles`` and ``semanticHash`` return
from the R-5 Rust unit as one-line class bindings over ``plan_introspect.py``,
so ``EXPECTED_DATAFRAME_DIR`` gains exactly those two names; ``core`` and the
package each gain exactly the one new module name ``plan_introspect``.
pins: df-plan-introspect-1/C-004
DF-RUST-3 (2026-09-16): ``freqItems`` and ``transpose`` join ``DataFrame`` —
``freqItems`` binds ``statistics.freqItems`` (the module moves to the top
import, so the bottom E402 line drops it) and ``transpose`` binds
``surface_a.transpose``, so ``EXPECTED_DATAFRAME_DIR`` gains exactly those two
names and neither surface list changes.
DF-SUBQUERY-1 (2026-09-15): the four subquery-surface methods (``scalar``,
``exists``, ``lateralJoin``, ``asTable``) bind on the class from
``subquery.DECLARED_MEMBERS`` — one tuple-assign line so the example-coverage
walk sees them — so ``EXPECTED_DATAFRAME_DIR`` gains exactly those four names;
``core`` and the package each gain exactly the one new module name ``subquery``.
pins: df-subquery-1/C-007
DF-METADATA-COL-1 (2026-09-16): ``DataFrame.metadataColumn`` binds
``metadata_column.metadataColumn``, so ``EXPECTED_DATAFRAME_DIR`` gains exactly
that one name; ``core`` and the package each gain exactly the one new module
name ``metadata_column``.
pins: df-metadata-col-1/M-4
"""

from __future__ import annotations

import typing
from types import ModuleType
from typing import Any

from _dfcore_1_expected import (
    EXPECTED_CORE_EXPORTS,
    EXPECTED_DATAFRAME_ALIASES,
    EXPECTED_DATAFRAME_DIR,
    EXPECTED_DATAFRAME_SLOTS,
    EXPECTED_NEW_CORE_SUBMODULES,
    EXPECTED_NEW_PACKAGE_SUBMODULES,
    EXPECTED_OVERLOADED_METHODS,
    EXPECTED_PACKAGE_EXPORTS,
)

import repark.spark.dataframe as dataframe_package
import repark.spark.dataframe.colregex as colregex  # noqa: F401
import repark.spark.dataframe.core as dataframe_core
import repark.spark.dataframe.export_errors as export_errors
import repark.spark.dataframe.grouped_udf as grouped_udf
import repark.spark.dataframe.plan_introspect as plan_introspect  # noqa: F401
import repark.spark.dataframe.rows_export as rows_export
import repark.spark.dataframe.udf_projection as udf_projection
import repark.spark.dataframe.udf_schema as udf_schema
import repark.spark.dataframe.udf_window_projection as udf_window_projection
import repark.spark.dataframe.writer_text as writer_text  # noqa: F401
from repark.spark.dataframe import DataFrame


def export_surface_names(module: Any) -> list[str]:
    """Return the sorted exported names of a module, minus dunders and submodules.

    Dunder attributes are interpreter state, and submodule attributes are bound by the
    import system when a re-export import executes; neither is a re-exported name. The
    package init itself copies only non-dunder names from core.
    """
    names: list[str] = []
    for name in dir(module):
        if name.startswith("__"):
            continue
        if isinstance(getattr(module, name, None), ModuleType):
            continue
        names.append(name)
    return sorted(names)


def test_package_export_set_unchanged() -> None:
    """Assert the package export surface still equals the pre-slice snapshot.

    Dunders are interpreter state (warning registries, import caches) and vary with
    test order, so the delta asserts cover non-dunder names only; the accepted gain
    is the submodule-attribute set bound by the import system when the facade
    imports each new home — the module docstring narrates membership per split.
    """
    expected_surface = [
        name
        for name in EXPECTED_PACKAGE_EXPORTS
        if not name.startswith("__")
        and not isinstance(getattr(dataframe_package, name, None), ModuleType)
    ]
    assert export_surface_names(dataframe_package) == expected_surface
    gained = {
        name
        for name in set(dir(dataframe_package)) - set(EXPECTED_PACKAGE_EXPORTS)
        if not name.startswith("__")
    }
    assert gained == EXPECTED_NEW_PACKAGE_SUBMODULES
    lost = {
        name
        for name in set(EXPECTED_PACKAGE_EXPORTS) - set(dir(dataframe_package))
        if not name.startswith("__")
    }
    assert lost == set()


def test_core_export_set_unchanged() -> None:
    """Assert the core export surface still equals the pre-slice snapshot.

    Dunders are interpreter state (warning registries, import caches) and vary with
    test order, so the delta asserts cover non-dunder names only. ``__annotations__``
    is separately asserted absent: the moved constants carried core's last annotated
    module-level assignments with them. The only accepted gain is the five new
    module bindings (DFCORE-2's two plus DFCORE-3's ``statistics`` plus DFCORE-4a's
    ``sampling`` plus DFCORE-4b's ``display``): ``select``, the statistics, the
    sampling, and the display wrappers delegate to the moved helpers through them.
    DF-EXPLAIN-1 (2026-09-08) binds no module on ``core``: its two private names
    import from ``explain.py`` into the frozen ``core``/package surfaces, and the
    class gains exactly ``_explain_text``.
    """
    expected_surface = [
        name
        for name in EXPECTED_CORE_EXPORTS
        if not name.startswith("__")
        and not isinstance(getattr(dataframe_core, name, None), ModuleType)
    ]
    assert export_surface_names(dataframe_core) == expected_surface
    gained = {
        name
        for name in set(dir(dataframe_core)) - set(EXPECTED_CORE_EXPORTS)
        if not name.startswith("__")
    }
    assert gained == EXPECTED_NEW_CORE_SUBMODULES
    lost = {
        name
        for name in set(EXPECTED_CORE_EXPORTS) - set(dir(dataframe_core))
        if not name.startswith("__")
    }
    assert lost == set()
    assert "__annotations__" not in dir(dataframe_core)


def test_dataframe_identity_unchanged() -> None:
    """Assert module, slots, and slot-only storage still match the snapshot."""
    assert DataFrame.__module__ == "repark.spark.dataframe.core"
    assert DataFrame.__slots__ == EXPECTED_DATAFRAME_SLOTS
    assert DataFrame.__slots__ == dataframe_core.DataFrame.__slots__
    instance = DataFrame.__new__(DataFrame)
    assert not hasattr(instance, "__dict__")
    assert "__dict__" not in dir(instance)


def test_dataframe_aliases_unchanged() -> None:
    """Assert every alias still binds the same underlying method."""
    seen: set[str] = set()
    for alias, target in EXPECTED_DATAFRAME_ALIASES:
        assert getattr(DataFrame, alias) is getattr(DataFrame, target)
        seen.add(alias)
    found = sorted(
        name
        for name, value in vars(DataFrame).items()
        if not name.startswith("__")
        and isinstance(value, type(DataFrame.filter))
        and value.__name__ != name
    )
    assert found == sorted(seen)


def test_dataframe_overloads_unchanged() -> None:
    """Assert the overloaded methods and their variant counts still match."""
    overloaded: dict[str, int] = {}
    for name, value in vars(DataFrame).items():
        if isinstance(value, (staticmethod, classmethod)):
            value = value.__func__
        if not isinstance(value, type(DataFrame.filter)):
            continue
        variants = typing.get_overloads(value)
        if variants:
            overloaded[name] = len(variants)
    assert overloaded == EXPECTED_OVERLOADED_METHODS


def test_dataframe_dir_unchanged() -> None:
    """Assert the non-dunder DataFrame attribute set still equals the snapshot.

    Class dunders are interpreter and stdlib-cache state: ``copyreg`` memoizes
    ``__slotnames__`` on the first pickle or copy of a frame, so the raw set varies
    with test order. Module, slots, and instance storage stay pinned by the identity
    test.
    """
    expected_names = [name for name in EXPECTED_DATAFRAME_DIR if not name.startswith("__")]
    current_names = sorted(name for name in dir(DataFrame) if not name.startswith("__"))
    assert current_names == expected_names


MOVED_HELPERS: dict[ModuleType, tuple[str, ...]] = {
    rows_export: (
        "_arrow_cell_to_spark_python",
        "_arrow_map_pairs",
        "_refuse_calendar_interval_python_value",
    ),
    export_errors: (
        "_EXPORT_MEMORY_ERROR_MARKERS",
        "_PYARROW_DYNAMIC_SOURCE_NOISE",
        "_export_engine_error",
        "_export_error_message",
        "_export_error_message_is_noise",
    ),
    udf_schema: ("_coerce_map_in_arrow_schema", "_validate_map_in_arrow_batch"),
    grouped_udf: (
        "_APPLY_IN_PANDAS_KEY_MISSING",
        "_apply_in_pandas_keys_equal",
        "_apply_in_pandas_row_key",
        "_apply_in_pandas_scalar_key_equal",
        "_apply_in_pandas_table_from_segments",
        "_iter_apply_in_pandas_group_tables",
        "_validate_apply_in_pandas_result_columns",
    ),
}


def test_moved_helpers_are_reexported_by_identity() -> None:
    """Every moved helper on core and on the package is the leaf module's own object."""
    for home, names in MOVED_HELPERS.items():
        for name in names:
            assert getattr(dataframe_core, name) is getattr(home, name), name
            assert getattr(dataframe_package, name) is getattr(home, name), name


MOVED_SELECT_HELPERS: dict[ModuleType, tuple[str, ...]] = {
    udf_projection: ("_select_with_pandas_udfs", "_select_with_python_udfs"),
    udf_window_projection: (
        "_select_with_window_pandas_udfs",
        "_select_with_ordered_window_pandas_udfs",
    ),
}


def test_moved_select_helpers_live_in_new_homes() -> None:
    """DFCORE-2: each moved select helper is its new home's own module function.

    The helpers take the frame as their first argument; the class keeps no copy.
    """
    import inspect

    for home, names in MOVED_SELECT_HELPERS.items():
        for name in names:
            helper = getattr(home, name)
            assert inspect.isfunction(helper), name
            assert helper.__module__ == home.__name__, name
            assert next(iter(inspect.signature(helper).parameters)) == "frame", name
            assert name not in vars(DataFrame), name


MOVED_STATISTICS_HELPERS: tuple[str, ...] = (
    "_approx_quantile",
    "_corr",
    "_cov",
    "_crosstab",
    "_describe",
    "_freq_items",
    "_summary",
)


def test_moved_statistics_helpers_live_in_new_home() -> None:
    """DFCORE-3: each moved statistics body is ``statistics``' own module function.

    The helpers take the frame as their first argument. Unlike DFCORE-2, the
    public methods stay on the class as one-line wrappers, so the class dir is
    unchanged; the leaf import stays function-local so the pin file never binds
    the moved home at collection time.
    """
    import inspect

    import repark.spark.dataframe.statistics as statistics

    for name in MOVED_STATISTICS_HELPERS:
        helper = getattr(statistics, name)
        assert inspect.isfunction(helper), name
        assert helper.__module__ == statistics.__name__, name
        assert next(iter(inspect.signature(helper).parameters)) == "frame", name
    assert "statistics" in dir(dataframe_core)
    assert "statistics" in dir(dataframe_package)


MOVED_SAMPLING_HELPERS: tuple[str, ...] = (
    "_sample",
    "_random_split",
    "_sample_by",
)


def test_moved_sampling_helpers_live_in_new_home() -> None:
    """DFCORE-4a: each moved sampling body is ``sampling``'s own module function.

    The three method bodies take the frame as their first argument. The two
    receiverless helpers move verbatim: ``_prepare_sample_args`` keeps its
    parameter list (it leaves the class with no wrapper) and
    ``_coerce_sample_seed`` is re-imported by ``core`` so both surfaces keep it
    by identity. The leaf import stays function-local so the pin file never
    binds the moved home at collection time.
    """
    import inspect

    import repark.spark.dataframe.sampling as sampling

    for name in MOVED_SAMPLING_HELPERS:
        helper = getattr(sampling, name)
        assert inspect.isfunction(helper), name
        assert helper.__module__ == sampling.__name__, name
        assert next(iter(inspect.signature(helper).parameters)) == "frame", name
    normalizer = sampling._prepare_sample_args
    assert inspect.isfunction(normalizer)
    assert normalizer.__module__ == sampling.__name__
    assert list(inspect.signature(normalizer).parameters) == [
        "withReplacement",
        "fraction",
        "seed",
    ]
    assert "_prepare_sample_args" not in vars(DataFrame)
    assert dataframe_core._coerce_sample_seed is sampling._coerce_sample_seed
    assert dataframe_package._coerce_sample_seed is sampling._coerce_sample_seed
    assert "sampling" in dir(dataframe_core)
    assert "sampling" in dir(dataframe_package)
