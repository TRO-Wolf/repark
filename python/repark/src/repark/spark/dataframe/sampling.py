"""DataFrame sampling bodies behind the public sample wrappers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import (
    IllegalArgumentException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._temp_views import scratch_view_name

if TYPE_CHECKING:
    from repark.spark.column import Column
    from repark.spark.dataframe.core import DataFrame


def _coerce_sample_seed(value: object, *, label: str) -> int:
    """Coerce a ``sample()`` seed argument to int; bools and non-numerics refuse."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"sample() {label} must be int, got {type(value).__name__}")
    return int(value)


def _prepare_sample_args(
    withReplacement: bool | float | None,  # noqa: N803
    fraction: float | None,
    seed: int | None,
) -> tuple[bool, float, int]:
    """Resolve sample overloads (PySpark classic/connect sample-arg helper parity).

    Default plan seed is ``42`` (not ``random.randint``) so unseeded samples are
    action-stable on the same plan.
    """
    default_seed = 42

    if (
        isinstance(withReplacement, bool)
        and isinstance(fraction, (int, float))
        and not (isinstance(fraction, bool))
    ):
        plan_seed = default_seed if seed is None else _coerce_sample_seed(seed, label="seed")
        return withReplacement, float(fraction), plan_seed

    if (
        withReplacement is None
        and isinstance(fraction, (int, float))
        and not isinstance(fraction, bool)
    ):
        plan_seed = default_seed if seed is None else _coerce_sample_seed(seed, label="seed")
        return False, float(fraction), plan_seed

    if isinstance(withReplacement, (int, float)) and not isinstance(withReplacement, bool):
        if fraction is not None:
            plan_seed = _coerce_sample_seed(fraction, label="seed")
        else:
            plan_seed = default_seed
        return False, float(withReplacement), plan_seed

    argtypes = [type(arg).__name__ for arg in (withReplacement, fraction, seed)]
    raise PySparkTypeError(
        errorClass="NOT_BOOL_OR_FLOAT_OR_INT",
        messageParameters={
            "arg_name": ("withReplacement (optional), fraction (required) and seed (optional)"),
            "arg_type": ", ".join(argtypes),
        },
    )


def _sample(
    frame: DataFrame,
    withReplacement: bool | float | None = None,  # noqa: N803
    fraction: float | None = None,
    seed: int | None = None,
) -> DataFrame:
    """Bernoulli sample of rows (PySpark ``DataFrame.sample``).

    Engine RNG ≠ Spark RNG — pins use seed determinism + count tolerance, not exact rows.
    ``withReplacement=True`` is not supported (loud error).

    Overload resolution mirrors PySpark's sample-arg helper (classic/connect DataFrame):

    * ``sample(fraction)`` / ``sample(fraction, seed)`` — first positional is a number
    * ``sample(withReplacement, fraction [, seed])`` — first positional is a bool
    * ``sample(fraction=…, seed=…)`` — keyword form

    When ``seed`` is omitted, repark bakes a default seed into the plan so repeated
    actions on the same sampled DataFrame return a stable multiset (Spark embeds a
    planning-time seed the same way).
    """
    frame._ensure_alive()
    replacement_flag, fraction_value, plan_seed = _prepare_sample_args(
        withReplacement, fraction, seed
    )
    if replacement_flag:
        raise UnsupportedOperationException(
            "sample(withReplacement=True) is not supported; use withReplacement=False"
        )
    if fraction_value < 0.0 or fraction_value > 1.0:
        raise IllegalArgumentException(
            f"requirement failed: Fraction must be in [0, 1], but got {fraction_value}"
        )
    view = scratch_view_name(frame._session, "__repark_samp_")
    frame._session.create_or_replace_temp_view(view, frame._plan())
    try:
        if fraction_value >= 1.0:
            planned = frame._session.sql(f"SELECT * FROM {view}")
        elif fraction_value <= 0.0:
            planned = frame._session.sql(f"SELECT * FROM {view} WHERE 1 = 0")
        else:
            order_fields = frame._engine_names if frame._engine_names is not None else frame.columns
            order_sql = ", ".join(_quote_ident_sql(c) for c in order_fields)
            order_clause = f"ORDER BY {order_sql}" if order_sql else ""
            planned = frame._session.sql(
                f"SELECT * EXCLUDE (__repark_rn) FROM ("
                f"  SELECT *, row_number() OVER ({order_clause}) AS __repark_rn FROM {view}"
                f") WHERE (abs((CAST(__repark_rn AS BIGINT) + {plan_seed}) "
                f"* 1103515245 + 12345) % 1000000) "
                f"/ 1000000.0 < {fraction_value}"
            )
        child = frame._spawn(planned)
        if frame._display_names is not None and frame._engine_names is not None:
            child._display_names = list(frame._display_names)
            child._engine_names = list(frame._engine_names)
            child._origin_map = dict(frame._origin_map) if frame._origin_map is not None else None
        return child
    finally:
        frame._session.drop_temp_view(view)


def _random_split(
    frame: DataFrame,
    weights: list[float] | tuple[float, ...],
    seed: int | None = None,
) -> list[DataFrame]:
    """Split rows into weighted buckets (PySpark ``DataFrame.randomSplit``).

    Weights are normalized like Spark. Engine RNG ≠ Spark — pin count-in-tolerance, disclose
    exact-row divergence.
    """
    frame._ensure_alive()
    if not isinstance(weights, (list, tuple)) or not weights:
        raise PySparkTypeError("randomSplit weights must be a non-empty list of floats")
    weight_list = [float(weight) for weight in weights]
    if any(weight < 0 for weight in weight_list):
        raise PySparkValueError("weights must be non-negative")
    total = sum(weight_list)
    if total == 0:
        raise PySparkValueError("weights must sum to a positive value")
    normalized = [weight / total for weight in weight_list]
    bounds: list[float] = []
    running = 0.0
    for weight in normalized:
        running += weight
        bounds.append(running)
    view = scratch_view_name(frame._session, "__repark_rsplit_")
    frame._session.create_or_replace_temp_view(view, frame._plan())
    try:
        order_fields = frame._engine_names if frame._engine_names is not None else frame.columns
        order_sql = ", ".join(_quote_ident_sql(c) for c in order_fields)
        order_clause = f"ORDER BY {order_sql}" if order_sql else ""
        if seed is None:
            bucket_sql = f"SELECT *, random() AS __repark_split_u FROM {view}"
        else:
            seed_expr = str(int(seed))
            bucket_sql = (
                f"SELECT * EXCLUDE (__repark_rn), "
                f"(abs((CAST(__repark_rn AS BIGINT) + {seed_expr}) "
                f"* 1103515245 + 12345) % 1000000) "
                f"/ 1000000.0 AS __repark_split_u FROM ("
                f"  SELECT *, row_number() OVER ({order_clause}) AS __repark_rn FROM {view}"
                f")"
            )
        scored_name = scratch_view_name(frame._session, "__repark_rsplit_s_")
        scored = frame._session.sql(bucket_sql)
        frame._session.create_or_replace_temp_view(scored_name, scored)
        try:
            frames: list[DataFrame] = []
            lower = 0.0
            for index, upper in enumerate(bounds):
                if index == len(bounds) - 1:
                    predicate = f"__repark_split_u >= {lower}"
                else:
                    predicate = f"__repark_split_u >= {lower} AND __repark_split_u < {upper}"
                part = frame._session.sql(
                    f"SELECT * EXCLUDE (__repark_split_u) FROM {scored_name} WHERE {predicate}"
                )
                child = frame._spawn(part)
                if frame._display_names is not None and frame._engine_names is not None:
                    child._display_names = list(frame._display_names)
                    child._engine_names = list(frame._engine_names)
                    child._origin_map = (
                        dict(frame._origin_map) if frame._origin_map is not None else None
                    )
                frames.append(child)
                lower = upper
            return frames
        finally:
            frame._session.drop_temp_view(scored_name)
    finally:
        frame._session.drop_temp_view(view)


def _sample_by(
    frame: DataFrame,
    col: Column | str,
    fractions: dict[Any, float],
    seed: int | None = None,
) -> DataFrame:
    """Stratified sample without replacement (PySpark ``DataFrame.sampleBy``).

    Rows whose stratum key is absent from ``fractions`` are dropped.

    Matches Spark's mechanism: ``rand(seed)`` (XORShiftRandom,
    ``seed + partitionIndex``; repark partitionIndex=0) compared per stratum
    (Spark ``DataFrameStatFunctions.sampleBy`` / ``randomExpressions.Rand``).
    Seeded counts match Spark single-partition layouts (Apache ``test_sampleby``
    band 35-36 at seed=0). Alias of ``stat.sampleBy``.
    """
    from repark.errors import PySparkTypeError
    from repark.spark.column import Column as ReparkColumn
    from repark.spark.functions import col as f_col
    from repark.spark.functions import lit, rand

    if isinstance(col, str):
        stratum = f_col(col)
    elif isinstance(col, ReparkColumn):
        stratum = col
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
        )
    if not isinstance(fractions, dict):
        raise PySparkTypeError(
            errorClass="NOT_DICT",
            messageParameters={"arg_name": "fractions", "arg_type": type(fractions).__name__},
        )
    normalized: dict[Any, float] = {}
    for key, value in fractions.items():
        if isinstance(key, bool) or not isinstance(key, (float, int, str)):
            raise PySparkTypeError(
                errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
                messageParameters={
                    "arg_name": "fractions",
                    "arg_type": type(fractions).__name__,
                    "allowed_types": "float, int, str",
                    "item_type": type(key).__name__,
                },
            )
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise PySparkTypeError(
                errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
                messageParameters={
                    "arg_name": "fractions",
                    "arg_type": type(fractions).__name__,
                    "allowed_types": "float, int",
                    "item_type": type(value).__name__,
                },
            )
        fraction_value = float(value)
        if fraction_value != fraction_value or fraction_value < 0.0 or fraction_value > 1.0:
            raise IllegalArgumentException(
                f"requirement failed: Fraction must be in [0, 1], but got {fraction_value}"
            )
        normalized[key] = fraction_value
    if not normalized:
        return frame.limit(0)
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise PySparkTypeError(
            errorClass="NOT_INT",
            messageParameters={
                "arg_name": "seed",
                "arg_type": type(seed).__name__,
            },
        )
    effective_seed = 0 if seed is None else int(seed)
    rng = rand(effective_seed)
    predicate: Column | None = None
    for key, fraction in normalized.items():
        piece = (stratum == lit(key)) & (rng < lit(fraction))
        predicate = piece if predicate is None else (predicate | piece)
    if predicate is None:
        return frame.limit(0)
    return frame.filter(predicate)
