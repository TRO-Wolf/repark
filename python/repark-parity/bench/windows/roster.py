"""Sliding-window probe roster and SQL shapes for the W-0 bench.

The roster is the finite domain chartered in ``w-0-window-bench`` C-002 / C-009.
Spark 4.1.2 built-in aggregates used as
``expr OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)``.
"""

from __future__ import annotations

from typing import Final

from pydantic import BaseModel, ConfigDict

FULL_UNPARTITIONED_ROWS: Final[int] = 10_000_000
QUICK_UNPARTITIONED_ROWS: Final[int] = 100_000
GATE_ROWS: Final[int] = 64
SLIDING_TIMED_FULL_ROWS: Final[int] = 1_000_000
SLIDING_TIMED_QUICK_ROWS: Final[int] = 100_000
SLIDING_PROBE_ROWS: Final[int] = 2_000
ICEBERG_FULL_ROWS: Final[int] = 1_000_000
ICEBERG_QUICK_ROWS: Final[int] = 10_000
MEMORY_LIMIT_FULL_ROWS: Final[int] = 2_000_000
MEMORY_LIMIT_QUICK_ROWS: Final[int] = 200_000
MEMORY_LIMIT_SETTING: Final[str] = "16M"
SLIDING_FRAME: Final[str] = "ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW"
TIMED_SLIDING_FRAME: Final[str] = "ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW"
CONSTANT_FRAME: Final[str] = "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
UNPARTITIONED_FRAME: Final[str] = "ORDER BY ts"
LEAD_LAG_SELECT: Final[str] = (
    "lag(v, 1) OVER (ORDER BY ts) AS lag_v, lead(v, 1) OVER (ORDER BY ts) AS lead_v"
)
DEFAULT_SEED: Final[int] = 42
DEFAULT_WARMUP: Final[int] = 1
DEFAULT_ITERATIONS: Final[int] = 3
QUICK_WARMUP: Final[int] = 1
QUICK_ITERATIONS: Final[int] = 2

RETRACT_NAMES: Final[frozenset[str]] = frozenset(
    {
        "avg",
        "count",
        "max",
        "mean",
        "min",
        "std",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "sum",
        "var_pop",
        "var_samp",
        "variance",
    }
)


class ProbeSpec(BaseModel):
    """One Spark aggregate spelling and the SQL expression the probe runs."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    sql_expr: str


def _unary(name: str, expr: str) -> ProbeSpec:
    """Build one unary probe spec."""
    return ProbeSpec(name=name, sql_expr=expr)


PROBE_SPECS: Final[tuple[ProbeSpec, ...]] = (
    _unary("any", "any(vi <> 0)"),
    _unary("any_value", "any_value(v)"),
    _unary("approx_count_distinct", "approx_count_distinct(vi)"),
    _unary("approx_percentile", "approx_percentile(v, 0.5)"),
    _unary("array_agg", "array_agg(v)"),
    _unary("avg", "avg(v)"),
    _unary("bit_and", "bit_and(vi)"),
    _unary("bit_or", "bit_or(vi)"),
    _unary("bit_xor", "bit_xor(vi)"),
    _unary("bool_and", "bool_and(vi <> 0)"),
    _unary("bool_or", "bool_or(vi <> 0)"),
    _unary("collect_list", "collect_list(v)"),
    _unary("collect_set", "collect_set(v)"),
    _unary("corr", "corr(v, v2)"),
    _unary("count", "count(v)"),
    _unary("count_if", "count_if(v > 0.5)"),
    _unary("covar_pop", "covar_pop(v, v2)"),
    _unary("covar_samp", "covar_samp(v, v2)"),
    _unary("every", "every(vi <> 0)"),
    _unary("first", "first(v)"),
    _unary("first_value", "first_value(v)"),
    _unary("kurtosis", "kurtosis(v)"),
    _unary("last", "last(v)"),
    _unary("last_value", "last_value(v)"),
    _unary("max", "max(v)"),
    _unary("max_by", "max_by(v, id)"),
    _unary("mean", "mean(v)"),
    _unary("median", "median(v)"),
    _unary("min", "min(v)"),
    _unary("min_by", "min_by(v, id)"),
    _unary("mode", "mode(v)"),
    _unary("percentile", "percentile(v, 0.5)"),
    _unary("percentile_approx", "percentile_approx(v, 0.5)"),
    _unary("regr_avgx", "regr_avgx(v, v2)"),
    _unary("regr_avgy", "regr_avgy(v, v2)"),
    _unary("regr_count", "regr_count(v, v2)"),
    _unary("regr_intercept", "regr_intercept(v, v2)"),
    _unary("regr_r2", "regr_r2(v, v2)"),
    _unary("regr_slope", "regr_slope(v, v2)"),
    _unary("regr_sxx", "regr_sxx(v, v2)"),
    _unary("regr_sxy", "regr_sxy(v, v2)"),
    _unary("regr_syy", "regr_syy(v, v2)"),
    _unary("skewness", "skewness(v)"),
    _unary("some", "some(vi <> 0)"),
    _unary("std", "std(v)"),
    _unary("stddev", "stddev(v)"),
    _unary("stddev_pop", "stddev_pop(v)"),
    _unary("stddev_samp", "stddev_samp(v)"),
    _unary("sum", "sum(v)"),
    _unary("try_avg", "try_avg(v)"),
    _unary("try_sum", "try_sum(v)"),
    _unary("var_pop", "var_pop(v)"),
    _unary("var_samp", "var_samp(v)"),
    _unary("variance", "variance(v)"),
)

PROBE_NAMES: Final[tuple[str, ...]] = tuple(spec.name for spec in PROBE_SPECS)

TIMED_SLIDING_NAMES: Final[tuple[str, ...]] = ("sum", "avg", "min", "max", "count")

RESCANNED_SLIDING_NAMES: Final[tuple[str, ...]] = (
    "approx_count_distinct",
    "approx_percentile",
    "bit_and",
    "bit_or",
    "bool_and",
    "bool_or",
    "collect_list",
    "collect_set",
    "corr",
    "covar_pop",
    "covar_samp",
    "percentile_approx",
    "try_sum",
)

REFUSING_SLIDING_NAMES: Final[tuple[str, ...]] = ()

ABSENT_PLANNING_NAMES: Final[tuple[str, ...]] = (
    "any",
    "any_value",
    "count_if",
    "every",
    "first",
    "kurtosis",
    "last",
    "max_by",
    "min_by",
    "mode",
    "percentile",
    "skewness",
    "some",
    "std",
    "variance",
)


def spec_by_name(name: str) -> ProbeSpec:
    """Return the probe spec for ``name``.

    Args:
        name: roster name.

    Returns:
        The matching :class:`ProbeSpec`.

    Raises:
        KeyError: ``name`` is not on the roster.
    """
    for spec in PROBE_SPECS:
        if spec.name == name:
            return spec
    raise KeyError(name)


def sliding_select(sql_expr: str, *, frame: str = SLIDING_FRAME) -> str:
    """Bare sliding-frame window over ``t``.

    Do not wrap in ``count(*)``: DataFusion elides the window and the probe
    never hits the sliding accumulator.

    Args:
        sql_expr: aggregate expression, for example ``sum(v)``.
        frame: window-frame clause after ``OVER``.

    Returns:
        ``SELECT <expr> OVER (...) AS w FROM t``.
    """
    return f"SELECT {sql_expr} OVER ({frame}) AS w FROM t"


def sliding_sum_select(sql_expr: str, *, frame: str = SLIDING_FRAME) -> str:
    """Numeric sink over a window so 1e7 rows are not shipped to Python.

    The outer ``sum(w)`` still requires every window value, so the operator
    runs. ``count(*)`` does not — it collapses to a constant.

    Args:
        sql_expr: numeric aggregate expression.
        frame: window-frame clause after ``OVER``.

    Returns:
        ``SELECT sum(w) FROM (SELECT <expr> OVER (...) AS w FROM t)``.
    """
    return f"SELECT sum(w) AS s FROM ({sliding_select(sql_expr, frame=frame)})"


def constant_select(sql_expr: str = "sum(v)") -> str:
    """Constant-frame window over ``t`` (unbounded / unbounded).

    Args:
        sql_expr: aggregate expression.

    Returns:
        A sum-sink SELECT.
    """
    return sliding_sum_select(sql_expr, frame=CONSTANT_FRAME)


def unpartitioned_select(sql_expr: str = "sum(v)") -> str:
    """Unpartitioned ``ORDER BY ts`` window over ``t``.

    Args:
        sql_expr: aggregate expression.

    Returns:
        A sum-sink SELECT.
    """
    return sliding_sum_select(sql_expr, frame=UNPARTITIONED_FRAME)


def lead_lag_select() -> str:
    """``lead`` / ``lag`` over ``ORDER BY ts``.

    Returns:
        A sum-sink SELECT of both functions so the window cannot be elided.
    """
    return (
        f"SELECT sum(lag_v) AS s_lag, sum(lead_v) AS s_lead FROM (SELECT {LEAD_LAG_SELECT} FROM t)"
    )


def retract_class(name: str) -> str:
    """Intake grouping: ``retract`` or ``nonretract``.

    Args:
        name: roster name.

    Returns:
        ``retract`` if the intake named this aggregate as retract-capable.
    """
    return "retract" if name in RETRACT_NAMES else "nonretract"
