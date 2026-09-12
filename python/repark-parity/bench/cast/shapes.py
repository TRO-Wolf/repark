"""SQL builders for the three PERF-CAST-1 CAST plan shapes."""

from __future__ import annotations

SHAPES: tuple[str, ...] = (
    "standalone",
    "over_aggregate",
    "projection_over_aggregate",
)
CAST_COUNTS: tuple[int, ...] = (50, 250, 2500)
ROW_COUNT: int = 200_000
SOURCE_VIEW: str = "cast_src"


def projection_items(cast_count: int, inner: str) -> str:
    """Comma-separated unique CAST((inner + i) AS VARCHAR) AS ci items."""
    return ", ".join(
        f"CAST(({inner} + {index}) AS VARCHAR) AS c{index}" for index in range(cast_count)
    )


def standalone_sql(cast_count: int, source: str = SOURCE_VIEW) -> str:
    """A projection of CAST expressions over the 200k-row source."""
    return f"SELECT {projection_items(cast_count, 'id')} FROM {source}"


def over_aggregate_sql(cast_count: int, source: str = SOURCE_VIEW) -> str:
    """CAST wrappers on a wide aggregate of the 200k-row source."""
    items = ", ".join(
        f"CAST(sum(id + {index}) AS VARCHAR) AS c{index}" for index in range(cast_count)
    )
    return f"SELECT {items} FROM {source}"


def projection_over_aggregate_sql(cast_count: int, source: str = SOURCE_VIEW) -> str:
    """CAST projection over a one-row aggregate of the 200k-row source."""
    inner = f"SELECT sum(id) AS total FROM {source}"
    return f"SELECT {projection_items(cast_count, 'total')} FROM ({inner})"


def sql_for_shape(shape: str, cast_count: int, source: str = SOURCE_VIEW) -> str:
    """Return the SQL string for one named plan shape."""
    builders = {
        "standalone": standalone_sql,
        "over_aggregate": over_aggregate_sql,
        "projection_over_aggregate": projection_over_aggregate_sql,
    }
    builder = builders.get(shape)
    if builder is None:
        known = ", ".join(SHAPES)
        raise ValueError(f"unknown CAST plan shape {shape!r}; choose from {known}")
    return builder(cast_count, source)


def cast_token_count(sql: str) -> int:
    """Count CAST( tokens in a SQL string."""
    return sql.count("CAST(")
