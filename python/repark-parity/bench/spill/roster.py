"""The H3-SPILL-1 operator roster: one row per operator the engine can plan."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

BASE_COLUMNS: tuple[str, ...] = (
    "id",
    "md5(cast(id as string)) AS h",
    "id % 1024 AS g",
    "concat(md5(cast(id as string)), md5(cast(id + 1 as string))) AS payload",
    "cast(id as double) * 1.5 AS v",
)

POOLS: tuple[str, ...] = ("none", "8G", "1G", "256M", "64M")
SCALES: tuple[int, ...] = (1_000_000, 10_000_000)


class OperatorSpec(BaseModel):
    """One matrix row: the query, the physical node it is about, and its answer probe."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operator: str
    kind: str = "sql"
    sql: str = ""
    focus: str = ""
    digest_sql: str | None = None
    digest_kind: str = "engine_checksum"
    conf: dict[str, str] = Field(default_factory=dict)
    right_rows: int | None = None
    api: str | None = None
    note: str = ""


CHECKSUM_COLUMNS: str = (
    "count(*) AS n, sum(crc32(cast(id as string))) AS c_id, "
    "sum(crc32(cast(g as string))) AS c_g, sum(crc32(cast(v as string))) AS c_v, "
    "sum(crc32(h)) AS c_h, sum(crc32(payload)) AS c_payload"
)


_SORT = "SELECT id, h FROM base ORDER BY h"
_TOPK = "SELECT id, h FROM base ORDER BY h LIMIT 100"
_AGG_MANY = "SELECT h, count(*) AS c FROM base GROUP BY h"
_AGG_FEW = "SELECT g, count(*) AS c, sum(v) AS s FROM base GROUP BY g"
_DISTINCT = "SELECT DISTINCT h FROM base"
_ARRAY_AGG = "SELECT g % 8 AS k, array_agg(h) AS a FROM base GROUP BY g % 8"
_HASH_JOIN = "SELECT l.id, r.payload FROM base l JOIN other r ON l.h = r.h"
_NLJ = "SELECT l.id, r.v FROM base l JOIN other r ON l.v < r.v"
_WIN_UNBOUNDED = "SELECT id, sum(v) OVER (PARTITION BY g) AS s FROM base"
_WIN_SLIDING = (
    "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS s FROM base"
)
_WIN_RANGE = (
    "SELECT id, sum(v) OVER (ORDER BY id RANGE BETWEEN 1000 PRECEDING AND CURRENT ROW) AS s "
    "FROM base"
)

ROSTER: tuple[OperatorSpec, ...] = (
    OperatorSpec(
        operator="sort",
        sql=_SORT,
        focus="SortExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(CASE WHEN prev IS NOT NULL AND prev > h THEN 1 ELSE 0 END) "
            "AS inversions, min(h) AS lo, max(h) AS hi FROM "
            "(SELECT h, lag(h) OVER (ORDER BY h) AS prev FROM base)"
        ),
        note="ExternalSorter: the reference spilling operator",
    ),
    OperatorSpec(
        operator="topk",
        sql=_TOPK,
        focus="SortExec",
        digest_sql=(
            "SELECT count(*) AS n, min(h) AS lo, max(h) AS hi FROM "
            "(SELECT h FROM base ORDER BY h LIMIT 100)"
        ),
        note="TopK: bounded heap, must never need the pool",
    ),
    OperatorSpec(
        operator="hash_agg_many_groups",
        sql=_AGG_MANY,
        focus="AggregateExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(c) AS total, max(c) AS mx, sum(crc32(h)) AS c_h FROM "
            "(SELECT h, count(*) AS c FROM base GROUP BY h)"
        ),
        note="one group per row: partial and final both under pressure",
    ),
    OperatorSpec(
        operator="hash_agg_few_groups",
        sql=_AGG_FEW,
        focus="AggregateExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(c) AS total, sum(cast(s as bigint)) AS vsum FROM "
            "(SELECT g, count(*) AS c, sum(v) AS s FROM base GROUP BY g)"
        ),
        note="1024 groups: the partial aggregate should hold",
    ),
    OperatorSpec(
        operator="distinct",
        sql=_DISTINCT,
        focus="AggregateExec",
        digest_sql=(
            "SELECT count(*) AS n, min(h) AS lo, max(h) AS hi FROM (SELECT DISTINCT h FROM base)"
        ),
    ),
    OperatorSpec(
        operator="collect_list",
        sql=_ARRAY_AGG,
        focus="AggregateExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(cardinality(a)) AS total FROM "
            "(SELECT g % 8 AS k, array_agg(h) AS a FROM base GROUP BY g % 8)"
        ),
        note="eight unbounded accumulators; array_agg has no spill path",
    ),
    OperatorSpec(
        operator="hash_join",
        sql=_HASH_JOIN,
        focus="HashJoinExec",
        right_rows=1_000_000,
        digest_sql=("SELECT count(*) AS n, sum(l.id) AS s FROM base l JOIN other r ON l.h = r.h"),
        note="build side collects in memory; DataFusion 54.1 has no hash-join spill",
    ),
    OperatorSpec(
        operator="sort_merge_join",
        sql=_HASH_JOIN,
        focus="SortMergeJoinExec",
        right_rows=1_000_000,
        conf={"datafusion.optimizer.prefer_hash_join": "false"},
        digest_sql=("SELECT count(*) AS n, sum(l.id) AS s FROM base l JOIN other r ON l.h = r.h"),
        note="the spilling join",
    ),
    OperatorSpec(
        operator="nested_loop_join",
        sql=_NLJ,
        focus="NestedLoopJoinExec",
        right_rows=64,
        digest_sql="SELECT count(*) AS n FROM base l JOIN other r ON l.v < r.v",
        note="left side buffered whole; right side held to 64 rows so the cell terminates",
    ),
    OperatorSpec(
        operator="window_unbounded",
        sql=_WIN_UNBOUNDED,
        focus="WindowAggExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(cast(s as bigint)) AS total FROM "
            "(SELECT id, sum(v) OVER (PARTITION BY g) AS s FROM base)"
        ),
    ),
    OperatorSpec(
        operator="window_sliding_rows",
        sql=_WIN_SLIDING,
        focus="BoundedWindowAggExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(cast(s as bigint)) AS total FROM "
            "(SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) "
            "AS s FROM base)"
        ),
    ),
    OperatorSpec(
        operator="window_range",
        sql=_WIN_RANGE,
        focus="BoundedWindowAggExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(cast(s as bigint)) AS total FROM "
            "(SELECT id, sum(v) OVER (ORDER BY id RANGE BETWEEN 1000 PRECEDING AND CURRENT ROW) "
            "AS s FROM base)"
        ),
    ),
    OperatorSpec(
        operator="repartition",
        sql=_AGG_MANY,
        focus="RepartitionExec",
        digest_sql=(
            "SELECT count(*) AS n, sum(c) AS total, max(c) AS mx, sum(crc32(h)) AS c_h FROM "
            "(SELECT h, count(*) AS c FROM base GROUP BY h)"
        ),
        note="read off the many-group plan: RepartitionExec is never a whole plan",
    ),
    OperatorSpec(
        operator="dynamic_flatten",
        kind="api",
        digest_kind="engine_checksum_over_flattened_frame",
        api="dynamic_flatten",
        focus="UnnestExec",
    ),
    OperatorSpec(
        operator="iceberg_scan_dv",
        kind="api",
        digest_kind="engine_checksum_over_scanned_table",
        api="iceberg_scan_dv",
        focus="IcebergScan",
    ),
    OperatorSpec(
        operator="merge_staging",
        kind="api",
        digest_kind="engine_checksum_over_merged_table",
        api="merge_staging",
        focus="MergeWrite",
    ),
    OperatorSpec(
        operator="collect",
        kind="api",
        digest_kind="python_row_crc32_sum_and_xor",
        api="collect",
        focus="facade boundary",
    ),
    OperatorSpec(
        operator="to_pandas",
        kind="api",
        digest_kind="pandas_hash_pandas_object_sum",
        api="to_pandas",
        focus="facade boundary",
    ),
)


def spec_for(operator: str) -> OperatorSpec:
    """Return the roster row named `operator`."""
    for spec in ROSTER:
        if spec.operator == operator:
            return spec
    raise KeyError(operator)
