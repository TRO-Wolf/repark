"""H3-SPILL-1 Never-OOM pins: a bounded pool spills, degrades or refuses; never aborts or lies."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys

import pytest

import repark.spark.session as session_module
from repark import ReparkSession
from repark.errors import PySparkException

_BASE_COLUMNS: tuple[str, ...] = (
    "id",
    "md5(cast(id as string)) AS h",
    "id % 1024 AS g",
    "concat(md5(cast(id as string)), md5(cast(id + 1 as string))) AS payload",
    "cast(id as double) * 1.5 AS v",
)

_SPILL_COUNT = re.compile(r"spill_count=(\d+)")

_FITS_ROWS = 400_000
_SPILL_ROWS = 1_000_000

_ANALYZE: dict[str, str] = {
    "sort": "SELECT id, h FROM base ORDER BY h",
    "topk": "SELECT id, h FROM base ORDER BY h LIMIT 100",
    "hash_agg_many_groups": "SELECT h, count(*) AS c FROM base GROUP BY h",
    "hash_agg_few_groups": "SELECT g, count(*) AS c, sum(v) AS s FROM base GROUP BY g",
    "distinct": "SELECT DISTINCT h FROM base",
    "collect_list": "SELECT g % 8 AS k, array_agg(h) AS a FROM base GROUP BY g % 8",
    "hash_join": "SELECT l.id, r.payload FROM base l JOIN other r ON l.h = r.h",
    "sort_merge_join": "SELECT l.id, r.payload FROM base l JOIN other r ON l.h = r.h",
    "window_unbounded": "SELECT id, sum(v) OVER (PARTITION BY g) AS s FROM base",
    "window_sliding_rows": "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING "
    "AND CURRENT ROW) AS s FROM base",
    "window_range": "SELECT id, sum(v) OVER (ORDER BY id RANGE BETWEEN 1000 PRECEDING "
    "AND CURRENT ROW) AS s FROM base",
}

_DIGEST: dict[str, str] = {
    "sort": "SELECT count(*) AS n, sum(CASE WHEN prev IS NOT NULL AND prev > h THEN 1 ELSE 0 END) "
    "AS inversions, min(h) AS lo, max(h) AS hi FROM "
    "(SELECT h, lag(h) OVER (ORDER BY h) AS prev FROM base)",
    "topk": "SELECT count(*) AS n, min(h) AS lo, max(h) AS hi FROM "
    "(SELECT h FROM base ORDER BY h LIMIT 100)",
    "hash_agg_many_groups": "SELECT count(*) AS n, sum(c) AS total, max(c) AS mx FROM "
    "(SELECT h, count(*) AS c FROM base GROUP BY h)",
    "hash_agg_few_groups": "SELECT count(*) AS n, sum(c) AS total, sum(cast(s as bigint)) AS vsum "
    "FROM (SELECT g, count(*) AS c, sum(v) AS s FROM base GROUP BY g)",
    "distinct": "SELECT count(*) AS n, min(h) AS lo, max(h) AS hi FROM "
    "(SELECT DISTINCT h FROM base)",
    "collect_list": "SELECT count(*) AS n, sum(cardinality(a)) AS total FROM "
    "(SELECT g % 8 AS k, array_agg(h) AS a FROM base GROUP BY g % 8)",
    "hash_join": "SELECT count(*) AS n, sum(l.id) AS s FROM base l JOIN other r ON l.h = r.h",
    "sort_merge_join": "SELECT count(*) AS n, sum(l.id) AS s FROM base l JOIN other r ON l.h = r.h",
    "window_unbounded": "SELECT count(*) AS n, sum(cast(s as bigint)) AS total FROM "
    "(SELECT id, sum(v) OVER (PARTITION BY g) AS s FROM base)",
    "window_sliding_rows": "SELECT count(*) AS n, sum(cast(s as bigint)) AS total FROM "
    "(SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS s "
    "FROM base)",
    "window_range": "SELECT count(*) AS n, sum(cast(s as bigint)) AS total FROM "
    "(SELECT id, sum(v) OVER (ORDER BY id RANGE BETWEEN 1000 PRECEDING AND CURRENT ROW) AS s "
    "FROM base)",
}

_JOIN_OPERATORS: frozenset[str] = frozenset({"hash_join", "sort_merge_join"})
_SMJ_CONF: dict[str, str] = {"datafusion.optimizer.prefer_hash_join": "false"}

_SPILLING_CELLS: tuple[tuple[str, str], ...] = (
    ("hash_agg_many_groups", "256M"),
    ("distinct", "256M"),
    ("collect_list", "256M"),
)

_FITTING_CELLS: tuple[tuple[str, str], ...] = (
    ("topk", "64M"),
    ("hash_agg_few_groups", "64M"),
    ("window_sliding_rows", "64M"),
    ("window_range", "64M"),
    ("hash_join", "256M"),
)

_REFUSING_CELLS: tuple[tuple[str, str], ...] = (
    ("sort", "64M"),
    ("hash_agg_many_groups", "64M"),
    ("distinct", "64M"),
    ("collect_list", "64M"),
    ("hash_join", "64M"),
    ("sort_merge_join", "64M"),
    ("window_unbounded", "64M"),
)


def _clear_active() -> None:
    session_module._active_session = None


@pytest.fixture(autouse=True)
def _isolate_session():
    _clear_active()
    yield
    _clear_active()


def _session(pool: str, operator: str) -> ReparkSession:
    _clear_active()
    builder = ReparkSession.builder.appName("h3-spill-pin")
    builder = builder.config("datafusion.runtime.memory_limit", "0" if pool == "none" else pool)
    builder = builder.config("datafusion.execution.target_partitions", "4")
    if operator == "sort_merge_join":
        for key, value in _SMJ_CONF.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()


def _register(spark: ReparkSession, operator: str, rows: int) -> None:
    spark.range(rows).selectExpr(*_BASE_COLUMNS).createOrReplaceTempView("base")
    if operator in _JOIN_OPERATORS:
        spark.range(rows).selectExpr(*_BASE_COLUMNS).createOrReplaceTempView("other")


def _digest(spark: ReparkSession, operator: str) -> str:
    table = spark.sql(_DIGEST[operator]).to_arrow()
    payload = json.dumps(
        {"columns": list(table.column_names), "rows": table.to_pylist()},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _answer(operator: str, pool: str, rows: int) -> str:
    spark = _session(pool, operator)
    _register(spark, operator, rows)
    return _digest(spark, operator)


def _max_spill_count(operator: str, pool: str, rows: int) -> int:
    spark = _session(pool, operator)
    _register(spark, operator, rows)
    rows_out = spark.sql(f"EXPLAIN ANALYZE {_ANALYZE[operator]}").collect()
    text = "\n".join(str(row) for row in rows_out)
    counts = [int(match.group(1)) for match in _SPILL_COUNT.finditer(text)]
    return max(counts, default=0)


@pytest.mark.parametrize(("operator", "pool"), _SPILLING_CELLS)
def test_a_spilling_operator_spills_and_still_answers_exactly(operator: str, pool: str) -> None:
    assert _max_spill_count(operator, pool, _SPILL_ROWS) > 0
    assert _answer(operator, pool, _SPILL_ROWS) == _answer(operator, "none", _SPILL_ROWS)


@pytest.mark.parametrize(("operator", "pool"), _FITTING_CELLS)
def test_a_bounded_pool_that_fits_answers_exactly(operator: str, pool: str) -> None:
    assert _answer(operator, pool, _FITS_ROWS) == _answer(operator, "none", _FITS_ROWS)


@pytest.mark.parametrize(("operator", "pool"), _REFUSING_CELLS)
def test_a_pool_refusal_is_the_documented_spark_shaped_exception(operator: str, pool: str) -> None:
    spark = _session(pool, operator)
    _register(spark, operator, _SPILL_ROWS)
    with pytest.raises(PySparkException) as raised:
        spark.sql(_ANALYZE[operator]).to_arrow()
    message = str(raised.value)
    lowered = message.lower()
    assert "memory" in lowered or "resources exhausted" in lowered, message
    assert "fair(" in lowered, message
    assert "greedy(" not in lowered, message
    assert "repark.memory.limit.gb" in message, message
    assert "datafusion.runtime.memory_limit" in message, message
    assert "a Rust panic was caught" not in message, message


def test_a_refusal_leaves_the_session_usable() -> None:
    spark = _session("64M", "sort")
    _register(spark, "sort", _SPILL_ROWS)
    with pytest.raises(PySparkException):
        spark.sql(_ANALYZE["sort"]).to_arrow()
    survivor = spark.sql("SELECT count(*) AS n FROM base").to_arrow().to_pylist()
    assert survivor == [{"n": _SPILL_ROWS}]


_WORKER = """
import json, resource, sys


def vm_size_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmSize:"):
            return int(line.split()[1]) * 1024
    return 0


def peak_rss_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    return 0


mode, pool, rows, headroom = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
partitions = sys.argv[5]
offset = int(sys.argv[6]) if len(sys.argv) > 6 else 0
from repark import ReparkSession

spark = (
    ReparkSession.builder.appName("h3-spill-worker")
    .config("datafusion.runtime.memory_limit", "0" if pool == "none" else pool)
    .config("datafusion.execution.target_partitions", partitions)
    .getOrCreate()
)
columns = [
    "id",
    "md5(cast(id as string)) AS h",
    "id % 1024 AS g",
    "concat(md5(cast(id as string)), md5(cast(id + 1 as string))) AS payload",
    "cast(id as double) * 1.5 AS v",
]
out = {"mode": mode, "pool": pool, "rows": rows}
try:
    if mode == "collect_under_a_ceiling":
        spark.range(8).selectExpr(*columns).createOrReplaceTempView("warm")
        spark.sql("SELECT count(*) FROM warm").collect()
        frame = spark.range(rows).selectExpr(*columns)
        ceiling = vm_size_bytes() + headroom
        out["ceiling_bytes"] = ceiling
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
        out["rows_out"] = len(frame.collect())
    else:
        spark.range(offset, offset + rows).selectExpr(*columns).createOrReplaceTempView("base")
        if mode == "aggregate":
            spark.sql("EXPLAIN ANALYZE SELECT h, count(*) AS c FROM base GROUP BY h").collect()
        elif mode == "to_pandas":
            frame = spark.sql("SELECT * FROM base").toPandas()
            out["rows_out"] = int(frame.shape[0])
            from pandas.util import hash_pandas_object

            digest = 0
            for start in range(0, max(int(frame.shape[0]), 1), 100_000):
                chunk = frame.iloc[start : start + 100_000]
                if chunk.shape[0] == 0:
                    continue
                hashed = hash_pandas_object(chunk, index=False).to_numpy(dtype="uint64")
                digest = (digest + int(hashed.sum(dtype="uint64"))) & 0xFFFFFFFFFFFFFFFF
            out["digest"] = str(frame.shape[0]) + ":" + str(digest)
        elif mode == "nested_loop_join":
            spark.range(64).selectExpr(*columns).createOrReplaceTempView("other")
            spark.sql(
                "EXPLAIN ANALYZE SELECT l.id, r.v FROM base l JOIN other r ON l.v < r.v"
            ).collect()
    out["outcome"] = "ok"
except BaseException as error:
    out["outcome"] = "error"
    out["message"] = (type(error).__name__ + ": " + str(error))[:400]
out["peak_rss_bytes"] = peak_rss_bytes()
print(json.dumps(out))
"""


def _run_worker(
    mode: str, pool: str, rows: int, headroom: int = 0, partitions: int = 4, offset: int = 0
) -> dict:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _WORKER,
            mode,
            pool,
            str(rows),
            str(headroom),
            str(partitions),
            str(offset),
        ],
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    tail = completed.stdout.strip().splitlines()
    assert tail, completed.stderr[-2000:]
    return json.loads(tail[-1])


def test_spilling_holds_resident_memory_far_below_the_unbounded_run() -> None:
    bounded = _run_worker("aggregate", "256M", 4_000_000)
    unbounded = _run_worker("aggregate", "none", 4_000_000)
    assert bounded["outcome"] == "ok", bounded
    assert unbounded["outcome"] == "ok", unbounded
    assert bounded["peak_rss_bytes"] < unbounded["peak_rss_bytes"] - 200 * 1024 * 1024
    assert bounded["peak_rss_bytes"] < 3 * 256 * 1024 * 1024


def test_the_pool_does_not_bound_the_facade_boundary() -> None:
    result = _run_worker("to_pandas", "64M", 2_000_000)
    assert result["outcome"] == "ok", result
    assert result["rows_out"] == 2_000_000
    assert result["peak_rss_bytes"] > 6 * 64 * 1024 * 1024, result


def test_the_facade_boundary_answers_the_same_at_every_pool() -> None:
    bounded = _run_worker("to_pandas", "64M", 400_000)
    unbounded = _run_worker("to_pandas", "none", 400_000)
    assert bounded["outcome"] == "ok", bounded
    assert unbounded["outcome"] == "ok", unbounded
    assert bounded["digest"], bounded
    assert bounded["digest"] == unbounded["digest"], (bounded, unbounded)


def test_the_boundary_digest_is_order_independent_and_content_sensitive() -> None:
    four = _run_worker("to_pandas", "none", 400_000, partitions=4)
    one = _run_worker("to_pandas", "none", 400_000, partitions=1)
    shorter = _run_worker("to_pandas", "none", 399_999, partitions=4)
    shifted = _run_worker("to_pandas", "none", 400_000, partitions=4, offset=1)
    assert four["digest"] == one["digest"], (four, one)
    assert four["digest"] != shorter["digest"], (four, shorter)
    assert four["digest"] != shifted["digest"], (four, shifted)
    assert four["rows_out"] == shifted["rows_out"], (four, shifted)


def test_h3_spill_nlj_1_a_tight_pool_refuses_a_nested_loop_join_with_the_typed_exception() -> None:
    result = _run_worker("nested_loop_join", "8M", 1_000_000)
    assert result["outcome"] == "error", result
    message = result["message"]
    lowered = message.lower()
    assert "memory" in lowered or "resources exhausted" in lowered, result
    assert "fair(" in lowered, result
    assert "greedy(" not in lowered, result
    assert "repark.memory.limit.gb" in message, result
    assert "datafusion.runtime.memory_limit" in message, result
    assert "a Rust panic was caught" not in message, result
    assert "partition not used yet" not in message, result
    control = _run_worker("nested_loop_join", "1G", 1_000_000)
    assert control["outcome"] == "ok", control


def test_h3_spill_collect_1_an_address_space_ceiling_makes_collect_raise_memory_error() -> None:
    result = _run_worker("collect_under_a_ceiling", "none", 4_000_000, 256 * 1024 * 1024)
    assert result["outcome"] == "error", result
    message = result["message"]
    assert message.startswith("MemoryError"), result
    assert "a Rust panic was caught" not in message, result
    assert "PyObject pointer is null" not in message, result
    control = _run_worker("collect_under_a_ceiling", "none", 4_000_000, 6 * 1024**3)
    assert control["outcome"] == "ok", control
    assert control["rows_out"] == 4_000_000
