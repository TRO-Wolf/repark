# Aggregate baseline — grouped `avg` (PERF-AGG-AVG-1)

Scope: the Spark `avg` / `try_avg` UDAF on grouped aggregation after PERF-AGG-AVG-1
gave it a `GroupsAccumulator` (PERF-ANALYSIS-1 §2 row 10, slate item 8). Before: the
UDAF implemented `accumulator()` only, so DataFusion boxed one accumulator per group;
`avg(l_quantity) GROUP BY l_partkey` (200 k groups, 6 M rows) cost 389 ms where `sum`
cost 88 ms, and TPC-H Q17 ran 11.9× DuckDB with `elapsed_compute=2.46 s` in the
partial `avg`. After: per-group `Vec<u64>` counts plus `Vec<Native>` sums over
Float64 and Decimal32/64/128/256, `try_avg` overflow on the 2×-MAX shape →
per-group NULL (the i128 sum-wrap shape is BACKLOG row `AVG-DEC-SUMWRAP-1` in
`docs/spark-sql-iceberg-parity.md`), the retract path untouched for window frames.
The `sum` leg beside every `avg` leg is the control:
it runs the same grouping through DataFusion's own groups accumulator, so the ratio
is the avg-specific cost with scan, grouping and load divided out.

Machine/profile (2026-09-05): this box, 8-thread parity
(`spark.sql.shuffle.partitions = 8` → `target_partitions = 8`), release module
(`__debug_assertions__ False`), TPC-H SF1 shared cache `~/.cache/repark-tpch/sf1`,
DuckDB 1.5.5 `PRAGMA threads=8` on the same box. Loads 12–17 (shared lane box);
every cell is median-of-5 after 1 warm-up unless noted.

| cell | before (base `6eaccd5e`, B 163,478,728) | after (tip, B 163,720,360) | DuckDB same box |
|---|---:|---:|---:|
| `decimal/sf1/avg_decimal_by_partkey/tp8` | 400.6 (spread 54.5) | 99.0–113.8 (floor 4.7) | 102.1 (spread 7.1) |
| `decimal/sf1/avg_double_by_partkey/tp8` | 433.3 (spread 66.0) | 98.8 (spread 10.2) | 88.7 (spread 5.0) |
| `decimal/sf1/sum_decimal_by_partkey/tp8` | 90.0 (spread 26.1) | 85.9–89.7 (floor 4.7) | 114.7 (spread 6.0) |
| avg/sum ratio | **4.45×** | **1.10–1.28×** (target ≤ 1.3×: met) | — |
| `tpch/sf1/q17/tp8` (`run_tpch.py --sf 1 --repeats 3`) | 0.521–0.721 s (13.8–18.3×) | 0.143–0.355 s (3.6–8.3×) | 0.036–0.043 s |
| committed probe (`test_many_groups_avg_costs_like_sum`, 1 partition) | 4.06× (bound 2.5: red) | 1.21× (bound 2.5: green) | — |

Notes. Isolated avg cost (avg − sum): 310.6 ms = 94× the 3.3 ms floor before,
10–25 ms = 2–5× the 4.7 ms floor after. DuckDB by-partkey legs use `to_arrow_table`,
not `fetchall` — materializing 2e5 Python decimals costs 480 ms and is not the engine
(the `fetchall` numbers were 182.7 / 596.5 and are discarded for that reason).
Q17 after ran four repeats-3 boards (0.143, 0.146, 0.216, 0.355 s) plus one repeats-5
median (0.268 s); the 1.89× by-partkey sample in §8 is the same scheduling noise, one
leg of a pair spiking while its sibling stays flat. Q17 ≤ 3× DuckDB is NOT met, and
no avg-only fix can meet it: `sum` on the same grouping already costs 82.6–89.7 ms
(2.2× DuckDB's whole-Q17 38 ms), so even a free `avg` lands at 2.2× before the join —
the residue is scan/grouping/join efficiency, PERF-ANALYSIS-1 candidate-5 territory.
`EXPLAIN ANALYZE` after the fix: partial `avg` `elapsed_compute` 555–558 ms summed
over 8 partitions, final 69–81 ms, the rest of the plan in microseconds.
Grouped float `avg` changes bit-for-bit vs the base: per-element sequential
summation replaces Arrow's lane-chunked `sum` kernel. On one group of
`[1e16, 1.0×64]` repark answers `153846153846153.84` where Spark answers
`153846153846154.34` (3.3e-15 relative, inside the 1e-12 the pin asserts) —
disclosed as registry row `FLOAT-AGG-3`. The many-groups fixture's exact-binary
values are unaffected.

Reproduce (from the repo root, release module, 8-thread parity):

```
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop --release   # from python/repark
.venv/bin/python - <<'EOF'
import time, statistics
from repark.spark import SparkSession
from pathlib import Path
spark = SparkSession.builder.appName("aggavg").config("spark.sql.shuffle.partitions", "8").getOrCreate()
cache = Path.home() / ".cache" / "repark-tpch" / "sf1"
spark.read.parquet(str(cache / "lineitem.parquet")).createOrReplaceTempView("lineitem")
def med(sql):
    spark.sql(sql).toArrow()
    s = []
    for _ in range(5):
        t = time.perf_counter(); spark.sql(sql).toArrow(); s.append((time.perf_counter() - t) * 1000.0)
    return statistics.median(s)
a = med("SELECT l_partkey, avg(l_quantity) AS a FROM lineitem GROUP BY l_partkey")
s = med("SELECT l_partkey, sum(l_quantity) AS a FROM lineitem GROUP BY l_partkey")
print(f"avg={a:.1f} sum={s:.1f} ratio={a / s:.2f}")
EOF
.venv/bin/python python/repark-parity/bench/tpch/run_tpch.py --sf 1 --repeats 3 --queries 17
.venv/bin/python -m pytest python/repark/tests/test_perf_agg_avg_1.py -q
```

The last line is the durable pin: `test_many_groups_avg_costs_like_sum` re-measures
the ratio (bound 2.5, single partition) on every run, and the file's 24 answer pins
plus 3 round-2 behavior pins and 7 live legs hold the Spark-equal answers the
accumulator must keep.
