# W-0 window-shape bench results

Measure-only. Ratios over absolutes on a noisy `schedutil` box (P-2 posture).
The 1e7 wall clock is one host's number, not a CI pin.

## Machine and pins

- scale: `full`
- seed: `42`
- engine: `repark-0.5.0`
- native: `release_or_stripped size_bytes=159402864`
- DuckDB: `1.5.5`
- PySpark: `4.1.2`
- cpu: `AMD_Ryzen_Threadripper_3970X_32-Core_Processor`
- cores: `64`
- governor: `schedutil`
- ram_gib: `125.7`
- process_hwm_rss_bytes: `2859900928`
- wall_seconds: `139.9`
- scratch_deleted: `True`

## Generated dataset sizes (bytes, before delete)

| path | bytes |
|---|---:|
| `/tmp/w0-full/seed_iceberg.parquet` | 10870080 |
| `/tmp/w0-full/seed_memory.parquet` | 21740637 |
| `/tmp/w0-full/seed_probe.parquet` | 29950 |
| `/tmp/w0-full/seed_sliding.parquet` | 10870080 |
| `/tmp/w0-full/seed_unpartitioned.parquet` | 108701433 |
| `/tmp/w0-full/warehouse` | 7147598 |

## Sliding-frame probe (C-002 / C-009)

| name | intake_class | outcome | message |
|---|---|---|---|
| `any` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'any'. Did you mean 'avg'? |
| `any_value` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'any_value'. Did you mean 'last_value'? |
| `approx_count_distinct` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: approx_distinct(datafusion.publ... |
| `approx_percentile` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: approx_percentile_cont(datafusi... |
| `array_agg` | nonretract | ok |  |
| `avg` | retract | ok |  |
| `bit_and` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: bit_and(datafusion.public.t.vi)... |
| `bit_or` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: bit_or(datafusion.public.t.vi) ... |
| `bit_xor` | nonretract | ok |  |
| `bool_and` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: bool_and(datafusion.public.t.vi... |
| `bool_or` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: bool_or(datafusion.public.t.vi ... |
| `collect_list` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: collect_list(datafusion.public.... |
| `collect_set` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: collect_set(datafusion.public.t... |
| `corr` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: corr(datafusion.public.t.v, dat... |
| `count` | retract | ok |  |
| `count_if` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'count_if'. Did you mean 'count'? |
| `covar_pop` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: covar_pop(datafusion.public.t.v... |
| `covar_samp` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: covar_samp(datafusion.public.t.... |
| `every` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'every'. Did you mean 'var'? |
| `first` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'first'. Did you mean 'min'? |
| `first_value` | nonretract | ok |  |
| `kurtosis` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'kurtosis'. Did you mean 'try_sum'? |
| `last` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'last'. Did you mean 'lag'? |
| `last_value` | nonretract | ok |  |
| `max` | retract | ok |  |
| `max_by` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'max_by'. Did you mean 'max'? |
| `mean` | retract | ok |  |
| `median` | nonretract | ok |  |
| `min` | retract | ok |  |
| `min_by` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'min_by'. Did you mean 'min'? |
| `mode` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'mode'. Did you mean 'min'? |
| `percentile` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'percentile'. Did you mean 'percentile_cont'? |
| `percentile_approx` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: approx_percentile_cont(datafusi... |
| `regr_avgx` | nonretract | ok |  |
| `regr_avgy` | nonretract | ok |  |
| `regr_count` | nonretract | ok |  |
| `regr_intercept` | nonretract | ok |  |
| `regr_r2` | nonretract | ok |  |
| `regr_slope` | nonretract | ok |  |
| `regr_sxx` | nonretract | ok |  |
| `regr_sxy` | nonretract | ok |  |
| `regr_syy` | nonretract | ok |  |
| `skewness` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'skewness'. Did you mean 'stddev'? |
| `some` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'some'. Did you mean 'sum'? |
| `std` | retract | absent | AnalysisException: Error during planning: Invalid function 'std'. Did you mean 'sum'? |
| `stddev` | retract | ok |  |
| `stddev_pop` | retract | ok |  |
| `stddev_samp` | retract | ok |  |
| `sum` | retract | ok |  |
| `try_avg` | nonretract | absent | AnalysisException: Error during planning: Invalid function 'try_avg'. Did you mean 'try_sum'? |
| `try_sum` | nonretract | refuse | PySparkException: This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: try_sum(datafusion.public.t.v) ... |
| `var_pop` | retract | ok |  |
| `var_samp` | retract | ok |  |
| `variance` | retract | absent | AnalysisException: Error during planning: Invalid function 'variance'. Did you mean 'median'? |

Refuse count: **13**. Each refuse name is a registry row `WIN-SLIDE-<name>`.

## Method

- `approx_count_distinct` is probed on int64 (`vi`). Float64 fails earlier
  (`approx_distinct` not implemented for Float64) and is not a sliding refuse.
- RSS is a process high-water mark (`ru_maxrss`), reported once per run as
  `process_hwm_rss_bytes`. It is not a per-cell figure.
- Over-`memory_limit` cells that sort before the window die in the upstream
  `SortExec` / FairSpillPool `ExternalSorter`. Window-exec spill behavior is
  **UNMEASURED**.
- Generated scratch is deleted in a `finally` block, including Spark-start abort.
- Unpartitioned `ORDER BY` at full scale is 10_000_000 rows (C-004).
- Iceberg lead/lag is the RePark shape; DuckDB and PySpark run the same SQL
  on in-memory tables.

## Cells

### `sliding_sum` — 1000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 564.9 | 508.0,564.9,639.3 | SortExec WindowAggExec BoundedWindowAggExec SortPreserving |  |
| duckdb | ok | 75.5 | 87.6,73.4,75.5 |  |  |
| pyspark | ok | 2277.1 | 2338.6,2277.1,2247.1 |  |  |

### `sliding_avg` — 1000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT avg(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 794.8 | 719.2,810.0,794.8 | SortExec WindowAggExec BoundedWindowAggExec SortPreserving |  |
| duckdb | ok | 76.8 | 106.2,76.8,74.5 |  |  |
| pyspark | ok | 2989.8 | 3047.4,2973.3,2989.8 |  |  |

### `sliding_min` — 1000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT min(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 654.0 | 654.0,660.8,541.1 | SortExec WindowAggExec BoundedWindowAggExec SortPreserving |  |
| duckdb | ok | 91.9 | 104.1,91.9,77.9 |  |  |
| pyspark | ok | 2192.9 | 2192.9,2249.8,2192.2 |  |  |

### `sliding_max` — 1000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT max(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 660.4 | 670.4,626.7,660.4 | SortExec WindowAggExec BoundedWindowAggExec SortPreserving |  |
| duckdb | ok | 86.7 | 86.6,86.7,87.2 |  |  |
| pyspark | ok | 2176.3 | 2173.8,2176.3,2201.6 |  |  |

### `sliding_count` — 1000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT count(v) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 485.2 | 485.2,506.8,476.9 | SortExec WindowAggExec BoundedWindowAggExec SortPreserving |  |
| duckdb | ok | 79.5 | 90.7,74.9,79.5 |  |  |
| pyspark | ok | 2195.6 | 2152.2,2195.6,2254.9 |  |  |

### `constant_sum` — 1000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT sum(v) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 18.7 | 18.7,23.6,15.1 | WindowAggExec |  |
| duckdb | ok | 10.9 | 19.3,9.4,10.9 |  |  |
| pyspark | ok | 366.1 | 413.1,362.6,366.1 |  |  |

### `unpartitioned_order_by` — 10000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT sum(v) OVER (ORDER BY ts) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 4409.9 | 4321.7,4409.9,4469.1 | SortExec WindowAggExec BoundedWindowAggExec SortPreserving |  |
| duckdb | ok | 367.1 | 359.3,367.7,367.1 |  |  |
| pyspark | ok | 9588.9 | 9388.7,9588.9,10464.4 |  |  |

### `iceberg_lead_lag` — 1000000 rows

```sql
SELECT sum(lag_v) AS s_lag, sum(lead_v) AS s_lead FROM (SELECT lag(v, 1) OVER (ORDER BY ts) AS lag_v, lead(v, 1) OVER (ORDER BY ts) AS lead_v FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | ok | 225.0 | 214.1,225.0,227.5 | SortExec WindowAggExec BoundedWindowAggExec Iceberg |  |
| duckdb | ok | 140.4 | 153.8,134.1,140.4 |  |  |
| pyspark | ok | 665.6 | 665.6,672.1,650.7 |  |  |

### `memory_limit_16M` — 2000000 rows

```sql
SELECT sum(w) AS s FROM (SELECT sum(v) OVER (ORDER BY ts) AS w FROM t)
```

| engine | outcome | median_ms | samples_ms | plan | message |
|---|---|---:|---|---|---|
| repark | oom |  |  | SortExec WindowAggExec BoundedWindowAggExec SortPreserving | PySparkException: Not enough memory to continue external sort. Consider increasing the memory limit config: 'datafusion.runtime.memory_limit', or decreasing the config: 'datafus... |

## Notes

- Over-`memory_limit` records the outcome class; it does not retry a different
  query (C-006 / C-010).

