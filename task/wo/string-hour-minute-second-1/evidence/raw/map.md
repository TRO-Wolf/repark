# task/wo/string-hour-minute-second-1/evidence/raw/ — `EXPLAIN` and cost records

- `main-explain.jsonl`, `spark-explain.jsonl` — `EXPLAIN` of seven queries over the `full` text's Iceberg table in
  `America/Los_Angeles`, ANSI on: main plans `hour(c)` as `hour(CAST(c AS Date32))`, `hour(CAST(c AS TIMESTAMP))` as
  `hour(to_timestamp(c))`, `EXTRACT(HOUR FROM c)` as `date_part('HOUR', __repark_cast_timestamp_ns__(c))`; Spark
  plans `hour(c)` and `hour(CAST(c AS TIMESTAMP))` alike, `hour(cast(c as timestamp), Some(America/Los_Angeles))`.
- `main-cost.json` — `probes/cost.py` on main: two million rows, nine runs per query, the medians, every run and the
  answers.

Up: [../map.md](../map.md).
