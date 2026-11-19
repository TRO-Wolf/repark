# task/wo/string-hour-minute-second-1/evidence/built/ — the build's probes and records (seat A)

Comment-free Python; every path is an argument. The raw runs (both packages' 75,032-cell tables, the sweeps, the pin
runs, the cost samples) stay in `N/string-hms-out/` (`sweep/base`, `sweep/head`, `pins/`, `rustprobe/`) and
`S/ddu-work/cost-shms/`.

- [oracle.py](oracle.py) — `oracle.py <step 0 raw> <out.json>`: Spark 4.1.2's answers for the pin file's 3,480 cells
  (the three functions, four `TIMESTAMP_LTZ` configurations, ruling 6's `TIMESTAMP_NTZ` texts, the multi-row doors),
  taken from step 0's recording.
- [bar.py](bar.py) — `bar.py <spark raw> <base raw> <head raw> <out>`: step 0's whole table on two builds against
  Spark: counts per scope and build, every moved cell, every EQUAL cell lost or gained, every not-EQUAL cell of the
  three functions on the head.
- [d4.py](d4.py) — `d4.py <spark> <base> <head> [unit 1 head]`: unit 1's step 0 cells (`rp.py` `W0`–`W3` and the
  midnight `Z0`–`Z7`) on two builds; the D4 cells are the `json_i` writes and the `b` column of `json_i`, `csv_ns`,
  `csv_0` that unit 1's head still answers unlike Spark.
- [explain.py](explain.py) — `explain.py <warehouse parent> <out>`: `EXPLAIN` and the answer of 15 queries over one
  Iceberg row (`America/Los_Angeles`, ANSI on).
- [cost_summary.py](cost_summary.py) — `cost_summary.py <cost dir>`: medians per case and build, the ratio, the
  control band and K1 against K1x.
- [explain-base.jsonl](explain-base.jsonl), [explain-head.jsonl](explain-head.jsonl) — `explain.py` on the base and
  the head package.
- [bar-summary.md](bar-summary.md) — `bar.py`'s output for the base and the head package.
- [d4.txt](d4.txt) — `d4.py`'s output.
- [doors.md](doors.md) — what the Rust `AnsiDialect` door and the native `repark.sql()` door answer on the head and
  on main.

Up: [../map.md](../map.md).
