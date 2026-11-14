# task/wo/double-decimal-store-round-1/evidence/fix/ — the build's evidence

- [seats.md](seats.md) — the four built seats, the Rust and facade pins with the red and green runs, the
  step 0 table on the head, and the halt the coercion fuzz raised.
- [spark_pins.py](spark_pins.py) — runs the facade pin file's doors and cells on Spark 4.1.2 (repark and
  pytest stubbed) and records each answer against the pinned value.
- [coercion_fuzz.py](coercion_fuzz.py) — 1,000 doubles through eight DECIMAL-meets-DOUBLE shapes on one
  engine and mode.
- [cost.py](cost.py) — the 10M-row cost probe (cast, `(38,18)`, `decimal = double` filter, stores).
- [union_string_probe.py](union_string_probe.py) — the UNION-to-string cells on one package and mode.
- [nocast_cost.py](nocast_cost.py) — the 10M-row no-cast cost probe: `mixed` (DECIMAL and DOUBLE columns, a
  projection, filter and aggregate over each type apart, a DECIMAL(16,4) to DECIMAL(20,6) store) and `double`
  (no DECIMAL: a DOUBLE projection and store); `REPARK_COST_REPEATS` sets the repeats, `REPARK_COST_EXPLAIN=1`
  records each query's plan instead of timing it.
- [finish_summary.py](finish_summary.py) — the finishing run's medians and ratios per query, the C-012
  like-for-like ratio from the replay harness's per-cell work times, and the per-sample load table.
- [cost-finish.md](cost-finish.md) — the finishing cost run on the merged head against main: the no-cast rows,
  the cast rows re-run, the C-012 like-for-like set.
- [raw/](raw/map.md) — the runs' records.

Up: [../map.md](../map.md).
