# task/wo/nan-minmax-1/evidence/delta2/ — the second delta round

NAN-MINMAX-1 part 1's second delta (`d7ae795a`, records `054024d4`), 2026-11-14.

- [oracle_gen.py](oracle_gen.py) — writes the pins' own data sets from the test module (`_write_null_partition_sets`, `_write_spread_nan_set`). Pins: none (generator).
- [oracle_spark.py](oracle_spark.py) and [spark-4.1.2.txt](spark-4.1.2.txt) — Spark 4.1.2's answers for the eight pinned cells (three runs each). Pins: the all-NULL and spread cells of `python/repark/tests/io/test_nan_minmax_stats_1.py`.
- [probe_umbrella.py](probe_umbrella.py), [umbrella-1a8f2749.txt](umbrella-1a8f2749.txt) and [umbrella-d7ae795a.txt](umbrella-d7ae795a.txt) — the aggregate dynamic filter switch, the umbrella key, `RESET` and `spark.conf.unset`, measured on `1a8f2749` and on `d7ae795a`, both doors. Pins: `float_bounds_tests.rs::the_dynamic_filter_umbrella_key_cannot_turn_the_aggregate_filter_on` and the facade's `test_only_the_specific_key_turns_the_aggregate_dynamic_filter_on_on_both_doors`.
- [c012-l4l.md](c012-l4l.md) — the C-012 like-for-like table for `d7ae795a` against main `631d283c`. Pins: none (evidence).

Up: [../map.md](../map.md).
