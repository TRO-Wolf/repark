# map — docs/examples/config

## Purpose

The two measured configuration profiles from PROFILES-1 step 3 (2026-09-12) as
`repark.toml` files — the values were swept one knob at a time over the ten-cell
bed and only measured wins ≥ 5 % on the profile's own workload class earned a
place; the derivation, the near-misses and the no-effect list are in
[../../guide/repark-toml.md](../../guide/repark-toml.md) "The measured `read` and
`write` profiles" and every number traces to a committed CSV under
[../../perf/config-profiles-2026-09-12/](../../perf/config-profiles-2026-09-12/map.md).
Select a profile with `REPARK_ENV` (`REPARK_ENV=read`), or name the file with
`Builder.config_file` / `REPARK_CONFIG`.

## Contents

- [read.toml](read.toml) — analytics scans, joins, aggregations: three measured
  wins (`session.batch_size` / `session.target_partitions`, which emit the
  `repark.batch.size` / `repark.target.partitions` conf keys the sweep measured
  under the `datafusion.execution.*` spellings, plus
  `datafusion.optimizer.repartition_joins` in `conf`).
  pins: profiles-1/C-009, C-010
- [write.toml](write.toml) — the `write` profile is the engine defaults: no
  swept value beat the default by ≥ 5 % on the write cells without costing a
  sibling write shape, so the committed file carries the bare `[write]` profile.
  pins: profiles-1/C-009, C-010
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)
- The `.config()` spellings:
  [../../guide/session-and-conf.md](../../guide/session-and-conf.md)
- The pin: [../../../python/repark-parity/tests/test_profiles_bed.py](../../../python/repark-parity/tests/test_profiles_bed.py)
  and the loader pin in `crates/repark-core/src/config_file/tests/wiring.rs`.
