# task/wo/map-column-panic-1/evidence/ — measurements of MAP-COLUMN-PANIC-1

Measured 2026-10-20 on main `e6ff5375` (the baseline) and the head of `fix/map-column-panic-1`; the wheels are
`maturin develop --profile ci`, the box load was 5 to 9, every run under `taskset -c 0-25`.

The recorded Spark values live with the pins, not here: the 33 cells of live PySpark 4.1.2 (`local[1]`, zone UTC,
ANSI on) are
[map_column_panic_1_spark_oracle.json](../../../../python/repark/tests/functions/map_column_panic_1_spark_oracle.json),
re-derived by its `_record_map_column_panic_1.py --check` (exit 0).

- [spark-probe-4.1.2.txt](spark-probe-4.1.2.txt) — the ad-hoc probe that fixed the pins' cells: 27 statements
  through live Spark 4.1.2, including `create_map` (Spark answers `UNRESOLVED_ROUTINE`, so RePark keeps it
  unresolved on the SQL door), a mixed `INT`/`STRING` value (Spark raises `CAST_INVALID_INPUT`, a gap this unit
  leaves) and `map()`.
- [red-on-main-rust.txt](red-on-main-rust.txt) — the 20 Rust pins on main: 18 red, 2 green controls. One source
  row panics `list array`; several rows raise `map requires key and value lists to have the same length`.
- [red-on-main-facade.txt](red-on-main-facade.txt) — the 44 facade tests on main: 35 red, 9 green. The 9 are the
  cells that already answered (a column key beside a column value, `NULL` values, `map_keys`, the literal and empty
  controls), the unresolved `create_map` refusal, the ordering refusal and the odd-count divergence pin.
- [fence-mutation-red.txt](fence-mutation-red.txt) — the `REPARK_PANIC_FATAL` pins go red when the resume is
  disabled.
- [corpus-delta.json](corpus-delta.json) — ZONE-ORACLE-1 (PR 1128, not on main when this was measured) replayed on
  main and on the head: `--main --check` on `e6ff5375` reports `drift=0` against the list recorded on `754fb6e5`
  (EQUAL 1150, DIFFERENT 400, REFUSED 20, ERROR 70); on the head the counts are EQUAL 1199, DIFFERENT 411,
  REFUSED 20, ERROR 10. Of the 60 `map(...)` ERROR cells, 49 become EQUAL and leave the list; 11 stay listed as
  DIFFERENT: they are container casts of a map over a timestamp column (`MAP<STRING, TIMESTAMP_NTZ>` of an
  instant, `MAP<STRING, STRING>` of an instant, `MAP<STRING, TIMESTAMP>` of a wall) whose zone answer is wrong
  for CONTAINER-CAST-ZONE-1; the panic used to hide that value, and now the value shows.
  No cell without `map(` changed, and no cell joined the list.
- [zone_oracle_1_main_difference.head.json](zone_oracle_1_main_difference.head.json) — the list re-recorded on the
  head (`--main --base e6ff5375`); it replaces `zone_oracle_1_main_difference.json` when PR 1128 and this change
  meet, whichever merges second.
- [cost.json](cost.json) — 10 runs per query over `range(1000000)`: the shapes main answers take 0.97x
  (literal map), 0.21x (`map(id, id + 1)`) and 0.98x (a materialised literal map) of main's median; the shapes main
  cannot answer take 7 to 25 ms.

Up: [../map.md](../map.md).
