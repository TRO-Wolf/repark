# task/wo/csv-string-field-text-1/evidence/ — probes and raw cells

- [probes/](probes/map.md) — the comment-free Python behind every number in [../domain-table.md](../domain-table.md)
  and [../sketch.md](../sketch.md).
- [raw/](raw/map.md) — the cells, verdicts and summaries; scratch paths are rewritten to `<scratchpad>`.

## Provenance

- **Builds:** main `S/ddu-work/pkg-rp1base-8bc26cba` (main `6d3739ab` has the same product tree); unit 1
  `S/ddu-work/pkg-rp1d-acd27d6e`. Interpreter `S/lane-nan-write-canonical-1/.venv/bin/python` with the package on
  `PYTHONPATH`, never `-I`; every run printed `repark._native.__file__`.
- **Spark 4.1.2:** the c26 oracle environment, Zulu JDK 17, the Iceberg 1.11.0 Spark runtime jar, through
  `spark_slot.sh`, live.
- **Runs (2026-11-20):** each command under `flock -s <measure lock> nice -n 10 taskset -c 32-47 prlimit
  --as=51539607552`, one part at a time, `TZ=UTC`; warehouses in `<scratchpad>`, deleted after.
- **Commands:** `domain.py gen _ <data> _ _`, then `domain.py <spark|repark> <wh> <data> <out> <part>:<ansi>` for
  the parts S, N, F, T, M, L (and E on RePark); `classify.py <raw> <out> main unit1`; `tables.py <verdicts>`.
  `seat_emu.py repark <wh> <data> <out> text_cast|text_try:<ansi>` on unit 1's package, classified with
  `classify.py <raw> <out> unit1 text_cast text_try`. `kernel.py gen`, then per engine and mode. `castrule.py`.
  `cost.py gen`, then `cost.py <case> base|seat <label> <data> <out>`.

Up: [../map.md](../map.md).
