# task/wo/ice-ntz-nested-refuse-1/evidence/ — the step-0 measurement of R-011 on main

The cells behind §5 of [../sketch.md](../sketch.md). Measured on main `c516c54a`, 2026-10-20.

- [probe.py](probe.py) — PR #1037's door probe (28 doors, both row-level modes, three zones, three schemas), with two sources added: `ntz_cast` (`CAST(<instant literal> AS TIMESTAMP_NTZ)`) and `ntz_cast_column` (`CAST(<TIMESTAMP column> AS TIMESTAMP_NTZ)`), the spelling the refusal's hint names. `run --engine repark` writes one JSON line per cell.
- [repark-main-c516c54a.jsonl](repark-main-c516c54a.jsonl) — the 1980 cells on main, one array per cell: schema, door, mode, zone, source, and the stored microseconds or the first line of the error.

Up: [../map.md](../map.md).
