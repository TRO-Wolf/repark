# task/wo/ice-ntz-nested-refuse-1/evidence/ — the step-0 measurement of R-011 on main

The cells behind §5 of [../sketch.md](../sketch.md) and C-011 of the ledger. Measured on main `c516c54a` and on the build, 2026-10-20.

- [probe.py](probe.py) — PR #1037's door probe (28 doors, both row-level modes, three zones, three schemas), with two sources added: `ntz_cast` (`CAST(<instant literal> AS TIMESTAMP_NTZ)`) and `ntz_cast_column` (`CAST(<TIMESTAMP column> AS TIMESTAMP_NTZ)`), the spelling the refusal's hint names. `run --engine repark` writes one JSON line per cell.
- [repark-main-c516c54a.jsonl](repark-main-c516c54a.jsonl) — the 1980 cells on main, one array per cell: schema, door, mode, zone, source, and the stored microseconds or the first line of the error.
- [repark-head.jsonl](repark-head.jsonl) — the same 1980 cells on the unit's build (2026-10-20): 192 cells move, every one to the refusal (ledger C-011).

Up: [../map.md](../map.md).
