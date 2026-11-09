# task/wo/door-protection-inventory-1/evidence/ — the inventory, the probe and the measured table

- [inventory.md](inventory.md) — Part 1: one row per protection (router file:line, the Rust pin, the Spark door's equivalent, the Python native door's equivalent or none, a statement), the ten inventoried-but-unmeasured protections, and the entry-point table.
- [probe_doors.py](probe_doors.py) — the measurement script. One door per run writes a leg header and one JSON line per cell; `--render` writes the measured table. Measurement only.
- [raw/native.jsonl](raw/native.jsonl) — the Python native door (`repark.sql()`), 47 cells.
- [raw/facade.jsonl](raw/facade.jsonl) — the facade `spark.sql()` (the Spark door), 47 cells.
- [raw/spark.jsonl](raw/spark.jsonl) — the Spark 4.1.2 oracle, 47 cells (five recorded as skipped).
- [measured.md](measured.md) — Part 2: the verdict per cell, the verdict counts and the reasons.
- [provenance.md](provenance.md) — the wheel and jar hashes, the commands, and what was not done.

Up: [../map.md](../map.md).
